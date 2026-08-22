#!/usr/bin/env python3
"""QUAL-04's state-size instrument, measured from OUTSIDE the engine.

Reports TWO numbers, and the distinction is load-bearing:

  live_bytes       the keyed state the job actually holds. Taken from the
                   newest per-subtask manifest, which is the backend's own
                   refcount: it lists every (operator, key) the checkpoint
                   references, with the size of each value object. Summed
                   over deduplicated hashes, that IS the live state.

  footprint_bytes  every object under the prefix, live or not. The store
                   is content-addressed and append-only within a run:
                   each update to a key writes a NEW value object, and the
                   old one is unreferenced the moment the next manifest
                   lands. purge() drops manifests but deliberately leaves
                   the objects; sweep(min_age) is the reclaimer, and it
                   has no caller anywhere in the engine, no CLI and no
                   endpoint. So the footprint grows with UPDATE VOLUME
                   while live state stays flat.

Gating on the footprint would let a campaign "reach" its state target on
garbage, which is why the size gate reads live_bytes and the footprint is
recorded beside it as the operational fact it is.

The manifest format is decoded here rather than through any engine code,
so this stays an independent measurement: u32 entry count, then per
entry u64 operator id, u32 key length + key, u32 hash length + hash,
u64 value size (see S3RemotePool::encode_manifest_).

A campaign without an instrument for its own pass criterion cannot fail,
which is worse than not running. The engine has no live keyed-state size
gauge for the deferring backends (last_snapshot_bytes() returns nothing
for RocksDB, ForSt, the S3 SST tiers and RemoteReadBackend), so rather
than trust a self-report this sums the object bytes the state backend
actually occupies in the store. The oracle never asks clink about clink,
and this is the same discipline applied to the resource claim.

Prints one integer (bytes) on stdout, or a key=value block with --detail.
"""
import argparse
import sys

import boto3
from botocore.config import Config


# Reporting only: how far behind the newest checkpoint a prefix must sit
# before it is called stale. Stale prefixes are still COUNTED (dedup by
# content hash makes that safe); the number is published because a rising
# count means restarts are leaving state behind in abandoned prefixes.
GENERATION_SLACK = 3


def decode_manifest(blob: bytes):
    """(hash, size) for every entry. Mirrors S3RemotePool::encode_manifest_
    exactly; a short or malformed manifest raises rather than silently
    under-reporting, because a quietly small live figure would read as a
    campaign that never grew."""
    out = []
    off = 0

    def u32():
        nonlocal off
        v = int.from_bytes(blob[off:off + 4], "little")
        off += 4
        return v

    def u64():
        nonlocal off
        v = int.from_bytes(blob[off:off + 8], "little")
        off += 8
        return v

    count = u32()
    for _ in range(count):
        u64()                      # operator id
        # NOT `off += u32()`: augmented assignment loads off BEFORE the
        # call, so the nonlocal advance inside u32() is discarded and
        # every subsequent field reads from the wrong offset. Caught by
        # the decoder's own round-trip test, which is the only reason
        # this instrument reports a real number.
        klen = u32()
        off += klen                # key bytes
        hlen = u32()
        h = blob[off:off + hlen].decode("ascii", errors="replace")
        off += hlen
        out.append((h, u64()))
    if off != len(blob):
        raise ValueError(f"manifest has {len(blob) - off} trailing bytes; "
                         f"format mismatch, refusing to report a size")
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", default="")
    ap.add_argument("--access-key", required=True)
    ap.add_argument("--secret-key", required=True)
    ap.add_argument("--detail", action="store_true",
                    help="print the full key=value block instead of one number")
    args = ap.parse_args()

    s3 = boto3.client(
        "s3", endpoint_url=args.endpoint, region_name="us-east-1",
        aws_access_key_id=args.access_key, aws_secret_access_key=args.secret_key,
        config=Config(connect_timeout=5, read_timeout=30,
                      retries={"max_attempts": 2},
                      s3={"addressing_style": "path"}))

    sizes = {}          # object key -> size, for every object under the prefix
    manifests = {}      # subtask prefix -> {checkpoint id: object key}
    token = None
    while True:
        kw = {"Bucket": args.bucket}
        if args.prefix:
            kw["Prefix"] = args.prefix
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for obj in resp.get("Contents") or []:
            key = obj["Key"]
            sizes[key] = int(obj.get("Size", 0))
            # <prefix>/<subtask>/manifests/cp-<id>  (rescaled-cp-<id> is a
            # sidecar the loader PREFERS, so it wins for the same id).
            if "/manifests/" in key:
                head_, leaf = key.rsplit("/manifests/", 1)
                if leaf.startswith("cp-") or leaf.startswith("rescaled-cp-"):
                    cp = leaf.rsplit("cp-", 1)[1]
                    if cp.isdigit():
                        slot = manifests.setdefault(head_, {})
                        prefer = leaf.startswith("rescaled-cp-")
                        cur = slot.get(int(cp))
                        if cur is None or prefer:
                            slot[int(cp)] = key
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")

    footprint = sum(sizes.values())

    # Live state: the newest manifest of every subtask prefix.
    #
    # One prefix per RUNNER, not per generation: a parallelism-4 job whose
    # DAG has four operators writes state/v1/0 .. state/v1/15, and only the
    # keyed operator's four hold anything - the rest carry a 4-byte empty
    # manifest or a source offset. Prefixes also stop advancing when a
    # restart moves an operator to different runner indices, leaving the
    # old prefix behind at the checkpoint it died on, still holding a
    # complete manifest.
    #
    # So this sums the newest manifest of EVERY prefix and deduplicates by
    # content hash. Deduplication is what makes that correct rather than
    # double-counting: a restart relocates the SAME values into the new
    # prefix, and identical content is one object in a content-addressed
    # store, so a value counts once however many prefixes reference it.
    # (Scoping to a "current generation" by checkpoint id was tried and is
    # wrong - it drops the keyed operator's prefixes whenever they sit
    # behind a still-advancing source's, and reported 8 bytes for 600 MB
    # of state.)
    newest_per_prefix = {pref: max(slot) for pref, slot in manifests.items() if slot}
    generation_cp = max(newest_per_prefix.values(), default=0)
    stale_prefixes = sum(1 for cp in newest_per_prefix.values()
                         if cp < generation_cp - GENERATION_SLACK)

    live_by_hash = {}
    live_entries = 0
    manifests_read = 0
    for pref in sorted(manifests):
        slot = manifests[pref]
        if not slot:
            continue
        newest = slot[max(slot)]
        try:
            blob = s3.get_object(Bucket=args.bucket, Key=newest)["Body"].read()
            entries = decode_manifest(blob)
        except Exception as exc:  # noqa: BLE001 - reported, never swallowed
            print(f"statesize: could not read manifest {newest}: {exc}",
                  file=sys.stderr)
            continue
        manifests_read += 1
        live_entries += len(entries)
        for h, size in entries:
            live_by_hash[h] = size

    live = sum(live_by_hash.values())

    if args.detail:
        print(f"state_live_bytes={live}")
        print(f"state_live_keys={live_entries}")
        print(f"state_footprint_bytes={footprint}")
        print(f"state_objects={len(sizes)}")
        print(f"state_manifests_read={manifests_read}")
        print(f"state_generation_checkpoint={generation_cp}")
        print(f"state_stale_prefixes={stale_prefixes}")
        print(f"state_gib={live / (1024 ** 3):.3f}")
        print(f"state_footprint_gib={footprint / (1024 ** 3):.3f}")
        ratio = (footprint / live) if live else 0.0
        print(f"state_footprint_ratio={ratio:.1f}")
    else:
        print(live)
    return 0


if __name__ == "__main__":
    sys.exit(main())
