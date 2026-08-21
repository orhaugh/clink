#!/usr/bin/env python3
"""QUAL-03 oracle. Runs on the ops host, reads the sink bucket directly,
and judges clink's staged-commit S3 sink without asking clink anything.

The pipeline is one line in, one line out (the line IS the event_id), so
exactly-once has three independently countable failure modes across the
visible objects:

  duplicates   the same event_id visible more than once across all
               objects - a replayed interval committed without its
               predecessor's work being deduplicated.
  gaps         a partition's committed sequences are not a contiguous
               prefix - data was lost. Contiguity is a valid invariant
               because a Kafka partition is read in offset order by one
               source subtask, whole barrier intervals become whole
               objects, and restore-time recovery re-commits pending
               handles at open() before any new pane commits.
  foreign      an event_id outside the range the generator produced -
               fabricated or mis-parsed output.

Two S3-specific modes on top:

  empty_object    a zero-byte object under the sink prefix. The sink
                  prepares nothing for an empty interval (no object at
                  all), so an empty object is a corrupted commit - the
                  exact artefact LocalStack's non-atomic complete
                  produced before the harness moved to MinIO.
  mutated_object  an object whose ETag or size changed after this oracle
                  judged it. Committed objects are immutable; a mutation
                  means something re-completed or overwrote evidence.

The oracle reads INCREMENTALLY: each object is fetched once, on first
sight, and only the listing (cheap, and the source of the ETag ledger)
runs every sample. QUAL-02's oracle re-judged the whole table each pass
and its own statement timeout choked once the sink passed ~3M rows on
cloud disks; an object store priced per GET makes the same mistake
expensive as well as fragile. Per-partition state is a sequence bitmap,
so memory stays flat at millions of events.

Pending multipart uploads are sampled throughout as the in-doubt witness
(the S3 analogue of pg_prepared_xacts). Orphans from a killed incarnation
whose checkpoint never became durable stay pending BY DESIGN (production
expires them with a lifecycle rule), so the count is recorded evidence,
never a finding. The listing deliberately passes NO server-side prefix:
MinIO's ListMultipartUploads matches only an empty prefix or the exact
key, so a server-side prefix silently blinds this witness (measured
against RELEASE.2025-09-07); filtering happens here instead.

Usage:
  verifier.py --endpoint http://... --bucket qual03 --prefix q3 \\
              --access-key ... --secret-key ... \\
              --progress /qual/q3-progress.json --out /qual/q3-verdict.json \\
              [--interval-s 20]
"""
import argparse
import json
import os
import sys
import time

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError


def read_progress(path: str):
    """Per-partition produced high-water (exclusive) from the generator.
    Returns None until the generator has flushed its first snapshot."""
    try:
        with open(path) as f:
            doc = json.load(f)
    except (OSError, ValueError):
        return None
    high = doc.get("produced_high") or {}
    if not high:
        return None
    return {int(k): int(v) for k, v in high.items()}


class PartitionLedger:
    """Sequence bitmap + counters for one partition. A set of 7M ints
    costs hundreds of MB; a bitmap costs under 1 MB per partition."""

    def __init__(self):
        self.bits = bytearray()
        self.distinct = 0
        self.total = 0
        self.max_seq = -1

    def add(self, seq: int) -> bool:
        """Record one sighting. Returns True if it is a DUPLICATE."""
        self.total += 1
        byte, bit = divmod(seq, 8)
        if byte >= len(self.bits):
            self.bits.extend(b"\x00" * (byte + 1 - len(self.bits)))
        mask = 1 << bit
        dup = bool(self.bits[byte] & mask)
        if not dup:
            self.bits[byte] |= mask
            self.distinct += 1
        self.max_seq = max(self.max_seq, seq)
        return dup

    def distinct_up_to(self, seq: int) -> int:
        """How many distinct sequences in [0, seq] have been seen."""
        if seq < 0:
            return 0
        byte, bit = divmod(seq, 8)
        if byte >= len(self.bits):
            return self.distinct
        n = int.from_bytes(self.bits[:byte], "little").bit_count()
        n += (self.bits[byte] & ((1 << (bit + 1)) - 1)).bit_count()
        return n


class Oracle:
    def __init__(self, s3, bucket: str, prefix: str):
        self.s3 = s3
        self.bucket = bucket
        self.prefix = prefix if prefix.endswith("/") else prefix + "/"
        self.read = {}          # key -> (etag, size) at the time it was judged
        self.parts = {}         # partition -> PartitionLedger
        self.settled_max = {}   # partition -> previous sample's max_seq
        self.lines_total = 0
        self.objects_read = 0

    def list_objects(self):
        out = []
        token = None
        while True:
            kw = {"Bucket": self.bucket, "Prefix": self.prefix}
            if token:
                kw["ContinuationToken"] = token
            resp = self.s3.list_objects_v2(**kw)
            out.extend(resp.get("Contents") or [])
            if not resp.get("IsTruncated"):
                return out
            token = resp.get("NextContinuationToken")

    def pending_uploads(self):
        """In-doubt witness. NO server-side prefix - see the module doc."""
        n = 0
        kw = {"Bucket": self.bucket}
        while True:
            resp = self.s3.list_multipart_uploads(**kw)
            for up in resp.get("Uploads") or []:
                if up.get("Key", "").startswith(self.prefix):
                    n += 1
            if not resp.get("IsTruncated"):
                return n
            kw["KeyMarker"] = resp.get("NextKeyMarker")
            kw["UploadIdMarker"] = resp.get("NextUploadIdMarker")

    def judge_line(self, line: str, produced_high: dict, findings: list, key: str):
        token, _, seq_s = line.partition("-")
        valid = token.startswith("p") and token[1:].isdigit() and seq_s.isdigit()
        if not valid:
            findings.append({"kind": "foreign", "object": key, "line": line[:120],
                             "detail": "event_id does not parse as p<part>-<seq>"})
            return
        p, seq = int(token[1:]), int(seq_s)
        if p not in produced_high:
            findings.append({"kind": "foreign", "object": key, "line": line[:120],
                             "detail": "no such partition was produced"})
            return
        ledger = self.parts.setdefault(p, PartitionLedger())
        if ledger.add(seq):
            findings.append({"kind": "duplicate", "partition": p, "seq": seq,
                             "object": key})
        self.lines_total += 1

    def sample(self, produced_high: dict):
        """One incremental judgement pass. Returns (findings, stats)."""
        findings = []
        listing = self.list_objects()
        for obj in listing:
            key, etag, size = obj["Key"], obj.get("ETag", ""), int(obj.get("Size", 0))
            prev = self.read.get(key)
            if prev is not None:
                if prev != (etag, size):
                    findings.append({"kind": "mutated_object", "object": key,
                                     "was": list(prev), "now": [etag, size]})
                    self.read[key] = (etag, size)
                continue
            if size == 0:
                findings.append({"kind": "empty_object", "object": key})
                self.read[key] = (etag, size)
                continue
            body = self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()
            self.objects_read += 1
            self.read[key] = (etag, size)
            for line in body.decode("utf-8", errors="replace").splitlines():
                if line:
                    self.judge_line(line, produced_high, findings, key)

        # Prefix contiguity per partition - judged only up to the PREVIOUS
        # sample's high-water. A paginated listing is not a snapshot, and
        # checkpoint keys sort lexicographically (sub0-2 sorts after
        # sub0-19), so a pane committing mid-listing can put its successor
        # on a page this pass reads while it was missed on a page already
        # read - a transient false gap. Judging one sample behind gives
        # every pane a full fresh listing to appear in; a pane still
        # missing then is genuinely lost, because commits per sink subtask
        # are ordered and recovery re-commits pending panes at open()
        # before new ones. NOT judged either: max_seq against
        # produced_high - the generator's snapshot is a LOWER bound
        # mid-flight (QUAL-02's oracle manufactured 32 "foreign" findings
        # from treating it as an upper one); the campaign driver's
        # post-drain check owns that call.
        ahead = 0
        stats = {"partitions": {}, "lines_total": self.lines_total,
                 "objects_seen": len(self.read), "objects_read": self.objects_read}
        for p, ledger in sorted(self.parts.items()):
            stats["partitions"][p] = {
                "lines_total": ledger.total, "distinct": ledger.distinct,
                "max_seq": ledger.max_seq,
                "produced_high": produced_high.get(p),
            }
            settled = self.settled_max.get(p, -1)
            if settled >= 0:
                have = ledger.distinct_up_to(settled)
                if have != settled + 1:
                    findings.append({"kind": "gap", "partition": p,
                                     "judged_up_to_seq": settled,
                                     "expected_distinct": settled + 1,
                                     "actual_distinct": have,
                                     "missing": (settled + 1) - have})
            self.settled_max[p] = ledger.max_seq
            high = produced_high.get(p)
            if high is not None and ledger.max_seq >= high:
                ahead += 1
        if ahead:
            stats["ahead_of_snapshot"] = ahead
        stats["pending_uploads"] = self.pending_uploads()
        return findings, stats


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", default="q3")
    ap.add_argument("--access-key", required=True)
    ap.add_argument("--secret-key", required=True)
    ap.add_argument("--progress", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--interval-s", type=int, default=20)
    args = ap.parse_args()

    # Bounded everywhere: a PAUSED store (the s3_unavailable fault) hangs
    # connections rather than refusing them, and an oracle that can hang
    # is an oracle whose silence reads as judging. One attempt, short
    # timeouts: the outage becomes counted sample errors, which the stuck
    # detector below bounds - and the fault's dwell is capped under that
    # bound, so injected outages can never fail the oracle by themselves.
    def connect():
        return boto3.client(
            "s3", endpoint_url=args.endpoint, region_name="us-east-1",
            aws_access_key_id=args.access_key,
            aws_secret_access_key=args.secret_key,
            config=Config(connect_timeout=5, read_timeout=10,
                          retries={"max_attempts": 1},
                          s3={"addressing_style": "path"}))

    oracle = Oracle(connect(), args.bucket, args.prefix)

    verdict = {
        "campaign": "QUAL-03",
        "started_wallclock": time.time(),
        "samples": 0,
        "findings": [],          # every defect ever seen, with its sample index
        "max_lines_committed": 0,
        "max_pending_uploads_seen": 0,
        "last_stats": None,
        "sample_errors": 0,
        "stuck": False,
    }
    consecutive_errors = 0

    while True:
        produced_high = read_progress(args.progress)
        if produced_high is None:
            print("verifier: waiting for the generator's first progress snapshot",
                  flush=True)
            time.sleep(5)
            continue
        try:
            findings, stats = oracle.sample(produced_high)
        except (ClientError, BotoCoreError, OSError) as exc:
            # A store blip is a fact worth recording, not a reason to stop
            # judging or to claim a defect in clink. But retrying forever
            # is its own failure: a persistently failing oracle looks
            # alive, writes a verdict with no findings, and judges
            # nothing - which reads as a clean campaign. A persistent
            # failure is written INTO the verdict as a stuck oracle, which
            # the summary treats as a failed campaign, never a pass.
            verdict["sample_errors"] += 1
            consecutive_errors += 1
            verdict["last_sample_error"] = f"{exc.__class__.__name__}: {exc}"
            print(f"verifier: sample failed ({exc.__class__.__name__}): {exc}",
                  flush=True)
            if consecutive_errors >= 10:
                verdict["stuck"] = True
                verdict["clean"] = False
                tmp = args.out + ".tmp"
                with open(tmp, "w") as f:
                    json.dump(verdict, f, indent=2)
                os.replace(tmp, args.out)
                print("verifier: 10 consecutive failed samples - the oracle cannot "
                      "judge. Recorded as stuck; this campaign has no verdict.",
                      flush=True)
            time.sleep(5)
            oracle.s3 = connect()
            continue
        consecutive_errors = 0

        verdict["samples"] += 1
        verdict["last_stats"] = stats
        verdict["max_lines_committed"] = max(verdict["max_lines_committed"],
                                             stats["lines_total"])
        verdict["max_pending_uploads_seen"] = max(verdict["max_pending_uploads_seen"],
                                                  stats["pending_uploads"])
        for f in findings:
            f["sample"] = verdict["samples"]
            verdict["findings"].append(f)

        verdict["clean"] = not verdict["findings"]
        tmp = args.out + ".tmp"
        with open(tmp, "w") as f:
            json.dump(verdict, f, indent=2)
        os.replace(tmp, args.out)

        print(f"verifier: sample {verdict['samples']}: "
              f"{stats['lines_total']} lines committed across "
              f"{stats['objects_seen']} objects, "
              f"{stats['pending_uploads']} uploads pending, "
              f"{len(verdict['findings'])} findings so far", flush=True)
        time.sleep(args.interval_s)


if __name__ == "__main__":
    sys.exit(main())
