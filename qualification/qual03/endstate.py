#!/usr/bin/env python3
"""QUAL-03 end-state pass: one fresh, full re-read of the settled bucket.

Runs on the ops host AFTER the drain, in a new process with no shared
state, so its counts are independent of the incremental oracle's ledger.
This is the authoritative half of two judgements the mid-flight oracle
deliberately cannot make:

  completeness  everything produced must be committed exactly once. The
                incremental oracle's totals depend on its own read
                ledger; this pass recounts from the store itself.
  foreign tail  a partition committed at or past the generator's FINAL
                produced count holds records this campaign never
                produced. Mid-flight the progress file is only a lower
                bound (QUAL-02's oracle manufactured 32 findings from
                treating it as an upper one); after the drain it is
                authoritative.

Output is completeness.txt-shaped key=value lines on stdout.
"""
import argparse
import json
import sys

import boto3
from botocore.config import Config


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", default="q3")
    ap.add_argument("--access-key", required=True)
    ap.add_argument("--secret-key", required=True)
    ap.add_argument("--progress", required=True)
    args = ap.parse_args()

    with open(args.progress) as f:
        produced_high = {int(k): int(v)
                         for k, v in json.load(f)["produced_high"].items()}

    s3 = boto3.client(
        "s3", endpoint_url=args.endpoint, region_name="us-east-1",
        aws_access_key_id=args.access_key, aws_secret_access_key=args.secret_key,
        config=Config(connect_timeout=5, read_timeout=30,
                      retries={"max_attempts": 3},
                      s3={"addressing_style": "path"}))

    prefix = args.prefix if args.prefix.endswith("/") else args.prefix + "/"
    keys = []
    empty_objects = 0
    token = None
    while True:
        kw = {"Bucket": args.bucket, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for obj in resp.get("Contents") or []:
            if int(obj.get("Size", 0)) == 0:
                empty_objects += 1
            else:
                keys.append(obj["Key"])
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")

    seen = {p: bytearray() for p in produced_high}
    distinct = 0
    dup_total = 0
    foreign_lines = 0
    max_seq = {p: -1 for p in produced_high}
    for key in keys:
        body = s3.get_object(Bucket=args.bucket, Key=key)["Body"].read()
        for line in body.decode("utf-8", errors="replace").splitlines():
            if not line:
                continue
            token_, _, seq_s = line.partition("-")
            if not (token_.startswith("p") and token_[1:].isdigit() and seq_s.isdigit()):
                foreign_lines += 1
                continue
            p, seq = int(token_[1:]), int(seq_s)
            if p not in seen:
                foreign_lines += 1
                continue
            bits = seen[p]
            byte, bit = divmod(seq, 8)
            if byte >= len(bits):
                bits.extend(b"\x00" * (byte + 1 - len(bits)))
            mask = 1 << bit
            if bits[byte] & mask:
                dup_total += 1
            else:
                bits[byte] |= mask
                distinct += 1
            max_seq[p] = max(max_seq[p], seq)

    foreign_ahead = sum(1 for p, high in produced_high.items()
                        if max_seq[p] >= high)

    print(f"produced_total={sum(produced_high.values())}")
    print(f"committed_distinct={distinct}")
    print(f"dup_total={dup_total}")
    print(f"foreign_lines={foreign_lines}")
    print(f"foreign_ahead_partitions={foreign_ahead}")
    print(f"empty_objects={empty_objects}")
    print(f"objects_total={len(keys) + empty_objects}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
