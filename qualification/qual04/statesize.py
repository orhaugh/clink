#!/usr/bin/env python3
"""QUAL-04's state-size instrument: the total bytes of keyed state the
job is holding, measured from OUTSIDE the engine.

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


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", default="")
    ap.add_argument("--access-key", required=True)
    ap.add_argument("--secret-key", required=True)
    ap.add_argument("--detail", action="store_true",
                    help="also print the object count and largest object")
    args = ap.parse_args()

    s3 = boto3.client(
        "s3", endpoint_url=args.endpoint, region_name="us-east-1",
        aws_access_key_id=args.access_key, aws_secret_access_key=args.secret_key,
        config=Config(connect_timeout=5, read_timeout=30,
                      retries={"max_attempts": 2},
                      s3={"addressing_style": "path"}))

    total = 0
    objects = 0
    largest = 0
    token = None
    while True:
        kw = {"Bucket": args.bucket}
        if args.prefix:
            kw["Prefix"] = args.prefix
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for obj in resp.get("Contents") or []:
            size = int(obj.get("Size", 0))
            total += size
            objects += 1
            largest = max(largest, size)
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")

    if args.detail:
        print(f"state_bytes={total}")
        print(f"state_objects={objects}")
        print(f"state_largest_object_bytes={largest}")
        print(f"state_gib={total / (1024 ** 3):.3f}")
    else:
        print(total)
    return 0


if __name__ == "__main__":
    sys.exit(main())
