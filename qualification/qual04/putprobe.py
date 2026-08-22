#!/usr/bin/env python3
"""Measure what the state store can absorb, through the S3 API, at the
concurrency the engine actually drives. Prints one integer: object
writes per second.

This exists because two earlier attempts at the same measurement were
wrong in instructive ways, and the campaign's rate gate depends on it:

  * Timing `dd` creating files on the store's filesystem reported ~435
    files/s. That was the cost of forking 435 dd processes - the same
    disk does 14,337 files/s from a single process - and it led to the
    conclusion that a Hetzner volume was the bottleneck, which it was
    not.

  * Measuring serially reported 171 PUT/s where 8 concurrent writers get
    ~495. A pipeline running at parallelism 8 drives concurrent writes,
    so a serial probe understates the ceiling by threefold and would
    refuse rates the store can comfortably serve.

The ceiling this finds is MinIO's own per-request overhead, which is CPU
bound on a shared-vCPU host rather than limited by the disk beneath it.
"""
import argparse
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.config import Config


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--access-key", required=True)
    ap.add_argument("--secret-key", required=True)
    ap.add_argument("--object-bytes", type=int, default=32768)
    ap.add_argument("--concurrency", type=int, default=8)
    ap.add_argument("--objects", type=int, default=1200)
    args = ap.parse_args()

    s3 = boto3.client(
        "s3", endpoint_url=args.endpoint, region_name="us-east-1",
        aws_access_key_id=args.access_key, aws_secret_access_key=args.secret_key,
        config=Config(max_pool_connections=max(args.concurrency * 2, 16),
                      connect_timeout=5, read_timeout=30,
                      retries={"max_attempts": 2},
                      s3={"addressing_style": "path"}))

    # Its own bucket, removed afterwards, so the probe never contributes
    # objects to the bucket whose size the campaign is measuring.
    bucket = "qual04-probe-" + uuid.uuid4().hex[:8]
    s3.create_bucket(Bucket=bucket)
    buf = b"x" * args.object_bytes
    keys = [f"p/{i}" for i in range(args.objects)]

    def put(key):
        s3.put_object(Bucket=bucket, Key=key, Body=buf)

    try:
        start = time.time()
        if args.concurrency <= 1:
            for k in keys:
                put(k)
        else:
            with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
                list(ex.map(put, keys))
        elapsed = time.time() - start
    finally:
        try:
            for i in range(0, len(keys), 1000):
                s3.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": [{"Key": k} for k in keys[i:i + 1000]]})
            s3.delete_bucket(Bucket=bucket)
        except Exception:  # noqa: BLE001 - cleanup must not mask the result
            pass

    print(int(args.objects / elapsed) if elapsed > 0 else 0)
    return 0


if __name__ == "__main__":
    sys.exit(main())
