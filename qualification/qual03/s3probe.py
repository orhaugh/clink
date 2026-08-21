#!/usr/bin/env python3
"""Tiny store probes for QUAL-03's campaign gates. One bounded call per
invocation, one integer on stdout: the campaign driver composes these
over ssh the way QUAL-02 composed psql one-liners.

  objects   visible objects under the prefix
  uploads   pending multipart uploads under the prefix (client-side
            filter - MinIO's ListMultipartUploads matches only an empty
            prefix or the exact key, so a server-side prefix silently
            returns nothing)
"""
import argparse
import sys

import boto3
from botocore.config import Config


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["objects", "uploads"])
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", default="q3")
    ap.add_argument("--access-key", required=True)
    ap.add_argument("--secret-key", required=True)
    args = ap.parse_args()

    s3 = boto3.client(
        "s3", endpoint_url=args.endpoint, region_name="us-east-1",
        aws_access_key_id=args.access_key, aws_secret_access_key=args.secret_key,
        config=Config(connect_timeout=5, read_timeout=10,
                      retries={"max_attempts": 1},
                      s3={"addressing_style": "path"}))
    prefix = args.prefix if args.prefix.endswith("/") else args.prefix + "/"

    n = 0
    if args.mode == "objects":
        token = None
        while True:
            kw = {"Bucket": args.bucket, "Prefix": prefix}
            if token:
                kw["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kw)
            n += len(resp.get("Contents") or [])
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
    else:
        kw = {"Bucket": args.bucket}
        while True:
            resp = s3.list_multipart_uploads(**kw)
            for up in resp.get("Uploads") or []:
                if up.get("Key", "").startswith(prefix):
                    n += 1
            if not resp.get("IsTruncated"):
                break
            kw["KeyMarker"] = resp.get("NextKeyMarker")
            kw["UploadIdMarker"] = resp.get("NextUploadIdMarker")
    print(n)
    return 0


if __name__ == "__main__":
    sys.exit(main())
