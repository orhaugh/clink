#!/usr/bin/env python3
"""Prove the QUAL-03 oracle can FAIL.

An oracle that has only ever been run against healthy data is not
evidence of anything - QUAL-01's verifier passed four separate times
while being wrong, and each defect was found only by making it judge
something it should have rejected. So each failure mode is injected here
deliberately, and the oracle must name it - plus the two S3-specific
hazards this oracle exists to be robust against: the paginated-listing
race (a transient hole must NOT be judged a gap until it survives a full
fresh listing) and MinIO's prefix-blind ListMultipartUploads (the
in-doubt witness must filter client-side).

Pure in-memory: the store is a duck-typed fake of the three boto3 calls
the oracle makes, so this runs anywhere python runs, containers or not.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from verifier import Oracle  # noqa: E402

FAILURES = []


def check(name, cond, detail=""):
    tag = "ok" if cond else "FAIL"
    print(f"  {tag}: {name}" + (f" ({detail})" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


class FakeS3:
    """The three calls Oracle makes, over an in-memory dict. ETags are
    content-derived so a mutated object changes its ETag like a real
    store."""

    def __init__(self):
        self.objects = {}   # key -> bytes
        self.uploads = []   # keys with a pending multipart upload

    def put(self, key, lines):
        self.objects[key] = ("\n".join(lines) + "\n").encode() if lines else b""

    def list_objects_v2(self, Bucket, Prefix, ContinuationToken=None):
        contents = [{"Key": k, "ETag": str(hash(v)), "Size": len(v)}
                    for k, v in sorted(self.objects.items())
                    if k.startswith(Prefix)]
        return {"Contents": contents, "IsTruncated": False}

    def get_object(self, Bucket, Key):
        class Body:
            def __init__(self, data):
                self._data = data

            def read(self):
                return self._data

        return {"Body": Body(self.objects[Key])}

    def list_multipart_uploads(self, Bucket, **kw):
        # Deliberately models MinIO: ANY server-side prefix would return
        # nothing, so the fake refuses one outright.
        assert "Prefix" not in kw, "the oracle must not pass a server-side prefix"
        return {"Uploads": [{"Key": k} for k in self.uploads],
                "IsTruncated": False}


def fill(s3, sub, ckpt, partition, lo, hi):
    s3.put(f"q3/sub{sub}-{ckpt}.ndjson",
           [f"p{partition}-{s}" for s in range(lo, hi)])


def kinds(findings):
    return sorted({f["kind"] for f in findings})


def fresh():
    s3 = FakeS3()
    return s3, Oracle(s3, "qual03", "q3")


HIGH = {0: 1000, 1: 1000}


def main() -> int:
    print("clean store: two samples, no findings")
    s3, oracle = fresh()
    fill(s3, 0, 1, 0, 0, 50)
    fill(s3, 0, 2, 0, 50, 100)
    f1, stats = oracle.sample(HIGH)
    f2, stats = oracle.sample(HIGH)
    check("no findings on clean data", not f1 and not f2, f"{f1 + f2}")
    check("lines counted once", stats["lines_total"] == 100, stats["lines_total"])

    print("duplicate: the same event_id in two objects")
    s3, oracle = fresh()
    fill(s3, 0, 1, 0, 0, 50)
    fill(s3, 0, 2, 0, 40, 100)   # 40..49 again
    f, _ = oracle.sample(HIGH)
    check("duplicate named", kinds(f) == ["duplicate"], kinds(f))
    check("all ten counted", len(f) == 10, len(f))

    print("gap: a missing pane, judged only once settled")
    s3, oracle = fresh()
    fill(s3, 0, 1, 0, 0, 50)
    fill(s3, 0, 3, 0, 100, 150)  # pane covering 50..99 absent
    f1, _ = oracle.sample(HIGH)
    check("no gap finding on the first sight (pagination race window)",
          not f1, f1)
    f2, _ = oracle.sample(HIGH)
    check("gap named once settled", kinds(f2) == ["gap"], kinds(f2))
    check("gap sized", f2 and f2[0].get("missing") == 50, f2)

    print("pagination race: a late-listed pane must NOT read as a gap")
    s3, oracle = fresh()
    fill(s3, 0, 1, 0, 0, 50)
    fill(s3, 0, 3, 0, 100, 150)
    f1, _ = oracle.sample(HIGH)          # hole open, not yet judged
    fill(s3, 0, 2, 0, 50, 100)           # the missed pane appears
    f2, _ = oracle.sample(HIGH)
    f3, _ = oracle.sample(HIGH)
    check("no finding once the pane arrived in time", not (f1 + f2 + f3),
          f1 + f2 + f3)

    print("foreign: tokens the generator never produced")
    s3, oracle = fresh()
    s3.put("q3/sub0-1.ndjson", ["p0-0", "p9-5", "not-an-id", "p0-x"])
    f, _ = oracle.sample(HIGH)
    check("foreign named", kinds(f) == ["foreign"], kinds(f))
    check("three foreign lines", len(f) == 3, len(f))

    print("empty object: a zero-byte commit is a corrupted commit")
    s3, oracle = fresh()
    fill(s3, 0, 1, 0, 0, 50)
    s3.put("q3/sub0-2.ndjson", [])
    f, _ = oracle.sample(HIGH)
    check("empty_object named", kinds(f) == ["empty_object"], kinds(f))

    print("mutated object: judged evidence must not change")
    s3, oracle = fresh()
    fill(s3, 0, 1, 0, 0, 50)
    f1, _ = oracle.sample(HIGH)
    fill(s3, 0, 1, 0, 0, 49)     # same key, different content
    f2, _ = oracle.sample(HIGH)
    check("clean before the mutation", not f1, f1)
    check("mutated_object named", kinds(f2) == ["mutated_object"], kinds(f2))

    print("ahead of the progress snapshot: a stat, never a finding")
    s3, oracle = fresh()
    fill(s3, 0, 1, 0, 0, 50)
    f, stats = oracle.sample({0: 30, 1: 1000})   # snapshot behind reality
    check("no finding for running ahead", not f, f)
    check("recorded as a stat", stats.get("ahead_of_snapshot") == 1, stats)

    print("in-doubt witness: pending uploads filtered client-side")
    s3, oracle = fresh()
    fill(s3, 0, 1, 0, 0, 50)
    s3.uploads = ["q3/sub0-2.ndjson", "elsewhere/x.ndjson"]
    _, stats = oracle.sample(HIGH)
    check("only prefix uploads counted", stats["pending_uploads"] == 1, stats)

    print()
    if FAILURES:
        print(f"ORACLE TEST FAILED: {len(FAILURES)} check(s): {FAILURES}")
        return 1
    print("oracle test: every failure mode was named; the oracle can fail.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
