#!/usr/bin/env python3
"""QUAL-05's authoritative correctness pass: run once, after the drain, in a
fresh process, against a settled table.

It recomputes what the sink SHOULD hold from the generator's seed - the same
pure function the generator produced events from - and compares the whole
table against it. Not a sample: the expected map is one entry per key ever
produced, which for this workload is small enough to hold, so every key is
judged rather than a chosen few.

The relationship that makes retention verifiable here: the generator's key
space advances with event time, so all of a key's events fall inside one
epoch. Provided the campaign's TTL exceeds the epoch length, no key's
aggregate is ever truncated mid-life and each key's final n is its true
event count. SUM(n) over the settled table must therefore equal the number
of events produced, exactly - one number asserting that every event was
folded exactly once across every fault in the run, while retention was
releasing state underneath it.

Fabricated keys are counted separately from missing ones. They are opposite
defects (inventing state versus losing it) and a single "mismatch" total
would hide which one happened.
"""
import argparse
import json
import sys

import psycopg2

sys.path.insert(0, "/qual")
from detspec import Spec  # noqa: E402


def expected_counts(spec, produced_high):
    """{key: event count} over everything the generator actually produced."""
    out = {}
    for p in range(spec.partitions):
        high = int(produced_high.get(str(p), produced_high.get(p, 0)))
        for seq in range(high):
            key, _amount, _ts = spec.event(p, seq)
            out[key] = out.get(key, 0) + 1
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", required=True)
    ap.add_argument("--table", default="public.q5_out")
    ap.add_argument("--progress", required=True)
    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--partitions", type=int, required=True)
    ap.add_argument("--keys", type=int, required=True)
    ap.add_argument("--eps", type=int, required=True, help="events/s per partition")
    ap.add_argument("--base-ms", type=int, required=True)
    ap.add_argument("--key-epoch-ms", type=int, required=True)
    args = ap.parse_args()

    with open(args.progress) as fh:
        prog = json.load(fh)
    produced_high = prog.get("produced_high", {})
    produced_total = sum(int(v) for v in produced_high.values())

    spec = Spec(
        args.seed,
        args.partitions,
        args.keys,
        args.eps,
        args.base_ms,
        0,
        10000,
        args.key_epoch_ms,
    )
    expected = expected_counts(spec, produced_high)

    conn = psycopg2.connect(args.dsn, connect_timeout=10)
    conn.autocommit = True
    actual = {}
    sum_n = 0
    null_n = 0
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = 600000")
        cur.execute(f"SELECT k, n FROM {args.table}")
        for k, n in cur:
            if n is None:
                null_n += 1
                continue
            actual[int(k)] = int(n)
            sum_n += int(n)
    conn.close()

    missing = 0
    wrong = 0
    for key, want in expected.items():
        got = actual.get(key)
        if got is None:
            missing += 1
        elif got != want:
            wrong += 1
    fabricated = sum(1 for key in actual if key not in expected)

    print(f"produced_total={produced_total}")
    print(f"sum_n={sum_n}")
    print(f"distinct_keys={len(actual)}")
    print(f"expected_keys={len(expected)}")
    print(f"keys_checked={len(expected)}")
    print(f"keys_missing={missing}")
    print(f"keys_wrong_n={wrong}")
    print(f"keys_fabricated={fabricated}")
    print(f"rows_with_null_n={null_n}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
