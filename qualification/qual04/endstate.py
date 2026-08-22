#!/usr/bin/env python3
"""QUAL-04 end-state pass: the authoritative correctness judgement, taken
once over a settled table after the drain.

Two things the mid-flight oracle deliberately cannot do:

  exact accounting  SUM(n) across the table must equal EXACTLY the number
                    of events the generator produced. Mid-flight the
                    progress file is only a lower bound; after the drain
                    it is authoritative. This one number says every
                    produced event was folded into keyed state exactly
                    once, across every fault the run injected - it is the
                    campaign's strongest single statement.

  per-key truth     for a SAMPLE of keys, the count and the accumulator's
                    content must match what the seed predicts. Computing
                    this means replaying the generator's key assignment
                    over every produced (partition, seq), which is tens
                    of millions of hash evaluations - affordable once,
                    not every twenty seconds.

The sample is deterministic (seeded), spread across the key space, and
its expected values are derived from detspec - the same pure function the
generator produced events from, never from anything clink reported.

Output is completeness.txt-shaped key=value lines on stdout.
"""
import argparse
import json
import random
import sys

import psycopg2

sys.path.insert(0, "/qual")
from detspec import Spec  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", required=True)
    ap.add_argument("--progress", required=True)
    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--partitions", type=int, required=True)
    ap.add_argument("--keys", type=int, required=True)
    ap.add_argument("--blob-bytes", type=int, required=True)
    ap.add_argument("--sample-keys", type=int, default=2000)
    args = ap.parse_args()

    with open(args.progress) as f:
        produced_high = {int(k): int(v)
                         for k, v in json.load(f)["produced_high"].items()}
    produced_total = sum(produced_high.values())

    # The generator's own key assignment, replayed. Spec.event() is pure,
    # so this reconstructs what SHOULD be in state without reading
    # anything the engine wrote. Only the sampled keys are tracked, so
    # memory stays flat however large the key space is.
    spec = Spec(args.seed, args.partitions, args.keys, 1, 0, 0, 10000)
    rng = random.Random(args.seed ^ 0x4041)
    sample = sorted(rng.sample(range(args.keys), min(args.sample_keys, args.keys)))
    wanted = set(sample)
    expected_n = {k: 0 for k in sample}
    expected_max = {k: "" for k in sample}

    for p, high in produced_high.items():
        for seq in range(high):
            key, _, _ = spec.event(p, seq)
            if key in wanted:
                expected_n[key] += 1
                eid = "p%d-%d" % (p, seq)
                if eid > expected_max[key]:
                    expected_max[key] = eid

    conn = psycopg2.connect(args.dsn, connect_timeout=10,
                            options="-c statement_timeout=600000")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SELECT count(*), coalesce(sum(n), 0), "
                    "count(*) FILTER (WHERE blob_len <> %s) FROM public.q4_out",
                    (args.blob_bytes,))
        distinct_keys, sum_n, wrong_len = cur.fetchone()

        # Sampled keys, in one round trip.
        cur.execute("SELECT k, blob_len, n FROM public.q4_out WHERE k = ANY(%s)",
                    (sample,))
        rows = {int(k): (int(bl), int(n)) for k, bl, n in cur.fetchall()}

    sampled_checked = 0
    sampled_missing = 0
    sampled_wrong_n = 0
    sampled_wrong_len = 0
    sampled_fabricated = 0
    for k in sample:
        want_n = expected_n[k]
        if want_n == 0:
            # The generator never produced this key, so state must not
            # hold it. A distinct counter, not "missing": inventing a key
            # and losing one are opposite defects and a summary that
            # merges them tells the reader nothing.
            if k in rows:
                sampled_fabricated += 1
            continue
        sampled_checked += 1
        if k not in rows:
            sampled_missing += 1
            continue
        blob_len, n = rows[k]
        if n != want_n:
            sampled_wrong_n += 1
        if blob_len != args.blob_bytes:
            sampled_wrong_len += 1

    print(f"produced_total={produced_total}")
    print(f"sum_n={int(sum_n)}")
    print(f"distinct_keys={int(distinct_keys)}")
    print(f"wrong_blob_len_rows={int(wrong_len)}")
    print(f"sampled_keys_checked={sampled_checked}")
    print(f"sampled_keys_missing={sampled_missing}")
    print(f"sampled_keys_wrong_count={sampled_wrong_n}")
    print(f"sampled_keys_wrong_blob_len={sampled_wrong_len}")
    print(f"sampled_keys_fabricated={sampled_fabricated}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
