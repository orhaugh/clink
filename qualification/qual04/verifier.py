#!/usr/bin/env python3
"""QUAL-04 oracle, mid-flight half. Runs on the ops host, reads the
verification table directly, and never asks clink anything.

QUAL-04's subject is keyed state AT SIZE, so the failure modes differ
from the delivery campaigns. The pipeline holds one fat accumulator per
key and maintains one row per key in Postgres through an upsert sink, so
three things are checkable cheaply and continuously:

  short_blob    a row whose blob_len is not exactly the configured blob
                size. The accumulator is a fixed-width padded string, so
                any other length means state was truncated, partially
                written, or restored from a torn value.
  shrinking     SUM(n) going DOWN between samples, or a key's own count
                going down. Counts are monotonic under a correct
                restore: a replay re-counts events the accumulator
                already had, so the total may pause or jump, never fall.
  overcount     SUM(n) exceeding what the generator has produced. The
                progress snapshot is a LOWER bound mid-flight (QUAL-02's
                oracle manufactured findings by treating it as an upper
                one), so this fires only on a margin far beyond snapshot
                lag, and the exact equality is asserted after the drain
                by endstate.py instead.

Per-key verification against the seed is deliberately NOT done here. It
means walking every produced (partition, seq) to work out which keys they
map to, which is tens of millions of hash evaluations - far too slow for
a 20-second sample loop, and pointless while the table is still moving.
endstate.py does it once, authoritatively, over a settled table.

Usage:
  verifier.py --dsn postgresql://... --progress /qual/q4-progress.json \\
              --blob-bytes 20480 --out /qual/q4-verdict.json [--interval-s 20]
"""
import argparse
import json
import os
import sys
import time

import psycopg2


def read_progress(path: str):
    """Total events the generator has recorded producing. A LOWER bound
    on what it has actually produced."""
    try:
        with open(path) as f:
            doc = json.load(f)
    except (OSError, ValueError):
        return None
    high = doc.get("produced_high") or {}
    if not high:
        return None
    return sum(int(v) for v in high.values())


def sample(cur, blob_bytes: int, produced_lower: int, prev):
    """One judgement pass over the verification table. Returns
    (findings, stats)."""
    findings = []
    # One scan, three answers. count(*) is the distinct-key count because
    # the primary key is k.
    cur.execute("""
        SELECT count(*),
               coalesce(sum(n), 0),
               count(*) FILTER (WHERE blob_len IS DISTINCT FROM %s),
               coalesce(min(blob_len), -1),
               coalesce(max(blob_len), -1)
        FROM public.q4_out
    """, (blob_bytes,))
    keys, total_n, wrong_len, min_len, max_len = cur.fetchone()
    stats = {
        "distinct_keys": int(keys),
        "sum_n": int(total_n),
        "wrong_blob_len_rows": int(wrong_len),
        "min_blob_len": int(min_len),
        "max_blob_len": int(max_len),
        "produced_lower_bound": produced_lower,
    }

    if wrong_len:
        cur.execute(
            "SELECT k, blob_len FROM public.q4_out "
            "WHERE blob_len IS DISTINCT FROM %s LIMIT 5",
            (blob_bytes,))
        findings.append({
            "kind": "short_blob", "rows": int(wrong_len),
            "expected_len": blob_bytes,
            "examples": [{"k": int(k), "blob_len": int(bl)} for k, bl in cur.fetchall()],
            "detail": "a per-key accumulator is not the full configured width",
        })

    if prev is not None:
        if stats["sum_n"] < prev["sum_n"]:
            findings.append({
                "kind": "shrinking", "was": prev["sum_n"], "now": stats["sum_n"],
                "detail": "total event count fell between samples; counts are "
                          "monotonic under a correct restore",
            })
        if stats["distinct_keys"] < prev["distinct_keys"]:
            findings.append({
                "kind": "shrinking", "was": prev["distinct_keys"],
                "now": stats["distinct_keys"],
                "detail": "distinct key count fell between samples; keyed state "
                          "was lost rather than restored",
            })

    # Generous margin: produced_lower is a snapshot the generator flushes
    # periodically, so the engine legitimately runs ahead of it. Only a
    # total far beyond any plausible lag is a finding; the exact equality
    # is endstate.py's job once the generator has stopped.
    if produced_lower and stats["sum_n"] > produced_lower * 1.5 + 1_000_000:
        findings.append({
            "kind": "overcount", "sum_n": stats["sum_n"],
            "produced_lower_bound": produced_lower,
            "detail": "counted far more events than the generator can have "
                      "produced, beyond any snapshot lag",
        })
    return findings, stats


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", required=True)
    ap.add_argument("--progress", required=True)
    ap.add_argument("--blob-bytes", type=int, required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--interval-s", type=int, default=20)
    args = ap.parse_args()

    verdict = {
        "campaign": "QUAL-04",
        "started_wallclock": time.time(),
        "samples": 0,
        "findings": [],
        "max_distinct_keys": 0,
        "max_sum_n": 0,
        "last_stats": None,
        "sample_errors": 0,
        "stuck": False,
    }
    consecutive_errors = 0

    def connect():
        # Same bounds and reasoning as QUAL-02's oracle: a paused or
        # overloaded server must turn into counted sample errors rather
        # than a silent hang, and 60s of statement headroom keeps a slow
        # plan over a large table from being read as a dead oracle.
        c = psycopg2.connect(args.dsn, connect_timeout=5,
                             options="-c statement_timeout=60000")
        c.autocommit = True
        return c

    conn = connect()
    prev = None

    while True:
        produced_lower = read_progress(args.progress) or 0
        try:
            with conn.cursor() as cur:
                findings, stats = sample(cur, args.blob_bytes, produced_lower, prev)
        except psycopg2.Error as exc:
            verdict["sample_errors"] += 1
            consecutive_errors += 1
            verdict["last_sample_error"] = f"{exc.__class__.__name__}: {exc}"
            print(f"verifier: sample failed ({exc.__class__.__name__}): {exc}", flush=True)
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
            try:
                conn.close()
            except psycopg2.Error:
                pass
            time.sleep(5)
            try:
                conn = connect()
            except psycopg2.Error:
                pass
            continue
        consecutive_errors = 0
        prev = stats

        verdict["samples"] += 1
        verdict["last_stats"] = stats
        verdict["max_distinct_keys"] = max(verdict["max_distinct_keys"],
                                           stats["distinct_keys"])
        verdict["max_sum_n"] = max(verdict["max_sum_n"], stats["sum_n"])
        for f in findings:
            f["sample"] = verdict["samples"]
            verdict["findings"].append(f)

        verdict["clean"] = not verdict["findings"]
        tmp = args.out + ".tmp"
        with open(tmp, "w") as f:
            json.dump(verdict, f, indent=2)
        os.replace(tmp, args.out)

        print(f"verifier: sample {verdict['samples']}: "
              f"{stats['distinct_keys']} keys, {stats['sum_n']} events counted, "
              f"{len(verdict['findings'])} findings so far", flush=True)
        time.sleep(args.interval_s)


if __name__ == "__main__":
    sys.exit(main())
