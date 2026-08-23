#!/usr/bin/env python3
"""QUAL-05's mid-flight oracle.

Reads the sink table directly and repeatedly, and never asks clink anything.
It judges the properties that are decidable while the job is still running:

  * shrinking      - the number of keys or the total count went BACKWARDS
                     between samples. An upsert table converges upward;
                     going down means committed output was lost.
  * overcount      - the folded total has passed what the generator can
                     possibly have produced. Generous margin, because
                     progress is only a lower bound mid-flight.
  * short_n        - a key whose count is zero or NULL. NULL is checked with
                     IS DISTINCT FROM, because `n <> 0` is NULL for a NULL n
                     and a gate written that way passes against an
                     all-NULL column (QUAL-04 shipped that mistake once).

What it deliberately does NOT judge is completeness: keys legitimately
missing because the pipeline has not caught up, and keys legitimately
absent from STATE because retention released them, are both normal here.
The authoritative pass is endstate.py, after the drain, in a fresh process.
"""
import argparse
import json
import os
import sys
import time

import psycopg2


def read_progress(path):
    """Lower bound on events produced, from the generator's own file."""
    try:
        with open(path) as fh:
            d = json.load(fh)
        return sum(int(v) for v in d.get("produced_high", {}).values())
    except Exception:
        return 0


def sample(cur, table):
    cur.execute(
        f"SELECT count(*), coalesce(sum(n), 0), "
        f"count(*) FILTER (WHERE n IS DISTINCT FROM NULL AND n > 0) "
        f"FROM {table}"
    )
    keys, sum_n, positive = cur.fetchone()
    return {
        "distinct_keys": int(keys),
        "sum_n": int(sum_n),
        "keys_with_positive_n": int(positive),
    }


def write_atomic(path, payload):
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(payload, fh)
    os.replace(tmp, path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", required=True)
    ap.add_argument("--table", default="public.q5_out")
    ap.add_argument("--progress", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--interval-s", type=int, default=20)
    args = ap.parse_args()

    state = {
        "campaign": "QUAL-05",
        "started_wallclock": time.time(),
        "samples": 0,
        "findings": [],
        "max_distinct_keys": 0,
        "max_sum_n": 0,
        "last_stats": {},
        "sample_errors": 0,
        "stuck": False,
        "clean": True,
    }
    consecutive_errors = 0
    stop_file = args.out + ".stop"

    while True:
        if os.path.exists(stop_file):
            break
        try:
            conn = psycopg2.connect(args.dsn, connect_timeout=5)
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = 60000")
                st = sample(cur, args.table)
            conn.close()
            consecutive_errors = 0
        except psycopg2.Error as exc:
            consecutive_errors += 1
            state["sample_errors"] += 1
            state["last_sample_error"] = str(exc)[:300]
            if consecutive_errors >= 10:
                # Ten in a row is not a blip. A campaign with no oracle has
                # no verdict, and saying so is the only honest outcome.
                state["stuck"] = True
                state["clean"] = False
                write_atomic(args.out, state)
            time.sleep(args.interval_s)
            continue

        produced_lower = read_progress(args.progress)
        st["produced_lower_bound"] = produced_lower
        state["samples"] += 1

        if st["distinct_keys"] < state["max_distinct_keys"] or st["sum_n"] < state["max_sum_n"]:
            state["findings"].append(
                {
                    "kind": "shrinking",
                    "sample": state["samples"],
                    "keys": st["distinct_keys"],
                    "prev_max_keys": state["max_distinct_keys"],
                    "sum_n": st["sum_n"],
                    "prev_max_sum_n": state["max_sum_n"],
                }
            )
        if st["distinct_keys"] != st["keys_with_positive_n"]:
            state["findings"].append(
                {
                    "kind": "short_n",
                    "sample": state["samples"],
                    "keys": st["distinct_keys"],
                    "keys_with_positive_n": st["keys_with_positive_n"],
                }
            )
        if produced_lower > 0 and st["sum_n"] > produced_lower * 1.5 + 1_000_000:
            state["findings"].append(
                {
                    "kind": "overcount",
                    "sample": state["samples"],
                    "sum_n": st["sum_n"],
                    "produced_lower_bound": produced_lower,
                }
            )

        state["max_distinct_keys"] = max(state["max_distinct_keys"], st["distinct_keys"])
        state["max_sum_n"] = max(state["max_sum_n"], st["sum_n"])
        state["last_stats"] = st
        state["clean"] = not state["findings"] and not state["stuck"]
        write_atomic(args.out, state)
        time.sleep(args.interval_s)

    state["clean"] = not state["findings"] and not state["stuck"]
    write_atomic(args.out, state)
    return 0


if __name__ == "__main__":
    sys.exit(main())
