#!/usr/bin/env python3
"""Cross-engine latency scoreboard.

Reads the per-engine latency result JSON files (written by measure_latency.py),
prints the percentiles side by side under the matched-premise banner, and
enforces the gates: count, positional content, pacer rate. Any failed gate on
either engine HALTS with a non-zero exit - no number is quotable (pipeline.md,
"Latency axis").

  latency_scoreboard.py --results-dir results-latency --premise "..."
"""
import argparse
import glob
import json
import os
import sys


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--premise", default="")
    args = ap.parse_args()

    by_engine = {}
    for path in glob.glob(os.path.join(args.results_dir, "*-lat.json")):
        with open(path) as fh:
            r = json.load(fh)
        by_engine[r["engine"]] = r
    if not by_engine:
        print(f"no latency result files in {args.results_dir}", file=sys.stderr)
        return 1

    print("\n" + "=" * 92)
    print("  clink vs Flink - Nexmark q0 per-record latency (apples-to-apples)")
    if args.premise:
        print(f"  premise: {args.premise}")
    print("  latency = output broker-append minus input broker-append per record,")
    print("  one broker clock, ms resolution, steady window (head/tail trimmed)")
    print("=" * 92)
    print(
        f"  {'engine':<7} {'p50':>6} {'p90':>6} {'p99':>6} {'p99.9':>7} {'max':>7} "
        f"{'mean':>7} {'in ev/s':>9}  {'gate':>22}"
    )
    print("  " + "-" * 88)

    all_ok = True
    for engine in ("clink", "flink"):
        r = by_engine.get(engine)
        if r is None:
            print(f"  {engine:<7} (no result)")
            all_ok = False
            continue
        gates = []
        if not r.get("ok_count"):
            gates.append(f"COUNT {r.get('count')}/{r.get('expected')}")
        if not r.get("ok_content"):
            gates.append(f"CONTENT x{r.get('mismatches')}")
        if not r.get("ok_pace"):
            gates.append(f"PACE {r.get('achieved_in_rate')}")
        gate = "ok" if not gates else "FAIL " + ",".join(gates)
        all_ok = all_ok and not gates
        ms = lambda k: ("-" if r.get(k) is None else str(r.get(k)))
        print(
            f"  {engine:<7} {ms('p50_ms'):>6} {ms('p90_ms'):>6} {ms('p99_ms'):>6} "
            f"{ms('p999_ms'):>7} {ms('max_ms'):>7} {ms('mean_ms'):>7} "
            f"{r.get('achieved_in_rate', 0):>9}  {gate:>22}"
        )

    print("  " + "-" * 88)
    if not all_ok:
        print("  GATE FAILURE: a gate failed above - no latency number is quotable.")
        print("=" * 92)
        return 1
    print("  all gates passed (count, positional content, pacer rate)")
    print("=" * 92)
    return 0


if __name__ == "__main__":
    sys.exit(main())
