#!/usr/bin/env python3
"""Cross-engine Nexmark scoreboard.

Reads per-(query, engine) result JSON files (written by measure_steady.py with an
added query/engine tag), prints a side-by-side steady-state table with the
matched-premise banner, and enforces the correctness gate: for each query both
engines must emit the SAME output-row count (the data-true relation), else the
query is HALTED (no ratio) per pipeline.md.

  scoreboard.py --results-dir results --premise "par=1, 1-partition, hot-path"
"""
import argparse
import glob
import json
import math
import os
import sys


def load(results_dir):
    by_query = {}
    for path in glob.glob(os.path.join(results_dir, "*.json")):
        with open(path) as fh:
            r = json.load(fh)
        by_query.setdefault(r["query"], {})[r["engine"]] = r
    return by_query


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--premise", default="")
    args = ap.parse_args()

    by_query = load(args.results_dir)
    if not by_query:
        print(f"no result files in {args.results_dir}", file=sys.stderr)
        return 1

    print("\n" + "=" * 78)
    print("  clink vs Flink - Nexmark (apples-to-apples)")
    if args.premise:
        print(f"  premise: {args.premise}")
    print("  steady-state events|panes/sec by broker append-time, middle 80%")
    print("=" * 78)
    hdr = f"  {'query':<6} {'clink/s':>14} {'flink/s':>14} {'ratio':>8}  {'gate (rows)':>22}"
    print(hdr)
    print("  " + "-" * 74)

    ratios = []
    gate_failures = []
    for q in sorted(by_query):
        pair = by_query[q]
        c = pair.get("clink")
        f = pair.get("flink")
        if not c or not f:
            have = ",".join(sorted(pair))
            print(f"  {q:<6} INCOMPLETE (have: {have})")
            continue
        # Correctness gate: identical output-row counts = same relation.
        gate_ok = c["count"] == f["count"]
        gate_str = f"{c['count']:,}={f['count']:,}" if gate_ok else f"{c['count']:,}!={f['count']:,}"
        if not gate_ok:
            gate_failures.append(q)
            print(f"  {q:<6} {'-':>14} {'-':>14} {'HALT':>8}  {gate_str:>22}  <- GATE FAIL")
            continue
        ce, fe = c["steady_eps"], f["steady_eps"]
        ratio = (ce / fe) if fe > 0 else float("nan")
        ratios.append(ratio)
        print(f"  {q:<6} {int(ce):>14,} {int(fe):>14,} {ratio:>7.2f}x  {gate_str:>22}")

    print("  " + "-" * 74)
    if ratios:
        geo = math.exp(sum(math.log(r) for r in ratios) / len(ratios))
        print(f"  geomean over {len(ratios)} gate-passing quer{'y' if len(ratios)==1 else 'ies'}:"
              f" clink {geo:.2f}x")
    if gate_failures:
        print(f"  GATE FAILURES (no ratio quoted): {', '.join(gate_failures)}")
    print("=" * 78 + "\n")
    # Non-zero exit if any query had a gate failure (a correctness divergence).
    return 1 if gate_failures else 0


if __name__ == "__main__":
    sys.exit(main())
