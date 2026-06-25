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
    ap.add_argument(
        "--geomean-queries",
        default="",
        help="space-separated queries whose rate is a comparable throughput proxy "
        "(output ~ input scale). Others still gate + print but their rate is marked "
        "indicative and excluded from the geomean. Empty = all.",
    )
    args = ap.parse_args()
    geomean_set = set(args.geomean_queries.split()) if args.geomean_queries else None

    by_query = load(args.results_dir)
    if not by_query:
        print(f"no result files in {args.results_dir}", file=sys.stderr)
        return 1

    print("\n" + "=" * 92)
    print("  clink vs Flink - Nexmark (apples-to-apples)")
    if args.premise:
        print(f"  premise: {args.premise}")
    print("  rate = steady events|panes/sec (broker append-time, middle 80%);")
    print("  eff  = input events / measured CPU-second (Cores*Time normalised, parallelism-independent)")
    print("=" * 92)
    print(f"  {'query':<6} {'clink/s':>11} {'flink/s':>11} {'rate':>7}   "
          f"{'clk ev/cpus':>11} {'flk ev/cpus':>11} {'eff':>7}  {'gate':>16}")
    print("  " + "-" * 88)

    rate_ratios = []
    eff_ratios = []
    gate_failures = []
    for q in sorted(by_query):
        pair = by_query[q]
        c = pair.get("clink")
        f = pair.get("flink")
        if not c or not f:
            have = ",".join(sorted(pair))
            print(f"  {q:<6} INCOMPLETE (have: {have})")
            continue
        gate_ok = c["count"] == f["count"]
        gate_str = f"{c['count']:,}={f['count']:,}" if gate_ok else f"{c['count']:,}!={f['count']:,}"
        if not gate_ok:
            gate_failures.append(q)
            print(f"  {q:<6} {'-':>11} {'-':>11} {'HALT':>7}   "
                  f"{'-':>11} {'-':>11} {'-':>7}  {gate_str:>16}  GATE FAIL")
            continue
        ce, fe = c["steady_eps"], f["steady_eps"]
        rate = (ce / fe) if fe > 0 else float("nan")
        # CPU-normalised efficiency: input events per measured CPU-second. This is
        # parallelism-independent (the canonical Nexmark Cores*Time basis), so it
        # is comparable for ALL gate-passing queries - including q8, whose CPU
        # reflects the join work over all input, not its tiny output.
        cev = c.get("events_per_cpu_sec", 0.0)
        fev = f.get("events_per_cpu_sec", 0.0)
        eff = (cev / fev) if fev > 0 else float("nan")
        # Both rate AND eff geomeans are over the SAME throughput set (output ~
        # input scale, job CPU-bound over a short window). A low-input/long-wall
        # query like q8 is excluded from BOTH: its rate is output-burst-bound and
        # its eff is baseline-CPU-bound (the engine's idle/runtime overhead over a
        # long watermark-gated wall dominates the per-event CPU). Both shown,
        # marked *, excluded from the aggregates.
        comparable = geomean_set is None or q in geomean_set
        mark = "" if comparable else "*"
        if comparable:
            rate_ratios.append(rate)
            if eff == eff and eff > 0:  # not nan
                eff_ratios.append(eff)
        print(f"  {q:<6} {int(ce):>11,} {int(fe):>11,} {rate:>5.2f}x{mark:<1}  "
              f"{int(cev):>11,} {int(fev):>11,} {eff:>5.2f}x{mark:<1} {gate_str:>16}")

    print("  " + "-" * 88)
    if rate_ratios:
        geo = math.exp(sum(math.log(r) for r in rate_ratios) / len(rate_ratios))
        print(f"  rate geomean over {len(rate_ratios)} throughput quer"
              f"{'y' if len(rate_ratios)==1 else 'ies'} (* = excluded, output-bound): clink {geo:.2f}x")
    if eff_ratios:
        geo = math.exp(sum(math.log(r) for r in eff_ratios) / len(eff_ratios))
        print(f"  eff  geomean over {len(eff_ratios)} throughput quer"
              f"{'y' if len(eff_ratios)==1 else 'ies'}: clink {geo:.2f}x  "
              f"(CPU footprint incl. engine/JVM baseline; * excluded = baseline-bound)")
    if gate_failures:
        print(f"  GATE FAILURES (no ratio quoted): {', '.join(gate_failures)}")
    print("=" * 92 + "\n")
    # Non-zero exit if any query had a gate failure (a correctness divergence).
    return 1 if gate_failures else 0


if __name__ == "__main__":
    sys.exit(main())
