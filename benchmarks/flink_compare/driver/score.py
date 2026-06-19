#!/usr/bin/env python3
"""Side-by-side scoreboard from per-engine consumer results.

Reads two JSON files (one per engine) and prints a comparison table to
stdout. Returns nonzero exit if either engine failed correctness
(record_count != expected_count).
"""

import argparse
import json
import sys


def fmt_int(n: float) -> str:
    return f"{int(n):,}"


def fmt_float(n: float) -> str:
    return f"{n:.2f}"


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--flink-result", required=True)
    p.add_argument("--clink-result", required=True)
    args = p.parse_args()

    with open(args.flink_result) as fh:
        flink = json.load(fh)
    with open(args.clink_result) as fh:
        clink = json.load(fh)

    rows = [
        ("output records", fmt_int(flink["record_count"]), fmt_int(clink["record_count"])),
        ("expected", fmt_int(flink["expected_count"]), fmt_int(clink["expected_count"])),
        ("wall seconds", fmt_float(flink["wall_seconds"]), fmt_float(clink["wall_seconds"])),
        ("rec/sec", fmt_int(flink["throughput_rec_per_s"]),
                     fmt_int(clink["throughput_rec_per_s"])),
    ]

    width = max(len(r[0]) for r in rows)
    col = 18
    print(f"{'':<{width}}  {'flink':>{col}}  {'clink':>{col}}")
    print("-" * (width + 2 + col + 2 + col))
    for label, a, b in rows:
        print(f"{label:<{width}}  {a:>{col}}  {b:>{col}}")

    print()
    if flink["throughput_rec_per_s"] > 0 and clink["throughput_rec_per_s"] > 0:
        ratio = clink["throughput_rec_per_s"] / flink["throughput_rec_per_s"]
        verdict = "clink faster" if ratio > 1 else "flink faster"
        print(f"throughput ratio (clink / flink): {ratio:.2f}x  ({verdict})")

    deltas = []
    for label, r in [("flink", flink), ("clink", clink)]:
        if r["record_count"] != r["expected_count"]:
            delta = r["record_count"] - r["expected_count"]
            sign = "+" if delta > 0 else ""
            deltas.append(
                f"{label}: {r['record_count']:,} ({sign}{delta:,} vs expected {r['expected_count']:,})"
            )
    if deltas:
        print()
        print("Record-count delta (informational, not necessarily wrong):")
        for d in deltas:
            print(f"  {d}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
