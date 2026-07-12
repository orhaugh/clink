#!/usr/bin/env python3
"""Measure embedded clink first-row latency and memory footprint.

`clink run -e "<sql>"` starts the whole engine in-process (JobManager +
TaskManager, no daemons), compiles and submits the job, and a bare SELECT
prints its result rows to stdout. This harness times the path from process
start to the FIRST result row, and measures peak resident memory at a tiny and
a large input so the per-in-flight-record cost can be estimated.

    python3 benchmarks/embedded_footprint.py <path-to-clink> [--runs N]

Numbers are only meaningful from a RELEASE build. Prints a JSON summary and a
human table; exits 0 regardless (measurement, not a gate - the budgeted
regression check lives in the test suite).
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import statistics
import subprocess
import sys
import tempfile
import time


def _write_ndjson(path: str, rows: int) -> None:
    with open(path, "w") as f:
        for i in range(rows):
            f.write(json.dumps({"user_id": i % 8, "amount": i}) + "\n")


def _query(path: str) -> str:
    # A bare multi-column SELECT over a file (row-channel) source -> the print
    # sink. Matches the embedded engine's tested bare-SELECT-to-print path.
    return (
        "CREATE TABLE orders (user_id BIGINT, amount BIGINT) "
        f"WITH (connector='file', format='json', path='{path}'); "
        "SELECT user_id, amount FROM orders"
    )


def first_row_ms(clink: str, query: str) -> float | None:
    """Wall-clock ms from spawning `clink run -e` to its first result row."""
    t0 = time.perf_counter()
    p = subprocess.Popen(
        [clink, "run", "-e", query],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    ms: float | None = None
    assert p.stdout is not None
    for line in p.stdout:
        if line.startswith("{"):  # a result row (JSON object); not an [info] log
            ms = (time.perf_counter() - t0) * 1000.0
            break
    p.stdout.close()
    p.wait()
    return ms


def peak_rss_mb(clink: str, query: str) -> float | None:
    """Peak resident set size (MB) of one `clink run -e`, via /usr/bin/time."""
    if platform.system() == "Darwin":
        r = subprocess.run(
            ["/usr/bin/time", "-l", clink, "run", "-e", query],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        m = re.search(r"(\d+)\s+maximum resident set size", r.stderr)
        return int(m.group(1)) / (1024.0 * 1024.0) if m else None  # bytes -> MB
    r = subprocess.run(
        ["/usr/bin/time", "-v", clink, "run", "-e", query],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    m = re.search(r"Maximum resident set size \(kbytes\):\s+(\d+)", r.stderr)
    return int(m.group(1)) / 1024.0 if m else None  # KB -> MB


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("clink", help="path to the clink binary (a Release build)")
    ap.add_argument("--runs", type=int, default=20, help="first-row samples")
    ap.add_argument("--small", type=int, default=10, help="rows for the idle/startup measure")
    ap.add_argument("--large", type=int, default=1_000_000, help="rows for the under-load measure")
    ap.add_argument(
        "--budget-ms",
        type=float,
        default=None,
        help="regression gate: exit non-zero if the median first-row exceeds this "
        "(Release builds only - a Debug build is far slower)",
    )
    args = ap.parse_args()

    with tempfile.TemporaryDirectory() as d:
        small = os.path.join(d, "small.ndjson")
        large = os.path.join(d, "large.ndjson")
        _write_ndjson(small, args.small)
        _write_ndjson(large, args.large)

        # First-row latency over N runs on the small input (warm the FS cache once).
        first_row_ms(args.clink, _query(small))
        samples = [first_row_ms(args.clink, _query(small)) for _ in range(args.runs)]
        samples = [s for s in samples if s is not None]

        rss_small = peak_rss_mb(args.clink, _query(small))
        rss_large = peak_rss_mb(args.clink, _query(large))

    per_record_bytes = None
    if rss_small is not None and rss_large is not None and args.large > args.small:
        per_record_bytes = (rss_large - rss_small) * 1024 * 1024 / (args.large - args.small)

    summary = {
        "runs": len(samples),
        "first_row_ms": {
            "min": round(min(samples), 1) if samples else None,
            "median": round(statistics.median(samples), 1) if samples else None,
            "max": round(max(samples), 1) if samples else None,
        },
        "rss_mb": {
            "startup_small_input": round(rss_small, 1) if rss_small is not None else None,
            "under_load_large_input": round(rss_large, 1) if rss_large is not None else None,
            "per_inflight_record_bytes": round(per_record_bytes, 1)
            if per_record_bytes is not None
            else None,
        },
        "large_rows": args.large,
    }
    print(json.dumps(summary, indent=2))
    fr = summary["first_row_ms"]
    print(
        f"\nfirst row: min {fr['min']} / median {fr['median']} / max {fr['max']} ms "
        f"({len(samples)} runs)"
    )
    rss = summary["rss_mb"]
    print(
        f"peak RSS: {rss['startup_small_input']} MB (startup) -> "
        f"{rss['under_load_large_input']} MB (under {args.large:,} rows); "
        f"~{rss['per_inflight_record_bytes']} bytes/record"
    )

    if args.budget_ms is not None:
        median = fr["median"]
        if median is None:
            print("FAIL: no first-row samples", file=sys.stderr)
            return 1
        if median > args.budget_ms:
            print(
                f"FAIL: median first row {median} ms exceeds budget {args.budget_ms} ms",
                file=sys.stderr,
            )
            return 1
        print(f"OK: median first row {median} ms within budget {args.budget_ms} ms")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
