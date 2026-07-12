#!/usr/bin/env python3
"""Measure clink's parallelism density envelope.

clink runs a thread per operator subtask, so a job of W operators at parallelism
P uses ~W*P threads. On a machine with C cores, throughput should rise with P
until the threads saturate the cores, then plateau and eventually degrade as
oversubscription adds context-switch and mutex-contention overhead. This harness
finds where that happens for a representative keyed GROUP BY (source -> shuffle
-> aggregate -> sink), so the supported envelope can be stated and the
"do we need a cooperative scheduler?" question answered with data.

    python3 benchmarks/density_envelope.py <path-to-clink> [--pars 1,2,4,8,...]

Throughput is total rows / wall time for a large input at each parallelism (the
input is sized so processing dominates the ~0.15 s startup). Numbers are only
meaningful from a RELEASE build. Prints a JSON summary and a table.

The embedded engine has a fixed slot count, so a job whose ops x parallelism
exceeds it is rejected up front - those parallelisms are reported as "rejected".
"""

from __future__ import annotations

import argparse
import json
import os
import re
import statistics
import subprocess
import tempfile
import time


def _write_ndjson(path: str, rows: int, keys: int) -> None:
    with open(path, "w") as f:
        for i in range(rows):
            f.write(json.dumps({"user_id": i % keys, "amount": i}) + "\n")


def _query(src: str) -> str:
    return (
        "CREATE TABLE orders (user_id BIGINT, amount BIGINT) "
        f"WITH (connector='file', format='json', path='{src}'); "
        "CREATE TABLE sink (user_id BIGINT, total BIGINT) WITH (connector='blackhole'); "
        "INSERT INTO sink SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id"
    )


def _run(clink: str, par: int, query: str) -> tuple[float, int | None, bool, bool]:
    """One `clink run --parallelism P`; return (wall_s, tasks, ok, rejected)."""
    t0 = time.perf_counter()
    p = subprocess.run(
        [clink, "run", "--parallelism", str(par), "-e", query],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    wall = time.perf_counter() - t0
    m = re.search(r"tasks=(\d+)", p.stderr)
    tasks = int(m.group(1)) if m else None
    ok = "jm.complete" in p.stderr and "ok" in p.stderr
    rejected = "insufficient free slots" in p.stderr or "slots" in p.stderr and "need" in p.stderr
    return wall, tasks, ok, rejected


def _median_wall(clink: str, par: int, query: str, reps: int) -> tuple[float, bool, int | None]:
    """Median wall over `reps` runs, plus whether the job was rejected and the
    actual thread/task count the JobManager reported (source + shuffle + agg +
    sink roles times parallelism, which grows faster than a par-1 probe implies)."""
    walls = []
    rejected = False
    tasks_seen: int | None = None
    for _ in range(reps):
        wall, tasks, ok, rej = _run(clink, par, query)
        rejected = rejected or rej
        if tasks:
            tasks_seen = tasks
        if ok:
            walls.append(wall)
    return (statistics.median(walls) if walls else float("nan")), rejected, tasks_seen


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("clink", help="path to the clink binary (a Release build)")
    ap.add_argument("--pars", default="1,2,3,4,6,8,12,16,24,32", help="parallelisms, comma-separated")
    ap.add_argument("--large", type=int, default=2_000_000, help="rows (sized so processing dominates)")
    ap.add_argument("--keys", type=int, default=1024)
    ap.add_argument("--reps", type=int, default=2)
    args = ap.parse_args()

    cores = os.cpu_count() or 0
    pars = [int(x) for x in args.pars.split(",") if x.strip()]

    with tempfile.TemporaryDirectory() as d:
        large = os.path.join(d, "large.ndjson")
        _write_ndjson(large, args.large, args.keys)
        ql = _query(large)

        # Thread count at parallelism 1 (a keyed chain folds the shuffle at par 1,
        # so the per-par count is not simply this x par - report the real one).
        _w, tasks_at_1, _ok, _r = _run(args.clink, 1, ql)
        ops_per_chain = tasks_at_1 if tasks_at_1 else None

        rows = []
        for par in pars:
            wl, rejected, tasks = _median_wall(args.clink, par, ql, args.reps)
            thpt = None if rejected or wl != wl else round(args.large / wl)  # wl!=wl => NaN
            rows.append(
                {
                    "parallelism": par,
                    "threads": tasks,
                    "threads_per_core": round(tasks / cores, 2) if tasks and cores else None,
                    "wall_s": None if rejected or wl != wl else round(wl, 3),
                    "throughput_rows_per_s": thpt,
                    "rejected": rejected,
                }
            )

    valid = [r for r in rows if r["throughput_rows_per_s"]]
    peak = max(valid, key=lambda r: r["throughput_rows_per_s"]) if valid else None
    summary = {
        "cores": cores,
        "ops_per_chain": ops_per_chain,
        "large_rows": args.large,
        "peak": peak,
        "by_parallelism": rows,
    }
    print(json.dumps(summary, indent=2))

    print(f"\ncores={cores}  ops/chain={ops_per_chain}  (threads = ops/chain x parallelism)")
    print(f"{'par':>4} {'threads':>8} {'thr/core':>9} {'wall_s':>8} {'rows/s':>12}")
    for r in rows:
        t = r["throughput_rows_per_s"]
        cell = "rejected" if r["rejected"] else (f"{t:,}" if t else "-")
        print(
            f"{r['parallelism']:>4} {str(r['threads']):>8} "
            f"{str(r['threads_per_core']):>9} {str(r['wall_s']):>8} {cell:>12}"
        )
    if peak:
        print(
            f"\npeak throughput {peak['throughput_rows_per_s']:,} rows/s at parallelism "
            f"{peak['parallelism']} ({peak['threads']} threads, "
            f"{peak['threads_per_core']}x cores)"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
