#!/usr/bin/env python3
"""The flight recorder's cost on a bounded job, measured on this machine.

Runs the example's filter and window query over a file of --rows events,
embedded (clink run job.sql), --runs times with the flight recorder off and
--runs times with it on, alternating, and prints the median wall time, the
median CPU time of the clink process and the bytes the captures took. The
feed in run.sh is paced, so its wall time says nothing about the recorder;
this job reads as fast as the engine can.

Usage: measure.py --bin=<dir with clink> --work=<dir> [--rows=N] [--runs=N]
"""

import argparse
import json
import os
import resource
import shutil
import statistics
import subprocess
import time


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--work", required=True)
    ap.add_argument("--rows", type=int, default=200000)
    ap.add_argument("--runs", type=int, default=3)
    args = ap.parse_args()
    os.makedirs(args.work, exist_ok=True)
    data = os.path.join(args.work, "in.jsonl")
    with open(data, "w") as f:
        for i in range(args.rows):
            f.write(json.dumps({"usr": "u%d" % (i % 7), "ts": i * 37, "amount": (i * 31) % 97}) + "\n")
    results = {"off": [], "on": []}
    capture_bytes = 0
    for run in range(args.runs):
        for mode in ("off", "on"):
            d = os.path.join(args.work, "%s-%d" % (mode, run))
            shutil.rmtree(d, ignore_errors=True)
            os.makedirs(d)
            sql = os.path.join(d, "job.sql")
            with open(sql, "w") as f:
                f.write(
                    "CREATE TABLE evt (usr TEXT, ts BIGINT, amount BIGINT) WITH (connector='file', "
                    "format='json', path='%s', event_time_column='ts');\n"
                    "CREATE TABLE totals (usr TEXT, window_start BIGINT, total BIGINT, n BIGINT) "
                    "WITH (connector='file', format='json', path='%s');\n"
                    "INSERT INTO totals SELECT usr, window_start, SUM(amount) AS total, COUNT(*) AS n "
                    "FROM evt WHERE amount > 3 GROUP BY TUMBLE(ts, INTERVAL '1' SECOND), usr;\n"
                    % (data, os.path.join(d, "out.jsonl")))
            cmd = [os.path.join(args.bin, "clink"), "run", sql,
                   "--checkpoint-dir=" + os.path.join(d, "ckpt"), "--checkpoint-interval-ms=200"]
            if mode == "on":
                cmd += ["--capture-dir=" + os.path.join(d, "capture"),
                        "--capture-records=" + str(args.rows)]
            before = resource.getrusage(resource.RUSAGE_CHILDREN)
            t0 = time.monotonic()
            subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                           env=dict(os.environ, CLINK_LOG_LEVEL="off"))
            wall = time.monotonic() - t0
            after = resource.getrusage(resource.RUSAGE_CHILDREN)
            cpu = (after.ru_utime - before.ru_utime) + (after.ru_stime - before.ru_stime)
            results[mode].append((wall, cpu))
            if mode == "on":
                capture_bytes = sum(os.path.getsize(os.path.join(p, n))
                                    for p, _, names in os.walk(os.path.join(d, "capture"))
                                    for n in names)
    off_wall = statistics.median(w for w, _ in results["off"])
    on_wall = statistics.median(w for w, _ in results["on"])
    off_cpu = statistics.median(c for _, c in results["off"])
    on_cpu = statistics.median(c for _, c in results["on"])
    summary = {
        "rows": args.rows, "runs": args.runs,
        "wall_seconds_off": round(off_wall, 3), "wall_seconds_on": round(on_wall, 3),
        "cpu_seconds_off": round(off_cpu, 3), "cpu_seconds_on": round(on_cpu, 3),
        "capture_bytes": capture_bytes,
    }
    with open(os.path.join(args.work, "overhead.json"), "w") as f:
        json.dump(summary, f, indent=2)
    print("    %d rows, median of %d runs: wall %.2f s off, %.2f s on (%+.0f%%); "
          "CPU %.2f s off, %.2f s on (%+.0f%%); captures %.1f MiB"
          % (args.rows, args.runs, off_wall, on_wall, 100 * (on_wall / off_wall - 1),
             off_cpu, on_cpu, 100 * (on_cpu / off_cpu - 1), capture_bytes / 1048576))


if __name__ == "__main__":
    main()
