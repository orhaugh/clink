#!/usr/bin/env python3
"""Attribute a clink worker's CPU to THREAD NAMES over the life of a run.

The split rig showed clink using ~1.6 of 4 cores while Flink used ~2.9, so the
question is not "how fast is a record processed" (clink already wins ~1.9x per
CPU-second) but "why is most of the machine idle". That is a question about which
threads run and which sit blocked, which per-process CPU cannot answer.

Reads /proc/<pid>/task/*/stat, whose utime+stime are CUMULATIVE per thread, so the
delta between two samples is exactly that thread's CPU over the interval. Polling
is cheap (one read per thread) and runs outside the measured cgroup only if invoked
from the host, which is how the rig calls it.

  thread_cpu.py --pid N --seconds 40 [--interval 0.2]

Prints one line per thread NAME (threads are pooled by name because clink names
them after the operator they run): total CPU seconds, peak concurrent busy count,
and how many distinct threads carried that name.
"""
import argparse
import collections
import glob
import os
import time

CLK_TCK = os.sysconf("SC_CLK_TCK")


def snapshot(pid):
    """{tid: (name, cpu_ticks)} for every thread of pid, tolerating thread exit."""
    out = {}
    for p in glob.glob(f"/proc/{pid}/task/*/stat"):
        try:
            with open(p) as f:
                raw = f.read()
        except OSError:
            continue
        # comm is field 2 and may contain spaces and parentheses, so split on the
        # LAST ')' rather than on whitespace.
        try:
            lp, rp = raw.index("("), raw.rindex(")")
        except ValueError:
            continue
        name = raw[lp + 1:rp]
        rest = raw[rp + 2:].split()
        try:
            utime, stime = int(rest[11]), int(rest[12])
        except (IndexError, ValueError):
            continue
        tid = os.path.basename(os.path.dirname(p))
        out[tid] = (name, utime + stime)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pid", type=int, required=True)
    ap.add_argument("--seconds", type=float, default=40.0)
    ap.add_argument("--interval", type=float, default=0.2)
    a = ap.parse_args()

    total = collections.Counter()      # name -> CPU seconds
    threads = collections.defaultdict(set)   # name -> tids seen
    peak_busy = 0
    busy_series = []

    prev = snapshot(a.pid)
    t_end = time.time() + a.seconds
    while time.time() < t_end:
        time.sleep(a.interval)
        cur = snapshot(a.pid)
        busy = 0
        for tid, (name, ticks) in cur.items():
            threads[name].add(tid)
            if tid in prev:
                d = ticks - prev[tid][1]
                if d > 0:
                    total[name] += d / CLK_TCK
                    # "Busy" = used at least half the interval, i.e. genuinely
                    # running rather than waking briefly to hand off a batch.
                    if d / CLK_TCK >= a.interval * 0.5:
                        busy += 1
        busy_series.append(busy)
        peak_busy = max(peak_busy, busy)
        prev = cur

    grand = sum(total.values())
    print(f"total CPU {grand:.1f}s over {a.seconds:.0f}s wall = {grand/a.seconds:.2f} cores")
    print(f"threads {sum(len(v) for v in threads.values())}, peak concurrently busy {peak_busy}, "
          f"mean busy {sum(busy_series)/max(1,len(busy_series)):.1f}")
    print()
    print(f"{'thread name':<20} {'cpu s':>8} {'% total':>8} {'cores':>7} {'#thr':>5}")
    print("-" * 52)
    for name, cpu in total.most_common(24):
        if cpu < 0.05:
            continue
        print(f"{name:<20} {cpu:>8.2f} {100*cpu/grand if grand else 0:>7.1f}% "
              f"{cpu/a.seconds:>7.2f} {len(threads[name]):>5}")


if __name__ == "__main__":
    raise SystemExit(main())
