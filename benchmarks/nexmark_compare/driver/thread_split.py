#!/usr/bin/env python3
"""Where does an engine's CPU actually go, thread by thread?

WHY. Dividing throughput by events-per-CPU-second gives cores CONSUMED, and on
nexmark q0 that came out at 2.1 for clink and 7.0 for Flink. It is tempting to
read the larger number as "uses the machine better", but it counts CPU consumed,
not CPU consumed USEFULLY, and those diverge for two reasons at once:

  * work that is not the query - a JVM's GC and JIT-compiler threads, Netty event
    loops, checkpoint RPC. Burned, not converted into throughput.
  * genuine pipelining - a Kafka fetcher on its own thread overlaps broker wait
    with processing, which a fused single-threaded chain cannot.

Those lead to opposite conclusions, so the split has to be measured. Threads
self-name, so this samples /proc/<pid>/task/*/stat inside the container twice and
attributes the delta by thread name.

  python3 driver/thread_split.py --container nxcompare-flink-taskmanager-1
  python3 driver/thread_split.py --container nxcompare-clink-worker1-1 --seconds 10

Uses only `cat` and `ls` inside the container, so it works against images with no
Python or ps.
"""

import argparse
import collections
import re
import subprocess
import sys
import time

# Thread-name prefixes grouped into what they actually do. A JVM names its own
# internals, which is what makes the attribution possible at all.
GROUPS = [
    ("GC / memory management", ("GC Thread", "G1 ", "G1Young", "G1Conc", "VM Thread",
                                "Reference Handl", "Finalizer", "VM Periodic")),
    ("JIT compilation", ("C1 Compiler", "C2 Compiler", "C1 CompilerThre", "C2 CompilerThre",
                         "Sweeper thread")),
    ("Kafka fetch", ("Kafka Fetcher", "kafka-", "rdk:", "Source Data Fet", "kafka_text_sour",
                    "kafka_source")),
    ("network / RPC", ("Netty", "flink-netty", "flink-akka", "flink-rest", "pekko",
                       "clink-net", "network_bridge")),
    ("task / operator work", ("Legacy Source", "Window", "Source:", "Map ", "Sink:", "Process",
                              "clink-task", "task-")),
    ("metrics / housekeeping", ("flink-metrics", "MetricFetcher", "clink-metrics",
                                "Timer", "pool-")),
]


def group_of(name):
    for label, prefixes in GROUPS:
        for p in prefixes:
            if name.startswith(p):
                return label
    return "other / unclassified"


def dexec(container, script):
    r = subprocess.run(["docker", "exec", container, "sh", "-c", script],
                       capture_output=True, text=True)
    return r.stdout


def find_pid(container):
    """The engine process: the /proc entry with the most threads."""
    out = dexec(container, 'for d in /proc/[0-9]*; do n=$(ls $d/task 2>/dev/null | wc -l); '
                           'echo "$n ${d#/proc/}"; done')
    best, best_pid = 0, None
    for line in out.splitlines():
        parts = line.split()
        if len(parts) != 2:
            continue
        try:
            n = int(parts[0])
        except ValueError:
            continue
        if n > best:
            best, best_pid = n, parts[1]
    return best_pid, best


def sample(container, pid):
    """{tid: (name, ticks)} for every thread."""
    out = dexec(container,
                f'for d in /proc/{pid}/task/*; do printf "%s|" "${{d##*/}}"; '
                f'cat $d/stat 2>/dev/null; done')
    res = {}
    for line in out.splitlines():
        if "|" not in line:
            continue
        tid, raw = line.split("|", 1)
        try:
            lp, rp = raw.index("("), raw.rindex(")")
        except ValueError:
            continue
        name = raw[lp + 1:rp]
        rest = raw[rp + 2:].split()
        try:
            ticks = int(rest[11]) + int(rest[12])  # utime + stime
        except (IndexError, ValueError):
            continue
        res[tid] = (name, ticks)
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--container", required=True)
    ap.add_argument("--seconds", type=float, default=8.0,
                    help="length of EACH window")
    ap.add_argument("--windows", type=int, default=1,
                    help="take this many consecutive windows and report the BUSIEST. A "
                         "single window has to be aimed at the drain by hand, and a "
                         "window that opens after the job finished reads near-zero - "
                         "which looks like an idle engine and means nothing.")
    ap.add_argument("--hz", type=float, default=100.0, help="kernel USER_HZ")
    ap.add_argument("--top", type=int, default=14)
    args = ap.parse_args()

    pid, nthreads = find_pid(args.container)
    if pid is None:
        print(f"could not find a process in {args.container}")
        return 1
    print(f"  {args.container}: pid {pid}, {nthreads} threads")

    # Take `windows` consecutive windows and keep the busiest. The drain is short
    # and its start is not known precisely, so a single hand-aimed window can miss
    # it entirely - which is exactly what happened on the first run of this tool.
    best = None
    for _ in range(max(1, args.windows)):
        first = sample(args.container, pid)
        if not first:
            print("  no thread stats readable")
            return 1
        t0 = time.time()
        time.sleep(args.seconds)
        second = sample(args.container, pid)
        elapsed = time.time() - t0
        total = 0.0
        for tid, (_, ticks) in second.items():
            total += max(0, ticks - first.get(tid, (None, 0))[1])
        rate = (total / args.hz) / elapsed if elapsed > 0 else 0.0
        if best is None or rate > best[0]:
            best = (rate, first, second, elapsed)
    _, first, second, elapsed = best

    per_thread = []
    per_group = collections.Counter()
    for tid, (name, ticks) in second.items():
        base = first.get(tid, (name, 0))[1]
        delta = max(0, ticks - base)
        cores = (delta / args.hz) / elapsed
        if cores <= 0.0:
            continue
        per_thread.append((cores, name, tid))
        per_group[group_of(name)] += cores
    per_thread.sort(reverse=True)

    total = sum(c for c, _, _ in per_thread)
    print(f"  sampled {elapsed:.1f}s; TOTAL {total:.2f} cores\n")
    print(f"  {'cores':>7}  {'%':>6}  thread")
    print("  " + "-" * 48)
    for cores, name, _ in per_thread[:args.top]:
        print(f"  {cores:7.3f}  {100.0 * cores / total if total else 0:5.1f}%  {name}")
    if len(per_thread) > args.top:
        rest = sum(c for c, _, _ in per_thread[args.top:])
        print(f"  {rest:7.3f}  {100.0 * rest / total if total else 0:5.1f}%  "
              f"({len(per_thread) - args.top} more threads)")
    print()
    print(f"  {'cores':>7}  {'%':>6}  category")
    print("  " + "-" * 48)
    for label, cores in per_group.most_common():
        print(f"  {cores:7.3f}  {100.0 * cores / total if total else 0:5.1f}%  {label}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
