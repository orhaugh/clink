#!/usr/bin/env python3
"""Per-recovery growth of the SURVIVING engine processes, for an A/B.

The leak under test only shows on a worker that outlives the restarts: a
killed worker starts clean. So this looks at each engine process, splits
its samples at every fault, and reports how much RSS, threads and fds it
gained per recovery cycle it survived - the number a fix must drive to
zero. Reported per process and as a median across cycles, because one
cycle's reading is one cycle's timing.

    ab_recovery.py --metrics DIR --events chaos.jsonl
"""
import argparse
import glob
import json
import statistics
from datetime import datetime, timezone


def load(metrics_dir):
    out = []
    for p in glob.glob(f"{metrics_dir}/*.jsonl"):
        for line in open(p):
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    out.sort(key=lambda s: s.get("ts", 0))
    return out


def fault_times(path):
    ts = []
    for line in open(path):
        try:
            e = json.loads(line)
        except json.JSONDecodeError:
            continue
        if e.get("fault", "").endswith(("_cleared", "_healed")):
            continue
        t = e.get("time")
        if t:
            ts.append(datetime.fromisoformat(t.replace("Z", "+00:00"))
                      .replace(tzinfo=timezone.utc).timestamp())
    return sorted(ts)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--metrics", required=True)
    ap.add_argument("--events", required=True)
    ap.add_argument("--settle-s", type=float, default=30.0,
                    help="ignore samples this soon after a fault (recovery churn)")
    a = ap.parse_args()

    samples = load(a.metrics)
    faults = fault_times(a.events)
    if not faults:
        print("no faults in the event log; nothing to attribute growth to")
        return 2

    # per process: [(ts, incarnation, rss_mb, threads, fds)]
    procs = {}
    for s in samples:
        for p in s.get("processes") or []:
            name = p.get("container") or ""
            if "clink" not in name:
                continue
            key = f"{s['host'].split('-')[-1]}/{name}"
            procs.setdefault(key, []).append(
                (s["ts"], p.get("incarnation"), (p.get("rss_kb") or 0) / 1024.0,
                 p.get("threads"), p.get("fds")))

    print(f"{len(faults)} fault(s); per-recovery growth of SURVIVING incarnations:\n")
    for key, rows in sorted(procs.items()):
        rows.sort()
        # windows between consecutive faults, settled
        deltas = []
        for f0, f1 in zip(faults, faults[1:]):
            win = [r for r in rows if f0 + a.settle_s <= r[0] < f1]
            if len(win) < 4:
                continue
            if len({r[1] for r in win}) != 1:
                continue  # this process was itself replaced in the window
            first, last = win[0], win[-1]
            deltas.append((last[2] - first[2], (last[3] or 0) - (first[3] or 0),
                           (last[4] or 0) - (first[4] or 0)))
        if not deltas:
            print(f"  {key}: never survived a full cycle (always the one killed)")
            continue
        rss = [d[0] for d in deltas]
        thr = [d[1] for d in deltas]
        fds = [d[2] for d in deltas]
        incs = len({r[1] for r in rows})
        print(f"  {key}: {len(deltas)} survived cycle(s), {incs} incarnation(s)")
        print(f"     RSS/cycle  median {statistics.median(rss):+7.1f} MiB   total {sum(rss):+8.1f} MiB")
        print(f"     thr/cycle  median {statistics.median(thr):+7.1f}       total {sum(thr):+8.0f}")
        print(f"     fds/cycle  median {statistics.median(fds):+7.1f}       total {sum(fds):+8.0f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
