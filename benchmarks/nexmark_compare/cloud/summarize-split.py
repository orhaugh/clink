#!/usr/bin/env python3
"""Print the split-rig comparison table, and the ratio that is the point of it.

Two figures per engine, because either alone misleads:
  sustained  records/s at the steepest 0.5s slope - throughput.
  cores      CPU-seconds per wall-second - how much of the node it took.
A throughput win bought with 4x the machine is not a win, so the table shows both
and the ratio line pairs them.
"""
import glob
import json
import os
import sys


def main():
    res = sys.argv[1]
    rows = []
    for p in sorted(glob.glob(os.path.join(res, "*.json"))):
        try:
            rows.append(json.load(open(p)))
        except Exception:
            continue
    if not rows:
        print("no results")
        return 0

    hdr = (f"{'query':<6} {'engine':<6} {'trial':>5} {'sustained':>11} {'drain':>10} "
           f"{'cores':>6} {'ev/cpu-s':>10} {'anon MB':>8} {'reached':>8}")
    print(hdr)
    print("-" * len(hdr))
    for r in sorted(rows, key=lambda x: (x.get("query", ""), x.get("engine", ""), x.get("trial", 0))):
        print(f"{r.get('query',''):<6} {r.get('engine',''):<6} {r.get('trial',0):>5} "
              f"{(r.get('sustained_slope') or 0):>11,.0f} {(r.get('drain_rate') or 0):>10,.0f} "
              f"{(r.get('cores') or 0):>6.2f} {(r.get('events_per_cpu_sec') or 0):>10,.0f} "
              f"{(r.get('anon_mb') or 0):>8.0f} {str(r.get('reached_target')):>8}")

    by = {}
    for r in rows:
        by.setdefault((r.get("query"), r.get("engine")), []).append(r)
    print()
    for q in sorted({r.get("query") for r in rows}):
        c, f = by.get((q, "clink")), by.get((q, "flink"))
        if not c or not f:
            continue
        # Best-of-trials on each side: the fairest read when the spread is the
        # thing being controlled for, and it cannot flatter clink selectively
        # because it is applied to both.
        cs = max((x.get("sustained_slope") or 0) for x in c)
        fs = max((x.get("sustained_slope") or 0) for x in f)
        cc = min((x.get("cores") or 99) for x in c)
        fc = min((x.get("cores") or 99) for x in f)
        ce = max((x.get("events_per_cpu_sec") or 0) for x in c)
        fe = max((x.get("events_per_cpu_sec") or 0) for x in f)
        cm = min((x.get("anon_mb") or 0) for x in c)
        fm = min((x.get("anon_mb") or 0) for x in f)
        print(f"{q}: throughput clink/flink = {cs/fs:.2f}x  ({cs:,.0f} vs {fs:,.0f} rec/s)")
        print(f"     cores {cc:.2f} vs {fc:.2f}   efficiency {ce/fe:.2f}x "
              f"({ce:,.0f} vs {fe:,.0f} events/cpu-s)" if fe else "")
        if cm and fm:
            print(f"     memory {cm:,.0f} MB vs {fm:,.0f} MB anon = {fm/cm:.1f}x lower")
    return 0


if __name__ == "__main__":
    sys.exit(main())
