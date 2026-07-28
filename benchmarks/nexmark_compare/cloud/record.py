#!/usr/bin/env python3
"""Fold one split-rig run's measurements into a single result JSON.

Exists as a file rather than an inline `python3 -c` string in split-run.sh: the
inline form interpolated shell variables straight into Python source, so a brace
in the Python (`d.update({...})`) sat inside a double-quoted shell string and
closed the enclosing shell FUNCTION instead - the script failed to parse before it
reached its first measurement. Values arrive as arguments here, and the Python is
just Python.

  record.py --out FILE --engine E --query Q --trial N --par P \
            --cpu-pre A --cpu-post B --wall-pre C --wall-post D \
            --input-events N  < sampler.json
"""
import argparse
import json
import sys


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--engine", required=True)
    ap.add_argument("--query", required=True)
    ap.add_argument("--trial", type=int, default=1)
    ap.add_argument("--par", type=int, default=4)
    ap.add_argument("--cpu-pre", type=float, required=True)
    ap.add_argument("--cpu-post", type=float, required=True)
    ap.add_argument("--wall-pre", type=float, required=True)
    ap.add_argument("--wall-post", type=float, required=True)
    ap.add_argument("--input-events", type=int, required=True)
    a = ap.parse_args()

    raw = sys.stdin.read().strip()
    try:
        d = json.loads(raw)
    except Exception:
        print(f"  {a.engine}  sampler produced no JSON: {raw[:200]}")
        return 1

    cpu = round(a.cpu_post - a.cpu_pre, 2)
    wall = round(a.wall_post - a.wall_pre, 2)
    d.update({
        "engine": a.engine, "query": a.query, "trial": a.trial, "par": a.par,
        "cpu_seconds": cpu, "wall_seconds": wall, "input_events": a.input_events,
        # Cores = CPU-seconds per wall-second: the honest "how much machine did
        # this take" figure, and the one that makes a throughput ratio meaningful
        # when the two engines occupy different amounts of the node.
        "cores": round(cpu / wall, 2) if wall else None,
    })
    # Efficiency divides events by the CPU that processed them, so the numerator
    # must be what this run ACTUALLY processed. It used to be input_events
    # unconditionally, which for a truncated run (sampler gave up mid-stall,
    # reached_target false) credits the engine with the whole input over the CPU
    # of a fraction of it. On the 2026-07-28 sweep that inflated Flink's q18
    # efficiency ~8.5x - it stalled at 1.08M of 9.2M events, and 9.2M was divided
    # by the CPU of the 1.08M-event window. A truncated run's efficiency is not
    # comparable to a drained run's either way (deploy and JVM warm-up dominate a
    # short window), so it is reported under a different key rather than the
    # headline one, and the headline is null.
    processed = int(d.get("processed") or 0)
    if d.get("reached_target") and cpu:
        d["events_per_cpu_sec"] = round(a.input_events / cpu)
    else:
        d["events_per_cpu_sec"] = None
        d["events_per_cpu_sec_partial"] = round(processed / cpu) if cpu and processed else None
        d["_efficiency_note"] = (
            f"run truncated at {processed:,} of {a.input_events:,} events; "
            "headline efficiency withheld - a partial window's CPU is dominated by "
            "deploy and warm-up and is not comparable to a drained run")
    with open(a.out, "w") as f:
        json.dump(d, f)

    sus = d.get("sustained_slope") or 0
    drain = d.get("drain_rate") or 0
    print(f"  {a.engine}  sustained {sus:,.0f} rec/s   drain {drain:,.0f} rec/s "
          f"({d.get('drain_seconds')}s)  reached={d.get('reached_target')}")
    print(f"  {a.engine}  cpu {cpu:.1f}s over {wall:.1f}s wall = "
          f"{d.get('cores')} cores, {d.get('events_per_cpu_sec'):,} events/cpu-s"
          if d.get("events_per_cpu_sec") else f"  {a.engine}  cpu {cpu:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
