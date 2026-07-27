#!/usr/bin/env python3
"""Find which operator a clink job is actually bottlenecked on.

WHY THIS EXISTS. A throughput number says a job is slow; it does not say which
stage is slow, and the two obvious guesses - "the source is too slow" and "a
downstream operator is too slow" - are distinguished by exactly one observable:
whether anything is QUEUED.

  * A downstream stage is the bottleneck  -> its INPUT queue fills and stays
    full, and backpressure propagates upstream.
  * The source cannot keep up            -> every queue in the pipeline sits
    near empty however fast the rest could run.

clink publishes input_depth and input_capacity per operator, so this samples them
throughout the drain and reports each operator's PEAK and MEAN occupancy. It must
sample during the drain: a snapshot taken after a job finishes reads zero
everywhere, which looks exactly like a starved pipeline and means nothing.

  python3 driver/bottleneck.py --base http://127.0.0.1:8095 --job 3
  python3 driver/bottleneck.py --base ... --job 3 --interval 0.05 --seconds 30
"""

import argparse
import json
import sys
import time
import urllib.request


def get_json(url, timeout=3.0):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read().decode())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True)
    ap.add_argument("--job", required=True)
    ap.add_argument("--interval", type=float, default=0.05)
    ap.add_argument("--seconds", type=float, default=60.0)
    ap.add_argument("--json", action="store_true", help="emit the raw summary as JSON")
    args = ap.parse_args()

    # {op_index: {"type":..., "cap":..., "depths":[...], "in":[...], "out":[...]}}
    seen = {}
    t0 = time.time()
    quiet = 0
    last_frontier = -1
    while time.time() - t0 < args.seconds:
        try:
            d = get_json(f"{args.base}/api/v1/jobs/{args.job}/operators")
        except Exception:
            time.sleep(args.interval)
            continue
        ops = d.get("operators", [])
        frontier = 0
        for i, o in enumerate(ops):
            e = seen.setdefault(
                i, {"type": o.get("op_type", "?"), "cap": o.get("input_capacity", 0),
                    "depths": [], "in": 0, "out": 0}
            )
            e["depths"].append(int(o.get("input_depth", 0) or 0))
            e["in"] = int(o.get("records_in", 0) or 0)
            e["out"] = int(o.get("records_out", 0) or 0)
            frontier = max(frontier, e["in"], e["out"])
        # Stop once the job stops making progress, so the samples cover the DRAIN
        # and not a long tail of zeros after it.
        if frontier == last_frontier:
            quiet += 1
            if quiet > int(2.0 / args.interval):
                break
        else:
            quiet = 0
            last_frontier = frontier
        time.sleep(args.interval)

    if not seen:
        print("no operator samples collected - wrong job id, or the job never ran")
        return 1

    rows = []
    for i in sorted(seen):
        e = seen[i]
        ds = [x for x in e["depths"] if x is not None]
        cap = e["cap"] or 0
        peak = max(ds) if ds else 0
        mean = (sum(ds) / len(ds)) if ds else 0.0
        rows.append({
            "op": e["type"], "capacity": cap, "peak_depth": peak,
            "mean_depth": round(mean, 1),
            "peak_pct": round(100.0 * peak / cap, 1) if cap else None,
            "records_in": e["in"], "records_out": e["out"],
        })

    if args.json:
        print(json.dumps({"samples": len(seen[0]["depths"]), "operators": rows}, indent=2))
        return 0

    print(f"  samples: {len(seen[0]['depths'])} over {time.time() - t0:.1f}s")
    print(f"  {'operator':32} {'cap':>6} {'peak':>7} {'mean':>7} {'peak%':>6} "
          f"{'rec_in':>11} {'rec_out':>11}")
    print("  " + "-" * 92)
    for r in rows:
        pct = f"{r['peak_pct']}%" if r["peak_pct"] is not None else "-"
        print(f"  {r['op'][:32]:32} {r['capacity']:>6} {r['peak_depth']:>7} "
              f"{r['mean_depth']:>7} {pct:>6} {r['records_in']:>11} {r['records_out']:>11}")
    # The verdict, stated rather than left to the reader.
    backed_up = [r for r in rows if r["peak_pct"] is not None and r["peak_pct"] >= 50.0]
    print()
    if backed_up:
        worst = max(backed_up, key=lambda r: r["peak_pct"])
        print(f"  VERDICT: '{worst['op']}' backed up to {worst['peak_pct']}% of its input queue.")
        print("           A stage whose input queue fills IS the bottleneck; everything")
        print("           upstream of it is being backpressured.")
    else:
        print("  VERDICT: no operator's input queue exceeded 50% at any sample, so no")
        print("           processing stage was the constraint. The pipeline was starved -")
        print("           the source could not supply it faster. Look at the source, not")
        print("           the operators.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
