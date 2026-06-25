#!/usr/bin/env python3
"""Summarize the engine-side sustained-throughput run.

Reads results-sampled/<query>-<engine>.json (written by throughput_sampled.sh),
prints a per-query table of the SUSTAINED rate (engine-counter max slope), the
whole-run rate, CPU consumed, and the clink/Flink ratios. Cross-checks that both
engines produced the same number of output rows (the correctness gate) and flags
any query where the FAST engine ran for too short a window to trust the slope.
"""
import argparse
import glob
import json
import os


def fmt(n):
    if n >= 1e6:
        return f"{n/1e6:.2f}M"
    if n >= 1e3:
        return f"{n/1e3:.0f}k"
    return f"{n:.0f}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--par", default="?")
    ap.add_argument("--events", default="?")
    args = ap.parse_args()

    by_q = {}
    for f in glob.glob(os.path.join(args.results_dir, "*.json")):
        d = json.load(open(f))
        by_q.setdefault(d["query"], {})[d["engine"]] = d

    print(f"\n  Sustained throughput - engine-side metrics, par={args.par}, {args.events} events")
    print(f"  drain = each engine's OWN records-processed counter; records / (time from first")
    print(f"  record to fully drained). Excludes deploy/JVM-warmup startup, averages over the")
    print(f"  whole drain (robust to coarse metric refresh). Not consumer-bound, not fooled by")
    print(f"  sink burst-flush. slope/whole-run shown as diagnostics only.\n")
    hdr = f"  {'query':6} {'engine':6} {'DRAIN rec/s':>12} {'drain(s)':>9} {'CPU-s':>7} {'ev/CPU-s':>10} {'out_rows':>10}"
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))

    issues = []
    geomean_terms = []
    for q in sorted(by_q):
        rows = by_q[q]
        c = rows.get("clink")
        fl = rows.get("flink")
        for eng, d in (("clink", c), ("flink", fl)):
            if not d:
                continue
            cpu = d.get("cpu_seconds", 0) or 0
            evcpu = (d["final_count"] / cpu) if cpu > 0 else 0
            ds = d.get("drain_seconds")
            print(f"  {q:6} {eng:6} {fmt(d['drain_rate']):>12} {(('%.1f' % ds) if ds else '-'):>9} "
                  f"{cpu:>7.1f} {fmt(evcpu):>10} {d.get('out_rows',0):>10}")
        if c and fl:
            # Only a meaningful correctness comparison if BOTH engines drained the
            # full input. A run that hit the timeout (reached_target false) has a
            # partial output - flag it as incomplete, not as a divergence.
            incomplete = [e for e, d in (("clink", c), ("flink", fl)) if not d.get("reached_target")]
            if incomplete:
                issues.append(f"{q}: INCOMPLETE run ({', '.join(incomplete)} did not drain the full input "
                              f"before the cap) - out_rows not comparable; raise --max-runtime / warm the engine")
            elif c.get("out_rows", -1) != fl.get("out_rows", -2):
                issues.append(f"{q}: OUTPUT ROW MISMATCH clink={c.get('out_rows')} flink={fl.get('out_rows')}")
            ratio_drain = (c["drain_rate"] / fl["drain_rate"]) if fl["drain_rate"] else 0
            ratio_cpu = ((c["final_count"] / c["cpu_seconds"]) / (fl["final_count"] / fl["cpu_seconds"])) \
                if c.get("cpu_seconds") and fl.get("cpu_seconds") else 0
            print(f"  {q:6} {'RATIO':6} {('%.2fx' % ratio_drain):>12} {'':>9} {'':>7} {('%.2fx' % ratio_cpu):>10}"
                  f"  (clink/Flink: drain-rate, CPU-efficiency)")
            if ratio_drain > 0:
                geomean_terms.append(ratio_drain)
            # short-window caveat for the fast engine
            ds_c = c.get("drain_seconds") or 0
            if c.get("reached_target") and ds_c < 1.5:
                issues.append(f"{q}: clink drain window <1.5s ({ds_c}s) - rate is coarse, "
                              f"raise EVENTS for a tighter number")
        print()

    if geomean_terms:
        gm = 1.0
        for r in geomean_terms:
            gm *= r
        gm = gm ** (1.0 / len(geomean_terms))
        print(f"  GEOMEAN drain-rate ratio (clink/Flink) over {len(geomean_terms)} queries: {gm:.2f}x\n")

    if issues:
        print("  NOTES / CAVEATS:")
        for i in issues:
            print(f"    - {i}")
        print()
    else:
        print("  Output-row counts match across engines (correctness gate held).\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
