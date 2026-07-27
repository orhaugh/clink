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
    hdr = (f"  {'query':6} {'engine':6} {'DRAIN rec/s':>12} {'drain(s)':>9} {'CPU-s':>7} "
           f"{'ev/CPU-s':>10} {'anon MB':>8} {'out_rows':>10}")
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))

    # The control row. If the broker serves at about the rate the engines drain
    # at, the benchmark is measuring Kafka and no ratio below means anything.
    bc = os.environ.get("BROKER_CEILING", "")
    if bc:
        try:
            b = json.loads(bc)
            if b.get("rate"):
                print(f"  {'--':6} {'BROKER':6} {fmt(b['rate']):>12} {b.get('seconds', 0):>9.1f} "
                      f"{'-':>7} {'-':>10} {'-':>8} {'control':>10}")
                print(f"  (broker serve-rate control: if an engine's drain rate approaches this, "
                      f"the run is input-bound and is not measuring the engine)\n")
        except Exception:
            pass

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
            orows = d.get("out_rows", 0)
            orows_s = "n/a(bh)" if orows is None or orows < 0 else str(orows)
            anon = d.get("anon_mb")
            anon_s = ("%.0f" % anon) if anon else "-"
            print(f"  {q:6} {eng:6} {fmt(d['drain_rate']):>12} {(('%.1f' % ds) if ds else '-'):>9} "
                  f"{cpu:>7.1f} {fmt(evcpu):>10} {anon_s:>8} {orows_s:>10}")
        if c and fl:
            # Only a meaningful comparison if BOTH engines drained the input. Use
            # the drained FRACTION, not reached_target: clink's counter is
            # cumulative across jobs so its baseline anchors a poll or two in,
            # leaving it a fraction of a percent short of the exact target even
            # when it fully drained. <95% is a genuine cutoff (e.g. a cold engine
            # killed at the cap); >=95% is the baseline-anchor slack.
            def drained_frac(d):
                t = d.get("target", 0) or 0
                return (d.get("processed", 0) / t) if t else 1.0
            incomplete = [e for e, d in (("clink", c), ("flink", fl)) if drained_frac(d) < 0.95]
            blackhole = c.get("sink") == "blackhole" or fl.get("sink") == "blackhole"
            if incomplete:
                issues.append(f"{q}: INCOMPLETE run ({', '.join(incomplete)} did not drain the full input "
                              f"before the cap) - NO RATIO PRINTED; raise --max-runtime / warm the engine")
            elif blackhole:
                # No output topic to count; completeness gate = both engines'
                # counters drained the full input (reached_target, asserted above).
                pass
            mismatch = False
            if not incomplete and not blackhole and c.get("out_rows", -1) != fl.get("out_rows", -2):
                mismatch = True
                issues.append(f"{q}: OUTPUT ROW MISMATCH clink={c.get('out_rows')} flink={fl.get('out_rows')}")
            # Hard gate: a ratio between a completed run and a truncated one is not
            # a comparison, and printing it anyway is how a caveat two screens down
            # gets quoted as a headline. Refuse rather than annotate.
            #
            # A row-count mismatch is the same kind of non-comparison and is
            # suppressed the same way: two engines that emitted different numbers
            # of rows from the same input did different amounts of work, so
            # whichever ran faster may simply have done less. That is the one
            # failure a throughput figure cannot survive, and it used to print the
            # ratio anyway with the mismatch noted further down the page.
            if incomplete or mismatch:
                why = "incomplete run" if incomplete else "output row mismatch"
                print(f"  {q:6} {'RATIO':6} {'--':>12} {'':>9} {'':>7} {'--':>10}"
                      f"  (suppressed: {why})")
                print()
                continue
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

    any_bh = any(d.get("sink") == "blackhole" for r in by_q.values() for d in r.values())
    if issues:
        print("  NOTES / CAVEATS:")
        for i in issues:
            print(f"    - {i}")
        print()
    elif any_bh:
        print("  Blackhole sink (output discarded): both engines drained the full input "
              "(reached_target); correctness is established separately by the Kafka-sink gate.\n")
    else:
        print("  Output-row counts match across engines (correctness gate held).\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
