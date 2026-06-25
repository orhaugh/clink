#!/usr/bin/env python3
"""Sustained-throughput sampler: poll an ENGINE's own records-processed counter
over time and report the sustained max processing rate.

This avoids the two confounds of downstream measurement: it is NOT bounded by a
slow consumer (we read the engine's counter, not the output topic) and NOT fooled
by sink burst-flush (we track input-side processing, not when records hit the
broker). The sustained rate is the steepest slope over a sliding window, which
excludes the warmup ramp and the end-of-stream taper.

  sample_rate.py clink --base http://127.0.0.1:8095 --job 1 --target 460000
  sample_rate.py flink --base http://127.0.0.1:8081 --job <hex> --target 460000

Both poll a "processing frontier" = max over operators/vertices of records read
(clink: max records_in; Flink: max read-records|write-records). Polls until the
frontier reaches --target (drained) or a quiet timeout, then prints JSON with the
sustained rate (max slope over --window seconds) and a whole-run average.
"""
import argparse
import json
import time
import urllib.request


def get_json(url, timeout=3.0):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read().decode())


def clink_frontier(base, job):
    # Returns (processed_frontier, clock_seconds). clink's per-op counters update
    # at fine granularity, so host wall-time is a fine clock (clock=None -> caller
    # uses wall). Frontier = max records_in across operators = input processed by
    # the busiest stage.
    d = get_json(f"{base}/api/v1/jobs/{job}/operators")
    best = 0
    for op in d.get("operators", []):
        best = max(best, int(op.get("records_in", 0)), int(op.get("records_out", 0)))
    return best, None


def flink_frontier(base, job):
    # Returns (frontier, job_duration_seconds). Flink's aggregated job metrics lag
    # the metric-fetcher interval (~10s) before they populate, so host wall-time is
    # a bad clock for the drain. Instead we use Flink's OWN job clock ("duration",
    # ms since RUNNING) which advances in real time regardless of metric lag - so
    # target/duration_at_completion is a fetch-lag-immune throughput.
    d = get_json(f"{base}/jobs/{job}")
    best = 0
    for v in d.get("vertices", []):
        m = v.get("metrics", {})
        best = max(best, int(m.get("read-records", 0) or 0), int(m.get("write-records", 0) or 0))
    clock = float(d.get("duration", 0) or 0) / 1000.0
    return best, clock


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("engine", choices=["clink", "flink"])
    ap.add_argument("--base", required=True)
    ap.add_argument("--job", required=True)
    ap.add_argument("--target", type=int, required=True, help="input events; stop when frontier reaches it")
    ap.add_argument("--interval", type=float, default=0.1, help="poll interval (s)")
    ap.add_argument("--window", type=float, default=0.5, help="sliding window for the sustained-rate slope (s)")
    ap.add_argument("--quiet-timeout", type=float, default=6.0,
                    help="after records START flowing, give up this long after the frontier stalls")
    ap.add_argument("--start-timeout", type=float, default=45.0,
                    help="give up if NO record flows within this long (job never deployed)")
    ap.add_argument("--max-runtime", type=float, default=180.0, help="absolute cap")
    args = ap.parse_args()

    frontier = clink_frontier if args.engine == "clink" else flink_frontier
    samples = []  # (t, processed) where processed = frontier - baseline
    t0 = time.time()
    baseline = None  # frontier at first poll; counters can be cumulative across jobs
    last_progress = None  # wall time the frontier first advanced past baseline
    t_first = None  # wall (rel) of first record processed this run
    t_drained = None  # wall (rel) the run reached --target
    t_last_inc = None  # wall (rel) of the LAST time proc grew (end of the drain)
    last_proc = 0
    # Poll until drained (processed >= target) or stalled. Stall detection only
    # arms AFTER records begin flowing, so the deploy/JVM-warmup startup gap does
    # not trip the quiet-timeout. Baseline-subtraction makes this robust to
    # engines whose per-operator counters accumulate across job submissions.
    # clink's per-op counters are cumulative across job submissions on a persistent
    # TM, so we anchor the baseline at the first strictly-positive reading (= the
    # op's prior total) and measure only the delta. Flink job_ids are unique per
    # submission with fresh vertices starting at 0, so its counts are absolute (no
    # baseline subtraction - subtracting would lose the metric-lagged ramp).
    cumulative = (args.engine == "clink")
    while True:
        now = time.time()
        try:
            c, clock = frontier(args.base, args.job)
        except Exception:
            c = (baseline or 0) + last_proc
            clock = None
        rel = clock if clock is not None else (now - t0)
        if cumulative:
            if baseline is None:
                if c <= 0:
                    samples.append((rel, 0))
                    if now - t0 > args.start_timeout or now - t0 > args.max_runtime:
                        break
                    time.sleep(args.interval)
                    continue
                baseline = c
            proc = max(0, c - baseline)
        else:
            baseline = 0
            proc = max(0, c)
        samples.append((rel, proc))
        if proc > 0 and t_first is None:
            t_first = rel
        if proc >= args.target:
            t_drained = rel
            t_last_inc = rel
            break
        if proc > last_proc:
            last_proc = proc
            last_progress = now
            t_last_inc = rel
        if last_progress is None:
            if rel > args.start_timeout:
                break  # job never produced a record
        elif now - last_progress > args.quiet_timeout:
            break  # drained, frontier plateaued
        if rel > args.max_runtime:
            break
        time.sleep(args.interval)

    n = len(samples)
    final_c = samples[-1][1] if samples else 0
    elapsed = samples[-1][0] if samples else 0.0
    # PRIMARY: drain-phase rate = records processed / (first record -> last record).
    # Measuring to the LAST increase (not an exact target hit) excludes both the
    # deploy/JVM startup gap AND the post-drain plateau, and is immune to the
    # baseline being anchored a few thousand records in (cumulative counters).
    # Averaging over the whole drain is robust to coarse metric refresh (Flink ~2s).
    proc_total = last_proc if t_drained is None else args.target
    # clink's counter is fine-grained: measure first-record -> last-record, which
    # excludes the deploy gap. Flink's metric is a step (reports late, all at once)
    # so first->last is meaningless; instead use Flink's own job clock from RUNNING
    # (clock 0) to completion - the job drained throughout, the metric just reported
    # late. Both exclude fixed startup (clink deploy / Flink JVM+schedule pre-RUNNING).
    if args.engine == "clink":
        span = (t_last_inc - t_first) if (t_last_inc is not None and t_first is not None) else None
    else:
        span = t_last_inc  # job-clock seconds since RUNNING
    drain_rate = (proc_total / span) if (span and span > 0) else 0.0
    # SECONDARY diagnostics: max slope over a window, and whole-run incl. startup.
    sustained = 0.0
    for i in range(n):
        for j in range(i + 1, n):
            dt = samples[j][0] - samples[i][0]
            if dt >= args.window:
                dc = samples[j][1] - samples[i][1]
                if dt > 0:
                    sustained = max(sustained, dc / dt)
                break
    avg = (final_c / elapsed) if elapsed > 0 else 0.0
    drain_s = span
    print(json.dumps({
        "engine": args.engine,
        "final_count": final_c,
        "processed": proc_total,
        "target": args.target,
        "reached_target": t_drained is not None,
        "baseline": baseline or 0,
        "elapsed_s": round(elapsed, 3),
        "t_first_s": round(t_first, 3) if t_first is not None else None,
        "drain_seconds": round(drain_s, 3) if drain_s is not None else None,
        "drain_rate": round(drain_rate, 1),
        "sustained_slope": round(sustained, 1),
        "whole_run_rate": round(avg, 1),
        "samples": n,
    }))


if __name__ == "__main__":
    main()
