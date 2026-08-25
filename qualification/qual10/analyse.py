#!/usr/bin/env python3
"""Turn QUAL-10's raw samples into a verdict and the charts behind it.

Reads the per-host JSONL the sampler appended, derives rates from the
cumulative counters, judges the leak bands, and writes SVGs. The charts are
not decoration: a leak verdict is a claim about shape, and a reader who can
see twelve flat hours with the kills marked on them does not have to take
the arithmetic on trust.

Counters are converted to rates HERE rather than in the sampler, because a
sampler that holds state across ticks reports a wrong first rate whenever it
restarts - an artefact that looks exactly like a step change.
"""
import argparse
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import chart  # noqa: E402
import trend  # noqa: E402

# Bands, written before the run. Percentages so one band covers a 3 GB
# worker and a 200 MB coordinator.
DEFAULTS = {
    "warmup_hours": 1.0,
    "rss_pct_per_hour": 0.5,
    "growth_ratio": 1.15,
    "min_incarnation_hours": 1.0,
    "max_gap_minutes": 5.0,
    "fd_thread_tolerance": 0.20,
}


def load(paths):
    out = []
    for p in paths:
        with open(p) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    continue  # a torn tail line is not a reason to lose a run
    out.sort(key=lambda s: s.get("ts", 0))
    return out


def rate(series):
    """Cumulative counter -> per-second rate. A DECREASE means the counter
    reset (a reboot, a replaced interface, a restarted process), which is a
    gap in the rate series rather than a huge negative spike."""
    out = []
    for (t0, v0), (t1, v1) in zip(series, series[1:]):
        if v0 is None or v1 is None or t1 <= t0 or v1 < v0:
            out.append((t1, None))
        else:
            out.append((t1, (v1 - v0) / (t1 - t0)))
    return out


def collect(samples, t0):
    """Regroup flat samples into the series the judge and the charts want."""
    hosts, procs = {}, {}
    for s in samples:
        if "host_metrics" not in s:
            continue
        host = s.get("host", "?")
        h = (s["ts"] - t0) / 3600.0
        hm = s["host_metrics"]
        H = hosts.setdefault(host, {
            "cpu_busy": [], "cpu_iowait": [], "mem_avail": [], "mem_used": [],
            "disk_read": [], "disk_write": [], "net_rx": [], "net_tx": [],
            "psi_cpu": [], "psi_io": [], "psi_mem": [], "fs_free": [], "hours": [],
        })
        H["hours"].append(h)
        cpu = (hm.get("cpu") or {}).get("cpu")
        if cpu:
            busy = sum(v or 0 for k, v in cpu.items()
                       if k not in ("idle", "iowait", "guest", "guest_nice"))
            H["cpu_busy"].append((s["ts"], busy))
            H["cpu_iowait"].append((s["ts"], cpu.get("iowait")))
        mem = hm.get("mem") or {}
        if mem.get("MemTotal"):
            H["mem_avail"].append((h, (mem.get("MemAvailable") or 0) / 1024.0))
            used = mem["MemTotal"] - (mem.get("MemAvailable") or 0)
            H["mem_used"].append((h, used / 1024.0))
        disk = hm.get("disk") or {}
        if disk:
            H["disk_read"].append((s["ts"], sum((d.get("read_sectors") or 0) for d in disk.values()) * 512))
            H["disk_write"].append((s["ts"], sum((d.get("write_sectors") or 0) for d in disk.values()) * 512))
        net = hm.get("net") or {}
        if net:
            H["net_rx"].append((s["ts"], sum((d.get("rx_bytes") or 0) for d in net.values())))
            H["net_tx"].append((s["ts"], sum((d.get("tx_bytes") or 0) for d in net.values())))
        psi = hm.get("pressure") or {}
        for key, dst in (("cpu_some_avg60", "psi_cpu"), ("io_some_avg60", "psi_io"),
                         ("memory_some_avg60", "psi_mem")):
            if key in psi:
                H[dst].append((h, psi[key]))
        fs = hm.get("fs") or {}
        for path, v in fs.items():
            if v and v.get("total_bytes"):
                H["fs_free"].append((h, v["free_bytes"] / 1e9))
                break

        for p in s.get("processes") or []:
            name = p.get("container") or f"pid-{p.get('pid')}"
            key = f"{host}/{name}"
            P = procs.setdefault(key, {"rss": [], "threads": [], "fds": [],
                                       "inc": [], "hours": [], "cgroup": []})
            inc = p.get("incarnation")
            P["hours"].append(h)
            if p.get("rss_kb") is not None:
                P["rss"].append((h, p["rss_kb"] / 1024.0))
                P["inc"].append((inc, h, p["rss_kb"] / 1024.0))
            if p.get("threads") is not None:
                P["threads"].append((h, p["threads"]))
            if p.get("fds") is not None:
                P["fds"].append((h, p["fds"]))
            cg = p.get("cgroup_memory") or {}
            if cg.get("current") is not None:
                P["cgroup"].append((h, cg["current"] / 1048576.0))
    return hosts, procs


def to_hours(series, t0):
    return [((t - t0) / 3600.0, v) for t, v in series]


def judge(procs, cfg, events_hours):
    findings, per_process = [], {}
    for key, P in sorted(procs.items()):
        if not P["inc"]:
            continue
        after = [(i, h, v) for i, h, v in P["inc"] if h >= cfg["warmup_hours"]]
        incs = trend.split_incarnations(after)
        judged, within = trend.judge_within(
            incs, cfg["rss_pct_per_hour"], cfg["min_incarnation_hours"])
        across = trend.judge_across(incs, cfg["growth_ratio"])
        gaps = trend.gap_report(sorted(h for h in P["hours"]), cfg["max_gap_minutes"])
        entry = {"incarnations_judged": judged, "within": within,
                 "across": across, "gaps": gaps,
                 "overall_slope_pct_per_hour": trend.slope_pct_per_hour(
                     [(h, v) for h, v in P["rss"] if h >= cfg["warmup_hours"]])}
        for metric in ("threads", "fds"):
            pts = [v for h, v in P[metric] if h >= cfg["warmup_hours"]]
            med = trend.median(pts)
            if med:
                hi = max(pts)
                entry[metric] = {"median": med, "max": hi,
                                 "over_tolerance": hi > med * (1 + cfg["fd_thread_tolerance"])}
        per_process[key] = entry
        if within:
            findings.append(f"{key}: RSS climbs within an incarnation "
                            f"({within[0]['slope_pct_per_hour']}%/h, band "
                            f"{cfg['rss_pct_per_hour']}%/h)")
        if across["verdict"] == "GROWTH":
            findings.append(f"{key}: RSS plateau marches across restarts "
                            f"(ratio {across.get('growth_ratio')}, "
                            f"{across['rising_pairs']}/{across['pairs']} pairs rising)")
        for metric in ("threads", "fds"):
            if entry.get(metric, {}).get("over_tolerance"):
                findings.append(f"{key}: {metric} peaked at {entry[metric]['max']} "
                                f"against a median of {entry[metric]['median']}")
        if judged == 0:
            # The two tests answer different questions, and this one did not
            # run. Frequent faults mean no incarnation lives long enough to
            # measure drift WITHIN it, so a steady-state leak - per record,
            # per checkpoint - would pass unseen while the across-restart
            # test reports a cheerful OK. Unexercised is not a pass: the
            # schedule owes this campaign a quiet window long enough to
            # judge, and if it did not provide one the run says so.
            findings.append(
                f"{key}: no incarnation ran {cfg['min_incarnation_hours']}h or "
                f"longer, so steady-state drift was never judged - the schedule "
                f"needs a quiet window, and this run cannot speak to a "
                f"per-record leak")
        if not gaps["ok"]:
            findings.append(f"{key}: sampling gap of {gaps['max_gap_minutes']}min "
                            f"exceeds {cfg['max_gap_minutes']}min - INCONCLUSIVE")
    return findings, per_process


def render(hosts, procs, t0, outdir, events):
    os.makedirs(outdir, exist_ok=True)
    written = []

    def write(name, svg):
        p = os.path.join(outdir, name)
        with open(p, "w") as fh:
            fh.write(svg)
        written.append(name)

    sub = "dashed lines are injected faults" if events else ""
    write("rss.svg", chart.line_chart(
        [{"label": k.split("/")[-1], "points": P["rss"]} for k, P in sorted(procs.items()) if P["rss"]],
        "Engine process memory (RSS)", "RSS (MiB)", events, subtitle=sub))
    write("threads.svg", chart.line_chart(
        [{"label": k.split("/")[-1], "points": P["threads"]} for k, P in sorted(procs.items()) if P["threads"]],
        "Threads per engine process", "threads", events, y_zero=True, subtitle=sub))
    write("fds.svg", chart.line_chart(
        [{"label": k.split("/")[-1], "points": P["fds"]} for k, P in sorted(procs.items()) if P["fds"]],
        "Open file descriptors per engine process", "fds", events, y_zero=True, subtitle=sub))
    ncpu = 1
    write("cpu.svg", chart.line_chart(
        [{"label": h, "points": [(x, v * 100.0 / ncpu) for x, v in
                                 to_hours(rate(H["cpu_busy"]), t0)]}
         for h, H in sorted(hosts.items()) if H["cpu_busy"]],
        "CPU busy per node", "jiffies/s (100 = one core)", events, y_zero=True))
    write("memory.svg", chart.line_chart(
        [{"label": h, "points": H["mem_used"]} for h, H in sorted(hosts.items()) if H["mem_used"]],
        "Node memory in use", "MiB", events, subtitle=sub))
    write("disk.svg", chart.line_chart(
        [{"label": f"{h} write", "points": [(x, v / 1e6) for x, v in to_hours(rate(H["disk_write"]), t0)]}
         for h, H in sorted(hosts.items()) if H["disk_write"]],
        "Disk write throughput per node", "MB/s", events, y_zero=True))
    write("network.svg", chart.line_chart(
        [{"label": f"{h} tx", "points": [(x, v / 1e6) for x, v in to_hours(rate(H["net_tx"]), t0)]}
         for h, H in sorted(hosts.items()) if H["net_tx"]],
        "Network transmit per node", "MB/s", events, y_zero=True))
    psi = [{"label": f"{h} {k[4:]}", "points": H[k]}
           for h, H in sorted(hosts.items()) for k in ("psi_cpu", "psi_io", "psi_mem") if H[k]]
    if psi:
        write("pressure.svg", chart.line_chart(
            psi, "Resource pressure (PSI, 60s average)",
            "% of time stalled", events, y_zero=True,
            subtitle="a busy node is not a saturated one; this separates them"))
    return written


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--samples", nargs="+", required=True)
    ap.add_argument("--events", default="", help="chaos jsonl, for fault markers")
    ap.add_argument("--charts-dir", default="")
    ap.add_argument("--out-json", default="")
    ap.add_argument("--warmup-hours", type=float, default=DEFAULTS["warmup_hours"])
    ap.add_argument("--plateau-seconds", type=float, default=0.0,
                    help="how long this workload needs before its state "
                         "PLATEAUS (its retention/TTL horizon). A judged "
                         "window shorter than a few of these cannot show "
                         "flatness at all - state is still filling by "
                         "design - so the leak bands are reported as not "
                         "judged rather than failed. Without this a "
                         "compressed rehearsal reports a cold start as an "
                         "831%/h leak, which is how a harness teaches its "
                         "owner to ignore it.")
    ap.add_argument("--judge-from-epoch", type=float, default=0.0,
                    help="wall-clock second at which the judged window opens. "
                         "Preferred over --warmup-hours because it is a FACT "
                         "the campaign recorded rather than a fraction of the "
                         "clock: the fill phase grows state on purpose, and a "
                         "warm-up guessed as a share of the run leaves that "
                         "cold-start climb inside the judged window, where it "
                         "reads as a leak")
    ap.add_argument("--min-incarnation-hours", type=float,
                    default=DEFAULTS["min_incarnation_hours"],
                    help="scales with the run: a compressed rehearsal has a "
                         "quiet window of minutes, and a fixed floor would "
                         "leave drift unjudged")
    args = ap.parse_args()

    samples = load(args.samples)
    if not samples:
        print("no samples: INCONCLUSIVE", file=sys.stderr)
        return 2
    t0 = samples[0]["ts"]
    duration = (samples[-1]["ts"] - t0) / 3600.0

    events = []
    if args.events and os.path.exists(args.events):
        for line in open(args.events):
            try:
                e = json.loads(line)
            except json.JSONDecodeError:
                continue
            # The chaos controller stamps ISO-8601 in "time"; accept an
            # epoch "ts" too rather than assuming one shape. Getting this
            # wrong is silent: the charts simply render with no fault
            # markers, and a reader sees a sawtooth with nothing explaining
            # it - which is the opposite of what the markers are for.
            ts = None
            if isinstance(e.get("ts"), (int, float)):
                ts = float(e["ts"])
            elif e.get("time"):
                try:
                    from datetime import datetime, timezone
                    ts = datetime.fromisoformat(
                        str(e["time"]).replace("Z", "+00:00")
                    ).replace(tzinfo=timezone.utc).timestamp()
                except ValueError:
                    ts = None
            if ts is not None:
                events.append(((ts - t0) / 3600.0,
                               e.get("fault") or e.get("kind") or "fault"))

    hosts, procs = collect(samples, t0)
    warmup_hours = args.warmup_hours
    if args.judge_from_epoch:
        warmup_hours = max(0.0, (args.judge_from_epoch - t0) / 3600.0)
    cfg = dict(DEFAULTS, warmup_hours=warmup_hours,
               min_incarnation_hours=args.min_incarnation_hours)
    judged_hours = max(0.0, duration - cfg["warmup_hours"])
    plateau_hours = args.plateau_seconds / 3600.0
    # Three horizons: one to fill, one to plateau, one to judge the plateau.
    rehearsal = bool(plateau_hours) and judged_hours < plateau_hours * 3
    findings, per_process = judge(procs, cfg, events)
    if rehearsal:
        # Not a pass and not a failure: this run verified the MACHINERY -
        # samplers on every host, collection, judgement, charts - and cannot
        # speak to flatness, because the workload it ran was still filling.
        findings = [f"this run is a machinery rehearsal, not a leak verdict: "
                    f"the judged window is {judged_hours * 60:.0f} min against a "
                    f"workload that needs {plateau_hours * 60:.0f} min just to "
                    f"plateau, so RSS is expected to climb and the bands are "
                    f"NOT judged"]

    charts = []
    if args.charts_dir:
        charts = render(hosts, procs, t0, args.charts_dir, events)

    report = {
        "duration_hours": round(duration, 2),
        "samples": len(samples),
        "hosts": sorted(hosts),
        "processes": sorted(procs),
        "fault_events": len(events),
        "bands": cfg,
        "per_process": per_process,
        "rehearsal": rehearsal,
        "findings": findings,
        "charts": charts,
    }
    if args.out_json:
        with open(args.out_json, "w") as fh:
            json.dump(report, fh, indent=1)
    print(json.dumps({k: report[k] for k in
                      ("duration_hours", "samples", "hosts", "processes",
                       "fault_events", "findings", "charts")}, indent=1))
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
