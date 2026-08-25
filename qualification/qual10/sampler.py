#!/usr/bin/env python3
"""QUAL-10's instrument: what every node is doing, sampled from outside.

Runs DETACHED on each rig host and appends one JSON object per sample to a
local file. Two rules shape it.

It measures from OUTSIDE the engine. Every number here comes from /proc,
the cgroup, or the filesystem - never from clink's own metrics endpoint. An
engine gauge that under-reports a leak is exactly the failure this campaign
exists to catch, so it cannot also be the instrument. Engine metrics are
collected separately as corroboration and are never the criterion.

It never dies quietly. Every probe is individually guarded: a metric that
cannot be read this tick records null and the sample still lands. A sampler
that exits on an unreadable file would leave a gap, and a gap in a leak
campaign is indistinguishable from a flat stretch - the run would read as
"no trend" when it actually means "no data". The driver treats a gap beyond
its threshold as INCONCLUSIVE for that reason.

Usage (per host, detached by the campaign):
    nohup ./sampler.py --out /qual/metrics/$(hostname).jsonl \\
        --interval 15 --state-dir /qual/state &
"""
import argparse
import glob
import json
import os
import socket
import subprocess
import sys
import time


def _read(path):
    try:
        with open(path) as fh:
            return fh.read()
    except OSError:
        return None


def _int(x):
    try:
        return int(x)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------- host ----

def cpu_totals():
    """Cumulative jiffies per state from /proc/stat.

    Cumulative on purpose: rates are computed at ANALYSIS time from
    successive samples. A sampler that computes its own rate has to hold
    state across ticks, and then a restarted sampler silently reports a
    wrong first rate - the kind of artefact that looks like a step change
    in a trend plot.
    """
    txt = _read("/proc/stat")
    if not txt:
        return None
    out = {}
    for line in txt.splitlines():
        parts = line.split()
        if not parts or not parts[0].startswith("cpu"):
            continue
        fields = ["user", "nice", "system", "idle", "iowait",
                  "irq", "softirq", "steal", "guest", "guest_nice"]
        vals = {f: _int(v) for f, v in zip(fields, parts[1:])}
        out[parts[0]] = vals
        if parts[0] != "cpu":
            continue
    return out


def meminfo():
    txt = _read("/proc/meminfo")
    if not txt:
        return None
    want = {"MemTotal", "MemFree", "MemAvailable", "Buffers", "Cached",
            "SwapTotal", "SwapFree", "Dirty", "Writeback", "Slab",
            "SReclaimable", "SUnreclaim", "PageTables", "Committed_AS"}
    out = {}
    for line in txt.splitlines():
        k, _, rest = line.partition(":")
        if k in want:
            out[k] = _int(rest.split()[0]) if rest.split() else None
    return out


def diskstats():
    """Per-device cumulative IO. Partitions and loop/ram devices dropped:
    they double-count their parent and bury the two devices that matter."""
    txt = _read("/proc/diskstats")
    if not txt:
        return None
    out = {}
    for line in txt.splitlines():
        f = line.split()
        if len(f) < 14:
            continue
        name = f[2]
        if name.startswith(("loop", "ram", "dm-")):
            continue
        out[name] = {
            "reads": _int(f[3]), "read_sectors": _int(f[5]),
            "read_ms": _int(f[6]),
            "writes": _int(f[7]), "write_sectors": _int(f[9]),
            "write_ms": _int(f[10]),
            "io_in_flight": _int(f[11]), "io_ms": _int(f[12]),
        }
    return out


def netdev():
    txt = _read("/proc/net/dev")
    if not txt:
        return None
    out = {}
    for line in txt.splitlines()[2:]:
        name, _, rest = line.partition(":")
        name = name.strip()
        f = rest.split()
        if name == "lo" or len(f) < 16:
            continue
        out[name] = {
            "rx_bytes": _int(f[0]), "rx_packets": _int(f[1]),
            "rx_errs": _int(f[2]), "rx_drop": _int(f[3]),
            "tx_bytes": _int(f[8]), "tx_packets": _int(f[9]),
            "tx_errs": _int(f[10]), "tx_drop": _int(f[11]),
        }
    return out


def pressure():
    """PSI: the most direct statement of "this node is starved" the kernel
    offers, and it separates a busy node from a saturated one - which a CPU
    percentage alone cannot."""
    out = {}
    for res in ("cpu", "memory", "io"):
        txt = _read(f"/proc/pressure/{res}")
        if not txt:
            continue
        for line in txt.splitlines():
            f = line.split()
            if not f:
                continue
            kind = f[0]
            for kv in f[1:]:
                k, _, v = kv.partition("=")
                try:
                    out[f"{res}_{kind}_{k}"] = float(v)
                except ValueError:
                    pass
    return out or None


def filesystems(paths):
    out = {}
    for p in paths:
        try:
            st = os.statvfs(p)
            out[p] = {
                "total_bytes": st.f_blocks * st.f_frsize,
                "free_bytes": st.f_bavail * st.f_frsize,
                "inodes_total": st.f_files,
                "inodes_free": st.f_favail,
            }
        except OSError:
            out[p] = None
    return out


# ----------------------------------------------------------- processes ----

def boot_time():
    txt = _read("/proc/stat") or ""
    for line in txt.splitlines():
        if line.startswith("btime"):
            return _int(line.split()[1])
    return None


def proc_sample(pid, btime):
    """Per-process footprint. The INCARNATION is the point: a kill replaces
    the process, and a trend across incarnations is a different object from
    a trend within one. Start time (jiffies since boot) plus boot time gives
    a stable id that survives pid reuse."""
    status = _read(f"/proc/{pid}/status")
    stat = _read(f"/proc/{pid}/stat")
    if not status or not stat:
        return None
    s = {}
    for line in status.splitlines():
        k, _, rest = line.partition(":")
        if k in ("VmRSS", "VmSize", "VmPeak", "VmHWM", "Threads", "FDSize"):
            parts = rest.split()
            s[k] = _int(parts[0]) if parts else None

    # /proc/pid/stat: fields after the comm field, which may itself contain
    # spaces and parentheses - split on the LAST ')' or the parse is wrong
    # for any process whose name has a space in it.
    after = stat[stat.rfind(")") + 2:].split()
    utime = _int(after[11]) if len(after) > 11 else None
    stime = _int(after[12]) if len(after) > 12 else None
    starttime = _int(after[19]) if len(after) > 19 else None
    hz = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100
    incarnation = None
    if starttime is not None and btime is not None:
        incarnation = btime + int(starttime / hz)

    try:
        fds = len(os.listdir(f"/proc/{pid}/fd"))
    except OSError:
        fds = None

    io = {}
    txt = _read(f"/proc/{pid}/io")
    if txt:
        for line in txt.splitlines():
            k, _, v = line.partition(":")
            if k in ("read_bytes", "write_bytes", "syscr", "syscw"):
                io[k] = _int(v.strip())

    return {
        "pid": pid,
        "incarnation": incarnation,
        "rss_kb": s.get("VmRSS"),
        "vsize_kb": s.get("VmSize"),
        "rss_peak_kb": s.get("VmHWM"),
        "vsize_peak_kb": s.get("VmPeak"),
        "threads": s.get("Threads"),
        "fds": fds,
        "utime_jiffies": utime,
        "stime_jiffies": stime,
        "io": io or None,
    }


def docker_containers():
    """(name, pid) for every running container. docker inspect gives the
    HOST pid, which is what /proc above needs - sampling inside the
    container would need a shell in each one every tick."""
    try:
        names = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=20, check=True,
        ).stdout.split()
    except Exception:  # noqa: BLE001
        return []
    out = []
    for n in names:
        try:
            pid = subprocess.run(
                ["docker", "inspect", "-f", "{{.State.Pid}}", n],
                capture_output=True, text=True, timeout=20, check=True,
            ).stdout.strip()
            p = _int(pid)
            if p:
                out.append((n, p))
        except Exception:  # noqa: BLE001
            continue
    return out


def cgroup_memory(name):
    """The container's own accounting, as a second view on RSS. A gap
    between this and the process's RSS is informative rather than
    contradictory: page cache and kernel memory are charged here too."""
    for pat in (f"/sys/fs/cgroup/system.slice/docker-*{name}*.scope",
                f"/sys/fs/cgroup/docker/*{name}*"):
        for d in glob.glob(pat):
            cur = _read(os.path.join(d, "memory.current"))
            peak = _read(os.path.join(d, "memory.peak"))
            if cur:
                return {"current": _int(cur.strip()),
                        "peak": _int(peak.strip()) if peak else None}
    return None


def snapshot_population(state_dir):
    """Retention as a TREND, not just at drain: the count of snapshot files
    per subtask directory at every sample. A retention defect that only
    shows between checkpoints is invisible to an end-of-run audit."""
    if not state_dir or not os.path.isdir(state_dir):
        return None
    out = {}
    try:
        for root, dirs, files in os.walk(state_dir):
            snaps = [f for f in files if f.endswith((".snap", ".arrow", ".sst"))]
            if snaps:
                rel = os.path.relpath(root, state_dir)
                out[rel] = len(snaps)
            if len(out) > 512:  # a pathological tree must not stall the tick
                out["_truncated"] = True
                break
    except OSError:
        return None
    return out


# ---------------------------------------------------------------- main ----

def sample(state_dir, fs_paths, btime):
    procs = []
    for name, pid in docker_containers():
        p = proc_sample(pid, btime)
        if p:
            p["container"] = name
            p["cgroup_memory"] = cgroup_memory(name)
            procs.append(p)
    return {
        "ts": time.time(),
        "host": socket.gethostname(),
        "host_metrics": {
            "cpu": cpu_totals(),
            "loadavg": _read("/proc/loadavg"),
            "mem": meminfo(),
            "disk": diskstats(),
            "net": netdev(),
            "pressure": pressure(),
            "fs": filesystems(fs_paths),
        },
        "processes": procs,
        "snapshots": snapshot_population(state_dir),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--interval", type=float, default=15.0)
    ap.add_argument("--state-dir", default="")
    ap.add_argument("--fs", action="append", default=[],
                    help="filesystem path to report free space for (repeatable)")
    ap.add_argument("--stop-file", default="",
                    help="sampler exits cleanly when this path appears")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    fs_paths = args.fs or ["/"]
    if args.state_dir:
        fs_paths.append(args.state_dir)
    btime = boot_time()

    while True:
        if args.stop_file and os.path.exists(args.stop_file):
            return 0
        try:
            s = sample(args.state_dir, fs_paths, btime)
        except Exception as e:  # noqa: BLE001
            # Never let one bad tick end the series: an error sample is
            # itself a data point, and a missing one is a gap the judge has
            # to treat as inconclusive.
            s = {"ts": time.time(), "host": socket.gethostname(),
                 "error": str(e)[:400]}
        try:
            with open(args.out, "a") as fh:
                fh.write(json.dumps(s) + "\n")
                fh.flush()
                os.fsync(fh.fileno())
        except OSError:
            pass
        time.sleep(args.interval)


if __name__ == "__main__":
    sys.exit(main())
