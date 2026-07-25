#!/usr/bin/env python3
"""Memory sampling for the cross-engine harness.

Companion to cpu.py. CPU is a cumulative counter, so cpu.py samples it before and
after the drain and takes the delta. Memory is a GAUGE, so a delta is meaningless:
what matters is the high-water mark and the steady-state level.

Both engines run containerised in throughput_sampled.sh, so both are read the same
way, from cgroup v2 inside the container:

  memory.peak        high-water mark since the cgroup was created. NOT resettable
                     from inside the container, so it is only meaningful on a
                     FRESHLY COMPOSED stack - which the harness already requires
                     for a measured variant (see the nexmark README). On a reused
                     stack it is the max across everything that ran, which is why
                     the harness records `mem_fresh_stack: false` in that case.
  memory.stat anon   anonymous memory: the engine's own heap and stacks. This is
                     the fair JVM-vs-native comparison, because it excludes the
                     page cache a Kafka consumer accumulates, which is charged to
                     the cgroup but is the kernel's to reclaim and is not the
                     engine's working set.
  memory.stat file   page cache charged to the cgroup, reported separately rather
                     than hidden, so the difference between "engine memory" and
                     "container memory" stays visible.
  memory.current     current total charge (anon + file + slab + ...), the
                     container-level number.

One `docker exec` per container at the END of a run, deliberately: sampling in a
loop would burn CPU INSIDE the container being measured and inflate the very
cpu_seconds figure this harness reports. `docker stats` would avoid that (the
daemon reads it out of band) but reports a single derived working-set number
rather than the anon/file split that makes the comparison honest.

  mem.py read <container> [container...]     -> JSON {peak, anon, file, current}
                                                summed across containers
  mem.py merge <result.json> --mem '<json>' [--fresh-stack]
        merges peak_bytes / anon_bytes / file_bytes / current_bytes plus
        anon_mb / peak_mb, and bytes_per_event_anon when input_events is present.
"""
import argparse
import json
import subprocess
import sys


def _read_file(container, path):
    try:
        out = subprocess.run(
            ["docker", "exec", container, "cat", path],
            capture_output=True, text=True, timeout=15,
        )
        return out.stdout.strip()
    except Exception:
        return ""


def read_one(container):
    """Per-container {peak, anon, file, current} in bytes; 0 for anything absent."""
    vals = {"peak": 0, "anon": 0, "file": 0, "current": 0}

    peak = _read_file(container, "/sys/fs/cgroup/memory.peak")
    if peak.isdigit():
        vals["peak"] = int(peak)

    cur = _read_file(container, "/sys/fs/cgroup/memory.current")
    if cur.isdigit():
        vals["current"] = int(cur)

    stat = _read_file(container, "/sys/fs/cgroup/memory.stat")
    for line in stat.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[0] in ("anon", "file") and parts[1].isdigit():
            vals[parts[0]] = int(parts[1])

    # memory.peak is absent on cgroup v1 (and on kernels before ~5.19); fall back to
    # the current charge so the field is never silently zero, and say so.
    if vals["peak"] == 0 and vals["current"] > 0:
        vals["peak"] = vals["current"]
        vals["peak_is_current_fallback"] = 1
    return vals


def read(containers):
    total = {"peak": 0, "anon": 0, "file": 0, "current": 0}
    fallback = False
    per = {}
    for c in containers:
        v = read_one(c)
        per[c] = v
        for k in ("peak", "anon", "file", "current"):
            total[k] += v[k]
        fallback = fallback or bool(v.get("peak_is_current_fallback"))
    total["per_container"] = per
    if fallback:
        total["peak_is_current_fallback"] = True
    return total


def main():
    if len(sys.argv) < 2:
        print("usage: mem.py {read|merge} ...", file=sys.stderr)
        return 2
    cmd = sys.argv[1]

    if cmd == "read":
        print(json.dumps(read(sys.argv[2:])))
        return 0

    if cmd == "merge":
        ap = argparse.ArgumentParser()
        ap.add_argument("path")
        ap.add_argument("--mem", required=True, help="JSON from `mem.py read`")
        ap.add_argument("--fresh-stack", action="store_true",
                        help="set when the stack was composed for THIS run, which is "
                             "what makes memory.peak attributable")
        a = ap.parse_args(sys.argv[2:])

        m = json.loads(a.mem)
        with open(a.path) as fh:
            r = json.load(fh)

        r["peak_bytes"] = m.get("peak", 0)
        r["anon_bytes"] = m.get("anon", 0)
        r["file_bytes"] = m.get("file", 0)
        r["current_bytes"] = m.get("current", 0)
        r["peak_mb"] = round(m.get("peak", 0) / 1048576, 1)
        r["anon_mb"] = round(m.get("anon", 0) / 1048576, 1)
        r["mem_fresh_stack"] = bool(a.fresh_stack)
        if m.get("peak_is_current_fallback"):
            r["peak_is_current_fallback"] = True
        if m.get("per_container"):
            r["mem_per_container"] = m["per_container"]

        ev = r.get("input_events") or r.get("processed") or 0
        if ev and m.get("anon"):
            r["bytes_per_event_anon"] = round(m["anon"] / ev, 2)

        with open(a.path, "w") as fh:
            json.dump(r, fh)
        print(json.dumps({k: r[k] for k in ("query", "engine", "anon_mb", "peak_mb",
                                            "mem_fresh_stack") if k in r}))
        return 0

    print(f"unknown command {cmd}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
