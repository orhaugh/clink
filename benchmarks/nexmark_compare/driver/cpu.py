#!/usr/bin/env python3
"""CPU sampling for the cross-engine harness (Cores*Time normalisation).

clink runs as host processes (macOS - read `ps -o cputime=`); Flink runs in Linux
containers (read cgroup v2 `cpu.stat usage_usec`). Both give CUMULATIVE CPU time,
so run.sh samples before submit and after the drain and merges the delta into the
result JSON.

  cpu.py read-clink <pid> [pid...]        -> cumulative CPU seconds (sum)
  cpu.py read-flink <container> [c...]     -> cumulative CPU seconds (sum)
  cpu.py merge <result.json> --cpu-pre A --cpu-post B --wall-pre C --wall-post D \
               --input-events N
        adds cpu_seconds, wall_seconds, cores (cpu/wall), input_events, and
        events_per_cpu_sec (input_events / cpu_seconds) to the result JSON.
"""
import argparse
import json
import subprocess
import sys


def parse_cputime(s):
    """macOS `ps -o cputime=` -> seconds. Format [[DD-]HH:]MM:SS.ss."""
    s = s.strip()
    if not s:
        return 0.0
    days = 0.0
    if "-" in s:
        d, s = s.split("-", 1)
        days = float(d)
    parts = s.split(":")
    parts = [float(p) for p in parts]
    secs = 0.0
    for p in parts:
        secs = secs * 60 + p
    return days * 86400 + secs


def read_clink(pids):
    total = 0.0
    for pid in pids:
        try:
            out = subprocess.run(
                ["ps", "-o", "cputime=", "-p", str(pid)],
                capture_output=True, text=True, timeout=10,
            ).stdout
            total += parse_cputime(out)
        except Exception:
            pass  # process gone -> contributes 0 (was sampled while alive)
    return total


def read_flink(containers):
    total = 0.0
    for c in containers:
        try:
            out = subprocess.run(
                ["docker", "exec", c, "cat", "/sys/fs/cgroup/cpu.stat"],
                capture_output=True, text=True, timeout=10,
            ).stdout
            for line in out.splitlines():
                if line.startswith("usage_usec"):
                    total += int(line.split()[1]) / 1e6
                    break
        except Exception:
            pass
    return total


def main():
    if len(sys.argv) < 2:
        print("usage: cpu.py {read-clink|read-flink|merge} ...", file=sys.stderr)
        return 2
    cmd = sys.argv[1]
    if cmd == "read-clink":
        print(f"{read_clink(sys.argv[2:]):.3f}")
        return 0
    if cmd == "read-flink":
        print(f"{read_flink(sys.argv[2:]):.3f}")
        return 0
    if cmd == "merge":
        ap = argparse.ArgumentParser()
        ap.add_argument("path")
        ap.add_argument("--cpu-pre", type=float, required=True)
        ap.add_argument("--cpu-post", type=float, required=True)
        ap.add_argument("--wall-pre", type=float, required=True)
        ap.add_argument("--wall-post", type=float, required=True)
        ap.add_argument("--input-events", type=int, required=True)
        a = ap.parse_args(sys.argv[2:])
        with open(a.path) as fh:
            r = json.load(fh)
        cpu = max(a.cpu_post - a.cpu_pre, 0.0)
        wall = max(a.wall_post - a.wall_pre, 1e-9)
        r["cpu_seconds"] = round(cpu, 3)
        r["wall_seconds"] = round(wall, 3)
        r["cores"] = round(cpu / wall, 2)
        r["input_events"] = a.input_events
        r["events_per_cpu_sec"] = round(a.input_events / cpu, 1) if cpu > 0 else 0.0
        with open(a.path, "w") as fh:
            json.dump(r, fh)
        print(json.dumps({k: r[k] for k in
                          ("query", "engine", "cpu_seconds", "cores", "events_per_cpu_sec")}))
        return 0
    print(f"unknown command {cmd}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
