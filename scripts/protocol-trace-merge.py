#!/usr/bin/env python3
"""Merge protocol trace files into one ordered NDJSON stream for validation.

    scripts/protocol-trace-merge.py [--job N] [--out merged.ndjson] PATH...

Each PATH is a .ndjson file the engine wrote (CLINK_PROTOCOL_TRACE_DIR) or a
directory of them: one file per process. Events are ordered by their
wall-clock timestamp, then by per-process sequence number; on one machine that
is the causal order, since causally related events from different processes
are separated by a network round trip. Events carrying a `job` field are kept
for one job (--job, else the job of the first Trigger seen, else the first job
seen); events without one (sink-side events know only their subtask) are kept
as they are, so a directory should hold one job's run.

Writes the merged stream and prints a one-line summary. Exit 2 when nothing
was found.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def collect(paths: list[str]) -> list[dict]:
    events: list[dict] = []
    for raw in paths:
        p = Path(raw)
        files = sorted(p.glob("*.ndjson")) if p.is_dir() else [p]
        for f in files:
            with f.open() as fh:
                for n, line in enumerate(fh, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        e = json.loads(line)
                    except json.JSONDecodeError as ex:
                        raise SystemExit(f"{f}:{n}: not JSON: {ex}") from ex
                    if "event" not in e or "seq" not in e or "ts" not in e:
                        raise SystemExit(f"{f}:{n}: missing event/seq/ts")
                    events.append(e)
    return events


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("paths", nargs="+")
    ap.add_argument("--job", type=int, default=None, help="keep this job's events (default: the first Trigger's job)")
    ap.add_argument("--out", default="-", help="output file (default stdout)")
    args = ap.parse_args(argv)

    events = collect(args.paths)
    if not events:
        print("protocol-trace-merge: no events found", file=sys.stderr)
        return 2
    events.sort(key=lambda e: (e["ts"], e.get("proc", ""), e["seq"]))

    job = args.job
    if job is None:
        for e in events:
            if e["event"] == "Trigger" and "job" in e:
                job = e["job"]
                break
    if job is None:
        for e in events:
            if "job" in e:
                job = e["job"]
                break
    kept = [e for e in events if "job" not in e or e["job"] == job]

    out = sys.stdout if args.out == "-" else open(args.out, "w")
    try:
        for e in kept:
            out.write(json.dumps(e, separators=(",", ":")) + "\n")
    finally:
        if out is not sys.stdout:
            out.close()
    kinds: dict[str, int] = {}
    for e in kept:
        kinds[e["event"]] = kinds.get(e["event"], 0) + 1
    summary = ", ".join(f"{k}={v}" for k, v in sorted(kinds.items()))
    print(f"protocol-trace-merge: {len(kept)} event(s) for job {job} from {len(events)} read ({summary})", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
