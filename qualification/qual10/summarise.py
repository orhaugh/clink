#!/usr/bin/env python3
"""QUAL-10's result logic: was the engine flat, and do we actually know?

Three ways this run can end, and the middle one is the one campaigns get
wrong:

  PASS          every band held, and every band was actually exercised.
  FAIL          a band was breached - a leak, named.
  INCONCLUSIVE  the run could not speak. A sampling gap, no metrics at all,
                or a criterion that never ran because the schedule did not
                give it the conditions it needs. That last one is why the
                analyser flags an unjudged drift test as a finding: a quiet
                window too short to judge is not evidence of no leak.

The correctness gates (every-key oracle, quiesce, catch-up) are the same
ones every campaign in this programme applies. A run that leaked nothing
but lost records is not a pass, and a flat memory graph over wrong output
would be the most misleading chart in the set.
"""
import argparse
import json
import os


def read_json(path):
    try:
        with open(path) as fh:
            return json.load(fh)
    except (OSError, json.JSONDecodeError):
        return None


def read_text(path):
    try:
        with open(path) as fh:
            return fh.read().strip()
    except OSError:
        return None


def build(out_dir, run_id, profile, local=False):
    leak = read_json(os.path.join(out_dir, "leak-report.json"))
    lines, problems, unknowns = [], [], []
    a = lines.append

    a(f"# QUAL-10 - leak and stability ({run_id})")
    a("")

    if leak is None:
        unknowns.append("no leak report: the metrics were never analysed")
    else:
        a(f"- duration: {leak['duration_hours']}h over {leak['samples']} samples")
        a(f"- hosts sampled: {len(leak['hosts'])} ({', '.join(leak['hosts'])})")
        a(f"- engine processes watched: {len(leak['processes'])}")
        a(f"- faults injected: {leak['fault_events']}")
        a("")
        b = leak["bands"]
        a("## Bands, written before the run")
        a("")
        a(f"- RSS drift within an incarnation: <= {b['rss_pct_per_hour']}%/h")
        a(f"- RSS plateau growth across incarnations: <= x{b['growth_ratio']}")
        a(f"- threads and fds: within +/-{int(b['fd_thread_tolerance'] * 100)}% of the median")
        a(f"- sampling gap: <= {b['max_gap_minutes']} minutes")
        a("")
        a("## Per process")
        a("")
        for name, p in sorted(leak["per_process"].items()):
            across = p["across"]
            slope = p.get("overall_slope_pct_per_hour")
            a(f"### `{name}`")
            a("")
            a(f"- incarnations judged for drift: {p['incarnations_judged']}")
            if slope is not None:
                a(f"- overall RSS slope: {slope:+.3f}%/h")
            a(f"- plateaus across restarts: {across['verdict']}"
              + (f", ratio {across['growth_ratio']} over {across['judged']} incarnations"
                 if across.get("growth_ratio") else ""))
            for metric in ("threads", "fds"):
                m = p.get(metric)
                if m:
                    a(f"- {metric}: median {m['median']}, peak {m['max']}")
            a("")
        for f in leak["findings"]:
            (unknowns if ("INCONCLUSIVE" in f or "never judged" in f) else problems).append(f)

        if leak.get("charts"):
            a("## What it looked like")
            a("")
            a("A leak verdict is a claim about shape. These are the samples "
              "themselves, with the injected faults marked, so the shape "
              "argues for itself rather than resting on the arithmetic above.")
            a("")
            for c in leak["charts"]:
                title = os.path.splitext(c)[0].replace("_", " ")
                a(f"![{title}](charts/{c})")
                a("")

    # The correctness gates: flat memory over wrong output is not a pass.
    for label, path, bad in (
        ("the job vanished mid-run", "job-gone.txt", True),
        ("the chaos controller died", "chaos-died.txt", True),
        ("the oracle was dirty", "oracle-dirty.txt", True),
        ("no metrics were retrieved", "metrics-missing.txt", True),
    ):
        txt = read_text(os.path.join(out_dir, path))
        if txt:
            (problems if bad else unknowns).append(f"{label}: {txt[:200]}")

    if problems:
        result = "FAIL"
    elif unknowns:
        result = "INCONCLUSIVE"
    elif leak is None:
        result = "INCONCLUSIVE"
    else:
        result = "PASS"

    if problems:
        a("## Findings")
        a("")
        for p in problems:
            a(f"- {p}")
        a("")
    if unknowns:
        a("## What this run could not say")
        a("")
        for u in unknowns:
            a(f"- {u}")
        a("")

    a(f"**RESULT: {result}**")
    a("")
    return "\n".join(lines) + "\n", result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--profile", default="trend")
    ap.add_argument("--local", action="store_true")
    args = ap.parse_args()
    text, _ = build(args.out_dir, args.run_id, args.profile, args.local)
    print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
