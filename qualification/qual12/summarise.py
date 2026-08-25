#!/usr/bin/env python3
"""QUAL-12 result logic: judge a run of the declared refusal matrix.

The campaign's claim is that clink never silently weakens a security
posture. That is a set of refusals, so the verdict logic guards the two
ways a refusal matrix rots:

1. A row whose measured outcome differs from its declared one is a FAIL,
   named. Declared REFUSE that accepted is the campaign's whole subject.
   Declared ACCEPT that refused matters just as much: a matrix with no
   accept rows would pass an engine that refused everything, which is not
   a secure engine but an unusable one.

2. A row that could not be exercised is INCONCLUSIVE, never a pass. An
   unproven row and a missing row must not look alike - that is precisely
   how this campaign's own premise went wrong before it started, when the
   programme audit recorded that the live suite "already proves refusal
   shapes against real SASL/TLS brokers" and it did not.

Live rows carry a further rule: they need a real server, and a runner
that could not start one must say so per row rather than quietly
downgrading the run to whatever it could reach hermetically.
"""
import argparse
import json
import os
import sys


def load_matrix(path):
    with open(path) as fh:
        doc = json.load(fh)
    rows = []
    for surface, entries in doc.items():
        if surface.startswith("_"):
            continue
        for e in entries:
            e = dict(e)
            e["surface"] = surface
            rows.append(e)
    return rows


def load_results(path):
    """{row id -> measured outcome}. The runner writes one JSON object per
    line: {"id": ..., "outcome": "REFUSE|WARN|ACCEPT|UNEXERCISED",
    "detail": "..."}."""
    out = {}
    if not os.path.exists(path):
        return out
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except Exception:  # noqa: BLE001
                continue
            if r.get("id"):
                out[r["id"]] = r
    return out


def build(matrix_path, results_path, run_id):
    rows = load_matrix(matrix_path)
    results = load_results(results_path)

    agreed, wrong, unexercised, missing = [], [], [], []
    for row in rows:
        got = results.get(row["id"])
        if got is None:
            missing.append(row)
            continue
        outcome = got.get("outcome", "")
        if outcome in ("UNEXERCISED", ""):
            unexercised.append((row, got))
        elif outcome == row["outcome"]:
            agreed.append((row, got))
        else:
            wrong.append((row, got))
    extra = sorted(set(results) - {r["id"] for r in rows})

    if wrong:
        result = "FAIL"
    elif unexercised or missing or extra or not rows:
        result = "INCONCLUSIVE"
    else:
        result = "PASS"

    lines = []
    a = lines.append
    a(f"# QUAL-12 - security downgrades: the refusal matrix ({run_id})")
    a("")
    a(f"- declared rows: {len(rows)}; measured as declared: {len(agreed)}")
    live = [r for r in rows if r.get("proof") == "live"]
    live_ok = [r for r, _ in agreed if r.get("proof") == "live"]
    a(f"- rows needing a real server: {len(live)}; of those proven: {len(live_ok)}")
    a("")
    for surface in sorted({r["surface"] for r in rows}):
        a(f"## {surface}")
        a("")
        for row in [r for r in rows if r["surface"] == surface]:
            got = results.get(row["id"])
            state = "NO RESULT"
            if got is not None:
                outcome = got.get("outcome", "")
                if outcome in ("UNEXERCISED", ""):
                    state = f"UNEXERCISED - {got.get('detail', 'no reason given')}"
                elif outcome == row["outcome"]:
                    state = f"{outcome} as declared"
                else:
                    state = f"DECLARED {row['outcome']}, MEASURED {outcome}"
            a(f"- `{row['id']}` ({row['proof']}): {state}")
            a(f"  - {row['case']}")
        a("")
    if wrong:
        a("## Rows that did not behave as declared")
        a("")
        for row, got in wrong:
            a(f"- `{row['id']}`: declared {row['outcome']}, measured "
              f"{got.get('outcome')} - {got.get('detail', '')}")
            a(f"  - why it is declared that way: {row['reason']}")
        a("")
    if unexercised:
        a("## Rows that could not be exercised (no evidence, never a pass)")
        a("")
        for row, got in unexercised:
            a(f"- `{row['id']}`: {got.get('detail', 'no reason given')}")
        a("")
    if missing:
        a(f"- ROWS WITH NO RESULT AT ALL: {', '.join(r['id'] for r in missing)}")
        a("")
    if extra:
        a(f"- RESULTS FOR UNDECLARED ROWS: {', '.join(extra)}")
        a("")
    a(f"**RESULT: {result}**")
    a("")
    return "\n".join(lines) + "\n", result


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser()
    ap.add_argument("--matrix", default=os.path.join(here, "refusals.json"))
    ap.add_argument("--results", required=True)
    ap.add_argument("--run-id", required=True)
    args = ap.parse_args()
    text, _ = build(args.matrix, args.results, args.run_id)
    sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
