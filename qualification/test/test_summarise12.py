#!/usr/bin/env python3
"""QUAL-12 summariser result logic against synthetic matrix results.

The shapes that matter for a REFUSAL campaign:

  * a declared REFUSE that accepted is the campaign's subject - FAIL;
  * a declared ACCEPT that refused is equally a FAIL, because a matrix
    with no accept rows would pass an engine that refused everything;
  * a row that could not be exercised is INCONCLUSIVE, never a pass - an
    unproven row and a missing row must not look alike, which is exactly
    how this campaign's own premise went wrong before it started;
  * a result for a row nobody declared is INCONCLUSIVE: the matrix is
    the contract, and a runner testing something else is drift.
"""
import json
import pathlib
import subprocess
import sys
import tempfile

HERE = pathlib.Path(__file__).resolve().parent
SUMMARISE = HERE.parent / "qual12" / "summarise.py"

MATRIX = {
    "_contract": ["synthetic"],
    "control_plane": [
        {"id": "cp.a", "case": "c", "outcome": "REFUSE", "proof": "unit", "reason": "r"},
        {"id": "cp.ok", "case": "c", "outcome": "ACCEPT", "proof": "unit", "reason": "r"},
    ],
    "postgres": [
        {"id": "pg.warn", "case": "c", "outcome": "WARN", "proof": "live", "reason": "r"},
    ],
}
ALL_GOOD = {"cp.a": "REFUSE", "cp.ok": "ACCEPT", "pg.warn": "WARN"}


def run(results):
    with tempfile.TemporaryDirectory() as tmp:
        d = pathlib.Path(tmp)
        (d / "m.json").write_text(json.dumps(MATRIX))
        (d / "r.jsonl").write_text(
            "\n".join(json.dumps({"id": k, "outcome": v, "detail": "d"})
                      for k, v in results.items()) + "\n")
        out = subprocess.run(
            [sys.executable, str(SUMMARISE), "--matrix", str(d / "m.json"),
             "--results", str(d / "r.jsonl"), "--run-id", "t"],
            capture_output=True, text=True)
        assert out.returncode == 0, out.stderr
        for line in out.stdout.splitlines():
            if line.startswith("**RESULT"):
                return line.strip("* ").replace("RESULT: ", ""), out.stdout
        return "(no result line)", out.stdout


failures = []


def check(name, got, want, text=""):
    if got == want:
        print(f"  PASS {name}")
    else:
        print(f"  FAIL {name}: result '{got}', wanted '{want}'")
        failures.append(name)


r, _ = run(ALL_GOOD)
check("every row behaving as declared is a PASS", r, "PASS")

r, text = run({**ALL_GOOD, "cp.a": "ACCEPT"})
check("a declared REFUSE that ACCEPTED is a FAIL", r, "FAIL")
check("the failing row is named with its declared reason",
      "cp.a" in text and "why it is declared" in text, True, text)

r, _ = run({**ALL_GOOD, "cp.ok": "REFUSE"})
check("a declared ACCEPT that REFUSED is a FAIL (refusing everything is not secure)",
      r, "FAIL")

r, _ = run({**ALL_GOOD, "pg.warn": "ACCEPT"})
check("a downgrade that was NOT stated (WARN measured as ACCEPT) is a FAIL", r, "FAIL")

r, text = run({**ALL_GOOD, "pg.warn": "UNEXERCISED"})
check("a row that could not be exercised is INCONCLUSIVE, never a pass", r, "INCONCLUSIVE")
check("the unexercised row is listed with its reason",
      "could not be exercised" in text, True, text)

r, _ = run({"cp.a": "REFUSE", "cp.ok": "ACCEPT"})
check("a row with no result at all is INCONCLUSIVE", r, "INCONCLUSIVE")

r, _ = run({**ALL_GOOD, "cp.undeclared": "REFUSE"})
check("a result for an undeclared row is INCONCLUSIVE (the matrix is the contract)",
      r, "INCONCLUSIVE")

r, _ = run({**ALL_GOOD, "cp.a": "ACCEPT", "pg.warn": "UNEXERCISED"})
check("a wrong outcome dominates an unexercised row (FAIL beats INCONCLUSIVE)", r, "FAIL")

print(f"\n{9 - len(failures)} passed, {len(failures)} failed")
sys.exit(1 if failures else 0)
