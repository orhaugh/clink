#!/usr/bin/env python3
"""QUAL-07 summariser result logic against synthetic verdict directories.

The shapes that matter for a SEMANTIC campaign: a content divergence is
the campaign working (FAIL, named); anything short of full gated
coverage - a not-gated verdict, a silent absence, an undeclared verdict,
or a verdict judged under a different class than the declaration - is
INCONCLUSIVE, never PASS.
"""
import json
import pathlib
import subprocess
import sys
import tempfile

HERE = pathlib.Path(__file__).resolve().parent
SUMMARISE = HERE.parent / "qual07" / "summarise.py"

DECLS = {
    "qa": {"mode": "append", "reason": "r"},
    "qb": {"mode": "append", "tol_fields": ["price"], "epsilon": 1e-6, "reason": "r"},
    "qc": {"mode": "materialised", "variant": "upsert", "key": ["cat"], "reason": "r"},
}


def verdict(query, *, gated=True, equal=True, detail="ok", **over):
    d = DECLS[query]
    v = {"query": query, "mode": d["mode"],
         "tol_fields": d.get("tol_fields", []), "epsilon": d.get("epsilon", 0.0),
         "gated": gated, "equal": equal, "detail": detail}
    if d["mode"] == "materialised":
        v["key"] = d["key"]
    v.update(over)
    return v


def run(verdicts):
    with tempfile.TemporaryDirectory() as tmp:
        d = pathlib.Path(tmp)
        (d / "queries.json").write_text(json.dumps(DECLS))
        for v in verdicts:
            (d / f"{v['query']}-verdict.json").write_text(json.dumps(v))
        out = subprocess.run(
            [sys.executable, str(SUMMARISE), "--out-dir", str(d), "--run-id", "t",
             "--declarations", str(d / "queries.json")],
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


ALL = [verdict("qa"), verdict("qb"), verdict("qc")]

r, _ = run(ALL)
check("every query gated and agreeing is a PASS", r, "PASS")

r, text = run([verdict("qa"), verdict("qb", equal=False, detail="row 3 differs"),
               verdict("qc")])
check("a content divergence is a FAIL", r, "FAIL")
check("the divergence names its query and rows",
      "qb: row 3 differs" in text, True, text)

r, _ = run([verdict("qa"), verdict("qb"),
            verdict("qc", gated=False, equal=False, detail="clink submit failed")])
check("a not-gated verdict is INCONCLUSIVE", r, "INCONCLUSIVE")

r, _ = run([verdict("qa"), verdict("qb")])
check("a declared query with no verdict is INCONCLUSIVE", r, "INCONCLUSIVE")

r, _ = run(ALL + [dict(verdict("qa"), query="qz")])
check("a verdict for an undeclared query is INCONCLUSIVE", r, "INCONCLUSIVE")

r, _ = run([verdict("qa"), verdict("qb", epsilon=1.0), verdict("qc")])
check("a verdict judged at a different epsilon is drift, INCONCLUSIVE",
      r, "INCONCLUSIVE")

r, _ = run([verdict("qa"), verdict("qb"), verdict("qc", key=["other"])])
check("a verdict judged under a different key is drift, INCONCLUSIVE",
      r, "INCONCLUSIVE")

r, _ = run([verdict("qa"), verdict("qb", mode="materialised", key=["price"]),
            verdict("qc")])
check("a verdict judged under a different mode is drift, INCONCLUSIVE",
      r, "INCONCLUSIVE")

r, _ = run([])
check("an empty verdict set is INCONCLUSIVE", r, "INCONCLUSIVE")

r, _ = run([verdict("qa", gated=False, equal=False, detail="drain failed"),
            verdict("qb", equal=False, detail="row 1 differs"), verdict("qc")])
check("a divergence dominates a coverage gap (FAIL beats INCONCLUSIVE)", r, "FAIL")

print(f"\n{11 - len(failures)} passed, {len(failures)} failed")
sys.exit(1 if failures else 0)
