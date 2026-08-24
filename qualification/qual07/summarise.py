#!/usr/bin/env python3
"""QUAL-07 result logic: the semantic-comparison campaign's verdict.

The campaign claims that clink and an independent, widely deployed
reference implementation compute the SAME ANSWERS for the declared query
set, each query judged under the class queries.json declared in advance.
The verdict logic guards the two ways that claim can rot:

  * a content divergence on any gated query is a FAIL, naming the query
    and its sample rows - that is the campaign finding its purpose;
  * anything short of full gated coverage is INCONCLUSIVE, never PASS:
    a NOT-GATED verdict (submit/settle/drain failure, empty side), a
    declared query with no verdict at all (a silent absence), a verdict
    for a query the declarations do not know, and DECLARATION DRIFT - a
    verdict whose judged class, key, tolerance fields, or epsilon differ
    from what queries.json declares NOW. The class was chosen before the
    run for a reason; a comparison judged under a different one proves a
    different statement.
"""
import argparse
import json
import os
import sys


def load_declarations(path):
    with open(path) as fh:
        return {k: v for k, v in json.load(fh).items() if not k.startswith("_")}


def drifted(verdict, decl):
    if verdict.get("mode") != decl.get("mode"):
        return f"mode {verdict.get('mode')} != declared {decl.get('mode')}"
    if sorted(verdict.get("tol_fields", [])) != sorted(decl.get("tol_fields", [])):
        return "tolerance fields differ from the declaration"
    if float(verdict.get("epsilon", 0.0)) != float(decl.get("epsilon", 0.0)):
        return f"epsilon {verdict.get('epsilon')} != declared {decl.get('epsilon', 0.0)}"
    if decl.get("mode") == "materialised" and list(verdict.get("key", [])) != list(decl.get("key", [])):
        return "key differs from the declaration"
    return None


def build(out_dir, run_id, declarations_path):
    decls = load_declarations(declarations_path)
    verdicts = {}
    for name in sorted(os.listdir(out_dir)) if os.path.isdir(out_dir) else []:
        if not name.endswith("-verdict.json"):
            continue
        with open(os.path.join(out_dir, name)) as fh:
            v = json.load(fh)
        verdicts[v.get("query", name)] = v

    diverged = []
    ungated = []
    drift = []
    agreed = []
    unknown = sorted(set(verdicts) - set(decls))
    missing = sorted(set(decls) - set(verdicts))
    for q in sorted(set(verdicts) & set(decls)):
        v = verdicts[q]
        d = drifted(v, decls[q])
        if d:
            drift.append((q, d))
        elif not v.get("gated"):
            ungated.append((q, v.get("detail", "")))
        elif not v.get("equal"):
            diverged.append((q, v.get("detail", "")))
        else:
            agreed.append(q)

    if diverged:
        result = "FAIL"
    elif ungated or drift or missing or unknown or not verdicts:
        result = "INCONCLUSIVE"
    else:
        result = "PASS"

    lines = []
    a = lines.append
    a(f"# QUAL-07 - semantic comparison against the reference engine ({run_id})")
    a("")
    a(f"- declared queries: {len(decls)}; verdicts: {len(verdicts)}")
    a(f"- agree under their declared class: {len(agreed)}")
    a("")
    a("## Per-query judgement")
    a("")
    for q in sorted(decls):
        d = decls[q]
        cls = d["mode"]
        if d.get("tol_fields"):
            cls += f" + tolerance({','.join(d['tol_fields'])} @ {d.get('epsilon')})"
        if q in (x for x, _ in diverged):
            a(f"- {q} [{cls}]: DIVERGED")
        elif q in (x for x, _ in drift):
            a(f"- {q} [{cls}]: DECLARATION DRIFT")
        elif q in (x for x, _ in ungated):
            a(f"- {q} [{cls}]: NOT GATED")
        elif q in agreed:
            v = verdicts[q]
            rows = v.get("rows", {})
            a(f"- {q} [{cls}]: agree ({v.get('detail', '')})")
        else:
            a(f"- {q} [{cls}]: NO VERDICT")
    a("")
    if diverged:
        a("## Divergences")
        a("")
        for q, detail in diverged:
            a(f"- {q}: {detail}")
        a("")
    if ungated:
        a("## Not gated (nothing proven)")
        a("")
        for q, detail in ungated:
            a(f"- {q}: {detail}")
        a("")
    if drift:
        a("## Declaration drift (judged under a different class than declared)")
        a("")
        for q, why in drift:
            a(f"- {q}: {why}")
        a("")
    if missing:
        a(f"- SILENT ABSENCES (declared, never judged): {', '.join(missing)}")
        a("")
    if unknown:
        a(f"- UNDECLARED VERDICTS: {', '.join(unknown)}")
        a("")
    a(f"**RESULT: {result}**")
    a("")
    return "\n".join(lines) + "\n", result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True, help="the runner's results directory")
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--declarations",
                    default=os.path.join(os.path.dirname(__file__), "..", "..",
                                         "benchmarks", "semantic_compare", "queries.json"))
    args = ap.parse_args()
    text, _ = build(args.out_dir, args.run_id, args.declarations)
    sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
