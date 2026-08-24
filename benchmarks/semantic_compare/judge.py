#!/usr/bin/env python3
"""Judge one query's two drained outputs under its DECLARED class.

Reads the declaration from queries.json (mode, key, tolerance), runs the
comparison, and writes a verdict JSON the campaign summariser consumes.
Three verdict shapes:

  gated + equal      the engines agree under the declared judgement.
  gated + not equal  a content divergence, named with sample rows/keys.
  not gated          nothing was proven: a submit failed, a drain failed,
                     or a side is EMPTY. An empty side is never a pass -
                     0 rows agrees with anything, which is a failure to
                     measure, not agreement. The reason is recorded.

The declared class is applied exactly as written; there is no downgrade
path here by construction.
"""
import argparse
import json
import pathlib
import sys

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import normalise  # noqa: E402


def judge(query, decl, a_path, b_path, not_gated_reason=None):
    verdict = {
        "query": query,
        "mode": decl["mode"],
        "tol_fields": list(decl.get("tol_fields", ())),
        "epsilon": decl.get("epsilon", 0.0),
    }
    if decl["mode"] == "materialised":
        verdict["key"] = list(decl["key"])
    if not_gated_reason:
        verdict.update(gated=False, equal=False, detail=not_gated_reason)
        return verdict
    try:
        if decl["mode"] == "materialised":
            a_n = len(normalise.load_state(a_path, tuple(decl["key"])))
            b_n = len(normalise.load_state(b_path, tuple(decl["key"])))
        else:
            a_n = len(normalise.load_rows(a_path))
            b_n = len(normalise.load_rows(b_path))
        if a_n == 0 or b_n == 0:
            verdict.update(
                gated=False, equal=False,
                detail=f"an empty side proves nothing: a={a_n} rows, b={b_n} rows")
            return verdict
        equal, detail = normalise.compare(
            a_path, b_path, mode=decl["mode"],
            key_fields=tuple(decl.get("key", ())),
            tol_fields=tuple(decl.get("tol_fields", ())),
            epsilon=decl.get("epsilon", 0.0))
        verdict.update(gated=True, equal=equal, detail=detail,
                       rows={"a": a_n, "b": b_n})
    except ValueError as e:
        verdict.update(gated=False, equal=False, detail=f"comparison refused: {e}")
    return verdict


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--query", required=True)
    ap.add_argument("--declarations", default=str(HERE / "queries.json"))
    ap.add_argument("--a", required=True, help="clink drain (values file or state dump)")
    ap.add_argument("--b", required=True, help="reference drain (same shape)")
    ap.add_argument("--out", required=True)
    ap.add_argument("--not-gated", default=None,
                    help="record this run-level failure instead of comparing")
    args = ap.parse_args()

    decls = json.loads(pathlib.Path(args.declarations).read_text())
    if args.query not in decls:
        raise SystemExit(f"{args.query} has no declaration in {args.declarations}")
    verdict = judge(args.query, decls[args.query], args.a, args.b, args.not_gated)
    pathlib.Path(args.out).write_text(json.dumps(verdict, indent=1) + "\n")
    status = ("agree" if verdict.get("equal")
              else ("NOT GATED" if not verdict.get("gated") else "DIVERGE"))
    print(f"  {args.query} [{verdict['mode']}]: {status} - {verdict['detail']}")
    return 0 if verdict.get("equal") else 1


if __name__ == "__main__":
    sys.exit(main())
