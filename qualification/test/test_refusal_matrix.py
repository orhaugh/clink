#!/usr/bin/env python3
"""The QUAL-12 matrix must stay honest about itself.

A declared matrix is only worth what its declarations are worth, so these
check the document rather than the engine: every row well-formed, every
outcome from the declared vocabulary, every row carrying the reason it
was declared that way, and - the load-bearing one - every surface
carrying at least one ACCEPT row.

That last rule is why: a matrix of nothing but refusals would be
satisfied by an engine that refused every configuration, which is not a
secure engine but an unusable one. The accept rows are what make the
refusals mean something.
"""
import json
import pathlib
import sys

HERE = pathlib.Path(__file__).resolve().parent
MATRIX = HERE.parent / "qual12" / "refusals.json"

OUTCOMES = {"REFUSE", "WARN", "ACCEPT"}
PROOFS = {"unit", "live"}

failures = []


def check(name, cond, detail=""):
    if cond:
        print(f"  PASS {name}")
    else:
        print(f"  FAIL {name} {detail}")
        failures.append(name)


doc = json.loads(MATRIX.read_text())
surfaces = {k: v for k, v in doc.items() if not k.startswith("_")}
# The id prefix per surface is DECLARED, not inferred: "cp" for
# control_plane and "pg" for postgres are deliberate, and a test that
# guessed the convention would fail correct rows.
prefixes = doc.get("_prefixes", {})
check("the matrix declares at least one surface", bool(surfaces))
check("every surface declares its id prefix",
      set(surfaces) <= set(prefixes), sorted(set(surfaces) - set(prefixes)))

ids = []
for surface, rows in surfaces.items():
    check(f"{surface}: has rows", bool(rows))
    for row in rows:
        rid = row.get("id", "<no id>")
        ids.append(rid)
        check(f"{rid}: names its case", bool(row.get("case")))
        check(f"{rid}: outcome is one of {sorted(OUTCOMES)}",
              row.get("outcome") in OUTCOMES, row.get("outcome"))
        check(f"{rid}: proof is one of {sorted(PROOFS)}",
              row.get("proof") in PROOFS, row.get("proof"))
        # A row without a reason is a rule nobody can review, and the
        # first thing to be argued away when it fails.
        check(f"{rid}: carries the reason it is declared that way",
              len(row.get("reason", "")) > 40, row.get("reason"))
        # A row that names no proof cannot be exercised, so it could
        # never legitimately pass - requiring both fields here means a
        # row can never drift away from the thing that proves it.
        check(f"{rid}: names the binary and test that prove it",
              bool(row.get("proof_binary")) and bool(row.get("proof_test")),
              f"{row.get('proof_binary')!r} / {row.get('proof_test')!r}")
        check(f"{rid}: id uses its surface's declared prefix",
              surface in prefixes and rid.startswith(prefixes[surface] + "."),
              f"{rid} vs prefix {prefixes.get(surface)!r}")

check("row ids are unique", len(ids) == len(set(ids)),
      [i for i in ids if ids.count(i) > 1])

for surface, rows in surfaces.items():
    accepts = [r for r in rows if r.get("outcome") == "ACCEPT"]
    check(f"{surface}: declares at least one ACCEPT row "
          f"(a matrix of pure refusals would pass an engine that refuses everything)",
          bool(accepts))

print(f"\n{len(failures)} failure(s)" if failures else "\nrefusal matrix is well-formed")
sys.exit(1 if failures else 0)
