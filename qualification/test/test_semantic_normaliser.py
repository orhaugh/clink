#!/usr/bin/env python3
"""The QUAL-07 comparator's rules, each proven to map ONLY true
equivalents. The normaliser is the campaign's false-pass surface: every
rule gets a positive case (true equivalents unify) AND a negative case
(a genuine difference survives it) - a normalisation tested only
positively is a mutation waiting to pass everything.
"""
import pathlib
import sys
import tempfile

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "benchmarks" / "semantic_compare"))
import normalise as N  # noqa: E402

failures = []


def check(name, cond, detail=""):
    if cond:
        print(f"  PASS {name}")
    else:
        print(f"  FAIL {name} {detail}")
        failures.append(name)


def cmp_files(a_lines, b_lines, **kw):
    with tempfile.TemporaryDirectory() as tmp:
        a = pathlib.Path(tmp) / "a"
        b = pathlib.Path(tmp) / "b"
        a.write_text("\n".join(a_lines) + "\n")
        b.write_text("\n".join(b_lines) + "\n")
        return N.compare(str(a), str(b), **kw)


# --- field order is presentation ------------------------------------------
eq, _ = cmp_files(['{"a":1,"b":2}'], ['{"b":2,"a":1}'], mode="append")
check("field order unifies", eq)
eq, why = cmp_files(['{"a":1,"b":2}'], ['{"a":1,"b":3}'], mode="append")
check("a value difference survives field-order normalisation", not eq, why)
eq, why = cmp_files(['{"a":1}'], ['{"a":1,"b":2}'], mode="append")
check("an extra field survives", not eq)

# --- integer-valued floats ---------------------------------------------------
eq, _ = cmp_files(['{"p":49975.0}'], ['{"p":49975}'], mode="append")
check("49975.0 unifies with 49975", eq)
eq, why = cmp_files(['{"p":49975.9}'], ['{"p":49975}'], mode="append")
check("49975.9 does NOT unify with 49975", not eq, why)
eq, why = cmp_files(['{"p":49975.9}'], ['{"p":49976.0}'], mode="append")
check("nearby non-integer floats stay different", not eq)

# --- ordering is removed, multiplicity is not ---------------------------------
eq, _ = cmp_files(['{"k":1}', '{"k":2}'], ['{"k":2}', '{"k":1}'], mode="append")
check("row order unifies", eq)
eq, why = cmp_files(['{"k":1}', '{"k":1}'], ['{"k":1}'], mode="append")
check("duplicate rows survive sorting (multiplicity kept)", not eq, why)

# --- corrupt input fails, never skips ------------------------------------------
try:
    cmp_files(['{"k":1}', 'not json'], ['{"k":1}'], mode="append")
    check("a corrupt sink line fails the comparison", False)
except ValueError:
    check("a corrupt sink line fails the comparison", True)

# --- materialised: update streams fold to final images -------------------------
eq, why = cmp_files(
    ['{"cat":1,"n":1}', '{"cat":1,"n":2}', '{"cat":2,"n":5}'],
    ['{"cat":1,"n":2}', '{"cat":2,"n":5}'],
    mode="materialised", key_fields=("cat",))
check("different update cadences with the same final images unify", eq, why)
eq, why = cmp_files(
    ['{"cat":1,"n":1}', '{"cat":1,"n":2}'],
    ['{"cat":1,"n":2}', '{"cat":1,"n":1}'],
    mode="materialised", key_fields=("cat",))
check("a different FINAL image survives folding", not eq, why)
try:
    cmp_files(['{"cat":1,"n":1}'], ['{"n":1}'],
              mode="materialised", key_fields=("cat",))
    check("a materialised row without its key field fails", False)
except ValueError:
    check("a materialised row without its key field fails", True)

# --- tolerance: declared fields only, everything else exact --------------------
eq, _ = cmp_files(['{"cat":1,"avg":10.0001}'], ['{"cat":1,"avg":10.0002}'],
                  mode="append", tol_fields=("avg",), epsilon=0.001)
check("a declared float field compares within epsilon", eq)
eq, why = cmp_files(['{"cat":1,"avg":10.0}'], ['{"cat":1,"avg":10.1}'],
                    mode="append", tol_fields=("avg",), epsilon=0.001)
check("outside epsilon survives", not eq, why)
eq, why = cmp_files(['{"cat":1,"avg":10.0}'], ['{"cat":2,"avg":10.0}'],
                    mode="append", tol_fields=("avg",), epsilon=0.001)
check("tolerance never leaks onto undeclared fields", not eq, why)

# --- count mismatches are named -------------------------------------------------
eq, why = cmp_files(['{"k":1}'], ['{"k":1}', '{"k":2}'], mode="append")
check("a count mismatch is named", not eq and "row-count mismatch" in why, why)

print(f"\n{len(failures)} failure(s)" if failures else "\nall comparator checks passed")
sys.exit(1 if failures else 0)
