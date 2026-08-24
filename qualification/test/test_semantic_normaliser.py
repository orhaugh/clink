#!/usr/bin/env python3
"""The QUAL-07 comparator's rules, each proven to map ONLY true
equivalents. The normaliser is the campaign's false-pass surface: every
rule gets a positive case (true equivalents unify) AND a negative case
(a genuine difference survives it) - a normalisation tested only
positively is a mutation waiting to pass everything.
"""
import json
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

# --- materialised: upsert state dumps compared BY KEY ---------------------------
# The inputs are read_upsert_topic.py --json dumps: the reducer already
# folded the changelog (last write per broker key, tombstones removed);
# these tests pin what the comparator does with two such states.


def cmp_states(a_state, b_state, **kw):
    with tempfile.TemporaryDirectory() as tmp:
        a = pathlib.Path(tmp) / "a"
        b = pathlib.Path(tmp) / "b"
        a.write_text(json.dumps({"topic": "a", "state": a_state}))
        b.write_text(json.dumps({"topic": "b", "state": b_state}))
        return N.compare(str(a), str(b), **kw)


eq, why = cmp_states({"1": '{"cat":1,"n":2}', "2": '{"n":5,"cat":2}'},
                     {"1": '{"n":2,"cat":1}', "2": '{"cat":2,"n":5}'},
                     mode="materialised")
check("identical final images unify (field order included)", eq, why)
eq, why = cmp_states({"1": '{"cat":1,"n":2}'}, {"1": '{"cat":1,"n":3}'},
                     mode="materialised")
check("a different final image survives and names its key",
      not eq and "key 1" in why, why)
eq, why = cmp_states({"1": '{"n":2}', "2": '{"n":5}'}, {"1": '{"n":2}'},
                     mode="materialised")
check("a missing key is a key-set mismatch", not eq and "key-set" in why, why)
eq, _ = cmp_states({"1": '{"avg":49975.0}'}, {"1": '{"avg":49975}'},
                   mode="materialised")
check("integer-valued floats collapse inside state values", eq)
# The pairing discipline: canonical rows sort by their alphabetically-first
# field, so with avgp differing slightly across engines a POSITIONAL zip of
# sorted values pairs key 1's row with key 2's. Keyed pairing must not.
eq, why = cmp_states(
    {"1": '{"avgp":10.0001,"total":7}', "2": '{"avgp":10.0002,"total":9}'},
    {"1": '{"avgp":10.0002,"total":7}', "2": '{"avgp":10.0001,"total":9}'},
    mode="materialised", tol_fields=("avgp",), epsilon=0.001)
check("rows pair by key, not by sorted position", eq, why)
eq, why = cmp_states({"1": '{"avgp":10.0,"total":7}'},
                     {"1": '{"avgp":10.5,"total":7}'},
                     mode="materialised", tol_fields=("avgp",), epsilon=0.001)
check("state tolerance outside epsilon survives", not eq, why)
eq, why = cmp_states({"1": '{"avgp":10.0,"total":7}'},
                     {"1": '{"avgp":10.0,"total":8}'},
                     mode="materialised", tol_fields=("avgp",), epsilon=0.001)
check("state tolerance never leaks onto undeclared fields", not eq, why)
try:
    with tempfile.TemporaryDirectory() as tmp:
        a = pathlib.Path(tmp) / "a"
        b = pathlib.Path(tmp) / "b"
        a.write_text(json.dumps({"topic": "t", "error": "topic t not found"}))
        b.write_text(json.dumps({"topic": "t", "state": {}}))
        N.compare(str(a), str(b), mode="materialised")
    check("a failed drain fails the comparison, never reads as empty", False)
except ValueError:
    check("a failed drain fails the comparison, never reads as empty", True)

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

# --- the classification cannot drift from the query definitions -----------------
# queries.json declares HOW each query is judged; gen_queries.py declares
# WHAT each query is. A query added to one and not the other, a
# materialised class without an upsert template to run, or a declared key
# that is not the template's primary key would each surface mid-campaign
# as an unrunnable or wrongly-judged query. Caught here instead.
SEM = HERE.parent.parent / "benchmarks" / "semantic_compare"
sys.path.insert(0, str(HERE.parent.parent / "benchmarks" / "nexmark_compare" / "queries"))
import gen_queries  # noqa: E402

decls = {k: v for k, v in json.loads((SEM / "queries.json").read_text()).items()
         if not k.startswith("_")}
defs = gen_queries.QUERIES
check("every defined query is classified, and nothing else",
      set(decls) == set(defs),
      f"only-classified={sorted(set(decls) - set(defs))} "
      f"only-defined={sorted(set(defs) - set(decls))}")
for q, d in sorted(decls.items()):
    if d["mode"] == "materialised":
        check(f"{q}: materialised declares the template's primary key",
              d.get("variant") == "upsert" and d.get("key") == defs[q].get("pk"),
              f"declared={d.get('key')} pk={defs[q].get('pk')}")
    else:
        check(f"{q}: append never claims the upsert variant",
              d["mode"] == "append" and d.get("variant") == "kafka")
    sink_fields = {name for name, _ in defs[q]["sink"]}
    check(f"{q}: tolerance fields exist in the sink schema",
          set(d.get("tol_fields", ())) <= sink_fields,
          f"declared={d.get('tol_fields')} sink={sorted(sink_fields)}")

print(f"\n{len(failures)} failure(s)" if failures else "\nall comparator checks passed")
sys.exit(1 if failures else 0)
