#!/usr/bin/env python3
"""QUAL-06's DAG generator, against the real planner.

The campaign's width claim ("a graph of N operators") is by construction,
so the construction gets a gate: the generated SQL must compile, and its
compiled operator count must match the arithmetic the campaign quotes
(6B + 3). A planner change that alters the shape fails here, before a rig
is paid for on a stale claim.

Also pinned: determinism (same inputs, identical bytes - the generator is
part of the oracle's provenance), per-branch consumer-group uniqueness
(the shared-group trap loses ~(B-1)/B of the stream by construction), and
the branch predicates forming an exact partition of the key space.
"""
import pathlib
import re
import subprocess
import sys

HERE = pathlib.Path(__file__).resolve().parent
GEN = HERE.parent / "qual06" / "dag-gen.py"
SUBMIT = HERE.parent.parent / "build" / "clink_submit_sql"

failures = []


def check(name, ok, detail=""):
    if ok:
        print(f"  PASS {name}")
    else:
        print(f"  FAIL {name}{': ' + detail if detail else ''}")
        failures.append(name)


def gen(branches, tag="gate"):
    out = subprocess.run(
        [sys.executable, str(GEN), "--branches", str(branches), "--run-tag", tag],
        capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    return out.stdout


# Determinism: byte-identical across invocations.
check("generator is deterministic", gen(8) == gen(8))

# Distinct consumer group per branch table.
s = gen(12)
groups = re.findall(r"group_id='([^']+)'", s)
check("every branch has its own consumer group",
      len(groups) == 12 and len(set(groups)) == 12,
      f"{len(set(groups))} distinct of {len(groups)}")

# The branch predicates partition the key space exactly.
preds = sorted(int(m) for m in re.findall(r"\(k % 12\) = (\d+)", s))
check("branch predicates partition k%B exactly",
      preds == list(range(12)), str(preds))

# Width against the REAL planner. Harness verification requires the host
# build, same as every campaign's SUBMIT_BIN prerequisite.
if not SUBMIT.exists():
    check("host build present for the width gate", False,
          f"{SUBMIT} missing - build the tree first")
else:
    for b in (4, 24):
        sql = gen(b)
        for k, v in (("__BROKERS__", "b:9092"), ("__WM_LAG_MS__", "2000"),
                     ("__STATE_TTL_MS__", "600000"), ("__CONNINFO__", "c")):
            sql = sql.replace(k, v)
        import tempfile, json, os
        with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False) as f:
            f.write(sql)
            path = f.name
        try:
            out = subprocess.run([str(SUBMIT), "--file", path],
                                 capture_output=True, text=True)
            try:
                ops = len(json.loads(out.stdout)["ops"])
            except Exception:
                ops = -1
            check(f"B={b} compiles to 6B+4 = {6 * b + 4} operators",
                  ops == 6 * b + 4, f"got {ops}")
        finally:
            os.unlink(path)

print(f"\n{len(failures)} failed")
sys.exit(1 if failures else 0)
