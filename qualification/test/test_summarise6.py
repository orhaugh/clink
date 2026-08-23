#!/usr/bin/env python3
"""QUAL-06 summariser result logic against synthetic evidence directories.

The shapes that matter for a SCALE campaign:

  * a rung that fails to DEPLOY is a measured boundary, not a campaign
    failure - the verdict stays PASS for the largest green rung;
  * a rung whose deployed operator count disagrees with the generator's
    arithmetic is a FAIL - the width claim is the campaign's subject and
    an unverified width is a number, not a claim;
  * everything the retention campaign learned about judging correctness
    carries over: exactness only when caught up, fabricated keys always
    fatal, coverage gaps INCONCLUSIVE.
"""
import json
import pathlib
import subprocess
import sys
import tempfile

HERE = pathlib.Path(__file__).resolve().parent
SUMMARISE = HERE.parent / "qual06" / "summarise.py"

sys.path.insert(0, str(HERE.parent / "qual06"))
import summarise as q6_summarise  # noqa: E402

MANDATORY = list(q6_summarise.MANDATORY_EVENTS)


def write_rung(d, n, *, branches=8, ops=51, subtasks=204, par=4,
               status="green", deploy_s=12, first_ckpt_s=18,
               expected_ops=None, reason=""):
    (d / f"rung-{n}.txt").write_text(
        f"branches={branches}\nexpected_ops={expected_ops if expected_ops is not None else ops}\n"
        f"deployed_ops={ops}\nsubtasks={subtasks}\nparallelism={par}\n"
        f"deploy_s={deploy_s}\nfirst_checkpoint_s={first_ckpt_s}\n"
        f"status={status}\nreason={reason}\n")


def write_evidence(d, *, rungs=2, boundary=False, width_mismatch=False,
                   findings=(), stuck=False, quiesced=True,
                   produced=1000, sum_n=1000, checked=500,
                   missing=0, wrong_n=0, fabricated=0, null_rows=0,
                   caught_up=True, covered=True, endstate=True,
                   job_gone=False, chaos_died=False, oracle_dirty=False):
    for n in range(1, rungs + 1):
        ops = 51 if n == 1 else 147
        write_rung(d, n, branches=8 if n == 1 else 24, ops=ops,
                   subtasks=204 if n == 1 else 588,
                   expected_ops=(ops + 1) if (width_mismatch and n == rungs) else ops)
    if boundary:
        write_rung(d, rungs + 1, branches=48, ops=-1, subtasks=2328,
                   status="capacity", reason="slots 2400 < subtasks 2328 after margin")
    (d / "q6-verdict.json").write_text(json.dumps({
        "samples": 30, "findings": list(findings), "stuck": stuck,
        "last_stats": {"sum_n": sum_n}}))
    (d / "final-quiesce.txt").write_text(f"quiesced={'yes' if quiesced else 'no'}\n")
    if endstate:
        (d / "completeness.txt").write_text(
            f"produced_total={produced}\nsum_n={sum_n}\ndistinct_keys=500\n"
            f"expected_keys={checked}\nkeys_checked={checked}\n"
            f"keys_missing={missing}\nkeys_wrong_n={wrong_n}\n"
            f"keys_fabricated={fabricated}\nrows_with_null_n={null_rows}\n")
    (d / "catchup.txt").write_text(
        f"caught_up={'yes' if caught_up else 'no'}\nproduced_final={produced}\n")
    if job_gone:
        (d / "job-gone.txt").write_text("gone\n")
    if chaos_died:
        (d / "chaos-died.txt").write_text("died\n")
    if oracle_dirty:
        (d / "oracle-dirty.txt").write_text("dirty\n")
    lines = []
    events = MANDATORY if covered else MANDATORY[1:]
    for ev in events:
        entry = {"fault": ev}
        if ev == "coordinator_restart":
            entry["worker_pids_before"] = {"w1": 10}
            entry["worker_pids_after"] = {"w1": 10}
        lines.append(json.dumps(entry))
    (d / "q6-chaos.jsonl").write_text("\n".join(lines) + "\n")


def run(d):
    out = subprocess.run(
        [sys.executable, str(SUMMARISE), "--out-dir", str(d), "--run-id", "sumtest",
         "--profile", "aggressive"],
        capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    for line in out.stdout.splitlines():
        if line.startswith("**"):
            return line.strip("* ").replace("RESULT: ", ""), out.stdout
    return "(no result line)", out.stdout


failures = []


def check(name, got, want):
    if got.startswith(want):
        print(f"  PASS {name}")
    else:
        print(f"  FAIL {name}: result '{got}', wanted '{want}...'")
        failures.append(name)


CASES = [
    ("two green rungs, clean battery at the top is a PASS", {}, "PASS"),
    # The scale-campaign essence: hitting a limit is a boundary, not a red.
    ("a capacity boundary above the greens is still a PASS",
     {"boundary": True}, "PASS"),
    ("a deployed width disagreeing with the claim is a FAIL",
     {"width_mismatch": True}, "FAIL"),
    ("no rung green at all is INCONCLUSIVE", {"rungs": 0, "boundary": True}, "INCONCLUSIVE"),
    # Correctness discipline carried over from QUAL-05.
    ("a finding is a FAIL", {"findings": [{"kind": "shrinking"}]}, "FAIL"),
    ("inexact accounting is a FAIL", {"sum_n": 990}, "FAIL"),
    ("a fabricated key is a FAIL", {"fabricated": 1}, "FAIL"),
    ("a NULL count is a FAIL", {"null_rows": 1}, "FAIL"),
    ("behind is INCONCLUSIVE, not a correctness FAIL",
     {"caught_up": False, "sum_n": 800, "missing": 12}, "INCONCLUSIVE"),
    ("a fabricated key is a FAIL even when behind",
     {"caught_up": False, "sum_n": 800, "fabricated": 1}, "FAIL"),
    ("no end-state pass is INCONCLUSIVE", {"endstate": False}, "INCONCLUSIVE"),
    ("missing mandatory-fault coverage is INCONCLUSIVE", {"covered": False}, "INCONCLUSIVE"),
    ("unquiesced is INCONCLUSIVE", {"quiesced": False}, "INCONCLUSIVE"),
    ("a vanished job is a FAIL", {"job_gone": True}, "FAIL"),
    ("a dirty oracle is a FAIL", {"oracle_dirty": True}, "FAIL"),
    ("a dead chaos controller is INCONCLUSIVE", {"chaos_died": True}, "INCONCLUSIVE"),
]

for name, kwargs, want in CASES:
    with tempfile.TemporaryDirectory() as tmp:
        d = pathlib.Path(tmp)
        write_evidence(d, **kwargs)
        result, _ = run(d)
        check(name, result, want)

print(f"\n{len(CASES) - len(failures)} passed, {len(failures)} failed")
sys.exit(1 if failures else 0)
