#!/usr/bin/env python3
"""QUAL-11 summariser result logic against synthetic evidence directories.

The shapes that matter for a SCHEMA-EVOLUTION campaign:

  * a refused pre-deploy check is INCONCLUSIVE (the campaign does not
    deploy past a refusal), but a check that ACCEPTS the deliberately
    broken job is a FAIL - the instrument is inert and the good run's
    pass proves nothing;
  * state loss wearing a healthy costume is fatal: a key whose count
    restarts at 1 after the boundary, or a migrated field that does not
    match the migration's predicted output;
  * no keys across the boundary is no evidence, never a pass;
  * everything the earlier campaigns established carries over: one
    logical job, exactness only when caught up, invented keys fatal.
"""
import json
import pathlib
import subprocess
import sys
import tempfile

HERE = pathlib.Path(__file__).resolve().parent
SUMMARISE = HERE.parent / "qual11" / "summarise.py"


def write_evidence(d, *, check_v2="pass", check_broken="refused", savepoint=True,
                   restore=True, same_job=True, caught_up=True, quiesced=True,
                   keys=500, missing=0, wrong_n=0, wrong_sum=0, fabricated=0,
                   conflicting=0, malformed=0, rows=5000,
                   across=400, carried=None, reset=0, effect_ok=None, effect_bad=0,
                   verify=True, job_gone=False, faults=("worker_sigkill", "coordinator_restart")):
    carried = across - reset if carried is None else carried
    effect_ok = across - effect_bad if effect_ok is None else effect_ok
    (d / "boundary.txt").write_text(
        f"savepoint_ok={'yes' if savepoint else 'no'}\nsavepoint_id=42\nsavepoint_s=2\n"
        f"check_v2={check_v2}\ncheck_v2_broken={check_broken}\n"
        f"restore_ok={'yes' if restore else 'no'}\nrestore_s=3\n"
        f"same_job_id={'yes' if same_job else 'no'}\n")
    (d / "catchup.txt").write_text(f"caught_up={'yes' if caught_up else 'no'}\n")
    (d / "final-quiesce.txt").write_text(f"quiesced={'yes' if quiesced else 'no'}\n")
    if verify:
        (d / "q11-verify.json").write_text(json.dumps({
            "topic": "qual11-out", "rows": rows, "malformed_rows": malformed,
            "conflicting_rows": conflicting, "keys_expected": keys, "keys_seen": keys,
            "keys_missing": missing, "keys_wrong_n": wrong_n, "keys_wrong_sum": wrong_sum,
            "keys_fabricated": fabricated, "keys_across_boundary": across,
            "keys_carried": carried, "keys_reset": reset,
            "migration_effect_ok": effect_ok, "migration_effect_bad": effect_bad,
            "samples": []}))
    if job_gone:
        (d / "job-gone.txt").write_text("gone\n")
    (d / "q11-chaos.jsonl").write_text(
        "\n".join(json.dumps({"fault": f}) for f in faults) + ("\n" if faults else ""))


def run(d, local=False):
    cmd = [sys.executable, str(SUMMARISE), "--out-dir", str(d), "--run-id", "t"]
    if local:
        cmd.append("--local")
    out = subprocess.run(cmd, capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    for line in out.stdout.splitlines():
        if line.startswith("**RESULT"):
            return line.strip("* ").replace("RESULT: ", ""), out.stdout
    return "(no result line)", out.stdout


failures = []


def check(name, got, want):
    if got == want:
        print(f"  PASS {name}")
    else:
        print(f"  FAIL {name}: result '{got}', wanted '{want}'")
        failures.append(name)


CASES = [
    # The campaign's essence.
    ("a clean evolution boundary is a PASS", {}, "PASS"),

    # Gate 1 + 2: the check and its negative control.
    ("a refused pre-deploy check is INCONCLUSIVE, not a FAIL",
     {"check_v2": "refused"}, "INCONCLUSIVE"),
    ("a check that ACCEPTS the broken job is a FAIL (the gate is inert)",
     {"check_broken": "accepted"}, "FAIL"),
    ("a missing negative control is INCONCLUSIVE",
     {"check_broken": "missing"}, "INCONCLUSIVE"),
    ("an inert gate is a FAIL even when everything else is perfect",
     {"check_broken": "accepted", "across": 400, "reset": 0}, "FAIL"),

    # Gate 3: continuity and exactness.
    ("a second job id (a fresh start, not a restore) is INCONCLUSIVE",
     {"same_job": False}, "INCONCLUSIVE"),
    ("a failed savepoint is INCONCLUSIVE", {"savepoint": False}, "INCONCLUSIVE"),
    ("a failed restore is INCONCLUSIVE", {"restore": False}, "INCONCLUSIVE"),
    ("a missing key while caught up is a FAIL", {"missing": 3}, "FAIL"),
    ("a wrong count is a FAIL", {"wrong_n": 1}, "FAIL"),
    ("a wrong sum is a FAIL", {"wrong_sum": 1}, "FAIL"),
    ("an invented key is a FAIL", {"fabricated": 1}, "FAIL"),
    ("conflicting duplicates are a FAIL", {"conflicting": 1}, "FAIL"),
    ("a malformed row is a FAIL", {"malformed": 1}, "FAIL"),
    ("behind is INCONCLUSIVE, not a correctness FAIL",
     {"caught_up": False, "missing": 12}, "INCONCLUSIVE"),
    ("no verifier output is INCONCLUSIVE", {"verify": False}, "INCONCLUSIVE"),
    ("unquiesced is INCONCLUSIVE", {"quiesced": False}, "INCONCLUSIVE"),
    ("a vanished job is a FAIL", {"job_gone": True}, "FAIL"),

    # Gate 4: the migration's effect.
    ("a key whose count RESET across the boundary is a FAIL (state loss)",
     {"reset": 1}, "FAIL"),
    ("a migrated field not matching the prediction is a FAIL",
     {"effect_bad": 1}, "FAIL"),
    ("no keys across the boundary is INCONCLUSIVE (no evidence)",
     {"across": 0, "carried": 0, "effect_ok": 0}, "INCONCLUSIVE"),

    # Coverage.
    ("a missing mandatory fault is INCONCLUSIVE",
     {"faults": ("worker_sigkill",)}, "INCONCLUSIVE"),

    # Precedence.
    ("state loss dominates a coverage gap (FAIL beats INCONCLUSIVE)",
     {"reset": 1, "faults": ()}, "FAIL"),
]

for name, kwargs, want in CASES:
    with tempfile.TemporaryDirectory() as tmp:
        d = pathlib.Path(tmp)
        write_evidence(d, **kwargs)
        result, _ = run(d)
        check(name, result, want)

print(f"\n{len(CASES) - len(failures)} passed, {len(failures)} failed")
sys.exit(1 if failures else 0)
