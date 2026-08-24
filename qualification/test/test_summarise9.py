#!/usr/bin/env python3
"""QUAL-09 summariser result logic against synthetic evidence directories.

The shapes that matter for an INFRASTRUCTURE campaign:

  * coverage credit for an infra fault requires ENGAGEMENT evidence - a
    fired-but-unengaged disk_pressure (no checkpoint even tried to fail)
    is INCONCLUSIVE, never PASS;
  * the environment split is loud: under --local, a skippable fault may
    be absent only WITH the controller's skip record - a silent absence
    is still a gap - and a cloud run (no --local) requires everything;
  * a failed revert is a FAIL: the rig is dirty and nothing after it is
    trustworthy; a drained revert is reported but tolerated (the drain
    exists for exactly that);
  * everything the retention campaign established about correctness
    carries over: exactness only when caught up, fabricated keys always
    fatal.
"""
import json
import pathlib
import subprocess
import sys
import tempfile

HERE = pathlib.Path(__file__).resolve().parent
SUMMARISE = HERE.parent / "qual09" / "summarise.py"

sys.path.insert(0, str(HERE.parent / "qual09"))
import summarise as q9  # noqa: E402


def write_evidence(d, *, local=False, findings=(), stuck=False, quiesced=True,
                   produced=1000, sum_n=1000, checked=500,
                   missing=0, wrong_n=0, fabricated=0, null_rows=0,
                   caught_up=True, endstate=True,
                   job_gone=False, chaos_died=False, oracle_dirty=False,
                   infra=("disk_pressure", "partition_sustained", "clock_step"),
                   unengaged=(), skips=(), revert_failed=(), revert_drained=()):
    (d / "q9-verdict.json").write_text(json.dumps({
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
    for ev in q9.MANDATORY_CORE:
        entry = {"fault": ev}
        if ev == "coordinator_restart":
            entry["worker_pids_before"] = {"w1": 10}
            entry["worker_pids_after"] = {"w1": 10}
        lines.append(json.dumps(entry))
    for fault in infra:
        lines.append(json.dumps({"fault": fault}))
        lines.append(json.dumps({
            "fault": q9.INFRA_ENGAGEMENT[fault],
            "engaged": fault not in unengaged}))
    for fault in skips:
        lines.append(json.dumps({"fault": "fault_skipped", "skipped": fault}))
    for label in revert_failed:
        lines.append(json.dumps({"fault": "revert_failed", "reverted": label}))
    for label in revert_drained:
        lines.append(json.dumps({"fault": "revert_drained", "reverted": label}))
    (d / "q9-chaos.jsonl").write_text("\n".join(lines) + "\n")
    return local


def run(d, local):
    cmd = [sys.executable, str(SUMMARISE), "--out-dir", str(d), "--run-id", "sumtest",
           "--profile", "infra"]
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
    if got.startswith(want):
        print(f"  PASS {name}")
    else:
        print(f"  FAIL {name}: result '{got}', wanted '{want}...'")
        failures.append(name)


CASES = [
    # The campaign's essence.
    ("cloud run, all infra engaged, clean oracle is a PASS", {}, "PASS"),
    ("a fired-but-unengaged disk_pressure is INCONCLUSIVE",
     {"unengaged": ("disk_pressure",)}, "INCONCLUSIVE"),
    ("a fired-but-unengaged partition is INCONCLUSIVE",
     {"unengaged": ("partition_sustained",)}, "INCONCLUSIVE"),
    ("a never-fired clock_step on a CLOUD run is INCONCLUSIVE",
     {"infra": ("disk_pressure", "partition_sustained")}, "INCONCLUSIVE"),
    # The environment split.
    ("local run with recorded skips is a PASS",
     {"local": True, "infra": ("partition_sustained",),
      "skips": ("disk_pressure", "clock_step")}, "PASS"),
    ("local run with a SILENT absence is INCONCLUSIVE",
     {"local": True, "infra": ("partition_sustained",),
      "skips": ("disk_pressure",)}, "INCONCLUSIVE"),
    ("skip records do NOT excuse a cloud run",
     {"infra": ("partition_sustained",),
      "skips": ("disk_pressure", "clock_step")}, "INCONCLUSIVE"),
    # Revert hygiene.
    ("a failed revert is a FAIL", {"revert_failed": ("clock_step",)}, "FAIL"),
    ("a drained revert is tolerated", {"revert_drained": ("disk_pressure",)}, "PASS"),
    # Correctness discipline carried over.
    ("a finding is a FAIL", {"findings": [{"kind": "shrinking"}]}, "FAIL"),
    ("inexact accounting is a FAIL", {"sum_n": 990}, "FAIL"),
    ("a fabricated key is a FAIL", {"fabricated": 1}, "FAIL"),
    ("a NULL count is a FAIL", {"null_rows": 1}, "FAIL"),
    ("behind is INCONCLUSIVE, not a correctness FAIL",
     {"caught_up": False, "sum_n": 800, "missing": 12}, "INCONCLUSIVE"),
    ("no end-state pass is INCONCLUSIVE", {"endstate": False}, "INCONCLUSIVE"),
    ("unquiesced is INCONCLUSIVE", {"quiesced": False}, "INCONCLUSIVE"),
    ("a vanished job is a FAIL", {"job_gone": True}, "FAIL"),
    ("a dirty oracle is a FAIL", {"oracle_dirty": True}, "FAIL"),
    ("a dead chaos controller is INCONCLUSIVE", {"chaos_died": True}, "INCONCLUSIVE"),
]

for name, kwargs, want in CASES:
    with tempfile.TemporaryDirectory() as tmp:
        d = pathlib.Path(tmp)
        local = kwargs.pop("local", False)
        write_evidence(d, **kwargs)
        result, _ = run(d, local)
        check(name, result, want)

print(f"\n{len(CASES) - len(failures)} passed, {len(failures)} failed")
sys.exit(1 if failures else 0)
