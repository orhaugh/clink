#!/usr/bin/env python3
"""QUAL-03 summariser result logic against synthetic evidence directories.

The summary is the last honest thing a campaign says; it must never call
an unproven run a pass. The QUAL-02 shapes are pinned again (a
fully-evidenced PASS, FAIL on findings, INCONCLUSIVE on an unsettled
verdict), plus QUAL-03's own edges: duplicates and empty objects in the
end-state re-read are FAILs, a foreign tail is a FAIL, and pending
multipart uploads after cancel are recorded but never gate - they are
orphans of never-durable checkpoints, expired by lifecycle rules in
production, not the locked-and-blocking hazard a prepared transaction is.
"""
import json
import pathlib
import subprocess
import sys
import tempfile

HERE = pathlib.Path(__file__).resolve().parent
SUMMARISE = HERE.parent / "qual03" / "summarise.py"

# Filled from the summariser ITSELF so the fixture stays in lockstep by
# construction (the QUAL-02 lesson: a fixture pinned to the wrong source
# hid the mandatory-set divergence that made PASS unreachable).
sys.path.insert(0, str(HERE.parent / "qual03"))
import summarise as q3_summarise  # noqa: E402

MANDATORY = list(q3_summarise.MANDATORY_EVENTS)


def write_evidence(d, *, findings, stuck, quiesced, produced, committed,
                   dup_total=0, empty_objects=0, foreign_ahead=0,
                   pending_after=0, covered=True):
    (d / "q3-verdict.json").write_text(json.dumps({
        "samples": 12, "findings": findings, "stuck": stuck,
        "max_pending_uploads_seen": 4,
        "last_stats": {"lines_total": committed},
    }))
    (d / "final-quiesce.txt").write_text(f"quiesced={'yes' if quiesced else 'no'}\n")
    (d / "completeness.txt").write_text(
        f"produced_total={produced}\ncommitted_distinct={committed}\n"
        f"dup_total={dup_total}\nforeign_lines=0\n"
        f"foreign_ahead_partitions={foreign_ahead}\n"
        f"empty_objects={empty_objects}\nobjects_total=42\n")
    (d / "pending-after-cancel.txt").write_text(
        f"pending_uploads={pending_after}\n")
    (d / "verification.txt").write_text("sink_family=s3_2pc_string_sink\n")
    lines = []
    events = MANDATORY if covered else MANDATORY[1:]
    for ev in events:
        entry = {"fault": ev}
        if ev == "coordinator_restart" or ev.startswith("twopc_recovered:coordinator."):
            entry["worker_pids_before"] = {"w1": 10}
            entry["worker_pids_after"] = {"w1": 10}
        lines.append(json.dumps(entry))
    (d / "q3-chaos.jsonl").write_text("\n".join(lines) + "\n")


def run(d):
    out = subprocess.run(
        [sys.executable, str(SUMMARISE), "--out-dir", str(d),
         "--run-id", "sumtest", "--duration-h", "2", "--profile", "steady"],
        capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    for line in out.stdout.splitlines():
        if line.startswith("**"):
            return line.strip("* "), out.stdout
    return "(no result line)", out.stdout


failures = []


def check(name, got, want_prefix):
    if got.startswith(want_prefix):
        print(f"  PASS {name}")
    else:
        print(f"  FAIL {name}: result '{got}', wanted '{want_prefix}...'")
        failures.append(name)


with tempfile.TemporaryDirectory() as tmp:
    d = pathlib.Path(tmp)
    write_evidence(d, findings=[], stuck=False, quiesced=True,
                   produced=1000, committed=1000)
    result, _ = run(d)
    check("fully-evidenced clean run is a PASS", result, "PASS")

with tempfile.TemporaryDirectory() as tmp:
    d = pathlib.Path(tmp)
    write_evidence(d, findings=[{"kind": "duplicate", "partition": 0, "seq": 7}],
                   stuck=False, quiesced=True, produced=1000, committed=1000)
    result, _ = run(d)
    check("a finding is a FAIL", result, "FAIL")

with tempfile.TemporaryDirectory() as tmp:
    d = pathlib.Path(tmp)
    write_evidence(d, findings=[], stuck=False, quiesced=False,
                   produced=1000, committed=1000)
    result, _ = run(d)
    check("clean but unquiesced is INCONCLUSIVE, never a weak pass",
          result, "INCONCLUSIVE")

with tempfile.TemporaryDirectory() as tmp:
    d = pathlib.Path(tmp)
    write_evidence(d, findings=[], stuck=False, quiesced=True,
                   produced=1000, committed=990)
    result, _ = run(d)
    check("a lost tail (incompleteness) is a FAIL", result, "FAIL")

with tempfile.TemporaryDirectory() as tmp:
    d = pathlib.Path(tmp)
    write_evidence(d, findings=[], stuck=False, quiesced=True,
                   produced=1000, committed=1000, dup_total=3)
    result, _ = run(d)
    check("duplicates in the end-state re-read are a FAIL", result, "FAIL")

with tempfile.TemporaryDirectory() as tmp:
    d = pathlib.Path(tmp)
    write_evidence(d, findings=[], stuck=False, quiesced=True,
                   produced=1000, committed=1000, empty_objects=1)
    result, _ = run(d)
    check("an empty object in the end state is a FAIL", result, "FAIL")

with tempfile.TemporaryDirectory() as tmp:
    d = pathlib.Path(tmp)
    write_evidence(d, findings=[], stuck=False, quiesced=True,
                   produced=1000, committed=1000, foreign_ahead=1)
    result, _ = run(d)
    check("a partition committed past the generator is a FAIL",
          result, "FAIL")

with tempfile.TemporaryDirectory() as tmp:
    d = pathlib.Path(tmp)
    write_evidence(d, findings=[], stuck=False, quiesced=True,
                   produced=1000, committed=1000, pending_after=3)
    result, _ = run(d)
    check("pending uploads after cancel are recorded, never a gate",
          result, "PASS")

with tempfile.TemporaryDirectory() as tmp:
    d = pathlib.Path(tmp)
    write_evidence(d, findings=[], stuck=False, quiesced=True,
                   produced=1000, committed=1000, covered=False)
    result, _ = run(d)
    check("missing mandatory-fault coverage is INCONCLUSIVE",
          result, "INCONCLUSIVE")

print(f"\n{9 - len(failures)} passed, {len(failures)} failed")
sys.exit(1 if failures else 0)
