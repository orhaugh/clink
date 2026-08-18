#!/usr/bin/env python3
"""QUAL-02 summariser result logic against synthetic evidence directories.

The summary is the last honest thing a campaign says; it must never call
an unproven run a pass. Three shapes are pinned: a fully-evidenced PASS,
a FAIL on findings, and the qual01-20260818c lesson - clean counters
whose verdict never settled must summarise INCONCLUSIVE, not PASS.
"""
import json
import pathlib
import subprocess
import sys
import tempfile

HERE = pathlib.Path(__file__).resolve().parent
SUMMARISE = HERE.parent / "qual02" / "summarise.py"

MANDATORY = None  # filled from chaos.py so the fixture stays in lockstep
sys.path.insert(0, str(HERE.parent / "chaos"))
from chaos import Chaos  # noqa: E402

MANDATORY = [
    "worker_sigkill", "coordinator_restart", "broker_restart",
    "network_latency", "partition_from_coordinator",
] + [f"twopc_fired:{p}" for p in Chaos.TWOPC_POINTS]


def write_evidence(d, *, findings, stuck, quiesced, produced, committed,
                   orphans="", covered=True):
    (d / "q2-verdict.json").write_text(json.dumps({
        "samples": 12, "findings": findings, "stuck": stuck,
        "max_prepared_xacts_seen": 4,
        "last_stats": {"rows_total": committed},
    }))
    (d / "final-quiesce.txt").write_text(f"quiesced={'yes' if quiesced else 'no'}\n")
    (d / "completeness.txt").write_text(
        f"produced_total={produced}\ncommitted_distinct={committed}\n")
    (d / "prepared-after-cancel.txt").write_text(orphans + "\n")
    (d / "verification.txt").write_text("gid_sample=clink_1_2_3\n")
    lines = []
    events = MANDATORY if covered else MANDATORY[1:]
    for ev in events:
        entry = {"fault": ev}
        if ev == "coordinator_restart" or ev.startswith("twopc_recovered:coordinator."):
            entry["worker_pids_before"] = {"w1": 10}
            entry["worker_pids_after"] = {"w1": 10}
        lines.append(json.dumps(entry))
    (d / "q2-chaos.jsonl").write_text("\n".join(lines) + "\n")


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
    write_evidence(d, findings=[{"kind": "duplicate", "partition": 0, "excess": 3}],
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
                   produced=1000, committed=1000, orphans="clink_1_9_9")
    result, _ = run(d)
    check("an orphaned prepared transaction after cancel is a FAIL",
          result, "FAIL")

with tempfile.TemporaryDirectory() as tmp:
    d = pathlib.Path(tmp)
    write_evidence(d, findings=[], stuck=False, quiesced=True,
                   produced=1000, committed=1000, covered=False)
    result, _ = run(d)
    check("missing mandatory-fault coverage is INCONCLUSIVE",
          result, "INCONCLUSIVE")

print(f"\n{6 - len(failures)} passed, {len(failures)} failed")
sys.exit(1 if failures else 0)
