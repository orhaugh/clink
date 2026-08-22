#!/usr/bin/env python3
"""QUAL-04 summariser result logic against synthetic evidence directories.

The shapes that matter for a SIZE campaign, on top of the usual ones: a
run that never reached its state target must be INCONCLUSIVE however
clean its counters are (otherwise the campaign quietly qualifies a small
job), inexact accounting is a FAIL, and a sampled key disagreeing with
the seed is a FAIL.
"""
import json
import pathlib
import subprocess
import sys
import tempfile

HERE = pathlib.Path(__file__).resolve().parent
SUMMARISE = HERE.parent / "qual04" / "summarise.py"

sys.path.insert(0, str(HERE.parent / "qual04"))
import summarise as q4_summarise  # noqa: E402

MANDATORY = list(q4_summarise.MANDATORY_EVENTS)


def write_evidence(d, *, findings=(), stuck=False, quiesced=True,
                   produced=1000, sum_n=1000, wrong_len=0, checked=2000,
                   missing=0, wrong_n=0, wrong_blob=0, state_gib=2.0,
                   covered=True):
    (d / "q4-verdict.json").write_text(json.dumps({
        "samples": 12, "findings": list(findings), "stuck": stuck,
        "last_stats": {"sum_n": sum_n},
    }))
    (d / "final-quiesce.txt").write_text(f"quiesced={'yes' if quiesced else 'no'}\n")
    (d / "completeness.txt").write_text(
        f"produced_total={produced}\nsum_n={sum_n}\ndistinct_keys=500\n"
        f"wrong_blob_len_rows={wrong_len}\nsampled_keys_checked={checked}\n"
        f"sampled_keys_missing={missing}\nsampled_keys_wrong_count={wrong_n}\n"
        f"sampled_keys_wrong_blob_len={wrong_blob}\n")
    (d / "state-size-final.txt").write_text(
        f"state_bytes={int(state_gib * 1024 ** 3)}\nstate_objects=100\n"
        f"state_largest_object_bytes=20480\nstate_gib={state_gib:.3f}\n")
    (d / "verification.txt").write_text(
        "blob_bytes=20480\nstate_backend=remote-read://b/state\n")
    lines = []
    events = MANDATORY if covered else MANDATORY[1:]
    for ev in events:
        entry = {"fault": ev}
        if ev == "coordinator_restart" or ev.startswith("twopc_recovered:coordinator."):
            entry["worker_pids_before"] = {"w1": 10}
            entry["worker_pids_after"] = {"w1": 10}
        lines.append(json.dumps(entry))
    (d / "q4-chaos.jsonl").write_text("\n".join(lines) + "\n")


def run(d, target=1.0):
    out = subprocess.run(
        [sys.executable, str(SUMMARISE), "--out-dir", str(d), "--run-id", "sumtest",
         "--duration-h", "2", "--profile", "steady",
         "--state-target-gib", str(target)],
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


CASES = [
    ("fully-evidenced clean run at target is a PASS", {}, 1.0, "PASS"),
    ("a finding is a FAIL", {"findings": [{"kind": "short_blob"}]}, 1.0, "FAIL"),
    ("clean but unquiesced is INCONCLUSIVE", {"quiesced": False}, 1.0, "INCONCLUSIVE"),
    ("inexact accounting is a FAIL", {"sum_n": 990}, 1.0, "FAIL"),
    ("a truncated accumulator is a FAIL", {"wrong_len": 3}, 1.0, "FAIL"),
    ("a sampled key missing from state is a FAIL", {"missing": 1}, 1.0, "FAIL"),
    ("a sampled key with the wrong count is a FAIL", {"wrong_n": 1}, 1.0, "FAIL"),
    ("missing mandatory-fault coverage is INCONCLUSIVE", {"covered": False}, 1.0, "INCONCLUSIVE"),
    ("a clean run BELOW its state target is INCONCLUSIVE, never a PASS",
     {"state_gib": 0.4}, 10.0, "INCONCLUSIVE"),
    ("a run whose sample checked nothing is INCONCLUSIVE",
     {"checked": 0}, 1.0, "INCONCLUSIVE"),
]

for name, kwargs, target, want in CASES:
    with tempfile.TemporaryDirectory() as tmp:
        d = pathlib.Path(tmp)
        write_evidence(d, **kwargs)
        result, _ = run(d, target)
        check(name, result, want)

print(f"\n{len(CASES) - len(failures)} passed, {len(failures)} failed")
sys.exit(1 if failures else 0)
