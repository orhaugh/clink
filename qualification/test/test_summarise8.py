#!/usr/bin/env python3
"""QUAL-08 summariser result logic against synthetic evidence directories.

The shapes that matter for an UPGRADE campaign:

  * every failure of the upgrade sequence itself is a FAIL, because the
    sequence IS the campaign's subject: renumbered operator ids, a refused
    check-savepoint, a restore that started clean - none of these may hide
    behind INCONCLUSIVE;
  * missing boundary timings on an otherwise-clean run is INCONCLUSIVE -
    the upgrade may have worked but the campaign cannot say what it cost;
  * a same-image run computes its verdict identically but is labelled a
    smoke, and the label must appear in the summary text;
  * everything the retention campaign established about judging
    correctness carries over: exactness only when caught up, fabricated
    keys always fatal, coverage gaps INCONCLUSIVE.
"""
import json
import pathlib
import subprocess
import sys
import tempfile

HERE = pathlib.Path(__file__).resolve().parent
SUMMARISE = HERE.parent / "qual08" / "summarise.py"

sys.path.insert(0, str(HERE.parent / "qual08"))
import summarise as q8_summarise  # noqa: E402

MANDATORY = list(q8_summarise.MANDATORY_EVENTS)


def write_upgrade(d, *, present=True, opid_match=True, savepoint_ok=True,
                  checksave="ok", restore_ok=True, restore_carried=True,
                  savepoint_s=12, restore_s=25, downtime_s=48,
                  same_image=False, digests_same=False):
    if not present:
        return
    (d / "upgrade.txt").write_text(
        f"opid_match={'yes' if opid_match else 'no'}\n"
        f"savepoint_ok={'yes' if savepoint_ok else 'no'}\n"
        f"savepoint_id=41\nsavepoint_s={savepoint_s}\nsavepoint_bytes=70000000\n"
        f"checksave={checksave}\n"
        f"restore_ok={'yes' if restore_ok else 'no'}\n"
        f"restore_s={restore_s}\ndowntime_s={downtime_s}\n"
        f"v1_first_ckpt_bytes={'60000000' if restore_carried else '120000'}\n"
        f"restore_carried={'yes' if restore_carried else 'no'}\n"
        f"same_image={'yes' if same_image else 'no'}\n"
        f"image_v0=ghcr.io/x/clink:sha-aaa\nimage_v1=ghcr.io/x/clink:sha-bbb\n"
        f"digest_v0=sha256:aaa\ndigest_v1={'sha256:aaa' if digests_same else 'sha256:bbb'}\n")


def write_evidence(d, *, upgrade_kwargs=None,
                   findings=(), stuck=False, quiesced=True,
                   produced=1000, sum_n=1000, checked=500,
                   missing=0, wrong_n=0, fabricated=0, null_rows=0,
                   caught_up=True, covered=True, endstate=True,
                   job_gone=False, chaos_died=False, oracle_dirty=False):
    write_upgrade(d, **(upgrade_kwargs or {}))
    (d / "q8-verdict.json").write_text(json.dumps({
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
    (d / "q8-chaos.jsonl").write_text("\n".join(lines) + "\n")


def run(d):
    out = subprocess.run(
        [sys.executable, str(SUMMARISE), "--out-dir", str(d), "--run-id", "sumtest",
         "--profile", "aggressive"],
        capture_output=True, text=True)
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
    ("clean pair upgrade with a clean battery is a PASS", {}, "PASS"),
    # The upgrade-sequence failures - each one is the campaign's subject.
    ("renumbered operator ids are a FAIL",
     {"upgrade_kwargs": {"opid_match": False}}, "FAIL"),
    ("a failed savepoint is a FAIL",
     {"upgrade_kwargs": {"savepoint_ok": False}}, "FAIL"),
    ("a refused check-savepoint is a FAIL",
     {"upgrade_kwargs": {"checksave": "refused"}}, "FAIL"),
    ("a restore that never submitted is a FAIL",
     {"upgrade_kwargs": {"restore_ok": False}}, "FAIL"),
    ("a restore that started clean is a FAIL",
     {"upgrade_kwargs": {"restore_carried": False}}, "FAIL"),
    ("no upgrade evidence at all is a FAIL",
     {"upgrade_kwargs": {"present": False}}, "FAIL"),
    ("two tags of one digest is a FAIL - nothing was upgraded",
     {"upgrade_kwargs": {"digests_same": True}}, "FAIL"),
    ("missing boundary timings are INCONCLUSIVE",
     {"upgrade_kwargs": {"downtime_s": -1}}, "INCONCLUSIVE"),
    # Correctness discipline carried over from QUAL-05.
    ("a finding is a FAIL", {"findings": [{"kind": "shrinking"}]}, "FAIL"),
    ("inexact accounting is a FAIL", {"sum_n": 990}, "FAIL"),
    ("a fabricated key is a FAIL", {"fabricated": 1}, "FAIL"),
    ("a NULL count is a FAIL", {"null_rows": 1}, "FAIL"),
    ("behind is INCONCLUSIVE, not a correctness FAIL",
     {"caught_up": False, "sum_n": 800, "missing": 12}, "INCONCLUSIVE"),
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

# The smoke label: a same-image run must be marked as machinery evidence.
with tempfile.TemporaryDirectory() as tmp:
    d = pathlib.Path(tmp)
    write_evidence(d, upgrade_kwargs={"same_image": True, "digests_same": True})
    result, text = run(d)
    if result.startswith("PASS") and "SINGLE-IMAGE SMOKE" in text:
        print("  PASS a same-image run passes as a labelled smoke")
    else:
        print(f"  FAIL a same-image run passes as a labelled smoke: result '{result}',"
              f" label {'present' if 'SINGLE-IMAGE SMOKE' in text else 'MISSING'}")
        failures.append("same-image smoke label")

total = len(CASES) + 1
print(f"\n{total - len(failures)} passed, {len(failures)} failed")
sys.exit(1 if failures else 0)
