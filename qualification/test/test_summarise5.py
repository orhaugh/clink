#!/usr/bin/env python3
"""QUAL-05 summariser result logic against synthetic evidence directories.

The shapes that matter for a RETENTION campaign, on top of the usual ones.
Two of them are the whole reason this campaign can fail at all:

  * a state curve that is still climbing must not be a PASS, however clean
    the counters are;
  * a plateau whose CONTROL arm did not grow must not be a PASS either. The
    control is what distinguishes "retention bounded this workload" from
    "this workload never accumulated anything", and without it a campaign
    measuring a flat line is measuring nothing.
"""
import json
import pathlib
import subprocess
import sys
import tempfile

HERE = pathlib.Path(__file__).resolve().parent
SUMMARISE = HERE.parent / "qual05" / "summarise.py"

sys.path.insert(0, str(HERE.parent / "qual05"))
import summarise as q5_summarise  # noqa: E402

MANDATORY = list(q5_summarise.MANDATORY_EVENTS)
MIB = 1024 * 1024


def flat_series(n=20, mean_mib=200.0, drift_frac=0.0, spike_at=None, spike_mult=1.0):
    """Steady-state samples: `drift_frac` of the mean across the window."""
    rows = []
    for i in range(n):
        frac = i / max(1, n - 1)
        y = mean_mib * (1.0 + drift_frac * (frac - 0.5))
        if spike_at is not None and i == spike_at:
            y *= spike_mult
        rows.append(f"{i * 120},{int(y * MIB)}")
    return "\n".join(rows) + "\n"


def write_evidence(d, *, findings=(), stuck=False, quiesced=True,
                   produced=1000, sum_n=1000, checked=500,
                   missing=0, wrong_n=0, fabricated=0, null_rows=0,
                   caught_up=True, covered=True,
                   series=None, control_first_mib=50.0, control_last_mib=400.0,
                   control_window_s=1200, expired_total=12345,
                   job_gone=False, chaos_died=False, oracle_dirty=False,
                   endstate=True):
    (d / "q5-verdict.json").write_text(json.dumps({
        "samples": 30, "findings": list(findings), "stuck": stuck,
        "last_stats": {"sum_n": sum_n},
    }))
    (d / "final-quiesce.txt").write_text(f"quiesced={'yes' if quiesced else 'no'}\n")
    if endstate:
        (d / "completeness.txt").write_text(
            f"produced_total={produced}\nsum_n={sum_n}\ndistinct_keys=500\n"
            f"expected_keys={checked}\nkeys_checked={checked}\n"
            f"keys_missing={missing}\nkeys_wrong_n={wrong_n}\n"
            f"keys_fabricated={fabricated}\nrows_with_null_n={null_rows}\n")
    (d / "catchup.txt").write_text(
        f"caught_up={'yes' if caught_up else 'no'}\nproduced_final={produced}\n"
        f"folded_at_catchup={sum_n}\ncatchup_seconds=60\n")
    (d / "verification.txt").write_text("state_ttl=10m\nkey_epoch_ms=60000\n")
    (d / "state-series-steady.csv").write_text(
        series if series is not None else flat_series())
    (d / "control.txt").write_text(
        f"control_first_bytes={int(control_first_mib * MIB)}\n"
        f"control_last_bytes={int(control_last_mib * MIB)}\n"
        f"control_window_s={control_window_s}\n")
    (d / "retention.txt").write_text(
        f"retention_expired_total={expired_total}\nretention_tracked_keys=55000\n")
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
    (d / "q5-chaos.jsonl").write_text("\n".join(lines) + "\n")


def run(d):
    out = subprocess.run(
        [sys.executable, str(SUMMARISE), "--out-dir", str(d), "--run-id", "sumtest",
         "--duration-h", "2", "--profile", "steady"],
        capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    for line in out.stdout.splitlines():
        if line.startswith("**"):
            return line.strip("* ").replace("RESULT: ", ""), out.stdout
    return "(no result line)", out.stdout


failures = []


def check(name, got, want_prefix):
    if got.startswith(want_prefix):
        print(f"  PASS {name}")
    else:
        print(f"  FAIL {name}: result '{got}', wanted '{want_prefix}...'")
        failures.append(name)


CASES = [
    ("fully-evidenced flat run with a grown control is a PASS", {}, "PASS"),

    # --- the plateau, which is the campaign's subject --------------------
    ("state still climbing steeply is INCONCLUSIVE, never a PASS",
     {"series": flat_series(drift_frac=1.4)}, "INCONCLUSIVE"),
    ("state climbing just past the band is INCONCLUSIVE",
     {"series": flat_series(drift_frac=0.40)}, "INCONCLUSIVE"),
    ("a little drift inside the band is still a PASS",
     {"series": flat_series(drift_frac=0.10)}, "PASS"),
    ("state that ends flat but spiked mid-window is INCONCLUSIVE",
     {"series": flat_series(spike_at=10, spike_mult=3.0)}, "INCONCLUSIVE"),
    ("too few steady-state samples to fit a trend is INCONCLUSIVE",
     {"series": flat_series(n=4)}, "INCONCLUSIVE"),
    ("no steady-state samples at all is INCONCLUSIVE",
     {"series": ""}, "INCONCLUSIVE"),

    # --- the control arm, which is what makes the plateau mean anything ---
    ("a flat run whose CONTROL did not grow is INCONCLUSIVE, never a PASS",
     {"control_first_mib": 200.0, "control_last_mib": 210.0}, "INCONCLUSIVE"),
    ("a control arm that was never measured is INCONCLUSIVE",
     {"control_first_mib": 0.0, "control_last_mib": 0.0}, "INCONCLUSIVE"),

    # --- retention has to have actually done something --------------------
    ("a flat run where retention released NOTHING is INCONCLUSIVE",
     {"expired_total": 0}, "INCONCLUSIVE"),

    # --- correctness ------------------------------------------------------
    ("a finding is a FAIL", {"findings": [{"kind": "shrinking"}]}, "FAIL"),
    ("a stuck oracle is a FAIL", {"stuck": True}, "FAIL"),
    ("inexact accounting is a FAIL", {"sum_n": 990}, "FAIL"),
    ("a key with the wrong count is a FAIL", {"wrong_n": 1}, "FAIL"),
    ("a fabricated key is a FAIL", {"fabricated": 1}, "FAIL"),
    ("a NULL count is a FAIL", {"null_rows": 1}, "FAIL"),
    ("a pipeline that never caught up is INCONCLUSIVE, not a correctness FAIL",
     {"caught_up": False, "sum_n": 800, "missing": 40, "wrong_n": 12}, "INCONCLUSIVE"),
    ("a fabricated key is a FAIL even when behind",
     {"caught_up": False, "sum_n": 800, "fabricated": 1}, "FAIL"),
    ("a shortfall while fully caught up is still a FAIL",
     {"caught_up": True, "sum_n": 800}, "FAIL"),
    ("a run with no end-state pass at all is INCONCLUSIVE",
     {"endstate": False}, "INCONCLUSIVE"),
    ("a run whose oracle judged nothing is INCONCLUSIVE", {"checked": 0}, "INCONCLUSIVE"),

    # --- infrastructure ---------------------------------------------------
    ("missing mandatory-fault coverage is INCONCLUSIVE", {"covered": False}, "INCONCLUSIVE"),
    ("clean but unquiesced is INCONCLUSIVE", {"quiesced": False}, "INCONCLUSIVE"),
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
