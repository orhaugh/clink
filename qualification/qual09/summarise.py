#!/usr/bin/env python3
"""QUAL-09 result logic: turn a run's retained evidence into a verdict.

The campaign claims exactly-once output under INFRASTRUCTURE faults, so
the verdict logic differs from its predecessors in one structural way:
coverage credit for an infra fault requires its ENGAGEMENT evidence, not
just its record. A disk-pressure window in which no checkpoint even
tried to fail, a "sustained" partition the watchdog never noticed, or a
clock step nobody measured each read as fired-but-proved-nothing, and
the verdict is INCONCLUSIVE, never PASS.

The environment split is explicit: --local names the harness-gate mode,
where disk_pressure and clock_step may be absent ONLY when the
controller recorded skipping them (a silent absence is still a gap).
A publishable claim comes exclusively from a cloud run - no --local,
full mandatory coverage, every engagement gate green.

Everything the retention campaign established about judging correctness
carries over: exactness only when caught up, fabricated keys always
fatal, coverage gaps INCONCLUSIVE.
"""
import argparse
import json
import os
import sys

TWOPC_POINTS = (
    "coordinator.before_completed_marker",
    "coordinator.after_completed_marker",
)

# The infra faults and the record that carries each one's engagement
# evidence. clock_step's engagement rides its own record; the other two
# put it on their release/heal records.
INFRA_ENGAGEMENT = {
    "disk_pressure": "disk_pressure_released",
    "partition_sustained": "partition_sustained_healed",
    "clock_step": "clock_step",
}

# Faults the LOCAL environment may skip - with a record, never silently.
LOCAL_SKIPPABLE = ("disk_pressure", "clock_step")

MANDATORY_CORE = (
    "worker_sigkill",
    "coordinator_restart",
) + tuple(f"twopc_fired:{p}" for p in TWOPC_POINTS)


def mandatory_events(local):
    infra = tuple(f for f in INFRA_ENGAGEMENT
                  if not (local and f in LOCAL_SKIPPABLE))
    return MANDATORY_CORE + infra


def read_kv(path):
    out = {}
    if not os.path.exists(path):
        return out
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if "=" in line:
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip()
    return out


def read_json(path):
    if not os.path.exists(path):
        return {}
    try:
        with open(path) as fh:
            return json.load(fh)
    except Exception:
        return {}


def load_chaos(path):
    recs = []
    if not os.path.exists(path):
        return recs
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                recs.append(json.loads(line))
            except Exception:
                continue
    return recs


def count_faults(recs):
    counts = {}
    for rec in recs:
        f = rec.get("fault")
        if f:
            counts[f] = counts.get(f, 0) + 1
    return counts


def build(out_dir, run_id, profile, local):
    verdict = read_json(os.path.join(out_dir, "q9-verdict.json"))
    comp = read_kv(os.path.join(out_dir, "completeness.txt"))
    quiesce = read_kv(os.path.join(out_dir, "final-quiesce.txt"))
    catchup = read_kv(os.path.join(out_dir, "catchup.txt"))
    recs = load_chaos(os.path.join(out_dir, "q9-chaos.jsonl"))
    faults = count_faults(recs)
    job_gone = os.path.exists(os.path.join(out_dir, "job-gone.txt"))
    chaos_died = os.path.exists(os.path.join(out_dir, "chaos-died.txt"))
    oracle_dirty = os.path.exists(os.path.join(out_dir, "oracle-dirty.txt"))

    findings = verdict.get("findings", [])
    stuck = bool(verdict.get("stuck", False))

    # --- infra coverage WITH engagement ------------------------------------
    # Credit demands both halves: the fault fired AND its evidence says it
    # bit. A fired-but-unengaged infra fault is a gap with a sharper name.
    skipped = {r.get("skipped") for r in recs if r.get("fault") == "fault_skipped"}
    skipped |= {r.get("fault", "").replace("_skipped", "")
                for r in recs if r.get("fault", "").endswith("_skipped")}
    engagement = {}
    for fault, evidence_rec in INFRA_ENGAGEMENT.items():
        hits = [r for r in recs if r.get("fault") == evidence_rec]
        engagement[fault] = any(r.get("engaged") is True for r in hits)

    gaps = []
    unengaged = []
    for e in mandatory_events(local):
        if e in INFRA_ENGAGEMENT:
            if faults.get(e, 0) == 0:
                gaps.append(e)
            elif not engagement[e]:
                unengaged.append(e)
        elif faults.get(e, 0) == 0:
            gaps.append(e)
    silent_absences = []
    if local:
        for f in LOCAL_SKIPPABLE:
            if faults.get(f, 0) == 0 and f not in skipped:
                silent_absences.append(f)

    # Revert hygiene: a drained revert at exit means a fault's own in-band
    # revert never ran - tolerable (the drain exists for exactly this) but
    # reported; a FAILED revert is a dirty rig and blocks PASS.
    reverts_failed = [r for r in recs if r.get("fault") == "revert_failed"]
    reverts_drained = [r for r in recs if r.get("fault") == "revert_drained"]

    # --- correctness -----------------------------------------------------
    caught_up = catchup.get("caught_up", "no") == "yes"
    produced = int(comp.get("produced_total", 0) or 0)
    sum_n = int(comp.get("sum_n", 0) or 0)
    missing = int(comp.get("keys_missing", 0) or 0)
    wrong = int(comp.get("keys_wrong_n", 0) or 0)
    fabricated = int(comp.get("keys_fabricated", 0) or 0)
    null_rows = int(comp.get("rows_with_null_n", 0) or 0)
    checked = int(comp.get("keys_checked", 0) or 0)
    have_endstate = bool(comp)
    exact = have_endstate and produced > 0 and produced == sum_n
    keys_clean = fabricated == 0 and null_rows == 0
    if caught_up:
        keys_clean = keys_clean and missing == 0 and wrong == 0
    quiesced = quiesce.get("quiesced", "no") == "yes"

    hard_fail = (
        bool(findings) or stuck or oracle_dirty or job_gone
        or bool(reverts_failed)
        or (caught_up and have_endstate and not exact)
        or (have_endstate and not keys_clean)
    )

    if hard_fail:
        result = "FAIL"
    elif not have_endstate or checked == 0 or not caught_up or not exact:
        result = "INCONCLUSIVE"
    elif gaps or unengaged or silent_absences or chaos_died or not quiesced:
        result = "INCONCLUSIVE"
    else:
        result = "PASS"

    lines = []
    a = lines.append
    a(f"# QUAL-09 - the infrastructure fault matrix ({run_id})")
    a("")
    if local:
        a("**LOCAL HARNESS GATE**: disk_pressure and clock_step are skipped in")
        a("this environment (recorded by the controller). This run gates the")
        a("harness; the claim is only ever made from a cloud run with full")
        a("coverage.")
        a("")
    a(f"- chaos profile: `{profile}`")
    a("")
    a("## Infrastructure coverage, with engagement")
    a("")
    for fault in INFRA_ENGAGEMENT:
        fired = faults.get(fault, 0)
        if fault in skipped:
            a(f"- {fault}: SKIPPED for this environment (recorded)")
        elif fired == 0:
            a(f"- {fault}: NEVER FIRED")
        else:
            a(f"- {fault}: fired {fired}x, engaged: "
              f"{'yes' if engagement[fault] else 'NO - proved nothing'}")
    if unengaged:
        a("")
        a(f"- UNENGAGED INFRA FAULTS: {', '.join(unengaged)}")
    if silent_absences:
        a("")
        a(f"- SILENT ABSENCES (no skip record): {', '.join(silent_absences)}")
    if reverts_drained:
        a("")
        a(f"- reverts drained at controller exit: "
          f"{', '.join(sorted(set(r.get('reverted', '?') for r in reverts_drained)))}"
          " (a fault's in-band revert did not run; the drain covered it)")
    if reverts_failed:
        a("")
        a(f"- REVERTS FAILED: "
          f"{', '.join(sorted(set(r.get('reverted', '?') for r in reverts_failed)))}"
          " - the rig may be dirty and nothing after the failure is trustworthy")
    a("")
    a("## Correctness")
    a("")
    a(f"- caught up before judging: {'yes' if caught_up else 'NO'}")
    a(f"- exact fold (produced == folded): {'yes' if exact else 'NO'}"
      f" ({produced} / {sum_n})")
    a(f"- keys judged: {checked}; missing {missing}, wrong {wrong},"
      f" invented {fabricated}, NULL {null_rows}")
    a("")
    a("## All faults applied")
    a("")
    for name in sorted(faults):
        a(f"- {name}: {faults[name]}")
    if gaps:
        a("")
        a(f"- COVERAGE GAPS: {', '.join(gaps)}")
    a("")
    a(f"- oracle samples: {verdict.get('samples', 0)}; findings: {len(findings)}")
    a(f"- quiesced: {'yes' if quiesced else 'NO'}")
    if chaos_died:
        a("- chaos controller died before the battery ended")
    if job_gone:
        a("- the job disappeared from the coordinator")
    a("")
    a(f"**RESULT: {result}**")
    a("")
    return "\n".join(lines) + "\n", result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--profile", default="?")
    ap.add_argument("--local", action="store_true",
                    help="harness-gate mode: the environment-skippable "
                         "faults may be absent WITH a skip record")
    args = ap.parse_args()
    text, _ = build(args.out_dir, args.run_id, args.profile, args.local)
    sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
