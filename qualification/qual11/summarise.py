#!/usr/bin/env python3
"""QUAL-11 result logic: turn a run's retained evidence into a verdict.

The campaign claims that a running stateful job survives a STATE SCHEMA
CHANGE - savepoint on v1, swap the operator code for a v2 whose state
type changed shape, restore through a registered migration - with
exactly-once continuity and a migration whose effect is exactly what its
pure function predicts. Four gates, each able to fail on its own:

1. THE PRE-DEPLOY CHECK RAN AND PASSED for the good v2. A refused check
   is INCONCLUSIVE, never a FAIL and never a deploy: the campaign does
   not push past a refusal, because a restore the engine says it cannot
   perform is not a claim about migration.

2. THE NEGATIVE CONTROL WAS REFUSED. The same check, in the same run,
   against a v2 built WITHOUT the migration must say no. If it says yes,
   the gate approves anything and gate 1 proved nothing - that is a
   FAIL, not an inconclusive, because the campaign's own instrument is
   broken and every prior green reading is suspect.

3. CONTINUITY AND EXACTNESS ACROSS THE BOUNDARY. One logical job (a
   second job id appearing means the "restore" was a fresh start), the
   generator's events all accounted for, and every key's final count and
   sum equal to the deterministic spec's recomputation. Re-emitted rows
   are NOT judged: the sink is at-least-once and the restore replays, so
   duplicates are by design (the verifier's header explains why even the
   running sum at a given count need not be stable across a replay).

4. THE MIGRATION'S EFFECT, PREDICTED AND MEASURED. Keys that existed
   before the boundary must CARRY their counts (a key restarting at 1 is
   state loss), and their first post-boundary row must show the migrated
   range fields collapsing onto that row's own amount - the signature the
   registered migration's sentinel seeding produces. Zero keys observed
   across the boundary is not a pass: it is no evidence, so INCONCLUSIVE.
"""
import argparse
import json
import os
import sys


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
    except Exception:  # noqa: BLE001
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
            except Exception:  # noqa: BLE001
                continue
    return recs


# The 2PC crash windows this campaign can actually fire. The job's sink
# is at-least-once, so NO sink-side point is reachable - arming one makes
# the coverage pre-pass chase a fault that can never fire, which is
# exactly what chaos.py's own comment warns about and what kept an
# otherwise-green local run INCONCLUSIVE: the controller burned a whole
# battery on two unreachable sink points, failed twice in a row, and
# never advanced far enough to restart the coordinator. The
# COORDINATOR-side checkpoint-marker windows are reachable whatever the
# sink is, and they are the ones that matter around a migrated job.
TWOPC_POINTS = (
    "coordinator.before_completed_marker",
    "coordinator.after_completed_marker",
)

MANDATORY_FAULTS = ("worker_sigkill", "coordinator_restart")


def build(out_dir, run_id, local):
    boundary = read_kv(os.path.join(out_dir, "boundary.txt"))
    verify = read_json(os.path.join(out_dir, "q11-verify.json"))
    catchup = read_kv(os.path.join(out_dir, "catchup.txt"))
    quiesce = read_kv(os.path.join(out_dir, "final-quiesce.txt"))
    recs = load_chaos(os.path.join(out_dir, "q11-chaos.jsonl"))
    faults = {}
    for r in recs:
        f = r.get("fault")
        if f:
            faults[f] = faults.get(f, 0) + 1
    job_gone = os.path.exists(os.path.join(out_dir, "job-gone.txt"))

    # --- gate 1 + 2: the pre-deploy check and its negative control ---------
    check_v2 = boundary.get("check_v2", "")          # pass | refused | missing
    check_broken = boundary.get("check_v2_broken", "")  # refused | accepted | missing
    gate1_ok = check_v2 == "pass"
    gate2_ok = check_broken == "refused"
    gate2_inert = check_broken == "accepted"

    # --- gate 3: continuity and exactness ----------------------------------
    same_job = boundary.get("same_job_id", "") == "yes"
    savepoint_ok = boundary.get("savepoint_ok", "") == "yes"
    restore_ok = boundary.get("restore_ok", "") == "yes"
    caught_up = catchup.get("caught_up", "no") == "yes"
    quiesced = quiesce.get("quiesced", "no") == "yes"
    have_verify = bool(verify) and "error" not in verify and verify.get("rows", 0) > 0
    keys_expected = int(verify.get("keys_expected", 0) or 0)
    missing = int(verify.get("keys_missing", 0) or 0)
    wrong_n = int(verify.get("keys_wrong_n", 0) or 0)
    wrong_sum = int(verify.get("keys_wrong_sum", 0) or 0)
    fabricated = int(verify.get("keys_fabricated", 0) or 0)
    duplicates = int(verify.get("duplicate_rows", 0) or 0)
    malformed = int(verify.get("malformed_rows", 0) or 0)
    exact = have_verify and keys_expected > 0 and missing == 0 and wrong_n == 0 and wrong_sum == 0
    # Duplicates are NOT a defect signal here: the sink is at-least-once
    # and the restore replays, so re-emission is by design (see the
    # verifier's header). Fabricated keys and malformed rows still are.
    clean = fabricated == 0 and malformed == 0

    # --- gate 4: the migration's effect, read from the STATE ----------------
    # From the savepoints, not the output stream: the sink is
    # at-least-once and buffers, so an unrelated fault can destroy the
    # stream-side evidence while the engine behaves perfectly (it did).
    effect = read_json(os.path.join(out_dir, "q11-effect.json"))
    have_effect = bool(effect) and "error" not in effect
    carried = int(effect.get("carried", 0) or 0)
    lost = int(effect.get("lost", 0) or 0)
    untouched = int(effect.get("untouched", 0) or 0)
    predicted_ok = int(effect.get("predicted_ok", 0) or 0)
    predicted_bad = int(effect.get("predicted_bad", 0) or 0)
    gate4_evidence = have_effect and carried + lost > 0
    gate4_ok = (gate4_evidence and lost == 0 and predicted_bad == 0
                and carried > 0 and predicted_ok > 0)

    gaps = [f for f in MANDATORY_FAULTS if faults.get(f, 0) == 0]

    hard_fail = (
        gate2_inert
        or job_gone
        or (have_verify and not clean)
        or (have_verify and caught_up and not exact)
        or (gate4_evidence and (lost > 0 or predicted_bad > 0))
    )
    if hard_fail:
        result = "FAIL"
    elif not (savepoint_ok and restore_ok and gate1_ok and gate2_ok):
        result = "INCONCLUSIVE"
    elif not have_verify or not caught_up or not exact or not same_job or not quiesced:
        result = "INCONCLUSIVE"
    elif not gate4_ok or gaps:
        result = "INCONCLUSIVE"
    else:
        result = "PASS"

    lines = []
    a = lines.append
    a(f"# QUAL-11 - state schema evolution across a live boundary ({run_id})")
    a("")
    if local:
        a("**LOCAL HARNESS GATE**: this run gates the machinery; the claim is")
        a("only ever made from a cloud run.")
        a("")
    a("## The evolution boundary")
    a("")
    a(f"- savepoint on v1: {'ok' if savepoint_ok else 'NO'}"
      f" (id {boundary.get('savepoint_id', '?')}, {boundary.get('savepoint_s', '?')}s)")
    a(f"- pre-deploy check of the v2 job: "
      f"{'PASS' if gate1_ok else check_v2.upper() or 'MISSING'}")
    a(f"- negative control (v2 WITHOUT the migration): "
      f"{'refused, as required' if gate2_ok else (check_broken.upper() or 'MISSING')}")
    if gate2_inert:
        a("  - THE CHECK ACCEPTED A JOB THAT CANNOT MIGRATE: the gate is inert "
          "and gate 1's pass proves nothing")
    a(f"- restore on v2: {'ok' if restore_ok else 'NO'}"
      f" ({boundary.get('restore_s', '?')}s), same logical job: "
      f"{'yes' if same_job else 'NO'}")
    a("")
    a("## Correctness across the boundary")
    a("")
    a(f"- caught up before judging: {'yes' if caught_up else 'NO'}")
    a(f"- keys judged: {keys_expected}; missing {missing}, wrong count {wrong_n},"
      f" wrong sum {wrong_sum}, invented {fabricated}")
    a(f"- re-emitted rows (expected under at-least-once + replay): {duplicates};"
      f" malformed rows: {malformed}")
    a("")
    a("## The migration's effect (read from the savepoints)")
    a("")
    if not gate4_evidence:
        a("- NO STATE EVIDENCE - the migration's effect was not measured")
    else:
        a(f"- keys carried across the boundary: {carried}; LOST: {lost}")
        a(f"- keys still untouched after the restore: {untouched}")
        a(f"- untouched keys holding the migration's exact predicted output: "
          f"{predicted_ok}; NOT holding it: {predicted_bad}")
        if untouched == 0:
            a("  - no untouched key survived to the second savepoint, so the")
            a("    predicted-output half has no evidence")
    for smp in effect.get("samples", [])[:5]:
        a(f"  - sample: {json.dumps(smp)}")
    a("")
    a("## Faults applied (on the migrated engine)")
    a("")
    for name in sorted(faults):
        a(f"- {name}: {faults[name]}")
    if gaps:
        a("")
        a(f"- COVERAGE GAPS: {', '.join(gaps)}")
    a("")
    a(f"- quiesced: {'yes' if quiesced else 'NO'}")
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
    ap.add_argument("--local", action="store_true")
    args = ap.parse_args()
    text, _ = build(args.out_dir, args.run_id, args.local)
    sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
