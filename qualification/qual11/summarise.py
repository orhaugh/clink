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
   sum equal to the deterministic spec's recomputation. Conflicting
   duplicates (same key and count, different value) are fatal
   separately: that is two answers to one question.

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
    conflicting = int(verify.get("conflicting_rows", 0) or 0)
    malformed = int(verify.get("malformed_rows", 0) or 0)
    exact = have_verify and keys_expected > 0 and missing == 0 and wrong_n == 0 and wrong_sum == 0
    clean = fabricated == 0 and conflicting == 0 and malformed == 0

    # --- gate 4: the migration's effect ------------------------------------
    across = int(verify.get("keys_across_boundary", 0) or 0)
    carried = int(verify.get("keys_carried", 0) or 0)
    reset = int(verify.get("keys_reset", 0) or 0)
    effect_ok = int(verify.get("migration_effect_ok", 0) or 0)
    effect_bad = int(verify.get("migration_effect_bad", 0) or 0)
    gate4_evidence = across > 0
    gate4_ok = gate4_evidence and reset == 0 and effect_bad == 0 and carried > 0 and effect_ok > 0

    gaps = [f for f in MANDATORY_FAULTS if faults.get(f, 0) == 0]

    hard_fail = (
        gate2_inert
        or job_gone
        or (have_verify and not clean)
        or (have_verify and caught_up and not exact)
        or (gate4_evidence and (reset > 0 or effect_bad > 0))
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
    a(f"- conflicting duplicate rows: {conflicting}; malformed rows: {malformed}")
    a("")
    a("## The migration's effect")
    a("")
    if not gate4_evidence:
        a("- NO KEYS OBSERVED ACROSS THE BOUNDARY - the migration has no evidence")
    else:
        a(f"- keys living across the boundary: {across}")
        a(f"- counts carried: {carried}; RESET (state loss): {reset}")
        a(f"- migrated range fields matching the prediction: {effect_ok}; "
          f"NOT matching: {effect_bad}")
    for s in verify.get("samples", [])[:5]:
        a(f"  - sample: {json.dumps(s)}")
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
