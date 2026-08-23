#!/usr/bin/env python3
"""QUAL-08 result logic: turn a run's retained evidence into a verdict.

The campaign claims that a stateful SQL job upgrades across engine
revisions with exactly-once continuity. The verdict logic differs from
the other campaigns in one structural way: the UPGRADE ITSELF is a gated
sequence with its own evidence file (upgrade.txt), and a failure at any
step of it is a FAIL of the campaign's actual subject - not an
infrastructure inconclusive. Specifically:

  * operator ids differing between the two revisions = FAIL (restored
    state would be silently orphaned; the pre-flight exists to catch it);
  * the new image refusing the savepoint (check-savepoint) = FAIL, and
    the refusal text is the finding;
  * a restore that "succeeded" but carried almost none of the savepoint's
    bytes = FAIL (the silent nothing-restored path);
  * missing boundary timings = INCONCLUSIVE (the upgrade may have worked,
    but the campaign cannot say what it cost).

Everything the retention campaign established about judging correctness
carries over unchanged: exactness only when caught up, fabricated keys
always fatal, coverage gaps INCONCLUSIVE. A same-image run is the local
smoke: the verdict is computed identically but labelled as machinery
evidence, never publishable as an upgrade claim.
"""
import argparse
import json
import os
import sys

TWOPC_POINTS = (
    "coordinator.before_completed_marker",
    "coordinator.after_completed_marker",
)

MANDATORY_EVENTS = (
    "worker_sigkill",
    "coordinator_restart",
    "broker_restart",
    "network_latency",
    "partition_from_coordinator",
) + tuple(f"twopc_fired:{p}" for p in TWOPC_POINTS)


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


def count_faults(path):
    counts = {}
    if not os.path.exists(path):
        return counts
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            f = rec.get("fault")
            if f:
                counts[f] = counts.get(f, 0) + 1
    return counts


def coordinator_restart_pids_stable(path):
    ok = 0
    total = 0
    if not os.path.exists(path):
        return ok, total
    with open(path) as fh:
        for line in fh:
            try:
                rec = json.loads(line)
            except Exception:
                continue
            if rec.get("fault") != "coordinator_restart":
                continue
            total += 1
            if rec.get("worker_pids_before") and \
               rec.get("worker_pids_before") == rec.get("worker_pids_after"):
                ok += 1
    return ok, total


def to_int(v, default=-1):
    try:
        return int(v)
    except Exception:
        return default


def build(out_dir, run_id, profile):
    upgrade = read_kv(os.path.join(out_dir, "upgrade.txt"))
    verdict = read_json(os.path.join(out_dir, "q8-verdict.json"))
    comp = read_kv(os.path.join(out_dir, "completeness.txt"))
    quiesce = read_kv(os.path.join(out_dir, "final-quiesce.txt"))
    catchup = read_kv(os.path.join(out_dir, "catchup.txt"))
    faults = count_faults(os.path.join(out_dir, "q8-chaos.jsonl"))
    job_gone = os.path.exists(os.path.join(out_dir, "job-gone.txt"))
    chaos_died = os.path.exists(os.path.join(out_dir, "chaos-died.txt"))
    oracle_dirty = os.path.exists(os.path.join(out_dir, "oracle-dirty.txt"))

    findings = verdict.get("findings", [])
    stuck = bool(verdict.get("stuck", False))

    # --- the upgrade sequence -----------------------------------------------
    have_upgrade = bool(upgrade)
    same_image = upgrade.get("same_image", "no") == "yes"
    opid_match = upgrade.get("opid_match", "no") == "yes"
    savepoint_ok = upgrade.get("savepoint_ok", "no") == "yes"
    checksave = upgrade.get("checksave", "missing")
    restore_ok = upgrade.get("restore_ok", "no") == "yes"
    restore_carried = upgrade.get("restore_carried", "no") == "yes"
    savepoint_s = to_int(upgrade.get("savepoint_s", -1))
    restore_s = to_int(upgrade.get("restore_s", -1))
    downtime_s = to_int(upgrade.get("downtime_s", -1))
    digests_same = (upgrade.get("digest_v0") and
                    upgrade.get("digest_v0") == upgrade.get("digest_v1"))

    upgrade_failures = []
    if have_upgrade:
        if not opid_match:
            upgrade_failures.append(
                "the two revisions numbered the script's operators differently; "
                "restored state would be silently orphaned")
        elif not savepoint_ok:
            upgrade_failures.append("the savepoint on v0 did not complete")
        elif checksave == "refused":
            upgrade_failures.append(
                "the new image refused the savepoint (check-savepoint); "
                "a migration path is missing for this pair")
        elif not restore_ok:
            upgrade_failures.append("the restored job did not submit on v1")
        elif not restore_carried:
            upgrade_failures.append(
                "the restore carried almost none of the savepoint's bytes; "
                "the job started clean instead of restored")
        if not same_image and digests_same:
            upgrade_failures.append(
                "both image tags resolve to the same digest; nothing was upgraded")
    timings_missing = restore_ok and (savepoint_s < 0 or restore_s < 0 or downtime_s < 0)

    # --- correctness, spanning the boundary -----------------------------------
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

    gaps = [e for e in MANDATORY_EVENTS if faults.get(e, 0) == 0]
    pid_ok, pid_total = coordinator_restart_pids_stable(
        os.path.join(out_dir, "q8-chaos.jsonl"))
    if pid_total > 0 and pid_ok == 0:
        gaps.append("coordinator_restart with stable worker PIDs")

    hard_fail = (
        bool(findings) or stuck or oracle_dirty or job_gone
        or not have_upgrade
        or bool(upgrade_failures)
        or (caught_up and have_endstate and not exact)
        or (have_endstate and not keys_clean)
    )

    if hard_fail:
        result = "FAIL"
    elif not have_endstate or checked == 0 or not caught_up or not exact:
        result = "INCONCLUSIVE"
    elif timings_missing or gaps or chaos_died or not quiesced:
        result = "INCONCLUSIVE"
    else:
        result = "PASS"

    lines = []
    a = lines.append
    a(f"# QUAL-08 - rolling upgrade across engine revisions ({run_id})")
    a("")
    if same_image:
        a("**SINGLE-IMAGE SMOKE**: both sides ran the same image. This run is")
        a("evidence about the savepoint/swap/restore MACHINERY only and must")
        a("never be published as an upgrade claim.")
        a("")
    a(f"- from: `{upgrade.get('image_v0', '?')}`")
    a(f"- to:   `{upgrade.get('image_v1', '?')}`")
    a(f"- chaos profile on v1: `{profile}`")
    a("")
    a("## The upgrade sequence")
    a("")
    a(f"- operator ids identical across the pair: {'yes' if opid_match else 'NO'}")
    a(f"- savepoint completed on v0: {'yes' if savepoint_ok else 'NO'}"
      + (f" (id {upgrade.get('savepoint_id','?')}, {savepoint_s}s,"
         f" {to_int(upgrade.get('savepoint_bytes', 0), 0)} bytes)" if savepoint_ok else ""))
    a(f"- check-savepoint under the new image: {checksave}")
    a(f"- restore on v1: {'yes' if restore_ok else 'NO'}"
      + (f" ({restore_s}s to RUNNING)" if restore_ok else ""))
    a(f"- restored state carried: {'yes' if restore_carried else 'NO'}"
      + (f" (first v1 checkpoint {to_int(upgrade.get('v1_first_ckpt_bytes', 0), 0)} bytes)"
         if restore_ok else ""))
    a(f"- downtime (savepoint done -> first v1 checkpoint): "
      + (f"{downtime_s}s" if downtime_s >= 0 else "NOT MEASURED"))
    if upgrade_failures:
        a("")
        for f in upgrade_failures:
            a(f"- UPGRADE FAILURE: {f}")
    if not have_upgrade:
        a("")
        a("- UPGRADE FAILURE: no upgrade.txt was retained; the sequence never ran")
    a("")
    a("## Correctness across the boundary")
    a("")
    a(f"- caught up before judging: {'yes' if caught_up else 'NO'}")
    a(f"- exact fold (produced == folded): {'yes' if exact else 'NO'}"
      f" ({produced} / {sum_n})")
    a(f"- keys judged: {checked}; missing {missing}, wrong {wrong},"
      f" invented {fabricated}, NULL {null_rows}")
    a("")
    a("## Faults applied on v1")
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
    args = ap.parse_args()
    text, _ = build(args.out_dir, args.run_id, args.profile)
    sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
