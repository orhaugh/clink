#!/usr/bin/env python3
"""QUAL-06 result logic: turn a run's retained evidence into a verdict.

The campaign claims that clink deploys and runs a job graph of N operators
as S network-bridged subtasks with exactly-once output under faults. The
claim is made at the LARGEST GREEN RUNG of a progressive ladder, so the
verdict logic has three jobs:

  1. every rung's record must be internally consistent: the deployed
     graph's operator count must equal what the generator's arithmetic
     promised (a width claim nobody verified is a number, not a claim);
  2. the final rung - where the claim is made - must carry the full
     correctness bundle: exact accounting, every key seed-checked, caught
     up, quiesced, full mandatory fault coverage;
  3. a rung that failed to deploy is a MEASURED LIMIT, not a campaign
     failure: the verdict stays PASS for the largest green rung, and the
     failed rung is reported as the boundary. Only a correctness violation
     at ANY rung, or a final rung that cannot be judged, blocks the PASS.
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


def load_rungs(out_dir):
    """rung-<n>.txt files, in ladder order."""
    rungs = []
    n = 1
    while True:
        p = os.path.join(out_dir, f"rung-{n}.txt")
        if not os.path.exists(p):
            break
        rungs.append((n, read_kv(p)))
        n += 1
    return rungs


def build(out_dir, run_id, profile):
    rungs = load_rungs(out_dir)
    verdict = read_json(os.path.join(out_dir, "q6-verdict.json"))
    comp = read_kv(os.path.join(out_dir, "completeness.txt"))
    quiesce = read_kv(os.path.join(out_dir, "final-quiesce.txt"))
    catchup = read_kv(os.path.join(out_dir, "catchup.txt"))
    faults = count_faults(os.path.join(out_dir, "q6-chaos.jsonl"))
    job_gone = os.path.exists(os.path.join(out_dir, "job-gone.txt"))
    chaos_died = os.path.exists(os.path.join(out_dir, "chaos-died.txt"))
    oracle_dirty = os.path.exists(os.path.join(out_dir, "oracle-dirty.txt"))

    findings = verdict.get("findings", [])
    stuck = bool(verdict.get("stuck", False))

    # --- per-rung consistency and the green/boundary split -----------------
    green = []
    boundary = None
    rung_problems = []
    for n, r in rungs:
        status = r.get("status", "?")
        expected_ops = int(r.get("expected_ops", 0) or 0)
        deployed_ops = int(r.get("deployed_ops", -1) or -1)
        if status == "green":
            if deployed_ops != expected_ops:
                rung_problems.append(
                    f"rung {n} deployed {deployed_ops} ops against a claim of {expected_ops}")
            green.append((n, r))
        elif status in ("capacity", "deploy-failed", "recovery-failed"):
            boundary = (n, r)
            break
        else:
            rung_problems.append(f"rung {n} has unrecognised status '{status}'")
            break

    final = green[-1] if green else None

    # --- final-rung correctness --------------------------------------------
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
        os.path.join(out_dir, "q6-chaos.jsonl"))
    if pid_total > 0 and pid_ok == 0:
        gaps.append("coordinator_restart with stable worker PIDs")

    hard_fail = (
        bool(findings) or stuck or oracle_dirty or job_gone
        or bool(rung_problems)
        or (caught_up and have_endstate and not exact)
        or (have_endstate and not keys_clean)
    )

    if hard_fail:
        result = "FAIL"
    elif final is None:
        result = "INCONCLUSIVE"
    elif not have_endstate or checked == 0 or not caught_up or not exact:
        result = "INCONCLUSIVE"
    elif gaps or chaos_died or not quiesced:
        result = "INCONCLUSIVE"
    else:
        result = "PASS"

    lines = []
    a = lines.append
    a(f"# QUAL-06 - large job graphs at high parallelism ({run_id})")
    a("")
    a(f"- chaos profile at the final rung: `{profile}`")
    a("")
    a("## The ladder")
    a("")
    a("| Rung | Branches | Operators | Subtasks | Deploy (s) | First checkpoint (s) | Status |")
    a("|---|---|---|---|---|---|---|")
    for n, r in rungs:
        a(f"| {n} | {r.get('branches','?')} | {r.get('deployed_ops','?')}"
          f" | {r.get('subtasks','?')} | {r.get('deploy_s','?')}"
          f" | {r.get('first_checkpoint_s','?')} | {r.get('status','?')} |")
    if boundary:
        n, r = boundary
        a("")
        a(f"- measured boundary: rung {n} ({r.get('subtasks','?')} subtasks) - "
          f"{r.get('status')}: {r.get('reason','')}")
    if rung_problems:
        a("")
        for p in rung_problems:
            a(f"- RUNG PROBLEM: {p}")
    a("")
    if final:
        n, r = final
        a(f"## The claim rung: {r.get('subtasks','?')} subtasks "
          f"({r.get('deployed_ops','?')} operators x parallelism {r.get('parallelism','?')})")
    else:
        a("## No rung went green")
    a("")
    a("## Correctness at the claim rung")
    a("")
    a(f"- caught up before judging: {'yes' if caught_up else 'NO'}")
    a(f"- exact fold (produced == folded): {'yes' if exact else 'NO'}"
      f" ({produced} / {sum_n})")
    a(f"- keys judged: {checked}; missing {missing}, wrong {wrong},"
      f" invented {fabricated}, NULL {null_rows}")
    a("")
    a("## Faults applied at the claim rung")
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
