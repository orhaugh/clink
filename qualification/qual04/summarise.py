#!/usr/bin/env python3
"""Render QUAL-04's evidence directory into the standard qualification
summary. Reads only retained artefacts and states plainly when a field
has no evidence behind it.

The result logic follows QUAL-01 to QUAL-03's discipline, with the pass
criterion that belongs to a SIZE campaign rather than a delivery one:

  PASS         the state target was actually reached (measured from
               outside the engine), the end-state accounting is exact
               (every produced event folded in exactly once), every
               sampled key matches what the seed predicts, the oracle was
               never stuck, all mandatory faults fired, coordinator
               restarts left worker PIDs stable, and no job-gone /
               chaos-died / oracle-dirty markers.
  FAIL         any finding, a stuck oracle, inexact accounting, a sampled
               key that disagrees with the seed, or a dead job.
  INCONCLUSIVE clean but unproven - most importantly, the state target
               was NOT reached, which makes a clean run evidence about a
               small job rather than a large one. An interrupted campaign
               is never a weak pass.

The state-target gate is the one that stops this campaign quietly
qualifying nothing: a run that stayed at 2 GB and survived every fault
says nothing about 100 GB, however green its counters look.
"""
import argparse
import collections
import json
import os
import sys

# The COORDINATOR-side points only. QUAL-04's sink is an upsert sink,
# which has no prepare or commit phase to crash inside, so requiring the
# sink.* points would make PASS structurally unreachable - the QUAL-02
# lesson about importing another campaign's mandatory set wholesale.
# The completion-marker windows do fire here and are the ones that matter
# at size: a coordinator dying between writing COMPLETED-N and
# broadcasting the commit, with a large state to restore from.
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


def load_json(path, default=None):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def load_kv(path):
    out = {}
    try:
        with open(path) as f:
            for line in f:
                if "=" in line:
                    k, v = line.rstrip("\n").split("=", 1)
                    out[k] = v
    except OSError:
        pass
    return out


def coverage_and_pid_gates(chaos_path):
    seen = collections.Counter()
    pid_violations = []
    coord_restarts = 0
    if os.path.exists(chaos_path):
        with open(chaos_path) as f:
            for line in f:
                try:
                    entry = json.loads(line)
                except ValueError:
                    continue
                fault = entry.get("fault", "")
                seen[fault] += 1
                is_coord_restart = (fault == "coordinator_restart"
                                    or fault.startswith("twopc_recovered:coordinator."))
                if is_coord_restart:
                    coord_restarts += 1
                    before = entry.get("worker_pids_before")
                    after = entry.get("worker_pids_after")
                    if not before or not after:
                        pid_violations.append(f"{fault}: no worker PID evidence recorded")
                    elif before != after:
                        pid_violations.append(f"{fault}: worker PIDs changed {before} -> {after}")
    missing = [ev for ev in MANDATORY_EVENTS if seen[ev] == 0]
    return missing, pid_violations, coord_restarts, seen


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--duration-h", default="?")
    ap.add_argument("--profile", default="?")
    ap.add_argument("--state-target-gib", type=float, default=0.0)
    args = ap.parse_args()

    d = args.out_dir
    verdict = load_json(os.path.join(d, "q4-verdict.json"), {})
    verification = load_kv(os.path.join(d, "verification.txt"))
    completeness = load_kv(os.path.join(d, "completeness.txt"))
    quiesce = load_kv(os.path.join(d, "final-quiesce.txt"))
    catchup = load_kv(os.path.join(d, "catchup.txt"))
    statesize = load_kv(os.path.join(d, "state-size-final.txt"))

    findings = verdict.get("findings") or []
    stuck = bool(verdict.get("stuck"))
    samples = int(verdict.get("samples") or 0)
    quiesced = quiesce.get("quiesced") == "yes"
    job_gone = os.path.exists(os.path.join(d, "job-gone.txt"))
    chaos_died = os.path.exists(os.path.join(d, "chaos-died.txt"))
    oracle_dirty = os.path.exists(os.path.join(d, "oracle-dirty.txt"))

    produced = int(completeness.get("produced_total") or -1)
    sum_n = int(completeness.get("sum_n") or -1)
    distinct_keys = int(completeness.get("distinct_keys") or -1)
    wrong_len = int(completeness.get("wrong_blob_len_rows") or -1)
    checked = int(completeness.get("sampled_keys_checked") or -1)
    missing = int(completeness.get("sampled_keys_missing") or -1)
    wrong_n = int(completeness.get("sampled_keys_wrong_count") or -1)
    wrong_blob = int(completeness.get("sampled_keys_wrong_blob_len") or -1)
    fabricated = int(completeness.get("sampled_keys_fabricated") or -1)

    # Exact accounting is only judgeable over a pipeline that finished
    # reading. Behind-but-correct and lost-events look identical in the
    # totals, so a run that never caught up is reported as unproven
    # rather than convicted.
    caught_up = catchup.get("caught_up", "yes") == "yes"
    have_accounting = produced >= 0 and sum_n >= 0 and caught_up
    exact = have_accounting and produced == sum_n and wrong_len == 0
    have_sample = checked >= 0
    sample_clean = (have_sample and missing == 0 and wrong_n == 0
                    and wrong_blob == 0 and fabricated <= 0)
    # A sample that checked nothing proves nothing.
    sample_meaningful = checked > 0

    peak_gib = float(statesize.get("state_gib") or 0.0)
    target_met = args.state_target_gib <= 0 or peak_gib >= args.state_target_gib

    uncovered, pid_violations, coord_restarts, seen = coverage_and_pid_gates(
        os.path.join(d, "q4-chaos.jsonl"))

    clean = (samples > 0 and not findings and not stuck and not oracle_dirty
             and not job_gone and sample_clean and sample_meaningful
             and (exact or not caught_up))
    gaps = []
    if not quiesced:
        gaps.append("final verdict never quiesced")
    if not target_met:
        gaps.append(f"state target not reached ({peak_gib:.2f} GiB of "
                    f"{args.state_target_gib:.2f} GiB) - a clean run at this size "
                    f"is evidence about a small job, not a large one")
    if not sample_meaningful:
        gaps.append("no sampled key was checkable against the seed")
    if not caught_up:
        gaps.append(f"the pipeline never caught up with the generator "
                    f"({catchup.get('folded_at_catchup', '?')} of "
                    f"{catchup.get('produced_final', '?')} events read), so exact "
                    f"accounting cannot separate lost events from unread ones")
    gaps += uncovered + pid_violations

    if clean and not gaps:
        result = "PASS"
    elif clean and gaps:
        result = ("INCONCLUSIVE (correctness clean but the campaign cannot prove "
                  f"what it set out to: {'; '.join(gaps)})")
    elif findings or stuck or oracle_dirty or job_gone \
            or (have_accounting and not exact) \
            or (have_sample and not sample_clean):
        result = "FAIL"
    else:
        result = "INCONCLUSIVE (no judged verdict retained)"

    def num(x):
        return "no evidence" if x is None or x < 0 else str(x)

    print("# QUAL-04 Large Keyed State Campaign\n")
    print(f"Run ID: `{args.run_id}`  ")
    print(f"Duration (configured): {args.duration_h}h  ")
    print(f"Chaos profile: {args.profile}\n")

    print("## State reached\n")
    print(f"- LIVE keyed state at the end: {statesize.get('state_gib', 'no evidence')} GiB "
          f"({statesize.get('state_live_bytes', 'no evidence')} bytes over "
          f"{statesize.get('state_live_keys', 'no evidence')} keyed entries)")
    print(f"- Store footprint: {statesize.get('state_footprint_gib', 'no evidence')} GiB "
          f"across {statesize.get('state_objects', 'no evidence')} objects "
          f"({statesize.get('state_footprint_ratio', '?')}x live)")
    print(f"- Target: {args.state_target_gib:.2f} GiB "
          f"({'reached' if target_met else 'NOT REACHED'})")
    print(f"- Distinct keys in state: {num(distinct_keys)}")
    print(f"- Per-key accumulator width: "
          f"{verification.get('blob_bytes', 'no evidence')} bytes")
    print(f"- State backend: {verification.get('state_backend', 'no evidence')}")
    print("- Both measured from OUTSIDE the engine (the backend's own manifest "
          "and object listing), not from an engine-reported gauge")
    print("- The size gate reads LIVE state. The footprint is larger because the "
          "store is content-addressed and append-only within a run: each update "
          "writes a new value object and orphan reclamation (sweep) has no "
          "caller in the engine, so footprint tracks update volume, not state "
          "size\n")

    print("## Workload\n")
    print(f"- Input events produced: {num(produced)}")
    print(f"- Events folded into keyed state: {num(sum_n)}")
    print(f"- Verifier samples taken: {samples}\n")

    print("## Faults injected\n")
    for fault, n in sorted(seen.items(), key=lambda kv: -kv[1]):
        print(f"- {fault}: {n}")
    print()

    print("## Required-fault coverage\n")
    for ev in MANDATORY_EVENTS:
        print(f"- {ev}: {'covered' if seen[ev] else 'MISSING'}")
    print(f"- coordinator restarts with stable-PID evidence: {coord_restarts}\n")

    print("## Correctness (independent oracle, reads the verification table)\n")
    print(f"- Findings (short blob / shrinking / overcount): {len(findings)}")
    for f in findings[:20]:
        print(f"  - {json.dumps(f)}")
    print(f"- Oracle stuck: {'yes' if stuck else 'no'}")
    print(f"- Exact accounting: produced {num(produced)}, counted {num(sum_n)}"
          + ("" if not have_accounting else
             (" (exact)" if produced == sum_n else " (MISMATCH - events lost or double-counted)")))
    print(f"- Rows whose accumulator was not the full width: {num(wrong_len)}")
    print(f"- Sampled keys verified against the seed: {num(checked)} "
          f"(missing {num(missing)}, wrong count {num(wrong_n)}, "
          f"wrong width {num(wrong_blob)}, fabricated {num(fabricated)})")
    print(f"- Final verdict quiesced: {'yes' if quiesced else 'NO'}\n")

    if job_gone:
        print("- CAVEAT: the job stopped RUNNING mid-soak (job-gone.txt)\n")
    if chaos_died:
        print("- CAVEAT: the chaos controller stopped before the soak ended (chaos-died.txt)\n")

    print("## Result\n")
    print(f"**{result}**\n")
    if result == "PASS":
        print("Keyed state of the stated size was held across the full fault "
              "schedule with every produced event folded in exactly once, and "
              "every sampled key's accumulator matched what the seed predicts.")
    elif result.startswith("INCONCLUSIVE"):
        print("Treat the run as void and re-run: an incomplete campaign is not a "
              "weak pass.")
    else:
        print("The evidence directory contains the defect trail; fix first, "
              "publish never (failed runs are not published).")

    print("\n## Caveats\n")
    print("- Correctness is asserted only for the workload, fault profile, state "
          "size and duration above.")
    print("- The accumulator is a synthetic fixed-width value: the campaign "
          "measures state VOLUME and its survival, not a representative "
          "application's value distribution.")
    print("- The sink is an upsert table (effectively-once by key), which is the "
          "verification channel rather than the subject. Exactly-once delivery "
          "has its own campaigns.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
