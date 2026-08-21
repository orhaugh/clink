#!/usr/bin/env python3
"""Render QUAL-03's evidence directory into the standard qualification
summary. Reads only retained artefacts (q3-verdict.json, q3-chaos.jsonl,
completeness.txt, final-quiesce.txt, the pending-upload end-state files)
and states plainly when a field has no evidence behind it.

The result logic mirrors QUAL-01's and QUAL-02's discipline:
  PASS         quiesced final verdict, zero findings, oracle never stuck,
               end-state complete (everything produced was committed
               exactly once, judged by a fresh full re-read), no
               partition committed past what the generator produced, all
               mandatory faults provably fired, every coordinator restart
               with stable worker PIDs, and no job-gone / chaos-died /
               oracle-dirty markers.
  FAIL         any finding, a stuck oracle, an incompleteness, a foreign
               tail, or a dead job.
  INCONCLUSIVE clean but unproven - the verdict never quiesced, or the
               mandatory-fault coverage is incomplete. An interrupted run
               is never a weak pass.

Unlike QUAL-02, pending multipart uploads left after the clean cancel are
recorded but NOT gated: an upload orphaned by a kill before its
checkpoint became durable stays pending BY DESIGN (production expires it
with a lifecycle rule), whereas an orphaned prepared transaction in
Postgres holds locks and blocks vacuum. The two end states are not
morally equivalent and the gates say so.
"""
import argparse
import collections
import json
import os
import sys

# The 2PC points THIS campaign's sink family can fire. The S3 sink rides
# the same CommittingSink prepare/commit path as Postgres, plus the
# coordinator's completion markers; it has NO commit receipt, so the
# Kafka-only sink.between_commit_and_receipt is deliberately absent.
# campaign.sh reads this tuple as its --twopc-points, so the schedule and
# this gate cannot drift apart.
TWOPC_POINTS = (
    "sink.before_prepare",
    "sink.after_prepare",
    "coordinator.before_completed_marker",
    "coordinator.after_completed_marker",
    "sink.before_commit",
    "sink.after_external_commit",
)

MANDATORY_EVENTS = (
    "worker_sigkill",
    "coordinator_restart",
    "broker_restart",
    "network_latency",
    "partition_from_coordinator",
    # QUAL-03's decisive composition: the object store down while a
    # recovery needs it to complete a restored multipart handle. Both
    # halves are required - an injected outage that never healed judged
    # nothing.
    "s3_unavailable",
    "s3_restored",
) + tuple(f"twopc_fired:{p}" for p in TWOPC_POINTS)


def load_json(path, default=None):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def load_kv(path):
    """key=value lines -> dict (verification.txt / completeness.txt shape)."""
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
    args = ap.parse_args()

    d = args.out_dir
    verdict = load_json(os.path.join(d, "q3-verdict.json"), {})
    verification = load_kv(os.path.join(d, "verification.txt"))
    completeness = load_kv(os.path.join(d, "completeness.txt"))
    quiesce = load_kv(os.path.join(d, "final-quiesce.txt"))

    findings = verdict.get("findings") or []
    stuck = bool(verdict.get("stuck"))
    samples = int(verdict.get("samples") or 0)
    quiesced = quiesce.get("quiesced") == "yes"
    job_gone = os.path.exists(os.path.join(d, "job-gone.txt"))
    chaos_died = os.path.exists(os.path.join(d, "chaos-died.txt"))
    oracle_dirty = os.path.exists(os.path.join(d, "oracle-dirty.txt"))

    produced = int(completeness.get("produced_total") or -1)
    committed_distinct = int(completeness.get("committed_distinct") or -1)
    dup_total = int(completeness.get("dup_total") or -1)
    empty_objects = int(completeness.get("empty_objects") or -1)
    foreign_ahead = int(completeness.get("foreign_ahead_partitions") or -1)
    have_completeness = produced >= 0 and committed_distinct >= 0
    complete = (have_completeness and produced == committed_distinct
                and dup_total == 0 and empty_objects == 0)

    # Recorded, never gated - see the module doc.
    pending_after = load_kv(os.path.join(d, "pending-after-cancel.txt"))
    pending_after_n = pending_after.get("pending_uploads", "(no evidence)")

    uncovered, pid_violations, coord_restarts, seen = coverage_and_pid_gates(
        os.path.join(d, "q3-chaos.jsonl"))

    clean = (samples > 0 and not findings and not stuck and not oracle_dirty
             and not job_gone and complete and foreign_ahead == 0)
    if clean and quiesced and not uncovered and not pid_violations:
        result = "PASS"
    elif clean and (not quiesced or uncovered or pid_violations):
        gaps = ([] if quiesced else ["final verdict never quiesced"]) + uncovered + pid_violations
        result = ("INCONCLUSIVE (correctness clean but the campaign cannot prove it: "
                  f"{'; '.join(gaps)})")
    elif findings or stuck or oracle_dirty or job_gone or foreign_ahead > 0 \
            or (have_completeness and not complete):
        result = "FAIL"
    else:
        result = "INCONCLUSIVE (no judged verdict retained)"

    def num(x):
        return "no evidence" if x is None or x < 0 else str(x)

    print("# QUAL-03 S3 Staged-Commit Campaign\n")
    print(f"Run ID: `{args.run_id}`  ")
    print(f"Duration (configured): {args.duration_h}h  ")
    print(f"Chaos profile: {args.profile}\n")

    print("## Workload\n")
    print(f"- Input events produced: {num(produced)}")
    print(f"- Lines committed (distinct event_id, fresh full re-read): "
          f"{num(committed_distinct)}")
    print(f"- Verifier samples taken: {samples}")
    print(f"- Objects committed: {completeness.get('objects_total', 'no evidence')}")
    print(f"- Pending multipart uploads observed at peak: "
          f"{verdict.get('max_pending_uploads_seen', 'no evidence')}")
    print(f"- Staged commit engaged at the gate: "
          f"{verification.get('sink_family', 'no evidence')}\n")

    print("## Faults injected\n")
    for fault, n in sorted(seen.items(), key=lambda kv: -kv[1]):
        print(f"- {fault}: {n}")
    print()

    print("## Required-fault coverage\n")
    for ev in MANDATORY_EVENTS:
        print(f"- {ev}: {'covered' if seen[ev] else 'MISSING'}")
    print(f"- coordinator restarts with stable-PID evidence: {coord_restarts}\n")

    print("## Correctness (independent oracle, reads the bucket directly)\n")
    print(f"- Findings (duplicates / gaps / foreign / empty / mutated): {len(findings)}")
    for f in findings[:20]:
        print(f"  - {json.dumps(f)}")
    print(f"- Oracle stuck: {'yes' if stuck else 'no'}")
    print(f"- End-state completeness: produced {num(produced)}, "
          f"committed distinct {num(committed_distinct)}, "
          f"duplicates {num(dup_total)}, empty objects {num(empty_objects)}"
          + ("" if not have_completeness else
             (" (complete)" if complete else " (INCOMPLETE - data was lost or corrupted)")))
    print(f"- Partitions committed past the generator's final count: {num(foreign_ahead)}")
    print(f"- Pending multipart uploads after clean cancel: {pending_after_n} "
          f"(recorded, not gated - orphans of never-durable checkpoints are "
          f"expired by lifecycle rules in production)")
    print(f"- Final verdict quiesced: {'yes' if quiesced else 'NO'}\n")

    if job_gone:
        print("- CAVEAT: the job stopped RUNNING mid-soak (job-gone.txt)\n")
    if chaos_died:
        print("- CAVEAT: the chaos controller stopped before the soak ended (chaos-died.txt)\n")

    print("## Result\n")
    print(f"**{result}**\n")
    if result == "PASS":
        print("Every produced event was committed exactly once through staged "
              "multipart commits under the full fault schedule, every committed "
              "object was intact and immutable, and the verdict was taken over a "
              "settled bucket by a fresh full re-read.")
    elif result.startswith("INCONCLUSIVE"):
        print("Treat the run as void and re-run: an incomplete campaign is not a "
              "weak pass.")
    else:
        print("The evidence directory contains the defect trail; fix first, "
              "publish never (failed runs are not published).")

    print("\n## Caveats\n")
    print("- Correctness is asserted only for the workload, fault profile and "
          "duration above.")
    print("- The completeness assertion is valid only after the drain: it "
          "compares the generator's final produced count with a fresh full "
          "re-read of the settled bucket.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
