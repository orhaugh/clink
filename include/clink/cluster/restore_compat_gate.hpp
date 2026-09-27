#pragma once

#include <cstdint>
#include <optional>
#include <set>
#include <string>
#include <vector>

// Pre-deploy restore-compatibility gate (schema evolution D, part 2).
//
// When a job is submitted (or HA-recovered) with a restore configured,
// the coordinator can fail fast if the savepoint's stored state versions cannot be
// migrated to the versions the job binary expects - instead of letting
// the job deploy and then throw at worker start (which C, the restore-time
// migrator, would do anyway). This is the coordinator-side automation of
// `clink check-savepoint --expected=<job.so>`.
//
// BEST-EFFORT by design: it only ever BLOCKS on a definitive
// incompatibility verdict. If it cannot read the savepoint (remote/shared
// storage the coordinator doesn't mount, missing file), cannot load a .so, or no
// .so exports the check, it returns "" and lets the deploy proceed - C
// still guarantees correctness at restore time. The gate buys an earlier,
// clearer error when the coordinator happens to have what it needs.

namespace clink::cluster {

// Read the restore savepoint's stored version map (subtask 0's snapshot
// under <restore_from_dir>/0/checkpoint-<id>.snap) and ask each job .so,
// .so-side (where its StateMigrationRegistry lives), whether it can
// migrate that map to its expected versions.
//
// Returns a human-readable reject reason if a .so reports a DEFINITE
// incompatibility; "" if compatible, no restore is configured
// (restore_from_dir empty), the savepoint is unreadable, or no .so
// exports clink_job_check_restore_compatibility.
[[nodiscard]] std::string check_restore_compatibility_via_plugins(
    const std::vector<std::string>& plugin_so_paths,
    const std::string& restore_from_dir,
    std::uint64_t restore_checkpoint_id);

// Pre-deploy restore LAYOUT gate.
//
// State is restored by job-global subtask index: the task deployed at index i
// reads <restore_from_dir>/v<gen>/<i>/, and the planner hands indices out as one
// contiguous block per chain in graph order. A plan whose index set differs from
// the one that took the checkpoint therefore hands its operators each other's
// state, and nothing notices, because every file it reads is individually
// valid. The in-memory rescale paths translate each task through a restore
// directive (JobState::task_op_identity); a submit-time restore - a savepoint, an
// explicit checkpoint id, an HA recovery - has no record of the old layout
// beyond the participant set the checkpoint's COMPLETED marker carries, so that
// set is what this gate compares.
//
// The v0.8.0 -> v0.9.0 upgrade is the shape that found it: SQL scalar subqueries
// and null-aware IN / NOT IN now run as single-instance operators, so a savepoint
// a job took at parallelism 2 records 16 subtasks where the same job now plans
// 15, and a downstream GROUP BY came back without any of its groups.
//
// Same contract as the gate above: it only ever BLOCKS on a definite verdict.
// No marker for the checkpoint, more than one (a checkpoint root shared by
// several jobs), a marker without a participant set, or a restore_from_dir the
// coordinator cannot list all mean "cannot check", and the deploy proceeds as it
// did before the gate existed.

// The participant set recorded for `checkpoint_id` by
// <restore_from_dir>/_jobs/<job>/COMPLETED-<id>. nullopt when no single marker
// can be identified, the marker carries no participant set (it predates one, or
// the list is empty, which no completed checkpoint can be), or a token does not
// parse. The identification rule is the one the restore itself applies
// (completed_participants in src/state/state_backend_factory.cpp), so the gate
// and the restore cannot disagree about which marker describes the checkpoint.
//
// Deliberately no fallback to counting snapshot files when the marker carries no
// participant set. The file name is the state backend's business - only the file
// backend writes checkpoint-<id>.snap - and every build that writes the
// generation-namespaced layout this engine reads also records the participant
// set (the two landed in adjacent commits and first shipped together in v0.7.0),
// so a marker without one belongs to a directory this build cannot restore by
// index anyway (design record 007).
[[nodiscard]] std::optional<std::set<std::uint32_t>> recorded_restore_participants(
    const std::string& restore_from_dir, std::uint64_t checkpoint_id);

// The refusal for restoring a checkpoint whose marker recorded `recorded`
// subtasks into a plan deploying `planned`, or "" when the two sets are equal.
// Pure, so the message is testable without a cluster. It names the checkpoint,
// both counts and the remedy, and `single_instance_ops` - the plan's operators
// that run at parallelism 1 whatever the submitted parallelism - because after
// an engine upgrade those are the likely cause: an earlier version may have run
// them in parallel.
[[nodiscard]] std::string restore_layout_refusal(
    const std::string& restore_from_dir,
    std::uint64_t checkpoint_id,
    const std::set<std::uint32_t>& recorded,
    const std::set<std::uint32_t>& planned,
    const std::vector<std::string>& single_instance_ops);

// The two together, as the coordinator calls it before anything deploys: "" when
// no restore is configured, the recorded layout cannot be identified, or it
// matches `planned`; otherwise the refusal.
[[nodiscard]] std::string check_restore_layout(const std::string& restore_from_dir,
                                               std::uint64_t restore_checkpoint_id,
                                               const std::set<std::uint32_t>& planned,
                                               const std::vector<std::string>& single_instance_ops);

}  // namespace clink::cluster
