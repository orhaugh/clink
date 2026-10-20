#pragma once

// Where a job's state lives, and where a restore reads it from.
//
// A job's state is namespaced by the job:
//
//     <base>/_state/<job id>/v<generation>/<subtask index>/...
//
// <base> is the job's state_backend_uri, or its checkpoint_dir when it names
// none (the file backend in the checkpoint directory). Without the job
// segment, jobs given one base - the INSERT statements of one script, two
// submissions pointed at one directory, any jobs sharing a --state-backend
// URI - wrote and renamed the same files, and their checkpoints failed or,
// worse, restored each other's state. See
// docs/design/013-state-namespaced-by-job.md.
//
// The coordinator composes the root and sends it to the workers in place of
// the base, so no state backend, StateBackendSpec field or plugin changes: a
// backend composes v<generation>/<subtask index> under whatever location it is
// given, as it always has.
//
// A COMPLETED marker records the root its checkpoint was written to (its
// state= line), which is how a restore finds the state. A marker without the
// line was written before the job segment existed, and its checkpoint is
// restored from where state lived then: the base itself.

#include <cstdint>
#include <optional>
#include <set>
#include <string>
#include <string_view>

namespace clink::cluster {

using JobId = std::uint64_t;  // matches protocol.hpp without dragging it in

// The location a job's state is composed under: state_backend_uri, or
// checkpoint_dir when that is empty.
[[nodiscard]] std::string state_base_of(const std::string& checkpoint_dir,
                                        const std::string& state_backend_uri);

// <base>/_state/<job id>. The segment goes into the URI's location, before any
// ?query, and into its `local` parameter, the local working root the
// object-store schemes keep beside their remote prefix. A base that names no
// location (empty, memory://, changelog://, disagg-local://) comes back
// unchanged: it keeps nothing another job could share.
[[nodiscard]] std::string job_state_root(std::string_view base, JobId job_id);

// What a COMPLETED-<id> marker records. Every field is optional because
// markers written by older builds lack the later ones.
struct CompletedMarker {
    std::optional<JobId> job;
    std::optional<std::uint64_t> checkpoint;
    std::optional<std::uint32_t> generation;
    // The job-global indices that acked the checkpoint. Absent for a marker
    // that predates the participant set, or whose list does not parse.
    std::optional<std::set<std::uint32_t>> subtasks;
    // Where the checkpoint's state was written (recorded_state_location).
    // Absent for a marker written before state was namespaced by job.
    std::optional<std::string> state;
};

[[nodiscard]] CompletedMarker parse_completed_marker(std::string_view body);

[[nodiscard]] std::string completed_marker_body(JobId job,
                                                std::uint64_t checkpoint,
                                                std::uint32_t generation,
                                                const std::set<std::uint32_t>& subtasks,
                                                std::string_view state);

// The state= value for a job whose state root is `root`: "_state/<job id>",
// relative to the checkpoint directory, when the root is that directory's own
// (the file backend in the checkpoint directory), so a copied checkpoint
// directory restores from the copy; `root` itself otherwise.
[[nodiscard]] std::string recorded_state_location(const std::string& checkpoint_dir,
                                                  JobId job_id,
                                                  const std::string& root);

// Where a restore reads its state.
struct RestoreSource {
    // The location the state backends restore from (StateBackendSpec::restore_uri).
    std::string state_root;
    // The generation that wrote the checkpoint.
    std::uint32_t generation{1};
    std::uint64_t checkpoint_id{0};
    // The marker that named the checkpoint; empty when there was none.
    std::string marker;
    // The job whose marker it is.
    std::optional<JobId> job;
    // The participant set the marker records; absent when it records none, or
    // when several jobs' markers name the checkpoint.
    std::optional<std::set<std::uint32_t>> participants;
    // The state is where it lived before state was namespaced by job: no
    // marker names the checkpoint, or its marker has no state= line.
    bool legacy{false};
};

// Resolve a restore of checkpoint `checkpoint_id` from `restore_dir`, which is
// either a job's marker directory (<checkpoint dir>/_jobs/<job id>, holding
// COMPLETED-<id> itself) or a checkpoint directory in which one job holds
// _jobs/<job id>/COMPLETED-<id>.
//
// A marker with a state= line names the state root. A marker without one, or
// no marker at all, is a checkpoint written before state was namespaced by
// job: its state is at `legacy_base`, or, when that is empty, at the
// checkpoint directory itself. The generation is the marker's, or the highest
// v<N> under the root when the marker records none.
//
// Throws when several jobs hold COMPLETED-<id> in a checkpoint directory and
// any of them recorded its state root: which job is meant is the caller's to
// say, by naming that job's marker directory. Where every one of them is an
// older marker, their state shares one location and the restore reads it as
// it always did. Also throws when a store cannot be read: a restore must not
// guess where its state is.
//
// A restore_dir that is not a checkpoint directory this process can open (a
// state backend URI) is restored from as given.
[[nodiscard]] RestoreSource resolve_restore_source(const std::string& restore_dir,
                                                   std::uint64_t checkpoint_id,
                                                   const std::string& legacy_base = {});

// The highest generation directory (v<N>) under a local state root, or 1 when
// there is none or the root is not a local path.
[[nodiscard]] std::uint32_t highest_generation_in(const std::string& root);

}  // namespace clink::cluster
