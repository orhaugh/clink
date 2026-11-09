#pragma once

// The replay checks of an incident bundle (design record 014), on the replay
// engine `clink replay` uses (EpochReplay). They live with the SQL frontend
// because replay rebuilds Row-channel operators; `clink incident-verify`
// reports them unsupported in a build without it.
//
// replay.determinism   two replays of a captured epoch emit the same rows in
//                      the same order. Determinism only: it says nothing about
//                      what production emitted.
// replay.recorded_output
//                      the replay's rows equal the rows production handed the
//                      operator's single downstream operator in the same
//                      epoch, read from that operator's own capture. An epoch
//                      whose topology does not make those the same stream (one
//                      consumer, one input, one subtask on both sides) is
//                      unsupported; one whose capture, or its consumer's, is
//                      missing or truncated is unavailable.
// replay.expected_output
//                      the selected epochs of one operator's subtask, each
//                      replayed from its own starting state, emit an
//                      independent expectation's rows, given as NDJSON and
//                      compared as multisets (an expectation need not know
//                      which epoch a row fell in, nor the order within one).
//
// The selection op, subtask and epoch name is the scope every check answers
// for; max_replays bounds how many of its epochs one call replays, and the
// epochs past it are counted as omitted, never dropped from the scope. Each
// check reports the selection's counts (incident::SelectionTally) and concludes
// from them: failed when any epoch failed, whatever the rest show; passed only
// when every selected epoch was checked whole and passed; otherwise
// unavailable when an epoch was omitted or lacked evidence (a truncated capture
// lacks the records past its cap), and unsupported when the only gap is epochs
// this build cannot replay or compare.
//
// No check writes outside the system's temporary directory or contacts a
// service: a replay rebuilds the operator from the binary's own registry,
// restores the bundle's snapshot into an in-memory backend and drives the
// captured events.

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "clink/incident/bundle.hpp"

namespace clink::sql {

struct IncidentReplayOptions {
    std::optional<std::uint64_t> op;
    std::optional<std::uint64_t> subtask;
    std::optional<std::uint64_t> epoch;
    std::string expected_path;  // NDJSON expected rows; needs one selected operator subtask
    std::size_t max_replays{64};
};

[[nodiscard]] std::vector<incident::Check> incident_replay_checks(
    const incident::Bundle& bundle, const IncidentReplayOptions& options);

// Freeze one captured epoch of the bundle as a replay regression bundle
// (emit_replay_regression) at `out_dir`, then run it once
// (run_replay_regression) so a bundle that does not pass is never handed
// back. Returns the golden emission count; throws with the reason.
std::size_t incident_regress(const incident::Bundle& bundle,
                             std::uint64_t op,
                             std::uint64_t epoch,
                             std::optional<std::uint64_t> subtask,
                             const std::string& out_dir);

}  // namespace clink::sql
