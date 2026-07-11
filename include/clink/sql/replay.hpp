#pragma once

// clink::sql::EpochReplay - the deterministic epoch-replay engine behind
// `clink replay`, as a library: load a captured operator's epoch (event
// stream + op.json sidecar + checkpoint state), then run() it any number
// of times - each run rebuilds a fresh operator over a freshly restored
// backend, puts its TimerService on a manual clock, and feeds the events
// through the production paths. Two run() calls are byte-identical (the
// determinism contract: docs/internals/replay-determinism.md).
//
// Used by the CLI (single-op and whole-job replay, --verify), and by
// tests that turn captured incidents into assertions.

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "clink/runtime/record_capture.hpp"
#include "clink/sql/row.hpp"

namespace clink::cluster {
struct OperatorFactory;
}

namespace clink::sql {

struct ReplayRequest {
    std::string capture_dir;
    // Checkpoint root for the starting state; may stay empty when the
    // resolved epoch starts from fresh state (epoch 1 / state_from 0).
    std::string checkpoint_dir;
    std::uint64_t op_id{0};
    // Subtask to replay; nullopt auto-resolves when the op has exactly one.
    std::optional<std::uint64_t> subtask;
    // "N" (state from checkpoint N-1) or "final" (requires state_from).
    std::string epoch;
    // Explicit starting checkpoint id, overriding the epoch-1 derivation.
    std::optional<std::uint64_t> state_from;
    // Run the operator's end-of-stream flush after the events.
    bool flush{false};
};

struct ReplayInfo {
    capture::OpSpecSidecar spec;
    std::uint64_t op_id{0};
    std::uint64_t subtask{0};
    std::string capture_path;
    std::uint32_t format_version{0};
    std::uint64_t records_seen{0};
    bool truncated{false};
    std::size_t data_count{0};
    std::size_t watermark_count{0};
    std::size_t clock_count{0};
    std::string state_desc;           // "fresh state" or the snapshot path
    std::string state_snapshot_path;  // resolved snapshot file; empty = fresh
};

class EpochReplay {
public:
    // Resolve and load everything a run needs. Throws std::runtime_error
    // with a actionable message on any resolution failure (missing
    // capture, unsupported channel, unknown factory, missing snapshot).
    static EpochReplay load(const ReplayRequest& request);

    [[nodiscard]] const ReplayInfo& info() const noexcept { return info_; }

    // One complete deterministic replay: rendered emissions in order
    // (changelog kinds prefixed, like the print sink).
    [[nodiscard]] std::vector<std::string> run() const;

private:
    EpochReplay() = default;

    ReplayInfo info_;
    std::vector<capture::CaptureEvent<Row>> events_;
    std::optional<std::vector<std::byte>> snap_bytes_;
    std::uint64_t state_id_{0};
    bool flush_{false};
    const cluster::OperatorFactory* factory_{nullptr};
};

// Every (op, subtask) with a capture directory under `capture_dir`,
// sorted by op id then subtask - the whole-job enumeration.
struct CapturedOp {
    std::uint64_t op_id{0};
    std::uint64_t subtask{0};
};
std::vector<CapturedOp> list_captured_ops(const std::string& capture_dir);

// ---- Regression bundles (incident -> test in one command) -------------
//
// emit_replay_regression materialises a SELF-CONTAINED bundle under
// `out_dir`: the op's capture (op.json + the epoch file), the starting
// snapshot, the golden emissions of the replay as of NOW, a
// bundle.json manifest, and a generated gtest source whose body is one
// call to run_replay_regression - so the emitted test never drifts
// from the library. Returns the golden emission count.
//
//   <out_dir>/bundle.json                      manifest (op, subtask, epoch, ...)
//   <out_dir>/capture/op-<id>/subtask-<s>/...  the epoch + sidecar, copied
//   <out_dir>/state/<s>/checkpoint-<N>.snap    the starting state (if any)
//   <out_dir>/golden.ndjson                    the expected emissions
//   <out_dir>/replay_regression_test.cpp       add to a test target linking clink::sql
std::size_t emit_replay_regression(const ReplayRequest& request, const std::string& out_dir);

// Replay the bundle at `bundle_dir` and compare to its golden file.
// Returns an empty string on success, otherwise a human-readable
// failure (first divergence / count mismatch / load error). The body
// of every generated regression test.
std::string run_replay_regression(const std::string& bundle_dir);

// Render one emitted Row like the print sink does (changelog kinds
// prefixed, the marker stripped).
std::string format_replay_row(const Row& row);

}  // namespace clink::sql
