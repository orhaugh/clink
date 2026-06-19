#pragma once

#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

#include "clink/state/state_backend.hpp"

namespace clink {

// Description of where a subtask's state lives. The factory consumes
// this and returns a backend ready for the operator runner.
//
// Fields:
//   * uri            - working location for this run. Schemes:
//                        ""               -> in-memory only
//                        "memory://"      -> in-memory only
//                        "/abs/path" or
//                        "file:///path"   -> on-disk under <path>/<subtask>
//                        "s3://..."       -> S3-backed (when registered)
//                      A bare path is interpreted as a file URI to keep the
//                      existing tests/CheckpointConfig.checkpoint_dir
//                      string contract working without modification.
//   * subtask_idx    - per-job global subtask index; the factory mints
//                      per-subtask sub-paths under `uri` so two subtasks on
//                      the same machine don't trample one another.
//   * restore_uri    - optional source location to restore from. Same
//                      scheme rules as `uri`. Empty = fresh start.
//   * restore_checkpoint_id - which checkpoint id under `restore_uri` to
//                      load. Ignored when `restore_uri` is empty.
struct StateBackendSpec {
    std::string uri;
    std::uint32_t subtask_idx{0};
    std::string restore_uri;
    std::uint64_t restore_checkpoint_id{0};
    // Rescale knob: when the parent old subtask whose state file this
    // new subtask should read differs from `subtask_idx`, set this to
    // the old idx. The file backend stages
    // <restore_uri>/<restore_from_subtask_idx>/checkpoint-<id>.snap
    // (not <subtask_idx>) into the new subtask's working dir. The
    // default UINT32_MAX means "read from own subtask_idx" - the
    // non-rescale path.
    //
    // For scale-down (old_p > new_p), one new subtask consumes the
    // state of `restore_from_parent_count` contiguous parents starting
    // at restore_from_subtask_idx. The factory concatenates their
    // snapshot files into the new subtask's working dir so a single
    // restore() loads all assigned keys. Default 1 keeps the single-
    // parent (scale-up / non-rescale) semantics.
    std::uint32_t restore_from_subtask_idx{std::numeric_limits<std::uint32_t>::max()};
    std::uint32_t restore_from_parent_count{1};
};

// What the factory returns. The caller is expected to install
// `backend` on JobConfig.state_backend and forward `restore_from` (when
// present) to JobConfig.restore_from so LocalExecutor calls
// backend->restore() once before any operator runs.
struct BuiltStateBackend {
    std::shared_ptr<StateBackend> backend;
    std::optional<Snapshot> restore_from;
};

// Process-wide registry that maps a URI scheme to a builder closure.
// Built-in schemes ("memory", "file") are pre-registered on first
// access of default_instance(). To add a third backend (S3, Azure,
// GCS, ...) call register_scheme("s3", ...) at program startup.
class StateBackendFactory {
public:
    using Builder = std::function<BuiltStateBackend(const StateBackendSpec&)>;

    static StateBackendFactory& default_instance();

    // Idempotent: replaces any prior builder for `scheme`. Plugin code
    // that wants to override a built-in (e.g. swap the file backend
    // for a checksumming variant) can re-register at startup.
    void register_scheme(std::string scheme, Builder builder);

    // Look up the builder for spec.uri's scheme and run it. A bare
    // path (no "://") is treated as "file://<path>"; empty uri uses
    // "memory://".
    BuiltStateBackend build(const StateBackendSpec& spec) const;

    // True if the scheme has a registered builder.
    bool has_scheme(const std::string& scheme) const;

private:
    StateBackendFactory();

    mutable std::mutex mu_;
    std::unordered_map<std::string, Builder> builders_;
};

}  // namespace clink
