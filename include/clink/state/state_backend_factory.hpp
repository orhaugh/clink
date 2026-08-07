#pragma once

#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
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
    // TOPOLOGY GENERATION. State lives under <base>/v<generation>/<subtask idx>,
    // because the job-global subtask index is NOT stable across a topology change:
    // the planner allocates one contiguous block of indices per operator in graph
    // order, so resizing one operator moves every later operator's block and a
    // directory silently changes owner.
    //
    // Four defects came from addressing state by that index alone - F38 (wrong
    // arithmetic), F59 (writing through it), F63 (skipping the translation on a
    // restart) and F65 (a new topology writing into the old one's directories).
    // Namespacing by generation makes each generation's files immutable with
    // respect to every other, which is the invariant all four break. See
    // docs/design/state-generations.md.
    //
    // `generation` is where this subtask WRITES. `restore_generation` is the one
    // that produced the checkpoint it reads, which differs exactly when a restore
    // crosses a rescale. Both default to 1, the generation of an initial deploy,
    // so a job that never rescales sees <base>/v1/<idx> for its whole life.
    std::uint32_t generation{1};
    std::uint32_t restore_generation{1};
};

// The ONE place a state path is composed, so eleven call sites across six backends
// cannot drift apart on it.
//
//     <base>/v<generation>/<subtask index>
//
// Every backend keeps its own file naming inside that directory; what has to be
// identical everywhere is the namespace, because a backend that forgets the
// generation writes into another generation's state and reintroduces F65.
[[nodiscard]] inline std::string state_dir_for(std::string_view base,
                                               std::uint32_t generation,
                                               std::uint32_t subtask_idx) {
    return std::string{base} + "/v" + std::to_string(generation) + "/" +
           std::to_string(subtask_idx);
}

// The same namespace as an object-store PREFIX rather than a filesystem path: no
// leading separator, always a trailing one, so callers append an object name.
[[nodiscard]] inline std::string state_prefix_for(std::string_view base,
                                                  std::uint32_t generation,
                                                  std::uint32_t subtask_idx) {
    std::string out{base};
    if (!out.empty() && out.back() != '/') {
        out.push_back('/');
    }
    out += "v" + std::to_string(generation) + "/" + std::to_string(subtask_idx) + "/";
    return out;
}

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
