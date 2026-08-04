#pragma once

// Reject a configuration that cannot do what it says, before it runs.
//
// Every check below is a setting that is currently accepted and then
// SILENTLY IGNORED, or a pair of settings that contradict each other. Not
// one is a style preference, and not one is a threshold picked by taste -
// each cites the line of engine code that makes it true, because a linter
// built on guesswork produces the mode='cdc' failure: it refuses things
// that work, and people learn to switch it off.
//
// This complements two gates that already exist and does not overlap them:
//
//   * the delivery-guarantee analyser (guarantee_gate.hpp) answers "what
//     guarantee does this pipeline provide", reasoning about connectors;
//   * the bounded-state validator (sql/bounded_state.hpp) answers "will
//     this query's state grow without limit".
//
// Neither notices that `--checkpoint-interval-ms=500` with no
// `--checkpoint-dir` produces no checkpoints at all.

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/protocol.hpp"

namespace clink::cluster {

enum class LintSeverity {
    // The configuration asks for something that will not happen. Rejected
    // at submission: an operator who set a flag and had it ignored is
    // worse off than one who was told the flag does not apply.
    Error,
    // Legitimate in some deployments, surprising in most. Logged.
    Warning,
};

struct ConfigProblem {
    LintSeverity severity{LintSeverity::Warning};
    std::string setting;  // the flag or field at fault
    std::string message;

    [[nodiscard]] bool is_error() const noexcept { return severity == LintSeverity::Error; }
};

// Lint one job's checkpoint/recovery configuration.
//
// Pure: it reads the config and nothing else, so it can run in a CLI, in a
// test, or at submission without a cluster.
[[nodiscard]] std::vector<ConfigProblem> lint_checkpoint_config(const CheckpointConfig& c);

// Lint one job's GRAPH: the per-operator settings, and the pairs of settings
// that contradict each other across the graph and the checkpoint config.
//
// Separate from lint_checkpoint_config because it needs the graph, and because
// the two answer different questions - that one is about whether recovery will
// happen at all, this one is about whether the operators as declared can do what
// they say.
//
// Why this exists at all: F38 was a key-group-versus-parallelism defect that ran
// for a long time writing keyed state which was silently discarded at restore.
// The relationship it got wrong is a property of the graph and is checkable
// before a single record moves.
//
// `available_slots` is the total free slot capacity of the cluster, when the
// caller knows it. Left unset in the CLI, where there is no cluster to ask.
//
// Pure, like the rest of this header: no cluster, no filesystem.
[[nodiscard]] std::vector<ConfigProblem> lint_job_graph(
    const JobGraphSpec& graph,
    const CheckpointConfig& checkpoint,
    std::optional<std::uint32_t> available_slots = std::nullopt);

// Lint the cluster-level liveness settings. Separate from the job config
// because these belong to the coordinator process, not to a submission.
[[nodiscard]] std::vector<ConfigProblem> lint_liveness_config(std::int64_t heartbeat_interval_ms,
                                                              std::int64_t heartbeat_timeout_ms,
                                                              std::int64_t watchdog_interval_ms);

// Render for a human. One line per problem, errors first.
[[nodiscard]] std::string render_problems(const std::vector<ConfigProblem>& problems);

// Non-empty when at least one Error was found: the caller turns it into a
// submission rejection. Mirrors check_delivery_guarantee's shape so the two
// gates are used the same way at the call site.
[[nodiscard]] std::string check_config(const CheckpointConfig& c,
                                       std::vector<ConfigProblem>* out_problems = nullptr);

// --- profiles ------------------------------------------------------------
//
// A profile is a coherent SET of recovery defaults. It exists because the
// individual knobs are independently settable and most of the dangerous
// states in this file are combinations - so "which six flags do I need for
// a durable job" is a question an operator should not have to answer from
// first principles.
//
// A profile only fills in what the submitter LEFT ALONE. It never
// overrides an explicit choice: silently rewriting a flag someone set is
// the same class of failure as ignoring it.
enum class ConfigProfile {
    // No checkpointing, fail fast. The right default for iterating on a
    // job locally, and honest about giving no recovery.
    Development,
    // Checkpointing on, self-healing restarts, and a durable backend
    // REQUIRED - selecting this profile with a memory backend is an error
    // rather than a silent downgrade, because the whole point of naming it
    // is to get the guarantees that go with the name.
    Production,
};

[[nodiscard]] const char* to_string(ConfigProfile p) noexcept;
[[nodiscard]] std::optional<ConfigProfile> profile_from_string(std::string_view s);

// Apply `p`'s defaults to `c`, leaving every explicitly-set field alone.
// `explicit_checkpoint_dir` / `explicit_interval` tell the function which
// fields the submitter actually supplied - a zero interval is
// indistinguishable from an unset one otherwise, and guessing wrong here
// would either ignore an explicit "no periodic checkpoints" or invent one.
void apply_profile(ConfigProfile p,
                   CheckpointConfig& c,
                   bool explicit_checkpoint_dir,
                   bool explicit_interval);

// Errors specific to a profile's promises, on top of lint_checkpoint_config.
[[nodiscard]] std::vector<ConfigProblem> lint_profile(ConfigProfile p, const CheckpointConfig& c);

}  // namespace clink::cluster
