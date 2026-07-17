#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/protocol.hpp"
#include "clink/state/state_backend.hpp"

namespace spdlog {
class logger;
}

namespace clink {
class StateBackendFactory;
class MetricsRegistry;
}  // namespace clink

namespace clink::cluster {

struct OperatorChainSpec;

// How records are distributed across the edges of one output group.
// Defined here (rather than in job_planner.hpp) so type_registry.hpp's
// chain-dispatch closures can use the enum values without pulling in
// the planner header. job_planner.hpp re-imports the symbol.
enum class RoutingMode : std::uint8_t {
    // 1:1 forward. The group has exactly one edge per upstream subtask,
    // pointing at the same-indexed downstream subtask. Records go to
    // the single peer, watermarks/barriers naturally do too.
    Forward = 0,
    // Round-robin across N downstream subtasks. Each upstream subtask
    // has all N edges in this group and cycles through them per
    // record. Used when upstream.parallelism != downstream.parallelism
    // (cross-parallelism rebalance).
    Rebalance = 1,
    // Hash partitioning. For each record, the upstream subtask calls
    // the named key extractor (resolved against KeyExtractorRegistry,
    // typed on the upstream's out_channel) and routes to peer
    // `hash(key) % N`. Same key always lands on the same downstream
    // subtask, which is what makes keyed state correct at parallelism
    // > 1. Watermarks and barriers broadcast to every peer.
    Hash = 2,
};

// ResolvedOutputGroup is the role-side view of one downstream consumer
// group after PeerUpdate has resolved peer addresses. Each
// SubtaskRunner builds typed NetworkBridgeSink<T> instances per peer
// in each group.
struct ResolvedOutputGroup {
    RoutingMode mode;
    std::vector<PeerAddress> peers;
    // Set when mode == Hash. Looked up against KeyExtractorRegistry by
    // (upstream_channel_type, key_extractor_fn) at runtime.
    std::string key_extractor_fn;
    // Set for named side outputs; empty for the main output group.
    // The runner uses this with SideOutputAttacherRegistry to wire
    // a typed network sink reading from the side channel.
    std::string side_output_tag;
    // Channel type for every peer in this group (they all carry the
    // same payload type). Copied from the underlying SubtaskEdge so
    // the side-output attacher can look up the right TypeOps.
    std::string channel_type;
};

// What a SubtaskRunner receives from the generic role on the worker. The
// runner knows the concrete C++ type T (captured at registration
// time); it casts in_bridges back to NetworkBridgeSource<T> via
// static_pointer_cast and looks up T's TypeOps via TypeRegistry to
// build the output side.
struct RunnerContext {
    // One per chain.input_edges entry, in the same order. Each is a
    // shared_ptr<void> bound to NetworkBridgeSource<T> for the right
    // T (provided by the planner's edge metadata).
    std::vector<std::shared_ptr<void>> in_bridges;
    // One per chain.output_groups entry, in the same order. Peers
    // are concrete (host, port) tuples post-PeerUpdate.
    std::vector<ResolvedOutputGroup> output_groups;
    // The whole chain spec; the runner pulls out op params, output
    // routing mode, selector fn name, channel names, etc.
    const OperatorChainSpec& chain;
    // Distributed-checkpointing parameters from DeployMsg. Empty/zero
    // means "no checkpointing" - the runner falls back to an in-memory
    // backend with no persistence. When checkpoint_dir is set the
    // runner wires a FileBackedStateBackend rooted at
    // `<checkpoint_dir>/<subtask_idx>` so each subtask owns a private
    // snapshot directory.
    std::string checkpoint_dir;
    std::string restore_from_dir;
    std::uint64_t restore_from_checkpoint_id{0};
    // Record-capture flight recorder (echoed from DeployMsg via the worker's
    // per-job checkpoint state); empty = capture off. Copied onto
    // JobConfig by make_subtask_job_config.
    std::string capture_dir;
    std::uint64_t capture_records{0};
    // Per-subtask state-backend URI, decoupled from checkpoint_dir. When
    // non-empty, make_subtask_job_config uses it as the StateBackendSpec
    // uri (so the factory builds a remote/disaggregated backend) and
    // checkpoint_dir stays the local coordination directory. Empty keeps
    // the legacy behaviour where checkpoint_dir is the backend URI.
    std::string state_backend_uri;
    // The HOST's StateBackendFactory (clink_node's process-wide singleton),
    // which has the dynamically-registered schemes (remote-read://, rocksdb://,
    // s3+rocksdb://) installed by install_linked_impls(). The runner builds its
    // backend through THIS rather than the .so-local default_instance(): a
    // dlopen'd plugin (RTLD_LOCAL + static clink_core) has its OWN factory
    // singleton holding only the ctor builtins (memory/file/changelog), so a
    // dynamically-registered scheme would otherwise report "no builder". nullptr
    // falls back to the local default_instance() (in-process / legacy paths).
    StateBackendFactory* state_backend_factory{nullptr};
    // The HOST's spdlog logger and metrics registry, captured in the clink_node
    // TU and carried across the dlopen boundary by data. Same rationale as
    // state_backend_factory above: make_subtask_job_config runs inside the
    // plugin .so on the SubtaskRunner dispatch path, where
    // clink::logging::host_logger() / MetricsRegistry::global() would resolve
    // the .so's OWN private singletons (RTLD_LOCAL + static clink_core), not
    // the node's. Setting these host-side and copying them onto JobConfig means
    // an operator's logs reach the node's sinks (/api/v1/logs) and its gauges
    // reach the node's /metrics. nullptr falls back to the in-process globals
    // (correct for same-address-space LocalExecutor / legacy paths).
    spdlog::logger* logger{nullptr};
    MetricsRegistry* metrics{nullptr};
    // Per-job alignment policy. false = aligned (default); true =
    // unaligned (barriers overtake in-flight records at multi-input
    // operators). Propagated through the wire from CheckpointConfig
    // and copied to JobConfig.unaligned_checkpoints by the plugin
    // runner.
    bool unaligned_checkpoints{false};
    // Packed expected state-version map (schema evolution) carried from
    // DeployMsg. Empty means "no declared versions" - the subtask
    // restores state verbatim. When non-empty, make_subtask_job_config
    // unpacks it onto JobConfig.expected_state_versions so the
    // LocalExecutor auto-migrates restored state to the declared schema.
    std::string expected_state_versions_packed;
    // Rescale-only restore overrides. restore_from_subtask_idx names
    // the parent old subtask whose state file this new subtask should
    // read (kRestoreFromSelf means "read own subtask_idx"). When
    // scale-down assigns multiple parents to one new subtask, the
    // contiguous range [restore_from_subtask_idx,
    // restore_from_subtask_idx + restore_from_parent_count) names them
    // all and the factory concatenates their state files. kg_filter
    // narrows the merged load to this new subtask's assigned slice;
    // the default {0, kNumKeyGroups} loads every group (the back-compat
    // restore-on-resubmit path).
    std::uint32_t restore_from_subtask_idx{kRestoreFromSelf};
    std::uint32_t restore_from_parent_count{1};
    KeyGroupRange restore_key_group_filter{};
    // Callback the runner invokes after each successful state snapshot
    // (per checkpoint id). The worker uses this to send SubtaskCheckpointed
    // back to the coordinator.
    std::function<void(std::uint64_t /*checkpoint_id*/, bool /*ok*/, std::string /*error*/)>
        on_checkpoint_ack;
    // Bounded-source EOS final-checkpoint hooks (see RuntimeContext /
    // JobConfig). request_final_checkpoint asks the coordinator for a final coordinated
    // checkpoint id (0 = declined); wait_final_committed blocks until this worker
    // observes CommitCheckpoint for that id. Empty for non-cluster runs.
    std::function<std::uint64_t()> request_final_checkpoint;
    std::function<bool(std::uint64_t, std::chrono::milliseconds)> wait_final_committed;
    // Source-side barrier injectors the runner hands to the worker before
    // it starts running. For source subtasks the vector has one entry
    // (the injector for the source's outbound channel). For non-source
    // subtasks the vector is empty - they receive barriers from
    // upstream over the wire. The worker stashes these in per_job_injectors_
    // so it can answer TriggerCheckpoint by pushing barriers into
    // every hosted source for the targeted job.
    using SourceInjectorFn = std::function<void(clink::CheckpointBarrier)>;
    std::function<void(std::vector<SourceInjectorFn>)> register_source_injectors;

    // 2PC sink commit callbacks. Sink subtasks register one callback
    // here at startup; the worker stashes them in per_job_committers_ and
    // invokes them when CommitCheckpoint arrives from the coordinator. Non-sink
    // subtasks leave the vector empty. nullptr/empty register call in
    // legacy/in-process paths is a no-op.
    using CommitCheckpointFn = std::function<void(std::uint64_t /*checkpoint_id*/)>;
    std::function<void(std::vector<CommitCheckpointFn>)> register_commit_callbacks;

    // Abort callbacks. Sink subtasks that pre-commit on a
    // CheckpointBarrier register a paired abort callback here; the worker
    // dispatches it on AbortCheckpoint so the sink can roll back its
    // prepared state (file_2pc removes staging file, kafka_2pc calls
    // abort_transaction). Same signature shape as the commit hook.
    using AbortCheckpointFn = std::function<void(std::uint64_t /*checkpoint_id*/)>;
    std::function<void(std::vector<AbortCheckpointFn>)> register_abort_callbacks;

    // Drain callbacks. Subtask runners that participate in
    // adaptive rescaling register one or more callbacks here. The worker
    // dispatches them on BeginRescale arriving for the operator this
    // subtask belongs to: the callback runs the drain choreography
    // (finish current barrier alignment, emit DrainMarker downstream,
    // close output channels). The target_parallelism argument is the
    // new parallelism count the rescale is moving towards; the
    // callback embeds it in the DrainMarker so downstream consumers
    // know the new subtask count.
    using DrainFn = std::function<void(std::uint32_t /*target_parallelism*/)>;
    std::function<void(std::vector<DrainFn>)> register_drain_callbacks;

    // Checkpoint-retention hook. make_subtask_job_config calls this once
    // with the subtask's freshly-built state backend so the worker can purge
    // that backend's superseded checkpoint artefacts when a newer
    // checkpoint completes (see CheckpointRetention). nullptr in
    // legacy/in-process paths is a no-op.
    std::function<void(std::shared_ptr<StateBackend>)> register_checkpoint_backend;

    // Shared cancellation flag the Worker owns; flipped to true by
    // the CancelJob handler. The runner closure threads this into the
    // JobConfig it hands LocalExecutor, so the executor's stop
    // predicate sees the flip without holding a reference to anything
    // on the runner's stack. nullptr in legacy/in-process paths that
    // don't go through the Worker.
    std::shared_ptr<std::atomic<bool>> cancel_token;

    // The DeploymentTask role this runner executes as - the same string
    // the coordinator tracks in task_records and targets when routing
    // queryable-state lookups. Threaded (with chain.subtask_idx) through
    // JobConfig into each RuntimeContext so operators can bind state
    // under the exact (role, subtask) slot external clients address.
    // Empty in in-process / legacy paths (operators skip binding).
    std::string runner_role;
};

// A SubtaskRunner is a type-erased closure that knows the concrete C++
// type(s) it operates on, constructs the typed Source/Operator/Sink
// via the user's factory, builds typed bridges via TypeRegistry, and
// runs the resulting Dag via LocalExecutor. Throws on errors; the
// worker's run_task_ converts to SubtaskFinished{had_error=true}.
using SubtaskRunner = std::function<void(const RunnerContext&)>;

// RunnerRegistry stores SubtaskRunner closures keyed by op-type name
// and channel-name(s). The keys mirror the lookup the planner does
// during validation and the role does during dispatch.
//
// Lookups optionally fall through to a `parent` registry on miss. The
// process-wide built-ins live in `default_instance()`; per-job registries
// (created by the coordinator/worker JobBundle) set parent=&default_instance() so
// they layer plugin/inline-lambda registrations on top of built-ins
// without duplicating them. `register_*` ALWAYS writes into the
// receiver, never the parent - this is one-way overlay.
class RunnerRegistry {
public:
    RunnerRegistry() = default;
    explicit RunnerRegistry(const RunnerRegistry* parent) : parent_(parent) {}

    // Sources have only an output channel.
    void register_source(std::string op_type, std::string out_channel, SubtaskRunner runner);
    // Operators bridge two channels.
    void register_operator(std::string op_type,
                           std::string in_channel,
                           std::string out_channel,
                           SubtaskRunner runner);
    // Sinks have only an input channel.
    void register_sink(std::string op_type, std::string in_channel, SubtaskRunner runner);
    // Joins: two typed inputs and one typed output.
    void register_join(std::string op_type,
                       std::string in1_channel,
                       std::string in2_channel,
                       std::string out_channel,
                       SubtaskRunner runner);
    // CoOperators (CoProcessFunction): two typed inputs and one
    // typed output. Same key shape as joins; separate map so the lookup
    // path can disambiguate which operator kind got registered.
    void register_co_operator(std::string op_type,
                              std::string in1_channel,
                              std::string in2_channel,
                              std::string out_channel,
                              SubtaskRunner runner);

    // Lookups return nullptr on miss.
    const SubtaskRunner* find_source(const std::string& op_type,
                                     const std::string& out_channel) const;
    const SubtaskRunner* find_operator(const std::string& op_type,
                                       const std::string& in_channel,
                                       const std::string& out_channel) const;
    const SubtaskRunner* find_sink(const std::string& op_type, const std::string& in_channel) const;
    const SubtaskRunner* find_join(const std::string& op_type,
                                   const std::string& in1_channel,
                                   const std::string& in2_channel,
                                   const std::string& out_channel) const;
    const SubtaskRunner* find_co_operator(const std::string& op_type,
                                          const std::string& in1_channel,
                                          const std::string& in2_channel,
                                          const std::string& out_channel) const;

    // Type-only checks. The planner uses these to classify an
    // OperatorSpec by op_type before the (in1, in2, out) channels
    // are known per-edge - equivalent to "does ANY join with this
    // type name exist in the registry (or its parent)?". Cheaper
    // than a full find_* call and lets plugins register joins/co-ops
    // without amending a hardcoded planner string match.
    [[nodiscard]] bool has_join_for_type(const std::string& op_type) const;
    [[nodiscard]] bool has_co_operator_for_type(const std::string& op_type) const;

    static RunnerRegistry& default_instance();

private:
    struct SourceKey {
        std::string type;
        std::string out;
        bool operator==(const SourceKey&) const = default;
    };
    struct OpKey {
        std::string type;
        std::string in;
        std::string out;
        bool operator==(const OpKey&) const = default;
    };
    struct SinkKey {
        std::string type;
        std::string in;
        bool operator==(const SinkKey&) const = default;
    };
    struct JoinKey {
        std::string type;
        std::string in1;
        std::string in2;
        std::string out;
        bool operator==(const JoinKey&) const = default;
    };
    struct StringTupleHash {
        std::size_t mix(std::size_t h, const std::string& s) const noexcept {
            return h ^ (std::hash<std::string>{}(s) + 0x9e3779b9 + (h << 6) + (h >> 2));
        }
        std::size_t operator()(const SourceKey& k) const noexcept {
            return mix(std::hash<std::string>{}(k.type), k.out);
        }
        std::size_t operator()(const OpKey& k) const noexcept {
            return mix(mix(std::hash<std::string>{}(k.type), k.in), k.out);
        }
        std::size_t operator()(const SinkKey& k) const noexcept {
            return mix(std::hash<std::string>{}(k.type), k.in);
        }
        std::size_t operator()(const JoinKey& k) const noexcept {
            return mix(mix(mix(std::hash<std::string>{}(k.type), k.in1), k.in2), k.out);
        }
    };

    mutable std::mutex mu_;
    std::unordered_map<SourceKey, SubtaskRunner, StringTupleHash> sources_;
    std::unordered_map<OpKey, SubtaskRunner, StringTupleHash> operators_;
    std::unordered_map<SinkKey, SubtaskRunner, StringTupleHash> sinks_;
    std::unordered_map<JoinKey, SubtaskRunner, StringTupleHash> joins_;
    std::unordered_map<JoinKey, SubtaskRunner, StringTupleHash> co_operators_;
    const RunnerRegistry* parent_{nullptr};
};

}  // namespace clink::cluster
