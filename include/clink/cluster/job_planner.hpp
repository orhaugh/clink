#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/job_manager.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/runner_registry.hpp"  // RoutingMode (re-imported here)

namespace clink::cluster {

// Sentinel role used by every subtask in a submitted job. The TM auto-
// registers a single handler under this role (the generic subtask role)
// that dispatches through the OperatorRegistry from the per-task
// OperatorChainSpec embedded in extra_config.
inline constexpr const char* kGenericSubtaskRole = "__clink_subtask";

// Description of how one op behaves in a chain, used by the generic
// subtask role on the TM. v1: each subtask hosts exactly one op (no
// in-process operator chaining yet), so a chain has length 1. The format
// is forward-compatible with multi-op chains: just extend the encoding to
// list multiple ops in the same subtask.
enum class OperatorKind : std::uint8_t {
    Source,
    Operator,
    Sink,
    // Join: two heterogeneous inputs (with potentially different
    // channel types) combined via Dag::interval_join into one output
    // stream of a third type. The OperatorChainSpec's `ops[0]` carries
    // the join's type (registry key) and the planner stores the two
    // input channel types in input_edges directly.
    Join,
    // CoOperator: the CoProcessFunction.
    // Two typed inputs dispatched element-wise through process_element1
    // / process_element2 on a user CoOperator subclass. Same input-edge
    // shape as Join; the runner is looked up in
    // RunnerRegistry::find_co_operator rather than find_join.
    CoOperator,
};

// Per-tag side output declaration carried over the wire to the TM. The
// plugin runner uses TypeRegistry to find TypeOps for `channel_type`
// and wire a typed network sink for the side channel.
struct ChainSideOutput {
    std::string tag;
    ChannelType channel_type;
};

struct ChainOp {
    std::string id;    // graph-local operator id (informational)
    std::string type;  // OperatorRegistry lookup key
    // User-supplied stable identifier (`.uid("...")`). Carried
    // across the wire so the TM-side runner can call set_uid on the
    // built operator before it lands in the Dag - making OperatorId =
    // hash("uid/" + uid), stable across topology edits. Empty for
    // legacy / stateless ops.
    std::string uid;
    // Human-readable display label. Logs / metrics / dashboard UI;
    // does not affect OperatorId.
    std::string display_name;
    OperatorKind kind{OperatorKind::Operator};
    ChannelType in_channel{std::string{clink::cluster::kChannelInt64}};   // ignored for Source
    ChannelType out_channel{std::string{clink::cluster::kChannelInt64}};  // ignored for Sink
    std::uint32_t parallelism{1};  // total parallel subtasks of this op
    std::map<std::string, std::string> params;
    std::vector<ChainSideOutput> side_outputs;
};

// One inbound/outbound edge of the subtask. peer_role + peer_subtask_idx
// identify the other endpoint of the edge in the JobPlan; channel_type
// must agree with the matching op's in_channel/out_channel and is used to
// pick the wire codec.
struct SubtaskEdge {
    std::string peer_role;  // always kGenericSubtaskRole in v1
    std::uint32_t peer_subtask_idx{};
    ChannelType channel_type{std::string{clink::cluster::kChannelInt64}};
};

// RoutingMode moved to runner_registry.hpp (alongside ResolvedOutputGroup
// where it's actually consumed) so type_registry.hpp's chain dispatch
// closures can use it without pulling in the full planner header.

// One output group represents the connection from this subtask to one
// downstream operator. Multiple groups within an OperatorChainSpec form
// a multi-consumer fork (each group gets its own copy of every record
// via an outer Dag::fork at execution time). Within a group, records
// are routed according to `mode`.
struct SubtaskOutputGroup {
    RoutingMode mode{RoutingMode::Forward};
    std::vector<SubtaskEdge> edges;
    // Name of the key extractor function (registered in
    // KeyExtractorRegistry) used when `mode == Hash`. Resolved against
    // the upstream's out_channel type. Empty for Forward/Rebalance.
    std::string key_extractor_fn;
    // Name of the side output this group carries. Empty -> main output.
    // When non-empty the runner looks up the TypeOps for the group's
    // edge channel_type and uses the typed side-attach hook to wire a
    // dedicated network bridge sourced from the operator's side channel.
    std::string side_output_tag;
};

// What gets serialised into DeploymentTask.extra_config. The TM's generic
// role parses it back, instantiates ops via the registry, sets up
// network bridges per input/output edge, and runs the resulting Dag via
// LocalExecutor.
struct OperatorChainSpec {
    // Global subtask index across the whole job (matches the
    // DeploymentTask.subtask_idx the JM dispatches with).
    std::uint32_t subtask_idx{};
    // Per-op subtask index (0..parallelism-1) within each op's
    // parallelism. Built-in factories read this to partition their work
    // (e.g., int64_range_source emits its strided slice based on it).
    std::uint32_t subtask_idx_in_op{};
    std::vector<ChainOp> ops;  // length 1 in v2
    std::vector<SubtaskEdge> input_edges;
    // Output groups: one entry per downstream operator. Multiple groups
    // express multi-consumer fork (broadcast) by default; within a
    // group, `mode` selects forward (1:1) vs rebalance (round-robin)
    // routing.
    std::vector<SubtaskOutputGroup> output_groups;
    // How the outer cross-group routing distributes records across
    // groups: Broadcast (every group gets every record via Dag::fork)
    // or Split (records route to one group via a named selector
    // function). When Split, `output_groups` order matches branch index
    // and `output_selector_fn` names the selector in SelectorRegistry.
    enum class OutputRouting : std::uint8_t { Broadcast = 0, Split = 1 };
    OutputRouting output_routing{OutputRouting::Broadcast};
    std::string output_selector_fn;

    // Optional fused source. When set, the chain task hosts this
    // source inline (via Dag::add_source) instead of receiving
    // records over input_edges/in_bridges. The planner populates
    // this when the chain's upstream is a single par=1 source whose
    // only consumer is this chain - then the source runs in the
    // same thread as the chain's ops, eliminating one inter-thread
    // channel hop (and the associated codec serde, even after the
    // LocalDataPlane bypass landed in d0fb879).
    //
    // When fused_source is set, input_edges is empty.
    std::optional<ChainOp> fused_source;

    // Optional fused sink. Same idea on the output side: the chain's
    // tail emits directly into this sink via a downstream
    // Dag::add_sink instead of routing through output_groups peer
    // bridges. Planner populates this when the chain's tail has
    // exactly one downstream sink at par=1.
    //
    // When fused_sink is set, output_groups is empty (or only
    // carries side-output groups that survived the fusion check).
    std::optional<ChainOp> fused_sink;

    std::string to_json() const;
    static OperatorChainSpec from_json(std::string_view json_text);
};

// Plan a JobGraphSpec into a JobPlan against the supplied snapshot of
// registered TMs. Validates the graph, expands per-op parallelism (v1:
// must be 1), assigns each resulting subtask a unique subtask_idx under
// kGenericSubtaskRole, and packs an OperatorChainSpec into each task's
// extra_config.
//
// Throws std::runtime_error if:
//   - the graph fails JobGraphSpec::validate (missing ids, cycle, ...)
//   - parallelism > 1 on any op (v1 limit)
//   - an op's type is not registered in `registry`
//   - the union of TM slot capacities is too small to host the job
//
// The returned JobPlan still has tasks with `tm_id` left empty - the
// caller (JobManager::deploy) does the actual placement using its
// existing greedy first-fit logic so we don't duplicate scheduling.
JobPlan plan_job(const JobGraphSpec& graph, const OperatorRegistry& registry);

// Per-job overload: uses the supplied RunnerRegistry (typically a
// per-job bundle's runner registry, parent-pointed at the
// default-instance) for source/operator/sink validation. The legacy
// OperatorRegistry overload above is kept for direct callers (tests,
// in-process pipelines) that don't have a bundle.
class RunnerRegistry;
JobPlan plan_job(const JobGraphSpec& graph,
                 const OperatorRegistry& registry,
                 const RunnerRegistry& runner_registry);

// Total number of subtasks the planned graph needs (= sum of parallelism
// across ops). Caller uses this for slot accounting before calling
// plan_job, so it can reject SubmitJob without doing the planning work.
std::size_t total_subtask_count(const JobGraphSpec& graph);

}  // namespace clink::cluster
