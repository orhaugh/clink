#include "clink/runtime/local_submitter.hpp"

#include <any>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "clink/api/stream_execution_environment.hpp"
#include "clink/cluster/dag_builder_registry.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/type_registry.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"

namespace clink::cluster {

void LocalSubmitter::submit(api::StreamExecutionEnvironment& env) {
    submit(env, JobConfig{});
}

void LocalSubmitter::submit(api::StreamExecutionEnvironment& env, JobConfig config) {
    const auto& graph = env.graph();

    // Run the planner's structural invariants. `validate` throws on
    // missing ids, duplicates, cycles, or input refs that don't resolve.
    graph.validate();

    if (graph.ops.empty()) {
        throw std::runtime_error(
            "LocalSubmitter::submit: graph has no ops; build a source + sink first");
    }

    // Parallelism > 1 is supported on Source / Operator / Sink in
    // uniform-parallelism forward chains (every op in a chain shares
    // the same parallelism N). Hash-shuffled cross-parallelism edges
    // (`op.key_by` set) and Rebalance / Split routing are follow-ups;
    // until then the walker enforces uniformity below.
    //
    // CoOperators + side outputs are parallelism = 1 only. v1 keeps
    // these on the single-handle path; mixing them with parallel
    // chains would require building per-subtask side-output channels
    // (matches the cluster runtime's NetworkBridge fan-out shape) -
    // a sizeable extension worth its own commit.

    // Topological walk. `validate` rejects cycles + dangling inputs, and
    // the fluent builder appends ops in producer-before-consumer order
    // by construction, so we can iterate `ops` directly.
    auto& builders = DagBuilderRegistry::default_instance();
    auto& types = TypeRegistry::default_instance();
    Dag dag;

    // op_id → DagOpHandle wrapped for downstream lookup. For ops that
    // emit side outputs, the main handle is here under op.id and each
    // side handle is stored under "<op_id>::<tag>" so downstream
    // `inputs` references resolve through one shared map. We track the
    // full DagOpHandle (not just main_handle) so the walker can enforce
    // upstream parallelism matching downstream.
    std::unordered_map<std::string, DagOpHandle> handles;

    // Pre-pass: count how many downstream consumers each producer
    // output has. An "output" is keyed the same way `inputs` reference
    // it - bare `<op_id>` for the main output, `<op_id>::<tag>` for a
    // side output. When the count is >1 we fork the producer's
    // BoundedChannel into N independent branches via `Dag::fork<T>`
    // and dispense them in lookup order, so the consumers each get
    // every record rather than racing on a single-reader channel.
    std::unordered_map<std::string, std::size_t> consumer_count;
    for (const auto& op : graph.ops) {
        for (const auto& input_id : op.inputs) {
            ++consumer_count[input_id];
        }
    }

    // For each producer key with >1 consumers, `branches` holds the
    // forked StageHandles<T> (typed-erased) and `next_idx` advances on
    // each consumer lookup.
    struct ForkBuf {
        std::vector<std::any> branches;
        std::size_t next_idx{0};
    };
    std::unordered_map<std::string, ForkBuf> forked;

    auto plan_forks = [&](const std::string& key,
                          const std::string& channel_type,
                          const std::any& upstream_handle,
                          std::uint32_t parallelism) {
        auto n_it = consumer_count.find(key);
        if (n_it == consumer_count.end() || n_it->second <= 1) {
            return;
        }
        const auto* type_ops = types.find(channel_type);
        if (type_ops == nullptr || !type_ops->make_fork_handles) {
            throw std::runtime_error("LocalSubmitter::submit: producer output '" + key + "' has " +
                                     std::to_string(n_it->second) +
                                     " consumers but its channel type '" + channel_type +
                                     "' is not registered with a fork closure. Call "
                                     "register_type<T>(...) for that element type before submit, "
                                     "and recompile against the latest type_registry.hpp.");
        }
        forked[key] = ForkBuf{
            type_ops->make_fork_handles(dag, upstream_handle, n_it->second, parallelism), 0};
    };

    for (const auto& op : graph.ops) {
        const auto* builder = builders.find(op.type);
        if (builder == nullptr) {
            throw std::runtime_error(
                "LocalSubmitter::submit: no DagBuilder registered for op_type '" + op.type +
                "'. Built-in factories register via install_defaults(); "
                "for custom plugin ops use PluginRegistry::register_source/operator/sink.");
        }

        std::vector<std::any> upstream;
        std::vector<std::uint32_t> upstream_parallelisms;
        upstream.reserve(op.inputs.size());
        upstream_parallelisms.reserve(op.inputs.size());
        for (const auto& input_id : op.inputs) {
            auto it = handles.find(input_id);
            if (it == handles.end()) {
                throw std::runtime_error("LocalSubmitter::submit: op '" + op.id +
                                         "' references unknown input '" + input_id +
                                         "' (side-output handles are keyed as "
                                         "'<producer_id>::<tag>'; check that "
                                         "the producer declared the side output)");
            }
            // If the producer has multiple consumers, plan_forks installed
            // a ForkBuf for input_id with N branches; dispense them in
            // first-come order so each consumer sees a full copy of the
            // stream instead of racing for items on a single channel.
            auto fork_it = forked.find(input_id);
            if (fork_it != forked.end()) {
                auto& fb = fork_it->second;
                if (fb.next_idx >= fb.branches.size()) {
                    throw std::runtime_error(
                        "LocalSubmitter::submit: more consumer lookups than "
                        "fork branches for producer output '" +
                        input_id +
                        "' (internal accounting bug - consumer count "
                        "and dispense count diverged).");
                }
                upstream.push_back(fb.branches[fb.next_idx++]);
            } else {
                upstream.push_back(it->second.main_handle);
            }
            upstream_parallelisms.push_back(it->second.parallelism);
        }

        // v1 supports 0-input source, 1-input operator/sink, or
        // 2-input co-operator. Higher arity (unions) is a follow-up.
        if (upstream.size() > 2) {
            throw std::runtime_error("LocalSubmitter::submit: op '" + op.id + "' has " +
                                     std::to_string(upstream.size()) +
                                     " inputs; v1 supports source (0 inputs), single-input "
                                     "operator/sink, or co-operator (2 inputs). Unions / "
                                     "higher arity are follow-ups.");
        }

        // Parallelism invariants enforced today:
        //  * CoOperator (2 inputs): both inputs + the op itself must
        //    be at parallelism 1.
        //  * Side outputs declared: the op must be at parallelism 1.
        //  * Mixed-parallelism edge (upstream.p != op.p) is rejected
        //    until hash-shuffle/rebalance routing lands; the user can
        //    either match parallelism throughout the chain or fan in
        //    via a parallelism-1 sink (handled by the sink builder).
        if (upstream.size() == 2 && op.parallelism != 1) {
            throw std::runtime_error(
                "LocalSubmitter::submit: op '" + op.id + "' is a co-operator at parallelism " +
                std::to_string(op.parallelism) + "; co-operators are parallelism = 1 only in v1.");
        }
        if (!op.side_outputs.empty() && op.parallelism != 1) {
            throw std::runtime_error(
                "LocalSubmitter::submit: op '" + op.id + "' declares side outputs at parallelism " +
                std::to_string(op.parallelism) + "; side outputs require parallelism = 1 in v1.");
        }
        if (upstream.size() == 1) {
            const auto up_p = upstream_parallelisms.front();
            if (up_p != op.parallelism) {
                throw std::runtime_error(
                    "LocalSubmitter::submit: op '" + op.id + "' has parallelism " +
                    std::to_string(op.parallelism) + " but its upstream '" + op.inputs.front() +
                    "' is at parallelism " + std::to_string(up_p) +
                    ". Mixed-parallelism edges (hash shuffle / rebalance) are a follow-up; "
                    "match parallelism throughout the chain for now.");
            }
        }

        // Plumb the OperatorSpec params + subtask info + parallelism
        // through the BuildContext the user's factory captures.
        plugin::BuildContext ctx;
        ctx.params = op.params;
        ctx.parallelism = op.parallelism;
        ctx.subtask_idx = 0;

        auto built = (*builder)(dag, upstream, ctx);
        // Make sure builder reported the same parallelism we asked for -
        // if a builder closure was written before the parallelism path
        // existed and silently dropped to single-handle mode, downstream
        // would silently fan in. Detect early.
        if (built.parallelism != op.parallelism) {
            throw std::runtime_error("LocalSubmitter::submit: DagBuilder for op_type '" + op.type +
                                     "' returned parallelism " + std::to_string(built.parallelism) +
                                     " but the OperatorSpec asked for " +
                                     std::to_string(op.parallelism) + " (op '" + op.id +
                                     "'). The builder probably predates the parallel-mode "
                                     "extension; recompile against the latest plugin.hpp.");
        }
        handles.emplace(op.id, built);

        // Wire any declared side outputs. The DagBuilder returns the
        // producer's runner_index; for each (tag, channel_type) we look
        // up the TypeOps for that channel and call its typed
        // `make_side_output_handle` closure - which calls
        // `Dag::side_output_by_index<T>(runner_index, OutputTag<T>(tag))`
        // and returns the resulting StageHandle<T> wrapped as std::any.
        for (const auto& side : op.side_outputs) {
            const auto* type_ops = types.find(side.channel_type);
            if (type_ops == nullptr || !type_ops->make_side_output_handle) {
                throw std::runtime_error("LocalSubmitter::submit: side output on op '" + op.id +
                                         "' uses channel '" + side.channel_type +
                                         "' that isn't registered - call register_type<T>(...) "
                                         "for the side element type before submit.");
            }
            auto side_handle = type_ops->make_side_output_handle(dag, built.runner_index, side.tag);
            // Side outputs are parallelism=1 only (enforced above), so
            // they always live in the single-handle form.
            handles.emplace(op.id + "::" + side.tag,
                            DagOpHandle{side_handle, 0, /*parallelism=*/1});
            // If this side output has >1 downstream consumer, fork it
            // now so they each get their own channel rather than
            // racing on the single producer-side BoundedChannel. Side
            // outputs are parallelism=1 only (enforced above).
            plan_forks(op.id + "::" + side.tag, side.channel_type, side_handle, /*parallelism=*/1);
        }

        // Main output: if multiple ops consume this producer, fork its
        // channel(s) into N branches. The TypeRegistry closure dispatches
        // on parallelism - at p=1 it forks the producer's single channel
        // via `Dag::fork<T>`; at p>1 it forks per-subtask via
        // `Dag::fork_parallel<T>` so each branch keeps the producer's
        // parallel shape and downstream `add_parallel_*` can wire it.
        const auto consumers_it = consumer_count.find(op.id);
        if (consumers_it != consumer_count.end() && consumers_it->second > 1) {
            plan_forks(op.id, op.out_channel, built.main_handle, op.parallelism);
        }
    }

    LocalExecutor exec(std::move(dag), std::move(config));
    exec.run();

    // Surface any operator errors so test callers can assert on them.
    auto errs = exec.operator_errors();
    if (!errs.empty()) {
        std::string msg = "LocalSubmitter::submit: operator failures during run:";
        for (const auto& [op_name, err] : errs) {
            msg.append("\n  ").append(op_name).append(": ").append(err);
        }
        throw std::runtime_error(msg);
    }
}

}  // namespace clink::cluster
