// DagBuilderRegistry - type-erased Dag construction callbacks indexed by
// op_type. Companion to RunnerRegistry: where RunnerRegistry stores
// `SubtaskRunner`s (per-subtask mini-Dags wired with network bridges, run
// by the TaskManager), DagBuilderRegistry stores callbacks that add the
// op directly to a single shared `clink::Dag` for in-process execution
// (no JM/TM, no network bridges, no per-op LocalExecutor).
//
// Each callback captures the user's typed factory and the channel types
// (In / Out for operators, T for source/sink) so it can call
// `dag.add_source<T>` / `add_operator<In,Out>` / `add_sink<T>` correctly
// at runtime. The caller passes upstream `StageHandle`s as `std::any`
// (since the runtime walker has no static knowledge of the upstream's
// channel type); the callback unpacks via `std::any_cast<StageHandle<In>>`.
//
// Populated as a side effect of `PluginRegistry::register_source<T>`,
// `register_operator<In, Out>`, `register_sink<T>` - same surface that
// populates `RunnerRegistry` today. Built-in factories (kafka, postgres,
// clickhouse, s3, ...) register both via the same install flow, so
// `install_defaults` is enough to bring the registry up.

#pragma once

#include <any>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace clink {
class Dag;  // fwd
}
namespace clink::plugin {
struct BuildContext;  // fwd; defined in clink/plugin/plugin.hpp
}

namespace clink::cluster {

// Handle returned by a DagBuilder.
//
// At parallelism == 1 the builder wraps a `StageHandle<Out>` in
// `main_handle` and reports the op's `runner_index`. At parallelism > 1
// the builder wraps a `ParallelStageHandle<Out>` instead; downstream
// builders branch on `parallelism` to pick which form to unpack.
//
// `runner_index` is meaningful only at parallelism == 1 (side-output
// wiring uses it). Parallel ops don't expose side outputs in v1.
struct DagOpHandle {
    std::any main_handle;          // empty for sinks
    std::size_t runner_index{0};   // valid only when parallelism == 1
    std::uint32_t parallelism{1};  // 1 = single StageHandle; >1 = ParallelStageHandle
};

// Signature for a Dag-builder callback.
//
// `upstream` carries one `std::any` per input edge, each wrapping the
// upstream op's StageHandle<T>. For sources `upstream` is empty; for
// single-input operators it has size 1; for co-operators size 2 (the
// order matches the OperatorSpec's `inputs` array, which the planner
// produces in registration order - i.e. In1 before In2).
using DagBuilder = std::function<DagOpHandle(
    Dag& dag, const std::vector<std::any>& upstream, const plugin::BuildContext& ctx)>;

class DagBuilderRegistry {
public:
    DagBuilderRegistry() = default;
    explicit DagBuilderRegistry(const DagBuilderRegistry* parent) : parent_(parent) {}

    // Register a builder under `op_type`. Latest registration wins (matches
    // RunnerRegistry semantics). Thread-safe.
    void register_builder(std::string op_type, DagBuilder fn);

    // Lookup. Returns nullptr if `op_type` isn't registered; falls through
    // to `parent` on miss.
    const DagBuilder* find(const std::string& op_type) const;

    // Process-wide default. Same convention as TypeRegistry / RunnerRegistry.
    static DagBuilderRegistry& default_instance();

private:
    mutable std::mutex mu_;
    std::unordered_map<std::string, DagBuilder> by_op_type_;
    const DagBuilderRegistry* parent_{nullptr};
};

}  // namespace clink::cluster
