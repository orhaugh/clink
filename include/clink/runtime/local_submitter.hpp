// LocalSubmitter - drives a fluent `StreamExecutionEnvironment` topology
// to completion in-process, without JobManager + TaskManager + network
// bridges. The cluster path
// (`StreamExecutionEnvironment::execute(job_name, JobSubmitter)`) is the
// production interface; this is its peer for tests, examples, and tight
// dev loops.
//
// Architecture:
//   1. Take the env's `JobGraphSpec` (the IR the fluent builder
//      produced).
//   2. Validate via `JobGraphSpec::validate` + planner-style invariants
//      (parallelism == 1, every op_type registered, no cycle).
//   3. Walk the ops in topological order. For each op, look up its
//      `clink::cluster::DagBuilder` (populated as a side effect of
//      `PluginRegistry::register_source / register_operator /
//      register_sink`) and call it with the upstream StageHandles
//      already materialised for the input edges.
//   4. Run the resulting single `Dag` through `LocalExecutor`.
//
// What this MVP covers (v1):
//   * Source / Operator / Sink ops.
//   * Single-input single-output operators.
//   * Linear topologies (one downstream consumer per op, no fork).
//   * Parallelism = 1 per op (matches the cluster planner's v1 floor).
//
// What this MVP does NOT cover yet (extend follow-up):
//   * CoOperators (two-input join / co-process). Same DagBuilder shape;
//     adds a second `std::any` element to the upstream vector.
//   * Side outputs. The Dag-side wiring already exists
//     (`Dag::side_output_by_index`); the builder closure just needs to
//     declare them based on `OperatorSpec.side_outputs`.
//   * `fork` / `split` / `Hash` routing for parallelism > 1. These
//     belong with the cluster path until the planner supports >1 here too.
//
// On encountering an unsupported feature the submitter throws
// `std::runtime_error` with a precise message so the caller knows to
// fall back to `JobSubmitter`-based execution.

#pragma once

#include <string>

#include "clink/runtime/job_config.hpp"

namespace clink::api {
class StreamExecutionEnvironment;
}

namespace clink::cluster {

class LocalSubmitter {
public:
    // Build + run the env's topology in-process. Returns when the
    // pipeline naturally terminates (all sources exhausted, all sinks
    // drained) or when an operator throws.
    //
    // Throws `std::runtime_error` if the topology uses features beyond
    // the v1 MVP scope (see header docs for what's covered).
    //
    // The two overloads:
    //   * `submit(env)` - runs with default JobConfig. Works for
    //     stateless pipelines.
    //   * `submit(env, JobConfig)` - threads the config through to the
    //     underlying LocalExecutor. Required when the topology uses
    //     keyed state (`RuntimeContext::keyed_state`), which throws
    //     without a configured `state_backend`.
    static void submit(api::StreamExecutionEnvironment& env);
    static void submit(api::StreamExecutionEnvironment& env, JobConfig config);
};

}  // namespace clink::cluster
