#pragma once

#include <cstdint>
#include <map>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "clink/cluster/operator_registry.hpp"
#include "clink/state/schema_version.hpp"

namespace clink::cluster {

// One operator in a serialised job graph. v1 fields:
//
//   * type        - factory key in the OperatorRegistry.
//   * id          - graph-local stable id (used by `inputs`).
//   * inputs      - upstream operator ids this op reads from. Empty for
//                   sources. Single-input only in v1; multi-input
//                   topologies (joins, unions) come in a follow-up.
//   * parallelism - number of subtasks this operator runs as. v1: must
//                   be 1; the JobPlanner enforces this.
//   * out_channel - element type emitted by this operator. Used by the
//                   generic subtask role on the TM to pick the right
//                   network bridge codec and to type-cast the operator
//                   factory's boxed result back to a concrete operator.
//   * params      - free-form key=value strings consumed by the registered
//                   factory.

// Declaration of one named side output an operator emits. The element
// type is identified by `channel_type` (TypeRegistry channel name). At
// run time the plugin runner uses TypeOps for that channel name to
// wire a typed network bridge for the side channel.
struct SideOutputDecl {
    std::string tag;
    ChannelType channel_type;
};

struct OperatorSpec {
    std::string type;
    std::string id;
    // Optional human-readable label. Used for logs, metrics, dashboard
    // UI. Defaults to empty; the planner / TM use `type` or `id` as
    // the display fallback. Set via DataStream<T>::name("...").
    std::string display_name;
    // Optional stable identifier - `.uid("...")`. When set,
    // the runtime derives the OperatorId from this string (hashed) so
    // keyed state survives topology edits (renames, reorderings, added
    // ops upstream). Strongly recommended for any stateful operator
    // whose state needs to outlive a savepoint. When empty, the
    // runtime falls back to the legacy stage-index-based hash.
    std::string uid;
    std::vector<std::string> inputs;
    std::uint32_t parallelism{1};
    // Bounds for adaptive rescaling. When both are zero
    // (the default), this operator does NOT participate in
    // autoscaling - its parallelism stays at the static value above.
    // When `max_parallelism > parallelism`, the JM's autoscaler
    // may scale this operator up to max_parallelism in
    // response to backpressure / load signals. When
    // `min_parallelism < parallelism`, it may scale down to
    // min_parallelism. Manual rescale via clink_rescale_job already
    // works without these bounds; these bounds define the policy
    // surface the RescaleCoordinator + autoscaler consume.
    //
    // Invariants (enforced by validate()):
    //   - If either bound is non-zero, both must be non-zero.
    //   - min_parallelism <= parallelism <= max_parallelism.
    //   - min_parallelism >= 1 (zero subtasks is undefined).
    std::uint32_t min_parallelism{0};
    std::uint32_t max_parallelism{0};
    ChannelType out_channel{std::string{clink::cluster::kChannelInt64}};
    std::map<std::string, std::string> params;
    // When non-empty, declares that this op is keyed: incoming edges
    // from every upstream are hash-partitioned via the named key
    // extractor (resolved against KeyExtractorRegistry, typed on the
    // upstream's out_channel). Same key always lands on the same
    // subtask -> keyed state with K is correct at parallelism > 1.
    // For CoOperator with two upstreams the extractor must be defined
    // on both upstream types separately under the same name.
    std::string key_by;
    // Named side outputs this op emits to. Each entry pairs a tag
    // string (passed to runtime()->side_output<T>(OutputTag<T>(tag)))
    // with the channel_type of T. Downstream consumers reference the
    // side output via "id::tag" in their `inputs` list.
    std::vector<SideOutputDecl> side_outputs;
};

// A SQL-declared scalar function the job's expressions call, shipped with
// the spec so every TaskManager can register it at deploy time (the module
// path in `definitions` is only readable where the CREATE FUNCTION ran).
// Types are Arrow ToString() names ("int64", "double", ...); the module
// payload is base64 so the spec stays valid JSON. Registration on the TM is
// process-wide and idempotent (a re-deploy replaces the same name), matching
// how C++-registered UDFs from a job plugin behave.
struct UdfSpec {
    std::string name;
    std::string language;
    std::vector<std::string> arg_types;
    std::string return_type;
    std::vector<std::string> definitions;
    std::string module_b64;
    // "scalar" (default; absent on the wire for back-compat) or
    // "aggregate" (CREATE AGGREGATE: registers an accumulator).
    std::string kind;
};

// Pack/unpack a UDF list as a JSON array string - the single-string form
// carried by the DeployMsg wire field (and embedded raw in the spec JSON).
std::string pack_udf_specs(const std::vector<UdfSpec>& udfs);
std::vector<UdfSpec> unpack_udf_specs(std::string_view packed);

// JobGraphSpec is the wire-shaped logical description of a job (
// JobGraph). The clink JM translates it into a JobPlan (
// ExecutionGraph) by applying placement and parallelism expansion.
struct JobGraphSpec {
    std::vector<OperatorSpec> ops;

    // Optional human-readable job name. Set by the submitter (e.g. the
    // HTTP ?name= param, or the SQL statement's derived name). Carried in
    // the submitted spec and the retained graph so it survives HA restart,
    // and surfaced in data lineage (the OpenLineage job name). Empty when
    // the submitter named nothing; consumers fall back to the job id.
    std::string name;

    // Optional column-level lineage captured at SQL compile, as a JSON
    // object keyed by sink op id:
    //   {"<sink_id>":[{"output":"c","transformation":"IDENTITY",
    //                  "inputs":[{"namespace":"..","name":"..","field":".."}]}]}
    // Empty for non-SQL jobs. Carried so the JM's extract_lineage can attach
    // it to the sink datasets; survives HA restart with the rest of the spec.
    std::string column_lineage;

    // State schema evolution: the versions the job expects per
    // (op, state_type), declared via env.expect_state_version(...).
    // Carried in the submitted spec so the JM/TM can migrate restored
    // state (and a future submit-gate can reject an unbridgeable
    // restore). Empty for jobs that declare nothing. Serialized as a
    // single packed string in to_json/from_json.
    StateVersionMap expected_state_versions;

    // SQL-declared scalar functions (CREATE FUNCTION ... LANGUAGE ...)
    // this job's expressions call, module payloads included. The JM
    // threads them to every TaskManager in the DeployMsg; a TM registers
    // each before running the job's subtasks. Empty for jobs that use
    // none. Survives HA restart with the rest of the spec.
    std::vector<UdfSpec> udfs;

    // Compact line-based format. Preserved for the round-trip tests in
    // tests/test_job_graph.cpp and for terse hand-written specs. JSON is
    // the format used by the submission protocol.
    //
    // Wire format (text, line-based):
    //   <op_type> [key=value [key=value ...]]
    //
    // Spaces in values are not supported in this MVP.
    std::string serialize() const;
    static JobGraphSpec parse(std::string_view text);

    // JSON shape:
    //   {
    //     "ops": [
    //       {"id": "src",  "type": "int64_range_source",
    //        "out_channel": "int64", "params": {"count": "5"}},
    //       {"id": "snk",  "type": "file_int64_sink",
    //        "inputs": ["src"], "out_channel": "int64",
    //        "params": {"path": "/tmp/out.txt"}}
    //     ]
    //   }
    //
    // out_channel is required for every op. parallelism defaults to 1
    // when absent. inputs defaults to empty.
    //
    // `from_json` auto-runs validate() on the parsed spec and throws
    // on any invariant violation (duplicate id, dangling input, cycle).
    // Callers don't need to remember to call validate() themselves;
    // by the time from_json returns, the spec is known well-formed.
    std::string to_json() const;
    static JobGraphSpec from_json(std::string_view json_text);

    // Topology helpers. Throw on validation failure with a precise
    // diagnostic. The planner runs this before acting on the spec;
    // `from_json` runs it automatically too. Idempotent - calling
    // multiple times on the same spec is a cheap O(V+E) re-check.
    void validate() const;  // checks: ids unique, inputs resolve, no cycles
};

// Convenience: parse a parameter as the requested numeric type.
inline std::int64_t param_int64(const OperatorSpec& op,
                                const std::string& key,
                                std::int64_t fallback = 0) {
    auto it = op.params.find(key);
    if (it == op.params.end()) {
        return fallback;
    }
    try {
        return std::stoll(it->second);
    } catch (...) {
        return fallback;
    }
}

inline std::string param_string(const OperatorSpec& op,
                                const std::string& key,
                                std::string fallback = {}) {
    auto it = op.params.find(key);
    return it == op.params.end() ? std::move(fallback) : it->second;
}

}  // namespace clink::cluster
