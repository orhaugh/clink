#pragma once

#include <map>
#include <string>
#include <vector>

#include "clink/cluster/job_graph.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/logical_plan.hpp"

// Physical planner: LogicalPlan -> clink::cluster::JobGraphSpec.
//
// The initial scope is INTENTIONALLY narrow to ship the end-to-end
// story before the format / multi-column work:
//
//   * Every table is single-column with type TEXT/VARCHAR (Arrow utf8).
//     The "format" layer that turns multi-column rows into a wire
//     codec is deferred. A multi-column table at this layer throws
//     TranslationError pointing at the column definition.
//   * Supported connectors: 'file' / 'filesystem' (registered in core)
//     and 'kafka' (registered when the kafka impl is linked). The
//     planner emits the right factory name; if the runtime doesn't
//     have the impl linked, submission fails at job-deploy time.
//   * Scan -> Project -> Sink. Project is implemented as an
//     identity_string passthrough since there's only one column.
//
// What this looks like end-to-end:
//
//   CREATE TABLE src (line TEXT) WITH (connector='file', path='/tmp/in.txt');
//   CREATE TABLE dst (line TEXT) WITH (connector='file', path='/tmp/out.txt');
//   INSERT INTO dst SELECT line FROM src;
//
// compiles to a 3-op JobGraphSpec:
//
//   src: file_text_source   path=/tmp/in.txt   out=string
//   proj: identity_string   inputs=[src]       out=string
//   snk: file_text_sink     inputs=[proj]      path=/tmp/out.txt

namespace clink::sql {

class PhysicalPlanner {
public:
    PhysicalPlanner() = default;

    // Compile a bound LogicalPlan (always rooted in a LogicalSink for
    // INSERT INTO ... SELECT) to a JobGraphSpec the coordinator can submit.
    // Throws TranslationError when the plan uses unsupported constructs
    // (multi-column tables, unknown connectors, etc.).
    [[nodiscard]] cluster::JobGraphSpec compile(const LogicalSink& root) const;

    // Opt into the async-state execution path for unbounded GROUP BY.
    // When set, aggregate_row operators are marked async_state=true: at
    // runtime they hold per-group state in KeyedState (checkpointed) and
    // take process_async() when the state backend can defer reads, so a
    // slow remote/disaggregated read suspends a record instead of blocking
    // the runner. Default off keeps the in-memory aggregate path unchanged.
    void set_async_state_for_aggregation(bool v) noexcept { async_state_for_aggregation_ = v; }

    // Opt out of the bounded-state gate for this statement. Set from the
    // script's `ALLOW UNBOUNDED STATE` clause. Deliberately not a
    // constructor default: a caller has to reach for it.
    void set_allow_unbounded_state(bool v) noexcept { allow_unbounded_state_ = v; }

private:
    bool async_state_for_aggregation_ = false;
    bool allow_unbounded_state_ = false;
};

// The INSERT statements of one EXECUTE STATEMENT SET, compiled each on its own,
// as one job: every op id is prefixed by its statement ("s0_", "s1_", ...),
// lineage and UDFs are combined, and PostgreSQL CDC sources that share a
// replication slot are fused into one source that routes each table's rows to
// its own branch.
cluster::JobGraphSpec merge_statement_set(std::vector<cluster::JobGraphSpec> specs);

// The planner's last pass over a compiled spec, which compile() runs: it
// promotes each producer's stashed output schema to the producer's
// `columnar_output` param when every consumer of the producer takes a columnar
// batch, and erases the stash otherwise. Declared here so a spec shape the
// binder does not emit, such as an inner join straight into the sink-boundary
// bind, can be checked against the rule directly.
void enable_columnar_output(cluster::JobGraphSpec& spec);

// Declares the rescale bounds [1, max_parallelism] on every operator of a
// compiled spec that a live rescale may resize, and returns their ids. Opt-in:
// clink run --coordinator-host and clink_submit_sql call it for
// --max-parallelism, and a max of 0 declares nothing. A rescale changes one
// operator's parallelism, so the edge from it to an operator it feeds becomes
// round-robin, and a key's rows reach the sink through different subtasks in
// either order. That is harmless for an operator that emits each result once
// and wrong for one that revises what it emitted, so only keyed operators whose
// output is inserts only are declared: the four window kinds without allowed
// lateness, the interval join, MATCH_RECOGNIZE and DISTINCT. Each is also kept
// alone in its chain (cluster::kChainAloneParam), so a rescale does not split
// it from the operators it feeds. Throws std::invalid_argument for a max above
// the key groups or below an operator's parallelism.
std::vector<std::string> declare_rescale_bounds(cluster::JobGraphSpec& spec,
                                                std::uint32_t max_parallelism);

// The Row-channel source factory + build params to scan `table` in process (for
// ANALYZE, via the SourceFactory registry + LocalExecutor). Mirrors the scan
// source the planner emits for a Row-channel table. Throws TranslationError if
// the table is not a direct bounded Row source - e.g. a kafka/string-channel
// source that needs a string->row bridge (and is unbounded).
struct ScanSourceSpec {
    std::string type;                           // source factory name
    std::map<std::string, std::string> params;  // OperatorBuildContext params
};
[[nodiscard]] ScanSourceSpec row_scan_source_spec(const TableDef& table);

}  // namespace clink::sql
