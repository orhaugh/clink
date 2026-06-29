#pragma once

#include <map>
#include <string>

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
    // INSERT INTO ... SELECT) to a JobGraphSpec the JM can submit.
    // Throws TranslationError when the plan uses unsupported constructs
    // (multi-column tables, unknown connectors, etc.).
    [[nodiscard]] cluster::JobGraphSpec compile(const LogicalSink& root) const;

    // Opt into the async-state execution path for unbounded GROUP BY.
    // When set, aggregate_row operators are marked async_state=true: at
    // runtime they hold per-group state in KeyedState (checkpointed) and
    // take process_async() when the state backend can defer reads, so a
    // slow remote/disaggregated read suspends a record instead of blocking
    // the runner. Default off keeps the in-memory aggregate path unchanged.
    // Flink analogue: table.exec.async-state.enabled.
    void set_async_state_for_aggregation(bool v) noexcept { async_state_for_aggregation_ = v; }

private:
    bool async_state_for_aggregation_ = false;
};

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
