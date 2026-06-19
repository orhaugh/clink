#pragma once

#include <string>

#include "clink/cluster/job_graph.hpp"
#include "clink/sql/logical_plan.hpp"

// Physical planner: LogicalPlan -> clink::cluster::JobGraphSpec.
//
// Phase 1 scope is INTENTIONALLY narrow to ship the end-to-end story
// before the format / multi-column work in Phase 2:
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
    // Throws TranslationError when the plan uses constructs outside
    // Phase 1 scope (multi-column tables, unknown connectors, etc.).
    [[nodiscard]] cluster::JobGraphSpec compile(const LogicalSink& root) const;
};

}  // namespace clink::sql
