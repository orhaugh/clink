#pragma once

// SQL script execution: the statement-processing loop shared by every
// front door that takes a SQL script (the clink_submit_sql tool, the
// `clink run <file>.sql` embedded runner, and the embeddable engine).
//
// run_script() folds DDL statements into the caller's catalog and hands
// every compiled job (INSERT INTO ... SELECT, materialized-view
// maintenance / refresh) to the caller's SubmitFn - the ONLY thing that
// differs between front doors. The tool's SubmitFn POSTs the spec JSON
// to a Coordinator (or prints it); the embedded engine's SubmitFn calls
// Coordinator::submit_job in-process.
//
// Bare top-level SELECT: rejected by default (the historical submit-tool
// behaviour). With ScriptRunOptions::bare_select_to_print the runner
// instead binds the SELECT for its output schema, registers a
// synthesised connector='print' sink table, and compiles the statement
// as INSERT INTO that table - which is what makes
// `clink run -e "SELECT ..."` print results to stdout.

#include <cstdint>
#include <functional>
#include <ostream>
#include <string>
#include <vector>

#include "clink/cluster/job_graph.hpp"
#include "clink/sql/catalog.hpp"

namespace clink::sql {

struct ScriptRunOptions {
    // Print each compiled statement's LogicalPlan instead of submitting.
    bool explain = false;
    // Uniform op parallelism applied to every compiled spec (>1 fans out).
    std::uint32_t parallelism = 1;
    // Job-name override for submitted jobs. Empty keeps the per-statement
    // defaults (INSERT: empty; materialized views: mv_<name> / refresh_<name>).
    std::string job_name;
    // Compile a bare top-level SELECT into a synthesised connector='print'
    // sink (see the header comment). Off = reject with the historical error.
    bool bare_select_to_print = false;
    // Compile a bare top-level SELECT into a synthesised connector='collect'
    // table instead (changelog='true' when the plan retracts, so the Arrow
    // stream carries a leading row_kind column); each synthesised table name
    // is appended here so the caller can attach a reader. Wins over
    // bare_select_to_print when non-null.
    std::vector<std::string>* bare_select_to_collect = nullptr;
};

struct ScriptIO {
    std::ostream* out = nullptr;  // statement output: EXPLAIN, SHOW TABLES, ...
    std::ostream* err = nullptr;  // diagnostics
};

// Receives every compiled job spec (parallelism already applied) with its
// job name. Return 0 to continue the script; non-zero aborts run_script
// with that code.
using SubmitFn = std::function<int(const cluster::JobGraphSpec& spec, const std::string& job_name)>;

// Process every statement of `sql` against `catalog`. Returns 0 on
// success; non-zero on the first failing statement (diagnostics on
// io.err, matching the historical clink_submit_sql messages).
int run_script(const std::string& sql,
               Catalog& catalog,
               const ScriptRunOptions& opts,
               const ScriptIO& io,
               const SubmitFn& submit);

// SubmitFn that POSTs the spec JSON to a Coordinator's HTTP submit
// endpoint (/api/v1/jobs/spec), forwarding the coordinator's response body to
// `out`. `state_backend_uri` (optional) rides as a query param.
SubmitFn make_http_submit(std::string coordinator_host,
                          std::uint16_t coordinator_port,
                          std::string state_backend_uri,
                          std::ostream& out,
                          std::ostream& err);

// SubmitFn that prints the spec JSON to `out` (the no-coordinator inspection mode).
SubmitFn make_print_submit(std::ostream& out);

}  // namespace clink::sql
