#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include <arrow/type.h>

#include "clink/config/json.hpp"
#include "clink/sql/ast.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/logical_plan.hpp"

// #59: a thin public surface over the binder's expression/predicate/type
// lowering helpers, so the programmatic Table API (clink::api) lowers
// expressions through EXACTLY the same code as the SQL frontend - one source
// of truth for the JSON-IR shape and type inference. The implementations are
// thin forwarders (in binder.cpp) onto the binder's file-local helpers; they
// carry distinct names to avoid recursing into themselves.

namespace clink::sql::lowering {

// Lower a value expression to its JSON-IR JsonValue (the {"col"|"lit"|"op"} form
// the runtime evaluator consumes). `source` resolves column refs/types; `alias`
// is the source alias for qualified refs ("" for none).
clink::config::JsonValue value_expr(const ast::Expression& expr,
                                    const TableDef& source,
                                    const std::string& source_alias);

// Lower a boolean expression to predicate JSON-IR (eq/lt/and/or/not/...).
clink::config::JsonValue predicate(const ast::Expression& expr, const TableDef& source);

// Bind-time Arrow type of a value expression (column types from `source`,
// SQL-standard arithmetic/decimal/function result types).
std::shared_ptr<arrow::DataType> expr_type(const ast::Expression& expr, const TableDef& source);

// Resolve a SELECT-style item list to ProjectOutput rows (name + expr_json +
// type), expanding `SELECT *` against the source. Mirrors the binder's path.
std::vector<ProjectOutput> select_items(const std::vector<ast::SelectItem>& items,
                                        const TableDef& source,
                                        const std::string& source_alias);

// Arrow result type of an aggregate function over an input column (SUM keeps
// the column type / widens, AVG -> float64, COUNT -> int64, etc.).
std::shared_ptr<arrow::DataType> aggregate_type(const std::string& fn,
                                                const TableDef& source,
                                                const std::string& input_column);

// Validate that a projected schema is INSERT-compatible with a sink table
// (column count + per-column type, incl. the #56 DECIMAL assignment
// coercions). Throws TranslationError with the same message as the SQL path.
void check_sink(const TableDef& sink, const arrow::Schema& source_schema, int loc);

// One output column of a GROUP BY result, in SELECT/agg() order: either a
// passthrough group key (resolved source column -> output field name) or an
// aggregate identified by its index into the parallel `aggregates` vector.
struct GroupOutputColumn {
    bool is_aggregate = false;
    std::size_t agg_index = 0;      // when is_aggregate: index into `aggregates`
    std::string key_source_column;  // when !is_aggregate: column name in `source`
    std::string key_output_name;    // when !is_aggregate: output field name
    // A windowed GROUP BY may also project the synthetic window bounds. When
    // is_window_bound, this column is window_start (window_is_end=false) or
    // window_end (window_is_end=true), emitted as BIGINT under key_output_name
    // (honouring a SELECT alias). key_source_column carries the literal runtime
    // column name ("window_start"/"window_end").
    bool is_window_bound = false;
    bool window_is_end = false;
};

// Assemble the Arrow output schema of a GROUP BY / aggregate in `columns`
// order: aggregate fields take their (output_name, type) from `aggregates`;
// key fields take their type from `source`. Single source of truth shared by
// the SQL binder and the programmatic Table API (#59) so the two cannot drift
// on output column ordering or naming.
std::shared_ptr<arrow::Schema> build_group_output_schema(
    const std::vector<GroupOutputColumn>& columns,
    const std::vector<AggregateOutput>& aggregates,
    const TableDef& source);

}  // namespace clink::sql::lowering
