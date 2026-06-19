#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "clink/sql/ast.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/logical_plan.hpp"

// Binder: ast::Statement + Catalog -> LogicalPlan.
//
// Phase 1 responsibilities:
//   * Resolve table names in FROM clauses against the catalog
//   * Resolve column names in SELECT against the source table's schema
//   * Build a Scan -> Project -> Sink tree for INSERT INTO SELECT
//   * Validate column count + type compatibility at the sink boundary
//
// CreateTableStmt is NOT bound here (it's not a relational plan); the
// CLI handles it by calling Catalog::register_table directly.
//
// All resolution errors throw TranslationError with the source
// position from the AST, distinguished from runtime errors that
// only surface at job execution.

namespace clink::sql {

class Binder {
public:
    explicit Binder(const Catalog& catalog) noexcept : catalog_(catalog) {}

    // Bind an INSERT INTO SELECT statement. Returns the rooted plan.
    std::unique_ptr<LogicalPlan> bind_insert(const ast::InsertStmt& stmt) const;

    // Bind a SELECT statement WITHOUT a sink. Returns the Scan->Project
    // subplan; the caller can wrap in a LogicalSink (e.g. for a CLI
    // preview that prints to stdout).
    std::unique_ptr<LogicalPlan> bind_select(const ast::SelectStmt& stmt) const;

private:
    // Construct a plan node for the given table name. When a CTE in
    // the current bind scope matches, that CTE's pre-bound plan is
    // moved out (each CTE is at-most-once in Phase 16). Otherwise a
    // plain LogicalScan against `resolved` is returned.
    std::unique_ptr<LogicalPlan> make_table_plan(const std::string& table_name,
                                                 const TableDef& resolved,
                                                 int pos) const;

    // #61 phase 2: bind FROM <table> MATCH_RECOGNIZE (...) into a
    // LogicalMatchRecognize (input scan + partition/order keys + pattern +
    // lowered DEFINE predicates + resolved MEASURES). v1 subset validated here.
    std::unique_ptr<LogicalPlan> bind_match_recognize(const ast::MatchRecognizeClause& mrc) const;

    // SQLOPT PTF: bind a process-table-function clause into a
    // LogicalProcessTableFunction (input scan + partition keys + the registered
    // function name + its declared output schema from PtfRegistry).
    std::unique_ptr<LogicalPlan> bind_process_table_function(
        const ast::ProcessTableFunctionClause& ptf) const;

    // Analytics depth: bind `SELECT *, agg(x) OVER (PARTITION BY ...
    // ORDER BY <event_time>) AS c, ...` into a LogicalOverAggregate.
    // window_targets are the indices of the SELECT items carrying an
    // OVER clause (all must share one partition/order spec). Running
    // frame only; bounded frames are rejected at parse time.
    std::unique_ptr<LogicalPlan> bind_over_aggregate(
        const ast::SelectStmt& stmt,
        const TableDef& source,
        const std::string& alias,
        const ast::TableRef& ref,
        const std::vector<std::size_t>& window_targets) const;

    // Inc 4: bind a simple SELECT whose WHERE carries a subquery
    // predicate (IN / NOT IN / EXISTS / NOT EXISTS / scalar). Rewrites
    // it to a LogicalSemiJoin or LogicalScalarBroadcast over the outer
    // scan (plus a filter for the non-subquery conjuncts), then applies
    // the SELECT projection.
    std::unique_ptr<LogicalPlan> bind_subquery_select(const ast::SelectStmt& stmt,
                                                      const TableDef& source,
                                                      const std::string& alias,
                                                      const ast::TableRef& ref) const;

    // #55: an uncorrelated single-aggregate scalar subquery as a SELECT
    // item -> append its value to every outer row (LogicalScalarProject)
    // and project the appended column under the item's name. v1 emits
    // append-only against the current scalar (same simplification as the
    // predicate-operand scalar path).
    std::unique_ptr<LogicalPlan> bind_scalar_select_projection(const ast::SelectStmt& stmt,
                                                               const TableDef& source,
                                                               const std::string& alias,
                                                               const ast::TableRef& ref,
                                                               const ast::SubLink& scalar_sl) const;

    // Bind an uncorrelated `(SELECT <agg>(col) FROM table [WHERE ...])`
    // to an ungrouped single-aggregate plan. Shared by the scalar
    // predicate-operand path and the SELECT-list path.
    struct ScalarSubplan {
        std::unique_ptr<LogicalPlan> plan;
        std::string output_name;
        std::shared_ptr<arrow::DataType> type;
    };
    ScalarSubplan bind_scalar_aggregate_subplan(const ast::SubLink& sl) const;

    const Catalog& catalog_;
    // Phase 16: WITH clause. Synthetic TableDefs serve column
    // resolution for CTE references inside the outer SELECT; matching
    // entries in cte_plans_ are consumed (moved) at the point where a
    // LogicalScan would otherwise be created. Each CTE is at-most-once
    // in Phase 16 v1 (no plan-tree deep copy yet); the binder errors
    // on a second reference.
    mutable std::unordered_map<std::string, TableDef> cte_synth_tables_;
    mutable std::unordered_map<std::string, std::unique_ptr<LogicalPlan>> cte_plans_;
    // Phase 21c: set when the outer WHERE was consumed by the
    // ROW_NUMBER pattern matcher; the regular projection path skips
    // the WHERE so the predicate doesn't get reapplied to the
    // already-bounded LogicalTopNPerKey output.
    mutable bool consumed_topn_where_ = false;
};

}  // namespace clink::sql
