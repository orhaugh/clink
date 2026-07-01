#pragma once

#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "clink/sql/ast.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/logical_plan.hpp"

// Binder: ast::Statement + Catalog -> LogicalPlan.
//
// Responsibilities:
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
    // moved out (each CTE is at-most-once). Otherwise a
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

    // SQL-native AI: bind ML_PREDICT(...) into a LogicalMlPredict (input scan +
    // model name + feature columns + the model's OUTPUT columns from the catalog;
    // output schema = input columns then model OUTPUT columns).
    std::unique_ptr<LogicalPlan> bind_ml_predict(const ast::MlPredictClause& mlp) const;

    // SQL-native AI: bind VECTOR_SEARCH(...) into a LogicalVectorSearch (input scan
    // + the vector table + query / index columns + top_k + metric; output schema =
    // input columns then vector-table columns then a synthetic score DOUBLE).
    std::unique_ptr<LogicalPlan> bind_vector_search(const ast::VectorSearchClause& vs) const;

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

    // Last-N-per-key rolling aggregate, the lowering target of a bounded
    // ROWS frame over a source with NO declared event_time_column:
    //   SELECT k, agg() OVER (PARTITION BY k ORDER BY oc ROWS BETWEEN n
    //                         PRECEDING AND CURRENT ROW)
    // Emits a per-key changelog (LogicalLastNAgg). Non-window SELECT items
    // must be the PARTITION BY columns (GROUP-BY-like shape).
    std::unique_ptr<LogicalPlan> bind_last_n_agg(
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

    // A bound relation in a (possibly nested) JOIN tree: its plan, the output
    // stream columns it produces, the base-table aliases it covers, and a map
    // from a qualified column ref ("alias.col") to the name that column has in
    // this relation's OUTPUT stream. Lets bind_join_rel build a left-deep tree
    // of binary EquiJoins for a multi-way join, resolving each join's keys and
    // output schema correctly across nested levels. A base table contributes
    // raw column names (its scan stream) and is prefixed by its alias at the
    // parent join; a sub-join contributes already-flat "<alias>_<col>" names and
    // is passed through unprefixed (empty alias) at the parent join.
    struct BoundRel {
        std::unique_ptr<LogicalPlan> plan;
        std::vector<ColumnSpec> columns;
        std::set<std::string> aliases;
        bool is_base = false;
        std::string alias;  // base-table alias; empty for a join
        std::map<std::string, std::string> qual_to_stream;
    };

    // Recursively bind a FROM item (base table, derived table, or nested INNER
    // equi-join) into a BoundRel. The 2-base-table top-level join (which also
    // covers interval / lookup / outer joins) stays on the existing inline path
    // in bind_select; this method powers the NESTED multi-way INNER equi-join
    // case, including a derived table / windowed aggregate as a join side.
    BoundRel bind_join_rel(const ast::FromItem& item) const;

    // Bind a base-table (or synthetic derived-table) reference into a BoundRel:
    // its plan, alias, and qualified-name -> stream-name map. Shared by the
    // TableRef and the derived-table (SubqueryItem) paths of bind_join_rel.
    BoundRel bind_base_table_rel(const ast::TableRef& ref) const;

    const Catalog& catalog_;
    // WITH clause. Synthetic TableDefs serve column
    // resolution for CTE references inside the outer SELECT; matching
    // entries in cte_plans_ are consumed (moved) at the point where a
    // LogicalScan would otherwise be created. Each CTE is at-most-once
    // (no plan-tree deep copy yet); the binder errors
    // on a second reference.
    mutable std::unordered_map<std::string, TableDef> cte_synth_tables_;
    mutable std::unordered_map<std::string, std::unique_ptr<LogicalPlan>> cte_plans_;
    // CREATE VIEW: the logical views currently being expanded (make_table_plan
    // re-binds a view's defining SELECT inline). A view re-entered while already
    // expanding is a cycle (direct or transitive self-reference) - rejected.
    mutable std::set<std::string> expanding_views_;
    // Set when the outer WHERE was consumed by the
    // ROW_NUMBER pattern matcher; the regular projection path skips
    // the WHERE so the predicate doesn't get reapplied to the
    // already-bounded LogicalTopNPerKey output.
    mutable bool consumed_topn_where_ = false;
};

}  // namespace clink::sql
