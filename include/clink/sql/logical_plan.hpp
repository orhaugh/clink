#pragma once

#include <memory>
#include <string>
#include <vector>

#include "clink/sql/catalog.hpp"

#include "arrow/api.h"

// Logical relational algebra for clink SQL.
//
// Phase 1 nodes: Scan, Project, Sink.
//
//   LogicalScan(table_def)
//       Reads rows from a catalog-registered table. Output schema is
//       derived directly from the TableDef's column list.
//
//   LogicalProject(input, column_indices, output_names)
//       Phase 1 is column-list projection only: each output column
//       references one input column by index. Expression projection
//       (a+b, fn(x), etc.) lands in Phase 3.
//
//   LogicalSink(input, sink_table_def)
//       Writes the input stream to a catalog-registered sink table.
//       Type-compatibility between input schema and sink columns is
//       checked at bind time, not here.
//
// Nodes are tree-owned via unique_ptr; visitors walk via inputs().
// All concrete nodes derive from LogicalPlan so the planner can
// dispatch generically on kind().

namespace clink::sql {

class LogicalPlan {
public:
    virtual ~LogicalPlan() = default;

    [[nodiscard]] virtual std::string kind() const = 0;
    [[nodiscard]] virtual std::shared_ptr<arrow::Schema> schema() const = 0;
    [[nodiscard]] virtual std::vector<const LogicalPlan*> inputs() const = 0;

    // Returns a multi-line indented string for EXPLAIN. Each node
    // prints its own line; recursion handled by the default impl.
    [[nodiscard]] virtual std::string explain(int indent = 0) const;
};

class LogicalScan final : public LogicalPlan {
public:
    explicit LogicalScan(const TableDef* table);

    [[nodiscard]] const TableDef& table() const noexcept { return *table_; }

    // Projection-pushdown hint set by the optimizer. When non-empty,
    // it lists the columns the rest of the plan actually references;
    // the physical planner threads this through as a connector hint.
    void set_projected_columns(std::vector<std::string> cols) {
        projected_columns_ = std::move(cols);
    }
    [[nodiscard]] const std::vector<std::string>& projected_columns() const noexcept {
        return projected_columns_;
    }

    [[nodiscard]] std::string kind() const override { return "Scan"; }
    [[nodiscard]] std::shared_ptr<arrow::Schema> schema() const override { return schema_; }
    [[nodiscard]] std::vector<const LogicalPlan*> inputs() const override { return {}; }

private:
    const TableDef* table_;
    std::shared_ptr<arrow::Schema> schema_;
    std::vector<std::string> projected_columns_;
};

// Filter: drops rows where the predicate evaluates to false. Schema
// passes through unchanged. The predicate carries the JSON form used
// by the runtime's filter_string_predicate op (see
// include/clink/operators/json_predicate.hpp for the format).
class LogicalFilter final : public LogicalPlan {
public:
    LogicalFilter(std::unique_ptr<LogicalPlan> input, std::string predicate_json);

    [[nodiscard]] const LogicalPlan& input() const noexcept { return *input_; }
    [[nodiscard]] std::unique_ptr<LogicalPlan>& input_mut() noexcept { return input_; }
    [[nodiscard]] const std::string& predicate_json() const noexcept { return predicate_json_; }
    // Replace the predicate, used by the optimizer's predicate pushdown to leave
    // the residual conjuncts (an empty `and` is the vacuously-true pass-through).
    void set_predicate_json(std::string predicate_json) {
        predicate_json_ = std::move(predicate_json);
    }

    [[nodiscard]] std::string kind() const override { return "Filter"; }
    [[nodiscard]] std::shared_ptr<arrow::Schema> schema() const override {
        return input_->schema();
    }
    [[nodiscard]] std::vector<const LogicalPlan*> inputs() const override { return {input_.get()}; }

private:
    std::unique_ptr<LogicalPlan> input_;
    std::string predicate_json_;
};

// Phase 28c-frontend: async lookup / enrichment map. Lowered from a
// SELECT whose sole projection is a registered async function applied
// to the row (`SELECT enrich(*) FROM t`). The runtime emits an
// async_lookup_row operator that drives the registered
// Row -> async::Task<Row> coroutine. The output schema is the
// enrichment target's schema (the INSERT sink's columns): the async
// function produces the enriched row shape, which the SQL layer cannot
// infer on its own, so the sink's declared columns define the contract.
class LogicalAsyncMap final : public LogicalPlan {
public:
    LogicalAsyncMap(std::unique_ptr<LogicalPlan> input,
                    std::string function_name,
                    const TableDef* output_table);

    [[nodiscard]] const LogicalPlan& input() const noexcept { return *input_; }
    [[nodiscard]] std::unique_ptr<LogicalPlan>& input_mut() noexcept { return input_; }
    [[nodiscard]] const std::string& function_name() const noexcept { return function_name_; }

    [[nodiscard]] std::string kind() const override { return "AsyncMap"; }
    [[nodiscard]] std::shared_ptr<arrow::Schema> schema() const override { return schema_; }
    [[nodiscard]] std::vector<const LogicalPlan*> inputs() const override { return {input_.get()}; }

private:
    std::unique_ptr<LogicalPlan> input_;
    std::string function_name_;
    std::shared_ptr<arrow::Schema> schema_;
};

// Phase 3.3: each output column is described by (name, expression
// JSON text, inferred Arrow type). For pure column-ref outputs the
// expression is {"col": "name"}; computed outputs hold a richer
// expression tree the runtime evaluates via
// clink::operators::evaluate_json_value_expr.
struct ProjectOutput {
    std::string name;
    std::string expr_json;
    std::shared_ptr<arrow::DataType> type;
};

class LogicalProject final : public LogicalPlan {
public:
    LogicalProject(std::unique_ptr<LogicalPlan> input, std::vector<ProjectOutput> outputs);

    [[nodiscard]] const LogicalPlan& input() const noexcept { return *input_; }
    [[nodiscard]] std::unique_ptr<LogicalPlan>& input_mut() noexcept { return input_; }
    [[nodiscard]] const std::vector<ProjectOutput>& outputs() const noexcept { return outputs_; }

    [[nodiscard]] std::string kind() const override { return "Project"; }
    [[nodiscard]] std::shared_ptr<arrow::Schema> schema() const override { return schema_; }
    [[nodiscard]] std::vector<const LogicalPlan*> inputs() const override { return {input_.get()}; }

private:
    std::unique_ptr<LogicalPlan> input_;
    std::vector<ProjectOutput> outputs_;
    std::shared_ptr<arrow::Schema> schema_;
};

// Phase 4: windowed aggregation.
//
// Window kinds and their parameters (all expressed in milliseconds):
//   TUMBLE(ts, size)        fixed non-overlapping windows
//   HOP(ts, size, slide)    overlapping windows; slide < size
//   SESSION(ts, gap)        gap-based dynamic windows
//
// The window_column carries the name of the event-time column; it
// matches the source's event_time_column property (the planner checks).
struct WindowSpec {
    enum class Kind { Tumble, Hop, Session, Cumulate };
    Kind kind = Kind::Tumble;
    std::string time_column;
    std::int64_t size_ms = 0;
    std::int64_t slide_ms = 0;  // HOP only
    std::int64_t gap_ms = 0;    // SESSION only
    std::int64_t step_ms = 0;   // CUMULATE only
};

// One aggregate output column produced by the window operator.
//   agg_fn: "sum" / "count" / "min" / "max" / "avg" / "string_agg"
//   input_column: empty for COUNT(*); column name otherwise
//   distinct: COUNT(DISTINCT x) / STRING_AGG(DISTINCT x, sep)
//   separator: STRING_AGG element separator (default ",")
struct AggregateOutput {
    std::string output_name;
    std::string agg_fn;
    std::string input_column;
    std::shared_ptr<arrow::DataType> type;
    bool distinct = false;
    std::string separator;
    double percentile = 0.0;  // fraction for PERCENTILE / APPROX_PERCENTILE
};

// Phase 8: unbounded GROUP BY (no window TVF). Each input record
// updates the running per-group aggregate; the runtime emits one
// Row per input carrying the latest finalised aggregate values.
// Downstream upsert-aware sinks dedupe by the group columns; an
// explicit retract/changelog wire is Phase 8 follow-up work.
class LogicalAggregate final : public LogicalPlan {
public:
    LogicalAggregate(std::unique_ptr<LogicalPlan> input,
                     std::vector<std::string> group_keys,
                     std::vector<AggregateOutput> aggregates,
                     std::shared_ptr<arrow::Schema> schema,
                     std::vector<std::string> key_output_names = {});

    [[nodiscard]] const LogicalPlan& input() const noexcept { return *input_; }
    [[nodiscard]] std::unique_ptr<LogicalPlan>& input_mut() noexcept { return input_; }
    [[nodiscard]] const std::vector<std::string>& group_keys() const noexcept {
        return group_keys_;
    }
    // Output column name for each group key, parallel to group_keys(). Honours a
    // SELECT alias on the key (`GROUP BY user_id` + `SELECT user_id AS uid` ->
    // "uid"); defaults to the raw key name. Empty means "same as group_keys".
    [[nodiscard]] const std::vector<std::string>& key_output_names() const noexcept {
        return key_output_names_;
    }
    [[nodiscard]] const std::vector<AggregateOutput>& aggregates() const noexcept {
        return aggregates_;
    }

    [[nodiscard]] std::string kind() const override { return "Aggregate"; }
    [[nodiscard]] std::shared_ptr<arrow::Schema> schema() const override { return schema_; }
    [[nodiscard]] std::vector<const LogicalPlan*> inputs() const override { return {input_.get()}; }

private:
    std::unique_ptr<LogicalPlan> input_;
    std::vector<std::string> group_keys_;
    std::vector<AggregateOutput> aggregates_;
    std::shared_ptr<arrow::Schema> schema_;
    std::vector<std::string> key_output_names_;
};

class LogicalWindowAggregate final : public LogicalPlan {
public:
    LogicalWindowAggregate(std::unique_ptr<LogicalPlan> input,
                           WindowSpec window,
                           std::vector<std::string> group_keys,
                           std::vector<AggregateOutput> aggregates,
                           std::shared_ptr<arrow::Schema> schema,
                           std::vector<std::string> key_output_names = {});

    [[nodiscard]] const LogicalPlan& input() const noexcept { return *input_; }
    [[nodiscard]] std::unique_ptr<LogicalPlan>& input_mut() noexcept { return input_; }
    [[nodiscard]] const WindowSpec& window() const noexcept { return window_; }
    [[nodiscard]] const std::vector<std::string>& group_keys() const noexcept {
        return group_keys_;
    }
    // Output column name per group key, parallel to group_keys(); see
    // LogicalAggregate::key_output_names().
    [[nodiscard]] const std::vector<std::string>& key_output_names() const noexcept {
        return key_output_names_;
    }
    [[nodiscard]] const std::vector<AggregateOutput>& aggregates() const noexcept {
        return aggregates_;
    }

    [[nodiscard]] std::string kind() const override { return "WindowAggregate"; }
    [[nodiscard]] std::shared_ptr<arrow::Schema> schema() const override { return schema_; }
    [[nodiscard]] std::vector<const LogicalPlan*> inputs() const override { return {input_.get()}; }

private:
    std::unique_ptr<LogicalPlan> input_;
    WindowSpec window_;
    std::vector<std::string> group_keys_;
    std::vector<AggregateOutput> aggregates_;
    std::shared_ptr<arrow::Schema> schema_;
    std::vector<std::string> key_output_names_;
};

// #61 phase 2: MATCH_RECOGNIZE row-pattern matching, lowered onto the CEP
// engine at runtime (a named match_recognize_row op wrapping CepOperator<Row,
// Row>). The binder has resolved column types, lowered DEFINE predicates to
// json_predicate IR (serialized), and resolved MEASURES to FIRST/LAST(var.col)
// specs. v1 subset documented in ast.hpp / preparse.
struct MrPatternStep {
    std::string name;
    std::uint32_t min_count = 1;
    std::uint32_t max_count = 1;
};
struct MrDefineSpec {
    std::string var;
    std::string predicate_json;  // json_predicate IR over a single input row
};
struct MrMeasureSpec {
    std::string output_name;
    std::string fn;      // "first" | "last"
    std::string var;     // pattern variable
    std::string column;  // input column
    std::shared_ptr<arrow::DataType> type;
};

class LogicalMatchRecognize final : public LogicalPlan {
public:
    LogicalMatchRecognize(std::unique_ptr<LogicalPlan> input,
                          std::vector<std::string> partition_columns,
                          std::string order_column,
                          std::vector<MrPatternStep> pattern,
                          std::vector<MrDefineSpec> defines,
                          std::vector<MrMeasureSpec> measures,
                          std::shared_ptr<arrow::Schema> schema)
        : input_(std::move(input)),
          partition_columns_(std::move(partition_columns)),
          order_column_(std::move(order_column)),
          pattern_(std::move(pattern)),
          defines_(std::move(defines)),
          measures_(std::move(measures)),
          schema_(std::move(schema)) {}

    [[nodiscard]] const LogicalPlan& input() const noexcept { return *input_; }
    [[nodiscard]] std::unique_ptr<LogicalPlan>& input_mut() noexcept { return input_; }
    [[nodiscard]] const std::vector<std::string>& partition_columns() const noexcept {
        return partition_columns_;
    }
    [[nodiscard]] const std::string& order_column() const noexcept { return order_column_; }
    [[nodiscard]] const std::vector<MrPatternStep>& pattern() const noexcept { return pattern_; }
    [[nodiscard]] const std::vector<MrDefineSpec>& defines() const noexcept { return defines_; }
    [[nodiscard]] const std::vector<MrMeasureSpec>& measures() const noexcept { return measures_; }

    [[nodiscard]] std::string kind() const override { return "MatchRecognize"; }
    [[nodiscard]] std::shared_ptr<arrow::Schema> schema() const override { return schema_; }
    [[nodiscard]] std::vector<const LogicalPlan*> inputs() const override { return {input_.get()}; }

private:
    std::unique_ptr<LogicalPlan> input_;
    std::vector<std::string> partition_columns_;
    std::string order_column_;
    std::vector<MrPatternStep> pattern_;
    std::vector<MrDefineSpec> defines_;
    std::vector<MrMeasureSpec> measures_;
    std::shared_ptr<arrow::Schema> schema_;
};

// Process-table-function (SQLOPT PTF): a registered keyed stateful Row->Rows
// function applied to a single input table, optionally PARTITION BY columns.
// The output schema is the function's declared columns (read from PtfRegistry at
// bind time) - a FRESH schema, like LogicalMatchRecognize, not a passthrough.
class LogicalProcessTableFunction final : public LogicalPlan {
public:
    LogicalProcessTableFunction(std::unique_ptr<LogicalPlan> input,
                                std::vector<std::string> partition_columns,
                                std::string fn_name,
                                std::shared_ptr<arrow::Schema> schema)
        : input_(std::move(input)),
          partition_columns_(std::move(partition_columns)),
          fn_name_(std::move(fn_name)),
          schema_(std::move(schema)) {}

    [[nodiscard]] const LogicalPlan& input() const noexcept { return *input_; }
    [[nodiscard]] std::unique_ptr<LogicalPlan>& input_mut() noexcept { return input_; }
    [[nodiscard]] const std::vector<std::string>& partition_columns() const noexcept {
        return partition_columns_;
    }
    [[nodiscard]] const std::string& fn_name() const noexcept { return fn_name_; }

    [[nodiscard]] std::string kind() const override { return "ProcessTableFunction"; }
    [[nodiscard]] std::shared_ptr<arrow::Schema> schema() const override { return schema_; }
    [[nodiscard]] std::vector<const LogicalPlan*> inputs() const override { return {input_.get()}; }

private:
    std::unique_ptr<LogicalPlan> input_;
    std::vector<std::string> partition_columns_;
    std::string fn_name_;
    std::shared_ptr<arrow::Schema> schema_;
};

// Join variety for the stream-stream joins (equi and interval). Inner emits
// matched pairs only; the outer variants additionally emit null-padded rows for
// unmatched rows on the kept side(s). The equi join retracts a null-padded row
// (changelog delete) when a match later arrives; the interval join's window is
// finite, so its null-pad at eviction is final (no retraction).
enum class JoinType : std::uint8_t { Inner, LeftOuter, RightOuter, FullOuter };

// Phase 5: stream-stream interval join. The condition pattern this
// node represents:
//
//   a JOIN b ON a.<left_key> = b.<right_key>
//          AND a.<left_ts>  BETWEEN b.<right_ts> + lower_offset_ms
//                              AND b.<right_ts> + upper_offset_ms
//
// (offsets can be negative for the lower bound). The binder folds
// arbitrary INTERVAL syntax into the millisecond offsets at bind time.
class LogicalIntervalJoin final : public LogicalPlan {
public:
    LogicalIntervalJoin(std::unique_ptr<LogicalPlan> left,
                        std::unique_ptr<LogicalPlan> right,
                        std::string left_alias,
                        std::string right_alias,
                        std::string left_key_column,
                        std::string right_key_column,
                        std::string left_ts_column,
                        std::string right_ts_column,
                        std::int64_t lower_offset_ms,
                        std::int64_t upper_offset_ms,
                        std::shared_ptr<arrow::Schema> schema,
                        JoinType join_type = JoinType::Inner);

    [[nodiscard]] const LogicalPlan& left() const noexcept { return *left_; }
    [[nodiscard]] const LogicalPlan& right() const noexcept { return *right_; }
    // Mutable child slots, used by the optimizer's predicate pushdown (INNER only).
    [[nodiscard]] std::unique_ptr<LogicalPlan>& left_mut() noexcept { return left_; }
    [[nodiscard]] std::unique_ptr<LogicalPlan>& right_mut() noexcept { return right_; }
    [[nodiscard]] const std::string& left_alias() const noexcept { return left_alias_; }
    [[nodiscard]] const std::string& right_alias() const noexcept { return right_alias_; }
    [[nodiscard]] const std::string& left_key_column() const noexcept { return left_key_column_; }
    [[nodiscard]] const std::string& right_key_column() const noexcept { return right_key_column_; }
    [[nodiscard]] const std::string& left_ts_column() const noexcept { return left_ts_column_; }
    [[nodiscard]] const std::string& right_ts_column() const noexcept { return right_ts_column_; }
    [[nodiscard]] std::int64_t lower_offset_ms() const noexcept { return lower_offset_ms_; }
    [[nodiscard]] std::int64_t upper_offset_ms() const noexcept { return upper_offset_ms_; }
    [[nodiscard]] JoinType join_type() const noexcept { return join_type_; }

    [[nodiscard]] std::string kind() const override { return "IntervalJoin"; }
    [[nodiscard]] std::shared_ptr<arrow::Schema> schema() const override { return schema_; }
    [[nodiscard]] std::vector<const LogicalPlan*> inputs() const override {
        return {left_.get(), right_.get()};
    }

private:
    std::unique_ptr<LogicalPlan> left_;
    std::unique_ptr<LogicalPlan> right_;
    std::string left_alias_;
    std::string right_alias_;
    std::string left_key_column_;
    std::string right_key_column_;
    std::string left_ts_column_;
    std::string right_ts_column_;
    std::int64_t lower_offset_ms_;
    std::int64_t upper_offset_ms_;
    std::shared_ptr<arrow::Schema> schema_;
    JoinType join_type_;
};

// Analytics depth: which ranking function a Top-N node implements.
// ROW_NUMBER assigns a unique 1..N per partition; RANK gives tied
// rows the same rank with a gap after the tie; DENSE_RANK gives tied
// rows the same rank with no gap. The kind changes which rows pass a
// `WHERE rn <= N` filter and how the runtime evicts on displacement.
enum class RankKind { RowNumber, Rank, DenseRank };

// Phase 21c: TOP-N-per-partition, the recognised target shape of
// `SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY p
// ORDER BY o) AS rn FROM t) WHERE rn <= N`. The binder rewrites
// the LogicalRowNumber + outer WHERE pattern into this node.
//
// Runtime semantics: maintains a per-partition bounded heap of N
// records ordered by the sort spec, and emits an insert for each
// new record that lands in the top N plus a delete for the
// displaced one when the heap was already full. Records are
// tagged via the __row_kind synthetic field defined in
// row_kind.hpp. The output schema matches the input - the rn
// column is purely conceptual and never materialised on the wire.
class LogicalTopNPerKey final : public LogicalPlan {
public:
    LogicalTopNPerKey(std::unique_ptr<LogicalPlan> input,
                      std::vector<std::string> partition_columns,
                      std::vector<std::string> sort_columns,
                      std::vector<bool> sort_descending,
                      std::int64_t count,
                      RankKind rank_kind = RankKind::RowNumber);

    [[nodiscard]] const LogicalPlan& input() const noexcept { return *input_; }
    [[nodiscard]] std::unique_ptr<LogicalPlan>& input_mut() noexcept { return input_; }
    [[nodiscard]] const std::vector<std::string>& partition_columns() const noexcept {
        return partition_columns_;
    }
    [[nodiscard]] const std::vector<std::string>& sort_columns() const noexcept {
        return sort_columns_;
    }
    [[nodiscard]] const std::vector<bool>& sort_descending() const noexcept {
        return sort_descending_;
    }
    [[nodiscard]] std::int64_t count() const noexcept { return count_; }
    [[nodiscard]] RankKind rank_kind() const noexcept { return rank_kind_; }

    [[nodiscard]] std::string kind() const override { return "TopNPerKey"; }
    [[nodiscard]] std::shared_ptr<arrow::Schema> schema() const override {
        return input_->schema();
    }
    [[nodiscard]] std::vector<const LogicalPlan*> inputs() const override { return {input_.get()}; }

private:
    std::unique_ptr<LogicalPlan> input_;
    std::vector<std::string> partition_columns_;
    std::vector<std::string> sort_columns_;
    std::vector<bool> sort_descending_;
    std::int64_t count_;
    RankKind rank_kind_;
};

// Phase 21b: window function projection. Carries the bound inner
// plan plus the partition / sort spec extracted from
// `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)`. Its output
// schema is the input's schema with a synthetic BIGINT column
// (`output_name`, conventionally `rn`) appended. By itself this
// node is not a viable runtime shape - emitting ROW_NUMBER over
// an unbounded stream needs unbounded state and isn't meaningful.
// Phase 21c's pattern matcher pairs it with a `WHERE rn <= N`
// outer filter and rewrites the combination into a bounded
// LogicalTopNPerKey; everything else is rejected at planning time.
class LogicalRowNumber final : public LogicalPlan {
public:
    LogicalRowNumber(std::unique_ptr<LogicalPlan> input,
                     std::vector<std::string> partition_columns,
                     std::vector<std::string> sort_columns,
                     std::vector<bool> sort_descending,
                     std::string output_name,
                     RankKind rank_kind = RankKind::RowNumber);

    [[nodiscard]] const LogicalPlan& input() const noexcept { return *input_; }
    [[nodiscard]] std::unique_ptr<LogicalPlan>& input_mut() noexcept { return input_; }
    [[nodiscard]] const std::vector<std::string>& partition_columns() const noexcept {
        return partition_columns_;
    }
    [[nodiscard]] const std::vector<std::string>& sort_columns() const noexcept {
        return sort_columns_;
    }
    [[nodiscard]] const std::vector<bool>& sort_descending() const noexcept {
        return sort_descending_;
    }
    [[nodiscard]] const std::string& output_name() const noexcept { return output_name_; }
    [[nodiscard]] RankKind rank_kind() const noexcept { return rank_kind_; }

    // Phase 21c: the pattern matcher rewrites this node into a
    // LogicalTopNPerKey by stealing the inner plan. The owning
    // wrapper is then dropped.
    std::unique_ptr<LogicalPlan> release_input() { return std::move(input_); }

    [[nodiscard]] std::string kind() const override { return "RowNumber"; }
    [[nodiscard]] std::shared_ptr<arrow::Schema> schema() const override { return schema_; }
    [[nodiscard]] std::vector<const LogicalPlan*> inputs() const override { return {input_.get()}; }

private:
    std::unique_ptr<LogicalPlan> input_;
    std::vector<std::string> partition_columns_;
    std::vector<std::string> sort_columns_;
    std::vector<bool> sort_descending_;
    std::string output_name_;
    RankKind rank_kind_;
    std::shared_ptr<arrow::Schema> schema_;
};

// Phase 18: stream-stream INNER equi-join.
//
//   a JOIN b ON a.<left_key> = b.<right_key>
//
// State is unbounded: every record seen on each side is retained
// forever, indexed by its join-key string, so a later arrival on
// the other side joins against the full history. Suitable for
// bounded sources or for snapshot-style joins; long-running streams
// need TTL or changelog semantics (future work).
class LogicalEquiJoin final : public LogicalPlan {
public:
    LogicalEquiJoin(std::unique_ptr<LogicalPlan> left,
                    std::unique_ptr<LogicalPlan> right,
                    std::string left_alias,
                    std::string right_alias,
                    std::string left_key_column,
                    std::string right_key_column,
                    std::shared_ptr<arrow::Schema> schema,
                    JoinType join_type = JoinType::Inner);

    [[nodiscard]] const LogicalPlan& left() const noexcept { return *left_; }
    [[nodiscard]] const LogicalPlan& right() const noexcept { return *right_; }
    // Mutable child slots, used by the optimizer's predicate pushdown to wrap a
    // side in a LogicalFilter (`join.left_mut() = make_filter(move(join.left_mut()), ...)`).
    [[nodiscard]] std::unique_ptr<LogicalPlan>& left_mut() noexcept { return left_; }
    [[nodiscard]] std::unique_ptr<LogicalPlan>& right_mut() noexcept { return right_; }
    [[nodiscard]] const std::string& left_alias() const noexcept { return left_alias_; }
    [[nodiscard]] const std::string& right_alias() const noexcept { return right_alias_; }
    [[nodiscard]] const std::string& left_key_column() const noexcept { return left_key_column_; }
    [[nodiscard]] const std::string& right_key_column() const noexcept { return right_key_column_; }
    [[nodiscard]] JoinType join_type() const noexcept { return join_type_; }

    [[nodiscard]] std::string kind() const override { return "EquiJoin"; }
    [[nodiscard]] std::shared_ptr<arrow::Schema> schema() const override { return schema_; }
    [[nodiscard]] std::vector<const LogicalPlan*> inputs() const override {
        return {left_.get(), right_.get()};
    }

private:
    std::unique_ptr<LogicalPlan> left_;
    std::unique_ptr<LogicalPlan> right_;
    std::string left_alias_;
    std::string right_alias_;
    std::string left_key_column_;
    std::string right_key_column_;
    std::shared_ptr<arrow::Schema> schema_;
    JoinType join_type_;
};

// Lookup (enrichment) join. The probe stream (input) is enriched
// per-row against a `connector='lookup'` dimension table whose
// `function` names a registered Row -> async::Task<Row> coroutine.
// Unlike LogicalEquiJoin this is not a stream-stream join: only the
// probe side is a real input; the dim side is the async lookup. Output
// schema mirrors a normal join (probe columns aliased <probe_alias>_<c>
// then dim columns aliased <dim_alias>_<c>), so the surrounding
// projection / sink machinery is identical. `outer` = true is a LEFT
// join (probe rows with no dim match survive, dim columns null);
// otherwise INNER (the physical plan drops misses with a trailing
// `<dim_alias>_<dim_key> IS NOT NULL` filter). Only the probe-on-left
// shape is built (RIGHT/FULL would require enumerating the dim, which a
// keyed lookup source cannot do); the binder rejects the rest.
class LogicalLookupJoin final : public LogicalPlan {
public:
    LogicalLookupJoin(std::unique_ptr<LogicalPlan> input,
                      std::string function_name,
                      std::string probe_alias,
                      std::string dim_alias,
                      std::vector<std::string> probe_columns,
                      std::vector<std::string> dim_columns,
                      std::string dim_key_column,
                      bool outer,
                      std::shared_ptr<arrow::Schema> schema);

    [[nodiscard]] const LogicalPlan& input() const noexcept { return *input_; }
    // Mutable probe-input slot, used by the optimizer's predicate pushdown to
    // push a probe-side conjunct below the lookup (the preserved side, always
    // safe; the dim is an async function with nothing to push into).
    [[nodiscard]] std::unique_ptr<LogicalPlan>& input_mut() noexcept { return input_; }
    [[nodiscard]] const std::string& function_name() const noexcept { return function_name_; }
    [[nodiscard]] const std::string& probe_alias() const noexcept { return probe_alias_; }
    [[nodiscard]] const std::string& dim_alias() const noexcept { return dim_alias_; }
    [[nodiscard]] const std::vector<std::string>& probe_columns() const noexcept {
        return probe_columns_;
    }
    [[nodiscard]] const std::vector<std::string>& dim_columns() const noexcept {
        return dim_columns_;
    }
    [[nodiscard]] const std::string& dim_key_column() const noexcept { return dim_key_column_; }
    [[nodiscard]] bool outer() const noexcept { return outer_; }

    [[nodiscard]] std::string kind() const override { return "LookupJoin"; }
    [[nodiscard]] std::shared_ptr<arrow::Schema> schema() const override { return schema_; }
    [[nodiscard]] std::vector<const LogicalPlan*> inputs() const override { return {input_.get()}; }

private:
    std::unique_ptr<LogicalPlan> input_;
    std::string function_name_;
    std::string probe_alias_;
    std::string dim_alias_;
    std::vector<std::string> probe_columns_;
    std::vector<std::string> dim_columns_;
    std::string dim_key_column_;
    bool outer_;
    std::shared_ptr<arrow::Schema> schema_;
};

// Phase 10: SELECT DISTINCT dedupe.
//
// Pass-through schema; the runtime maintains a per-key seen-set
// and emits each unique output Row at most once. State is unbounded
// for now (no TTL), matching the unbounded GROUP BY semantics.
class LogicalDistinct final : public LogicalPlan {
public:
    explicit LogicalDistinct(std::unique_ptr<LogicalPlan> input);

    [[nodiscard]] const LogicalPlan& input() const noexcept { return *input_; }
    [[nodiscard]] std::unique_ptr<LogicalPlan>& input_mut() noexcept { return input_; }

    [[nodiscard]] std::string kind() const override { return "Distinct"; }
    [[nodiscard]] std::shared_ptr<arrow::Schema> schema() const override {
        return input_->schema();
    }
    [[nodiscard]] std::vector<const LogicalPlan*> inputs() const override { return {input_.get()}; }

private:
    std::unique_ptr<LogicalPlan> input_;
};

// Phase 13: UNION ALL. Two children with structurally-identical
// schemas; the runtime merges records from both sides into one
// stream. Schema is taken from the left input - the binder validates
// the right matches column-by-column.
class LogicalUnion final : public LogicalPlan {
public:
    LogicalUnion(std::unique_ptr<LogicalPlan> left, std::unique_ptr<LogicalPlan> right);

    [[nodiscard]] const LogicalPlan& left() const noexcept { return *left_; }
    [[nodiscard]] const LogicalPlan& right() const noexcept { return *right_; }

    [[nodiscard]] std::string kind() const override { return "Union"; }
    [[nodiscard]] std::shared_ptr<arrow::Schema> schema() const override { return left_->schema(); }
    [[nodiscard]] std::vector<const LogicalPlan*> inputs() const override {
        return {left_.get(), right_.get()};
    }

private:
    std::unique_ptr<LogicalPlan> left_;
    std::unique_ptr<LogicalPlan> right_;
};

// Set operation INTERSECT / EXCEPT (distinct). Two children with
// union-compatible schemas; rows are matched on their full content
// (NULL = NULL, per SQL set-op semantics) and the output is distinct.
// INTERSECT keeps rows present in both; EXCEPT keeps rows in the left
// not present in the right (changelog: a left row is retracted when the
// right side later produces it). UNION distinct does not use this node
// (it lowers to Distinct(Union)); only INTERSECT/EXCEPT need the
// two-sided presence tracking. Schema is the left input's.
class LogicalSetOp final : public LogicalPlan {
public:
    LogicalSetOp(std::unique_ptr<LogicalPlan> left,
                 std::unique_ptr<LogicalPlan> right,
                 bool is_except,
                 bool all = false);

    [[nodiscard]] const LogicalPlan& left() const noexcept { return *left_; }
    [[nodiscard]] const LogicalPlan& right() const noexcept { return *right_; }
    [[nodiscard]] bool is_except() const noexcept { return is_except_; }
    // true for INTERSECT ALL / EXCEPT ALL (multiset); false keeps the
    // distinct (set) semantics.
    [[nodiscard]] bool is_all() const noexcept { return all_; }

    [[nodiscard]] std::string kind() const override { return "SetOp"; }
    [[nodiscard]] std::shared_ptr<arrow::Schema> schema() const override { return left_->schema(); }
    [[nodiscard]] std::vector<const LogicalPlan*> inputs() const override {
        return {left_.get(), right_.get()};
    }

private:
    std::unique_ptr<LogicalPlan> left_;
    std::unique_ptr<LogicalPlan> right_;
    bool is_except_;
    bool all_;
};

// Phase 17: TOP-N (ORDER BY + LIMIT). Pass-through schema; the
// runtime maintains a bounded heap of size `count` keyed by the
// sort columns and emits the top-n at end-of-stream. ORDER BY
// without LIMIT is rejected at the binder.
class LogicalTopN final : public LogicalPlan {
public:
    LogicalTopN(std::unique_ptr<LogicalPlan> input,
                std::vector<std::string> sort_columns,
                std::vector<bool> sort_descending,
                std::int64_t count,
                std::int64_t offset = 0);

    [[nodiscard]] const LogicalPlan& input() const noexcept { return *input_; }
    [[nodiscard]] std::unique_ptr<LogicalPlan>& input_mut() noexcept { return input_; }
    [[nodiscard]] const std::vector<std::string>& sort_columns() const noexcept {
        return sort_columns_;
    }
    [[nodiscard]] const std::vector<bool>& sort_descending() const noexcept {
        return sort_descending_;
    }
    [[nodiscard]] std::int64_t count() const noexcept { return count_; }
    [[nodiscard]] std::int64_t offset() const noexcept { return offset_; }

    [[nodiscard]] std::string kind() const override { return "TopN"; }
    [[nodiscard]] std::shared_ptr<arrow::Schema> schema() const override {
        return input_->schema();
    }
    [[nodiscard]] std::vector<const LogicalPlan*> inputs() const override { return {input_.get()}; }

private:
    std::unique_ptr<LogicalPlan> input_;
    std::vector<std::string> sort_columns_;
    std::vector<bool> sort_descending_;
    std::int64_t count_;
    std::int64_t offset_;
};

// Phase 11: LIMIT n. Pass-through schema; the runtime drops every
// record after the n-th. Local to each subtask: at parallelism > 1
// the wire emits up to n records per subtask, so author SQL with
// LIMIT against a single-source pipeline if global semantics matter.
class LogicalLimit final : public LogicalPlan {
public:
    LogicalLimit(std::unique_ptr<LogicalPlan> input, std::int64_t count, std::int64_t offset = 0);

    [[nodiscard]] const LogicalPlan& input() const noexcept { return *input_; }
    [[nodiscard]] std::unique_ptr<LogicalPlan>& input_mut() noexcept { return input_; }
    [[nodiscard]] std::int64_t count() const noexcept { return count_; }
    [[nodiscard]] std::int64_t offset() const noexcept { return offset_; }

    [[nodiscard]] std::string kind() const override { return "Limit"; }
    [[nodiscard]] std::shared_ptr<arrow::Schema> schema() const override {
        return input_->schema();
    }
    [[nodiscard]] std::vector<const LogicalPlan*> inputs() const override { return {input_.get()}; }

private:
    std::unique_ptr<LogicalPlan> input_;
    std::int64_t count_;
    std::int64_t offset_;
};

// Analytics depth: OVER (running) aggregates. Lowered from
//   SELECT *, agg(x) OVER (PARTITION BY k ORDER BY <event_time>) AS c, ...
// Append-only: each input row produces exactly one output row carrying
// the original columns plus the OVER output columns. A row is emitted
// once the watermark passes its event time, so the running frame up to
// and including it is complete - no retraction needed. v1 supports only
// the running frame (UNBOUNDED PRECEDING ... CURRENT ROW); the order
// column must be the source's event-time column.
struct OverOutput {
    std::string output_name;
    std::string fn;               // sum/count/avg/min/max/first_value/last_value/lag
    std::string input_column;     // "" for COUNT(*)
    std::int64_t lag_offset = 1;  // LAG(expr, k) only
    // Window frame (Wave 7): 0 = running (UNBOUNDED PRECEDING ... CURRENT
    // ROW), 1 = ROWS <n> PRECEDING, 2 = RANGE <n> PRECEDING (end CURRENT
    // ROW). frame_start is the row count (ROWS) or ms span (RANGE).
    int frame_mode = 0;
    std::int64_t frame_start = 0;
    std::shared_ptr<arrow::DataType> type;
};

class LogicalOverAggregate final : public LogicalPlan {
public:
    LogicalOverAggregate(std::unique_ptr<LogicalPlan> input,
                         std::vector<std::string> partition_columns,
                         std::string order_time_column,
                         std::vector<OverOutput> outputs,
                         std::shared_ptr<arrow::Schema> schema);

    [[nodiscard]] const LogicalPlan& input() const noexcept { return *input_; }
    [[nodiscard]] std::unique_ptr<LogicalPlan>& input_mut() noexcept { return input_; }
    [[nodiscard]] const std::vector<std::string>& partition_columns() const noexcept {
        return partition_columns_;
    }
    [[nodiscard]] const std::string& order_time_column() const noexcept {
        return order_time_column_;
    }
    [[nodiscard]] const std::vector<OverOutput>& outputs() const noexcept { return outputs_; }

    [[nodiscard]] std::string kind() const override { return "OverAggregate"; }
    [[nodiscard]] std::shared_ptr<arrow::Schema> schema() const override { return schema_; }
    [[nodiscard]] std::vector<const LogicalPlan*> inputs() const override { return {input_.get()}; }

private:
    std::unique_ptr<LogicalPlan> input_;
    std::vector<std::string> partition_columns_;
    std::string order_time_column_;
    std::vector<OverOutput> outputs_;
    std::shared_ptr<arrow::Schema> schema_;
};

// Inc 4: semi / anti join, the lowering target of IN / NOT IN and
// (correlated, equality) EXISTS / NOT EXISTS. Unlike LogicalEquiJoin
// (which emits both sides' columns), a semi/anti join is a filter on
// the left: its schema is the left input's schema unchanged. anti=true
// is NOT IN / NOT EXISTS; the runtime applies SQL-standard NULL
// semantics for NOT IN (a NULL on the right makes every probe UNKNOWN).
class LogicalSemiJoin final : public LogicalPlan {
public:
    // Composite key columns are paired positionally (left[i] = right[i]).
    // A single-column IN / single-equality EXISTS uses one-element vectors.
    LogicalSemiJoin(std::unique_ptr<LogicalPlan> left,
                    std::unique_ptr<LogicalPlan> right,
                    std::vector<std::string> left_key_columns,
                    std::vector<std::string> right_key_columns,
                    bool anti,
                    bool null_aware = true);

    [[nodiscard]] const LogicalPlan& left() const noexcept { return *left_; }
    [[nodiscard]] const LogicalPlan& right() const noexcept { return *right_; }
    [[nodiscard]] const std::vector<std::string>& left_key_columns() const noexcept {
        return left_key_columns_;
    }
    [[nodiscard]] const std::vector<std::string>& right_key_columns() const noexcept {
        return right_key_columns_;
    }
    [[nodiscard]] bool anti() const noexcept { return anti_; }
    // true = IN / NOT IN (SQL 3VL NULL poison on the anti side); false =
    // EXISTS / NOT EXISTS (plain anti, NULL keys simply do not match).
    [[nodiscard]] bool null_aware() const noexcept { return null_aware_; }

    [[nodiscard]] std::string kind() const override { return "SemiJoin"; }
    [[nodiscard]] std::shared_ptr<arrow::Schema> schema() const override { return left_->schema(); }
    [[nodiscard]] std::vector<const LogicalPlan*> inputs() const override {
        return {left_.get(), right_.get()};
    }

private:
    std::unique_ptr<LogicalPlan> left_;
    std::unique_ptr<LogicalPlan> right_;
    std::vector<std::string> left_key_columns_;
    std::vector<std::string> right_key_columns_;
    bool anti_;
    bool null_aware_;
};

// Inc 4: uncorrelated scalar-subquery filter. Lowered from
//   ... WHERE main.col <op> (SELECT agg(y) FROM s)
// The right input is a single-row, single-column aggregate; the op
// caches its latest value and passes main rows for which
// `main[test_column] <op> scalar` holds. comparison_op is one of
// eq/ne/lt/le/gt/ge (oriented so the main column is the left operand).
// Schema is the main input's schema unchanged.
class LogicalScalarBroadcast final : public LogicalPlan {
public:
    LogicalScalarBroadcast(std::unique_ptr<LogicalPlan> main,
                           std::unique_ptr<LogicalPlan> scalar,
                           std::string test_column,
                           std::string comparison_op,
                           std::string scalar_column);

    [[nodiscard]] const LogicalPlan& main() const noexcept { return *main_; }
    [[nodiscard]] const LogicalPlan& scalar() const noexcept { return *scalar_; }
    [[nodiscard]] const std::string& test_column() const noexcept { return test_column_; }
    [[nodiscard]] const std::string& comparison_op() const noexcept { return comparison_op_; }
    [[nodiscard]] const std::string& scalar_column() const noexcept { return scalar_column_; }

    [[nodiscard]] std::string kind() const override { return "ScalarBroadcast"; }
    [[nodiscard]] std::shared_ptr<arrow::Schema> schema() const override { return main_->schema(); }
    [[nodiscard]] std::vector<const LogicalPlan*> inputs() const override {
        return {main_.get(), scalar_.get()};
    }

private:
    std::unique_ptr<LogicalPlan> main_;
    std::unique_ptr<LogicalPlan> scalar_;
    std::string test_column_;
    std::string comparison_op_;
    std::string scalar_column_;
};

// Scalar subquery in the SELECT list: append the broadcast scalar (the single
// value produced by `scalar`, an empty-group aggregate subplan) to every row
// of `main` as a new column `output_column`. Schema = main schema + that
// column. Like LogicalScalarBroadcast this is append-only against the current
// scalar (v1); an empty subquery yields a NULL appended column.
class LogicalScalarProject final : public LogicalPlan {
public:
    LogicalScalarProject(std::unique_ptr<LogicalPlan> main,
                         std::unique_ptr<LogicalPlan> scalar,
                         std::string output_column,
                         std::shared_ptr<arrow::DataType> output_type);

    [[nodiscard]] const LogicalPlan& main() const noexcept { return *main_; }
    [[nodiscard]] const LogicalPlan& scalar() const noexcept { return *scalar_; }
    [[nodiscard]] const std::string& output_column() const noexcept { return output_column_; }
    // Name to read on the scalar side; in v1 the subplan emits a single column
    // named output_column_, so scalar_column_ == output_column_.
    [[nodiscard]] const std::string& scalar_column() const noexcept { return scalar_column_; }

    [[nodiscard]] std::string kind() const override { return "ScalarProject"; }
    [[nodiscard]] std::shared_ptr<arrow::Schema> schema() const override { return schema_; }
    [[nodiscard]] std::vector<const LogicalPlan*> inputs() const override {
        return {main_.get(), scalar_.get()};
    }

private:
    std::unique_ptr<LogicalPlan> main_;
    std::unique_ptr<LogicalPlan> scalar_;
    std::string output_column_;
    std::string scalar_column_;
    std::shared_ptr<arrow::Schema> schema_;
};

class LogicalSink final : public LogicalPlan {
public:
    LogicalSink(std::unique_ptr<LogicalPlan> input, const TableDef* table);

    [[nodiscard]] const LogicalPlan& input() const noexcept { return *input_; }
    [[nodiscard]] std::unique_ptr<LogicalPlan>& input_mut() noexcept { return input_; }
    [[nodiscard]] const TableDef& table() const noexcept { return *table_; }

    [[nodiscard]] std::string kind() const override { return "Sink"; }
    [[nodiscard]] std::shared_ptr<arrow::Schema> schema() const override {
        return input_->schema();
    }
    [[nodiscard]] std::vector<const LogicalPlan*> inputs() const override { return {input_.get()}; }

private:
    std::unique_ptr<LogicalPlan> input_;
    const TableDef* table_;
};

}  // namespace clink::sql
