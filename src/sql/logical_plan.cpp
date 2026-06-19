#include "clink/sql/logical_plan.hpp"

#include <sstream>
#include <utility>

#include "clink/sql/type.hpp"

namespace clink::sql {

namespace {

std::shared_ptr<arrow::Schema> schema_from_columns(const std::vector<ColumnSpec>& cols) {
    arrow::FieldVector fields;
    fields.reserve(cols.size());
    for (const auto& c : cols) {
        fields.push_back(arrow::field(c.name, c.type));
    }
    return arrow::schema(std::move(fields));
}

}  // namespace

std::string LogicalPlan::explain(int indent) const {
    std::ostringstream out;
    out << std::string(static_cast<std::size_t>(indent) * 2, ' ') << kind();
    // Render the schema as a SQL-shaped column list so EXPLAIN output
    // stays readable.
    auto s = schema();
    if (s) {
        out << "  [";
        for (int i = 0; i < s->num_fields(); ++i) {
            if (i > 0)
                out << ", ";
            out << s->field(i)->name() << " " << arrow_to_sql_type_string(*s->field(i)->type());
        }
        out << "]";
    }
    out << "\n";
    for (const auto* child : inputs()) {
        out << child->explain(indent + 1);
    }
    return out.str();
}

// --- LogicalScan ---------------------------------------------------

LogicalScan::LogicalScan(const TableDef* table) : table_(table) {
    schema_ = schema_from_columns(table->columns);
}

// --- LogicalProject ------------------------------------------------

LogicalProject::LogicalProject(std::unique_ptr<LogicalPlan> input,
                               std::vector<ProjectOutput> outputs)
    : input_(std::move(input)), outputs_(std::move(outputs)) {
    arrow::FieldVector fields;
    fields.reserve(outputs_.size());
    for (const auto& out : outputs_) {
        fields.push_back(arrow::field(out.name, out.type));
    }
    schema_ = arrow::schema(std::move(fields));
}

// --- LogicalFilter -------------------------------------------------

LogicalFilter::LogicalFilter(std::unique_ptr<LogicalPlan> input, std::string predicate_json)
    : input_(std::move(input)), predicate_json_(std::move(predicate_json)) {}

// --- LogicalAsyncMap -----------------------------------------------

LogicalAsyncMap::LogicalAsyncMap(std::unique_ptr<LogicalPlan> input,
                                 std::string function_name,
                                 const TableDef* output_table)
    : input_(std::move(input)), function_name_(std::move(function_name)) {
    schema_ = schema_from_columns(output_table->columns);
}

// --- LogicalAggregate ----------------------------------------------

LogicalAggregate::LogicalAggregate(std::unique_ptr<LogicalPlan> input,
                                   std::vector<std::string> group_keys,
                                   std::vector<AggregateOutput> aggregates,
                                   std::shared_ptr<arrow::Schema> schema,
                                   std::vector<std::string> key_output_names)
    : input_(std::move(input)),
      group_keys_(std::move(group_keys)),
      aggregates_(std::move(aggregates)),
      schema_(std::move(schema)),
      key_output_names_(std::move(key_output_names)) {}

// --- LogicalWindowAggregate ----------------------------------------

LogicalWindowAggregate::LogicalWindowAggregate(std::unique_ptr<LogicalPlan> input,
                                               WindowSpec window,
                                               std::vector<std::string> group_keys,
                                               std::vector<AggregateOutput> aggregates,
                                               std::shared_ptr<arrow::Schema> schema,
                                               std::vector<std::string> key_output_names)
    : input_(std::move(input)),
      window_(std::move(window)),
      group_keys_(std::move(group_keys)),
      aggregates_(std::move(aggregates)),
      schema_(std::move(schema)),
      key_output_names_(std::move(key_output_names)) {}

// --- LogicalIntervalJoin -------------------------------------------

LogicalIntervalJoin::LogicalIntervalJoin(std::unique_ptr<LogicalPlan> left,
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
                                         JoinType join_type)
    : left_(std::move(left)),
      right_(std::move(right)),
      left_alias_(std::move(left_alias)),
      right_alias_(std::move(right_alias)),
      left_key_column_(std::move(left_key_column)),
      right_key_column_(std::move(right_key_column)),
      left_ts_column_(std::move(left_ts_column)),
      right_ts_column_(std::move(right_ts_column)),
      lower_offset_ms_(lower_offset_ms),
      upper_offset_ms_(upper_offset_ms),
      schema_(std::move(schema)),
      join_type_(join_type) {}

// --- LogicalTopNPerKey ---------------------------------------------

LogicalTopNPerKey::LogicalTopNPerKey(std::unique_ptr<LogicalPlan> input,
                                     std::vector<std::string> partition_columns,
                                     std::vector<std::string> sort_columns,
                                     std::vector<bool> sort_descending,
                                     std::int64_t count,
                                     RankKind rank_kind)
    : input_(std::move(input)),
      partition_columns_(std::move(partition_columns)),
      sort_columns_(std::move(sort_columns)),
      sort_descending_(std::move(sort_descending)),
      count_(count),
      rank_kind_(rank_kind) {}

// --- LogicalRowNumber ----------------------------------------------

LogicalRowNumber::LogicalRowNumber(std::unique_ptr<LogicalPlan> input,
                                   std::vector<std::string> partition_columns,
                                   std::vector<std::string> sort_columns,
                                   std::vector<bool> sort_descending,
                                   std::string output_name,
                                   RankKind rank_kind)
    : input_(std::move(input)),
      partition_columns_(std::move(partition_columns)),
      sort_columns_(std::move(sort_columns)),
      sort_descending_(std::move(sort_descending)),
      output_name_(std::move(output_name)),
      rank_kind_(rank_kind) {
    auto in_schema = input_->schema();
    arrow::FieldVector fields;
    fields.reserve(static_cast<std::size_t>(in_schema->num_fields()) + 1);
    for (int i = 0; i < in_schema->num_fields(); ++i) {
        fields.push_back(in_schema->field(i));
    }
    fields.push_back(arrow::field(output_name_, arrow::int64()));
    schema_ = arrow::schema(std::move(fields));
}

// --- LogicalOverAggregate ------------------------------------------

LogicalOverAggregate::LogicalOverAggregate(std::unique_ptr<LogicalPlan> input,
                                           std::vector<std::string> partition_columns,
                                           std::string order_time_column,
                                           std::vector<OverOutput> outputs,
                                           std::shared_ptr<arrow::Schema> schema)
    : input_(std::move(input)),
      partition_columns_(std::move(partition_columns)),
      order_time_column_(std::move(order_time_column)),
      outputs_(std::move(outputs)),
      schema_(std::move(schema)) {}

// --- LogicalSemiJoin -----------------------------------------------

LogicalSemiJoin::LogicalSemiJoin(std::unique_ptr<LogicalPlan> left,
                                 std::unique_ptr<LogicalPlan> right,
                                 std::vector<std::string> left_key_columns,
                                 std::vector<std::string> right_key_columns,
                                 bool anti,
                                 bool null_aware)
    : left_(std::move(left)),
      right_(std::move(right)),
      left_key_columns_(std::move(left_key_columns)),
      right_key_columns_(std::move(right_key_columns)),
      anti_(anti),
      null_aware_(null_aware) {}

// --- LogicalScalarBroadcast ----------------------------------------

LogicalScalarBroadcast::LogicalScalarBroadcast(std::unique_ptr<LogicalPlan> main,
                                               std::unique_ptr<LogicalPlan> scalar,
                                               std::string test_column,
                                               std::string comparison_op,
                                               std::string scalar_column)
    : main_(std::move(main)),
      scalar_(std::move(scalar)),
      test_column_(std::move(test_column)),
      comparison_op_(std::move(comparison_op)),
      scalar_column_(std::move(scalar_column)) {}

// --- LogicalScalarProject ------------------------------------------

LogicalScalarProject::LogicalScalarProject(std::unique_ptr<LogicalPlan> main,
                                           std::unique_ptr<LogicalPlan> scalar,
                                           std::string output_column,
                                           std::shared_ptr<arrow::DataType> output_type)
    : main_(std::move(main)),
      scalar_(std::move(scalar)),
      output_column_(std::move(output_column)),
      scalar_column_(output_column_) {
    // Schema = the main input's fields plus the appended scalar column.
    arrow::FieldVector fields = main_->schema()->fields();
    fields.push_back(arrow::field(output_column_, std::move(output_type)));
    schema_ = arrow::schema(std::move(fields));
}

// --- LogicalEquiJoin -----------------------------------------------

LogicalEquiJoin::LogicalEquiJoin(std::unique_ptr<LogicalPlan> left,
                                 std::unique_ptr<LogicalPlan> right,
                                 std::string left_alias,
                                 std::string right_alias,
                                 std::string left_key_column,
                                 std::string right_key_column,
                                 std::shared_ptr<arrow::Schema> schema,
                                 JoinType join_type)
    : left_(std::move(left)),
      right_(std::move(right)),
      left_alias_(std::move(left_alias)),
      right_alias_(std::move(right_alias)),
      left_key_column_(std::move(left_key_column)),
      right_key_column_(std::move(right_key_column)),
      schema_(std::move(schema)),
      join_type_(join_type) {}

// --- LogicalLookupJoin ---------------------------------------------

LogicalLookupJoin::LogicalLookupJoin(std::unique_ptr<LogicalPlan> input,
                                     std::string function_name,
                                     std::string probe_alias,
                                     std::string dim_alias,
                                     std::vector<std::string> probe_columns,
                                     std::vector<std::string> dim_columns,
                                     std::string dim_key_column,
                                     bool outer,
                                     std::shared_ptr<arrow::Schema> schema)
    : input_(std::move(input)),
      function_name_(std::move(function_name)),
      probe_alias_(std::move(probe_alias)),
      dim_alias_(std::move(dim_alias)),
      probe_columns_(std::move(probe_columns)),
      dim_columns_(std::move(dim_columns)),
      dim_key_column_(std::move(dim_key_column)),
      outer_(outer),
      schema_(std::move(schema)) {}

// --- LogicalDistinct -----------------------------------------------

LogicalDistinct::LogicalDistinct(std::unique_ptr<LogicalPlan> input) : input_(std::move(input)) {}

// --- LogicalUnion --------------------------------------------------

LogicalUnion::LogicalUnion(std::unique_ptr<LogicalPlan> left, std::unique_ptr<LogicalPlan> right)
    : left_(std::move(left)), right_(std::move(right)) {}

// --- LogicalSetOp --------------------------------------------------

LogicalSetOp::LogicalSetOp(std::unique_ptr<LogicalPlan> left,
                           std::unique_ptr<LogicalPlan> right,
                           bool is_except,
                           bool all)
    : left_(std::move(left)), right_(std::move(right)), is_except_(is_except), all_(all) {}

// --- LogicalTopN ---------------------------------------------------

LogicalTopN::LogicalTopN(std::unique_ptr<LogicalPlan> input,
                         std::vector<std::string> sort_columns,
                         std::vector<bool> sort_descending,
                         std::int64_t count,
                         std::int64_t offset)
    : input_(std::move(input)),
      sort_columns_(std::move(sort_columns)),
      sort_descending_(std::move(sort_descending)),
      count_(count),
      offset_(offset) {}

// --- LogicalLimit --------------------------------------------------

LogicalLimit::LogicalLimit(std::unique_ptr<LogicalPlan> input,
                           std::int64_t count,
                           std::int64_t offset)
    : input_(std::move(input)), count_(count), offset_(offset) {}

// --- LogicalSink ---------------------------------------------------

LogicalSink::LogicalSink(std::unique_ptr<LogicalPlan> input, const TableDef* table)
    : input_(std::move(input)), table_(table) {}

}  // namespace clink::sql
