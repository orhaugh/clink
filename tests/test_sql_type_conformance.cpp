// Every value an expression produces has the type the binder declared for it.
//
// The binder types an expression once, from its operands' declared types; the
// evaluator computes a value per record. When the two disagree, the value's kind
// is not its column's: a DOUBLE column receives a DECIMAL, a BIGINT column a
// DOUBLE. What happens next depends on who reads it. The columnar carriers write
// a value they cannot hold as NULL, a typed sink refuses it, and an operator
// downstream takes the value's own arithmetic (integer division where the plan
// said double), so the same query gives different answers for different values.
//
// This suite binds each expression of a generated family against a table with
// a column of every numeric type, evaluates it over every combination of the
// operands' values, and requires each result to be of the declared type.

#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/dag_builder_registry.hpp"
#include "clink/config/decimal.hpp"
#include "clink/config/json.hpp"
#include "clink/operators/json_value_expr.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/sql/binder.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/install.hpp"
#include "clink/sql/logical_plan.hpp"
#include "clink/sql/optimizer.hpp"
#include "clink/sql/parser.hpp"
#include "clink/sql/physical_plan.hpp"
#include "clink/sql/row_columnar_batcher.hpp"
#include "clink/sql/row_kind.hpp"
#include "clink/state/in_memory_state_backend.hpp"

#include "arrow/api.h"

namespace clink::sql {
namespace {

using clink::config::JsonValue;

JsonValue dec(const char* text) {
    return clink::config::make_dec_value(*clink::config::dec_parse(text));
}

struct Column {
    std::string name;
    std::string sql_type;
    std::vector<JsonValue> values;  // as a source delivers them, NULL included
};

const std::vector<Column>& columns() {
    static const std::vector<Column> cols = {
        {"i",
         "BIGINT",
         {JsonValue{std::int64_t{0}},
          JsonValue{std::int64_t{7}},
          JsonValue{std::int64_t{-3}},
          JsonValue{std::int64_t{9007199254740993}},
          JsonValue{nullptr}}},
        {"n",
         "INTEGER",
         {JsonValue{std::int64_t{0}},
          JsonValue{std::int64_t{7}},
          JsonValue{std::int64_t{-3}},
          JsonValue{nullptr}}},
        {"h",
         "SMALLINT",
         {JsonValue{std::int64_t{2}}, JsonValue{std::int64_t{-1}}, JsonValue{nullptr}}},
        {"d",
         "DOUBLE PRECISION",
         {JsonValue{0.0}, JsonValue{2.0}, JsonValue{2.5}, JsonValue{-0.1}, JsonValue{nullptr}}},
        {"r", "REAL", {JsonValue{0.5}, JsonValue{3.0}, JsonValue{nullptr}}},
        {"m",
         "DECIMAL(10, 2)",
         {dec("1.50"), dec("3.00"), dec("-0.01"), dec("0.00"), JsonValue{nullptr}}},
        {"q", "DECIMAL(18, 4)", {dec("2.0000"), dec("0.1250"), JsonValue{nullptr}}},
        {"v",
         "VARCHAR",
         {JsonValue{std::string{"x"}}, JsonValue{std::string{"12"}}, JsonValue{nullptr}}},
        {"b", "BOOLEAN", {JsonValue{true}, JsonValue{false}, JsonValue{nullptr}}},
    };
    return cols;
}

// A sink table per column type an INSERT can name.
const std::vector<std::pair<std::string, std::string>>& sink_types() {
    static const std::vector<std::pair<std::string, std::string>> sinks = {
        {"s_bigint", "BIGINT"},
        {"s_integer", "INTEGER"},
        {"s_smallint", "SMALLINT"},
        {"s_double", "DOUBLE PRECISION"},
        {"s_real", "REAL"},
        {"s_dec", "DECIMAL(10, 2)"},
        {"s_dec4", "DECIMAL(18, 4)"},
        {"s_varchar", "VARCHAR"},
    };
    return sinks;
}

void register_ddl(Catalog& cat, const std::string& ddl) {
    auto s = parse(ddl);
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
}

Catalog make_catalog() {
    Catalog cat;
    std::string ddl = "CREATE TABLE t (";
    for (std::size_t k = 0; k < columns().size(); ++k) {
        ddl += (k ? ", " : "") + columns()[k].name + " " + columns()[k].sql_type;
    }
    ddl += ") WITH (connector='kafka', topic='t', bootstrap='localhost:9092')";
    register_ddl(cat, ddl);
    for (const auto& [name, type] : sink_types()) {
        register_ddl(cat,
                     "CREATE TABLE " + name + " (c " + type + ") WITH (connector='kafka', topic='" +
                         name + "', bootstrap='localhost:9092')");
    }
    return cat;
}

// The kind a declared type requires of a non-NULL value, or "" when the
// type has no rule here.
std::string required_kind(const arrow::DataType& t) {
    switch (t.id()) {
        case arrow::Type::INT64:
        case arrow::Type::INT32:
        case arrow::Type::INT16:
        case arrow::Type::INT8:
            return "integer";
        case arrow::Type::DOUBLE:
        case arrow::Type::FLOAT:
            return "double";
        case arrow::Type::DECIMAL128:
            return "decimal";
        case arrow::Type::BOOL:
            return "boolean";
        case arrow::Type::STRING:
            return "string";
        default:
            return "";
    }
}

std::string kind_of(const JsonValue& v) {
    if (v.is_null()) {
        return "null";
    }
    if (v.is_bool()) {
        return "boolean";
    }
    if (v.is_integral_number()) {
        return "integer";
    }
    if (v.is_number()) {
        return "double";
    }
    if (clink::config::is_dec_string(v)) {
        return "decimal";
    }
    if (v.is_string()) {
        return "string";
    }
    return v.is_array() ? "array" : "object";
}

struct Finding {
    std::string expr;
    std::string declared;
    std::string got;
    std::string example;
};

// The columns an expression reads. Every column name is a single letter, which
// is referenced when it stands alone (not inside a word such as "abs" or "ln").
std::vector<const Column*> referenced(const std::string& expr) {
    std::vector<const Column*> out;
    for (const auto& c : columns()) {
        for (std::size_t p = 0; p < expr.size(); ++p) {
            if (expr[p] != c.name[0]) {
                continue;
            }
            const bool left = p == 0 || !std::isalnum(static_cast<unsigned char>(expr[p - 1]));
            const bool right =
                p + 1 == expr.size() || !std::isalnum(static_cast<unsigned char>(expr[p + 1]));
            if (left && right) {
                out.push_back(&c);
                break;
            }
        }
    }
    return out;
}

// Whether `v`, a non-NULL value, is of declared type `t`: its kind, and for a
// DECIMAL its scale, since a DECIMAL column reads its value back at its own.
std::optional<std::string> mismatch(const arrow::DataType& t, const JsonValue& v) {
    const std::string want = required_kind(t);
    const std::string got = kind_of(v);
    if (want.empty() || got == "null") {
        return std::nullopt;
    }
    if (got != want) {
        return got;
    }
    if (t.id() == arrow::Type::DECIMAL128) {
        const auto d = clink::config::dec_parse(v.as_string());
        const int scale = static_cast<const arrow::Decimal128Type&>(t).scale();
        if (!d || d->scale != scale) {
            return "decimal at scale " + std::to_string(d ? d->scale : -1);
        }
    }
    return std::nullopt;
}

// Evaluate `expr_json` over every combination of the values of the columns
// `source_text` reads, and record each kind of value that is not of `declared`.
std::size_t check_values(const std::string& source_text,
                         const std::string& expr_json,
                         const arrow::DataType& declared,
                         std::vector<Finding>& findings) {
    const auto compiled =
        clink::operators::CompiledValueExpr::compile(clink::config::parse(expr_json));
    const auto cols = referenced(source_text);
    std::vector<std::size_t> idx(cols.size(), 0);
    std::set<std::string> seen;
    std::size_t evaluations = 0;
    while (true) {
        std::map<std::string, JsonValue> row;
        for (const auto& c : columns()) {
            row[c.name] = c.values.front();
        }
        std::string example;
        for (std::size_t k = 0; k < cols.size(); ++k) {
            row[cols[k]->name] = cols[k]->values[idx[k]];
            example += (k ? ", " : "") + cols[k]->name + "=" + row[cols[k]->name].serialize(0);
        }
        auto resolve = [&](const std::string& name) -> JsonValue {
            auto it = row.find(name);
            return it == row.end() ? JsonValue{nullptr} : it->second;
        };
        const clink::operators::ColumnLookup lookup{resolve};
        std::optional<std::string> wrong;
        try {
            wrong = mismatch(declared, compiled.evaluate(lookup));
        } catch (const std::exception& e) {
            wrong = std::string{"an exception: "} + e.what();
        }
        ++evaluations;
        if (wrong && seen.insert(*wrong).second) {
            findings.push_back({source_text, declared.ToString(), *wrong, example});
        }
        std::size_t k = 0;
        for (; k < cols.size(); ++k) {
            if (++idx[k] < cols[k]->values.size()) {
                break;
            }
            idx[k] = 0;
        }
        if (k == cols.size()) {
            return evaluations;
        }
    }
}

std::string report(const std::vector<Finding>& findings) {
    std::ostringstream out;
    for (const auto& f : findings) {
        out << "  " << f.expr << ": declared " << f.declared << ", got " << f.got << " for "
            << f.example << "\n";
    }
    return out.str();
}

// Operands every expression template is instantiated with: each numeric column
// and a literal of each numeric kind.
const std::vector<std::string>& operands() {
    static const std::vector<std::string> ops = {
        "i", "n", "h", "d", "r", "m", "q", "2", "2.5", "0.029"};
    return ops;
}

std::vector<std::string> scalar_family() {
    std::vector<std::string> out;
    for (const auto& a : operands()) {
        for (const auto& b : operands()) {
            for (const char* op : {"+", "-", "*", "/", "%"}) {
                out.push_back(a + " " + op + " " + b);
            }
            for (const char* fn : {"coalesce", "nullif", "greatest", "least", "mod", "power"}) {
                out.push_back(std::string{fn} + "(" + a + ", " + b + ")");
            }
            out.push_back("coalesce(NULL, " + a + ", " + b + ")");
            out.push_back("CASE WHEN b THEN " + a + " ELSE " + b + " END");
        }
        out.push_back("-" + a);
        for (const char* fn :
             {"abs", "floor", "ceil", "round", "trunc", "sign", "sqrt", "exp", "ln", "log10"}) {
            out.push_back(std::string{fn} + "(" + a + ")");
        }
        out.push_back("round(" + a + ", 1)");
        for (const char* t :
             {"BIGINT", "INTEGER", "DOUBLE PRECISION", "REAL", "DECIMAL(12, 3)", "VARCHAR"}) {
            out.push_back("CAST(" + a + " AS " + t + ")");
        }
    }
    return out;
}

TEST(SqlTypeConformance, EveryNumericExpressionIsOfItsDeclaredType) {
    const Catalog cat = make_catalog();
    const Binder binder(cat);
    std::vector<Finding> findings;
    std::vector<std::string> refused;
    std::size_t bound = 0;
    std::size_t evaluations = 0;
    for (const auto& expr : scalar_family()) {
        std::unique_ptr<LogicalPlan> plan;
        try {
            auto script = parse("SELECT " + expr + " AS r FROM t");
            plan = binder.bind_select(std::get<ast::SelectStmt>(script.statements[0]));
        } catch (const std::exception& e) {
            // A refusal is loud, so it is no mismatch. The only one this
            // family meets is a CASE whose branches differ in type.
            EXPECT_NE(std::string{e.what()}.find("CASE"), std::string::npos)
                << expr << ": " << e.what();
            refused.push_back(expr);
            continue;
        }
        ++bound;
        const auto* project = dynamic_cast<const LogicalProject*>(plan.get());
        ASSERT_NE(project, nullptr) << expr;
        const auto& out = project->outputs().at(0);
        evaluations += check_values(expr, out.expr_json, *out.type, findings);
    }
    EXPECT_GT(bound, 1000u);
    EXPECT_GT(evaluations, 10000u);
    EXPECT_TRUE(findings.empty()) << findings.size() << " of " << bound
                                  << " expressions gave a value of another type than declared:\n"
                                  << report(findings);
}

TEST(SqlTypeConformance, EveryInsertedValueIsOfItsColumnsType) {
    // INSERT casts what it assigns: each value a sink column receives is of the
    // column's type, whatever the SELECT's type was, or the INSERT is refused
    // at bind time.
    const Catalog cat = make_catalog();
    const Binder binder(cat);
    std::vector<std::string> sources = operands();
    for (const char* e : {"d * m",
                          "d * 0.029",
                          "m * 0.029",
                          "i + 1",
                          "i / 2",
                          "coalesce(i, 0.5)",
                          "abs(m)",
                          "greatest(n, q)",
                          "CAST(d AS DECIMAL(12, 3))"}) {
        sources.emplace_back(e);
    }
    std::vector<Finding> findings;
    std::map<std::string, std::size_t> accepted;
    for (const auto& [sink, type] : sink_types()) {
        for (const auto& expr : sources) {
            std::unique_ptr<LogicalPlan> plan;
            try {
                auto script = parse("INSERT INTO " + sink + " SELECT " + expr + " FROM t");
                plan = binder.bind_insert(std::get<ast::InsertStmt>(script.statements[0]));
            } catch (const std::exception& e) {
                EXPECT_NE(std::string{e.what()}.find("expects"), std::string::npos)
                    << sink << " <- " << expr << ": " << e.what();
                continue;
            }
            ++accepted[type];
            const auto* write = dynamic_cast<const LogicalSink*>(plan.get());
            ASSERT_NE(write, nullptr) << sink << " <- " << expr;
            const auto* project = dynamic_cast<const LogicalProject*>(&write->input());
            ASSERT_NE(project, nullptr) << sink << " <- " << expr;
            const auto& column_type = *cat.get_table(sink)->columns.at(0).type;
            EXPECT_TRUE(project->outputs().at(0).type->Equals(column_type))
                << sink << " <- " << expr << " is declared "
                << project->outputs().at(0).type->ToString();
            check_values(expr, project->outputs().at(0).expr_json, column_type, findings);
        }
    }
    // Every numeric column takes something; a VARCHAR column takes no number.
    for (const auto& [sink, type] : sink_types()) {
        if (type == "VARCHAR") {
            EXPECT_EQ(accepted[type], 0u);
        } else {
            EXPECT_GT(accepted[type], 0u) << type;
        }
    }
    EXPECT_TRUE(findings.empty()) << findings.size()
                                  << " INSERTs wrote a value of another type than the column's:\n"
                                  << report(findings);
}

TEST(SqlTypeConformance, ADoubleTimesADecimalIsADoubleWhateverItsValue) {
    // The case that found this: DOUBLE PRECISION * 0.029 is declared DOUBLE,
    // and a whole double such as 2.0 made it a DECIMAL.
    const Catalog cat = make_catalog();
    const Binder binder(cat);
    auto script = parse("SELECT d * 0.029 AS fee FROM t");
    auto plan = binder.bind_select(std::get<ast::SelectStmt>(script.statements[0]));
    const auto& out = static_cast<const LogicalProject&>(*plan).outputs().at(0);
    ASSERT_TRUE(out.type->Equals(*arrow::float64()));
    const auto compiled =
        clink::operators::CompiledValueExpr::compile(clink::config::parse(out.expr_json));
    for (const double d : {2.0, 0.0, -3.0, 2.5, 1e15}) {
        auto resolve = [&](const std::string&) { return JsonValue{d}; };
        const clink::operators::ColumnLookup lookup{resolve};
        const JsonValue fee = compiled.evaluate(lookup);
        EXPECT_EQ(kind_of(fee), "double") << d;
        EXPECT_DOUBLE_EQ(fee.as_number(), d * 0.029) << d;
    }
}

TEST(SqlTypeConformance, ADecimalInAFloatColumnIsConvertedNotNulled) {
    // A DECIMAL that reaches a DOUBLE or REAL column on a columnar carrier is
    // converted, as SQL assigns a DECIMAL to a float. It used to be NULL.
    for (const auto& type : {arrow::float64(), arrow::float32()}) {
        auto builder = clink::sql::row_columnar_detail::make_cell_builder(type);
        const JsonValue fee = dec("0.058");
        clink::sql::row_columnar_detail::append_json_cell(*builder, *type, &fee);
        const JsonValue none{nullptr};
        clink::sql::row_columnar_detail::append_json_cell(*builder, *type, &none);
        std::shared_ptr<arrow::Array> out;
        ASSERT_TRUE(builder->Finish(&out).ok());
        ASSERT_FALSE(out->IsNull(0)) << type->ToString();
        EXPECT_TRUE(out->IsNull(1)) << type->ToString();
        const double got =
            type->id() == arrow::Type::DOUBLE
                ? static_cast<const arrow::DoubleArray&>(*out).Value(0)
                : static_cast<double>(static_cast<const arrow::FloatArray&>(*out).Value(0));
        EXPECT_NEAR(got, 0.058, 1e-6) << type->ToString();
    }
}

// ---------------------------------------------------------------------------
// Aggregates.
//
// The binder types an aggregate from its function and its input column's type;
// the value comes from the operator the planner compiles the query to, which
// folds rows one at a time or, given a columnar batch, column at a time. Each
// aggregate of each column type is compiled from SQL and run through the
// compiled operators themselves, from the JSON decoder to the sink boundary, fed
// the JSON a topic carries, in groups of single values, mixed values, NULLs and
// whole doubles. Every value that reaches the sink boundary must be of the type
// the binder declared. Each query runs once with the columnar decoder (the
// vectorised folds) and once without it (the row folds).

void ensure_sql_operators_installed() {
    static const bool once = [] {
        // Built-ins first: install() registers Row-channel operators, and
        // registering one throws unless the Row type is already registered.
        clink::cluster::ensure_built_ins_registered();
        clink::plugin::PluginRegistry reg;
        clink::sql::install(reg);
        return true;
    }();
    (void)once;
}

// The SQL spelling of a declared type, for a sink column of exactly that type,
// so the INSERT adds no cast and the sink receives the aggregate's own value.
std::string sql_type_of(const arrow::DataType& t) {
    switch (t.id()) {
        case arrow::Type::INT64:
            return "BIGINT";
        case arrow::Type::INT32:
            return "INTEGER";
        case arrow::Type::INT16:
            return "SMALLINT";
        case arrow::Type::DOUBLE:
            return "DOUBLE PRECISION";
        case arrow::Type::FLOAT:
            return "REAL";
        case arrow::Type::BOOL:
            return "BOOLEAN";
        case arrow::Type::STRING:
            return "VARCHAR";
        case arrow::Type::DECIMAL128: {
            const auto& d = static_cast<const arrow::Decimal128Type&>(t);
            return "DECIMAL(" + std::to_string(d.precision()) + ", " + std::to_string(d.scale()) +
                   ")";
        }
        case arrow::Type::LIST: {
            const auto inner = sql_type_of(*static_cast<const arrow::ListType&>(t).value_type());
            return inner.empty() ? std::string{} : inner + "[]";
        }
        default:
            return {};
    }
}

// An input column type and the JSON values a topic carries for it, NULL
// included. The DOUBLE column also takes an integer token, which the decoder
// makes a double, and the BIGINT and DECIMAL columns values at their limits, so
// that a sum passes them.
struct AggColumn {
    std::string sql_type;
    std::vector<std::string> json;
};

const std::vector<AggColumn>& agg_columns() {
    static const std::vector<AggColumn> cols = {
        {"BIGINT", {"0", "7", "-3", "9007199254740993", "9223372036854775807", "null"}},
        {"INTEGER", {"0", "7", "-3", "null"}},
        {"SMALLINT", {"2", "-1", "null"}},
        {"DOUBLE PRECISION", {"0.0", "2.0", "2.5", "-0.1", "3", "null"}},
        {"REAL", {"0.5", "3.0", "null"}},
        {"DECIMAL(10, 2)", {"1.50", "3.00", "-0.01", "0.00", "99999999.99", "null"}},
        {"DECIMAL(18, 4)", {"2.0000", "0.1250", "99999999999999.9999", "null"}},
        {"VARCHAR", {"\"x\"", "\"12\"", "null"}},
        {"BOOLEAN", {"true", "false", "null"}},
    };
    return cols;
}

const std::vector<std::string>& aggregate_calls() {
    static const std::vector<std::string> calls = {"COUNT(c)",
                                                   "COUNT(*)",
                                                   "COUNT(DISTINCT c)",
                                                   "SUM(c)",
                                                   "AVG(c)",
                                                   "MIN(c)",
                                                   "MAX(c)",
                                                   "STDDEV(c)",
                                                   "STDDEV_POP(c)",
                                                   "STDDEV_SAMP(c)",
                                                   "VARIANCE(c)",
                                                   "VAR_POP(c)",
                                                   "VAR_SAMP(c)",
                                                   "STRING_AGG(c, ',')",
                                                   "LISTAGG(c, ',')",
                                                   "ARRAY_AGG(c)",
                                                   "COLLECT(c)",
                                                   "PERCENTILE(c, 0.5)",
                                                   "APPROX_PERCENTILE(c, 0.5)"};
    return calls;
}

// Each way of aggregating, with the operator it compiles to: the GROUP BY forms,
// and OVER, which also takes the navigation functions.
struct AggForm {
    std::string group_by;  // empty for OVER
    std::string op_type;
};

const std::vector<AggForm>& aggregation_forms() {
    static const std::vector<AggForm> forms = {
        {"k", "aggregate_row"},
        {"TUMBLE(ts, 1000), k", "tumbling_window_row"},
        {"HOP(ts, 1000, 500), k", "hopping_window_row"},
        {"CUMULATE(ts, 500, 1000), k", "cumulate_window_row"},
        {"SESSION(ts, 1000), k", "session_window_row"},
        {"", "over_aggregate_row"},
    };
    return forms;
}

std::vector<std::string> calls_for(const AggForm& form) {
    std::vector<std::string> calls = aggregate_calls();
    if (form.group_by.empty()) {
        for (const char* nav : {"FIRST_VALUE(c)", "LAST_VALUE(c)", "LAG(c)", "LAG(c, 2)"}) {
            calls.emplace_back(nav);
        }
    }
    return calls;
}

std::string aggregation_select(const AggForm& form, const std::string& call) {
    if (form.group_by.empty()) {
        // An OVER query keeps every input column.
        return "SELECT *, " + call + " OVER (PARTITION BY k ORDER BY ts) AS r FROM t";
    }
    return "SELECT k, " + call + " AS r FROM t GROUP BY " + form.group_by;
}

// The input: a group per value, one with every value, one with every non-NULL
// value in reverse, and one of NULLs only, all inside one window.
std::vector<std::string> aggregate_input(const AggColumn& col) {
    std::vector<std::string> lines;
    std::int64_t ts = 10;
    const auto line = [&](std::int64_t k, const std::string& v) {
        lines.push_back("{\"k\":" + std::to_string(k) + ",\"ts\":" + std::to_string(ts++) +
                        ",\"c\":" + v + "}");
    };
    for (std::size_t i = 0; i < col.json.size(); ++i) {
        line(static_cast<std::int64_t>(i + 1), col.json[i]);
    }
    for (const auto& v : col.json) {
        line(100, v);
    }
    for (auto it = col.json.rbegin(); it != col.json.rend(); ++it) {
        if (*it != "null") {
            line(101, *it);
        }
    }
    line(102, "null");
    line(102, "null");
    return lines;
}

// Run the compiled operators from the JSON decoder to the sink boundary over
// `lines`, and return what reached it.
std::vector<Record<Row>> run_to_sink_boundary(const cluster::JobGraphSpec& spec,
                                              const std::vector<std::string>& lines) {
    std::vector<Record<std::string>> input;
    input.reserve(lines.size());
    for (const auto& l : lines) {
        input.emplace_back(l);
    }
    Dag dag;
    std::any prev =
        dag.add_source<std::string>(std::make_shared<VectorSource<std::string>>(std::move(input)));
    bool reached = false;
    for (const auto& op : spec.ops) {
        if (op.inputs.empty()) {
            continue;  // the topic: the vector above stands in for it
        }
        const auto* builder = cluster::DagBuilderRegistry::default_instance().find(op.type);
        if (builder == nullptr) {
            throw std::runtime_error("no Dag builder for " + op.type);
        }
        clink::plugin::BuildContext ctx;
        ctx.params = op.params;
        ctx.parallelism = 1;
        prev = (*builder)(dag, std::vector<std::any>{std::move(prev)}, ctx).main_handle;
        if (op.type == "row_bind_columns") {
            reached = true;
            break;
        }
    }
    if (!reached) {
        throw std::runtime_error("the plan has no sink boundary (row_bind_columns)");
    }
    auto sink = std::make_shared<CollectingSink<Row>>();
    dag.add_sink<Row>(std::any_cast<StageHandle<Row>>(prev), sink);
    JobConfig cfg;
    cfg.state_backend = std::make_shared<InMemoryStateBackend>();
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();
    return sink->collected_records();
}

// Whether non-NULL `v` is a value of declared type `t`: its kind, a DECIMAL's
// scale and precision (a DECIMAL column holds no more digits than declared),
// and a list's elements.
std::optional<std::string> aggregate_mismatch(const arrow::DataType& t, const JsonValue& v) {
    if (v.is_null()) {
        return std::nullopt;
    }
    if (t.id() == arrow::Type::LIST) {
        if (!v.is_array()) {
            return kind_of(v);
        }
        const auto& elem = *static_cast<const arrow::ListType&>(t).value_type();
        for (const auto& e : v.as_array()) {
            if (auto wrong = aggregate_mismatch(elem, e)) {
                return "an element that is " + *wrong;
            }
        }
        return std::nullopt;
    }
    if (auto wrong = mismatch(t, v)) {
        return wrong;
    }
    if (t.id() == arrow::Type::DECIMAL128) {
        const auto d = clink::config::dec_parse(v.as_string());
        const int precision = static_cast<const arrow::Decimal128Type&>(t).precision();
        if (d && !d->unscaled.FitsInPrecision(precision)) {
            return "a decimal past precision " + std::to_string(precision) + " (" + v.serialize(0) +
                   ")";
        }
    }
    return std::nullopt;
}

// The source table: the group key, the event time and the aggregated column.
std::string aggregate_source_ddl(const std::string& column_type, bool columnar) {
    return "CREATE TABLE t (k BIGINT, ts BIGINT, c " + column_type +
           ") WITH (connector='kafka', topic='t', bootstrap='localhost:9092', format='json', "
           "event_time_column='ts'" +
           (columnar ? "" : ", columnar_decode='false'") + ")";
}

// The plan an INSERT of `select` compiles to, into a table of `sink_columns`.
cluster::JobGraphSpec compile_into_sink(const std::string& source_ddl,
                                        const std::string& select,
                                        const std::string& sink_columns) {
    Catalog cat;
    register_ddl(cat, source_ddl);
    register_ddl(cat,
                 "CREATE TABLE out_t (" + sink_columns +
                     ") WITH (connector='kafka', topic='o', bootstrap='localhost:9092', "
                     "format='json')");
    const Binder binder(cat);
    auto plan = optimize(binder.bind_insert(
        std::get<ast::InsertStmt>(parse("INSERT INTO out_t " + select).statements[0])));
    PhysicalPlanner planner;
    return planner.compile(static_cast<const LogicalSink&>(*plan));
}

// Whether a binder refusal of `call` over a column of `column_type` is one this
// family expects: a numeric aggregate over text or a boolean, or what OVER does
// not take.
bool expected_refusal(const std::string& why, const std::string& column_type) {
    if (why.find("needs a numeric column") != std::string::npos) {
        return column_type == "VARCHAR" || column_type == "BOOLEAN";
    }
    return why.find("DISTINCT is not supported inside an OVER aggregate") != std::string::npos ||
           why.find(" OVER requires a single column argument") != std::string::npos;
}

TEST(SqlTypeConformance, EveryAggregateIsOfItsDeclaredType) {
    ensure_sql_operators_installed();
    std::vector<Finding> findings;
    std::size_t ran = 0;
    std::size_t refused = 0;
    std::size_t values = 0;
    for (const auto& col : agg_columns()) {
        for (const bool columnar : {true, false}) {
            const std::string source_ddl = aggregate_source_ddl(col.sql_type, columnar);
            Catalog cat;
            register_ddl(cat, source_ddl);
            const Binder binder(cat);
            for (const auto& form : aggregation_forms()) {
                for (const auto& call : calls_for(form)) {
                    const std::string select = aggregation_select(form, call);
                    const std::string what = form.op_type + (columnar ? "" : " (row decode)") +
                                             ": " + call + " over " + col.sql_type;
                    std::shared_ptr<arrow::DataType> declared;
                    try {
                        auto plan = binder.bind_select(
                            std::get<ast::SelectStmt>(parse(select).statements[0]));
                        declared = plan->schema()->GetFieldByName("r")->type();
                    } catch (const std::exception& e) {
                        EXPECT_TRUE(expected_refusal(e.what(), col.sql_type))
                            << what << " was refused: " << e.what();
                        ++refused;
                        continue;
                    }
                    const std::string sink_type = sql_type_of(*declared);
                    ASSERT_FALSE(sink_type.empty()) << what << ": " << declared->ToString();
                    // A sink column of exactly the declared type: the INSERT adds
                    // no cast, so the sink boundary sees the aggregate's own value.
                    const std::string sink_columns =
                        (form.group_by.empty() ? "k BIGINT, ts BIGINT, c " + col.sql_type
                                               : std::string{"k BIGINT"}) +
                        ", r " + sink_type;
                    const auto spec = compile_into_sink(source_ddl, select, sink_columns);
                    bool has_op = false;
                    for (const auto& op : spec.ops) {
                        has_op = has_op || op.type == form.op_type;
                    }
                    EXPECT_TRUE(has_op) << what << " did not compile to " << form.op_type;
                    const auto out = run_to_sink_boundary(spec, aggregate_input(col));
                    ++ran;
                    std::set<std::string> seen;
                    for (const auto& rec : out) {
                        const auto r = rec.value().values.find("r");
                        if (r == rec.value().values.end()) {
                            continue;
                        }
                        ++values;
                        if (auto wrong = aggregate_mismatch(*declared, r->second);
                            wrong && seen.insert(*wrong).second) {
                            const auto k = rec.value().values.find("k");
                            findings.push_back(
                                {what,
                                 declared->ToString(),
                                 *wrong,
                                 "group " +
                                     (k == rec.value().values.end() ? std::string{"?"}
                                                                    : k->second.serialize(0)) +
                                     ", value " + r->second.serialize(0)});
                        }
                    }
                }
            }
        }
    }
    EXPECT_GT(ran, 1500u);
    EXPECT_GT(refused, 200u);
    EXPECT_GT(values, 15000u);
    EXPECT_TRUE(findings.empty()) << findings.size() << " of " << ran
                                  << " aggregations gave a value of another type than declared:\n"
                                  << report(findings);
}

// The last value each group's `column` took at the sink boundary, retractions
// skipped.
std::map<std::int64_t, JsonValue> final_per_group(const std::vector<Record<Row>>& out,
                                                  const std::string& column) {
    std::map<std::int64_t, JsonValue> last;
    for (const auto& rec : out) {
        const auto& values = rec.value().values;
        const auto kind = values.find(std::string{kRowKindField});
        if (kind != values.end() && kind->second.is_string() &&
            is_delete_like(kind->second.as_string())) {
            continue;
        }
        const auto k = values.find("k");
        const auto v = values.find(column);
        if (k != values.end() && v != values.end()) {
            last[k->second.as_int()] = v->second;
        }
    }
    return last;
}

TEST(SqlTypeConformance, ASumPastItsInputsRangeIsOfItsDeclaredType) {
    ensure_sql_operators_installed();
    for (const bool columnar : {true, false}) {
        SCOPED_TRACE(columnar ? "columnar decode" : "row decode");
        // A BIGINT total past the int64 range is NULL, as integer overflow is in
        // SQL arithmetic; it was the exact DECIMAL, which a BIGINT column holds
        // as NULL. One that comes back into range is the exact BIGINT.
        const auto bigint = final_per_group(
            run_to_sink_boundary(compile_into_sink(aggregate_source_ddl("BIGINT", columnar),
                                                   "SELECT k, SUM(c) AS r FROM t GROUP BY k",
                                                   "k BIGINT, r BIGINT"),
                                 {R"({"k":1,"ts":1,"c":9223372036854775807})",
                                  R"({"k":1,"ts":2,"c":7})",
                                  R"({"k":2,"ts":3,"c":9223372036854775807})",
                                  R"({"k":2,"ts":4,"c":-7})"}),
            "r");
        ASSERT_EQ(bigint.size(), 2u);
        EXPECT_TRUE(bigint.at(1).is_null()) << bigint.at(1).serialize(0);
        ASSERT_TRUE(bigint.at(2).is_integral_number()) << bigint.at(2).serialize(0);
        EXPECT_EQ(bigint.at(2).as_int(), std::int64_t{9223372036854775800});

        // A DECIMAL total keeps every digit: SUM over DECIMAL(10, 2) is
        // DECIMAL(38, 2), where it was DECIMAL(10, 2) and its column held this
        // total as NULL.
        const auto decimal = final_per_group(
            run_to_sink_boundary(
                compile_into_sink(aggregate_source_ddl("DECIMAL(10, 2)", columnar),
                                  "SELECT k, SUM(c) AS r FROM t GROUP BY k",
                                  "k BIGINT, r DECIMAL(38, 2)"),
                {R"({"k":1,"ts":1,"c":99999999.99})", R"({"k":1,"ts":2,"c":1.50})"}),
            "r");
        ASSERT_EQ(decimal.size(), 1u);
        ASSERT_TRUE(clink::config::is_dec_string(decimal.at(1))) << decimal.at(1).serialize(0);
        const auto total = clink::config::dec_parse(decimal.at(1).as_string());
        ASSERT_TRUE(total.has_value());
        EXPECT_EQ(clink::config::dec_format(*total), "100000001.49");
    }
}

TEST(SqlTypeConformance, ANumericAggregateOverTextIsRefusedWithWhatToDo) {
    // SUM('12') folded nothing and gave 0.0, typed as text.
    Catalog cat;
    register_ddl(cat, aggregate_source_ddl("VARCHAR", true));
    const Binder binder(cat);
    try {
        (void)binder.bind_select(std::get<ast::SelectStmt>(
            parse("SELECT k, SUM(c) AS r FROM t GROUP BY k").statements[0]));
        ADD_FAILURE() << "SUM over a VARCHAR column bound";
    } catch (const std::exception& e) {
        EXPECT_NE(std::string{e.what()}.find("sum(c) needs a numeric column, and c is VARCHAR"),
                  std::string::npos)
            << e.what();
        EXPECT_NE(std::string{e.what()}.find("CAST it to a number"), std::string::npos) << e.what();
    }
}

}  // namespace
}  // namespace clink::sql
