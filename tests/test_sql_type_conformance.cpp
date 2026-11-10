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

#include "clink/config/decimal.hpp"
#include "clink/config/json.hpp"
#include "clink/operators/json_value_expr.hpp"
#include "clink/sql/binder.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/logical_plan.hpp"
#include "clink/sql/parser.hpp"
#include "clink/sql/row_columnar_batcher.hpp"

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
        const double got = type->id() == arrow::Type::DOUBLE
                               ? static_cast<const arrow::DoubleArray&>(*out).Value(0)
                               : static_cast<const arrow::FloatArray&>(*out).Value(0);
        EXPECT_NEAR(got, 0.058, 1e-6) << type->ToString();
    }
}

}  // namespace
}  // namespace clink::sql
