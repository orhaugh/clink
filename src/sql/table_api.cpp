#include "clink/sql/table_api.hpp"

#include <algorithm>
#include <cctype>
#include <stdexcept>

#include "clink/sql/expr_lowering.hpp"
#include "clink/sql/optimizer.hpp"
#include "clink/sql/parser.hpp"  // TranslationError
#include "clink/sql/physical_plan.hpp"

namespace clink::api {

namespace {

namespace ast = clink::sql::ast;

// Wrap an ast node (value-type or unique_ptr alternative) as an Expr.
template <typename Node>
Expr wrap(Node node) {
    return Expr{ast::Expression{std::move(node)}};
}

Expr make_arith(ast::ArithKind op, Expr a, Expr b) {
    auto node = std::make_unique<ast::ArithOp>();
    node->op = op;
    node->args.push_back(std::move(a).release());
    node->args.push_back(std::move(b).release());
    return wrap(std::move(node));
}

Predicate make_cmp(ast::BinOp op, Expr a, Expr b) {
    auto node = std::make_unique<ast::BinaryOp>();
    node->op = op;
    node->left = std::move(a).release();
    node->right = std::move(b).release();
    return Predicate{ast::Expression{std::move(node)}};
}

Predicate make_logical(ast::LogicalKind kind, Predicate a, Predicate b) {
    auto node = std::make_unique<ast::LogicalOp>();
    node->op = kind;
    node->args.push_back(std::move(a).release());
    node->args.push_back(std::move(b).release());
    return Predicate{ast::Expression{std::move(node)}};
}

// Map a user SQL type name to the binder's canonical cast target_type.
std::string cast_target(std::string sql_type) {
    std::transform(sql_type.begin(), sql_type.end(), sql_type.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    if (sql_type == "int" || sql_type == "integer" || sql_type == "bigint" || sql_type == "int8" ||
        sql_type == "int4" || sql_type == "int2" || sql_type == "smallint") {
        return "int";
    }
    if (sql_type == "double" || sql_type == "float" || sql_type == "float8" ||
        sql_type == "float4" || sql_type == "real") {
        return "float";
    }
    if (sql_type == "numeric" || sql_type == "decimal") {
        return "decimal";
    }
    if (sql_type == "text" || sql_type == "varchar" || sql_type == "string" || sql_type == "char" ||
        sql_type == "bpchar") {
        return "str";
    }
    if (sql_type == "bool" || sql_type == "boolean") {
        return "bool";
    }
    throw clink::sql::TranslationError("CAST: unsupported target type '" + sql_type + "'", 0);
}

// Synthetic TableDef mirroring a plan node's output schema, for column
// resolution at the next fluent step.
clink::sql::TableDef table_def_from_schema(const std::shared_ptr<arrow::Schema>& schema) {
    clink::sql::TableDef td;
    td.name = "__table_api_derived";
    if (schema) {
        for (int i = 0; i < schema->num_fields(); ++i) {
            td.columns.push_back(
                clink::sql::ColumnSpec{schema->field(i)->name(), schema->field(i)->type()});
        }
    }
    return td;
}

}  // namespace

// --- value builders ---------------------------------------------------------

Expr col(std::string name) {
    ast::ColumnRef cr;
    cr.parts = {std::move(name)};
    cr.is_star = false;
    return wrap(std::move(cr));
}

Expr lit(std::int64_t v) {
    ast::IntLiteral lit;
    lit.value = v;
    return wrap(std::move(lit));
}
Expr lit(int v) {
    return lit(static_cast<std::int64_t>(v));
}
Expr lit(std::string v) {
    ast::StringLiteral s;
    s.value = std::move(v);
    return wrap(std::move(s));
}
Expr lit(const char* v) {
    return lit(std::string{v});
}
Expr lit(bool v) {
    ast::BoolLiteral b;
    b.value = v;
    return wrap(std::move(b));
}
Expr null_() {
    return wrap(ast::NullLiteral{});
}

Expr dec(std::string decimal_text) {
    ast::FloatLiteral f;
    // value is the lossy double for legacy callers; text drives the exact #56
    // decimal lowering in lower_value_expr.
    try {
        f.value = std::stod(decimal_text);
    } catch (...) {
        throw clink::sql::TranslationError("dec(): not a numeric literal: '" + decimal_text + "'",
                                           0);
    }
    f.text = std::move(decimal_text);
    return wrap(std::move(f));
}

Expr cast(Expr value, std::string sql_type, std::vector<int> typmods) {
    auto node = std::make_unique<ast::CastOp>();
    node->arg = std::move(value).release();
    node->target_type = cast_target(std::move(sql_type));
    node->typmods = std::move(typmods);
    return wrap(std::move(node));
}

Expr call(std::string fn, std::vector<Expr> args) {
    auto node = std::make_unique<ast::FunctionCall>();
    node->name = std::move(fn);
    for (auto& a : args) {
        node->args.push_back(std::move(a).release());
    }
    return wrap(std::move(node));
}

Expr operator+(Expr a, Expr b) {
    return make_arith(ast::ArithKind::Plus, std::move(a), std::move(b));
}
Expr operator-(Expr a, Expr b) {
    return make_arith(ast::ArithKind::Minus, std::move(a), std::move(b));
}
Expr operator*(Expr a, Expr b) {
    return make_arith(ast::ArithKind::Mul, std::move(a), std::move(b));
}
Expr operator/(Expr a, Expr b) {
    return make_arith(ast::ArithKind::Div, std::move(a), std::move(b));
}
Expr operator%(Expr a, Expr b) {
    return make_arith(ast::ArithKind::Mod, std::move(a), std::move(b));
}

// --- predicate builders -----------------------------------------------------

Predicate operator==(Expr a, Expr b) {
    return make_cmp(ast::BinOp::Eq, std::move(a), std::move(b));
}
Predicate operator!=(Expr a, Expr b) {
    return make_cmp(ast::BinOp::Ne, std::move(a), std::move(b));
}
Predicate operator<(Expr a, Expr b) {
    return make_cmp(ast::BinOp::Lt, std::move(a), std::move(b));
}
Predicate operator<=(Expr a, Expr b) {
    return make_cmp(ast::BinOp::Le, std::move(a), std::move(b));
}
Predicate operator>(Expr a, Expr b) {
    return make_cmp(ast::BinOp::Gt, std::move(a), std::move(b));
}
Predicate operator>=(Expr a, Expr b) {
    return make_cmp(ast::BinOp::Ge, std::move(a), std::move(b));
}
Predicate operator&&(Predicate a, Predicate b) {
    return make_logical(ast::LogicalKind::And, std::move(a), std::move(b));
}
Predicate operator||(Predicate a, Predicate b) {
    return make_logical(ast::LogicalKind::Or, std::move(a), std::move(b));
}
Predicate operator!(Predicate p) {
    auto node = std::make_unique<ast::NotOp>();
    node->arg = std::move(p).release();
    return Predicate{ast::Expression{std::move(node)}};
}

Item operator>>(Expr e, std::string name) {
    return Item{std::move(name), std::move(e)};
}

// --- aggregate builders -----------------------------------------------------

GroupKey key(std::string column) {
    return GroupKey{std::move(column)};
}

Agg sum(std::string column) {
    return Agg{"sum", std::move(column)};
}
Agg count_star() {
    return Agg{"count", ""};
}
Agg count(std::string column) {
    return Agg{"count", std::move(column)};
}
Agg count_distinct(std::string column) {
    return Agg{"count", std::move(column), /*distinct=*/true};
}
Agg min_(std::string column) {
    return Agg{"min", std::move(column)};
}
Agg max_(std::string column) {
    return Agg{"max", std::move(column)};
}
Agg avg(std::string column) {
    return Agg{"avg", std::move(column)};
}
Agg string_agg(std::string column, std::string separator) {
    return Agg{"string_agg", std::move(column), /*distinct=*/false, std::move(separator)};
}

AggItem operator>>(GroupKey k, std::string name) {
    AggItem it{std::move(k)};
    it.output_name = std::move(name);
    return it;
}
AggItem operator>>(Agg a, std::string name) {
    AggItem it{std::move(a)};
    it.output_name = std::move(name);
    return it;
}

// --- Table -----------------------------------------------------------------

Table Table::filter(Predicate p) && {
    auto pj = clink::sql::lowering::predicate(p.ast(), cur_).serialize(0);
    auto next = std::make_unique<clink::sql::LogicalFilter>(std::move(plan_), std::move(pj));
    return Table{std::move(next), std::move(cur_), catalog_};
}

Table Table::select_items(std::vector<Item> items) && {
    std::vector<ast::SelectItem> sel;
    sel.reserve(items.size());
    for (auto& it : items) {
        ast::SelectItem s;
        s.expr = std::move(it.expr).release();
        s.alias = std::move(it.name);
        sel.push_back(std::move(s));
    }
    auto outs = clink::sql::lowering::select_items(sel, cur_, "");
    auto proj = std::make_unique<clink::sql::LogicalProject>(std::move(plan_), std::move(outs));
    auto new_cur = table_def_from_schema(proj->schema());
    return Table{std::move(proj), std::move(new_cur), catalog_};
}

cluster::JobGraphSpec Table::insert_into(const std::string& sink_table) && {
    const clink::sql::TableDef* sink = catalog_->get_table(sink_table);
    if (sink == nullptr) {
        throw clink::sql::TranslationError("INSERT INTO unknown table: " + sink_table, 0);
    }
    // Validate the projected schema against the sink (same check + message as
    // the SQL frontend), then build the sink node, optimise, and lower.
    clink::sql::lowering::check_sink(*sink, *plan_->schema(), 0);
    auto sink_node = std::make_unique<clink::sql::LogicalSink>(std::move(plan_), sink);
    auto optimized = clink::sql::optimize(std::move(sink_node));
    clink::sql::PhysicalPlanner planner;
    return planner.compile(static_cast<const clink::sql::LogicalSink&>(*optimized));
}

GroupedTable Table::group_by(std::vector<std::string> keys) && {
    // Keys must be columns of the current schema (mirrors the binder's
    // resolve_value_column_name existence check on GROUP BY entries).
    for (const auto& k : keys) {
        bool found = false;
        for (const auto& c : cur_.columns) {
            if (c.name == k) {
                found = true;
                break;
            }
        }
        if (!found) {
            throw clink::sql::TranslationError("GROUP BY unknown column: " + k, 0);
        }
    }
    return GroupedTable{std::move(plan_), std::move(cur_), std::move(keys), catalog_};
}

Table GroupedTable::agg(std::vector<AggItem> items) && {
    std::vector<clink::sql::AggregateOutput> aggregates;
    std::vector<clink::sql::lowering::GroupOutputColumn> columns;
    columns.reserve(items.size());
    for (auto& it : items) {
        if (it.is_key) {
            // Key passthrough: must be a declared group key (same rule + message
            // as the SQL binder).
            bool in_group = false;
            for (const auto& k : keys_) {
                if (k == it.key_column) {
                    in_group = true;
                    break;
                }
            }
            if (!in_group) {
                throw clink::sql::TranslationError(
                    "column " + it.key_column + " referenced in SELECT must appear in GROUP BY", 0);
            }
            clink::sql::lowering::GroupOutputColumn gc;
            gc.is_aggregate = false;
            gc.key_source_column = it.key_column;
            gc.key_output_name = it.output_name.empty() ? it.key_column : it.output_name;
            columns.push_back(std::move(gc));
        } else {
            const Agg& a = it.aggregate;
            // DISTINCT is only meaningful for COUNT / STRING_AGG / ARRAY_AGG
            // (mirrors the binder). The count_distinct() builder is the only
            // one that sets it, but the public Agg constructor admits any
            // combination, so reject the invalid ones here too.
            if (a.is_distinct() && a.fn() != "count" && a.fn() != "string_agg" &&
                a.fn() != "array_agg") {
                throw clink::sql::TranslationError(
                    "DISTINCT is only supported for COUNT, STRING_AGG and ARRAY_AGG", 0);
            }
            // An aggregate over a named column must reference a real input
            // column. The SQL binder enforces this via resolve_value_column_name
            // before typing; aggregate_type() alone defaults silently, so check
            // here to keep the Table API from being more permissive than SQL.
            // COUNT(*) carries an empty input column and is exempt.
            if (!a.input_column().empty()) {
                bool exists = false;
                for (const auto& c : cur_.columns) {
                    if (c.name == a.input_column()) {
                        exists = true;
                        break;
                    }
                }
                if (!exists) {
                    throw clink::sql::TranslationError(
                        "column not found in " + cur_.name + ": " + a.input_column(), 0);
                }
            }
            clink::sql::AggregateOutput ao;
            ao.agg_fn = a.fn();
            ao.input_column = a.input_column();
            ao.distinct = a.is_distinct();
            ao.separator = a.separator();
            ao.percentile = 0.0;
            // Default output name matches the binder: fn + "_" + col, or
            // fn + "_star" for COUNT(*).
            ao.output_name = it.output_name.empty()
                                 ? a.fn() + (a.input_column().empty() ? std::string{"_star"}
                                                                      : "_" + a.input_column())
                                 : it.output_name;
            ao.type = clink::sql::lowering::aggregate_type(a.fn(), cur_, a.input_column());
            clink::sql::lowering::GroupOutputColumn gc;
            gc.is_aggregate = true;
            gc.agg_index = aggregates.size();
            aggregates.push_back(std::move(ao));
            columns.push_back(std::move(gc));
        }
    }
    if (aggregates.empty()) {
        throw clink::sql::TranslationError(
            "GROUP BY without aggregate functions in SELECT is not yet supported", 0);
    }
    auto out_schema = clink::sql::lowering::build_group_output_schema(columns, aggregates, cur_);
    // Output name per group key (parallel to keys_), honouring a key alias -
    // mirrors the SQL binder so the Table-API spec stays byte-identical to SQL.
    std::vector<std::string> key_output_names;
    key_output_names.reserve(keys_.size());
    for (const auto& gk : keys_) {
        std::string out_name = gk;
        for (const auto& c : columns) {
            if (!c.is_aggregate && c.key_source_column == gk) {
                out_name = c.key_output_name;
                break;
            }
        }
        key_output_names.push_back(std::move(out_name));
    }
    auto agg_plan = std::make_unique<clink::sql::LogicalAggregate>(
        std::move(plan_), keys_, std::move(aggregates), out_schema, std::move(key_output_names));
    auto new_cur = table_def_from_schema(out_schema);
    return Table{std::move(agg_plan), std::move(new_cur), catalog_};
}

// --- TableEnvironment -------------------------------------------------------

Table TableEnvironment::from(const std::string& table_name) const {
    const clink::sql::TableDef* td = catalog_->get_table(table_name);
    if (td == nullptr) {
        throw clink::sql::TranslationError("FROM unknown table: " + table_name, 0);
    }
    if (td->is_lookup()) {
        throw clink::sql::TranslationError(
            "FROM cannot scan a lookup table (use it as a JOIN dimension): " + table_name, 0);
    }
    auto scan = std::make_unique<clink::sql::LogicalScan>(td);
    return Table{std::move(scan), *td, catalog_};
}

}  // namespace clink::api
