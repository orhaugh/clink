#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "clink/cluster/job_graph.hpp"
#include "clink/sql/ast.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/logical_plan.hpp"

// #59: programmatic Table API - a fluent C++ builder that produces the same
// clink::sql::LogicalPlan the SQL frontend builds, then compiles it through the
// SAME optimiser + physical planner. Expressions/predicates/types are lowered
// via clink::sql::lowering (the binder's own helpers), so the JSON-IR and types
// are byte-identical to the equivalent SQL string - no duplicated type rules.
//
// v1 scope: from -> [filter] -> [select | group_by().agg()] -> insert_into.
// Deferred: joins, windows, OVER, set-ops, subqueries, DISTINCT, ORDER/LIMIT,
// HAVING, DDL. Each maps 1:1 to a single-input Logical node the planner already
// compiles.
//
// Expr/Predicate wrap an ast::Expression and are MOVE-ONLY (the AST holds
// unique_ptr nodes), so the builders consume their operands - build each
// expression once, inline, in the fluent chain.

namespace clink::api {

class Expr {
public:
    explicit Expr(sql::ast::Expression e) : e_(std::move(e)) {}
    [[nodiscard]] const sql::ast::Expression& ast() const& noexcept { return e_; }
    sql::ast::Expression release() && { return std::move(e_); }

private:
    sql::ast::Expression e_;
};

class Predicate {
public:
    explicit Predicate(sql::ast::Expression e) : e_(std::move(e)) {}
    [[nodiscard]] const sql::ast::Expression& ast() const& noexcept { return e_; }
    sql::ast::Expression release() && { return std::move(e_); }

private:
    sql::ast::Expression e_;
};

// --- value builders ---------------------------------------------------------
Expr col(std::string name);
Expr lit(std::int64_t v);
Expr lit(int v);
Expr lit(const char* v);
Expr lit(std::string v);
Expr lit(bool v);
Expr null_();
Expr dec(std::string decimal_text);  // exact DECIMAL literal (#56), e.g. dec("1.50")
Expr cast(Expr value, std::string sql_type, std::vector<int> typmods = {});
Expr call(std::string fn, std::vector<Expr> args);

Expr operator+(Expr a, Expr b);
Expr operator-(Expr a, Expr b);
Expr operator*(Expr a, Expr b);
Expr operator/(Expr a, Expr b);
Expr operator%(Expr a, Expr b);

// --- predicate builders -----------------------------------------------------
Predicate operator==(Expr a, Expr b);
Predicate operator!=(Expr a, Expr b);
Predicate operator<(Expr a, Expr b);
Predicate operator<=(Expr a, Expr b);
Predicate operator>(Expr a, Expr b);
Predicate operator>=(Expr a, Expr b);
Predicate operator&&(Predicate a, Predicate b);
Predicate operator||(Predicate a, Predicate b);
Predicate operator!(Predicate p);

// A named output expression for select(): (col("v") + lit(1)) >> "vp".
struct Item {
    std::string name;
    Expr expr;
};
Item operator>>(Expr e, std::string name);

// --- aggregates (for group_by().agg()) --------------------------------------

// An aggregate over a source column: sum("v"), count_star(), min_("x").
// Plain value type (no AST node) so it is copyable and can sit in an
// initializer_list. Build via the free functions below; alias with >>.
class Agg {
public:
    Agg() = default;
    Agg(std::string fn, std::string input_column, bool distinct = false, std::string separator = {})
        : fn_(std::move(fn)),
          input_column_(std::move(input_column)),
          distinct_(distinct),
          separator_(std::move(separator)) {}

    [[nodiscard]] const std::string& fn() const noexcept { return fn_; }
    [[nodiscard]] const std::string& input_column() const noexcept { return input_column_; }
    [[nodiscard]] bool is_distinct() const noexcept { return distinct_; }
    [[nodiscard]] const std::string& separator() const noexcept { return separator_; }

private:
    std::string fn_;
    std::string input_column_;
    bool distinct_ = false;
    std::string separator_;
};

// A group-key passthrough in agg(): key("k") emits the key column. Alias with
// >> to rename the output ("k" by default).
struct GroupKey {
    std::string column;
};
GroupKey key(std::string column);

Agg sum(std::string column);                                      // SUM(column)
Agg count_star();                                                 // COUNT(*)
Agg count(std::string column);                                    // COUNT(column)
Agg count_distinct(std::string column);                           // COUNT(DISTINCT column)
Agg min_(std::string column);                                     // MIN(column)
Agg max_(std::string column);                                     // MAX(column)
Agg avg(std::string column);                                      // AVG(column)
Agg string_agg(std::string column, std::string separator = ",");  // STRING_AGG / LISTAGG

// One item in agg(): either a key passthrough or an aggregate, with an
// optional output name. key()/Agg convert implicitly (default output name);
// use >> "name" to set the output column name.
struct AggItem {
    bool is_key = false;
    std::string key_column;   // when is_key
    std::string output_name;  // explicit alias; empty -> default
    Agg aggregate;            // when !is_key

    AggItem() = default;
    AggItem(GroupKey k) : is_key(true), key_column(std::move(k.column)) {}  // NOLINT(*-explicit-*)
    AggItem(Agg a) : aggregate(std::move(a)) {}                             // NOLINT(*-explicit-*)
};
AggItem operator>>(GroupKey k, std::string name);
AggItem operator>>(Agg a, std::string name);

class GroupedTable;

class Table {
public:
    // Filter rows by a predicate (-> LogicalFilter). Schema unchanged.
    Table filter(Predicate p) &&;

    // Project to named expressions (-> LogicalProject). Variadic so move-only
    // Items need no copying initializer_list.
    template <class... Items>
    Table select(Items&&... items) && {
        std::vector<Item> v;
        v.reserve(sizeof...(items));
        (v.emplace_back(std::forward<Items>(items)), ...);
        return std::move(*this).select_items(std::move(v));
    }
    Table select_items(std::vector<Item> items) &&;

    // Group by the given key columns; follow with .agg(...) to aggregate
    // (-> LogicalAggregate). Keys must be columns of the current schema.
    [[nodiscard]] GroupedTable group_by(std::vector<std::string> keys) &&;

    // Compile: build the LogicalSink for `sink_table`, validate, optimise, and
    // produce a JobGraphSpec the coordinator can submit (spec.to_json()).
    [[nodiscard]] cluster::JobGraphSpec insert_into(const std::string& sink_table) &&;

    // Test/EXPLAIN accessor for the current logical plan root.
    [[nodiscard]] const sql::LogicalPlan& plan() const noexcept { return *plan_; }

private:
    friend class TableEnvironment;
    friend class GroupedTable;
    Table(std::unique_ptr<sql::LogicalPlan> plan, sql::TableDef cur, const sql::Catalog* cat)
        : plan_(std::move(plan)), cur_(std::move(cur)), catalog_(cat) {}

    std::unique_ptr<sql::LogicalPlan> plan_;
    sql::TableDef cur_;  // synthetic TableDef mirroring the current output schema
    const sql::Catalog* catalog_;
};

// Intermediate state between group_by(...) and agg(...). Holds the grouped
// keys; agg() builds the LogicalAggregate and returns to a Table.
class GroupedTable {
public:
    // Aggregate. `items` mixes key("k") passthroughs and aggregates
    // (sum("v") >> "total"); the output column order is the item order. At
    // least one aggregate is required. Keys referenced via key() must be in
    // the group_by() key set.
    [[nodiscard]] Table agg(std::vector<AggItem> items) &&;

    [[nodiscard]] const std::vector<std::string>& keys() const noexcept { return keys_; }

private:
    friend class Table;
    GroupedTable(std::unique_ptr<sql::LogicalPlan> plan,
                 sql::TableDef cur,
                 std::vector<std::string> keys,
                 const sql::Catalog* cat)
        : plan_(std::move(plan)), cur_(std::move(cur)), keys_(std::move(keys)), catalog_(cat) {}

    std::unique_ptr<sql::LogicalPlan> plan_;
    sql::TableDef cur_;  // input schema (key types resolve against this)
    std::vector<std::string> keys_;
    const sql::Catalog* catalog_;
};

class TableEnvironment {
public:
    explicit TableEnvironment(const sql::Catalog& cat) : catalog_(&cat) {}

    // Begin a query from a registered (scannable, Row-channel) source table.
    [[nodiscard]] Table from(const std::string& table_name) const;

private:
    const sql::Catalog* catalog_;
};

}  // namespace clink::api
