#include <gtest/gtest.h>

#include "clink/operators/agg_function_registry.hpp"
#include "clink/sql/binder.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/parser.hpp"
#include "clink/sql/view.hpp"

#include "arrow/api.h"

namespace clink::sql {

namespace {

void register_clicks(Catalog& cat) {
    auto s = parse(
        "CREATE TABLE clicks ("
        "  user_id BIGINT,"
        "  ts TIMESTAMP(3),"
        "  url TEXT"
        ") WITH (connector='kafka', topic='clicks', bootstrap='localhost:9092')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
}

void register_sink(Catalog& cat, const char* name, const char* col_list) {
    std::string sql = std::string("CREATE TABLE ") + name + " (" + col_list +
                      ") WITH (connector='filesystem', path='/tmp/" + name + ".parquet')";
    auto s = parse(sql);
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
}

// A Row source declaring an event-time column, for OVER aggregates and
// other event-time features.
void register_events(Catalog& cat) {
    auto s = parse(
        "CREATE TABLE evt (user_id BIGINT, ts BIGINT, amount BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/evt.ndjson', event_time_column='ts')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
}

const ast::SelectStmt& as_select(const ast::Script& s) {
    return std::get<ast::SelectStmt>(s.statements[0]);
}

const ast::InsertStmt& as_insert(const ast::Script& s) {
    return std::get<ast::InsertStmt>(s.statements[0]);
}

// A top-level join now binds as a synthetic derived table under an outer
// Project (so the SELECT projection / WHERE / GROUP BY apply over the join
// output). Descend past the wrapper nodes to the join itself.
const LogicalPlan* join_node(const LogicalPlan* p) {
    while (p != nullptr) {
        const auto k = p->kind();
        if (k == "EquiJoin" || k == "IntervalJoin" || k == "LookupJoin") {
            return p;
        }
        auto ins = p->inputs();
        if (ins.empty()) {
            return nullptr;
        }
        p = ins[0];
    }
    return nullptr;
}

}  // namespace

// Helpers to read ProjectOutput names + expressions without
// re-parsing JSON in every test.
namespace {

std::vector<std::string> output_names(const LogicalProject& p) {
    std::vector<std::string> out;
    for (const auto& o : p.outputs())
        out.push_back(o.name);
    return out;
}

std::vector<std::string> output_exprs(const LogicalProject& p) {
    std::vector<std::string> out;
    for (const auto& o : p.outputs())
        out.push_back(o.expr_json);
    return out;
}

}  // namespace

TEST(SqlBinder, BindsSelectColumnList) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(as_select(parse("SELECT user_id, url FROM clicks")));
    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->kind(), "Project");

    const auto& project = static_cast<const LogicalProject&>(*plan);
    EXPECT_EQ(project.input().kind(), "Scan");
    EXPECT_EQ(output_names(project), std::vector<std::string>({"user_id", "url"}));
    auto exprs = output_exprs(project);
    EXPECT_NE(exprs[0].find("\"col\":\"user_id\""), std::string::npos);
    EXPECT_NE(exprs[1].find("\"col\":\"url\""), std::string::npos);

    auto schema = plan->schema();
    ASSERT_EQ(schema->num_fields(), 2);
    EXPECT_EQ(schema->field(0)->name(), "user_id");
    EXPECT_TRUE(schema->field(0)->type()->Equals(*arrow::int64()));
    EXPECT_EQ(schema->field(1)->name(), "url");
    EXPECT_TRUE(schema->field(1)->type()->Equals(*arrow::utf8()));
}

TEST(SqlBinder, SelectStarExpandsToAllColumns) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(as_select(parse("SELECT * FROM clicks")));
    const auto& project = static_cast<const LogicalProject&>(*plan);
    EXPECT_EQ(output_names(project), std::vector<std::string>({"user_id", "ts", "url"}));
}

TEST(SqlBinder, AppliesSelectItemAlias) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(as_select(parse("SELECT user_id AS uid FROM clicks")));
    const auto& project = static_cast<const LogicalProject&>(*plan);
    EXPECT_EQ(output_names(project), std::vector<std::string>{"uid"});
}

TEST(SqlBinder, QualifiedColumnReferenceResolvesAgainstSource) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(as_select(parse("SELECT clicks.user_id FROM clicks")));
    const auto& project = static_cast<const LogicalProject&>(*plan);
    EXPECT_EQ(output_names(project), std::vector<std::string>{"user_id"});
    EXPECT_NE(output_exprs(project)[0].find("\"col\":\"user_id\""), std::string::npos);
}

TEST(SqlBinder, AliasedFromAllowsAliasQualifier) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(as_select(parse("SELECT c.url FROM clicks c")));
    const auto& project = static_cast<const LogicalProject&>(*plan);
    EXPECT_NE(output_exprs(project)[0].find("\"col\":\"url\""), std::string::npos);
}

TEST(SqlBinder, ArithmeticExpressionInSelect) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(as_select(parse("SELECT user_id + 1 AS bump FROM clicks")));
    const auto& project = static_cast<const LogicalProject&>(*plan);
    ASSERT_EQ(project.outputs().size(), 1u);
    EXPECT_EQ(project.outputs()[0].name, "bump");
    EXPECT_NE(project.outputs()[0].expr_json.find("\"op\":\"add\""), std::string::npos);
    EXPECT_NE(project.outputs()[0].expr_json.find("\"col\":\"user_id\""), std::string::npos);
    EXPECT_NE(project.outputs()[0].expr_json.find("\"lit\":1"), std::string::npos);
    EXPECT_TRUE(project.outputs()[0].type->Equals(*arrow::float64()));
}

TEST(SqlBinder, FunctionCallAndConcatInSelect) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan =
        b.bind_select(as_select(parse("SELECT upper(url) AS u, url || '!' AS exc FROM clicks")));
    const auto& project = static_cast<const LogicalProject&>(*plan);
    ASSERT_EQ(project.outputs().size(), 2u);
    EXPECT_NE(project.outputs()[0].expr_json.find("\"op\":\"upper\""), std::string::npos);
    EXPECT_NE(project.outputs()[1].expr_json.find("\"op\":\"concat\""), std::string::npos);
    EXPECT_TRUE(project.outputs()[0].type->Equals(*arrow::utf8()));
}

TEST(SqlBinder, ArrayLiteralLowersToMakeArrayWithListType) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(as_select(parse("SELECT ARRAY[user_id, 7] AS a FROM clicks")));
    const auto& project = static_cast<const LogicalProject&>(*plan);
    ASSERT_EQ(project.outputs().size(), 1u);
    EXPECT_EQ(project.outputs()[0].name, "a");
    EXPECT_NE(project.outputs()[0].expr_json.find("\"op\":\"make_array\""), std::string::npos);
    // Element type follows the first element (user_id BIGINT -> int64).
    EXPECT_TRUE(project.outputs()[0].type->Equals(*arrow::list(arrow::int64())));
}

TEST(SqlBinder, EmptyArrayLiteralIsListOfNull) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(as_select(parse("SELECT ARRAY[] AS a FROM clicks")));
    const auto& project = static_cast<const LogicalProject&>(*plan);
    EXPECT_TRUE(project.outputs()[0].type->Equals(*arrow::list(arrow::null())));
}

TEST(SqlBinder, ArraySubscriptLowersToElementAtWithElementType) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan =
        b.bind_select(as_select(parse("SELECT (ARRAY[10, 20, 30])[2] AS second FROM clicks")));
    const auto& project = static_cast<const LogicalProject&>(*plan);
    ASSERT_EQ(project.outputs().size(), 1u);
    EXPECT_EQ(project.outputs()[0].name, "second");
    EXPECT_NE(project.outputs()[0].expr_json.find("\"op\":\"element_at\""), std::string::npos);
    // Indexing an int64 list yields an int64 element.
    EXPECT_TRUE(project.outputs()[0].type->Equals(*arrow::int64()));
}

TEST(SqlBinder, ArrayColumnTypeBindsToListSchema) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE t (id BIGINT, tags INT ARRAY) "
        "WITH (connector='file', format='json', path='/tmp/t.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    Binder b(cat);
    auto plan = b.bind_select(as_select(parse("SELECT tags[1] AS first_tag FROM t")));
    const auto& project = static_cast<const LogicalProject&>(*plan);
    EXPECT_NE(project.outputs()[0].expr_json.find("\"op\":\"element_at\""), std::string::npos);
    // tags is INT ARRAY (int32 list) -> element type int32.
    EXPECT_TRUE(project.outputs()[0].type->Equals(*arrow::int32()));
}

TEST(SqlBinder, ArraySubscriptOnNonArrayFallsBackToUtf8) {
    Catalog cat;
    register_clicks(cat);  // url TEXT
    Binder b(cat);
    // Subscripting a non-array (text) column is allowed by the grammar; the
    // element type falls back to utf8 (the runtime independently yields NULL).
    auto plan = b.bind_select(as_select(parse("SELECT (url)[1] AS c FROM clicks")));
    const auto& project = static_cast<const LogicalProject&>(*plan);
    EXPECT_NE(project.outputs()[0].expr_json.find("\"op\":\"element_at\""), std::string::npos);
    EXPECT_TRUE(project.outputs()[0].type->Equals(*arrow::utf8()));
}

TEST(SqlBinder, NestedArrayColumnPeelsOneLevelPerSubscript) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE g (grid INT[][]) "
        "WITH (connector='file', format='json', path='/tmp/g.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    Binder b(cat);
    // grid : list<list<int32>>; grid[1] peels one level; grid[1][2] peels two.
    auto plan =
        b.bind_select(as_select(parse("SELECT grid[1] AS row1, grid[1][2] AS cell FROM g")));
    const auto& project = static_cast<const LogicalProject&>(*plan);
    ASSERT_EQ(project.outputs().size(), 2u);
    EXPECT_TRUE(project.outputs()[0].type->Equals(*arrow::list(arrow::int32())));
    EXPECT_TRUE(project.outputs()[1].type->Equals(*arrow::int32()));
}

TEST(SqlBinder, RowConstructorLowersToMakeRowWithStructType) {
    Catalog cat;
    register_clicks(cat);  // user_id BIGINT, url TEXT
    Binder b(cat);
    auto plan = b.bind_select(as_select(parse("SELECT ROW(user_id, 7) AS r FROM clicks")));
    const auto& project = static_cast<const LogicalProject&>(*plan);
    ASSERT_EQ(project.outputs().size(), 1u);
    EXPECT_NE(project.outputs()[0].expr_json.find("\"op\":\"make_row\""), std::string::npos);
    // Field names: a column ref keeps its name; a bare literal gets fN.
    EXPECT_NE(project.outputs()[0].expr_json.find("user_id"), std::string::npos);
    EXPECT_NE(project.outputs()[0].expr_json.find("f2"), std::string::npos);
    // Type is a struct of the field types (user_id BIGINT, literal int64).
    auto expected = arrow::struct_(
        {arrow::field("user_id", arrow::int64()), arrow::field("f2", arrow::int64())});
    EXPECT_TRUE(project.outputs()[0].type->Equals(*expected));
}

TEST(SqlBinder, FieldAccessOnRowConstructorInfersFieldType) {
    Catalog cat;
    register_clicks(cat);  // user_id BIGINT, url TEXT
    Binder b(cat);
    // (ROW(user_id, url)).user_id resolves the named field's type (int64).
    auto plan =
        b.bind_select(as_select(parse("SELECT (ROW(user_id, url)).user_id AS c FROM clicks")));
    const auto& project = static_cast<const LogicalProject&>(*plan);
    EXPECT_NE(project.outputs()[0].expr_json.find("\"op\":\"field_at\""), std::string::npos);
    EXPECT_NE(project.outputs()[0].expr_json.find("\"field\":\"user_id\""), std::string::npos);
    EXPECT_TRUE(project.outputs()[0].type->Equals(*arrow::int64()));
}

TEST(SqlBinder, FieldAccessOnNonRowFallsBackToUtf8) {
    Catalog cat;
    register_clicks(cat);  // url TEXT
    Binder b(cat);
    // Field access on a non-struct base is allowed by the grammar; the type
    // falls back to utf8 (the runtime independently yields NULL).
    auto plan = b.bind_select(as_select(parse("SELECT (url).missing AS c FROM clicks")));
    const auto& project = static_cast<const LogicalProject&>(*plan);
    EXPECT_NE(project.outputs()[0].expr_json.find("\"op\":\"field_at\""), std::string::npos);
    EXPECT_TRUE(project.outputs()[0].type->Equals(*arrow::utf8()));
}

TEST(SqlBinder, MapConstructorInfersMapType) {
    Catalog cat;
    register_clicks(cat);  // user_id BIGINT
    Binder b(cat);
    auto plan = b.bind_select(as_select(parse("SELECT MAP('k', user_id) AS m FROM clicks")));
    const auto& project = static_cast<const LogicalProject&>(*plan);
    EXPECT_NE(project.outputs()[0].expr_json.find("\"op\":\"map\""), std::string::npos);
    // Key from a string literal (utf8), value from user_id (int64).
    EXPECT_TRUE(project.outputs()[0].type->Equals(*arrow::map(arrow::utf8(), arrow::int64())));
}

TEST(SqlBinder, MapSubscriptDispatchesToMapGetAndPeelsValueType) {
    Catalog cat;
    register_clicks(cat);  // user_id BIGINT
    Binder b(cat);
    // The base is statically a MAP, so the subscript lowers to map_get (NOT
    // element_at) and the result type is the map's value type (int64).
    auto plan = b.bind_select(as_select(parse("SELECT (MAP('k', user_id))['k'] AS v FROM clicks")));
    const auto& project = static_cast<const LogicalProject&>(*plan);
    EXPECT_NE(project.outputs()[0].expr_json.find("\"op\":\"map_get\""), std::string::npos);
    EXPECT_EQ(project.outputs()[0].expr_json.find("element_at"), std::string::npos);
    EXPECT_TRUE(project.outputs()[0].type->Equals(*arrow::int64()));
}

TEST(SqlBinder, MapOddArgCountRejected) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    EXPECT_THROW(b.bind_select(as_select(parse("SELECT MAP('a', 1, 'b') AS m FROM clicks"))),
                 TranslationError);
}

TEST(SqlBinder, ArrayAggBuildsAggregateWithListType) {
    Catalog cat;
    register_clicks(cat);  // user_id BIGINT, url TEXT
    Binder b(cat);
    auto plan = b.bind_select(
        as_select(parse("SELECT user_id, array_agg(url) AS urls FROM clicks GROUP BY user_id")));
    auto schema = plan->schema();
    auto field = schema->GetFieldByName("urls");
    ASSERT_NE(field, nullptr);
    EXPECT_TRUE(field->type()->Equals(*arrow::list(arrow::utf8())));
}

TEST(SqlBinder, ArrayAggDistinctIsAccepted) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    // DISTINCT is allowed for ARRAY_AGG (alongside COUNT / STRING_AGG).
    EXPECT_NO_THROW(b.bind_select(as_select(
        parse("SELECT user_id, array_agg(DISTINCT url) AS urls FROM clicks GROUP BY user_id"))));
}

TEST(SqlBinder, ExtendedScalarFunctionResultTypes) {
    Catalog cat;
    register_clicks(cat);  // user_id BIGINT, ts TIMESTAMP, url TEXT
    Binder b(cat);
    auto plan = b.bind_select(as_select(
        parse("SELECT char_length(url) AS cl, sqrt(user_id) AS sq, "
              "starts_with(url, 'h') AS sw, lpad(url, 5) AS lp, reverse(url) AS rv FROM clicks")));
    auto schema = plan->schema();
    EXPECT_TRUE(schema->GetFieldByName("cl")->type()->Equals(*arrow::int64()));
    EXPECT_TRUE(schema->GetFieldByName("sq")->type()->Equals(*arrow::float64()));
    EXPECT_TRUE(schema->GetFieldByName("sw")->type()->Equals(*arrow::boolean()));
    EXPECT_TRUE(schema->GetFieldByName("lp")->type()->Equals(*arrow::utf8()));
    EXPECT_TRUE(schema->GetFieldByName("rv")->type()->Equals(*arrow::utf8()));
}

TEST(SqlBinder, DateTimeFunctionResultTypes) {
    Catalog cat;
    register_clicks(cat);  // user_id BIGINT, ts TIMESTAMP, url TEXT
    Binder b(cat);
    auto plan = b.bind_select(as_select(
        parse("SELECT EXTRACT(YEAR FROM ts) AS y, DATE_TRUNC('day', ts) AS dt, "
              "TO_TIMESTAMP(url) AS tt, DATE_FORMAT(ts, 'yyyy-MM-dd') AS df FROM clicks")));
    auto schema = plan->schema();
    EXPECT_TRUE(schema->GetFieldByName("y")->type()->Equals(*arrow::int64()));
    EXPECT_TRUE(schema->GetFieldByName("dt")->type()->Equals(*arrow::int64()));
    EXPECT_TRUE(schema->GetFieldByName("tt")->type()->Equals(*arrow::int64()));
    EXPECT_TRUE(schema->GetFieldByName("df")->type()->Equals(*arrow::utf8()));
}

// Wall-clock functions are nondeterministic and rejected (clink keeps
// SQL replayable). NOW() arrives as a FunctionCall; CURRENT_TIMESTAMP as
// a SQLValueFunction rejected at parse.
TEST(SqlBinder, WallClockFunctionsRejectedForDeterminism) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    EXPECT_THROW(b.bind_select(as_select(parse("SELECT NOW() FROM clicks"))), TranslationError);
    EXPECT_THROW(parse("SELECT CURRENT_TIMESTAMP FROM clicks"), TranslationError);
}

TEST(SqlBinder, JsonFunctionResultTypes) {
    Catalog cat;
    register_clicks(cat);  // user_id BIGINT, ts TIMESTAMP, url TEXT
    Binder b(cat);
    auto plan = b.bind_select(
        as_select(parse("SELECT JSON_VALUE(url, '$.a') AS jv, JSON_QUERY(url, '$.a') AS jq, "
                        "JSON_EXISTS(url, '$.a') AS je, JSON_OBJECT('k', user_id) AS jo "
                        "FROM clicks")));
    auto schema = plan->schema();
    EXPECT_TRUE(schema->GetFieldByName("jv")->type()->Equals(*arrow::utf8()));
    EXPECT_TRUE(schema->GetFieldByName("jq")->type()->Equals(*arrow::utf8()));
    EXPECT_TRUE(schema->GetFieldByName("je")->type()->Equals(*arrow::boolean()));
    EXPECT_TRUE(schema->GetFieldByName("jo")->type()->Equals(*arrow::utf8()));
}

TEST(SqlBinder, CountDistinctAndStringAggBuildAggregates) {
    Catalog cat;
    auto regs = parse(
        "CREATE TABLE t (k BIGINT, v BIGINT, s TEXT) "
        "WITH (connector='file', format='json', path='/tmp/t.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[0]));
    Binder b(cat);
    auto plan = b.bind_select(as_select(
        parse("SELECT k, COUNT(DISTINCT v) AS cd, STRING_AGG(s, '|') AS sa FROM t GROUP BY k")));
    ASSERT_EQ(plan->kind(), "Aggregate");
    const auto& agg = static_cast<const LogicalAggregate&>(*plan);
    const AggregateOutput* cd = nullptr;
    const AggregateOutput* sa = nullptr;
    for (const auto& a : agg.aggregates()) {
        if (a.output_name == "cd")
            cd = &a;
        if (a.output_name == "sa")
            sa = &a;
    }
    ASSERT_NE(cd, nullptr);
    ASSERT_NE(sa, nullptr);
    EXPECT_EQ(cd->agg_fn, "count");
    EXPECT_TRUE(cd->distinct);
    EXPECT_TRUE(cd->type->Equals(*arrow::int64()));
    EXPECT_EQ(sa->agg_fn, "string_agg");
    EXPECT_FALSE(sa->distinct);
    EXPECT_EQ(sa->separator, "|");
    EXPECT_TRUE(sa->type->Equals(*arrow::utf8()));
}

// LISTAGG is accepted as an alias of STRING_AGG; the default separator
// is ",".
TEST(SqlBinder, ListaggAliasesStringAggWithDefaultSeparator) {
    Catalog cat;
    auto regs = parse(
        "CREATE TABLE t (k BIGINT, s TEXT) "
        "WITH (connector='file', format='json', path='/tmp/t.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[0]));
    Binder b(cat);
    auto plan = b.bind_select(as_select(parse("SELECT k, LISTAGG(s) AS l FROM t GROUP BY k")));
    const auto& agg = static_cast<const LogicalAggregate&>(*plan);
    const AggregateOutput* l = nullptr;
    for (const auto& a : agg.aggregates()) {
        if (a.output_name == "l")
            l = &a;
    }
    ASSERT_NE(l, nullptr);
    EXPECT_EQ(l->agg_fn, "string_agg");
    EXPECT_EQ(l->separator, ",");
}

// SQLOPT-3 UDAF: a registered aggregate UDF is recognised as an aggregate and
// types from the registry's declared return type.
TEST(SqlBinder, UdafBindsAsAggregate) {
    clink::AggFunctionRegistry::global().register_function(
        "b_udaf",
        arrow::int64(),
        []() { return clink::config::JsonValue{static_cast<std::int64_t>(0)}; },
        [](clink::config::JsonValue a, const std::vector<clink::config::JsonValue>&) { return a; },
        [](const clink::config::JsonValue& a) { return a; });
    Catalog cat;
    auto regs = parse(
        "CREATE TABLE t (k BIGINT, v BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/t.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[0]));
    Binder b(cat);
    auto plan = b.bind_select(as_select(parse("SELECT k, b_udaf(v) AS u FROM t GROUP BY k")));
    ASSERT_EQ(plan->kind(), "Aggregate");
    const auto& agg = static_cast<const LogicalAggregate&>(*plan);
    const AggregateOutput* u = nullptr;
    for (const auto& a : agg.aggregates()) {
        if (a.output_name == "u")
            u = &a;
    }
    ASSERT_NE(u, nullptr);
    EXPECT_EQ(u->agg_fn, "b_udaf");
    EXPECT_TRUE(u->type->Equals(*arrow::int64()));
    EXPECT_EQ(u->input_column, "v");
}

// DISTINCT and non-single-column-ref args are rejected for a UDAF, same as the
// built-in default aggregates.
TEST(SqlBinder, UdafDistinctAndArityRejected) {
    clink::AggFunctionRegistry::global().register_function(
        "b_udaf2",
        arrow::int64(),
        []() { return clink::config::JsonValue{static_cast<std::int64_t>(0)}; },
        [](clink::config::JsonValue a, const std::vector<clink::config::JsonValue>&) { return a; },
        [](const clink::config::JsonValue& a) { return a; });
    Catalog cat;
    auto regs = parse(
        "CREATE TABLE t (k BIGINT, v BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/t.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[0]));
    Binder b(cat);
    EXPECT_THROW(b.bind_select(as_select(parse("SELECT k, b_udaf2(DISTINCT v) FROM t GROUP BY k"))),
                 TranslationError);
    EXPECT_THROW(b.bind_select(as_select(parse("SELECT k, b_udaf2(v, v) FROM t GROUP BY k"))),
                 TranslationError);
}

// AggFunctionRegistry basics: register/contains/return_type + null-closure guard.
TEST(SqlBinder, AggFunctionRegistryBasics) {
    auto& reg = clink::AggFunctionRegistry::global();
    reg.register_function(
        "b_reg",
        arrow::utf8(),
        []() { return clink::config::JsonValue{nullptr}; },
        [](clink::config::JsonValue a, const std::vector<clink::config::JsonValue>&) { return a; },
        [](const clink::config::JsonValue& a) { return a; });
    EXPECT_TRUE(reg.contains("b_reg"));
    ASSERT_NE(reg.return_type("b_reg"), nullptr);
    EXPECT_TRUE(reg.return_type("b_reg")->Equals(*arrow::utf8()));
    EXPECT_FALSE(reg.contains("b_reg_absent"));
    EXPECT_EQ(reg.return_type("b_reg_absent"), nullptr);
    // init/accumulate/result are required.
    EXPECT_THROW(reg.register_function("b_bad", arrow::int64(), nullptr, nullptr, nullptr),
                 std::runtime_error);
}

TEST(SqlBinder, DistinctRejectedOnSumAndInOver) {
    Catalog cat;
    auto regs = parse(
        "CREATE TABLE ev (k BIGINT, v BIGINT, ts BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/ev.ndjson', event_time_column='ts')");
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[0]));
    Binder b(cat);
    // DISTINCT only on COUNT / STRING_AGG.
    EXPECT_THROW(b.bind_select(as_select(parse("SELECT k, SUM(DISTINCT v) FROM ev GROUP BY k"))),
                 TranslationError);
    // DISTINCT inside an OVER aggregate is rejected.
    EXPECT_THROW(b.bind_select(as_select(parse(
                     "SELECT *, COUNT(DISTINCT v) OVER (PARTITION BY k ORDER BY ts) FROM ev"))),
                 TranslationError);
}

TEST(SqlBinder, CastLowersToCastIntFloatStr) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(as_select(
        parse("SELECT CAST(user_id AS TEXT) AS uid_s, CAST(url AS BIGINT) AS u_i FROM clicks")));
    const auto& project = static_cast<const LogicalProject&>(*plan);
    EXPECT_NE(project.outputs()[0].expr_json.find("\"op\":\"cast_str\""), std::string::npos);
    EXPECT_NE(project.outputs()[1].expr_json.find("\"op\":\"cast_int\""), std::string::npos);
    EXPECT_TRUE(project.outputs()[0].type->Equals(*arrow::utf8()));
    EXPECT_TRUE(project.outputs()[1].type->Equals(*arrow::int64()));
}

TEST(SqlBinder, UnknownTableThrowsTranslationError) {
    Catalog cat;
    Binder b(cat);
    EXPECT_THROW(b.bind_select(as_select(parse("SELECT a FROM nope"))), TranslationError);
}

TEST(SqlBinder, UnknownColumnThrowsTranslationError) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    EXPECT_THROW(b.bind_select(as_select(parse("SELECT zzz FROM clicks"))), TranslationError);
}

TEST(SqlBinder, BindsInsertIntoSelect) {
    Catalog cat;
    register_clicks(cat);
    register_sink(cat, "sink_t", "user_id BIGINT, url TEXT");
    Binder b(cat);
    auto plan =
        b.bind_insert(as_insert(parse("INSERT INTO sink_t SELECT user_id, url FROM clicks")));
    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->kind(), "Sink");
    const auto& sink = static_cast<const LogicalSink&>(*plan);
    EXPECT_EQ(sink.table().name, "sink_t");
    EXPECT_EQ(sink.input().kind(), "Project");
}

TEST(SqlBinder, InsertColumnCountMismatchRejected) {
    Catalog cat;
    register_clicks(cat);
    register_sink(cat, "sink_one_col", "user_id BIGINT");
    Binder b(cat);
    EXPECT_THROW(
        b.bind_insert(as_insert(parse("INSERT INTO sink_one_col SELECT user_id, url FROM clicks"))),
        TranslationError);
}

TEST(SqlBinder, InsertColumnTypeMismatchRejected) {
    Catalog cat;
    register_clicks(cat);
    register_sink(cat, "sink_wrong_type", "user_id TEXT, url TEXT");
    Binder b(cat);
    EXPECT_THROW(b.bind_insert(as_insert(
                     parse("INSERT INTO sink_wrong_type SELECT user_id, url FROM clicks"))),
                 TranslationError);
}

TEST(SqlBinder, ExplainProducesReadableTree) {
    Catalog cat;
    register_clicks(cat);
    register_sink(cat, "sink_t", "user_id BIGINT, url TEXT");
    Binder b(cat);
    auto plan =
        b.bind_insert(as_insert(parse("INSERT INTO sink_t SELECT user_id, url FROM clicks")));
    auto text = plan->explain();
    EXPECT_NE(text.find("Sink"), std::string::npos);
    EXPECT_NE(text.find("Project"), std::string::npos);
    EXPECT_NE(text.find("Scan"), std::string::npos);
    EXPECT_NE(text.find("BIGINT"), std::string::npos);
}

// --- Aggregates + windowed GROUP BY -------------------------------

TEST(SqlBinder, AggregateWithoutGroupByRejected) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    EXPECT_THROW(b.bind_select(as_select(parse("SELECT SUM(user_id) FROM clicks"))),
                 TranslationError);
}

TEST(SqlBinder, GroupByWithTumbleBuildsWindowAggregate) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan =
        b.bind_select(as_select(parse("SELECT url, SUM(user_id) AS s FROM clicks "
                                      "GROUP BY TUMBLE(ts, 1000), url")));
    ASSERT_EQ(plan->kind(), "WindowAggregate");
    const auto& agg = static_cast<const LogicalWindowAggregate&>(*plan);
    EXPECT_EQ(agg.window().kind, WindowSpec::Kind::Tumble);
    EXPECT_EQ(agg.window().time_column, "ts");
    EXPECT_EQ(agg.window().size_ms, 1000);
    EXPECT_EQ(agg.group_keys(), std::vector<std::string>{"url"});
    ASSERT_EQ(agg.aggregates().size(), 1u);
    EXPECT_EQ(agg.aggregates()[0].agg_fn, "sum");
    EXPECT_EQ(agg.aggregates()[0].input_column, "user_id");
    EXPECT_EQ(agg.aggregates()[0].output_name, "s");
}

TEST(SqlBinder, CountStarRecognized) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(
        as_select(parse("SELECT url, COUNT(*) AS n FROM clicks GROUP BY TUMBLE(ts, 1000), url")));
    const auto& agg = static_cast<const LogicalWindowAggregate&>(*plan);
    EXPECT_EQ(agg.aggregates()[0].agg_fn, "count");
    EXPECT_TRUE(agg.aggregates()[0].input_column.empty());
}

TEST(SqlBinder, NonGroupedColumnInSelectRejected) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    // ts is not in GROUP BY but referenced in SELECT -> error.
    EXPECT_THROW(b.bind_select(as_select(parse("SELECT url, ts, SUM(user_id) FROM clicks "
                                               "GROUP BY TUMBLE(ts, 1000), url"))),
                 TranslationError);
}

TEST(SqlBinder, WindowBoundsSelectableInWindowedAggregate) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(
        as_select(parse("SELECT url, COUNT(*) AS n, window_start AS ws, window_end AS we "
                        "FROM clicks GROUP BY TUMBLE(ts, 1000), url")));
    const auto& agg = static_cast<const LogicalWindowAggregate&>(*plan);
    // The output schema carries the two bounds as BIGINT under their SELECT
    // aliases, interleaved in SELECT order.
    auto schema = agg.schema();
    ASSERT_EQ(schema->num_fields(), 4);
    EXPECT_EQ(schema->field(0)->name(), "url");
    EXPECT_EQ(schema->field(1)->name(), "n");
    EXPECT_EQ(schema->field(2)->name(), "ws");
    EXPECT_EQ(schema->field(3)->name(), "we");
    EXPECT_TRUE(schema->field(2)->type()->Equals(*arrow::int64()));
    EXPECT_TRUE(schema->field(3)->type()->Equals(*arrow::int64()));
    // The runtime op is told to emit each bound under its alias.
    EXPECT_EQ(agg.window_start_output(), "ws");
    EXPECT_EQ(agg.window_end_output(), "we");
}

TEST(SqlBinder, WindowBoundsSelectableBareName) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan =
        b.bind_select(as_select(parse("SELECT window_start, window_end, COUNT(*) AS n "
                                      "FROM clicks GROUP BY SESSION(ts, 500), url")));
    const auto& agg = static_cast<const LogicalWindowAggregate&>(*plan);
    auto schema = agg.schema();
    ASSERT_EQ(schema->num_fields(), 3);
    EXPECT_EQ(schema->field(0)->name(), "window_start");
    EXPECT_EQ(schema->field(1)->name(), "window_end");
    EXPECT_EQ(agg.window_start_output(), "window_start");
    EXPECT_EQ(agg.window_end_output(), "window_end");
}

TEST(SqlBinder, WindowBoundsRejectedWithoutWindowTVF) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    // A plain (non-windowed) GROUP BY has no window bounds to project.
    EXPECT_THROW(b.bind_select(as_select(
                     parse("SELECT url, window_start, COUNT(*) AS n FROM clicks GROUP BY url"))),
                 TranslationError);
}

TEST(SqlBinder, WindowBoundProjectedTwiceRejected) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    EXPECT_THROW(
        b.bind_select(as_select(parse("SELECT window_start AS a, window_start AS b, COUNT(*) AS n "
                                      "FROM clicks GROUP BY TUMBLE(ts, 1000), url"))),
        TranslationError);
}

TEST(SqlBinder, RealColumnNamedWindowStartCollidesWithBound) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE evts (window_start BIGINT, ts TIMESTAMP(3), amount BIGINT) "
        "WITH (connector='kafka', topic='e', bootstrap='localhost:9092')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    Binder b(cat);
    // A windowed GROUP BY emits a synthetic window_start, which would overwrite
    // the real same-named column -> reject the collision at bind time.
    EXPECT_THROW(b.bind_select(as_select(parse("SELECT window_start, COUNT(*) AS n FROM evts "
                                               "GROUP BY TUMBLE(ts, 1000), window_start"))),
                 TranslationError);
    // The same column is fine in a NON-windowed GROUP BY (no synthetic bound).
    EXPECT_NO_THROW(b.bind_select(
        as_select(parse("SELECT window_start, COUNT(*) AS n FROM evts GROUP BY window_start"))));
}

TEST(SqlBinder, TumbleAcceptsIntervalSyntax) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan =
        b.bind_select(as_select(parse("SELECT url, COUNT(*) AS n FROM clicks "
                                      "GROUP BY TUMBLE(ts, INTERVAL '5' SECOND), url")));
    const auto& agg = static_cast<const LogicalWindowAggregate&>(*plan);
    EXPECT_EQ(agg.window().size_ms, 5000);
}

TEST(SqlBinder, TumbleAcceptsIntervalMinuteAndHour) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    {
        auto plan =
            b.bind_select(as_select(parse("SELECT url, COUNT(*) AS n FROM clicks "
                                          "GROUP BY TUMBLE(ts, INTERVAL '10' MINUTE), url")));
        EXPECT_EQ(static_cast<const LogicalWindowAggregate&>(*plan).window().size_ms,
                  10LL * 60 * 1000);
    }
    {
        auto plan =
            b.bind_select(as_select(parse("SELECT url, COUNT(*) AS n FROM clicks "
                                          "GROUP BY TUMBLE(ts, INTERVAL '1' HOUR), url")));
        EXPECT_EQ(static_cast<const LogicalWindowAggregate&>(*plan).window().size_ms, 3600 * 1000);
    }
}

TEST(SqlBinder, StddevVarianceRecognizedAsAggregates) {
    Catalog cat;
    register_clicks(cat);  // user_id BIGINT, ts TIMESTAMP, url TEXT
    Binder b(cat);
    auto plan = b.bind_select(
        as_select(parse("SELECT user_id, STDDEV_POP(user_id) AS sd, VAR_SAMP(user_id) AS v "
                        "FROM clicks GROUP BY user_id")));
    ASSERT_EQ(plan->kind(), "Aggregate");
    const auto& agg = static_cast<const LogicalAggregate&>(*plan);
    ASSERT_EQ(agg.aggregates().size(), 2u);
    EXPECT_EQ(agg.aggregates()[0].agg_fn, "stddev_pop");
    EXPECT_TRUE(agg.aggregates()[0].type->Equals(*arrow::float64()));
    EXPECT_EQ(agg.aggregates()[1].agg_fn, "var_samp");
    EXPECT_TRUE(agg.aggregates()[1].type->Equals(*arrow::float64()));
}

TEST(SqlBinder, GroupByCumulateBuildsWindowAggregate) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan =
        b.bind_select(as_select(parse("SELECT url, COUNT(*) AS n FROM clicks "
                                      "GROUP BY CUMULATE(ts, 1000, 3000), url")));
    ASSERT_EQ(plan->kind(), "WindowAggregate");
    const auto& agg = static_cast<const LogicalWindowAggregate&>(*plan);
    EXPECT_EQ(agg.window().kind, WindowSpec::Kind::Cumulate);
    EXPECT_EQ(agg.window().time_column, "ts");
    EXPECT_EQ(agg.window().step_ms, 1000);  // CUMULATE(ts, step, size)
    EXPECT_EQ(agg.window().size_ms, 3000);
    EXPECT_EQ(agg.group_keys(), std::vector<std::string>{"url"});
}

TEST(SqlBinder, CumulateRejectsNonMultipleSize) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    // size must be an integer multiple of step.
    EXPECT_THROW(b.bind_select(as_select(parse("SELECT COUNT(*) AS n FROM clicks "
                                               "GROUP BY CUMULATE(ts, 1000, 2500)"))),
                 TranslationError);
}

TEST(SqlBinder, CumulateRejectsZeroStep) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    EXPECT_THROW(b.bind_select(as_select(parse("SELECT COUNT(*) AS n FROM clicks "
                                               "GROUP BY CUMULATE(ts, 0, 3000)"))),
                 TranslationError);
}

// --- Analytics depth: OVER (running) aggregates -------------------

TEST(SqlBinder, OverAggregateBuildsLogicalNode) {
    Catalog cat;
    register_events(cat);
    Binder b(cat);
    auto plan = b.bind_select(as_select(parse(
        "SELECT *, SUM(amount) OVER (PARTITION BY user_id ORDER BY ts) AS running FROM evt")));
    ASSERT_EQ(plan->kind(), "OverAggregate");
    const auto& ov = static_cast<const LogicalOverAggregate&>(*plan);
    ASSERT_EQ(ov.partition_columns().size(), 1u);
    EXPECT_EQ(ov.partition_columns()[0], "user_id");
    EXPECT_EQ(ov.order_time_column(), "ts");
    ASSERT_EQ(ov.outputs().size(), 1u);
    EXPECT_EQ(ov.outputs()[0].fn, "sum");
    EXPECT_EQ(ov.outputs()[0].input_column, "amount");
    EXPECT_EQ(ov.outputs()[0].output_name, "running");
    EXPECT_NE(plan->schema()->GetFieldByName("running"), nullptr);
}

TEST(SqlBinder, OverBoundedFrameBindsRowsAndRange) {
    Catalog cat;
    register_events(cat);
    Binder b(cat);
    auto plan = b.bind_select(as_select(parse(
        "SELECT *, "
        "SUM(amount) OVER (PARTITION BY user_id ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT "
        "ROW) AS r, "
        "SUM(amount) OVER (PARTITION BY user_id ORDER BY ts RANGE BETWEEN 5 PRECEDING AND CURRENT "
        "ROW) AS g FROM evt")));
    const auto& ov = static_cast<const LogicalOverAggregate&>(*plan);
    ASSERT_EQ(ov.outputs().size(), 2u);
    EXPECT_EQ(ov.outputs()[0].frame_mode, 1);  // ROWS
    EXPECT_EQ(ov.outputs()[0].frame_start, 3);
    EXPECT_EQ(ov.outputs()[1].frame_mode, 2);  // RANGE
    EXPECT_EQ(ov.outputs()[1].frame_start, 5);
}

TEST(SqlBinder, OverBoundedFrameRejectedForNavigationFns) {
    Catalog cat;
    register_events(cat);
    Binder b(cat);
    // A bounded frame on FIRST_VALUE / LAG is not supported (their semantics
    // are defined against the running frame).
    EXPECT_THROW(b.bind_select(as_select(
                     parse("SELECT *, FIRST_VALUE(amount) OVER (PARTITION BY user_id ORDER BY ts "
                           "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS fv FROM evt"))),
                 TranslationError);
}

TEST(SqlBinder, OverFollowingFrameRejected) {
    // A FOLLOWING end bound is rejected at translation.
    EXPECT_THROW(parse("SELECT SUM(x) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) "
                       "FROM t"),
                 TranslationError);
}

TEST(SqlBinder, OverAggregateLagAndNavBind) {
    Catalog cat;
    register_events(cat);
    Binder b(cat);
    auto plan = b.bind_select(
        as_select(parse("SELECT *, LAG(amount, 2) OVER (PARTITION BY user_id ORDER BY ts) AS prev, "
                        "FIRST_VALUE(amount) OVER (PARTITION BY user_id ORDER BY ts) AS fv, "
                        "COUNT(*) OVER (PARTITION BY user_id ORDER BY ts) AS c FROM evt")));
    const auto& ov = static_cast<const LogicalOverAggregate&>(*plan);
    ASSERT_EQ(ov.outputs().size(), 3u);
    EXPECT_EQ(ov.outputs()[0].fn, "lag");
    EXPECT_EQ(ov.outputs()[0].lag_offset, 2);
    EXPECT_EQ(ov.outputs()[1].fn, "first_value");
    EXPECT_EQ(ov.outputs()[2].fn, "count");
    EXPECT_TRUE(ov.outputs()[2].input_column.empty());  // COUNT(*)
}

TEST(SqlBinder, OverLeadRejected) {
    Catalog cat;
    register_events(cat);
    Binder b(cat);
    EXPECT_THROW(
        b.bind_select(as_select(
            parse("SELECT *, LEAD(amount) OVER (PARTITION BY user_id ORDER BY ts) AS x FROM evt"))),
        TranslationError);
}

TEST(SqlBinder, OverNtileRejected) {
    Catalog cat;
    register_events(cat);
    Binder b(cat);
    EXPECT_THROW(b.bind_select(as_select(parse(
                     "SELECT *, NTILE(4) OVER (PARTITION BY user_id ORDER BY ts) AS x FROM evt"))),
                 TranslationError);
}

TEST(SqlBinder, OverBoundedFrameShorthandBinds) {
    Catalog cat;
    register_events(cat);
    Binder b(cat);
    // The `ROWS <n> PRECEDING` shorthand (no BETWEEN) is equivalent to
    // `ROWS BETWEEN <n> PRECEDING AND CURRENT ROW`.
    auto plan =
        b.bind_select(as_select(parse("SELECT *, SUM(amount) OVER (PARTITION BY user_id "
                                      "ORDER BY ts ROWS 2 PRECEDING) AS s FROM evt")));
    const auto& ov = static_cast<const LogicalOverAggregate&>(*plan);
    ASSERT_EQ(ov.outputs().size(), 1u);
    EXPECT_EQ(ov.outputs()[0].frame_mode, 1);  // ROWS
    EXPECT_EQ(ov.outputs()[0].frame_start, 2);
}

TEST(SqlBinder, OverOrderMustBeEventTimeColumn) {
    Catalog cat;
    register_events(cat);
    Binder b(cat);
    // ORDER BY a non-event-time column is rejected (watermark alignment).
    EXPECT_THROW(b.bind_select(as_select(
                     parse("SELECT *, SUM(amount) OVER (PARTITION BY user_id ORDER BY amount) AS s "
                           "FROM evt"))),
                 TranslationError);
}

// --- Inc 4: subqueries -------------------------------------------

namespace {
void register_vips(Catalog& cat) {
    auto s = parse(
        "CREATE TABLE vips (user_id BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/vips.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
}
void register_amounts(Catalog& cat) {
    auto s = parse(
        "CREATE TABLE orders (id BIGINT, amount BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/orders.ndjson');"
        "CREATE TABLE refs (amount BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/refs.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
}
}  // namespace

TEST(SqlBinder, InSubqueryBuildsSemiJoin) {
    Catalog cat;
    register_clicks(cat);
    register_vips(cat);
    Binder b(cat);
    auto plan = b.bind_select(
        as_select(parse("SELECT * FROM clicks WHERE user_id IN (SELECT user_id FROM vips)")));
    ASSERT_EQ(plan->kind(), "SemiJoin");
    const auto& sj = static_cast<const LogicalSemiJoin&>(*plan);
    EXPECT_FALSE(sj.anti());
    EXPECT_EQ(sj.left_key_columns(), (std::vector<std::string>{"user_id"}));
    EXPECT_EQ(sj.right_key_columns(), (std::vector<std::string>{"user_id"}));
}

TEST(SqlBinder, NotInBuildsAntiJoin) {
    Catalog cat;
    register_clicks(cat);
    register_vips(cat);
    Binder b(cat);
    auto plan = b.bind_select(
        as_select(parse("SELECT * FROM clicks WHERE user_id NOT IN (SELECT user_id FROM vips)")));
    ASSERT_EQ(plan->kind(), "SemiJoin");
    EXPECT_TRUE(static_cast<const LogicalSemiJoin&>(*plan).anti());
}

TEST(SqlBinder, ExistsCorrelatedBuildsSemiJoin) {
    Catalog cat;
    register_clicks(cat);
    register_vips(cat);
    Binder b(cat);
    auto plan =
        b.bind_select(as_select(parse("SELECT * FROM clicks c WHERE EXISTS "
                                      "(SELECT 1 FROM vips v WHERE v.user_id = c.user_id)")));
    ASSERT_EQ(plan->kind(), "SemiJoin");
    const auto& sj = static_cast<const LogicalSemiJoin&>(*plan);
    EXPECT_FALSE(sj.anti());
    EXPECT_EQ(sj.left_key_columns(), (std::vector<std::string>{"user_id"}));
    EXPECT_EQ(sj.right_key_columns(), (std::vector<std::string>{"user_id"}));
}

TEST(SqlBinder, NotExistsBuildsAntiJoin) {
    Catalog cat;
    register_clicks(cat);
    register_vips(cat);
    Binder b(cat);
    auto plan =
        b.bind_select(as_select(parse("SELECT * FROM clicks c WHERE NOT EXISTS "
                                      "(SELECT 1 FROM vips v WHERE v.user_id = c.user_id)")));
    ASSERT_EQ(plan->kind(), "SemiJoin");
    EXPECT_TRUE(static_cast<const LogicalSemiJoin&>(*plan).anti());
}

TEST(SqlBinder, ExistsWithoutCorrelationRejected) {
    Catalog cat;
    register_clicks(cat);
    register_vips(cat);
    Binder b(cat);
    EXPECT_THROW(
        b.bind_select(as_select(parse(
            "SELECT * FROM clicks c WHERE EXISTS (SELECT 1 FROM vips v WHERE v.user_id = 5)"))),
        TranslationError);
}

TEST(SqlBinder, ScalarSubqueryBuildsBroadcast) {
    Catalog cat;
    register_amounts(cat);
    Binder b(cat);
    auto plan = b.bind_select(
        as_select(parse("SELECT * FROM orders WHERE amount > (SELECT avg(amount) FROM refs)")));
    ASSERT_EQ(plan->kind(), "ScalarBroadcast");
    const auto& sb = static_cast<const LogicalScalarBroadcast&>(*plan);
    EXPECT_EQ(sb.test_column(), "amount");
    EXPECT_EQ(sb.comparison_op(), "gt");
}

// #55: scalar subquery as a SELECT-list item -> ScalarProject under a
// Project. The scalar value is appended to every outer row.
TEST(SqlBinder, ScalarSubqueryInSelectBuildsScalarProject) {
    Catalog cat;
    register_amounts(cat);
    Binder b(cat);
    auto plan = b.bind_select(
        as_select(parse("SELECT id, (SELECT avg(amount) FROM refs) AS a FROM orders")));
    ASSERT_EQ(plan->kind(), "Project");
    ASSERT_EQ(plan->inputs().size(), 1U);
    const auto* base = plan->inputs()[0];
    ASSERT_EQ(base->kind(), "ScalarProject");
    const auto& sp = static_cast<const LogicalScalarProject&>(*base);
    EXPECT_EQ(sp.main().kind(), "Scan");         // outer orders scan
    EXPECT_EQ(sp.scalar().kind(), "Aggregate");  // empty-group avg(amount)
    // The appended column is on the ScalarProject schema and read by the
    // same name on the scalar side (v1 invariant).
    EXPECT_EQ(sp.scalar_column(), sp.output_column());
    EXPECT_NE(sp.schema()->GetFieldByName(sp.output_column()), nullptr);
    // The projection exposes the main column plus the aliased scalar.
    EXPECT_NE(plan->schema()->GetFieldByName("id"), nullptr);
    EXPECT_NE(plan->schema()->GetFieldByName("a"), nullptr);
}

TEST(SqlBinder, ScalarSubqueryInSelectStarAppendsColumn) {
    Catalog cat;
    register_amounts(cat);
    Binder b(cat);
    auto plan = b.bind_select(
        as_select(parse("SELECT *, (SELECT avg(amount) FROM refs) AS a FROM orders")));
    ASSERT_EQ(plan->kind(), "Project");
    EXPECT_NE(plan->schema()->GetFieldByName("id"), nullptr);
    EXPECT_NE(plan->schema()->GetFieldByName("amount"), nullptr);
    EXPECT_NE(plan->schema()->GetFieldByName("a"), nullptr);
}

TEST(SqlBinder, ScalarSubqueryInSelectMultipleRejected) {
    Catalog cat;
    register_amounts(cat);
    Binder b(cat);
    EXPECT_THROW(b.bind_select(as_select(parse("SELECT (SELECT avg(amount) FROM refs), "
                                               "(SELECT min(amount) FROM refs) FROM orders"))),
                 TranslationError);
}

TEST(SqlBinder, ScalarSubqueryInSelectWithGroupByRejected) {
    Catalog cat;
    register_amounts(cat);
    Binder b(cat);
    EXPECT_THROW(b.bind_select(as_select(
                     parse("SELECT id, (SELECT avg(amount) FROM refs) FROM orders GROUP BY id"))),
                 TranslationError);
}

// --- Wave 3: composite-key subqueries ----------------------------

namespace {
void register_ab_st(Catalog& cat) {
    for (const char* ddl : {"CREATE TABLE t (a BIGINT, b BIGINT) "
                            "WITH (connector='file', format='json', path='/tmp/t.ndjson')",
                            "CREATE TABLE s (x BIGINT, y BIGINT) "
                            "WITH (connector='file', format='json', path='/tmp/s.ndjson')"}) {
        cat.register_table(std::get<ast::CreateTableStmt>(parse(ddl).statements[0]));
    }
}
}  // namespace

TEST(SqlBinder, MultiColumnInBuildsCompositeSemiJoin) {
    Catalog cat;
    register_ab_st(cat);
    Binder b(cat);
    auto plan =
        b.bind_select(as_select(parse("SELECT * FROM t WHERE (a, b) IN (SELECT x, y FROM s)")));
    ASSERT_EQ(plan->kind(), "SemiJoin");
    const auto& sj = static_cast<const LogicalSemiJoin&>(*plan);
    EXPECT_FALSE(sj.anti());
    EXPECT_EQ(sj.left_key_columns(), (std::vector<std::string>{"a", "b"}));
    EXPECT_EQ(sj.right_key_columns(), (std::vector<std::string>{"x", "y"}));
}

// #49: multi-column NOT IN now binds to a composite null-aware anti join
// (previously rejected). The per-position NULL poison lives in the runtime op.
TEST(SqlBinder, MultiColumnNotInBuildsCompositeAntiJoin) {
    Catalog cat;
    register_ab_st(cat);
    Binder b(cat);
    auto plan =
        b.bind_select(as_select(parse("SELECT * FROM t WHERE (a, b) NOT IN (SELECT x, y FROM s)")));
    ASSERT_EQ(plan->kind(), "SemiJoin");
    const auto& sj = static_cast<const LogicalSemiJoin&>(*plan);
    EXPECT_TRUE(sj.anti());
    EXPECT_TRUE(sj.null_aware());  // NOT IN: per-position 3VL poison
    EXPECT_EQ(sj.left_key_columns(), (std::vector<std::string>{"a", "b"}));
    EXPECT_EQ(sj.right_key_columns(), (std::vector<std::string>{"x", "y"}));
}

TEST(SqlBinder, MultiColumnInArityMismatchRejected) {
    Catalog cat;
    register_ab_st(cat);
    Binder b(cat);
    EXPECT_THROW(
        b.bind_select(as_select(parse("SELECT * FROM t WHERE (a, b) IN (SELECT x FROM s)"))),
        TranslationError);
}

TEST(SqlBinder, MultiEqualityExistsBuildsCompositeSemiJoin) {
    Catalog cat;
    register_ab_st(cat);
    Binder b(cat);
    auto plan = b.bind_select(as_select(
        parse("SELECT * FROM t WHERE EXISTS (SELECT 1 FROM s WHERE s.x = t.a AND s.y = t.b)")));
    ASSERT_EQ(plan->kind(), "SemiJoin");
    const auto& sj = static_cast<const LogicalSemiJoin&>(*plan);
    EXPECT_FALSE(sj.anti());
    EXPECT_EQ(sj.left_key_columns(), (std::vector<std::string>{"a", "b"}));
    EXPECT_EQ(sj.right_key_columns(), (std::vector<std::string>{"x", "y"}));
}

TEST(SqlBinder, MultiEqualityNotExistsBuildsCompositePlainAnti) {
    Catalog cat;
    register_ab_st(cat);
    Binder b(cat);
    // Multi-equality NOT EXISTS -> composite anti, plain (null_aware=false).
    auto plan =
        b.bind_select(as_select(parse("SELECT * FROM t WHERE NOT EXISTS "
                                      "(SELECT 1 FROM s WHERE s.x = t.a AND s.y = t.b)")));
    ASSERT_EQ(plan->kind(), "SemiJoin");
    const auto& sj = static_cast<const LogicalSemiJoin&>(*plan);
    EXPECT_TRUE(sj.anti());
    EXPECT_FALSE(sj.null_aware());  // EXISTS-family: plain anti, not NOT-IN poison
    EXPECT_EQ(sj.left_key_columns(), (std::vector<std::string>{"a", "b"}));
    EXPECT_EQ(sj.right_key_columns(), (std::vector<std::string>{"x", "y"}));
}

TEST(SqlBinder, ScalarSubqueryNonAggregateRejected) {
    Catalog cat;
    register_amounts(cat);
    Binder b(cat);
    EXPECT_THROW(b.bind_select(as_select(
                     parse("SELECT * FROM orders WHERE amount > (SELECT amount FROM refs)"))),
                 TranslationError);
}
// --- 2PC SQL contract ---------------------------------------------

TEST(SqlBinder, ExactlyOnceFileSinkAccepted) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE src_t (k BIGINT, v BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/in.ndjson');"
        "CREATE TABLE eo_sink (k BIGINT, v BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/eo/', "
        "      delivery_guarantee='exactly_once')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    Binder b(cat);
    auto plan = b.bind_insert(as_insert(parse("INSERT INTO eo_sink SELECT k, v FROM src_t")));
    ASSERT_NE(plan, nullptr);
}

TEST(SqlBinder, ExactlyOnceKafkaSinkAccepted) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE src_t (k BIGINT, v BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/in.ndjson');"
        "CREATE TABLE kafka_eo (k BIGINT, v BIGINT) "
        "WITH (connector='kafka', format='json', topic='out', "
        "      brokers='localhost:9092', delivery_guarantee='exactly_once')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    Binder b(cat);
    auto plan = b.bind_insert(as_insert(parse("INSERT INTO kafka_eo SELECT k, v FROM src_t")));
    ASSERT_NE(plan, nullptr);
}

TEST(SqlBinder, ExactlyOnceOnUnsupportedConnectorRejected) {
    // clickhouse has no 2PC sink variant (append-only, no cross-statement
    // transactions), so delivery_guarantee='exactly_once' is rejected. (postgres
    // IS supported via PREPARE TRANSACTION - see PostgresExactlyOnceSinkSelected.)
    Catalog cat;
    auto s = parse(
        "CREATE TABLE src_t (k BIGINT, v BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/in.ndjson');"
        "CREATE TABLE ch_eo (k BIGINT, v BIGINT) "
        "WITH (connector='clickhouse', dsn='tcp://localhost', "
        "      delivery_guarantee='exactly_once')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    Binder b(cat);
    EXPECT_THROW(b.bind_insert(as_insert(parse("INSERT INTO ch_eo SELECT k, v FROM src_t"))),
                 TranslationError);
}

TEST(SqlBinder, ExactlyOncePlusUpsertRejected) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE src_t (k BIGINT, v BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/in.ndjson');"
        "CREATE TABLE both_modes (k BIGINT, v BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/out/', "
        "      mode='upsert', primary_key='k', "
        "      delivery_guarantee='exactly_once')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    Binder b(cat);
    EXPECT_THROW(b.bind_insert(as_insert(parse("INSERT INTO both_modes SELECT k, v FROM src_t"))),
                 TranslationError);
}

// --- Cross-sink commit group --------------------------------------

TEST(SqlBinder, CommitGroupOnExactlyOnceFileSinkAccepted) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE src_t (k BIGINT, v BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/in.ndjson');"
        "CREATE TABLE grouped_sink (k BIGINT, v BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/out/', "
        "      delivery_guarantee='exactly_once', commit_group='my-group')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    Binder b(cat);
    auto plan = b.bind_insert(as_insert(parse("INSERT INTO grouped_sink SELECT k, v FROM src_t")));
    ASSERT_NE(plan, nullptr);

    // The TableDef accessor surfaces the commit_group for the planner.
    const auto* table = cat.get_table("grouped_sink");
    ASSERT_NE(table, nullptr);
    EXPECT_EQ(table->commit_group(), "my-group");
    EXPECT_TRUE(table->has_commit_group());
}

TEST(SqlBinder, CommitGroupRequiresExactlyOnce) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE src_t (k BIGINT, v BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/in.ndjson');"
        "CREATE TABLE no_eo (k BIGINT, v BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson', "
        "      commit_group='my-group')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    Binder b(cat);
    // commit_group is meaningless without exactly_once - reject so
    // users don't think it's wired up.
    EXPECT_THROW(b.bind_insert(as_insert(parse("INSERT INTO no_eo SELECT k, v FROM src_t"))),
                 TranslationError);
}

TEST(SqlBinder, EmptyCommitGroupOnAppendSinkAccepted) {
    // The empty / absent default should NOT trigger the
    // commit_group-requires-exactly_once rejection. This catches a
    // common false-positive in the validation path.
    Catalog cat;
    auto s = parse(
        "CREATE TABLE src_t (k BIGINT, v BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/in.ndjson');"
        "CREATE TABLE append_sink (k BIGINT, v BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    Binder b(cat);
    auto plan = b.bind_insert(as_insert(parse("INSERT INTO append_sink SELECT k, v FROM src_t")));
    ASSERT_NE(plan, nullptr);

    const auto* table = cat.get_table("append_sink");
    ASSERT_NE(table, nullptr);
    EXPECT_FALSE(table->has_commit_group());
}

// --- Catalog upsert metadata + bind-time validation ---------------

TEST(SqlBinder, AppendSinkRejectsChangelogStream) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE src_t (user_id BIGINT, amount BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/in.ndjson');"
        "CREATE TABLE sink_append (user_id BIGINT, amount BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    Binder b(cat);
    EXPECT_THROW(
        b.bind_insert(as_insert(parse("INSERT INTO sink_append SELECT * FROM "
                                      "(SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id "
                                      "                              ORDER BY amount DESC) AS rn "
                                      "FROM src_t) sub WHERE rn <= 2"))),
        TranslationError);
}

TEST(SqlBinder, UpsertSinkAcceptsChangelogStream) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE src_t (user_id BIGINT, amount BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/in.ndjson');"
        "CREATE TABLE sink_up (user_id BIGINT, amount BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson', "
        "      mode='upsert', primary_key='user_id,amount')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    Binder b(cat);
    auto plan = b.bind_insert(as_insert(
        parse("INSERT INTO sink_up SELECT * FROM "
              "(SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY amount DESC) AS rn "
              "FROM src_t) sub WHERE rn <= 2")));
    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->kind(), "Sink");
}

TEST(SqlBinder, UpsertSinkWithoutPrimaryKeyRejected) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE src_t (user_id BIGINT, amount BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/in.ndjson');"
        "CREATE TABLE sink_up (user_id BIGINT, amount BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson', "
        "      mode='upsert')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    Binder b(cat);
    EXPECT_THROW(
        b.bind_insert(as_insert(parse("INSERT INTO sink_up SELECT user_id, amount FROM src_t"))),
        TranslationError);
}

TEST(SqlBinder, UpsertSinkPrimaryKeyMustExistInSelect) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE src_t (user_id BIGINT, amount BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/in.ndjson');"
        "CREATE TABLE sink_up (amount BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson', "
        "      mode='upsert', primary_key='user_id')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    Binder b(cat);
    EXPECT_THROW(b.bind_insert(as_insert(parse("INSERT INTO sink_up SELECT amount FROM src_t"))),
                 TranslationError);
}

TEST(SqlBinder, UpsertSinkAcceptsAppendStreamAsAllInserts) {
    Catalog cat;
    auto s = parse(
        "CREATE TABLE src_t (user_id BIGINT, amount BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/in.ndjson');"
        "CREATE TABLE sink_up (user_id BIGINT, amount BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson', "
        "      mode='upsert', primary_key='user_id')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[1]));
    Binder b(cat);
    auto plan =
        b.bind_insert(as_insert(parse("INSERT INTO sink_up SELECT user_id, amount FROM src_t")));
    ASSERT_NE(plan, nullptr);
}

// --- TOP-N-per-key pattern ----------------------------------------

TEST(SqlBinder, RowNumberWhereLeBuildsLogicalTopNPerKey) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(as_select(
        parse("SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ts DESC) "
              "AS rn FROM clicks) sub WHERE rn <= 3")));
    // Outer Project wraps the bound subquery (now LogicalTopNPerKey).
    ASSERT_EQ(plan->kind(), "Project");
    const auto& proj = static_cast<const LogicalProject&>(*plan);
    ASSERT_EQ(proj.input().kind(), "TopNPerKey");
    const auto& tn = static_cast<const LogicalTopNPerKey&>(proj.input());
    EXPECT_EQ(tn.count(), 3);
    ASSERT_EQ(tn.partition_columns().size(), 1u);
    EXPECT_EQ(tn.partition_columns()[0], "user_id");
    ASSERT_EQ(tn.sort_columns().size(), 1u);
    EXPECT_EQ(tn.sort_columns()[0], "ts");
    EXPECT_TRUE(tn.sort_descending()[0]);
    EXPECT_EQ(tn.rank_kind(), RankKind::RowNumber);
}

TEST(SqlBinder, RankWhereLeBuildsTopNPerKeyWithRankKind) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(as_select(
        parse("SELECT * FROM (SELECT *, RANK() OVER (PARTITION BY user_id ORDER BY ts DESC) "
              "AS rn FROM clicks) sub WHERE rn <= 3")));
    const auto& proj = static_cast<const LogicalProject&>(*plan);
    ASSERT_EQ(proj.input().kind(), "TopNPerKey");
    const auto& tn = static_cast<const LogicalTopNPerKey&>(proj.input());
    EXPECT_EQ(tn.count(), 3);
    EXPECT_EQ(tn.rank_kind(), RankKind::Rank);
}

TEST(SqlBinder, DenseRankWhereLeBuildsTopNPerKeyWithRankKind) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(as_select(
        parse("SELECT * FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY user_id ORDER BY ts DESC) "
              "AS rn FROM clicks) sub WHERE rn <= 2")));
    const auto& proj = static_cast<const LogicalProject&>(*plan);
    ASSERT_EQ(proj.input().kind(), "TopNPerKey");
    const auto& tn = static_cast<const LogicalTopNPerKey&>(proj.input());
    EXPECT_EQ(tn.rank_kind(), RankKind::DenseRank);
}

TEST(SqlBinder, RowNumberWhereEqOneSelectsTopOne) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(as_select(
        parse("SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ts) AS rn "
              "FROM clicks) sub WHERE rn = 1")));
    const auto& proj = static_cast<const LogicalProject&>(*plan);
    const auto& tn = static_cast<const LogicalTopNPerKey&>(proj.input());
    EXPECT_EQ(tn.count(), 1);
}

TEST(SqlBinder, RowNumberWhereLtConvertsToCountMinusOne) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(as_select(
        parse("SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ts) AS rn "
              "FROM clicks) sub WHERE rn < 5")));
    const auto& proj = static_cast<const LogicalProject&>(*plan);
    const auto& tn = static_cast<const LogicalTopNPerKey&>(proj.input());
    EXPECT_EQ(tn.count(), 4);
}

TEST(SqlBinder, RowNumberWhereOnNonRnColumnNotMatched) {
    // WHERE that doesn't reference rn falls through to the regular
    // derived-table path; the LogicalRowNumber is left in place and
    // the planner will surface the unpaired-ROW_NUMBER error.
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(as_select(
        parse("SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ts) AS rn "
              "FROM clicks) sub WHERE user_id > 100")));
    // Outer Project over a derived table; the inner is RowNumber, not
    // TopNPerKey.
    const auto& proj = static_cast<const LogicalProject&>(*plan);
    EXPECT_NE(proj.input().kind(), "TopNPerKey");
}

// --- ROW_NUMBER OVER parsing --------------------------------------

TEST(SqlBinder, RowNumberOverBuildsLogicalRowNumber) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(
        as_select(parse("SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ts DESC) AS rn "
                        "FROM clicks")));
    ASSERT_EQ(plan->kind(), "RowNumber");
    const auto& rn = static_cast<const LogicalRowNumber&>(*plan);
    ASSERT_EQ(rn.partition_columns().size(), 1u);
    EXPECT_EQ(rn.partition_columns()[0], "user_id");
    ASSERT_EQ(rn.sort_columns().size(), 1u);
    EXPECT_EQ(rn.sort_columns()[0], "ts");
    EXPECT_TRUE(rn.sort_descending()[0]);
    EXPECT_EQ(rn.output_name(), "rn");
    // Schema: inner cols + rn (BIGINT).
    EXPECT_EQ(plan->schema()->num_fields(), 4);
    EXPECT_EQ(plan->schema()->field(3)->name(), "rn");
    EXPECT_TRUE(plan->schema()->field(3)->type()->Equals(*arrow::int64()));
}

TEST(SqlBinder, RowNumberWithoutOrderByRejected) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    EXPECT_THROW(b.bind_select(as_select(parse(
                     "SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id) AS rn FROM clicks"))),
                 TranslationError);
}

TEST(SqlBinder, RankOverBuildsLogicalRowNumberWithRankKind) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(
        as_select(parse("SELECT *, RANK() OVER (PARTITION BY user_id ORDER BY ts DESC) AS rn "
                        "FROM clicks")));
    ASSERT_EQ(plan->kind(), "RowNumber");
    EXPECT_EQ(static_cast<const LogicalRowNumber&>(*plan).rank_kind(), RankKind::Rank);
}

TEST(SqlBinder, DenseRankOverBuildsLogicalRowNumberWithRankKind) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(
        as_select(parse("SELECT *, DENSE_RANK() OVER (PARTITION BY user_id ORDER BY ts) AS rn "
                        "FROM clicks")));
    ASSERT_EQ(plan->kind(), "RowNumber");
    EXPECT_EQ(static_cast<const LogicalRowNumber&>(*plan).rank_kind(), RankKind::DenseRank);
}

TEST(SqlBinder, RowNumberOverDefaultsToRowNumberRankKind) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(
        as_select(parse("SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ts) AS rn "
                        "FROM clicks")));
    EXPECT_EQ(static_cast<const LogicalRowNumber&>(*plan).rank_kind(), RankKind::RowNumber);
}

TEST(SqlBinder, UnsupportedWindowFunctionsRejected) {
    // NTILE / LEAD as bare ranking windows aren't supported; only
    // ROW_NUMBER / RANK / DENSE_RANK reach the Top-N path.
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    EXPECT_THROW(
        b.bind_select(as_select(
            parse("SELECT *, NTILE(4) OVER (PARTITION BY user_id ORDER BY ts) AS r FROM clicks"))),
        TranslationError);
    EXPECT_THROW(
        b.bind_select(as_select(
            parse("SELECT *, LEAD(url) OVER (PARTITION BY user_id ORDER BY ts) AS r FROM clicks"))),
        TranslationError);
}

TEST(SqlBinder, RowNumberOnlyAcceptsSelectStarShape) {
    // SELECT user_id, ROW_NUMBER()... isn't allowed in this phase -
    // only `SELECT *, ROW_NUMBER() AS rn`.
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    EXPECT_THROW(b.bind_select(as_select(
                     parse("SELECT user_id, ROW_NUMBER() OVER (ORDER BY ts) AS rn FROM clicks"))),
                 TranslationError);
}

// --- FROM-derived tables ------------------------------------------

TEST(SqlBinder, DerivedTableInlinesIntoOuterPlan) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(
        as_select(parse("SELECT url FROM (SELECT url FROM clicks WHERE user_id > 100) AS sub")));
    // Outer is Project over the subquery's bound plan (Project over
    // Filter over Scan). No Scan named 'sub' appears.
    ASSERT_EQ(plan->kind(), "Project");
    const auto& outer = static_cast<const LogicalProject&>(*plan);
    EXPECT_EQ(outer.input().kind(), "Project");
    const auto& inner_proj = static_cast<const LogicalProject&>(outer.input());
    EXPECT_EQ(inner_proj.input().kind(), "Filter");
}

TEST(SqlBinder, DerivedTableSchemaDrivesOuterResolution) {
    // The outer SELECT can reference any column declared in the
    // subquery's output schema (including aliased aggregates).
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(as_select(
        parse("SELECT n FROM (SELECT url AS u, COUNT(*) AS n FROM clicks GROUP BY url) AS s")));
    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->schema()->num_fields(), 1);
    EXPECT_EQ(plan->schema()->field(0)->name(), "n");
}

TEST(SqlBinder, DerivedTableMissingAliasRejected) {
    // PG's grammar accepts a subquery without an alias; we reject it
    // at ast_builder time so users get a clear message.
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    EXPECT_THROW(b.bind_select(as_select(parse("SELECT url FROM (SELECT url FROM clicks)"))),
                 TranslationError);
}

TEST(SqlBinder, DerivedTableReferencingUnknownColumnRejected) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    // Subquery exposes only `url`; outer asks for `user_id` which
    // isn't in the derived schema.
    EXPECT_THROW(
        b.bind_select(as_select(parse("SELECT user_id FROM (SELECT url FROM clicks) AS s"))),
        TranslationError);
}

// --- INSERT column list -------------------------------------------

TEST(SqlBinder, InsertColumnListReordersSelectOutputs) {
    Catalog cat;
    register_clicks(cat);
    register_sink(cat, "sink_t", "user_id BIGINT, url TEXT");
    Binder b(cat);
    // SELECT projects url then user_id; column-list says (url, user_id)
    // which matches the swapped SELECT order but the sink declares
    // (user_id, url). The binder must reorder.
    auto plan = b.bind_insert(
        as_insert(parse("INSERT INTO sink_t (url, user_id) SELECT url, user_id FROM clicks")));
    ASSERT_EQ(plan->kind(), "Sink");
    const auto& sink = static_cast<const LogicalSink&>(*plan);
    // After reorder, the project produces (user_id, url) in sink
    // declaration order.
    ASSERT_EQ(sink.input().schema()->num_fields(), 2);
    EXPECT_EQ(sink.input().schema()->field(0)->name(), "user_id");
    EXPECT_EQ(sink.input().schema()->field(1)->name(), "url");
}

TEST(SqlBinder, InsertColumnListWrongCountRejected) {
    Catalog cat;
    register_clicks(cat);
    register_sink(cat, "sink_t", "user_id BIGINT, url TEXT");
    Binder b(cat);
    EXPECT_THROW(b.bind_insert(as_insert(
                     parse("INSERT INTO sink_t (user_id) SELECT user_id, url FROM clicks"))),
                 TranslationError);
}

TEST(SqlBinder, InsertColumnListUnknownColumnRejected) {
    Catalog cat;
    register_clicks(cat);
    register_sink(cat, "sink_t", "user_id BIGINT, url TEXT");
    Binder b(cat);
    EXPECT_THROW(b.bind_insert(as_insert(
                     parse("INSERT INTO sink_t (user_id, page) SELECT user_id, url FROM clicks"))),
                 TranslationError);
}

TEST(SqlBinder, InsertPartialColumnListRejected) {
    Catalog cat;
    register_clicks(cat);
    register_sink(cat, "sink_t", "user_id BIGINT, ts TIMESTAMP(3), url TEXT");
    Binder b(cat);
    EXPECT_THROW(b.bind_insert(as_insert(
                     parse("INSERT INTO sink_t (user_id, url) SELECT user_id, url FROM clicks"))),
                 TranslationError);
}

// --- HAVING with direct aggregate ---------------------------------

TEST(SqlBinder, HavingDirectAggregateReferencesMatchingAlias) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    // HAVING SUM(user_id) > 100 - no alias for the aggregate.
    auto plan = b.bind_select(as_select(parse(
        "SELECT url, SUM(user_id) AS total FROM clicks GROUP BY url HAVING SUM(user_id) > 100")));
    ASSERT_EQ(plan->kind(), "Filter");
    const auto& f = static_cast<const LogicalFilter&>(*plan);
    // The aggregate FuncCall was rewritten into ColumnRef("total") -
    // the alias is what lower_predicate then resolves against the
    // synthetic post-aggregate schema.
    EXPECT_NE(f.predicate_json().find("\"col\":\"total\""), std::string::npos)
        << f.predicate_json();
    EXPECT_NE(f.predicate_json().find("\"op\":\"gt\""), std::string::npos);
}

TEST(SqlBinder, HavingDirectAggregateUnaliasedSelectStillWorks) {
    // Even without an explicit alias in SELECT, the binder synthesises
    // one ("sum_user_id") so HAVING SUM(user_id) still rewrites.
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(as_select(
        parse("SELECT url, SUM(user_id) FROM clicks GROUP BY url HAVING SUM(user_id) > 0")));
    ASSERT_EQ(plan->kind(), "Filter");
    const auto& f = static_cast<const LogicalFilter&>(*plan);
    EXPECT_NE(f.predicate_json().find("\"col\":\"sum_user_id\""), std::string::npos)
        << f.predicate_json();
}

TEST(SqlBinder, HavingDirectAggregateWithoutMatchInSelectRejected) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    // HAVING references SUM(user_id) but SELECT only has COUNT(*).
    EXPECT_THROW(b.bind_select(as_select(parse("SELECT url, COUNT(*) AS n FROM clicks GROUP BY url "
                                               "HAVING SUM(user_id) > 100"))),
                 TranslationError);
}

// --- OFFSET -------------------------------------------------------

TEST(SqlBinder, LimitOffsetWrapsLogicalLimitWithOffset) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(as_select(parse("SELECT url FROM clicks LIMIT 5 OFFSET 3")));
    ASSERT_EQ(plan->kind(), "Limit");
    const auto& lim = static_cast<const LogicalLimit&>(*plan);
    EXPECT_EQ(lim.count(), 5);
    EXPECT_EQ(lim.offset(), 3);
}

TEST(SqlBinder, OrderByLimitOffsetWrapsLogicalTopNWithOffset) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(
        as_select(parse("SELECT user_id FROM clicks ORDER BY user_id DESC LIMIT 5 OFFSET 2")));
    ASSERT_EQ(plan->kind(), "TopN");
    const auto& tn = static_cast<const LogicalTopN&>(*plan);
    EXPECT_EQ(tn.count(), 5);
    EXPECT_EQ(tn.offset(), 2);
}

TEST(SqlBinder, OffsetWithoutLimitRejected) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    EXPECT_THROW(b.bind_select(as_select(parse("SELECT url FROM clicks OFFSET 3"))),
                 TranslationError);
}

// --- IN literal-list ----------------------------------------------

TEST(SqlBinder, InListLowersToInPredicate) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan =
        b.bind_select(as_select(parse("SELECT url FROM clicks WHERE user_id IN (1, 2, 3)")));
    ASSERT_EQ(plan->kind(), "Project");
    const auto& proj = static_cast<const LogicalProject&>(*plan);
    ASSERT_EQ(proj.input().kind(), "Filter");
    const auto& f = static_cast<const LogicalFilter&>(proj.input());
    EXPECT_NE(f.predicate_json().find("\"op\":\"in\""), std::string::npos) << f.predicate_json();
    EXPECT_NE(f.predicate_json().find("\"col\":\"user_id\""), std::string::npos);
    EXPECT_NE(f.predicate_json().find("\"values\""), std::string::npos);
}

TEST(SqlBinder, NotInListWrapsInNot) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan =
        b.bind_select(as_select(parse("SELECT url FROM clicks WHERE user_id NOT IN (1, 2)")));
    const auto& proj = static_cast<const LogicalProject&>(*plan);
    const auto& f = static_cast<const LogicalFilter&>(proj.input());
    EXPECT_NE(f.predicate_json().find("\"op\":\"not\""), std::string::npos);
    EXPECT_NE(f.predicate_json().find("\"op\":\"in\""), std::string::npos);
}

// --- ORDER BY + LIMIT TOP-N ---------------------------------------

TEST(SqlBinder, OrderByLimitWrapsInLogicalTopN) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan =
        b.bind_select(as_select(parse("SELECT user_id FROM clicks ORDER BY user_id DESC LIMIT 5")));
    ASSERT_EQ(plan->kind(), "TopN");
    const auto& tn = static_cast<const LogicalTopN&>(*plan);
    ASSERT_EQ(tn.sort_columns().size(), 1u);
    EXPECT_EQ(tn.sort_columns()[0], "user_id");
    EXPECT_TRUE(tn.sort_descending()[0]);
    EXPECT_EQ(tn.count(), 5);
}

TEST(SqlBinder, MultiKeyMixedDirectionOrderBy) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(
        as_select(parse("SELECT user_id, url FROM clicks ORDER BY user_id ASC, url DESC LIMIT 3")));
    const auto& tn = static_cast<const LogicalTopN&>(*plan);
    ASSERT_EQ(tn.sort_columns().size(), 2u);
    EXPECT_EQ(tn.sort_columns()[0], "user_id");
    EXPECT_FALSE(tn.sort_descending()[0]);
    EXPECT_EQ(tn.sort_columns()[1], "url");
    EXPECT_TRUE(tn.sort_descending()[1]);
}

TEST(SqlBinder, OrderByWithoutLimitRejected) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    EXPECT_THROW(b.bind_select(as_select(parse("SELECT user_id FROM clicks ORDER BY user_id"))),
                 TranslationError);
}

TEST(SqlBinder, OrderByUnknownColumnRejected) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    EXPECT_THROW(
        b.bind_select(as_select(parse("SELECT user_id FROM clicks ORDER BY url DESC LIMIT 5"))),
        TranslationError);
}

// --- CTEs (WITH clause) -------------------------------------------

TEST(SqlBinder, SingleCteInlinesIntoOuterPlan) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(
        as_select(parse("WITH popular AS (SELECT url FROM clicks WHERE user_id > 100) "
                        "SELECT url FROM popular")));
    // Outer is a Project over the CTE's plan (Project over Filter
    // over Scan(clicks)). No Scan("popular") should appear: the CTE
    // plan replaced it inline.
    ASSERT_EQ(plan->kind(), "Project");
    const auto& outer = static_cast<const LogicalProject&>(*plan);
    EXPECT_EQ(outer.input().kind(), "Project");
    const auto& cte_proj = static_cast<const LogicalProject&>(outer.input());
    EXPECT_EQ(cte_proj.input().kind(), "Filter");
}

TEST(SqlBinder, ChainedCteSeesPriorCteInScope) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    // cte2 references cte1; binder must register cte1 before binding
    // cte2's body so the FROM cte1 resolves.
    auto plan =
        b.bind_select(as_select(parse("WITH a AS (SELECT url FROM clicks), "
                                      "     b AS (SELECT url FROM a) "
                                      "SELECT url FROM b")));
    ASSERT_EQ(plan->kind(), "Project");
}

TEST(SqlBinder, CteReusedMoreThanOnceRejected) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    EXPECT_THROW(b.bind_select(as_select(parse("WITH x AS (SELECT url FROM clicks) "
                                               "SELECT url FROM x UNION ALL SELECT url FROM x"))),
                 TranslationError);
}

TEST(SqlBinder, CteDefinedButUnreferencedIsAllowed) {
    // Match PG semantics: unreferenced CTEs are silently dropped.
    // Binder must not crash on the dangling plan when scope tears down.
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan =
        b.bind_select(as_select(parse("WITH unused AS (SELECT url FROM clicks) "
                                      "SELECT url FROM clicks")));
    ASSERT_NE(plan, nullptr);
}

TEST(SqlBinder, RecursiveCteRejected) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    EXPECT_THROW(b.bind_select(as_select(parse(
                     "WITH RECURSIVE r AS (SELECT user_id FROM clicks) SELECT user_id FROM r"))),
                 TranslationError);
}

TEST(SqlBinder, CteShadowsCatalogTableForOuterSelect) {
    // A CTE named after a real catalog table wins inside its scope.
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(
        as_select(parse("WITH clicks AS (SELECT user_id FROM clicks WHERE user_id > 100) "
                        "SELECT user_id FROM clicks")));
    ASSERT_EQ(plan->kind(), "Project");
    // The synthetic CTE def has one column (user_id), so the outer
    // schema reflects that.
    EXPECT_EQ(plan->schema()->num_fields(), 1);
    EXPECT_EQ(plan->schema()->field(0)->name(), "user_id");
}

// --- Extended built-in scalar functions ---------------------------

TEST(SqlBinder, SubstringLowersToSubstringOp) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan =
        b.bind_select(as_select(parse("SELECT SUBSTRING(url FROM 1 FOR 3) AS s FROM clicks")));
    const auto& project = static_cast<const LogicalProject&>(*plan);
    EXPECT_NE(project.outputs()[0].expr_json.find("\"op\":\"substring\""), std::string::npos);
    EXPECT_TRUE(project.outputs()[0].type->Equals(*arrow::utf8()));
}

TEST(SqlBinder, PositionLowersToPositionOpInt64) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(as_select(parse("SELECT POSITION('home' IN url) AS p FROM clicks")));
    const auto& project = static_cast<const LogicalProject&>(*plan);
    EXPECT_NE(project.outputs()[0].expr_json.find("\"op\":\"position\""), std::string::npos);
    EXPECT_TRUE(project.outputs()[0].type->Equals(*arrow::int64()));
}

TEST(SqlBinder, TrimVariantsLowerToLtrimRtrimBtrim) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan =
        b.bind_select(as_select(parse("SELECT TRIM(LEADING 'x' FROM url) AS l, "
                                      "       TRIM(TRAILING 'x' FROM url) AS r, "
                                      "       TRIM(url) AS b FROM clicks")));
    const auto& project = static_cast<const LogicalProject&>(*plan);
    EXPECT_NE(project.outputs()[0].expr_json.find("\"op\":\"ltrim\""), std::string::npos);
    EXPECT_NE(project.outputs()[1].expr_json.find("\"op\":\"rtrim\""), std::string::npos);
    EXPECT_NE(project.outputs()[2].expr_json.find("\"op\":\"btrim\""), std::string::npos);
}

TEST(SqlBinder, NumericFunctionsInferFloat64) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(
        as_select(parse("SELECT ABS(user_id) AS a, FLOOR(user_id) AS f, "
                        "       CEIL(user_id) AS c, ROUND(user_id, 2) AS r FROM clicks")));
    const auto& project = static_cast<const LogicalProject&>(*plan);
    for (const auto& out : project.outputs()) {
        EXPECT_TRUE(out.type->Equals(*arrow::float64())) << out.name;
    }
}

TEST(SqlBinder, NullIfLowersToNullifOp) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(as_select(parse("SELECT NULLIF(user_id, 0) AS n FROM clicks")));
    const auto& project = static_cast<const LogicalProject&>(*plan);
    EXPECT_NE(project.outputs()[0].expr_json.find("\"op\":\"nullif\""), std::string::npos);
    EXPECT_TRUE(project.outputs()[0].type->Equals(*arrow::int64()));
}

TEST(SqlBinder, GreatestLeastLowerToGreatestLeastOps) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(
        as_select(parse("SELECT GREATEST(user_id, 100) AS g, LEAST(user_id, 0) AS l FROM clicks")));
    const auto& project = static_cast<const LogicalProject&>(*plan);
    EXPECT_NE(project.outputs()[0].expr_json.find("\"op\":\"greatest\""), std::string::npos);
    EXPECT_NE(project.outputs()[1].expr_json.find("\"op\":\"least\""), std::string::npos);
    EXPECT_TRUE(project.outputs()[0].type->Equals(*arrow::int64()));
}

// --- UNION ALL ----------------------------------------------------

TEST(SqlBinder, UnionAllBuildsLogicalUnion) {
    Catalog cat;
    auto regs = parse(
        "CREATE TABLE a (id BIGINT, url TEXT) "
        "WITH (connector='file', format='json', path='/tmp/a.ndjson');"
        "CREATE TABLE b (id BIGINT, url TEXT) "
        "WITH (connector='file', format='json', path='/tmp/b.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[1]));
    Binder b(cat);
    auto plan =
        b.bind_select(as_select(parse("SELECT id, url FROM a UNION ALL SELECT id, url FROM b")));
    ASSERT_EQ(plan->kind(), "Union");
    const auto& un = static_cast<const LogicalUnion&>(*plan);
    EXPECT_EQ(un.left().kind(), "Project");
    EXPECT_EQ(un.right().kind(), "Project");
    ASSERT_EQ(plan->schema()->num_fields(), 2);
    EXPECT_EQ(plan->schema()->field(0)->name(), "id");
    EXPECT_TRUE(plan->schema()->field(0)->type()->Equals(*arrow::int64()));
}

namespace {
Catalog set_op_catalog() {
    Catalog cat;
    auto regs = parse(
        "CREATE TABLE a (id BIGINT) WITH (connector='file', format='json', path='/tmp/a');"
        "CREATE TABLE b (id BIGINT) WITH (connector='file', format='json', path='/tmp/b')");
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[1]));
    return cat;
}
}  // namespace

// UNION (no ALL) now binds to a distinct over the union.
TEST(SqlBinder, UnionDistinctBuildsDistinctOverUnion) {
    Catalog cat = set_op_catalog();
    Binder b(cat);
    auto plan = b.bind_select(as_select(parse("SELECT id FROM a UNION SELECT id FROM b")));
    ASSERT_EQ(plan->kind(), "Distinct");
    EXPECT_EQ(static_cast<const LogicalDistinct&>(*plan).input().kind(), "Union");
}

TEST(SqlBinder, IntersectBuildsSetOp) {
    Catalog cat = set_op_catalog();
    Binder b(cat);
    auto plan = b.bind_select(as_select(parse("SELECT id FROM a INTERSECT SELECT id FROM b")));
    ASSERT_EQ(plan->kind(), "SetOp");
    EXPECT_FALSE(static_cast<const LogicalSetOp&>(*plan).is_except());
}

TEST(SqlBinder, ExceptBuildsSetOp) {
    Catalog cat = set_op_catalog();
    Binder b(cat);
    auto plan = b.bind_select(as_select(parse("SELECT id FROM a EXCEPT SELECT id FROM b")));
    ASSERT_EQ(plan->kind(), "SetOp");
    EXPECT_TRUE(static_cast<const LogicalSetOp&>(*plan).is_except());
}

// The multiset ALL forms bind to a SetOp carrying is_all() = true.
TEST(SqlBinder, IntersectAllBuildsMultisetSetOp) {
    Catalog cat = set_op_catalog();
    Binder b(cat);
    auto plan = b.bind_select(as_select(parse("SELECT id FROM a INTERSECT ALL SELECT id FROM b")));
    ASSERT_EQ(plan->kind(), "SetOp");
    const auto& so = static_cast<const LogicalSetOp&>(*plan);
    EXPECT_FALSE(so.is_except());
    EXPECT_TRUE(so.is_all());
}

TEST(SqlBinder, ExceptAllBuildsMultisetSetOp) {
    Catalog cat = set_op_catalog();
    Binder b(cat);
    auto plan = b.bind_select(as_select(parse("SELECT id FROM a EXCEPT ALL SELECT id FROM b")));
    ASSERT_EQ(plan->kind(), "SetOp");
    const auto& so = static_cast<const LogicalSetOp&>(*plan);
    EXPECT_TRUE(so.is_except());
    EXPECT_TRUE(so.is_all());
}

TEST(SqlBinder, UnionAllColumnCountMismatchRejected) {
    Catalog cat;
    auto regs = parse(
        "CREATE TABLE a (id BIGINT, url TEXT) "
        "WITH (connector='file', format='json', path='/tmp/a.ndjson');"
        "CREATE TABLE b (id BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/b.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[1]));
    Binder b(cat);
    EXPECT_THROW(
        b.bind_select(as_select(parse("SELECT id, url FROM a UNION ALL SELECT id FROM b"))),
        TranslationError);
}

TEST(SqlBinder, UnionAllColumnTypeMismatchRejected) {
    Catalog cat;
    auto regs = parse(
        "CREATE TABLE a (id BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/a.ndjson');"
        "CREATE TABLE b (id TEXT) "
        "WITH (connector='file', format='json', path='/tmp/b.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[1]));
    Binder b(cat);
    EXPECT_THROW(b.bind_select(as_select(parse("SELECT id FROM a UNION ALL SELECT id FROM b"))),
                 TranslationError);
}

// --- CASE WHEN ----------------------------------------------------

TEST(SqlBinder, CaseWhenLowersToCaseValueExpression) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(as_select(
        parse("SELECT CASE WHEN user_id > 100 THEN 'big' ELSE 'small' END AS tier FROM clicks")));
    const auto& project = static_cast<const LogicalProject&>(*plan);
    ASSERT_EQ(project.outputs().size(), 1u);
    EXPECT_EQ(project.outputs()[0].name, "tier");
    const auto& expr = project.outputs()[0].expr_json;
    EXPECT_NE(expr.find("\"op\":\"case\""), std::string::npos) << expr;
    EXPECT_NE(expr.find("\"branches\""), std::string::npos) << expr;
    EXPECT_NE(expr.find("\"when\""), std::string::npos) << expr;
    EXPECT_NE(expr.find("\"then\""), std::string::npos) << expr;
    EXPECT_NE(expr.find("\"else\""), std::string::npos) << expr;
    EXPECT_NE(expr.find("\"lit\":\"big\""), std::string::npos) << expr;
    EXPECT_TRUE(project.outputs()[0].type->Equals(*arrow::utf8()));
}

TEST(SqlBinder, CaseWithoutElseElidesElseKey) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(
        as_select(parse("SELECT CASE WHEN user_id > 0 THEN 'pos' END AS tag FROM clicks")));
    const auto& project = static_cast<const LogicalProject&>(*plan);
    EXPECT_EQ(project.outputs()[0].expr_json.find("\"else\""), std::string::npos);
}

TEST(SqlBinder, CaseTypeMismatchAcrossThenBranchesRejected) {
    // First THEN returns utf8, second returns int64 -> error.
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    EXPECT_THROW(b.bind_select(as_select(parse("SELECT CASE WHEN user_id > 0 THEN 'pos' "
                                               "WHEN user_id < 0 THEN 1 END AS tag FROM clicks"))),
                 TranslationError);
}

TEST(SqlBinder, CaseElseTypeMismatchRejected) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    EXPECT_THROW(b.bind_select(as_select(parse(
                     "SELECT CASE WHEN user_id > 0 THEN 'pos' ELSE 0 END AS tag FROM clicks"))),
                 TranslationError);
}

TEST(SqlBinder, SimpleFormCaseRejected) {
    // CASE x WHEN 1 THEN ... is the simple form; we only support
    // searched-CASE.
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    EXPECT_THROW(b.bind_select(as_select(
                     parse("SELECT CASE user_id WHEN 1 THEN 'a' ELSE 'b' END AS x FROM clicks"))),
                 TranslationError);
}

TEST(SqlBinder, LimitWrapsPlanInLogicalLimit) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(as_select(parse("SELECT url FROM clicks LIMIT 5")));
    ASSERT_EQ(plan->kind(), "Limit");
    EXPECT_EQ(static_cast<const LogicalLimit&>(*plan).count(), 5);
}

TEST(SqlBinder, NegativeLimitRejected) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    EXPECT_THROW(b.bind_select(as_select(parse("SELECT url FROM clicks LIMIT -1"))),
                 TranslationError);
}

TEST(SqlBinder, SelectDistinctWrapsProjectionInDistinct) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(as_select(parse("SELECT DISTINCT url FROM clicks")));
    ASSERT_EQ(plan->kind(), "Distinct");
    EXPECT_EQ(static_cast<const LogicalDistinct&>(*plan).input().kind(), "Project");
}

TEST(SqlBinder, HavingClauseWrapsAggregateInFilter) {
    // HAVING on aggregate alias wraps the aggregate node in a
    // LogicalFilter. The filter's predicate references the aggregate's
    // emitted column name.
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(
        as_select(parse("SELECT url, COUNT(*) AS n FROM clicks GROUP BY url HAVING n > 10")));
    ASSERT_EQ(plan->kind(), "Filter");
    const auto& f = static_cast<const LogicalFilter&>(*plan);
    EXPECT_EQ(f.input().kind(), "Aggregate");
    EXPECT_NE(f.predicate_json().find("\"col\":\"n\""), std::string::npos);
    EXPECT_NE(f.predicate_json().find("\"op\":\"gt\""), std::string::npos);
}

TEST(SqlBinder, HavingClauseAllowsGroupColumnReference) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(
        as_select(parse("SELECT url, COUNT(*) AS n FROM clicks GROUP BY url HAVING url = 'home'")));
    ASSERT_EQ(plan->kind(), "Filter");
    const auto& f = static_cast<const LogicalFilter&>(*plan);
    EXPECT_NE(f.predicate_json().find("\"col\":\"url\""), std::string::npos);
}

TEST(SqlBinder, GroupByWithoutWindowTvfBuildsLogicalAggregate) {
    // GROUP BY without a window TVF is now supported and
    // lowers to LogicalAggregate (unbounded, upsert-style output).
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan =
        b.bind_select(as_select(parse("SELECT url, SUM(user_id) AS s FROM clicks GROUP BY url")));
    ASSERT_EQ(plan->kind(), "Aggregate");
    const auto& agg = static_cast<const LogicalAggregate&>(*plan);
    ASSERT_EQ(agg.group_keys().size(), 1u);
    EXPECT_EQ(agg.group_keys()[0], "url");
    ASSERT_EQ(agg.aggregates().size(), 1u);
    EXPECT_EQ(agg.aggregates()[0].agg_fn, "sum");
    EXPECT_EQ(agg.aggregates()[0].input_column, "user_id");
    EXPECT_EQ(agg.aggregates()[0].output_name, "s");
}

// --- Interval join ------------------------------------------------

TEST(SqlBinder, IntervalJoinBuildsLogicalIntervalJoin) {
    Catalog cat;
    auto regs = parse(
        "CREATE TABLE clicks (user_id BIGINT, click_ts BIGINT, page TEXT) "
        "WITH (connector='file', format='json', path='/tmp/clicks.ndjson', "
        "event_time_column='click_ts');"
        "CREATE TABLE impressions (user_id BIGINT, imp_ts BIGINT, ad TEXT) "
        "WITH (connector='file', format='json', path='/tmp/imps.ndjson', "
        "event_time_column='imp_ts')");
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[1]));

    Binder b(cat);
    auto plan =
        b.bind_select(as_select(parse("SELECT * "
                                      "FROM clicks c JOIN impressions i "
                                      "ON c.user_id = i.user_id "
                                      "AND c.click_ts BETWEEN i.imp_ts - INTERVAL '5' SECOND "
                                      "                   AND i.imp_ts + INTERVAL '10' SECOND")));
    const auto* jn = join_node(plan.get());
    ASSERT_NE(jn, nullptr);
    ASSERT_EQ(jn->kind(), "IntervalJoin");
    const auto& j = static_cast<const LogicalIntervalJoin&>(*jn);
    EXPECT_EQ(j.left_alias(), "c");
    EXPECT_EQ(j.right_alias(), "i");
    EXPECT_EQ(j.left_key_column(), "user_id");
    EXPECT_EQ(j.right_key_column(), "user_id");
    EXPECT_EQ(j.left_ts_column(), "click_ts");
    EXPECT_EQ(j.right_ts_column(), "imp_ts");
    EXPECT_EQ(j.lower_offset_ms(), -5000);
    EXPECT_EQ(j.upper_offset_ms(), 10000);
}

// Equi-join without an interval window is now supported.

TEST(SqlBinder, EquiJoinBuildsLogicalEquiJoin) {
    Catalog cat;
    auto regs = parse(
        "CREATE TABLE a (k BIGINT, v BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/a.ndjson');"
        "CREATE TABLE b (k BIGINT, w BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/b.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[1]));
    Binder b(cat);
    auto plan = b.bind_select(as_select(parse("SELECT * FROM a JOIN b ON a.k = b.k")));
    const auto* jn = join_node(plan.get());
    ASSERT_NE(jn, nullptr);
    ASSERT_EQ(jn->kind(), "EquiJoin");
    const auto& ej = static_cast<const LogicalEquiJoin&>(*jn);
    EXPECT_EQ(ej.left_alias(), "a");
    EXPECT_EQ(ej.right_alias(), "b");
    EXPECT_EQ(ej.left_key_column(), "k");
    EXPECT_EQ(ej.right_key_column(), "k");
    ASSERT_EQ(plan->schema()->num_fields(), 4);
    EXPECT_EQ(plan->schema()->field(0)->name(), "a_k");
    EXPECT_EQ(plan->schema()->field(2)->name(), "b_k");
    EXPECT_EQ(ej.join_type(), JoinType::Inner);
}

TEST(SqlBinder, DerivedWindowedAggregateAsJoinSideBinds) {
    Catalog cat;
    auto regs = parse(
        "CREATE TABLE a (k BIGINT, label BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/a.ndjson');"
        "CREATE TABLE b (k BIGINT, w BIGINT, ts BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/b.ndjson', event_time_column='ts')");
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[1]));
    Binder bd(cat);
    // A windowed aggregate (derived table M) as a join side, joined to base table a.
    auto plan = bd.bind_select(as_select(
        parse("SELECT a_label AS label, M_total AS total FROM a "
              "JOIN (SELECT k AS mk, SUM(w) AS total FROM b GROUP BY TUMBLE(ts, 1000), k) AS M "
              "ON a.k = M.mk")));
    const auto* jn = join_node(plan.get());
    ASSERT_NE(jn, nullptr);
    ASSERT_EQ(jn->kind(), "EquiJoin");
    const auto& ej = static_cast<const LogicalEquiJoin&>(*jn);
    EXPECT_EQ(ej.left_alias(), "a");   // base table
    EXPECT_EQ(ej.right_alias(), "m");  // derived windowed aggregate (alias lower-cased), base-like
    EXPECT_EQ(ej.left_key_column(), "k");
    EXPECT_EQ(ej.right_key_column(), "mk");
    EXPECT_EQ(ej.right().kind(), "WindowAggregate");  // the sub-plan is wired as the join child
}

TEST(SqlBinder, DerivedTableJoinAliasCollidingWithBaseTableRejected) {
    Catalog cat;
    auto regs = parse(
        "CREATE TABLE a (k BIGINT, v BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/a.ndjson');"
        "CREATE TABLE m (k BIGINT, w BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/m.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[1]));
    Binder bd(cat);
    // Aliasing the derived join side 'm' collides with base table 'm': registering
    // it in the cte overlay would shadow the base table, so reject at bind time.
    EXPECT_THROW(bd.bind_select(as_select(
                     parse("SELECT * FROM (SELECT k, SUM(v) AS total FROM a GROUP BY k) AS m "
                           "JOIN a ON m.k = a.k"))),
                 TranslationError);
}

TEST(SqlBinder, OuterEquiJoinsCarryJoinType) {
    Catalog cat;
    auto regs = parse(
        "CREATE TABLE a (k BIGINT, v BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/a.ndjson');"
        "CREATE TABLE b (k BIGINT, w BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/b.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[1]));
    Binder b(cat);
    auto kind_of = [&](const char* sql) {
        auto plan = b.bind_select(as_select(parse(sql)));
        const auto* jn = join_node(plan.get());
        return static_cast<const LogicalEquiJoin&>(*jn).join_type();
    };
    EXPECT_EQ(kind_of("SELECT * FROM a LEFT JOIN b ON a.k = b.k"), JoinType::LeftOuter);
    EXPECT_EQ(kind_of("SELECT * FROM a RIGHT JOIN b ON a.k = b.k"), JoinType::RightOuter);
    EXPECT_EQ(kind_of("SELECT * FROM a FULL JOIN b ON a.k = b.k"), JoinType::FullOuter);
}

TEST(SqlBinder, EquiJoinAcceptsReversedColumnOrder) {
    Catalog cat;
    auto regs = parse(
        "CREATE TABLE a (k BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/a.ndjson');"
        "CREATE TABLE b (k BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/b.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[1]));
    Binder b(cat);
    // b.k = a.k must still bind as an equi-join with the alias roles
    // swapped to match FROM-clause order.
    auto plan = b.bind_select(as_select(parse("SELECT * FROM a JOIN b ON b.k = a.k")));
    const auto* jn = join_node(plan.get());
    ASSERT_NE(jn, nullptr);
    EXPECT_EQ(jn->kind(), "EquiJoin");
}

TEST(SqlBinder, IntervalJoinShapeStillTakesIntervalPath) {
    // Regression: with the equi-join recogniser added, the eq+BETWEEN
    // shape must still land on LogicalIntervalJoin, not LogicalEquiJoin.
    Catalog cat;
    auto regs = parse(
        "CREATE TABLE c (k BIGINT, ts BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/c.ndjson', "
        "event_time_column='ts');"
        "CREATE TABLE d (k BIGINT, ts BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/d.ndjson', "
        "event_time_column='ts')");
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[1]));
    Binder b(cat);
    auto plan = b.bind_select(as_select(
        parse("SELECT * FROM c JOIN d ON c.k = d.k AND c.ts BETWEEN d.ts - 50 AND d.ts + 200")));
    const auto* jn = join_node(plan.get());
    ASSERT_NE(jn, nullptr);
    EXPECT_EQ(jn->kind(), "IntervalJoin");
}

TEST(SqlBinder, NonEqualityJoinPredicateRejected) {
    Catalog cat;
    auto regs = parse(
        "CREATE TABLE a (k BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/a.ndjson');"
        "CREATE TABLE b (k BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/b.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[1]));
    Binder b(cat);
    EXPECT_THROW(b.bind_select(as_select(parse("SELECT * FROM a JOIN b ON a.k > b.k"))),
                 TranslationError);
}

TEST(SqlBinder, EquiJoinSameAliasOnBothSidesRejected) {
    Catalog cat;
    auto regs = parse(
        "CREATE TABLE a (k BIGINT, k2 BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/a.ndjson');"
        "CREATE TABLE b (k BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/b.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[1]));
    Binder b(cat);
    // a.k = a.k2 doesn't mention `b` so it can't be a stream-stream
    // join; the recogniser rejects with the generic JOIN-ON message.
    EXPECT_THROW(b.bind_select(as_select(parse("SELECT * FROM a JOIN b ON a.k = a.k2"))),
                 TranslationError);
}

// OUTER equi-joins bind (see OuterEquiJoinsCarryJoinType), and as of SQLOPT-1
// OUTER over the *interval* (BETWEEN) shape binds too.
// SQLOPT-1: OUTER interval joins now bind (previously rejected). The runtime
// null-pads unmatched rows on the kept side at watermark eviction.
TEST(SqlBinder, OuterIntervalJoinBinds) {
    Catalog cat;
    auto regs = parse(
        "CREATE TABLE c (k BIGINT, ts BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/c.ndjson', "
        "event_time_column='ts');"
        "CREATE TABLE d (k BIGINT, ts BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/d.ndjson', "
        "event_time_column='ts')");
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[1]));
    Binder b(cat);
    auto plan =
        b.bind_select(as_select(parse("SELECT * FROM c LEFT JOIN d ON c.k = d.k "
                                      "AND c.ts BETWEEN d.ts - 50 AND d.ts + 200")));
    const auto* jn = join_node(plan.get());
    ASSERT_NE(jn, nullptr);
    ASSERT_EQ(jn->kind(), "IntervalJoin");
    const auto& ij = static_cast<const LogicalIntervalJoin&>(*jn);
    EXPECT_EQ(ij.join_type(), JoinType::LeftOuter);

    // FULL OUTER binds too.
    auto full =
        b.bind_select(as_select(parse("SELECT * FROM c FULL OUTER JOIN d ON c.k = d.k "
                                      "AND c.ts BETWEEN d.ts - 50 AND d.ts + 200")));
    const auto* full_jn = join_node(full.get());
    ASSERT_NE(full_jn, nullptr);
    ASSERT_EQ(full_jn->kind(), "IntervalJoin");
    EXPECT_EQ(static_cast<const LogicalIntervalJoin&>(*full_jn).join_type(), JoinType::FullOuter);
}

// --- Lookup (enrichment) join ---------------------------------------

namespace {
Catalog lookup_join_catalog() {
    Catalog cat;
    auto regs = parse(
        "CREATE TABLE orders (cust BIGINT, amt BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/o.ndjson');"
        "CREATE TABLE customers (id BIGINT, name TEXT) "
        "WITH (connector='lookup', function='cust_lookup');"
        "CREATE TABLE nofn (id BIGINT) WITH (connector='lookup')");
    for (const auto& st : regs.statements) {
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    }
    return cat;
}
}  // namespace

TEST(SqlBinder, LookupJoinBuildsLogicalLookupJoin) {
    Catalog cat = lookup_join_catalog();
    Binder b(cat);
    auto plan =
        b.bind_select(as_select(parse("SELECT * FROM orders o JOIN customers c ON o.cust = c.id")));
    const auto* jn = join_node(plan.get());
    ASSERT_NE(jn, nullptr);
    ASSERT_EQ(jn->kind(), "LookupJoin");
    const auto& lj = static_cast<const LogicalLookupJoin&>(*jn);
    EXPECT_EQ(lj.function_name(), "cust_lookup");
    EXPECT_EQ(lj.probe_alias(), "o");
    EXPECT_EQ(lj.dim_alias(), "c");
    EXPECT_FALSE(lj.outer());
    EXPECT_EQ(lj.dim_key_column(), "id");
    EXPECT_EQ(lj.probe_columns(), (std::vector<std::string>{"cust", "amt"}));
    EXPECT_EQ(lj.dim_columns(), (std::vector<std::string>{"id", "name"}));
    // Output schema mirrors a normal join: aliased probe then dim cols.
    auto sch = lj.schema();
    ASSERT_EQ(sch->num_fields(), 4);
    EXPECT_EQ(sch->field(0)->name(), "o_cust");
    EXPECT_EQ(sch->field(1)->name(), "o_amt");
    EXPECT_EQ(sch->field(2)->name(), "c_id");
    EXPECT_EQ(sch->field(3)->name(), "c_name");
}

TEST(SqlBinder, LookupJoinLeftOuterIsOuter) {
    Catalog cat = lookup_join_catalog();
    Binder b(cat);
    auto plan = b.bind_select(
        as_select(parse("SELECT * FROM orders o LEFT JOIN customers c ON o.cust = c.id")));
    const auto* jn = join_node(plan.get());
    ASSERT_NE(jn, nullptr);
    ASSERT_EQ(jn->kind(), "LookupJoin");
    EXPECT_TRUE(static_cast<const LogicalLookupJoin&>(*jn).outer());
}

// The synthetic join derived table uses the reserved alias "__join". A user CTE
// of that name in the same statement must collide loudly (like the subquery /
// MATCH_RECOGNIZE / PTF synthetic-derived paths), not silently clobber the CTE.
TEST(SqlBinder, JoinReservedAliasCollidesWithUserCte) {
    Catalog cat;
    auto regs = parse(
        "CREATE TABLE a (k BIGINT, v BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/a.ndjson');"
        "CREATE TABLE b (k BIGINT, w BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/b.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[1]));
    Binder b(cat);
    EXPECT_THROW(b.bind_select(as_select(parse(
                     "WITH __join AS (SELECT k FROM a) SELECT * FROM a JOIN b ON a.k = b.k"))),
                 TranslationError);
}

// The synthetic "__join" entry must not leak across statements on a reused
// Binder (the driver binds a whole script with one Binder). After binding a
// join, a later statement that references "__join" must see an unknown table,
// and a second join must bind cleanly.
TEST(SqlBinder, JoinSyntheticAliasDoesNotLeakAcrossBinds) {
    Catalog cat;
    auto regs = parse(
        "CREATE TABLE a (k BIGINT, v BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/a.ndjson');"
        "CREATE TABLE b (k BIGINT, w BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/b.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[0]));
    cat.register_table(std::get<ast::CreateTableStmt>(regs.statements[1]));
    Binder b(cat);
    auto p1 = b.bind_select(as_select(parse("SELECT * FROM a JOIN b ON a.k = b.k")));
    ASSERT_NE(join_node(p1.get()), nullptr);
    // __join was scratch for statement 1; it must be gone now.
    EXPECT_THROW(b.bind_select(as_select(parse("SELECT * FROM __join"))), TranslationError);
    // A second join binds cleanly (no stale-alias collision).
    auto p2 = b.bind_select(as_select(parse("SELECT * FROM a JOIN b ON a.k = b.k")));
    EXPECT_NE(join_node(p2.get()), nullptr);
}

TEST(SqlBinder, LookupJoinRejections) {
    Catalog cat = lookup_join_catalog();
    Binder b(cat);
    // Lookup table must be on the right (probe on the left).
    EXPECT_THROW(
        b.bind_select(as_select(parse("SELECT * FROM customers c JOIN orders o ON c.id = o.cust"))),
        TranslationError);
    // RIGHT / FULL would require enumerating the dim source.
    EXPECT_THROW(b.bind_select(as_select(
                     parse("SELECT * FROM orders o RIGHT JOIN customers c ON o.cust = c.id"))),
                 TranslationError);
    EXPECT_THROW(b.bind_select(as_select(
                     parse("SELECT * FROM orders o FULL JOIN customers c ON o.cust = c.id"))),
                 TranslationError);
    // A lookup table is not a stream: invalid as a plain FROM source.
    EXPECT_THROW(b.bind_select(as_select(parse("SELECT * FROM customers"))), TranslationError);
    // connector='lookup' without a function= property.
    EXPECT_THROW(
        b.bind_select(as_select(parse("SELECT * FROM orders o JOIN nofn n ON o.cust = n.id"))),
        TranslationError);
}

TEST(SqlBinder, EmptyFromRejected) {
    Catalog cat;
    Binder b(cat);
    EXPECT_THROW(b.bind_select(as_select(parse("SELECT 1"))), TranslationError);
}

// --- WHERE clause -------------------------------------------------

TEST(SqlBinder, WhereWrapsScanInLogicalFilter) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan =
        b.bind_select(as_select(parse("SELECT url FROM clicks WHERE url = 'http://example.com'")));
    // Project -> Filter -> Scan
    const auto& project = static_cast<const LogicalProject&>(*plan);
    EXPECT_EQ(project.input().kind(), "Filter");
    const auto& filter = static_cast<const LogicalFilter&>(project.input());
    EXPECT_EQ(filter.input().kind(), "Scan");
    EXPECT_NE(filter.predicate_json().find("\"op\":\"eq\""), std::string::npos);
    EXPECT_NE(filter.predicate_json().find("\"col\":\"url\""), std::string::npos);
    EXPECT_NE(filter.predicate_json().find("\"literal\":\"http://example.com\""),
              std::string::npos);
}

TEST(SqlBinder, WhereAndOrNotLowerToJsonPredicate) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan = b.bind_select(as_select(
        parse("SELECT url FROM clicks WHERE url = 'a' AND url != 'b' OR NOT (url = 'c')")));
    const auto& project = static_cast<const LogicalProject&>(*plan);
    const auto& filter = static_cast<const LogicalFilter&>(project.input());
    EXPECT_NE(filter.predicate_json().find("\"op\":\"or\""), std::string::npos);
    EXPECT_NE(filter.predicate_json().find("\"op\":\"and\""), std::string::npos);
    EXPECT_NE(filter.predicate_json().find("\"op\":\"not\""), std::string::npos);
    EXPECT_NE(filter.predicate_json().find("\"op\":\"ne\""), std::string::npos);
}

TEST(SqlBinder, WhereIsNullAndIsNotNullLower) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    {
        auto plan = b.bind_select(as_select(parse("SELECT url FROM clicks WHERE url IS NULL")));
        const auto& filter =
            static_cast<const LogicalFilter&>(static_cast<const LogicalProject&>(*plan).input());
        EXPECT_NE(filter.predicate_json().find("\"op\":\"is_null\""), std::string::npos);
    }
    {
        auto plan = b.bind_select(as_select(parse("SELECT url FROM clicks WHERE url IS NOT NULL")));
        const auto& filter =
            static_cast<const LogicalFilter&>(static_cast<const LogicalProject&>(*plan).input());
        EXPECT_NE(filter.predicate_json().find("\"op\":\"is_not_null\""), std::string::npos);
    }
}

TEST(SqlBinder, WhereLikeLowersToLikePredicate) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan =
        b.bind_select(as_select(parse("SELECT url FROM clicks WHERE url LIKE '%checkout%'")));
    const auto& filter =
        static_cast<const LogicalFilter&>(static_cast<const LogicalProject&>(*plan).input());
    EXPECT_NE(filter.predicate_json().find("\"op\":\"like\""), std::string::npos);
    EXPECT_NE(filter.predicate_json().find("\"pattern\":\"%checkout%\""), std::string::npos);
    EXPECT_NE(filter.predicate_json().find("\"col\":\"url\""), std::string::npos);
}

TEST(SqlBinder, WhereNotLikeWrapsInNot) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    auto plan =
        b.bind_select(as_select(parse("SELECT url FROM clicks WHERE url NOT LIKE '%error%'")));
    const auto& filter =
        static_cast<const LogicalFilter&>(static_cast<const LogicalProject&>(*plan).input());
    EXPECT_NE(filter.predicate_json().find("\"op\":\"not\""), std::string::npos);
    EXPECT_NE(filter.predicate_json().find("\"op\":\"like\""), std::string::npos);
    EXPECT_NE(filter.predicate_json().find("\"pattern\":\"%error%\""), std::string::npos);
}

TEST(SqlBinder, WhereOnUnknownColumnRejected) {
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    EXPECT_THROW(b.bind_select(as_select(parse("SELECT url FROM clicks WHERE nope = 'x'"))),
                 TranslationError);
}

TEST(SqlBinder, WhereLiteralOnLeftSideRejected) {
    // The binder expects column on the left, literal on the right.
    Catalog cat;
    register_clicks(cat);
    Binder b(cat);
    EXPECT_THROW(b.bind_select(as_select(parse("SELECT url FROM clicks WHERE 'x' = url"))),
                 TranslationError);
}

// ---------------------------------------------------------------------------
// CREATE VIEW: a logical view is expanded inline at each reference.
// ---------------------------------------------------------------------------
namespace {

// The leaf base-table scan reached by walking the primary input chain.
const LogicalScan* deepest_scan(const LogicalPlan* p) {
    while (p != nullptr) {
        if (p->kind() == "Scan") {
            return static_cast<const LogicalScan*>(p);
        }
        auto ins = p->inputs();
        if (ins.empty()) {
            return nullptr;
        }
        p = ins[0];
    }
    return nullptr;
}

// Does any node on the primary input chain have this kind?
bool chain_contains(const LogicalPlan* p, const std::string& kind) {
    while (p != nullptr) {
        if (p->kind() == kind) {
            return true;
        }
        auto ins = p->inputs();
        if (ins.empty()) {
            return false;
        }
        p = ins[0];
    }
    return false;
}

void make_view(Catalog& cat, const char* sql) {
    register_view(cat, std::move(std::get<ast::CreateViewStmt>(parse(sql).statements[0])));
}

void rename_via(Catalog& cat, const char* sql) {
    rename_object(cat, std::get<ast::RenameStmt>(parse(sql).statements[0]));
}

bool view_binds(const Catalog& cat, const char* name) {
    const ast::SelectStmt* q = cat.get_view_query(name);
    if (q == nullptr) {
        return false;
    }
    try {
        Binder b(cat);
        (void)b.bind_select(*q);
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

}  // namespace

TEST(SqlBinder, ViewExpandsToBaseTableAtReference) {
    Catalog cat;
    register_clicks(cat);
    make_view(cat, "CREATE VIEW v AS SELECT user_id, url FROM clicks");
    // The view's TableDef carries the inferred output columns.
    const TableDef* def = cat.get_table("v");
    ASSERT_NE(def, nullptr);
    EXPECT_TRUE(def->is_logical_view());
    ASSERT_EQ(def->columns.size(), 2u);
    EXPECT_EQ(def->columns[0].name, "user_id");
    EXPECT_EQ(def->columns[1].name, "url");

    // A reference expands inline: the leaf scan is the base table `clicks`,
    // never a scan of `v` (a view has no storage).
    Binder b(cat);
    auto plan = b.bind_select(as_select(parse("SELECT url FROM v")));
    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->kind(), "Project");
    const LogicalScan* scan = deepest_scan(plan.get());
    ASSERT_NE(scan, nullptr);
    EXPECT_EQ(scan->table().name, "clicks");
}

TEST(SqlBinder, ViewColumnAliasRenamesOutputColumns) {
    Catalog cat;
    register_clicks(cat);
    make_view(cat, "CREATE VIEW v (clk, link) AS SELECT user_id, url FROM clicks");
    // The alias list becomes the view's declared output column names.
    const TableDef* def = cat.get_table("v");
    ASSERT_NE(def, nullptr);
    ASSERT_EQ(def->columns.size(), 2u);
    EXPECT_EQ(def->columns[0].name, "clk");
    EXPECT_EQ(def->columns[1].name, "link");

    // A reference resolves the aliased names, and the expanded plan exposes them
    // on its output schema (the thin renaming projection over the base query).
    Binder b(cat);
    auto plan = b.bind_select(as_select(parse("SELECT clk, link FROM v")));
    ASSERT_NE(plan, nullptr);
    auto schema = plan->schema();
    ASSERT_NE(schema, nullptr);
    ASSERT_EQ(schema->num_fields(), 2);
    EXPECT_EQ(schema->field(0)->name(), "clk");
    EXPECT_EQ(schema->field(1)->name(), "link");
    EXPECT_EQ(deepest_scan(plan.get())->table().name, "clicks");

    // The underlying query's own names no longer resolve through the view.
    Binder b2(cat);
    EXPECT_THROW(b2.bind_select(as_select(parse("SELECT user_id FROM v"))), TranslationError);
}

TEST(SqlBinder, RegisterViewRejectsAliasArityMismatch) {
    Catalog cat;
    register_clicks(cat);
    EXPECT_THROW(make_view(cat, "CREATE VIEW v (a, b, c) AS SELECT user_id, url FROM clicks"),
                 TranslationError);
}

TEST(SqlBinder, RegisterViewRejectsDuplicateAlias) {
    Catalog cat;
    register_clicks(cat);
    EXPECT_THROW(make_view(cat, "CREATE VIEW v (dup, dup) AS SELECT user_id, url FROM clicks"),
                 TranslationError);
}

// rename_object rejects (and rolls back) a table rename that would break a view
// referencing the table by name.
TEST(SqlBinder, RenameTableRejectedWhenDependentViewBreaks) {
    Catalog cat;
    register_clicks(cat);
    make_view(cat, "CREATE VIEW v AS SELECT user_id FROM clicks");
    EXPECT_THROW(rename_via(cat, "ALTER TABLE clicks RENAME TO hits"), TranslationError);
    // Rolled back: the original name stays, the new name is absent, view still binds.
    EXPECT_NE(cat.get_table("clicks"), nullptr);
    EXPECT_EQ(cat.get_table("hits"), nullptr);
    EXPECT_TRUE(view_binds(cat, "v"));
}

// A column rename that the view depends on is likewise rejected and rolled back.
TEST(SqlBinder, RenameColumnRejectedWhenDependentViewBreaks) {
    Catalog cat;
    register_clicks(cat);
    make_view(cat, "CREATE VIEW v AS SELECT user_id FROM clicks");
    EXPECT_THROW(rename_via(cat, "ALTER TABLE clicks RENAME COLUMN user_id TO uid"),
                 TranslationError);
    // Rolled back: the column keeps its name and the view still binds.
    const TableDef* def = cat.get_table("clicks");
    ASSERT_NE(def, nullptr);
    EXPECT_EQ(def->columns[0].name, "user_id");
    EXPECT_TRUE(view_binds(cat, "v"));
}

// A rename that no view depends on goes through: a table no view references, and
// a column the view does not select.
TEST(SqlBinder, RenameAllowedWhenNoDependentViewBreaks) {
    Catalog cat;
    register_clicks(cat);
    make_view(cat, "CREATE VIEW v AS SELECT user_id FROM clicks");

    // `url` is not used by the view, so renaming it succeeds.
    EXPECT_NO_THROW(rename_via(cat, "ALTER TABLE clicks RENAME COLUMN url TO link"));
    EXPECT_EQ(cat.get_table("clicks")->columns[2].name, "link");
    EXPECT_TRUE(view_binds(cat, "v"));

    // A second, unreferenced table renames freely even with a view present.
    auto s =
        parse("CREATE TABLE other (k BIGINT) WITH (connector='kafka', topic='o', bootstrap='h:1')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
    EXPECT_NO_THROW(rename_via(cat, "ALTER TABLE other RENAME TO other2"));
    EXPECT_NE(cat.get_table("other2"), nullptr);
    EXPECT_EQ(cat.get_table("other"), nullptr);
}

TEST(SqlBinder, ViewPreservesItsOwnFilter) {
    Catalog cat;
    register_clicks(cat);
    make_view(cat, "CREATE VIEW big AS SELECT user_id, url FROM clicks WHERE user_id > 100");
    Binder b(cat);
    auto plan = b.bind_select(as_select(parse("SELECT url FROM big")));
    // The view's WHERE survives expansion: a Filter is on the chain over clicks.
    EXPECT_TRUE(chain_contains(plan.get(), "Filter"));
    EXPECT_EQ(deepest_scan(plan.get())->table().name, "clicks");
}

TEST(SqlBinder, ViewColumnResolutionRejectsUnknownColumn) {
    Catalog cat;
    register_clicks(cat);
    make_view(cat, "CREATE VIEW v AS SELECT user_id FROM clicks");
    Binder b(cat);
    // `url` is not exposed by the view (only user_id is).
    EXPECT_THROW(b.bind_select(as_select(parse("SELECT url FROM v"))), TranslationError);
}

TEST(SqlBinder, NestedViewExpandsTransitively) {
    Catalog cat;
    register_clicks(cat);
    make_view(cat, "CREATE VIEW v1 AS SELECT user_id, url FROM clicks");
    make_view(cat, "CREATE VIEW v2 AS SELECT user_id FROM v1");
    Binder b(cat);
    auto plan = b.bind_select(as_select(parse("SELECT user_id FROM v2")));
    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(deepest_scan(plan.get())->table().name, "clicks");
}

TEST(SqlBinder, CreateViewRejectsDuplicateWithoutOrReplace) {
    Catalog cat;
    register_clicks(cat);
    make_view(cat, "CREATE VIEW v AS SELECT user_id FROM clicks");
    EXPECT_THROW(make_view(cat, "CREATE VIEW v AS SELECT url FROM clicks"), TranslationError);
    // OR REPLACE updates the definition (and the exposed columns).
    EXPECT_NO_THROW(make_view(cat, "CREATE OR REPLACE VIEW v AS SELECT url FROM clicks"));
    const TableDef* def = cat.get_table("v");
    ASSERT_NE(def, nullptr);
    ASSERT_EQ(def->columns.size(), 1u);
    EXPECT_EQ(def->columns[0].name, "url");
}

TEST(SqlBinder, CreateViewRejectsNameCollisionWithTable) {
    Catalog cat;
    register_clicks(cat);
    // `clicks` is a base table, not a view - even shape-compatible, reject.
    EXPECT_THROW(make_view(cat, "CREATE VIEW clicks AS SELECT user_id FROM clicks"),
                 TranslationError);
}

// A view can be referenced more than once in a query (unlike a CTE, which is
// at-most-once here): each reference re-binds the defining query independently.
TEST(SqlBinder, ViewReferencedTwiceExpandsEachTime) {
    Catalog cat;
    register_clicks(cat);
    make_view(cat, "CREATE VIEW v AS SELECT user_id, url FROM clicks");
    Binder b(cat);
    // Two references in a UNION ALL: both expand (no at-most-once CTE error).
    auto plan = b.bind_select(as_select(parse("SELECT url FROM v UNION ALL SELECT url FROM v")));
    ASSERT_NE(plan, nullptr);
}

// A view on a join side expands to its defining query, and the join resolves
// its key against the view's exposed columns (the view name is a valid
// qualifier in the ON clause, exactly as a base-table name would be).
TEST(SqlBinder, ViewUsedAsJoinSide) {
    Catalog cat;
    register_clicks(cat);  // user_id, ts, url
    register_events(cat);  // user_id, ts, amount
    make_view(cat, "CREATE VIEW vclicks AS SELECT user_id, url FROM clicks");
    Binder b(cat);
    auto plan = b.bind_select(
        as_select(parse("SELECT * FROM vclicks JOIN evt ON vclicks.user_id = evt.user_id")));
    ASSERT_NE(plan, nullptr);
    EXPECT_NE(join_node(plan.get()), nullptr) << "the view-vs-table join must bind as a join";
}

// A reference cycle (introduced via OR REPLACE: v1 binds against the prior
// definition, so creation succeeds) is caught by the expanding-views guard the
// moment a query tries to expand it.
TEST(SqlBinder, CyclicViewReferenceRejectedAtBind) {
    Catalog cat;
    register_clicks(cat);
    make_view(cat, "CREATE VIEW v1 AS SELECT user_id FROM clicks");
    make_view(cat, "CREATE VIEW v2 AS SELECT user_id FROM v1");
    // Redefine v1 to reference v2 -> catalog now has v1 -> v2 -> v1.
    make_view(cat, "CREATE OR REPLACE VIEW v1 AS SELECT user_id FROM v2");
    Binder b(cat);
    EXPECT_THROW(b.bind_select(as_select(parse("SELECT user_id FROM v1"))), TranslationError);
}

}  // namespace clink::sql
