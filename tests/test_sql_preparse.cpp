// #61: pre-parser shim + composite DDL types (MAP/ROW/MULTISET). Covers the
// scanner (island detection, string/comment skipping, false-positive guards),
// the composite-type parser, full parse() -> Arrow type resolution, the catalog
// round-trip, and sink-compatibility for composite columns.

#include <filesystem>
#include <memory>
#include <variant>

#include <arrow/type.h>
#include <gtest/gtest.h>

#include "clink/operators/process_function.hpp"
#include "clink/sql/binder.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/logical_plan.hpp"
#include "clink/sql/materialized_view.hpp"
#include "clink/sql/optimizer.hpp"
#include "clink/sql/parser.hpp"
#include "clink/sql/physical_plan.hpp"
#include "clink/sql/preparse.hpp"
#include "clink/sql/ptf_registry.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/type.hpp"

using namespace clink;
using clink::sql::Binder;
using clink::sql::Catalog;
using clink::sql::parse;
using clink::sql::sql_type_to_arrow;
using clink::sql::TranslationError;
namespace pp = clink::sql::preparse;
namespace ast = clink::sql::ast;

namespace {

// Resolve the Arrow type of the single column of "CREATE TABLE t (c <type>)".
std::shared_ptr<arrow::DataType> col_type(const char* type_sql) {
    auto script = parse(std::string{"CREATE TABLE t (c "} + type_sql + ")");
    const auto& create = std::get<ast::CreateTableStmt>(script.statements[0]);
    return sql_type_to_arrow(create.columns[0].type);
}

}  // namespace

// ---- scanner: islands are only rewritten inside CREATE TABLE column lists ----

TEST(SqlPreparse, NonDdlLessThanUntouched) {
    // `a < b` and `map < 3` outside a CREATE TABLE must not be mistaken for
    // type brackets.
    auto r = pp::preparse("SELECT a FROM t WHERE a < b AND map < 3");
    EXPECT_EQ(r.rewritten_sql, "SELECT a FROM t WHERE a < b AND map < 3");
    EXPECT_TRUE(r.composite_types.empty());
}

TEST(SqlPreparse, StringAndCommentAreSkipped) {
    // A MAP<...> inside a string literal or comment must not be rewritten.
    auto r = pp::preparse("CREATE TABLE t (c TEXT /* MAP<a,b> */, d VARCHAR)");
    EXPECT_EQ(r.rewritten_sql.find("__clink_ctype_"), std::string::npos);
    EXPECT_TRUE(r.composite_types.empty());

    auto r2 = pp::preparse("INSERT INTO t VALUES ('MAP<x,y>')");
    EXPECT_EQ(r2.rewritten_sql, "INSERT INTO t VALUES ('MAP<x,y>')");
}

TEST(SqlPreparse, MapIslandRewrittenInDdl) {
    auto r = pp::preparse("CREATE TABLE t (m MAP<TEXT,BIGINT>)");
    EXPECT_NE(r.rewritten_sql.find("__clink_ctype_0"), std::string::npos);
    EXPECT_EQ(r.rewritten_sql.find("MAP<"), std::string::npos);
    ASSERT_EQ(r.composite_types.size(), 1U);
    EXPECT_EQ(r.composite_types[0].name, "map");
}

// ---- composite-type parser ----

TEST(SqlPreparse, ParseMap) {
    auto t = pp::parse_composite_type("MAP<TEXT, BIGINT>");
    EXPECT_EQ(t.name, "map");
    ASSERT_EQ(t.params.size(), 2U);
    EXPECT_TRUE(sql_type_to_arrow(t)->Equals(*arrow::map(arrow::utf8(), arrow::int64())));
}

TEST(SqlPreparse, ParseRow) {
    auto t = pp::parse_composite_type("ROW<a INT, b TEXT>");
    EXPECT_EQ(t.name, "row");
    ASSERT_EQ(t.field_names.size(), 2U);
    EXPECT_EQ(t.field_names[0], "a");
    auto expected =
        arrow::struct_({arrow::field("a", arrow::int32()), arrow::field("b", arrow::utf8())});
    EXPECT_TRUE(sql_type_to_arrow(t)->Equals(*expected));
}

TEST(SqlPreparse, ParseMultisetAndNesting) {
    EXPECT_TRUE(col_type("MULTISET<INT>")->Equals(*arrow::list(arrow::int32())));
    // nested MAP<TEXT, ARRAY<INT>>
    EXPECT_TRUE(col_type("MAP<TEXT, ARRAY<INT>>")
                    ->Equals(*arrow::map(arrow::utf8(), arrow::list(arrow::int32()))));
    // ROW containing a MAP
    auto rt = col_type("ROW<id BIGINT, attrs MAP<TEXT,TEXT>>");
    auto expected =
        arrow::struct_({arrow::field("id", arrow::int64()),
                        arrow::field("attrs", arrow::map(arrow::utf8(), arrow::utf8()))});
    EXPECT_TRUE(rt->Equals(*expected));
}

TEST(SqlPreparse, CompositeArraySuffix) {
    // MAP<..>[] -> list of map (placeholder carries the trailing dimension).
    EXPECT_TRUE(col_type("MAP<TEXT,BIGINT>[]")
                    ->Equals(*arrow::list(arrow::map(arrow::utf8(), arrow::int64()))));
}

// ---- catalog round-trip ----

TEST(SqlPreparse, CatalogRoundTripsCompositeColumns) {
    Catalog cat;
    auto ddl = parse(
        "CREATE TABLE t (id BIGINT, tags MULTISET<TEXT>, attrs MAP<TEXT,BIGINT>, "
        "addr ROW<city TEXT, zip INT>) WITH (connector='file', format='json', "
        "path='/tmp/x.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));

    const auto* td = cat.get_table("t");
    ASSERT_NE(td, nullptr);
    auto json = Catalog::to_json(*td);
    auto td2 = Catalog::from_json(json);
    ASSERT_EQ(td->columns.size(), td2.columns.size());
    for (std::size_t i = 0; i < td->columns.size(); ++i) {
        EXPECT_TRUE(td->columns[i].type->Equals(*td2.columns[i].type))
            << "column " << td->columns[i].name << " differs after round-trip";
    }
}

// ---- sink compatibility: composite source -> composite sink ----

TEST(SqlPreparse, MatchingCompositeSinkBinds) {
    Catalog cat;
    auto ddl = parse(
        "CREATE TABLE src (k TEXT, v BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/in.ndjson');"
        "CREATE TABLE out (m MAP<TEXT,BIGINT>) "
        "WITH (connector='file', format='json', path='/tmp/out.ndjson')");
    for (const auto& st : ddl.statements) {
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    }
    Binder b(cat);
    auto insert = parse("INSERT INTO out SELECT MAP(k, v) AS m FROM src");
    EXPECT_NO_THROW((void)b.bind_insert(std::get<ast::InsertStmt>(insert.statements[0])));
}

TEST(SqlPreparse, MismatchedCompositeSinkRejected) {
    Catalog cat;
    auto ddl = parse(
        "CREATE TABLE src (k TEXT, v BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/in.ndjson');"
        "CREATE TABLE out (m MAP<TEXT,TEXT>) "  // value type TEXT, source maps to BIGINT
        "WITH (connector='file', format='json', path='/tmp/out.ndjson')");
    for (const auto& st : ddl.statements) {
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    }
    Binder b(cat);
    auto insert = parse("INSERT INTO out SELECT MAP(k, v) AS m FROM src");
    EXPECT_THROW((void)b.bind_insert(std::get<ast::InsertStmt>(insert.statements[0])),
                 TranslationError);
}

// ---- MATCH_RECOGNIZE: structural parse into the AST clause (phase 2 inc 1) ----

namespace {
const ast::MatchRecognizeClause& mr_of(const ast::SelectStmt& sel) {
    return *std::get<std::unique_ptr<ast::MatchRecognizeClause>>(sel.from_items.at(0));
}
}  // namespace

TEST(SqlPreparse, MatchRecognizeParsesClause) {
    const char* sql =
        "SELECT * FROM events MATCH_RECOGNIZE ("
        "  PARTITION BY user_id"
        "  ORDER BY ts"
        "  MEASURES LAST(a.price) AS final_price, FIRST(a.price) AS first_price"
        "  ONE ROW PER MATCH"
        "  AFTER MATCH SKIP PAST LAST ROW"
        "  PATTERN (a b+ c?)"
        "  DEFINE a AS price > 100, b AS price > 200, c AS price > 300"
        ")";
    auto script = parse(sql);
    ASSERT_EQ(script.statements.size(), 1U);
    const auto& sel = std::get<ast::SelectStmt>(script.statements[0]);
    ASSERT_EQ(sel.from_items.size(), 1U);
    ASSERT_TRUE(
        std::holds_alternative<std::unique_ptr<ast::MatchRecognizeClause>>(sel.from_items[0]));
    const auto& mr = mr_of(sel);

    EXPECT_EQ(mr.input.name, "events");
    ASSERT_EQ(mr.partition_by.size(), 1U);
    EXPECT_EQ(mr.partition_by[0], "user_id");
    EXPECT_EQ(mr.order_by, "ts");

    ASSERT_EQ(mr.measures.size(), 2U);
    EXPECT_EQ(mr.measures[0].alias, "final_price");
    EXPECT_EQ(mr.measures[0].expr_sql, "LAST(a.price)");
    EXPECT_EQ(mr.measures[1].alias, "first_price");

    ASSERT_EQ(mr.pattern.size(), 3U);
    EXPECT_EQ(mr.pattern[0].name, "a");
    EXPECT_EQ(mr.pattern[0].min_count, 1U);
    EXPECT_EQ(mr.pattern[0].max_count, 1U);
    EXPECT_EQ(mr.pattern[1].name, "b");  // b+
    EXPECT_EQ(mr.pattern[1].min_count, 1U);
    EXPECT_GT(mr.pattern[1].max_count, 1U);
    EXPECT_EQ(mr.pattern[2].name, "c");  // c?
    EXPECT_EQ(mr.pattern[2].min_count, 0U);
    EXPECT_EQ(mr.pattern[2].max_count, 1U);

    ASSERT_EQ(mr.define.size(), 3U);
    EXPECT_EQ(mr.define[0].var, "a");
    EXPECT_EQ(mr.define[0].predicate_sql, "price > 100");
}

TEST(SqlPreparse, MatchRecognizeQuantifierForms) {
    const char* sql =
        "SELECT * FROM e MATCH_RECOGNIZE (PARTITION BY k ORDER BY ts "
        "PATTERN (a{2} b{1,3} c* d) DEFINE a AS x>0, b AS x>0, c AS x>0, d AS x>0)";
    auto script = parse(sql);
    const auto& mr = mr_of(std::get<ast::SelectStmt>(script.statements[0]));
    ASSERT_EQ(mr.pattern.size(), 4U);
    EXPECT_EQ(mr.pattern[0].min_count, 2U);
    EXPECT_EQ(mr.pattern[0].max_count, 2U);  // {2}
    EXPECT_EQ(mr.pattern[1].min_count, 1U);
    EXPECT_EQ(mr.pattern[1].max_count, 3U);  // {1,3}
    EXPECT_EQ(mr.pattern[2].min_count, 0U);
    EXPECT_GT(mr.pattern[2].max_count, 1U);  // c*
}

TEST(SqlPreparse, MatchRecognizeColumnNameUntouched) {
    // `match_recognize` as an identifier (not followed by '(' in FROM position)
    // must pass through unrewritten.
    auto r = pp::preparse("SELECT match_recognize FROM t");
    EXPECT_EQ(r.rewritten_sql, "SELECT match_recognize FROM t");
    EXPECT_TRUE(r.match_recognize.empty());
    // and as a function call outside FROM, also untouched.
    auto r2 = pp::preparse("SELECT match_recognize(x) FROM t");
    EXPECT_TRUE(r2.match_recognize.empty());
}

TEST(SqlPreparse, MatchRecognizeBindsLogicalNode) {
    Catalog cat;
    auto ddl = parse(
        "CREATE TABLE events (user_id BIGINT, ts BIGINT, price BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/ev.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    Binder b(cat);
    const char* sql =
        "SELECT * FROM events MATCH_RECOGNIZE ("
        "  PARTITION BY user_id ORDER BY ts"
        "  MEASURES LAST(a.price) AS final_price"
        "  PATTERN (a b+)"
        "  DEFINE a AS price > 100, b AS price > 200)";
    auto plan = b.bind_select(std::get<ast::SelectStmt>(parse(sql).statements[0]));

    const clink::sql::LogicalPlan* p = plan.get();
    const clink::sql::LogicalMatchRecognize* mr = nullptr;
    while (p != nullptr) {
        if (const auto* m = dynamic_cast<const clink::sql::LogicalMatchRecognize*>(p)) {
            mr = m;
            break;
        }
        if (p->inputs().empty()) {
            break;
        }
        p = p->inputs()[0];
    }
    ASSERT_NE(mr, nullptr);
    ASSERT_EQ(mr->partition_columns().size(), 1U);
    EXPECT_EQ(mr->partition_columns()[0], "user_id");
    EXPECT_EQ(mr->order_column(), "ts");
    ASSERT_EQ(mr->pattern().size(), 2U);
    EXPECT_EQ(mr->pattern()[1].name, "b");
    EXPECT_GT(mr->pattern()[1].max_count, 1U);
    ASSERT_EQ(mr->defines().size(), 2U);
    EXPECT_FALSE(mr->defines()[0].predicate_json.empty());
    ASSERT_EQ(mr->measures().size(), 1U);
    EXPECT_EQ(mr->measures()[0].fn, "last");
    EXPECT_EQ(mr->measures()[0].var, "a");
    EXPECT_EQ(mr->measures()[0].column, "price");
    EXPECT_EQ(mr->measures()[0].output_name, "final_price");
    // Output schema = partition cols + measures, in order.
    auto sch = mr->schema();
    ASSERT_EQ(sch->num_fields(), 2);
    EXPECT_EQ(sch->field(0)->name(), "user_id");
    EXPECT_EQ(sch->field(1)->name(), "final_price");
}

TEST(SqlPreparse, MatchRecognizeBinderRejections) {
    Catalog cat;
    auto ddl = parse(
        "CREATE TABLE events (user_id BIGINT, ts BIGINT, price BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/ev2.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    Binder b(cat);
    auto bind = [&](const char* sql) {
        return b.bind_select(std::get<ast::SelectStmt>(parse(sql).statements[0]));
    };
    // unknown PARTITION BY column
    EXPECT_THROW((void)bind("SELECT * FROM events MATCH_RECOGNIZE (PARTITION BY nope ORDER BY ts "
                            "PATTERN (a) DEFINE a AS price > 0)"),
                 TranslationError);
    // MEASURES references an unknown pattern variable
    EXPECT_THROW(
        (void)bind("SELECT * FROM events MATCH_RECOGNIZE (PARTITION BY user_id ORDER BY ts "
                   "MEASURES LAST(z.price) AS p PATTERN (a) DEFINE a AS price > 0)"),
        TranslationError);
    // MEASURES with an unsupported function
    EXPECT_THROW(
        (void)bind("SELECT * FROM events MATCH_RECOGNIZE (PARTITION BY user_id ORDER BY ts "
                   "MEASURES SUM(a.price) AS p PATTERN (a) DEFINE a AS price > 0)"),
        TranslationError);
}

TEST(SqlPreparse, MatchRecognizeCompilesToOp) {
    Catalog cat;
    auto ddl = parse(
        "CREATE TABLE events (user_id BIGINT, ts BIGINT, price BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/ev3.ndjson');"
        "CREATE TABLE out_mr (user_id BIGINT, final_price BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/out_mr.ndjson')");
    for (const auto& st : ddl.statements) {
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    }
    Binder b(cat);
    const char* sql =
        "INSERT INTO out_mr SELECT * FROM events MATCH_RECOGNIZE ("
        "  PARTITION BY user_id ORDER BY ts"
        "  MEASURES LAST(a.price) AS final_price"
        "  PATTERN (a b+)"
        "  DEFINE a AS price > 100, b AS price > 200)";
    auto plan = b.bind_insert(std::get<ast::InsertStmt>(parse(sql).statements[0]));
    plan = clink::sql::optimize(std::move(plan));
    clink::sql::PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const clink::sql::LogicalSink&>(*plan));

    bool has_mr = false;
    bool has_keyer = false;
    for (const auto& op : spec.ops) {
        if (op.type == "match_recognize_row") {
            has_mr = true;
            EXPECT_EQ(op.key_by, "row_key");
            EXPECT_EQ(op.params.at("partition_keys"), "user_id");
            EXPECT_EQ(op.params.at("order_column"), "ts");
            EXPECT_NE(op.params.at("pattern").find("\"name\""), std::string::npos);
            EXPECT_NE(op.params.at("measures").find("final_price"), std::string::npos);
            EXPECT_NE(op.params.at("defines").find("\"var\""), std::string::npos);
        }
        if (op.type == "row_compute_key") {
            has_keyer = true;
        }
    }
    EXPECT_TRUE(has_mr);
    EXPECT_TRUE(has_keyer);
}

TEST(SqlPreparse, MatchRecognizeRejections) {
    // missing DEFINE
    EXPECT_THROW((void)parse("SELECT * FROM e MATCH_RECOGNIZE (PARTITION BY k ORDER BY ts "
                             "PATTERN (a))"),
                 TranslationError);
    // missing PARTITION BY (v1 requires it)
    EXPECT_THROW((void)parse("SELECT * FROM e MATCH_RECOGNIZE (ORDER BY ts PATTERN (a) "
                             "DEFINE a AS x>0)"),
                 TranslationError);
    // ALL ROWS PER MATCH not supported
    EXPECT_THROW((void)parse("SELECT * FROM e MATCH_RECOGNIZE (PARTITION BY k ORDER BY ts "
                             "ALL ROWS PER MATCH PATTERN (a) DEFINE a AS x>0)"),
                 TranslationError);
    // MEASURES item without AS
    EXPECT_THROW((void)parse("SELECT * FROM e MATCH_RECOGNIZE (PARTITION BY k ORDER BY ts "
                             "MEASURES LAST(a.x) PATTERN (a) DEFINE a AS x>0)"),
                 TranslationError);
}

// ---- SQLOPT PTF: process-table-function island ----

namespace {
const ast::ProcessTableFunctionClause& ptf_of(const ast::SelectStmt& sel) {
    return *std::get<std::unique_ptr<ast::ProcessTableFunctionClause>>(sel.from_items.at(0));
}

// A trivial passthrough KeyedProcessFunction for registry/bind coverage. The
// factory is never invoked by the bind/physical-plan path (which only consults
// the declared output schema), so passthrough body is enough.
class PassthroughPtf final
    : public clink::KeyedProcessFunction<std::string, clink::sql::Row, clink::sql::Row> {
public:
    void process_element(const clink::sql::Row& in,
                         clink::ProcessFunctionContext<clink::sql::Row>& /*ctx*/,
                         clink::Collector<clink::sql::Row>& out) override {
        out.collect(in);
    }
};

void register_passthrough_ptf(const char* name) {
    clink::sql::PtfRegistry::global().register_function(
        name,
        {clink::sql::ColumnSpec{"user_id", arrow::int64()},
         clink::sql::ColumnSpec{"running", arrow::int64()}},
        []() -> std::shared_ptr<
                 clink::KeyedProcessFunction<std::string, clink::sql::Row, clink::sql::Row>> {
            return std::make_shared<PassthroughPtf>();
        });
}
}  // namespace

TEST(SqlPreparse, ProcessTableFunctionParsesClause) {
    auto script = parse("SELECT * FROM running_total(TABLE events PARTITION BY user_id) AS p");
    ASSERT_EQ(script.statements.size(), 1U);
    const auto& sel = std::get<ast::SelectStmt>(script.statements[0]);
    ASSERT_EQ(sel.from_items.size(), 1U);
    ASSERT_TRUE(std::holds_alternative<std::unique_ptr<ast::ProcessTableFunctionClause>>(
        sel.from_items[0]));
    const auto& ptf = ptf_of(sel);
    EXPECT_EQ(ptf.fn_name, "running_total");
    EXPECT_EQ(ptf.input.name, "events");
    ASSERT_EQ(ptf.partition_by.size(), 1U);
    EXPECT_EQ(ptf.partition_by[0], "user_id");
    ASSERT_TRUE(ptf.alias.has_value());
    EXPECT_EQ(*ptf.alias, "p");
}

TEST(SqlPreparse, ProcessTableFunctionWithoutPartitionParses) {
    auto script = parse("SELECT * FROM scan_fn(TABLE events)");
    const auto& ptf = ptf_of(std::get<ast::SelectStmt>(script.statements[0]));
    EXPECT_EQ(ptf.fn_name, "scan_fn");
    EXPECT_EQ(ptf.input.name, "events");
    EXPECT_TRUE(ptf.partition_by.empty());
}

TEST(SqlPreparse, ProcessTableFunctionColumnNameUntouched) {
    // `running_total` as a bare identifier (no TABLE arg) is not a PTF island.
    auto r = pp::preparse("SELECT running_total FROM t");
    EXPECT_EQ(r.rewritten_sql, "SELECT running_total FROM t");
    EXPECT_TRUE(r.table_functions.empty());
    // a scalar function call (no TABLE keyword) is left for libpg_query.
    auto r2 = pp::preparse("SELECT running_total(x) FROM t");
    EXPECT_TRUE(r2.table_functions.empty());
}

TEST(SqlPreparse, ProcessTableFunctionRejectsScalarArgs) {
    // A scalar argument alongside the TABLE arg (a comma in the table-argument
    // region) is rejected at parse time; v1 supports only `TABLE t PARTITION BY`.
    EXPECT_THROW((void)parse("SELECT * FROM running_total(TABLE events, 5 PARTITION BY k)"),
                 TranslationError);
}

TEST(SqlPreparse, ProcessTableFunctionBindsLogicalNode) {
    register_passthrough_ptf("running_total");
    Catalog cat;
    auto ddl = parse(
        "CREATE TABLE events (user_id BIGINT, amount BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/ptf_ev.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    Binder b(cat);
    auto plan = b.bind_select(std::get<ast::SelectStmt>(
        parse("SELECT * FROM running_total(TABLE events PARTITION BY user_id)").statements[0]));

    const clink::sql::LogicalPlan* p = plan.get();
    const clink::sql::LogicalProcessTableFunction* ptf = nullptr;
    while (p != nullptr) {
        if (const auto* m = dynamic_cast<const clink::sql::LogicalProcessTableFunction*>(p)) {
            ptf = m;
            break;
        }
        if (p->inputs().empty())
            break;
        p = p->inputs()[0];
    }
    ASSERT_NE(ptf, nullptr);
    EXPECT_EQ(ptf->fn_name(), "running_total");
    ASSERT_EQ(ptf->partition_columns().size(), 1U);
    EXPECT_EQ(ptf->partition_columns()[0], "user_id");
    auto sch = ptf->schema();
    ASSERT_EQ(sch->num_fields(), 2);
    EXPECT_EQ(sch->field(0)->name(), "user_id");
    EXPECT_EQ(sch->field(1)->name(), "running");
}

TEST(SqlPreparse, ProcessTableFunctionUnknownPartitionColumnRejected) {
    register_passthrough_ptf("running_total");
    Catalog cat;
    auto ddl = parse(
        "CREATE TABLE events (user_id BIGINT, amount BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/ptf_ev2.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    Binder b(cat);
    EXPECT_THROW(
        (void)b.bind_select(std::get<ast::SelectStmt>(
            parse("SELECT * FROM running_total(TABLE events PARTITION BY nope)").statements[0])),
        TranslationError);
}

TEST(SqlPreparse, ProcessTableFunctionOrderByRejectedAtBind) {
    // ORDER BY parses but v1 does not enforce ordering, so it is rejected at bind
    // time rather than silently ignored (honesty over a silent wrong result).
    register_passthrough_ptf("running_total");
    Catalog cat;
    auto ddl = parse(
        "CREATE TABLE events (user_id BIGINT, ts BIGINT, amount BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/ptf_ev_ord.ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
    Binder b(cat);
    EXPECT_THROW((void)b.bind_select(std::get<ast::SelectStmt>(
                     parse("SELECT * FROM running_total(TABLE events PARTITION BY user_id "
                           "ORDER BY ts)")
                         .statements[0])),
                 TranslationError);
}

TEST(SqlPreparse, ProcessTableFunctionCompilesToOp) {
    register_passthrough_ptf("running_total");
    Catalog cat;
    auto ddl = parse(
        "CREATE TABLE events (user_id BIGINT, amount BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/ptf_ev3.ndjson');"
        "CREATE TABLE out_ptf (user_id BIGINT, running BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/out_ptf.ndjson')");
    for (const auto& st : ddl.statements) {
        cat.register_table(std::get<ast::CreateTableStmt>(st));
    }
    Binder b(cat);
    auto plan = b.bind_insert(
        std::get<ast::InsertStmt>(parse("INSERT INTO out_ptf SELECT user_id, running FROM "
                                        "running_total(TABLE events PARTITION BY user_id)")
                                      .statements[0]));
    plan = clink::sql::optimize(std::move(plan));
    clink::sql::PhysicalPlanner pp;
    auto spec = pp.compile(static_cast<const clink::sql::LogicalSink&>(*plan));

    bool has_ptf = false;
    bool has_keyer = false;
    for (const auto& op : spec.ops) {
        if (op.type == "process_table_function_row") {
            has_ptf = true;
            EXPECT_EQ(op.key_by, "row_key");
            EXPECT_EQ(op.params.at("function_name"), "running_total");
            EXPECT_EQ(op.params.at("partition_keys"), "user_id");
        }
        if (op.type == "row_compute_key") {
            has_keyer = true;
        }
    }
    EXPECT_TRUE(has_ptf);
    EXPECT_TRUE(has_keyer);
}

// ---- MATTBL: CREATE MATERIALIZED VIEW parse + desugar ----

namespace {
ast::CreateMaterializedViewStmt parse_mv(const char* sql) {
    auto script = parse(sql);
    return std::move(std::get<ast::CreateMaterializedViewStmt>(script.statements[0]));
}

void register_source(Catalog& cat, const char* name, const char* cols) {
    auto ddl = parse(std::string{"CREATE TABLE "} + name + " (" + cols +
                     ") WITH (connector='file', format='json', path='/tmp/" + name + ".ndjson')");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
}
}  // namespace

TEST(SqlPreparse, MaterializedViewParsesAsStatement) {
    auto script = parse(
        "CREATE MATERIALIZED VIEW mv WITH (freshness='0', connector='file', format='json') "
        "AS SELECT a, b FROM t");
    ASSERT_EQ(script.statements.size(), 1U);
    ASSERT_TRUE(std::holds_alternative<ast::CreateMaterializedViewStmt>(script.statements[0]));
    const auto& mv = std::get<ast::CreateMaterializedViewStmt>(script.statements[0]);
    EXPECT_EQ(mv.view_name, "mv");
    ASSERT_EQ(mv.options.size(), 3U);
    bool saw_freshness = false;
    bool saw_connector = false;
    for (const auto& o : mv.options) {
        if (o.key == "freshness") {
            saw_freshness = true;
            EXPECT_EQ(o.value, "0");
        }
        if (o.key == "connector") {
            saw_connector = true;
            EXPECT_EQ(o.value, "file");
        }
    }
    EXPECT_TRUE(saw_freshness);
    EXPECT_TRUE(saw_connector);
    EXPECT_EQ(mv.query.target_list.size(), 2U);
}

TEST(SqlPreparse, PlainCreateTableAsRejected) {
    // CREATE TABLE AS / SELECT INTO emit a CreateTableAsStmt with a non-matview
    // objtype; they must not silently fall into the materialized-view path.
    EXPECT_THROW((void)parse("CREATE TABLE foo AS SELECT a FROM t"), TranslationError);
}

TEST(SqlPreparse, MaterializedViewDesugarsToBackingAndMaintenance) {
    Catalog cat;
    register_source(cat, "t", "a BIGINT, b BIGINT");
    auto plan = plan_materialized_view(
        parse_mv("CREATE MATERIALIZED VIEW mv "
                 "WITH (freshness='0', connector='file', format='json', path='/tmp/mv.ndjson') "
                 "AS SELECT a, b FROM t"),
        cat);

    // The backing table is registered and tagged as a materialized view.
    const auto* td = cat.get_table("mv");
    ASSERT_NE(td, nullptr);
    EXPECT_TRUE(td->is_materialized_view());
    EXPECT_EQ(td->freshness(), "0");
    ASSERT_EQ(td->columns.size(), 2U);
    EXPECT_EQ(td->columns[0].name, "a");
    EXPECT_EQ(td->columns[1].name, "b");
    // Append backing for a non-aggregating projection.
    EXPECT_FALSE(td->is_upsert());

    // The maintenance plan is a sink rooted at the backing table.
    const auto* sink = dynamic_cast<const clink::sql::LogicalSink*>(plan.maintenance.get());
    ASSERT_NE(sink, nullptr);
    EXPECT_EQ(sink->table().name, "mv");
}

TEST(SqlPreparse, MaterializedViewKeyedAggregationDerivesUpsert) {
    Catalog cat;
    register_source(cat, "t", "k BIGINT, v BIGINT");
    auto plan = plan_materialized_view(
        parse_mv("CREATE MATERIALIZED VIEW mv "
                 "WITH (freshness='0', connector='file', format='json', path='/tmp/mv2.ndjson') "
                 "AS SELECT k, SUM(v) AS s FROM t GROUP BY k"),
        cat);
    const auto* td = cat.get_table("mv");
    ASSERT_NE(td, nullptr);
    EXPECT_TRUE(td->is_upsert());
    EXPECT_EQ(td->properties.at("primary_key"), "k");
}

TEST(SqlPreparse, MaterializedViewGlobalAggregateRejected) {
    // An ungrouped aggregate has no key to materialise; rejected rather than
    // silently materialising a changelog of every intermediate.
    Catalog cat;
    register_source(cat, "t", "v BIGINT");
    EXPECT_THROW((void)plan_materialized_view(
                     parse_mv("CREATE MATERIALIZED VIEW mv "
                              "WITH (freshness='0', connector='file', format='json') "
                              "AS SELECT SUM(v) AS s FROM t"),
                     cat),
                 TranslationError);
}

TEST(SqlPreparse, MaterializedViewNonBothCapableConnectorRejected) {
    Catalog cat;
    register_source(cat, "t", "a BIGINT");
    EXPECT_THROW((void)plan_materialized_view(
                     parse_mv("CREATE MATERIALIZED VIEW mv "
                              "WITH (freshness='0', connector='postgres') AS SELECT a FROM t"),
                     cat),
                 TranslationError);
}

TEST(SqlPreparse, MaterializedViewFullRefreshAcceptedForBoundedSource) {
    Catalog cat;
    register_source(cat, "t", "a BIGINT");  // connector='file' (bounded)
    auto plan = plan_materialized_view(
        parse_mv("CREATE MATERIALIZED VIEW mv "
                 "WITH (freshness='5m', connector='file', format='json', path='/tmp/mv_fr.ndjson') "
                 "AS SELECT a FROM t"),
        cat);
    EXPECT_EQ(plan.arm, clink::sql::RefreshArm::Full);
    // The full-refresh backing overwrites wholesale on each refresh.
    ASSERT_NE(cat.get_table("mv"), nullptr);
    EXPECT_EQ(cat.get_table("mv")->properties.at("write_mode"), "overwrite");
    EXPECT_EQ(cat.get_table("mv")->properties.at("refresh_arm"), "full");
}

TEST(SqlPreparse, FullRefreshBackingSurvivesCatalogReload) {
    // HA / restart survival: a full-refresh backing persists its refresh metadata to
    // the catalog dir, so a new leader that reloads the catalog has everything the
    // scheduler needs to resume the view (refresh_arm + freshness_ms + definition_sql).
    const auto dir = std::filesystem::temp_directory_path() / "clink_mv_ha_catalog";
    std::filesystem::remove_all(dir);
    {
        Catalog cat;
        cat.set_persistence_dir(dir.string());
        register_source(cat, "t", "a BIGINT");  // connector='file' (bounded), persisted
        const std::string mv_sql =
            "CREATE MATERIALIZED VIEW mv "
            "WITH (freshness='30m', connector='file', format='json', path='/tmp/mv_ha.ndjson') "
            "AS SELECT a FROM t";
        (void)plan_materialized_view(parse_mv(mv_sql.c_str()), cat, mv_sql);
    }
    // A fresh catalog loaded from the dir (as a new JM leader would) sees the view.
    Catalog reloaded;
    reloaded.load_from_dir(dir.string());
    const auto* mv = reloaded.get_table("mv");
    ASSERT_NE(mv, nullptr);
    EXPECT_TRUE(mv->is_materialized_view());
    EXPECT_EQ(mv->properties.at("refresh_arm"), "full");
    EXPECT_EQ(mv->properties.at("freshness_ms"), std::to_string(30LL * 60 * 1000));
    EXPECT_EQ(mv->properties.at("write_mode"), "overwrite");
    EXPECT_FALSE(mv->definition_sql().empty());
    std::filesystem::remove_all(dir);
}

TEST(SqlPreparse, MaterializedViewFullRefreshRejectsUnboundedSource) {
    Catalog cat;
    // A kafka source is an unbounded stream: a full (scheduled) refresh recompute
    // would never finish, so it is rejected at plan time.
    cat.register_table(std::get<ast::CreateTableStmt>(
        parse("CREATE TABLE ks (a BIGINT) WITH (connector='kafka', format='json', topic='t')")
            .statements[0]));
    EXPECT_THROW((void)plan_materialized_view(
                     parse_mv("CREATE MATERIALIZED VIEW mv "
                              "WITH (freshness='5m', connector='file', format='json', "
                              "path='/tmp/mv_fr2.ndjson') AS SELECT a FROM ks"),
                     cat),
                 TranslationError);
}

TEST(SqlPreparse, MaterializedViewNameCollisionRejected) {
    Catalog cat;
    register_source(cat, "t", "a BIGINT");
    register_source(cat, "mv", "a BIGINT");  // a real table already owns the name
    EXPECT_THROW((void)plan_materialized_view(
                     parse_mv("CREATE MATERIALIZED VIEW mv "
                              "WITH (freshness='0', connector='file', format='json') "
                              "AS SELECT a FROM t"),
                     cat),
                 TranslationError);
}
