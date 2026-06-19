// #59: programmatic Table API tests. The primary safety net is byte-identical
// IR: a query built through the Table API must compile to the SAME JobGraphSpec
// as the equivalent SQL string, because both lower expressions/types through the
// shared clink::sql::lowering helpers and share the optimise + physical-plan
// tail. Plus build-time validation + the exact-decimal (#56) reuse.

#include <string>
#include <variant>

#include <gtest/gtest.h>

#include "clink/sql/binder.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/optimizer.hpp"
#include "clink/sql/parser.hpp"
#include "clink/sql/physical_plan.hpp"
#include "clink/sql/table_api.hpp"

using namespace clink;
using clink::sql::Binder;
using clink::sql::Catalog;
using clink::sql::optimize;
using clink::sql::parse;
using clink::sql::PhysicalPlanner;
using clink::sql::TranslationError;
namespace api = clink::api;

namespace {

// Compile a SQL INSERT statement to a JobGraphSpec via the binder + the same
// optimise/physical tail the Table API uses.
cluster::JobGraphSpec compile_sql(const Catalog& cat, const char* sql) {
    Binder b(cat);
    auto plan = b.bind_insert(std::get<clink::sql::ast::InsertStmt>(parse(sql).statements[0]));
    plan = optimize(std::move(plan));
    PhysicalPlanner pp;
    return pp.compile(static_cast<const clink::sql::LogicalSink&>(*plan));
}

// Catalog with a source `t(a,b BIGINT)` and sink `out(x,y BIGINT)`, both
// file/json (Row channel).
Catalog make_catalog() {
    Catalog cat;
    auto ddl = parse(
        "CREATE TABLE t (a BIGINT, b BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/tapi_t.ndjson');"
        "CREATE TABLE out (x BIGINT, y DOUBLE) "  // y is DOUBLE: a+b (int arith) types as float64
        "WITH (connector='file', format='json', path='/tmp/tapi_out.ndjson')");
    for (const auto& st : ddl.statements) {
        cat.register_table(std::get<clink::sql::ast::CreateTableStmt>(st));
    }
    return cat;
}

}  // namespace

// The headline property: Table API == SQL, byte-for-byte, at the JobGraphSpec.
TEST(TableApi, FilterAndProjectMatchSql) {
    Catalog cat = make_catalog();
    auto sql_spec =
        compile_sql(cat, "INSERT INTO out SELECT a AS x, a + b AS y FROM t WHERE b > 0");

    api::TableEnvironment tenv{cat};
    using namespace clink::api;
    auto api_spec = tenv.from("t")
                        .filter(col("b") > lit(std::int64_t{0}))
                        .select(col("a") >> "x", (col("a") + col("b")) >> "y")
                        .insert_into("out");

    EXPECT_EQ(api_spec.to_json(), sql_spec.to_json());
}

// Plan shape: the fluent chain builds Sink<-Project<-Filter<-Scan. (We inspect
// the pre-insert plan via .plan() for the inner chain.)
TEST(TableApi, BuildsExpectedPlanTree) {
    Catalog cat = make_catalog();
    api::TableEnvironment tenv{cat};
    using namespace clink::api;
    auto t = tenv.from("t").filter(col("b") > lit(std::int64_t{0})).select(col("a") >> "x");
    // top is Project, its input Filter, its input Scan.
    const auto& project = t.plan();
    EXPECT_EQ(project.kind(), "Project");
    ASSERT_EQ(project.inputs().size(), 1U);
    EXPECT_EQ(project.inputs()[0]->kind(), "Filter");
    ASSERT_EQ(project.inputs()[0]->inputs().size(), 1U);
    EXPECT_EQ(project.inputs()[0]->inputs()[0]->kind(), "Scan");
}

// Exact DECIMAL literal reuse: dec("1.50") lowers through the #56 path, so the
// projected column types + IR match the SQL `SELECT 1.50`.
TEST(TableApi, DecimalLiteralMatchesSql) {
    Catalog cat;
    auto ddl = parse(
        "CREATE TABLE src (id BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/tapi_d_in.ndjson');"
        "CREATE TABLE dout (id BIGINT, p DECIMAL(10,2)) "
        "WITH (connector='file', format='json', path='/tmp/tapi_d_out.ndjson')");
    for (const auto& st : ddl.statements) {
        cat.register_table(std::get<clink::sql::ast::CreateTableStmt>(st));
    }
    auto sql_spec = compile_sql(cat, "INSERT INTO dout SELECT id, 1.50 AS p FROM src");

    api::TableEnvironment tenv{cat};
    using namespace clink::api;
    auto api_spec =
        tenv.from("src").select(col("id") >> "id", dec("1.50") >> "p").insert_into("dout");

    EXPECT_EQ(api_spec.to_json(), sql_spec.to_json());
}

// GROUP BY + aggregate built fluently == the SQL string, byte-for-byte. The
// shared lowering::build_group_output_schema guarantees identical output
// column order/naming/types between the two front-ends.
TEST(TableApi, GroupBySumMatchesSql) {
    Catalog cat;
    auto ddl = parse(
        "CREATE TABLE g (k BIGINT, v BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/tapi_g.ndjson');"
        "CREATE TABLE gout (k BIGINT, total BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/tapi_gout.ndjson')");
    for (const auto& st : ddl.statements) {
        cat.register_table(std::get<clink::sql::ast::CreateTableStmt>(st));
    }
    auto sql_spec = compile_sql(
        cat, "INSERT INTO gout SELECT k, SUM(v) AS total FROM g WHERE v > 0 GROUP BY k");

    api::TableEnvironment tenv{cat};
    using namespace clink::api;
    auto api_spec = tenv.from("g")
                        .filter(col("v") > lit(std::int64_t{0}))
                        .group_by({"k"})
                        .agg({key("k"), sum("v") >> "total"})
                        .insert_into("gout");

    EXPECT_EQ(api_spec.to_json(), sql_spec.to_json());
}

// agg() validation mirrors the binder: a key not in group_by(), and an agg()
// with no aggregate functions, both reject.
TEST(TableApi, GroupByValidationErrors) {
    Catalog cat;
    auto ddl = parse(
        "CREATE TABLE g2 (k BIGINT, v BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/tapi_g2.ndjson');"
        "CREATE TABLE g2out (k BIGINT, total BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/tapi_g2out.ndjson')");
    for (const auto& st : ddl.statements) {
        cat.register_table(std::get<clink::sql::ast::CreateTableStmt>(st));
    }
    api::TableEnvironment tenv{cat};
    using namespace clink::api;

    // group_by on an unknown column.
    EXPECT_THROW((void)tenv.from("g2").group_by({"nope"}), TranslationError);

    // SELECT a key that is not in the group_by set.
    EXPECT_THROW((void)tenv.from("g2").group_by({"k"}).agg({key("v"), sum("v") >> "total"}),
                 TranslationError);

    // No aggregate function in agg().
    EXPECT_THROW((void)tenv.from("g2").group_by({"k"}).agg({key("k")}), TranslationError);

    // Aggregate over a column that does not exist (SQL rejects this in the
    // binder; the Table API must not be more permissive).
    EXPECT_THROW((void)tenv.from("g2").group_by({"k"}).agg({key("k"), sum("nope") >> "total"}),
                 TranslationError);

    // DISTINCT is only valid on COUNT / STRING_AGG / ARRAY_AGG. The public Agg
    // constructor admits any combination; agg() must reject SUM(DISTINCT ...)
    // the same way the binder does.
    EXPECT_THROW(
        (void)tenv.from("g2").group_by({"k"}).agg({key("k"), Agg{"sum", "v", true} >> "total"}),
        TranslationError);
}

// Every aggregate builder + mixed ordering + key aliasing, byte-identical to
// the SQL string. Exercises agg-before-key, COUNT(*), COUNT(DISTINCT),
// MIN/MAX/AVG types, and the STRING_AGG separator through the shared lowering.
TEST(TableApi, GroupByMultiAggMatchesSql) {
    Catalog cat;
    auto ddl = parse(
        "CREATE TABLE gm (k BIGINT, v BIGINT, name VARCHAR) "
        "WITH (connector='file', format='json', path='/tmp/tapi_gm.ndjson');"
        "CREATE TABLE mout (n BIGINT, kk BIGINT, s BIGINT, lo BIGINT, hi BIGINT, "
        "av DOUBLE, dc BIGINT, names VARCHAR) "
        "WITH (connector='file', format='json', path='/tmp/tapi_mout.ndjson')");
    for (const auto& st : ddl.statements) {
        cat.register_table(std::get<clink::sql::ast::CreateTableStmt>(st));
    }
    auto sql_spec = compile_sql(
        cat,
        "INSERT INTO mout SELECT COUNT(*) AS n, k AS kk, SUM(v) AS s, MIN(v) AS lo, "
        "MAX(v) AS hi, AVG(v) AS av, COUNT(DISTINCT v) AS dc, STRING_AGG(name, '|') AS names "
        "FROM gm WHERE v > 0 GROUP BY k");

    api::TableEnvironment tenv{cat};
    using namespace clink::api;
    auto api_spec = tenv.from("gm")
                        .filter(col("v") > lit(std::int64_t{0}))
                        .group_by({"k"})
                        .agg({count_star() >> "n",
                              key("k") >> "kk",
                              sum("v") >> "s",
                              min_("v") >> "lo",
                              max_("v") >> "hi",
                              avg("v") >> "av",
                              count_distinct("v") >> "dc",
                              string_agg("name", "|") >> "names"})
                        .insert_into("mout");

    EXPECT_EQ(api_spec.to_json(), sql_spec.to_json());
}

// Build-time validation surfaces the same way as SQL.
TEST(TableApi, ValidationErrors) {
    Catalog cat = make_catalog();
    api::TableEnvironment tenv{cat};
    using namespace clink::api;

    // Unknown source table.
    EXPECT_THROW((void)tenv.from("nope"), TranslationError);

    // Sink type mismatch: project a string into the BIGINT sink column.
    EXPECT_THROW((void)tenv.from("t")
                     .select(col("a") >> "x", lit(std::string{"hi"}) >> "y")
                     .insert_into("out"),
                 TranslationError);

    // Wrong column count for the sink.
    EXPECT_THROW((void)tenv.from("t").select(col("a") >> "x").insert_into("out"), TranslationError);
}
