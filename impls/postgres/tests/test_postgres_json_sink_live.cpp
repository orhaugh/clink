// Postgres JSON sink LIVE integration test (M4). SKIPPED unless
// CLINK_POSTGRES_CDC_TEST_DSN is set (reuses the postgres-cdc service from
// docker/integration-services.yml; a plain conninfo works for INSERT/SELECT).
// Proves against real Postgres: a multi-row INSERT round-trips; on_conflict=
// 'update' is idempotent by the conflict key.

#include <cstdlib>
#include <map>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#ifdef CLINK_HAS_POSTGRES
#include <libpq-fe.h>

#include "clink/connectors/postgres_json_sink.hpp"
#include "clink/connectors/postgres_row.hpp"
#include "clink/connectors/postgres_source.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"
#endif

#ifdef CLINK_HAS_POSTGRES

using clink::Batch;
using clink::Emitter;
using clink::PostgresJsonSink;
using clink::PostgresJsonSinkOptions;
using clink::PostgresRow;
using clink::PostgresSource;
using clink::StreamElement;

namespace {

bool pg_configured() {
    return std::getenv("CLINK_POSTGRES_CDC_TEST_DSN") != nullptr;
}
std::string pg_dsn() {
    return std::getenv("CLINK_POSTGRES_CDC_TEST_DSN");
}

std::string uniq() {
    return std::to_string(static_cast<long>(::getpid()));
}

void run_sql(const std::string& sql) {
    PGconn* c = PQconnectdb(pg_dsn().c_str());
    ASSERT_EQ(PQstatus(c), CONNECTION_OK) << PQerrorMessage(c);
    PGresult* r = PQexec(c, sql.c_str());
    const auto st = PQresultStatus(r);
    EXPECT_TRUE(st == PGRES_COMMAND_OK || st == PGRES_TUPLES_OK)
        << sql << " -> " << PQerrorMessage(c);
    PQclear(r);
    PQfinish(c);
}

// SELECT a single scalar as text (empty if no row).
std::string scalar(const std::string& sql) {
    PGconn* c = PQconnectdb(pg_dsn().c_str());
    EXPECT_EQ(PQstatus(c), CONNECTION_OK) << PQerrorMessage(c);
    PGresult* r = PQexec(c, sql.c_str());
    std::string out;
    if (PQresultStatus(r) == PGRES_TUPLES_OK && PQntuples(r) > 0 && !PQgetisnull(r, 0, 0)) {
        out = PQgetvalue(r, 0, 0);
    }
    PQclear(r);
    PQfinish(c);
    return out;
}

void sink_write(PostgresJsonSinkOptions o, const std::vector<std::string>& rows) {
    PostgresJsonSink sink(std::move(o));
    sink.open();
    Batch<std::string> b;
    for (const auto& r : rows) {
        b.emplace(std::string(r));
    }
    sink.on_data(b);
    sink.flush();
    sink.close();
}

PostgresJsonSinkOptions opts(const std::string& tbl) {
    PostgresJsonSinkOptions o;
    o.conninfo = pg_dsn();
    o.table = tbl;
    o.columns = {"id", "val"};
    return o;
}

}  // namespace

TEST(PostgresJsonSinkLive, MultiRowInsertRoundTrips) {
    if (!pg_configured()) {
        GTEST_SKIP() << "set CLINK_POSTGRES_CDC_TEST_DSN (docker/integration-services.yml)";
    }
    const std::string tbl = "pjs_" + uniq();
    run_sql("DROP TABLE IF EXISTS " + tbl);
    run_sql("CREATE TABLE " + tbl + " (id int primary key, val text)");

    std::vector<std::string> rows;
    for (int i = 1; i <= 20; ++i) {
        rows.push_back(R"({"id":)" + std::to_string(i) + R"(,"val":"v)" + std::to_string(i) +
                       R"("})");
    }
    sink_write(opts(tbl), rows);

    EXPECT_EQ(scalar("SELECT COUNT(*) FROM " + tbl), "20");
    EXPECT_EQ(scalar("SELECT val FROM " + tbl + " WHERE id=7"), "v7");
    run_sql("DROP TABLE IF EXISTS " + tbl);
}

TEST(PostgresJsonSinkLive, OnConflictUpdateIsIdempotentByKey) {
    if (!pg_configured()) {
        GTEST_SKIP() << "set CLINK_POSTGRES_CDC_TEST_DSN";
    }
    const std::string tbl = "pjs_uc_" + uniq();
    run_sql("DROP TABLE IF EXISTS " + tbl);
    run_sql("CREATE TABLE " + tbl + " (id int primary key, val text)");

    PostgresJsonSinkOptions o = opts(tbl);
    o.on_conflict = "update";
    o.conflict_columns = {"id"};
    sink_write(o, {R"({"id":1,"val":"first"})"});
    sink_write(o, {R"({"id":1,"val":"second"})"});  // same key -> overwrite

    EXPECT_EQ(scalar("SELECT COUNT(*) FROM " + tbl), "1")
        << "on_conflict update must not duplicate";
    EXPECT_EQ(scalar("SELECT val FROM " + tbl + " WHERE id=1"), "second")
        << "on_conflict update must overwrite";
    run_sql("DROP TABLE IF EXISTS " + tbl);
}

// M5b: PostgresSource must populate the per-cell null mask (PQgetisnull) so a SQL
// NULL is distinguishable from an empty string - the basis for the JSON source
// emitting JSON null rather than "".
TEST(PostgresJsonSinkLive, SourcePopulatesNullMask) {
    if (!pg_configured()) {
        GTEST_SKIP() << "set CLINK_POSTGRES_CDC_TEST_DSN";
    }
    const std::string tbl = "pjs_srcnull_" + uniq();
    run_sql("DROP TABLE IF EXISTS " + tbl);
    run_sql("CREATE TABLE " + tbl + " (id int primary key, val text)");
    run_sql("INSERT INTO " + tbl + " VALUES (1, NULL), (2, '')");  // NULL vs empty-string

    PostgresSource::Options o;
    o.conninfo = pg_dsn();
    o.query = "SELECT id, val FROM " + tbl + " ORDER BY id";
    o.batch_size = 10;
    PostgresSource src(o);
    src.open();
    std::vector<PostgresRow> rows;
    Emitter<PostgresRow> cap{[&rows](StreamElement<PostgresRow> e) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                rows.push_back(r.value());
            }
        }
        return true;
    }};
    for (int i = 0; i < 10 && rows.size() < 2; ++i) {
        src.produce(cap);
    }
    src.close();
    run_sql("DROP TABLE IF EXISTS " + tbl);

    ASSERT_EQ(rows.size(), 2u);
    EXPECT_FALSE(rows[0].is_null(0)) << "id is not null";
    EXPECT_TRUE(rows[0].is_null(1)) << "val IS NULL for id=1";
    EXPECT_FALSE(rows[1].is_null(1)) << "empty-string is NOT null for id=2";
    EXPECT_EQ(rows[1].at(1), "");
}

TEST(PostgresJsonSinkLive, NullAndMissingFieldsBecomeSqlNull) {
    if (!pg_configured()) {
        GTEST_SKIP() << "set CLINK_POSTGRES_CDC_TEST_DSN";
    }
    const std::string tbl = "pjs_null_" + uniq();
    run_sql("DROP TABLE IF EXISTS " + tbl);
    run_sql("CREATE TABLE " + tbl + " (id int primary key, val text)");

    sink_write(opts(tbl), {R"({"id":1,"val":null})", R"({"id":2})"});  // explicit null + missing

    EXPECT_EQ(scalar("SELECT COUNT(*) FROM " + tbl + " WHERE val IS NULL"), "2");
    run_sql("DROP TABLE IF EXISTS " + tbl);
}

#endif  // CLINK_HAS_POSTGRES
