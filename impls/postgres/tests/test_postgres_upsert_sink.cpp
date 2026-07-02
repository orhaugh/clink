// PostgresJsonUpsertSink tests - the changelog-aware upsert sink (mode='upsert').
//
// Construction validation runs anywhere libpq is linked (no server). The LIVE
// integration tests are SKIPPED unless CLINK_POSTGRES_CDC_TEST_DSN is set. They
// prove against real Postgres that a changelog stream maintains the table by
// primary key: an insert/update_after upserts, a delete/update_before removes by
// key, ops are netted within a flush, and a replayed batch converges to the same
// state (effectively-once on the sink table).

#include <cstdlib>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#ifdef CLINK_HAS_POSTGRES
#include <libpq-fe.h>

#include "clink/connectors/postgres_json_upsert_sink.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::CheckpointId;
using clink::PostgresJsonUpsertSink;
using clink::PostgresJsonUpsertSinkOptions;

namespace {

// --- construction validation (no server) --------------------------------

PostgresJsonUpsertSinkOptions valid_opts() {
    PostgresJsonUpsertSinkOptions o;
    o.conninfo = "host=localhost";
    o.table = "t";
    o.columns = {"id", "val"};
    o.key_columns = {"id"};
    return o;
}

TEST(PostgresUpsertSink, RejectsEmptyKeyColumns) {
    auto o = valid_opts();
    o.key_columns.clear();
    EXPECT_THROW(PostgresJsonUpsertSink{std::move(o)}, std::runtime_error);
}

TEST(PostgresUpsertSink, RejectsEmptyColumns) {
    auto o = valid_opts();
    o.columns.clear();
    EXPECT_THROW(PostgresJsonUpsertSink{std::move(o)}, std::runtime_error);
}

// --- live integration ---------------------------------------------------

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

PostgresJsonUpsertSinkOptions opts_for(const std::string& tbl) {
    PostgresJsonUpsertSinkOptions o;
    o.conninfo = pg_dsn();
    o.table = tbl;
    o.columns = {"id", "val"};
    o.key_columns = {"id"};
    return o;
}

// Apply one changelog batch through a fresh sink lifecycle (open -> data ->
// flush via barrier -> close).
void apply_changelog(const std::string& tbl, const std::vector<std::string>& rows) {
    PostgresJsonUpsertSink sink(opts_for(tbl));
    sink.open();
    Batch<std::string> b;
    for (const auto& r : rows) {
        b.emplace(std::string(r));
    }
    sink.on_data(b);
    sink.on_barrier(CheckpointBarrier{CheckpointId{1}});
    sink.close();
}

struct LiveTable {
    std::string name;
    explicit LiveTable(std::string n) : name(std::move(n)) {
        run_sql("DROP TABLE IF EXISTS " + name);
        run_sql("CREATE TABLE " + name + " (id int primary key, val text)");
    }
    ~LiveTable() { run_sql("DROP TABLE IF EXISTS " + name); }
};

#define REQUIRE_LIVE_PG()                                      \
    do {                                                       \
        if (!pg_configured())                                  \
            GTEST_SKIP() << "set CLINK_POSTGRES_CDC_TEST_DSN"; \
    } while (0)

TEST(PostgresUpsertSinkLive, InsertThenUpdateByKey) {
    REQUIRE_LIVE_PG();
    LiveTable t("pg_ups_upd_" + uniq());

    apply_changelog(t.name, {R"({"id":1,"val":"a"})", R"({"id":2,"val":"b"})"});
    EXPECT_EQ(scalar("SELECT COUNT(*) FROM " + t.name), "2");

    // update_after for id=1 overwrites; a new key 3 is inserted.
    apply_changelog(
        t.name, {R"({"id":1,"val":"a2","__row_kind":"update_after"})", R"({"id":3,"val":"c"})"});
    EXPECT_EQ(scalar("SELECT COUNT(*) FROM " + t.name), "3");
    EXPECT_EQ(scalar("SELECT val FROM " + t.name + " WHERE id=1"), "a2");
}

TEST(PostgresUpsertSinkLive, DeleteRemovesByKey) {
    REQUIRE_LIVE_PG();
    LiveTable t("pg_ups_del_" + uniq());

    apply_changelog(t.name, {R"({"id":1,"val":"a"})", R"({"id":2,"val":"b"})"});
    apply_changelog(t.name, {R"({"id":1,"__row_kind":"delete"})"});

    EXPECT_EQ(scalar("SELECT COUNT(*) FROM " + t.name), "1");
    EXPECT_EQ(scalar("SELECT val FROM " + t.name + " WHERE id=2"), "b");
    EXPECT_EQ(scalar("SELECT COUNT(*) FROM " + t.name + " WHERE id=1"), "0");
}

TEST(PostgresUpsertSinkLive, NettedWithinOneFlush) {
    REQUIRE_LIVE_PG();
    LiveTable t("pg_ups_net_" + uniq());

    // Within one flush: insert id=1, overwrite it, then delete it -> nets to gone.
    // id=2 inserted then overwritten -> nets to the last value.
    apply_changelog(t.name,
                    {R"({"id":1,"val":"first"})",
                     R"({"id":2,"val":"x"})",
                     R"({"id":1,"val":"second","__row_kind":"update_after"})",
                     R"({"id":2,"val":"y","__row_kind":"update_after"})",
                     R"({"id":1,"__row_kind":"delete"})"});

    EXPECT_EQ(scalar("SELECT COUNT(*) FROM " + t.name), "1");
    EXPECT_EQ(scalar("SELECT val FROM " + t.name + " WHERE id=2"), "y");
    EXPECT_EQ(scalar("SELECT COUNT(*) FROM " + t.name + " WHERE id=1"), "0");
}

TEST(PostgresUpsertSinkLive, ReplayIsIdempotent) {
    REQUIRE_LIVE_PG();
    LiveTable t("pg_ups_replay_" + uniq());

    const std::vector<std::string> batch = {
        R"({"id":1,"val":"a"})", R"({"id":2,"val":"b"})", R"({"id":1,"__row_kind":"delete"})"};
    apply_changelog(t.name, batch);
    apply_changelog(t.name, batch);  // replay the same batch: keyed idempotent ops

    EXPECT_EQ(scalar("SELECT COUNT(*) FROM " + t.name), "1");
    EXPECT_EQ(scalar("SELECT val FROM " + t.name + " WHERE id=2"), "b");
}

}  // namespace

#else  // !CLINK_HAS_POSTGRES

TEST(PostgresUpsertSink, SkippedWithoutLibpq) {
    GTEST_SKIP() << "built without libpq";
}

#endif  // CLINK_HAS_POSTGRES
