// Unit-test surface for PostgresSink that does not require a live
// Postgres. The real INSERT path is exercised in
// tests/integration/test_postgres_sink_integration.cpp under Docker. The
// barrier case at the end needs a server too, and is skipped unless
// CLINK_POSTGRES_CDC_TEST_DSN is set (docker/integration-services.yml).

#include <chrono>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#ifdef CLINK_HAS_POSTGRES
#include <libpq-fe.h>
#endif

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/connectors/postgres_sink.hpp"
#include "clink/core/record.hpp"
#include "clink/core/types.hpp"

namespace {

TEST(PostgresSinkUnit, IsRealImplementationWhenBuiltWithLibpq) {
    EXPECT_TRUE(clink::PostgresSink::is_real_implementation());
}

TEST(PostgresSinkUnit, EmptySqlThrowsAtConstruction) {
    clink::PostgresSink::Options opts;
    opts.conninfo = "host=127.0.0.1 dbname=nope user=nope";
    // sql intentionally empty.
    EXPECT_THROW({ clink::PostgresSink sink(opts); }, std::invalid_argument);
}

TEST(PostgresSinkUnit, OpenWithUnreachableConninfoThrows) {
    // 127.0.0.1:1 is a reserved low port; libpq fails fast on connect.
    clink::PostgresSink::Options opts;
    opts.conninfo = "host=127.0.0.1 port=1 dbname=does_not_exist user=nope connect_timeout=1";
    opts.sql = "INSERT INTO sink_unit_t VALUES ($1)";
    clink::PostgresSink sink(opts);
    EXPECT_THROW(sink.open(), std::runtime_error);
}

#ifdef CLINK_HAS_POSTGRES

// Runs one statement on a connection of its own and returns the first value
// of its first row as text (empty when there is none).
std::string legacy_sink_query(const std::string& dsn, const std::string& sql) {
    PGconn* c = PQconnectdb(dsn.c_str());
    EXPECT_EQ(PQstatus(c), CONNECTION_OK) << PQerrorMessage(c);
    PGresult* r = PQexec(c, sql.c_str());
    const auto st = PQresultStatus(r);
    EXPECT_TRUE(st == PGRES_COMMAND_OK || st == PGRES_TUPLES_OK)
        << sql << " -> " << PQerrorMessage(c);
    std::string out;
    if (st == PGRES_TUPLES_OK && PQntuples(r) > 0 && PQgetisnull(r, 0, 0) == 0) {
        out = PQgetvalue(r, 0, 0);
    }
    PQclear(r);
    PQfinish(c);
    return out;
}

// A checkpoint barrier commits the rows buffered before it. The sink used to
// commit only when its size or time trigger fired, so a checkpoint could
// complete while rows it covered were still in memory, and a process that died
// then lost them: the restore resumed the sources after them.
TEST(PostgresSinkLive, ABarrierCommitsTheRowsBeforeIt) {
    const char* dsn = std::getenv("CLINK_POSTGRES_CDC_TEST_DSN");
    if (dsn == nullptr) {
        GTEST_SKIP() << "set CLINK_POSTGRES_CDC_TEST_DSN (docker/integration-services.yml)";
    }
    const std::string tbl = "legacy_pg_barrier_" + std::to_string(static_cast<long>(::getpid()));
    legacy_sink_query(dsn, "DROP TABLE IF EXISTS " + tbl);
    legacy_sink_query(dsn, "CREATE TABLE " + tbl + " (id text, val text)");

    clink::PostgresSink::Options opts;
    opts.conninfo = dsn;
    opts.sql = "INSERT INTO " + tbl + " VALUES ($1, $2)";
    // Neither trigger fires during the test, so only the barrier can commit.
    opts.batch_rows = 1000;
    opts.batch_interval = std::chrono::hours{1};
    clink::PostgresSink sink(opts);
    sink.open();
    clink::Batch<std::vector<std::string>> batch;
    batch.emplace(std::vector<std::string>{"1", "a"});
    batch.emplace(std::vector<std::string>{"2", "b"});
    batch.emplace(std::vector<std::string>{"3", "c"});
    sink.on_data(batch);
    ASSERT_EQ(legacy_sink_query(dsn, "SELECT COUNT(*) FROM " + tbl), "0")
        << "a trigger committed the rows before the barrier, so the test proves nothing";

    sink.on_barrier(clink::CheckpointBarrier{clink::CheckpointId{1}});
    // Read on another connection: the rows are committed, not only sent.
    EXPECT_EQ(legacy_sink_query(dsn, "SELECT COUNT(*) FROM " + tbl), "3");

    sink.close();
    legacy_sink_query(dsn, "DROP TABLE IF EXISTS " + tbl);
}

#endif  // CLINK_HAS_POSTGRES

}  // namespace
