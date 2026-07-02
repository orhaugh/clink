// PostgresJsonSink2PC tests - the exactly-once Postgres sink (PREPARE
// TRANSACTION / COMMIT PREPARED) riding CommittingSink.
//
// Construction-validation tests run anywhere libpq is linked (no server).
// The LIVE integration tests are SKIPPED unless CLINK_POSTGRES_CDC_TEST_DSN is
// set (reuses the postgres service from docker/integration-services.yml) AND the
// server has max_prepared_transactions > 0. They prove against real Postgres:
//   * commit round-trip: barrier -> prepare, on_commit -> rows visible;
//   * crash recovery: a prepared transaction survives the session, and a fresh
//     sink sharing the checkpoint state COMMIT PREPAREs it at open;
//   * orphan rollback: a prepared transaction NOT in the restored pending set
//     is ROLLBACK PREPAREd at open;
//   * idempotent commit: a second on_commit is a no-op.

#include <cstdlib>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#ifdef CLINK_HAS_POSTGRES
#include <libpq-fe.h>

#include "clink/connectors/postgres_json_sink_2pc.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::CheckpointId;
using clink::InMemoryStateBackend;
using clink::OperatorId;
using clink::PostgresJsonSink2PC;
using clink::PostgresJsonSink2PCOptions;
using clink::RuntimeContext;

namespace {

// --- construction validation (no server) --------------------------------

PostgresJsonSink2PCOptions valid_opts() {
    PostgresJsonSink2PCOptions o;
    o.conninfo = "host=localhost";
    o.table = "t";
    o.columns = {"id", "val"};
    return o;
}

TEST(PostgresSink2PC, RejectsEmptyConninfo) {
    auto o = valid_opts();
    o.conninfo.clear();
    EXPECT_THROW(PostgresJsonSink2PC{std::move(o)}, std::runtime_error);
}

TEST(PostgresSink2PC, RejectsEmptyTable) {
    auto o = valid_opts();
    o.table.clear();
    EXPECT_THROW(PostgresJsonSink2PC{std::move(o)}, std::runtime_error);
}

TEST(PostgresSink2PC, RejectsEmptyColumns) {
    auto o = valid_opts();
    o.columns.clear();
    EXPECT_THROW(PostgresJsonSink2PC{std::move(o)}, std::runtime_error);
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

// Prepared transactions are disabled by default (max_prepared_transactions=0);
// the sink cannot run there, so skip rather than fail.
bool two_phase_enabled() {
    return scalar("SHOW max_prepared_transactions") != "0";
}

PostgresJsonSink2PCOptions opts_for(const std::string& tbl) {
    PostgresJsonSink2PCOptions o;
    o.conninfo = pg_dsn();
    o.table = tbl;
    o.columns = {"id", "val"};
    return o;
}

// Build a sink wired to a state backend + a stable identity, so the gid it
// derives (clink_pg2pc_<sub>_<ckpt>) is reproducible across "restarts".
std::shared_ptr<PostgresJsonSink2PC> make_sink(const std::string& tbl, RuntimeContext& rctx) {
    auto sink = std::make_shared<PostgresJsonSink2PC>(opts_for(tbl));
    sink->set_id(OperatorId{77});
    sink->set_uid("pg2pc");
    sink->attach_runtime(&rctx);
    return sink;
}

Batch<std::string> rows(const std::vector<std::string>& xs) {
    Batch<std::string> b;
    for (const auto& s : xs)
        b.emplace(s);
    return b;
}

struct LiveTable {
    std::string name;
    explicit LiveTable(std::string n) : name(std::move(n)) {
        run_sql("DROP TABLE IF EXISTS " + name);
        run_sql("CREATE TABLE " + name + " (id int primary key, val text)");
    }
    ~LiveTable() { run_sql("DROP TABLE IF EXISTS " + name); }
};

#define REQUIRE_LIVE_PG()                                                            \
    do {                                                                             \
        if (!pg_configured())                                                        \
            GTEST_SKIP() << "set CLINK_POSTGRES_CDC_TEST_DSN";                       \
        if (!two_phase_enabled())                                                    \
            GTEST_SKIP() << "server has max_prepared_transactions=0 (2PC disabled)"; \
    } while (0)

TEST(PostgresSink2PCLive, CommitRoundTrips) {
    REQUIRE_LIVE_PG();
    LiveTable t("pg2pc_commit_" + uniq());
    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{77}, "pg2pc", &state, nullptr);
    auto sink = make_sink(t.name, rctx);

    sink->open();
    sink->on_data(rows({R"({"id":1,"val":"a"})", R"({"id":2,"val":"b"})"}));
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    // Prepared, not yet committed: rows are invisible.
    EXPECT_EQ(scalar("SELECT COUNT(*) FROM " + t.name), "0");
    EXPECT_EQ(scalar("SELECT COUNT(*) FROM pg_prepared_xacts WHERE gid='clink_pg2pc_sub0_1'"), "1");

    sink->on_commit(1);
    EXPECT_EQ(scalar("SELECT COUNT(*) FROM " + t.name), "2");
    EXPECT_EQ(scalar("SELECT val FROM " + t.name + " WHERE id=2"), "b");
    EXPECT_EQ(scalar("SELECT COUNT(*) FROM pg_prepared_xacts WHERE gid='clink_pg2pc_sub0_1'"), "0");
    sink->close();
}

TEST(PostgresSink2PCLive, PreparedTransactionSurvivesCrashAndRecovers) {
    REQUIRE_LIVE_PG();
    LiveTable t("pg2pc_recover_" + uniq());
    InMemoryStateBackend state;  // shared across the two sink lifetimes (survives the "crash")
    RuntimeContext rctx(OperatorId{77}, "pg2pc", &state, nullptr);

    {
        auto crashed = make_sink(t.name, rctx);
        crashed->open();
        crashed->on_data(rows({R"({"id":10,"val":"x"})"}));
        crashed->on_barrier(CheckpointBarrier{CheckpointId{1}});
        // No on_commit - process "crashes". The prepared transaction persists on
        // the server independent of the session; the gid is durable in state.
    }
    EXPECT_EQ(scalar("SELECT COUNT(*) FROM pg_prepared_xacts WHERE gid='clink_pg2pc_sub0_1'"), "1");
    EXPECT_EQ(scalar("SELECT COUNT(*) FROM " + t.name), "0");

    // Restart: a fresh sink over the same state COMMIT PREPAREs the orphan at open.
    auto restarted = make_sink(t.name, rctx);
    restarted->open();
    EXPECT_EQ(scalar("SELECT COUNT(*) FROM " + t.name), "1");
    EXPECT_EQ(scalar("SELECT val FROM " + t.name + " WHERE id=10"), "x");
    EXPECT_EQ(scalar("SELECT COUNT(*) FROM pg_prepared_xacts WHERE gid='clink_pg2pc_sub0_1'"), "0");
    restarted->close();
}

TEST(PostgresSink2PCLive, OrphanPreparedTransactionRolledBackAtOpen) {
    REQUIRE_LIVE_PG();
    LiveTable t("pg2pc_orphan_" + uniq());

    // sink1 prepares under a shared state, then "crashes" BEFORE its checkpoint
    // snapshot became durable - modelled by giving the restart a FRESH (empty)
    // state, so the prepared gid is not in the restored pending set.
    {
        InMemoryStateBackend s1;
        RuntimeContext r1(OperatorId{77}, "pg2pc", &s1, nullptr);
        auto sink1 = make_sink(t.name, r1);
        sink1->open();
        sink1->on_data(rows({R"({"id":99,"val":"z"})"}));
        sink1->on_barrier(CheckpointBarrier{CheckpointId{1}});
    }
    EXPECT_EQ(scalar("SELECT COUNT(*) FROM pg_prepared_xacts WHERE gid='clink_pg2pc_sub0_1'"), "1");

    // Restart from a checkpoint that predates the prepare (empty pending set):
    // reconcile ROLLBACK PREPAREs the orphan; the row never lands.
    InMemoryStateBackend s2;
    RuntimeContext r2(OperatorId{77}, "pg2pc", &s2, nullptr);
    auto sink2 = make_sink(t.name, r2);
    sink2->open();
    EXPECT_EQ(scalar("SELECT COUNT(*) FROM pg_prepared_xacts WHERE gid='clink_pg2pc_sub0_1'"), "0");
    EXPECT_EQ(scalar("SELECT COUNT(*) FROM " + t.name), "0");
    sink2->close();
}

TEST(PostgresSink2PCLive, CommitIsIdempotent) {
    REQUIRE_LIVE_PG();
    LiveTable t("pg2pc_idem_" + uniq());
    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{77}, "pg2pc", &state, nullptr);
    auto sink = make_sink(t.name, rctx);

    sink->open();
    sink->on_data(rows({R"({"id":5,"val":"only"})"}));
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    sink->on_commit(1);
    EXPECT_NO_THROW(sink->on_commit(1));  // second (recovery-time) commit is a no-op
    EXPECT_EQ(scalar("SELECT COUNT(*) FROM " + t.name), "1");
    sink->close();
}

TEST(PostgresSink2PCLive, EmptyIntervalPreparesNothing) {
    REQUIRE_LIVE_PG();
    LiveTable t("pg2pc_empty_" + uniq());
    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{77}, "pg2pc", &state, nullptr);
    auto sink = make_sink(t.name, rctx);

    sink->open();
    // No data this interval -> prepare_commit returns nullopt -> no prepared txn.
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    EXPECT_EQ(scalar("SELECT COUNT(*) FROM pg_prepared_xacts WHERE gid='clink_pg2pc_sub0_1'"), "0");
    EXPECT_NO_THROW(sink->on_commit(1));
    sink->close();
}

}  // namespace

#else  // !CLINK_HAS_POSTGRES

TEST(PostgresSink2PC, SkippedWithoutLibpq) {
    GTEST_SKIP() << "built without libpq";
}

#endif  // CLINK_HAS_POSTGRES
