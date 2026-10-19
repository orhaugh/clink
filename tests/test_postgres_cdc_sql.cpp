// The PostgreSQL CDC contract through the SQL frontend, against a real server:
// a CDC table in changelog_mode='upsert' maintaining a PostgreSQL table
// through the upsert sink, while the source table takes inserts, updates,
// deletes and a key change. The source table is the oracle: once caught up,
// the replica holds exactly its rows. The job checkpoints, and the slot must
// advance as checkpoints complete, which is the completion notice reaching
// the source through the embedded runtime. SKIPPED unless
// CLINK_POSTGRES_CDC_TEST_DSN names a server with wal_level=logical.

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <libpq-fe.h>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>

#include <gtest/gtest.h>

#include "clink/embed/embedded_engine.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/postgres/install.hpp"

namespace {

using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

const char* dsn_env() {
    return std::getenv("CLINK_POSTGRES_CDC_TEST_DSN");
}

std::string query(const std::string& dsn, const std::string& sql) {
    PGconn* c = PQconnectdb(dsn.c_str());
    if (PQstatus(c) != CONNECTION_OK) {
        const std::string err = PQerrorMessage(c);
        PQfinish(c);
        throw std::runtime_error("connect: " + err);
    }
    PGresult* r = PQexec(c, sql.c_str());
    const auto st = PQresultStatus(r);
    if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK) {
        const std::string err = PQerrorMessage(c);
        PQclear(r);
        PQfinish(c);
        throw std::runtime_error(sql.substr(0, 100) + " -> " + err);
    }
    std::string out;
    if (st == PGRES_TUPLES_OK && PQntuples(r) > 0 && PQgetisnull(r, 0, 0) == 0) {
        out = PQgetvalue(r, 0, 0);
    }
    PQclear(r);
    PQfinish(c);
    return out;
}

void ensure_installed() {
    static const bool done = [] {
        {
            clink::embed::EngineOptions opts;
            std::ostringstream sink;
            opts.out = &sink;
            opts.err = &sink;
            const clink::embed::EmbeddedEngine first{std::move(opts)};
        }
        clink::plugin::PluginRegistry reg;
        clink::postgres::install(reg);
        return true;
    }();
    (void)done;
}

std::uint64_t lsn(const std::string& text) {
    const auto slash = text.find('/');
    if (slash == std::string::npos) {
        return 0;
    }
    return (std::stoull(text.substr(0, slash), nullptr, 16) << 32) |
           std::stoull(text.substr(slash + 1), nullptr, 16);
}

}  // namespace

TEST(PostgresCdcSql, AnUpsertChangelogKeepsAReplicaEqualToItsSource) {
    if (dsn_env() == nullptr) {
        GTEST_SKIP() << "set CLINK_POSTGRES_CDC_TEST_DSN (docker/integration-services.yml)";
    }
    ensure_installed();
    const std::string dsn = dsn_env();
    const std::string tag = std::to_string(::getpid());
    const std::string src = "orders_" + tag;
    const std::string dst = "replica_" + tag;
    const std::string slot = "slot_sql_" + tag;
    const std::string pub = "pub_sql_" + tag;
    const std::string columns =
        "(id bigint PRIMARY KEY, status text, amount numeric(12, 2), flag boolean, day date)";
    for (const auto& t : {src, dst}) {
        query(dsn, "DROP TABLE IF EXISTS " + t);
        query(dsn, "CREATE TABLE " + t + " " + columns);
    }
    query(dsn, "ALTER TABLE " + src + " REPLICA IDENTITY FULL");
    query(dsn, "DROP PUBLICATION IF EXISTS " + pub);
    query(dsn, "CREATE PUBLICATION " + pub + " FOR TABLE " + src);
    query(dsn,
          "INSERT INTO " + src +
              " SELECT g, 'new', g * 1.5, g % 2 = 0, date '2026-01-01' + g FROM "
              "generate_series(1, 300) g");

    const auto ckpt = std::filesystem::temp_directory_path() / ("clink_pgcdc_sql_" + tag);
    std::filesystem::remove_all(ckpt);
    std::ostringstream log;
    clink::embed::EngineOptions opts;
    opts.out = &log;
    opts.err = &log;
    opts.checkpoint_dir = ckpt.string();
    opts.checkpoint_interval_ms = 300;
    auto engine = std::make_unique<clink::embed::EmbeddedEngine>(std::move(opts));
    const std::string sql_columns =
        "(id BIGINT PRIMARY KEY, status VARCHAR, amount DECIMAL(12, 2), flag BOOLEAN, day DATE)";
    const std::string script =
        "CREATE TABLE orders_cdc " + sql_columns +
        " WITH (connector='postgres', format='json', mode='cdc', changelog_mode='upsert', "
        "conninfo='" +
        dsn + "', slot_name='" + slot + "', publication_names='" + pub + "', \"table\"='public." +
        src +
        "');"
        "CREATE TABLE replica " +
        sql_columns + " WITH (connector='postgres', format='json', mode='upsert', conninfo='" +
        dsn + "', \"table\"='public." + dst +
        "');"
        "INSERT INTO replica SELECT id, status, amount, flag, day FROM orders_cdc;";
    ASSERT_EQ(engine->execute_script(script), 0) << log.str();

    // The workload, while the snapshot and the stream run.
    query(dsn, "UPDATE " + src + " SET status = 'paid', amount = amount + 1 WHERE id <= 100");
    query(dsn, "DELETE FROM " + src + " WHERE id BETWEEN 150 AND 170");
    query(dsn, "UPDATE " + src + " SET id = id + 10000 WHERE id BETWEEN 200 AND 205");
    query(dsn,
          "INSERT INTO " + src +
              " SELECT g, 'late', g * 2.25, true, date '2026-06-01' FROM generate_series(400, "
              "450) g");
    query(dsn, "UPDATE " + src + " SET status = NULL, flag = NULL, day = NULL WHERE id = 401");

    const std::string differ = "SELECT (SELECT count(*) FROM (SELECT * FROM " + src +
                               " EXCEPT SELECT * FROM " + dst + ") a) + (SELECT count(*) FROM " +
                               "(SELECT * FROM " + dst + " EXCEPT SELECT * FROM " + src + ") b)";
    const auto deadline = Clock::now() + 90s;
    while (query(dsn, differ) != "0") {
        ASSERT_LT(Clock::now(), deadline) << "the replica never matched the source:\n" << log.str();
        std::this_thread::sleep_for(200ms);
    }
    EXPECT_EQ(query(dsn, "SELECT count(*) FROM " + dst), query(dsn, "SELECT count(*) FROM " + src));
    EXPECT_EQ(query(dsn, "SELECT count(*) FROM " + dst + " WHERE id BETWEEN 150 AND 170"), "0");
    EXPECT_EQ(query(dsn, "SELECT count(*) FROM " + dst + " WHERE id BETWEEN 200 AND 205"), "0");
    EXPECT_EQ(query(dsn, "SELECT count(*) FROM " + dst + " WHERE id BETWEEN 10200 AND 10205"), "6");

    // Completed checkpoints confirm the slot: it moves past the position the
    // job started at.
    const std::uint64_t created = lsn(query(
        dsn,
        "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '" + slot + "'"));
    query(dsn, "INSERT INTO " + src + " VALUES (999, 'tail', 1, true, date '2026-01-02')");
    const auto confirm_deadline = Clock::now() + 30s;
    while (lsn(query(dsn,
                     "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '" +
                         slot + "'")) <= created) {
        ASSERT_LT(Clock::now(), confirm_deadline) << "no completed checkpoint confirmed the slot";
        std::this_thread::sleep_for(200ms);
    }

    engine->cancel_all();
    engine.reset();
    for (int i = 0; i < 50; ++i) {
        if (query(dsn,
                  "SELECT active FROM pg_replication_slots WHERE slot_name = '" + slot + "'") !=
            "t") {
            break;
        }
        std::this_thread::sleep_for(100ms);
    }
    query(dsn,
          "SELECT pg_drop_replication_slot('" + slot +
              "') FROM pg_replication_slots WHERE "
              "slot_name = '" +
              slot + "'");
    query(dsn, "DROP PUBLICATION IF EXISTS " + pub);
    query(dsn, "DROP TABLE IF EXISTS " + src);
    query(dsn, "DROP TABLE IF EXISTS " + dst);
    std::filesystem::remove_all(ckpt);
}
