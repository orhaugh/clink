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
#include <vector>

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

// keep_rows='true' under REPLICA IDENTITY DEFAULT: one job keeps a per-status
// grouping in PostgreSQL, another a whole-row replica, while the source table
// takes updates that leave a large (out-of-line) value unchanged, rows moving
// between groups, a key change, deletes and then a TRUNCATE. The grouping needs
// every update's old row and every delete's whole row, the replica needs the
// unchanged values filled in and the TRUNCATE expanded; the source table is
// the oracle for both. A group whose rows have all gone stays with n = 0 (the
// grouping's own behaviour), so groups are compared where n > 0.
TEST(PostgresCdcSql, KeptRowsLetAGroupingAndAReplicaFollowTheTable) {
    if (dsn_env() == nullptr) {
        GTEST_SKIP() << "set CLINK_POSTGRES_CDC_TEST_DSN (docker/integration-services.yml)";
    }
    ensure_installed();
    const std::string dsn = dsn_env();
    const std::string tag = std::to_string(::getpid());
    const std::string src = "kept_" + tag;
    const std::string totals = "totals_" + tag;
    const std::string replica = "kept_replica_" + tag;
    const std::string pub = "pub_kept_" + tag;
    const std::string slot_totals = "slot_kept_totals_" + tag;
    const std::string slot_rows = "slot_kept_rows_" + tag;
    const std::string columns =
        "(id bigint PRIMARY KEY, status text, amount numeric(12, 2), body text)";
    for (const auto& t : {src, replica}) {
        query(dsn, "DROP TABLE IF EXISTS " + t);
        query(dsn, "CREATE TABLE " + t + " " + columns);
    }
    query(dsn, "DROP TABLE IF EXISTS " + totals);
    query(dsn,
          "CREATE TABLE " + totals + " (status text PRIMARY KEY, n bigint, total numeric(14, 2))");
    query(dsn, "DROP PUBLICATION IF EXISTS " + pub);
    query(dsn, "CREATE PUBLICATION " + pub + " FOR TABLE " + src);
    // Every third row carries a body PostgreSQL stores out of line.
    query(dsn,
          "INSERT INTO " + src +
              " SELECT g, 's' || (g % 4), g * 1.5, CASE WHEN g % 3 = 0 THEN (SELECT "
              "string_agg(md5((g * 1000 + i)::text), '') FROM generate_series(1, 1000) i) ELSE "
              "'short ' || g END FROM generate_series(1, 200) g");

    // One engine and checkpoint directory per job: jobs that share a
    // checkpoint directory share their subtasks' state directories.
    std::ostringstream log;
    std::vector<std::filesystem::path> ckpts;
    std::vector<std::unique_ptr<clink::embed::EmbeddedEngine>> engines;
    const auto run_job = [&](const std::string& script) {
        const auto ckpt = std::filesystem::temp_directory_path() /
                          ("clink_pgcdc_kept_" + tag + "_" + std::to_string(ckpts.size()));
        std::filesystem::remove_all(ckpt);
        ckpts.push_back(ckpt);
        clink::embed::EngineOptions opts;
        opts.out = &log;
        opts.err = &log;
        opts.checkpoint_dir = ckpt.string();
        opts.checkpoint_interval_ms = 300;
        engines.push_back(std::make_unique<clink::embed::EmbeddedEngine>(std::move(opts)));
        return engines.back()->execute_script(script);
    };
    const std::string cdc_columns =
        "(id BIGINT PRIMARY KEY, status VARCHAR, amount DECIMAL(12, 2), body VARCHAR)";
    const auto cdc_table = [&](const std::string& name, const std::string& slot) {
        return "CREATE TABLE " + name + " " + cdc_columns +
               " WITH (connector='postgres', format='json', mode='cdc', changelog_mode='upsert', "
               "keep_rows='true', conninfo='" +
               dsn + "', slot_name='" + slot + "', publication_names='" + pub +
               "', \"table\"='public." + src + "');";
    };
    ASSERT_EQ(run_job(cdc_table("all_rows", slot_rows) + "CREATE TABLE replica " + cdc_columns +
                      " WITH (connector='postgres', format='json', mode='upsert', conninfo='" +
                      dsn + "', \"table\"='public." + replica +
                      "');"
                      "INSERT INTO replica SELECT id, status, amount, body FROM all_rows;"),
              0)
        << log.str();
    // The grouping keeps one accumulator per status for the life of the job,
    // which the planner admits when asked.
    ASSERT_EQ(run_job(cdc_table("by_status", slot_totals) +
                      "CREATE TABLE totals (status VARCHAR PRIMARY KEY, n BIGINT, total "
                      "DECIMAL(14, 2)) WITH (connector='postgres', format='json', mode='upsert', "
                      "conninfo='" +
                      dsn + "', \"table\"='public." + totals +
                      "');"
                      "INSERT INTO totals SELECT status, COUNT(*) AS n, SUM(amount) AS total "
                      "FROM by_status GROUP BY status ALLOW UNBOUNDED STATE;"),
              0)
        << log.str();

    const std::string grouping =
        "SELECT status, count(*) AS n, sum(amount) AS total FROM " + src + " GROUP BY status";
    const std::string kept_groups = "SELECT status, n, total FROM " + totals + " WHERE n > 0";
    const std::string differ =
        "SELECT (SELECT count(*) FROM (" + grouping + " EXCEPT " + kept_groups +
        ") a) + (SELECT count(*) FROM (" + kept_groups + " EXCEPT " + grouping +
        ") b) + (SELECT count(*) FROM (SELECT * FROM " + src + " EXCEPT SELECT * FROM " + replica +
        ") c) + (SELECT count(*) FROM (SELECT * FROM " + replica + " EXCEPT SELECT * FROM " + src +
        ") d)";
    const auto converge = [&](const char* what) {
        const auto deadline = Clock::now() + 90s;
        while (query(dsn, differ) != "0") {
            ASSERT_LT(Clock::now(), deadline) << what << ": the grouping or the replica never "
                                              << "matched the table:\n"
                                              << log.str();
            std::this_thread::sleep_for(200ms);
        }
    };

    // The workload starts once the snapshot has reached both targets, so its
    // changes arrive through the stream.
    const auto snapshot_deadline = Clock::now() + 60s;
    while (query(dsn, "SELECT count(*) FROM " + replica) != "200" ||
           query(dsn, "SELECT coalesce(sum(n), 0) FROM " + totals) != "200") {
        ASSERT_LT(Clock::now(), snapshot_deadline) << "the snapshot never arrived:\n" << log.str();
        std::this_thread::sleep_for(100ms);
    }

    // Rows move between groups with their large bodies unchanged, so under
    // REPLICA IDENTITY DEFAULT those updates do not carry the body.
    query(dsn, "UPDATE " + src + " SET status = 's9' WHERE id % 5 = 0");
    query(dsn, "UPDATE " + src + " SET amount = amount + 1 WHERE id % 7 = 0");
    query(dsn, "DELETE FROM " + src + " WHERE id % 11 = 0");
    // A key change, the body unchanged: the new key's row takes it from the old one.
    query(dsn, "UPDATE " + src + " SET id = id + 1000 WHERE id BETWEEN 51 AND 57");
    converge("after the updates");
    EXPECT_EQ(
        query(dsn, "SELECT count(*) FROM " + replica + " WHERE id > 1000 AND length(body) > 30000"),
        "3");  // 1051, 1054 and 1057 kept their out-of-line bodies

    query(dsn, "TRUNCATE " + src);
    query(dsn,
          "INSERT INTO " + src +
              " SELECT g, 'after', g * 2.0, 'new ' || g FROM generate_series(1, 20) g");
    converge("after the TRUNCATE");
    EXPECT_EQ(query(dsn, "SELECT count(*) FROM " + replica), "20");

    for (auto& e : engines) {
        e->cancel_all();
        e.reset();
    }
    for (const auto& slot : {slot_totals, slot_rows}) {
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
                  "') FROM pg_replication_slots WHERE slot_name = '" + slot + "'");
    }
    query(dsn, "DROP PUBLICATION IF EXISTS " + pub);
    for (const auto& t : {src, totals, replica}) {
        query(dsn, "DROP TABLE IF EXISTS " + t);
    }
    for (const auto& c : ckpts) {
        std::filesystem::remove_all(c);
    }
}

// keep_rows on a slot that already exists: a run that starts with no
// checkpoint of its own on a slot an earlier run left behind takes the
// snapshot fresh and replays every change since the slot's confirmed position,
// the ones the snapshot already holds included. Among them are updates,
// leaving a large value unchanged, of rows deleted before the snapshot was
// read: the job holds no row for them, and must pass over them rather than
// stop.
TEST(PostgresCdcSql, KeptRowsConvergeAfterASnapshotTakenFresh) {
    if (dsn_env() == nullptr) {
        GTEST_SKIP() << "set CLINK_POSTGRES_CDC_TEST_DSN (docker/integration-services.yml)";
    }
    ensure_installed();
    const std::string dsn = dsn_env();
    const std::string tag = std::to_string(::getpid());
    const std::string src = "fresh_" + tag;
    const std::string replica = "fresh_replica_" + tag;
    const std::string pub = "pub_fresh_" + tag;
    const std::string slot = "slot_fresh_" + tag;
    const std::string columns =
        "(id bigint PRIMARY KEY, status text, amount numeric(12, 2), body text)";
    for (const auto& t : {src, replica}) {
        query(dsn, "DROP TABLE IF EXISTS " + t);
        query(dsn, "CREATE TABLE " + t + " " + columns);
    }
    query(dsn, "DROP PUBLICATION IF EXISTS " + pub);
    query(dsn, "CREATE PUBLICATION " + pub + " FOR TABLE " + src);
    query(dsn,
          "INSERT INTO " + src +
              " SELECT g, 'new', g * 1.5, (SELECT string_agg(md5((g * 1000 + i)::text), '') FROM "
              "generate_series(1, 1000) i) FROM generate_series(1, 100) g");
    const std::string sql_columns =
        "(id BIGINT PRIMARY KEY, status VARCHAR, amount DECIMAL(12, 2), body VARCHAR)";
    const std::string script =
        "CREATE TABLE rows_cdc " + sql_columns +
        " WITH (connector='postgres', format='json', mode='cdc', changelog_mode='upsert', "
        "keep_rows='true', conninfo='" +
        dsn + "', slot_name='" + slot + "', publication_names='" + pub + "', \"table\"='public." +
        src +
        "');"
        "CREATE TABLE replica " +
        sql_columns + " WITH (connector='postgres', format='json', mode='upsert', conninfo='" +
        dsn + "', \"table\"='public." + replica +
        "');"
        "INSERT INTO replica SELECT id, status, amount, body FROM rows_cdc;";
    const std::string differ = "SELECT (SELECT count(*) FROM (SELECT * FROM " + src +
                               " EXCEPT SELECT * FROM " + replica +
                               ") a) + (SELECT count(*) FROM " + "(SELECT * FROM " + replica +
                               " EXCEPT SELECT * FROM " + src + ") b)";
    std::ostringstream log;
    const auto wait_equal = [&](const char* what) {
        const auto deadline = Clock::now() + 90s;
        while (query(dsn, differ) != "0") {
            ASSERT_LT(Clock::now(), deadline) << what << ": the replica never matched:\n"
                                              << log.str();
            std::this_thread::sleep_for(200ms);
        }
    };
    const auto slot_idle = [&] {
        for (int i = 0; i < 100; ++i) {
            if (query(dsn,
                      "SELECT active FROM pg_replication_slots WHERE slot_name = '" + slot + "'") !=
                "t") {
                return;
            }
            std::this_thread::sleep_for(100ms);
        }
    };
    std::vector<std::filesystem::path> ckpts;
    const auto engine_options = [&] {
        const auto ckpt = std::filesystem::temp_directory_path() /
                          ("clink_pgcdc_fresh_" + tag + "_" + std::to_string(ckpts.size()));
        std::filesystem::remove_all(ckpt);
        ckpts.push_back(ckpt);
        clink::embed::EngineOptions opts;
        opts.out = &log;
        opts.err = &log;
        opts.checkpoint_dir = ckpt.string();
        opts.checkpoint_interval_ms = 300;
        return opts;
    };

    {
        clink::embed::EmbeddedEngine first{engine_options()};
        ASSERT_EQ(first.execute_script(script), 0) << log.str();
        wait_equal("the first run");
        first.cancel_all();
    }
    slot_idle();
    // The slot outlives the run: the next run takes it over.
    EXPECT_EQ(
        query(dsn, "SELECT count(*) FROM pg_replication_slots WHERE slot_name = '" + slot + "'"),
        "1");
    // While no job runs: updates that leave the large body unchanged, then
    // deletes of some of those rows, and a key change. Replayed after a fresh
    // snapshot, the deleted rows' updates are of rows the snapshot lacks.
    query(dsn, "UPDATE " + src + " SET status = 'paid' WHERE id % 2 = 0");
    query(dsn, "DELETE FROM " + src + " WHERE id % 4 = 0");
    query(dsn, "UPDATE " + src + " SET amount = amount + 1 WHERE id % 3 = 0");
    query(dsn, "UPDATE " + src + " SET id = id + 1000 WHERE id BETWEEN 10 AND 13");

    // A run with a checkpoint directory of its own, so nothing to restore: a
    // fresh snapshot on the existing slot, then the whole replay.
    auto second = std::make_unique<clink::embed::EmbeddedEngine>(engine_options());
    ASSERT_EQ(second->execute_script(script), 0) << log.str();
    wait_equal("the second run");
    // Changes past the snapshot's position still apply.
    query(dsn, "UPDATE " + src + " SET status = 'late' WHERE id % 5 = 0");
    wait_equal("after the replay");

    second->cancel_all();
    second.reset();
    slot_idle();
    query(dsn,
          "SELECT pg_drop_replication_slot('" + slot +
              "') FROM pg_replication_slots WHERE slot_name = '" + slot + "'");
    query(dsn, "DROP PUBLICATION IF EXISTS " + pub);
    for (const auto& t : {src, replica}) {
        query(dsn, "DROP TABLE IF EXISTS " + t);
    }
    for (const auto& c : ckpts) {
        std::filesystem::remove_all(c);
    }
}

// Several tables on one replication slot: an EXECUTE STATEMENT SET keeps a
// replica of each from one source and one walsender, the source routing each
// table's rows to its own branch, while transactions touch both. Checkpoints
// complete through the routed branches, so the slot is confirmed.
TEST(PostgresCdcSql, TablesOnOneSlotFeedTheirTargetsFromOneSource) {
    if (dsn_env() == nullptr) {
        GTEST_SKIP() << "set CLINK_POSTGRES_CDC_TEST_DSN (docker/integration-services.yml)";
    }
    ensure_installed();
    const std::string dsn = dsn_env();
    const std::string tag = std::to_string(::getpid());
    const std::string orders = "shared_orders_" + tag;
    const std::string customers = "shared_customers_" + tag;
    const std::string pub = "pub_shared_" + tag;
    const std::string slot = "slot_shared_" + tag;
    const std::string order_cols =
        "(id bigint PRIMARY KEY, customer_id bigint, amount numeric(12, 2))";
    const std::string customer_cols = "(id bigint PRIMARY KEY, name text)";
    for (const auto& [t, cols] :
         std::vector<std::pair<std::string, std::string>>{{orders, order_cols},
                                                          {orders + "_r", order_cols},
                                                          {customers, customer_cols},
                                                          {customers + "_r", customer_cols}}) {
        query(dsn, "DROP TABLE IF EXISTS " + t);
        query(dsn, "CREATE TABLE " + t + " " + cols);
    }
    query(dsn, "DROP PUBLICATION IF EXISTS " + pub);
    query(dsn, "CREATE PUBLICATION " + pub + " FOR TABLE " + orders + ", " + customers);
    query(dsn, "INSERT INTO " + customers + " SELECT g, 'c' || g FROM generate_series(1, 50) g");
    query(
        dsn,
        "INSERT INTO " + orders + " SELECT g, g % 50 + 1, g * 1.5 FROM generate_series(1, 200) g");

    const auto ckpt = std::filesystem::temp_directory_path() / ("clink_pgcdc_shared_" + tag);
    std::filesystem::remove_all(ckpt);
    std::ostringstream log;
    clink::embed::EngineOptions opts;
    opts.out = &log;
    opts.err = &log;
    opts.checkpoint_dir = ckpt.string();
    opts.checkpoint_interval_ms = 300;
    auto engine = std::make_unique<clink::embed::EmbeddedEngine>(std::move(opts));
    const std::string cdc =
        " WITH (connector='postgres', format='json', mode='cdc', "
        "changelog_mode='upsert', conninfo='" +
        dsn + "', slot_name='" + slot + "', publication_names='" + pub + "', \"table\"='public.";
    const std::string upsert =
        " WITH (connector='postgres', format='json', mode='upsert', "
        "conninfo='" +
        dsn + "', \"table\"='public.";
    const std::string order_sql =
        "(id BIGINT PRIMARY KEY, customer_id BIGINT, amount DECIMAL(12, 2))";
    const std::string customer_sql = "(id BIGINT PRIMARY KEY, name VARCHAR)";
    const std::string script =
        "CREATE TABLE orders_cdc " + order_sql + cdc + orders + "');" +
        "CREATE TABLE customers_cdc " + customer_sql + cdc + customers + "');" +
        "CREATE TABLE orders_r " + order_sql + upsert + orders + "_r');" +
        "CREATE TABLE customers_r " + customer_sql + upsert + customers + "_r');" +
        "EXECUTE STATEMENT SET BEGIN "
        "INSERT INTO orders_r SELECT id, customer_id, amount FROM orders_cdc;"
        "INSERT INTO customers_r SELECT id, name FROM customers_cdc;"
        "END;";
    ASSERT_EQ(engine->execute_script(script), 0) << log.str();

    const auto differ = [&](const std::string& t) {
        return "SELECT (SELECT count(*) FROM (SELECT * FROM " + t + " EXCEPT SELECT * FROM " + t +
               "_r) a) + (SELECT count(*) FROM (SELECT * FROM " + t + "_r EXCEPT SELECT * FROM " +
               t + ") b)";
    };
    const auto converge = [&](const char* what) {
        const auto deadline = Clock::now() + 90s;
        while (query(dsn, differ(orders)) != "0" || query(dsn, differ(customers)) != "0") {
            ASSERT_LT(Clock::now(), deadline) << what << ": a replica never matched:\n"
                                              << log.str();
            std::this_thread::sleep_for(200ms);
        }
    };
    converge("the snapshot");
    // Transactions across both tables.
    query(dsn,
          "BEGIN; UPDATE " + customers + " SET name = name || '!' WHERE id % 3 = 0; UPDATE " +
              orders + " SET amount = amount + 10 WHERE customer_id % 3 = 0; COMMIT");
    query(dsn, "DELETE FROM " + orders + " WHERE id % 7 = 0");
    query(dsn, "INSERT INTO " + customers + " VALUES (51, 'new')");
    query(dsn, "INSERT INTO " + orders + " VALUES (201, 51, 9.99)");
    converge("the stream");
    // One source: one walsender reads the slot.
    EXPECT_EQ(query(dsn,
                    "SELECT count(*) FROM pg_stat_replication WHERE application_name = "
                    "'clink_cdc_" +
                        slot + "'"),
              "1");
    // Checkpoints complete through the routed branches and confirm the slot.
    const auto confirmed = [&] {
        return lsn(
            query(dsn,
                  "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '" +
                      slot + "'"));
    };
    const std::uint64_t before = confirmed();
    query(dsn, "UPDATE " + customers + " SET name = 'tail' WHERE id = 1");
    const auto confirm_deadline = Clock::now() + 30s;
    while (confirmed() <= before) {
        ASSERT_LT(Clock::now(), confirm_deadline) << "no completed checkpoint confirmed the slot\n"
                                                  << log.str();
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
              "') FROM pg_replication_slots WHERE slot_name = '" + slot + "'");
    query(dsn, "DROP PUBLICATION IF EXISTS " + pub);
    for (const auto& t : {orders, orders + "_r", customers, customers + "_r"}) {
        query(dsn, "DROP TABLE IF EXISTS " + t);
    }
    std::filesystem::remove_all(ckpt);
}
