// The PostgreSQL CDC source with keep_rows='true' in append mode, into a
// ClickHouse change log and the ReplacingMergeTree(__version, __deleted) a
// materialised view keeps from it, across a column added the documented way
// (docs/connectors/postgres.md, "Adding a column"): the job is stopped with
// `clink stop`, the column is added to the PostgreSQL table, where its default
// gives every existing row a value without a change, and to the ClickHouse
// tables, and the job is resubmitted declaring it, restoring from the
// checkpoint the stop printed. The restored source reads the table again (a
// backfill), and the stage that keeps the rows gives each row it refreshes a
// __version above the row already in the change log for that key.
//
// The failure this pins: the backfilled row took the __version of the row it
// refreshed. ClickHouse orders neither ReplacingMergeTree's merge nor argMax
// between rows of equal version, so the documented rebuild could keep the row
// without the column. The checks below find two different rows under one
// version of a key directly, rather than waiting for a query to pick the wrong
// one.
//
// Pass criteria, once the backfill has run and again after later updates and
// deletes:
//   * no key has two different rows under one __version in the change log;
//   * the table's state, read from the ReplacingMergeTree with FINAL and
//     rebuilt from the change log with argMax over __version, equals SELECT
//     over the PostgreSQL table, every column of every row, the added column
//     included on rows no change touched since it was added.
// The second test kills the worker running the source in the middle of the
// backfill, after a checkpoint completed in it, and checks the same.
//
// Needs Docker, and the clink_node, clink_submit_sql and clink binaries of a
// build with fault injection compiled in. Each test prints "[pg-cdc]" lines.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <libpq-fe.h>
#include <memory>
#include <optional>
#include <regex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/fault/fault_injection.hpp"

#include "tests/integration/cluster_harness.hpp"
#include "tests/integration/docker_clickhouse.hpp"
#include "tests/integration/docker_postgres.hpp"

namespace {

using namespace std::chrono_literals;
using clink::itest::Cluster;
using clink::itest::ClusterSpec;
using clink::itest::Process;
using clink::itest::ProcOptions;
using clink::itest::ScopedDiagnostics;
using clink::test::DockerClickHouse;
using clink::test::DockerPostgres;
using Clock = std::chrono::steady_clock;

std::filesystem::path node_binary() {
#ifdef CLINK_NODE_BINARY
    return std::filesystem::path{CLINK_NODE_BINARY};
#else
    return {};
#endif
}

std::filesystem::path sql_binary() {
#ifdef CLINK_SUBMIT_SQL_BINARY
    return std::filesystem::path{CLINK_SUBMIT_SQL_BINARY};
#else
    return {};
#endif
}

std::filesystem::path cli_binary() {
#ifdef CLINK_CLI_BINARY
    return std::filesystem::path{CLINK_CLI_BINARY};
#else
    return {};
#endif
}

constexpr std::int64_t kSeedRows = 3000;

// Every row of a query's result, columns joined with '|', NULL as \N.
std::vector<std::string> pg_rows(const std::string& conninfo, const std::string& sql) {
    PGconn* c = PQconnectdb(conninfo.c_str());
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
        throw std::runtime_error(sql.substr(0, 120) + " -> " + err);
    }
    std::vector<std::string> out;
    for (int i = 0; i < PQntuples(r); ++i) {
        std::string row;
        for (int j = 0; j < PQnfields(r); ++j) {
            row += (j > 0 ? "|" : "");
            row += PQgetisnull(r, i, j) != 0 ? "\\N" : PQgetvalue(r, i, j);
        }
        out.push_back(std::move(row));
    }
    PQclear(r);
    PQfinish(c);
    return out;
}

class PostgresCdcClickHouseBackfill : public ::testing::Test {
protected:
    static void TearDownTestSuite() {
        ch_.reset();
        pg_.reset();
    }

    void SetUp() override {
        if (!DockerPostgres::docker_available()) {
            GTEST_SKIP() << "Docker not available; skipping the CDC backfill into ClickHouse";
        }
        if (!std::filesystem::exists(node_binary()) || !std::filesystem::exists(sql_binary()) ||
            !std::filesystem::exists(cli_binary())) {
            GTEST_SKIP() << "the cluster node, SQL submit or CLI binary is not built";
        }
        if (pg_ == nullptr) {
            clink::test::DockerPostgresOptions o;
            o.postgres_args = {"-c",
                               "wal_level=logical",
                               "-c",
                               "max_wal_senders=10",
                               "-c",
                               "max_replication_slots=10",
                               "-c",
                               "wal_sender_timeout=10s"};
            pg_ = std::make_unique<DockerPostgres>(std::move(o));
        }
        if (ch_ == nullptr) {
            ch_ = std::make_unique<DockerClickHouse>(
                clink::test::DockerClickHouseOptions{.line = "26.8", .replicas = 1});
        }
        static int counter = 0;
        const std::string suffix = std::to_string(::getpid()) + "_" + std::to_string(++counter);
        table_ = "orders_" + suffix;
        slot_ = "slot_" + suffix;
        pub_ = "pub_" + suffix;
        log_ = "orders_log_" + suffix;
        state_ = "orders_state_" + suffix;

        exec("CREATE TABLE " + table_ +
             " (id bigint PRIMARY KEY, rev int NOT NULL, status text, amount numeric(12, 2))");
        exec("CREATE PUBLICATION " + pub_ + " FOR TABLE " + table_);
        exec("INSERT INTO " + table_ +
             " SELECT g, 1, 'seed', g * 7 + 1.25 FROM generate_series(1, " +
             std::to_string(kSeedRows) + ") g");

        const std::string columns =
            "(id Int64, rev Nullable(Int32), status Nullable(String), amount Nullable(Decimal(12, "
            "2)), __version UInt64, __deleted UInt8, __op String)";
        ch_->query("CREATE TABLE default." + log_ + " " + columns +
                   " ENGINE = MergeTree ORDER BY (id, __version) SETTINGS async_insert = 0");
        ch_->query("CREATE TABLE default." + state_ + " " + columns +
                   " ENGINE = ReplacingMergeTree(__version, __deleted) ORDER BY id");
        create_view();
    }

    void TearDown() override {
        if (ch_ != nullptr && !log_.empty()) {
            (void)ch_->try_query("DROP VIEW IF EXISTS default." + state_ + "_mv SYNC");
            (void)ch_->try_query("DROP TABLE IF EXISTS default." + state_ + " SYNC");
            (void)ch_->try_query("DROP TABLE IF EXISTS default." + log_ + " SYNC");
        }
        if (pg_ != nullptr && !table_.empty()) {
            try {
                for (int i = 0; i < 100; ++i) {
                    const auto active =
                        rows("SELECT active FROM pg_replication_slots WHERE slot_name = '" + slot_ +
                             "'");
                    if (active.empty()) {
                        break;
                    }
                    if (active[0] == "f") {
                        exec("SELECT pg_drop_replication_slot('" + slot_ + "')");
                        break;
                    }
                    std::this_thread::sleep_for(100ms);
                }
                exec("DROP PUBLICATION IF EXISTS " + pub_);
                exec("DROP TABLE IF EXISTS " + table_);
            } catch (const std::exception&) {
            }
        }
    }

    std::vector<std::string> rows(const std::string& sql) const {
        return pg_rows(pg_->conninfo(), sql);
    }
    void exec(const std::string& sql) const { (void)rows(sql); }

    void create_view() const {
        // SELECT * in a materialised view is expanded when the view is created,
        // so the view is created again once the tables have the added column.
        ch_->query("CREATE MATERIALIZED VIEW default." + state_ + "_mv TO default." + state_ +
                   " AS SELECT * FROM default." + log_);
    }

    // The column added to the ClickHouse tables, while the job is stopped.
    void add_note_to_clickhouse() const {
        ch_->query("DROP VIEW default." + state_ + "_mv SYNC");
        for (const auto& t : {log_, state_}) {
            ch_->query("ALTER TABLE default." + t + " ADD COLUMN note Nullable(String)");
        }
        create_view();
    }

    std::string job_sql(bool with_note) const {
        const std::string rest = std::string{"rev INTEGER, status VARCHAR, amount DECIMAL(12, 2"} +
                                 (with_note ? "), note VARCHAR" : ")") +
                                 ", __version BIGINT, __deleted BOOLEAN, __op VARCHAR";
        return "CREATE TABLE orders_cdc (id BIGINT PRIMARY KEY, " + rest +
               ") WITH (connector='postgres', format='json', mode='cdc', "
               "changelog_mode='append', keep_rows='true', conninfo='" +
               pg_->conninfo() + "', slot_name='" + slot_ + "', publication_names='" + pub_ +
               "', \"table\"='public." + table_ +
               "', snapshot_batch_rows='200', status_interval_ms='1000'); "
               "CREATE TABLE ch (id BIGINT, " +
               rest +
               ") WITH (connector='clickhouse', insert_format='native', host='127.0.0.1', port='" +
               std::to_string(ch_->port(0)) + "', database='default', table='" + log_ +
               "', batch_interval_ms='200'); "
               "INSERT INTO ch SELECT * FROM orders_cdc;";
    }

    void submit(Cluster& cluster, const std::string& sql, const std::vector<std::string>& extra) {
        std::vector<std::string> argv{sql_binary().string(),
                                      "-e",
                                      sql,
                                      "--coordinator-host",
                                      "127.0.0.1",
                                      "--coordinator-port",
                                      std::to_string(cluster.http_port()),
                                      "--checkpoint-dir",
                                      cluster.checkpoint_dir().string(),
                                      "--checkpoint-interval-ms",
                                      "500",
                                      "--max-restarts-on-worker-loss",
                                      "8",
                                      "--parallelism",
                                      "2"};
        argv.insert(argv.end(), extra.begin(), extra.end());
        Process p;
        ASSERT_TRUE(p.spawn("submit-sql-" + std::to_string(++submits_),
                            sql_binary(),
                            std::move(argv),
                            cluster.log_dir()));
        const auto code = p.await_exit(60s);
        ASSERT_TRUE(code.has_value());
        ASSERT_EQ(*code, 0) << p.read_log();
    }

    // `clink stop`: a final checkpoint, then the job finishes. Its id.
    std::string stop(Cluster& cluster, int job) {
        Process p;
        EXPECT_TRUE(p.spawn("stop",
                            cli_binary(),
                            {cli_binary().string(),
                             "stop",
                             "--job-id=" + std::to_string(job),
                             "--coordinator-host=127.0.0.1",
                             "--coordinator-port=" + std::to_string(cluster.coordinator_port())},
                            cluster.log_dir()));
        const auto code = p.await_exit(90s);
        EXPECT_TRUE(code.has_value() && *code == 0) << p.read_log();
        std::smatch m;
        const std::string out = p.read_log();
        if (!std::regex_search(out, m, std::regex{"savepoint_checkpoint_id=([0-9]+)"})) {
            ADD_FAILURE() << "the stop printed no checkpoint: " << out;
            return {};
        }
        return m[1].str();
    }

    std::optional<std::size_t> worker_with(Cluster& cluster, std::string_view needle) const {
        for (std::size_t i = 0; i < 2; ++i) {
            if (cluster.worker(i).log_contains(needle)) {
                return i;
            }
        }
        return std::nullopt;
    }

    // A marker row committed now: once the stream (not a snapshot) has carried
    // it into the change log, every change committed before it is there.
    void await_caught_up(std::int64_t marker, std::chrono::seconds limit = 180s) {
        exec("INSERT INTO " + table_ + " (id, rev, status, amount) VALUES (" +
             std::to_string(marker) + ", 1, 'marker', 0.25)");
        const auto deadline = Clock::now() + limit;
        while (Clock::now() < deadline) {
            const auto r = ch_->try_query("SELECT count() FROM default." + log_ + " WHERE id = " +
                                          std::to_string(marker) + " AND __op != 'snapshot'");
            if (r.status == 0 && r.out.find_first_not_of("0\n") != std::string::npos) {
                return;
            }
            std::this_thread::sleep_for(250ms);
        }
        FAIL() << "the marker row " << marker << " never reached ClickHouse through the stream";
    }

    std::vector<std::string> ch_lines(const std::string& sql) const {
        std::vector<std::string> out;
        std::istringstream in(ch_->query(sql));
        for (std::string line; std::getline(in, line);) {
            if (!line.empty()) {
                std::replace(line.begin(), line.end(), '\t', '|');
                out.push_back(line);
            }
        }
        return out;
    }

    // The checks, against SELECT over the PostgreSQL table.
    void verify(const std::string& label) {
        const auto want =
            rows("SELECT id, rev, status, amount::text, note FROM " + table_ + " ORDER BY id");
        // Two different rows of one key under one version: the store keeps
        // either, so the state depends on which.
        const auto tied = ch_lines("SELECT id, __version FROM default." + log_ +
                                   " GROUP BY id, __version HAVING uniqExact(tuple(rev, status, "
                                   "amount, note, __deleted)) > 1 "
                                   "ORDER BY id LIMIT 5 FORMAT TabSeparated");
        const std::string tied_keys =
            ch_->scalar("SELECT count() FROM (SELECT id, __version FROM default." + log_ +
                        " GROUP BY id, __version HAVING uniqExact(tuple(rev, status, amount, note, "
                        "__deleted)) > 1)");
        EXPECT_TRUE(tied.empty()) << label << ": " << tied_keys
                                  << " key(s) have two different rows under one __version, e.g. "
                                     "(id|__version) "
                                  << tied.front();
        const auto final_rows =
            ch_lines("SELECT id, rev, status, toString(amount), note FROM default." + state_ +
                     " FINAL ORDER BY id FORMAT TabSeparated");
        const auto rebuilt = ch_lines(
            "SELECT id, argMax(rev, __version), argMax(status, __version), "
            "toString(argMax(amount, __version)), argMax(note, __version) FROM default." +
            log_ +
            " GROUP BY id HAVING argMax(__deleted, __version) = 0 ORDER BY id FORMAT TabSeparated");
        EXPECT_EQ(final_rows, want) << label << ": ReplacingMergeTree FINAL differs from the table";
        EXPECT_EQ(rebuilt, want) << label << ": argMax over the change log differs from the table";
        const std::string defaulted =
            ch_->scalar("SELECT count() FROM default." + state_ + " FINAL WHERE note = 'none'");
        std::printf("[pg-cdc] backfill %s: rows=%zu with_default=%s tied_keys=%s log_rows=%s\n",
                    label.c_str(),
                    want.size(),
                    defaulted.c_str(),
                    tied_keys.c_str(),
                    ch_->scalar("SELECT count() FROM default." + log_).c_str());
    }

    void run(bool interrupt) {
        ClusterSpec spec;
        spec.node_binary = node_binary();
        spec.workers = 2;
        spec.slots_per_worker = 4;
        spec.http = true;
        Cluster cluster(spec);
        ScopedDiagnostics diagnostics(cluster);
        ASSERT_TRUE(cluster.start_coordinator() && cluster.start_worker(0, {}) &&
                    cluster.start_worker(1, {}) && cluster.await_workers_registered(2));

        // The first run: the table captured, changes streamed, checkpoints.
        submit(cluster, job_sql(/*with_note=*/false), {});
        ASSERT_TRUE(clink::itest::await(
            [&] { return worker_with(cluster, "snapshot complete").has_value(); }, 120s))
            << "the first run's snapshot never completed";
        exec("UPDATE " + table_ + " SET rev = rev + 1, status = 'first' WHERE id % 10 = 0");
        exec("DELETE FROM " + table_ + " WHERE id % 97 = 0");
        exec("INSERT INTO " + table_ +
             " SELECT g, 1, 'inserted', g * 7 + 1.25 FROM generate_series(5001, 5050) g");
        await_caught_up(9'000'001);
        const std::string checkpoint = stop(cluster, 1);
        ASSERT_FALSE(checkpoint.empty());

        // While nothing runs: a change from before the column, the column (its
        // default gives every row a value, with no change to stream), changes
        // after it, deletes and inserts.
        exec("UPDATE " + table_ + " SET status = 'before the column' WHERE id % 13 = 0");
        exec("ALTER TABLE " + table_ + " ADD COLUMN note text DEFAULT 'none'");
        exec("UPDATE " + table_ + " SET note = 'n' || id, rev = rev + 1 WHERE id % 7 = 0");
        exec("DELETE FROM " + table_ + " WHERE id % 89 = 0");
        exec("INSERT INTO " + table_ +
             " SELECT g, 1, 'after the column', g * 7 + 1.25, 'new' FROM generate_series(5101, "
             "5120) "
             "g");
        add_note_to_clickhouse();

        std::size_t registrations = 2;
        const std::string point = "postgres_cdc.snapshot_batch";
        if (interrupt) {
            // The source, a singleton, runs on worker 0. Armed now, it parks in
            // the backfill's twelfth snapshot batch, with batches slowed so a
            // checkpoint completes in the backfill first.
            cluster.worker(0).kill_hard();
            ASSERT_TRUE(cluster.worker(0).await_exit(10s).has_value());
            ProcOptions armed;
            armed.fault = point + "=block@12," + point + "=delay:300";
            ASSERT_TRUE(cluster.restart_worker(0, armed));
            ASSERT_TRUE(cluster.await_workers_registered(++registrations));
        }
        submit(cluster,
               job_sql(/*with_note=*/true),
               {"--restore-from-dir",
                cluster.checkpoint_dir().string(),
                "--restore-from-checkpoint-id",
                checkpoint});
        if (interrupt) {
            const std::string witness =
                "[fault.injection] fired: " + point +
                " action=" + std::to_string(static_cast<int>(clink::fault::Action::Block));
            ASSERT_TRUE(
                clink::itest::await([&] { return cluster.worker(0).log_contains(witness); }, 120s))
                << "the source never parked in the backfill";
            const std::string parked_log = cluster.worker(0).read_log();
            cluster.worker(0).kill_hard();
            ASSERT_TRUE(cluster.worker(0).await_exit(10s).has_value());
            ASSERT_TRUE(cluster.restart_worker(0, {}));
            ASSERT_TRUE(cluster.await_workers_registered(++registrations));
            ASSERT_TRUE(clink::itest::await(
                [&] {
                    return cluster.count_in_coordinator_log(
                               "[coordinator.restart] [info] job_id=2 attempt=") >= 1;
                },
                90s))
                << "the job never restarted";
            EXPECT_NE(parked_log.find("declared since the checkpoint, so the table is read again"),
                      std::string::npos)
                << "the parked run was not a backfill";
        }
        await_caught_up(9'000'002);
        EXPECT_TRUE(worker_with(cluster,
                                interrupt ? "plan: restored mid-snapshot"
                                          : "declared since the checkpoint, so the "
                                            "table is read again")
                        .has_value())
            << "the restored source did not read the table again";
        verify(interrupt ? "after the interrupted backfill" : "after the backfill");

        // Later changes outrank the backfill: updates of rows it refreshed,
        // and deletes.
        exec("UPDATE " + table_ + " SET note = 'later', rev = rev + 1 WHERE id % 5 = 0");
        exec("DELETE FROM " + table_ + " WHERE id % 17 = 0");
        await_caught_up(9'000'003);
        verify(interrupt ? "after later changes (interrupted)" : "after later changes");
    }

    static std::unique_ptr<DockerPostgres> pg_;
    static std::unique_ptr<DockerClickHouse> ch_;
    std::string table_;
    std::string slot_;
    std::string pub_;
    std::string log_;
    std::string state_;
    int submits_{0};
};

std::unique_ptr<DockerPostgres> PostgresCdcClickHouseBackfill::pg_;
std::unique_ptr<DockerClickHouse> PostgresCdcClickHouseBackfill::ch_;

TEST_F(PostgresCdcClickHouseBackfill,
       TheBackfillOutranksTheRowsItRefreshesAndLaterChangesOutrankIt) {
    run(/*interrupt=*/false);
}

TEST_F(PostgresCdcClickHouseBackfill, ABackfillInterruptedByAWorkerKillConverges) {
    run(/*interrupt=*/true);
}

}  // namespace
