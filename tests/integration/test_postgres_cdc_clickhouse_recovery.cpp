// The PostgreSQL CDC source in a cluster, into ClickHouse: a coordinator and
// two clink_node workers run a SQL job that captures a PostgreSQL table
// (changelog_mode='append', one row per change) into a ClickHouse change log
// while a workload commits inserts, updates and deletes. A ClickHouse
// materialised view feeds a ReplacingMergeTree(__version, __deleted) that
// keeps the latest row per key. Each cell faults one phase of the capture:
//
//   * a worker killed during the snapshot, after a checkpoint completed in it;
//   * a worker killed at the transition from snapshot to stream;
//   * a worker killed after a streaming checkpoint recorded its position and
//     before that checkpoint completed;
//   * a worker killed just after a completed checkpoint's position was
//     confirmed to the replication slot;
//   * the PostgreSQL server restarted while the job streams.
//
// Workers are parked at the CDC source's fault points, and each cell waits for
// the fault framework's witness line before it kills, so the fault lands in the
// phase the cell names. The pass criteria, in every cell, once the source has
// streamed a marker row committed after the workload:
//
//   * the table's state, read from the ReplacingMergeTree with FINAL and
//     rebuilt from the change log with argMax over __version, equals SELECT
//     over the PostgreSQL table, every column of every row;
//   * every row version and delete the workload committed is in the change
//     log, so a change lost behind a later change of the same key is caught;
//   * the restarts the cell names, counted in the coordinator's log.
//
// Needs Docker, and the clink_node and clink_submit_sql binaries of a build
// with fault injection compiled in. Each cell prints one "[pg-cdc]" line.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <libpq-fe.h>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <random>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <utility>
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

enum class Cell { SnapshotRows, Transition, BeforeCompletion, AfterCompletion, PostgresRestart };

std::string cell_name(const ::testing::TestParamInfo<Cell>& info) {
    switch (info.param) {
        case Cell::SnapshotRows:
            return "WorkerKilledDuringTheSnapshot";
        case Cell::Transition:
            return "WorkerKilledAtTheTransitionToStreaming";
        case Cell::BeforeCompletion:
            return "WorkerKilledBeforeAStreamingCheckpointCompletes";
        case Cell::AfterCompletion:
            return "WorkerKilledAfterTheSlotWasConfirmed";
        case Cell::PostgresRestart:
            return "PostgresRestartedWhileStreaming";
    }
    return "Unknown";
}

constexpr std::int64_t kSeedRows = 3000;
constexpr std::int64_t kMarker = 9'000'000;

// ---- PostgreSQL ------------------------------------------------------------

class Pg {
public:
    explicit Pg(const std::string& conninfo) : c_(PQconnectdb(conninfo.c_str())) {
        if (PQstatus(c_) != CONNECTION_OK) {
            const std::string err = PQerrorMessage(c_);
            PQfinish(c_);
            c_ = nullptr;
            throw std::runtime_error("connect: " + err);
        }
    }
    ~Pg() {
        if (c_ != nullptr) {
            PQfinish(c_);
        }
    }
    Pg(const Pg&) = delete;
    Pg& operator=(const Pg&) = delete;
    Pg(Pg&&) = delete;
    Pg& operator=(Pg&&) = delete;

    // Every row of the result, columns joined with '|'.
    std::vector<std::string> rows(const std::string& sql) {
        PGresult* r = PQexec(c_, sql.c_str());
        const auto st = PQresultStatus(r);
        if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK) {
            const std::string err = PQerrorMessage(c_);
            PQclear(r);
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
        return out;
    }
    void exec(const std::string& sql) { (void)rows(sql); }

private:
    PGconn* c_;
};

std::string content(const std::string& rev) {
    return "rev = " + rev + ", status = 's' || ((" + rev + ") % 5), amount = (id * 7 + " + rev +
           ") + 0.25, at = timestamptz '2026-01-01 00:00:00+00' + (id * 1000 + " + rev +
           ") * interval '1 millisecond', note = 'n' || id || '-' || (" + rev + ")";
}

std::string insert_sql(const std::string& table, std::int64_t from, std::int64_t to) {
    return "INSERT INTO " + table +
           " (id, rev, status, amount, at, note) SELECT g, 1, 's1', g * 7 + 1.25, timestamptz "
           "'2026-01-01 00:00:00+00' + (g * 1000 + 1) * interval '1 millisecond', 'n' || g || '-1' "
           "FROM generate_series(" +
           std::to_string(from) + "::bigint, " + std::to_string(to) +
           "::bigint) g RETURNING id::text || ':' || rev";
}

// Commits transactions of inserts, updates and deletes until stopped,
// recording every row version and delete it committed.
class Workload {
public:
    Workload(std::string conninfo, std::string table, std::int64_t first_new_id)
        : conninfo_(std::move(conninfo)), table_(std::move(table)), next_id_(first_new_id) {
        for (std::int64_t id = 1; id <= kSeedRows; ++id) {
            live_.push_back(id);
        }
    }
    ~Workload() { stop(); }
    Workload(const Workload&) = delete;
    Workload& operator=(const Workload&) = delete;
    Workload(Workload&&) = delete;
    Workload& operator=(Workload&&) = delete;

    void start() {
        stop_.store(false);
        thread_ = std::thread([this] { run(); });
    }
    void stop() {
        stop_.store(true);
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    // Run until `n` more transactions have committed.
    void await_transactions(std::size_t n, std::chrono::seconds limit = 60s) {
        const std::size_t target = transactions_.load() + n;
        const auto deadline = Clock::now() + limit;
        while (transactions_.load() < target && Clock::now() < deadline) {
            std::this_thread::sleep_for(20ms);
        }
    }
    [[nodiscard]] std::vector<std::string> committed() const {
        std::lock_guard<std::mutex> lk(mu_);
        return committed_;
    }
    [[nodiscard]] std::string error() const {
        std::lock_guard<std::mutex> lk(mu_);
        return error_;
    }
    [[nodiscard]] std::size_t transactions() const { return transactions_.load(); }

private:
    void run() {
        try {
            Pg pg(conninfo_);
            std::mt19937_64 rng(42);
            std::size_t round = 0;
            while (!stop_.load()) {
                ++round;
                std::vector<std::string> versions;
                std::vector<std::int64_t> removed;
                pg.exec("BEGIN");
                const auto take = [&](std::vector<std::string> v) {
                    versions.insert(versions.end(), v.begin(), v.end());
                };
                take(pg.rows(insert_sql(table_, next_id_, next_id_ + 2)));
                const std::int64_t inserted_to = next_id_ + 2;
                // A large transaction now and then, spanning several batches.
                const std::size_t updates = round % 10 == 0 ? 400 : 3;
                std::set<std::int64_t> picked;
                for (std::size_t i = 0; i < updates && !live_.empty(); ++i) {
                    picked.insert(live_[rng() % live_.size()]);
                }
                if (!picked.empty()) {
                    std::string ids;
                    for (const auto id : picked) {
                        ids += (ids.empty() ? "" : ",") + std::to_string(id);
                    }
                    take(pg.rows("UPDATE " + table_ + " SET " + content("rev + 1") +
                                 " WHERE id IN (" + ids + ") RETURNING id::text || ':' || rev"));
                }
                if (live_.size() > 100) {
                    const std::size_t at = rng() % live_.size();
                    const std::int64_t gone = live_[at];
                    take(pg.rows("DELETE FROM " + table_ + " WHERE id = " + std::to_string(gone) +
                                 " RETURNING id::text || ':deleted'"));
                    removed.push_back(gone);
                }
                pg.exec("COMMIT");
                for (std::int64_t id = next_id_; id <= inserted_to; ++id) {
                    live_.push_back(id);
                }
                next_id_ = inserted_to + 1;
                for (const auto gone : removed) {
                    live_.erase(std::remove(live_.begin(), live_.end(), gone), live_.end());
                }
                {
                    std::lock_guard<std::mutex> lk(mu_);
                    committed_.insert(committed_.end(), versions.begin(), versions.end());
                }
                transactions_.fetch_add(1);
                std::this_thread::sleep_for(15ms);
            }
        } catch (const std::exception& e) {
            std::lock_guard<std::mutex> lk(mu_);
            error_ = e.what();
        }
    }

    std::string conninfo_;
    std::string table_;
    std::int64_t next_id_;
    std::vector<std::int64_t> live_;
    std::atomic<bool> stop_{false};
    std::atomic<std::size_t> transactions_{0};
    mutable std::mutex mu_;
    std::vector<std::string> committed_;
    std::string error_;
    std::thread thread_;
};

// ---- the fixture -------------------------------------------------------------

class PostgresCdcClickHouseRecovery : public ::testing::TestWithParam<Cell> {
protected:
    static void TearDownTestSuite() {
        ch_.reset();
        pg_.reset();
    }

    void SetUp() override {
        if (!DockerPostgres::docker_available()) {
            GTEST_SKIP() << "Docker not available; skipping the PostgreSQL CDC recovery cells";
        }
        if (!std::filesystem::exists(node_binary()) || !std::filesystem::exists(sql_binary())) {
            GTEST_SKIP() << "cluster node or SQL submit binary is not built";
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

        Pg pg(pg_->conninfo());
        pg.exec("CREATE TABLE " + table_ +
                " (id bigint PRIMARY KEY, rev int NOT NULL, status text, amount numeric(12, 2), at "
                "timestamptz(3), note text)");
        pg.exec("ALTER TABLE " + table_ + " REPLICA IDENTITY FULL");
        pg.exec("CREATE PUBLICATION " + pub_ + " FOR TABLE " + table_);
        (void)pg.rows(insert_sql(table_, 1, kSeedRows));

        const std::string columns =
            "(id Int64, rev Nullable(Int32), status Nullable(String), amount "
            "Nullable(Decimal(12, 2)), at Nullable(DateTime64(3, 'UTC')), note Nullable(String), "
            "__version UInt64, __deleted UInt8, __op String)";
        ch_->query("CREATE TABLE default." + log_ + " " + columns +
                   " ENGINE = MergeTree ORDER BY (id, __version) SETTINGS async_insert = 0");
        ch_->query("CREATE TABLE default." + state_ + " " + columns +
                   " ENGINE = ReplacingMergeTree(__version, __deleted) ORDER BY id");
        ch_->query("CREATE MATERIALIZED VIEW default." + state_ + "_mv TO default." + state_ +
                   " AS SELECT * FROM default." + log_);
    }

    void TearDown() override {
        if (ch_ != nullptr && !log_.empty()) {
            (void)ch_->try_query("DROP VIEW IF EXISTS default." + state_ + "_mv SYNC");
            (void)ch_->try_query("DROP TABLE IF EXISTS default." + state_ + " SYNC");
            (void)ch_->try_query("DROP TABLE IF EXISTS default." + log_ + " SYNC");
        }
        if (pg_ != nullptr && !table_.empty()) {
            try {
                Pg pg(pg_->conninfo());
                for (int i = 0; i < 100; ++i) {
                    const auto active =
                        pg.rows("SELECT active FROM pg_replication_slots WHERE slot_name = '" +
                                slot_ + "'");
                    if (active.empty()) {
                        break;
                    }
                    if (active[0] == "f") {
                        pg.exec("SELECT pg_drop_replication_slot('" + slot_ + "')");
                        break;
                    }
                    std::this_thread::sleep_for(100ms);
                }
                pg.exec("DROP PUBLICATION IF EXISTS " + pub_);
                pg.exec("DROP TABLE IF EXISTS " + table_);
            } catch (const std::exception&) {
            }
        }
    }

    std::string job_sql() const {
        const std::string rest =
            "rev INTEGER, status VARCHAR, amount DECIMAL(12, 2), at TIMESTAMP(3) WITH TIME ZONE, "
            "note VARCHAR, __version BIGINT, __deleted BOOLEAN, __op VARCHAR";
        const std::string cols = "(id BIGINT, " + rest;
        return "CREATE TABLE orders_cdc (id BIGINT PRIMARY KEY, " + rest +
               ") WITH (connector='postgres', format='json', mode='cdc', "
               "changelog_mode='append', conninfo='" +
               pg_->conninfo() + "', slot_name='" + slot_ + "', publication_names='" + pub_ +
               "', \"table\"='public." + table_ +
               "', snapshot_batch_rows='200', status_interval_ms='1000'); "
               "CREATE TABLE ch " +
               cols +
               ") WITH (connector='clickhouse', insert_format='native', host='127.0.0.1', port='" +
               std::to_string(ch_->port(0)) + "', database='default', table='" + log_ +
               "', batch_interval_ms='200'); "
               "INSERT INTO ch SELECT * FROM orders_cdc;";
    }

    void submit(Cluster& cluster) {
        Process submit;
        ASSERT_TRUE(submit.spawn("submit-sql",
                                 sql_binary(),
                                 {sql_binary().string(),
                                  "-e",
                                  job_sql(),
                                  "--coordinator-host",
                                  "127.0.0.1",
                                  "--coordinator-port",
                                  std::to_string(cluster.http_port()),
                                  "--name",
                                  "postgres-cdc-recovery",
                                  "--checkpoint-dir",
                                  cluster.checkpoint_dir().string(),
                                  "--checkpoint-interval-ms",
                                  "500",
                                  "--alignment",
                                  "aligned",
                                  "--max-restarts-on-worker-loss",
                                  "8",
                                  "--parallelism",
                                  "2"},
                                 cluster.log_dir()));
        const auto code = submit.await_exit(60s);
        ASSERT_TRUE(code.has_value());
        ASSERT_EQ(*code, 0) << submit.read_log();
    }

    std::unique_ptr<Cluster> make_cluster(const std::string& fault) {
        ClusterSpec spec;
        spec.node_binary = node_binary();
        spec.workers = 2;
        spec.slots_per_worker = 4;
        spec.http = true;
        auto cluster = std::make_unique<Cluster>(spec);
        if (const char* keep = std::getenv("CLINK_PG_CDC_KEEP"); keep != nullptr && *keep == '1') {
            cluster->keep_artifacts();
            std::printf("[pg-cdc] artifacts at %s\n", cluster->root().c_str());
        }
        // Placement visits workers in sorted id order, so the CDC source - a
        // singleton, subtask 0 - and the singleton chain behind it land on
        // worker 0. Only worker 0 is armed, and its replacement is not, so a
        // restored source never parks again.
        ProcOptions armed;
        armed.fault = fault;
        if (!cluster->start_coordinator() || !cluster->start_worker(0, armed) ||
            !cluster->start_worker(1, {}) || !cluster->await_workers_registered(2)) {
            ADD_FAILURE() << "the cluster did not start";
        }
        return cluster;
    }

    // Every log a worker wrote, including those of processes killed and
    // replaced, whose files the replacement truncates.
    std::vector<std::string> worker_logs(Cluster& cluster) const {
        std::vector<std::string> out = archived_logs_;
        for (std::size_t i = 0; i < 2; ++i) {
            out.push_back(cluster.worker(i).read_log());
        }
        return out;
    }

    std::optional<std::size_t> worker_with(Cluster& cluster, std::string_view needle) const {
        for (std::size_t i = 0; i < 2; ++i) {
            if (cluster.worker(i).log_contains(needle)) {
                return i;
            }
        }
        return std::nullopt;
    }

    void kill_and_replace(Cluster& cluster, std::size_t idx) {
        cluster.worker(idx).kill_hard();
        ASSERT_TRUE(cluster.worker(idx).await_exit(10s).has_value());
        archived_logs_.push_back(cluster.worker(idx).read_log());
        ASSERT_TRUE(cluster.restart_worker(idx, {}));
    }

    static std::size_t restarts(const Cluster& cluster) {
        return cluster.count_in_coordinator_log("[coordinator.restart] [info] job_id=1 attempt=");
    }

    // The marker row, committed after the workload: once the stream (not a
    // snapshot) has delivered it to ClickHouse, everything committed before it
    // has been delivered.
    void await_caught_up(std::chrono::seconds limit = 180s) {
        {
            Pg pg(pg_->conninfo());
            (void)pg.rows(insert_sql(table_, kMarker, kMarker));
        }
        const auto deadline = Clock::now() + limit;
        while (Clock::now() < deadline) {
            const auto r = ch_->try_query("SELECT count() FROM default." + log_ + " WHERE id = " +
                                          std::to_string(kMarker) + " AND __op != 'snapshot'");
            if (r.status == 0 && r.out.find_first_not_of("0\n") != std::string::npos) {
                return;
            }
            std::this_thread::sleep_for(250ms);
        }
        FAIL() << "the marker row never reached ClickHouse through the stream";
    }

    // id|rev|status|amount|epoch ms|note for every row of the table.
    std::vector<std::string> pg_rows() const {
        Pg pg(pg_->conninfo());
        return pg.rows(
            "SELECT id, rev, status, amount::text, (extract(epoch FROM at) * 1000)::bigint, "
            "note FROM " +
            table_ + " ORDER BY id");
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

    void verify(const Workload& workload,
                const std::string& cell_label,
                std::size_t restarts_spent) {
        const auto want = pg_rows();
        const auto final_rows = ch_lines(
            "SELECT id, rev, status, toString(amount), toUnixTimestamp64Milli(at), note FROM "
            "default." +
            state_ + " FINAL ORDER BY id FORMAT TabSeparated");
        const auto rebuilt = ch_lines(
            "SELECT id, argMax(rev, __version), argMax(status, __version), "
            "toString(argMax(amount, __version)), toUnixTimestamp64Milli(argMax(at, __version)), "
            "argMax(note, __version) FROM default." +
            log_ +
            " GROUP BY id HAVING argMax(__deleted, __version) = 0 ORDER BY id FORMAT TabSeparated");
        EXPECT_EQ(final_rows, want) << "ReplacingMergeTree FINAL differs from the table";
        EXPECT_EQ(rebuilt, want) << "argMax over the change log differs from the table";

        std::set<std::string> seen;
        for (const auto& line : ch_lines("SELECT DISTINCT id, rev, __deleted FROM default." + log_ +
                                         " FORMAT TabSeparated")) {
            const auto a = line.find('|');
            const auto b = line.rfind('|');
            const std::string id = line.substr(0, a);
            seen.insert(line.substr(b + 1) == "1" ? id + ":deleted"
                                                  : id + ":" + line.substr(a + 1, b - a - 1));
        }
        std::size_t missing = 0;
        const auto committed = workload.committed();
        for (const auto& v : committed) {
            if (seen.count(v) == 0) {
                if (++missing <= 5) {
                    ADD_FAILURE() << "committed version " << v << " is not in the change log";
                }
            }
        }
        const std::string log_rows = ch_->scalar("SELECT count() FROM default." + log_);
        std::printf(
            "[pg-cdc] cell=%s rows=%zu committed_versions=%zu transactions=%zu log_rows=%s "
            "restarts=%zu missing=%zu\n",
            cell_label.c_str(),
            want.size(),
            committed.size(),
            workload.transactions(),
            log_rows.c_str(),
            restarts_spent,
            missing);
        EXPECT_EQ(missing, 0U);
    }

    static std::unique_ptr<DockerPostgres> pg_;
    static std::unique_ptr<DockerClickHouse> ch_;
    std::string table_;
    std::string slot_;
    std::string pub_;
    std::string log_;
    std::string state_;
    std::vector<std::string> archived_logs_;
};

std::unique_ptr<DockerPostgres> PostgresCdcClickHouseRecovery::pg_;
std::unique_ptr<DockerClickHouse> PostgresCdcClickHouseRecovery::ch_;

bool any_contains(const std::vector<std::string>& logs, std::string_view needle) {
    return std::any_of(logs.begin(), logs.end(), [&](const std::string& l) {
        return l.find(needle) != std::string::npos;
    });
}

TEST_P(PostgresCdcClickHouseRecovery, TheTableConvergesAndNoChangeIsLost) {
    const Cell cell = GetParam();
    const std::string label = cell_name(::testing::TestParamInfo<Cell>(cell, 0));

    // The fault each cell parks the source at. Snapshot batches are slowed
    // where a checkpoint must complete in the middle of the snapshot.
    std::string fault;
    std::string point;
    switch (cell) {
        case Cell::SnapshotRows:
            point = "postgres_cdc.snapshot_batch";
            fault = point + "=block@12," + point + "=delay:300";
            break;
        case Cell::Transition:
            point = "postgres_cdc.snapshot_done";
            fault = point + "=block@1,postgres_cdc.snapshot_batch=delay:250";
            break;
        case Cell::BeforeCompletion:
            point = "postgres_cdc.offset_recorded";
            fault = point + "=block@12";
            break;
        case Cell::AfterCompletion:
            point = "postgres_cdc.wal_confirmed";
            fault = point + "=block@3";
            break;
        case Cell::PostgresRestart:
            break;
    }
    auto cluster = make_cluster(fault);
    ScopedDiagnostics diagnostics(*cluster);
    submit(*cluster);
    ASSERT_TRUE(clink::itest::await(
        [&] { return worker_with(*cluster, "postgres cdc source open:").has_value(); }, 90s))
        << "the CDC source never opened";

    Workload workload(pg_->conninfo(), table_, 100'000);
    workload.start();

    std::size_t expected_restarts = 1;
    if (cell == Cell::PostgresRestart) {
        expected_restarts = 0;
        ASSERT_TRUE(clink::itest::await(
            [&] { return worker_with(*cluster, "snapshot complete").has_value(); }, 90s));
        workload.await_transactions(30);
        // The workload pauses so no transaction's commit is in doubt across
        // the restart.
        workload.stop();
        pg_->restart();
        workload.start();
        workload.await_transactions(30);
        ASSERT_TRUE(clink::itest::await(
            [&] { return worker_with(*cluster, "replication resumed").has_value(); }, 90s))
            << "the source did not resume after the server restarted";
    } else {
        // The block's own witness: a delay armed on the same point prints the
        // same line with another action on every batch it slows.
        const std::string witness = "[fault.injection] fired: " + point + " action=" +
                                    std::to_string(static_cast<int>(clink::fault::Action::Block));
        std::optional<std::size_t> parked;
        ASSERT_TRUE(clink::itest::await(
            [&] {
                parked = worker_with(*cluster, witness);
                return parked.has_value();
            },
            120s))
            << "no worker reached " << point;
        ASSERT_EQ(*parked, 0U) << "the source was not placed on the armed worker";
        if (cell == Cell::BeforeCompletion || cell == Cell::AfterCompletion) {
            // The fault must have landed in the stream phase.
            EXPECT_TRUE(cluster->worker(*parked).log_contains("snapshot complete"))
                << point << " fired before the snapshot finished";
        }
        // Changes committed while the source is parked, before the kill.
        workload.await_transactions(20);
        kill_and_replace(*cluster, *parked);
        ASSERT_TRUE(cluster->await_workers_registered(3));
        ASSERT_TRUE(clink::itest::await([&] { return restarts(*cluster) >= 1; }, 90s))
            << "the job never restarted";
        workload.await_transactions(30);
    }
    workload.stop();
    ASSERT_EQ(workload.error(), "");
    await_caught_up();
    const std::size_t spent = restarts(*cluster);
    const auto logs = worker_logs(*cluster);
    if (cell == Cell::SnapshotRows || cell == Cell::Transition) {
        EXPECT_TRUE(any_contains(logs, "plan: restored mid-snapshot"))
            << "the restore was not from a checkpoint taken during the snapshot";
    }
    if (cell == Cell::BeforeCompletion || cell == Cell::AfterCompletion) {
        EXPECT_TRUE(any_contains(logs, "plan: resume from checkpoint"))
            << "the restore did not resume the stream from a checkpoint";
    }
    EXPECT_FALSE(any_contains(logs, "beyond the position this job resumes from"))
        << "a restore found the slot confirmed past its checkpoint";
    EXPECT_EQ(spent, expected_restarts);
    verify(workload, label, spent);
}

INSTANTIATE_TEST_SUITE_P(Cells,
                         PostgresCdcClickHouseRecovery,
                         ::testing::Values(Cell::SnapshotRows,
                                           Cell::Transition,
                                           Cell::BeforeCompletion,
                                           Cell::AfterCompletion,
                                           Cell::PostgresRestart),
                         cell_name);

}  // namespace
