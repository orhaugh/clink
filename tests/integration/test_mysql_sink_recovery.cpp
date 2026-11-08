// The MySQL sinks' recovery cells: a coordinator and two clink_node workers run
// a job that polls a feed this test serves (http_poll_source, replayable from
// its checkpointed cursor) into mysql_sink or mysql_upsert_sink, while the feed
// releases records at a steady rate. Record n names row k<n mod ids>, with a
// rising seq and v. Each cell parks worker 0 at one of the sinks' fault points,
// SIGKILLs it there, and lets the job restore:
//
//   1. append_before_commit: mysql_sink parked at mysql.sink_before_commit, the
//      flush's INSERTs executed inside a transaction that is not committed;
//   2. append_after_commit: mysql_sink parked at mysql.sink_after_commit, the
//      flush committed and the barrier that asked for it not acknowledged, so
//      the checkpoint it belongs to never completes;
//   3. upsert_before_commit and 4. upsert_after_commit: the same two points in
//      mysql_upsert_sink, where every seventh record of a row deletes it;
//   5. sql_after_commit: cell 4 as a SQL job (connector='http_poll' into
//      connector='mysql' mode='upsert') at parallelism 2, through a GROUP BY
//      whose counts replace by key with update_before and update_after rows,
//      into a key column whose collation tells values apart as the stream does.
//
// Pass criteria, read through the mysql client in the server's container and
// never from anything clink reports. The append cells: every record's row is
// in the table, column for column, and the records with more than one row are
// exactly those the sink committed after the checkpoint the job restored, each
// in two; before COMMIT that set is empty, after COMMIT it is not. Append mode
// is at-least-once, and the cells count its duplicates rather than forbid them.
// The upsert cells: every row the input's last records leave present, column
// for column, and every row they delete absent. Every cell: the one restart the
// kill costs, a replay (the feed was read again from the checkpoint's cursor),
// and the fault landing where the cell names, witnessed by the fault
// framework's log line and, before COMMIT, by the server's open transaction
// holding the flush's rows.
//
// Needs the server scripts/mysql-live.sh starts (CLINK_MYSQL_TEST_DSN and
// CLINK_MYSQL_TEST_CONTAINER) and the clink_node binary of a build with fault
// injection (clink_submit_sql too for cell 5). Each cell prints one
// "[mysql-sink-recovery]" line and, with CLINK_MYSQL_RECOVERY_LOG_DIR set, keeps
// the cluster's logs there.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <httplib.h>
#include <limits>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/job_graph.hpp"
#include "clink/http_connector/http_request.hpp"

#include "../checkpoint_markers.hpp"
#include "tests/integration/cluster_harness.hpp"
#include "tests/integration/docker_command.hpp"

namespace {

using namespace std::chrono_literals;
using clink::itest::Cluster;
using clink::itest::ClusterSpec;
using clink::itest::ProcOptions;
using clink::itest::ScopedDiagnostics;

constexpr const char* kPasswordVar = "CLINK_MYSQL_RECOVERY_PASSWORD";

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

std::string env_or_empty(const char* name) {
    const char* v = std::getenv(name);
    return v == nullptr ? std::string{} : std::string{v};
}

struct Dsn {
    std::string host{"127.0.0.1"};
    std::string port{"3306"};
    std::string user{"root"};
    std::string password;
    std::string database{"test"};
};

Dsn parse_dsn(const std::string& text) {
    Dsn d;
    std::istringstream in(text);
    for (std::string tok; in >> tok;) {
        const auto eq = tok.find('=');
        if (eq == std::string::npos) {
            continue;
        }
        const auto k = tok.substr(0, eq);
        const auto v = tok.substr(eq + 1);
        if (k == "host") {
            d.host = v;
        } else if (k == "port") {
            d.port = v;
        } else if (k == "user") {
            d.user = v;
        } else if (k == "password") {
            d.password = v;
        } else if (k == "database" || k == "dbname") {
            d.database = v;
        }
    }
    return d;
}

// The job's input. GET /feed?since=<seq> answers {"items":[...]}: up to 200 of
// the records released so far whose seq is after <seq>, ascending, the exclusive
// cursor http_poll_source replays from.
class MysqlSinkFeed {
public:
    MysqlSinkFeed(std::int64_t total, int ids, int per_second, bool deletes)
        : total_(total), ids_(ids), per_second_(per_second), deletes_(deletes) {
        svr_.Get("/feed", [this](const httplib::Request& req, httplib::Response& res) {
            std::int64_t since = 0;
            if (req.has_param("since")) {
                since = std::stoll(req.get_param_value("since"));
            }
            if (watching_.load()) {
                std::int64_t low = lowest_since_.load();
                while (since < low && !lowest_since_.compare_exchange_weak(low, since)) {
                }
            }
            const auto upto = std::min(released_.load(), since + 200);
            std::string items;
            for (std::int64_t n = since + 1; n <= upto; ++n) {
                items += (items.empty() ? "" : ",") + record(n);
            }
            std::int64_t prev = served_max_.load();
            while (upto > prev && !served_max_.compare_exchange_weak(prev, upto)) {
            }
            res.set_content("{\"items\":[" + items + "]}", "application/json");
        });
        port_ = svr_.bind_to_any_port("127.0.0.1");
        thread_ = std::thread([this] { svr_.listen_after_bind(); });
        while (!svr_.is_running()) {
            std::this_thread::sleep_for(2ms);
        }
    }
    ~MysqlSinkFeed() {
        stop_.store(true);
        if (releaser_.joinable()) {
            releaser_.join();
        }
        svr_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    MysqlSinkFeed(const MysqlSinkFeed&) = delete;
    MysqlSinkFeed& operator=(const MysqlSinkFeed&) = delete;

    [[nodiscard]] std::string url() const { return "http://127.0.0.1:" + std::to_string(port_); }

    void start() {
        releaser_ = std::thread([this] {
            const auto step = std::chrono::microseconds{1000000 / per_second_};
            auto next = std::chrono::steady_clock::now();
            while (!stop_.load() && released_.load() < total_) {
                released_.fetch_add(1);
                next += step;
                std::this_thread::sleep_until(next);
            }
        });
    }

    void watch_rewind() {
        lowest_since_.store(std::numeric_limits<std::int64_t>::max());
        watching_.store(true);
    }
    [[nodiscard]] std::int64_t lowest_since() const { return lowest_since_.load(); }
    [[nodiscard]] std::int64_t released() const { return released_.load(); }
    [[nodiscard]] std::int64_t served_max() const { return served_max_.load(); }
    [[nodiscard]] std::int64_t total() const { return total_; }
    [[nodiscard]] int ids() const { return ids_; }

    [[nodiscard]] std::string id_of(std::int64_t n) const { return "k" + std::to_string(n % ids_); }
    // About one record in seven deletes its row, at a different place in each
    // row's sequence, so some rows end deleted and the rest present.
    [[nodiscard]] bool deletes_row(std::int64_t n) const {
        return deletes_ && ((n / ids_) + (n % ids_)) % 7 == 6;
    }
    [[nodiscard]] std::string record(std::int64_t n) const {
        return R"({"id":")" + id_of(n) + R"(","seq":)" + std::to_string(n) + R"(,"v":)" +
               std::to_string(n * 10) + (deletes_row(n) ? R"(,"__row_kind":"delete"})" : "}");
    }

    // What the keyed table holds once the first `upto` records are applied:
    // each row's last record, "seq/v", or no entry when that record deletes it.
    [[nodiscard]] std::map<std::string, std::string> expected(std::int64_t upto) const {
        std::map<std::string, std::string> out;
        for (std::int64_t n = 1; n <= upto; ++n) {
            if (deletes_row(n)) {
                out.erase(id_of(n));
            } else {
                out[id_of(n)] = std::to_string(n) + "/" + std::to_string(n * 10);
            }
        }
        return out;
    }
    [[nodiscard]] std::size_t deleted(std::int64_t upto) const {
        return static_cast<std::size_t>(std::min<std::int64_t>(ids_, upto)) - expected(upto).size();
    }

private:
    std::int64_t total_;
    int ids_;
    int per_second_;
    bool deletes_;
    std::atomic<std::int64_t> released_{0};
    std::atomic<std::int64_t> served_max_{0};
    std::atomic<std::int64_t> lowest_since_{0};
    std::atomic<bool> watching_{false};
    std::atomic<bool> stop_{false};
    std::thread releaser_;
    httplib::Server svr_;
    int port_{0};
    std::thread thread_;
};

class MysqlSinkRecovery : public ::testing::Test {
protected:
    void SetUp() override {
        const auto dsn = env_or_empty("CLINK_MYSQL_TEST_DSN");
        container_ = env_or_empty("CLINK_MYSQL_TEST_CONTAINER");
        if (dsn.empty() || container_.empty()) {
            GTEST_SKIP() << "set CLINK_MYSQL_TEST_DSN and CLINK_MYSQL_TEST_CONTAINER "
                            "(scripts/mysql-live.sh does)";
        }
        if (!std::filesystem::exists(node_binary())) {
            GTEST_SKIP() << "the clink_node binary is not built";
        }
        dsn_ = parse_dsn(dsn);
        static int counter = 0;
        table_ = "rsnk_" + std::to_string(::getpid()) + "_" + std::to_string(++counter);
    }

    void TearDown() override {
        feed_.reset();
        if (!table_.empty() && !container_.empty()) {
            (void)run_mysql("DROP TABLE IF EXISTS `" + table_ + "`");
        }
    }

    // Statements through the mysql client inside the server's container: the
    // verifier's reads, apart from clink's own client. They go in on standard
    // input.
    clink::test::DockerCommandResult run_mysql(const std::string& statements) const {
        const auto file = std::filesystem::temp_directory_path() /
                          ("clink-mysql-sink-recovery-" + std::to_string(::getpid()) + ".sql");
        {
            std::ofstream out(file, std::ios::trunc);
            out << statements << "\n";
        }
        auto r = clink::test::run_docker_command(
            "docker exec -i -e MYSQL_PWD='" + dsn_.password + "' " + container_ +
            " mysql -N -B -u" + dsn_.user + " " + dsn_.database + " < '" + file.string() + "'");
        std::error_code ec;
        std::filesystem::remove(file, ec);
        return r;
    }

    std::string mysql(const std::string& statements) const {
        const auto r = run_mysql(statements);
        EXPECT_EQ(r.status, 0) << r.out;
        return r.out;
    }

    std::int64_t number(const std::string& query) const {
        const auto out = mysql(query);
        return out.empty() ? -1 : std::stoll(out);
    }

    void make_feed(bool deletes) {
        feed_ = std::make_unique<MysqlSinkFeed>(3000, 100, 300, deletes);
    }

    // The append table: a surrogate key of its own, so a record written twice
    // is two rows rather than a refused duplicate.
    void make_append_table() const {
        mysql("DROP TABLE IF EXISTS `" + table_ + "`; CREATE TABLE `" + table_ +
              "` (row_id BIGINT AUTO_INCREMENT PRIMARY KEY, id VARCHAR(16) NOT NULL, seq BIGINT "
              "NOT NULL, v BIGINT NOT NULL) ENGINE=InnoDB");
    }

    void make_keyed_table(const std::string& collation) const {
        mysql("DROP TABLE IF EXISTS `" + table_ + "`; CREATE TABLE `" + table_ +
              "` (id VARCHAR(16) COLLATE " + collation +
              " PRIMARY KEY, seq BIGINT NOT NULL, v BIGINT NOT NULL) ENGINE=InnoDB");
    }

    // The append table as seq -> its rows; a row whose id or v is not its
    // record's counts in `wrong`.
    std::map<std::int64_t, int> appended(std::size_t* wrong = nullptr) const {
        std::map<std::int64_t, int> out;
        std::istringstream in(mysql("SELECT seq, id, v FROM `" + table_ + "`"));
        for (std::string line; std::getline(in, line);) {
            std::istringstream cells(line);
            std::string seq;
            std::string id;
            std::string v;
            std::getline(cells, seq, '\t');
            std::getline(cells, id, '\t');
            std::getline(cells, v, '\t');
            if (seq.empty()) {
                continue;
            }
            const auto n = std::stoll(seq);
            ++out[n];
            if (wrong != nullptr && (id != feed_->id_of(n) || v != std::to_string(n * 10))) {
                ++*wrong;
            }
        }
        return out;
    }

    // The keyed table as "seq/v" by id.
    std::map<std::string, std::string> stored() const {
        std::map<std::string, std::string> out;
        std::istringstream in(mysql("SELECT id, seq, v FROM `" + table_ + "`"));
        for (std::string line; std::getline(in, line);) {
            std::istringstream cells(line);
            std::string id;
            std::string seq;
            std::string v;
            std::getline(cells, id, '\t');
            std::getline(cells, seq, '\t');
            std::getline(cells, v, '\t');
            if (!id.empty()) {
                out[id] = seq + "/" + v;
            }
        }
        return out;
    }

    // Rows changed by transactions the server holds open.
    std::int64_t open_rows() const {
        return number(
            "SELECT COALESCE(SUM(trx_rows_modified), 0) FROM information_schema.INNODB_TRX");
    }

    std::map<std::string, std::string> sink_params(bool keyed) const {
        std::map<std::string, std::string> p = {
            {"host", dsn_.host},
            {"port", dsn_.port},
            {"user", dsn_.user},
            {"password", std::string{"env://"} + kPasswordVar},
            {"database", dsn_.database},
            {"table", table_},
            {"columns", "id,seq,v"},
        };
        if (keyed) {
            p["key_columns"] = "id";
        }
        return p;
    }

    clink::cluster::JobGraphSpec graph(const std::string& sink) const {
        clink::cluster::JobGraphSpec g;
        clink::cluster::OperatorSpec src;
        src.type = "http_poll_source";
        src.id = "src";
        src.uid = "mysql-sink-recovery-src";
        src.out_channel = std::string{clink::cluster::kChannelString};
        src.params = {{"url", feed_->url()},
                      {"path", "/feed"},
                      {"cursor_param", "since"},
                      {"cursor_field", "seq"},
                      {"records_field", "items"},
                      {"poll_interval_ms", "20"}};
        g.ops.push_back(src);
        clink::cluster::OperatorSpec snk;
        snk.type = sink;
        snk.id = "snk";
        snk.uid = "mysql-sink-recovery-snk";
        snk.inputs = {"src"};
        snk.out_channel = std::string{clink::cluster::kChannelString};
        snk.params = sink_params(sink == "mysql_upsert_sink");
        g.ops.push_back(snk);
        return g;
    }

    ProcOptions worker_options(std::string fault = {}) const {
        ProcOptions o;
        o.fault = std::move(fault);
        o.env = {{kPasswordVar, dsn_.password}};
        return o;
    }

    // A coordinator and worker 0 only, armed with `fault`: the job submitted
    // next places every task on worker 0. Worker 1 joins after the deploy as
    // the spare a restart can use.
    std::unique_ptr<Cluster> make_cluster(const std::string& fault, int slots = 3) {
        ClusterSpec spec;
        spec.node_binary = node_binary();
        spec.workers = 2;
        spec.slots_per_worker = slots;
        spec.http = true;
        auto cluster = std::make_unique<Cluster>(spec);
        if (!cluster->start_coordinator() || !cluster->start_worker(0, worker_options(fault)) ||
            !cluster->await_workers_registered(1)) {
            ADD_FAILURE() << "the cluster did not start";
        }
        return cluster;
    }

    void add_spare_worker(Cluster& cluster) {
        ASSERT_TRUE(cluster.start_worker(1, worker_options()));
        ASSERT_TRUE(cluster.await_workers_registered(2));
    }

    static clink::http_connector::HttpRequest coordinator(const Cluster& cluster) {
        clink::http_connector::HttpRequest::Options o;
        o.base_url = "http://127.0.0.1:" + std::to_string(cluster.http_port());
        return clink::http_connector::HttpRequest(std::move(o));
    }

    void submit(Cluster& cluster, const std::string& sink) {
        auto http = coordinator(cluster);
        const auto r = http.post("/api/v1/jobs/spec?name=mysql-sink-recovery&checkpoint_dir=" +
                                     cluster.checkpoint_dir().string() +
                                     "&checkpoint_interval_ms=1000&alignment=aligned"
                                     "&max_restarts_on_worker_loss=8",
                                 graph(sink).to_json(),
                                 "application/json");
        ASSERT_NE(r.body.find("\"ok\":true"), std::string::npos)
            << "the job was not accepted: " << r.body;
    }

    void submit_sql(Cluster& cluster, const std::string& sql, int parallelism) {
        clink::itest::Process submit;
        ASSERT_TRUE(submit.spawn("submit-sql",
                                 sql_binary(),
                                 {sql_binary().string(),
                                  "-e",
                                  sql,
                                  "--coordinator-host",
                                  "127.0.0.1",
                                  "--coordinator-port",
                                  std::to_string(cluster.http_port()),
                                  "--name",
                                  "mysql-sink-recovery-sql",
                                  "--checkpoint-dir",
                                  cluster.checkpoint_dir().string(),
                                  "--checkpoint-interval-ms",
                                  "1000",
                                  "--alignment",
                                  "aligned",
                                  "--max-restarts-on-worker-loss",
                                  "8",
                                  "--parallelism",
                                  std::to_string(parallelism)},
                                 cluster.log_dir()));
        const auto code = submit.await_exit(60s);
        ASSERT_TRUE(code.has_value());
        ASSERT_EQ(*code, 0) << submit.read_log();
    }

    static void cancel_job(const Cluster& cluster) {
        auto http = coordinator(cluster);
        http.post("/api/v1/jobs/1/cancel", "", "application/json");
    }

    static std::size_t restarts(const Cluster& cluster) {
        return cluster.count_in_coordinator_log("[coordinator.restart] [info] job_id=1 attempt=");
    }

    static std::uint64_t latest_completed(const Cluster& cluster) {
        std::uint64_t latest = 0;
        std::error_code ec;
        for (const auto& e :
             std::filesystem::directory_iterator(cluster.checkpoint_dir() / "_jobs" / "1", ec)) {
            if (const auto id =
                    clink::test_markers::marker_id(e.path().filename().string(), "COMPLETED-")) {
                latest = std::max(latest, *id);
            }
        }
        return latest;
    }

    void await_first_checkpoint(Cluster& cluster) {
        ASSERT_TRUE(clink::itest::await([&] { return latest_completed(cluster) >= 1; }, 60s))
            << "no checkpoint completed";
    }

    // Every record released and `done` true, and two more checkpoints completed
    // after that, so nothing is still on its way into the table.
    void await_settled(Cluster& cluster, const std::function<bool()>& done) {
        ASSERT_TRUE(clink::itest::await(
            [&] { return feed_->released() == feed_->total() && done(); }, 240s))
            << "the table never held every record";
        const auto after = latest_completed(cluster);
        ASSERT_TRUE(
            clink::itest::await([&] { return latest_completed(cluster) >= after + 2; }, 60s))
            << "checkpoints stopped completing after the table converged";
    }

    // The keyed table equal to `want`, or the rows that are not, named.
    void expect_keyed(const std::map<std::string, std::string>& want) {
        const auto got = stored();
        std::size_t wrong = 0;
        for (const auto& [id, row] : want) {
            const auto it = got.find(id);
            if ((it == got.end() || it->second != row) && ++wrong <= 5) {
                ADD_FAILURE() << "row " << id << " is "
                              << (it == got.end() ? std::string{"missing"} : it->second) << ", not "
                              << row;
            }
        }
        for (const auto& [id, row] : got) {
            if (want.count(id) == 0 && ++wrong <= 10) {
                ADD_FAILURE() << "row " << id << " is present (" << row
                              << ") although its last record deletes it";
            }
        }
        EXPECT_EQ(wrong, 0U) << got.size() << " rows, " << wrong
                             << " not as the input's last records leave them";
    }

    void kill_and_replace(Cluster& cluster, std::size_t idx) {
        cluster.worker(idx).kill_hard();
        ASSERT_TRUE(cluster.worker(idx).await_exit(10s).has_value());
        archived_logs_.push_back(cluster.worker(idx).read_log());
        ASSERT_TRUE(cluster.restart_worker(idx, worker_options()));
    }

    static bool parked_at(Cluster& cluster, const std::string& point) {
        const std::string witness = "[fault.injection] fired: " + point;
        return clink::itest::await([&] { return cluster.worker(0).log_contains(witness); }, 120s);
    }

    void keep_logs(const std::string& cell, Cluster& cluster) const {
        const auto into = env_or_empty("CLINK_MYSQL_RECOVERY_LOG_DIR");
        if (into.empty()) {
            return;
        }
        std::error_code ec;
        const auto dir = std::filesystem::path(into) / ("sink-" + cell);
        std::filesystem::create_directories(dir, ec);
        std::filesystem::copy(cluster.log_dir(),
                              dir,
                              std::filesystem::copy_options::recursive |
                                  std::filesystem::copy_options::overwrite_existing,
                              ec);
        for (std::size_t i = 0; i < archived_logs_.size(); ++i) {
            std::ofstream(dir / ("killed-worker-" + std::to_string(i) + ".log"))
                << archived_logs_[i];
        }
    }

    void report(const std::string& cell, Cluster& cluster, const std::string& fields) {
        keep_logs(cell, cluster);
        std::printf("[mysql-sink-recovery] cell=%s records=%lld rows=%d restarts=%zu%s\n",
                    cell.c_str(),
                    static_cast<long long>(feed_->total()),
                    feed_->ids(),
                    restarts(cluster),
                    fields.c_str());
        std::fflush(stdout);
    }

    void append_cell(const std::string& point, const std::string& cell);
    void upsert_cell(const std::string& point, const std::string& cell);

    Dsn dsn_;
    std::string container_;
    std::string table_;
    std::unique_ptr<MysqlSinkFeed> feed_;
    std::vector<std::string> archived_logs_;
};

void MysqlSinkRecovery::append_cell(const std::string& point, const std::string& cell) {
    const bool before_commit = point == "mysql.sink_before_commit";
    make_feed(/*deletes=*/false);
    make_append_table();
    // Reached once per flush with rows, and the sink flushes at barriers: the
    // third lands mid-feed.
    auto cluster = make_cluster(point + "=block@3");
    ScopedDiagnostics diagnostics(*cluster);
    submit(*cluster, "mysql_sink");
    add_spare_worker(*cluster);
    await_first_checkpoint(*cluster);
    feed_->start();
    ASSERT_TRUE(parked_at(*cluster, point)) << "worker 0 never reached " << point;
    const auto served = feed_->served_max();
    const auto committed = number("SELECT COALESCE(MAX(seq), 0) FROM `" + table_ + "`");
    const auto open = open_rows();
    feed_->watch_rewind();
    kill_and_replace(*cluster, 0);
    ASSERT_TRUE(cluster->await_workers_registered(2));
    const auto total = feed_->total();
    await_settled(*cluster, [&] { return appended().size() == static_cast<std::size_t>(total); });
    std::size_t wrong = 0;
    const auto rows = appended(&wrong);
    // The restored source read again from its checkpoint's cursor: records up
    // to it were committed once before the kill, those after it and up to the
    // last committed before the kill twice, and the rest once.
    const auto restored = feed_->lowest_since();
    std::size_t missing = 0;
    std::size_t twice = 0;
    std::size_t unexpected = 0;
    for (std::int64_t n = 1; n <= total; ++n) {
        const int want = n > restored && n <= committed ? 2 : 1;
        const auto it = rows.find(n);
        const int got = it == rows.end() ? 0 : it->second;
        missing += got == 0 ? 1 : 0;
        twice += got == 2 ? 1 : 0;
        if (got != want && ++unexpected <= 5) {
            ADD_FAILURE() << "seq " << n << " is in " << got << " row(s), not " << want;
        }
    }
    std::size_t rows_total = 0;
    for (const auto& [n, c] : rows) {
        rows_total += static_cast<std::size_t>(c);
    }
    const auto replayed = served - std::min(served, restored);
    const auto spent = restarts(*cluster);
    cancel_job(*cluster);
    report(cell,
           *cluster,
           " served_at_fault=" + std::to_string(served) + " committed_at_fault=" +
               std::to_string(committed) + " uncommitted_rows_at_fault=" + std::to_string(open) +
               " restored_cursor=" + std::to_string(restored) + " replayed=" +
               std::to_string(replayed) + " table_rows=" + std::to_string(rows_total) +
               " written_twice=" + std::to_string(twice));
    EXPECT_EQ(rows.size(), static_cast<std::size_t>(total)) << "rows for records never served";
    EXPECT_EQ(missing, 0U) << "records were lost";
    EXPECT_EQ(unexpected, 0U) << "rows written twice other than the replay of committed records";
    EXPECT_EQ(wrong, 0U) << "rows whose columns are not their record's";
    EXPECT_EQ(spent, 1U) << "one worker loss is one restart";
    EXPECT_GT(replayed, 0) << "the restored job read nothing again";
    if (before_commit) {
        EXPECT_GT(open, 0) << "no open transaction held the parked flush's rows";
        EXPECT_LE(committed, restored)
            << "rows committed after the checkpoint the job restored, though the kill came "
               "before the flush's COMMIT";
    } else {
        EXPECT_GT(committed, restored) << "the parked flush was not committed before the kill";
        EXPECT_GT(twice, 0U);
    }
}

void MysqlSinkRecovery::upsert_cell(const std::string& point, const std::string& cell) {
    const bool before_commit = point == "mysql.sink_before_commit";
    make_feed(/*deletes=*/true);
    make_keyed_table("utf8mb4_0900_ai_ci");
    auto cluster = make_cluster(point + "=block@3");
    ScopedDiagnostics diagnostics(*cluster);
    submit(*cluster, "mysql_upsert_sink");
    add_spare_worker(*cluster);
    await_first_checkpoint(*cluster);
    feed_->start();
    ASSERT_TRUE(parked_at(*cluster, point)) << "worker 0 never reached " << point;
    const auto served = feed_->served_max();
    const auto stored_at_park = stored().size();
    const auto open = open_rows();
    feed_->watch_rewind();
    kill_and_replace(*cluster, 0);
    ASSERT_TRUE(cluster->await_workers_registered(2));
    const auto want = feed_->expected(feed_->total());
    await_settled(*cluster, [&] { return stored() == want; });
    expect_keyed(want);
    const auto replayed = served - std::min(served, feed_->lowest_since());
    const auto spent = restarts(*cluster);
    cancel_job(*cluster);
    report(cell,
           *cluster,
           " served_at_fault=" + std::to_string(served) +
               " rows_stored_at_fault=" + std::to_string(stored_at_park) +
               " uncommitted_rows_at_fault=" + std::to_string(open) + " replayed=" +
               std::to_string(replayed) + " rows_present_at_end=" + std::to_string(want.size()) +
               " rows_deleted_at_end=" + std::to_string(feed_->deleted(feed_->total())));
    EXPECT_EQ(spent, 1U) << "one worker loss is one restart";
    EXPECT_GT(replayed, 0) << "the restored job read nothing again";
    EXPECT_GT(stored_at_park, 0U) << "nothing was committed when the worker parked";
    if (before_commit) {
        EXPECT_GT(open, 0) << "no open transaction held the parked flush's rows";
    }
}

}  // namespace

TEST_F(MysqlSinkRecovery, AnAppendSinkKilledBeforeCommitWritesEveryRecord) {
    append_cell("mysql.sink_before_commit", "append_before_commit");
}

TEST_F(MysqlSinkRecovery, AnAppendSinkKilledAfterCommitWritesTheReplayedRecordsAgain) {
    append_cell("mysql.sink_after_commit", "append_after_commit");
}

TEST_F(MysqlSinkRecovery, AnUpsertSinkKilledBeforeCommitConverges) {
    upsert_cell("mysql.sink_before_commit", "upsert_before_commit");
}

TEST_F(MysqlSinkRecovery, AnUpsertSinkKilledAfterCommitConvergesOnReplay) {
    upsert_cell("mysql.sink_after_commit", "upsert_after_commit");
}

TEST_F(MysqlSinkRecovery, ASqlUpsertJobKilledAfterCommitConvergesOnReplay) {
    if (!std::filesystem::exists(sql_binary())) {
        GTEST_SKIP() << "the clink_submit_sql binary is not built";
    }
    make_feed(/*deletes=*/false);
    // At parallelism 2 the sink takes only a key column that tells values apart
    // as the stream does: a binary NO PAD collation.
    make_keyed_table("utf8mb4_0900_bin");
    // A GROUP BY's counts per row: every record replaces its row's count with an
    // update_before and update_after pair, a delete and an upsert by key.
    const std::string sql =
        "CREATE TABLE feed (id STRING, seq BIGINT, v BIGINT) WITH (connector='http_poll', "
        "format='json', url='" +
        feed_->url() +
        "', path='/feed', records_field='items', cursor_param='since', cursor_field='seq', "
        "poll_interval_ms='20');\n"
        "CREATE TABLE counts (id STRING, seq BIGINT, v BIGINT) WITH (connector='mysql', host='" +
        dsn_.host + "', port='" + dsn_.port + "', user='" + dsn_.user + "', password='env://" +
        kPasswordVar + "', database='" + dsn_.database + "', table='" + table_ +
        "', mode='upsert', primary_key='id');\n"
        "INSERT INTO counts SELECT id, COUNT(*) AS seq, MAX(v) AS v FROM feed GROUP BY id;\n";
    const std::string point = "mysql.sink_after_commit";
    auto cluster = make_cluster(point + "=block@3", /*slots=*/8);
    ScopedDiagnostics diagnostics(*cluster);
    submit_sql(*cluster, sql, /*parallelism=*/2);
    add_spare_worker(*cluster);
    await_first_checkpoint(*cluster);
    feed_->start();
    ASSERT_TRUE(parked_at(*cluster, point)) << "worker 0 never reached " << point;
    const auto served = feed_->served_max();
    const auto stored_at_park = stored().size();
    feed_->watch_rewind();
    kill_and_replace(*cluster, 0);
    ASSERT_TRUE(cluster->await_workers_registered(2));
    // Each row's count of records and its largest v, exactly: a replayed record
    // rewrites the row, and is never counted twice.
    std::map<std::string, std::int64_t> count;
    std::map<std::string, std::int64_t> last;
    for (std::int64_t n = 1; n <= feed_->total(); ++n) {
        ++count[feed_->id_of(n)];
        last[feed_->id_of(n)] = n;
    }
    std::map<std::string, std::string> want;
    for (const auto& [id, c] : count) {
        want[id] = std::to_string(c) + "/" + std::to_string(last[id] * 10);
    }
    await_settled(*cluster, [&] { return stored() == want; });
    expect_keyed(want);
    const auto replayed = served - std::min(served, feed_->lowest_since());
    const auto spent = restarts(*cluster);
    cancel_job(*cluster);
    report("sql_after_commit",
           *cluster,
           " served_at_fault=" + std::to_string(served) +
               " rows_stored_at_fault=" + std::to_string(stored_at_park) +
               " replayed=" + std::to_string(replayed) + " parallelism=2");
    EXPECT_EQ(spent, 1U) << "one worker loss is one restart";
    EXPECT_GT(replayed, 0) << "the restored job read nothing again";
    EXPECT_GT(stored_at_park, 0U);
}
