// The MySQL CDC source's recovery cells: a coordinator and two clink_node
// workers run a job reading a table's binlog (mysql_cdc_source) into the file
// two-phase-commit sink, and worker 0 is SIGKILLed while the unit the source
// keeps whole is open:
//
//   1. snapshot: the initial snapshot of a 30000-row table is slowed at
//      mysql_cdc.snapshot_after_emit, so it spans several checkpoint
//      intervals. The coordinator triggers checkpoints throughout and none
//      completes while the snapshot is partial; worker 0 is killed part-way,
//      the job restarts from no checkpoint, and the restored source snapshots
//      afresh. Changes made after the kill stream after the new snapshot.
//   2. transaction: after a checkpoint completes, one transaction inserts 20000
//      rows, and the stream is slowed inside it at
//      mysql_cdc.in_transaction_after_emit. No checkpoint completes while the
//      source is inside it; worker 0 is killed part-way, the job restores the
//      checkpoint before the transaction, and the source reads all of it again.
//   3. sql: cell 2 as a SQL job (connector='mysql', mode='cdc' into a
//      filesystem table with delivery_guarantee='exactly_once') submitted by
//      clink_submit_sql.
//
// The pass criteria come from the sink's committed files and from the table
// read through the mysql client in the server's container, never from what
// clink reports: every committed line is committed once, and the committed
// changes applied by key equal the table (cell 1), or every inserted row is
// committed exactly once (cells 2 and 3). The checkpoints triggered and not
// completed while the unit was open, the fault and the restart are witnessed
// through the coordinator's metrics, its checkpoint directory and the logs.
//
// Needs the server scripts/mysql-cdc-live.sh starts (CLINK_MYSQL_TEST_DSN and
// CLINK_MYSQL_TEST_CONTAINER) and the clink_node binary of a build with fault
// injection (clink_submit_sql too for cell 3). Each cell prints one
// "[mysql-cdc-recovery]" line and, with CLINK_MYSQL_RECOVERY_LOG_DIR set, keeps
// the cluster's logs there.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/job_graph.hpp"
#include "clink/config/json.hpp"
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

// Every committed line, in commit order: files by checkpoint id, lines in order.
std::vector<std::string> committed_lines(const std::filesystem::path& dir) {
    std::vector<std::pair<std::uint64_t, std::filesystem::path>> files;
    std::error_code ec;
    for (const auto& e : std::filesystem::directory_iterator(dir / "committed", ec)) {
        const auto stem = e.path().stem().string();  // sub<N>-<run>-<ckpt>
        const auto dash = stem.rfind('-');
        if (dash == std::string::npos) {
            continue;
        }
        files.emplace_back(std::stoull(stem.substr(dash + 1)), e.path());
    }
    std::sort(files.begin(), files.end());
    std::vector<std::string> lines;
    for (const auto& [ckpt, path] : files) {
        std::ifstream in(path);
        for (std::string line; std::getline(in, line);) {
            if (!line.empty()) {
                lines.push_back(line);
            }
        }
    }
    return lines;
}

// A field of a committed JSON line, as text whether written as a string or a
// number.
std::string field(const clink::config::JsonObject& o, const std::string& k) {
    const auto it = o.find(k);
    if (it == o.end()) {
        return {};
    }
    if (it->second.is_string()) {
        return it->second.as_string();
    }
    if (it->second.is_integral_number()) {
        return std::to_string(it->second.as_int());
    }
    return it->second.serialize();
}

class MysqlCdcRecovery : public ::testing::Test {
protected:
    void SetUp() override {
        const auto dsn = env_or_empty("CLINK_MYSQL_TEST_DSN");
        container_ = env_or_empty("CLINK_MYSQL_TEST_CONTAINER");
        if (dsn.empty() || container_.empty()) {
            GTEST_SKIP() << "set CLINK_MYSQL_TEST_DSN and CLINK_MYSQL_TEST_CONTAINER "
                            "(scripts/mysql-cdc-live.sh does)";
        }
        if (!std::filesystem::exists(node_binary())) {
            GTEST_SKIP() << "the clink_node binary is not built";
        }
        dsn_ = parse_dsn(dsn);
        static int counter = 0;
        table_ = "rcdc_" + std::to_string(::getpid()) + "_" + std::to_string(++counter);
        server_id_ = std::to_string(6100 + counter);
        mysql("DROP TABLE IF EXISTS `" + table_ + "`; CREATE TABLE `" + table_ +
              "` (id INT PRIMARY KEY, val VARCHAR(64)) ENGINE=InnoDB");
    }

    void TearDown() override {
        if (!table_.empty() && !container_.empty()) {
            (void)run_mysql("DROP TABLE IF EXISTS `" + table_ + "`");
        }
    }

    // Statements through the mysql client inside the server's container: the
    // workload's writes and the verifier's reads, apart from clink's own client.
    // They go in on standard input, which has no length limit.
    clink::test::DockerCommandResult run_mysql(const std::string& statements) const {
        const auto file = std::filesystem::temp_directory_path() /
                          ("clink-mysql-cdc-recovery-" + std::to_string(::getpid()) + ".sql");
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

    // id -> val, read from the table.
    std::map<std::string, std::string> table_state() const {
        std::map<std::string, std::string> out;
        std::istringstream in(mysql("SELECT id, val FROM `" + table_ + "`"));
        for (std::string line; std::getline(in, line);) {
            const auto tab = line.find('\t');
            if (tab != std::string::npos) {
                out[line.substr(0, tab)] = line.substr(tab + 1);
            }
        }
        return out;
    }

    // `count` rows id first.. with val v<id>, in one transaction of statements
    // of 1000 rows each.
    void insert_rows(int first, int count, bool one_transaction) const {
        std::string sql = one_transaction ? "BEGIN; " : "";
        for (int i = first; i < first + count; i += 1000) {
            sql += "INSERT INTO `" + table_ + "` VALUES ";
            for (int k = i; k < std::min(i + 1000, first + count); ++k) {
                sql += (k == i ? "(" : ",(") + std::to_string(k) + ",'v" + std::to_string(k) + "')";
            }
            sql += "; ";
        }
        if (one_transaction) {
            sql += "COMMIT;";
        }
        mysql(sql);
    }

    std::map<std::string, std::string> source_params(bool snapshot) const {
        std::map<std::string, std::string> p = {
            {"host", dsn_.host},
            {"port", dsn_.port},
            {"user", dsn_.user},
            {"password", std::string{"env://"} + kPasswordVar},
            {"database", dsn_.database},
            {"server_id", server_id_},
            {"tables", dsn_.database + "." + table_},
            {"heartbeat_ms", "200"},
        };
        if (snapshot) {
            p["initial_snapshot"] = "true";
        }
        return p;
    }

    clink::cluster::JobGraphSpec graph(bool snapshot) const {
        clink::cluster::JobGraphSpec g;
        clink::cluster::OperatorSpec src;
        src.type = "mysql_cdc_source";
        src.id = "src";
        src.uid = "mysql-cdc-recovery-src";
        src.out_channel = std::string{clink::cluster::kChannelString};
        src.params = source_params(snapshot);
        g.ops.push_back(src);
        clink::cluster::OperatorSpec snk;
        snk.type = "file_2pc_sink_string";
        snk.id = "snk";
        snk.uid = "mysql-cdc-recovery-snk";
        snk.inputs = {"src"};
        snk.out_channel = std::string{clink::cluster::kChannelString};
        snk.params = {{"dir", out_dir_.string()}, {"delivery_guarantee", "exactly_once"}};
        g.ops.push_back(snk);
        return g;
    }

    std::string sql_job() const {
        return "CREATE TABLE src (id INT, val STRING) WITH (connector='mysql', mode='cdc', "
               "format='json', host='" +
               dsn_.host + "', port='" + dsn_.port + "', user='" + dsn_.user +
               "', password='env://" + kPasswordVar + "', database='" + dsn_.database +
               "', server_id='" + server_id_ + "', tables='" + dsn_.database + "." + table_ +
               "', heartbeat_ms='200');\n"
               "CREATE TABLE out (id INT, val STRING) WITH (connector='filesystem', path='" +
               out_dir_.string() +
               "', format='json', delivery_guarantee='exactly_once');\n"
               "INSERT INTO out SELECT id, val FROM src;\n";
    }

    ProcOptions worker_options(std::string fault = {}) const {
        ProcOptions o;
        o.fault = std::move(fault);
        o.env = {{kPasswordVar, dsn_.password}};
        return o;
    }

    // A coordinator and worker 0, armed with `fault`: the job submitted next
    // places every task on worker 0. Worker 1 joins after the deploy, unarmed,
    // as the spare the restart uses.
    std::unique_ptr<Cluster> make_cluster(const std::string& fault) {
        ClusterSpec spec;
        spec.node_binary = node_binary();
        spec.workers = 2;
        spec.slots_per_worker = 4;
        spec.http = true;
        auto cluster = std::make_unique<Cluster>(spec);
        if (!cluster->start_coordinator() || !cluster->start_worker(0, worker_options(fault)) ||
            !cluster->await_workers_registered(1)) {
            ADD_FAILURE() << "the cluster did not start";
        }
        out_dir_ = cluster->checkpoint_dir().parent_path() / "mysql-cdc-out";
        return cluster;
    }

    static clink::http_connector::HttpRequest coordinator(const Cluster& cluster) {
        clink::http_connector::HttpRequest::Options o;
        o.base_url = "http://127.0.0.1:" + std::to_string(cluster.http_port());
        return clink::http_connector::HttpRequest(std::move(o));
    }

    void submit(Cluster& cluster, bool snapshot) {
        auto http = coordinator(cluster);
        const auto r = http.post("/api/v1/jobs/spec?name=mysql-cdc-recovery&checkpoint_dir=" +
                                     cluster.checkpoint_dir().string() +
                                     "&checkpoint_interval_ms=500&alignment=aligned"
                                     "&max_restarts_on_worker_loss=8",
                                 graph(snapshot).to_json(),
                                 "application/json");
        ASSERT_NE(r.body.find("\"ok\":true"), std::string::npos)
            << "the job was not accepted: " << r.body;
    }

    void submit_sql(Cluster& cluster) {
        ASSERT_TRUE(std::filesystem::exists(sql_binary())) << "clink_submit_sql is not built";
        clink::itest::Process submit;
        ASSERT_TRUE(submit.spawn("submit-sql",
                                 sql_binary(),
                                 {sql_binary().string(),
                                  "-e",
                                  sql_job(),
                                  "--coordinator-host",
                                  "127.0.0.1",
                                  "--coordinator-port",
                                  std::to_string(cluster.http_port()),
                                  "--name",
                                  "mysql-cdc-recovery-sql",
                                  "--checkpoint-dir",
                                  cluster.checkpoint_dir().string(),
                                  "--checkpoint-interval-ms",
                                  "500",
                                  "--alignment",
                                  "aligned",
                                  "--max-restarts-on-worker-loss",
                                  "8"},
                                 cluster.log_dir()));
        const auto code = submit.await_exit(60s);
        ASSERT_TRUE(code.has_value());
        ASSERT_EQ(*code, 0) << submit.read_log();
    }

    void add_spare_worker(Cluster& cluster) {
        ASSERT_TRUE(cluster.start_worker(1, worker_options()));
        ASSERT_TRUE(cluster.await_workers_registered(2));
        started_workers_ = 2;
    }

    std::vector<std::string> worker_logs(Cluster& cluster) const {
        std::vector<std::string> out = archived_logs_;
        for (std::size_t i = 0; i < started_workers_; ++i) {
            out.push_back(cluster.worker(i).read_log());
        }
        return out;
    }

    std::size_t count_in_workers(Cluster& cluster, const std::string& needle) const {
        std::size_t n = 0;
        for (const auto& log : worker_logs(cluster)) {
            for (auto pos = log.find(needle); pos != std::string::npos;
                 pos = log.find(needle, pos + 1)) {
                ++n;
            }
        }
        return n;
    }

    static std::size_t fired(Cluster& cluster, const std::string& point) {
        const auto log = cluster.worker(0).read_log();
        const std::string needle = "[fault.injection] fired: " + point;
        std::size_t n = 0;
        for (auto pos = log.find(needle); pos != std::string::npos;
             pos = log.find(needle, pos + 1)) {
            ++n;
        }
        return n;
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

    // The coordinator's count of checkpoints it has triggered.
    static std::uint64_t triggered(const Cluster& cluster) {
        auto http = coordinator(cluster);
        const auto body = http.get("/metrics").body;
        std::istringstream in(body);
        for (std::string line; std::getline(in, line);) {
            if (line.rfind("clink_ckpt_triggered_total", 0) == 0) {
                const auto space = line.rfind(' ');
                return static_cast<std::uint64_t>(std::stod(line.substr(space + 1)));
            }
        }
        return 0;
    }

    void await_first_checkpoint(Cluster& cluster) {
        ASSERT_TRUE(clink::itest::await([&] { return latest_completed(cluster) >= 1; }, 90s))
            << "no checkpoint completed";
    }

    bool await_fired(Cluster& cluster, const std::string& point, std::size_t n) {
        return clink::itest::await([&] { return fired(cluster, point) >= n; }, 120s);
    }

    void kill_and_replace(Cluster& cluster, std::size_t idx) {
        cluster.worker(idx).kill_hard();
        ASSERT_TRUE(cluster.worker(idx).await_exit(10s).has_value());
        archived_logs_.push_back(cluster.worker(idx).read_log());
        ASSERT_TRUE(cluster.restart_worker(idx, worker_options()));
    }

    // Two more checkpoints complete after `done` holds, so nothing is still on
    // its way into the committed files.
    void await_settled(Cluster& cluster, const std::function<bool()>& done) {
        ASSERT_TRUE(clink::itest::await(done, 240s)) << "the committed output never converged";
        const auto after = latest_completed(cluster);
        ASSERT_TRUE(
            clink::itest::await([&] { return latest_completed(cluster) >= after + 2; }, 60s))
            << "checkpoints stopped completing";
    }

    // Lines committed more than once.
    static std::size_t doubled(const std::vector<std::string>& lines) {
        std::map<std::string, int> seen;
        std::size_t n = 0;
        for (const auto& l : lines) {
            if (++seen[l] == 2) {
                ++n;
            }
        }
        return n;
    }

    void keep_logs(const char* cell, Cluster& cluster) const {
        const auto into = env_or_empty("CLINK_MYSQL_RECOVERY_LOG_DIR");
        if (into.empty()) {
            return;
        }
        std::error_code ec;
        const auto dir = std::filesystem::path(into) / cell;
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

    // The transaction cells: every inserted row committed exactly once.
    void expect_each_row_once(const std::vector<std::string>& lines, int first, int count) {
        std::map<std::string, int> ids;
        for (const auto& l : lines) {
            const auto j = clink::config::parse(l);
            ASSERT_TRUE(j.is_object()) << l;
            ++ids[field(j.as_object(), "id")];
        }
        std::size_t missing = 0;
        std::size_t extra = 0;
        for (int k = first; k < first + count; ++k) {
            const auto it = ids.find(std::to_string(k));
            if (it == ids.end()) {
                ++missing;
            } else if (it->second != 1) {
                ++extra;
            }
        }
        EXPECT_EQ(missing, 0U) << "rows of the transaction were never committed";
        EXPECT_EQ(extra, 0U) << "rows of the transaction were committed twice";
        EXPECT_EQ(lines.size(), static_cast<std::size_t>(count));
    }

    void transaction_cell(const char* cell, bool sql);

    Dsn dsn_;
    std::string container_;
    std::string table_;
    std::string server_id_;
    std::filesystem::path out_dir_;
    std::vector<std::string> archived_logs_;
    std::size_t started_workers_{1};
};

TEST_F(MysqlCdcRecovery, AKillDuringTheSnapshotRestartsItWithNoCheckpointInside) {
    constexpr int kRows = 30000;
    insert_rows(0, kRows, false);
    const std::string point = "mysql_cdc.snapshot_after_emit";
    auto cluster = make_cluster(point + "=delay:400");
    ScopedDiagnostics diagnostics(*cluster);
    submit(*cluster, /*snapshot=*/true);
    add_spare_worker(*cluster);
    ASSERT_TRUE(await_fired(*cluster, point, 2)) << "the snapshot never emitted";
    const auto triggered_early = triggered(*cluster);
    ASSERT_TRUE(await_fired(*cluster, point, 8)) << "the snapshot stopped part-way";
    // While the snapshot was partial the coordinator kept triggering, and no
    // checkpoint completed: none can cover part of it.
    const auto triggered_at_kill = triggered(*cluster);
    const auto completed_at_kill = latest_completed(*cluster);
    const auto committed_at_kill = committed_lines(out_dir_).size();
    EXPECT_GE(triggered_at_kill, triggered_early + 3) << "the coordinator stopped triggering";
    EXPECT_EQ(completed_at_kill, 0U) << "a checkpoint completed inside the snapshot";
    EXPECT_EQ(committed_at_kill, 0U);
    kill_and_replace(*cluster, 0);
    ASSERT_TRUE(cluster->await_workers_registered(2));

    // Changes after the kill, while the job restarts: in the new snapshot or
    // streamed after it.
    mysql("UPDATE `" + table_ + "` SET val = 'updated' WHERE id % 10 = 1; DELETE FROM `" + table_ +
          "` WHERE id % 10 = 2; INSERT INTO `" + table_ +
          "` VALUES (100000, 'new'), (100001, 'new')");
    // And changes after the restored source has finished its snapshot, which
    // only the stream can carry.
    ASSERT_TRUE(clink::itest::await(
        [&] { return count_in_workers(*cluster, " initial snapshot finished, ") >= 1; }, 120s))
        << "the restored source never finished its snapshot";
    mysql("UPDATE `" + table_ + "` SET val = 'streamed' WHERE id % 10 = 3; DELETE FROM `" + table_ +
          "` WHERE id % 10 = 4; INSERT INTO `" + table_ + "` VALUES (100002, 'streamed')");
    const auto expected = table_state();
    std::map<std::string, std::string> state;
    std::vector<std::string> lines;
    await_settled(*cluster, [&] {
        lines = committed_lines(out_dir_);
        state.clear();
        for (const auto& l : lines) {
            const auto j = clink::config::parse(l);
            if (!j.is_object()) {
                continue;
            }
            const auto& o = j.as_object();
            if (field(o, "__op") == "delete") {
                state.erase(field(o, "id"));
            } else {
                state[field(o, "id")] = field(o, "val");
            }
        }
        return state == expected;
    });
    lines = committed_lines(out_dir_);
    EXPECT_EQ(doubled(lines), 0U) << "lines were committed twice";
    EXPECT_EQ(state, expected);
    // The second wave came after the restored snapshot had finished, so each
    // of its changes arrived once, as a change. (Where the first wave lands,
    // in the snapshot or the stream, depends on when the restored snapshot
    // took its point.)
    std::size_t streamed = 0;
    for (const auto& l : lines) {
        const auto j = clink::config::parse(l);
        if (!j.is_object()) {
            continue;
        }
        const auto& o = j.as_object();
        const auto op = field(o, "__op");
        const auto id = field(o, "id");
        const bool wave2_update = op == "update" && field(o, "val") == "streamed";
        const bool wave2_delete = op == "delete" && !id.empty() && std::stoll(id) % 10 == 4;
        if (wave2_update || wave2_delete) {
            ++streamed;
        }
    }
    EXPECT_EQ(streamed, static_cast<std::size_t>(kRows / 10 * 2))
        << "the changes after the restored snapshot did not each stream once";
    EXPECT_EQ(restarts(*cluster), 1U);
    // The killed attempt's snapshot and the restored one's.
    EXPECT_GE(count_in_workers(*cluster, " initial snapshot of 1 table(s) as of "), 2U);
    keep_logs("snapshot", *cluster);
    std::printf(
        "[mysql-cdc-recovery] cell=snapshot rows=%d committed=%zu doubled=%zu "
        "streamed_after_snapshot=%zu triggered_while_partial=%llu completed_while_partial=%llu "
        "committed_at_kill=%zu restarts=%zu\n",
        kRows,
        lines.size(),
        doubled(lines),
        streamed,
        static_cast<unsigned long long>(triggered_at_kill - triggered_early),
        static_cast<unsigned long long>(completed_at_kill),
        committed_at_kill,
        restarts(*cluster));
    std::fflush(stdout);
}

void MysqlCdcRecovery::transaction_cell(const char* cell, bool sql) {
    constexpr int kRows = 20000;
    const std::string point = "mysql_cdc.in_transaction_after_emit";
    auto cluster = make_cluster(point + "=delay:400");
    ScopedDiagnostics diagnostics(*cluster);
    if (sql) {
        submit_sql(*cluster);
    } else {
        submit(*cluster, /*snapshot=*/false);
    }
    add_spare_worker(*cluster);
    await_first_checkpoint(*cluster);
    insert_rows(0, kRows, /*one_transaction=*/true);
    // Inside the transaction, slowed: the runner asks between every produce()
    // call, and the source holds each barrier it is given.
    ASSERT_TRUE(await_fired(*cluster, point, 3)) << "the source never read the transaction";
    const auto completed_inside = latest_completed(*cluster);
    const auto triggered_inside = triggered(*cluster);
    ASSERT_TRUE(await_fired(*cluster, point, 9)) << "the transaction ended too soon";
    const auto completed_at_kill = latest_completed(*cluster);
    const auto triggered_at_kill = triggered(*cluster);
    const auto committed_at_kill = committed_lines(out_dir_).size();
    EXPECT_GE(triggered_at_kill, triggered_inside + 3) << "the coordinator stopped triggering";
    EXPECT_EQ(completed_at_kill, completed_inside)
        << "a checkpoint completed inside the transaction";
    EXPECT_EQ(committed_at_kill, 0U) << "rows of the transaction were committed before its end";
    kill_and_replace(*cluster, 0);
    ASSERT_TRUE(cluster->await_workers_registered(2));
    std::vector<std::string> lines;
    await_settled(*cluster, [&] {
        lines = committed_lines(out_dir_);
        return lines.size() >= static_cast<std::size_t>(kRows);
    });
    lines = committed_lines(out_dir_);
    expect_each_row_once(lines, 0, kRows);
    EXPECT_EQ(restarts(*cluster), 1U);
    keep_logs(cell, *cluster);
    std::printf(
        "[mysql-cdc-recovery] cell=%s rows=%d committed=%zu doubled=%zu "
        "triggered_inside=%llu completed_inside=%llu committed_at_kill=%zu restarts=%zu\n",
        cell,
        kRows,
        lines.size(),
        doubled(lines),
        static_cast<unsigned long long>(triggered_at_kill - triggered_inside),
        static_cast<unsigned long long>(completed_at_kill - completed_inside),
        committed_at_kill,
        restarts(*cluster));
    std::fflush(stdout);
}

TEST_F(MysqlCdcRecovery, AKillInsideALargeTransactionReplaysAllOfIt) {
    transaction_cell("transaction", /*sql=*/false);
}

TEST_F(MysqlCdcRecovery, AKillInsideALargeTransactionReplaysAllOfItThroughSql) {
    transaction_cell("sql", /*sql=*/true);
}

}  // namespace
