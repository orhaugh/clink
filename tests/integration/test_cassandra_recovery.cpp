// The Cassandra and ScyllaDB sinks' recovery cells: a coordinator and two
// clink_node workers run a job that polls a feed this test serves
// (http_poll_source, replayable from its checkpointed cursor) into a sink, while
// the feed releases records at a steady rate. Record n names row k<n mod ids>,
// with a rising seq and v, so every row is written many times; in the upsert
// cells every seventh record of a row is a delete, so a row ends present or
// deleted depending on its last record. Each cell parks worker 0 at one of the
// sink's fault points, SIGKILLs it there, and lets the job restore:
//
//   1. the plain sink parked at cassandra_sink.in_flight: writes handed to the
//      server and not acknowledged, the barrier that asked for them waiting;
//   2. the upsert sink parked at cassandra_upsert_sink.before_write: rows
//      buffered, none of them sent;
//   3. the upsert sink parked at cassandra_upsert_sink.after_write: every write
//      before a barrier acknowledged, the barrier not yet acknowledged, so the
//      restored job writes rows the table already holds;
//   4. cell 3 as a SQL job (connector='http_poll' into connector='cassandra'
//      mode='upsert') at parallelism 2, through a GROUP BY whose counts replace
//      by key with update_before and update_after rows.
//
// Pass criteria, read with a driver session of the test's own and never from
// anything clink reports: every row the input's last records leave present,
// column for column; every row their last records delete, absent; the one
// restart the kill costs; a replay (the feed was read again from the
// checkpoint's cursor); and the fault landing where the cell names, witnessed by
// the fault framework's log line. The contract checked is the sinks' declared
// one, final-state convergence with each row written by one sink subtask; a
// replay rewrites rows, which the cells count and do not forbid.
//
// Needs the server scripts/cassandra-live.sh starts (CLINK_CASSANDRA_TEST_
// CONTACT_POINTS and _PORT) and the clink_node binary of a build with fault
// injection (and clink_submit_sql for cell 4). Each cell prints one
// "[cassandra-recovery]" line and, with CLINK_CASSANDRA_RECOVERY_LOG_DIR set,
// keeps the cluster's logs there.

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
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/job_graph.hpp"
#include "clink/http_connector/http_request.hpp"

#include "../checkpoint_markers.hpp"
#include "cassandra_live.hpp"
#include "tests/integration/cluster_harness.hpp"

namespace {

using namespace std::chrono_literals;
using clink::itest::Cluster;
using clink::itest::ClusterSpec;
using clink::itest::ProcOptions;
using clink::itest::ScopedDiagnostics;
namespace live = clink::cassandra::live;

std::filesystem::path recovery_node_binary() {
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

// The job's input. GET /feed?since=<seq> answers {"items":[...]}: up to 200 of
// the records released so far whose seq is after <seq>, ascending, the exclusive
// cursor http_poll_source replays from.
class Feed {
public:
    Feed(std::int64_t total, int ids, int per_second, bool deletes)
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
    ~Feed() {
        stop_.store(true);
        if (releaser_.joinable()) {
            releaser_.join();
        }
        svr_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    Feed(const Feed&) = delete;
    Feed& operator=(const Feed&) = delete;

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

    // What the table holds once the first `upto` records are written: each
    // row's last record, "seq/v", or no entry when that record deletes it.
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
    // The rows the first `upto` records end deleted.
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

class CassandraRecovery : public ::testing::Test {
protected:
    void SetUp() override {
        if (!live::configured()) {
            GTEST_SKIP() << "set CLINK_CASSANDRA_TEST_CONTACT_POINTS (scripts/cassandra-live.sh "
                            "does)";
        }
        if (!std::filesystem::exists(recovery_node_binary())) {
            GTEST_SKIP() << "the clink_node binary is not built";
        }
        session_ = std::make_unique<live::Session>();
        static int counter = 0;
        table_ = "rec_" + std::to_string(static_cast<long>(::getpid())) + "_" +
                 std::to_string(++counter);
    }

    void TearDown() override {
        feed_.reset();
        if (session_) {
            (void)session_->exec(std::string{"DROP TABLE IF EXISTS "} + live::kKeyspace + "." +
                                 table_);
        }
    }

    void make_feed(bool deletes) { feed_ = std::make_unique<Feed>(3000, 100, 300, deletes); }

    void make_table(const std::string& columns) { live::fresh_table(*session_, table_, columns); }

    // The table as "seq/v" by id, read with SELECT JSON.
    std::map<std::string, std::string> stored() const {
        std::map<std::string, std::string> out;
        for (const auto& r : session_->rows(std::string{live::kKeyspace} + "." + table_)) {
            const auto& o = r.as_object();
            out[o.at("id").as_string()] = o.at("seq").serialize(0) + "/" + o.at("v").serialize(0);
        }
        return out;
    }

    // Rows whose state differs from what the records served so far leave them
    // in: writes not yet applied.
    std::size_t in_flight_rows() const {
        const auto want = feed_->expected(feed_->served_max());
        const auto got = stored();
        std::size_t n = 0;
        for (const auto& [id, row] : want) {
            const auto it = got.find(id);
            if (it == got.end() || it->second != row) {
                ++n;
            }
        }
        for (const auto& [id, row] : got) {
            if (want.count(id) == 0) {
                ++n;
            }
        }
        return n;
    }

    clink::cluster::JobGraphSpec graph(const std::string& sink) const {
        clink::cluster::JobGraphSpec g;
        clink::cluster::OperatorSpec src;
        src.type = "http_poll_source";
        src.id = "src";
        src.uid = "cassandra-recovery-src";
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
        snk.uid = "cassandra-recovery-snk";
        snk.inputs = {"src"};
        snk.out_channel = std::string{clink::cluster::kChannelString};
        snk.params = {{"contact_points", live::contact_points()},
                      {"port", std::to_string(live::port())},
                      {"keyspace", live::kKeyspace},
                      {"table", table_},
                      {"max_in_flight", "32"}};
        if (sink == "cassandra_upsert_sink_string") {
            snk.params["key_columns"] = "id";
            snk.params["max_buffered_records"] = "40";
        }
        g.ops.push_back(snk);
        return g;
    }

    // A coordinator and worker 0 only, armed with `fault`; worker 1 joins after
    // the deploy as the spare a restart can use.
    std::unique_ptr<Cluster> make_cluster(const std::string& fault, int slots = 3) {
        ClusterSpec spec;
        spec.node_binary = recovery_node_binary();
        spec.workers = 2;
        spec.slots_per_worker = slots;
        spec.http = true;
        auto cluster = std::make_unique<Cluster>(spec);
        ProcOptions o;
        o.fault = fault;
        if (!cluster->start_coordinator() || !cluster->start_worker(0, o) ||
            !cluster->await_workers_registered(1)) {
            ADD_FAILURE() << "the cluster did not start";
        }
        return cluster;
    }

    void add_spare_worker(Cluster& cluster) {
        ASSERT_TRUE(cluster.start_worker(1, ProcOptions{}));
        ASSERT_TRUE(cluster.await_workers_registered(2));
        started_workers_ = 2;
    }

    static clink::http_connector::HttpRequest coordinator(const Cluster& cluster) {
        clink::http_connector::HttpRequest::Options o;
        o.base_url = "http://127.0.0.1:" + std::to_string(cluster.http_port());
        return clink::http_connector::HttpRequest(std::move(o));
    }

    void submit(Cluster& cluster, const std::string& sink) {
        auto http = coordinator(cluster);
        const auto r = http.post("/api/v1/jobs/spec?name=cassandra-recovery&checkpoint_dir=" +
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
                                  "cassandra-recovery-sql",
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

    // Every record released, the table holding exactly what they leave, and two
    // more checkpoints completed after that.
    void await_converged(Cluster& cluster,
                         const std::function<std::map<std::string, std::string>()>& want,
                         std::chrono::seconds timeout) {
        const bool converged = clink::itest::await(
            [&] { return feed_->released() == feed_->total() && stored() == want(); }, timeout);
        if (!converged) {
            const auto expected = want();
            const auto got = stored();
            std::size_t wrong = 0;
            for (const auto& [id, row] : expected) {
                const auto it = got.find(id);
                if ((it == got.end() || it->second != row) && ++wrong <= 5) {
                    ADD_FAILURE() << "row " << id << " is "
                                  << (it == got.end() ? std::string{"missing"} : it->second)
                                  << ", not " << row;
                }
            }
            for (const auto& [id, row] : got) {
                if (expected.count(id) == 0 && ++wrong <= 10) {
                    ADD_FAILURE() << "row " << id << " is present (" << row
                                  << ") although its last record deletes it";
                }
            }
            FAIL() << "the table did not converge: " << got.size() << " rows, " << wrong
                   << " not as the input's last records leave them";
        }
        const auto after = latest_completed(cluster);
        ASSERT_TRUE(
            clink::itest::await([&] { return latest_completed(cluster) >= after + 2; }, 60s))
            << "checkpoints stopped completing after the table converged";
    }

    void kill_and_replace(Cluster& cluster, std::size_t idx) {
        cluster.worker(idx).kill_hard();
        ASSERT_TRUE(cluster.worker(idx).await_exit(10s).has_value());
        archived_logs_.push_back(cluster.worker(idx).read_log());
        ASSERT_TRUE(cluster.restart_worker(idx, ProcOptions{}));
    }

    static bool parked_at(Cluster& cluster, const std::string& point) {
        const std::string witness = "[fault.injection] fired: " + point;
        return clink::itest::await([&] { return cluster.worker(0).log_contains(witness); }, 120s);
    }

    void keep_logs(const std::string& cell, Cluster& cluster) const {
        const auto into = live::env_or("CLINK_CASSANDRA_RECOVERY_LOG_DIR");
        if (into.empty()) {
            return;
        }
        std::error_code ec;
        const auto dir = std::filesystem::path(into) / (live::product() + "-" + cell);
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

    void report(const std::string& cell,
                Cluster& cluster,
                std::size_t restarts_spent,
                std::int64_t served_at_fault,
                std::size_t in_flight_at_fault,
                const std::string& extra) {
        keep_logs(cell, cluster);
        std::printf(
            "[cassandra-recovery] product=%s cell=%s records=%lld rows=%d restarts=%zu "
            "served_at_fault=%lld rows_in_flight_at_fault=%zu%s\n",
            live::product().c_str(),
            cell.c_str(),
            static_cast<long long>(feed_->total()),
            feed_->ids(),
            restarts_spent,
            static_cast<long long>(served_at_fault),
            in_flight_at_fault,
            extra.c_str());
        std::fflush(stdout);
    }

    // Parks worker 0 at `point` by `rule` (block@N: the Nth time it reaches it),
    // kills it there and checks the table converges.
    void kill_at(const std::string& sink,
                 const std::string& point,
                 const std::string& rule,
                 const std::string& cell);

    std::unique_ptr<live::Session> session_;
    std::string table_;
    std::unique_ptr<Feed> feed_;
    std::vector<std::string> archived_logs_;
    std::size_t started_workers_{1};
};

void CassandraRecovery::kill_at(const std::string& sink,
                                const std::string& point,
                                const std::string& rule,
                                const std::string& cell) {
    auto cluster = make_cluster(point + "=" + rule);
    ScopedDiagnostics diagnostics(*cluster);
    submit(*cluster, sink);
    add_spare_worker(*cluster);
    await_first_checkpoint(*cluster);
    feed_->start();
    ASSERT_TRUE(parked_at(*cluster, point)) << "worker 0 never reached " << point;
    const auto served = feed_->served_max();
    const auto in_flight = in_flight_rows();
    const auto stored_at_park = stored().size();
    feed_->watch_rewind();
    kill_and_replace(*cluster, 0);
    ASSERT_TRUE(cluster->await_workers_registered(2));
    await_converged(*cluster, [this] { return feed_->expected(feed_->total()); }, 240s);
    const auto replayed = served - std::min(served, feed_->lowest_since());
    const auto spent = restarts(*cluster);
    cancel_job(*cluster);
    report(cell,
           *cluster,
           spent,
           served,
           in_flight,
           " replayed=" + std::to_string(replayed) +
               " rows_stored_at_fault=" + std::to_string(stored_at_park) +
               " rows_deleted_at_end=" + std::to_string(feed_->deleted(feed_->total())));
    EXPECT_EQ(spent, 1U) << "one worker loss is one restart";
    EXPECT_GT(replayed, 0) << "the restored job read nothing again";
    if (cell == "before_write") {
        // The parked write-out held rows the table did not.
        EXPECT_GT(in_flight, 0U) << "nothing was buffered when the worker parked";
    } else {
        EXPECT_GT(stored_at_park, 0U) << "nothing was written when the worker parked";
    }
}

}  // namespace

TEST_F(CassandraRecovery, AWorkerKilledWithWritesInFlightLosesNothing) {
    make_feed(/*deletes=*/false);
    make_table("id text PRIMARY KEY, seq bigint, v bigint");
    // Reached once per barrier with writes in flight: the third lands mid-feed.
    kill_at("cassandra_sink_string", "cassandra_sink.in_flight", "block@3", "in_flight");
}

TEST_F(CassandraRecovery, AWorkerKilledBeforeAWriteOutLosesNothing) {
    make_feed(/*deletes=*/true);
    make_table("id text PRIMARY KEY, seq bigint, v bigint");
    // Reached at every write-out, early ones included.
    kill_at("cassandra_upsert_sink_string",
            "cassandra_upsert_sink.before_write",
            "block@10",
            "before_write");
}

TEST_F(CassandraRecovery, AWorkerKilledAfterAcknowledgedWritesConvergesOnReplay) {
    make_feed(/*deletes=*/true);
    make_table("id text PRIMARY KEY, seq bigint, v bigint");
    kill_at("cassandra_upsert_sink_string",
            "cassandra_upsert_sink.after_write",
            "block@3",
            "after_write");
}

TEST_F(CassandraRecovery, ASqlJobKilledAfterAcknowledgedWritesConvergesOnReplay) {
    if (!std::filesystem::exists(sql_binary())) {
        GTEST_SKIP() << "the clink_submit_sql binary is not built";
    }
    make_feed(/*deletes=*/false);
    // A GROUP BY's counts per row: every record replaces its row's count with an
    // update_before and update_after pair, a delete and an insert by key.
    live::fresh_table(*session_, table_, "id text PRIMARY KEY, seq bigint, v bigint");
    const std::string sql =
        "CREATE TABLE feed (id STRING, seq BIGINT, v BIGINT) WITH (connector='http_poll', "
        "format='json', url='" +
        feed_->url() +
        "', path='/feed', records_field='items', cursor_param='since', cursor_field='seq', "
        "poll_interval_ms='20');\n"
        "CREATE TABLE counts (id STRING, seq BIGINT, v BIGINT) WITH (connector='cassandra', "
        "contact_points='" +
        live::contact_points() + "', port='" + std::to_string(live::port()) + "', keyspace='" +
        live::kKeyspace + "', table='" + table_ +
        "', mode='upsert', primary_key='id', max_buffered_records='40', max_in_flight='32');\n"
        "INSERT INTO counts SELECT id, COUNT(*) AS seq, MAX(v) AS v FROM feed GROUP BY id;\n";
    const std::string point = "cassandra_upsert_sink.after_write";
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
    const auto want = [this] {
        std::map<std::string, std::int64_t> count;
        std::map<std::string, std::int64_t> last;
        for (std::int64_t n = 1; n <= feed_->total(); ++n) {
            ++count[feed_->id_of(n)];
            last[feed_->id_of(n)] = n;
        }
        std::map<std::string, std::string> out;
        for (const auto& [id, c] : count) {
            out[id] = std::to_string(c) + "/" + std::to_string(last[id] * 10);
        }
        return out;
    };
    await_converged(*cluster, want, 240s);
    const auto replayed = served - std::min(served, feed_->lowest_since());
    const auto spent = restarts(*cluster);
    cancel_job(*cluster);
    report("sql_after_write",
           *cluster,
           spent,
           served,
           0,
           " replayed=" + std::to_string(replayed) +
               " rows_stored_at_fault=" + std::to_string(stored_at_park) + " parallelism=2");
    EXPECT_EQ(spent, 1U) << "one worker loss is one restart";
    EXPECT_GT(replayed, 0) << "the restored job read nothing again";
    EXPECT_GT(stored_at_park, 0U);
}
