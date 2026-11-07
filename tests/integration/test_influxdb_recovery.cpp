// The InfluxDB sink's recovery cells: a coordinator and two clink_node workers
// run a job that polls a feed this test serves (http_poll_source, replayable
// from its checkpointed cursor) into influxdb_sink, while the feed releases
// records at a steady rate. Record n is {"id":"k<n mod ids>","seq":n,"v":10n}.
// Each cell parks worker 0 at influxdb_sink.after_write, where InfluxDB has
// confirmed a flush the checkpoint that asked for it does not yet cover,
// SIGKILLs it there and lets the job restore, so the restored job writes points
// the server already holds:
//
//   1. timestamped (tags='id', timestamp_field='seq'): every point is its
//      record's series and time, so the replay lands on points that exist;
//   2. server time (no timestamp_field): InfluxDB stamps each write, so the
//      replay adds points beside those the first writes left;
//   3. cell 1 as a SQL job (connector='http_poll' into connector='influxdb'),
//      submitted by clink_submit_sql at parallelism 2.
//
// Pass criteria, read with Flux through the test's own client and never from
// anything clink reports: in cells 1 and 3, exactly one point per record, at its
// series and time, with its value, and no other point; in cell 2, every record
// present at least once and the replay's duplicates present, as the server-time
// contract says. Each cell also checks the one restart the kill costs, a replay
// from the checkpoint's cursor, and the fault framework's witness of where the
// worker parked. Each prints one "[influxdb-recovery]" line and, with
// CLINK_INFLUXDB_RECOVERY_LOG_DIR set, keeps the cluster's logs there.
//
// Needs the server scripts/influxdb-live.sh starts (CLINK_INFLUXDB_TEST_ENDPOINT
// and the org, bucket and token variables) and the clink_node binary of a build
// with fault injection (and clink_submit_sql for cell 3).

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
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
#include "influx_live.hpp"
#include "tests/integration/cluster_harness.hpp"

namespace {

using namespace std::chrono_literals;
using clink::itest::Cluster;
using clink::itest::ClusterSpec;
using clink::itest::ProcOptions;
using clink::itest::ScopedDiagnostics;
namespace live = clink::http_connector::influx_live;

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

// RFC 3339 as Flux prints a time `ns` nanoseconds after the epoch, for ns < 1 s.
std::string flux_time(std::int64_t ns) {
    std::string frac = std::to_string(ns);
    frac.insert(0, 9 - frac.size(), '0');
    while (!frac.empty() && frac.back() == '0') {
        frac.pop_back();
    }
    return "1970-01-01T00:00:00" + (frac.empty() ? std::string{} : "." + frac) + "Z";
}

// The job's input. GET /feed?since=<seq> answers {"items":[...]}: up to 200 of
// the records released so far whose seq is after <seq>, ascending, the exclusive
// cursor http_poll_source replays from.
class Feed {
public:
    Feed(std::int64_t total, int ids, int per_second)
        : total_(total), ids_(ids), per_second_(per_second) {
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
    [[nodiscard]] std::string record(std::int64_t n) const {
        return R"({"id":")" + id_of(n) + R"(","seq":)" + std::to_string(n) + R"(,"v":)" +
               std::to_string(n * 10) + "}";
    }

private:
    std::int64_t total_;
    int ids_;
    int per_second_;
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

class InfluxDbRecovery : public ::testing::Test {
protected:
    void SetUp() override {
        if (!live::configured()) {
            GTEST_SKIP() << "set CLINK_INFLUXDB_TEST_ENDPOINT (scripts/influxdb-live.sh does)";
        }
        if (!std::filesystem::exists(recovery_node_binary())) {
            GTEST_SKIP() << "the clink_node binary is not built";
        }
        static int counter = 0;
        measurement_ = "clink_rec_" + std::to_string(static_cast<long>(::getpid())) + "_" +
                       std::to_string(++counter);
        feed_ = std::make_unique<Feed>(3000, 100, 300);
    }

    void TearDown() override { feed_.reset(); }

    clink::cluster::JobGraphSpec graph(bool timestamped) const {
        clink::cluster::JobGraphSpec g;
        clink::cluster::OperatorSpec src;
        src.type = "http_poll_source";
        src.id = "src";
        src.uid = "influxdb-recovery-src";
        src.out_channel = std::string{clink::cluster::kChannelString};
        src.params = {{"url", feed_->url()},
                      {"path", "/feed"},
                      {"cursor_param", "since"},
                      {"cursor_field", "seq"},
                      {"records_field", "items"},
                      {"poll_interval_ms", "20"}};
        g.ops.push_back(src);
        clink::cluster::OperatorSpec snk;
        snk.type = "influxdb_sink";
        snk.id = "snk";
        snk.uid = "influxdb-recovery-snk";
        snk.inputs = {"src"};
        snk.out_channel = std::string{clink::cluster::kChannelString};
        snk.params = {{"url", live::endpoint()},
                      {"org", live::org()},
                      {"bucket", live::bucket()},
                      {"token", "env://CLINK_INFLUXDB_TEST_TOKEN"},
                      {"measurement", measurement_},
                      {"tags", "id"},
                      {"batch_records", "100"}};
        if (timestamped) {
            snk.params["timestamp_field"] = "seq";
        }
        g.ops.push_back(snk);
        return g;
    }

    // Each point's (id, seq, v) as Flux returns it, keyed by series and time.
    std::map<std::string, live::Point> stored() const { return live::points(measurement_); }

    // Timestamped: exactly one point per record released so far, at its series
    // and time, with its value.
    std::map<std::string, std::string> expected_timestamped(std::int64_t upto) const {
        std::map<std::string, std::string> out;
        for (std::int64_t n = 1; n <= upto; ++n) {
            out["id=" + feed_->id_of(n) + "@" + flux_time(n)] = std::to_string(n * 10);
        }
        return out;
    }
    std::map<std::string, std::string> actual_timestamped() const {
        std::map<std::string, std::string> out;
        for (const auto& [key, p] : stored()) {
            const auto it = p.fields.find("v");
            out[key] = it == p.fields.end() ? "<no v>" : it->second.value;
            if (p.fields.size() != 1) {
                out[key] += " (and " + std::to_string(p.fields.size() - 1) + " other field(s))";
            }
        }
        return out;
    }

    // Server time: how many points carry each seq.
    std::map<std::int64_t, std::size_t> seq_counts() const {
        std::map<std::int64_t, std::size_t> out;
        for (const auto& [key, p] : stored()) {
            if (const auto it = p.fields.find("seq"); it != p.fields.end()) {
                ++out[static_cast<std::int64_t>(std::stod(it->second.value))];
            }
        }
        return out;
    }

    void keep_logs(const std::string& cell, Cluster& cluster) const {
        const auto into = live::env_or("CLINK_INFLUXDB_RECOVERY_LOG_DIR");
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

    // Parks worker 0 at influxdb_sink.after_write, kills it, and waits until
    // every record is released and `converged` holds, then two more
    // checkpoints.
    struct Outcome {
        std::int64_t served{0};
        std::int64_t replayed{0};
        std::size_t restarts{0};
        std::size_t points_at_fault{0};
    };
    template <typename Submit, typename Converged>
    Outcome run_cell(Submit submit_job, Converged converged, int slots = 3);

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

    void submit(Cluster& cluster, bool timestamped) {
        auto http = coordinator(cluster);
        const auto r = http.post("/api/v1/jobs/spec?name=influxdb-recovery&checkpoint_dir=" +
                                     cluster.checkpoint_dir().string() +
                                     "&checkpoint_interval_ms=1000&alignment=aligned"
                                     "&max_restarts_on_worker_loss=8",
                                 graph(timestamped).to_json(),
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
                                  "influxdb-recovery-sql",
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

    std::string measurement_;
    std::unique_ptr<Feed> feed_;
    std::vector<std::string> archived_logs_;
    std::size_t started_workers_{1};
};

template <typename Submit, typename Converged>
InfluxDbRecovery::Outcome InfluxDbRecovery::run_cell(Submit submit_job,
                                                     Converged converged,
                                                     int slots) {
    Outcome out;
    const std::string point = "influxdb_sink.after_write";
    auto cluster = make_cluster(point + "=block@3", slots);
    ScopedDiagnostics diagnostics(*cluster);
    submit_job(*cluster);
    add_spare_worker(*cluster);
    await_first_checkpoint(*cluster);
    feed_->start();
    EXPECT_TRUE(parked_at(*cluster, point)) << "worker 0 never reached " << point;
    out.served = feed_->served_max();
    out.points_at_fault = stored().size();
    feed_->watch_rewind();
    kill_and_replace(*cluster, 0);
    EXPECT_TRUE(cluster->await_workers_registered(2));
    const bool done = clink::itest::await(
        [&] { return feed_->released() == feed_->total() && converged(); }, 240s);
    EXPECT_TRUE(done) << "the measurement did not converge";
    const auto after = latest_completed(*cluster);
    EXPECT_TRUE(clink::itest::await([&] { return latest_completed(*cluster) >= after + 2; }, 60s))
        << "checkpoints stopped completing after the measurement converged";
    out.replayed = out.served - std::min(out.served, feed_->lowest_since());
    out.restarts = restarts(*cluster);
    cancel_job(*cluster);
    keep_logs(measurement_, *cluster);
    return out;
}

}  // namespace

TEST_F(InfluxDbRecovery, AWorkerKilledAfterTimestampedWritesLeavesOnePointPerRecord) {
    const auto o =
        run_cell([this](Cluster& c) { submit(c, /*timestamped=*/true); },
                 [this] { return actual_timestamped() == expected_timestamped(feed_->total()); });
    const auto got = actual_timestamped();
    const auto want = expected_timestamped(feed_->total());
    std::size_t wrong = 0;
    for (const auto& [key, v] : want) {
        const auto it = got.find(key);
        if ((it == got.end() || it->second != v) && ++wrong <= 5) {
            ADD_FAILURE() << "point " << key << " is "
                          << (it == got.end() ? std::string{"missing"} : it->second)
                          << ", not v=" << v;
        }
    }
    EXPECT_EQ(got.size(), want.size()) << "points beyond one per record";
    std::printf(
        "[influxdb-recovery] cell=timestamped records=%lld points=%zu restarts=%zu "
        "served_at_fault=%lld points_at_fault=%zu replayed=%lld\n",
        static_cast<long long>(feed_->total()),
        got.size(),
        o.restarts,
        static_cast<long long>(o.served),
        o.points_at_fault,
        static_cast<long long>(o.replayed));
    EXPECT_EQ(o.restarts, 1U) << "one worker loss is one restart";
    EXPECT_GT(o.replayed, 0) << "the restored job read nothing again";
    EXPECT_GT(o.points_at_fault, 0U) << "nothing was written when the worker parked";
}

TEST_F(InfluxDbRecovery, AWorkerKilledInServerTimeModeLeavesTheReplaysDuplicates) {
    const auto o = run_cell([this](Cluster& c) { submit(c, /*timestamped=*/false); },
                            [this] {
                                const auto counts = seq_counts();
                                return counts.size() == static_cast<std::size_t>(feed_->total());
                            });
    const auto counts = seq_counts();
    std::size_t points = 0;
    std::size_t duplicated = 0;
    for (const auto& [seq, n] : counts) {
        points += n;
        duplicated += n > 1 ? 1 : 0;
    }
    std::printf(
        "[influxdb-recovery] cell=server_time records=%lld points=%zu "
        "records_written_twice=%zu restarts=%zu served_at_fault=%lld "
        "points_at_fault=%zu replayed=%lld\n",
        static_cast<long long>(feed_->total()),
        points,
        duplicated,
        o.restarts,
        static_cast<long long>(o.served),
        o.points_at_fault,
        static_cast<long long>(o.replayed));
    EXPECT_EQ(counts.size(), static_cast<std::size_t>(feed_->total()))
        << "every record is present at least once";
    EXPECT_GT(duplicated, 0U) << "the replay rewrote applied records with new times";
    EXPECT_LE(duplicated, static_cast<std::size_t>(o.replayed))
        << "only replayed records can be written twice";
    EXPECT_EQ(o.restarts, 1U) << "one worker loss is one restart";
    EXPECT_GT(o.replayed, 0) << "the restored job read nothing again";
}

TEST_F(InfluxDbRecovery, ASqlJobKilledAfterTimestampedWritesLeavesOnePointPerRecord) {
    if (!std::filesystem::exists(sql_binary())) {
        GTEST_SKIP() << "the clink_submit_sql binary is not built";
    }
    const std::string sql =
        "CREATE TABLE feed (id STRING, seq BIGINT, v BIGINT) WITH (connector='http_poll', "
        "format='json', url='" +
        feed_->url() +
        "', path='/feed', records_field='items', cursor_param='since', cursor_field='seq', "
        "poll_interval_ms='20');\n"
        "CREATE TABLE points (id STRING, seq BIGINT, v BIGINT) WITH (connector='influxdb', url='" +
        live::endpoint() + "', org='" + live::org() + "', bucket='" + live::bucket() +
        "', token='env://CLINK_INFLUXDB_TEST_TOKEN', measurement='" + measurement_ +
        "', tags='id', timestamp_field='seq', batch_records='100');\n"
        "INSERT INTO points SELECT id, seq, v FROM feed;\n";
    const auto o =
        run_cell([&](Cluster& c) { submit_sql(c, sql, /*parallelism=*/2); },
                 [this] { return actual_timestamped() == expected_timestamped(feed_->total()); },
                 /*slots=*/8);
    const auto got = actual_timestamped();
    EXPECT_EQ(got, expected_timestamped(feed_->total()));
    std::printf(
        "[influxdb-recovery] cell=sql_timestamped records=%lld points=%zu restarts=%zu "
        "served_at_fault=%lld points_at_fault=%zu replayed=%lld parallelism=2\n",
        static_cast<long long>(feed_->total()),
        got.size(),
        o.restarts,
        static_cast<long long>(o.served),
        o.points_at_fault,
        static_cast<long long>(o.replayed));
    EXPECT_EQ(o.restarts, 1U) << "one worker loss is one restart";
    EXPECT_GT(o.replayed, 0) << "the restored job read nothing again";
}
