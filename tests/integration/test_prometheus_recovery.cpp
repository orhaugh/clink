// The Prometheus Pushgateway sink's recovery cells: a coordinator and two
// clink_node workers run a job that polls a feed this test serves
// (http_poll_source, replayable from its checkpointed cursor) into
// prometheus_sink, while the feed releases records at a steady rate. Record n
// is {"id":<id>,"seq":n}: seq is the cursor and the gauge's value, id its one
// label, so each id is a series whose latest value is the highest seq the feed
// gave it. The id is k<n mod ids>, except that every tenth record of the first
// 150 names one of five early series, which no later record names. Each cell
// parks worker 0 at prometheus_sink.after_write on its 8th push, where the
// gateway has confirmed a push the checkpoint that asked for it does not yet
// cover (a linger push between barriers, or a barrier's own), by when a
// checkpoint past the early records has completed. It SIGKILLs the worker
// there and lets the job restore from that checkpoint, so the restored job
// pushes again what the gateway already shows, and the replay never names the
// early series again:
//
//   1. push_series='all': every series at its latest value, the declared
//      contract: the restored run adopts the group's series, which alone keeps
//      the early ones, and the replay brings each other back to its latest;
//   2. push_series='batch': each push replaces the one before it, so the
//      gateway ends with the series of the last push, each at its latest, and
//      the early series gone;
//   3. cell 1 as a SQL job (connector='http_poll' into connector='prometheus'),
//      submitted by clink_submit_sql at parallelism 2, the planner keeping the
//      sink on one subtask.
//
// Pass criteria, read by scraping the gateway's /metrics with the test's own
// client and never from anything clink reports: in cells 1 and 3, exactly one
// series per id, at its latest value, the early ones included, and no other
// series; in cell 2, every series shown at its latest value, the last record's
// series among them and no early one. The
// gateway keeps values, not samples, so none of this says every record was
// observed. Each cell also checks the one restart the kill costs, that the
// job restored from a checkpoint past the early records, that the push the
// worker parked after carried records past it, and the fault framework's
// witness of where the worker parked.
// Each prints one "[prometheus-recovery]" line and, with
// CLINK_PUSHGATEWAY_RECOVERY_LOG_DIR set, keeps the cluster's logs there.
//
// Needs the gateway scripts/prometheus-live.sh starts
// (CLINK_PUSHGATEWAY_TEST_ENDPOINT) and the clink_node binary of a build with
// fault injection (and clink_submit_sql for cell 3).

#include <algorithm>
#include <atomic>
#include <charconv>
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
#include "prom_live.hpp"
#include "tests/integration/cluster_harness.hpp"

namespace {

using namespace std::chrono_literals;
using clink::itest::Cluster;
using clink::itest::ClusterSpec;
using clink::itest::ProcOptions;
using clink::itest::ScopedDiagnostics;
namespace live = clink::http_connector::prom_live;

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
    static constexpr std::int64_t kEarlyEnd = 150;  // the early series' records end here

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

    [[nodiscard]] std::string id_of(std::int64_t n) const {
        if (n <= kEarlyEnd && n % 10 == 0) {
            return "early" + std::to_string((n / 10) % 5);
        }
        return "k" + std::to_string(n % ids_);
    }
    [[nodiscard]] static bool early(const std::string& id) { return id.starts_with("early"); }
    [[nodiscard]] std::string record(std::int64_t n) const {
        return R"({"id":")" + id_of(n) + R"(","seq":)" + std::to_string(n) + "}";
    }
    // Each id's latest seq once every record is released.
    [[nodiscard]] std::map<std::string, std::int64_t> latest() const {
        std::map<std::string, std::int64_t> out;
        for (std::int64_t n = 1; n <= total_; ++n) {
            out[id_of(n)] = n;
        }
        return out;
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

// A scraped value as a number; -1 when it is not one.
std::int64_t as_seq(const std::string& text) {
    double d = 0;
    const auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), d);
    return ec == std::errc{} && ptr == text.data() + text.size() ? static_cast<std::int64_t>(d)
                                                                 : -1;
}

class PushgatewayRecovery : public ::testing::Test {
protected:
    void SetUp() override {
        if (!live::configured()) {
            GTEST_SKIP() << "set CLINK_PUSHGATEWAY_TEST_ENDPOINT (scripts/prometheus-live.sh does)";
        }
        if (!std::filesystem::exists(recovery_node_binary())) {
            GTEST_SKIP() << "the clink_node binary is not built";
        }
        static int counter = 0;
        const auto tag =
            std::to_string(static_cast<long>(::getpid())) + "_" + std::to_string(++counter);
        metric_ = "clink_rec_" + tag;
        job_ = "clink_rec_job_" + tag;
        feed_ = std::make_unique<Feed>(3000, 100, 300);
    }

    void TearDown() override {
        feed_.reset();
        if (live::configured()) {
            live::delete_group("/metrics/job/" + job_);
        }
    }

    clink::cluster::JobGraphSpec graph(const std::string& push_series) const {
        clink::cluster::JobGraphSpec g;
        clink::cluster::OperatorSpec src;
        src.type = "http_poll_source";
        src.id = "src";
        src.uid = "prometheus-recovery-src";
        src.out_channel = std::string{clink::cluster::kChannelString};
        src.params = {{"url", feed_->url()},
                      {"path", "/feed"},
                      {"cursor_param", "since"},
                      {"cursor_field", "seq"},
                      {"records_field", "items"},
                      {"poll_interval_ms", "20"}};
        g.ops.push_back(src);
        clink::cluster::OperatorSpec snk;
        snk.type = "prometheus_sink";
        snk.id = "snk";
        snk.uid = "prometheus-recovery-snk";
        snk.inputs = {"src"};
        snk.out_channel = std::string{clink::cluster::kChannelString};
        snk.params = {{"url", live::endpoint()},
                      {"job", job_},
                      {"metric_name", metric_},
                      {"value_field", "seq"},
                      {"push_series", push_series},
                      {"linger_ms", "500"}};
        if (push_series == "batch") {
            snk.params["batch_records"] = "100";
        }
        g.ops.push_back(snk);
        return g;
    }

    // Each id the gateway shows, with its value; "id" absent reads as "".
    std::map<std::string, std::int64_t> shown() const {
        std::map<std::string, std::int64_t> out;
        for (const auto& [labels, value] : live::series(metric_, job_)) {
            const auto it = labels.find("id");
            out[it == labels.end() || labels.size() != 1 ? "<other labels>" : it->second] =
                as_seq(value);
        }
        return out;
    }

    std::int64_t highest_shown() const {
        std::int64_t top = 0;
        for (const auto& [id, v] : shown()) {
            top = std::max(top, v);
        }
        return top;
    }

    void keep_logs(const std::string& cell, Cluster& cluster) const {
        const auto into = live::env_or("CLINK_PUSHGATEWAY_RECOVERY_LOG_DIR");
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
        // The cell's verdict rests on this scrape: each series the gateway
        // showed at the end, beside the value the feed says is its latest.
        std::ofstream scrape(dir / "final-scrape.txt");
        const auto latest = feed_->latest();
        for (const auto& [id, v] : shown()) {
            const auto it = latest.find(id);
            scrape << metric_ << "{id=\"" << id << "\"} " << v << " (latest "
                   << (it == latest.end() ? std::string{"none"} : std::to_string(it->second))
                   << ")\n";
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

    struct Outcome {
        std::int64_t served{0};
        std::int64_t replayed{0};
        std::int64_t rewound_to{0};
        std::int64_t highest_at_fault{0};
        std::size_t series_at_fault{0};
        std::size_t restarts{0};
    };
    template <typename Submit, typename Converged>
    Outcome run_cell(const std::string& cell,
                     Submit submit_job,
                     Converged converged,
                     int slots = 3);

    std::unique_ptr<Cluster> make_cluster(const std::string& fault, int slots) {
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

    static void add_spare_worker(Cluster& cluster) {
        ASSERT_TRUE(cluster.start_worker(1, ProcOptions{}));
        ASSERT_TRUE(cluster.await_workers_registered(2));
    }

    static clink::http_connector::HttpRequest coordinator(const Cluster& cluster) {
        clink::http_connector::HttpRequest::Options o;
        o.base_url = "http://127.0.0.1:" + std::to_string(cluster.http_port());
        return clink::http_connector::HttpRequest(std::move(o));
    }

    void submit(Cluster& cluster, const std::string& push_series) {
        auto http = coordinator(cluster);
        const auto r = http.post("/api/v1/jobs/spec?name=prometheus-recovery&checkpoint_dir=" +
                                     cluster.checkpoint_dir().string() +
                                     "&checkpoint_interval_ms=1000&alignment=aligned"
                                     "&max_restarts_on_worker_loss=8",
                                 graph(push_series).to_json(),
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
                                  "prometheus-recovery-sql",
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

    // The ids the gateway should show under push_series='all' once every record
    // is in: each at its latest seq, and nothing else.
    bool all_at_latest() const { return shown() == feed_->latest(); }

    std::string metric_;
    std::string job_;
    std::unique_ptr<Feed> feed_;
    std::vector<std::string> archived_logs_;
};

template <typename Submit, typename Converged>
PushgatewayRecovery::Outcome PushgatewayRecovery::run_cell(const std::string& cell,
                                                           Submit submit_job,
                                                           Converged converged,
                                                           int slots) {
    Outcome out;
    const std::string point = "prometheus_sink.after_write";
    auto cluster = make_cluster(point + "=block@8", slots);
    ScopedDiagnostics diagnostics(*cluster);
    submit_job(*cluster);
    add_spare_worker(*cluster);
    await_first_checkpoint(*cluster);
    feed_->start();
    EXPECT_TRUE(parked_at(*cluster, point)) << "worker 0 never reached " << point;
    out.served = feed_->served_max();
    out.series_at_fault = shown().size();
    out.highest_at_fault = highest_shown();
    feed_->watch_rewind();
    kill_and_replace(*cluster, 0);
    EXPECT_TRUE(cluster->await_workers_registered(2));
    const bool done = clink::itest::await(
        [&] { return feed_->released() == feed_->total() && converged(); }, 240s);
    EXPECT_TRUE(done) << "the gateway did not converge";
    const auto after = latest_completed(*cluster);
    EXPECT_TRUE(clink::itest::await([&] { return latest_completed(*cluster) >= after + 2; }, 60s))
        << "checkpoints stopped completing after the gateway converged";
    out.rewound_to = feed_->lowest_since();
    out.replayed = out.served - std::min(out.served, out.rewound_to);
    out.restarts = restarts(*cluster);
    cancel_job(*cluster);
    keep_logs(cell, *cluster);
    return out;
}

}  // namespace

TEST_F(PushgatewayRecovery, AWorkerKilledAfterAnUncoveredPushLeavesEverySeriesAtItsLatestValue) {
    const auto o = run_cell(
        "all", [this](Cluster& c) { submit(c, "all"); }, [this] { return all_at_latest(); });
    const auto got = shown();
    const auto want = feed_->latest();
    std::size_t wrong = 0;
    for (const auto& [id, v] : want) {
        const auto it = got.find(id);
        if ((it == got.end() || it->second != v) && ++wrong <= 5) {
            ADD_FAILURE() << "series id=" << id << " is "
                          << (it == got.end() ? std::string{"missing"} : std::to_string(it->second))
                          << ", not " << v;
        }
    }
    EXPECT_EQ(got.size(), want.size()) << "series beyond one per id";
    for (const auto& id : {"early0", "early1", "early2", "early3", "early4"}) {
        EXPECT_TRUE(got.contains(id)) << id << " is gone: the restored run did not adopt it";
    }
    std::printf(
        "[prometheus-recovery] cell=all records=%lld series=%zu restarts=%zu served_at_fault=%lld "
        "series_at_fault=%zu highest_at_fault=%lld rewound_to=%lld replayed=%lld\n",
        static_cast<long long>(feed_->total()),
        got.size(),
        o.restarts,
        static_cast<long long>(o.served),
        o.series_at_fault,
        static_cast<long long>(o.highest_at_fault),
        static_cast<long long>(o.rewound_to),
        static_cast<long long>(o.replayed));
    EXPECT_EQ(o.restarts, 1U) << "one worker loss is one restart";
    EXPECT_GT(o.replayed, 0) << "the restored job read nothing again";
    EXPECT_GT(o.highest_at_fault, o.rewound_to)
        << "the push the worker parked after carried records past the restored checkpoint";
    EXPECT_GE(o.rewound_to, Feed::kEarlyEnd)
        << "the job restored from before the early records' end, so the replay named them";
}

TEST_F(PushgatewayRecovery, InBatchModeTheGatewayEndsWithTheLastPushsSeriesAtTheirLatestValues) {
    const auto last_id = feed_->id_of(feed_->total());
    const auto o = run_cell(
        "batch",
        [this](Cluster& c) { submit(c, "batch"); },
        [&] {
            const auto s = shown();
            const auto it = s.find(last_id);
            return it != s.end() && it->second == feed_->total();
        });
    const auto got = shown();
    const auto latest = feed_->latest();
    for (const auto& [id, v] : got) {
        const auto it = latest.find(id);
        EXPECT_TRUE(it != latest.end() && it->second == v)
            << "series id=" << id << " is " << v << ", not its latest";
    }
    EXPECT_TRUE(got.contains(last_id));
    for (const auto& [id, v] : got) {
        EXPECT_FALSE(Feed::early(id)) << id << " outlived the pushes that did not carry it";
    }
    std::printf(
        "[prometheus-recovery] cell=batch records=%lld series=%zu of %zu restarts=%zu "
        "served_at_fault=%lld series_at_fault=%zu highest_at_fault=%lld rewound_to=%lld "
        "replayed=%lld\n",
        static_cast<long long>(feed_->total()),
        got.size(),
        latest.size(),
        o.restarts,
        static_cast<long long>(o.served),
        o.series_at_fault,
        static_cast<long long>(o.highest_at_fault),
        static_cast<long long>(o.rewound_to),
        static_cast<long long>(o.replayed));
    EXPECT_EQ(o.restarts, 1U) << "one worker loss is one restart";
    EXPECT_GT(o.replayed, 0) << "the restored job read nothing again";
    EXPECT_GT(o.highest_at_fault, o.rewound_to)
        << "the push the worker parked after carried records past the restored checkpoint";
    EXPECT_GE(o.rewound_to, Feed::kEarlyEnd)
        << "the job restored from before the early records' end, so the replay named them";
}

TEST_F(PushgatewayRecovery, ASqlJobKilledAfterAnUncoveredPushLeavesEverySeriesAtItsLatestValue) {
    if (!std::filesystem::exists(sql_binary())) {
        GTEST_SKIP() << "the clink_submit_sql binary is not built";
    }
    const std::string sql =
        "CREATE TABLE feed (id STRING, seq BIGINT) WITH (connector='http_poll', format='json', "
        "url='" +
        feed_->url() +
        "', path='/feed', records_field='items', cursor_param='since', cursor_field='seq', "
        "poll_interval_ms='20');\n"
        "CREATE TABLE gauges (id STRING, seq BIGINT) WITH (connector='prometheus', url='" +
        live::endpoint() + "', job='" + job_ + "', metric_name='" + metric_ +
        "', value_field='seq', push_series='all', linger_ms='500');\n"
        "INSERT INTO gauges SELECT id, seq FROM feed;\n";
    const auto o = run_cell(
        "sql",
        [&](Cluster& c) { submit_sql(c, sql, /*parallelism=*/2); },
        [this] { return all_at_latest(); },
        /*slots=*/8);
    const auto got = shown();
    EXPECT_EQ(got, feed_->latest());
    std::printf(
        "[prometheus-recovery] cell=sql records=%lld series=%zu restarts=%zu served_at_fault=%lld "
        "series_at_fault=%zu highest_at_fault=%lld rewound_to=%lld replayed=%lld parallelism=2\n",
        static_cast<long long>(feed_->total()),
        got.size(),
        o.restarts,
        static_cast<long long>(o.served),
        o.series_at_fault,
        static_cast<long long>(o.highest_at_fault),
        static_cast<long long>(o.rewound_to),
        static_cast<long long>(o.replayed));
    EXPECT_EQ(o.restarts, 1U) << "one worker loss is one restart";
    EXPECT_GT(o.replayed, 0) << "the restored job read nothing again";
    EXPECT_GT(o.highest_at_fault, o.rewound_to)
        << "the push the worker parked after carried records past the restored checkpoint";
    EXPECT_GE(o.rewound_to, Feed::kEarlyEnd)
        << "the job restored from before the early records' end, so the replay named them";
}
