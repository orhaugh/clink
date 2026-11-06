// The Elasticsearch and OpenSearch sinks' recovery cells: a coordinator and two
// clink_node workers run a job that polls a feed this test serves
// (http_poll_source, replayable from its checkpointed cursor) into the
// product's bulk sink with document_id, while the feed releases records at a
// steady rate. Every id is written many times with a rising value, so each cell
// checks per-document ordering as well as delivery. One instantiation per
// product, each skipped unless its server is named (CLINK_ELASTICSEARCH_TEST_
// ENDPOINT, CLINK_OPENSEARCH_TEST_ENDPOINT; scripts/search-live.sh sets one).
//
//   1. a worker parked at <sink>.before_write (an attempt's body built, nothing
//      of it sent) is SIGKILLed;
//   2. a worker parked at <sink>.after_write (the server confirmed every record
//      of a flush, the barrier that asked for it not yet acknowledged) is
//      SIGKILLed, so the restored job writes documents the index already holds;
//   3. the sink writes through BulkFaultProxy, which for a window answers every
//      bulk request with a 200 whose body confirms nothing ("errors":false and
//      no items) and forwards nothing: no checkpoint completes in the window,
//      the job restarts from its last one, and nothing is lost. A sink that took
//      that body as success would have cleared its batch and acknowledged the
//      checkpoint, losing the records;
//   4. the sink writes through TcpFaultProxy, which forwards one bulk request
//      whole and withholds the server's reply; the worker running the sink is
//      SIGKILLed while the reply is held, so the server has applied writes whose
//      outcome the job never learned.
//
// Pass criteria, read through the case's own client rather than anything clink
// reports: the index holds exactly one document per id, equal to that id's last
// record; the restarts each cell names; and the fault landing while records were
// in flight (the feed had served records whose writes the index did not hold).
// Cells 3 and 4 use a proxy to make the fault; what the index holds is read from
// the server itself. Each cell prints one "[search-recovery]" line.
//
// Needs the server scripts/search-live.sh starts and the clink_node binary of a
// build with fault injection.

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
#include <mutex>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/config/json.hpp"
#include "clink/http_connector/http_request.hpp"

#include "../checkpoint_markers.hpp"
#include "tests/integration/bulk_fault_proxy.hpp"
#include "tests/integration/cluster_harness.hpp"
#include "tests/integration/tcp_fault_proxy.hpp"

namespace {

using namespace std::chrono_literals;
using clink::itest::BulkFaultProxy;
using clink::itest::Cluster;
using clink::itest::ClusterSpec;
using clink::itest::ProcOptions;
using clink::itest::ScopedDiagnostics;
using clink::itest::TcpFaultProxy;

std::filesystem::path recovery_node_binary() {
#ifdef CLINK_NODE_BINARY
    return std::filesystem::path{CLINK_NODE_BINARY};
#else
    return {};
#endif
}

std::string env_or_empty(const char* name) {
    const char* v = std::getenv(name);
    return v == nullptr ? std::string{} : std::string{v};
}

struct Product {
    const char* name;  // "elasticsearch" or "opensearch"
    const char* env;   // the variable naming its server
    const char* sink;  // the sink's factory name, which its fault points carry
};

const Product kElasticsearch{
    "elasticsearch", "CLINK_ELASTICSEARCH_TEST_ENDPOINT", "elasticsearch_sink"};
const Product kOpenSearch{"opensearch", "CLINK_OPENSEARCH_TEST_ENDPOINT", "opensearch_sink"};

// The job's input. GET /feed?since=<seq> answers {"items":[...]}: up to 200 of
// the records released so far whose seq is after <seq>, ascending, which is the
// exclusive cursor http_poll_source replays from. Record n is
// {"id":"k<n mod ids>","seq":n,"v":n}, so each id is written total/ids times
// with a rising v. start() releases them at a steady rate.
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

    // From now on, note the lowest cursor a poll asks from: after a restore,
    // where the job's checkpoint put its source.
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
               std::to_string(n) + "}";
    }

    // Each id's last record of the first `upto` records, as the index should
    // hold it once they are all written.
    [[nodiscard]] std::map<std::string, std::string> latest(std::int64_t upto) const {
        std::map<std::string, std::string> out;
        for (std::int64_t n = 1; n <= upto; ++n) {
            out[id_of(n)] = record(n);
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

class SearchRecovery : public ::testing::TestWithParam<Product> {
protected:
    void SetUp() override {
        endpoint_ = env_or_empty(GetParam().env);
        if (endpoint_.empty()) {
            GTEST_SKIP() << "set " << GetParam().env << " (scripts/search-live.sh does)";
        }
        if (!std::filesystem::exists(recovery_node_binary())) {
            GTEST_SKIP() << "the clink_node binary is not built";
        }
        auto c = client();
        const auto r = c.get("/");
        ASSERT_EQ(r.status, 200) << "the server at " << endpoint_ << " did not answer";
        const auto j = clink::config::parse(r.body);
        const auto& version = j.as_object().at("version").as_object();
        const auto dist = version.find("distribution");
        const bool opensearch = dist != version.end() && dist->second.is_string() &&
                                dist->second.as_string() == "opensearch";
        ASSERT_EQ(opensearch, std::string{GetParam().name} == "opensearch")
            << GetParam().env << " names a server that is not " << GetParam().name;
        version_ = version.at("number").as_string();
        static int counter = 0;
        index_ = std::string{"clink-rec-"} + GetParam().name + "-" +
                 std::to_string(static_cast<long>(::getpid())) + "-" + std::to_string(++counter);
        feed_ = std::make_unique<Feed>(3000, 200, 300);
    }

    void TearDown() override {
        feed_.reset();
        if (!endpoint_.empty()) {
            auto c = client();
            c.del("/" + index_);
        }
    }

    clink::http_connector::HttpRequest client() const {
        clink::http_connector::HttpRequest::Options o;
        o.base_url = endpoint_;
        return clink::http_connector::HttpRequest{o};
    }

    std::uint16_t endpoint_port() const {
        return static_cast<std::uint16_t>(std::stoi(endpoint_.substr(endpoint_.rfind(':') + 1)));
    }

    // What the index holds: each document's _id and its source, as a value.
    std::map<std::string, std::string> stored() const {
        auto c = client();
        c.post("/" + index_ + "/_refresh", "", "application/json");
        const auto r = c.post("/" + index_ + "/_search",
                              R"({"query":{"match_all":{}},"size":10000})",
                              "application/json");
        std::map<std::string, std::string> out;
        if (r.status != 200) {
            return out;
        }
        const auto j = clink::config::parse(r.body);
        for (const auto& hit : j.as_object().at("hits").as_object().at("hits").as_array()) {
            const auto& h = hit.as_object();
            out[h.at("_id").as_string()] = h.at("_source").serialize(0);
        }
        return out;
    }

    // Ids whose newest record the feed has served is not what the index holds:
    // records in flight.
    std::size_t in_flight_ids() const {
        const auto want = feed_->latest(feed_->served_max());
        const auto got = stored();
        std::size_t n = 0;
        for (const auto& [id, rec] : want) {
            auto it = got.find(id);
            if (it == got.end() || it->second != clink::config::parse(rec).serialize(0)) {
                ++n;
            }
        }
        return n;
    }

    clink::cluster::JobGraphSpec graph(const std::string& sink_url) const {
        clink::cluster::JobGraphSpec g;
        clink::cluster::OperatorSpec src;
        src.type = "http_poll_source";
        src.id = "src";
        src.uid = "search-recovery-src";
        src.out_channel = std::string{clink::cluster::kChannelString};
        src.params = {{"url", feed_->url()},
                      {"path", "/feed"},
                      {"cursor_param", "since"},
                      {"cursor_field", "seq"},
                      {"records_field", "items"},
                      {"poll_interval_ms", "20"}};
        g.ops.push_back(src);
        clink::cluster::OperatorSpec snk;
        snk.type = GetParam().sink;
        snk.id = "snk";
        snk.uid = "search-recovery-snk";
        snk.inputs = {"src"};
        snk.out_channel = std::string{clink::cluster::kChannelString};
        snk.params = {{"url", sink_url},
                      {"index", index_},
                      {"document_id", "id"},
                      {"batch_records", "100"},
                      {"linger_ms", "200"},
                      {"max_retries", "3"}};
        g.ops.push_back(snk);
        return g;
    }

    // A coordinator and worker 0 only, armed with `fault`: the job submitted
    // next places every task on worker 0. Worker 1 joins after the deploy,
    // unarmed, as the spare a restart can use; so is worker 0's replacement.
    std::unique_ptr<Cluster> make_cluster(const std::string& fault) {
        ClusterSpec spec;
        spec.node_binary = recovery_node_binary();
        spec.workers = 2;
        spec.slots_per_worker = 3;
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

    void submit(Cluster& cluster, const std::string& sink_url, int max_restarts = 8) {
        auto http = coordinator(cluster);
        const auto r = http.post("/api/v1/jobs/spec?name=search-recovery&checkpoint_dir=" +
                                     cluster.checkpoint_dir().string() +
                                     "&checkpoint_interval_ms=1000&alignment=aligned"
                                     "&max_restarts_on_worker_loss=" +
                                     std::to_string(max_restarts),
                                 graph(sink_url).to_json(),
                                 "application/json");
        ASSERT_NE(r.body.find("\"ok\":true"), std::string::npos)
            << "the job was not accepted: " << r.body;
    }

    static void cancel_job(const Cluster& cluster) {
        auto http = coordinator(cluster);
        http.post("/api/v1/jobs/1/cancel", "", "application/json");
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

    // Every record released, the index holding each id's last record, and two
    // more checkpoints completed after that.
    void await_converged(Cluster& cluster, std::chrono::seconds timeout) {
        const auto want = feed_->latest(feed_->total());
        std::map<std::string, std::string> canon_want;
        for (const auto& [id, rec] : want) {
            canon_want[id] = clink::config::parse(rec).serialize(0);
        }
        const bool converged = clink::itest::await(
            [&] { return feed_->released() == feed_->total() && stored() == canon_want; }, timeout);
        if (!converged) {
            const auto got = stored();
            std::size_t wrong = 0;
            for (const auto& [id, rec] : canon_want) {
                auto it = got.find(id);
                if ((it == got.end() || it->second != rec) && ++wrong <= 5) {
                    ADD_FAILURE() << "document " << id << " is "
                                  << (it == got.end() ? std::string{"missing"} : it->second)
                                  << ", not " << rec;
                }
            }
            FAIL() << "the index did not converge: " << got.size() << " documents, " << wrong
                   << " not at their id's last record";
        }
        EXPECT_EQ(stored().size(), static_cast<std::size_t>(feed_->ids()));
        const auto after = latest_completed(cluster);
        ASSERT_TRUE(
            clink::itest::await([&] { return latest_completed(cluster) >= after + 2; }, 60s))
            << "checkpoints stopped completing after the index converged";
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

    // The cluster's logs, kept where CLINK_SEARCH_RECOVERY_LOG_DIR names (the
    // gate's report directory), whether the cell passed or not.
    void keep_logs(const std::string& cell, Cluster& cluster) const {
        const auto into = env_or_empty("CLINK_SEARCH_RECOVERY_LOG_DIR");
        if (into.empty()) {
            return;
        }
        std::error_code ec;
        const auto dir = std::filesystem::path(into) / (std::string{GetParam().name} + "-" + cell);
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

    // `in_flight_at_fault`: ids whose newest served record the index did not
    // hold when the fault landed. `extra` adds what a cell measures besides.
    void report(const std::string& cell,
                Cluster& cluster,
                std::size_t restarts_spent,
                std::int64_t served_at_fault,
                std::size_t in_flight_at_fault,
                const std::string& extra = {}) {
        keep_logs(cell, cluster);
        std::printf(
            "[search-recovery] product=%s version=%s cell=%s records=%lld ids=%d restarts=%zu "
            "served_at_fault=%lld in_flight_ids_at_fault=%zu%s\n",
            GetParam().name,
            version_.c_str(),
            cell.c_str(),
            static_cast<long long>(feed_->total()),
            feed_->ids(),
            restarts_spent,
            static_cast<long long>(served_at_fault),
            in_flight_at_fault,
            extra.c_str());
        std::fflush(stdout);
    }

    void kill_at(const std::string& stage, const std::string& rule);

    std::string endpoint_;
    std::string version_;
    std::string index_;
    std::unique_ptr<Feed> feed_;
    std::vector<std::string> archived_logs_;
    std::size_t started_workers_{1};
    std::size_t stored_at_park_{0};
};

void SearchRecovery::kill_at(const std::string& stage, const std::string& rule) {
    // Worker 0 runs the job; the rule's number of times it reaches the point, it
    // parks there, and the kill lands while it is parked.
    const std::string point = std::string{GetParam().sink} + "." + stage;
    auto cluster = make_cluster(point + "=" + rule);
    ScopedDiagnostics diagnostics(*cluster);
    submit(*cluster, endpoint_);
    add_spare_worker(*cluster);
    await_first_checkpoint(*cluster);
    feed_->start();
    ASSERT_TRUE(parked_at(*cluster, point)) << "worker 0 never reached " << point;
    const auto served = feed_->served_max();
    const auto in_flight = in_flight_ids();
    stored_at_park_ = stored().size();
    feed_->watch_rewind();
    kill_and_replace(*cluster, 0);
    ASSERT_TRUE(cluster->await_workers_registered(2));
    await_converged(*cluster, 240s);
    // The records the restored job read again: everything its checkpoint did
    // not cover, written or not.
    const auto replayed = served - std::min(served, feed_->lowest_since());
    const auto spent = restarts(*cluster);
    cancel_job(*cluster);
    report(stage, *cluster, spent, served, in_flight, " replayed=" + std::to_string(replayed));
    EXPECT_EQ(spent, 1U) << "one worker loss is one restart";
    EXPECT_GT(replayed, 0) << "the restored job read nothing again";
    if (stage == "before_write") {
        // The parked attempt held records the index did not.
        EXPECT_GT(in_flight, 0U) << "nothing was in flight when the worker parked";
    } else {
        // The source shares the sink's task, so it stops with it: the index
        // held what had been served, written by the flush the server
        // confirmed, and the replay rewrote it.
        EXPECT_GT(stored_at_park_, 0U);
    }
}

}  // namespace

TEST_P(SearchRecovery, AWorkerKilledBeforeAWriteLosesNothing) {
    kill_at("before_write", "block@10");
}

TEST_P(SearchRecovery, AWorkerKilledAfterTheServerConfirmedAFlushConvergesOnReplay) {
    kill_at("after_write", "block@10");
}

TEST_P(SearchRecovery, NoCheckpointCompletesWhileTheServerConfirmsNothing) {
    BulkFaultProxy proxy(endpoint_);
    auto cluster = make_cluster("");
    ScopedDiagnostics diagnostics(*cluster);
    submit(*cluster, proxy.url(), /*max_restarts=*/30);
    add_spare_worker(*cluster);
    await_first_checkpoint(*cluster);
    const auto idle = latest_completed(*cluster);
    feed_->start();
    // A checkpoint covering records has completed, so the restart resumes
    // mid-stream.
    ASSERT_TRUE(clink::itest::await(
        [&] { return latest_completed(*cluster) > idle && stored().size() >= 20; }, 60s))
        << "nothing was written and checkpointed before the fault";
    // From here every bulk request is answered 200 with a body that confirms
    // nothing, and none reaches the server.
    proxy.refuse(1000000, 200, R"({"took":0,"errors":false,"items":[]})");
    const auto before = latest_completed(*cluster);
    std::this_thread::sleep_for(8s);
    const auto during = latest_completed(*cluster);
    const auto refused = proxy.faults_applied();
    const auto served = feed_->served_max();
    const auto in_flight = in_flight_ids();
    proxy.clear();
    // A checkpoint whose barrier flush had already been confirmed when the fault
    // began may complete after it; none after that.
    EXPECT_LE(during, before + 1) << "checkpoints completed while no write was confirmed";
    EXPECT_GT(refused, 0U);
    EXPECT_GT(in_flight, 0U);
    await_converged(*cluster, 300s);
    const auto spent = restarts(*cluster);
    const auto unconfirmed = count_in_workers(*cluster, "does not confirm the batch");
    cancel_job(*cluster);
    report("unconfirmed_replies",
           *cluster,
           spent,
           served,
           in_flight,
           " checkpoint_before=" + std::to_string(before) + " checkpoint_after_8s=" +
               std::to_string(during) + " requests_refused=" + std::to_string(refused) +
               " unconfirmed_flushes_logged=" + std::to_string(unconfirmed));
    EXPECT_GE(spent, 1U) << "a flush that cannot confirm its batch fails the job, which restarts";
    EXPECT_GE(unconfirmed, 1U);
}

TEST_P(SearchRecovery, AReplyLostAfterTheServerAppliedAFlushThenAWorkerKilledConverges) {
    TcpFaultProxy tcp(endpoint_port());
    tcp.set_request_marker("POST /_bulk");
    auto cluster = make_cluster("");
    ScopedDiagnostics diagnostics(*cluster);
    submit(*cluster, "http://127.0.0.1:" + std::to_string(tcp.port()));
    add_spare_worker(*cluster);
    await_first_checkpoint(*cluster);
    const auto idle = latest_completed(*cluster);
    feed_->start();
    ASSERT_TRUE(clink::itest::await(
        [&] { return latest_completed(*cluster) > idle && stored().size() >= 20; }, 60s))
        << "nothing was written and checkpointed before the fault";
    const auto stored_before = stored();
    tcp.hold_at_upload(1);
    ASSERT_TRUE(clink::itest::await(
        [&] { return tcp.uploads_held() == 1 && tcp.held_clients_quiet(300ms); }, 60s))
        << "the proxy never held a bulk request";
    // The held request reached the server, which applied it: the index changed
    // although the sink has no reply.
    const bool applied = clink::itest::await([&] { return stored() != stored_before; }, 30s);
    const auto served = feed_->served_max();
    const auto in_flight = in_flight_ids();
    feed_->watch_rewind();
    kill_and_replace(*cluster, 0);
    tcp.drop_held();
    ASSERT_TRUE(cluster->await_workers_registered(2));
    await_converged(*cluster, 240s);
    const auto spent = restarts(*cluster);
    cancel_job(*cluster);
    const auto replayed = served - std::min(served, feed_->lowest_since());
    report("reply_lost_then_killed",
           *cluster,
           spent,
           served,
           in_flight,
           std::string{" applied_while_reply_held="} + (applied ? "yes" : "no") +
               " replayed=" + std::to_string(replayed));
    EXPECT_GT(replayed, 0) << "the restored job read nothing again";
    EXPECT_TRUE(applied) << "the server had not applied the held request";
    EXPECT_GT(in_flight, 0U);
    EXPECT_EQ(spent, 1U) << "one worker loss is one restart";
}

INSTANTIATE_TEST_SUITE_P(Elasticsearch,
                         SearchRecovery,
                         ::testing::Values(kElasticsearch),
                         [](const auto&) { return std::string{"es"}; });
INSTANTIATE_TEST_SUITE_P(OpenSearch,
                         SearchRecovery,
                         ::testing::Values(kOpenSearch),
                         [](const auto&) { return std::string{"os"}; });
