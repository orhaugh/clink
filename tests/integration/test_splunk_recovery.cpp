// The Splunk HEC sink's recovery cells: a coordinator and two clink_node
// workers run a job that polls a feed this test serves (http_poll_source,
// replayable from its checkpointed cursor) into splunk_hec_sink, which writes
// to a stub HEC this test also serves (impls/http_connector/tests/hec_stub.hpp).
// Record n is {"id":"k<n mod ids>","seq":n,"v":10n}. Each cell parks worker 0
// at splunk_hec_sink.after_write after the eighth accepted request, which no
// completed checkpoint covers while earlier checkpoints covered records,
// SIGKILLs it there and lets the job restore, so the restored job sends again
// the events the stub accepted past the restore point:
//
//   1. a job graph with ack='none': a barrier waits for HEC's acceptance;
//   2. a job graph with ack='indexer', the stub's token requiring indexer
//      acknowledgement and acknowledging each request at its second query:
//      a barrier waits for every acknowledgement, and each run sends on a
//      channel of its own;
//   3. cell 1 as a SQL job (connector='http_poll' into connector='splunk'),
//      submitted by clink_submit_sql at parallelism 2.
//
// Pass criteria, read from the stub's own acceptance log and never from
// anything clink reports: every record present at least once with exactly its
// payload and the sink's metadata, duplicates only of records the restored job
// read again, and duplicates present, as the at-least-once contract says. Each
// cell also checks the one restart the kill costs, a replay from the
// checkpoint's cursor, the fault framework's witness of where the worker
// parked, and that the parked request carried records past the cursor the job
// restored from. Each prints one "[splunk-recovery]" line and, with
// CLINK_SPLUNK_RECOVERY_LOG_DIR set, keeps the cluster's logs and the stub's
// acceptance log there.
//
// The stub is a protocol model, not Splunk; these cells are evidence about the
// sink and the engine, not about a Splunk server. They need the clink_node
// binary of a build with fault injection (and clink_submit_sql for cell 3).

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
#include <set>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/job_graph.hpp"
#include "clink/config/json.hpp"
#include "clink/http_connector/http_request.hpp"

#include "../checkpoint_markers.hpp"
#include "hec_stub.hpp"
#include "tests/integration/cluster_harness.hpp"

namespace {

using namespace std::chrono_literals;
using clink::http_connector::hec_stub::HecStub;
using clink::itest::Cluster;
using clink::itest::ClusterSpec;
using clink::itest::ProcOptions;
using clink::itest::ScopedDiagnostics;

constexpr char kTokenVar[] = "CLINK_SPLUNK_RECOVERY_TOKEN";
constexpr char kToken[] = "recovery-hec-token";
constexpr char kSourcetype[] = "clink:recovery";

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

std::string env_or(const char* name) {
    const char* v = std::getenv(name);
    return v == nullptr ? std::string{} : std::string{v};
}

// The job's input. GET /feed?since=<seq> answers {"items":[...]}: up to 200 of
// the records released so far whose seq is after <seq>, ascending, the exclusive
// cursor http_poll_source replays from.
class SplunkFeed {
public:
    SplunkFeed(std::int64_t total, int ids, int per_second)
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
    ~SplunkFeed() {
        stop_.store(true);
        if (releaser_.joinable()) {
            releaser_.join();
        }
        svr_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    SplunkFeed(const SplunkFeed&) = delete;
    SplunkFeed& operator=(const SplunkFeed&) = delete;

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

    [[nodiscard]] std::string record(std::int64_t n) const {
        return R"({"id":"k)" + std::to_string(n % ids_) + R"(","seq":)" + std::to_string(n) +
               R"(,"v":)" + std::to_string(n * 10) + "}";
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

// What the stub's acceptance log says about the records.
struct Tally {
    std::map<std::int64_t, std::size_t> copies;  // seq -> events carrying it
    std::size_t events{0};
    std::size_t wrong_payload{0};   // an event that is not its record, byte for byte
    std::size_t wrong_metadata{0};  // an event without exactly the sink's metadata
    std::int64_t highest{0};
    std::set<std::string> channels;
    std::string first_wrong;
};

class SplunkHecRecovery : public ::testing::Test {
protected:
    void SetUp() override {
        if (!std::filesystem::exists(recovery_node_binary())) {
            GTEST_SKIP() << "the clink_node binary is not built";
        }
        // The token reaches the workers as env://, so it is in no job spec or log.
        ::setenv(kTokenVar, kToken, 1);
        static int counter = 0;
        cell_ = "splunk_rec_" + std::to_string(static_cast<long>(::getpid())) + "_" +
                std::to_string(++counter);
        feed_ = std::make_unique<SplunkFeed>(3000, 100, 300);
    }

    void TearDown() override {
        feed_.reset();
        stub_.reset();
    }

    void start_stub(bool require_ack) {
        HecStub::Options o;
        o.token = kToken;
        o.require_ack = require_ack;
        o.ack_after = 1;  // an ackId reads true at its second query
        stub_ = std::make_unique<HecStub>(o);
    }

    clink::cluster::JobGraphSpec graph(bool indexer) const {
        clink::cluster::JobGraphSpec g;
        clink::cluster::OperatorSpec src;
        src.type = "http_poll_source";
        src.id = "src";
        src.uid = "splunk-recovery-src";
        src.out_channel = std::string{clink::cluster::kChannelString};
        src.params = {{"url", feed_->url()},
                      {"path", "/feed"},
                      {"cursor_param", "since"},
                      {"cursor_field", "seq"},
                      {"records_field", "items"},
                      {"poll_interval_ms", "20"}};
        g.ops.push_back(src);
        clink::cluster::OperatorSpec snk;
        snk.type = "splunk_hec_sink";
        snk.id = "snk";
        snk.uid = "splunk-recovery-snk";
        snk.inputs = {"src"};
        snk.out_channel = std::string{clink::cluster::kChannelString};
        snk.params = {{"url", stub_->url()},
                      {"token", std::string{"env://"} + kTokenVar},
                      {"sourcetype", kSourcetype},
                      {"batch_records", "100"}};
        if (indexer) {
            snk.params["ack"] = "indexer";
            snk.params["ack_poll_ms"] = "50";
        }
        g.ops.push_back(snk);
        return g;
    }

    // Every event the stub accepted, read against the records. `exact` compares
    // the payload byte for byte; otherwise as parsed JSON (the SQL path writes
    // the row its own way).
    Tally tally(bool exact) const {
        Tally t;
        for (const auto& e : stub_->accepted()) {
            ++t.events;
            t.channels.insert(e.channel);
            std::int64_t seq = -1;
            try {
                seq = clink::config::parse(e.event).at("seq").as_int();
            } catch (const std::exception&) {
            }
            const bool payload_ok =
                seq > 0 &&
                (exact ? e.event == feed_->record(seq)
                       : clink::config::parse(e.event) == clink::config::parse(feed_->record(seq)));
            if (!payload_ok && t.wrong_payload++ == 0) {
                t.first_wrong = e.event;
            }
            const auto st = e.meta.find("sourcetype");
            if (e.meta.size() != 1 || st == e.meta.end() ||
                st->second != "\"" + std::string{kSourcetype} + "\"") {
                ++t.wrong_metadata;
            }
            if (seq > 0) {
                ++t.copies[seq];
                t.highest = std::max(t.highest, seq);
            }
        }
        return t;
    }

    [[nodiscard]] bool complete(bool exact) const {
        return tally(exact).copies.size() == static_cast<std::size_t>(feed_->total());
    }

    void keep_logs(Cluster& cluster) const {
        const auto into = env_or("CLINK_SPLUNK_RECOVERY_LOG_DIR");
        if (into.empty()) {
            return;
        }
        std::error_code ec;
        const auto dir = std::filesystem::path(into) / cell_;
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
        // The stub's acceptance log, one line per accepted event: the evidence
        // each verdict rests on.
        std::ofstream log(dir / "acceptance-log.ndjson");
        for (const auto& e : stub_->accepted()) {
            log << R"({"request":)" << e.request << R"(,"channel":")" << e.channel
                << R"(","event":)" << e.event << "}\n";
        }
        std::ofstream acks(dir / "acknowledged.txt");
        for (const auto id : stub_->acknowledged()) {
            acks << id << "\n";
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
        std::int64_t highest_at_fault{0};  // the highest seq the stub held when the worker parked
        std::size_t events_at_fault{0};
        // The highest seq the stub held once the killed worker was gone: what
        // the run before the restore could have sent, another subtask's
        // requests between the park and the kill included.
        std::int64_t highest_old_run{0};
        std::size_t restarts{0};
    };

    // Parks worker 0 at splunk_hec_sink.after_write, kills it, and waits until
    // every record is released and the stub holds every one, then two more
    // checkpoints.
    template <typename Submit>
    Outcome run_cell(Submit submit_job, bool exact, int slots = 3);

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

    void submit(Cluster& cluster, bool indexer) const {
        auto http = coordinator(cluster);
        const auto r = http.post("/api/v1/jobs/spec?name=splunk-recovery&checkpoint_dir=" +
                                     cluster.checkpoint_dir().string() +
                                     "&checkpoint_interval_ms=1000&alignment=aligned"
                                     "&max_restarts_on_worker_loss=8",
                                 graph(indexer).to_json(),
                                 "application/json");
        ASSERT_NE(r.body.find("\"ok\":true"), std::string::npos)
            << "the job was not accepted: " << r.body;
    }

    static void submit_sql(Cluster& cluster, const std::string& sql, int parallelism) {
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
                                  "splunk-recovery-sql",
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

    static void await_first_checkpoint(Cluster& cluster) {
        ASSERT_TRUE(clink::itest::await([&] { return latest_completed(cluster) >= 1; }, 60s))
            << "no checkpoint completed";
    }

    // The checks every cell shares, printed as one line.
    void judge(const char* name, const Outcome& o, bool exact, const std::string& extra = {}) {
        const Tally t = tally(exact);
        std::size_t duplicated = 0;
        std::size_t outside = 0;  // duplicated records the restored job never read again
        for (const auto& [seq, n] : t.copies) {
            if (n > 1) {
                ++duplicated;
                if (seq <= o.rewound_to || seq > o.highest_old_run) {
                    ++outside;
                }
            }
        }
        std::size_t missing = 0;
        for (std::int64_t n = 1; n <= feed_->total(); ++n) {
            if (t.copies.find(n) == t.copies.end() && ++missing <= 5) {
                ADD_FAILURE() << "record " << n << " never reached the stub";
            }
        }
        std::printf(
            "[splunk-recovery] cell=%s records=%lld events=%zu records_twice=%zu missing=%zu "
            "wrong_payload=%zu wrong_metadata=%zu restarts=%zu served_at_fault=%lld "
            "highest_at_fault=%lld highest_old_run=%lld events_at_fault=%zu rewound_to=%lld "
            "replayed=%lld%s\n",
            name,
            static_cast<long long>(feed_->total()),
            t.events,
            duplicated,
            missing,
            t.wrong_payload,
            t.wrong_metadata,
            o.restarts,
            static_cast<long long>(o.served),
            static_cast<long long>(o.highest_at_fault),
            static_cast<long long>(o.highest_old_run),
            o.events_at_fault,
            static_cast<long long>(o.rewound_to),
            static_cast<long long>(o.replayed),
            extra.c_str());
        EXPECT_EQ(missing, 0U) << "every record must reach the stub at least once";
        EXPECT_EQ(t.wrong_payload, 0U) << "an event is not its record: " << t.first_wrong;
        EXPECT_EQ(t.wrong_metadata, 0U) << "an event lacks the sink's metadata";
        EXPECT_GT(duplicated, 0U) << "the replay sent accepted events again";
        EXPECT_EQ(outside, 0U) << "only records the restored job read again may be duplicated";
        EXPECT_EQ(o.restarts, 1U) << "one worker loss is one restart";
        EXPECT_GT(o.replayed, 0) << "the restored job read nothing again";
        EXPECT_GT(o.events_at_fault, 0U) << "nothing was accepted when the worker parked";
        // The parked request was accepted, and carried records no checkpoint
        // covered: the job restored from a cursor below them.
        EXPECT_GT(o.highest_at_fault, o.rewound_to)
            << "the worker parked after a request a checkpoint already covered";
        EXPECT_GT(o.rewound_to, 0)
            << "the job restored from before any record, so no checkpoint covered an accepted one";
    }

    std::string cell_;
    std::unique_ptr<SplunkFeed> feed_;
    std::unique_ptr<HecStub> stub_;
    std::vector<std::string> archived_logs_;
};

template <typename Submit>
SplunkHecRecovery::Outcome SplunkHecRecovery::run_cell(Submit submit_job, bool exact, int slots) {
    Outcome out;
    const std::string point = "splunk_hec_sink.after_write";
    // The eighth accepted request: by then checkpoints have covered records,
    // so the restore point tells records a checkpoint covered (never sent
    // again) from those it did not.
    auto cluster = make_cluster(point + "=block@8", slots);
    ScopedDiagnostics diagnostics(*cluster);
    submit_job(*cluster);
    add_spare_worker(*cluster);
    await_first_checkpoint(*cluster);
    feed_->start();
    EXPECT_TRUE(parked_at(*cluster, point)) << "worker 0 never reached " << point;
    out.served = feed_->served_max();
    const Tally at_fault = tally(exact);
    out.events_at_fault = at_fault.events;
    out.highest_at_fault = at_fault.highest;
    feed_->watch_rewind();
    kill_and_replace(*cluster, 0);
    out.highest_old_run = tally(exact).highest;
    EXPECT_TRUE(cluster->await_workers_registered(2));
    const bool done = clink::itest::await(
        [&] { return feed_->released() == feed_->total() && complete(exact); }, 240s);
    EXPECT_TRUE(done) << "the stub did not receive every record";
    const auto after = latest_completed(*cluster);
    EXPECT_TRUE(clink::itest::await([&] { return latest_completed(*cluster) >= after + 2; }, 60s))
        << "checkpoints stopped completing after every record arrived";
    out.rewound_to = feed_->lowest_since();
    out.replayed = out.served - std::min(out.served, out.rewound_to);
    out.restarts = restarts(*cluster);
    cancel_job(*cluster);
    keep_logs(*cluster);
    return out;
}

}  // namespace

TEST_F(SplunkHecRecovery, AWorkerKilledAfterAcceptanceLosesNoEventAndDuplicatesOnlyTheReplay) {
    start_stub(/*require_ack=*/false);
    const auto o = run_cell([this](Cluster& c) { submit(c, /*indexer=*/false); }, /*exact=*/true);
    judge("none", o, /*exact=*/true);
}

TEST_F(SplunkHecRecovery, AWorkerKilledBeforeItsAcknowledgementsLosesNoEvent) {
    start_stub(/*require_ack=*/true);
    const auto o = run_cell([this](Cluster& c) { submit(c, /*indexer=*/true); }, /*exact=*/true);
    const Tally t = tally(true);
    const auto acked = stub_->acknowledged().size();
    judge("indexer",
          o,
          /*exact=*/true,
          " channels=" + std::to_string(t.channels.size()) +
              " acknowledged=" + std::to_string(acked) +
              " ack_queries=" + std::to_string(stub_->ack_queries().size()));
    // Each run sent on its own channel, and acknowledgements were asked for
    // and given; no event was accepted without a channel.
    EXPECT_GE(t.channels.size(), 2U) << "the restored run reused the killed run's channel";
    EXPECT_EQ(t.channels.count(""), 0U);
    EXPECT_GT(acked, 0U);
}

TEST_F(SplunkHecRecovery, ASqlJobKilledAfterAcceptanceLosesNoEvent) {
    if (!std::filesystem::exists(sql_binary())) {
        GTEST_SKIP() << "the clink_submit_sql binary is not built";
    }
    start_stub(/*require_ack=*/false);
    const std::string sql =
        "CREATE TABLE feed (id STRING, seq BIGINT, v BIGINT) WITH (connector='http_poll', "
        "format='json', url='" +
        feed_->url() +
        "', path='/feed', records_field='items', cursor_param='since', cursor_field='seq', "
        "poll_interval_ms='20');\n"
        "CREATE TABLE events (id STRING, seq BIGINT, v BIGINT) WITH (connector='splunk', "
        "format='json', url='" +
        stub_->url() + "', token='env://" + kTokenVar + "', sourcetype='" + kSourcetype +
        "', batch_records='100');\n"
        "INSERT INTO events SELECT id, seq, v FROM feed;\n";
    const auto o = run_cell([&](Cluster& c) { submit_sql(c, sql, /*parallelism=*/2); },
                            /*exact=*/false,
                            /*slots=*/8);
    judge("sql", o, /*exact=*/false, " parallelism=2");
}
