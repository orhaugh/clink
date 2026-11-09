// The generic HTTP sink's recovery cells: a coordinator and two clink_node
// workers run a job that polls a feed this test serves (http_poll_source,
// replayable from its checkpointed cursor) into http_sink, posting JSON-array
// bodies to an endpoint this test also serves, while the feed releases records
// at a steady rate. Record n names row k<n mod ids>, with a rising seq and v.
// Each cell parks worker 0 at one of the sink's fault points, SIGKILLs it there,
// and lets the job restore:
//
//   1. before_write: parked at http_sink.before_write, a flush's request built
//      and nothing of it sent;
//   2. after_write: parked at http_sink.after_write, the endpoint having
//      answered 2xx for every record of a flush and the barrier that asked for
//      it not acknowledged, so the checkpoint it belongs to never completes;
//   3. sql_after_write: cell 2 as a SQL job (connector='http_poll' into
//      connector='http') at parallelism 2.
//
// Pass criteria, read from what the endpoint received, each body split into
// its records as written, and never from anything clink reports: every record
// arrives, exactly as written, at most twice, and the records that arrive twice
// are exactly those the sink posted after the checkpoint the job restored and
// before the kill (in the SQL cell, by two subtasks: only records after the
// restored checkpoint). The sink is at-least-once, and the cells count its
// duplicates rather than forbid them. Every cell: the one restart the kill
// costs, a replay (the feed was read again from the checkpoint's cursor), and
// the fault landing where the cell names, witnessed by the fault framework's
// log line; cell 1 also records in flight at the kill, served and not posted.
//
// Needs only the clink_node binary of a build with fault injection
// (clink_submit_sql too for cell 3). Each cell prints one
// "[http-sink-recovery]" line and, with CLINK_HTTP_SINK_RECOVERY_LOG_DIR set,
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
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/job_graph.hpp"
#include "clink/http_connector/http_request.hpp"

#include "../checkpoint_markers.hpp"
#include "tests/integration/cluster_harness.hpp"

namespace {

using namespace std::chrono_literals;
using clink::itest::Cluster;
using clink::itest::ClusterSpec;
using clink::itest::ProcOptions;
using clink::itest::ScopedDiagnostics;

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

// The sink's endpoint, served by this test: POST /ingest takes a JSON-array
// body and keeps each of its records as written, in arrival order. It splits
// the body at the commas between its top-level values rather than reparse it,
// so a record arrives here byte for byte as the sink sent it.
class Endpoint {
public:
    Endpoint() {
        svr_.Post("/ingest", [this](const httplib::Request& req, httplib::Response& res) {
            const auto records = split(req.body);
            if (!records) {
                res.status = 400;
                return;
            }
            const std::lock_guard<std::mutex> lock(mu_);
            seen_.insert(seen_.end(), records->begin(), records->end());
        });
        port_ = svr_.bind_to_any_port("127.0.0.1");
        thread_ = std::thread([this] { svr_.listen_after_bind(); });
        while (!svr_.is_running()) {
            std::this_thread::sleep_for(2ms);
        }
    }
    ~Endpoint() {
        svr_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    Endpoint(const Endpoint&) = delete;
    Endpoint& operator=(const Endpoint&) = delete;

    [[nodiscard]] std::string url() const { return "http://127.0.0.1:" + std::to_string(port_); }

    // Every record received so far, in arrival order.
    std::vector<std::string> payloads() {
        const std::lock_guard<std::mutex> lock(mu_);
        return seen_;
    }

private:
    // The top-level values of a JSON array as their own text, or nothing when
    // the body is not an array of values the walk can follow.
    static std::optional<std::vector<std::string>> split(const std::string& body) {
        if (body.size() < 2 || body.front() != '[' || body.back() != ']') {
            return std::nullopt;
        }
        std::vector<std::string> out;
        int depth = 0;
        bool in_string = false;
        bool escaped = false;
        std::size_t from = 1;
        for (std::size_t i = 1; i + 1 < body.size(); ++i) {
            const char c = body[i];
            if (in_string) {
                if (escaped) {
                    escaped = false;
                } else if (c == '\\') {
                    escaped = true;
                } else if (c == '"') {
                    in_string = false;
                }
                continue;
            }
            if (c == '"') {
                in_string = true;
            } else if (c == '{' || c == '[') {
                ++depth;
            } else if (c == '}' || c == ']') {
                --depth;
            } else if (c == ',' && depth == 0) {
                out.push_back(body.substr(from, i - from));
                from = i + 1;
            }
        }
        if (body.size() > 2) {
            out.push_back(body.substr(from, body.size() - 1 - from));
        }
        return out;
    }

    httplib::Server svr_;
    int port_{0};
    std::thread thread_;
    std::mutex mu_;
    std::vector<std::string> seen_;
};

// A number member of a flat JSON object the feed or the SQL sink wrote.
std::int64_t number_member(const std::string& json, const std::string& name) {
    const auto tag = "\"" + name + "\":";
    const auto at = json.find(tag);
    return at == std::string::npos ? -1 : std::stoll(json.substr(at + tag.size()));
}

// The job's input. GET /feed?since=<seq> answers {"items":[...]}: up to 200 of
// the records released so far whose seq is after <seq>, ascending, the exclusive
// cursor http_poll_source replays from.
class HttpSinkFeed {
public:
    HttpSinkFeed(std::int64_t total, int ids, int per_second, bool deletes)
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
    ~HttpSinkFeed() {
        stop_.store(true);
        if (releaser_.joinable()) {
            releaser_.join();
        }
        svr_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    HttpSinkFeed(const HttpSinkFeed&) = delete;
    HttpSinkFeed& operator=(const HttpSinkFeed&) = delete;

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

    // Each row's last record once the first `upto` records are applied, "seq/v",
    // or no entry when that record deletes it.
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

class HttpSinkRecovery : public ::testing::Test {
protected:
    void SetUp() override {
        if (!std::filesystem::exists(node_binary())) {
            GTEST_SKIP() << "the clink_node binary is not built";
        }
        endpoint_ = std::make_unique<Endpoint>();
    }

    void TearDown() override {
        feed_.reset();
        endpoint_.reset();
    }

    void make_feed() { feed_ = std::make_unique<HttpSinkFeed>(3000, 100, 300, false); }

    // The topic's messages as seq -> how many; a message that is not exactly
    // its record counts in `wrong` (`exact` false: only its id and v).
    std::map<std::int64_t, int> stored(bool exact, std::size_t* wrong = nullptr) {
        std::map<std::int64_t, int> out;
        for (const auto& p : endpoint_->payloads()) {
            const auto n = number_member(p, "seq");
            ++out[n];
            const bool same =
                exact ? p == feed_->record(n)
                      : p.find("\"id\":\"" + feed_->id_of(n) + "\"") != std::string::npos &&
                            number_member(p, "v") == n * 10;
            if (wrong != nullptr && !same) {
                ++*wrong;
            }
        }
        return out;
    }

    // The topic's messages at the moment, once two readings 300 ms apart
    // agree: what the parked sink published before the kill.
    std::map<std::int64_t, int> settled_topic() {
        auto last = stored(true);
        for (int i = 0; i < 20; ++i) {
            std::this_thread::sleep_for(300ms);
            auto now = stored(true);
            if (now == last) {
                return now;
            }
            last = std::move(now);
        }
        return last;
    }

    clink::cluster::JobGraphSpec graph() const {
        clink::cluster::JobGraphSpec g;
        clink::cluster::OperatorSpec src;
        src.type = "http_poll_source";
        src.id = "src";
        src.uid = "http-sink-recovery-src";
        src.out_channel = std::string{clink::cluster::kChannelString};
        src.params = {{"url", feed_->url()},
                      {"path", "/feed"},
                      {"cursor_param", "since"},
                      {"cursor_field", "seq"},
                      {"records_field", "items"},
                      {"poll_interval_ms", "20"}};
        g.ops.push_back(src);
        clink::cluster::OperatorSpec snk;
        snk.type = "http_sink";
        snk.id = "snk";
        snk.uid = "http-sink-recovery-snk";
        snk.inputs = {"src"};
        snk.out_channel = std::string{clink::cluster::kChannelString};
        snk.params = {{"url", endpoint_->url()}, {"path", "/ingest"}};
        g.ops.push_back(snk);
        return g;
    }

    static ProcOptions worker_options(std::string fault = {}) {
        ProcOptions o;
        o.fault = std::move(fault);
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

    static void add_spare_worker(Cluster& cluster) {
        ASSERT_TRUE(cluster.start_worker(1, worker_options()));
        ASSERT_TRUE(cluster.await_workers_registered(2));
    }

    static clink::http_connector::HttpRequest coordinator(const Cluster& cluster) {
        clink::http_connector::HttpRequest::Options o;
        o.base_url = "http://127.0.0.1:" + std::to_string(cluster.http_port());
        return clink::http_connector::HttpRequest(std::move(o));
    }

    void submit(Cluster& cluster) {
        auto http = coordinator(cluster);
        const auto r = http.post("/api/v1/jobs/spec?name=http-sink-recovery&checkpoint_dir=" +
                                     cluster.checkpoint_dir().string() +
                                     "&checkpoint_interval_ms=1000&alignment=aligned"
                                     "&max_restarts_on_worker_loss=8",
                                 graph().to_json(),
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
                                  "http-sink-recovery-sql",
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

    // Every record released and `done` true, and two more checkpoints completed
    // after that, so nothing is still on its way into the topic.
    void await_settled(Cluster& cluster, const std::function<bool()>& done) {
        ASSERT_TRUE(clink::itest::await(
            [&] { return feed_->released() == feed_->total() && done(); }, 240s))
            << "the topic never held every record";
        const auto after = latest_completed(cluster);
        ASSERT_TRUE(
            clink::itest::await([&] { return latest_completed(cluster) >= after + 2; }, 60s))
            << "checkpoints stopped completing after the topic converged";
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
        const auto into = env_or_empty("CLINK_HTTP_SINK_RECOVERY_LOG_DIR");
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
        std::printf("[http-sink-recovery] cell=%s records=%lld rows=%d restarts=%zu%s\n",
                    cell.c_str(),
                    static_cast<long long>(feed_->total()),
                    feed_->ids(),
                    restarts(cluster),
                    fields.c_str());
        std::fflush(stdout);
    }

    void cell(const std::string& point, const std::string& name);

    std::unique_ptr<Endpoint> endpoint_;
    std::unique_ptr<HttpSinkFeed> feed_;
    std::vector<std::string> archived_logs_;
};

void HttpSinkRecovery::cell(const std::string& point, const std::string& name) {
    make_feed();
    // Reached once per flush with messages, and the sink flushes at barriers:
    // the third lands mid-feed.
    auto cluster = make_cluster(point + "=block@3");
    ScopedDiagnostics diagnostics(*cluster);
    submit(*cluster);
    add_spare_worker(*cluster);
    await_first_checkpoint(*cluster);
    feed_->start();
    ASSERT_TRUE(parked_at(*cluster, point)) << "worker 0 never reached " << point;
    const auto served = feed_->served_max();
    const auto at_park = settled_topic();
    const auto published = at_park.empty() ? 0 : at_park.rbegin()->first;
    feed_->watch_rewind();
    kill_and_replace(*cluster, 0);
    ASSERT_TRUE(cluster->await_workers_registered(2));
    const auto total = feed_->total();
    await_settled(*cluster, [&] { return stored(true).size() == static_cast<std::size_t>(total); });
    std::size_t wrong = 0;
    const auto messages = stored(true, &wrong);
    // The restored source read again from its checkpoint's cursor: records up
    // to it were published once before the kill, those after it and up to the
    // last published before the kill twice, and the rest once.
    const auto restored = feed_->lowest_since();
    std::size_t missing = 0;
    std::size_t twice = 0;
    std::size_t unexpected = 0;
    for (std::int64_t n = 1; n <= total; ++n) {
        const int want = n > restored && n <= published ? 2 : 1;
        const auto it = messages.find(n);
        const int got = it == messages.end() ? 0 : it->second;
        missing += got == 0 ? 1 : 0;
        twice += got == 2 ? 1 : 0;
        if (got != want && ++unexpected <= 5) {
            ADD_FAILURE() << "seq " << n << " is in " << got << " messages, not " << want;
        }
    }
    std::size_t messages_total = 0;
    for (const auto& [n, c] : messages) {
        messages_total += static_cast<std::size_t>(c);
    }
    const auto replayed = served - std::min(served, restored);
    const auto spent = restarts(*cluster);
    cancel_job(*cluster);
    report(name,
           *cluster,
           " served_at_fault=" + std::to_string(served) + " posted_at_fault=" +
               std::to_string(published) + " restored_cursor=" + std::to_string(restored) +
               " replayed=" + std::to_string(replayed) + " received=" +
               std::to_string(messages_total) + " posted_twice=" + std::to_string(twice));
    EXPECT_EQ(messages.size(), static_cast<std::size_t>(total))
        << "messages for records never served";
    EXPECT_EQ(missing, 0U) << "records were lost";
    EXPECT_EQ(unexpected, 0U)
        << "messages published twice other than the replay of those published before the kill";
    EXPECT_EQ(wrong, 0U) << "messages that are not their record as written";
    EXPECT_EQ(spent, 1U) << "one worker loss is one restart";
    EXPECT_GT(replayed, 0) << "the restored job read nothing again";
    if (name == "before_write") {
        // Nothing of the parked request was sent: it held records the topic did
        // not.
        EXPECT_GT(served, published) << "nothing was in flight when the worker parked";
    } else {
        EXPECT_GT(published, restored)
            << "nothing was published after the restored checkpoint before the kill";
        EXPECT_GT(twice, 0U);
    }
}

}  // namespace

TEST_F(HttpSinkRecovery, ASinkKilledBeforeAPostLosesNothing) {
    cell("http_sink.before_write", "before_write");
}

TEST_F(HttpSinkRecovery, ASinkKilledAfterTheEndpointAnsweredPostsTheReplayAgain) {
    cell("http_sink.after_write", "after_write");
}

TEST_F(HttpSinkRecovery, ASqlJobKilledAfterTheEndpointAnsweredLosesNothing) {
    if (!std::filesystem::exists(sql_binary())) {
        GTEST_SKIP() << "the clink_submit_sql binary is not built";
    }
    make_feed();
    const std::string sql =
        "CREATE TABLE feed (id STRING, seq BIGINT, v BIGINT) WITH (connector='http_poll', "
        "format='json', url='" +
        feed_->url() +
        "', path='/feed', records_field='items', cursor_param='since', cursor_field='seq', "
        "poll_interval_ms='20');\n"
        "CREATE TABLE out_s (id STRING, seq BIGINT, v BIGINT) WITH (connector='http', "
        "format='json', url='" +
        endpoint_->url() +
        "', path='/ingest');\n"
        "INSERT INTO out_s SELECT id, seq, v FROM feed;\n";
    const std::string point = "http_sink.after_write";
    auto cluster = make_cluster(point + "=block@3", /*slots=*/8);
    ScopedDiagnostics diagnostics(*cluster);
    submit_sql(*cluster, sql, /*parallelism=*/2);
    add_spare_worker(*cluster);
    await_first_checkpoint(*cluster);
    feed_->start();
    ASSERT_TRUE(parked_at(*cluster, point)) << "worker 0 never reached " << point;
    const auto served = feed_->served_max();
    feed_->watch_rewind();
    kill_and_replace(*cluster, 0);
    ASSERT_TRUE(cluster->await_workers_registered(2));
    const auto total = feed_->total();
    await_settled(*cluster,
                  [&] { return stored(false).size() == static_cast<std::size_t>(total); });
    std::size_t wrong = 0;
    const auto messages = stored(false, &wrong);
    const auto restored = feed_->lowest_since();
    std::size_t missing = 0;
    std::size_t twice = 0;
    std::size_t unexpected = 0;
    for (std::int64_t n = 1; n <= total; ++n) {
        const auto it = messages.find(n);
        const int got = it == messages.end() ? 0 : it->second;
        missing += got == 0 ? 1 : 0;
        twice += got == 2 ? 1 : 0;
        if ((got < 1 || got > 2 || (got == 2 && n <= restored)) && ++unexpected <= 5) {
            ADD_FAILURE() << "seq " << n << " is in " << got << " messages";
        }
    }
    const auto replayed = served - std::min(served, restored);
    const auto spent = restarts(*cluster);
    cancel_job(*cluster);
    report("sql_after_write",
           *cluster,
           " served_at_fault=" + std::to_string(served) + " restored_cursor=" +
               std::to_string(restored) + " replayed=" + std::to_string(replayed) +
               " posted_twice=" + std::to_string(twice) + " parallelism=2");
    EXPECT_EQ(messages.size(), static_cast<std::size_t>(total));
    EXPECT_EQ(missing, 0U) << "records were lost";
    EXPECT_EQ(unexpected, 0U) << "a record more than twice, or twice before the restored cursor";
    EXPECT_EQ(wrong, 0U) << "messages whose id or v is not their record's";
    EXPECT_EQ(spent, 1U) << "one worker loss is one restart";
    EXPECT_GT(replayed, 0) << "the restored job read nothing again";
}
