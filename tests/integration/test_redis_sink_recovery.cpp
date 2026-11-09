// The Redis sinks' recovery cells: a coordinator and two clink_node workers run
// a job that polls a feed this test serves (http_poll_source, replayable from
// its checkpointed cursor) into redis_sink or redis_upsert_sink, while the feed
// releases records at a steady rate. Record n names row k<n mod ids>, with a
// rising seq and v. Each cell parks worker 0 at one of the sinks' fault points,
// SIGKILLs it there, and lets the job restore:
//
//   1. stream_before_flush: redis_sink parked at redis.sink_before_flush, a
//      barrier's XADDs built and none of them sent;
//   2. stream_after_flush: redis_sink parked at redis.sink_after_flush, the
//      flush applied and the barrier that asked for it not acknowledged, so the
//      checkpoint it belongs to never completes;
//   3. upsert_before_flush and 4. upsert_after_flush: the same two points in
//      redis_upsert_sink, where every seventh record of a row deletes it;
//   5. sql_after_flush: cell 4 as a SQL job (connector='http_poll' into
//      connector='redis' mode='upsert') at parallelism 2, through a GROUP BY
//      whose counts replace by key with update_before and update_after rows.
//
// Pass criteria, read through a RESP connection of the test's own, apart from
// the sinks' client library, and never from anything clink reports. The stream
// cells: every record's entry is in the stream, exactly as the record was
// written, and the records with more than one entry are exactly those the sink
// flushed after the checkpoint the job restored, each in two; before the flush
// that set is empty, after it it is not. The Streams sink is at-least-once,
// and the cells count its duplicates rather than forbid them. The keyed cells:
// every key the input's last records leave present holds that record as
// written, and every key they delete is absent. Every cell: the one restart
// the kill costs, a replay (the feed was read again from the checkpoint's
// cursor), and the fault landing where the cell names, witnessed by the fault
// framework's log line.
//
// Needs the server scripts/brokers-live.sh names (CLINK_REDIS_TEST_URL) and
// the clink_node binary of a build with fault injection (clink_submit_sql too
// for cell 5). Each cell prints one "[redis-sink-recovery]" line and, with
// CLINK_REDIS_RECOVERY_LOG_DIR set, keeps the cluster's logs there.

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
#include <netdb.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>

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

// The test's own Redis connection: RESP over a plain socket. A reply is a
// tree of these; an error reply throws.
struct RespValue {
    char type{'+'};
    std::string text;  // simple and bulk strings
    long long integer{0};
    bool absent{false};            // a nil bulk string or array
    std::vector<RespValue> items;  // arrays
};

class Resp {
public:
    Resp(const std::string& host, const std::string& port) {
        addrinfo hints{};
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        addrinfo* found = nullptr;
        if (::getaddrinfo(host.c_str(), port.c_str(), &hints, &found) != 0 || found == nullptr) {
            throw std::runtime_error("cannot resolve " + host);
        }
        fd_ = ::socket(found->ai_family, found->ai_socktype, found->ai_protocol);
        const int rc = fd_ < 0 ? -1 : ::connect(fd_, found->ai_addr, found->ai_addrlen);
        ::freeaddrinfo(found);
        if (rc != 0) {
            throw std::runtime_error("cannot connect to " + host + ":" + port);
        }
    }
    ~Resp() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }
    Resp(const Resp&) = delete;
    Resp& operator=(const Resp&) = delete;

    RespValue command(const std::vector<std::string>& args) {
        std::string out = "*" + std::to_string(args.size()) + "\r\n";
        for (const auto& a : args) {
            out += "$" + std::to_string(a.size()) + "\r\n" + a + "\r\n";
        }
        for (std::size_t sent = 0; sent < out.size();) {
            const auto n = ::send(fd_, out.data() + sent, out.size() - sent, 0);
            if (n <= 0) {
                throw std::runtime_error("the test's Redis connection closed");
            }
            sent += static_cast<std::size_t>(n);
        }
        return read_();
    }

private:
    void fill_() {
        char chunk[65536];
        const auto n = ::recv(fd_, chunk, sizeof chunk, 0);
        if (n <= 0) {
            throw std::runtime_error("the test's Redis connection closed");
        }
        buf_.append(chunk, static_cast<std::size_t>(n));
    }
    std::string line_() {
        std::size_t at = 0;
        while ((at = buf_.find("\r\n")) == std::string::npos) {
            fill_();
        }
        auto line = buf_.substr(0, at);
        buf_.erase(0, at + 2);
        return line;
    }
    std::string exact_(std::size_t n) {
        while (buf_.size() < n + 2) {
            fill_();
        }
        auto s = buf_.substr(0, n);
        buf_.erase(0, n + 2);
        return s;
    }
    RespValue read_() {
        const auto line = line_();
        RespValue v;
        v.type = line.empty() ? '?' : line[0];
        const auto rest = line.substr(1);
        switch (v.type) {
            case '+':
                v.text = rest;
                break;
            case '-':
                throw std::runtime_error("Redis refused the test's command: " + rest);
            case ':':
                v.integer = std::stoll(rest);
                break;
            case '$': {
                const auto n = std::stoll(rest);
                if (n < 0) {
                    v.absent = true;
                } else {
                    v.text = exact_(static_cast<std::size_t>(n));
                }
                break;
            }
            case '*': {
                const auto n = std::stoll(rest);
                v.absent = n < 0;
                for (long long i = 0; i < n; ++i) {
                    v.items.push_back(read_());
                }
                break;
            }
            default:
                throw std::runtime_error("an unreadable reply: " + line);
        }
        return v;
    }

    int fd_{-1};
    std::string buf_;
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
class RedisSinkFeed {
public:
    RedisSinkFeed(std::int64_t total, int ids, int per_second, bool deletes)
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
    ~RedisSinkFeed() {
        stop_.store(true);
        if (releaser_.joinable()) {
            releaser_.join();
        }
        svr_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    RedisSinkFeed(const RedisSinkFeed&) = delete;
    RedisSinkFeed& operator=(const RedisSinkFeed&) = delete;

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

class RedisSinkRecovery : public ::testing::Test {
protected:
    void SetUp() override {
        auto url = env_or_empty("CLINK_REDIS_TEST_URL");
        if (url.empty()) {
            GTEST_SKIP() << "set CLINK_REDIS_TEST_URL (scripts/brokers-live.sh does)";
        }
        if (!std::filesystem::exists(node_binary())) {
            GTEST_SKIP() << "the clink_node binary is not built";
        }
        if (url.rfind("redis://", 0) == 0) {
            url = url.substr(8);
        }
        const auto colon = url.find(':');
        host_ = url.substr(0, colon);
        port_ = colon == std::string::npos ? "6379" : url.substr(colon + 1);
        static int counter = 0;
        name_ = "clink:rsnk:" + std::to_string(::getpid()) + ":" + std::to_string(++counter);
        redis_ = std::make_unique<Resp>(host_, port_);
    }

    void TearDown() override {
        feed_.reset();
        if (redis_) {
            try {
                for (const auto& k : keys()) {
                    redis_->command({"DEL", k});
                }
                redis_->command({"DEL", name_});
            } catch (const std::exception&) {
            }
        }
    }

    void make_feed(bool deletes) {
        feed_ = std::make_unique<RedisSinkFeed>(3000, 100, 300, deletes);
    }

    [[nodiscard]] std::string prefix() const { return name_ + ":"; }

    // Every key under the keyed sink's prefix.
    std::vector<std::string> keys() {
        std::vector<std::string> out;
        std::string cursor = "0";
        do {
            const auto r =
                redis_->command({"SCAN", cursor, "MATCH", prefix() + "*", "COUNT", "1000"});
            cursor = r.items.at(0).text;
            for (const auto& k : r.items.at(1).items) {
                out.push_back(k.text);
            }
        } while (cursor != "0");
        return out;
    }

    // The stream's entries as seq -> how many; an entry that is not exactly
    // its record counts in `wrong`.
    std::map<std::int64_t, int> appended(std::size_t* wrong = nullptr) {
        std::map<std::int64_t, int> out;
        const auto r = redis_->command({"XRANGE", name_, "-", "+"});
        for (const auto& entry : r.items) {
            const auto& fields = entry.items.at(1).items;
            if (fields.size() != 2 || fields[0].text != "v") {
                if (wrong != nullptr) {
                    ++*wrong;
                }
                continue;
            }
            const auto n = number_member(fields[1].text, "seq");
            ++out[n];
            if (wrong != nullptr && fields[1].text != feed_->record(n)) {
                ++*wrong;
            }
        }
        return out;
    }

    // The keyed sink's values by row id.
    std::map<std::string, std::string> stored() {
        std::map<std::string, std::string> out;
        for (const auto& k : keys()) {
            const auto v = redis_->command({"GET", k});
            if (!v.absent) {
                out[k.substr(prefix().size())] = v.text;
            }
        }
        return out;
    }

    // What the keyed sink holds once the input is applied: each row's last
    // record as written, or no key when that record deletes it.
    [[nodiscard]] std::map<std::string, std::string> expected_values() const {
        std::map<std::string, std::string> out;
        for (std::int64_t n = 1; n <= feed_->total(); ++n) {
            if (feed_->deletes_row(n)) {
                out.erase(feed_->id_of(n));
            } else {
                out[feed_->id_of(n)] = feed_->record(n);
            }
        }
        return out;
    }

    std::map<std::string, std::string> sink_params(bool keyed) const {
        std::map<std::string, std::string> p = {{"host", host_}, {"port", port_}};
        if (keyed) {
            p["key_columns"] = "id";
            p["key_prefix"] = prefix();
        } else {
            p["stream"] = name_;
        }
        return p;
    }

    clink::cluster::JobGraphSpec graph(const std::string& sink) const {
        clink::cluster::JobGraphSpec g;
        clink::cluster::OperatorSpec src;
        src.type = "http_poll_source";
        src.id = "src";
        src.uid = "redis-sink-recovery-src";
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
        snk.uid = "redis-sink-recovery-snk";
        snk.inputs = {"src"};
        snk.out_channel = std::string{clink::cluster::kChannelString};
        snk.params = sink_params(sink == "redis_upsert_sink");
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

    void submit(Cluster& cluster, const std::string& sink) {
        auto http = coordinator(cluster);
        const auto r = http.post("/api/v1/jobs/spec?name=redis-sink-recovery&checkpoint_dir=" +
                                     cluster.checkpoint_dir().string() +
                                     "&checkpoint_interval_ms=1000&alignment=aligned"
                                     "&max_restarts_on_worker_loss=8",
                                 graph(sink).to_json(),
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
                                  "redis-sink-recovery-sql",
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
    // after that, so nothing is still on its way into Redis.
    void await_settled(Cluster& cluster, const std::function<bool()>& done) {
        ASSERT_TRUE(clink::itest::await(
            [&] { return feed_->released() == feed_->total() && done(); }, 240s))
            << "Redis never held every record";
        const auto after = latest_completed(cluster);
        ASSERT_TRUE(
            clink::itest::await([&] { return latest_completed(cluster) >= after + 2; }, 60s))
            << "checkpoints stopped completing after Redis converged";
    }

    // The keyed values equal to `want`, or the keys that are not, named.
    void expect_keyed(const std::map<std::string, std::string>& want) {
        const auto got = stored();
        std::size_t wrong = 0;
        for (const auto& [id, value] : want) {
            const auto it = got.find(id);
            if ((it == got.end() || it->second != value) && ++wrong <= 5) {
                ADD_FAILURE() << "key " << id << " holds "
                              << (it == got.end() ? std::string{"nothing"} : it->second) << ", not "
                              << value;
            }
        }
        for (const auto& [id, value] : got) {
            if (want.count(id) == 0 && ++wrong <= 10) {
                ADD_FAILURE() << "key " << id << " is present (" << value
                              << ") although its last record deletes it";
            }
        }
        EXPECT_EQ(wrong, 0U) << got.size() << " keys, " << wrong
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
        const auto into = env_or_empty("CLINK_REDIS_RECOVERY_LOG_DIR");
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
        std::printf("[redis-sink-recovery] cell=%s records=%lld rows=%d restarts=%zu%s\n",
                    cell.c_str(),
                    static_cast<long long>(feed_->total()),
                    feed_->ids(),
                    restarts(cluster),
                    fields.c_str());
        std::fflush(stdout);
    }

    void stream_cell(const std::string& point, const std::string& cell);
    void upsert_cell(const std::string& point, const std::string& cell);

    std::string host_;
    std::string port_;
    std::string name_;
    std::unique_ptr<Resp> redis_;
    std::unique_ptr<RedisSinkFeed> feed_;
    std::vector<std::string> archived_logs_;
};

void RedisSinkRecovery::stream_cell(const std::string& point, const std::string& cell) {
    const bool before_flush = point == "redis.sink_before_flush";
    make_feed(/*deletes=*/false);
    // Reached once per flush with records, and the sink flushes at barriers:
    // the third lands mid-feed.
    auto cluster = make_cluster(point + "=block@3");
    ScopedDiagnostics diagnostics(*cluster);
    submit(*cluster, "redis_sink");
    add_spare_worker(*cluster);
    await_first_checkpoint(*cluster);
    feed_->start();
    ASSERT_TRUE(parked_at(*cluster, point)) << "worker 0 never reached " << point;
    const auto served = feed_->served_max();
    const auto at_park = appended();
    const auto flushed = at_park.empty() ? 0 : at_park.rbegin()->first;
    feed_->watch_rewind();
    kill_and_replace(*cluster, 0);
    ASSERT_TRUE(cluster->await_workers_registered(2));
    const auto total = feed_->total();
    await_settled(*cluster, [&] { return appended().size() == static_cast<std::size_t>(total); });
    std::size_t wrong = 0;
    const auto entries = appended(&wrong);
    // The restored source read again from its checkpoint's cursor: records up
    // to it were flushed once before the kill, those after it and up to the
    // last flushed before the kill twice, and the rest once.
    const auto restored = feed_->lowest_since();
    std::size_t missing = 0;
    std::size_t twice = 0;
    std::size_t unexpected = 0;
    for (std::int64_t n = 1; n <= total; ++n) {
        const int want = n > restored && n <= flushed ? 2 : 1;
        const auto it = entries.find(n);
        const int got = it == entries.end() ? 0 : it->second;
        missing += got == 0 ? 1 : 0;
        twice += got == 2 ? 1 : 0;
        if (got != want && ++unexpected <= 5) {
            ADD_FAILURE() << "seq " << n << " is in " << got << " entries, not " << want;
        }
    }
    std::size_t entries_total = 0;
    for (const auto& [n, c] : entries) {
        entries_total += static_cast<std::size_t>(c);
    }
    const auto replayed = served - std::min(served, restored);
    const auto spent = restarts(*cluster);
    cancel_job(*cluster);
    report(cell,
           *cluster,
           " served_at_fault=" + std::to_string(served) + " flushed_at_fault=" +
               std::to_string(flushed) + " restored_cursor=" + std::to_string(restored) +
               " replayed=" + std::to_string(replayed) + " stream_entries=" +
               std::to_string(entries_total) + " written_twice=" + std::to_string(twice));
    EXPECT_EQ(entries.size(), static_cast<std::size_t>(total))
        << "entries for records never served";
    EXPECT_EQ(missing, 0U) << "records were lost";
    EXPECT_EQ(unexpected, 0U) << "entries written twice other than the replay of flushed records";
    EXPECT_EQ(wrong, 0U) << "entries that are not their record as written";
    EXPECT_EQ(spent, 1U) << "one worker loss is one restart";
    EXPECT_GT(replayed, 0) << "the restored job read nothing again";
    EXPECT_GT(flushed, 0) << "nothing was flushed when the worker parked";
    if (before_flush) {
        EXPECT_LE(flushed, restored)
            << "entries flushed after the checkpoint the job restored, though the kill came "
               "before the flush was sent";
    } else {
        EXPECT_GT(flushed, restored) << "the parked flush was not applied before the kill";
        EXPECT_GT(twice, 0U);
    }
}

void RedisSinkRecovery::upsert_cell(const std::string& point, const std::string& cell) {
    make_feed(/*deletes=*/true);
    auto cluster = make_cluster(point + "=block@3");
    ScopedDiagnostics diagnostics(*cluster);
    submit(*cluster, "redis_upsert_sink");
    add_spare_worker(*cluster);
    await_first_checkpoint(*cluster);
    feed_->start();
    ASSERT_TRUE(parked_at(*cluster, point)) << "worker 0 never reached " << point;
    const auto served = feed_->served_max();
    const auto stored_at_park = stored().size();
    feed_->watch_rewind();
    kill_and_replace(*cluster, 0);
    ASSERT_TRUE(cluster->await_workers_registered(2));
    const auto want = expected_values();
    await_settled(*cluster, [&] { return stored() == want; });
    expect_keyed(want);
    const auto replayed = served - std::min(served, feed_->lowest_since());
    const auto spent = restarts(*cluster);
    cancel_job(*cluster);
    report(cell,
           *cluster,
           " served_at_fault=" + std::to_string(served) + " keys_at_fault=" +
               std::to_string(stored_at_park) + " replayed=" + std::to_string(replayed) +
               " keys_present_at_end=" + std::to_string(want.size()) +
               " keys_deleted_at_end=" + std::to_string(feed_->deleted(feed_->total())));
    EXPECT_EQ(spent, 1U) << "one worker loss is one restart";
    EXPECT_GT(replayed, 0) << "the restored job read nothing again";
    EXPECT_GT(stored_at_park, 0U) << "nothing was written when the worker parked";
}

}  // namespace

TEST_F(RedisSinkRecovery, AStreamSinkKilledBeforeAFlushWritesEveryRecordOnce) {
    stream_cell("redis.sink_before_flush", "stream_before_flush");
}

TEST_F(RedisSinkRecovery, AStreamSinkKilledAfterAFlushAppendsTheReplayedRecordsAgain) {
    stream_cell("redis.sink_after_flush", "stream_after_flush");
}

TEST_F(RedisSinkRecovery, AKeyedSinkKilledBeforeAFlushConverges) {
    upsert_cell("redis.sink_before_flush", "upsert_before_flush");
}

TEST_F(RedisSinkRecovery, AKeyedSinkKilledAfterAFlushConvergesOnReplay) {
    upsert_cell("redis.sink_after_flush", "upsert_after_flush");
}

TEST_F(RedisSinkRecovery, ASqlUpsertJobKilledAfterAFlushConvergesOnReplay) {
    if (!std::filesystem::exists(sql_binary())) {
        GTEST_SKIP() << "the clink_submit_sql binary is not built";
    }
    make_feed(/*deletes=*/false);
    // A GROUP BY's counts per row: every record replaces its row's count with an
    // update_before and update_after pair, a delete and a SET by key.
    const std::string sql =
        "CREATE TABLE feed (id STRING, seq BIGINT, v BIGINT) WITH (connector='http_poll', "
        "format='json', url='" +
        feed_->url() +
        "', path='/feed', records_field='items', cursor_param='since', cursor_field='seq', "
        "poll_interval_ms='20');\n"
        "CREATE TABLE counts (id STRING, seq BIGINT, v BIGINT) WITH (connector='redis', host='" +
        host_ + "', port='" + port_ + "', key_prefix='" + prefix() +
        "', mode='upsert', primary_key='id');\n"
        "INSERT INTO counts SELECT id, COUNT(*) AS seq, MAX(v) AS v FROM feed GROUP BY id;\n";
    const std::string point = "redis.sink_after_flush";
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
    const auto counted = [&] {
        std::map<std::string, std::string> out;
        for (const auto& [id, value] : stored()) {
            out[id] = std::to_string(number_member(value, "seq")) + "/" +
                      std::to_string(number_member(value, "v"));
        }
        return out;
    };
    await_settled(*cluster, [&] { return counted() == want; });
    const auto got = counted();
    std::size_t wrong = 0;
    for (const auto& [id, row] : want) {
        const auto it = got.find(id);
        if ((it == got.end() || it->second != row) && ++wrong <= 5) {
            ADD_FAILURE() << "key " << id << " counts "
                          << (it == got.end() ? std::string{"nothing"} : it->second) << ", not "
                          << row;
        }
    }
    EXPECT_EQ(got.size(), want.size());
    const auto replayed = served - std::min(served, feed_->lowest_since());
    const auto spent = restarts(*cluster);
    cancel_job(*cluster);
    report("sql_after_flush",
           *cluster,
           " served_at_fault=" + std::to_string(served) +
               " keys_at_fault=" + std::to_string(stored_at_park) +
               " replayed=" + std::to_string(replayed) + " parallelism=2");
    EXPECT_EQ(wrong, 0U);
    EXPECT_EQ(spent, 1U) << "one worker loss is one restart";
    EXPECT_GT(replayed, 0) << "the restored job read nothing again";
    EXPECT_GT(stored_at_park, 0U);
}
