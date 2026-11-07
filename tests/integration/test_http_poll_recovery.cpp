// The HTTP polling source's recovery cells and SQL paths, against a feed the
// test serves: a retained dataset of 2,400 records, released at a steady rate,
// that answers GET /feed?since=<seq> with up to 100 records strictly after
// <seq>, ascending. Record n is {"seq":n,"id":"k<n>","big":<an integer past
// int64>,"v":"caf\u00e9 <n>"}, served without whitespace.
//
// The cells run a coordinator and two clink_node workers, the source in cursor
// mode (cursor_type integer) feeding a two-phase-commit file sink, and park
// worker 0 just after the source emitted a page (http_poll_source.after_emit),
// so the page's records are in flight: emitted, not covered by any completed
// checkpoint, and the source's cursor past them only in memory. The worker is
// killed there and started again; the job restores the last completed
// checkpoint, whose cursor sends the source back to before the page.
//
//   1. parallelism 1, a C++ job graph: every record committed once, in order,
//      byte for byte as the feed served it.
//   2. parallelism 2: the same, each record once (one subtask reads).
//   3. the same pipeline as SQL submitted by clink_submit_sql, into an
//      exactly-once filesystem table: every row once, its values the feed's.
//
// Each cell checks that the failure came with records in flight: the feed had
// served past what was committed when the worker died, and after the restart
// the source asked again from before the page it had emitted. The verdicts
// come from the committed files and the feed's own dataset, never from the
// job's view of itself.
//
// The SQL suite: `clink run` reads the feed bounded to its end and finishes,
// and the coordinator refuses a stronger delivery guarantee over a table in
// resource mode, naming the options that would make it replay.
//
// Needs the clink_node, clink_submit_sql and clink binaries of a build with
// fault injection. Each cell prints one "[http-poll-recovery]" line and, with
// CLINK_HTTP_POLL_RECOVERY_LOG_DIR set, keeps the cluster's logs there.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <httplib.h>
#include <map>
#include <memory>
#include <mutex>
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
#include "tests/integration/cluster_harness.hpp"

namespace {

using namespace std::chrono_literals;
using clink::itest::Cluster;
using clink::itest::ClusterSpec;
using clink::itest::ProcOptions;

std::filesystem::path hp_binary(const char* which) {
    const std::string w = which;
#ifdef CLINK_NODE_BINARY
    if (w == "node") {
        return std::filesystem::path{CLINK_NODE_BINARY};
    }
#endif
#ifdef CLINK_SUBMIT_SQL_BINARY
    if (w == "submit_sql") {
        return std::filesystem::path{CLINK_SUBMIT_SQL_BINARY};
    }
#endif
#ifdef CLINK_CLI_BINARY
    if (w == "cli") {
        return std::filesystem::path{CLINK_CLI_BINARY};
    }
#endif
    return {};
}

std::string hp_env(const char* name) {
    const char* v = std::getenv(name);
    return v != nullptr ? std::string{v} : std::string{};
}

// ---- the feed -------------------------------------------------------------------------

// Record n as the feed serves it, minified. "big" is past what a double or an
// int64 holds exactly, so only a source that keeps tokens as served delivers it.
std::string hp_record(std::int64_t n) {
    return "{\"seq\":" + std::to_string(n) + ",\"id\":\"k" + std::to_string(n) +
           "\",\"big\":" + std::to_string(18446744073709540000ULL + static_cast<std::uint64_t>(n)) +
           ",\"v\":\"caf\\u00e9 " + std::to_string(n) + "\"}";
}

class HpFeed {
public:
    using Clock = std::chrono::steady_clock;

    HpFeed(std::int64_t total, int per_second) : total_(total), per_second_(per_second) {
        svr_.Get("/feed", [this](const httplib::Request& req, httplib::Response& res) {
            std::int64_t since = 0;
            if (req.has_param("since")) {
                since = std::stoll(req.get_param_value("since"));
            }
            const auto upto = std::min(released_.load(), since + 100);
            std::string items;
            for (std::int64_t n = since + 1; n <= upto; ++n) {
                items += (items.empty() ? "" : ",") + hp_record(n);
            }
            {
                const std::lock_guard lk(mu_);
                asked_.emplace_back(Clock::now(), since);
                served_max_ = std::max(served_max_, upto);
            }
            res.set_content("{\"items\":[" + items + "]}", "application/json");
        });
        port_ = svr_.bind_to_any_port("127.0.0.1");
        thread_ = std::thread([this] { svr_.listen_after_bind(); });
        while (!svr_.is_running()) {
            std::this_thread::sleep_for(2ms);
        }
    }
    ~HpFeed() {
        stop_ = true;
        if (releaser_.joinable()) {
            releaser_.join();
        }
        svr_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    HpFeed(const HpFeed&) = delete;
    HpFeed& operator=(const HpFeed&) = delete;

    // Releases every record at once, for a bounded read.
    void release_all() { released_ = total_; }
    // Releases the records at per_second, from a thread.
    void start() {
        releaser_ = std::thread([this] {
            const auto begin = Clock::now();
            while (!stop_ && released_.load() < total_) {
                const auto due = static_cast<std::int64_t>(
                    std::chrono::duration<double>(Clock::now() - begin).count() * per_second_);
                released_ = std::min(total_, std::max<std::int64_t>(due, 1));
                std::this_thread::sleep_for(20ms);
            }
        });
    }

    [[nodiscard]] std::string url() const { return "http://127.0.0.1:" + std::to_string(port_); }
    [[nodiscard]] std::int64_t total() const { return total_; }
    std::int64_t served_max() {
        const std::lock_guard lk(mu_);
        return served_max_;
    }
    // The smallest cursor asked for at or after `since`.
    std::int64_t lowest_since_after(Clock::time_point since) {
        const std::lock_guard lk(mu_);
        std::int64_t low = -1;
        for (const auto& [at, s] : asked_) {
            if (at >= since && (low < 0 || s < low)) {
                low = s;
            }
        }
        return low;
    }

private:
    httplib::Server svr_;
    int port_{0};
    std::thread thread_;
    std::thread releaser_;
    std::atomic<bool> stop_{false};
    const std::int64_t total_;
    const int per_second_;
    std::atomic<std::int64_t> released_{0};
    std::mutex mu_;
    std::vector<std::pair<Clock::time_point, std::int64_t>> asked_;
    std::int64_t served_max_{0};
};

// ---- the committed output -------------------------------------------------------------

// Every committed line, in commit order: files by checkpoint id, lines in order.
std::vector<std::string> hp_committed(const std::filesystem::path& dir) {
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

struct HpVerdict {
    std::size_t committed{0};
    std::size_t missing{0};
    std::size_t duplicates{0};
    std::size_t unexpected{0};  // not a record of the feed, or not as it served it
    bool in_order{true};
};

// Lines compared byte for byte with the feed's records (the C++ cells), or as
// rows whose seq, id and v are the record's (the SQL cell).
HpVerdict hp_judge(const std::vector<std::string>& lines, std::int64_t total, bool as_rows) {
    HpVerdict v;
    v.committed = lines.size();
    std::map<std::int64_t, int> seen;
    std::int64_t previous = 0;
    for (const auto& line : lines) {
        std::int64_t seq = -1;
        bool exact = false;
        try {
            const auto obj = clink::config::parse(line);
            seq = obj.as_object().at("seq").as_int();
            if (seq >= 1 && seq <= total) {
                if (as_rows) {
                    exact =
                        obj.as_object().at("id").as_string() == "k" + std::to_string(seq) &&
                        obj.as_object().at("v").as_string() == "caf\xC3\xA9 " + std::to_string(seq);
                } else {
                    exact = line == hp_record(seq);
                }
            }
        } catch (...) {
            exact = false;
        }
        if (!exact) {
            ++v.unexpected;
            continue;
        }
        if (++seen[seq] > 1) {
            ++v.duplicates;
        }
        if (seq < previous) {
            v.in_order = false;
        }
        previous = seq;
    }
    for (std::int64_t n = 1; n <= total; ++n) {
        if (seen.find(n) == seen.end()) {
            ++v.missing;
        }
    }
    return v;
}

// ---- the cells ------------------------------------------------------------------------

class HttpPollRecovery : public ::testing::Test {
protected:
    void SetUp() override {
        for (const char* b : {"node", "submit_sql"}) {
            if (!std::filesystem::exists(hp_binary(b))) {
                GTEST_SKIP() << "the " << b << " binary is not built";
            }
        }
        root_ = std::filesystem::temp_directory_path() /
                ("clink-http-poll-recovery-" + std::to_string(::getpid()) + "-" +
                 ::testing::UnitTest::GetInstance()->current_test_info()->name());
        std::filesystem::remove_all(root_);
        std::filesystem::create_directories(root_);
        out_ = root_ / "out";
        feed_ = std::make_unique<HpFeed>(2400, 600);
    }
    void TearDown() override {
        feed_.reset();
        if (!HasFailure()) {
            std::error_code ec;
            std::filesystem::remove_all(root_, ec);
        }
    }

    clink::cluster::JobGraphSpec graph(std::uint32_t parallelism) const {
        clink::cluster::JobGraphSpec g;
        clink::cluster::OperatorSpec src;
        src.type = "http_poll_source";
        src.id = "src";
        src.uid = "http-poll-recovery-src";
        src.parallelism = parallelism;
        src.out_channel = std::string{clink::cluster::kChannelString};
        src.params = {{"url", feed_->url()},
                      {"path", "/feed"},
                      {"records_field", "items"},
                      {"cursor_param", "since"},
                      {"cursor_field", "seq"},
                      {"cursor_type", "integer"},
                      {"poll_interval_ms", "20"}};
        g.ops.push_back(src);
        clink::cluster::OperatorSpec snk;
        snk.type = "file_2pc_sink_string";
        snk.id = "snk";
        snk.uid = "http-poll-recovery-snk";
        snk.inputs = {"src"};
        snk.parallelism = parallelism;
        snk.out_channel = std::string{clink::cluster::kChannelString};
        snk.params = {{"dir", out_.string()}, {"delivery_guarantee", "exactly_once"}};
        g.ops.push_back(snk);
        return g;
    }

    std::string sql_job() const {
        return "CREATE TABLE feed (seq BIGINT, id STRING, v STRING) WITH (connector='http_poll', "
               "format='json', url='" +
               feed_->url() +
               "', path='/feed', records_field='items', cursor_param='since', "
               "cursor_field='seq', cursor_type='integer', poll_interval_ms='20');\n"
               "CREATE TABLE out (seq BIGINT, id STRING, v STRING) WITH (connector='filesystem', "
               "path='" +
               out_.string() +
               "', format='json', delivery_guarantee='exactly_once');\n"
               "INSERT INTO out SELECT seq, id, v FROM feed;\n";
    }

    std::unique_ptr<Cluster> make_cluster(const std::string& fault) {
        ClusterSpec spec;
        spec.node_binary = hp_binary("node");
        spec.workers = 2;
        spec.slots_per_worker = 8;
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

    void submit_graph(Cluster& cluster, std::uint32_t parallelism) {
        clink::http_connector::HttpRequest::Options o;
        o.base_url = "http://127.0.0.1:" + std::to_string(cluster.http_port());
        clink::http_connector::HttpRequest http(std::move(o));
        const auto r = http.post("/api/v1/jobs/spec?name=http-poll-recovery&checkpoint_dir=" +
                                     cluster.checkpoint_dir().string() +
                                     "&checkpoint_interval_ms=500&alignment=aligned"
                                     "&max_restarts_on_worker_loss=8",
                                 graph(parallelism).to_json(),
                                 "application/json");
        ASSERT_NE(r.body.find("\"ok\":true"), std::string::npos)
            << "the job was not accepted: " << r.body;
    }

    // `sql` compiled and submitted by clink_submit_sql, as a deployment does.
    // Returns its exit code and log.
    std::pair<int, std::string> submit_sql(Cluster& cluster, const std::string& sql, int p) {
        clink::itest::Process submit;
        if (!submit.spawn("submit-sql",
                          hp_binary("submit_sql"),
                          {hp_binary("submit_sql").string(),
                           "-e",
                           sql,
                           "--coordinator-host",
                           "127.0.0.1",
                           "--coordinator-port",
                           std::to_string(cluster.http_port()),
                           "--name",
                           "http-poll-sql",
                           "--checkpoint-dir",
                           cluster.checkpoint_dir().string(),
                           "--checkpoint-interval-ms",
                           "500",
                           "--alignment",
                           "aligned",
                           "--max-restarts-on-worker-loss",
                           "8",
                           "--parallelism",
                           std::to_string(p)},
                          cluster.log_dir())) {
            return {-1, "clink_submit_sql did not start"};
        }
        const auto code = submit.await_exit(60s);
        return {code.value_or(-1), submit.read_log()};
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

    void keep_logs(const char* cell, Cluster& cluster) const {
        const auto into = hp_env("CLINK_HTTP_POLL_RECOVERY_LOG_DIR");
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
        std::ofstream(dir / "killed-worker-0.log") << killed_log_;
    }

    // Parks worker 0 after the source's `page`-th emitted page, kills it, and
    // starts it again; then judges the committed output.
    void run_cell(const char* cell, std::uint32_t parallelism, int page, bool sql) {
        const std::string point = "http_poll_source.after_emit";
        auto cluster = make_cluster(point + "=block@" + std::to_string(page));
        feed_->start();
        if (sql) {
            const auto [code, log] = submit_sql(*cluster, sql_job(), static_cast<int>(parallelism));
            ASSERT_EQ(code, 0) << log;
        } else {
            submit_graph(*cluster, parallelism);
        }
        ASSERT_TRUE(cluster->start_worker(1, ProcOptions{}));
        ASSERT_TRUE(cluster->await_workers_registered(2));
        const std::string witness = "[fault.injection] fired: " + point;
        ASSERT_TRUE(
            clink::itest::await([&] { return cluster->worker(0).log_contains(witness); }, 120s))
            << "worker 0 never reached " << point << "\n"
            << cluster->worker(0).read_log();
        // In flight: the parked source emitted a page no checkpoint can cover
        // while it is parked, so the feed has served past what is committed.
        const auto served_at_kill = feed_->served_max();
        const auto committed_at_kill = hp_committed(out_).size();
        const auto checkpoint_at_kill = latest_completed(*cluster);
        const auto killed_at = std::chrono::steady_clock::now();
        cluster->worker(0).kill_hard();
        ASSERT_TRUE(cluster->worker(0).await_exit(10s).has_value());
        killed_log_ = cluster->worker(0).read_log();
        ASSERT_TRUE(cluster->restart_worker(0, ProcOptions{}));
        const auto total = feed_->total();
        const bool all = clink::itest::await(
            [&] { return hp_judge(hp_committed(out_), total, sql).missing == 0; }, 240s);
        EXPECT_TRUE(all) << "records were still not committed";
        const auto after = latest_completed(*cluster);
        EXPECT_TRUE(
            clink::itest::await([&] { return latest_completed(*cluster) >= after + 2; }, 60s))
            << "checkpoints stopped completing";
        const auto v = hp_judge(hp_committed(out_), total, sql);
        const auto replayed_from = feed_->lowest_since_after(killed_at);
        const auto restarts =
            cluster->count_in_coordinator_log("[coordinator.restart] [info] job_id=1 attempt=");
        EXPECT_EQ(v.missing, 0U);
        EXPECT_EQ(v.duplicates, 0U);
        EXPECT_EQ(v.unexpected, 0U);
        if (parallelism == 1 && !sql) {
            EXPECT_TRUE(v.in_order) << "records were committed out of order";
        }
        EXPECT_GE(restarts, 1U) << "the kill did not restart the job";
        EXPECT_NE(killed_log_.find(witness), std::string::npos);
        EXPECT_GE(checkpoint_at_kill, 1U) << "no checkpoint had completed before the kill";
        EXPECT_GT(served_at_kill, static_cast<std::int64_t>(committed_at_kill))
            << "nothing was in flight at the kill";
        EXPECT_GE(replayed_from, 0) << "the feed saw no request after the kill";
        EXPECT_LT(replayed_from, served_at_kill)
            << "the restored source did not ask again from before the page it had emitted";
        std::printf(
            "[http-poll-recovery] cell=%s total=%lld committed=%zu missing=%zu duplicates=%zu "
            "unexpected=%zu restarts=%zu served_at_kill=%lld committed_at_kill=%zu "
            "checkpoint_at_kill=%llu replayed_from=%lld\n",
            cell,
            static_cast<long long>(total),
            v.committed,
            v.missing,
            v.duplicates,
            v.unexpected,
            restarts,
            static_cast<long long>(served_at_kill),
            committed_at_kill,
            static_cast<unsigned long long>(checkpoint_at_kill),
            static_cast<long long>(replayed_from));
        std::fflush(stdout);
        keep_logs(cell, *cluster);
    }

    std::filesystem::path root_;
    std::filesystem::path out_;
    std::unique_ptr<HpFeed> feed_;
    std::string killed_log_;
};

TEST_F(HttpPollRecovery, AWorkerKilledWithAPageInFlightCommitsEveryRecordOnce) {
    run_cell("p1_kill_after_emit", 1, 60, /*sql=*/false);
}

TEST_F(HttpPollRecovery, TwoSubtasksKilledWithAPageInFlightCommitEveryRecordOnce) {
    run_cell("p2_kill_after_emit", 2, 80, /*sql=*/false);
}

TEST_F(HttpPollRecovery, ASqlJobKilledWithAPageInFlightCommitsEveryRowOnce) {
    run_cell("sql_kill_after_emit", 2, 70, /*sql=*/true);
}

// ---- the SQL paths --------------------------------------------------------------------

class HttpPollSql : public HttpPollRecovery {};

TEST_F(HttpPollSql, AnEmbeddedRunReadsTheFeedToItsEndAndFinishes) {
    if (!std::filesystem::exists(hp_binary("cli"))) {
        GTEST_SKIP() << "the clink binary is not built";
    }
    feed_->release_all();
    const auto out_file = root_ / "out.jsonl";
    const auto script = root_ / "pipeline.sql";
    std::ofstream(script)
        << "CREATE TABLE feed (seq BIGINT, id STRING, v STRING) WITH (connector='http_poll', "
           "format='json', url='"
        << feed_->url()
        << "', path='/feed', records_field='items', cursor_param='since', cursor_field='seq', "
           "cursor_type='integer', bounded='true', poll_interval_ms='0');\n"
           "CREATE TABLE out (seq BIGINT, id STRING, v STRING) WITH (connector='filesystem', "
           "path='"
        << out_file.string()
        << "', format='json');\n"
           "INSERT INTO out SELECT seq, id, v FROM feed;\n";
    clink::itest::Process run;
    ASSERT_TRUE(run.spawn(
        "clink-run", hp_binary("cli"), {hp_binary("cli").string(), "run", script.string()}, root_));
    const auto code = run.await_exit(120s);
    ASSERT_TRUE(code.has_value()) << "the bounded run did not finish\n" << run.read_log();
    ASSERT_EQ(*code, 0) << run.read_log();
    std::vector<std::string> lines;
    std::ifstream in(out_file);
    for (std::string line; std::getline(in, line);) {
        if (!line.empty()) {
            lines.push_back(line);
        }
    }
    const auto v = hp_judge(lines, feed_->total(), /*as_rows=*/true);
    EXPECT_EQ(v.committed, static_cast<std::size_t>(feed_->total()));
    EXPECT_EQ(v.missing, 0U);
    EXPECT_EQ(v.duplicates, 0U);
    EXPECT_EQ(v.unexpected, 0U);
    std::printf("[http-poll-sql] cell=embedded_bounded total=%lld written=%zu missing=%zu\n",
                static_cast<long long>(feed_->total()),
                v.committed,
                v.missing);
    std::fflush(stdout);
}

TEST_F(HttpPollSql, TheCoordinatorRefusesAGuaranteeResourceModeCannotGive) {
    auto cluster = make_cluster("");
    feed_->release_all();
    const auto resource =
        "CREATE TABLE feed (seq BIGINT, id STRING, v STRING) WITH (connector='http_poll', "
        "format='json', url='" +
        feed_->url() +
        "', path='/feed', records_field='items');\n"
        "CREATE TABLE out (seq BIGINT, id STRING, v STRING) WITH (connector='filesystem', "
        "path='" +
        (root_ / "refused.jsonl").string() +
        "', format='json', delivery_guarantee='at_least_once');\n"
        "INSERT INTO out SELECT seq, id, v FROM feed;\n";
    const auto [code, log] = submit_sql(*cluster, resource, 1);
    EXPECT_NE(code, 0) << log;
    EXPECT_NE(log.find("cannot replay from a checkpointed position"), std::string::npos) << log;
    EXPECT_NE(log.find("only with option 'cursor_field'"), std::string::npos) << log;
    std::printf("[http-poll-sql] cell=resource_mode_refused exit=%d\n", code);
    std::fflush(stdout);
}

}  // namespace
