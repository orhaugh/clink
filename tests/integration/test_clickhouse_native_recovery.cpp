// The native ClickHouse sink's kill matrix: a coordinator and two clink_node
// workers run a SQL job from a replayable Kafka topic (event ids 1 to N) into
// a real ClickHouse server, and each cell faults one layer of that path while
// rows flow: the worker process at the sink's own fault points, the network
// through an in-test TCP proxy, and the server itself through Docker.
//
// Every cell runs on both supported lines and against two targets on the same
// two-replica profile: a ReplicatedMergeTree, which keeps a deduplication log
// and replicates to the second replica, and a plain MergeTree, which keeps
// none. The sink always writes to the first replica, through the proxy.
//
// The pass criteria, in every cell:
//   * no loss: uniqExact(id) equals the number of events produced, and every
//     id in the table lies in 1..N;
//   * duplicates only where the sink allows them. A row is duplicated either
//     by a replay after a restart, which re-emits a contiguous run of each
//     source partition from the restored offset, or by a resend of an INSERT
//     the sink was in doubt about and counted in rows_maybe_duplicated. So a
//     cell that spends no restart has at most that many duplicates, and a
//     cell that spends restarts has, per partition, no more runs of
//     duplicated ids than replays and in-doubt INSERTs can make;
//   * the restarts the cell names, counted in the coordinator's log.
//
// Checkpoints are aligned throughout: the sink refuses unaligned and adaptive
// modes, so they have no cell here.
//
// Needs Docker, and the clink_node and clink_submit_sql binaries of a build
// with fault injection compiled in. Each cell prints one "[kill-matrix]" line
// with what it measured.

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <regex>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/connectors/kafka_message.hpp"
#include "clink/connectors/kafka_sink.hpp"

#include "tests/integration/cluster_harness.hpp"
#include "tests/integration/docker_clickhouse.hpp"
#include "tests/integration/docker_kafka.hpp"
#include "tests/integration/tcp_fault_proxy.hpp"

namespace {

using namespace std::chrono_literals;
using clink::itest::Cluster;
using clink::itest::ClusterSpec;
using clink::itest::Process;
using clink::itest::ProcOptions;
using clink::itest::ScopedDiagnostics;
using clink::itest::TcpFaultProxy;
using clink::test::DockerClickHouse;
using clink::test::DockerKafka;
using Clock = std::chrono::steady_clock;

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

enum class Target { Replicated, MergeTree };

struct Cell {
    int cell;
    std::string line;
    Target target;
};

std::string target_name(Target t) {
    return t == Target::Replicated ? "ReplicatedMergeTree" : "MergeTree";
}

void PrintTo(const Cell& c, std::ostream* os) {
    *os << "cell " << c.cell << " on " << c.line << " into " << target_name(c.target);
}

std::string cell_name(const ::testing::TestParamInfo<Cell>& info) {
    std::string line = info.param.line;
    std::replace(line.begin(), line.end(), '.', '_');
    return "Cell" + std::to_string(info.param.cell) + "_Line" + line + "_" +
           target_name(info.param.target);
}

std::vector<Cell> all_cells() {
    std::vector<Cell> out;
    for (const std::string line : {"26.3", "26.8"}) {
        for (int cell = 1; cell <= 9; ++cell) {
            for (const Target t : {Target::Replicated, Target::MergeTree}) {
                out.push_back({cell, line, t});
            }
        }
    }
    return out;
}

std::size_t count_of(std::string_view text, std::string_view needle) {
    std::size_t n = 0;
    for (std::size_t pos = text.find(needle); pos != std::string_view::npos;
         pos = text.find(needle, pos + needle.size())) {
        ++n;
    }
    return n;
}

std::string run_capture(const std::string& cmd) {
    std::string out;
    FILE* pipe = ::popen(cmd.c_str(), "r");
    if (pipe == nullptr) {
        return out;
    }
    std::array<char, 4096> buf{};
    std::size_t n = 0;
    while ((n = std::fread(buf.data(), 1, buf.size(), pipe)) > 0) {
        out.append(buf.data(), n);
    }
    ::pclose(pipe);
    return out;
}

// A payload that does not compress, so a block's size on the wire tracks its
// row count and a block is never small enough to hide in a query packet.
std::string payload_for(std::int64_t id) {
    std::uint64_t x = static_cast<std::uint64_t>(id) * 0x9E3779B97F4A7C15ULL;
    std::string out;
    for (int round = 0; round < 2; ++round) {
        x ^= x >> 30;
        x *= 0xBF58476D1CE4E5B9ULL;
        x ^= x >> 27;
        x *= 0x94D049BB133111EBULL;
        x ^= x >> 31;
        char hex[17];
        std::snprintf(hex, sizeof(hex), "%016llx", static_cast<unsigned long long>(x));
        out += hex;
    }
    return out;
}

std::string event_json(std::int64_t id) {
    return "{\"id\":" + std::to_string(id) + ",\"p\":\"" + payload_for(id) + "\"}";
}

// Produces events with ids 1, 2, 3, ... to a two-partition topic, id % 2
// choosing the partition, at a steady rate until it reaches its limit or is
// stopped. Within a partition the ids are in offset order, which is what lets
// a replay's duplicates be recognised as runs. The producer is idempotent, so
// a batch it retries is not written twice.
class Feeder {
public:
    Feeder(std::string brokers, std::string topic, std::int64_t limit, int per_second)
        : brokers_(std::move(brokers)),
          topic_(std::move(topic)),
          limit_(limit),
          per_tick_(std::max(per_second / 10, 1)) {}

    ~Feeder() { stop(); }

    void start() {
        thread_ = std::thread([this] { run(); });
    }

    // Stop producing and return the number of events produced.
    std::int64_t stop() {
        stop_.store(true);
        if (thread_.joinable()) {
            thread_.join();
        }
        return produced_.load();
    }

    [[nodiscard]] std::int64_t produced() const { return produced_.load(); }
    [[nodiscard]] bool done() const { return done_.load(); }
    [[nodiscard]] const std::string& error() const { return error_; }

private:
    void run() {
        try {
            clink::KafkaSink::Options opts;
            opts.brokers = brokers_;
            opts.topic = topic_;
            opts.metric_prefix.clear();
            opts.acks = "all";
            opts.conf["enable.idempotence"] = "true";
            clink::KafkaSink sink(std::move(opts));
            sink.open();
            std::int64_t next = 1;
            while (!stop_.load() && next <= limit_) {
                clink::Batch<clink::KafkaMessage> batch;
                for (int i = 0; i < per_tick_ && next <= limit_; ++i, ++next) {
                    clink::KafkaMessage message{event_json(next)};
                    message.partition = static_cast<std::int32_t>(next % 2);
                    batch.emplace(std::move(message));
                }
                sink.on_data(batch);
                sink.flush();
                produced_.store(next - 1);
                std::this_thread::sleep_for(100ms);
            }
            sink.close();
        } catch (const std::exception& e) {
            error_ = e.what();
        }
        done_.store(true);
    }

    std::string brokers_;
    std::string topic_;
    std::int64_t limit_;
    int per_tick_;
    std::thread thread_;
    std::atomic<bool> stop_{false};
    std::atomic<bool> done_{false};
    std::atomic<std::int64_t> produced_{0};
    std::string error_;
};

// What the sink reported about itself in its summaries and retry lines.
struct SinkReport {
    std::size_t summaries{0};
    std::uint64_t in_doubt{0};
    std::uint64_t rows_maybe_duplicated{0};
    std::uint64_t rows_resent_with_token{0};
    std::uint64_t abandoned_rows{0};
    std::map<std::string, std::size_t> failures_by_phase;  // retry lines
    std::size_t in_doubt_retries{0};
    [[nodiscard]] std::size_t failed_attempts() const {
        std::size_t n = 0;
        for (const auto& [phase, count] : failures_by_phase) {
            n += count;
        }
        return n;
    }
    [[nodiscard]] std::size_t failed_at(const std::string& phase) const {
        const auto it = failures_by_phase.find(phase);
        return it == failures_by_phase.end() ? 0 : it->second;
    }
};

std::uint64_t field(const std::string& line, const std::string& key) {
    const auto pos = line.find(" " + key + "=");
    if (pos == std::string::npos) {
        return 0;
    }
    try {
        return std::stoull(line.substr(pos + key.size() + 2));
    } catch (const std::exception&) {
        return 0;
    }
}

SinkReport parse_sink_report(const std::vector<std::string>& logs) {
    SinkReport r;
    static const std::regex retry(R"(attempt [0-9]+ failed at ([a-z]+) \(([a-z_]+)\))");
    for (const auto& log : logs) {
        std::istringstream in(log);
        std::string line;
        while (std::getline(in, line)) {
            if (line.find("clickhouse native sink cancelled:") != std::string::npos ||
                line.find("clickhouse native sink closed:") != std::string::npos) {
                ++r.summaries;
                r.in_doubt += field(line, "in_doubt");
                r.rows_maybe_duplicated += field(line, "rows_maybe_duplicated");
                r.rows_resent_with_token += field(line, "rows_resent_with_token");
                r.abandoned_rows += field(line, "abandoned_rows");
            }
            std::smatch m;
            if (std::regex_search(line, m, retry)) {
                ++r.failures_by_phase[m[1].str()];
                if (m[2].str() == "in_doubt") {
                    ++r.in_doubt_retries;
                }
            }
        }
    }
    return r;
}

// What landed.
struct Landed {
    std::int64_t count{0};
    std::int64_t uniq{0};
    std::int64_t out_of_range{0};
    std::int64_t max_copies{0};
    std::vector<std::int64_t> duplicated;  // ids with more than one row, ascending
    [[nodiscard]] std::int64_t duplicates() const { return count - uniq; }
};

// Runs of duplicated ids per partition: ids p, p+2, p+4, ... are consecutive
// offsets of partition p, so a replay or a resent INSERT makes one run.
std::size_t max_runs_per_partition(const std::vector<std::int64_t>& duplicated) {
    std::size_t worst = 0;
    for (int p = 0; p < 2; ++p) {
        std::size_t runs = 0;
        std::optional<std::int64_t> prev;
        for (const auto id : duplicated) {
            if (id % 2 != p) {
                continue;
            }
            if (!prev || id != *prev + 2) {
                ++runs;
            }
            prev = id;
        }
        worst = std::max(worst, runs);
    }
    return worst;
}

class ClickHouseNativeRecovery : public ::testing::TestWithParam<Cell> {
protected:
    static void TearDownTestSuite() {
        profile_.reset();
        kafka_.reset();
    }

    void SetUp() override {
        if (!DockerClickHouse::docker_available()) {
            GTEST_SKIP() << "Docker not available; skipping the native ClickHouse kill matrix";
        }
        if (!std::filesystem::exists(node_binary()) || !std::filesystem::exists(sql_binary())) {
            GTEST_SKIP() << "cluster node or SQL submit binary is not built";
        }
        const Cell& c = GetParam();
        // One profile per line, kept across that line's cells and replaced
        // when the line changes, so two lines never run at once.
        if (profile_ == nullptr || profile_->line() != c.line) {
            profile_.reset();
            profile_ = std::make_unique<DockerClickHouse>(
                clink::test::DockerClickHouseOptions{.line = c.line, .replicas = 2});
        }
        if (kafka_ == nullptr) {
            kafka_ = std::make_unique<DockerKafka>();
        }
        static int counter = 0;
        const std::string suffix = std::to_string(::getpid()) + "_" + std::to_string(++counter);
        table_ = "km_c" + std::to_string(c.cell) +
                 (c.target == Target::Replicated ? "_rmt_" : "_mt_") + suffix;
        topic_ = "clink_ch_kill_matrix_" + suffix;
        create_table();
        kafka_->create_topic(topic_, 2);
    }

    void TearDown() override {
        if (profile_ == nullptr || table_.empty()) {
            return;
        }
        // A server a cell stopped is started again, so the next cell finds
        // the profile whole.
        for (int i = 0; i < profile_->replicas(); ++i) {
            if (!profile_->ready(i)) {
                try {
                    profile_->start(i);
                } catch (const std::exception&) {
                    profile_.reset();
                    return;
                }
            }
            (void)profile_->try_query("DROP TABLE IF EXISTS default." + table_ + " SYNC", i);
        }
    }

    void create_table() {
        const Cell& c = GetParam();
        if (c.target == Target::MergeTree) {
            profile_->query("CREATE TABLE default." + table_ +
                            " (id Int64, p String) ENGINE = MergeTree ORDER BY id");
            return;
        }
        for (int i = 0; i < 2; ++i) {
            profile_->query("CREATE TABLE default." + table_ +
                                " (id Int64, p String) ENGINE = ReplicatedMergeTree("
                                "'/clickhouse/tables/kill_matrix/" +
                                table_ + "', '{replica}') ORDER BY id SETTINGS async_insert = 0",
                            i);
        }
    }

    // --- the job ------------------------------------------------------------

    struct JobShape {
        int parallelism{2};
        std::int64_t checkpoint_interval_ms{1000};
        std::string retry_window_ms{"60000"};
        std::string batch_interval_ms{"500"};
        std::string receive_timeout_ms{"10000"};
        std::string send_timeout_ms{"10000"};
        // Empty: the Kafka topic. Otherwise a bounded JSON file source.
        std::filesystem::path file;
    };

    std::string job_sql(const JobShape& shape, std::uint16_t port) const {
        std::string src;
        if (shape.file.empty()) {
            src =
                "CREATE TABLE src (id BIGINT, p VARCHAR) WITH (connector='kafka', format='json', "
                "brokers='" +
                kafka_->brokers() + "', topic='" + topic_ + "', group_id='" + topic_ +
                "', auto_offset_reset='earliest'); ";
        } else {
            src =
                "CREATE TABLE src (id BIGINT, p VARCHAR) WITH (connector='file', format='json', "
                "path='" +
                shape.file.string() + "'); ";
        }
        return src +
               "CREATE TABLE ch (id BIGINT, p VARCHAR) WITH (connector='clickhouse', "
               "insert_format='native', host='127.0.0.1', port='" +
               std::to_string(port) + "', database='default', table='" + table_ +
               "', retry_window_ms='" + shape.retry_window_ms + "', batch_interval_ms='" +
               shape.batch_interval_ms + "', connect_timeout_ms='2000', receive_timeout_ms='" +
               shape.receive_timeout_ms + "', send_timeout_ms='" + shape.send_timeout_ms +
               "'); INSERT INTO ch SELECT id, p FROM src;";
    }

    void submit(Cluster& cluster, const JobShape& shape, std::uint16_t port) {
        Process submit;
        ASSERT_TRUE(submit.spawn("submit-sql",
                                 sql_binary(),
                                 {sql_binary().string(),
                                  "-e",
                                  job_sql(shape, port),
                                  "--coordinator-host",
                                  "127.0.0.1",
                                  "--coordinator-port",
                                  std::to_string(cluster.http_port()),
                                  "--name",
                                  "clickhouse-kill-matrix",
                                  "--checkpoint-dir",
                                  cluster.checkpoint_dir().string(),
                                  "--checkpoint-interval-ms",
                                  std::to_string(shape.checkpoint_interval_ms),
                                  "--alignment",
                                  "aligned",
                                  "--max-restarts-on-worker-loss",
                                  "8",
                                  "--parallelism",
                                  std::to_string(shape.parallelism)},
                                 cluster.log_dir()));
        const auto code = submit.await_exit(60s);
        ASSERT_TRUE(code.has_value());
        ASSERT_EQ(*code, 0) << submit.read_log();
    }

    // Every log a worker wrote in this cell, including those of processes a
    // cell killed and replaced, whose files the replacement truncates.
    std::vector<std::string> worker_logs(Cluster& cluster) const {
        std::vector<std::string> out = archived_logs_;
        for (std::size_t i = 0; i < 2; ++i) {
            out.push_back(cluster.worker(i).read_log());
        }
        return out;
    }

    std::size_t count_in_workers(Cluster& cluster, std::string_view needle) const {
        std::size_t n = 0;
        for (const auto& log : worker_logs(cluster)) {
            n += count_of(log, needle);
        }
        return n;
    }

    // SIGKILL worker `idx`, keep its log, and start a clean replacement.
    void kill_and_replace(Cluster& cluster, std::size_t idx, const ProcOptions& opts) {
        cluster.worker(idx).kill_hard();
        ASSERT_TRUE(cluster.worker(idx).await_exit(10s).has_value());
        archived_logs_.push_back(cluster.worker(idx).read_log());
        ASSERT_TRUE(cluster.restart_worker(idx, opts));
    }

    // Restarts the coordinator spent on the job, from its own log: every
    // restart, whether a worker was lost or a subtask failed, ends in one
    // redeploy line.
    static std::size_t restarts(const Cluster& cluster) {
        return cluster.count_in_coordinator_log("[coordinator.restart] [info] job_id=1 attempt=");
    }

    bool cancel_job(const Cluster& cluster) const {
        const std::string out =
            run_capture("curl -s -X POST http://127.0.0.1:" + std::to_string(cluster.http_port()) +
                        "/api/v1/jobs/1/cancel 2>&1");
        return out.find("\"ok\":true") != std::string::npos;
    }

    Landed landed(int replica = 0) const {
        Landed l;
        const std::string t = "default." + table_;
        std::istringstream row(
            profile_->scalar("SELECT count(), uniqExact(id), countIf(id < 1 OR id > " +
                                 std::to_string(expected_) + ") FROM " + t,
                             replica));
        row >> l.count >> l.uniq >> l.out_of_range;
        l.max_copies = std::stoll(profile_->scalar(
            "SELECT max(c) FROM (SELECT count() AS c FROM " + t + " GROUP BY id)", replica));
        std::istringstream in(profile_->query(
            "SELECT id FROM " + t + " GROUP BY id HAVING count() > 1 ORDER BY id", replica));
        std::string id;
        while (std::getline(in, id)) {
            if (!id.empty()) {
                l.duplicated.push_back(std::stoll(id));
            }
        }
        return l;
    }

    std::int64_t uniq_now() const {
        const auto r = profile_->try_query("SELECT uniqExact(id) FROM default." + table_);
        if (r.status != 0) {
            return -1;
        }
        try {
            return std::stoll(r.out);
        } catch (const std::exception&) {
            return -1;
        }
    }

    // The newest COMPLETED checkpoint marker for the job.
    static std::uint64_t latest_completed(const Cluster& cluster) {
        std::uint64_t latest = 0;
        std::error_code ec;
        const auto dir = cluster.checkpoint_dir() / "_jobs" / "1";
        for (const auto& e : std::filesystem::directory_iterator(dir, ec)) {
            const auto name = e.path().filename().string();
            if (name.rfind("COMPLETED-", 0) == 0) {
                try {
                    latest = std::max<std::uint64_t>(latest, std::stoull(name.substr(10)));
                } catch (const std::exception&) {
                }
            }
        }
        return latest;
    }

    // Wait until every event produced has landed, then let two more
    // checkpoints complete so a late duplicate has the chance to land too: a
    // checkpoint completes only once every INSERT before its barrier is
    // acknowledged, so after two of them nothing sent earlier is still on
    // its way.
    void await_all_landed(Cluster& cluster, std::chrono::seconds timeout) {
        ASSERT_TRUE(clink::itest::await([&] { return uniq_now() >= expected_; }, timeout))
            << "only " << uniq_now() << " of " << expected_ << " ids landed";
        const auto after = latest_completed(cluster);
        ASSERT_TRUE(
            clink::itest::await([&] { return latest_completed(cluster) >= after + 2; }, 60s))
            << "checkpoints stopped completing after every row had landed";
    }

    // Cancel the job and wait for each running subtask's cancelled summary.
    void cancel_and_collect(Cluster& cluster, int parallelism) {
        const std::size_t before = count_in_workers(cluster, "clickhouse native sink cancelled:");
        ASSERT_TRUE(cancel_job(cluster)) << "the cancel was not accepted";
        EXPECT_TRUE(clink::itest::await(
            [&] {
                return count_in_workers(cluster, "clickhouse native sink cancelled:") >=
                       before + static_cast<std::size_t>(parallelism);
            },
            60s))
            << "the sink's cancelled summaries did not appear";
    }

    void report(const Landed& l,
                std::size_t restarts_spent,
                const SinkReport& sink,
                const std::string& extra = "") const {
        const Cell& c = GetParam();
        std::printf(
            "[kill-matrix] cell=%d line=%s target=%s produced=%lld count=%lld uniq=%lld "
            "duplicates=%lld max_copies=%lld dup_runs=%zu restarts=%zu in_doubt=%llu "
            "in_doubt_retries=%zu failed_attempts=%zu rows_maybe_duplicated=%llu "
            "rows_resent_with_token=%llu%s%s\n",
            c.cell,
            c.line.c_str(),
            target_name(c.target).c_str(),
            static_cast<long long>(expected_),
            static_cast<long long>(l.count),
            static_cast<long long>(l.uniq),
            static_cast<long long>(l.duplicates()),
            static_cast<long long>(l.max_copies),
            max_runs_per_partition(l.duplicated),
            restarts_spent,
            static_cast<unsigned long long>(sink.in_doubt),
            sink.in_doubt_retries,
            sink.failed_attempts(),
            static_cast<unsigned long long>(sink.rows_maybe_duplicated),
            static_cast<unsigned long long>(sink.rows_resent_with_token),
            extra.empty() ? "" : " ",
            extra.c_str());
        std::fflush(stdout);
    }

    // No loss, in every cell, and the second replica holds what the first does.
    void expect_no_loss(const Landed& l) const {
        EXPECT_EQ(l.uniq, expected_) << "rows were lost";
        EXPECT_EQ(l.out_of_range, 0) << "ids outside 1.." << expected_ << " landed";
        if (GetParam().target == Target::Replicated) {
            (void)profile_->query("SYSTEM SYNC REPLICA default." + table_, 1);
            EXPECT_EQ(profile_->scalar("SELECT count() FROM default." + table_, 1),
                      std::to_string(l.count))
                << "the second replica does not hold what the first acknowledged";
        }
    }

    // A cell that spends no restart may duplicate only what the sink counted.
    static void expect_duplicates_counted(const Landed& l, const SinkReport& sink) {
        EXPECT_LE(static_cast<std::uint64_t>(l.duplicates()), sink.rows_maybe_duplicated)
            << "more duplicates than the sink counted as possibly duplicated";
    }

    // A cell that spends restarts: each replay makes at most one run of
    // duplicated ids per partition and one more copy of each, and so does each
    // INSERT resent in doubt.
    static void expect_duplicates_replayed(const Landed& l,
                                           std::size_t replays,
                                           const SinkReport& sink) {
        const std::size_t sources = replays + sink.in_doubt_retries;
        EXPECT_LE(max_runs_per_partition(l.duplicated), sources)
            << "duplicated ids outside the replayed intervals";
        EXPECT_LE(l.max_copies, static_cast<std::int64_t>(1 + sources));
    }

    // Wait for the sinks to be open on every subtask.
    void await_open(Cluster& cluster, int parallelism) {
        ASSERT_TRUE(clink::itest::await(
            [&] {
                return count_in_workers(cluster, "clickhouse native sink open:") >=
                       static_cast<std::size_t>(parallelism);
            },
            90s))
            << "the sink did not open on every subtask";
    }

    std::unique_ptr<Cluster> make_cluster(const ProcOptions& w0, const ProcOptions& w1) {
        ClusterSpec spec;
        spec.node_binary = node_binary();
        spec.workers = 2;
        spec.slots_per_worker = 3;
        spec.http = true;
        auto cluster = std::make_unique<Cluster>(spec);
        // CLINK_KILL_MATRIX_KEEP=1 keeps every cell's logs and checkpoints,
        // passing or not, for reading a run after the fact.
        if (const char* keep = std::getenv("CLINK_KILL_MATRIX_KEEP");
            keep != nullptr && *keep == '1') {
            cluster->keep_artifacts();
            std::printf("[kill-matrix] artifacts at %s\n", cluster->root().c_str());
        }
        if (!cluster->start_coordinator() || !cluster->start_worker(0, w0) ||
            !cluster->start_worker(1, w1) || !cluster->await_workers_registered(2)) {
            ADD_FAILURE() << "the cluster did not start";
        }
        return cluster;
    }

    // --- the cells ------------------------------------------------------------

    // Cells 1 to 3: SIGKILL the worker parked at a sink fault point, replace
    // it, and let the job restart once.
    void kill_at_point(const std::string& point, bool needs_retry);
    void kill_server_during_end_insert();
    void drop_mid_insert();
    void outage(bool longer_than_window);
    void server_restarts();
    void bounded_final_checkpoint_outage();

    static std::unique_ptr<DockerClickHouse> profile_;
    static std::unique_ptr<DockerKafka> kafka_;
    std::string table_;
    std::string topic_;
    std::int64_t expected_{0};
    std::vector<std::string> archived_logs_;
};

std::unique_ptr<DockerClickHouse> ClickHouseNativeRecovery::profile_;
std::unique_ptr<DockerKafka> ClickHouseNativeRecovery::kafka_;

void ClickHouseNativeRecovery::kill_at_point(const std::string& point, bool needs_retry) {
    // The fourth time worker 1 reaches the point it parks there, and prints
    // the fault framework's witness line; the kill lands while it is parked.
    // A retry point is reached only after a failure, so that cell makes some:
    // the proxy cuts INSERT uploads, one at a time, until worker 1 retries.
    const std::string arm = point + "=block@" + (needs_retry ? "1" : "4");
    auto cluster = make_cluster({}, {.fault = arm});
    ScopedDiagnostics diagnostics(*cluster);
    TcpFaultProxy proxy(static_cast<std::uint16_t>(profile_->port(0)));
    JobShape shape;
    submit(*cluster, shape, proxy.port());
    await_open(*cluster, shape.parallelism);

    expected_ = 30'000;
    Feeder feed(kafka_->brokers(), topic_, expected_, 2000);
    feed.start();

    const std::string witness = "[fault.injection] fired: " + point;
    std::size_t cuts = 0;
    ASSERT_TRUE(clink::itest::await(
        [&] {
            if (cluster->worker(1).log_contains(witness)) {
                return true;
            }
            if (needs_retry && proxy.uploads_dropped() == cuts && cuts < 40) {
                proxy.drop_at_upload(1);
                ++cuts;
            }
            return false;
        },
        120s))
        << "worker 1 never reached " << point;
    kill_and_replace(*cluster, 1, {});
    ASSERT_TRUE(cluster->await_workers_registered(3));

    await_all_landed(*cluster, 240s);
    EXPECT_EQ(feed.stop(), expected_) << feed.error();
    const std::size_t spent = restarts(*cluster);
    cancel_and_collect(*cluster, shape.parallelism);
    const Landed l = landed();
    const SinkReport sink = parse_sink_report(worker_logs(*cluster));
    report(l, spent, sink, needs_retry ? "cuts=" + std::to_string(cuts) : "");
    expect_no_loss(l);
    EXPECT_EQ(spent, 1U) << "one worker loss is one restart";
    expect_duplicates_replayed(l, spent, sink);
    if (point == "clickhouse.after_end_insert") {
        // The INSERT was acknowledged and its checkpoint never was, so the
        // replay writes it again: the fault engaged.
        EXPECT_GT(l.duplicates(), 0) << "the acknowledged INSERT was not replayed";
    }
}

void ClickHouseNativeRecovery::kill_server_during_end_insert() {
    // The proxy withholds the server's reply to one INSERT once its data is
    // under way. When the client has sent everything and waits for the end of
    // the INSERT, the server is SIGKILLed, the held connection is reset, and
    // the server is started again within the retry window.
    auto cluster = make_cluster({}, {});
    ScopedDiagnostics diagnostics(*cluster);
    TcpFaultProxy proxy(static_cast<std::uint16_t>(profile_->port(0)));
    JobShape shape;
    submit(*cluster, shape, proxy.port());
    await_open(*cluster, shape.parallelism);

    expected_ = 30'000;
    Feeder feed(kafka_->brokers(), topic_, expected_, 2000);
    feed.start();
    ASSERT_TRUE(clink::itest::await([&] { return uniq_now() > 2000; }, 60s));

    proxy.hold_at_upload(1);
    ASSERT_TRUE(clink::itest::await([&] { return proxy.held_clients_quiet(500ms); }, 60s))
        << "no INSERT reached its end with the reply withheld";
    profile_->kill(0);
    proxy.drop_held();
    std::this_thread::sleep_for(2s);
    profile_->start(0);

    await_all_landed(*cluster, 240s);
    EXPECT_EQ(feed.stop(), expected_) << feed.error();
    const std::size_t spent = restarts(*cluster);
    cancel_and_collect(*cluster, shape.parallelism);
    const Landed l = landed();
    const SinkReport sink = parse_sink_report(worker_logs(*cluster));
    report(l, spent, sink);
    expect_no_loss(l);
    EXPECT_EQ(spent, 0U) << "a server restart inside the window spent a restart";
    EXPECT_GE(sink.in_doubt_retries, 1U) << "the INSERT ended by the kill was not counted in doubt";
    EXPECT_GE(sink.in_doubt, 1U);
    expect_duplicates_counted(l, sink);
}

void ClickHouseNativeRecovery::drop_mid_insert() {
    // Three INSERTs are cut in the middle: the proxy resets the connection at
    // the first bytes of each upload, before the server has seen a row.
    auto cluster = make_cluster({}, {});
    ScopedDiagnostics diagnostics(*cluster);
    TcpFaultProxy proxy(static_cast<std::uint16_t>(profile_->port(0)));
    JobShape shape;
    submit(*cluster, shape, proxy.port());
    await_open(*cluster, shape.parallelism);

    expected_ = 30'000;
    Feeder feed(kafka_->brokers(), topic_, expected_, 2000);
    feed.start();
    for (std::size_t cut = 1; cut <= 3; ++cut) {
        const auto before = uniq_now();
        ASSERT_TRUE(clink::itest::await([&] { return uniq_now() > before + 1000; }, 60s));
        proxy.drop_at_upload(1);
        ASSERT_TRUE(clink::itest::await([&] { return proxy.uploads_dropped() >= cut; }, 60s))
            << "no INSERT upload reached the proxy";
    }

    await_all_landed(*cluster, 240s);
    EXPECT_EQ(feed.stop(), expected_) << feed.error();
    const std::size_t spent = restarts(*cluster);
    cancel_and_collect(*cluster, shape.parallelism);
    const Landed l = landed();
    const SinkReport sink = parse_sink_report(worker_logs(*cluster));
    report(l, spent, sink, "cuts=" + std::to_string(proxy.uploads_dropped()));
    expect_no_loss(l);
    EXPECT_EQ(spent, 0U) << "a dropped connection spent a restart";
    // The server saw none of the cut INSERTs' rows, so nothing can repeat.
    EXPECT_EQ(l.duplicates(), 0);
    std::size_t failures = 0;
    for (const auto& [phase, n] : sink.failures_by_phase) {
        failures += n;
    }
    EXPECT_GE(failures, 3U) << "the cuts did not fail three attempts";
}

void ClickHouseNativeRecovery::outage(bool longer_than_window) {
    // Shorter: the network blackholes for 15 s against a 60 s window, so the
    // sink's calls wait out their 5 s timeouts and retry until it heals.
    // Longer: the proxy refuses every connection against a 30 s window, so
    // the INSERT gives up, the job restarts once, and the restarted sink's
    // open retries until the server is back. The outage ends on that restart
    // and past the window, not on a fixed clock: the sink gives up as soon as
    // its next backoff would cross the window, so a window ends anywhere from
    // one backoff cap (about 10 s) before its deadline to the deadline
    // itself. A fixed outage could then outlive the restarted open's window
    // as well and spend a second restart.
    auto cluster = make_cluster({}, {});
    ScopedDiagnostics diagnostics(*cluster);
    TcpFaultProxy proxy(static_cast<std::uint16_t>(profile_->port(0)));
    JobShape shape;
    shape.retry_window_ms = longer_than_window ? "30000" : "60000";
    shape.receive_timeout_ms = "5000";
    shape.send_timeout_ms = "5000";
    submit(*cluster, shape, proxy.port());
    await_open(*cluster, shape.parallelism);

    expected_ = 40'000;
    Feeder feed(kafka_->brokers(), topic_, expected_, 2000);
    feed.start();
    ASSERT_TRUE(clink::itest::await([&] { return uniq_now() > 4000; }, 60s));

    const auto start = Clock::now();
    if (longer_than_window) {
        proxy.set_refusing(true);
        ASSERT_TRUE(clink::itest::await([&] { return restarts(*cluster) >= 1; }, 60s))
            << "the INSERT did not give up while the server was unreachable";
        std::this_thread::sleep_until(start + 31s);
        proxy.set_refusing(false);
    } else {
        proxy.set_blackhole(true);
        std::this_thread::sleep_for(15s);
        proxy.set_blackhole(false);
    }
    const auto outage_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start).count();

    await_all_landed(*cluster, 300s);
    EXPECT_EQ(feed.stop(), expected_) << feed.error();
    const std::size_t spent = restarts(*cluster);
    cancel_and_collect(*cluster, shape.parallelism);
    const Landed l = landed();
    const SinkReport sink = parse_sink_report(worker_logs(*cluster));
    report(l, spent, sink, "outage_ms=" + std::to_string(outage_ms));
    expect_no_loss(l);
    if (longer_than_window) {
        EXPECT_GT(outage_ms, 30'000) << "the outage was not longer than the window";
        EXPECT_EQ(spent, 1U) << "an outage past the window should spend exactly one restart";
        EXPECT_GE(cluster->count_in_coordinator_log("clickhouse.retry_window_exhausted"), 1U)
            << "the restart was not caused by the exhausted window";
        expect_duplicates_replayed(l, spent, sink);
    } else {
        EXPECT_GE(sink.failed_attempts(), shape.parallelism)
            << "the outage did not reach the sink's calls";
        EXPECT_EQ(spent, 0U) << "an outage inside the window spent a restart";
        EXPECT_EQ(cluster->count_in_coordinator_log("coordinator.restart"), 0U)
            << "the coordinator logged a restart";
        expect_duplicates_counted(l, sink);
    }
}

void ClickHouseNativeRecovery::server_restarts() {
    // Eleven graceful restarts of the server, each waited out by the sink
    // inside its window. After each, both subtasks must have written again
    // before the next. Every new client reruns the server checks, so the
    // settings probe appears in system.query_log once per reconnect.
    auto cluster = make_cluster({}, {});
    ScopedDiagnostics diagnostics(*cluster);
    TcpFaultProxy proxy(static_cast<std::uint16_t>(profile_->port(0)));
    JobShape shape;
    shape.retry_window_ms = "120000";
    submit(*cluster, shape, proxy.port());
    await_open(*cluster, shape.parallelism);

    expected_ = 1'000'000;  // the feed is stopped once the restarts are done
    Feeder feed(kafka_->brokers(), topic_, expected_, 1000);
    feed.start();
    ASSERT_TRUE(clink::itest::await([&] { return uniq_now() > 2000; }, 60s));

    const std::string probe_count =
        "SELECT count() FROM system.query_log WHERE type = 'QueryFinish' AND "
        "query LIKE '%FROM system.settings WHERE name IN%' AND query NOT LIKE '%query_log%'";
    const std::string insert_subtasks_since =
        "SELECT uniqExact(extract(log_comment, ':sub([0-9]+):')) FROM system.query_log WHERE "
        "type = 'QueryFinish' AND query_kind = 'Insert' AND log_comment LIKE 'clink:%' AND "
        "query LIKE '%" +
        table_ + "%' AND event_time_microseconds > toDateTime64('";
    (void)profile_->query("SYSTEM FLUSH LOGS");
    const std::int64_t probes_before = std::stoll(profile_->scalar(probe_count));

    constexpr int kRestarts = 11;
    for (int i = 1; i <= kRestarts; ++i) {
        profile_->restart(0);
        const std::string since = profile_->scalar("SELECT toString(now64(6))");
        ASSERT_TRUE(clink::itest::await(
            [&] {
                (void)profile_->try_query("SYSTEM FLUSH LOGS");
                const auto r = profile_->try_query(insert_subtasks_since + since + "', 6)");
                return r.status == 0 && std::stoll(r.out) >= shape.parallelism;
            },
            120s))
            << "after restart " << i << " the subtasks did not all write again";
    }
    expected_ = feed.stop();
    ASSERT_TRUE(feed.error().empty()) << feed.error();

    await_all_landed(*cluster, 240s);
    const std::size_t spent = restarts(*cluster);
    cancel_and_collect(*cluster, shape.parallelism);
    (void)profile_->query("SYSTEM FLUSH LOGS");
    const std::int64_t probes = std::stoll(profile_->scalar(probe_count)) - probes_before;
    const Landed l = landed();
    const SinkReport sink = parse_sink_report(worker_logs(*cluster));
    // A failed attempt is followed by a new client. One that fails to connect
    // never reaches the checks, and one whose checks fail does not finish
    // them, so the finished probes are the connects that got through them.
    std::size_t failures = 0;
    for (const auto& [phase, n] : sink.failures_by_phase) {
        failures += n;
    }
    const auto at = [&](const std::string& phase) {
        const auto it = sink.failures_by_phase.find(phase);
        return it == sink.failures_by_phase.end() ? std::size_t{0} : it->second;
    };
    const std::int64_t reconnects =
        static_cast<std::int64_t>(failures) - static_cast<std::int64_t>(at("connect"));
    const std::int64_t checked = reconnects - static_cast<std::int64_t>(at("metadata"));
    report(l,
           spent,
           sink,
           "server_restarts=" + std::to_string(kRestarts) +
               " failed_attempts=" + std::to_string(failures) +
               " reconnects=" + std::to_string(reconnects) + " probes=" + std::to_string(probes));
    expect_no_loss(l);
    EXPECT_EQ(spent, 0U) << "server restarts inside the window spent a restart";
    EXPECT_GE(reconnects, static_cast<std::int64_t>(kRestarts) * shape.parallelism)
        << "a subtask came through a restart without a new client";
    EXPECT_EQ(probes, checked) << "a reconnect did not rerun the server checks, or ran them twice";
    expect_duplicates_counted(l, sink);
}

void ClickHouseNativeRecovery::bounded_final_checkpoint_outage() {
    // A bounded file source, one subtask, and no periodic checkpoint within
    // the job's life, so the only barrier is the end of input's final
    // checkpoint, and the only INSERT is its flush. The proxy cuts that INSERT
    // and refuses every connection until the final checkpoint has given up,
    // which takes the 10 s bound the workers are given: past the bound and far
    // inside the 60 s window. The job restarts once, and the second run lands
    // every row. The outage ends on the restart rather than on a clock,
    // because the restarted run's final checkpoint has the same bound: an
    // outage that outlived it too would spend a second restart.
    const ProcOptions env{.env = {{"CLINK_EOS_FINAL_CKPT_TIMEOUT_MS", "10000"}}};
    auto cluster = make_cluster(env, env);
    ScopedDiagnostics diagnostics(*cluster);
    TcpFaultProxy proxy(static_cast<std::uint16_t>(profile_->port(0)));

    expected_ = 20'000;
    JobShape shape;
    shape.parallelism = 1;
    shape.checkpoint_interval_ms = 600'000;
    shape.batch_interval_ms = "3600000";
    shape.file = cluster->root() / "events.ndjson";
    {
        std::ofstream out(shape.file);
        for (std::int64_t id = 1; id <= expected_; ++id) {
            out << event_json(id) << "\n";
        }
    }
    proxy.drop_at_upload(1'000'000);
    submit(*cluster, shape, proxy.port());
    ASSERT_TRUE(clink::itest::await([&] { return proxy.uploads_dropped() >= 1; }, 90s))
        << "the final checkpoint's INSERT never reached the proxy";
    proxy.set_refusing(true);
    const auto outage_start = Clock::now();
    ASSERT_TRUE(clink::itest::await([&] { return restarts(*cluster) >= 1; }, 60s))
        << "the final checkpoint did not give up while the server was unreachable";
    proxy.clear_upload_faults();
    proxy.set_refusing(false);
    const auto outage_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - outage_start).count();

    // The second run ends on its own: its sink closes cleanly once the final
    // checkpoint's INSERT is acknowledged.
    ASSERT_TRUE(clink::itest::await(
        [&] { return count_in_workers(*cluster, "clickhouse native sink closed:") >= 1; }, 180s))
        << "the restarted job did not finish";
    const std::size_t spent = restarts(*cluster);
    const Landed l = landed();
    const SinkReport sink = parse_sink_report(worker_logs(*cluster));
    report(l, spent, sink, "outage_ms=" + std::to_string(outage_ms));
    expect_no_loss(l);
    EXPECT_GT(outage_ms, 10'000) << "the outage was not longer than the final checkpoint's bound";
    EXPECT_EQ(spent, 1U) << "the final checkpoint held past its bound should cost one restart";
    EXPECT_GE(
        count_in_workers(*cluster, "source EOS final checkpoint did not commit within timeout"), 1U)
        << "the restart was not the final checkpoint's bound";
    // The first run's only INSERT was cut before the server saw a row.
    EXPECT_EQ(l.duplicates(), 0);
}

std::string cell_fault(int cell) {
    switch (cell) {
        case 1:
            return "SIGKILL a worker at clickhouse.after_end_insert";
        case 2:
            return "SIGKILL a worker at clickhouse.after_first_block";
        case 3:
            return "SIGKILL a worker at clickhouse.before_retry_wait";
        case 4:
            return "SIGKILL the server during EndInsert";
        case 5:
            return "drop the connection mid-INSERT";
        case 6:
            return "outage shorter than the retry window";
        case 7:
            return "outage longer than the retry window";
        case 8:
            return "eleven server restarts inside the window";
        case 9:
            return "outage at a bounded job's final checkpoint";
        default:
            return "?";
    }
}

TEST_P(ClickHouseNativeRecovery, CellHoldsItsGuarantee) {
    const Cell& c = GetParam();
    SCOPED_TRACE(cell_fault(c.cell));
    switch (c.cell) {
        case 1:
            kill_at_point("clickhouse.after_end_insert", false);
            break;
        case 2:
            kill_at_point("clickhouse.after_first_block", false);
            break;
        case 3:
            kill_at_point("clickhouse.before_retry_wait", true);
            break;
        case 4:
            kill_server_during_end_insert();
            break;
        case 5:
            drop_mid_insert();
            break;
        case 6:
            outage(false);
            break;
        case 7:
            outage(true);
            break;
        case 8:
            server_restarts();
            break;
        case 9:
            bounded_final_checkpoint_outage();
            break;
        default:
            FAIL() << "no cell " << c.cell;
    }
}

INSTANTIATE_TEST_SUITE_P(BothLines,
                         ClickHouseNativeRecovery,
                         ::testing::ValuesIn(all_cells()),
                         cell_name);

}  // namespace
