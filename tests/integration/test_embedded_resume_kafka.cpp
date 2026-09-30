// Crash-resume of an embedded `clink run`: a real Kafka JSON source through an
// event-time window into a transactional Kafka sink, run by the CLI in its own
// process, killed with SIGKILL mid-stream and started again with the same
// script and checkpoint directory. The second process must continue from the
// first one's checkpoints: every window committed exactly once, with its full
// count. Before resume existed the rerun started from nothing, re-read the
// topic from the start and committed every window the first process had
// already committed a second time.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <unistd.h>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/connectors/kafka_message.hpp"
#include "clink/connectors/kafka_sink.hpp"
#include "clink/connectors/kafka_source.hpp"

#include "tests/integration/cluster_harness.hpp"
#include "tests/integration/docker_kafka.hpp"

namespace {

using namespace std::chrono_literals;
using clink::itest::Process;

namespace fs = std::filesystem;

fs::path cli_binary() {
#ifdef CLINK_CLI_BINARY
    return fs::path{CLINK_CLI_BINARY};
#else
    return {};
#endif
}

constexpr int kKeys = 4;
constexpr int kEventsPerKeyWindow = 10;
constexpr std::int64_t kBaseTs = 1'000'000;
constexpr std::int64_t kWindowMs = 10'000;
constexpr int kWindows = 10;

std::uint64_t latest_completed(const fs::path& root) {
    std::uint64_t latest = 0;
    std::error_code ec;
    for (const auto& entry : fs::recursive_directory_iterator(root, ec)) {
        const auto name = entry.path().filename().string();
        if (name.rfind("COMPLETED-", 0) != 0) {
            continue;
        }
        try {
            latest = std::max<std::uint64_t>(latest, std::stoull(name.substr(10)));
        } catch (const std::exception&) {
        }
    }
    return latest;
}

void produce(const std::string& brokers,
             const std::string& topic,
             const std::vector<std::string>& payloads) {
    clink::KafkaSink::Options opts;
    opts.brokers = brokers;
    opts.topic = topic;
    opts.metric_prefix.clear();
    opts.acks = "all";
    // Idempotent, so a retried batch cannot put a duplicate in the input that
    // the oracle would then blame on the engine.
    opts.conf["enable.idempotence"] = "true";
    clink::KafkaSink sink(std::move(opts));
    sink.open();
    clink::Batch<clink::KafkaMessage> batch;
    for (const auto& p : payloads) {
        batch.emplace(clink::KafkaMessage{p});
    }
    sink.on_data(batch);
    sink.flush();
    sink.close();
}

std::vector<std::string> window_events(int first_window, int last_window) {
    std::vector<std::string> out;
    for (int w = first_window; w <= last_window; ++w) {
        for (int i = 0; i < kEventsPerKeyWindow; ++i) {
            for (int k = 0; k < kKeys; ++k) {
                const auto ts = kBaseTs + (w * kWindowMs) + (i * 100) + k;
                out.push_back("{\"k\":" + std::to_string(k) + ",\"ts\":" + std::to_string(ts) +
                              "}");
            }
        }
    }
    return out;
}

// Committed output only: an aborted transaction from the killed process must
// not be read as a duplicate, and a committed one must not be missed.
std::vector<std::string> consume_committed(const std::string& brokers,
                                           const std::string& topic,
                                           std::size_t minimum,
                                           std::chrono::milliseconds timeout) {
    clink::KafkaSource::Options opts;
    opts.brokers = brokers;
    opts.topic = topic;
    opts.group_id = "clink-embedded-resume-oracle-" + std::to_string(::getpid()) + "-" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    opts.auto_offset_reset = "earliest";
    opts.poll_timeout = 100ms;
    opts.batch_max_wait = 10ms;
    opts.conf["isolation.level"] = "read_committed";
    clink::KafkaSource source(std::move(opts));
    source.open();
    std::vector<std::string> records;
    clink::Emitter<clink::KafkaMessage> out(clink::Emitter<clink::KafkaMessage>::Forward(
        [&](clink::StreamElement<clink::KafkaMessage> element) {
            if (element.is_data()) {
                for (const auto& record : element.as_data()) {
                    records.push_back(record.value().payload);
                }
            }
            return true;
        }));
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    auto quiet_since = std::chrono::steady_clock::time_point{};
    std::size_t previous = 0;
    // Keep reading for a quiet period after the minimum arrives, so a
    // duplicate committed just after the expected rows is still seen.
    while (std::chrono::steady_clock::now() < deadline) {
        source.produce(out);
        if (records.size() != previous) {
            previous = records.size();
            quiet_since = std::chrono::steady_clock::now();
        } else if (records.size() >= minimum &&
                   quiet_since != std::chrono::steady_clock::time_point{} &&
                   std::chrono::steady_clock::now() - quiet_since >= 2s) {
            break;
        }
    }
    source.close();
    return records;
}

std::string pipeline_sql(const std::string& brokers,
                         const std::string& in_topic,
                         const std::string& out_topic) {
    return "CREATE TABLE src (k BIGINT, ts BIGINT) WITH (connector='kafka', format='json', "
           "brokers='" +
           brokers + "', topic='" + in_topic +
           "', group_id='embedded-resume', auto_offset_reset='earliest', "
           "event_time_column='ts', watermark_lag_ms='0');\n"
           "CREATE TABLE out_t (k BIGINT, ws BIGINT, cnt BIGINT) WITH (connector='kafka', "
           "format='json', brokers='" +
           brokers + "', topic='" + out_topic +
           "', delivery_guarantee='exactly_once', transactional_id='embedded-resume-" +
           std::to_string(::getpid()) +
           "');\n"
           "INSERT INTO out_t SELECT k, window_start AS ws, COUNT(*) AS cnt FROM src "
           "GROUP BY TUMBLE(ts, INTERVAL '10' SECOND), k;\n";
}

class EmbeddedResumeKafka : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        if (clink::test::DockerKafka::docker_available()) {
            broker_ = std::make_unique<clink::test::DockerKafka>();
        }
    }
    static void TearDownTestSuite() { broker_.reset(); }

    void SetUp() override {
        if (broker_ == nullptr) {
            GTEST_SKIP() << "Docker not available";
        }
        if (cli_binary().empty() || !fs::exists(cli_binary())) {
            GTEST_SKIP() << "clink CLI not built";
        }
        root_ = fs::temp_directory_path() /
                ("clink_it_embedded_resume_" + std::to_string(::getpid()) + "_" +
                 ::testing::UnitTest::GetInstance()->current_test_info()->name());
        fs::remove_all(root_);
        fs::create_directories(root_);
    }

    void TearDown() override {
        if (!HasFailure()) {
            std::error_code ec;
            fs::remove_all(root_, ec);
        }
    }

    bool spawn_run(Process& proc, const std::string& label, const fs::path& script) {
        return proc.spawn(
            label,
            cli_binary(),
            {cli_binary().string(),
             "run",
             script.string(),
             "--checkpoint-dir=" + (root_ / "ckpt").string(),
             "--checkpoint-interval-ms=200"},
            root_,
            clink::itest::ProcOptions{.env = {{"CLINK_PROTOCOL_TRACE_DIR", trace_dir().string()}}});
    }

    // Both processes trace into one directory: together they are one run of
    // the protocol, a coordinator that dies and one that recovers its job, which
    // scripts/formal-check.sh --trace validates against the specification.
    [[nodiscard]] fs::path trace_dir() const {
        if (const char* keep = std::getenv("CLINK_ITEST_KEEP_TRACE_DIR"); keep != nullptr) {
            return fs::path{keep};
        }
        return root_ / "protocol-trace";
    }

    static std::unique_ptr<clink::test::DockerKafka> broker_;
    fs::path root_;
};

std::unique_ptr<clink::test::DockerKafka> EmbeddedResumeKafka::broker_;

TEST_F(EmbeddedResumeKafka, AKilledRunResumedWithTheSameScriptCommitsEveryWindowExactlyOnce) {
    const std::string in_topic = "clink_embedded_resume_in";
    const std::string out_topic = "clink_embedded_resume_out";
    broker_->create_topic(in_topic);
    broker_->create_topic(out_topic);
    const auto brokers = broker_->brokers();
    const auto script = root_ / "pipeline.sql";
    {
        std::ofstream(script) << pipeline_sql(brokers, in_topic, out_topic);
    }

    // First half of the stream. Windows 0..3 close on window 4's watermark, so
    // the first process commits output before it is killed.
    produce(brokers, in_topic, window_events(0, (kWindows / 2) - 1));

    Process first;
    ASSERT_TRUE(spawn_run(first, "run-1", script));
    // Several checkpoints past the one that would have committed the closed
    // windows, so the kill lands with committed output behind it.
    const bool checkpointed = clink::itest::await(
        [&] { return latest_completed(root_ / "ckpt") >= 5 || !first.running(); }, 60s);
    ASSERT_TRUE(checkpointed && first.running()) << "first run never checkpointed:\n"
                                                 << first.read_log();
    const auto committed_before_kill = consume_committed(brokers, out_topic, 1, 10s);
    ASSERT_FALSE(committed_before_kill.empty())
        << "the first run must have committed output before the kill, or the rerun "
           "has nothing to duplicate:\n"
        << first.read_log();
    first.kill_hard();
    ASSERT_TRUE(first.await_exit(10s).has_value());
    const auto checkpoint_at_kill = latest_completed(root_ / "ckpt");

    // The rest of the stream, then one event far ahead whose watermark closes
    // every window (its own window stays open and is not expected).
    auto rest = window_events(kWindows / 2, kWindows - 1);
    rest.push_back("{\"k\":99,\"ts\":" + std::to_string(kBaseTs + ((kWindows + 5) * kWindowMs)) +
                   "}");
    produce(brokers, in_topic, rest);

    Process second;
    ASSERT_TRUE(spawn_run(second, "run-2", script));
    const auto payloads = consume_committed(brokers, out_topic, kKeys * kWindows, 90s);
    const bool still_running = second.running();
    second.kill_and_reap();

    const auto log = second.read_log();
    EXPECT_TRUE(still_running) << "the resumed run exited early:\n" << log;
    EXPECT_NE(log.find("resuming from checkpoint"), std::string::npos) << log;
    EXPECT_GE(latest_completed(root_ / "ckpt"), checkpoint_at_kill);

    std::map<std::pair<std::int64_t, std::int64_t>, std::vector<std::int64_t>> windows;
    for (const auto& p : payloads) {
        const auto json = clink::config::parse(p);
        windows[{json.at("k").as_int(), json.at("ws").as_int()}].push_back(json.at("cnt").as_int());
    }
    for (int w = 0; w < kWindows; ++w) {
        for (int k = 0; k < kKeys; ++k) {
            const auto ws = kBaseTs + (w * kWindowMs);
            const auto it = windows.find({k, ws});
            if (it == windows.end()) {
                ADD_FAILURE() << "window k=" << k << " ws=" << ws << " was never committed";
                continue;
            }
            EXPECT_EQ(it->second.size(), 1u) << "window k=" << k << " ws=" << ws << " committed "
                                             << it->second.size() << " times";
            EXPECT_EQ(it->second.front(), kEventsPerKeyWindow)
                << "window k=" << k << " ws=" << ws << " has the wrong count";
        }
    }
    EXPECT_EQ(payloads.size(), static_cast<std::size_t>(kKeys * kWindows))
        << "committed rows beyond the expected windows";
}

}  // namespace
