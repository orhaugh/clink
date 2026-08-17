// The complete QUAL-01 correctness graph at integration-test scale:
// real Kafka JSON source -> event-time keyed window -> transactional Kafka
// sink, recovered by a new HA coordinator while worker process ids stay
// stable. Component tests cannot detect a checkpoint whose source offset and
// operator state describe different cuts of the stream; the external oracle
// here can.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
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
using clink::itest::Cluster;
using clink::itest::ClusterSpec;
using clink::itest::Process;
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

std::uint64_t latest_marker(const std::filesystem::path& root, std::string_view prefix) {
    std::uint64_t latest = 0;
    std::error_code ec;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root, ec)) {
        if (ec) {
            break;
        }
        const auto name = entry.path().filename().string();
        if (!entry.is_regular_file() || name.rfind(prefix, 0) != 0) {
            continue;
        }
        try {
            latest = std::max(latest, std::stoull(name.substr(prefix.size())));
        } catch (const std::exception&) {
        }
    }
    return latest;
}

struct SeedRecord {
    std::string payload;
    std::int32_t partition;
};

void produce_json(const std::string& brokers,
                  const std::string& topic,
                  const std::vector<SeedRecord>& records) {
    clink::KafkaSink::Options opts;
    opts.brokers = brokers;
    opts.topic = topic;
    opts.metric_prefix.clear();
    clink::KafkaSink sink(std::move(opts));
    sink.open();
    clink::Batch<clink::KafkaMessage> batch;
    for (const auto& record : records) {
        clink::KafkaMessage message{record.payload};
        message.partition = record.partition;
        batch.emplace(std::move(message));
    }
    sink.on_data(batch);
    sink.flush();
    sink.close();
}

// Read committed records until the minimum have arrived, then retain a quiet
// period so duplicates committed in the same recovery transaction are not
// hidden merely because the expected count arrived first.
std::vector<std::string> consume_committed(const std::string& brokers,
                                           const std::string& topic,
                                           std::size_t minimum,
                                           std::chrono::milliseconds timeout) {
    clink::KafkaSource::Options opts;
    opts.brokers = brokers;
    opts.topic = topic;
    opts.group_id = "clink-window-recovery-oracle-" + std::to_string(::getpid()) + "-" +
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
    while (std::chrono::steady_clock::now() < deadline) {
        source.produce(out);
        if (records.size() != previous) {
            previous = records.size();
            quiet_since = std::chrono::steady_clock::now();
        } else if (records.size() >= minimum &&
                   quiet_since != std::chrono::steady_clock::time_point{} &&
                   std::chrono::steady_clock::now() - quiet_since >= 1500ms) {
            break;
        }
    }
    source.close();
    return records;
}

using WindowKey = std::pair<std::int64_t, std::int64_t>;
using Aggregate = std::pair<std::int64_t, std::int64_t>;

std::map<WindowKey, Aggregate> parse_output(const std::vector<std::string>& payloads) {
    std::map<WindowKey, Aggregate> rows;
    for (const auto& payload : payloads) {
        const auto json = clink::config::parse(payload);
        const auto key = WindowKey{json.at("k").as_int(), json.at("ws").as_int()};
        const auto value = Aggregate{json.at("cnt").as_int(), json.at("total").as_int()};
        const auto [_, inserted] = rows.emplace(key, value);
        EXPECT_TRUE(inserted) << "duplicate output for key=" << key.first
                              << " window_start=" << key.second << ": " << payload;
    }
    return rows;
}

std::vector<SeedRecord> window_records(std::int64_t window_start, int keys) {
    std::vector<SeedRecord> records;
    for (int key = 0; key < keys; ++key) {
        for (int copy = 0; copy < 2; ++copy) {
            const auto ts = window_start + 100 + (key * 2) + copy;
            const auto amount = key + copy + 1;
            records.push_back(SeedRecord{
                .payload =
                    "{\"event_id\":\"" + std::to_string(window_start) + "-" + std::to_string(key) +
                    "-" + std::to_string(copy) + "\",\"k\":" + std::to_string(key) +
                    ",\"amount\":" + std::to_string(amount) + ",\"ts\":" + std::to_string(ts) + "}",
                .partition = key % 4});
        }
    }
    return records;
}

class KafkaWindowRecoveryTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        if (clink::test::DockerKafka::docker_available()) {
            kafka_ = std::make_unique<clink::test::DockerKafka>();
        }
    }
    static void TearDownTestSuite() { kafka_.reset(); }

    void SetUp() override {
        if (kafka_ == nullptr) {
            GTEST_SKIP() << "Docker not available; skipping Kafka window recovery gate";
        }
        if (!std::filesystem::exists(node_binary()) || !std::filesystem::exists(sql_binary())) {
            GTEST_SKIP() << "cluster node or SQL submit binary is not built";
        }
        const auto suffix =
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        input_topic_ = "clink_window_recovery_in_" + suffix;
        output_topic_ = "clink_window_recovery_out_" + suffix;
        kafka_->create_topic(input_topic_, 4);
        kafka_->create_topic(output_topic_, 4);
    }

    static std::unique_ptr<clink::test::DockerKafka> kafka_;
    std::string input_topic_;
    std::string output_topic_;
};

std::unique_ptr<clink::test::DockerKafka> KafkaWindowRecoveryTest::kafka_;

TEST_F(KafkaWindowRecoveryTest, WorkerAndHaCoordinatorFailoverKeepSourceWindowAndSinkOnOneCut) {
    constexpr int kKeys = 10;
    constexpr std::int64_t kBase = 1'000'000;

    ClusterSpec spec;
    spec.node_binary = node_binary();
    spec.workers = 2;
    spec.slots_per_worker = 8;
    spec.ha = true;
    spec.http = true;
    Cluster cluster(spec);
    ScopedDiagnostics diagnostics(cluster);
    ASSERT_TRUE(cluster.start_ha_coordinators(1));
    ASSERT_TRUE(cluster.start_ha_worker(0));
    ASSERT_TRUE(cluster.start_ha_worker(1));
    ASSERT_TRUE(cluster.await_workers_registered(2));

    const std::string sql =
        "CREATE TABLE q_in (event_id TEXT, k BIGINT, amount BIGINT, ts BIGINT) WITH "
        "(connector='kafka', format='json', brokers='" +
        kafka_->brokers() + "', topic='" + input_topic_ +
        "', group_id='window-recovery', auto_offset_reset='earliest', "
        "event_time_column='ts', watermark_lag_ms='0'); "
        "CREATE TABLE q_out (k BIGINT, ws BIGINT, cnt BIGINT, total BIGINT) WITH "
        "(connector='kafka', format='json', brokers='" +
        kafka_->brokers() + "', topic='" + output_topic_ +
        "', delivery_guarantee='exactly_once', transactional_id='window-recovery'); "
        "INSERT INTO q_out SELECT k, window_start AS ws, COUNT(*) AS cnt, "
        "SUM(amount) AS total FROM q_in GROUP BY TUMBLE(ts, INTERVAL '10' SECOND), k;";

    Process submit;
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
                              "kafka-window-recovery",
                              "--checkpoint-dir",
                              cluster.checkpoint_dir().string(),
                              "--checkpoint-interval-ms",
                              "100",
                              "--max-restarts-on-worker-loss",
                              "8",
                              "--parallelism",
                              "4"},
                             cluster.log_dir()));
    const auto submit_code = submit.await_exit(30s);
    ASSERT_TRUE(submit_code.has_value());
    ASSERT_EQ(*submit_code, 0) << submit.read_log();

    std::map<WindowKey, Aggregate> expected;
    for (int window = 0; window < 5; ++window) {
        const auto start = kBase + (window * 10'000);
        produce_json(kafka_->brokers(), input_topic_, window_records(start, kKeys));
        for (int key = 0; key < kKeys; ++key) {
            expected[{key, start}] = {2, (2 * key) + 3};
        }
    }

    // Opening window 5 advances the watermark past windows 0..4. Their fifty
    // committed results prove the full graph is live before the failover, while
    // window 5 remains open and checkpointed across it.
    const auto open_start = kBase + 50'000;
    produce_json(kafka_->brokers(), input_topic_, window_records(open_start, kKeys));
    const auto before = consume_committed(kafka_->brokers(), output_topic_, 50, 45s);
    ASSERT_EQ(parse_output(before).size(), 50u)
        << "the pre-failover windows did not commit, so recovery would be vacuous";

    const auto confirmed_before = latest_marker(cluster.checkpoint_dir(), "CONFIRMED-");
    ASSERT_GT(confirmed_before, 0u);
    ASSERT_TRUE(clink::itest::await(
        [&] {
            return latest_marker(cluster.checkpoint_dir(), "CONFIRMED-") >= confirmed_before + 3;
        },
        30s))
        << "the open window was not carried through several completed commits before failover";

    // Match the rig's compound sequence: a worker process is genuinely lost,
    // the whole graph rolls back and the stable-id replacement rejoins.
    const auto before_worker_loss = latest_marker(cluster.checkpoint_dir(), "CONFIRMED-");
    cluster.worker(0).kill_and_reap();
    ASSERT_TRUE(cluster.start_ha_worker(0));
    ASSERT_TRUE(clink::itest::await(
        [&] { return latest_marker(cluster.checkpoint_dir(), "CONFIRMED-") > before_worker_loss; },
        30s))
        << "the four-partition job did not recover after the worker loss";

    // Close the first cross-recovery window and prove the worker restart kept
    // all four partition offsets on the same cut as the keyed state.
    produce_json(kafka_->brokers(), input_topic_, window_records(kBase + 70'000, kKeys));
    for (int key = 0; key < kKeys; ++key) {
        expected[{key, open_start}] = {2, (2 * key) + 3};
    }
    const auto after_worker =
        consume_committed(kafka_->brokers(), output_topic_, expected.size(), 45s);
    ASSERT_EQ(parse_output(after_worker), expected)
        << "worker recovery split Kafka offsets from keyed window state";

    // Window 7 is now open. Carry it through several durable cuts before the
    // coordinator replacement, just as window 5 was before the worker loss.
    const auto confirmed_after_worker = latest_marker(cluster.checkpoint_dir(), "CONFIRMED-");
    ASSERT_TRUE(clink::itest::await(
        [&] {
            return latest_marker(cluster.checkpoint_dir(), "CONFIRMED-") >=
                   confirmed_after_worker + 3;
        },
        30s));

    const auto worker_0_pid = cluster.worker(0).pid();
    const auto worker_1_pid = cluster.worker(1).pid();
    cluster.ha_coordinator(0).kill_and_reap();
    ASSERT_TRUE(cluster.start_ha_coordinators(1))
        << "a replacement coordinator did not acquire leadership";
    ASSERT_TRUE(clink::itest::await(
        [&] { return cluster.count_in_coordinator_log("recovered job_id=1") > 0; }, 30s))
        << "the standby never recovered the SQL job";
    ASSERT_TRUE(cluster.worker(0).running() && cluster.worker(1).running());
    EXPECT_EQ(cluster.worker(0).pid(), worker_0_pid);
    EXPECT_EQ(cluster.worker(1).pid(), worker_1_pid);

    // A later event advances the watermark and closes the window that crossed
    // the restore. Any source/state cut mismatch changes its ten aggregates.
    produce_json(kafka_->brokers(), input_topic_, window_records(kBase + 90'000, kKeys));
    for (int key = 0; key < kKeys; ++key) {
        expected[{key, kBase + 70'000}] = {2, (2 * key) + 3};
    }

    const auto after = consume_committed(kafka_->brokers(), output_topic_, expected.size(), 45s);
    const auto actual = parse_output(after);
    EXPECT_EQ(actual, expected)
        << "HA recovery restored Kafka offsets, keyed window state and the 2PC sink from "
           "different logical cuts";
}

}  // namespace
