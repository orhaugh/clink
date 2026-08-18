// The complete QUAL-01 correctness graph at integration-test scale:
// real Kafka JSON source -> event-time keyed window -> transactional Kafka
// sink, recovered by a new HA coordinator while worker process ids stay
// stable. Component tests cannot detect a checkpoint whose source offset and
// operator state describe different cuts of the stream; the external oracle
// here can.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
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
            latest = std::max(latest,
                              static_cast<std::uint64_t>(std::stoull(name.substr(prefix.size()))));
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

std::vector<SeedRecord> sustained_window_records(std::int64_t window_start,
                                                 int keys,
                                                 int copies_per_key) {
    std::vector<SeedRecord> records;
    records.reserve(static_cast<std::size_t>(keys * copies_per_key));
    for (int copy = 0; copy < copies_per_key; ++copy) {
        for (int key = 0; key < keys; ++key) {
            const auto ts = window_start + 100 + ((copy * keys + key) % 9'000);
            const auto amount = key + (copy % 7) + 1;
            records.push_back(SeedRecord{
                .payload = "{\"event_id\":\"live-" + std::to_string(window_start) + "-" +
                           std::to_string(key) + "-" + std::to_string(copy) + "\",\"k\":" +
                           std::to_string(key) + ",\"amount\":" + std::to_string(amount) +
                           ",\"ts\":" + std::to_string(ts) + "}",
                .partition = key % 4});
        }
    }
    return records;
}

void produce_json_sustained(const std::string& brokers,
                            const std::string& topic,
                            const std::vector<SeedRecord>& records,
                            std::atomic<std::size_t>& produced) {
    clink::KafkaSink::Options opts;
    opts.brokers = brokers;
    opts.topic = topic;
    opts.metric_prefix.clear();
    clink::KafkaSink sink(std::move(opts));
    sink.open();
    constexpr std::size_t kBatchSize = 40;
    for (std::size_t begin = 0; begin < records.size(); begin += kBatchSize) {
        clink::Batch<clink::KafkaMessage> batch;
        const auto end = std::min(begin + kBatchSize, records.size());
        for (auto i = begin; i < end; ++i) {
            clink::KafkaMessage message{records[i].payload};
            message.partition = records[i].partition;
            batch.emplace(std::move(message));
        }
        sink.on_data(batch);
        sink.flush();
        produced.store(end, std::memory_order_release);
        std::this_thread::sleep_for(2ms);
    }
    sink.close();
}

// Continuous producer for one open window: emits batches of records for
// every key until told to stop, and returns the exact per-key aggregate it
// sent so the oracle's expectation is built from what actually went in.
struct SustainedFeed {
    std::map<std::int64_t, Aggregate> per_key;
    std::size_t records{0};
};

// Continuous producer with ADVANCING event time: each batch moves the
// stream's event time forward ~1s, so 10s tumbling windows close every few
// dozen wall-milliseconds and every checkpoint interval carries fired
// panes. A gate about replaying committed output needs committed output in
// the stalled interval - a single open window emits nothing mid-episode
// and turns the oracle vacuous. Returns the exact per-(key, window_start)
// aggregate sent, closed and tail windows alike.
std::map<WindowKey, Aggregate> produce_json_advancing(const std::string& brokers,
                                                      const std::string& topic,
                                                      std::int64_t base,
                                                      int keys,
                                                      std::atomic<bool>& stop,
                                                      std::atomic<std::size_t>& produced) {
    clink::KafkaSink::Options opts;
    opts.brokers = brokers;
    opts.topic = topic;
    opts.metric_prefix.clear();
    clink::KafkaSink sink(std::move(opts));
    sink.open();
    std::map<WindowKey, Aggregate> sent;
    std::size_t records = 0;
    for (std::size_t copy = 0; !stop.load(std::memory_order_acquire); ++copy) {
        clink::Batch<clink::KafkaMessage> batch;
        // ~200ms of event time per ~5ms wall batch: a 10s window closes
        // every ~250ms wall, comfortably above the 300ms checkpoint
        // cadence, without racing so far ahead that the oracle's read
        // cannot drain the produced windows within its timeout (the first
        // 1s-per-batch version expected ~8000 windows and timed out).
        const auto ts = base + 100 + static_cast<std::int64_t>(copy) * 200;
        const auto ws = ts - (ts % 10'000);
        for (int key = 0; key < keys; ++key) {
            const auto amount =
                static_cast<std::int64_t>(key) + static_cast<std::int64_t>(copy % 7) + 1;
            clink::KafkaMessage message{"{\"event_id\":\"adv-" + std::to_string(copy) + "-" +
                                        std::to_string(key) + "\",\"k\":" + std::to_string(key) +
                                        ",\"amount\":" + std::to_string(amount) +
                                        ",\"ts\":" + std::to_string(ts) + "}"};
            message.partition = key % 4;
            batch.emplace(std::move(message));
            auto& agg = sent[{key, ws}];
            agg.first += 1;
            agg.second += amount;
            ++records;
        }
        sink.on_data(batch);
        sink.flush();
        produced.store(records, std::memory_order_release);
        std::this_thread::sleep_for(5ms);
    }
    sink.close();
    return sent;
}

// Initialise a transactional producer with `txn_id` and close it again:
// the init bumps the id's producer epoch broker-side, FENCING any prepared
// transaction a previous holder left open. This is the test's stand-in for
// the incarnation churn that fenced a stranded handle on the rig.
void fence_transactional_id(const std::string& brokers,
                            const std::string& topic,
                            const std::string& txn_id) {
    clink::KafkaSink::Options opts;
    opts.brokers = brokers;
    opts.topic = topic;
    opts.transactional_id = txn_id;
    opts.metric_prefix.clear();
    clink::KafkaSink sink(std::move(opts));
    sink.open();
    sink.close();
}

// Commit receipts under <checkpoint_dir>/_jobs/<job>/receipts with a
// checkpoint id above `floor`, across every job in the tree. Iterated with
// the error_code API throughout: this poller races the workers' retention
// sweep, which unlinks snapshots and receipts while we walk, and the
// throwing iterator overloads turn a vanished entry into std::terminate
// inside the await lambda.
std::size_t receipts_above(const std::filesystem::path& checkpoint_root, std::uint64_t floor) {
    std::size_t count = 0;
    std::error_code ec;
    std::filesystem::recursive_directory_iterator it{
        checkpoint_root, std::filesystem::directory_options::skip_permission_denied, ec};
    const std::filesystem::recursive_directory_iterator end;
    while (!ec && it != end) {
        std::error_code entry_ec;
        const auto path = it->path();
        if (it->is_regular_file(entry_ec) && !entry_ec &&
            path.parent_path().filename() == "receipts") {
            const auto name = path.filename().string();
            const auto dash = name.rfind('-');
            if (name.rfind("sub", 0) == 0 && dash != std::string::npos) {
                try {
                    if (std::stoull(name.substr(dash + 1)) > floor) {
                        ++count;
                    }
                } catch (const std::exception&) {
                }
            }
        }
        it.increment(ec);
    }
    return count;
}

SustainedFeed produce_json_until(const std::string& brokers,
                                 const std::string& topic,
                                 std::int64_t window_start,
                                 int keys,
                                 std::atomic<bool>& stop,
                                 std::atomic<std::size_t>& produced) {
    clink::KafkaSink::Options opts;
    opts.brokers = brokers;
    opts.topic = topic;
    opts.metric_prefix.clear();
    clink::KafkaSink sink(std::move(opts));
    sink.open();
    SustainedFeed feed;
    for (std::size_t copy = 0; !stop.load(std::memory_order_acquire); ++copy) {
        clink::Batch<clink::KafkaMessage> batch;
        for (int key = 0; key < keys; ++key) {
            const auto ts = window_start + 100 +
                            static_cast<std::int64_t>((copy * static_cast<std::size_t>(keys) +
                                                       static_cast<std::size_t>(key)) %
                                                      9'000);
            const auto amount =
                static_cast<std::int64_t>(key) + static_cast<std::int64_t>(copy % 7) + 1;
            clink::KafkaMessage message{"{\"event_id\":\"until-" + std::to_string(copy) + "-" +
                                        std::to_string(key) + "\",\"k\":" + std::to_string(key) +
                                        ",\"amount\":" + std::to_string(amount) +
                                        ",\"ts\":" + std::to_string(ts) + "}"};
            message.partition = key % 4;
            batch.emplace(std::move(message));
            auto& agg = feed.per_key[key];
            agg.first += 1;
            agg.second += amount;
            ++feed.records;
        }
        sink.on_data(batch);
        sink.flush();
        produced.store(feed.records, std::memory_order_release);
        std::this_thread::sleep_for(5ms);
    }
    sink.close();
    return feed;
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

    // Keep records arriving in the open window while the coordinator dies.
    // A quiescent source can make independently restored offsets and window
    // state look consistent because both happen to sit exactly on a completed
    // checkpoint. QUAL-01 is continuous, so its HA gate must cover records on
    // both sides of the selected checkpoint as well.
    constexpr int kLiveCopiesPerKey = 1'000;
    const auto live_records = sustained_window_records(kBase + 70'000, kKeys, kLiveCopiesPerKey);
    std::atomic<std::size_t> live_produced{0};
    std::jthread live_producer([&] {
        produce_json_sustained(kafka_->brokers(), input_topic_, live_records, live_produced);
    });
    ASSERT_TRUE(clink::itest::await(
        [&] { return live_produced.load(std::memory_order_acquire) >= 400; }, 15s))
        << "the sustained producer did not overlap the coordinator failure";

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
    live_producer.join();

    const auto confirmed_after_ha = latest_marker(cluster.checkpoint_dir(), "CONFIRMED-");
    ASSERT_TRUE(clink::itest::await(
        [&] {
            return latest_marker(cluster.checkpoint_dir(), "CONFIRMED-") >= confirmed_after_ha + 3;
        },
        30s));

    // A later event advances the watermark and closes the window that crossed
    // the restore. Any source/state cut mismatch changes its ten aggregates.
    produce_json(kafka_->brokers(), input_topic_, window_records(kBase + 90'000, kKeys));
    for (int key = 0; key < kKeys; ++key) {
        const auto whole_cycles = kLiveCopiesPerKey / 7;
        const auto remainder = kLiveCopiesPerKey % 7;
        const auto live_total = static_cast<std::int64_t>(kLiveCopiesPerKey) * (key + 1) +
                                static_cast<std::int64_t>(whole_cycles) * 21 +
                                (static_cast<std::int64_t>(remainder) * (remainder - 1)) / 2;
        expected[{key, kBase + 70'000}] = {2 + kLiveCopiesPerKey, (2 * key) + 3 + live_total};
    }

    const auto after = consume_committed(kafka_->brokers(), output_topic_, expected.size(), 45s);
    const auto actual = parse_output(after);
    EXPECT_EQ(actual, expected)
        << "HA recovery restored Kafka offsets, keyed window state and the 2PC sink from "
           "different logical cuts";
}

// QUAL-01 run C, deterministically: the coordinator dies at
// coordinator.after_completed_marker - AFTER the COMPLETED marker is durable,
// BEFORE the commit broadcast - while records are continuously entering an
// open keyed window. Its replacement inherits the still-armed fault (on the
// rig the auto-restarted coordinator re-reads the same arming), so the
// replacement recovers, redeploys, completes exactly one checkpoint and dies
// at ITS first marker: a one-second ghost incarnation whose consumed input
// and prepared transaction are both thrown away. Only the third coordinator
// runs clean. Worker processes must survive all of it with stable pids, the
// recovered job must number new checkpoints above the ghost's marker instead
// of overwriting it, and the committed output must match the exact external
// oracle. Run C failed this shape with 107,884 inflated aggregates: each
// recovery handed every source subtask the union of all restored offsets,
// and consumer-group churn after the redeploy seeked moved partitions back
// to stale restored offsets, absorbing replayed spans into open windows a
// second time.
TEST_F(KafkaWindowRecoveryTest, RepeatedCompletedMarkerCoordinatorCrashesKeepOneCut) {
    constexpr int kKeys = 10;
    constexpr std::int64_t kBase = 2'000'000;

    ClusterSpec spec;
    spec.node_binary = node_binary();
    spec.workers = 2;
    spec.slots_per_worker = 8;
    spec.ha = true;
    spec.http = true;
    Cluster cluster(spec);
    ScopedDiagnostics diagnostics(cluster);
    // Ordinal 12: the job's first eleven completed checkpoints go through
    // cleanly (a confirmed restore base plus committed pre-crash windows),
    // then the twelfth marker write kills the leader inside the
    // marker-durable-but-commit-unsent window, about a second into the
    // sustained feed.
    ASSERT_TRUE(cluster.start_ha_coordinators(
        1, ProcOptions{.fault = "coordinator.after_completed_marker=exit:72@12"}));
    ASSERT_TRUE(cluster.start_ha_worker(0));
    ASSERT_TRUE(cluster.start_ha_worker(1));
    ASSERT_TRUE(cluster.await_workers_registered(2));

    const std::string sql =
        "CREATE TABLE q_in (event_id TEXT, k BIGINT, amount BIGINT, ts BIGINT) WITH "
        "(connector='kafka', format='json', brokers='" +
        kafka_->brokers() + "', topic='" + input_topic_ +
        "', group_id='marker-crash', auto_offset_reset='earliest', "
        "event_time_column='ts', watermark_lag_ms='0'); "
        "CREATE TABLE q_out (k BIGINT, ws BIGINT, cnt BIGINT, total BIGINT) WITH "
        "(connector='kafka', format='json', brokers='" +
        kafka_->brokers() + "', topic='" + output_topic_ +
        "', delivery_guarantee='exactly_once', transactional_id='marker-crash'); "
        "INSERT INTO q_out SELECT k, window_start AS ws, COUNT(*) AS cnt, "
        "SUM(amount) AS total FROM q_in GROUP BY TUMBLE(ts, INTERVAL '10' SECOND), k;";

    Process submit;
    ASSERT_TRUE(submit.spawn("submit-sql-marker",
                             sql_binary(),
                             {sql_binary().string(),
                              "-e",
                              sql,
                              "--coordinator-host",
                              "127.0.0.1",
                              "--coordinator-port",
                              std::to_string(cluster.http_port()),
                              "--name",
                              "kafka-marker-crash",
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
    // Three windows that close and commit before the first crash: replay
    // across the recoveries must not re-emit or inflate them.
    for (int window = 0; window < 3; ++window) {
        const auto start = kBase + (window * 10'000);
        produce_json(kafka_->brokers(), input_topic_, window_records(start, kKeys));
        for (int key = 0; key < kKeys; ++key) {
            expected[{key, start}] = {2, (2 * key) + 3};
        }
    }

    // The open window that stays live across both coordinator deaths, fed
    // continuously the whole way.
    const auto open_start = kBase + 50'000;
    std::atomic<bool> stop_feed{false};
    std::atomic<std::size_t> produced{0};
    SustainedFeed feed;
    std::thread feeder([&] {
        feed = produce_json_until(
            kafka_->brokers(), input_topic_, open_start, kKeys, stop_feed, produced);
    });
    ASSERT_TRUE(
        clink::itest::await([&] { return produced.load(std::memory_order_acquire) >= 400; }, 15s))
        << "the sustained feed did not start before the armed marker crash";

    // First death: at the fault point, by its distinctive exit code, with the
    // interrupted checkpoint's COMPLETED marker on disk.
    ASSERT_TRUE(clink::itest::await([&] { return !cluster.ha_coordinator(0).running(); }, 60s))
        << "the coordinator never reached coordinator.after_completed_marker";
    const auto first_exit = cluster.ha_coordinator(0).poll_exit();
    ASSERT_TRUE(first_exit.has_value());
    ASSERT_EQ(*first_exit, 72) << "the first coordinator died for another reason";
    const auto markers_after_first = latest_marker(cluster.checkpoint_dir(), "COMPLETED-");
    ASSERT_GT(markers_after_first, 0u)
        << "no COMPLETED marker survived the first crash; the fault fired on the wrong side";

    const auto worker_0_pid = cluster.worker(0).pid();
    const auto worker_1_pid = cluster.worker(1).pid();

    // The ghost: the replacement inherits the still-armed fault and dies at
    // its OWN first completed marker, having recovered, redeployed and run
    // the job for about a second.
    ASSERT_TRUE(cluster.start_ha_coordinators(
        1, ProcOptions{.fault = "coordinator.after_completed_marker=exit:72@1"}));
    ASSERT_TRUE(clink::itest::await([&] { return !cluster.ha_coordinator(1).running(); }, 60s))
        << "the ghost coordinator never completed a checkpoint, so the double-kill "
           "shape was not reproduced";
    const auto ghost_exit = cluster.ha_coordinator(1).poll_exit();
    ASSERT_TRUE(ghost_exit.has_value());
    ASSERT_EQ(*ghost_exit, 72) << "the ghost coordinator died for another reason";
    const auto markers_after_ghost = latest_marker(cluster.checkpoint_dir(), "COMPLETED-");
    ASSERT_GT(markers_after_ghost, markers_after_first)
        << "the ghost incarnation completed no checkpoint of its own - it must, or this "
           "test is not exercising the id-reuse and stale-offset window";
    ASSERT_TRUE(cluster.worker(0).running() && cluster.worker(1).running())
        << "a worker process died across the ghost incarnation";
    EXPECT_EQ(cluster.worker(0).pid(), worker_0_pid);
    EXPECT_EQ(cluster.worker(1).pid(), worker_1_pid);

    // The clean successor.
    ASSERT_TRUE(cluster.start_ha_coordinators(1));
    ASSERT_TRUE(clink::itest::await(
        [&] { return cluster.count_in_coordinator_log("recovered job_id=1") >= 2; }, 60s))
        << "the third coordinator never recovered the job";
    ASSERT_TRUE(cluster.worker(0).running() && cluster.worker(1).running());
    EXPECT_EQ(cluster.worker(0).pid(), worker_0_pid);
    EXPECT_EQ(cluster.worker(1).pid(), worker_1_pid);
    // The ghost left a COMPLETED marker above the last confirmed
    // checkpoint. Two sound recoveries exist: resolution proves the
    // ghost's commits (receipts, or the wire with DescribeTransactions
    // disambiguation) and the restore point reaches the marker - no
    // numbering gap opens - or resolution cannot prove them, the restore
    // stays below, and the recovered job must announce the gap it numbers
    // across rather than rewriting the ghost's records. Before the
    // receipts era only the second path existed and this assertion pinned
    // its announcement alone; the id-reuse mutant is still caught because
    // reusing an id under EITHER path corrupts the ghost's snapshots and
    // fails the exact oracle below.
    EXPECT_GE(cluster.count_in_coordinator_log("numbers new checkpoints from") +
                  cluster.count_in_coordinator_log("commit-CONFIRMED by in-doubt resolution"),
              1u)
        << "recovery neither resolved the ghost's checkpoint nor announced the numbering gap";

    // Let the recovered job absorb a post-recovery stretch of the feed, then
    // close the open window and judge everything against the exact oracle.
    const auto produced_at_recovery = produced.load(std::memory_order_acquire);
    ASSERT_TRUE(clink::itest::await(
        [&] { return produced.load(std::memory_order_acquire) >= produced_at_recovery + 2'000; },
        30s));
    stop_feed.store(true, std::memory_order_release);
    feeder.join();
    for (int key = 0; key < kKeys; ++key) {
        const auto sent = feed.per_key[key];
        expected[{key, open_start}] = sent;
    }

    produce_json(kafka_->brokers(), input_topic_, window_records(kBase + 90'000, kKeys));

    const auto after = consume_committed(kafka_->brokers(), output_topic_, expected.size(), 90s);
    const auto actual = parse_output(after);
    EXPECT_EQ(actual, expected)
        << "repeated coordinator.after_completed_marker recoveries mixed checkpoint cuts: "
           "inflated aggregates mean a replayed span was absorbed twice, missing rows mean "
           "a committed interval was lost";
}

// qual01-20260818a: a sink.before_commit death is survivable alone (the
// matrix covers it), but CASCADED - a second worker lost while the first
// recovery is still settling - it left a completed checkpoint with partial
// external commits: the commit broadcast raced the restart drain, torn-down
// sinks aborted their prepared transactions at close, in-doubt resolution
// found fenced handles, and the fallback replay re-emitted the slices that
// HAD committed - 13,519 identical-value duplicates in one window. Two
// fixes close it: teardown preserves barrier-sealed prepared transactions
// for the resolver, and a checkpoint completing during a restart drain
// keeps its marker but not its broadcast, so the held resolution finalises
// it as one decision. This gate drives the full cascade against the exact
// oracle; the sink-level invariant is pinned deterministically by
// TxnResumeLive.CloseLeavesAPreparedTransactionForTheResolver.
TEST_F(KafkaWindowRecoveryTest, CascadingWorkerLossAcrossACommitWindowStaysExactlyOnce) {
    constexpr int kKeys = 10;
    constexpr std::int64_t kBase = 4'000'000;

    ClusterSpec spec;
    spec.node_binary = node_binary();
    spec.workers = 2;
    spec.slots_per_worker = 8;
    spec.ha = true;
    spec.http = true;
    Cluster cluster(spec);
    ScopedDiagnostics diagnostics(cluster);
    ASSERT_TRUE(cluster.start_ha_coordinators(1));
    ASSERT_TRUE(cluster.start_ha_worker(0, ProcOptions{.fault = "sink.before_commit=exit:72@3"}));
    ASSERT_TRUE(cluster.start_ha_worker(1));
    ASSERT_TRUE(cluster.await_workers_registered(2));

    const std::string sql =
        "CREATE TABLE q_in (event_id TEXT, k BIGINT, amount BIGINT, ts BIGINT) WITH "
        "(connector='kafka', format='json', brokers='" +
        kafka_->brokers() + "', topic='" + input_topic_ +
        "', group_id='cascade', auto_offset_reset='earliest', "
        "event_time_column='ts', watermark_lag_ms='0'); "
        "CREATE TABLE q_out (k BIGINT, ws BIGINT, cnt BIGINT, total BIGINT) WITH "
        "(connector='kafka', format='json', brokers='" +
        kafka_->brokers() + "', topic='" + output_topic_ +
        "', delivery_guarantee='exactly_once', transactional_id='cascade'); "
        "INSERT INTO q_out SELECT k, window_start AS ws, COUNT(*) AS cnt, "
        "SUM(amount) AS total FROM q_in GROUP BY TUMBLE(ts, INTERVAL '10' SECOND), k;";

    Process submit;
    ASSERT_TRUE(submit.spawn("submit-sql-cascade",
                             sql_binary(),
                             {sql_binary().string(),
                              "-e",
                              sql,
                              "--coordinator-host",
                              "127.0.0.1",
                              "--coordinator-port",
                              std::to_string(cluster.http_port()),
                              "--name",
                              "kafka-cascade",
                              "--checkpoint-dir",
                              cluster.checkpoint_dir().string(),
                              "--checkpoint-interval-ms",
                              "300",
                              "--max-restarts-on-worker-loss",
                              "8",
                              "--parallelism",
                              "4"},
                             cluster.log_dir()));
    const auto submit_code = submit.await_exit(30s);
    ASSERT_TRUE(submit_code.has_value());
    ASSERT_EQ(*submit_code, 0) << submit.read_log();

    std::map<WindowKey, Aggregate> expected;
    for (int window = 0; window < 2; ++window) {
        const auto start = kBase + (window * 10'000);
        produce_json(kafka_->brokers(), input_topic_, window_records(start, kKeys));
        for (int key = 0; key < kKeys; ++key) {
            expected[{key, start}] = {2, (2 * key) + 3};
        }
    }

    const auto open_start = kBase + 50'000;
    std::atomic<bool> stop_feed{false};
    std::atomic<std::size_t> produced{0};
    SustainedFeed feed;
    std::thread feeder([&] {
        feed = produce_json_until(
            kafka_->brokers(), input_topic_, open_start, kKeys, stop_feed, produced);
    });
    ASSERT_TRUE(
        clink::itest::await([&] { return produced.load(std::memory_order_acquire) >= 400; }, 15s));

    // First loss: worker 0 dies at the fault point, mid-commit-broadcast,
    // leaving its slice of a completed checkpoint prepared while worker 1's
    // slice committed - the partial-commit state.
    ASSERT_TRUE(clink::itest::await([&] { return !cluster.worker(0).running(); }, 60s))
        << "sink.before_commit never fired on worker 0";
    const auto w0_exit = cluster.worker(0).poll_exit();
    ASSERT_TRUE(w0_exit.has_value());
    ASSERT_EQ(*w0_exit, 72);
    ASSERT_TRUE(cluster.restart_worker_ha(0));

    // Let the first recovery deploy and settle just far enough to have
    // fresh sinks and an in-flight checkpoint...
    const auto confirmed_after_first = latest_marker(cluster.checkpoint_dir(), "CONFIRMED-");
    ASSERT_TRUE(clink::itest::await(
        [&] {
            return latest_marker(cluster.checkpoint_dir(), "CONFIRMED-") >=
                   confirmed_after_first + 2;
        },
        60s))
        << "the first recovery never resumed confirmed commits";

    // ...then the cascade: the second worker dies while the recovered job
    // is live, with commits in flight at a 300ms cadence.
    cluster.worker(1).kill_and_reap();
    ASSERT_TRUE(cluster.start_ha_worker(1));

    const auto confirmed_after_cascade = latest_marker(cluster.checkpoint_dir(), "CONFIRMED-");
    ASSERT_TRUE(clink::itest::await(
        [&] {
            return latest_marker(cluster.checkpoint_dir(), "CONFIRMED-") >=
                   confirmed_after_cascade + 3;
        },
        60s))
        << "the job never resumed confirmed commits after the cascade";

    const auto produced_at_recovery = produced.load(std::memory_order_acquire);
    ASSERT_TRUE(clink::itest::await(
        [&] { return produced.load(std::memory_order_acquire) >= produced_at_recovery + 1'500; },
        30s));
    stop_feed.store(true, std::memory_order_release);
    feeder.join();
    for (int key = 0; key < kKeys; ++key) {
        expected[{key, open_start}] = feed.per_key[key];
    }

    produce_json(kafka_->brokers(), input_topic_, window_records(kBase + 90'000, kKeys));

    const auto after = consume_committed(kafka_->brokers(), output_topic_, expected.size(), 90s);
    const auto actual = parse_output(after);
    EXPECT_EQ(actual, expected)
        << "the cascade produced duplicates (a completed checkpoint's committed slices were "
           "replayed) or losses (a prepared slice was discarded)";
}

// The partial-commit fallback closed end to end. One subtask's commit
// callback throws, stranding its PREPARED transaction inside a COMPLETED
// checkpoint while its siblings' commits execute (receipts on disk); the
// stranded transaction is then FENCED before resolution can finalise it.
// qual01-20260818b reached this through incarnation churn; the test reaches
// it deterministically by initialising the same transactional.id from
// outside during the sink's commit-wait window. The held restart's
// resolution takes the receipted handles as COMMITTED with no wire call,
// gets a fenced VERDICT for the stranded one, and falls back to the last
// confirmed restore point. The replayed interval must reach the output
// exactly once: the receipted sinks arm replay suppression and swallow
// their already-published re-emissions, the fenced subtask re-emits its
// aborted slice, and no process dies at any point - both worker pids hold
// end to end, pinning that this failure class is purely logical.
TEST_F(KafkaWindowRecoveryTest, AFencedPartialCommitFallsBackWithoutDuplicates) {
    constexpr int kKeys = 10;
    constexpr std::int64_t kBase = 6'000'000;

    ClusterSpec spec;
    spec.node_binary = node_binary();
    spec.workers = 2;
    spec.slots_per_worker = 8;
    spec.ha = true;
    spec.http = true;
    Cluster cluster(spec);
    ScopedDiagnostics diagnostics(cluster);
    ASSERT_TRUE(cluster.start_ha_coordinators(1));
    ASSERT_TRUE(cluster.start_ha_worker(0, ProcOptions{.fault = "sink.before_commit=throw@3"}));
    ASSERT_TRUE(cluster.start_ha_worker(1));
    ASSERT_TRUE(cluster.await_workers_registered(2));

    const std::string sql =
        "CREATE TABLE q_in (event_id TEXT, k BIGINT, amount BIGINT, ts BIGINT) WITH "
        "(connector='kafka', format='json', brokers='" +
        kafka_->brokers() + "', topic='" + input_topic_ +
        "', group_id='fenced', auto_offset_reset='earliest', "
        "event_time_column='ts', watermark_lag_ms='0'); "
        "CREATE TABLE q_out (k BIGINT, ws BIGINT, cnt BIGINT, total BIGINT) WITH "
        "(connector='kafka', format='json', brokers='" +
        kafka_->brokers() + "', topic='" + output_topic_ +
        "', delivery_guarantee='exactly_once', transactional_id='fenced'); "
        "INSERT INTO q_out SELECT k, window_start AS ws, COUNT(*) AS cnt, "
        "SUM(amount) AS total FROM q_in GROUP BY TUMBLE(ts, INTERVAL '10' SECOND), k;";

    Process submit;
    ASSERT_TRUE(submit.spawn("submit-sql-fenced",
                             sql_binary(),
                             {sql_binary().string(),
                              "-e",
                              sql,
                              "--coordinator-host",
                              "127.0.0.1",
                              "--coordinator-port",
                              std::to_string(cluster.http_port()),
                              "--name",
                              "kafka-fenced",
                              "--checkpoint-dir",
                              cluster.checkpoint_dir().string(),
                              "--checkpoint-interval-ms",
                              "300",
                              "--max-restarts-on-worker-loss",
                              "8",
                              "--parallelism",
                              "4"},
                             cluster.log_dir()));
    const auto submit_code = submit.await_exit(30s);
    ASSERT_TRUE(submit_code.has_value());
    ASSERT_EQ(*submit_code, 0) << submit.read_log();

    std::map<WindowKey, Aggregate> expected;
    for (int window = 0; window < 2; ++window) {
        const auto start = kBase + (window * 10'000);
        produce_json(kafka_->brokers(), input_topic_, window_records(start, kKeys));
        for (int key = 0; key < kKeys; ++key) {
            expected[{key, start}] = {2, (2 * key) + 3};
        }
    }

    const auto advance_base = kBase + 50'000;
    std::atomic<bool> stop_feed{false};
    std::atomic<std::size_t> produced{0};
    std::map<WindowKey, Aggregate> feed_windows;
    std::thread feeder([&] {
        feed_windows = produce_json_advancing(
            kafka_->brokers(), input_topic_, advance_base, kKeys, stop_feed, produced);
    });
    ASSERT_TRUE(
        clink::itest::await([&] { return produced.load(std::memory_order_acquire) >= 400; }, 15s));

    const auto w0_pid = cluster.worker(0).pid();
    const auto w1_pid = cluster.worker(1).pid();

    // The stall: the armed callback refused a commit (loudly), the same
    // checkpoint's sibling commits executed (their receipts are on disk),
    // and its CONFIRMED can now never arrive from inside the job.
    ASSERT_TRUE(clink::itest::await(
        [&] {
            return cluster.worker(0).log_contains("commit callback for checkpoint") &&
                   receipts_above(cluster.checkpoint_dir(),
                                  latest_marker(cluster.checkpoint_dir(), "CONFIRMED-")) > 0;
        },
        60s))
        << "the partial-commit stall never formed: fault fired="
        << cluster.worker(0).log_contains("commit callback for checkpoint");
    const auto completed_at_stall = latest_marker(cluster.checkpoint_dir(), "COMPLETED-");

    // Fence every sink transactional.id from outside while the stuck sink
    // waits inside its commit-wait window: resolution must now get a
    // VERDICT (fenced), not a resumable orphan. Fencing the healthy ids too
    // costs only their next transactional operation (a logical subtask
    // error, part of the same restart); their commits already executed.
    for (int sub = 0; sub < 4; ++sub) {
        fence_transactional_id(kafka_->brokers(), output_topic_, "fenced-" + std::to_string(sub));
    }

    // Recovery: the fallback restores below the stalled checkpoint, the
    // replay re-runs it, and new checkpoints (numbered above every durable
    // marker) confirm past it.
    ASSERT_TRUE(clink::itest::await(
        [&] { return latest_marker(cluster.checkpoint_dir(), "CONFIRMED-") > completed_at_stall; },
        120s))
        << "the job never confirmed past the fenced fallback";

    // Purely logical episode: no process was lost at any point.
    EXPECT_EQ(cluster.worker(0).pid(), w0_pid) << "worker 0 must survive the whole episode";
    EXPECT_EQ(cluster.worker(1).pid(), w1_pid) << "worker 1 must survive the whole episode";

    // Pin the branch this gate exists for: resolution really did fall back
    // with executed commits on the books (not resolve everything), and the
    // redeployed sinks really did arm suppression from their receipts.
    EXPECT_TRUE(
        cluster.ha_coordinator(0).log_contains("handles committed before resolution failed"))
        << "resolution did not take the fenced-fallback branch; the gate tested nothing";
    EXPECT_TRUE(cluster.worker(0).log_contains("replay suppression armed from receipt") ||
                cluster.worker(1).log_contains("replay suppression armed from receipt"))
        << "no sink armed replay suppression; the replay ran unguarded";

    const auto produced_at_recovery = produced.load(std::memory_order_acquire);
    ASSERT_TRUE(clink::itest::await(
        [&] { return produced.load(std::memory_order_acquire) >= produced_at_recovery + 1'500; },
        30s));
    stop_feed.store(true, std::memory_order_release);
    feeder.join();
    expected.insert(feed_windows.begin(), feed_windows.end());

    // Close the feed's tail window: seed one window comfortably above the
    // highest event time the feed reached, so its watermark fires everything.
    std::int64_t max_ws = advance_base;
    for (const auto& [key, agg] : feed_windows) {
        (void)agg;
        max_ws = std::max(max_ws, key.second);
    }
    // The closer is a pure watermark pusher: its own window has nothing
    // beyond it to advance the watermark past its end, so it never fires
    // and is deliberately NOT expected.
    const auto closer_start = max_ws + 20'000;
    produce_json(kafka_->brokers(), input_topic_, window_records(closer_start, kKeys));

    const auto after = consume_committed(kafka_->brokers(), output_topic_, expected.size(), 180s);
    const auto actual = parse_output(after);
    if (actual != expected) {
        // gtest elides large map dumps; print the symmetric difference so a
        // failure names the exact windows lost, invented, or mis-aggregated.
        for (const auto& [key, agg] : expected) {
            const auto it = actual.find(key);
            if (it == actual.end()) {
                ADD_FAILURE() << "MISSING k=" << key.first << " ws=" << key.second
                              << " expected cnt=" << agg.first << " total=" << agg.second;
            } else if (it->second != agg) {
                ADD_FAILURE() << "WRONG k=" << key.first << " ws=" << key.second
                              << " got cnt=" << it->second.first << " total=" << it->second.second
                              << " expected cnt=" << agg.first << " total=" << agg.second;
            }
        }
        for (const auto& [key, agg] : actual) {
            if (expected.count(key) == 0) {
                ADD_FAILURE() << "EXTRA k=" << key.first << " ws=" << key.second
                              << " cnt=" << agg.first << " total=" << agg.second;
            }
        }
    }
    EXPECT_EQ(actual, expected)
        << "the fenced fallback produced duplicates (already-published re-emissions were not "
           "suppressed) or losses (a record above the receipt horizon was swallowed)";
}

// qual01-20260818d, reproduced deterministically end to end. Two
// ingredients, both required: FIRST a plain worker-loss restore, whose
// union operator-state semantics replicate every sink's staged resume
// handle into every subtask's state (later snapshots re-persist the stale
// copies - the walk once saw 64 handles for 4 sinks and aborted on a
// fenced stale copy); THEN a kill inside the ack window - after the broker
// committed, before the receipt landed. Recovery must read each sink's
// handle only from its own subtask's snapshot, prove the unrecorded
// commit over the wire (re-EndTxn answers idempotently, pinned live), and
// advance CONFIRMED - restoring AT the checkpoint instead of replaying
// the committed slice as 4,560 duplicates.
TEST_F(KafkaWindowRecoveryTest, AKillInTheAckWindowAfterARestoreStaysExactlyOnce) {
    constexpr int kKeys = 10;
    constexpr std::int64_t kBase = 7'000'000;

    ClusterSpec spec;
    spec.node_binary = node_binary();
    spec.workers = 2;
    spec.slots_per_worker = 8;
    spec.ha = true;
    spec.http = true;
    Cluster cluster(spec);
    ScopedDiagnostics diagnostics(cluster);
    ASSERT_TRUE(cluster.start_ha_coordinators(1));
    // Worker 0 starts UNARMED: the fault must fire only after the
    // pollution-bake restore, and a hit ordinal cannot guarantee that -
    // sink placement varies, so a fixed ordinal raced the bake and fired
    // early. Phase 2 restarts worker 0 with the fault armed at its fresh
    // process's FIRST commit dispatch, which is post-restore by
    // construction.
    ASSERT_TRUE(cluster.start_ha_worker(0));
    ASSERT_TRUE(cluster.start_ha_worker(1));
    ASSERT_TRUE(cluster.await_workers_registered(2));

    const std::string sql =
        "CREATE TABLE q_in (event_id TEXT, k BIGINT, amount BIGINT, ts BIGINT) WITH "
        "(connector='kafka', format='json', brokers='" +
        kafka_->brokers() + "', topic='" + input_topic_ +
        "', group_id='ackwin', auto_offset_reset='earliest', "
        "event_time_column='ts', watermark_lag_ms='0'); "
        "CREATE TABLE q_out (k BIGINT, ws BIGINT, cnt BIGINT, total BIGINT) WITH "
        "(connector='kafka', format='json', brokers='" +
        kafka_->brokers() + "', topic='" + output_topic_ +
        "', delivery_guarantee='exactly_once', transactional_id='ackwin'); "
        "INSERT INTO q_out SELECT k, window_start AS ws, COUNT(*) AS cnt, "
        "SUM(amount) AS total FROM q_in GROUP BY TUMBLE(ts, INTERVAL '10' SECOND), k;";

    Process submit;
    ASSERT_TRUE(submit.spawn("submit-sql-ackwin",
                             sql_binary(),
                             {sql_binary().string(),
                              "-e",
                              sql,
                              "--coordinator-host",
                              "127.0.0.1",
                              "--coordinator-port",
                              std::to_string(cluster.http_port()),
                              "--name",
                              "kafka-ackwin",
                              "--checkpoint-dir",
                              cluster.checkpoint_dir().string(),
                              "--checkpoint-interval-ms",
                              "300",
                              "--max-restarts-on-worker-loss",
                              "8",
                              "--parallelism",
                              "4"},
                             cluster.log_dir()));
    const auto submit_code = submit.await_exit(30s);
    ASSERT_TRUE(submit_code.has_value());
    ASSERT_EQ(*submit_code, 0) << submit.read_log();

    std::map<WindowKey, Aggregate> expected;
    for (int window = 0; window < 2; ++window) {
        const auto start = kBase + (window * 10'000);
        produce_json(kafka_->brokers(), input_topic_, window_records(start, kKeys));
        for (int key = 0; key < kKeys; ++key) {
            expected[{key, start}] = {2, (2 * key) + 3};
        }
    }

    const auto advance_base = kBase + 50'000;
    std::atomic<bool> stop_feed{false};
    std::atomic<std::size_t> produced{0};
    std::map<WindowKey, Aggregate> feed_windows;
    std::thread feeder([&] {
        feed_windows = produce_json_advancing(
            kafka_->brokers(), input_topic_, advance_base, kKeys, stop_feed, produced);
    });
    // A fatal assertion returns from the test body early; an unjoined
    // feeder thread then terminates the whole binary and eats the actual
    // failure message.
    struct FeedGuard {
        std::atomic<bool>& stop;
        std::thread& t;
        ~FeedGuard() {
            stop.store(true, std::memory_order_release);
            if (t.joinable()) {
                t.join();
            }
        }
    } feed_guard{stop_feed, feeder};
    ASSERT_TRUE(
        clink::itest::await([&] { return produced.load(std::memory_order_acquire) >= 400; }, 15s));

    // Phase 1 - bake the union pollution: an ordinary worker loss and
    // restore. Every snapshot taken afterwards carries every sink's staged
    // handle rows.
    ASSERT_TRUE(clink::itest::await(
        [&] { return latest_marker(cluster.checkpoint_dir(), "CONFIRMED-") > 0; }, 60s))
        << "no confirmed checkpoint before the pollution bake";
    const auto confirmed_before_bake = latest_marker(cluster.checkpoint_dir(), "CONFIRMED-");
    cluster.worker(1).kill_and_reap();
    ASSERT_TRUE(cluster.start_ha_worker(1));
    ASSERT_TRUE(clink::itest::await(
        [&] {
            return latest_marker(cluster.checkpoint_dir(), "CONFIRMED-") >=
                   confirmed_before_bake + 3;
        },
        90s))
        << "the job never resumed confirmed commits after the pollution-bake restore";
    const auto w1_pid_after_bake = cluster.worker(1).pid();

    // Phase 2 - the ack-window kill. Worker 0 is restarted WITH the fault
    // armed at its first commit dispatch: another restore (more pollution
    // baked), then the fresh process's first broker-acknowledged commit
    // dies before its receipt lands.
    cluster.worker(0).kill_and_reap();
    ASSERT_TRUE(cluster.start_ha_worker(
        0, ProcOptions{.fault = "sink.between_commit_and_receipt=exit:72@1"}));
    ASSERT_TRUE(clink::itest::await([&] { return !cluster.worker(0).running(); }, 120s))
        << "sink.between_commit_and_receipt never fired on the restarted worker 0";
    const auto w0_exit = cluster.worker(0).poll_exit();
    ASSERT_TRUE(w0_exit.has_value());
    ASSERT_EQ(*w0_exit, 72);
    const auto completed_at_kill = latest_marker(cluster.checkpoint_dir(), "COMPLETED-");
    ASSERT_TRUE(cluster.restart_worker_ha(0));

    ASSERT_TRUE(clink::itest::await(
        [&] { return latest_marker(cluster.checkpoint_dir(), "CONFIRMED-") > completed_at_kill; },
        90s))
        << "the job never confirmed past the ack-window kill";

    // The branch this gate exists for: resolution PROVED the unrecorded
    // commit rather than falling back, and no stale union copy poisoned it.
    // Worker 1 must ride the whole ack-window episode in one process: the
    // failure under test is worker 0's alone.
    EXPECT_EQ(cluster.worker(1).pid(), w1_pid_after_bake)
        << "worker 1 must survive the ack-window episode";
    EXPECT_TRUE(cluster.ha_coordinator(0).log_contains("commit-CONFIRMED by in-doubt resolution"))
        << "recovery never proved the ack-window commit over the wire";
    EXPECT_FALSE(
        cluster.ha_coordinator(0).log_contains("handles committed before resolution failed"))
        << "the walk fell back - a stale union handle copy poisoned it again";

    const auto produced_at_recovery = produced.load(std::memory_order_acquire);
    ASSERT_TRUE(clink::itest::await(
        [&] { return produced.load(std::memory_order_acquire) >= produced_at_recovery + 1'500; },
        30s));
    stop_feed.store(true, std::memory_order_release);
    feeder.join();
    expected.insert(feed_windows.begin(), feed_windows.end());

    std::int64_t max_ws = advance_base;
    for (const auto& [key, agg] : feed_windows) {
        (void)agg;
        max_ws = std::max(max_ws, key.second);
    }
    const auto closer_start = max_ws + 20'000;
    produce_json(kafka_->brokers(), input_topic_, window_records(closer_start, kKeys));

    const auto after = consume_committed(kafka_->brokers(), output_topic_, expected.size(), 180s);
    const auto actual = parse_output(after);
    if (actual != expected) {
        for (const auto& [key, agg] : expected) {
            const auto it = actual.find(key);
            if (it == actual.end()) {
                ADD_FAILURE() << "MISSING k=" << key.first << " ws=" << key.second;
            } else if (it->second != agg) {
                ADD_FAILURE() << "WRONG k=" << key.first << " ws=" << key.second;
            }
        }
        for (const auto& [key, agg] : actual) {
            if (expected.count(key) == 0) {
                ADD_FAILURE() << "EXTRA k=" << key.first << " ws=" << key.second
                              << " cnt=" << agg.first << " total=" << agg.second;
            }
        }
    }
    EXPECT_EQ(actual, expected)
        << "the ack-window kill produced duplicates (the committed slice was replayed) or "
           "losses";
}

// A FAILED checkpoint must rewind, not sail on. The abort broadcast that
// follows a failed checkpoint discards every sink's barrier-sealed staged
// transaction - the records of one whole checkpoint interval - and the
// runner survives its own capture failure (it acks ok=false and keeps
// processing), so without a rewind those records simply never reached the
// output: silent loss from a transient snapshot error, the kind a
// 168-hour soak is certain to see at least once. The coordinator now
// initiates the same whole-job restart a subtask error does, and the
// replay re-produces the aborted interval. The armed fault throws inside
// one durable snapshot write - no process dies; both workers must hold
// their pids through the whole episode.
TEST_F(KafkaWindowRecoveryTest, AFailedCheckpointRewindsInsteadOfLosingItsInterval) {
    constexpr int kKeys = 10;
    constexpr std::int64_t kBase = 5'000'000;

    ClusterSpec spec;
    spec.node_binary = node_binary();
    spec.workers = 2;
    spec.slots_per_worker = 8;
    spec.ha = true;
    spec.http = true;
    Cluster cluster(spec);
    ScopedDiagnostics diagnostics(cluster);
    ASSERT_TRUE(cluster.start_ha_coordinators(1));
    ASSERT_TRUE(
        cluster.start_ha_worker(0, ProcOptions{.fault = "checkpoint.before_write=throw@6"}));
    ASSERT_TRUE(cluster.start_ha_worker(1));
    ASSERT_TRUE(cluster.await_workers_registered(2));

    const std::string sql =
        "CREATE TABLE q_in (event_id TEXT, k BIGINT, amount BIGINT, ts BIGINT) WITH "
        "(connector='kafka', format='json', brokers='" +
        kafka_->brokers() + "', topic='" + input_topic_ +
        "', group_id='ckptfail', auto_offset_reset='earliest', "
        "event_time_column='ts', watermark_lag_ms='0'); "
        "CREATE TABLE q_out (k BIGINT, ws BIGINT, cnt BIGINT, total BIGINT) WITH "
        "(connector='kafka', format='json', brokers='" +
        kafka_->brokers() + "', topic='" + output_topic_ +
        "', delivery_guarantee='exactly_once', transactional_id='ckptfail'); "
        "INSERT INTO q_out SELECT k, window_start AS ws, COUNT(*) AS cnt, "
        "SUM(amount) AS total FROM q_in GROUP BY TUMBLE(ts, INTERVAL '10' SECOND), k;";

    Process submit;
    ASSERT_TRUE(submit.spawn("submit-sql-ckptfail",
                             sql_binary(),
                             {sql_binary().string(),
                              "-e",
                              sql,
                              "--coordinator-host",
                              "127.0.0.1",
                              "--coordinator-port",
                              std::to_string(cluster.http_port()),
                              "--name",
                              "kafka-ckptfail",
                              "--checkpoint-dir",
                              cluster.checkpoint_dir().string(),
                              "--checkpoint-interval-ms",
                              "300",
                              "--max-restarts-on-worker-loss",
                              "8",
                              "--parallelism",
                              "4"},
                             cluster.log_dir()));
    const auto submit_code = submit.await_exit(30s);
    ASSERT_TRUE(submit_code.has_value());
    ASSERT_EQ(*submit_code, 0) << submit.read_log();

    std::map<WindowKey, Aggregate> expected;
    for (int window = 0; window < 2; ++window) {
        const auto start = kBase + (window * 10'000);
        produce_json(kafka_->brokers(), input_topic_, window_records(start, kKeys));
        for (int key = 0; key < kKeys; ++key) {
            expected[{key, start}] = {2, (2 * key) + 3};
        }
    }

    const auto open_start = kBase + 50'000;
    std::atomic<bool> stop_feed{false};
    std::atomic<std::size_t> produced{0};
    SustainedFeed feed;
    std::thread feeder([&] {
        feed = produce_json_until(
            kafka_->brokers(), input_topic_, open_start, kKeys, stop_feed, produced);
    });
    ASSERT_TRUE(
        clink::itest::await([&] { return produced.load(std::memory_order_acquire) >= 400; }, 15s));

    const auto worker_0_pid = cluster.worker(0).pid();
    const auto worker_1_pid = cluster.worker(1).pid();

    // The armed persist throws once; the checkpoint fails; the job must
    // REWIND rather than continue past its aborted interval.
    ASSERT_TRUE(clink::itest::await(
        [&] { return cluster.count_in_coordinator_log(" FAILED: subtask(s) ") >= 1; }, 60s))
        << "the armed snapshot failure never failed a checkpoint";
    ASSERT_TRUE(clink::itest::await(
        [&] {
            return cluster.count_in_coordinator_log("checkpoint failure -> whole-job restart") >= 1;
        },
        30s))
        << "a failed checkpoint did not initiate the rewind; its aborted interval is lost";

    // No process died for this: the fault threw inside one write.
    ASSERT_TRUE(cluster.worker(0).running() && cluster.worker(1).running());
    EXPECT_EQ(cluster.worker(0).pid(), worker_0_pid);
    EXPECT_EQ(cluster.worker(1).pid(), worker_1_pid);

    const auto confirmed_after_fail = latest_marker(cluster.checkpoint_dir(), "CONFIRMED-");
    ASSERT_TRUE(clink::itest::await(
        [&] {
            return latest_marker(cluster.checkpoint_dir(), "CONFIRMED-") >=
                   confirmed_after_fail + 3;
        },
        60s));

    const auto produced_at_recovery = produced.load(std::memory_order_acquire);
    ASSERT_TRUE(clink::itest::await(
        [&] { return produced.load(std::memory_order_acquire) >= produced_at_recovery + 1'000; },
        30s));
    stop_feed.store(true, std::memory_order_release);
    feeder.join();
    for (int key = 0; key < kKeys; ++key) {
        expected[{key, open_start}] = feed.per_key[key];
    }

    produce_json(kafka_->brokers(), input_topic_, window_records(kBase + 90'000, kKeys));

    const auto after = consume_committed(kafka_->brokers(), output_topic_, expected.size(), 90s);
    const auto actual = parse_output(after);
    EXPECT_EQ(actual, expected)
        << "a failed checkpoint lost its aborted interval (missing rows) or the rewind "
           "replayed committed output (duplicates)";
}

// ---------------------------------------------------------------------------
// The QUAL-01 2PC fault matrix, local and deterministic: every named fault
// point of the exactly-once protocol kills its process at exactly that point
// while records are continuously entering an open keyed window, the process
// is restarted clean, and the committed output must match the exact oracle.
// The campaign's weighted-random chaos cannot prove this coverage; this
// matrix is what a PASS verdict's required-fault gate points back to.
// ---------------------------------------------------------------------------

struct TwopcFaultCase {
    const char* point;
    bool on_coordinator;
    // Which occurrence kills the process: late enough that a confirmed
    // restore base exists below the interrupted checkpoint.
    int ordinal;
};

std::ostream& operator<<(std::ostream& os, const TwopcFaultCase& c) {
    return os << c.point << "@" << c.ordinal;
}

class KafkaTwopcFaultMatrixTest : public ::testing::TestWithParam<TwopcFaultCase> {
protected:
    static void SetUpTestSuite() {
        if (clink::test::DockerKafka::docker_available()) {
            kafka_ = std::make_unique<clink::test::DockerKafka>();
        }
    }
    static void TearDownTestSuite() { kafka_.reset(); }

    void SetUp() override {
        if (kafka_ == nullptr) {
            GTEST_SKIP() << "Docker not available; skipping 2PC fault matrix";
        }
        if (!std::filesystem::exists(node_binary()) || !std::filesystem::exists(sql_binary())) {
            GTEST_SKIP() << "cluster node or SQL submit binary is not built";
        }
        const auto suffix =
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        input_topic_ = "clink_twopc_matrix_in_" + suffix;
        output_topic_ = "clink_twopc_matrix_out_" + suffix;
        run_tag_ = "m" + suffix;
        kafka_->create_topic(input_topic_, 4);
        kafka_->create_topic(output_topic_, 4);
    }

    static std::unique_ptr<clink::test::DockerKafka> kafka_;
    std::string input_topic_;
    std::string output_topic_;
    std::string run_tag_;
};

std::unique_ptr<clink::test::DockerKafka> KafkaTwopcFaultMatrixTest::kafka_;

TEST_P(KafkaTwopcFaultMatrixTest, DiesAtThePointRecoversAndMatchesTheOracle) {
    const auto& fault_case = GetParam();
    constexpr int kKeys = 10;
    constexpr std::int64_t kBase = 3'000'000;

    ClusterSpec spec;
    spec.node_binary = node_binary();
    spec.workers = 2;
    spec.slots_per_worker = 8;
    spec.ha = true;
    spec.http = true;
    Cluster cluster(spec);
    ScopedDiagnostics diagnostics(cluster);
    const std::string arm =
        std::string{fault_case.point} + "=exit:72@" + std::to_string(fault_case.ordinal);
    ASSERT_TRUE(cluster.start_ha_coordinators(
        1, fault_case.on_coordinator ? ProcOptions{.fault = arm} : ProcOptions{}));
    ASSERT_TRUE(cluster.start_ha_worker(
        0, fault_case.on_coordinator ? ProcOptions{} : ProcOptions{.fault = arm}));
    ASSERT_TRUE(cluster.start_ha_worker(1));
    ASSERT_TRUE(cluster.await_workers_registered(2));

    const std::string sql =
        "CREATE TABLE q_in (event_id TEXT, k BIGINT, amount BIGINT, ts BIGINT) WITH "
        "(connector='kafka', format='json', brokers='" +
        kafka_->brokers() + "', topic='" + input_topic_ + "', group_id='" + run_tag_ +
        "', auto_offset_reset='earliest', "
        "event_time_column='ts', watermark_lag_ms='0'); "
        "CREATE TABLE q_out (k BIGINT, ws BIGINT, cnt BIGINT, total BIGINT) WITH "
        "(connector='kafka', format='json', brokers='" +
        kafka_->brokers() + "', topic='" + output_topic_ +
        "', delivery_guarantee='exactly_once', transactional_id='" + run_tag_ +
        "'); "
        "INSERT INTO q_out SELECT k, window_start AS ws, COUNT(*) AS cnt, "
        "SUM(amount) AS total FROM q_in GROUP BY TUMBLE(ts, INTERVAL '10' SECOND), k;";

    Process submit;
    ASSERT_TRUE(submit.spawn("submit-sql-" + run_tag_,
                             sql_binary(),
                             {sql_binary().string(),
                              "-e",
                              sql,
                              "--coordinator-host",
                              "127.0.0.1",
                              "--coordinator-port",
                              std::to_string(cluster.http_port()),
                              "--name",
                              "kafka-twopc-matrix",
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
    for (int window = 0; window < 2; ++window) {
        const auto start = kBase + (window * 10'000);
        produce_json(kafka_->brokers(), input_topic_, window_records(start, kKeys));
        for (int key = 0; key < kKeys; ++key) {
            expected[{key, start}] = {2, (2 * key) + 3};
        }
    }

    const auto open_start = kBase + 50'000;
    std::atomic<bool> stop_feed{false};
    std::atomic<std::size_t> produced{0};
    SustainedFeed feed;
    std::thread feeder([&] {
        feed = produce_json_until(
            kafka_->brokers(), input_topic_, open_start, kKeys, stop_feed, produced);
    });
    ASSERT_TRUE(
        clink::itest::await([&] { return produced.load(std::memory_order_acquire) >= 200; }, 15s));

    // The armed process must die AT the point, proven by the injected exit
    // code. Without this the matrix would pass vacuously on a renamed or
    // unreachable fault point - precisely the gap that let a campaign
    // record sink.before_prepare coverage while the kafka sink never fired
    // it.
    auto& armed = fault_case.on_coordinator ? cluster.ha_coordinator(0) : cluster.worker(0);
    ASSERT_TRUE(clink::itest::await([&] { return !armed.running(); }, 60s))
        << fault_case.point << " never fired: the armed process outlived the deadline, so "
        << "this fault point is not reachable in the kafka exactly-once pipeline";
    const auto armed_exit = armed.poll_exit();
    ASSERT_TRUE(armed_exit.has_value());
    ASSERT_EQ(*armed_exit, 72) << "the armed process died for a reason other than "
                               << fault_case.point;

    const auto confirmed_at_death = latest_marker(cluster.checkpoint_dir(), "CONFIRMED-");
    if (fault_case.on_coordinator) {
        const auto worker_0_pid = cluster.worker(0).pid();
        const auto worker_1_pid = cluster.worker(1).pid();
        ASSERT_TRUE(cluster.start_ha_coordinators(1));
        ASSERT_TRUE(clink::itest::await(
            [&] { return cluster.count_in_coordinator_log("recovered job_id=1") >= 1; }, 60s));
        ASSERT_TRUE(cluster.worker(0).running() && cluster.worker(1).running());
        EXPECT_EQ(cluster.worker(0).pid(), worker_0_pid);
        EXPECT_EQ(cluster.worker(1).pid(), worker_1_pid);
    } else {
        ASSERT_TRUE(cluster.restart_worker_ha(0));
    }
    // Progress proves recovery: commits confirmed beyond anything from
    // before the death.
    ASSERT_TRUE(clink::itest::await(
        [&] {
            return latest_marker(cluster.checkpoint_dir(), "CONFIRMED-") >= confirmed_at_death + 3;
        },
        60s))
        << "the job never resumed confirmed commits after the " << fault_case.point << " recovery";

    const auto produced_at_recovery = produced.load(std::memory_order_acquire);
    ASSERT_TRUE(clink::itest::await(
        [&] { return produced.load(std::memory_order_acquire) >= produced_at_recovery + 1'000; },
        30s));
    stop_feed.store(true, std::memory_order_release);
    feeder.join();
    for (int key = 0; key < kKeys; ++key) {
        expected[{key, open_start}] = feed.per_key[key];
    }

    produce_json(kafka_->brokers(), input_topic_, window_records(kBase + 90'000, kKeys));

    const auto after = consume_committed(kafka_->brokers(), output_topic_, expected.size(), 90s);
    const auto actual = parse_output(after);
    EXPECT_EQ(actual, expected)
        << fault_case.point
        << ": recovery broke exactly-once (inflated = double-absorbed replay, missing = lost "
           "interval, extra = replayed an externally committed transaction)";
}

INSTANTIATE_TEST_SUITE_P(
    Qual01FaultPoints,
    KafkaTwopcFaultMatrixTest,
    ::testing::Values(TwopcFaultCase{"sink.before_prepare", false, 3},
                      TwopcFaultCase{"sink.after_prepare", false, 3},
                      TwopcFaultCase{"coordinator.before_completed_marker", true, 6},
                      TwopcFaultCase{"coordinator.after_completed_marker", true, 6},
                      TwopcFaultCase{"sink.before_commit", false, 3},
                      TwopcFaultCase{"sink.between_commit_and_receipt", false, 3},
                      TwopcFaultCase{"sink.after_external_commit", false, 3}),
    [](const ::testing::TestParamInfo<TwopcFaultCase>& info) {
        std::string name{info.param.point};
        for (auto& ch : name) {
            if (ch == '.') {
                ch = '_';
            }
        }
        return name;
    });

}  // namespace
