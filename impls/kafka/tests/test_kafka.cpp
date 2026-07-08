// Kafka source/sink integration tests, driven by librdkafka's in-process
// mock cluster (rdkafka_mock.h). The mock speaks the real Kafka wire
// protocol on a localhost port, so our C++ code uses the same code path
// as production with no special-casing - yet every test runs in <1s on
// a normal laptop, no Docker required.
//
// Tests self-skip when:
//   * The build has CLINK_HAS_KAFKA off (no librdkafka), or
//   * librdkafka is too old to ship rdkafka_mock.h (CLINK_HAS_KAFKA_MOCK
//     off).

#include <gtest/gtest.h>

#if defined(CLINK_HAS_KAFKA) && defined(CLINK_HAS_KAFKA_MOCK)

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafka_mock.h>

#include "clink/cluster/operator_registry.hpp"
#include "clink/connectors/kafka_message.hpp"
#include "clink/connectors/kafka_sink.hpp"
#include "clink/connectors/kafka_source.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

// Fixture wrapping rd_kafka_mock_cluster_t. Keep one per test so any
// fault injection done by a test can't bleed into the next.
class MockCluster {
public:
    MockCluster() {
        char err[512] = {};
        rd_kafka_conf_t* conf = rd_kafka_conf_new();
        // The host producer is a stub used only to attach the mock; its
        // own log channel is just noise. Silence at WARN+below.
        rd_kafka_conf_set(conf, "log_level", "0", err, sizeof(err));
        host_ = rd_kafka_new(RD_KAFKA_PRODUCER, conf, err, sizeof(err));
        if (host_ == nullptr) {
            throw std::runtime_error(std::string{"MockCluster host: "} + err);
        }
        cluster_ = rd_kafka_mock_cluster_new(host_, /*broker_count*/ 1);
        if (cluster_ == nullptr) {
            rd_kafka_destroy(host_);
            throw std::runtime_error("MockCluster: failed to start");
        }
        bootstrap_ = rd_kafka_mock_cluster_bootstraps(cluster_);
    }

    ~MockCluster() {
        if (cluster_ != nullptr) {
            rd_kafka_mock_cluster_destroy(cluster_);
        }
        if (host_ != nullptr) {
            rd_kafka_destroy(host_);
        }
    }

    MockCluster(const MockCluster&) = delete;
    MockCluster& operator=(const MockCluster&) = delete;
    MockCluster(MockCluster&&) = delete;
    MockCluster& operator=(MockCluster&&) = delete;

    std::string brokers() const { return bootstrap_; }

    // Force the mock broker to return the given error code on the next
    // `count` Produce requests. After they're consumed, normal handling
    // resumes. ApiKey numbers come from the Kafka protocol spec; they
    // aren't in librdkafka's public headers.
    static constexpr std::int16_t kApiKeyProduce = 0;
    static constexpr std::int16_t kApiKeyFetch = 1;

    void inject_request_error(std::int16_t api_key,
                              rd_kafka_resp_err_t err,
                              std::size_t count = 1) {
        std::vector<rd_kafka_resp_err_t> errors(count, err);
        rd_kafka_mock_push_request_errors_array(cluster_, api_key, count, errors.data());
    }

    // Permanently fail every produce against `topic` until cleared with
    // err=RD_KAFKA_RESP_ERR_NO_ERROR.
    void set_topic_error(const std::string& topic, rd_kafka_resp_err_t err) {
        rd_kafka_mock_topic_set_error(cluster_, topic.c_str(), err);
    }

    void create_topic(const std::string& topic, int partitions = 1) {
        rd_kafka_mock_topic_create(cluster_, topic.c_str(), partitions, /*replication*/ 1);
    }

    rd_kafka_mock_cluster_t* raw() { return cluster_; }

private:
    rd_kafka_t* host_{nullptr};
    rd_kafka_mock_cluster_t* cluster_{nullptr};
    std::string bootstrap_;
};

// Small helper: synchronously consume up to `expected` messages from
// `topic` using a dedicated KafkaSource. Returns the messages it saw.
// Bounded by `deadline` so a missing message can't hang the test forever.
std::vector<KafkaMessage> drain(KafkaSource& src,
                                std::size_t expected,
                                std::chrono::milliseconds deadline) {
    std::vector<KafkaMessage> collected;
    BoundedChannel<StreamElement<KafkaMessage>> ch(64);
    Emitter<KafkaMessage> em(&ch);

    const auto end = std::chrono::steady_clock::now() + deadline;
    while (collected.size() < expected && std::chrono::steady_clock::now() < end) {
        src.produce(em);
        while (auto el = ch.try_pop()) {
            if (!el->is_data()) {
                continue;
            }
            for (auto& r : el->as_data()) {
                collected.push_back(std::move(r.value()));
                if (collected.size() == expected) {
                    return collected;
                }
            }
        }
    }
    return collected;
}

// Wraps a sink with a RuntimeContext + MetricsRegistry so we can assert
// on the counters it exports.
struct SinkHarness {
    MetricsRegistry metrics;
    RuntimeContext ctx{OperatorId{1}, "kafka_sink", nullptr, &metrics};
    std::unique_ptr<KafkaSink> sink;

    explicit SinkHarness(KafkaSink::Options opts) {
        sink = std::make_unique<KafkaSink>(std::move(opts));
        sink->attach_runtime(&ctx);
        sink->open();
    }

    ~SinkHarness() {
        if (sink) {
            sink->close();
        }
    }
};

struct SourceHarness {
    MetricsRegistry metrics;
    RuntimeContext ctx{OperatorId{2}, "kafka_source", nullptr, &metrics};
    std::unique_ptr<KafkaSource> source;

    explicit SourceHarness(KafkaSource::Options opts) {
        source = std::make_unique<KafkaSource>(std::move(opts));
        source->attach_runtime(&ctx);
        source->open();
    }

    ~SourceHarness() {
        if (source) {
            source->close();
        }
    }
};

}  // namespace

TEST(Kafka, SinkProducesAndSourceConsumes) {
    MockCluster mock;
    const std::string topic = "round-trip";
    mock.create_topic(topic);

    KafkaSink::Options sink_opts;
    sink_opts.brokers = mock.brokers();
    sink_opts.topic = topic;
    sink_opts.metric_prefix = "rt";
    SinkHarness sink_h(sink_opts);

    Batch<KafkaMessage> batch;
    for (int i = 0; i < 10; ++i) {
        batch.emplace(KafkaMessage{"payload-" + std::to_string(i)});
    }
    sink_h.sink->on_data(batch);
    sink_h.sink->flush();

    EXPECT_EQ(sink_h.sink->delivered_count(), 10u);
    EXPECT_EQ(sink_h.sink->delivery_error_count(), 0u);

    KafkaSource::Options src_opts;
    src_opts.brokers = mock.brokers();
    src_opts.topic = topic;
    src_opts.group_id = "rt-group";
    src_opts.auto_offset_reset = "earliest";
    src_opts.metric_prefix = "rt";
    SourceHarness src_h(src_opts);

    auto received = drain(*src_h.source, 10, 5s);
    ASSERT_EQ(received.size(), 10u);
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(received[i].payload, "payload-" + std::to_string(i));
    }
}

TEST(Kafka, ProduceEmitsPartialBatchWithinBatchMaxWait) {
    // batch_max_wait bounds TOTAL batch formation time: on a paced input
    // whose inter-arrival is far below poll_timeout (so the quiet-break
    // never fires), produce() must emit a PARTIAL batch once the bound
    // elapses instead of accumulating max_batch_size records. Without the
    // bound, every paced call would return a full 256-record batch after
    // ~256 * inter-arrival of waiting - the per-record latency defect the
    // nexmark latency axis measured.
    MockCluster mock;
    const std::string topic = "paced-batching";
    mock.create_topic(topic);

    KafkaSink::Options sink_opts;
    sink_opts.brokers = mock.brokers();
    sink_opts.topic = topic;
    sink_opts.linger_ms = std::chrono::milliseconds{0};  // per-send delivery, no clumping
    SinkHarness sink_h(sink_opts);

    // Paced background producer: one record every ~2ms, enough records
    // (15000 = ~30s) to outlast the worst-case subscribe/assignment prime
    // (10s) plus the whole check window (8s); stopped early via the flag
    // as soon as the check completes.
    std::atomic<bool> stop{false};
    std::thread producer([&] {
        for (int i = 0; i < 15000 && !stop.load(std::memory_order_acquire); ++i) {
            Batch<KafkaMessage> b;
            b.emplace(KafkaMessage{"r-" + std::to_string(i)});
            sink_h.sink->on_data(b);
            std::this_thread::sleep_for(2ms);
        }
    });

    KafkaSource::Options src_opts;
    src_opts.brokers = mock.brokers();
    src_opts.topic = topic;
    src_opts.group_id = "paced-group";
    src_opts.auto_offset_reset = "earliest";
    src_opts.batch_max_wait = std::chrono::milliseconds{20};
    SourceHarness src_h(src_opts);

    // Prime past subscribe/assignment (can take seconds on the mock); the
    // records produced meanwhile sit in the local consumer queue as a
    // backlog, so the first calls may legitimately return full batches.
    (void)drain(*src_h.source, 1, 10s);

    // Property: while records keep arriving every ~2ms, some produce() call
    // must return a partial batch (backlog drained -> bound fires). Without
    // batch_max_wait no paced call ever returns partial.
    BoundedChannel<StreamElement<KafkaMessage>> ch(8);
    Emitter<KafkaMessage> em(&ch);
    bool saw_partial = false;
    std::chrono::steady_clock::duration partial_elapsed{};
    const auto deadline = std::chrono::steady_clock::now() + 8s;
    while (std::chrono::steady_clock::now() < deadline) {
        const auto t0 = std::chrono::steady_clock::now();
        src_h.source->produce(em);
        const auto elapsed = std::chrono::steady_clock::now() - t0;
        std::size_t got = 0;
        while (auto el = ch.try_pop()) {
            if (el->is_data()) {
                got += el->as_data().size();
            }
        }
        if (got >= 1 && got < src_opts.max_batch_size) {
            saw_partial = true;
            partial_elapsed = elapsed;
            break;
        }
    }
    stop.store(true, std::memory_order_release);
    producer.join();

    ASSERT_TRUE(saw_partial) << "produce() never emitted a partial batch on a paced input";
    // The partial call is deadline-bound: first record may block up to
    // poll_timeout (100ms), the fill window adds batch_max_wait (20ms).
    // 400ms leaves generous scheduler/sanitizer headroom while staying far
    // below the unbounded fill (~256 records * 2ms > 500ms).
    EXPECT_LT(partial_elapsed, 400ms);
}

TEST(Kafka, KeysAndHeadersRoundTrip) {
    MockCluster mock;
    const std::string topic = "with-meta";
    mock.create_topic(topic);

    KafkaSink::Options sink_opts;
    sink_opts.brokers = mock.brokers();
    sink_opts.topic = topic;
    SinkHarness sink_h(sink_opts);

    KafkaMessage m{"hello", "alice"};
    m.headers.push_back(KafkaHeader{.key = "trace-id", .value = "abc-123"});
    m.headers.push_back(KafkaHeader{.key = "schema", .value = "v1"});

    Batch<KafkaMessage> batch;
    batch.emplace(std::move(m));
    sink_h.sink->on_data(batch);
    sink_h.sink->flush();

    KafkaSource::Options src_opts;
    src_opts.brokers = mock.brokers();
    src_opts.topic = topic;
    src_opts.group_id = "meta-group";
    src_opts.auto_offset_reset = "earliest";
    SourceHarness src_h(src_opts);

    auto got = drain(*src_h.source, 1, 5s);
    ASSERT_EQ(got.size(), 1u);
    EXPECT_EQ(got[0].payload, "hello");
    ASSERT_TRUE(got[0].key.has_value());
    EXPECT_EQ(*got[0].key, "alice");
    ASSERT_EQ(got[0].headers.size(), 2u);
    EXPECT_EQ(got[0].headers[0].key, "trace-id");
    EXPECT_EQ(got[0].headers[0].value, "abc-123");
    EXPECT_EQ(got[0].headers[1].key, "schema");
    EXPECT_EQ(got[0].headers[1].value, "v1");
    EXPECT_GE(got[0].offset, 0);
    EXPECT_GE(got[0].partition, 0);
}

TEST(Kafka, BinaryPayloadsAreNotMangled) {
    MockCluster mock;
    const std::string topic = "binary";
    mock.create_topic(topic);

    KafkaSink::Options sink_opts;
    sink_opts.brokers = mock.brokers();
    sink_opts.topic = topic;
    SinkHarness sink_h(sink_opts);

    std::string binary;
    for (int b = 0; b < 256; ++b) {
        binary.push_back(static_cast<char>(b));
    }
    Batch<KafkaMessage> batch;
    batch.emplace(KafkaMessage{binary});
    sink_h.sink->on_data(batch);
    sink_h.sink->flush();

    KafkaSource::Options src_opts;
    src_opts.brokers = mock.brokers();
    src_opts.topic = topic;
    src_opts.group_id = "bin-group";
    src_opts.auto_offset_reset = "earliest";
    SourceHarness src_h(src_opts);

    auto got = drain(*src_h.source, 1, 5s);
    ASSERT_EQ(got.size(), 1u);
    EXPECT_EQ(got[0].payload.size(), binary.size());
    EXPECT_EQ(got[0].payload, binary);
}

TEST(Kafka, SinkRecordsDeliveryFailureViaMetricsAndAccessors) {
    // Point the sink at a definitely-unreachable broker. With a short
    // produce_timeout, librdkafka's queued message expires and the
    // delivery report fires with err == __MSG_TIMED_OUT, exercising
    // the sink's failure-tracking path.
    //
    // We deliberately avoid rd_kafka_mock_topic_set_error here. An
    // earlier version of this test used set_topic_error(
    // TOPIC_AUTHORIZATION_FAILED), which segfaulted librdkafka 2.x's
    // rdk:main thread on Debian 13 (visible in gdb backtraces, crash
    // entirely inside librdkafka.so). Pointing at 127.0.0.1:1 is
    // portable across librdkafka versions and platforms.
    KafkaSink::Options sink_opts;
    sink_opts.brokers = "127.0.0.1:1";
    sink_opts.topic = "fail-topic";
    sink_opts.metric_prefix = "fail";
    sink_opts.produce_timeout = 200ms;
    sink_opts.flush_timeout = 2000ms;
    SinkHarness sink_h(sink_opts);

    Batch<KafkaMessage> batch;
    batch.emplace(KafkaMessage{"will-fail"});
    sink_h.sink->on_data(batch);
    sink_h.sink->flush();

    EXPECT_EQ(sink_h.sink->delivered_count(), 0u);
    EXPECT_GE(sink_h.sink->delivery_error_count(), 1u);
    EXPECT_FALSE(sink_h.sink->last_error().empty());

    auto snap = sink_h.metrics.snapshot();
    bool saw_err = false;
    for (const auto& [name, value] : snap.counters) {
        if (name == "kafka_sink.fail.delivery_errors" && value >= 1) {
            saw_err = true;
        }
    }
    EXPECT_TRUE(saw_err);
}

TEST(Kafka, SourceCounterIncrementsPerConsumedMessage) {
    MockCluster mock;
    const std::string topic = "count-topic";
    mock.create_topic(topic);

    KafkaSink::Options sink_opts;
    sink_opts.brokers = mock.brokers();
    sink_opts.topic = topic;
    SinkHarness sink_h(sink_opts);

    Batch<KafkaMessage> batch;
    for (int i = 0; i < 3; ++i) {
        batch.emplace(KafkaMessage{"x" + std::to_string(i)});
    }
    sink_h.sink->on_data(batch);
    sink_h.sink->flush();

    KafkaSource::Options src_opts;
    src_opts.brokers = mock.brokers();
    src_opts.topic = topic;
    src_opts.group_id = "ctr-group";
    src_opts.auto_offset_reset = "earliest";
    src_opts.metric_prefix = "ctr";
    SourceHarness src_h(src_opts);

    auto got = drain(*src_h.source, 3, 5s);
    EXPECT_EQ(got.size(), 3u);

    auto snap = src_h.metrics.snapshot();
    bool saw = false;
    for (const auto& [name, v] : snap.counters) {
        if (name == "kafka_source.ctr.consumed" && v == 3u) {
            saw = true;
        }
    }
    EXPECT_TRUE(saw);
}

TEST(Kafka, SourceCancelStopsProduceLoop) {
    MockCluster mock;
    const std::string topic = "cancel-topic";
    mock.create_topic(topic);

    KafkaSource::Options src_opts;
    src_opts.brokers = mock.brokers();
    src_opts.topic = topic;
    src_opts.group_id = "cancel-group";
    src_opts.auto_offset_reset = "earliest";
    src_opts.poll_timeout = 200ms;
    SourceHarness src_h(src_opts);

    std::atomic<bool> ran{true};
    BoundedChannel<StreamElement<KafkaMessage>> ch(64);
    Emitter<KafkaMessage> em(&ch);

    std::thread producer([&] {
        // Spin until cancelled - produce() returns false at that point.
        while (ran.load(std::memory_order_acquire)) {
            if (!src_h.source->produce(em)) {
                ran.store(false);
                return;
            }
        }
    });

    std::this_thread::sleep_for(100ms);
    src_h.source->cancel();

    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (ran.load() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(20ms);
    }
    EXPECT_FALSE(ran.load());
    if (producer.joinable()) {
        producer.join();
    }
}

TEST(Kafka, SourceTransientErrorsIncrementCounterButContinue) {
    MockCluster mock;
    const std::string topic = "flaky-fetch";
    mock.create_topic(topic);

    KafkaSink::Options sink_opts;
    sink_opts.brokers = mock.brokers();
    sink_opts.topic = topic;
    SinkHarness sink_h(sink_opts);

    // Fault: one transient fetch error (BROKER_NOT_AVAILABLE retries
    // by default), then normal delivery.
    mock.inject_request_error(MockCluster::kApiKeyFetch, RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE, 1);

    Batch<KafkaMessage> batch;
    for (int i = 0; i < 4; ++i) {
        batch.emplace(KafkaMessage{"resilient-" + std::to_string(i)});
    }
    sink_h.sink->on_data(batch);
    sink_h.sink->flush();

    KafkaSource::Options src_opts;
    src_opts.brokers = mock.brokers();
    src_opts.topic = topic;
    src_opts.group_id = "flaky-group";
    src_opts.auto_offset_reset = "earliest";
    src_opts.metric_prefix = "flaky";
    SourceHarness src_h(src_opts);

    // Despite the injected error, librdkafka transparently retries; the
    // source should eventually deliver all 4 records.
    auto got = drain(*src_h.source, 4, 8s);
    EXPECT_EQ(got.size(), 4u);
}

TEST(Kafka, SinkPerRecordPartitionOverridesFixed) {
    MockCluster mock;
    const std::string topic = "partitioned";
    mock.create_topic(topic, /*partitions*/ 3);

    KafkaSink::Options sink_opts;
    sink_opts.brokers = mock.brokers();
    sink_opts.topic = topic;
    sink_opts.fixed_partition = 0;
    SinkHarness sink_h(sink_opts);

    KafkaMessage m1{"to-fixed"};     // partition will be 0 (fixed)
    KafkaMessage m2{"to-explicit"};  // partition will be 2 (per-record)
    m2.partition = 2;

    Batch<KafkaMessage> batch;
    batch.emplace(std::move(m1));
    batch.emplace(std::move(m2));
    sink_h.sink->on_data(batch);
    sink_h.sink->flush();
    EXPECT_EQ(sink_h.sink->delivered_count(), 2u);

    KafkaSource::Options src_opts;
    src_opts.brokers = mock.brokers();
    src_opts.topic = topic;
    src_opts.group_id = "p-group";
    src_opts.auto_offset_reset = "earliest";
    SourceHarness src_h(src_opts);

    auto got = drain(*src_h.source, 2, 5s);
    ASSERT_EQ(got.size(), 2u);

    int found_p0 = 0;
    int found_p2 = 0;
    for (const auto& m : got) {
        if (m.partition == 0 && m.payload == "to-fixed") {
            ++found_p0;
        }
        if (m.partition == 2 && m.payload == "to-explicit") {
            ++found_p2;
        }
    }
    EXPECT_EQ(found_p0, 1);
    EXPECT_EQ(found_p2, 1);
}

TEST(Kafka, ManualCommitMode) {
    MockCluster mock;
    const std::string topic = "manual-commit";
    mock.create_topic(topic);

    KafkaSink::Options sink_opts;
    sink_opts.brokers = mock.brokers();
    sink_opts.topic = topic;
    SinkHarness sink_h(sink_opts);

    Batch<KafkaMessage> batch;
    for (int i = 0; i < 5; ++i) {
        batch.emplace(KafkaMessage{"m" + std::to_string(i)});
    }
    sink_h.sink->on_data(batch);
    sink_h.sink->flush();

    // Stage 1: consume 5, commit explicitly.
    {
        KafkaSource::Options src_opts;
        src_opts.brokers = mock.brokers();
        src_opts.topic = topic;
        src_opts.group_id = "manual-group";
        src_opts.auto_offset_reset = "earliest";
        src_opts.commit_mode = KafkaSource::CommitMode::Manual;
        SourceHarness src_h(src_opts);

        auto got = drain(*src_h.source, 5, 5s);
        ASSERT_EQ(got.size(), 5u);
        EXPECT_TRUE(src_h.source->commit_current());
    }

    // Stage 2: same group, auto_offset_reset=earliest. Manual commits
    // from stage 1 mean we should see no records - group offset is past
    // the end of the partition.
    {
        KafkaSource::Options src_opts;
        src_opts.brokers = mock.brokers();
        src_opts.topic = topic;
        src_opts.group_id = "manual-group";  // same group!
        src_opts.auto_offset_reset = "earliest";
        src_opts.commit_mode = KafkaSource::CommitMode::Auto;
        SourceHarness src_h(src_opts);

        auto got = drain(*src_h.source, 1, 1s);  // expect nothing within 1s
        EXPECT_TRUE(got.empty());
    }
}

// Regression: aborting a 2PC transaction must leave a FRESH transaction
// open so the sink can keep producing for the next checkpoint. Before
// the fix, abort_transaction() rolled back without re-beginning, so the
// first on_data() after an abort had no open transaction and the produce
// failed (and the next commit would error). open() and commit_transaction()
// both leave a fresh transaction open; abort_transaction() must too.
TEST(Kafka, SinkReopensTransactionAfterAbort) {
    MockCluster mock;
    const std::string topic = "txn-abort-reopen";
    mock.create_topic(topic);

    KafkaSink::Options sink_opts;
    sink_opts.brokers = mock.brokers();
    sink_opts.topic = topic;
    sink_opts.transactional_id = "txn-abort-reopen-tid";  // enable 2PC
    sink_opts.metric_prefix = "txa";
    SinkHarness sink_h(sink_opts);  // open(): init_transactions + begin txn 1
    auto& sink = *sink_h.sink;

    // Transaction 1: produce a record, then ABORT it.
    Batch<KafkaMessage> aborted;
    aborted.emplace(KafkaMessage{"aborted"});
    sink.on_data(aborted);
    sink.flush();
    sink.abort_transaction();  // rolls back txn1 AND re-opens txn2 (the fix)

    // Transaction 2 (post-abort): with a fresh transaction open, this
    // produce succeeds and commits cleanly. Without the fix the produce
    // has no open transaction to land in.
    Batch<KafkaMessage> committed;
    committed.emplace(KafkaMessage{"committed"});
    sink.on_data(committed);
    sink.flush();
    EXPECT_EQ(sink.delivery_error_count(), 0u)
        << "post-abort produce must land in a freshly-opened transaction";
    sink.commit_transaction();

    // The committed record is durable and readable.
    KafkaSource::Options src_opts;
    src_opts.brokers = mock.brokers();
    src_opts.topic = topic;
    src_opts.group_id = "txa-group";
    src_opts.auto_offset_reset = "earliest";
    src_opts.metric_prefix = "txa";
    SourceHarness src_h(src_opts);
    // rd_kafka_mock does not simulate read_committed isolation (no LSO /
    // abort-marker filtering), so a consumer sees BOTH the aborted and the
    // committed record here; against a real broker a read_committed consumer
    // would see only "committed". What this test pins is the SINK side: after
    // an abort the transaction is re-opened, so the post-abort produce lands
    // in a fresh transaction and commits durably - i.e. "committed" is
    // present and readable. (delivery_error_count == 0 above already proved
    // the produce had an open transaction to land in.)
    auto received = drain(*src_h.source, 2, 5s);
    bool saw_committed = false;
    for (const auto& m : received) {
        if (m.payload == "committed") {
            saw_committed = true;
        }
    }
    EXPECT_TRUE(saw_committed) << "the post-abort committed record must be durably produced";
}

// Offset-map serialization round-trips (broker-independent).
TEST(Kafka, OffsetMapEncodeDecodeRoundTrip) {
    std::map<std::int32_t, std::int64_t> offsets{{0, 6}, {3, 0}, {7, 1234567890123LL}};
    const auto bytes = KafkaSource::encode_offsets(offsets);
    EXPECT_EQ(KafkaSource::decode_offsets(bytes), offsets);
    EXPECT_TRUE(KafkaSource::decode_offsets(std::string_view{}).empty());
    EXPECT_TRUE(KafkaSource::encode_offsets({}).size() >= 4u);  // count prefix only
}

// Source replay: a source that consumed part of a partition snapshots its
// offset; a fresh source restored from that snapshot seeks past the consumed
// records on assignment, so it resumes at the next offset rather than
// re-reading from the start. Manual commit + earliest reset means WITHOUT the
// seek the restored source would replay from offset 0 - so this genuinely
// exercises the clink-checkpoint-as-source-of-truth path.
TEST(Kafka, SourceReplaysFromSnapshottedOffset) {
    MockCluster mock;
    const std::string topic = "offset-replay";
    mock.create_topic(topic);

    KafkaSink::Options sink_opts;
    sink_opts.brokers = mock.brokers();
    sink_opts.topic = topic;
    SinkHarness sink_h(sink_opts);
    Batch<KafkaMessage> batch;
    for (int i = 0; i < 10; ++i) {
        batch.emplace(KafkaMessage{"payload-" + std::to_string(i)});
    }
    sink_h.sink->on_data(batch);
    sink_h.sink->flush();
    ASSERT_EQ(sink_h.sink->delivered_count(), 10u);

    InMemoryStateBackend backend;
    const OperatorId op_id{20};

    KafkaSource::Options src_opts;
    src_opts.brokers = mock.brokers();
    src_opts.topic = topic;
    src_opts.group_id = "replay-group";
    src_opts.auto_offset_reset = "earliest";
    src_opts.commit_mode = KafkaSource::CommitMode::Manual;  // no Kafka-side commit
    // One record per produce() so the first run advances the consumer by
    // exactly 6, not the whole topic in a single 256-record batch.
    src_opts.max_batch_size = 1;

    // First run: consume 6 of 10 (offsets 0..5), then snapshot the offset.
    {
        SourceHarness a(src_opts);
        auto got = drain(*a.source, 6, 5s);
        ASSERT_EQ(got.size(), 6u);
        EXPECT_EQ(got.front().payload, "payload-0");
        EXPECT_EQ(got.back().payload, "payload-5");
        a.source->snapshot_offset(backend, op_id, CheckpointId{1});
    }

    // The snapshot recorded next-offset 6 for partition 0 as a per-partition
    // operator-state row (key "__kafka_off__:0", value i64 LE).
    {
        auto v = backend.get_operator_state(op_id, StateBackend::KeyView{"__kafka_off__:0", 15});
        ASSERT_TRUE(v.has_value());
        ASSERT_GE(v->size(), 8u);
        std::uint64_t off = 0;
        for (int i = 0; i < 8; ++i) {
            off |= static_cast<std::uint64_t>(
                       static_cast<std::uint8_t>((*v)[static_cast<std::size_t>(i)]))
                   << (i * 8);
        }
        EXPECT_EQ(off, 6u);
    }

    // Restart: restore BEFORE open() (mirrors the dag source runner), so the
    // rebalance callback seeks partition 0 to offset 6 on assignment. Use a
    // fresh group so this isolates the SEEK: with a distinct group and
    // earliest reset, the restored source would read from offset 0 (payloads
    // 0..3) WITHOUT the seek, so resuming at payload-6 proves the seek.
    src_opts.group_id = "replay-group-restored";
    MetricsRegistry metrics;
    RuntimeContext ctx{op_id, "kafka_source", nullptr, &metrics};
    KafkaSource second(src_opts);
    second.attach_runtime(&ctx);
    ASSERT_TRUE(second.restore_offset(backend, op_id));
    second.open();
    auto resumed = drain(second, 4, 5s);
    second.close();

    ASSERT_EQ(resumed.size(), 4u) << "restored source must resume the 4 un-consumed records";
    EXPECT_EQ(resumed.front().payload, "payload-6");  // not payload-0
    EXPECT_EQ(resumed.back().payload, "payload-9");
}

// #57: the string/SQL Kafka source (kafka_text_source = StringKafkaSource
// wrapping KafkaSource) must FORWARD snapshot_offset/restore_offset to the
// inner source. Without the forward the wrapper's default no-op hooks run and
// the SQL Kafka path silently loses exactly-once on restart. Build the wrapper
// through the registered factory (the real SQL/string path) and assert the
// inner source's offset state is written / read back.
TEST(Kafka, StringWrapperDelegatesSourceReplay) {
    MockCluster mock;
    const std::string topic = "wrapper-replay";
    mock.create_topic(topic);
    {
        KafkaSink::Options sink_opts;
        sink_opts.brokers = mock.brokers();
        sink_opts.topic = topic;
        SinkHarness sink_h(sink_opts);
        Batch<KafkaMessage> batch;
        for (int i = 0; i < 10; ++i) {
            batch.emplace(KafkaMessage{"p-" + std::to_string(i)});
        }
        sink_h.sink->on_data(batch);
        sink_h.sink->flush();
    }

    const auto* sf = clink::cluster::OperatorRegistry::default_instance().find_source(
        "kafka_text_source", "string");
    ASSERT_NE(sf, nullptr);
    clink::cluster::OperatorBuildContext bctx;
    bctx.params = {{"brokers", mock.brokers()},
                   {"topic", topic},
                   {"group_id", "wrapper-grp"},
                   {"auto_offset_reset", "earliest"}};
    auto src = std::static_pointer_cast<Source<std::string>>(sf->build(bctx));
    ASSERT_NE(src, nullptr);

    InMemoryStateBackend backend;
    const OperatorId op_id{77};
    MetricsRegistry metrics;
    RuntimeContext ctx{op_id, "kafka_text_source", nullptr, &metrics};
    src->attach_runtime(&ctx);
    src->open();

    // Drain at least one record so the inner source advances its offset.
    BoundedChannel<StreamElement<std::string>> ch(64);
    Emitter<std::string> em(&ch);
    std::size_t got = 0;
    const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (got < 10 && std::chrono::steady_clock::now() < end) {
        src->produce(em);
        while (auto el = ch.try_pop()) {
            if (el->is_data()) {
                got += el->as_data().size();
            }
        }
    }
    ASSERT_GT(got, 0u) << "wrapper produced no records";
    src->snapshot_offset(backend, op_id, CheckpointId{1});
    src->close();

    // Delegation proof: the INNER KafkaSource's per-partition offset row was
    // written (key "__kafka_off__:0"); the no-op base hook would write nothing.
    auto v = backend.get_operator_state(op_id, StateBackend::KeyView{"__kafka_off__:0", 15});
    ASSERT_TRUE(v.has_value()) << "wrapper did not delegate snapshot_offset to the inner source";

    // And a fresh wrapper restores it (forwards restore_offset).
    auto src2 = std::static_pointer_cast<Source<std::string>>(sf->build(bctx));
    EXPECT_TRUE(src2->restore_offset(backend, op_id))
        << "wrapper did not delegate restore_offset to the inner source";
}

// #54 owned-only hygiene: after a rescale a subtask restores the full union of
// partition offsets but owns only some. Once assignment is known, snapshot
// must PRUNE the per-partition rows it does not own, so each subtask's
// checkpoint converges to owned-only (no bloat, no stale rows that could
// rewind on a later rescale).
TEST(Kafka, SnapshotPrunesUnownedPartitionRowsAfterAssignment) {
    MockCluster mock;
    const std::string topic = "prune-owned";
    mock.create_topic(topic, 1);  // one partition -> this source owns partition 0

    KafkaSink::Options sink_opts;
    sink_opts.brokers = mock.brokers();
    sink_opts.topic = topic;
    {
        SinkHarness sink_h(sink_opts);
        Batch<KafkaMessage> b;
        for (int i = 0; i < 3; ++i) {
            b.emplace(KafkaMessage{"m" + std::to_string(i)});
        }
        sink_h.sink->on_data(b);
        sink_h.sink->flush();
    }

    InMemoryStateBackend backend;
    const OperatorId op_id{2};
    auto le8 = [](std::int64_t v) {
        std::string s(8, '\0');
        const auto u = static_cast<std::uint64_t>(v);
        for (int i = 0; i < 8; ++i) {
            s[static_cast<std::size_t>(i)] = static_cast<char>((u >> (i * 8)) & 0xFF);
        }
        return s;
    };
    // Simulate the restored union from a rescale: rows for partitions 0,1,2.
    // Only partition 0 exists in this topic, so 1 and 2 are NOT owned.
    backend.put_operator_state(
        op_id, StateBackend::KeyView{"__kafka_off__:0", 15}, StateBackend::ValueView{le8(0)});
    backend.put_operator_state(
        op_id, StateBackend::KeyView{"__kafka_off__:1", 15}, StateBackend::ValueView{le8(100)});
    backend.put_operator_state(
        op_id, StateBackend::KeyView{"__kafka_off__:2", 15}, StateBackend::ValueView{le8(200)});

    KafkaSource::Options src_opts;
    src_opts.brokers = mock.brokers();
    src_opts.topic = topic;
    src_opts.group_id = "prune-grp";
    src_opts.auto_offset_reset = "earliest";
    src_opts.commit_mode = KafkaSource::CommitMode::Manual;
    src_opts.max_batch_size = 1;

    MetricsRegistry metrics;
    RuntimeContext ctx{op_id, "kafka_source", nullptr, &metrics};
    KafkaSource src(src_opts);
    src.attach_runtime(&ctx);
    ASSERT_TRUE(src.restore_offset(backend, op_id));  // reads union {0,1,2}
    src.open();
    drain(src, 1, 5s);  // first poll triggers assignment (owns partition 0)
    src.snapshot_offset(backend, op_id, CheckpointId{1});
    src.close();

    EXPECT_TRUE(backend.get_operator_state(op_id, StateBackend::KeyView{"__kafka_off__:0", 15})
                    .has_value());
    EXPECT_FALSE(
        backend.get_operator_state(op_id, StateBackend::KeyView{"__kafka_off__:1", 15}).has_value())
        << "non-owned partition 1 must be pruned";
    EXPECT_FALSE(
        backend.get_operator_state(op_id, StateBackend::KeyView{"__kafka_off__:2", 15}).has_value())
        << "non-owned partition 2 must be pruned";
}

#else  // !CLINK_HAS_KAFKA || !CLINK_HAS_KAFKA_MOCK

TEST(Kafka, BuildHasKafkaSupport) {
    GTEST_SKIP() << "Built without librdkafka or rdkafka_mock.h; "
                    "skipping mock-broker tests.";
}

#endif
