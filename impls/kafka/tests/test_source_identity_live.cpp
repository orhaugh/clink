// The Kafka source's identity check against real brokers. Apache Kafka gives a
// topic a new id when it is deleted and created again under its name, and a
// restore of positions recorded before is refused, naming both ids as
// kafka-topics.sh prints them. Redpanda 24.2 reports no topic ids, so there
// only the topic and the cluster are compared and a topic created again is not
// told apart, the limit docs/connectors/kafka.md states. librdkafka's mock
// cluster cannot delete a topic, which is why these run against Docker. Skips
// without it.
#include <chrono>
#include <cstddef>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/connectors/kafka_message.hpp"
#include "clink/connectors/kafka_sink.hpp"
#include "clink/connectors/kafka_source.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"

#include "tests/integration/docker_kafka.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

void identity_live_produce(const std::string& brokers,
                           const std::string& topic,
                           const std::string& prefix,
                           int count) {
    KafkaSink::Options opts;
    opts.brokers = brokers;
    opts.topic = topic;
    MetricsRegistry metrics;
    RuntimeContext ctx{OperatorId{1}, "kafka_sink", nullptr, &metrics};
    KafkaSink sink(opts);
    sink.attach_runtime(&ctx);
    sink.open();
    Batch<KafkaMessage> batch;
    for (int i = 0; i < count; ++i) {
        batch.emplace(KafkaMessage{prefix + std::to_string(i)});
    }
    sink.on_data(batch);
    sink.flush();
    sink.close();
}

// A source as a job's runner holds one: one record per produce(), manual
// commit and the earliest reset, so a source that resumed from its checkpoint
// is told apart from one that started again at the beginning.
struct IdentityLiveSource {
    MetricsRegistry metrics;
    RuntimeContext ctx;
    KafkaSource source;

    IdentityLiveSource(const std::string& brokers,
                       const std::string& topic,
                       const std::string& group,
                       OperatorId op)
        : ctx{op, "kafka_source", nullptr, &metrics}, source(options(brokers, topic, group)) {
        source.attach_runtime(&ctx);
    }

    static KafkaSource::Options options(const std::string& brokers,
                                        const std::string& topic,
                                        const std::string& group) {
        KafkaSource::Options o;
        o.brokers = brokers;
        o.topic = topic;
        o.group_id = group;
        o.auto_offset_reset = "earliest";
        o.commit_mode = KafkaSource::CommitMode::Manual;
        o.max_batch_size = 1;
        return o;
    }

    std::vector<std::string> read(std::size_t n) {
        std::vector<std::string> out;
        BoundedChannel<StreamElement<KafkaMessage>> ch(64);
        Emitter<KafkaMessage> em(&ch);
        const auto end = std::chrono::steady_clock::now() + 30s;
        while (out.size() < n && std::chrono::steady_clock::now() < end) {
            source.produce(em);
            while (auto el = ch.try_pop()) {
                if (el->is_data()) {
                    for (auto& r : el->as_data()) {
                        out.push_back(std::move(r.value().payload));
                    }
                }
            }
        }
        return out;
    }

    // What open() refused with, or empty when it opened.
    std::string refusal() {
        try {
            source.open();
        } catch (const std::runtime_error& e) {
            return e.what();
        }
        source.close();
        return "";
    }
};

std::optional<KafkaSource::Identity> identity_live_recorded(const InMemoryStateBackend& backend,
                                                            OperatorId op) {
    const auto row = backend.get_operator_state(op, std::string_view{"__kafka_identity__"});
    if (!row.has_value()) {
        return std::nullopt;
    }
    return KafkaSource::decode_identity(
        std::string_view{reinterpret_cast<const char*>(row->data()), row->size()});
}

// Writes ten records to `topic`, reads six and snapshots the source into
// `checkpoint`.
void identity_live_checkpoint(const std::string& brokers,
                              const std::string& topic,
                              OperatorId op,
                              InMemoryStateBackend& checkpoint) {
    identity_live_produce(brokers, topic, "before-", 10);
    IdentityLiveSource first(brokers, topic, "identity-live-first", op);
    first.source.open();
    const auto read = first.read(6);
    first.source.snapshot_offset(checkpoint, op, CheckpointId{1});
    first.source.close();
    EXPECT_EQ(read.size(), 6U);
}

}  // namespace

// The checkpoint records the topic's id as kafka-topics.sh prints it, and
// restores on the same topic. Deleted and created again under its name, the
// topic has a new id, and the restore is refused: without the check the
// source would resume at offset 6 of the new topic and never read its first
// six records.
TEST(KafkaSourceIdentityLive, ARestoreOntoTheTopicCreatedAgainIsRefused) {
    if (!test::DockerKafka::docker_available()) {
        GTEST_SKIP() << "Docker not available";
    }
    test::DockerKafkaOptions opts;
    opts.apache_kafka = true;
    test::DockerKafka kafka(opts);
    const std::string topic = "orders";
    kafka.create_topic(topic);
    const OperatorId op{50};
    InMemoryStateBackend checkpoint;
    identity_live_checkpoint(kafka.brokers(), topic, op, checkpoint);

    const auto recorded = identity_live_recorded(checkpoint, op);
    ASSERT_TRUE(recorded.has_value());
    const std::string old_id = kafka.topic_id(topic);
    ASSERT_FALSE(old_id.empty());
    EXPECT_EQ(recorded->topic, topic);
    EXPECT_EQ(recorded->topic_id, old_id);
    EXPECT_FALSE(recorded->cluster_id.empty());

    {
        IdentityLiveSource same(kafka.brokers(), topic, "identity-live-same", op);
        ASSERT_TRUE(same.source.restore_offset(checkpoint, op));
        same.source.open();
        const auto resumed = same.read(4);
        same.source.close();
        ASSERT_EQ(resumed.size(), 4U);
        EXPECT_EQ(resumed.front(), "before-6");
    }

    kafka.recreate_topic(topic);
    identity_live_produce(kafka.brokers(), topic, "after-", 10);
    const std::string new_id = kafka.topic_id(topic);
    ASSERT_FALSE(new_id.empty());
    ASSERT_NE(new_id, old_id);
    IdentityLiveSource again(kafka.brokers(), topic, "identity-live-again", op);
    ASSERT_TRUE(again.source.restore_offset(checkpoint, op));
    const std::string refusal = again.refusal();
    ASSERT_FALSE(refusal.empty()) << "positions of the deleted topic were seeked in the new one";
    for (const auto& named : {old_id, new_id, std::string{"deleted and created again"}}) {
        EXPECT_NE(refusal.find(named), std::string::npos) << named << " not in: " << refusal;
    }
}

// Redpanda 24.2 reports its cluster's id and no topic ids. The checkpoint
// records the cluster and no topic id, and a topic created again under its
// name is not told apart: the restore resumes at offset 6 of the new topic.
// This pins the limit the connector page states, so a Redpanda that reports
// topic ids shows up here.
TEST(KafkaSourceIdentityLive, OnRedpandaATopicCreatedAgainIsNotToldApart) {
    if (!test::DockerKafka::docker_available()) {
        GTEST_SKIP() << "Docker not available";
    }
    test::DockerKafka redpanda;
    const std::string topic = "orders";
    redpanda.create_topic(topic);
    const OperatorId op{51};
    InMemoryStateBackend checkpoint;
    identity_live_checkpoint(redpanda.brokers(), topic, op, checkpoint);

    const auto recorded = identity_live_recorded(checkpoint, op);
    ASSERT_TRUE(recorded.has_value());
    EXPECT_EQ(recorded->topic, topic);
    EXPECT_TRUE(recorded->cluster_id.starts_with("redpanda.")) << recorded->cluster_id;
    EXPECT_TRUE(recorded->topic_id.empty()) << recorded->topic_id;

    redpanda.recreate_topic(topic);
    identity_live_produce(redpanda.brokers(), topic, "after-", 10);
    IdentityLiveSource again(redpanda.brokers(), topic, "identity-live-again", op);
    ASSERT_TRUE(again.source.restore_offset(checkpoint, op));
    again.source.open();
    const auto resumed = again.read(1);
    again.source.close();
    ASSERT_EQ(resumed.size(), 1U);
    EXPECT_EQ(resumed.front(), "after-6");
}
