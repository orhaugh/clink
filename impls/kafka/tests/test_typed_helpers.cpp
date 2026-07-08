// Verifies the typed Kafka source/sink fluent helpers produce the
// expected descriptors and connect through the registered factories.

#include <cstdint>
#include <optional>
#include <string>

#include <gtest/gtest.h>

#include "clink/api/stream_execution_environment.hpp"
#include "clink/cluster/built_in_factories.hpp"
#include "clink/connectors/kafka_message.hpp"
#include "clink/kafka/install.hpp"
#include "clink/kafka/kafka_message_codec.hpp"
#include "clink/kafka/typed_sink.hpp"
#include "clink/kafka/typed_source.hpp"

TEST(KafkaTypedHelpers, MessageSourceProducesTypedDescriptor) {
    clink::cluster::ensure_built_ins_registered();
    clink::api::StreamExecutionEnvironment env;
    clink::kafka::install(env.registry());

    auto stream = clink::kafka::message_source(env,
                                               clink::kafka::KafkaSourceOptions{
                                                   .brokers = "localhost:9092",
                                                   .topic = "my-topic",
                                                   .group_id = "my-group",
                                                   .auto_offset_reset = "earliest",
                                               });

    const auto& g = env.graph();
    ASSERT_EQ(g.ops.size(), 1u);
    const auto& op = g.ops.front();
    EXPECT_EQ(op.type, "kafka_message_source");
    EXPECT_EQ(op.out_channel, std::string{clink::kChannelKafkaMessage});
    EXPECT_EQ(op.params.at("brokers"), "localhost:9092");
    EXPECT_EQ(op.params.at("topic"), "my-topic");
    EXPECT_EQ(op.params.at("group_id"), "my-group");
    EXPECT_EQ(op.params.at("auto_offset_reset"), "earliest");

    EXPECT_EQ(stream.channel_type(), std::string{clink::kChannelKafkaMessage});
}

TEST(KafkaTypedHelpers, TextSourceComposesTextSourceWithDecoderMap) {
    // text_source<T> = kafka_text_source + a synthetic flat_map_<n> op
    // that decodes each string into an optional<T>. Two ops in the
    // graph; both reference text-channel-or-target.
    clink::cluster::ensure_built_ins_registered();
    clink::api::StreamExecutionEnvironment env;
    clink::kafka::install(env.registry());

    auto stream = clink::kafka::text_source<std::int64_t>(
        env,
        clink::kafka::KafkaSourceOptions{.brokers = "b:9092", .topic = "t"},
        [](const std::string& s) -> std::optional<std::int64_t> {
            try {
                return std::stoll(s);
            } catch (...) {
                return std::nullopt;
            }
        });

    const auto& g = env.graph();
    ASSERT_EQ(g.ops.size(), 2u);
    EXPECT_EQ(g.ops[0].type, "kafka_text_source");
    EXPECT_EQ(g.ops[0].out_channel, std::string{"string"});
    // The flat_map op type is minted as _inline_flat_map_<n>; we only
    // pin its shape (one input, int64 out_channel) since the synthetic
    // counter resets per-env so the suffix isn't stable enough to
    // assert directly.
    EXPECT_TRUE(g.ops[1].type.starts_with("_inline_flat_map_"));
    EXPECT_EQ(g.ops[1].inputs.size(), 1u);
    EXPECT_EQ(g.ops[1].inputs.front(), g.ops[0].id);
    EXPECT_EQ(g.ops[1].out_channel, std::string{"int64"});

    EXPECT_EQ(stream.channel_type(), std::string{"int64"});
}

TEST(KafkaTypedHelpers, MessageSinkConsumesMessageStream) {
    clink::cluster::ensure_built_ins_registered();
    clink::api::StreamExecutionEnvironment env;
    clink::kafka::install(env.registry());

    auto stream = clink::kafka::message_source(
        env, clink::kafka::KafkaSourceOptions{.brokers = "b:9092", .topic = "in"});
    clink::kafka::message_sink(
        stream,
        clink::kafka::KafkaSinkOptions{
            .brokers = "b:9092", .topic = "out", .acks = "all", .linger_ms = "0"});

    const auto& g = env.graph();
    ASSERT_EQ(g.ops.size(), 2u);
    EXPECT_EQ(g.ops[1].type, "kafka_message_sink");
    EXPECT_EQ(g.ops[1].out_channel, std::string{clink::kChannelKafkaMessage});
    EXPECT_EQ(g.ops[1].params.at("brokers"), "b:9092");
    EXPECT_EQ(g.ops[1].params.at("topic"), "out");
    EXPECT_EQ(g.ops[1].params.at("acks"), "all");
    EXPECT_EQ(g.ops[1].params.at("linger_ms"), "0");
}
