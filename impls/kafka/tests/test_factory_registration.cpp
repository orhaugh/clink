// Verifies clink::kafka::install() registers both the legacy
// kafka_text_* string ops and the typed kafka_message_* ops on their
// respective channel types. Also smoke-tests the KafkaMessage codec
// for round-trip fidelity (payload, key, headers, broker metadata).

#include <gtest/gtest.h>

#include "clink/cluster/runner_registry.hpp"
#include "clink/cluster/type_registry.hpp"
#include "clink/kafka/kafka_message_codec.hpp"

namespace {

using clink::cluster::RunnerRegistry;
using clink::cluster::TypeRegistry;

TEST(KafkaFactoryRegistration, KafkaTextSourceAndSinkAreRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_source("kafka_text_source", "string"), nullptr);
    EXPECT_NE(rr.find_sink("kafka_text_sink", "string"), nullptr);
}

TEST(KafkaFactoryRegistration, KafkaMessageTypedChannelAndOpsAreRegistered) {
    const auto& tr = TypeRegistry::default_instance();
    ASSERT_NE(tr.find(clink::kChannelKafkaMessage), nullptr);

    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_source("kafka_message_source", clink::kChannelKafkaMessage), nullptr);
    EXPECT_NE(rr.find_sink("kafka_message_sink", clink::kChannelKafkaMessage), nullptr);
}

TEST(KafkaMessageCodec, RoundTripsAllFieldsIncludingBrokerMetadata) {
    clink::KafkaMessage in;
    in.payload = "hello\x00world";
    in.payload.push_back('!');  // exercise NUL inside payload
    in.key = "user-42";
    in.headers.push_back({"trace-id", "abc-123"});
    in.headers.push_back({"binary", std::string("\x00\x01\x02", 3)});
    in.offset = 9'999'999;
    in.partition = 3;
    in.timestamp_ms = 1'700'000'000'000;

    const auto codec = clink::kafka_message_codec();
    auto bytes = codec.encode(in);
    auto round = codec.decode(bytes);
    ASSERT_TRUE(round.has_value());
    EXPECT_EQ(round->payload, in.payload);
    ASSERT_TRUE(round->key.has_value());
    EXPECT_EQ(*round->key, *in.key);
    ASSERT_EQ(round->headers.size(), 2u);
    EXPECT_EQ(round->headers[0].key, "trace-id");
    EXPECT_EQ(round->headers[0].value, "abc-123");
    EXPECT_EQ(round->headers[1].value, std::string("\x00\x01\x02", 3));
    EXPECT_EQ(round->offset, in.offset);
    EXPECT_EQ(round->partition, in.partition);
    EXPECT_EQ(round->timestamp_ms, in.timestamp_ms);
}

TEST(KafkaMessageCodec, RoundTripsAbsentKeyAndEmptyHeaders) {
    clink::KafkaMessage in;
    in.payload = "no-key-no-headers";
    // key intentionally left nullopt; headers empty; metadata defaults.

    const auto codec = clink::kafka_message_codec();
    auto bytes = codec.encode(in);
    auto round = codec.decode(bytes);
    ASSERT_TRUE(round.has_value());
    EXPECT_EQ(round->payload, in.payload);
    EXPECT_FALSE(round->key.has_value());
    EXPECT_TRUE(round->headers.empty());
    EXPECT_EQ(round->offset, -1);
    EXPECT_EQ(round->partition, -1);
    EXPECT_EQ(round->timestamp_ms, -1);
}

}  // namespace
