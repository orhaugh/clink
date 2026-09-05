// The registry formats against a real broker and a real Confluent-compatible
// Schema Registry: Redpanda's built-in one, on Docker. Each format writes rows
// through kafka_sink_string, which registers a derived schema under
// "<topic>-value", and reads them back through kafka_source_string; the
// registry's own view of the subject is checked too. Skips without Docker.
#include <gtest/gtest.h>

#if defined(CLINK_HAS_SCHEMA_REGISTRY)

#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/kafka/string_channel.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/schema_registry/client.hpp"
#include "clink/schema_registry/formats.hpp"

#include "tests/integration/docker_kafka.hpp"

using namespace std::chrono_literals;

namespace {

std::unique_ptr<clink::test::DockerKafka> live_broker_;

class KafkaRegistryFormatsLive : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        if (clink::test::DockerKafka::docker_available()) {
            clink::test::DockerKafkaOptions opts;
            opts.schema_registry = true;
            live_broker_ = std::make_unique<clink::test::DockerKafka>(opts);
        }
    }
    static void TearDownTestSuite() { live_broker_.reset(); }
    void SetUp() override {
        if (live_broker_ == nullptr) {
            GTEST_SKIP() << "Docker not available";
        }
    }

    struct Runtime {
        clink::MetricsRegistry metrics;
        clink::RuntimeContext ctx{clink::OperatorId{9}, "kafka_string_live", nullptr, &metrics};
    };

    static clink::plugin::BuildContext ctx_for(const std::string& topic,
                                               std::map<std::string, std::string> extra) {
        clink::plugin::BuildContext ctx;
        ctx.params = std::move(extra);
        ctx.params["brokers"] = live_broker_->brokers();
        ctx.params["topic"] = topic;
        ctx.params["schema_registry_url"] = live_broker_->schema_registry_url();
        return ctx;
    }

    // Write two rows with the format, read them back with it.
    static std::vector<std::string> round_trip(const std::string& format,
                                               const std::string& topic) {
        live_broker_->create_topic(topic);
        auto sink = clink::kafka::build_string_sink(ctx_for(
            topic,
            {{"format", format}, {"schema_columns", "id:i64;name:str;amount:dec_18_2;ok:bool"}}));
        Runtime srt;
        sink->attach_runtime(&srt.ctx);
        sink->open();
        clink::Batch<std::string> rows;
        rows.emplace(R"({"amount":12.50,"id":1,"name":"a","ok":true})");
        rows.emplace(R"({"amount":-0.05,"id":2,"name":null,"ok":false})");
        sink->on_data(rows);
        sink->flush();
        sink->close();

        auto src = clink::kafka::build_string_source(
            ctx_for(topic, {{"format", format}, {"group_id", "live-" + format}}));
        Runtime rt;
        src->attach_runtime(&rt.ctx);
        src->open();
        std::vector<std::string> out;
        clink::BoundedChannel<clink::StreamElement<std::string>> ch(64);
        clink::Emitter<std::string> em(&ch);
        const auto end = std::chrono::steady_clock::now() + 30s;
        while (out.size() < 2 && std::chrono::steady_clock::now() < end) {
            src->produce(em);
            while (auto el = ch.try_pop()) {
                if (!el->is_data()) {
                    continue;
                }
                for (auto& r : el->as_data()) {
                    out.push_back(std::move(r.value()));
                }
            }
        }
        src->close();
        return out;
    }

    static clink::schema_registry::RegisteredSchema registered(const std::string& topic) {
        clink::schema_registry::Client client(
            clink::schema_registry::ClientOptions{.url = live_broker_->schema_registry_url()});
        return client.latest(topic + "-value");
    }
};

#ifdef CLINK_SCHEMA_REGISTRY_HAS_AVRO
TEST_F(KafkaRegistryFormatsLive, AvroRoundTripsAndRegistersTheSubject) {
    const auto out = round_trip("avro", "live_avro");
    ASSERT_EQ(out.size(), 2u);
    EXPECT_EQ(out[0], R"({"amount":"12.50","id":1,"name":"a","ok":true})");
    EXPECT_EQ(out[1], R"({"amount":"-0.05","id":2,"name":null,"ok":false})");
    const auto s = registered("live_avro");
    EXPECT_EQ(s.type, clink::schema_registry::SchemaType::Avro);
    EXPECT_EQ(s.version, 1);
    EXPECT_NE(s.schema.find("\"logicalType\":\"decimal\""), std::string::npos) << s.schema;
}
#endif

TEST_F(KafkaRegistryFormatsLive, JsonSchemaRoundTripsAndRegistersTheSubject) {
    const auto out = round_trip("json-schema", "live_json_schema");
    ASSERT_EQ(out.size(), 2u);
    EXPECT_EQ(out[0], R"({"id":1,"name":"a","amount":12.50,"ok":true})")
        << "declared columns, schema order, exact digits";
    EXPECT_EQ(out[1], R"({"id":2,"name":null,"amount":-0.05,"ok":false})");
    const auto s = registered("live_json_schema");
    EXPECT_EQ(s.type, clink::schema_registry::SchemaType::Json);
    EXPECT_NE(s.schema.find("\"title\":\"live_json_schema\""), std::string::npos) << s.schema;
}

#ifdef CLINK_SCHEMA_REGISTRY_HAS_PROTOBUF
TEST_F(KafkaRegistryFormatsLive, ProtobufRoundTripsAndRegistersTheSubject) {
    const auto out = round_trip("protobuf", "live_protobuf");
    ASSERT_EQ(out.size(), 2u);
    // Decimals travel as proto3 strings; a null bool is the proto3 default.
    EXPECT_EQ(out[0], R"({"amount":"12.50","id":1,"name":"a","ok":true})");
    EXPECT_EQ(out[1], R"({"amount":"-0.05","id":2,"name":"","ok":false})");
    const auto s = registered("live_protobuf");
    EXPECT_EQ(s.type, clink::schema_registry::SchemaType::Protobuf);
    EXPECT_NE(s.schema.find("message live_protobuf {"), std::string::npos) << s.schema;
}
#endif

}  // namespace

#else
TEST(KafkaRegistryFormatsLive, NotAvailableInThisBuild) {
    GTEST_SKIP() << "needs impls/schema_registry";
}
#endif
