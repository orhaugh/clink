// The registry-framed value formats on the string-channel Kafka factories,
// end to end: rows written through kafka_sink_string with format='avro' land
// on a (mock) broker as Confluent-framed Avro under a schema the sink
// registered, and kafka_source_string with the same format reads them back as
// the channel's JSON text. Plus the decode_error policy for a message the
// format cannot decode, and the build-time refusals.
#include <gtest/gtest.h>

#if defined(CLINK_HAS_KAFKA_MOCK) && defined(CLINK_HAS_SCHEMA_REGISTRY) && \
    defined(CLINK_SCHEMA_REGISTRY_HAS_AVRO)

#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafka_mock.h>

#include "clink/config/json.hpp"
#include "clink/connectors/kafka_message.hpp"
#include "clink/connectors/kafka_sink.hpp"
#include "clink/connectors/kafka_source.hpp"
#include "clink/kafka/string_channel.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/schema_registry/client.hpp"
#include "clink/schema_registry/formats.hpp"
#include "clink/schema_registry/wire_format.hpp"

#include "fake_registry.hpp"

using namespace clink;
using namespace std::chrono_literals;
using clink::plugin::BuildContext;

namespace {

// A one-broker rd_kafka_mock cluster (the same shape test_kafka.cpp uses;
// its own name here because anonymous-namespace twins collide under g++).
class RegistryMockCluster {
public:
    RegistryMockCluster() {
        char err[512] = {};
        rd_kafka_conf_t* conf = rd_kafka_conf_new();
        rd_kafka_conf_set(conf, "log_level", "0", err, sizeof(err));
        host_ = rd_kafka_new(RD_KAFKA_PRODUCER, conf, err, sizeof(err));
        if (host_ == nullptr) {
            throw std::runtime_error(std::string{"mock host: "} + err);
        }
        cluster_ = rd_kafka_mock_cluster_new(host_, 1);
        if (cluster_ == nullptr) {
            rd_kafka_destroy(host_);
            throw std::runtime_error("mock cluster failed to start");
        }
        bootstrap_ = rd_kafka_mock_cluster_bootstraps(cluster_);
    }
    ~RegistryMockCluster() {
        if (cluster_ != nullptr) {
            rd_kafka_mock_cluster_destroy(cluster_);
        }
        if (host_ != nullptr) {
            rd_kafka_destroy(host_);
        }
    }
    RegistryMockCluster(const RegistryMockCluster&) = delete;
    RegistryMockCluster& operator=(const RegistryMockCluster&) = delete;
    [[nodiscard]] std::string brokers() const { return bootstrap_; }
    void create_topic(const std::string& topic) {
        rd_kafka_mock_topic_create(cluster_, topic.c_str(), 1, 1);
    }

private:
    rd_kafka_t* host_{nullptr};
    rd_kafka_mock_cluster_t* cluster_{nullptr};
    std::string bootstrap_;
};

BuildContext ctx_for(const std::string& brokers,
                     const std::string& topic,
                     std::map<std::string, std::string> extra) {
    BuildContext ctx;
    ctx.params = std::move(extra);
    ctx.params["brokers"] = brokers;
    ctx.params["topic"] = topic;
    return ctx;
}

struct Runtime {
    MetricsRegistry metrics;
    RuntimeContext ctx{OperatorId{7}, "kafka_string", nullptr, &metrics};
};

std::vector<std::string> drain_strings(Source<std::string>& src,
                                       std::size_t expected,
                                       std::chrono::milliseconds deadline) {
    std::vector<std::string> out;
    BoundedChannel<StreamElement<std::string>> ch(64);
    Emitter<std::string> em(&ch);
    const auto end = std::chrono::steady_clock::now() + deadline;
    while (out.size() < expected && std::chrono::steady_clock::now() < end) {
        src.produce(em);
        while (auto el = ch.try_pop()) {
            if (!el->is_data()) {
                continue;
            }
            for (auto& r : el->as_data()) {
                out.push_back(std::move(r.value()));
            }
        }
    }
    return out;
}

std::vector<KafkaMessage> drain_raw(KafkaSource& src,
                                    std::size_t expected,
                                    std::chrono::milliseconds deadline) {
    std::vector<KafkaMessage> out;
    BoundedChannel<StreamElement<KafkaMessage>> ch(64);
    Emitter<KafkaMessage> em(&ch);
    const auto end = std::chrono::steady_clock::now() + deadline;
    while (out.size() < expected && std::chrono::steady_clock::now() < end) {
        src.produce(em);
        while (auto el = ch.try_pop()) {
            if (!el->is_data()) {
                continue;
            }
            for (auto& r : el->as_data()) {
                out.push_back(std::move(r.value()));
            }
        }
    }
    return out;
}

void produce_raw(const std::string& brokers,
                 const std::string& topic,
                 std::vector<std::string> payloads) {
    KafkaSink::Options o;
    o.brokers = brokers;
    o.topic = topic;
    Runtime rt;
    KafkaSink sink(o);
    sink.attach_runtime(&rt.ctx);
    sink.open();
    Batch<KafkaMessage> b;
    for (auto& p : payloads) {
        b.emplace(KafkaMessage{std::move(p)});
    }
    sink.on_data(b);
    sink.flush();
    sink.close();
}

TEST(KafkaRegistryFormats, AvroRowsRoundTripThroughTheBrokerAndTheRegistry) {
    RegistryMockCluster mock;
    mock.create_topic("orders");
    schema_registry::test::FakeRegistry reg;

    // Sink: the planner's params for a format='avro' table. Building it
    // registers the derived schema under "orders-value".
    auto sink =
        kafka::build_string_sink(ctx_for(mock.brokers(),
                                         "orders",
                                         {{"format", "avro"},
                                          {"schema_registry_url", reg.url()},
                                          {"schema_columns", "id:i64;name:str;amount:dec_18_2"}}));
    ASSERT_EQ(reg.schema_count(), 1u);
    Runtime srt;
    sink->attach_runtime(&srt.ctx);
    sink->open();
    Batch<std::string> rows;
    rows.emplace(R"({"amount":12.50,"id":1,"name":"a"})");
    rows.emplace(R"({"amount":-0.05,"id":2,"name":null,"__row_kind":"insert"})");
    sink->on_data(rows);
    sink->flush();
    sink->close();

    // What landed is Confluent-framed: magic byte + the registered id.
    {
        KafkaSource::Options o;
        o.brokers = mock.brokers();
        o.topic = "orders";
        o.group_id = "raw-check";
        Runtime rt;
        KafkaSource raw(o);
        raw.attach_runtime(&rt.ctx);
        raw.open();
        const auto msgs = drain_raw(raw, 2, 15s);
        raw.close();
        ASSERT_EQ(msgs.size(), 2u);
        const auto f = schema_registry::parse_frame(msgs[0].payload, false);
        ASSERT_TRUE(f.has_value());
        EXPECT_EQ(f->schema_id, 1);
        EXPECT_EQ(schema_registry::Client(schema_registry::ClientOptions{.url = reg.url()})
                      .schema_by_id(1)
                      .type,
                  schema_registry::SchemaType::Avro);
    }

    // Source: the same format decodes them back to the channel's JSON text.
    auto src = kafka::build_string_source(
        ctx_for(mock.brokers(),
                "orders",
                {{"format", "avro"}, {"schema_registry_url", reg.url()}, {"group_id", "decoded"}}));
    Runtime rt;
    src->attach_runtime(&rt.ctx);
    src->open();
    const auto out = drain_strings(*src, 2, 15s);
    src->close();
    ASSERT_EQ(out.size(), 2u);
    EXPECT_EQ(out[0], R"({"amount":"12.50","id":1,"name":"a"})");
    EXPECT_EQ(out[1], R"({"amount":"-0.05","id":2,"name":null})");
}

TEST(KafkaRegistryFormats, UndecodableMessagesFailTheSourceOrAreSkippedByPolicy) {
    RegistryMockCluster mock;
    mock.create_topic("mixed");
    schema_registry::test::FakeRegistry reg;
    auto client =
        std::make_shared<schema_registry::Client>(schema_registry::ClientOptions{.url = reg.url()});
    schema_registry::FormatOptions fo;
    fo.format = schema_registry::Format::Avro;
    fo.subject = "mixed-value";
    fo.columns = "id:i64";
    auto enc = schema_registry::make_encoder(fo, client);
    produce_raw(mock.brokers(),
                "mixed",
                {"{\"id\":1} plain json, not framed", enc->from_json(R"({"id":2})")});

    {
        auto src = kafka::build_string_source(ctx_for(
            mock.brokers(),
            "mixed",
            {{"format", "avro"}, {"schema_registry_url", reg.url()}, {"group_id", "strict"}}));
        Runtime rt;
        src->attach_runtime(&rt.ctx);
        src->open();
        try {
            drain_strings(*src, 1, 15s);
            FAIL() << "decode_error defaults to fail";
        } catch (const std::runtime_error& e) {
            const std::string what = e.what();
            EXPECT_NE(what.find("topic 'mixed'"), std::string::npos) << what;
            EXPECT_NE(what.find("offset 0"), std::string::npos) << what;
            EXPECT_NE(what.find("magic byte"), std::string::npos) << what;
        }
        src->close();
    }
    {
        auto src = kafka::build_string_source(ctx_for(mock.brokers(),
                                                      "mixed",
                                                      {{"format", "avro"},
                                                       {"schema_registry_url", reg.url()},
                                                       {"group_id", "lenient"},
                                                       {"decode_error", "skip"}}));
        Runtime rt;
        src->attach_runtime(&rt.ctx);
        src->open();
        const auto out = drain_strings(*src, 1, 15s);
        src->close();
        ASSERT_EQ(out.size(), 1u);
        EXPECT_EQ(out[0], R"({"id":2})");
    }
}

TEST(KafkaRegistryFormats, MisconfigurationIsRefusedAtBuildTime) {
    schema_registry::test::FakeRegistry reg;
    // A registry format without its URL.
    try {
        kafka::build_string_source(ctx_for("b:9092", "t", {{"format", "avro"}}));
        FAIL() << "expected a refusal";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string(e.what()).find("schema_registry_url"), std::string::npos) << e.what();
    }
    // An unknown decode_error policy.
    EXPECT_THROW(
        kafka::build_string_source(ctx_for(
            "b:9092",
            "t",
            {{"format", "avro"}, {"schema_registry_url", reg.url()}, {"decode_error", "maybe"}})),
        std::runtime_error);
    // A sink talks to the registry when built, so an unreachable one fails there, not on the first
    // row.
    EXPECT_THROW(kafka::build_string_sink(ctx_for("b:9092",
                                                  "t",
                                                  {{"format", "avro"},
                                                   {"schema_registry_url", "http://127.0.0.1:1"},
                                                   {"schema_registry_timeout_ms", "300"},
                                                   {"schema_columns", "id:i64"}})),
                 std::runtime_error);
    // A source is lazy (ids arrive with the data), so the same URL builds fine.
    EXPECT_NE(
        kafka::build_string_source(ctx_for(
            "b:9092", "t", {{"format", "avro"}, {"schema_registry_url", "http://127.0.0.1:1"}})),
        nullptr);
    // Plain tables build neither codec and accept no decode_error nonsense either way.
    EXPECT_NE(kafka::build_string_sink(ctx_for("b:9092", "t", {})), nullptr);
    // The formats the connector declares include the registry ones this build compiled in.
    const auto formats = kafka::string_channel_formats();
    EXPECT_NE(std::find(formats.begin(), formats.end(), "avro"), formats.end());
    EXPECT_NE(std::find(formats.begin(), formats.end(), "json-schema"), formats.end());
    EXPECT_NE(std::find(formats.begin(), formats.end(), "json"), formats.end());
}

}  // namespace

#else
TEST(KafkaRegistryFormats, NotAvailableInThisBuild) {
    GTEST_SKIP() << "needs rdkafka_mock, impls/schema_registry and avro-cpp";
}
#endif
