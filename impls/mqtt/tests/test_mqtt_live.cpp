// MQTT LIVE integration test. SKIPPED unless CLINK_MQTT_TEST_URL is set (e.g.
// mqtt://localhost:1883). Proves against a real broker: a sink->source round-trip
// delivers every record verbatim; include_topic wraps each message as a JSON
// envelope; and a retained message is delivered to a fresh subscriber. The TLS
// case is gated separately on CLINK_MQTT_TLS_TEST_URL.

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <set>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"

#ifdef CLINK_HAS_MQTT
#include "clink/mqtt/mqtt_client.hpp"
#include "clink/mqtt/mqtt_sink.hpp"
#include "clink/mqtt/mqtt_source.hpp"
#endif

#ifdef CLINK_HAS_MQTT

using clink::Batch;
using clink::Emitter;
using clink::StreamElement;
using clink::mqtt::ConnectOptions;
using clink::mqtt::MqttSink;
using clink::mqtt::MqttSinkOptions;
using clink::mqtt::MqttSource;
using clink::mqtt::MqttSourceOptions;

namespace {

bool mqtt_configured() {
    return std::getenv("CLINK_MQTT_TEST_URL") != nullptr;
}

// Parse [mqtt://]host:port into host + port.
void parse_host_port(std::string url, std::string& host, std::uint16_t& port) {
    if (auto p = url.find("://"); p != std::string::npos) {
        url = url.substr(p + 3);
    }
    if (auto slash = url.find('/'); slash != std::string::npos) {
        url = url.substr(0, slash);
    }
    if (auto c = url.rfind(':'); c != std::string::npos) {
        host = url.substr(0, c);
        port = static_cast<std::uint16_t>(std::stoi(url.substr(c + 1)));
    } else {
        host = url;
        port = 1883;
    }
}

// A distinct client_id per call keeps clients from colliding (an MQTT broker
// disconnects a duplicate id) and clean_session keeps tests isolated.
ConnectOptions mqtt_conn() {
    static int n = 0;
    ConnectOptions o;
    parse_host_port(std::getenv("CLINK_MQTT_TEST_URL"), o.host, o.port);
    o.client_id =
        "clink-it-" + std::to_string(static_cast<long>(::getpid())) + "-" + std::to_string(n++);
    o.clean_session = true;
    return o;
}

std::string unique_topic() {
    static int n = 0;
    return "clink/it/" + std::to_string(static_cast<long>(::getpid())) + "/" + std::to_string(n++);
}

struct Captured {
    std::vector<std::string> values;
};
Emitter<std::string> capturing(Captured& sink) {
    return Emitter<std::string>{[&sink](StreamElement<std::string> e) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                sink.values.push_back(r.value());
            }
        }
        return true;
    }};
}

void drain(MqttSource& src, Captured& cap, std::size_t want, int timeout_ms) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{timeout_ms};
    while (cap.values.size() < want && std::chrono::steady_clock::now() < deadline) {
        auto em = capturing(cap);
        src.produce(em);
    }
}

void publish_records(const std::string& topic, std::size_t n, bool retain = false) {
    MqttSinkOptions o;
    o.conn = mqtt_conn();
    o.topic = topic;
    o.qos = 1;
    o.retain = retain;
    MqttSink sink(std::move(o));
    sink.open();
    Batch<std::string> b;
    for (std::size_t i = 0; i < n; ++i) {
        b.emplace(R"({"i":)" + std::to_string(i) + "}");
    }
    sink.on_data(b);
    sink.flush();
    sink.close();
}

MqttSourceOptions source_opts(const std::string& topic) {
    MqttSourceOptions o;
    o.conn = mqtt_conn();
    o.topic = topic;
    o.qos = 1;
    o.block = std::chrono::milliseconds{200};
    return o;
}

}  // namespace

TEST(MqttLive, SinkThenSourceRoundTrip) {
    if (!mqtt_configured()) {
        GTEST_SKIP() << "set CLINK_MQTT_TEST_URL (e.g. mqtt://localhost:1883)";
    }
    const std::string topic = unique_topic();
    constexpr std::size_t kN = 20;

    // Subscribe FIRST (a fresh MQTT subscription only receives messages published
    // after it is active), let the SUBSCRIBE settle, then publish and drain.
    MqttSource src(source_opts(topic));
    src.open();
    std::this_thread::sleep_for(std::chrono::milliseconds{200});
    publish_records(topic, kN);

    Captured cap;
    drain(src, cap, kN, /*timeout_ms=*/15000);
    src.close();

    EXPECT_EQ(cap.values.size(), kN) << "every published record should arrive exactly once";
    std::set<std::string> got(cap.values.begin(), cap.values.end());
    EXPECT_EQ(got.size(), kN) << "every record should round-trip back verbatim";
    for (std::size_t i = 0; i < kN; ++i) {
        EXPECT_EQ(got.count(R"({"i":)" + std::to_string(i) + "}"), 1u) << "missing record " << i;
    }
}

TEST(MqttLive, IncludeTopicWrapsEnvelope) {
    if (!mqtt_configured()) {
        GTEST_SKIP() << "set CLINK_MQTT_TEST_URL";
    }
    const std::string topic = unique_topic();
    auto o = source_opts(topic);
    o.include_topic = true;
    MqttSource src(std::move(o));
    src.open();
    std::this_thread::sleep_for(std::chrono::milliseconds{200});
    publish_records(topic, 1);

    Captured cap;
    drain(src, cap, 1, /*timeout_ms=*/15000);
    src.close();

    ASSERT_EQ(cap.values.size(), 1u);
    auto j = clink::config::parse(cap.values[0]);
    ASSERT_TRUE(j.is_object());
    EXPECT_EQ(j.as_object().at("topic").as_string(), topic);
    EXPECT_EQ(j.as_object().at("payload").as_string(), R"({"i":0})");
}

TEST(MqttLive, RetainedMessageDeliveredToNewSubscriber) {
    if (!mqtt_configured()) {
        GTEST_SKIP() << "set CLINK_MQTT_TEST_URL";
    }
    const std::string topic = unique_topic();
    // Publish a retained message BEFORE anyone subscribes; the broker stores it
    // and delivers it to the next subscriber of this topic.
    publish_records(topic, 1, /*retain=*/true);

    MqttSource src(source_opts(topic));
    src.open();
    Captured cap;
    drain(src, cap, 1, /*timeout_ms=*/15000);
    src.close();

    ASSERT_GE(cap.values.size(), 1u) << "the retained message should be delivered on subscribe";
    EXPECT_EQ(cap.values[0], R"({"i":0})");

    // Clear the retained message (empty retained publish) so the topic does not
    // leak state to other runs sharing this broker.
    MqttSinkOptions clr;
    clr.conn = mqtt_conn();
    clr.topic = topic;
    clr.retain = true;
    MqttSink sink(std::move(clr));
    sink.open();
    Batch<std::string> empty_b;
    empty_b.emplace("");
    sink.on_data(empty_b);
    sink.flush();
    sink.close();
}

// TLS round-trip. SKIPPED unless CLINK_MQTT_TLS_TEST_URL (host:port) points at a
// TLS-enabled broker. CLINK_MQTT_TLS_CA may give the CA cert (PEM); without it,
// verification is skipped (self-signed dev cert: encrypt but do not authenticate).
TEST(MqttLive, TlsConnectionRoundTrip) {
    const char* url = std::getenv("CLINK_MQTT_TLS_TEST_URL");
    if (url == nullptr) {
        GTEST_SKIP() << "set CLINK_MQTT_TLS_TEST_URL to a TLS broker host:port";
    }
    const std::string topic = unique_topic();

    auto make_conn = [&]() {
        ConnectOptions o;
        parse_host_port(url, o.host, o.port);
        static int n = 0;
        o.client_id = "clink-tls-it-" + std::to_string(static_cast<long>(::getpid())) + "-" +
                      std::to_string(n++);
        o.clean_session = true;
        o.tls = true;
        if (const char* ca = std::getenv("CLINK_MQTT_TLS_CA"); ca != nullptr) {
            o.tls_ca = ca;
        }
        o.tls_verify = false;  // self-signed dev cert: encrypt, do not authenticate
        return o;
    };

    MqttSourceOptions so;
    so.conn = make_conn();
    so.topic = topic;
    so.qos = 1;
    so.block = std::chrono::milliseconds{200};
    MqttSource src(std::move(so));
    src.open();
    std::this_thread::sleep_for(std::chrono::milliseconds{200});

    MqttSinkOptions ko;
    ko.conn = make_conn();
    ko.topic = topic;
    ko.qos = 1;
    MqttSink sink(std::move(ko));
    sink.open();
    Batch<std::string> b;
    b.emplace("hello-tls");
    sink.on_data(b);
    sink.flush();
    sink.close();

    Captured cap;
    drain(src, cap, 1, /*timeout_ms=*/15000);
    src.close();
    ASSERT_EQ(cap.values.size(), 1u) << "an encrypted round-trip should deliver the message";
    EXPECT_EQ(cap.values[0], "hello-tls");
}

#endif  // CLINK_HAS_MQTT
