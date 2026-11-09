// MQTT LIVE integration test. SKIPPED unless CLINK_MQTT_TEST_URL is set (e.g.
// mqtt://localhost:1883). Proves against a real broker: a sink->source round-trip
// delivers every record verbatim; include_topic wraps each message as a JSON
// envelope; and a retained message is delivered to a fresh subscriber. The TLS
// case is gated separately on CLINK_MQTT_TLS_TEST_URL.
//
// MqttSourceAckLive is the source's recovery contract against the broker: a
// message is acknowledged only once a completed checkpoint covers it, so a
// crash, an aborted checkpoint or a close delivers again exactly what no
// completed checkpoint covers, and one checkpoint interval reads at most the
// session's in-flight window. scripts/brokers-live.sh runs it and fails on a
// skip.

#include <algorithm>
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
#include "clink/state/in_memory_state_backend.hpp"

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

void publish_records(const std::string& topic, std::size_t n, bool retain = false, int qos = 1) {
    MqttSinkOptions o;
    o.conn = mqtt_conn();
    o.topic = topic;
    o.qos = qos;
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
    // The session persists; this lets the test broker forget it.
    o.session_expiry_s = 300;
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

// An MQTT broker drops a client it has heard nothing from for one and a half
// keepalives, and libmosquitto sends the keepalive's ping from its network
// loop, which the sink ran only to flush a batch. A sink with nothing to publish
// was dropped, and its next flush failed the job: a job whose sink publishes
// rarely restarted every ninety seconds of quiet at the default keepalive.
//
// With barriers arriving, as a checkpointing job's do, the sink stays connected
// however long it idles: the barriers service the connection, and it is never
// reconnected.
TEST(MqttLive, AnIdleSinkStaysConnectedAcrossItsBarriers) {
    if (!mqtt_configured()) {
        GTEST_SKIP() << "set CLINK_MQTT_TEST_URL";
    }
    const std::string topic = unique_topic();
    MqttSource src(source_opts(topic));
    src.open();
    std::this_thread::sleep_for(std::chrono::milliseconds{200});

    MqttSinkOptions o;
    o.conn = mqtt_conn();
    o.conn.keepalive = 5;  // the broker drops it after 7.5 s of silence
    o.topic = topic;
    o.qos = 1;
    MqttSink sink(std::move(o));
    sink.open();
    Batch<std::string> first;
    first.emplace(R"({"i":0})");
    sink.on_data(first);
    sink.flush();
    // Twelve seconds of nothing to publish, a barrier every half second.
    const auto until = std::chrono::steady_clock::now() + std::chrono::seconds{12};
    std::uint64_t id = 1;
    while (std::chrono::steady_clock::now() < until) {
        sink.on_barrier(clink::CheckpointBarrier{clink::CheckpointId{id++}});
        std::this_thread::sleep_for(std::chrono::milliseconds{500});
    }
    Batch<std::string> second;
    second.emplace(R"({"i":1})");
    sink.on_data(second);
    ASSERT_NO_THROW(sink.flush()) << "the idle sink's connection did not survive its barriers";
    EXPECT_EQ(sink.reconnects(), 0u)
        << "the barriers did not keep the connection alive: the broker dropped it and the "
           "flush had to reconnect";
    sink.close();

    Captured cap;
    drain(src, cap, 2, /*timeout_ms=*/15000);
    src.close();
    EXPECT_EQ(cap.values, (std::vector<std::string>{R"({"i":0})", R"({"i":1})"}));
}

// With no barrier for longer than the keepalive (checkpointing off, or an
// interval longer than it), the broker drops the idle sink. Nothing was in
// flight on the connection, so the next batch reconnects and is delivered,
// rather than failing the job.
TEST(MqttLive, ASinkDroppedWhileIdleReconnectsForItsNextBatch) {
    if (!mqtt_configured()) {
        GTEST_SKIP() << "set CLINK_MQTT_TEST_URL";
    }
    const std::string topic = unique_topic();
    MqttSource src(source_opts(topic));
    src.open();
    std::this_thread::sleep_for(std::chrono::milliseconds{200});

    MqttSinkOptions o;
    o.conn = mqtt_conn();
    o.conn.keepalive = 5;
    o.topic = topic;
    o.qos = 1;
    MqttSink sink(std::move(o));
    sink.open();
    Batch<std::string> first;
    first.emplace(R"({"i":0})");
    sink.on_data(first);
    sink.flush();
    // Past one and a half keepalives with no barrier: the broker drops it. The
    // source's own produce() keeps its connection alive meanwhile.
    Captured cap;
    drain(src, cap, 1, /*timeout_ms=*/15000);
    const auto until = std::chrono::steady_clock::now() + std::chrono::seconds{12};
    while (std::chrono::steady_clock::now() < until) {
        auto em = capturing(cap);
        src.produce(em);
    }
    Batch<std::string> second;
    second.emplace(R"({"i":1})");
    sink.on_data(second);
    ASSERT_NO_THROW(sink.flush()) << "a connection dropped while idle failed the next flush";
    EXPECT_EQ(sink.reconnects(), 1u) << "the broker should have dropped the idle sink once";
    sink.close();

    drain(src, cap, 2, /*timeout_ms=*/15000);
    src.close();
    EXPECT_EQ(cap.values, (std::vector<std::string>{R"({"i":0})", R"({"i":1})"}));
}

namespace {

clink::InMemoryStateBackend ack_backend;

void seal(MqttSource& src, std::uint64_t id) {
    src.snapshot_offset(ack_backend, clink::OperatorId{1}, clink::CheckpointId{id});
}

// Every record the source emits within `for_ms`, however many.
std::vector<std::string> everything_within(MqttSource& src, int for_ms) {
    Captured cap;
    const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds{for_ms};
    while (std::chrono::steady_clock::now() < until) {
        auto em = capturing(cap);
        src.produce(em);
    }
    return cap.values;
}

std::string rec(std::size_t i) {
    return R"({"i":)" + std::to_string(i) + "}";
}

std::vector<std::string> recs(std::size_t from, std::size_t to) {
    std::vector<std::string> out;
    for (std::size_t i = from; i < to; ++i) {
        out.push_back(rec(i));
    }
    return out;
}

std::vector<std::string> sorted(std::vector<std::string> v) {
    std::sort(v.begin(), v.end());
    return v;
}

// Subscribe, then let the SUBSCRIBE settle before anything is published.
void open_settled(MqttSource& src) {
    src.open();
    std::this_thread::sleep_for(std::chrono::milliseconds{200});
}

}  // namespace

// A crash before any checkpoint completes leaves every message with the
// session; once a checkpoint covering them completes and its acknowledgements
// go out, the session holds nothing more.
TEST(MqttSourceAckLive, NothingIsAcknowledgedBeforeACheckpointCompletes) {
    if (!mqtt_configured()) {
        GTEST_SKIP() << "set CLINK_MQTT_TEST_URL";
    }
    const std::string topic = unique_topic();
    const auto opts = source_opts(topic);
    {
        MqttSource src(opts);
        open_settled(src);
        publish_records(topic, 5);
        Captured cap;
        drain(src, cap, 5, 15000);
        ASSERT_EQ(cap.values.size(), 5u);
        seal(src, 1);
        EXPECT_EQ(src.acknowledged_total(), 0u);
        // destroyed without close(): a crash
    }
    MqttSource again(opts);
    again.open();
    Captured cap;
    drain(again, cap, 5, 15000);
    EXPECT_EQ(sorted(cap.values), recs(0, 5)) << "the crash lost what no checkpoint covered";
    seal(again, 2);
    again.notify_checkpoint_complete(clink::CheckpointId{2});
    everything_within(again, 500);
    EXPECT_EQ(again.acknowledged_total(), 5u);
    again.close();

    MqttSource after(opts);
    after.open();
    EXPECT_TRUE(everything_within(after, 1500).empty())
        << "a message a completed checkpoint covers was delivered again";
    after.close();
}

TEST(MqttSourceAckLive, ACrashAfterACompletionDeliversOnlyWhatItDidNotCover) {
    if (!mqtt_configured()) {
        GTEST_SKIP() << "set CLINK_MQTT_TEST_URL";
    }
    const std::string topic = unique_topic();
    const auto opts = source_opts(topic);
    {
        MqttSource src(opts);
        open_settled(src);
        publish_records(topic, 3);
        Captured cap;
        drain(src, cap, 3, 15000);
        seal(src, 1);
        publish_records(topic, 5);  // a second interval: payloads i=0 to i=4
        drain(src, cap, 8, 15000);
        ASSERT_EQ(cap.values.size(), 8u);
        src.notify_checkpoint_complete(clink::CheckpointId{1});
        everything_within(src, 300);  // the acknowledgements go out
        EXPECT_EQ(src.acknowledged_total(), 3u);
        // destroyed without close(): a crash
    }
    MqttSource again(opts);
    again.open();
    Captured cap;
    drain(again, cap, 5, 15000);
    EXPECT_EQ(sorted(cap.values), recs(0, 5))
        << "after the crash the session should hold the second interval only";
    EXPECT_TRUE(everything_within(again, 1000).empty());
    again.close();
}

// MQTT has no negative acknowledgement: an aborted checkpoint reconnects, and
// the new connection delivers its messages again. Nothing of them is
// acknowledged until a later checkpoint covering the new delivery completes.
TEST(MqttSourceAckLive, AnAbortedIntervalIsDeliveredAgain) {
    if (!mqtt_configured()) {
        GTEST_SKIP() << "set CLINK_MQTT_TEST_URL";
    }
    const std::string topic = unique_topic();
    const auto opts = source_opts(topic);
    MqttSource src(opts);
    open_settled(src);
    publish_records(topic, 3);
    Captured cap;
    drain(src, cap, 3, 15000);
    seal(src, 1);
    src.notify_checkpoint_aborted(clink::CheckpointId{1});
    drain(src, cap, 6, 15000);
    ASSERT_EQ(cap.values.size(), 6u) << "the aborted interval was not delivered again";
    EXPECT_EQ(src.reconnects(), 1u);
    EXPECT_EQ(src.acknowledged_total(), 0u);
    seal(src, 2);
    src.notify_checkpoint_complete(clink::CheckpointId{2});
    everything_within(src, 300);
    EXPECT_EQ(src.acknowledged_total(), 3u);
    src.close();

    MqttSource after(opts);
    after.open();
    EXPECT_TRUE(everything_within(after, 1500).empty());
    after.close();
}

// close() acknowledges what completed checkpoints cover and leaves the open
// interval with the session for the next connection.
TEST(MqttSourceAckLive, CloseAcknowledgesTheCompletedAndLeavesTheRest) {
    if (!mqtt_configured()) {
        GTEST_SKIP() << "set CLINK_MQTT_TEST_URL";
    }
    const std::string topic = unique_topic();
    const auto opts = source_opts(topic);
    {
        MqttSource src(opts);
        open_settled(src);
        publish_records(topic, 2);
        Captured cap;
        drain(src, cap, 2, 15000);
        seal(src, 1);
        src.notify_checkpoint_complete(clink::CheckpointId{1});
        src.close();  // no produce() turn after the completion: close sends them
    }
    {
        MqttSource src(opts);
        src.open();
        EXPECT_TRUE(everything_within(src, 1500).empty())
            << "close() did not acknowledge what a completed checkpoint covered";
        publish_records(topic, 1);
        Captured cap;
        drain(src, cap, 1, 15000);
        seal(src, 2);
        src.close();  // checkpoint 2 never completes
    }
    MqttSource after(opts);
    after.open();
    Captured cap;
    drain(after, cap, 1, 15000);
    EXPECT_EQ(cap.values, (std::vector<std::string>{rec(0)}));
    after.close();
}

// The broker sends at most receive_maximum messages unacknowledged, and the
// source acknowledges only at completion, so that many is what one checkpoint
// interval reads.
TEST(MqttSourceAckLive, TheReceiveMaximumIsWhatOneCheckpointCanRead) {
    if (!mqtt_configured()) {
        GTEST_SKIP() << "set CLINK_MQTT_TEST_URL";
    }
    const std::string topic = unique_topic();
    auto opts = source_opts(topic);
    opts.receive_maximum = 50;
    MqttSource src(opts);
    open_settled(src);
    publish_records(topic, 200);
    EXPECT_EQ(everything_within(src, 2000).size(), 50u);
    seal(src, 1);
    src.notify_checkpoint_complete(clink::CheckpointId{1});
    EXPECT_EQ(everything_within(src, 2000).size(), 50u)
        << "the completion should release the next window and no more";
    src.close();
}

// MQTT 3.1.1 has no Receive Maximum: the broker's own in-flight limit decides
// how much one checkpoint interval reads. The test broker runs mosquitto's
// default, 20.
TEST(MqttSourceAckLive, UnderMqtt311TheBrokersInFlightLimitIsWhatOneCheckpointCanRead) {
    if (!mqtt_configured()) {
        GTEST_SKIP() << "set CLINK_MQTT_TEST_URL";
    }
    const std::string topic = unique_topic();
    auto opts = source_opts(topic);
    opts.protocol_version = clink::mqtt::protocol::kMqtt311;
    MqttSource src(opts);
    open_settled(src);
    publish_records(topic, 100);
    EXPECT_EQ(everything_within(src, 2000).size(), 20u);
    seal(src, 1);
    src.notify_checkpoint_complete(clink::CheckpointId{1});
    EXPECT_EQ(everything_within(src, 2000).size(), 20u);
    src.close();
}

// QoS 2: the PUBREC is what hands a message over, so it waits for completion
// as a PUBACK does, and a crash before it delivers the message again.
TEST(MqttSourceAckLive, AQos2MessageIsDeliveredAgainUntilItsCheckpointCompletes) {
    if (!mqtt_configured()) {
        GTEST_SKIP() << "set CLINK_MQTT_TEST_URL";
    }
    const std::string topic = unique_topic();
    auto opts = source_opts(topic);
    opts.qos = 2;
    {
        MqttSource src(opts);
        open_settled(src);
        publish_records(topic, 2, /*retain=*/false, /*qos=*/2);
        Captured cap;
        drain(src, cap, 2, 15000);
        ASSERT_EQ(cap.values.size(), 2u);
        seal(src, 1);
        // destroyed without close(): a crash
    }
    MqttSource again(opts);
    again.open();
    Captured cap;
    drain(again, cap, 2, 15000);
    EXPECT_EQ(sorted(cap.values), recs(0, 2));
    seal(again, 2);
    again.notify_checkpoint_complete(clink::CheckpointId{2});
    everything_within(again, 500);  // PUBREC, then the PUBREL is answered
    again.close();

    MqttSource after(opts);
    after.open();
    EXPECT_TRUE(everything_within(after, 1500).empty());
    after.close();
}

// Under MQTT 5 retained messages come only with a subscription that is new, so
// a source that resumes its session does not receive a retained message again
// once a completed checkpoint covered it.
TEST(MqttSourceAckLive, AResumedSessionDoesNotReceiveARetainedMessageAgain) {
    if (!mqtt_configured()) {
        GTEST_SKIP() << "set CLINK_MQTT_TEST_URL";
    }
    const std::string topic = unique_topic();
    publish_records(topic, 1, /*retain=*/true);
    const auto opts = source_opts(topic);
    {
        MqttSource src(opts);
        src.open();
        Captured cap;
        drain(src, cap, 1, 15000);
        ASSERT_EQ(cap.values.size(), 1u);
        seal(src, 1);
        src.notify_checkpoint_complete(clink::CheckpointId{1});
        src.close();
    }
    MqttSource again(opts);
    again.open();
    EXPECT_TRUE(everything_within(again, 1500).empty())
        << "the retained message came again with a subscription the session already held";
    again.close();

    // Clear the retained message so the topic leaks nothing to other runs.
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

#endif  // CLINK_HAS_MQTT
