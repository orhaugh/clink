// The MQTT sink's contract against a live broker. SKIPPED unless
// CLINK_MQTT_TEST_URL names a broker (mqtt://host:port); scripts/brokers-live.sh
// runs it against the pinned Mosquitto image and fails when any case is skipped.
// The refusal cases need a broker that denies publishes under clink/denied/,
// as docker/mosquitto.acl has the gate's.
//
// Every case judges by what a subscriber receives, read through the impl's own
// MQTT 5 client (MqttSubscriber), apart from the libmosquitto connection the
// sink publishes on. The cases that need a broker that stops answering, or a
// connection that drops during a flush, connect the sink through a TCP proxy
// (tests/integration/tcp_fault_proxy.hpp).

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/operator_registry.hpp"
#include "clink/core/record.hpp"
#include "clink/mqtt/mqtt_sink.hpp"
#include "clink/mqtt/mqtt_subscriber.hpp"
#include "clink/runtime/runtime_context.hpp"

#include "tests/integration/tcp_fault_proxy.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::mqtt::ConnectOptions;
using clink::mqtt::MqttSink;
using clink::mqtt::MqttSinkOptions;

namespace {

bool mqtt_live() {
    return std::getenv("CLINK_MQTT_TEST_URL") != nullptr;
}

#define REQUIRE_MQTT()                                                              \
    do {                                                                            \
        if (!mqtt_live()) {                                                         \
            GTEST_SKIP() << "set CLINK_MQTT_TEST_URL (e.g. mqtt://localhost:1883)"; \
        }                                                                           \
    } while (0)

ConnectOptions broker() {
    std::string url = std::getenv("CLINK_MQTT_TEST_URL");
    if (const auto p = url.find("://"); p != std::string::npos) {
        url = url.substr(p + 3);
    }
    ConnectOptions o;
    const auto c = url.rfind(':');
    o.host = url.substr(0, c);
    o.port = static_cast<std::uint16_t>(std::stoi(url.substr(c + 1)));
    return o;
}

std::string unique(const std::string& what) {
    static std::atomic<int> n{0};
    return what + "-" + std::to_string(static_cast<long>(::getpid())) + "-" +
           std::to_string(n.fetch_add(1));
}

// A subscriber on its own thread that takes every message on a topic,
// acknowledging each.
class Collector {
public:
    explicit Collector(const std::string& topic) {
        clink::mqtt::SubscriberOptions o;
        o.conn = broker();
        o.conn.client_id = unique("clink-sinkc-collector");
        o.session_expiry_s = 60;  // the subscriber needs one; a minute is plenty here
        o.name = "collector";
        sub_ = std::make_unique<clink::mqtt::MqttSubscriber>(o);
        (void)sub_->connect();
        (void)sub_->subscribe(topic, 2);
        thread_ = std::thread([this] { run(); });
    }
    ~Collector() {
        stop_.store(true);
        thread_.join();
        sub_->disconnect();
    }
    Collector(const Collector&) = delete;
    Collector& operator=(const Collector&) = delete;

    std::vector<std::string> payloads() {
        const std::lock_guard<std::mutex> lock(mu_);
        return payloads_;
    }
    // Waits up to `limit` until at least `n` messages have come.
    bool wait_for(std::size_t n,
                  std::chrono::milliseconds limit = std::chrono::milliseconds{5000}) {
        const auto until = std::chrono::steady_clock::now() + limit;
        while (std::chrono::steady_clock::now() < until) {
            if (payloads().size() >= n) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds{20});
        }
        return payloads().size() >= n;
    }

private:
    void run() {
        while (!stop_.load()) {
            std::vector<clink::mqtt::Delivery> got;
            if (!sub_->poll(std::chrono::milliseconds{50}, got)) {
                return;
            }
            for (const auto& d : got) {
                if (d.qos > 0) {
                    (void)sub_->acknowledge(d.packet_id, d.qos);
                }
                const std::lock_guard<std::mutex> lock(mu_);
                payloads_.push_back(d.payload);
            }
        }
    }

    std::unique_ptr<clink::mqtt::MqttSubscriber> sub_;
    std::thread thread_;
    std::atomic<bool> stop_{false};
    std::mutex mu_;
    std::vector<std::string> payloads_;
};

MqttSinkOptions sink_opts(const std::string& topic) {
    MqttSinkOptions o;
    o.conn = broker();
    o.conn.client_id = unique("clink-sinkc-sink");
    o.topic = topic;
    return o;
}

MqttSinkOptions proxied_opts(const std::string& topic, const clink::itest::TcpFaultProxy& p) {
    auto o = sink_opts(topic);
    o.conn.host = "127.0.0.1";
    o.conn.port = p.port();
    return o;
}

Batch<std::string> batch(const std::vector<std::string>& records) {
    Batch<std::string> b;
    for (const auto& r : records) {
        b.emplace(std::string{r});
    }
    return b;
}

template <typename F>
std::string thrown(F&& f) {
    try {
        std::forward<F>(f)();
    } catch (const std::exception& e) {
        return e.what();
    }
    return "";
}

const CheckpointBarrier kBarrier{clink::CheckpointId{1}};

struct Attached {
    explicit Attached(MqttSink& s) : sink(s) {
        ctx = std::make_unique<clink::RuntimeContext>(
            clink::OperatorId{84}, "mqtt_sink", nullptr, nullptr);
        ctx->set_cancel_signal(clink::CancelSignal{cancel, nullptr});
        s.attach_runtime(ctx.get());
    }
    ~Attached() { sink.attach_runtime(nullptr); }
    Attached(const Attached&) = delete;
    Attached& operator=(const Attached&) = delete;
    void cancel_after(std::chrono::milliseconds wait) {
        canceller = std::thread([this, wait] {
            std::this_thread::sleep_for(wait);
            cancel->store(true);
        });
    }
    MqttSink& sink;
    std::shared_ptr<std::atomic<bool>> cancel = std::make_shared<std::atomic<bool>>(false);
    std::unique_ptr<clink::RuntimeContext> ctx;
    std::thread canceller;
};

}  // namespace

TEST(MqttSinkContractLive, MessagesArriveExactlyAsWrittenAtEachQos) {
    REQUIRE_MQTT();
    const std::vector<std::string> records = {R"({"id":1,"amount":12345678901234567890.5})",
                                              std::string{"nul\0inside", 10},
                                              "caf\xc3\xa9 \xf0\x9f\x98\x80",
                                              "",
                                              std::string(100U << 10, 'x')};
    for (const auto version : {clink::mqtt::protocol::kMqtt5, clink::mqtt::protocol::kMqtt311}) {
        for (const int qos : {0, 1, 2}) {
            const auto topic = "clink/sinkc/" + unique("exact");
            Collector c(topic);
            auto o = sink_opts(topic);
            o.qos = qos;
            o.protocol_version = version;
            MqttSink sink(std::move(o));
            sink.open();
            sink.on_data(batch(records));
            sink.on_barrier(kBarrier);
            sink.close();
            ASSERT_TRUE(c.wait_for(records.size()))
                << "protocol " << int{version} << " qos " << qos;
            EXPECT_EQ(c.payloads(), records) << "protocol " << int{version} << " qos " << qos;
        }
    }
}

// Under MQTT 5 a broker that refuses a message says so in its acknowledgement
// (the PUBACK at qos 1, the PUBREC at qos 2), and the flush fails once every
// message is answered, naming the broker's reason. The broker denies publishes
// under clink/denied/.
TEST(MqttSinkContractLive, ABrokerThatRefusesAMessageFailsTheFlushNamingItsReason) {
    REQUIRE_MQTT();
    for (const int qos : {1, 2}) {
        auto o = sink_opts("clink/denied/" + unique("refused"));
        o.qos = qos;
        MqttSink sink(std::move(o));
        Attached attached(sink);
        sink.open();
        sink.on_data(batch({"a", "b", "c"}));
        const auto why = thrown([&] { sink.on_barrier(kBarrier); });
        EXPECT_NE(why.find("the broker refused 3 of the flush's 3 messages (Not authorized)"),
                  std::string::npos)
            << "qos " << qos << ": " << why
            << " (the broker must deny publishes under clink/denied/, as docker/mosquitto.acl "
               "has the gate's)";
        EXPECT_NE(thrown([&] { sink.flush(); }).find("the sink failed earlier"), std::string::npos)
            << "qos " << qos;
        sink.close();
    }
}

// Under MQTT 5 a broker that ends a connection says why in its DISCONNECT, and
// the failure carries that reason: here a second connection under the sink's
// client id takes its session over while a flush waits for its
// acknowledgements.
TEST(MqttSinkContractLive, ABrokerThatEndsTheConnectionGivesItsReason) {
    REQUIRE_MQTT();
    const auto topic = "clink/sinkc/" + unique("takeover");
    Collector c(topic);
    auto o = sink_opts(topic);
    o.batch_records = 1000000;
    MqttSink sink(o);
    Attached attached(sink);
    sink.open();
    sink.on_data(batch(std::vector<std::string>(100000, "0123456789")));
    std::thread usurper([&] {
        // Once the broker is taking the flush, connect under the sink's id.
        (void)c.wait_for(1000, std::chrono::milliseconds{10000});
        clink::mqtt::SubscriberOptions u;
        u.conn = o.conn;
        u.session_expiry_s = 1;
        u.name = "usurper";
        clink::mqtt::MqttSubscriber sub(u);
        (void)sub.connect();
        sub.disconnect();
    });
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    usurper.join();
    EXPECT_NE(why.find("the broker ended the connection while waiting for the broker's "
                       "acknowledgements (Session taken over)"),
              std::string::npos)
        << why;
    EXPECT_NE(why.find("of the flush's 100000 messages"), std::string::npos) << why;
    sink.close();
}

// MQTT 3.1.1 has no reason code: the same broker acknowledges a publish it
// refuses and drops it, so the flush succeeds and the message is gone. The case
// holds the limitation the documentation states, and fails if a broker ever
// says otherwise.
TEST(MqttSinkContractLive, UnderMqtt311ABrokerAcknowledgesAMessageItRefuses) {
    REQUIRE_MQTT();
    auto o = sink_opts("clink/denied/" + unique("dropped"));
    o.protocol_version = clink::mqtt::protocol::kMqtt311;
    MqttSink sink(std::move(o));
    Attached attached(sink);
    sink.open();
    sink.on_data(batch({"a", "b", "c"}));
    EXPECT_NO_THROW(sink.on_barrier(kBarrier));
    sink.close();
}

// Acknowledgements that do not come within ack_timeout fail the flush with the
// messages unacknowledged, and they are only that: once the network carries
// again, the broker delivers the messages the sink gave up on.
TEST(MqttSinkContractLive, AcknowledgementsThatDoNotComeLeaveTheMessagesUnknownAndTheyMayLand) {
    REQUIRE_MQTT();
    const auto topic = "clink/sinkc/" + unique("silent");
    Collector c(topic);
    clink::itest::TcpFaultProxy proxy(broker().port, broker().host);
    auto o = proxied_opts(topic, proxy);
    o.ack_timeout = std::chrono::milliseconds{500};
    MqttSink sink(std::move(o));
    sink.open();
    proxy.set_blackhole(true);
    sink.on_data(batch({"a", "b", "c"}));
    const auto t0 = std::chrono::steady_clock::now();
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    const auto took = std::chrono::steady_clock::now() - t0;
    EXPECT_GE(took, std::chrono::milliseconds{500});
    EXPECT_LT(took, std::chrono::seconds{3});
    EXPECT_NE(why.find("within the acknowledgement timeout (500 ms)"), std::string::npos) << why;
    EXPECT_NE(why.find("of the flush's 3 messages, 0 were acknowledged and the other 3 are "
                       "unacknowledged and may or may not have reached the broker"),
              std::string::npos)
        << why;
    EXPECT_NE(thrown([&] { sink.flush(); }).find("failed earlier"), std::string::npos);
    proxy.set_blackhole(false);
    EXPECT_TRUE(c.wait_for(3)) << "the unacknowledged messages reached the broker once the "
                                  "network carried again";
    sink.close();
}

TEST(MqttSinkContractLive, AConnectionDroppedDuringAFlushFailsTheSinkUntilItIsOpenedAgain) {
    REQUIRE_MQTT();
    const auto topic = "clink/sinkc/" + unique("dropped");
    Collector c(topic);
    clink::itest::TcpFaultProxy proxy(broker().port, broker().host);
    MqttSink sink(proxied_opts(topic, proxy));
    sink.open();
    proxy.set_blackhole(true);
    sink.on_data(batch({"a", "b"}));
    std::thread dropper([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds{300});
        proxy.set_blackhole(false);
        proxy.drop_all();
    });
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    dropper.join();
    EXPECT_NE(why.find("the connection failed while waiting for the broker's acknowledgements"),
              std::string::npos)
        << why;
    EXPECT_NE(why.find("of the flush's 2 messages"), std::string::npos) << why;
    EXPECT_NE(thrown([&] { sink.on_data(batch({"x"})); }).find("failed earlier"),
              std::string::npos);
    EXPECT_NE(thrown([&] { sink.on_barrier(kBarrier); }).find("failed earlier"), std::string::npos);

    sink.open();
    sink.on_data(batch({"after"}));
    EXPECT_NO_THROW(sink.on_barrier(kBarrier));
    sink.close();
    // The reopened sink's message arrives once; the dropped flush's messages are
    // unknown, and any that arrive are only those.
    ASSERT_TRUE(c.wait_for(1));
    std::this_thread::sleep_for(std::chrono::milliseconds{300});
    std::size_t after = 0;
    for (const auto& m : c.payloads()) {
        if (m == "after") {
            ++after;
        } else {
            EXPECT_TRUE(m == "a" || m == "b") << "an unexpected message: " << m;
        }
    }
    EXPECT_EQ(after, 1U);
}

TEST(MqttSinkContractLive, ACancelEndsAWaitForAcknowledgements) {
    REQUIRE_MQTT();
    const auto topic = "clink/sinkc/" + unique("cancel");
    clink::itest::TcpFaultProxy proxy(broker().port, broker().host);
    MqttSink sink(proxied_opts(topic, proxy));  // waits up to 30 s for acknowledgements
    Attached a(sink);
    sink.open();
    proxy.set_blackhole(true);
    sink.on_data(batch({"a"}));
    a.cancel_after(std::chrono::milliseconds{300});
    const auto t0 = std::chrono::steady_clock::now();
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    a.canceller.join();
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds{2});
    EXPECT_NE(why.find("the task was cancelled while waiting for the broker's acknowledgements"),
              std::string::npos)
        << why;
    const auto c0 = std::chrono::steady_clock::now();
    EXPECT_NO_THROW(sink.close());
    EXPECT_LT(std::chrono::steady_clock::now() - c0, std::chrono::seconds{1});
    proxy.set_blackhole(false);
}

TEST(MqttSinkContractLive, ARecordAboveMaxRecordBytesIsRefusedBeforeItIsPublished) {
    REQUIRE_MQTT();
    const auto topic = "clink/sinkc/" + unique("big");
    Collector c(topic);
    auto o = sink_opts(topic);
    o.max_record_bytes = 1024;
    MqttSink sink(std::move(o));
    sink.open();
    const auto why = thrown([&] { sink.on_data(batch({"small", std::string(2048, 'x')})); });
    EXPECT_NE(why.find("a record is 2048 bytes, more than max_record_bytes (1024)"),
              std::string::npos)
        << why;
    sink.close();
    std::this_thread::sleep_for(std::chrono::milliseconds{300});
    EXPECT_TRUE(c.payloads().empty()) << "nothing of the batch was published";
}

TEST(MqttSinkContractLive, CloseDiscardsWhatIsBufferedAndNeverWaits) {
    REQUIRE_MQTT();
    const auto topic = "clink/sinkc/" + unique("close");
    Collector c(topic);
    clink::itest::TcpFaultProxy proxy(broker().port, broker().host);
    MqttSink sink(proxied_opts(topic, proxy));
    sink.open();
    proxy.set_blackhole(true);
    sink.on_data(batch({"buffered"}));
    const auto t0 = std::chrono::steady_clock::now();
    EXPECT_NO_THROW(sink.close());
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds{1});
    proxy.set_blackhole(false);
    std::this_thread::sleep_for(std::chrono::milliseconds{300});
    EXPECT_TRUE(c.payloads().empty()) << "close() published what it was meant to discard";
}

TEST(MqttSinkContractLive, TheFactoryBuildsASinkThatPublishes) {
    REQUIRE_MQTT();
    const auto topic = "clink/sinkc/" + unique("factory");
    Collector c(topic);
    const auto* f = clink::cluster::OperatorRegistry::default_instance().find_sink(
        "mqtt_sink", std::string{clink::cluster::kChannelString});
    ASSERT_NE(f, nullptr);
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = {{"host", broker().host},
                  {"port", std::to_string(broker().port)},
                  {"client_id", unique("clink-sinkc-factory")},
                  {"topic", topic},
                  {"qos", "2"},
                  {"batch_records", "2"},
                  {"ack_timeout_ms", "5000"}};
    auto sink = std::static_pointer_cast<clink::Sink<std::string>>(f->build(ctx));
    sink->open();
    sink->on_data(batch({R"({"id":1})", R"({"id":2})", R"({"id":3})"}));
    sink->on_barrier(kBarrier);
    sink->close();
    ASSERT_TRUE(c.wait_for(3));
    EXPECT_EQ(c.payloads(),
              (std::vector<std::string>{R"({"id":1})", R"({"id":2})", R"({"id":3})"}));
}

// An MQTT broker disconnects a client when another connects with its client id,
// so two sinks built from the same options, as two jobs writing to one broker
// with the default client id are, must not share one.
TEST(MqttSinkContractLive, TwoSinksBuiltFromTheSameOptionsKeepTheirOwnConnections) {
    REQUIRE_MQTT();
    const auto topic = "clink/sinkc/" + unique("twins");
    Collector c(topic);
    const auto* f = clink::cluster::OperatorRegistry::default_instance().find_sink(
        "mqtt_sink", std::string{clink::cluster::kChannelString});
    ASSERT_NE(f, nullptr);
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = {{"host", broker().host},
                  {"port", std::to_string(broker().port)},
                  {"topic", topic},
                  {"ack_timeout_ms", "5000"}};
    constexpr int kRounds = 20;
    constexpr int kPerRound = 100;
    std::atomic<int> failures{0};
    std::vector<std::string> errors(2);
    std::vector<std::uint64_t> reconnects(2);
    std::vector<std::thread> writers;
    for (int w = 0; w < 2; ++w) {
        writers.emplace_back([&, w] {
            auto sink = std::static_pointer_cast<MqttSink>(f->build(ctx));
            sink->open();
            for (int r = 0; r < kRounds; ++r) {
                std::vector<std::string> records;
                for (int i = 0; i < kPerRound; ++i) {
                    records.push_back(std::to_string(w) + "/" + std::to_string(r * kPerRound + i));
                }
                const auto e = thrown([&] {
                    sink->on_data(batch(records));
                    sink->on_barrier(kBarrier);
                });
                if (!e.empty()) {
                    failures.fetch_add(1);
                    errors[static_cast<std::size_t>(w)] = e;
                    sink->open();
                }
            }
            reconnects[static_cast<std::size_t>(w)] = sink->reconnects();
            sink->close();
        });
    }
    for (auto& t : writers) {
        t.join();
    }
    EXPECT_EQ(failures.load(), 0) << errors[0] << " | " << errors[1];
    EXPECT_EQ(reconnects[0], 0U);
    EXPECT_EQ(reconnects[1], 0U);
    EXPECT_TRUE(c.wait_for(2 * kRounds * kPerRound));
}
