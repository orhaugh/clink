// The MQTT source acknowledges a message only once a completed checkpoint
// covers it, against an in-process broker (fake_mqtt_broker.hpp) that records
// each acknowledgement in the order it came. No real broker: these run in the
// ci workflow. The same contract against mosquitto is MqttSourceAckLive.

#include <chrono>
#include <cstdint>
#include <functional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/record.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/state/in_memory_state_backend.hpp"

#ifdef CLINK_HAS_MQTT
#include "clink/mqtt/mqtt_source.hpp"

#include "fake_mqtt_broker.hpp"
#endif

#ifdef CLINK_HAS_MQTT

using clink::CheckpointId;
using clink::Emitter;
using clink::OperatorId;
using clink::StreamElement;
using clink::mqtt::MqttSource;
using clink::mqtt::MqttSourceOptions;
using clink::mqtt::testing::FakeMqttBroker;
namespace proto = clink::mqtt::protocol;
using namespace std::chrono_literals;

namespace {

struct Received {
    std::vector<std::string> values;
};

class SourceAckTest : public ::testing::Test {
protected:
    MqttSourceOptions options(std::uint8_t version = proto::kMqtt5, int qos = 1) const {
        MqttSourceOptions o;
        o.conn.host = "127.0.0.1";
        o.conn.port = broker.port();
        o.conn.client_id = "clink-src";
        o.conn.connack_timeout = 2000ms;
        o.topic = "t";
        o.qos = qos;
        o.block = 20ms;
        o.protocol_version = version;
        o.receive_maximum = 100;
        return o;
    }

    // produce() until `want` records have arrived in all, or two seconds pass.
    void read(MqttSource& src, std::size_t want) {
        const auto until = std::chrono::steady_clock::now() + 2s;
        while (got.values.size() < want && std::chrono::steady_clock::now() < until) {
            turn(src);
        }
        ASSERT_EQ(got.values.size(), want);
    }

    void turn(MqttSource& src) {
        Emitter<std::string> em{[this](StreamElement<std::string> e) {
            if (e.is_data()) {
                for (const auto& r : e.as_data()) {
                    got.values.push_back(r.value());
                }
            }
            return true;
        }};
        src.produce(em);
    }

    void seal(MqttSource& src, std::uint64_t id) {
        src.snapshot_offset(backend, OperatorId{1}, CheckpointId{id});
    }

    FakeMqttBroker broker;
    clink::InMemoryStateBackend backend;
    Received got;
};

// The broker's wait_for runs its predicate under the broker's lock, so it cannot
// call the broker's accessors; this polls them from outside.
bool eventually(const std::function<bool()>& pred, std::chrono::milliseconds timeout = 1s) {
    const auto until = std::chrono::steady_clock::now() + timeout;
    while (!pred()) {
        if (std::chrono::steady_clock::now() >= until) {
            return false;
        }
        std::this_thread::sleep_for(5ms);
    }
    return true;
}

}  // namespace

TEST_F(SourceAckTest, NothingIsAcknowledgedUntilACompletedCheckpointCoversIt) {
    MqttSource src(options());
    src.open();
    broker.publish("t", "a", 1);
    broker.publish("t", "b", 1);
    broker.publish("t", "c", 1);
    read(src, 3);
    seal(src, 1);
    for (int i = 0; i < 5; ++i) {
        turn(src);
    }
    EXPECT_TRUE(broker.acknowledged().empty()) << "acknowledged before the checkpoint completed";
    EXPECT_EQ(src.unacknowledged(), 3u);

    src.notify_checkpoint_complete(CheckpointId{1});
    turn(src);
    ASSERT_TRUE(eventually([&] { return broker.acknowledged().size() == 3; }));
    EXPECT_EQ(broker.acknowledged(), (std::vector<std::string>{"a", "b", "c"}));
    EXPECT_EQ(src.unacknowledged(), 0u);
    EXPECT_EQ(src.acknowledged_total(), 3u);
    src.close();
}

TEST_F(SourceAckTest, ACompletionAcknowledgesOnlyTheIntervalsItCovers) {
    MqttSource src(options());
    src.open();
    broker.publish("t", "a", 1);
    broker.publish("t", "b", 1);
    read(src, 2);
    seal(src, 1);
    broker.publish("t", "c", 1);
    read(src, 3);
    seal(src, 2);
    broker.publish("t", "d", 1);
    read(src, 4);  // still open: no checkpoint covers it

    src.notify_checkpoint_complete(CheckpointId{1});
    turn(src);
    ASSERT_TRUE(eventually([&] { return broker.acknowledged().size() == 2; }));
    turn(src);
    EXPECT_EQ(broker.acknowledged(), (std::vector<std::string>{"a", "b"}));

    // A late or repeated notification moves nothing more.
    src.notify_checkpoint_complete(CheckpointId{1});
    turn(src);
    src.notify_checkpoint_complete(CheckpointId{2});
    turn(src);
    ASSERT_TRUE(eventually([&] { return broker.acknowledged().size() == 3; }));
    EXPECT_EQ(broker.acknowledged(), (std::vector<std::string>{"a", "b", "c"}));
    EXPECT_EQ(broker.unacknowledged(), (std::vector<std::string>{"d"}));
    EXPECT_EQ(broker.stray_acknowledgements(), 0u);
    src.close();
}

TEST_F(SourceAckTest, AnAbortedCheckpointIsDeliveredAgainAndAcknowledgedOnlyOnce) {
    MqttSource src(options());
    src.open();
    broker.publish("t", "a", 1);
    broker.publish("t", "b", 1);
    read(src, 2);
    seal(src, 1);
    src.notify_checkpoint_aborted(CheckpointId{1});
    // The next turn reconnects; the broker delivers both again on the new connection.
    read(src, 4);
    EXPECT_EQ(src.reconnects(), 1u);
    EXPECT_EQ(got.values, (std::vector<std::string>{"a", "b", "a", "b"}));
    EXPECT_TRUE(broker.acknowledged().empty())
        << "an aborted checkpoint's messages were acknowledged";

    seal(src, 2);
    src.notify_checkpoint_complete(CheckpointId{2});
    turn(src);
    ASSERT_TRUE(eventually([&] { return broker.acknowledged().size() == 2; }));
    EXPECT_EQ(broker.acknowledged(), (std::vector<std::string>{"a", "b"}));
    EXPECT_EQ(broker.stray_acknowledgements(), 0u);
    src.close();
}

// MQTT requires acknowledgements in arrival order, and has no negative one.
// With an aborted interval ahead of a completed one, nothing after the aborted
// message is acknowledged on that connection: the reconnect delivers both again.
TEST_F(SourceAckTest, AnAbortAheadOfACompletionAcknowledgesNothingOutOfOrder) {
    MqttSource src(options());
    src.open();
    broker.publish("t", "a", 1);
    read(src, 1);
    seal(src, 1);
    broker.publish("t", "b", 1);
    read(src, 2);
    seal(src, 2);
    src.notify_checkpoint_aborted(CheckpointId{1});
    src.notify_checkpoint_complete(CheckpointId{2});
    read(src, 4);
    EXPECT_EQ(src.reconnects(), 1u);
    EXPECT_TRUE(broker.acknowledged().empty()) << "b was acknowledged ahead of a";

    seal(src, 3);
    src.notify_checkpoint_complete(CheckpointId{3});
    turn(src);
    ASSERT_TRUE(eventually([&] { return broker.acknowledged().size() == 2; }));
    EXPECT_EQ(broker.acknowledged(), (std::vector<std::string>{"a", "b"}));
    src.close();
}

// After a reconnect the broker may reuse a packet identifier for another
// message, so a completion never acknowledges on a new connection what arrived
// on an old one: the copy the broker delivered again is acknowledged instead.
TEST_F(SourceAckTest, AfterALostConnectionNothingFromTheOldConnectionIsAcknowledged) {
    MqttSource src(options());
    src.open();
    broker.publish("t", "a", 1);
    read(src, 1);
    seal(src, 1);
    broker.drop_connection();
    read(src, 2);  // reconnected, and "a" delivered again
    EXPECT_EQ(src.reconnects(), 1u);

    src.notify_checkpoint_complete(CheckpointId{1});
    for (int i = 0; i < 5; ++i) {
        turn(src);
    }
    EXPECT_TRUE(broker.acknowledged().empty())
        << "the old connection's acknowledgement went out on the new one";

    seal(src, 2);
    src.notify_checkpoint_complete(CheckpointId{2});
    turn(src);
    ASSERT_TRUE(eventually([&] { return broker.acknowledged().size() == 1; }));
    EXPECT_EQ(broker.acknowledged(), (std::vector<std::string>{"a"}));
    EXPECT_EQ(broker.stray_acknowledgements(), 0u);
    src.close();
}

TEST_F(SourceAckTest, AQos2MessageIsTakenOverOnlyAtCompletion) {
    MqttSource src(options(proto::kMqtt5, 2));
    src.open();
    broker.publish("t", "a", 2);
    read(src, 1);
    seal(src, 1);
    for (int i = 0; i < 3; ++i) {
        turn(src);
    }
    EXPECT_TRUE(broker.acknowledged().empty()) << "PUBREC went out before the checkpoint completed";

    src.notify_checkpoint_complete(CheckpointId{1});
    turn(src);  // PUBREC; the broker answers PUBREL
    ASSERT_TRUE(eventually([&] { return broker.acknowledged().size() == 1; }));
    ASSERT_TRUE(eventually([&] {
        turn(src);  // the PUBREL is answered with PUBCOMP
        return broker.pubcomps() == 1;
    }));
    EXPECT_EQ(got.values, (std::vector<std::string>{"a"}));
    src.close();
}

TEST_F(SourceAckTest, CloseAcknowledgesWhatCompletedAndLeavesTheRestToTheSession) {
    MqttSource src(options());
    src.open();
    broker.publish("t", "a", 1);
    broker.publish("t", "b", 1);
    read(src, 2);
    seal(src, 1);
    broker.publish("t", "c", 1);
    read(src, 3);
    src.notify_checkpoint_complete(CheckpointId{1});
    src.close();
    ASSERT_TRUE(eventually([&] { return broker.disconnects() == 1; }));
    EXPECT_EQ(broker.acknowledged(), (std::vector<std::string>{"a", "b"}));
    EXPECT_EQ(broker.unacknowledged(), (std::vector<std::string>{"c"}));

    // The next source on the same client id resumes the session and gets "c" again.
    got.values.clear();
    MqttSource next(options());
    next.open();
    read(next, 1);
    EXPECT_EQ(got.values, (std::vector<std::string>{"c"}));
    next.close();
}

// A crash between a completion and its acknowledgements: nothing went out, so
// the next source delivers those messages again.
TEST_F(SourceAckTest, ACrashBeforeTheAcknowledgementsWentOutDeliversThemAgain) {
    {
        MqttSource src(options());
        src.open();
        broker.publish("t", "a", 1);
        read(src, 1);
        seal(src, 1);
        src.notify_checkpoint_complete(CheckpointId{1});
        // destroyed without a produce() turn or close(): a crash
    }
    got.values.clear();
    MqttSource next(options());
    next.open();
    read(next, 1);
    EXPECT_EQ(got.values, (std::vector<std::string>{"a"}));
    EXPECT_TRUE(broker.acknowledged().empty());
    next.close();
}

// An MQTT 3.1.1 broker may send a message again while it waits for the
// acknowledgement; the subscriber hands it over once.
TEST_F(SourceAckTest, AMessageSentAgainOnTheSameConnectionIsDeliveredOnce) {
    MqttSource src(options(proto::kMqtt311));
    src.open();
    const auto id = broker.publish("t", "a", 1);
    read(src, 1);
    broker.resend(id);
    broker.publish("t", "b", 1);
    read(src, 2);
    for (int i = 0; i < 3; ++i) {
        turn(src);
    }
    EXPECT_EQ(got.values, (std::vector<std::string>{"a", "b"}));
    seal(src, 1);
    src.notify_checkpoint_complete(CheckpointId{1});
    turn(src);
    ASSERT_TRUE(eventually([&] { return broker.acknowledged().size() == 2; }));
    EXPECT_EQ(broker.stray_acknowledgements(), 0u);
    src.close();
}

TEST_F(SourceAckTest, ConnectAsksForAPersistentSessionAndStatesTheWindow) {
    {
        MqttSource src(options(proto::kMqtt5));
        src.open();
        const auto c = broker.last_connect();
        ASSERT_TRUE(c.has_value());
        EXPECT_EQ(c->version, proto::kMqtt5);
        EXPECT_FALSE(c->clean_start);
        EXPECT_EQ(c->client_id, "clink-src");
        ASSERT_TRUE(c->receive_maximum.has_value());
        EXPECT_EQ(*c->receive_maximum, 100);
        ASSERT_TRUE(c->session_expiry.has_value());
        EXPECT_EQ(*c->session_expiry, proto::kSessionNeverExpires);
        src.close();
    }
    auto o = options(proto::kMqtt311);
    o.conn.client_id = "clink-src-311";
    MqttSource src(std::move(o));
    src.open();
    const auto c = broker.last_connect();
    ASSERT_TRUE(c.has_value());
    EXPECT_EQ(c->version, proto::kMqtt311);
    EXPECT_FALSE(c->clean_start);
    EXPECT_FALSE(c->receive_maximum.has_value());
    src.close();
}

TEST_F(SourceAckTest, ARefusedConnectionSaysWhy) {
    broker.refuse_with(0x86);
    MqttSource src(options());
    try {
        src.open();
        FAIL() << "open() accepted a refused connection";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string(e.what()).find("bad user name or password"), std::string::npos)
            << e.what();
    }
}

TEST_F(SourceAckTest, AnIdleConnectionIsKeptAliveWithPings) {
    auto o = options();
    o.conn.keepalive = 5;  // the client pings once half of it passes with nothing sent
    MqttSource src(std::move(o));
    src.open();
    const auto until = std::chrono::steady_clock::now() + 3500ms;
    while (std::chrono::steady_clock::now() < until) {
        turn(src);
    }
    EXPECT_GE(broker.pingreqs(), 1u);
    EXPECT_EQ(src.reconnects(), 0u);
    src.close();
}

#endif  // CLINK_HAS_MQTT
