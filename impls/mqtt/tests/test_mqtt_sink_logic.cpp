// Offline logic tests for the MQTT sink: option validation and the flush/buffer
// state machine that does not need a broker (a flush with buffered records before
// open() is an error; an empty flush is a no-op), and, against the in-process
// broker (fake_mqtt_broker.hpp), the failure a broker's DISCONNECT gives a flush.

#include <chrono>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

#include <gtest/gtest.h>

#include "clink/core/record.hpp"

#ifdef CLINK_HAS_MQTT
#include "clink/mqtt/mqtt_sink.hpp"

#include "fake_mqtt_broker.hpp"
#endif

#ifdef CLINK_HAS_MQTT

using clink::Batch;
using clink::mqtt::MqttSink;
using clink::mqtt::MqttSinkOptions;

namespace {

MqttSinkOptions opts(const std::string& topic) {
    MqttSinkOptions o;
    o.topic = topic;
    return o;
}

}  // namespace

TEST(MqttSinkLogic, EmptyTopicThrows) {
    MqttSinkOptions o;  // topic empty
    EXPECT_THROW({ MqttSink s(std::move(o)); }, std::runtime_error);
}

TEST(MqttSinkLogic, BadQosThrows) {
    auto o = opts("t");
    o.qos = -1;
    EXPECT_THROW({ MqttSink s(std::move(o)); }, std::runtime_error);
}

TEST(MqttSinkLogic, EmptyFlushIsNoop) {
    MqttSink s(opts("t"));
    EXPECT_NO_THROW(s.flush());  // nothing buffered, no connection needed
}

// A record before open() is refused rather than buffered for a connection that
// does not exist; it once sat in the buffer until a flush threw.
TEST(MqttSinkLogic, ARecordBeforeOpenIsRefused) {
    MqttSink s(opts("t"));
    Batch<std::string> b;
    b.emplace("x");
    try {
        s.on_data(b);
        ADD_FAILURE() << "a record before open() was accepted";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string{e.what()}.find("before open()"), std::string::npos) << e.what();
    }
    EXPECT_NO_THROW(s.flush()) << "nothing was buffered, so there is nothing to flush";
    EXPECT_NO_THROW(s.close());
}

// Under MQTT 5 a broker that ends a connection says why in its DISCONNECT, and a
// flush waiting for its acknowledgements fails naming that reason and what
// became of its messages. The in-process broker takes the flush and
// acknowledges none of it, so the sink has written everything and is only
// reading when the DISCONNECT comes, and reads it. Against a real broker the
// sink is still writing a large flush when the broker closes, and the close can
// reset the connection before the client reads why
// (MqttSinkContractLive.ABrokerThatEndsTheConnectionDuringAFlushFailsIt).
TEST(MqttSinkLogic, ABrokerThatEndsTheConnectionGivesItsReason) {
    clink::mqtt::testing::FakeMqttBroker broker;
    auto o = opts("clink/sink/ended");
    o.conn.host = "127.0.0.1";
    o.conn.port = broker.port();
    o.conn.client_id = "clink-sink-ended";
    o.ack_timeout = std::chrono::seconds{20};
    MqttSink s(std::move(o));
    s.open();
    Batch<std::string> b;
    for (const char* r : {"a", "b", "c"}) {
        b.emplace(std::string{r});
    }
    s.on_data(b);
    std::thread broker_ends([&] {
        if (broker.wait_for_publishes(3, std::chrono::seconds{10})) {
            broker.end_connection(0x8E);  // Session taken over
        }
    });
    std::string why;
    try {
        s.on_barrier(clink::CheckpointBarrier{clink::CheckpointId{1}});
    } catch (const std::runtime_error& e) {
        why = e.what();
    }
    broker_ends.join();
    EXPECT_NE(why.find("the broker ended the connection while waiting for the broker's "
                       "acknowledgements (Session taken over)"),
              std::string::npos)
        << why;
    EXPECT_NE(why.find("of the flush's 3 messages, 0 were acknowledged and the other 3 are "
                       "unacknowledged"),
              std::string::npos)
        << why;
    try {
        s.flush();
        ADD_FAILURE() << "a failed sink flushed";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string{e.what()}.find("failed earlier"), std::string::npos) << e.what();
    }
    s.close();
}

#endif  // CLINK_HAS_MQTT
