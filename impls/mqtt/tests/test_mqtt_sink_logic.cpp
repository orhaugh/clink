// Offline logic tests for the MQTT sink: option validation and the flush/buffer
// state machine that does not need a broker (a flush with buffered records before
// open() is an error; an empty flush is a no-op).

#include <stdexcept>
#include <string>
#include <utility>

#include <gtest/gtest.h>

#include "clink/core/record.hpp"

#ifdef CLINK_HAS_MQTT
#include "clink/mqtt/mqtt_sink.hpp"
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

#endif  // CLINK_HAS_MQTT
