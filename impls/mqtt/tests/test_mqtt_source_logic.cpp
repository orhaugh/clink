// Offline logic tests for the MQTT source: option validation, the effective
// subscribe topic (plain vs $share shared subscription), and the dormant-subtask
// rule. No broker is contacted (construction does not connect; open() does).

#include <stdexcept>
#include <string>
#include <utility>

#include <gtest/gtest.h>

#ifdef CLINK_HAS_MQTT
#include "clink/mqtt/mqtt_source.hpp"
#endif

#ifdef CLINK_HAS_MQTT

using clink::mqtt::MqttSource;
using clink::mqtt::MqttSourceOptions;

namespace {

MqttSourceOptions opts(const std::string& topic) {
    MqttSourceOptions o;
    o.topic = topic;
    return o;
}

}  // namespace

TEST(MqttSourceLogic, EmptyTopicThrows) {
    MqttSourceOptions o;  // topic empty
    EXPECT_THROW({ MqttSource s(std::move(o)); }, std::runtime_error);
}

TEST(MqttSourceLogic, BadQosThrows) {
    auto o = opts("t");
    o.qos = 3;
    EXPECT_THROW({ MqttSource s(std::move(o)); }, std::runtime_error);
}

TEST(MqttSourceLogic, PlainSubscribeTopic) {
    MqttSource s(opts("sensors/temp"));
    EXPECT_EQ(s.subscribe_topic(), "sensors/temp");
    EXPECT_FALSE(s.dormant());
}

TEST(MqttSourceLogic, SharedGroupRewritesSubscribeTopic) {
    auto o = opts("sensors/+/temp");
    o.shared_group = "g1";
    o.subtask_idx = 2;
    MqttSource s(std::move(o));
    EXPECT_EQ(s.subscribe_topic(), "$share/g1/sensors/+/temp");
    // A shared subscription load-balances across subtasks, so every subtask is
    // active (not dormant) even at a non-zero index.
    EXPECT_FALSE(s.dormant());
}

TEST(MqttSourceLogic, NonZeroSubtaskIsDormantWithoutSharedGroup) {
    auto o = opts("t");
    o.subtask_idx = 1;
    MqttSource s(std::move(o));
    EXPECT_TRUE(s.dormant());
}

TEST(MqttSourceLogic, SubtaskZeroIsActive) {
    auto o = opts("t");
    o.subtask_idx = 0;
    MqttSource s(std::move(o));
    EXPECT_FALSE(s.dormant());
}

#endif  // CLINK_HAS_MQTT
