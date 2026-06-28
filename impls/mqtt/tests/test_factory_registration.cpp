// Verifies clink::mqtt::install() makes the mqtt_source / mqtt_sink factories
// reachable through the RunnerRegistry on the "string" channel.

#include <gtest/gtest.h>

#include "clink/cluster/runner_registry.hpp"

namespace {

using clink::cluster::RunnerRegistry;

TEST(MqttFactoryRegistration, SinkIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_sink("mqtt_sink", "string"), nullptr);
}

TEST(MqttFactoryRegistration, SourceIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_source("mqtt_source", "string"), nullptr);
}

}  // namespace
