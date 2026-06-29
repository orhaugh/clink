// Verifies clink::rabbitmq::install() registers the source + sink string-channel factories.

#include <gtest/gtest.h>

#include "clink/cluster/runner_registry.hpp"

namespace {

using clink::cluster::RunnerRegistry;

TEST(RabbitMqFactoryRegistration, SourceAndSinkAreRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_source("rabbitmq_source_string", "string"), nullptr);
    EXPECT_NE(rr.find_sink("rabbitmq_sink_string", "string"), nullptr);
}

}  // namespace
