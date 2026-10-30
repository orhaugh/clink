// Verifies clink::pulsar::install() registers the source + sink string-channel factories.

#include <stdexcept>

#include <gtest/gtest.h>

#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/runner_registry.hpp"
#include "clink/pulsar/pulsar_source.hpp"

namespace {

using clink::cluster::RunnerRegistry;

TEST(PulsarFactoryRegistration, SourceAndSinkAreRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_source("pulsar_source_string", "string"), nullptr);
    EXPECT_NE(rr.find_sink("pulsar_sink_string", "string"), nullptr);
}

// An exclusive subscription has a single consumer, and a restore's seek moves the cursor for
// every consumer on it: the factory refuses it at parallelism above one rather than leave the
// second subtask failing to subscribe.
TEST(PulsarFactoryRegistration, AnExclusiveSubscriptionIsRefusedAboveParallelismOne) {
    const auto* f = clink::cluster::OperatorRegistry::default_instance().find_source(
        "pulsar_source_string", "string");
    ASSERT_NE(f, nullptr);
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = {{"topic", "t"}, {"subscription_type", "exclusive"}};
    ctx.parallelism = 2;
    EXPECT_THROW((void)f->build(ctx), std::runtime_error);
    ctx.parallelism = 1;
    EXPECT_NE(f->build(ctx), nullptr);
}

TEST(PulsarFactoryRegistration, AnUnknownSubscriptionTypeIsRefused) {
    clink::pulsar::PulsarSource::Options o;
    o.topic = "t";
    o.subscription_type = "failover";
    EXPECT_THROW(clink::pulsar::PulsarSource{o}, std::runtime_error);
}

}  // namespace
