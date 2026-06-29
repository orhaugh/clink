// Verifies clink::pulsar::install() registers the source + sink string-channel factories.

#include <gtest/gtest.h>

#include "clink/cluster/runner_registry.hpp"

namespace {

using clink::cluster::RunnerRegistry;

TEST(PulsarFactoryRegistration, SourceAndSinkAreRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_source("pulsar_source_string", "string"), nullptr);
    EXPECT_NE(rr.find_sink("pulsar_sink_string", "string"), nullptr);
}

}  // namespace
