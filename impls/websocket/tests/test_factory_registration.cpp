// Verifies clink::websocket::install() makes websocket_source_string
// reachable through the RunnerRegistry on the "string" channel.

#include <gtest/gtest.h>

#include "clink/cluster/runner_registry.hpp"

namespace {

using clink::cluster::RunnerRegistry;

TEST(WebSocketFactoryRegistration, SourceIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_source("websocket_source_string", "string"), nullptr);
}

}  // namespace
