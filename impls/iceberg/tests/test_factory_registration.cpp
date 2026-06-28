// Verifies clink::iceberg::install() makes the iceberg_row_sink factory reachable
// through the RunnerRegistry on the "row" channel.

#include <gtest/gtest.h>

#include "clink/cluster/runner_registry.hpp"

namespace {

using clink::cluster::RunnerRegistry;

TEST(IcebergFactoryRegistration, SinkIsRegistered) {
    EXPECT_NE(RunnerRegistry::default_instance().find_sink("iceberg_row_sink", "row"), nullptr);
}

}  // namespace
