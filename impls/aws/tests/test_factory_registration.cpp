// Verifies clink::aws::install() makes the AWS-family sink factories reachable
// through the RunnerRegistry on the "string" channel.

#include <gtest/gtest.h>

#include "clink/cluster/runner_registry.hpp"

namespace {

using clink::cluster::RunnerRegistry;

TEST(AwsFactoryRegistration, DynamoDbSinkIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_sink("dynamodb_sink", "string"), nullptr);
}

}  // namespace
