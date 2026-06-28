// Verifies clink::mongodb::install() makes the mongo_cdc_source / mongo_sink
// factories reachable through the RunnerRegistry on the "string" channel.

#include <gtest/gtest.h>

#include "clink/cluster/runner_registry.hpp"

namespace {

using clink::cluster::RunnerRegistry;

TEST(MongoFactoryRegistration, SinkIsRegistered) {
    EXPECT_NE(RunnerRegistry::default_instance().find_sink("mongo_sink", "string"), nullptr);
}

TEST(MongoFactoryRegistration, SourceIsRegistered) {
    EXPECT_NE(RunnerRegistry::default_instance().find_source("mongo_cdc_source", "string"),
              nullptr);
}

}  // namespace
