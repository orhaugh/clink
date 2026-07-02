// Verifies clink::redis::install() makes the redis_source / redis_sink factories
// reachable through the RunnerRegistry on the "string" channel.

#include <gtest/gtest.h>

#include "clink/cluster/runner_registry.hpp"

namespace {

using clink::cluster::RunnerRegistry;

TEST(RedisFactoryRegistration, SinkIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_sink("redis_sink", "string"), nullptr);
}

TEST(RedisFactoryRegistration, UpsertSinkIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_sink("redis_upsert_sink", "string"), nullptr);
}

TEST(RedisFactoryRegistration, SourceIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_source("redis_source", "string"), nullptr);
}

}  // namespace
