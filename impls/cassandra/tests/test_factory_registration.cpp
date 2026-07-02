// Verifies clink::cassandra::install() registers the sink string-channel factory.

#include <gtest/gtest.h>

#include "clink/cluster/runner_registry.hpp"

namespace {

using clink::cluster::RunnerRegistry;

TEST(CassandraFactoryRegistration, SinkIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_sink("cassandra_sink_string", "string"), nullptr);
}

TEST(CassandraFactoryRegistration, UpsertSinkIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_sink("cassandra_upsert_sink_string", "string"), nullptr);
}

}  // namespace
