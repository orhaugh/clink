// Verifies clink::clickhouse::install() makes every factory reachable
// through the RunnerRegistry: clickhouse_sink + clickhouse_row_source +
// clickhouse_text_source.

#include <gtest/gtest.h>

#include "clink/cluster/runner_registry.hpp"

namespace {

using clink::cluster::RunnerRegistry;

TEST(ClickHouseFactoryRegistration, ClickHouseSinkIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_sink("clickhouse_sink", "string"), nullptr);
}

TEST(ClickHouseFactoryRegistration, ClickHouseTextSourceIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_source("clickhouse_text_source", "string"), nullptr);
}

TEST(ClickHouseFactoryRegistration, ClickHouseJsonSourceIsRegistered) {
    // M2: the JSON source on the string channel (delimited text source kept).
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_source("clickhouse_source", "string"), nullptr);
}

TEST(ClickHouseFactoryRegistration, ClickHouseRowSourceIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_source("clickhouse_row_source", "clickhouse.row"), nullptr);
}

}  // namespace
