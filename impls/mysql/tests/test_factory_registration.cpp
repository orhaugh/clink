// Verifies clink::mysql::install() makes the mysql_source / mysql_sink factories
// reachable through the RunnerRegistry on the "string" channel.

#include <gtest/gtest.h>

#include "clink/cluster/runner_registry.hpp"

namespace {

using clink::cluster::RunnerRegistry;

TEST(MysqlFactoryRegistration, SinkIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_sink("mysql_sink", "string"), nullptr);
}

TEST(MysqlFactoryRegistration, UpsertSinkIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_sink("mysql_upsert_sink", "string"), nullptr);
}

TEST(MysqlFactoryRegistration, SourceIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_source("mysql_source", "string"), nullptr);
}

TEST(MysqlFactoryRegistration, CdcSourceIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_source("mysql_cdc_source", "string"), nullptr);
}

}  // namespace
