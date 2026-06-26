// Verifies clink::http_connector::install() makes http_sink reachable through
// the RunnerRegistry on the "string" channel.

#include <gtest/gtest.h>

#include "clink/cluster/runner_registry.hpp"

namespace {

using clink::cluster::RunnerRegistry;

TEST(HttpConnectorFactoryRegistration, HttpSinkIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_sink("http_sink", "string"), nullptr);
}

TEST(HttpConnectorFactoryRegistration, ElasticsearchAndOpensearchSinksRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_sink("elasticsearch_sink", "string"), nullptr);
    EXPECT_NE(rr.find_sink("opensearch_sink", "string"), nullptr);
}

}  // namespace
