// Verifies clink::pulsar::install() registers the source + sink string-channel factories.

#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/runner_registry.hpp"
#include "clink/connectors/capability.hpp"
#include "clink/pulsar/pulsar_source.hpp"

namespace {

using clink::cluster::RunnerRegistry;

TEST(PulsarFactoryRegistration, SourceAndSinkAreRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_source("pulsar_source_string", "string"), nullptr);
    EXPECT_NE(rr.find_sink("pulsar_sink_string", "string"), nullptr);
}

// An exclusive subscription has a single consumer, and a restore's seek moves the cursor for
// every consumer on it: the factory refuses it at parallelism above one rather than leave the
// second subtask failing to subscribe.
TEST(PulsarFactoryRegistration, AnExclusiveSubscriptionIsRefusedAboveParallelismOne) {
    const auto* f = clink::cluster::OperatorRegistry::default_instance().find_source(
        "pulsar_source_string", "string");
    ASSERT_NE(f, nullptr);
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = {{"topic", "t"}, {"subscription_type", "exclusive"}};
    ctx.parallelism = 2;
    EXPECT_THROW((void)f->build(ctx), std::runtime_error);
    ctx.parallelism = 1;
    EXPECT_NE(f->build(ctx), nullptr);
}

TEST(PulsarFactoryRegistration, AnUnknownSubscriptionTypeIsRefused) {
    clink::pulsar::PulsarSource::Options o;
    o.topic = "t";
    o.subscription_type = "failover";
    EXPECT_THROW(clink::pulsar::PulsarSource{o}, std::runtime_error);
}

template <typename F>
std::string pulsar_thrown(F&& f) {
    try {
        std::forward<F>(f)();
    } catch (const std::exception& e) {
        return e.what();
    }
    return "";
}

// Building a sink connects to nothing, so a value is refused here before any
// broker is involved.
TEST(PulsarFactoryRegistration, TheSinkFactoryReadsItsOptionsStrictly) {
    const auto* f = clink::cluster::OperatorRegistry::default_instance().find_sink(
        "pulsar_sink_string", std::string{clink::cluster::kChannelString});
    ASSERT_NE(f, nullptr);
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = {{"topic", "t"}};
    EXPECT_NO_THROW((void)f->build(ctx));
    auto every = ctx;
    every.params.insert({{"operation_timeout_s", "3600"},
                         {"tls_verify_hostname", "false"},
                         {"batching", "false"},
                         {"send_timeout_ms", "3600000"},
                         {"max_pending", "500000"},
                         {"max_pending_bytes", "4294967296"}});
    EXPECT_NO_THROW((void)f->build(every));
    // send_timeout_ms='0' once reached the client, which reads 0 as no timeout,
    // so a barrier could wait for ever.
    for (const auto& [key, value] :
         std::vector<std::pair<std::string, std::string>>{{"operation_timeout_s", "0"},
                                                          {"send_timeout_ms", "0"},
                                                          {"send_timeout_ms", "30s"},
                                                          {"max_pending", "0"},
                                                          {"max_pending", "500001"},
                                                          {"max_pending_bytes", "0"}}) {
        auto c = ctx;
        c.params[key] = value;
        EXPECT_NE(
            pulsar_thrown([&] { (void)f->build(c); }).find("'" + key + "' must be an integer"),
            std::string::npos)
            << key << "=" << value;
    }
    for (const char* key : {"batching", "tls_verify_hostname"}) {
        auto c = ctx;
        c.params[key] = "no";
        EXPECT_NE(pulsar_thrown([&] {
                      (void)f->build(c);
                  }).find("'" + std::string{key} + "' must be 'true' or 'false'"),
                  std::string::npos)
            << key;
    }
}

TEST(PulsarFactoryRegistration, TheRecordStatesTheSinksContract) {
    const auto* caps = clink::connectors::CapabilityRegistry::instance().find("pulsar");
    ASSERT_NE(caps, nullptr);
    EXPECT_TRUE(caps->self_check().empty());
    EXPECT_EQ(caps->version, "2");
    EXPECT_EQ(caps->delivery, clink::connectors::DeliveryGuarantee::AtLeastOnce);
    // Only a token authenticates; the record once claimed passwords too.
    EXPECT_EQ(caps->auth_methods, (std::vector<std::string>{"none", "token"}));
    EXPECT_EQ(caps->timeout_options,
              (std::vector<std::string>{"operation_timeout_s", "send_timeout_ms"}));
}

}  // namespace
