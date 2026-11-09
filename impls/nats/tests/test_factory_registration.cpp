// Verifies clink::nats::install() registers the source + sink string-channel factories, that the
// sink's factory reads its options strictly, and the record the analyser reads.

#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/runner_registry.hpp"
#include "clink/connectors/capability.hpp"

namespace {

using clink::cluster::RunnerRegistry;

TEST(NatsFactoryRegistration, SourceAndSinkAreRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_source("nats_source_string", "string"), nullptr);
    EXPECT_NE(rr.find_sink("nats_sink_string", "string"), nullptr);
}

template <typename F>
std::string nats_thrown(F&& f) {
    try {
        std::forward<F>(f)();
    } catch (const std::exception& e) {
        return e.what();
    }
    return "";
}

// Building a sink connects to nothing, so a value is refused here before any
// server is involved.
TEST(NatsFactoryRegistration, TheSinkFactoryReadsItsOptionsStrictly) {
    const auto* f = clink::cluster::OperatorRegistry::default_instance().find_sink(
        "nats_sink_string", std::string{clink::cluster::kChannelString});
    ASSERT_NE(f, nullptr);
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = {{"subject", "s"}};
    EXPECT_NO_THROW((void)f->build(ctx));
    auto every = ctx;
    every.params.insert({{"connect_timeout_ms", "300000"},
                         {"write_timeout_ms", "3600000"},
                         {"max_pending", "1000000"},
                         {"max_pending_bytes", "4294967296"},
                         {"publish_timeout_ms", "3600000"}});
    EXPECT_NO_THROW((void)f->build(every));
    for (const auto& [key, value] :
         std::vector<std::pair<std::string, std::string>>{{"connect_timeout_ms", "0"},
                                                          {"write_timeout_ms", "0"},
                                                          {"max_pending", "0"},
                                                          {"max_pending", "4k"},
                                                          {"max_pending_bytes", "0"},
                                                          {"publish_timeout_ms", "-1"},
                                                          {"publish_timeout_ms", "30s"}}) {
        auto c = ctx;
        c.params[key] = value;
        EXPECT_NE(nats_thrown([&] { (void)f->build(c); }).find("'" + key + "' must be an integer"),
                  std::string::npos)
            << key << "=" << value;
    }
    auto none = ctx;
    none.params.erase("subject");
    EXPECT_NE(nats_thrown([&] { (void)f->build(none); }).find("'subject' is required"),
              std::string::npos);
}

// Building a source connects to nothing either. Its own options were read
// leniently: a typo fell back to the default, and ack_wait_s='1.5' read as 1.
TEST(NatsFactoryRegistration, TheSourceFactoryReadsItsOptionsStrictly) {
    const auto* f = clink::cluster::OperatorRegistry::default_instance().find_source(
        "nats_source_string", std::string{clink::cluster::kChannelString});
    ASSERT_NE(f, nullptr);
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = {{"subject", "s"}};
    EXPECT_NO_THROW((void)f->build(ctx));
    auto every = ctx;
    every.params.insert({{"batch", "1000000"},
                         {"fetch_timeout_ms", "600000"},
                         {"ack_wait_s", "31536000"},
                         {"max_lease_s", "31536000"},
                         {"max_ack_pending", "2147483647"}});
    EXPECT_NO_THROW((void)f->build(every));
    for (const auto& [key, value] :
         std::vector<std::pair<std::string, std::string>>{{"batch", "0"},
                                                          {"batch", "1.5"},
                                                          {"fetch_timeout_ms", "0"},
                                                          {"fetch_timeout_ms", "soon"},
                                                          {"ack_wait_s", "0"},
                                                          {"ack_wait_s", "1.5"},
                                                          {"max_lease_s", "x"},
                                                          {"max_ack_pending", "0"},
                                                          {"max_ack_pending", "2147483648"}}) {
        auto c = ctx;
        c.params[key] = value;
        EXPECT_NE(nats_thrown([&] { (void)f->build(c); }).find("'" + key + "' must be an integer"),
                  std::string::npos)
            << key << "=" << value;
    }
    auto short_lease = ctx;
    short_lease.params.insert({{"ack_wait_s", "60"}, {"max_lease_s", "30"}});
    EXPECT_NE(nats_thrown([&] {
                  (void)f->build(short_lease);
              }).find("'max_lease_s' must be at least 'ack_wait_s'"),
              std::string::npos);
}

TEST(NatsFactoryRegistration, TheRecordStatesTheSinksContract) {
    const auto* caps = clink::connectors::CapabilityRegistry::instance().find("nats");
    ASSERT_NE(caps, nullptr);
    EXPECT_TRUE(caps->self_check().empty());
    EXPECT_EQ(caps->version, "2");
    EXPECT_EQ(caps->delivery, clink::connectors::DeliveryGuarantee::AtLeastOnce);
    EXPECT_FALSE(caps->retries);
    EXPECT_EQ(
        caps->timeout_options,
        (std::vector<std::string>{"connect_timeout_ms", "write_timeout_ms", "publish_timeout_ms"}));
}

}  // namespace
