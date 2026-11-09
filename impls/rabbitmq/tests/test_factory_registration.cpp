// Verifies clink::rabbitmq::install() registers the source + sink string-channel factories,
// that the sink's factory reads its options strictly, and the record the analyser reads.

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

TEST(RabbitMqFactoryRegistration, SourceAndSinkAreRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_source("rabbitmq_source_string", "string"), nullptr);
    EXPECT_NE(rr.find_sink("rabbitmq_sink_string", "string"), nullptr);
}

template <typename F>
std::string rmq_thrown(F&& f) {
    try {
        std::forward<F>(f)();
    } catch (const std::exception& e) {
        return e.what();
    }
    return "";
}

// Building a sink connects to nothing, so a value is refused here before any
// broker is involved.
TEST(RabbitMqFactoryRegistration, TheSinkFactoryReadsItsOptionsStrictly) {
    const auto* f = clink::cluster::OperatorRegistry::default_instance().find_sink(
        "rabbitmq_sink_string", std::string{clink::cluster::kChannelString});
    ASSERT_NE(f, nullptr);
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = {{"routing_key", "q"}};
    EXPECT_NO_THROW((void)f->build(ctx));
    auto every = ctx;
    every.params.insert({{"port", "65535"},
                         {"heartbeat_s", "0"},
                         {"connect_timeout_ms", "300000"},
                         {"persistent", "false"},
                         {"mandatory", "false"},
                         {"confirm_timeout_ms", "3600000"},
                         {"max_unconfirmed", "1000000"},
                         {"max_record_bytes", "536870912"}});
    EXPECT_NO_THROW((void)f->build(every));
    for (const auto& [key, value] :
         std::vector<std::pair<std::string, std::string>>{{"port", "0"},
                                                          {"port", "5672x"},
                                                          {"heartbeat_s", "-1"},
                                                          {"connect_timeout_ms", "0"},
                                                          {"confirm_timeout_ms", "0"},
                                                          {"confirm_timeout_ms", "thirty"},
                                                          {"max_unconfirmed", "0"},
                                                          {"max_record_bytes", "536870913"}}) {
        auto c = ctx;
        c.params[key] = value;
        EXPECT_NE(rmq_thrown([&] { (void)f->build(c); }).find("'" + key + "' must be an integer"),
                  std::string::npos)
            << key << "=" << value;
    }
    // persistent='False' and mandatory='FALSE' once read as true, and a
    // mandatory='no' meant to let the exchange drop unroutable messages did not.
    for (const char* key : {"persistent", "mandatory"}) {
        auto c = ctx;
        c.params[key] = "no";
        EXPECT_NE(rmq_thrown([&] {
                      (void)f->build(c);
                  }).find("'" + std::string{key} + "' must be 'true' or 'false'"),
                  std::string::npos)
            << key;
    }
    auto none = ctx;
    none.params.erase("routing_key");
    EXPECT_NE(rmq_thrown([&] { (void)f->build(none); }).find("'routing_key' is required"),
              std::string::npos);
}

// Building a source connects to nothing either. Its own options were read
// leniently: a typo fell back to the default, prefetch='1.5' read as 1, and
// declare_queue='False' declared the queue.
TEST(RabbitMqFactoryRegistration, TheSourceFactoryReadsItsOptionsStrictly) {
    const auto* f = clink::cluster::OperatorRegistry::default_instance().find_source(
        "rabbitmq_source_string", std::string{clink::cluster::kChannelString});
    ASSERT_NE(f, nullptr);
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = {{"queue", "q"}};
    EXPECT_NO_THROW((void)f->build(ctx));
    auto every = ctx;
    every.params.insert({{"prefetch", "65535"},
                         {"poll_timeout_ms", "600000"},
                         {"batch_size", "1000000"},
                         {"declare_queue", "false"}});
    EXPECT_NO_THROW((void)f->build(every));
    for (const auto& [key, value] :
         std::vector<std::pair<std::string, std::string>>{{"prefetch", "0"},
                                                          {"prefetch", "1.5"},
                                                          {"prefetch", "lots"},
                                                          {"prefetch", "65536"},
                                                          {"poll_timeout_ms", "0"},
                                                          {"poll_timeout_ms", "600001"},
                                                          {"batch_size", "0"},
                                                          {"batch_size", "10x"}}) {
        auto c = ctx;
        c.params[key] = value;
        EXPECT_NE(rmq_thrown([&] { (void)f->build(c); }).find("'" + key + "' must be an integer"),
                  std::string::npos)
            << key << "=" << value;
    }
    for (const char* value : {"False", "no"}) {
        auto c = ctx;
        c.params["declare_queue"] = value;
        EXPECT_NE(rmq_thrown([&] {
                      (void)f->build(c);
                  }).find("'declare_queue' must be 'true' or 'false'"),
                  std::string::npos)
            << value;
    }
}

TEST(RabbitMqFactoryRegistration, TheRecordStatesTheSinksContract) {
    const auto* caps = clink::connectors::CapabilityRegistry::instance().find("rabbitmq");
    ASSERT_NE(caps, nullptr);
    EXPECT_TRUE(caps->self_check().empty());
    EXPECT_EQ(caps->version, "2");
    EXPECT_EQ(caps->delivery, clink::connectors::DeliveryGuarantee::AtLeastOnce);
    EXPECT_FALSE(caps->retries);
    EXPECT_FALSE(caps->tls);
    EXPECT_EQ(caps->timeout_options,
              (std::vector<std::string>{"connect_timeout_ms", "confirm_timeout_ms"}));
}

}  // namespace
