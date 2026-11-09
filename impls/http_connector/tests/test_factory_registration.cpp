// Verifies clink::http_connector::install() makes http_sink reachable through
// the RunnerRegistry on the "string" channel.

#include <functional>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/guarantee_gate.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/runner_registry.hpp"
#include "clink/connectors/capability.hpp"
#include "clink/connectors/delivery_guarantee.hpp"

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

TEST(HttpConnectorFactoryRegistration, SplunkAndPrometheusSinksRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_sink("splunk_hec_sink", "string"), nullptr);
    EXPECT_NE(rr.find_sink("prometheus_sink", "string"), nullptr);
}

TEST(HttpConnectorFactoryRegistration, HttpPollSourceIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_source("http_poll_source", "string"), nullptr);
}

TEST(HttpConnectorFactoryRegistration, PubSubSinkAndSourceRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_sink("pubsub_sink", "string"), nullptr);
    EXPECT_NE(rr.find_source("pubsub_source", "string"), nullptr);
}

std::string pubsub_thrown(const std::function<void()>& f) {
    try {
        f();
    } catch (const std::exception& e) {
        return e.what();
    }
    return "";
}

// Building a sink connects to nothing, so a value is refused here before any
// service is involved.
TEST(PubSubFactory, TheSinkFactoryReadsItsOptionsStrictly) {
    const auto* f = clink::cluster::OperatorRegistry::default_instance().find_sink(
        "pubsub_sink", std::string{clink::cluster::kChannelString});
    ASSERT_NE(f, nullptr);
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = {{"project", "p"}, {"topic", "t"}, {"endpoint", "http://127.0.0.1:1"}};
    EXPECT_NO_THROW((void)f->build(ctx));
    auto every = ctx;
    for (const auto& [k, v] :
         std::vector<std::pair<std::string, std::string>>{{"auth_token", "t"},
                                                          {"headers", "X-A: 1"},
                                                          {"verify_tls", "false"},
                                                          {"connect_timeout_ms", "600000"},
                                                          {"request_timeout_ms", "3600000"},
                                                          {"batch_records", "1000"},
                                                          {"batch_bytes", "1073741824"},
                                                          {"max_retries", "20"},
                                                          {"dlq", "drop"},
                                                          {"linger_ms", "3600000"}}) {
        every.params[k] = v;
    }
    EXPECT_NO_THROW((void)f->build(every));
    for (const auto& [key, value] :
         std::vector<std::pair<std::string, std::string>>{{"batch_records", "0"},
                                                          {"batch_records", "1001"},
                                                          {"batch_bytes", "-1"},
                                                          {"max_retries", "21"},
                                                          {"linger_ms", "soon"},
                                                          {"connect_timeout_ms", "0"},
                                                          {"request_timeout_ms", "-5"}}) {
        auto c = ctx;
        c.params[key] = value;
        EXPECT_NE(
            pubsub_thrown([&] { (void)f->build(c); }).find("'" + key + "' must be an integer"),
            std::string::npos)
            << key << "=" << value;
    }
    // verify_tls='FALSE' once read as true, and dlq='DROP' as fail.
    auto tls = ctx;
    tls.params["verify_tls"] = "FALSE";
    EXPECT_NE(
        pubsub_thrown([&] { (void)f->build(tls); }).find("'verify_tls' must be 'true' or 'false'"),
        std::string::npos);
    auto dlq = ctx;
    dlq.params["dlq"] = "DROP";
    EXPECT_NE(pubsub_thrown([&] { (void)f->build(dlq); }).find("'dlq' must be 'fail' or 'drop'"),
              std::string::npos);
    auto both = ctx;
    both.params["auth_token"] = "t";
    both.params["auth_token_file"] = "/token";
    EXPECT_NE(pubsub_thrown([&] { (void)f->build(both); }).find("not both"), std::string::npos);
    auto empty = ctx;
    empty.params["endpoint"] = "";
    EXPECT_NE(pubsub_thrown([&] { (void)f->build(empty); }).find("'endpoint' is empty"),
              std::string::npos);
}

TEST(PubSubFactory, TheRecordNamesItsTimeoutsAndTheSinksLimits) {
    const auto* caps = clink::connectors::CapabilityRegistry::instance().find("pubsub");
    ASSERT_NE(caps, nullptr);
    EXPECT_EQ(caps->version, "2");
    EXPECT_EQ(caps->timeout_options,
              (std::vector<std::string>{"connect_timeout_ms", "request_timeout_ms"}));
    bool empty_record = false;
    for (const auto& l : caps->limitations) {
        empty_record = empty_record || l.find("an empty record is refused") != std::string::npos;
    }
    EXPECT_TRUE(empty_record);
}

// The record the delivery analyser and the coordinator read: the source
// acknowledges at checkpoint completion and the server delivers again what was
// not acknowledged, so a job over it is at-least-once. It used to declare no
// position model, which the analyser read as at-most-once, refusing even an
// at_least_once request.
TEST(SourceCapabilityRecord, TheSourceAcknowledgesAtCheckpointCompletion) {
    const auto* caps = clink::connectors::CapabilityRegistry::instance().find("pubsub");
    ASSERT_NE(caps, nullptr);
    EXPECT_EQ(caps->offset_model, clink::connectors::OffsetModel::Acknowledgement);
    EXPECT_FALSE(caps->replayable);
    EXPECT_TRUE(caps->checkpoint_integrated);
    EXPECT_TRUE(caps->self_check().empty());
    EXPECT_TRUE(caps->scale_down_refusal.empty());
}

clink::cluster::OperatorSpec gate_op(std::string type,
                                     std::string id,
                                     std::vector<std::string> inputs = {},
                                     std::map<std::string, std::string> params = {}) {
    clink::cluster::OperatorSpec s;
    s.type = std::move(type);
    s.id = std::move(id);
    s.inputs = std::move(inputs);
    s.params = std::move(params);
    return s;
}

TEST(SourceCapabilityRecord, AJobOverTheSourceIsAtLeastOnceAndExactlyOnceIsRefused) {
    clink::cluster::CheckpointConfig ckpt;
    ckpt.checkpoint_dir = "/tmp/ckpt";
    ckpt.interval_ms = 1000;
    ckpt.state_backend_uri = "file:///tmp/state";
    clink::cluster::JobGraphSpec g;
    g.ops.push_back(gate_op("pubsub_source", "src"));
    g.ops.push_back(gate_op("file_2pc_sink_string",
                            "snk",
                            {"src"},
                            {{"dir", "/tmp/out"}, {"delivery_guarantee", "at_least_once"}}));
    clink::connectors::GuaranteeReport report;
    EXPECT_EQ(clink::cluster::check_delivery_guarantee(g, ckpt, &report), "");
    EXPECT_EQ(report.level, clink::connectors::EndToEndGuarantee::AtLeastOnceSource)
        << report.render_text();

    g.ops[1].params["delivery_guarantee"] = "exactly_once";
    const auto refused = clink::cluster::check_delivery_guarantee(g, ckpt, &report);
    EXPECT_NE(refused.find("AT_LEAST_ONCE_SOURCE"), std::string::npos) << refused;
    EXPECT_NE(refused.find("pubsub_source"), std::string::npos) << refused;

    // Without checkpointing nothing completes, so nothing stronger than
    // at-most-once can be asked for.
    g.ops[1].params["delivery_guarantee"] = "at_least_once";
    EXPECT_NE(
        clink::cluster::check_delivery_guarantee(g, clink::cluster::CheckpointConfig{}, &report)
            .find("checkpointing is disabled"),
        std::string::npos);
}

}  // namespace
