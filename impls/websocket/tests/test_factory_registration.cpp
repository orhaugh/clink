// Verifies clink::websocket::install() makes websocket_source_string
// reachable through the RunnerRegistry on the "string" channel, reads its
// options strictly, and declares a record the delivery analyser reads as
// at-most-once.

#include <functional>
#include <map>
#include <string>
#include <tuple>
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

std::string thrown(const std::function<void()>& f) {
    try {
        f();
    } catch (const std::exception& e) {
        return e.what();
    }
    return "";
}

TEST(WebSocketFactoryRegistration, SourceIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_source("websocket_source_string", "string"), nullptr);
}

// Building the source connects to nothing, so a value is refused here.
TEST(WebSocketFactoryRegistration, TheFactoryReadsItsOptionsStrictly) {
    const auto* f = clink::cluster::OperatorRegistry::default_instance().find_source(
        "websocket_source_string", std::string{clink::cluster::kChannelString});
    ASSERT_NE(f, nullptr);
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = {{"url", "ws://127.0.0.1:1/feed"}};
    ctx.subtask_idx = 1;  // dormant, so building never dials
    EXPECT_NO_THROW((void)f->build(ctx));
    auto every = ctx;
    every.params.insert({{"subscribe", R"({"op":"subscribe"})"},
                         {"read_block_ms", "60000"},
                         {"ping_interval_ms", "0"},
                         {"open_timeout_ms", "600000"},
                         {"max_messages", "10"},
                         {"reconnect", "false"},
                         {"reconnect_backoff_max_ms", "3600000"},
                         {"tls_verify", "false"}});
    EXPECT_NO_THROW((void)f->build(every));
    for (const auto& [key, value, why] :
         std::vector<std::tuple<std::string, std::string, std::string>>{
             {"read_block_ms", "0", "'read_block_ms' must be an integer"},
             {"ping_interval_ms", "-1", "'ping_interval_ms' must be an integer"},
             {"open_timeout_ms", "soon", "'open_timeout_ms' must be an integer"},
             {"max_messages", "-5", "'max_messages' must be an integer"},
             {"reconnect_backoff_max_ms", "0", "'reconnect_backoff_max_ms' must be an integer"},
             // reconnect='FALSE' once read as true, and tls_verify='FALSE' too.
             {"reconnect", "FALSE", "'reconnect' must be 'true' or 'false'"},
             {"tls_verify", "FALSE", "'tls_verify' must be 'true' or 'false'"}}) {
        auto c = ctx;
        c.params[key] = value;
        EXPECT_NE(thrown([&] { (void)f->build(c); }).find(why), std::string::npos)
            << key << "=" << value;
    }
}

// The record says what the client does: no handshake headers, the open
// timeout among the waits it bounds, and messages lost while it is down.
TEST(WebSocketFactoryRegistration, TheRecordStatesWhatTheSourceDoes) {
    const auto* caps = clink::connectors::CapabilityRegistry::instance().find("websocket");
    ASSERT_NE(caps, nullptr);
    EXPECT_EQ(caps->version, "2");
    EXPECT_EQ(caps->delivery, clink::connectors::DeliveryGuarantee::AtMostOnce);
    EXPECT_EQ(caps->auth_methods, (std::vector<std::string>{"none"}));
    EXPECT_EQ(caps->timeout_options,
              (std::vector<std::string>{"open_timeout_ms", "reconnect_backoff_max_ms"}));
    EXPECT_TRUE(caps->self_check().empty());
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

// A job over the source is at-most-once: asking a sink for at-least-once over
// it is refused, since nothing the feed sent can be read again.
TEST(WebSocketFactoryRegistration, AJobOverTheSourceRefusesAStrongerGuarantee) {
    clink::cluster::CheckpointConfig ckpt;
    ckpt.checkpoint_dir = "/tmp/ckpt";
    ckpt.interval_ms = 1000;
    ckpt.state_backend_uri = "file:///tmp/state";
    clink::cluster::JobGraphSpec g;
    g.ops.push_back(gate_op("websocket_source_string", "src"));
    g.ops.push_back(gate_op("file_2pc_sink_string",
                            "snk",
                            {"src"},
                            {{"dir", "/tmp/out"}, {"delivery_guarantee", "at_least_once"}}));
    clink::connectors::GuaranteeReport report;
    const auto refused = clink::cluster::check_delivery_guarantee(g, ckpt, &report);
    EXPECT_NE(refused, "") << report.render_text();
    EXPECT_NE(refused.find("websocket_source_string"), std::string::npos) << refused;
}

}  // namespace
