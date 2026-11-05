// Verifies clink::redis::install() makes the redis_source / redis_sink factories
// reachable through the RunnerRegistry on the "string" channel.

#include <map>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/guarantee_gate.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/runner_registry.hpp"
#include "clink/connectors/capability.hpp"
#include "clink/connectors/delivery_guarantee.hpp"

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

// The record the delivery analyser and the coordinator read: the source
// acknowledges at checkpoint completion and the server delivers again what was
// not acknowledged, so a job over it is at-least-once. It used to declare no
// position model, which the analyser read as at-most-once, refusing even an
// at_least_once request.
TEST(SourceCapabilityRecord, TheSourceAcknowledgesAtCheckpointCompletion) {
    const auto* caps = clink::connectors::CapabilityRegistry::instance().find("redis");
    ASSERT_NE(caps, nullptr);
    EXPECT_EQ(caps->offset_model, clink::connectors::OffsetModel::Acknowledgement);
    EXPECT_FALSE(caps->replayable);
    EXPECT_TRUE(caps->checkpoint_integrated);
    EXPECT_TRUE(caps->self_check().empty());
    EXPECT_FALSE(caps->scale_down_refusal.empty());
    EXPECT_EQ(clink::cluster::source_scale_down_refusal("redis_source"), caps->scale_down_refusal)
        << "the coordinator would not refuse a scale-down of the source";
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
    g.ops.push_back(gate_op("redis_source", "src"));
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
    EXPECT_NE(refused.find("redis_source"), std::string::npos) << refused;

    // Without checkpointing nothing completes, so nothing stronger than
    // at-most-once can be asked for.
    g.ops[1].params["delivery_guarantee"] = "at_least_once";
    EXPECT_NE(
        clink::cluster::check_delivery_guarantee(g, clink::cluster::CheckpointConfig{}, &report)
            .find("checkpointing is disabled"),
        std::string::npos);
}

}  // namespace
