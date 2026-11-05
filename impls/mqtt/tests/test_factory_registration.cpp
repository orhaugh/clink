// Verifies clink::mqtt::install() makes the mqtt_source / mqtt_sink factories
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

TEST(MqttFactoryRegistration, SinkIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_sink("mqtt_sink", "string"), nullptr);
}

TEST(MqttFactoryRegistration, SourceIsRegistered) {
    const auto& rr = RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_source("mqtt_source", "string"), nullptr);
}

// The source and the sink carry different guarantees, so each has a record.
// The source acknowledges a message once a completed checkpoint covers it and
// keeps no position, so its record is the acknowledging sources' at-least-once.
// The sink keeps its own at-least-once.
TEST(MqttCapabilityRecords, TheSourceDeclaresWhatItDoesAndTheSinkKeepsItsOwn) {
    const auto& reg = clink::connectors::CapabilityRegistry::instance();
    const auto* source = reg.find("mqtt_source");
    ASSERT_NE(source, nullptr);
    EXPECT_TRUE(source->is_source);
    EXPECT_FALSE(source->is_sink);
    EXPECT_TRUE(source->checkpoint_integrated);
    EXPECT_FALSE(source->replayable);
    EXPECT_EQ(source->offset_model, clink::connectors::OffsetModel::Acknowledgement);
    EXPECT_EQ(source->delivery, clink::connectors::DeliveryGuarantee::AtLeastOnce);
    EXPECT_TRUE(source->self_check().empty());
    const auto* sink = reg.find("mqtt");
    ASSERT_NE(sink, nullptr);
    EXPECT_FALSE(sink->is_source);
    EXPECT_TRUE(sink->is_sink);
    EXPECT_EQ(sink->delivery, clink::connectors::DeliveryGuarantee::AtLeastOnce);
    EXPECT_TRUE(sink->self_check().empty());
    EXPECT_EQ(clink::cluster::connector_name_for_op_type("mqtt_source"), "mqtt_source");
    EXPECT_EQ(clink::cluster::connector_name_for_op_type("mqtt_sink"), "mqtt");
}

// With shared_group set, each subtask holds a session of the shared
// subscription, and a scale-down would strand what a removed subtask's session
// holds: the coordinator refuses it. Without shared_group only subtask 0
// subscribes, and the subtasks a scale-down removes hold no session.
TEST(MqttCapabilityRecords, OnlyASharedSubscriptionRefusesAScaleDown) {
    const auto* source = clink::connectors::CapabilityRegistry::instance().find("mqtt_source");
    ASSERT_NE(source, nullptr);
    EXPECT_FALSE(source->scale_down_refusal.empty());
    EXPECT_EQ(source->scale_down_refusal_param, "shared_group");
    EXPECT_EQ(clink::cluster::source_scale_down_refusal("mqtt_source",
                                                        {{"topic", "t"}, {"shared_group", "g"}}),
              source->scale_down_refusal)
        << "the coordinator would not refuse a scale-down of a shared subscription";
    EXPECT_EQ(clink::cluster::source_scale_down_refusal("mqtt_source", {{"topic", "t"}}), "");
    EXPECT_EQ(clink::cluster::source_scale_down_refusal("mqtt_source", {{"shared_group", ""}}), "");
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

TEST(MqttCapabilityRecords, AJobFromTheSourceIsAtLeastOnceAndExactlyOnceIsRefused) {
    clink::cluster::CheckpointConfig ckpt;
    ckpt.checkpoint_dir = "/tmp/ckpt";
    ckpt.interval_ms = 1000;
    ckpt.state_backend_uri = "file:///tmp/state";
    clink::cluster::JobGraphSpec from_mqtt;
    from_mqtt.ops.push_back(gate_op("mqtt_source", "src"));
    from_mqtt.ops.push_back(gate_op("file_2pc_sink_string",
                                    "snk",
                                    {"src"},
                                    {{"dir", "/tmp/o"}, {"delivery_guarantee", "at_least_once"}}));
    clink::connectors::GuaranteeReport report;
    EXPECT_EQ(clink::cluster::check_delivery_guarantee(from_mqtt, ckpt, &report), "");
    EXPECT_EQ(report.level, clink::connectors::EndToEndGuarantee::AtLeastOnceSource)
        << report.render_text();

    from_mqtt.ops[1].params["delivery_guarantee"] = "exactly_once";
    const auto refused = clink::cluster::check_delivery_guarantee(from_mqtt, ckpt, &report);
    EXPECT_NE(refused.find("AT_LEAST_ONCE_SOURCE"), std::string::npos) << refused;
    EXPECT_NE(refused.find("mqtt_source"), std::string::npos) << refused;

    clink::cluster::JobGraphSpec into_mqtt;
    into_mqtt.ops.push_back(gate_op("file_line_source", "src", {}, {{"path", "/tmp/i"}}));
    into_mqtt.ops.push_back(gate_op(
        "mqtt_sink", "snk", {"src"}, {{"topic", "t"}, {"delivery_guarantee", "at_least_once"}}));
    EXPECT_EQ(clink::cluster::check_delivery_guarantee(into_mqtt, ckpt, &report), "");
}

}  // namespace
