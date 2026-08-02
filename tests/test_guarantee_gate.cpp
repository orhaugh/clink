// Submission-time delivery-guarantee gate.
//
// The analyser's own logic is covered by test_connector_capability. What
// is under test here is the BRIDGE: does a real JobGraphSpec produce the
// right facts, does op-type-to-connector resolution pick the right record,
// and does a job that asks for more than it can have get refused.

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/guarantee_gate.hpp"

namespace {

using namespace clink;
using namespace clink::cluster;

OperatorSpec op(std::string type,
                std::string id,
                std::vector<std::string> inputs = {},
                std::map<std::string, std::string> params = {}) {
    OperatorSpec s;
    s.type = std::move(type);
    s.id = std::move(id);
    s.inputs = std::move(inputs);
    s.params = std::move(params);
    return s;
}

CheckpointConfig durable_checkpointing() {
    CheckpointConfig c;
    c.checkpoint_dir = "/tmp/ckpt";
    c.interval_ms = 1000;
    c.state_backend_uri = "file:///tmp/state";
    return c;
}

class GuaranteeGateTest : public ::testing::Test {
protected:
    void SetUp() override { ensure_built_ins_registered(); }
};

// --- op type -> connector name ---------------------------------------------

TEST_F(GuaranteeGateTest, LongestPrefixWinsSoTwoPhaseVariantsAreNotMistakenForPlainOnes) {
    // The single most consequential rule in the bridge. "file_2pc_sink_string"
    // starts with BOTH "file" and "file_2pc"; picking the shorter one would
    // report an at-least-once sink as exactly-once, or vice versa.
    EXPECT_EQ(connector_name_for_op_type("file_2pc_sink_string"), "file_2pc");
    EXPECT_EQ(connector_name_for_op_type("file_line_sink"), "file");
    EXPECT_EQ(connector_name_for_op_type("parquet_2pc_sink_row"), "parquet_2pc");
    EXPECT_EQ(connector_name_for_op_type("parquet_int64_sink"), "parquet");
}

TEST_F(GuaranteeGateTest, AnUnknownOpTypeResolvesToNothing) {
    EXPECT_TRUE(connector_name_for_op_type("some_third_party_sink").empty());
}

// --- graph -> facts ---------------------------------------------------------

TEST_F(GuaranteeGateTest, RolesComeFromTheGraphNotFromTheTypeName) {
    // Source = no inputs; sink = nothing consumes it. Derived from edges,
    // because a name-based guess ("*_sink") misses every connector that
    // does not follow the convention.
    JobGraphSpec g;
    g.ops.push_back(op("file_line_source", "src"));
    g.ops.push_back(op("identity_string", "mid", {"src"}));
    g.ops.push_back(op("file_line_sink", "snk", {"mid"}));

    const auto facts = pipeline_facts_from_graph(g, durable_checkpointing());
    // The mid-chain operator has no delivery semantics and must not appear.
    ASSERT_EQ(facts.connectors.size(), 2U);
    EXPECT_TRUE(facts.connectors[0].is_source);
    EXPECT_FALSE(facts.connectors[1].is_source);
}

TEST_F(GuaranteeGateTest, MemoryBackendIsNotCountedAsDurable) {
    JobGraphSpec g;
    g.ops.push_back(op("file_line_source", "src"));
    g.ops.push_back(op("file_2pc_sink_string", "snk", {"src"}, {{"dir", "/tmp/o"}}));

    CheckpointConfig c = durable_checkpointing();
    c.state_backend_uri = "memory://";
    const auto facts = pipeline_facts_from_graph(g, c);
    EXPECT_FALSE(facts.durable_state_backend)
        << "memory:// state does not survive a process restart and must not count as durable";
}

TEST_F(GuaranteeGateTest, NoCheckpointIntervalMeansCheckpointingIsOff) {
    JobGraphSpec g;
    g.ops.push_back(op("file_line_source", "src"));
    g.ops.push_back(op("file_line_sink", "snk", {"src"}));
    CheckpointConfig c = durable_checkpointing();
    c.interval_ms = 0;
    EXPECT_FALSE(pipeline_facts_from_graph(g, c).checkpointing_enabled);
}

TEST_F(GuaranteeGateTest, AnUndeclaredConnectorIsMarkedMissingNotDropped) {
    JobGraphSpec g;
    g.ops.push_back(op("file_line_source", "src"));
    g.ops.push_back(op("some_third_party_sink", "snk", {"src"}));

    const auto facts = pipeline_facts_from_graph(g, durable_checkpointing());
    ASSERT_EQ(facts.connectors.size(), 2U);
    // Dropping it would let an unrecognised sink silently not count towards
    // the weakest link, which is the failure this whole gate exists to stop.
    EXPECT_TRUE(facts.connectors[1].declaration_missing);
}

TEST_F(GuaranteeGateTest, TheStrongestRequestedGuaranteeAcrossTheGraphIsTheOneAsked) {
    JobGraphSpec g;
    g.ops.push_back(op("file_line_source", "src"));
    g.ops.push_back(
        op("file_line_sink", "weak", {"src"}, {{"delivery_guarantee", "at_least_once"}}));
    g.ops.push_back(op("file_2pc_sink_string",
                       "strong",
                       {"src"},
                       {{"dir", "/tmp/o"}, {"delivery_guarantee", "exactly_once"}}));

    const auto facts = pipeline_facts_from_graph(g, durable_checkpointing());
    ASSERT_TRUE(facts.requested.has_value());
    EXPECT_EQ(*facts.requested, connectors::DeliveryGuarantee::ExactlyOnceTwoPhaseCommit);
}

// --- the decision -----------------------------------------------------------

TEST_F(GuaranteeGateTest, AWeakJobThatAsksForNothingIsAllowed) {
    // Most jobs are at-least-once and that is a legitimate choice. The gate
    // must not turn "weak" into "rejected".
    JobGraphSpec g;
    g.ops.push_back(op("file_line_source", "src"));
    g.ops.push_back(op("file_line_sink", "snk", {"src"}));
    connectors::GuaranteeReport report;
    EXPECT_TRUE(check_delivery_guarantee(g, durable_checkpointing(), &report).empty());
    EXPECT_EQ(report.level, connectors::EndToEndGuarantee::StateExactlyOnceOutputAtLeastOnce);
}

TEST_F(GuaranteeGateTest, ExactlyOnceIsAllowedWhenThePipelineCanActuallyProvideIt) {
    JobGraphSpec g;
    g.ops.push_back(op("file_line_source", "src"));
    g.ops.push_back(op("file_2pc_sink_string",
                       "snk",
                       {"src"},
                       {{"dir", "/tmp/o"}, {"delivery_guarantee", "exactly_once"}}));
    connectors::GuaranteeReport report;
    const auto reject = check_delivery_guarantee(g, durable_checkpointing(), &report);
    EXPECT_TRUE(reject.empty()) << reject;
    EXPECT_EQ(report.level, connectors::EndToEndGuarantee::EndToEndExactlyOnce);
}

TEST_F(GuaranteeGateTest, ExactlyOnceIsRejectedWhenASinkCannotProvideIt) {
    JobGraphSpec g;
    g.ops.push_back(op("file_line_source", "src"));
    g.ops.push_back(op("file_line_sink", "snk", {"src"}, {{"delivery_guarantee", "exactly_once"}}));
    const auto reject = check_delivery_guarantee(g, durable_checkpointing(), nullptr);
    ASSERT_FALSE(reject.empty()) << "a plain append sink cannot provide exactly-once";
    // The message must be actionable: what was asked, and what caps it.
    EXPECT_NE(reject.find("exactly_once_two_phase_commit"), std::string::npos);
    EXPECT_NE(reject.find("file_line_sink"), std::string::npos);
}

TEST_F(GuaranteeGateTest, ExactlyOnceIsRejectedWhenCheckpointingIsOff) {
    JobGraphSpec g;
    g.ops.push_back(op("file_line_source", "src"));
    g.ops.push_back(op("file_2pc_sink_string",
                       "snk",
                       {"src"},
                       {{"dir", "/tmp/o"}, {"delivery_guarantee", "exactly_once"}}));
    CheckpointConfig c = durable_checkpointing();
    c.interval_ms = 0;
    const auto reject = check_delivery_guarantee(g, c, nullptr);
    ASSERT_FALSE(reject.empty());
    EXPECT_NE(reject.find("checkpointing is disabled"), std::string::npos);
}

TEST_F(GuaranteeGateTest, ExactlyOnceIsRejectedWhenTheSinkIsMissingItsRequiredOption) {
    // file_2pc declares 'dir' as required for its mechanism. Without it the
    // sink cannot stage anything, so its declared level must not be taken
    // at face value.
    JobGraphSpec g;
    g.ops.push_back(op("file_line_source", "src"));
    g.ops.push_back(
        op("file_2pc_sink_string", "snk", {"src"}, {{"delivery_guarantee", "exactly_once"}}));
    const auto reject = check_delivery_guarantee(g, durable_checkpointing(), nullptr);
    EXPECT_FALSE(reject.empty())
        << "a 2PC sink with no 'dir' cannot stage, so exactly-once must be refused";
}

TEST_F(GuaranteeGateTest, ExactlyOnceIsRejectedOnANonDurableStateBackend) {
    JobGraphSpec g;
    g.ops.push_back(op("file_line_source", "src"));
    g.ops.push_back(op("file_2pc_sink_string",
                       "snk",
                       {"src"},
                       {{"dir", "/tmp/o"}, {"delivery_guarantee", "exactly_once"}}));
    CheckpointConfig c = durable_checkpointing();
    c.state_backend_uri = "memory://";
    const auto reject = check_delivery_guarantee(g, c, nullptr);
    ASSERT_FALSE(reject.empty());
    EXPECT_NE(reject.find("non-durable state backend"), std::string::npos);
}

}  // namespace
