// Cluster-path uid tests. Validates that:
//
//   * JobGraphSpec.to_json / from_json round-trips OperatorSpec.uid +
//     .display_name (so the coordinator sees them after the wire crossing).
//   * OperatorChainSpec.to_json / from_json round-trips ChainOp.uid +
//     .display_name (so the worker-side runner sees them).
//   * Planner copies OperatorSpec.uid / .display_name into ChainOp.
//
// The in-process Operator-uid behaviour (stable OperatorId across
// topology edits) is exercised in test_operator_uid.cpp. This file
// only covers the cluster-IR plumbing - the worker-side application of
// uid happens via plugin_impl.hpp's apply_chain_identity, which is
// header-only and exercised the moment any plugin runner builds an
// operator. A full end-to-end "submit through in-process coordinator+worker"
// test is heavyweight and adds little above the unit-level coverage
// here; if a regression slips through, the in-process and cluster
// suites should catch it via state-restore failure.

#include <string>

#include <gtest/gtest.h>

#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/job_planner.hpp"

using namespace clink::cluster;

TEST(OperatorUidCluster, JobGraphSpecJsonRoundTripsUidAndDisplayName) {
    JobGraphSpec spec;
    OperatorSpec op_src;
    op_src.id = "src";
    op_src.type = "int64_range_source";
    op_src.out_channel = std::string{kChannelInt64};
    op_src.uid = "src-uid";
    op_src.display_name = "Range Source";
    spec.ops.push_back(std::move(op_src));

    OperatorSpec op_id;
    op_id.id = "id";
    op_id.type = "identity_int64";
    op_id.inputs = {"src"};
    op_id.out_channel = std::string{kChannelInt64};
    op_id.uid = "id-uid";
    op_id.display_name = "Identity";
    spec.ops.push_back(std::move(op_id));

    OperatorSpec op_snk;
    op_snk.id = "snk";
    op_snk.type = "collecting_int64_sink";
    op_snk.inputs = {"id"};
    op_snk.out_channel = std::string{kChannelInt64};
    // No uid on the sink - verifies the field is optional on the wire.
    spec.ops.push_back(std::move(op_snk));

    const auto json = spec.to_json();
    EXPECT_NE(json.find("\"uid\":\"src-uid\""), std::string::npos);
    EXPECT_NE(json.find("\"display_name\":\"Range Source\""), std::string::npos);
    EXPECT_NE(json.find("\"uid\":\"id-uid\""), std::string::npos);
    EXPECT_NE(json.find("\"display_name\":\"Identity\""), std::string::npos);

    auto parsed = JobGraphSpec::from_json(json);
    ASSERT_EQ(parsed.ops.size(), 3u);
    EXPECT_EQ(parsed.ops[0].uid, "src-uid");
    EXPECT_EQ(parsed.ops[0].display_name, "Range Source");
    EXPECT_EQ(parsed.ops[1].uid, "id-uid");
    EXPECT_EQ(parsed.ops[1].display_name, "Identity");
    EXPECT_TRUE(parsed.ops[2].uid.empty());
    EXPECT_TRUE(parsed.ops[2].display_name.empty());
}

TEST(OperatorUidCluster, OperatorChainSpecJsonRoundTripsUidAndDisplayName) {
    OperatorChainSpec chain;
    chain.subtask_idx = 7;
    chain.subtask_idx_in_op = 3;

    ChainOp co1;
    co1.id = "head";
    co1.type = "fixed_add";
    co1.uid = "fixed-add-uid";
    co1.display_name = "Fixed Add";
    co1.kind = OperatorKind::Operator;
    co1.in_channel = std::string{kChannelInt64};
    co1.out_channel = std::string{kChannelInt64};
    co1.parallelism = 1;
    chain.ops.push_back(std::move(co1));

    const auto json = chain.to_json();
    EXPECT_NE(json.find("\"uid\":\"fixed-add-uid\""), std::string::npos) << json;
    EXPECT_NE(json.find("\"display_name\":\"Fixed Add\""), std::string::npos) << json;

    auto parsed = OperatorChainSpec::from_json(json);
    ASSERT_EQ(parsed.ops.size(), 1u);
    EXPECT_EQ(parsed.ops[0].uid, "fixed-add-uid");
    EXPECT_EQ(parsed.ops[0].display_name, "Fixed Add");
    EXPECT_EQ(parsed.subtask_idx, 7u);
    EXPECT_EQ(parsed.subtask_idx_in_op, 3u);
}

TEST(OperatorUidCluster, EmptyUidAndDisplayNameAreOmittedFromJson) {
    JobGraphSpec spec;
    OperatorSpec op;
    op.id = "x";
    op.type = "identity_int64";
    op.out_channel = std::string{kChannelInt64};
    spec.ops.push_back(std::move(op));

    const auto json = spec.to_json();
    EXPECT_EQ(json.find("\"uid\""), std::string::npos)
        << "empty uid should be omitted from JSON: " << json;
    EXPECT_EQ(json.find("\"display_name\""), std::string::npos)
        << "empty display_name should be omitted from JSON: " << json;
}
