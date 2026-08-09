// JobPlanner translates a JobGraphSpec (logical) into a JobPlan
// (physical) using the default OperatorRegistry. It owns:
//   - topology validation (cycle detection, dangling refs, fan-out
//     constraints for v1)
//   - parallelism expansion (v1: must be 1 per op)
//   - per-subtask OperatorChainSpec encoding into extra_config
// These tests pin the contracts the coordinator relies on for slot accounting,
// peer-ref bookkeeping, and dispatch.

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/job_planner.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/rescale_dispatch.hpp"
#include "clink/cluster/runner_registry.hpp"
#include "clink/cluster/type_registry.hpp"
#include "clink/core/codec.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/runtime/key_groups.hpp"

using namespace clink::cluster;

namespace {

JobGraphSpec linear_int64_graph() {
    JobGraphSpec g;
    g.ops.push_back(OperatorSpec{
        .type = "int64_range_source",
        .id = "src",
        .inputs = {},
        .parallelism = 1,
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"count", "5"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "file_int64_sink",
        .id = "snk",
        .inputs = {"src"},
        .parallelism = 1,
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"path", "/tmp/x"}},
    });
    return g;
}

}  // namespace

TEST(JobPlanner, LinearGraphProducesOneTaskPerOpUnderGenericRole) {
    auto g = linear_int64_graph();
    auto plan = plan_job(g, OperatorRegistry::default_instance());
    ASSERT_EQ(plan.tasks.size(), 2u);
    EXPECT_EQ(plan.tasks[0].role, kGenericSubtaskRole);
    EXPECT_EQ(plan.tasks[1].role, kGenericSubtaskRole);

    // Each subtask gets a distinct subtask_idx.
    EXPECT_NE(plan.tasks[0].subtask_idx, plan.tasks[1].subtask_idx);

    // The source has no input edges; sink has one input edge from src.
    auto src_chain = OperatorChainSpec::from_json(plan.tasks[0].extra_config);
    auto snk_chain = OperatorChainSpec::from_json(plan.tasks[1].extra_config);
    EXPECT_TRUE(src_chain.input_edges.empty());
    EXPECT_EQ(snk_chain.input_edges.size(), 1u);
    ASSERT_EQ(src_chain.output_groups.size(), 1u);
    EXPECT_EQ(src_chain.output_groups.front().edges.size(), 1u);
    EXPECT_TRUE(snk_chain.output_groups.empty());

    // Edge channel types must agree on the boundary.
    EXPECT_EQ(src_chain.output_groups.front().edges.front().channel_type,
              std::string{clink::cluster::kChannelInt64});
    EXPECT_EQ(snk_chain.input_edges.front().channel_type,
              std::string{clink::cluster::kChannelInt64});
}

TEST(JobPlanner, SourceTaskCarriesPeerRefToSink) {
    auto g = linear_int64_graph();
    auto plan = plan_job(g, OperatorRegistry::default_instance());
    // Producer (source) holds peer_refs to its downstream so coordinator can
    // hand it the sink's address once the sink reports listening.
    EXPECT_EQ(plan.tasks[0].peer_refs.size(), 1u);
    EXPECT_EQ(plan.tasks[1].peer_refs.size(), 0u);
}

TEST(JobPlanner, MissingInputRefIsRejected) {
    JobGraphSpec g;
    g.ops.push_back(OperatorSpec{
        .type = "int64_range_source",
        .id = "src",
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"count", "5"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "file_int64_sink",
        .id = "snk",
        .inputs = {"ghost"},  // unknown id
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"path", "/tmp/x"}},
    });
    EXPECT_THROW((void)plan_job(g, OperatorRegistry::default_instance()), std::runtime_error);
}

TEST(JobPlanner, CycleIsRejected) {
    JobGraphSpec g;
    g.ops.push_back(OperatorSpec{
        .type = "noop_a",
        .id = "a",
        .inputs = {"b"},
        .out_channel = std::string{clink::cluster::kChannelInt64},
    });
    g.ops.push_back(OperatorSpec{
        .type = "noop_b",
        .id = "b",
        .inputs = {"a"},
        .out_channel = std::string{clink::cluster::kChannelInt64},
    });
    EXPECT_THROW((void)plan_job(g, OperatorRegistry::default_instance()), std::runtime_error);
}

TEST(JobPlanner, EqualParallelismOnAllOpsExpandsToNSubtasksEach) {
    auto g = linear_int64_graph();
    g.ops[0].parallelism = 3;
    g.ops[1].parallelism = 3;
    auto plan = plan_job(g, OperatorRegistry::default_instance());
    // 3 source subtasks + 3 sink subtasks.
    ASSERT_EQ(plan.tasks.size(), 6u);
    // Subtask indices must be unique.
    std::set<std::uint32_t> idxs;
    for (const auto& t : plan.tasks) {
        idxs.insert(t.subtask_idx);
    }
    EXPECT_EQ(idxs.size(), 6u);
}

TEST(JobPlanner, MismatchedParallelismEmitsRebalanceGroup) {
    // src (par 1) -> snk (par 2): the planner emits a Rebalance output
    // group on the source so records round-robin across the 2 sinks.
    auto g = linear_int64_graph();
    g.ops[0].parallelism = 1;
    g.ops[1].parallelism = 2;
    auto plan = plan_job(g, OperatorRegistry::default_instance());
    ASSERT_EQ(plan.tasks.size(), 3u);  // 1 source + 2 sink subtasks

    // The source subtask has one output group, mode = Rebalance, with
    // 2 edges (one per sink subtask).
    bool found_rebalance = false;
    for (const auto& t : plan.tasks) {
        auto chain = OperatorChainSpec::from_json(t.extra_config);
        if (chain.output_groups.empty()) {
            continue;
        }
        for (const auto& gg : chain.output_groups) {
            if (gg.mode == RoutingMode::Rebalance) {
                EXPECT_EQ(gg.edges.size(), 2u);
                found_rebalance = true;
            }
        }
    }
    EXPECT_TRUE(found_rebalance);
}

TEST(JobPlanner, KeyByOnDownstreamEmitsHashRoutingFromUpstream) {
    // src (par 1) -> identity_int64 (par 2, key_by="identity"):
    // the planner must emit a Hash output group on the source with
    // key_extractor_fn carrying the extractor name, so each upstream
    // subtask hash-partitions records across the 2 downstream subtasks.
    JobGraphSpec g;
    g.ops.push_back(OperatorSpec{
        .type = "int64_range_source",
        .id = "src",
        .inputs = {},
        .parallelism = 1,
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"count", "5"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "identity_int64",
        .id = "mid",
        .inputs = {"src"},
        .parallelism = 2,
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .key_by = "identity",
    });
    g.ops.push_back(OperatorSpec{
        .type = "file_int64_sink",
        .id = "snk",
        .inputs = {"mid"},
        .parallelism = 1,
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"path", "/tmp/x"}},
    });

    auto plan = plan_job(g, OperatorRegistry::default_instance());

    bool found_hash = false;
    for (const auto& t : plan.tasks) {
        auto chain = OperatorChainSpec::from_json(t.extra_config);
        for (const auto& gg : chain.output_groups) {
            if (gg.mode == RoutingMode::Hash) {
                EXPECT_EQ(gg.key_extractor_fn, "identity");
                EXPECT_EQ(gg.edges.size(), 2u)
                    << "hash group must fan out to every downstream subtask";
                found_hash = true;
            }
        }
    }
    EXPECT_TRUE(found_hash) << "planner should emit a Hash group when downstream "
                            << "has key_by set";
}

TEST(JobPlanner, KeyedHeadGetsFanInOnInputEdgesAtParallelismOne) {
    // A keyed downstream still needs every upstream subtask listed in
    // its input_edges so it can listen on N inbound bridges, even if
    // upstream parallelism == downstream parallelism. (At par=1 both,
    // forward would normally collapse to a single edge.)
    JobGraphSpec g;
    g.ops.push_back(OperatorSpec{
        .type = "int64_range_source",
        .id = "src",
        .parallelism = 2,
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"count", "4"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "identity_int64",
        .id = "keyed",
        .inputs = {"src"},
        .parallelism = 2,
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .key_by = "identity",
    });
    g.ops.push_back(OperatorSpec{
        .type = "file_int64_sink",
        .id = "snk",
        .inputs = {"keyed"},
        .parallelism = 1,
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"path", "/tmp/x"}},
    });

    auto plan = plan_job(g, OperatorRegistry::default_instance());
    bool checked = false;
    for (const auto& t : plan.tasks) {
        auto chain = OperatorChainSpec::from_json(t.extra_config);
        if (chain.ops.empty() || chain.ops[0].type != "identity_int64") {
            continue;
        }
        // Each keyed downstream subtask must receive from BOTH upstream
        // subtasks, not the same-indexed one only.
        EXPECT_EQ(chain.input_edges.size(), 2u);
        checked = true;
    }
    EXPECT_TRUE(checked);
}

TEST(JobPlanner, UnknownOpTypeIsRejected) {
    JobGraphSpec g;
    g.ops.push_back(OperatorSpec{
        .type = "totally_unknown",
        .id = "src",
        .out_channel = std::string{clink::cluster::kChannelInt64},
    });
    g.ops.push_back(OperatorSpec{
        .type = "file_int64_sink",
        .id = "snk",
        .inputs = {"src"},
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"path", "/tmp/x"}},
    });
    EXPECT_THROW((void)plan_job(g, OperatorRegistry::default_instance()), std::runtime_error);
}

TEST(JobGraphSpecJson, RoundTripsThroughJson) {
    auto g = linear_int64_graph();
    const auto j = g.to_json();
    auto g2 = JobGraphSpec::from_json(j);
    ASSERT_EQ(g2.ops.size(), g.ops.size());
    EXPECT_EQ(g2.ops[0].id, g.ops[0].id);
    EXPECT_EQ(g2.ops[0].type, g.ops[0].type);
    EXPECT_EQ(g2.ops[0].out_channel, g.ops[0].out_channel);
    EXPECT_EQ(g2.ops[1].inputs, g.ops[1].inputs);
    EXPECT_EQ(g2.ops[1].params.at("path"), g.ops[1].params.at("path"));
}

TEST(JobPlanner, MidChainOperatorIsAccepted) {
    // src -> multiply_int64 (factor=3) -> sink
    JobGraphSpec g;
    g.ops.push_back(OperatorSpec{
        .type = "int64_range_source",
        .id = "src",
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"count", "5"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "multiply_int64",
        .id = "mul",
        .inputs = {"src"},
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"factor", "3"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "file_int64_sink",
        .id = "snk",
        .inputs = {"mul"},
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"path", "/tmp/x"}},
    });
    auto plan = plan_job(g, OperatorRegistry::default_instance());
    ASSERT_EQ(plan.tasks.size(), 3u);

    // The middle subtask must encode a chain spec with kind=operator
    // and an input edge from src + output edge to snk.
    const auto mid_chain = OperatorChainSpec::from_json(plan.tasks[1].extra_config);
    ASSERT_EQ(mid_chain.ops.size(), 1u);
    EXPECT_EQ(mid_chain.ops[0].kind, OperatorKind::Operator);
    EXPECT_EQ(mid_chain.ops[0].in_channel, std::string{clink::cluster::kChannelInt64});
    EXPECT_EQ(mid_chain.ops[0].out_channel, std::string{clink::cluster::kChannelInt64});
    EXPECT_EQ(mid_chain.input_edges.size(), 1u);
    ASSERT_EQ(mid_chain.output_groups.size(), 1u);
    EXPECT_EQ(mid_chain.output_groups.front().edges.size(), 1u);
}

TEST(JobPlanner, CrossChannelTypeOperatorIsAccepted) {
    // string_source -> string_to_int64 -> int64_sink: the channel type
    // changes across the operator.
    JobGraphSpec g;
    g.ops.push_back(OperatorSpec{
        .type = "string_lines_source",
        .id = "src",
        .out_channel = std::string{clink::cluster::kChannelString},
        .params = {{"lines", "1,2,3"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "string_to_int64",
        .id = "parse",
        .inputs = {"src"},
        .out_channel = std::string{clink::cluster::kChannelInt64},
    });
    g.ops.push_back(OperatorSpec{
        .type = "file_int64_sink",
        .id = "snk",
        .inputs = {"parse"},
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"path", "/tmp/x"}},
    });
    auto plan = plan_job(g, OperatorRegistry::default_instance());
    ASSERT_EQ(plan.tasks.size(), 3u);

    const auto mid_chain = OperatorChainSpec::from_json(plan.tasks[1].extra_config);
    EXPECT_EQ(mid_chain.ops[0].in_channel, std::string{clink::cluster::kChannelString});
    EXPECT_EQ(mid_chain.ops[0].out_channel, std::string{clink::cluster::kChannelInt64});
}

TEST(JobPlanner, AdjacentOperatorsAreChainedIntoOneSubtask) {
    // src -> mul(2) -> mul(5) -> sink. The two mul ops are mid-chain
    // Operator-kind with a single consumer and single input each, so
    // the planner packs them into one chain. Result: 3 subtasks total
    // (source, chained-mul, sink) instead of 4.
    JobGraphSpec g;
    g.ops.push_back(OperatorSpec{
        .type = "int64_range_source",
        .id = "src",
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"count", "4"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "multiply_int64",
        .id = "mulA",
        .inputs = {"src"},
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"factor", "2"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "multiply_int64",
        .id = "mulB",
        .inputs = {"mulA"},
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"factor", "5"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "file_int64_sink",
        .id = "snk",
        .inputs = {"mulB"},
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"path", "/tmp/x"}},
    });
    auto plan = plan_job(g, OperatorRegistry::default_instance());
    ASSERT_EQ(plan.tasks.size(), 3u);  // chain folds the two mul ops.

    // Find the subtask whose extra_config carries 2 chained ops.
    bool found_chain = false;
    for (const auto& t : plan.tasks) {
        auto chain = OperatorChainSpec::from_json(t.extra_config);
        if (chain.ops.size() == 2) {
            EXPECT_EQ(chain.ops[0].id, "mulA");
            EXPECT_EQ(chain.ops[1].id, "mulB");
            EXPECT_EQ(chain.ops[0].out_channel, chain.ops[1].in_channel);
            found_chain = true;
        }
    }
    EXPECT_TRUE(found_chain);
}

TEST(JobPlanner, SplitOpEmitsSplitOutputRouting) {
    // src -> splitter (selector_fn=int64_even_odd) -> evens (branch 0)
    //                                              -> odds  (branch 1)
    JobGraphSpec g;
    g.ops.push_back(OperatorSpec{
        .type = "int64_range_source",
        .id = "src",
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"count", "4"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "identity_int64",
        .id = "splitter",
        .inputs = {"src"},
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"selector_fn", "int64_even_odd"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "file_int64_sink",
        .id = "evens",
        .inputs = {"splitter.0"},
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"path", "/tmp/e"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "file_int64_sink",
        .id = "odds",
        .inputs = {"splitter.1"},
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"path", "/tmp/o"}},
    });
    auto plan = plan_job(g, OperatorRegistry::default_instance());
    bool found_split = false;
    for (const auto& t : plan.tasks) {
        auto chain = OperatorChainSpec::from_json(t.extra_config);
        if (chain.output_routing == OperatorChainSpec::OutputRouting::Split) {
            EXPECT_EQ(chain.output_selector_fn, "int64_even_odd");
            EXPECT_EQ(chain.output_groups.size(), 2u);
            found_split = true;
        }
    }
    EXPECT_TRUE(found_split);
}

TEST(JobPlanner, PluginRegisteredJoinIsClassifiedAsKindJoin) {
    // Verifies the registry-driven is_join_op_type path: a plugin can
    // register a SubtaskRunner under a new op_type name via
    // RunnerRegistry::register_join, and the planner will classify
    // ops of that type as OperatorKind::Join without any hardcoded
    // string match in the planner.
    clink::cluster::RunnerRegistry rr(&clink::cluster::RunnerRegistry::default_instance());
    rr.register_join("custom_int64_join",
                     std::string{clink::cluster::kChannelInt64},
                     std::string{clink::cluster::kChannelInt64},
                     std::string{clink::cluster::kChannelString},
                     [](const clink::cluster::RunnerContext&) {
                         // unused - we only assert the planner classification here.
                     });

    JobGraphSpec g;
    g.ops.push_back(OperatorSpec{
        .type = "int64_range_source",
        .id = "left",
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"count", "1"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "int64_range_source",
        .id = "right",
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"count", "1"}, {"start", "2"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "custom_int64_join",
        .id = "j",
        .inputs = {"left", "right"},
        .out_channel = std::string{clink::cluster::kChannelString},
    });
    g.ops.push_back(OperatorSpec{
        .type = "file_line_sink",
        .id = "snk",
        .inputs = {"j"},
        .out_channel = std::string{clink::cluster::kChannelString},
        .params = {{"path", "/tmp/j_plugin"}},
    });
    auto plan = clink::cluster::plan_job(g, OperatorRegistry::default_instance(), rr);
    bool found = false;
    for (const auto& t : plan.tasks) {
        auto chain = OperatorChainSpec::from_json(t.extra_config);
        for (const auto& cop : chain.ops) {
            if (cop.type == "custom_int64_join") {
                EXPECT_EQ(cop.kind, OperatorKind::Join);
                found = true;
            }
        }
    }
    EXPECT_TRUE(found);
}

TEST(JobPlanner, JoinOpIsKindJoin) {
    JobGraphSpec g;
    g.ops.push_back(OperatorSpec{
        .type = "int64_range_source",
        .id = "left",
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"count", "3"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "int64_range_source",
        .id = "right",
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"count", "3"}, {"start", "2"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "int64_int64_match_join",
        .id = "j",
        .inputs = {"left", "right"},
        .out_channel = std::string{clink::cluster::kChannelString},
    });
    g.ops.push_back(OperatorSpec{
        .type = "file_line_sink",
        .id = "snk",
        .inputs = {"j"},
        .out_channel = std::string{clink::cluster::kChannelString},
        .params = {{"path", "/tmp/j"}},
    });
    auto plan = plan_job(g, OperatorRegistry::default_instance());
    bool found_join = false;
    for (const auto& t : plan.tasks) {
        auto chain = OperatorChainSpec::from_json(t.extra_config);
        for (const auto& cop : chain.ops) {
            if (cop.kind == OperatorKind::Join) {
                EXPECT_EQ(cop.type, "int64_int64_match_join");
                EXPECT_EQ(cop.out_channel, std::string{clink::cluster::kChannelString});
                found_join = true;
            }
        }
    }
    EXPECT_TRUE(found_join);
}

TEST(JobPlanner, TwoInputOpStampsInputIndexPerSideAtParallelism) {
    // Regression for the distributed two-input co-operator/join bug: at par>1
    // each side contributes one input edge per upstream subtask, so the runner
    // must know which edges belong to In1 vs In2. The planner stamps each edge
    // with input_index (0 = first input, 1 = second). Two par=2 sources -> a
    // par=2 two-input op: its input_edges must carry index 0 for the first
    // upstream's edges and index 1 for the second's (2 each, fanned in).
    JobGraphSpec g;
    g.ops.push_back(OperatorSpec{
        .type = "int64_range_source",
        .id = "left",
        .parallelism = 2,
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"count", "4"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "int64_range_source",
        .id = "right",
        .parallelism = 2,
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"count", "4"}, {"start", "2"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "int64_int64_match_join",
        .id = "j",
        .inputs = {"left", "right"},
        .parallelism =
            3,  // != upstream par -> fan-in (each side's 2 subtasks -> every join subtask)
        .out_channel = std::string{clink::cluster::kChannelString},
    });
    g.ops.push_back(OperatorSpec{
        .type = "file_line_sink",
        .id = "snk",
        .inputs = {"j"},
        .parallelism = 1,
        .out_channel = std::string{clink::cluster::kChannelString},
        .params = {{"path", "/tmp/j"}},
    });
    auto plan = plan_job(g, OperatorRegistry::default_instance());

    bool checked_a_join_subtask = false;
    for (const auto& t : plan.tasks) {
        auto chain = OperatorChainSpec::from_json(t.extra_config);
        const bool is_join =
            !chain.ops.empty() && chain.ops.front().type == "int64_int64_match_join";
        if (!is_join) {
            continue;
        }
        checked_a_join_subtask = true;
        // Survives a JSON round-trip (input_index is serialized).
        chain = OperatorChainSpec::from_json(chain.to_json());
        std::size_t n0 = 0, n1 = 0;
        for (const auto& e : chain.input_edges) {
            EXPECT_LT(e.input_index, 2u) << "input_index must be 0 (In1) or 1 (In2)";
            if (e.input_index == 0) {
                ++n0;
            } else if (e.input_index == 1) {
                ++n1;
            }
        }
        // Each side fans in from its 2 upstream subtasks.
        EXPECT_EQ(n0, 2u) << "In1 (left) should contribute 2 edges at par 2";
        EXPECT_EQ(n1, 2u) << "In2 (right) should contribute 2 edges at par 2";
    }
    EXPECT_TRUE(checked_a_join_subtask) << "no join subtask found in the plan";
}

TEST(JobPlanner, SplitOpWithoutSelectorFnIsRejected) {
    JobGraphSpec g;
    g.ops.push_back(OperatorSpec{
        .type = "int64_range_source",
        .id = "src",
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"count", "1"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "identity_int64",
        .id = "s",
        .inputs = {"src"},
        .out_channel = std::string{clink::cluster::kChannelInt64},
        // missing selector_fn
    });
    g.ops.push_back(OperatorSpec{
        .type = "file_int64_sink",
        .id = "snk",
        .inputs = {"s.0"},
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"path", "/tmp/x"}},
    });
    EXPECT_THROW((void)plan_job(g, OperatorRegistry::default_instance()), std::runtime_error);
}

// ---------------------------------------------------------------------
// M1 - Side-output channel resolution.
// ---------------------------------------------------------------------
//
// These three tests pin the contract that `plan_job` correctly resolves
// an input edge's channel type when the edge takes the "<op_id>::<tag>"
// side-output form, instead of falling back to the upstream op's main
// `out_channel`. Pre-fix, the planner's 1-input operator validation
// (and the 2-input co-operator validation, and the sink validation)
// failed to parse the `::tag` suffix and so silently used the wrong
// channel for the (op_type, in, out) factory lookup. This broke any
// topology that fed a typed side output into a downstream consumer with
// a different element type - the CDC-dispatcher-feeds-typed-co-operator
// pattern is the canonical example.

namespace {

// Test-only CoOperator. Same shape as test_plugin_registry.cpp's
// DummyCoOp; copied locally so the planner test compiles standalone.
class StringStringIntCoOp final : public clink::CoOperator<std::string, std::string, std::int64_t> {
public:
    void process_element1(const clink::StreamElement<std::string>&,
                          clink::Emitter<std::int64_t>&) override {}
    void process_element2(const clink::StreamElement<std::string>&,
                          clink::Emitter<std::int64_t>&) override {}
    std::string name() const override { return "string_string_int_co_op"; }
};

}  // namespace

TEST(JobPlanner, OpWithSideOutputCanBeChainedWithMainConsumer) {
    // Before 2026-05-22 the planner excluded ANY side-output-emitting
    // op from chaining, even when its main output had exactly one
    // downstream. That cost an extra subtask + thread per such op.
    // Now: an op with side outputs whose MAIN output has exactly one
    // consumer DOES chain with that consumer; side outputs ride on
    // the chain's outbound groups (one per side tag).
    JobGraphSpec g;
    g.ops.push_back(OperatorSpec{
        .type = "string_lines_source",
        .id = "src",
        .out_channel = std::string{kChannelString},
        .params = {{"lines", "a,b,c"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "identity_string",
        .id = "p",
        .inputs = {"src"},
        .out_channel = std::string{kChannelString},
        .side_outputs = {{.tag = "errors", .channel_type = std::string{kChannelInt64}}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "identity_string",
        .id = "q",
        .inputs = {"p"},
        .out_channel = std::string{kChannelString},
    });
    g.ops.push_back(OperatorSpec{
        .type = "file_line_sink",
        .id = "main_snk",
        .inputs = {"q"},
        .out_channel = std::string{kChannelString},
        .params = {{"path", "/tmp/m"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "file_int64_sink",
        .id = "err_snk",
        .inputs = {"p::errors"},
        .out_channel = std::string{kChannelInt64},
        .params = {{"path", "/tmp/e"}},
    });
    auto plan = plan_job(g, OperatorRegistry::default_instance());

    // Find the chain containing p; it should also include q (and the
    // sink-led chain folds main_snk on top of that). The chain spec's
    // side_outputs list should carry the "errors" tag, and one of
    // the chain's outbound output_groups should be tagged "errors"
    // pointing at err_snk.
    bool found_chain_with_p = false;
    for (const auto& t : plan.tasks) {
        auto chain = OperatorChainSpec::from_json(t.extra_config);
        bool has_p = false;
        bool has_q = false;
        for (const auto& cop : chain.ops) {
            if (cop.id == "p")
                has_p = true;
            if (cop.id == "q")
                has_q = true;
        }
        if (has_p) {
            EXPECT_TRUE(has_q) << "p should now be chained with its main consumer q";
            // Side output for the "errors" tag should appear in
            // both the inner-op declaration list and as a tagged
            // outbound group.
            bool inner_has_errors = false;
            for (const auto& cop : chain.ops) {
                if (cop.id == "p") {
                    for (const auto& s : cop.side_outputs) {
                        if (s.tag == "errors")
                            inner_has_errors = true;
                    }
                }
            }
            EXPECT_TRUE(inner_has_errors);
            bool outbound_has_errors = false;
            for (const auto& g : chain.output_groups) {
                if (g.side_output_tag == "errors")
                    outbound_has_errors = true;
            }
            EXPECT_TRUE(outbound_has_errors) << "chain must have an outbound group "
                                                "for the side-output tag";
            found_chain_with_p = true;
        }
    }
    EXPECT_TRUE(found_chain_with_p);
}
TEST(JobPlanner, SinkConsumingSideOutputResolvesChannelFromDecl) {
    // identity_string declares a typed side output {tag="errors",
    // channel_type=int64}. file_int64_sink consumes "p::errors".
    //
    // Pre-fix: by_id.find("p::errors") missed (the raw key isn't in the
    // map), in_ct stayed at op.out_channel = int64 - same as the side-
    // output channel, so the lookup *happened to succeed*. The bug only
    // visibly fires when the sink's `out_channel` differs from the side-
    // output's channel; setting it to "string" here would have made the
    // pre-fix lookup fail. Post-fix we use the side-output decl regardless,
    // so the lookup is correct for any sink wiring.
    JobGraphSpec g;
    g.ops.push_back(OperatorSpec{
        .type = "string_lines_source",
        .id = "src",
        .out_channel = std::string{kChannelString},
        .params = {{"lines", "a,b,c"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "identity_string",
        .id = "p",
        .inputs = {"src"},
        .out_channel = std::string{kChannelString},
        .side_outputs = {{.tag = "errors", .channel_type = std::string{kChannelInt64}}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "file_line_sink",
        .id = "main_snk",
        .inputs = {"p"},
        .out_channel = std::string{kChannelString},
        .params = {{"path", "/tmp/m"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "file_int64_sink",
        .id = "err_snk",
        .inputs = {"p::errors"},
        .out_channel = std::string{kChannelInt64},
        .params = {{"path", "/tmp/e"}},
    });
    EXPECT_NO_THROW((void)plan_job(g, OperatorRegistry::default_instance()));
}

TEST(JobPlanner, OperatorConsumingTypedSideOutputResolvesChannelFromDecl) {
    // identity_int64 emits side-output("errors", string). The downstream
    // string_to_int64 op consumes that side output. The op's in channel
    // is string (from the side-output decl), out is int64. Pre-fix the
    // planner couldn't see the side-output channel and so looked up
    // find_operator("string_to_int64", int64, int64) instead of
    // ("string_to_int64", string, int64) - the bug-exposing case.
    JobGraphSpec g;
    g.ops.push_back(OperatorSpec{
        .type = "int64_range_source",
        .id = "src",
        .out_channel = std::string{kChannelInt64},
        .params = {{"count", "3"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "identity_int64",
        .id = "p",
        .inputs = {"src"},
        .out_channel = std::string{kChannelInt64},
        .side_outputs = {{.tag = "errors", .channel_type = std::string{kChannelString}}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "string_to_int64",
        .id = "parse_err",
        .inputs = {"p::errors"},
        .out_channel = std::string{kChannelInt64},
    });
    g.ops.push_back(OperatorSpec{
        .type = "file_int64_sink",
        .id = "main_snk",
        .inputs = {"p"},
        .out_channel = std::string{kChannelInt64},
        .params = {{"path", "/tmp/m"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "file_int64_sink",
        .id = "err_snk",
        .inputs = {"parse_err"},
        .out_channel = std::string{kChannelInt64},
        .params = {{"path", "/tmp/e"}},
    });
    EXPECT_NO_THROW((void)plan_job(g, OperatorRegistry::default_instance()));
}

TEST(JobPlanner, CoOperatorConsumingTypedSideOutputResolvesBothInChannels) {
    // The dispatcher pattern in compact form. Source A emits a typed
    // side output of channel "string". Source B is independently a
    // "string" source. A registered co-operator (string, string) ->
    // int64 connects them. Pre-fix `resolve_co_op_in_channels` returned
    // (int64, string) from the two upstreams' main out_channels -
    // missing the registered co-op (which is (string, string)) - and so
    // `is_co_op` was false. The op fell through to the single-input
    // path which then threw "no operator factory registered for type
    // string_string_int_co_op with in=int64 out=int64".
    //
    // Post-fix `resolve_co_op_in_channels` honours the side-output
    // suffix and returns (string, string), matching the registered
    // co-op.
    auto child_rr = std::make_unique<RunnerRegistry>(&RunnerRegistry::default_instance());
    clink::cluster::TypeRegistry tr;
    clink::cluster::SelectorRegistry sr;
    clink::plugin::PluginRegistry reg(tr, *child_rr, sr);
    reg.register_type<std::string>("string", clink::string_codec());
    reg.register_type<std::int64_t>("int64", clink::int64_codec());
    reg.register_co_operator<std::string, std::string, std::int64_t>(
        "string_string_int_co_op",
        [](const clink::plugin::BuildContext&) { return std::make_shared<StringStringIntCoOp>(); });

    JobGraphSpec g;
    g.ops.push_back(OperatorSpec{
        .type = "int64_range_source",
        .id = "src_a",
        .out_channel = std::string{kChannelInt64},
        .params = {{"count", "3"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "identity_int64",
        .id = "p_a",
        .inputs = {"src_a"},
        .out_channel = std::string{kChannelInt64},
        .side_outputs = {{.tag = "as_str", .channel_type = std::string{kChannelString}}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "string_lines_source",
        .id = "src_b",
        .out_channel = std::string{kChannelString},
        .params = {{"lines", "a,b,c"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "string_string_int_co_op",
        .id = "co",
        .inputs = {"p_a::as_str", "src_b"},
        .out_channel = std::string{kChannelInt64},
    });
    g.ops.push_back(OperatorSpec{
        .type = "file_int64_sink",
        .id = "out_snk",
        .inputs = {"co"},
        .out_channel = std::string{kChannelInt64},
        .params = {{"path", "/tmp/co"}},
    });
    // The graph also needs a sink off `p_a`'s main int64 output so the
    // planner doesn't reject p_a for having a "stuck" main output that
    // no downstream consumes. (The side-output suffix counts as a
    // consumer of p_a, but only of the side channel; the main output
    // would be dangling without this.)
    g.ops.push_back(OperatorSpec{
        .type = "file_int64_sink",
        .id = "main_snk",
        .inputs = {"p_a"},
        .out_channel = std::string{kChannelInt64},
        .params = {{"path", "/tmp/main"}},
    });
    EXPECT_NO_THROW((void)plan_job(g, OperatorRegistry::default_instance(), *child_rr));
}

TEST(JobGraphSpecJson, ValidateRejectsDuplicateIds) {
    JobGraphSpec g;
    g.ops.push_back(OperatorSpec{
        .type = "a",
        .id = "same",
        .out_channel = std::string{clink::cluster::kChannelInt64},
    });
    g.ops.push_back(OperatorSpec{
        .type = "b",
        .id = "same",
        .inputs = {"same"},
        .out_channel = std::string{clink::cluster::kChannelInt64},
    });
    EXPECT_THROW(g.validate(), std::runtime_error);
}

// --- key-group ranges --------------------------------------------------------
//
// The planner assigns each task the slice of the key space its keyed state
// belongs to. It is the only place that can: the slice depends on the task's
// index WITHIN its operator and on that operator's parallelism, and deploy
// sees neither - every operator shares the generic subtask role, and the
// subtask index it carries is global to the job.
//
// Deploy used to derive the slice from those global numbers, which split the
// key space between DIFFERENT operators. That is F38: a parallelism-1 keyed
// operator in a three-operator job was told it owned 43 of 128 key groups,
// wrote state for every key it saw, and discarded everything outside the
// slice at the next restore. Silently, and only on the restore path.

TEST(JobPlanner, EveryOperatorAtParallelismOneOwnsTheWholeKeySpace) {
    // The exact shape that lost state: several operators, each parallelism 1.
    // Each must own ALL key groups, because each is the only subtask of its
    // operator. A task owning a third of the space is the defect.
    auto g = linear_int64_graph();
    auto plan = plan_job(g, OperatorRegistry::default_instance());
    ASSERT_GE(plan.tasks.size(), 2u);

    for (const auto& t : plan.tasks) {
        EXPECT_EQ(t.key_group_first, 0u)
            << "task " << t.subtask_idx << " of a parallelism-1 operator does not own key group 0";
        EXPECT_EQ(t.key_group_last, static_cast<std::uint32_t>(clink::kNumKeyGroups))
            << "task " << t.subtask_idx
            << " of a parallelism-1 operator owns only part of the key space [" << t.key_group_first
            << ", " << t.key_group_last
            << "). Anything it stores for a key outside that slice is discarded at restore.";
    }
}

TEST(JobPlanner, APartitionedOperatorsSlicesTileTheKeySpaceExactlyOnce) {
    // The other half of the contract: at parallelism > 1 the slices must
    // cover every key group with no gap and no overlap. A gap loses state; an
    // overlap restores the same key into two subtasks.
    JobGraphSpec g;
    g.ops.push_back(OperatorSpec{
        .type = "int64_range_source",
        .id = "src",
        .inputs = {},
        .parallelism = 1,
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"count", "5"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "file_int64_sink",
        .id = "snk",
        .inputs = {"src"},
        .parallelism = 3,
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"path", "/tmp/x"}},
    });
    auto plan = plan_job(g, OperatorRegistry::default_instance());

    // Collect the sink's three slices - the tasks whose range is not the
    // whole space, which is what a parallelism-1 operator gets.
    std::vector<std::pair<std::uint32_t, std::uint32_t>> slices;
    for (const auto& t : plan.tasks) {
        if (t.key_group_last - t.key_group_first !=
            static_cast<std::uint32_t>(clink::kNumKeyGroups)) {
            slices.emplace_back(t.key_group_first, t.key_group_last);
        }
    }
    ASSERT_EQ(slices.size(), 3u) << "expected three partial slices for a parallelism-3 operator";
    std::sort(slices.begin(), slices.end());

    EXPECT_EQ(slices.front().first, 0u) << "the first slice does not start at key group 0";
    EXPECT_EQ(slices.back().second, static_cast<std::uint32_t>(clink::kNumKeyGroups))
        << "the last slice does not reach the end of the key space";
    for (std::size_t i = 1; i < slices.size(); ++i) {
        EXPECT_EQ(slices[i].first, slices[i - 1].second)
            << "slice " << i << " starts at " << slices[i].first << " but the previous ended at "
            << slices[i - 1].second << " - a gap loses state, an overlap duplicates it";
    }
}

// --- Replanning at a new parallelism ---------------------------------
//
// A live rescale changes one operator's parallelism and re-derives the whole
// task set from the graph. Everything that makes the result correct is a pure
// function of the graph, so it can be checked here rather than only by standing
// up a cluster - which matters, because the failure this guards against (F41)
// destroyed the job and was invisible until a multi-process test looked at its
// output.

namespace {

// src (par 1) -> counter (keyed, par varies) -> snk (par 1). The shape a
// rescale is asked for in practice: a stateful keyed operator between a
// single-subtask source and sink.
JobGraphSpec keyed_three_op_graph(std::uint32_t counter_parallelism) {
    JobGraphSpec g;
    g.ops.push_back(OperatorSpec{
        .type = "int64_range_source",
        .id = "src",
        .inputs = {},
        .parallelism = 1,
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"count", "100"}},
    });
    // Designator order must match OperatorSpec's declaration order
    // (parallelism, min_parallelism, max_parallelism, out_channel, params,
    // key_by): g++ rejects any other order outright, where clang accepts it.
    g.ops.push_back(OperatorSpec{
        .type = "identity_int64",
        .id = "counter",
        .inputs = {"src"},
        .parallelism = counter_parallelism,
        .min_parallelism = 1,
        .max_parallelism = 8,
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .key_by = "identity",
    });
    g.ops.push_back(OperatorSpec{
        .type = "file_int64_sink",
        .id = "snk",
        .inputs = {"counter"},
        .parallelism = 1,
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"path", "/tmp/x"}},
    });
    return g;
}

// Every (role, subtask_idx) the plan deploys.
std::set<std::pair<std::string, std::uint32_t>> deployed_keys(const JobPlan& plan) {
    std::set<std::pair<std::string, std::uint32_t>> keys;
    for (const auto& t : plan.tasks) {
        keys.emplace(t.role, t.subtask_idx);
    }
    return keys;
}

}  // namespace

TEST(JobPlannerReplan, EveryPeerReferenceNamesATaskThatExists) {
    // The F41 failure mode, reduced to a property. The role-based rescale
    // resized the deployed task set by cloning a DeploymentTask and left its
    // chain spec naming subtask indices that were no longer deployed; the job
    // died with "missing resolved peer for edge" at every restart attempt. A
    // replanned task set cannot contain that, and this is the assertion that
    // says so - at every parallelism the operator can be scaled to.
    for (const std::uint32_t par : {1u, 2u, 4u, 8u}) {
        auto g = keyed_three_op_graph(par);
        auto plan = plan_job(g, OperatorRegistry::default_instance());
        const auto keys = deployed_keys(plan);
        for (const auto& t : plan.tasks) {
            for (const auto& [peer_role, peer_sub] : t.peer_refs) {
                EXPECT_TRUE(keys.count({peer_role, peer_sub}) != 0)
                    << "counter at parallelism " << par << ": task " << t.subtask_idx
                    << " references peer " << peer_role << "/" << peer_sub
                    << ", which the plan does not deploy";
            }
        }
        // The same check through the chain spec, which is what the worker
        // actually resolves against. peer_refs is derived from it, so a
        // divergence between the two would be invisible above.
        for (const auto& t : plan.tasks) {
            const auto chain = OperatorChainSpec::from_json(t.extra_config);
            for (const auto& group : chain.output_groups) {
                for (const auto& e : group.edges) {
                    EXPECT_TRUE(keys.count({e.peer_role, e.peer_subtask_idx}) != 0)
                        << "counter at parallelism " << par << ": output edge to " << e.peer_role
                        << "/" << e.peer_subtask_idx << " is not deployed";
                }
            }
            for (const auto& e : chain.input_edges) {
                EXPECT_TRUE(keys.count({e.peer_role, e.peer_subtask_idx}) != 0)
                    << "counter at parallelism " << par << ": input edge from " << e.peer_role
                    << "/" << e.peer_subtask_idx << " is not deployed";
            }
        }
    }
}

TEST(JobPlannerReplan, EachOperatorGetsExactlyItsParallelismInContiguousIndices) {
    // Two properties the rescale restore path depends on:
    //
    //  - an operator's subtask_idx_in_op values are exactly 0..parallelism-1,
    //    because that index is what the parent mapping is computed from;
    //  - an operator's GLOBAL indices are one contiguous block, because a
    //    scale-down tells the state backend to read parents
    //    [base, base + count) and the backend walks consecutive directories.
    //
    // If the planner ever allocated indices per-operator interleaved, a
    // scale-down would silently merge another operator's snapshots.
    for (const std::uint32_t par : {1u, 2u, 4u, 8u}) {
        auto g = keyed_three_op_graph(par);
        auto plan = plan_job(g, OperatorRegistry::default_instance());

        std::map<std::string, std::vector<std::uint32_t>> in_op;
        std::map<std::string, std::vector<std::uint32_t>> global;
        for (const auto& t : plan.tasks) {
            ASSERT_FALSE(t.op_id.empty()) << "planner left op_id unset at parallelism " << par;
            in_op[t.op_id].push_back(t.subtask_idx_in_op);
            global[t.op_id].push_back(t.subtask_idx);
        }
        const std::map<std::string, std::uint32_t> expected{
            {"src", 1}, {"counter", par}, {"snk", 1}};
        for (const auto& [op_id, want] : expected) {
            ASSERT_EQ(in_op[op_id].size(), want) << op_id << " at counter parallelism " << par;
            auto idxs = in_op[op_id];
            std::sort(idxs.begin(), idxs.end());
            for (std::uint32_t i = 0; i < want; ++i) {
                EXPECT_EQ(idxs[i], i) << op_id << ": index within operator " << i << " is missing";
            }
            auto g_idxs = global[op_id];
            std::sort(g_idxs.begin(), g_idxs.end());
            for (std::size_t i = 1; i < g_idxs.size(); ++i) {
                EXPECT_EQ(g_idxs[i], g_idxs[i - 1] + 1)
                    << op_id << ": global indices are not contiguous (" << g_idxs[i - 1] << " then "
                    << g_idxs[i] << "); scale-down restore reads a consecutive run of them";
            }
        }
    }
}

TEST(JobPlannerReplan, TheKeyedOperatorsSlicesTileTheKeySpaceAtEveryParallelism) {
    // Gaps lose state, overlaps duplicate it, and either is silent. Asserted
    // per operator rather than over the whole plan, because the bug this
    // replaced (F38) came from mixing operators together.
    for (const std::uint32_t par : {1u, 2u, 4u, 8u}) {
        auto g = keyed_three_op_graph(par);
        auto plan = plan_job(g, OperatorRegistry::default_instance());
        std::vector<std::pair<std::uint32_t, std::uint32_t>> slices;
        for (const auto& t : plan.tasks) {
            if (t.op_id == "counter") {
                slices.emplace_back(t.key_group_first, t.key_group_last);
            }
        }
        ASSERT_EQ(slices.size(), par);
        std::sort(slices.begin(), slices.end());
        EXPECT_EQ(slices.front().first, 0u) << "parallelism " << par << ": first slice misses 0";
        EXPECT_EQ(slices.back().second, static_cast<std::uint32_t>(clink::kNumKeyGroups))
            << "parallelism " << par << ": last slice does not reach the end";
        for (std::size_t i = 1; i < slices.size(); ++i) {
            EXPECT_EQ(slices[i].first, slices[i - 1].second)
                << "parallelism " << par << ": slice " << i << " leaves a gap or overlap";
        }
    }
}

TEST(JobPlannerReplan, RescalingOneOperatorMovesTheOthersGlobalIndices) {
    // The reason a replanned rescale has to give EVERY task an explicit restore
    // directive, not just the rescaled operator's.
    //
    // Snapshots live at <checkpoint_dir>/<global subtask_idx>/, and the planner
    // allocates one contiguous block of global indices per operator in graph
    // order. Grow the middle operator and the sink's block moves. A sink left on
    // "restore from my own index" would read a directory belonging to the
    // counter. This test pins the fact the design rests on, so that if the
    // planner ever stops moving indices the reason for that machinery is still
    // recorded.
    auto before = plan_job(keyed_three_op_graph(1), OperatorRegistry::default_instance());
    auto after = plan_job(keyed_three_op_graph(4), OperatorRegistry::default_instance());

    const auto global_of = [](const JobPlan& plan, const std::string& op_id) {
        std::vector<std::uint32_t> out;
        for (const auto& t : plan.tasks) {
            if (t.op_id == op_id) {
                out.push_back(t.subtask_idx);
            }
        }
        std::sort(out.begin(), out.end());
        return out;
    };

    const auto snk_before = global_of(before, "snk");
    const auto snk_after = global_of(after, "snk");
    ASSERT_EQ(snk_before.size(), 1u);
    ASSERT_EQ(snk_after.size(), 1u);
    EXPECT_NE(snk_before.front(), snk_after.front())
        << "growing the counter did not move the sink's global index; if that is now the "
           "planner's behaviour, the per-task restore translation in restart_job_locked_ "
           "is no longer load-bearing and this test should be revisited rather than deleted";

    // And the counter's own block: 1 subtask before, 4 after, still contiguous.
    EXPECT_EQ(global_of(before, "counter").size(), 1u);
    EXPECT_EQ(global_of(after, "counter").size(), 4u);
}

TEST(JobPlannerReplan, TranslatingAParentThroughTheOldBlockLandsInTheRightOperator) {
    // The whole restore-addressing calculation, end to end, without a cluster:
    // for each task of the new plan, map its index within its operator onto a
    // parent index, translate that through the operator's OLD block base, and
    // check the result is a global index the OLD plan actually deployed FOR THE
    // SAME OPERATOR. Reading another operator's snapshot directory is the
    // failure this guards, and it is silent.
    struct Change {
        std::uint32_t from;
        std::uint32_t to;
    };
    for (const auto ch : {Change{1, 2},
                          Change{1, 4},
                          Change{2, 4},
                          Change{4, 2},
                          Change{4, 1},
                          Change{2, 8},
                          Change{8, 2}}) {
        auto before = plan_job(keyed_three_op_graph(ch.from), OperatorRegistry::default_instance());
        auto after = plan_job(keyed_three_op_graph(ch.to), OperatorRegistry::default_instance());

        // Old block base + parallelism per operator, and which operator owns
        // each old global index.
        std::map<std::string, std::pair<std::uint32_t, std::uint32_t>> old_block;  // base, count
        std::map<std::uint32_t, std::string> owner_of_old_global;
        for (const auto& t : before.tasks) {
            const std::uint32_t base = t.subtask_idx - t.subtask_idx_in_op;
            auto& blk = old_block[t.op_id];
            blk.first = blk.second == 0 ? base : std::min(blk.first, base);
            ++blk.second;
            owner_of_old_global[t.subtask_idx] = t.op_id;
        }

        for (const auto& t : after.tasks) {
            const auto blk = old_block.at(t.op_id);
            std::uint32_t new_p = 0;
            for (const auto& u : after.tasks) {
                if (u.op_id == t.op_id) {
                    ++new_p;
                }
            }
            const auto mapping = rescale_parent_mapping(blk.second, new_p, t.subtask_idx_in_op);
            ASSERT_TRUE(mapping.ok)
                << ch.from << "->" << ch.to << " op " << t.op_id << ": " << mapping.error;
            for (std::uint32_t i = 0; i < mapping.parent_count; ++i) {
                const std::uint32_t old_global = blk.first + mapping.parent_idx + i;
                const auto owner = owner_of_old_global.find(old_global);
                ASSERT_NE(owner, owner_of_old_global.end())
                    << ch.from << "->" << ch.to << ": " << t.op_id << " subtask "
                    << t.subtask_idx_in_op << " would restore from global index " << old_global
                    << ", which the previous deploy did not use";
                EXPECT_EQ(owner->second, t.op_id)
                    << ch.from << "->" << ch.to << ": " << t.op_id << " subtask "
                    << t.subtask_idx_in_op << " would restore from global index " << old_global
                    << ", which belonged to operator '" << owner->second << "'";
            }
        }
    }
}

// --- drain_registration_keys ------------------------------------------
//
// The keys a worker indexes a task's drain callbacks under. BeginRescale
// addresses an OPERATOR; the worker must find the task from any operator id
// the chain hosts, or the dispatch matches nothing (F40, item 27). The
// chained case matters most: the planner packs adjacent ops into one task,
// and a request naming the tail op must still reach it.

TEST(DrainRegistrationKeys, AChainedTaskIsAddressableByEveryOpItHosts) {
    // Same graph shape as AdjacentOperatorsAreChainedIntoOneSubtask: the
    // two mid-chain ops fold into one task. Planned rather than
    // hand-built, so the keys are derived from what a worker actually
    // receives in extra_config.
    JobGraphSpec g;
    g.ops.push_back(OperatorSpec{
        .type = "int64_range_source",
        .id = "src",
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"count", "4"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "multiply_int64",
        .id = "mulA",
        .inputs = {"src"},
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"factor", "2"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "multiply_int64",
        .id = "mulB",
        .inputs = {"mulA"},
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"factor", "5"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "file_int64_sink",
        .id = "snk",
        .inputs = {"mulB"},
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"path", "/tmp/x"}},
    });
    auto plan = plan_job(g, OperatorRegistry::default_instance());

    bool saw_chain = false;
    for (const auto& t : plan.tasks) {
        const auto chain = OperatorChainSpec::from_json(t.extra_config);
        const auto keys = drain_registration_keys(chain, kGenericSubtaskRole);
        if (chain.ops.size() == 2) {
            saw_chain = true;
            ASSERT_EQ(keys.size(), 2u);
            EXPECT_EQ(keys[0], "mulA");
            EXPECT_EQ(keys[1], "mulB");
        } else {
            ASSERT_EQ(chain.ops.size(), 1u);
            ASSERT_EQ(keys.size(), 1u);
            EXPECT_EQ(keys[0], chain.ops[0].id);
        }
        // Whatever the shape, the generic role must never be a key: a
        // BeginRescale can only name real operators, and a role-keyed
        // entry would be unreachable dead weight.
        for (const auto& k : keys) {
            EXPECT_NE(k, kGenericSubtaskRole);
        }
    }
    EXPECT_TRUE(saw_chain) << "the planner stopped chaining mulA+mulB; the chained case above "
                              "is no longer exercised";
}

TEST(DrainRegistrationKeys, FusedEndpointsAreAddressableAndBlanksFallBackToTheRole) {
    // Fused endpoints ride the chain rather than owning a task, but they
    // are still operators a rescale request can name.
    OperatorChainSpec fused;
    fused.ops.push_back(ChainOp{.id = "map", .type = "multiply_int64"});
    fused.fused_source = ChainOp{.id = "gen", .type = "int64_range_source"};
    fused.fused_sink = ChainOp{.id = "out", .type = "file_int64_sink"};
    const auto keys = drain_registration_keys(fused, kGenericSubtaskRole);
    ASSERT_EQ(keys.size(), 3u);
    EXPECT_EQ(keys[0], "map");
    EXPECT_EQ(keys[1], "gen");
    EXPECT_EQ(keys[2], "out");

    // Duplicate ids collapse to one key, so one BeginRescale fires the
    // callback set once, not once per mention.
    OperatorChainSpec dup;
    dup.ops.push_back(ChainOp{.id = "same", .type = "a"});
    dup.ops.push_back(ChainOp{.id = "same", .type = "b"});
    const auto dedup = drain_registration_keys(dup, kGenericSubtaskRole);
    ASSERT_EQ(dedup.size(), 1u);
    EXPECT_EQ(dedup[0], "same");

    // A chain that names no ops (custom-role contract): the role is the
    // operator id, so it is the key.
    OperatorChainSpec blank;
    blank.ops.push_back(ChainOp{.id = "", .type = "anon"});
    const auto fallback = drain_registration_keys(blank, "my_role");
    ASSERT_EQ(fallback.size(), 1u);
    EXPECT_EQ(fallback[0], "my_role");
}

// --- rescale annotation on output groups -------------------------------------
//
// A group feeding an operator that declares rescale bounds carries that op's
// id and max_parallelism; the worker's attach path keys the hold-and-swap
// machinery off exactly these two fields, so a plan that loses them builds a
// job no rescale request can ever cut over.

TEST(JobPlanner, OutputGroupsCarryTheDownstreamRescaleAnnotation) {
    JobGraphSpec g;
    g.ops.push_back(OperatorSpec{
        .type = "int64_range_source",
        .id = "src",
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"count", "8"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "identity_int64",
        .id = "agg",
        .inputs = {"src"},
        .parallelism = 2,
        .min_parallelism = 1,
        .max_parallelism = 8,
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .key_by = "identity",
    });
    g.ops.push_back(OperatorSpec{
        .type = "file_int64_sink",
        .id = "snk",
        .inputs = {"agg"},
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"path", "/tmp/x"}},
    });
    auto plan = plan_job(g, OperatorRegistry::default_instance());

    bool saw_feed_into_agg = false;
    bool saw_feed_into_snk = false;
    for (const auto& t : plan.tasks) {
        const auto chain = OperatorChainSpec::from_json(t.extra_config);
        for (const auto& grp : chain.output_groups) {
            if (grp.downstream_op_id == "agg") {
                saw_feed_into_agg = true;
                EXPECT_EQ(grp.downstream_max_parallelism, 8u)
                    << "the group feeding the bounded op lost its ceiling - the attach path "
                       "would build it with no cutover machinery";
                EXPECT_EQ(grp.mode, RoutingMode::Hash);
            }
            if (grp.downstream_op_id == "snk") {
                saw_feed_into_snk = true;
                EXPECT_EQ(grp.downstream_max_parallelism, 0u)
                    << "an op with no declared bounds must not be marked eligible";
            }
        }
        // And the annotation survives the wire format the worker parses.
        const auto reparsed = OperatorChainSpec::from_json(chain.to_json());
        ASSERT_EQ(reparsed.output_groups.size(), chain.output_groups.size());
        for (std::size_t i = 0; i < chain.output_groups.size(); ++i) {
            EXPECT_EQ(reparsed.output_groups[i].downstream_op_id,
                      chain.output_groups[i].downstream_op_id);
            EXPECT_EQ(reparsed.output_groups[i].downstream_max_parallelism,
                      chain.output_groups[i].downstream_max_parallelism);
        }
    }
    EXPECT_TRUE(saw_feed_into_agg) << "no group feeds the keyed op; the topology under test "
                                      "is not the one this test believes it plans";
    EXPECT_TRUE(saw_feed_into_snk);
}

TEST(JobPlanner, FanInputEdgesCarryTheUpstreamRescaleAnnotation) {
    // Same graph as above, read from the INPUT side: the tasks fed by a
    // fan (hash into the keyed op; rebalance into the sink) must know
    // which op produces their edges and whether it can rescale, because
    // that is what keys the downstream rebind machinery.
    JobGraphSpec g;
    g.ops.push_back(OperatorSpec{
        .type = "int64_range_source",
        .id = "src",
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"count", "8"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "identity_int64",
        .id = "agg",
        .inputs = {"src"},
        .parallelism = 2,
        .min_parallelism = 1,
        .max_parallelism = 8,
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .key_by = "identity",
    });
    g.ops.push_back(OperatorSpec{
        .type = "file_int64_sink",
        .id = "snk",
        .inputs = {"agg"},
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"path", "/tmp/x"}},
    });
    auto plan = plan_job(g, OperatorRegistry::default_instance());

    bool saw_agg_input = false;
    bool saw_snk_input = false;
    for (const auto& t : plan.tasks) {
        const auto chain = OperatorChainSpec::from_json(t.extra_config);
        if (chain.ops.empty()) {
            continue;
        }
        if (chain.ops[0].id == "agg") {
            for (const auto& e : chain.input_edges) {
                saw_agg_input = true;
                EXPECT_EQ(e.upstream_op_id, "src");
                EXPECT_EQ(e.upstream_max_parallelism, 0u)
                    << "the source declares no bounds; marking its edges eligible would build "
                       "rebind machinery for an op the request path will refuse";
            }
        }
        if (chain.ops[0].id == "snk") {
            for (const auto& e : chain.input_edges) {
                saw_snk_input = true;
                EXPECT_EQ(e.upstream_op_id, "agg")
                    << "the sink's fan edges lost their producer identity - CutoverRebind(agg) "
                       "could never find this task";
                EXPECT_EQ(e.upstream_max_parallelism, 8u);
            }
        }
    }
    EXPECT_TRUE(saw_agg_input);
    EXPECT_TRUE(saw_snk_input);
}

// --- plan_hot_cutover ---------------------------------------------------------
//
// The post-cutover subtasks of ONE operator, planned by the real planner at
// the new parallelism and then rewritten: their own indices appended past
// the deployed allocation, every edge translated back to the DEPLOYED
// peers (the fresh plan shifts every op after the rescaled one), restores
// mapped onto the parents' deployed globals. Getting any of these wrong is
// silent: wrong edges wire the new subtasks to other operators' listeners,
// wrong restores load other operators' state.

namespace {

clink::cluster::JobGraphSpec hot_cutover_graph() {
    JobGraphSpec g;
    g.ops.push_back(OperatorSpec{
        .type = "int64_range_source",
        .id = "src",
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"count", "8"}},
    });
    g.ops.push_back(OperatorSpec{
        .type = "identity_int64",
        .id = "agg",
        .inputs = {"src"},
        .parallelism = 2,
        .min_parallelism = 1,
        .max_parallelism = 8,
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .key_by = "identity",
    });
    g.ops.push_back(OperatorSpec{
        .type = "file_int64_sink",
        .id = "snk",
        .inputs = {"agg"},
        .out_channel = std::string{clink::cluster::kChannelInt64},
        .params = {{"path", "/tmp/x"}},
    });
    return g;
}

// The deployed layout the graph above plans to: src=0, agg=1..2, snk=3.
clink::cluster::TaskOpIdentityMap hot_cutover_deployed_identity() {
    clink::cluster::TaskOpIdentityMap m;
    m["__clink_subtask:0"] = {.op_id = "src", .subtask_idx_in_op = 0};
    m["__clink_subtask:1"] = {.op_id = "agg", .subtask_idx_in_op = 0};
    m["__clink_subtask:2"] = {.op_id = "agg", .subtask_idx_in_op = 1};
    m["__clink_subtask:3"] = {.op_id = "snk", .subtask_idx_in_op = 0};
    return m;
}

}  // namespace

TEST(PlanHotCutover, AppendsIndicesRemapsEdgesAndMapsRestoresToDeployedParents) {
    const auto g = hot_cutover_graph();
    const auto deployed = hot_cutover_deployed_identity();

    const auto plan = clink::cluster::plan_hot_cutover(
        g, "agg", 4, deployed, OperatorRegistry::default_instance());
    ASSERT_TRUE(plan.ok) << plan.error;
    ASSERT_EQ(plan.tasks.size(), 4u);
    EXPECT_EQ(plan.teardown_keys,
              (std::vector<std::string>{"__clink_subtask:1", "__clink_subtask:2"}));

    for (std::uint32_t i = 0; i < 4; ++i) {
        const auto& t = plan.tasks[i];
        // Append-only: past the deployed max (3), in op order.
        EXPECT_EQ(t.subtask_idx, 4u + i);
        EXPECT_EQ(t.op_id, "agg");
        EXPECT_EQ(t.subtask_idx_in_op, i);
        // Restore: parents are agg's DEPLOYED globals 1 and 2, split 2->4.
        EXPECT_EQ(t.restore_from_subtask_idx, 1u + i / 2)
            << "subtask " << i << " restores from the wrong deployed parent";
        EXPECT_EQ(t.restore_from_parent_count, 1u);

        const auto chain = OperatorChainSpec::from_json(t.extra_config);
        EXPECT_EQ(chain.subtask_idx, t.subtask_idx)
            << "the chain spec still carries the fresh plan's index - built-in sources "
               "would partition their work by the wrong subtask";
        EXPECT_EQ(chain.subtask_idx_in_op, i);
        // Input edges: the feeding src kept its deployed global 0. The
        // fresh plan cannot be trusted here - it re-numbers everything.
        for (const auto& e : chain.input_edges) {
            EXPECT_EQ(e.peer_subtask_idx, 0u)
                << "an input edge points at a fresh-plan index instead of the deployed src";
        }
        // Output groups: the fed snk kept its deployed global 3 - in the
        // fresh plan at parallelism 4 it sits at index 5, so an
        // untranslated edge would target a listener that does not exist.
        for (const auto& grp : chain.output_groups) {
            for (const auto& e : grp.edges) {
                EXPECT_EQ(e.peer_subtask_idx, 3u)
                    << "an output edge points at the fresh plan's shifted sink index";
            }
        }
        for (const auto& [pr_role, pr_sub] : t.peer_refs) {
            EXPECT_EQ(pr_sub, 3u) << "a peer ref was not translated to the deployed sink";
        }
    }
}

TEST(PlanHotCutover, ScaleDownMergesDeployedParents) {
    const auto plan = clink::cluster::plan_hot_cutover(hot_cutover_graph(),
                                                       "agg",
                                                       1,
                                                       hot_cutover_deployed_identity(),
                                                       OperatorRegistry::default_instance());
    ASSERT_TRUE(plan.ok) << plan.error;
    ASSERT_EQ(plan.tasks.size(), 1u);
    EXPECT_EQ(plan.tasks[0].subtask_idx, 4u);
    EXPECT_EQ(plan.tasks[0].restore_from_subtask_idx, 1u);
    EXPECT_EQ(plan.tasks[0].restore_from_parent_count, 2u)
        << "the single new subtask must merge BOTH deployed parents or half the key space "
           "restores from nowhere";
}

TEST(PlanHotCutover, RefusesWhatItCannotTranslate) {
    const auto g = hot_cutover_graph();

    // Unknown operator.
    auto plan = clink::cluster::plan_hot_cutover(
        g, "nope", 4, hot_cutover_deployed_identity(), OperatorRegistry::default_instance());
    EXPECT_FALSE(plan.ok);

    // An inconsistent deployed block for the rescaled op: refuse rather
    // than guess a base.
    auto identity = hot_cutover_deployed_identity();
    identity["__clink_subtask:9"] = {.op_id = "agg", .subtask_idx_in_op = 0};
    plan = clink::cluster::plan_hot_cutover(
        g, "agg", 4, identity, OperatorRegistry::default_instance());
    EXPECT_FALSE(plan.ok);
    EXPECT_NE(plan.error.find("inconsistent"), std::string::npos) << plan.error;

    // A neighbour whose deployed parallelism disagrees with the graph
    // (snk missing from the deployed set): its edges cannot be translated.
    auto missing_snk = hot_cutover_deployed_identity();
    missing_snk.erase("__clink_subtask:3");
    plan = clink::cluster::plan_hot_cutover(
        g, "agg", 4, missing_snk, OperatorRegistry::default_instance());
    EXPECT_FALSE(plan.ok) << "edges to an operator with no deployed identity were guessed";
}
