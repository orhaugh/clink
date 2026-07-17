// JobPlanner translates a JobGraphSpec (logical) into a JobPlan
// (physical) using the default OperatorRegistry. It owns:
//   - topology validation (cycle detection, dangling refs, fan-out
//     constraints for v1)
//   - parallelism expansion (v1: must be 1 per op)
//   - per-subtask OperatorChainSpec encoding into extra_config
// These tests pin the contracts the coordinator relies on for slot accounting,
// peer-ref bookkeeping, and dispatch.

#include <cstdint>
#include <memory>
#include <set>
#include <string>

#include <gtest/gtest.h>

#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/job_planner.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/runner_registry.hpp"
#include "clink/cluster/type_registry.hpp"
#include "clink/core/codec.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/plugin/plugin.hpp"

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
