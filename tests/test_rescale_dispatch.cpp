// Phase 29f: unit tests for plan_operator_cutover. Drives the pure
// planning helper that the JM uses when the RescaleCoordinator
// transitions an operator from Draining to CuttingOver.

#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/protocol.hpp"
#include "clink/cluster/rescale_dispatch.hpp"
#include "clink/runtime/key_groups.hpp"

namespace clink::cluster {
namespace {

DeploymentTask make_template(const std::string& role, const std::string& extra_config = "key=val") {
    DeploymentTask t;
    t.role = role;
    t.subtask_idx = 0;  // overwritten by the planner
    t.data_port = 0;
    t.extra_config = extra_config;
    return t;
}

TEST(PlanOperatorCutover, ScaleUpDoublesParallelismAndSplitsKeyGroups) {
    auto plan = plan_operator_cutover("agg",
                                      /*current_parallelism=*/2,
                                      /*target_parallelism=*/4,
                                      /*cutover_checkpoint=*/42,
                                      /*restore_from_dir=*/"/tmp/ckpt",
                                      make_template("agg"),
                                      /*old_subtask_keys=*/{"agg:0", "agg:1"},
                                      /*tm_free_slots=*/{{"tm-a", 4}, {"tm-b", 4}});

    ASSERT_TRUE(plan.ok) << plan.error;
    ASSERT_EQ(plan.new_tasks.size(), 4u);
    EXPECT_EQ(plan.teardown_keys.size(), 2u);

    // Subtask 0 and 1 share parent 0; subtask 2 and 3 share parent 1.
    EXPECT_EQ(plan.new_tasks[0].second.restore_from_subtask_idx, 0u);
    EXPECT_EQ(plan.new_tasks[1].second.restore_from_subtask_idx, 0u);
    EXPECT_EQ(plan.new_tasks[2].second.restore_from_subtask_idx, 1u);
    EXPECT_EQ(plan.new_tasks[3].second.restore_from_subtask_idx, 1u);
    for (const auto& [_, t] : plan.new_tasks) {
        EXPECT_EQ(t.restore_from_parent_count, 1u);
    }

    // Key-group ranges should match the standard per-subtask formula
    // for parallelism=4.
    for (std::uint32_t i = 0; i < 4; ++i) {
        const auto range = key_group_range_for_subtask(i, 4);
        EXPECT_EQ(plan.new_tasks[i].second.key_group_first, range.first);
        EXPECT_EQ(plan.new_tasks[i].second.key_group_last, range.second);
        EXPECT_EQ(plan.new_tasks[i].second.subtask_idx, i);
        EXPECT_EQ(plan.new_tasks[i].second.role, "agg");
        EXPECT_EQ(plan.new_tasks[i].second.extra_config, "key=val");
        EXPECT_EQ(plan.new_tasks[i].second.data_port, 0);
    }
}

TEST(PlanOperatorCutover, ScaleDownMergesParentSlices) {
    auto plan = plan_operator_cutover("agg",
                                      /*current_parallelism=*/4,
                                      /*target_parallelism=*/2,
                                      /*cutover_checkpoint=*/9,
                                      "/tmp/ckpt",
                                      make_template("agg"),
                                      {"agg:0", "agg:1", "agg:2", "agg:3"},
                                      {{"tm-a", 4}});

    ASSERT_TRUE(plan.ok) << plan.error;
    ASSERT_EQ(plan.new_tasks.size(), 2u);
    EXPECT_EQ(plan.teardown_keys.size(), 4u);

    // Each new subtask owns 2 parent slices (k_down = 4/2 = 2).
    EXPECT_EQ(plan.new_tasks[0].second.restore_from_subtask_idx, 0u);
    EXPECT_EQ(plan.new_tasks[0].second.restore_from_parent_count, 2u);
    EXPECT_EQ(plan.new_tasks[1].second.restore_from_subtask_idx, 2u);
    EXPECT_EQ(plan.new_tasks[1].second.restore_from_parent_count, 2u);

    for (std::uint32_t i = 0; i < 2; ++i) {
        const auto range = key_group_range_for_subtask(i, 2);
        EXPECT_EQ(plan.new_tasks[i].second.key_group_first, range.first);
        EXPECT_EQ(plan.new_tasks[i].second.key_group_last, range.second);
    }
}

TEST(PlanOperatorCutover, RoundRobinPlacementAcrossTMs) {
    auto plan = plan_operator_cutover("agg",
                                      /*current=*/1,
                                      /*target=*/4,
                                      /*ckpt=*/1,
                                      "/tmp/ckpt",
                                      make_template("agg"),
                                      {"agg:0"},
                                      {{"tm-a", 2}, {"tm-b", 2}});

    ASSERT_TRUE(plan.ok) << plan.error;
    ASSERT_EQ(plan.new_tasks.size(), 4u);
    EXPECT_EQ(plan.new_tasks[0].first, "tm-a");
    EXPECT_EQ(plan.new_tasks[1].first, "tm-b");
    EXPECT_EQ(plan.new_tasks[2].first, "tm-a");
    EXPECT_EQ(plan.new_tasks[3].first, "tm-b");
}

TEST(PlanOperatorCutover, RejectsInsufficientCapacity) {
    auto plan = plan_operator_cutover("agg",
                                      /*current=*/2,
                                      /*target=*/4,
                                      /*ckpt=*/1,
                                      "/tmp/ckpt",
                                      make_template("agg"),
                                      {"agg:0", "agg:1"},
                                      {{"tm-a", 1}, {"tm-b", 2}});

    EXPECT_FALSE(plan.ok);
    EXPECT_TRUE(plan.new_tasks.empty());
    EXPECT_NE(plan.error.find("insufficient free TM slots"), std::string::npos);
}

TEST(PlanOperatorCutover, RejectsNonIntegerScaleFactor) {
    auto plan_up = plan_operator_cutover("agg",
                                         /*current=*/2,
                                         /*target=*/3,
                                         1,
                                         "/tmp",
                                         make_template("agg"),
                                         {"agg:0", "agg:1"},
                                         {{"tm-a", 4}});
    EXPECT_FALSE(plan_up.ok);
    EXPECT_NE(plan_up.error.find("integer multiple"), std::string::npos);

    auto plan_down = plan_operator_cutover("agg",
                                           /*current=*/6,
                                           /*target=*/4,
                                           1,
                                           "/tmp",
                                           make_template("agg"),
                                           {"agg:0", "agg:1", "agg:2", "agg:3", "agg:4", "agg:5"},
                                           {{"tm-a", 4}});
    EXPECT_FALSE(plan_down.ok);
    EXPECT_NE(plan_down.error.find("integer multiple"), std::string::npos);
}

TEST(PlanOperatorCutover, RejectsZeroParallelism) {
    auto plan =
        plan_operator_cutover("agg", 0, 4, 1, "/tmp", make_template("agg"), {}, {{"tm-a", 4}});
    EXPECT_FALSE(plan.ok);
    EXPECT_NE(plan.error.find("non-zero"), std::string::npos);
}

TEST(PlanOperatorCutover, RejectsNoOpRescale) {
    auto plan = plan_operator_cutover("agg",
                                      4,
                                      4,
                                      1,
                                      "/tmp",
                                      make_template("agg"),
                                      {"agg:0", "agg:1", "agg:2", "agg:3"},
                                      {{"tm-a", 8}});
    EXPECT_FALSE(plan.ok);
    EXPECT_NE(plan.error.find("no-op"), std::string::npos);
}

TEST(PlanOperatorCutover, ClonesTemplatePeersIntoEveryNewSubtask) {
    DeploymentTask templ = make_template("agg");
    templ.peers.push_back(
        PeerAddress{.role = "snk", .subtask_idx = 0, .host = "h", .data_port = 0});
    templ.peers.push_back(
        PeerAddress{.role = "snk", .subtask_idx = 1, .host = "h", .data_port = 0});

    auto plan = plan_operator_cutover("agg", 1, 2, 1, "/tmp", templ, {"agg:0"}, {{"tm-a", 4}});
    ASSERT_TRUE(plan.ok) << plan.error;
    ASSERT_EQ(plan.new_tasks.size(), 2u);
    for (const auto& [_, t] : plan.new_tasks) {
        EXPECT_EQ(t.peers.size(), 2u);
        EXPECT_EQ(t.peers[0].role, "snk");
        EXPECT_EQ(t.peers[1].subtask_idx, 1u);
    }
}

}  // namespace
}  // namespace clink::cluster
