// Unit tests for plan_operator_cutover. Drives the pure
// planning helper that the coordinator uses when the RescaleCoordinator
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

// --- rescale_parent_mapping ------------------------------------------
//
// The arithmetic that decides which parent snapshot a rescaled subtask reads.
// Getting it wrong loses or duplicates keyed state silently: a subtask that
// reads the wrong parent restores state for keys it does not own and finds
// nothing for the keys it does. Exhaustive rather than sampled, because the
// only cheap place to be exhaustive about it is here.

TEST(RescaleParentMapping, ScalingUpSharesEachParentAcrossItsChildren) {
    // 1 -> 4: every child reads parent 0 and filters to its key-group slice.
    for (std::uint32_t i = 0; i < 4; ++i) {
        const auto m = rescale_parent_mapping(1, 4, i);
        ASSERT_TRUE(m.ok) << m.error << " (i=" << i << ")";
        EXPECT_EQ(m.parent_idx, 0u) << "i=" << i;
        EXPECT_EQ(m.parent_count, 1u) << "i=" << i;
    }
    // 2 -> 4: children 0,1 read parent 0; children 2,3 read parent 1.
    const std::uint32_t expected_2_to_4[] = {0, 0, 1, 1};
    for (std::uint32_t i = 0; i < 4; ++i) {
        const auto m = rescale_parent_mapping(2, 4, i);
        ASSERT_TRUE(m.ok) << m.error;
        EXPECT_EQ(m.parent_idx, expected_2_to_4[i]) << "i=" << i;
        EXPECT_EQ(m.parent_count, 1u) << "i=" << i;
    }
}

TEST(RescaleParentMapping, ScalingDownMergesAContiguousRunOfParents) {
    // 4 -> 2: child 0 merges parents 0,1; child 1 merges parents 2,3.
    const auto a = rescale_parent_mapping(4, 2, 0);
    ASSERT_TRUE(a.ok) << a.error;
    EXPECT_EQ(a.parent_idx, 0u);
    EXPECT_EQ(a.parent_count, 2u);
    const auto b = rescale_parent_mapping(4, 2, 1);
    ASSERT_TRUE(b.ok) << b.error;
    EXPECT_EQ(b.parent_idx, 2u);
    EXPECT_EQ(b.parent_count, 2u);
    // 4 -> 1: the single child absorbs all four.
    const auto c = rescale_parent_mapping(4, 1, 0);
    ASSERT_TRUE(c.ok) << c.error;
    EXPECT_EQ(c.parent_idx, 0u);
    EXPECT_EQ(c.parent_count, 4u);
}

TEST(RescaleParentMapping, EveryParentIsCoveredExactlyOnceWhenScalingDown) {
    // The property that matters, not just the two endpoints: scaling down must
    // read each old subtask's snapshot exactly once across all new subtasks. A
    // gap loses that parent's state; an overlap restores it twice.
    for (const auto [old_p, new_p] : std::vector<std::pair<std::uint32_t, std::uint32_t>>{
             {4, 2}, {4, 1}, {8, 2}, {8, 4}, {6, 3}, {12, 4}}) {
        std::vector<int> reads(old_p, 0);
        for (std::uint32_t i = 0; i < new_p; ++i) {
            const auto m = rescale_parent_mapping(old_p, new_p, i);
            ASSERT_TRUE(m.ok) << m.error << " (" << old_p << "->" << new_p << " i=" << i << ")";
            for (std::uint32_t k = 0; k < m.parent_count; ++k) {
                ASSERT_LT(m.parent_idx + k, old_p)
                    << old_p << "->" << new_p << ": subtask " << i << " reads parent "
                    << (m.parent_idx + k) << ", which does not exist";
                ++reads[m.parent_idx + k];
            }
        }
        for (std::uint32_t j = 0; j < old_p; ++j) {
            EXPECT_EQ(reads[j], 1) << old_p << "->" << new_p << ": parent " << j << " was read "
                                   << reads[j] << " times, not once";
        }
    }
}

TEST(RescaleParentMapping, EveryChildHasAParentWhenScalingUp) {
    // The mirror property: scaling up, every new subtask must map to a parent
    // that exists, and the children of one parent must be contiguous.
    for (const auto [old_p, new_p] : std::vector<std::pair<std::uint32_t, std::uint32_t>>{
             {1, 2}, {1, 4}, {2, 4}, {2, 8}, {3, 6}, {4, 12}}) {
        std::uint32_t prev = 0;
        for (std::uint32_t i = 0; i < new_p; ++i) {
            const auto m = rescale_parent_mapping(old_p, new_p, i);
            ASSERT_TRUE(m.ok) << m.error;
            EXPECT_LT(m.parent_idx, old_p) << old_p << "->" << new_p << " i=" << i;
            EXPECT_EQ(m.parent_count, 1u);
            EXPECT_GE(m.parent_idx, prev) << "parents must be non-decreasing across children";
            prev = m.parent_idx;
        }
        // The last child must map to the last parent, or some parent's state
        // was never handed to anybody.
        const auto last = rescale_parent_mapping(old_p, new_p, new_p - 1);
        EXPECT_EQ(last.parent_idx, old_p - 1) << old_p << "->" << new_p;
    }
}

TEST(RescaleParentMapping, UnchangedParallelismRestoresFromSelf) {
    // Not an error. A job where one operator is rescaled replans every
    // operator, and the untouched ones must restore from their own snapshots.
    for (std::uint32_t i = 0; i < 3; ++i) {
        const auto m = rescale_parent_mapping(3, 3, i);
        ASSERT_TRUE(m.ok) << m.error;
        EXPECT_EQ(m.parent_idx, i);
        EXPECT_EQ(m.parent_count, 1u);
    }
}

TEST(RescaleParentMapping, RefusesNonIntegerFactorsAndImpossibleInputs) {
    // 2 -> 3 would leave a key group straddling two parents.
    const auto up = rescale_parent_mapping(2, 3, 0);
    EXPECT_FALSE(up.ok);
    EXPECT_NE(up.error.find("integer multiple"), std::string::npos) << up.error;

    const auto down = rescale_parent_mapping(3, 2, 0);
    EXPECT_FALSE(down.ok);
    EXPECT_NE(down.error.find("integer multiple"), std::string::npos) << down.error;

    EXPECT_FALSE(rescale_parent_mapping(0, 4, 0).ok);
    EXPECT_FALSE(rescale_parent_mapping(4, 0, 0).ok);
    // A subtask index at or beyond the new parallelism is a caller bug, and
    // silently returning parent 0 would restore the wrong slice.
    const auto oob = rescale_parent_mapping(2, 4, 4);
    EXPECT_FALSE(oob.ok);
    EXPECT_NE(oob.error.find("outside the new parallelism"), std::string::npos) << oob.error;
}

TEST(PlanOperatorCutover, ScaleUpDoublesParallelismAndSplitsKeyGroups) {
    auto plan = plan_operator_cutover("agg",
                                      /*current_parallelism=*/2,
                                      /*target_parallelism=*/4,
                                      /*cutover_checkpoint=*/42,
                                      /*restore_from_dir=*/"/tmp/ckpt",
                                      make_template("agg"),
                                      /*old_subtask_keys=*/{"agg:0", "agg:1"},
                                      /*worker_free_slots=*/{{"worker-a", 4}, {"worker-b", 4}});

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
                                      {{"worker-a", 4}});

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
                                      {{"worker-a", 2}, {"worker-b", 2}});

    ASSERT_TRUE(plan.ok) << plan.error;
    ASSERT_EQ(plan.new_tasks.size(), 4u);
    EXPECT_EQ(plan.new_tasks[0].first, "worker-a");
    EXPECT_EQ(plan.new_tasks[1].first, "worker-b");
    EXPECT_EQ(plan.new_tasks[2].first, "worker-a");
    EXPECT_EQ(plan.new_tasks[3].first, "worker-b");
}

TEST(PlanOperatorCutover, RejectsInsufficientCapacity) {
    auto plan = plan_operator_cutover("agg",
                                      /*current=*/2,
                                      /*target=*/4,
                                      /*ckpt=*/1,
                                      "/tmp/ckpt",
                                      make_template("agg"),
                                      {"agg:0", "agg:1"},
                                      {{"worker-a", 1}, {"worker-b", 2}});

    EXPECT_FALSE(plan.ok);
    EXPECT_TRUE(plan.new_tasks.empty());
    EXPECT_NE(plan.error.find("insufficient free worker slots"), std::string::npos);
}

TEST(PlanOperatorCutover, RejectsNonIntegerScaleFactor) {
    auto plan_up = plan_operator_cutover("agg",
                                         /*current=*/2,
                                         /*target=*/3,
                                         1,
                                         "/tmp",
                                         make_template("agg"),
                                         {"agg:0", "agg:1"},
                                         {{"worker-a", 4}});
    EXPECT_FALSE(plan_up.ok);
    EXPECT_NE(plan_up.error.find("integer multiple"), std::string::npos);

    auto plan_down = plan_operator_cutover("agg",
                                           /*current=*/6,
                                           /*target=*/4,
                                           1,
                                           "/tmp",
                                           make_template("agg"),
                                           {"agg:0", "agg:1", "agg:2", "agg:3", "agg:4", "agg:5"},
                                           {{"worker-a", 4}});
    EXPECT_FALSE(plan_down.ok);
    EXPECT_NE(plan_down.error.find("integer multiple"), std::string::npos);
}

TEST(PlanOperatorCutover, RejectsZeroParallelism) {
    auto plan =
        plan_operator_cutover("agg", 0, 4, 1, "/tmp", make_template("agg"), {}, {{"worker-a", 4}});
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
                                      {{"worker-a", 8}});
    EXPECT_FALSE(plan.ok);
    EXPECT_NE(plan.error.find("no-op"), std::string::npos);
}

TEST(PlanOperatorCutover, ClonesTemplatePeersIntoEveryNewSubtask) {
    DeploymentTask templ = make_template("agg");
    templ.peers.push_back(
        PeerAddress{.role = "snk", .subtask_idx = 0, .host = "h", .data_port = 0});
    templ.peers.push_back(
        PeerAddress{.role = "snk", .subtask_idx = 1, .host = "h", .data_port = 0});

    auto plan = plan_operator_cutover("agg", 1, 2, 1, "/tmp", templ, {"agg:0"}, {{"worker-a", 4}});
    ASSERT_TRUE(plan.ok) << plan.error;
    ASSERT_EQ(plan.new_tasks.size(), 2u);
    for (const auto& [_, t] : plan.new_tasks) {
        EXPECT_EQ(t.peers.size(), 2u);
        EXPECT_EQ(t.peers[0].role, "snk");
        EXPECT_EQ(t.peers[1].subtask_idx, 1u);
    }
}

// --- Operator identity translation -----------------------------------
//
// The rescale state machine is keyed by operator id; a worker ack names
// (role, job-global subtask index), and under the chain planner every task
// shares the generic role. These helpers are the only bridge between the
// two vocabularies (F40, item 27), so their policy is pinned here: the
// deploy-time identity record is authoritative when present, and the role
// fallback exists solely for custom-role tasks that have no record.

TEST(OpScopedAck, TranslatesAGenericRoleAckThroughTheIdentityRecord) {
    TaskOpIdentityMap identity;
    // A two-operator job: "src" holds global indices 0..1, "agg" holds 2..5.
    identity["__clink_subtask:2"] = TaskOpIdentity{.op_id = "agg", .subtask_idx_in_op = 0};
    identity["__clink_subtask:5"] = TaskOpIdentity{.op_id = "agg", .subtask_idx_in_op = 3};

    const auto a = op_scoped_ack(identity, "__clink_subtask", 2);
    EXPECT_EQ(a.op_id, "agg");
    EXPECT_EQ(a.subtask_idx_in_op, 0u);
    const auto b = op_scoped_ack(identity, "__clink_subtask", 5);
    EXPECT_EQ(b.op_id, "agg");
    // The index the state machine counts is the index WITHIN the operator,
    // not the job-global one - counting global indices against the op's
    // parallelism would never reach the CuttingOver threshold.
    EXPECT_EQ(b.subtask_idx_in_op, 3u);
}

TEST(OpScopedAck, FallsBackToTheRoleForATaskWithNoIdentityRecord) {
    TaskOpIdentityMap identity;
    identity["__clink_subtask:0"] = TaskOpIdentity{.op_id = "src", .subtask_idx_in_op = 0};

    // Custom-role task: its role IS its operator id and its index is
    // op-local already (the pre-planner contract).
    const auto a = op_scoped_ack(identity, "my_sink", 1);
    EXPECT_EQ(a.op_id, "my_sink");
    EXPECT_EQ(a.subtask_idx_in_op, 1u);

    // Defensive: a record with an empty op_id must not translate an ack
    // into the empty operator, which nothing registered.
    identity["__clink_subtask:9"] = TaskOpIdentity{.op_id = "", .subtask_idx_in_op = 4};
    const auto b = op_scoped_ack(identity, "__clink_subtask", 9);
    EXPECT_EQ(b.op_id, "__clink_subtask");
    EXPECT_EQ(b.subtask_idx_in_op, 9u);
}

TEST(TaskHostsOp, TheIdentityRecordIsAuthoritativeWhenPresent) {
    TaskOpIdentityMap identity;
    identity["__clink_subtask:3"] = TaskOpIdentity{.op_id = "agg", .subtask_idx_in_op = 1};

    EXPECT_TRUE(task_hosts_op(identity, "__clink_subtask", 3, "agg"));
    EXPECT_FALSE(task_hosts_op(identity, "__clink_subtask", 3, "src"));
    // No role fallback for a task WITH identity: otherwise a request naming
    // the shared generic role would address every task of every operator.
    EXPECT_FALSE(task_hosts_op(identity, "__clink_subtask", 3, "__clink_subtask"));
}

TEST(TaskHostsOp, FallsBackToTheRoleOnlyForTasksWithNoRecord) {
    const TaskOpIdentityMap empty;
    EXPECT_TRUE(task_hosts_op(empty, "my_sink", 0, "my_sink"));
    EXPECT_FALSE(task_hosts_op(empty, "my_sink", 0, "other"));
}

}  // namespace
}  // namespace clink::cluster

// When a RESTART must translate its restore through the pre-rescale layout.
//
// The window: a rescale redeploys from latest_completed_checkpoint_id, and that id
// still names a PRE-rescale checkpoint until one completes under the new topology.
// A restart inside it is not a replan, so without this guard every task falls back
// to "restore from my own subtask index" - into a directory holding the state of
// whichever operator occupied that index under the OLD layout. Same class as F38
// and F59: an index reused across a topology change.
//
// Exhaustive here because the window itself is short and timing-dependent; a
// multi-process test could only enter it by luck, which is not evidence. The
// translation this guards is rescale_parent_mapping, tested above.
TEST(RestorePreRescaleLayout, TranslatesOnlyWhileTheRestorePointPredatesTheRescale) {
    // No retained layout: nothing to translate, whatever the ids say.
    EXPECT_FALSE(clink::cluster::restore_needs_pre_rescale_layout(false, 10, 10));
    EXPECT_FALSE(clink::cluster::restore_needs_pre_rescale_layout(false, 1, 99));

    // Retained, and the restore point is the rescale's own - the case that was
    // silently corrupting state.
    EXPECT_TRUE(clink::cluster::restore_needs_pre_rescale_layout(true, 20, 20));
    // Retained, and the restore point is older still (an earlier checkpoint was
    // chosen because the newest failed verification).
    EXPECT_TRUE(clink::cluster::restore_needs_pre_rescale_layout(true, 12, 20));

    // A checkpoint has completed under the NEW topology, so each subtask's own
    // directory is now correct and translating would read the wrong one.
    EXPECT_FALSE(clink::cluster::restore_needs_pre_rescale_layout(true, 21, 20));
    EXPECT_FALSE(clink::cluster::restore_needs_pre_rescale_layout(true, 99, 20));

    // Nothing has completed at all: there is no state to restore, so an empty
    // restore is right and a translation would invent a parent.
    EXPECT_FALSE(clink::cluster::restore_needs_pre_rescale_layout(true, 0, 20));
    EXPECT_FALSE(clink::cluster::restore_needs_pre_rescale_layout(true, 0, 0));
}
