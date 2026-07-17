// RescaleCoordinator unit tests.
//
// Drives the per-operator rescale state machine end-to-end without
// any cluster machinery: register an operator with bounds, request
// rescales, walk through Preparing -> Draining -> CuttingOver ->
// Complete via the mark_* methods, verify each transition and the
// rejection paths.

#include <string>

#include <gtest/gtest.h>

#include "clink/cluster/rescale_coordinator.hpp"

using namespace clink::cluster;

namespace {

// RescaleCoordinator holds a std::mutex so it isn't copyable. Set up
// helper that builds-in-place via a callback: caller writes the
// coordinator-using body inside it.
void setup_coord(RescaleCoordinator& c,
                 const std::string& op_id = "join",
                 std::uint32_t current = 2,
                 std::uint32_t min = 1,
                 std::uint32_t max = 8) {
    c.register_operator(op_id, current, min, max);
}

// Walk the state machine to Complete with `current` old subtasks and
// `target` new subtasks. Helper for tests that need to start from a
// fresh post-rescale state.
void run_to_complete(RescaleCoordinator& c,
                     const std::string& op_id,
                     std::uint32_t current,
                     std::uint32_t target) {
    c.mark_checkpoint_ready(op_id, /*ckpt_id=*/42);
    for (std::uint32_t i = 0; i < current; ++i) {
        c.mark_old_drained(op_id, i);
    }
    for (std::uint32_t i = 0; i < target; ++i) {
        c.mark_new_ready(op_id, i);
    }
}

}  // namespace

TEST(RescaleCoordinator, RegisterStartsIdle) {
    RescaleCoordinator c;
    setup_coord(c, "join", 2, 1, 8);
    auto st = c.status("join");
    ASSERT_TRUE(st.has_value());
    EXPECT_EQ(st->state, RescaleState::Idle);
    EXPECT_EQ(st->current_parallelism, 2u);
    EXPECT_EQ(st->target_parallelism, 2u);
    EXPECT_EQ(st->min_parallelism, 1u);
    EXPECT_EQ(st->max_parallelism, 8u);
}

TEST(RescaleCoordinator, RequestRescaleHonoursBounds) {
    RescaleCoordinator c;
    setup_coord(c, "join", 2, 1, 4);

    // Out of bounds: above max.
    auto r = c.request_rescale("join", 5);
    EXPECT_FALSE(r.ok);
    EXPECT_NE(r.reason.find("above max_parallelism"), std::string::npos);

    // Out of bounds: below min.
    r = c.request_rescale("join", 0);
    EXPECT_FALSE(r.ok);
    EXPECT_NE(r.reason.find("below min_parallelism"), std::string::npos);

    // Equal to current: no-op rejection.
    r = c.request_rescale("join", 2);
    EXPECT_FALSE(r.ok);
    EXPECT_NE(r.reason.find("equals current"), std::string::npos);

    // In bounds: accepted; state moves to Preparing.
    r = c.request_rescale("join", 4);
    EXPECT_TRUE(r.ok);
    EXPECT_EQ(r.accepted_target, 4u);
    auto st = c.status("join");
    ASSERT_TRUE(st.has_value());
    EXPECT_EQ(st->state, RescaleState::Preparing);
    EXPECT_EQ(st->target_parallelism, 4u);
}

TEST(RescaleCoordinator, RequestRejectedWhenOperatorNotScalable) {
    RescaleCoordinator c;
    // 0/0 bounds = not scalable by convention.
    c.register_operator("static_op", /*current=*/3, /*min=*/0, /*max=*/0);
    auto r = c.request_rescale("static_op", 5);
    EXPECT_FALSE(r.ok);
    EXPECT_NE(r.reason.find("no autoscale bounds"), std::string::npos);
}

TEST(RescaleCoordinator, RequestRejectedWhenAlreadyInProgress) {
    RescaleCoordinator c;
    setup_coord(c, "join", 2, 1, 8);
    auto r = c.request_rescale("join", 4);
    ASSERT_TRUE(r.ok);
    // Second request while still Preparing must reject.
    auto r2 = c.request_rescale("join", 6);
    EXPECT_FALSE(r2.ok);
    EXPECT_NE(r2.reason.find("already has a rescale in progress"), std::string::npos);
}

TEST(RescaleCoordinator, FullLifecycleAdvancesThroughEveryState) {
    RescaleCoordinator c;
    setup_coord(c, "join", 2, 1, 8);
    auto r = c.request_rescale("join", 4);
    ASSERT_TRUE(r.ok);
    EXPECT_EQ(c.status("join")->state, RescaleState::Preparing);

    EXPECT_TRUE(c.mark_checkpoint_ready("join", /*ckpt=*/77));
    {
        auto st = c.status("join");
        ASSERT_TRUE(st.has_value());
        EXPECT_EQ(st->state, RescaleState::Draining);
        EXPECT_EQ(st->cutover_checkpoint, 77u);
    }

    // Two old subtasks drain in any order.
    EXPECT_TRUE(c.mark_old_drained("join", /*subtask=*/0));
    EXPECT_EQ(c.status("join")->state, RescaleState::Draining);
    EXPECT_EQ(c.status("join")->old_subtasks_drained, 1u);

    EXPECT_TRUE(c.mark_old_drained("join", /*subtask=*/1));
    EXPECT_EQ(c.status("join")->state, RescaleState::CuttingOver);
    EXPECT_EQ(c.status("join")->old_subtasks_drained, 2u);

    // Four new subtasks come online (target = 4).
    for (std::uint32_t i = 0; i < 4; ++i) {
        EXPECT_TRUE(c.mark_new_ready("join", i));
    }
    auto st = c.status("join");
    ASSERT_TRUE(st.has_value());
    EXPECT_EQ(st->state, RescaleState::Complete);
    EXPECT_EQ(st->current_parallelism, 4u);
    EXPECT_EQ(st->new_subtasks_ready, 4u);
}

TEST(RescaleCoordinator, MarkOldDrainedIdempotent) {
    RescaleCoordinator c;
    setup_coord(c, "join", 2, 1, 8);
    c.request_rescale("join", 4);
    c.mark_checkpoint_ready("join", 1);
    c.mark_old_drained("join", 0);
    c.mark_old_drained("join", 0);  // re-ack same subtask
    EXPECT_EQ(c.status("join")->old_subtasks_drained, 1u);
}

TEST(RescaleCoordinator, MarkRejectedFromWrongState) {
    RescaleCoordinator c;
    setup_coord(c, "join", 2, 1, 8);
    // Idle: every mark_* should reject.
    EXPECT_FALSE(c.mark_checkpoint_ready("join", 1));
    EXPECT_FALSE(c.mark_old_drained("join", 0));
    EXPECT_FALSE(c.mark_new_ready("join", 0));

    // Preparing: only mark_checkpoint_ready is valid.
    c.request_rescale("join", 4);
    EXPECT_FALSE(c.mark_old_drained("join", 0));
    EXPECT_FALSE(c.mark_new_ready("join", 0));
    EXPECT_TRUE(c.mark_checkpoint_ready("join", 1));

    // Draining: only mark_old_drained is valid.
    EXPECT_FALSE(c.mark_checkpoint_ready("join", 2));
    EXPECT_FALSE(c.mark_new_ready("join", 0));
}

TEST(RescaleCoordinator, AbortFromDrainingResetsToAborted) {
    RescaleCoordinator c;
    setup_coord(c, "join", 2, 1, 8);
    c.request_rescale("join", 4);
    c.mark_checkpoint_ready("join", 1);
    c.mark_old_drained("join", 0);

    EXPECT_TRUE(c.abort("join", "worker failure"));
    auto st = c.status("join");
    ASSERT_TRUE(st.has_value());
    EXPECT_EQ(st->state, RescaleState::Aborted);
    EXPECT_EQ(st->current_parallelism, 2u);  // unchanged from pre-rescale
    EXPECT_EQ(st->target_parallelism, 2u);   // rolled back
}

TEST(RescaleCoordinator, AbortFromIdleRejected) {
    RescaleCoordinator c;
    setup_coord(c, "join", 2, 1, 8);
    EXPECT_FALSE(c.abort("join", "?"));
}

TEST(RescaleCoordinator, NewRequestAfterCompleteAccepted) {
    RescaleCoordinator c;
    setup_coord(c, "join", 2, 1, 8);
    c.request_rescale("join", 4);
    run_to_complete(c, "join", /*current=*/2, /*target=*/4);
    EXPECT_EQ(c.status("join")->state, RescaleState::Complete);

    // A second rescale starts from the new current_parallelism (4).
    auto r2 = c.request_rescale("join", 6);
    EXPECT_TRUE(r2.ok);
    EXPECT_EQ(c.status("join")->state, RescaleState::Preparing);
    EXPECT_EQ(c.status("join")->current_parallelism, 4u);
    EXPECT_EQ(c.status("join")->target_parallelism, 6u);
}

TEST(RescaleCoordinator, NewRequestAfterAbortAccepted) {
    RescaleCoordinator c;
    setup_coord(c, "join", 2, 1, 8);
    c.request_rescale("join", 4);
    c.mark_checkpoint_ready("join", 1);
    c.abort("join", "worker failure");
    EXPECT_EQ(c.status("join")->state, RescaleState::Aborted);

    // Aborted operator must accept a fresh request.
    auto r = c.request_rescale("join", 3);
    EXPECT_TRUE(r.ok);
    EXPECT_EQ(c.status("join")->state, RescaleState::Preparing);
}

TEST(RescaleCoordinator, UnknownOperatorRejected) {
    RescaleCoordinator c;
    EXPECT_FALSE(c.status("nope").has_value());
    auto r = c.request_rescale("nope", 4);
    EXPECT_FALSE(r.ok);
    EXPECT_NE(r.reason.find("not registered"), std::string::npos);
    EXPECT_FALSE(c.mark_checkpoint_ready("nope", 1));
    EXPECT_FALSE(c.mark_old_drained("nope", 0));
    EXPECT_FALSE(c.mark_new_ready("nope", 0));
    EXPECT_FALSE(c.abort("nope", "?"));
}

TEST(RescaleCoordinator, AllReturnsSortedSnapshot) {
    RescaleCoordinator c;
    c.register_operator("zeta", 1, 1, 4);
    c.register_operator("alpha", 2, 1, 4);
    c.register_operator("mu", 3, 1, 4);
    auto all = c.all();
    ASSERT_EQ(all.size(), 3u);
    EXPECT_EQ(all[0].op_id, "alpha");
    EXPECT_EQ(all[1].op_id, "mu");
    EXPECT_EQ(all[2].op_id, "zeta");
}

TEST(RescaleCoordinator, ToStringCoversEveryState) {
    EXPECT_EQ(to_string(RescaleState::Idle), "Idle");
    EXPECT_EQ(to_string(RescaleState::Preparing), "Preparing");
    EXPECT_EQ(to_string(RescaleState::Draining), "Draining");
    EXPECT_EQ(to_string(RescaleState::CuttingOver), "CuttingOver");
    EXPECT_EQ(to_string(RescaleState::Complete), "Complete");
    EXPECT_EQ(to_string(RescaleState::Aborted), "Aborted");
}
