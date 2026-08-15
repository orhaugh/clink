// Adaptive checkpoint mode: the policy discipline (thresholds,
// hysteresis, bounded history) and its wiring into the
// CheckpointCoordinator's trigger path. Every test drives the policy
// with a synthetic pressure sequence - never machine load - so the
// behaviours are deterministic:
//
//   1. a healthy pipeline stays aligned;
//   2. sustained pressure switches to unaligned;
//   3. a short spike buys no switch;
//   4. pressure removed, the policy eventually returns to aligned;
//   5. history is bounded;
//   6. the coordinator's trigger stamps the policy's decision on the
//      barrier (and per-trigger override still bypasses it).
//
// Checkpoint CORRECTNESS under either mode is pinned elsewhere: the
// aligner honours the per-barrier stamp (test_co_operator_unaligned
// proves capture/replay for unaligned and no-capture for aligned, and
// the aligner pins the first-seen mode per checkpoint id), so a mode
// flip between checkpoints exercises already-proven per-mode paths.

#include <gtest/gtest.h>

#include "clink/checkpoint/adaptive_mode_policy.hpp"
#include "clink/checkpoint/checkpoint_coordinator.hpp"
#include "clink/state/in_memory_state_backend.hpp"

namespace {

using clink::CheckpointBarrier;
using clink::CheckpointCoordinator;
using clink::checkpoint::AdaptiveModePolicy;
using clink::checkpoint::AdaptiveModePolicyConfig;

AdaptiveModePolicyConfig two_up_three_down() {
    AdaptiveModePolicyConfig cfg;
    cfg.pressure_threshold = 0.5;
    cfg.windows_to_unaligned = 2;
    cfg.windows_to_aligned = 3;
    return cfg;
}

TEST(AdaptiveModePolicy, AHealthyPipelineStaysAligned) {
    AdaptiveModePolicy p{two_up_three_down()};
    for (int i = 0; i < 100; ++i) {
        EXPECT_EQ(p.observe(0.1), CheckpointBarrier::Mode::Aligned);
    }
    EXPECT_EQ(p.switches(), 0U);
}

TEST(AdaptiveModePolicy, SustainedPressureSwitchesToUnaligned) {
    AdaptiveModePolicy p{two_up_three_down()};
    EXPECT_EQ(p.observe(0.9), CheckpointBarrier::Mode::Aligned) << "one window is not sustained";
    EXPECT_EQ(p.observe(0.9), CheckpointBarrier::Mode::Unaligned);
    EXPECT_EQ(p.switches(), 1U);
    // And it stays there while pressure persists.
    EXPECT_EQ(p.observe(0.9), CheckpointBarrier::Mode::Unaligned);
    EXPECT_EQ(p.switches(), 1U);
}

TEST(AdaptiveModePolicy, AShortSpikeBuysNoSwitch) {
    AdaptiveModePolicy p{two_up_three_down()};
    // spike - calm - spike - calm: the pressured run never reaches 2.
    for (int i = 0; i < 20; ++i) {
        EXPECT_EQ(p.observe(1.0), CheckpointBarrier::Mode::Aligned);
        EXPECT_EQ(p.observe(0.0), CheckpointBarrier::Mode::Aligned);
    }
    EXPECT_EQ(p.switches(), 0U);
}

TEST(AdaptiveModePolicy, RemovedPressureEventuallyReturnsAligned) {
    AdaptiveModePolicy p{two_up_three_down()};
    p.observe(0.9);
    p.observe(0.9);
    ASSERT_EQ(p.mode(), CheckpointBarrier::Mode::Unaligned);
    // Three consecutive calm windows to come back - not one, so a brief
    // lull doesn't bounce the mode straight back into the stall it left.
    EXPECT_EQ(p.observe(0.1), CheckpointBarrier::Mode::Unaligned);
    EXPECT_EQ(p.observe(0.1), CheckpointBarrier::Mode::Unaligned);
    EXPECT_EQ(p.observe(0.1), CheckpointBarrier::Mode::Aligned);
    EXPECT_EQ(p.switches(), 2U);
}

TEST(AdaptiveModePolicy, ACalmWindowResetsThePressuredRun) {
    AdaptiveModePolicy p{two_up_three_down()};
    p.observe(0.9);
    p.observe(0.1);  // resets the run
    EXPECT_EQ(p.observe(0.9), CheckpointBarrier::Mode::Aligned)
        << "the pressured run must restart after a calm window";
}

TEST(AdaptiveModePolicy, HistoryIsBoundedAndPressureClamped) {
    auto cfg = two_up_three_down();
    cfg.history_capacity = 4;
    AdaptiveModePolicy p{cfg};
    for (int i = 0; i < 100; ++i) {
        p.observe(2.0);   // clamped to 1.0
        p.observe(-3.0);  // clamped to 0.0
    }
    EXPECT_EQ(p.recent().size(), 4U);
    for (const double v : p.recent()) {
        EXPECT_GE(v, 0.0);
        EXPECT_LE(v, 1.0);
    }
}

// --- coordinator wiring -----------------------------------------------------

TEST(AdaptiveModePolicy, CoordinatorTriggerStampsThePolicyDecision) {
    auto backend = std::make_shared<clink::InMemoryStateBackend>();
    CheckpointCoordinator coord{backend};
    double pressure = 0.0;
    auto cfg = two_up_three_down();
    coord.enable_adaptive_mode([&pressure] { return pressure; }, cfg);

    // Healthy: aligned.
    EXPECT_EQ(coord.trigger().mode(), CheckpointBarrier::Mode::Aligned);
    EXPECT_EQ(coord.trigger().mode(), CheckpointBarrier::Mode::Aligned);

    // Sustained pressure: the second pressured trigger goes unaligned.
    pressure = 0.9;
    EXPECT_EQ(coord.trigger().mode(), CheckpointBarrier::Mode::Aligned);
    EXPECT_EQ(coord.trigger().mode(), CheckpointBarrier::Mode::Unaligned);

    // Pressure removed: three calm triggers to return.
    pressure = 0.0;
    EXPECT_EQ(coord.trigger().mode(), CheckpointBarrier::Mode::Unaligned);
    EXPECT_EQ(coord.trigger().mode(), CheckpointBarrier::Mode::Unaligned);
    EXPECT_EQ(coord.trigger().mode(), CheckpointBarrier::Mode::Aligned);

    ASSERT_NE(coord.adaptive_policy(), nullptr);
    EXPECT_EQ(coord.adaptive_policy()->switches(), 2U);
}

TEST(AdaptiveModePolicy, ExplicitOverrideStillBypassesTheAdaptivePolicy) {
    auto backend = std::make_shared<clink::InMemoryStateBackend>();
    CheckpointCoordinator coord{backend};
    coord.enable_adaptive_mode([] { return 0.0; }, two_up_three_down());
    const auto b = coord.trigger(CheckpointBarrier::Mode::Unaligned);
    EXPECT_EQ(b.mode(), CheckpointBarrier::Mode::Unaligned)
        << "a per-trigger override exists precisely to bypass the policy";
    EXPECT_EQ(coord.adaptive_policy()->switches(), 0U);
}

}  // namespace
