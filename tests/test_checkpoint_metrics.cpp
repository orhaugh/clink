// Checkpoint metric helpers + MultiInputAlignment metric emission.

#include <chrono>
#include <string>

#include <gtest/gtest.h>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/metrics/checkpoint_metrics.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/runtime/multi_input_alignment.hpp"

using namespace clink;

namespace {

std::uint64_t counter_value(const std::string& name) {
    auto snap = MetricsRegistry::global().snapshot();
    for (const auto& [n, v] : snap.counters) {
        if (n == name) {
            return v;
        }
    }
    return 0;
}

// restore + barrier_align_wait are histograms now (OBS-1b).
std::uint64_t hist_count(const std::string& base) {
    return MetricsRegistry::global().histogram(base).snapshot().count;
}

}  // namespace

TEST(CheckpointMetrics, HelperFunctionsAccumulate) {
    using namespace clink::metrics;
    init_checkpoint_metrics();

    const auto trig_before = counter_value(kCheckpointTriggered);
    const auto comp_before = counter_value(kCheckpointCompleted);
    const auto fail_before = counter_value(kCheckpointFailed);
    const auto ok_before = counter_value(kSubtaskSnapshotAck);
    const auto bad_before = counter_value(kSubtaskSnapshotFailure);

    ckpt::triggered();
    ckpt::triggered();
    ckpt::completed(120);
    ckpt::completed(80);
    ckpt::failed();
    ckpt::subtask_ack_ok();
    ckpt::subtask_ack_failure();
    ckpt::restore_observe(5000);

    EXPECT_EQ(counter_value(kCheckpointTriggered) - trig_before, 2u);
    EXPECT_EQ(counter_value(kCheckpointCompleted) - comp_before, 2u);
    EXPECT_EQ(counter_value(kCheckpointFailed) - fail_before, 1u);
    EXPECT_EQ(counter_value(kSubtaskSnapshotAck) - ok_before, 1u);
    EXPECT_EQ(counter_value(kSubtaskSnapshotFailure) - bad_before, 1u);
    EXPECT_GE(counter_value(kCheckpointDurationMsSum), 200u);
    EXPECT_GE(hist_count(kRestoreFromSavepointNs), 1u);
}

TEST(CheckpointMetrics, MultiInputAlignmentEmitsBarrierAlignWait) {
    MultiInputAlignment a(2);
    const std::uint64_t op_id = 4242;
    a.set_operator_id(op_id);

    const auto before =
        counter_value(clink::metrics::op_metric_name("barrier_alignments_total", op_id));

    // Deliver the same barrier id on both inputs - alignment should
    // fire on the second delivery and emit the metric.
    CheckpointBarrier b1{CheckpointId{7}, /*terminal=*/false, CheckpointBarrier::Mode::Aligned};
    auto adv1 = a.on_barrier(0, b1);
    EXPECT_FALSE(adv1.forward);
    auto adv2 = a.on_barrier(1, b1);
    EXPECT_TRUE(adv2.forward);

    EXPECT_EQ(
        counter_value(clink::metrics::op_metric_name("barrier_alignments_total", op_id)) - before,
        1u);
    EXPECT_GE(hist_count(clink::metrics::op_metric_name("barrier_align_wait_ns", op_id)), 1u);
}

TEST(CheckpointMetrics, AlignmentMetricsDisabledWhenOpIdUnset) {
    MultiInputAlignment a(1);
    // No set_operator_id call -> op_id_for_metrics_ stays 0.
    CheckpointBarrier b{CheckpointId{99}, /*terminal=*/false, CheckpointBarrier::Mode::Aligned};
    auto adv = a.on_barrier(0, b);
    EXPECT_TRUE(adv.forward);
    // No counter created for op_id=0 because the aligner short-circuits.
    EXPECT_EQ(counter_value(clink::metrics::op_metric_name("barrier_alignments_total", 0)), 0u);
}
