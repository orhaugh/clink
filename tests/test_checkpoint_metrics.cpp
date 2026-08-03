// Checkpoint metric helpers + MultiInputAlignment metric emission.

#include <chrono>
#include <numeric>
#include <string>

#include <gtest/gtest.h>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/metrics/checkpoint_metrics.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/metrics/prometheus.hpp"
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

std::int64_t gauge_value(const std::string& name) {
    auto snap = MetricsRegistry::global().snapshot();
    for (const auto& [n, v] : snap.gauges) {
        if (n == name) {
            return v;
        }
    }
    return -1;  // distinguishable from a legitimate 0
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
    // Duration is a histogram now. The sum is still asserted, because the
    // `_sum`/`_count` exposition names are unchanged and existing queries
    // depend on them; the observations and a quantile are asserted too,
    // because a histogram whose sum is right and whose buckets are empty
    // would satisfy the old assertion alone.
    const auto duration = MetricsRegistry::global()
                              .histogram(kCheckpointDurationMs, checkpoint_duration_buckets_ms())
                              .snapshot();
    EXPECT_GE(duration.sum, 200.0);
    EXPECT_GE(duration.count, 2u);
    EXPECT_GT(std::accumulate(duration.bucket_counts.begin(), duration.bucket_counts.end(), 0ULL),
              0ULL)
        << "the duration histogram recorded a sum but landed no observation in any bucket";
    // 120ms and 80ms both fall in the 100ms/250ms region, so a p99 has to sit
    // inside the plausible range rather than at 0 or at the +Inf edge.
    const auto p99 = duration.quantile(0.99);
    EXPECT_GT(p99, 0.0) << "p99 checkpoint duration is unavailable, which is the reason this is a "
                           "histogram rather than a counter pair";
    EXPECT_LE(p99, 60000.0);
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

// --- checkpoint staleness ------------------------------------------------

TEST(CheckpointMetrics, TheLastCompletedTimestampExistsBeforeAnyCheckpointCompletes) {
    // The series has to EXIST from startup, not from the first completion.
    // An alert of the form `time() - clink_ckpt_last_completed_unix_seconds
    // > N` has nothing to evaluate against a missing series and silently
    // does not fire - and a fresh coordinator whose very first checkpoint
    // never completes is exactly when that alert is needed most.
    clink::metrics::init_checkpoint_metrics();
    EXPECT_NE(gauge_value(clink::metrics::kCheckpointLastCompletedUnixSeconds), -1)
        << "the gauge is not pre-registered, so an alert on checkpoint staleness would have no "
           "series until the first checkpoint completed";
}

TEST(CheckpointMetrics, TheLastCompletedTimestampAdvancesOnCompletion) {
    clink::metrics::init_checkpoint_metrics();
    const auto before = gauge_value(clink::metrics::kCheckpointLastCompletedUnixSeconds);
    clink::metrics::ckpt::last_completed_now();
    const auto after = gauge_value(clink::metrics::kCheckpointLastCompletedUnixSeconds);
    EXPECT_GT(after, before);
    // A plausible epoch second, not a duration or a millisecond count: the
    // whole point is that a dashboard can subtract it from time(), and a
    // unit mistake would make the alert silently wrong rather than absent.
    // 1.7e9 is 2023; 4e9 is 2096.
    EXPECT_GT(after, 1'700'000'000) << "value " << after << " is not an epoch SECOND";
    EXPECT_LT(after, 4'000'000'000) << "value " << after << " is not an epoch second";
}

TEST(CheckpointMetrics, CompletionCountAndTimestampAnswerDifferentQuestions) {
    // Both are emitted, and neither substitutes for the other: the counter
    // says how many, the timestamp says how recently. This asserts they are
    // separate series rather than one wired to look like two.
    clink::metrics::init_checkpoint_metrics();
    const auto count_before = counter_value(clink::metrics::kCheckpointCompleted);
    const auto stamp_before = gauge_value(clink::metrics::kCheckpointLastCompletedUnixSeconds);

    clink::metrics::ckpt::completed(42);
    EXPECT_EQ(counter_value(clink::metrics::kCheckpointCompleted), count_before + 1);
    EXPECT_EQ(gauge_value(clink::metrics::kCheckpointLastCompletedUnixSeconds), stamp_before)
        << "completed() moved the timestamp; the two must be independent so a caller cannot "
           "stamp a completion it did not have";

    clink::metrics::ckpt::last_completed_now();
    EXPECT_EQ(counter_value(clink::metrics::kCheckpointCompleted), count_before + 1)
        << "last_completed_now() incremented the completion counter";
}

TEST(CheckpointMetrics, TheDurationHistogramExposesOneSumAndOneCountNotTwo) {
    // Duration moved from a counter PAIR to a histogram, and the histogram's
    // derived names are exactly the names the counters had. That is what
    // keeps existing queries working - and it is also the trap: if the
    // counters were ever reinstated alongside the histogram, the scrape would
    // carry two `clink_ckpt_duration_ms_sum` lines. Prometheus rejects a
    // duplicated series, so the whole endpoint fails, not just this metric.
    //
    // Counting occurrences rather than checking presence is the point; a
    // contains() assertion passes just as happily on a broken scrape.
    clink::metrics::init_checkpoint_metrics();
    clink::metrics::ckpt::completed(120);

    const auto text = clink::metrics::render_prometheus(MetricsRegistry::global().snapshot());
    const auto occurrences = [&text](const std::string& needle) {
        std::size_t n = 0;
        std::size_t pos = 0;
        while ((pos = text.find(needle, pos)) != std::string::npos) {
            ++n;
            pos += needle.size();
        }
        return n;
    };

    EXPECT_EQ(occurrences("clink_ckpt_duration_ms_sum"), 1u)
        << "duplicate _sum series would make the entire scrape endpoint invalid";
    EXPECT_EQ(occurrences("clink_ckpt_duration_ms_count"), 1u);
    EXPECT_GT(occurrences("clink_ckpt_duration_ms_bucket"), 1u)
        << "no bucket lines, so a p99 is still unavailable and this is a counter pair by another "
           "name";
}
