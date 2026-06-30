// Idle-source detection tests (WatermarkStrategy.withIdleness).
//
// Two layers exercised:
//   * MultiInputAlignment: an idle input is skipped from the running
//     min watermark; coming back from idle clamps to global; all-idle
//     emits a single idle marker downstream rather than degenerating
//     to Watermark::max().
//   * WatermarkAssignerOperator: when no record arrives for the
//     configured duration, the operator emits an idle watermark via
//     its processing-time timer; the next record returns it to active.

#include <chrono>
#include <cstdint>
#include <memory>
#include <thread>
#include <tuple>
#include <vector>

#include <gtest/gtest.h>

#include "clink/metrics/metrics_registry.hpp"
#include "clink/operators/watermark_assigner_operator.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/multi_input_alignment.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/runtime/timer_service.hpp"

using namespace clink;
using namespace std::chrono_literals;

// ---------------------------------------------------------------------------
// MultiInputAlignment
// ---------------------------------------------------------------------------

TEST(MultiInputAlignmentIdleness, IdleInputDoesNotConstrainMinWatermark) {
    MultiInputAlignment a(2);

    // Input 0 advances to t=100, input 1 advances to t=50. Min = 50.
    auto adv = a.on_watermark(0, Watermark{EventTime{100}});
    EXPECT_FALSE(adv.forward) << "input 1 still at min; no advance yet";
    adv = a.on_watermark(1, Watermark{EventTime{50}});
    EXPECT_TRUE(adv.forward);
    EXPECT_EQ(adv.watermark.timestamp().millis(), 50);

    // Input 1 goes idle. With it excluded, min becomes input 0's 100.
    adv = a.on_watermark(1, Watermark::idle(EventTime{50}));
    EXPECT_TRUE(adv.forward);
    EXPECT_EQ(adv.watermark.timestamp().millis(), 100);
    EXPECT_FALSE(adv.watermark.is_idle()) << "global wm is active when at least one input is";
}

TEST(MultiInputAlignmentIdleness, IdleInputReturningActiveClampsToGlobal) {
    MultiInputAlignment a(2);

    a.on_watermark(0, Watermark{EventTime{100}});
    a.on_watermark(1, Watermark{EventTime{50}});
    a.on_watermark(1, Watermark::idle(EventTime{50}));
    // Global is now 100 (input 1 excluded).

    // Input 1 comes back with a watermark at t=70 - below global 100.
    // It should NOT regress global; the input's effective wm is clamped
    // to 100.
    auto adv = a.on_watermark(1, Watermark{EventTime{70}});
    EXPECT_FALSE(adv.forward) << "clamped re-entry must not advance below global";

    // Now input 0 advances to t=200. Min = min(200, 100) = 100. No
    // change. (Input 1 was clamped to global=100; needs more time
    // to actually constrain things.)
    adv = a.on_watermark(0, Watermark{EventTime{200}});
    EXPECT_FALSE(adv.forward);

    // Input 1 advances to t=150 - above global 100. Min = min(200,
    // 150) = 150. Advance.
    adv = a.on_watermark(1, Watermark{EventTime{150}});
    EXPECT_TRUE(adv.forward);
    EXPECT_EQ(adv.watermark.timestamp().millis(), 150);
}

TEST(MultiInputAlignmentIdleness, AllAliveInputsIdleEmitsSingleIdleMarker) {
    MultiInputAlignment a(2);

    a.on_watermark(0, Watermark{EventTime{100}});
    a.on_watermark(1, Watermark{EventTime{100}});

    // Both go idle.
    a.on_watermark(0, Watermark::idle(EventTime{100}));
    auto adv = a.on_watermark(1, Watermark::idle(EventTime{100}));
    ASSERT_TRUE(adv.forward);
    EXPECT_TRUE(adv.watermark.is_idle()) << "all-alive-idle should emit idle marker";

    // Subsequent idle re-affirmations: no further forwards (don't
    // spam downstream).
    adv = a.on_watermark(0, Watermark::idle(EventTime{100}));
    EXPECT_FALSE(adv.forward);
}

TEST(MultiInputAlignmentIdleness, AllClosedStillEmitsMaxWatermark) {
    // Regression guard: closed != idle. When ALL inputs close (not
    // idle), the aligner forwards Watermark::max() - the historic
    // end-of-stream behavior - not an idle marker.
    MultiInputAlignment a(2);

    a.on_watermark(0, Watermark{EventTime{100}});
    a.on_watermark(1, Watermark{EventTime{100}});

    a.on_input_closed(0);
    auto adv = a.on_input_closed(1);
    (void)adv;
    auto refresh = a.refresh_watermark();
    EXPECT_TRUE(refresh.forward);
    EXPECT_EQ(refresh.watermark, Watermark::max());
    EXPECT_FALSE(refresh.watermark.is_idle());
}

// ---------------------------------------------------------------------------
// WatermarkAssignerOperator with idleness
// ---------------------------------------------------------------------------

TEST(WatermarkAssignerIdleness, EmitsIdleAfterTimeoutAndActiveOnNextRecord) {
    BoundedChannel<StreamElement<int>> out_ch(64);
    Emitter<int> em(&out_ch);

    auto strategy = std::make_unique<MonotonicWatermarkStrategy<int>>();
    WatermarkAssignerOperator<int> op([](const int& v) { return EventTime{v}; },
                                      std::move(strategy));
    op.with_idleness(50ms);

    RuntimeContext ctx(OperatorId{1}, "assigner", nullptr, nullptr);
    op.attach_runtime(&ctx);
    op.open();

    // Push one record with event_time=100 - strategy should emit a
    // non-idle watermark.
    {
        Batch<int> b;
        b.emplace(100, EventTime{100});
        op.process(StreamElement<int>::data(std::move(b)), em);
    }

    // Wait for the idleness timer to fire (50ms threshold + probe at
    // 25ms cadence). Give plenty of slack.
    std::this_thread::sleep_for(150ms);
    // Manually invoke timer fire - TimerService doesn't auto-fire;
    // operator_base's fire_due_timers is what the runner uses.
    op.fire_due_timers(em,
                       std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count());

    auto elements = [&] {
        std::vector<StreamElement<int>> out;
        while (auto e = out_ch.try_pop()) {
            out.push_back(std::move(*e));
        }
        return out;
    }();

    bool saw_active_wm = false;
    bool saw_idle_wm = false;
    for (const auto& e : elements) {
        if (e.is_watermark()) {
            if (e.as_watermark().is_idle()) {
                saw_idle_wm = true;
            } else {
                saw_active_wm = true;
            }
        }
    }
    EXPECT_TRUE(saw_active_wm) << "active watermark after record";
    EXPECT_TRUE(saw_idle_wm) << "idle watermark after idleness timeout";

    // A new record should return us to active. Drain again afterwards.
    {
        Batch<int> b;
        b.emplace(200, EventTime{200});
        op.process(StreamElement<int>::data(std::move(b)), em);
    }
    bool saw_post_active = false;
    while (auto e = out_ch.try_pop()) {
        if (e->is_watermark() && !e->as_watermark().is_idle()) {
            saw_post_active = true;
        }
    }
    EXPECT_TRUE(saw_post_active) << "active watermark resumes after a record";
}

TEST(WatermarkAssignerIdleness, PerPartitionIdleAdvancesPastQuietPartition) {
    // One source, two partitions. P1 lags and goes quiet; P0 stays active. The
    // per-partition min would stay pinned to P1 forever; after the idle timeout
    // the probe excludes P1 and the watermark advances to P0.
    BoundedChannel<StreamElement<int>> out_ch(64);
    Emitter<int> em(&out_ch);

    auto strategy = std::make_unique<PartitionAwareBoundedOutOfOrdernessStrategy<int>>(0ms);
    WatermarkAssignerOperator<int> op([](const int& v) { return EventTime{v}; },
                                      std::move(strategy));
    op.with_idleness(50ms);
    RuntimeContext ctx(OperatorId{1}, "assigner", nullptr, nullptr);
    op.attach_runtime(&ctx);
    op.open();

    auto feed = [&](std::vector<std::tuple<int, std::int64_t, std::int32_t>> recs) {
        Batch<int> b;
        for (auto [v, ts, partition] : recs) {
            Record<int> r{v, EventTime{ts}};
            r.set_source_partition(partition);
            b.push(std::move(r));
        }
        op.process(StreamElement<int>::data(std::move(b)), em);
    };
    auto max_active_wm = [&]() -> std::int64_t {
        std::int64_t mx = -1;
        while (auto e = out_ch.try_pop()) {
            if (e->is_watermark() && !e->as_watermark().is_idle()) {
                mx = std::max(mx, e->as_watermark().timestamp().millis());
            }
        }
        return mx;
    };

    // Both partitions in one batch so the min (P1=50) is the first watermark
    // (a per-partition batch would race the watermark to whichever came first).
    feed({{1, 100, /*partition=*/0}, {2, 50, /*partition=*/1}});
    EXPECT_EQ(max_active_wm(), 50) << "min across partitions follows the slower P1";

    // P1 now goes quiet. P0 keeps advancing - but the min stays pinned to P1=50.
    std::this_thread::sleep_for(80ms);
    feed({{3, 300, /*partition=*/0}});
    EXPECT_EQ(max_active_wm(), -1) << "P1 still pins the min; no advance from a record alone";

    // The idle probe fires: P1 (quiet > 50ms) is excluded, the min jumps to P0.
    op.fire_due_timers(em,
                       std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count());
    EXPECT_EQ(max_active_wm(), 300) << "watermark advances past the idle partition";
}

TEST(WatermarkAssignerIdleness, NoTimeoutWhenIdlenessDisabled) {
    // With idleness=0 (default), the operator should never emit idle
    // watermarks even after sitting quiet.
    BoundedChannel<StreamElement<int>> out_ch(64);
    Emitter<int> em(&out_ch);

    auto strategy = std::make_unique<MonotonicWatermarkStrategy<int>>();
    WatermarkAssignerOperator<int> op([](const int& v) { return EventTime{v}; },
                                      std::move(strategy));
    // No with_idleness() call.

    RuntimeContext ctx(OperatorId{1}, "assigner", nullptr, nullptr);
    op.attach_runtime(&ctx);
    op.open();

    {
        Batch<int> b;
        b.emplace(100, EventTime{100});
        op.process(StreamElement<int>::data(std::move(b)), em);
    }

    std::this_thread::sleep_for(50ms);
    op.fire_due_timers(em,
                       std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count());

    bool saw_idle = false;
    while (auto e = out_ch.try_pop()) {
        if (e->is_watermark() && e->as_watermark().is_idle()) {
            saw_idle = true;
        }
    }
    EXPECT_FALSE(saw_idle) << "idleness disabled - no idle markers";
}

// ---------------------------------------------------------------------------
// Wire format round-trip
// ---------------------------------------------------------------------------

TEST(WatermarkIdleness, IdleWatermarkConstructsAndPreservesFlag) {
    auto active = Watermark{EventTime{500}};
    EXPECT_FALSE(active.is_idle());

    auto idle = Watermark::idle(EventTime{500});
    EXPECT_TRUE(idle.is_idle());
    EXPECT_EQ(idle.timestamp().millis(), 500);

    auto explicit_idle = Watermark{EventTime{42}, true};
    EXPECT_TRUE(explicit_idle.is_idle());
    EXPECT_EQ(explicit_idle.timestamp().millis(), 42);
}
