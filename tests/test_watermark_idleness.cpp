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

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/metrics/metrics_registry.hpp"
#include "clink/operators/watermark_assigner_operator.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/multi_input_alignment.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/runtime/timer_service.hpp"
#include "clink/state/in_memory_state_backend.hpp"

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

// ---------------------------------------------------------------------------
// The watermark's per-partition progress across a restore
// ---------------------------------------------------------------------------

namespace {

using PartitionedRecords = std::vector<std::tuple<int, std::int64_t, std::int32_t>>;

void feed_partitioned(WatermarkAssignerOperator<int>& op,
                      Emitter<int>& em,
                      const PartitionedRecords& records) {
    Batch<int> b;
    for (const auto& [v, ts, partition] : records) {
        Record<int> r{v, EventTime{ts}};
        r.set_source_partition(partition);
        b.push(std::move(r));
    }
    op.process(StreamElement<int>::data(std::move(b)), em);
}

// The greatest non-idle watermark emitted since the last call, or -1.
std::int64_t greatest_watermark(BoundedChannel<StreamElement<int>>& ch) {
    std::int64_t mx = -1;
    while (auto e = ch.try_pop()) {
        if (e->is_watermark() && !e->as_watermark().is_idle()) {
            mx = std::max(mx, e->as_watermark().timestamp().millis());
        }
    }
    return mx;
}

// An assigner running as subtask `index`, or, with no index, one that is not
// told which subtask it is.
std::unique_ptr<WatermarkAssignerOperator<int>> partition_aware_assigner(
    std::optional<std::uint32_t> index = 0) {
    auto op = std::make_unique<WatermarkAssignerOperator<int>>(
        [](const int& v) { return EventTime{v}; },
        std::make_unique<PartitionAwareBoundedOutOfOrdernessStrategy<int>>(0ms));
    if (index.has_value()) {
        op->with_subtask_index(*index);
    }
    return op;
}

// Partitions read unevenly by subtask `index`, checkpointed into `backend`.
void checkpoint_partitions(StateBackend& backend,
                           std::uint32_t index,
                           const PartitionedRecords& records) {
    BoundedChannel<StreamElement<int>> ch(64);
    Emitter<int> em(&ch);
    auto op = partition_aware_assigner(index);
    RuntimeContext ctx(OperatorId{7}, "assigner", nullptr, nullptr);
    op->attach_runtime(&ctx);
    op->open();
    feed_partitioned(*op, em, records);
    op->snapshot_timers(backend, OperatorId{7});
}

// One subtask reading three partitions: partition 1 is the slowest, at 5 s,
// and partition 2 is far ahead, at 90 s.
void checkpoint_uneven_partitions(StateBackend& backend) {
    checkpoint_partitions(backend, 0, {{1, 20'000, 0}, {2, 5'000, 1}, {3, 90'000, 2}});
}

}  // namespace

// A restored assigner holds the watermark at the slowest partition it was
// waiting for. It used to start knowing no partition, so the first to deliver
// after the restore set the watermark alone: a partition restored far ahead of
// the others fired every open window before their records arrived, and those
// records, late, were dropped.
TEST(WatermarkAssignerRestore, ARestoredAssignerWaitsForThePartitionsItWasWaitingFor) {
    InMemoryStateBackend backend;
    checkpoint_uneven_partitions(backend);

    BoundedChannel<StreamElement<int>> ch(64);
    Emitter<int> em(&ch);
    auto op = partition_aware_assigner();
    RuntimeContext ctx(OperatorId{7}, "assigner", nullptr, nullptr);
    op->attach_runtime(&ctx);
    op->restore_timers(backend, OperatorId{7});
    op->open();
    // The partition furthest ahead delivers first.
    feed_partitioned(*op, em, {{4, 95'000, 2}});
    EXPECT_EQ(greatest_watermark(ch), 5'000)
        << "partition 1 was at 5 s and has delivered nothing since the restore";
    feed_partitioned(*op, em, {{5, 30'000, 1}});
    EXPECT_EQ(greatest_watermark(ch), 20'000) << "partition 0, at 20 s, now holds the min";
}

// A restore hands every subtask every subtask's rows, so an assigner keeps
// what its own subtask index wrote. Restored with a peer's partitions, which it
// never reads, its watermark waited for them for ever: the Kafka window
// recovery suite's restarts at parallelism 4 never fired another window.
TEST(WatermarkAssignerRestore, ARestoredAssignerKeepsWhatItsSubtaskRecorded) {
    InMemoryStateBackend backend;
    // Subtask 0 reads partitions 0 and 2, subtask 1 partitions 1 and 3, and
    // both write into the one backend a restore hands each of them.
    checkpoint_partitions(backend, 0, {{1, 20'000, 0}, {2, 90'000, 2}});
    checkpoint_partitions(backend, 1, {{3, 5'000, 1}, {4, 60'000, 3}});

    const auto restored = [&](std::uint32_t index) {
        auto op = partition_aware_assigner(index);
        auto ctx = std::make_unique<RuntimeContext>(OperatorId{7}, "assigner", nullptr, nullptr);
        op->attach_runtime(ctx.get());
        op->restore_timers(backend, OperatorId{7});
        op->open();
        return std::pair{std::move(op), std::move(ctx)};
    };
    {
        BoundedChannel<StreamElement<int>> ch(64);
        Emitter<int> em(&ch);
        auto [op, ctx] = restored(1);
        feed_partitioned(*op, em, {{5, 70'000, 3}});
        EXPECT_EQ(greatest_watermark(ch), 5'000) << "partition 1, at 5 s, holds subtask 1";
        feed_partitioned(*op, em, {{6, 80'000, 1}});
        EXPECT_EQ(greatest_watermark(ch), 70'000)
            << "partitions 0 and 2 are subtask 0's and must not hold subtask 1";
    }
    {
        BoundedChannel<StreamElement<int>> ch(64);
        Emitter<int> em(&ch);
        auto [op, ctx] = restored(0);
        feed_partitioned(*op, em, {{7, 95'000, 2}});
        EXPECT_EQ(greatest_watermark(ch), 20'000) << "partition 0, at 20 s, holds subtask 0";
    }
}

// A rescale moves partitions between subtask indexes, so a rescaled assigner
// carries no progress over and starts as a fresh one does.
TEST(WatermarkAssignerRestore, ARescaledAssignerStartsAsAFreshOneDoes) {
    InMemoryStateBackend backend;
    checkpoint_uneven_partitions(backend);

    BoundedChannel<StreamElement<int>> ch(64);
    Emitter<int> em(&ch);
    auto op = partition_aware_assigner();
    RuntimeContext ctx(OperatorId{7}, "assigner", nullptr, nullptr);
    ctx.set_restore_key_group_range(KeyGroupRange{0, kNumKeyGroups / 2});
    op->attach_runtime(&ctx);
    op->restore_timers(backend, OperatorId{7});
    op->open();
    feed_partitioned(*op, em, {{4, 95'000, 2}});
    EXPECT_EQ(greatest_watermark(ch), 95'000);
}

// A restore that crossed a rescale moves the topology to a new generation; a
// source's chain is not keyed, so its key-group range is not narrowed, and the
// generations are what say the partitions moved.
TEST(WatermarkAssignerRestore, AnAssignerRestoredAcrossARescaleStartsAsAFreshOneDoes) {
    InMemoryStateBackend backend;
    checkpoint_uneven_partitions(backend);

    BoundedChannel<StreamElement<int>> ch(64);
    Emitter<int> em(&ch);
    auto op = partition_aware_assigner();
    RuntimeContext ctx(OperatorId{7}, "assigner", nullptr, nullptr);
    ctx.set_restore_succession(
        RestoreSuccession{.first = 0, .count = 1, .generation = 2, .restore_generation = 1});
    op->attach_runtime(&ctx);
    op->restore_timers(backend, OperatorId{7});
    op->open();
    feed_partitioned(*op, em, {{4, 95'000, 2}});
    EXPECT_EQ(greatest_watermark(ch), 95'000);
}

// An assigner that is not told its subtask index cannot tell its own rows
// from a peer's, so it carries no progress over, as before.
TEST(WatermarkAssignerRestore, AnAssignerWithoutASubtaskIndexStartsAsAFreshOneDoes) {
    InMemoryStateBackend backend;
    checkpoint_uneven_partitions(backend);

    BoundedChannel<StreamElement<int>> ch(64);
    Emitter<int> em(&ch);
    auto op = partition_aware_assigner(std::nullopt);
    RuntimeContext ctx(OperatorId{7}, "assigner", nullptr, nullptr);
    op->attach_runtime(&ctx);
    op->restore_timers(backend, OperatorId{7});
    op->open();
    feed_partitioned(*op, em, {{4, 95'000, 2}});
    EXPECT_EQ(greatest_watermark(ch), 95'000);
}

// A restored partition that never delivers again stops holding the watermark
// once it has been quiet for the idleness window, as one that went quiet
// before the checkpoint would.
TEST(WatermarkAssignerRestore, ARestoredPartitionThatStaysQuietGoesIdle) {
    InMemoryStateBackend backend;
    checkpoint_uneven_partitions(backend);

    BoundedChannel<StreamElement<int>> ch(64);
    Emitter<int> em(&ch);
    auto op = partition_aware_assigner();
    op->with_idleness(50ms);
    RuntimeContext ctx(OperatorId{7}, "assigner", nullptr, nullptr);
    op->attach_runtime(&ctx);
    op->restore_timers(backend, OperatorId{7});
    op->open();
    feed_partitioned(*op, em, {{4, 95'000, 2}});
    EXPECT_EQ(greatest_watermark(ch), 5'000);

    std::this_thread::sleep_for(80ms);
    feed_partitioned(*op, em, {{5, 96'000, 2}});
    op->fire_due_timers(em,
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count());
    EXPECT_EQ(greatest_watermark(ch), 96'000)
        << "partitions 0 and 1, quiet since the restore, no longer hold the min";
}
