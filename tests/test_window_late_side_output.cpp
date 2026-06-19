// Late-data side output tests across all 6 window operators.
//
// AllowedLateness gives records arriving within (window_end + lateness)
// a re-fire window. Records arriving AFTER that band are by default
// dropped silently. With .late_output_tag(OutputTag<Value>) the operator
// instead forwards the late-late record (untouched, with its original
// event_time) to the named side output, mirroring // .sideOutputLateData(OutputTag<T>).
//
// Coverage:
//   * TumblingWindowOperator
//   * SlidingWindowOperator
//   * SessionWindowOperator
//   * EvictingTumblingWindowOperator
//   * TumblingProcessWindowAdapter
//   * SlidingProcessWindowAdapter
//   * SessionProcessWindowAdapter

#include <chrono>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/metrics/metrics_registry.hpp"
#include "clink/operators/evicting_tumbling_window_operator.hpp"
#include "clink/operators/process_function.hpp"
#include "clink/operators/session_window_operator.hpp"
#include "clink/operators/sliding_window_operator.hpp"
#include "clink/operators/tumbling_window_operator.hpp"
#include "clink/operators/window_evictor.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/runtime_context.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

// Drain a typed channel into a flat list of stream elements.
template <typename T>
std::vector<StreamElement<T>> drain(BoundedChannel<StreamElement<T>>& ch) {
    std::vector<StreamElement<T>> out;
    while (auto e = ch.try_pop()) {
        out.push_back(std::move(*e));
    }
    return out;
}

// Pull out just the data records (ignore watermarks/barriers).
template <typename T>
std::vector<std::pair<T, std::int64_t>> data_records(
    const std::vector<StreamElement<T>>& elements) {
    std::vector<std::pair<T, std::int64_t>> out;
    for (const auto& e : elements) {
        if (!e.is_data()) {
            continue;
        }
        for (const auto& r : e.as_data()) {
            const auto ts = r.event_time().has_value() ? r.event_time()->millis() : 0;
            out.emplace_back(r.value(), ts);
        }
    }
    return out;
}

}  // namespace

TEST(TumblingWindowLateSideOutput, RecordPastLatenessRoutedToSideOutput) {
    // Main output: per-key aggregate. Late side output: raw int value
    // that arrived after the lateness window expired.
    BoundedChannel<StreamElement<std::pair<int, int>>> main_ch(64);
    BoundedChannel<StreamElement<int>> late_ch(64);

    Emitter<std::pair<int, int>> main_em(&main_ch);

    TumblingWindowOperator<int, int, int> op(
        1000ms, [] { return 0; }, [](int a, int v) { return a + v; });
    op.allowed_lateness(500ms);
    OutputTag<int> late_tag("late");
    op.late_output_tag(late_tag);

    // Attach a RuntimeContext that owns a typed side-output channel
    // backing the late tag. This mirrors what the executor sets up
    // for an operator's RuntimeContext::set_side_output_channels.
    RuntimeContext ctx(OperatorId{1}, "tumbling_window", nullptr, nullptr);
    SideOutputChannelMap channels;
    auto late_ch_shared =
        std::shared_ptr<BoundedChannel<StreamElement<int>>>(&late_ch, [](auto*) {});
    SideOutputChannelEntry entry;
    entry.channel = std::static_pointer_cast<void>(late_ch_shared);
    entry.close_fn = [late_ch_shared] { late_ch_shared->close(); };
    channels.emplace("late", std::move(entry));
    ctx.set_side_output_channels(std::move(channels));
    op.attach_runtime(&ctx);

    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), main_em);
    };
    auto wm = [&](std::int64_t ts) {
        op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{ts}}),
                   main_em);
    };

    // Records in window [0, 1000): sum=3.
    send(1, 100);
    send(2, 500);

    // Watermark to 1000 - on-time pane fires (sum=3).
    wm(1000);
    auto first = data_records<std::pair<int, int>>(drain(main_ch));
    ASSERT_EQ(first.size(), 1u);
    EXPECT_EQ(first[0].first.second, 3);

    // Watermark to 1700 - beyond window_end (1000) + allowed_lateness
    // (500) = 1500, so the window is now purged. Any subsequent record
    // for this window is late-late.
    wm(1700);
    (void)drain(main_ch);  // drop any state-driven emissions; none
                           // expected for the lateness deadline tick.

    // Send a record at t=200 - belongs to window [0, 1000), which is
    // long gone. Expect: no aggregate emission; record forwarded to
    // the "late" side output untouched.
    send(99, 200);
    auto main_after = data_records<std::pair<int, int>>(drain(main_ch));
    EXPECT_TRUE(main_after.empty()) << "late-late record must not feed the aggregate";

    auto late_records = data_records<int>(drain(late_ch));
    ASSERT_EQ(late_records.size(), 1u);
    EXPECT_EQ(late_records[0].first, 99);
    EXPECT_EQ(late_records[0].second, 200);  // event_time preserved
}

TEST(TumblingWindowLateSideOutput, RecordWithinLatenessStillRefires) {
    // Sanity: a late record WITHIN the lateness band still drives the
    // aggregate (existing behavior). The side-output path is only for
    // records past the deadline; this confirms we didn't break the
    // within-lateness re-fire.
    BoundedChannel<StreamElement<std::pair<int, int>>> main_ch(64);
    BoundedChannel<StreamElement<int>> late_ch(64);

    Emitter<std::pair<int, int>> main_em(&main_ch);

    TumblingWindowOperator<int, int, int> op(
        1000ms, [] { return 0; }, [](int a, int v) { return a + v; });
    op.allowed_lateness(500ms);
    OutputTag<int> late_tag("late");
    op.late_output_tag(late_tag);

    RuntimeContext ctx(OperatorId{1}, "tumbling_window", nullptr, nullptr);
    SideOutputChannelMap channels;
    auto late_ch_shared =
        std::shared_ptr<BoundedChannel<StreamElement<int>>>(&late_ch, [](auto*) {});
    SideOutputChannelEntry entry;
    entry.channel = std::static_pointer_cast<void>(late_ch_shared);
    entry.close_fn = [late_ch_shared] { late_ch_shared->close(); };
    channels.emplace("late", std::move(entry));
    ctx.set_side_output_channels(std::move(channels));
    op.attach_runtime(&ctx);

    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), main_em);
    };
    auto wm = [&](std::int64_t ts) {
        op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{ts}}),
                   main_em);
    };

    send(1, 100);
    wm(1000);  // on-time pane fires sum=1
    (void)drain(main_ch);

    // Now send a record at t=200 - window already fired but still
    // within lateness (wm=1000, purge_at=1500). Should re-fire,
    // updated aggregate=3. Should NOT go to side output.
    wm(1200);  // bump watermark but still within lateness band
    send(2, 200);
    auto refire = data_records<std::pair<int, int>>(drain(main_ch));
    ASSERT_EQ(refire.size(), 1u);
    EXPECT_EQ(refire[0].first.second, 3);

    auto late = data_records<int>(drain(late_ch));
    EXPECT_TRUE(late.empty()) << "within-lateness records must not hit the side output";
}

TEST(TumblingWindowLateSideOutput, NoTagPreservesHistoricBehavior) {
    // Without a late_output_tag the operator must NOT change behavior.
    // The historic "late record creates a fresh bucket" semantics
    // (documented in test_tumbling_window_operator's
    // FiringPurgesBucketSoLateRecordsCannotResurrectIt + the existing
    // allowed_lateness suite) must hold. This test guards the
    // backward-compat hinge that the new side-output path is
    // strictly additive.
    BoundedChannel<StreamElement<std::pair<int, int>>> main_ch(64);
    Emitter<std::pair<int, int>> main_em(&main_ch);

    TumblingWindowOperator<int, int, int> op(
        1000ms, [] { return 0; }, [](int a, int v) { return a + v; });
    op.allowed_lateness(500ms);
    // No late_output_tag - historic semantics apply.

    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), main_em);
    };
    auto wm = [&](std::int64_t ts) {
        op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{ts}}),
                   main_em);
    };

    send(1, 100);
    wm(2000);  // past purge_at (1500). On-time pane fires.
    (void)drain(main_ch);

    // Late-late record: with no tag, this falls through to
    // handle_record_ which creates a fresh bucket. That bucket
    // fires on the next watermark (or flush) - matches the
    // historic behavior captured by
    // FiringPurgesBucketSoLateRecordsCannotResurrectIt.
    send(99, 200);
    op.flush(main_em);
    auto out = data_records<std::pair<int, int>>(drain(main_ch));
    EXPECT_FALSE(out.empty()) << "no-tag late record should create fresh bucket (historic)";
}

// ---------------------------------------------------------------------------
// Shared helper: attach a single named side-output channel to a fresh
// RuntimeContext and install it onto `op`. All the per-operator tests
// below use the same pattern; factoring it out keeps each test tight.
// ---------------------------------------------------------------------------
namespace {
template <typename Op, typename SideT>
RuntimeContext attach_late_output_(Op& op,
                                   BoundedChannel<StreamElement<SideT>>& side_ch,
                                   const std::string& tag) {
    RuntimeContext ctx(OperatorId{1}, "op_under_test", nullptr, nullptr);
    SideOutputChannelMap channels;
    auto shared = std::shared_ptr<BoundedChannel<StreamElement<SideT>>>(&side_ch, [](auto*) {});
    SideOutputChannelEntry entry;
    entry.channel = std::static_pointer_cast<void>(shared);
    entry.close_fn = [shared] { shared->close(); };
    channels.emplace(tag, std::move(entry));
    ctx.set_side_output_channels(std::move(channels));
    op.attach_runtime(&ctx);
    return ctx;
}
}  // namespace

// ----- SlidingWindowOperator ----------------------------------------------

TEST(SlidingWindowLateSideOutput, RecordPastAllCoveringWindowsRoutedToSideOutput) {
    BoundedChannel<StreamElement<std::pair<int, int>>> main_ch(64);
    BoundedChannel<StreamElement<int>> late_ch(64);
    Emitter<std::pair<int, int>> main_em(&main_ch);

    // Slide=500, size=1000 - each record belongs to 2 windows.
    SlidingWindowOperator<int, int, int> op(
        1000ms, 500ms, [] { return 0; }, [](int a, int v) { return a + v; });
    op.allowed_lateness(200ms);
    OutputTag<int> late_tag("late");
    op.late_output_tag(late_tag);

    auto ctx = attach_late_output_<decltype(op), int>(op, late_ch, "late");
    (void)ctx;

    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), main_em);
    };
    auto wm = [&](std::int64_t ts) {
        op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{ts}}),
                   main_em);
    };

    // Record at t=100 → windows [-500, 500) and [0, 1000). Latest end
    // = 1000; purge_at = 1200.
    send(1, 100);
    wm(2000);  // far past purge - both covering windows are gone.
    (void)drain(main_ch);

    send(99, 200);  // late-late
    auto main_after = data_records<std::pair<int, int>>(drain(main_ch));
    EXPECT_TRUE(main_after.empty());
    auto late = data_records<int>(drain(late_ch));
    ASSERT_EQ(late.size(), 1u);
    EXPECT_EQ(late[0].first, 99);
    EXPECT_EQ(late[0].second, 200);
}

TEST(SlidingWindowLateSideOutput, NoTagPreservesHistoricBehavior) {
    BoundedChannel<StreamElement<std::pair<int, int>>> main_ch(64);
    Emitter<std::pair<int, int>> main_em(&main_ch);

    SlidingWindowOperator<int, int, int> op(
        1000ms, 500ms, [] { return 0; }, [](int a, int v) { return a + v; });
    op.allowed_lateness(200ms);
    // No tag.

    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), main_em);
    };
    auto wm = [&](std::int64_t ts) {
        op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{ts}}),
                   main_em);
    };

    send(1, 100);
    wm(2000);
    (void)drain(main_ch);
    send(99, 200);  // late-late; historic path = fresh bucket.
    op.flush(main_em);
    auto out = data_records<std::pair<int, int>>(drain(main_ch));
    EXPECT_FALSE(out.empty());
}

// ----- SessionWindowOperator -----------------------------------------------

TEST(SessionWindowLateSideOutput, RecordPastLatenessRoutedToSideOutput) {
    BoundedChannel<StreamElement<std::pair<int, int>>> main_ch(64);
    BoundedChannel<StreamElement<int>> late_ch(64);
    Emitter<std::pair<int, int>> main_em(&main_ch);

    SessionWindowOperator<int, int, int> op(
        500ms,
        [] { return 0; },
        [](int a, int v) { return a + v; },
        [](int a, int b) { return a + b; });
    op.allowed_lateness(100ms);
    OutputTag<int> late_tag("late");
    op.late_output_tag(late_tag);

    auto ctx = attach_late_output_<decltype(op), int>(op, late_ch, "late");
    (void)ctx;

    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), main_em);
    };
    auto wm = [&](std::int64_t ts) {
        op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{ts}}),
                   main_em);
    };

    // Establish a session at ts=100, gap=500 → end=600.
    send(1, 100);
    wm(2000);  // wm > 100 + 500 + 100 = 700 → record at ts<=1400 is late-late.
    (void)drain(main_ch);

    send(99, 200);  // ts + gap + lateness = 800 < wm=2000 → late-late.
    auto main_after = data_records<std::pair<int, int>>(drain(main_ch));
    EXPECT_TRUE(main_after.empty());
    auto late = data_records<int>(drain(late_ch));
    ASSERT_EQ(late.size(), 1u);
    EXPECT_EQ(late[0].first, 99);
}

TEST(SessionWindowLateSideOutput, NoTagPreservesHistoricBehavior) {
    BoundedChannel<StreamElement<std::pair<int, int>>> main_ch(64);
    Emitter<std::pair<int, int>> main_em(&main_ch);

    SessionWindowOperator<int, int, int> op(
        500ms,
        [] { return 0; },
        [](int a, int v) { return a + v; },
        [](int a, int b) { return a + b; });
    op.allowed_lateness(100ms);

    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), main_em);
    };
    auto wm = [&](std::int64_t ts) {
        op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{ts}}),
                   main_em);
    };

    send(1, 100);
    wm(2000);
    (void)drain(main_ch);
    send(99, 200);  // late-late; historic path = new session.
    op.flush(main_em);
    auto out = data_records<std::pair<int, int>>(drain(main_ch));
    EXPECT_FALSE(out.empty());
}

// ----- EvictingTumblingWindowOperator -------------------------------------

TEST(EvictingTumblingWindowLateSideOutput, RecordPastLatenessRoutedToSideOutput) {
    BoundedChannel<StreamElement<std::pair<int, int>>> main_ch(64);
    BoundedChannel<StreamElement<int>> late_ch(64);
    Emitter<std::pair<int, int>> main_em(&main_ch);

    // Process function: emit count of records as the output value.
    EvictingTumblingWindowOperator<int, int, int> op(
        1000ms,
        [](const std::vector<Record<int>>& records, const TimeWindow&) {
            return static_cast<int>(records.size());
        },
        std::make_unique<CountEvictor<int>>(100));
    op.allowed_lateness(500ms);
    OutputTag<int> late_tag("late");
    op.late_output_tag(late_tag);

    auto ctx = attach_late_output_<decltype(op), int>(op, late_ch, "late");
    (void)ctx;

    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), main_em);
    };
    auto wm = [&](std::int64_t ts) {
        op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{ts}}),
                   main_em);
    };

    send(1, 100);
    wm(2000);  // past purge_at (1500).
    (void)drain(main_ch);
    send(99, 200);  // late-late
    auto main_after = data_records<std::pair<int, int>>(drain(main_ch));
    EXPECT_TRUE(main_after.empty());
    auto late = data_records<int>(drain(late_ch));
    ASSERT_EQ(late.size(), 1u);
    EXPECT_EQ(late[0].first, 99);
}

// ----- TumblingProcessWindowAdapter ---------------------------------------

namespace {
// ProcessWindowFunction that emits the count of elements as a single
// Out=int per window.
struct CountWindowFn final : public ProcessWindowFunction<int, int, int> {
    void process(const int&,
                 const WindowContext&,
                 const std::vector<int>& elements,
                 Collector<int>& out) override {
        out.collect(static_cast<int>(elements.size()));
    }
};
}  // namespace

TEST(TumblingProcessWindowLateSideOutput, RecordPastWindowEndRoutedToSideOutput) {
    BoundedChannel<StreamElement<int>> main_ch(64);
    BoundedChannel<StreamElement<int>> late_ch(64);
    Emitter<int> main_em(&main_ch);

    auto fn = std::make_shared<CountWindowFn>();
    detail::TumblingProcessWindowAdapter<int, int, int> op(1000ms, fn);
    OutputTag<int> late_tag("late");
    op.late_output_tag(late_tag);

    auto ctx = attach_late_output_<decltype(op), int>(op, late_ch, "late");
    (void)ctx;

    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), main_em);
    };
    auto wm = [&](std::int64_t ts) {
        op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{ts}}),
                   main_em);
    };

    send(1, 100);
    wm(1500);  // window [0, 1000) fires; bucket erased.
    (void)drain(main_ch);

    send(99, 200);  // late-late: wm=1500 >= end=1000.
    auto main_after = data_records<int>(drain(main_ch));
    EXPECT_TRUE(main_after.empty());
    auto late = data_records<int>(drain(late_ch));
    ASSERT_EQ(late.size(), 1u);
    EXPECT_EQ(late[0].first, 99);
}

// ----- SlidingProcessWindowAdapter ----------------------------------------

TEST(SlidingProcessWindowLateSideOutput, RecordPastAllCoveringWindowsRoutedToSideOutput) {
    BoundedChannel<StreamElement<int>> main_ch(64);
    BoundedChannel<StreamElement<int>> late_ch(64);
    Emitter<int> main_em(&main_ch);

    auto fn = std::make_shared<CountWindowFn>();
    detail::SlidingProcessWindowAdapter<int, int, int> op(1000ms, 500ms, fn);
    OutputTag<int> late_tag("late");
    op.late_output_tag(late_tag);

    auto ctx = attach_late_output_<decltype(op), int>(op, late_ch, "late");
    (void)ctx;

    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), main_em);
    };
    auto wm = [&](std::int64_t ts) {
        op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{ts}}),
                   main_em);
    };

    send(1, 100);
    wm(2000);
    (void)drain(main_ch);

    send(99, 200);  // latest end for ts=200 is 1000; wm=2000 → late-late.
    auto main_after = data_records<int>(drain(main_ch));
    EXPECT_TRUE(main_after.empty());
    auto late = data_records<int>(drain(late_ch));
    ASSERT_EQ(late.size(), 1u);
    EXPECT_EQ(late[0].first, 99);
}

// ----- SessionProcessWindowAdapter ----------------------------------------

TEST(SessionProcessWindowLateSideOutput, RecordPastLatenessRoutedToSideOutput) {
    BoundedChannel<StreamElement<int>> main_ch(64);
    BoundedChannel<StreamElement<int>> late_ch(64);
    Emitter<int> main_em(&main_ch);

    auto fn = std::make_shared<CountWindowFn>();
    detail::SessionProcessWindowAdapter<int, int, int> op(500ms, fn);
    OutputTag<int> late_tag("late");
    op.late_output_tag(late_tag);

    auto ctx = attach_late_output_<decltype(op), int>(op, late_ch, "late");
    (void)ctx;

    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), main_em);
    };
    auto wm = [&](std::int64_t ts) {
        op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{ts}}),
                   main_em);
    };

    send(1, 100);
    wm(3000);  // ts + 2*gap = 1100 << 3000 → late-late threshold.
    (void)drain(main_ch);

    send(99, 200);  // ts=200 + 2*gap=500 = 1200 < wm=3000 → late.
    auto main_after = data_records<int>(drain(main_ch));
    EXPECT_TRUE(main_after.empty());
    auto late = data_records<int>(drain(late_ch));
    ASSERT_EQ(late.size(), 1u);
    EXPECT_EQ(late[0].first, 99);
}
