// allowed_lateness tests for the three ProcessWindowAdapter variants.
//
// Each variant now has the same lateness contract as the basic +
// evicting operators:
//   * Records arriving within (window_end + lateness) re-fire the
//     window with the updated contents.
//   * Records past the band route to the late_output_tag (or fall
//     through to the historic fresh-bucket path when no tag).
//   * lateness=0 (default) preserves single-fire-then-purge.

#include <chrono>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/operators/process_function.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/runtime_context.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

// Emits the count of elements per window invocation.
struct CountWindowFn final : public ProcessWindowFunction<int, int, int> {
    void process(const int&,
                 const WindowContext&,
                 const std::vector<int>& elements,
                 Collector<int>& out) override {
        out.collect(static_cast<int>(elements.size()));
    }
};

template <typename T>
std::vector<T> drain_values(BoundedChannel<StreamElement<T>>& ch) {
    std::vector<T> out;
    while (auto e = ch.try_pop()) {
        if (!e->is_data()) {
            continue;
        }
        for (const auto& r : e->as_data()) {
            out.push_back(r.value());
        }
    }
    return out;
}

}  // namespace

// ----- TumblingProcessWindowAdapter ---------------------------------------

TEST(TumblingProcessWindowAllowedLateness, RecordWithinLatenessReFires) {
    BoundedChannel<StreamElement<int>> ch(64);
    Emitter<int> em(&ch);

    auto fn = std::make_shared<CountWindowFn>();
    detail::TumblingProcessWindowAdapter<int, int, int> op(1000ms, fn);
    op.allowed_lateness(500ms);

    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), em);
    };
    auto wm = [&](std::int64_t ts) {
        op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{ts}}), em);
    };

    send(1, 100);
    send(2, 500);
    wm(1000);  // on-time pane fires with 2 elements.
    auto first = drain_values<int>(ch);
    ASSERT_EQ(first.size(), 1u);
    EXPECT_EQ(first[0], 2);

    // Late record within lateness band: re-fire with 3 elements.
    wm(1200);  // wm advances but still < 1500 purge_at.
    send(3, 200);
    auto refire = drain_values<int>(ch);
    ASSERT_EQ(refire.size(), 1u);
    EXPECT_EQ(refire[0], 3);
}

TEST(TumblingProcessWindowAllowedLateness, RecordPastLatenessRoutesToSideOutput) {
    BoundedChannel<StreamElement<int>> main_ch(64);
    BoundedChannel<StreamElement<int>> late_ch(64);
    Emitter<int> main_em(&main_ch);

    auto fn = std::make_shared<CountWindowFn>();
    detail::TumblingProcessWindowAdapter<int, int, int> op(1000ms, fn);
    op.allowed_lateness(500ms);
    OutputTag<int> late_tag("late");
    op.late_output_tag(late_tag);

    // Wire a RuntimeContext with the late tag channel.
    RuntimeContext ctx(OperatorId{1}, "tumbling_pw", nullptr, nullptr);
    SideOutputChannelMap channels;
    auto late_shared = std::shared_ptr<BoundedChannel<StreamElement<int>>>(&late_ch, [](auto*) {});
    SideOutputChannelEntry entry;
    entry.channel = std::static_pointer_cast<void>(late_shared);
    entry.close_fn = [late_shared] { late_shared->close(); };
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
    wm(2000);  // > purge_at (1500). Window fires, then purges.
    (void)drain_values<int>(main_ch);

    send(99, 200);  // past lateness.
    auto main = drain_values<int>(main_ch);
    EXPECT_TRUE(main.empty()) << "no main emission for past-lateness record";
    auto late = drain_values<int>(late_ch);
    ASSERT_EQ(late.size(), 1u);
    EXPECT_EQ(late[0], 99);
}

TEST(TumblingProcessWindowAllowedLateness, ZeroLatenessPreservesSingleFireBehavior) {
    BoundedChannel<StreamElement<int>> ch(64);
    Emitter<int> em(&ch);

    auto fn = std::make_shared<CountWindowFn>();
    detail::TumblingProcessWindowAdapter<int, int, int> op(1000ms, fn);
    // No allowed_lateness call - default 0.

    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), em);
    };
    auto wm = [&](std::int64_t ts) {
        op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{ts}}), em);
    };

    send(1, 100);
    wm(1000);  // fires + purges immediately (lateness=0 → end+0 <= 1000).
    auto first = drain_values<int>(ch);
    ASSERT_EQ(first.size(), 1u);
    EXPECT_EQ(first[0], 1);

    // Late record creates fresh bucket (historic behavior).
    send(2, 200);
    op.flush(em);
    auto second = drain_values<int>(ch);
    ASSERT_EQ(second.size(), 1u);
    EXPECT_EQ(second[0], 1) << "fresh bucket has only the new record";
}

// ----- SlidingProcessWindowAdapter ----------------------------------------

TEST(SlidingProcessWindowAllowedLateness, RecordWithinLatenessReFiresAllCoveringWindows) {
    BoundedChannel<StreamElement<int>> ch(64);
    Emitter<int> em(&ch);

    auto fn = std::make_shared<CountWindowFn>();
    detail::SlidingProcessWindowAdapter<int, int, int> op(1000ms, 500ms, fn);
    op.allowed_lateness(500ms);

    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), em);
    };
    auto wm = [&](std::int64_t ts) {
        op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{ts}}), em);
    };

    // Record at t=100 → windows [-500, 500) and [0, 1000).
    send(1, 100);
    wm(1000);  // fires [-500, 500) (end=500) and [0, 1000) (end=1000).
    auto first = drain_values<int>(ch);
    EXPECT_EQ(first.size(), 2u);

    // Late record within lateness band - still in window [0, 1000)
    // (purge_at = 1500 > current wm). Re-fire that window.
    wm(1200);
    send(2, 200);
    auto refire = drain_values<int>(ch);
    EXPECT_FALSE(refire.empty());
}

// ----- SessionProcessWindowAdapter ----------------------------------------

TEST(SessionProcessWindowAllowedLateness, RecordWithinLatenessReFiresSession) {
    BoundedChannel<StreamElement<int>> ch(64);
    Emitter<int> em(&ch);

    auto fn = std::make_shared<CountWindowFn>();
    detail::SessionProcessWindowAdapter<int, int, int> op(500ms, fn);
    op.allowed_lateness(500ms);

    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), em);
    };
    auto wm = [&](std::int64_t ts) {
        op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{ts}}), em);
    };

    send(1, 100);  // session at [100, 101), gap=500.
    wm(700);       // end (101) + gap (500) = 601 <= 700. Fires.
    auto first = drain_values<int>(ch);
    ASSERT_EQ(first.size(), 1u);
    EXPECT_EQ(first[0], 1);

    // Late record within lateness band: session purge_at = 601 + 500 = 1101.
    // wm=700 is still within. Record at t=200 overlaps the fired
    // session (200 within session [100, 101+gap=601]) → merge,
    // re-fire.
    send(2, 200);
    auto refire = drain_values<int>(ch);
    ASSERT_EQ(refire.size(), 1u);
    EXPECT_EQ(refire[0], 2) << "re-fire emits merged session count";
}

TEST(SessionProcessWindowAllowedLateness, RecordPastLatenessRoutesToSideOutput) {
    BoundedChannel<StreamElement<int>> main_ch(64);
    BoundedChannel<StreamElement<int>> late_ch(64);
    Emitter<int> main_em(&main_ch);

    auto fn = std::make_shared<CountWindowFn>();
    detail::SessionProcessWindowAdapter<int, int, int> op(500ms, fn);
    op.allowed_lateness(100ms);
    OutputTag<int> late_tag("late");
    op.late_output_tag(late_tag);

    RuntimeContext ctx(OperatorId{1}, "session_pw", nullptr, nullptr);
    SideOutputChannelMap channels;
    auto late_shared = std::shared_ptr<BoundedChannel<StreamElement<int>>>(&late_ch, [](auto*) {});
    SideOutputChannelEntry entry;
    entry.channel = std::static_pointer_cast<void>(late_shared);
    entry.close_fn = [late_shared] { late_shared->close(); };
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
    wm(3000);  // ts(200) + 2*gap(500) + lateness(100) = 1300; wm=3000 → late.
    (void)drain_values<int>(main_ch);

    send(99, 200);
    auto main = drain_values<int>(main_ch);
    EXPECT_TRUE(main.empty());
    auto late = drain_values<int>(late_ch);
    ASSERT_EQ(late.size(), 1u);
    EXPECT_EQ(late[0], 99);
}
