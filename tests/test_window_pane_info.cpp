// Tests for the PaneInfo metadata Window operators attach to each
// emission. Pin semantics:
//   * Default EventTimeTrigger fire = OnTime.
//   * Late-arrival re-fire under allowed_lateness = Late.
//   * Pre-watermark fire (CountTrigger, ProcessingTimeTrigger,
//     custom triggers) = Early.
//   * pane_index increments per emission for the same window.
//   * is_first true on first emission, false thereafter.
//   * is_last true when the emission is followed by a purge.

#include <chrono>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/operators/sliding_window_operator.hpp"
#include "clink/operators/tumbling_window_operator.hpp"
#include "clink/operators/window_trigger.hpp"
#include "clink/runtime/bounded_channel.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

template <typename T>
std::vector<StreamElement<T>> drain(BoundedChannel<StreamElement<T>>& ch) {
    std::vector<StreamElement<T>> out;
    while (auto e = ch.try_pop()) {
        out.push_back(std::move(*e));
    }
    return out;
}

template <typename T>
std::vector<Record<T>> collect_records(const std::vector<StreamElement<T>>& es) {
    std::vector<Record<T>> out;
    for (const auto& e : es) {
        if (!e.is_data()) {
            continue;
        }
        for (const auto& r : e.as_data()) {
            out.push_back(r);
        }
    }
    return out;
}

}  // namespace

TEST(WindowPaneInfo, EventTimeTriggerProducesOnTimePane) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    TumblingWindowOperator<int, int, int> op(
        1s, [] { return 0; }, [](int a, int v) { return a + v; });

    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), em);
    };
    send(3, 100);
    send(4, 500);
    op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{1'000}}), em);

    auto records = collect_records(drain(ch));
    ASSERT_EQ(records.size(), 1u);
    ASSERT_TRUE(records[0].pane().has_value());
    EXPECT_EQ(records[0].pane()->timing, PaneInfo::Timing::OnTime);
    EXPECT_EQ(records[0].pane()->pane_index, 0);
    EXPECT_TRUE(records[0].pane()->is_first);
    // Default lateness = 0 means the on-time fire also purges, so this
    // is the last pane for this window.
    EXPECT_TRUE(records[0].pane()->is_last);
}

TEST(WindowPaneInfo, AllowedLatenessProducesLatePane) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    TumblingWindowOperator<int, int, int> op(
        1000ms, [] { return 0; }, [](int a, int v) { return a + v; });
    op.allowed_lateness(500ms);

    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), em);
    };

    send(1, 100);
    op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{1'000}}), em);
    auto first = collect_records(drain(ch));
    ASSERT_EQ(first.size(), 1u);
    EXPECT_EQ(first[0].pane()->timing, PaneInfo::Timing::OnTime);
    EXPECT_EQ(first[0].pane()->pane_index, 0);
    EXPECT_TRUE(first[0].pane()->is_first);
    EXPECT_FALSE(first[0].pane()->is_last);  // window retained for lateness

    send(10, 200);  // late
    auto refire = collect_records(drain(ch));
    ASSERT_EQ(refire.size(), 1u);
    EXPECT_EQ(refire[0].pane()->timing, PaneInfo::Timing::Late);
    EXPECT_EQ(refire[0].pane()->pane_index, 1);
    EXPECT_FALSE(refire[0].pane()->is_first);
}

TEST(WindowPaneInfo, CountTriggerProducesEarlyPane) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    TumblingWindowOperator<int, int, int> op(
        10s, [] { return 0; }, [](int a, int v) { return a + v; });
    op.with_trigger(std::make_unique<CountTrigger<int>>(2));

    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), em);
    };
    send(3, 100);
    send(7, 200);

    auto records = collect_records(drain(ch));
    ASSERT_EQ(records.size(), 1u);
    // Watermark is still at min while data is being sent; the trigger
    // fired before watermark crossed window.end -> Early.
    EXPECT_EQ(records[0].pane()->timing, PaneInfo::Timing::Early);
    EXPECT_TRUE(records[0].pane()->is_first);
    // CountTrigger returns FireAndPurge, so this is also the last pane.
    EXPECT_TRUE(records[0].pane()->is_last);
}

TEST(WindowPaneInfo, PaneIndexIncrementsAcrossLatePanes) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    TumblingWindowOperator<int, int, int> op(
        1000ms, [] { return 0; }, [](int a, int v) { return a + v; });
    op.allowed_lateness(2000ms);

    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), em);
    };

    send(1, 100);
    op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{1'000}}), em);
    send(2, 200);  // late pane 1
    send(3, 300);  // late pane 2

    auto records = collect_records(drain(ch));
    ASSERT_EQ(records.size(), 3u);
    EXPECT_EQ(records[0].pane()->pane_index, 0);
    EXPECT_EQ(records[1].pane()->pane_index, 1);
    EXPECT_EQ(records[2].pane()->pane_index, 2);
    EXPECT_TRUE(records[0].pane()->is_first);
    EXPECT_FALSE(records[1].pane()->is_first);
    EXPECT_FALSE(records[2].pane()->is_first);
}

TEST(WindowPaneInfo, SlidingWindowsAlsoPopulatePaneInfo) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    SlidingWindowOperator<int, int, int> op(
        1000ms, 500ms, [] { return 0; }, [](int a, int v) { return a + v; });

    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), em);
    };
    send(7, 250);
    op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{1'500}}), em);

    auto records = collect_records(drain(ch));
    // Two windows fire (the two windows containing ts=250 - starts at
    // 0 and -500, ends at 1000 and 500 respectively).
    ASSERT_GE(records.size(), 1u);
    for (const auto& r : records) {
        ASSERT_TRUE(r.pane().has_value());
        EXPECT_EQ(r.pane()->timing, PaneInfo::Timing::OnTime);
        EXPECT_EQ(r.pane()->pane_index, 0);
        EXPECT_TRUE(r.pane()->is_first);
    }
}
