// Tests for the pluggable Trigger framework on tumbling and sliding
// window operators. Demonstrates that:
//   * The default EventTimeTrigger preserves the engine's pre-trigger
//     fire-on-watermark behavior.
//   * CountTrigger fires after N records regardless of watermark.
//   * ProcessingTimeTrigger fires when wall-clock time crosses the
//     window's end (we drive this deterministically by checking
//     immediately after a sleep past window.end).
//   * Custom user triggers compose with allowed_lateness and the
//     normal aggregate-state machinery.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <thread>
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

template <typename Key, typename Agg>
std::vector<std::tuple<Key, Agg, std::int64_t>> collect_data(
    const std::vector<StreamElement<std::pair<Key, Agg>>>& es) {
    std::vector<std::tuple<Key, Agg, std::int64_t>> out;
    for (const auto& e : es) {
        if (!e.is_data()) {
            continue;
        }
        for (const auto& r : e.as_data()) {
            const auto ts = r.event_time().has_value() ? r.event_time()->millis() : 0;
            out.emplace_back(r.value().first, r.value().second, ts);
        }
    }
    return out;
}

}  // namespace

// ----- CountTrigger -----

TEST(WindowTriggers, CountTriggerFiresEveryNRecords) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    TumblingWindowOperator<int, int, int> op(
        10s, [] { return 0; }, [](int a, int v) { return a + v; });
    op.with_trigger(std::make_unique<CountTrigger<int>>(3));

    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), em);
    };

    // Three records at the same window - fires on the third regardless
    // of watermark. CountTrigger returns FireAndPurge so the operator
    // emits and clears state.
    send(1, 100);
    send(2, 200);
    send(3, 300);
    auto first = collect_data<int, int>(drain(ch));
    ASSERT_EQ(first.size(), 1u);
    EXPECT_EQ(std::get<1>(first[0]), 6);

    // Three more records - fires again with a fresh sum.
    send(10, 400);
    send(20, 500);
    send(30, 600);
    auto second = collect_data<int, int>(drain(ch));
    ASSERT_EQ(second.size(), 1u);
    EXPECT_EQ(std::get<1>(second[0]), 60);

    // Two records - not enough to fire. Flush at end shouldn't pick
    // them up either, since CountTrigger purged the previous state and
    // EventTimeTrigger isn't in play; the window state lingers but the
    // operator's fire-on-watermark uses the (replaced) trigger's
    // on_event_time, which is Continue for CountTrigger.
    send(7, 700);
    send(7, 800);
    op.flush(em);
    auto trailing = collect_data<int, int>(drain(ch));
    EXPECT_TRUE(trailing.empty());
}

TEST(WindowTriggers, CountTriggerOnSlidingWindowFiresPerWindow) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    // size=10s, slide=5s - each record contributes to 2 windows.
    SlidingWindowOperator<int, int, int> op(
        10s, 5s, [] { return 0; }, [](int a, int v) { return a + v; });
    op.with_trigger(std::make_unique<CountTrigger<int>>(2));

    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), em);
    };

    // Records at ts=1s and ts=2s both land in windows starting at -5s
    // and 0s. After the second record arrives, both windows hit 2
    // elements and fire.
    send(5, 1'000);
    send(7, 2'000);

    auto data = collect_data<int, int>(drain(ch));
    // Expect 2 emissions (one per window), each with sum=12.
    ASSERT_EQ(data.size(), 2u);
    for (const auto& [k, agg, ts] : data) {
        EXPECT_EQ(agg, 12);
    }
}

// ----- ProcessingTimeTrigger -----

TEST(WindowTriggers, ProcessingTimeTriggerFiresAfterWallClockCrossing) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    // Tiny window in event-time terms - but the trigger is processing
    // time, so event-time semantics don't matter.
    TumblingWindowOperator<int, int, int> op(
        50ms, [] { return 0; }, [](int a, int v) { return a + v; });
    op.with_trigger(std::make_unique<ProcessingTimeTrigger<int>>());

    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), em);
    };

    // First record establishes window [0, 50ms). The trigger checks
    // current_processing_time against window.end - wall-clock is way
    // past 50ms (it's the Unix epoch in millis), so the trigger
    // returns FireAndPurge on the first on_element call.
    send(7, 0);
    auto data = collect_data<int, int>(drain(ch));
    ASSERT_EQ(data.size(), 1u);
    EXPECT_EQ(std::get<1>(data[0]), 7);
}

// ----- Custom user trigger -----

namespace {

// A trigger that fires whenever a "marker" record arrives - used for
// content-driven flushing (e.g., punctuation tokens).
class MarkerTrigger final : public Trigger<int, TimeWindow> {
public:
    explicit MarkerTrigger(int marker_value) : marker_(marker_value) {}

    TriggerResult on_element(const int& value,
                             std::int64_t,
                             const TimeWindow&,
                             TriggerContext<TimeWindow>&) override {
        return value == marker_ ? TriggerResult::FireAndPurge : TriggerResult::Continue;
    }
    TriggerResult on_event_time(std::int64_t,
                                const TimeWindow&,
                                TriggerContext<TimeWindow>&) override {
        return TriggerResult::Continue;
    }

private:
    int marker_;
};

}  // namespace

TEST(WindowTriggers, CustomTriggerFiresOnMarkerRecord) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    TumblingWindowOperator<int, int, int> op(
        10s, [] { return 0; }, [](int a, int v) { return a + v; });
    op.with_trigger(std::make_unique<MarkerTrigger>(/*marker*/ -1));

    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), em);
    };

    send(5, 100);
    send(7, 200);
    send(-1, 300);  // marker - fires. agg includes the marker (−1).
    auto data = collect_data<int, int>(drain(ch));
    ASSERT_EQ(data.size(), 1u);
    EXPECT_EQ(std::get<1>(data[0]), 11);  // 5 + 7 + (-1)

    // After purge, next batch starts fresh.
    send(100, 400);
    send(-1, 500);
    auto data2 = collect_data<int, int>(drain(ch));
    ASSERT_EQ(data2.size(), 1u);
    EXPECT_EQ(std::get<1>(data2[0]), 99);  // 100 + (-1)
}

// ----- Default behaviour preserved -----

TEST(WindowTriggers, DefaultTriggerStillFiresOnWatermark) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    TumblingWindowOperator<int, int, int> op(
        1s, [] { return 0; }, [](int a, int v) { return a + v; });
    // No with_trigger() call - defaults to EventTimeTrigger.

    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), em);
    };

    send(3, 100);
    send(4, 500);
    op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{1'000}}), em);
    auto data = collect_data<int, int>(drain(ch));
    ASSERT_EQ(data.size(), 1u);
    EXPECT_EQ(std::get<1>(data[0]), 7);
}
