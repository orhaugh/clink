// Tests for EvictingTumblingWindowOperator + Evictor implementations.
// Mirrors WindowedStream.evictor() integration:
//   * The operator buffers raw records per window.
//   * On Trigger Fire, the evictor pre-filters the buffer; the
//     ProcessFn sees the filtered records and produces an Out.
//
// CountEvictor: keep most-recently-arrived N records.
// TimeEvictor:  keep records within `max_age` of the latest event time.

#include <chrono>
#include <cstdint>
#include <memory>
#include <numeric>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/operators/evicting_tumbling_window_operator.hpp"
#include "clink/operators/window_evictor.hpp"
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

template <typename Key, typename Out>
std::vector<std::tuple<Key, Out, std::int64_t>> collect_data(
    const std::vector<StreamElement<std::pair<Key, Out>>>& es) {
    std::vector<std::tuple<Key, Out, std::int64_t>> out;
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

// Sum the values of all records still present after eviction.
auto sum_records = [](const std::vector<Record<int>>& records, const TimeWindow&) -> int {
    int s = 0;
    for (const auto& r : records) {
        s += r.value();
    }
    return s;
};

}  // namespace

TEST(WindowEvictors, CountEvictorKeepsMostRecentRecords) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    // Window of 1s; CountEvictor keeps last 3 records.
    EvictingTumblingWindowOperator<int, int, int> op(
        1s, sum_records, std::make_unique<CountEvictor<int>>(3));

    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), em);
    };

    // Five records all in window [0, 1000): 1, 2, 3, 4, 5. Evictor keeps
    // last three: {3, 4, 5}. Sum = 12.
    send(1, 100);
    send(2, 200);
    send(3, 300);
    send(4, 400);
    send(5, 500);
    op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{1'000}}), em);

    auto data = collect_data<int, int>(drain(ch));
    ASSERT_EQ(data.size(), 1u);
    EXPECT_EQ(std::get<1>(data[0]), 12);
}

TEST(WindowEvictors, TimeEvictorKeepsOnlyRecordsWithinMaxAge) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    // Window of 1s; TimeEvictor keeps records within 200ms of the
    // latest record's event-time.
    EvictingTumblingWindowOperator<int, int, int> op(
        1s, sum_records, std::make_unique<TimeEvictor<int>>(200ms));

    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), em);
    };

    // ts = 100, 300, 500, 800. Latest = 800; threshold = 800 - 200 = 600.
    // Keep records with ts >= 600: only ts=800. Sum = 4.
    send(1, 100);
    send(2, 300);
    send(3, 500);
    send(4, 800);
    op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{1'000}}), em);

    auto data = collect_data<int, int>(drain(ch));
    ASSERT_EQ(data.size(), 1u);
    EXPECT_EQ(std::get<1>(data[0]), 4);
}

TEST(WindowEvictors, EmittedRecordCarriesPaneInfo) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    EvictingTumblingWindowOperator<int, int, int> op(
        1s, sum_records, std::make_unique<CountEvictor<int>>(2));

    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), em);
    };
    send(1, 100);
    send(2, 200);
    op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{1'000}}), em);

    auto elements = drain(ch);
    bool saw_record = false;
    for (const auto& e : elements) {
        if (!e.is_data()) {
            continue;
        }
        for (const auto& r : e.as_data()) {
            saw_record = true;
            ASSERT_TRUE(r.pane().has_value());
            EXPECT_EQ(r.pane()->timing, PaneInfo::Timing::OnTime);
            EXPECT_EQ(r.pane()->pane_index, 0);
            EXPECT_TRUE(r.pane()->is_first);
            EXPECT_TRUE(r.pane()->is_last);
        }
    }
    EXPECT_TRUE(saw_record);
}

TEST(WindowEvictors, CountEvictorComposedWithCountTrigger) {
    // Trigger fires every 2 records. Evictor keeps the most-recent 1
    // record. So each fire emits the value of the most-recent record.
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    EvictingTumblingWindowOperator<int, int, int> op(
        100s, sum_records, std::make_unique<CountEvictor<int>>(1));
    op.with_trigger(std::make_unique<CountTrigger<int>>(2));

    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), em);
    };
    send(10, 100);
    send(20, 200);  // 2nd record fires the trigger
    auto first = collect_data<int, int>(drain(ch));
    ASSERT_EQ(first.size(), 1u);
    EXPECT_EQ(std::get<1>(first[0]), 20);  // evictor kept only the latest
}

TEST(WindowEvictors, RejectsNullEvictorAndTrigger) {
    using Op = EvictingTumblingWindowOperator<int, int, int>;
    EXPECT_THROW(Op(1s, sum_records, nullptr), std::invalid_argument);

    Op op(1s, sum_records, std::make_unique<CountEvictor<int>>(3));
    EXPECT_THROW(op.with_trigger(nullptr), std::invalid_argument);
}
