// Unit tests for SlidingWindowOperator. Mirrors the structure of
// test_tumbling_window_operator.cpp; the realistic 1h/1m scenario lives
// in test_sliding_window_scenario.cpp.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/operators/sliding_window_operator.hpp"
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
    std::sort(out.begin(), out.end());
    return out;
}

}  // namespace

TEST(SlidingWindowOperator, ValidatesSizeIsMultipleOfSlide) {
    const auto bad = [] {
        SlidingWindowOperator<int, int, int> op(
            300ms, 200ms, [] { return 0; }, [](int a, int b) { return a + b; });
        (void)op;
    };
    EXPECT_THROW(bad(), std::invalid_argument);
}

TEST(SlidingWindowOperator, ValidatesSlideAndSizePositive) {
    using Op = SlidingWindowOperator<int, int, int>;
    EXPECT_THROW(Op(
                     100ms, 0ms, [] { return 0; }, [](int a, int b) { return a + b; }),
                 std::invalid_argument);
    EXPECT_THROW(Op(
                     0ms, 100ms, [] { return 0; }, [](int a, int b) { return a + b; }),
                 std::invalid_argument);
}

TEST(SlidingWindowOperator, AssignsRecordToAllOverlappingWindows) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    // size=1000, slide=200 -> a record at ts=900 should land in 5
    // windows: [0,1000), [200,1200), [400,1400), [600,1600), [800,1800).
    SlidingWindowOperator<int, int, int> op(
        1000ms, 200ms, [] { return 0; }, [](int agg, int v) { return agg + v; });

    Batch<std::pair<int, int>> b;
    b.emplace(std::pair<int, int>{1, 7}, EventTime{900});
    op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), em);

    // No watermark yet - nothing fires. flush() at end fires every
    // window that received the record. Expect 5 emissions, each with
    // sum = 7 for key 1.
    op.flush(em);

    auto data = collect_data<int, int>(drain(ch));
    ASSERT_EQ(data.size(), 5u);
    for (const auto& [k, agg, ts] : data) {
        EXPECT_EQ(k, 1);
        EXPECT_EQ(agg, 7);
    }
    // window_end timestamps reported are window_end - 1, evenly spaced
    // by `slide`: 999, 1199, 1399, 1599, 1799.
    std::vector<std::int64_t> ends;
    ends.reserve(data.size());
    for (const auto& d : data) {
        ends.push_back(std::get<2>(d));
    }
    EXPECT_EQ(ends, (std::vector<std::int64_t>{999, 1199, 1399, 1599, 1799}));
}

TEST(SlidingWindowOperator, FiresEachWindowAsWatermarkAdvances) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    SlidingWindowOperator<int, int, int> op(
        1000ms, 200ms, [] { return 0; }, [](int agg, int v) { return agg + v; });

    // Three records at increasing ts so several windows accumulate.
    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), em);
    };
    send(1, 100);
    send(2, 300);
    send(3, 700);

    // Sliding windows are half-open [start, start+size); a window fires
    // when the watermark reaches start+size. Since size > slide, every
    // record contributes to multiple overlapping windows including some
    // with negative start times. With size=1000, slide=200:
    //   record 1@100 contributes to starts {-800, -600, -400, -200, 0}
    //   record 2@300 contributes to starts {-600, -400, -200,    0, 200}
    //   record 3@700 contributes to starts {-200,    0,  200,  400, 600}
    // Union of unique starts -> 8 windows total.

    // Watermark at 200 - fires only end=200 (start=-800), sum=1.
    op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{200}}), em);
    auto data0 = collect_data<int, int>(drain(ch));
    ASSERT_EQ(data0.size(), 1u);
    EXPECT_EQ(std::get<1>(data0[0]), 1);
    EXPECT_EQ(std::get<2>(data0[0]) + 1, 200);

    // Watermark at 1000 - newly fires ends in (200, 1000]: 400, 600, 800, 1000.
    //   end=400  (start=-600): {r1, r2}        => 3
    //   end=600  (start=-400): {r1, r2}        => 3
    //   end=800  (start=-200): {r1, r2, r3}    => 6
    //   end=1000 (start=0):    {r1, r2, r3}    => 6
    op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{1000}}), em);
    auto data1 = collect_data<int, int>(drain(ch));
    ASSERT_EQ(data1.size(), 4u);
    std::vector<std::pair<std::int64_t, int>> by_end1;
    by_end1.reserve(data1.size());
    for (const auto& d : data1) {
        by_end1.emplace_back(std::get<2>(d) + 1, std::get<1>(d));
    }
    std::sort(by_end1.begin(), by_end1.end());
    EXPECT_EQ(by_end1,
              (std::vector<std::pair<std::int64_t, int>>{{400, 3}, {600, 3}, {800, 6}, {1000, 6}}));

    // Watermark at 1400 - newly fires ends in (1000, 1400]: 1200, 1400.
    //   end=1200 (start=200): {r2, r3} => 5
    //   end=1400 (start=400): {r3}     => 3
    op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{1400}}), em);
    auto data2 = collect_data<int, int>(drain(ch));
    ASSERT_EQ(data2.size(), 2u);
    std::vector<std::pair<std::int64_t, int>> by_end2;
    by_end2.reserve(data2.size());
    for (const auto& d : data2) {
        by_end2.emplace_back(std::get<2>(d) + 1, std::get<1>(d));
    }
    std::sort(by_end2.begin(), by_end2.end());
    EXPECT_EQ(by_end2, (std::vector<std::pair<std::int64_t, int>>{{1200, 5}, {1400, 3}}));

    // Final flush - the remaining ends-1600 windows fire too:
    //   end=1600 (start=600): {r3} => 3
    // Note: only one window ends at 1600 in this scenario (r3's last
    // contribution: starts 600), so flush emits exactly 1.
    op.flush(em);
    auto data3 = collect_data<int, int>(drain(ch));
    ASSERT_EQ(data3.size(), 1u);
    EXPECT_EQ(std::get<1>(data3[0]), 3);
    EXPECT_EQ(std::get<2>(data3[0]) + 1, 1600);
}

TEST(SlidingWindowOperator, KeyedAggregationStaysIsolated) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    SlidingWindowOperator<int, int, int> op(
        500ms, 100ms, [] { return 0; }, [](int agg, int v) { return agg + v; });

    auto send = [&](int key, int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{key, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), em);
    };
    send(1, 10, 50);
    send(2, 20, 50);

    op.flush(em);

    auto data = collect_data<int, int>(drain(ch));
    // Each key produced 5 windows (size/slide = 5). 5 + 5 = 10 emissions.
    ASSERT_EQ(data.size(), 10u);
    int sum_k1 = 0;
    int sum_k2 = 0;
    for (const auto& [k, agg, ts] : data) {
        if (k == 1)
            sum_k1 += agg;
        if (k == 2)
            sum_k2 += agg;
    }
    // 5 windows * 10 for key 1, 5 * 20 for key 2.
    EXPECT_EQ(sum_k1, 50);
    EXPECT_EQ(sum_k2, 100);
}

TEST(SlidingWindowOperator, ForwardsWatermarkAndBarrier) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    SlidingWindowOperator<int, int, int> op(
        1000ms, 500ms, [] { return 0; }, [](int agg, int v) { return agg + v; });

    op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{1234}}), em);
    op.process(StreamElement<std::pair<int, int>>::barrier(CheckpointBarrier{CheckpointId{42}}),
               em);

    auto elements = drain(ch);
    bool saw_wm = false;
    bool saw_barrier = false;
    for (const auto& e : elements) {
        if (e.is_watermark()) {
            saw_wm = true;
            EXPECT_EQ(e.as_watermark().timestamp().millis(), 1234);
        }
        if (e.is_barrier()) {
            saw_barrier = true;
            EXPECT_EQ(e.as_barrier().id(), CheckpointId{42});
        }
    }
    EXPECT_TRUE(saw_wm);
    EXPECT_TRUE(saw_barrier);
}
