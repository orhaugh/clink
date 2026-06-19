// Tests for SessionWindowOperator. Mirrors
// EventTimeSessionWindows behavior: dynamic windows that grow as
// records arrive and merge when their gap-bounded windows overlap.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/operators/session_window_operator.hpp"
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

template <typename Key, typename Value, typename Agg>
void send(SessionWindowOperator<Key, Value, Agg>& op,
          Emitter<std::pair<Key, Agg>>& em,
          const Key& k,
          const Value& v,
          std::int64_t ts) {
    Batch<std::pair<Key, Value>> b;
    b.emplace(std::pair<Key, Value>{k, v}, EventTime{ts});
    op.process(StreamElement<std::pair<Key, Value>>::data(std::move(b)), em);
}

auto sum_initial = []() { return 0; };
auto sum_combine = [](int agg, int v) { return agg + v; };
auto sum_merge = [](int a, int b) { return a + b; };

}  // namespace

TEST(SessionWindowOperator, ValidatesGapPositive) {
    using Op = SessionWindowOperator<int, int, int>;
    EXPECT_THROW(Op(0ms, sum_initial, sum_combine, sum_merge), std::invalid_argument);
}

TEST(SessionWindowOperator, SingleRecordFiresAtWatermarkPastGap) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    SessionWindowOperator<int, int, int> op(100ms, sum_initial, sum_combine, sum_merge);

    send(op, em, 1, 7, 200);

    // session is [200, 300]. Watermark 250 - too early.
    op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{250}}), em);
    {
        auto early = collect_data<int, int>(drain(ch));
        EXPECT_TRUE(early.empty());
    }

    // Watermark 300 - exactly at session end (>= fires per ).
    op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{300}}), em);
    auto data = collect_data<int, int>(drain(ch));
    ASSERT_EQ(data.size(), 1u);
    EXPECT_EQ(std::get<0>(data[0]), 1);
    EXPECT_EQ(std::get<1>(data[0]), 7);
    // Emit time is session_end - 1 = 299.
    EXPECT_EQ(std::get<2>(data[0]), 299);
}

TEST(SessionWindowOperator, RecordsWithinGapMergeIntoOneSession) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    SessionWindowOperator<int, int, int> op(100ms, sum_initial, sum_combine, sum_merge);

    // Three records 50ms apart, gap = 100ms, so they merge: session
    // [100, 200+100] = [100, 300]. Sum = 1+2+3 = 6.
    send(op, em, 1, 1, 100);
    send(op, em, 1, 2, 150);
    send(op, em, 1, 3, 200);

    op.flush(em);
    auto data = collect_data<int, int>(drain(ch));
    ASSERT_EQ(data.size(), 1u);
    EXPECT_EQ(std::get<1>(data[0]), 6);
    EXPECT_EQ(std::get<2>(data[0]), 299);  // session_end (300) - 1
}

TEST(SessionWindowOperator, RecordsBeyondGapStartNewSession) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    SessionWindowOperator<int, int, int> op(100ms, sum_initial, sum_combine, sum_merge);

    // Record at 100 -> session [100, 200].
    // Record at 350 -> 350 - 200 = 150 > gap=100, so a new session
    // [350, 450]. Two distinct sessions.
    send(op, em, 1, 5, 100);
    send(op, em, 1, 7, 350);

    op.flush(em);
    auto data = collect_data<int, int>(drain(ch));
    ASSERT_EQ(data.size(), 2u);
    EXPECT_EQ(std::get<1>(data[0]), 5);
    EXPECT_EQ(std::get<2>(data[0]), 199);  // session_end (200) - 1
    EXPECT_EQ(std::get<1>(data[1]), 7);
    EXPECT_EQ(std::get<2>(data[1]), 449);  // session_end (450) - 1
}

TEST(SessionWindowOperator, OutOfOrderRecordTriggersBackwardMerge) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    SessionWindowOperator<int, int, int> op(100ms, sum_initial, sum_combine, sum_merge);

    // First two records create sessions A=[100, 200] and B=[400, 500].
    // Out-of-order record at 180 has provisional [180, 280]:
    //   vs A=[100,200]: 100 <= 280 && 200 >= 180  -> intersects, merge.
    //   vs B=[400,500]: 400 <= 280? no            -> no merge.
    // After merge: A becomes [100, 280], sum = 1 + 9 = 10. B unchanged.
    send(op, em, 1, 1, 100);  // session A
    send(op, em, 1, 5, 400);  // session B
    send(op, em, 1, 9, 180);  // merges with A only

    op.flush(em);
    auto data = collect_data<int, int>(drain(ch));
    ASSERT_EQ(data.size(), 2u);
    bool saw_merged_a = false;
    bool saw_b = false;
    for (const auto& [k, agg, ts] : data) {
        if (agg == 10 && ts == 279)
            saw_merged_a = true;
        if (agg == 5 && ts == 499)
            saw_b = true;
    }
    EXPECT_TRUE(saw_merged_a);
    EXPECT_TRUE(saw_b);
}

TEST(SessionWindowOperator, OutOfOrderRecordMergesTwoExistingSessions) {
    // gap=200; sessions A=[100,300] and B=[500,700] don't overlap. A
    // late record at ts=300 has provisional window [300,500], which
    // overlaps both A (start<=500 && end>=300) and B (start<=500 &&
    // end>=300). All three should collapse into one merged session.
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    SessionWindowOperator<int, int, int> op(200ms, sum_initial, sum_combine, sum_merge);

    send(op, em, 1, 10, 100);  // A
    send(op, em, 1, 20, 500);  // B
    send(op, em, 1, 30, 300);  // bridge

    op.flush(em);
    auto data = collect_data<int, int>(drain(ch));
    ASSERT_EQ(data.size(), 1u);
    EXPECT_EQ(std::get<1>(data[0]), 60);   // 10 + 20 + 30
    EXPECT_EQ(std::get<2>(data[0]), 699);  // session_end (700) - 1
}

TEST(SessionWindowOperator, KeysAreIsolated) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    SessionWindowOperator<int, int, int> op(100ms, sum_initial, sum_combine, sum_merge);

    send(op, em, 1, 5, 100);
    send(op, em, 2, 7, 100);
    // Both keys have session [100, 200], no merge across keys.
    op.flush(em);
    auto data = collect_data<int, int>(drain(ch));
    ASSERT_EQ(data.size(), 2u);
    EXPECT_EQ(std::get<0>(data[0]), 1);
    EXPECT_EQ(std::get<1>(data[0]), 5);
    EXPECT_EQ(std::get<0>(data[1]), 2);
    EXPECT_EQ(std::get<1>(data[1]), 7);
}

TEST(SessionWindowOperator, ForwardsWatermarkAfterFiring) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    SessionWindowOperator<int, int, int> op(100ms, sum_initial, sum_combine, sum_merge);

    send(op, em, 1, 5, 100);
    op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{300}}), em);

    auto elements = drain(ch);
    bool saw_data = false;
    bool saw_wm = false;
    for (const auto& e : elements) {
        if (e.is_data())
            saw_data = true;
        if (e.is_watermark()) {
            saw_wm = true;
            EXPECT_EQ(e.as_watermark().timestamp().millis(), 300);
        }
    }
    EXPECT_TRUE(saw_data);
    EXPECT_TRUE(saw_wm);
}

TEST(SessionWindowOperator, ForwardsBarrier) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    SessionWindowOperator<int, int, int> op(100ms, sum_initial, sum_combine, sum_merge);

    op.process(StreamElement<std::pair<int, int>>::barrier(CheckpointBarrier{CheckpointId{42}}),
               em);

    auto elements = drain(ch);
    ASSERT_EQ(elements.size(), 1u);
    ASSERT_TRUE(elements[0].is_barrier());
    EXPECT_EQ(elements[0].as_barrier().id(), CheckpointId{42});
}
