// Tests for allowed_lateness across the three window operator types.
// Mirrors WindowedStream.allowedLateness contract:
//   * After watermark crosses window_end, the window emits the on-time
//     pane and is marked as fired.
//   * Records arriving while watermark is in
//     [window_end, window_end + allowed_lateness] update the aggregate
//     and immediately re-emit (late pane).
//   * Records arriving after watermark passes window_end +
//     allowed_lateness are dropped silently.
//
// The default of allowed_lateness=0 keeps the previous fire-and-purge
// behavior, so existing tests in the per-operator suites are unaffected.

#include <chrono>
#include <cstdint>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/operators/session_window_operator.hpp"
#include "clink/operators/sliding_window_operator.hpp"
#include "clink/operators/tumbling_window_operator.hpp"
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

// ----- Tumbling -----

TEST(TumblingWindowAllowedLateness, LateRecordWithinDeadlineRefires) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    TumblingWindowOperator<int, int, int> op(
        1000ms, [] { return 0; }, [](int a, int v) { return a + v; });
    op.allowed_lateness(500ms);

    // Two records in window [0, 1000): sum = 3.
    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), em);
    };
    send(1, 100);
    send(2, 500);

    // Watermark at 1000 - on-time pane fires (sum=3, ts=999).
    op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{1000}}), em);
    auto first = collect_data<int, int>(drain(ch));
    ASSERT_EQ(first.size(), 1u);
    EXPECT_EQ(std::get<1>(first[0]), 3);
    EXPECT_EQ(std::get<2>(first[0]), 999);

    // Late record arriving in the lateness window: ts=200 in [0,1000),
    // wm currently 1000 < window_end + lateness (1500). Should re-fire
    // with sum=3+10=13.
    send(10, 200);
    auto refire = collect_data<int, int>(drain(ch));
    ASSERT_EQ(refire.size(), 1u);
    EXPECT_EQ(std::get<1>(refire[0]), 13);
    EXPECT_EQ(std::get<2>(refire[0]), 999);

    // Watermark at 1500 = purge boundary. Window state cleared.
    op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{1500}}), em);
    (void)drain(ch);

    // Record after purge - creates a fresh bucket, no re-fire of the
    // old one.
    send(99, 200);
    op.flush(em);
    auto after_purge = collect_data<int, int>(drain(ch));
    ASSERT_EQ(after_purge.size(), 1u);
    EXPECT_EQ(std::get<1>(after_purge[0]), 99);
}

TEST(TumblingWindowAllowedLateness, RecordPastDeadlineCreatesFreshBucket) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    TumblingWindowOperator<int, int, int> op(
        1000ms, [] { return 0; }, [](int a, int v) { return a + v; });
    op.allowed_lateness(100ms);

    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), em);
    };

    send(1, 100);
    op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{1100}}), em);
    // wm 1100 >= window_end+lateness (1100); purge happens here.
    auto first = collect_data<int, int>(drain(ch));
    ASSERT_EQ(first.size(), 1u);
    EXPECT_EQ(std::get<1>(first[0]), 1);

    // Record arriving after purge - fresh bucket, sum starts from
    // initial(). The flush() emits it.
    send(50, 100);
    op.flush(em);
    auto after = collect_data<int, int>(drain(ch));
    ASSERT_EQ(after.size(), 1u);
    EXPECT_EQ(std::get<1>(after[0]), 50);  // not 1+50
}

TEST(TumblingWindowAllowedLateness, DefaultZeroLatenessKeepsLegacyBehavior) {
    // No allowed_lateness call: default 0 means the window fires AND
    // purges in the same tick. Late records create fresh buckets.
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    TumblingWindowOperator<int, int, int> op(
        1000ms, [] { return 0; }, [](int a, int v) { return a + v; });

    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), em);
    };
    send(5, 200);
    op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{1000}}), em);
    (void)drain(ch);

    // Late record creates a new bucket - sum=5, not 10.
    send(5, 200);
    op.flush(em);
    auto data = collect_data<int, int>(drain(ch));
    ASSERT_EQ(data.size(), 1u);
    EXPECT_EQ(std::get<1>(data[0]), 5);
}

// ----- Sliding -----

TEST(SlidingWindowAllowedLateness, LateRecordRefiresAllOverlappingWindows) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    // size=400, slide=200 => 2 overlapping windows per record.
    SlidingWindowOperator<int, int, int> op(
        400ms, 200ms, [] { return 0; }, [](int a, int v) { return a + v; });
    op.allowed_lateness(300ms);

    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), em);
    };

    // Record at ts=300: contributes to windows starting at -100, 100, 300.
    // Wait - size/slide = 2, so 2 windows: starts at floor(300/200)*200 =
    // 200, then 200-200=0. Both contain ts=300:
    //   [0, 400):  contains 300
    //   [200, 600): contains 300
    send(7, 300);

    // Watermark 600 - fires both windows. Window [0, 400) ends at 400
    // (fired); window [200, 600) ends at 600 (fired).
    op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{600}}), em);
    auto first = collect_data<int, int>(drain(ch));
    ASSERT_EQ(first.size(), 2u);
    // Both windows have sum=7 since they share the single record.

    // Late record at ts=300: still in lateness window for both
    // (purge at end+300 = 700 for [0,400), 900 for [200,600)).
    // wm currently 600 < 700, < 900, so both retained.
    send(11, 300);
    auto late_refire = collect_data<int, int>(drain(ch));
    ASSERT_EQ(late_refire.size(), 2u);
    for (const auto& [k, agg, ts] : late_refire) {
        EXPECT_EQ(agg, 18);  // 7 + 11
    }
}

// ----- Session -----

TEST(SessionWindowAllowedLateness, LateRecordRefiresAndExtendsFiredSession) {
    BoundedChannel<StreamElement<std::pair<int, int>>> ch(64);
    Emitter<std::pair<int, int>> em(&ch);

    SessionWindowOperator<int, int, int> op(
        100ms,
        [] { return 0; },
        [](int a, int v) { return a + v; },
        [](int a, int b) { return a + b; });
    op.allowed_lateness(500ms);

    auto send = [&](int v, std::int64_t ts) {
        Batch<std::pair<int, int>> b;
        b.emplace(std::pair<int, int>{1, v}, EventTime{ts});
        op.process(StreamElement<std::pair<int, int>>::data(std::move(b)), em);
    };

    // Session at [100, 200], sum=5.
    send(5, 100);
    // Watermark 200 - fires session.
    op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{200}}), em);
    auto first = collect_data<int, int>(drain(ch));
    ASSERT_EQ(first.size(), 1u);
    EXPECT_EQ(std::get<1>(first[0]), 5);

    // Late record at ts=180: provisional [180, 280] overlaps the fired
    // session [100, 200]. Merge -> [100, 280], sum = 5 + 7 = 12,
    // fired=true (carried). Re-emit.
    send(7, 180);
    auto refire = collect_data<int, int>(drain(ch));
    ASSERT_EQ(refire.size(), 1u);
    EXPECT_EQ(std::get<1>(refire[0]), 12);
    EXPECT_EQ(std::get<2>(refire[0]), 279);  // new session_end (280) - 1

    // Watermark 800 - past purge_at (200 + 500 = 700; wait, but the
    // session_end has now been extended to 280, so purge_at = 280 +
    // 500 = 780). At wm=800 the session is purged.
    op.process(StreamElement<std::pair<int, int>>::watermark(Watermark{EventTime{800}}), em);
    (void)drain(ch);

    // Record at ts=180: now creates a fresh session, no merge.
    send(99, 180);
    op.flush(em);
    auto fresh = collect_data<int, int>(drain(ch));
    ASSERT_EQ(fresh.size(), 1u);
    EXPECT_EQ(std::get<1>(fresh[0]), 99);
}
