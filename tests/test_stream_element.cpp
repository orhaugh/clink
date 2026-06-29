// StreamElement is the in-band envelope flowing on every operator-to-
// operator channel. It carries one of three payload kinds: data, watermark,
// or barrier. Operators dispatch on kind, and the engine's correctness
// (alignment, exactly-once) depends on the variant being handled
// uniformly. These tests fix the construction, accessor, and visit-
// dispatch behavior so a future refactor can't silently drop a kind.

#include <cstdint>
#include <string>

#include <gtest/gtest.h>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/core/record.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/time/event_time.hpp"
#include "clink/time/watermark.hpp"

using namespace clink;

TEST(StreamElement, DataElementHoldsBatchAndReportsKind) {
    Batch<std::int64_t> b;
    b.emplace(7);
    b.emplace(8);

    auto e = StreamElement<std::int64_t>::data(std::move(b));
    EXPECT_EQ(e.kind(), StreamElement<std::int64_t>::Kind::Data);
    EXPECT_TRUE(e.is_data());
    EXPECT_FALSE(e.is_watermark());
    EXPECT_FALSE(e.is_barrier());

    ASSERT_EQ(e.as_data().size(), 2u);
    EXPECT_EQ(e.as_data()[0].value(), 7);
    EXPECT_EQ(e.as_data()[1].value(), 8);
}

TEST(StreamElement, WatermarkElementCarriesTimestamp) {
    auto e = StreamElement<std::int64_t>::watermark(Watermark{EventTime::from_millis(42)});
    EXPECT_EQ(e.kind(), StreamElement<std::int64_t>::Kind::Watermark);
    EXPECT_TRUE(e.is_watermark());
    EXPECT_EQ(e.as_watermark().timestamp().millis(), 42);
}

TEST(StreamElement, BarrierElementCarriesId) {
    auto e = StreamElement<std::int64_t>::barrier(CheckpointBarrier{CheckpointId{99}});
    EXPECT_EQ(e.kind(), StreamElement<std::int64_t>::Kind::Barrier);
    EXPECT_TRUE(e.is_barrier());
    EXPECT_EQ(e.as_barrier().id(), CheckpointId{99});
}

TEST(StreamElement, VisitDispatchesToActiveAlternative) {
    int data_calls = 0;
    int watermark_calls = 0;
    int barrier_calls = 0;
    auto visitor = [&](auto&& payload) {
        using T = std::decay_t<decltype(payload)>;
        if constexpr (std::is_same_v<T, Batch<std::int64_t>>) {
            data_calls++;
        } else if constexpr (std::is_same_v<T, Watermark>) {
            watermark_calls++;
        } else if constexpr (std::is_same_v<T, CheckpointBarrier>) {
            barrier_calls++;
        }
    };

    Batch<std::int64_t> b;
    b.emplace(1);
    StreamElement<std::int64_t>::data(std::move(b)).visit(visitor);
    StreamElement<std::int64_t>::watermark(Watermark{EventTime::from_millis(0)}).visit(visitor);
    StreamElement<std::int64_t>::barrier(CheckpointBarrier{CheckpointId{1}}).visit(visitor);

    EXPECT_EQ(data_calls, 1);
    EXPECT_EQ(watermark_calls, 1);
    EXPECT_EQ(barrier_calls, 1);
}

TEST(StreamElement, MutableVisitCanModifyData) {
    Batch<std::int64_t> b;
    b.emplace(10);
    auto e = StreamElement<std::int64_t>::data(std::move(b));

    e.visit([](auto& payload) {
        using T = std::decay_t<decltype(payload)>;
        if constexpr (std::is_same_v<T, Batch<std::int64_t>>) {
            payload[0] = Record<std::int64_t>{99};
        }
    });

    EXPECT_EQ(e.as_data()[0].value(), 99);
}

TEST(Record, EventTimeIsOptionalAndMutable) {
    Record<std::string> r{"x"};
    EXPECT_FALSE(r.event_time().has_value());

    r.set_event_time(EventTime::from_millis(123));
    ASSERT_TRUE(r.event_time().has_value());
    EXPECT_EQ(r.event_time()->millis(), 123);
}

TEST(Watermark, MinAndMaxAreSentinels) {
    EXPECT_LT(Watermark::min(), Watermark{EventTime::from_millis(0)});
    EXPECT_GT(Watermark::max(), Watermark{EventTime::from_millis(0)});
    EXPECT_LT(Watermark::min(), Watermark::max());
}

TEST(Watermark, ComparisonIsValueBased) {
    Watermark a{EventTime::from_millis(100)};
    Watermark b{EventTime::from_millis(100)};
    Watermark c{EventTime::from_millis(101)};
    EXPECT_EQ(a, b);
    EXPECT_LT(a, c);
}

TEST(CheckpointBarrier, EqualityComparesId) {
    EXPECT_EQ(CheckpointBarrier{CheckpointId{1}}, CheckpointBarrier{CheckpointId{1}});
    EXPECT_NE(CheckpointBarrier{CheckpointId{1}}, CheckpointBarrier{CheckpointId{2}});
}

// --- Drain marker -------------------------------------------------

TEST(StreamElement, DrainElementCarriesMarkerFields) {
    DrainMarker d{.subtask_idx = 3, .target_parallelism = 8};
    auto e = StreamElement<std::int64_t>::drain(d);
    EXPECT_EQ(e.kind(), StreamElement<std::int64_t>::Kind::Drain);
    EXPECT_TRUE(e.is_drain());
    EXPECT_FALSE(e.is_data());
    EXPECT_FALSE(e.is_watermark());
    EXPECT_FALSE(e.is_barrier());
    EXPECT_EQ(e.as_drain().subtask_idx, 3u);
    EXPECT_EQ(e.as_drain().target_parallelism, 8u);
}

TEST(DrainMarker, EqualityComparesAllFields) {
    EXPECT_EQ((DrainMarker{1, 4}), (DrainMarker{1, 4}));
    EXPECT_NE((DrainMarker{1, 4}), (DrainMarker{1, 5}));
    EXPECT_NE((DrainMarker{1, 4}), (DrainMarker{2, 4}));
}
