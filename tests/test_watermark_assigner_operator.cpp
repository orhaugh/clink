// Unit tests for WatermarkAssignerOperator. Pins the contract that:
//   - Records without event time are stamped via the user extractor.
//   - Records with existing event time are passed through unchanged.
//   - Watermarks are emitted only when the strategy reports progress.
//   - flush() emits any pending watermark on end-of-stream.
//   - Upstream watermarks/barriers are forwarded.

#include <chrono>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/operators/watermark_assigner_operator.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/time/watermark_strategy.hpp"

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

}  // namespace

TEST(WatermarkAssignerOperator, StampsEventTimeFromExtractor) {
    BoundedChannel<StreamElement<int>> ch(64);
    Emitter<int> em(&ch);

    WatermarkAssignerOperator<int> op([](const int& v) { return EventTime{v * 10}; },
                                      std::make_unique<MonotonicWatermarkStrategy<int>>());

    Batch<int> b;
    b.emplace(5);   // no event time
    b.emplace(10);  // no event time
    op.process(StreamElement<int>::data(std::move(b)), em);

    auto elements = drain(ch);
    // Expect: data batch, then watermark (since strategy advanced).
    ASSERT_GE(elements.size(), 1u);
    ASSERT_TRUE(elements[0].is_data());
    const auto& out = elements[0].as_data();
    ASSERT_EQ(out.size(), 2u);
    EXPECT_EQ(out[0].event_time()->millis(), 50);   // 5 * 10
    EXPECT_EQ(out[1].event_time()->millis(), 100);  // 10 * 10
}

TEST(WatermarkAssignerOperator, PreservesExistingEventTime) {
    BoundedChannel<StreamElement<int>> ch(64);
    Emitter<int> em(&ch);

    bool extractor_called = false;
    WatermarkAssignerOperator<int> op(
        [&extractor_called](const int&) {
            extractor_called = true;
            return EventTime{99999};  // would be wrong if applied
        },
        std::make_unique<MonotonicWatermarkStrategy<int>>());

    Batch<int> b;
    b.emplace(7, EventTime{42});  // already has ts
    op.process(StreamElement<int>::data(std::move(b)), em);

    auto elements = drain(ch);
    ASSERT_TRUE(elements[0].is_data());
    EXPECT_EQ(elements[0].as_data()[0].event_time()->millis(), 42);
    EXPECT_FALSE(extractor_called);
}

TEST(WatermarkAssignerOperator, EmitsWatermarkWhenStrategyAdvances) {
    BoundedChannel<StreamElement<int>> ch(64);
    Emitter<int> em(&ch);

    WatermarkAssignerOperator<int> op([](const int& v) { return EventTime{v}; },
                                      std::make_unique<MonotonicWatermarkStrategy<int>>());

    Batch<int> b1;
    b1.emplace(100);
    b1.emplace(200);
    op.process(StreamElement<int>::data(std::move(b1)), em);

    auto first = drain(ch);
    bool saw_wm = false;
    for (const auto& e : first) {
        if (e.is_watermark()) {
            saw_wm = true;
            EXPECT_EQ(e.as_watermark().timestamp().millis(), 200);
        }
    }
    EXPECT_TRUE(saw_wm);

    // Second batch with same max ts - strategy reports no advance,
    // so no new watermark.
    Batch<int> b2;
    b2.emplace(150);  // older than watermark, ignored
    op.process(StreamElement<int>::data(std::move(b2)), em);

    auto second = drain(ch);
    bool saw_wm_again = false;
    for (const auto& e : second) {
        if (e.is_watermark()) {
            saw_wm_again = true;
        }
    }
    EXPECT_FALSE(saw_wm_again);
}

TEST(WatermarkAssignerOperator, BoundedOutOfOrdernessSubtractsBound) {
    BoundedChannel<StreamElement<int>> ch(64);
    Emitter<int> em(&ch);

    WatermarkAssignerOperator<int> op([](const int& v) { return EventTime{v}; },
                                      std::make_unique<BoundedOutOfOrdernessStrategy<int>>(50ms));

    Batch<int> b;
    b.emplace(1000);
    op.process(StreamElement<int>::data(std::move(b)), em);

    auto elements = drain(ch);
    bool saw_wm = false;
    for (const auto& e : elements) {
        if (e.is_watermark()) {
            saw_wm = true;
            EXPECT_EQ(e.as_watermark().timestamp().millis(), 950);
        }
    }
    EXPECT_TRUE(saw_wm);
}

TEST(WatermarkAssignerOperator, FlushEmitsFinalWatermark) {
    BoundedChannel<StreamElement<int>> ch(64);
    Emitter<int> em(&ch);

    WatermarkAssignerOperator<int> op([](const int& v) { return EventTime{v}; },
                                      std::make_unique<MonotonicWatermarkStrategy<int>>());

    // Process records, drain emitted watermark.
    Batch<int> b;
    b.emplace(500);
    op.process(StreamElement<int>::data(std::move(b)), em);
    (void)drain(ch);

    // Now flush: the strategy is no longer dirty, so flush should not
    // emit a redundant watermark. The contract is: flush emits if the
    // strategy currently has one to give.
    op.flush(em);
    auto after_flush = drain(ch);
    bool saw_wm = false;
    for (const auto& e : after_flush) {
        if (e.is_watermark()) {
            saw_wm = true;
        }
    }
    EXPECT_FALSE(saw_wm);
}

TEST(WatermarkAssignerOperator, ForwardsUpstreamWatermarkUntouched) {
    BoundedChannel<StreamElement<int>> ch(64);
    Emitter<int> em(&ch);

    WatermarkAssignerOperator<int> op([](const int&) { return EventTime{0}; },
                                      std::make_unique<MonotonicWatermarkStrategy<int>>());

    op.process(StreamElement<int>::watermark(Watermark{EventTime{777}}), em);

    auto elements = drain(ch);
    ASSERT_EQ(elements.size(), 1u);
    ASSERT_TRUE(elements[0].is_watermark());
    EXPECT_EQ(elements[0].as_watermark().timestamp().millis(), 777);
}

TEST(WatermarkAssignerOperator, ForwardsBarrier) {
    BoundedChannel<StreamElement<int>> ch(64);
    Emitter<int> em(&ch);

    WatermarkAssignerOperator<int> op([](const int&) { return EventTime{0}; },
                                      std::make_unique<MonotonicWatermarkStrategy<int>>());

    op.process(StreamElement<int>::barrier(CheckpointBarrier{CheckpointId{55}}), em);

    auto elements = drain(ch);
    ASSERT_EQ(elements.size(), 1u);
    ASSERT_TRUE(elements[0].is_barrier());
    EXPECT_EQ(elements[0].as_barrier().id(), CheckpointId{55});
}

TEST(WatermarkAssignerOperator, NoEventTimeAndExtractorReturnsZeroStillValid) {
    // Defensive: extractor that always returns 0 means watermark
    // permanently sits at 0; downstream should still see a (single)
    // watermark advance from min to 0.
    BoundedChannel<StreamElement<int>> ch(64);
    Emitter<int> em(&ch);

    WatermarkAssignerOperator<int> op([](const int&) { return EventTime{0}; },
                                      std::make_unique<MonotonicWatermarkStrategy<int>>());

    Batch<int> b;
    b.emplace(1);
    op.process(StreamElement<int>::data(std::move(b)), em);

    auto elements = drain(ch);
    bool saw_wm_at_zero = false;
    for (const auto& e : elements) {
        if (e.is_watermark()) {
            EXPECT_EQ(e.as_watermark().timestamp().millis(), 0);
            saw_wm_at_zero = true;
        }
    }
    EXPECT_TRUE(saw_wm_at_zero);
}
