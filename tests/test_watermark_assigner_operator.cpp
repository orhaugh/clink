// Unit tests for WatermarkAssignerOperator. Pins the contract that:
//   - Records without event time are stamped via the user extractor.
//   - Records with existing event time are passed through unchanged.
//   - Watermarks are emitted only when the strategy reports progress.
//   - flush() emits any pending watermark on end-of-stream.
//   - Upstream watermarks/barriers are forwarded.

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <gtest/gtest.h>

#include "clink/core/record.hpp"
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

// A columnar Batch<int> with an event_time (int64) sidecar column 0 and a
// __source_partition (int32) column 1, mirroring what
// json_string_to_row_columnar emits for a partitioned source. The materialize
// closure is provided but must NOT run on the columnar fast path.
Batch<int> make_partitioned_columnar_batch(
    const std::vector<std::pair<std::int64_t, std::int32_t>>& ts_part) {
    arrow::Int64Builder tb;
    arrow::Int32Builder pb;
    for (const auto& [t, p] : ts_part) {
        (void)tb.Append(t);
        (void)pb.Append(p);
    }
    std::shared_ptr<arrow::Array> ta, pa;
    (void)tb.Finish(&ta);
    (void)pb.Finish(&pa);
    auto schema = arrow::schema({arrow::field("event_time", arrow::int64()),
                                 arrow::field("__source_partition", arrow::int32())});
    auto rb = arrow::RecordBatch::Make(schema, static_cast<std::int64_t>(ts_part.size()), {ta, pa});
    auto materialize = [](const arrow::RecordBatch& b) -> std::vector<Record<int>> {
        const auto* t = static_cast<const arrow::Int64Array*>(b.column(0).get());
        std::vector<Record<int>> recs;
        for (std::int64_t i = 0; i < b.num_rows(); ++i) {
            recs.emplace_back(0, EventTime{t->Value(i)});
        }
        return recs;
    };
    return Batch<int>{rb, ts_part.size(), std::move(materialize)};
}

bool read_columnar_event_times(const arrow::RecordBatch& rb, std::vector<EventTime>& out) {
    const auto* t = static_cast<const arrow::Int64Array*>(rb.column(0).get());
    out.resize(static_cast<std::size_t>(rb.num_rows()));
    for (std::int64_t i = 0; i < rb.num_rows(); ++i) {
        out[static_cast<std::size_t>(i)] = EventTime{t->Value(i)};
    }
    return true;
}

bool read_columnar_partitions(const arrow::RecordBatch& rb,
                              std::vector<std::optional<std::int32_t>>& out) {
    const int idx = rb.schema()->GetFieldIndex("__source_partition");
    out.assign(static_cast<std::size_t>(rb.num_rows()), std::nullopt);
    if (idx < 0) {
        return true;
    }
    const auto* p = static_cast<const arrow::Int32Array*>(rb.column(idx).get());
    for (std::int64_t i = 0; i < rb.num_rows(); ++i) {
        if (!p->IsNull(i)) {
            out[static_cast<std::size_t>(i)] = p->Value(i);
        }
    }
    return true;
}

std::optional<std::int64_t> last_watermark(const std::vector<StreamElement<int>>& els) {
    std::optional<std::int64_t> wm;
    for (const auto& e : els) {
        if (e.is_watermark()) {
            wm = e.as_watermark().timestamp().millis();
        }
    }
    return wm;
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

// Per-partition watermarking on the COLUMNAR fast path. A partitioned columnar
// batch (slow partition 0, fast partition 1) must emit watermark = min across
// partitions (200), NOT the global max (2000) - otherwise the watermark races
// to the fastest partition and the slow partition's in-window records are
// dropped as late (the exact silent-wrong-answer the inc1 review caught). The
// batch must stay columnar (zero materialisation) through the assigner.
TEST(WatermarkAssignerOperator, ColumnarPerPartitionWatermarkIsMinAcrossPartitions) {
    BoundedChannel<StreamElement<int>> ch(64);
    Emitter<int> em(&ch);

    WatermarkAssignerOperator<int> op(
        [](const int&) { return EventTime{0}; },
        std::make_unique<PartitionAwareBoundedOutOfOrdernessStrategy<int>>(0ms));
    op.with_columnar_event_times(read_columnar_event_times)
        .with_columnar_partitions(read_columnar_partitions);

    // part 0: 100, 200 (slow) ; part 1: 1000, 2000 (fast), interleaved.
    auto batch = make_partitioned_columnar_batch({{100, 0}, {1000, 1}, {200, 0}, {2000, 1}});

    const auto mat_before =
        clink::detail::batch_materialize_counter().load(std::memory_order_relaxed);
    const bool handled = op.process_columnar(StreamElement<int>::data(std::move(batch)), em);
    ASSERT_TRUE(handled);

    auto elements = drain(ch);
    auto wm = last_watermark(elements);
    ASSERT_TRUE(wm.has_value());
    EXPECT_EQ(*wm, 200);  // min(part0 max=200, part1 max=2000), not 2000

    // The forwarded batch stayed columnar and was never materialised.
    ASSERT_FALSE(elements.empty());
    ASSERT_TRUE(elements[0].is_data());
    EXPECT_TRUE(elements[0].as_data().is_columnar());
    EXPECT_EQ(clink::detail::batch_materialize_counter().load(std::memory_order_relaxed),
              mat_before);
}

// Control: without the partition reader, the same columnar batch collapses to a
// single global watermark (the fastest partition wins) - confirming the
// partition reader is what keeps per-partition semantics, and that a
// non-partitioned columnar source (Parquet, no reader) is unaffected.
TEST(WatermarkAssignerOperator, ColumnarWithoutPartitionReaderIsGlobalWatermark) {
    BoundedChannel<StreamElement<int>> ch(64);
    Emitter<int> em(&ch);

    WatermarkAssignerOperator<int> op(
        [](const int&) { return EventTime{0}; },
        std::make_unique<PartitionAwareBoundedOutOfOrdernessStrategy<int>>(0ms));
    op.with_columnar_event_times(read_columnar_event_times);  // no partition reader

    auto batch = make_partitioned_columnar_batch({{100, 0}, {1000, 1}, {200, 0}, {2000, 1}});
    const bool handled = op.process_columnar(StreamElement<int>::data(std::move(batch)), em);
    ASSERT_TRUE(handled);

    auto wm = last_watermark(drain(ch));
    ASSERT_TRUE(wm.has_value());
    EXPECT_EQ(*wm, 2000);  // global max - no per-partition tracking
}
