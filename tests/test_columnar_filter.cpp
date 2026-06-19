// Columnar-native execution, increment 1: ColumnarFilterOperator + the
// Batch<T> Arrow sidecar with lazy row materialization.
//
// The load-bearing claims under test:
//   - the columnar fast path filters correctly AND decodes zero rows
//     (batch_materialize_counter stays put while the operator runs),
//   - a columnar batch handed to a ROW operator lazily materializes exactly
//     once (backward compatibility),
//   - the asymmetric pairings degrade cleanly: row input -> columnar operator
//     falls back to the row predicate; columnar input -> row operator works,
//   - event-time rides through arrow::compute::Filter,
//   - an end-to-end ColumnarVectorSource -> ColumnarFilterOperator ->
//     columnar sink pipeline runs through the real runner with zero row
//     materialization anywhere.

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "clink/core/arrow_batcher.hpp"
#include "clink/core/record.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/operators/columnar_filter_operator.hpp"
#include "clink/operators/columnar_vector_source.hpp"
#include "clink/operators/filter_operator.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"

namespace {

using namespace clink;
using V = std::int64_t;

std::uint64_t materialize_count() {
    return detail::batch_materialize_counter().load(std::memory_order_relaxed);
}

// Build a columnar Batch<int64_t> directly from columns (no Record rows).
Batch<V> make_columnar(const std::vector<V>& vals,
                       const std::vector<std::optional<std::int64_t>>& ts = {}) {
    auto batcher = int64_arrow_batcher();
    arrow::Int64Builder tb;
    arrow::Int64Builder vb;
    for (std::size_t i = 0; i < vals.size(); ++i) {
        if (i < ts.size() && ts[i].has_value()) {
            (void)tb.Append(*ts[i]);
        } else {
            (void)tb.AppendNull();
        }
        (void)vb.Append(vals[i]);
    }
    std::shared_ptr<arrow::Array> ta;
    std::shared_ptr<arrow::Array> va;
    (void)tb.Finish(&ta);
    (void)vb.Finish(&va);
    auto rb = arrow::RecordBatch::Make(
        batcher.schema(), static_cast<std::int64_t>(vals.size()), {ta, va});
    auto parse = batcher.parse;
    Batch<V>::MaterializeFn mat = [parse](const arrow::RecordBatch& b) -> std::vector<Record<V>> {
        auto x = parse(b);
        return x ? x->take_records() : std::vector<Record<V>>{};
    };
    return Batch<V>{std::move(rb), vals.size(), std::move(mat)};
}

// Capture emitted StreamElements via the Emitter forward ctor.
struct Capture {
    std::vector<StreamElement<V>> elems;
    Emitter<V> emitter() {
        return Emitter<V>([this](StreamElement<V> e) {
            elems.push_back(std::move(e));
            return true;
        });
    }
    std::vector<V> values() {
        std::vector<V> out;
        for (auto& e : elems) {
            if (e.is_data()) {
                for (const auto& r : e.as_data()) {
                    out.push_back(r.value());
                }
            }
        }
        return out;
    }
};

TEST(ColumnarFilter, FastPathFiltersAndDecodesZeroRows) {
    ColumnarFilterOperator op(50);
    Capture cap;
    auto em = cap.emitter();
    const auto before = materialize_count();

    const bool handled =
        op.process_columnar(StreamElement<V>::data(make_columnar({10, 60, 50, 99, 3})), em);

    // The operator ran the vectorized path and emitted columnar output
    // without decoding a single row.
    EXPECT_TRUE(handled);
    EXPECT_EQ(materialize_count() - before, 0u) << "columnar fast path must not decode rows";
    ASSERT_EQ(cap.elems.size(), 1u);
    ASSERT_TRUE(cap.elems[0].is_data());
    EXPECT_TRUE(cap.elems[0].as_data().is_columnar()) << "output should stay columnar";

    // Reading the values for the assertion materializes the OUTPUT batch
    // (that is the test decoding, not the operator).
    EXPECT_EQ(cap.values(), (std::vector<V>{60, 50, 99}));
}

TEST(ColumnarFilter, ProcessColumnarRejectsRowBatch) {
    ColumnarFilterOperator op(50);
    Capture cap;
    auto em = cap.emitter();
    Batch<V> row_batch;  // pure row batch, no sidecar
    row_batch.emplace(60);
    EXPECT_FALSE(op.process_columnar(StreamElement<V>::data(std::move(row_batch)), em))
        << "a row batch must signal fallback to process()";
}

TEST(ColumnarFilter, RowInputFallbackFiltersCorrectly) {
    // The runner falls back to process() for a row batch; verify the row
    // predicate matches the columnar one.
    ColumnarFilterOperator op(50);
    Capture cap;
    auto em = cap.emitter();
    Batch<V> row_batch;
    for (V v : {10, 60, 50, 99, 3}) {
        row_batch.emplace(v);
    }
    op.process(StreamElement<V>::data(std::move(row_batch)), em);
    EXPECT_EQ(cap.values(), (std::vector<V>{60, 50, 99}));
}

TEST(ColumnarFilter, ColumnarBatchLazilyMaterializesForRowOperator) {
    // Backward compat: a columnar producer feeding a ROW operator triggers
    // exactly one lazy decode.
    FilterOperator<V> row_filter([](const V& v) { return v >= 50; });
    Capture cap;
    auto em = cap.emitter();
    const auto before = materialize_count();

    row_filter.process(StreamElement<V>::data(make_columnar({10, 60, 50, 99, 3})), em);

    EXPECT_EQ(materialize_count() - before, 1u) << "row operator must decode the sidecar once";
    EXPECT_EQ(cap.values(), (std::vector<V>{60, 50, 99}));
}

TEST(ColumnarFilter, EventTimeSurvivesColumnarFilter) {
    ColumnarFilterOperator op(50);
    Capture cap;
    auto em = cap.emitter();
    op.process_columnar(StreamElement<V>::data(make_columnar({10, 60, 99}, {{100}, {200}, {300}})),
                        em);
    ASSERT_EQ(cap.elems.size(), 1u);
    std::vector<std::pair<V, std::int64_t>> kept;
    for (const auto& r : cap.elems[0].as_data()) {
        ASSERT_TRUE(r.event_time().has_value());
        kept.emplace_back(r.value(), r.event_time()->millis());
    }
    EXPECT_EQ(kept, (std::vector<std::pair<V, std::int64_t>>{{60, 200}, {99, 300}}));
}

// A columnar-aware sink: reads the value column straight from the Arrow
// sidecar (no row decode) when present, else falls back to rows.
class ColumnarCollectingSink final : public Sink<V> {
public:
    using Sink<V>::on_data;  // keep the base rvalue overload visible
    void on_data(const Batch<V>& batch) override {
        if (batch.is_columnar() && batch.arrow()) {
            const auto* v = static_cast<const arrow::Int64Array*>(batch.arrow()->column(1).get());
            for (std::int64_t i = 0; i < v->length(); ++i) {
                received.push_back(v->Value(i));
            }
        } else {
            for (const auto& r : batch) {
                received.push_back(r.value());
            }
        }
    }
    std::string name() const override { return "columnar_collect"; }
    std::vector<V> received;
};

TEST(ColumnarFilter, EndToEndColumnarPipelineDecodesZeroRows) {
    std::vector<V> input;
    for (V i = 0; i < 1000; ++i) {
        input.push_back(i);
    }
    auto sink = std::make_shared<ColumnarCollectingSink>();
    Dag dag;
    auto src = std::make_shared<ColumnarVectorSource>(input, /*batch_size=*/256);
    auto op = std::make_shared<ColumnarFilterOperator>(500);
    auto h0 = dag.add_source<V>(src);
    auto h1 = dag.add_operator<V, V>(h0, op);
    dag.add_sink<V>(h1, sink);

    const auto before = materialize_count();
    JobConfig cfg;
    LocalExecutor exec(std::move(dag), cfg);
    exec.run();

    // 500..999 pass the >= 500 filter.
    ASSERT_EQ(sink->received.size(), 500u);
    EXPECT_EQ(sink->received.front(), 500);
    EXPECT_EQ(sink->received.back(), 999);
    EXPECT_EQ(materialize_count() - before, 0u)
        << "columnar source -> columnar filter -> columnar sink must decode zero rows";
}

}  // namespace
