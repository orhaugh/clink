// Columnar-native execution, increments 2 + 6: ColumnarKeyedAggregateOperator
// (Sum/Count/Min/Max) + the 3-column {event_time, key, value} Arrow batcher.
//
// The load-bearing claims under test:
//   - the columnar fast path aggregates correctly AND decodes zero rows
//     (batch_materialize_counter stays put while the operator ingests),
//   - every AggKind (Sum/Count/Min/Max) is BYTE-IDENTICAL between the columnar
//     and row paths (they share fold_one_), including negative values (Min/Max
//     must seed from the first value, never a 0 sentinel),
//   - the per-key results span multiple columnar batches,
//   - a row batch handed to process_columnar signals fallback (returns false)
//     and does NOT touch the accumulator (no double-count on re-run),
//   - a columnar batch fed to the row path lazily materializes exactly once,
//   - an end-to-end pipeline runs through the real runner with zero row
//     materialization on the ingest path.

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "clink/core/arrow_batcher.hpp"
#include "clink/core/record.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/operators/columnar_keyed_aggregate_operator.hpp"
#include "clink/operators/columnar_keyed_vector_source.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"

namespace {

using namespace clink;
using KV = std::pair<std::int64_t, std::int64_t>;

std::uint64_t materialize_count() {
    return detail::batch_materialize_counter().load(std::memory_order_relaxed);
}

// Build a columnar Batch<KV> directly from columns (no Record rows).
Batch<KV> make_columnar_kv(const std::vector<KV>& kvs) {
    auto batcher = int64_keyed_arrow_batcher();
    arrow::Int64Builder tb;
    arrow::Int64Builder kb;
    arrow::Int64Builder vb;
    for (const auto& kv : kvs) {
        (void)tb.AppendNull();  // event_time null
        (void)kb.Append(kv.first);
        (void)vb.Append(kv.second);
    }
    std::shared_ptr<arrow::Array> ta;
    std::shared_ptr<arrow::Array> ka;
    std::shared_ptr<arrow::Array> va;
    (void)tb.Finish(&ta);
    (void)kb.Finish(&ka);
    (void)vb.Finish(&va);
    auto rb = arrow::RecordBatch::Make(
        batcher.schema(), static_cast<std::int64_t>(kvs.size()), {ta, ka, va});
    auto parse = batcher.parse;
    Batch<KV>::MaterializeFn mat = [parse](const arrow::RecordBatch& b) -> std::vector<Record<KV>> {
        auto x = parse(b);
        return x ? x->take_records() : std::vector<Record<KV>>{};
    };
    return Batch<KV>{std::move(rb), kvs.size(), std::move(mat)};
}

struct Capture {
    std::vector<StreamElement<KV>> elems;
    Emitter<KV> emitter() {
        return Emitter<KV>([this](StreamElement<KV> e) {
            elems.push_back(std::move(e));
            return true;
        });
    }
    std::vector<KV> totals() {
        std::vector<KV> out;
        for (auto& e : elems) {
            if (e.is_data()) {
                for (const auto& r : e.as_data()) {
                    out.push_back(r.value());
                }
            }
        }
        std::sort(out.begin(), out.end());
        return out;
    }
};

// Run the same data through the columnar fast path and the row path; assert the
// columnar arm decoded zero rows and the row arm decoded once, and return both
// result sets (sorted) for an equivalence check.
std::pair<std::vector<KV>, std::vector<KV>> run_both_paths(AggKind kind,
                                                           const std::vector<KV>& kvs) {
    ColumnarKeyedAggregateOperator col(kind);
    Capture col_cap;
    auto col_em = col_cap.emitter();
    const auto before_col = materialize_count();
    EXPECT_TRUE(col.process_columnar(StreamElement<KV>::data(make_columnar_kv(kvs)), col_em));
    EXPECT_EQ(materialize_count() - before_col, 0u) << "columnar ingest must not decode rows";
    col.flush(col_em);

    ColumnarKeyedAggregateOperator row(kind, "row", /*enable_columnar=*/false);
    Capture row_cap;
    auto row_em = row_cap.emitter();
    const auto before_row = materialize_count();
    row.process(StreamElement<KV>::data(make_columnar_kv(kvs)), row_em);
    EXPECT_EQ(materialize_count() - before_row, 1u) << "row path decodes the batch once";
    row.flush(row_em);

    return {col_cap.totals(), row_cap.totals()};
}

}  // namespace

TEST(ColumnarKeyedAggregate, FastPathSumsAndDecodesZeroRows) {
    ColumnarKeyedAggregateOperator op;  // default AggKind::Sum
    Capture cap;
    auto em = cap.emitter();
    const auto before = materialize_count();

    // keys 1,2,3 with values; key 1: 10+40=50, key 2: 20+5=25, key 3: 30.
    const bool handled = op.process_columnar(
        StreamElement<KV>::data(make_columnar_kv({{1, 10}, {2, 20}, {3, 30}, {1, 40}, {2, 5}})),
        em);
    EXPECT_TRUE(handled);
    EXPECT_EQ(materialize_count() - before, 0u) << "columnar ingest must not decode rows";
    EXPECT_TRUE(cap.elems.empty()) << "aggregation emits at flush(), not per batch";

    op.flush(em);
    EXPECT_EQ(cap.totals(), (std::vector<KV>{{1, 50}, {2, 25}, {3, 30}}));
    EXPECT_EQ(materialize_count() - before, 0u);
}

TEST(ColumnarKeyedAggregate, PerKeyTotalsSpanMultipleColumnarBatches) {
    ColumnarKeyedAggregateOperator op;
    Capture cap;
    auto em = cap.emitter();
    const auto before = materialize_count();

    op.process_columnar(StreamElement<KV>::data(make_columnar_kv({{1, 10}, {2, 20}})), em);
    op.process_columnar(StreamElement<KV>::data(make_columnar_kv({{1, 100}, {3, 7}})), em);
    op.process_columnar(StreamElement<KV>::data(make_columnar_kv({{2, 2}, {1, 1}})), em);
    op.flush(em);

    EXPECT_EQ(materialize_count() - before, 0u) << "all three batches ingested columnar";
    EXPECT_EQ(cap.totals(), (std::vector<KV>{{1, 111}, {2, 22}, {3, 7}}));
}

TEST(ColumnarKeyedAggregate, ProcessColumnarRejectsRowBatchWithoutTouchingAccumulator) {
    ColumnarKeyedAggregateOperator op;
    Capture cap;
    auto em = cap.emitter();
    Batch<KV> row_batch;  // pure row batch, no sidecar
    row_batch.emplace(KV{1, 999});
    EXPECT_FALSE(op.process_columnar(StreamElement<KV>::data(std::move(row_batch)), em))
        << "a row batch must signal fallback to process()";
    op.flush(em);
    EXPECT_TRUE(cap.totals().empty()) << "rejected batch must not have been aggregated";
}

TEST(ColumnarKeyedAggregate, ColumnarBatchLazilyMaterializesForRowPath) {
    // enable_columnar=false forces the row path; a columnar upstream batch then
    // lazily decodes exactly once (the same backward-compat contract the filter
    // has).
    ColumnarKeyedAggregateOperator op(AggKind::Sum, "row_keyed_sum", /*enable_columnar=*/false);
    EXPECT_FALSE(op.supports_columnar());
    Capture cap;
    auto em = cap.emitter();
    const auto before = materialize_count();

    op.process(StreamElement<KV>::data(make_columnar_kv({{1, 10}, {2, 20}, {1, 5}})), em);
    EXPECT_EQ(materialize_count() - before, 1u) << "row path decodes the sidecar once";
    op.flush(em);
    EXPECT_EQ(cap.totals(), (std::vector<KV>{{1, 15}, {2, 20}}));
}

TEST(ColumnarKeyedAggregate, EmptyColumnarBatchAndEmptyFlushEmitNothing) {
    ColumnarKeyedAggregateOperator op;
    Capture cap;
    auto em = cap.emitter();
    const auto before = materialize_count();

    EXPECT_TRUE(op.process_columnar(StreamElement<KV>::data(make_columnar_kv({})), em));
    op.flush(em);
    EXPECT_TRUE(cap.elems.empty());
    EXPECT_EQ(materialize_count() - before, 0u);
}

// --- Increment 6: Count / Min / Max, each byte-identical columnar vs row. ---

TEST(ColumnarKeyedAggregate, CountColumnarMatchesRowAndIsCorrect) {
    // key 1: 3 records, key 2: 2 records. Values are ignored by Count.
    auto [col, row] = run_both_paths(AggKind::Count, {{1, 10}, {1, 20}, {2, 5}, {1, 7}, {2, 99}});
    EXPECT_EQ(col, row) << "Count columnar path must equal the row path";
    EXPECT_EQ(col, (std::vector<KV>{{1, 3}, {2, 2}}));
}

TEST(ColumnarKeyedAggregate, MinColumnarMatchesRowAndIsCorrect) {
    auto [col, row] = run_both_paths(AggKind::Min, {{1, 10}, {1, 3}, {1, 20}, {2, 5}, {2, 50}});
    EXPECT_EQ(col, row) << "Min columnar path must equal the row path";
    EXPECT_EQ(col, (std::vector<KV>{{1, 3}, {2, 5}}));
}

TEST(ColumnarKeyedAggregate, MaxColumnarMatchesRowAndIsCorrect) {
    auto [col, row] = run_both_paths(AggKind::Max, {{1, 10}, {1, 3}, {1, 20}, {2, 5}, {2, 50}});
    EXPECT_EQ(col, row) << "Max columnar path must equal the row path";
    EXPECT_EQ(col, (std::vector<KV>{{1, 20}, {2, 50}}));
}

// Min/Max fold correctly ACROSS multiple columnar batches (acc_ persists
// between process_columnar calls until flush).
TEST(ColumnarKeyedAggregate, MinFoldsAcrossMultipleColumnarBatches) {
    ColumnarKeyedAggregateOperator op(AggKind::Min);
    Capture cap;
    auto em = cap.emitter();
    const auto before = materialize_count();
    op.process_columnar(StreamElement<KV>::data(make_columnar_kv({{1, 50}, {2, 5}})), em);
    op.process_columnar(StreamElement<KV>::data(make_columnar_kv({{1, 8}, {2, 9}})), em);
    op.process_columnar(StreamElement<KV>::data(make_columnar_kv({{1, 20}})), em);
    op.flush(em);
    EXPECT_EQ(materialize_count() - before, 0u);
    EXPECT_EQ(cap.totals(), (std::vector<KV>{{1, 8}, {2, 5}}))
        << "min must span batches: key1 min(50,8,20)=8, key2 min(5,9)=5";
}

// Min/Max must seed from the FIRST value, not a 0 sentinel - otherwise all-
// negative keys would wrongly clamp at 0.
TEST(ColumnarKeyedAggregate, MinMaxSeedFromFirstValueNotZeroSentinel) {
    auto [min_col, min_row] = run_both_paths(AggKind::Min, {{1, -5}, {1, -10}, {1, -3}});
    EXPECT_EQ(min_col, min_row);
    EXPECT_EQ(min_col, (std::vector<KV>{{1, -10}})) << "Min over all-negative must be -10, not 0";

    auto [max_col, max_row] = run_both_paths(AggKind::Max, {{1, -5}, {1, -10}, {1, -3}});
    EXPECT_EQ(max_col, max_row);
    EXPECT_EQ(max_col, (std::vector<KV>{{1, -3}})) << "Max over all-negative must be -3, not 0";
}

// Collects (key, agg) pairs from the (row) flush output.
class Int64PairCollectingSink final : public Sink<KV> {
public:
    using Sink<KV>::on_data;
    void on_data(const Batch<KV>& batch) override {
        for (const auto& r : batch) {
            received.push_back(r.value());
        }
    }
    std::string name() const override { return "pair_collect"; }
    std::vector<KV> received;
};

TEST(ColumnarKeyedAggregate, EndToEndColumnarPipelineDecodesZeroRowsOnIngest) {
    // 1000 records round-robin across 4 keys; each key's MAX is the largest i it
    // was assigned (i increases, so the last occurrence of each residue).
    std::vector<KV> input;
    std::vector<std::int64_t> expected_max(4, -1);
    for (std::int64_t i = 0; i < 1000; ++i) {
        const std::int64_t k = i % 4;
        input.emplace_back(k, i);
        expected_max[static_cast<std::size_t>(k)] = i;  // monotonically increasing
    }
    auto sink = std::make_shared<Int64PairCollectingSink>();
    Dag dag;
    auto src = std::make_shared<ColumnarKeyedVectorSource>(input, /*batch_size=*/256);
    auto op = std::make_shared<ColumnarKeyedAggregateOperator>(AggKind::Max);
    auto h0 = dag.add_source<KV>(src);
    auto h1 = dag.add_operator<KV, KV>(h0, op);
    dag.add_sink<KV>(h1, sink);

    const auto before = materialize_count();
    JobConfig cfg;
    LocalExecutor exec(std::move(dag), cfg);
    exec.run();

    EXPECT_EQ(materialize_count() - before, 0u)
        << "columnar source -> columnar keyed-agg must decode zero rows on ingest";
    ASSERT_EQ(sink->received.size(), 4u);
    std::sort(sink->received.begin(), sink->received.end());
    for (std::int64_t k = 0; k < 4; ++k) {
        EXPECT_EQ(sink->received[static_cast<std::size_t>(k)],
                  (KV{k, expected_max[static_cast<std::size_t>(k)]}));
    }
}
