// Columnar-native execution, increment 7b: ColumnarKeyedStringAggregateOperator
// (Sum/Count/Min/Max over string keys) + the {event_time, key:utf8, value:int64}
// Arrow batcher.
//
// The load-bearing claims under test:
//   - the columnar fast path aggregates correctly AND decodes zero rows (it
//     scans the StringArray via string_view; batch_materialize_counter stays
//     put while the operator ingests),
//   - every AggKind is BYTE-IDENTICAL between the columnar and row paths (they
//     share fold_one_), including negative values,
//   - per-key results span multiple columnar batches,
//   - an end-to-end pipeline decodes zero rows on ingest.

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "clink/core/arrow_batcher.hpp"
#include "clink/core/record.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/operators/columnar_keyed_aggregate_operator.hpp"  // AggKind
#include "clink/operators/columnar_keyed_string_aggregate_operator.hpp"
#include "clink/operators/columnar_string_keyed_vector_source.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"

namespace {

using namespace clink;
using KV = std::pair<std::string, std::int64_t>;

std::uint64_t materialize_count() {
    return detail::batch_materialize_counter().load(std::memory_order_relaxed);
}

Batch<KV> make_columnar_strkv(const std::vector<KV>& kvs) {
    auto batcher = string_keyed_arrow_batcher();
    arrow::Int64Builder tb;
    arrow::StringBuilder kb;
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

std::pair<std::vector<KV>, std::vector<KV>> run_both_paths(AggKind kind,
                                                           const std::vector<KV>& kvs) {
    ColumnarKeyedStringAggregateOperator col(kind);
    Capture col_cap;
    auto col_em = col_cap.emitter();
    const auto before_col = materialize_count();
    EXPECT_TRUE(col.process_columnar(StreamElement<KV>::data(make_columnar_strkv(kvs)), col_em));
    EXPECT_EQ(materialize_count() - before_col, 0u) << "columnar ingest must not decode rows";
    col.flush(col_em);

    ColumnarKeyedStringAggregateOperator row(kind, "row", /*enable_columnar=*/false);
    Capture row_cap;
    auto row_em = row_cap.emitter();
    const auto before_row = materialize_count();
    row.process(StreamElement<KV>::data(make_columnar_strkv(kvs)), row_em);
    EXPECT_EQ(materialize_count() - before_row, 1u) << "row path decodes the batch once";
    row.flush(row_em);

    return {col_cap.totals(), row_cap.totals()};
}

}  // namespace

TEST(ColumnarKeyedStringAggregate, SumColumnarMatchesRowAndIsCorrect) {
    auto [col, row] =
        run_both_paths(AggKind::Sum, {{"a", 10}, {"b", 20}, {"a", 5}, {"c", 1}, {"b", 2}});
    EXPECT_EQ(col, row);
    EXPECT_EQ(col, (std::vector<KV>{{"a", 15}, {"b", 22}, {"c", 1}}));
}

TEST(ColumnarKeyedStringAggregate, CountColumnarMatchesRowAndIsCorrect) {
    auto [col, row] =
        run_both_paths(AggKind::Count, {{"a", 10}, {"b", 20}, {"a", 5}, {"a", 7}, {"b", 99}});
    EXPECT_EQ(col, row);
    EXPECT_EQ(col, (std::vector<KV>{{"a", 3}, {"b", 2}}));
}

TEST(ColumnarKeyedStringAggregate, MinColumnarMatchesRowAndIsCorrect) {
    auto [col, row] =
        run_both_paths(AggKind::Min, {{"a", 10}, {"a", -3}, {"a", 20}, {"b", 5}, {"b", 50}});
    EXPECT_EQ(col, row);
    EXPECT_EQ(col, (std::vector<KV>{{"a", -3}, {"b", 5}}));
}

TEST(ColumnarKeyedStringAggregate, MaxColumnarMatchesRowAndIsCorrect) {
    auto [col, row] =
        run_both_paths(AggKind::Max, {{"a", 10}, {"a", -3}, {"a", 20}, {"b", 5}, {"b", 50}});
    EXPECT_EQ(col, row);
    EXPECT_EQ(col, (std::vector<KV>{{"a", 20}, {"b", 50}}));
}

TEST(ColumnarKeyedStringAggregate, MinSeedsFromFirstValueNotZeroSentinel) {
    auto [col, row] = run_both_paths(AggKind::Min, {{"a", -5}, {"a", -10}, {"a", -3}});
    EXPECT_EQ(col, row);
    EXPECT_EQ(col, (std::vector<KV>{{"a", -10}})) << "all-negative min must be -10, not 0";
}

TEST(ColumnarKeyedStringAggregate, SumFoldsAcrossMultipleColumnarBatches) {
    ColumnarKeyedStringAggregateOperator op(AggKind::Sum);
    Capture cap;
    auto em = cap.emitter();
    const auto before = materialize_count();
    op.process_columnar(StreamElement<KV>::data(make_columnar_strkv({{"a", 10}, {"b", 20}})), em);
    op.process_columnar(StreamElement<KV>::data(make_columnar_strkv({{"a", 100}, {"c", 7}})), em);
    op.process_columnar(StreamElement<KV>::data(make_columnar_strkv({{"b", 2}, {"a", 1}})), em);
    op.flush(em);
    EXPECT_EQ(materialize_count() - before, 0u) << "all three batches ingested columnar";
    EXPECT_EQ(cap.totals(), (std::vector<KV>{{"a", 111}, {"b", 22}, {"c", 7}}));
}

TEST(ColumnarKeyedStringAggregate, ProcessColumnarRejectsRowBatchWithoutTouchingAccumulator) {
    ColumnarKeyedStringAggregateOperator op;
    Capture cap;
    auto em = cap.emitter();
    Batch<KV> row_batch;  // pure row batch, no sidecar
    row_batch.emplace(KV{"a", 999});
    EXPECT_FALSE(op.process_columnar(StreamElement<KV>::data(std::move(row_batch)), em));
    op.flush(em);
    EXPECT_TRUE(cap.totals().empty()) << "rejected batch must not have been aggregated";
}

class StringPairCollectingSink final : public Sink<KV> {
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

TEST(ColumnarKeyedStringAggregate, EndToEndColumnarPipelineDecodesZeroRowsOnIngest) {
    std::vector<KV> input;
    std::vector<std::int64_t> expected(10, 0);
    for (std::int64_t i = 0; i < 1000; ++i) {
        const std::int64_t k = i % 10;
        input.emplace_back("key_label_" + std::to_string(k), i);
        expected[static_cast<std::size_t>(k)] += i;
    }
    auto sink = std::make_shared<StringPairCollectingSink>();
    Dag dag;
    auto src = std::make_shared<ColumnarStringKeyedVectorSource>(input, /*batch_size=*/128);
    auto op = std::make_shared<ColumnarKeyedStringAggregateOperator>(AggKind::Sum);
    auto h0 = dag.add_source<KV>(src);
    auto h1 = dag.add_operator<KV, KV>(h0, op);
    dag.add_sink<KV>(h1, sink);

    const auto before = materialize_count();
    JobConfig cfg;
    LocalExecutor exec(std::move(dag), cfg);
    exec.run();

    EXPECT_EQ(materialize_count() - before, 0u)
        << "columnar string-keyed agg must decode zero rows on ingest";
    ASSERT_EQ(sink->received.size(), 10u);
    std::sort(sink->received.begin(), sink->received.end());
    for (std::int64_t k = 0; k < 10; ++k) {
        EXPECT_EQ(sink->received[static_cast<std::size_t>(k)],
                  (KV{"key_label_" + std::to_string(k), expected[static_cast<std::size_t>(k)]}));
    }
}
