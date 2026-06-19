// Columnar-native execution, increment 3: TumblingWindowOperator's columnar
// ingest fast path (process_columnar) over the 3-column {event_time, key,
// value} Arrow sidecar.
//
// The load-bearing claim: the columnar path is EXACTLY equivalent to the row
// path (same windows, same panes, same pane indices, same late re-fires) and
// only skips the Record<pair> decode. The tests prove this by feeding the SAME
// columnar batch + watermarks through two operator instances - one driven via
// process_columnar, one via process() - and asserting byte-identical panes,
// while the columnar arm decodes zero rows and the row arm decodes one per batch.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <tuple>
#include <vector>

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "clink/core/arrow_batcher.hpp"
#include "clink/core/record.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/operators/columnar_keyed_vector_source.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/operators/session_window_operator.hpp"
#include "clink/operators/sliding_window_operator.hpp"
#include "clink/operators/tumbling_window_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"

namespace {

using namespace clink;
using namespace std::chrono_literals;
using KV = std::pair<std::int64_t, std::int64_t>;
using Op = TumblingWindowOperator<std::int64_t, std::int64_t, std::int64_t>;

std::uint64_t materialize_count() {
    return detail::batch_materialize_counter().load(std::memory_order_relaxed);
}

Op make_sum_op() {
    return Op(
        1000ms,
        [] { return std::int64_t{0}; },
        [](std::int64_t a, std::int64_t v) { return a + v; });
}

// (event_time_ms, key, value) tuples -> a columnar Batch<KV> with the 3-column
// {event_time, key, value} sidecar. ts<0 means "null event time".
Batch<KV> make_columnar_kv_ts(
    const std::vector<std::tuple<std::int64_t, std::int64_t, std::int64_t>>& rows) {
    auto batcher = int64_keyed_arrow_batcher();
    arrow::Int64Builder tb;
    arrow::Int64Builder kb;
    arrow::Int64Builder vb;
    for (const auto& [ts, k, v] : rows) {
        if (ts < 0) {
            (void)tb.AppendNull();
        } else {
            (void)tb.Append(ts);
        }
        (void)kb.Append(k);
        (void)vb.Append(v);
    }
    std::shared_ptr<arrow::Array> ta;
    std::shared_ptr<arrow::Array> ka;
    std::shared_ptr<arrow::Array> va;
    (void)tb.Finish(&ta);
    (void)kb.Finish(&ka);
    (void)vb.Finish(&va);
    auto rb = arrow::RecordBatch::Make(
        batcher.schema(), static_cast<std::int64_t>(rows.size()), {ta, ka, va});
    auto parse = batcher.parse;
    Batch<KV>::MaterializeFn mat = [parse](const arrow::RecordBatch& b) -> std::vector<Record<KV>> {
        auto x = parse(b);
        return x ? x->take_records() : std::vector<Record<KV>>{};
    };
    return Batch<KV>{std::move(rb), rows.size(), std::move(mat)};
}

struct Pane {
    std::int64_t key{};
    std::int64_t agg{};
    std::int64_t end_ts{-1};
    std::int64_t pane_index{-1};
    bool is_first{false};
    auto operator<=>(const Pane&) const = default;
};

struct Capture {
    std::vector<StreamElement<KV>> elems;
    Emitter<KV> emitter() {
        return Emitter<KV>([this](StreamElement<KV> e) {
            elems.push_back(std::move(e));
            return true;
        });
    }
    std::vector<Pane> panes() {
        std::vector<Pane> out;
        for (auto& e : elems) {
            if (e.is_data()) {
                for (const auto& r : e.as_data()) {
                    Pane p;
                    p.key = r.value().first;
                    p.agg = r.value().second;
                    p.end_ts = r.event_time().has_value() ? r.event_time()->millis() : -1;
                    if (r.pane().has_value()) {
                        p.pane_index = r.pane()->pane_index;
                        p.is_first = r.pane()->is_first;
                    }
                    out.push_back(p);
                }
            }
        }
        std::sort(out.begin(), out.end());
        return out;
    }
};

template <typename O>
void watermark(O& op, Emitter<KV>& em, std::int64_t ts) {
    op.process(StreamElement<KV>::watermark(Watermark{EventTime{ts}}), em);
}

}  // namespace

// THE equivalence test: identical input through the columnar fast path and the
// row path must produce byte-identical panes; the columnar arm decodes 0 rows,
// the row arm decodes 1 (the batch).
TEST(ColumnarWindowAgg, ColumnarAndRowPathsProduceIdenticalPanes) {
    const std::vector<std::tuple<std::int64_t, std::int64_t, std::int64_t>> rows = {
        {100, 1, 10},   // window [0,1000) key 1
        {200, 1, 20},   // window [0,1000) key 1
        {1100, 1, 30},  // window [1000,2000) key 1
        {150, 2, 5},    // window [0,1000) key 2
        {1200, 2, 7},   // window [1000,2000) key 2
    };

    // Columnar arm.
    Op col = make_sum_op();
    Capture col_cap;
    auto col_em = col_cap.emitter();
    const auto before_col = materialize_count();
    ASSERT_TRUE(col.process_columnar(StreamElement<KV>::data(make_columnar_kv_ts(rows)), col_em));
    EXPECT_EQ(materialize_count() - before_col, 0u) << "columnar ingest must not decode rows";
    watermark(col, col_em, 2000);

    // Row arm (same batch via process(), which materializes).
    Op row = make_sum_op();
    Capture row_cap;
    auto row_em = row_cap.emitter();
    const auto before_row = materialize_count();
    row.process(StreamElement<KV>::data(make_columnar_kv_ts(rows)), row_em);
    EXPECT_EQ(materialize_count() - before_row, 1u) << "row path decodes the batch once";
    watermark(row, row_em, 2000);

    const auto col_panes = col_cap.panes();
    const auto row_panes = row_cap.panes();
    EXPECT_EQ(col_panes, row_panes) << "columnar path must be byte-identical to the row path";
    // Sanity on the actual values: [0,1000) k1=30 k2=5; [1000,2000) k1=30 k2=7.
    ASSERT_EQ(col_panes.size(), 4u);
    std::vector<std::pair<std::int64_t, std::int64_t>> kv_by_end;
    for (const auto& p : col_panes) {
        kv_by_end.emplace_back(p.end_ts, p.agg);
    }
    std::sort(kv_by_end.begin(), kv_by_end.end());
    EXPECT_EQ(kv_by_end,
              (std::vector<std::pair<std::int64_t, std::int64_t>>{
                  {999, 5}, {999, 30}, {1999, 7}, {1999, 30}}));
}

// A late record that re-fires an already-fired window must behave identically on
// both paths (same late pane, same continued pane_index).
TEST(ColumnarWindowAgg, LateRefireEquivalence) {
    Op col = make_sum_op();
    col.allowed_lateness(1000ms);
    Capture col_cap;
    auto col_em = col_cap.emitter();
    Op row = make_sum_op();
    row.allowed_lateness(1000ms);
    Capture row_cap;
    auto row_em = row_cap.emitter();

    // First batch: one record in [0,1000). Fire it on-time. Then a late record
    // (still within lateness) re-fires.
    col.process_columnar(StreamElement<KV>::data(make_columnar_kv_ts({{100, 1, 10}})), col_em);
    row.process(StreamElement<KV>::data(make_columnar_kv_ts({{100, 1, 10}})), row_em);
    watermark(col, col_em, 1000);  // on-time fire (window not yet purged: lateness 1000)
    watermark(row, row_em, 1000);
    col.process_columnar(StreamElement<KV>::data(make_columnar_kv_ts({{200, 1, 5}})), col_em);
    row.process(StreamElement<KV>::data(make_columnar_kv_ts({{200, 1, 5}})), row_em);

    EXPECT_EQ(col_cap.panes(), row_cap.panes())
        << "late re-fire must be byte-identical across paths";
    // Expect an on-time pane (agg 10, pane 0) and a late pane (agg 15, pane 1).
    const auto p = col_cap.panes();
    ASSERT_EQ(p.size(), 2u);
}

// Null event_time buckets at ts=0 (matching the row path's value_or(0)).
TEST(ColumnarWindowAgg, NullEventTimeBucketsAtZero) {
    Op col = make_sum_op();
    Capture cap;
    auto em = cap.emitter();
    // ts<0 in the helper => null event time.
    col.process_columnar(StreamElement<KV>::data(make_columnar_kv_ts({{-1, 1, 10}, {-1, 1, 20}})),
                         em);
    watermark(col, em, 1000);  // window [0,1000) fires
    const auto p = cap.panes();
    ASSERT_EQ(p.size(), 1u);
    EXPECT_EQ(p[0].key, 1);
    EXPECT_EQ(p[0].agg, 30);
    EXPECT_EQ(p[0].end_ts, 999);
}

// A non-int64 key/value window must NOT advertise columnar support (the
// if-constexpr guard) and degrades to the row path.
TEST(ColumnarWindowAgg, NonInt64KeyDoesNotSupportColumnar) {
    TumblingWindowOperator<std::string, std::int64_t, std::int64_t> op(
        1000ms,
        [] { return std::int64_t{0}; },
        [](std::int64_t a, std::int64_t v) { return a + v; });
    EXPECT_FALSE(op.supports_columnar());
}

// set_columnar_enabled(false) turns off the fast path (used by the bench).
TEST(ColumnarWindowAgg, ColumnarToggleControlsSupport) {
    Op op = make_sum_op();
    EXPECT_TRUE(op.supports_columnar());
    op.set_columnar_enabled(false);
    EXPECT_FALSE(op.supports_columnar());
}

// Collects (key, agg, end_ts) from window output rows.
class PaneCollectingSink final : public Sink<KV> {
public:
    using Sink<KV>::on_data;
    void on_data(const Batch<KV>& batch) override {
        for (const auto& r : batch) {
            received.emplace_back(r.value().first,
                                  r.value().second,
                                  r.event_time().has_value() ? r.event_time()->millis() : -1);
        }
    }
    std::string name() const override { return "pane_collect"; }
    std::vector<std::tuple<std::int64_t, std::int64_t, std::int64_t>> received;
};

// End-to-end: ColumnarKeyedVectorSource (with event times) -> columnar window
// op -> sink, through the real runner, decoding zero rows on the ingest path.
TEST(ColumnarWindowAgg, EndToEndColumnarWindowDecodesZeroRowsOnIngest) {
    // 1000 records, key = i%4, value = i, event_time spread across 1s windows
    // (10 records per window: ts = (i/10)*... no, simpler: ts = i so windows of
    // 1000ms hold 1000 ts each -> we want several windows, use ts = i so
    // [0,1000) holds i in [0,1000): all of them. Use ts = i*5 to span 5 windows.
    std::vector<KV> data;
    std::vector<std::int64_t> ts;
    for (std::int64_t i = 0; i < 1000; ++i) {
        data.emplace_back(i % 4, i);
        ts.push_back(i * 5);  // 0..4995 -> windows [0,1000)..[4000,5000)
    }
    auto sink = std::make_shared<PaneCollectingSink>();
    Dag dag;
    auto src = std::make_shared<ColumnarKeyedVectorSource>(data, /*batch_size=*/128, ts);
    auto op = std::make_shared<Op>(
        1000ms,
        [] { return std::int64_t{0}; },
        [](std::int64_t a, std::int64_t v) { return a + v; });
    auto h0 = dag.add_source<KV>(src);
    auto h1 = dag.add_operator<KV, KV>(h0, op);
    dag.add_sink<KV>(h1, sink);

    const auto before = materialize_count();
    JobConfig cfg;
    LocalExecutor exec(std::move(dag), cfg);
    exec.run();

    EXPECT_EQ(materialize_count() - before, 0u)
        << "columnar source -> columnar window must decode zero rows on ingest";
    // 5 windows x 4 keys = 20 (key, window) panes, each summing its 50 records.
    EXPECT_EQ(sink->received.size(), 20u);
    std::int64_t total = 0;
    for (const auto& [k, agg, end_ts] : sink->received) {
        total += agg;
    }
    // Sum of all values 0..999 = 999*1000/2 = 499500, partitioned across panes.
    EXPECT_EQ(total, 499500);
}

// ---- Sliding window (increment 4): each record fans into N overlapping
// windows. Same byte-identical-equivalence + zero-decode contract. ----

namespace {
using SlidingOp = SlidingWindowOperator<std::int64_t, std::int64_t, std::int64_t>;
}  // namespace

// THE sliding equivalence test: with size=2000, slide=1000 each record lands in
// 2 overlapping windows. Columnar and row paths must produce byte-identical
// panes; columnar decodes 0 rows, row decodes 1.
TEST(ColumnarSlidingWindowAgg, ColumnarAndRowPathsProduceIdenticalPanes) {
    const std::vector<std::tuple<std::int64_t, std::int64_t, std::int64_t>> rows = {
        {1500, 1, 10},  // windows [0,2000) and [1000,3000) key 1
        {1500, 2, 7},   // both windows key 2
        {2500, 1, 3},   // windows [1000,3000) and [2000,4000) key 1
    };

    SlidingOp col(
        2000ms,
        1000ms,
        [] { return std::int64_t{0}; },
        [](std::int64_t a, std::int64_t v) { return a + v; });
    Capture col_cap;
    auto col_em = col_cap.emitter();
    const auto before_col = materialize_count();
    ASSERT_TRUE(col.process_columnar(StreamElement<KV>::data(make_columnar_kv_ts(rows)), col_em));
    EXPECT_EQ(materialize_count() - before_col, 0u) << "columnar ingest must not decode rows";
    watermark(col, col_em, 4000);

    SlidingOp row(
        2000ms,
        1000ms,
        [] { return std::int64_t{0}; },
        [](std::int64_t a, std::int64_t v) { return a + v; });
    Capture row_cap;
    auto row_em = row_cap.emitter();
    const auto before_row = materialize_count();
    row.process(StreamElement<KV>::data(make_columnar_kv_ts(rows)), row_em);
    EXPECT_EQ(materialize_count() - before_row, 1u) << "row path decodes the batch once";
    watermark(row, row_em, 4000);

    EXPECT_EQ(col_cap.panes(), row_cap.panes())
        << "columnar sliding path must be byte-identical to the row path";
    // Spot-check: window [1000,3000) key 1 sees both records (10 + 3 = 13).
    bool found_overlap = false;
    for (const auto& p : col_cap.panes()) {
        if (p.key == 1 && p.end_ts == 2999) {  // window [1000,3000) max ts
            EXPECT_EQ(p.agg, 13);
            found_overlap = true;
        }
    }
    EXPECT_TRUE(found_overlap)
        << "overlapping window [1000,3000) key 1 must aggregate both records";
}

TEST(ColumnarSlidingWindowAgg, NonInt64KeyDoesNotSupportColumnar) {
    SlidingWindowOperator<std::string, std::int64_t, std::int64_t> op(
        2000ms,
        1000ms,
        [] { return std::int64_t{0}; },
        [](std::int64_t a, std::int64_t v) { return a + v; });
    EXPECT_FALSE(op.supports_columnar());
}

TEST(ColumnarSlidingWindowAgg, EndToEndColumnarSlidingDecodesZeroRowsOnIngest) {
    // size=2000, slide=1000: each record in 2 overlapping windows.
    std::vector<KV> data;
    std::vector<std::int64_t> ts;
    for (std::int64_t i = 0; i < 1000; ++i) {
        data.emplace_back(i % 4, i);
        ts.push_back(i * 5);  // 0..4995
    }
    auto sink = std::make_shared<PaneCollectingSink>();
    Dag dag;
    auto src = std::make_shared<ColumnarKeyedVectorSource>(data, /*batch_size=*/128, ts);
    auto op = std::make_shared<SlidingOp>(
        2000ms,
        1000ms,
        [] { return std::int64_t{0}; },
        [](std::int64_t a, std::int64_t v) { return a + v; });
    auto h0 = dag.add_source<KV>(src);
    auto h1 = dag.add_operator<KV, KV>(h0, op);
    dag.add_sink<KV>(h1, sink);

    const auto before = materialize_count();
    JobConfig cfg;
    LocalExecutor exec(std::move(dag), cfg);
    exec.run();

    EXPECT_EQ(materialize_count() - before, 0u)
        << "columnar source -> columnar sliding window must decode zero rows on ingest";
    // Each value lands in 2 windows, so the grand total double-counts the input:
    // 2 * (0+..+999) = 2 * 499500 = 999000.
    std::int64_t total = 0;
    for (const auto& [k, agg, end_ts] : sink->received) {
        total += agg;
    }
    EXPECT_EQ(total, 999000);
}

// ---- Session window (increment 5): the merge/create logic lives inside
// handle_record_, which both paths call via ingest_one_. Same byte-identical
// equivalence + zero-decode contract. ----

namespace {
using SessionOp = SessionWindowOperator<std::int64_t, std::int64_t, std::int64_t>;

SessionOp make_session_sum_op() {
    return SessionOp(
        1000ms,
        [] { return std::int64_t{0}; },
        [](std::int64_t a, std::int64_t v) { return a + v; },
        [](std::int64_t a, std::int64_t b) { return a + b; });
}
}  // namespace

// THE session equivalence test: records that MERGE sessions must merge
// identically on both paths. gap=1000: {100,500} for key 1 merge into one
// session [100,1500); {3000} is a separate session.
TEST(ColumnarSessionWindowAgg, ColumnarAndRowPathsProduceIdenticalPanes) {
    const std::vector<std::tuple<std::int64_t, std::int64_t, std::int64_t>> rows = {
        {100, 1, 10},  // session [100,1100) key 1
        {500, 1, 20},  // prov [500,1500) overlaps -> merge [100,1500) agg=30
        {3000, 1, 5},  // separate session [3000,4000) key 1
        {200, 2, 7},   // session [200,1200) key 2
    };

    SessionOp col = make_session_sum_op();
    Capture col_cap;
    auto col_em = col_cap.emitter();
    const auto before_col = materialize_count();
    ASSERT_TRUE(col.process_columnar(StreamElement<KV>::data(make_columnar_kv_ts(rows)), col_em));
    EXPECT_EQ(materialize_count() - before_col, 0u) << "columnar ingest must not decode rows";
    watermark(col, col_em, 5000);

    SessionOp row = make_session_sum_op();
    Capture row_cap;
    auto row_em = row_cap.emitter();
    const auto before_row = materialize_count();
    row.process(StreamElement<KV>::data(make_columnar_kv_ts(rows)), row_em);
    EXPECT_EQ(materialize_count() - before_row, 1u) << "row path decodes the batch once";
    watermark(row, row_em, 5000);

    EXPECT_EQ(col_cap.panes(), row_cap.panes())
        << "columnar session path must be byte-identical to the row path";
    // The merged session [100,1500) key 1 sums 10+20=30 (end_ts 1499).
    bool found_merged = false;
    for (const auto& p : col_cap.panes()) {
        if (p.key == 1 && p.end_ts == 1499) {
            EXPECT_EQ(p.agg, 30);
            found_merged = true;
        }
    }
    EXPECT_TRUE(found_merged) << "merged session [100,1500) key 1 must sum both records";
}

TEST(ColumnarSessionWindowAgg, NonInt64KeyDoesNotSupportColumnar) {
    SessionWindowOperator<std::string, std::int64_t, std::int64_t> op(
        1000ms,
        [] { return std::int64_t{0}; },
        [](std::int64_t a, std::int64_t v) { return a + v; },
        [](std::int64_t a, std::int64_t b) { return a + b; });
    EXPECT_FALSE(op.supports_columnar());
}

TEST(ColumnarSessionWindowAgg, EndToEndColumnarSessionDecodesZeroRowsOnIngest) {
    // 1000 records, key = i%4, event times 5ms apart (< gap 1000) so every
    // key's records chain-merge into a single session.
    std::vector<KV> data;
    std::vector<std::int64_t> ts;
    for (std::int64_t i = 0; i < 1000; ++i) {
        data.emplace_back(i % 4, i);
        ts.push_back(i * 5);
    }
    auto sink = std::make_shared<PaneCollectingSink>();
    Dag dag;
    auto src = std::make_shared<ColumnarKeyedVectorSource>(data, /*batch_size=*/128, ts);
    auto op = std::make_shared<SessionOp>(
        1000ms,
        [] { return std::int64_t{0}; },
        [](std::int64_t a, std::int64_t v) { return a + v; },
        [](std::int64_t a, std::int64_t b) { return a + b; });
    auto h0 = dag.add_source<KV>(src);
    auto h1 = dag.add_operator<KV, KV>(h0, op);
    dag.add_sink<KV>(h1, sink);

    const auto before = materialize_count();
    JobConfig cfg;
    LocalExecutor exec(std::move(dag), cfg);
    exec.run();

    EXPECT_EQ(materialize_count() - before, 0u)
        << "columnar source -> columnar session window must decode zero rows on ingest";
    // Each key's records chain-merge into ONE session; 4 keys -> 4 sessions.
    // No double-counting (each value folded into exactly one session): total =
    // sum(0..999) = 499500.
    EXPECT_EQ(sink->received.size(), 4u);
    std::int64_t total = 0;
    for (const auto& [k, agg, end_ts] : sink->received) {
        total += agg;
    }
    EXPECT_EQ(total, 499500);
}
