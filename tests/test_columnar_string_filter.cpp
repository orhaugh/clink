// Columnar-native execution, increment 7a: ColumnarStringFilterOperator over
// the {event_time, value:utf8} Arrow sidecar.
//
// The load-bearing claims under test:
//   - the columnar fast path filters by prefix AND constructs zero std::strings
//     (batch_materialize_counter stays put while the operator runs),
//   - the columnar result is byte-identical to the row path,
//   - a row batch handed to process_columnar signals fallback (returns false),
//   - a columnar batch fed to the row path lazily materializes exactly once,
//   - an end-to-end ColumnarStringVectorSource -> ColumnarStringFilterOperator
//     -> columnar sink pipeline decodes zero rows.

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "clink/core/arrow_batcher.hpp"
#include "clink/core/record.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/operators/columnar_string_filter_operator.hpp"
#include "clink/operators/columnar_string_vector_source.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"

namespace {

using namespace clink;
using S = std::string;

std::uint64_t materialize_count() {
    return detail::batch_materialize_counter().load(std::memory_order_relaxed);
}

// Build a columnar Batch<std::string> directly from values (no Record rows).
Batch<S> make_columnar_str(const std::vector<S>& vals) {
    auto batcher = string_arrow_batcher();
    arrow::Int64Builder tb;
    arrow::StringBuilder vb;
    for (const auto& v : vals) {
        (void)tb.AppendNull();  // event_time null
        (void)vb.Append(v);
    }
    std::shared_ptr<arrow::Array> ta;
    std::shared_ptr<arrow::Array> va;
    (void)tb.Finish(&ta);
    (void)vb.Finish(&va);
    auto rb = arrow::RecordBatch::Make(
        batcher.schema(), static_cast<std::int64_t>(vals.size()), {ta, va});
    auto parse = batcher.parse;
    Batch<S>::MaterializeFn mat = [parse](const arrow::RecordBatch& b) -> std::vector<Record<S>> {
        auto x = parse(b);
        return x ? x->take_records() : std::vector<Record<S>>{};
    };
    return Batch<S>{std::move(rb), vals.size(), std::move(mat)};
}

struct Capture {
    std::vector<StreamElement<S>> elems;
    Emitter<S> emitter() {
        return Emitter<S>([this](StreamElement<S> e) {
            elems.push_back(std::move(e));
            return true;
        });
    }
    std::vector<S> values() {
        std::vector<S> out;
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

}  // namespace

TEST(ColumnarStringFilter, FastPathFiltersByPrefixAndDecodesZeroRows) {
    ColumnarStringFilterOperator op("ab");
    Capture cap;
    auto em = cap.emitter();
    const auto before = materialize_count();

    const bool handled = op.process_columnar(
        StreamElement<S>::data(make_columnar_str({"abc", "axe", "abz", "b", "ab"})), em);
    EXPECT_TRUE(handled);
    EXPECT_EQ(materialize_count() - before, 0u) << "columnar string scan must not decode rows";
    ASSERT_EQ(cap.elems.size(), 1u);
    EXPECT_TRUE(cap.elems[0].as_data().is_columnar()) << "output should stay columnar";
    // "abc", "abz", "ab" start with "ab"; "axe" and "b" do not.
    EXPECT_EQ(cap.values(), (std::vector<S>{"abc", "abz", "ab"}));
}

TEST(ColumnarStringFilter, ColumnarAndRowPathsAgree) {
    const std::vector<S> in = {"abc", "axe", "abz", "b", "ab", "aardvark", "abba"};
    ColumnarStringFilterOperator col("ab");
    Capture col_cap;
    auto col_em = col_cap.emitter();
    col.process_columnar(StreamElement<S>::data(make_columnar_str(in)), col_em);

    ColumnarStringFilterOperator row("ab");
    Capture row_cap;
    auto row_em = row_cap.emitter();
    row.process(StreamElement<S>::data(make_columnar_str(in)), row_em);

    EXPECT_EQ(col_cap.values(), row_cap.values())
        << "columnar string filter must be byte-identical to the row path";
    EXPECT_EQ(col_cap.values(), (std::vector<S>{"abc", "abz", "ab", "abba"}));
}

TEST(ColumnarStringFilter, ProcessColumnarRejectsRowBatch) {
    ColumnarStringFilterOperator op("ab");
    Capture cap;
    auto em = cap.emitter();
    Batch<S> row_batch;  // pure row batch, no sidecar
    row_batch.emplace("abc");
    EXPECT_FALSE(op.process_columnar(StreamElement<S>::data(std::move(row_batch)), em))
        << "a row batch must signal fallback to process()";
}

TEST(ColumnarStringFilter, ColumnarBatchLazilyMaterializesForRowPath) {
    // enable... the operator's own row path (process()) materializes the
    // columnar sidecar exactly once.
    ColumnarStringFilterOperator op("ab");
    Capture cap;
    auto em = cap.emitter();
    const auto before = materialize_count();
    op.process(StreamElement<S>::data(make_columnar_str({"abc", "x", "abz"})), em);
    EXPECT_EQ(materialize_count() - before, 1u) << "row path decodes the sidecar once";
    EXPECT_EQ(cap.values(), (std::vector<S>{"abc", "abz"}));
}

// A columnar-aware sink: reads the value column straight from the Arrow sidecar
// (no row decode) when present.
class StringCollectingSink final : public Sink<S> {
public:
    using Sink<S>::on_data;
    void on_data(const Batch<S>& batch) override {
        if (batch.is_columnar() && batch.arrow()) {
            const auto* v = static_cast<const arrow::StringArray*>(batch.arrow()->column(1).get());
            for (std::int64_t i = 0; i < v->length(); ++i) {
                received.emplace_back(v->GetView(i));
            }
        } else {
            for (const auto& r : batch) {
                received.push_back(r.value());
            }
        }
    }
    std::string name() const override { return "string_collect"; }
    std::vector<S> received;
};

TEST(ColumnarStringFilter, EndToEndColumnarPipelineDecodesZeroRows) {
    // 1000 strings; keep those starting with "k7".
    std::vector<S> input;
    std::size_t expected = 0;
    for (int i = 0; i < 1000; ++i) {
        S s = "k" + std::to_string(i % 100);  // k0..k99
        if (s.rfind("k7", 0) == 0) {
            ++expected;
        }
        input.push_back(std::move(s));
    }
    auto sink = std::make_shared<StringCollectingSink>();
    Dag dag;
    auto src = std::make_shared<ColumnarStringVectorSource>(input, /*batch_size=*/128);
    auto op = std::make_shared<ColumnarStringFilterOperator>("k7");
    auto h0 = dag.add_source<S>(src);
    auto h1 = dag.add_operator<S, S>(h0, op);
    dag.add_sink<S>(h1, sink);

    const auto before = materialize_count();
    JobConfig cfg;
    LocalExecutor exec(std::move(dag), cfg);
    exec.run();

    // k7, k70..k79 -> 11 distinct values, each appearing 10x in 1000 records.
    EXPECT_EQ(sink->received.size(), expected);
    EXPECT_EQ(materialize_count() - before, 0u)
        << "columnar string source -> filter -> columnar sink must decode zero rows";
    for (const auto& s : sink->received) {
        EXPECT_EQ(s.rfind("k7", 0), 0u) << "every kept value must start with k7";
    }
}
