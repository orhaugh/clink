// SubtaskEmitter routes records to N parallel downstream subtask
// channels. These tests cover all three routing modes (forward, hash
// partition, fan-in), plus the metadata-broadcast paths for watermarks
// and barriers, and the late-binding attach() pathway used by the DAG
// builder.

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "clink/core/arrow_batcher.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/subtask_emitter.hpp"

using namespace clink;

namespace {

// A columnar Batch<int64> (Arrow sidecar set, rows lazily materialized) for the
// columnar-partition test.
Batch<std::int64_t> make_columnar_i64(const std::vector<std::int64_t>& vals) {
    auto batcher = int64_arrow_batcher();
    arrow::Int64Builder tb;
    arrow::Int64Builder vb;
    for (auto v : vals) {
        (void)tb.AppendNull();
        (void)vb.Append(v);
    }
    std::shared_ptr<arrow::Array> ta;
    std::shared_ptr<arrow::Array> va;
    (void)tb.Finish(&ta);
    (void)vb.Finish(&va);
    auto rb = arrow::RecordBatch::Make(
        batcher.schema(), static_cast<std::int64_t>(vals.size()), {ta, va});
    auto parse = batcher.parse;
    Batch<std::int64_t>::MaterializeFn mat =
        [parse](const arrow::RecordBatch& b) -> std::vector<Record<std::int64_t>> {
        auto x = parse(b);
        return x ? x->take_records() : std::vector<Record<std::int64_t>>{};
    };
    return Batch<std::int64_t>{std::move(rb), vals.size(), std::move(mat)};
}

std::vector<std::int64_t> collect_values(const std::vector<StreamElement<std::int64_t>>& elems) {
    std::vector<std::int64_t> v;
    for (const auto& e : elems) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                v.push_back(r.value());
            }
        }
    }
    return v;
}

template <typename T>
std::vector<std::shared_ptr<BoundedChannel<StreamElement<T>>>> make_outputs(std::size_t n,
                                                                            std::size_t cap = 64) {
    std::vector<std::shared_ptr<BoundedChannel<StreamElement<T>>>> out;
    out.reserve(n);
    for (std::size_t i = 0; i < n; ++i) {
        out.push_back(std::make_shared<BoundedChannel<StreamElement<T>>>(cap));
    }
    return out;
}

template <typename T>
std::vector<StreamElement<T>> drain(BoundedChannel<StreamElement<T>>& ch) {
    std::vector<StreamElement<T>> out;
    while (auto e = ch.try_pop()) {
        out.push_back(std::move(*e));
    }
    return out;
}

}  // namespace

TEST(SubtaskEmitter, ForwardRoutingToSingleOutput) {
    auto outputs = make_outputs<int>(1);
    SubtaskEmitter<int> em(outputs);

    Batch<int> b;
    b.emplace(1);
    b.emplace(2);
    b.emplace(3);
    EXPECT_TRUE(em.emit_data(std::move(b)));

    auto elems = drain(*outputs[0]);
    ASSERT_EQ(elems.size(), 1u);
    ASSERT_TRUE(elems[0].is_data());
    EXPECT_EQ(elems[0].as_data().size(), 3u);
}

TEST(SubtaskEmitter, HashPartitionRoutesToCorrectOutput) {
    auto outputs = make_outputs<int>(3);
    SubtaskEmitter<int> em(outputs, [](const int& v) { return static_cast<std::size_t>(v); });

    Batch<int> b;
    for (int i = 0; i < 9; ++i) {
        b.emplace(i);
    }
    EXPECT_TRUE(em.emit_data(std::move(b)));

    // values 0,3,6 -> output 0; 1,4,7 -> output 1; 2,5,8 -> output 2
    for (std::size_t i = 0; i < 3; ++i) {
        auto elems = drain(*outputs[i]);
        ASSERT_EQ(elems.size(), 1u);
        ASSERT_TRUE(elems[0].is_data());
        const auto& batch = elems[0].as_data();
        EXPECT_EQ(batch.size(), 3u);
        for (const auto& r : batch) {
            EXPECT_EQ(static_cast<std::size_t>(r.value()) % 3, i);
        }
    }
}

TEST(SubtaskEmitter, FanInIsHashPartitionWithOneChannel) {
    auto outputs = make_outputs<int>(1);
    SubtaskEmitter<int> em(outputs, [](const int&) { return std::size_t{99}; });

    Batch<int> b;
    b.emplace(1);
    b.emplace(2);
    em.emit_data(std::move(b));

    auto elems = drain(*outputs[0]);
    ASSERT_EQ(elems.size(), 1u);
    EXPECT_EQ(elems[0].as_data().size(), 2u);
}

TEST(SubtaskEmitter, EmptyBatchIsHandledGracefully) {
    auto outputs = make_outputs<int>(2);
    SubtaskEmitter<int> em(outputs, [](const int& v) { return static_cast<std::size_t>(v); });

    Batch<int> empty;
    EXPECT_TRUE(em.emit_data(std::move(empty)));
    EXPECT_TRUE(drain(*outputs[0]).empty());
    EXPECT_TRUE(drain(*outputs[1]).empty());
}

TEST(SubtaskEmitter, NoOutputsIsNoOpButReturnsTrue) {
    SubtaskEmitter<int> em;  // default ctor, zero outputs
    Batch<int> b;
    b.emplace(1);
    EXPECT_TRUE(em.emit_data(std::move(b)));
    EXPECT_TRUE(em.emit_watermark(Watermark{EventTime{42}}));
    EXPECT_TRUE(em.emit_barrier(CheckpointBarrier{CheckpointId{1}}));
    EXPECT_EQ(em.output_count(), 0u);
}

TEST(SubtaskEmitter, WatermarkBroadcastsToEveryOutput) {
    auto outputs = make_outputs<int>(3);
    SubtaskEmitter<int> em(outputs);
    EXPECT_TRUE(em.emit_watermark(Watermark{EventTime{100}}));

    for (auto& out : outputs) {
        auto elems = drain(*out);
        ASSERT_EQ(elems.size(), 1u);
        ASSERT_TRUE(elems[0].is_watermark());
        EXPECT_EQ(elems[0].as_watermark().timestamp().millis(), 100);
    }
}

TEST(SubtaskEmitter, BarrierBroadcastsToEveryOutput) {
    auto outputs = make_outputs<int>(3);
    SubtaskEmitter<int> em(outputs);
    EXPECT_TRUE(em.emit_barrier(CheckpointBarrier{CheckpointId{42}}));

    for (auto& out : outputs) {
        auto elems = drain(*out);
        ASSERT_EQ(elems.size(), 1u);
        ASSERT_TRUE(elems[0].is_barrier());
        EXPECT_EQ(elems[0].as_barrier().id(), CheckpointId{42});
    }
}

TEST(SubtaskEmitter, AttachLateBindsOutputs) {
    SubtaskEmitter<int> em;
    EXPECT_EQ(em.output_count(), 0u);

    auto outputs = make_outputs<int>(2);
    em.attach(outputs, [](const int& v) { return static_cast<std::size_t>(v); });
    EXPECT_EQ(em.output_count(), 2u);

    Batch<int> b;
    b.emplace(0);  // -> output 0
    b.emplace(1);  // -> output 1
    em.emit_data(std::move(b));

    EXPECT_EQ(drain(*outputs[0]).size(), 1u);
    EXPECT_EQ(drain(*outputs[1]).size(), 1u);
}

TEST(SubtaskEmitter, CloseAllPropagatesToEveryChannel) {
    auto outputs = make_outputs<int>(3);
    SubtaskEmitter<int> em(outputs);
    em.close_all();

    for (auto& out : outputs) {
        // Pop on a closed empty channel returns nullopt.
        EXPECT_FALSE(out->pop().has_value());
    }
}

TEST(SubtaskEmitter, SingleOutputConvenienceCtor) {
    auto out = std::make_shared<BoundedChannel<StreamElement<int>>>(64);
    SubtaskEmitter<int> em(out);
    EXPECT_EQ(em.output_count(), 1u);

    Batch<int> b;
    b.emplace(99);
    em.emit_data(std::move(b));

    auto elems = drain(*out);
    ASSERT_EQ(elems.size(), 1u);
    EXPECT_EQ(elems[0].as_data()[0].value(), 99);
}

// ----- Type-instantiation diversity -----
//
// gcov reports each template instantiation as a separate function;
// instantiating SubtaskEmitter with a few extra types lifts function
// coverage by exercising those instantiations end-to-end.

TEST(SubtaskEmitter, WorksWithStringPayload) {
    auto outputs = make_outputs<std::string>(2);
    SubtaskEmitter<std::string> em(outputs, [](const std::string& s) { return s.size(); });

    Batch<std::string> b;
    b.emplace(std::string{"a"});      // size 1 -> output 1
    b.emplace(std::string{"hello"});  // size 5 -> output 1
    b.emplace(std::string{"hi"});     // size 2 -> output 0
    em.emit_data(std::move(b));
    em.emit_watermark(Watermark{EventTime{0}});
    em.emit_barrier(CheckpointBarrier{CheckpointId{1}});
    em.close_all();
    SUCCEED();
}

TEST(SubtaskEmitter, WorksWithPairPayload) {
    using PairT = std::pair<std::int64_t, std::string>;
    auto outputs = make_outputs<PairT>(2);
    SubtaskEmitter<PairT> em(outputs,
                             [](const PairT& p) { return static_cast<std::size_t>(p.first); });

    Batch<PairT> b;
    b.emplace(PairT{0, "zero"});
    b.emplace(PairT{1, "one"});
    em.emit_data(std::move(b));
    em.emit_watermark(Watermark{EventTime{0}});
    em.emit_barrier(CheckpointBarrier{CheckpointId{1}});
    em.close_all();
    SUCCEED();
}

TEST(SubtaskEmitter, ColumnarHashPartitionKeepsColumnarAndRoutesIdentically) {
    const std::vector<std::int64_t> vals = {0, 1, 2, 3, 4, 5, 6, 7, 8};
    auto partitioner = [](const std::int64_t& v) { return static_cast<std::size_t>(v); };

    // Columnar input -> per-subtask columnar sub-batches.
    auto col_out = make_outputs<std::int64_t>(3);
    SubtaskEmitter<std::int64_t> col_em(col_out, partitioner);
    EXPECT_TRUE(col_em.emit_data(make_columnar_i64(vals)));

    // Row input through the same partitioner is the routing reference.
    auto row_out = make_outputs<std::int64_t>(3);
    SubtaskEmitter<std::int64_t> row_em(row_out, partitioner);
    Batch<std::int64_t> rb;
    for (auto v : vals) {
        rb.emplace(v);
    }
    EXPECT_TRUE(row_em.emit_data(std::move(rb)));

    for (std::size_t t = 0; t < 3; ++t) {
        auto col_elems = drain(*col_out[t]);
        for (const auto& e : col_elems) {
            if (e.is_data()) {
                EXPECT_TRUE(e.as_data().is_columnar())
                    << "shuffle sub-batch must stay columnar for subtask " << t;
            }
        }
        EXPECT_EQ(collect_values(col_elems), collect_values(drain(*row_out[t])))
            << "columnar routing must match row routing for subtask " << t;
    }
}
