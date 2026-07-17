// 2PC columnar Parquet sink: staging -> committed atomic-rename protocol
// for a custom CLINK_ARROW_FIELDS struct, exercised directly against an
// InMemoryStateBackend (no cluster).
//
//   * commit moves the pre-committed file staging/ -> committed/ and the
//     committed file is a valid, externally-typed Parquet file;
//   * a barrier without commit leaves the file in staging/ + a state key;
//   * a fresh sink over the same dir+state commits it on open() (crash
//     recovery);
//   * abort deletes the staging file and clears state.

#ifndef CLINK_HAS_PARQUET
#error "test_parquet_2pc_sink requires Parquet (ships with Arrow)"
#endif

#include <cstdint>
#include <filesystem>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>

#include "clink/connectors/columnar_parquet.hpp"  // make_columnar_parquet_source
#include "clink/connectors/parquet_2pc_sink.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;

struct ColPq2pcTrade {
    std::int64_t id;
    std::string symbol;
    double price;
    std::int32_t qty;
    bool buy;
};

CLINK_ARROW_FIELDS(ColPq2pcTrade, id, symbol, price, qty, buy);

namespace {

std::filesystem::path tmp_dir(const std::string& tag) {
    static std::mt19937_64 rng{std::random_device{}()};
    auto p = std::filesystem::temp_directory_path() /
             ("clink_pq2pc_" + tag + "_" + std::to_string(rng()));
    std::filesystem::create_directories(p);
    return p;
}

std::size_t count_parquet(const std::filesystem::path& dir) {
    std::size_t n = 0;
    if (!std::filesystem::exists(dir)) {
        return 0;
    }
    for (const auto& e : std::filesystem::directory_iterator(dir)) {
        if (e.path().extension() == ".parquet") {
            ++n;
        }
    }
    return n;
}

bool trade_eq(const ColPq2pcTrade& a, const ColPq2pcTrade& b) {
    return a.id == b.id && a.symbol == b.symbol && a.price == b.price && a.qty == b.qty &&
           a.buy == b.buy;
}

template <typename T>
struct CapturedBatches {
    std::vector<Batch<T>> batches;
};

template <typename T>
Emitter<T> make_capturing_emitter(CapturedBatches<T>& sink) {
    return Emitter<T>{[&sink](StreamElement<T> e) {
        if (e.is_data()) {
            sink.batches.push_back(std::move(e.as_data()));
        }
        return true;
    }};
}

Batch<ColPq2pcTrade> sample_batch() {
    Batch<ColPq2pcTrade> b;
    b.emplace(ColPq2pcTrade{1, "AAPL", 191.25, 100, true}, EventTime{100});
    b.emplace(ColPq2pcTrade{2, "MSFT", 410.10, 50, false}, EventTime{200});
    b.emplace(ColPq2pcTrade{3, "NVDA", 1203.99, 7, true});  // no event-time
    return b;
}

const std::vector<ColPq2pcTrade> kExpected = {
    {1, "AAPL", 191.25, 100, true},
    {2, "MSFT", 410.10, 50, false},
    {3, "NVDA", 1203.99, 7, true},
};

// Read every record from a committed Parquet file via the columnar source.
std::vector<ColPq2pcTrade> read_committed(const std::filesystem::path& file) {
    auto source = make_columnar_parquet_source<ColPq2pcTrade>(file);
    source.open();
    CapturedBatches<ColPq2pcTrade> cap;
    auto em = make_capturing_emitter(cap);
    while (source.produce(em)) {
    }
    source.close();
    std::vector<ColPq2pcTrade> out;
    for (const auto& b : cap.batches) {
        for (const auto& r : b) {
            out.push_back(r.value());
        }
    }
    return out;
}

}  // namespace

TEST(ParquetSink2PC, CommitMovesStagingToCommittedAndIsExternallyTyped) {
    const auto dir = tmp_dir("commit");
    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{42}, "pq2pc", &state, /*metrics=*/nullptr);

    auto sink = std::make_shared<ParquetSink2PC<ColPq2pcTrade>>(
        dir, make_columnar_arrow_batcher<ColPq2pcTrade>(), /*subtask_idx=*/0);
    sink->set_id(OperatorId{42});
    sink->attach_runtime(&rctx);

    sink->open();
    sink->on_data(sample_batch());
    sink->on_barrier(CheckpointBarrier{CheckpointId{11}});

    // Pre-commit: file staged, state tracks it, nothing committed yet.
    EXPECT_EQ(count_parquet(dir / "staging"), 1u);
    EXPECT_EQ(count_parquet(dir / "committed"), 0u);
    EXPECT_TRUE(state.get_operator_state(OperatorId{42}, "_xo_pending_sub0_11").has_value());

    sink->on_commit(11);

    // Post-commit: file under committed/, staging file gone, state cleared.
    EXPECT_EQ(count_parquet(dir / "committed"), 1u);
    EXPECT_FALSE(state.get_operator_state(OperatorId{42}, "_xo_pending_sub0_11").has_value());

    const auto committed_file = dir / "committed" / "sub0-11.parquet";
    ASSERT_TRUE(std::filesystem::exists(committed_file));

    // Externally typed: open with parquet::arrow directly.
    auto in = arrow::io::ReadableFile::Open(committed_file.string());
    ASSERT_TRUE(in.ok());
    auto reader_result = parquet::arrow::OpenFile(*in, arrow::default_memory_pool());
    ASSERT_TRUE(reader_result.ok());
    std::shared_ptr<arrow::Schema> schema;
    ASSERT_TRUE((*reader_result)->GetSchema(&schema).ok());
    ASSERT_EQ(schema->num_fields(), 6);
    EXPECT_EQ(schema->field(1)->name(), "id");
    EXPECT_EQ(schema->field(1)->type()->id(), arrow::Type::INT64);
    EXPECT_EQ(schema->field(2)->type()->id(), arrow::Type::STRING);
    EXPECT_EQ(schema->field(5)->type()->id(), arrow::Type::BOOL);

    // Round-trip the data back.
    const auto got = read_committed(committed_file);
    ASSERT_EQ(got.size(), kExpected.size());
    for (std::size_t i = 0; i < kExpected.size(); ++i) {
        EXPECT_TRUE(trade_eq(got[i], kExpected[i]));
    }

    std::filesystem::remove_all(dir);
}

TEST(ParquetSink2PC, BarrierWithoutCommitLeavesStagingAndState) {
    const auto dir = tmp_dir("nocommit");
    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{42}, "pq2pc", &state, /*metrics=*/nullptr);

    auto sink = std::make_shared<ParquetSink2PC<ColPq2pcTrade>>(
        dir, make_columnar_arrow_batcher<ColPq2pcTrade>(), 0);
    sink->set_id(OperatorId{42});
    sink->attach_runtime(&rctx);

    sink->open();
    sink->on_data(sample_batch());
    sink->on_barrier(CheckpointBarrier{CheckpointId{7}});
    // No on_commit - simulate a crash before the coordinator declares ckpt 7 done.

    EXPECT_EQ(count_parquet(dir / "staging"), 1u);
    EXPECT_EQ(count_parquet(dir / "committed"), 0u);
    EXPECT_TRUE(state.get_operator_state(OperatorId{42}, "_xo_pending_sub0_7").has_value());

    std::filesystem::remove_all(dir);
}

TEST(ParquetSink2PC, FreshSinkRecoversPendingOnOpen) {
    const auto dir = tmp_dir("recover");
    InMemoryStateBackend state;  // survives the "crash" (shared across sinks)
    RuntimeContext rctx(OperatorId{42}, "pq2pc", &state, /*metrics=*/nullptr);

    {
        auto sink1 = std::make_shared<ParquetSink2PC<ColPq2pcTrade>>(
            dir, make_columnar_arrow_batcher<ColPq2pcTrade>(), 0);
        sink1->set_id(OperatorId{42});
        sink1->attach_runtime(&rctx);
        sink1->open();
        sink1->on_data(sample_batch());
        sink1->on_barrier(CheckpointBarrier{CheckpointId{5}});
        // sink1 dies here without on_commit - file stuck in staging/.
    }
    ASSERT_EQ(count_parquet(dir / "staging"), 1u);
    ASSERT_EQ(count_parquet(dir / "committed"), 0u);

    // Restart: a fresh sink over the same dir + state commits the orphan.
    auto sink2 = std::make_shared<ParquetSink2PC<ColPq2pcTrade>>(
        dir, make_columnar_arrow_batcher<ColPq2pcTrade>(), 0);
    sink2->set_id(OperatorId{42});
    sink2->attach_runtime(&rctx);
    sink2->open();  // recover_pending_ commits sub0-5.parquet

    EXPECT_EQ(count_parquet(dir / "committed"), 1u);
    EXPECT_FALSE(state.get_operator_state(OperatorId{42}, "_xo_pending_sub0_5").has_value());

    const auto got = read_committed(dir / "committed" / "sub0-5.parquet");
    ASSERT_EQ(got.size(), kExpected.size());
    for (std::size_t i = 0; i < kExpected.size(); ++i) {
        EXPECT_TRUE(trade_eq(got[i], kExpected[i]));
    }

    std::filesystem::remove_all(dir);
}

TEST(ParquetSink2PC, AbortRemovesStagingAndCommitIsThenNoop) {
    const auto dir = tmp_dir("abort");
    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{42}, "pq2pc", &state, /*metrics=*/nullptr);

    auto sink = std::make_shared<ParquetSink2PC<ColPq2pcTrade>>(
        dir, make_columnar_arrow_batcher<ColPq2pcTrade>(), 0);
    sink->set_id(OperatorId{42});
    sink->attach_runtime(&rctx);

    sink->open();
    sink->on_data(sample_batch());
    sink->on_barrier(CheckpointBarrier{CheckpointId{3}});
    ASSERT_EQ(count_parquet(dir / "staging"), 1u);

    sink->on_abort(3);
    EXPECT_EQ(count_parquet(dir / "staging"), 0u);
    EXPECT_EQ(count_parquet(dir / "committed"), 0u);
    EXPECT_FALSE(state.get_operator_state(OperatorId{42}, "_xo_pending_sub0_3").has_value());

    // Commit after abort is a no-op (nothing tracked in state).
    EXPECT_NO_THROW(sink->on_commit(3));
    EXPECT_EQ(count_parquet(dir / "committed"), 0u);

    std::filesystem::remove_all(dir);
}
