// ParquetFsSink2PC<T> tests: the Arrow-filesystem two-phase-commit Parquet sink,
// exercised over arrow::fs::LocalFileSystem against an InMemoryStateBackend (no
// cluster, no cloud). Covers the staging -> committed promotion, pre-commit
// without commit, crash recovery (a fresh sink commits pending state on open),
// abort, commit idempotency, and a read-back of the committed output through
// MultiObjectParquetSource (the shared seam the object-store reader uses).

#ifndef CLINK_HAS_PARQUET
#error "test_parquet_fs_2pc_sink requires Parquet (ships with Arrow)"
#endif

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include <arrow/filesystem/localfs.h>
#include <gtest/gtest.h>

#include "clink/connectors/multi_object_parquet_source.hpp"
#include "clink/connectors/parquet_fs_2pc_sink.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;

namespace {

std::filesystem::path make_temp_dir(const std::string& tag) {
    static std::mt19937_64 rng{std::random_device{}()};
    auto p = std::filesystem::temp_directory_path() /
             ("clink_fs2pc_" + tag + "_" + std::to_string(rng()));
    std::filesystem::create_directories(p);
    return p;
}

auto local_fs_factory() {
    return []() -> std::shared_ptr<arrow::fs::FileSystem> {
        return std::make_shared<arrow::fs::LocalFileSystem>();
    };
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

std::shared_ptr<ParquetFsSink2PC<std::int64_t>> make_sink(const std::filesystem::path& base,
                                                          RuntimeContext& rctx,
                                                          OperatorId id) {
    ParquetFsSink2PC<std::int64_t>::Options o;
    o.base = base.string();
    o.subtask_idx = 0;
    auto sink = std::make_shared<ParquetFsSink2PC<std::int64_t>>(
        local_fs_factory(), o, int64_arrow_batcher());
    sink->set_id(id);
    sink->attach_runtime(&rctx);
    return sink;
}

void feed(ParquetFsSink2PC<std::int64_t>& sink, const std::vector<std::int64_t>& vals) {
    Batch<std::int64_t> b;
    for (auto v : vals) {
        b.emplace(v);
    }
    sink.on_data(b);
}

std::vector<std::int64_t> read_committed(const std::filesystem::path& base) {
    MultiObjectParquetSource<std::int64_t>::Options o;
    o.prefix = (base / "committed").string();
    MultiObjectParquetSource<std::int64_t> src(local_fs_factory(), o, int64_arrow_batcher());
    std::vector<std::int64_t> out;
    Emitter<std::int64_t> em([&out](StreamElement<std::int64_t> e) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                out.push_back(r.value());
            }
        }
        return true;
    });
    src.open();
    while (src.produce(em)) {
    }
    src.close();
    std::sort(out.begin(), out.end());
    return out;
}

}  // namespace

TEST(ParquetFsSink2PC, CommitPromotesStagingToCommittedAndIsReadable) {
    const auto base = make_temp_dir("commit");
    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{42}, "fs2pc", &state, /*metrics=*/nullptr);
    auto sink = make_sink(base, rctx, OperatorId{42});

    sink->open();
    feed(*sink, {10, 20, 30});
    sink->on_barrier(CheckpointBarrier{CheckpointId{11}});

    // Pre-commit: staged, state tracks it, nothing committed yet.
    EXPECT_EQ(count_parquet(base / "staging"), 1u);
    EXPECT_EQ(count_parquet(base / "committed"), 0u);
    EXPECT_TRUE(state.get(OperatorId{42}, "_2pc_pending_sub0_11").has_value());

    sink->on_commit(11);

    // Post-commit: under committed/, staging gone, state cleared.
    EXPECT_EQ(count_parquet(base / "committed"), 1u);
    EXPECT_EQ(count_parquet(base / "staging"), 0u);
    EXPECT_FALSE(state.get(OperatorId{42}, "_2pc_pending_sub0_11").has_value());

    EXPECT_EQ(read_committed(base), (std::vector<std::int64_t>{10, 20, 30}));
    std::filesystem::remove_all(base);
}

TEST(ParquetFsSink2PC, BarrierWithoutCommitLeavesStagingAndState) {
    const auto base = make_temp_dir("pre");
    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{7}, "fs2pc", &state, nullptr);
    auto sink = make_sink(base, rctx, OperatorId{7});

    sink->open();
    feed(*sink, {1, 2});
    sink->on_barrier(CheckpointBarrier{CheckpointId{7}});

    EXPECT_EQ(count_parquet(base / "staging"), 1u);
    EXPECT_EQ(count_parquet(base / "committed"), 0u);
    EXPECT_TRUE(state.get(OperatorId{7}, "_2pc_pending_sub0_7").has_value());
    EXPECT_TRUE(read_committed(base).empty());
    std::filesystem::remove_all(base);
}

TEST(ParquetFsSink2PC, FreshSinkCommitsPendingOnOpen) {
    const auto base = make_temp_dir("recover");
    InMemoryStateBackend state;  // shared across the two sink lifetimes (survives the "crash")

    {
        RuntimeContext rctx(OperatorId{5}, "fs2pc", &state, nullptr);
        auto sink1 = make_sink(base, rctx, OperatorId{5});
        sink1->open();
        feed(*sink1, {100, 200});
        sink1->on_barrier(CheckpointBarrier{CheckpointId{5}});
        // "crash": never on_commit, never clean close.
    }
    EXPECT_EQ(count_parquet(base / "staging"), 1u);
    EXPECT_EQ(count_parquet(base / "committed"), 0u);

    {
        RuntimeContext rctx(OperatorId{5}, "fs2pc", &state, nullptr);
        auto sink2 = make_sink(base, rctx, OperatorId{5});
        sink2->open();  // recovery commits the pending staging object
    }
    EXPECT_EQ(count_parquet(base / "committed"), 1u);
    EXPECT_FALSE(state.get(OperatorId{5}, "_2pc_pending_sub0_5").has_value());
    EXPECT_EQ(read_committed(base), (std::vector<std::int64_t>{100, 200}));
    std::filesystem::remove_all(base);
}

TEST(ParquetFsSink2PC, AbortDeletesStagingAndClearsState) {
    const auto base = make_temp_dir("abort");
    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{9}, "fs2pc", &state, nullptr);
    auto sink = make_sink(base, rctx, OperatorId{9});

    sink->open();
    feed(*sink, {42});
    sink->on_barrier(CheckpointBarrier{CheckpointId{9}});
    EXPECT_EQ(count_parquet(base / "staging"), 1u);

    sink->on_abort(9);
    EXPECT_EQ(count_parquet(base / "staging"), 0u);
    EXPECT_EQ(count_parquet(base / "committed"), 0u);
    EXPECT_FALSE(state.get(OperatorId{9}, "_2pc_pending_sub0_9").has_value());
    std::filesystem::remove_all(base);
}

TEST(ParquetFsSink2PC, CommitIsIdempotent) {
    const auto base = make_temp_dir("idem");
    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{3}, "fs2pc", &state, nullptr);
    auto sink = make_sink(base, rctx, OperatorId{3});

    sink->open();
    feed(*sink, {7});
    sink->on_barrier(CheckpointBarrier{CheckpointId{3}});
    sink->on_commit(3);
    EXPECT_NO_THROW(sink->on_commit(3));  // a second (recovery-time) commit is a no-op
    EXPECT_EQ(count_parquet(base / "committed"), 1u);
    EXPECT_EQ(read_committed(base), (std::vector<std::int64_t>{7}));
    std::filesystem::remove_all(base);
}

TEST(ParquetFsSink2PC, TwoIntervalsEachCommitIndependently) {
    const auto base = make_temp_dir("two");
    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{1}, "fs2pc", &state, nullptr);
    auto sink = make_sink(base, rctx, OperatorId{1});

    sink->open();
    feed(*sink, {1, 2});
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    sink->on_commit(1);
    feed(*sink, {3, 4});
    sink->on_barrier(CheckpointBarrier{CheckpointId{2}});
    sink->on_commit(2);

    EXPECT_EQ(count_parquet(base / "committed"), 2u);
    EXPECT_EQ(read_committed(base), (std::vector<std::int64_t>{1, 2, 3, 4}));
    std::filesystem::remove_all(base);
}
