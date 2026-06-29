// MultiObjectParquetSource<T> tests: read every Parquet object under a prefix on
// an Arrow filesystem, shard the object list across subtasks, validate per-file
// schema, and replay across files. Uses arrow::fs::LocalFileSystem (always in
// libarrow), so it runs in CI with no S3/GCS/Azure infrastructure: the same
// seam the cloud connectors instantiate with their own filesystem.

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include <arrow/filesystem/localfs.h>
#include <gtest/gtest.h>

#include "clink/connectors/multi_object_parquet_source.hpp"
#include "clink/connectors/parquet_sink.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;

namespace {

std::filesystem::path make_temp_dir(const std::string& tag) {
    static std::mt19937_64 rng{std::random_device{}()};
    auto p = std::filesystem::temp_directory_path() /
             ("clink_multiparquet_" + tag + "_" + std::to_string(rng()));
    std::filesystem::create_directories(p);
    return p;
}

// Write one int64 Parquet file containing the given values.
void write_int64_file(const std::filesystem::path& path, const std::vector<std::int64_t>& vals) {
    ParquetSink<std::int64_t> sink(path, int64_arrow_batcher());
    sink.open();
    Batch<std::int64_t> b;
    for (auto v : vals) {
        b.emplace(v);
    }
    sink.on_data(b);
    sink.close();
}

auto local_fs_factory() {
    return []() -> std::shared_ptr<arrow::fs::FileSystem> {
        return std::make_shared<arrow::fs::LocalFileSystem>();
    };
}

// Drain a source to a flat, sorted vector of its int64 values.
std::vector<std::int64_t> drain(MultiObjectParquetSource<std::int64_t>& src) {
    std::vector<std::int64_t> out;
    Emitter<std::int64_t> em([&out](StreamElement<std::int64_t> e) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                out.push_back(r.value());
            }
        }
        return true;
    });
    while (src.produce(em)) {
    }
    std::sort(out.begin(), out.end());
    return out;
}

MultiObjectParquetSource<std::int64_t>::Options dir_opts(const std::filesystem::path& dir,
                                                         int subtask,
                                                         int par) {
    MultiObjectParquetSource<std::int64_t>::Options o;
    o.prefix = dir.string();
    o.subtask_idx = subtask;
    o.parallelism = par;
    return o;
}

}  // namespace

TEST(MultiObjectParquetSource, ReadsEveryObjectUnderPrefix) {
    auto dir = make_temp_dir("readall");
    write_int64_file(dir / "a.parquet", {1, 2, 3});
    write_int64_file(dir / "b.parquet", {4, 5});
    write_int64_file(dir / "c.parquet", {6, 7, 8, 9});

    MultiObjectParquetSource<std::int64_t> src(
        local_fs_factory(), dir_opts(dir, 0, 1), int64_arrow_batcher());
    src.open();
    auto got = drain(src);
    src.close();

    EXPECT_EQ(got, (std::vector<std::int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9}));
}

TEST(MultiObjectParquetSource, NonParquetFilesAreIgnored) {
    auto dir = make_temp_dir("filter");
    write_int64_file(dir / "data.parquet", {11, 22});
    std::ofstream(dir / "_SUCCESS") << "marker";      // not .parquet
    std::ofstream(dir / "notes.txt") << "ignore me";  // not .parquet

    MultiObjectParquetSource<std::int64_t> src(
        local_fs_factory(), dir_opts(dir, 0, 1), int64_arrow_batcher());
    src.open();
    auto got = drain(src);
    src.close();
    EXPECT_EQ(got, (std::vector<std::int64_t>{11, 22}));
}

TEST(MultiObjectParquetSource, ShardsDisjointlyAndCompletelyAcrossSubtasks) {
    auto dir = make_temp_dir("shard");
    // Five files; each subtask of a parallelism-3 source reads a disjoint subset,
    // and the union over all subtasks must equal the whole dataset.
    write_int64_file(dir / "f0.parquet", {0});
    write_int64_file(dir / "f1.parquet", {10});
    write_int64_file(dir / "f2.parquet", {20});
    write_int64_file(dir / "f3.parquet", {30});
    write_int64_file(dir / "f4.parquet", {40});

    std::vector<std::int64_t> all;
    for (int s = 0; s < 3; ++s) {
        MultiObjectParquetSource<std::int64_t> src(
            local_fs_factory(), dir_opts(dir, s, 3), int64_arrow_batcher());
        src.open();
        auto part = drain(src);
        src.close();
        all.insert(all.end(), part.begin(), part.end());
    }
    std::sort(all.begin(), all.end());
    EXPECT_EQ(all, (std::vector<std::int64_t>{0, 10, 20, 30, 40}));
    EXPECT_EQ(all.size(), 5u);  // disjoint: no object read by two subtasks
}

TEST(MultiObjectParquetSource, SchemaMismatchThrows) {
    auto dir = make_temp_dir("schema");
    // A string-typed Parquet file read with the int64 batcher must throw at open().
    {
        ParquetSink<std::string> sink(dir / "s.parquet", string_arrow_batcher());
        sink.open();
        Batch<std::string> b;
        b.emplace(std::string{"x"});
        sink.on_data(b);
        sink.close();
    }
    MultiObjectParquetSource<std::int64_t> src(
        local_fs_factory(), dir_opts(dir, 0, 1), int64_arrow_batcher());
    EXPECT_THROW(src.open(), std::runtime_error);
    EXPECT_NO_THROW(src.close());
}

TEST(MultiObjectParquetSource, EmptyPrefixYieldsNoRecordsButRequireMatchThrows) {
    auto dir = make_temp_dir("empty");  // created, but no parquet files

    MultiObjectParquetSource<std::int64_t> lenient(
        local_fs_factory(), dir_opts(dir, 0, 1), int64_arrow_batcher());
    lenient.open();
    EXPECT_TRUE(drain(lenient).empty());
    lenient.close();

    auto strict_opts = dir_opts(dir, 0, 1);
    strict_opts.require_match = true;
    MultiObjectParquetSource<std::int64_t> strict(
        local_fs_factory(), strict_opts, int64_arrow_batcher());
    EXPECT_THROW(strict.open(), std::runtime_error);
}

TEST(MultiObjectParquetSource, ReplayResumesAcrossFilesWithoutLossOrDuplication) {
    auto dir = make_temp_dir("replay");
    // One row per file so each produce() crosses a file boundary; replay must
    // resume mid-sequence.
    for (int i = 0; i < 6; ++i) {
        write_int64_file(dir / ("p" + std::to_string(i) + ".parquet"), {i * 100});
    }
    InMemoryStateBackend backend;
    const OperatorId op_id{42};

    // First run: emit 3 batches, then snapshot the offset.
    std::vector<std::int64_t> first;
    {
        MultiObjectParquetSource<std::int64_t> src(
            local_fs_factory(), dir_opts(dir, 0, 1), int64_arrow_batcher());
        src.open();
        int emitted = 0;
        Emitter<std::int64_t> em([&](StreamElement<std::int64_t> e) {
            if (e.is_data()) {
                for (const auto& r : e.as_data()) {
                    first.push_back(r.value());
                }
            }
            return true;
        });
        while (emitted < 3 && src.produce(em)) {
            ++emitted;
        }
        src.snapshot_offset(backend, op_id, CheckpointId{1});
        src.close();
    }
    ASSERT_EQ(first.size(), 3u);

    // Second run: restore the offset, then drain the rest.
    std::vector<std::int64_t> second;
    {
        MultiObjectParquetSource<std::int64_t> src(
            local_fs_factory(), dir_opts(dir, 0, 1), int64_arrow_batcher());
        ASSERT_TRUE(src.restore_offset(backend, op_id));
        src.open();
        second = drain(src);
        src.close();
    }

    // The two runs together cover every value exactly once.
    std::vector<std::int64_t> all = first;
    all.insert(all.end(), second.begin(), second.end());
    std::sort(all.begin(), all.end());
    EXPECT_EQ(all, (std::vector<std::int64_t>{0, 100, 200, 300, 400, 500}));
}
