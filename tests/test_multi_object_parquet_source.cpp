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
#include <stdexcept>
#include <string>
#include <vector>

#include <arrow/filesystem/localfs.h>
#include <arrow/io/file.h>
#include <gtest/gtest.h>
#include <parquet/arrow/writer.h>

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

// Write a Parquet file the way another tool would: plain Arrow columns, with
// none of clink's own (no event_time).
void write_foreign_file(const std::filesystem::path& path,
                        const std::shared_ptr<arrow::Table>& table) {
    auto out = arrow::io::FileOutputStream::Open(path.string()).ValueOrDie();
    ASSERT_TRUE(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 1024).ok());
    ASSERT_TRUE(out->Close().ok());
}

std::shared_ptr<arrow::Array> int64_array(const std::vector<std::int64_t>& vals) {
    arrow::Int64Builder b;
    EXPECT_TRUE(b.AppendValues(vals).ok());
    return b.Finish().ValueOrDie();
}

std::shared_ptr<arrow::Array> utf8_array(const std::vector<std::string>& vals) {
    arrow::StringBuilder b;
    EXPECT_TRUE(b.AppendValues(vals).ok());
    return b.Finish().ValueOrDie();
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

namespace {

// One row per file, so each produce() call is one file: file i holds i * 100.
std::filesystem::path six_files(const std::string& tag) {
    auto dir = make_temp_dir(tag);
    for (int i = 0; i < 6; ++i) {
        write_int64_file(dir / ("p" + std::to_string(i) + ".parquet"), {i * 100});
    }
    return dir;
}

// The values `calls` produce() calls emit, in order.
std::vector<std::int64_t> emit_calls(MultiObjectParquetSource<std::int64_t>& src, int calls) {
    std::vector<std::int64_t> out;
    Emitter<std::int64_t> em([&out](StreamElement<std::int64_t> e) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                out.push_back(r.value());
            }
        }
        return true;
    });
    for (int i = 0; i < calls && src.produce(em); ++i) {
    }
    return out;
}

// Reads `calls` batches with a source over `dir` and checkpoints it.
void read_and_checkpoint(const std::filesystem::path& dir,
                         int subtask,
                         int par,
                         int calls,
                         InMemoryStateBackend& backend,
                         OperatorId op) {
    MultiObjectParquetSource<std::int64_t> src(
        local_fs_factory(), dir_opts(dir, subtask, par), int64_arrow_batcher());
    src.open();
    (void)emit_calls(src, calls);
    src.snapshot_offset(backend, op, CheckpointId{1});
    src.close();
}

// Everything a source over `dir` restored from `backend` emits, sorted.
std::vector<std::int64_t> restore_and_drain(const std::filesystem::path& dir,
                                            int subtask,
                                            int par,
                                            InMemoryStateBackend& backend,
                                            OperatorId op) {
    MultiObjectParquetSource<std::int64_t> src(
        local_fs_factory(), dir_opts(dir, subtask, par), int64_arrow_batcher());
    EXPECT_TRUE(src.restore_offset(backend, op));
    src.open();
    auto out = drain(src);
    src.close();
    return out;
}

}  // namespace

// An object removed before the position moved the count: restored by the count
// alone, the source skipped p3. The listing differs, so it reads every object
// again.
TEST(MultiObjectParquetSource, ARestoreAfterAnEarlierObjectWasRemovedReadsEveryObjectAgain) {
    const auto dir = six_files("removed");
    InMemoryStateBackend backend;
    read_and_checkpoint(dir, 0, 1, 3, backend, OperatorId{7});
    std::filesystem::remove(dir / "p0.parquet");
    EXPECT_EQ(restore_and_drain(dir, 0, 1, backend, OperatorId{7}),
              (std::vector<std::int64_t>{100, 200, 300, 400, 500}));
}

// An object added before the position moved the count the other way: restored
// by the count alone, the source read p2 again and never read the new object.
TEST(MultiObjectParquetSource, ARestoreAfterAnObjectWasAddedReadsEveryObjectAgain) {
    const auto dir = six_files("added");
    InMemoryStateBackend backend;
    read_and_checkpoint(dir, 0, 1, 3, backend, OperatorId{7});
    write_int64_file(dir / "a.parquet", {-1});
    EXPECT_EQ(restore_and_drain(dir, 0, 1, backend, OperatorId{7}),
              (std::vector<std::int64_t>{-1, 0, 100, 200, 300, 400, 500}));
}

// At another parallelism each subtask is assigned other objects, so a count
// taken over the old assignment skipped some of the new one.
TEST(MultiObjectParquetSource, ARestoreAtAnotherParallelismReadsEveryAssignedObjectAgain) {
    const auto dir = six_files("rescaled");
    InMemoryStateBackend backend;
    read_and_checkpoint(dir, 0, 2, 2, backend, OperatorId{7});  // p0 and p2
    // At parallelism 3, subtask 0 is assigned p0 and p3.
    EXPECT_EQ(restore_and_drain(dir, 0, 3, backend, OperatorId{7}),
              (std::vector<std::int64_t>{0, 300}));
}

// Over the same listing, by the same subtask, the restore resumes exactly.
TEST(MultiObjectParquetSource, ARestoreOverTheSameListingResumesAfterTheCount) {
    const auto dir = six_files("same");
    InMemoryStateBackend backend;
    read_and_checkpoint(dir, 1, 2, 2, backend, OperatorId{7});  // p1 and p3
    EXPECT_EQ(restore_and_drain(dir, 1, 2, backend, OperatorId{7}),
              (std::vector<std::int64_t>{500}));
}

// An earlier release's checkpoint holds the count alone, which names no
// listing, so every object is read again rather than skipped on trust.
TEST(MultiObjectParquetSource, AnEarlierReleasesCountReadsEveryObjectAgain) {
    const auto dir = six_files("earlier");
    InMemoryStateBackend backend;
    const std::string key = "__multi_parquet_source_offset__";
    std::string count(8, '\0');
    count[0] = 3;
    backend.put_operator_state(OperatorId{7},
                               StateBackend::KeyView{key.data(), key.size()},
                               StateBackend::ValueView{count.data(), count.size()});
    EXPECT_EQ(restore_and_drain(dir, 0, 1, backend, OperatorId{7}),
              (std::vector<std::int64_t>{0, 100, 200, 300, 400, 500}));
}

// The versioned form round-trips, a checkpoint taken before open() holds the
// restored position again, and a form neither release wrote is refused.
TEST(MultiObjectParquetSource, TheCheckpointsFormRoundTripsAndAnOddOneIsRefused) {
    const auto dir = six_files("forms");
    const std::string key = "__multi_parquet_source_offset__";
    const StateBackend::KeyView k{key.data(), key.size()};
    InMemoryStateBackend backend;
    read_and_checkpoint(dir, 0, 1, 2, backend, OperatorId{7});
    const auto written = backend.get_operator_state(OperatorId{7}, k);
    ASSERT_TRUE(written.has_value());
    EXPECT_EQ(written->size(), 25U);
    EXPECT_EQ(std::to_integer<int>((*written)[0]), 2);

    MultiObjectParquetSource<std::int64_t> unopened(
        local_fs_factory(), dir_opts(dir, 0, 1), int64_arrow_batcher());
    ASSERT_TRUE(unopened.restore_offset(backend, OperatorId{7}));
    InMemoryStateBackend again;
    unopened.snapshot_offset(again, OperatorId{7}, CheckpointId{2});
    EXPECT_EQ(again.get_operator_state(OperatorId{7}, k), written);

    InMemoryStateBackend odd;
    const std::string five = "abcde";
    odd.put_operator_state(OperatorId{7}, k, StateBackend::ValueView{five.data(), five.size()});
    MultiObjectParquetSource<std::int64_t> refused(
        local_fs_factory(), dir_opts(dir, 0, 1), int64_arrow_batcher());
    EXPECT_THROW((void)refused.restore_offset(odd, OperatorId{7}), std::runtime_error);
}

// A file another tool wrote has no event_time column. Its rows read in full,
// each with no event time, rather than the file being refused for the missing
// engine column.
TEST(MultiObjectParquetSource, AFileAnotherToolWroteReadsWithNoEventTime) {
    auto dir = make_temp_dir("foreign");
    const auto schema = arrow::schema({arrow::field("value", arrow::int64(), false)});
    write_foreign_file(dir / "a.parquet", arrow::Table::Make(schema, {int64_array({1, 2, 3})}));
    write_foreign_file(dir / "b.parquet", arrow::Table::Make(schema, {int64_array({4, 5})}));

    std::vector<std::int64_t> values;
    std::size_t with_time = 0;
    for (int sub = 0; sub < 2; ++sub) {
        MultiObjectParquetSource<std::int64_t> src(
            local_fs_factory(), dir_opts(dir, sub, 2), int64_arrow_batcher());
        src.open();
        Emitter<std::int64_t> em([&](StreamElement<std::int64_t> e) {
            if (e.is_data()) {
                for (const auto& r : e.as_data()) {
                    values.push_back(r.value());
                    with_time += r.event_time().has_value() ? 1 : 0;
                }
            }
            return true;
        });
        while (src.produce(em)) {
        }
        src.close();
    }
    std::sort(values.begin(), values.end());
    EXPECT_EQ(values, (std::vector<std::int64_t>{1, 2, 3, 4, 5}));
    EXPECT_EQ(with_time, 0u);
    std::filesystem::remove_all(dir);
}

// Only the engine's own column may be absent: an event_time of another type is
// a user's column, still refused, and a missing declared column still names it.
TEST(MultiObjectParquetSource, OnlyTheEngineEventTimeColumnMayBeAbsent) {
    auto dir = make_temp_dir("foreign_bad");
    write_foreign_file(
        dir / "typed.parquet",
        arrow::Table::Make(arrow::schema({arrow::field("event_time", arrow::utf8()),
                                          arrow::field("value", arrow::int64(), false)}),
                           {utf8_array({"x"}), int64_array({1})}));
    {
        MultiObjectParquetSource<std::int64_t> src(
            local_fs_factory(), dir_opts(dir, 0, 1), int64_arrow_batcher());
        try {
            src.open();
            FAIL() << "an event_time column of another type must be refused";
        } catch (const std::runtime_error& e) {
            EXPECT_NE(std::string(e.what()).find("'event_time'"), std::string::npos) << e.what();
        }
        src.close();
    }
    std::filesystem::remove_all(dir);

    dir = make_temp_dir("foreign_missing");
    write_foreign_file(
        dir / "other.parquet",
        arrow::Table::Make(arrow::schema({arrow::field("other", arrow::int64(), false)}),
                           {int64_array({1})}));
    MultiObjectParquetSource<std::int64_t> src(
        local_fs_factory(), dir_opts(dir, 0, 1), int64_arrow_batcher());
    try {
        src.open();
        FAIL() << "a missing declared column must be refused";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string(e.what()).find("'value'"), std::string::npos) << e.what();
    }
    src.close();
    std::filesystem::remove_all(dir);
}
