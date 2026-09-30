// ParquetRollingSink: the plain `parquet` connector's sink. A Parquet file
// cannot be appended to and has no footer until it is closed, so a sink that
// writes one file per subtask loses everything when its process dies, and a
// restored run can only start the file again from empty. These pin the
// directory-of-parts contract that replaces it: complete parts at every
// barrier, kept across a restore, replaced by a fresh start, never torn.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include <arrow/filesystem/mockfs.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/connectors/parquet_rolling_sink.hpp"
#include "clink/connectors/parquet_source.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/runtime/runtime_context.hpp"

namespace {

namespace fs = std::filesystem;
using clink::Batch;
using clink::CheckpointBarrier;
using clink::CheckpointId;
using clink::ParquetRollingSink;

fs::path rolling_tmp_dir(const std::string& tag) {
    static std::mt19937_64 rng{std::random_device{}()};
    auto p = fs::temp_directory_path() / ("clink_pqroll_" + tag + "_" + std::to_string(rng()));
    fs::remove_all(p);
    return p;
}

std::vector<fs::path> files_in(const fs::path& dir) {
    std::vector<fs::path> out;
    std::error_code ec;
    for (const auto& e : fs::directory_iterator(dir, ec)) {
        out.push_back(e.path());
    }
    std::sort(out.begin(), out.end());
    return out;
}

std::vector<fs::path> parts_in(const fs::path& dir) {
    auto all = files_in(dir);
    std::erase_if(all, [](const fs::path& p) { return p.extension() != ".parquet"; });
    return all;
}

std::vector<std::int64_t> read_part(const fs::path& file) {
    clink::ParquetSource<std::int64_t> source(file, clink::int64_arrow_batcher());
    source.open();
    std::vector<std::int64_t> values;
    clink::Emitter<std::int64_t> em{[&values](clink::StreamElement<std::int64_t> e) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                values.push_back(r.value());
            }
        }
        return true;
    }};
    while (source.produce(em)) {
    }
    source.close();
    return values;
}

std::vector<std::int64_t> read_all(const fs::path& dir) {
    std::vector<std::int64_t> all;
    for (const auto& p : parts_in(dir)) {
        const auto v = read_part(p);
        all.insert(all.end(), v.begin(), v.end());
    }
    std::sort(all.begin(), all.end());
    return all;
}

Batch<std::int64_t> values(std::int64_t from, std::int64_t to) {
    Batch<std::int64_t> b;
    for (auto v = from; v < to; ++v) {
        b.emplace(v);
    }
    return b;
}

std::unique_ptr<ParquetRollingSink<std::int64_t>> make_sink(const fs::path& dir,
                                                            std::uint32_t subtask = 0,
                                                            std::uint32_t parallelism = 1) {
    typename ParquetRollingSink<std::int64_t>::Options o;
    o.dir = dir.string();
    o.subtask_idx = subtask;
    o.parallelism = parallelism;
    return std::make_unique<ParquetRollingSink<std::int64_t>>(
        clink::local_parquet_filesystem(), std::move(o), clink::int64_arrow_batcher());
}

// A well-formed run token for names a test writes by hand.
constexpr std::string_view kDeadRun = "0123456789abcdef01";

CheckpointBarrier barrier(std::uint64_t id) {
    return CheckpointBarrier{CheckpointId{id}};
}

void touch(const fs::path& p) {
    std::ofstream(p) << "x";
}

}  // namespace

TEST(ParquetRollingSink, EachCheckpointIntervalBecomesOneCompletePart) {
    const auto dir = rolling_tmp_dir("intervals");
    auto sink = make_sink(dir);
    sink->open();
    sink->on_data(values(0, 10));
    sink->on_barrier(barrier(1));
    // Readable at once, while the job is still running.
    ASSERT_EQ(parts_in(dir).size(), 1U);
    EXPECT_EQ(read_part(parts_in(dir)[0]).size(), 10U);

    sink->on_data(values(10, 15));
    sink->on_data(values(15, 20));
    sink->on_barrier(barrier(2));
    sink->on_barrier(barrier(3));  // an interval with no rows writes no part
    sink->on_data(values(20, 25));
    sink->flush();  // end of input: the rows after the last barrier
    sink->close();

    const auto parts = parts_in(dir);
    ASSERT_EQ(parts.size(), 3U);
    // Names sort in the order the parts were written.
    EXPECT_EQ(read_part(parts[0]).size(), 10U);
    EXPECT_EQ(read_part(parts[1]).size(), 10U);
    EXPECT_EQ(read_part(parts[2]).size(), 5U);
    std::vector<std::int64_t> expected(25);
    for (std::int64_t i = 0; i < 25; ++i) {
        expected[static_cast<std::size_t>(i)] = i;
    }
    EXPECT_EQ(read_all(dir), expected);
    EXPECT_EQ(files_in(dir).size(), parts.size()) << "nothing but complete parts is left";
    fs::remove_all(dir);
}

// What the old single-file sink could not do: a restored run keeps every part
// an earlier run finished, and adds its own beside them.
TEST(ParquetRollingSink, ARestoredRunKeepsEveryFinishedPart) {
    const auto dir = rolling_tmp_dir("restore");
    {
        auto first = make_sink(dir);
        first->open();
        first->on_data(values(0, 10));
        first->on_barrier(barrier(1));
        first->on_data(values(10, 20));
        first->on_barrier(barrier(2));
        // Killed: rows after barrier 2 are in an unfinished part.
        first->on_data(values(20, 30));
    }
    ASSERT_EQ(parts_in(dir).size(), 2U);
    ASSERT_EQ(files_in(dir).size(), 2U) << "the unfinished part is abandoned, never published";
    // A real kill leaves the .inprogress file behind; the next open removes it.
    touch(dir / (ParquetRollingSink<std::int64_t>::part_name(0, kDeadRun, 9) + ".inprogress"));

    clink::RuntimeContext ctx(clink::OperatorId{7}, "pq", nullptr, nullptr);
    ctx.set_commit_receipts((dir / "receipts").string(), /*restore_from_ckpt=*/1);
    auto resumed = make_sink(dir);
    resumed->attach_runtime(&ctx);
    resumed->open();
    // The restore is from checkpoint 1, so the sources replay everything after
    // it: rows 10..30 arrive again. At-least-once, so part 2's rows repeat.
    resumed->on_data(values(10, 30));
    resumed->flush();
    resumed->close();

    EXPECT_EQ(parts_in(dir).size(), 3U);
    EXPECT_EQ(files_in(dir).size(), 3U) << "the dead run's .inprogress file is removed";
    const auto all = read_all(dir);
    EXPECT_EQ(all.size(), 40U) << "0..10 once, 10..20 twice, 20..30 once";
    for (std::int64_t v = 0; v < 30; ++v) {
        EXPECT_GE(std::count(all.begin(), all.end(), v), 1) << "row " << v << " was lost";
    }
    fs::remove_all(dir);
}

// A run with no restore point replaces the previous output, as a truncating
// file sink does, and touches only this sink's parts.
TEST(ParquetRollingSink, AFreshRunReplacesOnlyItsOwnOutput) {
    const auto dir = rolling_tmp_dir("fresh");
    for (std::uint32_t sub = 0; sub < 4; ++sub) {
        auto old_run = make_sink(dir, sub, 4);
        old_run->open();
        const auto base = static_cast<std::int64_t>(sub) * 100;
        old_run->on_data(values(base, base + 5));
        old_run->flush();
        old_run->close();
    }
    touch(dir / "README.txt");
    touch(dir / "sub1-other.csv");
    ASSERT_EQ(parts_in(dir).size(), 4U);

    // A narrower fresh run: subtask 0 clears its own parts and those of
    // subtasks 2 and 3, which this run will not write; subtask 1 clears its own.
    auto s0 = make_sink(dir, 0, 2);
    s0->open();
    EXPECT_EQ(parts_in(dir).size(), 1U) << "only subtask 1's old part is left";
    auto s1 = make_sink(dir, 1, 2);
    s1->open();
    EXPECT_TRUE(parts_in(dir).empty());
    s0->on_data(values(1000, 1003));
    s1->on_data(values(2000, 2002));
    s0->flush();
    s1->flush();
    s0->close();
    s1->close();

    EXPECT_EQ(read_all(dir), (std::vector<std::int64_t>{1000, 1001, 1002, 2000, 2001}));
    EXPECT_TRUE(fs::exists(dir / "README.txt")) << "files that are not parts are left alone";
    EXPECT_TRUE(fs::exists(dir / "sub1-other.csv"));
    fs::remove_all(dir);
}

TEST(ParquetRollingSink, ACloseWithoutEndOfInputKeepsWhatWasWritten) {
    const auto dir = rolling_tmp_dir("cancel");
    auto sink = make_sink(dir);
    sink->open();
    sink->on_data(values(0, 7));
    sink->close();  // a cancel: no flush()
    EXPECT_EQ(read_all(dir).size(), 7U);
    fs::remove_all(dir);
}

TEST(ParquetRollingSink, PartNamesIdentifyTheirSubtaskAndNothingElse) {
    using S = ParquetRollingSink<std::int64_t>;
    const auto name = S::part_name(12, kDeadRun, 3);
    EXPECT_EQ(name, "sub12-" + std::string{kDeadRun} + "-000003.parquet");
    EXPECT_EQ(S::part_subtask(name), 12U);
    EXPECT_EQ(S::part_subtask(name + ".inprogress"), 12U);
    EXPECT_EQ(S::part_subtask(S::part_name(0, kDeadRun, 1234567)), 0U);
    // Anything that is not exactly a part name is left alone when clearing.
    EXPECT_FALSE(S::part_subtask("sub3-x.parquet").has_value()) << "run token too short";
    EXPECT_FALSE(S::part_subtask("sub3-" + std::string{kDeadRun} + "-12.parquet").has_value())
        << "sequence under six digits";
    EXPECT_FALSE(S::part_subtask("sub03-" + std::string{kDeadRun} + "-000001.parquet").has_value())
        << "leading zero";
    EXPECT_FALSE(S::part_subtask("sub3-0123456789ABCDEF01-000001.parquet").has_value())
        << "upper-case hex";
    EXPECT_FALSE(
        S::part_subtask("sub99999999999-" + std::string{kDeadRun} + "-000001.parquet").has_value())
        << "subtask overflows 32 bits";
    EXPECT_FALSE(S::part_subtask("sub-x.parquet").has_value());
    EXPECT_FALSE(S::part_subtask("sub3.parquet").has_value());
    EXPECT_FALSE(S::part_subtask("sub3-x.csv").has_value());
    EXPECT_FALSE(S::part_subtask("data.parquet").has_value());
}

// The read side: a `parquet` source whose path is a directory reads every part.
TEST(ParquetRollingSink, TheDirectorySourceReadsEveryPart) {
    const auto dir = rolling_tmp_dir("source");
    auto sink = make_sink(dir);
    sink->open();
    sink->on_data(values(0, 4));
    sink->on_barrier(barrier(1));
    sink->on_data(values(4, 9));
    sink->flush();
    sink->close();

    auto source = clink::make_local_parquet_directory_source<std::int64_t>(
        dir.string(), 0, 1, clink::int64_arrow_batcher(), "pq");
    ASSERT_NE(source, nullptr);
    EXPECT_EQ(clink::make_local_parquet_directory_source<std::int64_t>(
                  parts_in(dir)[0].string(), 0, 1, clink::int64_arrow_batcher(), "pq"),
              nullptr)
        << "a file path is not a directory source";
    source->open();
    std::vector<std::int64_t> got;
    clink::Emitter<std::int64_t> em{[&got](clink::StreamElement<std::int64_t> e) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                got.push_back(r.value());
            }
        }
        return true;
    }};
    while (source->produce(em)) {
    }
    source->close();
    std::sort(got.begin(), got.end());
    EXPECT_EQ(got, (std::vector<std::int64_t>{0, 1, 2, 3, 4, 5, 6, 7, 8}));
    fs::remove_all(dir);
}

// A store that is neither S3 nor a rename filesystem (Azure, GCS, anything
// unknown) gets <name>.inprogress copied into place. The in-memory mock
// filesystem reports its own type, which takes that path.
TEST(ParquetRollingSink, OnACopyStoreAPartAppearsOnlyWhenComplete) {
    auto mock =
        std::make_shared<arrow::fs::internal::MockFileSystem>(std::chrono::system_clock::now());
    const auto factory = [mock]() -> std::shared_ptr<arrow::fs::FileSystem> { return mock; };
    const auto make = [&](std::uint32_t sub) {
        typename ParquetRollingSink<std::int64_t>::Options o;
        o.dir = "bucket/out";
        o.subtask_idx = sub;
        o.parallelism = 1;
        return std::make_unique<ParquetRollingSink<std::int64_t>>(
            factory, std::move(o), clink::int64_arrow_batcher());
    };
    const auto keys = [&] {
        arrow::fs::FileSelector sel;
        sel.base_dir = "bucket/out";
        sel.allow_not_found = true;
        std::vector<std::string> out;
        for (const auto& info : mock->GetFileInfo(sel).ValueOrDie()) {
            out.push_back(info.path());
        }
        return out;
    };
    const auto rows = [&] {
        std::int64_t n = 0;
        for (const auto& k : keys()) {
            auto in = mock->OpenInputFile(k).ValueOrDie();
            auto reader = parquet::arrow::OpenFile(in, arrow::default_memory_pool()).ValueOrDie();
            n += reader->parquet_reader()->metadata()->num_rows();
        }
        return n;
    };

    auto sink = make(0);
    sink->open();
    sink->on_data(values(0, 6));
    for (const auto& k : keys()) {
        EXPECT_FALSE(k.ends_with(".parquet")) << k << ": a part being written is not visible";
    }
    sink->on_barrier(barrier(1));
    sink->on_data(values(6, 9));
    sink->flush();
    sink->close();
    ASSERT_EQ(keys().size(), 2U);
    for (const auto& k : keys()) {
        EXPECT_TRUE(k.ends_with(".parquet")) << k << ": the in-progress object is deleted";
    }
    EXPECT_EQ(rows(), 9);

    // A fresh run over the same prefix replaces the output.
    auto fresh = make(0);
    fresh->open();
    EXPECT_TRUE(keys().empty());
    fresh->on_data(values(100, 102));
    fresh->flush();
    fresh->close();
    EXPECT_EQ(rows(), 2);
}

// The restore point reaches the sink through the executor even when the job has
// no receipts directory (a savepoint restore into a job without a checkpoint
// directory of its own). Before, it arrived only with receipts on, so such a
// restore looked like a fresh start and deleted every earlier part.
TEST(ParquetRollingSink, ARestoreWithoutACheckpointDirectoryStillKeepsParts) {
    const auto dir = rolling_tmp_dir("noreceipts");
    {
        auto earlier = make_sink(dir);
        earlier->open();
        earlier->on_data(values(0, 4));
        earlier->flush();
        earlier->close();
    }
    ASSERT_EQ(read_all(dir).size(), 4U);

    std::vector<clink::Record<std::int64_t>> recs;
    for (std::int64_t v = 4; v < 6; ++v) {
        recs.emplace_back(v);
    }
    clink::Dag dag;
    auto src =
        dag.add_source<std::int64_t>(std::make_shared<clink::VectorSource<std::int64_t>>(recs));
    dag.add_sink<std::int64_t>(src, std::shared_ptr<clink::Sink<std::int64_t>>(make_sink(dir)));
    clink::JobConfig cfg;
    cfg.restore_from_checkpoint_id = 7;  // and no commit_receipt_dir
    clink::LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    EXPECT_EQ(read_all(dir), (std::vector<std::int64_t>{0, 1, 2, 3, 4, 5}))
        << "the restored run must keep the parts written before it";
    fs::remove_all(dir);
}

// An exactly-once Parquet sink writes only staging/ and committed/ under its
// path; reading that path would yield nothing, so the source says where to look.
TEST(ParquetRollingSink, ReadingAnExactlyOnceSinksDirectoryPointsAtCommitted) {
    const auto dir = rolling_tmp_dir("committed");
    fs::create_directories(dir / "committed");
    fs::create_directories(dir / "staging");
    try {
        (void)clink::make_local_parquet_directory_source<std::int64_t>(
            dir.string(), 0, 1, clink::int64_arrow_batcher(), "pq");
        ADD_FAILURE() << "expected a refusal";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string{e.what()}.find("committed"), std::string::npos) << e.what();
    }
    fs::create_directories(dir / "empty");
    EXPECT_NE(clink::make_local_parquet_directory_source<std::int64_t>(
                  (dir / "empty").string(), 0, 1, clink::int64_arrow_batcher(), "pq"),
              nullptr)
        << "an empty directory is a legitimate zero-row read";
    fs::remove_all(dir);
}

// An earlier release's single-file sink left a file where this sink writes a
// directory; the sink refuses by name.
TEST(ParquetRollingSink, APathHoldingAnOldSingleFileIsRefusedByName) {
    const auto dir = rolling_tmp_dir("oldfile");
    touch(dir);
    auto sink = make_sink(dir);
    try {
        sink->open();
        ADD_FAILURE() << "expected a refusal";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string{e.what()}.find("is a file"), std::string::npos) << e.what();
    }
    fs::remove(dir);
}
