// The plain s3_parquet sink over a real S3 API (MinIO): a directory of
// complete part objects, one per subtask per checkpoint interval. Gated on
// CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET like the other MinIO tests.

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include <arrow/api.h>
#include <arrow/filesystem/s3fs.h>
#include <arrow/io/api.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/connectors/parquet_rolling_sink.hpp"
#include "clink/connectors/parquet_s3_sink.hpp"  // ensure_arrow_s3_initialised
#include "clink/core/arrow_batcher.hpp"
#include "clink/runtime/runtime_context.hpp"

namespace {

bool rolling_minio_configured() {
    return std::getenv("CLINK_S3_TEST_ENDPOINT") != nullptr &&
           std::getenv("CLINK_S3_TEST_BUCKET") != nullptr;
}

std::shared_ptr<arrow::fs::FileSystem> rolling_minio_fs() {
    clink::detail::ensure_arrow_s3_initialised();
    auto o = arrow::fs::S3Options::Defaults();
    o.endpoint_override = std::string{std::getenv("CLINK_S3_TEST_ENDPOINT")};
    o.scheme = "http";
    o.region = "us-east-1";
    return arrow::fs::S3FileSystem::Make(o).ValueOrDie();
}

clink::Batch<std::int64_t> rolling_values(std::int64_t from, std::int64_t to) {
    clink::Batch<std::int64_t> b;
    for (auto v = from; v < to; ++v) {
        b.emplace(v);
    }
    return b;
}

}  // namespace

TEST(ParquetRollingS3, PartsSurviveARestoreAndAKillLeavesNoPartialObject) {
    if (!rolling_minio_configured()) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET";
    }
    auto fs = rolling_minio_fs();
    const std::string dir = std::string{std::getenv("CLINK_S3_TEST_BUCKET")} +
                            "/clink-test/rolling-" + std::to_string(::getpid());
    const auto factory = [fs]() { return fs; };
    const auto make = [&]() {
        clink::ParquetRollingSink<std::int64_t>::Options o;
        o.dir = dir;
        return std::make_unique<clink::ParquetRollingSink<std::int64_t>>(
            factory, std::move(o), clink::int64_arrow_batcher());
    };
    const auto objects = [&] {
        arrow::fs::FileSelector sel;
        sel.base_dir = dir;
        sel.allow_not_found = true;
        std::vector<std::string> out;
        for (const auto& info : fs->GetFileInfo(sel).ValueOrDie()) {
            if (info.type() == arrow::fs::FileType::File) {
                out.push_back(info.path());
            }
        }
        return out;
    };
    const auto rows = [&] {
        std::int64_t n = 0;
        for (const auto& key : objects()) {
            auto in = fs->OpenInputFile(key).ValueOrDie();
            auto reader = parquet::arrow::OpenFile(in, arrow::default_memory_pool()).ValueOrDie();
            n += reader->parquet_reader()->metadata()->num_rows();
        }
        return n;
    };

    {
        auto first = make();
        first->open();  // a fresh run clears the prefix of any earlier test's parts
        first->on_data(rolling_values(0, 10));
        first->on_barrier(clink::CheckpointBarrier{clink::CheckpointId{1}});
        // Killed mid-part: its upload never completes, so no object appears.
        first->on_data(rolling_values(10, 20));
        EXPECT_EQ(objects().size(), 1U);
    }
    EXPECT_EQ(rows(), 10);

    clink::RuntimeContext ctx(clink::OperatorId{9}, "pq", nullptr, nullptr);
    ctx.set_commit_receipts("unused", /*restore_from_ckpt=*/1);
    auto resumed = make();
    resumed->attach_runtime(&ctx);
    resumed->open();
    EXPECT_EQ(rows(), 10) << "a restored run keeps the parts already written";
    resumed->on_data(rolling_values(10, 20));
    resumed->flush();
    resumed->close();
    EXPECT_EQ(objects().size(), 2U);
    EXPECT_EQ(rows(), 20);

    for (const auto& key : objects()) {
        (void)fs->DeleteFile(key);
    }
}
