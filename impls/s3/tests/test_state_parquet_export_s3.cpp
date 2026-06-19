// export_state_to_parquet_s3 (ASYNC-11): partitioned, externally-queryable
// state export to object storage. Construction validation, then a MinIO-gated
// round-trip proving each operator's rows land in their own op_id=<id> Parquet
// object and read back from S3 byte-faithfully (the open format DuckDB/pyarrow
// read directly off the bucket).

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

#include "clink/connectors/parquet_s3_sink.hpp"  // ensure_arrow_s3_initialised
#include "clink/s3/state_parquet_export_s3.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;
using clink::s3::export_state_to_parquet_s3;
using clink::s3::S3ExportOptions;

namespace {

std::string_view sv(const char* s) {
    return std::string_view{s};
}

bool minio_configured() {
    return std::getenv("CLINK_S3_TEST_ENDPOINT") != nullptr &&
           std::getenv("CLINK_S3_TEST_BUCKET") != nullptr;
}

std::string unique_prefix(const std::string& tag) {
    static int n = 0;
    return "clink-test/" + tag + "-" + std::to_string(static_cast<long>(::getpid())) + "-" +
           std::to_string(n++);
}

std::shared_ptr<arrow::fs::S3FileSystem> minio_fs() {
    clink::detail::ensure_arrow_s3_initialised();
    auto o = arrow::fs::S3Options::Defaults();
    o.endpoint_override = std::string{std::getenv("CLINK_S3_TEST_ENDPOINT")};
    o.scheme = "http";
    o.region = "us-east-1";
    return arrow::fs::S3FileSystem::Make(o).ValueOrDie();
}

std::int64_t s3_parquet_rows(arrow::fs::S3FileSystem& fs, const std::string& key) {
    auto in = fs.OpenInputFile(key);
    EXPECT_TRUE(in.ok()) << key << ": " << in.status().ToString();
    auto reader_res = parquet::arrow::OpenFile(*in, arrow::default_memory_pool());
    EXPECT_TRUE(reader_res.ok());
    std::shared_ptr<arrow::Table> table;
    EXPECT_TRUE((*reader_res)->ReadTable(&table).ok());
    return table->num_rows();
}

}  // namespace

TEST(StateParquetExportS3, RejectsEmptyBucket) {
    InMemoryStateBackend backend;
    S3ExportOptions opts;  // empty bucket
    EXPECT_THROW(export_state_to_parquet_s3(backend, {OperatorId{1}}, opts), std::invalid_argument);
}

TEST(StateParquetExportS3, PartitionedExportReadableBackFromS3) {
    if (!minio_configured()) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET";
    }
    InMemoryStateBackend backend;
    backend.put(OperatorId{1}, sv("a"), sv("1"));
    backend.put(OperatorId{1}, sv("b"), sv("2"));
    backend.put(OperatorId{2}, sv("c"), sv("3"));

    S3ExportOptions opts;
    opts.bucket = std::getenv("CLINK_S3_TEST_BUCKET");
    opts.prefix = unique_prefix("stexport");
    opts.endpoint_override = std::string{std::getenv("CLINK_S3_TEST_ENDPOINT")};
    opts.region = "us-east-1";

    const auto stats = export_state_to_parquet_s3(backend, {OperatorId{1}, OperatorId{2}}, opts);
    EXPECT_EQ(stats.rows, 3);
    EXPECT_EQ(stats.operators, 2u);

    // Each operator's rows land in its own partition object and read back from S3.
    auto fs = minio_fs();
    const std::string base = std::string{opts.bucket} + "/" + opts.prefix;
    EXPECT_EQ(s3_parquet_rows(*fs, base + "/op_id=1/state.parquet"), 2);
    EXPECT_EQ(s3_parquet_rows(*fs, base + "/op_id=2/state.parquet"), 1);
}
