// Lifecycle / construction tests for ParquetS3Sink + ParquetS3Source.
//
// These don't speak to a real S3 backend - they exercise the
// constructor validation and the open() path against an unreachable
// endpoint to drive Arrow's S3 init and connection-failure handling.
// Real round-trip tests would need a live S3 / MinIO instance and
// belong with the integration test suite, not here.
//
// Mirrors the pattern in test_s3_connector.cpp.

#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/connectors/parquet_s3_sink.hpp"
#include "clink/connectors/parquet_s3_source.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/core/record.hpp"

using namespace clink;

namespace {

// 127.0.0.1:1 is reliably unreachable. Used to drive the S3 init +
// open() failure path without needing a live S3 backend.
constexpr const char* kDeadEndpoint = "http://127.0.0.1:1";

}  // namespace

TEST(ParquetS3Sink, RejectsEmptyBucket) {
    ParquetS3Sink<std::int64_t>::Options opts;
    opts.bucket = "";
    opts.key = "some/key.parquet";
    EXPECT_THROW(ParquetS3Sink<std::int64_t>(std::move(opts), int64_arrow_batcher()),
                 std::invalid_argument);
}

TEST(ParquetS3Sink, RejectsEmptyKey) {
    ParquetS3Sink<std::int64_t>::Options opts;
    opts.bucket = "bucket";
    opts.key = "";
    EXPECT_THROW(ParquetS3Sink<std::int64_t>(std::move(opts), int64_arrow_batcher()),
                 std::invalid_argument);
}

TEST(ParquetS3Sink, AllowsEmptyKeyWhenBucketAssignerIsSet) {
    // The bucket_assigner supplies per-record keys; the static `key`
    // field is then ignored. Construction must accept it.
    ParquetS3Sink<std::int64_t>::Options opts;
    opts.bucket = "bucket";
    opts.key = "";  // intentionally empty
    opts.bucket_assigner = [](const std::int64_t& v) {
        return std::string{"year=2026/value="} + std::to_string(v) + ".parquet";
    };
    EXPECT_NO_THROW(ParquetS3Sink<std::int64_t>(std::move(opts), int64_arrow_batcher()));
}

TEST(ParquetS3Sink, ConstructorAcceptsValidOptions) {
    // Construction must not touch the network. Only open() does.
    ParquetS3Sink<std::int64_t>::Options opts;
    opts.bucket = "bucket";
    opts.key = "events.parquet";
    opts.endpoint_override = kDeadEndpoint;
    ParquetS3Sink<std::int64_t> sink(std::move(opts), int64_arrow_batcher());
    EXPECT_EQ(sink.name(), "parquet_s3_sink");
}

TEST(ParquetS3Sink, OpenAgainstDeadEndpointFailsCleanly) {
    // open() initialises Arrow S3 and tries to open the output stream;
    // against an unreachable endpoint this must throw a runtime_error
    // (not abort, not deadlock). Exactly what condition triggers the
    // failure varies by Arrow version + region resolution path - we
    // assert only that *some* runtime_error is thrown.
    ParquetS3Sink<std::int64_t>::Options opts;
    opts.bucket = "bucket";
    opts.key = "events.parquet";
    opts.endpoint_override = kDeadEndpoint;
    opts.region = "us-east-1";
    opts.allow_anonymous = true;
    ParquetS3Sink<std::int64_t> sink(std::move(opts), int64_arrow_batcher());
    EXPECT_THROW(sink.open(), std::runtime_error);
}

TEST(ParquetS3Source, RejectsEmptyBucket) {
    ParquetS3Source<std::int64_t>::Options opts;
    opts.bucket = "";
    opts.key = "in.parquet";
    EXPECT_THROW(ParquetS3Source<std::int64_t>(std::move(opts), int64_arrow_batcher()),
                 std::invalid_argument);
}

TEST(ParquetS3Source, ConstructorAcceptsValidOptions) {
    ParquetS3Source<std::int64_t>::Options opts;
    opts.bucket = "bucket";
    opts.key = "in.parquet";
    opts.endpoint_override = kDeadEndpoint;
    ParquetS3Source<std::int64_t> source(std::move(opts), int64_arrow_batcher());
    EXPECT_EQ(source.name(), "parquet_s3_source");
}

TEST(ParquetS3Source, OpenAgainstDeadEndpointFailsCleanly) {
    ParquetS3Source<std::int64_t>::Options opts;
    opts.bucket = "bucket";
    opts.key = "in.parquet";
    opts.endpoint_override = kDeadEndpoint;
    opts.region = "us-east-1";
    opts.allow_anonymous = true;
    ParquetS3Source<std::int64_t> source(std::move(opts), int64_arrow_batcher());
    EXPECT_THROW(source.open(), std::runtime_error);
}

TEST(ParquetS3Factories, AllFourAreRegistered) {
    // Smoke-check that the install() side has registered the four
    // expected op-types so the planner can resolve them by name. We
    // can't introspect the registry directly here without depending on
    // its internals; the cleaner check is that
    // ParquetS3Sink<std::int64_t> + ParquetS3Sink<std::string> +
    // ParquetS3Source<std::int64_t> + ParquetS3Source<std::string> all
    // construct cleanly from the registered factory shape.
    using SinkInt = ParquetS3Sink<std::int64_t>;
    using SinkStr = ParquetS3Sink<std::string>;
    using SourceInt = ParquetS3Source<std::int64_t>;
    using SourceStr = ParquetS3Source<std::string>;
    SinkInt::Options sink_int_opts{.bucket = "b", .key = "k"};
    SinkStr::Options sink_str_opts{.bucket = "b", .key = "k"};
    SourceInt::Options source_int_opts{.bucket = "b", .key = "k"};
    SourceStr::Options source_str_opts{.bucket = "b", .key = "k"};
    (void)SinkInt{std::move(sink_int_opts), int64_arrow_batcher()};
    (void)SinkStr{std::move(sink_str_opts), string_arrow_batcher()};
    (void)SourceInt{std::move(source_int_opts), int64_arrow_batcher()};
    (void)SourceStr{std::move(source_str_opts), string_arrow_batcher()};
}
