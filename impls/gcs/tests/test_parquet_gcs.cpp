// Offline tests for the GCS Parquet sink + source: required-param validation and clean failure
// against a dead endpoint (no GCS / emulator needed). The write->read round-trip lives in the
// env-gated test_parquet_gcs_live.cpp.

#include <cstdint>
#include <stdexcept>
#include <utility>

#include <gtest/gtest.h>

#include "clink/connectors/parquet_gcs_sink.hpp"
#include "clink/connectors/parquet_gcs_source.hpp"
#include "clink/core/arrow_batcher.hpp"

using clink::int64_arrow_batcher;
using clink::ParquetGcsSink;
using clink::ParquetGcsSource;

namespace {

ParquetGcsSink<std::int64_t>::Options dead_sink_opts() {
    ParquetGcsSink<std::int64_t>::Options o;
    o.bucket = "b";
    o.key = "k.parquet";
    o.anonymous = true;
    o.endpoint_override = "localhost:1";  // no server
    o.scheme = "http";
    o.retry_limit_seconds = 2.0;  // fail fast instead of Arrow's 15-minute default
    return o;
}

ParquetGcsSource<std::int64_t>::Options dead_source_opts() {
    ParquetGcsSource<std::int64_t>::Options o;
    o.bucket = "b";
    o.key = "k.parquet";
    o.anonymous = true;
    o.endpoint_override = "localhost:1";
    o.scheme = "http";
    o.retry_limit_seconds = 2.0;
    return o;
}

}  // namespace

TEST(ParquetGcsSink, RequiresBucketAndKey) {
    ParquetGcsSink<std::int64_t>::Options o;  // bucket + key empty
    EXPECT_THROW((ParquetGcsSink<std::int64_t>{o, int64_arrow_batcher()}), std::invalid_argument);
}

TEST(ParquetGcsSource, RequiresBucketAndKey) {
    ParquetGcsSource<std::int64_t>::Options o;  // bucket + key empty
    EXPECT_THROW((ParquetGcsSource<std::int64_t>{o, int64_arrow_batcher()}), std::invalid_argument);
}

TEST(ParquetGcsSink, OpenAgainstDeadEndpointFailsCleanly) {
    ParquetGcsSink<std::int64_t> sink(dead_sink_opts(), int64_arrow_batcher());
    EXPECT_THROW(sink.open(), std::runtime_error);
    EXPECT_NO_THROW(sink.close());  // close after a failed open must be safe
}

TEST(ParquetGcsSource, OpenAgainstDeadEndpointFailsCleanly) {
    ParquetGcsSource<std::int64_t> src(dead_source_opts(), int64_arrow_batcher());
    EXPECT_THROW(src.open(), std::runtime_error);
    EXPECT_NO_THROW(src.close());
}
