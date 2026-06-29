// GCS Parquet LIVE integration test. SKIPPED unless CLINK_GCS_TEST_ENDPOINT is set (a GCS-API
// endpoint host:port, e.g. localhost:4443 from a fake-gcs-server emulator). Proves end-to-end:
// the sink writes a Parquet object that the source reads back with the same values. The bucket is
// created up front via Arrow's GcsFileSystem (writes do not auto-create buckets).

#include <cstdint>
#include <string>
#include <unistd.h>
#include <vector>

#include <arrow/filesystem/gcsfs.h>
#include <gtest/gtest.h>

#include "clink/connectors/parquet_gcs_sink.hpp"
#include "clink/connectors/parquet_gcs_source.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/core/record.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/operators/operator_base.hpp"

using clink::Batch;
using clink::Emitter;
using clink::int64_arrow_batcher;
using clink::ParquetGcsSink;
using clink::ParquetGcsSource;
using clink::StreamElement;

namespace {

bool gcs_configured() {
    return std::getenv("CLINK_GCS_TEST_ENDPOINT") != nullptr;
}
std::string gcs_endpoint() {
    return std::getenv("CLINK_GCS_TEST_ENDPOINT");
}

}  // namespace

TEST(GcsParquetLive, WriteThenReadRoundTrip) {
    if (!gcs_configured()) {
        GTEST_SKIP() << "set CLINK_GCS_TEST_ENDPOINT (e.g. localhost:4443 fake-gcs-server)";
    }
    const std::string endpoint = gcs_endpoint();
    const std::string bucket = "clink-it-" + std::to_string(static_cast<long>(::getpid()));
    const std::string key = "data.parquet";

    // Create the bucket (writes do not auto-create it). project_id is required for bucket creation.
    arrow::fs::GcsOptions go = arrow::fs::GcsOptions::Anonymous();
    go.endpoint_override = endpoint;
    go.scheme = "http";
    go.project_id = "clink-test";
    go.retry_limit_seconds = 5.0;
    auto fs_res = arrow::fs::GcsFileSystem::Make(go);
    ASSERT_TRUE(fs_res.ok()) << fs_res.status().ToString();
    ASSERT_TRUE((*fs_res)->CreateDir(bucket, /*recursive=*/true).ok());

    auto conn = [&](auto& o) {
        o.bucket = bucket;
        o.key = key;
        o.anonymous = true;
        o.endpoint_override = endpoint;
        o.scheme = "http";
        o.project_id = "clink-test";
        o.retry_limit_seconds = 10.0;
    };

    {
        ParquetGcsSink<std::int64_t>::Options so;
        conn(so);
        ParquetGcsSink<std::int64_t> sink(so, int64_arrow_batcher());
        sink.open();
        Batch<std::int64_t> b;
        b.emplace(10);
        b.emplace(20);
        b.emplace(30);
        sink.on_data(b);
        sink.close();  // finalises the Parquet object
    }

    std::vector<std::int64_t> got;
    Emitter<std::int64_t> em([&](StreamElement<std::int64_t> e) -> bool {
        if (e.is_data()) {
            for (const auto& rec : e.as_data()) {
                got.push_back(rec.value());
            }
        }
        return true;
    });
    ParquetGcsSource<std::int64_t>::Options ro;
    conn(ro);
    ParquetGcsSource<std::int64_t> src(ro, int64_arrow_batcher());
    src.open();
    while (src.produce(em)) {
    }
    src.close();

    ASSERT_EQ(got.size(), 3u);
    EXPECT_EQ(got[0], 10);
    EXPECT_EQ(got[1], 20);
    EXPECT_EQ(got[2], 30);
}
