// WebHDFS Parquet LIVE integration test. SKIPPED unless CLINK_WEBHDFS_TEST_ENDPOINT is set (a
// WebHDFS NameNode or HttpFS gateway root, e.g. http://127.0.0.1:14000). Proves end-to-end: the
// sink writes a Parquet file that the source reads back with the same values. Optional
// CLINK_WEBHDFS_TEST_USER sets user.name (an HDFS user with write permission).

#include <cstdint>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/connectors/webhdfs_parquet_sink.hpp"
#include "clink/connectors/webhdfs_parquet_source.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/core/record.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/operators/operator_base.hpp"

using clink::Batch;
using clink::Emitter;
using clink::int64_arrow_batcher;
using clink::StreamElement;
using clink::WebHdfsParquetSink;
using clink::WebHdfsParquetSource;

namespace {

bool webhdfs_configured() {
    return std::getenv("CLINK_WEBHDFS_TEST_ENDPOINT") != nullptr;
}

}  // namespace

TEST(WebHdfsParquetLive, WriteThenReadRoundTrip) {
    if (!webhdfs_configured()) {
        GTEST_SKIP() << "set CLINK_WEBHDFS_TEST_ENDPOINT (e.g. http://127.0.0.1:14000 HttpFS)";
    }
    const std::string endpoint = std::getenv("CLINK_WEBHDFS_TEST_ENDPOINT");
    const char* user_env = std::getenv("CLINK_WEBHDFS_TEST_USER");
    const std::string path =
        "/tmp/clink-it-" + std::to_string(static_cast<long>(::getpid())) + ".parquet";

    auto conn = [&](auto& o) {
        o.base_url = endpoint;
        o.path = path;
        if (user_env != nullptr) {
            o.user = std::string(user_env);
        }
    };

    {
        WebHdfsParquetSink<std::int64_t>::Options so;
        conn(so);
        so.overwrite = true;
        WebHdfsParquetSink<std::int64_t> sink(so, int64_arrow_batcher());
        sink.open();
        Batch<std::int64_t> b;
        b.emplace(10);
        b.emplace(20);
        b.emplace(30);
        sink.on_data(b);
        sink.close();  // finalises + uploads the Parquet file
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
    WebHdfsParquetSource<std::int64_t>::Options ro;
    conn(ro);
    WebHdfsParquetSource<std::int64_t> src(ro, int64_arrow_batcher());
    src.open();
    while (src.produce(em)) {
    }
    src.close();

    ASSERT_EQ(got.size(), 3u);
    EXPECT_EQ(got[0], 10);
    EXPECT_EQ(got[1], 20);
    EXPECT_EQ(got[2], 30);
}
