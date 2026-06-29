// Azure Blob Parquet LIVE integration test. SKIPPED unless CLINK_AZURE_TEST_ENDPOINT is set (an
// Azure Blob endpoint authority host:port, e.g. 127.0.0.1:10000 from an Azurite emulator). Proves
// end-to-end: the sink writes a Parquet blob that the source reads back with the same values. The
// container is created up front via Arrow's AzureFileSystem (writes do not auto-create it). Auth
// uses Azurite's well-known account + shared key.

#include <cstdint>
#include <string>
#include <unistd.h>
#include <vector>

#include <arrow/filesystem/azurefs.h>
#include <gtest/gtest.h>

#include "clink/connectors/parquet_azure_sink.hpp"
#include "clink/connectors/parquet_azure_source.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/core/record.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/operators/operator_base.hpp"

using clink::Batch;
using clink::Emitter;
using clink::int64_arrow_batcher;
using clink::ParquetAzureSink;
using clink::ParquetAzureSource;
using clink::StreamElement;

namespace {

// Azurite's well-known development account + shared key (public, documented defaults).
constexpr const char* kAzuriteAccount = "devstoreaccount1";
constexpr const char* kAzuriteKey =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
    "K1SZFPTOtr/KBHBeksoGMGw==";

bool azure_configured() {
    return std::getenv("CLINK_AZURE_TEST_ENDPOINT") != nullptr;
}
std::string azure_endpoint() {
    return std::getenv("CLINK_AZURE_TEST_ENDPOINT");
}

}  // namespace

TEST(AzureParquetLive, WriteThenReadRoundTrip) {
    if (!azure_configured()) {
        GTEST_SKIP() << "set CLINK_AZURE_TEST_ENDPOINT (e.g. 127.0.0.1:10000 Azurite blob port)";
    }
    const std::string endpoint = azure_endpoint();
    const std::string container = "clink-it-" + std::to_string(static_cast<long>(::getpid()));
    const std::string key = "data.parquet";

    // Create the container (writes do not auto-create it).
    arrow::fs::AzureOptions ao;
    ao.account_name = kAzuriteAccount;
    ao.blob_storage_authority = endpoint;
    ao.dfs_storage_authority = endpoint;
    ao.blob_storage_scheme = "http";
    ao.dfs_storage_scheme = "http";
    ASSERT_TRUE(ao.ConfigureAccountKeyCredential(kAzuriteKey).ok());
    auto fs_res = arrow::fs::AzureFileSystem::Make(ao);
    ASSERT_TRUE(fs_res.ok()) << fs_res.status().ToString();
    ASSERT_TRUE((*fs_res)->CreateDir(container, /*recursive=*/true).ok());

    auto conn = [&](auto& o) {
        o.container = container;
        o.key = key;
        o.account_name = kAzuriteAccount;
        o.account_key = std::string(kAzuriteKey);
        o.blob_storage_authority = endpoint;
        o.blob_storage_scheme = "http";
    };

    {
        ParquetAzureSink<std::int64_t>::Options so;
        conn(so);
        ParquetAzureSink<std::int64_t> sink(so, int64_arrow_batcher());
        sink.open();
        Batch<std::int64_t> b;
        b.emplace(10);
        b.emplace(20);
        b.emplace(30);
        sink.on_data(b);
        sink.close();  // finalises the Parquet blob
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
    ParquetAzureSource<std::int64_t>::Options ro;
    conn(ro);
    ParquetAzureSource<std::int64_t> src(ro, int64_arrow_batcher());
    src.open();
    while (src.produce(em)) {
    }
    src.close();

    ASSERT_EQ(got.size(), 3u);
    EXPECT_EQ(got[0], 10);
    EXPECT_EQ(got[1], 20);
    EXPECT_EQ(got[2], 30);
}
