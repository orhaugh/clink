// Azure Blob Parquet LIVE integration test. SKIPPED unless CLINK_AZURE_TEST_ENDPOINT is set (an
// Azure Blob endpoint authority host:port, e.g. 127.0.0.1:10000 from an Azurite emulator). Proves
// end-to-end: the sink writes a Parquet blob that the source reads back with the same values. The
// container is created up front via Arrow's AzureFileSystem (writes do not auto-create it). Auth
// uses Azurite's well-known account + shared key.

#include <cstdint>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include <arrow/filesystem/azurefs.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/connectors/parquet_azure_sink.hpp"
#include "clink/connectors/parquet_azure_source.hpp"
#include "clink/connectors/parquet_rolling_sink.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/core/record.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/runtime_context.hpp"

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

// The rolling sink the Azure factories now build, against a real Azurite Blob endpoint: while a
// part is being written, and after a sink is torn down mid-part (the in-process
// stand-in for a kill), no .parquet object exists for it. Azure creates a blob as soon as a stream
// opens, so the sink writes <name>.inprogress and copies it into place. Parts written at a barrier
// survive a restore.
TEST(AzureParquetLive, RollingSinkNeverPublishesAnUnfinishedPart) {
    if (!azure_configured()) {
        GTEST_SKIP() << "set CLINK_AZURE_TEST_ENDPOINT (e.g. 127.0.0.1:10000 Azurite blob port)";
    }
    const std::string container = "clink-roll-" + std::to_string(static_cast<long>(::getpid()));
    arrow::fs::AzureOptions ao;
    ao.account_name = kAzuriteAccount;
    ao.blob_storage_authority = azure_endpoint();
    ao.dfs_storage_authority = azure_endpoint();
    ao.blob_storage_scheme = "http";
    ao.dfs_storage_scheme = "http";
    ASSERT_TRUE(ao.ConfigureAccountKeyCredential(kAzuriteKey).ok());
    std::shared_ptr<arrow::fs::FileSystem> fs = arrow::fs::AzureFileSystem::Make(ao).ValueOrDie();
    ASSERT_TRUE(fs->CreateDir(container, /*recursive=*/true).ok());
    const std::string dir = container + "/rolling";
    const auto factory = [fs]() -> std::shared_ptr<arrow::fs::FileSystem> { return fs; };
    const auto make = [&]() {
        clink::ParquetRollingSink<std::int64_t>::Options o;
        o.dir = dir;
        return std::make_unique<clink::ParquetRollingSink<std::int64_t>>(
            factory, std::move(o), int64_arrow_batcher());
    };
    const auto values = [](std::int64_t from, std::int64_t to) {
        Batch<std::int64_t> b;
        for (auto v = from; v < to; ++v) {
            b.emplace(v);
        }
        return b;
    };
    const auto finished_parts = [&] {
        arrow::fs::FileSelector sel;
        sel.base_dir = dir;
        sel.allow_not_found = true;
        std::vector<std::string> out;
        for (const auto& info : fs->GetFileInfo(sel).ValueOrDie()) {
            if (info.type() == arrow::fs::FileType::File && info.path().ends_with(".parquet")) {
                out.push_back(info.path());
            }
        }
        return out;
    };
    const auto rows_in = [&](const std::vector<std::string>& keys) {
        std::int64_t n = 0;
        for (const auto& k : keys) {
            auto in = fs->OpenInputFile(k).ValueOrDie();
            auto reader = parquet::arrow::OpenFile(in, arrow::default_memory_pool()).ValueOrDie();
            n += reader->parquet_reader()->metadata()->num_rows();
        }
        return n;
    };

    {
        auto first = make();
        first->open();
        first->on_data(values(0, 5));
        EXPECT_TRUE(finished_parts().empty()) << "a part being written is not visible";
        first->on_barrier(clink::CheckpointBarrier{clink::CheckpointId{1}});
        EXPECT_EQ(finished_parts().size(), 1U);
        first->on_data(values(5, 9));  // torn down before the next barrier
    }
    const auto after_kill = finished_parts();
    EXPECT_EQ(after_kill.size(), 1U) << "the unfinished part published nothing";
    EXPECT_EQ(rows_in(after_kill), 5);

    clink::RuntimeContext ctx(clink::OperatorId{11}, "pq", nullptr, nullptr);
    ctx.set_restore_from_checkpoint_id(1);
    auto resumed = make();
    resumed->attach_runtime(&ctx);
    resumed->open();
    resumed->on_data(values(5, 9));
    resumed->flush();
    resumed->close();
    const auto parts = finished_parts();
    EXPECT_EQ(parts.size(), 2U);
    EXPECT_EQ(rows_in(parts), 9);
}
