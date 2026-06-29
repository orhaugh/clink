// Offline tests for the Azure Blob Parquet sink + source: required-param validation and clean
// failure against a dead endpoint (no Azure / emulator needed). The write->read round-trip lives
// in the env-gated test_parquet_azure_live.cpp.

#include <cstdint>
#include <stdexcept>
#include <utility>

#include <gtest/gtest.h>

#include "clink/connectors/parquet_azure_sink.hpp"
#include "clink/connectors/parquet_azure_source.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/core/record.hpp"
#include "clink/core/stream_element.hpp"

using clink::Batch;
using clink::int64_arrow_batcher;
using clink::ParquetAzureSink;
using clink::ParquetAzureSource;

namespace {

ParquetAzureSink<std::int64_t>::Options dead_sink_opts() {
    ParquetAzureSink<std::int64_t>::Options o;
    o.container = "c";
    o.key = "k.parquet";
    o.account_name = "devstoreaccount1";
    o.anonymous = true;  // no credential validation; the dead endpoint is what fails
    o.blob_storage_authority = "127.0.0.1:1";  // no server
    o.blob_storage_scheme = "http";
    return o;
}

ParquetAzureSource<std::int64_t>::Options dead_source_opts() {
    ParquetAzureSource<std::int64_t>::Options o;
    o.container = "c";
    o.key = "k.parquet";
    o.account_name = "devstoreaccount1";
    o.anonymous = true;
    o.blob_storage_authority = "127.0.0.1:1";
    o.blob_storage_scheme = "http";
    return o;
}

}  // namespace

TEST(ParquetAzureSink, RequiresContainerKeyAndAccount) {
    {
        ParquetAzureSink<std::int64_t>::Options o;  // all empty
        EXPECT_THROW((ParquetAzureSink<std::int64_t>{o, int64_arrow_batcher()}),
                     std::invalid_argument);
    }
    {
        ParquetAzureSink<std::int64_t>::Options o;
        o.container = "c";
        o.key = "k.parquet";  // account_name still empty
        EXPECT_THROW((ParquetAzureSink<std::int64_t>{o, int64_arrow_batcher()}),
                     std::invalid_argument);
    }
    {
        ParquetAzureSink<std::int64_t>::Options o;
        o.container = "c";
        o.account_name = "a";  // key still empty, no bucket_assigner
        EXPECT_THROW((ParquetAzureSink<std::int64_t>{o, int64_arrow_batcher()}),
                     std::invalid_argument);
    }
}

TEST(ParquetAzureSource, RequiresContainerKeyAndAccount) {
    {
        ParquetAzureSource<std::int64_t>::Options o;  // all empty
        EXPECT_THROW((ParquetAzureSource<std::int64_t>{o, int64_arrow_batcher()}),
                     std::invalid_argument);
    }
    {
        ParquetAzureSource<std::int64_t>::Options o;
        o.container = "c";
        o.key = "k.parquet";  // account_name still empty
        EXPECT_THROW((ParquetAzureSource<std::int64_t>{o, int64_arrow_batcher()}),
                     std::invalid_argument);
    }
}

TEST(ParquetAzureSink, OpenAgainstDeadEndpointFailsCleanly) {
    // AzureFileSystem buffers writes in the background, so a dead endpoint may surface the
    // failure on the first write or at close() rather than at open() (this differs from the
    // GCS sink, whose OpenOutputStream connects eagerly). Assert the whole write cycle fails
    // cleanly with a runtime_error and that teardown does not crash.
    EXPECT_THROW(
        {
            ParquetAzureSink<std::int64_t> sink(dead_sink_opts(), int64_arrow_batcher());
            sink.open();
            Batch<std::int64_t> b;
            b.emplace(10);
            b.emplace(20);
            sink.on_data(b);
            sink.close();
        },
        std::runtime_error);
}

TEST(ParquetAzureSource, OpenAgainstDeadEndpointFailsCleanly) {
    ParquetAzureSource<std::int64_t> src(dead_source_opts(), int64_arrow_batcher());
    EXPECT_THROW(src.open(), std::runtime_error);
    EXPECT_NO_THROW(src.close());  // close after a failed open must be safe
}
