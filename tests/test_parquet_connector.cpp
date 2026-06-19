// Parquet sink/source round-trip + externally-readable contract tests.
//
// Goals:
//   1. Round-trip a Batch<int64> and Batch<string> through
//      ParquetSink → ParquetSource and confirm identity.
//   2. Open the sink-produced file directly via parquet::arrow's
//      reader (no clink classes) to lock in the "any standard Parquet
//      consumer can read this" property - same shape as the
//      InMemoryStateBackend.SnapshotBytesParseAsArrowIPC test.

#include <algorithm>
#include <filesystem>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>

#include "clink/connectors/parquet_sink.hpp"
#include "clink/connectors/parquet_source.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/core/codec.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;

namespace {

std::filesystem::path tmp_parquet(const std::string& tag) {
    static std::mt19937_64 rng{std::random_device{}()};
    auto p = std::filesystem::temp_directory_path() /
             ("clink_parquet_test_" + tag + "_" + std::to_string(rng()) + ".parquet");
    return p;
}

// Emitter capturing data batches. Emitter is not virtual; the forward
// constructor takes a std::function<bool(StreamElement)> we route data
// frames through.
template <typename T>
struct CapturedBatches {
    std::vector<Batch<T>> batches;
};

template <typename T>
Emitter<T> make_capturing_emitter(CapturedBatches<T>& sink) {
    return Emitter<T>{[&sink](StreamElement<T> e) {
        if (e.is_data()) {
            sink.batches.push_back(std::move(e.as_data()));
        }
        return true;
    }};
}

}  // namespace

TEST(ParquetConnector, Int64RoundTripPreservesValuesAndEventTimes) {
    const auto path = tmp_parquet("int64");

    {
        ParquetSink<std::int64_t> sink(path, int64_arrow_batcher());
        sink.open();
        Batch<std::int64_t> b1;
        b1.emplace(10, EventTime{100});
        b1.emplace(20, EventTime{200});
        b1.emplace(30);  // no event_time
        sink.on_data(b1);
        Batch<std::int64_t> b2;
        b2.emplace(40, EventTime{400});
        sink.on_data(b2);
        sink.close();
    }

    {
        ParquetSource<std::int64_t> source(path, int64_arrow_batcher());
        source.open();
        CapturedBatches<std::int64_t> cap;
        auto em = make_capturing_emitter(cap);
        while (source.produce(em)) {
            // drain
        }
        source.close();

        // The Arrow reader may coalesce written row groups into one or
        // more RecordBatches at its discretion - we don't depend on
        // exact batch boundaries on read. Just flatten and check.
        std::vector<Record<std::int64_t>> all;
        for (const auto& b : cap.batches) {
            for (const auto& r : b) {
                all.push_back(r);
            }
        }
        ASSERT_EQ(all.size(), 4u);
        EXPECT_EQ(all[0].value(), 10);
        ASSERT_TRUE(all[0].event_time().has_value());
        EXPECT_EQ(all[0].event_time()->millis(), 100);
        EXPECT_EQ(all[1].value(), 20);
        EXPECT_EQ(all[2].value(), 30);
        EXPECT_FALSE(all[2].event_time().has_value());
        EXPECT_EQ(all[3].value(), 40);
        EXPECT_EQ(all[3].event_time()->millis(), 400);
    }

    std::filesystem::remove(path);
}

TEST(ParquetConnector, StringRoundTripPreservesValuesAndEventTimes) {
    const auto path = tmp_parquet("string");

    {
        ParquetSink<std::string> sink(path, string_arrow_batcher());
        sink.open();
        Batch<std::string> b;
        b.emplace(std::string{"alpha"}, EventTime{1});
        b.emplace(std::string{"beta"});
        b.emplace(std::string{"gamma"}, EventTime{3});
        sink.on_data(b);
        sink.close();
    }

    {
        ParquetSource<std::string> source(path, string_arrow_batcher());
        source.open();
        CapturedBatches<std::string> cap;
        auto em = make_capturing_emitter(cap);
        while (source.produce(em)) {
            // drain
        }
        source.close();

        ASSERT_EQ(cap.batches.size(), 1u);
        const auto& got = cap.batches[0];
        ASSERT_EQ(got.size(), 3u);
        EXPECT_EQ(got[0].value(), "alpha");
        EXPECT_EQ(got[1].value(), "beta");
        EXPECT_FALSE(got[1].event_time().has_value());
        EXPECT_EQ(got[2].value(), "gamma");
    }

    std::filesystem::remove(path);
}

TEST(ParquetConnector, FileIsReadableByStandardParquetReader) {
    // Write via ParquetSink, then open via parquet::arrow's FileReader
    // directly - no clink wrapping - to lock in the on-disk shape as a
    // valid Parquet file (any pyarrow/duckdb/polars consumer can read).
    const auto path = tmp_parquet("external");

    {
        ParquetSink<std::int64_t> sink(path, int64_arrow_batcher());
        sink.open();
        Batch<std::int64_t> b;
        for (std::int64_t i = 0; i < 100; ++i) {
            b.emplace(i, EventTime{i * 10});
        }
        sink.on_data(b);
        sink.close();
    }

    auto in_result = arrow::io::ReadableFile::Open(path.string());
    ASSERT_TRUE(in_result.ok()) << in_result.status().ToString();
    auto reader_result = parquet::arrow::OpenFile(*in_result, arrow::default_memory_pool());
    ASSERT_TRUE(reader_result.ok()) << reader_result.status().ToString();
    auto reader = std::move(*reader_result);

    std::shared_ptr<arrow::Schema> schema;
    ASSERT_TRUE(reader->GetSchema(&schema).ok());
    ASSERT_EQ(schema->num_fields(), 2);
    EXPECT_EQ(schema->field(0)->name(), "event_time");
    EXPECT_EQ(schema->field(0)->type()->id(), arrow::Type::INT64);
    EXPECT_TRUE(schema->field(0)->nullable());
    EXPECT_EQ(schema->field(1)->name(), "value");
    EXPECT_EQ(schema->field(1)->type()->id(), arrow::Type::INT64);

    std::shared_ptr<arrow::Table> table;
    ASSERT_TRUE(reader->ReadTable(&table).ok());
    EXPECT_EQ(table->num_rows(), 100);
    EXPECT_EQ(table->num_columns(), 2);

    std::filesystem::remove(path);
}

TEST(ParquetConnector, SourceSchemaMismatchThrows) {
    // Write an int64 file, try to open it as a string source - the
    // schema check at open() should reject with a clear message.
    const auto path = tmp_parquet("mismatch");

    {
        ParquetSink<std::int64_t> sink(path, int64_arrow_batcher());
        sink.open();
        Batch<std::int64_t> b;
        b.emplace(1);
        sink.on_data(b);
        sink.close();
    }

    ParquetSource<std::string> source(path, string_arrow_batcher());
    EXPECT_THROW(source.open(), std::runtime_error);

    std::filesystem::remove(path);
}

TEST(ParquetConnector, EmptyBatchProducesNoRowGroup) {
    // Sink ignores empty batches (writing a zero-row row group is legal
    // but wasteful). Confirm the file still finalises cleanly with no
    // logical rows, and the source's produce() loop terminates without
    // emitting anything.
    const auto path = tmp_parquet("empty");

    {
        ParquetSink<std::int64_t> sink(path, int64_arrow_batcher());
        sink.open();
        sink.on_data(Batch<std::int64_t>{});  // empty
        sink.close();
    }

    ParquetSource<std::int64_t> source(path, int64_arrow_batcher());
    source.open();
    CapturedBatches<std::int64_t> cap;
    auto em = make_capturing_emitter(cap);
    while (source.produce(em)) {
    }
    source.close();
    EXPECT_TRUE(cap.batches.empty());

    std::filesystem::remove(path);
}

// #57: source replay. Read part of the file, snapshot the batch cursor, then a
// fresh source restores it and drains the rest - every record appears exactly
// once across the restart (no re-read, none dropped), regardless of how the
// Arrow reader chunks row groups into RecordBatches.
TEST(ParquetConnector, SourceReplaysFromSnapshottedBatch) {
    const auto path = tmp_parquet("replay");
    {
        ParquetSink<std::int64_t> sink(path, int64_arrow_batcher());
        sink.open();
        for (std::int64_t v : {10, 20, 30, 40, 50}) {
            Batch<std::int64_t> b;
            b.emplace(v);
            sink.on_data(b);  // one row group per value
        }
        sink.close();
    }

    InMemoryStateBackend backend;
    const OperatorId op_id{42};
    std::vector<std::int64_t> seen;
    auto collect = [&seen](const CapturedBatches<std::int64_t>& cap) {
        for (const auto& b : cap.batches) {
            for (const auto& r : b) {
                seen.push_back(r.value());
            }
        }
    };

    // First run: emit one batch, snapshot the cursor between produce() calls.
    {
        ParquetSource<std::int64_t> s1(path, int64_arrow_batcher());
        s1.open();
        CapturedBatches<std::int64_t> cap;
        auto em = make_capturing_emitter(cap);
        s1.produce(em);
        collect(cap);
        s1.snapshot_offset(backend, op_id, CheckpointId{1});
        s1.close();
    }
    // Restart: restore (before open), then drain the remainder.
    {
        ParquetSource<std::int64_t> s2(path, int64_arrow_batcher());
        ASSERT_TRUE(s2.restore_offset(backend, op_id));
        s2.open();
        CapturedBatches<std::int64_t> cap;
        auto em = make_capturing_emitter(cap);
        while (s2.produce(em)) {
        }
        collect(cap);
        s2.close();
    }

    std::sort(seen.begin(), seen.end());
    EXPECT_EQ(seen, (std::vector<std::int64_t>{10, 20, 30, 40, 50}))
        << "every record must appear exactly once across the restart";

    std::filesystem::remove(path);
}
