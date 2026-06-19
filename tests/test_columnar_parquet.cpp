// Custom-struct columnar Parquet: a CLINK_ARROW_FIELDS type writes and
// reads externally-typed Parquet via the generated columnar batcher.
//
//   1. ParquetSink<T> -> ParquetSource<T> round-trips a Batch<T> with
//      values + event-times intact (across multiple row groups).
//   2. Opening the produced file directly with parquet::arrow (no clink
//      classes) shows one TYPED column per field, not the opaque
//      value_bytes:binary fallback - i.e. any standard Parquet consumer
//      reads it field-by-field.

#ifndef CLINK_HAS_PARQUET
#error "test_columnar_parquet requires Parquet (ships with Arrow)"
#endif

#include <cstdint>
#include <filesystem>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>

#include "clink/connectors/columnar_parquet.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"

using namespace clink;

// Custom aggregate, unique name, namespace scope for the macro.
struct ColPqTrade {
    std::int64_t id;
    std::string symbol;
    double price;
    std::int32_t qty;
    bool buy;
};

CLINK_ARROW_FIELDS(ColPqTrade, id, symbol, price, qty, buy);

namespace {

std::filesystem::path tmp_parquet(const std::string& tag) {
    static std::mt19937_64 rng{std::random_device{}()};
    return std::filesystem::temp_directory_path() /
           ("clink_colpq_" + tag + "_" + std::to_string(rng()) + ".parquet");
}

template <typename T>
struct CapturedBatches {
    std::vector<Batch<T>> batches;
};

template <typename T>
Emitter<T> make_capturing_emitter(CapturedBatches<T>& sink) {
    return Emitter<T>{[&sink](StreamElement<T> e) {
        if (e.is_data())
            sink.batches.push_back(std::move(e.as_data()));
        return true;
    }};
}

bool trade_eq(const ColPqTrade& a, const ColPqTrade& b) {
    return a.id == b.id && a.symbol == b.symbol && a.price == b.price && a.qty == b.qty &&
           a.buy == b.buy;
}

}  // namespace

TEST(ColumnarParquet, CustomStructRoundTrip) {
    const auto path = tmp_parquet("roundtrip");

    const std::vector<ColPqTrade> rows = {
        {1, "AAPL", 191.25, 100, true},
        {2, "MSFT", 410.10, 50, false},
        {3, "NVDA", 1203.99, 7, true},
        {4, "AMZN", 178.0, 12, false},
    };

    {
        auto sink = make_columnar_parquet_sink<ColPqTrade>(path);
        sink.open();
        Batch<ColPqTrade> b1;
        b1.emplace(rows[0], EventTime{100});
        b1.emplace(rows[1], EventTime{200});
        b1.emplace(rows[2]);  // no event-time
        sink.on_data(b1);
        Batch<ColPqTrade> b2;
        b2.emplace(rows[3], EventTime{400});
        sink.on_data(b2);
        sink.close();
    }

    {
        auto source = make_columnar_parquet_source<ColPqTrade>(path);
        source.open();
        CapturedBatches<ColPqTrade> cap;
        auto em = make_capturing_emitter(cap);
        while (source.produce(em)) {
        }
        source.close();

        std::vector<Record<ColPqTrade>> all;
        for (const auto& b : cap.batches)
            for (const auto& r : b)
                all.push_back(r);

        ASSERT_EQ(all.size(), rows.size());
        for (std::size_t i = 0; i < rows.size(); ++i)
            EXPECT_TRUE(trade_eq(all[i].value(), rows[i]));
        ASSERT_TRUE(all[0].event_time().has_value());
        EXPECT_EQ(all[0].event_time()->millis(), 100);
        EXPECT_FALSE(all[2].event_time().has_value());
        ASSERT_TRUE(all[3].event_time().has_value());
        EXPECT_EQ(all[3].event_time()->millis(), 400);
    }

    std::filesystem::remove(path);
}

// Open the produced file with parquet::arrow directly (no clink classes)
// and assert the stored schema is typed columns - the property external
// readers (pyarrow/duckdb/polars) depend on.
TEST(ColumnarParquet, FileIsExternallyTyped) {
    const auto path = tmp_parquet("typed");

    {
        auto sink = make_columnar_parquet_sink<ColPqTrade>(path);
        sink.open();
        Batch<ColPqTrade> b;
        b.emplace(ColPqTrade{1, "AAPL", 191.25, 100, true}, EventTime{100});
        sink.on_data(b);
        sink.close();
    }

    auto in = arrow::io::ReadableFile::Open(path.string());
    ASSERT_TRUE(in.ok());
    auto reader_result = parquet::arrow::OpenFile(*in, arrow::default_memory_pool());
    ASSERT_TRUE(reader_result.ok());
    auto reader = std::move(*reader_result);

    std::shared_ptr<arrow::Schema> schema;
    ASSERT_TRUE(reader->GetSchema(&schema).ok());

    ASSERT_EQ(schema->num_fields(), 6);
    EXPECT_EQ(schema->field(0)->name(), "event_time");
    EXPECT_EQ(schema->field(1)->name(), "id");
    EXPECT_EQ(schema->field(1)->type()->id(), arrow::Type::INT64);
    EXPECT_EQ(schema->field(2)->name(), "symbol");
    EXPECT_EQ(schema->field(2)->type()->id(), arrow::Type::STRING);
    EXPECT_EQ(schema->field(3)->name(), "price");
    EXPECT_EQ(schema->field(3)->type()->id(), arrow::Type::DOUBLE);
    EXPECT_EQ(schema->field(4)->name(), "qty");
    EXPECT_EQ(schema->field(4)->type()->id(), arrow::Type::INT32);
    EXPECT_EQ(schema->field(5)->name(), "buy");
    EXPECT_EQ(schema->field(5)->type()->id(), arrow::Type::BOOL);
    for (int i = 0; i < schema->num_fields(); ++i)
        EXPECT_NE(schema->field(i)->type()->id(), arrow::Type::BINARY);

    std::filesystem::remove(path);
}
