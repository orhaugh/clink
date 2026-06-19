// export_state_to_parquet (DISAGG-9): the externally-queryable state export.
// Proves the (op_id, key, value) rows of a state backend round-trip through a
// Parquet file readable by Arrow's own reader - i.e. the same on-disk format
// pyarrow / DuckDB / polars read - so engine state leaves the private formats
// as an open, queryable columnar dump.

#include <filesystem>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>

#include "clink/connectors/state_parquet_export.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;

namespace {

std::string_view sv(const char* s) {
    return std::string_view{s};
}

std::filesystem::path scratch(const std::string& tag) {
    static int n = 0;
    auto p =
        std::filesystem::temp_directory_path() / ("clink_stexport_" + tag + std::to_string(n++));
    std::error_code ec;
    std::filesystem::remove_all(p, ec);
    std::filesystem::create_directories(p, ec);
    return p / "state.parquet";
}

std::shared_ptr<arrow::Table> read_parquet(const std::filesystem::path& path) {
    auto in = arrow::io::ReadableFile::Open(path.string());
    EXPECT_TRUE(in.ok());
    auto reader_res = parquet::arrow::OpenFile(*in, arrow::default_memory_pool());
    EXPECT_TRUE(reader_res.ok());
    auto reader = std::move(*reader_res);
    std::shared_ptr<arrow::Table> table;
    EXPECT_TRUE(reader->ReadTable(&table).ok());
    return table;
}

}  // namespace

TEST(StateParquetExport, RoundTripReadableByArrow) {
    InMemoryStateBackend backend;
    backend.put(OperatorId{1}, sv("a"), sv("1"));
    backend.put(OperatorId{1}, sv("b"), sv("2"));
    backend.put(OperatorId{2}, sv("c"), sv("3"));

    const auto path = scratch("rt");
    const auto stats = export_state_to_parquet(backend, {OperatorId{1}, OperatorId{2}}, path);
    EXPECT_EQ(stats.rows, 3);
    EXPECT_EQ(stats.operators, 2u);

    auto table = read_parquet(path);
    ASSERT_EQ(table->num_rows(), 3);
    ASSERT_EQ(table->num_columns(), 3);
    EXPECT_EQ(table->schema()->field(0)->name(), "op_id");
    EXPECT_EQ(table->schema()->field(1)->name(), "key");
    EXPECT_EQ(table->schema()->field(2)->name(), "value");

    auto op_col = std::static_pointer_cast<arrow::UInt64Array>(table->column(0)->chunk(0));
    auto key_col = std::static_pointer_cast<arrow::BinaryArray>(table->column(1)->chunk(0));
    auto val_col = std::static_pointer_cast<arrow::BinaryArray>(table->column(2)->chunk(0));
    std::set<std::string> got;
    for (int i = 0; i < static_cast<int>(table->num_rows()); ++i) {
        got.insert(std::to_string(op_col->Value(i)) + ":" + key_col->GetString(i) + "=" +
                   val_col->GetString(i));
    }
    const std::set<std::string> want{"1:a=1", "1:b=2", "2:c=3"};
    EXPECT_EQ(got, want);
}

TEST(StateParquetExport, OnlyExportsRequestedOperators) {
    InMemoryStateBackend backend;
    backend.put(OperatorId{1}, sv("keep"), sv("x"));
    backend.put(OperatorId{2}, sv("drop"), sv("y"));

    const auto path = scratch("subset");
    const auto stats = export_state_to_parquet(backend, {OperatorId{1}}, path);  // op 2 omitted
    EXPECT_EQ(stats.rows, 1);

    auto table = read_parquet(path);
    ASSERT_EQ(table->num_rows(), 1);
    auto key_col = std::static_pointer_cast<arrow::BinaryArray>(table->column(1)->chunk(0));
    EXPECT_EQ(key_col->GetString(0), "keep");
}

TEST(StateParquetExport, EmptyExportIsAReadableEmptyFile) {
    InMemoryStateBackend backend;
    const auto path = scratch("empty");
    const auto stats = export_state_to_parquet(backend, {OperatorId{9}}, path);
    EXPECT_EQ(stats.rows, 0);

    auto table = read_parquet(path);
    EXPECT_EQ(table->num_rows(), 0);
    ASSERT_EQ(table->num_columns(), 3);
    EXPECT_EQ(table->schema()->field(0)->name(), "op_id");
}
