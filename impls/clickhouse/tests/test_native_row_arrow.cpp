// Row batches into typed Arrow chunks: the shared rule for the batcher's own
// scalar types, and the per-cell refusal for every type the sink converts
// itself.

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

#include <arrow/api.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/json/from_string.h>
#include <arrow/util/byte_size.h>
#include <gtest/gtest.h>

#include "clink/config/decimal.hpp"
#include "clink/config/json.hpp"
#include "clink/core/record.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_columnar_batcher.hpp"

#include "native/arrow_to_block.hpp"
#include "native/errors.hpp"
#include "native/intake.hpp"
#include "native/row_arrow.hpp"
#include "native/types.hpp"

namespace clink::clickhouse::native {
namespace {

using clink::config::JsonValue;

std::vector<SqlColumn> ra_columns(const std::string& spec) {
    return parse_sql_column_types(spec);
}

// One record per NDJSON line, so a test reads as the rows it feeds.
Batch<sql::Row> ra_rows(const std::vector<std::string>& lines) {
    std::vector<Record<sql::Row>> records;
    for (const auto& line : lines) {
        auto object = clink::config::parse_object(line);
        EXPECT_TRUE(object.has_value()) << line;
        sql::Row row;
        if (object) {
            row.values = sql::row_columns_from_json(std::move(*object));
        }
        records.emplace_back(std::move(row));
    }
    return Batch<sql::Row>{std::move(records)};
}

// One record from values JSON text cannot spell, such as a decimal string.
Batch<sql::Row> ra_value_rows(
    const std::vector<std::vector<std::pair<std::string, JsonValue>>>& rows) {
    std::vector<Record<sql::Row>> records;
    for (const auto& entries : rows) {
        clink::config::JsonObject object;
        for (const auto& [name, value] : entries) {
            object[name] = value;
        }
        sql::Row row;
        row.values = sql::row_columns_from_json(std::move(object));
        records.emplace_back(std::move(row));
    }
    return Batch<sql::Row>{std::move(records)};
}

JsonValue ra_decimal(std::int64_t unscaled, int scale) {
    return clink::config::make_dec_value(
        clink::config::Decimal{arrow::Decimal128(unscaled), scale});
}

std::shared_ptr<arrow::RecordBatch> ra_build(const RowArrowBuilder& builder,
                                             const Batch<sql::Row>& batch) {
    auto chunk = builder.build(batch);
    EXPECT_NE(chunk, nullptr);
    if (chunk) {
        const auto status = chunk->ValidateFull();
        EXPECT_TRUE(status.ok()) << status.ToString();
        EXPECT_TRUE(chunk->schema()->Equals(*builder.schema()));
    }
    return chunk;
}

// The ConversionError a batch raises, or a marker the assertions cannot match.
ConversionError ra_refusal(const RowArrowBuilder& builder, const Batch<sql::Row>& batch) {
    try {
        (void)builder.build(batch);
    } catch (const ConversionError& e) {
        EXPECT_EQ(e.code(), code::kConversionFailed);
        return e;
    }
    ADD_FAILURE() << "no ConversionError";
    return ConversionError("<none>", -1, "<none>");
}

// The full message for one cell rejected in the given column and row.
std::string ra_message(const std::string& column, std::int64_t row, const std::string& reason) {
    return "[clickhouse.conversion_failed] column `" + column + "`, row " + std::to_string(row) +
           ": " + reason;
}

template <typename ArrayT>
std::shared_ptr<ArrayT> ra_col(const std::shared_ptr<arrow::RecordBatch>& chunk, int i) {
    return std::static_pointer_cast<ArrayT>(chunk->column(i));
}

std::vector<std::int64_t> ra_int64_values(const arrow::Array& array) {
    const auto& typed = static_cast<const arrow::Int64Array&>(array);
    std::vector<std::int64_t> out;
    for (std::int64_t i = 0; i < typed.length(); ++i) {
        out.push_back(typed.Value(i));
    }
    return out;
}

TEST(NativeRowArrow, SchemaIsTheDeclaredColumnsInOrderAllNullable) {
    const RowArrowBuilder builder(
        ra_columns("t:TINYINT;s:SMALLINT;i:INTEGER;b:BIGINT;r:REAL;f:DOUBLE;o:BOOLEAN;v:VARCHAR;"
                   "d:DECIMAL(10, 2);dt:DATE;ts:TIMESTAMP(9);tz:TIMESTAMP(3) WITH TIME ZONE;"
                   "a:BIGINT ARRAY;m:MAP<VARCHAR, BIGINT>;w:ROW<x BIGINT, y VARCHAR>"));
    const auto expected = arrow::schema({
        arrow::field("t", arrow::int8()),
        arrow::field("s", arrow::int16()),
        arrow::field("i", arrow::int32()),
        arrow::field("b", arrow::int64()),
        arrow::field("r", arrow::float32()),
        arrow::field("f", arrow::float64()),
        arrow::field("o", arrow::boolean()),
        arrow::field("v", arrow::utf8()),
        arrow::field("d", arrow::decimal128(10, 2)),
        arrow::field("dt", arrow::date32()),
        arrow::field("ts", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("tz", arrow::timestamp(arrow::TimeUnit::MILLI, "UTC")),
        arrow::field("a", arrow::list(arrow::int64())),
        arrow::field("m", arrow::map(arrow::utf8(), arrow::int64())),
        arrow::field(
            "w",
            arrow::struct_({arrow::field("x", arrow::int64()), arrow::field("y", arrow::utf8())})),
    });
    ASSERT_NE(builder.schema(), nullptr);
    EXPECT_TRUE(builder.schema()->Equals(*expected)) << builder.schema()->ToString();
    for (const auto& field : builder.schema()->fields()) {
        EXPECT_TRUE(field->nullable()) << field->name();
    }
}

TEST(NativeRowArrow, ColumnsWithNoArrowLayoutAreRefusedAtConstruction) {
    const std::vector<std::pair<std::string, std::string>> cases = {
        {"t:TIME", "column `t`: TIME has no Arrow layout in the native sink"},
        {"b:BYTEA", "column `b`: BYTEA has no Arrow layout in the native sink"},
        {"x:INTERVAL", "column `x`: INTERVAL has no Arrow layout in the native sink"},
        {"a:TIME ARRAY", "column `a`: TIME ARRAY has no Arrow layout in the native sink"},
    };
    for (const auto& [spec, message] : cases) {
        try {
            const RowArrowBuilder builder(ra_columns(spec));
            ADD_FAILURE() << "no refusal for " << spec;
        } catch (const NativeSinkError& e) {
            EXPECT_EQ(e.code(), code::kColumnPlan) << spec;
            EXPECT_EQ(std::string(e.what()), "[clickhouse.column_plan] " + message);
        }
    }
}

TEST(NativeRowArrow, EmptyBatchGivesAnEmptyChunkWithTheSchema) {
    const RowArrowBuilder builder(ra_columns("b:BIGINT;ts:TIMESTAMP(3);a:SMALLINT ARRAY"));
    const auto chunk = ra_build(builder, Batch<sql::Row>{});
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->num_rows(), 0);
    EXPECT_EQ(chunk->num_columns(), 3);
}

// Every TIMESTAMP(p), with or without a zone, holds the Row's epoch
// milliseconds unchanged: the declared precision rescales nothing.
TEST(NativeRowArrow, EveryTimestampPrecisionStoresEpochMillisUnchanged) {
    const RowArrowBuilder builder(
        ra_columns("t0:TIMESTAMP(0);t3:TIMESTAMP(3);t6:TIMESTAMP(6);t9:TIMESTAMP(9);"
                   "tz:TIMESTAMP(6) WITH TIME ZONE"));
    const std::vector<std::string> cells = {
        "1700000000123",
        "-1",
        "-86400001",
        "0",
        "9223372036854775807",
        "-9223372036854775808",
        R"("1700000000123")",
        R"("-62135596800000")",
        "1700000000000.0",
    };
    const std::vector<std::int64_t> expected = {
        1700000000123,
        -1,
        -86400001,
        0,
        std::numeric_limits<std::int64_t>::max(),
        std::numeric_limits<std::int64_t>::min(),
        1700000000123,
        -62135596800000,
        1700000000000,
    };
    std::vector<std::string> lines;
    for (const auto& cell : cells) {
        lines.push_back(R"({"t0":)" + cell + R"(,"t3":)" + cell + R"(,"t6":)" + cell + R"(,"t9":)" +
                        cell + R"(,"tz":)" + cell + "}");
    }
    const auto chunk = ra_build(builder, ra_rows(lines));
    ASSERT_NE(chunk, nullptr);
    for (int c = 0; c < chunk->num_columns(); ++c) {
        const auto& column = *chunk->column(c);
        EXPECT_EQ(column.type()->id(), arrow::Type::TIMESTAMP);
        EXPECT_EQ(static_cast<const arrow::TimestampType&>(*column.type()).unit(),
                  arrow::TimeUnit::MILLI);
        EXPECT_EQ(column.null_count(), 0);
        const auto& values = static_cast<const arrow::TimestampArray&>(column);
        for (std::int64_t i = 0; i < values.length(); ++i) {
            EXPECT_EQ(values.Value(i), expected[static_cast<std::size_t>(i)])
                << chunk->schema()->field(c)->name() << " row " << i;
        }
    }
    EXPECT_EQ(static_cast<const arrow::TimestampType&>(*chunk->column(4)->type()).timezone(),
              "UTC");
}

TEST(NativeRowArrow, IsoTimestampTextFailsNamingColumnAndRowWithoutTheText) {
    const RowArrowBuilder builder(ra_columns("id:BIGINT;ts:TIMESTAMP(3)"));
    const auto e = ra_refusal(
        builder,
        ra_rows({R"({"id":1,"ts":1700000000123})", R"({"id":2,"ts":"2024-01-01T00:00:00.000Z"})"}));
    EXPECT_EQ(e.column(), "ts");
    EXPECT_EQ(e.row(), 1);
    EXPECT_EQ(std::string(e.what()),
              ra_message("ts",
                         1,
                         "timestamp text is not epoch milliseconds for TIMESTAMP(3), got "
                         "text(len 24)"));
    EXPECT_EQ(std::string(e.what()).find("2024"), std::string::npos);
}

TEST(NativeRowArrow, TimestampCellsOfOtherKindsFail) {
    const RowArrowBuilder builder(ra_columns("ts:TIMESTAMP(6) WITH TIME ZONE"));
    const std::string type = "TIMESTAMP(6) WITH TIME ZONE";
    const std::vector<std::pair<std::string, std::string>> cases = {
        {R"({"ts":1.5})", "value 1.5 is not a whole number for " + type},
        {R"({"ts":true})", "expected epoch milliseconds for " + type + ", got a boolean"},
        {R"({"ts":[1]})", "expected epoch milliseconds for " + type + ", got array(size 1)"},
        {R"({"ts":"99999999999999999999"})", "text(len 20) out of range for " + type},
        {R"({"ts":"+5"})",
         "timestamp text is not epoch milliseconds for " + type + ", got text(len 2)"},
        {R"({"ts":""})",
         "timestamp text is not epoch milliseconds for " + type + ", got text(len 0)"},
        {R"({"ts":1e300})", "value 1e+300 out of range for " + type},
    };
    for (const auto& [line, reason] : cases) {
        const auto e = ra_refusal(builder, ra_rows({line}));
        EXPECT_EQ(std::string(e.what()), ra_message("ts", 0, reason)) << line;
    }
}

TEST(NativeRowArrow, DateFromDaysAndCivilTextIncludingBefore1970) {
    const RowArrowBuilder builder(ra_columns("d:DATE"));
    const std::vector<std::pair<std::string, std::int32_t>> cases = {
        {"0", 0},
        {"-1", -1},
        {"19723", 19723},
        {"2147483647", std::numeric_limits<std::int32_t>::max()},
        {"-2147483648", std::numeric_limits<std::int32_t>::min()},
        {"7.0", 7},
        {R"("1970-01-01")", 0},
        {R"("1969-12-31")", -1},
        {R"("2024-01-01")", 19723},
        {R"("2024-02-29")", 19782},
        {R"("2000-02-29")", 11016},
        {R"("1900-01-01")", -25567},
        {R"("2299-12-31")", 120529},
        {R"("0000-03-01")", -719468},
        {R"("-0001-12-31")", -719529},
        {R"("19723")", 19723},
        {R"("-25567")", -25567},
    };
    std::vector<std::string> lines;
    for (const auto& [cell, days] : cases) {
        lines.push_back(R"({"d":)" + cell + "}");
    }
    const auto chunk = ra_build(builder, ra_rows(lines));
    ASSERT_NE(chunk, nullptr);
    const auto dates = ra_col<arrow::Date32Array>(chunk, 0);
    ASSERT_EQ(dates->length(), static_cast<std::int64_t>(cases.size()));
    EXPECT_EQ(dates->null_count(), 0);
    for (std::size_t i = 0; i < cases.size(); ++i) {
        EXPECT_EQ(dates->Value(static_cast<std::int64_t>(i)), cases[i].second) << cases[i].first;
    }
}

TEST(NativeRowArrow, InvalidDateCellsFail) {
    const RowArrowBuilder builder(ra_columns("d:DATE"));
    const std::string text_reason = "date text is not days or a valid YYYY-MM-DD date for DATE, ";
    const std::vector<std::pair<std::string, std::string>> cases = {
        {R"({"d":"2023-02-29"})", text_reason + "got text(len 10)"},
        {R"({"d":"1900-02-29"})", text_reason + "got text(len 10)"},
        {R"({"d":"2024-13-01"})", text_reason + "got text(len 10)"},
        {R"({"d":"2024-00-10"})", text_reason + "got text(len 10)"},
        {R"({"d":"2024-04-31"})", text_reason + "got text(len 10)"},
        {R"({"d":"2024-01-00"})", text_reason + "got text(len 10)"},
        {R"({"d":"2024-1-01"})", text_reason + "got text(len 9)"},
        {R"({"d":"01/02/2024"})", text_reason + "got text(len 10)"},
        {R"({"d":"2024-01-01T00:00:00Z"})", text_reason + "got text(len 20)"},
        {R"({"d":"yesterday"})", text_reason + "got text(len 9)"},
        {R"({"d":""})", text_reason + "got text(len 0)"},
        {R"({"d":2147483648})", "value 2147483648 out of range for DATE"},
        {R"({"d":"2147483648"})", "text(len 10) out of range for DATE"},
        {R"({"d":1.5})", "value 1.5 is not a whole number for DATE"},
        {R"({"d":false})", "expected days since 1970-01-01 or YYYY-MM-DD for DATE, got a boolean"},
        {R"({"d":{"y":2024}})",
         "expected days since 1970-01-01 or YYYY-MM-DD for DATE, got object(size 1)"},
    };
    for (const auto& [line, reason] : cases) {
        const auto e = ra_refusal(builder, ra_rows({line}));
        EXPECT_EQ(std::string(e.what()), ra_message("d", 0, reason)) << line;
    }
}

TEST(NativeRowArrow, SmallintAndTinyintOutOfRangeFail) {
    const RowArrowBuilder smallint(ra_columns("s:SMALLINT"));
    const RowArrowBuilder tinyint(ra_columns("t:TINYINT"));
    EXPECT_EQ(std::string(ra_refusal(smallint, ra_rows({R"({"s":40000})"})).what()),
              ra_message("s", 0, "value 40000 out of range for SMALLINT"));
    EXPECT_EQ(std::string(ra_refusal(tinyint, ra_rows({R"({"t":200})"})).what()),
              ra_message("t", 0, "value 200 out of range for TINYINT"));
    EXPECT_EQ(std::string(ra_refusal(smallint, ra_rows({R"({"s":-32769})"})).what()),
              ra_message("s", 0, "value -32769 out of range for SMALLINT"));
    EXPECT_EQ(std::string(ra_refusal(tinyint, ra_rows({R"({"t":-129})"})).what()),
              ra_message("t", 0, "value -129 out of range for TINYINT"));
}

TEST(NativeRowArrow, SmallintAndTinyintTakeWholeNumbersAndDigitText) {
    const RowArrowBuilder builder(ra_columns("s:SMALLINT;t:TINYINT"));
    const auto chunk = ra_build(builder,
                                ra_rows({
                                    R"({"s":32767,"t":127})",
                                    R"({"s":-32768,"t":-128})",
                                    R"({"s":7.0,"t":-3.0})",
                                    R"({"s":"123","t":"-12"})",
                                }));
    ASSERT_NE(chunk, nullptr);
    const auto s = ra_col<arrow::Int16Array>(chunk, 0);
    const auto t = ra_col<arrow::Int8Array>(chunk, 1);
    EXPECT_EQ(s->Value(0), 32767);
    EXPECT_EQ(s->Value(1), -32768);
    EXPECT_EQ(s->Value(2), 7);
    EXPECT_EQ(s->Value(3), 123);
    EXPECT_EQ(t->Value(0), 127);
    EXPECT_EQ(t->Value(1), -128);
    EXPECT_EQ(t->Value(2), -3);
    EXPECT_EQ(t->Value(3), -12);
}

TEST(NativeRowArrow, SmallintCellsOfOtherKindsFail) {
    const RowArrowBuilder builder(ra_columns("s:SMALLINT"));
    const std::vector<std::pair<std::string, std::string>> cases = {
        {R"({"s":1.5})", "value 1.5 is not a whole number for SMALLINT"},
        {R"({"s":"abc"})", "expected a whole number for SMALLINT, got text(len 3)"},
        {R"({"s":"1.0"})", "expected a whole number for SMALLINT, got text(len 3)"},
        {R"({"s":"40000"})", "text(len 5) out of range for SMALLINT"},
        {R"({"s":true})", "expected a whole number for SMALLINT, got a boolean"},
        {R"({"s":[1]})", "expected a whole number for SMALLINT, got array(size 1)"},
    };
    for (const auto& [line, reason] : cases) {
        const auto e = ra_refusal(builder, ra_rows({line}));
        EXPECT_EQ(std::string(e.what()), ra_message("s", 0, reason)) << line;
    }
}

TEST(NativeRowArrow, ConversionErrorNamesTheRowWithinTheBatch) {
    const RowArrowBuilder builder(ra_columns("id:BIGINT;s:SMALLINT"));
    const auto e = ra_refusal(builder,
                              ra_rows({R"({"id":1,"s":1})",
                                       R"({"id":2,"s":2})",
                                       R"({"id":3,"s":99999})",
                                       R"({"id":4,"s":4})"}));
    EXPECT_EQ(e.column(), "s");
    EXPECT_EQ(e.row(), 2);
    EXPECT_EQ(std::string(e.what()), ra_message("s", 2, "value 99999 out of range for SMALLINT"));
}

// A JSON null, an absent column and an absent ROW field are NULL whatever
// the type, composites included.
TEST(NativeRowArrow, NullAndAbsentCellsAreNullForEveryType) {
    const RowArrowBuilder builder(
        ra_columns("t:TINYINT;s:SMALLINT;i:INTEGER;b:BIGINT;r:REAL;f:DOUBLE;o:BOOLEAN;v:VARCHAR;"
                   "d:DECIMAL(10, 2);dt:DATE;ts:TIMESTAMP(3);a:BIGINT ARRAY;m:MAP<VARCHAR, BIGINT>;"
                   "w:ROW<x BIGINT, y VARCHAR>"));
    const auto chunk =
        ra_build(builder,
                 ra_rows({
                     R"({"t":null,"s":null,"i":null,"b":null,"r":null,"f":null,"o":null,"v":null,)"
                     R"("d":null,"dt":null,"ts":null,"a":null,"m":null,"w":null})",
                     R"({"unrelated":1})",
                 }));
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->num_rows(), 2);
    for (int c = 0; c < chunk->num_columns(); ++c) {
        EXPECT_EQ(chunk->column(c)->null_count(), 2) << chunk->schema()->field(c)->name();
    }
}

// The shared types keep the batcher's rule at the top level: they are built
// by the batcher's own column builder, so the two agree cell for cell.
TEST(NativeRowArrow, SharedTypesMatchTheBatchersOwnColumnExactly) {
    const auto columns =
        ra_columns("b:BIGINT;i:INTEGER;r:REAL;f:DOUBLE;o:BOOLEAN;v:VARCHAR;d:DECIMAL(10, 2)");
    const RowArrowBuilder builder(columns);
    const auto batch = ra_value_rows({
        {{"b", JsonValue{std::int64_t{9007199254740993}}},
         {"i", JsonValue{-7}},
         {"r", JsonValue{1.25}},
         {"f", JsonValue{0.1}},
         {"o", JsonValue{true}},
         {"v", JsonValue{"text"}},
         {"d", ra_decimal(12345, 2)}},
        {{"b", JsonValue{"abc"}},
         {"i", JsonValue{std::int64_t{2147483648}}},
         {"r", JsonValue{true}},
         {"f", JsonValue{"x"}},
         {"o", JsonValue{1}},
         {"v", JsonValue{42}},
         {"d", JsonValue{1.5}}},
        {{"b", JsonValue{1.9}},
         {"i", JsonValue{1e10}},
         {"v", JsonValue{1e-7}},
         {"d", JsonValue{3}}},
    });
    const auto chunk = ra_build(builder, batch);
    ASSERT_NE(chunk, nullptr);
    for (std::size_t c = 0; c < columns.size(); ++c) {
        const auto type = arrow_type_for(columns[c].type);
        const auto reference = sql::row_columnar_detail::build_column(
            columns[c].name, sql::row_columnar_detail::effective_type(type), batch);
        ASSERT_NE(reference, nullptr);
        EXPECT_TRUE(chunk->column(static_cast<int>(c))->Equals(*reference))
            << columns[c].name << ": " << chunk->column(static_cast<int>(c))->ToString() << " vs "
            << reference->ToString();
    }
}

// A wrong-kind or out-of-range top-level cell of a shared type is NULL, not
// an error. VARCHAR renders every kind through the shared rendering, so it has
// no wrong kind: a number or an object lands as its text.
TEST(NativeRowArrow, WrongKindTopLevelSharedCellsBecomeNull) {
    const RowArrowBuilder builder(ra_columns("b:BIGINT;i:INTEGER;v:VARCHAR;d:DECIMAL(10, 2)"));
    const auto chunk = ra_build(builder,
                                ra_rows({
                                    R"({"b":"abc","i":"5","v":42,"d":"1.5"})",
                                    R"({"b":true,"i":2147483648,"v":{"k":1},"d":1.5})",
                                    R"({"b":[1],"i":1e10,"v":1e-7,"d":true})",
                                }));
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->column(0)->null_count(), 3);
    EXPECT_EQ(chunk->column(1)->null_count(), 3);
    EXPECT_EQ(chunk->column(3)->null_count(), 3);
    const auto v = ra_col<arrow::StringArray>(chunk, 2);
    EXPECT_EQ(v->null_count(), 0);
    EXPECT_EQ(v->GetString(0), "42");
    EXPECT_EQ(v->GetString(1), R"({"k":1})");
    EXPECT_EQ(v->GetString(2), "1e-07");
}

TEST(NativeRowArrow, BigintPastTwoToTheFiftyThirdIsExact) {
    const RowArrowBuilder builder(ra_columns("b:BIGINT;a:BIGINT ARRAY;m:MAP<BIGINT, BIGINT>"));
    const auto chunk =
        ra_build(builder,
                 ra_rows({R"({"b":9007199254740993,"a":[9007199254740993,-9007199254740993],)"
                          R"("m":{"9007199254740993":9007199254740995}})"}));
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(ra_col<arrow::Int64Array>(chunk, 0)->Value(0), 9007199254740993);
    const auto list = ra_col<arrow::ListArray>(chunk, 1);
    EXPECT_EQ(ra_int64_values(*list->values()),
              (std::vector<std::int64_t>{9007199254740993, -9007199254740993}));
    const auto map = ra_col<arrow::MapArray>(chunk, 2);
    EXPECT_EQ(ra_int64_values(*map->keys()), (std::vector<std::int64_t>{9007199254740993}));
    EXPECT_EQ(ra_int64_values(*map->items()), (std::vector<std::int64_t>{9007199254740995}));
}

TEST(NativeRowArrow, ArraysKeepElementsNullsAndEmptiness) {
    const RowArrowBuilder builder(
        ra_columns("a:BIGINT ARRAY;v:VARCHAR ARRAY;n:SMALLINT ARRAY ARRAY"));
    const auto chunk = ra_build(builder,
                                ra_rows({
                                    R"({"a":[1,null,3],"v":[1,"a",{"k":1}],"n":[[1,2],[],[3]]})",
                                    R"({"a":[],"v":[],"n":[]})",
                                    R"({"a":null,"v":null,"n":null})",
                                }));
    ASSERT_NE(chunk, nullptr);

    const auto a = ra_col<arrow::ListArray>(chunk, 0);
    EXPECT_EQ(a->value_length(0), 3);
    EXPECT_EQ(a->value_length(1), 0);
    EXPECT_FALSE(a->IsNull(1));
    EXPECT_TRUE(a->IsNull(2));
    const auto& a_values = static_cast<const arrow::Int64Array&>(*a->values());
    EXPECT_EQ(a_values.Value(0), 1);
    EXPECT_TRUE(a_values.IsNull(1));
    EXPECT_EQ(a_values.Value(2), 3);

    const auto v = ra_col<arrow::ListArray>(chunk, 1);
    const auto& v_values = static_cast<const arrow::StringArray&>(*v->values());
    ASSERT_EQ(v_values.length(), 3);
    EXPECT_EQ(v_values.GetString(0), "1");
    EXPECT_EQ(v_values.GetString(1), "a");
    EXPECT_EQ(v_values.GetString(2), R"({"k":1})");

    const auto n = ra_col<arrow::ListArray>(chunk, 2);
    EXPECT_EQ(n->value_length(0), 3);
    const auto& inner = static_cast<const arrow::ListArray&>(*n->values());
    EXPECT_EQ(inner.value_length(0), 2);
    EXPECT_EQ(inner.value_length(1), 0);
    EXPECT_EQ(inner.value_length(2), 1);
    const auto& leaves = static_cast<const arrow::Int16Array&>(*inner.values());
    EXPECT_EQ(leaves.Value(0), 1);
    EXPECT_EQ(leaves.Value(1), 2);
    EXPECT_EQ(leaves.Value(2), 3);
}

// Inside a composite even the shared types refuse a wrong-kind element: the
// shared list rule would have stored a NULL.
TEST(NativeRowArrow, WrongKindElementInsideABigintArrayFails) {
    const RowArrowBuilder builder(ra_columns("a:BIGINT ARRAY"));
    EXPECT_EQ(std::string(ra_refusal(builder, ra_rows({R"({"a":[1,"two",3]})"})).what()),
              ra_message("a", 0, "element 1: expected a number for BIGINT, got text(len 3)"));
    EXPECT_EQ(std::string(ra_refusal(builder, ra_rows({R"({"a":[1,true]})"})).what()),
              ra_message("a", 0, "element 1: expected a number for BIGINT, got a boolean"));
    EXPECT_EQ(std::string(ra_refusal(builder, ra_rows({R"({"a":[1e300]})"})).what()),
              ra_message("a", 0, "element 0: value 1e+300 out of range for BIGINT"));
}

TEST(NativeRowArrow, ElementRulesRefuseWhatTheSharedRuleWouldNull) {
    const std::vector<std::pair<std::string, std::string>> cases = {
        {R"({"c":[1.5,"x"]})", "element 1: expected a number for REAL, got text(len 1)"},
        {R"({"c":[1e300]})", "element 0: value 1e+300 out of range for REAL"},
    };
    const RowArrowBuilder real(ra_columns("c:REAL ARRAY"));
    for (const auto& [line, reason] : cases) {
        EXPECT_EQ(std::string(ra_refusal(real, ra_rows({line})).what()),
                  ra_message("c", 0, reason));
    }
    const RowArrowBuilder integer(ra_columns("c:INTEGER ARRAY"));
    EXPECT_EQ(std::string(ra_refusal(integer, ra_rows({R"({"c":[2147483648]})"})).what()),
              ra_message("c", 0, "element 0: value 2147483648 out of range for INTEGER"));
    const RowArrowBuilder boolean(ra_columns("c:BOOLEAN ARRAY"));
    EXPECT_EQ(std::string(ra_refusal(boolean, ra_rows({R"({"c":[true,1]})"})).what()),
              ra_message("c", 0, "element 1: expected true or false for BOOLEAN, got 1"));
    const RowArrowBuilder dbl(ra_columns("c:DOUBLE ARRAY"));
    EXPECT_EQ(std::string(ra_refusal(dbl, ra_rows({R"({"c":[{"k":1}]})"})).what()),
              ra_message("c", 0, "element 0: expected a number for DOUBLE, got object(size 1)"));
    const RowArrowBuilder decimal(ra_columns("c:DECIMAL(5, 2) ARRAY"));
    EXPECT_EQ(std::string(ra_refusal(decimal, ra_rows({R"({"c":[1234.5]})"})).what()),
              ra_message("c", 0, "element 0: value 1234.5 out of range for DECIMAL(5, 2)"));
    EXPECT_EQ(
        std::string(ra_refusal(decimal, ra_rows({R"({"c":["1.5"]})"})).what()),
        ra_message(
            "c", 0, "element 0: expected a decimal number for DECIMAL(5, 2), got text(len 3)"));
}

// Inside a composite every integer type refuses a number with a fraction, as
// SMALLINT does at the top level, where the shared rule truncates: 1.9 must
// never land as 1.
TEST(NativeRowArrow, FractionalIntegerElementsFailRatherThanTruncate) {
    const std::vector<std::tuple<std::string, std::string, std::string>> cases = {
        {"c:BIGINT ARRAY",
         R"({"c":[1.9,-2.7]})",
         "element 0: value 1.9 is not a whole number for "
         "BIGINT"},
        {"c:BIGINT ARRAY",
         R"({"c":[1,-2.7]})",
         "element 1: value -2.7 is not a whole number for "
         "BIGINT"},
        {"c:INTEGER ARRAY",
         R"({"c":[3.99]})",
         "element 0: value 3.99 is not a whole number for "
         "INTEGER"},
        {"c:SMALLINT ARRAY",
         R"({"c":[0.5]})",
         "element 0: value 0.5 is not a whole number for "
         "SMALLINT"},
        {"c:TINYINT ARRAY",
         R"({"c":[-0.5]})",
         "element 0: value -0.5 is not a whole number for "
         "TINYINT"},
        {"c:ROW<x BIGINT>",
         R"({"c":{"x":1.5}})",
         "field `x`: value 1.5 is not a whole number for "
         "BIGINT"},
        {"c:MAP<VARCHAR, INTEGER>",
         R"({"c":{"k":2.5}})",
         "entry 0 value: value 2.5 is not a "
         "whole number for INTEGER"},
        {"c:INTEGER ARRAY",
         R"({"c":[-2147483649.0]})",
         "element 0: value -2147483649 out of "
         "range for INTEGER"},
        // Digit text is still the wrong kind: a composite's numbers arrive as
        // numbers, from a columnar batch too.
        {"c:BIGINT ARRAY",
         R"({"c":["5"]})",
         "element 0: expected a number for BIGINT, got "
         "text(len 1)"},
    };
    for (const auto& [spec, line, reason] : cases) {
        const RowArrowBuilder builder(ra_columns(spec));
        EXPECT_EQ(std::string(ra_refusal(builder, ra_rows({line})).what()),
                  ra_message("c", 0, reason))
            << spec << " " << line;
    }
}

TEST(NativeRowArrow, WholeDoubleIntegerElementsAreKept) {
    const RowArrowBuilder builder(ra_columns("a:BIGINT ARRAY;i:INTEGER ARRAY"));
    const auto chunk = ra_build(
        builder, ra_rows({R"({"a":[7.0,-3.0,1e15],"i":[2147483647.0,-2147483648.0,-0.0]})"}));
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(ra_int64_values(*ra_col<arrow::ListArray>(chunk, 0)->values()),
              (std::vector<std::int64_t>{7, -3, 1000000000000000}));
    const auto& i =
        static_cast<const arrow::Int32Array&>(*ra_col<arrow::ListArray>(chunk, 1)->values());
    ASSERT_EQ(i.length(), 3);
    EXPECT_EQ(i.Value(0), std::numeric_limits<std::int32_t>::max());
    EXPECT_EQ(i.Value(1), std::numeric_limits<std::int32_t>::min());
    EXPECT_EQ(i.Value(2), 0);
}

TEST(NativeRowArrow, NestedElementFailureNamesThePath) {
    const RowArrowBuilder builder(ra_columns("n:SMALLINT ARRAY ARRAY"));
    EXPECT_EQ(std::string(ra_refusal(builder, ra_rows({R"({"n":[[1],[40000]]})"})).what()),
              ra_message("n", 0, "element 1, element 0: value 40000 out of range for SMALLINT"));
}

TEST(NativeRowArrow, NonArrayCellInAnArrayColumnFails) {
    const RowArrowBuilder builder(ra_columns("a:BIGINT ARRAY"));
    const std::vector<std::pair<std::string, std::string>> cases = {
        {R"({"a":5})", "expected an array for BIGINT ARRAY, got 5"},
        {R"({"a":{"k":1}})", "expected an array for BIGINT ARRAY, got object(size 1)"},
        {R"({"a":"not an array"})", "expected an array for BIGINT ARRAY, got text(len 12)"},
        {R"({"a":"{\"k\":1}"})", "expected an array for BIGINT ARRAY, got text(len 7)"},
    };
    for (const auto& [line, reason] : cases) {
        EXPECT_EQ(std::string(ra_refusal(builder, ra_rows({line})).what()),
                  ra_message("a", 0, reason))
            << line;
    }
}

// Decimals inside a composite: a JSON decode leaves them as plain numbers, so
// a double is read through its own numeral, an integer exactly, and a decimal
// string as it is; each is rescaled to the declared scale.
TEST(NativeRowArrow, DecimalElementsTakeNumbersAndDecimalStrings) {
    const RowArrowBuilder builder(ra_columns("c:DECIMAL(10, 2) ARRAY"));
    const auto chunk =
        ra_build(builder,
                 ra_value_rows({{{"c",
                                  JsonValue{clink::config::JsonArray{JsonValue{1.25},
                                                                     JsonValue{3},
                                                                     ra_decimal(-45678, 3),
                                                                     JsonValue{0.1},
                                                                     JsonValue{}}}}}}));
    ASSERT_NE(chunk, nullptr);
    const auto list = ra_col<arrow::ListArray>(chunk, 0);
    const auto& values = static_cast<const arrow::Decimal128Array&>(*list->values());
    ASSERT_EQ(values.length(), 5);
    EXPECT_EQ(arrow::Decimal128(values.GetValue(0)), arrow::Decimal128(125));
    EXPECT_EQ(arrow::Decimal128(values.GetValue(1)), arrow::Decimal128(300));
    EXPECT_EQ(arrow::Decimal128(values.GetValue(2)), arrow::Decimal128(-4568));
    EXPECT_EQ(arrow::Decimal128(values.GetValue(3)), arrow::Decimal128(10));
    EXPECT_TRUE(values.IsNull(4));
}

TEST(NativeRowArrow, MapsReadNumericKeysFromTheirKeyText) {
    const RowArrowBuilder builder(ra_columns("m:MAP<BIGINT, VARCHAR>"));
    const auto chunk = ra_build(builder,
                                ra_rows({
                                    R"({"m":{"1":"a","-5":"b","9007199254740993":"c"}})",
                                    R"({"m":{}})",
                                    R"({"m":{"7":null}})",
                                    R"({"m":null})",
                                }));
    ASSERT_NE(chunk, nullptr);
    const auto map = ra_col<arrow::MapArray>(chunk, 0);
    // Entries come in key-text order, which is how the object stores them.
    EXPECT_EQ(ra_int64_values(*map->keys()),
              (std::vector<std::int64_t>{-5, 1, 9007199254740993, 7}));
    const auto& items = static_cast<const arrow::StringArray&>(*map->items());
    EXPECT_EQ(items.GetString(0), "b");
    EXPECT_EQ(items.GetString(1), "a");
    EXPECT_EQ(items.GetString(2), "c");
    EXPECT_TRUE(items.IsNull(3));
    EXPECT_EQ(map->value_length(0), 3);
    EXPECT_EQ(map->value_length(1), 0);
    EXPECT_FALSE(map->IsNull(1));
    EXPECT_EQ(map->value_length(2), 1);
    EXPECT_TRUE(map->IsNull(3));
}

TEST(NativeRowArrow, MapKeysOfOtherTypesFollowTheirOwnRules) {
    const RowArrowBuilder builder(
        ra_columns("dm:MAP<DATE, BIGINT>;xm:MAP<DECIMAL(5, 2), BIGINT>;om:MAP<BOOLEAN, BIGINT>;"
                   "fm:MAP<DOUBLE, BIGINT>;sm:MAP<VARCHAR, DOUBLE>"));
    const auto chunk =
        ra_build(builder,
                 ra_rows({R"({"dm":{"2024-01-01":1,"0":2},"xm":{"1.5":1},"om":{"true":1},)"
                          R"("fm":{"-2.5":1},"sm":{"":1.5,"k":2}})"}));
    ASSERT_NE(chunk, nullptr);
    const auto& dates =
        static_cast<const arrow::Date32Array&>(*ra_col<arrow::MapArray>(chunk, 0)->keys());
    EXPECT_EQ(dates.Value(0), 0);
    EXPECT_EQ(dates.Value(1), 19723);
    const auto& decimals =
        static_cast<const arrow::Decimal128Array&>(*ra_col<arrow::MapArray>(chunk, 1)->keys());
    EXPECT_EQ(arrow::Decimal128(decimals.GetValue(0)), arrow::Decimal128(150));
    const auto& bools =
        static_cast<const arrow::BooleanArray&>(*ra_col<arrow::MapArray>(chunk, 2)->keys());
    EXPECT_TRUE(bools.Value(0));
    const auto& doubles =
        static_cast<const arrow::DoubleArray&>(*ra_col<arrow::MapArray>(chunk, 3)->keys());
    EXPECT_EQ(doubles.Value(0), -2.5);
    const auto& strings =
        static_cast<const arrow::StringArray&>(*ra_col<arrow::MapArray>(chunk, 4)->keys());
    EXPECT_EQ(strings.GetString(0), "");
    EXPECT_EQ(strings.GetString(1), "k");
}

// A key that fails names its position and never its text.
TEST(NativeRowArrow, BadMapKeyFailsNamingItsPositionNotItsText) {
    const RowArrowBuilder bigint(ra_columns("m:MAP<BIGINT, VARCHAR>"));
    const auto e = ra_refusal(bigint, ra_rows({R"({"m":{"1":"a","secret-key":"b"}})"}));
    EXPECT_EQ(
        std::string(e.what()),
        ra_message("m", 0, "entry 1 key: expected a number for BIGINT, got key text(len 10)"));
    EXPECT_EQ(std::string(e.what()).find("secret"), std::string::npos);

    const RowArrowBuilder smallint(ra_columns("m:MAP<SMALLINT, BIGINT>"));
    EXPECT_EQ(std::string(ra_refusal(smallint, ra_rows({R"({"m":{"40000":1}})"})).what()),
              ra_message("m", 0, "entry 0 key: key text(len 5) out of range for SMALLINT"));

    const RowArrowBuilder date(ra_columns("m:MAP<DATE, BIGINT>"));
    EXPECT_EQ(std::string(ra_refusal(date, ra_rows({R"({"m":{"2023-02-29":1}})"})).what()),
              ra_message("m",
                         0,
                         "entry 0 key: date text is not days or a valid YYYY-MM-DD date for "
                         "DATE, got key text(len 10)"));

    const RowArrowBuilder decimal(ra_columns("m:MAP<DECIMAL(5, 2), BIGINT>"));
    EXPECT_EQ(std::string(ra_refusal(decimal, ra_rows({R"({"m":{"12.5x":1}})"})).what()),
              ra_message("m",
                         0,
                         "entry 0 key: expected a decimal number for DECIMAL(5, 2), got key "
                         "text(len 5)"));
    EXPECT_EQ(std::string(ra_refusal(decimal, ra_rows({R"({"m":{"12345.5":1}})"})).what()),
              ra_message("m", 0, "entry 0 key: key text(len 7) out of range for DECIMAL(5, 2)"));
}

// Truncating would turn the distinct keys "1", "1.5" and "1.9" into the key
// 1 three times.
TEST(NativeRowArrow, FractionalIntegerMapKeysFailNamingThePositionNotTheText) {
    for (const std::string type : {"BIGINT", "INTEGER", "SMALLINT", "TINYINT"}) {
        const RowArrowBuilder builder(ra_columns("m:MAP<" + type + ", VARCHAR>"));
        const auto e = ra_refusal(builder, ra_rows({R"({"m":{"1":"a","1.5":"b","1.9":"c"}})"}));
        EXPECT_EQ(e.column(), "m");
        EXPECT_EQ(e.row(), 0);
        EXPECT_EQ(
            std::string(e.what()),
            ra_message("m", 0, "entry 1 key: key text(len 3) is not a whole number for " + type));
        EXPECT_EQ(std::string(e.what()).find("1.5"), std::string::npos);
    }
}

// The object holds each key text once, but two texts can convert to the
// same key, which ClickHouse would store twice. Entries are in key-text
// order, so entry 1 is the later text.
TEST(NativeRowArrow, MapKeysThatConvertToTheSameValueFail) {
    const auto reason = [](std::size_t length, const std::string& key, const std::string& map) {
        return "entry 1 key: key text(len " + std::to_string(length) + ") is the same " + key +
               " as entry 0 key, and each key of " + map + " must be distinct";
    };
    const std::vector<std::tuple<std::string, std::string, std::string>> cases = {
        {"MAP<DATE, BIGINT>",
         R"({"m":{"19723":1,"2024-01-01":2}})",
         reason(10, "DATE", "MAP<DATE, BIGINT>")},
        {"MAP<BIGINT, VARCHAR>",
         R"({"m":{"1":"a","1.0":"b"}})",
         reason(3, "BIGINT", "MAP<BIGINT, VARCHAR>")},
        {"MAP<SMALLINT, VARCHAR>",
         R"({"m":{"-1":"a","-1e0":"b"}})",
         reason(4, "SMALLINT", "MAP<SMALLINT, VARCHAR>")},
        {"MAP<TIMESTAMP(3), BIGINT>",
         R"({"m":{"01":1,"1":2}})",
         reason(1, "TIMESTAMP(3)", "MAP<TIMESTAMP(3), BIGINT>")},
        {"MAP<DECIMAL(5, 2), BIGINT>",
         R"({"m":{"1.505":1,"1.51":2}})",
         reason(4, "DECIMAL(5, 2)", "MAP<DECIMAL(5, 2), BIGINT>")},
        {"MAP<DOUBLE, BIGINT>",
         R"({"m":{"-0.0":1,"0":2}})",
         reason(1, "DOUBLE", "MAP<DOUBLE, BIGINT>")},
        {"MAP<BIGINT ARRAY, VARCHAR>",
         R"({"m":{"[1, 2]":"a","[1,2.0]":"b"}})",
         reason(7, "BIGINT ARRAY", "MAP<BIGINT ARRAY, VARCHAR>")},
    };
    for (const auto& [type, line, expected] : cases) {
        const RowArrowBuilder builder(ra_columns("m:" + type));
        EXPECT_EQ(std::string(ra_refusal(builder, ra_rows({line})).what()),
                  ra_message("m", 0, expected))
            << type << " " << line;
    }

    // A repeat further in names the entry it repeats and the path to the map.
    // Key texts sort as text, so "1970-01-02" comes before "19723".
    const RowArrowBuilder nested(ra_columns("w:ROW<m MAP<DATE, BIGINT>>"));
    EXPECT_EQ(
        std::string(
            ra_refusal(nested,
                       ra_rows({R"({"w":{"m":{"0":1,"19723":2,"1970-01-02":3,"2024-01-01":4}}})"}))
                .what()),
        ra_message("w",
                   0,
                   "field `m`, entry 3 key: key text(len 10) is the same DATE as entry 2 key, and "
                   "each key of MAP<DATE, BIGINT> must be distinct"));
}

// Keys are compared by the values they convert to, recorded so that no two
// values share a record. The array keys below would read the same if the
// record dropped an array length or a null marker: 72340172838076673 is
// eight 0x01 bytes, which is what a run of present markers looks like.
TEST(NativeRowArrow, DistinctMapKeysAreKeptWhateverTheirType) {
    const RowArrowBuilder builder(
        ra_columns("dm:MAP<DATE, BIGINT>;fm:MAP<DOUBLE, BIGINT>;am:MAP<BIGINT ARRAY, BIGINT>;"
                   "vm:MAP<VARCHAR ARRAY, BIGINT>;bm:MAP<BIGINT, BIGINT>;"
                   "rm:MAP<ROW<a BIGINT ARRAY, b BIGINT ARRAY>, BIGINT>"));
    const auto chunk =
        ra_build(builder,
                 ra_rows({R"({"dm":{"19723":1,"2024-01-02":2},"fm":{"1":1,"1.5":2,"-0.5":3},)"
                          R"("am":{"[1,2]":1,"[12]":2,"[1]":3,"[]":4,"[null]":5,)"
                          R"("[0,null]":6,"[null,0]":7},)"
                          R"("vm":{"[\"ab\",\"c\"]":1,"[\"a\",\"bc\"]":2,)"
                          R"("[null,\"x\"]":3,"[\"x\",null]":4},)"
                          R"("bm":{"1":1,"-1":2,"10":3},)"
                          R"("rm":{"{\"a\":[72340172838076673],\"b\":[]}":1,)"
                          R"("{\"a\":[],\"b\":[72340172838076673]}":2}})"}));
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(ra_col<arrow::MapArray>(chunk, 0)->value_length(0), 2);
    EXPECT_EQ(ra_col<arrow::MapArray>(chunk, 1)->value_length(0), 3);
    EXPECT_EQ(ra_col<arrow::MapArray>(chunk, 2)->value_length(0), 7);
    EXPECT_EQ(ra_col<arrow::MapArray>(chunk, 3)->value_length(0), 4);
    EXPECT_EQ(ra_int64_values(*ra_col<arrow::MapArray>(chunk, 4)->keys()),
              (std::vector<std::int64_t>{-1, 1, 10}));
    EXPECT_EQ(ra_col<arrow::MapArray>(chunk, 5)->value_length(0), 2);
}

TEST(NativeRowArrow, BadMapValueOrNonObjectFails) {
    const RowArrowBuilder builder(ra_columns("m:MAP<VARCHAR, DOUBLE>"));
    EXPECT_EQ(std::string(ra_refusal(builder, ra_rows({R"({"m":{"a":1,"b":"abc"}})"})).what()),
              ra_message("m", 0, "entry 1 value: expected a number for DOUBLE, got text(len 3)"));
    EXPECT_EQ(std::string(ra_refusal(builder, ra_rows({R"({"m":[1,2]})"})).what()),
              ra_message("m", 0, "expected an object for MAP<VARCHAR, DOUBLE>, got array(size 2)"));
}

TEST(NativeRowArrow, RowsReadFieldsByNameWithMissingFieldsNull) {
    const RowArrowBuilder builder(ra_columns("w:ROW<x BIGINT, y VARCHAR, z TIMESTAMP(3)>"));
    const auto chunk = ra_build(builder,
                                ra_rows({
                                    R"({"w":{"x":1,"y":"a","z":1700000000123}})",
                                    R"({"w":{"x":2,"extra":true}})",
                                    R"({"w":{}})",
                                    R"({"w":null})",
                                }));
    ASSERT_NE(chunk, nullptr);
    const auto w = ra_col<arrow::StructArray>(chunk, 0);
    const auto& x = static_cast<const arrow::Int64Array&>(*w->field(0));
    const auto& y = static_cast<const arrow::StringArray&>(*w->field(1));
    const auto& z = static_cast<const arrow::TimestampArray&>(*w->field(2));
    EXPECT_EQ(x.Value(0), 1);
    EXPECT_EQ(y.GetString(0), "a");
    EXPECT_EQ(z.Value(0), 1700000000123);
    EXPECT_EQ(x.Value(1), 2);
    EXPECT_TRUE(y.IsNull(1));
    EXPECT_TRUE(z.IsNull(1));
    EXPECT_FALSE(w->IsNull(2));
    EXPECT_TRUE(x.IsNull(2));
    EXPECT_TRUE(w->IsNull(3));
}

TEST(NativeRowArrow, NonObjectRowCellOrBadFieldFails) {
    const RowArrowBuilder builder(ra_columns("w:ROW<a BIGINT, b VARCHAR>"));
    const std::string type = "ROW<a BIGINT, b VARCHAR>";
    const std::vector<std::pair<std::string, std::string>> cases = {
        {R"({"w":5})", "expected an object for " + type + ", got 5"},
        {R"({"w":[1,2]})", "expected an object for " + type + ", got array(size 2)"},
        {R"({"w":"private words"})", "expected an object for " + type + ", got text(len 13)"},
        {R"({"w":"[1,2]"})", "expected an object for " + type + ", got text(len 5)"},
        {R"({"w":true})", "expected an object for " + type + ", got a boolean"},
        {R"({"w":{"a":"x"}})", "field `a`: expected a number for BIGINT, got text(len 1)"},
    };
    for (const auto& [line, reason] : cases) {
        const auto e = ra_refusal(builder, ra_rows({line}));
        EXPECT_EQ(std::string(e.what()), ra_message("w", 0, reason)) << line;
        EXPECT_EQ(std::string(e.what()).find("private"), std::string::npos);
    }
}

TEST(NativeRowArrow, CompositesNestThroughEachOther) {
    const RowArrowBuilder builder(
        ra_columns("c:ROW<tags VARCHAR ARRAY, attrs MAP<VARCHAR, ROW<n SMALLINT>>>"));
    const auto chunk =
        ra_build(builder, ra_rows({R"({"c":{"tags":["a","b"],"attrs":{"k":{"n":3},"l":null}}})"}));
    ASSERT_NE(chunk, nullptr);
    const auto c = ra_col<arrow::StructArray>(chunk, 0);
    const auto& tags = static_cast<const arrow::ListArray&>(*c->field(0));
    EXPECT_EQ(tags.value_length(0), 2);
    const auto& attrs = static_cast<const arrow::MapArray&>(*c->field(1));
    const auto& items = static_cast<const arrow::StructArray&>(*attrs.items());
    EXPECT_EQ(static_cast<const arrow::Int16Array&>(*items.field(0)).Value(0), 3);
    EXPECT_TRUE(items.IsNull(1));

    const auto e = ra_refusal(builder, ra_rows({R"({"c":{"attrs":{"k":{"n":3},"l":{"n":"x"}}}})"}));
    EXPECT_EQ(std::string(e.what()),
              ra_message("c",
                         0,
                         "field `attrs`, entry 1 value, field `n`: expected a whole number for "
                         "SMALLINT, got text(len 1)"));
}

// A columnar batch, which arrives after a columnar operator or a shuffle, is
// read through its rows. Its sidecar carries every type outside the shared set
// as text, so its rows hold digits and JSON text where a row batch holds
// numbers, arrays and objects; both give the same chunk.
TEST(NativeRowArrow, ColumnarBatchMaterialisesToTheSameChunk) {
    const auto columns = ra_columns(
        "b:BIGINT;s:SMALLINT;t:TINYINT;ts:TIMESTAMP(3);d:DATE;dn:DATE;"
        "a:BIGINT ARRAY;m:MAP<VARCHAR, BIGINT>;w:ROW<x BIGINT, y VARCHAR>;v:VARCHAR");
    const RowArrowBuilder builder(columns);
    const auto rows = ra_rows({
        R"({"b":9007199254740993,"s":-123,"t":7,"ts":-1700000000123,"d":"1969-07-20",)"
        R"("dn":-165,"a":[1,null,3],"m":{"k":1},"w":{"x":5,"y":"q"},"v":"text"})",
        R"({"b":null,"s":null,"ts":0,"dn":0,"a":[],"m":{},"w":{}})",
    });

    std::vector<sql::RowColumn> declared;
    for (const auto& c : columns) {
        declared.push_back(sql::RowColumn{c.name, arrow_type_for(c.type)});
    }
    const auto batcher = sql::make_row_columnar_arrow_batcher(declared);
    const auto sidecar = batcher.build(rows);
    ASSERT_NE(sidecar, nullptr);
    const auto columnar = batcher.parse(*sidecar);
    ASSERT_TRUE(columnar.has_value());
    ASSERT_TRUE(columnar->is_columnar());
    // The premise: the materialised rows carry these types as text.
    const auto& first = columnar->records().front().value();
    EXPECT_TRUE(first.values.find("ts")->second.is_string());
    EXPECT_TRUE(first.values.find("s")->second.is_string());
    EXPECT_TRUE(first.values.find("dn")->second.is_string());
    EXPECT_TRUE(first.values.find("a")->second.is_string());

    const auto from_rows = ra_build(builder, rows);
    const auto from_columnar = ra_build(builder, *columnar);
    ASSERT_NE(from_rows, nullptr);
    ASSERT_NE(from_columnar, nullptr);
    EXPECT_TRUE(from_rows->Equals(*from_columnar)) << from_rows->ToString() << "\nvs\n"
                                                   << from_columnar->ToString();
    EXPECT_EQ(ra_col<arrow::TimestampArray>(from_columnar, 3)->Value(0), -1700000000123);
    EXPECT_EQ(ra_col<arrow::Date32Array>(from_columnar, 4)->Value(0), -165);
}

TEST(NativeRowArrow, ColumnarBatchThatMaterialisesTooFewRowsIsRefused) {
    const RowArrowBuilder builder(ra_columns("b:BIGINT"));
    arrow::Int64Builder ints;
    ASSERT_TRUE(ints.AppendValues({1, 2, 3}).ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(ints.Finish(&array).ok());
    auto sidecar =
        arrow::RecordBatch::Make(arrow::schema({arrow::field("b", arrow::int64())}), 3, {array});
    const Batch<sql::Row> batch(std::move(sidecar), 3, [](const arrow::RecordBatch&) {
        return std::vector<Record<sql::Row>>{};
    });
    try {
        (void)builder.build(batch);
        ADD_FAILURE() << "a short materialisation was accepted";
    } catch (const std::logic_error& e) {
        EXPECT_EQ(std::string(e.what()),
                  "clickhouse native sink: a columnar batch of 3 rows materialised 0");
    }
}

TEST(NativeRowArrow, UndeclaredRowValuesAreIgnored) {
    const RowArrowBuilder builder(ra_columns("b:BIGINT"));
    const auto chunk = ra_build(builder, ra_rows({R"({"b":1,"other":"x","__row_kind":"+I"})"}));
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->num_columns(), 1);
    EXPECT_EQ(ra_col<arrow::Int64Array>(chunk, 0)->Value(0), 1);
}

TEST(NativeRowArrow, MovedBuilderKeepsItsSchemaAndBuilds) {
    RowArrowBuilder original(ra_columns("s:SMALLINT"));
    const RowArrowBuilder moved(std::move(original));
    EXPECT_EQ(moved.schema()->num_fields(), 1);
    const auto chunk = ra_build(moved, ra_rows({R"({"s":5})"}));
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(ra_col<arrow::Int16Array>(chunk, 0)->Value(0), 5);
}

// --- The columnar intake --------------------------------------------------------

// A sidecar in the layout columnar Row producers emit: an event-time column,
// then one value column per (name, array).
std::shared_ptr<arrow::RecordBatch> ra_sidecar(
    const std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>>& columns,
    std::shared_ptr<arrow::DataType> time_type = arrow::int64()) {
    const std::int64_t rows = columns.empty() ? 0 : columns.front().second->length();
    std::unique_ptr<arrow::ArrayBuilder> times = arrow::MakeBuilder(time_type).ValueOrDie();
    for (std::int64_t i = 0; i < rows; ++i) {
        EXPECT_TRUE(times->AppendNull().ok());
    }
    arrow::FieldVector fields = {arrow::field("event_time", time_type)};
    std::vector<std::shared_ptr<arrow::Array>> arrays = {times->Finish().ValueOrDie()};
    for (const auto& [name, array] : columns) {
        fields.push_back(arrow::field(name, array->type()));
        arrays.push_back(array);
    }
    return arrow::RecordBatch::Make(arrow::schema(fields), rows, std::move(arrays));
}

std::shared_ptr<arrow::Array> ra_json(const std::shared_ptr<arrow::DataType>& type,
                                      const std::string& json) {
    auto made = arrow::json::ArrayFromJSONString(type, json);
    EXPECT_TRUE(made.ok()) << type->ToString() << " " << json << ": " << made.status().ToString();
    return made.ValueOrDie();
}

// What a build gave: a chunk, or the full text of the ConversionError.
struct RaOutcome {
    std::shared_ptr<arrow::RecordBatch> chunk;
    std::optional<std::string> error;
};

template <typename F>
RaOutcome ra_outcome(F&& build) {
    RaOutcome out;
    try {
        out.chunk = build();
    } catch (const ConversionError& e) {
        out.error = e.what();
    }
    return out;
}

// The intake on `sidecar` against build() over the rows the same sidecar
// materialises to through the self-describing reader: the same chunk, or the
// same ConversionError, and no Row built by the intake. Checked on the
// sidecar as given and on a slice of it at a non-zero offset.
void ra_expect_intake_matches(const std::string& spec,
                              const std::shared_ptr<arrow::RecordBatch>& full) {
    const auto columns = ra_columns(spec);
    const RowArrowBuilder builder(columns);
    for (const std::int64_t offset : {std::int64_t{0}, std::int64_t{1}}) {
        if (offset > full->num_rows()) {
            continue;
        }
        SCOPED_TRACE(spec + ", offset " + std::to_string(offset));
        const auto sidecar = full->Slice(offset);
        const auto compiled = compile_intake(*sidecar->schema(), columns);
        ASSERT_TRUE(std::holds_alternative<IntakePlan>(compiled))
            << to_string(std::get<IntakeDecline>(compiled));
        const auto& plan = std::get<IntakePlan>(compiled);

        const auto before = clink::detail::batch_materialize_counter().load();
        const RaOutcome columnar =
            ra_outcome([&] { return builder.build_columnar(*sidecar, plan); });
        EXPECT_EQ(clink::detail::batch_materialize_counter().load(), before)
            << "the intake built rows";
        const Batch<sql::Row> rows{
            sidecar, static_cast<std::size_t>(sidecar->num_rows()), sql::row_materialize_fn()};
        const RaOutcome row = ra_outcome([&] { return builder.build(rows); });

        EXPECT_EQ(columnar.error, row.error);
        if (columnar.chunk && row.chunk) {
            const auto status = columnar.chunk->ValidateFull();
            EXPECT_TRUE(status.ok()) << status.ToString();
            EXPECT_TRUE(columnar.chunk->schema()->Equals(*builder.schema()));
            EXPECT_TRUE(columnar.chunk->Equals(*row.chunk, arrow::EqualOptions().nans_equal(true)))
                << columnar.chunk->ToString() << "\nvs\n"
                << row.chunk->ToString();
        }
    }
}

// The shared types from every sidecar type the reader carries, including the
// ones the shared rule nulls: a value of the wrong kind, an int64 out of the
// INTEGER range, a double past a float, a decimal past its precision.
TEST(NativeIntake, SharedTypesMatchTheRowPathFromEverySidecarType) {
    const auto int64s = ra_json(arrow::int64(),
                                "[0, null, -1, 9223372036854775807, -9223372036854775808, "
                                "2147483648, -2147483649, 9007199254740993]");
    const auto int32s = ra_json(arrow::int32(), "[0, null, -1, 2147483647, -2147483648, 7, 8, 9]");
    arrow::DoubleBuilder doubles_builder;
    ASSERT_TRUE(doubles_builder
                    .AppendValues({0.5,
                                   -0.0,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   std::numeric_limits<double>::infinity(),
                                   1e300,
                                   -2.5e9,
                                   3.4e38,
                                   9.3e18})
                    .ok());
    ASSERT_TRUE(doubles_builder.AppendNull().ok());
    std::shared_ptr<arrow::Array> doubles;
    ASSERT_TRUE(doubles_builder.Finish(&doubles).ok());
    arrow::FloatBuilder floats_builder;
    ASSERT_TRUE(floats_builder
                    .AppendValues({0.1F,
                                   -0.0F,
                                   std::numeric_limits<float>::quiet_NaN(),
                                   -std::numeric_limits<float>::infinity(),
                                   3.0F,
                                   1e10F,
                                   2.5F,
                                   -7.0F})
                    .ok());
    ASSERT_TRUE(floats_builder.AppendNull().ok());
    std::shared_ptr<arrow::Array> floats;
    ASSERT_TRUE(floats_builder.Finish(&floats).ok());
    const auto bools =
        ra_json(arrow::boolean(), "[true, false, null, true, false, true, false, true]");
    const auto texts = ra_json(
        arrow::utf8(), R"(["12", null, "", "abc", "\u0001xy", "\u000112.50", "1e3", "-0"])");
    const auto decimals = ra_json(
        arrow::decimal128(12, 4),
        R"(["12.3400", null, "-0.0001", "99999999.9999", "12345678.9999", "0.0050", "1.0000", "-5.5000"])");
    const auto narrow =
        ra_json(arrow::decimal128(10, 2),
                R"(["12.34", null, "-0.01", "99999999.99", "0.00", "1.00", "2.50", "-3.00"])");

    for (const std::string declared : {"BIGINT",
                                       "INTEGER",
                                       "REAL",
                                       "DOUBLE",
                                       "BOOLEAN",
                                       "VARCHAR",
                                       "DECIMAL(10, 2)",
                                       "DECIMAL(4, 1)"}) {
        for (const auto& array :
             {int64s, int32s, doubles, floats, bools, texts, decimals, narrow}) {
            SCOPED_TRACE(declared + " from " + array->type()->ToString());
            ra_expect_intake_matches(
                "c:" + declared,
                ra_sidecar({{"c", array->Slice(0, std::min<std::int64_t>(array->length(), 9))}}));
        }
    }
}

// A decimal128 array holds whatever was stored in it: a value past the
// array's own precision, as the decoder stores one unchecked, is nulled by
// both paths alike.
TEST(NativeIntake, ADecimalPastItsArraysPrecisionMatchesTheRowPath) {
    arrow::Decimal128Builder b(arrow::decimal128(5, 2));
    ASSERT_TRUE(b.Append(arrow::Decimal128(12345)).ok());
    ASSERT_TRUE(b.Append(arrow::Decimal128(123456789)).ok());
    ASSERT_TRUE(b.AppendNull().ok());
    ASSERT_TRUE(b.Append(arrow::Decimal128(-99999)).ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(b.Finish(&array).ok());
    for (const std::string declared :
         {"DECIMAL(5, 2)", "DECIMAL(12, 2)", "DECIMAL(10, 4)", "VARCHAR", "DOUBLE"}) {
        ra_expect_intake_matches("d:" + declared, ra_sidecar({{"d", array}}));
    }
}

// The sink-owned types read the text forms the sidecar stores them in, and the
// integer forms a computed column holds; a cell neither form fits fails with
// the same ConversionError on both paths.
TEST(NativeIntake, SinkOwnedTypesMatchTheRowPathIncludingTheirRefusals) {
    struct Case {
        std::string declared;
        std::shared_ptr<arrow::Array> array;
    };
    const std::vector<Case> cases = {
        {"TIMESTAMP(3)",
         ra_json(arrow::utf8(), R"(["1700000000123", null, "-1", "-2208988800000", "0"])")},
        {"TIMESTAMP(9)",
         ra_json(arrow::int64(), "[1700000000123, null, -1, -2208988800000, 9223372036854]")},
        {"TIMESTAMP(3) WITH TIME ZONE", ra_json(arrow::int64(), "[0, null, -86400000]")},
        {"TIMESTAMP(6)", ra_json(arrow::utf8(), R"(["1", "2024-01-01T00:00:00Z"])")},
        {"TIMESTAMP(0)", ra_json(arrow::int32(), "[0, -1, 2147483647]")},
        {"TIMESTAMP(3)", ra_json(arrow::utf8(), R"(["99999999999999999999"])")},
        {"TIMESTAMP(3)", ra_json(arrow::float64(), "[1.5]")},
        {"DATE", ra_json(arrow::utf8(), R"(["19675", "1969-07-20", null, "-1", "2299-12-31"])")},
        {"DATE", ra_json(arrow::int32(), "[19675, null, -165, 0]")},
        {"DATE", ra_json(arrow::int64(), "[19675, -165]")},
        {"DATE", ra_json(arrow::utf8(), R"(["2024-02-30"])")},
        {"DATE", ra_json(arrow::utf8(), R"(["24-1-1"])")},
        {"DATE", ra_json(arrow::int64(), "[9223372036854775807]")},
        {"SMALLINT", ra_json(arrow::utf8(), R"(["-123", null, "32767", "0"])")},
        {"SMALLINT", ra_json(arrow::utf8(), R"(["12a"])")},
        {"SMALLINT", ra_json(arrow::utf8(), R"(["40000"])")},
        {"SMALLINT", ra_json(arrow::int64(), "[1, -32768, 32768]")},
        {"SMALLINT", ra_json(arrow::int32(), "[5, null, -5]")},
        {"TINYINT", ra_json(arrow::utf8(), R"(["7", "-128", null])")},
        {"TINYINT", ra_json(arrow::int32(), "[127, 128]")},
        {"BIGINT ARRAY",
         ra_json(arrow::utf8(), R"(["[1,null,3]", "[]", null, "[9007199254740993]"])")},
        {"BIGINT ARRAY", ra_json(arrow::utf8(), R"(["[1,\"x\"]"])")},
        {"BIGINT ARRAY", ra_json(arrow::utf8(), R"(["not json"])")},
        {"REAL ARRAY", ra_json(arrow::list(arrow::float32()), "[[1.5, null, -0.0], [], null]")},
        {"BIGINT ARRAY", ra_json(arrow::list(arrow::float32()), "[[1, 2], [1.5]]")},
        {"MAP<VARCHAR, BIGINT>", ra_json(arrow::utf8(), R"(["{\"k\":1,\"j\":null}", "{}", null])")},
        {"MAP<BIGINT, VARCHAR>", ra_json(arrow::utf8(), R"(["{\"1\":\"a\",\"01\":\"b\"}"])")},
        {"ROW<x BIGINT, y VARCHAR>",
         ra_json(arrow::utf8(), R"(["{\"x\":5,\"y\":\"q\"}", "{}", null, "{\"x\":\"no\"}"])")},
        {"ROW<x BIGINT, y VARCHAR>", ra_json(arrow::int64(), "[1]")},
    };
    for (const auto& c : cases) {
        SCOPED_TRACE(c.declared + " from " + c.array->ToString());
        ra_expect_intake_matches("c:" + c.declared, ra_sidecar({{"c", c.array}}));
    }
}

// Random columns of every carried type into every declared type, nulls
// included, several columns to a batch so a failure's column order matters.
TEST(NativeIntake, RandomSidecarsMatchTheRowPath) {
    std::mt19937_64 rng(20261004);
    const auto pick = [&rng](std::uint64_t n) { return rng() % n; };
    const auto random_array =
        [&](const std::shared_ptr<arrow::DataType>& type) -> std::shared_ptr<arrow::Array> {
        std::unique_ptr<arrow::ArrayBuilder> b = arrow::MakeBuilder(type).ValueOrDie();
        for (int i = 0; i < 40; ++i) {
            if (pick(8) == 0) {
                EXPECT_TRUE(b->AppendNull().ok());
                continue;
            }
            const std::int64_t small = static_cast<std::int64_t>(pick(200)) - 100;
            switch (type->id()) {
                case arrow::Type::INT64:
                    EXPECT_TRUE(static_cast<arrow::Int64Builder&>(*b)
                                    .Append(pick(4) == 0 ? static_cast<std::int64_t>(rng()) : small)
                                    .ok());
                    break;
                case arrow::Type::INT32:
                    EXPECT_TRUE(static_cast<arrow::Int32Builder&>(*b)
                                    .Append(static_cast<std::int32_t>(pick(4) == 0 ? rng() : small))
                                    .ok());
                    break;
                case arrow::Type::DOUBLE:
                    EXPECT_TRUE(static_cast<arrow::DoubleBuilder&>(*b)
                                    .Append(static_cast<double>(small) / 4.0)
                                    .ok());
                    break;
                case arrow::Type::FLOAT:
                    EXPECT_TRUE(static_cast<arrow::FloatBuilder&>(*b)
                                    .Append(static_cast<float>(small) / 8.0F)
                                    .ok());
                    break;
                case arrow::Type::BOOL:
                    EXPECT_TRUE(static_cast<arrow::BooleanBuilder&>(*b).Append(pick(2) == 0).ok());
                    break;
                case arrow::Type::DECIMAL128:
                    EXPECT_TRUE(static_cast<arrow::Decimal128Builder&>(*b)
                                    .Append(arrow::Decimal128(small * 1000 + 7))
                                    .ok());
                    break;
                default: {
                    const std::vector<std::string> texts = {std::to_string(small),
                                                            "1969-07-20",
                                                            "[1,2]",
                                                            "{\"x\":1}",
                                                            "\x01"
                                                            "5.5",
                                                            "text"};
                    EXPECT_TRUE(static_cast<arrow::StringBuilder&>(*b)
                                    .Append(texts[pick(pick(3) == 0 ? texts.size() : 1)])
                                    .ok());
                    break;
                }
            }
        }
        return b->Finish().ValueOrDie();
    };
    const std::vector<std::shared_ptr<arrow::DataType>> types = {arrow::int64(),
                                                                 arrow::int32(),
                                                                 arrow::float64(),
                                                                 arrow::float32(),
                                                                 arrow::boolean(),
                                                                 arrow::decimal128(9, 3),
                                                                 arrow::utf8()};
    const std::vector<std::string> declared = {"BIGINT",
                                               "INTEGER",
                                               "REAL",
                                               "DOUBLE",
                                               "BOOLEAN",
                                               "VARCHAR",
                                               "DECIMAL(9, 3)",
                                               "DECIMAL(6, 1)",
                                               "SMALLINT",
                                               "DATE",
                                               "TIMESTAMP(3)",
                                               "BIGINT ARRAY",
                                               "ROW<x BIGINT>"};
    for (int round = 0; round < 60; ++round) {
        std::string spec;
        std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>> columns;
        for (int c = 0; c < 3; ++c) {
            const std::string name = "c" + std::to_string(c);
            spec += (spec.empty() ? "" : ";") + name + ":" + declared[pick(declared.size())];
            columns.emplace_back(name, random_array(types[pick(types.size())]));
        }
        SCOPED_TRACE("round " + std::to_string(round));
        ra_expect_intake_matches(spec, ra_sidecar(columns));
    }
}

// Names resolve as the self-describing reader resolves them: value columns
// from index 1, so a value column may share the event-time column's name;
// __source_partition is dropped; __key and __row_kind are ordinary names; a
// declared column the sidecar lacks is NULL; and an undeclared name may
// repeat.
TEST(NativeIntake, NamesResolveAsTheReaderResolvesThem) {
    const auto ints = ra_json(arrow::int64(), "[1, 2]");
    const auto texts = ra_json(arrow::utf8(), R"(["a", "b"])");
    const auto parts = ra_json(arrow::int32(), "[0, 1]");
    const auto sidecar = ra_sidecar({{"__source_partition", parts},
                                     {"event_time", ints},
                                     {"__row_kind", texts},
                                     {"__key", texts},
                                     {"extra", ints},
                                     {"extra", texts}});
    const auto columns = ra_columns(
        "event_time:BIGINT;__row_kind:VARCHAR;__key:VARCHAR;missing:BIGINT;__source_partition:"
        "INTEGER");
    const auto compiled = compile_intake(*sidecar->schema(), columns);
    ASSERT_TRUE(std::holds_alternative<IntakePlan>(compiled));
    EXPECT_EQ(std::get<IntakePlan>(compiled).source, (std::vector<int>{2, 3, 4, -1, -1}));
    ra_expect_intake_matches(
        "event_time:BIGINT;__row_kind:VARCHAR;__key:VARCHAR;missing:BIGINT;__source_partition:"
        "INTEGER",
        sidecar);
}

// A sidecar the reader would not read as the intake does is declined, with
// its reason, before anything is built.
TEST(NativeIntake, ASidecarTheReaderCannotReadIsDeclinedWithItsReason) {
    const auto columns = ra_columns("a:BIGINT;b:DATE");
    const auto ints = ra_json(arrow::int64(), "[1]");
    const auto check = [&columns](const std::shared_ptr<arrow::RecordBatch>& sidecar,
                                  IntakeDecline want) {
        const auto compiled = compile_intake(*sidecar->schema(), columns);
        ASSERT_TRUE(std::holds_alternative<IntakeDecline>(compiled)) << sidecar->ToString();
        EXPECT_EQ(std::get<IntakeDecline>(compiled), want);
    };
    check(ra_sidecar({{"a", ints}}, arrow::int32()), IntakeDecline::EventTime);
    check(arrow::RecordBatch::Make(
              arrow::schema(arrow::FieldVector{}), 1, std::vector<std::shared_ptr<arrow::Array>>{}),
          IntakeDecline::EventTime);
    check(ra_sidecar({{"a", ints}, {"b", ra_json(arrow::date32(), "[1]")}}),
          IntakeDecline::UnsupportedType);
    // Even a column nobody declared: the reader refuses the whole batch.
    check(ra_sidecar({{"a", ints}, {"z", ra_json(arrow::list(arrow::int64()), "[[1]]")}}),
          IntakeDecline::UnsupportedType);
    check(ra_sidecar({{"a", ints}, {"a", ra_json(arrow::utf8(), R"(["x"])")}}),
          IntakeDecline::DuplicateName);
    EXPECT_STREQ(to_string(IntakeDecline::EventTime), "event_time");
    EXPECT_STREQ(to_string(IntakeDecline::UnsupportedType), "unsupported_type");
    EXPECT_STREQ(to_string(IntakeDecline::DuplicateName), "duplicate_name");
}

TEST(NativeIntake, AnEmptySidecarGivesAnEmptyChunkWithTheSchema) {
    const auto columns = ra_columns("b:BIGINT;ts:TIMESTAMP(3);a:SMALLINT ARRAY");
    const RowArrowBuilder builder(columns);
    const auto sidecar = ra_sidecar({{"b", ra_json(arrow::int64(), "[]")}});
    const auto compiled = compile_intake(*sidecar->schema(), columns);
    ASSERT_TRUE(std::holds_alternative<IntakePlan>(compiled));
    const auto chunk = builder.build_columnar(*sidecar, std::get<IntakePlan>(compiled));
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->num_rows(), 0);
    EXPECT_TRUE(chunk->schema()->Equals(*builder.schema()));
}

// --- Reuse ----------------------------------------------------------------------

// The intake on `sidecar` as compiled, against the same plan with every fast
// path off, which is the cell-by-cell path: the same chunk, or the same
// ConversionError. Returns the reused-column count of the compiled build.
std::size_t ra_expect_reuse_matches(const std::string& spec,
                                    const std::shared_ptr<arrow::RecordBatch>& sidecar) {
    const auto columns = ra_columns(spec);
    const RowArrowBuilder builder(columns);
    const auto compiled = compile_intake(*sidecar->schema(), columns);
    if (!std::holds_alternative<IntakePlan>(compiled)) {
        ADD_FAILURE() << to_string(std::get<IntakeDecline>(compiled));
        return 0;
    }
    const IntakePlan& plan = std::get<IntakePlan>(compiled);
    IntakePlan cell_by_cell = plan;
    cell_by_cell.reuse.clear();
    std::size_t reused = 0;
    std::size_t none = 1;
    const RaOutcome fast =
        ra_outcome([&] { return builder.build_columnar(*sidecar, plan, &reused); });
    const RaOutcome slow =
        ra_outcome([&] { return builder.build_columnar(*sidecar, cell_by_cell, &none); });
    EXPECT_EQ(fast.error, slow.error);
    if (fast.chunk && slow.chunk) {
        EXPECT_EQ(none, 0U);
        const auto status = fast.chunk->ValidateFull();
        EXPECT_TRUE(status.ok()) << status.ToString();
        EXPECT_TRUE(fast.chunk->schema()->Equals(*builder.schema()));
        EXPECT_TRUE(fast.chunk->Equals(*slow.chunk, arrow::EqualOptions().nans_equal(true)))
            << fast.chunk->ToString() << "\nvs\n"
            << slow.chunk->ToString();
    }
    return reused;
}

// One random array of `type` built by an Arrow builder, as the decoder and
// the born-columnar producers build theirs: edge values, ordinary ones and
// nulls, or no nulls at all, so no validity bitmap.
std::shared_ptr<arrow::Array> ra_random_fixed(const std::shared_ptr<arrow::DataType>& type,
                                              std::mt19937_64& rng,
                                              int length,
                                              bool nulls) {
    std::unique_ptr<arrow::ArrayBuilder> b = arrow::MakeBuilder(type).ValueOrDie();
    const auto pick = [&rng](std::uint64_t n) { return rng() % n; };
    for (int i = 0; i < length; ++i) {
        if (nulls && pick(5) == 0) {
            EXPECT_TRUE(b->AppendNull().ok());
            continue;
        }
        const bool edge = pick(3) == 0;
        switch (type->id()) {
            case arrow::Type::INT64: {
                using L = std::numeric_limits<std::int64_t>;
                const std::int64_t edges[] = {L::min(), L::max(), 0, -1, 1, -2208988800000};
                EXPECT_TRUE(static_cast<arrow::Int64Builder&>(*b)
                                .Append(edge ? edges[pick(6)] : static_cast<std::int64_t>(rng()))
                                .ok());
                break;
            }
            case arrow::Type::INT32: {
                using L = std::numeric_limits<std::int32_t>;
                const std::int32_t edges[] = {L::min(), L::max(), 0, -1, 19723, -719162};
                EXPECT_TRUE(static_cast<arrow::Int32Builder&>(*b)
                                .Append(edge ? edges[pick(6)] : static_cast<std::int32_t>(rng()))
                                .ok());
                break;
            }
            case arrow::Type::FLOAT: {
                using L = std::numeric_limits<float>;
                const float edges[] = {L::quiet_NaN(),
                                       -0.0F,
                                       L::infinity(),
                                       -L::infinity(),
                                       L::max(),
                                       L::denorm_min()};
                EXPECT_TRUE(static_cast<arrow::FloatBuilder&>(*b)
                                .Append(edge ? edges[pick(6)]
                                             : static_cast<float>(
                                                   static_cast<std::int64_t>(pick(2000)) - 1000) /
                                                   7.0F)
                                .ok());
                break;
            }
            case arrow::Type::DOUBLE: {
                using L = std::numeric_limits<double>;
                const double edges[] = {
                    L::quiet_NaN(), -0.0, L::infinity(), -L::infinity(), L::max(), L::denorm_min()};
                EXPECT_TRUE(
                    static_cast<arrow::DoubleBuilder&>(*b)
                        .Append(edge ? edges[pick(6)]
                                     : static_cast<double>(static_cast<std::int64_t>(rng() >> 11)) /
                                           3.0)
                        .ok());
                break;
            }
            default:
                EXPECT_TRUE(static_cast<arrow::BooleanBuilder&>(*b).Append(pick(2) == 0).ok());
                break;
        }
    }
    return b->Finish().ValueOrDie();
}

// Every fast path against the cell-by-cell path and the row path on the same
// input: random arrays with edge values, with and without nulls, as given
// and sliced at several offsets. Each is reused.
TEST(NativeIntakeReuse, EveryFastPathEqualsTheCellPath) {
    struct Pair {
        std::shared_ptr<arrow::DataType> type;
        std::string declared;
    };
    const std::vector<Pair> pairs = {{arrow::int64(), "BIGINT"},
                                     {arrow::int32(), "INTEGER"},
                                     {arrow::float32(), "REAL"},
                                     {arrow::float64(), "DOUBLE"},
                                     {arrow::boolean(), "BOOLEAN"},
                                     {arrow::int64(), "TIMESTAMP(3)"},
                                     {arrow::int64(), "TIMESTAMP(9)"},
                                     {arrow::int64(), "TIMESTAMP(6) WITH TIME ZONE"},
                                     {arrow::int32(), "DATE"}};
    std::mt19937_64 rng(20261005);
    for (const auto& pair : pairs) {
        for (int round = 0; round < 12; ++round) {
            const bool nulls = round % 3 != 0;
            const auto array = ra_random_fixed(pair.type, rng, 70, nulls);
            const auto full = ra_sidecar({{"c", array}});
            for (const auto& [offset, length] : std::vector<std::pair<std::int64_t, std::int64_t>>{
                     {0, 70}, {1, 69}, {3, 40}, {9, 61}, {65, 5}, {70, 0}}) {
                SCOPED_TRACE(pair.declared + " from " + pair.type->ToString() + ", round " +
                             std::to_string(round) + ", slice " + std::to_string(offset) + "+" +
                             std::to_string(length));
                const auto sidecar = full->Slice(offset, length);
                EXPECT_EQ(ra_expect_reuse_matches("c:" + pair.declared, sidecar), 1U);
            }
            ra_expect_intake_matches("c:" + pair.declared, full);
        }
    }
}

// Only the table's pairs are fast paths: an int64 into INTEGER narrows, an
// int32 into BIGINT or a float into DOUBLE widens, and utf8 carriage of a
// TIMESTAMP or a DATE parses, so each goes cell by cell. The reused columns
// take the sidecar's own value buffers.
TEST(NativeIntakeReuse, BuilderMadeColumnsOfTheTablesPairsAreReusedAndShareTheirBuffers) {
    const auto int64s = ra_json(arrow::int64(), "[1, null, -3]");
    const auto int32s = ra_json(arrow::int32(), "[4, 5, null]");
    const auto floats = ra_json(arrow::float32(), "[1.5, null, -0.0]");
    const auto doubles = ra_json(arrow::float64(), "[2.5, 3.5, null]");
    const auto bools = ra_json(arrow::boolean(), "[true, null, false]");
    const auto texts = ra_json(arrow::utf8(), R"(["19723", null, "0"])");
    const auto sidecar = ra_sidecar({{"b", int64s},
                                     {"i", int32s},
                                     {"r", floats},
                                     {"d", doubles},
                                     {"o", bools},
                                     {"t", int64s},
                                     {"tz", int64s},
                                     {"day", int32s},
                                     {"narrow", int64s},
                                     {"wide", int32s},
                                     {"fd", floats},
                                     {"st", texts},
                                     {"sd", texts},
                                     {"v", texts}});
    const std::string spec =
        "b:BIGINT;i:INTEGER;r:REAL;d:DOUBLE;o:BOOLEAN;t:TIMESTAMP(3);"
        "tz:TIMESTAMP(3) WITH TIME ZONE;day:DATE;narrow:INTEGER;wide:BIGINT;fd:DOUBLE;"
        "st:TIMESTAMP(3);sd:DATE;v:VARCHAR";
    EXPECT_EQ(ra_expect_reuse_matches(spec, sidecar), 8U);
    ra_expect_intake_matches(spec, sidecar);

    const auto columns = ra_columns(spec);
    const RowArrowBuilder builder(columns);
    const auto compiled = compile_intake(*sidecar->schema(), columns);
    ASSERT_TRUE(std::holds_alternative<IntakePlan>(compiled));
    const auto& plan = std::get<IntakePlan>(compiled);
    EXPECT_EQ(plan.reuse,
              (std::vector<IntakeReuse>{IntakeReuse::Same,
                                        IntakeReuse::Same,
                                        IntakeReuse::Same,
                                        IntakeReuse::Same,
                                        IntakeReuse::Same,
                                        IntakeReuse::Retype,
                                        IntakeReuse::Retype,
                                        IntakeReuse::Retype,
                                        IntakeReuse::None,
                                        IntakeReuse::None,
                                        IntakeReuse::None,
                                        IntakeReuse::None,
                                        IntakeReuse::None,
                                        IntakeReuse::None}));
    const auto chunk = builder.build_columnar(*sidecar, plan);
    for (int k = 0; k < chunk->num_columns(); ++k) {
        const auto& from = *sidecar->column_data(plan.source[static_cast<std::size_t>(k)]);
        const auto& to = *chunk->column_data(k);
        const bool shared = to.buffers[1] == from.buffers[1];
        EXPECT_EQ(shared, k < 8) << chunk->schema()->field(k)->name();
    }
    EXPECT_TRUE(chunk->column(5)->type()->Equals(arrow::timestamp(arrow::TimeUnit::MILLI)));
    EXPECT_TRUE(chunk->column(6)->type()->Equals(arrow::timestamp(arrow::TimeUnit::MILLI, "UTC")));
    EXPECT_TRUE(chunk->column(7)->type()->Equals(arrow::date32()));
    // The retyped column's own ArrayData is new; the sidecar's keeps its type.
    EXPECT_TRUE(sidecar->column(6)->type()->Equals(arrow::int64()));
}

// An array that does not own its buffers goes cell by cell: a buffer with a
// parent pins memory the chunk's charge does not count, and an immutable one
// may be an import's. Two real sources of such arrays: an Arrow IPC frame
// read back, whose buffers are slices of the frame's body, and a batch
// imported through the C Data Interface, as the Iceberg source builds its
// batches. Both land the same chunk as the builder-made batch they carry.
std::shared_ptr<arrow::RecordBatch> ra_ipc_round_trip(const arrow::RecordBatch& batch) {
    auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto writer = arrow::ipc::MakeStreamWriter(sink, batch.schema()).ValueOrDie();
    EXPECT_TRUE(writer->WriteRecordBatch(batch).ok());
    EXPECT_TRUE(writer->Close().ok());
    const auto frame = sink->Finish().ValueOrDie();
    auto reader =
        arrow::ipc::RecordBatchStreamReader::Open(std::make_shared<arrow::io::BufferReader>(frame))
            .ValueOrDie();
    std::shared_ptr<arrow::RecordBatch> out;
    EXPECT_TRUE(reader->ReadNext(&out).ok());
    return out;
}

std::shared_ptr<arrow::RecordBatch> ra_c_data_round_trip(const arrow::RecordBatch& batch) {
    struct ArrowArray c_array{};
    struct ArrowSchema c_schema{};
    EXPECT_TRUE(arrow::ExportRecordBatch(batch, &c_array, &c_schema).ok());
    return arrow::ImportRecordBatch(&c_array, &c_schema).ValueOrDie();
}

TEST(NativeIntakeReuse, IpcDecodedAndCDataImportedColumnsTakeTheCellPath) {
    const auto int64s = ra_json(arrow::int64(), "[1, null, -3, 9223372036854775807]");
    const auto int32s = ra_json(arrow::int32(), "[4, 5, null, -2147483648]");
    const auto floats = ra_json(arrow::float32(), "[1.5, null, -0.0, 7]");
    const auto doubles = ra_json(arrow::float64(), "[2.5, 3.5, null, -1e300]");
    const auto bools = ra_json(arrow::boolean(), "[true, null, false, true]");
    const auto built = ra_sidecar({{"b", int64s},
                                   {"i", int32s},
                                   {"r", floats},
                                   {"d", doubles},
                                   {"o", bools},
                                   {"t", int64s},
                                   {"day", int32s}});
    const std::string spec = "b:BIGINT;i:INTEGER;r:REAL;d:DOUBLE;o:BOOLEAN;t:TIMESTAMP(3);day:DATE";
    ASSERT_EQ(ra_expect_reuse_matches(spec, built), 7U);

    const auto columns = ra_columns(spec);
    const RowArrowBuilder builder(columns);
    const auto reference = builder.build_columnar(
        *built, std::get<IntakePlan>(compile_intake(*built->schema(), columns)));

    const auto ipc = ra_ipc_round_trip(*built);
    const auto imported = ra_c_data_round_trip(*built);
    // The premises: an IPC column has a buffer with a parent, and an imported
    // one has no parent but is immutable.
    for (int k = 1; k < built->num_columns(); ++k) {
        bool parented = false;
        for (const auto& buffer : ipc->column_data(k)->buffers) {
            parented = parented || (buffer && buffer->parent() != nullptr);
        }
        EXPECT_TRUE(parented) << "IPC column " << k;
        for (const auto& buffer : imported->column_data(k)->buffers) {
            if (buffer) {
                EXPECT_EQ(buffer->parent(), nullptr) << "imported column " << k;
                EXPECT_FALSE(buffer->is_mutable()) << "imported column " << k;
            }
        }
        EXPECT_FALSE(owns_its_buffers(*ipc->column_data(k)));
        EXPECT_FALSE(owns_its_buffers(*imported->column_data(k)));
        EXPECT_TRUE(owns_its_buffers(*built->column_data(k)));
    }
    for (const auto& [name, sidecar] :
         std::vector<std::pair<std::string, std::shared_ptr<arrow::RecordBatch>>>{
             {"IPC", ipc}, {"C Data", imported}, {"C Data, sliced", imported->Slice(1)}}) {
        SCOPED_TRACE(name);
        EXPECT_EQ(ra_expect_reuse_matches(spec, sidecar), 0U);
        ra_expect_intake_matches(spec, sidecar);
    }
    std::size_t reused = 1;
    const auto from_ipc = builder.build_columnar(
        *ipc, std::get<IntakePlan>(compile_intake(*ipc->schema(), columns)), &reused);
    EXPECT_EQ(reused, 0U);
    EXPECT_TRUE(from_ipc->Equals(*reference, arrow::EqualOptions().nans_equal(true)));
    reused = 1;
    const auto from_import = builder.build_columnar(
        *imported, std::get<IntakePlan>(compile_intake(*imported->schema(), columns)), &reused);
    EXPECT_EQ(reused, 0U);
    EXPECT_TRUE(from_import->Equals(*reference, arrow::EqualOptions().nans_equal(true)));
}

// An imported batch's every buffer keeps the whole exported batch alive, wide
// columns the sink does not read included. Its chunk is the intake's own copy,
// so once the import is dropped the exported memory is gone and the chunk
// keeps alive only buffers chunk_bytes counts.
TEST(NativeIntakeReuse, AnImportedBatchsChunkKeepsNothingOfTheExportAlive) {
    arrow::ProxyMemoryPool exported(arrow::default_memory_pool());
    arrow::Int64Builder times(&exported);
    arrow::Int64Builder ids(&exported);
    arrow::Int64Builder stamps(&exported);
    arrow::StringBuilder wide(&exported);
    constexpr int kRows = 64;
    const std::string filler(16 * 1024, 'w');
    for (int i = 0; i < kRows; ++i) {
        ASSERT_TRUE(times.AppendNull().ok());
        ASSERT_TRUE(ids.Append(i).ok());
        ASSERT_TRUE(stamps.Append(1700000000000 + i).ok());
        ASSERT_TRUE(wide.Append(filler).ok());
    }
    auto source =
        arrow::RecordBatch::Make(arrow::schema({arrow::field("event_time", arrow::int64()),
                                                arrow::field("id", arrow::int64()),
                                                arrow::field("ts", arrow::int64()),
                                                arrow::field("wide", arrow::utf8())}),
                                 kRows,
                                 {times.Finish().ValueOrDie(),
                                  ids.Finish().ValueOrDie(),
                                  stamps.Finish().ValueOrDie(),
                                  wide.Finish().ValueOrDie()});
    auto imported = ra_c_data_round_trip(*source);
    source.reset();
    const std::int64_t exported_bytes = exported.bytes_allocated();
    ASSERT_GT(exported_bytes, kRows * static_cast<std::int64_t>(filler.size()));

    const auto columns = ra_columns("id:BIGINT;ts:TIMESTAMP(3)");
    const RowArrowBuilder builder(columns);
    std::size_t reused = 1;
    const auto chunk = builder.build_columnar(
        *imported, std::get<IntakePlan>(compile_intake(*imported->schema(), columns)), &reused);
    EXPECT_EQ(reused, 0U);
    imported.reset();
    // Reusing the imported columns would have kept all of it alive while
    // charging a few hundred bytes.
    EXPECT_EQ(exported.bytes_allocated(), 0);
    EXPECT_LT(chunk_bytes(*chunk), static_cast<std::size_t>(exported_bytes) / 100);
    EXPECT_EQ(chunk_bytes(*chunk), static_cast<std::size_t>(arrow::util::TotalBufferSize(*chunk)));
    EXPECT_EQ(chunk->num_rows(), kRows);
    EXPECT_EQ(static_cast<const arrow::Int64Array&>(*chunk->column(0)).Value(kRows - 1), kRows - 1);
}

}  // namespace
}  // namespace clink::clickhouse::native
