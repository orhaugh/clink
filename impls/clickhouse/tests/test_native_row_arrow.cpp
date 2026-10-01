// Row batches into typed Arrow chunks: the shared rule for the batcher's own
// scalar types, and the per-cell refusal for every type the sink converts
// itself.

#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "clink/config/decimal.hpp"
#include "clink/config/json.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_columnar_batcher.hpp"

#include "native/errors.hpp"
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

}  // namespace
}  // namespace clink::clickhouse::native
