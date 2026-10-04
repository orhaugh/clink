// Declared SQL types from sql_column_types, the input columns of a typed
// struct from its batcher's schema, ClickHouse types from system.columns.type,
// and the header spelling the pinned client gives each.

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "clink/core/arrow_batcher.hpp"
#include "clink/core/columnar_batcher.hpp"

#include "native/errors.hpp"
#include "native/types.hpp"

// Typed structs whose batcher schemas the mapping reads. CLINK_FIELDS
// specialises a clink template, so these live at namespace scope.
struct NtTypesInner {
    std::int32_t n;
    std::string label;
};
CLINK_FIELDS(NtTypesInner, n, label);

// Every leaf CLINK_FIELDS emits, and each composite.
struct NtTypesEvery {
    std::int8_t i8;
    std::int16_t i16;
    std::int32_t i32;
    std::int64_t i64;
    std::uint8_t u8;
    std::uint16_t u16;
    std::uint32_t u32;
    std::uint64_t u64;
    float f32;
    double f64;
    bool flag;
    std::string text;
    std::optional<std::int64_t> maybe;
    std::vector<std::uint16_t> list;
    std::map<std::string, std::uint64_t> counts;
    NtTypesInner inner;
};
CLINK_FIELDS(NtTypesEvery,
             i8,
             i16,
             i32,
             i64,
             u8,
             u16,
             u32,
             u64,
             f32,
             f64,
             flag,
             text,
             maybe,
             list,
             counts,
             inner);

// A struct with a field of its own called event_time.
struct NtTypesClock {
    std::int64_t event_time;
    std::string name;
};
CLINK_FIELDS(NtTypesClock, event_time, name);

namespace clink::clickhouse::native {
namespace {

// One declared column, by its spelling alone.
SqlType types_parse_one(const std::string& spelling) {
    const auto columns = parse_sql_column_types("c:" + spelling);
    EXPECT_EQ(columns.size(), 1U);
    return columns.empty() ? SqlType{} : columns.front().type;
}

std::string types_spec_refusal(const std::string& spec) {
    try {
        (void)parse_sql_column_types(spec);
    } catch (const NativeSinkError& e) {
        EXPECT_EQ(e.code(), code::kOptionInvalid) << spec;
        return e.what();
    }
    ADD_FAILURE() << "no refusal for '" << spec << "'";
    return {};
}

// The scalar spellings arrow_to_sql_type_string renders, exactly.
TEST(NativeSqlTypes, ParsesEveryScalarSpellingTheRendererProduces) {
    const std::vector<std::pair<std::string, SqlKind>> cases = {
        {"BIGINT", SqlKind::BigInt},
        {"INTEGER", SqlKind::Integer},
        {"SMALLINT", SqlKind::SmallInt},
        {"TINYINT", SqlKind::TinyInt},
        {"BOOLEAN", SqlKind::Boolean},
        {"REAL", SqlKind::Real},
        {"DOUBLE", SqlKind::Double},
        {"VARCHAR", SqlKind::Varchar},
        {"BYTEA", SqlKind::Bytea},
        {"DATE", SqlKind::Date},
        {"TIME", SqlKind::Time},
    };
    for (const auto& [spelling, kind] : cases) {
        const SqlType t = types_parse_one(spelling);
        EXPECT_EQ(t.kind, kind) << spelling;
        EXPECT_EQ(t.spelling, spelling);
        EXPECT_TRUE(t.children.empty()) << spelling;
    }
}

TEST(NativeSqlTypes, ParsesEveryTimestampPrecisionWithAndWithoutZone) {
    for (int p = 0; p <= 9; ++p) {
        for (const bool zone : {false, true}) {
            const std::string spelling =
                "TIMESTAMP(" + std::to_string(p) + ")" + (zone ? " WITH TIME ZONE" : "");
            const SqlType t = types_parse_one(spelling);
            EXPECT_EQ(t.kind, SqlKind::Timestamp) << spelling;
            EXPECT_EQ(t.precision, p) << spelling;
            EXPECT_EQ(t.with_time_zone, zone) << spelling;
            EXPECT_EQ(t.spelling, spelling);
        }
    }
}

TEST(NativeSqlTypes, ParsesDecimalPrecisionAndScale) {
    const std::vector<std::pair<int, int>> cases = {{38, 9}, {1, 0}, {18, 2}, {10, 10}};
    for (const auto& [p, s] : cases) {
        const std::string spelling =
            "DECIMAL(" + std::to_string(p) + ", " + std::to_string(s) + ")";
        const SqlType t = types_parse_one(spelling);
        EXPECT_EQ(t.kind, SqlKind::Decimal) << spelling;
        EXPECT_EQ(t.precision, p);
        EXPECT_EQ(t.scale, s);
        EXPECT_EQ(t.spelling, spelling);
    }
}

TEST(NativeSqlTypes, ParsesChainedArraySuffixes) {
    const SqlType t = types_parse_one("BIGINT ARRAY ARRAY");
    ASSERT_EQ(t.kind, SqlKind::Array);
    EXPECT_EQ(t.spelling, "BIGINT ARRAY ARRAY");
    ASSERT_EQ(t.children.size(), 1U);
    ASSERT_EQ(t.children[0].kind, SqlKind::Array);
    EXPECT_EQ(t.children[0].spelling, "BIGINT ARRAY");
    ASSERT_EQ(t.children[0].children.size(), 1U);
    EXPECT_EQ(t.children[0].children[0].kind, SqlKind::BigInt);
    EXPECT_EQ(t.children[0].children[0].spelling, "BIGINT");
}

TEST(NativeSqlTypes, ParsesMapsAndRowsNestedInEachOther) {
    const SqlType m =
        types_parse_one("MAP<VARCHAR, ROW<x DOUBLE, y TIMESTAMP(3) WITH TIME ZONE> ARRAY>");
    ASSERT_EQ(m.kind, SqlKind::Map);
    ASSERT_EQ(m.children.size(), 2U);
    EXPECT_EQ(m.children[0].kind, SqlKind::Varchar);
    const SqlType& list = m.children[1];
    ASSERT_EQ(list.kind, SqlKind::Array);
    EXPECT_EQ(list.spelling, "ROW<x DOUBLE, y TIMESTAMP(3) WITH TIME ZONE> ARRAY");
    const SqlType& row = list.children.at(0);
    ASSERT_EQ(row.kind, SqlKind::Row);
    EXPECT_EQ(row.field_names, (std::vector<std::string>{"x", "y"}));
    ASSERT_EQ(row.children.size(), 2U);
    EXPECT_EQ(row.children[0].kind, SqlKind::Double);
    EXPECT_EQ(row.children[1].kind, SqlKind::Timestamp);
    EXPECT_EQ(row.children[1].precision, 3);
    EXPECT_TRUE(row.children[1].with_time_zone);

    const SqlType r = types_parse_one(
        "ROW<tags VARCHAR ARRAY, attrs MAP<INTEGER, DECIMAL(10, 2)>, inner ROW<z BOOLEAN>> ARRAY");
    ASSERT_EQ(r.kind, SqlKind::Array);
    const SqlType& outer = r.children.at(0);
    ASSERT_EQ(outer.kind, SqlKind::Row);
    EXPECT_EQ(outer.field_names, (std::vector<std::string>{"tags", "attrs", "inner"}));
    ASSERT_EQ(outer.children.size(), 3U);
    EXPECT_EQ(outer.children[0].kind, SqlKind::Array);
    EXPECT_EQ(outer.children[0].children.at(0).kind, SqlKind::Varchar);
    ASSERT_EQ(outer.children[1].kind, SqlKind::Map);
    EXPECT_EQ(outer.children[1].children.at(0).kind, SqlKind::Integer);
    EXPECT_EQ(outer.children[1].children.at(1).kind, SqlKind::Decimal);
    EXPECT_EQ(outer.children[1].children.at(1).precision, 10);
    EXPECT_EQ(outer.children[1].children.at(1).scale, 2);
    EXPECT_EQ(outer.children[1].spelling, "MAP<INTEGER, DECIMAL(10, 2)>");
    ASSERT_EQ(outer.children[2].kind, SqlKind::Row);
    EXPECT_EQ(outer.children[2].field_names, (std::vector<std::string>{"z"}));
    EXPECT_EQ(outer.children[2].children.at(0).kind, SqlKind::Boolean);
}

TEST(NativeSqlTypes, ParsesAPlannerSpecColumnByColumn) {
    const auto columns = parse_sql_column_types("a:BIGINT;b:VARCHAR;ts:TIMESTAMP(3);d:DATE");
    ASSERT_EQ(columns.size(), 4U);
    EXPECT_EQ(columns[0].name, "a");
    EXPECT_EQ(columns[0].type.kind, SqlKind::BigInt);
    EXPECT_EQ(columns[1].name, "b");
    EXPECT_EQ(columns[1].type.kind, SqlKind::Varchar);
    EXPECT_EQ(columns[2].name, "ts");
    EXPECT_EQ(columns[2].type.kind, SqlKind::Timestamp);
    EXPECT_EQ(columns[2].type.precision, 3);
    EXPECT_EQ(columns[3].name, "d");
    EXPECT_EQ(columns[3].type.kind, SqlKind::Date);
}

// The renderer falls back to Arrow's own ToString for every type it does not
// name; those parse as Unsupported under that spelling, top level or nested.
TEST(NativeSqlTypes, ArrowFallbackSpellingsAreUnsupported) {
    const std::vector<std::shared_ptr<arrow::DataType>> fallbacks = {
        arrow::uint8(),
        arrow::uint64(),
        arrow::float16(),
        arrow::large_utf8(),
        arrow::utf8_view(),
        arrow::large_binary(),
        arrow::fixed_size_binary(16),
        arrow::null(),
        arrow::date64(),
        arrow::time32(arrow::TimeUnit::MILLI),
        arrow::duration(arrow::TimeUnit::SECOND),
        arrow::month_interval(),
        arrow::decimal256(40, 2),
        arrow::large_list(arrow::int64()),
        arrow::fixed_size_list(arrow::int64(), 3),
        arrow::dictionary(arrow::int32(), arrow::utf8()),
    };
    for (const auto& type : fallbacks) {
        const std::string spelling = type->ToString();
        const SqlType top = types_parse_one(spelling);
        EXPECT_EQ(top.kind, SqlKind::Unsupported) << spelling;
        EXPECT_EQ(top.spelling, spelling);

        const SqlType as_value = types_parse_one("MAP<VARCHAR, " + spelling + ">");
        ASSERT_EQ(as_value.kind, SqlKind::Map) << spelling;
        EXPECT_EQ(as_value.children.at(1).kind, SqlKind::Unsupported) << spelling;
        EXPECT_EQ(as_value.children.at(1).spelling, spelling);

        const SqlType as_key = types_parse_one("MAP<" + spelling + ", BIGINT>");
        ASSERT_EQ(as_key.kind, SqlKind::Map) << spelling;
        EXPECT_EQ(as_key.children.at(0).spelling, spelling);
        EXPECT_EQ(as_key.children.at(1).kind, SqlKind::BigInt);

        const SqlType as_field = types_parse_one("ROW<a " + spelling + ", b BIGINT>");
        ASSERT_EQ(as_field.kind, SqlKind::Row) << spelling;
        EXPECT_EQ(as_field.children.at(0).kind, SqlKind::Unsupported) << spelling;
        EXPECT_EQ(as_field.children.at(0).spelling, spelling);
        EXPECT_EQ(as_field.children.at(1).kind, SqlKind::BigInt);

        const SqlType as_element = types_parse_one(spelling + " ARRAY");
        ASSERT_EQ(as_element.kind, SqlKind::Array) << spelling;
        EXPECT_EQ(as_element.children.at(0).kind, SqlKind::Unsupported) << spelling;
        EXPECT_EQ(as_element.children.at(0).spelling, spelling);
    }
}

TEST(NativeSqlTypes, NearMissSpellingsAreUnsupportedNotMalformed) {
    const std::vector<std::string> spellings = {
        "bigint",
        "BIGINTX",
        " BIGINT",
        "BIGINT ",
        "BIGINT ARRAYS",
        "TIMESTAMP",
        "TIMESTAMP(12)",
        "TIMESTAMP(3) WITH TIME ZONEX",
        "TIMESTAMP(3) WITH ZONE",
        "DECIMAL(18,2)",
        "DECIMAL(40, 2)",
        "DECIMAL(0, 0)",
        "DECIMAL(5, 7)",
        "MAP<VARCHAR,BIGINT>",
        "MAP<VARCHAR, BIGINT",
        "MAP<VARCHAR>",
        "ROW<>",
        "ROW<a>",
        "ROW<a BIGINT,b BIGINT>",
        "BIGINT,VARCHAR",
        "VARCHAR>",
    };
    for (const auto& spelling : spellings) {
        const SqlType t = types_parse_one(spelling);
        EXPECT_EQ(t.kind, SqlKind::Unsupported) << "'" << spelling << "'";
        EXPECT_EQ(t.spelling, spelling);
    }
}

TEST(NativeSqlTypes, DeepNestingIsUnsupportedRatherThanExhaustingTheStack) {
    std::string spelling;
    for (int i = 0; i < 10000; ++i) {
        spelling += "MAP<VARCHAR, ";
    }
    spelling += "BIGINT";
    for (int i = 0; i < 10000; ++i) {
        spelling += ">";
    }
    EXPECT_EQ(types_parse_one(spelling).kind, SqlKind::Unsupported);
}

TEST(NativeSqlTypes, MalformedSpecsRefuseOptionInvalid) {
    EXPECT_EQ(types_spec_refusal(""),
              "[clickhouse.option_invalid] option 'sql_column_types' must not be empty");
    EXPECT_EQ(types_spec_refusal("a"),
              "[clickhouse.option_invalid] option 'sql_column_types' entry 'a' is not name:TYPE");
    EXPECT_EQ(types_spec_refusal(":BIGINT"),
              "[clickhouse.option_invalid] option 'sql_column_types' entry ':BIGINT' has no "
              "column name");
    EXPECT_EQ(types_spec_refusal("a:"),
              "[clickhouse.option_invalid] option 'sql_column_types' entry 'a:' has no type");
    EXPECT_EQ(types_spec_refusal("a:BIGINT;"),
              "[clickhouse.option_invalid] option 'sql_column_types' has an empty entry at "
              "position 2");
    EXPECT_EQ(types_spec_refusal(";a:BIGINT"),
              "[clickhouse.option_invalid] option 'sql_column_types' has an empty entry at "
              "position 1");
    EXPECT_EQ(types_spec_refusal("a:BIGINT;a:INTEGER"),
              "[clickhouse.option_invalid] option 'sql_column_types' names column 'a' twice");
}

TEST(NativeSqlTypes, AColonInsideAFallbackSpellingStaysInTheType) {
    const auto columns = parse_sql_column_types("xs:large_list<item: int64>;y:BIGINT");
    ASSERT_EQ(columns.size(), 2U);
    EXPECT_EQ(columns[0].name, "xs");
    EXPECT_EQ(columns[0].type.kind, SqlKind::Unsupported);
    EXPECT_EQ(columns[0].type.spelling, "large_list<item: int64>");
    EXPECT_EQ(columns[1].type.kind, SqlKind::BigInt);
}

TEST(NativeSqlTypes, ArrowLayoutOfEveryDeclaredType) {
    const std::vector<std::pair<std::string, std::shared_ptr<arrow::DataType>>> cases = {
        {"TINYINT", arrow::int8()},
        {"SMALLINT", arrow::int16()},
        {"INTEGER", arrow::int32()},
        {"BIGINT", arrow::int64()},
        {"REAL", arrow::float32()},
        {"DOUBLE", arrow::float64()},
        {"BOOLEAN", arrow::boolean()},
        {"VARCHAR", arrow::utf8()},
        {"DECIMAL(18, 2)", arrow::decimal128(18, 2)},
        {"DECIMAL(38, 9)", arrow::decimal128(38, 9)},
        {"DATE", arrow::date32()},
        {"BIGINT ARRAY", arrow::list(arrow::int64())},
        {"VARCHAR ARRAY ARRAY", arrow::list(arrow::list(arrow::utf8()))},
        {"MAP<VARCHAR, DOUBLE>", arrow::map(arrow::utf8(), arrow::float64())},
        {"ROW<a BIGINT, b VARCHAR>",
         arrow::struct_({arrow::field("a", arrow::int64()), arrow::field("b", arrow::utf8())})},
        {"ROW<d DATE, t TIMESTAMP(9)> ARRAY",
         arrow::list(
             arrow::struct_({arrow::field("d", arrow::date32()),
                             arrow::field("t", arrow::timestamp(arrow::TimeUnit::MILLI))}))},
    };
    for (const auto& [spelling, expected] : cases) {
        const auto actual = arrow_type_for(types_parse_one(spelling));
        ASSERT_NE(actual, nullptr) << spelling;
        EXPECT_TRUE(actual->Equals(*expected))
            << spelling << ": " << actual->ToString() << " vs " << expected->ToString();
    }
}

// The Row value is epoch milliseconds whatever p, so every precision builds
// the same millisecond column.
TEST(NativeSqlTypes, EveryTimestampPrecisionIsMillisecondsInArrow) {
    for (int p = 0; p <= 9; ++p) {
        const auto plain = arrow_type_for(types_parse_one("TIMESTAMP(" + std::to_string(p) + ")"));
        ASSERT_NE(plain, nullptr);
        EXPECT_TRUE(plain->Equals(*arrow::timestamp(arrow::TimeUnit::MILLI))) << plain->ToString();
        const auto zoned =
            arrow_type_for(types_parse_one("TIMESTAMP(" + std::to_string(p) + ") WITH TIME ZONE"));
        ASSERT_NE(zoned, nullptr);
        EXPECT_TRUE(zoned->Equals(*arrow::timestamp(arrow::TimeUnit::MILLI, "UTC")))
            << zoned->ToString();
    }
}

TEST(NativeSqlTypes, NoArrowLayoutForTimeByteaOrUnsupported) {
    for (const std::string spelling : {"TIME",
                                       "BYTEA",
                                       "large_string",
                                       "BYTEA ARRAY",
                                       "MAP<VARCHAR, TIME>",
                                       "ROW<a BIGINT, b uint8>"}) {
        EXPECT_EQ(arrow_type_for(types_parse_one(spelling)), nullptr) << spelling;
    }
    SqlType bad_decimal;
    bad_decimal.kind = SqlKind::Decimal;
    bad_decimal.precision = 50;
    bad_decimal.scale = 2;
    EXPECT_EQ(arrow_type_for(bad_decimal), nullptr);
}

ChType types_ch(const std::string& spelling) {
    ChType t = parse_ch_type(spelling);
    EXPECT_EQ(t.spelling, spelling);
    return t;
}

TEST(NativeChTypes, ParsesEveryPlainName) {
    const std::vector<std::pair<std::string, ChKind>> cases = {
        {"Int8", ChKind::Int8},       {"Int16", ChKind::Int16},       {"Int32", ChKind::Int32},
        {"Int64", ChKind::Int64},     {"Int128", ChKind::Int128},     {"UInt8", ChKind::UInt8},
        {"UInt16", ChKind::UInt16},   {"UInt32", ChKind::UInt32},     {"UInt64", ChKind::UInt64},
        {"UInt128", ChKind::UInt128}, {"Float32", ChKind::Float32},   {"Float64", ChKind::Float64},
        {"Bool", ChKind::Bool},       {"String", ChKind::String},     {"UUID", ChKind::UUID},
        {"IPv4", ChKind::IPv4},       {"IPv6", ChKind::IPv6},         {"Date", ChKind::Date},
        {"Date32", ChKind::Date32},   {"DateTime", ChKind::DateTime},
    };
    for (const auto& [spelling, kind] : cases) {
        const ChType t = types_ch(spelling);
        EXPECT_EQ(t.kind, kind) << spelling;
        EXPECT_FALSE(t.nullable) << spelling;
        EXPECT_FALSE(t.low_cardinality) << spelling;
        EXPECT_TRUE(t.unsupported_reason.empty()) << spelling << ": " << t.unsupported_reason;
    }
}

TEST(NativeChTypes, ParsesParameterisedTypes) {
    const ChType dt = types_ch("DateTime('Europe/London')");
    EXPECT_EQ(dt.kind, ChKind::DateTime);
    EXPECT_EQ(dt.timezone, "Europe/London");

    const ChType dt64 = types_ch("DateTime64(3)");
    EXPECT_EQ(dt64.kind, ChKind::DateTime64);
    EXPECT_EQ(dt64.precision, 3);
    EXPECT_TRUE(dt64.timezone.empty());

    const ChType dt64z = types_ch("DateTime64(9, 'UTC')");
    EXPECT_EQ(dt64z.kind, ChKind::DateTime64);
    EXPECT_EQ(dt64z.precision, 9);
    EXPECT_EQ(dt64z.timezone, "UTC");

    for (int p = 0; p <= 9; ++p) {
        EXPECT_EQ(types_ch("DateTime64(" + std::to_string(p) + ")").precision, p);
    }

    const ChType dec = types_ch("Decimal(18, 4)");
    EXPECT_EQ(dec.kind, ChKind::Decimal);
    EXPECT_EQ(dec.precision, 18);
    EXPECT_EQ(dec.scale, 4);

    const std::vector<std::tuple<std::string, int, int>> sized = {
        {"Decimal32(2)", 9, 2}, {"Decimal64(4)", 18, 4}, {"Decimal128(10)", 38, 10}};
    for (const auto& [spelling, p, s] : sized) {
        const ChType t = types_ch(spelling);
        EXPECT_EQ(t.kind, ChKind::Decimal) << spelling;
        EXPECT_EQ(t.precision, p) << spelling;
        EXPECT_EQ(t.scale, s) << spelling;
    }

    const ChType fixed = types_ch("FixedString(16)");
    EXPECT_EQ(fixed.kind, ChKind::FixedString);
    EXPECT_EQ(fixed.fixed_size, 16U);
}

TEST(NativeChTypes, ParsesEnumItemsWithTheirValues) {
    const ChType e8 = types_ch("Enum8('a' = 1, 'b' = -2, 'it\\'s' = 3, 'back\\\\slash' = 4)");
    EXPECT_EQ(e8.kind, ChKind::Enum8);
    const std::vector<std::pair<std::string, std::int16_t>> items8 = {
        {"a", 1}, {"b", -2}, {"it's", 3}, {"back\\slash", 4}};
    EXPECT_EQ(e8.enum_items, items8);

    const ChType e16 = types_ch("Enum16('low' = -1000, 'high' = 30000)");
    EXPECT_EQ(e16.kind, ChKind::Enum16);
    const std::vector<std::pair<std::string, std::int16_t>> items16 = {{"low", -1000},
                                                                       {"high", 30000}};
    EXPECT_EQ(e16.enum_items, items16);
}

TEST(NativeChTypes, ParsesNullableAndLowCardinality) {
    const ChType n = types_ch("Nullable(Int64)");
    EXPECT_EQ(n.kind, ChKind::Int64);
    EXPECT_TRUE(n.nullable);
    EXPECT_FALSE(n.low_cardinality);

    const ChType lc = types_ch("LowCardinality(String)");
    EXPECT_EQ(lc.kind, ChKind::String);
    EXPECT_TRUE(lc.low_cardinality);
    EXPECT_FALSE(lc.nullable);

    const ChType lcn = types_ch("LowCardinality(Nullable(String))");
    EXPECT_EQ(lcn.kind, ChKind::String);
    EXPECT_TRUE(lcn.low_cardinality);
    EXPECT_TRUE(lcn.nullable);

    const ChType lcf = types_ch("LowCardinality(FixedString(2))");
    EXPECT_EQ(lcf.kind, ChKind::FixedString);
    EXPECT_EQ(lcf.fixed_size, 2U);
    EXPECT_TRUE(lcf.low_cardinality);

    const ChType nd = types_ch("Nullable(DateTime64(6, 'Asia/Tokyo'))");
    EXPECT_EQ(nd.kind, ChKind::DateTime64);
    EXPECT_TRUE(nd.nullable);
    EXPECT_EQ(nd.precision, 6);
    EXPECT_EQ(nd.timezone, "Asia/Tokyo");
}

TEST(NativeChTypes, ParsesArraysMapsAndTuples) {
    const ChType a = types_ch("Array(Nullable(String))");
    ASSERT_EQ(a.kind, ChKind::Array);
    ASSERT_EQ(a.children.size(), 1U);
    EXPECT_EQ(a.children[0].kind, ChKind::String);
    EXPECT_TRUE(a.children[0].nullable);
    EXPECT_EQ(a.children[0].spelling, "Nullable(String)");

    const ChType aa = types_ch("Array(Array(Int32))");
    ASSERT_EQ(aa.kind, ChKind::Array);
    ASSERT_EQ(aa.children.at(0).kind, ChKind::Array);
    EXPECT_EQ(aa.children.at(0).children.at(0).kind, ChKind::Int32);

    const ChType m = types_ch("Map(LowCardinality(String), Array(Float64))");
    ASSERT_EQ(m.kind, ChKind::Map);
    ASSERT_EQ(m.children.size(), 2U);
    EXPECT_EQ(m.children[0].kind, ChKind::String);
    EXPECT_TRUE(m.children[0].low_cardinality);
    EXPECT_EQ(m.children[0].spelling, "LowCardinality(String)");
    ASSERT_EQ(m.children[1].kind, ChKind::Array);
    EXPECT_EQ(m.children[1].children.at(0).kind, ChKind::Float64);

    const ChType unnamed = types_ch("Tuple(Int32, String)");
    ASSERT_EQ(unnamed.kind, ChKind::Tuple);
    ASSERT_EQ(unnamed.children.size(), 2U);
    EXPECT_TRUE(unnamed.element_names.empty());
    EXPECT_EQ(unnamed.children[0].kind, ChKind::Int32);
    EXPECT_EQ(unnamed.children[1].kind, ChKind::String);

    const ChType named = types_ch("Tuple(a Int32, b Nullable(String), String String)");
    ASSERT_EQ(named.kind, ChKind::Tuple);
    EXPECT_EQ(named.element_names, (std::vector<std::string>{"a", "b", "String"}));
    EXPECT_EQ(named.children.at(1).kind, ChKind::String);
    EXPECT_TRUE(named.children.at(1).nullable);
    EXPECT_EQ(named.children.at(2).kind, ChKind::String);

    const ChType quoted = types_ch("Tuple(`a b` Int32, `c``d` String)");
    ASSERT_EQ(quoted.kind, ChKind::Tuple);
    EXPECT_EQ(quoted.element_names, (std::vector<std::string>{"a b", "c`d"}));

    const ChType nested = types_ch("Tuple(x Array(Tuple(y DateTime64(3), z Map(String, UInt8))))");
    ASSERT_EQ(nested.kind, ChKind::Tuple);
    const ChType& inner = nested.children.at(0).children.at(0);
    ASSERT_EQ(inner.kind, ChKind::Tuple);
    EXPECT_EQ(inner.element_names, (std::vector<std::string>{"y", "z"}));
    EXPECT_EQ(inner.children.at(1).kind, ChKind::Map);
}

TEST(NativeChTypes, RefusesEveryUnsupportedNameWithTheSameReason) {
    const std::vector<std::string> refused = {
        "Int256",
        "UInt256",
        "BFloat16",
        "Variant(String, UInt64)",
        "Dynamic",
        "Dynamic(max_types=8)",
        "JSON",
        "JSON(max_dynamic_paths=10, a.b UInt32)",
        "Object('json')",
        "Time",
        "Time64(3)",
        "Point",
        "Ring",
        "Polygon",
        "MultiPolygon",
        "LineString",
        "AggregateFunction(sum, UInt64)",
        "AggregateFunction(quantiles(0.5, 0.9), Float64)",
        "SimpleAggregateFunction(sum, UInt64)",
        "Nothing",
        "IntervalSecond",
        "Nested(a Int32, b String)",
        "Foo",
    };
    for (const auto& spelling : refused) {
        const ChType t = types_ch(spelling);
        EXPECT_EQ(t.kind, ChKind::Unsupported) << spelling;
        EXPECT_EQ(t.unsupported_reason, spelling + " is not supported by the native sink");
    }
}

TEST(NativeChTypes, RefusesDecimalsWiderThan128Bits) {
    for (const std::string spelling : {"Decimal(39, 2)", "Decimal(76, 10)", "Decimal256(4)"}) {
        const ChType t = types_ch(spelling);
        EXPECT_EQ(t.kind, ChKind::Unsupported) << spelling;
        EXPECT_EQ(t.unsupported_reason,
                  spelling +
                      " is not supported by the native sink: clickhouse-cpp holds a decimal in at "
                      "most 128 bits, which caps its precision at 38");
    }
    EXPECT_EQ(types_ch("Decimal(38, 2)").kind, ChKind::Decimal);
}

TEST(NativeChTypes, RefusesLowCardinalityTheClientCannotBuild) {
    for (const std::string spelling : {"LowCardinality(Int32)",
                                       "LowCardinality(UUID)",
                                       "LowCardinality(Nullable(FixedString(2)))",
                                       "LowCardinality(Date)"}) {
        const ChType t = types_ch(spelling);
        EXPECT_EQ(t.kind, ChKind::Unsupported) << spelling;
        EXPECT_EQ(t.unsupported_reason,
                  spelling +
                      " is not supported by the native sink: clickhouse-cpp builds "
                      "LowCardinality only over String, FixedString(N) and Nullable(String)");
    }
}

TEST(NativeChTypes, ARefusedElementRefusesItsComposite) {
    const ChType a = types_ch("Array(Int256)");
    EXPECT_EQ(a.kind, ChKind::Unsupported);
    EXPECT_EQ(a.unsupported_reason, "Int256 is not supported by the native sink");

    const ChType m = types_ch("Map(String, Nullable(Nothing))");
    EXPECT_EQ(m.kind, ChKind::Unsupported);
    EXPECT_EQ(m.unsupported_reason, "Nothing is not supported by the native sink");

    const ChType t = types_ch("Tuple(a Int32, b Array(Decimal(50, 2)))");
    EXPECT_EQ(t.kind, ChKind::Unsupported);
    EXPECT_EQ(t.unsupported_reason,
              "Decimal(50, 2) is not supported by the native sink: clickhouse-cpp holds a "
              "decimal in at most 128 bits, which caps its precision at 38");

    const ChType nl = types_ch("Nullable(LowCardinality(String))");
    EXPECT_EQ(nl.kind, ChKind::Unsupported);
    EXPECT_EQ(nl.unsupported_reason,
              "Nullable(LowCardinality(String)) is not supported by the native sink");

    const ChType mixed = types_ch("Tuple(a Int32, String)");
    EXPECT_EQ(mixed.kind, ChKind::Unsupported);
    EXPECT_EQ(mixed.unsupported_reason,
              "Tuple(a Int32, String) is not supported by the native sink: it mixes named and "
              "unnamed elements");
}

TEST(NativeChTypes, UnreadableSpellingsAreUnsupportedAndNeverThrow) {
    const std::vector<std::string> broken = {
        "",
        "Nullable(",
        "Array(Int32",
        "Int32 Int64",
        "Int32(5)",
        "Decimal(18)",
        "Decimal(10, 12)",
        "FixedString(0)",
        "Enum8('a' = 300)",
        "Enum16('a')",
        "DateTime64(10)",
        "DateTime64()",
        "DateTime('unterminated)",
        "Tuple()",
        "Map(String)",
        "Decimal(99999999999999999999, 1)",
        "(Int32)",
    };
    for (const auto& spelling : broken) {
        ChType t;
        ASSERT_NO_THROW(t = parse_ch_type(spelling)) << spelling;
        EXPECT_EQ(t.kind, ChKind::Unsupported) << spelling;
        EXPECT_EQ(t.spelling, spelling);
        EXPECT_TRUE(t.unsupported_reason.starts_with(
            "the native sink cannot read the ClickHouse type '" + spelling + "'"))
            << spelling << ": " << t.unsupported_reason;
    }
}

TEST(NativeChTypes, DeepNestingIsUnsupportedRatherThanExhaustingTheStack) {
    std::string spelling;
    for (int i = 0; i < 10000; ++i) {
        spelling += "Array(";
    }
    spelling += "Int8";
    for (int i = 0; i < 10000; ++i) {
        spelling += ")";
    }
    EXPECT_EQ(parse_ch_type(spelling).kind, ChKind::Unsupported);
}

// The spellings the header check compares: what the pinned client regenerates
// from the server's own spelling.
TEST(NativeClientHeader, RegeneratesTheClientsOwnSpelling) {
    const std::vector<std::pair<std::string, std::string>> cases = {
        {"Bool", "UInt8"},
        {"Nullable(Bool)", "Nullable(UInt8)"},
        {"Decimal32(2)", "Decimal(9,2)"},
        {"Decimal64(4)", "Decimal(18,4)"},
        {"Decimal(18, 4)", "Decimal(18,4)"},
        {"DateTime", "DateTime"},
        {"DateTime('UTC')", "DateTime('UTC')"},
        {"DateTime64(3, 'Europe/London')", "DateTime64(3, 'Europe/London')"},
        {"LowCardinality(String)", "LowCardinality(String)"},
        {"LowCardinality(Nullable(String))", "LowCardinality(Nullable(String))"},
        {"LowCardinality(FixedString(4))", "LowCardinality(FixedString(4))"},
        {"Tuple(a Int32, b String)", "Tuple(a Int32, b String)"},
        {"Tuple(Int32, String)", "Tuple(Int32, String)"},
        {"Map(String, Array(Nullable(Int64)))", "Map(String, Array(Nullable(Int64)))"},
        {"Enum8('b' = 2, 'a' = 1)", "Enum8('a' = 1, 'b' = 2)"},
        {"Int64", "Int64"},
        {"UUID", "UUID"},
    };
    for (const auto& [server, client] : cases) {
        EXPECT_EQ(client_header_spelling(server), client) << server;
    }
}

TEST(NativeClientHeader, EmptyWhenTheClientCannotBuildTheType) {
    for (const std::string spelling : {"",
                                       "Int256",
                                       "Decimal256(2)",
                                       "Nothing",
                                       "LowCardinality(Int32)",
                                       "Array(DateTime64)",
                                       "Nullable(Null)",
                                       "Array(Tuple(Int8, DateTime64))",
                                       "LowCardinality(Nullable(Time64))",
                                       "Map(String, DateTime64)",
                                       "Decimal(99999999999999999999, 1)",
                                       "Array(Int32"}) {
        std::string header;
        ASSERT_NO_THROW(header = client_header_spelling(spelling)) << spelling;
        EXPECT_EQ(header, "") << spelling;
    }
}

// --- The input columns of a typed struct --------------------------------------

std::shared_ptr<arrow::Schema> types_schema(const arrow::FieldVector& fields) {
    return arrow::schema(fields);
}

TEST(NativeArrowSchemaColumns, EveryLeafAndCompositeOfAClinkFieldsStructMaps) {
    const auto schema = clink::make_columnar_arrow_batcher<NtTypesEvery>().schema();
    ASSERT_EQ(schema->num_fields(), 17);
    ASSERT_TRUE(clink::detail::is_event_time_field(*schema->field(0)));

    const std::vector<SqlColumn> columns = columns_from_arrow_schema(*schema);
    ASSERT_EQ(columns.size(), 16U);
    const std::vector<std::pair<std::string, SqlKind>> want = {
        {"i8", SqlKind::TinyInt},
        {"i16", SqlKind::SmallInt},
        {"i32", SqlKind::Integer},
        {"i64", SqlKind::BigInt},
        {"u8", SqlKind::UTinyInt},
        {"u16", SqlKind::USmallInt},
        {"u32", SqlKind::UInteger},
        {"u64", SqlKind::UBigInt},
        {"f32", SqlKind::Real},
        {"f64", SqlKind::Double},
        {"flag", SqlKind::Boolean},
        {"text", SqlKind::Varchar},
        {"maybe", SqlKind::BigInt},
        {"list", SqlKind::Array},
        {"counts", SqlKind::Map},
        {"inner", SqlKind::Row},
    };
    for (std::size_t i = 0; i < want.size(); ++i) {
        const auto& c = columns[i];
        EXPECT_EQ(c.name, want[i].first);
        EXPECT_EQ(c.type.kind, want[i].second) << c.name;
        // Every spelling is the Arrow type's own, field 0 being event_time.
        EXPECT_EQ(c.type.spelling, schema->field(static_cast<int>(i) + 1)->type()->ToString())
            << c.name;
    }
    EXPECT_EQ(columns[4].type.spelling, "uint8");
    EXPECT_EQ(columns[7].type.spelling, "uint64");

    const SqlType& list = columns[13].type;
    ASSERT_EQ(list.children.size(), 1U);
    EXPECT_EQ(list.children[0].kind, SqlKind::USmallInt);
    EXPECT_EQ(list.children[0].spelling, "uint16");

    const SqlType& counts = columns[14].type;
    ASSERT_EQ(counts.children.size(), 2U);
    EXPECT_EQ(counts.children[0].kind, SqlKind::Varchar);
    EXPECT_EQ(counts.children[1].kind, SqlKind::UBigInt);

    const SqlType& inner = columns[15].type;
    ASSERT_EQ(inner.children.size(), 2U);
    EXPECT_EQ(inner.field_names, (std::vector<std::string>{"n", "label"}));
    EXPECT_EQ(inner.children[0].kind, SqlKind::Integer);
    EXPECT_EQ(inner.children[1].kind, SqlKind::Varchar);
}

TEST(NativeArrowSchemaColumns, TimestampsTakeTheirUnitAndZoneDecimalsAndDatesTheirParts) {
    const auto schema = types_schema({
        arrow::field("s", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("ms", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("us", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC")),
        arrow::field("ns", arrow::timestamp(arrow::TimeUnit::NANO, "Asia/Tokyo")),
        arrow::field("amount", arrow::decimal128(18, 4)),
        arrow::field("day", arrow::date32()),
    });
    const std::vector<SqlColumn> columns = columns_from_arrow_schema(*schema);
    ASSERT_EQ(columns.size(), 6U);
    const int digits[] = {0, 3, 6, 9};
    for (int i = 0; i < 4; ++i) {
        const SqlType& t = columns[static_cast<std::size_t>(i)].type;
        EXPECT_EQ(t.kind, SqlKind::Timestamp) << i;
        EXPECT_EQ(t.unit_digits, digits[i]) << i;
        EXPECT_EQ(t.precision, digits[i]) << i;
        EXPECT_EQ(t.with_time_zone, i >= 2) << i;
        EXPECT_EQ(t.spelling, schema->field(i)->type()->ToString());
    }
    EXPECT_EQ(columns[4].type.kind, SqlKind::Decimal);
    EXPECT_EQ(columns[4].type.precision, 18);
    EXPECT_EQ(columns[4].type.scale, 4);
    EXPECT_EQ(columns[4].type.spelling, "decimal128(18, 4)");
    EXPECT_EQ(columns[5].type.kind, SqlKind::Date);
}

TEST(NativeSqlTypes, ADeclaredTimestampKeepsTheRowsMilliseconds) {
    for (int p = 0; p <= 9; ++p) {
        const SqlType t = types_parse_one("TIMESTAMP(" + std::to_string(p) + ")");
        EXPECT_EQ(t.unit_digits, 3) << p;
    }
    EXPECT_EQ(SqlType{}.unit_digits, 3);
}

TEST(NativeArrowSchemaColumns, OnlyALeadingInt64EventTimeIsTheEngineColumn) {
    // The batcher's own event_time comes first; the struct's field of the same
    // name is a column like any other.
    const auto clock = clink::make_columnar_arrow_batcher<NtTypesClock>().schema();
    ASSERT_EQ(clock->num_fields(), 3);
    const std::vector<SqlColumn> columns = columns_from_arrow_schema(*clock);
    ASSERT_EQ(columns.size(), 2U);
    EXPECT_EQ(columns[0].name, "event_time");
    EXPECT_EQ(columns[0].type.kind, SqlKind::BigInt);
    EXPECT_EQ(columns[1].name, "name");

    // A first field named event_time that is not int64 is not the engine's.
    const auto text = columns_from_arrow_schema(*types_schema(
        {arrow::field("event_time", arrow::utf8()), arrow::field("id", arrow::int64())}));
    ASSERT_EQ(text.size(), 2U);
    EXPECT_EQ(text[0].name, "event_time");
    EXPECT_EQ(text[0].type.kind, SqlKind::Varchar);

    // Nor is an int64 event_time anywhere but first.
    const auto later = columns_from_arrow_schema(*types_schema(
        {arrow::field("id", arrow::int64()), arrow::field("event_time", arrow::int64())}));
    ASSERT_EQ(later.size(), 2U);
    EXPECT_EQ(later[1].name, "event_time");
}

TEST(NativeArrowSchemaColumns, AnyOtherArrowTypeIsUnsupportedUnderItsOwnSpelling) {
    const std::vector<std::shared_ptr<arrow::DataType>> others = {
        arrow::binary(),
        arrow::large_utf8(),
        arrow::date64(),
        arrow::decimal256(40, 2),
        arrow::float16(),
        arrow::large_list(arrow::int64()),
        arrow::time32(arrow::TimeUnit::MILLI),
        arrow::dictionary(arrow::int32(), arrow::utf8()),
    };
    for (const auto& type : others) {
        const auto columns = columns_from_arrow_schema(*types_schema({arrow::field("c", type)}));
        ASSERT_EQ(columns.size(), 1U);
        EXPECT_EQ(columns[0].type.kind, SqlKind::Unsupported) << type->ToString();
        EXPECT_EQ(columns[0].type.spelling, type->ToString());
    }
    // Inside a composite only the element is unsupported, so a refusal can
    // name it.
    const auto nested =
        columns_from_arrow_schema(*types_schema({arrow::field("c", arrow::list(arrow::binary()))}));
    ASSERT_EQ(nested.size(), 1U);
    ASSERT_EQ(nested[0].type.kind, SqlKind::Array);
    EXPECT_EQ(nested[0].type.children.at(0).kind, SqlKind::Unsupported);
    EXPECT_EQ(nested[0].type.children.at(0).spelling, "binary");
}

TEST(NativeArrowSchemaColumns, NoSqlTableDeclaresAnUnsignedKindSoItHasNoRowLayout) {
    for (const SqlKind kind :
         {SqlKind::UTinyInt, SqlKind::USmallInt, SqlKind::UInteger, SqlKind::UBigInt}) {
        SqlType t;
        t.kind = kind;
        EXPECT_EQ(arrow_type_for(t), nullptr);
    }
}

}  // namespace
}  // namespace clink::clickhouse::native
