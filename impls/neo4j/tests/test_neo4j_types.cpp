// The typed JSON each declared SQL type becomes, read back with a JSON parser
// rather than compared as text, and the cells a type refuses.

#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/config/decimal.hpp"
#include "clink/config/json.hpp"
#include "clink/sql/row.hpp"

#include "options.hpp"
#include "types.hpp"

using clink::config::JsonArray;
using clink::config::JsonObject;
using clink::config::JsonValue;
using clink::neo4j::Column;
using clink::neo4j::Kind;
using clink::neo4j::Neo4jError;
using clink::neo4j::parse_column_types;
using clink::sql::Row;

namespace {

Row row_of(std::vector<std::pair<std::string, JsonValue>> cells) {
    JsonObject obj;
    for (auto& [k, v] : cells) {
        obj[k] = std::move(v);
    }
    Row r;
    r.values = clink::sql::row_columns_from_json(std::move(obj));
    return r;
}

// The typed value the encoder wrote for `column`.
JsonValue encoded(const std::string& types, const Row& row, const std::string& column) {
    const auto columns = parse_column_types(types);
    std::string out;
    clink::neo4j::encode_row(columns, row, out);
    const JsonValue map = clink::config::parse(out);
    EXPECT_EQ(map.at("$type").as_string(), "Map");
    return map.at("_value").at(column);
}

std::string type_of(const JsonValue& typed) {
    return typed.at("$type").as_string();
}
std::string text_of(const JsonValue& typed) {
    return typed.at("_value").as_string();
}

// The refusal's message, or empty when the row encodes.
std::string refused(const std::string& types, const Row& row) {
    try {
        std::string out;
        clink::neo4j::encode_row(parse_column_types(types), row, out);
    } catch (const Neo4jError& e) {
        EXPECT_EQ(e.code(), clink::neo4j::code::kValueInvalid);
        return e.what();
    }
    return {};
}

}  // namespace

TEST(Neo4jTypes, SpellingsParseToTheirKinds) {
    const auto cols = parse_column_types(
        "a:TINYINT;b:SMALLINT;c:INTEGER;d:BIGINT;e:REAL;f:DOUBLE;g:BOOLEAN;h:VARCHAR;i:DATE;"
        "j:TIMESTAMP(3);k:TIMESTAMP(6) WITH TIME ZONE;l:BIGINT ARRAY ARRAY;"
        "m:MAP<VARCHAR, DOUBLE>;n:ROW<x BIGINT, y ROW<z VARCHAR>>;o:DECIMAL(10, 2);p:TIME;"
        "q:BYTEA;r:uint64");
    ASSERT_EQ(cols.size(), 18U);
    const std::vector<Kind> kinds = {Kind::TinyInt,
                                     Kind::SmallInt,
                                     Kind::Integer,
                                     Kind::BigInt,
                                     Kind::Real,
                                     Kind::Double,
                                     Kind::Boolean,
                                     Kind::Varchar,
                                     Kind::Date,
                                     Kind::Timestamp,
                                     Kind::Timestamp,
                                     Kind::Array,
                                     Kind::Map,
                                     Kind::Row,
                                     Kind::Decimal,
                                     Kind::Time,
                                     Kind::Bytea,
                                     Kind::Unsupported};
    for (std::size_t i = 0; i < kinds.size(); ++i) {
        EXPECT_EQ(cols[i].type.kind, kinds[i]) << cols[i].name << " " << cols[i].type.spelling;
    }
    EXPECT_FALSE(cols[9].type.with_time_zone);
    EXPECT_TRUE(cols[10].type.with_time_zone);
    EXPECT_EQ(cols[11].type.children.front().kind, Kind::Array);
    EXPECT_EQ(cols[11].type.children.front().children.front().kind, Kind::BigInt);
    ASSERT_EQ(cols[13].type.field_names.size(), 2U);
    EXPECT_EQ(cols[13].type.field_names[1], "y");
    EXPECT_EQ(cols[13].type.children[1].kind, Kind::Row);
}

TEST(Neo4jTypes, IntegersStayExactAcrossTheWholeInt64Range) {
    for (const std::int64_t v : {std::numeric_limits<std::int64_t>::min(),
                                 std::int64_t{-1},
                                 std::int64_t{0},
                                 std::int64_t{9007199254740993},
                                 std::numeric_limits<std::int64_t>::max()}) {
        const auto t = encoded("v:BIGINT", row_of({{"v", JsonValue{v}}}), "v");
        EXPECT_EQ(type_of(t), "Integer");
        EXPECT_EQ(text_of(t), std::to_string(v));
    }
    // An integral double and the digits of an integer as text are whole numbers too.
    EXPECT_EQ(text_of(encoded("v:INTEGER", row_of({{"v", JsonValue{42.0}}}), "v")), "42");
    EXPECT_EQ(text_of(encoded("v:SMALLINT", row_of({{"v", JsonValue{std::string("-7")}}}), "v")),
              "-7");
    EXPECT_NE(refused("v:BIGINT", row_of({{"v", JsonValue{1.5}}})).find("whole number"),
              std::string::npos);
    EXPECT_NE(
        refused("v:BIGINT", row_of({{"v", JsonValue{std::string("12x")}}})).find("column 'v'"),
        std::string::npos);
    EXPECT_FALSE(refused("v:BIGINT", row_of({{"v", JsonValue{1e300}}})).empty());
    EXPECT_FALSE(refused("v:BIGINT", row_of({{"v", JsonValue{true}}})).empty());
}

TEST(Neo4jTypes, FloatsKeepNaNTheInfinitiesAndTheirLowBits) {
    const auto f = [](double d) { return encoded("v:DOUBLE", row_of({{"v", JsonValue{d}}}), "v"); };
    EXPECT_EQ(type_of(f(std::nan(""))), "Float");
    EXPECT_EQ(text_of(f(std::nan(""))), "NaN");
    EXPECT_EQ(text_of(f(std::numeric_limits<double>::infinity())), "Infinity");
    EXPECT_EQ(text_of(f(-std::numeric_limits<double>::infinity())), "-Infinity");
    for (const double d : {0.1,
                           -0.0,
                           1e-320,
                           std::numeric_limits<double>::max(),
                           std::numeric_limits<double>::lowest(),
                           123456789.125}) {
        const std::string text = text_of(f(d));
        // strtod, not stod: stod throws on a subnormal.
        const double back = std::strtod(text.c_str(), nullptr);
        EXPECT_EQ(back, d) << text << " does not read back as the same double";
        EXPECT_EQ(std::signbit(back), std::signbit(d)) << text;
    }
    // An integer that a double holds exactly is accepted, and one it would round is not.
    EXPECT_EQ(text_of(encoded("v:REAL", row_of({{"v", JsonValue{std::int64_t{1} << 53}}}), "v")),
              "9007199254740992");
    EXPECT_NE(refused("v:DOUBLE", row_of({{"v", JsonValue{std::int64_t{9007199254740993}}}}))
                  .find("cannot hold exactly"),
              std::string::npos);
    EXPECT_FALSE(refused("v:DOUBLE", row_of({{"v", JsonValue{std::string("NaN")}}})).empty());
}

TEST(Neo4jTypes, NullsAndMissingColumnsAreNull) {
    const auto cols = "a:BIGINT;b:VARCHAR;c:ROW<x BIGINT>;d:BIGINT ARRAY";
    const Row row = row_of({{"a", JsonValue{}}});
    for (const char* c : {"a", "b", "c", "d"}) {
        const auto t = encoded(cols, row, c);
        EXPECT_EQ(type_of(t), "Null") << c;
        EXPECT_TRUE(t.at("_value").is_null()) << c;
    }
    const auto nested = encoded("r:ROW<x BIGINT, y VARCHAR>",
                                row_of({{"r", JsonValue{JsonObject{{"x", JsonValue{}}}}}}),
                                "r");
    EXPECT_EQ(type_of(nested.at("_value").at("x")), "Null");
    EXPECT_EQ(type_of(nested.at("_value").at("y")), "Null");
}

TEST(Neo4jTypes, TextIsEscapedAndInvalidUtf8IsRefusedWithoutEchoingIt) {
    const std::string tricky = "quote\" back\\ nl\n tab\t ctl\x01 unicode \xC3\xA9\xF0\x9F\x99\x82";
    const auto t = encoded("s:VARCHAR", row_of({{"s", JsonValue{tricky}}}), "s");
    EXPECT_EQ(type_of(t), "String");
    EXPECT_EQ(text_of(t), tricky);
    // Numbers and booleans in a text column are written as their exact text.
    EXPECT_EQ(text_of(encoded(
                  "s:VARCHAR", row_of({{"s", JsonValue{std::int64_t{9007199254740993}}}}), "s")),
              "9007199254740993");
    EXPECT_EQ(text_of(encoded("s:VARCHAR", row_of({{"s", JsonValue{false}}}), "s")), "false");
    const std::string bad = "secret-value\xC3\x28";
    const std::string m = refused("s:VARCHAR", row_of({{"s", JsonValue{bad}}}));
    EXPECT_NE(m.find("not valid UTF-8"), std::string::npos) << m;
    EXPECT_EQ(m.find("secret-value"), std::string::npos) << "the refusal quoted the value: " << m;
    // A decimal reaching a text column is its canonical digits.
    const auto dec = clink::config::make_dec_value(*clink::config::dec_parse("12.50"));
    EXPECT_EQ(text_of(encoded("s:VARCHAR", row_of({{"s", dec}}), "s")), "12.50");
}

TEST(Neo4jTypes, DatesAndTimestampsAreCalendarText) {
    EXPECT_EQ(text_of(encoded("d:DATE", row_of({{"d", JsonValue{std::int64_t{0}}}}), "d")),
              "1970-01-01");
    EXPECT_EQ(text_of(encoded("d:DATE", row_of({{"d", JsonValue{std::int64_t{-1}}}}), "d")),
              "1969-12-31");
    EXPECT_EQ(
        type_of(encoded("d:DATE", row_of({{"d", JsonValue{std::string("2024-02-29")}}}), "d")),
        "Date");
    EXPECT_FALSE(refused("d:DATE", row_of({{"d", JsonValue{std::string("2024-13-01")}}})).empty());
    EXPECT_FALSE(refused("d:DATE", row_of({{"d", JsonValue{std::int64_t{99999999}}}})).empty());

    const auto ts =
        encoded("t:TIMESTAMP(3)", row_of({{"t", JsonValue{std::int64_t{1700000000123}}}}), "t");
    EXPECT_EQ(type_of(ts), "LocalDateTime");
    EXPECT_EQ(text_of(ts), "2023-11-14T22:13:20.123");
    const auto before =
        encoded("t:TIMESTAMP(3)", row_of({{"t", JsonValue{std::int64_t{-1}}}}), "t");
    EXPECT_EQ(text_of(before), "1969-12-31T23:59:59.999");
    const auto tz = encoded("t:TIMESTAMP(3) WITH TIME ZONE",
                            row_of({{"t", JsonValue{std::int64_t{1700000000123}}}}),
                            "t");
    EXPECT_EQ(type_of(tz), "OffsetDateTime");
    EXPECT_EQ(text_of(tz), "2023-11-14T22:13:20.123Z");
    EXPECT_NE(refused("t:TIMESTAMP(3)", row_of({{"t", JsonValue{std::string("2023-11-14")}}}))
                  .find("epoch milliseconds"),
              std::string::npos);
}

TEST(Neo4jTypes, ArraysMapsAndRowsNest) {
    const JsonValue arr{JsonArray{JsonValue{JsonArray{JsonValue{std::int64_t{1}}, JsonValue{}}},
                                  JsonValue{JsonArray{}}}};
    const auto list = encoded("v:BIGINT ARRAY ARRAY", row_of({{"v", arr}}), "v");
    EXPECT_EQ(type_of(list), "List");
    const auto& outer = list.at("_value").as_array();
    ASSERT_EQ(outer.size(), 2U);
    EXPECT_EQ(type_of(outer[0]), "List");
    EXPECT_EQ(text_of(outer[0].at("_value").as_array()[0]), "1");
    EXPECT_EQ(type_of(outer[0].at("_value").as_array()[1]), "Null");
    EXPECT_TRUE(outer[1].at("_value").as_array().empty());

    const auto map = encoded("m:MAP<VARCHAR, DOUBLE>",
                             row_of({{"m", JsonValue{JsonObject{{"k\"ey", JsonValue{2.5}}}}}}),
                             "m");
    EXPECT_EQ(type_of(map), "Map");
    EXPECT_EQ(text_of(map.at("_value").at("k\"ey")), "2.5");

    const auto r = encoded(
        "r:ROW<id BIGINT, tags VARCHAR ARRAY>",
        row_of(
            {{"r",
              JsonValue{JsonObject{{"id", JsonValue{std::int64_t{7}}},
                                   {"tags", JsonValue{JsonArray{JsonValue{std::string("a")}}}}}}}}),
        "r");
    EXPECT_EQ(text_of(r.at("_value").at("id")), "7");
    EXPECT_EQ(text_of(r.at("_value").at("tags").at("_value").as_array()[0]), "a");

    EXPECT_NE(
        refused("v:BIGINT ARRAY", row_of({{"v", JsonValue{std::int64_t{1}}}})).find("an array"),
        std::string::npos);
    const std::string m = refused(
        "v:BIGINT ARRAY", row_of({{"v", JsonValue{JsonArray{JsonValue{std::string("x")}}}}}));
    EXPECT_NE(m.find("v[0]"), std::string::npos) << "the refusal names the element: " << m;
}

TEST(Neo4jTypes, CalendarFormattingAndUtf8Checks) {
    std::string out;
    EXPECT_TRUE(clink::neo4j::format_date(-719162, out));
    EXPECT_EQ(out, "0001-01-01");
    out.clear();
    EXPECT_TRUE(clink::neo4j::format_date(2932896, out));
    EXPECT_EQ(out, "9999-12-31");
    out.clear();
    EXPECT_FALSE(clink::neo4j::format_date(2932897, out));
    EXPECT_FALSE(clink::neo4j::format_date(-719163, out));
    EXPECT_TRUE(clink::neo4j::valid_utf8("plain ascii"));
    EXPECT_TRUE(clink::neo4j::valid_utf8("\xE2\x82\xAC"));
    EXPECT_FALSE(clink::neo4j::valid_utf8("\xC0\xAF"));          // overlong
    EXPECT_FALSE(clink::neo4j::valid_utf8("\xED\xA0\x80"));      // surrogate
    EXPECT_FALSE(clink::neo4j::valid_utf8("\xF4\x90\x80\x80"));  // past U+10FFFF
    EXPECT_FALSE(clink::neo4j::valid_utf8("\xE2\x82"));          // cut short
}
