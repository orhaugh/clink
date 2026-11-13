// How declared SQL types and row values become Relate values: the spellings
// the planner sends, each type written into a store and read back, the values
// that are refused, and how a batch is weighed against Relate's own limits.

#include <cmath>
#include <cstdint>
#include <functional>
#include <limits>
#include <map>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/sql/row.hpp"

#include "options.hpp"
#include "relate_sink.hpp"
#include "store_reader.hpp"
#include "types.hpp"

using clink::config::JsonArray;
using clink::config::JsonObject;
using clink::config::JsonValue;
using clink::relate::Column;
using clink::relate::Kind;
using clink::relate::parse_column_types;
using clink::relate::parse_options;
using clink::relate::RelateError;
using clink::relate::RelateSink;
using clink::relate::row_weight;
using clink::relate::RowsParam;
using clink::relate::testing::query;
using clink::relate::testing::TempDir;
using clink::sql::Row;
namespace code = clink::relate::code;

namespace {

Row make_row(JsonObject obj) {
    Row r;
    r.values = clink::sql::row_columns_from_json(std::move(obj));
    return r;
}

struct Refusal {
    std::string code;
    std::string message;
};

// What adding one value of `type` to a batch is refused with, if it is.
Refusal refusal(const std::string& type, JsonValue value) {
    const auto columns = parse_column_types("c:" + type);
    JsonObject obj;
    obj["c"] = std::move(value);
    const Row row = make_row(std::move(obj));
    RowsParam batch;
    try {
        batch.add(columns, row, row_weight(columns, row));
    } catch (const RelateError& e) {
        return {e.code(), e.what()};
    }
    return {};
}

}  // namespace

TEST(RelateTypes, TheSpellingsThePlannerSendsAreParsed) {
    const auto columns = parse_column_types(
        "a:TINYINT;b:SMALLINT;c:INTEGER;d:BIGINT;e:REAL;f:DOUBLE;g:BOOLEAN;h:VARCHAR;i:DATE;"
        "j:TIMESTAMP(3);k:TIMESTAMP(3) WITH TIME ZONE;l:VARCHAR ARRAY;m:MAP<VARCHAR, BIGINT>;"
        "n:ROW<name VARCHAR, at TIMESTAMP(3)>;o:DECIMAL(10, 2);p:TIME;q:BYTEA;r:GEOMETRY");
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
    ASSERT_EQ(columns.size(), kinds.size());
    for (std::size_t i = 0; i < kinds.size(); ++i) {
        EXPECT_EQ(columns[i].type.kind, kinds[i]) << columns[i].name;
    }
    EXPECT_FALSE(columns[9].type.with_time_zone);
    EXPECT_TRUE(columns[10].type.with_time_zone);
    EXPECT_EQ(columns[11].type.children.front().kind, Kind::Varchar);
    EXPECT_EQ(columns[13].type.field_names, (std::vector<std::string>{"name", "at"}));
    const auto problems = clink::relate::unsupported_columns(columns);
    ASSERT_EQ(problems.size(), 4U);
    EXPECT_NE(problems[0].find("column 'o' (DECIMAL(10, 2))"), std::string::npos) << problems[0];
    EXPECT_NE(problems[3].find("column 'r' (GEOMETRY)"), std::string::npos) << problems[3];
}

TEST(RelateTypes, EveryTypeIsWrittenAsItsRelateValueAndReadBack) {
    const TempDir dir;
    const auto store = dir.path("types.rl");
    RelateSink sink(parse_options(
        {{"path", store},
         {"statement",
          "UNWIND $rows AS row "
          "CREATE (:V {id: row.id, i: row.i, f: row.f, b: row.b, s: row.s, d: row.d, ts: row.ts, "
          "tz: row.tz, tags: row.tags, attr_a: row.attrs.a, who_name: row.who.name, "
          "who_age: row.who.age})"},
         {"sql_column_types",
          "id:BIGINT;i:INTEGER;f:DOUBLE;b:BOOLEAN;s:VARCHAR;d:DATE;ts:TIMESTAMP(3);"
          "tz:TIMESTAMP(3) WITH TIME ZONE;tags:VARCHAR ARRAY;attrs:MAP<VARCHAR, BIGINT>;"
          "who:ROW<name VARCHAR, age INTEGER>"}},
        0,
        1));
    sink.open();

    clink::Batch<Row> batch;
    {
        JsonObject o;
        o["id"] = JsonValue{std::int64_t{1}};
        o["i"] = JsonValue{std::int64_t{42}};
        o["f"] = JsonValue{2.5};
        o["b"] = JsonValue{true};
        o["s"] =
            JsonValue{std::string("gr\xC3\xBC\xC3\x9F"
                                  "e \xF0\x9F\x8C\x8D")};
        o["d"] = JsonValue{std::int64_t{20000}};
        o["ts"] = JsonValue{std::int64_t{1794301510123}};
        o["tz"] = JsonValue{std::int64_t{1794301510123}};
        o["tags"] = JsonValue{JsonArray{JsonValue{"a"}, JsonValue{"b"}}};
        JsonObject attrs;
        attrs["a"] = JsonValue{std::int64_t{7}};
        o["attrs"] = JsonValue{std::move(attrs)};
        JsonObject who;
        who["name"] = JsonValue{"Ada"};
        who["age"] = JsonValue{std::int64_t{36}};
        o["who"] = JsonValue{std::move(who)};
        batch.emplace(make_row(std::move(o)));
    }
    {
        // Nothing but the key: every other column is absent from the row.
        JsonObject o;
        o["id"] = JsonValue{std::int64_t{2}};
        batch.emplace(make_row(std::move(o)));
    }
    {
        JsonObject o;
        o["id"] = JsonValue{std::int64_t{3}};
        o["i"] = JsonValue{std::string("-7")};  // digits as text
        o["f"] = JsonValue{std::numeric_limits<double>::quiet_NaN()};
        o["b"] = JsonValue{false};
        o["s"] = JsonValue{std::int64_t{12}};  // a number in a VARCHAR column is its text
        o["d"] = JsonValue{std::string("2024-02-29")};
        o["ts"] = JsonValue{std::int64_t{-1}};
        o["tz"] = JsonValue{std::int64_t{0}};
        o["tags"] = JsonValue{JsonArray{}};
        o["who"] = JsonValue{JsonObject{}};
        batch.emplace(make_row(std::move(o)));
    }
    {
        JsonObject o;
        o["id"] = JsonValue{std::int64_t{4}};
        o["f"] = JsonValue{-std::numeric_limits<double>::infinity()};
        o["i"] = JsonValue{3.0};  // an integral double in an INTEGER column
        batch.emplace(make_row(std::move(o)));
    }
    sink.on_data(batch);
    sink.flush();

    const std::string read =
        "MATCH (v:V {id: $ID}) RETURN v.i, v.f, v.b, v.s, v.d, v.ts, v.tz, v.tags, v.attr_a, "
        "v.who_name, v.who_age";
    const auto at = [&](int id) {
        std::string q = read;
        q.replace(q.find("$ID"), 3, std::to_string(id));
        const auto rows = query(store, q);
        return rows.size() == 1 ? rows[0] : std::vector<std::string>{"no single row"};
    };
    EXPECT_EQ(at(1),
              (std::vector<std::string>{"42",
                                        "2.5",
                                        "true",
                                        "gr\xC3\xBC\xC3\x9F"
                                        "e \xF0\x9F\x8C\x8D",
                                        "2024-10-04",
                                        "2026-11-10T09:05:10.123",
                                        "2026-11-10T09:05:10.123Z",
                                        "[a, b]",
                                        "7",
                                        "Ada",
                                        "36"}));
    EXPECT_EQ(at(2), std::vector<std::string>(11, "null"));
    EXPECT_EQ(at(3),
              (std::vector<std::string>{"-7",
                                        "NaN",
                                        "false",
                                        "12",
                                        "2024-02-29",
                                        "1969-12-31T23:59:59.999",
                                        "1970-01-01T00:00:00.000Z",
                                        "[]",
                                        "null",
                                        "null",
                                        "null"}));
    EXPECT_EQ(at(4)[0], "3");
    EXPECT_EQ(at(4)[1], "-Infinity");
    sink.close();
}

TEST(RelateTypes, AValueThatDoesNotFitItsColumnIsRefusedWithoutBeingQuoted) {
    const auto text = refusal("BIGINT", JsonValue{std::string("12x-secret")});
    EXPECT_EQ(text.code, code::kValueInvalid);
    EXPECT_NE(text.message.find("column 'c' (BIGINT): expected a whole number, got a string"),
              std::string::npos)
        << text.message;
    EXPECT_EQ(text.message.find("secret"), std::string::npos) << "the value is not quoted";

    EXPECT_EQ(refusal("BIGINT", JsonValue{1.5}).code, code::kValueInvalid);
    EXPECT_EQ(refusal("INTEGER", JsonValue{true}).code, code::kValueInvalid);
    EXPECT_EQ(refusal("DOUBLE", JsonValue{std::string("1.5")}).code, code::kValueInvalid);
    // 2^53 + 1: an integer no double holds.
    EXPECT_EQ(refusal("DOUBLE", JsonValue{std::int64_t{9007199254740993}}).code,
              code::kValueInvalid);
    EXPECT_EQ(refusal("BOOLEAN", JsonValue{std::int64_t{1}}).code, code::kValueInvalid);
    EXPECT_EQ(refusal("VARCHAR", JsonValue{JsonArray{}}).code, code::kValueInvalid);
    EXPECT_EQ(refusal("VARCHAR ARRAY", JsonValue{std::string("a,b")}).code, code::kValueInvalid);
    EXPECT_EQ(refusal("MAP<VARCHAR, BIGINT>", JsonValue{JsonArray{}}).code, code::kValueInvalid);

    const auto utf8 = refusal("VARCHAR",
                              JsonValue{std::string("a\xFF"
                                                    "b")});
    EXPECT_EQ(utf8.code, code::kValueInvalid);
    EXPECT_NE(utf8.message.find("Relate refused the value"), std::string::npos) << utf8.message;

    EXPECT_EQ(refusal("DATE", JsonValue{std::int64_t{3000000}}).code, code::kValueInvalid);
    EXPECT_EQ(refusal("DATE", JsonValue{std::string("10/11/2026")}).code, code::kValueInvalid);
    const auto february = refusal("DATE", JsonValue{std::string("2026-02-30")});
    EXPECT_EQ(february.code, code::kValueInvalid);
    EXPECT_NE(february.message.find("Relate refused the value"), std::string::npos)
        << february.message;
    EXPECT_EQ(refusal("TIMESTAMP(3)", JsonValue{std::int64_t{400000000000000}}).code,
              code::kValueInvalid);

    JsonObject keyed;
    keyed[std::string("a\0b", 3)] = JsonValue{std::int64_t{1}};
    EXPECT_EQ(refusal("MAP<VARCHAR, BIGINT>", JsonValue{std::move(keyed)}).code,
              code::kValueInvalid);

    // An element of an array is named by its place.
    const auto element =
        refusal("BIGINT ARRAY", JsonValue{JsonArray{JsonValue{std::int64_t{1}}, JsonValue{"x"}}});
    EXPECT_NE(element.message.find("column 'c[1]'"), std::string::npos) << element.message;
}

TEST(RelateTypes, ARowIsWeighedByItsNamesAndValues) {
    const auto columns = parse_column_types("id:BIGINT;name:VARCHAR;tags:VARCHAR ARRAY");
    JsonObject o;
    o["id"] = JsonValue{std::int64_t{1}};
    o["name"] = JsonValue{std::string("0123456789")};
    o["tags"] = JsonValue{JsonArray{JsonValue{"ab"}, JsonValue{"cde"}}};
    // The row's map, then per column its name and value: a number is 16 + 8, a
    // string 16 + its bytes, a list 16 + its items.
    EXPECT_EQ(row_weight(columns, make_row(std::move(o))),
              16U + (2 + 24) + (4 + 16 + 10) + (4 + 16 + (16 + 2) + (16 + 3)));
    // A column the row does not carry is a null.
    EXPECT_EQ(row_weight(columns, make_row(JsonObject{})), 16U + (2 + 24) + (4 + 24) + (4 + 24));
}

TEST(RelateTypes, ABatchAsHeavyAsBatchBytesAllowsIsWithinRelatesOwnLimit) {
    const auto columns = parse_column_types("id:BIGINT;name:VARCHAR");
    JsonObject o;
    o["id"] = JsonValue{std::int64_t{1}};
    o["name"] = JsonValue{std::string(1000, 'x')};
    const Row row = make_row(std::move(o));
    const std::uint64_t weight = row_weight(columns, row);
    // The greatest batch_bytes the sink accepts.
    constexpr std::uint64_t kMaxBatchBytes = 12ULL << 20;
    RowsParam batch;
    while (batch.bytes() + weight <= kMaxBatchBytes) {
        ASSERT_NO_THROW(batch.add(columns, row, weight)) << "after " << batch.rows() << " rows";
    }
    EXPECT_GT(batch.rows(), 11000U);
}

TEST(RelateTypes, ABatchHoldsAtMostAHundredThousandRows) {
    const auto columns = parse_column_types("id:BIGINT");
    JsonObject o;
    o["id"] = JsonValue{std::int64_t{1}};
    const Row row = make_row(std::move(o));
    const std::uint64_t weight = row_weight(columns, row);
    RowsParam batch;
    for (int i = 0; i < 100'000; ++i) {
        batch.add(columns, row, weight);
    }
    EXPECT_EQ(batch.rows(), 100'000U);
    try {
        batch.add(columns, row, weight);
        FAIL() << "the builder took a row past Relate's limit";
    } catch (const RelateError& e) {
        EXPECT_EQ(e.code(), code::kRowTooLarge);
    }
    batch.clear();
    EXPECT_EQ(batch.rows(), 0U);
    EXPECT_EQ(batch.bytes(), 0U);
    EXPECT_NO_THROW(batch.add(columns, row, weight));
}
