// Unit tests for the shared, dialect-parameterised SQL builder (M6) used by both
// the MySQL and Postgres JSON sinks. Confirms the dialect knobs (identifier quote,
// boolean literal) and the shared head+VALUES / value-mapping / schema-parsing.

#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>

#include "clink/connectors/sql_json_builder.hpp"

namespace {

using clink::sqljson::build_insert_head_values;
using clink::sqljson::columns_from_schema;
using clink::sqljson::kMysql;
using clink::sqljson::kPostgres;
using clink::sqljson::quote_ident;

// Deterministic escaper (doubles single quotes) for assertions.
std::string esc(std::string_view s) {
    std::string o;
    for (char c : s) {
        if (c == '\'') {
            o += "''";
        } else {
            o += c;
        }
    }
    return o;
}

TEST(SqlJsonBuilder, IdentifierQuoteIsDialectSpecific) {
    EXPECT_EQ(quote_ident("col", kMysql), "`col`");
    EXPECT_EQ(quote_ident("col", kPostgres), "\"col\"");
    EXPECT_THROW(quote_ident("a`b", kMysql), std::runtime_error);
    EXPECT_THROW(quote_ident("a\"b", kPostgres), std::runtime_error);
    EXPECT_THROW(quote_ident("a;b", kPostgres), std::runtime_error);
    EXPECT_THROW(quote_ident("", kMysql), std::runtime_error);
}

TEST(SqlJsonBuilder, HeadValuesIsDialectQuotedAndMapsTypes) {
    // MySQL: backticks + bool 1/0.
    EXPECT_EQ(build_insert_head_values("t", {"a", "b"}, {R"({"a":true,"b":"x"})"}, esc, kMysql),
              "INSERT INTO `t` (`a`,`b`) VALUES (1,'x')");
    // Postgres: double-quotes + bool TRUE/FALSE.
    EXPECT_EQ(build_insert_head_values("t", {"a", "b"}, {R"({"a":true,"b":"x"})"}, esc, kPostgres),
              "INSERT INTO \"t\" (\"a\",\"b\") VALUES (TRUE,'x')");
}

TEST(SqlJsonBuilder, MissingKeyAndNullBecomeSqlNullAcrossRows) {
    EXPECT_EQ(build_insert_head_values(
                  "t", {"a", "b"}, {R"({"a":1})", R"({"a":null,"b":2})"}, esc, kPostgres),
              "INSERT INTO \"t\" (\"a\",\"b\") VALUES (1,NULL),(NULL,2)");
}

TEST(SqlJsonBuilder, StringValueEscaped) {
    EXPECT_EQ(build_insert_head_values("t", {"a"}, {R"({"a":"O'Brien"})"}, esc, kMysql),
              "INSERT INTO `t` (`a`) VALUES ('O''Brien')");
}

TEST(SqlJsonBuilder, EmptyColumnsThrows) {
    EXPECT_THROW(build_insert_head_values("t", {}, {R"({"a":1})"}, esc, kMysql),
                 std::runtime_error);
}

TEST(SqlJsonBuilder, NonObjectRowThrows) {
    EXPECT_THROW(build_insert_head_values("t", {"a"}, {"5"}, esc, kPostgres), std::runtime_error);
    EXPECT_THROW(build_insert_head_values("t", {"a"}, {"[1,2]"}, esc, kMysql), std::runtime_error);
}

// An integer goes into the statement exactly. It used to pass through a
// double, which holds integers only up to 2^53: 9007199254740993 was written as
// 9007199254740992, a different value, and a DELETE keyed on it named a
// different row.
TEST(SqlJsonBuilder, AnIntegerPast2To53IsWrittenExactly) {
    EXPECT_EQ(build_insert_head_values("t",
                                       {"a", "b", "c"},
                                       {R"({"a":9007199254740993,"b":-9223372036854775808,)"
                                        R"("c":9223372036854775807})"},
                                       esc,
                                       kPostgres),
              "INSERT INTO \"t\" (\"a\",\"b\",\"c\") VALUES "
              "(9007199254740993,-9223372036854775808,9223372036854775807)");
    // A fraction still goes through the double.
    EXPECT_EQ(build_insert_head_values("t", {"a"}, {R"({"a":0.5})"}, esc, kMysql),
              "INSERT INTO `t` (`a`) VALUES (0.5)");
}

TEST(SqlJsonBuilder, ColumnsFromSchema) {
    EXPECT_EQ(columns_from_schema("a:i;b:s;c:d"), (std::vector<std::string>{"a", "b", "c"}));
    EXPECT_TRUE(columns_from_schema("").empty());
}

}  // namespace

// A TIMESTAMP reaches these sinks as epoch milliseconds, which a timestamp
// column refuses as an integer. A column the planner names in
// timestamp_columns is written as a timestamp literal, the instant in UTC.
TEST(SqlJsonBuilder, ATimestampColumnIsWrittenAsATimestampLiteral) {
    const auto times = clink::sqljson::parse_timestamp_columns("at:ts,seen:tstz");
    const std::vector<std::string> rows = {
        R"({"at":1792863688105,"seen":1792863688105,"n":1792863688105})"};
    EXPECT_EQ(build_insert_head_values("t", {"at", "seen", "n"}, rows, esc, kPostgres, times),
              "INSERT INTO \"t\" (\"at\",\"seen\",\"n\") VALUES "
              "('2026-10-24 17:41:28.105','2026-10-24 17:41:28.105+00',1792863688105)");
    // MySQL has no zone-aware literal its columns all honour; its sinks run the
    // session in UTC instead.
    EXPECT_EQ(build_insert_head_values("t", {"at", "seen"}, rows, esc, kMysql, times),
              "INSERT INTO `t` (`at`,`seen`) VALUES "
              "('2026-10-24 17:41:28.105','2026-10-24 17:41:28.105')");
}

TEST(SqlJsonBuilder, TimestampLiteralsCoverTheEdgesOfTheCalendar) {
    using clink::sqljson::epoch_ms_to_sql_timestamp;
    using clink::sqljson::TimeKind;
    EXPECT_EQ(epoch_ms_to_sql_timestamp(0, TimeKind::Timestamp, kPostgres),
              "'1970-01-01 00:00:00.000'");
    // Before 1970 the millisecond still counts forward from the second.
    EXPECT_EQ(epoch_ms_to_sql_timestamp(-1, TimeKind::Timestamp, kPostgres),
              "'1969-12-31 23:59:59.999'");
    EXPECT_EQ(epoch_ms_to_sql_timestamp(951782400000, TimeKind::TimestampTz, kPostgres),
              "'2000-02-29 00:00:00.000+00'");
    EXPECT_EQ(epoch_ms_to_sql_timestamp(-62135596800000, TimeKind::Timestamp, kMysql),
              "'0001-01-01 00:00:00.000'");
    EXPECT_EQ(epoch_ms_to_sql_timestamp(253402300799999, TimeKind::Timestamp, kMysql),
              "'9999-12-31 23:59:59.999'");
    EXPECT_THROW((void)epoch_ms_to_sql_timestamp(253402300800000, TimeKind::Timestamp, kMysql),
                 std::runtime_error);
    EXPECT_THROW((void)epoch_ms_to_sql_timestamp(-62135596800001, TimeKind::Timestamp, kMysql),
                 std::runtime_error);
}

TEST(SqlJsonBuilder, ATimestampColumnKeepsNullAndText) {
    // NULL stays NULL; a value that arrives as text (a VARCHAR cast upstream)
    // is quoted as it is, for the server to parse.
    const auto times = clink::sqljson::parse_timestamp_columns("at:ts");
    const std::vector<std::string> rows = {R"({"at":null})", R"({"at":"2024-05-19 10:00:00"})"};
    EXPECT_EQ(build_insert_head_values("t", {"at"}, rows, esc, kPostgres, times),
              "INSERT INTO \"t\" (\"at\") VALUES (NULL),('2024-05-19 10:00:00')");
}

TEST(SqlJsonBuilder, ATimestampKeyIsWrittenTheSameWayInADelete) {
    const auto times = clink::sqljson::parse_timestamp_columns("at:ts");
    EXPECT_EQ(clink::sqljson::build_delete_by_keys_sql(
                  "t", {"at"}, {R"({"at":0})"}, esc, kPostgres, times),
              "DELETE FROM \"t\" WHERE \"at\" IN ('1970-01-01 00:00:00.000')");
}

TEST(SqlJsonBuilder, TimestampColumnsAreParsedStrictly) {
    using clink::sqljson::parse_timestamp_columns;
    EXPECT_TRUE(parse_timestamp_columns("").empty());
    EXPECT_EQ(parse_timestamp_columns("a:ts,b:tstz").size(), 2U);
    EXPECT_THROW((void)parse_timestamp_columns("a:date"), std::runtime_error);
    EXPECT_THROW((void)parse_timestamp_columns("a"), std::runtime_error);
    EXPECT_THROW((void)parse_timestamp_columns(":ts"), std::runtime_error);
}
