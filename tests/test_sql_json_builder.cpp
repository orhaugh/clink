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

TEST(SqlJsonBuilder, ColumnsFromSchema) {
    EXPECT_EQ(columns_from_schema("a:i;b:s;c:d"), (std::vector<std::string>{"a", "b", "c"}));
    EXPECT_TRUE(columns_from_schema("").empty());
}

}  // namespace
