// Offline logic tests for the MySQL sink: option validation + the build_insert_sql
// SQL builder (the load-bearing pure function). No server required.

#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>

#include "clink/mysql/mysql_sink.hpp"
#include "clink/mysql/mysql_sql.hpp"

namespace {

using clink::mysql::build_insert_sql;
using clink::mysql::columns_from_schema;
using clink::mysql::MysqlSink;
using clink::mysql::MysqlSinkOptions;
using clink::mysql::quote_ident;

// Minimal deterministic escaper for assertions (backslash-escape ' and \).
std::string esc(std::string_view s) {
    std::string o;
    for (char c : s) {
        if (c == '\'' || c == '\\') {
            o += '\\';
        }
        o += c;
    }
    return o;
}

MysqlSinkOptions valid_opts() {
    MysqlSinkOptions o;
    o.table = "t";
    o.columns = {"a", "b"};
    return o;
}

TEST(MysqlSinkLogic, EmptyTableThrows) {
    MysqlSinkOptions o;
    o.columns = {"a"};
    EXPECT_THROW(MysqlSink{std::move(o)}, std::runtime_error);
}

TEST(MysqlSinkLogic, EmptyColumnsThrows) {
    MysqlSinkOptions o;
    o.table = "t";
    EXPECT_THROW(MysqlSink{std::move(o)}, std::runtime_error);
}

TEST(MysqlSinkLogic, BadIdentifierThrowsAtConstruction) {
    MysqlSinkOptions o;
    o.table = "t; DROP TABLE x";  // not a plain identifier
    o.columns = {"a"};
    EXPECT_THROW(MysqlSink{std::move(o)}, std::runtime_error);
}

TEST(MysqlSinkLogic, ValidConstructsAndNames) {
    MysqlSink sink{valid_opts()};
    EXPECT_EQ(sink.name(), "mysql_sink");
    EXPECT_NO_THROW(sink.flush());  // nothing buffered, no connection touched
    EXPECT_NO_THROW(sink.close());
}

TEST(MysqlSinkLogic, AppendInsertMapsTypesAndQuotesIdentifiers) {
    const std::string sql =
        build_insert_sql("t", {"a", "b"}, /*upsert=*/false, {}, {R"({"a":1,"b":"x"})"}, esc);
    EXPECT_EQ(sql, "INSERT INTO `t` (`a`,`b`) VALUES (1,'x')");
}

TEST(MysqlSinkLogic, MissingKeyAndJsonNullBothBecomeSqlNull) {
    EXPECT_EQ(build_insert_sql("t", {"a", "b"}, false, {}, {R"({"a":1})"}, esc),
              "INSERT INTO `t` (`a`,`b`) VALUES (1,NULL)");
    EXPECT_EQ(build_insert_sql("t", {"a", "b"}, false, {}, {R"({"a":null,"b":2})"}, esc),
              "INSERT INTO `t` (`a`,`b`) VALUES (NULL,2)");
}

TEST(MysqlSinkLogic, BoolMapsToOneZeroAndStringIsEscaped) {
    EXPECT_EQ(build_insert_sql("t", {"a"}, false, {}, {R"({"a":true})"}, esc),
              "INSERT INTO `t` (`a`) VALUES (1)");
    EXPECT_EQ(build_insert_sql("t", {"a"}, false, {}, {R"({"a":false})"}, esc),
              "INSERT INTO `t` (`a`) VALUES (0)");
    EXPECT_EQ(build_insert_sql("t", {"a"}, false, {}, {R"({"a":"O'Brien"})"}, esc),
              "INSERT INTO `t` (`a`) VALUES ('O\\'Brien')");
}

TEST(MysqlSinkLogic, MultipleRowsAreOneStatement) {
    EXPECT_EQ(build_insert_sql("t", {"a"}, false, {}, {R"({"a":1})", R"({"a":2})"}, esc),
              "INSERT INTO `t` (`a`) VALUES (1),(2)");
}

TEST(MysqlSinkLogic, UpsertAppendsOnDuplicateKeyUpdate) {
    EXPECT_EQ(build_insert_sql("t", {"a", "b"}, /*upsert=*/true, {}, {R"({"a":1,"b":2})"}, esc),
              "INSERT INTO `t` (`a`,`b`) VALUES (1,2) AS clink_new ON DUPLICATE KEY UPDATE "
              "`a`=clink_new.`a`,`b`=clink_new.`b`");
}

TEST(MysqlSinkLogic, UpsertUpdateColumnsLimitsSetList) {
    EXPECT_EQ(build_insert_sql("t", {"a", "b"}, true, {"b"}, {R"({"a":1,"b":2})"}, esc),
              "INSERT INTO `t` (`a`,`b`) VALUES (1,2) AS clink_new ON DUPLICATE KEY UPDATE "
              "`b`=clink_new.`b`");
}

TEST(MysqlSinkLogic, EmptyColumnsBuilderThrows) {
    EXPECT_THROW(build_insert_sql("t", {}, false, {}, {R"({"a":1})"}, esc), std::runtime_error);
}

TEST(MysqlSinkLogic, ColumnsDerivedFromSchemaColumns) {
    // The SQL Row path injects "name:typecode;name:typecode"; the sink derives its
    // projection from it when no explicit columns= is given.
    EXPECT_EQ(columns_from_schema("a:i;b:s;c:d"), (std::vector<std::string>{"a", "b", "c"}));
    EXPECT_EQ(columns_from_schema("only:i"), (std::vector<std::string>{"only"}));
    EXPECT_TRUE(columns_from_schema("").empty());
}

TEST(MysqlSinkLogic, QuoteIdentRejectsInjection) {
    EXPECT_THROW(quote_ident("a`b"), std::runtime_error);
    EXPECT_THROW(quote_ident("a.b"), std::runtime_error);
    EXPECT_THROW(quote_ident(""), std::runtime_error);
    EXPECT_EQ(quote_ident("col_1$"), "`col_1$`");
}

}  // namespace
