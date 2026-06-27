// Offline logic tests for the MySQL source: factory validation + the
// build_select_sql cursor-query builder. No server required (make_mysql_poll_source
// does not connect until the first poll).

#include <stdexcept>
#include <string>
#include <string_view>

#include <gtest/gtest.h>

#include "clink/mysql/mysql_source.hpp"
#include "clink/mysql/mysql_sql.hpp"

namespace {

using clink::mysql::build_select_sql;
using clink::mysql::make_mysql_poll_source;
using clink::mysql::MysqlPollOptions;

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

MysqlPollOptions valid_opts() {
    MysqlPollOptions o;
    o.table = "events";
    o.cursor_column = "id";
    return o;
}

TEST(MysqlSourceLogic, EmptyTableThrows) {
    MysqlPollOptions o;
    o.cursor_column = "id";
    EXPECT_THROW(make_mysql_poll_source(o), std::runtime_error);
}

TEST(MysqlSourceLogic, EmptyCursorColumnThrows) {
    MysqlPollOptions o;
    o.table = "events";
    EXPECT_THROW(make_mysql_poll_source(o), std::runtime_error);
}

TEST(MysqlSourceLogic, BadIdentifierThrows) {
    MysqlPollOptions o;
    o.table = "events";
    o.cursor_column = "id; DROP";
    EXPECT_THROW(make_mysql_poll_source(o), std::runtime_error);
}

TEST(MysqlSourceLogic, ValidConstructsUnboundedSource) {
    auto src = make_mysql_poll_source(valid_opts());
    ASSERT_NE(src, nullptr);
    EXPECT_FALSE(src->is_bounded());
    EXPECT_EQ(src->name(), "mysql_source");
}

TEST(MysqlSourceLogic, SelectWithCursorIsExclusiveOrderedLimited) {
    EXPECT_EQ(build_select_sql("events", "id", /*id_column=*/"", "100", 500, esc),
              "SELECT * FROM `events` WHERE `id` > '100' ORDER BY `id` ASC LIMIT 500");
}

TEST(MysqlSourceLogic, EmptyCursorOmitsWhere) {
    EXPECT_EQ(build_select_sql("events", "id", "", "", 500, esc),
              "SELECT * FROM `events` ORDER BY `id` ASC LIMIT 500");
}

TEST(MysqlSourceLogic, CursorValueIsEscaped) {
    EXPECT_EQ(build_select_sql("events", "id", "", "x'y", 10, esc),
              "SELECT * FROM `events` WHERE `id` > 'x\\'y' ORDER BY `id` ASC LIMIT 10");
}

// With id_column set the SELECT uses keyset pagination over the (cursor, id)
// tuple, so rows sharing a cursor value are not dropped at a page boundary.
TEST(MysqlSourceLogic, IdColumnUsesCompositeKeyset) {
    const std::string composite = std::string("100") + clink::mysql::kCompositeCursorSep + "5";
    EXPECT_EQ(build_select_sql("events", "ts", "id", composite, 500, esc),
              "SELECT * FROM `events` WHERE (`ts`,`id`) > ('100','5') "
              "ORDER BY `ts` ASC,`id` ASC LIMIT 500");
}

TEST(MysqlSourceLogic, IdColumnEmptyCursorOmitsWhereButOrdersByBoth) {
    EXPECT_EQ(build_select_sql("events", "ts", "id", "", 500, esc),
              "SELECT * FROM `events` ORDER BY `ts` ASC,`id` ASC LIMIT 500");
}

}  // namespace
