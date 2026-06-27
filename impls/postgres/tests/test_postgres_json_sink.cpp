// Offline logic tests for the Postgres JSON sink (M4): option validation + the
// pure build_insert_sql / quote_ident / columns_from_schema builders. No server.

#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>

#include "clink/connectors/postgres_json_sink.hpp"
#include "clink/connectors/postgres_sql.hpp"

namespace {

using clink::PostgresJsonSink;
using clink::PostgresJsonSinkOptions;
using clink::pgsql::build_insert_sql;
using clink::pgsql::columns_from_schema;
using clink::pgsql::quote_ident;

// Deterministic Postgres-style escaper for assertions (double single quotes).
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

PostgresJsonSinkOptions valid_opts() {
    PostgresJsonSinkOptions o;
    o.conninfo = "host=localhost";
    o.table = "t";
    o.columns = {"a", "b"};
    return o;
}

TEST(PostgresJsonSinkLogic, RequiredOptionsValidated) {
    PostgresJsonSinkOptions no_conn;
    no_conn.table = "t";
    no_conn.columns = {"a"};
    EXPECT_THROW(PostgresJsonSink{std::move(no_conn)}, std::runtime_error);

    PostgresJsonSinkOptions no_table;
    no_table.conninfo = "host=localhost";
    no_table.columns = {"a"};
    EXPECT_THROW(PostgresJsonSink{std::move(no_table)}, std::runtime_error);

    PostgresJsonSinkOptions no_cols;
    no_cols.conninfo = "host=localhost";
    no_cols.table = "t";
    EXPECT_THROW(PostgresJsonSink{std::move(no_cols)}, std::runtime_error);
}

TEST(PostgresJsonSinkLogic, OnConflictRequiresConflictColumns) {
    PostgresJsonSinkOptions o = valid_opts();
    o.on_conflict = "update";  // no conflict_columns
    EXPECT_THROW(PostgresJsonSink{std::move(o)}, std::runtime_error);

    PostgresJsonSinkOptions bad = valid_opts();
    bad.on_conflict = "bogus";
    EXPECT_THROW(PostgresJsonSink{std::move(bad)}, std::runtime_error);
}

TEST(PostgresJsonSinkLogic, ValidConstructsAndIsSafePreOpen) {
    PostgresJsonSink sink{valid_opts()};
    EXPECT_EQ(sink.name(), "postgres_sink");
    EXPECT_NO_THROW(sink.flush());
    EXPECT_NO_THROW(sink.close());
}

TEST(PostgresJsonSinkLogic, AppendInsertQuotesAndMapsTypes) {
    EXPECT_EQ(build_insert_sql("t", {"a", "b"}, "", {}, {}, {R"({"a":1,"b":"x"})"}, esc),
              "INSERT INTO \"t\" (\"a\",\"b\") VALUES (1,'x')");
}

TEST(PostgresJsonSinkLogic, BoolMapsToTrueFalseAndNullsHandled) {
    EXPECT_EQ(build_insert_sql("t", {"a", "b"}, "", {}, {}, {R"({"a":true,"b":false})"}, esc),
              "INSERT INTO \"t\" (\"a\",\"b\") VALUES (TRUE,FALSE)");
    EXPECT_EQ(build_insert_sql("t", {"a", "b"}, "", {}, {}, {R"({"a":null})"}, esc),
              "INSERT INTO \"t\" (\"a\",\"b\") VALUES (NULL,NULL)");
}

TEST(PostgresJsonSinkLogic, StringIsEscapedWithDoubledQuotes) {
    EXPECT_EQ(build_insert_sql("t", {"a"}, "", {}, {}, {R"({"a":"O'Brien"})"}, esc),
              "INSERT INTO \"t\" (\"a\") VALUES ('O''Brien')");
}

TEST(PostgresJsonSinkLogic, OnConflictUpdateDefaultsToNonKeyColumns) {
    EXPECT_EQ(build_insert_sql("t", {"a", "b"}, "update", {"a"}, {}, {R"({"a":1,"b":2})"}, esc),
              "INSERT INTO \"t\" (\"a\",\"b\") VALUES (1,2) ON CONFLICT (\"a\") DO UPDATE SET "
              "\"b\"=EXCLUDED.\"b\"");
}

TEST(PostgresJsonSinkLogic, OnConflictNothing) {
    EXPECT_EQ(build_insert_sql("t", {"a", "b"}, "nothing", {"a"}, {}, {R"({"a":1,"b":2})"}, esc),
              "INSERT INTO \"t\" (\"a\",\"b\") VALUES (1,2) ON CONFLICT (\"a\") DO NOTHING");
}

TEST(PostgresJsonSinkLogic, OnConflictUpdateAllKeyColumnsCollapsesToNothing) {
    // Every column is the conflict target -> nothing left to SET -> DO NOTHING.
    EXPECT_EQ(build_insert_sql("t", {"a"}, "update", {"a"}, {}, {R"({"a":1})"}, esc),
              "INSERT INTO \"t\" (\"a\") VALUES (1) ON CONFLICT (\"a\") DO NOTHING");
}

TEST(PostgresJsonSinkLogic, OnConflictWithoutTargetThrows) {
    EXPECT_THROW(build_insert_sql("t", {"a"}, "update", {}, {}, {R"({"a":1})"}, esc),
                 std::runtime_error);
}

TEST(PostgresJsonSinkLogic, QuoteIdentDoubleQuotesAndRejectsInjection) {
    EXPECT_EQ(quote_ident("col_1$"), "\"col_1$\"");
    EXPECT_THROW(quote_ident("a\"b"), std::runtime_error);
    EXPECT_THROW(quote_ident("a;b"), std::runtime_error);
    EXPECT_THROW(quote_ident(""), std::runtime_error);
}

TEST(PostgresJsonSinkLogic, ColumnsFromSchema) {
    EXPECT_EQ(columns_from_schema("a:i;b:s;c:d"), (std::vector<std::string>{"a", "b", "c"}));
    EXPECT_TRUE(columns_from_schema("").empty());
}

}  // namespace
