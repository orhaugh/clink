// The incident-reconcile reader (clink/clickhouse/reconcile_destination.hpp)
// against a real server: what it reads for each supported type is the
// canonical cell the expected side makes of the same value, its identity and
// mutation reads, its bounds, and the server's refusal of a write under the
// readonly setting every statement carries. Skips unless
// CLINK_CLICKHOUSE_TEST_HOST names a server; scripts/clickhouse-live.sh runs
// it on each line.

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <unistd.h>
#include <vector>

#include <clickhouse/client.h>
#include <gtest/gtest.h>

#include "clink/clickhouse/reconcile_destination.hpp"
#include "clink/config/decimal.hpp"
#include "clink/config/json.hpp"
#include "clink/incident/reconcile.hpp"

namespace rc = clink::incident::reconcile;
using clink::config::JsonValue;

namespace {

bool reconcile_server_configured() {
    const char* host = std::getenv("CLINK_CLICKHOUSE_TEST_HOST");
    return host != nullptr && *host != '\0';
}

#define REQUIRE_RECONCILE_SERVER()                                                              \
    do {                                                                                        \
        if (!reconcile_server_configured()) {                                                   \
            GTEST_SKIP() << "set CLINK_CLICKHOUSE_TEST_HOST (scripts/clickhouse-live.sh does)"; \
        }                                                                                       \
    } while (0)

std::uint16_t reconcile_port() {
    const char* p = std::getenv("CLINK_CLICKHOUSE_TEST_PORT");
    return p != nullptr && *p != '\0' ? static_cast<std::uint16_t>(std::stoi(p))
                                      : std::uint16_t{9000};
}

clickhouse::Client reconcile_admin() {
    clickhouse::ClientOptions o;
    o.SetHost(std::getenv("CLINK_CLICKHOUSE_TEST_HOST"));
    o.SetPort(reconcile_port());
    if (const char* user = std::getenv("CLINK_CLICKHOUSE_TEST_USER"); user != nullptr) {
        o.SetUser(user);
    }
    if (const char* pw = std::getenv("CLINK_CLICKHOUSE_TEST_PASSWORD"); pw != nullptr) {
        o.SetPassword(pw);
    }
    return clickhouse::Client(o);
}

clink::clickhouse::ReconcileConnection reconcile_connection(const std::string& table) {
    clink::clickhouse::ReconcileConnection c;
    c.host = std::getenv("CLINK_CLICKHOUSE_TEST_HOST");
    c.port = reconcile_port();
    if (const char* user = std::getenv("CLINK_CLICKHOUSE_TEST_USER"); user != nullptr) {
        c.user = user;
    }
    if (const char* pw = std::getenv("CLINK_CLICKHOUSE_TEST_PASSWORD"); pw != nullptr) {
        c.password = pw;
    }
    c.database = "default";
    c.table = table;
    return c;
}

std::string reconcile_table() {
    static int n = 0;
    return "clink_reconcile_" + std::to_string(static_cast<long>(::getpid())) + "_" +
           std::to_string(++n);
}

const std::vector<std::pair<std::string, std::string>>& every_type() {
    static const std::vector<std::pair<std::string, std::string>> cols = {
        {"id", "Int64"},
        {"u", "UInt32"},
        {"f", "Float64"},
        {"f32", "Float32"},
        {"d", "Decimal(18, 2)"},
        {"t", "DateTime64(3)"},
        {"t6", "DateTime64(6, 'UTC')"},
        {"t0", "DateTime"},
        {"day", "Date"},
        {"day32", "Date32"},
        {"b", "Bool"},
        {"s", "String"},
        {"ls", "LowCardinality(String)"},
        {"n", "Nullable(Int64)"},
        {"nd", "Nullable(Decimal(18, 2))"},
    };
    return cols;
}

std::vector<rc::Column> columns_of(const std::vector<std::pair<std::string, std::string>>& cols) {
    std::vector<rc::Column> out;
    for (const auto& [name, type] : cols) {
        out.push_back(rc::Column{name, rc::parse_column_type(type)});
    }
    return out;
}

}  // namespace

TEST(ClickHouseReconcileLive, WhatItReadsIsTheCellTheExpectationMakesOfTheSameValue) {
    REQUIRE_RECONCILE_SERVER();
    auto admin = reconcile_admin();
    const auto t = reconcile_table();
    std::string ddl = "CREATE TABLE default." + t + " (";
    for (std::size_t i = 0; i < every_type().size(); ++i) {
        ddl += (i == 0 ? "" : ", ") + every_type()[i].first + " " + every_type()[i].second;
    }
    admin.Execute(ddl + ") ENGINE = MergeTree ORDER BY id SETTINGS async_insert = 0");
    // The same two rows, as a Row channel record would carry them and as SQL
    // literals the server parses itself.
    admin.Execute(
        "INSERT INTO default." + t +
        " VALUES (1, 4000000000, 0.1, 0.1, 12.30, '2026-01-02 03:04:05.678', "
        "'2026-01-02 03:04:05.678912', '2026-01-02 03:04:05', '2026-01-02', '1950-06-07', true, "
        "'a''b\\\\c\\0d', 'x', NULL, NULL), (2, 0, -0.0, nan, -0.05, '1969-12-31 23:59:59.999', "
        "'1970-01-01 00:00:00.000001', '1970-01-01 00:00:00', '1970-01-01', '1900-01-01', false, "
        "'', '', -7, 1.5)");
    const auto cols = columns_of(every_type());
    auto dest =
        clink::clickhouse::make_reconcile_destination(reconcile_connection(t), rc::ReadBounds{});
    const auto rows = dest->read(cols, {0}, {{"I1"}, {"I2"}});
    ASSERT_EQ(rows.size(), 2U);
    const std::string dec = std::string{clink::config::kDecimalSentinel};
    const std::vector<std::vector<JsonValue>> expected = {
        {JsonValue{std::int64_t{1}},
         JsonValue{std::int64_t{4000000000}},
         JsonValue{0.1},
         JsonValue{0.1},
         JsonValue{dec + "12.30"},
         JsonValue{std::int64_t{1767323045678}},
         JsonValue{std::int64_t{1767323045678}},
         JsonValue{std::int64_t{1767323045000}},
         JsonValue{std::string{"2026-01-02"}},
         JsonValue{std::string{"1950-06-07"}},
         JsonValue{true},
         JsonValue{std::string{"a'b\\c"} + std::string(1, '\0') + "d"},
         JsonValue{std::string{"x"}},
         JsonValue{},
         JsonValue{}},
        {JsonValue{std::int64_t{2}},
         JsonValue{std::int64_t{0}},
         JsonValue{-0.0},
         JsonValue{std::nan("")},
         JsonValue{dec + "-0.05"},
         JsonValue{std::int64_t{-1}},
         JsonValue{std::int64_t{0}},
         JsonValue{std::int64_t{0}},
         JsonValue{std::int64_t{0}},
         JsonValue{std::string{"1900-01-01"}},
         JsonValue{false},
         JsonValue{std::string{}},
         JsonValue{std::string{}},
         JsonValue{std::int64_t{-7}},
         JsonValue{dec + "1.50"}},
    };
    for (const auto& row : rows) {
        const std::size_t which = row.at(0) == "I1" ? 0 : 1;
        for (std::size_t i = 0; i < cols.size(); ++i) {
            // t6 holds microseconds the millisecond expectation cannot carry:
            // only its row 1 value, 678912 us, differs from 678 ms, as it must.
            if (cols[i].name == "t6") {
                EXPECT_EQ(row[i], which == 0 ? "T1767323045678912@6" : "T1@6");
                continue;
            }
            EXPECT_EQ(row[i], rc::canon_expected(expected[which][i], cols[i].type))
                << "column " << cols[i].name << " of row " << which + 1;
        }
    }
    admin.Execute("DROP TABLE default." + t);
}

TEST(ClickHouseReconcileLive, ItSaysWhoTheServerAndTableAreAndListsMutations) {
    REQUIRE_RECONCILE_SERVER();
    auto admin = reconcile_admin();
    const auto t = reconcile_table();
    admin.Execute("CREATE TABLE default." + t +
                  " (id Int64, v String) ENGINE = MergeTree ORDER BY id SETTINGS async_insert = 0");
    admin.Execute("INSERT INTO default." + t + " VALUES (1, 'a'), (2, 'b')");
    auto dest =
        clink::clickhouse::make_reconcile_destination(reconcile_connection(t), rc::ReadBounds{});
    const auto d = dest->describe();
    EXPECT_EQ(d.engine, "MergeTree");
    EXPECT_EQ(d.columns.size(), 2U);
    EXPECT_FALSE(d.server_uuid.empty());
    std::string uuid;
    admin.Select(
        "SELECT toString(uuid) FROM system.tables WHERE database = 'default' AND name = '" + t +
            "'",
        [&](const clickhouse::Block& b) {
            if (b.GetRowCount() > 0) {
                uuid = std::string{b[0]->As<clickhouse::ColumnString>()->At(0)};
            }
        });
    EXPECT_EQ(d.table_uuid, uuid);
    EXPECT_TRUE(dest->mutations().empty());
    admin.Execute("ALTER TABLE default." + t +
                  " UPDATE v = 'z' WHERE id = 2 SETTINGS mutations_sync = 2");
    const auto m = dest->mutations();
    ASSERT_EQ(m.size(), 1U);
    EXPECT_TRUE(m.front().done);
    EXPECT_NE(m.front().command.find("UPDATE"), std::string::npos);
    // A table that is not there describes with no identity, which the
    // reconciliation refuses as another destination.
    auto gone = clink::clickhouse::make_reconcile_destination(reconcile_connection(t + "_gone"),
                                                              rc::ReadBounds{});
    EXPECT_TRUE(gone->describe().table_uuid.empty());
    admin.Execute("DROP TABLE default." + t);
}

TEST(ClickHouseReconcileLive, AReadPastItsBoundFailsWholeRatherThanReturningPart) {
    REQUIRE_RECONCILE_SERVER();
    auto admin = reconcile_admin();
    const auto t = reconcile_table();
    admin.Execute("CREATE TABLE default." + t +
                  " (id Int64, k Int64) ENGINE = MergeTree ORDER BY id SETTINGS async_insert = 0");
    admin.Execute("INSERT INTO default." + t + " SELECT number, number % 7 FROM numbers(3000000)");
    const auto cols = columns_of({{"id", "Int64"}, {"k", "Int64"}});
    rc::ReadBounds rows_bound;
    rows_bound.max_rows_per_query = 1;
    auto few = clink::clickhouse::make_reconcile_destination(reconcile_connection(t), rows_bound);
    EXPECT_THROW((void)few->read(cols, {0}, {{"I1"}, {"I2"}}), rc::DestinationUnavailable);
    // The key here is not the sorting key, so the read scans every row, and
    // one millisecond is not enough.
    rc::ReadBounds time_bound;
    time_bound.query_timeout = std::chrono::milliseconds{1};
    auto slow = clink::clickhouse::make_reconcile_destination(reconcile_connection(t), time_bound);
    try {
        (void)slow->read(cols, {1}, {{"I3"}});
        ADD_FAILURE() << "a read past its time bound returned";
    } catch (const rc::DestinationUnavailable& e) {
        EXPECT_NE(std::string{e.what()}.find("imeout"), std::string::npos) << e.what();
    }
    // Within its bounds the same reader answers whole.
    auto ok =
        clink::clickhouse::make_reconcile_destination(reconcile_connection(t), rc::ReadBounds{});
    EXPECT_EQ(ok->read(cols, {0}, {{"I1"}, {"I2"}}).size(), 2U);
    admin.Execute("DROP TABLE default." + t);
}

// The whole-table read a complete-table audit makes: every row, or nothing
// past its bound, and a parts marker that changes when an INSERT or a merge
// does and not otherwise.
TEST(ClickHouseReconcileLive, AWholeReadIsBoundedAndItsPartsMarkerSeesEveryChange) {
    REQUIRE_RECONCILE_SERVER();
    auto admin = reconcile_admin();
    const auto t = reconcile_table();
    admin.Execute("CREATE TABLE default." + t +
                  " (id Int64, s Nullable(String)) ENGINE = MergeTree ORDER BY id "
                  "SETTINGS async_insert = 0");
    admin.Execute("INSERT INTO default." + t + " VALUES (1, 'a'), (2, NULL)");
    const auto cols = columns_of({{"id", "Int64"}, {"s", "Nullable(String)"}});
    auto d =
        clink::clickhouse::make_reconcile_destination(reconcile_connection(t), rc::ReadBounds{});
    auto all = d->read_all(cols, 10);
    std::sort(all.begin(), all.end());
    EXPECT_EQ(all, (std::vector<rc::Row>{{"I1", "Sa"}, {"I2", "N"}}));
    EXPECT_THROW((void)d->read_all(cols, 1), rc::DestinationUnavailable);
    const std::string still = d->parts_marker();
    EXPECT_FALSE(still.empty());
    EXPECT_EQ(d->parts_marker(), still) << "nothing changed";
    admin.Execute("INSERT INTO default." + t + " VALUES (3, 'c')");
    const std::string inserted = d->parts_marker();
    EXPECT_NE(inserted, still) << "an INSERT adds a part";
    admin.Execute("OPTIMIZE TABLE default." + t + " FINAL");
    EXPECT_NE(d->parts_marker(), inserted) << "a merge replaces parts";
    EXPECT_EQ(d->read_all(cols, 10).size(), 3U);
    admin.Execute("DROP TABLE default." + t);
}

TEST(ClickHouseReconcileLive, TheServerRefusesAWriteUnderTheSettingsEveryReadCarries) {
    REQUIRE_RECONCILE_SERVER();
    auto admin = reconcile_admin();
    const auto t = reconcile_table();
    admin.Execute("CREATE TABLE default." + t + " (id Int64) ENGINE = MergeTree ORDER BY id");
    const std::string settings = clink::clickhouse::reconcile_settings(rc::ReadBounds{});
    EXPECT_NE(settings.find("readonly = 2"), std::string::npos);
    try {
        admin.Execute("INSERT INTO default." + t + " " + settings + " VALUES (1)");
        ADD_FAILURE() << "the server took a write under " << settings;
    } catch (const clickhouse::ServerException& e) {
        EXPECT_EQ(e.GetCode(), 164) << e.what();  // READONLY
    }
    std::uint64_t n = 99;
    admin.Select("SELECT count() FROM default." + t, [&](const clickhouse::Block& b) {
        if (b.GetRowCount() > 0) {
            n = b[0]->As<clickhouse::ColumnUInt64>()->At(0);
        }
    });
    EXPECT_EQ(n, 0U);
    admin.Execute("DROP TABLE default." + t);
}

TEST(ClickHouseReconcileStatements, KeysAreLiteralsAndNamesAreIdentifiers) {
    const auto cols = columns_of({{"id", "Int64"}, {"na`me", "String"}});
    const auto sql = clink::clickhouse::reconcile_read_statement(
        "db", "t`x", cols, {1}, {{"Sa'b\\c\n"}}, rc::ReadBounds{});
    EXPECT_NE(sql.find("`db`.`t\\`x`"), std::string::npos) << sql;
    EXPECT_NE(sql.find("`na\\`me` IN ('a\\'b\\\\c\\x0A')"), std::string::npos) << sql;
    EXPECT_EQ(sql.rfind("SELECT ", 0), 0U);
    EXPECT_NE(sql.find("readonly = 2"), std::string::npos);
    EXPECT_THROW((void)clink::clickhouse::reconcile_literal("I1; DROP TABLE x",
                                                            rc::parse_column_type("Int64")),
                 std::invalid_argument);
    const auto composite = clink::clickhouse::reconcile_read_statement(
        "db", "t", cols, {0, 1}, {{"I1", "Sa"}, {"I2", "Sb"}}, rc::ReadBounds{});
    EXPECT_NE(composite.find("(`id`, `na\\`me`) IN ((1, 'a'), (2, 'b'))"), std::string::npos)
        << composite;
}
