// Offline tests for the MySQL CDC initial-snapshot pure helpers (mysql_snapshot.hpp):
// table qualification, the snapshot SELECT (identifier quoting + injection guard),
// and row->Insert-CdcEvent (NULL handling, byte-identical to a streamed insert
// through cdc_event_to_json_row). No live server.

#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/connectors/cdc_json.hpp"

#ifdef CLINK_HAS_MYSQL
#include "clink/mysql/mysql_cdc_source.hpp"  // make_mysql_cdc_source (ctor does not connect)
#include "clink/mysql/mysql_snapshot.hpp"
#endif

#ifdef CLINK_HAS_MYSQL

using clink::mysql::build_snapshot_select;
using clink::mysql::make_mysql_cdc_source;
using clink::mysql::MysqlCdcOptions;
using clink::mysql::qualify_table;
using clink::mysql::snapshot_row_to_event;

TEST(MysqlSnapshot, QualifyQualifiedName) {
    auto q = qualify_table("shop.orders", "ignored");
    EXPECT_EQ(q.db, "shop");
    EXPECT_EQ(q.table, "orders");
}

TEST(MysqlSnapshot, QualifyBareNameUsesDefaultDb) {
    auto q = qualify_table("orders", "shop");
    EXPECT_EQ(q.db, "shop");
    EXPECT_EQ(q.table, "orders");
}

TEST(MysqlSnapshot, QualifyBareNameWithoutDefaultThrows) {
    EXPECT_THROW(qualify_table("orders", ""), std::runtime_error);
}

TEST(MysqlSnapshot, QualifyEmptyPartsThrow) {
    EXPECT_THROW(qualify_table(".orders", "db"), std::runtime_error);  // empty db
    EXPECT_THROW(qualify_table("shop.", "db"), std::runtime_error);    // empty table
}

TEST(MysqlSnapshot, BuildSelectQuotesIdentifiers) {
    EXPECT_EQ(build_snapshot_select({"shop", "orders"}), "SELECT * FROM `shop`.`orders`");
}

TEST(MysqlSnapshot, BuildSelectRejectsInjection) {
    // quote_ident validates [A-Za-z0-9_$]; a table name with a backtick/space is
    // rejected rather than escaped into the query.
    EXPECT_THROW(build_snapshot_select({"shop", "orders`; DROP TABLE x; --"}), std::runtime_error);
}

TEST(MysqlSnapshot, RowToEventIsInsertWithNullHandling) {
    std::vector<std::string> names{"id", "name", "note"};
    std::vector<std::optional<std::string>> cells{
        std::string("7"), std::string("ada"), std::nullopt};
    auto ev = snapshot_row_to_event("shop.orders", "binlog.000003:1547", names, cells);
    EXPECT_EQ(ev.op, clink::CdcEvent::Op::Insert);
    EXPECT_EQ(ev.table, "shop.orders");
    EXPECT_EQ(ev.lsn, "binlog.000003:1547");
    EXPECT_EQ(ev.xid, 0);
    ASSERT_EQ(ev.values.size(), 3u);
    EXPECT_EQ(ev.values[0].value, "7");
    EXPECT_FALSE(ev.values[0].is_null);
    EXPECT_TRUE(ev.values[2].is_null);

    // The JSON row is what flows downstream: data columns at top level (NULL -> JSON
    // null), __op=insert + __table/__lsn metadata - identical to a streamed insert.
    auto js = clink::cdc::cdc_event_to_json_row(ev);
    ASSERT_TRUE(js.has_value());
    auto j = clink::config::parse(*js);
    ASSERT_TRUE(j.is_object());
    const auto& o = j.as_object();
    EXPECT_EQ(o.at("id").as_string(), "7");
    EXPECT_EQ(o.at("name").as_string(), "ada");
    EXPECT_TRUE(o.at("note").is_null());
    EXPECT_EQ(o.at("__op").as_string(), "insert");
    EXPECT_EQ(o.at("__table").as_string(), "shop.orders");
    EXPECT_EQ(o.at("__lsn").as_string(), "binlog.000003:1547");
}

TEST(MysqlSnapshot, RowToEventLengthMismatchThrows) {
    std::vector<std::string> names{"id", "name"};
    std::vector<std::optional<std::string>> cells{std::string("1")};  // one cell, two names
    EXPECT_THROW(snapshot_row_to_event("t", "p", names, cells), std::runtime_error);
}

// make_mysql_cdc_source validates the snapshot scope at construction (no connect).
// The snapshot allowlist must be non-empty AND fully qualified, so the snapshot and
// the change stream cover exactly the same tables (a bare name would stream
// same-named tables in other DBs the snapshot never bootstraps).
TEST(MysqlSnapshot, SnapshotRequiresNonEmptyTables) {
    MysqlCdcOptions o;
    o.server_id = 1;
    o.enable_initial_snapshot = true;  // tables empty
    EXPECT_THROW(make_mysql_cdc_source(o), std::runtime_error);
}

TEST(MysqlSnapshot, SnapshotRejectsBareTableName) {
    MysqlCdcOptions o;
    o.server_id = 1;
    o.enable_initial_snapshot = true;
    o.tables = {"orders"};  // bare, not db-qualified
    EXPECT_THROW(make_mysql_cdc_source(o), std::runtime_error);
}

TEST(MysqlSnapshot, SnapshotAcceptsQualifiedTableName) {
    MysqlCdcOptions o;
    o.server_id = 1;
    o.enable_initial_snapshot = true;
    o.tables = {"shop.orders"};
    EXPECT_NE(make_mysql_cdc_source(o), nullptr);  // constructs; does not connect
}

TEST(MysqlSnapshot, BareTableNameAllowedWithoutSnapshot) {
    MysqlCdcOptions o;
    o.server_id = 1;
    o.tables = {"orders"};  // bare is fine for the stream-only path
    EXPECT_NE(make_mysql_cdc_source(o), nullptr);
}

#endif  // CLINK_HAS_MYSQL
