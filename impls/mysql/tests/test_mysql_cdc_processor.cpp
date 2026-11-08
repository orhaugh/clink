// The MySQL CDC source's event processing without a server: binlog events
// built by hand go through BinlogEventProcessor, which must place transaction
// boundaries, decode rows with the definition in force, refuse what it cannot
// decode under the fail policy, report it under the drop policy, and never
// decode an event with a definition it does not know was in force.

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/mysql/mysql_cdc_processor.hpp"
#include "clink/mysql/mysql_cdc_rules.hpp"

using clink::mysql::BinlogEvent;
using clink::mysql::BinlogEventProcessor;
using clink::mysql::DecodeFault;
using clink::mysql::MysqlCdcDecodeError;
using clink::mysql::MysqlDecodeErrorPolicy;
using clink::mysql::ProcessorOptions;
using clink::mysql::ResolvedColumnSpec;

namespace {

constexpr std::uint64_t kTableId = 7;
constexpr std::uint64_t kOtherId = 8;

// The table d.t (a INT, b VARCHAR(20)) as the binlog describes it.
const std::vector<std::uint8_t> kTypes = {0x03, 0x0f};
const std::vector<std::uint8_t> kMeta = {0x14, 0x00};

std::map<std::string, std::string> defs_ab(bool a_unsigned = false) {
    return {{"d.t",
             clink::mysql::schema_fingerprint({ResolvedColumnSpec{"a", "int", a_unsigned, {}},
                                               ResolvedColumnSpec{"b", "varchar", false, {}}})}};
}

// One row (a, b) of d.t: the null bitmap, a as four bytes, b with its length.
std::vector<std::uint8_t> row(std::uint32_t a, const std::string& b) {
    std::vector<std::uint8_t> out = {0x00};
    for (int i = 0; i < 4; ++i) {
        out.push_back(static_cast<std::uint8_t>((a >> (8 * i)) & 0xFF));
    }
    out.push_back(static_cast<std::uint8_t>(b.size()));
    out.insert(out.end(), b.begin(), b.end());
    return out;
}

std::vector<std::uint8_t> cat(std::vector<std::uint8_t> a, const std::vector<std::uint8_t>& b) {
    a.insert(a.end(), b.begin(), b.end());
    return a;
}

BinlogEvent gtid(std::uint64_t next) {
    BinlogEvent e;
    e.type = 33;
    e.next_pos = next;
    return e;
}

BinlogEvent query(const std::string& sql, std::uint64_t next) {
    BinlogEvent e;
    e.type = 2;
    e.next_pos = next;
    e.query_db = "d";
    e.query = sql;
    return e;
}

BinlogEvent xid(std::uint64_t next) {
    BinlogEvent e;
    e.type = 16;
    e.next_pos = next;
    return e;
}

BinlogEvent table_map(std::uint64_t next,
                      std::vector<std::string> names = {},
                      std::vector<std::uint8_t> signedness = {},
                      std::uint64_t id = kTableId,
                      const std::string& table = "t") {
    BinlogEvent e;
    e.type = 19;
    e.next_pos = next;
    e.table_id = id;
    e.db = "d";
    e.table = table;
    e.column_types = kTypes;
    e.metadata = kMeta;
    e.column_names = std::move(names);
    e.signedness = std::move(signedness);
    return e;
}

BinlogEvent rows(int type,
                 const std::vector<std::uint8_t>& data,
                 std::uint64_t next,
                 std::uint64_t id = kTableId) {
    BinlogEvent e;
    e.type = type;
    e.next_pos = next;
    e.table_id = id;
    e.column_count = 2;
    e.present_columns = {0x03};
    e.row_data = data.data();
    e.row_data_size = data.size();
    return e;
}

struct Harness {
    explicit Harness(MysqlDecodeErrorPolicy policy = MysqlDecodeErrorPolicy::Fail,
                     std::vector<std::string> tables = {"d.t"},
                     const std::map<std::string, std::string>& defs = defs_ab())
        : p(ProcessorOptions{std::move(tables), policy}) {
        p.start("mysql-bin.000001", 4, defs, "no definition was recorded (test)");
    }
    std::vector<std::string> feed(const BinlogEvent& e) {
        std::vector<std::string> out;
        p.process(e, out);
        return out;
    }
    BinlogEventProcessor p;
};

clink::config::JsonObject parsed(const std::string& json) {
    auto v = clink::config::parse(json);
    EXPECT_TRUE(v.is_object()) << json;
    return v.is_object() ? v.as_object() : clink::config::JsonObject{};
}

std::string field(const clink::config::JsonObject& o, const std::string& k) {
    const auto it = o.find(k);
    return it != o.end() && it->second.is_string() ? it->second.as_string() : "<absent>";
}

// The fault the processor threw, or nullopt when it threw no MysqlCdcDecodeError
// with one. Fails the test when nothing was thrown.
template <typename F>
std::optional<DecodeFault> fault_of(F&& f, std::string* what = nullptr) {
    try {
        f();
    } catch (const MysqlCdcDecodeError& e) {
        if (what != nullptr) {
            *what = e.what();
        }
        return e.fault();
    }
    ADD_FAILURE() << "nothing was thrown";
    return std::nullopt;
}

}  // namespace

TEST(MysqlCdcProcessor, ATransactionIsOneGroupAndItsCommitTheBoundary) {
    Harness h;
    const auto data = row(1, "ab");
    h.feed(gtid(100));
    EXPECT_FALSE(h.p.between_transactions());
    h.feed(query("BEGIN", 150));
    h.feed(table_map(200));
    const auto out = h.feed(rows(30, data, 250));
    ASSERT_EQ(out.size(), 1U);
    const auto o = parsed(out[0]);
    EXPECT_EQ(field(o, "__op"), "insert");
    EXPECT_EQ(field(o, "__table"), "d.t");
    EXPECT_EQ(field(o, "a"), "1");
    EXPECT_EQ(field(o, "b"), "ab");
    EXPECT_FALSE(h.p.between_transactions());
    EXPECT_EQ(h.p.committed_pos(), 4U);  // nothing committed yet
    h.feed(xid(300));
    EXPECT_TRUE(h.p.between_transactions());
    EXPECT_EQ(h.p.committed_file(), "mysql-bin.000001");
    EXPECT_EQ(h.p.committed_pos(), 300U);
}

TEST(MysqlCdcProcessor, ARotateMovesTheCommittedCoordinateToTheNextFile) {
    Harness h;
    BinlogEvent r;
    r.type = 4;
    r.rotate_file = "mysql-bin.000002";
    r.rotate_pos = 4;
    h.feed(r);
    EXPECT_EQ(h.p.committed_file(), "mysql-bin.000002");
    EXPECT_EQ(h.p.committed_pos(), 4U);
}

TEST(MysqlCdcProcessor, ARowEventWithoutItsTableMapFailsOrIsReported) {
    const auto data = row(1, "secret-value");
    Harness fail;
    std::string what;
    EXPECT_EQ(fault_of([&] { fail.feed(rows(30, data, 250)); }, &what),
              DecodeFault::MissingTableMap);
    // The message names the fault and the position, never the row.
    EXPECT_NE(what.find("mysql-bin.000001:250"), std::string::npos) << what;
    EXPECT_EQ(what.find("secret"), std::string::npos) << what;
    EXPECT_EQ(what.find("7365637265742d76616c7565"), std::string::npos) << what;

    Harness drop(MysqlDecodeErrorPolicy::Drop);
    EXPECT_TRUE(drop.feed(rows(30, data, 250)).empty());
    const auto dropped = drop.p.take_dropped();
    ASSERT_EQ(dropped.size(), 1U);
    EXPECT_EQ(dropped[0].fault, DecodeFault::MissingTableMap);
    // The dead-letter report carries the payload; the error text does not.
    EXPECT_NE(dropped[0].payload.find("7365637265742d76616c7565"), std::string::npos);
    EXPECT_EQ(dropped[0].why.find("secret"), std::string::npos);
    EXPECT_EQ(dropped[0].location, "mysql-bin.000001:250");
}

TEST(MysqlCdcProcessor, AnEventThatFailsToDecodeEmitsNoneOfItsRows) {
    // Two rows, the second cut short: the first decodes, but the event is
    // refused whole rather than passed on in part.
    auto data = cat(row(1, "ok"), row(2, "truncated"));
    data.resize(data.size() - 3);
    Harness fail;
    fail.feed(table_map(200));
    EXPECT_EQ(fault_of([&] { fail.feed(rows(30, data, 250)); }), DecodeFault::CorruptPayload);

    Harness drop(MysqlDecodeErrorPolicy::Drop);
    drop.feed(table_map(200));
    EXPECT_TRUE(drop.feed(rows(30, data, 250)).empty());
    ASSERT_EQ(drop.p.take_dropped().size(), 1U);
    // The stream goes on after it.
    const auto good = row(3, "next");
    EXPECT_EQ(drop.feed(rows(30, good, 300)).size(), 1U);
}

TEST(MysqlCdcProcessor, APartialRowImageIsRefused) {
    Harness h;
    h.feed(table_map(200));
    const auto data = row(1, "ab");
    auto ev = rows(30, data, 250);
    ev.present_columns = {0x01};  // binlog_row_image=MINIMAL wrote only column a
    EXPECT_EQ(fault_of([&] { h.feed(ev); }), DecodeFault::PartialRowImage);

    // A version 2 UPDATE whose after image lacks a column.
    Harness u;
    u.feed(table_map(200));
    const auto upd = cat(cat({0x01}, row(1, "ab")), row(1, "cd"));
    EXPECT_EQ(fault_of([&] { u.feed(rows(31, upd, 250)); }), DecodeFault::PartialRowImage);
}

TEST(MysqlCdcProcessor, UpdatesOfBothRowsEventVersionsDecodeTheirAfterImage) {
    Harness v2;
    v2.feed(table_map(200));
    const auto upd2 = cat(cat({0x03}, row(1, "ab")), row(1, "cd"));
    auto out = v2.feed(rows(31, upd2, 250));
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(field(parsed(out[0]), "b"), "cd");

    // A version 1 UPDATE: the client library has already taken its after-image
    // bitmap out of the row data.
    Harness v1;
    v1.feed(table_map(200));
    const auto upd1 = cat(row(1, "ab"), row(1, "cd"));
    auto ev = rows(24, upd1, 250);
    ev.update_present_columns = {0x03};
    out = v1.feed(ev);
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(field(parsed(out[0]), "b"), "cd");
    EXPECT_EQ(field(parsed(out[0]), "__op"), "update");
}

TEST(MysqlCdcProcessor, MinimalMetadataDecodesWithTheDefinitionInForce) {
    Harness h;
    h.feed(table_map(200, {}, {0x00}));
    const auto data = row(4000000000U, "x");
    auto out = h.feed(rows(30, data, 250));
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(field(parsed(out[0]), "a"), "-294967296");  // signed INT

    Harness uns(MysqlDecodeErrorPolicy::Fail, {"d.t"}, defs_ab(/*a_unsigned=*/true));
    uns.feed(table_map(200, {}, {0x80}));
    out = uns.feed(rows(30, data, 250));
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(field(parsed(out[0]), "a"), "4000000000");
}

TEST(MysqlCdcProcessor, ATableMapThatDisagreesWithTheDefinitionIsRefused) {
    // The binlog says a is unsigned; the definition in force says signed.
    Harness sign;
    EXPECT_EQ(fault_of([&] { sign.feed(table_map(200, {}, {0x80})); }),
              DecodeFault::SchemaMismatch);

    // The definition has a BIGINT where the binlog writes an INT.
    Harness type(
        MysqlDecodeErrorPolicy::Fail,
        {"d.t"},
        {{"d.t",
          clink::mysql::schema_fingerprint({ResolvedColumnSpec{"a", "bigint", false, {}},
                                            ResolvedColumnSpec{"b", "varchar", false, {}}})}});
    EXPECT_EQ(fault_of([&] { type.feed(table_map(200)); }), DecodeFault::SchemaMismatch);

    // Three columns in force, two in the binlog. Under drop the table's rows are
    // reported, not decoded.
    Harness count(
        MysqlDecodeErrorPolicy::Drop,
        {"d.t"},
        {{"d.t",
          clink::mysql::schema_fingerprint({ResolvedColumnSpec{"a", "int", false, {}},
                                            ResolvedColumnSpec{"b", "varchar", false, {}},
                                            ResolvedColumnSpec{"c", "int", false, {}}})}});
    count.feed(table_map(200));
    const auto data = row(1, "ab");
    EXPECT_TRUE(count.feed(rows(30, data, 250)).empty());
    const auto dropped = count.p.take_dropped();
    ASSERT_EQ(dropped.size(), 2U);  // the TABLE_MAP, then its rows
    EXPECT_EQ(dropped[1].fault, DecodeFault::SchemaMismatch);
}

TEST(MysqlCdcProcessor, MinimalMetadataWithoutAKnownDefinitionFailsUnderEitherPolicy) {
    for (const auto policy : {MysqlDecodeErrorPolicy::Fail, MysqlDecodeErrorPolicy::Drop}) {
        Harness h(policy, {"d.t"}, {});
        std::string what;
        EXPECT_EQ(fault_of([&] { h.feed(table_map(200)); }, &what), std::nullopt);
        EXPECT_NE(what.find("no definition was recorded (test)"), std::string::npos) << what;
        EXPECT_NE(what.find("binlog_row_metadata=FULL"), std::string::npos) << what;
    }
}

TEST(MysqlCdcProcessor, AColumnChangeEndsTheDefinitionAndAnIndexChangeDoesNot) {
    const auto data = row(1, "ab");
    for (const char* sql : {"ALTER TABLE t ADD INDEX i (a)",
                            "CREATE INDEX j ON d.t (b)",
                            "ALTER TABLE t COMMENT='x', ALGORITHM=INPLACE"}) {
        Harness h;
        h.feed(gtid(100));
        h.feed(query(sql, 150));
        EXPECT_TRUE(h.p.between_transactions()) << sql;
        h.feed(table_map(200));
        EXPECT_EQ(h.feed(rows(30, data, 250)).size(), 1U) << sql;
    }
    // A rename of the same type, a swap of two columns, a type change: each keeps
    // the column count, so only the statement tells the source, which refuses to
    // decode the next MINIMAL TABLE_MAP with a definition it no longer knows.
    for (const char* sql : {"ALTER TABLE t RENAME COLUMN b TO c",
                            "ALTER TABLE t MODIFY COLUMN a INT AFTER b",
                            "ALTER TABLE t MODIFY a BIGINT",
                            "ALTER TABLE d.t ADD COLUMN c INT",
                            "RENAME TABLE t TO t_old, t_new TO t"}) {
        Harness h(MysqlDecodeErrorPolicy::Drop);
        h.feed(gtid(100));
        h.feed(query(sql, 150));
        std::string what;
        EXPECT_EQ(fault_of([&] { h.feed(table_map(200)); }, &what), std::nullopt) << sql;
        EXPECT_NE(what.find("mysql-bin.000001:150"), std::string::npos) << what;
    }
}

TEST(MysqlCdcProcessor, FullMetadataDecodesEachEventWithItsOwnDefinition) {
    // After a column change, a TABLE_MAP that names its columns needs no
    // definition: the event carries it.
    Harness h(MysqlDecodeErrorPolicy::Fail, {"d.t"}, {});
    h.feed(query("ALTER TABLE t RENAME COLUMN b TO c", 150));
    h.feed(table_map(200, {"a", "c"}, {0x00}));
    const auto data = row(1, "ab");
    const auto out = h.feed(rows(30, data, 250));
    ASSERT_EQ(out.size(), 1U);
    const auto o = parsed(out[0]);
    EXPECT_EQ(field(o, "c"), "ab");
    EXPECT_EQ(field(o, "b"), "<absent>");
}

TEST(MysqlCdcProcessor, FullMetadataTakesEnumMembersFromTheEvent) {
    Harness h(MysqlDecodeErrorPolicy::Fail, {"d.e"}, {});
    BinlogEvent tm;
    tm.type = 19;
    tm.next_pos = 200;
    tm.table_id = kTableId;
    tm.db = "d";
    tm.table = "e";
    tm.column_types = {0xfe};
    tm.metadata = {0xf7, 0x01};  // ENUM, one byte
    tm.column_names = {"state"};
    tm.enum_values = std::vector<std::vector<std::string>>{{"new", "paid"}};
    h.feed(tm);
    const std::vector<std::uint8_t> data = {0x00, 0x02};
    BinlogEvent r = rows(30, data, 250);
    r.column_count = 1;
    r.present_columns = {0x01};
    const auto out = h.feed(r);
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(field(parsed(out[0]), "state"), "paid");

    // Names without the member lists: the TABLE_MAP is malformed.
    Harness bad(MysqlDecodeErrorPolicy::Fail, {"d.e"}, {});
    tm.enum_values.reset();
    EXPECT_EQ(fault_of([&] { bad.feed(tm); }), DecodeFault::SchemaMismatch);
}

TEST(MysqlCdcProcessor, ChangesNotLoggedAsRowsAreRefused) {
    for (const char* sql : {"TRUNCATE t",
                            "DROP TABLE t",
                            "RENAME TABLE t TO elsewhere",
                            "ALTER TABLE t DROP PARTITION p1",
                            "DROP DATABASE d",
                            "INSERT INTO t VALUES (1, 'x')",
                            "DELETE t, u FROM t JOIN u ON t.a = u.a"}) {
        Harness h;
        EXPECT_EQ(fault_of([&] { h.feed(query(sql, 150)); }), DecodeFault::UnsupportedStatement)
            << sql;
        // The same statements on a table the source does not capture pass, except
        // a multi-table change it cannot name.
        Harness other(MysqlDecodeErrorPolicy::Fail, {"d.captured"}, {});
        if (std::string{sql}.find("JOIN") == std::string::npos &&
            std::string{sql}.find("DATABASE") == std::string::npos) {
            EXPECT_NO_THROW(other.feed(query(sql, 150))) << sql;
        }
    }
}

TEST(MysqlCdcProcessor, ADroppedTableIsRecordedAsUnknownAcrossARestart) {
    Harness h(MysqlDecodeErrorPolicy::Drop);
    h.feed(query("DROP TABLE t", 150));
    ASSERT_EQ(h.p.take_dropped().size(), 1U);
    const auto defs = h.p.definitions();
    ASSERT_EQ(defs.count("d.t"), 1U);
    EXPECT_EQ(defs.at("d.t").front(), clink::mysql::kUnknownDefinition);

    // A processor started from what the checkpoint keeps refuses the table's
    // next MINIMAL TABLE_MAP, naming the drop.
    BinlogEventProcessor restored(ProcessorOptions{{"d.t"}, MysqlDecodeErrorPolicy::Drop});
    restored.start("mysql-bin.000001", 150, defs, "unrecorded");
    std::vector<std::string> out;
    std::string what;
    EXPECT_EQ(fault_of([&] { restored.process(table_map(200), out); }, &what), std::nullopt);
    EXPECT_NE(what.find("dropped at mysql-bin.000001:150"), std::string::npos) << what;
}

TEST(MysqlCdcProcessor, RowsInsideAnXaTransactionAreRefused) {
    const auto data = row(1, "ab");
    Harness h;
    h.feed(gtid(100));
    h.feed(query("XA START X'31',X'',1", 150));
    h.feed(table_map(200));
    EXPECT_EQ(fault_of([&] { h.feed(rows(30, data, 250)); }), DecodeFault::UnsupportedStatement);

    // An XA transaction that changes no captured table passes, and its group
    // ends at XA PREPARE.
    Harness other(MysqlDecodeErrorPolicy::Fail, {"d.t"}, defs_ab());
    other.feed(gtid(100));
    other.feed(query("XA START X'31',X'',1", 150));
    other.feed(table_map(200, {}, {}, kOtherId, "u"));
    EXPECT_TRUE(other.feed(rows(30, data, 250, kOtherId)).empty());
    other.feed(query("XA END X'31',X'',1", 300));
    EXPECT_FALSE(other.p.between_transactions());
    BinlogEvent prepare;
    prepare.type = 38;
    prepare.next_pos = 350;
    EXPECT_NO_THROW(other.feed(prepare));
    EXPECT_TRUE(other.p.between_transactions());
    EXPECT_EQ(other.p.committed_pos(), 350U);
    EXPECT_EQ(other.p.filtered_events(), 1U);

    // XA COMMIT ... ONE PHASE closes its group.
    Harness one(MysqlDecodeErrorPolicy::Fail, {"d.t"}, defs_ab());
    one.feed(gtid(100));
    one.feed(query("XA START X'32',X'',1", 150));
    one.feed(query("XA END X'32',X'',1", 200));
    one.feed(query("XA COMMIT X'32',X'',1 ONE PHASE", 250));
    EXPECT_TRUE(one.p.between_transactions());
}

TEST(MysqlCdcProcessor, UnsupportedEventsFailOrAreReported) {
    BinlogEvent compressed;
    compressed.type = 40;
    compressed.next_pos = 400;
    Harness fail;
    fail.feed(gtid(100));
    std::string what;
    EXPECT_EQ(fault_of([&] { fail.feed(compressed); }, &what), DecodeFault::UnsupportedEvent);
    EXPECT_NE(what.find("binlog_transaction_compression"), std::string::npos) << what;

    Harness drop(MysqlDecodeErrorPolicy::Drop);
    drop.feed(gtid(100));
    drop.feed(compressed);
    EXPECT_EQ(drop.p.take_dropped().size(), 1U);
    EXPECT_TRUE(drop.p.between_transactions());  // the payload held the whole transaction

    BinlogEvent partial_json;
    partial_json.type = 39;
    Harness pj;
    EXPECT_EQ(fault_of([&] { pj.feed(partial_json); }), DecodeFault::UnsupportedEvent);

    BinlogEvent unknown;
    unknown.type = 222;
    Harness u;
    EXPECT_EQ(fault_of([&] { u.feed(unknown); }), DecodeFault::UnsupportedEvent);
}

TEST(MysqlCdcProcessor, AnIncidentFailsUnderEitherPolicy) {
    BinlogEvent incident;
    incident.type = 26;
    Harness h(MysqlDecodeErrorPolicy::Drop);
    EXPECT_EQ(fault_of([&] { h.feed(incident); }), std::nullopt);
}

TEST(MysqlCdcProcessor, RowsOfOtherTablesAreCountedNotDecoded) {
    Harness h;
    h.feed(table_map(200, {}, {}, kOtherId, "u"));
    const auto data = row(1, "ab");
    EXPECT_TRUE(h.feed(rows(30, data, 250, kOtherId)).empty());
    EXPECT_EQ(h.p.filtered_events(), 1U);
    EXPECT_TRUE(h.p.take_dropped().empty());
}
