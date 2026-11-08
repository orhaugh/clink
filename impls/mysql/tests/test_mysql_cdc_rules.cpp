// The MySQL CDC source's pure rules, without a server: the checkpoint row and
// its strict reading (including the earlier release's format), the binlog
// event classes, the classes of QUERY-event statements and the tables they
// change, the transaction-boundary tracker, the TABLE_MAP optional-metadata
// readers, the type check, and the table-definition text a checkpoint carries.

#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/mysql/mysql_cdc_rules.hpp"

using clink::mysql::BinlogEventClass;
using clink::mysql::classify_binlog_event;
using clink::mysql::classify_query;
using clink::mysql::decode_mysql_cdc_checkpoint;
using clink::mysql::encode_mysql_cdc_checkpoint;
using clink::mysql::InformationSchemaColumn;
using clink::mysql::MysqlCdcCheckpoint;
using clink::mysql::MysqlCdcCheckpointError;
using clink::mysql::QueryStatement;
using clink::mysql::ResolvedColumnSpec;
using clink::mysql::TransactionTracker;

namespace {

using Kind = QueryStatement::Kind;
using Effect = QueryStatement::Effect;

const std::string kUuid = "3e11fa47-71ca-11e1-9e33-c80aa9429562";

MysqlCdcCheckpoint sample_checkpoint() {
    MysqlCdcCheckpoint cp;
    cp.file = "mysql-bin.000042";
    cp.pos = 123456;
    cp.server_uuid = kUuid;
    cp.schemas["shop.orders"] = clink::mysql::schema_fingerprint(
        {ResolvedColumnSpec{"id", "int", false, {}},
         ResolvedColumnSpec{"state", "enum", false, {"new", "paid"}}});
    cp.schemas["shop.gone"] = "!it was dropped at mysql-bin.000041:99";
    return cp;
}

// The earlier release's row: u32 file length, the file, u64 position.
std::string v0_row(const std::string& file, std::uint64_t pos) {
    std::string b;
    const auto n = static_cast<std::uint32_t>(file.size());
    for (int i = 0; i < 4; ++i) {
        b.push_back(static_cast<char>((n >> (8 * i)) & 0xFF));
    }
    b += file;
    for (int i = 0; i < 8; ++i) {
        b.push_back(static_cast<char>((pos >> (8 * i)) & 0xFF));
    }
    return b;
}

void expect_corrupt(const std::string& bytes, const std::string& fragment) {
    try {
        (void)decode_mysql_cdc_checkpoint(bytes);
        ADD_FAILURE() << "decoded a row that should be corrupt (" << fragment << ")";
    } catch (const MysqlCdcCheckpointError& e) {
        EXPECT_NE(std::string{e.what()}.find(fragment), std::string::npos) << e.what();
    }
}

QueryStatement::Target only_target(const QueryStatement& q) {
    EXPECT_EQ(q.targets.size(), 1U);
    return q.targets.empty() ? QueryStatement::Target{} : q.targets.front();
}

}  // namespace

// ---- the checkpoint row ---------------------------------------------------------------

TEST(MysqlCdcCheckpointRow, RoundTripsEveryField) {
    auto cp = sample_checkpoint();
    const auto back = decode_mysql_cdc_checkpoint(encode_mysql_cdc_checkpoint(cp));
    EXPECT_EQ(back.version, 1);
    EXPECT_EQ(back.file, cp.file);
    EXPECT_EQ(back.pos, cp.pos);
    EXPECT_EQ(back.server_uuid, cp.server_uuid);
    EXPECT_EQ(back.schemas, cp.schemas);
    EXPECT_FALSE(back.snapshot_unfinished);

    cp.snapshot_unfinished = true;
    EXPECT_TRUE(decode_mysql_cdc_checkpoint(encode_mysql_cdc_checkpoint(cp)).snapshot_unfinished);
}

TEST(MysqlCdcCheckpointRow, ReadsTheEarlierReleasesRow) {
    const auto cp = decode_mysql_cdc_checkpoint(v0_row("mysql-bin.000003", 4794));
    EXPECT_EQ(cp.version, 0);
    EXPECT_EQ(cp.file, "mysql-bin.000003");
    EXPECT_EQ(cp.pos, 4794U);
    EXPECT_TRUE(cp.server_uuid.empty());
    EXPECT_TRUE(cp.schemas.empty());
}

TEST(MysqlCdcCheckpointRow, RefusesAMalformedEarlierRow) {
    expect_corrupt(std::string{}, "");
    expect_corrupt(v0_row("mysql-bin.000003", 4794).substr(0, 10), "");
    expect_corrupt(v0_row("mysql-bin.000003", 4794) + "x", "bytes after its position");
    expect_corrupt(v0_row("not a binlog", 4794), "");
    expect_corrupt(v0_row("mysql-bin.000003", 3), "");
    expect_corrupt(v0_row("mysql-bin.000003", std::numeric_limits<std::uint64_t>::max()), "");
}

TEST(MysqlCdcCheckpointRow, RefusesEveryMalformedField) {
    const auto good = encode_mysql_cdc_checkpoint(sample_checkpoint());
    // Every truncation of a good row is refused, never read as a shorter one.
    for (std::size_t n = 0; n < good.size(); ++n) {
        EXPECT_THROW((void)decode_mysql_cdc_checkpoint(good.substr(0, n)), MysqlCdcCheckpointError)
            << "a row cut to " << n << " bytes was read";
    }
    expect_corrupt(good + "x", "after its last field");

    auto bad_version = good;
    bad_version[5] = 9;
    expect_corrupt(bad_version, "version 9");
    auto bad_phase = good;
    bad_phase[6] = 2;
    expect_corrupt(bad_phase, "phase");

    auto cp = sample_checkpoint();
    cp.server_uuid = "not-a-uuid";
    expect_corrupt(encode_mysql_cdc_checkpoint(cp), "uuid");

    cp = sample_checkpoint();
    cp.schemas["shop.broken"] = "3:id3:int";  // a definition cut short
    expect_corrupt(encode_mysql_cdc_checkpoint(cp), "shop.broken");

    cp = sample_checkpoint();
    cp.schemas["nodot"] =
        clink::mysql::schema_fingerprint({ResolvedColumnSpec{"a", "int", false, {}}});
    expect_corrupt(encode_mysql_cdc_checkpoint(cp), "db.table");

    cp = sample_checkpoint();
    cp.schemas["shop.empty"] = "";
    expect_corrupt(encode_mysql_cdc_checkpoint(cp), "shop.empty");

    cp = sample_checkpoint();
    cp.file = "mysql-bin";
    expect_corrupt(encode_mysql_cdc_checkpoint(cp), "");
    cp = sample_checkpoint();
    cp.pos = 2;
    expect_corrupt(encode_mysql_cdc_checkpoint(cp), "");
}

TEST(MysqlCdcCheckpointRow, NeverWritesARowItWouldRefuse) {
    auto cp = sample_checkpoint();
    cp.schemas["shop.huge"] = std::string(clink::mysql::kMaxDefinitionBytes + 1, '!');
    EXPECT_THROW((void)encode_mysql_cdc_checkpoint(cp), MysqlCdcCheckpointError);
}

TEST(MysqlCdcCheckpointRow, BinlogFileNamesAndUuids) {
    EXPECT_TRUE(clink::mysql::valid_binlog_file_name("mysql-bin.000001"));
    EXPECT_TRUE(clink::mysql::valid_binlog_file_name("binlog.1234567"));
    EXPECT_FALSE(clink::mysql::valid_binlog_file_name("mysql-bin.00001"));
    EXPECT_FALSE(clink::mysql::valid_binlog_file_name("mysql-bin"));
    EXPECT_FALSE(clink::mysql::valid_binlog_file_name("../etc/passwd.000001"));
    EXPECT_FALSE(clink::mysql::valid_binlog_file_name(std::string(300, 'a') + ".000001"));
    EXPECT_TRUE(clink::mysql::valid_server_uuid(kUuid));
    EXPECT_FALSE(clink::mysql::valid_server_uuid("3e11fa47-71ca-11e1-9e33-c80aa942956"));
    EXPECT_FALSE(clink::mysql::valid_server_uuid("3e11fa47x71ca-11e1-9e33-c80aa9429562"));
}

// ---- table definitions ----------------------------------------------------------------

TEST(MysqlCdcDefinitions, AFingerprintReadsBackWhateverTheNamesHold) {
    const std::vector<ResolvedColumnSpec> cols = {
        ResolvedColumnSpec{"12:odd]name[", "varchar", false, {}},
        ResolvedColumnSpec{"u", "bigint", true, {}},
        ResolvedColumnSpec{"e", "enum", false, {"a:b", "", "]["}},
    };
    const auto text = clink::mysql::schema_fingerprint(cols);
    const auto back = clink::mysql::parse_schema_fingerprint(text);
    ASSERT_TRUE(back.has_value());
    ASSERT_EQ(back->size(), 3U);
    EXPECT_EQ((*back)[0].name, "12:odd]name[");
    EXPECT_TRUE((*back)[1].is_unsigned);
    EXPECT_EQ((*back)[2].labels, (std::vector<std::string>{"a:b", "", "]["}));

    EXPECT_FALSE(clink::mysql::parse_schema_fingerprint("").has_value());
    EXPECT_FALSE(clink::mysql::parse_schema_fingerprint(text + "x").has_value());
    EXPECT_FALSE(
        clink::mysql::parse_schema_fingerprint(text.substr(0, text.size() - 1)).has_value());
    EXPECT_FALSE(clink::mysql::parse_schema_fingerprint("01:a3:ints0[]").has_value());
}

TEST(MysqlCdcDefinitions, ResolvesSignednessAndMembersFromColumnType) {
    const auto u = clink::mysql::resolve_column({"n", "INT", "int unsigned zerofill"});
    EXPECT_EQ(u.data_type, "int");
    EXPECT_TRUE(u.is_unsigned);
    const auto e = clink::mysql::resolve_column({"e", "enum", "enum('signed','unsigned','it''s')"});
    EXPECT_FALSE(e.is_unsigned);  // a member named unsigned is not signedness
    EXPECT_EQ(e.labels, (std::vector<std::string>{"signed", "unsigned", "it's"}));
}

// ---- binlog events ----------------------------------------------------------------------

TEST(MysqlCdcEvents, EveryChangeCarryingTypeIsDecodedOrRefused) {
    for (const int t : {23, 24, 25, 30, 31, 32}) {
        EXPECT_EQ(classify_binlog_event(t), BinlogEventClass::Rows) << t;
    }
    std::string why;
    EXPECT_EQ(classify_binlog_event(40, &why), BinlogEventClass::Unsupported);
    EXPECT_NE(why.find("binlog_transaction_compression"), std::string::npos);
    EXPECT_EQ(classify_binlog_event(39, &why), BinlogEventClass::Unsupported);
    EXPECT_NE(why.find("PARTIAL_JSON"), std::string::npos);
    EXPECT_EQ(classify_binlog_event(38), BinlogEventClass::Unsupported);
    EXPECT_EQ(classify_binlog_event(20), BinlogEventClass::Unsupported);
    EXPECT_EQ(classify_binlog_event(200, &why), BinlogEventClass::Unsupported);
    EXPECT_NE(why.find("does not know"), std::string::npos);
    EXPECT_EQ(classify_binlog_event(26), BinlogEventClass::Fatal);
    EXPECT_EQ(classify_binlog_event(33), BinlogEventClass::TransactionStart);
    EXPECT_EQ(classify_binlog_event(34), BinlogEventClass::TransactionStart);
    EXPECT_EQ(classify_binlog_event(16), BinlogEventClass::Xid);
    EXPECT_EQ(classify_binlog_event(27), BinlogEventClass::Heartbeat);
    EXPECT_EQ(classify_binlog_event(15), BinlogEventClass::Informational);
    EXPECT_EQ(classify_binlog_event(29), BinlogEventClass::Informational);
    EXPECT_TRUE(clink::mysql::is_v1_update_rows_event(24));
    EXPECT_FALSE(clink::mysql::is_v1_update_rows_event(31));
}

// ---- statements --------------------------------------------------------------------------

TEST(MysqlCdcStatements, TransactionControl) {
    EXPECT_EQ(classify_query("BEGIN", "d").kind, Kind::Begin);
    EXPECT_EQ(classify_query("/* app */ START TRANSACTION", "d").kind, Kind::Begin);
    EXPECT_EQ(classify_query("COMMIT", "d").kind, Kind::Commit);
    EXPECT_EQ(classify_query("ROLLBACK", "d").kind, Kind::Rollback);
    EXPECT_EQ(classify_query("ROLLBACK TO SAVEPOINT s1", "d").kind, Kind::InTransaction);
    EXPECT_EQ(classify_query("SAVEPOINT s1", "d").kind, Kind::InTransaction);
    EXPECT_EQ(classify_query("XA START X'31',X'',1", "d").kind, Kind::XaStart);
    EXPECT_EQ(classify_query("XA END X'31',X'',1", "d").kind, Kind::InTransaction);
    EXPECT_EQ(classify_query("XA COMMIT X'31',X'',1", "d").kind, Kind::InTransaction);
    EXPECT_EQ(classify_query("XA COMMIT X'31',X'',1 ONE PHASE", "d").kind, Kind::Commit);
}

TEST(MysqlCdcStatements, AlterClausesThatLeaveTheColumnsAlone) {
    for (const char* sql : {
             "ALTER TABLE t ADD INDEX i (a)",
             "ALTER TABLE t ADD UNIQUE KEY u (a), ADD CONSTRAINT c CHECK (a > 0)",
             "ALTER TABLE t DROP INDEX i",
             "ALTER TABLE t DROP PRIMARY KEY, ADD PRIMARY KEY (a, b)",
             "ALTER TABLE t RENAME INDEX i TO j",
             "ALTER TABLE t ALTER COLUMN a SET DEFAULT 5",
             "ALTER TABLE t ALTER a DROP DEFAULT",
             "ALTER TABLE t ALTER COLUMN a SET INVISIBLE",
             "ALTER TABLE t ENGINE=InnoDB COMMENT='orders', ALGORITHM=INPLACE, LOCK=NONE",
             "ALTER TABLE t DEFAULT CHARACTER SET utf8mb4",
             "ALTER TABLE t FORCE",
             "ALTER TABLE t PARTITION BY HASH(a) PARTITIONS 4",
             "ALTER TABLE t ADD PARTITION (PARTITION p9 VALUES LESS THAN (900))",
             "CREATE INDEX i ON t (a)",
             "DROP INDEX i ON t",
         }) {
        const auto q = classify_query(sql, "d");
        EXPECT_EQ(q.kind, Kind::Ddl) << sql;
        const auto t = only_target(q);
        EXPECT_EQ(t.effect, Effect::Alter) << sql;
        EXPECT_FALSE(t.columns) << sql;
        EXPECT_EQ(t.db, "d") << sql;
        EXPECT_EQ(t.table, "t") << sql;
    }
}

TEST(MysqlCdcStatements, AlterClausesThatMayChangeTheColumns) {
    for (const char* sql : {
             "ALTER TABLE t ADD COLUMN c INT",
             "ALTER TABLE t ADD c INT AFTER a",
             "ALTER TABLE t ADD (c INT, d INT)",
             "ALTER TABLE t DROP COLUMN c",
             "ALTER TABLE t DROP c",
             "ALTER TABLE t MODIFY a BIGINT",
             "ALTER TABLE t MODIFY COLUMN a INT AFTER b",
             "ALTER TABLE t CHANGE a b INT",
             "ALTER TABLE t RENAME COLUMN a TO b",
             "ALTER TABLE t ALTER COLUMN a SET DATA TYPE BIGINT",
             "ALTER TABLE t CONVERT TO CHARACTER SET utf8mb4",
             "ALTER TABLE t ADD INDEX i (a), ADD COLUMN c INT",
             "ALTER TABLE t SOMETHING_NEW",
         }) {
        const auto q = classify_query(sql, "d");
        const auto t = only_target(q);
        EXPECT_EQ(t.effect, Effect::Alter) << sql;
        EXPECT_TRUE(t.columns) << sql;
    }
}

TEST(MysqlCdcStatements, ChangesWithoutRowEvents) {
    for (const char* sql : {
             "TRUNCATE t",
             "TRUNCATE TABLE d.t",
             "ALTER TABLE t TRUNCATE PARTITION p1",
             "ALTER TABLE t DROP PARTITION p1",
             "ALTER TABLE t DISCARD TABLESPACE",
             "ALTER TABLE t IMPORT TABLESPACE",
         }) {
        const auto q = classify_query(sql, "d");
        ASSERT_FALSE(q.targets.empty()) << sql;
        EXPECT_EQ(q.targets.front().effect, Effect::Truncate) << sql;
        EXPECT_EQ(q.targets.front().table, "t") << sql;
    }
}

TEST(MysqlCdcStatements, AnExchangedPartitionChangesBothTables) {
    const auto q = classify_query("ALTER TABLE t EXCHANGE PARTITION p1 WITH TABLE d2.t2", "d");
    ASSERT_GE(q.targets.size(), 2U);
    EXPECT_EQ(q.targets[0].effect, Effect::Truncate);
    EXPECT_EQ(q.targets[0].table, "t");
    EXPECT_EQ(q.targets[1].effect, Effect::Truncate);
    EXPECT_EQ(q.targets[1].db, "d2");
    EXPECT_EQ(q.targets[1].table, "t2");
}

TEST(MysqlCdcStatements, TablesThatMoveAppearOrGo) {
    auto q = classify_query("ALTER TABLE t RENAME TO other.u", "d");
    ASSERT_EQ(q.targets.size(), 2U);
    EXPECT_EQ(q.targets[0].effect, Effect::Rename);
    EXPECT_EQ(q.targets[0].table, "t");
    EXPECT_EQ(q.targets[1].effect, Effect::Create);
    EXPECT_EQ(q.targets[1].db, "other");
    EXPECT_EQ(q.targets[1].table, "u");

    // The swap an online schema-change tool makes: t keeps its name with another
    // definition, t_old appears.
    q = classify_query("RENAME TABLE t TO t_old, t_new TO t", "d");
    ASSERT_EQ(q.targets.size(), 3U);
    EXPECT_EQ(q.targets[0].effect, Effect::Alter);
    EXPECT_EQ(q.targets[0].table, "t");
    EXPECT_TRUE(q.targets[0].columns);
    EXPECT_EQ(q.targets[1].effect, Effect::Rename);
    EXPECT_EQ(q.targets[1].table, "t_new");
    EXPECT_EQ(q.targets[2].effect, Effect::Create);
    EXPECT_EQ(q.targets[2].table, "t_old");

    q = classify_query("DROP TABLE IF EXISTS a, `odd``name`, d2.b", "d");
    ASSERT_EQ(q.targets.size(), 3U);
    EXPECT_EQ(q.targets[1].table, "odd`name");
    EXPECT_EQ(q.targets[2].db, "d2");
    for (const auto& t : q.targets) {
        EXPECT_EQ(t.effect, Effect::Drop);
    }

    EXPECT_EQ(only_target(classify_query("CREATE TABLE IF NOT EXISTS n (a INT)", "d")).effect,
              Effect::Create);
    EXPECT_TRUE(classify_query("CREATE TEMPORARY TABLE n (a INT)", "d").targets.empty());
    EXPECT_TRUE(classify_query("DROP TEMPORARY TABLE n", "d").targets.empty());
    EXPECT_EQ(only_target(classify_query("DROP DATABASE shop", "d")).effect, Effect::DropDatabase);
}

TEST(MysqlCdcStatements, StatementLoggedChangesAndTheirTables) {
    auto t = only_target(classify_query("INSERT INTO d2.t VALUES (1)", "d"));
    EXPECT_EQ(t.effect, Effect::Write);
    EXPECT_EQ(t.db, "d2");
    EXPECT_EQ(only_target(classify_query("REPLACE t SET a=1", "d")).table, "t");
    EXPECT_EQ(only_target(classify_query("UPDATE LOW_PRIORITY t SET a=1", "d")).table, "t");
    EXPECT_FALSE(classify_query("UPDATE t, u SET t.a=u.a", "d").targets_known);
    EXPECT_FALSE(classify_query("UPDATE t JOIN u ON t.a=u.a SET t.b=1", "d").targets_known);
    EXPECT_EQ(classify_query("DELETE FROM t WHERE a=1", "d").kind, Kind::Dml);
    // A versioned comment is statement text; a plain one is not.
    EXPECT_EQ(classify_query("/*!40000 ALTER TABLE t DISABLE KEYS */", "d").kind, Kind::Ddl);
    EXPECT_EQ(classify_query("-- INSERT INTO t VALUES (1)\nSET @a = 1", "d").kind, Kind::Other);
    EXPECT_EQ(classify_query("GRANT SELECT ON d.* TO 'u'", "d").kind, Kind::Other);
}

// A literal that never closes, even one ending in a backslash, ends the
// statement's text; the lexer never steps past it.
TEST(MysqlCdcStatements, AnUnterminatedLiteralEndsTheText) {
    EXPECT_EQ(classify_query("SELECT 'abc\\", "d").kind, Kind::Other);
    EXPECT_EQ(classify_query("SET @a = 'x", "d").kind, Kind::Other);
    const auto q = classify_query("INSERT INTO t VALUES ('x\\", "d");
    EXPECT_EQ(q.kind, Kind::Dml);
    ASSERT_EQ(q.targets.size(), 1U);
    EXPECT_EQ(q.targets[0].table, "t");
    EXPECT_EQ(classify_query("ALTER TABLE t COMMENT = 'open\\", "d").targets.size(), 1U);
}

// ---- transaction boundaries ---------------------------------------------------------------

TEST(MysqlCdcBoundaries, MysqlGroupsCloseAtXidOrTheirStatement) {
    TransactionTracker t;
    t.on_gtid(false);
    EXPECT_TRUE(t.open());
    t.on_begin();
    t.on_rows();
    EXPECT_FALSE(t.on_statement());  // a savepoint inside BEGIN
    EXPECT_TRUE(t.open());
    t.on_end();
    EXPECT_FALSE(t.open());

    t.on_gtid(false);  // a DDL statement is its own group
    EXPECT_TRUE(t.on_statement());
    EXPECT_FALSE(t.open());
}

// ---- TABLE_MAP optional metadata ---------------------------------------------------------

TEST(MysqlCdcMetadata, ReadsColumnNamesAndMemberLists) {
    const std::vector<std::uint8_t> names = {2, 'i', 'd', 5, 's', 't', 'a', 't', 'e'};
    EXPECT_EQ(clink::mysql::parse_column_names(names.data(), names.size(), 2),
              (std::vector<std::string>{"id", "state"}));
    EXPECT_TRUE(clink::mysql::parse_column_names(names.data(), names.size(), 3).empty());
    EXPECT_TRUE(clink::mysql::parse_column_names(names.data(), names.size() - 1, 2).empty());

    const std::vector<std::uint8_t> lists = {2, 1, 'a', 2, 'b', 'c', 1, 0};
    const auto parsed = clink::mysql::parse_str_value_lists(lists.data(), lists.size());
    ASSERT_TRUE(parsed.has_value());
    ASSERT_EQ(parsed->size(), 2U);
    EXPECT_EQ((*parsed)[0], (std::vector<std::string>{"a", "bc"}));
    EXPECT_EQ((*parsed)[1], (std::vector<std::string>{""}));
    EXPECT_FALSE(clink::mysql::parse_str_value_lists(lists.data(), 4).has_value());
}

TEST(MysqlCdcMetadata, BinlogTypesMatchTheirDataTypesOnly) {
    EXPECT_TRUE(clink::mysql::binlog_type_matches(3, 0, "int"));
    EXPECT_FALSE(clink::mysql::binlog_type_matches(3, 0, "bigint"));
    EXPECT_TRUE(clink::mysql::binlog_type_matches(8, 0, "bigint"));
    EXPECT_TRUE(clink::mysql::binlog_type_matches(15, 0, "varchar"));
    EXPECT_FALSE(clink::mysql::binlog_type_matches(15, 0, "char"));
    EXPECT_TRUE(clink::mysql::binlog_type_matches(254, 247, "enum"));
    EXPECT_TRUE(clink::mysql::binlog_type_matches(254, 248, "set"));
    EXPECT_TRUE(clink::mysql::binlog_type_matches(254, 0xEE, "char"));  // a long CHAR
    EXPECT_FALSE(clink::mysql::binlog_type_matches(254, 247, "char"));
    EXPECT_TRUE(clink::mysql::binlog_type_matches(252, 0, "mediumtext"));
    EXPECT_TRUE(clink::mysql::binlog_type_matches(13, 0, "year"));
    EXPECT_FALSE(clink::mysql::binlog_type_matches(99, 0, "int"));
}
