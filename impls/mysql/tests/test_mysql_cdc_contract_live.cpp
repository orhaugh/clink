// The MySQL CDC source's contract against a live server. SKIPPED unless
// CLINK_MYSQL_TEST_DSN names a MySQL server writing binlog_format=ROW and
// binlog_row_image=FULL; scripts/mysql-cdc-live.sh runs it against pinned
// MySQL 8.0 and 8.4 servers and fails when any case is skipped.
//
// Each case reads its expectation from the server independently (a query of
// the table, the binlog's own coordinates), not from what the source emitted.
// Cases that change a global setting restore it; the suite runs its cases one
// after another against a server of its own.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <map>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"

#ifdef CLINK_HAS_MYSQL
#include "clink/mysql/mysql_cdc_rules.hpp"
#include "clink/mysql/mysql_cdc_source.hpp"
#include "clink/mysql/mysql_client.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/state_backend.hpp"

using clink::CheckpointId;
using clink::Emitter;
using clink::InMemoryStateBackend;
using clink::OperatorId;
using clink::Source;
using clink::StateBackend;
using clink::StreamElement;
using clink::mysql::Connection;
using clink::mysql::ConnectOptions;
using clink::mysql::make_mysql_cdc_source;
using clink::mysql::MysqlCdcOptions;
using clink::mysql::MysqlDecodeErrorPolicy;
using clink::mysql::Result;

namespace {

bool configured() {
    return std::getenv("CLINK_MYSQL_TEST_DSN") != nullptr;
}

#define REQUIRE_MYSQL()                                                             \
    do {                                                                            \
        if (!configured()) {                                                        \
            GTEST_SKIP() << "set CLINK_MYSQL_TEST_DSN against a ROW-binlog server"; \
        }                                                                           \
    } while (0)

ConnectOptions dsn() {
    ConnectOptions o;
    std::istringstream iss(std::getenv("CLINK_MYSQL_TEST_DSN"));
    std::string tok;
    while (iss >> tok) {
        const auto eq = tok.find('=');
        if (eq == std::string::npos) {
            continue;
        }
        const std::string k = tok.substr(0, eq);
        const std::string v = tok.substr(eq + 1);
        if (k == "host") {
            o.host = v;
        } else if (k == "port") {
            o.port = static_cast<std::uint16_t>(std::stoi(v));
        } else if (k == "user") {
            o.user = v;
        } else if (k == "password") {
            o.password = v;
        } else if (k == "database" || k == "dbname") {
            o.database = v;
        }
    }
    return o;
}

// A connection for the test's own statements and its independent reads.
struct Db {
    Db() : c(dsn()) {}
    void exec(const std::string& sql) { c.exec(sql); }
    std::string scalar(const std::string& sql) {
        Result r = c.query(sql);
        MYSQL_ROW row = r.fetch_row();
        return row != nullptr && row[0] != nullptr ? std::string{row[0]} : std::string{};
    }
    // id -> val for a table (id INT PRIMARY KEY, val ...).
    std::map<std::string, std::string> table(const std::string& name) {
        std::map<std::string, std::string> out;
        Result r = c.query("SELECT id, val FROM `" + name + "`");
        while (MYSQL_ROW row = r.fetch_row()) {
            out[row[0]] = row[1] != nullptr ? row[1] : "<null>";
        }
        return out;
    }
    std::pair<std::string, std::uint64_t> head() {
        std::optional<Result> r;
        try {
            r.emplace(c.query("SHOW BINARY LOG STATUS"));
        } catch (const std::runtime_error&) {
            r.emplace(c.query("SHOW MASTER STATUS"));
        }
        MYSQL_ROW row = r->fetch_row();
        return {row[0], std::strtoull(row[1], nullptr, 10)};
    }
    Connection c;
};

// Sets a global variable for the case and puts it back afterwards.
class GlobalSetting {
public:
    GlobalSetting(std::string name, const std::string& value) : name_(std::move(name)) {
        Db db;
        old_ = db.scalar("SELECT @@global." + name_);
        db.exec("SET GLOBAL " + name_ + " = '" + value + "'");
    }
    ~GlobalSetting() {
        try {
            Db db;
            db.exec("SET GLOBAL " + name_ + " = '" + old_ + "'");
        } catch (...) {
        }
    }
    GlobalSetting(const GlobalSetting&) = delete;
    GlobalSetting& operator=(const GlobalSetting&) = delete;

private:
    std::string name_;
    std::string old_;
};

std::string unique(const char* stem) {
    static int n = 0;
    return std::string{stem} + "_" + std::to_string(static_cast<long>(::getpid())) + "_" +
           std::to_string(n++);
}

std::uint32_t next_server_id() {
    static std::uint32_t id = 7000;
    return ++id;
}

MysqlCdcOptions opts_for(const std::vector<std::string>& tables,
                         MysqlDecodeErrorPolicy policy = MysqlDecodeErrorPolicy::Fail) {
    MysqlCdcOptions o;
    o.conn = dsn();
    o.server_id = next_server_id();
    for (const auto& t : tables) {
        o.tables.push_back(o.conn.database + "." + t);
    }
    o.heartbeat_ms = 200;
    o.decode_error_policy = policy;
    return o;
}

struct Rows {
    std::vector<clink::config::JsonObject> all;
    [[nodiscard]] bool has(const std::string& key, const std::string& value) const {
        for (const auto& o : all) {
            if (const auto it = o.find(key);
                it != o.end() && it->second.is_string() && it->second.as_string() == value) {
                return true;
            }
        }
        return false;
    }
};

std::string str(const clink::config::JsonObject& o, const std::string& k) {
    const auto it = o.find(k);
    if (it == o.end()) {
        return "<absent>";
    }
    return it->second.is_string() ? it->second.as_string() : "<not a string>";
}

Emitter<std::string> into(Rows& rows) {
    return Emitter<std::string>{[&rows](StreamElement<std::string> e) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                auto j = clink::config::parse(r.value());
                if (j.is_object()) {
                    rows.all.push_back(j.as_object());
                }
            }
        }
        return true;
    }};
}

// Calls produce until `done` holds or the time runs out.
bool pump(Source<std::string>& s,
          Rows& rows,
          const std::function<bool()>& done,
          std::chrono::milliseconds limit = std::chrono::milliseconds{20000}) {
    const auto deadline = std::chrono::steady_clock::now() + limit;
    while (!done() && std::chrono::steady_clock::now() < deadline) {
        auto em = into(rows);
        s.produce(em);
    }
    return done();
}

// Calls produce until it throws; returns the message, or "" if it never did.
std::string pump_until_throw(Source<std::string>& s,
                             Rows& rows,
                             std::chrono::milliseconds limit = std::chrono::milliseconds{20000}) {
    const auto deadline = std::chrono::steady_clock::now() + limit;
    try {
        while (std::chrono::steady_clock::now() < deadline) {
            auto em = into(rows);
            s.produce(em);
        }
    } catch (const std::exception& e) {
        return e.what();
    }
    return "";
}

std::string open_error(Source<std::string>& s) {
    try {
        s.open();
    } catch (const std::exception& e) {
        return e.what();
    }
    return "";
}

void create_kv(Db& db, const std::string& t, const char* val_type = "VARCHAR(64)") {
    db.exec("DROP TABLE IF EXISTS `" + t + "`");
    db.exec("CREATE TABLE `" + t + "` (id INT PRIMARY KEY, val " + std::string{val_type} +
            ") ENGINE=InnoDB");
}

constexpr OperatorId kOp{41};
const std::string kPosKey = "__mysql_cdc_pos__";

void put_checkpoint(StateBackend& backend, const std::string& bytes) {
    backend.put_operator_state(kOp, kPosKey, StateBackend::ValueView{bytes.data(), bytes.size()});
}

// A checkpoint as the runner takes one: at a barrier the source lets pass.
void checkpoint(Source<std::string>& s, Rows& rows, StateBackend& backend) {
    ASSERT_TRUE(pump(s, rows, [&] { return s.barrier_ready(); }));
    s.snapshot_offset(backend, kOp, CheckpointId{1});
}

std::string server_uuid() {
    Db db;
    return db.scalar("SELECT @@server_uuid");
}

}  // namespace

// The SIGNEDNESS bitmap gives a bit to each numeric column. If YEAR took one,
// every unsigned column after it would be read with the wrong bit.
TEST(MysqlCdcContractLive, AnUnsignedColumnAfterAYearDecodesUnderBothMetadataModes) {
    REQUIRE_MYSQL();
    for (const char* mode : {"MINIMAL", "FULL"}) {
        GlobalSetting meta("binlog_row_metadata", mode);
        Db db;
        const auto t = unique("cdc_year");
        db.exec("DROP TABLE IF EXISTS `" + t + "`");
        db.exec("CREATE TABLE `" + t +
                "` (id INT PRIMARY KEY, y YEAR, bt BIT(8), u INT UNSIGNED, s TINYINT, "
                "d DECIMAL(10,2) UNSIGNED, f DOUBLE, m MEDIUMINT UNSIGNED, sm SMALLINT, "
                "b BIGINT UNSIGNED)");
        auto src = make_mysql_cdc_source(opts_for({t}));
        src->open();
        db.exec("INSERT INTO `" + t +
                "` VALUES (1, 2026, b'00000101', 4294967295, -5, 12.50, -1.5, 16777215, -300, "
                "18446744073709551615)");
        Rows rows;
        ASSERT_TRUE(pump(*src, rows, [&] { return !rows.all.empty(); })) << mode;
        EXPECT_EQ(str(rows.all[0], "y"), "2026") << mode;
        EXPECT_EQ(str(rows.all[0], "u"), "4294967295") << mode;
        EXPECT_EQ(str(rows.all[0], "s"), "-5") << mode;
        EXPECT_EQ(str(rows.all[0], "d"), "12.50") << mode;
        EXPECT_EQ(str(rows.all[0], "m"), "16777215") << mode;
        EXPECT_EQ(str(rows.all[0], "sm"), "-300") << mode;
        EXPECT_EQ(str(rows.all[0], "b"), "18446744073709551615") << mode;
        src->close();
    }
}

// A CHAR column longer than 255 bytes keeps part of its length in the type
// byte of its metadata.
TEST(MysqlCdcContractLive, LongCharColumnsDecode) {
    REQUIRE_MYSQL();
    Db db;
    const auto t = unique("cdc_char");
    db.exec("DROP TABLE IF EXISTS `" + t + "`");
    db.exec("CREATE TABLE `" + t +
            "` (id INT PRIMARY KEY, c CHAR(100) CHARACTER SET utf8mb4, e ENUM('x','y')) ");
    auto src = make_mysql_cdc_source(opts_for({t}));
    src->open();
    const std::string value(90, 'z');
    db.exec("INSERT INTO `" + t + "` VALUES (1, '" + value + "', 'y')");
    Rows rows;
    ASSERT_TRUE(pump(*src, rows, [&] { return !rows.all.empty(); }));
    EXPECT_EQ(str(rows.all[0], "c"), value);
    EXPECT_EQ(str(rows.all[0], "e"), "y");
}

// A transaction larger than one produce() call: the source holds barriers
// until its commit, so a checkpoint never splits it.
TEST(MysqlCdcContractLive, ABarrierWaitsForTheEndOfALargeTransaction) {
    REQUIRE_MYSQL();
    Db db;
    const auto t = unique("cdc_big");
    create_kv(db, t);
    auto src = make_mysql_cdc_source(opts_for({t}));
    src->open();
    EXPECT_TRUE(src->barrier_ready());
    constexpr int kRows = 6000;
    db.exec("BEGIN");
    for (int i = 0; i < kRows; i += 500) {
        std::string sql = "INSERT INTO `" + t + "` VALUES ";
        for (int k = i; k < i + 500; ++k) {
            sql += (k == i ? "(" : ",(") + std::to_string(k) + ",'v" + std::to_string(k) + "')";
        }
        db.exec(sql);
    }
    db.exec("COMMIT");

    Rows rows;
    bool held_inside = false;
    ASSERT_TRUE(pump(*src, rows, [&] {
        if (!rows.all.empty() && rows.all.size() < static_cast<std::size_t>(kRows) &&
            !src->barrier_ready()) {
            held_inside = true;  // inside the transaction, after part of it
        }
        return rows.all.size() >= static_cast<std::size_t>(kRows);
    }));
    EXPECT_TRUE(held_inside) << "the transaction never spanned a produce() call";
    // Its commit is past: the source is between transactions again.
    ASSERT_TRUE(pump(*src, rows, [&] { return src->barrier_ready(); }));
    EXPECT_EQ(rows.all.size(), static_cast<std::size_t>(kRows));

    // A checkpoint here covers the whole transaction: a restore replays none of it.
    InMemoryStateBackend backend;
    checkpoint(*src, rows, backend);
    src->close();
    db.exec("INSERT INTO `" + t + "` VALUES (100000, 'after')");
    auto restored = make_mysql_cdc_source(opts_for({t}));
    ASSERT_TRUE(restored->restore_offset(backend, kOp));
    restored->open();
    Rows again;
    ASSERT_TRUE(pump(*restored, again, [&] { return again.has("id", "100000"); }));
    EXPECT_EQ(again.all.size(), 1U) << "a checkpoint after the commit replayed rows of it";
}

// A checkpoint taken inside a transaction (which the runner never takes, since
// the source holds barriers there) records the boundary before it, so a
// restore replays the whole transaction: duplicates, never a loss.
TEST(MysqlCdcContractLive, ACheckpointInsideATransactionReplaysAllOfIt) {
    REQUIRE_MYSQL();
    Db db;
    const auto t = unique("cdc_mid");
    create_kv(db, t);
    auto src = make_mysql_cdc_source(opts_for({t}));
    src->open();
    db.exec("BEGIN");
    for (int i = 0; i < 3000; i += 500) {
        std::string sql = "INSERT INTO `" + t + "` VALUES ";
        for (int k = i; k < i + 500; ++k) {
            sql += (k == i ? "(" : ",(") + std::to_string(k) + ",'v')";
        }
        db.exec(sql);
    }
    db.exec("COMMIT");
    Rows rows;
    ASSERT_TRUE(pump(*src, rows, [&] { return !rows.all.empty(); }));
    ASSERT_FALSE(src->barrier_ready()) << "the first produce() call ended the transaction";
    InMemoryStateBackend backend;
    src->snapshot_offset(backend, kOp, CheckpointId{1});
    src->close();

    auto restored = make_mysql_cdc_source(opts_for({t}));
    ASSERT_TRUE(restored->restore_offset(backend, kOp));
    restored->open();
    Rows again;
    ASSERT_TRUE(pump(*restored, again, [&] { return again.all.size() >= 3000; }));
    EXPECT_EQ(again.all.size(), 3000U);
    EXPECT_TRUE(again.has("id", "0"));
    EXPECT_TRUE(again.has("id", "2999"));
}

// A checkpoint before a binlog rotation restores across it with nothing lost
// or repeated.
TEST(MysqlCdcContractLive, ARotationKeepsTheStreamWhole) {
    REQUIRE_MYSQL();
    Db db;
    const auto t = unique("cdc_rot");
    create_kv(db, t);
    auto src = make_mysql_cdc_source(opts_for({t}));
    src->open();
    db.exec("INSERT INTO `" + t + "` VALUES (1, 'a')");
    Rows rows;
    ASSERT_TRUE(pump(*src, rows, [&] { return rows.has("id", "1"); }));
    InMemoryStateBackend backend;
    checkpoint(*src, rows, backend);
    const auto before = db.head().first;
    db.exec("FLUSH BINARY LOGS");
    db.exec("INSERT INTO `" + t + "` VALUES (2, 'b')");
    ASSERT_NE(db.head().first, before);
    ASSERT_TRUE(pump(*src, rows, [&] { return rows.has("id", "2"); }));
    src->close();

    auto restored = make_mysql_cdc_source(opts_for({t}));
    ASSERT_TRUE(restored->restore_offset(backend, kOp));
    restored->open();
    db.exec("INSERT INTO `" + t + "` VALUES (3, 'c')");
    Rows again;
    ASSERT_TRUE(pump(*restored, again, [&] { return again.has("id", "3"); }));
    ASSERT_EQ(again.all.size(), 2U);
    EXPECT_EQ(str(again.all[0], "id"), "2");
    EXPECT_EQ(str(again.all[1], "id"), "3");
}

TEST(MysqlCdcContractLive, APurgedBinlogFailsTheRestore) {
    REQUIRE_MYSQL();
    Db db;
    const auto t = unique("cdc_purge");
    create_kv(db, t);
    auto src = make_mysql_cdc_source(opts_for({t}));
    src->open();
    InMemoryStateBackend backend;
    Rows rows;
    checkpoint(*src, rows, backend);
    src->close();
    db.exec("FLUSH BINARY LOGS");
    db.exec("FLUSH BINARY LOGS");
    db.exec("PURGE BINARY LOGS TO '" + db.head().first + "'");

    auto restored = make_mysql_cdc_source(opts_for({t}));
    ASSERT_TRUE(restored->restore_offset(backend, kOp));
    const auto err = open_error(*restored);
    EXPECT_NE(err.find("no longer has"), std::string::npos) << err;
}

TEST(MysqlCdcContractLive, APositionPastTheFileFailsTheRestore) {
    REQUIRE_MYSQL();
    Db db;
    const auto [file, pos] = db.head();
    clink::mysql::MysqlCdcCheckpoint cp;
    cp.file = file;
    cp.pos = pos + 100000;
    cp.server_uuid = server_uuid();
    InMemoryStateBackend backend;
    put_checkpoint(backend, clink::mysql::encode_mysql_cdc_checkpoint(cp));
    auto src = make_mysql_cdc_source(opts_for({"anything"}));
    ASSERT_TRUE(src->restore_offset(backend, kOp));
    const auto err = open_error(*src);
    EXPECT_NE(err.find("which holds"), std::string::npos) << err;
}

TEST(MysqlCdcContractLive, ACorruptCheckpointFailsTheRestore) {
    REQUIRE_MYSQL();
    InMemoryStateBackend backend;
    put_checkpoint(backend, std::string("MYCDC\x01\x00garbage", 14));
    auto src = make_mysql_cdc_source(opts_for({"anything"}));
    try {
        (void)src->restore_offset(backend, kOp);
        ADD_FAILURE() << "a corrupt checkpoint restored";
    } catch (const std::exception& e) {
        EXPECT_NE(std::string{e.what()}.find("corrupt"), std::string::npos) << e.what();
        EXPECT_NE(std::string{e.what()}.find("never starts from the binlog head"),
                  std::string::npos)
            << e.what();
    }
}

TEST(MysqlCdcContractLive, AnotherServersCheckpointFailsTheRestore) {
    REQUIRE_MYSQL();
    Db db;
    const auto [file, pos] = db.head();
    clink::mysql::MysqlCdcCheckpoint cp;
    cp.file = file;
    cp.pos = pos;
    cp.server_uuid = "00000000-0000-0000-0000-000000000001";
    InMemoryStateBackend backend;
    put_checkpoint(backend, clink::mysql::encode_mysql_cdc_checkpoint(cp));
    auto src = make_mysql_cdc_source(opts_for({"anything"}));
    ASSERT_TRUE(src->restore_offset(backend, kOp));
    const auto err = open_error(*src);
    EXPECT_NE(err.find("belongs to server 00000000-0000-0000-0000-000000000001"), std::string::npos)
        << err;
}

TEST(MysqlCdcContractLive, ACheckpointFromTheSnapshotFailsTheRestore) {
    REQUIRE_MYSQL();
    Db db;
    const auto [file, pos] = db.head();
    clink::mysql::MysqlCdcCheckpoint cp;
    cp.file = file;
    cp.pos = pos;
    cp.server_uuid = server_uuid();
    cp.snapshot_unfinished = true;
    InMemoryStateBackend backend;
    put_checkpoint(backend, clink::mysql::encode_mysql_cdc_checkpoint(cp));
    auto src = make_mysql_cdc_source(opts_for({"anything"}));
    ASSERT_TRUE(src->restore_offset(backend, kOp));
    const auto err = open_error(*src);
    EXPECT_NE(err.find("during the initial snapshot"), std::string::npos) << err;
}

// A checkpoint the earlier release wrote (the coordinate alone) still restores.
TEST(MysqlCdcContractLive, TheEarlierReleasesCheckpointRestores) {
    REQUIRE_MYSQL();
    Db db;
    const auto t = unique("cdc_v0");
    create_kv(db, t);
    const auto [file, pos] = db.head();
    std::string row;
    const auto n = static_cast<std::uint32_t>(file.size());
    for (int i = 0; i < 4; ++i) {
        row.push_back(static_cast<char>((n >> (8 * i)) & 0xFF));
    }
    row += file;
    for (int i = 0; i < 8; ++i) {
        row.push_back(static_cast<char>((pos >> (8 * i)) & 0xFF));
    }
    InMemoryStateBackend backend;
    put_checkpoint(backend, row);
    db.exec("INSERT INTO `" + t + "` VALUES (1, 'after the old checkpoint')");
    auto src = make_mysql_cdc_source(opts_for({t}));
    ASSERT_TRUE(src->restore_offset(backend, kOp));
    src->open();
    Rows rows;
    ASSERT_TRUE(pump(*src, rows, [&] { return rows.has("id", "1"); }));
}

TEST(MysqlCdcContractLive, AStatementLoggedChangeFailsTheSourceOrIsDropped) {
    REQUIRE_MYSQL();
    Db db;
    const auto t = unique("cdc_stmt");
    create_kv(db, t);
    for (const auto policy : {MysqlDecodeErrorPolicy::Fail, MysqlDecodeErrorPolicy::Drop}) {
        auto src = make_mysql_cdc_source(opts_for({t}, policy));
        src->open();
        Db writer;
        writer.exec("SET SESSION binlog_format = 'STATEMENT'");
        writer.exec("INSERT INTO `" + t + "` VALUES (" +
                    std::to_string(policy == MysqlDecodeErrorPolicy::Fail ? 1 : 2) + ", 's')");
        writer.exec("SET SESSION binlog_format = 'ROW'");
        writer.exec("INSERT INTO `" + t + "` VALUES (" +
                    std::to_string(policy == MysqlDecodeErrorPolicy::Fail ? 11 : 12) + ", 'r')");
        Rows rows;
        if (policy == MysqlDecodeErrorPolicy::Fail) {
            const auto err = pump_until_throw(*src, rows);
            EXPECT_NE(err.find("statement-logged"), std::string::npos) << err;
            EXPECT_TRUE(rows.all.empty());
        } else {
            ASSERT_TRUE(pump(*src, rows, [&] { return rows.has("id", "12"); }));
            EXPECT_FALSE(rows.has("id", "2"));
        }
        src->close();
    }
}

TEST(MysqlCdcContractLive, APartialRowImageFailsTheSource) {
    REQUIRE_MYSQL();
    Db db;
    const auto t = unique("cdc_image");
    create_kv(db, t);
    db.exec("INSERT INTO `" + t + "` VALUES (1, 'a')");
    auto src = make_mysql_cdc_source(opts_for({t}));
    src->open();
    Db writer;
    writer.exec("SET SESSION binlog_row_image = 'MINIMAL'");
    writer.exec("UPDATE `" + t + "` SET val = 'b' WHERE id = 1");
    Rows rows;
    const auto err = pump_until_throw(*src, rows);
    EXPECT_NE(err.find("partial row image"), std::string::npos) << err;
}

TEST(MysqlCdcContractLive, AServerWithoutRowLoggingIsRefused) {
    REQUIRE_MYSQL();
    {
        GlobalSetting image("binlog_row_image", "MINIMAL");
        auto src = make_mysql_cdc_source(opts_for({"anything"}));
        const auto err = open_error(*src);
        EXPECT_NE(err.find("binlog_row_image=MINIMAL"), std::string::npos) << err;
    }
    {
        GlobalSetting format("binlog_format", "MIXED");
        auto src = make_mysql_cdc_source(opts_for({"anything"}));
        const auto err = open_error(*src);
        EXPECT_NE(err.find("binlog_format=MIXED"), std::string::npos) << err;
    }
}

// The client library refuses a compressed transaction payload (an event type
// it does not know, which replicas may not ignore), so the source stops under
// either policy, saying why.
TEST(MysqlCdcContractLive, ACompressedTransactionFailsTheSourceUnderEitherPolicy) {
    REQUIRE_MYSQL();
    Db db;
    const auto t = unique("cdc_zstd");
    create_kv(db, t);
    int id = 0;
    for (const auto policy : {MysqlDecodeErrorPolicy::Fail, MysqlDecodeErrorPolicy::Drop}) {
        auto src = make_mysql_cdc_source(opts_for({t}, policy));
        src->open();
        Db writer;
        writer.exec("INSERT INTO `" + t + "` VALUES (" + std::to_string(++id) + ", 'before')");
        writer.exec("SET SESSION binlog_transaction_compression = ON");
        writer.exec("INSERT INTO `" + t + "` VALUES (" + std::to_string(++id) + ", 'z')");
        Rows rows;
        const auto err = pump_until_throw(*src, rows);
        EXPECT_NE(err.find("binlog_transaction_compression"), std::string::npos) << err;
        EXPECT_NE(err.find("whatever on_decode_error says"), std::string::npos) << err;
        // The change before it was passed on before the source stopped.
        ASSERT_EQ(rows.all.size(), 1U);
        EXPECT_EQ(str(rows.all[0], "val"), "before");
        src->close();
    }
}

TEST(MysqlCdcContractLive, APartialJsonUpdateFailsTheSource) {
    REQUIRE_MYSQL();
    Db db;
    const auto t = unique("cdc_pjson");
    create_kv(db, t, "JSON");
    db.exec("INSERT INTO `" + t + "` VALUES (1, '{\"a\": 1, \"b\": \"x\"}')");
    auto src = make_mysql_cdc_source(opts_for({t}));
    src->open();
    Db writer;
    writer.exec("SET SESSION binlog_row_value_options = 'PARTIAL_JSON'");
    writer.exec("UPDATE `" + t + "` SET val = JSON_SET(val, '$.a', 2) WHERE id = 1");
    Rows rows;
    const auto err = pump_until_throw(*src, rows);
    EXPECT_NE(err.find("PARTIAL_JSON"), std::string::npos) << err;
    EXPECT_TRUE(rows.all.empty());
}

TEST(MysqlCdcContractLive, AnXaTransactionOnACapturedTableFailsTheSourceOrIsDropped) {
    REQUIRE_MYSQL();
    Db db;
    const auto t = unique("cdc_xa");
    create_kv(db, t);
    {
        auto src = make_mysql_cdc_source(opts_for({t}));
        src->open();
        Db writer;
        writer.exec("XA START 'clink-xa1'");
        writer.exec("INSERT INTO `" + t + "` VALUES (1, 'xa')");
        writer.exec("XA END 'clink-xa1'");
        writer.exec("XA PREPARE 'clink-xa1'");
        writer.exec("XA COMMIT 'clink-xa1'");
        Rows rows;
        const auto err = pump_until_throw(*src, rows);
        EXPECT_NE(err.find("XA transaction"), std::string::npos) << err;
        EXPECT_TRUE(rows.all.empty());
    }
    {
        // Under drop the transaction's rows are skipped, its group ends at its
        // prepare, and the changes after it arrive.
        auto src = make_mysql_cdc_source(opts_for({t}, MysqlDecodeErrorPolicy::Drop));
        src->open();
        Db writer;
        writer.exec("XA START 'clink-xa2'");
        writer.exec("INSERT INTO `" + t + "` VALUES (2, 'xa')");
        writer.exec("XA END 'clink-xa2'");
        writer.exec("XA PREPARE 'clink-xa2'");
        writer.exec("XA COMMIT 'clink-xa2'");
        writer.exec("INSERT INTO `" + t + "` VALUES (3, 'plain')");
        Rows rows;
        ASSERT_TRUE(pump(*src, rows, [&] { return rows.has("id", "3"); }));
        EXPECT_FALSE(rows.has("id", "2"));
        EXPECT_TRUE(src->barrier_ready());
    }
}

TEST(MysqlCdcContractLive, TruncateFailsTheSource) {
    REQUIRE_MYSQL();
    Db db;
    const auto t = unique("cdc_trunc");
    create_kv(db, t);
    db.exec("INSERT INTO `" + t + "` VALUES (1, 'a')");
    auto src = make_mysql_cdc_source(opts_for({t}));
    src->open();
    db.exec("TRUNCATE TABLE `" + t + "`");
    Rows rows;
    const auto err = pump_until_throw(*src, rows);
    EXPECT_NE(err.find("TRUNCATE"), std::string::npos) << err;
}

// binlog_row_metadata=MINIMAL: an index changes nothing; a same-count rename,
// a swap of two columns of one type and an added column each stop the source
// at the table's next event rather than decode it with today's definition.
TEST(MysqlCdcContractLive, MinimalMetadataStopsAtAColumnChangeButNotAtAnIndex) {
    REQUIRE_MYSQL();
    GlobalSetting meta("binlog_row_metadata", "MINIMAL");
    {
        Db db;
        const auto t = unique("cdc_idx");
        create_kv(db, t);
        auto src = make_mysql_cdc_source(opts_for({t}));
        src->open();
        db.exec("ALTER TABLE `" + t + "` ADD INDEX by_val (val)");
        db.exec("INSERT INTO `" + t + "` VALUES (1, 'after the index')");
        Rows rows;
        ASSERT_TRUE(pump(*src, rows, [&] { return rows.has("val", "after the index"); }));
    }
    for (const char* change : {"RENAME COLUMN val TO renamed",
                               "ADD COLUMN extra INT",
                               "MODIFY COLUMN val VARCHAR(64) FIRST"}) {
        Db db;
        const auto t = unique("cdc_col");
        create_kv(db, t);
        for (const auto policy : {MysqlDecodeErrorPolicy::Fail, MysqlDecodeErrorPolicy::Drop}) {
            auto src = make_mysql_cdc_source(opts_for({t}, policy));
            src->open();
            db.exec("ALTER TABLE `" + t + "` " + change);
            db.exec("INSERT INTO `" + t + "` (id) VALUES (" +
                    std::to_string(policy == MysqlDecodeErrorPolicy::Fail ? 1 : 2) + ")");
            Rows rows;
            const auto err = pump_until_throw(*src, rows);
            EXPECT_NE(err.find("may have changed its columns"), std::string::npos)
                << change << ": " << err;
            EXPECT_TRUE(rows.all.empty()) << change;
            src->close();
            // The next case starts from the table as it is now.
            create_kv(db, t);
        }
    }
}

// binlog_row_metadata=FULL: each event carries its definition, so a restore
// to a checkpoint before a column change decodes the events on both sides of
// it, each with its own.
TEST(MysqlCdcContractLive, FullMetadataDecodesEachEventWithItsOwnDefinitionAcrossARestore) {
    REQUIRE_MYSQL();
    GlobalSetting meta("binlog_row_metadata", "FULL");
    Db db;
    const auto t = unique("cdc_full");
    create_kv(db, t);
    auto src = make_mysql_cdc_source(opts_for({t}));
    src->open();
    InMemoryStateBackend backend;
    Rows rows;
    checkpoint(*src, rows, backend);
    db.exec("INSERT INTO `" + t + "` VALUES (1, 'before')");
    db.exec("ALTER TABLE `" + t + "` RENAME COLUMN val TO note, ADD COLUMN n INT");
    db.exec("INSERT INTO `" + t + "` VALUES (2, 'after', 7)");
    ASSERT_TRUE(pump(*src, rows, [&] { return rows.has("id", "2"); }));
    src->close();

    auto restored = make_mysql_cdc_source(opts_for({t}));
    ASSERT_TRUE(restored->restore_offset(backend, kOp));
    restored->open();
    Rows again;
    ASSERT_TRUE(pump(*restored, again, [&] { return again.has("id", "2"); }));
    ASSERT_EQ(again.all.size(), 2U);
    EXPECT_EQ(str(again.all[0], "val"), "before");
    EXPECT_EQ(str(again.all[0], "note"), "<absent>");
    EXPECT_EQ(str(again.all[1], "note"), "after");
    EXPECT_EQ(str(again.all[1], "n"), "7");
}

// binlog_row_metadata=MINIMAL: the checkpoint carries the definitions in force
// at its coordinate, so a restore decodes the events before a later column
// change with them (not with today's), then stops at the change.
TEST(MysqlCdcContractLive, MinimalMetadataReplaysWithTheCheckpointsDefinitionUpToTheChange) {
    REQUIRE_MYSQL();
    GlobalSetting meta("binlog_row_metadata", "MINIMAL");
    Db db;
    const auto t = unique("cdc_hist");
    create_kv(db, t);
    auto src = make_mysql_cdc_source(opts_for({t}));
    src->open();
    InMemoryStateBackend backend;
    Rows before;
    checkpoint(*src, before, backend);
    src->close();
    db.exec("INSERT INTO `" + t + "` VALUES (1, 'before')");
    db.exec("ALTER TABLE `" + t + "` RENAME COLUMN val TO note");
    db.exec("INSERT INTO `" + t + "` VALUES (2, 'after')");

    auto restored = make_mysql_cdc_source(opts_for({t}));
    ASSERT_TRUE(restored->restore_offset(backend, kOp));
    restored->open();
    Rows rows;
    const auto err = pump_until_throw(*restored, rows);
    ASSERT_EQ(rows.all.size(), 1U) << err;
    EXPECT_EQ(str(rows.all[0], "val"), "before");  // the name it had then
    EXPECT_EQ(str(rows.all[0], "note"), "<absent>");
    EXPECT_NE(err.find("may have changed its columns"), std::string::npos) << err;
}

// The locked snapshot under concurrent writes: the snapshot's rows and the
// changes streamed after them, applied by key, equal the table read afterwards.
TEST(MysqlCdcContractLive, ASnapshotUnderConcurrentWritesConvergesOnTheTable) {
    REQUIRE_MYSQL();
    for (const bool locked : {true, false}) {
        Db db;
        const auto t = unique("cdc_snapw");
        create_kv(db, t);
        for (int i = 0; i < 20000; i += 1000) {
            std::string sql = "INSERT INTO `" + t + "` VALUES ";
            for (int k = i; k < i + 1000; ++k) {
                sql += (k == i ? "(" : ",(") + std::to_string(k) + ",'s" + std::to_string(k) + "')";
            }
            db.exec(sql);
        }
        auto opts = opts_for({t});
        opts.enable_initial_snapshot = true;
        opts.snapshot_lock = locked;
        auto src = make_mysql_cdc_source(opts);
        std::atomic<bool> stop{false};
        std::thread writer([&] {
            Db w;
            int n = 0;
            while (!stop.load()) {
                const int k = (n * 7919) % 20000;
                switch (n % 3) {
                    case 0:
                        w.exec("UPDATE `" + t + "` SET val = 'u" + std::to_string(n) +
                               "' WHERE id = " + std::to_string(k));
                        break;
                    case 1:
                        w.exec("DELETE FROM `" + t + "` WHERE id = " + std::to_string(k));
                        break;
                    default:
                        w.exec("INSERT INTO `" + t + "` VALUES (" + std::to_string(30000 + n) +
                               ", 'i') ON DUPLICATE KEY UPDATE val = 'i'");
                        break;
                }
                ++n;
            }
        });
        src->open();
        Rows rows;
        // Read part of the snapshot while the writer runs.
        ASSERT_TRUE(pump(*src, rows, [&] { return rows.all.size() >= 5000; }));
        EXPECT_FALSE(src->barrier_ready()) << "a barrier could pass inside the snapshot";
        stop = true;
        writer.join();
        db.exec("INSERT INTO `" + t + "` VALUES (99999, 'marker')");
        ASSERT_TRUE(pump(
            *src, rows, [&] { return rows.has("id", "99999"); }, std::chrono::milliseconds{60000}));
        std::map<std::string, std::string> state;
        for (const auto& r : rows.all) {
            const auto op = str(r, "__op");
            if (op == "delete") {
                state.erase(str(r, "id"));
            } else {
                state[str(r, "id")] = str(r, "val");
            }
        }
        const auto table = db.table(t);
        std::string diff;
        for (const auto& [k, v] : table) {
            const auto it = state.find(k);
            if (it == state.end()) {
                diff += " missing " + k + "=" + v + ";";
            } else if (it->second != v) {
                diff += " " + k + " is " + it->second + " not " + v + ";";
            }
        }
        for (const auto& [k, v] : state) {
            if (!table.contains(k)) {
                diff += " extra " + k + "=" + v + ";";
            }
        }
        EXPECT_TRUE(diff.empty()) << (locked ? "locked:" : "unlocked:") << diff.substr(0, 2000);
        src->close();
    }
}

TEST(MysqlCdcContractLive, BarriersAreHeldUntilTheSnapshotFinishes) {
    REQUIRE_MYSQL();
    Db db;
    const auto t = unique("cdc_snapb");
    create_kv(db, t);
    db.exec("INSERT INTO `" + t + "` VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    auto opts = opts_for({t});
    opts.enable_initial_snapshot = true;
    auto src = make_mysql_cdc_source(opts);
    src->open();
    EXPECT_FALSE(src->barrier_ready());
    Rows rows;
    ASSERT_TRUE(pump(*src, rows, [&] { return src->barrier_ready(); }));
    EXPECT_EQ(rows.all.size(), 3U);  // every snapshot row before the first barrier
}

// FLUSH TABLES WITH READ LOCK waits behind a table lock, and blocks the
// server's writers while it waits. The source gives up after
// snapshot_lock_wait_s and closes the connection, so nothing stays blocked.
TEST(MysqlCdcContractLive, TheSnapshotLockWaitIsBoundedAndReleased) {
    REQUIRE_MYSQL();
    Db db;
    const auto t = unique("cdc_lock");
    const auto other = unique("cdc_free");
    create_kv(db, t);
    create_kv(db, other);
    Db holder;
    holder.exec("LOCK TABLES `" + t + "` WRITE");
    auto opts = opts_for({t});
    opts.enable_initial_snapshot = true;
    opts.snapshot_lock_wait_s = 1;
    auto src = make_mysql_cdc_source(opts);
    const auto started = std::chrono::steady_clock::now();
    const auto err = open_error(*src);
    const auto waited = std::chrono::steady_clock::now() - started;
    EXPECT_NE(err.find("snapshot_lock_wait_s=1"), std::string::npos) << err;
    EXPECT_LT(waited, std::chrono::seconds{10});
    holder.exec("UNLOCK TABLES");
    // Nothing of the source's holds a lock: a writer proceeds at once.
    Db writer;
    writer.exec("SET SESSION lock_wait_timeout = 2");
    EXPECT_NO_THROW(writer.exec("INSERT INTO `" + other + "` VALUES (1, 'free')"));
    EXPECT_EQ(db.scalar("SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE STATE LIKE "
                        "'%global read lock%'"),
              "0");
}

// A snapshot scan that fails part-way releases its connection when produce()
// throws, before the source is closed: no read view or lock outlives it.
TEST(MysqlCdcContractLive, AFailedSnapshotReleasesItsConnection) {
    REQUIRE_MYSQL();
    Db db;
    const auto t = unique("cdc_kill");
    create_kv(db, t, "VARCHAR(255)");
    // About 55 MB, doubled on the server: far more than the network path
    // between the server and the source buffers, so the scan is still running
    // on the server when the source has read its first chunk.
    std::string seed = "INSERT INTO `" + t + "` VALUES ";
    for (int k = 0; k < 1024; ++k) {
        seed += (k == 0 ? "(" : ",(") + std::to_string(k) + ",'" + std::string(200, 'p') + "')";
    }
    db.exec(seed);
    for (int n = 1024; n < 262144; n *= 2) {
        db.exec("INSERT INTO `" + t + "` SELECT id + " + std::to_string(n) + ", val FROM `" + t +
                "`");
    }
    auto opts = opts_for({t});
    opts.enable_initial_snapshot = true;
    auto src = make_mysql_cdc_source(opts);
    src->open();
    Rows rows;
    ASSERT_TRUE(pump(*src, rows, [&] { return !rows.all.empty(); }));
    // The snapshot connection is the one reading the table.
    const auto id = db.scalar(
        "SELECT ID FROM information_schema.PROCESSLIST WHERE ID <> CONNECTION_ID() AND "
        "COMMAND = 'Query' AND INFO LIKE 'SELECT % FROM `%`.`" +
        t + "`%' LIMIT 1");
    ASSERT_FALSE(id.empty()) << "no snapshot scan in the process list";
    // KILL QUERY ends the scan and leaves the connection open: only the source
    // closing it removes it from the process list.
    db.exec("KILL QUERY " + id);
    const auto err = pump_until_throw(*src, rows);
    EXPECT_FALSE(err.empty());
    // Released before close(): the process list no longer has it.
    std::string still;
    for (int i = 0; i < 50; ++i) {
        still = db.scalar("SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE ID = " + id);
        if (still == "0") {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
    }
    EXPECT_EQ(still, "0");
}

TEST(MysqlCdcContractLive, SnapshotTablesMustExistAndBeInnoDb) {
    REQUIRE_MYSQL();
    Db db;
    const auto myisam = unique("cdc_myisam");
    db.exec("DROP TABLE IF EXISTS `" + myisam + "`");
    db.exec("CREATE TABLE `" + myisam + "` (id INT PRIMARY KEY, val INT) ENGINE=MyISAM");
    auto opts = opts_for({myisam});
    opts.enable_initial_snapshot = true;
    auto src = make_mysql_cdc_source(opts);
    auto err = open_error(*src);
    EXPECT_NE(err.find("MyISAM"), std::string::npos) << err;

    opts = opts_for({unique("cdc_missing")});
    opts.enable_initial_snapshot = true;
    src = make_mysql_cdc_source(opts);
    err = open_error(*src);
    EXPECT_NE(err.find("does not exist"), std::string::npos) << err;
}

#endif  // CLINK_HAS_MYSQL
