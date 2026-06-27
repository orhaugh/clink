// MySQL binlog CDC LIVE integration test. SKIPPED unless CLINK_MYSQL_TEST_DSN is
// set AND the server runs binlog_format=ROW + binlog_row_image=FULL with a
// non-zero server-id (docker/integration-services.yml's mysql service sets these).
// Proves against a real MySQL: INSERT/UPDATE/DELETE produce the matching __op
// change events with the right columns, and a checkpoint + restore resumes the
// stream from the saved position (reader 2 sees only the changes made after the
// checkpoint, with no gap and no overlap).

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
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
#include "clink/mysql/mysql_cdc_source.hpp"
#include "clink/mysql/mysql_client.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/state_backend.hpp"

using clink::CheckpointId;
using clink::Emitter;
using clink::InMemoryStateBackend;
using clink::OperatorId;
using clink::Source;
using clink::StreamElement;
using clink::mysql::Connection;
using clink::mysql::ConnectOptions;
using clink::mysql::make_mysql_cdc_source;
using clink::mysql::MysqlCdcOptions;

namespace {

bool mysql_configured() {
    return std::getenv("CLINK_MYSQL_TEST_DSN") != nullptr;
}

ConnectOptions parse_dsn() {
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

std::string unique_table() {
    static int n = 0;
    return "clink_cdc_" + std::to_string(static_cast<long>(::getpid())) + "_" + std::to_string(n++);
}

struct Change {
    std::string op;
    std::string table;
    clink::config::JsonValue row;
};
struct Captured {
    std::vector<Change> changes;
};
Emitter<std::string> capturing(Captured& cap) {
    return Emitter<std::string>{[&cap](StreamElement<std::string> e) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                auto j = clink::config::parse(r.value());
                if (!j.is_object()) {
                    continue;
                }
                const auto& o = j.as_object();
                Change c;
                if (auto it = o.find("__op"); it != o.end() && it->second.is_string()) {
                    c.op = it->second.as_string();
                }
                if (auto it = o.find("__table"); it != o.end() && it->second.is_string()) {
                    c.table = it->second.as_string();
                }
                c.row = j;
                cap.changes.push_back(std::move(c));
            }
        }
        return true;
    }};
}

void drain(Source<std::string>& src, Captured& cap, std::size_t want, int timeout_ms) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{timeout_ms};
    while (cap.changes.size() < want && std::chrono::steady_clock::now() < deadline) {
        auto em = capturing(cap);
        src.produce(em);
    }
}

MysqlCdcOptions cdc_opts(const std::string& tbl, std::uint32_t server_id) {
    MysqlCdcOptions o;
    o.conn = parse_dsn();
    o.server_id = server_id;
    o.tables = {tbl};      // bare table name (the source matches db.table or bare)
    o.heartbeat_ms = 200;  // drain fast in the test
    return o;
}

int id_of(const clink::config::JsonValue& row) {
    if (row.is_object()) {
        const auto& o = row.as_object();
        if (auto it = o.find("id"); it != o.end() && it->second.is_string()) {
            return std::stoi(it->second.as_string());
        }
    }
    return -1;
}

}  // namespace

TEST(MysqlCdcLive, InsertUpdateDeleteProduceOps) {
    if (!mysql_configured()) {
        GTEST_SKIP() << "set CLINK_MYSQL_TEST_DSN against a ROW-binlog master";
    }
    const std::string tbl = unique_table();
    {
        Connection c{parse_dsn()};
        c.exec("DROP TABLE IF EXISTS `" + tbl + "`");
        c.exec("CREATE TABLE `" + tbl + "` (id INT PRIMARY KEY, val VARCHAR(64))");
    }
    auto opts = cdc_opts(tbl, 1001);
    auto source = make_mysql_cdc_source(opts);
    source->open();  // captures from the current head; DML below is what we stream

    {
        Connection c{parse_dsn()};
        c.exec("INSERT INTO `" + tbl + "` VALUES (1, 'a')");
        c.exec("UPDATE `" + tbl + "` SET val='b' WHERE id=1");
        c.exec("DELETE FROM `" + tbl + "` WHERE id=1");
    }

    Captured cap;
    drain(*source, cap, 3, /*timeout_ms=*/20000);
    source->close();
    {
        Connection c{parse_dsn()};
        c.exec("DROP TABLE IF EXISTS `" + tbl + "`");
    }

    ASSERT_EQ(cap.changes.size(), 3u) << "expected insert+update+delete change events";
    EXPECT_EQ(cap.changes[0].op, "insert");
    EXPECT_EQ(cap.changes[1].op, "update");
    EXPECT_EQ(cap.changes[2].op, "delete");
    for (const auto& ch : cap.changes) {
        EXPECT_NE(ch.table.find(tbl), std::string::npos) << "table=" << ch.table;
        EXPECT_EQ(id_of(ch.row), 1);
    }
    EXPECT_EQ(cap.changes[1].row.as_object().at("val").as_string(), "b")
        << "update must carry the AFTER image";
}

TEST(MysqlCdcLive, RichColumnTypesDecodeCorrectly) {
    if (!mysql_configured()) {
        GTEST_SKIP() << "set CLINK_MYSQL_TEST_DSN against a ROW-binlog master";
    }
    const std::string tbl = unique_table();
    {
        Connection c{parse_dsn()};
        c.exec("DROP TABLE IF EXISTS `" + tbl + "`");
        // ENUM/SET (label decoding), JSON (binary->text), and a VIRTUAL generated
        // column (MySQL logs it in the row image, so it must NOT misalign/drop).
        c.exec("CREATE TABLE `" + tbl +
               "` (id INT PRIMARY KEY, status ENUM('active','closed'), doc JSON, "
               "tags SET('x','y','z'), vcol INT AS (id+1) VIRTUAL)");
    }
    auto source = make_mysql_cdc_source(cdc_opts(tbl, 1003));
    source->open();
    {
        Connection c{parse_dsn()};
        c.exec("INSERT INTO `" + tbl +
               "` (id,status,doc,tags) VALUES (1,'closed','{\"a\":1,\"b\":\"hi\"}','x,z')");
    }
    Captured cap;
    drain(*source, cap, 1, /*timeout_ms=*/20000);
    source->close();
    {
        Connection c{parse_dsn()};
        c.exec("DROP TABLE IF EXISTS `" + tbl + "`");
    }

    ASSERT_EQ(cap.changes.size(), 1u) << "virtual column must not cause the row to drop";
    const auto& o = cap.changes[0].row.as_object();
    EXPECT_EQ(o.at("status").as_string(), "closed") << "ENUM must decode to its label";
    EXPECT_EQ(o.at("tags").as_string(), "x,z") << "SET must decode to comma-joined labels";
    EXPECT_EQ(o.at("doc").as_string(), R"({"a":1,"b":"hi"})") << "JSON must decode to text";
    EXPECT_EQ(o.at("vcol").as_string(), "2")
        << "the generated column is logged (id+1) and flows through";
}

TEST(MysqlCdcLive, CheckpointResumesWithoutGap) {
    if (!mysql_configured()) {
        GTEST_SKIP() << "set CLINK_MYSQL_TEST_DSN against a ROW-binlog master";
    }
    const std::string tbl = unique_table();
    {
        Connection c{parse_dsn()};
        c.exec("DROP TABLE IF EXISTS `" + tbl + "`");
        c.exec("CREATE TABLE `" + tbl + "` (id INT PRIMARY KEY, val VARCHAR(32))");
    }

    InMemoryStateBackend backend;
    const OperatorId op{1};

    // Reader 1: capture inserts 1..5, then checkpoint and close.
    auto src1 = make_mysql_cdc_source(cdc_opts(tbl, 1002));
    src1->open();
    {
        Connection c{parse_dsn()};
        for (int i = 1; i <= 5; ++i) {
            c.exec("INSERT INTO `" + tbl + "` VALUES (" + std::to_string(i) + ", 'x')");
        }
    }
    Captured cap1;
    drain(*src1, cap1, 5, /*timeout_ms=*/20000);
    ASSERT_EQ(cap1.changes.size(), 5u);
    src1->snapshot_offset(backend, op, CheckpointId{1});
    src1->close();

    // Changes made while no reader is attached.
    {
        Connection c{parse_dsn()};
        for (int i = 6; i <= 10; ++i) {
            c.exec("INSERT INTO `" + tbl + "` VALUES (" + std::to_string(i) + ", 'y')");
        }
    }

    // Reader 2: restore the checkpoint, then resume. It must see ONLY 6..10. A
    // broken restore would fresh-start at the current head and see nothing.
    auto src2 = make_mysql_cdc_source(cdc_opts(tbl, 1002));
    ASSERT_TRUE(src2->restore_offset(backend, op)) << "checkpoint must restore";
    src2->open();
    Captured cap2;
    drain(*src2, cap2, 5, /*timeout_ms=*/20000);
    src2->close();
    {
        Connection c{parse_dsn()};
        c.exec("DROP TABLE IF EXISTS `" + tbl + "`");
    }

    std::vector<int> ids2;
    for (const auto& ch : cap2.changes) {
        ids2.push_back(id_of(ch.row));
    }
    ASSERT_EQ(cap2.changes.size(), 5u) << "resume must deliver exactly the post-checkpoint changes";
    for (int i = 6; i <= 10; ++i) {
        EXPECT_NE(std::find(ids2.begin(), ids2.end(), i), ids2.end()) << "missing id " << i;
    }
    for (int id : ids2) {
        EXPECT_GE(id, 6) << "reader 2 must not re-see a pre-checkpoint row";
    }
}

#endif  // CLINK_HAS_MYSQL
