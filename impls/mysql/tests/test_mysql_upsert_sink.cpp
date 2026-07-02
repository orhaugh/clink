// MysqlJsonUpsertSink tests - the changelog-aware upsert sink (mode='upsert').
//
// Construction validation runs anywhere. The LIVE integration tests are SKIPPED
// unless CLINK_MYSQL_TEST_DSN is set. They prove a changelog stream maintains the
// table by primary key: insert/update_after upserts (ON DUPLICATE KEY UPDATE),
// delete/update_before removes by key, netted within a flush, replay-idempotent.

#include <cstdint>
#include <cstdlib>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"

#ifdef CLINK_HAS_MYSQL
#include "clink/mysql/mysql_client.hpp"
#include "clink/mysql/mysql_json_upsert_sink.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::CheckpointId;
using clink::mysql::Connection;
using clink::mysql::ConnectOptions;
using clink::mysql::MysqlJsonUpsertSink;
using clink::mysql::MysqlJsonUpsertSinkOptions;
using clink::mysql::Result;

namespace {

// --- construction validation (no server) --------------------------------

TEST(MysqlUpsertSink, RejectsEmptyKeyColumns) {
    MysqlJsonUpsertSinkOptions o;
    o.table = "t";
    o.columns = {"id", "val"};
    EXPECT_THROW(MysqlJsonUpsertSink{std::move(o)}, std::runtime_error);
}

// --- live integration ---------------------------------------------------

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

std::string uniq() {
    return std::to_string(static_cast<long>(::getpid()));
}

std::string scalar(const std::string& sql) {
    Connection c{parse_dsn()};
    Result r = c.query(sql);
    if (MYSQL_ROW row = r.fetch_row()) {
        return row[0] != nullptr ? std::string(row[0]) : std::string{};
    }
    return {};
}

MysqlJsonUpsertSinkOptions opts_for(const std::string& tbl) {
    MysqlJsonUpsertSinkOptions o;
    o.conn = parse_dsn();
    o.table = tbl;
    o.columns = {"id", "val"};
    o.key_columns = {"id"};
    return o;
}

void apply_changelog(const std::string& tbl, const std::vector<std::string>& rows) {
    MysqlJsonUpsertSink sink(opts_for(tbl));
    sink.open();
    Batch<std::string> b;
    for (const auto& r : rows) {
        b.emplace(std::string(r));
    }
    sink.on_data(b);
    sink.on_barrier(CheckpointBarrier{CheckpointId{1}});
    sink.close();
}

struct LiveTable {
    std::string name;
    explicit LiveTable(std::string n) : name(std::move(n)) {
        Connection c{parse_dsn()};
        c.exec("DROP TABLE IF EXISTS `" + name + "`");
        c.exec("CREATE TABLE `" + name + "` (id INT PRIMARY KEY, val TEXT)");
    }
    ~LiveTable() {
        Connection c{parse_dsn()};
        c.exec("DROP TABLE IF EXISTS `" + name + "`");
    }
};

#define REQUIRE_LIVE_MYSQL()                            \
    do {                                                \
        if (!mysql_configured())                        \
            GTEST_SKIP() << "set CLINK_MYSQL_TEST_DSN"; \
    } while (0)

TEST(MysqlUpsertSinkLive, InsertThenUpdateByKey) {
    REQUIRE_LIVE_MYSQL();
    LiveTable t("my_ups_upd_" + uniq());

    apply_changelog(t.name, {R"({"id":1,"val":"a"})", R"({"id":2,"val":"b"})"});
    EXPECT_EQ(scalar("SELECT COUNT(*) FROM `" + t.name + "`"), "2");

    apply_changelog(
        t.name, {R"({"id":1,"val":"a2","__row_kind":"update_after"})", R"({"id":3,"val":"c"})"});
    EXPECT_EQ(scalar("SELECT COUNT(*) FROM `" + t.name + "`"), "3");
    EXPECT_EQ(scalar("SELECT val FROM `" + t.name + "` WHERE id=1"), "a2");
}

TEST(MysqlUpsertSinkLive, DeleteRemovesByKey) {
    REQUIRE_LIVE_MYSQL();
    LiveTable t("my_ups_del_" + uniq());

    apply_changelog(t.name, {R"({"id":1,"val":"a"})", R"({"id":2,"val":"b"})"});
    apply_changelog(t.name, {R"({"id":1,"__row_kind":"delete"})"});

    EXPECT_EQ(scalar("SELECT COUNT(*) FROM `" + t.name + "`"), "1");
    EXPECT_EQ(scalar("SELECT val FROM `" + t.name + "` WHERE id=2"), "b");
}

TEST(MysqlUpsertSinkLive, NettedWithinOneFlush) {
    REQUIRE_LIVE_MYSQL();
    LiveTable t("my_ups_net_" + uniq());

    apply_changelog(t.name,
                    {R"({"id":1,"val":"first"})",
                     R"({"id":2,"val":"x"})",
                     R"({"id":2,"val":"y","__row_kind":"update_after"})",
                     R"({"id":1,"__row_kind":"delete"})"});

    EXPECT_EQ(scalar("SELECT COUNT(*) FROM `" + t.name + "`"), "1");
    EXPECT_EQ(scalar("SELECT val FROM `" + t.name + "` WHERE id=2"), "y");
}

TEST(MysqlUpsertSinkLive, ReplayIsIdempotent) {
    REQUIRE_LIVE_MYSQL();
    LiveTable t("my_ups_replay_" + uniq());

    const std::vector<std::string> batch = {
        R"({"id":1,"val":"a"})", R"({"id":2,"val":"b"})", R"({"id":1,"__row_kind":"delete"})"};
    apply_changelog(t.name, batch);
    apply_changelog(t.name, batch);

    EXPECT_EQ(scalar("SELECT COUNT(*) FROM `" + t.name + "`"), "1");
    EXPECT_EQ(scalar("SELECT val FROM `" + t.name + "` WHERE id=2"), "b");
}

}  // namespace

#else  // !CLINK_HAS_MYSQL

TEST(MysqlUpsertSink, SkippedWithoutMysql) {
    GTEST_SKIP() << "built without mariadb-connector-c";
}

#endif  // CLINK_HAS_MYSQL
