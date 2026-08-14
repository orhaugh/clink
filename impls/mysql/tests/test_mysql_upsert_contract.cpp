// Live instantiation of the public upsert contract suite against a real
// MySQL: idempotency-key collapse via INSERT ... ON DUPLICATE KEY UPDATE on
// a real primary key. Self-skips without CLINK_MYSQL_TEST_DSN, same
// convention as the other live tests in this directory.

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/mysql/mysql_client.hpp"
#include "clink/mysql/mysql_json_upsert_sink.hpp"
#include "clink/test/upsert_contract.hpp"

namespace {

using clink::mysql::Connection;
using clink::mysql::ConnectOptions;
using clink::mysql::MysqlJsonUpsertSink;
using clink::mysql::MysqlJsonUpsertSinkOptions;
using clink::mysql::Result;
using clink::test::UpsertContractFixture;
using clink::test::UpsertState;

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

std::string row_json(int id, const std::string& val) {
    return "{\"id\":" + std::to_string(id) + ",\"val\":\"" + val + "\"}";
}

UpsertState table_rows(const std::string& table) {
    UpsertState rows;
    Connection c{parse_dsn()};
    Result r = c.query("SELECT id, val FROM `" + table + "`");
    while (MYSQL_ROW row = r.fetch_row()) {
        rows.emplace_back(row[0] != nullptr ? row[0] : "", row[1] != nullptr ? row[1] : "");
    }
    return rows;
}

std::string table_for(const std::filesystem::path& dir) {
    std::string name = "clink_ups_contract_" + dir.filename().string();
    for (auto& ch : name) {
        if (ch >= 'A' && ch <= 'Z') {
            ch = static_cast<char>(ch - 'A' + 'a');
        } else if ((ch < 'a' || ch > 'z') && (ch < '0' || ch > '9')) {
            ch = '_';
        }
    }
    if (name.size() > 60) {
        name.resize(60);
    }
    return name;
}

struct MysqlUpsertContract {
    using Value = std::string;
    static constexpr std::string_view kCapabilityName = "mysql_upsert";

    static bool available() { return mysql_configured(); }

    static UpsertContractFixture<std::string> make(const std::filesystem::path& dir) {
        const auto table = table_for(dir);
        {
            Connection c{parse_dsn()};
            c.exec("DROP TABLE IF EXISTS `" + table + "`");
            c.exec("CREATE TABLE `" + table + "` (id INT PRIMARY KEY, val TEXT)");
        }

        UpsertContractFixture<std::string> fx;
        fx.records = {row_json(1, "a"), row_json(2, "b"), row_json(3, "c")};
        fx.updated_records = {row_json(1, "a2"), row_json(2, "b2"), row_json(3, "c2")};
        fx.expected = {{"1", "a"}, {"2", "b"}, {"3", "c"}};
        fx.expected_updated = {{"1", "a2"}, {"2", "b2"}, {"3", "c2"}};
        fx.fresh = [table] {
            MysqlJsonUpsertSinkOptions o;
            o.conn = parse_dsn();
            o.table = table;
            o.columns = {"id", "val"};
            o.key_columns = {"id"};
            return std::make_shared<MysqlJsonUpsertSink>(o);
        };
        fx.state = [table] { return table_rows(table); };
        return fx;
    }
};

}  // namespace

namespace clink::test {

INSTANTIATE_TYPED_TEST_SUITE_P(MysqlUpsert, UpsertContractSuite, MysqlUpsertContract);

}  // namespace clink::test
