// MySQL LIVE integration test. SKIPPED unless CLINK_MYSQL_TEST_DSN is set (e.g.
// "host=localhost port=3306 user=root password=mysql database=test" against
// docker/integration-services.yml). Proves against a real MySQL: a sink->source
// round-trip delivers every row; mode='upsert' is idempotent by primary key; and
// the cursor source does not re-emit the boundary row.

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <set>
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
#include "clink/mysql/mysql_client.hpp"
#include "clink/mysql/mysql_sink.hpp"
#include "clink/mysql/mysql_source.hpp"
#endif

#ifdef CLINK_HAS_MYSQL

using clink::Batch;
using clink::Emitter;
using clink::PollingSource;
using clink::StreamElement;
using clink::mysql::Connection;
using clink::mysql::ConnectOptions;
using clink::mysql::MysqlPollOptions;
using clink::mysql::MysqlSink;
using clink::mysql::MysqlSinkOptions;
using clink::mysql::Result;

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
    return "clink_it_" + std::to_string(static_cast<long>(::getpid())) + "_" + std::to_string(n++);
}

void create_table(const std::string& tbl) {
    Connection c{parse_dsn()};
    c.exec("DROP TABLE IF EXISTS `" + tbl + "`");
    c.exec("CREATE TABLE `" + tbl + "` (id INT PRIMARY KEY, val VARCHAR(64))");
}
void drop_table(const std::string& tbl) {
    Connection c{parse_dsn()};
    c.exec("DROP TABLE IF EXISTS `" + tbl + "`");
}

struct Captured {
    std::vector<std::string> values;
};
Emitter<std::string> capturing(Captured& sink) {
    return Emitter<std::string>{[&sink](StreamElement<std::string> e) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                sink.values.push_back(r.value());
            }
        }
        return true;
    }};
}

MysqlSinkOptions sink_opts(const std::string& tbl, bool upsert) {
    MysqlSinkOptions o;
    o.conn = parse_dsn();
    o.table = tbl;
    o.columns = {"id", "val"};
    o.upsert = upsert;
    return o;
}

MysqlPollOptions source_opts(const std::string& tbl, int batch) {
    MysqlPollOptions o;
    o.conn = parse_dsn();
    o.table = tbl;
    o.cursor_column = "id";
    o.batch_size = batch;
    o.interval = std::chrono::milliseconds{50};  // drain fast in the test
    return o;
}

void drain(PollingSource<std::string>& src, Captured& cap, std::size_t want, int timeout_ms) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{timeout_ms};
    while (cap.values.size() < want && std::chrono::steady_clock::now() < deadline) {
        auto em = capturing(cap);
        src.produce(em);
    }
}

void write_rows(const std::string& tbl, int lo, int hi, const std::string& prefix, bool upsert) {
    MysqlSink sink(sink_opts(tbl, upsert));
    sink.open();
    Batch<std::string> b;
    for (int i = lo; i <= hi; ++i) {
        b.emplace(R"({"id":)" + std::to_string(i) + R"(,"val":")" + prefix + std::to_string(i) +
                  R"("})");
    }
    sink.on_data(b);
    sink.flush();
    sink.close();
}

// Extract the integer "id" field from an emitted source JSON row (values are text).
int id_of(const std::string& json) {
    auto j = clink::config::parse(json);
    if (j.is_object()) {
        const auto& o = j.as_object();
        auto it = o.find("id");
        if (it != o.end() && it->second.is_string()) {
            return std::stoi(it->second.as_string());
        }
    }
    return -1;
}

}  // namespace

TEST(MysqlLive, SinkThenSourceRoundTrip) {
    if (!mysql_configured()) {
        GTEST_SKIP() << "set CLINK_MYSQL_TEST_DSN (docker/integration-services.yml)";
    }
    const std::string tbl = unique_table();
    create_table(tbl);
    constexpr int kN = 20;
    write_rows(tbl, 1, kN, "v", /*upsert=*/false);

    auto src = source_opts(tbl, 1000);
    auto source = clink::mysql::make_mysql_poll_source(src);
    Captured cap;
    drain(*source, cap, kN, /*timeout_ms=*/15000);
    source.reset();  // frees the lazy connection
    drop_table(tbl);

    std::set<int> ids;
    for (const auto& v : cap.values) {
        ids.insert(id_of(v));
    }
    EXPECT_EQ(ids.size(), static_cast<std::size_t>(kN)) << "every inserted row should be read back";
    for (int i = 1; i <= kN; ++i) {
        EXPECT_EQ(ids.count(i), 1u) << "missing row " << i;
    }
}

TEST(MysqlLive, UpsertIsIdempotentByPrimaryKey) {
    if (!mysql_configured()) {
        GTEST_SKIP() << "set CLINK_MYSQL_TEST_DSN";
    }
    const std::string tbl = unique_table();
    create_table(tbl);

    write_rows(tbl, 1, 1, "first_", /*upsert=*/true);   // id=1 val=first_1
    write_rows(tbl, 1, 1, "second_", /*upsert=*/true);  // id=1 val=second_1 (overwrites)

    int count = -1;
    std::string val;
    {
        Connection c{parse_dsn()};
        Result r = c.query("SELECT COUNT(*) FROM `" + tbl + "`");
        if (MYSQL_ROW row = r.fetch_row()) {
            count = std::stoi(row[0]);
        }
        Result r2 = c.query("SELECT val FROM `" + tbl + "` WHERE id=1");
        if (MYSQL_ROW row = r2.fetch_row()) {
            val = row[0] ? row[0] : "";
        }
    }
    drop_table(tbl);

    EXPECT_EQ(count, 1) << "upsert by PK must not duplicate the row";
    EXPECT_EQ(val, "second_1") << "upsert must overwrite the value";
}

// HIGH #5 regression: a NON-UNIQUE cursor (ties on `ts`) with an id_column
// tie-breaker must deliver every row, even when a page (LIMIT) boundary falls in
// the middle of a tie group. Without the composite keyset the single-column '>'
// would drop the tie members beyond the page.
TEST(MysqlLive, CompositeCursorHandlesNonUniqueCursor) {
    if (!mysql_configured()) {
        GTEST_SKIP() << "set CLINK_MYSQL_TEST_DSN";
    }
    const std::string tbl = unique_table();
    {
        Connection c{parse_dsn()};
        c.exec("DROP TABLE IF EXISTS `" + tbl + "`");
        c.exec("CREATE TABLE `" + tbl + "` (id INT PRIMARY KEY, ts INT NOT NULL, val VARCHAR(16))");
        // ties on ts (10 x3, 20 x2) so a LIMIT-2 page boundary splits a tie group.
        c.exec("INSERT INTO `" + tbl +
               "` VALUES (1,10,'a'),(2,10,'b'),(3,10,'c'),(4,20,'d'),(5,20,'e'),(6,30,'f')");
    }
    MysqlPollOptions o;
    o.conn = parse_dsn();
    o.table = tbl;
    o.cursor_column = "ts";  // non-unique
    o.id_column = "id";      // unique tie-breaker
    o.batch_size = 2;        // force page boundaries inside the tie groups
    o.interval = std::chrono::milliseconds{50};
    auto source = clink::mysql::make_mysql_poll_source(o);
    Captured cap;
    drain(*source, cap, 6, /*timeout_ms=*/15000);
    source.reset();
    drop_table(tbl);

    EXPECT_EQ(cap.values.size(), 6u) << "no rows dropped at a non-unique-cursor page boundary";
    std::set<int> ids;
    for (const auto& v : cap.values) {
        ids.insert(id_of(v));
    }
    for (int i = 1; i <= 6; ++i) {
        EXPECT_EQ(ids.count(i), 1u) << "missing id " << i;
    }
}

TEST(MysqlLive, TlsConnectionIsEncrypted) {
    if (!mysql_configured()) {
        GTEST_SKIP() << "set CLINK_MYSQL_TEST_DSN";
    }
    // mysql:8.0 has TLS enabled by default with a self-signed cert, so connect
    // with ssl + ssl_verify=false (encrypted, cert not authenticated) and confirm
    // the session is actually using a cipher.
    ConnectOptions o = parse_dsn();
    o.ssl = true;
    o.ssl_verify = false;
    Connection c{o};
    Result r = c.query("SHOW SESSION STATUS LIKE 'Ssl_cipher'");
    std::string cipher;
    if (MYSQL_ROW row = r.fetch_row()) {
        cipher = row[1] != nullptr ? row[1] : "";
    }
    EXPECT_FALSE(cipher.empty()) << "ssl=true must yield an encrypted session (Ssl_cipher set)";
}

TEST(MysqlLive, CursorSourceDoesNotReemitBoundaryRow) {
    if (!mysql_configured()) {
        GTEST_SKIP() << "set CLINK_MYSQL_TEST_DSN";
    }
    const std::string tbl = unique_table();
    create_table(tbl);
    constexpr int kN = 10;
    write_rows(tbl, 1, kN, "v", /*upsert=*/false);

    // batch_size 5 forces at least two polls, exercising the exclusive cursor
    // across the page boundary.
    auto src = source_opts(tbl, 5);
    auto source = clink::mysql::make_mysql_poll_source(src);
    Captured cap;
    drain(*source, cap, kN, /*timeout_ms=*/15000);
    source.reset();
    drop_table(tbl);

    EXPECT_EQ(cap.values.size(), static_cast<std::size_t>(kN))
        << "exclusive cursor must deliver each row exactly once (no boundary re-emit)";
    std::set<int> ids;
    for (const auto& v : cap.values) {
        ids.insert(id_of(v));
    }
    EXPECT_EQ(ids.size(), static_cast<std::size_t>(kN));
}

#endif  // CLINK_HAS_MYSQL
