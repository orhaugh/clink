// The MySQL sinks' contract against a live server. SKIPPED unless
// CLINK_MYSQL_TEST_DSN names a MySQL server; scripts/mysql-live.sh runs it
// against pinned MySQL 8.0 and 8.4 servers and fails when any case is skipped.
//
// Expectations come from the server, not from the sinks. The keyed cases apply
// the same changes one statement at a time, through the test's own client, to a
// reference table of the same definition, and require the sink's table to equal
// it row for row: MySQL itself decides which keys are one row (a collation's
// case and padding rules, a number written as text), and the sink must end
// where applying each change in order ends.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/operator_registry.hpp"
#include "clink/core/record.hpp"
#include "clink/fault/fault_injection.hpp"
#include "clink/runtime/runtime_context.hpp"

#ifdef CLINK_HAS_MYSQL
#include "clink/mysql/mysql_client.hpp"
#include "clink/mysql/mysql_json_upsert_sink.hpp"
#include "clink/mysql/mysql_sink.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::mysql::Connection;
using clink::mysql::ConnectOptions;
using clink::mysql::MysqlJsonUpsertSink;
using clink::mysql::MysqlJsonUpsertSinkOptions;
using clink::mysql::MysqlSink;
using clink::mysql::MysqlSinkOptions;
using clink::mysql::Result;

namespace {

bool configured() {
    return std::getenv("CLINK_MYSQL_TEST_DSN") != nullptr;
}

#define REQUIRE_MYSQL()                                                        \
    do {                                                                       \
        if (!configured()) {                                                   \
            GTEST_SKIP() << "set CLINK_MYSQL_TEST_DSN against a MySQL server"; \
        }                                                                      \
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
        const auto k = tok.substr(0, eq);
        const auto v = tok.substr(eq + 1);
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

// The test's own client: its statements, and the reads every case judges by.
struct Db {
    Db() : c(dsn()) { c.exec("SET SESSION time_zone = '+00:00'"); }
    void exec(const std::string& sql) { c.exec(sql); }
    std::string scalar(const std::string& sql) {
        Result r = c.query(sql);
        MYSQL_ROW row = r.fetch_row();
        return row != nullptr && row[0] != nullptr ? std::string{row[0]} : std::string{"NULL"};
    }
    // Every row, each cell as text (NULL for NULL), sorted.
    std::vector<std::string> rows(const std::string& sql) {
        std::vector<std::string> out;
        Result r = c.query(sql);
        const auto n = r.num_fields();
        while (MYSQL_ROW row = r.fetch_row()) {
            const unsigned long* len = r.lengths();
            std::string line;
            for (unsigned i = 0; i < n; ++i) {
                line += (i > 0 ? "|" : "") +
                        (row[i] != nullptr ? std::string(row[i], len[i]) : std::string{"NULL"});
            }
            out.push_back(std::move(line));
        }
        std::sort(out.begin(), out.end());
        return out;
    }
    Connection c;
};

// Sets a global variable for the case, `literal` as SQL (quoted when a string),
// and puts it back afterwards. A new connection takes the global value.
class GlobalSetting {
public:
    GlobalSetting(std::string name, const std::string& literal) : name_(std::move(name)) {
        Db db;
        old_ = db.scalar("SELECT @@global." + name_);
        if (old_.empty() || old_.find_first_not_of("0123456789") != std::string::npos) {
            old_ = "'" + old_ + "'";
        }
        db.exec("SET GLOBAL " + name_ + " = " + literal);
    }
    ~GlobalSetting() {
        try {
            Db db;
            db.exec("SET GLOBAL " + name_ + " = " + old_);
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

void fresh(Db& db, const std::string& table, const std::string& definition) {
    db.exec("DROP TABLE IF EXISTS `" + table + "`");
    db.exec("CREATE TABLE `" + table + "` " + definition);
}

Batch<std::string> batch(const std::vector<std::string>& records) {
    Batch<std::string> b;
    for (const auto& r : records) {
        b.emplace(std::string{r});
    }
    return b;
}

template <typename F>
std::string thrown(F&& f) {
    try {
        f();
    } catch (const std::exception& e) {
        return e.what();
    }
    return "";
}

const CheckpointBarrier kBarrier{clink::CheckpointId{1}};

MysqlSinkOptions append_opts(const std::string& table, std::vector<std::string> columns) {
    MysqlSinkOptions o;
    o.conn = dsn();
    o.table = table;
    o.columns = std::move(columns);
    return o;
}

MysqlJsonUpsertSinkOptions upsert_opts(const std::string& table,
                                       std::vector<std::string> columns,
                                       std::vector<std::string> keys) {
    MysqlJsonUpsertSinkOptions o;
    o.conn = dsn();
    o.table = table;
    o.columns = std::move(columns);
    o.key_columns = std::move(keys);
    return o;
}

// One change of a keyed case: the record the sink gets, and the statement the
// reference table gets for it.
struct Change {
    std::string record;
    std::string reference_sql;  // with %T for the reference table's name
};

std::string with_table(std::string sql, const std::string& table) {
    for (auto at = sql.find("%T"); at != std::string::npos; at = sql.find("%T")) {
        sql.replace(at, 2, "`" + table + "`");
    }
    return sql;
}

Change up(const std::string& k_json,
          const std::string& v_json,
          const std::string& k_sql,
          const std::string& v_sql) {
    return Change{R"({"k":)" + k_json + R"(,"v":)" + v_json + "}",
                  "INSERT INTO %T (k, v) VALUES (" + k_sql + ", " + v_sql +
                      ") AS n ON DUPLICATE KEY UPDATE k = n.k, v = n.v"};
}
Change del(const std::string& k_json, const std::string& k_sql) {
    return Change{R"({"k":)" + k_json + R"(,"v":null,"__row_kind":"delete"})",
                  "DELETE FROM %T WHERE k = " + k_sql};
}

// Applies `changes` to a fresh table of `definition` through the sink, `per_flush`
// changes to a flush, and one statement at a time to a reference table of the
// same definition, and returns both tables' rows.
std::pair<std::vector<std::string>, std::vector<std::string>> keyed_case(
    const std::string& definition, const std::vector<Change>& changes, std::size_t per_flush) {
    Db db;
    const auto t = unique("snk_key");
    const auto ref = unique("snk_ref");
    fresh(db, t, definition);
    fresh(db, ref, definition);
    MysqlJsonUpsertSink sink(upsert_opts(t, {"k", "v"}, {"k"}));
    sink.open();
    std::vector<std::string> pending;
    for (const auto& c : changes) {
        db.exec(with_table(c.reference_sql, ref));
        pending.push_back(c.record);
        if (pending.size() == per_flush) {
            sink.on_data(batch(pending));
            sink.on_barrier(kBarrier);
            pending.clear();
        }
    }
    sink.on_data(batch(pending));
    sink.on_barrier(kBarrier);
    sink.close();
    return {db.rows("SELECT k, v FROM `" + t + "`"), db.rows("SELECT k, v FROM `" + ref + "`")};
}

void expect_keyed(const std::string& definition, const std::vector<Change>& changes) {
    for (const std::size_t per_flush : {changes.size(), std::size_t{1}}) {
        const auto [got, want] = keyed_case(definition, changes, per_flush);
        EXPECT_EQ(got, want) << definition << ", " << per_flush << " change(s) per flush";
    }
}

}  // namespace

// ---- keys the table equates -----------------------------------------------------------

TEST(MysqlSinkContractLive, ACaseInsensitiveKeyEndsAsTheLastChangeLeftIt) {
    REQUIRE_MYSQL();
    const std::string ci =
        "(k VARCHAR(16) COLLATE utf8mb4_0900_ai_ci PRIMARY KEY, v VARCHAR(16)) ENGINE=InnoDB";
    expect_keyed(ci, {del(R"("abc")", "'abc'"), up(R"("ABC")", R"("y")", "'ABC'", "'y'")});
    expect_keyed(ci, {up(R"("abc")", R"("x")", "'abc'", "'x'"), del(R"("ABC")", "'ABC'")});
    expect_keyed(ci,
                 {up(R"("Abc")", R"("1")", "'Abc'", "'1'"),
                  up(R"("aBC")", R"("2")", "'aBC'", "'2'"),
                  up(R"("cafe")", R"("3")", "'cafe'", "'3'"),
                  del(R"("CAFÉ")", "'CAFÉ'"),
                  up(R"("café")", R"("4")", "'café'", "'4'")});
}

TEST(MysqlSinkContractLive, TrailingSpacesFollowTheCollationsPadding) {
    REQUIRE_MYSQL();
    for (const char* coll : {"utf8mb4_bin", "utf8mb4_0900_bin"}) {
        const std::string def = "(k VARCHAR(16) COLLATE " + std::string{coll} +
                                " PRIMARY KEY, v VARCHAR(16)) ENGINE=InnoDB";
        expect_keyed(def,
                     {up(R"("a")", R"("1")", "'a'", "'1'"),
                      up(R"("a ")", R"("2")", "'a '", "'2'"),
                      del(R"("a")", "'a'")});
    }
}

TEST(MysqlSinkContractLive, NumericKeysInEveryJsonSpelling) {
    REQUIRE_MYSQL();
    const std::string i = "(k INT PRIMARY KEY, v VARCHAR(16)) ENGINE=InnoDB";
    expect_keyed(i,
                 {up("1", R"("a")", "1", "'a'"),
                  del(R"("1")", "'1'"),
                  up("1.0", R"("b")", "1", "'b'"),
                  up("10e-1", R"("c")", "1", "'c'"),
                  del("2", "2"),
                  up(R"("2")", R"("d")", "'2'", "'d'")});
    const std::string d = "(k DECIMAL(10,2) PRIMARY KEY, v VARCHAR(16)) ENGINE=InnoDB";
    expect_keyed(d,
                 {up("1.5", R"("a")", "1.5", "'a'"),
                  del(R"("1.50")", "'1.50'"),
                  up("15e-1", R"("b")", "1.5", "'b'")});
}

TEST(MysqlSinkContractLive, CompositeAndTimestampKeys) {
    REQUIRE_MYSQL();
    Db db;
    const auto t = unique("snk_comp");
    fresh(db,
          t,
          "(a VARCHAR(8) COLLATE utf8mb4_0900_ai_ci, b INT, at DATETIME(3), v INT, "
          "PRIMARY KEY (a, b, at)) ENGINE=InnoDB");
    auto o = upsert_opts(t, {"a", "b", "at", "v"}, {"a", "b", "at"});
    o.timestamp_columns = {{"at", clink::sqljson::TimeKind::Timestamp}};
    MysqlJsonUpsertSink sink(o);
    sink.open();
    sink.on_data(batch({R"({"a":"x","b":1,"at":1792863688105,"v":1})",
                        R"({"a":"X","b":1,"at":1792863688105,"v":2})",
                        R"({"a":"x","b":2,"at":1792863688105,"v":3})",
                        R"({"a":"x","b":2,"at":1792863688105,"v":0,"__row_kind":"delete"})",
                        R"({"a":"y","b":1,"at":1792863688106,"v":4})"}));
    sink.on_barrier(kBarrier);
    EXPECT_EQ(db.rows("SELECT a, b, DATE_FORMAT(at, '%Y-%m-%d %H:%i:%s.%f'), v FROM `" + t + "`"),
              (std::vector<std::string>{"X|1|2026-10-24 17:41:28.105000|2",
                                        "y|1|2026-10-24 17:41:28.106000|4"}));
}

// ---- tables the sinks refuse ----------------------------------------------------------

TEST(MysqlSinkContractLive, TablesWhoseIdentityTheSinksCannotHonourAreRefused) {
    REQUIRE_MYSQL();
    Db db;
    const auto two = unique("snk_two");
    fresh(db, two, "(k INT PRIMARY KEY, v INT, UNIQUE KEY uv (v)) ENGINE=InnoDB");
    auto o = append_opts(two, {"k", "v"});
    o.upsert = true;
    MysqlSink odku(o);
    EXPECT_NE(thrown([&] { odku.open(); }).find("2 unique indexes"), std::string::npos);
    MysqlJsonUpsertSink extra(upsert_opts(two, {"k", "v"}, {"k"}));
    EXPECT_NE(thrown([&] { extra.open(); }).find("also has unique index uv"), std::string::npos);

    const auto none = unique("snk_none");
    fresh(db, none, "(k INT, v INT) ENGINE=InnoDB");
    o = append_opts(none, {"k", "v"});
    o.upsert = true;
    MysqlSink keyless(o);
    EXPECT_NE(thrown([&] { keyless.open(); }).find("needs a PRIMARY KEY or UNIQUE index"),
              std::string::npos);

    const auto prefix = unique("snk_prefix");
    fresh(db, prefix, "(k VARCHAR(64), v INT, UNIQUE KEY uk (k(4))) ENGINE=InnoDB");
    MysqlJsonUpsertSink prefixed(upsert_opts(prefix, {"k", "v"}, {"k"}));
    EXPECT_NE(thrown([&] { prefixed.open(); }).find("prefix"), std::string::npos);

    const auto myisam = unique("snk_myisam");
    fresh(db, myisam, "(k INT PRIMARY KEY, v INT) ENGINE=MyISAM");
    MysqlJsonUpsertSink keyed(upsert_opts(myisam, {"k", "v"}, {"k"}));
    EXPECT_NE(thrown([&] { keyed.open(); }).find("MyISAM"), std::string::npos);
    // The append sink writes it, without a transaction.
    MysqlSink plain(append_opts(myisam, {"k", "v"}));
    plain.open();
    plain.on_data(batch({R"({"k":1,"v":1})"}));
    plain.on_barrier(kBarrier);
    EXPECT_EQ(db.rows("SELECT k, v FROM `" + myisam + "`"), (std::vector<std::string>{"1|1"}));

    const auto three = unique("snk_three");
    fresh(db, three, "(k INT PRIMARY KEY, v INT, w INT) ENGINE=InnoDB");
    auto partial = upsert_opts(three, {"k", "v", "w"}, {"k"});
    partial.update_columns = {"v"};
    MysqlJsonUpsertSink part(partial);
    EXPECT_NE(thrown([&] { part.open(); }).find("leaves out column 'w'"), std::string::npos);

    const auto ci = unique("snk_ci");
    fresh(db, ci, "(k VARCHAR(16) COLLATE utf8mb4_0900_ai_ci PRIMARY KEY, v INT) ENGINE=InnoDB");
    auto parallel = upsert_opts(ci, {"k", "v"}, {"k"});
    parallel.parallelism = 2;
    MysqlJsonUpsertSink two_subtasks(parallel);
    EXPECT_NE(thrown([&] { two_subtasks.open(); }).find("utf8mb4_0900_ai_ci"), std::string::npos);
    const auto bin = unique("snk_bin");
    fresh(db, bin, "(k VARCHAR(16) COLLATE utf8mb4_0900_bin PRIMARY KEY, v INT) ENGINE=InnoDB");
    parallel = upsert_opts(bin, {"k", "v"}, {"k"});
    parallel.parallelism = 2;
    MysqlJsonUpsertSink exact(parallel);
    EXPECT_NO_THROW(exact.open());

    const auto gen = unique("snk_gen");
    fresh(db, gen, "(k INT PRIMARY KEY, v INT, g INT AS (v * 2)) ENGINE=InnoDB");
    MysqlSink generated(append_opts(gen, {"k", "v", "g"}));
    EXPECT_NE(thrown([&] { generated.open(); }).find("generated"), std::string::npos);
    MysqlSink missing(append_opts(unique("snk_absent"), {"k"}));
    EXPECT_NE(thrown([&] { missing.open(); }).find("does not exist"), std::string::npos);
}

// ---- values -----------------------------------------------------------------------------

TEST(MysqlSinkContractLive, ValuesLandExactly) {
    REQUIRE_MYSQL();
    Db db;
    const auto t = unique("snk_vals");
    fresh(db,
          t,
          "(id INT PRIMARY KEY, u BIGINT UNSIGNED, d DECIMAL(65,30), s VARCHAR(64) CHARACTER SET "
          "utf8mb4, b VARBINARY(16), j JSON, at DATETIME(6), ts TIMESTAMP(3) NULL, f DOUBLE, "
          "flag TINYINT) ENGINE=InnoDB");
    auto o = append_opts(t, {"id", "u", "d", "s", "b", "j", "at", "ts", "f", "flag"});
    o.timestamp_columns = {{"at", clink::sqljson::TimeKind::Timestamp},
                           {"ts", clink::sqljson::TimeKind::TimestampTz}};
    MysqlSink sink(o);
    sink.open();
    sink.on_data(batch({
        R"({"id":1,"u":18446744073709551615,"d":12345678901234567890123456789012345.123456789012345678901234567891,"s":"caf\u00e9 \ud83d\ude00 'q' \\b","b":"a\u0000\u0007z","j":{"x":[1,2],"y":"z"},"at":-62135596800000,"ts":1000,"f":0.1,"flag":true})",
        R"({"id":2,"u":0,"d":-0.000000000000000000000000000001,"s":"","b":"","j":[],"at":253402300799999,"ts":2147483647999,"f":-1.5e300,"flag":false})",
        R"({"id":3,"u":null,"d":null,"s":null,"b":null,"j":null,"at":null,"ts":null,"f":null})",
    }));
    sink.on_barrier(kBarrier);
    EXPECT_EQ(
        db.rows(
            "SELECT id, u, d, HEX(s), HEX(b), CAST(j AS CHAR), DATE_FORMAT(at, "
            "'%Y-%m-%d %H:%i:%s.%f'), CAST(UNIX_TIMESTAMP(ts) * 1000 AS SIGNED), f, flag FROM `" +
            t + "`"),
        (std::vector<std::string>{
            "1|18446744073709551615|12345678901234567890123456789012345."
            "123456789012345678901234567891|"
            "636166C3A920F09F988020277127205C62|6100077A|{\"x\": [1, 2], \"y\": \"z\"}|"
            "0001-01-01 00:00:00.000000|1000|0.1|1",
            "2|0|-0.000000000000000000000000000001|||[]|9999-12-31 23:59:59.999000|2147483647999|"
            "-1.5e300|0",
            "3|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL"}));
}

TEST(MysqlSinkContractLive, NullAndMissingMembersBothWriteNullButAKeyMustBeThere) {
    REQUIRE_MYSQL();
    Db db;
    const auto t = unique("snk_null");
    fresh(db, t, "(k INT PRIMARY KEY, v VARCHAR(8) DEFAULT 'dflt', w INT) ENGINE=InnoDB");
    MysqlJsonUpsertSink sink(upsert_opts(t, {"k", "v", "w"}, {"k"}));
    sink.open();
    sink.on_data(batch({R"({"k":1,"v":"a","w":1})", R"({"k":2,"v":"b","w":2})"}));
    sink.on_barrier(kBarrier);
    sink.on_data(batch({R"({"k":1,"v":null,"w":10})", R"({"k":2,"w":20})"}));
    sink.on_barrier(kBarrier);
    EXPECT_EQ(db.rows("SELECT k, v, w FROM `" + t + "`"),
              (std::vector<std::string>{"1|NULL|10", "2|NULL|20"}));
    EXPECT_NE(thrown([&] { sink.on_data(batch({R"({"v":"x"})"})); }).find("key member \"k\""),
              std::string::npos);
}

TEST(MysqlSinkContractLive, AValueTheColumnCannotHoldFailsTheFlushAndCommitsNothing) {
    REQUIRE_MYSQL();
    struct Bad {
        std::string definition;
        std::string good;  // a row the column holds
        std::string record;
        std::string expect;
    };
    for (const auto& c : std::vector<Bad>{
             {"(id INT PRIMARY KEY, x DECIMAL(10,2))",
              R"({"id":1,"x":1.25})",
              R"({"id":2,"x":1.234})",
              "1265"},
             {"(id INT PRIMARY KEY, x VARCHAR(3))",
              R"({"id":1,"x":"abc"})",
              R"({"id":2,"x":"abcd"})",
              "1406"},
             {"(id INT PRIMARY KEY, x TINYINT)",
              R"({"id":1,"x":127})",
              R"({"id":2,"x":300})",
              "1264"},
             {"(id INT PRIMARY KEY, x VARCHAR(8) CHARACTER SET latin1)",
              R"({"id":1,"x":"caf\u00e9"})",
              R"({"id":2,"x":"\ud83d\ude00"})",
              "1366"},
             {"(id INT PRIMARY KEY, x INT)",
              R"({"id":1,"x":1})",
              R"({"id":2,"x":"12abc"})",
              "1265"},
             {"(id INT PRIMARY KEY, x DATE)",
              R"({"id":1,"x":"2026-02-28"})",
              R"({"id":2,"x":"2026-02-30"})",
              "1292"},
             {"(id INT PRIMARY KEY, x INT NOT NULL)",
              R"({"id":1,"x":1})",
              R"({"id":2,"x":null})",
              "1048"},
         }) {
        Db db;
        const auto t = unique("snk_bad");
        fresh(db, t, c.definition + " ENGINE=InnoDB");
        MysqlSink sink(append_opts(t, {"id", "x"}));
        sink.open();
        // The good row shares the flush's transaction, so it must not land either.
        sink.on_data(batch({c.good, c.record}));
        const auto why = thrown([&] { sink.on_barrier(kBarrier); });
        EXPECT_NE(why.find(c.expect), std::string::npos) << c.definition << ": " << why;
        EXPECT_EQ(db.scalar("SELECT COUNT(*) FROM `" + t + "`"), "0") << c.definition;
        EXPECT_NE(thrown([&] { sink.on_barrier(kBarrier); }).find("failed earlier"),
                  std::string::npos);
        // Opened again, it writes the good row.
        sink.open();
        sink.on_data(batch({c.good}));
        sink.on_barrier(kBarrier);
        EXPECT_EQ(db.scalar("SELECT COUNT(*) FROM `" + t + "`"), "1") << c.definition;
    }
}

TEST(MysqlSinkContractLive, TheSessionIsStrictAndEscapesWhateverTheServersMode) {
    REQUIRE_MYSQL();
    GlobalSetting mode("sql_mode", "'NO_BACKSLASH_ESCAPES'");
    Db db;
    const auto t = unique("snk_mode");
    fresh(db, t, "(id INT PRIMARY KEY, x VARCHAR(16)) ENGINE=InnoDB");
    MysqlSink sink(append_opts(t, {"id", "x"}));
    sink.open();
    sink.on_data(batch({R"({"id":1,"x":"a\\b'c\"d"})"}));
    sink.on_barrier(kBarrier);
    // a, backslash, b, quote, c, double quote, d: each byte as sent.
    EXPECT_EQ(db.scalar("SELECT HEX(x) FROM `" + t + "` WHERE id = 1"), "615C6227632264");
    // Not strict on the server, still strict for the sink.
    sink.on_data(batch({R"({"id":2,"x":"far too long a value"})"}));
    EXPECT_NE(thrown([&] { sink.on_barrier(kBarrier); }).find("1406"), std::string::npos);
}

// ---- transactions, sizes, locks and cancels -------------------------------------------

TEST(MysqlSinkContractLive, AFlushOfManyStatementsIsOneTransaction) {
    REQUIRE_MYSQL();
    Db db;
    const auto t = unique("snk_tx");
    fresh(db, t, "(id INT PRIMARY KEY, x VARCHAR(600)) ENGINE=InnoDB");
    auto o = append_opts(t, {"id", "x"});
    o.max_statement_bytes = 2048;
    o.max_record_bytes = 1024;
    MysqlSink sink(o);
    sink.open();
    std::vector<std::string> records;
    for (int i = 0; i < 40; ++i) {
        records.push_back(R"({"id":)" + std::to_string(i) + R"(,"x":")" + std::string(500, 'x') +
                          "\"}");
    }
    // The last row collides with the first: the flush's statements roll back
    // together.
    records.push_back(R"({"id":0,"x":"again"})");
    sink.on_data(batch(records));
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    EXPECT_NE(why.find("1062"), std::string::npos) << why;
    EXPECT_NE(why.find("<value>"), std::string::npos) << why;  // the key is not quoted
    EXPECT_EQ(db.scalar("SELECT COUNT(*) FROM `" + t + "`"), "0");
    sink.open();
    records.pop_back();
    sink.on_data(batch(records));
    sink.on_barrier(kBarrier);
    EXPECT_EQ(db.scalar("SELECT COUNT(*) FROM `" + t + "`"), "40");
    EXPECT_EQ(db.scalar("SELECT COUNT(*) FROM `" + t + "` WHERE LENGTH(x) = 500"), "40");
}

TEST(MysqlSinkContractLive, StatementsStayUnderTheServersPacketLimit) {
    REQUIRE_MYSQL();
    GlobalSetting packet("max_allowed_packet", "65536");
    Db db;
    const auto t = unique("snk_pkt");
    fresh(db, t, "(id INT PRIMARY KEY, x VARCHAR(1100)) ENGINE=InnoDB");
    MysqlSink sink(append_opts(t, {"id", "x"}));
    sink.open();
    std::vector<std::string> records;
    for (int i = 0; i < 300; ++i) {  // about 300 KB as one statement
        records.push_back(R"({"id":)" + std::to_string(i) + R"(,"x":")" + std::string(1000, 'p') +
                          "\"}");
    }
    sink.on_data(batch(records));
    sink.on_barrier(kBarrier);
    EXPECT_EQ(db.scalar("SELECT COUNT(*) FROM `" + t + "`"), "300");
    auto small = append_opts(t, {"id", "x"});
    small.max_record_bytes = 512;
    MysqlSink limited(small);
    limited.open();
    EXPECT_NE(thrown([&] {
                  limited.on_data(batch({R"({"id":999,"x":")" + std::string(1000, 'q') + "\"}"}));
              }).find("more than the 512"),
              std::string::npos);
}

TEST(MysqlSinkContractLive, ALockedRowFailsTheFlushWithinItsBound) {
    REQUIRE_MYSQL();
    Db db;
    const auto t = unique("snk_lock");
    fresh(db, t, "(k INT PRIMARY KEY, v INT) ENGINE=InnoDB");
    db.exec("INSERT INTO `" + t + "` VALUES (1, 0)");
    Db holder;
    holder.exec("BEGIN");
    holder.exec("UPDATE `" + t + "` SET v = 99 WHERE k = 1");
    auto o = upsert_opts(t, {"k", "v"}, {"k"});
    o.lock_wait_timeout = std::chrono::seconds{1};
    o.max_retries = 1;
    MysqlJsonUpsertSink sink(o);
    sink.open();
    sink.on_data(batch({R"({"k":1,"v":1})"}));
    const auto started = std::chrono::steady_clock::now();
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    const auto waited = std::chrono::steady_clock::now() - started;
    EXPECT_NE(why.find("gave up after 1 retries"), std::string::npos) << why;
    EXPECT_NE(why.find("1205"), std::string::npos) << why;
    EXPECT_LT(waited, std::chrono::seconds{10});
    holder.exec("ROLLBACK");
    EXPECT_EQ(db.rows("SELECT k, v FROM `" + t + "`"), (std::vector<std::string>{"1|0"}));
}

TEST(MysqlSinkContractLive, ACancelEndsAFlushWaitingOnALock) {
    REQUIRE_MYSQL();
    Db db;
    const auto t = unique("snk_cancel");
    fresh(db, t, "(k INT PRIMARY KEY, v INT) ENGINE=InnoDB");
    db.exec("INSERT INTO `" + t + "` VALUES (1, 0)");
    Db holder;
    holder.exec("BEGIN");
    holder.exec("UPDATE `" + t + "` SET v = 99 WHERE k = 1");
    MysqlJsonUpsertSink sink(upsert_opts(t, {"k", "v"}, {"k"}));  // waits up to 30 s
    auto cancel = std::make_shared<std::atomic<bool>>(false);
    clink::RuntimeContext ctx(clink::OperatorId{77}, "mysql_upsert_sink", nullptr, nullptr);
    ctx.set_cancel_signal(clink::CancelSignal{cancel, nullptr});
    sink.attach_runtime(&ctx);
    sink.open();
    sink.on_data(batch({R"({"k":1,"v":1})"}));
    std::thread canceller([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds{300});
        cancel->store(true);
    });
    const auto started = std::chrono::steady_clock::now();
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    canceller.join();
    EXPECT_LT(std::chrono::steady_clock::now() - started, std::chrono::seconds{10});
    EXPECT_NE(why.find("cancelled"), std::string::npos) << why;
    EXPECT_NO_THROW(sink.close());
    sink.attach_runtime(nullptr);
    holder.exec("ROLLBACK");
    EXPECT_EQ(db.rows("SELECT k, v FROM `" + t + "`"), (std::vector<std::string>{"1|0"}));
}

// A connection killed while its COMMIT is on the way: the sink reports that the
// flush may or may not have been committed. Here the server rolled it back,
// which the table shows; the sink does not claim either.
TEST(MysqlSinkContractLive, ACommitWhoseReplyIsLostIsReportedAsUnknown) {
    REQUIRE_MYSQL();
#if !CLINK_FAULT_ENABLED
    GTEST_SKIP() << "needs a build with fault injection";
#else
    Db db;
    const auto t = unique("snk_lost");
    fresh(db, t, "(k INT PRIMARY KEY, v INT) ENGINE=InnoDB");
    MysqlSink sink(append_opts(t, {"k", "v"}));
    sink.open();
    const auto id = db.scalar(
        "SELECT ID FROM information_schema.PROCESSLIST WHERE ID <> "
        "CONNECTION_ID() AND DB = DATABASE() ORDER BY ID DESC LIMIT 1");
    clink::fault::ScopedFault park("mysql.sink_before_commit=delay:1500");
    sink.on_data(batch({R"({"k":1,"v":1})"}));
    std::thread killer([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds{500});
        db.exec("KILL " + id);
    });
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    killer.join();
    EXPECT_NE(why.find("may or may not have been committed"), std::string::npos) << why;
    Db reader;
    EXPECT_EQ(reader.scalar("SELECT COUNT(*) FROM `" + t + "`"), "0");
#endif
}

// ---- the factories' path ----------------------------------------------------------------

TEST(MysqlSinkContractLive, TheFactoriesBuildSinksThatWrite) {
    REQUIRE_MYSQL();
    Db db;
    const auto a = unique("snk_fa");
    const auto k = unique("snk_fk");
    fresh(db, a, "(id INT PRIMARY KEY, v VARCHAR(8)) ENGINE=InnoDB");
    fresh(db, k, "(id INT PRIMARY KEY, v VARCHAR(8)) ENGINE=InnoDB");
    const auto c = dsn();
    for (const auto& [type, table] : std::vector<std::pair<std::string, std::string>>{
             {"mysql_sink", a}, {"mysql_upsert_sink", k}}) {
        const auto* f = clink::cluster::OperatorRegistry::default_instance().find_sink(
            type, std::string{clink::cluster::kChannelString});
        ASSERT_NE(f, nullptr);
        clink::cluster::OperatorBuildContext ctx;
        ctx.params = {{"host", c.host},
                      {"port", std::to_string(c.port)},
                      {"user", c.user},
                      {"password", c.password},
                      {"database", c.database},
                      {"table", table},
                      {"schema_columns", "id:i64;v:str"},
                      {"key_columns", "id"},
                      {"batch_records", "2"}};
        auto sink = std::static_pointer_cast<clink::Sink<std::string>>(f->build(ctx));
        sink->open();
        sink->on_data(batch({R"({"id":1,"v":"a"})", R"({"id":2,"v":"b"})", R"({"id":3,"v":"c"})"}));
        sink->on_barrier(kBarrier);
        sink->close();
        EXPECT_EQ(db.rows("SELECT id, v FROM `" + table + "`"),
                  (std::vector<std::string>{"1|a", "2|b", "3|c"}))
            << type;
    }
}

#endif  // CLINK_HAS_MYSQL
