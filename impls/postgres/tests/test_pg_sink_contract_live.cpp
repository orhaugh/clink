// The PostgreSQL sinks' contract against a live server. SKIPPED unless
// CLINK_POSTGRES_CDC_TEST_DSN names a PostgreSQL server; scripts/postgres-cdc-live.sh
// runs it against the pinned PostgreSQL 16 server and fails when any case is
// skipped.
//
// Expectations come from the server, not from the sinks. The keyed cases apply
// the same changes one statement at a time, through the test's own client, to a
// reference table of the same definition, and require the sink's table to equal
// it row for row: PostgreSQL itself decides which keys are one row (a
// nondeterministic collation, char(n) padding, a number written as text), and
// the sink must end where applying each change in order ends.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/operator_registry.hpp"
#include "clink/core/record.hpp"
#include "clink/fault/fault_injection.hpp"
#include "clink/runtime/runtime_context.hpp"

#ifdef CLINK_HAS_POSTGRES
#include <libpq-fe.h>

#include "clink/connectors/postgres_json_sink.hpp"
#include "clink/connectors/postgres_json_upsert_sink.hpp"
#include "clink/connectors/postgres_sink.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::PostgresJsonSink;
using clink::PostgresJsonSinkOptions;
using clink::PostgresJsonUpsertSink;
using clink::PostgresJsonUpsertSinkOptions;

namespace {

bool pg_live() {
    return std::getenv("CLINK_POSTGRES_CDC_TEST_DSN") != nullptr;
}

std::string dsn() {
    return std::getenv("CLINK_POSTGRES_CDC_TEST_DSN");
}

#define REQUIRE_PG()                                                                       \
    do {                                                                                   \
        if (!pg_live()) {                                                                  \
            GTEST_SKIP() << "set CLINK_POSTGRES_CDC_TEST_DSN against a PostgreSQL server"; \
        }                                                                                  \
    } while (0)

// The test's own client: its statements, and the reads every case judges by.
class Pg {
public:
    Pg() : c_(PQconnectdb(dsn().c_str())) {
        if (PQstatus(c_) != CONNECTION_OK) {
            ADD_FAILURE() << PQerrorMessage(c_);
        }
        PQsetNoticeProcessor(c_, [](void*, const char*) {}, nullptr);
        exec("SET TIME ZONE 'UTC'");
    }
    ~Pg() { PQfinish(c_); }
    Pg(const Pg&) = delete;
    Pg& operator=(const Pg&) = delete;

    void exec(const std::string& sql) {
        PGresult* r = PQexec(c_, sql.c_str());
        const auto st = PQresultStatus(r);
        EXPECT_TRUE(st == PGRES_COMMAND_OK || st == PGRES_TUPLES_OK)
            << sql << " -> " << PQerrorMessage(c_);
        PQclear(r);
    }
    std::string scalar(const std::string& sql) {
        PGresult* r = PQexec(c_, sql.c_str());
        std::string out = "NULL";
        if (PQresultStatus(r) == PGRES_TUPLES_OK && PQntuples(r) > 0 && !PQgetisnull(r, 0, 0)) {
            out = PQgetvalue(r, 0, 0);
        }
        PQclear(r);
        return out;
    }
    // Every row, each cell as text (NULL for NULL), sorted.
    std::vector<std::string> rows(const std::string& sql) {
        std::vector<std::string> out;
        PGresult* r = PQexec(c_, sql.c_str());
        EXPECT_EQ(PQresultStatus(r), PGRES_TUPLES_OK) << sql << " -> " << PQerrorMessage(c_);
        for (int i = 0; i < PQntuples(r); ++i) {
            std::string line;
            for (int j = 0; j < PQnfields(r); ++j) {
                line +=
                    (j > 0 ? "|" : "") +
                    (PQgetisnull(r, i, j) ? std::string{"NULL"} : std::string{PQgetvalue(r, i, j)});
            }
            out.push_back(std::move(line));
        }
        PQclear(r);
        std::sort(out.begin(), out.end());
        return out;
    }

private:
    PGconn* c_;
};

std::string unique(const char* stem) {
    static int n = 0;
    return std::string{stem} + "_" + std::to_string(static_cast<long>(::getpid())) + "_" +
           std::to_string(n++);
}

void fresh(Pg& db, const std::string& table, const std::string& definition) {
    db.exec("DROP TABLE IF EXISTS \"" + table + "\"");
    db.exec("CREATE TABLE \"" + table + "\" " + definition);
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
        std::forward<F>(f)();
    } catch (const std::exception& e) {
        return e.what();
    }
    return "";
}

const CheckpointBarrier kBarrier{clink::CheckpointId{1}};

PostgresJsonSinkOptions plain_opts(const std::string& table, std::vector<std::string> columns) {
    PostgresJsonSinkOptions o;
    o.conninfo = dsn();
    o.table = table;
    o.columns = std::move(columns);
    return o;
}

PostgresJsonUpsertSinkOptions upsert_opts(const std::string& table,
                                          std::vector<std::string> columns,
                                          std::vector<std::string> keys) {
    PostgresJsonUpsertSinkOptions o;
    o.conninfo = dsn();
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
        sql.replace(at, 2, "\"" + table + "\"");
    }
    return sql;
}

Change up(const std::string& k_json,
          const std::string& v_json,
          const std::string& k_sql,
          const std::string& v_sql) {
    return Change{R"({"k":)" + k_json + R"(,"v":)" + v_json + "}",
                  "INSERT INTO %T (k, v) VALUES (" + k_sql + ", " + v_sql +
                      ") ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v"};
}
Change del(const std::string& k_json, const std::string& k_sql) {
    return Change{R"({"k":)" + k_json + R"(,"v":null,"__row_kind":"delete"})",
                  "DELETE FROM %T WHERE k = " + k_sql};
}

// Applies `changes` to a fresh table of `definition` through the upsert sink,
// `per_flush` changes to a flush, and one statement at a time to a reference
// table of the same definition, and returns both tables' rows.
std::pair<std::vector<std::string>, std::vector<std::string>> keyed_case(
    const std::string& definition, const std::vector<Change>& changes, std::size_t per_flush) {
    Pg db;
    const auto t = unique("snk_key");
    const auto ref = unique("snk_ref");
    fresh(db, t, definition);
    fresh(db, ref, definition);
    PostgresJsonUpsertSink sink(upsert_opts(t, {"k", "v"}, {"k"}));
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
    return {db.rows("SELECT k::text, v FROM \"" + t + "\""),
            db.rows("SELECT k::text, v FROM \"" + ref + "\"")};
}

void expect_keyed(const std::string& definition, const std::vector<Change>& changes) {
    for (const std::size_t per_flush : {changes.size(), std::size_t{1}}) {
        const auto [got, want] = keyed_case(definition, changes, per_flush);
        EXPECT_EQ(got, want) << definition << ", " << per_flush << " change(s) per flush";
    }
}

}  // namespace

// ---- keys the table treats as one -----------------------------------------------------

TEST(PgSinkContractLive, ANondeterministicCollationKeyEndsAsTheLastChangeLeftIt) {
    REQUIRE_PG();
    Pg db;
    db.exec(
        "CREATE COLLATION IF NOT EXISTS clink_ci (provider = icu, locale = 'und-u-ks-level2', "
        "deterministic = false)");
    const std::string ci = "(k text COLLATE clink_ci PRIMARY KEY, v text)";
    expect_keyed(ci, {del(R"("abc")", "'abc'"), up(R"("ABC")", R"("y")", "'ABC'", "'y'")});
    expect_keyed(ci, {up(R"("abc")", R"("x")", "'abc'", "'x'"), del(R"("ABC")", "'ABC'")});
    expect_keyed(ci,
                 {up(R"("Abc")", R"("1")", "'Abc'", "'1'"),
                  up(R"("aBC")", R"("2")", "'aBC'", "'2'"),
                  up(R"("cafe")", R"("3")", "'cafe'", "'3'")});
}

TEST(PgSinkContractLive, CharKeysIgnoreTrailingSpacesAsTheTableDoes) {
    REQUIRE_PG();
    const std::string def = "(k char(8) PRIMARY KEY, v text)";
    expect_keyed(def,
                 {up(R"("a")", R"("1")", "'a'", "'1'"),
                  up(R"("a ")", R"("2")", "'a '", "'2'"),
                  del(R"("a")", "'a'"),
                  up(R"("b  ")", R"("3")", "'b  '", "'3'")});
}

TEST(PgSinkContractLive, NumericKeysInEveryJsonSpelling) {
    REQUIRE_PG();
    const std::string i = "(k int PRIMARY KEY, v text)";
    expect_keyed(i,
                 {up("1", R"("a")", "1", "'a'"),
                  del(R"("1")", "'1'"),
                  up("1.0", R"("b")", "1", "'b'"),
                  up("10e-1", R"("c")", "1", "'c'"),
                  up(R"("1")", R"("d")", "'1'", "'d'"),
                  up("2", R"("e")", "2", "'e'")});
    const std::string d = "(k numeric(10,2) PRIMARY KEY, v text)";
    expect_keyed(d,
                 {up("1.5", R"("a")", "1.5", "'a'"),
                  del(R"("1.50")", "'1.50'"),
                  up("15e-1", R"("b")", "1.5", "'b'"),
                  up(R"("1.5")", R"("c")", "'1.5'", "'c'")});
}

TEST(PgSinkContractLive, CompositeAndTimestampKeys) {
    REQUIRE_PG();
    Pg db;
    const auto t = unique("snk_comp");
    fresh(db, t, "(a text, b int, at timestamp(3), v int, PRIMARY KEY (a, b, at))");
    auto o = upsert_opts(t, {"a", "b", "at", "v"}, {"a", "b", "at"});
    o.timestamp_columns = {{"at", clink::sqljson::TimeKind::Timestamp}};
    PostgresJsonUpsertSink sink(o);
    sink.open();
    sink.on_data(batch({R"({"a":"x","b":1,"at":1792863688105,"v":1})",
                        R"({"a":"x","b":1,"at":1792863688105,"v":2})",
                        R"({"a":"x","b":2,"at":1792863688105,"v":3})",
                        R"({"a":"x","b":2,"at":1792863688105,"v":0,"__row_kind":"delete"})",
                        R"({"a":"y","b":1,"at":1792863688106,"v":4})"}));
    sink.on_barrier(kBarrier);
    EXPECT_EQ(db.rows("SELECT a, b, to_char(at, 'YYYY-MM-DD HH24:MI:SS.MS'), v FROM \"" + t + "\""),
              (std::vector<std::string>{"x|1|2026-10-24 17:41:28.105|2",
                                        "y|1|2026-10-24 17:41:28.106|4"}));
}

// Two stream keys of one row in one flush ('1' and 1 in an integer column):
// PostgreSQL refuses them in one INSERT, and the sink writes them a row to a
// statement, in order.
TEST(PgSinkContractLive, TwoKeysOfOneRowInOneFlushEndAsTheLaterLeftIt) {
    REQUIRE_PG();
    Pg db;
    const auto t = unique("snk_two_keys");
    fresh(db, t, "(k int PRIMARY KEY, v text)");
    PostgresJsonUpsertSink sink(upsert_opts(t, {"k", "v"}, {"k"}));
    sink.open();
    sink.on_data(batch({R"({"k":1,"v":"first"})", R"({"k":"1","v":"second"})"}));
    sink.on_barrier(kBarrier);
    EXPECT_EQ(db.rows("SELECT k, v FROM \"" + t + "\""), (std::vector<std::string>{"1|second"}));
    // The ordinary sink in on_conflict='update' likewise, and a repeated key.
    const auto p = unique("snk_two_keys_plain");
    fresh(db, p, "(k int PRIMARY KEY, v text)");
    auto o = plain_opts(p, {"k", "v"});
    o.on_conflict = "update";
    o.conflict_columns = {"k"};
    PostgresJsonSink plain(o);
    plain.open();
    plain.on_data(batch({R"({"k":1,"v":"a"})",
                         R"({"k":2,"v":"b"})",
                         R"({"k":"1","v":"c"})",
                         R"({"k":2,"v":"d"})"}));
    plain.on_barrier(kBarrier);
    EXPECT_EQ(db.rows("SELECT k, v FROM \"" + p + "\""), (std::vector<std::string>{"1|c", "2|d"}));
}

// ---- tables the sinks refuse ----------------------------------------------------------

TEST(PgSinkContractLive, TablesWhoseIdentityTheSinksCannotHonourAreRefused) {
    REQUIRE_PG();
    Pg db;
    const auto keyless = unique("snk_keyless");
    fresh(db, keyless, "(k int, v int)");
    PostgresJsonUpsertSink upsert(upsert_opts(keyless, {"k", "v"}, {"k"}));
    EXPECT_NE(thrown([&] { upsert.open(); }).find("no PRIMARY KEY or UNIQUE index"),
              std::string::npos);
    auto o = plain_opts(keyless, {"k", "v"});
    o.on_conflict = "nothing";
    o.conflict_columns = {"k"};
    PostgresJsonSink nothing(o);
    EXPECT_NE(thrown([&] { nothing.open(); }).find("no PRIMARY KEY or UNIQUE index"),
              std::string::npos);

    const auto partial = unique("snk_partial");
    fresh(db, partial, "(k int, v int)");
    db.exec("CREATE UNIQUE INDEX \"" + partial + "_k\" ON \"" + partial + "\" (k) WHERE v > 0");
    PostgresJsonUpsertSink on_partial(upsert_opts(partial, {"k", "v"}, {"k"}));
    EXPECT_NE(thrown([&] { on_partial.open(); }).find("covers part of the rows"),
              std::string::npos);

    const auto expr = unique("snk_expr");
    fresh(db, expr, "(k text, v int)");
    db.exec("CREATE UNIQUE INDEX \"" + expr + "_k\" ON \"" + expr + "\" (lower(k))");
    PostgresJsonUpsertSink on_expr(upsert_opts(expr, {"k", "v"}, {"k"}));
    EXPECT_NE(thrown([&] { on_expr.open(); }).find("no PRIMARY KEY or UNIQUE index"),
              std::string::npos);

    const auto base = unique("snk_base");
    fresh(db,
          base,
          "(k int PRIMARY KEY, v int, g int GENERATED ALWAYS AS (v * 2) STORED, "
          "i int GENERATED ALWAYS AS IDENTITY)");
    db.exec("CREATE VIEW \"" + base + "_v\" AS SELECT k, v FROM \"" + base + "\"");
    PostgresJsonUpsertSink on_view(upsert_opts(base + "_v", {"k", "v"}, {"k"}));
    EXPECT_NE(thrown([&] { on_view.open(); }).find("a view"), std::string::npos);
    PostgresJsonSink through_view(plain_opts(base + "_v", {"k", "v"}));
    EXPECT_NO_THROW(through_view.open());  // an INSERT through an updatable view
    PostgresJsonSink generated(plain_opts(base, {"k", "v", "g"}));
    EXPECT_NE(thrown([&] { generated.open(); }).find("generated column"), std::string::npos);
    PostgresJsonSink identity(plain_opts(base, {"k", "v", "i"}));
    EXPECT_NE(thrown([&] { identity.open(); }).find("GENERATED ALWAYS AS IDENTITY"),
              std::string::npos);

    db.exec(
        "CREATE COLLATION IF NOT EXISTS clink_ci (provider = icu, locale = 'und-u-ks-level2', "
        "deterministic = false)");
    const auto ci = unique("snk_ci");
    fresh(db, ci, "(k text COLLATE clink_ci PRIMARY KEY, v int)");
    auto parallel = upsert_opts(ci, {"k", "v"}, {"k"});
    parallel.parallelism = 2;
    PostgresJsonUpsertSink two(parallel);
    EXPECT_NE(thrown([&] { two.open(); }).find("clink_ci"), std::string::npos);
    const auto exact = unique("snk_exact");
    fresh(db, exact, "(k text PRIMARY KEY, v int)");
    parallel = upsert_opts(exact, {"k", "v"}, {"k"});
    parallel.parallelism = 2;
    PostgresJsonUpsertSink fine(parallel);
    EXPECT_NO_THROW(fine.open());

    PostgresJsonSink missing(plain_opts(unique("snk_absent"), {"k"}));
    EXPECT_NE(thrown([&] { missing.open(); }).find("does not exist"), std::string::npos);
}

// ---- values -----------------------------------------------------------------------------

TEST(PgSinkContractLive, ValuesLandExactly) {
    REQUIRE_PG();
    Pg db;
    const auto t = unique("snk_vals");
    fresh(db,
          t,
          "(id int PRIMARY KEY, big bigint, d numeric(60,30), huge numeric, s text, b bytea, "
          "j jsonb, at timestamp(6), ts timestamptz(3), f double precision, flag boolean)");
    auto o = plain_opts(t, {"id", "big", "d", "huge", "s", "b", "j", "at", "ts", "f", "flag"});
    o.timestamp_columns = {{"at", clink::sqljson::TimeKind::Timestamp},
                           {"ts", clink::sqljson::TimeKind::TimestampTz}};
    PostgresJsonSink sink(o);
    sink.open();
    sink.on_data(batch({
        R"({"id":1,"big":9223372036854775807,"d":123456789012345678901234567890.123456789012345678901234567891,"huge":123456789012345678901234567890123456789012345678901234567890,"s":"caf\u00e9 \ud83d\ude00 'q' \\b","b":"a\u0000\u0007z","j":{"x":[1,2],"y":"z"},"at":-62135596800000,"ts":1000,"f":0.1,"flag":true})",
        R"({"id":2,"big":-9223372036854775808,"d":-0.000000000000000000000000000001,"huge":1e100,"s":"","b":"","j":[],"at":253402300799999,"ts":4102444800123,"f":-1.5e300,"flag":false})",
        R"({"id":3,"big":null,"d":null,"huge":null,"s":null,"b":null,"j":null,"at":null,"ts":null,"f":null})",
    }));
    sink.on_barrier(kBarrier);
    EXPECT_EQ(
        db.rows("SELECT id, big, d, huge, encode(convert_to(s, 'UTF8'), 'hex'), encode(b, 'hex'), "
                "j::text, to_char(at, 'YYYY-MM-DD HH24:MI:SS.US'), "
                "(extract(epoch from ts) * 1000)::bigint, f, flag FROM \"" +
                t + "\""),
        (std::vector<std::string>{
            "1|9223372036854775807|123456789012345678901234567890.123456789012345678901234567891|"
            "123456789012345678901234567890123456789012345678901234567890|"
            "636166c3a920f09f988020277127205c62|6100077a|{\"x\": [1, 2], \"y\": \"z\"}|"
            "0001-01-01 00:00:00.000000|1000|0.1|t",
            "2|-9223372036854775808|-0.000000000000000000000000000001|"
            "1" +
                std::string(100, '0') +
                "|||[]|9999-12-31 23:59:59.999000|4102444800123|-1.5e+300|f",
            "3|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL"}));
}

TEST(PgSinkContractLive, NullAndMissingMembersBothWriteNullButAKeyMustBeThere) {
    REQUIRE_PG();
    Pg db;
    const auto t = unique("snk_null");
    fresh(db, t, "(k int PRIMARY KEY, v text DEFAULT 'dflt', w int)");
    PostgresJsonUpsertSink sink(upsert_opts(t, {"k", "v", "w"}, {"k"}));
    sink.open();
    sink.on_data(batch({R"({"k":1,"v":"a","w":1})", R"({"k":2,"v":"b","w":2})"}));
    sink.on_barrier(kBarrier);
    sink.on_data(batch({R"({"k":1,"v":null,"w":10})", R"({"k":2,"w":20})"}));
    sink.on_barrier(kBarrier);
    EXPECT_EQ(db.rows("SELECT k, v, w FROM \"" + t + "\""),
              (std::vector<std::string>{"1|NULL|10", "2|NULL|20"}));
    EXPECT_NE(thrown([&] { sink.on_data(batch({R"({"v":"x"})"})); }).find("key member \"k\""),
              std::string::npos);
}

TEST(PgSinkContractLive, AValueTheServerRefusesFailsTheFlushAndCommitsNothing) {
    REQUIRE_PG();
    struct Bad {
        std::string definition;
        std::string good;  // a row the column holds
        std::string record;
        std::string expect;
    };
    for (const auto& c : std::vector<Bad>{
             {"(id int PRIMARY KEY, x smallint)",
              R"({"id":1,"x":7})",
              R"({"id":2,"x":300000})",
              "22003"},
             {"(id int PRIMARY KEY, x int)",
              R"({"id":1,"x":1})",
              R"({"id":2,"x":"12abc"})",
              "22P02"},
             {"(id int PRIMARY KEY, x date)",
              R"({"id":1,"x":"2026-02-28"})",
              R"({"id":2,"x":"2026-02-30"})",
              "22008"},
             {"(id int PRIMARY KEY, x int NOT NULL)",
              R"({"id":1,"x":1})",
              R"({"id":2,"x":null})",
              "23502"},
             {"(id int PRIMARY KEY, x int CHECK (x > 0))",
              R"({"id":1,"x":1})",
              R"({"id":2,"x":-1})",
              "23514"},
         }) {
        Pg db;
        const auto t = unique("snk_bad");
        fresh(db, t, c.definition);
        PostgresJsonSink sink(plain_opts(t, {"id", "x"}));
        sink.open();
        // The good row shares the flush's transaction, so it must not land either.
        sink.on_data(batch({c.good, c.record}));
        const auto why = thrown([&] { sink.on_barrier(kBarrier); });
        EXPECT_NE(why.find(c.expect), std::string::npos) << c.definition << ": " << why;
        EXPECT_EQ(why.find("12abc"), std::string::npos) << why;
        EXPECT_EQ(why.find("2026-02-30"), std::string::npos) << why;
        EXPECT_EQ(db.scalar("SELECT count(*) FROM \"" + t + "\""), "0") << c.definition;
        EXPECT_NE(thrown([&] { sink.on_barrier(kBarrier); }).find("failed earlier"),
                  std::string::npos);
        sink.open();
        sink.on_data(batch({c.good}));
        sink.on_barrier(kBarrier);
        EXPECT_EQ(db.scalar("SELECT count(*) FROM \"" + t + "\""), "1") << c.definition;
    }
}

TEST(PgSinkContractLive, AValueTheColumnWouldRoundOrCutIsRefusedBeforeItIsWritten) {
    REQUIRE_PG();
    struct Bad {
        std::string definition;
        std::string record;
    };
    for (const auto& c : std::vector<Bad>{
             {"(id int PRIMARY KEY, x numeric(10,2))", R"({"id":2,"x":1.234})"},
             {"(id int PRIMARY KEY, x numeric(10,2))", R"({"id":2,"x":"1.234"})"},
             {"(id int PRIMARY KEY, x int)", R"({"id":2,"x":1.5})"},
             {"(id int PRIMARY KEY, x varchar(3))", R"({"id":2,"x":"abcd"})"},
             {"(id int PRIMARY KEY, x varchar(3))", R"({"id":2,"x":"abc  "})"},
             {"(id int PRIMARY KEY, x text)", R"({"id":2,"x":"a\u0000b"})"},
         }) {
        Pg db;
        const auto t = unique("snk_round");
        fresh(db, t, c.definition);
        PostgresJsonSink sink(plain_opts(t, {"id", "x"}));
        sink.open();
        sink.on_data(batch({R"({"id":1,"x":1})"}));
        EXPECT_NE(thrown([&] { sink.on_data(batch({c.record})); }).find("cannot be written"),
                  std::string::npos)
            << c.definition << " " << c.record;
        EXPECT_EQ(db.scalar("SELECT count(*) FROM \"" + t + "\""), "0") << c.definition;
    }
}

// The server's and database's settings do not reach what the sink writes: its
// session is UTC, UTF8 and standard_conforming_strings, whatever they say.
TEST(PgSinkContractLive, TheSessionIgnoresTheDatabasesSettings) {
    REQUIRE_PG();
    Pg db;
    db.exec(
        "DO $$ BEGIN EXECUTE format('ALTER DATABASE %I SET timezone = %L', current_database(), "
        "'America/New_York'); EXECUTE format('ALTER DATABASE %I SET "
        "standard_conforming_strings = off', current_database()); END $$");
    const auto t = unique("snk_session");
    fresh(db, t, "(id int PRIMARY KEY, s text, at timestamp(3), tz timestamptz(3))");
    auto o = plain_opts(t, {"id", "s", "at", "tz"});
    o.timestamp_columns = {{"at", clink::sqljson::TimeKind::Timestamp},
                           {"tz", clink::sqljson::TimeKind::TimestampTz}};
    PostgresJsonSink sink(o);
    const auto run = [&] {
        sink.open();
        sink.on_data(batch({R"({"id":1,"s":"a\\b'c\"d","at":1792863688105,"tz":1792863688105})"}));
        sink.on_barrier(kBarrier);
        sink.close();
    };
    run();
    db.exec(
        "DO $$ BEGIN EXECUTE format('ALTER DATABASE %I RESET timezone', current_database()); "
        "EXECUTE format('ALTER DATABASE %I RESET standard_conforming_strings', "
        "current_database()); END $$");
    EXPECT_EQ(db.scalar("SELECT encode(convert_to(s, 'UTF8'), 'hex') FROM \"" + t + "\""),
              "615c6227632264");
    EXPECT_EQ(db.scalar("SELECT to_char(at, 'YYYY-MM-DD HH24:MI:SS.MS') FROM \"" + t + "\""),
              "2026-10-24 17:41:28.105");
    EXPECT_EQ(db.scalar("SELECT (extract(epoch from tz) * 1000)::bigint FROM \"" + t + "\""),
              "1792863688105");
}

// ---- transactions, sizes, locks and cancels -------------------------------------------

TEST(PgSinkContractLive, AFlushOfManyStatementsIsOneTransaction) {
    REQUIRE_PG();
    Pg db;
    const auto t = unique("snk_tx");
    fresh(db, t, "(id int PRIMARY KEY, x text)");
    auto o = plain_opts(t, {"id", "x"});
    o.max_statement_bytes = 2048;
    o.max_record_bytes = 1024;
    PostgresJsonSink sink(o);
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
    EXPECT_NE(why.find("23505"), std::string::npos) << why;
    EXPECT_EQ(why.find("(id)=(0)"), std::string::npos) << why;  // the DETAIL is never read
    EXPECT_EQ(db.scalar("SELECT count(*) FROM \"" + t + "\""), "0");
    sink.open();
    records.pop_back();
    sink.on_data(batch(records));
    sink.on_barrier(kBarrier);
    EXPECT_EQ(db.scalar("SELECT count(*) FROM \"" + t + "\""), "40");
    EXPECT_EQ(db.scalar("SELECT count(*) FROM \"" + t + "\" WHERE length(x) = 500"), "40");
}

TEST(PgSinkContractLive, ALockedRowFailsTheFlushWithinItsBound) {
    REQUIRE_PG();
    Pg db;
    const auto t = unique("snk_lock");
    fresh(db, t, "(k int PRIMARY KEY, v int)");
    db.exec("INSERT INTO \"" + t + "\" VALUES (1, 0)");
    Pg holder;
    holder.exec("BEGIN");
    holder.exec("UPDATE \"" + t + "\" SET v = 99 WHERE k = 1");
    auto o = upsert_opts(t, {"k", "v"}, {"k"});
    o.lock_wait_timeout = std::chrono::seconds{1};
    o.max_retries = 1;
    PostgresJsonUpsertSink sink(o);
    sink.open();
    sink.on_data(batch({R"({"k":1,"v":1})"}));
    const auto started = std::chrono::steady_clock::now();
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    const auto waited = std::chrono::steady_clock::now() - started;
    EXPECT_NE(why.find("gave up after 1 retries"), std::string::npos) << why;
    EXPECT_NE(why.find("55P03"), std::string::npos) << why;
    EXPECT_LT(waited, std::chrono::seconds{10});
    holder.exec("ROLLBACK");
    EXPECT_EQ(db.rows("SELECT k, v FROM \"" + t + "\""), (std::vector<std::string>{"1|0"}));
}

TEST(PgSinkContractLive, ACancelEndsAFlushWaitingOnALock) {
    REQUIRE_PG();
    Pg db;
    const auto t = unique("snk_cancel");
    fresh(db, t, "(k int PRIMARY KEY, v int)");
    db.exec("INSERT INTO \"" + t + "\" VALUES (1, 0)");
    Pg holder;
    holder.exec("BEGIN");
    holder.exec("UPDATE \"" + t + "\" SET v = 99 WHERE k = 1");
    PostgresJsonUpsertSink sink(upsert_opts(t, {"k", "v"}, {"k"}));  // waits up to 30 s
    auto cancel = std::make_shared<std::atomic<bool>>(false);
    clink::RuntimeContext ctx(clink::OperatorId{78}, "postgres_upsert_sink", nullptr, nullptr);
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
    EXPECT_EQ(db.rows("SELECT k, v FROM \"" + t + "\""), (std::vector<std::string>{"1|0"}));
}

// A connection ended while its COMMIT is on the way: the sink reports that the
// flush may or may not have been committed. Here the server rolled it back,
// which the table shows; the sink does not claim either.
TEST(PgSinkContractLive, ACommitWhoseReplyIsLostIsReportedAsUnknown) {
    REQUIRE_PG();
#if !CLINK_FAULT_ENABLED
    GTEST_SKIP() << "needs a build with fault injection";
#else
    Pg db;
    const auto t = unique("snk_lost");
    fresh(db, t, "(k int PRIMARY KEY, v int)");
    PostgresJsonSink sink(plain_opts(t, {"k", "v"}));
    sink.open();
    clink::fault::ScopedFault park("postgres.sink_before_commit=delay:1500");
    sink.on_data(batch({R"({"k":1,"v":1})"}));
    std::thread killer([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds{500});
        Pg admin;
        admin.exec(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = "
            "current_database() AND pid <> pg_backend_pid() AND state = 'idle in "
            "transaction'");
    });
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    killer.join();
    EXPECT_NE(why.find("may or may not have been committed"), std::string::npos) << why;
    EXPECT_EQ(db.scalar("SELECT count(*) FROM \"" + t + "\""), "0");
#endif
}

// ---- the factories' path and the C++ sink ---------------------------------------------

TEST(PgSinkContractLive, TheFactoriesBuildSinksThatWrite) {
    REQUIRE_PG();
    Pg db;
    const auto a = unique("snk_fa");
    const auto k = unique("snk_fk");
    fresh(db, a, "(id int PRIMARY KEY, v text)");
    fresh(db, k, "(id int PRIMARY KEY, v text)");
    for (const auto& [type, table] : std::vector<std::pair<std::string, std::string>>{
             {"postgres_sink", a}, {"postgres_upsert_sink", k}}) {
        const auto* f = clink::cluster::OperatorRegistry::default_instance().find_sink(
            type, std::string{clink::cluster::kChannelString});
        ASSERT_NE(f, nullptr);
        clink::cluster::OperatorBuildContext ctx;
        ctx.params = {{"conninfo", dsn()},
                      {"table", table},
                      {"schema_columns", "id:i64;v:str"},
                      {"key_columns", "id"},
                      {"batch_records", "2"}};
        auto sink = std::static_pointer_cast<clink::Sink<std::string>>(f->build(ctx));
        sink->open();
        sink->on_data(batch({R"({"id":1,"v":"a"})", R"({"id":2,"v":"b"})", R"({"id":3,"v":"c"})"}));
        sink->on_barrier(kBarrier);
        sink->close();
        EXPECT_EQ(db.rows("SELECT id, v FROM \"" + table + "\""),
                  (std::vector<std::string>{"1|a", "2|b", "3|c"}))
            << type;
    }
}

TEST(PgSinkContractLive, ThePostgresSinkWritesItsStatementsAndStaysFailed) {
    REQUIRE_PG();
    Pg db;
    const auto t = unique("snk_cpp");
    fresh(db, t, "(id int PRIMARY KEY, v text)");
    clink::PostgresSink::Options o;
    o.conninfo = dsn();
    o.sql = "INSERT INTO \"" + t + "\" (id, v) VALUES ($1, $2)";
    clink::PostgresSink sink(o);
    sink.open();
    Batch<std::vector<std::string>> rows;
    rows.emplace(std::vector<std::string>{"1", "it's"});
    rows.emplace(std::vector<std::string>{"2", "b"});
    sink.on_data(rows);
    sink.on_barrier(kBarrier);
    EXPECT_EQ(db.rows("SELECT id, v FROM \"" + t + "\""),
              (std::vector<std::string>{"1|it's", "2|b"}));
    Batch<std::vector<std::string>> dup;
    dup.emplace(std::vector<std::string>{"3", "c"});
    dup.emplace(std::vector<std::string>{"1", "again"});
    sink.on_data(dup);
    EXPECT_NE(thrown([&] { sink.on_barrier(kBarrier); }).find("23505"), std::string::npos);
    EXPECT_NE(thrown([&] { sink.flush(); }).find("failed earlier"), std::string::npos);
    EXPECT_EQ(db.scalar("SELECT count(*) FROM \"" + t + "\""), "2");
    EXPECT_NO_THROW(sink.close());
}

#endif  // CLINK_HAS_POSTGRES
