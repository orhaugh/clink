// The PostgreSQL sinks' pure rules, without a server: the destination contract
// each mode checks, the rendering of values and keys as PostgreSQL literals,
// the statements' heads and tails, and the redaction of record values from
// server messages. The shared record rules have their own tests
// (tests/test_sink_record_rules.cpp).

#include <limits>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/postgres/pg_sink_rules.hpp"

using clink::postgres::JsonMember;
using clink::postgres::JsonMembers;
using clink::postgres::PgColumn;
using clink::postgres::PgSinkMode;
using clink::postgres::PgSinkTarget;
using clink::postgres::PgTable;
using clink::postgres::PgUniqueIndex;
using clink::postgres::RecordRefused;
using clink::sqljson::TimeKind;

namespace {

std::string esc(std::string_view s) {
    std::string out;
    for (const char c : s) {
        if (c == '\'') {
            out.push_back('\'');
        }
        out.push_back(c);
    }
    return out;
}

JsonMembers read(const std::string& json) {
    JsonMembers m;
    const auto why = clink::postgres::read_json_object(json, m);
    EXPECT_TRUE(why.empty()) << why;
    return m;
}

PgColumn column(std::string name, std::string type_name, std::string type = {}) {
    PgColumn c;
    c.name = std::move(name);
    c.type = type.empty() ? type_name : std::move(type);
    c.type_name = std::move(type_name);
    return c;
}

PgColumn numeric(std::string name, int scale) {
    auto c = column(std::move(name), "numeric", "numeric(20," + std::to_string(scale) + ")");
    c.numeric_constrained = true;
    c.numeric_scale = scale;
    return c;
}

PgUniqueIndex index(std::string name, std::vector<std::string> columns, bool primary = false) {
    PgUniqueIndex u;
    u.name = std::move(name);
    u.columns = std::move(columns);
    u.primary = primary;
    return u;
}

PgTable table_with(std::vector<PgColumn> cols, std::vector<PgUniqueIndex> unique = {}) {
    PgTable t;
    t.exists = true;
    t.kind = 'r';
    t.columns = std::move(cols);
    t.unique_indexes = std::move(unique);
    return t;
}

std::string lit(const std::string& member_json,
                const PgColumn* col,
                TimeKind time = TimeKind::None) {
    const auto m = read("{\"v\":" + member_json + "}");
    return clink::postgres::render_pg_literal(m.at("v"), col, time, esc);
}

std::vector<std::string> reasons(const PgSinkTarget& t, const PgTable& s) {
    return clink::postgres::check_pg_target(t, s);
}

bool mentions(const std::vector<std::string>& v, const std::string& needle) {
    for (const auto& x : v) {
        if (x.find(needle) != std::string::npos) {
            return true;
        }
    }
    return false;
}

}  // namespace

// ---- rendering --------------------------------------------------------------------------

TEST(PgSinkRender, LiteralsByKind) {
    EXPECT_EQ(lit("null", nullptr), "NULL");
    EXPECT_EQ(lit("true", nullptr), "TRUE");
    EXPECT_EQ(lit("false", nullptr), "FALSE");
    EXPECT_EQ(lit(R"("it's")", nullptr), "'it''s'");
    EXPECT_EQ(lit("1.5E3", nullptr), "1500");
    EXPECT_EQ(lit("18446744073709551615123", nullptr), "18446744073709551615123");
    EXPECT_EQ(lit(R"({"a":1})", nullptr), "'{\"a\":1}'");
    // Past a plain decimal of 4096 characters the token itself is written:
    // PostgreSQL reads it as an exact numeric too.
    EXPECT_EQ(lit("1e5000", nullptr), "1e5000");
}

TEST(PgSinkRender, IntegerAndNumericColumnsRefuseWhatTheyWouldRound) {
    const auto i = column("i", "int8", "bigint");
    EXPECT_EQ(lit("12", &i), "12");
    EXPECT_EQ(lit("1.20e1", &i), "12");
    EXPECT_THROW((void)lit("1.5", &i), RecordRefused);
    const auto n = numeric("n", 2);
    EXPECT_EQ(lit("1.25", &n), "1.25");
    EXPECT_EQ(lit("1.250", &n), "1.25");
    EXPECT_THROW((void)lit("1.234", &n), RecordRefused);
    // A number written as a string rounds just the same.
    EXPECT_EQ(lit(R"(" 1.25 ")", &n), "' 1.25 '");
    EXPECT_THROW((void)lit(R"("1.234")", &n), RecordRefused);
    EXPECT_EQ(lit(R"("not a number")", &n), "'not a number'");  // the server's to refuse
    // PostgreSQL 15's negative scale keeps multiples of a power of ten.
    const auto tens = numeric("t", -2);
    EXPECT_EQ(lit("1200", &tens), "1200");
    EXPECT_THROW((void)lit("1234", &tens), RecordRefused);
    // Plain numeric keeps every digit.
    const auto plain = column("p", "numeric");
    EXPECT_EQ(lit("0.000000000000000000000000000000000001", &plain),
              "0.000000000000000000000000000000000001");
    try {
        (void)lit("1.234", &n);
    } catch (const RecordRefused& e) {
        EXPECT_EQ(std::string{e.what()}.find("1.234"), std::string::npos) << e.what();
    }
}

TEST(PgSinkRender, NeededScaleCountsTheDigitsAfterThePoint) {
    using clink::postgres::needed_scale;
    EXPECT_EQ(needed_scale("1.25"), 2);
    EXPECT_EQ(needed_scale("15"), 0);
    EXPECT_EQ(needed_scale("1.50e1"), 0);
    EXPECT_EQ(needed_scale("150"), -1);
    EXPECT_EQ(needed_scale("-0.0001"), 4);
    EXPECT_EQ(needed_scale("1e-3"), 3);
    EXPECT_EQ(needed_scale("+2.5"), 1);
    EXPECT_EQ(needed_scale("0.000"), std::numeric_limits<std::int64_t>::min());
}

TEST(PgSinkRender, FloatColumnsTakeTheTokenAsWritten) {
    const auto f = column("f", "float8", "double precision");
    EXPECT_EQ(lit("-1.5e300", &f), "-1.5e300");
    EXPECT_EQ(lit("0.1", &f), "0.1");
    EXPECT_THROW((void)lit("1e400", &f), RecordRefused);
}

TEST(PgSinkRender, AStringHoldingNulIsRefused) {
    EXPECT_THROW((void)lit(R"("a\u0000b")", nullptr), RecordRefused);
}

TEST(PgSinkRender, VarcharLengthCountsCharactersNotBytes) {
    auto vc = column("s", "varchar", "character varying(4)");
    vc.char_length = 4;
    EXPECT_EQ(lit(R"("café")", &vc), "'caf\xc3\xa9'");  // five bytes, four characters
    EXPECT_THROW((void)lit(R"("cafes")", &vc), RecordRefused);
    // PostgreSQL would cut trailing spaces past the length without a word.
    EXPECT_THROW((void)lit(R"("abc  ")", &vc), RecordRefused);
    auto bp = column("c", "bpchar", "character(2)");
    bp.char_length = 2;
    EXPECT_EQ(lit(R"("ab")", &bp), "'ab'");
    EXPECT_THROW((void)lit(R"("abc")", &bp), RecordRefused);
}

TEST(PgSinkRender, ByteaTakesTheStringsBytes) {
    const auto b = column("b", "bytea");
    EXPECT_EQ(lit(R"("a\u0000b")", &b), "'\\x610062'");
    EXPECT_EQ(lit(R"("Aé")", &b), "'\\x41c3a9'");
}

TEST(PgSinkRender, TimestampsKeepTheirMillisecondsOrAreRefused) {
    auto at = column("at", "timestamp", "timestamp(3) without time zone");
    at.time_precision = 3;
    EXPECT_EQ(lit("1792863688105", &at, TimeKind::Timestamp), "'2026-10-24 17:41:28.105'");
    at.time_precision = -1;  // the default, microseconds
    EXPECT_EQ(lit("1792863688105", &at, TimeKind::Timestamp), "'2026-10-24 17:41:28.105'");
    at.time_precision = 0;
    EXPECT_THROW((void)lit("1792863688105", &at, TimeKind::Timestamp), RecordRefused);
    EXPECT_EQ(lit("1792863688000", &at, TimeKind::Timestamp), "'2026-10-24 17:41:28.000'");
    auto tz = column("tz", "timestamptz", "timestamp with time zone");
    EXPECT_EQ(lit("1792863688105", &tz, TimeKind::TimestampTz), "'2026-10-24 17:41:28.105+00'");
    EXPECT_THROW((void)lit("1792863688.5", &tz, TimeKind::TimestampTz), RecordRefused);
    EXPECT_THROW((void)lit("253402300800000", &tz, TimeKind::TimestampTz), RecordRefused);
}

TEST(PgSinkRender, RowsAndKeysNullMissingMembersButNeverKeys) {
    const auto s = table_with({column("id", "int4"), column("v", "text")});
    const auto m = read(R"({"id":7,"v":null,"secret":"x"})");
    EXPECT_EQ(clink::postgres::render_pg_row(m, {"id", "v", "w"}, s, {}, esc), "(7,NULL,NULL)");
    EXPECT_EQ(clink::postgres::render_pg_key(m, {"id"}, s, {}, esc), "7");
    EXPECT_EQ(clink::postgres::render_pg_key(read(R"({"a":1,"b":"x"})"), {"a", "b"}, s, {}, esc),
              "(1,'x')");
    try {
        (void)clink::postgres::render_pg_key(
            read(R"({"id":null,"v":"top secret"})"), {"id"}, s, {}, esc);
        ADD_FAILURE() << "a null key was rendered";
    } catch (const RecordRefused& e) {
        EXPECT_NE(std::string{e.what()}.find("\"id\""), std::string::npos) << e.what();
        EXPECT_EQ(std::string{e.what()}.find("top secret"), std::string::npos) << e.what();
    }
}

TEST(PgSinkRender, KeysAreWrittenAsTheirColumnsCompare) {
    using clink::postgres::check_pg_key_spelling;
    auto t = column("t", "text");
    const auto s = table_with({column("n", "int8"), numeric("d", 2), column("at", "timestamp"), t});
    const std::vector<std::string> keys = {"n", "d", "at", "t"};
    EXPECT_NO_THROW(check_pg_key_spelling(read(R"({"n":1,"d":1.5,"at":1000,"t":"x"})"), keys, s));
    for (const char* bad : {R"({"n":"1","d":1.5,"at":1000,"t":"x"})",
                            R"({"n":1,"d":"1.5","at":1000,"t":"x"})",
                            R"({"n":1,"d":1.5,"at":"1970-01-01","t":"x"})",
                            R"({"n":1,"d":1.5,"at":1000,"t":1})"}) {
        EXPECT_THROW(check_pg_key_spelling(read(bad), keys, s), RecordRefused) << bad;
    }
}

// ---- statements -------------------------------------------------------------------------

TEST(PgSinkStatements, HeadsAndTails) {
    using namespace clink::postgres;
    EXPECT_EQ(pg_insert_head("public.t", {"a", "b"}),
              R"(INSERT INTO "public"."t" ("a","b") VALUES )");
    EXPECT_EQ(pg_update_tail({"k"}, {"a", "b"}),
              R"( ON CONFLICT ("k") DO UPDATE SET "a" = EXCLUDED."a","b" = EXCLUDED."b")");
    EXPECT_EQ(pg_update_tail({"k"}, {}), R"( ON CONFLICT ("k") DO NOTHING)");
    EXPECT_EQ(pg_nothing_tail({"a", "b"}), R"( ON CONFLICT ("a","b") DO NOTHING)");
    EXPECT_EQ(pg_delete_head("t", {"k"}), R"(DELETE FROM "t" WHERE "k" IN ()");
    EXPECT_EQ(pg_delete_head("t", {"a", "b"}), R"(DELETE FROM "t" WHERE ("a","b") IN ()");
}

TEST(PgSinkStatements, ServerMessagesLoseTheirValues) {
    using clink::postgres::redact_pg_message;
    EXPECT_EQ(redact_pg_message(R"(invalid input syntax for type integer: "12abc")"),
              "invalid input syntax for type integer: <value>");
    EXPECT_EQ(redact_pg_message(R"(value "300000" is out of range for type smallint)"),
              "value <value> is out of range for type smallint");
    EXPECT_EQ(redact_pg_message(R"(date/time field value out of range: "2026-02-30")"),
              "date/time field value out of range: <value>");
    // Identifiers stay.
    EXPECT_EQ(redact_pg_message(R"(duplicate key value violates unique constraint "t_pkey")"),
              R"(duplicate key value violates unique constraint "t_pkey")");
    EXPECT_EQ(redact_pg_message(
                  R"(null value in column "v" of relation "t" violates not-null constraint)"),
              R"(null value in column "v" of relation "t" violates not-null constraint)");
}

// ---- the destination ------------------------------------------------------------------

TEST(PgSinkTarget, KeyIdentityIsExactOnlyWherePostgresComparesAsTheStreamDoes) {
    using clink::postgres::pg_key_identity_exact;
    EXPECT_TRUE(pg_key_identity_exact(column("i", "int4"), false));
    EXPECT_TRUE(pg_key_identity_exact(numeric("n", 2), false));
    EXPECT_TRUE(pg_key_identity_exact(column("t", "text"), false));
    auto ci = column("t", "text");
    ci.collation_deterministic = false;
    ci.collation = "und-u-ks-level2";
    EXPECT_FALSE(pg_key_identity_exact(ci, false));
    EXPECT_FALSE(pg_key_identity_exact(column("c", "bpchar"), false));
    EXPECT_FALSE(pg_key_identity_exact(column("u", "uuid"), false));
    EXPECT_FALSE(pg_key_identity_exact(column("d", "date"), false));
    auto at = column("at", "timestamptz");
    EXPECT_TRUE(pg_key_identity_exact(at, true));
    EXPECT_FALSE(pg_key_identity_exact(at, false));
    at.time_precision = 0;
    EXPECT_FALSE(pg_key_identity_exact(at, true));
}

TEST(PgSinkTarget, EveryModeChecksItsColumns) {
    PgSinkTarget t;
    t.table = "t";
    t.columns = {"id", "v", "g", "x", "at"};
    t.timestamp_columns = {{"at", TimeKind::Timestamp}};
    auto g = column("g", "int4");
    g.generated = true;
    auto x = column("x", "int8");
    x.identity_always = true;
    const auto s =
        table_with({column("id", "int4"), column("v", "text"), g, x, column("at", "date")});
    const auto why = reasons(t, s);
    EXPECT_TRUE(mentions(why, "'g' is a generated column"));
    EXPECT_TRUE(mentions(why, "'x' is GENERATED ALWAYS AS IDENTITY"));
    EXPECT_TRUE(mentions(why, "'at' carries a TIMESTAMP and is date"));
    t.columns = {"id", "missing"};
    t.timestamp_columns = {};
    EXPECT_TRUE(mentions(reasons(t, s), "has no column 'missing'"));
    EXPECT_TRUE(mentions(reasons(t, PgTable{}), "does not exist"));
}

TEST(PgSinkTarget, KeyedModesNeedATableAndAnIndexOnExactlyTheirKey) {
    PgSinkTarget t;
    t.table = "t";
    t.columns = {"a", "b", "v"};
    t.mode = PgSinkMode::OnConflictUpdate;
    t.conflict_columns = {"b", "a"};
    const std::vector<PgColumn> cols = {
        column("a", "int4"), column("b", "int4"), column("v", "text")};
    EXPECT_TRUE(reasons(t, table_with(cols, {index("t_pkey", {"a", "b"}, true)})).empty());
    EXPECT_TRUE(mentions(reasons(t, table_with(cols, {index("t_a", {"a"})})), "no PRIMARY KEY"));
    auto partial = index("t_ab", {"a", "b"});
    partial.partial = true;
    const auto why = reasons(t, table_with(cols, {partial}));
    EXPECT_TRUE(mentions(why, "t_ab covers part of the rows"));
    // Another unique index is PostgreSQL's to enforce: it is not refused.
    EXPECT_TRUE(
        reasons(t, table_with(cols, {index("t_pkey", {"a", "b"}, true), index("t_v", {"v"})}))
            .empty());
    auto view = table_with(cols, {index("t_pkey", {"a", "b"}, true)});
    view.kind = 'v';
    EXPECT_TRUE(mentions(reasons(t, view), "a view"));
    t.mode = PgSinkMode::Append;
    EXPECT_TRUE(reasons(t, view).empty());  // INSERT through an updatable view
    t.mode = PgSinkMode::OnConflictNothing;
    t.columns = {"a", "v"};
    EXPECT_TRUE(mentions(reasons(t, table_with(cols, {index("t_pkey", {"a", "b"}, true)})),
                         "key column 'b' is not one of the columns"));
}

TEST(PgSinkTarget, ParallelChangelogNeedsExactKeys) {
    PgSinkTarget t;
    t.table = "t";
    t.columns = {"k", "v"};
    t.mode = PgSinkMode::Changelog;
    t.conflict_columns = {"k"};
    t.parallelism = 2;
    auto ci = column("k", "text");
    ci.collation_deterministic = false;
    ci.collation = "case_insensitive";
    const auto why =
        reasons(t, table_with({ci, column("v", "text")}, {index("t_pkey", {"k"}, true)}));
    EXPECT_TRUE(mentions(why, "case_insensitive"));
    EXPECT_TRUE(reasons(t,
                        table_with({column("k", "text"), column("v", "text")},
                                   {index("t_pkey", {"k"}, true)}))
                    .empty());
    t.parallelism = 1;
    EXPECT_TRUE(
        reasons(t, table_with({ci, column("v", "text")}, {index("t_pkey", {"k"}, true)})).empty());
}
