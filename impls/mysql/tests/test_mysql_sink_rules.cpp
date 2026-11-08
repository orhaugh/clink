// The MySQL sinks' pure rules, without a server: the strict reading of a record,
// exact numbers, the destination contract each mode checks, the rendering of
// values and keys, the changelog marker, statement packing, the changelog
// buffer's order, and the redaction of record values from server messages.

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/mysql/mysql_sink_rules.hpp"

using clink::mysql::ChangeKind;
using clink::mysql::ChangelogBuffer;
using clink::mysql::ColumnInfo;
using clink::mysql::JsonMember;
using clink::mysql::JsonMembers;
using clink::mysql::RecordRefused;
using clink::mysql::SinkMode;
using clink::mysql::SinkTarget;
using clink::mysql::TableSchema;
using clink::mysql::UniqueIndex;
using clink::sqljson::TimeKind;

namespace {

std::string esc(std::string_view s) {
    std::string out;
    for (const char c : s) {
        if (c == '\'' || c == '\\') {
            out.push_back('\\');
        }
        out.push_back(c);
    }
    return out;
}

JsonMembers read(const std::string& json) {
    JsonMembers m;
    const auto why = clink::mysql::read_json_object(json, m);
    EXPECT_TRUE(why.empty()) << why;
    return m;
}

std::string refusal(const std::string& json) {
    JsonMembers m;
    return clink::mysql::read_json_object(json, m);
}

ColumnInfo column(std::string name, std::string type, std::string coll = {}, bool no_pad = false) {
    ColumnInfo c;
    c.name = std::move(name);
    c.data_type = type;
    c.column_type = type;
    c.collation = std::move(coll);
    c.collation_no_pad = no_pad;
    return c;
}

TableSchema table_with(std::vector<ColumnInfo> cols, std::vector<UniqueIndex> unique = {}) {
    TableSchema s;
    s.exists = true;
    s.engine = "InnoDB";
    s.transactional = true;
    s.columns = std::move(cols);
    s.unique_indexes = std::move(unique);
    return s;
}

UniqueIndex index(std::string name, std::vector<std::string> cols, bool primary = false) {
    UniqueIndex u;
    u.name = std::move(name);
    u.columns = std::move(cols);
    u.primary = primary;
    return u;
}

std::string reasons(const SinkTarget& t, const TableSchema& s) {
    std::string out;
    for (const auto& r : clink::mysql::check_target(t, s)) {
        out += r + "\n";
    }
    return out;
}

}  // namespace

// ---- reading a record -----------------------------------------------------------------

TEST(MysqlSinkRecord, KeepsNumbersExactlyAndDecodesStrings) {
    const auto m = read(
        R"({"big":18446744073709551615,"dec":12345678901234567.89,"e":1.5E3,"s":"a\"b\\cé😀","t":true,"n":null,"o":{"x":[1,2]}})");
    EXPECT_EQ(m.at("big").kind, JsonMember::Kind::Number);
    EXPECT_EQ(m.at("big").text, "18446744073709551615");
    EXPECT_EQ(m.at("dec").text, "12345678901234567.89");
    EXPECT_EQ(m.at("e").text, "1.5E3");
    EXPECT_EQ(m.at("s").text, "a\"b\\c\xc3\xa9\xf0\x9f\x98\x80");
    EXPECT_EQ(m.at("t").kind, JsonMember::Kind::Bool);
    EXPECT_EQ(m.at("n").kind, JsonMember::Kind::Null);
    EXPECT_EQ(m.at("o").kind, JsonMember::Kind::Composite);
    EXPECT_EQ(m.at("o").text, R"({"x":[1,2]})");
}

TEST(MysqlSinkRecord, RefusesWhatIsNotOneStrictObject) {
    EXPECT_NE(refusal("").find("empty"), std::string::npos);
    EXPECT_NE(refusal("[1]").find("not a JSON object"), std::string::npos);
    EXPECT_NE(refusal(R"({"a":1} x)").find("after its closing"), std::string::npos);
    EXPECT_NE(refusal(R"({"a":1,"a":2})").find("twice"), std::string::npos);
    EXPECT_NE(refusal(R"({"a":01})").find("malformed number"), std::string::npos);
    EXPECT_NE(refusal(R"({"a":1.})").find("malformed number"), std::string::npos);
    EXPECT_NE(refusal(R"({"a":-})").find("malformed number"), std::string::npos);
    EXPECT_NE(refusal("{\"a\":\"x\ty\"}").find("control character"), std::string::npos);
    EXPECT_NE(refusal("{\"a\":\"\xc0\x80\"}").find("UTF-8"), std::string::npos);
    EXPECT_NE(refusal("{\"a\":\"\xed\xa0\x80\"}").find("UTF-8"), std::string::npos);
    EXPECT_NE(refusal(R"({"a":"\ud800"})").find("surrogate"), std::string::npos);
    EXPECT_NE(refusal(R"({"a":"\x"})").find("unknown escape"), std::string::npos);
    EXPECT_NE(refusal(R"({"a":tru})").find("not a JSON value"), std::string::npos);
    std::string deep = R"({"a":)";
    for (int i = 0; i < 300; ++i) {
        deep += "[";
    }
    EXPECT_NE(refusal(deep).find("nested deeper"), std::string::npos);
    // A nested object may repeat a name; it is stored as written.
    EXPECT_TRUE(refusal(R"({"a":{"x":1,"x":2}})").empty());
}

TEST(MysqlSinkRecord, AReasonNeverQuotesTheRecord) {
    const auto why = refusal(R"({"card":"4111-1111-1111-1111", "x": 01})");
    EXPECT_FALSE(why.empty());
    EXPECT_EQ(why.find("4111"), std::string::npos) << why;
}

TEST(MysqlSinkRecord, PlainDecimalIsExact) {
    using clink::mysql::plain_decimal;
    EXPECT_EQ(plain_decimal("1e3", 100), "1000");
    EXPECT_EQ(plain_decimal("1.50", 100), "1.5");
    EXPECT_EQ(plain_decimal("-0", 100), "0");
    EXPECT_EQ(plain_decimal("0.000", 100), "0");
    EXPECT_EQ(plain_decimal("123.4500e-2", 100), "1.2345");
    EXPECT_EQ(plain_decimal("1e-5", 100), "0.00001");
    EXPECT_EQ(plain_decimal("-12345678901234567890123", 100), "-12345678901234567890123");
    EXPECT_EQ(plain_decimal("1e200", 100), "");
}

// ---- rendering --------------------------------------------------------------------------

TEST(MysqlSinkRender, LiteralsByKind) {
    using clink::mysql::render_literal;
    const auto lit = [](JsonMember::Kind k, std::string t) {
        return render_literal(JsonMember{k, std::move(t)}, nullptr, TimeKind::None, esc);
    };
    EXPECT_EQ(lit(JsonMember::Kind::Null, ""), "NULL");
    EXPECT_EQ(lit(JsonMember::Kind::Bool, "true"), "1");
    EXPECT_EQ(lit(JsonMember::Kind::Bool, "false"), "0");
    EXPECT_EQ(lit(JsonMember::Kind::String, "it's"), "'it\\'s'");
    EXPECT_EQ(lit(JsonMember::Kind::Number, "1.5E3"), "1500");
    EXPECT_EQ(lit(JsonMember::Kind::Number, "18446744073709551615"), "18446744073709551615");
    EXPECT_EQ(lit(JsonMember::Kind::Composite, R"({"a":1})"), "'{\"a\":1}'");
    EXPECT_THROW(lit(JsonMember::Kind::Number, "1e300"), RecordRefused);
}

// A FLOAT or DOUBLE column takes the token as the server reads it, so a value
// no exact column could hold is written; past a DOUBLE's range it is refused,
// without the value.
TEST(MysqlSinkRender, ApproximateColumnsTakeTheTokenAsWritten) {
    using clink::mysql::render_literal;
    const auto dbl = column("f", "double");
    const auto flt = column("g", "float");
    const auto dec = column("d", "decimal");
    const JsonMember big{JsonMember::Kind::Number, "-1.5e300"};
    EXPECT_EQ(render_literal(big, &dbl, TimeKind::None, esc), "-1.5e300");
    EXPECT_EQ(
        render_literal(JsonMember{JsonMember::Kind::Number, "0.1"}, &flt, TimeKind::None, esc),
        "0.1");
    EXPECT_THROW((void)render_literal(big, &dec, TimeKind::None, esc), RecordRefused);
    try {
        (void)render_literal(
            JsonMember{JsonMember::Kind::Number, "1e400"}, &dbl, TimeKind::None, esc);
        ADD_FAILURE() << "a number past a DOUBLE's range was written";
    } catch (const RecordRefused& e) {
        EXPECT_EQ(std::string{e.what()}.find("1e400"), std::string::npos) << e.what();
    }
}

TEST(MysqlSinkRender, TimestampsKeepTheirMillisecondsOrAreRefused) {
    using clink::mysql::render_literal;
    auto at = column("at", "datetime");
    at.datetime_precision = 3;
    const JsonMember ms{JsonMember::Kind::Number, "1792863688105"};
    EXPECT_EQ(render_literal(ms, &at, TimeKind::Timestamp, esc), "'2026-10-24 17:41:28.105'");
    at.datetime_precision = 0;
    EXPECT_THROW((void)render_literal(ms, &at, TimeKind::Timestamp, esc), RecordRefused);
    EXPECT_EQ(
        render_literal(
            JsonMember{JsonMember::Kind::Number, "1792863688000"}, &at, TimeKind::Timestamp, esc),
        "'2026-10-24 17:41:28.000'");
    at.datetime_precision = 2;
    EXPECT_NO_THROW((void)render_literal(
        JsonMember{JsonMember::Kind::Number, "1792863688120"}, &at, TimeKind::Timestamp, esc));
    EXPECT_THROW(
        (void)render_literal(
            JsonMember{JsonMember::Kind::Number, "1792863688.5"}, &at, TimeKind::Timestamp, esc),
        RecordRefused);
    EXPECT_THROW((void)render_literal(JsonMember{JsonMember::Kind::Number, "253402300800000"},
                                      nullptr,
                                      TimeKind::Timestamp,
                                      esc),
                 RecordRefused);  // year 10000
}

TEST(MysqlSinkRender, RowsAndKeysNullMissingMembersButNeverKeys) {
    const auto s = table_with({column("id", "int"), column("v", "varchar")});
    const auto m = read(R"({"id":7,"v":null,"secret":"x"})");
    EXPECT_EQ(clink::mysql::render_row(m, {"id", "v", "w"}, s, {}, esc), "(7,NULL,NULL)");
    EXPECT_EQ(clink::mysql::render_key(m, {"id"}, s, {}, esc), "7");
    EXPECT_EQ(clink::mysql::render_key(read(R"({"a":1,"b":"x"})"), {"a", "b"}, s, {}, esc),
              "(1,'x')");
    try {
        (void)clink::mysql::render_key(read(R"({"id":null,"v":"top secret"})"), {"id"}, s, {}, esc);
        ADD_FAILURE() << "a null key was rendered";
    } catch (const RecordRefused& e) {
        EXPECT_NE(std::string{e.what()}.find("\"id\""), std::string::npos) << e.what();
        EXPECT_EQ(std::string{e.what()}.find("top secret"), std::string::npos) << e.what();
    }
}

TEST(MysqlSinkRender, StreamKeysIdentifyNumbersByValue) {
    using clink::mysql::stream_key;
    EXPECT_EQ(stream_key(read(R"({"k":1.0})"), {"k"}), stream_key(read(R"({"k":1.00})"), {"k"}));
    EXPECT_NE(stream_key(read(R"({"k":1})"), {"k"}), stream_key(read(R"({"k":"1"})"), {"k"}));
    EXPECT_NE(stream_key(read(R"({"a":"x","b":"yz"})"), {"a", "b"}),
              stream_key(read(R"({"a":"xy","b":"z"})"), {"a", "b"}));
    EXPECT_THROW((void)stream_key(read(R"({"k":null})"), {"k"}), RecordRefused);
}

TEST(MysqlSinkRender, KeysAreWrittenAsTheirColumnsCompare) {
    using clink::mysql::check_key_spelling;
    const auto s = table_with({column("n", "bigint"),
                               column("d", "decimal"),
                               column("at", "datetime"),
                               column("s", "varchar", "utf8mb4_0900_bin", true),
                               column("b", "varbinary")});
    const std::vector<std::string> keys = {"n", "d", "at", "s", "b"};
    EXPECT_NO_THROW(
        check_key_spelling(read(R"({"n":1,"d":1.5,"at":1000,"s":"x","b":"y"})"), keys, s));
    for (const char* bad : {R"({"n":"1","d":1.5,"at":1000,"s":"x","b":"y"})",
                            R"({"n":1,"d":"1.5","at":1000,"s":"x","b":"y"})",
                            R"({"n":1,"d":1.5,"at":"1970-01-01","s":"x","b":"y"})",
                            R"({"n":1,"d":1.5,"at":1000,"s":1,"b":"y"})",
                            R"({"n":1,"d":1.5,"at":1000,"s":"x","b":true})"}) {
        EXPECT_THROW(check_key_spelling(read(bad), keys, s), RecordRefused) << bad;
    }
}

TEST(MysqlSinkRender, ChangelogMarkersAreValidated) {
    using clink::mysql::change_kind_of;
    EXPECT_EQ(change_kind_of(read(R"({"k":1})")), ChangeKind::Upsert);
    EXPECT_EQ(change_kind_of(read(R"({"__row_kind":"insert"})")), ChangeKind::Upsert);
    EXPECT_EQ(change_kind_of(read(R"({"__row_kind":"update_after"})")), ChangeKind::Upsert);
    EXPECT_EQ(change_kind_of(read(R"({"__row_kind":"delete"})")), ChangeKind::Delete);
    EXPECT_EQ(change_kind_of(read(R"({"__row_kind":"update_before"})")), ChangeKind::Delete);
    EXPECT_THROW((void)change_kind_of(read(R"({"__row_kind":"DELETE"})")), RecordRefused);
    EXPECT_THROW((void)change_kind_of(read(R"({"__row_kind":"upsert"})")), RecordRefused);
    EXPECT_THROW((void)change_kind_of(read(R"({"__row_kind":1})")), RecordRefused);
    EXPECT_THROW((void)change_kind_of(read(R"({"__row_kind":null})")), RecordRefused);
}

// ---- statements -------------------------------------------------------------------------

TEST(MysqlSinkStatements, PackKeepsOrderWithinTheBound) {
    using clink::mysql::pack_statements;
    const std::vector<std::string> items = {"(1)", "(22)", "(333)", "(4)"};
    // head "H " (2) + items + tail ";" (1)
    const auto one = pack_statements("H ", items, ";", 100);
    ASSERT_EQ(one.size(), 1U);
    EXPECT_EQ(one[0], "H (1),(22),(333),(4);");
    const auto split = pack_statements("H ", items, ";", 12);
    ASSERT_EQ(split.size(), 2U);
    EXPECT_EQ(split[0], "H (1),(22);");
    EXPECT_EQ(split[1], "H (333),(4);");
    EXPECT_EQ(split[1].size(), 12U);  // exactly the bound
    EXPECT_THROW((void)pack_statements("H ", {"(123456789)"}, ";", 12), std::length_error);
}

TEST(MysqlSinkStatements, HeadsAndTails) {
    EXPECT_EQ(clink::mysql::insert_head("db.t", {"a", "b"}),
              "INSERT INTO `db`.`t` (`a`,`b`) VALUES ");
    EXPECT_EQ(clink::mysql::upsert_tail({"a", "b"}),
              " AS clink_new ON DUPLICATE KEY UPDATE `a`=clink_new.`a`,`b`=clink_new.`b`");
    EXPECT_EQ(clink::mysql::delete_head("t", {"k"}), "DELETE FROM `t` WHERE `k` IN (");
    EXPECT_EQ(clink::mysql::delete_head("t", {"a", "b"}), "DELETE FROM `t` WHERE (`a`,`b`) IN (");
}

TEST(MysqlSinkStatements, ServerMessagesLoseTheirValues) {
    using clink::mysql::redact_server_message;
    EXPECT_EQ(redact_server_message("Duplicate entry 'alice@example.com' for key 'u.email'"),
              "Duplicate entry '<value>' for key 'u.email'");
    EXPECT_EQ(redact_server_message(
                  "Incorrect string value: '\\xF0\\x9F\\x98\\x80' for column 'v' at row 1"),
              "Incorrect string value: '<value>' for column 'v' at row 1");
    EXPECT_EQ(redact_server_message("Incorrect datetime value: '2026-13-01' for column 'at' at "
                                    "row 2"),
              "Incorrect datetime value: '<value>' for column 'at' at row 2");
    EXPECT_EQ(redact_server_message("Data too long for column 'v' at row 1"),
              "Data too long for column 'v' at row 1");
}

// ---- the changelog buffer -------------------------------------------------------------

TEST(MysqlSinkChangelog, ASurvivorTakesItsLastChangesPlace) {
    ChangelogBuffer b;
    b.add("a", {ChangeKind::Upsert, "(a1)", ""});
    b.add("b", {ChangeKind::Upsert, "(b1)", ""});
    b.add("a", {ChangeKind::Upsert, "(a2)", ""});  // moves after b
    b.add("c", {ChangeKind::Delete, "", "c"});
    b.add("b", {ChangeKind::Delete, "", "b"});  // b now a delete, last
    ASSERT_EQ(b.size(), 3U);
    const auto runs = b.runs();
    ASSERT_EQ(runs.size(), 2U);
    EXPECT_EQ(runs[0].first, ChangeKind::Upsert);
    EXPECT_EQ(runs[0].second, (std::vector<std::string>{"(a2)"}));
    EXPECT_EQ(runs[1].first, ChangeKind::Delete);
    EXPECT_EQ(runs[1].second, (std::vector<std::string>{"c", "b"}));
    EXPECT_EQ(b.bytes(), std::string{"(a2)cb"}.size());
}

// A delete of 'abc' then an upsert of 'ABC': two stream keys, one row under a
// case-insensitive collation. The buffer keeps them in the order they came, so
// the row ends present, as the upsert left it; applying every upsert before
// every delete would end it deleted.
TEST(MysqlSinkChangelog, KeysTheTableEquatesMeetItInOrder) {
    ChangelogBuffer b;
    b.add("s3:abc", {ChangeKind::Delete, "", "'abc'"});
    b.add("s3:ABC", {ChangeKind::Upsert, "('ABC',1)", ""});
    const auto runs = b.runs();
    ASSERT_EQ(runs.size(), 2U);
    EXPECT_EQ(runs[0].first, ChangeKind::Delete);
    EXPECT_EQ(runs[1].first, ChangeKind::Upsert);
}

// ---- the destination ------------------------------------------------------------------

TEST(MysqlSinkTarget, KeyIdentityIsExactOnlyWhereMysqlComparesAsTheStreamDoes) {
    using clink::mysql::key_identity_exact;
    EXPECT_TRUE(key_identity_exact(column("k", "bigint"), false));
    EXPECT_TRUE(key_identity_exact(column("k", "decimal"), false));
    EXPECT_TRUE(key_identity_exact(column("k", "varbinary"), false));
    EXPECT_TRUE(key_identity_exact(column("k", "varchar", "utf8mb4_0900_bin", true), false));
    EXPECT_FALSE(key_identity_exact(column("k", "varchar", "utf8mb4_bin", false), false));
    EXPECT_FALSE(key_identity_exact(column("k", "varchar", "utf8mb4_0900_ai_ci", true), false));
    EXPECT_FALSE(key_identity_exact(column("k", "char", "utf8mb4_0900_bin", true), false));
    EXPECT_FALSE(key_identity_exact(column("k", "binary"), false));
    EXPECT_FALSE(key_identity_exact(column("k", "double"), false));
    EXPECT_FALSE(key_identity_exact(column("k", "date"), false));
    auto ts = column("k", "datetime");
    ts.datetime_precision = 3;
    EXPECT_TRUE(key_identity_exact(ts, true));
    EXPECT_FALSE(key_identity_exact(ts, false));
    ts.datetime_precision = 0;
    EXPECT_FALSE(key_identity_exact(ts, true));
}

TEST(MysqlSinkTarget, EveryModeChecksItsColumns) {
    SinkTarget t;
    t.table = "t";
    t.columns = {"id", "v", "g"};
    TableSchema missing;
    EXPECT_NE(reasons(t, missing).find("does not exist"), std::string::npos);
    auto g = column("g", "int");
    g.generated = true;
    const auto s = table_with({column("id", "int"), g});
    const auto r = reasons(t, s);
    EXPECT_NE(r.find("no column 'v'"), std::string::npos) << r;
    EXPECT_NE(r.find("'g' is a generated column"), std::string::npos) << r;
    t.columns = {"id"};
    t.timestamp_columns = {{"id", TimeKind::Timestamp}};
    EXPECT_NE(reasons(t, s).find("must be DATETIME or TIMESTAMP"), std::string::npos);
}

TEST(MysqlSinkTarget, InsertOrUpdateNeedsExactlyOneUniqueIndexItWrites) {
    SinkTarget t;
    t.table = "t";
    t.mode = SinkMode::InsertOrUpdate;
    t.columns = {"id", "v"};
    const std::vector<ColumnInfo> cols = {column("id", "int"), column("v", "varchar")};
    EXPECT_NE(reasons(t, table_with(cols)).find("needs a PRIMARY KEY or UNIQUE index"),
              std::string::npos);
    EXPECT_TRUE(reasons(t, table_with(cols, {index("PRIMARY", {"id"}, true)})).empty());
    EXPECT_NE(reasons(t, table_with(cols, {index("PRIMARY", {"id"}, true), index("uv", {"v"})}))
                  .find("2 unique indexes"),
              std::string::npos);
    EXPECT_NE(reasons(t, table_with(cols, {index("uw", {"w"})})).find("whose column 'w'"),
              std::string::npos);
    t.update_columns = {"x"};
    EXPECT_NE(
        reasons(t, table_with(cols, {index("PRIMARY", {"id"}, true)})).find("update column 'x'"),
        std::string::npos);
}

TEST(MysqlSinkTarget, ChangelogNeedsTransactionsOneIdentityAndExactKeysWhenParallel) {
    SinkTarget t;
    t.table = "t";
    t.mode = SinkMode::Changelog;
    t.columns = {"k", "v"};
    t.key_columns = {"k"};
    const std::vector<ColumnInfo> ci = {column("k", "varchar", "utf8mb4_0900_ai_ci", true),
                                        column("v", "int")};
    auto ok = table_with(ci, {index("PRIMARY", {"k"}, true)});
    EXPECT_TRUE(reasons(t, ok).empty()) << reasons(t, ok);

    auto myisam = ok;
    myisam.engine = "MyISAM";
    myisam.transactional = false;
    EXPECT_NE(reasons(t, myisam).find("no transactions"), std::string::npos);

    EXPECT_NE(reasons(t, table_with(ci, {index("PRIMARY", {"k", "v"}, true)}))
                  .find("no PRIMARY KEY or UNIQUE index on exactly (k)"),
              std::string::npos);
    EXPECT_NE(reasons(t, table_with(ci, {index("PRIMARY", {"k"}, true), index("uv", {"v"})}))
                  .find("also has unique index uv"),
              std::string::npos);
    auto prefixed = ok;
    prefixed.unique_indexes[0].prefix = true;
    EXPECT_NE(reasons(t, prefixed).find("prefix"), std::string::npos);

    t.update_columns = {"k"};
    EXPECT_NE(reasons(t, ok).find("leaves out column 'v'"), std::string::npos);
    t.update_columns = {"v"};
    EXPECT_TRUE(reasons(t, ok).empty());

    t.parallelism = 4;
    EXPECT_NE(reasons(t, ok).find("utf8mb4_0900_ai_ci"), std::string::npos);
    const std::vector<ColumnInfo> exact = {column("k", "varchar", "utf8mb4_0900_bin", true),
                                           column("v", "int")};
    EXPECT_TRUE(reasons(t, table_with(exact, {index("PRIMARY", {"k"}, true)})).empty());
}
