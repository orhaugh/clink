// The SQL sinks' shared record rules, without a server: the strict reading of a
// record, exact numbers, the key a record names as the stream holds it, the
// changelog marker, statement packing and the changelog buffer's order.

#include <stdexcept>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/connectors/sink_record_rules.hpp"

using clink::sinkrules::ChangeKind;
using clink::sinkrules::ChangelogBuffer;
using clink::sinkrules::JsonMember;
using clink::sinkrules::JsonMembers;
using clink::sinkrules::RecordRefused;

namespace {

JsonMembers read(const std::string& json) {
    JsonMembers m;
    const auto why = clink::sinkrules::read_json_object(json, m);
    EXPECT_TRUE(why.empty()) << why;
    return m;
}

std::string refusal(const std::string& json) {
    JsonMembers m;
    return clink::sinkrules::read_json_object(json, m);
}

}  // namespace

TEST(SinkRecordRules, KeepsNumbersExactlyAndDecodesStrings) {
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

TEST(SinkRecordRules, RefusesWhatIsNotOneStrictObject) {
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

TEST(SinkRecordRules, AReasonNeverQuotesTheRecord) {
    const auto why = refusal(R"({"card":"4111-1111-1111-1111", "x": 01})");
    EXPECT_FALSE(why.empty());
    EXPECT_EQ(why.find("4111"), std::string::npos) << why;
}

TEST(SinkRecordRules, PlainDecimalIsExact) {
    using clink::sinkrules::plain_decimal;
    EXPECT_EQ(plain_decimal("1e3", 100), "1000");
    EXPECT_EQ(plain_decimal("1.50", 100), "1.5");
    EXPECT_EQ(plain_decimal("-0", 100), "0");
    EXPECT_EQ(plain_decimal("0.000", 100), "0");
    EXPECT_EQ(plain_decimal("123.4500e-2", 100), "1.2345");
    EXPECT_EQ(plain_decimal("1e-5", 100), "0.00001");
    EXPECT_EQ(plain_decimal("-12345678901234567890123", 100), "-12345678901234567890123");
    EXPECT_EQ(plain_decimal("1e200", 100), "");
}

TEST(SinkRecordKeys, StreamKeysIdentifyNumbersByValue) {
    using clink::sinkrules::stream_key;
    EXPECT_EQ(stream_key(read(R"({"k":1.0})"), {"k"}), stream_key(read(R"({"k":1.00})"), {"k"}));
    EXPECT_NE(stream_key(read(R"({"k":1})"), {"k"}), stream_key(read(R"({"k":"1"})"), {"k"}));
    EXPECT_NE(stream_key(read(R"({"a":"x","b":"yz"})"), {"a", "b"}),
              stream_key(read(R"({"a":"xy","b":"z"})"), {"a", "b"}));
    EXPECT_THROW((void)stream_key(read(R"({"k":null})"), {"k"}), RecordRefused);
}

// A number too long to write plainly keeps its token, under a kind of its own:
// two such numbers stay two keys, and neither meets a short number's key.
TEST(SinkRecordKeys, NumbersTooLongToWritePlainlyStayDistinct) {
    using clink::sinkrules::stream_key;
    const auto a = stream_key(read(R"({"k":1e5000})"), {"k"});
    const auto b = stream_key(read(R"({"k":2e5000})"), {"k"});
    EXPECT_NE(a, b);
    EXPECT_NE(a, stream_key(read(R"({"k":1})"), {"k"}));
    EXPECT_EQ(stream_key(read(R"({"k":10e4999})"), {"k"}),
              stream_key(read(R"({"k":10e4999})"), {"k"}));
}

TEST(SinkRecordKeys, ChangelogMarkersAreValidated) {
    using clink::sinkrules::change_kind_of;
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

TEST(SinkRecordStatements, PackKeepsOrderWithinTheBound) {
    using clink::sinkrules::pack_statements;
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

TEST(SinkChangelogBuffer, ASurvivorTakesItsLastChangesPlace) {
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
TEST(SinkChangelogBuffer, KeysTheTableEquatesMeetItInOrder) {
    ChangelogBuffer b;
    b.add("s3:abc", {ChangeKind::Delete, "", "'abc'"});
    b.add("s3:ABC", {ChangeKind::Upsert, "('ABC',1)", ""});
    const auto runs = b.runs();
    ASSERT_EQ(runs.size(), 2U);
    EXPECT_EQ(runs[0].first, ChangeKind::Delete);
    EXPECT_EQ(runs[1].first, ChangeKind::Upsert);
}
