// The Redis keyed sink's key rule, without a server: a string key is its
// characters, a number its exact plain decimal, a composite key joined by
// U+001F, and the keys that could not name one row, or could name two, are
// refused.

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/redis/redis_sink_rules.hpp"

namespace {

using clink::redis::JsonMembers;
using clink::redis::read_json_object;
using clink::redis::RecordRefused;
using clink::redis::redis_key;

std::string key_of(const std::string& text,
                   const std::vector<std::string>& columns,
                   const std::string& prefix = "") {
    JsonMembers m;
    const auto why = read_json_object(text, m);
    EXPECT_TRUE(why.empty()) << why;
    return redis_key(m, columns, prefix);
}

std::string refusal(const std::string& text, const std::vector<std::string>& columns) {
    try {
        (void)key_of(text, columns);
    } catch (const RecordRefused& e) {
        return e.what();
    }
    return "";
}

TEST(RedisSinkRules, AStringKeyIsItsCharacters) {
    EXPECT_EQ(key_of(R"({"id":"lisbon-o00940"})", {"id"}, "order:"), "order:lisbon-o00940");
    EXPECT_EQ(key_of("{\"id\":\"caf\xc3\xa9\"}", {"id"}), "caf\xc3\xa9");
    EXPECT_EQ(key_of(R"({"id":"a\"b"})", {"id"}), "a\"b");
    EXPECT_EQ(key_of(R"({"id":""})", {"id"}, "p:"), "p:");
}

TEST(RedisSinkRules, ANumberKeyIsItsExactPlainDecimal) {
    EXPECT_EQ(key_of(R"({"id":100})", {"id"}), "100");
    EXPECT_EQ(key_of(R"({"id":1.50})", {"id"}), "1.5");
    EXPECT_EQ(key_of(R"({"id":1e3})", {"id"}), "1000");
    EXPECT_EQ(key_of(R"({"id":-0.0})", {"id"}), "0");
    // Past a double's precision two keys stay two keys.
    EXPECT_EQ(key_of(R"({"id":9007199254740993})", {"id"}), "9007199254740993");
    EXPECT_EQ(key_of(R"({"id":9007199254740992})", {"id"}), "9007199254740992");
    EXPECT_EQ(key_of(R"({"id":12345678901234567890.123456789})", {"id"}),
              "12345678901234567890.123456789");
}

TEST(RedisSinkRules, ABooleanKeyIsTrueOrFalse) {
    EXPECT_EQ(key_of(R"({"id":true})", {"id"}), "true");
    EXPECT_EQ(key_of(R"({"id":false})", {"id"}), "false");
}

TEST(RedisSinkRules, ACompositeKeyJoinsItsColumnsInKeyOrder) {
    EXPECT_EQ(key_of(R"({"a":"x","b":2})", {"a", "b"}, "p:"), std::string{"p:x\x1f"} + "2");
    EXPECT_EQ(key_of(R"({"b":2,"a":"x"})", {"a", "b"}), std::string{"x\x1f"} + "2");
}

TEST(RedisSinkRules, ACompositeKeyMemberHoldingTheSeparatorIsRefused) {
    // "a<US>b" then "c", and "a" then "b<US>c", would name one key.
    const auto why = refusal(R"({"a":"a\u001fb","b":"c"})", {"a", "b"});
    EXPECT_NE(why.find("U+001F"), std::string::npos) << why;
    // A key of one column joins nothing, so it may hold one.
    EXPECT_EQ(key_of(R"({"a":"a\u001fb"})", {"a"}), std::string{"a\x1f"} + "b");
}

TEST(RedisSinkRules, AKeyThatNamesNoKeyIsRefused) {
    EXPECT_NE(refusal(R"({"v":1})", {"id"}).find("missing or null"), std::string::npos);
    EXPECT_NE(refusal(R"({"id":null})", {"id"}).find("missing or null"), std::string::npos);
    EXPECT_NE(refusal(R"({"id":{"x":1}})", {"id"}).find("object or array"), std::string::npos);
    EXPECT_NE(refusal(R"({"id":[1]})", {"id"}).find("object or array"), std::string::npos);
    EXPECT_NE(refusal(R"({"id":1e5000})", {"id"}).find("too long"), std::string::npos);
}

TEST(RedisSinkRules, ARefusalNamesTheColumnNotTheRecord) {
    const auto why = refusal(R"({"id":null,"secret":"hunter2"})", {"id"});
    EXPECT_NE(why.find("\"id\""), std::string::npos) << why;
    EXPECT_EQ(why.find("hunter2"), std::string::npos) << why;
}

}  // namespace
