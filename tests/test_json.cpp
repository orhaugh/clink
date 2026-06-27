// Tests for the minimal JSON parser used by the pipeline-config loader.
// Pin: round-trip identity, malformed input rejection, type-mismatch
// errors. Exhaustive coverage isn't the goal - the parser is intentionally
// a subset of full JSON; the goal is that the subset is correct.

#include <cstdint>
#include <string>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"

using namespace clink::config;

// ----- Primitive parsing -----

TEST(Json, ParsesPrimitives) {
    EXPECT_TRUE(parse("null").is_null());
    EXPECT_TRUE(parse("true").as_bool());
    EXPECT_FALSE(parse("false").as_bool());
    EXPECT_EQ(parse("42").as_number(), 42.0);
    EXPECT_EQ(parse("-3.5").as_number(), -3.5);
    EXPECT_EQ(parse("1e3").as_number(), 1000.0);
    EXPECT_EQ(parse("\"hello\"").as_string(), "hello");
}

TEST(Json, ParsesEmptyContainers) {
    EXPECT_TRUE(parse("[]").is_array());
    EXPECT_TRUE(parse("[]").as_array().empty());
    EXPECT_TRUE(parse("{}").is_object());
    EXPECT_TRUE(parse("{}").as_object().empty());
}

TEST(Json, ParsesNestedStructures) {
    auto v = parse(R"({"name":"alice","tags":["admin","user"],"meta":{"age":30}})");
    ASSERT_TRUE(v.is_object());
    EXPECT_EQ(v.at("name").as_string(), "alice");
    ASSERT_TRUE(v.at("tags").is_array());
    EXPECT_EQ(v.at("tags").as_array()[0].as_string(), "admin");
    EXPECT_EQ(v.at("tags").as_array()[1].as_string(), "user");
    EXPECT_EQ(v.at("meta").at("age").as_number(), 30.0);
}

TEST(Json, ParsesEscapeSequences) {
    auto v = parse(R"("line1\nline2\ttab\"quote\\slash")");
    EXPECT_EQ(v.as_string(), "line1\nline2\ttab\"quote\\slash");
}

TEST(Json, ParsesUnicodeEscape) {
    auto v = parse(R"("é")");  // é (U+00E9, 2-byte UTF-8)
    EXPECT_EQ(v.as_string(), "\xC3\xA9");

    auto v2 = parse(R"("中")");  // 中 (U+4E2D, 3-byte UTF-8)
    EXPECT_EQ(v2.as_string(), "\xE4\xB8\xAD");
}

TEST(Json, IgnoresWhitespace) {
    auto v = parse(R"({
  "a" : 1 ,
  "b" :   [ 2 , 3 ]
})");
    EXPECT_EQ(v.at("a").as_number(), 1.0);
    EXPECT_EQ(v.at("b").as_array().size(), 2u);
}

// ----- Round-trip -----

TEST(Json, RoundTripsPrimitives) {
    EXPECT_EQ(parse("null").serialize(), "null");
    EXPECT_EQ(parse("true").serialize(), "true");
    EXPECT_EQ(parse("42").serialize(), "42");
    EXPECT_EQ(parse("-3.5").serialize(), "-3.5");
    EXPECT_EQ(parse("\"hi\"").serialize(), "\"hi\"");
}

TEST(Json, IntegerNumbersSerializeWithoutDecimal) {
    EXPECT_EQ(parse("100").serialize(), "100");
    EXPECT_EQ(parse("-7").serialize(), "-7");
}

TEST(Json, RoundTripsObjectAndArray) {
    const auto src = R"({"k":[1,"two",true,null]})";
    auto v = parse(src);
    EXPECT_EQ(v.serialize(), src);
}

TEST(Json, PrettyPrintProducesIndentedOutput) {
    auto v = parse(R"({"a":1,"b":[2,3]})");
    auto pretty = v.serialize(2);
    EXPECT_NE(pretty.find('\n'), std::string::npos);
    // Round-trip: parsing the pretty form yields the same value.
    EXPECT_EQ(parse(pretty).serialize(), v.serialize());
}

// ----- Convenience accessors -----

TEST(Json, AccessorsHandleMissingKeysWithFallback) {
    auto v = parse(R"({"x":"hello"})");
    EXPECT_EQ(v.string_or("x", "default"), "hello");
    EXPECT_EQ(v.string_or("missing", "default"), "default");
    EXPECT_EQ(v.int_or("missing", 7), 7);
    EXPECT_EQ(v.bool_or("missing", true), true);
}

TEST(Json, AccessorsThrowOnTypeMismatch) {
    auto v = parse(R"({"x":"not-a-number"})");
    EXPECT_THROW((void)v.int_or("x", 0), std::runtime_error);
}

TEST(Json, ContainsAndAtThrowsForMissing) {
    auto v = parse(R"({"a":1})");
    EXPECT_TRUE(v.contains("a"));
    EXPECT_FALSE(v.contains("b"));
    EXPECT_THROW((void)v.at("b"), std::runtime_error);
}

// ----- Malformed input rejection -----

TEST(Json, RejectsTrailingCharacters) {
    EXPECT_THROW(parse("42 garbage"), ParseError);
}

TEST(Json, RejectsTrailingComma) {
    EXPECT_THROW(parse("[1, 2,]"), ParseError);
    EXPECT_THROW(parse(R"({"a":1,})"), ParseError);
}

TEST(Json, RejectsUnterminatedString) {
    EXPECT_THROW(parse(R"("oops)"), ParseError);
}

TEST(Json, RejectsUnknownEscape) {
    EXPECT_THROW(parse(R"("\x00")"), ParseError);
}

TEST(Json, RejectsLeadingZero) {
    EXPECT_THROW(parse("01"), ParseError);
}

TEST(Json, RejectsBareIdentifier) {
    EXPECT_THROW(parse("undefined"), ParseError);
    EXPECT_THROW(parse("True"), ParseError);  // case-sensitive
}

TEST(Json, RejectsControlCharInString) {
    std::string raw{"\""};
    raw.push_back('\x01');
    raw.push_back('"');
    EXPECT_THROW(parse(raw), ParseError);
}

TEST(Json, RejectsEmptyInput) {
    EXPECT_THROW(parse(""), ParseError);
    EXPECT_THROW(parse("   "), ParseError);
}

TEST(Json, EscapesControlCharsOnSerialize) {
    /* Adjacent literals split \x01 from b so the hex escape stops cleanly. */
    JsonValue v{
        std::string{"a\x01"
                    "b"}};
    auto serialized = v.serialize();
    EXPECT_NE(serialized.find("\\u0001"), std::string::npos);
}

TEST(Json, SerializeOutOfRangeDoubleAvoidsInt64CastUB) {
    // > 2^63: integer-valued but out of int64 range. Must render as a double
    // (not via the UB int64 cast). In-range integer-valued doubles still render
    // without a trailing ".0".
    EXPECT_EQ(JsonValue{42.0}.serialize(), "42");
    const std::string big = JsonValue{1e19}.serialize();
    EXPECT_NE(big.find('e'), std::string::npos) << big;  // scientific/float form
    EXPECT_NO_FATAL_FAILURE((void)JsonValue{9223372036854775808.0}.serialize());  // exactly 2^63
}

TEST(Json, IntOrOutOfRangeReturnsFallback) {
    JsonObject o;
    o["big"] = JsonValue{1e19};  // out of int64 range
    o["ok"] = JsonValue{static_cast<std::int64_t>(123)};
    JsonValue v{std::move(o)};
    EXPECT_EQ(v.int_or("big", -1), -1);  // unrepresentable as int64 -> fallback
    EXPECT_EQ(v.int_or("ok", -7), 123);
}

TEST(Json, ConfigStyleDocumentRoundTrips) {
    // A representative pipeline-config document, to make sure the parser
    // handles the exact shape we'll see in test_pipeline_config.cpp.
    const std::string text = R"({
  "pipeline": {
    "stages": [
      {
        "name": "input",
        "type": "kafka_source",
        "params": {
          "brokers": "localhost:9092",
          "topic": "in",
          "group_id": "demo"
        }
      },
      {
        "name": "uppercase",
        "type": "map",
        "input": "input",
        "params": {"fn": "uppercase"}
      },
      {
        "name": "out",
        "type": "kafka_sink",
        "input": "uppercase",
        "params": {"brokers": "localhost:9092", "topic": "out"}
      }
    ]
  }
})";
    auto v = parse(text);
    EXPECT_EQ(v.at("pipeline").at("stages").as_array().size(), 3u);
    EXPECT_EQ(v.at("pipeline").at("stages").as_array()[0].at("name").as_string(), "input");
    EXPECT_EQ(v.at("pipeline").at("stages").as_array()[1].at("params").at("fn").as_string(),
              "uppercase");
}
