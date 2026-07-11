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

// ----- FlatMap container semantics -----
//
// JsonObject is a FlatMap (sorted contiguous storage). These pin the
// std::map-compatible contract the engine relies on, independently of
// the parse/serialize behaviour above.

TEST(FlatMap, IterationIsSortedRegardlessOfInsertOrder) {
    JsonObject o;
    o.emplace("zeta", JsonValue{1.0});
    o.emplace("alpha", JsonValue{2.0});
    o.emplace("mid", JsonValue{3.0});
    std::string keys;
    for (const auto& [k, v] : o) {
        keys += k;
        keys += ',';
    }
    EXPECT_EQ(keys, "alpha,mid,zeta,");
}

TEST(FlatMap, EmplaceKeepsFirstDuplicate) {
    JsonObject o;
    auto [it1, fresh1] = o.emplace("k", JsonValue{std::string{"first"}});
    auto [it2, fresh2] = o.emplace("k", JsonValue{std::string{"second"}});
    EXPECT_TRUE(fresh1);
    EXPECT_FALSE(fresh2);
    EXPECT_EQ(it2->second.as_string(), "first");
    EXPECT_EQ(o.size(), 1u);
}

TEST(FlatMap, InitializerListDeduplicatesFirstWins) {
    JsonObject o{{"a", JsonValue{1.0}}, {"a", JsonValue{2.0}}, {"b", JsonValue{3.0}}};
    EXPECT_EQ(o.size(), 2u);
    EXPECT_EQ(o.at("a").as_number(), 1.0);
    EXPECT_EQ(o.at("b").as_number(), 3.0);
}

TEST(FlatMap, OperatorBracketInsertsNullThenOverwrites) {
    JsonObject o;
    EXPECT_TRUE(o["fresh"].is_null());
    o["fresh"] = JsonValue{true};
    EXPECT_TRUE(o.at("fresh").as_bool());
    EXPECT_EQ(o.size(), 1u);
}

TEST(FlatMap, HeterogeneousLookupNeedsNoTemporaryString) {
    JsonObject o;
    o.emplace("key", JsonValue{4.0});
    const std::string_view probe{"key"};
    EXPECT_TRUE(o.contains(probe));
    EXPECT_EQ(o.count(probe), 1u);
    EXPECT_EQ(o.find(probe)->second.as_number(), 4.0);
    EXPECT_EQ(o.find(std::string_view{"nope"}), o.end());
    EXPECT_THROW(o.at("nope"), std::out_of_range);
}

TEST(FlatMap, EraseByKeyAndByIteratorWhileIterating) {
    JsonObject o;
    o.emplace("a", JsonValue{1.0});
    o.emplace("b", JsonValue{2.0});
    o.emplace("c", JsonValue{3.0});
    EXPECT_EQ(o.erase("b"), 1u);
    EXPECT_EQ(o.erase("b"), 0u);
    // erase(iterator) returns the next iterator (erase-while-iterate).
    for (auto it = o.begin(); it != o.end();) {
        it = (it->first == "a") ? o.erase(it) : std::next(it);
    }
    EXPECT_EQ(o.size(), 1u);
    EXPECT_TRUE(o.contains("c"));
}

TEST(FlatMap, InsertOrAssignOverwritesExisting) {
    JsonObject o;
    o.insert_or_assign("k", JsonValue{1.0});
    o.insert_or_assign("k", JsonValue{2.0});
    EXPECT_EQ(o.size(), 1u);
    EXPECT_EQ(o.at("k").as_number(), 2.0);
}

TEST(FlatMap, EqualityComparesKeysAndValues) {
    JsonObject a{{"x", JsonValue{1.0}}};
    JsonObject b{{"x", JsonValue{1.0}}};
    JsonObject c{{"x", JsonValue{2.0}}};
    EXPECT_TRUE(a == b);
    EXPECT_TRUE(a != c);
}

// ----- serialize_into + double-format contract -----
//
// Group/join keys are built by appending serialize_into output, and the
// state key format must stay byte-stable across releases. These pin
// (a) serialize_into == serialize(0) appended, and (b) the exact double
// rendering (ostream-default general format, precision 6) the writer
// reproduces without an ostream.

TEST(Json, SerializeIntoAppendsCompactForm) {
    auto v = parse(R"({"b": [1, 2.5, "x\n"], "a": null})");
    std::string out = "prefix|";
    v.serialize_into(out);
    EXPECT_EQ(out, "prefix|" + v.serialize(0));
}

TEST(Json, DoubleRenderingMatchesOstreamGeneralFormat) {
    auto render = [](double d) {
        JsonValue v{d};
        return v.serialize(0);
    };
    EXPECT_EQ(render(0.5), "0.5");
    EXPECT_EQ(render(-0.5), "-0.5");
    EXPECT_EQ(render(0.123456789), "0.123457");   // precision 6, rounded
    EXPECT_EQ(render(1234567.5), "1.23457e+06");  // switches to scientific
    EXPECT_EQ(render(1e300), "1e+300");
    EXPECT_EQ(render(-2.5e-08), "-2.5e-08");
    EXPECT_EQ(render(42.0), "42");  // integral fast path
    EXPECT_EQ(render(-9007199254740992.0), "-9007199254740992");
}

TEST(Json, IntegerTokensAreCarriedExactlyPastDoubleMantissa) {
    // 2^53 + 1 is not representable as a double; the old double-backed value
    // model rounded it to 2^53. It must now round-trip exactly.
    const std::int64_t big = 9007199254740993LL;  // 2^53 + 1
    auto v = parse("9007199254740993");
    EXPECT_TRUE(v.is_number());
    EXPECT_TRUE(v.is_integral_number());
    EXPECT_EQ(v.as_int(), big);
    EXPECT_EQ(v.serialize(0), "9007199254740993");  // no double rounding on the way out

    // A fractional or out-of-range token stays a double (not integral).
    auto d = parse("2.5");
    EXPECT_TRUE(d.is_number());
    EXPECT_FALSE(d.is_integral_number());
    EXPECT_DOUBLE_EQ(d.as_number(), 2.5);
}

TEST(Json, NumbersCompareByValueAcrossIntAndDouble) {
    // An int-constructed and a double-constructed value of the same magnitude
    // are equal (the representation split must be invisible to equality).
    EXPECT_EQ(JsonValue{std::int64_t{5}}, JsonValue{5.0});
    EXPECT_EQ(JsonValue{5}, JsonValue{5.0});
    EXPECT_NE(JsonValue{5}, JsonValue{6.0});
    // Two exact integers past 2^53 compare exactly, not as widened doubles.
    EXPECT_NE(JsonValue{std::int64_t{9007199254740993LL}},
              JsonValue{std::int64_t{9007199254740992LL}});
    // Nested numbers inside arrays/objects also compare by value.
    EXPECT_EQ(parse("[1, 2, 3]"), parse("[1.0, 2.0, 3.0]"));
}

TEST(Json, IntConstructedValueSerialisesAsInteger) {
    EXPECT_EQ((JsonValue{std::int64_t{9223372036854775807LL}}).serialize(0), "9223372036854775807");
    EXPECT_EQ((JsonValue{-42}).serialize(0), "-42");
    EXPECT_EQ((JsonValue{0}).serialize(0), "0");
}

// ----- from_entries + parse_object (the row-decode fast path) -----

TEST(FlatMap, FromEntriesSortedInputIsAdoptedVerbatim) {
    std::vector<JsonObject::value_type> entries;
    entries.emplace_back("a", JsonValue{1.0});
    entries.emplace_back("b", JsonValue{2.0});
    auto m = JsonObject::from_entries(std::move(entries));
    EXPECT_EQ(m.size(), 2u);
    EXPECT_EQ(m.at("a").as_number(), 1.0);
    EXPECT_EQ(m.at("b").as_number(), 2.0);
}

TEST(FlatMap, FromEntriesUnsortedInputIsSortedFirstWins) {
    std::vector<JsonObject::value_type> entries;
    entries.emplace_back("z", JsonValue{1.0});
    entries.emplace_back("a", JsonValue{2.0});
    entries.emplace_back("z", JsonValue{3.0});  // duplicate: first wins
    entries.emplace_back("m", JsonValue{4.0});
    auto m = JsonObject::from_entries(std::move(entries));
    EXPECT_EQ(m.size(), 3u);
    std::string order;
    for (const auto& [k, v] : m)
        order += k;
    EXPECT_EQ(order, "amz");
    EXPECT_EQ(m.at("z").as_number(), 1.0);
}

TEST(FlatMap, FromEntriesEmpty) {
    EXPECT_TRUE(JsonObject::from_entries({}).empty());
}

TEST(Json, ParseObjectMatchesParseForObjects) {
    const std::string text = R"({"z": 1, "a": {"nested": [1, 2]}, "z": 99, "m": "x"})";
    auto direct = parse_object(text);
    ASSERT_TRUE(direct.has_value());
    // Identical to the generic path, including first-duplicate-wins.
    EXPECT_TRUE(JsonValue{*direct} == parse(text));
    EXPECT_EQ(direct->at("z").as_number(), 1.0);
}

TEST(Json, ParseObjectRejectsNonObjectAndMalformed) {
    EXPECT_FALSE(parse_object("[1, 2]").has_value());
    EXPECT_FALSE(parse_object("42").has_value());
    EXPECT_FALSE(parse_object("{broken").has_value());
    EXPECT_FALSE(parse_object("").has_value());
}

TEST(Json, ParseObjectFilteredKeepsOnlyListedKeys) {
    const std::vector<std::string> keep{"b", "z"};
    auto obj = parse_object(R"({"a": 1, "b": {"n": [1, 2]}, "m": "drop", "z": 9, "b": 99})", keep);
    ASSERT_TRUE(obj.has_value());
    EXPECT_EQ(obj->size(), 2u);
    EXPECT_EQ(obj->at("z").as_number(), 9.0);
    // Nested value under a kept key is intact; duplicate kept key first-wins.
    EXPECT_EQ(obj->at("b").at("n").as_array().size(), 2u);
    EXPECT_FALSE(obj->contains("a"));
    EXPECT_FALSE(obj->contains("m"));
}
