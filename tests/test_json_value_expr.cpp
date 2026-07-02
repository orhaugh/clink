#include <string>
#include <utility>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/operators/json_value_expr.hpp"

namespace clink::operators {

namespace {

using clink::config::JsonValue;

auto resolver_from(std::map<std::string, JsonValue> table) {
    return [table = std::move(table)](const std::string& name) -> JsonValue {
        auto it = table.find(name);
        if (it == table.end())
            return JsonValue{nullptr};
        return it->second;
    };
}

JsonValue eval(const std::string& json_text, std::map<std::string, JsonValue> row = {}) {
    auto expr = clink::config::parse(json_text);
    auto r = resolver_from(std::move(row));
    return evaluate_json_value_expr(expr, r);
}

}  // namespace

TEST(JsonValueExpr, LiteralsRoundTrip) {
    EXPECT_TRUE(eval(R"({"lit":42})").is_number());
    EXPECT_EQ(eval(R"({"lit":42})").as_number(), 42.0);
    EXPECT_TRUE(eval(R"({"lit":"hello"})").is_string());
    EXPECT_EQ(eval(R"({"lit":"hello"})").as_string(), "hello");
    EXPECT_TRUE(eval(R"({"lit":true})").is_bool());
    EXPECT_TRUE(eval(R"({"lit":null})").is_null());
}

TEST(JsonValueExpr, ColumnRefViaResolver) {
    auto v = eval(R"({"col":"name"})", {{"name", JsonValue{std::string{"alice"}}}});
    ASSERT_TRUE(v.is_string());
    EXPECT_EQ(v.as_string(), "alice");
}

TEST(JsonValueExpr, ArithmeticBasics) {
    auto v = eval(R"({"op":"add","args":[{"lit":3},{"lit":4}]})");
    ASSERT_TRUE(v.is_number());
    EXPECT_EQ(v.as_number(), 7.0);
    EXPECT_EQ(eval(R"({"op":"sub","args":[{"lit":10},{"lit":3}]})").as_number(), 7.0);
    EXPECT_EQ(eval(R"({"op":"mul","args":[{"lit":3},{"lit":4}]})").as_number(), 12.0);
    EXPECT_EQ(eval(R"({"op":"div","args":[{"lit":10},{"lit":4}]})").as_number(), 2.5);
    EXPECT_EQ(eval(R"({"op":"mod","args":[{"lit":10},{"lit":3}]})").as_number(), 1.0);
}

TEST(JsonValueExpr, ArithmeticNullPropagates) {
    EXPECT_TRUE(eval(R"({"op":"add","args":[{"col":"missing"},{"lit":1}]})").is_null());
    EXPECT_TRUE(eval(R"({"op":"mul","args":[{"lit":1},{"col":"missing"}]})").is_null());
}

TEST(JsonValueExpr, DivByZeroIsNan) {
    auto v = eval(R"({"op":"div","args":[{"lit":1},{"lit":0}]})");
    ASSERT_TRUE(v.is_number());
    EXPECT_TRUE(std::isnan(v.as_number()));
}

TEST(JsonValueExpr, Concat) {
    auto v = eval(R"({"op":"concat","args":[{"lit":"a"},{"lit":"_"},{"lit":"b"}]})");
    ASSERT_TRUE(v.is_string());
    EXPECT_EQ(v.as_string(), "a_b");
}

TEST(JsonValueExpr, UpperLowerLength) {
    EXPECT_EQ(eval(R"({"op":"upper","args":[{"lit":"hello"}]})").as_string(), "HELLO");
    EXPECT_EQ(eval(R"({"op":"lower","args":[{"lit":"HELLO"}]})").as_string(), "hello");
    EXPECT_EQ(eval(R"({"op":"length","args":[{"lit":"hello"}]})").as_number(), 5.0);
    EXPECT_TRUE(eval(R"({"op":"length","args":[{"col":"missing"}]})").is_null());
}

TEST(JsonValueExpr, Coalesce) {
    EXPECT_EQ(
        eval(R"({"op":"coalesce","args":[{"col":"missing"},{"lit":"fallback"}]})").as_string(),
        "fallback");
    EXPECT_EQ(eval(R"({"op":"coalesce","args":[{"lit":"first"},{"lit":"second"}]})").as_string(),
              "first");
    EXPECT_TRUE(eval(R"({"op":"coalesce","args":[{"col":"a"},{"col":"b"}]})").is_null());
}

TEST(JsonValueExpr, Casts) {
    EXPECT_EQ(eval(R"({"op":"cast_int","args":[{"lit":"42"}]})").as_number(), 42.0);
    EXPECT_EQ(eval(R"({"op":"cast_int","args":[{"lit":3.7}]})").as_number(), 3.0);
    EXPECT_EQ(eval(R"({"op":"cast_float","args":[{"lit":"3.14"}]})").as_number(), 3.14);
    EXPECT_EQ(eval(R"({"op":"cast_str","args":[{"lit":42}]})").as_string(), "42");
    EXPECT_TRUE(eval(R"({"op":"cast_int","args":[{"lit":"not a number"}]})").is_null());
}

TEST(JsonValueExpr, NestedArithmeticWithColumnRefs) {
    auto v = eval(R"({"op":"add","args":[
                       {"col":"x"},
                       {"op":"mul","args":[{"col":"y"},{"lit":2}]}
                    ]})",
                  {{"x", JsonValue{std::int64_t{10}}}, {"y", JsonValue{std::int64_t{3}}}});
    ASSERT_TRUE(v.is_number());
    EXPECT_EQ(v.as_number(), 16.0);
}

// --- Extended scalar built-ins -------------------------

TEST(JsonValueExpr, Substring) {
    EXPECT_EQ(
        eval(R"({"op":"substring","args":[{"lit":"hello"},{"lit":2},{"lit":3}]})").as_string(),
        "ell");
    EXPECT_EQ(eval(R"({"op":"substring","args":[{"lit":"hello"},{"lit":3}]})").as_string(), "llo");
    EXPECT_EQ(eval(R"({"op":"substring","args":[{"lit":"hello"},{"lit":10}]})").as_string(), "");
    EXPECT_TRUE(eval(R"({"op":"substring","args":[{"col":"m"},{"lit":1},{"lit":1}]})").is_null());
}

TEST(JsonValueExpr, Position) {
    EXPECT_EQ(
        eval(R"({"op":"position","args":[{"lit":"hello world"},{"lit":"world"}]})").as_number(),
        7.0);
    EXPECT_EQ(eval(R"({"op":"position","args":[{"lit":"hello"},{"lit":"zz"}]})").as_number(), 0.0);
    EXPECT_TRUE(eval(R"({"op":"position","args":[{"col":"m"},{"lit":"x"}]})").is_null());
}

TEST(JsonValueExpr, Replace) {
    EXPECT_EQ(eval(R"({"op":"replace","args":[{"lit":"foo_foo"},{"lit":"foo"},{"lit":"bar"}]})")
                  .as_string(),
              "bar_bar");
    EXPECT_EQ(eval(R"({"op":"replace","args":[{"lit":"abc"},{"lit":""},{"lit":"-"}]})").as_string(),
              "abc");
}

TEST(JsonValueExpr, TrimVariants) {
    EXPECT_EQ(eval(R"({"op":"btrim","args":[{"lit":"  hi  "}]})").as_string(), "hi");
    EXPECT_EQ(eval(R"({"op":"ltrim","args":[{"lit":"  hi  "}]})").as_string(), "hi  ");
    EXPECT_EQ(eval(R"({"op":"rtrim","args":[{"lit":"  hi  "}]})").as_string(), "  hi");
    EXPECT_EQ(eval(R"({"op":"btrim","args":[{"lit":"xxhixx"},{"lit":"x"}]})").as_string(), "hi");
}

TEST(JsonValueExpr, RegexpExtract) {
    // 2 args -> the whole match (group 0).
    EXPECT_EQ(eval(R"({"op":"regexp_extract","args":[{"lit":"abc123def"},{"lit":"[0-9]+"}]})")
                  .as_string(),
              "123");
    // 3 args -> the given capture group. (Custom raw-string delimiter: the
    // regex contains )" which would otherwise close a plain R"(...)".)
    EXPECT_EQ(
        eval(
            R"j({"op":"regexp_extract","args":[{"lit":"id=42&x=9"},{"lit":"id=([0-9]+)"},{"lit":1}]})j")
            .as_string(),
        "42");
    // No match -> null.
    EXPECT_TRUE(
        eval(R"({"op":"regexp_extract","args":[{"lit":"foo"},{"lit":"[0-9]+"}]})").is_null());
    // Group out of range -> null.
    EXPECT_TRUE(eval(R"({"op":"regexp_extract","args":[{"lit":"a1"},{"lit":"[0-9]+"},{"lit":5}]})")
                    .is_null());
    // Null arg -> null.
    EXPECT_TRUE(eval(R"({"op":"regexp_extract","args":[{"col":"m"},{"lit":"x"}]})").is_null());
}

TEST(JsonValueExpr, SplitIndex) {
    // 0-based (Flink semantics).
    EXPECT_EQ(
        eval(R"({"op":"split_index","args":[{"lit":"a/b/c"},{"lit":"/"},{"lit":0}]})").as_string(),
        "a");
    EXPECT_EQ(
        eval(R"({"op":"split_index","args":[{"lit":"a/b/c"},{"lit":"/"},{"lit":2}]})").as_string(),
        "c");
    // Multi-character delimiter.
    EXPECT_EQ(eval(R"({"op":"split_index","args":[{"lit":"a::b::c"},{"lit":"::"},{"lit":1}]})")
                  .as_string(),
              "b");
    // Index past the last field -> null.
    EXPECT_TRUE(
        eval(R"({"op":"split_index","args":[{"lit":"a/b"},{"lit":"/"},{"lit":5}]})").is_null());
    // Null arg -> null.
    EXPECT_TRUE(
        eval(R"({"op":"split_index","args":[{"col":"m"},{"lit":"/"},{"lit":0}]})").is_null());
}

TEST(JsonValueExpr, AbsFloorCeilRound) {
    EXPECT_EQ(eval(R"({"op":"abs","args":[{"lit":-3.5}]})").as_number(), 3.5);
    EXPECT_EQ(eval(R"({"op":"floor","args":[{"lit":3.7}]})").as_number(), 3.0);
    EXPECT_EQ(eval(R"({"op":"ceil","args":[{"lit":3.2}]})").as_number(), 4.0);
    EXPECT_EQ(eval(R"({"op":"round","args":[{"lit":3.5}]})").as_number(), 4.0);
    EXPECT_EQ(eval(R"({"op":"round","args":[{"lit":3.14159},{"lit":2}]})").as_number(), 3.14);
    EXPECT_TRUE(eval(R"({"op":"abs","args":[{"col":"m"}]})").is_null());
}

TEST(JsonValueExpr, NullIf) {
    EXPECT_TRUE(eval(R"({"op":"nullif","args":[{"lit":1},{"lit":1}]})").is_null());
    EXPECT_EQ(eval(R"({"op":"nullif","args":[{"lit":1},{"lit":2}]})").as_number(), 1.0);
    EXPECT_TRUE(eval(R"({"op":"nullif","args":[{"col":"m"},{"lit":1}]})").is_null());
}

TEST(JsonValueExpr, GreatestLeastIgnoreNulls) {
    EXPECT_EQ(eval(R"({"op":"greatest","args":[{"lit":1},{"lit":7},{"lit":3}]})").as_number(), 7.0);
    EXPECT_EQ(eval(R"({"op":"least","args":[{"lit":5},{"lit":2},{"lit":9}]})").as_number(), 2.0);
    EXPECT_EQ(eval(R"({"op":"greatest","args":[{"col":"m"},{"lit":4}]})").as_number(), 4.0);
    EXPECT_TRUE(eval(R"({"op":"greatest","args":[{"col":"m"},{"col":"n"}]})").is_null());
}

TEST(JsonValueExpr, CaseFirstMatchingThenWins) {
    auto v = eval(R"({"op":"case","branches":[
                       {"when":{"op":"gt","col":"score","literal":50},"then":{"lit":"high"}},
                       {"when":{"op":"gt","col":"score","literal":0},"then":{"lit":"low"}}
                     ],"else":{"lit":"zero"}})",
                  {{"score", JsonValue{std::int64_t{60}}}});
    ASSERT_TRUE(v.is_string());
    EXPECT_EQ(v.as_string(), "high");
}

TEST(JsonValueExpr, CaseFallsThroughToElse) {
    auto v = eval(R"({"op":"case","branches":[
                       {"when":{"op":"gt","col":"score","literal":50},"then":{"lit":"high"}}
                     ],"else":{"lit":"low"}})",
                  {{"score", JsonValue{std::int64_t{3}}}});
    EXPECT_EQ(v.as_string(), "low");
}

TEST(JsonValueExpr, CaseWithoutElseReturnsNullWhenNoMatch) {
    auto v = eval(R"({"op":"case","branches":[
                       {"when":{"op":"eq","col":"x","literal":1},"then":{"lit":"one"}}
                     ]})",
                  {{"x", JsonValue{std::int64_t{2}}}});
    EXPECT_TRUE(v.is_null());
}

TEST(JsonValueExpr, CaseUnknownWhenDoesNotMatch) {
    // Comparing a NULL column with a literal yields Unknown under
    // three-valued logic; that branch must NOT match.
    auto v = eval(R"({"op":"case","branches":[
                       {"when":{"op":"eq","col":"missing","literal":1},"then":{"lit":"hit"}}
                     ],"else":{"lit":"miss"}})");
    EXPECT_EQ(v.as_string(), "miss");
}

// --- Parity wave: extended string builtins ---

TEST(JsonValueExpr, CharLengthAndAscii) {
    EXPECT_EQ(eval(R"({"op":"char_length","args":[{"lit":"hello"}]})").as_number(), 5.0);
    EXPECT_EQ(eval(R"({"op":"character_length","args":[{"lit":"hi"}]})").as_number(), 2.0);
    EXPECT_EQ(eval(R"({"op":"ascii","args":[{"lit":"A"}]})").as_number(), 65.0);
    EXPECT_TRUE(eval(R"({"op":"char_length","args":[{"col":"x"}]})").is_null());
}

TEST(JsonValueExpr, ReverseRepeatInitcapChr) {
    EXPECT_EQ(eval(R"({"op":"reverse","args":[{"lit":"abc"}]})").as_string(), "cba");
    EXPECT_EQ(eval(R"({"op":"repeat","args":[{"lit":"ab"},{"lit":3}]})").as_string(), "ababab");
    EXPECT_EQ(eval(R"({"op":"initcap","args":[{"lit":"hello world"}]})").as_string(),
              "Hello World");
    EXPECT_EQ(eval(R"({"op":"chr","args":[{"lit":65}]})").as_string(), "A");
}

TEST(JsonValueExpr, LeftRightLpadRpad) {
    EXPECT_EQ(eval(R"({"op":"left","args":[{"lit":"hello"},{"lit":3}]})").as_string(), "hel");
    EXPECT_EQ(eval(R"({"op":"right","args":[{"lit":"hello"},{"lit":2}]})").as_string(), "lo");
    EXPECT_EQ(eval(R"({"op":"right","args":[{"lit":"hi"},{"lit":9}]})").as_string(), "hi");
    EXPECT_EQ(eval(R"({"op":"lpad","args":[{"lit":"7"},{"lit":3},{"lit":"0"}]})").as_string(),
              "007");
    EXPECT_EQ(eval(R"({"op":"rpad","args":[{"lit":"7"},{"lit":3},{"lit":"0"}]})").as_string(),
              "700");
    EXPECT_EQ(eval(R"({"op":"lpad","args":[{"lit":"abcd"},{"lit":2}]})").as_string(), "ab");
}

TEST(JsonValueExpr, SplitPartAndStartsWith) {
    EXPECT_EQ(
        eval(R"({"op":"split_part","args":[{"lit":"a,b,c"},{"lit":","},{"lit":2}]})").as_string(),
        "b");
    EXPECT_EQ(
        eval(R"({"op":"split_part","args":[{"lit":"a,b"},{"lit":","},{"lit":9}]})").as_string(),
        "");
    EXPECT_TRUE(eval(R"({"op":"starts_with","args":[{"lit":"hello"},{"lit":"he"}]})").as_bool());
    EXPECT_FALSE(eval(R"({"op":"starts_with","args":[{"lit":"hello"},{"lit":"x"}]})").as_bool());
}

// --- Parity wave: extended math builtins ---

TEST(JsonValueExpr, MathBuiltins) {
    EXPECT_EQ(eval(R"({"op":"sqrt","args":[{"lit":9}]})").as_number(), 3.0);
    EXPECT_EQ(eval(R"({"op":"power","args":[{"lit":2},{"lit":10}]})").as_number(), 1024.0);
    EXPECT_EQ(eval(R"({"op":"pow","args":[{"lit":3},{"lit":2}]})").as_number(), 9.0);
    EXPECT_EQ(eval(R"({"op":"sign","args":[{"lit":-5}]})").as_number(), -1.0);
    EXPECT_EQ(eval(R"({"op":"sign","args":[{"lit":7}]})").as_number(), 1.0);
    EXPECT_EQ(eval(R"({"op":"log10","args":[{"lit":1000}]})").as_number(), 3.0);
    EXPECT_EQ(eval(R"({"op":"trunc","args":[{"lit":3.78}]})").as_number(), 3.0);
    EXPECT_EQ(eval(R"({"op":"trunc","args":[{"lit":3.14159},{"lit":2}]})").as_number(), 3.14);
    EXPECT_TRUE(eval(R"({"op":"sqrt","args":[{"col":"x"}]})").is_null());
}

// Date/time builtins operate on epoch-millis (UTC). Anchored to known
// instants: 0 = 1970-01-01 00:00:00 (a Thursday); 1000000000000 ms =
// 2001-09-09 01:46:40.
TEST(JsonValueExpr, ExtractFields) {
    auto ex = [](const char* field, long long ms) {
        return eval(std::string{R"({"op":"extract","args":[{"lit":")"} + field + R"("},{"lit":)" +
                    std::to_string(ms) + "}]}")
            .as_number();
    };
    EXPECT_EQ(ex("year", 0), 1970);
    EXPECT_EQ(ex("month", 0), 1);
    EXPECT_EQ(ex("day", 0), 1);
    EXPECT_EQ(ex("dow", 0), 4);  // Thursday
    EXPECT_EQ(ex("year", 1000000000000LL), 2001);
    EXPECT_EQ(ex("month", 1000000000000LL), 9);
    EXPECT_EQ(ex("day", 1000000000000LL), 9);
    EXPECT_EQ(ex("hour", 1000000000000LL), 1);
    EXPECT_EQ(ex("minute", 1000000000000LL), 46);
    EXPECT_EQ(ex("second", 1000000000000LL), 40);
    EXPECT_EQ(ex("quarter", 1000000000000LL), 3);
    EXPECT_EQ(ex("epoch", 1000000000000LL), 1000000000);
    EXPECT_TRUE(eval(R"({"op":"extract","args":[{"lit":"year"},{"lit":null}]})").is_null());
}

TEST(JsonValueExpr, DateTruncUnits) {
    // 2001-09-09 01:46:40 -> truncations.
    EXPECT_EQ(
        eval(R"({"op":"date_trunc","args":[{"lit":"day"},{"lit":1000000000000}]})").as_number(),
        999993600000);  // 2001-09-09 00:00:00
    EXPECT_EQ(
        eval(R"({"op":"date_trunc","args":[{"lit":"hour"},{"lit":1000000000000}]})").as_number(),
        999997200000);  // 2001-09-09 01:00:00
    EXPECT_EQ(
        eval(R"({"op":"date_trunc","args":[{"lit":"minute"},{"lit":1000000000000}]})").as_number(),
        999999960000);  // 2001-09-09 01:46:00
    // Truncating to year is monotonic and idempotent.
    auto y = static_cast<long long>(
        eval(R"({"op":"date_trunc","args":[{"lit":"year"},{"lit":1000000000000}]})").as_number());
    EXPECT_EQ(eval(R"({"op":"extract","args":[{"lit":"year"},{"lit":)" + std::to_string(y) + "}]}")
                  .as_number(),
              2001);
    EXPECT_EQ(eval(R"({"op":"extract","args":[{"lit":"month"},{"lit":)" + std::to_string(y) + "}]}")
                  .as_number(),
              1);
}

TEST(JsonValueExpr, DateFormatAndToTimestampRoundTrip) {
    EXPECT_EQ(eval(R"({"op":"date_format","args":[{"lit":0},{"lit":"yyyy-MM-dd HH:mm:ss"}]})")
                  .as_string(),
              "1970-01-01 00:00:00");
    EXPECT_EQ(
        eval(R"({"op":"date_format","args":[{"lit":1000000000000},{"lit":"yyyy-MM-dd HH:mm:ss"}]})")
            .as_string(),
        "2001-09-09 01:46:40");
    // Default-format TO_TIMESTAMP parses 'yyyy-MM-dd HH:mm:ss'.
    EXPECT_EQ(eval(R"({"op":"to_timestamp","args":[{"lit":"2001-09-09 01:46:40"}]})").as_number(),
              1000000000000);
    // Round-trip with an explicit format.
    EXPECT_EQ(
        eval(R"({"op":"date_format","args":[{"op":"to_timestamp","args":[{"lit":"2021-06-15"},)"
             R"({"lit":"yyyy-MM-dd"}]},{"lit":"yyyy-MM-dd"}]})")
            .as_string(),
        "2021-06-15");
    // A string that does not match the format yields null.
    EXPECT_TRUE(eval(R"({"op":"to_timestamp","args":[{"lit":"not-a-date"}]})").is_null());
}

// JSON-path functions. The document arrives via the resolver as a
// JSON string (the common case); coerce_json parses it.
TEST(JsonValueExpr, JsonValueExtractsScalars) {
    const JsonValue doc{std::string{R"({"user":{"name":"ann","age":30},"tags":["x","y"]})"}};
    auto jv = [&](const char* path) {
        return eval(
            std::string{R"({"op":"json_value","args":[{"col":"p"},{"lit":")"} + path + R"("}]})",
            {{"p", doc}});
    };
    EXPECT_EQ(jv("$.user.name").as_string(), "ann");
    EXPECT_EQ(jv("$.user.age").as_string(), "30");  // number rendered as text
    EXPECT_EQ(jv("$.tags[1]").as_string(), "y");
    EXPECT_TRUE(jv("$.user.email").is_null());  // missing path
    EXPECT_TRUE(jv("$.user").is_null());        // object is not a scalar
    EXPECT_TRUE(jv("$.tags").is_null());        // array is not a scalar
}

TEST(JsonValueExpr, JsonValueAcceptsNativeObject) {
    // When the source already parsed the field, the arg is structured.
    auto v = eval(R"({"op":"json_value","args":[{"col":"p"},{"lit":"$.a"}]})",
                  {{"p", clink::config::parse(R"({"a":42})")}});
    EXPECT_EQ(v.as_string(), "42");
}

TEST(JsonValueExpr, JsonQueryReturnsObjectsAndArrays) {
    const JsonValue doc{std::string{R"({"user":{"name":"ann","age":30},"tags":["x","y"]})"}};
    auto jq = [&](const char* path) {
        return eval(
            std::string{R"({"op":"json_query","args":[{"col":"p"},{"lit":")"} + path + R"("}]})",
            {{"p", doc}});
    };
    EXPECT_EQ(jq("$.user").as_string(), R"({"age":30,"name":"ann"})");  // keys sorted
    EXPECT_EQ(jq("$.tags").as_string(), R"(["x","y"])");
    EXPECT_TRUE(jq("$.user.name").is_null());  // scalar -> null for JSON_QUERY
}

TEST(JsonValueExpr, JsonExistsAndConstruct) {
    const JsonValue doc{std::string{R"({"user":{"name":"ann"}})"}};
    auto je = [&](const char* path) {
        return eval(
            std::string{R"({"op":"json_exists","args":[{"col":"p"},{"lit":")"} + path + R"("}]})",
            {{"p", doc}});
    };
    EXPECT_TRUE(je("$.user.name").as_bool());
    EXPECT_FALSE(je("$.user.email").as_bool());
    EXPECT_FALSE(je("bogus").as_bool());  // malformed path
    // JSON_OBJECT builds JSON text with sorted keys; values keep type.
    EXPECT_EQ(eval(R"({"op":"json_object","args":[{"lit":"b"},{"lit":2},{"lit":"a"},{"lit":"x"}]})")
                  .as_string(),
              R"({"a":"x","b":2})");
    // Null document propagates to null.
    EXPECT_TRUE(eval(R"({"op":"json_value","args":[{"lit":null},{"lit":"$.a"}]})").is_null());
}

// --- Wave 5c: ROW make_row / field_at -----------------------------------

TEST(JsonValueExpr, MakeRowBuildsNamedObject) {
    auto v = eval(R"({"op":"make_row","fields":["a","b"],"args":[{"lit":1},{"lit":"x"}]})");
    ASSERT_TRUE(v.is_object());
    EXPECT_EQ(v.as_object().at("a").as_number(), 1.0);
    EXPECT_EQ(v.as_object().at("b").as_string(), "x");
}

TEST(JsonValueExpr, MakeRowKeepsNullFieldAndIsOrderCanonical) {
    // A NULL field value is kept; std::map keys mean ROW(b,a) == ROW(a,b)
    // serialises identically (canonical order) for changelog equality.
    auto v1 = eval(R"({"op":"make_row","fields":["b","a"],"args":[{"lit":2},{"lit":null}]})");
    auto v2 = eval(R"({"op":"make_row","fields":["a","b"],"args":[{"lit":null},{"lit":2}]})");
    ASSERT_TRUE(v1.is_object());
    EXPECT_TRUE(v1.as_object().at("a").is_null());
    EXPECT_EQ(v1.as_object().at("b").as_number(), 2.0);
    EXPECT_EQ(v1.serialize(0), v2.serialize(0));
}

TEST(JsonValueExpr, FieldAtLooksUpNamedFieldElseNull) {
    const std::string row = R"({"op":"make_row","fields":["city","age"],)"
                            R"("args":[{"lit":"London"},{"lit":30}]})";
    EXPECT_EQ(eval(R"({"op":"field_at","field":"city","args":[)" + row + "]}").as_string(),
              "London");
    EXPECT_EQ(eval(R"({"op":"field_at","field":"age","args":[)" + row + "]}").as_number(), 30.0);
    // Missing field -> NULL; non-object base -> NULL.
    EXPECT_TRUE(eval(R"({"op":"field_at","field":"nope","args":[)" + row + "]}").is_null());
    EXPECT_TRUE(eval(R"({"op":"field_at","field":"x","args":[{"lit":5}]})").is_null());
}

TEST(JsonValueExpr, FieldAtOnNestedSourceObject) {
    // The dominant ROW use: deconstruct a nested object source column.
    const JsonValue profile{clink::config::parse(R"({"city":"Paris","age":41})")};
    EXPECT_EQ(eval(R"({"op":"field_at","field":"city","args":[{"col":"p"}]})", {{"p", profile}})
                  .as_string(),
              "Paris");
    EXPECT_TRUE(eval(R"({"op":"field_at","field":"gone","args":[{"col":"p"}]})", {{"p", profile}})
                    .is_null());
}

// --- Wave 5b: MAP map / map_get -----------------------------------------

TEST(JsonValueExpr, MapBuildsObjectAndLastWins) {
    auto v = eval(R"({"op":"map","args":[{"lit":"k1"},{"lit":1},{"lit":"k2"},{"lit":2}]})");
    ASSERT_TRUE(v.is_object());
    EXPECT_EQ(v.as_object().at("k1").as_number(), 1.0);
    EXPECT_EQ(v.as_object().at("k2").as_number(), 2.0);
    // Duplicate key is last-wins.
    auto d = eval(R"({"op":"map","args":[{"lit":"k"},{"lit":"a"},{"lit":"k"},{"lit":"b"}]})");
    EXPECT_EQ(d.as_object().at("k").as_string(), "b");
}

TEST(JsonValueExpr, MapNullKeyYieldsNullMap) {
    EXPECT_TRUE(eval(R"({"op":"map","args":[{"lit":null},{"lit":1}]})").is_null());
}

TEST(JsonValueExpr, MapKeyCollapseAcrossIntAndString) {
    // Documented v1: INT 1 and VARCHAR '1' canonicalise to the same key.
    auto v = eval(R"({"op":"map","args":[{"lit":1},{"lit":"a"},{"lit":"1"},{"lit":"b"}]})");
    ASSERT_TRUE(v.is_object());
    EXPECT_EQ(v.as_object().size(), 1u);
    EXPECT_EQ(v.as_object().at("1").as_string(), "b");
}

TEST(JsonValueExpr, MapGetLooksUpKeyElseNull) {
    const std::string m =
        R"({"op":"map","args":[{"lit":"US"},{"lit":"United States"},{"lit":"GB"},{"lit":"UK"}]})";
    EXPECT_EQ(eval(R"({"op":"map_get","args":[)" + m + R"(,{"lit":"US"}]})").as_string(),
              "United States");
    // Absent key, NULL key, and non-map base all yield NULL.
    EXPECT_TRUE(eval(R"({"op":"map_get","args":[)" + m + R"(,{"lit":"XX"}]})").is_null());
    EXPECT_TRUE(eval(R"({"op":"map_get","args":[)" + m + R"(,{"lit":null}]})").is_null());
    EXPECT_TRUE(eval(R"({"op":"map_get","args":[{"lit":5},{"lit":"k"}]})").is_null());
}

// --- functions not previously exercised (coverage of the dispatch tail) ---

TEST(JsonValueExpr, Neg) {
    EXPECT_EQ(eval(R"({"op":"neg","args":[{"lit":5}]})").as_number(), -5.0);
    EXPECT_EQ(eval(R"({"op":"neg","args":[{"lit":-2.5}]})").as_number(), 2.5);
    EXPECT_TRUE(eval(R"({"op":"neg","args":[{"col":"missing"}]})").is_null());
}

TEST(JsonValueExpr, ExpAndLn) {
    EXPECT_DOUBLE_EQ(eval(R"({"op":"exp","args":[{"lit":0}]})").as_number(), 1.0);
    EXPECT_DOUBLE_EQ(eval(R"({"op":"ln","args":[{"lit":1}]})").as_number(), 0.0);
    // exp(ln(x)) round-trips.
    const double e = eval(R"({"op":"exp","args":[{"lit":1}]})").as_number();
    EXPECT_NEAR(e, 2.718281828, 1e-6);
    EXPECT_TRUE(eval(R"({"op":"ln","args":[{"col":"missing"}]})").is_null());
}

TEST(JsonValueExpr, CeilingAliasOfCeil) {
    EXPECT_EQ(eval(R"({"op":"ceiling","args":[{"lit":1.2}]})").as_number(), 2.0);
    EXPECT_EQ(eval(R"({"op":"ceiling","args":[{"lit":-1.2}]})").as_number(), -1.0);
}

TEST(JsonValueExpr, CastBool) {
    EXPECT_TRUE(eval(R"({"op":"cast_bool","args":[{"lit":5}]})").as_bool());
    EXPECT_FALSE(eval(R"({"op":"cast_bool","args":[{"lit":0}]})").as_bool());
    EXPECT_TRUE(eval(R"({"op":"cast_bool","args":[{"lit":"yes"}]})").as_bool());
    EXPECT_FALSE(eval(R"({"op":"cast_bool","args":[{"lit":"off"}]})").as_bool());
    EXPECT_TRUE(eval(R"({"op":"cast_bool","args":[{"lit":true}]})").as_bool());
    // Unrecognised string and NULL -> NULL (UNKNOWN).
    EXPECT_TRUE(eval(R"({"op":"cast_bool","args":[{"lit":"maybe"}]})").is_null());
    EXPECT_TRUE(eval(R"({"op":"cast_bool","args":[{"col":"missing"}]})").is_null());
}

TEST(JsonValueExpr, CastDecimal) {
    // Valid re-quantise to scale=2 within precision=10 -> non-null.
    EXPECT_FALSE(
        eval(R"({"op":"cast_decimal","args":[{"lit":"3.14159"}],"scale":2,"precision":10})")
            .is_null());
    // Overflow past the declared precision -> NULL.
    EXPECT_TRUE(
        eval(R"({"op":"cast_decimal","args":[{"lit":"123456.789"}],"scale":2,"precision":3})")
            .is_null());
    EXPECT_TRUE(eval(R"({"op":"cast_decimal","args":[{"col":"missing"}],"scale":2})").is_null());
}

TEST(JsonValueExpr, MakeArrayAndElementAt) {
    const std::string arr = R"({"op":"make_array","args":[{"lit":"a"},{"lit":"b"},{"lit":"c"}]})";
    auto v = eval(arr);
    ASSERT_TRUE(v.is_array());
    EXPECT_EQ(v.as_array().size(), 3u);
    // element_at is 1-based.
    EXPECT_EQ(eval(R"({"op":"element_at","args":[)" + arr + R"(,{"lit":1}]})").as_string(), "a");
    EXPECT_EQ(eval(R"({"op":"element_at","args":[)" + arr + R"(,{"lit":3}]})").as_string(), "c");
    // Out-of-range, non-array base, and NULL index all yield NULL (no error).
    EXPECT_TRUE(eval(R"({"op":"element_at","args":[)" + arr + R"(,{"lit":0}]})").is_null());
    EXPECT_TRUE(eval(R"({"op":"element_at","args":[)" + arr + R"(,{"lit":9}]})").is_null());
    EXPECT_TRUE(eval(R"({"op":"element_at","args":[{"lit":5},{"lit":1}]})").is_null());
    EXPECT_TRUE(eval(R"({"op":"element_at","args":[)" + arr + R"(,{"col":"missing"}]})").is_null());
}

}  // namespace clink::operators
