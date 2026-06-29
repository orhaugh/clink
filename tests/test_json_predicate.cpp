#include <optional>
#include <string>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/operators/json_predicate.hpp"

namespace clink::operators {

namespace {

auto resolver(clink::config::JsonValue value) {
    return [v = std::move(value)](const std::string&) -> clink::config::JsonValue { return v; };
}

bool eval(const std::string& json_text, clink::config::JsonValue column_value) {
    auto pred = clink::config::parse(json_text);
    auto r = resolver(std::move(column_value));
    return evaluate_json_predicate(pred, r);
}

bool eval_str(const std::string& json_text, std::optional<std::string> column_value) {
    if (column_value.has_value()) {
        return eval(json_text, clink::config::JsonValue{*column_value});
    }
    return eval(json_text, clink::config::JsonValue{nullptr});
}

}  // namespace

TEST(JsonPredicate, EqMatchesExactString) {
    EXPECT_TRUE(eval_str(R"({"op":"eq","col":"x","literal":"hello"})", std::string{"hello"}));
    EXPECT_FALSE(eval_str(R"({"op":"eq","col":"x","literal":"hello"})", std::string{"world"}));
}

TEST(JsonPredicate, NeAndComparisons) {
    EXPECT_TRUE(eval_str(R"({"op":"ne","col":"x","literal":"a"})", std::string{"b"}));
    EXPECT_TRUE(eval_str(R"({"op":"lt","col":"x","literal":"b"})", std::string{"a"}));
    EXPECT_TRUE(eval_str(R"({"op":"le","col":"x","literal":"a"})", std::string{"a"}));
    EXPECT_TRUE(eval_str(R"({"op":"gt","col":"x","literal":"a"})", std::string{"b"}));
    EXPECT_TRUE(eval_str(R"({"op":"ge","col":"x","literal":"b"})", std::string{"b"}));
}

TEST(JsonPredicate, AndOrNot) {
    const std::string and_pred =
        R"({"op":"and","args":[
              {"op":"eq","col":"x","literal":"a"},
              {"op":"ne","col":"x","literal":"b"}
           ]})";
    EXPECT_TRUE(eval_str(and_pred, std::string{"a"}));
    EXPECT_FALSE(eval_str(and_pred, std::string{"b"}));

    const std::string or_pred =
        R"({"op":"or","args":[
              {"op":"eq","col":"x","literal":"a"},
              {"op":"eq","col":"x","literal":"b"}
           ]})";
    EXPECT_TRUE(eval_str(or_pred, std::string{"a"}));
    EXPECT_TRUE(eval_str(or_pred, std::string{"b"}));
    EXPECT_FALSE(eval_str(or_pred, std::string{"c"}));

    const std::string not_pred = R"({"op":"not","arg":{"op":"eq","col":"x","literal":"a"}})";
    EXPECT_FALSE(eval_str(not_pred, std::string{"a"}));
    EXPECT_TRUE(eval_str(not_pred, std::string{"b"}));
}

TEST(JsonPredicate, IsNullAndIsNotNull) {
    EXPECT_TRUE(eval_str(R"({"op":"is_null","col":"x"})", std::nullopt));
    EXPECT_FALSE(eval_str(R"({"op":"is_null","col":"x"})", std::string{""}));
    EXPECT_TRUE(eval_str(R"({"op":"is_not_null","col":"x"})", std::string{"value"}));
    EXPECT_FALSE(eval_str(R"({"op":"is_not_null","col":"x"})", std::nullopt));
}

TEST(JsonPredicate, LikePatterns) {
    EXPECT_TRUE(eval_str(R"({"op":"like","col":"x","pattern":"hello"})", std::string{"hello"}));
    EXPECT_FALSE(eval_str(R"({"op":"like","col":"x","pattern":"hello"})", std::string{"world"}));
    EXPECT_TRUE(eval_str(R"({"op":"like","col":"x","pattern":"hel%"})", std::string{"hello"}));
    EXPECT_TRUE(eval_str(R"({"op":"like","col":"x","pattern":"%lo"})", std::string{"hello"}));
    EXPECT_TRUE(eval_str(R"({"op":"like","col":"x","pattern":"%ll%"})", std::string{"hello"}));
    EXPECT_TRUE(eval_str(R"({"op":"like","col":"x","pattern":"h_llo"})", std::string{"hello"}));
    EXPECT_FALSE(eval_str(R"({"op":"like","col":"x","pattern":"h_llo"})", std::string{"hxxllo"}));
    EXPECT_TRUE(eval_str(R"({"op":"like","col":"x","pattern":"%"})", std::string{""}));
}

// --- Three-valued logic ---------------------------------

namespace {

clink::operators::TriBool eval_tri(const std::string& json_text,
                                   clink::config::JsonValue column_value) {
    auto pred = clink::config::parse(json_text);
    auto r = resolver(std::move(column_value));
    return evaluate_json_predicate_tri(pred, r);
}

}  // namespace

TEST(JsonPredicate, ComparisonWithNullIsUnknown) {
    using TriBool = clink::operators::TriBool;
    EXPECT_EQ(eval_tri(R"({"op":"eq","col":"x","literal":"a"})", clink::config::JsonValue{nullptr}),
              TriBool::Unknown);
    EXPECT_EQ(eval_tri(R"({"op":"lt","col":"x","literal":5})", clink::config::JsonValue{nullptr}),
              TriBool::Unknown);
    EXPECT_EQ(
        eval_tri(R"({"op":"like","col":"x","pattern":"%"})", clink::config::JsonValue{nullptr}),
        TriBool::Unknown);
}

TEST(JsonPredicate, WhereDropsNullPredicateRows) {
    // WHERE keeps only True - both False and Unknown drop the row.
    EXPECT_FALSE(eval_str(R"({"op":"eq","col":"x","literal":"a"})", std::nullopt));
    EXPECT_FALSE(eval_str(R"({"op":"like","col":"x","pattern":"%"})", std::nullopt));
}

TEST(JsonPredicate, IsNullIsAlwaysTwoValued) {
    using TriBool = clink::operators::TriBool;
    EXPECT_EQ(eval_tri(R"({"op":"is_null","col":"x"})", clink::config::JsonValue{nullptr}),
              TriBool::True);
    EXPECT_EQ(eval_tri(R"({"op":"is_null","col":"x"})", clink::config::JsonValue{std::string{""}}),
              TriBool::False);
    EXPECT_EQ(eval_tri(R"({"op":"is_not_null","col":"x"})", clink::config::JsonValue{nullptr}),
              TriBool::False);
}

TEST(JsonPredicate, AndOrNotFollowThreeValuedTruthTables) {
    using TriBool = clink::operators::TriBool;
    using clink::config::JsonValue;
    // unknown AND true = unknown
    const std::string and_unk_t =
        R"({"op":"and","args":[
              {"op":"eq","col":"x","literal":"a"},
              {"op":"is_not_null","col":"x"}
           ]})";
    EXPECT_EQ(eval_tri(and_unk_t, JsonValue{nullptr}), TriBool::False);
    // unknown AND false = false (short-circuits)
    const std::string and_unk_f =
        R"({"op":"and","args":[
              {"op":"eq","col":"x","literal":"a"},
              {"op":"is_null","col":"y"}
           ]})";
    // With x=null and column y not present (resolves null too), is_null is True;
    // eq is Unknown; result = Unknown AND True = Unknown.
    EXPECT_EQ(eval_tri(and_unk_f, JsonValue{nullptr}), TriBool::Unknown);
    // true OR unknown = true (short-circuits)
    const std::string or_t_unk =
        R"({"op":"or","args":[
              {"op":"is_null","col":"x"},
              {"op":"eq","col":"x","literal":"a"}
           ]})";
    EXPECT_EQ(eval_tri(or_t_unk, JsonValue{nullptr}), TriBool::True);
    // false OR unknown = unknown
    const std::string or_f_unk =
        R"({"op":"or","args":[
              {"op":"is_not_null","col":"x"},
              {"op":"eq","col":"x","literal":"a"}
           ]})";
    EXPECT_EQ(eval_tri(or_f_unk, JsonValue{nullptr}), TriBool::Unknown);
    // NOT unknown = unknown
    const std::string not_unk = R"({"op":"not","arg":{"op":"eq","col":"x","literal":"a"}})";
    EXPECT_EQ(eval_tri(not_unk, JsonValue{nullptr}), TriBool::Unknown);
}

TEST(JsonPredicate, RejectsUnknownOp) {
    EXPECT_THROW(eval_str(R"({"op":"weird","col":"x","literal":"y"})", std::string{"z"}),
                 std::runtime_error);
}

// --- Typed-literal comparisons --------------------------

TEST(JsonPredicate, NumericComparisonAvoidsStringLex) {
    using clink::config::JsonValue;
    // Plain numeric: 9 < 100 (with old string-lex this was false).
    EXPECT_TRUE(eval(R"({"op":"lt","col":"x","literal":100})", JsonValue{std::int64_t{9}}));
    EXPECT_FALSE(eval(R"({"op":"gt","col":"x","literal":100})", JsonValue{std::int64_t{9}}));
    EXPECT_TRUE(eval(R"({"op":"ge","col":"x","literal":42})", JsonValue{std::int64_t{42}}));
    EXPECT_TRUE(eval(R"({"op":"eq","col":"x","literal":42})", JsonValue{std::int64_t{42}}));
}

TEST(JsonPredicate, NumericVsStringCoerces) {
    using clink::config::JsonValue;
    // Column is a string "5", literal is int 5: coerces to numeric.
    EXPECT_TRUE(eval(R"({"op":"eq","col":"x","literal":5})", JsonValue{std::string{"5"}}));
    EXPECT_TRUE(eval(R"({"op":"lt","col":"x","literal":10})", JsonValue{std::string{"9"}}));
    // Non-numeric string vs numeric literal: collapses to false.
    EXPECT_FALSE(eval(R"({"op":"eq","col":"x","literal":5})", JsonValue{std::string{"hi"}}));
}

TEST(JsonPredicate, BoolEquality) {
    using clink::config::JsonValue;
    EXPECT_TRUE(eval(R"({"op":"eq","col":"x","literal":true})", JsonValue{true}));
    EXPECT_FALSE(eval(R"({"op":"eq","col":"x","literal":true})", JsonValue{false}));
}

TEST(JsonPredicate, InMatchesAnyValue) {
    using clink::config::JsonValue;
    EXPECT_TRUE(eval(R"({"op":"in","col":"x","values":[1,2,3]})", JsonValue{std::int64_t{2}}));
    EXPECT_FALSE(eval(R"({"op":"in","col":"x","values":[1,2,3]})", JsonValue{std::int64_t{4}}));
    EXPECT_TRUE(eval(R"({"op":"in","col":"x","values":["a","b"]})", JsonValue{std::string{"a"}}));
}

TEST(JsonPredicate, InEmptyListIsFalse) {
    using clink::config::JsonValue;
    EXPECT_FALSE(eval(R"({"op":"in","col":"x","values":[]})", JsonValue{std::int64_t{1}}));
}

TEST(JsonPredicate, InWithNullColumnIsUnknown) {
    using clink::config::JsonValue;
    EXPECT_FALSE(eval(R"({"op":"in","col":"x","values":[1,2]})", JsonValue{nullptr}));
}

TEST(JsonPredicate, InWithNullValueIsUnknownOnMiss) {
    using clink::config::JsonValue;
    // x=4 not in (1,2,null): Unknown -> drops under two-valued WHERE
    EXPECT_FALSE(eval(R"({"op":"in","col":"x","values":[1,2,null]})", JsonValue{std::int64_t{4}}));
    // x=1 matches even though list contains null.
    EXPECT_TRUE(eval(R"({"op":"in","col":"x","values":[1,2,null]})", JsonValue{std::int64_t{1}}));
}

}  // namespace clink::operators
