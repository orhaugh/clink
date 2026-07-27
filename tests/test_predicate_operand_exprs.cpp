// WHERE predicates whose operands are expressions, not bare column references.
//
// `WHERE MOD(auction, 123) = 0` and `WHERE 0.908 * price > 1000000` are ordinary
// SQL and two of the nexmark queries, but the predicate format compares a NAMED
// COLUMN against a literal or another column - there is no column to name. The
// binder now emits such an operand as col_expr / rhs_expr, and
// PredicateOperandExprs binds each to a synthetic name so the predicate handed
// to every existing evaluator is the plain named-column shape it already
// handles.
//
// Claims under test:
//   - the rewrite is faithful: expression operands become synthetic references,
//     and a predicate without one comes back untouched and empty(),
//   - the filter drops exactly the rows the expression says to, on the row path
//     and the columnar path, with identical results (parity),
//   - the columnar path still decodes zero rows and emits columnar, so an
//     expression predicate costs no row materialization,
//   - expressions nest: inside and / or / not, on either side of a comparison,
//     and several in one predicate keep distinct bindings,
//   - synthetic-name recognition does not capture a real column whose name
//     merely resembles one.

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "clink/config/decimal.hpp"
#include "clink/config/json.hpp"
#include "clink/core/record.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/operators/columnar_row_filter_operator.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/operators/predicate_operand_exprs.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_columnar_batcher.hpp"

namespace {

using clink::Batch;
using clink::ColumnarRowFilterOperator;
using clink::Emitter;
using clink::StreamElement;
using clink::operators::PredicateOperandExprs;
using clink::sql::Row;
using clink::sql::RowColumn;

std::uint64_t poe_materialize_count() {
    return clink::detail::batch_materialize_counter().load(std::memory_order_relaxed);
}

Row poe_row(std::int64_t amount, std::string region) {
    Row r;
    r.values["amount"] = clink::config::JsonValue{amount};
    r.values["region"] = clink::config::JsonValue{std::move(region)};
    return r;
}

const std::vector<RowColumn>& poe_schema() {
    static const std::vector<RowColumn> s = {{"amount", arrow::int64()}, {"region", arrow::utf8()}};
    return s;
}

Batch<Row> poe_columnar(const std::vector<Row>& rows) {
    auto batcher = clink::sql::make_row_columnar_arrow_batcher(poe_schema());
    Batch<Row> in;
    for (const auto& r : rows) {
        in.emplace(r);
    }
    auto rb = batcher.build(in);
    return Batch<Row>{std::move(rb), rows.size(), clink::sql::row_materialize_fn()};
}

Batch<Row> poe_rows(const std::vector<Row>& rows) {
    Batch<Row> b;
    for (const auto& r : rows) {
        b.emplace(r);
    }
    return b;
}

std::shared_ptr<clink::config::JsonValue> poe_pred(const std::string& json) {
    return std::make_shared<clink::config::JsonValue>(clink::config::parse(json));
}

struct PoeCapture {
    std::vector<StreamElement<Row>> elems;
    Emitter<Row> emitter() {
        return Emitter<Row>([this](StreamElement<Row> e) {
            elems.push_back(std::move(e));
            return true;
        });
    }
    std::vector<std::int64_t> amounts() {
        std::vector<std::int64_t> out;
        for (auto& e : elems) {
            if (!e.is_data()) {
                continue;
            }
            for (const auto& r : e.as_data()) {
                auto it = r.value().values.find("amount");
                if (it != r.value().values.end() && it->second.is_number()) {
                    out.push_back(static_cast<std::int64_t>(it->second.as_number()));
                }
            }
        }
        return out;
    }
    bool any_columnar() {
        for (auto& e : elems) {
            if (e.is_data() && e.as_data().is_columnar()) {
                return true;
            }
        }
        return false;
    }
};

// amount: 10 20 33 40 55 60
const std::vector<Row>& poe_sample() {
    static const std::vector<Row> rows = {poe_row(10, "eu"),
                                          poe_row(20, "us"),
                                          poe_row(33, "eu"),
                                          poe_row(40, "us"),
                                          poe_row(55, "eu"),
                                          poe_row(60, "us")};
    return rows;
}

// Run one predicate down both paths and assert they agree with `expected`.
void expect_both_paths(const std::string& pred_json, const std::vector<std::int64_t>& expected) {
    {
        ColumnarRowFilterOperator op(poe_pred(pred_json));
        PoeCapture cap;
        auto em = cap.emitter();
        op.process(StreamElement<Row>::data(poe_rows(poe_sample())), em);
        EXPECT_EQ(cap.amounts(), expected) << "row path: " << pred_json;
    }
    {
        ColumnarRowFilterOperator op(poe_pred(pred_json));
        PoeCapture cap;
        auto em = cap.emitter();
        const auto before = poe_materialize_count();
        const bool handled =
            op.process_columnar(StreamElement<Row>::data(poe_columnar(poe_sample())), em);
        ASSERT_TRUE(handled) << "columnar path declined: " << pred_json;
        EXPECT_EQ(poe_materialize_count(), before)
            << "an expression predicate must not materialize rows: " << pred_json;
        EXPECT_EQ(cap.amounts(), expected) << "columnar path: " << pred_json;
        if (!expected.empty()) {
            EXPECT_TRUE(cap.any_columnar()) << "output must stay columnar: " << pred_json;
        }
    }
}

// MOD(amount, 20) = 0  ->  20, 40, 60
constexpr const char* kModPred =
    R"({"op":"eq","col_expr":{"op":"mod","args":[{"col":"amount"},{"lit":20}]},"literal":0})";

}  // namespace

TEST(PredicateOperandExprs, RewritesAnExpressionOperandToASyntheticReference) {
    auto built = PredicateOperandExprs::build(clink::config::parse(kModPred));
    ASSERT_FALSE(built.empty());
    const auto& obj = built.predicate().as_object();
    EXPECT_FALSE(built.predicate().contains("col_expr")) << "col_expr must be consumed";
    ASSERT_TRUE(built.predicate().contains("col"));
    EXPECT_EQ(obj.at("col").as_string(), "$expr0");
    EXPECT_EQ(obj.at("op").as_string(), "eq");
    EXPECT_EQ(obj.at("literal").as_number(), 0);
}

TEST(PredicateOperandExprs, LeavesAPlainPredicateUntouched) {
    const auto plain = clink::config::parse(R"({"op":"gt","col":"amount","literal":30})");
    auto built = PredicateOperandExprs::build(plain);
    EXPECT_TRUE(built.empty());
    EXPECT_EQ(built.predicate().as_object().at("col").as_string(), "amount");
}

TEST(PredicateOperandExprs, FiltersOnAnArithmeticExpression) {
    expect_both_paths(kModPred, {20, 40, 60});
}

// The expression on the RHS: amount > amount / 2 holds for every positive
// amount, and the shape exercises rhs_expr rather than col_expr.
TEST(PredicateOperandExprs, FiltersOnAnExpressionOnTheRightHandSide) {
    expect_both_paths(
        R"({"op":"gt","col":"amount","rhs_expr":{"op":"div","args":[{"col":"amount"},{"lit":2}]}})",
        {10, 20, 33, 40, 55, 60});
}

// Both operands expressions at once - two independent bindings in one predicate.
TEST(PredicateOperandExprs, BindsBothOperandsIndependently) {
    auto built = PredicateOperandExprs::build(clink::config::parse(
        R"({"op":"gt","col_expr":{"op":"mul","args":[{"col":"amount"},{"lit":2}]},
            "rhs_expr":{"op":"add","args":[{"col":"amount"},{"lit":30}]}})"));
    const auto& obj = built.predicate().as_object();
    EXPECT_EQ(obj.at("col").as_string(), "$expr0");
    EXPECT_EQ(obj.at("rhs_col").as_string(), "$expr1")
        << "the second operand must get its own binding, not reuse the first";
    // 2*amount > amount + 30  <=>  amount > 30
    expect_both_paths(
        R"({"op":"gt","col_expr":{"op":"mul","args":[{"col":"amount"},{"lit":2}]},
            "rhs_expr":{"op":"add","args":[{"col":"amount"},{"lit":30}]}})",
        {33, 40, 55, 60});
}

// Nested under and / or / not: the walk must reach operands at any depth, and
// each one gets its own binding.
TEST(PredicateOperandExprs, RewritesExpressionsNestedUnderLogicalOperators) {
    const std::string nested =
        R"({"op":"and","args":[
             {"op":"eq","col_expr":{"op":"mod","args":[{"col":"amount"},{"lit":20}]},"literal":0},
             {"op":"not","arg":{"op":"lt","col_expr":{"op":"add","args":[{"col":"amount"},{"lit":5}]},"literal":30}}]})";
    auto built = PredicateOperandExprs::build(clink::config::parse(nested));
    ASSERT_FALSE(built.empty());
    const auto& args = built.predicate().as_object().at("args").as_array();
    EXPECT_EQ(args[0].as_object().at("col").as_string(), "$expr0");
    EXPECT_EQ(args[1].as_object().at("arg").as_object().at("col").as_string(), "$expr1");
    // divisible by 20, and NOT (amount + 5 < 30)  ->  40, 60
    expect_both_paths(nested, {40, 60});
}

// A predicate mixing an expression operand with a plain column comparison: the
// plain side must keep its direct column reference, since that is what the typed
// columnar program accelerates.
TEST(PredicateOperandExprs, KeepsPlainColumnOperandsDirect) {
    const std::string mixed =
        R"({"op":"or","args":[
             {"op":"eq","col_expr":{"op":"mod","args":[{"col":"amount"},{"lit":20}]},"literal":0},
             {"op":"eq","col":"region","literal":"eu"}]})";
    auto built = PredicateOperandExprs::build(clink::config::parse(mixed));
    const auto& args = built.predicate().as_object().at("args").as_array();
    EXPECT_EQ(args[0].as_object().at("col").as_string(), "$expr0");
    EXPECT_EQ(args[1].as_object().at("col").as_string(), "region")
        << "a bare column operand must stay a direct reference";
    // divisible by 20, or region eu  ->  10, 20, 33, 40, 55, 60
    expect_both_paths(mixed, {10, 20, 33, 40, 55, 60});
}

// String expressions, not just arithmetic - the value evaluator's full grammar
// is available to a WHERE clause.
TEST(PredicateOperandExprs, FiltersOnAStringExpression) {
    expect_both_paths(R"({"op":"eq","col_expr":{"op":"upper","args":[{"col":"region"}]},
                          "literal":"EU"})",
                      {10, 33, 55});
}

// Synthetic-name recognition must not fire on anything that only looks like one,
// or a real column called "$expr"-something would be shadowed.
TEST(PredicateOperandExprs, RecognisesOnlyWellFormedSyntheticNames) {
    EXPECT_TRUE(PredicateOperandExprs::synthetic_index("$expr0").has_value());
    EXPECT_EQ(*PredicateOperandExprs::synthetic_index("$expr12"), 12u);
    EXPECT_FALSE(PredicateOperandExprs::synthetic_index("$expr").has_value()) << "no index";
    EXPECT_FALSE(PredicateOperandExprs::synthetic_index("$exprX").has_value()) << "non-numeric";
    EXPECT_FALSE(PredicateOperandExprs::synthetic_index("$expr1x").has_value()) << "trailing junk";
    EXPECT_FALSE(PredicateOperandExprs::synthetic_index("expr0").has_value()) << "no sigil";
    EXPECT_FALSE(PredicateOperandExprs::synthetic_index("amount").has_value());
    EXPECT_FALSE(PredicateOperandExprs::synthetic_index("").has_value());
}

// An out-of-range binding must be inert rather than reading past the vector -
// a hand-written predicate naming "$expr9" with no ninth expression is malformed
// input, and malformed predicate input is defined to be lazily null/false here,
// not a crash.
TEST(PredicateOperandExprs, OutOfRangeSyntheticNameResolvesNull) {
    ColumnarRowFilterOperator op(poe_pred(R"({"op":"eq","col":"$expr9","literal":0})"));
    PoeCapture cap;
    auto em = cap.emitter();
    op.process(StreamElement<Row>::data(poe_rows(poe_sample())), em);
    EXPECT_TRUE(cap.amounts().empty()) << "NULL = 0 is Unknown, which drops the row";
}

// A FLOAT literal inside a WHERE expression, which the binder lowers as an EXACT
// DECIMAL rather than a double.
//
// Nexmark q14 is `WHERE 0.908 * price > 250`, and the binder emits that 0.908 as a
// dec-string - the sentinel-tagged exact-decimal form - not as a JSON number. Both
// evaluators handle dec-strings on their own (arithmetic returns a dec-string,
// comparison compares exactly), so the composite has to be checked rather than
// assumed: the multiply's RESULT is itself a dec-string, and it is that result the
// comparison then sees through a synthetic operand binding.
//
// The cross-engine gate caught this shape returning ZERO rows where Flink returned
// 917,499 of 920,000. It was invisible until the gate's q14 threshold was lowered:
// the official `> 1000000` excludes every row of the generated stream, so the gate
// had been comparing 0 against 0 and passing.
TEST(PredicateOperandExprs, FiltersOnAnExactDecimalTimesAColumn) {
    const auto dec = clink::config::make_dec_value(*clink::config::dec_parse("0.908"));
    ASSERT_TRUE(clink::config::is_dec_string(dec))
        << "the binder lowers a float literal as a dec-string; this test is about that form";
    clink::config::JsonObject mul;
    clink::config::JsonArray args;
    clink::config::JsonObject lit;
    lit["lit"] = dec;  // the grammar's literal wrapper, exactly as the binder emits it
    args.emplace_back(clink::config::JsonValue{std::move(lit)});
    clink::config::JsonObject col;
    col["col"] = clink::config::JsonValue{std::string{"amount"}};
    args.emplace_back(clink::config::JsonValue{std::move(col)});
    mul["op"] = clink::config::JsonValue{std::string{"mul"}};
    mul["args"] = clink::config::JsonValue{std::move(args)};
    clink::config::JsonObject pred;
    pred["op"] = clink::config::JsonValue{std::string{"gt"}};
    pred["col_expr"] = clink::config::JsonValue{std::move(mul)};
    pred["literal"] = clink::config::JsonValue{static_cast<std::int64_t>(25)};

    // 0.908 * amount > 25  ->  amount > 27.5  ->  33, 40, 55, 60 survive.
    auto p = std::make_shared<clink::config::JsonValue>(clink::config::JsonValue{std::move(pred)});
    {
        ColumnarRowFilterOperator op(p);
        PoeCapture cap;
        auto em = cap.emitter();
        op.process(StreamElement<Row>::data(poe_rows(poe_sample())), em);
        EXPECT_EQ(cap.amounts(), (std::vector<std::int64_t>{33, 40, 55, 60})) << "row path";
    }
    {
        ColumnarRowFilterOperator op(p);
        PoeCapture cap;
        auto em = cap.emitter();
        const bool handled =
            op.process_columnar(StreamElement<Row>::data(poe_columnar(poe_sample())), em);
        ASSERT_TRUE(handled);
        EXPECT_EQ(cap.amounts(), (std::vector<std::int64_t>{33, 40, 55, 60})) << "columnar path";
    }
}

// The dec-string sentinel must survive the plan's JSON round-trip.
//
// A predicate reaches an operator as a STRING in the OperatorSpec params: the
// planner serialises it, the worker parses it back. An exact-decimal literal is
// tagged with a leading 0x01 control byte, so the round-trip has to preserve that
// byte exactly - if it is lost, is_dec_string() stops recognising the literal, the
// value becomes an ordinary string, arithmetic on it collapses to NULL, and every
// row is dropped by a comparison that returns Unknown.
//
// This is the shape nexmark q14 runs, and it emitted 0 rows against Flink's
// 917,499 while processing all 920,000 inputs without error.
TEST(PredicateOperandExprs, DecStringLiteralSurvivesThePlanJsonRoundTrip) {
    const auto dec = clink::config::make_dec_value(*clink::config::dec_parse("0.908"));
    ASSERT_TRUE(clink::config::is_dec_string(dec));

    clink::config::JsonObject lit;
    lit["lit"] = dec;
    clink::config::JsonArray args;
    args.emplace_back(clink::config::JsonValue{std::move(lit)});
    clink::config::JsonObject col;
    col["col"] = clink::config::JsonValue{std::string{"amount"}};
    args.emplace_back(clink::config::JsonValue{std::move(col)});
    clink::config::JsonObject mul;
    mul["op"] = clink::config::JsonValue{std::string{"mul"}};
    mul["args"] = clink::config::JsonValue{std::move(args)};
    clink::config::JsonObject pred;
    pred["op"] = clink::config::JsonValue{std::string{"gt"}};
    pred["col_expr"] = clink::config::JsonValue{std::move(mul)};
    pred["literal"] = clink::config::JsonValue{static_cast<std::int64_t>(25)};
    const clink::config::JsonValue original{std::move(pred)};

    // Exactly what the planner and the worker do.
    const std::string wire = original.serialize(0);
    const auto reparsed = clink::config::parse(wire);
    const auto& round_tripped_lit = reparsed.as_object()
                                        .at("col_expr")
                                        .as_object()
                                        .at("args")
                                        .as_array()[0]
                                        .as_object()
                                        .at("lit");
    EXPECT_TRUE(clink::config::is_dec_string(round_tripped_lit))
        << "the 0x01 decimal sentinel did not survive serialise+parse; the literal came back as "
        << round_tripped_lit.serialize(0);

    ColumnarRowFilterOperator op(std::make_shared<clink::config::JsonValue>(std::move(reparsed)));
    PoeCapture cap;
    auto em = cap.emitter();
    op.process(StreamElement<Row>::data(poe_rows(poe_sample())), em);
    EXPECT_EQ(cap.amounts(), (std::vector<std::int64_t>{33, 40, 55, 60}))
        << "0.908 * amount > 25 after a plan round-trip";
}

// The same predicate over the value range nexmark actually produces.
//
// The earlier decimal cases used amounts of 10 to 60; nexmark bid prices run to
// about 100,000, and an EXACT-decimal multiply's result grows in both digits and
// scale. If the product exceeds what the decimal type can represent, dec_mul
// returns nothing, the expression collapses to NULL, and the comparison returns
// Unknown - which drops the row. That failure is invisible at small magnitudes.
TEST(PredicateOperandExprs, ExactDecimalMultiplyHoldsAtNexmarkPriceMagnitudes) {
    const auto dec = clink::config::make_dec_value(*clink::config::dec_parse("0.908"));
    clink::config::JsonObject lit;
    lit["lit"] = dec;
    clink::config::JsonArray args;
    args.emplace_back(clink::config::JsonValue{std::move(lit)});
    clink::config::JsonObject col;
    col["col"] = clink::config::JsonValue{std::string{"amount"}};
    args.emplace_back(clink::config::JsonValue{std::move(col)});
    clink::config::JsonObject mul;
    mul["op"] = clink::config::JsonValue{std::string{"mul"}};
    mul["args"] = clink::config::JsonValue{std::move(args)};
    clink::config::JsonObject pred;
    pred["op"] = clink::config::JsonValue{std::string{"gt"}};
    pred["col_expr"] = clink::config::JsonValue{std::move(mul)};
    pred["literal"] = clink::config::JsonValue{static_cast<std::int64_t>(250)};
    auto p = std::make_shared<clink::config::JsonValue>(clink::config::JsonValue{std::move(pred)});

    // Real nexmark magnitudes. 0.908 * 8 = 7.264, below 250; the rest are above.
    const std::vector<Row> rows = {poe_row(8, "eu"),
                                   poe_row(276, "us"),
                                   poe_row(21497, "eu"),
                                   poe_row(49798, "us"),
                                   poe_row(99988, "eu")};
    {
        ColumnarRowFilterOperator op(p);
        PoeCapture cap;
        auto em = cap.emitter();
        op.process(StreamElement<Row>::data(poe_rows(rows)), em);
        EXPECT_EQ(cap.amounts(), (std::vector<std::int64_t>{276, 21497, 49798, 99988}))
            << "row path at nexmark magnitudes";
    }
    {
        ColumnarRowFilterOperator op(p);
        PoeCapture cap;
        auto em = cap.emitter();
        ASSERT_TRUE(op.process_columnar(StreamElement<Row>::data(poe_columnar(rows)), em));
        EXPECT_EQ(cap.amounts(), (std::vector<std::int64_t>{276, 21497, 49798, 99988}))
            << "columnar path at nexmark magnitudes";
    }
}
