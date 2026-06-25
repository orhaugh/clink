// WS1 increment 1: ColumnarPredicateProgram parity oracle.
//
// The load-bearing claim: for every predicate the typed program COMPILES, its
// per-row keep mask is byte-identical to the row interpreter (the existing
// evaluate_json_predicate walked over read_cell) - the exact contract that lets
// the columnar filter swap in the typed program with zero behaviour change. And
// for shapes it does not model (mixed-type coercion, LIKE, column-vs-column,
// missing column) compile() returns nullopt so the operator falls back to that
// same row interpreter.

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "clink/config/decimal.hpp"
#include "clink/config/json.hpp"
#include "clink/operators/columnar_expr_program.hpp"
#include "clink/operators/json_predicate.hpp"
#include "clink/operators/json_value_expr.hpp"
#include "clink/sql/row_columnar_batcher.hpp"

namespace {

using clink::config::JsonArray;
using clink::config::JsonObject;
using clink::config::JsonValue;
using clink::operators::ColumnarPredicateProgram;
using clink::operators::ColumnarValueProgram;

// --- IR builders ---------------------------------------------------------
JsonValue cmp(const std::string& op, const std::string& col, JsonValue lit) {
    return JsonValue{
        JsonObject{{"op", JsonValue{op}}, {"col", JsonValue{col}}, {"literal", std::move(lit)}}};
}
JsonValue is_null_p(const std::string& col) {
    return JsonValue{
        JsonObject{{"op", JsonValue{std::string{"is_null"}}}, {"col", JsonValue{col}}}};
}
JsonValue is_not_null_p(const std::string& col) {
    return JsonValue{
        JsonObject{{"op", JsonValue{std::string{"is_not_null"}}}, {"col", JsonValue{col}}}};
}
JsonValue land(JsonArray args) {
    return JsonValue{
        JsonObject{{"op", JsonValue{std::string{"and"}}}, {"args", JsonValue{std::move(args)}}}};
}
JsonValue lor(JsonArray args) {
    return JsonValue{
        JsonObject{{"op", JsonValue{std::string{"or"}}}, {"args", JsonValue{std::move(args)}}}};
}
JsonValue lnot(JsonValue arg) {
    return JsonValue{JsonObject{{"op", JsonValue{std::string{"not"}}}, {"arg", std::move(arg)}}};
}

// --- a batch with nulls across every supported physical type -------------
std::shared_ptr<arrow::RecordBatch> make_batch() {
    auto dec_t = arrow::decimal128(10, 2);
    auto schema = arrow::schema({
        arrow::field("price", arrow::int64()),
        arrow::field("ratio", arrow::float64()),
        arrow::field("name", arrow::utf8()),
        arrow::field("flag", arrow::boolean()),
        arrow::field("amt", dec_t),
    });

    arrow::Int64Builder price;
    arrow::DoubleBuilder ratio;
    arrow::StringBuilder name;
    arrow::BooleanBuilder flag;
    arrow::Decimal128Builder amt(dec_t);

    auto dec = [](std::int64_t unscaled) { return arrow::Decimal128(unscaled); };
    // row:  price  ratio  name    flag   amt(scale2)
    (void)price.Append(50);
    (void)ratio.Append(0.5);
    (void)name.Append("bob");
    (void)flag.Append(true);
    (void)amt.Append(dec(999));
    (void)price.Append(250);
    (void)ratio.Append(1.5);
    (void)name.Append("amy");
    (void)flag.Append(false);
    (void)amt.Append(dec(500));
    (void)price.AppendNull();
    (void)ratio.Append(2.0);
    (void)name.AppendNull();
    (void)flag.Append(true);
    (void)amt.AppendNull();
    (void)price.Append(100);
    (void)ratio.Append(1.0);
    (void)name.Append("zoe");
    (void)flag.Append(true);
    (void)amt.Append(dec(1250));
    (void)price.Append(300);
    (void)ratio.Append(0.9);
    (void)name.Append("m");
    (void)flag.Append(false);
    (void)amt.Append(dec(999));
    (void)price.Append(150);
    (void)ratio.Append(1.5);
    (void)name.Append("bob");
    (void)flag.Append(true);
    (void)amt.Append(dec(2000));

    std::shared_ptr<arrow::Array> a_price, a_ratio, a_name, a_flag, a_amt;
    (void)price.Finish(&a_price);
    (void)ratio.Finish(&a_ratio);
    (void)name.Finish(&a_name);
    (void)flag.Finish(&a_flag);
    (void)amt.Finish(&a_amt);
    return arrow::RecordBatch::Make(schema, 6, {a_price, a_ratio, a_name, a_flag, a_amt});
}

// The row interpreter's keep decision for row i (the parity oracle).
bool row_keep(const JsonValue& pred, const arrow::RecordBatch& rb, std::int64_t i) {
    auto resolve = [&](const std::string& nm) -> JsonValue {
        const int idx = rb.schema()->GetFieldIndex(nm);
        if (idx < 0) {
            return JsonValue{nullptr};
        }
        return clink::sql::row_columnar_detail::read_cell(
            rb.schema()->field(idx)->type(), *rb.column(idx), i);
    };
    return clink::operators::evaluate_json_predicate(pred, resolve);
}

// Compile + evaluate, asserting the program mask matches the row oracle row by
// row. Returns kept count for spot checks.
std::int64_t expect_parity(const JsonValue& pred, const arrow::RecordBatch& rb) {
    auto prog = ColumnarPredicateProgram::compile(pred, *rb.schema());
    EXPECT_TRUE(prog.has_value()) << "predicate should compile: " << pred.serialize(0);
    if (!prog) {
        return -1;
    }
    auto res = prog->evaluate(rb);
    EXPECT_TRUE(res.has_value());
    if (!res) {
        return -1;
    }
    std::int64_t kept = 0;
    for (std::int64_t i = 0; i < rb.num_rows(); ++i) {
        const bool prog_keep = res->mask->Value(i);
        EXPECT_EQ(prog_keep, row_keep(pred, rb, i))
            << "row " << i << " diverges for " << pred.serialize(0);
        kept += prog_keep ? 1 : 0;
    }
    EXPECT_EQ(kept, res->kept);
    return kept;
}

TEST(ColumnarPredicateProgram, ParityAcrossTypesAndLogic) {
    auto rb = make_batch();

    // Numeric (int64 + double), string, bool.
    EXPECT_EQ(expect_parity(cmp("gt", "price", JsonValue{std::int64_t{100}}), *rb),
              3);                                                          // 250,300,150
    EXPECT_EQ(expect_parity(cmp("le", "ratio", JsonValue{1.5}), *rb), 5);  // all but 2.0
    EXPECT_EQ(expect_parity(cmp("eq", "name", JsonValue{"bob"}), *rb), 2);
    EXPECT_EQ(expect_parity(cmp("lt", "name", JsonValue{"m"}), *rb),
              3);  // bob,amy,bob < m (null drops)
    EXPECT_EQ(expect_parity(cmp("eq", "flag", JsonValue{true}), *rb), 4);

    // Exact decimal: amt >= 9.99 (unscaled 999, scale 2).
    auto lit_999 = clink::config::make_dec_value(clink::config::Decimal{arrow::Decimal128(999), 2});
    EXPECT_EQ(expect_parity(cmp("ge", "amt", lit_999), *rb),
              4);  // 9.99,12.50,9.99,20.00 (null drops)

    // Logical composition + IS [NOT] NULL.
    expect_parity(land(JsonArray{cmp("gt", "price", JsonValue{std::int64_t{100}}),
                                 cmp("eq", "flag", JsonValue{true})}),
                  *rb);
    expect_parity(
        lor(JsonArray{cmp("eq", "name", JsonValue{"amy"}), cmp("eq", "flag", JsonValue{true})}),
        *rb);
    expect_parity(lnot(cmp("gt", "price", JsonValue{std::int64_t{100}})), *rb);
    EXPECT_EQ(expect_parity(is_null_p("price"), *rb), 1);
    EXPECT_EQ(expect_parity(is_not_null_p("name"), *rb), 5);

    // Nested three-valued logic touching the null row (i=2).
    expect_parity(land(JsonArray{lor(JsonArray{cmp("gt", "price", JsonValue{std::int64_t{100}}),
                                               is_null_p("price")}),
                                 lnot(cmp("eq", "flag", JsonValue{false}))}),
                  *rb);
}

TEST(ColumnarPredicateProgram, UnsupportedShapesFallBack) {
    auto rb = make_batch();
    auto nullopt_compile = [&](const JsonValue& pred, const char* why) {
        EXPECT_FALSE(ColumnarPredicateProgram::compile(pred, *rb->schema()).has_value()) << why;
    };

    // LIKE is not a typed kernel yet.
    nullopt_compile(JsonValue{JsonObject{{"op", JsonValue{std::string{"like"}}},
                                         {"col", JsonValue{std::string{"name"}}},
                                         {"pattern", JsonValue{std::string{"%b%"}}}}},
                    "LIKE");
    // IN is not a typed kernel yet.
    nullopt_compile(
        JsonValue{JsonObject{
            {"op", JsonValue{std::string{"in"}}},
            {"col", JsonValue{std::string{"price"}}},
            {"values",
             JsonValue{JsonArray{JsonValue{std::int64_t{50}}, JsonValue{std::int64_t{100}}}}}}},
        "IN");
    // Column-vs-column.
    nullopt_compile(JsonValue{JsonObject{{"op", JsonValue{std::string{"eq"}}},
                                         {"col", JsonValue{std::string{"price"}}},
                                         {"rhs_col", JsonValue{std::string{"ratio"}}}}},
                    "rhs_col");
    // Mixed type: numeric column vs string literal (per-row coercion).
    nullopt_compile(cmp("gt", "price", JsonValue{"100"}), "numeric col vs string literal");
    // Mixed type: string column vs numeric literal.
    nullopt_compile(cmp("gt", "name", JsonValue{std::int64_t{5}}), "string col vs numeric literal");
    // Decimal column vs a plain (non-dec-string) numeric literal.
    nullopt_compile(cmp("ge", "amt", JsonValue{9.99}), "decimal col vs plain number literal");
    // Missing column.
    nullopt_compile(cmp("eq", "missing", JsonValue{std::int64_t{1}}), "missing column");
    // NULL literal.
    nullopt_compile(cmp("eq", "price", JsonValue{nullptr}), "null literal");
}

// --- value-expression IR builders (op + args:[operands], operands are
//     {col}/{lit}/nested {op}) ---
JsonValue vcol(const std::string& c) {
    return JsonValue{JsonObject{{"col", JsonValue{c}}}};
}
JsonValue vlit(JsonValue x) {
    return JsonValue{JsonObject{{"lit", std::move(x)}}};
}
JsonValue vop(const std::string& op, JsonArray args) {
    return JsonValue{JsonObject{{"op", JsonValue{op}}, {"args", JsonValue{std::move(args)}}}};
}

// Parity oracle for the value program: read_cell of the float64 output column
// must equal evaluate_json_value_expr over read_cell of the input row, compared
// via canonical serialize(0) (covers null, NaN, and int-as-double identically).
void expect_value_parity(const JsonValue& expr, const arrow::RecordBatch& rb) {
    auto vp = ColumnarValueProgram::compile(expr, *rb.schema());
    ASSERT_TRUE(vp.has_value()) << "value expr should compile: " << expr.serialize(0);
    auto col = vp->evaluate(rb);
    ASSERT_NE(col, nullptr);
    for (std::int64_t i = 0; i < rb.num_rows(); ++i) {
        auto resolve = [&](const std::string& nm) -> JsonValue {
            const int idx = rb.schema()->GetFieldIndex(nm);
            if (idx < 0) {
                return JsonValue{nullptr};
            }
            return clink::sql::row_columnar_detail::read_cell(
                rb.schema()->field(idx)->type(), *rb.column(idx), i);
        };
        const JsonValue prog_cell =
            clink::sql::row_columnar_detail::read_cell(arrow::float64(), *col, i);
        const JsonValue row_cell = clink::operators::evaluate_json_value_expr(expr, resolve);
        EXPECT_EQ(prog_cell.serialize(0), row_cell.serialize(0))
            << "row " << i << " diverges for " << expr.serialize(0);
    }
}

TEST(ColumnarValueProgram, ArithmeticParity) {
    auto rb = make_batch();  // price int64 (null at row 2), ratio float64

    expect_value_parity(vop("add", JsonArray{vcol("price"), vcol("price")}), *rb);
    expect_value_parity(vop("mul", JsonArray{vcol("ratio"), vlit(JsonValue{2.0})}), *rb);
    expect_value_parity(vop("sub", JsonArray{vcol("price"), vlit(JsonValue{std::int64_t{10}})}),
                        *rb);
    expect_value_parity(vop("neg", JsonArray{vcol("ratio")}), *rb);
    // div / mod by zero -> NaN on both paths; null operand (row 2) -> null.
    expect_value_parity(vop("div", JsonArray{vcol("price"), vlit(JsonValue{std::int64_t{0}})}),
                        *rb);
    expect_value_parity(vop("mod", JsonArray{vcol("price"), vlit(JsonValue{std::int64_t{7}})}),
                        *rb);
    // Nested: (price * 2) + ratio.
    expect_value_parity(
        vop("add",
            JsonArray{vop("mul", JsonArray{vcol("price"), vlit(JsonValue{2.0})}), vcol("ratio")}),
        *rb);
}

TEST(ColumnarValueProgram, NonNumericOperandsFallBack) {
    auto rb = make_batch();
    // decimal column operand -> row fallback (exact decimal arithmetic).
    EXPECT_FALSE(ColumnarValueProgram::compile(
                     vop("add", JsonArray{vcol("amt"), vlit(JsonValue{1.0})}), *rb->schema())
                     .has_value());
    // string column operand -> row fallback.
    EXPECT_FALSE(ColumnarValueProgram::compile(vop("add", JsonArray{vcol("name"), vcol("name")}),
                                               *rb->schema())
                     .has_value());
    // dec-string literal operand -> row fallback.
    auto lit_dec = clink::config::make_dec_value(clink::config::Decimal{arrow::Decimal128(150), 2});
    EXPECT_FALSE(ColumnarValueProgram::compile(vop("add", JsonArray{vcol("price"), vlit(lit_dec)}),
                                               *rb->schema())
                     .has_value());
    // unsupported op (concat) -> row fallback.
    EXPECT_FALSE(ColumnarValueProgram::compile(vop("concat", JsonArray{vcol("name"), vcol("name")}),
                                               *rb->schema())
                     .has_value());
}

// A utf8 column can carry a dec-string (0x01-sentinel) cell, which the row
// interpreter reads as a DECIMAL (detail::compare's coercion branch), not a
// lexicographic string. The typed Str kernel cannot reproduce that, so it must
// defer the whole batch to the row interpreter (evaluate -> nullopt). Found by
// the WS1 inc1 adversarial parity review.
TEST(ColumnarPredicateProgram, SentinelStringCellDefersToRowPath) {
    auto schema = arrow::schema({arrow::field("s", arrow::utf8())});
    arrow::StringBuilder sb;
    std::string sentinel;
    sentinel.push_back(clink::config::kDecimalSentinel);
    sentinel += "9.99";  // a dec-string cell in a utf8 column
    (void)sb.Append("foo");
    (void)sb.Append(sentinel);
    std::shared_ptr<arrow::Array> sa;
    (void)sb.Finish(&sa);
    auto rb = arrow::RecordBatch::Make(schema, 2, {sa});

    // Compiles (string column vs a plain string literal)...
    auto prog = ColumnarPredicateProgram::compile(cmp("ne", "s", JsonValue{"foo"}), *schema);
    ASSERT_TRUE(prog.has_value());
    // ...but bails at eval when it meets the sentinel cell, so the operator
    // falls back to the (correct) row interpreter for this batch.
    EXPECT_FALSE(prog->evaluate(*rb).has_value())
        << "a sentinel-bearing string column must defer to the row interpreter";

    // A clean string column with no sentinel still evaluates on the fast path.
    arrow::StringBuilder clean;
    (void)clean.Append("foo");
    (void)clean.Append("bar");
    std::shared_ptr<arrow::Array> ca;
    (void)clean.Finish(&ca);
    auto rb_clean = arrow::RecordBatch::Make(schema, 2, {ca});
    auto res = prog->evaluate(*rb_clean);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->kept, 1);  // "bar" != "foo" keeps; "foo" drops
}

}  // namespace
