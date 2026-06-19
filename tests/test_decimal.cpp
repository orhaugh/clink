// Unit tests for the exact DECIMAL substrate (#56). Cluster-free, so they run
// under the sanitizer matrix (the new value logic is exercised here, not only
// in cluster ITs).
#include <optional>
#include <string>

#include <gtest/gtest.h>

#include "clink/config/decimal.hpp"

using clink::config::as_decimal;
using clink::config::dec_add;
using clink::config::dec_canonical_key;
using clink::config::dec_compare;
using clink::config::dec_div;
using clink::config::dec_format;
using clink::config::dec_mul;
using clink::config::dec_negate;
using clink::config::dec_parse;
using clink::config::dec_rescale;
using clink::config::dec_sub;
using clink::config::Decimal;
using clink::config::is_dec_string;
using clink::config::JsonValue;
using clink::config::make_dec_value;

namespace {

// Parse helper: dec_parse must succeed.
Decimal D(const std::string& s) {
    auto d = dec_parse(s);
    EXPECT_TRUE(d.has_value()) << "dec_parse failed: " << s;
    return d.value_or(Decimal{});
}

std::string fmt(const std::optional<Decimal>& d) {
    return d ? dec_format(*d) : std::string{"<null>"};
}

}  // namespace

// --- parse / format round-trip ---------------------------------------------

TEST(Decimal, ParseFormatPreservesScale) {
    EXPECT_EQ(dec_format(D("1.10")), "1.10");  // trailing zero is significant
    EXPECT_EQ(dec_format(D("0")), "0");
    EXPECT_EQ(dec_format(D("-3")), "-3");
    EXPECT_EQ(dec_format(D("123.456")), "123.456");
    EXPECT_EQ(dec_format(D("0.00")), "0.00");
    EXPECT_EQ(D("1.10").scale, 2);
    EXPECT_EQ(D("5").scale, 0);
}

TEST(Decimal, NegativeZeroNormalises) {
    // -0.0 should render without the sign.
    auto z = dec_sub(D("1.5"), D("1.5"));
    EXPECT_EQ(fmt(z), "0.0");
}

TEST(Decimal, ParseRejectsNonNumeric) {
    EXPECT_FALSE(dec_parse("abc").has_value());
    EXPECT_FALSE(dec_parse("1.2.3").has_value());
}

// --- sentinel tagging -------------------------------------------------------

TEST(Decimal, SentinelTagging) {
    JsonValue dv = make_dec_value(D("1.10"));
    EXPECT_TRUE(is_dec_string(dv));
    EXPECT_EQ(dv.as_string().front(), '\x01');
    // A plain VARCHAR "1.10" is NOT a decimal (no sentinel) - the no-silent
    // -promotion rule.
    EXPECT_FALSE(is_dec_string(JsonValue{std::string{"1.10"}}));
    // Round-trip through the tag.
    auto back = dec_parse(dv.as_string());
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(dec_format(*back), "1.10");
}

TEST(Decimal, AsDecimalCoercion) {
    EXPECT_TRUE(as_decimal(make_dec_value(D("2.5"))).has_value());
    // Integral JSON number -> scale-0 decimal.
    auto five = as_decimal(JsonValue{static_cast<std::int64_t>(5)});
    ASSERT_TRUE(five.has_value());
    EXPECT_EQ(dec_format(*five), "5");
    // Non-integral double and non-numeric -> nullopt (caller falls back).
    EXPECT_FALSE(as_decimal(JsonValue{1.5}).has_value());
    EXPECT_FALSE(as_decimal(JsonValue{std::string{"1.10"}}).has_value());
    EXPECT_FALSE(as_decimal(JsonValue{nullptr}).has_value());
}

// --- add / sub / mul (exact) ------------------------------------------------

TEST(Decimal, AddIsExact) {
    EXPECT_EQ(fmt(dec_add(D("0.1"), D("0.2"))), "0.3");    // the classic double trap
    EXPECT_EQ(fmt(dec_add(D("1.5"), D("2.25"))), "3.75");  // scale = max(1,2)
    EXPECT_EQ(fmt(dec_add(D("1.10"), D("2"))), "3.10");    // scale preserved
}

TEST(Decimal, SubIsExact) {
    EXPECT_EQ(fmt(dec_sub(D("0.3"), D("0.1"))), "0.2");
    EXPECT_EQ(fmt(dec_sub(D("5"), D("2.25"))), "2.75");
}

TEST(Decimal, MulScaleAdds) {
    EXPECT_EQ(fmt(dec_mul(D("1.5"), D("1.5"))), "2.25");  // scale 1+1=2
    EXPECT_EQ(fmt(dec_mul(D("1.10"), D("2"))), "2.20");   // scale 2+0=2
    EXPECT_EQ(fmt(dec_mul(D("-1.5"), D("2"))), "-3.0");
}

// --- division + HALF_UP rounding --------------------------------------------

TEST(Decimal, DivExactAndRounded) {
    EXPECT_EQ(fmt(dec_div(D("10"), D("4"), 2)), "2.50");
    EXPECT_EQ(fmt(dec_div(D("1"), D("3"), 6)), "0.333333");
    EXPECT_EQ(fmt(dec_div(D("2"), D("3"), 6)), "0.666667");  // HALF_UP at last digit
    EXPECT_EQ(fmt(dec_div(D("1"), D("8"), 2)), "0.13");      // 0.125 -> 0.13 half-up
}

TEST(Decimal, DivNegativeHalfUpAwayFromZero) {
    EXPECT_EQ(fmt(dec_div(D("-1"), D("8"), 2)), "-0.13");  // -0.125 -> -0.13
    EXPECT_EQ(fmt(dec_div(D("2"), D("3"), 0)), "1");       // 0.666 -> 1
    EXPECT_EQ(fmt(dec_div(D("1"), D("3"), 0)), "0");       // 0.333 -> 0
}

TEST(Decimal, DivByZeroIsNull) {
    EXPECT_FALSE(dec_div(D("1"), D("0"), 2).has_value());
    EXPECT_FALSE(dec_div(D("1"), D("0.0"), 2).has_value());
}

// Regression: an extreme-scale divisor makes the internal scale shift exceed
// Decimal256's headroom; arrow's IncreaseScaleBy would SILENTLY WRAP and return
// a wrong non-null result (e.g. 1 / 1e-38 -> 0). Must fail soft to NULL (the
// true quotient ~1e38 also exceeds 38 digits).
TEST(Decimal, DivExtremeScaleDivisorYieldsNull) {
    Decimal tiny = D("0.00000000000000000000000000000000000001");  // 1e-38, scale 38
    EXPECT_FALSE(dec_div(D("1"), tiny, 38).has_value());
    EXPECT_FALSE(dec_div(D("2"), tiny, 38).has_value());
    EXPECT_FALSE(dec_div(D("5"), tiny, 38).has_value());
    // A safe division at the same large result scale still computes.
    EXPECT_TRUE(dec_div(D("1"), D("4"), 30).has_value());
}

// --- rescale (HALF_UP) ------------------------------------------------------

TEST(Decimal, RescaleHalfUp) {
    EXPECT_EQ(fmt(dec_rescale(D("1.255"), 2)), "1.26");
    EXPECT_EQ(fmt(dec_rescale(D("1.254"), 2)), "1.25");
    EXPECT_EQ(fmt(dec_rescale(D("-1.255"), 2)), "-1.26");  // away from zero
    EXPECT_EQ(fmt(dec_rescale(D("1.2"), 4)), "1.2000");    // scale up is exact
    EXPECT_EQ(fmt(dec_rescale(D("2.5"), 0)), "3");         // 2.5 -> 3
    EXPECT_EQ(fmt(dec_rescale(D("-2.5"), 0)), "-3");
}

// --- negate / compare -------------------------------------------------------

TEST(Decimal, Negate) {
    EXPECT_EQ(dec_format(dec_negate(D("3.14"))), "-3.14");
    EXPECT_EQ(dec_format(dec_negate(D("-3.14"))), "3.14");
}

TEST(Decimal, CompareByValueScaleInsensitive) {
    EXPECT_EQ(dec_compare(D("1.10"), D("1.1")), 0);  // value equality
    EXPECT_GT(dec_compare(D("1.2"), D("1.19")), 0);
    EXPECT_LT(dec_compare(D("-1"), D("1")), 0);
    EXPECT_EQ(dec_compare(D("0"), D("0.000")), 0);
}

// --- canonical key (GROUP BY / DISTINCT) ------------------------------------

TEST(Decimal, CanonicalKeyCollapsesTrailingZeros) {
    EXPECT_EQ(dec_canonical_key(D("1.10")), dec_canonical_key(D("1.1")));
    EXPECT_EQ(dec_canonical_key(D("1.100")), dec_canonical_key(D("1.1")));
    EXPECT_EQ(dec_canonical_key(D("2.0")), dec_canonical_key(D("2")));
    EXPECT_NE(dec_canonical_key(D("1.1")), dec_canonical_key(D("1.2")));
    EXPECT_NE(dec_canonical_key(D("1.11")), dec_canonical_key(D("1.1")));
}

// --- overflow ---------------------------------------------------------------

TEST(Decimal, MulOverflowYieldsNull) {
    // Two ~20-digit values -> ~40-digit product -> exceeds 38 digits -> null.
    Decimal big = D("99999999999999999999");  // 20 nines
    EXPECT_FALSE(dec_mul(big, big).has_value());
}

TEST(Decimal, AddOverflowYieldsNull) {
    Decimal near_max = D("99999999999999999999999999999999999999");  // 38 nines
    EXPECT_FALSE(dec_add(near_max, near_max).has_value());           // ~2e38 > 10^38
}

TEST(Decimal, LargeButFittingIsExact) {
    Decimal a = D("12345678901234567890.12345678");  // 28 digits, scale 8
    Decimal b = D("1.00000001");
    auto sum = dec_add(a, b);
    ASSERT_TRUE(sum.has_value());
    EXPECT_EQ(dec_format(*sum), "12345678901234567891.12345679");
}
