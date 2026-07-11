#pragma once

#include <algorithm>
#include <optional>
#include <string>
#include <string_view>

#include <arrow/util/decimal.h>

#include "clink/config/json.hpp"

// Exact fixed-point DECIMAL arithmetic for the SQL runtime (#56).
//
// The runtime value model (clink::config::JsonValue) has no decimal kind -
// numbers are IEEE double. To carry exact decimals without growing the variant,
// a DECIMAL value lives in the existing String alternative as a CANONICAL,
// SENTINEL-TAGGED dec-string:
//
//     0x01 <canonical-decimal-text>
//
// The leading 0x01 (a US control byte, illegal in well-formed text and JSON
// -escaped distinctly) is the type tag JsonValue otherwise lacks, so a real
// VARCHAR "1.10" is never confused with a DECIMAL. Canonical text preserves
// scale (trailing fractional zeros are significant): 1.10 stays "1.10".
//
// Math is done in 128-bit fixed point via arrow::Decimal128 (the full 38-digit
// coefficient fits, since 10^38 < 2^127), widening to Decimal256 for the
// add/sub/mul/div intermediates because 128-bit ops wrap silently on overflow.
// Rounding is HALF_UP (away from zero) everywhere a scale decreases, supplied by
// arrow's ReduceScaleBy(n, round=true). Overflow past 38 digits yields nullopt
// (the callers map that to SQL NULL, matching clink's soft-fail convention).
//
// Non-decimal numbers never touch any of this: every decimal branch in the
// evaluators is gated by is_dec_string(), so plain doubles stay on the
// untouched fast path.

namespace clink::config {

// Type tag byte for a decimal-bearing String value.
inline constexpr char kDecimalSentinel = '\x01';
// SQL DECIMAL coefficient ceiling.
inline constexpr int kMaxDecimalPrecision = 38;

// A fixed-point decimal: value = unscaled * 10^-scale, scale >= 0.
struct Decimal {
    arrow::Decimal128 unscaled{};
    int scale = 0;
};

namespace dec_detail {

inline arrow::Decimal256 widen(const arrow::Decimal128& d) {
    return arrow::Decimal256(arrow::BasicDecimal256(d));
}

// Narrow a 256-bit coefficient (carrying a value at `scale`) back to a
// Decimal. nullopt if it exceeds 38 significant digits (overflow).
inline std::optional<Decimal> narrow(const arrow::Decimal256& v, int scale) {
    if (!v.FitsInPrecision(kMaxDecimalPrecision)) {
        return std::nullopt;
    }
    auto res = arrow::Decimal128::FromString(v.ToString(0));  // scale-0 integer round-trip
    if (!res.ok()) {
        return std::nullopt;
    }
    return Decimal{*res, scale};
}

}  // namespace dec_detail

// --- sentinel-string helpers -------------------------------------------------

// Is this JsonValue a decimal-tagged String?
inline bool is_dec_string(const JsonValue& v) {
    return v.is_string() && !v.as_string().empty() && v.as_string().front() == kDecimalSentinel;
}

// Canonical decimal text (NO sentinel), e.g. "1.10", "-3", "0". Trailing
// fractional zeros are preserved (scale is significant); "-0" normalises to "0".
inline std::string dec_format(const Decimal& d) {
    std::string s = d.unscaled.ToString(d.scale < 0 ? 0 : d.scale);
    bool magnitude_zero = true;
    for (char c : s) {
        if (c != '-' && c != '.' && c != '0') {
            magnitude_zero = false;
            break;
        }
    }
    if (magnitude_zero && !s.empty() && s.front() == '-') {
        s.erase(s.begin());
    }
    return s;
}

// Wrap a Decimal as a sentinel-tagged dec-string JsonValue.
inline JsonValue make_dec_value(const Decimal& d) {
    std::string out;
    out.reserve(2 + dec_format(d).size());
    out.push_back(kDecimalSentinel);
    out += dec_format(d);
    return JsonValue{std::move(out)};
}

// Parse canonical decimal text (sentinel stripped if present) -> Decimal.
// nullopt if the text is not a valid decimal numeral.
inline std::optional<Decimal> dec_parse(std::string_view text) {
    if (!text.empty() && text.front() == kDecimalSentinel) {
        text.remove_prefix(1);
    }
    arrow::Decimal128 out;
    int32_t precision = 0;
    int32_t scale = 0;
    auto st = arrow::Decimal128::FromString(std::string(text), &out, &precision, &scale);
    if (!st.ok()) {
        return std::nullopt;
    }
    if (scale < 0) {
        // e.g. "1e3" parses to (1, scale=-3); fold into a scale-0 integer.
        out = out.IncreaseScaleBy(-scale);
        scale = 0;
    }
    return Decimal{out, scale};
}

// Coerce a value to a Decimal for mixed-operand arithmetic/comparison: a
// dec-string yields its decimal; an INTEGRAL JSON number yields a scale-0
// decimal; anything else (non-integral double, string, null) yields nullopt so
// the caller falls back to the double path or to SQL UNKNOWN.
inline std::optional<Decimal> as_decimal(const JsonValue& v) {
    if (is_dec_string(v)) {
        return dec_parse(v.as_string());
    }
    if (v.is_number()) {
        double n = v.as_number();
        auto as_i64 = static_cast<std::int64_t>(n);
        if (n == static_cast<double>(as_i64)) {
            return Decimal{arrow::Decimal128(as_i64), 0};
        }
    }
    return std::nullopt;
}

// --- arithmetic (all exact; nullopt on overflow / div-by-zero) ---------------

inline std::optional<Decimal> dec_add(const Decimal& a, const Decimal& b) {
    int s = std::max(a.scale, b.scale);
    arrow::Decimal256 x = dec_detail::widen(a.unscaled).IncreaseScaleBy(s - a.scale);
    arrow::Decimal256 y = dec_detail::widen(b.unscaled).IncreaseScaleBy(s - b.scale);
    x += y;
    return dec_detail::narrow(x, s);
}

inline std::optional<Decimal> dec_sub(const Decimal& a, const Decimal& b) {
    int s = std::max(a.scale, b.scale);
    arrow::Decimal256 x = dec_detail::widen(a.unscaled).IncreaseScaleBy(s - a.scale);
    arrow::Decimal256 y = dec_detail::widen(b.unscaled).IncreaseScaleBy(s - b.scale);
    x -= y;
    return dec_detail::narrow(x, s);
}

inline std::optional<Decimal> dec_mul(const Decimal& a, const Decimal& b) {
    arrow::Decimal256 x = dec_detail::widen(a.unscaled);
    x *= dec_detail::widen(b.unscaled);
    return dec_detail::narrow(x, a.scale + b.scale);  // product scale = sa + sb
}

// a / b at the given result scale, HALF_UP. nullopt on div-by-zero or overflow.
inline std::optional<Decimal> dec_div(const Decimal& a, const Decimal& b, int result_scale) {
    if (b.unscaled == arrow::Decimal128(0)) {
        return std::nullopt;
    }
    if (result_scale < 0) {
        result_scale = 0;
    }
    // Compute the quotient at (result_scale + 1) by truncating integer division,
    // then drop the single guard digit HALF_UP. One guard digit is sufficient:
    // HALF_UP at result_scale depends only on the digit at position
    // result_scale+1 (>=5 rounds up), and truncation preserves that digit.
    int guard = result_scale + 1;
    int shift = guard + b.scale - a.scale;
    arrow::Decimal256 num = dec_detail::widen(a.unscaled);
    if (shift >= 0) {
        // Guard against a SILENT Decimal256 wrap: arrow's IncreaseScaleBy does
        // not bounds-check, so a.unscaled * 10^shift must fit in ~76 digits. If
        // it cannot, the true quotient exceeds 38 digits anyway -> NULL (fail
        // soft). 0 fits any precision, so 0 / x stays correct.
        const int kHeadroom = kMaxDecimalPrecision * 2;  // Decimal256 ~76 digits
        if (shift > kHeadroom || !a.unscaled.FitsInPrecision(kHeadroom - shift)) {
            return std::nullopt;
        }
        num = num.IncreaseScaleBy(shift);
    } else {
        num = num.ReduceScaleBy(-shift, /*round=*/false);
    }
    arrow::Decimal256 den = dec_detail::widen(b.unscaled);
    auto dres = num.Divide(den);
    if (!dres.ok()) {
        return std::nullopt;
    }
    arrow::Decimal256 rounded =
        dres->first.ReduceScaleBy(1, /*round=*/true);  // drop guard digit, HALF_UP
    return dec_detail::narrow(rounded, result_scale);
}

// a mod b at the common (max) scale: exact truncated remainder. nullopt on
// div-by-zero. SQL MOD: result has the sign of the dividend.
inline std::optional<Decimal> dec_mod(const Decimal& a, const Decimal& b) {
    if (b.unscaled == arrow::Decimal128(0)) {
        return std::nullopt;
    }
    int s = std::max(a.scale, b.scale);
    arrow::Decimal256 x = dec_detail::widen(a.unscaled).IncreaseScaleBy(s - a.scale);
    arrow::Decimal256 y = dec_detail::widen(b.unscaled).IncreaseScaleBy(s - b.scale);
    auto dres = x.Divide(y);  // truncated quotient + remainder (remainder takes dividend sign)
    if (!dres.ok()) {
        return std::nullopt;
    }
    return dec_detail::narrow(dres->second, s);
}

inline Decimal dec_negate(const Decimal& a) {
    return Decimal{arrow::Decimal128(0) - a.unscaled, a.scale};
}

inline Decimal dec_abs(const Decimal& a) {
    return Decimal{arrow::Decimal128::Abs(a.unscaled), a.scale};
}

// Lossy conversion to double (for mixed decimal/double arithmetic and the
// float-returning scalar functions, which the binder already types float64).
inline double dec_to_double(const Decimal& a) {
    return a.unscaled.ToDouble(a.scale);
}

// Exact conversion to int64 when the value is a whole number (scale 0) that
// fits int64; nullopt otherwise (a fractional value, or out of int64 range).
// Lets an all-integer SUM emit an exact integer instead of rounding through a
// double.
inline std::optional<std::int64_t> dec_to_int64(const Decimal& a) {
    if (a.scale != 0) {
        return std::nullopt;
    }
    std::int64_t out = 0;
    if (a.unscaled.ToInteger(&out).ok()) {
        return out;
    }
    return std::nullopt;
}

// Rescale to a target scale, HALF_UP when scaling down, exact when scaling up.
// nullopt on overflow.
inline std::optional<Decimal> dec_rescale(const Decimal& a, int target_scale) {
    if (target_scale < 0) {
        target_scale = 0;
    }
    if (target_scale == a.scale) {
        return a;
    }
    arrow::Decimal256 x = dec_detail::widen(a.unscaled);
    if (target_scale > a.scale) {
        x = x.IncreaseScaleBy(target_scale - a.scale);
    } else {
        x = x.ReduceScaleBy(a.scale - target_scale, /*round=*/true);
    }
    return dec_detail::narrow(x, target_scale);
}

// 3-valued compare: <0, 0, >0 by VALUE (scale-insensitive: 1.10 == 1.1).
inline int dec_compare(const Decimal& a, const Decimal& b) {
    int s = std::max(a.scale, b.scale);
    arrow::Decimal256 x = dec_detail::widen(a.unscaled).IncreaseScaleBy(s - a.scale);
    arrow::Decimal256 y = dec_detail::widen(b.unscaled).IncreaseScaleBy(s - b.scale);
    if (x < y) {
        return -1;
    }
    if (x > y) {
        return 1;
    }
    return 0;
}

// Serialize a JsonValue for EXTERNAL OUTPUT (file/NDJSON sinks), rendering a
// dec-string as an unquoted JSON number token (sentinel stripped) rather than a
// quoted string. The operator-to-operator wire keeps the plain serialize (the
// dec-string rides as an escaped String and round-trips exactly), so only the
// terminal sink needs this. Non-decimal scalars defer to JsonValue::serialize.
inline std::string serialize_output(const JsonValue& v) {
    if (is_dec_string(v)) {
        return v.as_string().substr(1);  // canonical decimal, unquoted, no sentinel
    }
    if (v.is_array()) {
        std::string out = "[";
        bool first = true;
        for (const auto& e : v.as_array()) {
            if (!first) {
                out += ',';
            }
            first = false;
            out += serialize_output(e);
        }
        out += ']';
        return out;
    }
    if (v.is_object()) {
        std::string out = "{";
        bool first = true;
        for (const auto& [k, val] : v.as_object()) {
            if (!first) {
                out += ',';
            }
            first = false;
            out += JsonValue{k}.serialize(0);  // quoted + escaped key
            out += ':';
            out += serialize_output(val);
        }
        out += '}';
        return out;
    }
    return v.serialize(0);
}

// Scale-invariant canonical key for GROUP BY / DISTINCT / min-max bucketing, so
// 1.10 and 1.1 collapse to one group. Strips trailing fractional zeros to a
// minimal (coefficient, scale) form rendered as text.
inline std::string dec_canonical_key(const Decimal& a) {
    Decimal d = a;
    const arrow::Decimal128 ten(10);
    while (d.scale > 0) {
        auto dres = d.unscaled.Divide(ten);
        if (!dres.ok() || dres->second != arrow::Decimal128(0)) {
            break;
        }
        d.unscaled = dres->first;
        --d.scale;
    }
    return dec_format(d);
}

}  // namespace clink::config
