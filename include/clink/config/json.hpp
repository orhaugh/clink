#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "clink/config/flat_map.hpp"

namespace clink::config {

// Minimal JSON value type. Sufficient for pipeline config files;
// deliberately not a full-featured library:
//
//   * Numbers are stored as either an exact int64 (integer tokens) or a
//     double (fractional / out-of-int64-range tokens). is_number() is true
//     for both; as_number() widens either to double for callers that do not
//     care; as_int()/is_integral_number() read the exact integer. This keeps
//     BIGINT values exact past 2^53 instead of rounding through double.
//   * Object key order is sorted lexicographically (FlatMap, a sorted
//     contiguous container - see flat_map.hpp for the semantics it
//     guarantees and the iterator-invalidation rule it does not). Tools
//     that care about authoring order will produce different output.
//   * No comments, no trailing commas (strict JSON), no streaming parser.
//
// Throws ParseError on malformed input.
class JsonValue;

using JsonObject = FlatMap<JsonValue>;
using JsonArray = std::vector<JsonValue>;

// How a writer renders a double that JSON has no number token for: NaN, +Inf
// and -Inf. RFC 8259 has none, and parse() below is strict, so printing them
// bare ("nan", "inf") gives text that no reader accepts, this one included.
// Every writer here takes one of three deliberate forms, chosen by
// append_non_finite() so the writers cannot drift apart.
enum class NonFinite : std::uint8_t {
    // Text clink reads back itself: key text, operator state, the row codec. A
    // string tagged with kNonFiniteTag ("\u0002NaN", "\u0002Infinity",
    // "\u0002-Infinity") that parse() turns back into the same double. Valid
    // JSON, so a foreign reader still parses it, as a string. NaN reads back as
    // a quiet NaN: its sign and payload are not kept.
    Tagged,
    // Output for a reader other than clink (sink output, HTTP API responses):
    // null. The default for every external writer.
    Null,
    // Output for a reader other than clink that must tell NaN from a missing
    // value: the strings "NaN", "Infinity" and "-Infinity". A sink opts in
    // with the json_non_finite option.
    String,
};

// The leading byte of a Tagged non-finite string. A control byte, so no text a
// person writes carries it by accident, and distinct from the decimal tag
// (kDecimalSentinel, 0x01, in decimal.hpp).
inline constexpr char kNonFiniteTag = '\x02';

// "NaN", "Infinity" or "-Infinity" for a non-finite double; the spelling the
// String form writes, quotes aside. Precondition: !std::isfinite(d).
[[nodiscard]] std::string_view non_finite_name(double d) noexcept;

// Append the JSON text for a non-finite double under `policy`.
// Precondition: !std::isfinite(d).
void append_non_finite(std::string& out, double d, NonFinite policy);

// The sink option choosing the external form: json_non_finite = 'null' (the
// default) or 'string'. kJsonNonFiniteValues lists the accepted values for a
// diagnostic.
inline constexpr std::string_view kJsonNonFiniteOption = "json_non_finite";
inline constexpr std::string_view kJsonNonFiniteValues = "'null', 'string'";

// "null" -> Null, "string" -> String; any other value, Tagged included, is not
// an accepted option value and gives nullopt.
[[nodiscard]] std::optional<NonFinite> non_finite_from_option(std::string_view value) noexcept;

// As non_finite_from_option, throwing std::invalid_argument that names `owner`
// (a sink or table) and the accepted values when `value` is not one of them.
[[nodiscard]] NonFinite require_non_finite_option(std::string_view value, std::string_view owner);

class JsonValue {
public:
    // Int is appended LAST so the existing alternatives keep their indices
    // (type() is static_cast<Type>(value_.index()), so the enum order and the
    // variant order must stay in lock-step). Number = the double alternative,
    // Int = the int64 alternative; is_number() covers both.
    enum class Type { Null, Bool, Number, String, Array, Object, Int };

    JsonValue() = default;  // null
    JsonValue(std::nullptr_t) : value_(std::monostate{}) {}
    JsonValue(bool v) : value_(v) {}
    JsonValue(int v) : value_(static_cast<std::int64_t>(v)) {}
    JsonValue(std::int64_t v) : value_(v) {}
    JsonValue(double v) : value_(v) {}
    JsonValue(const char* v) : value_(std::string{v}) {}
    JsonValue(std::string v) : value_(std::move(v)) {}
    JsonValue(JsonArray v) : value_(std::move(v)) {}
    JsonValue(JsonObject v) : value_(std::move(v)) {}

    // Retained payload estimate, including vector capacity and nested values.
    // String capacity includes inline storage, deliberately conservative.
    [[nodiscard]] std::size_t retained_bytes() const {
        std::size_t bytes = sizeof(*this);
        if (is_string())
            bytes += as_string().capacity() + 1;
        else if (is_array()) {
            bytes += as_array().capacity() * sizeof(JsonValue);
            for (const auto& v : as_array())
                bytes += v.retained_bytes() - sizeof(v);
        } else if (is_object()) {
            bytes += as_object().capacity() * sizeof(JsonObject::value_type);
            for (const auto& [k, v] : as_object())
                bytes += k.capacity() + 1 + v.retained_bytes() - sizeof(v);
        }
        return bytes;
    }

    Type type() const noexcept { return static_cast<Type>(value_.index()); }

    bool is_null() const noexcept { return type() == Type::Null; }
    bool is_bool() const noexcept { return type() == Type::Bool; }
    bool is_number() const noexcept {
        const Type t = type();
        return t == Type::Number || t == Type::Int;
    }
    // True only for an exact integer (an integral token or an int-constructed
    // value), not a double. Use with as_int() for the exact-arithmetic paths.
    bool is_integral_number() const noexcept { return type() == Type::Int; }
    bool is_string() const noexcept { return type() == Type::String; }
    bool is_array() const noexcept { return type() == Type::Array; }
    bool is_object() const noexcept { return type() == Type::Object; }

    bool as_bool() const { return std::get<bool>(value_); }
    // Widening read: an int64 value is returned as a double (lossy past 2^53),
    // a double verbatim. The tolerant accessor almost all callers use.
    double as_number() const {
        if (const auto* i = std::get_if<std::int64_t>(&value_)) {
            return static_cast<double>(*i);
        }
        return std::get<double>(value_);
    }
    // Exact integer read. A double value is truncated toward zero; callers on
    // the exact path should gate on is_integral_number() first.
    std::int64_t as_int() const {
        if (const auto* i = std::get_if<std::int64_t>(&value_)) {
            return *i;
        }
        return static_cast<std::int64_t>(std::get<double>(value_));
    }
    const std::string& as_string() const { return std::get<std::string>(value_); }
    const JsonArray& as_array() const { return std::get<JsonArray>(value_); }
    const JsonObject& as_object() const { return std::get<JsonObject>(value_); }
    JsonArray& as_array() { return std::get<JsonArray>(value_); }
    JsonObject& as_object() { return std::get<JsonObject>(value_); }

    // Convenience accessors for object members. Throws if not an object
    // or the key is missing (caller can use .contains() to pre-check).
    bool contains(std::string_view key) const;
    const JsonValue& at(std::string_view key) const;
    // String/number/bool with default. Throws on type mismatch when the
    // key is present.
    std::string string_or(std::string_view key, std::string fallback) const;
    std::int64_t int_or(std::string_view key, std::int64_t fallback) const;
    bool bool_or(std::string_view key, bool fallback) const;

    // Stringify back to JSON text. `indent_width = 0` produces compact
    // output; > 0 produces pretty-printed output. A non-finite double is
    // written in the Tagged form (see NonFinite), so the text always parses
    // and reads back as the same value. serialize_into() and
    // serialize_kind_preserving_into() do the same.
    std::string serialize(int indent_width = 0) const;
    // Append the compact form to `out` (no clear). The zero-temporary
    // seam for hot per-record callers building composite strings -
    // group/join keys append each key column's value in place instead
    // of materialising an intermediate string via serialize(0).
    void serialize_into(std::string& out) const;
    // Compact form that keeps every number's kind through a parse: a double with
    // an integral value is written with a ".0" fraction (9.0, not 9; -0.0 keeps
    // its sign), so parse() reads it back as a double, while an integer stays a
    // bare integer. Every other byte matches serialize_into(). The output is
    // plain JSON, so any reader accepts it. For the row codec between operators,
    // where the kind decides integer or floating arithmetic downstream; external
    // output and key text keep serialize()/serialize_output().
    void serialize_kind_preserving_into(std::string& out) const;
    // Compact form for a reader other than clink: sink output and HTTP API
    // responses. Byte-identical to serialize_into() except that a non-finite
    // double is written per `policy`, null by default. parse() does not read
    // the Null form back as the double, by design; text clink reads back
    // itself keeps serialize_into().
    void serialize_external_into(std::string& out, NonFinite policy = NonFinite::Null) const;
    [[nodiscard]] std::string serialize_external(NonFinite policy = NonFinite::Null) const;

    // Deep structural equality (recurses through arrays and objects). Numbers
    // compare by VALUE across the int64/double split, so an int-constructed and
    // a double-constructed value of the same magnitude are equal (e.g. after
    // the representation change, JsonValue{5} == JsonValue{5.0}). Two integers
    // compare exactly; any comparison involving a genuine double widens to
    // double. Cannot be `= default`, which would compare the variant
    // alternative and split int64 from double.
    bool operator==(const JsonValue& other) const {
        const bool a_num = is_number();
        const bool b_num = other.is_number();
        if (a_num != b_num) {
            return false;
        }
        if (a_num) {
            if (is_integral_number() && other.is_integral_number()) {
                return as_int() == other.as_int();
            }
            return as_number() == other.as_number();
        }
        return value_ == other.value_;
    }

private:
    using Storage = std::
        variant<std::monostate, bool, double, std::string, JsonArray, JsonObject, std::int64_t>;
    Storage value_;
};

class ParseError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// Parse a JSON document. Throws ParseError on malformed input. A string that
// is exactly a Tagged non-finite (see NonFinite) is read as that double, so
// text clink wrote round-trips; the bare tokens NaN, Infinity, nan and inf are
// not JSON and are rejected. parse_object() below does the same.
JsonValue parse(std::string_view input);

// Row-decode fast path: parse a document whose root must be an object
// and build the JsonObject directly (no generic JsonValue root, no
// exception on malformed input). Returns nullopt for malformed input
// or a non-object root - exactly the cases where a
// try { parse() } catch -> skip caller would drop the record.
std::optional<JsonObject> parse_object(std::string_view input);

// As above, keeping ONLY the top-level keys listed in `keep_keys`: a
// key not in the list is skipped before its value is built, so
// projection pushdown never materialises dropped columns. Membership
// is a linear scan - the list is expected to be small (a query's
// projected columns). Duplicate kept keys keep the first occurrence,
// as in parse().
std::optional<JsonObject> parse_object(std::string_view input,
                                       std::span<const std::string> keep_keys);

// Exact source text of the named top-level fields that are JSON number
// literals, keyed by field name. The generic parse rounds a number to an IEEE
// double, so a DECIMAL column fed a numeral past ~15-17 significant digits
// loses precision; capturing the raw token lets the caller ingest the original
// digits exactly. Only numeric fields present in `fields` are returned (string,
// object, boolean, null, and absent keys are skipped). Each returned string is
// the bare numeral (sign, digits, '.', exponent), trimmed of any trailing
// structural bytes. `input` must be a JSON object; a non-object or malformed
// line yields an empty map. `fields` is treated as a set of names (any mapped
// value is ignored), so a decimal-scale map can be passed directly.
std::map<std::string, std::string> raw_number_tokens(std::string_view input,
                                                     const std::map<std::string, int>& fields);

}  // namespace clink::config
