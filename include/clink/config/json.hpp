#pragma once

#include <cstdint>
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
    // output; > 0 produces pretty-printed output.
    std::string serialize(int indent_width = 0) const;
    // Append the compact form to `out` (no clear). The zero-temporary
    // seam for hot per-record callers building composite strings -
    // group/join keys append each key column's value in place instead
    // of materialising an intermediate string via serialize(0).
    void serialize_into(std::string& out) const;

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

// Parse a JSON document. Throws ParseError on malformed input.
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

}  // namespace clink::config
