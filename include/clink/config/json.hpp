#pragma once

#include <cstdint>
#include <map>
#include <stdexcept>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace clink::config {

// Minimal JSON value type. Sufficient for pipeline config files;
// deliberately not a full-featured library:
//
//   * Numbers are stored as double; the exact-integer/floating distinction
//     isn't preserved on round-trip.
//   * Object key order is sorted lexicographically (std::map). Tools that
//     care about authoring order will produce slightly different output.
//   * No comments, no trailing commas (strict JSON), no streaming parser.
//
// Throws ParseError on malformed input.
class JsonValue;

using JsonObject = std::map<std::string, JsonValue>;
using JsonArray = std::vector<JsonValue>;

class JsonValue {
public:
    enum class Type { Null, Bool, Number, String, Array, Object };

    JsonValue() = default;  // null
    JsonValue(std::nullptr_t) : value_(std::monostate{}) {}
    JsonValue(bool v) : value_(v) {}
    JsonValue(int v) : value_(static_cast<double>(v)) {}
    JsonValue(std::int64_t v) : value_(static_cast<double>(v)) {}
    JsonValue(double v) : value_(v) {}
    JsonValue(const char* v) : value_(std::string{v}) {}
    JsonValue(std::string v) : value_(std::move(v)) {}
    JsonValue(JsonArray v) : value_(std::move(v)) {}
    JsonValue(JsonObject v) : value_(std::move(v)) {}

    Type type() const noexcept { return static_cast<Type>(value_.index()); }

    bool is_null() const noexcept { return type() == Type::Null; }
    bool is_bool() const noexcept { return type() == Type::Bool; }
    bool is_number() const noexcept { return type() == Type::Number; }
    bool is_string() const noexcept { return type() == Type::String; }
    bool is_array() const noexcept { return type() == Type::Array; }
    bool is_object() const noexcept { return type() == Type::Object; }

    bool as_bool() const { return std::get<bool>(value_); }
    double as_number() const { return std::get<double>(value_); }
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

private:
    using Storage = std::variant<std::monostate, bool, double, std::string, JsonArray, JsonObject>;
    Storage value_;
};

class ParseError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// Parse a JSON document. Throws ParseError on malformed input.
JsonValue parse(std::string_view input);

}  // namespace clink::config
