#include "clink/config/json.hpp"

#include <cctype>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <optional>
#include <simdjson.h>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace clink::config {

namespace {

// Single-pass recursive-descent JSON parser. Whitespace per RFC 8259:
// space, tab, LF, CR. No comments. Strict - trailing commas and bare
// numbers like "01" are rejected.
// config::parse is backed by simdjson (DOM API): SIMD-accelerated
// tokenising, number parsing and string unescaping, feeding the same
// JsonValue tree as before. This path runs on EVERY Row-channel hop -
// the source's per-record decode and each network bridge's wire decode -
// so its speed sets the ceiling for the whole row pipeline. Semantics
// preserved from the hand-rolled parser it replaced: strict RFC 8259,
// no trailing content, numbers surface as double, duplicate object keys
// keep the FIRST occurrence, \uXXXX escapes decode to UTF-8, and
// malformed input throws ParseError.
JsonValue from_dom(const simdjson::dom::element& e) {
    switch (e.type()) {
        case simdjson::dom::element_type::OBJECT: {
            const auto o = simdjson::dom::object(e);
            std::vector<JsonObject::value_type> entries;
            entries.reserve(o.size());
            for (auto [key, value] : o) {
                entries.emplace_back(std::string(key), from_dom(value));
            }
            // from_entries keeps the FIRST duplicate (documented parse
            // semantics) and is O(n) for already-key-sorted documents.
            return JsonValue{JsonObject::from_entries(std::move(entries))};
        }
        case simdjson::dom::element_type::ARRAY: {
            JsonArray arr;
            for (auto item : simdjson::dom::array(e)) {
                arr.push_back(from_dom(item));
            }
            return JsonValue{std::move(arr)};
        }
        case simdjson::dom::element_type::STRING:
            return JsonValue{std::string(std::string_view(e))};
        case simdjson::dom::element_type::INT64:
            // Integer tokens are carried EXACTLY as int64 (no double round-trip),
            // so a BIGINT column survives past 2^53.
            return JsonValue{std::int64_t(e)};
        case simdjson::dom::element_type::UINT64: {
            // Fits int64 -> exact; above INT64_MAX -> widen to double (lossy for
            // very large unsigned values, but never wraps negative).
            const std::uint64_t u = std::uint64_t(e);
            if (u <= static_cast<std::uint64_t>(INT64_MAX)) {
                return JsonValue{static_cast<std::int64_t>(u)};
            }
            return JsonValue{static_cast<double>(u)};
        }
        case simdjson::dom::element_type::DOUBLE:
            return JsonValue{double(e)};
        case simdjson::dom::element_type::BOOL:
            return JsonValue{bool(e)};
        case simdjson::dom::element_type::NULL_VALUE:
            return JsonValue{nullptr};
    }
    throw ParseError("unhandled JSON element type");
}

// Serialisation builds directly into a std::string. This runs on hot
// per-record paths (group/join key building, DISTINCT, changelog
// payloads), so there is deliberately no ostream layer; the output is
// byte-identical to the previous ostringstream-based writer, including
// the double rendering ("%g" is the printf conversion that ostream's
// default general float format is specified in terms of).
void escape_string(std::string& out, const std::string& s) {
    out.push_back('"');
    for (const char c : s) {
        switch (c) {
            case '"':
                out += "\\\"";
                break;
            case '\\':
                out += "\\\\";
                break;
            case '\b':
                out += "\\b";
                break;
            case '\f':
                out += "\\f";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", c);
                    out += buf;
                } else {
                    out.push_back(c);
                }
        }
    }
    out.push_back('"');
}

void serialize_append(std::string& out, const JsonValue& v, int indent_width, int depth) {
    auto indent = [&](int n) {
        if (indent_width > 0) {
            out.append(static_cast<std::size_t>(n) * static_cast<std::size_t>(indent_width), ' ');
        }
    };

    switch (v.type()) {
        case JsonValue::Type::Null:
            out += "null";
            return;
        case JsonValue::Type::Bool:
            out += v.as_bool() ? "true" : "false";
            return;
        case JsonValue::Type::String:
            escape_string(out, v.as_string());
            return;
        case JsonValue::Type::Int: {
            // Exact integer: render verbatim via to_chars (no double round-trip,
            // so values past 2^53 print exactly).
            char buf[32];
            auto res = std::to_chars(buf, buf + sizeof(buf), v.as_int());
            out.append(buf, static_cast<std::size_t>(res.ptr - buf));
            return;
        }
        case JsonValue::Type::Number: {
            const double d = v.as_number();
            // Render integers without trailing ".0" for nicer output. The range
            // guard is load-bearing: a double->int64 cast is UB outside int64, and
            // the magnitude check must short-circuit BEFORE the equality cast.
            // 2^63 is exactly representable as a double, so the upper bound is '<'.
            constexpr double kInt64Lo = -9223372036854775808.0;          // -2^63
            constexpr double kInt64HiExclusive = 9223372036854775808.0;  //  2^63
            char buf[32];
            if (std::isfinite(d) && d >= kInt64Lo && d < kInt64HiExclusive &&
                d == static_cast<double>(static_cast<std::int64_t>(d))) {
                auto res = std::to_chars(buf, buf + sizeof(buf), static_cast<std::int64_t>(d));
                out.append(buf, static_cast<std::size_t>(res.ptr - buf));
            } else {
                const int n = std::snprintf(buf, sizeof(buf), "%g", d);
                out.append(buf, static_cast<std::size_t>(n));
            }
            return;
        }
        case JsonValue::Type::Array: {
            const auto& arr = v.as_array();
            if (arr.empty()) {
                out += "[]";
                return;
            }
            out.push_back('[');
            for (std::size_t i = 0; i < arr.size(); ++i) {
                if (indent_width > 0) {
                    out.push_back('\n');
                    indent(depth + 1);
                }
                serialize_append(out, arr[i], indent_width, depth + 1);
                if (i + 1 < arr.size()) {
                    out.push_back(',');
                    if (indent_width > 0) {
                        out.push_back(' ');
                    }
                }
            }
            if (indent_width > 0) {
                out.push_back('\n');
                indent(depth);
            }
            out.push_back(']');
            return;
        }
        case JsonValue::Type::Object: {
            const auto& obj = v.as_object();
            if (obj.empty()) {
                out += "{}";
                return;
            }
            out.push_back('{');
            std::size_t i = 0;
            for (const auto& [k, val] : obj) {
                if (indent_width > 0) {
                    out.push_back('\n');
                    indent(depth + 1);
                }
                escape_string(out, k);
                out.push_back(':');
                if (indent_width > 0) {
                    out.push_back(' ');
                }
                serialize_append(out, val, indent_width, depth + 1);
                if (i + 1 < obj.size()) {
                    out.push_back(',');
                    if (indent_width > 0) {
                        out.push_back(' ');
                    }
                }
                ++i;
            }
            if (indent_width > 0) {
                out.push_back('\n');
                indent(depth);
            }
            out.push_back('}');
            return;
        }
    }
}

}  // namespace

bool JsonValue::contains(std::string_view key) const {
    if (!is_object()) {
        throw std::runtime_error("JsonValue::contains: not an object");
    }
    return as_object().contains(key);
}

const JsonValue& JsonValue::at(std::string_view key) const {
    if (!is_object()) {
        throw std::runtime_error("JsonValue::at: not an object");
    }
    auto it = as_object().find(key);
    if (it == as_object().end()) {
        throw std::runtime_error("JsonValue::at: missing key '" + std::string{key} + "'");
    }
    return it->second;
}

std::string JsonValue::string_or(std::string_view key, std::string fallback) const {
    if (!is_object() || !contains(key)) {
        return fallback;
    }
    const auto& v = at(key);
    if (!v.is_string()) {
        throw std::runtime_error("JsonValue::string_or: '" + std::string{key} +
                                 "' is not a string");
    }
    return v.as_string();
}

std::int64_t JsonValue::int_or(std::string_view key, std::int64_t fallback) const {
    if (!is_object() || !contains(key)) {
        return fallback;
    }
    const auto& v = at(key);
    if (!v.is_number()) {
        throw std::runtime_error("JsonValue::int_or: '" + std::string{key} + "' is not a number");
    }
    // Exact integer: return it directly (no lossy double round-trip).
    if (v.is_integral_number()) {
        return v.as_int();
    }
    // A double->int64 cast is UB for NaN/Inf or a magnitude >= 2^63; such a value
    // cannot be represented as int64, so return the fallback rather than risk UB.
    const double d = v.as_number();
    constexpr double kInt64Lo = -9223372036854775808.0;          // -2^63
    constexpr double kInt64HiExclusive = 9223372036854775808.0;  //  2^63
    if (!std::isfinite(d) || d < kInt64Lo || d >= kInt64HiExclusive) {
        return fallback;
    }
    return static_cast<std::int64_t>(d);
}

bool JsonValue::bool_or(std::string_view key, bool fallback) const {
    if (!is_object() || !contains(key)) {
        return fallback;
    }
    const auto& v = at(key);
    if (!v.is_bool()) {
        throw std::runtime_error("JsonValue::bool_or: '" + std::string{key} + "' is not a bool");
    }
    return v.as_bool();
}

std::string JsonValue::serialize(int indent_width) const {
    std::string out;
    serialize_append(out, *this, indent_width, 0);
    return out;
}

void JsonValue::serialize_into(std::string& out) const {
    serialize_append(out, *this, 0, 0);
}

namespace {

simdjson::dom::parser& thread_parser() {
    // One parser per thread: its internal padded buffer and structural
    // tape are reused across calls, so a hot decode loop allocates only
    // for the JsonValue tree itself. parse() copies the input into the
    // padded buffer (simdjson needs SIMDJSON_PADDING readable bytes past
    // the end), which a future zero-copy seam can lift by having sources
    // hand over pre-padded buffers. Heap-allocated and deliberately never
    // destroyed: TLS destructor order at thread exit is not sequenced
    // against the rest of teardown, and reclaiming one per-thread buffer
    // is the OS's job at exit anyway.
    thread_local auto* parser = new simdjson::dom::parser();
    return *parser;
}

}  // namespace

JsonValue parse(std::string_view input) {
    auto result = thread_parser().parse(input.data(), input.size());
    if (result.error() != simdjson::SUCCESS) {
        throw ParseError(simdjson::error_message(result.error()));
    }
    return from_dom(result.value_unsafe());
}

std::optional<JsonObject> parse_object(std::string_view input) {
    // The row-decode fast path: parse and build the top-level object
    // directly, skipping the generic JsonValue root and the exception
    // round-trip for malformed input. Returns nullopt where the generic
    // path would throw ParseError or yield a non-object, so callers that
    // previously did try { parse() } catch -> skip keep identical
    // behaviour. Duplicate keys keep the FIRST occurrence, as in parse().
    auto result = thread_parser().parse(input.data(), input.size());
    if (result.error() != simdjson::SUCCESS ||
        result.value_unsafe().type() != simdjson::dom::element_type::OBJECT) {
        return std::nullopt;
    }
    const auto o = simdjson::dom::object(result.value_unsafe());
    std::vector<JsonObject::value_type> entries;
    entries.reserve(o.size());
    for (auto [key, value] : o) {
        entries.emplace_back(std::string(key), from_dom(value));
    }
    return JsonObject::from_entries(std::move(entries));
}

std::optional<JsonObject> parse_object(std::string_view input,
                                       std::span<const std::string> keep_keys) {
    auto result = thread_parser().parse(input.data(), input.size());
    if (result.error() != simdjson::SUCCESS ||
        result.value_unsafe().type() != simdjson::dom::element_type::OBJECT) {
        return std::nullopt;
    }
    auto keep = [&](std::string_view key) {
        for (const auto& k : keep_keys) {
            if (k == key) {
                return true;
            }
        }
        return false;
    };
    const auto o = simdjson::dom::object(result.value_unsafe());
    std::vector<JsonObject::value_type> entries;
    entries.reserve(keep_keys.size());
    for (auto [key, value] : o) {
        // A dropped key's value is never built - projection pushdown does
        // no work for columns the query does not carry.
        if (keep(key)) {
            entries.emplace_back(std::string(key), from_dom(value));
        }
    }
    return JsonObject::from_entries(std::move(entries));
}

namespace {

simdjson::ondemand::parser& thread_ondemand_parser() {
    // A separate thread-local parser for raw-token extraction. Same lifetime
    // rationale as thread_parser(): reused per thread, never destroyed.
    thread_local auto* parser = new simdjson::ondemand::parser();
    return *parser;
}

// The bare numeric prefix of a raw token: sign, digits, decimal point, exponent.
// On-demand raw_json_token() can carry trailing structural bytes (a comma or
// closing brace) or whitespace for a scalar, so trim to the numeral before it
// is handed to the decimal parser.
std::string_view numeric_prefix(std::string_view tok) {
    std::size_t n = 0;
    while (n < tok.size()) {
        const char c = tok[n];
        const bool numeric =
            (c >= '0' && c <= '9') || c == '+' || c == '-' || c == '.' || c == 'e' || c == 'E';
        if (!numeric) {
            break;
        }
        ++n;
    }
    return tok.substr(0, n);
}

}  // namespace

std::map<std::string, std::string> raw_number_tokens(std::string_view input,
                                                     const std::map<std::string, int>& fields) {
    std::map<std::string, std::string> out;
    if (fields.empty()) {
        return out;
    }
    // On-demand needs SIMDJSON_PADDING readable bytes past the end; padded_string
    // owns a padded copy, so raw_json_token() views stay valid until it drops.
    simdjson::padded_string padded(input);
    auto doc = thread_ondemand_parser().iterate(padded);
    if (doc.error()) {
        return out;
    }
    auto obj = doc.value_unsafe().get_object();
    if (obj.error()) {
        return out;
    }
    for (auto field : obj.value_unsafe()) {
        auto key = field.unescaped_key();
        if (key.error()) {
            continue;
        }
        std::string name(key.value_unsafe());
        if (fields.find(name) == fields.end()) {
            continue;
        }
        auto value = field.value();
        if (value.error()) {
            continue;
        }
        auto type = value.value_unsafe().type();
        if (type.error() || type.value_unsafe() != simdjson::ondemand::json_type::number) {
            continue;
        }
        const std::string_view num = numeric_prefix(value.value_unsafe().raw_json_token());
        if (!num.empty()) {
            out.emplace(std::move(name), std::string(num));
        }
    }
    return out;
}

}  // namespace clink::config
