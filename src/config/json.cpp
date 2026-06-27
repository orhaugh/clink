#include "clink/config/json.hpp"

#include <cctype>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>

namespace clink::config {

namespace {

// Single-pass recursive-descent JSON parser. Whitespace per RFC 8259:
// space, tab, LF, CR. No comments. Strict - trailing commas and bare
// numbers like "01" are rejected.
class Parser {
public:
    explicit Parser(std::string_view input) : src_(input) {}

    JsonValue parse_document() {
        skip_ws();
        JsonValue v = parse_value();
        skip_ws();
        if (pos_ != src_.size()) {
            throw ParseError("trailing characters after JSON document at " + std::to_string(pos_));
        }
        return v;
    }

private:
    JsonValue parse_value() {
        skip_ws();
        if (pos_ >= src_.size()) {
            throw ParseError("unexpected end of input");
        }
        const char c = src_[pos_];
        switch (c) {
            case '{':
                return parse_object();
            case '[':
                return parse_array();
            case '"':
                return parse_string();
            case 't':
            case 'f':
                return parse_bool();
            case 'n':
                return parse_null();
            default:
                if (c == '-' || (c >= '0' && c <= '9')) {
                    return parse_number();
                }
                throw ParseError("unexpected character '" + std::string(1, c) + "' at " +
                                 std::to_string(pos_));
        }
    }

    JsonValue parse_object() {
        consume('{');
        JsonObject obj;
        skip_ws();
        if (peek() == '}') {
            ++pos_;
            return obj;
        }
        while (true) {
            skip_ws();
            if (peek() != '"') {
                throw ParseError("expected object key at " + std::to_string(pos_));
            }
            std::string key = parse_string_raw();
            skip_ws();
            consume(':');
            JsonValue val = parse_value();
            obj.emplace(std::move(key), std::move(val));
            skip_ws();
            if (peek() == ',') {
                ++pos_;
                continue;
            }
            if (peek() == '}') {
                ++pos_;
                return obj;
            }
            throw ParseError("expected ',' or '}' at " + std::to_string(pos_));
        }
    }

    JsonValue parse_array() {
        consume('[');
        JsonArray arr;
        skip_ws();
        if (peek() == ']') {
            ++pos_;
            return arr;
        }
        while (true) {
            arr.push_back(parse_value());
            skip_ws();
            if (peek() == ',') {
                ++pos_;
                continue;
            }
            if (peek() == ']') {
                ++pos_;
                return arr;
            }
            throw ParseError("expected ',' or ']' at " + std::to_string(pos_));
        }
    }

    JsonValue parse_string() { return JsonValue{parse_string_raw()}; }

    std::string parse_string_raw() {
        consume('"');
        std::string out;
        while (pos_ < src_.size()) {
            const char c = src_[pos_++];
            if (c == '"') {
                return out;
            }
            if (c == '\\') {
                if (pos_ >= src_.size()) {
                    throw ParseError("unterminated escape");
                }
                const char esc = src_[pos_++];
                switch (esc) {
                    case '"':
                        out.push_back('"');
                        break;
                    case '\\':
                        out.push_back('\\');
                        break;
                    case '/':
                        out.push_back('/');
                        break;
                    case 'b':
                        out.push_back('\b');
                        break;
                    case 'f':
                        out.push_back('\f');
                        break;
                    case 'n':
                        out.push_back('\n');
                        break;
                    case 'r':
                        out.push_back('\r');
                        break;
                    case 't':
                        out.push_back('\t');
                        break;
                    case 'u': {
                        if (pos_ + 4 > src_.size()) {
                            throw ParseError("incomplete \\u escape");
                        }
                        std::uint32_t cp = 0;
                        for (int i = 0; i < 4; ++i) {
                            cp = (cp << 4) | hex_digit_(src_[pos_++]);
                        }
                        // Encode as UTF-8. Surrogate pairs aren't handled
                        // (rare in config files); a high surrogate just
                        // emits its raw 3-byte UTF-8 form.
                        if (cp < 0x80) {
                            out.push_back(static_cast<char>(cp));
                        } else if (cp < 0x800) {
                            out.push_back(static_cast<char>(0xC0 | (cp >> 6)));
                            out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
                        } else {
                            out.push_back(static_cast<char>(0xE0 | (cp >> 12)));
                            out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
                            out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
                        }
                        break;
                    }
                    default:
                        throw ParseError("unknown escape \\" + std::string(1, esc));
                }
            } else if (static_cast<unsigned char>(c) < 0x20) {
                throw ParseError("unescaped control char in string");
            } else {
                out.push_back(c);
            }
        }
        throw ParseError("unterminated string");
    }

    JsonValue parse_bool() {
        if (src_.substr(pos_, 4) == "true") {
            pos_ += 4;
            return JsonValue{true};
        }
        if (src_.substr(pos_, 5) == "false") {
            pos_ += 5;
            return JsonValue{false};
        }
        throw ParseError("expected true/false at " + std::to_string(pos_));
    }

    JsonValue parse_null() {
        if (src_.substr(pos_, 4) == "null") {
            pos_ += 4;
            return JsonValue{};
        }
        throw ParseError("expected null at " + std::to_string(pos_));
    }

    JsonValue parse_number() {
        const std::size_t start = pos_;
        if (src_[pos_] == '-') {
            ++pos_;
        }
        // integer part: 0 alone, or 1-9 followed by digits.
        if (pos_ >= src_.size()) {
            throw ParseError("malformed number");
        }
        if (src_[pos_] == '0') {
            ++pos_;
        } else if (src_[pos_] >= '1' && src_[pos_] <= '9') {
            while (pos_ < src_.size() && std::isdigit(static_cast<unsigned char>(src_[pos_]))) {
                ++pos_;
            }
        } else {
            throw ParseError("malformed number at " + std::to_string(pos_));
        }
        // fraction
        if (pos_ < src_.size() && src_[pos_] == '.') {
            ++pos_;
            const std::size_t frac_start = pos_;
            while (pos_ < src_.size() && std::isdigit(static_cast<unsigned char>(src_[pos_]))) {
                ++pos_;
            }
            if (pos_ == frac_start) {
                throw ParseError("expected digits after '.'");
            }
        }
        // exponent
        if (pos_ < src_.size() && (src_[pos_] == 'e' || src_[pos_] == 'E')) {
            ++pos_;
            if (pos_ < src_.size() && (src_[pos_] == '+' || src_[pos_] == '-')) {
                ++pos_;
            }
            const std::size_t exp_start = pos_;
            while (pos_ < src_.size() && std::isdigit(static_cast<unsigned char>(src_[pos_]))) {
                ++pos_;
            }
            if (pos_ == exp_start) {
                throw ParseError("expected exponent digits");
            }
        }

        const std::string token{src_.substr(start, pos_ - start)};
        try {
            return JsonValue{std::stod(token)};
        } catch (const std::exception&) {
            throw ParseError("number out of range: " + token);
        }
    }

    void consume(char c) {
        if (pos_ >= src_.size() || src_[pos_] != c) {
            throw ParseError("expected '" + std::string(1, c) + "' at " + std::to_string(pos_));
        }
        ++pos_;
    }

    void skip_ws() {
        while (pos_ < src_.size()) {
            const char c = src_[pos_];
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                ++pos_;
            } else {
                return;
            }
        }
    }

    char peek() const { return pos_ < src_.size() ? src_[pos_] : '\0'; }

    static unsigned hex_digit_(char c) {
        if (c >= '0' && c <= '9')
            return c - '0';
        if (c >= 'a' && c <= 'f')
            return 10 + (c - 'a');
        if (c >= 'A' && c <= 'F')
            return 10 + (c - 'A');
        throw ParseError("bad hex digit '" + std::string(1, c) + "'");
    }

    std::string_view src_;
    std::size_t pos_{0};
};

void escape_string(std::ostringstream& out, const std::string& s) {
    out.put('"');
    for (const char c : s) {
        switch (c) {
            case '"':
                out << "\\\"";
                break;
            case '\\':
                out << "\\\\";
                break;
            case '\b':
                out << "\\b";
                break;
            case '\f':
                out << "\\f";
                break;
            case '\n':
                out << "\\n";
                break;
            case '\r':
                out << "\\r";
                break;
            case '\t':
                out << "\\t";
                break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", c);
                    out << buf;
                } else {
                    out.put(c);
                }
        }
    }
    out.put('"');
}

void serialize_to(std::ostringstream& out, const JsonValue& v, int indent_width, int depth) {
    auto indent = [&](int n) {
        if (indent_width > 0) {
            for (int i = 0; i < n * indent_width; ++i) {
                out.put(' ');
            }
        }
    };

    switch (v.type()) {
        case JsonValue::Type::Null:
            out << "null";
            return;
        case JsonValue::Type::Bool:
            out << (v.as_bool() ? "true" : "false");
            return;
        case JsonValue::Type::String:
            escape_string(out, v.as_string());
            return;
        case JsonValue::Type::Number: {
            const double d = v.as_number();
            // Render integers without trailing ".0" for nicer output. The range
            // guard is load-bearing: a double->int64 cast is UB outside int64, and
            // the magnitude check must short-circuit BEFORE the equality cast.
            // 2^63 is exactly representable as a double, so the upper bound is '<'.
            constexpr double kInt64Lo = -9223372036854775808.0;          // -2^63
            constexpr double kInt64HiExclusive = 9223372036854775808.0;  //  2^63
            if (std::isfinite(d) && d >= kInt64Lo && d < kInt64HiExclusive &&
                d == static_cast<double>(static_cast<std::int64_t>(d))) {
                out << static_cast<std::int64_t>(d);
            } else {
                out << d;
            }
            return;
        }
        case JsonValue::Type::Array: {
            const auto& arr = v.as_array();
            if (arr.empty()) {
                out << "[]";
                return;
            }
            out.put('[');
            for (std::size_t i = 0; i < arr.size(); ++i) {
                if (indent_width > 0) {
                    out.put('\n');
                    indent(depth + 1);
                }
                serialize_to(out, arr[i], indent_width, depth + 1);
                if (i + 1 < arr.size()) {
                    out.put(',');
                    if (indent_width > 0) {
                        out.put(' ');
                    }
                }
            }
            if (indent_width > 0) {
                out.put('\n');
                indent(depth);
            }
            out.put(']');
            return;
        }
        case JsonValue::Type::Object: {
            const auto& obj = v.as_object();
            if (obj.empty()) {
                out << "{}";
                return;
            }
            out.put('{');
            std::size_t i = 0;
            for (const auto& [k, val] : obj) {
                if (indent_width > 0) {
                    out.put('\n');
                    indent(depth + 1);
                }
                escape_string(out, k);
                out.put(':');
                if (indent_width > 0) {
                    out.put(' ');
                }
                serialize_to(out, val, indent_width, depth + 1);
                if (i + 1 < obj.size()) {
                    out.put(',');
                    if (indent_width > 0) {
                        out.put(' ');
                    }
                }
                ++i;
            }
            if (indent_width > 0) {
                out.put('\n');
                indent(depth);
            }
            out.put('}');
            return;
        }
    }
}

}  // namespace

bool JsonValue::contains(std::string_view key) const {
    if (!is_object()) {
        throw std::runtime_error("JsonValue::contains: not an object");
    }
    return as_object().find(std::string{key}) != as_object().end();
}

const JsonValue& JsonValue::at(std::string_view key) const {
    if (!is_object()) {
        throw std::runtime_error("JsonValue::at: not an object");
    }
    auto it = as_object().find(std::string{key});
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
    std::ostringstream out;
    serialize_to(out, *this, indent_width, 0);
    return out.str();
}

JsonValue parse(std::string_view input) {
    Parser p(input);
    return p.parse_document();
}

}  // namespace clink::config
