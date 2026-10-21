#include "types.hpp"

#include <array>
#include <charconv>
#include <cmath>
#include <cstddef>
#include <limits>
#include <system_error>
#include <utility>

#include "clink/config/decimal.hpp"
#include "clink/config/json.hpp"

#include "options.hpp"

namespace clink::neo4j {

namespace {

using clink::config::JsonValue;

std::string_view trim(std::string_view s) {
    while (!s.empty() && s.front() == ' ') {
        s.remove_prefix(1);
    }
    while (!s.empty() && s.back() == ' ') {
        s.remove_suffix(1);
    }
    return s;
}

// Splits at the commas (or `sep`) that are not inside <...> or (...).
std::vector<std::string_view> split_top(std::string_view s, char sep) {
    std::vector<std::string_view> out;
    int depth = 0;
    std::size_t start = 0;
    for (std::size_t i = 0; i < s.size(); ++i) {
        const char c = s[i];
        if (c == '<' || c == '(') {
            ++depth;
        } else if (c == '>' || c == ')') {
            --depth;
        } else if (c == sep && depth == 0) {
            out.push_back(s.substr(start, i - start));
            start = i + 1;
        }
    }
    out.push_back(s.substr(start));
    return out;
}

SqlType parse_type(std::string_view raw) {
    const std::string_view s = trim(raw);
    SqlType t;
    t.spelling = std::string(s);
    constexpr std::string_view kArray = " ARRAY";
    if (s.size() > kArray.size() && s.ends_with(kArray)) {
        t.kind = Kind::Array;
        t.children.push_back(parse_type(s.substr(0, s.size() - kArray.size())));
        return t;
    }
    if (s.starts_with("ROW<") && s.ends_with('>')) {
        t.kind = Kind::Row;
        const auto body = s.substr(4, s.size() - 5);
        for (const auto field : split_top(body, ',')) {
            const auto f = trim(field);
            const auto space = f.find(' ');
            if (f.empty() || space == std::string_view::npos || space == 0) {
                t.kind = Kind::Unsupported;
                return t;
            }
            t.field_names.emplace_back(f.substr(0, space));
            t.children.push_back(parse_type(f.substr(space + 1)));
        }
        return t;
    }
    if (s.starts_with("MAP<") && s.ends_with('>')) {
        const auto parts = split_top(s.substr(4, s.size() - 5), ',');
        if (parts.size() != 2) {
            return t;
        }
        t.kind = Kind::Map;
        t.children.push_back(parse_type(parts[0]));
        t.children.push_back(parse_type(parts[1]));
        return t;
    }
    if (s.starts_with("DECIMAL(")) {
        t.kind = Kind::Decimal;
        return t;
    }
    if (s.starts_with("TIMESTAMP(")) {
        const auto close = s.find(')');
        if (close == std::string_view::npos) {
            return t;
        }
        const auto rest = trim(s.substr(close + 1));
        if (rest.empty()) {
            t.kind = Kind::Timestamp;
        } else if (rest == "WITH TIME ZONE") {
            t.kind = Kind::Timestamp;
            t.with_time_zone = true;
        }
        return t;
    }
    static constexpr std::array<std::pair<std::string_view, Kind>, 11> kPlain{{
        {"TINYINT", Kind::TinyInt},
        {"SMALLINT", Kind::SmallInt},
        {"INTEGER", Kind::Integer},
        {"BIGINT", Kind::BigInt},
        {"REAL", Kind::Real},
        {"DOUBLE", Kind::Double},
        {"BOOLEAN", Kind::Boolean},
        {"VARCHAR", Kind::Varchar},
        {"DATE", Kind::Date},
        {"TIME", Kind::Time},
        {"BYTEA", Kind::Bytea},
    }};
    for (const auto& [name, kind] : kPlain) {
        if (s == name) {
            t.kind = kind;
            return t;
        }
    }
    return t;
}

// Why a declared type cannot be sent, or empty when it can.
std::string refusal(const SqlType& t) {
    switch (t.kind) {
        case Kind::Decimal:
            return "Neo4j has no decimal type; CAST the column to VARCHAR to keep every digit, or "
                   "to DOUBLE to store it rounded";
        case Kind::Time:
            return "TIME is not supported by this sink; CAST it to VARCHAR";
        case Kind::Bytea:
            return "BYTEA is not supported by this sink; CAST it to VARCHAR";
        case Kind::Unsupported:
            return "the type has no Neo4j mapping in this sink";
        case Kind::Array:
            return t.children.empty() ? "an ARRAY without an element type"
                                      : refusal(t.children.front());
        case Kind::Map:
            if (t.children.size() != 2) {
                return "a MAP without a key and value type";
            }
            if (t.children[0].kind != Kind::Varchar) {
                return "a Neo4j map is keyed by strings: declare MAP<VARCHAR, ...>";
            }
            return refusal(t.children[1]);
        case Kind::Row:
            for (const auto& c : t.children) {
                if (auto why = refusal(c); !why.empty()) {
                    return why;
                }
            }
            return {};
        default:
            return {};
    }
}

const char* kind_of(const JsonValue& v) {
    if (v.is_null()) {
        return "null";
    }
    if (v.is_bool()) {
        return "a boolean";
    }
    if (v.is_integral_number()) {
        return "an integer";
    }
    if (v.is_number()) {
        return "a floating-point number";
    }
    if (clink::config::is_dec_string(v)) {
        return "a decimal";
    }
    if (v.is_string()) {
        return "a string";
    }
    if (v.is_array()) {
        return "an array";
    }
    return "an object";
}

[[noreturn]] void wrong(const std::string& path, const SqlType& t, const std::string& why) {
    throw Neo4jSinkError(code::kValueInvalid,
                         "neo4j sink: column '" + path + "' (" + t.spelling + "): " + why);
}

void typed(std::string_view type, std::string_view value_text, std::string& out) {
    out += "{\"$type\":\"";
    out += type;
    out += "\",\"_value\":\"";
    out += value_text;
    out += "\"}";
}

// Strictly the whole of `s` as a signed decimal integer.
bool parse_int(std::string_view s, std::int64_t& v) {
    if (s.empty()) {
        return false;
    }
    const auto* first = s.data();
    const auto* last = s.data() + s.size();
    const auto [ptr, ec] = std::from_chars(first, last, v);
    return ec == std::errc{} && ptr == last;
}

// The value of an integer cell: an exact integer, an integral double that an
// int64 holds, or the digits of one as text.
bool integer_of(const JsonValue& v, std::int64_t& out) {
    if (v.is_integral_number()) {
        out = v.as_int();
        return true;
    }
    if (v.is_number()) {
        const double d = v.as_number();
        if (!std::isfinite(d) || d != std::trunc(d) || d < -9223372036854775808.0 ||
            d >= 9223372036854775808.0) {
            return false;
        }
        out = static_cast<std::int64_t>(d);
        return true;
    }
    if (v.is_string() && !clink::config::is_dec_string(v)) {
        return parse_int(v.as_string(), out);
    }
    return false;
}

void append_float(double d, std::string& out) {
    if (std::isnan(d)) {
        out += "NaN";
        return;
    }
    if (std::isinf(d)) {
        out += d > 0 ? "Infinity" : "-Infinity";
        return;
    }
    std::array<char, 32> buf{};
    const auto [ptr, ec] = std::to_chars(buf.data(), buf.data() + buf.size(), d);
    (void)ec;
    out.append(buf.data(), ptr);
}

void encode_value(const SqlType& t, const JsonValue& v, const std::string& path, std::string& out);

void encode_text(const SqlType& t, const JsonValue& v, const std::string& path, std::string& out) {
    std::string text;
    if (clink::config::is_dec_string(v)) {
        text = v.as_string().substr(1);
    } else if (v.is_string()) {
        text = v.as_string();
    } else if (v.is_bool()) {
        text = v.as_bool() ? "true" : "false";
    } else if (v.is_integral_number()) {
        text = std::to_string(v.as_int());
    } else if (v.is_number()) {
        append_float(v.as_number(), text);
    } else {
        wrong(path, t, std::string("expected text, got ") + kind_of(v));
    }
    if (!valid_utf8(text)) {
        wrong(path, t, "the text is not valid UTF-8");
    }
    out += "{\"$type\":\"String\",\"_value\":";
    append_json_string(text, out);
    out += "}";
}

void encode_value(const SqlType& t, const JsonValue& v, const std::string& path, std::string& out) {
    if (v.is_null()) {
        out += "{\"$type\":\"Null\",\"_value\":null}";
        return;
    }
    switch (t.kind) {
        case Kind::TinyInt:
        case Kind::SmallInt:
        case Kind::Integer:
        case Kind::BigInt: {
            std::int64_t i = 0;
            if (!integer_of(v, i)) {
                wrong(path, t, std::string("expected a whole number, got ") + kind_of(v));
            }
            typed("Integer", std::to_string(i), out);
            return;
        }
        case Kind::Real:
        case Kind::Double: {
            if (!v.is_number()) {
                wrong(path, t, std::string("expected a number, got ") + kind_of(v));
            }
            double d = v.as_number();
            if (v.is_integral_number()) {
                const std::int64_t i = v.as_int();
                // An integer a double cannot hold exactly would be rounded.
                if (d >= 9223372036854775808.0 || static_cast<std::int64_t>(d) != i) {
                    wrong(path, t, "an integer that a floating-point value cannot hold exactly");
                }
                d = static_cast<double>(i);
            }
            std::string text;
            append_float(d, text);
            typed("Float", text, out);
            return;
        }
        case Kind::Boolean:
            if (!v.is_bool()) {
                wrong(path, t, std::string("expected true or false, got ") + kind_of(v));
            }
            out += v.as_bool() ? "{\"$type\":\"Boolean\",\"_value\":true}"
                               : "{\"$type\":\"Boolean\",\"_value\":false}";
            return;
        case Kind::Varchar:
            encode_text(t, v, path, out);
            return;
        case Kind::Date: {
            std::string text;
            if (v.is_string() && !clink::config::is_dec_string(v)) {
                const auto& s = v.as_string();
                std::int64_t probe = 0;
                if (parse_int(s, probe)) {
                    if (!format_date(probe, text)) {
                        wrong(path, t, "the date is outside years 1 to 9999");
                    }
                } else {
                    // YYYY-MM-DD, checked by formatting it back.
                    int y = 0;
                    int m = 0;
                    int d = 0;
                    if (s.size() != 10 || s[4] != '-' || s[7] != '-' ||
                        std::from_chars(s.data(), s.data() + 4, y).ptr != s.data() + 4 ||
                        std::from_chars(s.data() + 5, s.data() + 7, m).ptr != s.data() + 7 ||
                        std::from_chars(s.data() + 8, s.data() + 10, d).ptr != s.data() + 10 ||
                        y < 1 || m < 1 || m > 12 || d < 1 || d > 31) {
                        wrong(path, t, "expected days since 1970-01-01 or YYYY-MM-DD");
                    }
                    text = s;
                }
            } else {
                std::int64_t days = 0;
                if (!integer_of(v, days)) {
                    wrong(
                        path, t, std::string("expected days since 1970-01-01, got ") + kind_of(v));
                }
                if (!format_date(days, text)) {
                    wrong(path, t, "the date is outside years 1 to 9999");
                }
            }
            typed("Date", text, out);
            return;
        }
        case Kind::Timestamp: {
            std::int64_t ms = 0;
            if (!integer_of(v, ms)) {
                wrong(path, t, std::string("expected epoch milliseconds, got ") + kind_of(v));
            }
            std::string text;
            if (!format_local_datetime(ms, text)) {
                wrong(path, t, "the timestamp is outside years 1 to 9999");
            }
            if (t.with_time_zone) {
                text += 'Z';
                typed("OffsetDateTime", text, out);
            } else {
                typed("LocalDateTime", text, out);
            }
            return;
        }
        case Kind::Array: {
            if (!v.is_array()) {
                wrong(path, t, std::string("expected an array, got ") + kind_of(v));
            }
            out += "{\"$type\":\"List\",\"_value\":[";
            const auto& arr = v.as_array();
            for (std::size_t i = 0; i < arr.size(); ++i) {
                if (i > 0) {
                    out += ',';
                }
                encode_value(t.children.front(), arr[i], path + "[" + std::to_string(i) + "]", out);
            }
            out += "]}";
            return;
        }
        case Kind::Map: {
            if (!v.is_object()) {
                wrong(path, t, std::string("expected an object, got ") + kind_of(v));
            }
            out += "{\"$type\":\"Map\",\"_value\":{";
            bool first = true;
            for (const auto& [key, value] : v.as_object()) {
                if (!valid_utf8(key)) {
                    wrong(path, t, "a key is not valid UTF-8");
                }
                if (!first) {
                    out += ',';
                }
                first = false;
                append_json_string(key, out);
                out += ':';
                encode_value(t.children[1], value, path + "." + key, out);
            }
            out += "}}";
            return;
        }
        case Kind::Row: {
            if (!v.is_object()) {
                wrong(path, t, std::string("expected an object, got ") + kind_of(v));
            }
            const auto& obj = v.as_object();
            out += "{\"$type\":\"Map\",\"_value\":{";
            for (std::size_t i = 0; i < t.children.size(); ++i) {
                if (i > 0) {
                    out += ',';
                }
                const auto& name = t.field_names[i];
                append_json_string(name, out);
                out += ':';
                const auto it = obj.find(name);
                static const JsonValue kNull;
                encode_value(
                    t.children[i], it == obj.end() ? kNull : it->second, path + "." + name, out);
            }
            out += "}}";
            return;
        }
        default:
            // Refused at open; never reached with a value.
            wrong(path, t, refusal(t));
    }
}

// Howard Hinnant's civil_from_days.
void civil_from_days(std::int64_t z, std::int64_t& y, unsigned& m, unsigned& d) {
    z += 719468;
    const std::int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    const auto doe = static_cast<unsigned>(z - era * 146097);
    const unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    y = static_cast<std::int64_t>(yoe) + era * 400;
    const unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const unsigned mp = (5 * doy + 2) / 153;
    d = doy - (153 * mp + 2) / 5 + 1;
    m = mp < 10 ? mp + 3 : mp - 9;
    y += m <= 2 ? 1 : 0;
}

void append_padded(std::int64_t v, int width, std::string& out) {
    std::string digits = std::to_string(v);
    while (static_cast<int>(digits.size()) < width) {
        digits.insert(digits.begin(), '0');
    }
    out += digits;
}

// 0001-01-01 and 9999-12-31 as days since 1970-01-01.
constexpr std::int64_t kMinDays = -719162;
constexpr std::int64_t kMaxDays = 2932896;

}  // namespace

std::vector<Column> parse_column_types(const std::string& spec) {
    std::vector<Column> out;
    if (spec.empty()) {
        return out;
    }
    for (const auto entry : split_top(spec, ';')) {
        const auto colon = entry.find(':');
        if (colon == std::string_view::npos || colon == 0) {
            throw Neo4jSinkError(
                code::kOptionInvalid,
                "neo4j sink: sql_column_types entry '" + std::string(entry) + "' is not name:TYPE");
        }
        out.push_back(
            Column{std::string(entry.substr(0, colon)), parse_type(entry.substr(colon + 1))});
    }
    return out;
}

std::vector<std::string> unsupported_columns(const std::vector<Column>& columns) {
    std::vector<std::string> out;
    for (const auto& c : columns) {
        if (auto why = refusal(c.type); !why.empty()) {
            out.push_back("column '" + c.name + "' (" + c.type.spelling + "): " + why);
        }
    }
    return out;
}

void encode_row(const std::vector<Column>& columns, const clink::sql::Row& row, std::string& out) {
    out += "{\"$type\":\"Map\",\"_value\":{";
    for (std::size_t i = 0; i < columns.size(); ++i) {
        if (i > 0) {
            out += ',';
        }
        const auto& c = columns[i];
        append_json_string(c.name, out);
        out += ':';
        const auto it = row.values.find(c.name);
        static const JsonValue kNull;
        encode_value(c.type, it == row.values.end() ? kNull : it->second, c.name, out);
    }
    out += "}}";
}

bool format_date(std::int64_t days, std::string& out) {
    if (days < kMinDays || days > kMaxDays) {
        return false;
    }
    std::int64_t y = 0;
    unsigned m = 0;
    unsigned d = 0;
    civil_from_days(days, y, m, d);
    append_padded(y, 4, out);
    out += '-';
    append_padded(m, 2, out);
    out += '-';
    append_padded(d, 2, out);
    return true;
}

bool format_local_datetime(std::int64_t epoch_ms, std::string& out) {
    std::int64_t days = epoch_ms / 86'400'000;
    std::int64_t rem = epoch_ms % 86'400'000;
    if (rem < 0) {
        rem += 86'400'000;
        --days;
    }
    if (!format_date(days, out)) {
        return false;
    }
    out += 'T';
    append_padded(rem / 3'600'000, 2, out);
    out += ':';
    append_padded((rem / 60'000) % 60, 2, out);
    out += ':';
    append_padded((rem / 1000) % 60, 2, out);
    out += '.';
    append_padded(rem % 1000, 3, out);
    return true;
}

bool valid_utf8(std::string_view s) noexcept {
    std::size_t i = 0;
    while (i < s.size()) {
        const auto c = static_cast<unsigned char>(s[i]);
        if (c < 0x80) {
            ++i;
            continue;
        }
        std::size_t len = 0;
        std::uint32_t cp = 0;
        if ((c & 0xE0) == 0xC0) {
            len = 2;
            cp = c & 0x1F;
        } else if ((c & 0xF0) == 0xE0) {
            len = 3;
            cp = c & 0x0F;
        } else if ((c & 0xF8) == 0xF0) {
            len = 4;
            cp = c & 0x07;
        } else {
            return false;
        }
        if (i + len > s.size()) {
            return false;
        }
        for (std::size_t k = 1; k < len; ++k) {
            const auto cc = static_cast<unsigned char>(s[i + k]);
            if ((cc & 0xC0) != 0x80) {
                return false;
            }
            cp = (cp << 6) | (cc & 0x3F);
        }
        const bool overlong =
            (len == 2 && cp < 0x80) || (len == 3 && cp < 0x800) || (len == 4 && cp < 0x10000);
        if (overlong || cp > 0x10FFFF || (cp >= 0xD800 && cp <= 0xDFFF)) {
            return false;
        }
        i += len;
    }
    return true;
}

void append_json_string(std::string_view s, std::string& out) {
    static constexpr char kHex[] = "0123456789abcdef";
    out += '"';
    for (const char ch : s) {
        const auto c = static_cast<unsigned char>(ch);
        switch (c) {
            case '"':
                out += "\\\"";
                break;
            case '\\':
                out += "\\\\";
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
            case '\b':
                out += "\\b";
                break;
            case '\f':
                out += "\\f";
                break;
            default:
                if (c < 0x20) {
                    out += "\\u00";
                    out += kHex[c >> 4];
                    out += kHex[c & 0x0F];
                } else {
                    out += ch;
                }
        }
    }
    out += '"';
}

}  // namespace clink::neo4j
