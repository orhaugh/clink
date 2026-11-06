// The typed SQL row of a MongoDB change event; the contract is in sql_row.hpp.

#include "sql_row.hpp"

#include <cctype>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <limits>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace clink::mongodb::sql {

namespace {

using clink::config::JsonValue;

std::string upper(std::string_view s) {
    std::string out(s);
    for (auto& c : out) {
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }
    return out;
}

void append_json_string(std::string& out, std::string_view s) {
    out += '"';
    for (const char c : s) {
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
                if (static_cast<unsigned char>(c) < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned char>(c));
                    out += buf;
                } else {
                    out += c;
                }
        }
    }
    out += '"';
}

std::string json_string(std::string_view s) {
    std::string out;
    append_json_string(out, s);
    return out;
}

// A value of a canonical Extended JSON document, by its BSON type.
enum class Bson {
    Missing,
    Null,
    Bool,
    String,
    ObjectId,
    Int32,
    Int64,
    Double,
    Decimal,
    Date,
    Document,
    Array,
    Other,
};

struct Value {
    Bson type{Bson::Missing};
    std::string text;   // the string, the hex digits, the numeral, the milliseconds
    std::string other;  // an Other's BSON type, for messages
    const JsonValue* raw{nullptr};
};

std::string describe(const Value& v) {
    switch (v.type) {
        case Bson::Missing:
            return "missing";
        case Bson::Null:
            return "null";
        case Bson::Bool:
            return "a boolean";
        case Bson::String:
            return "a string";
        case Bson::ObjectId:
            return "an ObjectId";
        case Bson::Int32:
            return "a 32-bit integer";
        case Bson::Int64:
            return "a 64-bit integer";
        case Bson::Double:
            return "a double";
        case Bson::Decimal:
            return "a decimal";
        case Bson::Date:
            return "a date";
        case Bson::Document:
            return "an embedded document";
        case Bson::Array:
            return "an array";
        case Bson::Other:
            return v.other;
    }
    return "a value";
}

// The SQL type that holds a BSON type, for a refusal's advice.
std::string suggestion(const Value& v) {
    switch (v.type) {
        case Bson::String:
        case Bson::ObjectId:
        case Bson::Document:
        case Bson::Array:
            return "declare it VARCHAR";
        case Bson::Int32:
        case Bson::Int64:
            return "declare it BIGINT";
        case Bson::Double:
            return "declare it DOUBLE";
        case Bson::Decimal:
            return "declare it DECIMAL with room for its digits";
        case Bson::Bool:
            return "declare it BOOLEAN";
        case Bson::Date:
            return "declare it TIMESTAMP";
        default:
            return "no SQL type holds it; leave the field undeclared";
    }
}

Value classify(const JsonValue* v) {
    Value out;
    out.raw = v;
    if (v == nullptr) {
        out.type = Bson::Missing;
        return out;
    }
    if (v->is_null()) {
        out.type = Bson::Null;
    } else if (v->is_bool()) {
        out.type = Bson::Bool;
        out.text = v->as_bool() ? "true" : "false";
    } else if (v->is_string()) {
        out.type = Bson::String;
        out.text = v->as_string();
    } else if (v->is_array()) {
        out.type = Bson::Array;
    } else if (v->is_number()) {
        // Canonical Extended JSON wraps every number; a bare one is read as the
        // type its JSON form names.
        out.type = v->is_integral_number() ? Bson::Int64 : Bson::Double;
        out.text = v->serialize(0);
    } else if (v->is_object()) {
        const auto& o = v->as_object();
        const auto wrapped = [&](const char* key) -> const JsonValue* {
            if (o.size() != 1) {
                return nullptr;
            }
            const auto it = o.find(key);
            return it == o.end() ? nullptr : &it->second;
        };
        if (const auto* s = wrapped("$oid"); s != nullptr && s->is_string()) {
            out.type = Bson::ObjectId;
            out.text = s->as_string();
        } else if (const auto* i = wrapped("$numberInt"); i != nullptr && i->is_string()) {
            out.type = Bson::Int32;
            out.text = i->as_string();
        } else if (const auto* l = wrapped("$numberLong"); l != nullptr && l->is_string()) {
            out.type = Bson::Int64;
            out.text = l->as_string();
        } else if (const auto* d = wrapped("$numberDouble"); d != nullptr && d->is_string()) {
            out.type = Bson::Double;
            out.text = d->as_string();
        } else if (const auto* m = wrapped("$numberDecimal"); m != nullptr && m->is_string()) {
            out.type = Bson::Decimal;
            out.text = m->as_string();
        } else if (const auto* dt = wrapped("$date"); dt != nullptr) {
            out.type = Bson::Date;
            if (dt->is_object()) {
                const auto& inner = dt->as_object();
                if (const auto it = inner.find("$numberLong");
                    it != inner.end() && it->second.is_string()) {
                    out.text = it->second.as_string();
                }
            }
            if (out.text.empty()) {
                out.type = Bson::Other;
                out.other = "a date in a form the source cannot read";
            }
        } else {
            bool all_operators = !o.empty();
            for (const auto& [k, _] : o) {
                all_operators = all_operators && !k.empty() && k.front() == '$';
            }
            if (all_operators) {
                out.type = Bson::Other;
                const auto first = o.begin()->first;
                if (first == "$binary") {
                    out.other = "binary data";
                } else if (first == "$timestamp") {
                    out.other = "a BSON timestamp";
                } else if (first == "$regularExpression") {
                    out.other = "a regular expression";
                } else if (first == "$code") {
                    out.other = "JavaScript code";
                } else if (first == "$minKey" || first == "$maxKey") {
                    out.other = "a " + first.substr(1);
                } else {
                    out.other = "a " + first + " value";
                }
            } else {
                out.type = Bson::Document;
            }
        }
    }
    return out;
}

std::optional<std::int64_t> as_int64(std::string_view text) {
    std::int64_t v = 0;
    const auto* end = text.data() + text.size();
    const auto [ptr, ec] = std::from_chars(text.data(), end, v);
    if (ec != std::errc{} || ptr != end) {
        return std::nullopt;
    }
    return v;
}

// A finite double from its text, or nullopt.
std::optional<double> as_finite_double(const std::string& text) {
    if (text == "NaN" || text == "Infinity" || text == "-Infinity") {
        return std::nullopt;
    }
    char* end = nullptr;
    const double d = std::strtod(text.c_str(), &end);
    if (end == text.c_str() || *end != '\0' || !std::isfinite(d)) {
        return std::nullopt;
    }
    return d;
}

// A double's shortest round-trip text, an integral one with a ".0" so it reads
// back as a double rather than an integer.
std::string double_token(double d) {
    std::string out;
    JsonValue{d}.serialize_kind_preserving_into(out);
    return out;
}

// A decimal numeral as plain digits: sign, integer part without leading zeros
// (one "0" when there are none), and the fraction as written, its trailing
// zeros kept. "1.50E+2" is "150"; "1E-3" is "0.001". nullopt on a malformed one.
struct PlainDecimal {
    std::string text;
    std::size_t int_digits{0};   // significant integer digits
    std::size_t frac_digits{0};  // digits after the point
};

std::optional<PlainDecimal> plain_decimal(std::string_view text) {
    std::size_t i = 0;
    bool negative = false;
    if (i < text.size() && (text[i] == '-' || text[i] == '+')) {
        negative = text[i] == '-';
        ++i;
    }
    std::string digits;
    std::size_t point = std::string::npos;
    for (; i < text.size() && text[i] != 'E' && text[i] != 'e'; ++i) {
        if (text[i] == '.') {
            if (point != std::string::npos) {
                return std::nullopt;
            }
            point = digits.size();
        } else if (std::isdigit(static_cast<unsigned char>(text[i])) != 0) {
            digits += text[i];
        } else {
            return std::nullopt;
        }
    }
    if (digits.empty()) {
        return std::nullopt;
    }
    long exponent = 0;
    if (i < text.size()) {
        const auto e = text.substr(i + 1);
        const auto parsed = as_int64(e.front() == '+' ? e.substr(1) : e);
        if (!parsed || *parsed > 10000 || *parsed < -10000) {
            return std::nullopt;
        }
        exponent = static_cast<long>(*parsed);
    }
    // The point's place in `digits`, after the exponent moves it.
    long place = static_cast<long>(point == std::string::npos ? digits.size() : point) + exponent;
    if (place < 0) {
        digits.insert(0, static_cast<std::size_t>(-place), '0');
        place = 0;
    } else if (place > static_cast<long>(digits.size())) {
        digits.append(static_cast<std::size_t>(place) - digits.size(), '0');
    }
    std::string int_part = digits.substr(0, static_cast<std::size_t>(place));
    const std::string frac_part = digits.substr(static_cast<std::size_t>(place));
    const auto nonzero = int_part.find_first_not_of('0');
    int_part = nonzero == std::string::npos ? "" : int_part.substr(nonzero);
    PlainDecimal out;
    out.int_digits = int_part.size();
    out.frac_digits = frac_part.size();
    const bool zero = int_part.empty() && frac_part.find_first_not_of('0') == std::string::npos;
    out.text = (negative && !zero ? "-" : "") + (int_part.empty() ? "0" : int_part) +
               (frac_part.empty() ? "" : "." + frac_part);
    return out;
}

// Days since 1970-01-01 to a civil date (Howard Hinnant's algorithm).
std::string civil_date(std::int64_t days) {
    days += 719468;
    const std::int64_t era = (days >= 0 ? days : days - 146096) / 146097;
    const auto doe = static_cast<unsigned>(days - era * 146097);
    const unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    const std::int64_t y = static_cast<std::int64_t>(yoe) + era * 400;
    const unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const unsigned mp = (5 * doy + 2) / 153;
    const unsigned d = doy - (153 * mp + 2) / 5 + 1;
    const unsigned m = mp < 10 ? mp + 3 : mp - 9;
    char buf[32];
    std::snprintf(
        buf, sizeof(buf), "%04lld-%02u-%02u", static_cast<long long>(y + (m <= 2 ? 1 : 0)), m, d);
    return buf;
}

// Where a value came from, for a refusal.
struct Origin {
    const std::string& ns;
    const std::string& id;
    std::string field;  // "_id" for __id
};

[[noreturn]] void refuse(const Origin& at,
                         const Column& col,
                         const Value& v,
                         const std::string& why,
                         const std::string& advice) {
    throw ConversionError("change on " + at.ns + " to document " + at.id + ": field '" + at.field +
                          "' is " + describe(v) + ", which column " + col.name + " (" +
                          col.sql_type + ") " + why + "; " + advice);
}

std::string integer_in(
    const Origin& at, const Column& col, const Value& v, std::int64_t lo, std::int64_t hi) {
    const auto n = as_int64(v.text);
    if (!n) {
        refuse(at, col, v, "cannot read", "the value is malformed");
    }
    if (*n < lo || *n > hi) {
        refuse(at, col, v, "cannot hold: " + v.text + " is outside its range", "declare it BIGINT");
    }
    return std::to_string(*n);
}

std::string convert(const Origin& at, const Column& col, const Value& v) {
    if (v.type == Bson::Missing || v.type == Bson::Null) {
        return "null";
    }
    const bool integer = v.type == Bson::Int32 || v.type == Bson::Int64;
    switch (col.kind) {
        case Kind::BigInt:
            if (integer) {
                return integer_in(at,
                                  col,
                                  v,
                                  std::numeric_limits<std::int64_t>::min(),
                                  std::numeric_limits<std::int64_t>::max());
            }
            break;
        case Kind::Integer:
            if (integer) {
                return integer_in(at,
                                  col,
                                  v,
                                  std::numeric_limits<std::int32_t>::min(),
                                  std::numeric_limits<std::int32_t>::max());
            }
            break;
        case Kind::SmallInt:
            if (integer) {
                return integer_in(at, col, v, -32768, 32767);
            }
            break;
        case Kind::Double:
            if (v.type == Bson::Double) {
                const auto d = as_finite_double(v.text);
                if (!d) {
                    refuse(at, col, v, "cannot hold: " + v.text + " is not finite", "leave it out");
                }
                return double_token(*d);
            }
            if (integer) {
                return double_token(static_cast<double>(std::stoll(
                    integer_in(at, col, v, -(std::int64_t{1} << 53), std::int64_t{1} << 53))));
            }
            break;
        case Kind::Real:
            if (v.type == Bson::Double) {
                const auto d = as_finite_double(v.text);
                if (!d || static_cast<double>(static_cast<float>(*d)) != *d) {
                    refuse(at,
                           col,
                           v,
                           "cannot hold exactly: " + v.text + " is not a float",
                           "declare it DOUBLE");
                }
                return double_token(*d);
            }
            if (integer) {
                return double_token(static_cast<double>(std::stoll(
                    integer_in(at, col, v, -(std::int64_t{1} << 24), std::int64_t{1} << 24))));
            }
            break;
        case Kind::Decimal: {
            if (v.type != Bson::Decimal && !integer) {
                break;
            }
            if (v.text == "NaN" || v.text == "Infinity" || v.text == "-Infinity") {
                refuse(at, col, v, "cannot hold: " + v.text + " is not finite", "leave it out");
            }
            const auto plain = plain_decimal(v.text);
            if (!plain) {
                refuse(at, col, v, "cannot read: " + v.text, "the value is malformed");
            }
            const auto int_room = static_cast<std::size_t>(col.precision - col.scale);
            if (plain->int_digits > int_room ||
                plain->frac_digits > static_cast<std::size_t>(col.scale)) {
                refuse(at,
                       col,
                       v,
                       "cannot hold without rounding: " + v.text,
                       "widen the declared precision or scale");
            }
            return plain->text;
        }
        case Kind::Boolean:
            if (v.type == Bson::Bool) {
                return v.text;
            }
            break;
        case Kind::Timestamp:
            if (v.type == Bson::Date) {
                const auto ms = as_int64(v.text);
                if (!ms) {
                    refuse(at, col, v, "cannot read", "the value is malformed");
                }
                return std::to_string(*ms);
            }
            break;
        case Kind::Date:
            if (v.type == Bson::Date) {
                const auto ms = as_int64(v.text);
                if (!ms) {
                    refuse(at, col, v, "cannot read", "the value is malformed");
                }
                constexpr std::int64_t kDay = 86400000;
                if (*ms % kDay != 0) {
                    refuse(at, col, v, "cannot hold: it has a time of day", "declare it TIMESTAMP");
                }
                return json_string(civil_date(*ms / kDay));
            }
            break;
        case Kind::Varchar:
            switch (v.type) {
                case Bson::String:
                case Bson::ObjectId:
                    return json_string(v.text);
                case Bson::Int32:
                case Bson::Int64:
                    return json_string(integer_in(at,
                                                  col,
                                                  v,
                                                  std::numeric_limits<std::int64_t>::min(),
                                                  std::numeric_limits<std::int64_t>::max()));
                case Bson::Document:
                case Bson::Array:
                    return json_string(v.raw->serialize(0));
                default:
                    break;
            }
            break;
        case Kind::MetaOp:
        case Kind::MetaNs:
        case Kind::MetaImage:
            break;
    }
    refuse(at, col, v, "cannot hold", suggestion(v));
}

std::string describe_id(const JsonValue& id) {
    const auto v = classify(&id);
    switch (v.type) {
        case Bson::ObjectId:
            return "ObjectId(" + v.text + ")";
        case Bson::Int32:
        case Bson::Int64:
            return v.text;
        case Bson::String:
            return json_string(v.text);
        case Bson::Null:
        case Bson::Missing:
            return "(no _id)";
        default:
            return id.serialize(0);
    }
}

}  // namespace

namespace {

// The data type of `col` (name and sql_type set) from its SQL type.
void set_data_kind(Column& col) {
    const std::string t = upper(col.sql_type);
    if (t == "BIGINT") {
        col.kind = Kind::BigInt;
    } else if (t == "INTEGER") {
        col.kind = Kind::Integer;
    } else if (t == "SMALLINT") {
        col.kind = Kind::SmallInt;
    } else if (t == "DOUBLE") {
        col.kind = Kind::Double;
    } else if (t == "REAL") {
        col.kind = Kind::Real;
    } else if (t == "BOOLEAN") {
        col.kind = Kind::Boolean;
    } else if (t == "DATE") {
        col.kind = Kind::Date;
    } else if (t == "VARCHAR") {
        col.kind = Kind::Varchar;
    } else if (t.rfind("TIMESTAMP(", 0) == 0) {
        col.kind = Kind::Timestamp;
    } else if (t.rfind("DECIMAL(", 0) == 0) {
        int p = 0;
        int s = 0;
        if (std::sscanf(t.c_str(), "DECIMAL(%d, %d)", &p, &s) != 2 &&
            std::sscanf(t.c_str(), "DECIMAL(%d,%d)", &p, &s) != 2) {
            throw ConversionError("column " + col.name + ": cannot read the type " + col.sql_type);
        }
        col.kind = Kind::Decimal;
        col.precision = p;
        col.scale = s;
    } else {
        throw ConversionError("column " + col.name + ": SQL type " + col.sql_type +
                              " is not one the MongoDB connector maps to BSON. Supported: BIGINT, "
                              "INTEGER, SMALLINT, DOUBLE, REAL, DECIMAL(p, s), BOOLEAN, DATE, "
                              "TIMESTAMP, TIMESTAMP WITH TIME ZONE and VARCHAR");
    }
}

// The entries of "name:TYPE;name:TYPE", name and sql_type set.
std::vector<Column> split_declared(std::string_view text) {
    std::vector<Column> out;
    std::size_t start = 0;
    while (start <= text.size()) {
        const auto end = text.find(';', start);
        const auto entry = text.substr(
            start, end == std::string_view::npos ? std::string_view::npos : end - start);
        start = end == std::string_view::npos ? text.size() + 1 : end + 1;
        if (entry.empty()) {
            continue;
        }
        const auto colon = entry.find(':');
        if (colon == std::string_view::npos || colon == 0) {
            throw ConversionError("cannot read the declared column '" + std::string(entry) + "'");
        }
        Column col;
        col.name = std::string(entry.substr(0, colon));
        col.sql_type = std::string(entry.substr(colon + 1));
        out.push_back(std::move(col));
    }
    return out;
}

}  // namespace

std::vector<Column> parse_declared_types(std::string_view text) {
    std::vector<Column> out;
    for (auto& col : split_declared(text)) {
        if (col.name.rfind("__", 0) == 0) {
            if (col.name == "__op" || col.name == "__ns" || col.name == "__image") {
                if (upper(col.sql_type) != "VARCHAR") {
                    throw ConversionError("metadata column " + col.name +
                                          " must be declared VARCHAR (declared " + col.sql_type +
                                          ")");
                }
                col.kind = col.name == "__op"   ? Kind::MetaOp
                           : col.name == "__ns" ? Kind::MetaNs
                                                : Kind::MetaImage;
                out.push_back(std::move(col));
                continue;
            }
            if (col.name != "__id") {
                throw ConversionError("column " + col.name +
                                      ": names starting with __ are reserved for change metadata "
                                      "(__op, __ns, __id, __image)");
            }
            col.is_id = true;
        }
        set_data_kind(col);
        out.push_back(std::move(col));
    }
    return out;
}

std::string change_event_to_sql_row(const clink::config::JsonValue& canonical_event,
                                    FullDocument mode,
                                    const std::vector<Column>& columns) {
    const auto parts = decompose_change(canonical_event, mode);
    const std::string id = describe_id(parts.id);
    std::string out = "{";
    bool first = true;
    for (const auto& col : columns) {
        if (!first) {
            out += ',';
        }
        first = false;
        append_json_string(out, col.name);
        out += ':';
        switch (col.kind) {
            case Kind::MetaOp:
                append_json_string(out, parts.op);
                continue;
            case Kind::MetaNs:
                append_json_string(out, parts.ns);
                continue;
            case Kind::MetaImage:
                append_json_string(out, parts.image);
                continue;
            default:
                break;
        }
        if (col.is_id) {
            out += convert(Origin{parts.ns, id, "_id"}, col, classify(&parts.id));
            continue;
        }
        // _id cannot change, so where the image does not carry it (an update
        // read without a document) the key does.
        const auto it = parts.body.find(col.name);
        const clink::config::JsonValue* value = it == parts.body.end() ? nullptr : &it->second;
        if (value == nullptr && col.name == "_id" && !parts.id.is_null()) {
            value = &parts.id;
        }
        out += convert(Origin{parts.ns, id, col.name}, col, classify(value));
    }
    out += '}';
    return out;
}

namespace {

// Days since 1970-01-01 of a civil date (Howard Hinnant's algorithm).
std::int64_t days_from_civil(std::int64_t y, unsigned m, unsigned d) {
    y -= m <= 2 ? 1 : 0;
    const std::int64_t era = (y >= 0 ? y : y - 399) / 400;
    const auto yoe = static_cast<unsigned>(y - era * 400);
    const unsigned doy = (153 * (m > 2 ? m - 3 : m + 9) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + static_cast<std::int64_t>(doe) - 719468;
}

// 'YYYY-MM-DD' as days since the epoch, or nullopt for anything else,
// 2023-02-30 included.
std::optional<std::int64_t> date_days(const std::string& text) {
    int y = 0;
    unsigned m = 0;
    unsigned d = 0;
    char extra = 0;
    if (text.size() != 10 || std::sscanf(text.c_str(), "%4d-%2u-%2u%c", &y, &m, &d, &extra) != 3 ||
        m < 1 || m > 12 || d < 1 || d > 31) {
        return std::nullopt;
    }
    const auto days = days_from_civil(y, m, d);
    if (civil_date(days) != text) {
        return std::nullopt;
    }
    return days;
}

std::string wrapped(const char* type, const std::string& text) {
    return std::string("{\"") + type + "\":" + json_string(text) + "}";
}

std::string date_value(std::int64_t ms) {
    return R"({"$date":{"$numberLong":)" + json_string(std::to_string(ms)) + "}}";
}

[[noreturn]] void refuse_sink(const Column& col, const JsonValue& v, const std::string& why) {
    throw ConversionError("column " + col.name + " (" + col.sql_type + ") holds " + v.serialize(0) +
                          ", which " + why);
}

// A double's canonical Extended JSON text: shortest round trip, the
// non-finite values by name.
std::string double_text(double d) {
    if (std::isnan(d)) {
        return "NaN";
    }
    if (std::isinf(d)) {
        return d > 0 ? "Infinity" : "-Infinity";
    }
    std::string out;
    JsonValue{d}.serialize_kind_preserving_into(out);
    return out;
}

std::string to_bson_value(const Column& col, const JsonValue& v, const std::string* raw_number) {
    if (v.is_null()) {
        return "null";
    }
    switch (col.kind) {
        case Kind::BigInt:
            if (v.is_integral_number()) {
                return wrapped("$numberLong", std::to_string(v.as_int()));
            }
            refuse_sink(col, v, "is not an integer");
        case Kind::Integer:
        case Kind::SmallInt:
            if (v.is_integral_number() && v.as_int() >= std::numeric_limits<std::int32_t>::min() &&
                v.as_int() <= std::numeric_limits<std::int32_t>::max()) {
                return wrapped("$numberInt", std::to_string(v.as_int()));
            }
            refuse_sink(col, v, "is not a 32-bit integer");
        case Kind::Double:
        case Kind::Real:
            if (v.is_number()) {
                return wrapped("$numberDouble",
                               double_text(v.is_integral_number() ? static_cast<double>(v.as_int())
                                                                  : v.as_number()));
            }
            refuse_sink(col, v, "is not a number");
        case Kind::Decimal:
            if (raw_number != nullptr) {
                return wrapped("$numberDecimal", *raw_number);
            }
            refuse_sink(col, v, "is not a decimal numeral");
        case Kind::Boolean:
            if (v.is_bool()) {
                return v.as_bool() ? "true" : "false";
            }
            refuse_sink(col, v, "is not a boolean");
        case Kind::Timestamp:
            if (v.is_integral_number()) {
                return date_value(v.as_int());
            }
            refuse_sink(col, v, "is not epoch milliseconds");
        case Kind::Date:
            if (v.is_string()) {
                if (const auto days = date_days(v.as_string())) {
                    return date_value(*days * 86400000);
                }
            }
            refuse_sink(col, v, "is not a date in the form YYYY-MM-DD");
        case Kind::Varchar:
            if (v.is_string()) {
                return json_string(v.as_string());
            }
            refuse_sink(col, v, "is not a string");
        case Kind::MetaOp:
        case Kind::MetaNs:
        case Kind::MetaImage:
            break;
    }
    refuse_sink(col, v, "has no BSON form here");
}

}  // namespace

std::vector<Column> parse_sink_columns(std::string_view text) {
    auto out = split_declared(text);
    for (auto& col : out) {
        if (col.name == "__row_kind") {
            throw ConversionError("column __row_kind: the name is reserved for the change kind");
        }
        set_data_kind(col);
    }
    return out;
}

SinkDocument sql_row_to_document(std::string_view row_json, const std::vector<Column>& columns) {
    const auto parsed = clink::config::parse(row_json);
    if (!parsed.is_object()) {
        throw ConversionError("a row that is not a JSON object");
    }
    const auto& row = parsed.as_object();
    SinkDocument out;
    out.kind = "insert";
    if (const auto it = row.find("__row_kind"); it != row.end()) {
        if (!it->second.is_string()) {
            throw ConversionError("a row whose __row_kind is not a string");
        }
        out.kind = it->second.as_string();
    }
    // The exact numerals of the decimal columns: the parse above reads a number
    // as a double, which would round digits past the 16th.
    std::map<std::string, int> decimals;
    for (const auto& col : columns) {
        if (col.kind == Kind::Decimal) {
            decimals.emplace(col.name, col.scale);
        }
    }
    const auto raw = decimals.empty() ? std::map<std::string, std::string>{}
                                      : clink::config::raw_number_tokens(row_json, decimals);
    out.json = "{";
    bool first = true;
    for (const auto& col : columns) {
        if (!first) {
            out.json += ',';
        }
        first = false;
        append_json_string(out.json, col.name);
        out.json += ':';
        const auto it = row.find(col.name);
        if (it == row.end()) {
            out.json += "null";
            continue;
        }
        const auto r = raw.find(col.name);
        out.json += to_bson_value(col, it->second, r == raw.end() ? nullptr : &r->second);
    }
    out.json += '}';
    return out;
}

}  // namespace clink::mongodb::sql
