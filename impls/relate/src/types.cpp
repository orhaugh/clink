#include "types.hpp"

#include <array>
#include <charconv>
#include <cmath>
#include <string_view>
#include <utility>

#include "clink/config/json.hpp"

#include "errors.hpp"

namespace clink::relate {

namespace {

using clink::config::JsonValue;

std::string take(char* owned) {
    std::string out = owned != nullptr ? owned : "";
    relate_free_string(owned);
    return out;
}

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
            return "Relate has no decimal type; CAST the column to VARCHAR to keep every digit, "
                   "or to DOUBLE to store it rounded";
        case Kind::Time:
            return "TIME is not supported by this sink; CAST it to VARCHAR";
        case Kind::Bytea:
            return "BYTEA is not supported by this sink; CAST it to VARCHAR";
        case Kind::Unsupported:
            return "the type has no Relate mapping in this sink";
        case Kind::Array:
            return t.children.empty() ? "an ARRAY without an element type"
                                      : refusal(t.children.front());
        case Kind::Map:
            if (t.children.size() != 2) {
                return "a MAP without a key and value type";
            }
            if (t.children[0].kind != Kind::Varchar) {
                return "a Relate map is keyed by strings: declare MAP<VARCHAR, ...>";
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
    throw RelateError(code::kValueInvalid,
                      "relate sink: column '" + path + "' (" + t.spelling + "): " + why);
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

// Howard Hinnant's civil_from_days.
void civil_from_days(std::int64_t z, std::int64_t& y, std::int64_t& m, std::int64_t& d) {
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

// 0001-01-01 and 9999-12-31 as days since 1970-01-01.
constexpr std::int64_t kMinDays = -719162;
constexpr std::int64_t kMaxDays = 2932896;
constexpr std::int64_t kMsPerDay = 86'400'000;
// A name Relate's builder takes: a C string of at most 4096 bytes.
constexpr std::size_t kMaxNameBytes = 4096;

// Reports a putter's refusal as the column's, with Relate's reason.
void settled(relate_status status, char* err, const std::string& path, const SqlType& t) {
    if (status == RELATE_OK) {
        return;
    }
    wrong(path, t, "Relate refused the value: " + parse_engine_error(take(err)).message);
}

// Puts `v` as a value of type `t` under `name` in `into`; inside a list `name`
// is nullptr and the value is appended.
void put(const SqlType& t,
         const JsonValue& v,
         const std::string& path,
         relate_params into,
         const char* name) {
    char* err = nullptr;
    if (v.is_null()) {
        settled(relate_params_put_null(into, name, &err), err, path, t);
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
            settled(relate_params_put_int(into, name, i, &err), err, path, t);
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
            settled(relate_params_put_float(into, name, d, &err), err, path, t);
            return;
        }
        case Kind::Boolean:
            if (!v.is_bool()) {
                wrong(path, t, std::string("expected true or false, got ") + kind_of(v));
            }
            settled(relate_params_put_bool(into, name, v.as_bool() ? 1 : 0, &err), err, path, t);
            return;
        case Kind::Varchar: {
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
            settled(
                relate_params_put_string(into, name, text.data(), text.size(), &err), err, path, t);
            return;
        }
        case Kind::Date: {
            std::int64_t y = 0;
            std::int64_t m = 0;
            std::int64_t d = 0;
            std::int64_t days = 0;
            bool have_days = false;
            if (v.is_string() && !clink::config::is_dec_string(v)) {
                const auto& s = v.as_string();
                if (parse_int(s, days)) {
                    have_days = true;
                } else {
                    // YYYY-MM-DD; Relate checks that it is a real date.
                    int yy = 0;
                    int mm = 0;
                    int dd = 0;
                    if (s.size() != 10 || s[4] != '-' || s[7] != '-' ||
                        std::from_chars(s.data(), s.data() + 4, yy).ptr != s.data() + 4 ||
                        std::from_chars(s.data() + 5, s.data() + 7, mm).ptr != s.data() + 7 ||
                        std::from_chars(s.data() + 8, s.data() + 10, dd).ptr != s.data() + 10 ||
                        yy < 1) {
                        wrong(path, t, "expected days since 1970-01-01 or YYYY-MM-DD");
                    }
                    y = yy;
                    m = mm;
                    d = dd;
                }
            } else if (integer_of(v, days)) {
                have_days = true;
            } else {
                wrong(path, t, std::string("expected days since 1970-01-01, got ") + kind_of(v));
            }
            if (have_days) {
                if (days < kMinDays || days > kMaxDays) {
                    wrong(path, t, "the date is outside years 1 to 9999");
                }
                civil_from_days(days, y, m, d);
            }
            settled(relate_params_put_date(into, name, y, m, d, &err), err, path, t);
            return;
        }
        case Kind::Timestamp: {
            std::int64_t ms = 0;
            if (!integer_of(v, ms)) {
                wrong(path, t, std::string("expected epoch milliseconds, got ") + kind_of(v));
            }
            std::int64_t days = ms / kMsPerDay;
            std::int64_t rest = ms % kMsPerDay;
            if (rest < 0) {
                rest += kMsPerDay;
                --days;
            }
            if (days < kMinDays || days > kMaxDays) {
                wrong(path, t, "the timestamp is outside years 1 to 9999");
            }
            std::int64_t y = 0;
            std::int64_t m = 0;
            std::int64_t d = 0;
            civil_from_days(days, y, m, d);
            const std::int64_t hour = rest / 3'600'000;
            const std::int64_t minute = rest / 60'000 % 60;
            const std::int64_t second = rest / 1000 % 60;
            const std::int64_t nanos = rest % 1000 * 1'000'000;
            const auto status =
                t.with_time_zone
                    ? relate_params_put_datetime(
                          into, name, y, m, d, hour, minute, second, nanos, 0, nullptr, &err)
                    : relate_params_put_local_datetime(
                          into, name, y, m, d, hour, minute, second, nanos, &err);
            settled(status, err, path, t);
            return;
        }
        case Kind::Array: {
            if (!v.is_array()) {
                wrong(path, t, std::string("expected an array, got ") + kind_of(v));
            }
            relate_params list = nullptr;
            settled(relate_params_put_list(into, name, &list, &err), err, path, t);
            const auto& arr = v.as_array();
            for (std::size_t i = 0; i < arr.size(); ++i) {
                put(t.children.front(),
                    arr[i],
                    path + "[" + std::to_string(i) + "]",
                    list,
                    nullptr);
            }
            return;
        }
        case Kind::Map: {
            if (!v.is_object()) {
                wrong(path, t, std::string("expected an object, got ") + kind_of(v));
            }
            relate_params map = nullptr;
            settled(relate_params_put_map(into, name, &map, &err), err, path, t);
            for (const auto& [key, value] : v.as_object()) {
                const std::string k(key);
                if (k.size() > kMaxNameBytes || k.find('\0') != std::string::npos) {
                    wrong(path,
                          t,
                          "a key longer than 4096 bytes or holding a NUL, which Relate cannot "
                          "take as a name");
                }
                put(t.children[1], value, path + "." + k, map, k.c_str());
            }
            return;
        }
        case Kind::Row: {
            if (!v.is_object()) {
                wrong(path, t, std::string("expected an object, got ") + kind_of(v));
            }
            relate_params map = nullptr;
            settled(relate_params_put_map(into, name, &map, &err), err, path, t);
            const auto& obj = v.as_object();
            static const JsonValue kNull;
            for (std::size_t i = 0; i < t.children.size(); ++i) {
                const auto& field = t.field_names[i];
                const auto it = obj.find(field);
                put(t.children[i],
                    it == obj.end() ? kNull : it->second,
                    path + "." + field,
                    map,
                    field.c_str());
            }
            return;
        }
        default:
            // Refused at open; never reached with a value.
            wrong(path, t, refusal(t));
    }
}

constexpr std::uint64_t kEntryBytes = 16;

std::uint64_t weigh(const JsonValue& v) {
    if (v.is_string()) {
        return kEntryBytes + v.as_string().size();
    }
    if (v.is_array()) {
        std::uint64_t sum = kEntryBytes;
        for (const auto& item : v.as_array()) {
            sum += weigh(item);
        }
        return sum;
    }
    if (v.is_object()) {
        std::uint64_t sum = kEntryBytes;
        for (const auto& [key, value] : v.as_object()) {
            sum += std::string_view(key).size() + weigh(value);
        }
        return sum;
    }
    return kEntryBytes + 8;
}

}  // namespace

std::vector<Column> parse_column_types(const std::string& spec) {
    std::vector<Column> out;
    if (spec.empty()) {
        return out;
    }
    for (const auto entry : split_top(spec, ';')) {
        const auto colon = entry.find(':');
        if (colon == std::string_view::npos || colon == 0) {
            throw RelateError(code::kOptionInvalid,
                              "relate sink: sql_column_types entry '" + std::string(entry) +
                                  "' is not name:TYPE");
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

std::uint64_t row_weight(const std::vector<Column>& columns, const clink::sql::Row& row) {
    std::uint64_t sum = kEntryBytes;
    for (const auto& c : columns) {
        const auto it = row.values.find(c.name);
        sum += c.name.size() + (it == row.values.end() ? kEntryBytes + 8 : weigh(it->second));
    }
    return sum;
}

// ---- RowsParam ---------------------------------------------------------------

RowsParam::RowsParam() {
    reset_();
}

RowsParam::~RowsParam() {
    relate_params_free(root_);
}

void RowsParam::reset_() {
    relate_params_free(root_);
    root_ = relate_params_new();
    list_ = nullptr;
    rows_ = 0;
    bytes_ = 0;
    char* err = nullptr;
    if (root_ == nullptr || relate_params_put_list(root_, "rows", &list_, &err) != RELATE_OK) {
        throw RelateError(code::kEngineFailure,
                          "relate sink: Relate's parameter builder could not be started: " +
                              parse_engine_error(take(err)).message);
    }
}

void RowsParam::clear() {
    reset_();
}

void RowsParam::add(const std::vector<Column>& columns,
                    const clink::sql::Row& row,
                    std::uint64_t weight) {
    relate_params map = nullptr;
    char* err = nullptr;
    if (relate_params_put_map(list_, nullptr, &map, &err) != RELATE_OK) {
        throw RelateError(code::kRowTooLarge,
                          "relate sink: Relate's parameter builder took no more rows after " +
                              std::to_string(rows_) + ": " + parse_engine_error(take(err)).message +
                              "; lower batch_size or batch_bytes");
    }
    static const JsonValue kNull;
    for (const auto& c : columns) {
        const auto it = row.values.find(c.name);
        put(c.type, it == row.values.end() ? kNull : it->second, c.name, map, c.name.c_str());
    }
    ++rows_;
    bytes_ += weight;
}

}  // namespace clink::relate
