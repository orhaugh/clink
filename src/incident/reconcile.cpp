#include "clink/incident/reconcile.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <charconv>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <fstream>
#include <map>
#include <set>
#include <sstream>
#include <thread>

#include "clink/cluster/state_location.hpp"
#include "clink/config/decimal.hpp"
#include "clink/incident/reference.hpp"
#include "clink/runtime/record_capture.hpp"
#include "clink/runtime/sink_evidence.hpp"
#include "clink/state/in_memory_state_backend.hpp"

namespace clink::incident::reconcile {

namespace fs = std::filesystem;

namespace {

std::string trim(std::string_view s) {
    while (!s.empty() && (s.front() == ' ' || s.front() == '\t')) {
        s.remove_prefix(1);
    }
    while (!s.empty() && (s.back() == ' ' || s.back() == '\t')) {
        s.remove_suffix(1);
    }
    return std::string{s};
}

// "Name(args)" -> args, when `s` is exactly that.
std::optional<std::string> unwrap(std::string_view s, std::string_view name) {
    if (s.size() < name.size() + 2 || s.substr(0, name.size()) != name || s[name.size()] != '(' ||
        s.back() != ')') {
        return std::nullopt;
    }
    return std::string{s.substr(name.size() + 1, s.size() - name.size() - 2)};
}

// Top-level comma-separated arguments.
std::vector<std::string> split_args(std::string_view s) {
    std::vector<std::string> out;
    int depth = 0;
    bool quoted = false;
    std::string cur;
    for (std::size_t i = 0; i < s.size(); ++i) {
        const char c = s[i];
        if (quoted) {
            cur += c;
            if (c == '\\' && i + 1 < s.size()) {
                cur += s[++i];
            } else if (c == '\'') {
                quoted = false;
            }
            continue;
        }
        if (c == '\'') {
            quoted = true;
        } else if (c == '(') {
            ++depth;
        } else if (c == ')') {
            --depth;
        } else if (c == ',' && depth == 0) {
            out.push_back(trim(cur));
            cur.clear();
            continue;
        }
        cur += c;
    }
    if (!trim(cur).empty()) {
        out.push_back(trim(cur));
    }
    return out;
}

std::optional<int> small_int(std::string_view s) {
    int v = 0;
    const auto r = std::from_chars(s.data(), s.data() + s.size(), v);
    if (r.ec != std::errc{} || r.ptr != s.data() + s.size() || v < 0) {
        return std::nullopt;
    }
    return v;
}

bool all_digits(std::string_view s) {
    return !s.empty() &&
           std::all_of(s.begin(), s.end(), [](char c) { return c >= '0' && c <= '9'; });
}

std::string strip_zeros(std::string_view digits) {
    std::size_t i = 0;
    while (i + 1 < digits.size() && digits[i] == '0') {
        ++i;
    }
    return std::string{digits.substr(i)};
}

// Howard Hinnant's civil-date algorithms.
std::int64_t days_from_civil(std::int64_t y, unsigned m, unsigned d) {
    y -= m <= 2 ? 1 : 0;
    const std::int64_t era = (y >= 0 ? y : y - 399) / 400;
    const auto yoe = static_cast<unsigned>(y - era * 400);
    const unsigned doy = ((153 * (m > 2 ? m - 3 : m + 9)) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + static_cast<std::int64_t>(doe) - 719468;
}

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

std::string date_text(std::int64_t days) {
    std::int64_t y = 0;
    unsigned m = 0;
    unsigned d = 0;
    civil_from_days(days, y, m, d);
    std::array<char, 32> buf{};
    std::snprintf(buf.data(), buf.size(), "%04lld-%02u-%02u", static_cast<long long>(y), m, d);
    return buf.data();
}

std::optional<std::int64_t> pow10(int n) {
    std::int64_t v = 1;
    for (int i = 0; i < n; ++i) {
        if (v > std::numeric_limits<std::int64_t>::max() / 10) {
            return std::nullopt;
        }
        v *= 10;
    }
    return v;
}

std::string bad(std::string why) {
    return "!" + std::move(why);
}

// A JSON value as integer text: an integer, a whole double, or numeric text.
std::optional<std::string> integer_text(const config::JsonValue& v) {
    if (v.is_integral_number()) {
        return std::to_string(v.as_int());
    }
    if (v.is_number()) {
        const double d = v.as_number();
        if (std::isfinite(d) && std::trunc(d) == d && std::fabs(d) < 9.2e18) {
            return std::to_string(static_cast<std::int64_t>(d));
        }
        return std::nullopt;
    }
    if (v.is_string()) {
        std::string s = v.as_string();
        if (config::is_dec_string(v)) {
            s = s.substr(1);
        }
        return s;
    }
    return std::nullopt;
}

std::optional<std::int64_t> as_i64(std::string_view text) {
    std::int64_t v = 0;
    const auto r = std::from_chars(text.data(), text.data() + text.size(), v);
    if (r.ec != std::errc{} || r.ptr != text.data() + text.size()) {
        return std::nullopt;
    }
    return v;
}

std::string shortest(double d) {
    std::array<char, 64> buf{};
    const auto r = std::to_chars(buf.data(), buf.data() + buf.size(), d);
    return std::string{buf.data(), r.ptr};
}

std::string shortest(float f) {
    std::array<char, 64> buf{};
    const auto r = std::to_chars(buf.data(), buf.data() + buf.size(), f);
    return std::string{buf.data(), r.ptr};
}

}  // namespace

// ---- column types -------------------------------------------------------------

ColumnType parse_column_type(std::string_view spelling) {
    ColumnType t;
    t.spelling = std::string{spelling};
    std::string s = trim(spelling);
    const auto unsupported = [&](std::string why) {
        t.kind = TypeKind::Unsupported;
        t.unsupported_reason = std::move(why);
        return t;
    };
    if (auto inner = unwrap(s, "LowCardinality")) {
        s = trim(*inner);
    }
    if (auto inner = unwrap(s, "Nullable")) {
        t.nullable = true;
        s = trim(*inner);
    }
    if (auto inner = unwrap(s, "LowCardinality")) {
        s = trim(*inner);
    }
    static const std::array<std::string_view, 4> kInts = {"Int8", "Int16", "Int32", "Int64"};
    static const std::array<std::string_view, 4> kUInts = {"UInt8", "UInt16", "UInt32", "UInt64"};
    if (std::find(kInts.begin(), kInts.end(), s) != kInts.end()) {
        t.kind = TypeKind::Int;
        return t;
    }
    if (std::find(kUInts.begin(), kUInts.end(), s) != kUInts.end()) {
        t.kind = TypeKind::UInt;
        return t;
    }
    if (s == "Float32") {
        t.kind = TypeKind::Float32;
        return t;
    }
    if (s == "Float64") {
        t.kind = TypeKind::Float64;
        return t;
    }
    if (s == "Bool") {
        t.kind = TypeKind::Bool;
        return t;
    }
    if (s == "String") {
        t.kind = TypeKind::String;
        return t;
    }
    if (s == "Date" || s == "Date32") {
        t.kind = TypeKind::Date;
        return t;
    }
    if (s == "DateTime" || unwrap(s, "DateTime")) {
        t.kind = TypeKind::DateTime;
        return t;
    }
    if (auto inner = unwrap(s, "DateTime64")) {
        const auto args = split_args(*inner);
        const auto p = args.empty() ? std::nullopt : small_int(args.front());
        if (!p || *p > 9) {
            return unsupported("a DateTime64 precision it cannot read: " + std::string{spelling});
        }
        t.kind = TypeKind::DateTime64;
        t.precision = *p;
        return t;
    }
    if (auto inner = unwrap(s, "Decimal")) {
        const auto args = split_args(*inner);
        const auto p = args.size() == 2 ? small_int(args[0]) : std::nullopt;
        const auto sc = args.size() == 2 ? small_int(args[1]) : std::nullopt;
        if (!p || !sc || *p > 38 || *sc > *p) {
            return unsupported("a Decimal it cannot read: " + std::string{spelling});
        }
        t.kind = TypeKind::Decimal;
        t.precision = *p;
        t.scale = *sc;
        return t;
    }
    for (const auto& [name, p] : std::array<std::pair<std::string_view, int>, 3>{
             {{"Decimal32", 9}, {"Decimal64", 18}, {"Decimal128", 38}}}) {
        if (auto inner = unwrap(s, name)) {
            const auto sc = small_int(trim(*inner));
            if (!sc || *sc > p) {
                return unsupported("a Decimal it cannot read: " + std::string{spelling});
            }
            t.kind = TypeKind::Decimal;
            t.precision = p;
            t.scale = *sc;
            return t;
        }
    }
    if (s.rfind("FixedString", 0) == 0) {
        return unsupported(
            "FixedString pads its values with zero bytes, which this release does "
            "not compare");
    }
    return unsupported("columns of type " + std::string{spelling} +
                       " are not compared by this release");
}

// ---- canonical cells ------------------------------------------------------------

namespace canon {

std::string null_cell() {
    return "N";
}

std::string integer(std::string_view text) {
    bool neg = false;
    std::string_view digits = text;
    if (!digits.empty() && (digits.front() == '-' || digits.front() == '+')) {
        neg = digits.front() == '-';
        digits.remove_prefix(1);
    }
    if (!all_digits(digits)) {
        return bad("not an integer: " + std::string{text.substr(0, 40)});
    }
    const std::string d = strip_zeros(digits);
    return "I" + std::string{neg && d != "0" ? "-" : ""} + d;
}

std::string float64_bits(std::uint64_t bits) {
    const double d = std::bit_cast<double>(bits);
    if (std::isnan(d)) {
        return "Fnan";
    }
    std::array<char, 17> hex{};
    std::snprintf(hex.data(), hex.size(), "%016llx", static_cast<unsigned long long>(bits));
    return "F" + std::string{hex.data()};
}

std::string float32_bits(std::uint32_t bits) {
    const float f = std::bit_cast<float>(bits);
    if (std::isnan(f)) {
        return "fnan";
    }
    std::array<char, 9> hex{};
    std::snprintf(hex.data(), hex.size(), "%08x", bits);
    return "f" + std::string{hex.data()};
}

std::string boolean(bool b) {
    return b ? "B1" : "B0";
}

std::string string(std::string_view bytes) {
    return "S" + std::string{bytes};
}

std::string decimal(std::string_view text, int scale) {
    bool neg = false;
    std::string_view s = text;
    if (!s.empty() && (s.front() == '-' || s.front() == '+')) {
        neg = s.front() == '-';
        s.remove_prefix(1);
    }
    const auto dot = s.find('.');
    std::string_view whole = dot == std::string_view::npos ? s : s.substr(0, dot);
    std::string_view frac = dot == std::string_view::npos ? std::string_view{} : s.substr(dot + 1);
    if (whole.empty() && frac.empty()) {
        return bad("not a decimal: " + std::string{text.substr(0, 40)});
    }
    if ((!whole.empty() && !all_digits(whole)) || (!frac.empty() && !all_digits(frac))) {
        return bad("not a decimal: " + std::string{text.substr(0, 40)});
    }
    std::string f{frac};
    if (static_cast<int>(f.size()) > scale) {
        const std::string extra = f.substr(static_cast<std::size_t>(scale));
        if (extra.find_first_not_of('0') != std::string::npos) {
            return bad("not exact at scale " + std::to_string(scale) + ": " +
                       std::string{text.substr(0, 40)});
        }
        f.resize(static_cast<std::size_t>(scale));
    }
    f.append(static_cast<std::size_t>(scale) - f.size(), '0');
    const std::string unscaled = strip_zeros(std::string{whole.empty() ? "0" : whole} + f);
    return "D" + std::string{neg && unscaled != "0" ? "-" : ""} + unscaled + "@" +
           std::to_string(scale);
}

std::string ticks(std::int64_t n, int precision) {
    return "T" + std::to_string(n) + "@" + std::to_string(precision);
}

std::string days(std::int64_t n) {
    return "d" + std::to_string(n);
}

}  // namespace canon

std::string canon_expected(const config::JsonValue& v, const ColumnType& type) {
    if (v.is_null()) {
        return canon::null_cell();
    }
    switch (type.kind) {
        case TypeKind::Int:
        case TypeKind::UInt: {
            const auto text = integer_text(v);
            if (!text) {
                return bad("not a whole number for " + type.spelling);
            }
            const std::string c = canon::integer(*text);
            if (type.kind == TypeKind::UInt && c.size() > 1 && c[1] == '-') {
                return bad("negative for " + type.spelling);
            }
            return c;
        }
        case TypeKind::Float64:
        case TypeKind::Float32: {
            double d = 0;
            if (v.is_integral_number()) {
                d = static_cast<double>(v.as_int());
            } else if (v.is_number()) {
                d = v.as_number();
            } else {
                return bad("not a number for " + type.spelling);
            }
            if (type.kind == TypeKind::Float64) {
                return canon::float64_bits(std::bit_cast<std::uint64_t>(d));
            }
            return canon::float32_bits(std::bit_cast<std::uint32_t>(static_cast<float>(d)));
        }
        case TypeKind::Bool:
            if (!v.is_bool()) {
                return bad("not a boolean for " + type.spelling);
            }
            return canon::boolean(v.as_bool());
        case TypeKind::String:
            if (!v.is_string()) {
                return bad("not text for " + type.spelling);
            }
            return canon::string(config::is_dec_string(v)
                                     ? std::string_view{v.as_string()}.substr(1)
                                     : std::string_view{v.as_string()});
        case TypeKind::Decimal: {
            std::string text;
            if (config::is_dec_string(v)) {
                text = v.as_string().substr(1);
            } else if (v.is_string()) {
                text = v.as_string();
            } else if (v.is_integral_number()) {
                text = std::to_string(v.as_int());
            } else if (v.is_number()) {
                text = shortest(v.as_number());
                if (text.find_first_of("eE") != std::string::npos) {
                    return bad("a number in exponent form for " + type.spelling);
                }
            } else {
                return bad("not a decimal for " + type.spelling);
            }
            return canon::decimal(text, type.scale);
        }
        case TypeKind::DateTime64:
        case TypeKind::DateTime: {
            const auto text = integer_text(v);
            const auto ms = text ? as_i64(*text) : std::nullopt;
            if (!ms) {
                return bad("not epoch milliseconds for " + type.spelling);
            }
            const int p = type.kind == TypeKind::DateTime ? 0 : type.precision;
            if (p >= 3) {
                const auto mul = pow10(p - 3);
                if (!mul || (*ms != 0 &&
                             std::llabs(*ms) > std::numeric_limits<std::int64_t>::max() / *mul)) {
                    return bad("out of range for " + type.spelling);
                }
                return canon::ticks(*ms * *mul, p);
            }
            const std::int64_t div = *pow10(3 - p);
            if (*ms % div != 0) {
                return bad("not whole at precision " + std::to_string(p) + " for " + type.spelling);
            }
            return canon::ticks(*ms / div, p);
        }
        case TypeKind::Date: {
            if (v.is_string() && !config::is_dec_string(v)) {
                const std::string& s = v.as_string();
                if (s.size() == 10 && s[4] == '-' && s[7] == '-') {
                    const auto y = as_i64(std::string_view{s}.substr(0, 4));
                    const auto m = small_int(std::string_view{s}.substr(5, 2));
                    const auto d = small_int(std::string_view{s}.substr(8, 2));
                    if (y && m && d && *m >= 1 && *m <= 12 && *d >= 1 && *d <= 31) {
                        return canon::days(days_from_civil(
                            *y, static_cast<unsigned>(*m), static_cast<unsigned>(*d)));
                    }
                }
            }
            const auto text = integer_text(v);
            const auto n = text ? as_i64(*text) : std::nullopt;
            if (!n) {
                return bad("not days or YYYY-MM-DD for " + type.spelling);
            }
            return canon::days(*n);
        }
        case TypeKind::Unsupported:
            break;
    }
    return bad("type " + type.spelling + " is not compared");
}

std::string display(std::string_view c, std::size_t max_bytes) {
    if (c.empty()) {
        return "?";
    }
    const char tag = c.front();
    const std::string_view body = c.substr(1);
    switch (tag) {
        case 'N':
            return "NULL";
        case 'I':
            return std::string{body};
        case 'B':
            return body == "1" ? "true" : "false";
        case 'F': {
            if (body == "nan") {
                return "NaN";
            }
            std::uint64_t bits = 0;
            std::from_chars(body.data(), body.data() + body.size(), bits, 16);
            return shortest(std::bit_cast<double>(bits));
        }
        case 'f': {
            if (body == "nan") {
                return "NaN";
            }
            std::uint32_t bits = 0;
            std::from_chars(body.data(), body.data() + body.size(), bits, 16);
            return shortest(std::bit_cast<float>(bits));
        }
        case 'S': {
            std::string out = "\"";
            const std::size_t n = std::min(body.size(), max_bytes);
            for (std::size_t i = 0; i < n; ++i) {
                const auto ch = static_cast<unsigned char>(body[i]);
                if (ch == '"' || ch == '\\') {
                    out += '\\';
                    out += static_cast<char>(ch);
                } else if (ch < 0x20 || ch == 0x7f) {
                    std::array<char, 8> esc{};
                    std::snprintf(esc.data(), esc.size(), "\\x%02x", ch);
                    out += esc.data();
                } else {
                    out += static_cast<char>(ch);
                }
            }
            out += '"';
            if (body.size() > n) {
                out += " (+" + std::to_string(body.size() - n) + " bytes)";
            }
            return out;
        }
        case 'D': {
            const auto at = body.rfind('@');
            if (at == std::string_view::npos) {
                return std::string{c};
            }
            const auto scale = small_int(body.substr(at + 1)).value_or(0);
            std::string n{body.substr(0, at)};
            const bool neg = !n.empty() && n.front() == '-';
            if (neg) {
                n.erase(n.begin());
            }
            if (scale > 0) {
                if (static_cast<int>(n.size()) <= scale) {
                    n.insert(0, static_cast<std::size_t>(scale) + 1 - n.size(), '0');
                }
                n.insert(n.size() - static_cast<std::size_t>(scale), ".");
            }
            return (neg ? "-" : "") + n;
        }
        case 'T': {
            const auto at = body.rfind('@');
            if (at == std::string_view::npos) {
                return std::string{c};
            }
            const auto n = as_i64(body.substr(0, at));
            const auto p = small_int(body.substr(at + 1));
            if (!n || !p) {
                return std::string{c};
            }
            const std::int64_t unit = *pow10(*p);
            std::int64_t secs = *n / unit;
            std::int64_t frac = *n % unit;
            if (frac < 0) {
                frac += unit;
                --secs;
            }
            const std::int64_t day = secs >= 0 ? secs / 86400 : (secs - 86399) / 86400;
            const std::int64_t sod = secs - day * 86400;
            std::array<char, 64> buf{};
            std::snprintf(buf.data(),
                          buf.size(),
                          "%sT%02lld:%02lld:%02lld",
                          date_text(day).c_str(),
                          static_cast<long long>(sod / 3600),
                          static_cast<long long>((sod / 60) % 60),
                          static_cast<long long>(sod % 60));
            std::string out = buf.data();
            if (*p > 0) {
                std::string f = std::to_string(frac);
                f.insert(0, static_cast<std::size_t>(*p) - f.size(), '0');
                out += "." + f;
            }
            return out + "Z";
        }
        case 'd': {
            const auto n = as_i64(body);
            return n ? date_text(*n) : std::string{c};
        }
        case '!':
            return "(" + std::string{body} + ")";
        default:
            return std::string{c};
    }
}

std::vector<Row> Destination::read_all(const std::vector<Column>& /*columns*/,
                                       std::uint64_t /*max_rows*/) {
    throw DestinationUnavailable("this destination cannot be read whole");
}

std::string Destination::parts_marker() {
    throw DestinationUnavailable("this destination cannot say whether its table changed");
}

const char* to_string(Discrepancy::Kind k) noexcept {
    switch (k) {
        case Discrepancy::Kind::Missing:
            return "missing";
        case Discrepancy::Kind::Duplicate:
            return "duplicate";
        case Discrepancy::Kind::Mismatch:
            return "changed";
        case Discrepancy::Kind::Unexpected:
            return "unexpected";
    }
    return "?";
}

// ---- the scope ----------------------------------------------------------------

namespace {

constexpr std::uintmax_t kMaxIntervalBytes = 64ULL << 20;
constexpr std::size_t kMaxIntervalsWalked = 256;
// A lineage from a run's start, through its restores.
constexpr std::size_t kMaxLineageIntervals = 100000;

std::string read_bounded(const fs::path& path, std::uintmax_t max_bytes) {
    std::error_code ec;
    const auto size = fs::file_size(path, ec);
    if (ec) {
        throw std::runtime_error("cannot read " + path.string() + ": " + ec.message());
    }
    if (size > max_bytes) {
        throw std::runtime_error(path.string() + " is " + std::to_string(size) +
                                 " bytes, past the " + std::to_string(max_bytes) + " allowed");
    }
    std::ifstream in(path, std::ios::binary);
    std::string out(static_cast<std::size_t>(size), '\0');
    in.read(out.data(), static_cast<std::streamsize>(size));
    if (static_cast<std::uintmax_t>(in.gcount()) != size) {
        throw std::runtime_error("cannot read " + path.string() + " whole");
    }
    return out;
}

std::uint64_t u64_of(const config::JsonValue& o, const char* key) {
    if (!o.is_object() || !o.contains(key)) {
        throw std::runtime_error(std::string{"no "} + key);
    }
    const auto& v = o.at(key);
    if (v.is_integral_number() && v.as_int() >= 0) {
        return static_cast<std::uint64_t>(v.as_int());
    }
    if (v.is_string() && all_digits(v.as_string())) {
        return std::stoull(v.as_string());
    }
    throw std::runtime_error(std::string{key} + " is not a whole number");
}

std::string string_of(const config::JsonValue& o, const char* key) {
    if (!o.is_object() || !o.contains(key) || !o.at(key).is_string()) {
        throw std::runtime_error(std::string{"no text "} + key);
    }
    return o.at(key).as_string();
}

IntervalEvidence read_interval(const Bundle& b, const SinkInterval& iv) {
    const config::JsonValue doc = config::parse(read_bounded(b.root / iv.path, kMaxIntervalBytes));
    if (string_of(doc, "format") != capture::kSinkIntervalFormat) {
        throw std::runtime_error("not sink interval evidence");
    }
    if (u64_of(doc, "version") != static_cast<std::uint64_t>(capture::kSinkIntervalVersion)) {
        throw std::runtime_error("a version this build does not read");
    }
    if (u64_of(doc, "op") != iv.op || u64_of(doc, "subtask") != iv.subtask ||
        u64_of(doc, "checkpoint") != iv.epoch) {
        throw std::runtime_error(
            "its contents name another sink, subtask or checkpoint than its path");
    }
    IntervalEvidence e;
    e.epoch = iv.epoch;
    e.subtask = iv.subtask;
    e.path = iv.path;
    e.previous_barrier = u64_of(doc, "previous_barrier");
    e.attempt = string_of(doc, "attempt");
    const auto& rec = doc.at("records");
    e.capture = string_of(rec, "capture");
    e.records_seen = u64_of(rec, "seen");
    e.truncated =
        rec.contains("truncated") && rec.at("truncated").is_bool() && rec.at("truncated").as_bool();
    if (doc.contains("watermark_ms") && doc.at("watermark_ms").is_integral_number()) {
        e.watermark_ms = doc.at("watermark_ms").as_int();
    }
    e.sink = doc.contains("sink") ? doc.at("sink") : config::JsonValue{};
    if (e.sink.is_object() && e.sink.contains("subtask")) {
        e.sink_subtask = u64_of(e.sink, "subtask");
        e.parallelism = u64_of(e.sink, "parallelism");
    }
    return e;
}

// The data records of a captured epoch, each the Row channel's JSON object.
std::vector<config::JsonValue> capture_rows(const fs::path& path, std::uint64_t& stored) {
    const std::string bytes = read_bounded(path, 8ULL << 30);
    Codec<std::string> codec;
    codec.encode = [](const std::string& v) {
        const auto* p = reinterpret_cast<const std::byte*>(v.data());
        return Codec<std::string>::Bytes(p, p + v.size());
    };
    codec.decode = [](Codec<std::string>::BytesView b) -> std::optional<std::string> {
        return std::string(reinterpret_cast<const char*>(b.data()), b.size());
    };
    const auto parsed = capture::read_capture_events<std::string>(
        std::span<const std::byte>{reinterpret_cast<const std::byte*>(bytes.data()), bytes.size()},
        codec);
    if (!parsed) {
        throw std::runtime_error("not a capture file this build reads");
    }
    std::vector<config::JsonValue> out;
    for (const auto& ev : parsed->second) {
        if (const auto* r = std::get_if<Record<std::string>>(&ev)) {
            config::JsonValue v = config::parse(r->value());
            if (!v.is_object()) {
                throw std::runtime_error("a record that is not a row");
            }
            out.push_back(std::move(v));
        }
    }
    stored = out.size();
    return out;
}

std::string completed_marker_path(JobId job, std::uint64_t checkpoint) {
    return "checkpoint/_jobs/" + std::to_string(job) + "/COMPLETED-" + std::to_string(checkpoint);
}

}  // namespace

namespace {

constexpr std::string_view kKafkaOffsetPrefix = "__kafka_off__:";
// The log the positions belong to, which the source records beside them
// (KafkaSource::encode_identity in impls/kafka).
constexpr std::string_view kKafkaIdentityKey = "__kafka_identity__";

std::optional<SourceLog> parse_source_log(std::string_view bytes) {
    try {
        const auto v = config::parse(bytes);
        if (!v.is_object() || !v.contains("version") || !v.at("version").is_integral_number() ||
            v.at("version").as_int() != 1) {
            return std::nullopt;
        }
        SourceLog log;
        for (const auto& [field, out] : {std::pair{"topic", &log.topic},
                                         std::pair{"cluster_id", &log.cluster_id},
                                         std::pair{"topic_id", &log.topic_id}}) {
            if (!v.contains(field) || !v.at(field).is_string()) {
                return std::nullopt;
            }
            *out = v.at(field).as_string();
        }
        if (log.topic.empty()) {
            return std::nullopt;
        }
        return log;
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

std::string describe_log(const SourceLog& log) {
    return "topic '" + log.topic + "' on cluster " +
           (log.cluster_id.empty() ? std::string{"(id not recorded)"} : log.cluster_id) +
           ", topic id " + (log.topic_id.empty() ? std::string{"(not recorded)"} : log.topic_id);
}

// Every Kafka source position in the bundle's snapshots of `checkpoint`:
// (operator, partition) -> next offset to read, and into `logs` each source's
// log as it recorded it. `complete` when every participant the checkpoint's
// marker names had its snapshot read.
std::map<std::pair<std::uint64_t, std::int32_t>, std::int64_t> kafka_positions(
    const Bundle& b,
    std::uint64_t checkpoint,
    bool& complete,
    std::vector<std::string>& notes,
    std::map<std::uint64_t, SourceLog>& logs) {
    std::map<std::pair<std::uint64_t, std::int32_t>, std::int64_t> out;
    std::set<std::uint64_t> unreadable_logs;
    std::set<std::uint64_t> disagreeing_logs;
    complete = false;
    const std::string suffix = "/checkpoint-" + std::to_string(checkpoint) + ".snap";
    const std::string prefix = "checkpoint/_state/" + std::to_string(b.job()) + "/";
    std::set<std::string> read_dirs;
    std::uint64_t stale = 0;
    for (const auto& a : b.artifacts) {
        if ((a.role != "snapshot" && a.role != "replay_state" && a.role != "interval_state") ||
            a.path.rfind(prefix, 0) != 0 || !a.path.ends_with(suffix)) {
            continue;
        }
        try {
            const std::string bytes = read_bounded(b.root / a.path, 8ULL << 30);
            std::vector<std::byte> v(bytes.size());
            std::memcpy(v.data(), bytes.data(), bytes.size());
            InMemoryStateBackend backend;
            backend.restore(
                Snapshot{.checkpoint_id = CheckpointId{checkpoint}, .bytes = std::move(v)});
            for (const auto op : backend.operator_ids()) {
                backend.scan_operator_state(
                    op, [&](StateBackend::KeyView key, StateBackend::ValueView value) {
                        if (key == kKafkaIdentityKey) {
                            const auto log = parse_source_log(value);
                            if (!log) {
                                unreadable_logs.insert(op.value());
                                return;
                            }
                            const auto [it, fresh] = logs.emplace(op.value(), *log);
                            if (!fresh && it->second != *log) {
                                disagreeing_logs.insert(op.value());
                            }
                            return;
                        }
                        if (!key.starts_with(kKafkaOffsetPrefix) || value.size() < 8) {
                            return;
                        }
                        const auto p = small_int(key.substr(kKafkaOffsetPrefix.size()));
                        if (!p) {
                            return;
                        }
                        std::uint64_t u = 0;
                        for (int i = 0; i < 8; ++i) {
                            u |= static_cast<std::uint64_t>(
                                     static_cast<unsigned char>(value[static_cast<std::size_t>(i)]))
                                 << (i * 8);
                        }
                        // A checkpoint taken after a restore by an engine that kept
                        // every participant's operator rows in every subtask holds
                        // copies of the sources' positions at the restore point,
                        // which only the source's own subtask moves on; a copy never
                        // runs ahead of it. The greatest is the source's own, as the
                        // engine's restore keeps the greater offset on a collision
                        // (state_backend_factory). A restore now keeps only the rows
                        // of the operators a subtask runs, so newer checkpoints hold
                        // none, but a bundle may come from an older one.
                        const std::pair<std::uint64_t, std::int32_t> k{
                            op.value(), static_cast<std::int32_t>(*p)};
                        const auto offset = static_cast<std::int64_t>(u);
                        const auto it = out.find(k);
                        if (it == out.end()) {
                            out.emplace(k, offset);
                        } else if (it->second != offset) {
                            ++stale;
                            it->second = std::max(it->second, offset);
                        }
                    });
            }
            read_dirs.insert(a.path.substr(0, a.path.size() - suffix.size()));
        } catch (const std::exception& e) {
            notes.push_back(a.path + " could not be read for source positions: " + e.what());
        }
    }
    for (const auto op : unreadable_logs) {
        notes.push_back("checkpoint " + std::to_string(checkpoint) + ": the source (operator " +
                        std::to_string(op) +
                        ") recorded its log in a form this release does not read");
    }
    for (const auto op : disagreeing_logs) {
        notes.push_back("checkpoint " + std::to_string(checkpoint) +
                        ": the snapshots record different logs for the source (operator " +
                        std::to_string(op) + "); the first read is reported");
    }
    if (stale > 0) {
        notes.push_back("checkpoint " + std::to_string(checkpoint) + ": " + std::to_string(stale) +
                        " older cop" + (stale == 1 ? "y" : "ies") +
                        " of a source position, which an earlier engine's restore left in every "
                        "subtask, set aside for the source's own");
    }
    const Artifact* marker = b.find("checkpoint/_jobs/" + std::to_string(b.job()) + "/COMPLETED-" +
                                    std::to_string(checkpoint));
    if (marker != nullptr) {
        try {
            const auto m =
                cluster::parse_completed_marker(read_bounded(b.root / marker->path, 1 << 20));
            complete = m.subtasks.has_value() && m.subtasks->size() == read_dirs.size();
        } catch (const std::exception&) {
            complete = false;
        }
    }
    return out;
}

// The partitions each watermark assigner subtask had been declared, as it
// recorded them at `checkpoint` (WatermarkAssignerOperator's
// "__clink_wm_declared<slot>:<subtask>" rows): subtask -> partitions.
std::map<std::uint32_t, std::set<std::int32_t>> declared_partitions(const Bundle& b,
                                                                    std::uint64_t checkpoint) {
    constexpr std::string_view kDeclared = "__clink_wm_declared";
    std::map<std::uint32_t, std::set<std::int32_t>> out;
    const std::string suffix = "/checkpoint-" + std::to_string(checkpoint) + ".snap";
    const std::string prefix = "checkpoint/_state/" + std::to_string(b.job()) + "/";
    for (const auto& a : b.artifacts) {
        if ((a.role != "snapshot" && a.role != "replay_state" && a.role != "interval_state") ||
            a.path.rfind(prefix, 0) != 0 || !a.path.ends_with(suffix)) {
            continue;
        }
        try {
            const std::string bytes = read_bounded(b.root / a.path, 8ULL << 30);
            std::vector<std::byte> v(bytes.size());
            std::memcpy(v.data(), bytes.data(), bytes.size());
            InMemoryStateBackend backend;
            backend.restore(
                Snapshot{.checkpoint_id = CheckpointId{checkpoint}, .bytes = std::move(v)});
            for (const auto op : backend.operator_ids()) {
                backend.scan_operator_state(
                    op, [&](StateBackend::KeyView key, StateBackend::ValueView value) {
                        if (!key.starts_with(kDeclared)) {
                            return;
                        }
                        const auto colon = key.rfind(':');
                        const auto sub = colon == std::string_view::npos
                                             ? std::nullopt
                                             : small_int(key.substr(colon + 1));
                        if (!sub) {
                            return;
                        }
                        auto& set = out[static_cast<std::uint32_t>(*sub)];
                        std::string_view list{reinterpret_cast<const char*>(value.data()),
                                              value.size()};
                        while (!list.empty()) {
                            const auto comma = list.find(',');
                            if (const auto p = small_int(list.substr(0, comma)); p) {
                                set.insert(static_cast<std::int32_t>(*p));
                            }
                            list = comma == std::string_view::npos ? std::string_view{}
                                                                   : list.substr(comma + 1);
                        }
                    });
            }
        } catch (const std::exception&) {
            // An unreadable snapshot is reported by the position scan.
        }
    }
    return out;
}

void describe_sources(const Bundle& b, SourcePositions& sp) {
    sp.isolation = "read_committed (librdkafka's default; the job sets no isolation.level)";
    sp.isolation_level = "read_committed";
    sp.isolation_set_by_job = false;
    if (b.find("job/manifest.json") == nullptr) {
        sp.notes.push_back(
            "the bundle holds no job manifest, so the sources' topics are not named");
        return;
    }
    try {
        const auto m = config::parse(read_bounded(b.root / "job/manifest.json", 64ULL << 20));
        const auto text = m.string_or("graph_json", "");
        const auto graph = config::parse(text);
        for (const auto& op : graph.at("ops").as_array()) {
            const std::string type = op.string_or("type", "");
            if (type.find("kafka") == std::string::npos || !op.contains("params") ||
                !op.at("params").is_object()) {
                continue;
            }
            const auto& params = op.at("params");
            if (!params.contains("topic")) {
                continue;
            }
            sp.topics.push_back(params.string_or("topic", "") + " (" + type + ")");
            for (const auto& [k, v] : params.as_object()) {
                if (std::string_view{k}.ends_with("isolation.level") && v.is_string()) {
                    sp.isolation = v.as_string() + " (set by the job)";
                    sp.isolation_level = v.as_string();
                    sp.isolation_set_by_job = true;
                }
            }
        }
    } catch (const std::exception& e) {
        sp.notes.push_back(std::string{"the job graph could not be read for the sources: "} +
                           e.what());
    }
}

}  // namespace

std::optional<bool> SourceIdentity::same_log() const {
    if (!start || !end) {
        return std::nullopt;
    }
    const auto agree = [](const std::string& a, const std::string& b) {
        return a.empty() || b.empty() || a == b;
    };
    return start->topic == end->topic && agree(start->cluster_id, end->cluster_id) &&
           agree(start->topic_id, end->topic_id);
}

Scope load_scope(const Bundle& b,
                 std::uint64_t checkpoint,
                 std::uint64_t sink_op,
                 bool whole_run,
                 bool for_audit,
                 std::optional<std::uint64_t> since) {
    Scope sc;
    // A window's output depends on every record before it, so an audit of a
    // windowed job spans its whole lineage, through the restores the bundle
    // records, back to a start with no state; or, with `since`, the
    // increment after an earlier checkpoint of that lineage, which a
    // reference computes from its own state there.
    const WindowAnalysis analysis = analyse_window(b);
    if (since) {
        if (!analysis.windowed) {
            throw ScopeRefused(
                "--since names the start of an increment of a windowed job's lineage, and this "
                "job is record-wise: its scope already starts at the previous completed "
                "checkpoint");
        }
        if (*since == 0) {
            throw ScopeRefused("--since names a completed checkpoint, not 0");
        }
        whole_run = false;
    }
    const bool lineage = analysis.windowed && (whole_run || for_audit || since.has_value());
    if (analysis.windowed) {
        if (analysis.semantics) {
            sc.window = analysis.semantics;
        } else {
            sc.window_unsupported = analysis.unsupported;
        }
    }
    // A lineage spans the run from its start; an increment, from its base.
    whole_run = whole_run || (lineage && !since);
    sc.whole_run = whole_run;
    if (b.find("job/manifest.json") == nullptr) {
        sc.notes.push_back(
            "the bundle holds no job graph (it was collected without --ha-dir, or after the job "
            "ended), so that the job is record-wise is assumed, not read: a windowed job's audit "
            "needs the graph");
    }
    sc.job = b.job();
    sc.checkpoint = checkpoint != 0 ? checkpoint : b.checkpoint();
    const std::uint64_t n = sc.checkpoint;
    if (b.find(completed_marker_path(sc.job, n)) == nullptr) {
        throw ScopeRefused("checkpoint " + std::to_string(n) + " of job " + std::to_string(sc.job) +
                           " is not a completed checkpoint in this bundle; only a completed "
                           "checkpoint's effects are owed to the destination");
    }
    if (since && (*since >= n || b.find(completed_marker_path(sc.job, *since)) == nullptr)) {
        throw ScopeRefused("--since=" + std::to_string(*since) +
                           " is not a completed checkpoint before " + std::to_string(n) +
                           " in this bundle");
    }
    std::set<std::uint64_t> ops;
    for (const auto& iv : b.sink_intervals) {
        if (iv.epoch == n) {
            ops.insert(iv.op);
        }
    }
    if (ops.empty()) {
        throw ScopeRefused("the bundle holds no sink interval evidence for checkpoint " +
                           std::to_string(n) +
                           "; it is recorded when the Workers run with --capture-dir, and "
                           "collected with --include-captures");
    }
    if (sink_op != 0 && !ops.contains(sink_op)) {
        throw ScopeRefused("sink " + std::to_string(sink_op) +
                           " has no interval evidence for checkpoint " + std::to_string(n));
    }
    if (sink_op == 0 && ops.size() > 1) {
        std::string list;
        for (const auto op : ops) {
            list += (list.empty() ? "" : ", ") + std::to_string(op);
        }
        throw ScopeRefused("several sinks have interval evidence for checkpoint " +
                           std::to_string(n) + " (" + list + "); name one with --sink-op");
    }
    sc.sink_op = sink_op != 0 ? sink_op : *ops.begin();

    std::set<std::uint64_t> completed;
    const std::string marker_prefix = "checkpoint/_jobs/" + std::to_string(sc.job) + "/COMPLETED-";
    for (const auto& a : b.artifacts) {
        if (a.path.rfind(marker_prefix, 0) == 0 &&
            all_digits(a.path.substr(marker_prefix.size()))) {
            completed.insert(std::stoull(a.path.substr(marker_prefix.size())));
        }
    }

    const auto gap = [&](std::string why, bool unsupported = false) -> Scope& {
        if (!sc.gap) {
            sc.gap = std::move(why);
            sc.gap_unsupported = unsupported;
        }
        return sc;
    };

    // The intervals of one epoch, every sink subtask's, or a gap.
    const auto load_epoch =
        [&](std::uint64_t epoch) -> std::optional<std::vector<IntervalEvidence>> {
        std::vector<IntervalEvidence> got;
        for (const auto& iv : b.sink_intervals) {
            if (iv.op != sc.sink_op || iv.epoch != epoch) {
                continue;
            }
            try {
                got.push_back(read_interval(b, iv));
            } catch (const std::exception& e) {
                gap(iv.path + " cannot be read: " + e.what());
                return std::nullopt;
            }
        }
        if (got.empty()) {
            gap("the bundle holds no interval evidence for the sink's interval ending at barrier " +
                std::to_string(epoch) + "; collect again with --epochs reaching back to it");
            return std::nullopt;
        }
        for (const auto& e : got) {
            if (!e.sink.is_object() || !e.sink.contains("connector")) {
                gap(e.path +
                        " carries no account from the sink, which this release needs to "
                        "reconcile it",
                    true);
                return std::nullopt;
            }
            if (e.sink.at("connector").as_string() != "clickhouse_native") {
                gap(e.path + " is from a " + e.sink.at("connector").as_string() +
                        " sink, which this release does not reconcile",
                    true);
                return std::nullopt;
            }
        }
        const std::uint64_t p = got.front().parallelism;
        std::set<std::uint64_t> subtasks;
        for (const auto& e : got) {
            if (e.parallelism != p) {
                gap("the sink's subtasks report different parallelisms at barrier " +
                    std::to_string(epoch));
                return std::nullopt;
            }
            subtasks.insert(e.sink_subtask);
        }
        if (p == 0 || subtasks.size() != got.size() || subtasks.size() != p ||
            *subtasks.rbegin() != p - 1) {
            std::string have;
            for (const auto st : subtasks) {
                have += (have.empty() ? "" : ", ") + std::to_string(st);
            }
            gap("interval evidence at barrier " + std::to_string(epoch) + " covers sink subtasks " +
                have + " of " + std::to_string(p) + "; every subtask's is needed");
            return std::nullopt;
        }
        for (const auto& e : got) {
            if (e.previous_barrier != got.front().previous_barrier) {
                gap("the sink's subtasks saw different barriers before " + std::to_string(epoch));
                return std::nullopt;
            }
        }
        return got;
    };

    // Back from the checkpoint to the previous completed one, or to the start
    // of the run: everything in between is what this checkpoint completed. A
    // lineage walk carries on through each restore the bundle records into the
    // run that completed the restored checkpoint, until a run that began with
    // no state.
    std::uint64_t epoch = n;
    std::size_t segment = 0;  // runs walked back through, for the attempt check
    std::map<std::uint64_t, std::size_t> segment_of;
    const std::size_t walk_bound = whole_run || since ? kMaxLineageIntervals : kMaxIntervalsWalked;
    std::optional<std::vector<cluster::RestoredRecord>> restores_seen;
    while (true) {
        if (sc.epochs.size() >= walk_bound) {
            return gap("more than " + std::to_string(walk_bound) + " intervals " +
                       (since       ? "since checkpoint " + std::to_string(*since)
                        : whole_run ? std::string{"since the run's start"}
                                    : std::string{"since the previous completed checkpoint"}));
        }
        auto got = load_epoch(epoch);
        if (!got) {
            return sc;
        }
        sc.epochs.insert(sc.epochs.begin(), epoch);
        segment_of[epoch] = segment;
        const std::uint64_t prev = got->front().previous_barrier;
        for (auto& e : *got) {
            sc.intervals.push_back(std::move(e));
        }
        if (since) {
            // An increment: back through the completed checkpoints and the
            // restores between to the base, which must be on the lineage.
            const auto not_on = [&](const std::string& why) {
                throw ScopeRefused("checkpoint " + std::to_string(*since) +
                                   " is not on checkpoint " + std::to_string(n) +
                                   "'s lineage: " + why);
            };
            if (prev == *since) {
                sc.starts_after = prev;
                break;
            }
            if (prev == 0) {
                if (!restores_seen) {
                    try {
                        restores_seen = recorded_restores(b);
                    } catch (const std::exception& e) {
                        return gap(std::string{"the job's restore record cannot be read: "} +
                                   e.what());
                    }
                }
                const auto start = epoch_start(b, *restores_seen, epoch);
                if (!start.known) {
                    return gap("the run that began at barrier " + std::to_string(epoch) +
                                   " cannot be placed in the job's lineage: " + start.reason,
                               start.unsupported);
                }
                if (start.checkpoint == *since) {
                    sc.restores.insert(sc.restores.begin(),
                                       LineageRestore{start.checkpoint, epoch});
                    sc.starts_after = *since;
                    break;
                }
                if (start.checkpoint > *since) {
                    if (!completed.contains(start.checkpoint)) {
                        return gap("the run that began at barrier " + std::to_string(epoch) +
                                   " restored from checkpoint " + std::to_string(start.checkpoint) +
                                   ", which the bundle does not hold");
                    }
                    sc.restores.insert(sc.restores.begin(),
                                       LineageRestore{start.checkpoint, epoch});
                    ++segment;
                    epoch = start.checkpoint;
                    continue;
                }
                not_on(start.checkpoint == 0
                           ? "the run that began at barrier " + std::to_string(epoch) +
                                 " began with no state"
                           : "the run that began at barrier " + std::to_string(epoch) +
                                 " restored from checkpoint " + std::to_string(start.checkpoint) +
                                 "; an increment can start there");
            }
            if (prev >= epoch) {
                return gap("interval evidence names barrier " + std::to_string(prev) +
                           " as the one before " + std::to_string(epoch));
            }
            if (prev < *since) {
                not_on("the run's barriers go from " + std::to_string(prev) + " to " +
                       std::to_string(epoch) + " without it");
            }
            epoch = prev;
            continue;
        }
        if (prev == 0 && lineage) {
            if (!restores_seen) {
                try {
                    restores_seen = recorded_restores(b);
                } catch (const std::exception& e) {
                    return gap(std::string{"the job's restore record cannot be read: "} + e.what());
                }
            }
            const auto start = epoch_start(b, *restores_seen, epoch);
            if (!start.known) {
                return gap("the run that began at barrier " + std::to_string(epoch) +
                               " cannot be placed in the job's lineage: " + start.reason,
                           start.unsupported);
            }
            if (start.checkpoint > 0) {
                if (!completed.contains(start.checkpoint)) {
                    return gap("the run that began at barrier " + std::to_string(epoch) +
                               " restored from checkpoint " + std::to_string(start.checkpoint) +
                               ", which the bundle does not hold: the history a window's result "
                               "depends on is not all in it");
                }
                sc.restores.insert(sc.restores.begin(), LineageRestore{start.checkpoint, epoch});
                sc.notes.push_back("the run that began at barrier " + std::to_string(epoch) +
                                   " restored from checkpoint " + std::to_string(start.checkpoint) +
                                   "; the lineage carries on through the run that completed it");
                ++segment;
                epoch = start.checkpoint;
                continue;
            }
        }
        if (prev == 0) {
            sc.starts_at_run_start = true;
            break;
        }
        if (prev >= epoch) {
            return gap("interval evidence names barrier " + std::to_string(prev) +
                       " as the one before " + std::to_string(epoch));
        }
        if (completed.contains(prev) && !whole_run) {
            sc.starts_after = prev;
            break;
        }
        if (!completed.contains(prev)) {
            sc.notes.push_back("checkpoint " + std::to_string(prev) +
                               " did not complete, so checkpoint " + std::to_string(n) +
                               " also completed the interval ending at its barrier");
        }
        epoch = prev;
    }
    std::sort(sc.intervals.begin(), sc.intervals.end(), [](const auto& x, const auto& y) {
        return std::tie(x.epoch, x.sink_subtask) < std::tie(y.epoch, y.sink_subtask);
    });

    // A run that began at a restore numbers its epochs from the RESTORED
    // record's next; one that began at a rescale merged several subtasks'
    // state, and its first interval is not attributed here.
    if (sc.starts_at_run_start) {
        const auto restores = recorded_restores(b);
        const auto start = epoch_start(b, restores, sc.epochs.front());
        if (!start.known && start.unsupported) {
            return gap("the scope begins at the run's first interval, which " + start.reason, true);
        }
        if (start.known) {
            sc.starts_after = start.checkpoint;
            sc.notes.push_back(start.checkpoint == 0
                                   ? "the scope begins where the job's run began, with no state"
                                   : "the scope begins where the run restored from checkpoint " +
                                         std::to_string(start.checkpoint));
        } else {
            sc.notes.push_back(
                "the scope begins at the start of a run the bundle does not place: " +
                start.reason);
        }
    }

    // One destination for the whole scope.
    sc.destination = sc.intervals.front().sink.at("destination");
    for (const auto& e : sc.intervals) {
        if (!(e.sink.at("destination") == sc.destination)) {
            return gap("the sink's subtasks describe different destinations (" + e.path + ")");
        }
    }
    try {
        for (const auto& c : sc.destination.at("columns").as_array()) {
            sc.columns.push_back(
                Column{string_of(c, "name"), parse_column_type(string_of(c, "target"))});
        }
    } catch (const std::exception& e) {
        return gap(std::string{"the evidence's column plan cannot be read: "} + e.what());
    }
    if (sc.columns.empty()) {
        return gap("the evidence names no columns the sink wrote");
    }

    // One runner attempt per sink subtask across each run the scope walks: the
    // walk follows one run's barriers, so another attempt there means mixed
    // evidence.
    {
        std::map<std::pair<std::size_t, std::uint64_t>, std::string> attempt_of;
        for (const auto& e : sc.intervals) {
            const auto [it, fresh] =
                attempt_of.emplace(std::pair{segment_of[e.epoch], e.sink_subtask}, e.attempt);
            if (!fresh && it->second != e.attempt) {
                return gap("sink subtask " + std::to_string(e.sink_subtask) +
                           "'s intervals in one run of the scope come from two runner attempts (" +
                           it->second + ", " + e.attempt + ")");
            }
        }
    }

    // The expected rows: every row the sink received in the scope's intervals.
    std::uint64_t ledger_rows = 0;
    bool ledger_whole = true;
    for (const auto& e : sc.intervals) {
        if (e.capture == "disarmed") {
            return gap(e.path +
                       ": the flight recorder stopped recording this sink after a write "
                       "failed, so its input for the interval is not known");
        }
        if (e.truncated) {
            return gap(e.path +
                       ": the interval's capture was truncated at --capture-records, so "
                       "it holds part of what the sink received");
        }
        if (e.sink.contains("rows_acknowledged") &&
            e.sink.at("rows_acknowledged").is_integral_number()) {
            const auto acked = static_cast<std::uint64_t>(e.sink.at("rows_acknowledged").as_int());
            ledger_rows += acked;
            // The barrier waited for every INSERT of the interval and none of
            // the next had begun, so the sink acknowledged exactly the rows it
            // received. Anything else means the record of its input is not the
            // record of its writes, and rows it wrote could sit under keys the
            // comparison never reads.
            if (acked != e.records_seen) {
                return gap(e.path + ": the sink acknowledged " + std::to_string(acked) +
                           " rows in the interval and its recorded input holds " +
                           std::to_string(e.records_seen));
            }
        } else {
            ledger_whole = false;
        }
        if (e.capture == "empty") {
            if (e.records_seen != 0) {
                return gap(e.path + " says the interval was empty and also that it held " +
                           std::to_string(e.records_seen) + " records");
            }
            continue;
        }
        if (e.capture != "written") {
            return gap(e.path + ": an unknown capture state '" + e.capture + "'");
        }
        const CapturedEpoch* cap = nullptr;
        for (const auto& c : b.sink_captures) {
            if (c.op == sc.sink_op && c.subtask == e.subtask && c.epoch == e.epoch) {
                cap = &c;
            }
        }
        if (cap == nullptr) {
            return gap("the sink's captured input for the interval ending at barrier " +
                       std::to_string(e.epoch) + " (subtask " + std::to_string(e.subtask) +
                       ") is not in the bundle");
        }
        if (cap->truncated) {
            return gap(cap->path + " is truncated");
        }
        std::uint64_t stored = 0;
        std::vector<config::JsonValue> rows;
        try {
            rows = capture_rows(b.root / cap->path, stored);
        } catch (const std::exception& ex) {
            return gap(cap->path + " cannot be read: " + ex.what());
        }
        if (stored != e.records_seen) {
            return gap(cap->path + " holds " + std::to_string(stored) + " rows of the " +
                       std::to_string(e.records_seen) + " the sink received in the interval");
        }
        for (const auto& r : rows) {
            Row row;
            row.reserve(sc.columns.size());
            for (const auto& col : sc.columns) {
                const auto& obj = r.as_object();
                const auto it = obj.find(col.name);
                row.push_back(
                    canon_expected(it == obj.end() ? config::JsonValue{} : it->second, col.type));
            }
            sc.expected.push_back(std::move(row));
            sc.expected_epochs.push_back(e.epoch);
        }
    }
    if (ledger_whole && ledger_rows != sc.expected.size()) {
        sc.notes.push_back("the sink's ledger acknowledges " + std::to_string(ledger_rows) +
                           " rows for the scope, and its captured input holds " +
                           std::to_string(sc.expected.size()));
    }

    // Where the sources stood at the checkpoints bounding the scope.
    describe_sources(b, sc.source);
    std::map<std::pair<std::uint64_t, std::int32_t>, std::int64_t> from;
    std::map<std::uint64_t, SourceLog> logs_from;
    std::map<std::uint64_t, SourceLog> logs_to;
    if (sc.starts_after > 0) {
        from = kafka_positions(
            b, sc.starts_after, sc.source.from_complete, sc.source.notes, logs_from);
    } else {
        sc.source.from_complete = sc.starts_at_run_start;
    }
    const auto to =
        kafka_positions(b, sc.checkpoint, sc.source.to_complete, sc.source.notes, logs_to);
    std::set<std::pair<std::uint64_t, std::int32_t>> parts;
    for (const auto& [k, _] : from) {
        parts.insert(k);
    }
    for (const auto& [k, _] : to) {
        parts.insert(k);
    }
    for (const auto& k : parts) {
        SourceRange r;
        r.op = k.first;
        r.partition = k.second;
        if (const auto it = from.find(k); it != from.end()) {
            r.from = it->second;
        }
        if (const auto it = to.find(k); it != to.end()) {
            r.to = it->second;
        }
        sc.source.ranges.push_back(r);
    }
    if (parts.empty()) {
        sc.source.notes.push_back(
            "no Kafka source position is in the bundle's snapshots of the bounding checkpoints "
            "(collect with --include-state)");
    }
    // The log each source's positions belong to. A range is a range of one
    // log only when the source recorded the same one at both ends: a topic
    // deleted and created again restarts its offsets at 0.
    std::set<std::uint64_t> source_ops;
    for (const auto& k : parts) {
        source_ops.insert(k.first);
    }
    for (const auto* logs : {&logs_from, &logs_to}) {
        for (const auto& [op, _] : *logs) {
            source_ops.insert(op);
        }
    }
    for (const auto op : source_ops) {
        SourceIdentity id;
        id.op = op;
        if (const auto it = logs_from.find(op); it != logs_from.end()) {
            id.start = it->second;
        }
        if (const auto it = logs_to.find(op); it != logs_to.end()) {
            id.end = it->second;
        }
        if (id.same_log() == std::optional<bool>{false}) {
            sc.source.notes.push_back(
                "the source (operator " + std::to_string(op) + ") recorded " +
                describe_log(*id.start) + " at checkpoint " + std::to_string(sc.starts_after) +
                ", and " + describe_log(*id.end) + " at checkpoint " +
                std::to_string(sc.checkpoint) +
                ": the scope's offsets of that source are not a range of one log");
        } else if (sc.starts_after > 0 && !id.start && id.end) {
            sc.source.notes.push_back("checkpoint " + std::to_string(sc.starts_after) +
                                      " records no log for the source (operator " +
                                      std::to_string(op) +
                                      "), so whether the scope's offsets are of one log is "
                                      "not known");
        }
        sc.source.identities.push_back(std::move(id));
    }
    if (!parts.empty() && logs_from.empty() && logs_to.empty()) {
        sc.source.notes.push_back(
            "the bounding checkpoints record no log for the sources' positions (their topic, "
            "cluster and topic ids): a release before identities were recorded took them");
    }
    // An increment's base: where the source stood there (the scope's start)
    // and the watermark the sink recorded at its barrier, from its interval
    // evidence, which the bundle must hold.
    if (since) {
        LineageCheckpoint base;
        base.checkpoint = *since;
        if (!sc.source.from_complete || from.empty()) {
            return gap("checkpoint " + std::to_string(*since) +
                       "'s snapshots are not all in the bundle, so where the source stood at the "
                       "increment's base is not known (collect with --include-state)");
        }
        for (const auto& [k, offset] : from) {
            base.positions.push_back(SourceRange{k.first, k.second, std::nullopt, offset});
        }
        std::uint64_t subtasks = 0;
        std::uint64_t parallelism = 0;
        bool every = true;
        for (const auto& iv : b.sink_intervals) {
            if (iv.op != sc.sink_op || iv.epoch != *since) {
                continue;
            }
            IntervalEvidence e;
            try {
                e = read_interval(b, iv);
            } catch (const std::exception& ex) {
                return gap(iv.path + " cannot be read: " + ex.what());
            }
            ++subtasks;
            parallelism = e.parallelism;
            if (!e.watermark_ms) {
                every = false;
            } else if (!base.watermark_ms || *e.watermark_ms < *base.watermark_ms) {
                base.watermark_ms = e.watermark_ms;
            }
        }
        if (subtasks == 0 || subtasks != parallelism) {
            return gap(
                "the bundle holds the sink's interval evidence for the increment's base, "
                "checkpoint " +
                std::to_string(*since) + ", from " + std::to_string(subtasks) + " of " +
                std::to_string(parallelism) +
                " subtasks, so the watermark recorded there is not known (collect with "
                "--epochs reaching back to it)");
        }
        if (!every) {
            base.watermark_ms.reset();
        }
        sc.base = std::move(base);
    }
    // A windowed lineage: every completed checkpoint it passes, with where the
    // source stood (from that checkpoint's own snapshots) and the watermark
    // the sink had at its barrier, the least over its subtasks.
    if (lineage) {
        for (const auto ep : sc.epochs) {
            if (!completed.contains(ep)) {
                continue;
            }
            LineageCheckpoint lc;
            lc.checkpoint = ep;
            std::map<std::pair<std::uint64_t, std::int32_t>, std::int64_t> pos;
            if (ep == sc.checkpoint) {
                pos = to;
            } else {
                bool whole = false;
                std::map<std::uint64_t, SourceLog> logs;
                std::vector<std::string> notes;
                pos = kafka_positions(b, ep, whole, notes, logs);
                if (!whole || pos.empty()) {
                    return gap("checkpoint " + std::to_string(ep) +
                               "'s snapshots are not all in the bundle, so where the source "
                               "stood at it in the lineage is not known (collect with "
                               "--include-state)");
                }
                for (const auto& [op, log] : logs) {
                    const auto it = logs_to.find(op);
                    if (it == logs_to.end() || !(it->second == log)) {
                        return gap("checkpoint " + std::to_string(ep) + " records " +
                                   describe_log(log) + " for the source (operator " +
                                   std::to_string(op) + "), and checkpoint " +
                                   std::to_string(sc.checkpoint) +
                                   (it == logs_to.end() ? std::string{" none"}
                                                        : " " + describe_log(it->second)) +
                                   ": the lineage's positions are not of one log");
                    }
                }
            }
            for (const auto& [k, offset] : pos) {
                lc.positions.push_back(SourceRange{k.first, k.second, std::nullopt, offset});
            }
            bool every = true;
            for (const auto& e : sc.intervals) {
                if (e.epoch != ep) {
                    continue;
                }
                if (!e.watermark_ms) {
                    every = false;
                } else if (!lc.watermark_ms || *e.watermark_ms < *lc.watermark_ms) {
                    lc.watermark_ms = e.watermark_ms;
                }
            }
            if (!every) {
                lc.watermark_ms.reset();
            }
            sc.lineage.push_back(std::move(lc));
        }
        // Several partitions to a source subtask: its watermark is the least
        // over them only if it held for each one that had not delivered, which
        // a Worker that did records as the partitions its source declared. A
        // subtask that owns none marks its output idle and holds nothing.
        if (sc.window) {
            const std::uint64_t par = sc.window->source_parallelism;
            std::map<std::uint64_t, std::set<std::int32_t>> owned_by;
            for (const auto& [k, _] : to) {
                owned_by[par == 0 ? 0 : static_cast<std::uint64_t>(k.second) % par].insert(
                    k.second);
            }
            bool several = false;
            for (const auto& [sub, parts] : owned_by) {
                several = several || parts.size() > 1;
            }
            if (several) {
                // Every assigner of a Worker that holds writes its record at
                // every barrier, empty until its source declares partitions;
                // one that wrote none did not hold. A record that names
                // partitions must name every one its subtask owned there.
                std::string why;
                std::vector<LineageCheckpoint> held = sc.lineage;
                if (sc.base) {
                    held.insert(held.begin(), *sc.base);
                }
                for (const auto& lc : held) {
                    const auto declared = declared_partitions(b, lc.checkpoint);
                    for (const auto& [sub, _] : owned_by) {
                        const auto it = declared.find(static_cast<std::uint32_t>(sub));
                        if (it == declared.end()) {
                            if (why.empty()) {
                                why = "at checkpoint " + std::to_string(lc.checkpoint) +
                                      " the watermark of source subtask " + std::to_string(sub) +
                                      " records no partitions its source declared (a Worker "
                                      "that did not yet hold the watermark for a partition that "
                                      "had delivered nothing), and the source's subtasks read "
                                      "several partitions each, so the order of their first "
                                      "fetches could decide which records were late";
                            }
                            continue;
                        }
                        if (it->second.empty()) {
                            continue;  // no batch had reached it: it emitted no watermark
                        }
                        for (const auto& p : lc.positions) {
                            if (par != 0 && static_cast<std::uint64_t>(p.partition) % par == sub &&
                                !it->second.contains(p.partition) && why.empty()) {
                                why = "at checkpoint " + std::to_string(lc.checkpoint) +
                                      " the watermark of source subtask " + std::to_string(sub) +
                                      " was not waiting for partition " +
                                      std::to_string(p.partition) + ", which it owned";
                            }
                        }
                    }
                }
                if (!why.empty()) {
                    sc.window_unsupported = why;
                    sc.window.reset();
                }
            }
        }
    }
    if (b.manifest.contains("delivery")) {
        sc.delivery = b.manifest.at("delivery");
    }
    return sc;
}

// ---- the comparison -----------------------------------------------------------

namespace {

std::string key_of(const Row& row, const std::vector<std::size_t>& key) {
    std::string k;
    for (const auto i : key) {
        const auto& c = row.at(i);
        k += std::to_string(c.size());
        k += ':';
        k += c;
    }
    return k;
}

Discrepancy discrepancy(Discrepancy::Kind kind, Row key, Row expected, Row observed) {
    Discrepancy d;
    d.kind = kind;
    d.key = std::move(key);
    d.expected = std::move(expected);
    d.observed = std::move(observed);
    return d;
}

Row key_cells(const Row& row, const std::vector<std::size_t>& key) {
    Row out;
    for (const auto i : key) {
        out.push_back(row.at(i));
    }
    return out;
}

}  // namespace

Comparison compare(const std::vector<Row>& expected,
                   const std::vector<std::size_t>& key,
                   const std::vector<Row>& first,
                   const std::vector<Row>& second,
                   std::size_t list_limit) {
    Comparison c;
    std::map<std::string, const Row*> want;
    for (const auto& r : expected) {
        want.emplace(key_of(r, key), &r);
    }
    std::map<std::string, std::vector<Row>> one;
    std::map<std::string, std::vector<Row>> two;
    for (const auto& r : first) {
        one[key_of(r, key)].push_back(r);
    }
    for (const auto& r : second) {
        two[key_of(r, key)].push_back(r);
    }
    std::set<std::string> keys;
    for (const auto& [k, _] : want) {
        keys.insert(k);
    }
    for (const auto& [k, _] : one) {
        keys.insert(k);
    }
    for (const auto& [k, _] : two) {
        keys.insert(k);
    }
    const auto list = [&](Discrepancy d) {
        if (c.listed.size() < list_limit) {
            c.listed.push_back(std::move(d));
        }
    };
    for (const auto& k : keys) {
        std::vector<Row> o1 = one.contains(k) ? one[k] : std::vector<Row>{};
        std::vector<Row> o2 = two.contains(k) ? two[k] : std::vector<Row>{};
        std::sort(o1.begin(), o1.end());
        std::sort(o2.begin(), o2.end());
        const auto w = want.find(k);
        if (w != want.end()) {
            ++c.expected_rows;
        }
        if (o1 != o2) {
            ++c.unstable_keys;
            continue;
        }
        c.observed_rows += o1.size();
        if (w == want.end()) {
            for (const auto& r : o1) {
                ++c.unexpected;
                list(discrepancy(Discrepancy::Kind::Unexpected, key_cells(r, key), {}, r));
            }
            continue;
        }
        const Row& e = *w->second;
        const auto exact = static_cast<std::uint64_t>(std::count(o1.begin(), o1.end(), e));
        if (o1.empty()) {
            ++c.missing;
            list(discrepancy(Discrepancy::Kind::Missing, key_cells(e, key), e, {}));
            continue;
        }
        std::vector<Row> others;
        for (const auto& r : o1) {
            if (r != e) {
                others.push_back(r);
            }
        }
        if (exact == 0) {
            ++c.mismatched;
            Discrepancy d =
                discrepancy(Discrepancy::Kind::Mismatch, key_cells(e, key), e, others.front());
            for (std::size_t i = 0; i < e.size(); ++i) {
                if (i < others.front().size() && others.front()[i] != e[i]) {
                    d.differing.push_back(i);
                }
            }
            list(std::move(d));
            others.erase(others.begin());
        } else {
            ++c.matched;
            if (exact > 1) {
                c.duplicated += exact - 1;
                Discrepancy d = discrepancy(Discrepancy::Kind::Duplicate, key_cells(e, key), e, e);
                d.copies = exact - 1;
                list(std::move(d));
            }
        }
        for (const auto& r : others) {
            ++c.unexpected;
            list(discrepancy(Discrepancy::Kind::Unexpected, key_cells(r, key), {}, r));
        }
    }
    return c;
}

// ---- running it ---------------------------------------------------------------

namespace {

std::string utc_now() {
    const auto now = std::chrono::system_clock::now();
    const std::time_t t = std::chrono::system_clock::to_time_t(now);
    const auto ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count() %
        1000;
    std::tm tm{};
    gmtime_r(&t, &tm);
    // Room for any year an int holds, so the text is never cut.
    std::array<char, 96> buf{};
    std::snprintf(buf.data(),
                  buf.size(),
                  "%04d-%02d-%02dT%02d:%02d:%02d.%03lldZ",
                  tm.tm_year + 1900,
                  tm.tm_mon + 1,
                  tm.tm_mday,
                  tm.tm_hour,
                  tm.tm_min,
                  tm.tm_sec,
                  static_cast<long long>(ms));
    return buf.data();
}

Check make_check(std::string id) {
    return Check{.id = std::move(id),
                 .verdict = Verdict::Unavailable,
                 .evidence = {},
                 .reason = {},
                 .next_action = {},
                 .details = {},
                 .counts = {}};
}

std::string recorded(const config::JsonValue& dest, const char* key) {
    if (dest.is_object() && dest.contains(key) && dest.at(key).is_string()) {
        return dest.at(key).as_string();
    }
    return {};
}

std::string recorded_server(const config::JsonValue& dest, const char* key) {
    if (dest.is_object() && dest.contains("server")) {
        return recorded(dest.at("server"), key);
    }
    return {};
}

// What the evidence says about resends in the scope: rows the sink resent in
// doubt under a fresh token (which may land twice), and under its own token
// to a target that keeps a log (which the target drops).
struct Resends {
    std::uint64_t fresh_rows{0};
    std::uint64_t kept_rows{0};
    std::uint64_t inserts{0};
};

Resends resends_of(const Scope& sc) {
    Resends r;
    for (const auto& e : sc.intervals) {
        if (!e.sink.is_object() || !e.sink.contains("inserts") ||
            !e.sink.at("inserts").is_array()) {
            continue;
        }
        for (const auto& i : e.sink.at("inserts").as_array()) {
            ++r.inserts;
            if (!i.is_object() || !i.contains("resend") || !i.at("resend").is_string()) {
                continue;
            }
            const auto rows = i.contains("rows") && i.at("rows").is_integral_number()
                                  ? static_cast<std::uint64_t>(i.at("rows").as_int())
                                  : 0;
            (i.at("resend").as_string() == "may_duplicate" ? r.fresh_rows : r.kept_rows) += rows;
        }
    }
    return r;
}

std::string row_text(const Row& row, const std::vector<Column>& cols) {
    std::string out = "{";
    for (std::size_t i = 0; i < row.size() && i < cols.size(); ++i) {
        out += (i == 0 ? "" : ", ") + cols[i].name + "=" + display(row[i]);
    }
    return out + "}";
}

std::string key_text(const Row& key,
                     const std::vector<std::size_t>& idx,
                     const std::vector<Column>& cols) {
    std::string out;
    for (std::size_t i = 0; i < key.size() && i < idx.size(); ++i) {
        out += (i == 0 ? "" : ", ") + cols[idx[i]].name + "=" + display(key[i]);
    }
    return out;
}

}  // namespace

namespace {

// ---- the windows of an audit -------------------------------------------------------
//
// A window's row is owed once the watermark passes its end plus the allowed
// lateness. The recorded evidence of that is the watermark the sink had at
// each barrier (the least over its subtasks): a window operator forwards a
// watermark after firing what it closes, and with aligned checkpoints it has
// processed everything before a barrier, on every input, before the barrier
// passes. The source's records bound the same watermark from above: at a
// checkpoint, no partition can have advanced it past its greatest event time
// before its position, less the lag. The reference computes that bound
// independently; these checks hold the recorded evidence to it.

struct WindowJudgement {
    Check semantics = make_check("reference.semantics");
    Check watermark = make_check("reference.watermark");
    Check finality = make_check("reference.finality");
    std::set<std::string> undetermined;  // identity keys: compared nowhere
    std::set<std::string> open;          // identity keys: owed nothing yet
    std::vector<Row> captured;           // the captured rows the reference is compared with
    WindowAudit audit;
};

std::string cells_key(const Row& cells) {
    std::string k;
    for (const auto& c : cells) {
        k += std::to_string(c.size());
        k += ':';
        k += c;
    }
    return k;
}

std::optional<std::int64_t> integer_cell(const std::string& cell) {
    if (cell.size() < 2 || cell.front() != 'I') {
        return std::nullopt;
    }
    return as_i64(std::string_view{cell}.substr(1));
}

std::string ms_text(const std::optional<std::int64_t>& v) {
    return v ? std::to_string(*v) + " ms" : std::string{"none"};
}

WindowJudgement judge_windows(const Scope& sc,
                              const Reference& ref,
                              const std::vector<std::size_t>& key,
                              const config::JsonValue& document) {
    WindowJudgement j;
    const WindowSemantics& w = *sc.window;
    WindowAudit& a = j.audit;
    a.final_windows = ref.rows.size();
    a.reference_semantics = ref.semantics ? *ref.semantics : config::JsonValue{};
    a.records = ref.records;

    // The rules.
    a.semantics_differences = semantics_differences(
        document.at("transform"), ref.semantics ? *ref.semantics : config::JsonValue{});
    j.semantics.evidence = {"job/manifest.json", ref.path};
    if (a.semantics_differences.empty()) {
        j.semantics.verdict = Verdict::Passed;
        j.semantics.reason =
            "the reference implements the job's window rules: tumbling windows of " +
            std::to_string(w.size_ms) + " ms on " + w.time_column +
            ", the per-partition watermark " + std::to_string(w.lag_ms) +
            " ms behind, an allowed lateness of " + std::to_string(w.allowed_lateness_ms) +
            " ms, late records dropped";
    } else {
        j.semantics.verdict = Verdict::Failed;
        j.semantics.reason =
            "the job's window rules are not the reference's: " + a.semantics_differences.front();
        j.semantics.details = a.semantics_differences;
        j.semantics.next_action =
            "correct the job's window or source options, or the reference's specification";
    }

    // The windows left out.
    for (const auto& x : ref.windows) {
        (x.status == "open" ? j.open : j.undetermined).insert(cells_key(x.identity));
    }
    a.open_windows = j.open.size();
    a.undetermined_windows = j.undetermined.size();

    // The watermark: recorded against the source's bound at every lineage
    // checkpoint, and every window the sink received closed by the bound at
    // the checkpoint after it.
    std::map<std::uint64_t, std::optional<std::int64_t>> bound;
    for (const auto& b : ref.source_bounds) {
        bound[b.checkpoint] = b.watermark_ms;
    }
    std::vector<std::string> ahead;  // recorded watermarks past the source's bound
    std::uint64_t early = 0;         // windows received before the bound let them close
    std::size_t equal = 0;
    for (const auto& lc : sc.lineage) {
        const auto sb = bound.contains(lc.checkpoint) ? bound.at(lc.checkpoint) : std::nullopt;
        a.watermarks.push_back(WindowAudit::Mark{lc.checkpoint, lc.watermark_ms, sb});
        if (lc.watermark_ms && !sb) {
            ahead.push_back("at checkpoint " + std::to_string(lc.checkpoint) +
                            " the sink recorded a watermark of " + ms_text(lc.watermark_ms) +
                            " while a partition had delivered nothing, so the source's records "
                            "allow none");
        } else if (lc.watermark_ms && *lc.watermark_ms > *sb) {
            ahead.push_back("at checkpoint " + std::to_string(lc.checkpoint) +
                            " the sink's recorded watermark was " + ms_text(lc.watermark_ms) +
                            ", past the " + ms_text(sb) + " the source's records allow");
        }
        equal += lc.watermark_ms == sb ? 1 : 0;
    }
    std::size_t end_at = sc.columns.size();
    for (std::size_t i = 0; i < sc.columns.size(); ++i) {
        if (sc.columns[i].name == w.end_column) {
            end_at = i;
        }
    }
    const auto bound_after =
        [&](std::uint64_t epoch) -> std::optional<std::optional<std::int64_t>> {
        for (const auto& lc : sc.lineage) {
            if (lc.checkpoint >= epoch) {
                return bound.contains(lc.checkpoint) ? bound.at(lc.checkpoint) : std::nullopt;
            }
        }
        return std::nullopt;
    };
    for (std::size_t i = 0; i < sc.expected.size(); ++i) {
        const Row& row = sc.expected[i];
        const std::string k = key_of(row, key);
        const auto end = end_at < row.size() ? integer_cell(row[end_at]) : std::nullopt;
        const std::uint64_t epoch = i < sc.expected_epochs.size() ? sc.expected_epochs[i] : 0;
        const auto sb = bound_after(epoch);
        if (!end || !sb || !*sb || *end + w.allowed_lateness_ms > **sb) {
            if (a.early.size() < 20) {
                a.early.push_back(
                    "window " + key_text(key_cells(row, key), key, sc.columns) +
                    " was received in the interval ending at barrier " + std::to_string(epoch) +
                    ", and the watermark the source's records allow by the checkpoint after it " +
                    "is " + (sb ? ms_text(*sb) : std::string{"not known"}) + ", short of its end " +
                    (end ? std::to_string(*end) : std::string{"(unreadable)"}) +
                    " plus the allowed lateness " + std::to_string(w.allowed_lateness_ms) + " ms");
            }
            ++early;
        }
        if (j.undetermined.contains(k)) {
            ++a.captured_left_out;
        } else if (!j.open.contains(k)) {
            j.captured.push_back(row);
        }
    }
    j.watermark.evidence = {ref.path};
    j.watermark.counts = {{"checkpoints", sc.lineage.size()},
                          {"equal_to_bound", equal},
                          {"windows_received", sc.expected.size()},
                          {"closed_early", early}};
    if (j.semantics.verdict != Verdict::Passed) {
        j.watermark.verdict = Verdict::Unavailable;
        j.watermark.reason =
            "not checked: the reference computes the source's bound under other window rules than "
            "the job declares (reference.semantics)";
    } else if (!ahead.empty() || early > 0) {
        j.watermark.verdict = Verdict::Failed;
        j.watermark.reason = "the job's watermark ran ahead of what the source's records allow: " +
                             (ahead.empty() ? a.early.front() : ahead.front());
        if (early > 0) {
            ahead.push_back(std::to_string(early) +
                            " window row(s) were received before the source's records let them "
                            "close");
        }
        for (const auto& e : ahead) {
            j.watermark.details.push_back(e);
        }
        for (const auto& e : a.early) {
            j.watermark.details.push_back(e);
        }
        j.watermark.next_action =
            "inspect the watermark the job assigned: a window that closes early holds part of its "
            "records, and the rest are dropped as late";
    } else {
        j.watermark.verdict = Verdict::Passed;
        j.watermark.reason =
            "at each of the lineage's " + std::to_string(sc.lineage.size()) +
            " checkpoint(s) the sink's recorded watermark is within what the source's records "
            "allow (equal at " +
            std::to_string(equal) + "), and each of the " + std::to_string(sc.expected.size()) +
            " window row(s) the sink received had closed by the watermark the source allowed at "
            "the checkpoint after it";
    }

    // Finality: every window the reference computed is decided, and every row
    // it gives is of a window the recorded watermark had closed.
    const std::optional<std::int64_t> final_wm =
        sc.lineage.empty() ? std::nullopt : sc.lineage.back().watermark_ms;
    std::uint64_t not_final = 0;
    std::string first_not_final;
    // An increment owes only the windows that closed after its base: one the
    // base's recorded watermark had closed was owed, and received, before.
    const std::optional<std::int64_t> base_wm =
        sc.base ? sc.base->watermark_ms : std::optional<std::int64_t>{};
    std::uint64_t earlier = 0;
    std::string first_earlier;
    for (const auto& row : ref.rows) {
        const auto end = end_at < row.size() ? integer_cell(row[end_at]) : std::nullopt;
        if (!end || !final_wm || *end + w.allowed_lateness_ms > *final_wm) {
            if (not_final++ == 0) {
                first_not_final = key_text(key_cells(row, key), key, sc.columns);
            }
        } else if (base_wm && *end + w.allowed_lateness_ms <= *base_wm) {
            if (earlier++ == 0) {
                first_earlier = key_text(key_cells(row, key), key, sc.columns);
            }
        }
    }
    // A window left out as open is owed nothing only if the recorded
    // watermark had not closed it: one it had closed would be hidden from the
    // comparison.
    std::uint64_t not_open = 0;
    std::string first_not_open;
    for (const auto& x : ref.windows) {
        if (x.status == "open" && final_wm && x.end_ms + w.allowed_lateness_ms <= *final_wm) {
            if (not_open++ == 0) {
                first_not_open = key_text(x.identity, key, sc.columns);
            }
        }
    }
    j.finality.evidence = {ref.path};
    j.finality.counts = {{"final", ref.rows.size()},
                         {"open", a.open_windows},
                         {"undetermined", a.undetermined_windows},
                         {"captured_left_out", a.captured_left_out}};
    if (not_open > 0) {
        j.finality.verdict = Verdict::Failed;
        j.finality.reason = "the reference leaves out as open " + std::to_string(not_open) +
                            " window(s) the recorded watermark at checkpoint " +
                            std::to_string(sc.checkpoint) + " (" + ms_text(final_wm) +
                            ") had closed, such as " + first_not_open +
                            ", which keeps them from being compared";
        j.finality.next_action =
            "a window is open only while the recorded watermark is short of its end plus the "
            "allowed lateness; correct the reference";
    } else if (earlier > 0) {
        j.finality.verdict = Verdict::Failed;
        j.finality.reason = "the reference gives " + std::to_string(earlier) +
                            " row(s) for windows the watermark recorded at the increment's base, "
                            "checkpoint " +
                            std::to_string(sc.base->checkpoint) + " (" + ms_text(base_wm) +
                            "), had already closed, such as " + first_earlier +
                            ": an earlier increment owed them";
        j.finality.next_action =
            "an increment owes the windows that close after its base and by its checkpoint; "
            "correct the reference, or the state it started from";
    } else if (not_final > 0) {
        j.finality.verdict = Verdict::Failed;
        j.finality.reason = "the reference gives " + std::to_string(not_final) +
                            " row(s) for windows the recorded watermark at checkpoint " +
                            std::to_string(sc.checkpoint) + " (" + ms_text(final_wm) +
                            ") had not closed, such as " + first_not_final;
        j.finality.next_action =
            "a window is final at a checkpoint only once the watermark recorded there passes its "
            "end plus the allowed lateness; correct the reference";
    } else if (!ref.windows.empty() && a.undetermined_windows > 0) {
        j.finality.verdict = Verdict::Unavailable;
        std::string why;
        for (const auto& x : ref.windows) {
            if (x.status == "undetermined") {
                why = x.why;
                break;
            }
        }
        j.finality.reason = std::to_string(a.undetermined_windows) +
                            " window(s) are left out undecided, so the scope's windows are not all "
                            "compared: " +
                            why;
        j.finality.next_action =
            "audit a checkpoint where the evidence decides every window, or read the windows left "
            "out in the reference";
    } else {
        j.finality.verdict = Verdict::Passed;
        j.finality.reason =
            "every window the reference computed from the scope's records is decided: " +
            std::to_string(ref.rows.size()) + " final by the watermark recorded at checkpoint " +
            std::to_string(sc.checkpoint) + " (" + ms_text(final_wm) + ")" +
            (sc.base ? " and not by the increment's base, checkpoint " +
                           std::to_string(sc.base->checkpoint) + " (" + ms_text(base_wm) + ")"
                     : std::string{}) +
            ", " + std::to_string(a.open_windows) + " open and owed nothing yet";
    }
    return j;
}

// destination.coverage: the table read whole against the expected rows (the
// reference's when there is one, the captured input's otherwise). Established
// only for a scope that spans a run begun with no state, whose sink the bundle
// shows quiet after the audited checkpoint (two or more empty intervals of
// every subtask, none with rows), and a table whose parts did not change
// across the read; that nothing else writes the table is the operator's
// declaration, recorded as an assumption.
void audit_whole_table(const Bundle& b,
                       const Scope& sc,
                       const std::vector<std::size_t>& key,
                       Destination& dest,
                       const Options& o,
                       const std::vector<Row>* reference,
                       const std::set<std::string>* left_out,
                       const std::vector<std::string>& unsettled,
                       Check& check,
                       Report& r) {
    r.assumptions.push_back(
        "the operator declared the table dedicated to this one bounded run "
        "(--coverage=complete-table): nothing else writes it, and the run wrote nothing after the "
        "audited checkpoint that the bundle does not show");
    const auto unavailable = [&](std::string why, std::string next) {
        check.verdict = Verdict::Unavailable;
        check.reason = why;
        check.next_action = std::move(next);
        r.coverage.complete_reason = std::move(why);
    };
    if (!(sc.whole_run && sc.starts_at_run_start && sc.starts_after == 0)) {
        unavailable(
            sc.starts_after > 0
                ? "the scope begins where the run restored from checkpoint " +
                      std::to_string(sc.starts_after) +
                      ", so rows an earlier run wrote can be in the table and outside the scope"
                : "the scope does not span the job's writes from a run that began with no state",
            "audit a run that began with no state, from its start");
        return;
    }
    // The run had stopped writing: the bundle shows every sink subtask's
    // interval for at least two barriers after the checkpoint, all empty. A
    // run still receiving rows shows them, or has no such quiet stretch yet.
    const std::uint64_t parallelism = sc.intervals.front().parallelism;
    std::map<std::uint64_t, std::set<std::uint64_t>> quiet;  // epoch -> sink subtasks
    for (const auto& iv : b.sink_intervals) {
        if (iv.op != sc.sink_op || iv.epoch <= sc.checkpoint) {
            continue;
        }
        IntervalEvidence e;
        try {
            e = read_interval(b, iv);
        } catch (const std::exception& ex) {
            unavailable(iv.path + " cannot be read: " + ex.what(), "collect the bundle again");
            return;
        }
        if (e.records_seen > 0) {
            unavailable("the sink received " + std::to_string(e.records_seen) +
                            " row(s) in the interval ending at barrier " + std::to_string(e.epoch) +
                            ", after checkpoint " + std::to_string(sc.checkpoint) +
                            ": the run was still writing, and the table can hold rows the scope "
                            "does not",
                        "audit a checkpoint after the run's last rows, once two empty intervals "
                        "have followed it");
            return;
        }
        quiet[e.epoch].insert(e.sink_subtask);
    }
    std::size_t whole_quiet = 0;
    for (const auto& [epoch, subtasks] : quiet) {
        whole_quiet += subtasks.size() == parallelism ? 1 : 0;
    }
    if (whole_quiet < 2) {
        unavailable("the bundle shows " + std::to_string(whole_quiet) +
                        " empty interval(s) of every sink subtask after checkpoint " +
                        std::to_string(sc.checkpoint) +
                        "; two are needed to show the run had stopped writing, as a run still "
                        "writing can add rows the scope does not hold",
                    "audit a checkpoint two or more intervals before the bundle's last, after "
                    "the run's last rows");
        return;
    }
    if (!unsettled.empty()) {
        unavailable("a mutation of the table was in progress or began while it was read",
                    "run again once the table's mutations are done");
        return;
    }
    try {
        const std::string before = dest.parts_marker();
        std::vector<Row> all = dest.read_all(sc.columns, o.max_table_rows);
        const std::string after = dest.parts_marker();
        if (before != after) {
            unavailable(
                "the table's parts changed while it was read whole, so the read is not of one "
                "state of the table",
                "run again once nothing writes or merges the table");
            return;
        }
        r.coverage.table_rows_read = all.size();
        // Rows of windows the reference leaves undecided are not compared.
        if (left_out != nullptr && !left_out->empty()) {
            std::vector<Row> kept;
            std::uint64_t dropped = 0;
            for (auto& row : all) {
                if (left_out->contains(key_of(row, key))) {
                    ++dropped;
                } else {
                    kept.push_back(std::move(row));
                }
            }
            all = std::move(kept);
            if (r.window_audit) {
                r.window_audit->destination_left_out = dropped;
            }
            if (dropped > 0) {
                check.details.push_back(std::to_string(dropped) +
                                        " row(s) of windows the reference leaves undecided were "
                                        "read and not compared");
            }
        }
        const std::vector<Row>& want = reference != nullptr ? *reference : sc.expected;
        const std::string whose = reference != nullptr ? "the reference's" : "the captured input's";
        Comparison t = compare_multiset(want, key, all, all, o.list_limit);
        check.counts = {{"expected", t.expected_rows},
                        {"rows_read", all.size()},
                        {"matched", t.matched},
                        {"missing", t.missing},
                        {"changed", t.mismatched},
                        {"unexpected", t.unexpected},
                        {"duplicated", t.duplicated}};
        check.evidence.push_back(std::to_string(all.size()) + " row(s) read whole, parts " +
                                 (before.size() > 64 ? before.substr(0, 64) + "..." : before));
        r.coverage.complete_established = true;
        if (t.missing + t.mismatched + t.unexpected + t.duplicated > 0) {
            check.verdict = Verdict::Failed;
            check.reason =
                "the table, read whole (" + std::to_string(all.size()) +
                " row(s)), does not hold exactly " + whose + " " + std::to_string(want.size()) +
                " expected row(s): " + std::to_string(t.missing) + " missing, " +
                std::to_string(t.mismatched) + " changed, " + std::to_string(t.unexpected) +
                " unexpected, " + std::to_string(t.duplicated) + " duplicated";
            check.details.push_back(
                "unexpected rows here include rows of keys neither the reference nor the "
                "captured input names, which a selected-key read cannot see");
            check.next_action = "inspect the listed rows in the table";
        } else {
            check.verdict = Verdict::Passed;
            check.reason = "the table, read whole, holds exactly " + whose + " " +
                           std::to_string(want.size()) +
                           " expected row(s) and nothing else; its parts did not change across "
                           "the read, and the scope spans the run from a start with no state";
        }
        r.coverage.complete_reason = check.reason;
        r.table_comparison = std::move(t);
    } catch (const DestinationUnavailable& e) {
        unavailable(std::string{"the table could not be read whole: "} + e.what(),
                    "raise --max-table-rows, or audit a smaller run; part of a table is never "
                    "compared");
    }
}

}  // namespace

Report run(const fs::path& bundle_dir, const DestinationFactory& open, const Options& o) {
    Report r;
    r.bundle = bundle_dir.string();
    r.key = o.key;
    r.assumptions = {
        "the sink's captured input for the scope is what it was asked to write: the flight "
        "recorder takes each row before the sink does, at the same barriers",
        "every row the scope's intervals hold was acknowledged by the destination before the "
        "checkpoint completed, which is the native sink's barrier rule",
        "the key identifies one row the job writes, and no other checkpoint's interval writes "
        "the same key, so rows of other keys are outside the scope",
        "the table's content changes only by INSERT, merges that keep every row (MergeTree), and "
        "mutations, which are read from system.mutations",
    };
    Bundle b;
    try {
        b = read_bundle(bundle_dir);
    } catch (const BundleRefused& e) {
        r.refused = std::string{"the bundle cannot be read: "} + e.what();
        return r;
    }
    if (o.job && *o.job != b.job()) {
        r.refused = "wrong run: the bundle is evidence of job " + std::to_string(b.job()) +
                    ", not job " + std::to_string(*o.job);
        return r;
    }

    VerifyOptions vo;
    vo.profile = "integrity";
    bool intact = true;
    for (auto& c : verify_core(b, vo)) {
        intact = intact && c.verdict == Verdict::Passed;
        r.checks.push_back(std::move(c));
    }
    Check scope_check = make_check("reconcile.scope");
    Check dest_check = make_check("reconcile.destination");
    Check rows_check = make_check("reconcile.rows");
    // The audit's, when a reference is given or the whole table is asked for.
    const bool auditing = !o.reference.empty();
    const bool complete_table = o.coverage == Options::Coverage::CompleteTable;
    Check ref_scope_check = make_check("reference.scope");
    Check ref_capture_check = make_check("reference.capture");
    Check ref_dest_check = make_check("reference.destination");
    Check coverage_check = make_check("destination.coverage");
    // A windowed job's audit adds three: its window rules, its watermark and
    // the windows' finality.
    Check ref_semantics_check = make_check("reference.semantics");
    Check ref_watermark_check = make_check("reference.watermark");
    Check ref_finality_check = make_check("reference.finality");
    bool window_checks = false;
    r.coverage.mode = complete_table ? "complete-table" : "selected-keys";
    const auto finish = [&]() -> Report& {
        std::vector<Check*> order{&scope_check};
        if (auditing) {
            order.push_back(&ref_scope_check);
            if (window_checks) {
                order.push_back(&ref_semantics_check);
                order.push_back(&ref_watermark_check);
                order.push_back(&ref_finality_check);
            }
            order.push_back(&ref_capture_check);
        }
        order.push_back(&dest_check);
        order.push_back(&rows_check);
        if (auditing) {
            order.push_back(&ref_dest_check);
        }
        if (complete_table) {
            order.push_back(&coverage_check);
        }
        for (Check* c : order) {
            if (c->reason.empty()) {
                c->reason = "not run: a check it depends on did not pass";
            }
            r.checks.push_back(std::move(*c));
        }
        const auto counted = [&](const char* id) {
            std::string out;
            for (const auto& c : r.checks) {
                if (c.id == id) {
                    for (const auto& [name, n] : c.counts) {
                        if (n > 0 && name != "expected" && name != "observed" &&
                            name != "matched" && name != "queries" && name != "rows_read" &&
                            name != "unstable_keys") {
                            out += (out.empty() ? "" : ", ") + std::to_string(n) + " " + name;
                        }
                    }
                }
            }
            return out;
        };
        for (const auto& c : r.checks) {
            if (c.verdict != Verdict::Failed) {
                continue;
            }
            if (c.id == "reference.semantics") {
                r.findings.push_back(Finding{"transformation",
                                             "the job's window rules are not the ones the "
                                             "reference implements: " +
                                                 c.reason.substr(c.reason.find(": ") + 2)});
            } else if (c.id == "reference.watermark") {
                r.findings.push_back(Finding{
                    "transformation",
                    "the job's watermark ran ahead of what the source's records allow, so windows "
                    "closed before all their records could arrive"});
            } else if (c.id == "reference.finality") {
                r.findings.push_back(
                    Finding{"transformation",
                            "the reference and the recorded watermark disagree on which windows "
                            "are final: the reference is wrong"});
            } else if (c.id == "reference.capture") {
                r.findings.push_back(Finding{
                    "transformation",
                    "the job's sink received other rows than the independent reference computes "
                    "from the same source range (" +
                        counted("reference.capture") +
                        "): the transformation is wrong, or the "
                        "reference is"});
            } else if (c.id == "reconcile.rows") {
                r.findings.push_back(Finding{"delivery",
                                             "the destination does not hold what the sink "
                                             "received (" +
                                                 counted("reconcile.rows") + ")"});
            } else if (c.id == "reference.destination") {
                r.findings.push_back(Finding{"reference-destination",
                                             "the destination does not hold what the independent "
                                             "reference computes (" +
                                                 counted("reference.destination") + ")"});
            } else if (c.id == "destination.coverage") {
                r.findings.push_back(Finding{"coverage",
                                             "the dedicated table does not hold exactly the "
                                             "expected rows (" +
                                                 counted("destination.coverage") + ")"});
            }
        }
        return r;
    };
    if (!intact) {
        for (Check* c : {&scope_check, &dest_check, &rows_check}) {
            c->reason =
                "not run: the bundle's integrity checks did not pass, so its evidence is "
                "not read";
            c->next_action = "collect the bundle again";
        }
        return finish();
    }
    try {
        if (o.since && complete_table) {
            throw ScopeRefused(
                "an increment (--since) is of a job that is still writing its table, which "
                "--coverage=complete-table cannot read whole; read the increment's keys");
        }
        r.scope = load_scope(b,
                             o.checkpoint,
                             o.sink_op,
                             (o.from_run_start || complete_table) && !o.since,
                             auditing || complete_table,
                             o.since);
    } catch (const ScopeRefused& e) {
        r.refused = e.what();
        return r;
    }
    const Scope& sc = r.scope;
    window_checks = auditing && sc.window.has_value();

    // The key: columns the sink wrote, of a type a key can be read by. A
    // window's row is identified by its group key and its bounds.
    std::vector<std::size_t> key;
    std::vector<std::string> key_names = o.key;
    if (sc.window) {
        const std::set<std::string> want(sc.window->identity.begin(), sc.window->identity.end());
        const std::set<std::string> given(o.key.begin(), o.key.end());
        if (!o.key.empty() && given != want) {
            std::string id;
            for (const auto& c : sc.window->identity) {
                id += (id.empty() ? "" : ",") + c;
            }
            r.refused = "--key names other columns than the window's identity (" + id +
                        "): a windowed job's row is identified by its group key and the window's "
                        "start and end";
            return r;
        }
        key_names = sc.window->identity;
        r.key = key_names;
    }
    if (key_names.empty()) {
        r.refused = "--key is required: the columns that identify one row the job writes";
        return r;
    }
    for (const auto& name : key_names) {
        std::size_t idx = sc.columns.size();
        for (std::size_t i = 0; i < sc.columns.size(); ++i) {
            if (sc.columns[i].name == name) {
                idx = i;
            }
        }
        if (idx == sc.columns.size()) {
            if (sc.gap && sc.columns.empty()) {
                break;  // the scope's own gap says why there are no columns
            }
            r.refused = "--key names " + name + ", which is not a column the sink wrote";
            return r;
        }
        const auto k = sc.columns[idx].type.kind;
        if (k != TypeKind::Int && k != TypeKind::UInt && k != TypeKind::String) {
            r.refused = "--key names " + name + " (" + sc.columns[idx].type.spelling +
                        "); a key column must be an integer or String column";
            return r;
        }
        if (sc.columns[idx].type.nullable) {
            r.refused = "--key names " + name + ", which is Nullable; a key column must not be";
            return r;
        }
        key.push_back(idx);
    }

    // The scope.
    scope_check.evidence.push_back(completed_marker_path(sc.job, sc.checkpoint));
    for (const auto& e : sc.intervals) {
        scope_check.evidence.push_back(e.path);
    }
    std::string epochs;
    for (const auto e : sc.epochs) {
        epochs += (epochs.empty() ? "" : ", ") + std::to_string(e);
    }
    scope_check.details.push_back("sink " + std::to_string(sc.sink_op) +
                                  ", intervals ending at barrier(s) " + epochs + ", after " +
                                  (sc.starts_after == 0 && sc.starts_at_run_start
                                       ? std::string{"the start of the run"}
                                       : "checkpoint " + std::to_string(sc.starts_after)));
    for (const auto& n : sc.notes) {
        scope_check.details.push_back(n);
    }
    if (sc.gap) {
        scope_check.verdict = sc.gap_unsupported ? Verdict::Unsupported : Verdict::Unavailable;
        scope_check.reason = *sc.gap;
        scope_check.next_action =
            sc.gap_unsupported ? "" : "collect the bundle again with the evidence named";
    } else {
        // A window is owed once, so a window's identity received twice is the
        // transformation's fault, which the comparisons count; a record-wise
        // key received twice cannot pair rows at all.
        std::set<std::string> seen;
        std::string dup;
        for (const auto& row : sc.expected) {
            if (!seen.insert(key_of(row, key)).second && dup.empty() && !sc.window) {
                dup = key_text(key_cells(row, key), key, sc.columns);
            }
        }
        if (!dup.empty()) {
            scope_check.verdict = Verdict::Unsupported;
            scope_check.reason = "the key does not identify one row of the scope: " + dup +
                                 " is received more than once, so rows cannot be paired by it";
            scope_check.next_action = "name key columns that identify one row the job writes";
        } else {
            scope_check.verdict = Verdict::Passed;
            scope_check.reason =
                "the sink's input for checkpoint " + std::to_string(sc.checkpoint) +
                " is in the bundle whole: " + std::to_string(sc.expected.size()) + " row(s) over " +
                std::to_string(sc.intervals.size()) + " subtask interval(s), none truncated";
        }
    }
    scope_check.counts = {{"intervals", sc.intervals.size()},
                          {"expected_rows", sc.expected.size()}};

    // The reference: bound to this scope, whole, and complete by its own
    // account, or refused. Nothing it names is run or read.
    std::optional<Reference> ref;
    std::optional<WindowJudgement> wj;
    if (auditing) {
        r.reference_path = o.reference;
        r.assumptions.push_back(
            "the reference implementation computes the transformation independently of the job: "
            "the audit establishes that the artifact is whole and bound to this scope, not who "
            "wrote it or how");
        if (sc.window_unsupported) {
            ref_scope_check.verdict = Verdict::Unsupported;
            ref_scope_check.reason =
                "the job keeps state across records in a form the audit does not support: " +
                *sc.window_unsupported;
            ref_scope_check.next_action =
                "check the delivery alone, with incident-reconcile and no reference";
        } else if (sc.columns.empty()) {
            ref_scope_check.reason =
                "not read: the scope names no columns, so the reference has nothing to be bound to";
        } else {
            r.scope_document = scope_document(b, sc);
            try {
                ref = read_reference(
                    o.reference, *r.scope_document, sc.columns, o.max_reference_bytes);
            } catch (const ReferenceRefused& e) {
                r.refused = std::string{"the reference cannot be audited: "} + e.what();
                return r;
            }
            r.reference_complete = ref->complete;
            r.reference_incomplete = ref->incomplete;
            r.reference_implementation = ref->implementation;
            r.reference_rows_sha256 = ref->rows_sha256;
            r.reference_rows = ref->rows.size();
            r.reference_recorded = ref->recorded;
            r.reference_writer_verified = ref->writer_verified;
            r.reference_verified = ref->verified;
            ref_scope_check.evidence.push_back(o.reference);
            ref_scope_check.counts = {{"rows", ref->rows.size()}};
            for (const auto& v : ref->verified) {
                ref_scope_check.details.push_back("verified: " + v);
            }
            for (const auto& v : ref->writer_verified) {
                ref_scope_check.details.push_back("its writer says it checked: " + v);
            }
            for (const auto& v : ref->recorded) {
                ref_scope_check.details.push_back("recorded, not established: " + v);
            }
            const bool sources_whole = r.scope_document->bool_or("sources_complete", false);
            if (sc.gap) {
                ref_scope_check.verdict =
                    sc.gap_unsupported ? Verdict::Unsupported : Verdict::Unavailable;
                ref_scope_check.reason =
                    "the scope could not be established whole, so the reference is not compared";
            } else if (!ref->complete) {
                std::string why;
                for (const auto& w : ref->incomplete) {
                    why += (why.empty() ? "" : "; ") + w;
                }
                ref_scope_check.reason = "the reference is incomplete by its own account: " + why;
                ref_scope_check.next_action =
                    "compute the reference again once the whole source range can be read; an "
                    "incomplete reference is never compared";
            } else if (!sources_whole) {
                ref_scope_check.reason =
                    "the scope does not record every source range and log, so no reference can "
                    "be complete for it, whatever its writer says";
                ref_scope_check.next_action = "collect the bundle with --include-state";
            } else {
                ref_scope_check.verdict = Verdict::Passed;
                ref_scope_check.reason =
                    "the reference is bound to this scope, whole, and complete by its own "
                    "account: " +
                    std::to_string(ref->rows.size()) + " row(s) from " + ref->implementation;
            }
        }
        if (window_checks && ref_scope_check.verdict == Verdict::Passed &&
            scope_check.verdict == Verdict::Passed) {
            wj = judge_windows(sc, *ref, key, *r.scope_document);
            ref_semantics_check = wj->semantics;
            ref_watermark_check = wj->watermark;
            ref_finality_check = wj->finality;
            r.window_audit = wj->audit;
        } else if (window_checks) {
            for (Check* c : {&ref_semantics_check, &ref_watermark_check, &ref_finality_check}) {
                c->verdict = ref_scope_check.verdict == Verdict::Unsupported ||
                                     scope_check.verdict == Verdict::Unsupported
                                 ? Verdict::Unsupported
                                 : Verdict::Unavailable;
                c->reason = "not run: the scope or the reference could not be established";
            }
        }
        if (ref_scope_check.verdict == Verdict::Passed && scope_check.verdict == Verdict::Passed) {
            // A windowed job's capture is compared without the windows the
            // reference leaves out: open ones are owed nothing yet (one
            // received is reference.watermark's), undetermined ones are not
            // decided.
            const std::vector<Row>& captured = wj ? wj->captured : sc.expected;
            Comparison rc = compare_multiset(ref->rows, key, captured, captured, o.list_limit);
            ref_capture_check.counts = {{"expected", rc.expected_rows},
                                        {"observed", rc.observed_rows},
                                        {"matched", rc.matched},
                                        {"missing", rc.missing},
                                        {"changed", rc.mismatched},
                                        {"unexpected", rc.unexpected},
                                        {"duplicated", rc.duplicated}};
            if (rc.missing + rc.mismatched + rc.unexpected + rc.duplicated > 0) {
                ref_capture_check.verdict = Verdict::Failed;
                ref_capture_check.reason =
                    "the job's sink received other rows than the reference computes from the same "
                    "source range: " +
                    std::to_string(rc.missing) + " missing, " + std::to_string(rc.mismatched) +
                    " changed, " + std::to_string(rc.unexpected) + " unexpected, " +
                    std::to_string(rc.duplicated) + " duplicated";
                ref_capture_check.details.push_back(
                    "missing rows the reference computes and the job did not produce; changed, a "
                    "row under the key with other values; unexpected, rows the job produced and "
                    "the reference does not; duplicated, further copies of a reference row");
                ref_capture_check.details.push_back(
                    "this is the transformation, not the delivery: the comparison is of what the "
                    "sink received, before any write");
                ref_capture_check.next_action =
                    "inspect the listed rows against the source records the scope's ranges name";
            } else {
                ref_capture_check.verdict = Verdict::Passed;
                ref_capture_check.reason =
                    "the sink received exactly the " + std::to_string(rc.expected_rows) +
                    " row(s) the reference computes from the scope's source range, value for "
                    "value and copy for copy";
            }
            if (wj) {
                ref_capture_check.details.push_back(
                    "compared over the reference's " + std::to_string(wj->audit.final_windows) +
                    " final window(s); " + std::to_string(wj->audit.open_windows) + " open and " +
                    std::to_string(wj->audit.undetermined_windows) +
                    " undetermined window(s) left out, the sink having received " +
                    std::to_string(wj->audit.captured_left_out) + " row(s) of undetermined ones");
            }
            r.reference_capture = std::move(rc);
        } else if (ref_capture_check.reason.empty()) {
            ref_capture_check.verdict = scope_check.verdict == Verdict::Unsupported ||
                                                ref_scope_check.verdict == Verdict::Unsupported
                                            ? Verdict::Unsupported
                                            : Verdict::Unavailable;
            ref_capture_check.reason =
                "not compared: the scope or the reference could not be established";
        }
    }

    // The destination: the one the evidence names, in a form the comparison
    // supports.
    const config::JsonValue& rec = sc.destination;
    const std::string want_db = recorded(rec, "database");
    const std::string want_table = recorded(rec, "table");
    if (want_db.empty() || want_table.empty()) {
        if (sc.gap) {
            dest_check.reason = "not read: the scope does not name a destination";
            rows_check.reason = "not compared: the scope could not be established";
            return finish();
        }
        r.refused = "the evidence does not name the destination's database and table";
        return r;
    }
    if ((!o.database.empty() && o.database != want_db) ||
        (!o.table.empty() && o.table != want_table)) {
        r.refused = "wrong destination: the evidence is of " + want_db + "." + want_table +
                    ", not " + (o.database.empty() ? want_db : o.database) + "." +
                    (o.table.empty() ? want_table : o.table);
        return r;
    }
    std::unique_ptr<Destination> owned;
    try {
        owned = open(want_db, want_table);
    } catch (const std::exception& e) {
        r.refused = std::string{"the destination cannot be opened: "} + e.what();
        return r;
    }
    if (!owned) {
        r.refused = "the destination cannot be opened";
        return r;
    }
    Destination& dest = *owned;
    DestinationDescription d;
    try {
        d = dest.describe();
    } catch (const DestinationUnavailable& e) {
        dest_check.reason = std::string{"the destination could not be read: "} + e.what();
        dest_check.next_action = "check the connection and the user's read access, then run again";
        rows_check.reason = "not run: the destination could not be read";
        return finish();
    }
    r.destination = d;
    {
        const std::string want_server = recorded_server(rec, "uuid");
        const std::string want_uuid = recorded(rec, "table_uuid");
        if (want_server.empty() || want_uuid.empty()) {
            r.refused =
                "the evidence does not record the destination's server and table "
                "identity, so no destination can be checked against it";
            return r;
        }
        if (d.server_uuid != want_server) {
            r.refused = "wrong destination: the server's UUID is " + d.server_uuid +
                        ", and the evidence was written to server " + want_server + " (" +
                        recorded_server(rec, "host_name") + ")";
            return r;
        }
        if (d.database != want_db || d.table != want_table || d.table_uuid != want_uuid) {
            r.refused = "wrong destination: " + d.database + "." + d.table + " has UUID " +
                        d.table_uuid + ", and the evidence was written to " + want_db + "." +
                        want_table + " with UUID " + want_uuid;
            return r;
        }
    }
    dest_check.evidence.push_back(d.database + "." + d.table + " (" + d.table_uuid + ") on " +
                                  d.host_name + " " + d.version + " (" + d.server_uuid + ")");
    std::vector<std::string> form_problems;
    bool form_unsupported = false;
    if (d.engine != "MergeTree") {
        form_unsupported = true;
        form_problems.push_back(
            "the table's engine is " + d.engine +
            "; this release reconciles a plain MergeTree, whose merges keep every row and which "
            "has no replicas to lag behind one another");
    }
    if (d.create_table_query.find(" TTL ") != std::string::npos ||
        d.create_table_query.find("\nTTL ") != std::string::npos) {
        form_unsupported = true;
        form_problems.push_back("the table has a TTL, which can remove the scope's rows by itself");
    }
    for (const auto& col : sc.columns) {
        if (col.type.kind == TypeKind::Unsupported) {
            form_unsupported = true;
            form_problems.push_back("column " + col.name + ": " + col.type.unsupported_reason);
        }
        const auto live = std::find_if(
            d.columns.begin(), d.columns.end(), [&](const auto& c) { return c.first == col.name; });
        if (live == d.columns.end()) {
            form_problems.push_back("column " + col.name +
                                    " the sink wrote is gone from the table");
        } else if (live->second != col.type.spelling) {
            form_problems.push_back("column " + col.name + " is now " + live->second +
                                    ", and was " + col.type.spelling + " when the sink wrote it");
        }
    }
    if (form_problems.empty()) {
        dest_check.verdict = Verdict::Passed;
        dest_check.reason =
            "the destination is the one the evidence names (server and table "
            "UUIDs), a MergeTree without TTL, with every column the sink wrote "
            "as it was";
    } else {
        dest_check.verdict = form_unsupported ? Verdict::Unsupported : Verdict::Unavailable;
        dest_check.reason = form_problems.front();
        for (std::size_t i = 1; i < form_problems.size(); ++i) {
            dest_check.details.push_back(form_problems[i]);
        }
        dest_check.next_action =
            form_unsupported ? "" : "reconcile before the table changes, or not at all";
    }

    if (scope_check.verdict != Verdict::Passed || dest_check.verdict != Verdict::Passed) {
        rows_check.verdict = scope_check.verdict == Verdict::Unsupported ||
                                     dest_check.verdict == Verdict::Unsupported
                                 ? Verdict::Unsupported
                                 : Verdict::Unavailable;
        rows_check.reason = "not compared: the scope or the destination check did not pass";
        return finish();
    }

    // Two reads of every key, with the table's mutations around them: the
    // captured input's keys, and with a reference the reference's too, so a
    // row the job dropped is looked for and one it introduced is read.
    const bool ref_usable = ref && ref_scope_check.verdict == Verdict::Passed;
    std::vector<Row> keys;
    std::set<std::string> capture_keys;
    {
        std::set<std::string> seen;
        for (const auto& row : sc.expected) {
            capture_keys.insert(key_of(row, key));
            if (seen.insert(key_of(row, key)).second) {
                keys.push_back(key_cells(row, key));
            }
        }
        if (ref_usable) {
            for (const auto& row : ref->rows) {
                if (seen.insert(key_of(row, key)).second) {
                    keys.push_back(key_cells(row, key));
                }
            }
        }
    }
    r.coverage.keys_selected = keys.size();
    std::vector<Row> first;
    std::vector<Row> second;
    std::vector<Mutation> before;
    std::vector<Mutation> after;
    std::uint64_t queries = 0;
    const std::size_t per = std::max<std::size_t>(o.bounds.keys_per_query, 1);
    try {
        before = dest.mutations();
        r.observed_from = utc_now();
        r.server_time_from = d.now;
        for (int pass = 0; pass < 2; ++pass) {
            auto& into = pass == 0 ? first : second;
            for (std::size_t i = 0; i < keys.size(); i += per) {
                const std::vector<Row> chunk(
                    keys.begin() + static_cast<std::ptrdiff_t>(i),
                    keys.begin() + static_cast<std::ptrdiff_t>(std::min(keys.size(), i + per)));
                auto got = dest.read(sc.columns, key, chunk);
                ++queries;
                into.insert(into.end(),
                            std::make_move_iterator(got.begin()),
                            std::make_move_iterator(got.end()));
            }
            if (pass == 0 && o.settle.count() > 0) {
                std::this_thread::sleep_for(o.settle);
            }
        }
        after = dest.mutations();
        const DestinationDescription again = dest.describe();
        r.observed_to = utc_now();
        r.server_time_to = again.now;
        if (again.table_uuid != d.table_uuid ||
            again.metadata_modification_time != d.metadata_modification_time) {
            rows_check.reason =
                "the table changed while it was read (its metadata or identity), "
                "so no comparison is stable";
            rows_check.next_action = "run again once the table is no longer being altered";
            return finish();
        }
    } catch (const DestinationUnavailable& e) {
        r.observed_to = utc_now();
        rows_check.reason = std::string{"the destination could not be read whole: "} + e.what();
        rows_check.next_action =
            "raise the bound named, or narrow the read, and run again; part of a read is never "
            "compared";
        return finish();
    }
    std::set<std::string> before_ids;
    for (const auto& m : before) {
        before_ids.insert(m.id);
        if (m.done) {
            r.mutations_noted.push_back(m.id + " (" + m.created + "): " + m.command);
        }
    }
    std::vector<std::string> unsettled;
    for (const auto& m : before) {
        if (!m.done) {
            unsettled.push_back(m.id + " is in progress: " + m.command);
        }
    }
    for (const auto& m : after) {
        if (!before_ids.contains(m.id)) {
            unsettled.push_back(m.id + " began while the table was read: " + m.command);
        } else if (!m.done) {
            bool was_listed = false;
            for (const auto& u : unsettled) {
                was_listed = was_listed || u.rfind(m.id + " ", 0) == 0;
            }
            if (!was_listed) {
                unsettled.push_back(m.id + " is in progress: " + m.command);
            }
        }
    }

    // The captured input against the destination, over the captured keys only,
    // as without a reference.
    const auto of_capture = [&](const std::vector<Row>& rows) {
        std::vector<Row> out;
        for (const auto& row : rows) {
            if (capture_keys.contains(key_of(row, key))) {
                out.push_back(row);
            }
        }
        return out;
    };
    // A windowed job's identities are compared as multisets: a window the job
    // emitted twice is counted, not refused.
    Comparison c =
        sc.window ? compare_multiset(
                        sc.expected, key, of_capture(first), of_capture(second), o.list_limit)
                  : compare(sc.expected, key, of_capture(first), of_capture(second), o.list_limit);
    const bool empty_scope = sc.expected.empty();

    // The whole table, when the operator declares it holds this one bounded
    // run's writes and nothing else.
    if (complete_table) {
        audit_whole_table(b,
                          sc,
                          key,
                          dest,
                          o,
                          ref_usable ? &ref->rows : nullptr,
                          wj ? &wj->undetermined : nullptr,
                          unsettled,
                          coverage_check,
                          r);
    }
    rows_check.counts = {{"expected", c.expected_rows},
                         {"observed", c.observed_rows},
                         {"matched", c.matched},
                         {"missing", c.missing},
                         {"changed", c.mismatched},
                         {"unexpected", c.unexpected},
                         {"duplicated", c.duplicated},
                         {"unstable_keys", c.unstable_keys},
                         {"queries", queries}};
    const std::uint64_t broken = c.missing + c.mismatched + c.unexpected;
    if (broken + c.duplicated > 0) {
        rows_check.verdict = Verdict::Failed;
        rows_check.reason =
            "the destination does not hold the scope's rows exactly: " + std::to_string(c.missing) +
            " missing, " + std::to_string(c.mismatched) + " changed, " +
            std::to_string(c.unexpected) + " unexpected, " + std::to_string(c.duplicated) +
            " duplicated";
        if (broken > 0) {
            rows_check.details.push_back(
                "missing, changed and unexpected rows are outside the sink's at-least-once "
                "guarantee: the destination lost or altered what the sink wrote, or holds rows "
                "the job did not write for these keys");
        }
        if (c.duplicated > 0) {
            rows_check.details.push_back(
                "duplicates are within the sink's at-least-once guarantee, but the destination "
                "does not hold the scope's effects exactly once");
            const Resends rs = resends_of(sc);
            if (rs.fresh_rows > 0) {
                rows_check.details.push_back(
                    "the evidence shows " + std::to_string(rs.fresh_rows) +
                    " row(s) the sink resent after a send left them in doubt, under a fresh "
                    "token or to a target that keeps no deduplication log, which can land twice");
            }
            if (sc.starts_at_run_start && sc.starts_after > 0) {
                rows_check.details.push_back(
                    "the scope begins where the run restored from checkpoint " +
                    std::to_string(sc.starts_after) +
                    ": rows the discarded run wrote after it were written again by the restored "
                    "run, which at-least-once delivery allows");
            }
            for (const auto& rs_ : sc.restores) {
                rows_check.details.push_back(
                    "the lineage passes the restore from checkpoint " +
                    std::to_string(rs_.restored_from) + " (the run numbering from barrier " +
                    std::to_string(rs_.first_epoch) +
                    "): rows the discarded run wrote after that checkpoint were written again by "
                    "the restored run, which at-least-once delivery allows");
            }
            if (rs.fresh_rows == 0 && !(sc.starts_at_run_start && sc.starts_after > 0) &&
                sc.restores.empty()) {
                rows_check.details.push_back(
                    "the evidence records no resend in doubt and no restore that would explain "
                    "them");
            }
        }
        if (c.unstable_keys > 0) {
            rows_check.details.push_back(std::to_string(c.unstable_keys) +
                                         " key(s) read differently twice were not compared");
        }
        rows_check.next_action =
            "inspect the listed rows in the destination; the bundle holds "
            "what the sink received for them";
    } else if (!unsettled.empty()) {
        rows_check.reason =
            "a mutation of the table was in progress or began while it was read, "
            "so the comparison is not stable";
        rows_check.details = unsettled;
        rows_check.next_action = "run again once the table's mutations are done";
    } else if (empty_scope && coverage_check.verdict == Verdict::Passed) {
        rows_check.verdict = Verdict::Passed;
        rows_check.reason =
            "the scope's intervals held no rows, and the whole table, read under "
            "destination.coverage, holds none of its own either";
    } else if (empty_scope) {
        rows_check.reason =
            "the scope's intervals held no rows, so there was nothing to compare and the "
            "destination is not established either way";
        rows_check.next_action =
            "reconcile a checkpoint whose intervals carried rows, or read a dedicated table whole "
            "(--coverage=complete-table)";
    } else if (c.unstable_keys > 0) {
        rows_check.reason = std::to_string(c.unstable_keys) +
                            " key(s) of the scope were read differently twice: rows of the "
                            "scope were being written while it was read";
        rows_check.next_action = "run again once nothing writes the scope's keys";
    } else {
        rows_check.verdict = Verdict::Passed;
        rows_check.reason = "the destination holds exactly the " + std::to_string(c.expected_rows) +
                            " row(s) the sink received for checkpoint " +
                            std::to_string(sc.checkpoint) +
                            ", each once and value for value, and two reads agreed";
        const Resends rs = resends_of(sc);
        if (rs.kept_rows > 0) {
            rows_check.details.push_back(
                std::to_string(rs.kept_rows) +
                " row(s) were resent after a send left them in doubt, under their own token to a "
                "target that keeps a deduplication log; none is duplicated");
        }
    }
    for (const auto& m : r.mutations_noted) {
        rows_check.details.push_back("mutation done before the read: " + m);
    }
    r.comparison = std::move(c);

    // The reference against the destination, over the union of keys.
    if (auditing) {
        if (!ref_usable) {
            ref_dest_check.verdict = ref_scope_check.verdict == Verdict::Unsupported
                                         ? Verdict::Unsupported
                                         : Verdict::Unavailable;
            ref_dest_check.reason = "not compared: the reference could not be established";
        } else {
            // Without the windows the reference leaves out: an open window's
            // rows are owed nothing yet, an undetermined one's are not decided.
            const auto decided = [&](const std::vector<Row>& rows) {
                if (!wj) {
                    return rows;
                }
                std::vector<Row> out;
                for (const auto& row : rows) {
                    const auto k = key_of(row, key);
                    if (!wj->undetermined.contains(k) && !wj->open.contains(k)) {
                        out.push_back(row);
                    }
                }
                return out;
            };
            Comparison rd =
                compare_multiset(ref->rows, key, decided(first), decided(second), o.list_limit);
            ref_dest_check.counts = {{"expected", rd.expected_rows},
                                     {"observed", rd.observed_rows},
                                     {"matched", rd.matched},
                                     {"missing", rd.missing},
                                     {"changed", rd.mismatched},
                                     {"unexpected", rd.unexpected},
                                     {"duplicated", rd.duplicated},
                                     {"unstable_keys", rd.unstable_keys},
                                     {"keys_selected", keys.size()}};
            if (rd.missing + rd.mismatched + rd.unexpected + rd.duplicated > 0) {
                ref_dest_check.verdict = Verdict::Failed;
                ref_dest_check.reason =
                    "the destination does not hold what the reference computes, over the " +
                    std::to_string(keys.size()) +
                    " key(s) of the reference and the captured "
                    "input: " +
                    std::to_string(rd.missing) + " missing, " + std::to_string(rd.mismatched) +
                    " changed, " + std::to_string(rd.unexpected) + " unexpected, " +
                    std::to_string(rd.duplicated) + " duplicated";
                ref_dest_check.next_action =
                    "read reference.capture and reconcile.rows: one names the transformation, "
                    "the other the delivery";
            } else if (!unsettled.empty()) {
                ref_dest_check.reason =
                    "a mutation of the table was in progress or began while it was read";
            } else if (keys.empty() && coverage_check.verdict == Verdict::Passed) {
                ref_dest_check.verdict = Verdict::Passed;
                ref_dest_check.reason =
                    "the reference and the captured input are both empty, and the whole table, "
                    "read under destination.coverage, is empty as well";
            } else if (keys.empty()) {
                ref_dest_check.reason =
                    "the reference and the captured input are both empty, so no key is selected "
                    "and whether the destination is empty for the scope is not established";
                ref_dest_check.next_action =
                    "read a dedicated table whole (--coverage=complete-table)";
            } else if (rd.unstable_keys > 0) {
                ref_dest_check.reason =
                    std::to_string(rd.unstable_keys) + " key(s) were read differently twice";
                ref_dest_check.next_action = "run again once nothing writes the scope's keys";
            } else {
                ref_dest_check.verdict = Verdict::Passed;
                ref_dest_check.reason =
                    "the destination holds exactly the " + std::to_string(rd.expected_rows) +
                    " row(s) the reference computes, over its " + std::to_string(keys.size()) +
                    " selected key(s); rows of other keys were " +
                    (coverage_check.verdict == Verdict::Passed ? std::string{"read whole under "
                                                                             "destination.coverage"}
                                                               : std::string{"not read"});
            }
            r.reference_destination = std::move(rd);
        }
    }
    if (!complete_table) {
        r.coverage.complete_reason =
            "the read selected " + std::to_string(keys.size()) +
            " key(s); whether the table holds rows of other keys is not established, which "
            "needs a table dedicated to one bounded run read whole (--coverage=complete-table)";
    }
    return finish();
}

int Report::exit_code() const {
    if (refused) {
        return 2;
    }
    bool partial = false;
    for (const auto& c : checks) {
        if (c.verdict == Verdict::Failed) {
            return 1;
        }
        partial = partial || c.verdict != Verdict::Passed;
    }
    return partial || checks.empty() ? 3 : 0;
}

namespace {

const char* result_of(int code) {
    switch (code) {
        case 0:
            return "satisfied";
        case 1:
            return "failed";
        case 2:
            return "refused";
        default:
            return "partial";
    }
}

config::JsonValue row_json(const Row& row, const std::vector<Column>& cols) {
    config::JsonObject o;
    for (std::size_t i = 0; i < row.size() && i < cols.size(); ++i) {
        o[cols[i].name] = config::JsonValue{display(row[i])};
    }
    return config::JsonValue{std::move(o)};
}

}  // namespace

std::string Report::render_text() const {
    std::ostringstream out;
    out << "bundle:      " << bundle << "\n";
    if (refused) {
        out << "\nrefused: " << *refused << "\n\nresult: refused (exit 2)\n";
        return out.str();
    }
    out << "scope:       job " << scope.job << ", checkpoint " << scope.checkpoint << ", sink "
        << scope.sink_op << "\n";
    if (destination) {
        out << "destination: " << destination->database << "." << destination->table << " on "
            << destination->host_name << " " << destination->version << "\n";
    }
    std::string k;
    for (const auto& name : key) {
        k += (k.empty() ? "" : ", ") + name;
    }
    out << "key:         " << k << "\n";
    if (reference_path) {
        out << "reference:   " << *reference_path;
        if (!reference_implementation.empty()) {
            out << ", " << reference_implementation << ", " << reference_rows << " row(s), "
                << (reference_complete ? "complete" : "incomplete") << " by its own account";
        }
        out << "\n";
    }
    if (!observed_from.empty()) {
        out << "observed:    " << observed_from << " to " << observed_to << " (server "
            << server_time_from << " to " << server_time_to << ")\n";
    }
    out << "method:      every expected row against every row of its key, read twice, compared "
           "cell by cell as typed values\n";
    if (!scope.source.ranges.empty() || !scope.source.topics.empty()) {
        out << "source:      ";
        for (std::size_t i = 0; i < scope.source.topics.size(); ++i) {
            out << (i == 0 ? "" : ", ") << scope.source.topics[i];
        }
        out << (scope.source.topics.empty() ? "" : "; ") << "isolation " << scope.source.isolation
            << "\n";
        for (const auto& r : scope.source.ranges) {
            out << "             op " << r.op << " partition " << r.partition << ": offsets ["
                << (r.from ? std::to_string(*r.from) : std::string{"start"}) << ", "
                << (r.to ? std::to_string(*r.to) : std::string{"?"}) << ")";
            if (r.from && r.to) {
                out << ", " << (*r.to - *r.from) << " record(s)";
            }
            out << "\n";
        }
        if (!scope.source.from_complete || !scope.source.to_complete) {
            out << "             (positions from part of the participants' snapshots only)\n";
        }
        for (const auto& id : scope.source.identities) {
            out << "             op " << id.op << ": "
                << (id.end ? describe_log(*id.end) : std::string{"no log recorded"});
            if (id.start && id.same_log() == std::optional<bool>{false}) {
                out << "; at the scope's start " << describe_log(*id.start);
            }
            out << "\n";
        }
    }
    if (scope.window) {
        const auto& w = *scope.window;
        out << "window:      tumbling, " << w.size_ms << " ms on " << w.time_column
            << "; watermark " << w.lag_ms << " ms behind the least partition maximum, no "
            << "idleness; allowed lateness " << w.allowed_lateness_ms
            << " ms, later records dropped\n";
        out << "lineage:     " << scope.lineage.size() << " completed checkpoint(s) from a start "
            << "with no state";
        for (const auto& rs : scope.restores) {
            out << "; restored from checkpoint " << rs.restored_from << " at barrier "
                << rs.first_epoch;
        }
        out << "\n";
    } else if (scope.window_unsupported) {
        out << "window:      not supported: " << *scope.window_unsupported << "\n";
    }
    out << "\n";
    for (const auto& c : checks) {
        out << "[" << to_string(c.verdict) << "] " << c.id << "\n    " << c.reason << "\n";
        if (!c.counts.empty()) {
            out << "    counts:";
            for (std::size_t i = 0; i < c.counts.size(); ++i) {
                out << (i == 0 ? " " : ", ") << c.counts[i].first << " " << c.counts[i].second;
            }
            out << "\n";
        }
        for (const auto& d : c.details) {
            out << "    - " << d << "\n";
        }
        if (!c.evidence.empty()) {
            out << "    evidence: ";
            for (std::size_t i = 0; i < c.evidence.size() && i < 6; ++i) {
                out << (i == 0 ? "" : ", ") << c.evidence[i];
            }
            if (c.evidence.size() > 6) {
                out << ", and " << (c.evidence.size() - 6) << " more";
            }
            out << "\n";
        }
        if (!c.next_action.empty()) {
            out << "    next: " << c.next_action << "\n";
        }
    }
    std::vector<std::size_t> idx;
    for (const auto& name : key) {
        for (std::size_t i = 0; i < scope.columns.size(); ++i) {
            if (scope.columns[i].name == name) {
                idx.push_back(i);
            }
        }
    }
    const auto list_text = [&](const char* title, const Comparison& cmp) {
        if (cmp.listed.empty()) {
            return;
        }
        out << "\n" << title << " (first " << cmp.listed.size() << "):\n";
        for (const auto& d : cmp.listed) {
            out << "  " << to_string(d.kind) << " " << key_text(d.key, idx, scope.columns);
            if (d.kind == Discrepancy::Kind::Duplicate) {
                out << ": " << d.copies << " extra cop" << (d.copies == 1 ? "y" : "ies");
            } else if (d.kind == Discrepancy::Kind::Mismatch) {
                for (const auto i : d.differing) {
                    out << "\n      " << scope.columns[i].name << ": expected "
                        << display(d.expected[i]) << ", found " << display(d.observed[i]);
                }
            } else if (d.kind == Discrepancy::Kind::Unexpected) {
                out << ": " << row_text(d.observed, scope.columns);
            } else {
                out << ": " << row_text(d.expected, scope.columns);
            }
            out << "\n";
        }
    };
    if (!findings.empty()) {
        out << "\nfindings:\n";
        for (const auto& f : findings) {
            out << "  " << f.relationship << ": " << f.summary << "\n";
        }
    }
    if (window_audit) {
        const auto& wa = *window_audit;
        out << "\nwindows:     " << wa.final_windows << " final, " << wa.open_windows << " open, "
            << wa.undetermined_windows << " undetermined";
        if (wa.captured_left_out + wa.destination_left_out > 0) {
            out << "; left out: " << wa.captured_left_out << " captured row(s), "
                << wa.destination_left_out << " table row(s)";
        }
        out << "\n";
        const auto ms = [](const std::optional<std::int64_t>& v) {
            return v ? std::to_string(*v) : std::string{"none"};
        };
        out << "watermarks:  recorded / allowed by the source, per lineage checkpoint\n";
        const std::size_t n = wa.watermarks.size();
        for (std::size_t i = 0; i < n; ++i) {
            if (n > 12 && i == 6) {
                out << "             ... " << (n - 12) << " more\n";
                i = n - 6;
            }
            const auto& m = wa.watermarks[i];
            out << "             " << m.checkpoint << ": " << ms(m.recorded_ms) << " / "
                << ms(m.source_bound_ms) << "\n";
        }
    }
    out << "\ncoverage:    " << coverage.mode << ", " << coverage.keys_selected
        << " key(s) selected";
    if (coverage.complete_established) {
        out << "; the table read whole (" << coverage.table_rows_read << " row(s))";
    }
    if (!coverage.complete_reason.empty()) {
        out << "\n             " << coverage.complete_reason;
    }
    out << "\n";
    if (reference_capture) {
        list_text("reference against captured input", *reference_capture);
    }
    if (reference_destination) {
        list_text("reference against destination", *reference_destination);
    }
    if (table_comparison) {
        list_text("whole table against the expected rows", *table_comparison);
    }
    if (comparison && !comparison->listed.empty()) {
        out << "\ndiscrepancies (first " << comparison->listed.size() << "):\n";
        for (const auto& d : comparison->listed) {
            out << "  " << to_string(d.kind) << " " << key_text(d.key, idx, scope.columns);
            if (d.kind == Discrepancy::Kind::Duplicate) {
                out << ": " << d.copies << " extra cop" << (d.copies == 1 ? "y" : "ies");
            } else if (d.kind == Discrepancy::Kind::Mismatch) {
                for (const auto i : d.differing) {
                    out << "\n      " << scope.columns[i].name << ": expected "
                        << display(d.expected[i]) << ", found " << display(d.observed[i]);
                }
            } else if (d.kind == Discrepancy::Kind::Unexpected) {
                out << ": " << row_text(d.observed, scope.columns);
            } else {
                out << ": " << row_text(d.expected, scope.columns);
            }
            out << "\n";
        }
    }
    out << "\nassumptions:\n";
    for (const auto& a : assumptions) {
        out << "  - " << a << "\n";
    }
    const int code = exit_code();
    out << "\nresult: " << result_of(code) << " (exit " << code << ")";
    if (code == 3) {
        out << ": nothing failed, but the reconciliation is not complete";
    } else if (code == 0 && !coverage.complete_established) {
        out << ": for the selected keys; rows of other keys were not read";
    }
    out << "\n";
    return out.str();
}

std::string Report::render_json() const {
    const int code = exit_code();
    config::JsonObject o;
    o["format"] = config::JsonValue{std::string{kReportFormat}};
    o["version"] = config::JsonValue{kReportVersion};
    o["bundle"] = config::JsonValue{bundle};
    o["result"] = config::JsonValue{std::string{result_of(code)}};
    o["exit_code"] = config::JsonValue{static_cast<std::int64_t>(code)};
    if (refused) {
        o["refused"] = config::JsonValue{*refused};
        return config::JsonValue{std::move(o)}.serialize(2) + "\n";
    }
    config::JsonObject scope_o;
    scope_o["job"] = config::JsonValue{std::to_string(scope.job)};
    scope_o["checkpoint"] = config::JsonValue{static_cast<std::int64_t>(scope.checkpoint)};
    scope_o["sink_op"] = config::JsonValue{std::to_string(scope.sink_op)};
    config::JsonArray epochs;
    for (const auto e : scope.epochs) {
        epochs.emplace_back(static_cast<std::int64_t>(e));
    }
    scope_o["intervals_ending_at"] = config::JsonValue{std::move(epochs)};
    scope_o["starts_after_checkpoint"] =
        config::JsonValue{static_cast<std::int64_t>(scope.starts_after)};
    scope_o["starts_at_run_start"] = config::JsonValue{scope.starts_at_run_start};
    config::JsonArray ivs;
    for (const auto& e : scope.intervals) {
        config::JsonObject iv;
        iv["path"] = config::JsonValue{e.path};
        iv["ends_at_barrier"] = config::JsonValue{static_cast<std::int64_t>(e.epoch)};
        iv["previous_barrier"] = config::JsonValue{static_cast<std::int64_t>(e.previous_barrier)};
        iv["sink_subtask"] = config::JsonValue{static_cast<std::int64_t>(e.sink_subtask)};
        iv["attempt"] = config::JsonValue{e.attempt};
        iv["records"] = config::JsonValue{static_cast<std::int64_t>(e.records_seen)};
        iv["watermark_ms"] =
            e.watermark_ms ? config::JsonValue{*e.watermark_ms} : config::JsonValue{};
        if (e.sink.is_object()) {
            for (const char* f : {"token_nonce", "rows_acknowledged", "inserts_unlisted"}) {
                if (e.sink.contains(f)) {
                    iv[f] = e.sink.at(f);
                }
            }
            if (e.sink.contains("inserts") && e.sink.at("inserts").is_array()) {
                iv["inserts"] = e.sink.at("inserts");
            }
        }
        ivs.emplace_back(std::move(iv));
    }
    scope_o["intervals"] = config::JsonValue{std::move(ivs)};
    scope_o["expected_rows"] = config::JsonValue{static_cast<std::int64_t>(scope.expected.size())};
    config::JsonArray notes;
    for (const auto& n : scope.notes) {
        notes.emplace_back(n);
    }
    scope_o["notes"] = config::JsonValue{std::move(notes)};
    config::JsonObject src;
    src["offset_semantics"] = config::JsonValue{std::string{
        "a position is the next offset to read, so the scope consumed [from, to) of each "
        "partition; for a record-wise job the sink's rows in the scope derive from exactly those "
        "records"}};
    src["isolation_level"] = config::JsonValue{scope.source.isolation};
    config::JsonArray topics;
    for (const auto& t : scope.source.topics) {
        topics.emplace_back(t);
    }
    src["topics"] = config::JsonValue{std::move(topics)};
    config::JsonArray ranges;
    for (const auto& r : scope.source.ranges) {
        config::JsonObject rj;
        rj["op"] = config::JsonValue{std::to_string(r.op)};
        rj["partition"] = config::JsonValue{static_cast<std::int64_t>(r.partition)};
        rj["from"] = r.from ? config::JsonValue{*r.from} : config::JsonValue{};
        rj["to"] = r.to ? config::JsonValue{*r.to} : config::JsonValue{};
        ranges.emplace_back(std::move(rj));
    }
    src["ranges"] = config::JsonValue{std::move(ranges)};
    src["from_complete"] = config::JsonValue{scope.source.from_complete};
    src["to_complete"] = config::JsonValue{scope.source.to_complete};
    config::JsonArray src_notes;
    for (const auto& n : scope.source.notes) {
        src_notes.emplace_back(n);
    }
    src["notes"] = config::JsonValue{std::move(src_notes)};
    const auto log_json = [](const std::optional<SourceLog>& log) {
        if (!log) {
            return config::JsonValue{};
        }
        const auto id_or_null = [](const std::string& id) {
            return id.empty() ? config::JsonValue{} : config::JsonValue{id};
        };
        config::JsonObject lj;
        lj["topic"] = config::JsonValue{log->topic};
        lj["cluster_id"] = id_or_null(log->cluster_id);
        lj["topic_id"] = id_or_null(log->topic_id);
        return config::JsonValue{std::move(lj)};
    };
    config::JsonArray identities;
    for (const auto& id : scope.source.identities) {
        config::JsonObject ij;
        ij["op"] = config::JsonValue{std::to_string(id.op)};
        ij["start"] = log_json(id.start);
        ij["end"] = log_json(id.end);
        const auto same = id.same_log();
        ij["same_log"] = same ? config::JsonValue{*same} : config::JsonValue{};
        identities.emplace_back(std::move(ij));
    }
    src["identities"] = config::JsonValue{std::move(identities)};
    scope_o["source"] = config::JsonValue{std::move(src)};
    if (scope.window) {
        const auto& w = *scope.window;
        config::JsonObject wj;
        wj["kind"] = config::JsonValue{std::string{"tumbling-window"}};
        wj["size_ms"] = config::JsonValue{w.size_ms};
        wj["time_column"] = config::JsonValue{w.time_column};
        wj["lag_ms"] = config::JsonValue{w.lag_ms};
        wj["allowed_lateness_ms"] = config::JsonValue{w.allowed_lateness_ms};
        config::JsonArray idj;
        for (const auto& c : w.identity) {
            idj.emplace_back(c);
        }
        wj["identity"] = config::JsonValue{std::move(idj)};
        wj["source_parallelism"] =
            config::JsonValue{static_cast<std::int64_t>(w.source_parallelism)};
        wj["alignment"] = config::JsonValue{w.alignment};
        scope_o["window"] = config::JsonValue{std::move(wj)};
        config::JsonArray rsj;
        for (const auto& rs : scope.restores) {
            config::JsonObject x;
            x["restored_from"] = config::JsonValue{static_cast<std::int64_t>(rs.restored_from)};
            x["first_epoch"] = config::JsonValue{static_cast<std::int64_t>(rs.first_epoch)};
            rsj.emplace_back(std::move(x));
        }
        scope_o["restores"] = config::JsonValue{std::move(rsj)};
        scope_o["lineage_checkpoints"] =
            config::JsonValue{static_cast<std::int64_t>(scope.lineage.size())};
    } else if (scope.window_unsupported) {
        scope_o["window_unsupported"] = config::JsonValue{*scope.window_unsupported};
    }
    o["scope"] = config::JsonValue{std::move(scope_o)};
    o["delivery"] = scope.delivery;

    config::JsonObject dest_o;
    dest_o["recorded"] = scope.destination;
    if (destination) {
        config::JsonObject obs;
        obs["server_uuid"] = config::JsonValue{destination->server_uuid};
        obs["host_name"] = config::JsonValue{destination->host_name};
        obs["version"] = config::JsonValue{destination->version};
        obs["database"] = config::JsonValue{destination->database};
        obs["table"] = config::JsonValue{destination->table};
        obs["table_uuid"] = config::JsonValue{destination->table_uuid};
        obs["engine"] = config::JsonValue{destination->engine};
        dest_o["observed"] = config::JsonValue{std::move(obs)};
    }
    o["destination"] = config::JsonValue{std::move(dest_o)};

    config::JsonObject obs_o;
    obs_o["from"] = config::JsonValue{observed_from};
    obs_o["to"] = config::JsonValue{observed_to};
    obs_o["server_from"] = config::JsonValue{server_time_from};
    obs_o["server_to"] = config::JsonValue{server_time_to};
    config::JsonArray k;
    for (const auto& name : key) {
        k.emplace_back(name);
    }
    obs_o["key"] = config::JsonValue{std::move(k)};
    obs_o["method"] = config::JsonValue{std::string{
        "every expected row against every row of its key, read twice, compared cell by cell as "
        "typed values (canonical cells: integers exact, floats by their bits, decimals unscaled at "
        "the column's scale, times as ticks at the column's precision, NULL equal only to NULL)"}};
    config::JsonArray muts;
    for (const auto& m : mutations_noted) {
        muts.emplace_back(m);
    }
    obs_o["mutations_done_before"] = config::JsonValue{std::move(muts)};
    o["observation"] = config::JsonValue{std::move(obs_o)};

    config::JsonArray assume;
    for (const auto& a : assumptions) {
        assume.emplace_back(a);
    }
    o["assumptions"] = config::JsonValue{std::move(assume)};

    config::JsonArray checks_a;
    for (const auto& c : checks) {
        config::JsonObject co;
        co["id"] = config::JsonValue{c.id};
        co["verdict"] = config::JsonValue{std::string{to_string(c.verdict)}};
        co["reason"] = config::JsonValue{c.reason};
        co["next_action"] = config::JsonValue{c.next_action};
        config::JsonArray ev;
        for (const auto& e : c.evidence) {
            ev.emplace_back(e);
        }
        co["evidence"] = config::JsonValue{std::move(ev)};
        config::JsonArray det;
        for (const auto& e : c.details) {
            det.emplace_back(e);
        }
        co["details"] = config::JsonValue{std::move(det)};
        if (!c.counts.empty()) {
            config::JsonObject cnt;
            for (const auto& [name, n] : c.counts) {
                cnt[name] = config::JsonValue{static_cast<std::int64_t>(n)};
            }
            co["counts"] = config::JsonValue{std::move(cnt)};
        }
        checks_a.emplace_back(std::move(co));
    }
    o["checks"] = config::JsonValue{std::move(checks_a)};

    std::vector<std::size_t> idx;
    for (const auto& name : key) {
        for (std::size_t i = 0; i < scope.columns.size(); ++i) {
            if (scope.columns[i].name == name) {
                idx.push_back(i);
            }
        }
    }
    const auto listed_json = [&](const Comparison& cmp) {
        config::JsonArray ds;
        for (const auto& d : cmp.listed) {
            config::JsonObject dj;
            dj["kind"] = config::JsonValue{std::string{to_string(d.kind)}};
            config::JsonObject kj;
            for (std::size_t i = 0; i < d.key.size() && i < idx.size(); ++i) {
                kj[scope.columns[idx[i]].name] = config::JsonValue{display(d.key[i])};
            }
            dj["key"] = config::JsonValue{std::move(kj)};
            if (!d.expected.empty()) {
                dj["expected"] = row_json(d.expected, scope.columns);
            }
            if (!d.observed.empty()) {
                dj["observed"] = row_json(d.observed, scope.columns);
            }
            if (!d.differing.empty()) {
                config::JsonArray cols;
                for (const auto i : d.differing) {
                    cols.emplace_back(scope.columns[i].name);
                }
                dj["columns"] = config::JsonValue{std::move(cols)};
            }
            if (d.kind == Discrepancy::Kind::Duplicate) {
                dj["extra_copies"] = config::JsonValue{static_cast<std::int64_t>(d.copies)};
            }
            ds.emplace_back(std::move(dj));
        }
        return config::JsonValue{std::move(ds)};
    };
    {
        config::JsonObject cov;
        cov["mode"] = config::JsonValue{coverage.mode};
        cov["keys_selected"] = config::JsonValue{static_cast<std::int64_t>(coverage.keys_selected)};
        cov["complete_established"] = config::JsonValue{coverage.complete_established};
        cov["complete_reason"] = config::JsonValue{coverage.complete_reason};
        cov["table_rows_read"] =
            config::JsonValue{static_cast<std::int64_t>(coverage.table_rows_read)};
        o["coverage"] = config::JsonValue{std::move(cov)};
    }
    if (reference_path) {
        config::JsonObject a;
        config::JsonObject rj;
        const auto strings = [](const std::vector<std::string>& v) {
            config::JsonArray out;
            for (const auto& x : v) {
                out.emplace_back(x);
            }
            return config::JsonValue{std::move(out)};
        };
        rj["path"] = config::JsonValue{*reference_path};
        rj["implementation"] = config::JsonValue{reference_implementation};
        rj["rows"] = config::JsonValue{static_cast<std::int64_t>(reference_rows)};
        rj["rows_sha256"] = config::JsonValue{reference_rows_sha256};
        rj["complete"] = config::JsonValue{reference_complete};
        rj["incomplete"] = strings(reference_incomplete);
        rj["recorded"] = strings(reference_recorded);
        rj["writer_verified"] = strings(reference_writer_verified);
        rj["verified"] = strings(reference_verified);
        a["reference"] = config::JsonValue{std::move(rj)};
        if (scope_document) {
            a["scope_sha256"] = config::JsonValue{scope_document->string_or("scope_sha256", "")};
        }
        config::JsonArray fs_;
        for (const auto& f : findings) {
            config::JsonObject fj;
            fj["relationship"] = config::JsonValue{f.relationship};
            fj["summary"] = config::JsonValue{f.summary};
            fs_.emplace_back(std::move(fj));
        }
        a["findings"] = config::JsonValue{std::move(fs_)};
        if (reference_capture) {
            a["reference_capture_discrepancies"] = listed_json(*reference_capture);
        }
        if (reference_destination) {
            a["reference_destination_discrepancies"] = listed_json(*reference_destination);
        }
        if (window_audit) {
            const auto& wa = *window_audit;
            const auto n64 = [](std::uint64_t v) {
                return config::JsonValue{static_cast<std::int64_t>(v)};
            };
            const auto opt = [](const std::optional<std::int64_t>& v) {
                return v ? config::JsonValue{*v} : config::JsonValue{};
            };
            config::JsonObject wj;
            wj["final"] = n64(wa.final_windows);
            wj["open"] = n64(wa.open_windows);
            wj["undetermined"] = n64(wa.undetermined_windows);
            wj["captured_left_out"] = n64(wa.captured_left_out);
            wj["destination_left_out"] = n64(wa.destination_left_out);
            wj["semantics_differences"] = strings(wa.semantics_differences);
            wj["reference_semantics"] = wa.reference_semantics;
            wj["closed_early"] = strings(wa.early);
            wj["records"] = wa.records;
            config::JsonArray marks;
            for (const auto& m : wa.watermarks) {
                config::JsonObject mj;
                mj["checkpoint"] = n64(m.checkpoint);
                mj["recorded_ms"] = opt(m.recorded_ms);
                mj["source_bound_ms"] = opt(m.source_bound_ms);
                marks.emplace_back(std::move(mj));
            }
            wj["watermarks"] = config::JsonValue{std::move(marks)};
            a["window"] = config::JsonValue{std::move(wj)};
        }
        o["audit"] = config::JsonValue{std::move(a)};
    }
    if (table_comparison) {
        o["table_discrepancies"] = listed_json(*table_comparison);
    }
    if (comparison) {
        config::JsonArray ds;
        for (const auto& d : comparison->listed) {
            config::JsonObject dj;
            dj["kind"] = config::JsonValue{std::string{to_string(d.kind)}};
            config::JsonObject kj;
            for (std::size_t i = 0; i < d.key.size() && i < idx.size(); ++i) {
                kj[scope.columns[idx[i]].name] = config::JsonValue{display(d.key[i])};
            }
            dj["key"] = config::JsonValue{std::move(kj)};
            if (!d.expected.empty()) {
                dj["expected"] = row_json(d.expected, scope.columns);
            }
            if (!d.observed.empty()) {
                dj["observed"] = row_json(d.observed, scope.columns);
            }
            if (!d.differing.empty()) {
                config::JsonArray cols;
                for (const auto i : d.differing) {
                    cols.emplace_back(scope.columns[i].name);
                }
                dj["columns"] = config::JsonValue{std::move(cols)};
            }
            if (d.kind == Discrepancy::Kind::Duplicate) {
                dj["extra_copies"] = config::JsonValue{static_cast<std::int64_t>(d.copies)};
            }
            ds.emplace_back(std::move(dj));
        }
        o["discrepancies"] = config::JsonValue{std::move(ds)};
    }
    return config::JsonValue{std::move(o)}.serialize(2) + "\n";
}

std::string expected_ndjson(const Scope& scope) {
    std::vector<std::string> lines;
    lines.reserve(scope.expected.size());
    for (const auto& row : scope.expected) {
        config::JsonObject o;
        for (std::size_t i = 0; i < row.size() && i < scope.columns.size(); ++i) {
            o[scope.columns[i].name] = config::JsonValue{row[i]};
        }
        lines.push_back(config::JsonValue{std::move(o)}.serialize(0));
    }
    std::sort(lines.begin(), lines.end());
    std::string out;
    for (const auto& l : lines) {
        out += l;
        out += '\n';
    }
    return out;
}

}  // namespace clink::incident::reconcile
