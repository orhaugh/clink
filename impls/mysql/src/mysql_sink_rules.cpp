// The MySQL sinks' pure rules. See mysql_sink_rules.hpp.

#include "clink/mysql/mysql_sink_rules.hpp"

#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <iterator>
#include <set>
#include <string>
#include <system_error>
#include <utility>

#include "clink/mysql/mysql_sql.hpp"

namespace clink::mysql {

namespace {

// A number's exponent is read up to this magnitude: far past any plain decimal
// kMaxNumberChars allows.
constexpr std::int64_t kExponentCap = 1'000'000;
// Nesting deeper than this inside a member is refused, so a record cannot
// exhaust the reader's stack.
constexpr int kMaxDepth = 256;

bool is_digit(char c) {
    return c >= '0' && c <= '9';
}

int hex_value(char c) {
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
        return c - 'A' + 10;
    }
    return -1;
}

void put_utf8(std::string& out, std::uint32_t cp) {
    if (cp < 0x80) {
        out.push_back(static_cast<char>(cp));
    } else if (cp < 0x800) {
        out.push_back(static_cast<char>(0xC0 | (cp >> 6)));
        out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    } else if (cp < 0x10000) {
        out.push_back(static_cast<char>(0xE0 | (cp >> 12)));
        out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    } else {
        out.push_back(static_cast<char>(0xF0 | (cp >> 18)));
        out.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    }
}

// The length of the well-formed UTF-8 sequence starting at s[i], or 0.
std::size_t utf8_sequence(std::string_view s, std::size_t i) {
    const auto b = [&](std::size_t k) { return static_cast<unsigned char>(s[i + k]); };
    const auto cont = [&](std::size_t k) { return i + k < s.size() && (b(k) & 0xC0) == 0x80; };
    const unsigned char c = b(0);
    if (c < 0x80) {
        return 1;
    }
    if (c >= 0xC2 && c <= 0xDF) {
        return cont(1) ? 2 : 0;
    }
    if (c >= 0xE0 && c <= 0xEF) {
        if (!cont(1) || !cont(2)) {
            return 0;
        }
        if ((c == 0xE0 && b(1) < 0xA0) || (c == 0xED && b(1) >= 0xA0)) {
            return 0;  // overlong, or a surrogate
        }
        return 3;
    }
    if (c >= 0xF0 && c <= 0xF4) {
        if (!cont(1) || !cont(2) || !cont(3)) {
            return 0;
        }
        if ((c == 0xF0 && b(1) < 0x90) || (c == 0xF4 && b(1) >= 0x90)) {
            return 0;  // overlong, or past U+10FFFF
        }
        return 4;
    }
    return 0;
}

class Reader {
public:
    explicit Reader(std::string_view s) : s_(s) {}

    std::string object(JsonMembers& out) {
        ws();
        if (p_ >= s_.size() || s_[p_] != '{') {
            return s_.find_first_not_of(" \t\r\n") == std::string_view::npos
                       ? "it is not a JSON object: it is empty"
                       : "it is not a JSON object";
        }
        ++p_;
        ws();
        if (p_ < s_.size() && s_[p_] == '}') {
            ++p_;
            return trailing();
        }
        for (;;) {
            ws();
            std::string name;
            if (p_ >= s_.size() || s_[p_] != '"') {
                return "a member name is not a string";
            }
            if (auto why = string(&name); !why.empty()) {
                return why;
            }
            ws();
            if (p_ >= s_.size() || s_[p_] != ':') {
                return "a member name is not followed by ':'";
            }
            ++p_;
            ws();
            JsonMember m;
            if (auto why = member(m); !why.empty()) {
                return "member \"" + printable(name) + "\" " + why;
            }
            if (!out.emplace(name, std::move(m)).second) {
                return "it names member \"" + printable(name) + "\" twice";
            }
            ws();
            if (p_ < s_.size() && s_[p_] == ',') {
                ++p_;
                continue;
            }
            if (p_ < s_.size() && s_[p_] == '}') {
                ++p_;
                return trailing();
            }
            return "a member is not followed by ',' or '}'";
        }
    }

private:
    // A member name for a message: printable ASCII, cut short.
    static std::string printable(std::string_view name) {
        std::string out;
        for (const char c : name) {
            if (out.size() >= 64) {
                out += "...";
                break;
            }
            const auto u = static_cast<unsigned char>(c);
            out.push_back(u >= 0x20 && u < 0x7F && c != '"' && c != '\\' ? c : '?');
        }
        return out;
    }

    std::string trailing() {
        ws();
        return p_ == s_.size() ? std::string{} : "it has text after its closing '}'";
    }

    void ws() {
        while (p_ < s_.size() &&
               (s_[p_] == ' ' || s_[p_] == '\t' || s_[p_] == '\n' || s_[p_] == '\r')) {
            ++p_;
        }
    }

    bool literal(std::string_view word) {
        if (s_.substr(p_, word.size()) == word) {
            p_ += word.size();
            return true;
        }
        return false;
    }

    std::string member(JsonMember& m) {
        if (p_ >= s_.size()) {
            return "has no value";
        }
        const char c = s_[p_];
        if (c == '"') {
            m.kind = JsonMember::Kind::String;
            return string(&m.text);
        }
        if (c == '{' || c == '[') {
            const auto begin = p_;
            if (auto why = composite(0); !why.empty()) {
                return why;
            }
            m.kind = JsonMember::Kind::Composite;
            m.text = std::string{s_.substr(begin, p_ - begin)};
            return {};
        }
        if (c == '-' || is_digit(c)) {
            const auto begin = p_;
            if (auto why = number(); !why.empty()) {
                return why;
            }
            m.kind = JsonMember::Kind::Number;
            m.text = std::string{s_.substr(begin, p_ - begin)};
            return {};
        }
        if (literal("true")) {
            m.kind = JsonMember::Kind::Bool;
            m.text = "true";
            return {};
        }
        if (literal("false")) {
            m.kind = JsonMember::Kind::Bool;
            m.text = "false";
            return {};
        }
        if (literal("null")) {
            m.kind = JsonMember::Kind::Null;
            return {};
        }
        return "is not a JSON value";
    }

    // -?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?
    std::string number() {
        if (s_[p_] == '-') {
            ++p_;
        }
        if (p_ >= s_.size() || !is_digit(s_[p_])) {
            return "is a malformed number";
        }
        if (s_[p_] == '0') {
            ++p_;
            if (p_ < s_.size() && is_digit(s_[p_])) {
                return "is a malformed number (a leading zero)";
            }
        } else {
            while (p_ < s_.size() && is_digit(s_[p_])) {
                ++p_;
            }
        }
        if (p_ < s_.size() && s_[p_] == '.') {
            ++p_;
            if (p_ >= s_.size() || !is_digit(s_[p_])) {
                return "is a malformed number";
            }
            while (p_ < s_.size() && is_digit(s_[p_])) {
                ++p_;
            }
        }
        if (p_ < s_.size() && (s_[p_] == 'e' || s_[p_] == 'E')) {
            ++p_;
            if (p_ < s_.size() && (s_[p_] == '+' || s_[p_] == '-')) {
                ++p_;
            }
            if (p_ >= s_.size() || !is_digit(s_[p_])) {
                return "is a malformed number";
            }
            while (p_ < s_.size() && is_digit(s_[p_])) {
                ++p_;
            }
        }
        return {};
    }

    // A string at p_ (the opening quote), decoded into *out when out is set.
    std::string string(std::string* out) {
        ++p_;
        for (;;) {
            if (p_ >= s_.size()) {
                return "has a string that does not end";
            }
            const auto c = static_cast<unsigned char>(s_[p_]);
            if (c == '"') {
                ++p_;
                return {};
            }
            if (c < 0x20) {
                return "has a control character inside a string";
            }
            if (c == '\\') {
                if (p_ + 1 >= s_.size()) {
                    return "has a string that does not end";
                }
                const char e = s_[p_ + 1];
                p_ += 2;
                char plain = 0;
                switch (e) {
                    case '"':
                        plain = '"';
                        break;
                    case '\\':
                        plain = '\\';
                        break;
                    case '/':
                        plain = '/';
                        break;
                    case 'b':
                        plain = '\b';
                        break;
                    case 'f':
                        plain = '\f';
                        break;
                    case 'n':
                        plain = '\n';
                        break;
                    case 'r':
                        plain = '\r';
                        break;
                    case 't':
                        plain = '\t';
                        break;
                    case 'u': {
                        std::uint32_t cp = 0;
                        if (!hex4(&cp)) {
                            return "has a malformed \\u escape";
                        }
                        if (cp >= 0xDC00 && cp <= 0xDFFF) {
                            return "has a \\u escape of a lone low surrogate";
                        }
                        if (cp >= 0xD800 && cp <= 0xDBFF) {
                            std::uint32_t low = 0;
                            if (s_.substr(p_, 2) != "\\u") {
                                return "has a \\u escape of a lone high surrogate";
                            }
                            p_ += 2;
                            if (!hex4(&low) || low < 0xDC00 || low > 0xDFFF) {
                                return "has a \\u escape of a lone high surrogate";
                            }
                            cp = 0x10000 + ((cp - 0xD800) << 10) + (low - 0xDC00);
                        }
                        if (out != nullptr) {
                            put_utf8(*out, cp);
                        }
                        continue;
                    }
                    default:
                        return "has an unknown escape inside a string";
                }
                if (out != nullptr) {
                    out->push_back(plain);
                }
                continue;
            }
            const auto n = utf8_sequence(s_, p_);
            if (n == 0) {
                return "is not valid UTF-8";
            }
            if (out != nullptr) {
                out->append(s_.substr(p_, n));
            }
            p_ += n;
        }
    }

    bool hex4(std::uint32_t* cp) {
        if (p_ + 4 > s_.size()) {
            return false;
        }
        std::uint32_t v = 0;
        for (std::size_t k = 0; k < 4; ++k) {
            const int h = hex_value(s_[p_ + k]);
            if (h < 0) {
                return false;
            }
            v = (v << 4) | static_cast<std::uint32_t>(h);
        }
        p_ += 4;
        *cp = v;
        return true;
    }

    // An object or array at p_, checked and skipped.
    std::string composite(int depth) {
        if (depth >= kMaxDepth) {
            return "is nested deeper than " + std::to_string(kMaxDepth) + " levels";
        }
        const char open = s_[p_];
        const char close = open == '{' ? '}' : ']';
        ++p_;
        ws();
        if (p_ < s_.size() && s_[p_] == close) {
            ++p_;
            return {};
        }
        for (;;) {
            ws();
            if (open == '{') {
                // A nested object's repeated name is kept as written: the
                // member is stored as text, and MySQL's JSON keeps the last.
                if (p_ >= s_.size() || s_[p_] != '"') {
                    return "holds an object whose member name is not a string";
                }
                if (auto why = string(nullptr); !why.empty()) {
                    return why;
                }
                ws();
                if (p_ >= s_.size() || s_[p_] != ':') {
                    return "holds an object whose member name is not followed by ':'";
                }
                ++p_;
                ws();
            }
            if (p_ >= s_.size()) {
                return "ends inside an object or array";
            }
            if (s_[p_] == '{' || s_[p_] == '[') {
                if (auto why = composite(depth + 1); !why.empty()) {
                    return why;
                }
            } else {
                JsonMember inner;
                if (auto why = member(inner); !why.empty()) {
                    return "holds a value that " + why;
                }
            }
            ws();
            if (p_ < s_.size() && s_[p_] == ',') {
                ++p_;
                continue;
            }
            if (p_ < s_.size() && s_[p_] == close) {
                ++p_;
                return {};
            }
            return "holds an object or array whose item is not followed by ',' or its close";
        }
    }

    std::string_view s_;
    std::size_t p_{0};
};

bool contains(const std::vector<std::string>& v, std::string_view x) {
    return std::find(v.begin(), v.end(), x) != v.end();
}

std::string joined(const std::vector<std::string>& v) {
    std::string out;
    for (const auto& x : v) {
        out += (out.empty() ? "" : ", ") + x;
    }
    return out;
}

}  // namespace

// ---- one record -----------------------------------------------------------------------

std::string read_json_object(std::string_view text, JsonMembers& out) {
    out.clear();
    return Reader(text).object(out);
}

std::string plain_decimal(std::string_view token, std::size_t max_chars) {
    std::size_t i = 0;
    const bool negative = token[i] == '-';
    if (negative) {
        ++i;
    }
    const auto int_begin = i;
    while (i < token.size() && is_digit(token[i])) {
        ++i;
    }
    const auto int_part = token.substr(int_begin, i - int_begin);
    std::string_view frac_part;
    if (i < token.size() && token[i] == '.') {
        const auto frac_begin = ++i;
        while (i < token.size() && is_digit(token[i])) {
            ++i;
        }
        frac_part = token.substr(frac_begin, i - frac_begin);
    }
    std::int64_t exponent = 0;
    if (i < token.size() && (token[i] == 'e' || token[i] == 'E')) {
        ++i;
        bool exponent_negative = false;
        if (token[i] == '+' || token[i] == '-') {
            exponent_negative = token[i] == '-';
            ++i;
        }
        for (; i < token.size(); ++i) {
            exponent = std::min<std::int64_t>((exponent * 10) + (token[i] - '0'), kExponentCap);
        }
        if (exponent_negative) {
            exponent = -exponent;
        }
    }
    // value = digits x 10^e
    std::string digits;
    digits.reserve(int_part.size() + frac_part.size());
    digits.append(int_part);
    digits.append(frac_part);
    const auto first = digits.find_first_not_of('0');
    if (first == std::string::npos) {
        return "0";
    }
    digits.erase(0, first);
    std::int64_t e = exponent - static_cast<std::int64_t>(frac_part.size());
    const auto last = digits.find_last_not_of('0');
    e += static_cast<std::int64_t>(digits.size() - 1 - last);
    digits.erase(last + 1);
    const auto len = static_cast<std::int64_t>(digits.size());
    std::int64_t plain = 0;
    if (e >= 0) {
        plain = len + e;
    } else if (len > -e) {
        plain = len + 1;
    } else {
        plain = 2 - e;  // "0." then -e - len zeros, then the digits
    }
    if (plain + (negative ? 1 : 0) > static_cast<std::int64_t>(max_chars)) {
        return {};
    }
    std::string out;
    out.reserve(static_cast<std::size_t>(plain) + 1);
    if (negative) {
        out.push_back('-');
    }
    if (e >= 0) {
        out += digits;
        out.append(static_cast<std::size_t>(e), '0');
    } else if (len > -e) {
        const auto point = static_cast<std::size_t>(len + e);
        out.append(digits, 0, point);
        out.push_back('.');
        out.append(digits, point);
    } else {
        out += "0.";
        out.append(static_cast<std::size_t>(-e - len), '0');
        out += digits;
    }
    return out;
}

// ---- the destination table ------------------------------------------------------------

const ColumnInfo* TableSchema::column(std::string_view name) const {
    for (const auto& c : columns) {
        if (c.name == name) {
            return &c;
        }
    }
    return nullptr;
}

bool key_identity_exact(const ColumnInfo& c, bool epoch_ms) {
    static const std::set<std::string, std::less<>> kIntegers = {
        "tinyint", "smallint", "mediumint", "int", "integer", "bigint"};
    if (kIntegers.count(c.data_type) > 0 || c.data_type == "decimal" || c.data_type == "numeric" ||
        c.data_type == "varbinary") {
        return true;
    }
    if (c.data_type == "varchar") {
        const auto& coll = c.collation;
        return c.collation_no_pad && coll.size() > 4 &&
               coll.compare(coll.size() - 4, 4, "_bin") == 0;
    }
    if (c.data_type == "datetime" || c.data_type == "timestamp") {
        return epoch_ms && c.datetime_precision >= 3;
    }
    return false;
}

std::vector<std::string> check_target(const SinkTarget& t, const TableSchema& s) {
    std::vector<std::string> why;
    if (!s.exists) {
        why.push_back("table " + t.table + " does not exist, or the sink's user cannot see it");
        return why;
    }
    const bool keyed = t.mode != SinkMode::Append;
    if (s.view && keyed) {
        why.push_back("table " + t.table + " is a view, which has no keys to write by");
        return why;
    }
    for (const auto& name : t.columns) {
        const auto* c = s.column(name);
        if (c == nullptr) {
            why.push_back("table " + t.table + " has no column '" + name + "'");
        } else if (c->generated) {
            why.push_back("column '" + name + "' is a generated column, which takes no value");
        }
    }
    for (const auto& [name, kind] : t.timestamp_columns) {
        (void)kind;
        const auto* c = s.column(name);
        if (c != nullptr && contains(t.columns, name) && c->data_type != "datetime" &&
            c->data_type != "timestamp") {
            why.push_back("column '" + name + "' carries a TIMESTAMP and is " + c->column_type +
                          " in the table; it must be DATETIME or TIMESTAMP");
        }
    }
    if (!why.empty()) {
        return why;
    }
    for (const auto& idx : s.unique_indexes) {
        if (keyed && idx.prefix) {
            why.push_back("unique index " + idx.name +
                          " covers a prefix of a column's values, so rows whose values differ "
                          "past that prefix collide on it");
        }
    }
    if (t.mode == SinkMode::InsertOrUpdate) {
        for (const auto& u : t.update_columns) {
            if (!contains(t.columns, u)) {
                why.push_back("update column '" + u +
                              "' is not one of the columns the sink writes");
            }
        }
        if (s.unique_indexes.empty()) {
            why.push_back("on_duplicate='update' needs a PRIMARY KEY or UNIQUE index on table " +
                          t.table + ": without one a replay inserts every row again");
        } else if (s.unique_indexes.size() > 1) {
            std::vector<std::string> names;
            for (const auto& idx : s.unique_indexes) {
                names.push_back(idx.name);
            }
            why.push_back("table " + t.table + " has " + std::to_string(names.size()) +
                          " unique indexes (" + joined(names) +
                          "): ON DUPLICATE KEY UPDATE updates whichever row a record collides "
                          "with first, so on_duplicate='update' needs exactly one");
        } else {
            for (const auto& c : s.unique_indexes.front().columns) {
                if (!contains(t.columns, c)) {
                    why.push_back("on_duplicate='update' keys rows by index " +
                                  s.unique_indexes.front().name + ", whose column '" + c +
                                  "' the sink does not write");
                }
            }
        }
    }
    if (t.mode == SinkMode::Changelog) {
        if (!s.transactional) {
            why.push_back("table " + t.table + " is " +
                          (s.engine.empty() ? std::string{"of an unknown engine"}
                                            : "a " + s.engine + " table") +
                          ", which has no transactions; mysql_upsert_sink applies each flush's "
                          "upserts and deletes as one transaction");
        }
        for (const auto& k : t.key_columns) {
            if (!contains(t.columns, k)) {
                why.push_back("key column '" + k + "' is not one of the columns the sink writes");
            }
        }
        const std::set<std::string> key(t.key_columns.begin(), t.key_columns.end());
        const UniqueIndex* identity = nullptr;
        for (const auto& idx : s.unique_indexes) {
            if (std::set<std::string>(idx.columns.begin(), idx.columns.end()) == key &&
                idx.columns.size() == key.size()) {
                identity = &idx;
            }
        }
        if (identity == nullptr) {
            why.push_back("table " + t.table + " has no PRIMARY KEY or UNIQUE index on exactly (" +
                          joined(t.key_columns) + "), the key the sink writes rows by");
        }
        for (const auto& idx : s.unique_indexes) {
            if (&idx != identity) {
                why.push_back("table " + t.table + " also has unique index " + idx.name + " (" +
                              joined(idx.columns) +
                              "): an upsert colliding with it would update the row it names "
                              "rather than the key's");
            }
        }
        if (!t.update_columns.empty()) {
            for (const auto& c : t.columns) {
                if (!contains(t.key_columns, c) && !contains(t.update_columns, c)) {
                    why.push_back("update_columns leaves out column '" + c +
                                  "': an upsert would keep its value from an earlier record, "
                                  "and the table would not hold each key's last record");
                }
            }
        }
        if (t.parallelism > 1) {
            for (const auto& k : t.key_columns) {
                const auto* c = s.column(k);
                if (c == nullptr) {
                    continue;
                }
                const bool epoch_ms = t.timestamp_columns.count(k) > 0;
                if (!key_identity_exact(*c, epoch_ms)) {
                    why.push_back(
                        "key column '" + k + "' is " + c->column_type +
                        (c->collation.empty() ? std::string{} : " COLLATE " + c->collation) +
                        ", under which values the stream tells apart can be one row; with " +
                        std::to_string(t.parallelism) +
                        " subtasks such values can reach the table from two of them in either "
                        "order. Run the sink at parallelism 1, or key the table by an exact "
                        "type (an integer, DECIMAL, VARBINARY, VARCHAR with a binary NO PAD "
                        "collation such as utf8mb4_0900_bin, or a DATETIME(3) or TIMESTAMP(3) "
                        "the stream writes as a TIMESTAMP)");
                }
            }
        }
    }
    return why;
}

// ---- rendering --------------------------------------------------------------------------

std::string render_literal(const JsonMember& v,
                           const ColumnInfo* col,
                           sqljson::TimeKind time,
                           const sqljson::EscapeFn& esc) {
    switch (v.kind) {
        case JsonMember::Kind::Null:
            return "NULL";
        case JsonMember::Kind::Bool:
            return v.text == "true" ? "1" : "0";
        case JsonMember::Kind::String:
            return "'" + esc(v.text) + "'";
        case JsonMember::Kind::Composite:
            return "'" + esc(v.text) + "'";
        case JsonMember::Kind::Number:
            break;
    }
    // An approximate column holds the number the server reads from the token,
    // which JSON spells as MySQL does. Only a value past a DOUBLE's range is
    // refused here, where the server's refusal would quote it.
    if (time == sqljson::TimeKind::None && col != nullptr &&
        (col->data_type == "double" || col->data_type == "float")) {
        if (!std::isfinite(std::strtod(v.text.c_str(), nullptr))) {
            throw RecordRefused("a number outside the range of a DOUBLE");
        }
        return v.text;
    }
    const auto plain = plain_decimal(v.text, kMaxNumberChars);
    if (plain.empty()) {
        throw RecordRefused("a number whose plain decimal form is longer than " +
                            std::to_string(kMaxNumberChars) + " characters");
    }
    if (time == sqljson::TimeKind::None) {
        return plain;
    }
    std::int64_t ms = 0;
    const auto [end, ec] = std::from_chars(plain.data(), plain.data() + plain.size(), ms);
    if (plain.find('.') != std::string::npos || ec != std::errc{} ||
        end != plain.data() + plain.size()) {
        throw RecordRefused("a TIMESTAMP that is not a whole number of milliseconds");
    }
    const int fsp = col != nullptr ? col->datetime_precision : 3;
    if (fsp >= 0 && fsp < 3) {
        std::int64_t unit = 1;
        for (int k = fsp; k < 3; ++k) {
            unit *= 10;
        }
        if (ms % unit != 0) {
            throw RecordRefused("a TIMESTAMP with milliseconds that column " +
                                (col != nullptr ? "'" + col->name + "' " : std::string{}) +
                                "would round away (it keeps " + std::to_string(fsp) +
                                " fractional digits)");
        }
    }
    try {
        return sqljson::epoch_ms_to_sql_timestamp(ms, time, sqljson::kMysql);
    } catch (const std::runtime_error&) {
        throw RecordRefused("a TIMESTAMP outside years 1 to 9999");
    }
}

std::string render_row(const JsonMembers& m,
                       const std::vector<std::string>& columns,
                       const TableSchema& schema,
                       const sqljson::TimeColumns& times,
                       const sqljson::EscapeFn& esc) {
    std::string out = "(";
    for (std::size_t i = 0; i < columns.size(); ++i) {
        if (i > 0) {
            out.push_back(',');
        }
        const auto it = m.find(columns[i]);
        if (it == m.end()) {
            out += "NULL";
            continue;
        }
        try {
            out += render_literal(it->second,
                                  schema.column(columns[i]),
                                  sqljson::time_kind_of(times, columns[i]),
                                  esc);
        } catch (const RecordRefused& e) {
            throw RecordRefused("member \"" + columns[i] + "\" is " + e.what());
        }
    }
    out.push_back(')');
    return out;
}

std::string render_key(const JsonMembers& m,
                       const std::vector<std::string>& key_columns,
                       const TableSchema& schema,
                       const sqljson::TimeColumns& times,
                       const sqljson::EscapeFn& esc) {
    std::string out = key_columns.size() > 1 ? "(" : "";
    for (std::size_t i = 0; i < key_columns.size(); ++i) {
        if (i > 0) {
            out.push_back(',');
        }
        const auto it = m.find(key_columns[i]);
        if (it == m.end() || it->second.kind == JsonMember::Kind::Null) {
            throw RecordRefused("its key member \"" + key_columns[i] +
                                "\" is missing or null, so it names no row");
        }
        try {
            out += render_literal(it->second,
                                  schema.column(key_columns[i]),
                                  sqljson::time_kind_of(times, key_columns[i]),
                                  esc);
        } catch (const RecordRefused& e) {
            throw RecordRefused("key member \"" + key_columns[i] + "\" is " + e.what());
        }
    }
    if (key_columns.size() > 1) {
        out.push_back(')');
    }
    return out;
}

void check_key_spelling(const JsonMembers& m,
                        const std::vector<std::string>& key_columns,
                        const TableSchema& schema) {
    for (const auto& k : key_columns) {
        const auto it = m.find(k);
        const auto* c = schema.column(k);
        if (it == m.end() || c == nullptr || it->second.kind == JsonMember::Kind::Null) {
            continue;  // stream_key refuses a missing or null key
        }
        const bool text = c->data_type == "varchar" || c->data_type == "varbinary";
        if (it->second.kind != (text ? JsonMember::Kind::String : JsonMember::Kind::Number)) {
            throw RecordRefused("its key member \"" + k + "\" is not a JSON " +
                                (text ? "string" : "number") + ", as column '" + k + "' (" +
                                c->column_type +
                                ") needs with more than one subtask: a key written two ways "
                                "can reach two subtasks, which write one row in either order");
        }
    }
}

std::string stream_key(const JsonMembers& m, const std::vector<std::string>& key_columns) {
    std::string out;
    for (const auto& k : key_columns) {
        const auto it = m.find(k);
        if (it == m.end() || it->second.kind == JsonMember::Kind::Null) {
            throw RecordRefused("its key member \"" + k +
                                "\" is missing or null, so it names no row");
        }
        // A number is identified by its value, so 1.0 and 1.00 are one key.
        const auto text = it->second.kind == JsonMember::Kind::Number
                              ? plain_decimal(it->second.text, kMaxNumberChars)
                              : it->second.text;
        out.push_back(static_cast<char>('0' + static_cast<int>(it->second.kind)));
        out += std::to_string(text.size());
        out.push_back(':');
        out += text;
    }
    return out;
}

// ---- the changelog marker -------------------------------------------------------------

ChangeKind change_kind_of(const JsonMembers& m) {
    const auto it = m.find(kRowKindMember);
    if (it == m.end()) {
        return ChangeKind::Upsert;
    }
    if (it->second.kind != JsonMember::Kind::String) {
        throw RecordRefused("its __row_kind is not a string");
    }
    const auto& k = it->second.text;
    if (k == "insert" || k == "update_after") {
        return ChangeKind::Upsert;
    }
    if (k == "delete" || k == "update_before") {
        return ChangeKind::Delete;
    }
    throw RecordRefused("its __row_kind is none of insert, update_after, delete and update_before");
}

// ---- statements -------------------------------------------------------------------------

std::vector<std::string> pack_statements(const std::string& head,
                                         const std::vector<std::string>& items,
                                         const std::string& tail,
                                         std::size_t max_bytes) {
    std::vector<std::string> out;
    std::string current;
    for (const auto& item : items) {
        if (head.size() + item.size() + tail.size() > max_bytes) {
            throw std::length_error("an item of " + std::to_string(item.size()) +
                                    " bytes does not fit a statement of at most " +
                                    std::to_string(max_bytes) + " bytes");
        }
        if (!current.empty() && current.size() + 1 + item.size() + tail.size() > max_bytes) {
            out.push_back(std::move(current) + tail);
            current.clear();
        }
        if (current.empty()) {
            current = head;
        } else {
            current.push_back(',');
        }
        current += item;
    }
    if (!current.empty()) {
        out.push_back(std::move(current) + tail);
    }
    return out;
}

std::string insert_head(const std::string& table, const std::vector<std::string>& columns) {
    std::string sql = "INSERT INTO " + quote_table_ident(table) + " (";
    for (std::size_t i = 0; i < columns.size(); ++i) {
        sql += (i > 0 ? "," : "") + quote_ident(columns[i]);
    }
    return sql + ") VALUES ";
}

std::string upsert_tail(const std::vector<std::string>& set_columns) {
    std::string sql = " AS clink_new ON DUPLICATE KEY UPDATE ";
    for (std::size_t i = 0; i < set_columns.size(); ++i) {
        const auto q = quote_ident(set_columns[i]);
        sql += (i > 0 ? "," : "") + q + "=clink_new." + q;
    }
    return sql;
}

std::string delete_head(const std::string& table, const std::vector<std::string>& key_columns) {
    std::string sql = "DELETE FROM " + quote_table_ident(table) + " WHERE ";
    if (key_columns.size() == 1) {
        return sql + quote_ident(key_columns.front()) + " IN (";
    }
    sql += "(";
    for (std::size_t i = 0; i < key_columns.size(); ++i) {
        sql += (i > 0 ? "," : "") + quote_ident(key_columns[i]);
    }
    return sql + ") IN (";
}

// ---- server messages ------------------------------------------------------------------

std::string redact_server_message(std::string_view message) {
    std::string out{message};
    static constexpr std::string_view kMarkers[] = {"entry '", "value: '", "value '"};
    for (const auto marker : kMarkers) {
        std::size_t from = 0;
        while ((from = out.find(marker, from)) != std::string::npos) {
            const auto begin = from + marker.size();
            auto end = out.find("' for ", begin);
            if (end == std::string::npos) {
                end = out.rfind('\'');
                if (end == std::string::npos || end < begin) {
                    break;
                }
            }
            out.replace(begin, end - begin, "<value>");
            from = begin + 7;
        }
    }
    return out;
}

// ---- the changelog buffer -------------------------------------------------------------

void ChangelogBuffer::add(const std::string& stream_key, Change change) {
    if (const auto it = index_.find(stream_key); it != index_.end()) {
        bytes_ -= weight(it->second->change);
        changes_.erase(it->second);
        index_.erase(it);
    }
    bytes_ += weight(change);
    changes_.push_back(Entry{stream_key, std::move(change)});
    index_.emplace(stream_key, std::prev(changes_.end()));
}

void ChangelogBuffer::clear() {
    changes_.clear();
    index_.clear();
    bytes_ = 0;
}

std::vector<std::pair<ChangeKind, std::vector<std::string>>> ChangelogBuffer::runs() const {
    std::vector<std::pair<ChangeKind, std::vector<std::string>>> out;
    for (const auto& e : changes_) {
        if (out.empty() || out.back().first != e.change.kind) {
            out.emplace_back(e.change.kind, std::vector<std::string>{});
        }
        out.back().second.push_back(e.change.kind == ChangeKind::Upsert ? e.change.row
                                                                        : e.change.key);
    }
    return out;
}

}  // namespace clink::mysql
