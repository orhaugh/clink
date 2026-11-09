// The SQL sinks' shared record rules. See sink_record_rules.hpp.

#include "clink/connectors/sink_record_rules.hpp"

#include <cstdint>
#include <iterator>
#include <string>
#include <utility>

namespace clink::sinkrules {

namespace {

// The longest plain decimal a stream key takes a number's value in; a longer
// number keeps its token.
constexpr std::size_t kStreamKeyNumberChars = 4096;

// A number's exponent is read up to this magnitude: far past any plain decimal
// a sink writes.
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

namespace {

// The end of the JSON string that starts at `i` (its opening quote), and its
// decoded text in `out`. The text has passed read_json_object, so it is well
// formed.
std::size_t scan_string(std::string_view s, std::size_t i, std::string* out) {
    ++i;
    while (i < s.size() && s[i] != '"') {
        if (s[i] != '\\') {
            if (out != nullptr) {
                out->push_back(s[i]);
            }
            ++i;
            continue;
        }
        const char e = s[i + 1];
        if (e != 'u') {
            if (out != nullptr) {
                static constexpr std::string_view kFrom = "\"\\/bfnrt";
                static constexpr std::string_view kTo = "\"\\/\b\f\n\r\t";
                out->push_back(kTo[kFrom.find(e)]);
            }
            i += 2;
            continue;
        }
        const auto hex = [&](std::size_t at) {
            std::uint32_t v = 0;
            for (std::size_t k = 0; k < 4; ++k) {
                v = (v << 4U) | static_cast<std::uint32_t>(hex_value(s[at + k]));
            }
            return v;
        };
        std::uint32_t cp = hex(i + 2);
        i += 6;
        if (cp >= 0xD800 && cp <= 0xDBFF) {
            cp = 0x10000 + ((cp - 0xD800) << 10U) + (hex(i + 2) - 0xDC00);
            i += 6;
        }
        if (out != nullptr) {
            put_utf8(*out, cp);
        }
    }
    return i + 1;
}

// The end of the value that starts at `i`.
std::size_t scan_value(std::string_view s, std::size_t i) {
    if (s[i] == '"') {
        return scan_string(s, i, nullptr);
    }
    if (s[i] != '{' && s[i] != '[') {
        while (i < s.size() && s[i] != ',' && s[i] != '}' && s[i] != ']' && s[i] != ' ' &&
               s[i] != '\t' && s[i] != '\n' && s[i] != '\r') {
            ++i;
        }
        return i;
    }
    int depth = 0;
    while (i < s.size()) {
        const char c = s[i];
        if (c == '"') {
            i = scan_string(s, i, nullptr);
            continue;
        }
        if (c == '{' || c == '[') {
            ++depth;
        } else if (c == '}' || c == ']') {
            if (--depth == 0) {
                return i + 1;
            }
        }
        ++i;
    }
    return i;
}

std::size_t skip_space(std::string_view s, std::size_t i) {
    while (i < s.size() && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r')) {
        ++i;
    }
    return i;
}

}  // namespace

std::string remove_member(std::string_view text, std::string_view name) {
    std::size_t i = skip_space(text, 0) + 1;  // past '{'
    std::size_t previous_end = i;             // where the previous member ended
    bool first = true;
    while (true) {
        i = skip_space(text, i);
        if (i >= text.size() || text[i] == '}') {
            return std::string{text};
        }
        const std::size_t member_begin = i;
        std::string key;
        i = scan_string(text, i, &key);
        i = skip_space(text, i) + 1;  // past ':'
        i = scan_value(text, skip_space(text, i));
        const std::size_t member_end = i;
        i = skip_space(text, i);
        const bool comma = i < text.size() && text[i] == ',';
        if (key == name) {
            std::string out{text};
            if (comma) {
                // This member and the comma after it.
                out.erase(member_begin, skip_space(text, i + 1) - member_begin);
            } else if (!first) {
                // The last member, and the comma before it.
                out.erase(previous_end, member_end - previous_end);
            } else {
                out.erase(member_begin, member_end - member_begin);
            }
            return out;
        }
        first = false;
        previous_end = member_end;
        if (!comma) {
            return std::string{text};
        }
        ++i;
    }
}

// ---- keys ----------------------------------------------------------------------------

std::string stream_key(const JsonMembers& m, const std::vector<std::string>& key_columns) {
    std::string out;
    for (const auto& k : key_columns) {
        const auto it = m.find(k);
        if (it == m.end() || it->second.kind == JsonMember::Kind::Null) {
            throw RecordRefused("its key member \"" + k +
                                "\" is missing or null, so it names no row");
        }
        // A number is identified by its value, so 1.0 and 1.00 are one key; one
        // too long to write plainly keeps its token, under a kind of its own.
        std::string text = it->second.text;
        char kind = static_cast<char>('0' + static_cast<int>(it->second.kind));
        if (it->second.kind == JsonMember::Kind::Number) {
            if (auto plain = plain_decimal(text, kStreamKeyNumberChars); !plain.empty()) {
                text = std::move(plain);
            } else {
                kind = 'n';
            }
        }
        out.push_back(kind);
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

}  // namespace clink::sinkrules
