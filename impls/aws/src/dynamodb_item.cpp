// The DynamoDB sink's records: the JSON parse that maps a record to an item
// with every number kept exact, DynamoDB's numbers and their equality, item
// sizes, and the item identity buffered records are keyed by. The rules come
// from DynamoDB's documentation (Constraints, item sizes and formats); where it
// is approximate, as for a number's size, the sink takes the larger reading.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "clink/aws/dynamodb_sink.hpp"

namespace clink::aws {

namespace {

constexpr std::int64_t kExponentCap = 1'000'000'000;  // far past DynamoDB's range

// floor(a / 2) for any sign.
std::int64_t half_floor(std::int64_t a) {
    return a >= 0 ? a / 2 : -((-a + 1) / 2);
}

std::string shorten(std::string_view s, std::size_t max = 64) {
    return s.size() <= max ? std::string{s} : std::string{s.substr(0, max)} + "...";
}

// A string as JSON shows it, for messages: quoted, with every byte below 0x20,
// '"' and '\' escaped, cut at 64 bytes. Not named quoted(): with a std::string
// argument, lookup would also find std::quoted, and libstdc++ prefers it.
std::string json_quoted(std::string_view s) {
    std::string out = "\"";
    const auto cut = std::min<std::size_t>(s.size(), 64);
    for (std::size_t i = 0; i < cut; ++i) {
        const auto c = static_cast<unsigned char>(s[i]);
        if (c == '"' || c == '\\') {
            out.push_back('\\');
            out.push_back(static_cast<char>(c));
        } else if (c < 0x20) {
            static constexpr char kHex[] = "0123456789abcdef";
            out += "\\u00";
            out.push_back(kHex[c >> 4]);
            out.push_back(kHex[c & 0xF]);
        } else {
            out.push_back(static_cast<char>(c));
        }
    }
    out.push_back('"');
    if (cut < s.size()) {
        out += "...";
    }
    return out;
}

const char* type_name(DynamoDbKeyType t) {
    switch (t) {
        case DynamoDbKeyType::S:
            return "S";
        case DynamoDbKeyType::N:
            return "N";
        case DynamoDbKeyType::B:
            return "B";
    }
    return "?";
}

const char* kind_name(DynamoDbValue::Kind k) {
    switch (k) {
        case DynamoDbValue::Kind::S:
            return "a string";
        case DynamoDbValue::Kind::N:
            return "a number";
        case DynamoDbValue::Kind::Bool:
            return "a boolean";
        case DynamoDbValue::Kind::Null:
            return "null";
        case DynamoDbValue::Kind::M:
            return "an object";
        case DynamoDbValue::Kind::L:
            return "an array";
    }
    return "a value";
}

// Bytes a string takes in JSON at most: each byte escaped as the SDK's writer
// would (a control byte as \u00XX, '"' and '\' with a backslash), plus quotes.
std::size_t json_string_bytes(std::string_view s) {
    std::size_t n = 2;
    for (const char ch : s) {
        const auto c = static_cast<unsigned char>(ch);
        n += c < 0x20 ? 6 : (c == '"' || c == '\\') ? 2 : 1;
    }
    return n;
}

std::size_t value_bytes(const DynamoDbValue& v);

std::size_t number_bytes(const std::string& canonical) {
    const auto n = parse_dynamodb_number(canonical, /*json=*/false);
    // A canonical numeral always parses; were it not to, charge the most a
    // number can take (19 pairs and the exponent byte, and one for alignment).
    return n ? n->size_bytes() : 21;
}

std::size_t value_bytes(const DynamoDbValue& v) {
    switch (v.kind) {
        case DynamoDbValue::Kind::S:
            return v.text.size();
        case DynamoDbValue::Kind::N:
            return number_bytes(v.text);
        case DynamoDbValue::Kind::Bool:
        case DynamoDbValue::Kind::Null:
            return 1;
        case DynamoDbValue::Kind::M:
        case DynamoDbValue::Kind::L: {
            std::size_t n = 3;
            for (const auto& [name, member] : v.members) {
                n += name.size() + value_bytes(member) + 1;
            }
            return n;
        }
    }
    return 0;
}

// The bytes one AttributeValue at nesting depth `d` takes, at most, in the JSON
// the AWS SDK sends. The SDK writes request bodies indented: an AttributeValue
// is "{", a newline, d + 1 tabs, its type in quotes, ":", a tab, the payload, a
// newline, d tabs and "}". A map's members sit at d + 2, each on its own line;
// a list's elements are AttributeValues at d + 2, separated by ", ".
std::size_t value_json_bytes(const DynamoDbValue& v, std::size_t d) {
    std::size_t n = 2 * d + 14;
    switch (v.kind) {
        case DynamoDbValue::Kind::S:
            return n + json_string_bytes(v.text);
        case DynamoDbValue::Kind::N:
            return n + v.text.size() + 2;
        case DynamoDbValue::Kind::Bool:
            return n + 5;
        case DynamoDbValue::Kind::Null:
            return n + 4;
        case DynamoDbValue::Kind::M:
            n += 4 + d + 1;
            for (const auto& [name, member] : v.members) {
                n += (d + 2) + json_string_bytes(name) + 4 + value_json_bytes(member, d + 2);
            }
            return n;
        case DynamoDbValue::Kind::L:
            n += 2;
            for (const auto& [name, member] : v.members) {
                (void)name;
                n += value_json_bytes(member, d + 2) + 2;
            }
            return n;
    }
    return n;
}

// Where a request puts an item's attributes: RequestItems, the table's list,
// the list's element, PutRequest and Item enclose them, so each attribute's
// line is indented 6 tabs.
constexpr std::size_t kAttributeDepth = 6;

// ---- the record parse -----------------------------------------------------------

// A strict RFC 8259 parse of one record into an item. Strings must be valid
// UTF-8 with control characters escaped and surrogates paired, as the engine's
// own parser requires, and must not hold U+0000, which the SDK cannot send. Numbers are kept as
// written, never through a double. An object may not repeat a name: which value a duplicate meant
// is not knowable, and DynamoDB has no item with two attributes of one name.
class RecordParser {
public:
    explicit RecordParser(std::string_view in) : in_(in) {}

    DynamoDbItem parse_item() {
        skip_ws();
        if (pos_ >= in_.size() || in_[pos_] != '{') {
            fail("the record is not a JSON object");
        }
        auto members = parse_object_members(/*depth=*/1, /*top=*/true);
        skip_ws();
        if (pos_ != in_.size()) {
            fail("the record has content after its JSON object");
        }
        return members;
    }

private:
    [[noreturn]] void fail(const std::string& why) const {
        throw DynamoDbRecordError(why + " (at byte " + std::to_string(pos_) + ")");
    }

    void skip_ws() {
        while (pos_ < in_.size() &&
               (in_[pos_] == ' ' || in_[pos_] == '\t' || in_[pos_] == '\n' || in_[pos_] == '\r')) {
            ++pos_;
        }
    }

    // At '{'. Members' values sit at `depth` (an attribute's own value is 1).
    std::vector<std::pair<std::string, DynamoDbValue>> parse_object_members(int depth, bool top) {
        ++pos_;
        std::vector<std::pair<std::string, DynamoDbValue>> members;
        skip_ws();
        if (pos_ < in_.size() && in_[pos_] == '}') {
            ++pos_;
            return members;
        }
        for (;;) {
            skip_ws();
            if (pos_ >= in_.size() || in_[pos_] != '"') {
                fail("expected a quoted name in a JSON object");
            }
            std::string name = parse_string();
            if (name.empty()) {
                fail(top ? "an attribute name is empty; DynamoDB refuses an empty attribute name"
                         : "a map key is empty; DynamoDB refuses an empty attribute name");
            }
            if (name.size() > kDynamoDbMaxAttributeNameBytes) {
                fail("the attribute name starting " + json_quoted(name) + " is " +
                     std::to_string(name.size()) + " bytes; DynamoDB allows at most " +
                     std::to_string(kDynamoDbMaxAttributeNameBytes));
            }
            skip_ws();
            if (pos_ >= in_.size() || in_[pos_] != ':') {
                fail("expected ':' after a name in a JSON object");
            }
            ++pos_;
            skip_ws();
            DynamoDbValue value = parse_value(depth);
            members.emplace_back(std::move(name), std::move(value));
            skip_ws();
            if (pos_ < in_.size() && in_[pos_] == ',') {
                ++pos_;
                continue;
            }
            if (pos_ < in_.size() && in_[pos_] == '}') {
                ++pos_;
                break;
            }
            fail("expected ',' or '}' in a JSON object");
        }
        std::stable_sort(members.begin(), members.end(), [](const auto& a, const auto& b) {
            return a.first < b.first;
        });
        for (std::size_t i = 1; i < members.size(); ++i) {
            if (members[i].first == members[i - 1].first) {
                fail((top ? "the attribute " : "the map key ") + json_quoted(members[i].first) +
                     " appears twice in one object; the sink does not choose between them");
            }
        }
        return members;
    }

    DynamoDbValue parse_value(int depth) {
        if (pos_ >= in_.size()) {
            fail("the record ends where a value was expected");
        }
        DynamoDbValue v;
        const char c = in_[pos_];
        if (c == '{' || c == '[') {
            if (depth > kDynamoDbMaxContainerDepth) {
                fail("a map or list is nested more than " +
                     std::to_string(kDynamoDbMaxContainerDepth) +
                     " levels below its attribute; DynamoDB allows 32 levels counting the item");
            }
            if (c == '{') {
                v.kind = DynamoDbValue::Kind::M;
                v.members = parse_object_members(depth + 1, /*top=*/false);
            } else {
                v.kind = DynamoDbValue::Kind::L;
                v.members = parse_array(depth + 1);
            }
        } else if (c == '"') {
            v.kind = DynamoDbValue::Kind::S;
            v.text = parse_string();
        } else if (c == '-' || (c >= '0' && c <= '9')) {
            v.kind = DynamoDbValue::Kind::N;
            v.text = parse_number();
        } else if (literal("true")) {
            v.kind = DynamoDbValue::Kind::Bool;
            v.flag = true;
        } else if (literal("false")) {
            v.kind = DynamoDbValue::Kind::Bool;
        } else if (literal("null")) {
            v.kind = DynamoDbValue::Kind::Null;
        } else {
            fail("expected a JSON value");
        }
        return v;
    }

    bool literal(std::string_view word) {
        if (in_.substr(pos_, word.size()) != word) {
            return false;
        }
        pos_ += word.size();
        return true;
    }

    // At '['. Elements sit at `depth`.
    std::vector<std::pair<std::string, DynamoDbValue>> parse_array(int depth) {
        ++pos_;
        std::vector<std::pair<std::string, DynamoDbValue>> elements;
        skip_ws();
        if (pos_ < in_.size() && in_[pos_] == ']') {
            ++pos_;
            return elements;
        }
        for (;;) {
            skip_ws();
            elements.emplace_back(std::string{}, parse_value(depth));
            skip_ws();
            if (pos_ < in_.size() && in_[pos_] == ',') {
                ++pos_;
                continue;
            }
            if (pos_ < in_.size() && in_[pos_] == ']') {
                ++pos_;
                return elements;
            }
            fail("expected ',' or ']' in a JSON array");
        }
    }

    // The numeral at pos_, checked and made canonical.
    std::string parse_number() {
        const auto start = pos_;
        while (pos_ < in_.size()) {
            const char c = in_[pos_];
            if ((c >= '0' && c <= '9') || c == '-' || c == '+' || c == '.' || c == 'e' ||
                c == 'E') {
                ++pos_;
            } else {
                break;
            }
        }
        const auto numeral = in_.substr(start, pos_ - start);
        std::string why;
        const auto n = parse_dynamodb_number(numeral, /*json=*/true, &why);
        if (!n) {
            pos_ = start;
            fail("the number " + shorten(numeral) + " " + why);
        }
        return n->text();
    }

    unsigned hex4() {
        if (pos_ + 4 > in_.size()) {
            fail("a \\u escape needs four hex digits");
        }
        unsigned v = 0;
        for (int i = 0; i < 4; ++i) {
            const char c = in_[pos_++];
            v <<= 4;
            if (c >= '0' && c <= '9') {
                v |= static_cast<unsigned>(c - '0');
            } else if (c >= 'a' && c <= 'f') {
                v |= static_cast<unsigned>(c - 'a' + 10);
            } else if (c >= 'A' && c <= 'F') {
                v |= static_cast<unsigned>(c - 'A' + 10);
            } else {
                --pos_;
                fail("a \\u escape needs four hex digits");
            }
        }
        return v;
    }

    static void append_utf8(std::string& out, unsigned cp) {
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

    // The length of the well-formed UTF-8 sequence at pos_ (Unicode table 3-7),
    // or 0 when it is not one.
    std::size_t utf8_length() const {
        const auto byte = [&](std::size_t i) -> unsigned {
            return pos_ + i < in_.size() ? static_cast<unsigned char>(in_[pos_ + i]) : 0x100U;
        };
        const auto cont = [](unsigned b, unsigned lo = 0x80, unsigned hi = 0xBF) {
            return b >= lo && b <= hi;
        };
        const unsigned b0 = byte(0);
        if (b0 >= 0xC2 && b0 <= 0xDF) {
            return cont(byte(1)) ? 2 : 0;
        }
        if (b0 == 0xE0) {
            return cont(byte(1), 0xA0, 0xBF) && cont(byte(2)) ? 3 : 0;
        }
        if ((b0 >= 0xE1 && b0 <= 0xEC) || b0 == 0xEE || b0 == 0xEF) {
            return cont(byte(1)) && cont(byte(2)) ? 3 : 0;
        }
        if (b0 == 0xED) {  // excludes the surrogates D800-DFFF
            return cont(byte(1), 0x80, 0x9F) && cont(byte(2)) ? 3 : 0;
        }
        if (b0 == 0xF0) {
            return cont(byte(1), 0x90, 0xBF) && cont(byte(2)) && cont(byte(3)) ? 4 : 0;
        }
        if (b0 >= 0xF1 && b0 <= 0xF3) {
            return cont(byte(1)) && cont(byte(2)) && cont(byte(3)) ? 4 : 0;
        }
        if (b0 == 0xF4) {
            return cont(byte(1), 0x80, 0x8F) && cont(byte(2)) && cont(byte(3)) ? 4 : 0;
        }
        return 0;
    }

    // At '"'.
    std::string parse_string() {
        ++pos_;
        std::string out;
        for (;;) {
            if (pos_ >= in_.size()) {
                fail("a string is not terminated");
            }
            const auto c = static_cast<unsigned char>(in_[pos_]);
            if (c == '"') {
                ++pos_;
                return out;
            }
            if (c == '\\') {
                ++pos_;
                if (pos_ >= in_.size()) {
                    fail("a string ends inside an escape");
                }
                const char e = in_[pos_++];
                switch (e) {
                    case '"':
                    case '\\':
                    case '/':
                        out.push_back(e);
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
                        unsigned cp = hex4();
                        if (cp >= 0xD800 && cp <= 0xDBFF) {
                            if (pos_ + 2 > in_.size() || in_[pos_] != '\\' ||
                                in_[pos_ + 1] != 'u') {
                                fail("a high surrogate escape is not followed by a low one");
                            }
                            pos_ += 2;
                            const unsigned lo = hex4();
                            if (lo < 0xDC00 || lo > 0xDFFF) {
                                fail("a high surrogate escape is not followed by a low one");
                            }
                            cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                        } else if (cp >= 0xDC00 && cp <= 0xDFFF) {
                            fail("a low surrogate escape has no high surrogate before it");
                        } else if (cp == 0) {
                            // The AWS SDK for C++ ends a string at its first NUL when it
                            // writes the request, in names and values alike, so the item
                            // stored would hold a shorter string than the record: a key
                            // could even name another item.
                            fail(
                                "a string holds U+0000, which the AWS SDK for C++ cuts the "
                                "string at; DynamoDB would store a shorter string");
                        }
                        append_utf8(out, cp);
                        break;
                    }
                    default:
                        --pos_;
                        fail("a string has an unknown escape");
                }
                continue;
            }
            if (c < 0x20) {
                fail("a string holds an unescaped control character");
            }
            if (c < 0x80) {
                out.push_back(static_cast<char>(c));
                ++pos_;
                continue;
            }
            const auto len = utf8_length();
            if (len == 0) {
                fail("a string is not valid UTF-8");
            }
            out.append(in_.substr(pos_, len));
            pos_ += len;
        }
    }

    std::string_view in_;
    std::size_t pos_{0};
};

const DynamoDbValue* find_attribute(const DynamoDbItem& item, const std::string& name) {
    const auto it =
        std::lower_bound(item.begin(), item.end(), name, [](const auto& a, const std::string& n) {
            return a.first < n;
        });
    return it != item.end() && it->first == name ? &it->second : nullptr;
}

// A key attribute's value, or the reason the record's value cannot be one.
std::optional<DynamoDbKeyValue> key_value(const DynamoDbItem& item,
                                          const DynamoDbKeyAttribute& attr,
                                          const char* role,
                                          std::size_t max_bytes,
                                          std::string* why) {
    const auto set = [&](std::string s) {
        if (why != nullptr) {
            *why = std::move(s);
        }
        return std::nullopt;
    };
    const auto* v = find_attribute(item, attr.name);
    const std::string which = std::string{role} + " key attribute " + json_quoted(attr.name);
    if (v == nullptr) {
        return set("the " + which + " is missing");
    }
    if (attr.type == DynamoDbKeyType::B) {
        return set("the " + which + " is binary (B), which the sink does not write");
    }
    const bool want_string = attr.type == DynamoDbKeyType::S;
    if (v->kind == DynamoDbValue::Kind::Null) {
        return set("the " + which + " is null");
    }
    if ((want_string && v->kind != DynamoDbValue::Kind::S) ||
        (!want_string && v->kind != DynamoDbValue::Kind::N)) {
        return set("the " + which + " must be " + (want_string ? "a string" : "a number") +
                   " (the table declares it " + type_name(attr.type) + "), and the record has " +
                   kind_name(v->kind));
    }
    if (want_string) {
        if (v->text.empty()) {
            return set("the " + which + " is an empty string; DynamoDB refuses an empty key");
        }
        if (v->text.size() > max_bytes) {
            return set("the " + which + " is " + std::to_string(v->text.size()) +
                       " bytes; DynamoDB allows at most " + std::to_string(max_bytes) + " for a " +
                       role + " key");
        }
        return DynamoDbKeyValue{DynamoDbKeyType::S, v->text};
    }
    const auto n = parse_dynamodb_number(v->text, /*json=*/false);
    if (!n) {
        return set("the " + which + " is not a valid number");
    }
    return DynamoDbKeyValue{DynamoDbKeyType::N, n->text()};
}

}  // namespace

// ---- numbers ------------------------------------------------------------------------

std::optional<DynamoDbNumber> parse_dynamodb_number(std::string_view text,
                                                    bool json,
                                                    std::string* why) {
    const auto refuse = [&](std::string s) -> std::optional<DynamoDbNumber> {
        if (why != nullptr) {
            *why = std::move(s);
        }
        return std::nullopt;
    };
    const auto digit = [](char c) { return c >= '0' && c <= '9'; };
    std::size_t i = 0;
    bool negative = false;
    if (i < text.size() && (text[i] == '-' || (!json && text[i] == '+'))) {
        negative = text[i] == '-';
        ++i;
    }
    std::string all;  // the integer and fraction digits, in order
    const auto int_start = i;
    while (i < text.size() && digit(text[i])) {
        all.push_back(text[i++]);
    }
    const auto int_len = i - int_start;
    if (json && (int_len == 0 || (int_len > 1 && text[int_start] == '0'))) {
        return refuse("is not a JSON number");
    }
    std::size_t frac_len = 0;
    if (i < text.size() && text[i] == '.') {
        ++i;
        while (i < text.size() && digit(text[i])) {
            all.push_back(text[i++]);
            ++frac_len;
        }
        if (json && frac_len == 0) {
            return refuse("is not a JSON number");
        }
    }
    if (all.empty()) {
        return refuse("is not a number");
    }
    std::int64_t exp = 0;
    if (i < text.size() && (text[i] == 'e' || text[i] == 'E')) {
        ++i;
        bool exp_negative = false;
        if (i < text.size() && (text[i] == '-' || text[i] == '+')) {
            exp_negative = text[i] == '-';
            ++i;
        }
        const auto exp_start = i;
        while (i < text.size() && digit(text[i])) {
            if (exp < kExponentCap) {
                exp = exp * 10 + (text[i] - '0');
            }
            ++i;
        }
        if (i == exp_start) {
            return refuse("is not a number");
        }
        exp = exp_negative ? -std::min(exp, kExponentCap) : std::min(exp, kExponentCap);
    }
    if (i != text.size()) {
        return refuse(json ? "is not a JSON number" : "is not a number");
    }
    DynamoDbNumber n;
    const auto first = all.find_first_not_of('0');
    if (first == std::string::npos) {
        return n;  // zero, whatever its sign or exponent
    }
    const auto last = all.find_last_not_of('0');
    n.negative = negative;
    n.digits = all.substr(first, last - first + 1);
    n.exponent = exp - static_cast<std::int64_t>(frac_len) +
                 static_cast<std::int64_t>(all.size() - 1 - last);
    if (n.digits.size() > static_cast<std::size_t>(kDynamoDbMaxNumberDigits)) {
        return refuse("has " + std::to_string(n.digits.size()) +
                      " significant digits; DynamoDB keeps at most 38 (store it as a string)");
    }
    const std::int64_t scientific = n.exponent + static_cast<std::int64_t>(n.digits.size()) - 1;
    if (scientific < -130 || scientific > 125) {
        return refuse(
            "is outside DynamoDB's range: a magnitude from 1E-130 to "
            "9.9999999999999999999999999999999999999E+125, or zero");
    }
    return n;
}

std::string DynamoDbNumber::text() const {
    if (digits.empty()) {
        return "0";
    }
    const auto n = static_cast<std::int64_t>(digits.size());
    const std::int64_t scientific = exponent + n - 1;
    std::string out = negative ? "-" : "";
    if (exponent >= 0 && n + exponent <= 40) {
        out += digits;
        out.append(static_cast<std::size_t>(exponent), '0');
        return out;
    }
    if (exponent < 0 && scientific >= 0) {
        const auto int_digits = static_cast<std::size_t>(scientific + 1);
        out += digits.substr(0, int_digits);
        out.push_back('.');
        out += digits.substr(int_digits);
        return out;
    }
    if (scientific < 0 && 2 + (-scientific - 1) + n <= 40) {
        out += "0.";
        out.append(static_cast<std::size_t>(-scientific - 1), '0');
        out += digits;
        return out;
    }
    out.push_back(digits[0]);
    if (n > 1) {
        out.push_back('.');
        out += digits.substr(1);
    }
    out += scientific >= 0 ? "E+" : "E-";
    out += std::to_string(scientific >= 0 ? scientific : -scientific);
    return out;
}

std::size_t DynamoDbNumber::size_bytes() const {
    if (digits.empty()) {
        return 2;  // charged as one digit pair: no smaller than any other number
    }
    const std::int64_t lowest = exponent;
    const std::int64_t highest = exponent + static_cast<std::int64_t>(digits.size()) - 1;
    return static_cast<std::size_t>(half_floor(highest) - half_floor(lowest) + 1) + 1;
}

// ---- identity -----------------------------------------------------------------------

std::string describe_dynamodb_key(const DynamoDbItemKey& key) {
    const auto one = [](const DynamoDbKeyValue& v) {
        return v.type == DynamoDbKeyType::N ? v.bytes : json_quoted(v.bytes);
    };
    std::string out = "(" + one(key.partition);
    if (key.sort) {
        out += ", " + one(*key.sort);
    }
    return out + ")";
}

std::optional<DynamoDbItemKey> dynamodb_item_key(const DynamoDbItem& item,
                                                 const DynamoDbKeySchema& schema) {
    auto partition =
        key_value(item, schema.partition, "partition", kDynamoDbMaxPartitionKeyBytes, nullptr);
    if (!partition) {
        return std::nullopt;
    }
    DynamoDbItemKey key{std::move(*partition), std::nullopt};
    if (schema.sort) {
        auto sort = key_value(item, *schema.sort, "sort", kDynamoDbMaxSortKeyBytes, nullptr);
        if (!sort) {
            return std::nullopt;
        }
        key.sort = std::move(*sort);
    }
    return key;
}

// ---- records ------------------------------------------------------------------------

std::size_t dynamodb_item_bytes(const DynamoDbItem& item) {
    std::size_t n = 0;
    for (const auto& [name, value] : item) {
        n += name.size() + value_bytes(value);
    }
    return n;
}

std::size_t dynamodb_request_bytes(const DynamoDbItem& item) {
    // The list element around the item: {"PutRequest": {"Item": {...}}} with
    // its own lines and indentation, and the ", " before the next element.
    std::size_t n = 96;
    for (const auto& [name, value] : item) {
        n += kAttributeDepth + json_string_bytes(name) + 4 +
             value_json_bytes(value, kAttributeDepth);
    }
    return n;
}

DynamoDbPreparedItem prepare_dynamodb_item(std::string_view record,
                                           const DynamoDbKeySchema& schema) {
    DynamoDbPreparedItem out;
    out.item = RecordParser(record).parse_item();
    std::string why;
    auto partition =
        key_value(out.item, schema.partition, "partition", kDynamoDbMaxPartitionKeyBytes, &why);
    if (!partition) {
        throw DynamoDbRecordError(why);
    }
    out.key.partition = std::move(*partition);
    if (schema.sort) {
        auto sort = key_value(out.item, *schema.sort, "sort", kDynamoDbMaxSortKeyBytes, &why);
        if (!sort) {
            throw DynamoDbRecordError(why);
        }
        out.key.sort = std::move(*sort);
    }
    out.item_bytes = dynamodb_item_bytes(out.item);
    if (out.item_bytes > kDynamoDbMaxItemBytes) {
        throw DynamoDbRecordError("the item " + describe_dynamodb_key(out.key) + " is " +
                                  std::to_string(out.item_bytes) +
                                  " bytes by DynamoDB's size rules; DynamoDB allows at most " +
                                  std::to_string(kDynamoDbMaxItemBytes) + " (400 KB)");
    }
    out.request_bytes = dynamodb_request_bytes(out.item);
    return out;
}

}  // namespace clink::aws
