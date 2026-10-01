#include "native/types.hpp"

#include <charconv>
#include <cstddef>
#include <limits>
#include <optional>
#include <set>
#include <string_view>
#include <system_error>

#include <clickhouse/columns/factory.h>

// The client's type parser header declares its tokenizer with the client's
// own deprecated StringView. The AST it exposes is the only way to see, before
// calling the factory, whether the factory would build a null child column
// and then dereference it, so the warning is silenced for this include alone.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include <clickhouse/types/type_parser.h>
#pragma GCC diagnostic pop

#include "native/errors.hpp"

namespace clink::clickhouse::native {

namespace {

// Both parsers recurse once per nesting level. No real schema comes near this
// depth; the bound only keeps a hostile option value from exhausting the stack.
constexpr int kMaxNesting = 64;

// The sql_column_types spellings.

// Raised inside the declared-type parser when a composite spelling breaks the
// grammar. It never escapes: the column then parses as Unsupported, which the
// column plan refuses by name.
struct SqlSpellingBroken {};

SqlType unsupported_sql(std::string_view spelling) {
    SqlType t;
    t.kind = SqlKind::Unsupported;
    t.spelling = std::string(spelling);
    return t;
}

// Recursive descent over exactly the spellings arrow_to_sql_type_string
// renders. Anything else is not an error but an Unsupported type: the
// renderer's fallback is Arrow's own ToString, and the plan names it in its
// refusal. Inside a MAP or a ROW an unknown spelling is skipped to the end of
// its element, so the refusal can name the element rather than the whole
// column.
class SqlSpellingParser {
public:
    explicit SqlSpellingParser(std::string_view text) : s_(text) {}

    SqlType parse_whole() {
        try {
            SqlType t = type(0);
            if (pos_ == s_.size()) {
                return t;
            }
        } catch (const SqlSpellingBroken&) {
        }
        return unsupported_sql(s_);
    }

private:
    static constexpr std::string_view kArraySuffix = " ARRAY";

    std::string_view s_;
    std::size_t pos_{0};

    [[nodiscard]] std::string_view rest() const { return s_.substr(pos_); }

    // A type ends at the end of the text, at the separator or closer of the
    // composite it sits in, or at an ARRAY suffix that itself ends there.
    [[nodiscard]] bool ends_at(std::size_t p) const {
        if (p >= s_.size() || s_[p] == ',' || s_[p] == '>') {
            return true;
        }
        return array_suffix_at(p);
    }

    [[nodiscard]] bool array_suffix_at(std::size_t p) const {
        return p < s_.size() && s_.substr(p).starts_with(kArraySuffix) &&
               ends_at(p + kArraySuffix.size());
    }

    void expect(std::string_view text) {
        if (!rest().starts_with(text)) {
            throw SqlSpellingBroken{};
        }
        pos_ += text.size();
    }

    SqlType type(int depth) {
        if (depth > kMaxNesting) {
            throw SqlSpellingBroken{};
        }
        const std::size_t start = pos_;
        SqlType t = primary(depth);
        while (array_suffix_at(pos_)) {
            pos_ += kArraySuffix.size();
            SqlType outer;
            outer.kind = SqlKind::Array;
            outer.children.push_back(std::move(t));
            outer.spelling = std::string(s_.substr(start, pos_ - start));
            t = std::move(outer);
        }
        return t;
    }

    SqlType primary(int depth) {
        struct Keyword {
            std::string_view text;
            SqlKind kind;
        };
        // TIMESTAMP is tried below; TIME only matches when the type ends right
        // after it, so the shared prefix cannot mislead.
        static constexpr Keyword kScalars[] = {
            {"BIGINT", SqlKind::BigInt},
            {"INTEGER", SqlKind::Integer},
            {"SMALLINT", SqlKind::SmallInt},
            {"TINYINT", SqlKind::TinyInt},
            {"BOOLEAN", SqlKind::Boolean},
            {"REAL", SqlKind::Real},
            {"DOUBLE", SqlKind::Double},
            {"VARCHAR", SqlKind::Varchar},
            {"BYTEA", SqlKind::Bytea},
            {"DATE", SqlKind::Date},
            {"TIME", SqlKind::Time},
        };
        const std::size_t start = pos_;
        for (const auto& k : kScalars) {
            if (rest().starts_with(k.text) && ends_at(pos_ + k.text.size())) {
                pos_ += k.text.size();
                SqlType t;
                t.kind = k.kind;
                t.spelling = std::string(k.text);
                return t;
            }
        }
        if (auto t = timestamp()) {
            return std::move(*t);
        }
        if (auto t = decimal()) {
            return std::move(*t);
        }
        if (rest().starts_with("MAP<")) {
            pos_ += 4;
            SqlType t;
            t.kind = SqlKind::Map;
            t.children.push_back(type(depth + 1));
            expect(", ");
            t.children.push_back(type(depth + 1));
            expect(">");
            return finish_composite(std::move(t), start);
        }
        if (rest().starts_with("ROW<")) {
            pos_ += 4;
            SqlType t;
            t.kind = SqlKind::Row;
            while (true) {
                t.field_names.push_back(field_name());
                expect(" ");
                t.children.push_back(type(depth + 1));
                if (!rest().starts_with(", ")) {
                    break;
                }
                pos_ += 2;
            }
            expect(">");
            return finish_composite(std::move(t), start);
        }
        return opaque(start);
    }

    SqlType finish_composite(SqlType t, std::size_t start) {
        if (!ends_at(pos_)) {
            throw SqlSpellingBroken{};
        }
        t.spelling = std::string(s_.substr(start, pos_ - start));
        return t;
    }

    // ROW field names come from the SQL pre-parser, which admits identifier
    // characters only, so a space always ends one.
    std::string field_name() {
        const std::size_t start = pos_;
        while (pos_ < s_.size() && s_[pos_] != ' ' && s_[pos_] != ',' && s_[pos_] != '<' &&
               s_[pos_] != '>') {
            ++pos_;
        }
        if (pos_ == start) {
            throw SqlSpellingBroken{};
        }
        return std::string(s_.substr(start, pos_ - start));
    }

    std::optional<int> small_number() {
        const std::size_t start = pos_;
        while (pos_ < s_.size() && pos_ - start < 3 && s_[pos_] >= '0' && s_[pos_] <= '9') {
            ++pos_;
        }
        if (pos_ == start) {
            return std::nullopt;
        }
        int v = 0;
        const auto parsed = std::from_chars(s_.data() + start, s_.data() + pos_, v);
        if (parsed.ec != std::errc{}) {
            return std::nullopt;
        }
        return v;
    }

    // "TIMESTAMP(" digit ")" [" WITH TIME ZONE"]. A near miss rewinds and
    // falls through to the opaque path, so it is refused by its spelling.
    std::optional<SqlType> timestamp() {
        const std::size_t start = pos_;
        constexpr std::string_view kOpen = "TIMESTAMP(";
        constexpr std::string_view kZone = " WITH TIME ZONE";
        if (!rest().starts_with(kOpen) || pos_ + kOpen.size() + 1 >= s_.size()) {
            return std::nullopt;
        }
        const char digit = s_[pos_ + kOpen.size()];
        if (digit < '0' || digit > '9' || s_[pos_ + kOpen.size() + 1] != ')') {
            return std::nullopt;
        }
        pos_ += kOpen.size() + 2;
        SqlType t;
        t.kind = SqlKind::Timestamp;
        t.precision = digit - '0';
        if (rest().starts_with(kZone)) {
            pos_ += kZone.size();
            t.with_time_zone = true;
        }
        if (!ends_at(pos_)) {
            pos_ = start;
            return std::nullopt;
        }
        t.spelling = std::string(s_.substr(start, pos_ - start));
        return t;
    }

    // "DECIMAL(" p ", " s ")". Arrow's decimal128 holds precision 1 to 38 and
    // a scale no larger than the precision; anything else is not a type this
    // sink can build, so it is Unsupported under its own spelling.
    std::optional<SqlType> decimal() {
        const std::size_t start = pos_;
        constexpr std::string_view kOpen = "DECIMAL(";
        if (!rest().starts_with(kOpen)) {
            return std::nullopt;
        }
        pos_ += kOpen.size();
        const auto p = small_number();
        bool ok = p.has_value() && rest().starts_with(", ");
        std::optional<int> sc;
        if (ok) {
            pos_ += 2;
            sc = small_number();
            ok = sc.has_value() && rest().starts_with(")");
        }
        if (ok) {
            ++pos_;
            ok = ends_at(pos_);
        }
        if (!ok) {
            pos_ = start;
            return std::nullopt;
        }
        const std::string_view spelling = s_.substr(start, pos_ - start);
        if (*p < 1 || *p > 38 || *sc < 0 || *sc > *p) {
            return unsupported_sql(spelling);
        }
        SqlType t;
        t.kind = SqlKind::Decimal;
        t.precision = *p;
        t.scale = *sc;
        t.spelling = std::string(spelling);
        return t;
    }

    // An unknown spelling runs to the end of its element: the first comma,
    // closer or ARRAY suffix outside any bracket of its own. Arrow's fallback
    // spellings nest their arguments in <>, () or [], so this keeps
    // "dictionary<values=string, indices=int32, ordered=0>" whole.
    SqlType opaque(std::size_t start) {
        int depth = 0;
        std::size_t p = pos_;
        while (p < s_.size()) {
            const char c = s_[p];
            if (depth == 0 &&
                (c == ',' || c == '>' || c == ')' || c == ']' || array_suffix_at(p))) {
                break;
            }
            if (c == '<' || c == '(' || c == '[') {
                ++depth;
            } else if (c == '>' || c == ')' || c == ']') {
                --depth;
            }
            ++p;
        }
        if (p == pos_) {
            throw SqlSpellingBroken{};
        }
        pos_ = p;
        return unsupported_sql(s_.substr(start, p - start));
    }
};

[[noreturn]] void refuse_spec(const std::string& message) {
    throw NativeSinkError(code::kOptionInvalid, "option 'sql_column_types' " + message);
}

// The system.columns.type spellings.

// Raised inside the ClickHouse type parser for a spelling it cannot read. It
// never escapes parse_ch_type, which turns it into an Unsupported type whose
// reason says what was wrong.
struct ChSpellingBroken {
    std::string why;
};

constexpr std::string_view kNotSupported = " is not supported by the native sink";

ChType refused_ch(std::string_view spelling, std::string reason) {
    ChType t;
    t.kind = ChKind::Unsupported;
    t.spelling = std::string(spelling);
    t.unsupported_reason = std::move(reason);
    return t;
}

// A composite whose element is refused is refused for that element's reason,
// so the plan's message names the type the client cannot carry.
ChType carry_refusal(const ChType& child, std::string_view spelling) {
    return refused_ch(spelling, child.unsupported_reason);
}

bool is_composite(ChKind k) {
    return k == ChKind::Array || k == ChKind::Map || k == ChKind::Tuple;
}

std::optional<ChKind> plain_kind(std::string_view name) {
    struct Plain {
        std::string_view name;
        ChKind kind;
    };
    static constexpr Plain kPlain[] = {
        {"Int8", ChKind::Int8},       {"Int16", ChKind::Int16},     {"Int32", ChKind::Int32},
        {"Int64", ChKind::Int64},     {"Int128", ChKind::Int128},   {"UInt8", ChKind::UInt8},
        {"UInt16", ChKind::UInt16},   {"UInt32", ChKind::UInt32},   {"UInt64", ChKind::UInt64},
        {"UInt128", ChKind::UInt128}, {"Float32", ChKind::Float32}, {"Float64", ChKind::Float64},
        {"Bool", ChKind::Bool},       {"String", ChKind::String},   {"UUID", ChKind::UUID},
        {"IPv4", ChKind::IPv4},       {"IPv6", ChKind::IPv6},       {"Date", ChKind::Date},
        {"Date32", ChKind::Date32},
    };
    for (const auto& p : kPlain) {
        if (p.name == name) {
            return p.kind;
        }
    }
    return std::nullopt;
}

class ChSpellingParser {
public:
    explicit ChSpellingParser(std::string_view text) : s_(text) {}

    ChType parse_whole() {
        ChType t = type(0);
        skip_spaces();
        if (pos_ != s_.size()) {
            throw ChSpellingBroken{"unexpected text after the type"};
        }
        return t;
    }

private:
    std::string_view s_;
    std::size_t pos_{0};

    void skip_spaces() {
        while (pos_ < s_.size() && (s_[pos_] == ' ' || s_[pos_] == '\t' || s_[pos_] == '\n')) {
            ++pos_;
        }
    }

    bool peek(char c) {
        skip_spaces();
        return pos_ < s_.size() && s_[pos_] == c;
    }

    bool eat(char c) {
        if (peek(c)) {
            ++pos_;
            return true;
        }
        return false;
    }

    void expect(char c) {
        if (!eat(c)) {
            throw ChSpellingBroken{std::string("expected '") + c + "'"};
        }
    }

    // The type text from `start` to here, without surrounding spaces: the
    // spelling an element carries for messages.
    [[nodiscard]] std::string text_from(std::size_t start) const {
        std::string_view t = s_.substr(start, pos_ - start);
        while (!t.empty() && t.back() == ' ') {
            t.remove_suffix(1);
        }
        return std::string(t);
    }

    static bool is_alpha(char c) { return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z'); }
    static bool is_digit(char c) { return c >= '0' && c <= '9'; }

    std::string_view identifier() {
        skip_spaces();
        const std::size_t start = pos_;
        if (pos_ < s_.size() && (is_alpha(s_[pos_]) || s_[pos_] == '_')) {
            ++pos_;
            while (pos_ < s_.size() &&
                   (is_alpha(s_[pos_]) || is_digit(s_[pos_]) || s_[pos_] == '_')) {
                ++pos_;
            }
        }
        return s_.substr(start, pos_ - start);
    }

    // A '...' literal as the server writes one: a backslash escapes the next
    // character, with the usual letters standing for control characters.
    std::string quoted_string() {
        expect('\'');
        std::string out;
        while (pos_ < s_.size()) {
            const char c = s_[pos_++];
            if (c == '\'') {
                return out;
            }
            if (c != '\\') {
                out.push_back(c);
                continue;
            }
            if (pos_ >= s_.size()) {
                break;
            }
            const char e = s_[pos_++];
            switch (e) {
                case 'n':
                    out.push_back('\n');
                    break;
                case 't':
                    out.push_back('\t');
                    break;
                case 'r':
                    out.push_back('\r');
                    break;
                case '0':
                    out.push_back('\0');
                    break;
                case 'b':
                    out.push_back('\b');
                    break;
                case 'f':
                    out.push_back('\f');
                    break;
                case 'a':
                    out.push_back('\a');
                    break;
                case 'v':
                    out.push_back('\v');
                    break;
                default:
                    out.push_back(e);
                    break;
            }
        }
        throw ChSpellingBroken{"unterminated string literal"};
    }

    // A `...` or "..." identifier: the quote doubled, or after a backslash,
    // stands for itself.
    std::string quoted_identifier() {
        skip_spaces();
        const char quote = s_[pos_++];
        std::string out;
        while (pos_ < s_.size()) {
            const char c = s_[pos_++];
            if (c == '\\' && pos_ < s_.size()) {
                out.push_back(s_[pos_++]);
            } else if (c == quote) {
                if (pos_ < s_.size() && s_[pos_] == quote) {
                    out.push_back(quote);
                    ++pos_;
                } else {
                    return out;
                }
            } else {
                out.push_back(c);
            }
        }
        throw ChSpellingBroken{"unterminated quoted identifier"};
    }

    std::int64_t integer() {
        skip_spaces();
        const std::size_t start = pos_;
        if (pos_ < s_.size() && s_[pos_] == '-') {
            ++pos_;
        }
        while (pos_ < s_.size() && is_digit(s_[pos_])) {
            ++pos_;
        }
        std::int64_t v = 0;
        const auto [end, ec] = std::from_chars(s_.data() + start, s_.data() + pos_, v);
        if (ec != std::errc{} || end != s_.data() + pos_) {
            throw ChSpellingBroken{"expected an integer"};
        }
        return v;
    }

    // Skips a parenthesised argument list of a type the sink refuses by
    // name, so that the refusal can quote the whole spelling.
    void skip_arguments() {
        if (!peek('(')) {
            return;
        }
        int depth = 0;
        while (pos_ < s_.size()) {
            const char c = s_[pos_];
            if (c == '\'') {
                (void)quoted_string();
                continue;
            }
            if (c == '`' || c == '"') {
                (void)quoted_identifier();
                continue;
            }
            ++pos_;
            if (c == '(') {
                ++depth;
            } else if (c == ')' && --depth == 0) {
                return;
            }
        }
        throw ChSpellingBroken{"unbalanced parentheses"};
    }

    ChType one_argument(int depth) {
        expect('(');
        ChType inner = type(depth + 1);
        expect(')');
        return inner;
    }

    ChType type(int depth) {
        if (depth > kMaxNesting) {
            throw ChSpellingBroken{"nested too deeply"};
        }
        skip_spaces();
        const std::size_t start = pos_;
        const std::string_view name = identifier();
        if (name.empty()) {
            throw ChSpellingBroken{"expected a type name"};
        }

        if (name == "Nullable") {
            ChType inner = one_argument(depth);
            const std::string spelling = text_from(start);
            if (inner.kind == ChKind::Unsupported) {
                return carry_refusal(inner, spelling);
            }
            if (inner.nullable || inner.low_cardinality || is_composite(inner.kind)) {
                return refused_ch(spelling, spelling + std::string(kNotSupported));
            }
            inner.nullable = true;
            inner.spelling = spelling;
            return inner;
        }
        if (name == "LowCardinality") {
            ChType inner = one_argument(depth);
            const std::string spelling = text_from(start);
            if (inner.kind == ChKind::Unsupported) {
                return carry_refusal(inner, spelling);
            }
            // The client's factory builds LowCardinality over these three
            // only, and throws for the rest.
            const bool buildable =
                !inner.low_cardinality && (inner.kind == ChKind::String ||
                                           (inner.kind == ChKind::FixedString && !inner.nullable));
            if (!buildable) {
                return refused_ch(spelling,
                                  spelling + std::string(kNotSupported) +
                                      ": clickhouse-cpp builds LowCardinality only over String, "
                                      "FixedString(N) and Nullable(String)");
            }
            inner.low_cardinality = true;
            inner.spelling = spelling;
            return inner;
        }
        if (name == "Array") {
            ChType element = one_argument(depth);
            const std::string spelling = text_from(start);
            if (element.kind == ChKind::Unsupported) {
                return carry_refusal(element, spelling);
            }
            ChType t;
            t.kind = ChKind::Array;
            t.children.push_back(std::move(element));
            t.spelling = spelling;
            return t;
        }
        if (name == "Map") {
            expect('(');
            ChType key = type(depth + 1);
            expect(',');
            ChType value = type(depth + 1);
            expect(')');
            const std::string spelling = text_from(start);
            for (const ChType* part : {&key, &value}) {
                if (part->kind == ChKind::Unsupported) {
                    return carry_refusal(*part, spelling);
                }
            }
            ChType t;
            t.kind = ChKind::Map;
            t.children.push_back(std::move(key));
            t.children.push_back(std::move(value));
            t.spelling = spelling;
            return t;
        }
        if (name == "Tuple") {
            return tuple(depth, start);
        }
        if (name == "Decimal") {
            expect('(');
            const std::int64_t p = integer();
            expect(',');
            const std::int64_t sc = integer();
            expect(')');
            return decimal(p, sc, text_from(start));
        }
        if (name == "Decimal32" || name == "Decimal64" || name == "Decimal128") {
            const std::int64_t p = name == "Decimal32" ? 9 : (name == "Decimal64" ? 18 : 38);
            expect('(');
            const std::int64_t sc = integer();
            expect(')');
            return decimal(p, sc, text_from(start));
        }
        if (name == "Decimal256") {
            skip_arguments();
            return decimal_too_wide(text_from(start));
        }
        if (name == "DateTime") {
            ChType t;
            t.kind = ChKind::DateTime;
            if (eat('(')) {
                t.timezone = quoted_string();
                expect(')');
            }
            t.spelling = text_from(start);
            return t;
        }
        if (name == "DateTime64") {
            expect('(');
            const std::int64_t p = integer();
            ChType t;
            t.kind = ChKind::DateTime64;
            if (eat(',')) {
                skip_spaces();
                t.timezone = quoted_string();
            }
            expect(')');
            if (p < 0 || p > 9) {
                throw ChSpellingBroken{"DateTime64 precision must be 0 to 9"};
            }
            t.precision = static_cast<int>(p);
            t.spelling = text_from(start);
            return t;
        }
        if (name == "FixedString") {
            expect('(');
            const std::int64_t n = integer();
            expect(')');
            if (n < 1 || n > std::numeric_limits<std::uint32_t>::max()) {
                throw ChSpellingBroken{"FixedString length must be positive"};
            }
            ChType t;
            t.kind = ChKind::FixedString;
            t.fixed_size = static_cast<std::uint32_t>(n);
            t.spelling = text_from(start);
            return t;
        }
        if (name == "Enum8" || name == "Enum16") {
            return enumeration(name == "Enum8", start);
        }
        if (const auto kind = plain_kind(name)) {
            if (peek('(')) {
                throw ChSpellingBroken{std::string(name) + " takes no arguments"};
            }
            ChType t;
            t.kind = *kind;
            t.spelling = text_from(start);
            return t;
        }
        // Int256, UInt256, BFloat16, Variant, Dynamic, JSON, Object, Time,
        // Time64, the geo types, AggregateFunction, SimpleAggregateFunction,
        // Nothing, and every name this parser does not know. clickhouse-cpp
        // has no column for some of them and reads unknown names as Void.
        skip_arguments();
        const std::string spelling = text_from(start);
        return refused_ch(spelling, spelling + std::string(kNotSupported));
    }

    static ChType decimal_too_wide(const std::string& spelling) {
        return refused_ch(spelling,
                          spelling + std::string(kNotSupported) +
                              ": clickhouse-cpp holds a decimal in at most 128 bits, which caps "
                              "its precision at 38");
    }

    static ChType decimal(std::int64_t p, std::int64_t sc, const std::string& spelling) {
        if (p < 1 || p > 76 || sc < 0 || sc > p) {
            throw ChSpellingBroken{"Decimal precision or scale out of range"};
        }
        if (p > 38) {
            return decimal_too_wide(spelling);
        }
        ChType t;
        t.kind = ChKind::Decimal;
        t.precision = static_cast<int>(p);
        t.scale = static_cast<int>(sc);
        t.spelling = spelling;
        return t;
    }

    ChType enumeration(bool eight, std::size_t start) {
        const std::int64_t lo = eight ? std::numeric_limits<std::int8_t>::min()
                                      : std::numeric_limits<std::int16_t>::min();
        const std::int64_t hi = eight ? std::numeric_limits<std::int8_t>::max()
                                      : std::numeric_limits<std::int16_t>::max();
        ChType t;
        t.kind = eight ? ChKind::Enum8 : ChKind::Enum16;
        expect('(');
        do {
            skip_spaces();
            std::string item = quoted_string();
            expect('=');
            const std::int64_t v = integer();
            if (v < lo || v > hi) {
                throw ChSpellingBroken{"enum value out of range"};
            }
            t.enum_items.emplace_back(std::move(item), static_cast<std::int16_t>(v));
        } while (eat(','));
        expect(')');
        t.spelling = text_from(start);
        return t;
    }

    // Tuple(T, ...) or Tuple(name T, ...). An element is named when its first
    // word is quoted, or is followed by another word rather than by its own
    // arguments or the end of the element.
    ChType tuple(int depth, std::size_t start) {
        expect('(');
        ChType t;
        t.kind = ChKind::Tuple;
        std::vector<std::string> names;
        bool any_named = false;
        bool any_unnamed = false;
        do {
            std::string element_name;
            if (peek('`') || peek('"')) {
                element_name = quoted_identifier();
            } else {
                const std::size_t before = pos_;
                const std::string_view first = identifier();
                skip_spaces();
                const bool looks_named = !first.empty() && pos_ < s_.size() && s_[pos_] != ',' &&
                                         s_[pos_] != ')' && s_[pos_] != '(';
                if (looks_named) {
                    element_name = std::string(first);
                } else {
                    pos_ = before;
                }
            }
            const bool named = !element_name.empty();
            any_named = any_named || named;
            any_unnamed = any_unnamed || !named;
            names.push_back(std::move(element_name));
            t.children.push_back(type(depth + 1));
        } while (eat(','));
        expect(')');
        const std::string spelling = text_from(start);
        for (const auto& child : t.children) {
            if (child.kind == ChKind::Unsupported) {
                return carry_refusal(child, spelling);
            }
        }
        if (any_named && any_unnamed) {
            return refused_ch(
                spelling,
                spelling + std::string(kNotSupported) + ": it mixes named and unnamed elements");
        }
        if (any_named) {
            t.element_names = std::move(names);
        }
        t.spelling = spelling;
        return t;
    }
};

// What the client's factory can build.

bool client_builds(const ::clickhouse::TypeAst& ast);

// Mirrors the terminal switch of the client's factory: the codes it returns a
// column for, and the two that need an argument it does not check for.
bool client_builds_terminal(const ::clickhouse::TypeAst& ast) {
    using Code = ::clickhouse::Type::Code;
    switch (ast.code) {
        case Code::DateTime64:
        case Code::Time64:
            return !ast.elements.empty();
        case Code::Array:
        case Code::Nullable:
        case Code::Tuple:
        case Code::Enum8:
        case Code::Enum16:
        case Code::LowCardinality:
        case Code::Map:
            return false;
        default:
            return true;
    }
}

// Whether CreateColumnByType would return a column for this AST rather than
// null. Array and Nullable dereference their child's column without checking
// it, so a null child crashes the factory instead of failing it; this walk
// lets client_header_spelling return empty for those spellings. Spellings the
// factory throws on pass here, because the throw is caught.
bool client_builds(const ::clickhouse::TypeAst& ast) {
    using Meta = ::clickhouse::TypeAst::Meta;
    switch (ast.meta) {
        case Meta::Array:
        case Meta::Nullable:
            return !ast.elements.empty() && client_builds(ast.elements.front());
        case Meta::Terminal:
            return client_builds_terminal(ast);
        case Meta::Tuple:
        case Meta::Map:
            for (const auto& e : ast.elements) {
                if (!client_builds(e)) {
                    return false;
                }
            }
            return true;
        case Meta::Enum:
            return true;
        case Meta::LowCardinality: {
            if (ast.elements.empty()) {
                return false;
            }
            const auto& nested = ast.elements.front();
            if (nested.code == ::clickhouse::Type::Code::Nullable) {
                return !nested.elements.empty() && client_builds(nested.elements.front());
            }
            return true;
        }
        case Meta::SimpleAggregateFunction:
            return !ast.elements.empty() && client_builds_terminal(ast.elements.back());
        default:
            return false;
    }
}

}  // namespace

std::vector<SqlColumn> parse_sql_column_types(const std::string& spec) {
    if (spec.empty()) {
        refuse_spec("must not be empty");
    }
    std::vector<SqlColumn> columns;
    std::set<std::string, std::less<>> seen;
    std::string_view rest = spec;
    std::size_t position = 0;
    while (true) {
        ++position;
        const std::size_t semi = rest.find(';');
        const std::string_view entry = rest.substr(0, semi);
        if (entry.empty()) {
            refuse_spec("has an empty entry at position " + std::to_string(position));
        }
        const std::string quoted = "'" + std::string(entry) + "'";
        // Names never contain ':' (the planner's own rule), so the first one
        // ends the name; a type's own spelling may contain more.
        const std::size_t colon = entry.find(':');
        if (colon == std::string_view::npos) {
            refuse_spec("entry " + quoted + " is not name:TYPE");
        }
        const std::string_view name = entry.substr(0, colon);
        const std::string_view spelling = entry.substr(colon + 1);
        if (name.empty()) {
            refuse_spec("entry " + quoted + " has no column name");
        }
        if (spelling.empty()) {
            refuse_spec("entry " + quoted + " has no type");
        }
        if (!seen.emplace(name).second) {
            refuse_spec("names column '" + std::string(name) + "' twice");
        }
        columns.push_back(SqlColumn{std::string(name), SqlSpellingParser(spelling).parse_whole()});
        if (semi == std::string_view::npos) {
            break;
        }
        rest = rest.substr(semi + 1);
    }
    return columns;
}

std::shared_ptr<arrow::DataType> arrow_type_for(const SqlType& type) {
    switch (type.kind) {
        case SqlKind::TinyInt:
            return arrow::int8();
        case SqlKind::SmallInt:
            return arrow::int16();
        case SqlKind::Integer:
            return arrow::int32();
        case SqlKind::BigInt:
            return arrow::int64();
        case SqlKind::Real:
            return arrow::float32();
        case SqlKind::Double:
            return arrow::float64();
        case SqlKind::Boolean:
            return arrow::boolean();
        case SqlKind::Varchar:
            return arrow::utf8();
        case SqlKind::Decimal:
            // decimal128() aborts on a precision it cannot hold rather than
            // failing, so check before asking for one.
            if (type.precision < 1 || type.precision > 38 || type.scale < 0 ||
                type.scale > type.precision) {
                return nullptr;
            }
            return arrow::decimal128(type.precision, type.scale);
        case SqlKind::Date:
            return arrow::date32();
        case SqlKind::Timestamp:
            // A Row carries every TIMESTAMP(p) as epoch milliseconds, so the
            // declared precision changes nothing about the value.
            return type.with_time_zone ? arrow::timestamp(arrow::TimeUnit::MILLI, "UTC")
                                       : arrow::timestamp(arrow::TimeUnit::MILLI);
        case SqlKind::Array: {
            if (type.children.size() != 1) {
                return nullptr;
            }
            auto element = arrow_type_for(type.children.front());
            return element ? arrow::list(std::move(element)) : nullptr;
        }
        case SqlKind::Map: {
            if (type.children.size() != 2) {
                return nullptr;
            }
            auto key = arrow_type_for(type.children[0]);
            auto value = arrow_type_for(type.children[1]);
            if (!key || !value) {
                return nullptr;
            }
            return arrow::map(std::move(key), std::move(value));
        }
        case SqlKind::Row: {
            if (type.children.empty() || type.children.size() != type.field_names.size()) {
                return nullptr;
            }
            arrow::FieldVector fields;
            fields.reserve(type.children.size());
            for (std::size_t i = 0; i < type.children.size(); ++i) {
                auto field_type = arrow_type_for(type.children[i]);
                if (!field_type) {
                    return nullptr;
                }
                fields.push_back(arrow::field(type.field_names[i], std::move(field_type)));
            }
            return arrow::struct_(std::move(fields));
        }
        case SqlKind::Time:
        case SqlKind::Bytea:
        case SqlKind::Unsupported:
            return nullptr;
    }
    return nullptr;
}

ChType parse_ch_type(const std::string& spelling) {
    ChType out;
    try {
        out = ChSpellingParser(spelling).parse_whole();
    } catch (const ChSpellingBroken& broken) {
        out = refused_ch(
            spelling,
            "the native sink cannot read the ClickHouse type '" + spelling + "': " + broken.why);
    } catch (...) {
        out = refused_ch(spelling,
                         "the native sink cannot read the ClickHouse type '" + spelling + "'");
    }
    out.spelling = spelling;
    return out;
}

std::string client_header_spelling(const std::string& spelling) {
    try {
        const ::clickhouse::TypeAst* ast = ::clickhouse::ParseTypeName(spelling);
        if (ast == nullptr || !client_builds(*ast)) {
            return {};
        }
        const auto column = ::clickhouse::CreateColumnByType(spelling);
        if (!column) {
            return {};
        }
        return column->Type()->GetName();
    } catch (...) {
        return {};
    }
}

}  // namespace clink::clickhouse::native
