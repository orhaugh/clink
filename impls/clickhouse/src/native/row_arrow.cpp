#include "native/row_arrow.hpp"

#include <charconv>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include "clink/config/decimal.hpp"
#include "clink/config/json.hpp"
#include "clink/operators/json_value_expr.hpp"
#include "clink/sql/row_columnar_batcher.hpp"

#include "native/errors.hpp"

namespace clink::clickhouse::native {

namespace {

using clink::config::JsonValue;
namespace shared = clink::sql::row_columnar_detail;

// Builder calls fail only when Arrow cannot allocate. That is not a fault in
// the row, so it is not a ConversionError.
void arrow_ok(const arrow::Status& status) {
    if (!status.ok()) {
        throw std::runtime_error("clickhouse native sink: building the Arrow chunk failed: " +
                                 status.ToString());
    }
}

// The types whose top-level cells go through the shared batcher's rule.
bool shared_scalar(SqlKind kind) {
    switch (kind) {
        case SqlKind::BigInt:
        case SqlKind::Integer:
        case SqlKind::Real:
        case SqlKind::Double:
        case SqlKind::Boolean:
        case SqlKind::Varchar:
        case SqlKind::Decimal:
            return true;
        default:
            return false;
    }
}

// The declared spelling, for messages. A type built by hand rather than parsed
// has none, so one is rendered from its parts.
std::string type_name(const SqlType& t) {
    if (!t.spelling.empty()) {
        return t.spelling;
    }
    switch (t.kind) {
        case SqlKind::TinyInt:
            return "TINYINT";
        case SqlKind::SmallInt:
            return "SMALLINT";
        case SqlKind::Integer:
            return "INTEGER";
        case SqlKind::BigInt:
            return "BIGINT";
        case SqlKind::Real:
            return "REAL";
        case SqlKind::Double:
            return "DOUBLE";
        case SqlKind::Boolean:
            return "BOOLEAN";
        case SqlKind::Varchar:
            return "VARCHAR";
        case SqlKind::Decimal:
            return "DECIMAL(" + std::to_string(t.precision) + ", " + std::to_string(t.scale) + ")";
        case SqlKind::Date:
            return "DATE";
        case SqlKind::Timestamp:
            return "TIMESTAMP(" + std::to_string(t.precision) + ")" +
                   (t.with_time_zone ? " WITH TIME ZONE" : "");
        case SqlKind::Time:
            return "TIME";
        case SqlKind::Bytea:
            return "BYTEA";
        case SqlKind::Array:
            return (t.children.empty() ? std::string("?") : type_name(t.children.front())) +
                   " ARRAY";
        case SqlKind::Map:
            if (t.children.size() == 2) {
                return "MAP<" + type_name(t.children[0]) + ", " + type_name(t.children[1]) + ">";
            }
            return "MAP";
        case SqlKind::Row: {
            std::string out = "ROW<";
            for (std::size_t i = 0; i < t.children.size(); ++i) {
                if (i > 0) {
                    out += ", ";
                }
                out += (i < t.field_names.size() ? t.field_names[i] : std::string("?")) + " " +
                       type_name(t.children[i]);
            }
            return out + ">";
        }
        case SqlKind::Unsupported:
            break;
    }
    return "an unsupported type";
}

// What a cell of each owned type must hold, for the wrong-kind message.
const char* expectation(SqlKind kind) {
    switch (kind) {
        case SqlKind::TinyInt:
        case SqlKind::SmallInt:
            return "a whole number";
        case SqlKind::Integer:
        case SqlKind::BigInt:
        case SqlKind::Real:
        case SqlKind::Double:
            return "a number";
        case SqlKind::Boolean:
            return "true or false";
        case SqlKind::Decimal:
            return "a decimal number";
        case SqlKind::Timestamp:
            return "epoch milliseconds";
        case SqlKind::Date:
            return "days since 1970-01-01 or YYYY-MM-DD";
        case SqlKind::Array:
            return "an array";
        case SqlKind::Map:
        case SqlKind::Row:
            return "an object";
        default:
            return "a value";
    }
}

// Row data may be confidential, so a cell is described by its kind: a number
// or a decimal is shown, text only by its length, an array or an object only
// by its size.
std::string describe(const JsonValue& v) {
    if (clink::config::is_dec_string(v)) {
        return "decimal " + v.as_string().substr(1);
    }
    switch (v.type()) {
        case JsonValue::Type::Null:
            return "null";
        case JsonValue::Type::Bool:
            return "a boolean";
        case JsonValue::Type::Number:
        case JsonValue::Type::Int:
            return v.serialize(0);
        case JsonValue::Type::String:
            return "text(len " + std::to_string(v.as_string().size()) + ")";
        case JsonValue::Type::Array:
            return "array(size " + std::to_string(v.as_array().size()) + ")";
        case JsonValue::Type::Object:
            return "object(size " + std::to_string(v.as_object().size()) + ")";
    }
    return "a value";
}

bool shows_value(const JsonValue& v) {
    return v.is_number() || clink::config::is_dec_string(v);
}

// Text the cell's own value would be shown as, numbers only.
std::string value_text(const JsonValue& v) {
    return clink::config::is_dec_string(v) ? v.as_string().substr(1) : v.serialize(0);
}

enum class TextInt : std::uint8_t { Ok, NotDigits, OutOfRange };

// An optional '-' and digits, and nothing else: no sign, no space, no
// fraction, no exponent.
TextInt parse_text_int(std::string_view s, std::int64_t& out) {
    const char* first = s.data();
    const char* last = first + s.size();
    const auto [ptr, ec] = std::from_chars(first, last, out);
    if (ptr != last || s.empty()) {
        return TextInt::NotDigits;
    }
    if (ec == std::errc::result_out_of_range) {
        return TextInt::OutOfRange;
    }
    return ec == std::errc{} ? TextInt::Ok : TextInt::NotDigits;
}

bool is_leap(std::int64_t y) {
    return (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
}

unsigned days_in_month(std::int64_t y, unsigned m) {
    static constexpr unsigned kDays[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    return m == 2 && is_leap(y) ? 29U : kDays[m - 1];
}

// Text exactly -?YYYY-MM-DD naming a real day of the proleptic Gregorian
// calendar, as days since 1970-01-01. A four-digit year always fits int32.
std::optional<std::int32_t> parse_civil_date(std::string_view s) {
    const bool negative = s.starts_with('-');
    if (negative) {
        s.remove_prefix(1);
    }
    if (s.size() != 10 || s[4] != '-' || s[7] != '-') {
        return std::nullopt;
    }
    auto digits = [s](std::size_t pos, std::size_t n) -> std::optional<unsigned> {
        unsigned out = 0;
        for (std::size_t i = pos; i < pos + n; ++i) {
            if (s[i] < '0' || s[i] > '9') {
                return std::nullopt;
            }
            out = out * 10 + static_cast<unsigned>(s[i] - '0');
        }
        return out;
    };
    const auto year = digits(0, 4);
    const auto month = digits(5, 2);
    const auto day = digits(8, 2);
    if (!year || !month || !day || *month < 1 || *month > 12) {
        return std::nullopt;
    }
    const std::int64_t y = negative ? -static_cast<std::int64_t>(*year) : *year;
    if (*day < 1 || *day > days_in_month(y, *month)) {
        return std::nullopt;
    }
    return static_cast<std::int32_t>(
        clink::operators::value_expr_detail::days_from_civil(y, *month, *day));
}

// JSON text read back to the value it encodes, or null when it is not JSON.
JsonValue parse_json_text(const std::string& text) {
    try {
        return clink::config::parse(text);
    } catch (const clink::config::ParseError&) {
        return JsonValue{};
    }
}

enum class Fault : std::uint8_t { WrongKind, NotWhole, OutOfRange, TimestampText, DateText };

// Where the converter is: the declared column, the row within the Row batch,
// and the path into a composite cell. Every ConversionError is raised here, so
// its wording and its redaction live in one place.
class Cursor {
public:
    explicit Cursor(const std::string& column) : column_(column) {}

    void at_row(std::int64_t row) { row_ = row; }

    struct Step {
        enum class Kind : std::uint8_t { Element, Key, Value, Field };
        Kind kind;
        std::size_t index;
        const std::string* field;
    };

    // Pushes a path step for the life of a scope.
    class Scope {
    public:
        Scope(Cursor& cursor, Step step) : cursor_(cursor) { cursor_.path_.push_back(step); }
        ~Scope() { cursor_.path_.pop_back(); }
        Scope(const Scope&) = delete;
        Scope& operator=(const Scope&) = delete;

    private:
        Cursor& cursor_;
    };

    // While a map key is read, every failure describes the key text by its
    // length alone, never by the value parsed from it.
    class KeyText {
    public:
        KeyText(Cursor& cursor, const std::string& text)
            : cursor_(cursor), saved_(cursor.key_text_) {
            cursor_.key_text_ = &text;
        }
        ~KeyText() { cursor_.key_text_ = saved_; }
        KeyText(const KeyText&) = delete;
        KeyText& operator=(const KeyText&) = delete;

    private:
        Cursor& cursor_;
        const std::string* saved_;
    };

    [[noreturn]] void fail(Fault fault, const SqlType& t, const JsonValue& v) const {
        const std::string type = type_name(t);
        const bool key = key_text_ != nullptr;
        const std::string got =
            key ? "key text(len " + std::to_string(key_text_->size()) + ")" : describe(v);
        const std::string shown = !key && shows_value(v) ? "value " + value_text(v) : got;
        std::string reason;
        switch (fault) {
            case Fault::WrongKind:
                reason = std::string("expected ") + expectation(t.kind) + " for " + type +
                         ", got " + got;
                break;
            case Fault::NotWhole:
                reason = shown + " is not a whole number for " + type;
                break;
            case Fault::OutOfRange:
                reason = shown + " out of range for " + type;
                break;
            case Fault::TimestampText:
                reason = "timestamp text is not epoch milliseconds for " + type + ", got " + got;
                break;
            case Fault::DateText:
                reason =
                    "date text is not days or a valid YYYY-MM-DD date for " + type + ", got " + got;
                break;
        }
        throw ConversionError(column_, row_, where() + reason);
    }

private:
    const std::string& column_;
    std::int64_t row_{0};
    std::vector<Step> path_;
    const std::string* key_text_{nullptr};

    // "element 2, entry 0 value, field `x`: ", or nothing at the top level.
    // Positions count from 0, as the row does; a map's entries are in key
    // order, which is the order they are built in.
    [[nodiscard]] std::string where() const {
        std::string out;
        for (const auto& step : path_) {
            if (!out.empty()) {
                out += ", ";
            }
            switch (step.kind) {
                case Step::Kind::Element:
                    out += "element " + std::to_string(step.index);
                    break;
                case Step::Kind::Key:
                    out += "entry " + std::to_string(step.index) + " key";
                    break;
                case Step::Kind::Value:
                    out += "entry " + std::to_string(step.index) + " value";
                    break;
                case Step::Kind::Field:
                    out += "field `" + *step.field + "`";
                    break;
            }
        }
        return out.empty() ? out : out + ": ";
    }
};

// Appends cells under the sink's own rule: a cell of the wrong kind, or out of
// range, fails the row rather than becoming NULL. Used for the top-level
// columns of the types the shared batcher does not cover and for every
// element inside a composite, the shared types included.
//
// A columnar batch carries every type outside the shared set in its text
// fallback (row_columnar_detail::to_utf8), so once materialised a SMALLINT,
// TINYINT, TIMESTAMP or DATE cell holds its digits as text, and an ARRAY, MAP
// or ROW cell holds its JSON text. Those are the engine's own encoding of a
// valid value, not drift, so they are read back; any other text still fails.
class CellWriter {
public:
    explicit CellWriter(Cursor& cursor) : cursor_(cursor) {}

    void append(arrow::ArrayBuilder& b, const SqlType& t, const JsonValue* v) {
        if (v == nullptr || v->is_null()) {
            arrow_ok(b.AppendNull());
            return;
        }
        append_value(b, t, *v);
    }

private:
    Cursor& cursor_;

    [[noreturn]] void fail(Fault fault, const SqlType& t, const JsonValue& v) const {
        cursor_.fail(fault, t, v);
    }

    static bool is_plain_text(const JsonValue& v) {
        return v.is_string() && !clink::config::is_dec_string(v);
    }

    void append_value(arrow::ArrayBuilder& b, const SqlType& t, const JsonValue& v) {
        switch (t.kind) {
            case SqlKind::TinyInt:
                arrow_ok(static_cast<arrow::Int8Builder&>(b).Append(small_int<std::int8_t>(t, v)));
                return;
            case SqlKind::SmallInt:
                arrow_ok(
                    static_cast<arrow::Int16Builder&>(b).Append(small_int<std::int16_t>(t, v)));
                return;
            case SqlKind::Integer:
                append_integer(static_cast<arrow::Int32Builder&>(b), t, v);
                return;
            case SqlKind::BigInt:
                append_bigint(static_cast<arrow::Int64Builder&>(b), t, v);
                return;
            case SqlKind::Real:
                append_real(static_cast<arrow::FloatBuilder&>(b), t, v);
                return;
            case SqlKind::Double:
                if (!v.is_number()) {
                    fail(Fault::WrongKind, t, v);
                }
                arrow_ok(static_cast<arrow::DoubleBuilder&>(b).Append(v.as_number()));
                return;
            case SqlKind::Boolean:
                if (!v.is_bool()) {
                    fail(Fault::WrongKind, t, v);
                }
                arrow_ok(static_cast<arrow::BooleanBuilder&>(b).Append(v.as_bool()));
                return;
            case SqlKind::Varchar:
                // The shared rendering, which accepts every kind.
                arrow_ok(static_cast<arrow::StringBuilder&>(b).Append(shared::to_utf8(v)));
                return;
            case SqlKind::Decimal:
                append_decimal(static_cast<arrow::Decimal128Builder&>(b), t, v);
                return;
            case SqlKind::Timestamp:
                arrow_ok(static_cast<arrow::TimestampBuilder&>(b).Append(epoch_ms(t, v)));
                return;
            case SqlKind::Date:
                arrow_ok(static_cast<arrow::Date32Builder&>(b).Append(date_days(t, v)));
                return;
            case SqlKind::Array:
                append_array(static_cast<arrow::ListBuilder&>(b), t, v);
                return;
            case SqlKind::Map:
                append_map(static_cast<arrow::MapBuilder&>(b), t, v);
                return;
            case SqlKind::Row:
                append_row(static_cast<arrow::StructBuilder&>(b), t, v);
                return;
            case SqlKind::Time:
            case SqlKind::Bytea:
            case SqlKind::Unsupported:
                break;
        }
        // The constructor refuses these, so no builder exists for them.
        throw std::logic_error("clickhouse native sink: no Arrow layout for " + type_name(t));
    }

    // A whole number in [lo, hi]: an integer, a double with no fraction, or
    // digit text. Text that is not digits raises `text_fault`.
    std::int64_t whole(const SqlType& t,
                       const JsonValue& v,
                       std::int64_t lo,
                       std::int64_t hi,
                       Fault text_fault) const {
        std::int64_t out = 0;
        if (v.is_integral_number()) {
            out = v.as_int();
        } else if (v.is_number()) {
            const double d = v.as_number();
            if (std::isnan(d) || (std::isfinite(d) && d != std::trunc(d))) {
                fail(Fault::NotWhole, t, v);
            }
            if (!shared::double_fits_int64(d)) {
                fail(Fault::OutOfRange, t, v);
            }
            out = static_cast<std::int64_t>(d);
        } else if (is_plain_text(v)) {
            switch (parse_text_int(v.as_string(), out)) {
                case TextInt::Ok:
                    break;
                case TextInt::OutOfRange:
                    fail(Fault::OutOfRange, t, v);
                case TextInt::NotDigits:
                    fail(text_fault, t, v);
            }
        } else {
            fail(Fault::WrongKind, t, v);
        }
        if (out < lo || out > hi) {
            fail(Fault::OutOfRange, t, v);
        }
        return out;
    }

    // TINYINT and SMALLINT: a whole number in the type's range, or its digits.
    template <typename Int>
    Int small_int(const SqlType& t, const JsonValue& v) const {
        return static_cast<Int>(whole(t,
                                      v,
                                      std::numeric_limits<Int>::min(),
                                      std::numeric_limits<Int>::max(),
                                      Fault::WrongKind));
    }

    // Epoch milliseconds whatever the declared precision, stored as written:
    // the precision tells the converter nothing about the value.
    std::int64_t epoch_ms(const SqlType& t, const JsonValue& v) const {
        return whole(t,
                     v,
                     std::numeric_limits<std::int64_t>::min(),
                     std::numeric_limits<std::int64_t>::max(),
                     Fault::TimestampText);
    }

    // Days since 1970-01-01 as an int32, or text -?YYYY-MM-DD.
    std::int32_t date_days(const SqlType& t, const JsonValue& v) const {
        if (is_plain_text(v)) {
            std::int64_t days = 0;
            if (parse_text_int(v.as_string(), days) == TextInt::NotDigits) {
                if (const auto civil = parse_civil_date(v.as_string())) {
                    return *civil;
                }
                fail(Fault::DateText, t, v);
            }
        }
        return static_cast<std::int32_t>(whole(t,
                                               v,
                                               std::numeric_limits<std::int32_t>::min(),
                                               std::numeric_limits<std::int32_t>::max(),
                                               Fault::DateText));
    }

    // The shared BIGINT rule, failing where it would give NULL: an integer
    // exactly, a double truncated when an int64 holds it.
    void append_bigint(arrow::Int64Builder& b, const SqlType& t, const JsonValue& v) const {
        if (v.is_integral_number()) {
            arrow_ok(b.Append(v.as_int()));
        } else if (v.is_number()) {
            if (!shared::double_fits_int64(v.as_number())) {
                fail(Fault::OutOfRange, t, v);
            }
            arrow_ok(b.Append(static_cast<std::int64_t>(v.as_number())));
        } else {
            fail(Fault::WrongKind, t, v);
        }
    }

    // The shared INTEGER rule, failing where it would give NULL.
    void append_integer(arrow::Int32Builder& b, const SqlType& t, const JsonValue& v) const {
        constexpr auto kLo = std::numeric_limits<std::int32_t>::min();
        constexpr auto kHi = std::numeric_limits<std::int32_t>::max();
        if (v.is_integral_number()) {
            if (v.as_int() < kLo || v.as_int() > kHi) {
                fail(Fault::OutOfRange, t, v);
            }
            arrow_ok(b.Append(static_cast<std::int32_t>(v.as_int())));
        } else if (v.is_number()) {
            const double d = v.as_number();
            if (!std::isfinite(d) || d <= static_cast<double>(kLo) - 1.0 ||
                d >= static_cast<double>(kHi) + 1.0) {
                fail(Fault::OutOfRange, t, v);
            }
            arrow_ok(b.Append(static_cast<std::int32_t>(d)));
        } else {
            fail(Fault::WrongKind, t, v);
        }
    }

    // A finite double past float's range has no float value; the cast would be
    // undefined, so it fails instead.
    void append_real(arrow::FloatBuilder& b, const SqlType& t, const JsonValue& v) const {
        if (!v.is_number()) {
            fail(Fault::WrongKind, t, v);
        }
        const double d = v.as_number();
        if (std::isfinite(d) &&
            std::fabs(d) > static_cast<double>(std::numeric_limits<float>::max())) {
            fail(Fault::OutOfRange, t, v);
        }
        arrow_ok(b.Append(static_cast<float>(d)));
    }

    // A decimal string, an integer read exactly, or a double read through its
    // shortest round-trip text, which is the numeral the source wrote. JSON
    // decoding turns only top-level DECIMAL columns into decimal strings, so a
    // decimal inside a composite arrives as a plain number. Rescaled half up,
    // as the shared rule does, and refused when the declared precision cannot
    // hold it.
    void append_decimal(arrow::Decimal128Builder& b, const SqlType& t, const JsonValue& v) const {
        std::optional<clink::config::Decimal> d;
        if (clink::config::is_dec_string(v)) {
            d = clink::config::dec_parse(v.as_string());
        } else if (v.is_integral_number()) {
            d = clink::config::Decimal{arrow::Decimal128(v.as_int()), 0};
        } else if (v.is_number()) {
            if (!std::isfinite(v.as_number())) {
                fail(Fault::OutOfRange, t, v);
            }
            d = clink::config::dec_parse(v.serialize(0));
        }
        if (!d) {
            fail(Fault::WrongKind, t, v);
        }
        const auto scaled = clink::config::dec_rescale(*d, t.scale);
        if (!scaled || !scaled->unscaled.FitsInPrecision(t.precision)) {
            fail(Fault::OutOfRange, t, v);
        }
        arrow_ok(b.Append(scaled->unscaled));
    }

    // The value itself, or the value its JSON text encodes, when that is of
    // the kind wanted; nullptr otherwise.
    static const JsonValue* composite(const JsonValue& v, JsonValue& parsed, bool want_array) {
        const JsonValue* c = &v;
        if (is_plain_text(v)) {
            parsed = parse_json_text(v.as_string());
            c = &parsed;
        }
        return (want_array ? c->is_array() : c->is_object()) ? c : nullptr;
    }

    void append_array(arrow::ListBuilder& b, const SqlType& t, const JsonValue& v) {
        JsonValue parsed;
        const JsonValue* list = composite(v, parsed, /*want_array=*/true);
        if (list == nullptr) {
            fail(Fault::WrongKind, t, v);
        }
        arrow_ok(b.Append());
        arrow::ArrayBuilder& values = *b.value_builder();
        const SqlType& element = t.children.front();
        const auto& items = list->as_array();
        for (std::size_t i = 0; i < items.size(); ++i) {
            const Cursor::Scope at(cursor_, {Cursor::Step::Kind::Element, i, nullptr});
            append(values, element, &items[i]);
        }
    }

    void append_map(arrow::MapBuilder& b, const SqlType& t, const JsonValue& v) {
        JsonValue parsed;
        const JsonValue* object = composite(v, parsed, /*want_array=*/false);
        if (object == nullptr) {
            fail(Fault::WrongKind, t, v);
        }
        arrow_ok(b.Append());
        arrow::ArrayBuilder& keys = *b.key_builder();
        arrow::ArrayBuilder& items = *b.item_builder();
        std::size_t i = 0;
        for (const auto& [key, value] : object->as_object()) {
            {
                const Cursor::Scope at(cursor_, {Cursor::Step::Kind::Key, i, nullptr});
                append_key(keys, t.children[0], key);
            }
            {
                const Cursor::Scope at(cursor_, {Cursor::Step::Kind::Value, i, nullptr});
                append(items, t.children[1], &value);
            }
            ++i;
        }
    }

    // A map key is its canonical key text (operators::value_expr_detail::
    // to_map_key): a string verbatim, a decimal without its tag, anything else
    // as its JSON text. Read back by the key type's own rule.
    void append_key(arrow::ArrayBuilder& b, const SqlType& t, const std::string& text) {
        const Cursor::KeyText redact(cursor_, text);
        switch (t.kind) {
            case SqlKind::Varchar:
                arrow_ok(static_cast<arrow::StringBuilder&>(b).Append(text));
                return;
            case SqlKind::Timestamp:
            case SqlKind::Date:
                append_value(b, t, JsonValue{text});
                return;
            case SqlKind::Decimal: {
                const auto d = clink::config::dec_parse(text);
                if (!d) {
                    fail(Fault::WrongKind, t, JsonValue{text});
                }
                append_value(b, t, clink::config::make_dec_value(*d));
                return;
            }
            default: {
                // A key is never null, so text that is not JSON, or is JSON
                // null, is the wrong kind.
                const JsonValue parsed = parse_json_text(text);
                if (parsed.is_null()) {
                    fail(Fault::WrongKind, t, JsonValue{text});
                }
                append_value(b, t, parsed);
                return;
            }
        }
    }

    void append_row(arrow::StructBuilder& b, const SqlType& t, const JsonValue& v) {
        JsonValue parsed;
        const JsonValue* object = composite(v, parsed, /*want_array=*/false);
        if (object == nullptr) {
            fail(Fault::WrongKind, t, v);
        }
        arrow_ok(b.Append());
        const auto& fields = object->as_object();
        for (std::size_t i = 0; i < t.children.size(); ++i) {
            const std::string& name = t.field_names[i];
            const Cursor::Scope at(cursor_, {Cursor::Step::Kind::Field, i, &name});
            const auto it = fields.find(name);
            append(*b.field_builder(static_cast<int>(i)),
                   t.children[i],
                   it == fields.end() ? nullptr : &it->second);
        }
    }
};

}  // namespace

struct RowArrowBuilder::Impl {
    struct Column {
        SqlColumn declared;
        std::shared_ptr<arrow::DataType> type;
        bool shared{false};
    };
    std::vector<Column> columns;
    std::shared_ptr<arrow::Schema> schema;
};

RowArrowBuilder::RowArrowBuilder(std::vector<SqlColumn> columns) : impl_(std::make_unique<Impl>()) {
    arrow::FieldVector fields;
    fields.reserve(columns.size());
    impl_->columns.reserve(columns.size());
    for (auto& c : columns) {
        auto type = arrow_type_for(c.type);
        if (!type) {
            throw NativeSinkError(code::kColumnPlan,
                                  "column `" + c.name + "`: " + type_name(c.type) +
                                      " has no Arrow layout in the native sink");
        }
        fields.push_back(arrow::field(c.name, type, /*nullable=*/true));
        const bool is_shared = shared_scalar(c.type.kind);
        impl_->columns.push_back(Impl::Column{std::move(c), std::move(type), is_shared});
    }
    impl_->schema = arrow::schema(std::move(fields));
}

RowArrowBuilder::~RowArrowBuilder() = default;
RowArrowBuilder::RowArrowBuilder(RowArrowBuilder&&) noexcept = default;
RowArrowBuilder& RowArrowBuilder::operator=(RowArrowBuilder&&) noexcept = default;

std::shared_ptr<arrow::RecordBatch> RowArrowBuilder::build(const Batch<sql::Row>& batch) const {
    // Materialises a columnar batch. A sidecar whose decoder cannot read its
    // schema yields no rows, and building from that would drop them in
    // silence.
    const auto& records = batch.records();
    if (records.size() != batch.size()) {
        throw std::logic_error("clickhouse native sink: a columnar batch of " +
                               std::to_string(batch.size()) + " rows materialised " +
                               std::to_string(records.size()));
    }
    const auto rows = static_cast<std::int64_t>(records.size());

    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.reserve(impl_->columns.size());
    for (const auto& c : impl_->columns) {
        const std::string& name = c.declared.name;
        std::shared_ptr<arrow::Array> array;
        if (c.shared) {
            // The batcher's own column builder, cell rule and lookup.
            array = shared::build_column(name, c.type, batch);
            if (!array) {
                throw std::runtime_error(
                    "clickhouse native sink: building the Arrow chunk failed for column `" + name +
                    "`");
            }
        } else {
            auto made = arrow::MakeBuilder(c.type, arrow::default_memory_pool());
            arrow_ok(made.status());
            std::unique_ptr<arrow::ArrayBuilder> builder = std::move(made).ValueUnsafe();
            arrow_ok(builder->Reserve(rows));
            Cursor cursor(name);
            CellWriter writer(cursor);
            for (std::int64_t i = 0; i < rows; ++i) {
                cursor.at_row(i);
                writer.append(*builder,
                              c.declared.type,
                              shared::field(records[static_cast<std::size_t>(i)].value(), name));
            }
            arrow_ok(builder->Finish(&array));
        }
        arrays.push_back(std::move(array));
    }
    return arrow::RecordBatch::Make(impl_->schema, rows, std::move(arrays));
}

const std::shared_ptr<arrow::Schema>& RowArrowBuilder::schema() const noexcept {
    return impl_->schema;
}

}  // namespace clink::clickhouse::native
