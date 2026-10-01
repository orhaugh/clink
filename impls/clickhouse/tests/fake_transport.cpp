#include "fake_transport.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cerrno>
#include <charconv>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <deque>
#include <exception>
#include <set>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <thread>

#include <clickhouse/columns/array.h>
#include <clickhouse/columns/column.h>
#include <clickhouse/columns/ip4.h>
#include <clickhouse/columns/ip6.h>
#include <clickhouse/columns/itemview.h>
#include <clickhouse/columns/lowcardinality.h>
#include <clickhouse/columns/map.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/tuple.h>
#include <clickhouse/error_codes.h>
#include <clickhouse/exceptions.h>
#include <clickhouse/types/types.h>

// The client's output stream header returns a pointer difference as size_t
// unconverted. Only its OutputStream base is used here, to serialise a column
// as the client does, so the warning is silenced for this include alone.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-conversion"
#include <clickhouse/base/output.h>
#pragma GCC diagnostic pop

#include "native/types.hpp"

namespace clink::clickhouse::native::testing {

namespace {

namespace ch = ::clickhouse;
using Clock = std::chrono::steady_clock;
using Deadline = std::optional<Clock::time_point>;

constexpr std::size_t kSteps = 5;

// How often a blocked call looks at what nothing notifies it of: a server
// switched down with its live connections broken.
constexpr auto kPollSlice = std::chrono::milliseconds{10};

[[noreturn]] void throw_system(int code, const std::string& message) {
    throw std::system_error(code, std::system_category(), message);
}

[[noreturn]] void throw_server(int code, const std::string& message) {
    auto e = std::make_shared<ch::Exception>();
    e->code = code;
    e->name = "DB::Exception";
    e->display_text = message;
    throw ch::ServerException(e);
}

std::string endpoint_text(const Endpoint& ep) {
    return ep.host + ":" + std::to_string(ep.port);
}

std::size_t step_index(Step step) {
    return static_cast<std::size_t>(step);
}

// ---------------------------------------------------------------------------
// The fault script

// The earliest-armed fault of `step`, once it has seen its nth call since it
// became the earliest. counts[step] holds the calls seen so far.
std::optional<Fault> take_fault(std::deque<Fault>& faults,
                                std::array<std::size_t, kSteps>& counts,
                                Step step) {
    const auto it = std::find_if(
        faults.begin(), faults.end(), [step](const Fault& f) { return f.step == step; });
    std::size_t& seen = counts[step_index(step)];
    if (it == faults.end()) {
        seen = 0;
        return std::nullopt;
    }
    if (++seen < it->nth) {
        return std::nullopt;
    }
    seen = 0;
    Fault fault = std::move(*it);
    faults.erase(it);
    return fault;
}

void validate_fault(const Fault& fault) {
    if (fault.nth == 0) {
        throw std::invalid_argument("fake server: a fault fires on its nth call, counted from 1");
    }
    if (fault.landing != Fault::Landing::Nothing && fault.step != Step::End) {
        throw std::invalid_argument("fake server: a landing applies to Step::End only");
    }
}

// The outcome of a fault whose kind is a failure.
[[noreturn]] void raise_failure(const Fault& fault) {
    switch (fault.kind) {
        case Fault::Kind::ServerError:
            throw_server(fault.code, fault.message);
        case Fault::Kind::SystemError:
            throw_system(fault.code != 0 ? fault.code : ECONNRESET, fault.message);
        case Fault::Kind::TlsError:
            throw ch::OpenSSLError(fault.message);
        case Fault::Kind::ProtocolError:
            throw ch::ProtocolError(fault.message);
        case Fault::Kind::CompressionError:
            throw ch::CompressionError(fault.message);
        case Fault::Kind::Unimplemented:
            throw ch::UnimplementedError(fault.message);
        case Fault::Kind::ValidationError:
            throw ch::ValidationError(fault.message);
        case Fault::Kind::BadOptionalAccess:
            throw std::bad_optional_access();
        case Fault::Kind::Other:
            throw std::runtime_error(fault.message);
        case Fault::Kind::Delay:
        case Fault::Kind::Hang:
        case Fault::Kind::Uninterruptible:
            break;
    }
    throw std::logic_error("fake server: raise_failure called for a kind that does not fail");
}

bool is_failure(Fault::Kind kind) {
    return kind != Fault::Kind::Delay && kind != Fault::Kind::Hang &&
           kind != Fault::Kind::Uninterruptible;
}

// Process-wide, because a faulty() wrapper has no server to release it, and
// deliberately leaked, so a detached thread still held at exit never waits on
// a destroyed condition variable.
struct HoldGate {
    std::mutex mu;
    std::condition_variable cv;
    std::uint64_t generation{0};
};

HoldGate& hold_gate() {
    static auto* const gate = new HoldGate;
    return *gate;
}

void hold_until_release() {
    HoldGate& gate = hold_gate();
    std::unique_lock<std::mutex> lock(gate.mu);
    const std::uint64_t entered = gate.generation;
    gate.cv.wait(lock, [&] { return gate.generation != entered; });
}

// ---------------------------------------------------------------------------
// Reading the statements

// The text of a literal opened by `quote` at s[pos], with backslash escapes
// undone; pos ends past the closing quote.
std::optional<std::string> read_quoted(std::string_view s, std::size_t& pos, char quote) {
    if (pos >= s.size() || s[pos] != quote) {
        return std::nullopt;
    }
    std::string out;
    for (std::size_t i = pos + 1; i < s.size(); ++i) {
        const char c = s[i];
        if (c == '\\' && i + 1 < s.size()) {
            out += s[++i];
        } else if (c == quote) {
            pos = i + 1;
            return out;
        } else {
            out += c;
        }
    }
    return std::nullopt;
}

bool is_bare_identifier_char(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
}

std::optional<std::string> read_identifier(std::string_view s, std::size_t& pos) {
    if (pos < s.size() && s[pos] == '`') {
        return read_quoted(s, pos, '`');
    }
    const std::size_t start = pos;
    while (pos < s.size() && is_bare_identifier_char(s[pos])) {
        ++pos;
    }
    if (pos == start) {
        return std::nullopt;
    }
    return std::string(s.substr(start, pos - start));
}

void skip_spaces(std::string_view s, std::size_t& pos) {
    while (pos < s.size() && s[pos] == ' ') {
        ++pos;
    }
}

std::optional<std::string> literal_after(std::string_view sql, std::string_view marker) {
    std::size_t pos = sql.find(marker);
    if (pos == std::string_view::npos) {
        return std::nullopt;
    }
    pos += marker.size();
    skip_spaces(sql, pos);
    return read_quoted(sql, pos, '\'');
}

// The quoted names of a "name IN ('a', 'b')" filter; nullopt when the
// statement has none.
std::optional<std::set<std::string>> name_filter(std::string_view sql) {
    constexpr std::string_view kMarker = "name IN (";
    std::size_t pos = sql.find(kMarker);
    if (pos == std::string_view::npos) {
        return std::nullopt;
    }
    pos += kMarker.size();
    std::set<std::string> names;
    for (;;) {
        skip_spaces(sql, pos);
        auto name = read_quoted(sql, pos, '\'');
        if (!name) {
            return std::nullopt;
        }
        names.insert(std::move(*name));
        skip_spaces(sql, pos);
        if (pos < sql.size() && sql[pos] == ',') {
            ++pos;
            continue;
        }
        if (pos < sql.size() && sql[pos] == ')') {
            return names;
        }
        return std::nullopt;
    }
}

std::string required_literal(std::string_view sql, std::string_view marker) {
    auto value = literal_after(sql, marker);
    if (!value) {
        throw_server(ch::SYNTAX_ERROR,
                     "fake server: no literal after \"" + std::string(marker) +
                         "\" in: " + std::string(sql));
    }
    return *value;
}

struct ParsedInsert {
    std::string database{"default"};
    std::string table;
    std::vector<std::string> columns;  // empty without a column list
    std::string token;
};

ParsedInsert parse_insert(std::string_view sql) {
    const auto fail = [&]() -> ParsedInsert {
        throw_server(ch::SYNTAX_ERROR,
                     "fake server: cannot read the INSERT target in: " + std::string(sql));
    };
    constexpr std::string_view kPrefix = "INSERT INTO ";
    std::size_t pos = 0;
    skip_spaces(sql, pos);
    if (sql.substr(pos, kPrefix.size()) != kPrefix) {
        return fail();
    }
    pos += kPrefix.size();
    ParsedInsert out;
    auto first = read_identifier(sql, pos);
    if (!first) {
        return fail();
    }
    if (pos < sql.size() && sql[pos] == '.') {
        ++pos;
        auto second = read_identifier(sql, pos);
        if (!second) {
            return fail();
        }
        out.database = std::move(*first);
        out.table = std::move(*second);
    } else {
        out.table = std::move(*first);
    }
    skip_spaces(sql, pos);
    if (pos < sql.size() && sql[pos] == '(') {
        ++pos;
        for (;;) {
            skip_spaces(sql, pos);
            auto column = read_identifier(sql, pos);
            if (!column) {
                return fail();
            }
            out.columns.push_back(std::move(*column));
            skip_spaces(sql, pos);
            if (pos < sql.size() && sql[pos] == ',') {
                ++pos;
                continue;
            }
            if (pos < sql.size() && sql[pos] == ')') {
                break;
            }
            return fail();
        }
    }
    if (auto token = literal_after(sql, "insert_deduplication_token=")) {
        out.token = std::move(*token);
    }
    return out;
}

// ---------------------------------------------------------------------------
// Rendering values

static_assert(std::endian::native == std::endian::little,
              "the fake reads 128-bit values as two little-endian halves");

template <typename T>
T read_item(const ch::ItemView& item) {
    T out{};
    if (item.data.size() != sizeof(T)) {
        throw std::logic_error("fake server: a " + std::to_string(item.data.size()) +
                               "-byte item read as " + std::to_string(sizeof(T)) + " bytes");
    }
    std::memcpy(&out, item.data.data(), sizeof(T));
    return out;
}

// (hi, lo) divided by 10 in place; returns the remainder.
std::uint64_t divide_by_ten(std::uint64_t& hi, std::uint64_t& lo) {
    std::uint64_t rem = hi % 10;
    hi /= 10;
    const std::uint64_t mid = (rem << 32) | (lo >> 32);
    const std::uint64_t q_mid = mid / 10;
    rem = mid % 10;
    const std::uint64_t low = (rem << 32) | (lo & 0xffffffffULL);
    const std::uint64_t q_low = low / 10;
    rem = low % 10;
    lo = (q_mid << 32) | q_low;
    return rem;
}

std::string unsigned_128_text(std::uint64_t hi, std::uint64_t lo) {
    if (hi == 0) {
        return std::to_string(lo);
    }
    std::string digits;
    while (hi != 0 || lo != 0) {
        digits += static_cast<char>('0' + divide_by_ten(hi, lo));
    }
    std::reverse(digits.begin(), digits.end());
    return digits;
}

// A two's-complement 128-bit value as its sign and magnitude digits.
std::pair<bool, std::string> signed_128_digits(std::uint64_t hi, std::uint64_t lo) {
    const bool negative = (hi >> 63) != 0;
    if (negative) {
        hi = ~hi;
        lo = ~lo + 1;
        if (lo == 0) {
            ++hi;
        }
    }
    return {negative, unsigned_128_text(hi, lo)};
}

std::pair<bool, std::string> signed_digits(const ch::ItemView& item) {
    switch (item.data.size()) {
        case 4: {
            const auto v = static_cast<std::int64_t>(read_item<std::int32_t>(item));
            return signed_128_digits(v < 0 ? ~0ULL : 0ULL, static_cast<std::uint64_t>(v));
        }
        case 8: {
            const auto v = read_item<std::int64_t>(item);
            return signed_128_digits(v < 0 ? ~0ULL : 0ULL, static_cast<std::uint64_t>(v));
        }
        case 16: {
            const auto halves = read_item<std::array<std::uint64_t, 2>>(item);
            return signed_128_digits(halves[1], halves[0]);
        }
        default:
            throw std::logic_error("fake server: an integer item of " +
                                   std::to_string(item.data.size()) + " bytes");
    }
}

std::string decimal_text(const ch::ItemView& item, std::size_t scale) {
    auto [negative, digits] = signed_digits(item);
    if (scale > 0) {
        if (digits.size() <= scale) {
            digits.insert(0, scale + 1 - digits.size(), '0');
        }
        digits.insert(digits.size() - scale, ".");
    }
    return negative ? "-" + digits : digits;
}

template <typename T>
std::string float_text(T value) {
    std::array<char, 64> buf{};
    const auto res = std::to_chars(buf.data(), buf.data() + buf.size(), value);
    return std::string(buf.data(), res.ptr);
}

std::int64_t floor_div(std::int64_t a, std::int64_t b) {
    std::int64_t q = a / b;
    if ((a % b != 0) && ((a < 0) != (b < 0))) {
        --q;
    }
    return q;
}

std::string date_text(std::int64_t days) {
    const std::chrono::year_month_day ymd{
        std::chrono::sys_days{std::chrono::days{static_cast<int>(days)}}};
    std::array<char, 32> buf{};
    std::snprintf(buf.data(),
                  buf.size(),
                  "%04d-%02u-%02u",
                  static_cast<int>(ymd.year()),
                  static_cast<unsigned>(ymd.month()),
                  static_cast<unsigned>(ymd.day()));
    return buf.data();
}

std::string date_time_text(std::int64_t seconds) {
    const std::int64_t days = floor_div(seconds, 86400);
    const std::int64_t of_day = seconds - days * 86400;
    std::array<char, 16> buf{};
    std::snprintf(buf.data(),
                  buf.size(),
                  " %02d:%02d:%02d",
                  static_cast<int>(of_day / 3600),
                  static_cast<int>(of_day / 60 % 60),
                  static_cast<int>(of_day % 60));
    return date_text(days) + buf.data();
}

std::string date_time64_text(std::int64_t ticks, std::size_t precision) {
    std::int64_t scale = 1;
    for (std::size_t i = 0; i < precision; ++i) {
        scale *= 10;
    }
    const std::int64_t seconds = floor_div(ticks, scale);
    std::string out = date_time_text(seconds);
    if (precision > 0) {
        std::string fraction = std::to_string(ticks - seconds * scale);
        fraction.insert(0, precision - fraction.size(), '0');
        out += "." + fraction;
    }
    return out;
}

std::string uuid_text(const ch::ItemView& item) {
    const auto halves = read_item<std::array<std::uint64_t, 2>>(item);
    std::array<char, 40> buf{};
    std::snprintf(buf.data(),
                  buf.size(),
                  "%08llx-%04llx-%04llx-%04llx-%012llx",
                  static_cast<unsigned long long>(halves[0] >> 32),
                  static_cast<unsigned long long>((halves[0] >> 16) & 0xffffULL),
                  static_cast<unsigned long long>(halves[0] & 0xffffULL),
                  static_cast<unsigned long long>(halves[1] >> 48),
                  static_cast<unsigned long long>(halves[1] & 0xffffffffffffULL));
    return buf.data();
}

std::string quoted(std::string_view text) {
    std::string out = "'";
    for (const char c : text) {
        if (c == '\'' || c == '\\') {
            out += '\\';
        }
        out += c;
    }
    out += '\'';
    return out;
}

std::string text_value(std::string_view text, bool nested) {
    return nested ? quoted(text) : std::string(text);
}

const ch::Type& unwrap_nullable(const ch::Type& type) {
    if (type.GetCode() == ch::Type::Nullable) {
        return *type.As<ch::NullableType>()->GetNestedType();
    }
    return type;
}

std::string enum_text(const ch::Type& type, std::int16_t value, bool nested) {
    const auto* enum_type = type.As<ch::EnumType>();
    if (!enum_type->HasEnumValue(value)) {
        return std::to_string(value);
    }
    return text_value(enum_type->GetEnumName(value), nested);
}

// A scalar the column hands out as an ItemView, typed by `type` (the column's
// own, or a LowCardinality column's dictionary type).
std::string item_text(const ch::ItemView& item, const ch::Type& type, bool nested) {
    switch (item.type) {
        case ch::Type::Void:
            return "NULL";
        case ch::Type::Int8:
            return std::to_string(read_item<std::int8_t>(item));
        case ch::Type::Int16:
            return std::to_string(read_item<std::int16_t>(item));
        case ch::Type::Int32:
            return std::to_string(read_item<std::int32_t>(item));
        case ch::Type::Int64:
            return std::to_string(read_item<std::int64_t>(item));
        case ch::Type::UInt8:
            return std::to_string(read_item<std::uint8_t>(item));
        case ch::Type::UInt16:
            return std::to_string(read_item<std::uint16_t>(item));
        case ch::Type::UInt32:
            return std::to_string(read_item<std::uint32_t>(item));
        case ch::Type::UInt64:
            return std::to_string(read_item<std::uint64_t>(item));
        case ch::Type::Int128: {
            auto [negative, digits] = signed_digits(item);
            return negative ? "-" + digits : digits;
        }
        case ch::Type::UInt128: {
            const auto halves = read_item<std::array<std::uint64_t, 2>>(item);
            return unsigned_128_text(halves[1], halves[0]);
        }
        case ch::Type::Float32:
            return float_text(read_item<float>(item));
        case ch::Type::Float64:
            return float_text(read_item<double>(item));
        case ch::Type::Bool:
            return read_item<std::uint8_t>(item) != 0 ? "true" : "false";
        case ch::Type::String:
        case ch::Type::FixedString:
            return text_value(item.data, nested);
        case ch::Type::Date:
            return text_value(date_text(read_item<std::uint16_t>(item)), nested);
        case ch::Type::Date32:
            return text_value(date_text(read_item<std::int32_t>(item)), nested);
        case ch::Type::DateTime:
            return text_value(date_time_text(read_item<std::uint32_t>(item)), nested);
        case ch::Type::DateTime64:
            return text_value(date_time64_text(read_item<std::int64_t>(item),
                                               type.As<ch::DateTime64Type>()->GetPrecision()),
                              nested);
        case ch::Type::Decimal:
        case ch::Type::Decimal32:
        case ch::Type::Decimal64:
        case ch::Type::Decimal128:
            return decimal_text(item, type.As<ch::DecimalType>()->GetScale());
        case ch::Type::Enum8:
            return enum_text(type, read_item<std::int8_t>(item), nested);
        case ch::Type::Enum16:
            return enum_text(type, read_item<std::int16_t>(item), nested);
        case ch::Type::UUID:
            return text_value(uuid_text(item), nested);
        default:
            throw std::logic_error("fake server: cannot render a " + type.GetName() + " value");
    }
}

std::string value_text(const ch::Column& column, std::size_t row, bool nested) {
    const ch::Type& type = column.GetType();
    switch (type.GetCode()) {
        case ch::Type::Nullable: {
            const auto nullable = column.As<ch::ColumnNullable>();
            return nullable->IsNull(row) ? "NULL" : value_text(*nullable->Nested(), row, nested);
        }
        case ch::Type::Array: {
            const auto array = column.As<ch::ColumnArray>();
            const auto data = array->GetData();
            const std::size_t offset = array->GetOffset(row);
            const std::size_t size = array->GetSize(row);
            std::string out = "[";
            for (std::size_t i = 0; i < size; ++i) {
                out += (i > 0 ? "," : "") + value_text(*data, offset + i, true);
            }
            return out + "]";
        }
        case ch::Type::Tuple: {
            const auto tuple = column.As<ch::ColumnTuple>();
            std::string out = "(";
            for (std::size_t i = 0; i < tuple->TupleSize(); ++i) {
                out += (i > 0 ? "," : "") + value_text(*tuple->At(i), row, true);
            }
            return out + ")";
        }
        case ch::Type::Map: {
            const auto pairs = column.As<ch::ColumnMap>()->GetAsColumn(row)->As<ch::ColumnTuple>();
            std::string out = "{";
            for (std::size_t i = 0; i < pairs->Size(); ++i) {
                out += (i > 0 ? "," : "") + value_text(*pairs->At(0), i, true) + ":" +
                       value_text(*pairs->At(1), i, true);
            }
            return out + "}";
        }
        case ch::Type::LowCardinality: {
            const auto& dictionary =
                unwrap_nullable(*type.As<ch::LowCardinalityType>()->GetNestedType());
            return item_text(column.GetItem(row), dictionary, nested);
        }
        case ch::Type::IPv4:
            return text_value(column.As<ch::ColumnIPv4>()->AsString(row), nested);
        case ch::Type::IPv6:
            return text_value(column.As<ch::ColumnIPv6>()->AsString(row), nested);
        default:
            return item_text(column.GetItem(row), type, nested);
    }
}

// The integer a partition is computed from: the first column's value, NULL
// as 0. nullopt for a column that holds no integers.
std::optional<std::int64_t> partition_input(const ch::Column& column, std::size_t row) {
    const ch::Column* c = &column;
    if (c->GetType().GetCode() == ch::Type::Nullable) {
        const auto nullable = c->As<ch::ColumnNullable>();
        if (nullable->IsNull(row)) {
            return 0;
        }
        c = nullable->Nested().get();
    }
    switch (c->GetType().GetCode()) {
        case ch::Type::Int8:
            return read_item<std::int8_t>(c->GetItem(row));
        case ch::Type::Int16:
            return read_item<std::int16_t>(c->GetItem(row));
        case ch::Type::Int32:
            return read_item<std::int32_t>(c->GetItem(row));
        case ch::Type::Int64:
            return read_item<std::int64_t>(c->GetItem(row));
        case ch::Type::UInt8:
            return read_item<std::uint8_t>(c->GetItem(row));
        case ch::Type::UInt16:
            return read_item<std::uint16_t>(c->GetItem(row));
        case ch::Type::UInt32:
            return read_item<std::uint32_t>(c->GetItem(row));
        case ch::Type::UInt64:
            return std::bit_cast<std::int64_t>(read_item<std::uint64_t>(c->GetItem(row)));
        default:
            return std::nullopt;
    }
}

class StringOutput final : public ch::OutputStream {
public:
    explicit StringOutput(std::string& out) : out_(out) {}

protected:
    std::size_t DoWrite(const void* data, std::size_t len) override {
        out_.append(static_cast<const char*>(data), len);
        return len;
    }

private:
    std::string& out_;
};

// ---------------------------------------------------------------------------
// The server's state

std::string default_kind_text(DefaultKind kind) {
    switch (kind) {
        case DefaultKind::None:
            return "";
        case DefaultKind::Default:
            return "DEFAULT";
        case DefaultKind::Materialized:
            return "MATERIALIZED";
        case DefaultKind::Alias:
            return "ALIAS";
        case DefaultKind::Ephemeral:
            return "EPHEMERAL";
    }
    return "";
}

using Settings = std::vector<std::pair<std::string, std::string>>;

// What a 26.8 server lists for the settings the sink reads, at that line's
// defaults.
Settings default_settings() {
    return {
        {"async_insert", "1"},
        {"wait_for_async_insert", "1"},
        {"insert_deduplication_token", ""},
        {"input_format_native_allow_types_conversion", "1"},
        {"input_format_null_as_default", "1"},
        {"throw_on_max_partitions_per_insert_block", "1"},
        {"log_comment", ""},
        {"max_execution_time", "0"},
        {"timeout_overflow_mode", "throw"},
        {"distributed_foreground_insert", "0"},
        {"min_insert_block_size_rows", "1048449"},
        {"min_insert_block_size_bytes", "268402944"},
        {"deduplicate_insert", "enable"},
        {"use_strict_insert_block_limits", "0"},
        {"insert_deduplicate", "1"},
        {"max_partitions_per_insert_block", "100"},
        {"insert_quorum", "0"},
    };
}

Settings default_merge_tree_settings() {
    return {
        {"async_insert", "0"},
        {"non_replicated_deduplication_window", "0"},
        {"replicated_deduplication_window", "10000"},
    };
}

void upsert(Settings& settings, const std::string& name, const std::string& value) {
    for (auto& [n, v] : settings) {
        if (n == name) {
            v = value;
            return;
        }
    }
    settings.emplace_back(name, value);
}

void erase(Settings& settings, const std::string& name) {
    std::erase_if(settings, [&](const auto& nv) { return nv.first == name; });
}

Settings overlay(Settings base, const Settings& overrides) {
    for (const auto& [n, v] : overrides) {
        upsert(base, n, v);
    }
    return base;
}

std::vector<std::vector<std::string>> filtered(const Settings& settings,
                                               const std::optional<std::set<std::string>>& names) {
    std::vector<std::vector<std::string>> rows;
    for (const auto& [n, v] : settings) {
        if (!names || names->contains(n)) {
            rows.push_back({n, v});
        }
    }
    return rows;
}

struct TableState {
    FakeTable def;
    std::vector<LandedBlock> landed;
    std::deque<std::string> log;  // oldest first
    std::set<std::string> logged;
    std::vector<ReceivedInsert> inserts;

    [[nodiscard]] std::string key() const { return def.database + "." + def.name; }

    // Lands one part unless the deduplication log already holds its key.
    void land(LandedBlock block) {
        const bool keyed = !block.token.empty() && def.dedup_window > 0;
        if (keyed) {
            if (logged.contains(block.token)) {
                return;
            }
            log.push_back(block.token);
            logged.insert(block.token);
            while (log.size() > def.dedup_window) {
                logged.erase(log.front());
                log.pop_front();
            }
        }
        landed.push_back(std::move(block));
    }
};

struct ClusterState {
    std::vector<FakeServer*> replicas;
    std::set<std::size_t> unreadable;
};

std::atomic<std::uint64_t> g_next_server{1};

}  // namespace

struct FakeServer::Impl {
    mutable std::mutex mu;
    const std::string display_name{"fake-" + std::to_string(g_next_server.fetch_add(1))};
    std::uint64_t major{26}, minor{8}, patch{1};
    Settings settings{default_settings()};
    Settings merge_tree{default_merge_tree_settings()};
    Settings replicated_overrides;
    bool has_replicated_table{true};
    std::deque<TableState> tables;
    std::map<std::string, ClusterState> clusters;
    std::deque<Fault> faults;
    std::array<std::size_t, kSteps> fault_counts{};
    std::vector<std::string> statements;
    std::size_t abandoned{0};
    std::size_t destroyed{0};
    std::size_t connects{0};
    std::uint64_t foreign_seq{0};
    bool down{false};
    // Raised by each set_down that breaks live connections; a connection made
    // at an earlier value is broken.
    std::atomic<std::uint64_t> break_epoch{0};

    // Under mu.
    TableState* find(const std::string& database, const std::string& name) {
        for (auto& t : tables) {
            if (t.def.database == database && t.def.name == name) {
                return &t;
            }
        }
        return nullptr;
    }
    TableState* find(const std::string& key) {
        for (auto& t : tables) {
            if (t.key() == key) {
                return &t;
            }
        }
        for (auto& t : tables) {
            if (t.def.name == key) {
                return &t;
            }
        }
        return nullptr;
    }
    TableState& require(const std::string& key) {
        TableState* t = find(key);
        if (t == nullptr) {
            throw std::invalid_argument("fake server " + display_name + " has no table " + key);
        }
        return *t;
    }
    const TableState& require(const std::string& key) const {
        return const_cast<Impl*>(this)->require(key);
    }

    std::optional<Fault> take(Step step) {
        const std::lock_guard<std::mutex> lock(mu);
        return take_fault(faults, fault_counts, step);
    }

    void record_statement(const std::string& sql) {
        const std::lock_guard<std::mutex> lock(mu);
        statements.push_back(sql);
    }

    ServerIdentity identity(const Endpoint& endpoint) {
        const std::lock_guard<std::mutex> lock(mu);
        ServerIdentity id;
        id.display_name = display_name;
        id.major = major;
        id.minor = minor;
        id.patch = patch;
        id.endpoint = endpoint;
        return id;
    }

    // What a clusterAllReplicas read sees: each replica's display name and a
    // copy of what `read` takes from it, under that replica's own lock only.
    template <typename Read>
    auto across_cluster(const std::string& cluster, Read read) {
        std::vector<FakeServer*> replicas;
        {
            const std::lock_guard<std::mutex> lock(mu);
            const auto it = clusters.find(cluster);
            if (it == clusters.end()) {
                throw_server(ch::CLUSTER_DOESNT_EXIST,
                             "Requested cluster '" + cluster + "' not found");
            }
            if (!it->second.unreadable.empty()) {
                const std::size_t index = *it->second.unreadable.begin();
                const std::string host = index < it->second.replicas.size()
                                             ? it->second.replicas[index]->display_name()
                                             : "replica " + std::to_string(index);
                throw_server(ch::ALL_CONNECTION_TRIES_FAILED,
                             "All connection tries failed while connecting to " + host +
                                 " of cluster '" + cluster + "'");
            }
            replicas = it->second.replicas;
        }
        using Result = decltype(read(std::declval<Impl&>()));
        std::vector<std::pair<std::string, Result>> out;
        for (FakeServer* replica : replicas) {
            Impl& r = replica->impl();
            const std::lock_guard<std::mutex> lock(r.mu);
            out.emplace_back(r.display_name, read(r));
        }
        return out;
    }

    // Under mu: what system.replicated_merge_tree_settings shows.
    [[nodiscard]] Settings replicated_view() const {
        return overlay(merge_tree, replicated_overrides);
    }

    ResultSet answer(MetaQuery kind, const std::string& sql);
};

ResultSet FakeServer::Impl::answer(MetaQuery kind, const std::string& sql) {
    ResultSet out;
    const auto names = name_filter(sql);
    switch (kind) {
        case MetaQuery::ServerSettings: {
            const std::lock_guard<std::mutex> lock(mu);
            out.columns = {"name", "value"};
            out.rows = filtered(settings, names);
            break;
        }
        case MetaQuery::MergeTreeSettings: {
            const std::lock_guard<std::mutex> lock(mu);
            out.columns = {"name", "value"};
            out.rows = filtered(merge_tree, names);
            break;
        }
        case MetaQuery::ReplicatedMergeTreeSettings: {
            const std::lock_guard<std::mutex> lock(mu);
            if (!has_replicated_table) {
                throw_server(ch::UNKNOWN_TABLE,
                             "Table system.replicated_merge_tree_settings does not exist");
            }
            out.columns = {"name", "value"};
            out.rows = filtered(replicated_view(), names);
            break;
        }
        case MetaQuery::Table: {
            const std::string db = required_literal(sql, "database = ");
            const std::string name = required_literal(sql, "AND name = ");
            const std::lock_guard<std::mutex> lock(mu);
            out.columns = {"engine", "engine_full"};
            if (const TableState* t = find(db, name)) {
                out.rows.push_back({t->def.engine, t->def.engine_full});
            }
            break;
        }
        case MetaQuery::Columns: {
            const std::string db = required_literal(sql, "database = ");
            const std::string name = required_literal(sql, "AND table = ");
            const std::lock_guard<std::mutex> lock(mu);
            out.columns = {"name", "type", "default_kind", "position"};
            if (const TableState* t = find(db, name)) {
                std::vector<std::pair<std::uint32_t, const TargetColumn*>> ordered;
                for (std::size_t i = 0; i < t->def.columns.size(); ++i) {
                    const TargetColumn& c = t->def.columns[i];
                    ordered.emplace_back(
                        c.position != 0 ? c.position : static_cast<std::uint32_t>(i + 1), &c);
                }
                std::stable_sort(ordered.begin(), ordered.end(), [](const auto& a, const auto& b) {
                    return a.first < b.first;
                });
                for (const auto& [position, c] : ordered) {
                    out.rows.push_back({c->name,
                                        c->type,
                                        default_kind_text(c->default_kind),
                                        std::to_string(position)});
                }
            }
            break;
        }
        case MetaQuery::ClusterReplicaCount: {
            const std::string cluster = required_literal(sql, "cluster = ");
            const std::lock_guard<std::mutex> lock(mu);
            const auto it = clusters.find(cluster);
            out.columns = {"count"};
            out.rows.push_back(
                {std::to_string(it == clusters.end() ? 0 : it->second.replicas.size())});
            break;
        }
        case MetaQuery::ClusterTables: {
            const std::string cluster = required_literal(sql, "clusterAllReplicas(");
            const std::string db = required_literal(sql, "database = ");
            const std::string name = required_literal(sql, "AND name = ");
            out.columns = {"host", "engine", "engine_full"};
            const auto per_replica = across_cluster(cluster, [&](Impl& r) {
                std::optional<std::pair<std::string, std::string>> row;
                if (const TableState* t = r.find(db, name)) {
                    row.emplace(t->def.engine, t->def.engine_full);
                }
                return row;
            });
            for (const auto& [host, row] : per_replica) {
                if (row) {
                    out.rows.push_back({host, row->first, row->second});
                }
            }
            break;
        }
        case MetaQuery::ClusterMergeTreeSettings:
        case MetaQuery::ClusterReplicatedMergeTreeSettings: {
            const bool replicated = kind == MetaQuery::ClusterReplicatedMergeTreeSettings;
            const std::string cluster = required_literal(sql, "clusterAllReplicas(");
            out.columns = {"host", "name", "value"};
            const auto per_replica = across_cluster(cluster, [&](Impl& r) {
                if (replicated && !r.has_replicated_table) {
                    throw_server(ch::UNKNOWN_TABLE,
                                 "Table system.replicated_merge_tree_settings does not exist on " +
                                     r.display_name);
                }
                return filtered(replicated ? r.replicated_view() : r.merge_tree, names);
            });
            for (const auto& [host, rows] : per_replica) {
                for (const auto& row : rows) {
                    out.rows.push_back({host, row[0], row[1]});
                }
            }
            break;
        }
    }
    return out;
}

FakeServer::FakeServer() : impl_(std::make_unique<Impl>()) {}
FakeServer::~FakeServer() = default;

void FakeServer::set_version(std::uint64_t major, std::uint64_t minor, std::uint64_t patch) {
    const std::lock_guard<std::mutex> lock(impl_->mu);
    impl_->major = major;
    impl_->minor = minor;
    impl_->patch = patch;
}

void FakeServer::set_settings(std::vector<std::pair<std::string, std::string>> name_values) {
    const std::lock_guard<std::mutex> lock(impl_->mu);
    impl_->settings = std::move(name_values);
}

void FakeServer::set_setting(const std::string& name, const std::string& value) {
    const std::lock_guard<std::mutex> lock(impl_->mu);
    upsert(impl_->settings, name, value);
}

void FakeServer::remove_setting(const std::string& name) {
    const std::lock_guard<std::mutex> lock(impl_->mu);
    erase(impl_->settings, name);
}

void FakeServer::set_merge_tree_setting(const std::string& name, const std::string& value) {
    const std::lock_guard<std::mutex> lock(impl_->mu);
    upsert(impl_->merge_tree, name, value);
}

void FakeServer::remove_merge_tree_setting(const std::string& name) {
    const std::lock_guard<std::mutex> lock(impl_->mu);
    erase(impl_->merge_tree, name);
}

void FakeServer::set_replicated_merge_tree_setting(const std::string& name,
                                                   const std::string& value) {
    const std::lock_guard<std::mutex> lock(impl_->mu);
    upsert(impl_->replicated_overrides, name, value);
}

void FakeServer::set_replicated_merge_tree_settings_table(bool present) {
    const std::lock_guard<std::mutex> lock(impl_->mu);
    impl_->has_replicated_table = present;
}

void FakeServer::add_table(FakeTable table) {
    const std::lock_guard<std::mutex> lock(impl_->mu);
    if (TableState* existing = impl_->find(table.database, table.name)) {
        existing->def = std::move(table);
        return;
    }
    impl_->tables.push_back(TableState{std::move(table), {}, {}, {}, {}});
}

void FakeServer::add_cluster(const std::string& name, std::vector<FakeServer*> replicas) {
    const std::lock_guard<std::mutex> lock(impl_->mu);
    impl_->clusters[name].replicas = std::move(replicas);
}

void FakeServer::set_unreadable_replica(const std::string& cluster,
                                        std::size_t replica,
                                        bool unreadable) {
    const std::lock_guard<std::mutex> lock(impl_->mu);
    auto& state = impl_->clusters[cluster];
    if (unreadable) {
        state.unreadable.insert(replica);
    } else {
        state.unreadable.erase(replica);
    }
}

void FakeServer::alter_column_type(const std::string& table,
                                   const std::string& column,
                                   const std::string& type) {
    const std::lock_guard<std::mutex> lock(impl_->mu);
    TableState& t = impl_->require(table);
    for (auto& c : t.def.columns) {
        if (c.name == column) {
            c.type = type;
            return;
        }
    }
    throw std::invalid_argument("fake server: table " + t.key() + " has no column " + column);
}

void FakeServer::inject(Fault fault) {
    validate_fault(fault);
    const std::lock_guard<std::mutex> lock(impl_->mu);
    impl_->faults.push_back(std::move(fault));
}

void FakeServer::set_down(bool down, bool break_live_connections) {
    const std::lock_guard<std::mutex> lock(impl_->mu);
    impl_->down = down;
    if (down && break_live_connections) {
        impl_->break_epoch.fetch_add(1);
    }
}

void FakeServer::land_foreign(const std::string& table, std::size_t n) {
    const std::lock_guard<std::mutex> lock(impl_->mu);
    TableState& t = impl_->require(table);
    for (std::size_t i = 0; i < n; ++i) {
        LandedBlock block;
        block.token = "foreign-" + std::to_string(++impl_->foreign_seq) + "_0";
        t.land(std::move(block));
    }
}

void FakeServer::release() {
    HoldGate& gate = hold_gate();
    {
        const std::lock_guard<std::mutex> lock(gate.mu);
        ++gate.generation;
    }
    gate.cv.notify_all();
}

const std::string& FakeServer::display_name() const noexcept {
    return impl_->display_name;
}

std::vector<LandedBlock> FakeServer::landed(const std::string& table) const {
    const std::lock_guard<std::mutex> lock(impl_->mu);
    return impl_->require(table).landed;
}

std::uint64_t FakeServer::rows(const std::string& table) const {
    const std::lock_guard<std::mutex> lock(impl_->mu);
    std::uint64_t total = 0;
    for (const auto& block : impl_->require(table).landed) {
        total += block.rows;
    }
    return total;
}

std::vector<ReceivedInsert> FakeServer::inserts(const std::string& table) const {
    const std::lock_guard<std::mutex> lock(impl_->mu);
    return impl_->require(table).inserts;
}

std::vector<std::string> FakeServer::statements() const {
    const std::lock_guard<std::mutex> lock(impl_->mu);
    return impl_->statements;
}

std::size_t FakeServer::abandoned_mid_insert() const {
    const std::lock_guard<std::mutex> lock(impl_->mu);
    return impl_->abandoned;
}

std::size_t FakeServer::destroyed_mid_insert() const {
    const std::lock_guard<std::mutex> lock(impl_->mu);
    return impl_->destroyed;
}

std::size_t FakeServer::connects() const {
    const std::lock_guard<std::mutex> lock(impl_->mu);
    return impl_->connects;
}

// ---------------------------------------------------------------------------
// The transport

namespace {

struct Connection {
    std::shared_ptr<FakeServer> server;
    std::uint64_t epoch{0};
    bool dead{false};

    [[nodiscard]] bool broken() const { return server->impl().break_epoch.load() > epoch; }
};

// One received block as the transport keeps it for the commit.
struct HeldBlock {
    std::vector<std::vector<std::string>> values;
    // The first column's values, for partition_of; empty when it holds no
    // integers.
    std::vector<std::optional<std::int64_t>> partition_inputs;
};

struct OpenInsert {
    std::shared_ptr<FakeServer> server;
    std::string database, table;
    std::string token;
    std::vector<HeaderColumn> header;
    std::size_t record{0};  // index into TableState::inserts
    std::vector<HeldBlock> blocks;
    // False once the server-side INSERT has failed: nothing more of it can land.
    bool pending{true};
    std::size_t parts_landed{0};
    std::string deferred_error;  // a block that did not match the header
};

struct Part {
    std::vector<std::vector<std::string>> rows;
};

}  // namespace

struct FakeTransport::Impl {
    std::function<std::shared_ptr<FakeServer>(const Endpoint&)> route;

    mutable std::mutex mu;
    std::condition_variable cv;
    bool interrupted{false};
    std::uint64_t abandons{0};
    Deadline deadline;
    std::optional<Connection> conn;
    std::optional<OpenInsert> insert;
    ServerIdentity identity;  // owning thread only

    std::atomic<std::uint64_t> written{0};
    std::atomic<std::uint64_t> read{0};
    std::atomic<std::uint64_t> connects{0};

    // Under mu, at the start of every call that needs a connection.
    void require_connected() const {
        if (!conn) {
            throw_system(ENOTCONN, "fake transport: not connected");
        }
    }

    // Under mu: what a socket call would find before it reached the server.
    void require_live() const {
        if (interrupted) {
            throw_system(ECONNABORTED, "fake transport: interrupted");
        }
        if (conn && (conn->dead || conn->broken())) {
            throw_system(ECONNRESET,
                         "fake server " + conn->server->display_name() + ": connection reset");
        }
        if (deadline && Clock::now() >= *deadline) {
            throw_system(ETIMEDOUT, "fake transport: past the attempt deadline");
        }
    }

    // Under mu: what may have happened while a call waited.
    void check_wait(std::uint64_t abandons_at_start, bool needs_connection) const {
        if (interrupted) {
            throw_system(ECONNABORTED, "fake transport: interrupted");
        }
        if (abandons != abandons_at_start) {
            throw_system(ECONNABORTED, "fake transport: abandoned during the call");
        }
        if (needs_connection && conn && conn->broken()) {
            throw_system(ECONNRESET,
                         "fake server " + conn->server->display_name() + ": connection reset");
        }
    }

    // Waits until `until`, or for ever without one, and returns then. Throws
    // ETIMEDOUT at the deadline when that comes first, and whatever
    // check_wait finds as soon as it happens.
    void wait(std::unique_lock<std::mutex>& lock,
              Deadline until,
              std::uint64_t abandons_at_start,
              bool needs_connection) {
        const Deadline limit = deadline;
        const bool times_out = limit && (!until || *limit < *until);
        const Deadline end = times_out ? limit : until;
        for (;;) {
            check_wait(abandons_at_start, needs_connection);
            const auto now = Clock::now();
            if (end && now >= *end) {
                if (times_out) {
                    throw_system(ETIMEDOUT, "fake transport: past the attempt deadline");
                }
                return;
            }
            auto next = now + kPollSlice;
            if (end && *end < next) {
                next = *end;
            }
            cv.wait_until(lock, next);
        }
    }

    // Plays a fault that fired: the delay, the landing, on_fire, the outcome.
    // Returns only when the outcome is success.
    void play(std::unique_lock<std::mutex>& lock,
              const Fault& fault,
              std::uint64_t abandons_at_start,
              bool needs_connection,
              const std::function<void(const Fault&)>& land) {
        if (fault.delay.count() > 0) {
            if (fault.kind == Fault::Kind::Uninterruptible) {
                lock.unlock();
                std::this_thread::sleep_for(fault.delay);
                lock.lock();
            } else {
                wait(lock, Clock::now() + fault.delay, abandons_at_start, needs_connection);
            }
        }
        if (land) {
            land(fault);
        }
        if (fault.on_fire) {
            lock.unlock();
            fault.on_fire();
            lock.lock();
        }
        if (is_failure(fault.kind)) {
            raise_failure(fault);
        }
        switch (fault.kind) {
            case Fault::Kind::Hang:
                wait(lock, std::nullopt, abandons_at_start, needs_connection);
                break;
            case Fault::Kind::Uninterruptible:
                lock.unlock();
                hold_until_release();
                lock.lock();
                break;
            default:
                break;
        }
        check_wait(abandons_at_start, needs_connection);
    }

    // Under mu: the open INSERT can land nothing more, and a std::system_error
    // also leaves its connection dead.
    void fail_insert(bool connection_lost) {
        if (connection_lost && conn) {
            conn->dead = true;
        }
        if (!insert) {
            return;
        }
        insert->pending = false;
        FakeServer::Impl& s = insert->server->impl();
        const std::lock_guard<std::mutex> lock(s.mu);
        if (TableState* t = s.find(insert->database, insert->table)) {
            auto& record = t->inserts.at(insert->record);
            if (record.outcome == ReceivedInsert::Outcome::Open) {
                record.outcome = ReceivedInsert::Outcome::Failed;
            }
        }
    }

    // The INSERT's rows, squashed and split by partition in the order the
    // rows first reach each one.
    [[nodiscard]] std::vector<Part> parts() const {
        const OpenInsert& in = *insert;
        FakeServer::Impl& s = in.server->impl();
        std::function<std::int64_t(std::int64_t)> partition_of;
        {
            const std::lock_guard<std::mutex> lock(s.mu);
            if (TableState* t = s.find(in.database, in.table)) {
                partition_of = t->def.partition_of;
            }
        }
        std::vector<Part> out;
        std::map<std::int64_t, std::size_t> index_of;
        for (const HeldBlock& block : in.blocks) {
            for (std::size_t r = 0; r < block.values.size(); ++r) {
                std::size_t index = 0;
                if (partition_of) {
                    if (block.partition_inputs.empty() || !block.partition_inputs[r]) {
                        throw std::logic_error(
                            "fake server: partition_of needs an integer first "
                            "column in the INSERT into " +
                            in.database + "." + in.table);
                    }
                    const std::int64_t partition = partition_of(*block.partition_inputs[r]);
                    const auto [it, added] = index_of.emplace(partition, out.size());
                    if (added) {
                        out.emplace_back();
                    }
                    index = it->second;
                } else if (out.empty()) {
                    out.emplace_back();
                }
                out[index].rows.push_back(block.values[r]);
            }
        }
        return out;
    }

    // Under mu: lands parts [parts_landed, upto) of the open INSERT.
    void land_parts(const std::vector<Part>& all, std::size_t upto) {
        OpenInsert& in = *insert;
        upto = std::min(upto, all.size());
        FakeServer::Impl& s = in.server->impl();
        const std::lock_guard<std::mutex> lock(s.mu);
        TableState* t = s.find(in.database, in.table);
        if (t == nullptr) {
            return;
        }
        for (std::size_t i = in.parts_landed; i < upto; ++i) {
            LandedBlock block;
            if (!in.token.empty()) {
                block.token = in.token + "_" + std::to_string(i);
            }
            block.rows = all[i].rows.size();
            block.values = all[i].rows;
            t->land(std::move(block));
        }
        in.parts_landed = std::max(in.parts_landed, upto);
    }

    // Under mu: marks the open INSERT's record and forgets it.
    void close_insert(ReceivedInsert::Outcome outcome) {
        FakeServer::Impl& s = insert->server->impl();
        {
            const std::lock_guard<std::mutex> lock(s.mu);
            if (TableState* t = s.find(insert->database, insert->table)) {
                auto& record = t->inserts.at(insert->record);
                if (record.outcome == ReceivedInsert::Outcome::Open ||
                    outcome == ReceivedInsert::Outcome::Committed) {
                    record.outcome = outcome;
                }
            }
        }
        insert.reset();
    }
};

namespace {

std::vector<HeaderColumn> insert_header(FakeServer::Impl& s, const ParsedInsert& in) {
    const std::lock_guard<std::mutex> lock(s.mu);
    const TableState* t = s.find(in.database, in.table);
    if (t == nullptr) {
        throw_server(ch::UNKNOWN_TABLE,
                     "Table " + in.database + "." + in.table + " does not exist");
    }
    std::vector<const TargetColumn*> listed;
    if (in.columns.empty()) {
        for (const auto& c : t->def.columns) {
            if (c.default_kind == DefaultKind::None || c.default_kind == DefaultKind::Default) {
                listed.push_back(&c);
            }
        }
    } else {
        for (const auto& name : in.columns) {
            const auto it = std::find_if(t->def.columns.begin(),
                                         t->def.columns.end(),
                                         [&](const TargetColumn& c) { return c.name == name; });
            if (it == t->def.columns.end()) {
                throw_server(ch::NO_SUCH_COLUMN_IN_TABLE,
                             "No such column " + name + " in table " + t->key());
            }
            if (it->default_kind == DefaultKind::Materialized ||
                it->default_kind == DefaultKind::Alias) {
                throw_server(ch::ILLEGAL_COLUMN,
                             "Cannot insert column " + name + ", because it is " +
                                 default_kind_text(it->default_kind) + " column");
            }
            listed.push_back(&*it);
        }
    }
    std::vector<HeaderColumn> header;
    for (const TargetColumn* c : listed) {
        std::string spelling = client_header_spelling(c->type);
        if (spelling.empty()) {
            // What the client throws when it cannot build a header column.
            throw ch::UnimplementedError("unsupported column type: " + c->type);
        }
        header.push_back(HeaderColumn{c->name, std::move(spelling)});
    }
    return header;
}

// Serialises and renders one block, and finds where its structure departs
// from the header, if it does.
struct Captured {
    ReceivedBlock received;
    HeldBlock held;
    std::string mismatch;
};

Captured capture(const ch::Block& block, const std::vector<HeaderColumn>& header) {
    Captured out;
    const std::size_t rows = block.GetRowCount();
    out.received.rows = rows;
    StringOutput bytes(out.received.bytes);
    bool consistent = true;
    for (std::size_t c = 0; c < block.GetColumnCount(); ++c) {
        const ch::ColumnRef column = block[c];
        const std::string& name = block.GetColumnName(c);
        const std::string type = column->Type()->GetName();
        out.received.bytes += name;
        out.received.bytes += '\0';
        out.received.bytes += type;
        out.received.bytes += '\0';
        column->Save(&bytes);
        if (column->Size() != rows) {
            consistent = false;
            if (out.mismatch.empty()) {
                out.mismatch = "column `" + name + "` has " + std::to_string(column->Size()) +
                               " rows and the block says " + std::to_string(rows);
            }
        }
    }
    if (out.mismatch.empty()) {
        if (block.GetColumnCount() != header.size()) {
            out.mismatch = "the block has " + std::to_string(block.GetColumnCount()) +
                           " columns and the INSERT " + std::to_string(header.size());
        } else {
            for (std::size_t c = 0; c < header.size(); ++c) {
                const std::string type = block[c]->Type()->GetName();
                if (block.GetColumnName(c) != header[c].name || type != header[c].type) {
                    out.mismatch = "column " + std::to_string(c + 1) + " is `" +
                                   block.GetColumnName(c) + "` " + type + " and the INSERT has `" +
                                   header[c].name + "` " + header[c].type;
                    break;
                }
            }
        }
    }
    if (!consistent) {
        return out;
    }
    out.received.values.resize(rows);
    for (std::size_t r = 0; r < rows; ++r) {
        auto& row = out.received.values[r];
        row.reserve(block.GetColumnCount());
        for (std::size_t c = 0; c < block.GetColumnCount(); ++c) {
            row.push_back(value_text(*block[c], r, false));
        }
    }
    out.held.values = out.received.values;
    if (block.GetColumnCount() > 0) {
        const ch::ColumnRef first = block[0];
        std::vector<std::optional<std::int64_t>> inputs;
        inputs.reserve(rows);
        bool integers = true;
        for (std::size_t r = 0; r < rows && integers; ++r) {
            inputs.push_back(partition_input(*first, r));
            integers = inputs.back().has_value();
        }
        if (integers) {
            out.held.partition_inputs = std::move(inputs);
        }
    }
    return out;
}

std::uint64_t result_bytes(const ResultSet& result) {
    std::uint64_t n = 0;
    for (const auto& row : result.rows) {
        for (const auto& cell : row) {
            n += cell.size();
        }
    }
    return n;
}

}  // namespace

FakeTransport::FakeTransport(std::shared_ptr<FakeServer> server) : impl_(std::make_unique<Impl>()) {
    impl_->route = [server = std::move(server)](const Endpoint&) { return server; };
}

FakeTransport::FakeTransport(std::map<Endpoint, std::shared_ptr<FakeServer>> servers)
    : impl_(std::make_unique<Impl>()) {
    impl_->route = [servers = std::move(servers)](const Endpoint& ep) {
        const auto it = servers.find(ep);
        return it == servers.end() ? nullptr : it->second;
    };
}

FakeTransport::~FakeTransport() {
    // The real client's destructor ends an open INSERT with the end-of-data
    // marker, which commits it, unless the socket can no longer carry it.
    try {
        std::unique_lock<std::mutex> lock(impl_->mu);
        if (!impl_->insert) {
            return;
        }
        FakeServer::Impl& s = impl_->insert->server->impl();
        {
            const std::lock_guard<std::mutex> server_lock(s.mu);
            ++s.destroyed;
        }
        const bool live =
            impl_->conn && !impl_->conn->dead && !impl_->conn->broken() && !impl_->interrupted;
        if (live && impl_->insert->pending && impl_->insert->deferred_error.empty()) {
            const auto all = impl_->parts();
            impl_->land_parts(all, all.size());
            impl_->close_insert(ReceivedInsert::Outcome::Committed);
        } else {
            impl_->close_insert(ReceivedInsert::Outcome::Failed);
        }
    } catch (...) {
        // A destructor reports nothing; the counter above already shows it.
    }
}

void FakeTransport::connect(const Endpoint& endpoint) {
    std::unique_lock<std::mutex> lock(impl_->mu);
    if (impl_->interrupted) {
        throw_system(ECONNABORTED, "fake transport: interrupted");
    }
    if (impl_->conn) {
        return;
    }
    std::shared_ptr<FakeServer> server = impl_->route(endpoint);
    if (!server) {
        throw_system(ECONNREFUSED, "no fake server at " + endpoint_text(endpoint));
    }
    FakeServer::Impl& s = server->impl();
    {
        const std::lock_guard<std::mutex> server_lock(s.mu);
        ++s.connects;
    }
    const std::uint64_t abandons_at_start = impl_->abandons;
    if (impl_->deadline && Clock::now() >= *impl_->deadline) {
        throw_system(ETIMEDOUT, "fake transport: past the attempt deadline");
    }
    if (auto fault = s.take(Step::Connect)) {
        impl_->play(lock, *fault, abandons_at_start, false, nullptr);
    }
    bool down = false;
    {
        const std::lock_guard<std::mutex> server_lock(s.mu);
        down = s.down;
    }
    if (down) {
        throw_system(
            ECONNREFUSED,
            "fake server " + s.display_name + " at " + endpoint_text(endpoint) + " is down");
    }
    impl_->identity = s.identity(endpoint);
    impl_->conn = Connection{server, s.break_epoch.load(), false};
    impl_->connects.fetch_add(1);
}

bool FakeTransport::connected() const noexcept {
    const std::lock_guard<std::mutex> lock(impl_->mu);
    return impl_->conn.has_value();
}

const ServerIdentity& FakeTransport::server() const {
    return impl_->identity;
}

ResultSet FakeTransport::select(MetaQuery kind, const std::string& sql) {
    std::unique_lock<std::mutex> lock(impl_->mu);
    impl_->require_connected();
    if (impl_->insert) {
        throw ch::ValidationError("cannot execute query while executing another operation");
    }
    try {
        impl_->require_live();
        const std::shared_ptr<FakeServer> server = impl_->conn->server;
        FakeServer::Impl& s = server->impl();
        s.record_statement(sql);
        impl_->written.fetch_add(sql.size());
        const std::uint64_t abandons_at_start = impl_->abandons;
        if (auto fault = s.take(Step::Select)) {
            impl_->play(lock, *fault, abandons_at_start, true, nullptr);
        }
        ResultSet out = s.answer(kind, sql);
        impl_->read.fetch_add(result_bytes(out));
        return out;
    } catch (const std::system_error&) {
        impl_->fail_insert(true);
        throw;
    }
}

std::vector<HeaderColumn> FakeTransport::begin_insert(const std::string& sql) {
    std::unique_lock<std::mutex> lock(impl_->mu);
    impl_->require_connected();
    if (impl_->insert) {
        throw ch::ValidationError("cannot execute query while executing another operation");
    }
    try {
        impl_->require_live();
        const std::shared_ptr<FakeServer> server = impl_->conn->server;
        FakeServer::Impl& s = server->impl();
        s.record_statement(sql);
        impl_->written.fetch_add(sql.size());
        const std::uint64_t abandons_at_start = impl_->abandons;
        if (auto fault = s.take(Step::Begin)) {
            impl_->play(lock, *fault, abandons_at_start, true, nullptr);
        }
        const ParsedInsert parsed = parse_insert(sql);
        std::vector<HeaderColumn> header = insert_header(s, parsed);
        OpenInsert in;
        in.server = server;
        in.database = parsed.database;
        in.table = parsed.table;
        in.token = parsed.token;
        in.header = header;
        {
            const std::lock_guard<std::mutex> server_lock(s.mu);
            TableState* t = s.find(parsed.database, parsed.table);
            if (t == nullptr) {
                throw_server(ch::UNKNOWN_TABLE,
                             "Table " + parsed.database + "." + parsed.table + " does not exist");
            }
            in.record = t->inserts.size();
            ReceivedInsert record;
            record.token = parsed.token;
            record.sql = sql;
            t->inserts.push_back(std::move(record));
        }
        impl_->insert = std::move(in);
        std::uint64_t header_bytes = 0;
        for (const auto& c : header) {
            header_bytes += c.name.size() + c.type.size();
        }
        impl_->read.fetch_add(header_bytes);
        return header;
    } catch (const std::system_error&) {
        impl_->fail_insert(true);
        throw;
    }
}

void FakeTransport::send_block(const ::clickhouse::Block& block) {
    std::unique_lock<std::mutex> lock(impl_->mu);
    impl_->require_connected();
    if (!impl_->insert) {
        throw ch::ValidationError("illegal to send insert data without first calling BeginInsert");
    }
    try {
        impl_->require_live();
        if (!impl_->insert->pending) {
            throw ch::ProtocolError("fake server: this INSERT has already failed");
        }
        FakeServer::Impl& s = impl_->insert->server->impl();
        const std::uint64_t abandons_at_start = impl_->abandons;
        if (auto fault = s.take(Step::Send)) {
            impl_->play(lock, *fault, abandons_at_start, true, nullptr);
        }
        Captured captured = capture(block, impl_->insert->header);
        impl_->written.fetch_add(captured.received.bytes.size());
        OpenInsert& in = *impl_->insert;
        if (in.deferred_error.empty() && !captured.mismatch.empty()) {
            in.deferred_error = std::move(captured.mismatch);
        }
        {
            const std::lock_guard<std::mutex> server_lock(s.mu);
            if (TableState* t = s.find(in.database, in.table)) {
                t->inserts.at(in.record).blocks.push_back(std::move(captured.received));
            }
        }
        in.blocks.push_back(std::move(captured.held));
    } catch (const std::system_error&) {
        impl_->fail_insert(true);
        throw;
    } catch (...) {
        impl_->fail_insert(false);
        throw;
    }
}

void FakeTransport::end_insert() {
    std::unique_lock<std::mutex> lock(impl_->mu);
    impl_->require_connected();
    if (!impl_->insert) {
        return;  // as the client: EndInsert outside an INSERT does nothing
    }
    try {
        impl_->require_live();
        if (!impl_->insert->pending) {
            throw ch::ProtocolError("fake server: this INSERT has already failed");
        }
        if (!impl_->insert->deferred_error.empty()) {
            throw_server(ch::TYPE_MISMATCH,
                         "fake server: a block does not match the INSERT header: " +
                             impl_->insert->deferred_error);
        }
        const std::vector<Part> all = impl_->parts();
        FakeServer::Impl& s = impl_->insert->server->impl();
        const std::uint64_t abandons_at_start = impl_->abandons;
        if (auto fault = s.take(Step::End)) {
            impl_->play(lock, *fault, abandons_at_start, true, [&](const Fault& f) {
                switch (f.landing) {
                    case Fault::Landing::Nothing:
                        break;
                    case Fault::Landing::Everything:
                        impl_->land_parts(all, all.size());
                        break;
                    case Fault::Landing::FirstPartitions:
                        impl_->land_parts(all, f.landed_partitions);
                        break;
                }
            });
        }
        impl_->land_parts(all, all.size());
        impl_->close_insert(ReceivedInsert::Outcome::Committed);
    } catch (const std::system_error&) {
        impl_->fail_insert(true);
        throw;
    } catch (...) {
        impl_->fail_insert(false);
        throw;
    }
}

void FakeTransport::abandon() noexcept {
    const std::lock_guard<std::mutex> lock(impl_->mu);
    ++impl_->abandons;
    impl_->cv.notify_all();
    if (impl_->insert) {
        FakeServer::Impl& s = impl_->insert->server->impl();
        {
            const std::lock_guard<std::mutex> server_lock(s.mu);
            ++s.abandoned;
        }
        try {
            impl_->close_insert(ReceivedInsert::Outcome::Abandoned);
        } catch (...) {
            impl_->insert.reset();
        }
    }
    impl_->conn.reset();
}

void FakeTransport::interrupt() noexcept {
    const std::lock_guard<std::mutex> lock(impl_->mu);
    impl_->interrupted = true;
    impl_->cv.notify_all();
}

void FakeTransport::set_deadline(
    std::optional<std::chrono::steady_clock::time_point> deadline) noexcept {
    const std::lock_guard<std::mutex> lock(impl_->mu);
    impl_->deadline = deadline;
}

TransportCounters FakeTransport::counters() const noexcept {
    TransportCounters out;
    out.bytes_written = impl_->written.load();
    out.bytes_read = impl_->read.load();
    out.connects = impl_->connects.load();
    return out;
}

TransportFactory fake_factory(std::shared_ptr<FakeServer> server) {
    return [server = std::move(server)](const SinkOptions&) -> std::unique_ptr<InsertTransport> {
        return std::make_unique<FakeTransport>(server);
    };
}

TransportFactory fake_factory(std::map<Endpoint, std::shared_ptr<FakeServer>> servers) {
    return [servers = std::move(servers)](const SinkOptions&) -> std::unique_ptr<InsertTransport> {
        return std::make_unique<FakeTransport>(servers);
    };
}

// ---------------------------------------------------------------------------
// faulty()

namespace {

class FaultyTransport final : public InsertTransport {
public:
    FaultyTransport(std::unique_ptr<InsertTransport> inner,
                    std::shared_ptr<std::deque<Fault>> faults)
        : inner_(std::move(inner)), faults_(std::move(faults)) {}

    void connect(const Endpoint& endpoint) override {
        if (!inner_->connected()) {
            fire(Step::Connect, nullptr);
        }
        inner_->connect(endpoint);
    }
    [[nodiscard]] bool connected() const noexcept override { return inner_->connected(); }
    [[nodiscard]] const ServerIdentity& server() const override { return inner_->server(); }
    ResultSet select(MetaQuery kind, const std::string& sql) override {
        fire(Step::Select, nullptr);
        return inner_->select(kind, sql);
    }
    std::vector<HeaderColumn> begin_insert(const std::string& sql) override {
        fire(Step::Begin, nullptr);
        return inner_->begin_insert(sql);
    }
    void send_block(const ::clickhouse::Block& block) override {
        fire(Step::Send, nullptr);
        inner_->send_block(block);
    }
    void end_insert() override {
        bool ended = false;
        fire(Step::End, [&] {
            inner_->end_insert();
            ended = true;
        });
        if (!ended) {
            inner_->end_insert();
        }
    }
    void abandon() noexcept override {
        {
            const std::lock_guard<std::mutex> lock(mu_);
            ++abandons_;
        }
        cv_.notify_all();
        inner_->abandon();
    }
    void interrupt() noexcept override {
        {
            const std::lock_guard<std::mutex> lock(mu_);
            interrupted_ = true;
        }
        cv_.notify_all();
        inner_->interrupt();
    }
    void set_deadline(
        std::optional<std::chrono::steady_clock::time_point> deadline) noexcept override {
        {
            const std::lock_guard<std::mutex> lock(mu_);
            deadline_ = deadline;
        }
        inner_->set_deadline(deadline);
    }
    [[nodiscard]] TransportCounters counters() const noexcept override {
        return inner_->counters();
    }

private:
    void check(std::uint64_t abandons_at_start) const {
        if (interrupted_) {
            throw_system(ECONNABORTED, "faulty transport: interrupted");
        }
        if (abandons_ != abandons_at_start) {
            throw_system(ECONNABORTED, "faulty transport: abandoned during the call");
        }
    }

    void wait(std::unique_lock<std::mutex>& lock, Deadline until, std::uint64_t abandons_at_start) {
        const Deadline limit = deadline_;
        const bool times_out = limit && (!until || *limit < *until);
        const Deadline end = times_out ? limit : until;
        for (;;) {
            check(abandons_at_start);
            const auto now = Clock::now();
            if (end && now >= *end) {
                if (times_out) {
                    throw_system(ETIMEDOUT, "faulty transport: past the attempt deadline");
                }
                return;
            }
            if (end) {
                cv_.wait_until(lock, *end);
            } else {
                cv_.wait(lock);
            }
        }
    }

    // `land_everything` delegates the End, for Landing::Everything.
    void fire(Step step, const std::function<void()>& land_everything) {
        std::unique_lock<std::mutex> lock(mu_);
        std::optional<Fault> fault;
        if (faults_) {
            fault = take_fault(*faults_, counts_, step);
        }
        if (!fault) {
            return;
        }
        validate_fault(*fault);
        const std::uint64_t abandons_at_start = abandons_;
        if (fault->delay.count() > 0) {
            if (fault->kind == Fault::Kind::Uninterruptible) {
                lock.unlock();
                std::this_thread::sleep_for(fault->delay);
                lock.lock();
            } else {
                wait(lock, Clock::now() + fault->delay, abandons_at_start);
            }
        }
        if (fault->landing == Fault::Landing::FirstPartitions) {
            throw std::logic_error("faulty(): Landing::FirstPartitions needs a FakeServer");
        }
        if (fault->landing == Fault::Landing::Everything && land_everything) {
            lock.unlock();
            land_everything();
            lock.lock();
        }
        if (fault->on_fire) {
            lock.unlock();
            fault->on_fire();
            lock.lock();
        }
        if (is_failure(fault->kind)) {
            raise_failure(*fault);
        }
        if (fault->kind == Fault::Kind::Hang) {
            wait(lock, std::nullopt, abandons_at_start);
        } else if (fault->kind == Fault::Kind::Uninterruptible) {
            lock.unlock();
            hold_until_release();
            lock.lock();
        }
        check(abandons_at_start);
    }

    std::unique_ptr<InsertTransport> inner_;
    std::shared_ptr<std::deque<Fault>> faults_;
    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::array<std::size_t, kSteps> counts_{};
    bool interrupted_{false};
    std::uint64_t abandons_{0};
    Deadline deadline_;
};

}  // namespace

std::unique_ptr<InsertTransport> faulty(std::unique_ptr<InsertTransport> inner,
                                        std::shared_ptr<std::deque<Fault>> faults) {
    if (!inner) {
        throw std::invalid_argument("faulty(): no transport to wrap");
    }
    return std::make_unique<FaultyTransport>(std::move(inner), std::move(faults));
}

}  // namespace clink::clickhouse::native::testing
