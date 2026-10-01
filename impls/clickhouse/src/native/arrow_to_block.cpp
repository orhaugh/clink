#include "native/arrow_to_block.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <cstring>
#include <limits>
#include <map>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

#include <arpa/inet.h>
#include <arrow/util/byte_size.h>
#include <arrow/util/decimal.h>
#include <clickhouse/columns/array.h>
#include <clickhouse/columns/bool.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/decimal.h>
#include <clickhouse/columns/enum.h>
#include <clickhouse/columns/ip4.h>
#include <clickhouse/columns/ip6.h>
#include <clickhouse/columns/map.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/tuple.h>
#include <clickhouse/columns/uuid.h>
#include <netinet/in.h>

#include "native/errors.hpp"

namespace clink::clickhouse::native {

namespace {

namespace ch = ::clickhouse;

// An empty value still gets a real address, so that writing the block out
// never hands memcpy a null pointer.
constexpr std::string_view kEmptyText{""};

constexpr std::size_t kViewBytes = sizeof(std::string_view);

// ColumnString::Reserve(n) also reserves its table of storage blocks, one
// entry per 16 values, and a sliced String column holds one entry. An entry is
// two size_t fields and a buffer pointer in the pinned client.
constexpr std::size_t kStringBlockEntryBytes =
    2 * sizeof(std::size_t) + sizeof(std::unique_ptr<char[]>);

// The offsets of an Array, or of the Array under a Map, are UInt64.
constexpr std::size_t kOffsetBytes = sizeof(std::uint64_t);

// DateTime64 and Date32 hold 1900-01-01 to 2299-12-31 23:59:59.
constexpr std::int64_t kFirstSecond = -2208988800;
constexpr std::int64_t kSecondAfterLast = 10413792000;
constexpr std::int32_t kFirstDay = -25567;
constexpr std::int32_t kLastDay = 120529;

// A cell the target cannot take. `at` counts from the first row of the
// append that found it; each composite maps it back to its own row on the
// way out, so the builder can name the chunk row.
struct CellFailure {
    std::int64_t at;
    std::string column;
    std::string reason;
};

// What a column tree costs, in the two measures the writer charges.
struct Tally {
    std::size_t payload{0};
    std::size_t owned{0};
};

std::size_t to_size(std::int64_t v) {
    return static_cast<std::size_t>(v);
}

std::string ticked(const std::string& name) {
    return "`" + name + "`";
}

std::string text_length(std::size_t bytes) {
    return "text of " + std::to_string(bytes) + " bytes";
}

std::int64_t pow10_64(int n) {
    std::int64_t v = 1;
    for (int i = 0; i < n; ++i) {
        v *= 10;
    }
    return v;
}

ch::Int128 pow10_128(int n) {
    ch::Int128 v = 1;
    for (int i = 0; i < n; ++i) {
        v *= 10;
    }
    return v;
}

// The storage ColumnDecimal picks for a precision.
std::size_t decimal_width(int precision) {
    return precision <= 9 ? 4 : (precision <= 18 ? 8 : 16);
}

int unit_digits(arrow::TimeUnit::type unit) {
    switch (unit) {
        case arrow::TimeUnit::SECOND:
            return 0;
        case arrow::TimeUnit::MILLI:
            return 3;
        case arrow::TimeUnit::MICRO:
            return 6;
        case arrow::TimeUnit::NANO:
            return 9;
    }
    return 3;
}

const char* unit_suffix(int digits) {
    switch (digits) {
        case 0:
            return "s";
        case 6:
            return "us";
        case 9:
            return "ns";
        default:
            return "ms";
    }
}

arrow::Type::type arrow_layout(SqlKind kind) {
    switch (kind) {
        case SqlKind::TinyInt:
            return arrow::Type::INT8;
        case SqlKind::SmallInt:
            return arrow::Type::INT16;
        case SqlKind::Integer:
            return arrow::Type::INT32;
        case SqlKind::BigInt:
            return arrow::Type::INT64;
        case SqlKind::Real:
            return arrow::Type::FLOAT;
        case SqlKind::Double:
            return arrow::Type::DOUBLE;
        case SqlKind::Boolean:
            return arrow::Type::BOOL;
        case SqlKind::Varchar:
            return arrow::Type::STRING;
        case SqlKind::Decimal:
            return arrow::Type::DECIMAL128;
        case SqlKind::Date:
            return arrow::Type::DATE32;
        case SqlKind::Timestamp:
            return arrow::Type::TIMESTAMP;
        case SqlKind::Array:
            return arrow::Type::LIST;
        case SqlKind::Map:
            return arrow::Type::MAP;
        case SqlKind::Row:
            return arrow::Type::STRUCT;
        case SqlKind::Time:
        case SqlKind::Bytea:
        case SqlKind::Unsupported:
            break;
    }
    return arrow::Type::NA;
}

// A chunk built by anything but RowArrowBuilder (or V2's columnar path) for
// this plan is a defect upstream of the sink, not a bad row.
[[noreturn]] void layout_mismatch(const ColumnBinding& b, const arrow::Array& array) {
    throw std::invalid_argument("clickhouse native sink: column " + ticked(b.name) +
                                " arrives as " + array.type()->ToString() +
                                ", but the plan declares it " + b.source.spelling);
}

[[noreturn]] void no_converter(const ColumnBinding& b) {
    throw std::invalid_argument("clickhouse native sink: no converter for column " +
                                ticked(b.name) + ", " + b.source.spelling + " into " +
                                b.target.spelling);
}

// Reserves room for `needed` values in `column`, growing by at least half
// again so that a block built from many small appends copies its values a
// bounded number of times. Returns the bytes newly reserved at `slot_bytes` a
// value.
template <typename Column>
std::size_t grow(Column& column,
                 std::size_t& reserved,
                 std::size_t needed,
                 std::size_t slot_bytes) {
    if (needed <= reserved) {
        return 0;
    }
    const std::size_t capacity = std::max(needed, reserved + reserved / 2);
    column.Reserve(capacity);
    const std::size_t added = capacity - reserved;
    reserved = capacity;
    return added * slot_bytes;
}

// A Map column that keeps its own handle on the Array(Tuple(K, V)) it wraps,
// as the client's typed ColumnMapT does. ColumnMap's handle is private, and a
// split needs to measure the copies Slice makes inside a map.
class MapColumn final : public ch::ColumnMap {
public:
    explicit MapColumn(std::shared_ptr<ch::ColumnArray> entries)
        : ch::ColumnMap(entries), entries_(std::move(entries)) {}

    [[nodiscard]] const std::shared_ptr<ch::ColumnArray>& entries() const noexcept {
        return entries_;
    }

    ch::ColumnRef Slice(std::size_t begin, std::size_t len) const override {
        return std::make_shared<MapColumn>(entries_->Slice(begin, len)->As<ch::ColumnArray>());
    }

    ch::ColumnRef CloneEmpty() const override {
        return std::make_shared<MapColumn>(entries_->CloneEmpty()->As<ch::ColumnArray>());
    }

    void Swap(ch::Column& other) override {
        auto& map = dynamic_cast<MapColumn&>(other);
        entries_.swap(map.entries_);
        ch::ColumnMap::Swap(other);
    }

private:
    std::shared_ptr<ch::ColumnArray> entries_;
};

// A DateTime column whose slices and empty clones keep its time zone. The
// client's ColumnDateTime builds both without one, so a split part of a
// DateTime('UTC') column would go out as plain DateTime, which an INSERT that
// refuses type conversion need not accept. Nullable, Array, Tuple and Map
// slice and clone their children through these overrides, so the zone holds
// at every depth.
class DateTimeColumn final : public ch::ColumnDateTime {
public:
    explicit DateTimeColumn(std::string timezone) : ch::ColumnDateTime(std::move(timezone)) {}

    DateTimeColumn(std::string timezone, std::vector<std::uint32_t>&& values)
        : ch::ColumnDateTime(std::move(timezone), std::move(values)) {}

    ch::ColumnRef Slice(std::size_t begin, std::size_t len) const override {
        // The client's slice copies the values into a vector of exactly the
        // part's size, which the zoned column takes over without a copy.
        const auto plain = ch::ColumnDateTime::Slice(begin, len)->As<ch::ColumnDateTime>();
        return std::make_shared<DateTimeColumn>(Timezone(), std::move(plain->GetWritableData()));
    }

    ch::ColumnRef CloneEmpty() const override {
        return std::make_shared<DateTimeColumn>(Timezone());
    }
};

// Converts one binding's Arrow array into its client column, and owns that
// column until take(). Every row index is logical: Arrow's accessors add the
// array's own offset() to both the validity bitmap and the value buffers.
class Node {
public:
    Node(const ColumnBinding& binding, Tally& tally) : binding_(binding), tally_(tally) {}
    virtual ~Node() = default;
    Node(const Node&) = delete;
    Node& operator=(const Node&) = delete;
    Node(Node&&) = delete;
    Node& operator=(Node&&) = delete;

    // Starts a new, empty column tree, children included.
    void reset() {
        nulls_.reset();
        nulls_reserved_ = 0;
        ch::ColumnRef values = fresh();
        if (binding_.target.nullable) {
            nulls_ = std::make_shared<ch::ColumnUInt8>();
            column_ = std::make_shared<ch::ColumnNullable>(std::move(values), nulls_);
        } else {
            column_ = std::move(values);
        }
    }

    // Appends rows [begin, begin + count) of `array`. Throws CellFailure.
    void append(const arrow::Array& array, std::int64_t begin, std::int64_t count) {
        check_layout(array);
        if (count == 0) {
            return;
        }
        const bool has_nulls = array.null_count() != 0;
        if (binding_.target.nullable) {
            const std::size_t n = to_size(count);
            tally_.owned += grow(*nulls_, nulls_reserved_, nulls_->Size() + n, 1);
            auto& flags = nulls_->GetWritableData();
            for (std::int64_t i = 0; i < count; ++i) {
                flags.push_back(static_cast<std::uint8_t>(has_nulls && array.IsNull(begin + i)));
            }
            tally_.payload += n;
        } else if (has_nulls) {
            for (std::int64_t i = 0; i < count; ++i) {
                if (array.IsNull(begin + i)) {
                    fail(i, null_reason());
                }
            }
        }
        put(array, begin, count, has_nulls);
    }

    [[nodiscard]] const ch::ColumnRef& column() const noexcept { return column_; }

protected:
    // Builds the empty value column, without the Nullable wrapper.
    virtual ch::ColumnRef fresh() = 0;
    // Appends the values. A null row, which only a Nullable target reaches,
    // gets a placeholder and no checks.
    virtual void put(const arrow::Array& array,
                     std::int64_t begin,
                     std::int64_t count,
                     bool has_nulls) = 0;

    virtual void check_layout(const arrow::Array& array) const {
        if (array.type_id() != arrow_layout(binding_.source.kind)) {
            layout_mismatch(binding_, array);
        }
    }

    [[nodiscard]] virtual std::string null_reason() const {
        return "null in non-Nullable column " + ticked(binding_.name);
    }

    [[noreturn]] void fail(std::int64_t at, std::string reason) const {
        throw CellFailure{at, binding_.name, std::move(reason)};
    }

    static bool is_null(const arrow::Array& array, bool has_nulls, std::int64_t row) {
        return has_nulls && array.IsNull(row);
    }

    const ColumnBinding& binding_;
    Tally& tally_;

private:
    ch::ColumnRef column_;
    std::shared_ptr<ch::ColumnUInt8> nulls_;
    std::size_t nulls_reserved_{0};
};

template <typename T>
constexpr const char* integer_name() {
    if constexpr (std::is_same_v<T, std::int8_t>) {
        return "Int8";
    } else if constexpr (std::is_same_v<T, std::int16_t>) {
        return "Int16";
    } else if constexpr (std::is_same_v<T, std::int32_t>) {
        return "Int32";
    } else if constexpr (std::is_same_v<T, std::int64_t>) {
        return "Int64";
    } else if constexpr (std::is_same_v<T, std::uint8_t>) {
        return "UInt8";
    } else if constexpr (std::is_same_v<T, std::uint16_t>) {
        return "UInt16";
    } else if constexpr (std::is_same_v<T, std::uint32_t>) {
        return "UInt32";
    } else {
        return "UInt64";
    }
}

// Integers and floats: a copy when the widths match, a widening, or a
// narrowing checked per value.
template <typename SourceType, typename Target>
class NumberNode final : public Node {
public:
    using Node::Node;

private:
    using Source = typename SourceType::c_type;

    std::shared_ptr<ch::ColumnVector<Target>> values_;
    std::size_t reserved_{0};

    ch::ColumnRef fresh() override {
        values_ = std::make_shared<ch::ColumnVector<Target>>();
        reserved_ = 0;
        return values_;
    }

    void put(const arrow::Array& array,
             std::int64_t begin,
             std::int64_t count,
             bool has_nulls) override {
        const std::size_t n = to_size(count);
        tally_.owned += grow(*values_, reserved_, values_->Size() + n, sizeof(Target));
        auto& out = values_->GetWritableData();
        const std::size_t base = out.size();
        out.resize(base + n);
        const Source* in =
            static_cast<const arrow::NumericArray<SourceType>&>(array).raw_values() + begin;
        if constexpr (std::is_same_v<Source, Target>) {
            std::memcpy(out.data() + base, in, n * sizeof(Target));
            if (has_nulls) {
                for (std::size_t i = 0; i < n; ++i) {
                    if (array.IsNull(begin + static_cast<std::int64_t>(i))) {
                        out[base + i] = Target{};
                    }
                }
            }
        } else {
            for (std::size_t i = 0; i < n; ++i) {
                const auto at = static_cast<std::int64_t>(i);
                out[base + i] =
                    is_null(array, has_nulls, begin + at) ? Target{} : convert(in[i], at);
            }
        }
        tally_.payload += n * sizeof(Target);
    }

    Target convert(Source v, std::int64_t at) const {
        if constexpr (std::is_floating_point_v<Target>) {
            return static_cast<Target>(v);
        } else if constexpr (std::is_same_v<Target, ch::Int128>) {
            return ch::Int128(v);
        } else {
            if (!std::in_range<Target>(v)) {
                fail(at, std::string("value out of range for ") + integer_name<Target>());
            }
            return static_cast<Target>(v);
        }
    }
};

// BOOLEAN unpacks Arrow's bitmap into one byte a value. A Bool target is sent
// as ColumnBool, so the server sees "Bool"; UInt8 as ColumnUInt8.
template <typename Column>
class BoolNode final : public Node {
public:
    using Node::Node;

private:
    std::shared_ptr<Column> values_;
    std::size_t reserved_{0};

    ch::ColumnRef fresh() override {
        values_ = std::make_shared<Column>();
        reserved_ = 0;
        return values_;
    }

    void put(const arrow::Array& array,
             std::int64_t begin,
             std::int64_t count,
             bool has_nulls) override {
        const auto& bits = static_cast<const arrow::BooleanArray&>(array);
        const std::size_t n = to_size(count);
        tally_.owned += grow(*values_, reserved_, values_->Size() + n, 1);
        for (std::int64_t i = 0; i < count; ++i) {
            const std::int64_t row = begin + i;
            const bool v = !is_null(array, has_nulls, row) && bits.Value(row);
            if constexpr (std::is_same_v<Column, ch::ColumnBool>) {
                values_->Append(v);
            } else {
                values_->GetWritableData().push_back(static_cast<std::uint8_t>(v));
            }
        }
        tally_.payload += n;
    }
};

// VARCHAR into String, LowCardinality(String) included: views into the
// chunk's own buffers, which the writer keeps alive until the INSERT is
// acknowledged.
class StringNode final : public Node {
public:
    using Node::Node;

private:
    std::shared_ptr<ch::ColumnString> values_;
    std::size_t reserved_{0};

    static std::size_t table_entries(std::size_t capacity) {
        return capacity == 0 ? 0 : std::max<std::size_t>(1, capacity / 16);
    }

    ch::ColumnRef fresh() override {
        values_ = std::make_shared<ch::ColumnString>();
        reserved_ = 0;
        return values_;
    }

    void put(const arrow::Array& array,
             std::int64_t begin,
             std::int64_t count,
             bool has_nulls) override {
        const auto& text = static_cast<const arrow::StringArray&>(array);
        const std::size_t entries_before = table_entries(reserved_);
        tally_.owned += grow(*values_, reserved_, values_->Size() + to_size(count), kViewBytes);
        tally_.owned += (table_entries(reserved_) - entries_before) * kStringBlockEntryBytes;
        for (std::int64_t i = 0; i < count; ++i) {
            const std::int64_t row = begin + i;
            std::string_view v = is_null(array, has_nulls, row) ? kEmptyText : text.GetView(row);
            if (v.empty()) {
                v = kEmptyText;
            }
            values_->AppendNoManagedLifetime(v);
            tally_.payload += v.size() + 1;
        }
    }
};

// VARCHAR into FixedString(N): up to N bytes, zero-padded by the client.
class FixedStringNode final : public Node {
public:
    using Node::Node;

private:
    std::shared_ptr<ch::ColumnFixedString> values_;
    std::size_t reserved_{0};

    ch::ColumnRef fresh() override {
        values_ = std::make_shared<ch::ColumnFixedString>(binding_.target.fixed_size);
        reserved_ = 0;
        return values_;
    }

    void put(const arrow::Array& array,
             std::int64_t begin,
             std::int64_t count,
             bool has_nulls) override {
        const auto& text = static_cast<const arrow::StringArray&>(array);
        const std::size_t width = binding_.target.fixed_size;
        const std::size_t n = to_size(count);
        tally_.owned += grow(*values_, reserved_, values_->Size() + n, width);
        for (std::int64_t i = 0; i < count; ++i) {
            const std::int64_t row = begin + i;
            const std::string_view v =
                is_null(array, has_nulls, row) ? kEmptyText : text.GetView(row);
            if (v.size() > width) {
                fail(i,
                     "string of " + std::to_string(v.size()) +
                         " bytes is longer than FixedString(" + std::to_string(width) + ")");
            }
            values_->Append(v.empty() ? kEmptyText : v);
        }
        tally_.payload += n * width;
    }
};

// VARCHAR into Enum8 or Enum16, by exact item name.
template <typename T>
class EnumNode final : public Node {
public:
    EnumNode(const ColumnBinding& binding, Tally& tally) : Node(binding, tally) {
        if (binding.target.enum_items.empty()) {
            no_converter(binding);
        }
        for (const auto& [name, value] : binding.target.enum_items) {
            by_name_.emplace(name, static_cast<T>(value));
        }
        // A NULL row needs some item in the nested column; the server ignores
        // it, but a value outside the enum would be malformed.
        placeholder_ = static_cast<T>(binding.target.enum_items.front().second);
    }

private:
    std::map<std::string, T, std::less<>> by_name_;
    T placeholder_{};
    std::shared_ptr<ch::ColumnEnum<T>> values_;
    std::size_t reserved_{0};

    ch::ColumnRef fresh() override {
        const auto& items = binding_.target.enum_items;
        auto type = sizeof(T) == 1 ? ch::Type::CreateEnum8(items) : ch::Type::CreateEnum16(items);
        values_ = std::make_shared<ch::ColumnEnum<T>>(std::move(type));
        reserved_ = 0;
        return values_;
    }

    void put(const arrow::Array& array,
             std::int64_t begin,
             std::int64_t count,
             bool has_nulls) override {
        const auto& text = static_cast<const arrow::StringArray&>(array);
        const std::size_t n = to_size(count);
        tally_.owned += grow(*values_, reserved_, values_->Size() + n, sizeof(T));
        for (std::int64_t i = 0; i < count; ++i) {
            const std::int64_t row = begin + i;
            if (is_null(array, has_nulls, row)) {
                values_->Append(placeholder_);
                continue;
            }
            const std::string_view v = text.GetView(row);
            const auto it = by_name_.find(v);
            if (it == by_name_.end()) {
                fail(i, text_length(v.size()) + " is not an item of " + binding_.target.spelling);
            }
            values_->Append(it->second);
        }
        tally_.payload += n * sizeof(T);
    }
};

int hex_digit(char c) {
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

// The canonical 8-4-4-4-12 form, read in text order: the first 16 digits are
// the high half, which ClickHouse stores first.
bool parse_uuid(std::string_view text, std::uint64_t& high, std::uint64_t& low) {
    if (text.size() != 36 || text[8] != '-' || text[13] != '-' || text[18] != '-' ||
        text[23] != '-') {
        return false;
    }
    std::uint64_t halves[2] = {0, 0};
    int digits = 0;
    for (std::size_t i = 0; i < text.size(); ++i) {
        if (i == 8 || i == 13 || i == 18 || i == 23) {
            continue;
        }
        const int d = hex_digit(text[i]);
        if (d < 0) {
            return false;
        }
        std::uint64_t& half = halves[digits / 16];
        half = (half << 4) | static_cast<std::uint64_t>(d);
        ++digits;
    }
    high = halves[0];
    low = halves[1];
    return true;
}

class UuidNode final : public Node {
public:
    using Node::Node;

private:
    std::shared_ptr<ch::ColumnUInt64> halves_;
    std::size_t reserved_{0};

    ch::ColumnRef fresh() override {
        halves_ = std::make_shared<ch::ColumnUInt64>();
        reserved_ = 0;
        return std::make_shared<ch::ColumnUUID>(halves_);
    }

    void put(const arrow::Array& array,
             std::int64_t begin,
             std::int64_t count,
             bool has_nulls) override {
        const auto& text = static_cast<const arrow::StringArray&>(array);
        const std::size_t n = to_size(count);
        // ColumnUUID::Reserve counts 64-bit halves, not values, so the halves
        // are reserved directly.
        tally_.owned += grow(*halves_, reserved_, halves_->Size() + 2 * n, sizeof(std::uint64_t));
        auto& out = halves_->GetWritableData();
        for (std::int64_t i = 0; i < count; ++i) {
            const std::int64_t row = begin + i;
            std::uint64_t high = 0;
            std::uint64_t low = 0;
            if (!is_null(array, has_nulls, row)) {
                const std::string_view v = text.GetView(row);
                if (!parse_uuid(v, high, low)) {
                    fail(i,
                         text_length(v.size()) +
                             " is not a UUID in the 8-4-4-4-12 hexadecimal form");
                }
            }
            out.push_back(high);
            out.push_back(low);
        }
        tally_.payload += n * 16;
    }
};

// Arrow views are not NUL-terminated, and inet_pton reads to a NUL, so each
// value is copied into a terminated buffer first. `Limit` is the longest text
// the address family can spell.
template <std::size_t Limit>
bool terminated_copy(std::string_view text, std::array<char, Limit + 1>& buffer) {
    if (text.size() > Limit) {
        return false;
    }
    if (!text.empty()) {
        std::memcpy(buffer.data(), text.data(), text.size());
    }
    buffer[text.size()] = '\0';
    return true;
}

// A dotted quad as the host-order number ClickHouse stores. ColumnIPv4 over a
// UInt32 column applies no conversion of its own.
class Ipv4Node final : public Node {
public:
    using Node::Node;

private:
    std::shared_ptr<ch::ColumnUInt32> words_;
    std::size_t reserved_{0};

    ch::ColumnRef fresh() override {
        words_ = std::make_shared<ch::ColumnUInt32>();
        reserved_ = 0;
        return std::make_shared<ch::ColumnIPv4>(words_);
    }

    void put(const arrow::Array& array,
             std::int64_t begin,
             std::int64_t count,
             bool has_nulls) override {
        const auto& text = static_cast<const arrow::StringArray&>(array);
        const std::size_t n = to_size(count);
        tally_.owned += grow(*words_, reserved_, words_->Size() + n, sizeof(std::uint32_t));
        auto& out = words_->GetWritableData();
        std::array<char, INET_ADDRSTRLEN> buffer{};
        for (std::int64_t i = 0; i < count; ++i) {
            const std::int64_t row = begin + i;
            std::uint32_t word = 0;
            if (!is_null(array, has_nulls, row)) {
                const std::string_view v = text.GetView(row);
                in_addr address{};
                if (!terminated_copy<INET_ADDRSTRLEN - 1>(v, buffer) ||
                    inet_pton(AF_INET, buffer.data(), &address) != 1) {
                    fail(i, text_length(v.size()) + " is not a dotted-quad IPv4 address");
                }
                word = ntohl(address.s_addr);
            }
            out.push_back(word);
        }
        tally_.payload += n * sizeof(std::uint32_t);
    }
};

// IPv6 text, or dotted IPv4 text as the mapped address ::ffff:a.b.c.d.
class Ipv6Node final : public Node {
public:
    using Node::Node;

private:
    std::shared_ptr<ch::ColumnIPv6> values_;
    std::size_t reserved_{0};

    ch::ColumnRef fresh() override {
        values_ = std::make_shared<ch::ColumnIPv6>();
        reserved_ = 0;
        return values_;
    }

    static bool parse(std::string_view text, in6_addr& out) {
        std::array<char, INET6_ADDRSTRLEN> buffer{};
        if (!terminated_copy<INET6_ADDRSTRLEN - 1>(text, buffer)) {
            return false;
        }
        if (inet_pton(AF_INET6, buffer.data(), &out) == 1) {
            return true;
        }
        in_addr v4{};
        if (inet_pton(AF_INET, buffer.data(), &v4) != 1) {
            return false;
        }
        std::memset(&out, 0, sizeof(out));
        out.s6_addr[10] = 0xff;
        out.s6_addr[11] = 0xff;
        std::memcpy(&out.s6_addr[12], &v4.s_addr, sizeof(v4.s_addr));
        return true;
    }

    void put(const arrow::Array& array,
             std::int64_t begin,
             std::int64_t count,
             bool has_nulls) override {
        const auto& text = static_cast<const arrow::StringArray&>(array);
        const std::size_t n = to_size(count);
        tally_.owned += grow(*values_, reserved_, values_->Size() + n, sizeof(in6_addr));
        for (std::int64_t i = 0; i < count; ++i) {
            const std::int64_t row = begin + i;
            in6_addr address{};
            if (!is_null(array, has_nulls, row)) {
                const std::string_view v = text.GetView(row);
                if (!parse(v, address)) {
                    fail(i, text_length(v.size()) + " is not an IPv6 or dotted-quad IPv4 address");
                }
            }
            values_->Append(address);
        }
        tally_.payload += n * sizeof(in6_addr);
    }
};

// DECIMAL(p, s) into Decimal(P, S): the unscaled value times 10^(S - s), in
// 128 bits. The open rule makes every declared value fit, so the range check
// is a backstop against a chunk that breaks its own declared precision.
class DecimalNode final : public Node {
public:
    using Node::Node;

private:
    std::shared_ptr<ch::ColumnDecimal> values_;
    std::size_t reserved_{0};

    [[nodiscard]] std::string out_of_range() const {
        return "value out of range for Decimal(" + std::to_string(binding_.target.precision) +
               ", " + std::to_string(binding_.target.scale) + ")";
    }

    ch::ColumnRef fresh() override {
        values_ =
            std::make_shared<ch::ColumnDecimal>(static_cast<std::size_t>(binding_.target.precision),
                                                static_cast<std::size_t>(binding_.target.scale));
        reserved_ = 0;
        return values_;
    }

    void check_layout(const arrow::Array& array) const override {
        // The plan's multiplier assumes the declared scale.
        if (array.type_id() != arrow::Type::DECIMAL128 ||
            static_cast<const arrow::Decimal128Type&>(*array.type()).scale() !=
                binding_.source.scale) {
            layout_mismatch(binding_, array);
        }
    }

    void put(const arrow::Array& array,
             std::int64_t begin,
             std::int64_t count,
             bool has_nulls) override {
        const auto& decimals = static_cast<const arrow::Decimal128Array&>(array);
        const std::size_t width = decimal_width(binding_.target.precision);
        const std::size_t n = to_size(count);
        tally_.owned += grow(*values_, reserved_, values_->Size() + n, width);
        const ch::Int128 limit = pow10_128(binding_.target.precision);
        const ch::Int128 multiplier = binding_.multiplier;
        const ch::Int128 headroom = std::numeric_limits<ch::Int128>::max() / multiplier;
        for (std::int64_t i = 0; i < count; ++i) {
            const std::int64_t row = begin + i;
            if (is_null(array, has_nulls, row)) {
                values_->Append(ch::Int128(0));
                continue;
            }
            const arrow::Decimal128 d(decimals.GetValue(row));
            ch::Int128 v = absl::MakeInt128(d.high_bits(), d.low_bits());
            if (v > headroom || v < -headroom) {
                fail(i, out_of_range());
            }
            v *= multiplier;
            if (v >= limit || v <= -limit) {
                fail(i, out_of_range());
            }
            values_->Append(v);
        }
        tally_.payload += n * width;
    }
};

// Timestamps carry their unit in the Arrow type: milliseconds in V1, any unit
// from the columnar path. The conversion is exact or refused, never floored.
class TimestampNode : public Node {
public:
    using Node::Node;

protected:
    void check_layout(const arrow::Array& array) const override {
        if (array.type_id() != arrow::Type::TIMESTAMP) {
            layout_mismatch(binding_, array);
        }
    }

    static int digits_of(const arrow::Array& array) {
        return unit_digits(static_cast<const arrow::TimestampType&>(*array.type()).unit());
    }

    // `v` in units of 10^-from seconds, as units of 10^-to seconds; nullopt
    // when that overflows int64, or when `v` is finer than the target unit,
    // in which case `exact` is false: such a value is refused, not floored.
    static std::optional<std::int64_t> rescale(std::int64_t v, int from, int to, bool& exact) {
        exact = true;
        if (to >= from) {
            std::int64_t out = 0;
            if (__builtin_mul_overflow(v, pow10_64(to - from), &out)) {
                return std::nullopt;
            }
            return out;
        }
        const std::int64_t divisor = pow10_64(from - to);
        if (v % divisor != 0) {
            exact = false;
            return std::nullopt;
        }
        return v / divisor;
    }

    [[nodiscard]] std::string sub_second(std::int64_t v,
                                         int from,
                                         const std::string& target) const {
        return std::to_string(v) + " " + unit_suffix(from) + " has sub-second digits " + target +
               " cannot hold; target DateTime64(" + std::to_string(from) +
               ") or write whole seconds";
    }
};

class DateTime64Node final : public TimestampNode {
public:
    using TimestampNode::TimestampNode;

private:
    std::shared_ptr<ch::ColumnDateTime64> values_;
    std::size_t reserved_{0};

    ch::ColumnRef fresh() override {
        const auto precision = static_cast<std::size_t>(binding_.target.precision);
        values_ = binding_.target.timezone.empty()
                      ? std::make_shared<ch::ColumnDateTime64>(precision)
                      : std::make_shared<ch::ColumnDateTime64>(precision, binding_.target.timezone);
        reserved_ = 0;
        return values_;
    }

    void put(const arrow::Array& array,
             std::int64_t begin,
             std::int64_t count,
             bool has_nulls) override {
        const auto* in = static_cast<const arrow::TimestampArray&>(array).raw_values() + begin;
        const int from = digits_of(array);
        const int to = binding_.target.precision;
        const std::string target = "DateTime64(" + std::to_string(to) + ")";
        const ch::Int128 first = ch::Int128(kFirstSecond) * pow10_128(to);
        const ch::Int128 last = ch::Int128(kSecondAfterLast) * pow10_128(to) - 1;
        const std::size_t n = to_size(count);
        tally_.owned += grow(*values_, reserved_, values_->Size() + n, sizeof(std::int64_t));
        for (std::int64_t i = 0; i < count; ++i) {
            if (is_null(array, has_nulls, begin + i)) {
                values_->Append(std::int64_t{0});
                continue;
            }
            const std::int64_t v = in[i];
            bool exact = true;
            const auto out = rescale(v, from, to, exact);
            if (!exact) {
                fail(i, sub_second(v, from, target));
            }
            if (!out || ch::Int128(*out) < first || ch::Int128(*out) > last) {
                fail(i,
                     std::to_string(v) + " " + unit_suffix(from) + " is outside the range of " +
                         target + ", 1900-01-01 00:00:00 to 2299-12-31 23:59:59");
            }
            values_->Append(*out);
        }
        tally_.payload += n * sizeof(std::int64_t);
    }
};

class DateTimeNode final : public TimestampNode {
public:
    using TimestampNode::TimestampNode;

private:
    std::shared_ptr<DateTimeColumn> values_;
    std::size_t reserved_{0};

    ch::ColumnRef fresh() override {
        // An empty zone spells plain DateTime, as the client's own default.
        values_ = std::make_shared<DateTimeColumn>(binding_.target.timezone);
        reserved_ = 0;
        return values_;
    }

    void put(const arrow::Array& array,
             std::int64_t begin,
             std::int64_t count,
             bool has_nulls) override {
        const auto* in = static_cast<const arrow::TimestampArray&>(array).raw_values() + begin;
        const int from = digits_of(array);
        const std::size_t n = to_size(count);
        tally_.owned += grow(*values_, reserved_, values_->Size() + n, sizeof(std::uint32_t));
        for (std::int64_t i = 0; i < count; ++i) {
            if (is_null(array, has_nulls, begin + i)) {
                values_->AppendRaw(0);
                continue;
            }
            const std::int64_t v = in[i];
            bool exact = true;
            const auto seconds = rescale(v, from, 0, exact);
            if (!exact) {
                fail(i, sub_second(v, from, "DateTime"));
            }
            if (!seconds || !std::in_range<std::uint32_t>(*seconds)) {
                fail(i,
                     std::to_string(v) + " " + unit_suffix(from) +
                         " is outside the range of DateTime, 1970-01-01 00:00:00 to 2106-02-07 "
                         "06:28:15");
            }
            values_->AppendRaw(static_cast<std::uint32_t>(*seconds));
        }
        tally_.payload += n * sizeof(std::uint32_t);
    }
};

// DATE (days since 1970-01-01) into Date32 or Date, range-checked.
template <typename Column, typename Raw>
class DateNode final : public Node {
public:
    using Node::Node;

private:
    std::shared_ptr<Column> values_;
    std::size_t reserved_{0};

    ch::ColumnRef fresh() override {
        values_ = std::make_shared<Column>();
        reserved_ = 0;
        return values_;
    }

    void put(const arrow::Array& array,
             std::int64_t begin,
             std::int64_t count,
             bool has_nulls) override {
        constexpr bool kDate32 = std::is_same_v<Column, ch::ColumnDate32>;
        const auto* in = static_cast<const arrow::Date32Array&>(array).raw_values() + begin;
        const std::size_t n = to_size(count);
        tally_.owned += grow(*values_, reserved_, values_->Size() + n, sizeof(Raw));
        for (std::int64_t i = 0; i < count; ++i) {
            if (is_null(array, has_nulls, begin + i)) {
                values_->AppendRaw(Raw{0});
                continue;
            }
            const std::int32_t v = in[i];
            const bool fits = kDate32 ? (v >= kFirstDay && v <= kLastDay) : std::in_range<Raw>(v);
            if (!fits) {
                fail(i,
                     std::to_string(v) +
                         (kDate32 ? " days is outside the range of Date32, -25567 to 120529 days "
                                    "(1900-01-01 to 2299-12-31)"
                                  : " days is outside the range of Date, 0 to 65535 days "
                                    "(1970-01-01 to 2149-06-06)"));
            }
            values_->AppendRaw(static_cast<Raw>(v));
        }
        tally_.payload += n * sizeof(Raw);
    }
};

// The row, counted from the first row of an append, whose entries hold
// `element`, counted from the first entry of that append. `offsets` points at
// the append's first row; empty rows own nothing and are skipped.
std::int64_t owning_row(const std::int32_t* offsets, std::int64_t count, std::int64_t element) {
    const std::int64_t target = static_cast<std::int64_t>(offsets[0]) + element;
    const std::int32_t* end = std::upper_bound(offsets + 1, offsets + count + 1, target);
    return static_cast<std::int64_t>(end - (offsets + 1));
}

// Appends the end offsets of rows [begin, begin + count) of a list or map,
// rebased onto the entries already in the column: with the list's own offsets
// (which already include the array's offset()), row i ends at
// offsets[begin + i + 1] - offsets[begin] past the entries before it.
void append_ends(ch::ColumnUInt64& ends_column,
                 const std::int32_t* offsets,
                 std::int64_t begin,
                 std::int64_t count) {
    auto& ends = ends_column.GetWritableData();
    const std::uint64_t base = ends.empty() ? 0 : ends.back();
    const std::int32_t first = offsets[begin];
    for (std::int64_t i = 0; i < count; ++i) {
        ends.push_back(base + static_cast<std::uint64_t>(offsets[begin + i + 1] - first));
    }
}

// T ARRAY into Array(T').
class ListNode final : public Node {
public:
    ListNode(const ColumnBinding& binding, Tally& tally, std::unique_ptr<Node> element)
        : Node(binding, tally), element_(std::move(element)) {}

private:
    std::unique_ptr<Node> element_;
    std::shared_ptr<ch::ColumnUInt64> ends_;
    std::size_t reserved_{0};

    ch::ColumnRef fresh() override {
        element_->reset();
        ends_ = std::make_shared<ch::ColumnUInt64>();
        reserved_ = 0;
        return std::make_shared<ch::ColumnArray>(element_->column(), ends_);
    }

    [[nodiscard]] std::string null_reason() const override { return "Array cannot be NULL"; }

    void put(const arrow::Array& array,
             std::int64_t begin,
             std::int64_t count,
             bool /*has_nulls*/) override {
        const auto& list = static_cast<const arrow::ListArray&>(array);
        const std::int32_t* offsets = list.raw_value_offsets();
        tally_.owned += grow(*ends_, reserved_, ends_->Size() + to_size(count), kOffsetBytes);
        append_ends(*ends_, offsets, begin, count);
        tally_.payload += to_size(count) * kOffsetBytes;
        const std::int64_t first = offsets[begin];
        try {
            element_->append(*list.values(), first, offsets[begin + count] - first);
        } catch (CellFailure& f) {
            f.at = owning_row(offsets + begin, count, f.at);
            throw;
        }
    }
};

// MAP<K, V> into Map(K', V'): Array(Tuple(K', V')) under the client's Map.
class MapNode final : public Node {
public:
    MapNode(const ColumnBinding& binding,
            Tally& tally,
            std::unique_ptr<Node> key,
            std::unique_ptr<Node> value)
        : Node(binding, tally), key_(std::move(key)), value_(std::move(value)) {}

private:
    std::unique_ptr<Node> key_;
    std::unique_ptr<Node> value_;
    std::shared_ptr<ch::ColumnUInt64> ends_;
    std::size_t reserved_{0};

    ch::ColumnRef fresh() override {
        key_->reset();
        value_->reset();
        ends_ = std::make_shared<ch::ColumnUInt64>();
        reserved_ = 0;
        auto pairs = std::make_shared<ch::ColumnTuple>(
            std::vector<ch::ColumnRef>{key_->column(), value_->column()});
        return std::make_shared<MapColumn>(
            std::make_shared<ch::ColumnArray>(std::move(pairs), ends_));
    }

    void check_layout(const arrow::Array& array) const override {
        if (array.type_id() != arrow::Type::MAP) {
            layout_mismatch(binding_, array);
        }
    }

    [[nodiscard]] std::string null_reason() const override { return "Map cannot be NULL"; }

    void put(const arrow::Array& array,
             std::int64_t begin,
             std::int64_t count,
             bool /*has_nulls*/) override {
        const auto& map = static_cast<const arrow::MapArray&>(array);
        // field() applies the entries' own offset, which keys() and items()
        // would not.
        const auto& entries = static_cast<const arrow::StructArray&>(*map.values());
        const std::shared_ptr<arrow::Array> keys = entries.field(0);
        const std::shared_ptr<arrow::Array> items = entries.field(1);
        const std::int32_t* offsets = map.raw_value_offsets();
        tally_.owned += grow(*ends_, reserved_, ends_->Size() + to_size(count), kOffsetBytes);
        append_ends(*ends_, offsets, begin, count);
        tally_.payload += to_size(count) * kOffsetBytes;
        const std::int64_t first = offsets[begin];
        const std::int64_t entries_count = offsets[begin + count] - first;
        try {
            key_->append(*keys, first, entries_count);
            value_->append(*items, first, entries_count);
        } catch (CellFailure& f) {
            f.at = owning_row(offsets + begin, count, f.at);
            throw;
        }
    }
};

// ROW<...> into Tuple(...), field by field in order, with the target's
// element names when it has them.
class TupleNode final : public Node {
public:
    TupleNode(const ColumnBinding& binding, Tally& tally, std::vector<std::unique_ptr<Node>> fields)
        : Node(binding, tally), fields_(std::move(fields)) {}

private:
    std::vector<std::unique_ptr<Node>> fields_;

    ch::ColumnRef fresh() override {
        std::vector<ch::ColumnRef> columns;
        columns.reserve(fields_.size());
        for (const auto& f : fields_) {
            f->reset();
            columns.push_back(f->column());
        }
        const auto& names = binding_.target.element_names;
        if (names.empty()) {
            return std::make_shared<ch::ColumnTuple>(columns);
        }
        return std::make_shared<ch::ColumnTuple>(columns, names);
    }

    void check_layout(const arrow::Array& array) const override {
        if (array.type_id() != arrow::Type::STRUCT ||
            static_cast<std::size_t>(array.type()->num_fields()) != fields_.size()) {
            layout_mismatch(binding_, array);
        }
    }

    [[nodiscard]] std::string null_reason() const override { return "Tuple cannot be NULL"; }

    void put(const arrow::Array& array,
             std::int64_t begin,
             std::int64_t count,
             bool /*has_nulls*/) override {
        const auto& record = static_cast<const arrow::StructArray&>(array);
        for (std::size_t k = 0; k < fields_.size(); ++k) {
            // field() applies the struct's own offset and length, so its rows
            // line up with the struct's.
            fields_[k]->append(*record.field(static_cast<int>(k)), begin, count);
        }
    }
};

std::unique_ptr<Node> make_node(const ColumnBinding& b, Tally& tally);

template <typename SourceType>
std::unique_ptr<Node> integer_node(const ColumnBinding& b, Tally& tally) {
    switch (b.target.kind) {
        case ChKind::Int8:
            return std::make_unique<NumberNode<SourceType, std::int8_t>>(b, tally);
        case ChKind::Int16:
            return std::make_unique<NumberNode<SourceType, std::int16_t>>(b, tally);
        case ChKind::Int32:
            return std::make_unique<NumberNode<SourceType, std::int32_t>>(b, tally);
        case ChKind::Int64:
            return std::make_unique<NumberNode<SourceType, std::int64_t>>(b, tally);
        case ChKind::Int128:
            return std::make_unique<NumberNode<SourceType, ch::Int128>>(b, tally);
        case ChKind::UInt8:
            return std::make_unique<NumberNode<SourceType, std::uint8_t>>(b, tally);
        case ChKind::UInt16:
            return std::make_unique<NumberNode<SourceType, std::uint16_t>>(b, tally);
        case ChKind::UInt32:
            return std::make_unique<NumberNode<SourceType, std::uint32_t>>(b, tally);
        case ChKind::UInt64:
            return std::make_unique<NumberNode<SourceType, std::uint64_t>>(b, tally);
        default:
            no_converter(b);
    }
}

std::unique_ptr<Node> number_node(const ColumnBinding& b, Tally& tally) {
    switch (b.source.kind) {
        case SqlKind::TinyInt:
            return integer_node<arrow::Int8Type>(b, tally);
        case SqlKind::SmallInt:
            return integer_node<arrow::Int16Type>(b, tally);
        case SqlKind::Integer:
            return integer_node<arrow::Int32Type>(b, tally);
        case SqlKind::BigInt:
            return integer_node<arrow::Int64Type>(b, tally);
        case SqlKind::Real:
            if (b.target.kind == ChKind::Float32) {
                return std::make_unique<NumberNode<arrow::FloatType, float>>(b, tally);
            }
            if (b.target.kind == ChKind::Float64) {
                return std::make_unique<NumberNode<arrow::FloatType, double>>(b, tally);
            }
            break;
        case SqlKind::Double:
            if (b.target.kind == ChKind::Float64) {
                return std::make_unique<NumberNode<arrow::DoubleType, double>>(b, tally);
            }
            break;
        default:
            break;
    }
    no_converter(b);
}

std::unique_ptr<Node> make_node(const ColumnBinding& b, Tally& tally) {
    const std::size_t children = b.children.size();
    switch (b.conversion) {
        case Conversion::Copy:
        case Conversion::WidenInt:
        case Conversion::NarrowInt:
        case Conversion::SignedToUnsigned:
        case Conversion::RealToDouble:
            return number_node(b, tally);
        case Conversion::BoolUnpack:
            if (b.target.kind == ChKind::Bool) {
                return std::make_unique<BoolNode<ch::ColumnBool>>(b, tally);
            }
            if (b.target.kind == ChKind::UInt8) {
                return std::make_unique<BoolNode<ch::ColumnUInt8>>(b, tally);
            }
            break;
        case Conversion::StringZeroCopy:
            return std::make_unique<StringNode>(b, tally);
        case Conversion::StringToFixed:
            return std::make_unique<FixedStringNode>(b, tally);
        case Conversion::StringToEnum:
            if (b.target.kind == ChKind::Enum8) {
                return std::make_unique<EnumNode<std::int8_t>>(b, tally);
            }
            if (b.target.kind == ChKind::Enum16) {
                return std::make_unique<EnumNode<std::int16_t>>(b, tally);
            }
            break;
        case Conversion::StringToUuid:
            return std::make_unique<UuidNode>(b, tally);
        case Conversion::StringToIpv4:
            return std::make_unique<Ipv4Node>(b, tally);
        case Conversion::StringToIpv6:
            return std::make_unique<Ipv6Node>(b, tally);
        case Conversion::DecimalRescale:
            if (b.multiplier >= 1 && b.target.precision >= 1 && b.target.precision <= 38) {
                return std::make_unique<DecimalNode>(b, tally);
            }
            break;
        case Conversion::TimestampToDateTime64:
            return std::make_unique<DateTime64Node>(b, tally);
        case Conversion::TimestampToDateTime:
            return std::make_unique<DateTimeNode>(b, tally);
        case Conversion::DateToDate32:
            return std::make_unique<DateNode<ch::ColumnDate32, std::int32_t>>(b, tally);
        case Conversion::DateToDate:
            return std::make_unique<DateNode<ch::ColumnDate, std::uint16_t>>(b, tally);
        case Conversion::List:
            if (children == 1) {
                return std::make_unique<ListNode>(b, tally, make_node(b.children[0], tally));
            }
            break;
        case Conversion::Map:
            if (children == 2) {
                return std::make_unique<MapNode>(
                    b, tally, make_node(b.children[0], tally), make_node(b.children[1], tally));
            }
            break;
        case Conversion::Struct:
            if (children != 0) {
                std::vector<std::unique_ptr<Node>> fields;
                fields.reserve(children);
                for (const auto& child : b.children) {
                    fields.push_back(make_node(child, tally));
                }
                return std::make_unique<TupleNode>(b, tally, std::move(fields));
            }
            break;
    }
    no_converter(b);
}

// The fixed width of a target value, or 0 for String and the composites.
std::size_t fixed_width(const ChType& t) {
    switch (t.kind) {
        case ChKind::Int8:
        case ChKind::UInt8:
        case ChKind::Bool:
        case ChKind::Enum8:
            return 1;
        case ChKind::Int16:
        case ChKind::UInt16:
        case ChKind::Enum16:
        case ChKind::Date:
            return 2;
        case ChKind::Int32:
        case ChKind::UInt32:
        case ChKind::Float32:
        case ChKind::Date32:
        case ChKind::DateTime:
        case ChKind::IPv4:
            return 4;
        case ChKind::Int64:
        case ChKind::UInt64:
        case ChKind::Float64:
        case ChKind::DateTime64:
            return 8;
        case ChKind::Int128:
        case ChKind::UInt128:
        case ChKind::UUID:
        case ChKind::IPv6:
            return 16;
        case ChKind::Decimal:
            return decimal_width(t.precision);
        case ChKind::FixedString:
            return t.fixed_size;
        default:
            return 0;
    }
}

// The payload measure of rows [begin, begin + count) of `array`, read from
// the Arrow buffers without converting anything.
std::size_t estimate(const ColumnBinding& b,
                     const arrow::Array& array,
                     std::int64_t begin,
                     std::int64_t count) {
    if (count <= 0) {
        return 0;
    }
    if (array.type_id() != arrow_layout(b.source.kind)) {
        layout_mismatch(b, array);
    }
    const std::size_t n = to_size(count);
    const std::size_t nulls = b.target.nullable ? n : 0;
    switch (b.conversion) {
        case Conversion::StringZeroCopy: {
            const auto& text = static_cast<const arrow::StringArray&>(array);
            return nulls + n +
                   static_cast<std::size_t>(text.value_offset(begin + count) -
                                            text.value_offset(begin));
        }
        case Conversion::List: {
            const auto& list = static_cast<const arrow::ListArray&>(array);
            const std::int32_t first = list.value_offset(begin);
            return n * kOffsetBytes + estimate(b.children.at(0),
                                               *list.values(),
                                               first,
                                               list.value_offset(begin + count) - first);
        }
        case Conversion::Map: {
            const auto& map = static_cast<const arrow::MapArray&>(array);
            const auto& entries = static_cast<const arrow::StructArray&>(*map.values());
            const std::int32_t first = map.value_offset(begin);
            const std::int64_t entries_count = map.value_offset(begin + count) - first;
            return n * kOffsetBytes +
                   estimate(b.children.at(0), *entries.field(0), first, entries_count) +
                   estimate(b.children.at(1), *entries.field(1), first, entries_count);
        }
        case Conversion::Struct: {
            const auto& record = static_cast<const arrow::StructArray&>(array);
            std::size_t total = 0;
            for (std::size_t k = 0; k < b.children.size(); ++k) {
                total += estimate(b.children[k], *record.field(static_cast<int>(k)), begin, count);
            }
            return total;
        }
        default:
            return nulls + n * fixed_width(b.target);
    }
}

// The width of a leaf column a slice holds, by its client type.
std::size_t slice_width(const ch::Column& column) {
    switch (column.GetType().GetCode()) {
        case ch::Type::Int8:
        case ch::Type::UInt8:
        case ch::Type::Bool:
        case ch::Type::Enum8:
            return 1;
        case ch::Type::Int16:
        case ch::Type::UInt16:
        case ch::Type::Enum16:
        case ch::Type::Date:
            return 2;
        case ch::Type::Int32:
        case ch::Type::UInt32:
        case ch::Type::Float32:
        case ch::Type::Date32:
        case ch::Type::DateTime:
        case ch::Type::IPv4:
            return 4;
        case ch::Type::Int64:
        case ch::Type::UInt64:
        case ch::Type::Float64:
        case ch::Type::DateTime64:
            return 8;
        case ch::Type::Int128:
        case ch::Type::UInt128:
        case ch::Type::UUID:
        case ch::Type::IPv6:
            return 16;
        default:
            throw std::logic_error("clickhouse native sink: a split met a " +
                                   column.GetType().GetName() +
                                   " column, which the block builder never makes");
    }
}

// What a column made by Column::Slice costs. Slice copies values and null
// flags into vectors of exactly the slice's size, and a String slice copies
// its values into one storage block of exactly their total length. An Array
// slice appends its offsets one at a time, so their vector can hold up to
// twice what they need, and its real capacity is what gets counted.
Tally measure(const ch::Column& column) {
    const std::size_t rows = column.Size();
    if (const auto* nullable = dynamic_cast<const ch::ColumnNullable*>(&column)) {
        Tally t = measure(*nullable->Nested());
        t.payload += rows;
        t.owned += rows;
        return t;
    }
    if (const auto* array = dynamic_cast<const ch::ColumnArray*>(&column)) {
        Tally t = measure(*array->GetData());
        t.payload += rows * kOffsetBytes;
        t.owned += array->GetOffsets()->Capacity() * kOffsetBytes;
        return t;
    }
    if (const auto* map = dynamic_cast<const MapColumn*>(&column)) {
        return measure(*map->entries());
    }
    if (const auto* tuple = dynamic_cast<const ch::ColumnTuple*>(&column)) {
        Tally t;
        for (std::size_t k = 0; k < tuple->TupleSize(); ++k) {
            const Tally part = measure(*tuple->At(k));
            t.payload += part.payload;
            t.owned += part.owned;
        }
        return t;
    }
    if (const auto* text = dynamic_cast<const ch::ColumnString*>(&column)) {
        std::size_t bytes = 0;
        for (std::size_t i = 0; i < rows; ++i) {
            bytes += text->At(i).size();
        }
        return Tally{bytes + rows,
                     rows * kViewBytes + bytes + (rows != 0 ? kStringBlockEntryBytes : 0)};
    }
    if (const auto* fixed = dynamic_cast<const ch::ColumnFixedString*>(&column)) {
        const std::size_t bytes = fixed->FixedSize() * rows;
        return Tally{bytes, bytes};
    }
    if (const auto* decimal = dynamic_cast<const ch::ColumnDecimal*>(&column)) {
        const std::size_t bytes = decimal_width(static_cast<int>(decimal->GetPrecision())) * rows;
        return Tally{bytes, bytes};
    }
    const std::size_t bytes = slice_width(column) * rows;
    return Tally{bytes, bytes};
}

// The longest-named top-level binding that `column` names or is an element of.
const ColumnBinding* binding_for(const ColumnPlan& plan, const std::string& column) {
    const ColumnBinding* best = nullptr;
    for (const auto& b : plan.columns) {
        const bool names =
            column == b.name || (column.size() > b.name.size() && column.starts_with(b.name) &&
                                 column[b.name.size()] == '.');
        if (names && (best == nullptr || b.name.size() > best->name.size())) {
            best = &b;
        }
    }
    return best;
}

std::string shortest(double v) {
    std::array<char, 32> buffer{};
    const auto result = std::to_chars(buffer.data(), buffer.data() + buffer.size(), v);
    return result.ec == std::errc{} ? std::string(buffer.data(), result.ptr) : std::string("?");
}

// The offending cell, by the redaction rule: a number shown, anything a
// person wrote shown only by its length.
std::string describe_cell(const ColumnBinding& b, const arrow::Array& array, std::int64_t row) {
    if (array.type_id() != arrow_layout(b.source.kind)) {
        return "value not shown";
    }
    if (array.IsNull(row)) {
        return "NULL";
    }
    switch (b.source.kind) {
        case SqlKind::TinyInt:
            return std::to_string(static_cast<const arrow::Int8Array&>(array).Value(row));
        case SqlKind::SmallInt:
            return std::to_string(static_cast<const arrow::Int16Array&>(array).Value(row));
        case SqlKind::Integer:
            return std::to_string(static_cast<const arrow::Int32Array&>(array).Value(row));
        case SqlKind::BigInt:
            return std::to_string(static_cast<const arrow::Int64Array&>(array).Value(row));
        case SqlKind::Real:
            return shortest(
                static_cast<double>(static_cast<const arrow::FloatArray&>(array).Value(row)));
        case SqlKind::Double:
            return shortest(static_cast<const arrow::DoubleArray&>(array).Value(row));
        case SqlKind::Boolean:
            return static_cast<const arrow::BooleanArray&>(array).Value(row) ? "true" : "false";
        case SqlKind::Decimal: {
            const auto& decimals = static_cast<const arrow::Decimal128Array&>(array);
            const auto& type = static_cast<const arrow::Decimal128Type&>(*array.type());
            return arrow::Decimal128(decimals.GetValue(row)).ToString(type.scale());
        }
        case SqlKind::Date:
            return std::to_string(static_cast<const arrow::Date32Array&>(array).Value(row)) +
                   " days";
        case SqlKind::Timestamp: {
            const auto& type = static_cast<const arrow::TimestampType&>(*array.type());
            return std::to_string(static_cast<const arrow::TimestampArray&>(array).Value(row)) +
                   " " + unit_suffix(unit_digits(type.unit()));
        }
        case SqlKind::Varchar:
            return "len " +
                   std::to_string(static_cast<const arrow::StringArray&>(array).value_length(row));
        case SqlKind::Array:
            return "len " +
                   std::to_string(static_cast<const arrow::ListArray&>(array).value_length(row));
        case SqlKind::Map:
            return "len " +
                   std::to_string(static_cast<const arrow::MapArray&>(array).value_length(row));
        case SqlKind::Row:
            return "len " + std::to_string(array.type()->num_fields());
        case SqlKind::Time:
        case SqlKind::Bytea:
        case SqlKind::Unsupported:
            break;
    }
    return "value not shown";
}

}  // namespace

struct BlockBuilder::Impl {
    ColumnPlan plan;
    Tally tally;
    std::vector<std::unique_ptr<Node>> nodes;
    std::size_t rows{0};
    // Set when an append threw part-way: the columns no longer line up.
    bool broken{false};

    void reset() {
        for (const auto& node : nodes) {
            node->reset();
        }
        tally = Tally{};
        rows = 0;
        broken = false;
    }

    void usable(const char* what) const {
        if (broken) {
            throw std::logic_error(std::string("clickhouse native sink: BlockBuilder::") + what +
                                   " after a failed append; reset() first");
        }
    }
};

BlockBuilder::BlockBuilder(const ColumnPlan& plan) : impl_(std::make_unique<Impl>()) {
    impl_->plan = plan;
    impl_->nodes.reserve(impl_->plan.columns.size());
    for (const auto& b : impl_->plan.columns) {
        impl_->nodes.push_back(make_node(b, impl_->tally));
    }
    impl_->reset();
}

BlockBuilder::~BlockBuilder() = default;
BlockBuilder::BlockBuilder(BlockBuilder&&) noexcept = default;
BlockBuilder& BlockBuilder::operator=(BlockBuilder&&) noexcept = default;

void BlockBuilder::append(const arrow::RecordBatch& chunk,
                          std::int64_t offset,
                          std::int64_t length) {
    auto& impl = *impl_;
    impl.usable("append");
    if (offset < 0 || length < 0 || offset > chunk.num_rows() ||
        length > chunk.num_rows() - offset) {
        throw std::out_of_range("clickhouse native sink: rows [" + std::to_string(offset) + ", " +
                                std::to_string(offset + length) + ") are outside a chunk of " +
                                std::to_string(chunk.num_rows()) + " rows");
    }
    for (const auto& b : impl.plan.columns) {
        if (b.input_index < 0 || b.input_index >= chunk.num_columns()) {
            throw std::invalid_argument("clickhouse native sink: column " + ticked(b.name) +
                                        " is input " + std::to_string(b.input_index) +
                                        ", but the chunk has " +
                                        std::to_string(chunk.num_columns()) + " columns");
        }
    }
    if (length == 0) {
        return;
    }
    try {
        for (std::size_t k = 0; k < impl.nodes.size(); ++k) {
            impl.nodes[k]->append(*chunk.column(impl.plan.columns[k].input_index), offset, length);
        }
    } catch (const CellFailure& f) {
        impl.broken = true;
        throw ConversionError(f.column, offset + f.at, f.reason);
    } catch (...) {
        impl.broken = true;
        throw;
    }
    impl.rows += to_size(length);
}

std::size_t BlockBuilder::rows() const noexcept {
    return impl_ ? impl_->rows : 0;
}

std::size_t BlockBuilder::payload_bytes() const noexcept {
    return impl_ ? impl_->tally.payload : 0;
}

std::size_t BlockBuilder::owned_bytes() const noexcept {
    return impl_ ? impl_->tally.owned : 0;
}

::clickhouse::Block BlockBuilder::take() {
    auto& impl = *impl_;
    impl.usable("take");
    if (impl.rows == 0) {
        throw std::logic_error(
            "clickhouse native sink: BlockBuilder::take on an empty block; a 0-row block is never "
            "sent");
    }
    ::clickhouse::Block block;
    for (std::size_t k = 0; k < impl.nodes.size(); ++k) {
        block.AppendColumn(impl.plan.columns[k].name, impl.nodes[k]->column());
    }
    block.RefreshRowCount();
    impl.reset();
    return block;
}

void BlockBuilder::reset() {
    impl_->reset();
}

double BlockBuilder::bytes_per_row(const arrow::RecordBatch& chunk) const {
    const std::int64_t rows = chunk.num_rows();
    if (rows == 0) {
        return 0.0;
    }
    std::size_t total = 0;
    for (const auto& b : impl_->plan.columns) {
        if (b.input_index < 0 || b.input_index >= chunk.num_columns()) {
            throw std::invalid_argument("clickhouse native sink: column " + ticked(b.name) +
                                        " is not in the chunk");
        }
        total += estimate(b, *chunk.column(b.input_index), 0, rows);
    }
    return static_cast<double>(total) / static_cast<double>(rows);
}

BlockSlice slice_block(const ::clickhouse::Block& block, std::size_t begin, std::size_t length) {
    const std::size_t rows = block.GetRowCount();
    if (length == 0 || begin > rows || length > rows - begin) {
        throw std::out_of_range("clickhouse native sink: cannot slice rows [" +
                                std::to_string(begin) + ", " + std::to_string(begin + length) +
                                ") from a block of " + std::to_string(rows) + " rows");
    }
    BlockSlice out;
    for (std::size_t k = 0; k < block.GetColumnCount(); ++k) {
        ch::ColumnRef part = block[k]->Slice(begin, length);
        const Tally t = measure(*part);
        out.payload_bytes += t.payload;
        out.owned_bytes += t.owned;
        out.block.AppendColumn(block.GetColumnName(k), part);
    }
    out.block.RefreshRowCount();
    out.rows = length;
    return out;
}

std::size_t chunk_bytes(const arrow::RecordBatch& chunk) {
    return static_cast<std::size_t>(arrow::util::TotalBufferSize(chunk));
}

std::string redacted_row(const ColumnPlan& plan,
                         const arrow::RecordBatch& chunk,
                         std::int64_t row,
                         const std::string& offending_column) {
    const ColumnBinding* offending = binding_for(plan, offending_column);
    std::string out;
    for (const auto& b : plan.columns) {
        if (!out.empty()) {
            out += ", ";
        }
        out += b.name + "=" + b.target.spelling;
        if (&b != offending) {
            continue;
        }
        if (b.input_index < 0 || b.input_index >= chunk.num_columns() || row < 0 ||
            row >= chunk.num_rows()) {
            out += "(row not in chunk)";
            continue;
        }
        out += "(" + describe_cell(b, *chunk.column(b.input_index), row) + ")";
    }
    return out;
}

}  // namespace clink::clickhouse::native
