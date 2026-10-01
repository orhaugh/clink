#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <arrow/api.h>

namespace clink::clickhouse::native {

// Declared SQL type of one input column, parsed from the sql_column_types
// spelling that clink::sql::arrow_to_sql_type_string renders.
enum class SqlKind : std::uint8_t {
    TinyInt,
    SmallInt,
    Integer,
    BigInt,
    Real,
    Double,
    Boolean,
    Varchar,
    Decimal,
    Date,
    Timestamp,
    Time,
    Bytea,
    Array,
    Map,
    Row,
    Unsupported
};

struct SqlType {
    SqlKind kind{SqlKind::Unsupported};
    int precision{0};                      // DECIMAL p; TIMESTAMP p as rendered (0, 3, 6 or 9)
    int scale{0};                          // DECIMAL s
    bool with_time_zone{false};            // TIMESTAMP ... WITH TIME ZONE
    std::vector<SqlType> children;         // ARRAY: element; MAP: key, value; ROW: fields
    std::vector<std::string> field_names;  // ROW
    std::string spelling;                  // as received, for messages
};

struct SqlColumn {
    std::string name;
    SqlType type;
};

// "name:SPELLING;name:SPELLING". Throws NativeSinkError(option_invalid) on a
// malformed spec; an unknown spelling yields SqlKind::Unsupported, which the
// column plan refuses with the rest.
[[nodiscard]] std::vector<SqlColumn> parse_sql_column_types(const std::string& spec);

// The sink-local Arrow layout of a declared column. nullptr for Time, Bytea
// and Unsupported.
[[nodiscard]] std::shared_ptr<arrow::DataType> arrow_type_for(const SqlType&);

// A ClickHouse column type, parsed from system.columns.type.
enum class ChKind : std::uint8_t {
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    Float32,
    Float64,
    Bool,
    String,
    FixedString,
    Enum8,
    Enum16,
    UUID,
    IPv4,
    IPv6,
    Date,
    Date32,
    DateTime,
    DateTime64,
    Decimal,
    Array,
    Map,
    Tuple,
    Unsupported
};

struct ChType {
    ChKind kind{ChKind::Unsupported};
    bool nullable{false};
    bool low_cardinality{false};
    int precision{0};             // Decimal P, DateTime64 P
    int scale{0};                 // Decimal S
    std::uint32_t fixed_size{0};  // FixedString N
    std::string timezone;         // DateTime, DateTime64
    std::vector<std::pair<std::string, std::int16_t>> enum_items;
    std::vector<ChType> children;            // Array: element; Map: key, value; Tuple: elements
    std::vector<std::string> element_names;  // named Tuple, all or none
    std::string spelling;                    // system.columns.type verbatim
    std::string unsupported_reason;
};

// Never throws: an unsupported type comes back as ChKind::Unsupported with a
// reason. Handles Nullable, LowCardinality, Array, Map, named and unnamed
// Tuple, Decimal(P,S), Decimal32/64/128(S), Decimal256 (refused),
// DateTime([tz]), DateTime64(P[, tz]), FixedString(N), Enum8/16('a' = 1, ...),
// and the plain names.
[[nodiscard]] ChType parse_ch_type(const std::string& spelling);

// The type name the pinned client gives a header column built from this
// server spelling: ::clickhouse::CreateColumnByType(spelling)->Type()->GetName().
// Empty when the client cannot build it. Bool comes back as "UInt8", because
// the pinned build maps Bool to UInt8 when parsing.
[[nodiscard]] std::string client_header_spelling(const std::string& spelling);

}  // namespace clink::clickhouse::native
