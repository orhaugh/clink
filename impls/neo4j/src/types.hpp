#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "clink/sql/row.hpp"

namespace clink::neo4j {

// A declared SQL column type, parsed from the sql_column_types spelling that
// clink::sql::arrow_to_sql_type_string renders.
enum class Kind : std::uint8_t {
    TinyInt,
    SmallInt,
    Integer,
    BigInt,
    Real,
    Double,
    Boolean,
    Varchar,
    Date,
    Timestamp,
    Array,
    Map,
    Row,
    // Declared, but with no Neo4j mapping this sink accepts. The open check
    // refuses them with the reason.
    Decimal,
    Time,
    Bytea,
    Unsupported,
};

struct SqlType {
    Kind kind{Kind::Unsupported};
    bool with_time_zone{false};            // Timestamp
    std::vector<SqlType> children;         // Array: element; Map: key, value; Row: fields
    std::vector<std::string> field_names;  // Row
    std::string spelling;                  // as declared, for messages
};

struct Column {
    std::string name;
    SqlType type;
};

// "name:SPELLING;name:SPELLING". Throws Neo4jSinkError(option_invalid) on a
// malformed list; an unknown spelling parses as Kind::Unsupported.
[[nodiscard]] std::vector<Column> parse_column_types(const std::string& spec);

// Every column this sink cannot send, one line each with the reason and what to
// declare instead. Empty when all are accepted.
[[nodiscard]] std::vector<std::string> unsupported_columns(const std::vector<Column>& columns);

// Appends one row as a typed JSON Map, {"$type":"Map","_value":{...}}, keyed by
// the declared column names. A column the row does not carry is Null. Throws
// Neo4jSinkError(value_invalid) naming the column and the kind it holds, never
// the value, when a value does not fit its declared type.
void encode_row(const std::vector<Column>& columns, const clink::sql::Row& row, std::string& out);

// Days since 1970-01-01 as YYYY-MM-DD, and epoch milliseconds as
// YYYY-MM-DDTHH:MM:SS.mmm, for years 1 to 9999; false outside them.
[[nodiscard]] bool format_date(std::int64_t days, std::string& out);
[[nodiscard]] bool format_local_datetime(std::int64_t epoch_ms, std::string& out);

// Whether `s` is well-formed UTF-8 (no overlong forms, surrogates or code
// points past U+10FFFF).
[[nodiscard]] bool valid_utf8(std::string_view s) noexcept;

// A JSON string literal for `s`, quotes included. `s` must be valid UTF-8.
void append_json_string(std::string_view s, std::string& out);

}  // namespace clink::neo4j
