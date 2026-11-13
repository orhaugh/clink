#pragma once

// The declared SQL types of a sink table and how a row's values become the
// typed $rows parameter, through Relate's parameter builder. No value passes
// through text on the way:
//
//   TINYINT, SMALLINT, INTEGER, BIGINT   INTEGER
//   REAL, DOUBLE                         FLOAT (NaN and the infinities included)
//   BOOLEAN                              BOOLEAN
//   VARCHAR                              STRING
//   DATE                                 DATE
//   TIMESTAMP                            LOCAL DATETIME
//   TIMESTAMP WITH TIME ZONE             ZONED DATETIME, at UTC
//   ARRAY                                LIST
//   MAP<VARCHAR, ...>, ROW               MAP
//   SQL NULL                             null
//
// DECIMAL, TIME and BYTEA are refused when the sink opens.

#include <cstdint>
#include <relate.h>
#include <string>
#include <vector>

#include "clink/sql/row.hpp"

namespace clink::relate {

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
    // Declared, but with no mapping this sink accepts. Refused at open, with
    // the reason.
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

// "name:SPELLING;name:SPELLING". Throws RelateError(option_invalid) on a
// malformed list; an unknown spelling parses as Kind::Unsupported.
[[nodiscard]] std::vector<Column> parse_column_types(const std::string& spec);

// Every column this sink cannot send, one line each with the reason and what to
// declare instead. Empty when all are accepted.
[[nodiscard]] std::vector<std::string> unsupported_columns(const std::vector<Column>& columns);

// What a row adds to a batch, as the sink counts it against batch_bytes: each
// name, each string's bytes, and sixteen bytes for every value and container,
// eight more for a number or a boolean. Never less than what Relate's builder
// counts against its own limit for the same row.
[[nodiscard]] std::uint64_t row_weight(const std::vector<Column>& columns,
                                       const clink::sql::Row& row);

// The $rows parameter of one batch while it is built: a list with one map per
// row, keyed by the declared column names.
class RowsParam {
public:
    RowsParam();
    ~RowsParam();
    RowsParam(const RowsParam&) = delete;
    RowsParam& operator=(const RowsParam&) = delete;

    // Appends `row`. A column the row does not carry is null. Throws
    // RelateError(value_invalid), naming the column and the kind of value it
    // holds, never the value, when a value does not fit its declared type or
    // Relate refuses it; and RelateError(row_too_large) when the builder can
    // take no more rows. After a throw the batch may hold part of a row and
    // must not be sent.
    void add(const std::vector<Column>& columns, const clink::sql::Row& row, std::uint64_t weight);

    // The parameters to run the statement with: {rows: [...]}.
    [[nodiscard]] relate_params get() const noexcept { return root_; }
    [[nodiscard]] std::uint64_t rows() const noexcept { return rows_; }
    [[nodiscard]] std::uint64_t bytes() const noexcept { return bytes_; }
    // Empties the batch.
    void clear();

private:
    void reset_();

    relate_params root_{nullptr};
    relate_params list_{nullptr};
    std::uint64_t rows_{0};
    std::uint64_t bytes_{0};
};

}  // namespace clink::relate
