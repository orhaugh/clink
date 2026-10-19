#pragma once

// The JSON row form of a PostgreSQL change event for the SQL CDC contract
// (`postgres_cdc_source` with `changelog_mode` set).
//
// Each declared column is converted from PostgreSQL's text form to the value
// the engine holds for its declared SQL type, and a value that type cannot hold
// exactly is refused rather than rounded, truncated or carried as the wrong
// kind. The supported pairs:
//
//   declared SQL type        PostgreSQL types        row value
//   BIGINT                   int2, int4, int8        integer
//   INTEGER                  int2, int4              integer
//   SMALLINT                 int2                    integer
//   DOUBLE                   float4, float8          number (NaN, Infinity refused)
//   REAL                     float4                  number (NaN, Infinity refused)
//   DECIMAL(p, s)            numeric, int2/4/8       exact decimal; more than p - s
//                                                    integer digits or s fraction
//                                                    digits refused, as is NaN
//   BOOLEAN                  bool                    true / false
//   DATE                     date                    'YYYY-MM-DD' (BC, infinity refused)
//   TIMESTAMP(p)             timestamp               epoch milliseconds, the wall time
//                                                    read as UTC
//   TIMESTAMP(p) WITH TIME   timestamptz             epoch milliseconds
//   ZONE
//   VARCHAR                  any type                PostgreSQL's text form, verbatim
//
// A timestamp with non-zero digits below the millisecond is refused: the
// engine holds every TIMESTAMP as epoch milliseconds. Declare the column
// VARCHAR to carry the full text instead, or let the column drop those digits
// (on_sub_millisecond='truncate'), which rounds the value down to the
// millisecond. A key column is never truncated: two keys could become one.
//
// Change metadata is available as declared columns with reserved names:
//   __op       'snapshot', 'insert', 'update' or 'delete'
//   __lsn      the change's WAL position as text ('0/16E2A38'); for a snapshot
//              row, the position streaming starts from
//   __version  the change's WAL position as a number, 0 for a snapshot row.
//              Per key it increases with every change, so it can version a
//              table that keeps the latest row per key
//   __deleted  true for a delete
//   __table    the schema-qualified source table
//   __xid      the transaction id (0 for a snapshot row)
//
// In upsert mode each row also carries the changelog kind under __row_kind:
// insert for a snapshot row or an INSERT, update_after for an UPDATE, delete
// for a DELETE. In append mode no row carries one: every change is an
// appended row.

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "clink/connectors/cdc_event.hpp"

namespace clink::postgres::cdc {

// How changes reach SQL.
enum class ChangelogMode : std::uint8_t {
    // A changelog keyed by the primary key, tagged with __row_kind.
    Upsert,
    // An append-only stream with one row per change.
    Append,
};

// The engine-side kinds a declared column can have.
enum class Kind : std::uint8_t {
    BigInt,
    Integer,
    SmallInt,
    Double,
    Real,
    Decimal,
    Boolean,
    Date,
    Timestamp,
    TimestampTz,
    Varchar,
    // Metadata columns.
    MetaOp,
    MetaLsn,
    MetaVersion,
    MetaDeleted,
    MetaTable,
    MetaXid,
};

struct ColumnPlan {
    std::string name;
    std::string sql_type;  // as declared, for messages
    Kind kind{Kind::Varchar};
    int precision{0};  // DECIMAL only
    int scale{0};      // DECIMAL only
    // TIMESTAMP columns: digits below the millisecond are dropped, rounding
    // the value down, rather than refused.
    bool truncate_sub_millisecond{false};
};

// A value or a declaration the contract cannot carry. The message names the
// column and says what to change.
class EncodeError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// Parse the declared columns ("name:SQL TYPE;name:SQL TYPE;..." as the SQL
// planner passes them in `declared_types`) into a plan. Throws EncodeError for
// a type the contract does not support or an unknown reserved name.
std::vector<ColumnPlan> plan_columns(std::string_view declared_types);

// True when `name` is a reserved metadata column.
bool is_metadata_column(std::string_view name);

// Let every TIMESTAMP column of `plan` that is not one of `key_columns` drop
// digits below the millisecond (on_sub_millisecond='truncate').
void truncate_sub_millisecond_outside_key(std::vector<ColumnPlan>& plan,
                                          const std::vector<std::string>& key_columns);

// Why a PostgreSQL column of type `pg_type` (pg_type.typname, e.g. "int4",
// "timestamptz", or "schema.name" for a user-defined type) cannot feed
// `column`, or nullopt when it can.
std::optional<std::string> type_pair_problem(std::string_view pg_type, const ColumnPlan& column);

// Convert one non-null value from PostgreSQL's text form to the JSON token the
// engine reads for the column's declared type. Throws EncodeError when the
// value cannot be held exactly, unless the column allows the one loss it
// may: then `truncated`, when given, is set when digits were dropped.
std::string convert_value(std::string_view pg_type,
                          std::string_view text,
                          const ColumnPlan& column,
                          bool* truncated = nullptr);

// Epoch milliseconds of a PostgreSQL timestamp or timestamptz text value
// ('2026-10-19 10:00:00.123' or with an offset '+05:30'). A timestamp without
// an offset is read as UTC. Throws EncodeError for BC dates, infinity, text
// that is not a timestamp, and non-zero digits below the millisecond unless
// `truncate` is set, when they are dropped (the value rounds down, before
// 1970 too) and `truncated`, when given, says so.
std::int64_t timestamp_to_epoch_ms(std::string_view text,
                                   bool with_zone,
                                   bool truncate = false,
                                   bool* truncated = nullptr);

// Parse "X/Y" (hexadecimal halves) into an LSN. nullopt on malformed text.
std::optional<std::uint64_t> parse_lsn(std::string_view text);

class RowEncoder {
public:
    RowEncoder(std::vector<ColumnPlan> columns, ChangelogMode mode);

    // The JSON object for one Insert, Update or Delete event, or nullopt for a
    // transaction marker or other event kind. `snapshot_row` marks a row of the
    // initial snapshot. Throws EncodeError when a value cannot be carried. Each
    // column whose value lost digits below the millisecond is appended to
    // `truncated`, when given.
    [[nodiscard]] std::optional<std::string> encode(
        const CdcEvent& ev, bool snapshot_row, std::vector<std::string>* truncated = nullptr) const;

    [[nodiscard]] const std::vector<ColumnPlan>& columns() const noexcept { return columns_; }
    [[nodiscard]] ChangelogMode mode() const noexcept { return mode_; }

private:
    std::vector<ColumnPlan> columns_;
    ChangelogMode mode_;
};

}  // namespace clink::postgres::cdc
