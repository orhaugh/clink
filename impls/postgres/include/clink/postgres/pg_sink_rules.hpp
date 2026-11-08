#pragma once

// Pure rules of the PostgreSQL sinks, testable without a server: the
// destination table as pg_catalog describes it and the contract each sink mode
// needs of it, the rendering of a record's values as PostgreSQL literals, the
// statements' heads and tails, and the redaction of record values from server
// messages. The record reader, keys, the changelog marker, statement packing
// and the changelog buffer are the SQL sinks' shared rules
// (clink/connectors/sink_record_rules.hpp).

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "clink/connectors/sink_record_rules.hpp"
#include "clink/connectors/sql_json_builder.hpp"

namespace clink::postgres {

using sinkrules::change_kind_of;
using sinkrules::ChangeKind;
using sinkrules::ChangelogBuffer;
using sinkrules::JsonMember;
using sinkrules::JsonMembers;
using sinkrules::kRowKindMember;
using sinkrules::pack_statements;
using sinkrules::plain_decimal;
using sinkrules::read_json_object;
using sinkrules::RecordRefused;
using sinkrules::stream_key;

// ---- the destination table ------------------------------------------------------------

struct PgColumn {
    std::string name;
    std::string type_name;  // the base type's pg_type name: "int4", "numeric", "varchar", ...
    std::string type;       // format_type(), for messages: "numeric(10,2)"
    bool numeric_constrained{false};  // numeric(p, s), not plain numeric
    int numeric_scale{0};             // its s, negative from PostgreSQL 15 (rounds to tens)
    int char_length{-1};              // varchar(n), bpchar(n): n; -1 when unbounded
    int time_precision{-1};           // timestamp(p), timestamptz(p), time(p): p; -1 the default, 6
    bool generated{false};            // GENERATED ALWAYS AS (...) STORED
    bool identity_always{false};      // GENERATED ALWAYS AS IDENTITY
    bool collation_deterministic{true};
    std::string collation;  // empty for a type without one
};

struct PgUniqueIndex {
    std::string name;
    bool primary{false};
    std::vector<std::string> columns;  // its plain columns, in index order
    bool partial{false};               // a WHERE clause limits it
    bool expression{false};            // it indexes an expression
};

struct PgTable {
    bool exists{false};
    char kind{0};  // pg_class.relkind: 'r' table, 'p' partitioned table, 'v' view, 'f' foreign
    std::vector<PgColumn> columns;  // ordinal order
    std::vector<PgUniqueIndex> unique_indexes;
    [[nodiscard]] const PgColumn* column(std::string_view name) const;
};

enum class PgSinkMode : std::uint8_t {
    Append,             // postgres_sink: INSERT
    OnConflictUpdate,   // postgres_sink, on_conflict='update'
    OnConflictNothing,  // postgres_sink, on_conflict='nothing'
    Changelog,          // postgres_upsert_sink: upserts and deletes by key, in order
};

// What a sink writes: checked against the table when it opens.
struct PgSinkTarget {
    std::string table;
    std::vector<std::string> columns;
    sqljson::TimeColumns timestamp_columns;
    PgSinkMode mode{PgSinkMode::Append};
    std::vector<std::string> conflict_columns;  // the keyed modes' ON CONFLICT target
    std::vector<std::string> update_columns;    // OnConflictUpdate: the SET list (empty = all)
    std::uint32_t parallelism{1};               // subtasks writing the table
};

// Whether PostgreSQL tells the column's values apart exactly as the stream
// does: integers and numerics (as JSON numbers, by value), text and varchar
// under a deterministic collation, and a timestamp or timestamptz that arrives
// as epoch milliseconds (`epoch_ms`) and keeps them. A char(n) column (trailing
// spaces ignored), a nondeterministic collation, a uuid or a date (each with
// several spellings of one value), a float or a bytea can take two values the
// stream holds apart as one row.
[[nodiscard]] bool pg_key_identity_exact(const PgColumn& c, bool epoch_ms);

// Why the table cannot be written as `t` describes, one reason each; empty when
// it can.
[[nodiscard]] std::vector<std::string> check_pg_target(const PgSinkTarget& t, const PgTable& s);

// ---- rendering --------------------------------------------------------------------------

// One member as a PostgreSQL literal for column `col` (nullptr when the sink
// knows no column of that name). A number is written exactly, and is refused
// where the column would round it: a fraction for an integer column, more
// fractional digits than a numeric(p, s) column keeps. A FLOAT column takes the
// number as written. A string is refused when it holds U+0000, when it is
// longer than a varchar(n) or char(n) column, or when it is a number with more
// fractional digits than a numeric(p, s) column keeps; a bytea column takes its
// UTF-8 bytes. A TIMESTAMP column takes epoch milliseconds, written as its UTC
// time, and refuses milliseconds the column would round away.
[[nodiscard]] std::string render_pg_literal(const JsonMember& v,
                                            const PgColumn* col,
                                            sqljson::TimeKind time,
                                            const sqljson::EscapeFn& esc);

// A record's values for `columns` as one VALUES row, "(v1,v2,...)". A member
// the record lacks is NULL, as a null one is.
[[nodiscard]] std::string render_pg_row(const JsonMembers& m,
                                        const std::vector<std::string>& columns,
                                        const PgTable& table,
                                        const sqljson::TimeColumns& times,
                                        const sqljson::EscapeFn& esc);

// A record's key as one DELETE ... IN list item: "v" for one key column,
// "(v1,v2)" for several. A key member missing or null is refused.
[[nodiscard]] std::string render_pg_key(const JsonMembers& m,
                                        const std::vector<std::string>& key_columns,
                                        const PgTable& table,
                                        const sqljson::TimeColumns& times,
                                        const sqljson::EscapeFn& esc);

// Refuses a key member not written as its column's values compare: a number for
// an integer, numeric or timestamp key column, a string for a text or varchar
// one. With more than one subtask a row's changes reach one subtask only if its
// key is written one way.
void check_pg_key_spelling(const JsonMembers& m,
                           const std::vector<std::string>& key_columns,
                           const PgTable& table);

// The scale a decimal number's exact value needs: 2 for "1.25", 0 for "15" or
// "1.50e1", -1 for "150" (a multiple of ten), and the least possible value for
// zero, which every scale holds. A column of scale s keeps the value exactly
// when this is at most s. The text is a decimal number: an optional sign,
// digits with an optional point, an optional exponent.
[[nodiscard]] std::int64_t needed_scale(std::string_view number);

// ---- statements -------------------------------------------------------------------------

// The INSERT's head ("INSERT INTO "t" ("a","b") VALUES ") and the keyed tails:
// " ON CONFLICT ("k") DO UPDATE SET "a" = EXCLUDED."a",..." (DO NOTHING when no
// column is left to set) and " ON CONFLICT ("k") DO NOTHING".
[[nodiscard]] std::string pg_insert_head(const std::string& table,
                                         const std::vector<std::string>& columns);
[[nodiscard]] std::string pg_update_tail(const std::vector<std::string>& conflict_columns,
                                         const std::vector<std::string>& set_columns);
[[nodiscard]] std::string pg_nothing_tail(const std::vector<std::string>& conflict_columns);
// The DELETE's head ("DELETE FROM "t" WHERE "k" IN (") and tail (")").
[[nodiscard]] std::string pg_delete_head(const std::string& table,
                                         const std::vector<std::string>& key_columns);
inline constexpr std::string_view kPgDeleteTail = ")";

// ---- server messages ------------------------------------------------------------------

// A server message's primary text with the record values it quotes replaced by
// <value>: the input a type refused, the value out of a type's range.
// Identifiers stay. The DETAIL, which can quote a whole row, is never read.
[[nodiscard]] std::string redact_pg_message(std::string_view primary);

}  // namespace clink::postgres
