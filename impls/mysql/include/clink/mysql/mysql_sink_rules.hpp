#pragma once

// Pure rules of the MySQL sinks, testable without a server: the destination
// table as information_schema describes it and the contract each sink mode
// needs of it, the rendering of a record's values as MySQL literals, the
// statements' heads and tails, and the redaction of record values from server
// messages. The record reader, keys, the changelog marker, statement packing
// and the changelog buffer are the SQL sinks' shared rules
// (clink/connectors/sink_record_rules.hpp).

#include <cstddef>
#include <cstdint>
#include <list>
#include <map>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "clink/connectors/sink_record_rules.hpp"
#include "clink/connectors/sql_json_builder.hpp"

namespace clink::mysql {

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

struct ColumnInfo {
    std::string name;
    std::string data_type;         // DATA_TYPE in lower case, e.g. "varchar"
    std::string column_type;       // COLUMN_TYPE in lower case, e.g. "int unsigned"
    bool generated{false};         // a VIRTUAL or STORED generated column
    int datetime_precision{-1};    // fractional digits of a DATETIME, TIMESTAMP or TIME
    std::string collation;         // empty for a column that holds no text
    bool collation_no_pad{false};  // the collation's PAD_ATTRIBUTE is NO PAD
};

struct UniqueIndex {
    std::string name;
    bool primary{false};
    std::vector<std::string> columns;  // in index order
    bool prefix{false};                // a column indexed on a prefix of its values
};

struct TableSchema {
    bool exists{false};
    bool view{false};
    std::string engine;
    bool transactional{false};        // the engine supports transactions
    std::vector<ColumnInfo> columns;  // ordinal order
    std::vector<UniqueIndex> unique_indexes;
    [[nodiscard]] const ColumnInfo* column(std::string_view name) const;
};

enum class SinkMode : std::uint8_t {
    Append,          // mysql_sink: INSERT
    InsertOrUpdate,  // mysql_sink, on_duplicate='update': INSERT ... ON DUPLICATE KEY UPDATE
    Changelog,       // mysql_upsert_sink: upserts and deletes by key, in order
};

// What a sink writes: checked against the table when it opens.
struct SinkTarget {
    std::string table;
    std::vector<std::string> columns;
    sqljson::TimeColumns timestamp_columns;
    SinkMode mode{SinkMode::Append};
    std::vector<std::string> update_columns;  // InsertOrUpdate: the SET list (empty = all)
    std::vector<std::string> key_columns;     // Changelog: the row's identity
    std::uint32_t parallelism{1};             // subtasks writing the table
};

// Whether MySQL tells the column's values apart exactly as the stream does:
// integers, exact decimals, VARBINARY, VARCHAR under a binary NO PAD collation,
// and a DATETIME or TIMESTAMP that arrives as epoch milliseconds (`epoch_ms`)
// and keeps them. A CHAR column (trailing spaces dropped), a case- or
// accent-insensitive collation, a PAD SPACE one, a BINARY column (zero padded),
// a FLOAT, or a date written as text in more than one spelling can take two
// values the stream holds apart as one row.
[[nodiscard]] bool key_identity_exact(const ColumnInfo& c, bool epoch_ms);

// Why the table cannot be written as `t` describes, one reason each; empty when
// it can.
[[nodiscard]] std::vector<std::string> check_target(const SinkTarget& t, const TableSchema& s);

// ---- rendering --------------------------------------------------------------------------

// The longest plain decimal a number is written with; MySQL's exact values
// have at most 65 digits.
inline constexpr std::size_t kMaxNumberChars = 100;

// One member as a SQL literal for column `col` (nullptr when the sink knows no
// column of that name). A number is written as its exact plain decimal, except
// to a FLOAT or DOUBLE column, which takes the token as the server reads it. A
// TIMESTAMP column takes epoch milliseconds, written as its UTC time, and
// refuses milliseconds the column would round away.
[[nodiscard]] std::string render_literal(const JsonMember& v,
                                         const ColumnInfo* col,
                                         sqljson::TimeKind time,
                                         const sqljson::EscapeFn& esc);

// A record's values for `columns` as one VALUES row, "(v1,v2,...)". A member
// the record lacks is NULL, as a null one is.
[[nodiscard]] std::string render_row(const JsonMembers& m,
                                     const std::vector<std::string>& columns,
                                     const TableSchema& schema,
                                     const sqljson::TimeColumns& times,
                                     const sqljson::EscapeFn& esc);

// A record's key as one DELETE ... IN list item: "v" for one key column,
// "(v1,v2)" for several. A key member missing or null is refused.
[[nodiscard]] std::string render_key(const JsonMembers& m,
                                     const std::vector<std::string>& key_columns,
                                     const TableSchema& schema,
                                     const sqljson::TimeColumns& times,
                                     const sqljson::EscapeFn& esc);

// Refuses a key member not written as its column's values compare: a number for
// an integer, DECIMAL, DATETIME or TIMESTAMP key column, a string for a VARCHAR
// or VARBINARY one. With more than one subtask a row's changes reach one
// subtask only if its key is written one way.
void check_key_spelling(const JsonMembers& m,
                        const std::vector<std::string>& key_columns,
                        const TableSchema& schema);

// ---- statements -------------------------------------------------------------------------

// The INSERT's head ("INSERT INTO `t` (`a`,`b`) VALUES ") and, for an upsert,
// its tail (" AS clink_new ON DUPLICATE KEY UPDATE `a`=clink_new.`a`,...").
[[nodiscard]] std::string insert_head(const std::string& table,
                                      const std::vector<std::string>& columns);
[[nodiscard]] std::string upsert_tail(const std::vector<std::string>& set_columns);
// The DELETE's head ("DELETE FROM `t` WHERE `k` IN (") and tail (")").
[[nodiscard]] std::string delete_head(const std::string& table,
                                      const std::vector<std::string>& key_columns);
inline constexpr std::string_view kDeleteTail = ")";

// ---- server messages ------------------------------------------------------------------

// A server error or warning message with the record values it quotes replaced
// by <value>: the entry of a duplicate key, the value a conversion refused.
// Column and index names stay.
[[nodiscard]] std::string redact_server_message(std::string_view message);

}  // namespace clink::mysql
