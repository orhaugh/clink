#pragma once

// The rules the Cassandra and ScyllaDB sinks check before they write: names,
// the key column types the upsert sink accepts, and how one changelog record
// becomes an insert or a delete with its identity.

#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace clink::cassandra {

// The longest keyspace or table name Cassandra and ScyllaDB accept.
inline constexpr std::size_t kCqlMaxTableNameLength = 48;
// A column name: the sinks' own bound, far above any real schema.
inline constexpr std::size_t kCqlMaxColumnNameLength = 255;

// A keyspace, table or column name as the sinks accept it: a CQL unquoted
// identifier, a letter followed by letters, digits or underscores, at most
// `max_length` characters. CQL folds such a name to lower case, and so does
// this: the result is the folded name, which the sinks write double-quoted so a
// name that is also a CQL keyword still names the column. Returns nullopt, with
// the reason in *why when given, for any other name (one needing quotes, such
// as a mixed-case name created quoted, is not supported).
[[nodiscard]] std::optional<std::string> cql_identifier(std::string_view name,
                                                        std::size_t max_length,
                                                        std::string* why = nullptr);

// The CQL types a primary-key column may have for the upsert sink. A key of any
// other type (a frozen collection, tuple or user-defined type, a custom type)
// is refused when the sink opens.
enum class CqlKeyType : std::uint8_t {
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Varint,
    Decimal,
    Float,
    Double,
    Text,  // text and varchar
    Ascii,
    Boolean,
    Uuid,
    TimeUuid,
    Timestamp,
    Date,
    Time,
    Inet,
    Blob,
};

// The key type a schema type name denotes, or nullopt when the sink does not
// accept it as a key.
[[nodiscard]] std::optional<CqlKeyType> cql_key_type(std::string_view schema_type);
[[nodiscard]] std::string_view cql_key_type_name(CqlKeyType t) noexcept;

struct CassandraKeyColumn {
    std::string name;  // folded
    CqlKeyType type{CqlKeyType::Text};
};

class CassandraRecordError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// One changelog record, checked and ready to write.
struct CassandraUpsertRecord {
    bool is_delete{false};
    // Insert: the record as an INSERT ... JSON statement takes it. With no
    // __row_kind member it is the record's text unchanged; otherwise the
    // record's other members, each name and value exactly as written.
    std::string insert_json;
    // Each key column's value as written in the record, in key order. A delete
    // binds these to `<column> = fromJson(?)`, so the server reads a key the
    // same way for a delete as for an insert.
    std::vector<std::string> key_json;
    // The record's key: the key values' JSON text, length-prefixed. Two records
    // with equal identities name the same row.
    std::string identity;
};

// Checks one record for the upsert sink against the table's key and prepares
// it. The record is read once with clink's JSON reader to check it, which
// refuses an integer of 2^64 or more and a number past a double's range; what
// is written is the record's own text, so no number is rounded on the way out.
// The record must be one JSON object with no two member names equal when
// folded to lower case (CQL matches a JSON field to a column that way). Its
// __row_kind, when present, must be the string "insert", "update_after",
// "delete" or "update_before"; a record without one is an insert. Every key
// column must be present, not null, and a value of its type:
//   tinyint, smallint, int, bigint  a JSON integer within the type's range
//   varint                          a JSON integer below 2^64 in magnitude
//   decimal, float, double          a JSON number
//   text, varchar                   a JSON string
//   ascii                           a JSON string of ASCII characters
//   boolean                         true or false
//   uuid, timeuuid                  a JSON string in the 8-4-4-4-12 hex form; a
//                                   timeuuid's version digit must be 1
//   timestamp, date, time           a JSON string or integer, as CQL's fromJson
//                                   reads them
//   inet                            a JSON string
//   blob                            a JSON string "0x" followed by hex digit pairs
// Throws CassandraRecordError naming the rule the record breaks and the column,
// never the record's values.
[[nodiscard]] CassandraUpsertRecord prepare_cassandra_upsert_record(
    std::string_view record, const std::vector<CassandraKeyColumn>& key);

// A server or driver message as the sinks repeat it: one line, at most 300
// bytes, with every quoted span emptied ('...' and "...") and anything from a
// JSON parser's "[Source:" excerpt on cut, so a record's values do not reach a
// log or a failure report through it.
[[nodiscard]] std::string redact_cassandra_message(std::string_view message);

}  // namespace clink::cassandra
