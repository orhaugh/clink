#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace clink {

// One column's payload in a row-level CDC event. The field carries enough
// metadata to round-trip through downstream operators with type fidelity:
//
//   * `name` - the column name as published by the source.
//   * `value` - the text representation of the value. Empty string for SQL
//     NULL (distinguish via `is_null`) and for unchanged-TOAST columns
//     (where the source omitted the value because it wasn't modified).
//   * `type` - the Postgres type name, e.g. "int4", "text", "numeric".
//     Empty when the source can't supply one (test_decoding output, or
//     pgoutput before a Relation/Type message has been seen).
//   * `is_null` - true iff the underlying value is SQL NULL. Distinguishes
//     a real NULL from an unchanged-TOAST sentinel (both have empty
//     `value`).
struct CdcField {
    std::string name;
    std::string value;
    std::string type;
    bool is_null{false};
};

// CdcEvent is the unit emitted by a CDC source. One event per logical
// decoding message: a transaction boundary, a row-level change, or a
// table-level operation like TRUNCATE.
struct CdcEvent {
    enum class Op : std::uint8_t {
        Begin,
        Insert,
        Update,
        Delete,
        Truncate,
        Commit,
        // The plugin output couldn't be parsed; the raw payload lives in
        // `values[0].value` so a user-facing error path can surface it.
        Unknown,
    };

    Op op{Op::Unknown};
    std::string table;    // schema-qualified, e.g. "public.users". Empty for Begin/Commit.
    std::string lsn;      // WAL position as text, e.g. "0/16E2A38". Empty if unknown.
    std::int64_t xid{0};  // transaction id; 0 if not present in the message.

    using Field = CdcField;
    std::vector<Field> values;
};

}  // namespace clink
