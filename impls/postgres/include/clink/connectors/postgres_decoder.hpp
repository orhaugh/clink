#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "clink/connectors/cdc_event.hpp"

namespace clink::pg {

// Big/little-endian helpers - Postgres's protocol is big-endian on the wire.
std::uint16_t read_be16(const char* p) noexcept;
std::uint32_t read_be32(const char* p) noexcept;
std::uint64_t read_be64(const char* p) noexcept;
void write_be64(char* p, std::uint64_t v) noexcept;

// Read a NUL-terminated cstring from the cursor, advancing it past the NUL.
// On unterminated input the cursor is consumed and an empty string returned.
std::string read_cstring(std::string_view& cursor);

// Format a 64-bit LSN as Postgres's "%X/%X" textual form.
std::string format_lsn(std::uint64_t lsn);

// Returns true iff `s` begins with `prefix`.
bool starts_with(std::string_view s, std::string_view prefix) noexcept;

// Built-in Postgres type-OID lookup. Returns nullptr for unknown OIDs.
// Intentionally not exhaustive (no JSON arrays, no domain types) - extend
// as needed.
const char* lookup_builtin_type_name(std::uint32_t oid) noexcept;

// Postgres-epoch (2000-01-01 00:00:00 UTC) microseconds since 1970.
inline constexpr std::int64_t kPgEpochOffsetUs = 946684800LL * 1'000'000LL;

// Microseconds-since-Postgres-epoch from the system clock right now.
std::int64_t postgres_epoch_us_now() noexcept;

// =====================================================================
// test_decoding plugin parsers - line-oriented text protocol.
// =====================================================================

// Parse the trailing xid from a "BEGIN N" / "COMMIT N" payload. Returns 0
// if the payload is malformed.
std::int64_t parse_xid(std::string_view payload);

// Parse a sequence of `name[type]:value` field tokens. Quoted strings use
// SQL escaping (doubled single quote = literal single quote). The literal
// token `null` (unquoted) becomes `is_null = true`.
std::vector<CdcField> parse_test_decoding_fields(std::string_view rest);

// Parse one full test_decoding line into a CdcEvent. Recognised forms:
//
//   "BEGIN N"                                   -> Op::Begin
//   "COMMIT N"                                  -> Op::Commit
//   "table SCHEMA.NAME: OP: FIELDS"             -> Op::{Insert,Update,Delete}
//
// Any other line lands as Op::Unknown with the raw payload preserved in
// values[0] (name = "raw", value = original line).
CdcEvent parse_test_decoding(std::string_view payload, std::string lsn_text);

}  // namespace clink::pg
