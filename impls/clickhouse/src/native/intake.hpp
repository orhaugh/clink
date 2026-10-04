#pragma once

#include <cstdint>
#include <variant>
#include <vector>

#include <arrow/api.h>

#include "native/types.hpp"

namespace clink::clickhouse::native {

// Why the sink takes a columnar batch through its row accessors instead of
// reading its sidecar. Each is a sidecar the self-describing reader
// (clink::sql::rows_from_record_batch) would not read as the intake does.
enum class IntakeDecline : std::uint8_t {
    // Column 0 is not an int64 event-time column.
    EventTime,
    // A value column has a type the Row sidecar does not carry, so the reader
    // would materialise no rows at all.
    UnsupportedType,
    // Two value columns carry one declared column's name. The reader keeps
    // the last of them, which no rule here should have to copy.
    DuplicateName,
};

// The value of the `reason` tag: event_time, unsupported_type, duplicate_name.
[[nodiscard]] const char* to_string(IntakeDecline reason) noexcept;

// Where each declared column's cells come from in one sidecar schema.
struct IntakePlan {
    // One entry per declared column, in declared order: the index of the
    // sidecar column carrying it, or -1 when the sidecar has none, which
    // makes every cell of the column NULL, as an absent Row value is.
    std::vector<int> source;
};

using IntakeResult = std::variant<IntakePlan, IntakeDecline>;

// Resolves the declared columns against a sidecar schema by the
// self-describing reader's rules: value columns start at index 1,
// `__source_partition` is skipped, and every other name, `__key` and
// `__row_kind` included, is an ordinary column name. A plan depends only on
// the schema, so a caller may keep it for every batch whose schema is equal.
[[nodiscard]] IntakeResult compile_intake(const arrow::Schema& schema,
                                          const std::vector<SqlColumn>& columns);

}  // namespace clink::clickhouse::native
