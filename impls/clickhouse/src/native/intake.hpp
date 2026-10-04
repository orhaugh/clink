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

// How a declared column may take its sidecar array without converting it
// cell by cell. Each yields the chunk the per-cell path builds from the same
// array, and is taken only for an array whose buffers are all owned pool
// allocations (owns_its_buffers); any other array goes cell by cell.
enum class IntakeReuse : std::uint8_t {
    // Cell by cell.
    None,
    // The array itself: int64, int32, float, double and bool into BIGINT,
    // INTEGER, REAL, DOUBLE and BOOLEAN.
    Same,
    // The array's buffers under the declared column's type: int64 into
    // TIMESTAMP(p) [WITH TIME ZONE], int32 into DATE.
    Retype,
};

// Where each declared column's cells come from in one sidecar schema.
struct IntakePlan {
    // One entry per declared column, in declared order: the index of the
    // sidecar column carrying it, or -1 when the sidecar has none, which
    // makes every cell of the column NULL, as an absent Row value is.
    std::vector<int> source;
    // One entry per declared column, or empty for no reuse at all.
    std::vector<IntakeReuse> reuse;
};

using IntakeResult = std::variant<IntakePlan, IntakeDecline>;

// Resolves the declared columns against a sidecar schema by the
// self-describing reader's rules: value columns start at index 1,
// `__source_partition` is skipped, and every other name, `__key` and
// `__row_kind` included, is an ordinary column name. A plan depends only on
// the schema, so a caller may keep it for every batch whose schema is equal.
[[nodiscard]] IntakeResult compile_intake(const arrow::Schema& schema,
                                          const std::vector<SqlColumn>& columns);

// True when every buffer of `data`, its children's and its dictionary's is an
// owned pool allocation: no parent() and is_mutable(). Such an array's
// buffers hold only themselves, so the chunk that takes them is charged
// (chunk_bytes, arrow::util::TotalBufferSize) for at least what it keeps
// alive. A slice of an IPC frame has a parent, which it pins; a buffer
// imported through the C Data Interface has none but keeps the whole exported
// structure alive, and is immutable. Neither is reused.
[[nodiscard]] bool owns_its_buffers(const arrow::ArrayData& data);

}  // namespace clink::clickhouse::native
