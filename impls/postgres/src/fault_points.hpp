#pragma once

// Named CLINK_FAULT_POINT sites for the PostgreSQL sinks' recovery tests. They
// compile to nothing without CLINK_FAULT_INJECTION, and a reached point prints
// the fault framework's witness line, so a test can prove the fault landed
// where it meant. (The change stream's points are in cdc_fault_points.hpp.)

namespace clink::postgres::points {

// A flush's statements have all succeeded inside its transaction, and COMMIT
// has not been sent: a kill here leaves nothing of the flush in the table.
inline constexpr char kSinkBeforeCommit[] = "postgres.sink_before_commit";
// A checkpoint barrier's flush has committed and the barrier has not been
// passed on, so the checkpoint it belongs to has not completed.
inline constexpr char kSinkAfterCommit[] = "postgres.sink_after_commit";

}  // namespace clink::postgres::points
