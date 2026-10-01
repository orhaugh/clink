#pragma once

// Named CLINK_FAULT_POINT sites for the native sink's kill matrix. They compile
// to nothing without CLINK_FAULT_INJECTION.

namespace clink::clickhouse::native::points {

// Mid-INSERT: the first block has been sent, EndInsert has not.
inline constexpr char kAfterFirstBlock[] = "clickhouse.after_first_block";
// The INSERT is acknowledged, its flush ticket not yet completed.
inline constexpr char kAfterEndInsert[] = "clickhouse.after_end_insert";
// Inside a retry, before the backoff wait.
inline constexpr char kBeforeRetryWait[] = "clickhouse.before_retry_wait";

}  // namespace clink::clickhouse::native::points
