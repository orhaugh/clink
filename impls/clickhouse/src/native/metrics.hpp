#pragma once

// Metric names for the native ClickHouse sink. The cross-connector series
// (clink::metrics::connector, MetricsRegistry::global()) carry the record name
// as their connector label, so the legacy and native sinks stay separate. The
// sink's own series go to RuntimeContext::metrics(), tagged with the sink's
// op_id.

#include <array>

namespace clink::clickhouse::native::metric {

inline constexpr char kConnector[] = "clickhouse_native";

inline constexpr char kInsertsTotal[] = "clink_clickhouse_inserts_total";  // outcome
inline constexpr char kRowsTotal[] = "clink_clickhouse_rows_total";
inline constexpr char kInsertLatencyNs[] = "clink_clickhouse_insert_latency_ns";
inline constexpr char kBlockRows[] = "clink_clickhouse_block_rows";
inline constexpr char kRetriesTotal[] = "clink_clickhouse_retries_total";  // class
inline constexpr char kInDoubtTotal[] = "clink_clickhouse_in_doubt_total";
inline constexpr char kRetryWaitNs[] = "clink_clickhouse_retry_wait_ns";
inline constexpr char kQueueBytes[] = "clink_clickhouse_queue_bytes";
inline constexpr char kBackpressureBlockedNs[] = "clink_clickhouse_backpressure_blocked_ns";
inline constexpr char kBarrierFlushNs[] = "clink_clickhouse_barrier_flush_ns";
inline constexpr char kPartsBackoffTotal[] = "clink_clickhouse_parts_backoff_total";
inline constexpr char kRefusalsTotal[] = "clink_clickhouse_refusals_total";  // reason
inline constexpr char kReconnectsTotal[] = "clink_clickhouse_reconnects_total";
inline constexpr char kRowsMaybeDuplicatedTotal[] = "clink_clickhouse_rows_maybe_duplicated_total";

// Values of the `outcome` tag on kInsertsTotal.
inline constexpr char kOutcomeOk[] = "ok";
inline constexpr char kOutcomeRetriedOk[] = "retried_ok";
inline constexpr char kOutcomeFailed[] = "failed";
inline constexpr char kOutcomeAbandoned[] = "abandoned";

// Bounds, fixed at a histogram's first creation.
inline constexpr std::array<double, 14> kNsBounds = {
    1e5, 1e6, 1e7, 5e7, 1e8, 2.5e8, 5e8, 1e9, 2.5e9, 5e9, 1e10, 3e10, 6e10, 1.2e11};
inline constexpr std::array<double, 10> kBlockRowsBounds = {
    1, 10, 100, 1e3, 1e4, 1e5, 2.5e5, 5e5, 1e6, 2e6};

}  // namespace clink::clickhouse::native::metric
