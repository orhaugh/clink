#pragma once

// Metric names for the Relate sink. Its own series go to
// RuntimeContext::metrics(), tagged with its op_id; the cross-connector series
// (clink::metrics::connector, MetricsRegistry::global()) carry the record name
// as their connector label.

#include <array>

namespace clink::relate::metric {

inline constexpr char kConnector[] = "relate";

// Batches and rows in transactions Relate committed.
inline constexpr char kBatchesCommittedTotal[] = "clink_relate_batches_committed_total";
inline constexpr char kRowsCommittedTotal[] = "clink_relate_rows_committed_total";
// The committing attempt, from its begin to its commit.
inline constexpr char kCommitLatencyNs[] = "clink_relate_commit_latency_ns";
// Attempts that did not commit, by whether Relate said a retry can succeed
// (retryable: true or false).
inline constexpr char kAttemptsFailedTotal[] = "clink_relate_attempts_failed_total";
inline constexpr char kRetriesTotal[] = "clink_relate_retries_total";
inline constexpr char kBufferedRows[] = "clink_relate_buffered_rows";
inline constexpr char kBufferedBytes[] = "clink_relate_buffered_bytes";

inline constexpr std::array<double, 12> kNsBounds = {
    1e4, 1e5, 1e6, 5e6, 1e7, 5e7, 1e8, 2.5e8, 5e8, 1e9, 5e9, 3e10};

}  // namespace clink::relate::metric
