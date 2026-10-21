#pragma once

// Metric names for the Neo4j sink and lookup. The sink's own series go to
// RuntimeContext::metrics(), tagged with its op_id; the cross-connector
// series (clink::metrics::connector, MetricsRegistry::global()) carry the
// record name as their connector label.

#include <array>

namespace clink::neo4j::metric {

inline constexpr char kConnector[] = "neo4j";

// Rows and request bytes in requests the server received whole, retries
// included: what was sent, not what was committed.
inline constexpr char kRowsSubmittedTotal[] = "clink_neo4j_rows_submitted_total";
inline constexpr char kBytesSubmittedTotal[] = "clink_neo4j_bytes_submitted_total";
// Batches and rows the server reported committed.
inline constexpr char kBatchesCommittedTotal[] = "clink_neo4j_batches_committed_total";
inline constexpr char kRowsCommittedTotal[] = "clink_neo4j_rows_committed_total";
// The committing attempt's request, from send to the committed reply.
inline constexpr char kCommitLatencyNs[] = "clink_neo4j_commit_latency_ns";
// Attempts that did not commit, by verdict (rolled_back, refused, not_sent,
// unknown, tls_refused).
inline constexpr char kAttemptsFailedTotal[] = "clink_neo4j_attempts_failed_total";
inline constexpr char kRetriesTotal[] = "clink_neo4j_retries_total";
// Attempts whose outcome was lost after the server received them: each may
// have committed, so its retry may apply the batch a second time.
inline constexpr char kOutcomeUnknownTotal[] = "clink_neo4j_outcome_unknown_total";
inline constexpr char kBufferedRows[] = "clink_neo4j_buffered_rows";
inline constexpr char kBufferedBytes[] = "clink_neo4j_buffered_bytes";

// The lookup's series, tagged with its op_id. A lookup is one probe row's
// read: its latency runs from the moment it was queued to its result,
// retries included; a request is one HTTP attempt.
inline constexpr char kLookupRequestsTotal[] = "clink_neo4j_lookup_requests_total";
inline constexpr char kLookupRetriesTotal[] = "clink_neo4j_lookup_retries_total";
inline constexpr char kLookupMatchesTotal[] = "clink_neo4j_lookup_matches_total";
inline constexpr char kLookupNoMatchTotal[] = "clink_neo4j_lookup_no_match_total";
// Probe rows whose key was NULL: a no-match decided without a request.
inline constexpr char kLookupNullKeysTotal[] = "clink_neo4j_lookup_null_keys_total";
// Lookups that failed, by error code; each one fails the task.
inline constexpr char kLookupFailuresTotal[] = "clink_neo4j_lookup_failures_total";
inline constexpr char kLookupLatencyNs[] = "clink_neo4j_lookup_latency_ns";
inline constexpr char kLookupRequestLatencyNs[] = "clink_neo4j_lookup_request_latency_ns";
// Lookups started and not yet finished, and those of them still waiting for a
// request thread.
inline constexpr char kLookupInFlight[] = "clink_neo4j_lookup_in_flight";
inline constexpr char kLookupQueued[] = "clink_neo4j_lookup_queued";

inline constexpr std::array<double, 14> kNsBounds = {
    1e5, 1e6, 1e7, 5e7, 1e8, 2.5e8, 5e8, 1e9, 2.5e9, 5e9, 1e10, 3e10, 6e10, 1.2e11};

}  // namespace clink::neo4j::metric
