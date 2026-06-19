#pragma once

// Async primitive observability.
//
// Coverage:
//   - clink_circuit_breaker_state_transitions_total{from,to}
//   - clink_circuit_breaker_trips_total       (transitions to OPEN)
//   - clink_circuit_breaker_calls_allowed_total
//   - clink_circuit_breaker_calls_rejected_total
//   - clink_retry_attempts_total              (total retry firings,
//                                              i.e. attempts after the
//                                              first)
//   - clink_retry_success_total               (call succeeded after
//                                              one or more retries)
//   - clink_retry_giveup_total                (call still failing
//                                              after max_attempts)
//   - clink_io_uring_completions_total
//   - clink_io_uring_completion_latency_ns_sum / count
//   - clink_http_pool_connections (gauge of in-use connections)
//   - clink_http_pool_idle_connections (gauge)
//   - clink_http_pool_acquire_blocks_total

#include <cstdint>
#include <string>

#include "clink/metrics/metrics_registry.hpp"

namespace clink::metrics {

inline std::string circuit_state_transition_name(const char* from, const char* to) {
    std::string out = "clink_circuit_breaker_state_transitions_total{from=\"";
    out += from;
    out += "\",to=\"";
    out += to;
    out += "\"}";
    return out;
}

inline constexpr const char* kCbTrips = "clink_circuit_breaker_trips_total";
inline constexpr const char* kCbAllowed = "clink_circuit_breaker_calls_allowed_total";
inline constexpr const char* kCbRejected = "clink_circuit_breaker_calls_rejected_total";
inline constexpr const char* kRetryAttempts = "clink_retry_attempts_total";
inline constexpr const char* kRetrySuccess = "clink_retry_success_total";
inline constexpr const char* kRetryGiveup = "clink_retry_giveup_total";
inline constexpr const char* kIoUringCompletions = "clink_io_uring_completions_total";
// Completion-latency histogram base (OBS-1b): exposes
// clink_io_uring_completion_latency_ns_{bucket,sum,count}.
inline constexpr const char* kIoUringLatNs = "clink_io_uring_completion_latency_ns";
inline constexpr const char* kHttpPoolConnections = "clink_http_pool_connections";
inline constexpr const char* kHttpPoolIdleConnections = "clink_http_pool_idle_connections";
inline constexpr const char* kHttpPoolAcquireBlocks = "clink_http_pool_acquire_blocks_total";

namespace async {

inline void circuit_transition(const char* from, const char* to) {
    MetricsRegistry::global().counter(circuit_state_transition_name(from, to)).increment();
}
inline void circuit_tripped() {
    MetricsRegistry::global().counter(kCbTrips).increment();
}
inline void circuit_call_allowed() {
    MetricsRegistry::global().counter(kCbAllowed).increment();
}
inline void circuit_call_rejected() {
    MetricsRegistry::global().counter(kCbRejected).increment();
}
inline void retry_attempted() {
    MetricsRegistry::global().counter(kRetryAttempts).increment();
}
inline void retry_succeeded() {
    MetricsRegistry::global().counter(kRetrySuccess).increment();
}
inline void retry_gave_up() {
    MetricsRegistry::global().counter(kRetryGiveup).increment();
}
inline void io_uring_completion(std::uint64_t latency_ns) {
    MetricsRegistry::global().counter(kIoUringCompletions).increment();
    MetricsRegistry::global().histogram(kIoUringLatNs).observe(static_cast<double>(latency_ns));
}
inline void http_pool_in_use_set(std::int64_t n) {
    MetricsRegistry::global().gauge(kHttpPoolConnections).set(n);
}
inline void http_pool_idle_set(std::int64_t n) {
    MetricsRegistry::global().gauge(kHttpPoolIdleConnections).set(n);
}
inline void http_pool_acquire_blocked() {
    MetricsRegistry::global().counter(kHttpPoolAcquireBlocks).increment();
}

}  // namespace async

}  // namespace clink::metrics
