#pragma once

// State-backend observability.
//
// Coverage:
//   - state_snapshot_total            : count of successful snapshots
//   - state_snapshot_failures_total   : snapshot exceptions / errors
//   - state_snapshot_bytes_sum        : cumulative bytes written
//                                       across snapshots (rate gives
//                                       throughput, sum gives volume)
//   - state_snapshot_duration_ns_sum / count : aggregated snapshot
//                                              wall time
//   - state_restore_total             : count of successful restores
//   - state_restore_failures_total
//   - state_restore_duration_ns_sum / count
//   - state_keyed_keys                : gauge of (approx) live key
//                                       count per backend instance,
//                                       sampled at snapshot time
//
// Metric names use the `clink_state_` prefix and carry the backend
// type as a tag-shaped suffix (e.g.
// `clink_state_snapshot_total{backend="rocksdb"}`) so multi-backend
// processes can disambiguate. The clink registry has no tag support
// yet, so we encode the label inline in the metric name; the suffix
// is a stable string the backend supplies.

#include <cstdint>
#include <string>

#include "clink/metrics/metrics_registry.hpp"

namespace clink::metrics {

inline constexpr const char* kStateMetricPrefix = "clink_state_";

inline std::string state_metric_name(const char* metric, const std::string& backend) {
    std::string out = kStateMetricPrefix;
    out += metric;
    out += "{backend=\"";
    out += backend;
    out += "\"}";
    return out;
}

namespace state {

inline void snapshot_completed(const std::string& backend,
                               std::uint64_t bytes,
                               std::uint64_t duration_ns) {
    MetricsRegistry::global().counter(state_metric_name("snapshot_total", backend)).increment();
    MetricsRegistry::global()
        .counter(state_metric_name("snapshot_bytes_sum", backend))
        .increment(bytes);
    // Duration histogram (OBS-1b): keeps snapshot_duration_ns_sum / _count, adds _bucket.
    MetricsRegistry::global()
        .histogram(state_metric_name("snapshot_duration_ns", backend))
        .observe(static_cast<double>(duration_ns));
}

inline void snapshot_failed(const std::string& backend) {
    MetricsRegistry::global()
        .counter(state_metric_name("snapshot_failures_total", backend))
        .increment();
}

inline void restore_completed(const std::string& backend, std::uint64_t duration_ns) {
    MetricsRegistry::global().counter(state_metric_name("restore_total", backend)).increment();
    // Duration histogram (OBS-1b): keeps restore_duration_ns_sum / _count, adds _bucket.
    MetricsRegistry::global()
        .histogram(state_metric_name("restore_duration_ns", backend))
        .observe(static_cast<double>(duration_ns));
}

inline void restore_failed(const std::string& backend) {
    MetricsRegistry::global()
        .counter(state_metric_name("restore_failures_total", backend))
        .increment();
}

inline void keyed_keys_set(const std::string& backend, std::int64_t n) {
    MetricsRegistry::global().gauge(state_metric_name("keyed_keys", backend)).set(n);
}

}  // namespace state

}  // namespace clink::metrics
