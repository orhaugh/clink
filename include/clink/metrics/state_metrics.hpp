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

// Keyed entries a restore DISCARDED because their key group falls outside
// the restoring subtask's assigned range.
//
// Legitimate during a rescale, where a subtask reads a parent's snapshot and
// keeps only its own share. Outside a rescale it is silent state loss: the
// entry was written by a subtask that did not own its key group, so nothing
// will ever restore it, and the job comes back with a hole in its state and
// no error. That is the shape of F38 - half a job's keyed state gone, job
// reports success.
//
// A counter rather than a log line alone, so the condition is alertable:
// non-zero outside a rescale window means state was dropped.
inline void restore_keys_dropped(const std::string& backend, std::uint64_t n) {
    MetricsRegistry::global()
        .counter(state_metric_name("restore_keys_dropped_total", backend))
        .increment(n);
}

inline void restore_failed(const std::string& backend) {
    MetricsRegistry::global()
        .counter(state_metric_name("restore_failures_total", backend))
        .increment();
}

inline void keyed_keys_set(const std::string& backend, std::int64_t n) {
    MetricsRegistry::global().gauge(state_metric_name("keyed_keys", backend)).set(n);
}

// Per-JOB state size, as reported by one worker: the sum of the last snapshot
// sizes of every backend that worker hosts for the job.
//
// Tagged by job because capacity planning is a per-job question - "which job is
// growing" is not answerable from a process-wide total. Summing across workers is
// the scrape's job (`sum by (job_id)`), which is why this is per worker per job
// rather than a single number the coordinator aggregates: it avoids putting state
// sizes on the heartbeat and it is how a Prometheus deployment would total it
// anyway.
//
// SERIALISED size at the last checkpoint, not live heap residency, and stale
// between checkpoints. A job with checkpointing off never reports one. The
// alternative - an exact live figure - costs either a counter on the put/erase
// hot path or a full scan per sample, for a number whose consumer is a capacity
// trend.
inline void job_state_bytes_set(std::uint64_t job_id, std::int64_t bytes) {
    std::string name = kStateMetricPrefix;
    name += "job_bytes{job_id=\"";
    name += std::to_string(job_id);
    name += "\"}";
    MetricsRegistry::global().gauge(name).set(bytes);
}

// How many of the job's backends on this worker could report a size. A gauge
// that silently covered 2 of 8 subtasks would read as a small job rather than an
// unmeasured one, so the count is exported beside the bytes and a scrape can
// compare it with the subtask count.
inline void job_state_reporting_backends_set(std::uint64_t job_id, std::int64_t n) {
    std::string name = kStateMetricPrefix;
    name += "job_reporting_backends{job_id=\"";
    name += std::to_string(job_id);
    name += "\"}";
    MetricsRegistry::global().gauge(name).set(n);
}

// --- retention (state_ttl) ---------------------------------------------------
//
// A job that declares a `state_ttl` has asked the engine for a bound. These
// two say whether it is holding: how many keys are currently under
// retention, and how many have been released so far. Without them an
// operator cannot tell a TTL that is keeping up from one that has silently
// stopped evicting - the state gauges above answer "how big", never "is
// retention working", and on a disk-backed or disaggregated backend they do
// not answer at all.
//
// Tagged by operator id, because retention is a per-operator policy and a
// process-wide total cannot say which construct is the one still growing.
// Reported from the eviction sweep, so a job whose watermark has stalled
// stops updating them - which is itself the signal, and the reason
// `tracked` is a gauge rather than something derived from the counter.
inline std::string ttl_metric_name(const char* metric, std::uint64_t op_id) {
    std::string out = kStateMetricPrefix;
    out += "ttl_";
    out += metric;
    out += "{op_id=\"";
    out += std::to_string(op_id);
    out += "\"}";
    return out;
}

// Keys currently carrying a deadline: the live population retention is
// responsible for. Flat over time is the plateau a TTL exists to produce.
inline void ttl_tracked_keys_set(std::uint64_t op_id, std::int64_t n) {
    MetricsRegistry::global().gauge(ttl_metric_name("tracked_keys", op_id)).set(n);
}

// Keys released by retention over the operator's life. Zero on a job that
// declared a TTL means nothing has ever expired, which is the difference
// between a bound that holds and a bound that was only accepted.
inline void ttl_expired_total_set(std::uint64_t op_id, std::int64_t n) {
    MetricsRegistry::global().gauge(ttl_metric_name("expired_total", op_id)).set(n);
}

}  // namespace state

}  // namespace clink::metrics
