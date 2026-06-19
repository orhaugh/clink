#pragma once

// Per-operator data-plane metrics.
//
// Coverage:
//   - records_in_total       (counter)  : elements popped from the input
//                                          channel by the runner before
//                                          process() is invoked.
//   - records_out_total      (counter)  : elements pushed downstream by
//                                          the operator's Emitter.
//   - records_dropped_total  (counter)  : elements consumed but not
//                                          emitted (filter rejections,
//                                          window evictions, etc.).
//   - side_output_records_total (counter): elements pushed to any of an
//                                           operator's side outputs.
//   - window_panes_fired_total (counter): tumbling / sliding / session
//                                          pane closes that produced a
//                                          downstream emit.
//   - join_matches_total     (counter)  : interval-join / window-join
//                                          successful (left, right) pair
//                                          emits.
//   - async_lookup_hits_total / misses_total : per-record async-lookup
//                                              join outcomes.
//   - retractions_emitted_total (counter): delete / update-before
//                                          row-kind emits, useful for
//                                          retract-stream observability.
//   - process_latency_ns_sum / count     : aggregated process-call wall
//                                          time (gauge + counter pair
//                                          because the registry has no
//                                          histogram type yet; mean
//                                          = sum / count).
//
// Each metric is keyed by the operator's runtime OperatorId via the
// `for_op(operator_id_value)` family of accessors so multiple operator
// instances in the same process don't collide. The string form is
// `clink_op_<metric>_total{op_id=<n>}` once the registry grows tag
// support; in the meantime the operator id is appended to the metric
// name.

#include <cstddef>
#include <cstdint>
#include <string>

#include "clink/metrics/counter.hpp"
#include "clink/metrics/gauge.hpp"
#include "clink/metrics/metrics_registry.hpp"

namespace clink::metrics {

// Prefix shared by every per-operator metric. Tools / dashboards can
// match on this to find every operator-scoped metric in a snapshot.
inline constexpr const char* kOpMetricPrefix = "clink_op_";

inline std::string op_metric_name(const char* metric, std::uint64_t op_id) {
    std::string out = kOpMetricPrefix;
    out += metric;
    out += "{op_id=\"";
    out += std::to_string(op_id);
    out += "\"}";
    return out;
}

// Per-shard variant: clink_op_<metric>{op_id="N",shard="i"}. Used by the
// share-nothing sharded keyed stage so a single keyed operator's per-shard
// throughput is observable (shard skew shows as uneven counters).
inline std::string op_shard_metric_name(const char* metric,
                                        std::uint64_t op_id,
                                        std::size_t shard) {
    std::string out = kOpMetricPrefix;
    out += metric;
    out += "{op_id=\"";
    out += std::to_string(op_id);
    out += "\",shard=\"";
    out += std::to_string(shard);
    out += "\"}";
    return out;
}

namespace op {

inline void records_in_inc(std::uint64_t op_id, std::uint64_t n = 1) {
    MetricsRegistry::global().counter(op_metric_name("records_in_total", op_id)).increment(n);
}
// Per-shard records-in for the sharded keyed stage: each shard worker counts
// the records it processed, so shard skew is observable per (op_id, shard).
inline void shard_records_in_inc(std::uint64_t op_id, std::size_t shard, std::uint64_t n = 1) {
    MetricsRegistry::global()
        .counter(op_shard_metric_name("shard_records_in_total", op_id, shard))
        .increment(n);
}
inline void records_out_inc(std::uint64_t op_id, std::uint64_t n = 1) {
    MetricsRegistry::global().counter(op_metric_name("records_out_total", op_id)).increment(n);
}
inline void records_dropped_inc(std::uint64_t op_id, std::uint64_t n = 1) {
    MetricsRegistry::global().counter(op_metric_name("records_dropped_total", op_id)).increment(n);
}
inline void side_output_records_inc(std::uint64_t op_id, std::uint64_t n = 1) {
    MetricsRegistry::global()
        .counter(op_metric_name("side_output_records_total", op_id))
        .increment(n);
}
inline void window_panes_fired_inc(std::uint64_t op_id, std::uint64_t n = 1) {
    MetricsRegistry::global()
        .counter(op_metric_name("window_panes_fired_total", op_id))
        .increment(n);
}
inline void join_matches_inc(std::uint64_t op_id, std::uint64_t n = 1) {
    MetricsRegistry::global().counter(op_metric_name("join_matches_total", op_id)).increment(n);
}
inline void async_lookup_hit_inc(std::uint64_t op_id, std::uint64_t n = 1) {
    MetricsRegistry::global()
        .counter(op_metric_name("async_lookup_hits_total", op_id))
        .increment(n);
}
inline void async_lookup_miss_inc(std::uint64_t op_id, std::uint64_t n = 1) {
    MetricsRegistry::global()
        .counter(op_metric_name("async_lookup_misses_total", op_id))
        .increment(n);
}
inline void retractions_emitted_inc(std::uint64_t op_id, std::uint64_t n = 1) {
    MetricsRegistry::global()
        .counter(op_metric_name("retractions_emitted_total", op_id))
        .increment(n);
}

// Aggregated process() latency as a histogram (OBS-1b). The exposition keeps
// the historical process_latency_ns_sum / _count line names (a Prometheus
// histogram emits both) and adds _bucket lines, so the moving mean still works
// and p50/p95/p99 are now available.
inline void process_latency_observe(std::uint64_t op_id, std::uint64_t nanos) {
    MetricsRegistry::global()
        .histogram(op_metric_name("process_latency_ns", op_id))
        .observe(static_cast<double>(nanos));
}

}  // namespace op

}  // namespace clink::metrics
