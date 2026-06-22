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

// Named user accumulator: clink_op_acc{op_id="N",name="<name>"}. A gauge (not a
// counter) so it supports +/- deltas, and because a gauge is a single atomic
// per (op_id,name) it merges every subtask of the operator that runs in the
// SAME process automatically; the JM aggregator sums it across TMs for the
// operator-wide value. This is how clink surfaces Flink-style accumulators -
// the host metrics registry is already threaded into every RuntimeContext and
// scraped by the JM, so it is the natural cross-subtask transport (a separate
// per-subtask wire message would just duplicate it).
inline std::string op_acc_metric_name(std::uint64_t op_id, const std::string& acc_name) {
    std::string out = kOpMetricPrefix;
    out += "acc{op_id=\"";
    out += std::to_string(op_id);
    out += "\",name=\"";
    out += acc_name;
    out += "\"}";
    return out;
}

namespace op {

// IMPORTANT: every accessor takes the MetricsRegistry to write into as its
// first argument and no-ops on nullptr. This is load-bearing on the cluster
// path: operator code runs inside a dlopen'd job .so (RTLD_LOCAL + static
// clink_core), so MetricsRegistry::global() there resolves the .so's PRIVATE
// singleton, NOT the host registry the node's /metrics endpoint reads. Callers
// must pass the HOST registry, reached via RuntimeContext::metrics() (threaded
// as JobConfig::metrics across the plugin boundary by data). See the same
// pattern for the host logger in runtime_context.hpp.

inline void records_in_inc(MetricsRegistry* reg, std::uint64_t op_id, std::uint64_t n = 1) {
    if (reg == nullptr)
        return;
    reg->counter(op_metric_name("records_in_total", op_id)).increment(n);
}
// Per-shard records-in for the sharded keyed stage: each shard worker counts
// the records it processed, so shard skew is observable per (op_id, shard).
inline void shard_records_in_inc(MetricsRegistry* reg,
                                 std::uint64_t op_id,
                                 std::size_t shard,
                                 std::uint64_t n = 1) {
    if (reg == nullptr)
        return;
    reg->counter(op_shard_metric_name("shard_records_in_total", op_id, shard)).increment(n);
}
inline void records_out_inc(MetricsRegistry* reg, std::uint64_t op_id, std::uint64_t n = 1) {
    if (reg == nullptr)
        return;
    reg->counter(op_metric_name("records_out_total", op_id)).increment(n);
}
inline void records_dropped_inc(MetricsRegistry* reg, std::uint64_t op_id, std::uint64_t n = 1) {
    if (reg == nullptr)
        return;
    reg->counter(op_metric_name("records_dropped_total", op_id)).increment(n);
}
inline void side_output_records_inc(MetricsRegistry* reg,
                                    std::uint64_t op_id,
                                    std::uint64_t n = 1) {
    if (reg == nullptr)
        return;
    reg->counter(op_metric_name("side_output_records_total", op_id)).increment(n);
}
inline void window_panes_fired_inc(MetricsRegistry* reg, std::uint64_t op_id, std::uint64_t n = 1) {
    if (reg == nullptr)
        return;
    reg->counter(op_metric_name("window_panes_fired_total", op_id)).increment(n);
}
inline void join_matches_inc(MetricsRegistry* reg, std::uint64_t op_id, std::uint64_t n = 1) {
    if (reg == nullptr)
        return;
    reg->counter(op_metric_name("join_matches_total", op_id)).increment(n);
}
inline void async_lookup_hit_inc(MetricsRegistry* reg, std::uint64_t op_id, std::uint64_t n = 1) {
    if (reg == nullptr)
        return;
    reg->counter(op_metric_name("async_lookup_hits_total", op_id)).increment(n);
}
inline void async_lookup_miss_inc(MetricsRegistry* reg, std::uint64_t op_id, std::uint64_t n = 1) {
    if (reg == nullptr)
        return;
    reg->counter(op_metric_name("async_lookup_misses_total", op_id)).increment(n);
}
inline void retractions_emitted_inc(MetricsRegistry* reg,
                                    std::uint64_t op_id,
                                    std::uint64_t n = 1) {
    if (reg == nullptr)
        return;
    reg->counter(op_metric_name("retractions_emitted_total", op_id)).increment(n);
}

// Aggregated process() latency as a histogram (OBS-1b). The exposition keeps
// the historical process_latency_ns_sum / _count line names (a Prometheus
// histogram emits both) and adds _bucket lines, so the moving mean still works
// and p50/p95/p99 are now available.
inline void process_latency_observe(MetricsRegistry* reg,
                                    std::uint64_t op_id,
                                    std::uint64_t nanos) {
    if (reg == nullptr)
        return;
    reg->histogram(op_metric_name("process_latency_ns", op_id)).observe(static_cast<double>(nanos));
}

// Last-value gauge for the operator's current low-watermark (event time, ms).
// A gauge (not counter) because a watermark moves monotonically forward but is
// a level, not an accumulation. idle = 1 when the operator has seen the
// long-max idle watermark (no active event-time progress).
inline void watermark_set(MetricsRegistry* reg, std::uint64_t op_id, std::int64_t millis) {
    if (reg == nullptr)
        return;
    reg->gauge(op_metric_name("watermark_ms", op_id)).set(millis);
}

// Per-operator bytes crossing the network bridge. Counted only at a serialising
// boundary (cross-TM edges); intra-process / chained edges move shared_ptr<Batch>
// with no serialisation, so they are deliberately not counted here.
inline void bytes_sent_inc(MetricsRegistry* reg, std::uint64_t op_id, std::uint64_t n) {
    if (reg == nullptr)
        return;
    reg->counter(op_metric_name("bytes_sent_total", op_id)).increment(n);
}
inline void bytes_received_inc(MetricsRegistry* reg, std::uint64_t op_id, std::uint64_t n) {
    if (reg == nullptr)
        return;
    reg->counter(op_metric_name("bytes_received_total", op_id)).increment(n);
}

// Add a delta to a named user accumulator for this operator (see
// op_acc_metric_name). v may be negative.
inline void accumulator_add(MetricsRegistry* reg,
                            std::uint64_t op_id,
                            const std::string& name,
                            std::int64_t v) {
    if (reg == nullptr)
        return;
    reg->gauge(op_acc_metric_name(op_id, name)).add(v);
}

// Identity mapping: clink_op_info{op_id="N",node="op_X",uid="..."} = 1. Emitted
// once per subtask runner into the HOST registry so a metrics scraper can join
// the numeric op_id back to the spec graph node id (which the runtime derives
// privately, and which is not otherwise recomputable for non-uid operators). A
// gauge whose value is always 1; the information lives in the labels (the
// Prometheus "info metric" convention). `node` is the JobGraphSpec id (op_0,
// op_1, ...); `uid` is the operator's stable uid (empty for unkeyed/stateless
// ops). Label values are kept to safe tokens (ids/uids) so no escaping is needed.
inline void op_info_set(MetricsRegistry* reg,
                        std::uint64_t op_id,
                        const std::string& node,
                        const std::string& uid) {
    if (reg == nullptr || node.empty())
        return;
    std::string name = kOpMetricPrefix;
    name += "info{op_id=\"";
    name += std::to_string(op_id);
    name += "\",node=\"";
    name += node;
    name += "\",uid=\"";
    name += uid;
    name += "\"}";
    reg->gauge(name).set(1);
}

}  // namespace op

}  // namespace clink::metrics
