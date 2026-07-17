#pragma once

// Checkpointing observability.
//
// Coverage:
//   - checkpoints_triggered_total      : coordinator-side trigger fires
//   - checkpoints_completed_total      : all-acked completions
//   - checkpoints_failed_total         : aborted / hit max retries
//   - checkpoint_duration_ms_sum/count : end-to-end coordinator trigger -> all
//                                        acked, aggregated
//   - barrier_alignments_total         : per-operator successful
//                                        alignments (every alive
//                                        input delivered the same
//                                        barrier id)
//   - barrier_align_wait_ns_sum/count  : aggregated wall time from
//                                        first input's barrier
//                                        delivery to alignment - the
//                                        "alignment lag" metric
//                                        operators watch for slow
//                                        paths
//   - subtask_snapshot_ack_total       : SubtaskCheckpointed ok acks
//   - subtask_snapshot_failure_total   : SubtaskCheckpointed not-ok
//                                        acks (snapshot threw or
//                                        backend errored)
//   - restore_from_savepoint_ns_sum / count : aggregate time spent
//                                              loading a savepoint at
//                                              subtask startup. Sums
//                                              every state backend
//                                              restore performed at
//                                              task open().
//
// The coordinator-side counters use the `clink_ckpt_` prefix; the per-operator
// alignment counters use `clink_op_barrier_*` and are keyed by
// op_id like the operator data-plane metrics.

#include <cstdint>
#include <string>

#include "clink/metrics/metrics_registry.hpp"
#include "clink/metrics/operator_metrics.hpp"

namespace clink::metrics {

inline constexpr const char* kCheckpointTriggered = "clink_ckpt_triggered_total";
inline constexpr const char* kCheckpointCompleted = "clink_ckpt_completed_total";
inline constexpr const char* kCheckpointFailed = "clink_ckpt_failed_total";
inline constexpr const char* kCheckpointDurationMsSum = "clink_ckpt_duration_ms_sum";
inline constexpr const char* kCheckpointDurationMsCount = "clink_ckpt_duration_ms_count";
inline constexpr const char* kSubtaskSnapshotAck = "clink_ckpt_subtask_snapshot_ack_total";
inline constexpr const char* kSubtaskSnapshotFailure = "clink_ckpt_subtask_snapshot_failure_total";
// Restore-from-savepoint latency histogram base (OBS-1b). Exposes
// clink_ckpt_restore_ns_{bucket,sum,count}.
inline constexpr const char* kRestoreFromSavepointNs = "clink_ckpt_restore_ns";

inline void init_checkpoint_metrics() {
    auto& r = MetricsRegistry::global();
    (void)r.counter(kCheckpointTriggered);
    (void)r.counter(kCheckpointCompleted);
    (void)r.counter(kCheckpointFailed);
    (void)r.counter(kCheckpointDurationMsSum);
    (void)r.counter(kCheckpointDurationMsCount);
    (void)r.counter(kSubtaskSnapshotAck);
    (void)r.counter(kSubtaskSnapshotFailure);
    (void)r.histogram(kRestoreFromSavepointNs);
}

namespace ckpt {

inline void triggered() {
    MetricsRegistry::global().counter(kCheckpointTriggered).increment();
}
inline void completed(std::uint64_t duration_ms) {
    MetricsRegistry::global().counter(kCheckpointCompleted).increment();
    MetricsRegistry::global().counter(kCheckpointDurationMsSum).increment(duration_ms);
    MetricsRegistry::global().counter(kCheckpointDurationMsCount).increment();
}
inline void failed() {
    MetricsRegistry::global().counter(kCheckpointFailed).increment();
}
inline void subtask_ack_ok() {
    MetricsRegistry::global().counter(kSubtaskSnapshotAck).increment();
}
inline void subtask_ack_failure() {
    MetricsRegistry::global().counter(kSubtaskSnapshotFailure).increment();
}
inline void restore_observe(std::uint64_t duration_ns) {
    MetricsRegistry::global()
        .histogram(kRestoreFromSavepointNs)
        .observe(static_cast<double>(duration_ns));
}

// Per-operator barrier alignment. Wired from MultiInputAlignment::on_barrier
// which stamps first-delivery time per checkpoint and observes the
// aligned-duration when every alive input has delivered the same id.
inline void barrier_aligned(std::uint64_t op_id, std::uint64_t wait_ns) {
    MetricsRegistry::global()
        .counter(op_metric_name("barrier_alignments_total", op_id))
        .increment();
    MetricsRegistry::global()
        .histogram(op_metric_name("barrier_align_wait_ns", op_id))
        .observe(static_cast<double>(wait_ns));
}

}  // namespace ckpt

}  // namespace clink::metrics
