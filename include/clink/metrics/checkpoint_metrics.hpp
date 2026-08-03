#pragma once

// Checkpointing observability.
//
// Coverage:
//   - checkpoints_triggered_total      : coordinator-side trigger fires
//   - checkpoints_completed_total      : all-acked completions
//   - checkpoints_failed_total         : aborted / hit max retries
//   - checkpoint_duration_ms_{bucket,sum,count} : end-to-end coordinator
//                                        trigger -> all acked, as a
//                                        histogram so a p99 is available and
//                                        not just a mean
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

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

#include "clink/metrics/metrics_registry.hpp"
#include "clink/metrics/operator_metrics.hpp"

namespace clink::metrics {

inline constexpr const char* kCheckpointTriggered = "clink_ckpt_triggered_total";
inline constexpr const char* kCheckpointCompleted = "clink_ckpt_completed_total";
inline constexpr const char* kCheckpointFailed = "clink_ckpt_failed_total";
// Wall-clock second at which a checkpoint last reached GLOBAL completion.
//
// A gauge, and a timestamp rather than an age, because the question an
// operator needs answered is "how long since the last successful
// checkpoint" - the canonical streaming alert, since a stalled checkpoint
// means the recovery window is growing without bound.
//
// kCheckpointCompleted cannot answer it. A counter that has stopped moving
// looks identical to an idle job over any short window, and rate() over a
// window long enough to distinguish them smears the signal past use. A
// timestamp is exact: a dashboard computes
// time() - clink_ckpt_last_completed_unix_seconds and alerts above a
// threshold.
//
// A timestamp rather than an in-process age, because an age must be
// refreshed on a timer to stay true, and a gauge that is only correct when
// something remembers to update it is how metrics come to read 0 forever.
// This changes exactly when the event happens.
inline constexpr const char* kCheckpointLastCompletedUnixSeconds =
    "clink_ckpt_last_completed_unix_seconds";
// Checkpoint duration as a histogram rather than a counter pair.
//
// The exposition is unchanged where it already existed: a histogram named
// `clink_ckpt_duration_ms` renders `_sum` and `_count` under exactly the
// names the two counters used, so every existing query keeps working. What
// it adds is `_bucket{le}`, and with it a p99 - which is the number that
// matters here, because the mean of a checkpoint duration hides precisely
// the case worth alerting on. A job whose checkpoints usually take 200ms and
// occasionally take 40s has a healthy-looking mean and a broken tail, and
// the tail is what eats the interval and stalls the barrier.
//
// Buckets are milliseconds, ascending, spanning the range a checkpoint
// plausibly occupies: sub-100ms for a small in-memory job, seconds for a
// large RocksDB one, and a minute at the top because a checkpoint that slow
// is the thing being hunted rather than an outlier to clip.
inline constexpr const char* kCheckpointDurationMs = "clink_ckpt_duration_ms";

inline std::vector<double> checkpoint_duration_buckets_ms() {
    return {10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000};
}
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
    // Pre-registered so the series EXISTS before the first checkpoint
    // completes. Without it an alert on "time() - last_completed" has no
    // series to evaluate on a fresh coordinator and silently does not fire -
    // which is the window where a broken job most needs the alert.
    (void)r.gauge(kCheckpointLastCompletedUnixSeconds);
    (void)r.histogram(kCheckpointDurationMs, checkpoint_duration_buckets_ms());
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
    MetricsRegistry::global()
        .histogram(kCheckpointDurationMs, checkpoint_duration_buckets_ms())
        .observe(static_cast<double>(duration_ms));
}
// Stamp the completion time. Called alongside completed(), not instead of
// it: the counter answers "how many" and this answers "how recently", and
// an alert needs the second.
inline void last_completed_now() {
    MetricsRegistry::global()
        .gauge(kCheckpointLastCompletedUnixSeconds)
        .set(static_cast<std::int64_t>(std::chrono::duration_cast<std::chrono::seconds>(
                                           std::chrono::system_clock::now().time_since_epoch())
                                           .count()));
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
