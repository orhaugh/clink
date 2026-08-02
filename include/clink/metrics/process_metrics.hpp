#pragma once

// Process-level metric names + thin helpers around MetricsRegistry::global().
//
// Each clink_node process (coordinator or worker) owns its own MetricsRegistry::global();
// the /metrics HTTP endpoint snapshots it and renders Prometheus text. The
// helpers below define stable metric names in one place so callers don't have
// to remember strings and so naming stays consistent for dashboards.
//
// Counters use the `_total` suffix per Prometheus convention. Gauges describe
// instantaneous state without a suffix.

#include <cstdint>

#include "clink/metrics/counter.hpp"
#include "clink/metrics/gauge.hpp"
#include "clink/metrics/metrics_registry.hpp"

namespace clink::metrics {

// Stable metric name strings. Exposed so tests / dashboards can refer to them.
inline constexpr const char* kCoordinatorJobsSubmittedTotal =
    "clink_coordinator_jobs_submitted_total";
inline constexpr const char* kCoordinatorJobsCompletedTotal =
    "clink_coordinator_jobs_completed_total";
inline constexpr const char* kCoordinatorJobsCancelledTotal =
    "clink_coordinator_jobs_cancelled_total";
inline constexpr const char* kCoordinatorJobsFailedTotal = "clink_coordinator_jobs_failed_total";
inline constexpr const char* kCoordinatorJobsRunning = "clink_coordinator_jobs_running";
inline constexpr const char* kCoordinatorWorkersRegistered = "clink_coordinator_workers_registered";
inline constexpr const char* kCoordinatorWorkersLostTotal = "clink_coordinator_workers_lost_total";
inline constexpr const char* kCoordinatorSlotsCapacity = "clink_coordinator_slots_capacity";
inline constexpr const char* kCoordinatorSlotsInUse = "clink_coordinator_slots_in_use";

inline constexpr const char* kWorkerSubtasksStartedTotal = "clink_worker_subtasks_started_total";
inline constexpr const char* kWorkerSubtasksCompletedTotal =
    "clink_worker_subtasks_completed_total";
inline constexpr const char* kWorkerSubtasksFailedTotal = "clink_worker_subtasks_failed_total";
// Control frames dropped because they came from a coordinator that had
// already been superseded. Any non-zero value is a split-brain signal:
// two processes believed they held leadership at the same moment.
inline constexpr const char* kWorkerFencedFramesTotal = "clink_worker_fenced_frames_total";
inline constexpr const char* kWorkerSubtasksRunning = "clink_worker_subtasks_running";
inline constexpr const char* kWorkerSlotsCapacity = "clink_worker_slots_capacity";
inline constexpr const char* kWorkerSlotsInUse = "clink_worker_slots_in_use";

// Columnar-execution observability. The engine carries an Arrow sidecar alongside
// a batch and only decodes it into rows when a row accessor is touched, so
// "how often did a batch have to be materialised into rows" is the single number
// that says whether the columnar path is actually reaching the end of a pipeline.
// Without it the question is only answerable by inference from CPU profiles, which
// is how a 0.37us-per-record cost in a sink that does nothing but increment a
// counter went unexplained.
//
// A GAUGE of a cumulative count, deliberately: the underlying value is a plain
// process-wide atomic incremented on the hot path (clink::detail::
// batch_materialize_counter), and routing that through a registry lookup per
// materialisation would cost more than the thing being measured. It is sampled at
// scrape time instead, like the system gauges.
inline constexpr const char* kBatchMaterializationsTotal = "clink_batch_materializations_total";

// Data-plane routing. A shuffle edge between two subtasks in the SAME process can
// hand the StreamElement over in memory instead of encoding it, sending it over a
// socket and decoding it again. LocalDataPlane counted both outcomes already, but
// nothing read the counters, so whether the fast path fires was unknowable from
// outside a debugger - and it had in fact been dead in every container deployment
// until 3cd2058, which nothing would have revealed either.
//
// A rising fallback count on a single-worker job means edges that could pass a
// pointer are paying for serialisation instead.
inline constexpr const char* kDataplaneLocalHitsTotal = "clink_dataplane_local_hits_total";
inline constexpr const char* kDataplaneSocketFallbacksTotal =
    "clink_dataplane_socket_fallbacks_total";

inline constexpr const char* kHttpRequestsTotal = "clink_http_requests_total";
inline constexpr const char* kHttpErrorsTotal = "clink_http_errors_total";

// Per-process-role helpers. All call MetricsRegistry::global() under the hood
// so callers don't have to thread a registry pointer through every site. The
// lookup-by-name cost is fine at coordinator/worker event rates (job submit, subtask
// complete) - these are NOT in operator inner loops.

// Materialize every metric at zero so /metrics returns them on the very
// first scrape (before any jobs/tasks/etc. have fired). Prometheus-friendly:
// dashboards can graph a counter from 0 instead of "no data".
inline void init_coordinator_metrics() {
    auto& r = MetricsRegistry::global();
    (void)r.counter(kCoordinatorJobsSubmittedTotal);
    (void)r.counter(kCoordinatorJobsCompletedTotal);
    (void)r.counter(kCoordinatorJobsCancelledTotal);
    (void)r.counter(kCoordinatorJobsFailedTotal);
    (void)r.counter(kCoordinatorWorkersLostTotal);
    (void)r.counter(kHttpRequestsTotal);
    (void)r.counter(kHttpErrorsTotal);
    (void)r.gauge(kCoordinatorJobsRunning);
    (void)r.gauge(kCoordinatorWorkersRegistered);
    (void)r.gauge(kCoordinatorSlotsCapacity);
    (void)r.gauge(kCoordinatorSlotsInUse);
}

inline void init_worker_metrics() {
    auto& r = MetricsRegistry::global();
    (void)r.counter(kWorkerSubtasksStartedTotal);
    (void)r.counter(kWorkerSubtasksCompletedTotal);
    (void)r.counter(kWorkerSubtasksFailedTotal);
    (void)r.counter(kWorkerFencedFramesTotal);
    (void)r.counter(kHttpRequestsTotal);
    (void)r.counter(kHttpErrorsTotal);
    (void)r.gauge(kWorkerSubtasksRunning);
    (void)r.gauge(kWorkerSlotsCapacity);
    (void)r.gauge(kWorkerSlotsInUse);
}

namespace coordinator {

inline void job_submitted() {
    MetricsRegistry::global().counter(kCoordinatorJobsSubmittedTotal).increment();
    MetricsRegistry::global().gauge(kCoordinatorJobsRunning).add(1);
}

inline void job_completed_ok() {
    MetricsRegistry::global().counter(kCoordinatorJobsCompletedTotal).increment();
    MetricsRegistry::global().gauge(kCoordinatorJobsRunning).sub(1);
}

inline void job_cancelled() {
    MetricsRegistry::global().counter(kCoordinatorJobsCancelledTotal).increment();
    MetricsRegistry::global().gauge(kCoordinatorJobsRunning).sub(1);
}

inline void job_failed() {
    MetricsRegistry::global().counter(kCoordinatorJobsFailedTotal).increment();
    MetricsRegistry::global().gauge(kCoordinatorJobsRunning).sub(1);
}

inline void worker_registered(std::uint32_t slot_capacity) {
    MetricsRegistry::global().gauge(kCoordinatorWorkersRegistered).add(1);
    MetricsRegistry::global()
        .gauge(kCoordinatorSlotsCapacity)
        .add(static_cast<std::int64_t>(slot_capacity));
}

inline void worker_lost(std::uint32_t slot_capacity, std::uint32_t slots_in_use) {
    MetricsRegistry::global().counter(kCoordinatorWorkersLostTotal).increment();
    MetricsRegistry::global().gauge(kCoordinatorWorkersRegistered).sub(1);
    MetricsRegistry::global()
        .gauge(kCoordinatorSlotsCapacity)
        .sub(static_cast<std::int64_t>(slot_capacity));
    MetricsRegistry::global()
        .gauge(kCoordinatorSlotsInUse)
        .sub(static_cast<std::int64_t>(slots_in_use));
}

inline void slots_in_use_delta(std::int64_t delta) {
    MetricsRegistry::global().gauge(kCoordinatorSlotsInUse).add(delta);
}

}  // namespace coordinator

namespace worker {

inline void slot_capacity_set(std::uint32_t cap) {
    MetricsRegistry::global().gauge(kWorkerSlotsCapacity).set(static_cast<std::int64_t>(cap));
}

inline void subtask_started() {
    MetricsRegistry::global().counter(kWorkerSubtasksStartedTotal).increment();
    MetricsRegistry::global().gauge(kWorkerSubtasksRunning).add(1);
    MetricsRegistry::global().gauge(kWorkerSlotsInUse).add(1);
}

inline void subtask_completed_ok() {
    MetricsRegistry::global().counter(kWorkerSubtasksCompletedTotal).increment();
    MetricsRegistry::global().gauge(kWorkerSubtasksRunning).sub(1);
    MetricsRegistry::global().gauge(kWorkerSlotsInUse).sub(1);
}

// A control frame arrived from a superseded coordinator and was dropped.
inline void frame_fenced() {
    MetricsRegistry::global().counter(kWorkerFencedFramesTotal).increment();
}

inline void subtask_failed() {
    MetricsRegistry::global().counter(kWorkerSubtasksFailedTotal).increment();
    MetricsRegistry::global().gauge(kWorkerSubtasksRunning).sub(1);
    MetricsRegistry::global().gauge(kWorkerSlotsInUse).sub(1);
}

}  // namespace worker

namespace http {

inline void request_seen() {
    MetricsRegistry::global().counter(kHttpRequestsTotal).increment();
}
inline void request_error() {
    MetricsRegistry::global().counter(kHttpErrorsTotal).increment();
}

}  // namespace http

}  // namespace clink::metrics
