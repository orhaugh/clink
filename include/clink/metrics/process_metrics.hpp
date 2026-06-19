#pragma once

// Process-level metric names + thin helpers around MetricsRegistry::global().
//
// Each clink_node process (JM or TM) owns its own MetricsRegistry::global();
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
inline constexpr const char* kJmJobsSubmittedTotal = "clink_jm_jobs_submitted_total";
inline constexpr const char* kJmJobsCompletedTotal = "clink_jm_jobs_completed_total";
inline constexpr const char* kJmJobsCancelledTotal = "clink_jm_jobs_cancelled_total";
inline constexpr const char* kJmJobsFailedTotal = "clink_jm_jobs_failed_total";
inline constexpr const char* kJmJobsRunning = "clink_jm_jobs_running";
inline constexpr const char* kJmTmsRegistered = "clink_jm_tms_registered";
inline constexpr const char* kJmTmsLostTotal = "clink_jm_tms_lost_total";
inline constexpr const char* kJmSlotsCapacity = "clink_jm_slots_capacity";
inline constexpr const char* kJmSlotsInUse = "clink_jm_slots_in_use";

inline constexpr const char* kTmSubtasksStartedTotal = "clink_tm_subtasks_started_total";
inline constexpr const char* kTmSubtasksCompletedTotal = "clink_tm_subtasks_completed_total";
inline constexpr const char* kTmSubtasksFailedTotal = "clink_tm_subtasks_failed_total";
inline constexpr const char* kTmSubtasksRunning = "clink_tm_subtasks_running";
inline constexpr const char* kTmSlotsCapacity = "clink_tm_slots_capacity";
inline constexpr const char* kTmSlotsInUse = "clink_tm_slots_in_use";

inline constexpr const char* kHttpRequestsTotal = "clink_http_requests_total";
inline constexpr const char* kHttpErrorsTotal = "clink_http_errors_total";

// Per-process-role helpers. All call MetricsRegistry::global() under the hood
// so callers don't have to thread a registry pointer through every site. The
// lookup-by-name cost is fine at JM/TM event rates (job submit, subtask
// complete) - these are NOT in operator inner loops.

// Materialize every metric at zero so /metrics returns them on the very
// first scrape (before any jobs/tasks/etc. have fired). Prometheus-friendly:
// dashboards can graph a counter from 0 instead of "no data".
inline void init_jm_metrics() {
    auto& r = MetricsRegistry::global();
    (void)r.counter(kJmJobsSubmittedTotal);
    (void)r.counter(kJmJobsCompletedTotal);
    (void)r.counter(kJmJobsCancelledTotal);
    (void)r.counter(kJmJobsFailedTotal);
    (void)r.counter(kJmTmsLostTotal);
    (void)r.counter(kHttpRequestsTotal);
    (void)r.counter(kHttpErrorsTotal);
    (void)r.gauge(kJmJobsRunning);
    (void)r.gauge(kJmTmsRegistered);
    (void)r.gauge(kJmSlotsCapacity);
    (void)r.gauge(kJmSlotsInUse);
}

inline void init_tm_metrics() {
    auto& r = MetricsRegistry::global();
    (void)r.counter(kTmSubtasksStartedTotal);
    (void)r.counter(kTmSubtasksCompletedTotal);
    (void)r.counter(kTmSubtasksFailedTotal);
    (void)r.counter(kHttpRequestsTotal);
    (void)r.counter(kHttpErrorsTotal);
    (void)r.gauge(kTmSubtasksRunning);
    (void)r.gauge(kTmSlotsCapacity);
    (void)r.gauge(kTmSlotsInUse);
}

namespace jm {

inline void job_submitted() {
    MetricsRegistry::global().counter(kJmJobsSubmittedTotal).increment();
    MetricsRegistry::global().gauge(kJmJobsRunning).add(1);
}

inline void job_completed_ok() {
    MetricsRegistry::global().counter(kJmJobsCompletedTotal).increment();
    MetricsRegistry::global().gauge(kJmJobsRunning).sub(1);
}

inline void job_cancelled() {
    MetricsRegistry::global().counter(kJmJobsCancelledTotal).increment();
    MetricsRegistry::global().gauge(kJmJobsRunning).sub(1);
}

inline void job_failed() {
    MetricsRegistry::global().counter(kJmJobsFailedTotal).increment();
    MetricsRegistry::global().gauge(kJmJobsRunning).sub(1);
}

inline void tm_registered(std::uint32_t slot_capacity) {
    MetricsRegistry::global().gauge(kJmTmsRegistered).add(1);
    MetricsRegistry::global().gauge(kJmSlotsCapacity).add(static_cast<std::int64_t>(slot_capacity));
}

inline void tm_lost(std::uint32_t slot_capacity, std::uint32_t slots_in_use) {
    MetricsRegistry::global().counter(kJmTmsLostTotal).increment();
    MetricsRegistry::global().gauge(kJmTmsRegistered).sub(1);
    MetricsRegistry::global().gauge(kJmSlotsCapacity).sub(static_cast<std::int64_t>(slot_capacity));
    MetricsRegistry::global().gauge(kJmSlotsInUse).sub(static_cast<std::int64_t>(slots_in_use));
}

inline void slots_in_use_delta(std::int64_t delta) {
    MetricsRegistry::global().gauge(kJmSlotsInUse).add(delta);
}

}  // namespace jm

namespace tm {

inline void slot_capacity_set(std::uint32_t cap) {
    MetricsRegistry::global().gauge(kTmSlotsCapacity).set(static_cast<std::int64_t>(cap));
}

inline void subtask_started() {
    MetricsRegistry::global().counter(kTmSubtasksStartedTotal).increment();
    MetricsRegistry::global().gauge(kTmSubtasksRunning).add(1);
    MetricsRegistry::global().gauge(kTmSlotsInUse).add(1);
}

inline void subtask_completed_ok() {
    MetricsRegistry::global().counter(kTmSubtasksCompletedTotal).increment();
    MetricsRegistry::global().gauge(kTmSubtasksRunning).sub(1);
    MetricsRegistry::global().gauge(kTmSlotsInUse).sub(1);
}

inline void subtask_failed() {
    MetricsRegistry::global().counter(kTmSubtasksFailedTotal).increment();
    MetricsRegistry::global().gauge(kTmSubtasksRunning).sub(1);
    MetricsRegistry::global().gauge(kTmSlotsInUse).sub(1);
}

}  // namespace tm

namespace http {

inline void request_seen() {
    MetricsRegistry::global().counter(kHttpRequestsTotal).increment();
}
inline void request_error() {
    MetricsRegistry::global().counter(kHttpErrorsTotal).increment();
}

}  // namespace http

}  // namespace clink::metrics
