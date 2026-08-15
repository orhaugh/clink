#pragma once

// Cluster orchestration observability.
//
// Coverage:
//   - clink_rescale_state_transitions_total{from,to}
//   - clink_rescale_requests_total{result="accepted"|"rejected"}
//   - clink_rescale_cutover_deploys_total
//   - clink_rescale_aborts_total
//   - clink_autoscaler_ticks_total
//   - clink_autoscaler_decisions_total{outcome}
//     where outcome ∈ {requested, accepted, rejected, idle,
//                       cooldown, mid_rescale, no_bounds}
//   - clink_ha_leader_takeovers_total
//   - clink_ha_recovered_jobs_total
//   - clink_coordinator_job_restarts_total
//   - clink_coordinator_subtask_redeploys_total
//   - clink_protocol_mismatches_total
//   - clink_malformed_frames_total
//   - clink_client_connections_refused_total
//
// The metrics surface here is sized for dashboards and alert rules
// rather than per-operator inner loops. Costs are bounded by the
// orchestration cadence (rescale + tick) which is multi-second.

#include <cstdint>
#include <string>

#include "clink/metrics/metrics_registry.hpp"

namespace clink::metrics {

inline std::string rescale_state_transition_name(const char* from, const char* to) {
    std::string out = "clink_rescale_state_transitions_total{from=\"";
    out += from;
    out += "\",to=\"";
    out += to;
    out += "\"}";
    return out;
}

inline std::string rescale_request_name(const char* result) {
    std::string out = "clink_rescale_requests_total{result=\"";
    out += result;
    out += "\"}";
    return out;
}

inline std::string autoscaler_decision_name(const char* outcome) {
    std::string out = "clink_autoscaler_decisions_total{outcome=\"";
    out += outcome;
    out += "\"}";
    return out;
}

inline constexpr const char* kRescaleCutoverDeploys = "clink_rescale_cutover_deploys_total";
inline constexpr const char* kRescaleAborts = "clink_rescale_aborts_total";
inline constexpr const char* kAutoscalerTicks = "clink_autoscaler_ticks_total";
inline constexpr const char* kHaLeaderTakeovers = "clink_ha_leader_takeovers_total";
inline constexpr const char* kHaRecoveredJobs = "clink_ha_recovered_jobs_total";
// Whole-job restarts the coordinator has performed.
//
// There was no restart metric at all, which left the most informative
// unhealthy state invisible: a job that keeps failing and being redeployed
// is working as designed right up to the moment its budget runs out, and
// only then does clink_coordinator_jobs_failed_total move. So the signal
// arrived after the recovery had already been spent, rather than on the
// first restart.
//
// Restart LOOPS are the shape this catches - the crash-per-record job an
// unknown SQL function used to produce, or a source that cannot replay.
inline constexpr const char* kJobRestarts = "clink_coordinator_job_restarts_total";
// Per-SUBTASK redeploys, which are a separate mechanism from the whole-job
// restart above and were equally uncounted.
//
// Two paths exist: a checkpointed job rolls the WHOLE job back to its last
// checkpoint (kJobRestarts), because a per-subtask redeploy would leave the
// others un-rolled-back and break exactly-once; a job WITHOUT a checkpoint
// directory retries the failing subtask in place. Counting only the first
// left every non-checkpointed job's retries invisible - and those are the
// jobs with no recovery, so their retries matter more, not less.
//
// Separate series rather than one total, because they mean different things
// operationally: a whole-job restart replays from a checkpoint, a subtask
// redeploy does not.
inline constexpr const char* kSubtaskRedeploys = "clink_coordinator_subtask_redeploys_total";
// Handshakes refused because the peer speaks a wire protocol this build
// cannot honour. Non-zero during a rolling upgrade means the roll has
// reached a version boundary it cannot cross; non-zero otherwise means a
// node is running the wrong binary.
inline constexpr const char* kProtocolMismatches = "clink_protocol_mismatches_total";
// Connections dropped because a frame did not decode. A peer that cannot
// produce a well-formed frame is either badly version-skewed or hostile;
// either way this counts it rather than letting it pass unnoticed.
inline constexpr const char* kMalformedFrames = "clink_malformed_frames_total";
// Clients turned away because the coordinator was already at its
// connection limit. Non-zero means either a client leak somewhere or a
// limit set too low - both worth knowing before it becomes an outage.
inline constexpr const char* kClientConnectionsRefused = "clink_client_connections_refused_total";
inline constexpr const char* kWorkerConnectionsRefused =
    "clink_coordinator_worker_connections_refused_total";
// In-doubt transaction resolution at recovery: the two OUTCOMES a
// restore can have when a checkpoint completed but its external commit
// may not have executed.
//
// Resolved means every staged handle of that checkpoint was PROVEN
// committed by its connector's resolver, so the restore point advances
// and the run stays exactly-once. Unresolved means the walk stopped -
// no resolver, a refusal, an unreachable broker - and the job falls
// back to the commit-confirmed contract, which trades a bounded replay
// (duplicates for one interval) for never losing data.
//
// These are counted separately because the difference is the difference
// between two DIFFERENT guarantees, and an operator - or a
// qualification campaign judging duplicate output - cannot tell them
// apart from logs alone.
inline constexpr const char* kInDoubtResolved = "clink_recovery_in_doubt_resolved_total";
inline constexpr const char* kInDoubtUnresolved = "clink_recovery_in_doubt_unresolved_total";
// Control frames whose MessageKind this build does not handle.
//
// Both dispatch loops used to drop an unknown kind with a bare
// `default: break;`, so a peer speaking a newer protocol got SILENCE:
// the sender waits for an effect that never happens while the version
// handshake reports the pair compatible. During a rolling upgrade that
// is the difference between "the roll crossed a version boundary" and
// "a graceful stop hung for no visible reason".
inline constexpr const char* kUnknownControlFrames = "clink_unknown_control_frames_total";

namespace orch {

inline void rescale_state_transition(const char* from, const char* to) {
    MetricsRegistry::global().counter(rescale_state_transition_name(from, to)).increment();
}
inline void rescale_request_accepted() {
    MetricsRegistry::global().counter(rescale_request_name("accepted")).increment();
}
inline void rescale_request_rejected() {
    MetricsRegistry::global().counter(rescale_request_name("rejected")).increment();
}
inline void rescale_cutover_deploy() {
    MetricsRegistry::global().counter(kRescaleCutoverDeploys).increment();
}
inline void rescale_aborted() {
    MetricsRegistry::global().counter(kRescaleAborts).increment();
}
inline void autoscaler_tick() {
    MetricsRegistry::global().counter(kAutoscalerTicks).increment();
}
inline void autoscaler_decision(const char* outcome) {
    MetricsRegistry::global().counter(autoscaler_decision_name(outcome)).increment();
}
inline void unknown_control_frame() {
    MetricsRegistry::global().counter(kUnknownControlFrames).increment();
}
inline void in_doubt_resolved() {
    MetricsRegistry::global().counter(kInDoubtResolved).increment();
}
inline void in_doubt_unresolved() {
    MetricsRegistry::global().counter(kInDoubtUnresolved).increment();
}
inline void ha_leader_takeover() {
    MetricsRegistry::global().counter(kHaLeaderTakeovers).increment();
}
inline void job_restarted() {
    MetricsRegistry::global().counter(kJobRestarts).increment();
}
inline void subtask_redeployed() {
    MetricsRegistry::global().counter(kSubtaskRedeploys).increment();
}
inline void client_connection_refused() {
    MetricsRegistry::global().counter(kClientConnectionsRefused).increment();
}

// A worker registration refused for being at the connection limit. Counted
// separately from the client refusal: they mean different things operationally -
// one is a CLI or dashboard being turned away, the other is a worker that will
// not join the cluster.
inline void worker_connection_refused() {
    MetricsRegistry::global().counter(kWorkerConnectionsRefused).increment();
}
inline void malformed_frame() {
    MetricsRegistry::global().counter(kMalformedFrames).increment();
}
inline void protocol_mismatch() {
    MetricsRegistry::global().counter(kProtocolMismatches).increment();
}
inline void ha_recovered_jobs_inc(std::uint64_t n = 1) {
    MetricsRegistry::global().counter(kHaRecoveredJobs).increment(n);
}

}  // namespace orch

}  // namespace clink::metrics
