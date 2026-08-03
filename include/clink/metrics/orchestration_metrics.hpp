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
//   - clink_protocol_mismatches_total
//   - clink_malformed_frames_total
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
// Handshakes refused because the peer speaks a wire protocol this build
// cannot honour. Non-zero during a rolling upgrade means the roll has
// reached a version boundary it cannot cross; non-zero otherwise means a
// node is running the wrong binary.
inline constexpr const char* kProtocolMismatches = "clink_protocol_mismatches_total";
// Connections dropped because a frame did not decode. A peer that cannot
// produce a well-formed frame is either badly version-skewed or hostile;
// either way this counts it rather than letting it pass unnoticed.
inline constexpr const char* kMalformedFrames = "clink_malformed_frames_total";

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
inline void ha_leader_takeover() {
    MetricsRegistry::global().counter(kHaLeaderTakeovers).increment();
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
