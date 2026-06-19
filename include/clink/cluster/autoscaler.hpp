#pragma once

// Phase 29g: adaptive autoscaler loop.
//
// Closes the Phase 29 arc by driving `JobManager::request_operator_rescale`
// from a periodic metric poll, one PidController per operator. The
// autoscaler is intentionally decoupled from JobManager via three
// function objects (sample / request / status) so it's testable
// without a cluster and so callers can plug their own metric source.
//
// Per-tick algorithm:
//   1. For each registered operator, status_fn(op_id) returns its
//      current parallelism + Phase 29a [min, max] bounds + whether a
//      rescale is already in flight.
//   2. If the operator is mid-rescale, skip the tick - the PidController
//      keeps its state but emits no decision until the rescale completes.
//   3. Otherwise sample_fn(op_id) returns a normalized 0..1 saturation
//      signal (queue-fill ratio, cpu_pct, custom). The PidController
//      consumes (sample, dt) and returns a control output in
//      [pid.output_min, pid.output_max].
//   4. If |output| < rescale_threshold, idle (hysteresis: small
//      controller swings don't trigger a rescale).
//   5. Otherwise map output to a discrete delta of +/- 1 subtask and
//      compute new_parallelism = clamp(current + delta, min, max).
//      If new equals current (already at a bound) skip.
//   6. Cooldown gate: if last_request for this op was less than
//      cfg.cooldown ago, skip.
//   7. Call request_fn(op_id, new_parallelism). Stamp last_request
//      on accept; reset the PidController if the request was rejected
//      with a hard failure (operator not scalable, bounds clamp at
//      both ends, etc.) so it doesn't keep recommending impossible
//      rescales.
//
// The loop thread is started by start() and joined by stop() / dtor.
// tick() is also exposed for tests that want deterministic single-step
// control.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "clink/async/pid_controller.hpp"
#include "clink/cluster/rescale_coordinator.hpp"

namespace clink::cluster {

struct AutoscalerConfig {
    // Wall-clock interval between metric polls. The PidController
    // receives dt = sample_period on each tick so derivative and
    // integral terms scale consistently.
    std::chrono::milliseconds sample_period{std::chrono::seconds{5}};
    // Target normalized saturation. Setpoints in 0.5 .. 0.8 are
    // typical: low enough to leave headroom for spikes, high enough
    // to not over-provision.
    double setpoint{0.7};
    // PidController gains + output clamp. Default clamps to [-1, 1];
    // map() interprets that as +/- 1 subtask per tick.
    clink::async::PidConfig pid{};
    // Hysteresis threshold on |pid_output|. Below this the autoscaler
    // is idle. Prevents rapid +/- flapping when the metric oscillates
    // around the setpoint.
    double rescale_threshold{0.5};
    // Minimum wall-clock interval between two rescale requests for the
    // same operator. Mirrors the cluster-level cooldown that gives
    // each rescale time to settle before another is even considered.
    std::chrono::milliseconds cooldown{std::chrono::seconds{30}};
};

// Single tick's decision for one operator. Returned by tick() so
// tests and dashboards can inspect what the autoscaler did this round.
struct AutoscalerDecision {
    std::string op_id;
    double sample{0.0};
    double pid_output{0.0};
    std::uint32_t current_parallelism{0};
    std::uint32_t target_parallelism{0};
    bool requested{false};
    bool accepted{false};
    std::string reason;
};

class Autoscaler {
public:
    using SampleFn = std::function<double(const std::string& op_id)>;
    using RequestRescaleFn = std::function<RescaleCoordinator::RequestResult(
        const std::string& op_id, std::uint32_t new_parallelism)>;
    using StatusFn = std::function<std::optional<OperatorRescaleStatus>(const std::string& op_id)>;

    Autoscaler(AutoscalerConfig cfg, SampleFn sample, RequestRescaleFn request, StatusFn status);
    ~Autoscaler();

    Autoscaler(const Autoscaler&) = delete;
    Autoscaler& operator=(const Autoscaler&) = delete;

    // Add op_id to the rotation. Idempotent. Tick will poll it on the
    // next pass.
    void register_operator(const std::string& op_id);

    // Drop op_id from the rotation. Idempotent.
    void unregister_operator(const std::string& op_id);

    // Synchronous evaluation of every registered operator. Public so
    // tests can drive the controller without spinning the background
    // thread. dt is fed to the per-op PidController.
    std::vector<AutoscalerDecision> tick(std::chrono::milliseconds dt);

    // Start the background polling thread. Safe to call once; later
    // calls are no-ops.
    void start();

    // Signal the polling thread to stop and join. Idempotent; the
    // destructor calls this. Safe to call before start().
    void stop();

    // Reset the PidController state for one operator (e.g., after an
    // explicit user-driven rescale that re-baselined the system).
    void reset_operator(const std::string& op_id);

    // Total ticks executed by the background thread. Test diagnostic.
    [[nodiscard]] std::uint64_t ticks() const noexcept {
        return ticks_.load(std::memory_order_relaxed);
    }

private:
    void run_();

    struct OpState {
        clink::async::PidController pid;
        std::chrono::steady_clock::time_point last_request{};
        bool has_last_request{false};
    };

    AutoscalerConfig cfg_;
    SampleFn sample_fn_;
    RequestRescaleFn request_fn_;
    StatusFn status_fn_;

    mutable std::mutex mu_;
    std::unordered_map<std::string, OpState> ops_;

    std::atomic<bool> running_{false};
    std::atomic<bool> stop_{false};
    std::atomic<std::uint64_t> ticks_{0};
    std::thread thread_;
};

}  // namespace clink::cluster
