#pragma once

// Phase 29e: PID controller for adaptive rescaling.
//
// A textbook PID controller: given a series of (measured, dt) samples
// and a setpoint, produces a continuous control output that drives
// the measured signal towards the setpoint. The output is clamped
// to a configured range so the consumer (Phase 29c's
// RescaleCoordinator) can map it onto discrete scaling decisions
// (+1 / 0 / -1 subtask) without having to handle unbounded values.
//
// This is a standalone utility - the controller knows nothing about
// operators, metrics, or io_uring. Autoscaler integration happens
// in 29c/d; here we just ship the math.
//
// Anti-windup: when the output saturates (hits min/max), the integral
// term stops accumulating in the saturating direction. Prevents the
// classic "integrator winds up while output is pinned, overshoots
// when finally back in range" failure mode.
//
// Sample-period awareness: `update(measured, dt)` takes the time
// since the previous update. Tests pass a fixed dt; production
// autoscalers pass the wall-clock elapsed between metric polls.

#include <algorithm>
#include <chrono>

namespace clink::async {

struct PidConfig {
    // Proportional / integral / derivative gains. Tuning is workload-
    // specific; sane defaults below produce a slightly damped response
    // when measured is on the same scale as setpoint (e.g., 0..1
    // queue-fill ratio).
    double kp{1.0};
    double ki{0.1};
    double kd{0.05};
    // Output clamp. The controller never returns outside [min, max].
    double output_min{-1.0};
    double output_max{1.0};
};

class PidController {
public:
    explicit PidController(PidConfig cfg) : cfg_(cfg) {}

    // Set or change the setpoint. The next update() call uses the new
    // target. Does not reset integral state.
    void set_setpoint(double sp) noexcept { setpoint_ = sp; }
    [[nodiscard]] double setpoint() const noexcept { return setpoint_; }

    // Clear accumulated integral + derivative state. Call when the
    // controlled system has been externally re-baselined (e.g.,
    // operator rescaled by an explicit user request) so the
    // controller doesn't fight historical error.
    void reset() noexcept {
        integral_ = 0.0;
        prev_error_ = 0.0;
        output_ = 0.0;
        has_prev_ = false;
    }

    // Feed a measurement + the time since the previous update; get
    // back the next control output, clamped to [output_min,
    // output_max]. dt of zero is tolerated (returns the proportional
    // term only) so callers don't need to special-case the first
    // sample.
    double update(double measured, std::chrono::milliseconds dt) noexcept {
        const double error = setpoint_ - measured;
        const double dt_s = static_cast<double>(dt.count()) / 1000.0;

        // Compute candidate output. The integral term is updated
        // tentatively; if the result saturates, we roll back the
        // integral accumulation (anti-windup).
        const double prev_integral = integral_;
        if (dt_s > 0.0) {
            integral_ += error * dt_s;
        }

        double derivative = 0.0;
        if (has_prev_ && dt_s > 0.0) {
            derivative = (error - prev_error_) / dt_s;
        }

        double raw = cfg_.kp * error + cfg_.ki * integral_ + cfg_.kd * derivative;
        double clamped = std::clamp(raw, cfg_.output_min, cfg_.output_max);

        // Anti-windup: if we clipped, the integrator's contribution
        // pushed us out of range. Roll it back so it doesn't keep
        // accumulating while we're pinned.
        if (clamped != raw) {
            integral_ = prev_integral;
        }

        prev_error_ = error;
        has_prev_ = true;
        output_ = clamped;
        return clamped;
    }

    // Last output returned by update(). Zero before the first call.
    [[nodiscard]] double output() const noexcept { return output_; }

    // Diagnostic accessor for the accumulated integral term. Useful
    // for verifying anti-windup in tests.
    [[nodiscard]] double integral() const noexcept { return integral_; }

    [[nodiscard]] PidConfig config() const noexcept { return cfg_; }

private:
    PidConfig cfg_;
    double setpoint_{0.0};
    double integral_{0.0};
    double prev_error_{0.0};
    double output_{0.0};
    bool has_prev_{false};
};

}  // namespace clink::async
