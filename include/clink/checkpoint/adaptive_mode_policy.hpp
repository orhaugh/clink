#pragma once

// Adaptive checkpoint-mode policy.
//
// Decides, one observation window at a time, whether the next checkpoint's
// barriers should be stamped Aligned or Unaligned. The inputs are
// normalised pressure observations in [0, 1] - the caller defines what
// pressure means for its runtime (channel occupancy at trigger for the
// in-process Dag runtime; last checkpoint duration relative to the
// interval for the cluster coordinator) - and the policy owns only the
// decision discipline:
//
//   * hysteresis: one pressured window never flips the mode. Switching to
//     unaligned takes `windows_to_unaligned` CONSECUTIVE pressured
//     observations, and switching back takes `windows_to_aligned`
//     consecutive calm ones, so a transient spike costs nothing and the
//     mode cannot oscillate faster than the configured runs;
//   * explicit thresholds: an observation is pressured iff
//     `pressure >= pressure_threshold`. No smoothing hides the number
//     that made the decision;
//   * bounded history: the recent observations are kept (capped) for
//     diagnostics, never for the decision - the decision state is just
//     the two consecutive-run counters.
//
// The policy is deliberately deterministic and single-threaded: callers
// serialise observe() under their own trigger cadence (the coordinator's
// trigger sweep, the CheckpointCoordinator's periodic trigger thread).
// Tests drive it with synthetic pressure sequences, never machine load.

#include <cstddef>
#include <cstdint>
#include <deque>

#include "clink/checkpoint/checkpoint_barrier.hpp"

namespace clink::checkpoint {

struct AdaptiveModePolicyConfig {
    // An observation at or above this is a pressured window.
    double pressure_threshold{0.5};
    // Consecutive pressured windows before the mode switches to
    // Unaligned. Minimum 1; 2+ is what makes a one-window spike free.
    std::uint32_t windows_to_unaligned{2};
    // Consecutive calm windows before the mode returns to Aligned.
    // Deliberately defaulted higher than windows_to_unaligned: leaving
    // unaligned mode too eagerly re-pays the alignment stall the switch
    // existed to avoid.
    std::uint32_t windows_to_aligned{3};
    // Bounded diagnostic history of recent observations.
    std::size_t history_capacity{64};
};

class AdaptiveModePolicy {
public:
    explicit AdaptiveModePolicy(AdaptiveModePolicyConfig cfg = {}) : cfg_(cfg) {
        if (cfg_.windows_to_unaligned == 0) {
            cfg_.windows_to_unaligned = 1;
        }
        if (cfg_.windows_to_aligned == 0) {
            cfg_.windows_to_aligned = 1;
        }
    }

    // Record one observation window and return the mode the NEXT
    // checkpoint should use. Pressure outside [0, 1] is clamped.
    CheckpointBarrier::Mode observe(double pressure) {
        if (pressure < 0.0) {
            pressure = 0.0;
        }
        if (pressure > 1.0) {
            pressure = 1.0;
        }
        if (history_.size() == cfg_.history_capacity && !history_.empty()) {
            history_.pop_front();
        }
        if (cfg_.history_capacity > 0) {
            history_.push_back(pressure);
        }
        const bool pressured = pressure >= cfg_.pressure_threshold;
        if (pressured) {
            ++pressured_run_;
            calm_run_ = 0;
        } else {
            ++calm_run_;
            pressured_run_ = 0;
        }
        if (mode_ == CheckpointBarrier::Mode::Aligned &&
            pressured_run_ >= cfg_.windows_to_unaligned) {
            mode_ = CheckpointBarrier::Mode::Unaligned;
            ++switches_;
        } else if (mode_ == CheckpointBarrier::Mode::Unaligned &&
                   calm_run_ >= cfg_.windows_to_aligned) {
            mode_ = CheckpointBarrier::Mode::Aligned;
            ++switches_;
        }
        return mode_;
    }

    [[nodiscard]] CheckpointBarrier::Mode mode() const noexcept { return mode_; }
    [[nodiscard]] std::uint64_t switches() const noexcept { return switches_; }
    [[nodiscard]] const std::deque<double>& recent() const noexcept { return history_; }
    [[nodiscard]] const AdaptiveModePolicyConfig& config() const noexcept { return cfg_; }

private:
    AdaptiveModePolicyConfig cfg_;
    CheckpointBarrier::Mode mode_{CheckpointBarrier::Mode::Aligned};
    std::uint32_t pressured_run_{0};
    std::uint32_t calm_run_{0};
    std::uint64_t switches_{0};
    std::deque<double> history_;
};

}  // namespace clink::checkpoint
