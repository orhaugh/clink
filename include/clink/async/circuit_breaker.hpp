#pragma once

// Phase 28d: circuit-breaker for async lookup pipelines.
//
// Three-state machine. The breaker sits in front of a downstream
// service (HTTP endpoint, database). On normal call results it stays
// CLOSED and forwards traffic. Once consecutive failures exceed the
// configured threshold it trips OPEN: subsequent calls fast-fail
// without touching the downstream. After a cooldown the breaker
// transitions to HALF_OPEN, letting a single probe through; success
// closes the breaker, failure re-opens it for another cooldown.
//
// This is a textbook pattern (Hystrix, resilience4j). The clink
// implementation is small, header-only, lock-protected so multiple
// in-flight lookups against the same upstream can share a single
// breaker safely.
//
// Usage:
//   CircuitBreaker cb({.failure_threshold = 5,
//                       .cooldown = std::chrono::seconds{30}});
//   if (!cb.allow_call()) {
//       // Fast-fail; do not invoke the upstream.
//       return fallback_response;
//   }
//   try {
//       auto resp = upstream_call();
//       cb.record_success();
//       return resp;
//   } catch (const std::exception&) {
//       cb.record_failure();
//       throw;
//   }
//
// State diagram:
//
//   CLOSED -- (consecutive failures >= threshold) --> OPEN
//   OPEN   -- (cooldown elapsed)                   --> HALF_OPEN
//   HALF_OPEN -- (probe success)                   --> CLOSED
//   HALF_OPEN -- (probe failure)                   --> OPEN
//
// allow_call() is the gate: returns true if the call should proceed
// (CLOSED, or HALF_OPEN and the probe slot is available), false
// otherwise. The HALF_OPEN probe is exclusive - only one in-flight
// probe at a time; concurrent allow_call() during HALF_OPEN return
// false for everyone except the first.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>

#include "clink/metrics/async_metrics.hpp"

namespace clink::async {

enum class CircuitState : std::uint8_t {
    Closed = 0,
    Open = 1,
    HalfOpen = 2,
};

struct CircuitBreakerConfig {
    // Consecutive failures (in CLOSED) before tripping to OPEN.
    int failure_threshold{5};
    // Time spent in OPEN before transitioning to HALF_OPEN.
    std::chrono::milliseconds cooldown{std::chrono::seconds{30}};
};

class CircuitBreaker {
public:
    explicit CircuitBreaker(CircuitBreakerConfig cfg) : cfg_(cfg) {}

    // Decide whether the caller should invoke the upstream right now.
    // Returns true when CLOSED, false when OPEN, and true for the
    // FIRST caller in HALF_OPEN (false for concurrent probers). The
    // caller MUST follow up with record_success/record_failure to
    // resolve the probe.
    bool allow_call() {
        std::lock_guard lock(mu_);
        const auto prev = state_;
        maybe_transition_to_half_open_();
        if (prev != state_ && state_ == CircuitState::HalfOpen) {
            clink::metrics::async::circuit_transition("Open", "HalfOpen");
        }
        if (state_ == CircuitState::Closed) {
            clink::metrics::async::circuit_call_allowed();
            return true;
        }
        if (state_ == CircuitState::HalfOpen) {
            if (probe_in_flight_) {
                clink::metrics::async::circuit_call_rejected();
                return false;
            }
            probe_in_flight_ = true;
            clink::metrics::async::circuit_call_allowed();
            return true;
        }
        clink::metrics::async::circuit_call_rejected();
        return false;  // OPEN
    }

    void record_success() {
        std::lock_guard lock(mu_);
        consecutive_failures_ = 0;
        if (state_ != CircuitState::Closed) {
            const auto prev = state_;
            state_ = CircuitState::Closed;
            probe_in_flight_ = false;
            clink::metrics::async::circuit_transition(
                prev == CircuitState::HalfOpen ? "HalfOpen" : "Open", "Closed");
        }
    }

    void record_failure() {
        std::lock_guard lock(mu_);
        if (state_ == CircuitState::HalfOpen) {
            // Probe failed: back to OPEN with a fresh cooldown.
            state_ = CircuitState::Open;
            opened_at_ = clock_now_();
            probe_in_flight_ = false;
            clink::metrics::async::circuit_transition("HalfOpen", "Open");
            clink::metrics::async::circuit_tripped();
            return;
        }
        ++consecutive_failures_;
        if (state_ == CircuitState::Closed && consecutive_failures_ >= cfg_.failure_threshold) {
            state_ = CircuitState::Open;
            opened_at_ = clock_now_();
            clink::metrics::async::circuit_transition("Closed", "Open");
            clink::metrics::async::circuit_tripped();
        }
    }

    [[nodiscard]] CircuitState state() const {
        std::lock_guard lock(mu_);
        return state_;
    }
    [[nodiscard]] int consecutive_failures() const {
        std::lock_guard lock(mu_);
        return consecutive_failures_;
    }
    [[nodiscard]] CircuitBreakerConfig config() const noexcept { return cfg_; }

private:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    // Hook to enable injection of a test clock - we read clock_now_
    // wherever we need "now" so a future overload could replace it.
    TimePoint clock_now_() const { return Clock::now(); }

    void maybe_transition_to_half_open_() {
        if (state_ != CircuitState::Open) {
            return;
        }
        if (clock_now_() - opened_at_ >= cfg_.cooldown) {
            state_ = CircuitState::HalfOpen;
            probe_in_flight_ = false;
        }
    }

    CircuitBreakerConfig cfg_;
    mutable std::mutex mu_;
    CircuitState state_{CircuitState::Closed};
    int consecutive_failures_{0};
    TimePoint opened_at_{};
    bool probe_in_flight_{false};
};

}  // namespace clink::async
