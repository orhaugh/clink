#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <random>

#include "clink/runtime/runtime_context.hpp"

namespace clink::clickhouse::native {

// Full jitter: the nth wait is uniform in [0, min(cap, initial * 2^n)].
class Backoff {
public:
    explicit Backoff(std::chrono::milliseconds initial = std::chrono::milliseconds{100},
                     std::chrono::milliseconds cap = std::chrono::seconds{10},
                     std::uint64_t seed = std::random_device{}());
    [[nodiscard]] std::chrono::milliseconds next();
    void reset();
    // The waits handed out since construction or the last reset().
    [[nodiscard]] std::uint32_t attempts() const noexcept { return attempts_; }

private:
    std::chrono::milliseconds initial_;
    std::chrono::milliseconds cap_;
    std::mt19937_64 rng_;
    std::uint32_t attempts_{0};
};

// Per INSERT: starts at the INSERT's first failure; at open: starts when
// open() begins. The two halves of a split INSERT take a copy of their
// parent's window, deadline included, so one window covers the whole split
// tree. start() on a window that has already started keeps its deadline, so a
// half that calls it at its own first failure does not get a fresh window.
class RetryWindow {
public:
    explicit RetryWindow(std::chrono::milliseconds window);
    void start(std::chrono::steady_clock::time_point now);
    [[nodiscard]] bool started() const noexcept { return started_; }
    // True when a wait of `wait` from `now` still ends before the deadline.
    // A window that has not started measures from `now`, as if it started
    // there.
    [[nodiscard]] bool allows(std::chrono::steady_clock::time_point now,
                              std::chrono::milliseconds wait) const;
    // Precondition: started(). The deadline a later attempt's reads run
    // under. A window that has not started returns the clock's epoch.
    [[nodiscard]] std::chrono::steady_clock::time_point deadline() const noexcept {
        return deadline_;
    }

private:
    std::chrono::milliseconds window_;
    std::chrono::steady_clock::time_point deadline_{};
    bool started_{false};
};

// Waits `d` in slices of at most 50 ms. Returns false as soon as `cancel`
// reports a cancel or `stop` is set, including before the first slice and
// after the last; true when the whole wait passed without either.
[[nodiscard]] bool cancellable_wait(std::chrono::milliseconds d,
                                    const CancelSignal& cancel,
                                    const std::atomic<bool>& stop);

}  // namespace clink::clickhouse::native
