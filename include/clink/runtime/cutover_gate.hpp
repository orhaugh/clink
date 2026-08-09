#pragma once

// Per-output-group cutover gate (hot rescale, design record 008).
//
// One instance coordinates one output group's hold-and-swap across the three
// parties that share it inside a single task process:
//
//   * the SPLIT stage routing records to the group's branches - it holds
//     after broadcasting the armed barrier C, and resumes with the new live
//     branch count once the swap completes;
//   * the group's branch SINKS - each reports when it has pushed C to its
//     peer, which is the moment its outbound stream is exactly "everything
//     up to C";
//   * the WORKER control path - it arms the gate when BeginRescale names a
//     cutover checkpoint, performs the endpoint swaps once every branch has
//     flushed (the split is held and the branch queues are drained, so
//     nothing races the swap), and releases.
//
// The contract the parties rely on: after `wait_released` returns normally,
// every branch sink points at the post-cutover peer set and `live` is the
// new branch count; nothing was routed anywhere between the barrier
// broadcast and that moment. Abort (job cancel, rescale abort) releases the
// hold with `aborted` set so the split can wind down instead of resuming.
//
// No sleeps: every wait is a condition. The waits take a should_stop
// probe and re-check it on a bounded slice, because cancellation sets an
// external flag without notifying this cv.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>

namespace clink {

class GroupCutoverGate {
public:
    explicit GroupCutoverGate(std::uint32_t initial_live_branches) : live_(initial_live_branches) {}

    // --- worker control side -------------------------------------------

    // Arm for cutover checkpoint C. The split holds after broadcasting C;
    // branch sinks report flushes for C. Re-arming while armed is refused
    // (one cutover at a time per group).
    bool arm(std::uint64_t cutover_checkpoint_id) {
        std::lock_guard lock(mu_);
        if (armed_ != 0 || cutover_checkpoint_id == 0) {
            return false;
        }
        armed_ = cutover_checkpoint_id;
        flushed_ = 0;
        released_ = false;
        return true;
    }

    // Block until every branch sink has pushed barrier C to its peer. The
    // worker calls this before swapping endpoints: a sink that has not yet
    // flushed may still be pushing pre-C elements, and swapping under it
    // would send them to the wrong peer. False on timeout or stop.
    [[nodiscard]] bool await_all_flushed(std::uint32_t branch_count,
                                         std::chrono::milliseconds timeout,
                                         const std::function<bool()>& should_stop) {
        return timed_wait_(timeout, should_stop, [&] { return flushed_ >= branch_count; });
    }

    // Swap is done: install the new live branch count and wake the split.
    void release(std::uint32_t new_live_branches) {
        {
            std::lock_guard lock(mu_);
            live_.store(new_live_branches, std::memory_order_release);
            released_ = true;
            armed_ = 0;
        }
        cv_.notify_all();
    }

    // Abandon the cutover (cancel, rescale abort). Wakes every waiter;
    // the split resumes tearing down rather than routing.
    void abort() {
        {
            std::lock_guard lock(mu_);
            aborted_ = true;
        }
        cv_.notify_all();
    }

    // --- split side ------------------------------------------------------

    // Is this barrier the armed cutover? The split checks after
    // broadcasting each barrier; equality only, same as the runner arms.
    [[nodiscard]] bool is_armed_for(std::uint64_t barrier_id) const {
        std::lock_guard lock(mu_);
        return armed_ != 0 && armed_ == barrier_id;
    }

    // Hold until the swap completes (true) or the gate aborts / the stop
    // probe fires / the bound expires (false). The split calls this after
    // broadcasting the armed barrier, so no record can be routed with the
    // old divisor once C has passed.
    [[nodiscard]] bool wait_released(std::chrono::milliseconds timeout,
                                     const std::function<bool()>& should_stop) {
        return timed_wait_(timeout, should_stop, [&] { return released_; });
    }

    // The selector's divisor: the number of live branches records route
    // across. Reads are lock-free on the routing hot path.
    [[nodiscard]] std::uint32_t live() const noexcept {
        return live_.load(std::memory_order_acquire);
    }

    // --- branch sink side -------------------------------------------------

    // A branch sink has pushed barrier C to its peer (or swallowed it while
    // parked): its outbound stream is exactly "everything up to C".
    void mark_branch_flushed(std::uint64_t barrier_id) {
        {
            std::lock_guard lock(mu_);
            if (armed_ == 0 || armed_ != barrier_id) {
                return;
            }
            ++flushed_;
        }
        cv_.notify_all();
    }

    [[nodiscard]] std::uint32_t flushed_count() const {
        std::lock_guard lock(mu_);
        return flushed_;
    }
    [[nodiscard]] bool aborted() const {
        std::lock_guard lock(mu_);
        return aborted_;
    }

private:
    template <typename Pred>
    [[nodiscard]] bool timed_wait_(std::chrono::milliseconds timeout,
                                   const std::function<bool()>& should_stop,
                                   Pred done) {
        auto stopped = [&] { return aborted_ || (should_stop && should_stop()); };
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        std::unique_lock lock(mu_);
        while (!done()) {
            if (stopped()) {
                return false;
            }
            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline) {
                return false;
            }
            // Sliced so an external stop (which does not notify this cv)
            // is noticed within one slice, not at the full bound.
            const auto slice = std::min<std::chrono::steady_clock::duration>(
                std::chrono::milliseconds{250}, deadline - now);
            cv_.wait_for(lock, slice, [&] { return done() || stopped(); });
        }
        return true;
    }

    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::uint64_t armed_{0};
    std::uint32_t flushed_{0};
    bool released_{false};
    bool aborted_{false};
    std::atomic<std::uint32_t> live_;
};

}  // namespace clink
