#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <deque>
#include <mutex>
#include <optional>
#include <string>
#include <utility>

namespace clink {

// Why a channel closed. The distinction is load-bearing for event time
// (followups item 79): a FINISHED input has genuinely ended - its max
// watermark already flowed, and downstream alignment may stop letting it
// constrain the running minimum - while a CANCELLED close is teardown,
// where reading end-of-input would advance downstream time to end-of-time
// and fire every open window into a still-live sink. QUAL-07 measured
// exactly that: a cancelled job appended a nondeterministic partial tail
// of open cumulate panes, correct-valued and premature.
enum class ChannelCloseReason : std::uint8_t {
    Finished = 0,   // clean end-of-stream: the producer ran out of input
    Cancelled = 1,  // teardown: cancel, failover, shutdown - NOT end-of-input
};

// A simple, thread-safe, bounded MPMC channel.
//
// This is the unit of backpressure: when the channel is full, push() blocks
// (or returns false in try_push) which propagates pressure upstream. Producers
// and consumers can be on different threads.
//
// Closing the channel is one-way; once closed pop() returns nullopt after the
// queue drains. The first close's reason wins (a Finished close racing a
// teardown close keeps whichever landed first - by then the consumer's
// behaviour is already decided).
template <typename T>
class BoundedChannel {
public:
    explicit BoundedChannel(std::size_t capacity, std::string name = {})
        : capacity_(capacity), name_(std::move(name)) {}

    BoundedChannel(const BoundedChannel&) = delete;
    BoundedChannel& operator=(const BoundedChannel&) = delete;
    BoundedChannel(BoundedChannel&&) = delete;
    BoundedChannel& operator=(BoundedChannel&&) = delete;

    void set_name(std::string name) {
        std::lock_guard lock(mu_);
        name_ = std::move(name);
    }

    // Declare that an idle pop on this channel is normal. Some consumers wait
    // an unbounded time for their next item by design - the snapshot worker
    // sits between checkpoints - and a long empty wait there is not the stall
    // the BOUNDED_CHANNEL_STUCK diagnostic exists to catch. It printed one
    // regardless: a worker with nothing to do logged "STUCK" in capitals every
    // few seconds, escalating to held=189s, which is the first thing a new
    // user reads in an idle worker's log. Push-side warnings are unaffected:
    // a producer blocked on a full channel is still a stall worth naming.
    void mark_idle_pop_normal() {
        std::lock_guard lock(mu_);
        warn_on_idle_pop_ = false;
    }

    // The spacing between stuck-warnings for one continuous wait: double the
    // previous gap, capped at five minutes. The first QUAL-06 width probe
    // filled three workers' ENTIRE 200,000-line log windows with one line per
    // channel per 3s tick - the diagnostic rotated out the task starts and
    // bridge errors that would have named the stall it existed to diagnose
    // (item 76). With this schedule a 40-minute stall costs ~12 lines, each
    // still carrying the cumulative held= time, so nothing about the stall is
    // lost - only the repetition.
    static std::chrono::milliseconds stuck_warn_backoff(std::chrono::milliseconds previous) {
        return std::min(previous * 2, std::chrono::milliseconds{kStuckWarnCapSeconds * 1000});
    }
    static constexpr int kStuckWarnCapSeconds = 300;

    // Test seam: the schedule is real time, and a test proving two warns at
    // the default base would take nine seconds of it.
    void set_stuck_warn_base_for_testing(std::chrono::milliseconds base) {
        std::lock_guard lock(mu_);
        stuck_warn_base_ = base;
    }

    // Blocking push. Returns false if the channel was closed before the value
    // could be enqueued.
    bool push(T value) {
        std::unique_lock lock(mu_);
        if (queue_.size() >= capacity_ && !closed_) {
            ++push_waiters_;
            // Periodic stuck-warning: every kStuckWarnInterval seconds
            // a still-blocked push prints (name, size/capacity, waiters)
            // to stderr. Logs land in clink_worker.log naturally; greppable
            // signature is "BOUNDED_CHANNEL_STUCK". Used for backpressure-
            // deadlock diagnosis - works across the RTLD_LOCAL plugin
            // boundary because each .so logs its own channels through
            // its own stderr, and clink_node forwards both streams.
            using clock = std::chrono::steady_clock;
            const auto start = clock::now();
            auto gap = stuck_warn_base_;
            auto next_warn = start + gap;
            int warns = 0;
            while (queue_.size() >= capacity_ && !closed_) {
                if (not_full_.wait_until(lock, next_warn) == std::cv_status::timeout) {
                    const auto held =
                        std::chrono::duration_cast<std::chrono::seconds>(clock::now() - start)
                            .count();
                    std::fprintf(stderr,
                                 "BOUNDED_CHANNEL_STUCK push name=\"%s\" ch=%p size=%zu cap=%zu "
                                 "push_waiters=%d pop_waiters=%d held=%llds\n",
                                 name_.c_str(),
                                 static_cast<const void*>(this),
                                 queue_.size(),
                                 capacity_,
                                 push_waiters_,
                                 pop_waiters_,
                                 static_cast<long long>(held));
                    ++warns;
                    gap = stuck_warn_backoff(gap);
                    next_warn += gap;
                }
            }
            --push_waiters_;
            if (warns > 0) {
                // The other half of the diagnosis: a wait that warned and then
                // moved is a stall that CLEARED, and when it cleared is often
                // the fact that names the mechanism. One line per warned wait.
                const auto held =
                    std::chrono::duration_cast<std::chrono::seconds>(clock::now() - start).count();
                std::fprintf(stderr,
                             "BOUNDED_CHANNEL_UNBLOCKED push name=\"%s\" ch=%p held=%llds "
                             "warns=%d closed=%d\n",
                             name_.c_str(),
                             static_cast<const void*>(this),
                             static_cast<long long>(held),
                             warns,
                             closed_ ? 1 : 0);
            }
        }
        if (closed_) {
            return false;
        }
        queue_.push_back(std::move(value));
        max_depth_.store(std::max(max_depth_.load(), queue_.size()), std::memory_order_relaxed);
        lock.unlock();
        not_empty_.notify_one();
        return true;
    }

    // Non-blocking push. Returns true on success, false if the channel was full
    // or closed.
    bool try_push(T value) {
        std::unique_lock lock(mu_);
        if (closed_ || queue_.size() >= capacity_) {
            return false;
        }
        queue_.push_back(std::move(value));
        max_depth_.store(std::max(max_depth_.load(), queue_.size()), std::memory_order_relaxed);
        lock.unlock();
        not_empty_.notify_one();
        return true;
    }

    // Blocking pop. Returns nullopt only when the channel is closed AND empty.
    std::optional<T> pop() {
        std::unique_lock lock(mu_);
        if (queue_.empty() && !closed_ && !warn_on_idle_pop_) {
            // Idle by design (mark_idle_pop_normal): a plain wait, no diagnostic.
            ++pop_waiters_;
            not_empty_.wait(lock, [this] { return !queue_.empty() || closed_; });
            --pop_waiters_;
        } else if (queue_.empty() && !closed_) {
            ++pop_waiters_;
            // See push() for the stuck-warning rationale.
            using clock = std::chrono::steady_clock;
            const auto start = clock::now();
            auto gap = stuck_warn_base_;
            auto next_warn = start + gap;
            int warns = 0;
            while (queue_.empty() && !closed_) {
                if (not_empty_.wait_until(lock, next_warn) == std::cv_status::timeout) {
                    const auto held =
                        std::chrono::duration_cast<std::chrono::seconds>(clock::now() - start)
                            .count();
                    std::fprintf(stderr,
                                 "BOUNDED_CHANNEL_STUCK pop  name=\"%s\" ch=%p size=%zu cap=%zu "
                                 "push_waiters=%d pop_waiters=%d held=%llds\n",
                                 name_.c_str(),
                                 static_cast<const void*>(this),
                                 queue_.size(),
                                 capacity_,
                                 push_waiters_,
                                 pop_waiters_,
                                 static_cast<long long>(held));
                    ++warns;
                    gap = stuck_warn_backoff(gap);
                    next_warn += gap;
                }
            }
            --pop_waiters_;
            if (warns > 0) {
                const auto held =
                    std::chrono::duration_cast<std::chrono::seconds>(clock::now() - start).count();
                std::fprintf(stderr,
                             "BOUNDED_CHANNEL_UNBLOCKED pop  name=\"%s\" ch=%p held=%llds "
                             "warns=%d closed=%d\n",
                             name_.c_str(),
                             static_cast<const void*>(this),
                             static_cast<long long>(held),
                             warns,
                             closed_ ? 1 : 0);
            }
        }
        if (queue_.empty()) {
            return std::nullopt;
        }
        T value = std::move(queue_.front());
        queue_.pop_front();
        lock.unlock();
        not_full_.notify_one();
        return value;
    }

    // Non-blocking pop. Returns nullopt if empty (whether closed or not).
    std::optional<T> try_pop() {
        std::unique_lock lock(mu_);
        if (queue_.empty()) {
            return std::nullopt;
        }
        T value = std::move(queue_.front());
        queue_.pop_front();
        lock.unlock();
        not_full_.notify_one();
        return value;
    }

    // Pop with a timeout. Returns the element if one became available
    // before the timeout, otherwise nullopt. Caller can distinguish
    // "timed out" from "closed and drained" via closed() + empty.
    template <typename Rep, typename Period>
    std::optional<T> pop_for(std::chrono::duration<Rep, Period> timeout) {
        std::unique_lock lock(mu_);
        if (queue_.empty() && !closed_) {
            ++pop_waiters_;
            not_empty_.wait_for(lock, timeout, [this] { return !queue_.empty() || closed_; });
            --pop_waiters_;
        }
        if (queue_.empty()) {
            return std::nullopt;
        }
        T value = std::move(queue_.front());
        queue_.pop_front();
        lock.unlock();
        not_full_.notify_one();
        return value;
    }

    void close(ChannelCloseReason reason = ChannelCloseReason::Finished) {
        {
            std::lock_guard lock(mu_);
            if (!closed_) {
                close_reason_ = reason;
            }
            closed_ = true;
        }
        not_empty_.notify_all();
        not_full_.notify_all();
    }

    // Meaningful once closed(); Finished until then.
    [[nodiscard]] ChannelCloseReason close_reason() const {
        std::lock_guard lock(mu_);
        return close_reason_;
    }
    [[nodiscard]] bool close_cancelled() const {
        std::lock_guard lock(mu_);
        return closed_ && close_reason_ == ChannelCloseReason::Cancelled;
    }

    std::size_t size() const {
        std::lock_guard lock(mu_);
        return queue_.size();
    }

    std::size_t capacity() const noexcept { return capacity_; }
    std::size_t high_water_mark() const noexcept {
        return max_depth_.load(std::memory_order_relaxed);
    }

    bool closed() const {
        std::lock_guard lock(mu_);
        return closed_;
    }

    [[nodiscard]] const std::string& name() const noexcept { return name_; }

private:
    // Seconds before a still-blocked push/pop logs a stuck-warning.
    // Picked > the longest legitimate backpressure stall we expect
    // under steady-state load so normal slow consumers don't spam.
    // Seconds before a still-blocked push/pop logs its FIRST stuck-warning.
    // Picked > the longest legitimate backpressure stall we expect under
    // steady-state load so normal slow consumers don't spam.
    static constexpr int kStuckWarnInterval = 3;

    mutable std::mutex mu_;
    std::condition_variable not_full_;
    std::condition_variable not_empty_;
    std::deque<T> queue_;
    std::size_t capacity_{};
    std::string name_;
    std::chrono::milliseconds stuck_warn_base_{kStuckWarnInterval * 1000};
    bool warn_on_idle_pop_{true};
    bool closed_{false};
    ChannelCloseReason close_reason_{ChannelCloseReason::Finished};
    int push_waiters_{0};
    int pop_waiters_{0};
    std::atomic<std::size_t> max_depth_{0};
};

}  // namespace clink
