#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdio>
#include <deque>
#include <mutex>
#include <optional>
#include <string>
#include <utility>

namespace clink {

// A simple, thread-safe, bounded MPMC channel.
//
// This is the unit of backpressure: when the channel is full, push() blocks
// (or returns false in try_push) which propagates pressure upstream. Producers
// and consumers can be on different threads.
//
// Closing the channel is one-way; once closed pop() returns nullopt after the
// queue drains.
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

    // Blocking push. Returns false if the channel was closed before the value
    // could be enqueued.
    bool push(T value) {
        std::unique_lock lock(mu_);
        if (queue_.size() >= capacity_ && !closed_) {
            ++push_waiters_;
            // Periodic stuck-warning: every kStuckWarnInterval seconds
            // a still-blocked push prints (name, size/capacity, waiters)
            // to stderr. Logs land in clink_tm.log naturally; greppable
            // signature is "BOUNDED_CHANNEL_STUCK". Used for backpressure-
            // deadlock diagnosis - works across the RTLD_LOCAL plugin
            // boundary because each .so logs its own channels through
            // its own stderr, and clink_node forwards both streams.
            using clock = std::chrono::steady_clock;
            const auto start = clock::now();
            auto next_warn = start + std::chrono::seconds{kStuckWarnInterval};
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
                    next_warn += std::chrono::seconds{kStuckWarnInterval};
                }
            }
            --push_waiters_;
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
        if (queue_.empty() && !closed_) {
            ++pop_waiters_;
            // See push() for the stuck-warning rationale.
            using clock = std::chrono::steady_clock;
            const auto start = clock::now();
            auto next_warn = start + std::chrono::seconds{kStuckWarnInterval};
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
                    next_warn += std::chrono::seconds{kStuckWarnInterval};
                }
            }
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

    void close() {
        {
            std::lock_guard lock(mu_);
            closed_ = true;
        }
        not_empty_.notify_all();
        not_full_.notify_all();
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
    static constexpr int kStuckWarnInterval = 3;

    mutable std::mutex mu_;
    std::condition_variable not_full_;
    std::condition_variable not_empty_;
    std::deque<T> queue_;
    std::size_t capacity_{};
    std::string name_;
    bool closed_{false};
    int push_waiters_{0};
    int pop_waiters_{0};
    std::atomic<std::size_t> max_depth_{0};
};

}  // namespace clink
