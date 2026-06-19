#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <future>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "clink/operators/operator_base.hpp"

namespace clink {

// Retry policy applied per-record when the user's async function
// throws. Mirrors AsyncRetryStrategy: cap on attempts plus a
// (possibly exponential) backoff between them. attempts=1 means no
// retry - the failing exception is propagated, matching the historic
// behavior of AsyncMapOperator before this struct existed.
struct AsyncRetryStrategy {
    // Total number of attempts including the first. 1 = no retry.
    int max_attempts{1};
    // Wait this long before the second attempt (so before the first
    // retry). Subsequent waits multiply by backoff_multiplier.
    std::chrono::milliseconds initial_backoff{0};
    // 1.0 = fixed delay, 2.0 = exponential doubling, etc.
    double backoff_multiplier{1.0};
    // Cap on any single backoff, applied after multiplication.
    std::chrono::milliseconds max_backoff{std::chrono::milliseconds{60000}};
    // Optional predicate. nullptr = retry on every exception. Return
    // true to retry, false to surface immediately even if attempts
    // remain. Mirrors AsyncRetryStrategy::canRetry in .
    std::function<bool(const std::exception&)> should_retry{nullptr};
};

// Async-map operator - RichAsyncFunction analogue.
//
// The user supplies a (synchronous-style) callable In -> Out that
// performs slow work (HTTP, DB, S3 lookup). The framework runs it
// on a fixed-size worker pool so multiple records are in flight
// concurrently, and emits results in INPUT ORDER. That order
// guarantee matches "ordered" output mode, the safe default
// - operators downstream that key by some field implicit in In's
// position still see records in the order the source produced them.
//
// Backpressure: process() blocks once max_in_flight pending records
// would be queued, so a slow lookup naturally throttles the upstream
// runner. Watermarks and checkpoint barriers wait until every
// in-flight call has completed before they propagate downstream;
// this preserves exactly-once with operator-side state.
//
// Retry: when fn_ throws, the worker re-invokes it according to
// retry_ (a AsyncRetryStrategy). The retry happens
// on the SAME worker thread, holding the slot's input-order
// position so downstream order is preserved across retry. The
// per-attempt backoff sleeps inside the worker; other workers keep
// servicing other records so per-record retry does not stall the
// pipeline as a whole.
//
// Lifecycle: open() spawns the worker threads, close() drains and
// joins them. The user fn should not capture mutable state without
// its own locking - different records may run on different workers.
template <typename In, typename Out>
class AsyncMapOperator final : public Operator<In, Out> {
public:
    using Func = std::function<Out(const In&)>;

    AsyncMapOperator(Func fn,
                     std::size_t worker_count = 4,
                     std::size_t max_in_flight = 64,
                     std::string name = "async_map")
        : fn_(std::move(fn)),
          worker_count_(worker_count == 0 ? 1 : worker_count),
          max_in_flight_(max_in_flight == 0 ? worker_count_ : max_in_flight),
          name_(std::move(name)) {}

    // Same as above with a retry strategy. Calling set_retry_strategy
    // after open() has no effect on in-flight records; set it during
    // construction or before exec.run().
    AsyncMapOperator(Func fn,
                     AsyncRetryStrategy retry,
                     std::size_t worker_count = 4,
                     std::size_t max_in_flight = 64,
                     std::string name = "async_map")
        : fn_(std::move(fn)),
          retry_(std::move(retry)),
          worker_count_(worker_count == 0 ? 1 : worker_count),
          max_in_flight_(max_in_flight == 0 ? worker_count_ : max_in_flight),
          name_(std::move(name)) {}

    void set_retry_strategy(AsyncRetryStrategy retry) { retry_ = std::move(retry); }

    void open() override {
        stop_.store(false, std::memory_order_relaxed);
        workers_.reserve(worker_count_);
        for (std::size_t i = 0; i < worker_count_; ++i) {
            workers_.emplace_back([this] { worker_loop_(); });
        }
    }

    void close() override {
        {
            std::lock_guard lock(mu_);
            stop_.store(true, std::memory_order_relaxed);
        }
        not_empty_.notify_all();
        for (auto& w : workers_) {
            if (w.joinable()) {
                w.join();
            }
        }
        workers_.clear();
    }

    void process(const StreamElement<In>& element, Emitter<Out>& out) override {
        if (element.is_data()) {
            for (const auto& record : element.as_data()) {
                submit_record_(record);
            }
            drain_completed_(out);
        } else if (element.is_watermark()) {
            wait_for_all_in_flight_(out);
            this->on_watermark(element.as_watermark(), out);
        } else {
            wait_for_all_in_flight_(out);
            this->on_barrier(element.as_barrier(), out);
        }
    }

    void flush(Emitter<Out>& out) override { wait_for_all_in_flight_(out); }

    std::string name() const override { return name_; }

private:
    // One in-flight record. Slots stay in the queue in input order
    // until their `result` is set; at that point drain_completed_
    // pops them from the front and emits, preserving the order
    // guarantee even when worker B finishes before worker A.
    struct Slot {
        Record<In> input;
        std::promise<Out> promise;
        std::future<Out> result;
        bool result_ready{false};
        std::optional<Out> value;
    };

    void submit_record_(const Record<In>& record) {
        std::unique_lock lock(mu_);
        slot_available_.wait(lock, [this] {
            return pending_input_.size() + in_flight_.size() < max_in_flight_ ||
                   stop_.load(std::memory_order_relaxed);
        });
        if (stop_.load(std::memory_order_relaxed)) {
            return;
        }
        auto slot = std::make_shared<Slot>();
        slot->input = record;
        slot->result = slot->promise.get_future();
        ordered_.push_back(slot);
        pending_input_.push_back(slot);
        not_empty_.notify_one();
    }

    void worker_loop_() {
        for (;;) {
            std::shared_ptr<Slot> slot;
            {
                std::unique_lock lock(mu_);
                not_empty_.wait(lock, [this] {
                    return !pending_input_.empty() || stop_.load(std::memory_order_relaxed);
                });
                if (stop_.load(std::memory_order_relaxed) && pending_input_.empty()) {
                    return;
                }
                slot = pending_input_.front();
                pending_input_.pop_front();
                in_flight_.push_back(slot);
            }
            run_with_retry_(slot);
            {
                std::lock_guard lock(mu_);
                std::erase(in_flight_, slot);
            }
            completed_.notify_all();
            slot_available_.notify_one();
        }
    }

    // Pop every slot from the front of `ordered_` whose future is
    // already ready and emit. Stops at the first not-ready slot to
    // preserve input order downstream.
    void drain_completed_(Emitter<Out>& out) {
        std::vector<std::shared_ptr<Slot>> to_emit;
        {
            std::lock_guard lock(mu_);
            while (!ordered_.empty()) {
                auto& front = ordered_.front();
                if (front->result.wait_for(std::chrono::seconds{0}) != std::future_status::ready) {
                    break;
                }
                to_emit.push_back(front);
                ordered_.pop_front();
            }
        }
        for (auto& slot : to_emit) {
            emit_slot_(slot, out);
        }
        if (!to_emit.empty()) {
            slot_available_.notify_all();
        }
    }

    // Block until `ordered_` drains completely. Called before
    // forwarding a watermark or barrier so downstream sees those
    // signals after every preceding record's async result.
    void wait_for_all_in_flight_(Emitter<Out>& out) {
        for (;;) {
            std::vector<std::shared_ptr<Slot>> to_emit;
            std::shared_ptr<Slot> pending_front;
            {
                std::unique_lock lock(mu_);
                while (!ordered_.empty()) {
                    auto& front = ordered_.front();
                    if (front->result.wait_for(std::chrono::seconds{0}) ==
                        std::future_status::ready) {
                        to_emit.push_back(front);
                        ordered_.pop_front();
                        continue;
                    }
                    pending_front = front;
                    break;
                }
            }
            for (auto& slot : to_emit) {
                emit_slot_(slot, out);
            }
            if (!to_emit.empty()) {
                slot_available_.notify_all();
            }
            if (!pending_front) {
                return;  // queue empty - all in-flight drained
            }
            pending_front->result.wait();
        }
    }

    // Execute fn_ for `slot` honoring retry_. Total attempts capped
    // at retry_.max_attempts (at least 1). Backoff sleeps between
    // attempts; the slot stays in input-order position so retried
    // records cannot pass undelayed siblings downstream.
    void run_with_retry_(std::shared_ptr<Slot>& slot) {
        const int max_attempts = std::max(retry_.max_attempts, 1);
        auto backoff = retry_.initial_backoff;
        std::exception_ptr last_exc;
        int attempts_done = 0;
        for (int attempt = 1; attempt <= max_attempts; ++attempt) {
            attempts_done = attempt;
            try {
                slot->promise.set_value(fn_(slot->input.value()));
                if (attempt > 1) {
                    retries_observed_.fetch_add(static_cast<std::uint64_t>(attempt - 1),
                                                std::memory_order_relaxed);
                }
                return;
            } catch (const std::exception& e) {
                last_exc = std::current_exception();
                if (attempt == max_attempts) {
                    break;
                }
                if (retry_.should_retry && !retry_.should_retry(e)) {
                    break;
                }
            } catch (...) {
                last_exc = std::current_exception();
                // No std::exception payload to feed should_retry -
                // surface unconditionally rather than guess.
                break;
            }
            if (backoff.count() > 0) {
                std::this_thread::sleep_for(backoff);
            }
            // Exponentiate for next attempt; saturate at max_backoff.
            if (retry_.backoff_multiplier > 1.0) {
                const auto next = std::chrono::milliseconds{static_cast<long>(
                    static_cast<double>(backoff.count()) * retry_.backoff_multiplier)};
                backoff = std::min(next, retry_.max_backoff);
            }
        }
        if (attempts_done > 1) {
            retries_observed_.fetch_add(static_cast<std::uint64_t>(attempts_done - 1),
                                        std::memory_order_relaxed);
        }
        slot->promise.set_exception(last_exc);
    }

    void emit_slot_(std::shared_ptr<Slot>& slot, Emitter<Out>& out) {
        try {
            auto value = slot->result.get();
            Batch<Out> b;
            if (slot->input.event_time().has_value()) {
                b.emplace(std::move(value), *slot->input.event_time());
            } else {
                b.emplace(std::move(value));
            }
            out.emit_data(std::move(b));
        } catch (...) {
            // User function threw. v1 surfaces this by re-throwing on
            // the operator thread, where the runner converts it into
            // SubtaskFinished{had_error=true}. A future slice could
            // add a retry / dead-letter side output a la // AsyncRetryStrategy.
            throw;
        }
    }

public:
    // Exposed for tests + metrics: total number of retry attempts
    // performed by this operator instance (i.e. excludes the initial
    // attempt). Increments atomically across workers.
    std::uint64_t retries_observed() const {
        return retries_observed_.load(std::memory_order_relaxed);
    }

private:
    Func fn_;
    AsyncRetryStrategy retry_{};
    std::atomic<std::uint64_t> retries_observed_{0};
    std::size_t worker_count_;
    std::size_t max_in_flight_;
    std::string name_;

    std::atomic<bool> stop_{false};
    std::mutex mu_;
    std::condition_variable not_empty_;       // wakes workers
    std::condition_variable completed_;       // unused for now; reserved
    std::condition_variable slot_available_;  // wakes submit_record_

    // ordered_ owns the canonical input order; pending_input_ is the
    // worker queue; in_flight_ is the set currently being computed.
    // A slot lives in (at most) two of these at once: ordered + one
    // of pending_input_ / in_flight_. Drained slots are gone.
    std::deque<std::shared_ptr<Slot>> ordered_;
    std::deque<std::shared_ptr<Slot>> pending_input_;
    std::vector<std::shared_ptr<Slot>> in_flight_;
    std::vector<std::thread> workers_;
};

}  // namespace clink
