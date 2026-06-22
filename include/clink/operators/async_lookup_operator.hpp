#pragma once

// AsyncLookupOperator<In, Out>
//
// Phase 28b - coroutine-shaped analogue of AsyncMapOperator. Where
// AsyncMapOperator takes `std::function<Out(const In&)>` and runs it
// on a worker-thread pool, AsyncLookupOperator takes
// `std::function<async::Task<Out>(const In&)>` and drives the
// returned coroutine forward on the operator's own thread. The user
// expresses concurrency through `co_await` inside the lookup body
// (e.g. `co_await http_pool.get(...)`); the operator drives any
// number of in-flight coroutines, polling each one's done() state
// and emitting completed results in input order.
//
// Why not just reuse AsyncMapOperator? Two practical wins:
//   1. No worker threads. A lookup that mostly waits for I/O
//      shouldn't pin a thread; with a coroutine + io_uring/HTTP-pool
//      (28d/e) the operator scales to 1000s of concurrent lookups
//      on one thread.
//   2. Natural composition. `co_await another_lookup()` is just
//      another awaitable; cascading async work doesn't need
//      `then()`-style continuation chaining.
//
// Scheduling model for 28b:
//   - process(data): for each record, kick off lookup_fn(record),
//     get back a Task<Out>, resume() once. If the task completes in
//     a single step (synchronous lookup), the result is captured
//     immediately. If the task suspends on a co_await, the slot
//     waits for an external signal (the awaitable's continuation
//     mechanism) to resume.
//   - drain(): emit completed tasks from the head of the ordered
//     queue. Stops at the first not-done task to preserve input
//     order. Unordered emit (out_of_order) emits any done task.
//   - on_watermark / on_barrier: drain everything in-flight. For
//     synchronous-completing tasks this is immediate; for tasks
//     suspended on real I/O, this currently spin-waits with a
//     short sleep + resume() to let the user's awaitable advance.
//     A proper async-aware barrier is part of 28e.
//   - max_in_flight: process() blocks once max_in_flight tasks are
//     queued so a slow lookup naturally throttles upstream.
//
// Exception handling: a throwing Task<Out> body is captured on the
// promise (see clink::async::Task). emit_slot_() calls Task::get()
// which rethrows; the runner sees the throw and treats it the same
// as any other operator failure (typically: log + abort the job).
// A user-configurable retry policy is left for a follow-on slice.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>

#include "clink/async/task.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink {

template <typename In, typename Out>
class AsyncLookupOperator final : public Operator<In, Out> {
public:
    using LookupFn = std::function<async::Task<Out>(const In&)>;

    AsyncLookupOperator(LookupFn fn,
                        std::size_t max_in_flight = 64,
                        bool ordered = true,
                        std::string name = "async_lookup")
        : fn_(std::move(fn)),
          max_in_flight_(max_in_flight == 0 ? 1 : max_in_flight),
          ordered_(ordered),
          name_(std::move(name)) {}

    void process(const StreamElement<In>& element, Emitter<Out>& out) override {
        if (element.is_data()) {
            for (const auto& record : element.as_data()) {
                // Backpressure: if we're at capacity, drain whatever
                // is ready, then if still full nudge the suspended
                // tasks with resume() and sleep briefly. For purely
                // synchronous lookups the drain after kickoff brings
                // us under capacity in one shot.
                while (in_flight_.size() >= max_in_flight_) {
                    drain_(out);
                    if (in_flight_.size() < max_in_flight_)
                        break;
                    nudge_in_flight_();
                    std::this_thread::sleep_for(std::chrono::microseconds{100});
                }
                kickoff_(record.value());
            }
            drain_(out);
        } else if (element.is_watermark()) {
            wait_for_all_(out);
            this->on_watermark(element.as_watermark(), out);
        } else {
            wait_for_all_(out);
            this->on_barrier(element.as_barrier(), out);
        }
    }

    // EOS hook: drain anything left in-flight before the runner
    // tears the operator down. Mirrors AsyncMapOperator::flush.
    void flush(Emitter<Out>& out) override { wait_for_all_(out); }

    std::string name() const override { return name_; }

    // Test/diagnostic accessors. Not part of the public Operator
    // contract but useful for UTs to observe scheduling behaviour
    // without having to drive the input channel.
    [[nodiscard]] std::size_t in_flight_count() const noexcept { return in_flight_.size(); }
    [[nodiscard]] std::size_t emitted_count() const noexcept { return emitted_count_; }

private:
    // One in-flight lookup. Stays in the ordered queue until the
    // Task completes and emit_slot_() pulls its result. Slots are
    // owned by shared_ptr so we can transition them between queues
    // without invalidating the underlying coroutine handle.
    struct Slot {
        async::Task<Out> task;
        Slot() = default;
        explicit Slot(async::Task<Out> t) : task(std::move(t)) {}
    };

    void kickoff_(const In& record) {
        // Build the lookup coroutine. fn_ returns a Task<Out>; we
        // resume() it once to advance past initial_suspend
        // (lazy-start) into the body. A synchronous body finishes
        // here; an async body suspends on its first co_await.
        auto slot = std::make_shared<Slot>(fn_(record));
        slot->task.resume();
        in_flight_.push_back(std::move(slot));
    }

    // Drain whatever is ready. Ordered emit pops from the head of
    // in_flight_ while the head is done; stops at the first not-
    // done task to preserve input order. Unordered emit walks the
    // whole queue and emits everything ready.
    void drain_(Emitter<Out>& out) {
        if (ordered_) {
            while (!in_flight_.empty() && in_flight_.front()->task.done()) {
                auto slot = std::move(in_flight_.front());
                in_flight_.pop_front();
                emit_slot_(*slot, out);
            }
        } else {
            // Walk-and-rebuild: emit done slots, keep the rest in
            // their relative order.
            std::deque<std::shared_ptr<Slot>> kept;
            for (auto& slot : in_flight_) {
                if (slot->task.done()) {
                    emit_slot_(*slot, out);
                } else {
                    kept.push_back(std::move(slot));
                }
            }
            in_flight_ = std::move(kept);
        }
    }

    // Nudge every in-flight task. For coroutines whose first
    // co_await is a still-pending awaitable, resume() is a no-op
    // (the coroutine is suspended; only the awaitable's signal can
    // advance it). For tasks whose awaitable has been signalled
    // out-of-band, the next resume() drives them to the next
    // suspension or to completion. Combined with the small sleep
    // in process()'s backpressure loop and wait_for_all_, this
    // gives async awaitables time to deliver their continuation.
    void nudge_in_flight_() {
        for (auto& slot : in_flight_) {
            if (!slot->task.done()) {
                slot->task.resume();
            }
        }
    }

    // Block until every in-flight task is done, then drain. Used
    // on barriers and watermarks where downstream needs every
    // pending result delivered before the marker propagates. For
    // tests with synchronous lookup bodies the first drain_ empties
    // the queue; for async I/O bodies the spin-wait gives the
    // backend a chance to deliver completions.
    void wait_for_all_(Emitter<Out>& out) {
        while (!in_flight_.empty()) {
            drain_(out);
            if (in_flight_.empty())
                break;
            nudge_in_flight_();
            std::this_thread::sleep_for(std::chrono::microseconds{100});
        }
    }

    void emit_slot_(Slot& slot, Emitter<Out>& out) {
        Batch<Out> b;
        b.emplace(slot.task.get());
        out.emit_data(std::move(b));
        ++emitted_count_;
        // Phase 30d (metrics-coverage pass): every completed slot
        // counts as one async-lookup completion. The hit/miss
        // distinction lives in the user-supplied LookupFn (the
        // operator can't tell - Out is opaque); user code can
        // call clink::metrics::op::async_lookup_miss_inc(id) from
        // within the LookupFn when it returns a miss sentinel.
        clink::metrics::op::async_lookup_hit_inc(
            this->runtime() ? this->runtime()->metrics() : nullptr, this->id().value());
    }

    LookupFn fn_;
    std::size_t max_in_flight_;
    bool ordered_;
    std::string name_;
    std::deque<std::shared_ptr<Slot>> in_flight_;
    std::size_t emitted_count_{0};
};

}  // namespace clink
