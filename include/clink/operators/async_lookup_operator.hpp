#pragma once

// AsyncLookupOperator<In, Out>
//
// Coroutine-shaped analogue of AsyncMapOperator. Where
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
//      the operator scales to 1000s of concurrent lookups
//      on one thread.
//   2. Natural composition. `co_await another_lookup()` is just
//      another awaitable; cascading async work doesn't need
//      `then()`-style continuation chaining.
//
// Scheduling model:
//   - process(data): for each record, kick off lookup_fn(record),
//     get back a Task<Out>, resume() once. If the task completes in
//     a single step (synchronous lookup), the result is captured
//     immediately. If the task suspends on a co_await, the slot
//     waits for an external signal (the awaitable's continuation
//     mechanism) to resume.
//   - drain(): emit completed tasks from the head of the ordered
//     queue. Stops at the first not-done task to preserve input
//     order. Unordered emit (out_of_order) emits any done task.
//   - watermark: leaves after every record that arrived before it
//     and before any that arrived after it, in both modes, but does
//     not stop lookups starting. It waits in the queue behind the
//     records ahead of it, the lookups of the records behind it start
//     as they arrive, and drain() forwards it once nothing is ahead of
//     it. A run of watermarks with no record between them keeps only
//     its newest progress: the newest watermark that is not idle,
//     then an idle mark if the run ends idle.
//   - barrier and end of input: drain everything in flight first,
//     since the operator keeps no state and nothing may be in flight
//     across a checkpoint. For synchronous-completing tasks this is
//     immediate; for tasks suspended on real I/O it spin-waits with a
//     short sleep + resume() to let the user's awaitable advance.
//   - wake-ups: while anything is in flight, next_timer_deadline_ms()
//     asks the runner to wake the operator every kPollMs, and
//     fire_due_timers() emits what has finished, so a result or a
//     waiting watermark does not wait for the next input element.
//   - max_in_flight: process() blocks once max_in_flight lookups are
//     in flight so a slow lookup naturally throttles upstream;
//     waiting watermarks do not count.
//
// Exception handling: a throwing Task<Out> body is captured on the
// promise (see clink::async::Task). emit_slot_() calls Task::get()
// which rethrows; the runner sees the throw and treats it the same
// as any other operator failure (typically: log + abort the job).
// A user-configurable retry policy is left for a follow-on slice.

#include <algorithm>
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
                while (lookups_ >= max_in_flight_) {
                    drain_(out);
                    if (lookups_ < max_in_flight_)
                        break;
                    nudge_in_flight_();
                    std::this_thread::sleep_for(std::chrono::microseconds{100});
                }
                kickoff_(record.value());
            }
            drain_(out);
        } else if (element.is_watermark()) {
            // The watermark may not pass the records ahead of it, but it
            // need not stop the lookups behind it: it waits in the queue.
            drain_(out);
            if (queue_.empty()) {
                this->on_watermark(element.as_watermark(), out);
            } else {
                enqueue_watermark_(element.as_watermark());
            }
        } else {
            wait_for_all_(out);
            this->on_barrier(element.as_barrier(), out);
        }
    }

    // EOS hook: drain anything left in-flight before the runner
    // tears the operator down. Mirrors AsyncMapOperator::flush.
    void flush(Emitter<Out>& out) override { wait_for_all_(out); }

    // Advances every in-flight lookup once and emits what has finished
    // (ordered: the finished head only), with any watermark that no
    // longer has a record ahead of it. Never blocks, so an operator
    // that wraps this one can call it between inputs, when the runner
    // wakes it, instead of holding finished results until the next
    // marker or until max_in_flight is reached.
    void poll(Emitter<Out>& out) {
        nudge_in_flight_();
        drain_(out);
    }

    // While anything is in flight, the runner wakes the operator every
    // kPollMs to emit what has finished, so neither a result nor a
    // watermark waiting behind it waits for the next input element.
    void fire_due_timers(Emitter<Out>& out, std::int64_t now_ms) override {
        Operator<In, Out>::fire_due_timers(out, now_ms);
        if (!queue_.empty()) {
            poll(out);
        }
    }

    [[nodiscard]] std::optional<std::int64_t> next_timer_deadline_ms() const override {
        auto next = Operator<In, Out>::next_timer_deadline_ms();
        auto* rt = this->runtime();
        if (!queue_.empty() && rt != nullptr) {
            const std::int64_t soon = rt->timer_service()->now_ms() + kPollMs;
            return next ? std::min(*next, soon) : soon;
        }
        return next;
    }

    std::string name() const override { return name_; }

    // Test/diagnostic accessors. Not part of the public Operator
    // contract but useful for UTs to observe scheduling behaviour
    // without having to drive the input channel.
    [[nodiscard]] std::size_t in_flight_count() const noexcept { return lookups_; }
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

    // One place in the queue: a lookup in flight, or a watermark waiting
    // for the records that arrived before it.
    struct Entry {
        std::shared_ptr<Slot> slot;
        std::optional<Watermark> watermark;
    };

    static constexpr std::int64_t kPollMs = 2;

    void kickoff_(const In& record) {
        // Build the lookup coroutine. fn_ returns a Task<Out>; we
        // resume() it once to advance past initial_suspend
        // (lazy-start) into the body. A synchronous body finishes
        // here; an async body suspends on its first co_await.
        auto slot = std::make_shared<Slot>(fn_(record));
        slot->task.resume();
        queue_.push_back(Entry{std::move(slot), std::nullopt});
        ++lookups_;
    }

    // Keeps a run of watermarks with no record between them to its newest
    // progress, so the queue is bounded by the lookups in flight: a
    // watermark that is not idle replaces the run, and an idle one replaces
    // an idle mark at the end of it.
    void enqueue_watermark_(Watermark wm) {
        if (!wm.is_idle()) {
            while (!queue_.empty() && queue_.back().watermark) {
                queue_.pop_back();
            }
        } else if (!queue_.empty() && queue_.back().watermark &&
                   queue_.back().watermark->is_idle()) {
            queue_.pop_back();
        }
        queue_.push_back(Entry{nullptr, wm});
    }

    // Drain whatever is ready. Ordered emit pops from the head of
    // queue_ while the head is done or a watermark; stops at the first
    // not-done task to preserve input order. Unordered emit walks the
    // queue and emits every done task up to the first watermark that
    // still has a task ahead of it: records leave in any order between
    // two watermarks, but none passes one.
    void drain_(Emitter<Out>& out) {
        if (ordered_) {
            while (!queue_.empty()) {
                auto& head = queue_.front();
                if (head.watermark) {
                    const Watermark wm = *head.watermark;
                    queue_.pop_front();
                    this->on_watermark(wm, out);
                    continue;
                }
                if (!head.slot->task.done()) {
                    break;
                }
                auto slot = std::move(head.slot);
                queue_.pop_front();
                --lookups_;
                emit_slot_(*slot, out);
            }
        } else {
            // Walk-and-rebuild: emit done slots, keep the rest in
            // their relative order.
            std::deque<Entry> kept;
            bool held = false;
            for (auto& entry : queue_) {
                if (held) {
                    kept.push_back(std::move(entry));
                } else if (entry.watermark) {
                    if (kept.empty()) {
                        this->on_watermark(*entry.watermark, out);
                    } else {
                        held = true;
                        kept.push_back(std::move(entry));
                    }
                } else if (entry.slot->task.done()) {
                    --lookups_;
                    emit_slot_(*entry.slot, out);
                } else {
                    kept.push_back(std::move(entry));
                }
            }
            queue_ = std::move(kept);
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
        for (auto& entry : queue_) {
            if (entry.slot && !entry.slot->task.done()) {
                entry.slot->task.resume();
            }
        }
    }

    // Block until every in-flight task is done, then drain, waiting
    // watermarks included. Used on barriers and at the end of input,
    // where downstream needs every pending result delivered before the
    // marker propagates. For tests with synchronous lookup bodies the
    // first drain_ empties the queue; for async I/O bodies the
    // spin-wait gives the backend a chance to deliver completions.
    void wait_for_all_(Emitter<Out>& out) {
        while (!queue_.empty()) {
            drain_(out);
            if (queue_.empty())
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
        // Metrics-coverage pass: every completed slot
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
    std::deque<Entry> queue_;
    std::size_t lookups_{0};
    std::size_t emitted_count_{0};
};

}  // namespace clink
