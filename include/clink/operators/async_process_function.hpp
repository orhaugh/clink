#pragma once

// AsyncKeyedProcessFunction - the async-state analogue of KeyedProcessFunction.
//
// The marquee "do anything" keyed operator on the async-state execution path:
// per-record processing that CO_AWAITS keyed-state reads (so a slow/remote read
// for one key overlaps progress on other keys), plus processing-time AND
// event-time timers, side outputs, and per-key state - all checkpointed.
//
// It is the first general-purpose, user-reachable production adopter of the
// per-key-gated record path (AsyncExecutionController) AND the gated
// processing-time timer path (gated_timer_fire.hpp). The existing synchronous
// KeyedProcessFunction (process_function.hpp) is untouched.
//
// Shape (vs the sync KeyedProcessFunction):
//   * process_element is a COROUTINE (async::Task<void>) - the body co_awaits
//     the user's own member KeyedState<K,V>.get_async(key) for non-blocking
//     reads, mutates state synchronously (put/erase) after the await, emits via
//     the Collector, and registers timers via the context.
//   * The key is an EXPLICIT PARAMETER to both process_element and on_timer,
//     never a shared member - so concurrent distinct-key coroutines (interleaved
//     across co_await suspension points) cannot race on a current_key field.
//     There is deliberately NO current_key() accessor.
//   * on_timer is SYNCHRONOUS. Processing-time timers fire from inside the
//     per-key gate (the runner's gated processing-time fire path), so a
//     synchronous state read there cannot race a same-key in-flight read;
//     event-time timers fire inside the epoch-release closure after the epoch
//     drains. Both are safe for synchronous state access.
//
// Contract for the user:
//   * Hold the KeyedState as a MEMBER set up in open() - never co_await
//     get_async on a temporary (its encoded key is copied into the read's
//     coroutine frame, but the KeyedState itself must outlive the await).
//   * The throughput win exists ONLY on a deferring/disaggregated backend
//     (RemoteReadBackend, a future ForSt) AND when you actually co_await. On an
//     InMemory/RocksDB backend the operator runs the byte-identical synchronous
//     path (get_async inline-completes) - correct, but no speedup.
//   * Timers are registered through the context's KEYLESS helpers; the adapter
//     supplies the per-key gate bytes, so the gate-key contract (timer key ==
//     record gate key) holds by construction and cannot be mis-keyed.

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/async/task.hpp"
#include "clink/core/codec.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/operators/process_function.hpp"  // Collector, TimeDomain
#include "clink/runtime/async_execution_controller.hpp"
#include "clink/runtime/output_tag.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/runtime/timer_service.hpp"

namespace clink {

// Per-call context for AsyncKeyedProcessFunction::process_element. Constructed
// fresh inside each record's coroutine frame (so its fields are never shared
// across concurrent coroutines). Timer registration is KEYLESS: the adapter
// stamps in the current call's gate bytes, so the user cannot register a timer
// under a key that disagrees with the record gate key.
template <typename O>
class AsyncKeyedProcessContext {
public:
    AsyncKeyedProcessContext(std::optional<EventTime> ts,
                             RuntimeContext* rt,
                             std::string gate_key) noexcept
        : ts_(ts), rt_(rt), gate_key_(std::move(gate_key)) {}

    // The current record's event-time (or the timer's firing time in on_timer).
    std::optional<EventTime> timestamp() const noexcept { return ts_; }

    // Register a timer keyed by THIS call's gate bytes (the adapter's record
    // gate key). on_timer fires with the same key, so it serialises against
    // same-key records through the per-key gate.
    void register_processing_time_timer(std::int64_t ts) {
        rt_->timer_service()->register_processing_time_timer(ts, gate_key_);
    }
    void register_event_time_timer(std::int64_t ts) {
        rt_->timer_service()->register_event_time_timer(ts, gate_key_);
    }

    template <typename U>
    Emitter<U> side_output(const OutputTag<U>& tag) {
        return rt_->template side_output<U>(tag);
    }

protected:
    std::optional<EventTime> ts_;
    RuntimeContext* rt_{nullptr};
    std::string gate_key_;
};

// Same surface plus the firing timer's time domain.
template <typename O>
class AsyncKeyedOnTimerContext : public AsyncKeyedProcessContext<O> {
public:
    AsyncKeyedOnTimerContext(EventTime fired_at,
                             TimeDomain domain,
                             RuntimeContext* rt,
                             std::string gate_key) noexcept
        : AsyncKeyedProcessContext<O>(fired_at, rt, std::move(gate_key)), domain_(domain) {}

    TimeDomain time_domain() const noexcept { return domain_; }

private:
    TimeDomain domain_;
};

// User-facing async keyed process function. Subclass and override
// process_element (and optionally on_timer / open / close / flush).
template <typename K, typename I, typename O>
class AsyncKeyedProcessFunction {
public:
    virtual ~AsyncKeyedProcessFunction() = default;

    virtual void open(RuntimeContext& /*ctx*/) {}
    virtual void close() {}

    // The async per-record entry point. `key` is threaded explicitly (never a
    // member). co_await the function's member KeyedState<K,V>.get_async(key) for
    // a non-blocking read; mutate state synchronously after the await; emit via
    // `out`; register timers via `ctx`.
    virtual async::Task<void> process_element(const K& key,
                                              const I& value,
                                              AsyncKeyedProcessContext<O>& ctx,
                                              Collector<O>& out) = 0;

    // Synchronous timer callback (processing-time fires under the gate,
    // event-time inside the epoch release - both safe for synchronous state
    // access). `key` is the timer's key, threaded explicitly.
    virtual void on_timer(const K& /*key*/,
                          std::int64_t /*timestamp_ms*/,
                          AsyncKeyedOnTimerContext<O>& /*ctx*/,
                          Collector<O>& /*out*/) {}

    virtual void flush(Collector<O>& /*out*/) {}
    virtual std::string name() const { return "async_keyed_process_function"; }
};

namespace detail {

// Operator<I,O> adapter that drives an AsyncKeyedProcessFunction. A PEER of the
// sync KeyedProcessFunctionAdapter (NOT a subclass: the sync adapter sets a
// shared current_key member, which the async path must never do). The adapter
// owns the key Codec<K> and is the SINGLE SOURCE OF TRUTH for the gate bytes
// (gate_of_), used identically as the AEC record gate key, the timer
// registration key, and the key decoded for on_timer - so the gate-key contract
// holds by construction.
template <typename K, typename I, typename O>
class AsyncKeyedProcessFunctionAdapter : public Operator<I, O> {
public:
    using KeyFn = std::function<K(const I&)>;

    AsyncKeyedProcessFunctionAdapter(std::shared_ptr<AsyncKeyedProcessFunction<K, I, O>> fn,
                                     KeyFn key_fn,
                                     Codec<K> key_codec,
                                     std::string name = "async_keyed_process_function")
        : fn_(std::move(fn)),
          key_fn_(std::move(key_fn)),
          key_codec_(std::move(key_codec)),
          name_(std::move(name)) {}

    void open() override {
        if (auto* rt = this->runtime()) {
            fn_->open(*rt);
        }
    }
    void close() override { fn_->close(); }

    // --- synchronous path (non-deferring backend) ---
    // Drives the SAME user coroutine to inline completion: on a non-deferring
    // backend KeyedState::get_async is co_return get() (no suspension), so a
    // single resume() chains the whole body to its co_return via symmetric
    // transfer. One user body, two equivalent drives -> byte-identical output.
    void process(const StreamElement<I>& element, Emitter<O>& out) override {
        if (element.is_data()) {
            for (const auto& rec : element.as_data()) {
                const K k = key_fn_(rec.value());
                AsyncKeyedProcessContext<O> ctx(rec.event_time(), this->runtime(), gate_of_(k));
                Collector<O> col(&out);
                auto task = fn_->process_element(k, rec.value(), ctx, col);
                task.resume();
                if (!task.done()) {
                    throw std::runtime_error(
                        name_ +
                        ": process_element suspended on the synchronous path; async state reads "
                        "require a deferring backend (supports_async_get)");
                }
                if (task.has_exception()) {
                    task.get();  // rethrow on the runner thread (matches the sync process() throw)
                }
            }
        } else if (element.is_watermark()) {
            this->on_watermark(element.as_watermark(), out);
        } else {
            this->on_barrier(element.as_barrier(), out);
        }
    }

    // --- async path (deferring backend) ---
    [[nodiscard]] bool supports_async() const noexcept override { return true; }

    void process_async(const StreamElement<I>& element,
                       Emitter<O>& out,
                       AsyncExecutionController& aec) override {
        if (!element.is_data()) {
            return;  // the runner routes watermarks/barriers through the controller
        }
        for (const auto& rec : element.as_data()) {
            const K k = key_fn_(rec.value());
            const std::string gate = gate_of_(k);
            const std::optional<EventTime> ts = rec.event_time();
            // fn_ (shared_ptr), value/k/ts/gate by value into the coroutine
            // frame; rt + &out by pointer/reference (both outlive poll/drain).
            // The context + collector are built INSIDE the coroutine, so they
            // are frame-local and never shared across concurrent coroutines.
            auto factory =
                [fn = fn_, value = rec.value(), k, ts, gate, rt = this->runtime(), &out]()
                -> async::Task<void> {
                AsyncKeyedProcessContext<O> ctx(ts, rt, gate);
                Collector<O> col(&out);
                co_await fn->process_element(k, value, ctx, col);
                co_return;
            };
            while (!aec.submit(gate, factory)) {
                aec.poll();
            }
        }
    }

    void on_processing_time_timer(std::int64_t ts,
                                  const std::string& key,
                                  Emitter<O>& out) override {
        dispatch_timer_(ts, key, TimeDomain::ProcessingTime, out);
    }
    void on_event_time_timer(std::int64_t ts, const std::string& key, Emitter<O>& out) override {
        dispatch_timer_(ts, key, TimeDomain::EventTime, out);
    }

    void flush(Emitter<O>& out) override {
        Collector<O> col(&out);
        fn_->flush(col);
    }

    [[nodiscard]] bool fires_state_touching_timers() const noexcept override { return true; }
    [[nodiscard]] bool fires_state_touching_processing_time_timers() const noexcept override {
        return true;
    }
    std::string name() const override { return name_; }

private:
    // The per-key gate bytes: the bare key codec encoding (deliberately NOT
    // KeyedState's kg-prefixed encode_key - the AEC gate only needs an injective
    // function of K, consistent between records and timers, which this is).
    std::string gate_of_(const K& k) const {
        const auto b = key_codec_.encode(k);
        return std::string(reinterpret_cast<const char*>(b.data()), b.size());
    }
    K decode_key_(const std::string& key) const {
        auto v = key_codec_.decode(
            std::span<const std::byte>{reinterpret_cast<const std::byte*>(key.data()), key.size()});
        if (!v.has_value()) {
            throw std::runtime_error(name_ + ": corrupt timer key (key decode failed)");
        }
        return *v;
    }
    void dispatch_timer_(std::int64_t ts,
                         const std::string& key,
                         TimeDomain domain,
                         Emitter<O>& out) {
        const K k = decode_key_(key);
        AsyncKeyedOnTimerContext<O> ctx(EventTime{ts}, domain, this->runtime(), key);
        Collector<O> col(&out);
        fn_->on_timer(k, ts, ctx, col);
    }

    std::shared_ptr<AsyncKeyedProcessFunction<K, I, O>> fn_;
    KeyFn key_fn_;
    Codec<K> key_codec_;
    std::string name_;
};

}  // namespace detail

}  // namespace clink
