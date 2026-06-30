#pragma once

// AsyncKeyedCoProcessFunction - the async-state analogue of the two-input
// KeyedCoProcessFunction (the deferred multi-input async path).
//
// Two keyed input streams share one keyed state and one output. Each side's
// process_element is a coroutine that CO_AWAITS the user's member
// KeyedState.get_async(key) (non-blocking on a deferring backend), so a slow or
// remote read for one key overlaps progress on other keys - across BOTH inputs.
// The co-operator runner submits both sides into ONE per-subtask
// AsyncExecutionController, so they share the per-key gate AND the epoch: a
// left-record and a right-record for the same key serialise (each observes the
// other's writes), distinct keys overlap, and the merged (min) watermark closes
// the epoch that an event-time timer fires after.
//
// Mirrors AsyncKeyedProcessFunction (single-input) method-for-method; reuses its
// AsyncKeyedProcessContext / AsyncKeyedOnTimerContext (keyless timer
// registration - the adapter stamps the per-key gate bytes, so the gate-key
// contract holds by construction). The synchronous CoProcessFunction is
// untouched. Same contract + honest scope as the single-input async path: the
// key is an explicit per-call parameter (never a shared member, so concurrent
// distinct-key coroutines cannot race); hold the KeyedState as a member set in
// open(); the throughput win exists only on a deferring backend and only when
// the user co_awaits (on InMemory/RocksDB the byte-identical sync fallback runs).

#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/async/task.hpp"
#include "clink/core/codec.hpp"
#include "clink/operators/async_process_function.hpp"  // AsyncKeyedProcessContext/OnTimerContext
#include "clink/operators/operator_base.hpp"
#include "clink/operators/process_function.hpp"  // Collector, TimeDomain
#include "clink/runtime/async_execution_controller.hpp"

namespace clink {

// User-facing async two-input keyed co-process function. Subclass and override
// process_element1 / process_element2 (and optionally on_timer / open / close /
// flush). The key is threaded explicitly per call.
template <typename K, typename I1, typename I2, typename O>
class AsyncKeyedCoProcessFunction {
public:
    virtual ~AsyncKeyedCoProcessFunction() = default;

    virtual void open(RuntimeContext& /*ctx*/) {}
    virtual void close() {}

    // Async per-record entry points for the two inputs. co_await the function's
    // member KeyedState.get_async(key); both sides share the same keyed state,
    // so a same-key record from either input serialises through the per-key gate.
    virtual async::Task<void> process_element1(const K& key,
                                               const I1& value,
                                               AsyncKeyedProcessContext<O>& ctx,
                                               Collector<O>& out) = 0;
    virtual async::Task<void> process_element2(const K& key,
                                               const I2& value,
                                               AsyncKeyedProcessContext<O>& ctx,
                                               Collector<O>& out) = 0;

    // Synchronous timer callback (processing-time fires under the gate,
    // event-time inside the merged-watermark epoch release - both safe for
    // synchronous state access). `key` is the timer's key, threaded explicitly.
    virtual void on_timer(const K& /*key*/,
                          std::int64_t /*timestamp_ms*/,
                          AsyncKeyedOnTimerContext<O>& /*ctx*/,
                          Collector<O>& /*out*/) {}

    virtual void flush(Collector<O>& /*out*/) {}
    virtual std::string name() const { return "async_keyed_co_process_function"; }
};

namespace detail {

// CoOperator<I1,I2,O> adapter driving an AsyncKeyedCoProcessFunction. A PEER of
// the sync KeyedCoProcessFunctionAdapter (NOT a subclass: the sync adapter sets
// a shared current_key, which the async path must never do). The adapter owns
// ONE Codec<K> and a single gate_of_ used identically for the input1 submit
// gate, the input2 submit gate, the timer registration key, and the on_timer
// dispatch decode - so the same logical key from either input maps to the same
// gate bytes and the gate-key contract holds by construction.
template <typename K, typename I1, typename I2, typename O>
class AsyncKeyedCoProcessFunctionAdapter : public CoOperator<I1, I2, O> {
public:
    using KeyFn1 = std::function<K(const I1&)>;
    using KeyFn2 = std::function<K(const I2&)>;

    AsyncKeyedCoProcessFunctionAdapter(
        std::shared_ptr<AsyncKeyedCoProcessFunction<K, I1, I2, O>> fn,
        KeyFn1 key_fn1,
        KeyFn2 key_fn2,
        Codec<K> key_codec,
        std::string name = "async_keyed_co_process_function")
        : fn_(std::move(fn)),
          key_fn1_(std::move(key_fn1)),
          key_fn2_(std::move(key_fn2)),
          key_codec_(std::move(key_codec)),
          name_(std::move(name)) {}

    void open() override {
        if (auto* rt = this->runtime()) {
            fn_->open(*rt);
        }
    }
    void close() override { fn_->close(); }

    [[nodiscard]] bool supports_async() const noexcept override { return true; }

    // --- synchronous fallback (non-deferring backend): drive the SAME user
    // coroutine to inline completion (one resume; symmetric transfer chains the
    // inline get_async to co_return) - byte-identical to the async path. ---
    void process_element1(const StreamElement<I1>& element, Emitter<O>& out) override {
        if (!element.is_data()) {
            return;
        }
        for (const auto& rec : element.as_data()) {
            const K k = key_fn1_(rec.value());
            AsyncKeyedProcessContext<O> ctx(rec.event_time(), this->runtime(), gate_of_(k));
            Collector<O> col(&out);
            drive_inline_(fn_->process_element1(k, rec.value(), ctx, col));
        }
    }
    void process_element2(const StreamElement<I2>& element, Emitter<O>& out) override {
        if (!element.is_data()) {
            return;
        }
        for (const auto& rec : element.as_data()) {
            const K k = key_fn2_(rec.value());
            AsyncKeyedProcessContext<O> ctx(rec.event_time(), this->runtime(), gate_of_(k));
            Collector<O> col(&out);
            drive_inline_(fn_->process_element2(k, rec.value(), ctx, col));
        }
    }

    // --- async path: submit one coroutine per record under the per-key gate.
    // Both inputs submit into the same controller -> shared gate + epoch. ---
    void process_async1(const StreamElement<I1>& element,
                        Emitter<O>& out,
                        AsyncExecutionController& aec) override {
        if (!element.is_data()) {
            return;
        }
        for (const auto& rec : element.as_data()) {
            const K k = key_fn1_(rec.value());
            const std::string gate = gate_of_(k);
            const std::optional<EventTime> ts = rec.event_time();
            auto factory =
                [fn = fn_, value = rec.value(), k, ts, gate, rt = this->runtime(), &out]()
                -> async::Task<void> {
                AsyncKeyedProcessContext<O> ctx(ts, rt, gate);
                Collector<O> col(&out);
                co_await fn->process_element1(k, value, ctx, col);
                co_return;
            };
            while (!aec.submit(gate, factory)) {
                aec.poll_or_flush();  // flush parked coalesced reads so the cap can free
            }
        }
    }
    void process_async2(const StreamElement<I2>& element,
                        Emitter<O>& out,
                        AsyncExecutionController& aec) override {
        if (!element.is_data()) {
            return;
        }
        for (const auto& rec : element.as_data()) {
            const K k = key_fn2_(rec.value());
            const std::string gate = gate_of_(k);
            const std::optional<EventTime> ts = rec.event_time();
            auto factory =
                [fn = fn_, value = rec.value(), k, ts, gate, rt = this->runtime(), &out]()
                -> async::Task<void> {
                AsyncKeyedProcessContext<O> ctx(ts, rt, gate);
                Collector<O> col(&out);
                co_await fn->process_element2(k, value, ctx, col);
                co_return;
            };
            while (!aec.submit(gate, factory)) {
                aec.poll_or_flush();  // flush parked coalesced reads so the cap can free
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
    // The per-key gate bytes: bare key codec encoding (NOT KeyedState's
    // kg-prefixed encode_key); injective and side-agnostic, so the same K from
    // either input + its timers all share one gate.
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
    void drive_inline_(async::Task<void> task) {
        task.resume();
        if (!task.done()) {
            throw std::runtime_error(
                name_ +
                ": process_element suspended on the synchronous path; async state reads require a "
                "deferring backend (supports_async_get)");
        }
        if (task.has_exception()) {
            task.get();  // rethrow on the runner thread
        }
    }

    std::shared_ptr<AsyncKeyedCoProcessFunction<K, I1, I2, O>> fn_;
    KeyFn1 key_fn1_;
    KeyFn2 key_fn2_;
    Codec<K> key_codec_;
    std::string name_;
};

}  // namespace detail

}  // namespace clink
