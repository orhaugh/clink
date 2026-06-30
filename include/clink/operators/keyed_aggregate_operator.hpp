#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <utility>

#include "clink/async/task.hpp"
#include "clink/core/codec.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/async_execution_controller.hpp"
#include "clink/state/keyed_state.hpp"

namespace clink {

// KeyedAggregateOperator<K, V, Agg> - a per-key running aggregate held in
// KeyedState (the StateBackend), emitting (k, running_agg) after every
// input record. It is the recoverable, backend-backed sibling of
// ReduceOperator (whose state is in-memory), and it is the first
// production operator to adopt the async-state execution path:
//
//   * process()       - synchronous: kv.get(k) -> combine -> kv.put(k).
//   * process_async() - opt-in: submits one coroutine per record to the
//     AsyncExecutionController, each co_awaiting kv.get_async(k) so a slow
//     (remote/disaggregated) read suspends that record instead of blocking
//     the runner thread. The controller enforces per-key FIFO ordering and
//     watermark/event-time completeness; the runner routes watermarks and
//     barriers through it, draining at checkpoint barriers.
//
// The two paths must produce identical output for the same input; the
// async branch only changes WHEN the state read completes, not the result.
// Because state lives in KeyedState, the aggregate is checkpointed and
// restored like any other keyed state, and it can ride a disaggregated
// backend (e.g. a future ForSt backend) with no operator change.
//
// No timers are used, so this operator is safe under the async runner
// (fires_state_touching_timers() stays false).
template <typename K, typename V, typename Agg>
class KeyedAggregateOperator final : public Operator<std::pair<K, V>, std::pair<K, Agg>> {
public:
    using Initial = std::function<Agg()>;
    using Combiner = std::function<Agg(const Agg&, const V&)>;

    KeyedAggregateOperator(Initial initial,
                           Combiner combiner,
                           Codec<K> key_codec,
                           Codec<Agg> agg_codec,
                           std::string name = "keyed_aggregate")
        : initial_(std::move(initial)),
          combiner_(std::move(combiner)),
          key_codec_(std::move(key_codec)),
          agg_codec_(std::move(agg_codec)),
          name_(std::move(name)) {}

    void process(const StreamElement<std::pair<K, V>>& element,
                 Emitter<std::pair<K, Agg>>& out) override {
        if (element.is_data()) {
            auto kv = state_();
            Batch<std::pair<K, Agg>> batch;
            for (const auto& rec : element.as_data()) {
                const auto& [k, v] = rec.value();
                const Agg next = combiner_(kv.get(k).value_or(initial_()), v);
                kv.put(k, next);
                if (rec.event_time().has_value()) {
                    batch.emplace(std::make_pair(k, next), *rec.event_time());
                } else {
                    batch.emplace(std::make_pair(k, next));
                }
            }
            if (!batch.empty()) {
                out.emit_data(std::move(batch));
            }
        } else if (element.is_watermark()) {
            this->on_watermark(element.as_watermark(), out);
        } else {
            this->on_barrier(element.as_barrier(), out);
        }
    }

    [[nodiscard]] bool supports_async() const noexcept override { return true; }

    void process_async(const StreamElement<std::pair<K, V>>& element,
                       Emitter<std::pair<K, Agg>>& out,
                       AsyncExecutionController& aec) override {
        if (!element.is_data()) {
            return;  // the runner routes watermarks/barriers through the controller
        }
        for (const auto& rec : element.as_data()) {
            const auto& [k, v] = rec.value();
            const auto ts = rec.event_time();
            // KeyedState is captured by value (it owns only a backend pointer
            // + codecs and outlives the job), key/value/event-time by value,
            // and the combiner/initial by value, so nothing dangles across a
            // suspension. The emitter is resumed on the runner thread.
            auto kv = state_();
            auto factory = [kv, k, v, ts, initial = initial_, combiner = combiner_, &out]() mutable
                -> async::Task<void> {
                auto cur = co_await kv.get_async(k);
                const Agg next = combiner(cur.value_or(initial()), v);
                kv.put(k, next);
                Batch<std::pair<K, Agg>> batch;
                if (ts.has_value()) {
                    batch.emplace(std::make_pair(k, next), *ts);
                } else {
                    batch.emplace(std::make_pair(k, next));
                }
                out.emit_data(std::move(batch));
                co_return;
            };
            // Per-key gate key: any injective function of K. The encoded key
            // bytes serve, so same-key records serialise through the gate.
            const auto kb = key_codec_.encode(k);
            std::string gate(reinterpret_cast<const char*>(kb.data()), kb.size());
            // Honour backpressure: at the in-flight cap submit() refuses
            // (without consuming the factory); poll() drains completions and
            // we retry, so no record is dropped.
            while (!aec.submit(gate, factory)) {
                aec.poll_or_flush();  // flush parked coalesced reads so the cap can free
            }
        }
    }

    std::string name() const override { return name_; }

private:
    KeyedState<K, Agg> state_() {
        return this->runtime()->template keyed_state<K, Agg>("agg", key_codec_, agg_codec_);
    }

    Initial initial_;
    Combiner combiner_;
    Codec<K> key_codec_;
    Codec<Agg> agg_codec_;
    std::string name_;
};

}  // namespace clink
