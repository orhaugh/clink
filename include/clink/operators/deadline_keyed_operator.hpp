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

// DeadlineKeyedAggregateOperator<K, V, Agg> - a per-key running aggregate held
// in KeyedState, identical in result to KeyedAggregateOperator, but each
// record carries a DEADLINE (an order_key: lower = more urgent) so that, on a
// deferring/disaggregated backend, the most urgent records' state reads resume
// first. It is the first ASYNC-12 consumer: an operator that TAGS its reads.
//
//   * process()       - synchronous: kv.get(k) -> combine -> kv.put(k). The
//     deadline is irrelevant when reads do not suspend; byte-identical to
//     KeyedAggregateOperator's sync path.
//   * process_async() - opt-in: one coroutine per record, each co_awaiting
//     kv.get_async(k, deadline_fn(k, v)). The runner, seeing deadline_aware(),
//     flips its AsyncExecutionController to ResumeOrder::Priority and wires the
//     order_key-carrying hand-back, so when several cold reads complete in the
//     same poll the runner resumes the lowest-deadline (most urgent) records
//     first. With a backend that does not defer, the path collapses to a
//     synchronous read and the deadline is inert (resume order is moot).
//
// The deadline is purely a RESUME-ORDERING hint among already-ready, distinct-
// key completions (the per-key gate guarantees one in-flight read per key); it
// never changes the aggregate result, only WHICH urgent record's post-read
// stage runs first when reads land together. No timers are used, so the
// operator is safe under the async runner (fires_state_touching_timers() stays
// false).
template <typename K, typename V, typename Agg>
class DeadlineKeyedAggregateOperator final : public Operator<std::pair<K, V>, std::pair<K, Agg>> {
public:
    using Initial = std::function<Agg()>;
    using Combiner = std::function<Agg(const Agg&, const V&)>;
    // Maps a record to its resume priority (lower = sooner; e.g. a deadline in
    // ms, or a fixed priority class). Pure: called once per record on the
    // runner thread before the read is issued.
    using DeadlineFn = std::function<std::uint64_t(const K&, const V&)>;

    DeadlineKeyedAggregateOperator(Initial initial,
                                   Combiner combiner,
                                   DeadlineFn deadline_fn,
                                   Codec<K> key_codec,
                                   Codec<Agg> agg_codec,
                                   std::string name = "deadline_keyed_aggregate")
        : initial_(std::move(initial)),
          combiner_(std::move(combiner)),
          deadline_fn_(std::move(deadline_fn)),
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
    // Opt into priority resume: the runner orders ready completions by the
    // order_key this operator tags its reads with.
    [[nodiscard]] bool deadline_aware() const noexcept override { return true; }

    void process_async(const StreamElement<std::pair<K, V>>& element,
                       Emitter<std::pair<K, Agg>>& out,
                       AsyncExecutionController& aec) override {
        if (!element.is_data()) {
            return;  // the runner routes watermarks/barriers through the controller
        }
        for (const auto& rec : element.as_data()) {
            const auto& [k, v] = rec.value();
            const auto ts = rec.event_time();
            const std::uint64_t deadline = deadline_fn_(k, v);
            // Captured by value so nothing dangles across the suspension (see
            // KeyedAggregateOperator). KeyedState owns only a backend pointer +
            // codecs and outlives the job.
            auto kv = state_();
            auto factory =
                [kv, k, v, ts, deadline, initial = initial_, combiner = combiner_, &out]() mutable
                -> async::Task<void> {
                auto cur = co_await kv.get_async(k, deadline);  // tagged read
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
            const auto kb = key_codec_.encode(k);
            std::string gate(reinterpret_cast<const char*>(kb.data()), kb.size());
            // Backpressure: at the in-flight cap submit() refuses (without
            // consuming the factory); poll() drains completions and we retry.
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
    DeadlineFn deadline_fn_;
    Codec<K> key_codec_;
    Codec<Agg> agg_codec_;
    std::string name_;
};

}  // namespace clink
