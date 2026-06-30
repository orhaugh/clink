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

// DeadlineKeyedCoAggregateOperator<K, V, Agg> - the two-input sibling of
// DeadlineKeyedAggregateOperator: a per-key running aggregate fed by BOTH
// inputs (both fold into the SAME KeyedState<K, Agg>), where each record tags
// its state read with a deadline (order_key, lower = more urgent). It is the
// deadline-aware CO-OPERATOR consumer of ASYNC-12.
//
// Both inputs share the one per-subtask AsyncExecutionController (the co-op
// runner submits process_async1 and process_async2 records to it), so a poll's
// ready batch can mix completions from EITHER input - and under
// ResumeOrder::Priority the runner resumes them most-urgent-first ACROSS inputs.
// The per-key gate still serialises same-key records from either side (so they
// observe each other's writes), and the result is independent of resume order;
// only WHICH urgent record's post-read stage runs first changes.
//
//   * process_element1/2 - synchronous fallback for a non-deferring backend:
//     get -> combine -> put -> emit. Deadline is irrelevant (no suspension).
//   * process_async1/2   - one coroutine per record, each co_awaiting
//     kv.get_async(k, deadline_fn(k, v)). Byte-identical result to the sync
//     path; only the read-completion order is reprioritised.
//
// Both inputs carry the same value type V and share one combiner + one
// deadline_fn (the common "merge two streams of the same record into a per-key
// aggregate" shape). No timers, so the operator is safe under the async runner.
template <typename K, typename V, typename Agg>
class DeadlineKeyedCoAggregateOperator final
    : public CoOperator<std::pair<K, V>, std::pair<K, V>, std::pair<K, Agg>> {
public:
    using Initial = std::function<Agg()>;
    using Combiner = std::function<Agg(const Agg&, const V&)>;
    using DeadlineFn = std::function<std::uint64_t(const K&, const V&)>;

    DeadlineKeyedCoAggregateOperator(Initial initial,
                                     Combiner combiner,
                                     DeadlineFn deadline_fn,
                                     Codec<K> key_codec,
                                     Codec<Agg> agg_codec,
                                     std::string name = "deadline_keyed_co_aggregate")
        : initial_(std::move(initial)),
          combiner_(std::move(combiner)),
          deadline_fn_(std::move(deadline_fn)),
          key_codec_(std::move(key_codec)),
          agg_codec_(std::move(agg_codec)),
          name_(std::move(name)) {}

    void process_element1(const StreamElement<std::pair<K, V>>& element,
                          Emitter<std::pair<K, Agg>>& out) override {
        fold_sync_(element, out);
    }
    void process_element2(const StreamElement<std::pair<K, V>>& element,
                          Emitter<std::pair<K, Agg>>& out) override {
        fold_sync_(element, out);
    }

    [[nodiscard]] bool supports_async() const noexcept override { return true; }
    [[nodiscard]] bool deadline_aware() const noexcept override { return true; }

    void process_async1(const StreamElement<std::pair<K, V>>& element,
                        Emitter<std::pair<K, Agg>>& out,
                        AsyncExecutionController& aec) override {
        submit_each_(element, out, aec);
    }
    void process_async2(const StreamElement<std::pair<K, V>>& element,
                        Emitter<std::pair<K, Agg>>& out,
                        AsyncExecutionController& aec) override {
        submit_each_(element, out, aec);
    }

    std::string name() const override { return name_; }

private:
    // The co-op runner routes watermarks/barriers to on_watermark/on_barrier
    // directly, so the data path only handles data elements.
    void fold_sync_(const StreamElement<std::pair<K, V>>& element,
                    Emitter<std::pair<K, Agg>>& out) {
        if (!element.is_data()) {
            return;
        }
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
    }

    void submit_each_(const StreamElement<std::pair<K, V>>& element,
                      Emitter<std::pair<K, Agg>>& out,
                      AsyncExecutionController& aec) {
        if (!element.is_data()) {
            return;
        }
        for (const auto& rec : element.as_data()) {
            const auto& [k, v] = rec.value();
            const auto ts = rec.event_time();
            const std::uint64_t deadline = deadline_fn_(k, v);
            auto kv = state_();
            auto factory =
                [kv, k, v, ts, deadline, initial = initial_, combiner = combiner_, &out]() mutable
                -> async::Task<void> {
                auto cur = co_await kv.get_async(k, deadline);  // deadline-tagged read
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
            while (!aec.submit(gate, factory)) {
                aec.poll_or_flush();  // flush parked coalesced reads so the cap can free
            }
        }
    }

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
