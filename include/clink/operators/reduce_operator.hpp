#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>
#include <utility>

#include "clink/core/hash_map.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink {

// ReduceOperator maintains a per-key running aggregate.
//
// EmitMode controls *when* output records are emitted:
//   - OnEachInput: emit (k, current_agg) after every input record. The
//     downstream sees a stream of running totals; the latest emission per
//     key is the canonical value at any given time.
//   - OnFlush: do not emit during processing. On end-of-stream (flush()),
//     emit one (k, final_agg) per key. Natural for snapshot pipelines
//     where you want exactly one aggregate per key per run.
//
// State is in-memory in this MVP. Porting onto KeyedState (mirroring
// TumblingWindowOperator) is a small follow-up.
enum class ReduceEmitMode : std::uint8_t { OnEachInput, OnFlush };

template <typename K, typename V, typename Agg>
class ReduceOperator final : public Operator<std::pair<K, V>, std::pair<K, Agg>> {
public:
    using Initial = std::function<Agg()>;
    using Combiner = std::function<Agg(const Agg&, const V&)>;

    ReduceOperator(Initial initial,
                   Combiner combiner,
                   std::string name = "reduce",
                   ReduceEmitMode mode = ReduceEmitMode::OnEachInput)
        : initial_(std::move(initial)),
          combiner_(std::move(combiner)),
          name_(std::move(name)),
          mode_(mode) {}

    void process(const StreamElement<std::pair<K, V>>& element,
                 Emitter<std::pair<K, Agg>>& out) override {
        if (element.is_data()) {
            Batch<std::pair<K, Agg>> batch;
            for (const auto& rec : element.as_data()) {
                const auto& [k, v] = rec.value();
                auto it = state_.find(k);
                if (it == state_.end()) {
                    it = state_.emplace(k, initial_()).first;
                }
                it->second = combiner_(it->second, v);
                if (mode_ == ReduceEmitMode::OnEachInput) {
                    if (rec.event_time().has_value()) {
                        batch.emplace(std::make_pair(k, it->second), *rec.event_time());
                    } else {
                        batch.emplace(std::make_pair(k, it->second));
                    }
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

    void flush(Emitter<std::pair<K, Agg>>& out) override {
        if (mode_ != ReduceEmitMode::OnFlush || state_.empty()) {
            return;
        }
        Batch<std::pair<K, Agg>> batch;
        for (auto& [k, agg] : state_) {
            batch.emplace(std::make_pair(k, std::move(agg)));
        }
        out.emit_data(std::move(batch));
        state_.clear();
    }

    std::string name() const override { return name_; }

private:
    Initial initial_;
    Combiner combiner_;
    std::string name_;
    ReduceEmitMode mode_;
    clink::FlatMap<K, Agg> state_;
};

}  // namespace clink
