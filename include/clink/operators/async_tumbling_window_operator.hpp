#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "clink/core/codec.hpp"
#include "clink/operators/async_window_operator.hpp"

namespace clink {

// AsyncTumblingWindowOperator<Key, Value, Agg> - event-time tumbling-window
// aggregate on the async-state execution path. The shared async machinery
// (disaggregated accumulators, per-key-gated async fold, epoch-gated
// watermark firing, durable timer-driven enumeration, late-drop,
// checkpoint/restore) lives in AsyncWindowOperator; this only assigns each
// record to its single window [start, start + size).
template <typename Key, typename Value, typename Agg>
class AsyncTumblingWindowOperator final : public AsyncWindowOperator<Key, Value, Agg> {
public:
    using Base = AsyncWindowOperator<Key, Value, Agg>;
    using Initial = typename Base::Initial;
    using Combiner = typename Base::Combiner;

    AsyncTumblingWindowOperator(std::int64_t window_size_ms,
                                Initial initial,
                                Combiner combiner,
                                Codec<Key> key_codec,
                                Codec<Agg> agg_codec,
                                std::string name = "async_tumbling_window")
        : Base(window_size_ms,
               std::move(initial),
               std::move(combiner),
               std::move(key_codec),
               std::move(agg_codec),
               std::move(name)) {}

protected:
    std::vector<std::int64_t> assign_windows(std::int64_t ts) const override {
        return {(ts / this->window_size_ms_) * this->window_size_ms_};
    }
};

}  // namespace clink
