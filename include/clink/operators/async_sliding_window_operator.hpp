#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "clink/core/codec.hpp"
#include "clink/operators/async_window_operator.hpp"

namespace clink {

// AsyncSlidingWindowOperator<Key, Value, Agg> - event-time sliding (hopping)
// window aggregate on the async-state execution path. A record at `ts` belongs
// to every window [s, s + size) whose start s is a multiple of `slide` with
// s <= ts < s + size (size/slide overlapping windows). Each (window_start, key)
// has its own accumulator + event-time timer, so the overlapping windows fire
// independently at their own ends.
//
// All the async machinery (disaggregated accumulators, per-key-gated async fold,
// epoch-gated watermark firing, durable timer-driven enumeration, late-drop,
// checkpoint/restore) is inherited from AsyncWindowOperator; this only assigns
// the overlapping window set. With slide == size it degenerates to tumbling.
//
// v1: windows with a negative start are clamped away (event-times are assumed
// non-negative), matching the SQL HOP semantics.
template <typename Key, typename Value, typename Agg>
class AsyncSlidingWindowOperator final : public AsyncWindowOperator<Key, Value, Agg> {
public:
    using Base = AsyncWindowOperator<Key, Value, Agg>;
    using Initial = typename Base::Initial;
    using Combiner = typename Base::Combiner;

    AsyncSlidingWindowOperator(std::int64_t window_size_ms,
                               std::int64_t slide_ms,
                               Initial initial,
                               Combiner combiner,
                               Codec<Key> key_codec,
                               Codec<Agg> agg_codec,
                               std::string name = "async_sliding_window")
        : Base(window_size_ms,
               std::move(initial),
               std::move(combiner),
               std::move(key_codec),
               std::move(agg_codec),
               std::move(name)),
          slide_ms_(slide_ms) {
        if (slide_ms_ <= 0 || slide_ms_ > window_size_ms) {
            throw std::invalid_argument(
                "AsyncSlidingWindowOperator: slide_ms must be in (0, window_size_ms]");
        }
    }

protected:
    std::vector<std::int64_t> assign_windows(std::int64_t ts) const override {
        const std::int64_t size = this->window_size_ms_;
        // Windows start at multiples of slide. The record at `ts` falls in the
        // windows from the earliest start that still covers it up to the latest.
        std::int64_t last = (ts / slide_ms_) * slide_ms_;
        std::int64_t first = ((ts - size + slide_ms_) / slide_ms_) * slide_ms_;
        if (first < 0) {
            first = 0;  // clamp negative-start windows (non-negative event times)
        }
        std::vector<std::int64_t> out;
        out.reserve(static_cast<std::size_t>((last - first) / slide_ms_ + 1));
        for (std::int64_t s = first; s <= last; s += slide_ms_) {
            if (ts >= s && ts < s + size) {
                out.push_back(s);
            }
        }
        return out;
    }

private:
    std::int64_t slide_ms_;
};

}  // namespace clink
