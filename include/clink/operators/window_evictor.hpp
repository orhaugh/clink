#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <limits>
#include <vector>

#include "clink/core/record.hpp"
#include "clink/operators/window_trigger.hpp"

namespace clink {

// An Evictor pre-filters the records buffered in a window before the
// user's ProcessWindowFunction runs. Mirrors Evictor<T, W>:
//
//   * evict_before - runs immediately before the window function. The
//     evictor mutates the supplied record vector in place; remaining
//     records are passed to the user function.
//   * evict_after  - runs after the window function. Used by some
//     evictors (e.g. those that retain a sliding count) to keep state
//     bounded between firings. Default is a no-op.
//
// Evictors are paired with a different operator (EvictingWindowOperator)
// than the aggregate-based ones, because evictors require the raw
// record buffer to be available - you can't un-combine an aggregate
// to drop an old record.
template <typename T, typename Window>
class Evictor {
public:
    Evictor() = default;
    virtual ~Evictor() = default;
    Evictor(const Evictor&) = delete;
    Evictor& operator=(const Evictor&) = delete;
    Evictor(Evictor&&) = delete;
    Evictor& operator=(Evictor&&) = delete;

    virtual void evict_before(std::vector<Record<T>>& records,
                              std::int64_t record_count,
                              const Window& window,
                              TriggerContext<Window>& ctx) = 0;

    virtual void evict_after(std::vector<Record<T>>& /*records*/,
                             std::int64_t /*record_count*/,
                             const Window& /*window*/,
                             TriggerContext<Window>& /*ctx*/) {}
};

// Keep the most-recently-arrived `max_count` records; drop older.
// Mirrors CountEvictor.
template <typename T>
class CountEvictor final : public Evictor<T, TimeWindow> {
public:
    explicit CountEvictor(std::int64_t max_count) : max_(max_count) {}

    void evict_before(std::vector<Record<T>>& records,
                      std::int64_t /*record_count*/,
                      const TimeWindow& /*window*/,
                      TriggerContext<TimeWindow>&) override {
        if (static_cast<std::int64_t>(records.size()) > max_) {
            const std::size_t drop = records.size() - static_cast<std::size_t>(max_);
            records.erase(records.begin(), records.begin() + static_cast<std::ptrdiff_t>(drop));
        }
    }

private:
    std::int64_t max_;
};

// Keep records whose event-time is no more than `max_age` behind the
// most-recently-arrived record. Records without event time are kept.
// Mirrors TimeEvictor (default doEvictAfter=false).
template <typename T>
class TimeEvictor final : public Evictor<T, TimeWindow> {
public:
    explicit TimeEvictor(std::chrono::milliseconds max_age) : max_age_ms_(max_age.count()) {}

    void evict_before(std::vector<Record<T>>& records,
                      std::int64_t /*record_count*/,
                      const TimeWindow& /*window*/,
                      TriggerContext<TimeWindow>&) override {
        std::int64_t latest_ts = std::numeric_limits<std::int64_t>::min();
        for (const auto& r : records) {
            if (r.event_time().has_value()) {
                latest_ts = std::max(latest_ts, r.event_time()->millis());
            }
        }
        const std::int64_t threshold = latest_ts - max_age_ms_;
        records.erase(std::remove_if(records.begin(),
                                     records.end(),
                                     [threshold](const Record<T>& r) {
                                         return r.event_time().has_value() &&
                                                r.event_time()->millis() < threshold;
                                     }),
                      records.end());
    }

private:
    std::int64_t max_age_ms_;
};

}  // namespace clink
