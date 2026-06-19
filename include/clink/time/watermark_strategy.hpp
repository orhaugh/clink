#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>

#include "clink/core/record.hpp"
#include "clink/time/event_time.hpp"
#include "clink/time/watermark.hpp"

namespace clink {

// A WatermarkStrategy decides, given a stream of records, when and what
// watermark to emit. The contract is intentionally side-effect-free: the
// strategy only observes records and reports a current watermark; the engine
// (specifically `WatermarkAssignerOperator`) is the one that actually emits.
//
// Strategies are stateful but single-threaded relative to a given assigner
// instance. They never need their own locking.
template <typename T>
class WatermarkStrategy {
public:
    virtual ~WatermarkStrategy() = default;

    // Called once per record, in stream order, after extract_event_time has
    // been applied (so r.event_time() is guaranteed to be set if the source
    // had one).
    virtual void on_record(const Record<T>& r) = 0;

    // The assigner calls this on a cadence (per batch boundary in the MVP).
    // Returns the current watermark if the strategy has progressed since the
    // last query, otherwise nullopt.
    virtual std::optional<Watermark> current_watermark() = 0;
};

// Watermark = max event-time observed so far. Asserts records arrive in
// non-decreasing event-time order; out-of-order arrivals are tolerated but
// the watermark will not regress.
template <typename T>
class MonotonicWatermarkStrategy final : public WatermarkStrategy<T> {
public:
    void on_record(const Record<T>& r) override {
        if (auto t = r.event_time(); t.has_value()) {
            if (!seen_ || *t > max_ts_) {
                max_ts_ = *t;
                seen_ = true;
                dirty_ = true;
            }
        }
    }

    std::optional<Watermark> current_watermark() override {
        if (!seen_ || !dirty_) {
            return std::nullopt;
        }
        dirty_ = false;
        return Watermark{max_ts_};
    }

private:
    EventTime max_ts_{};
    bool seen_{false};
    bool dirty_{false};
};

// Watermark = max event-time observed - bounded_lateness. Tolerates events
// arriving up to `bound` ms late without dropping them, while still letting
// downstream windows fire.
template <typename T>
class BoundedOutOfOrdernessStrategy final : public WatermarkStrategy<T> {
public:
    explicit BoundedOutOfOrdernessStrategy(std::chrono::milliseconds bound) : bound_(bound) {}

    void on_record(const Record<T>& r) override {
        if (auto t = r.event_time(); t.has_value()) {
            if (!seen_ || *t > max_ts_) {
                max_ts_ = *t;
                seen_ = true;
                dirty_ = true;
            }
        }
    }

    std::optional<Watermark> current_watermark() override {
        if (!seen_ || !dirty_) {
            return std::nullopt;
        }
        dirty_ = false;
        const std::int64_t adjusted = max_ts_.millis() - bound_.count();
        return Watermark{EventTime{adjusted}};
    }

private:
    std::chrono::milliseconds bound_;
    EventTime max_ts_{};
    bool seen_{false};
    bool dirty_{false};
};

}  // namespace clink
