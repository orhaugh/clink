#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <vector>

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

    // Per-partition idleness hook. The assigner owns the wall clock, so it
    // tells the strategy which source partitions have gone quiet (no record
    // for the configured idleness duration). A partition-aware strategy
    // excludes these from its min-watermark computation, so a stalled
    // partition stops pinning the watermark low. Default no-op: strategies
    // without per-partition state ignore it. Idempotent - the assigner passes
    // the full idle set each probe.
    virtual void set_idle_partitions(const std::vector<std::int32_t>& /*idle*/) {}
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

// Per-partition (per-source-split) bounded-out-of-orderness. When a source
// reads several Kafka partitions and interleaves them into one stream, a single
// global max-seen watermark races to the FASTEST partition and marks slower
// partitions' in-window records late. This strategy instead tracks the max
// event-time PER partition (Record::source_partition) and emits
// watermark = (min over partitions of per-partition max) - bound, so the
// watermark advances only as fast as the slowest partition and no in-window
// record is falsely late.
//
// Records WITHOUT a source_partition (file / generator / single-stream sources)
// fold into one global bucket, so behaviour is byte-identical to
// BoundedOutOfOrdernessStrategy for them. The watermark is only re-emitted when
// the min actually advances (it is monotonic: each per-partition max only
// grows, so the min never regresses).
//
// Per-partition idleness: a partition that produces some records then goes
// QUIET would keep pinning the min low (its max never advances). The owning
// `WatermarkAssignerOperator` detects quiet partitions on its processing-time
// probe and calls `set_idle_partitions`; those partitions are then excluded
// from the min, so the watermark advances as fast as the slowest STILL-ACTIVE
// partition. A partition re-activates the instant it produces again
// (`on_record` clears it). This is the per-partition mirror of the assigner's
// stream-level idleness.
template <typename T>
class PartitionAwareBoundedOutOfOrdernessStrategy final : public WatermarkStrategy<T> {
public:
    explicit PartitionAwareBoundedOutOfOrdernessStrategy(std::chrono::milliseconds bound)
        : bound_(bound) {}

    void on_record(const Record<T>& r) override {
        auto t = r.event_time();
        if (!t.has_value()) {
            return;
        }
        if (auto p = r.source_partition(); p.has_value()) {
            auto it = part_max_.find(*p);
            if (it == part_max_.end()) {
                part_max_.emplace(*p, *t);
            } else if (*t > it->second) {
                it->second = *t;
            }
            // A record re-activates its partition immediately (do not wait for
            // the next idle probe to notice it came back).
            if (idle_partitions_.erase(*p) != 0) {
                dirty_ = true;
            }
        } else {
            if (!global_seen_ || *t > global_max_) {
                global_max_ = *t;
                global_seen_ = true;
            }
        }
        dirty_ = true;
    }

    void set_idle_partitions(const std::vector<std::int32_t>& idle) override {
        std::set<std::int32_t> next(idle.begin(), idle.end());
        if (next != idle_partitions_) {
            idle_partitions_ = std::move(next);
            dirty_ = true;  // the min may now advance past a newly-idle partition
        }
    }

    std::optional<Watermark> current_watermark() override {
        if (!dirty_) {
            return std::nullopt;
        }
        dirty_ = false;
        std::optional<EventTime> base;
        for (const auto& [part, mx] : part_max_) {
            if (idle_partitions_.count(part) != 0) {
                continue;  // quiet partition: excluded from the min
            }
            if (!base || mx < *base) {
                base = mx;
            }
        }
        if (global_seen_ && (!base || global_max_ < *base)) {
            base = global_max_;
        }
        if (!base) {
            return std::nullopt;
        }
        if (emitted_ && !(*base > last_base_)) {
            return std::nullopt;  // the min did not advance
        }
        last_base_ = *base;
        emitted_ = true;
        return Watermark{EventTime{base->millis() - bound_.count()}};
    }

private:
    std::chrono::milliseconds bound_;
    std::map<std::int32_t, EventTime> part_max_;
    std::set<std::int32_t> idle_partitions_;
    EventTime global_max_{};
    bool global_seen_{false};
    EventTime last_base_{};
    bool emitted_{false};
    bool dirty_{false};
};

}  // namespace clink
