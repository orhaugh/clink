#pragma once

// Per-key retention for the stateful SQL operators.
//
// The bounded-state gate (bounded_state.hpp) refuses a windowless GROUP BY,
// a DISTINCT, or an unwindowed join over an unbounded source unless the
// query declares `state_ttl`. This is the machinery that makes that
// declaration true at runtime rather than merely accepted at plan time.
//
// It is shared rather than reimplemented per operator for one reason: the
// interesting parts are all decisions that must be identical everywhere,
// and four independent copies would drift. Specifically:
//
//   * Deadlines are ABSOLUTE, and persisted as such. A restored key
//     resumes its original deadline rather than getting a fresh full TTL;
//     otherwise every restart silently extends retention, and a job that
//     restarts often never expires anything.
//
//   * The deadline is owned by the OPERATOR, not by KeyedState's own
//     TtlConfig. These operators keep their hot state in an in-memory map
//     that is flushed to the backend at every checkpoint, and a KeyedState
//     TTL stamps on every put - so each flush would refresh the deadline
//     and a key touched once would live for ever. The TTL would appear to
//     work while bounding nothing. Here, a key's clock starts when the
//     DATA last touched it.
//
//   * Under event time, nothing expires until the first watermark arrives.
//     A zero watermark would make every stamped key look long expired and
//     wipe the operator's state the instant the job started. This matches
//     KeyedState's rule exactly so the two cannot disagree.
//
//   * The clock is monotonic. A watermark regression must not resurrect
//     expired keys, or retention would depend on the order watermarks
//     happened to arrive in.
//
// Usage in an operator:
//
//   ctor:          ttl_(state_ttl_ms, ttl_event_time)
//   on write:      ttl_.touch(key)
//   on watermark:  if (ttl_.advance_watermark(ms))
//                      for (auto& k : ttl_.expired()) { erase k; ttl_.forget(k); }
//   at flush:      ttl_.flush(deadline_slot)
//   at open:       ttl_.restore(deadline_slot)

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "clink/core/codec.hpp"
#include "clink/metrics/state_metrics.hpp"
#include "clink/state/keyed_state.hpp"

namespace clink::sql {

class StateTtlTracker {
public:
    using DeadlineSlot = KeyedState<std::string, std::int64_t>;

    StateTtlTracker() = default;
    StateTtlTracker(std::int64_t ttl_ms, bool event_time)
        : ttl_ms_(ttl_ms), event_time_(event_time) {}

    [[nodiscard]] bool enabled() const noexcept { return ttl_ms_ > 0; }
    [[nodiscard]] std::uint64_t expired_total() const noexcept { return expired_total_; }
    [[nodiscard]] std::size_t tracked() const noexcept { return deadlines_.size(); }

    // Record that `key` was touched by data now, pushing its deadline out.
    //
    // Before event time exists there is no clock to stamp a deadline from -
    // a deadline computed from a zero watermark would place the key in 1970
    // and evict it on the first real watermark. Such keys are HELD rather
    // than dropped from tracking: they enter the TTL regime when the first
    // watermark establishes the clock (deadline = first watermark + ttl).
    // Dropping them instead made every key written before the first
    // watermark IMMORTAL unless data happened to touch it again - in a real
    // job that is the whole first batch, silently exempt from the very
    // bound the state gate accepted (found by the join TTL release probes,
    // whose write-then-watermark scripts never expired anything).
    void touch(const std::string& key) {
        if (!enabled()) {
            return;
        }
        const auto now = now_ms();
        // The deadline base is the LATER of the clock and the newest record
        // event time this operator has observed - never the watermark alone.
        //
        // Under partition skew (a catch-up burst, a recovery replay, one
        // lagging partition) a fast input delivers records whose event time
        // is far ahead of the aligned min-watermark. Stamping those
        // watermark + ttl gave a key a deadline that could precede its own
        // record's timestamp: watermark 78s + 45s ttl = 123s for a record
        // stamped 150s. The slow input's watermark then advanced past the
        // deadline on its way TO 150s, the key evicted, and the slow
        // input's exactly-on-time records re-opened it from zero - a
        // silent, permanent under-count measured at 13,868 of 53,550
        // events on a fault-free two-partition backlog read (QUAL-05).
        //
        // The observed high event time is a max over the operator's whole
        // input rather than the touching record's own timestamp, so it can
        // only lengthen retention relative to the per-record stamp - never
        // shorten it - and the excess is bounded by the skew, converging
        // to the watermark lag once caught up.
        if (!now.has_value()) {
            if (event_time_ && high_event_ts_ != kNoEventTs) {
                // No watermark yet, but the data itself carries a clock.
                // ts + ttl is the faithful stamp, and nothing can expire
                // against it until the first watermark arrives anyway.
                deadlines_[key] = high_event_ts_ + ttl_ms_;
                dirty_.insert(key);
                return;
            }
            pre_clock_.insert(key);
            return;
        }
        const auto base =
            (event_time_ && high_event_ts_ != kNoEventTs) ? std::max(*now, high_event_ts_) : *now;
        deadlines_[key] = base + ttl_ms_;
        dirty_.insert(key);
    }

    // Record that this operator has SEEN a record stamped `ts_ms`. Called
    // once per data record by the TTL'd operators before folding, so the
    // deadline base above can never lag the data. Event-time domain only;
    // the processing-time clock is the wall clock and needs no help.
    void observe_event_time(std::int64_t ts_ms) {
        if (!enabled() || !event_time_) {
            return;
        }
        if (high_event_ts_ == kNoEventTs || ts_ms > high_event_ts_) {
            high_event_ts_ = ts_ms;
        }
    }

    // Move the clock forward. Returns true when it actually moved, so a
    // caller can skip the sweep on a repeated or regressing watermark.
    bool advance_watermark(std::int64_t ms) {
        if (!enabled() || !event_time_) {
            // Processing time ignores watermarks for the clock, but still
            // uses their arrival as a convenient sweep trigger.
            return enabled();
        }
        if (seen_watermark_ && ms <= watermark_ms_) {
            return false;
        }
        watermark_ms_ = ms;
        seen_watermark_ = true;
        // The clock now exists: keys touched before it entered the TTL
        // regime here, with the full ttl measured from this first
        // watermark. Never earlier (they must not expire against a clock
        // that had not started) and never later (or they never expire at
        // all).
        if (!pre_clock_.empty()) {
            for (const auto& key : pre_clock_) {
                deadlines_[key] = watermark_ms_ + ttl_ms_;
                dirty_.insert(key);
            }
            pre_clock_.clear();
        }
        return true;
    }

    // Keys whose deadline has passed. The caller erases each from its own
    // state and calls forget(); this class does not know where the data
    // lives, only when it is due.
    [[nodiscard]] std::vector<std::string> expired() const {
        std::vector<std::string> out;
        if (!enabled() || deadlines_.empty()) {
            return out;
        }
        const auto now = now_ms();
        if (!now.has_value()) {
            return out;
        }
        for (const auto& [key, deadline] : deadlines_) {
            if (deadline <= *now) {
                out.push_back(key);
            }
        }
        return out;
    }

    void forget(const std::string& key) {
        deadlines_.erase(key);
        dirty_.erase(key);
        pre_clock_.erase(key);
        ++expired_total_;
    }

    // Persist the deadlines touched since the last flush. Runs alongside
    // the operator's own state flush so a restore sees deadlines consistent
    // with the entries they belong to.
    void flush(DeadlineSlot& slot) {
        if (!enabled() || dirty_.empty()) {
            return;
        }
        for (const auto& key : dirty_) {
            if (const auto it = deadlines_.find(key); it != deadlines_.end()) {
                slot.put(key, it->second);
            }
        }
        dirty_.clear();
    }

    // Also erase the persisted deadline for an evicted key, so the slot
    // does not accumulate records for entries that no longer exist.
    static void erase_persisted(DeadlineSlot& slot, const std::string& key) { slot.erase(key); }

    void restore(DeadlineSlot& slot) {
        if (!enabled()) {
            return;
        }
        slot.scan([&](const std::string& key, const std::int64_t& deadline) {
            deadlines_[key] = deadline;
        });
    }

    // For a restoring operator scanning its restored DATA keys: a key that
    // has data but no persisted deadline was pre-clock at snapshot time
    // (touched before the job's first watermark, crashed before one
    // arrived). Enrolling it here re-creates exactly the pre-crash
    // position, so the first post-restore watermark starts its clock.
    // Without this the restored key would be immortal again until data
    // happened to touch it.
    void enrol_restored_key(const std::string& key) {
        if (!enabled() || deadlines_.count(key) != 0) {
            return;
        }
        pre_clock_.insert(key);
    }

    // Publish this operator's retention position: the population currently
    // under a deadline, and the number released over its life.
    //
    // Called from the eviction sweep rather than from touch(), so the cost
    // is per advancing watermark rather than per record. A job whose
    // watermark has stalled therefore stops updating these, which is the
    // honest reading - retention has stalled with it.
    void report_metrics(OperatorId op) const {
        if (!enabled()) {
            return;
        }
        clink::metrics::state::ttl_tracked_keys_set(op.value(),
                                                    static_cast<std::int64_t>(deadlines_.size()));
        clink::metrics::state::ttl_expired_total_set(op.value(),
                                                     static_cast<std::int64_t>(expired_total_));
    }

    // The clock this tracker runs on. nullopt means event time has not
    // started; nothing can be judged expired against a clock that has not
    // started.
    [[nodiscard]] std::optional<std::int64_t> now_ms() const {
        if (!event_time_) {
            return std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                .count();
        }
        return seen_watermark_ ? std::optional<std::int64_t>{watermark_ms_} : std::nullopt;
    }

private:
    static constexpr std::int64_t kNoEventTs = std::numeric_limits<std::int64_t>::min();

    std::int64_t ttl_ms_{0};
    bool event_time_{true};
    std::int64_t watermark_ms_{0};
    bool seen_watermark_{false};
    // Newest record event time observed (kNoEventTs until the first). The
    // second clock touch() stamps deadlines from; see observe_event_time.
    std::int64_t high_event_ts_{kNoEventTs};
    std::unordered_map<std::string, std::int64_t> deadlines_;
    std::unordered_set<std::string> dirty_;
    // Keys touched before the first watermark, awaiting the clock. See
    // touch(). Runtime-only by design: at flush time each of these either
    // has no deadline yet (nothing true to persist) or was promoted into
    // deadlines_ by the first advance. A key restored WITH data but WITHOUT
    // a persisted deadline was pre-clock at snapshot time; the restoring
    // operator re-enrols it via touch() on its next write, or it waits for
    // enrol_restored_key() where the operator scans its restored keys.
    std::unordered_set<std::string> pre_clock_;
    std::uint64_t expired_total_{0};
};

// The two op params the planner stamps on any operator that can enforce
// retention. Named once so the planner and every factory agree.
inline constexpr const char* kStateTtlMsParam = "state_ttl_ms";
inline constexpr const char* kStateTtlDomainParam = "state_ttl_domain";

}  // namespace clink::sql
