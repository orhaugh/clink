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

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "clink/core/codec.hpp"
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
    // A no-op before event time exists: a deadline computed from a zero
    // watermark would place the key in 1970 and evict it on the first real
    // watermark that arrives.
    void touch(const std::string& key) {
        if (!enabled()) {
            return;
        }
        const auto now = now_ms();
        if (!now.has_value()) {
            return;
        }
        deadlines_[key] = *now + ttl_ms_;
        dirty_.insert(key);
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
    std::int64_t ttl_ms_{0};
    bool event_time_{true};
    std::int64_t watermark_ms_{0};
    bool seen_watermark_{false};
    std::unordered_map<std::string, std::int64_t> deadlines_;
    std::unordered_set<std::string> dirty_;
    std::uint64_t expired_total_{0};
};

// The two op params the planner stamps on any operator that can enforce
// retention. Named once so the planner and every factory agree.
inline constexpr const char* kStateTtlMsParam = "state_ttl_ms";
inline constexpr const char* kStateTtlDomainParam = "state_ttl_domain";

}  // namespace clink::sql
