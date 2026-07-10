#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace clink {

// TimerService manages processing-time timers for one operator subtask.
//
// clink models processing-time timers: user code calls
// register_processing_time_timer(t, k) and the runtime fires
// on_processing_time_timer(t, k) on the operator's thread once
// system_clock::now() >= t. Firing happens between input pops so the
// operator's process()/on_timer() callbacks see a single-threaded view
// of state (no synchronisation needed).
//
// Keys are opaque strings - users encode whatever they need (an entity
// id, a window identifier, a key+window tuple). This matches
// implicit "current key" model without forcing virtual templates on the
// operator base class.
//
// Event-time timers are out of scope for v1; they'll layer on top of
// the same priority queue with a watermark-driven trigger.
class TimerService {
public:
    // Optional injection point for tests: supply a clock that returns
    // milliseconds-since-epoch. Default uses system_clock.
    using NowFn = std::function<std::int64_t()>;

    TimerService() = default;
    explicit TimerService(NowFn now_fn) : now_fn_(std::move(now_fn)) {}

    // Add a timer. Re-registering the same (timestamp, key) is idempotent.
    void register_processing_time_timer(std::int64_t timestamp_ms, std::string key = {}) {
        timers_.emplace(timestamp_ms, std::move(key));
    }

    // Remove a timer. No-op if not present.
    void delete_processing_time_timer(std::int64_t timestamp_ms, std::string key = {}) {
        timers_.erase(std::make_pair(timestamp_ms, std::move(key)));
    }

    // Peek the earliest registered timestamp, or nullopt if no timers
    // are registered.
    std::optional<std::int64_t> next_timestamp() const noexcept {
        if (timers_.empty()) {
            return std::nullopt;
        }
        return timers_.begin()->first;
    }

    bool empty() const noexcept { return timers_.empty(); }
    std::size_t size() const noexcept { return timers_.size(); }

    // Current wall-clock time in milliseconds. Uses the injected NowFn
    // when supplied, otherwise system_clock.
    std::int64_t now_ms() const {
        if (now_fn_) {
            return now_fn_();
        }
        using namespace std::chrono;
        return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    }

    // Pop and fire all timers whose timestamp is <= now_ms. Callback
    // receives (timestamp, key). Returns the number of timers fired.
    //
    // The callback may register new timers; they are NOT fired in the
    // same poll call (only seen on the next poll). This matches
    // semantics and prevents starvation when on_timer schedules a follow-up.
    // To enforce that, we snapshot the due entries up-front and iterate
    // the snapshot - new registrations land in `timers_` but not the
    // snapshot, so they're deferred to the next poll.
    template <typename F>
    std::size_t poll_due(std::int64_t now_ms, F&& cb) {
        std::vector<std::pair<std::int64_t, std::string>> to_fire;
        for (auto it = timers_.begin(); it != timers_.end() && it->first <= now_ms;) {
            to_fire.push_back(*it);
            it = timers_.erase(it);
        }
        for (auto& entry : to_fire) {
            cb(entry.first, entry.second);
        }
        return to_fire.size();
    }

    // Convenience: poll using TimerService's own clock.
    template <typename F>
    std::size_t poll_due_now(F&& cb) {
        return poll_due(now_ms(), std::forward<F>(cb));
    }

    // Event-time timer surface (TimerService.registerEventTime
    // Timer equivalent). Timers fire when an UPSTREAM watermark advances
    // past the registered timestamp, not on wall-clock progress. The
    // operator runner is responsible for calling poll_due_event_time
    // each time it forwards a watermark.
    void register_event_time_timer(std::int64_t timestamp_ms, std::string key = {}) {
        event_timers_.emplace(timestamp_ms, std::move(key));
    }

    void delete_event_time_timer(std::int64_t timestamp_ms, std::string key = {}) {
        event_timers_.erase(std::make_pair(timestamp_ms, std::move(key)));
    }

    std::optional<std::int64_t> next_event_timestamp() const noexcept {
        if (event_timers_.empty()) {
            return std::nullopt;
        }
        return event_timers_.begin()->first;
    }

    // Fire every event-time timer whose timestamp is <= the supplied
    // watermark timestamp. Same snapshot-then-iterate discipline as
    // poll_due so a callback that registers a new timer doesn't get
    // fired in the same poll. Returns the count fired.
    template <typename F>
    std::size_t poll_due_event_time(std::int64_t watermark_ts, F&& cb) {
        std::vector<std::pair<std::int64_t, std::string>> to_fire;
        for (auto it = event_timers_.begin();
             it != event_timers_.end() && it->first <= watermark_ts;) {
            to_fire.push_back(*it);
            it = event_timers_.erase(it);
        }
        for (auto& entry : to_fire) {
            cb(entry.first, entry.second);
        }
        return to_fire.size();
    }

    bool event_timers_empty() const noexcept { return event_timers_.empty(); }
    std::size_t event_timers_size() const noexcept { return event_timers_.size(); }

    void clear() noexcept {
        timers_.clear();
        event_timers_.clear();
    }

    // Replace the clock. The operator test harness points this at its
    // manual clock so an operator reading now_ms() sees deterministic
    // time; the runner never calls it (it injects via the constructor).
    void set_now_fn(NowFn now_fn) { now_fn_ = std::move(now_fn); }

    // Non-destructive inspection of the registered timer sets, ordered by
    // (timestamp, key) - the firing order, with lexicographic key order
    // breaking timestamp ties. Diagnostics and the test harness read
    // these; production firing goes through poll_due*.
    using TimerSet = std::set<std::pair<std::int64_t, std::string>>;
    const TimerSet& processing_time_timers() const noexcept { return timers_; }
    const TimerSet& event_time_timers() const noexcept { return event_timers_; }

    // ---- Checkpointing -------------------------------------------------
    //
    // Serialize every registered timer (processing-time first, then
    // event-time) to a self-describing byte blob so the runtime can
    // persist it through a checkpoint and reload it on restore. Layout
    // (little-endian, fixed-width):
    //   [u32 n_processing]
    //     repeat: [i64 timestamp][u32 key_len][key bytes]
    //   [u32 n_event]
    //     repeat: [i64 timestamp][u32 key_len][key bytes]
    std::vector<std::byte> serialize() const {
        std::vector<std::byte> out;
        auto put_u32 = [&out](std::uint32_t v) {
            for (int i = 0; i < 4; ++i) {
                out.push_back(static_cast<std::byte>((v >> (i * 8)) & 0xFF));
            }
        };
        auto put_i64 = [&out](std::int64_t v) {
            const auto u = static_cast<std::uint64_t>(v);
            for (int i = 0; i < 8; ++i) {
                out.push_back(static_cast<std::byte>((u >> (i * 8)) & 0xFF));
            }
        };
        auto put_set = [&](const std::set<std::pair<std::int64_t, std::string>>& s) {
            put_u32(static_cast<std::uint32_t>(s.size()));
            for (const auto& [ts, key] : s) {
                put_i64(ts);
                put_u32(static_cast<std::uint32_t>(key.size()));
                for (char c : key) {
                    out.push_back(static_cast<std::byte>(static_cast<unsigned char>(c)));
                }
            }
        };
        put_set(timers_);
        put_set(event_timers_);
        return out;
    }

    // Replace the current timer sets with the contents of a blob produced
    // by serialize(). Returns false (leaving the service unchanged) if the
    // blob is truncated or malformed, so a corrupt checkpoint cannot
    // half-populate the service.
    //
    // `keep`, when set, filters which timers are loaded by their key - used by
    // the rescale restore path to route each timer to the subtask owning its
    // key group (a timer whose key falls outside this subtask's range is
    // skipped). Empty keeps every timer (same-parallelism restore).
    bool restore_from(const std::vector<std::byte>& bytes,
                      const std::function<bool(const std::string&)>& keep = {}) {
        std::size_t off = 0;
        const std::size_t n = bytes.size();
        auto get_u32 = [&](std::uint32_t& v) -> bool {
            if (off + 4 > n) {
                return false;
            }
            v = 0;
            for (int i = 0; i < 4; ++i) {
                v |= static_cast<std::uint32_t>(std::to_integer<unsigned char>(bytes[off + i]))
                     << (i * 8);
            }
            off += 4;
            return true;
        };
        auto get_i64 = [&](std::int64_t& v) -> bool {
            if (off + 8 > n) {
                return false;
            }
            std::uint64_t u = 0;
            for (int i = 0; i < 8; ++i) {
                u |= static_cast<std::uint64_t>(std::to_integer<unsigned char>(bytes[off + i]))
                     << (i * 8);
            }
            off += 8;
            v = static_cast<std::int64_t>(u);
            return true;
        };
        auto read_set = [&](std::set<std::pair<std::int64_t, std::string>>& s) -> bool {
            std::uint32_t count = 0;
            if (!get_u32(count)) {
                return false;
            }
            for (std::uint32_t i = 0; i < count; ++i) {
                std::int64_t ts = 0;
                if (!get_i64(ts)) {
                    return false;
                }
                std::uint32_t klen = 0;
                if (!get_u32(klen)) {
                    return false;
                }
                if (off + klen > n) {
                    return false;
                }
                std::string key(reinterpret_cast<const char*>(bytes.data() + off), klen);
                off += klen;
                if (keep && !keep(key)) {
                    continue;  // routed to another subtask on rescale
                }
                s.emplace(ts, std::move(key));
            }
            return true;
        };
        std::set<std::pair<std::int64_t, std::string>> proc;
        std::set<std::pair<std::int64_t, std::string>> evt;
        if (!read_set(proc) || !read_set(evt)) {
            return false;
        }
        timers_ = std::move(proc);
        event_timers_ = std::move(evt);
        return true;
    }

private:
    // Ordered by (timestamp, key) so peek/pop are O(log n) and
    // duplicates are naturally deduped.
    TimerSet timers_;
    TimerSet event_timers_;
    NowFn now_fn_;
};

}  // namespace clink
