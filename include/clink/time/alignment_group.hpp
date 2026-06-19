#pragma once

// Watermark alignment group - // `WatermarkStrategy.withWatermarkAlignment(group, maxDrift)`
// analogue. Lets multiple WatermarkAssignerOperators (typically
// per-source) coordinate so the fastest can be back-pressured when
// it gets too far ahead of the slowest.
//
// Design:
//   * Per group_name a shared AlignmentGroup tracks every active
//     member's current watermark.
//   * On each watermark update a member calls update_watermark().
//   * Before emitting more records, a member calls
//     wait_until_within_drift() - blocks if its own wm exceeds
//     (group_min + max_drift). The condvar wakes when the
//     min advances (any peer publishes a higher wm) or the
//     member is cancelled / leaves the group.
//
// Cancellation:
//   * Members register with a cancel token; the group calls it on
//     destructor / explicit shutdown so blocked waiters wake cleanly.
//
// Process-wide registry (AlignmentGroupRegistry) keyed by group_name.
// Tests can inject a private registry; production uses the default
// instance.

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "clink/time/watermark.hpp"

namespace clink {

class AlignmentGroup {
public:
    using MemberId = std::size_t;

    // Register a fresh member; returns its id (stable for the
    // member's lifetime). Initial watermark is Watermark::min().
    MemberId join() {
        std::lock_guard lock(mu_);
        members_.push_back(Watermark::min());
        return members_.size() - 1;
    }

    // Update member `id`'s watermark and notify waiters in case the
    // group min advanced.
    void update_watermark(MemberId id, Watermark wm) {
        {
            std::lock_guard lock(mu_);
            if (id >= members_.size()) {
                return;
            }
            if (wm.timestamp() > members_[id].timestamp()) {
                members_[id] = wm;
            }
        }
        cv_.notify_all();
    }

    // Block until my_wm - group_min <= max_drift OR the group is
    // shutdown OR the deadline passes. Returns true if alignment was
    // achieved, false on shutdown / timeout.
    template <typename Rep, typename Period>
    bool wait_until_within_drift(MemberId id,
                                 Watermark my_wm,
                                 std::int64_t max_drift_ms,
                                 std::chrono::duration<Rep, Period> max_wait) {
        std::unique_lock lock(mu_);
        const auto deadline = std::chrono::steady_clock::now() + max_wait;
        while (!shutdown_) {
            if (id < members_.size()) {
                // Publish own state, refresh min.
                if (my_wm.timestamp() > members_[id].timestamp()) {
                    members_[id] = my_wm;
                }
            }
            const auto min_wm = compute_min_locked_();
            // Saturating subtraction: a peer at Watermark::min()
            // (EventTime::min() = INT64_MIN) would overflow the raw
            // subtraction, wrap negative, and trick the comparison
            // into "we're aligned" when actually we're maximally
            // out-of-bounds. Detect the sentinel and treat drift
            // as INT64_MAX in that case.
            std::int64_t drift;
            if (min_wm.timestamp() == EventTime::min()) {
                drift = std::numeric_limits<std::int64_t>::max();
            } else {
                drift = my_wm.timestamp().millis() - min_wm.timestamp().millis();
            }
            if (drift <= max_drift_ms) {
                return true;
            }
            if (cv_.wait_until(lock, deadline) == std::cv_status::timeout) {
                return false;
            }
        }
        return false;
    }

    // Shutdown the group: wake every waiter. Members that re-join
    // after shutdown are accepted; this is purely a wake-up signal
    // for tests and JM-driven cancellation.
    void shutdown() {
        {
            std::lock_guard lock(mu_);
            shutdown_ = true;
        }
        cv_.notify_all();
    }

    // For tests / introspection.
    [[nodiscard]] Watermark min_watermark() const {
        std::lock_guard lock(mu_);
        return compute_min_locked_();
    }

    [[nodiscard]] std::size_t member_count() const {
        std::lock_guard lock(mu_);
        return members_.size();
    }

private:
    Watermark compute_min_locked_() const {
        if (members_.empty()) {
            return Watermark::min();
        }
        Watermark min_wm = Watermark::max();
        for (const auto& m : members_) {
            if (m.timestamp() < min_wm.timestamp()) {
                min_wm = m;
            }
        }
        return min_wm;
    }

    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::vector<Watermark> members_;
    bool shutdown_{false};
};

// Process-wide registry of alignment groups. Multiple
// WatermarkAssignerOperators referencing the same `group_name`
// resolve to the same AlignmentGroup instance.
class AlignmentGroupRegistry {
public:
    static AlignmentGroupRegistry& default_instance() {
        static AlignmentGroupRegistry inst;
        return inst;
    }

    std::shared_ptr<AlignmentGroup> get_or_create(const std::string& name) {
        std::lock_guard lock(mu_);
        auto it = groups_.find(name);
        if (it != groups_.end()) {
            return it->second;
        }
        auto g = std::make_shared<AlignmentGroup>();
        groups_.emplace(name, g);
        return g;
    }

    // Drop a group entirely. Subsequent get_or_create with the same
    // name returns a fresh group. Useful for test isolation.
    void erase(const std::string& name) {
        std::lock_guard lock(mu_);
        auto it = groups_.find(name);
        if (it != groups_.end()) {
            it->second->shutdown();
            groups_.erase(it);
        }
    }

private:
    std::mutex mu_;
    std::unordered_map<std::string, std::shared_ptr<AlignmentGroup>> groups_;
};

}  // namespace clink
