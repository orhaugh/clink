#pragma once

#include <cstddef>
#include <cstdint>
#include <set>
#include <vector>

#include "clink/core/types.hpp"  // CheckpointId

namespace clink::cluster {

// Bounds checkpoint storage by retaining only the most recent
// `num_retained` *completed* checkpoints. Drive it from the
// globally-complete signal (CommitCheckpoint): record_completed(id)
// returns the checkpoint ids that are now subsumed and should be purged.
//
// Safe because it only ever drops checkpoints older than the retained
// window; the latest completed checkpoint - the one recovery restores
// from - is always kept. num_retained is clamped to at least 1 so the
// latest can never be purged. Recording the same id twice is idempotent.
class CheckpointRetention {
public:
    explicit CheckpointRetention(std::size_t num_retained)
        : num_retained_(num_retained == 0 ? 1 : num_retained) {}

    // Record a newly-completed checkpoint and return the ids to purge
    // (oldest first) so the retained set holds at most num_retained.
    std::vector<CheckpointId> record_completed(CheckpointId id) {
        completed_.insert(id.value());
        std::vector<CheckpointId> to_purge;
        while (completed_.size() > num_retained_) {
            auto oldest = completed_.begin();
            to_purge.push_back(CheckpointId{*oldest});
            completed_.erase(oldest);
        }
        return to_purge;
    }

    [[nodiscard]] std::size_t num_retained() const noexcept { return num_retained_; }
    [[nodiscard]] std::size_t retained_count() const noexcept { return completed_.size(); }

private:
    std::size_t num_retained_;
    std::set<std::uint64_t> completed_;
};

}  // namespace clink::cluster
