#pragma once

// CheckpointAckLedger: the acknowledgements a source owes its broker, each
// held until the checkpoint that covers it has completed.
//
// A source whose position lives on the broker (a Redis consumer group's pending
// entries, a Pub/Sub subscription's leases) cannot acknowledge a record at the
// barrier: the checkpoint can still fail after it, and the broker does not
// deliver an acknowledged record again. The ledger keeps each acknowledgement in
// the checkpoint interval its record was delivered in:
//
//   - delivered() (produce thread) adds one to the open interval;
//   - seal(id) (produce thread, from snapshot_offset) closes the open interval as
//     checkpoint id's;
//   - complete(id) (commit-dispatch thread) makes every interval sealed at or
//     below id ready to acknowledge, and none above it;
//   - abort(id) (commit-dispatch thread) releases id's interval unacknowledged,
//     for the broker to deliver again;
//   - take() (produce thread) hands over what is ready and what was released, so
//     every broker call stays on the thread that owns the client.
//
// complete() and abort() are idempotent and need no order: a repeated, late or
// overlapping notification moves only intervals still held, and never one
// sealed above the id it names. The open interval belongs to no checkpoint yet,
// so nothing moves it but seal(). outstanding() counts every acknowledgement not
// yet handed over, which a source holds against its bound before reading more.

#include <cstddef>
#include <cstdint>
#include <iterator>
#include <map>
#include <mutex>
#include <utility>
#include <vector>

namespace clink::connectors {

template <typename Ack>
class CheckpointAckLedger {
public:
    struct Handover {
        std::vector<Ack> ready;     // a completed checkpoint covers these: acknowledge them
        std::vector<Ack> released;  // their checkpoint aborted: leave them unacknowledged
    };

    void delivered(Ack ack) {
        std::lock_guard lk(mu_);
        open_.push_back(std::move(ack));
        ++outstanding_;
    }

    void seal(std::uint64_t ckpt) {
        std::lock_guard lk(mu_);
        if (open_.empty()) {
            return;
        }
        append(sealed_[ckpt], open_);
    }

    void complete(std::uint64_t ckpt) {
        std::lock_guard lk(mu_);
        for (auto it = sealed_.begin(); it != sealed_.end() && it->first <= ckpt;) {
            append(ready_, it->second);
            it = sealed_.erase(it);
        }
    }

    void abort(std::uint64_t ckpt) {
        std::lock_guard lk(mu_);
        const auto it = sealed_.find(ckpt);
        if (it == sealed_.end()) {
            return;
        }
        append(released_, it->second);
        sealed_.erase(it);
    }

    Handover take() {
        std::lock_guard lk(mu_);
        Handover h;
        h.ready.swap(ready_);
        h.released.swap(released_);
        outstanding_ -= h.ready.size() + h.released.size();
        return h;
    }

    [[nodiscard]] std::size_t outstanding() const {
        std::lock_guard lk(mu_);
        return outstanding_;
    }

    // Held per state, for tests and diagnostics.
    [[nodiscard]] std::size_t open_count() const {
        std::lock_guard lk(mu_);
        return open_.size();
    }
    [[nodiscard]] std::size_t sealed_count() const {
        std::lock_guard lk(mu_);
        std::size_t n = 0;
        for (const auto& [_, acks] : sealed_) {
            n += acks.size();
        }
        return n;
    }

private:
    static void append(std::vector<Ack>& to, std::vector<Ack>& from) {
        to.insert(
            to.end(), std::make_move_iterator(from.begin()), std::make_move_iterator(from.end()));
        from.clear();
    }

    mutable std::mutex mu_;
    std::vector<Ack> open_;
    std::map<std::uint64_t, std::vector<Ack>> sealed_;
    std::vector<Ack> ready_;
    std::vector<Ack> released_;
    std::size_t outstanding_{0};
};

}  // namespace clink::connectors
