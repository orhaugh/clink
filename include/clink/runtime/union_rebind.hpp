#pragma once

// Mid-run membership for a union input stage (hot rescale downstream
// rebind, design record 008).
//
// The task downstream of a rescaled operator keeps running across the
// cutover; what changes is its input set: the old subtasks' channels end
// (barrier C, then close), and the new subtasks' channels join carrying
// records after C. The union runner owns its channel vector on its own
// thread, so the join is a hand-off: the worker's rebind path binds a new
// inbound bridge, pumps it into a fresh channel on a worker-owned thread,
// and splices that channel here; the union runner drains the slot at its
// loop head and grows its poll set - single writer, no live Dag mutation.
//
// Admission defers to alignment: MultiInputAlignment::add_input refuses
// while a barrier is in flight (its delivery bitmaps were sized to the
// membership at first delivery), so the runner re-queues the channel and
// admits it once the barrier completes. The cutover choreography makes the
// wait momentary - the checkpoint clock pauses between the cutover
// checkpoint and Complete - but correctness never depends on that.

#include <atomic>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "clink/core/stream_element.hpp"
#include "clink/runtime/bounded_channel.hpp"

namespace clink {

template <typename T>
class UnionRebindSlot {
public:
    using Channel = BoundedChannel<StreamElement<T>>;

    // Worker side: hand a new input channel to the union runner.
    void splice(std::shared_ptr<Channel> ch) {
        {
            std::lock_guard lock(mu_);
            pending_.push_back(std::move(ch));
        }
    }

    // Runner side: take everything currently pending. The runner admits
    // each through alignment and re-queues any the aligner refuses.
    [[nodiscard]] std::vector<std::shared_ptr<Channel>> take() {
        std::lock_guard lock(mu_);
        return std::exchange(pending_, {});
    }

    // Runner side: put a refused channel back at the front so admission
    // order is preserved across retries.
    void requeue_front(std::shared_ptr<Channel> ch) {
        std::lock_guard lock(mu_);
        pending_.insert(pending_.begin(), std::move(ch));
    }

    [[nodiscard]] bool empty() const {
        std::lock_guard lock(mu_);
        return pending_.empty();
    }

    // Hold the union open across the cutover gap. The old inputs end
    // (barrier C, then close) BEFORE the new subtasks exist to splice, and
    // a union whose entire membership has closed normally exits - taking
    // the still-running downstream task with it. The arm (which precedes
    // C) sets the hold; the union clears it once it has admitted a spliced
    // channel, at which point the membership is live again. A cancel
    // overrides the hold - the runner's stop predicate is checked first.
    void set_hold_open(bool hold) noexcept { hold_open_.store(hold, std::memory_order_release); }
    [[nodiscard]] bool holding_open() const noexcept {
        return hold_open_.load(std::memory_order_acquire);
    }

private:
    mutable std::mutex mu_;
    std::vector<std::shared_ptr<Channel>> pending_;
    std::atomic<bool> hold_open_{false};
};

}  // namespace clink
