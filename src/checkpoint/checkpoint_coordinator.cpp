#include "clink/checkpoint/checkpoint_coordinator.hpp"

#include <utility>

namespace clink {

CheckpointCoordinator::CheckpointCoordinator(std::shared_ptr<StateBackend> backend, Config cfg)
    : backend_(std::move(backend)), cfg_(cfg) {}

CheckpointCoordinator::~CheckpointCoordinator() {
    stop_periodic_trigger();
}

void CheckpointCoordinator::set_source_injectors(std::vector<BarrierInjector> injectors) {
    std::lock_guard lock(periodic_mu_);
    source_injectors_ = std::move(injectors);
}

void CheckpointCoordinator::start_periodic_trigger() {
    if (cfg_.interval.count() <= 0) {
        return;
    }
    bool expected = false;
    if (!periodic_running_.compare_exchange_strong(expected, true)) {
        return;
    }
    periodic_stop_.store(false, std::memory_order_release);
    periodic_thread_ = std::thread(&CheckpointCoordinator::periodic_loop, this);
}

void CheckpointCoordinator::stop_periodic_trigger() {
    if (!periodic_running_.load(std::memory_order_acquire)) {
        return;
    }
    periodic_stop_.store(true, std::memory_order_release);
    periodic_cv_.notify_all();
    if (periodic_thread_.joinable()) {
        periodic_thread_.join();
    }
    periodic_running_.store(false, std::memory_order_release);
}

void CheckpointCoordinator::periodic_loop() {
    while (!periodic_stop_.load(std::memory_order_acquire)) {
        {
            std::unique_lock lock(periodic_mu_);
            periodic_cv_.wait_for(lock, cfg_.interval, [this] {
                return periodic_stop_.load(std::memory_order_acquire);
            });
        }
        if (periodic_stop_.load(std::memory_order_acquire)) {
            break;
        }
        const CheckpointBarrier barrier = trigger();
        std::vector<BarrierInjector> injectors_copy;
        {
            std::lock_guard lock(periodic_mu_);
            injectors_copy = source_injectors_;
        }
        for (const auto& inj : injectors_copy) {
            inj(barrier);
        }
    }
}

void CheckpointCoordinator::register_operator(OperatorId id) {
    std::lock_guard lock(mu_);
    registered_ops_.insert(id);
}

CheckpointBarrier CheckpointCoordinator::trigger() {
    // Allocate the id under the lock, then consult the resolver
    // (without the lock; the resolver may call back into the coordinator
    // for read-only state). Apply the resolver's decision as the
    // final stamped mode.
    CheckpointId id;
    CheckpointBarrier::Mode mode = cfg_.default_mode;
    {
        std::lock_guard lock(mu_);
        id = CheckpointId{next_id_.fetch_add(1, std::memory_order_relaxed)};
        InFlight inf;
        inf.id = id;
        inf.started_at = Clock::now();
        inf.pending = registered_ops_;
        in_flight_.emplace(id, std::move(inf));
    }
    if (mode_resolver_) {
        mode = mode_resolver_(id, cfg_.default_mode);
    }
    return CheckpointBarrier{id, /*terminal=*/false, mode};
}

CheckpointBarrier CheckpointCoordinator::trigger(CheckpointBarrier::Mode mode_override) {
    // Explicit per-trigger override bypasses the resolver - caller
    // wants a specific mode for this single checkpoint regardless
    // of the adaptive logic.
    std::lock_guard lock(mu_);
    const CheckpointId id{next_id_.fetch_add(1, std::memory_order_relaxed)};
    InFlight inf;
    inf.id = id;
    inf.started_at = Clock::now();
    inf.pending = registered_ops_;
    in_flight_.emplace(id, std::move(inf));
    return CheckpointBarrier{id, /*terminal=*/false, mode_override};
}

bool CheckpointCoordinator::acknowledge(CheckpointId id, OperatorId op) {
    bool completed = false;
    OnComplete on_complete_copy;
    {
        std::lock_guard lock(mu_);
        auto it = in_flight_.find(id);
        if (it == in_flight_.end() || it->second.aborted) {
            return false;
        }
        it->second.pending.erase(op);
        if (it->second.pending.empty()) {
            // Snapshot state and mark complete.
            backend_->snapshot(id);
            last_completed_ = id;
            in_flight_.erase(it);
            completed = true;
            on_complete_copy = on_complete_;
        }
    }
    if (completed && on_complete_copy) {
        on_complete_copy(id);
    }
    return completed;
}

void CheckpointCoordinator::abort(CheckpointId id, std::string_view reason) {
    OnAbort cb_copy;
    {
        std::lock_guard lock(mu_);
        auto it = in_flight_.find(id);
        if (it == in_flight_.end()) {
            return;
        }
        it->second.aborted = true;
        cb_copy = on_abort_;
    }
    if (cb_copy) {
        cb_copy(id, reason);
    }
}

bool CheckpointCoordinator::is_complete(CheckpointId id) const {
    std::lock_guard lock(mu_);
    return last_completed_ >= id && in_flight_.find(id) == in_flight_.end();
}

CheckpointId CheckpointCoordinator::last_completed_id() const {
    std::lock_guard lock(mu_);
    return last_completed_;
}

}  // namespace clink
