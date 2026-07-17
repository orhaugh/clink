#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/core/types.hpp"
#include "clink/state/state_backend.hpp"

namespace clink {

// Configuration for CheckpointCoordinator. Defined at namespace scope so the
// in-class default-member initializers are fully visible before any default
// argument that uses {} would be evaluated.
struct CheckpointCoordinatorConfig {
    std::chrono::milliseconds interval{0};  // 0 disables periodic checkpointing
    std::chrono::milliseconds timeout{60'000};
    // Default barrier mode. The coordinator stamps every
    // issued barrier with this mode unless a per-trigger override is
    // supplied. JobConfig::unaligned_checkpoints is plumbed here to
    // pick the global default; per-operator overrides are surfaced
    // separately.
    CheckpointBarrier::Mode default_mode{CheckpointBarrier::Mode::Aligned};
};

// CheckpointCoordinator is the single source of truth for checkpoint lifecycle.
//
// In  terms, the coordinator periodically broadcasts a barrier to every
// source. Sources insert the barrier into their output streams; operators
// align on barriers from all input channels, snapshot their state, and ack
// back to the coordinator. When all operators have acked, the checkpoint is
// considered complete and can be promoted to a recovery point.
//
// For the MVP we implement only the bookkeeping side: barrier creation,
// per-operator ack tracking, completion detection, and snapshot persistence
// via the StateBackend. The actual broadcast-into-the-DAG path is wired in
// later (see docs/checkpointing.md for the staged plan).
class CheckpointCoordinator {
public:
    using Clock = std::chrono::steady_clock;
    using Config = CheckpointCoordinatorConfig;

    using OnComplete = std::function<void(CheckpointId)>;
    using OnAbort = std::function<void(CheckpointId, std::string_view reason)>;
    using BarrierInjector = std::function<void(CheckpointBarrier)>;

    explicit CheckpointCoordinator(std::shared_ptr<StateBackend> backend, Config cfg = {});

    ~CheckpointCoordinator();

    CheckpointCoordinator(const CheckpointCoordinator&) = delete;
    CheckpointCoordinator& operator=(const CheckpointCoordinator&) = delete;
    CheckpointCoordinator(CheckpointCoordinator&&) = delete;
    CheckpointCoordinator& operator=(CheckpointCoordinator&&) = delete;

    // Begin a new checkpoint; returns the assigned id and a barrier to inject
    // into source streams. Operators participating in this checkpoint must be
    // declared via register_operator() *before* triggering, otherwise their
    // ack will be ignored as "unknown".
    //
    // The returned barrier carries the coordinator's configured
    // default_mode. An explicit per-trigger override can be passed
    // to force a specific mode for this one checkpoint (used by 26c's
    // adaptive policy when a backpressure signal flips the
    // engine into unaligned mode for the next checkpoint only).
    CheckpointBarrier trigger();
    CheckpointBarrier trigger(CheckpointBarrier::Mode mode_override);

    // Register an operator as a participant in future checkpoints.
    void register_operator(OperatorId id);

    // Acknowledge that the named operator has snapshotted its state for the
    // given checkpoint id. Returns true if this ack completed the checkpoint.
    bool acknowledge(CheckpointId id, OperatorId op);

    // Abort an in-flight checkpoint. Idempotent.
    void abort(CheckpointId id, std::string_view reason);

    // Has this checkpoint been fully acknowledged?
    bool is_complete(CheckpointId id) const;

    // Returns the most recently completed checkpoint id, or 0 if none.
    CheckpointId last_completed_id() const;

    void set_on_complete(OnComplete cb) { on_complete_ = std::move(cb); }
    void set_on_abort(OnAbort cb) { on_abort_ = std::move(cb); }

    // Adaptive-mode resolver. Called on every trigger to
    // decide which mode to stamp on this checkpoint's barrier; the
    // resolver sees the prospective checkpoint id and the
    // coordinator's config default, and returns the mode to use.
    //
    // Typical use case: the resolver consults a backpressure signal
    // (sum of saturation_events across this job's NetworkChannelSinks
    // since the last checkpoint) and returns Unaligned when the
    // signal crosses a threshold, Aligned otherwise. The plumbing
    // from NetworkChannelSink up to the coordinator is left to the
    // hosting runtime (LocalExecutor / Worker); the coordinator
    // only exposes the resolver seam.
    //
    // When not set, the coordinator falls back to the configured
    // default_mode.
    using ModeResolver =
        std::function<CheckpointBarrier::Mode(CheckpointId, CheckpointBarrier::Mode default_mode)>;
    void set_mode_resolver(ModeResolver resolver) { mode_resolver_ = std::move(resolver); }

    StateBackend& backend() noexcept { return *backend_; }

    Config config() const noexcept { return cfg_; }

    // Bind the set of source-side barrier injectors. Each call replaces any
    // previously-bound set. Sources must be registered as operators (via
    // register_operator) before periodic triggering, otherwise their acks
    // will be ignored.
    void set_source_injectors(std::vector<BarrierInjector> injectors);

    // Start a background thread that fires trigger() every cfg_.interval and
    // pushes the resulting barrier into each registered source via the bound
    // injectors. No-op if cfg_.interval == 0 or if already running.
    void start_periodic_trigger();

    // Stop the periodic trigger thread. Idempotent.
    void stop_periodic_trigger();

    // Returns true if the periodic-trigger thread is currently running.
    bool periodic_running() const noexcept {
        return periodic_running_.load(std::memory_order_acquire);
    }

private:
    struct InFlight {
        CheckpointId id;
        Clock::time_point started_at;
        std::unordered_set<OperatorId> pending;  // expected acks
        bool aborted{false};
    };

    std::shared_ptr<StateBackend> backend_;
    Config cfg_;
    mutable std::mutex mu_;
    std::unordered_set<OperatorId> registered_ops_;
    std::unordered_map<CheckpointId, InFlight> in_flight_;
    CheckpointId last_completed_{};
    std::atomic<std::uint64_t> next_id_{1};

    OnComplete on_complete_{};
    OnAbort on_abort_{};
    ModeResolver mode_resolver_{};

    // Periodic-trigger machinery
    std::vector<BarrierInjector> source_injectors_{};
    std::atomic<bool> periodic_running_{false};
    std::atomic<bool> periodic_stop_{false};
    std::condition_variable periodic_cv_;
    std::mutex periodic_mu_;
    std::thread periodic_thread_;

    void periodic_loop();
};

}  // namespace clink
