#pragma once

// Phase 29c: per-operator RescaleCoordinator on the JM.
//
// Tracks per-operator rescale state across the lifecycle:
//
//   Idle
//     -> request_rescale -> Preparing (next checkpoint will gate
//                                       the cutover)
//   Preparing
//     -> mark_checkpoint_ready -> Draining (old subtasks asked to
//                                            emit DrainMarker
//                                            downstream)
//   Draining
//     -> all old subtasks acked drain -> CuttingOver (new subtasks
//                                                      coming online
//                                                      from checkpoint)
//   CuttingOver
//     -> all new subtasks acked ready -> Complete (current_parallelism
//                                                   = target_parallelism)
//   any state
//     -> abort -> Aborted
//
// Each transition is mutex-protected; the coordinator is safe to
// drive from JM RPC handlers running on multiple threads.
//
// Phase 29c scope: the state machine + bounds validation. The actual
// JM-side dispatch (BeginRescale message to old TMs, deploying new
// subtasks, the DrainMarker emit) is Phase 29d's job; this object
// is the state record those handlers update. Tests drive the state
// machine end-to-end without spinning up a cluster.

#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace clink::cluster {

enum class RescaleState : std::uint8_t {
    Idle = 0,
    Preparing = 1,
    Draining = 2,
    CuttingOver = 3,
    Complete = 4,
    Aborted = 5,
};

// Snapshot of one operator's rescale lifecycle. Returned by status()
// / all() under the coordinator's lock; safe to inspect after release.
struct OperatorRescaleStatus {
    std::string op_id;
    std::uint32_t current_parallelism{0};
    std::uint32_t target_parallelism{0};
    std::uint32_t min_parallelism{0};
    std::uint32_t max_parallelism{0};
    RescaleState state{RescaleState::Idle};
    // The checkpoint id the cutover is gated on, once known. Zero
    // while state is Idle or Preparing (before a checkpoint has
    // been chosen).
    std::uint64_t cutover_checkpoint{0};
    // Diagnostic ack tallies. The coordinator uses them to fire the
    // Draining -> CuttingOver and CuttingOver -> Complete
    // transitions when every old subtask has drained / every new
    // subtask has come online.
    std::uint32_t old_subtasks_drained{0};
    std::uint32_t new_subtasks_ready{0};
};

class RescaleCoordinator {
public:
    // Register an operator with its current parallelism + bounds.
    // Idempotent: re-registering the same op_id overwrites the
    // entry (typical case: the JM rebuilds coordinator state on
    // recovery from the JobGraphSpec).
    //
    // min == 0 && max == 0 signals "no autoscaling for this operator"
    // - subsequent request_rescale calls are rejected with reason
    // "operator not scalable." Mirrors the OperatorSpec convention
    // from Phase 29a.
    void register_operator(std::string op_id,
                           std::uint32_t current_parallelism,
                           std::uint32_t min_parallelism,
                           std::uint32_t max_parallelism);

    // Result of a request_rescale call. Mirrors the std::expected
    // shape without requiring C++23 std::expected. `ok=true` +
    // `accepted_target` for accepted requests; `ok=false` + `reason`
    // for rejections.
    struct RequestResult {
        bool ok{false};
        std::uint32_t accepted_target{0};
        std::string reason;
    };

    // Request a rescale. Validates against the operator's bounds and
    // refuses if a rescale is already in-flight for this op. On
    // accept, transitions the operator's state to Preparing - the JM
    // dispatch (29d) will fire the next-checkpoint gate and drive
    // the state machine forward via the mark_* methods.
    RequestResult request_rescale(const std::string& op_id, std::uint32_t new_parallelism);

    // Transition Preparing -> Draining. Called by the JM when the
    // next checkpoint has been triggered; the coordinator records
    // the checkpoint id so subsequent mark_old_drained / mark_new_ready
    // calls associate to the same cutover.
    //
    // Returns true on success; false if the op is not in Preparing
    // or is unknown.
    bool mark_checkpoint_ready(const std::string& op_id, std::uint64_t checkpoint_id);

    // Transition Draining -> CuttingOver. Caller indicates which
    // old-subtask index has drained; once every old subtask
    // (current_parallelism of them) has acked, the state advances.
    bool mark_old_drained(const std::string& op_id, std::uint32_t subtask_idx);

    // Transition CuttingOver -> Complete. Caller indicates which
    // new-subtask index is ready; once every new subtask
    // (target_parallelism of them) has acked, the rescale is done
    // and current_parallelism is set to target_parallelism.
    bool mark_new_ready(const std::string& op_id, std::uint32_t subtask_idx);

    // Force-abort an in-progress rescale (any state except Idle /
    // Complete). Typical trigger: a hard TM failure during cutover
    // that makes the rescale unrecoverable. The operator returns to
    // its pre-rescale parallelism; current_parallelism is unchanged.
    bool abort(const std::string& op_id, std::string reason);

    // Look up status for one operator. Returns nullopt if the op
    // isn't registered.
    [[nodiscard]] std::optional<OperatorRescaleStatus> status(const std::string& op_id) const;

    // Snapshot every registered operator's status. Stable order
    // by op_id so tests / dashboards see deterministic output.
    [[nodiscard]] std::vector<OperatorRescaleStatus> all() const;

private:
    struct Entry {
        OperatorRescaleStatus status;
        // Sets used during Draining / CuttingOver to track which
        // subtasks have already acked. Cleared on transition.
        std::unordered_set<std::uint32_t> drained_subtasks;
        std::unordered_set<std::uint32_t> ready_subtasks;
    };

    mutable std::mutex mu_;
    std::unordered_map<std::string, Entry> ops_;
};

// String accessor for diagnostic logging - returns "Idle" / "Preparing"
// / etc. for the given state value.
[[nodiscard]] std::string_view to_string(RescaleState s) noexcept;

}  // namespace clink::cluster
