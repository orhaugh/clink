#pragma once

// Cutover-deployment planner for adaptive rescale.
//
// When the coordinator's RescaleCoordinator transitions an operator from
// Draining to CuttingOver, the coordinator has to bring up the new-parallelism
// subtasks. This header centralises the planning math so it can be
// tested in isolation (no Coordinator, no workers, no sockets):
//
//   - Pick the (subtask_idx -> key_group_range) assignment for the
//     new parallelism using the same key-group math the coordinator uses on
//     restart.
//   - Compute restore_from_subtask_idx + restore_from_parent_count
//     per new subtask so the state backend's snapshot loader can
//     read the right SST slice from the cutover checkpoint:
//       scale-up   (new_p > old_p): k = new_p / old_p new subtasks
//         per parent. parent_idx = new_idx / k. parent_count = 1.
//         The new subtask filters the parent's snapshot down to its
//         key-group slice via kg_filter on read.
//       scale-down (new_p < old_p): k_down = old_p / new_p parents
//         per new subtask. parent_idx = new_idx * k_down. parent_count
//         = k_down. Snapshot loader concatenates the k_down parent
//         slices into one merged state.
//     Only integer scale factors are supported (mirroring the existing
//     rescale_job constraint).
//   - Greedy round-robin placement onto workers with free slot capacity.
//     Returns ok=false + a descriptive reason if total free capacity
//     is below target_parallelism.

#include <cstdint>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "clink/cluster/protocol.hpp"

namespace clink::cluster {

struct CutoverDeployment {
    // (worker_id, DeploymentTask). Each entry is one new-parallelism
    // subtask placed on the given worker. role / extra_config / peers are
    // cloned from the template; data_port stays 0 (worker picks an
    // ephemeral); restore_* + key_group_* fields are populated.
    std::vector<std::pair<std::string, DeploymentTask>> new_tasks;

    // "role:subtask_idx" keys of OLD subtasks to tear down. Caller
    // uses these to remove entries from task_records / tasks_by_worker
    // and to free the worker's slots_in_use counter. Old subtasks have
    // already drained (the coordinator is in CuttingOver by the time
    // this planner runs); their SubtaskFinished arrivals already
    // counted as drained acks, so the caller must NOT also count
    // them toward completed_count.
    std::vector<std::string> teardown_keys;

    bool ok{false};
    std::string error;
};

// Inputs:
//   op_id                  - role/operator id being rescaled (matches
//                            OperatorSpec.id and DeploymentTask.role).
//   current_parallelism    - operator's parallelism BEFORE rescale.
//                            Used to compute scale-up/down factor.
//   target_parallelism     - operator's parallelism AFTER rescale.
//   cutover_checkpoint     - checkpoint id chosen by the coordinator;
//                            new subtasks restore_from this id.
//   restore_from_dir       - checkpoint root dir; written verbatim
//                            into the Deploy message's
//                            restore_from_dir field.
//   template_task          - DeploymentTask cloned for every new
//                            subtask (role, extra_config, peers).
//                            Typically one of the old subtasks of
//                            this operator from the coordinator's task_records.
//   old_subtask_keys       - "role:idx" keys for every CURRENT
//                            subtask of this operator. Returned
//                            verbatim in teardown_keys for the caller.
//   worker_free_slots          - (worker_id, free_slot_count) snapshot. The
//                            planner picks placement greedily in
//                            list order, decrementing the local copy.
//                            Caller is responsible for committing the
//                            slot accounting back to the coordinator after
//                            calling this.
//
// Returns CutoverDeployment with ok=true on success. On failure
// (e.g. insufficient capacity, non-integer scale factor) returns
// ok=false + a descriptive error and an empty new_tasks vector.
CutoverDeployment plan_operator_cutover(
    const std::string& op_id,
    std::uint32_t current_parallelism,
    std::uint32_t target_parallelism,
    std::uint64_t cutover_checkpoint,
    const std::string& restore_from_dir,
    const DeploymentTask& template_task,
    const std::vector<std::string>& old_subtask_keys,
    std::vector<std::pair<std::string, std::uint32_t>> worker_free_slots);

}  // namespace clink::cluster
