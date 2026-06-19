#include "clink/cluster/rescale_dispatch.hpp"

#include <algorithm>

#include "clink/runtime/key_groups.hpp"

namespace clink::cluster {

CutoverDeployment plan_operator_cutover(
    const std::string& op_id,
    std::uint32_t current_parallelism,
    std::uint32_t target_parallelism,
    std::uint64_t cutover_checkpoint,
    const std::string& restore_from_dir,
    const DeploymentTask& template_task,
    const std::vector<std::string>& old_subtask_keys,
    std::vector<std::pair<std::string, std::uint32_t>> tm_free_slots) {
    CutoverDeployment out;
    out.teardown_keys = old_subtask_keys;

    if (current_parallelism == 0 || target_parallelism == 0) {
        out.error = "rescale: parallelism must be non-zero";
        return out;
    }
    if (current_parallelism == target_parallelism) {
        out.error = "rescale: target equals current; no-op";
        return out;
    }

    const std::uint32_t new_p = target_parallelism;
    const std::uint32_t old_p = current_parallelism;

    // Match the integer-multiple/divisor constraint used by the
    // whole-job rescale path. Non-integer scale factors aren't
    // supported because the parent-snapshot mapping below collapses.
    if (new_p > old_p) {
        if (new_p % old_p != 0) {
            out.error = "rescale: target parallelism (" + std::to_string(new_p) +
                        ") must be an integer multiple of current (" + std::to_string(old_p) + ")";
            return out;
        }
    } else {
        if (old_p % new_p != 0) {
            out.error = "rescale: current parallelism (" + std::to_string(old_p) +
                        ") must be an integer multiple of target (" + std::to_string(new_p) + ")";
            return out;
        }
    }

    std::uint32_t total_free = 0;
    for (const auto& [_, slots] : tm_free_slots) {
        total_free += slots;
    }
    if (total_free < new_p) {
        out.error = "rescale: insufficient free TM slots (" + std::to_string(total_free) +
                    ") for target parallelism " + std::to_string(new_p);
        return out;
    }

    out.new_tasks.reserve(new_p);
    std::size_t rr = 0;
    for (std::uint32_t i = 0; i < new_p; ++i) {
        DeploymentTask d = template_task;
        d.role = op_id;
        d.subtask_idx = i;
        d.data_port = 0;  // TM binds ephemerally; SubtaskListening reports the port.

        const auto range = key_group_range_for_subtask(i, new_p);
        d.key_group_first = range.first;
        d.key_group_last = range.second;

        if (new_p > old_p) {
            const std::uint32_t k_up = new_p / old_p;
            d.restore_from_subtask_idx = i / k_up;
            d.restore_from_parent_count = 1;
        } else {
            const std::uint32_t k_down = old_p / new_p;
            d.restore_from_subtask_idx = i * k_down;
            d.restore_from_parent_count = k_down;
        }

        // Greedy placement: scan starting at rr looking for a TM with
        // free capacity. We've already validated total_free >= new_p
        // above so the scan is guaranteed to find a slot.
        std::string picked_tm;
        for (std::size_t step = 0; step < tm_free_slots.size(); ++step) {
            auto& slot = tm_free_slots[(rr + step) % tm_free_slots.size()];
            if (slot.second > 0) {
                picked_tm = slot.first;
                --slot.second;
                rr = (rr + step + 1) % tm_free_slots.size();
                break;
            }
        }
        if (picked_tm.empty()) {
            out.error = "rescale: ran out of TM slots while placing subtask " + std::to_string(i);
            out.new_tasks.clear();
            return out;
        }

        out.new_tasks.emplace_back(std::move(picked_tm), std::move(d));
    }

    // Carry forward the checkpoint restore handle on every new task.
    // These are uniform across all new subtasks of the operator; the
    // per-task mapping into the snapshot is via restore_from_subtask_idx
    // + restore_from_parent_count which we set above.
    (void)cutover_checkpoint;
    (void)restore_from_dir;
    // Deploy-level fields (restore_from_dir + restore_from_checkpoint_id)
    // sit on DeployMsg, not DeploymentTask; the caller copies them into
    // the outgoing DeployMsg envelope when packaging the per-TM frames.

    out.ok = true;
    return out;
}

}  // namespace clink::cluster
