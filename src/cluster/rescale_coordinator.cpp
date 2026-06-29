#include "clink/cluster/rescale_coordinator.hpp"

#include <algorithm>
#include <utility>

#include "clink/metrics/orchestration_metrics.hpp"

namespace clink::cluster {

std::string_view to_string(RescaleState s) noexcept {
    switch (s) {
        case RescaleState::Idle:
            return "Idle";
        case RescaleState::Preparing:
            return "Preparing";
        case RescaleState::Draining:
            return "Draining";
        case RescaleState::CuttingOver:
            return "CuttingOver";
        case RescaleState::Complete:
            return "Complete";
        case RescaleState::Aborted:
            return "Aborted";
    }
    return "?";
}

void RescaleCoordinator::register_operator(std::string op_id,
                                           std::uint32_t current_parallelism,
                                           std::uint32_t min_parallelism,
                                           std::uint32_t max_parallelism) {
    std::lock_guard lock(mu_);
    auto& entry = ops_[op_id];
    entry.status.op_id = std::move(op_id);
    entry.status.current_parallelism = current_parallelism;
    entry.status.target_parallelism = current_parallelism;
    entry.status.min_parallelism = min_parallelism;
    entry.status.max_parallelism = max_parallelism;
    entry.status.state = RescaleState::Idle;
    entry.status.cutover_checkpoint = 0;
    entry.status.old_subtasks_drained = 0;
    entry.status.new_subtasks_ready = 0;
    entry.drained_subtasks.clear();
    entry.ready_subtasks.clear();
}

RescaleCoordinator::RequestResult RescaleCoordinator::request_rescale(
    const std::string& op_id, std::uint32_t new_parallelism) {
    std::lock_guard lock(mu_);
    auto it = ops_.find(op_id);
    if (it == ops_.end()) {
        clink::metrics::orch::rescale_request_rejected();
        return RequestResult{.ok = false, .reason = "operator '" + op_id + "' not registered"};
    }
    auto& entry = it->second;
    auto& st = entry.status;

    if (st.min_parallelism == 0 && st.max_parallelism == 0) {
        clink::metrics::orch::rescale_request_rejected();
        return RequestResult{.ok = false,
                             .reason = "operator '" + op_id +
                                       "' has no autoscale bounds "
                                       "(min_parallelism / max_parallelism unset)"};
    }
    if (new_parallelism < st.min_parallelism) {
        clink::metrics::orch::rescale_request_rejected();
        return RequestResult{.ok = false,
                             .reason = "requested parallelism " + std::to_string(new_parallelism) +
                                       " below min_parallelism " +
                                       std::to_string(st.min_parallelism)};
    }
    if (new_parallelism > st.max_parallelism) {
        clink::metrics::orch::rescale_request_rejected();
        return RequestResult{.ok = false,
                             .reason = "requested parallelism " + std::to_string(new_parallelism) +
                                       " above max_parallelism " +
                                       std::to_string(st.max_parallelism)};
    }
    if (new_parallelism == st.current_parallelism) {
        clink::metrics::orch::rescale_request_rejected();
        return RequestResult{.ok = false, .reason = "requested parallelism equals current; no-op"};
    }
    if (st.state != RescaleState::Idle && st.state != RescaleState::Complete &&
        st.state != RescaleState::Aborted) {
        clink::metrics::orch::rescale_request_rejected();
        return RequestResult{.ok = false,
                             .reason = "operator '" + op_id +
                                       "' already has a rescale in progress (state=" +
                                       std::string{to_string(st.state)} + ")"};
    }

    const auto prev_state = st.state;
    st.target_parallelism = new_parallelism;
    st.state = RescaleState::Preparing;
    st.cutover_checkpoint = 0;
    st.old_subtasks_drained = 0;
    st.new_subtasks_ready = 0;
    entry.drained_subtasks.clear();
    entry.ready_subtasks.clear();
    clink::metrics::orch::rescale_request_accepted();
    clink::metrics::orch::rescale_state_transition(std::string{to_string(prev_state)}.c_str(),
                                                   "Preparing");
    return RequestResult{.ok = true, .accepted_target = new_parallelism};
}

bool RescaleCoordinator::mark_checkpoint_ready(const std::string& op_id,
                                               std::uint64_t checkpoint_id) {
    std::lock_guard lock(mu_);
    auto it = ops_.find(op_id);
    if (it == ops_.end())
        return false;
    auto& st = it->second.status;
    if (st.state != RescaleState::Preparing)
        return false;
    st.cutover_checkpoint = checkpoint_id;
    st.state = RescaleState::Draining;
    clink::metrics::orch::rescale_state_transition("Preparing", "Draining");
    return true;
}

bool RescaleCoordinator::mark_old_drained(const std::string& op_id, std::uint32_t subtask_idx) {
    std::lock_guard lock(mu_);
    auto it = ops_.find(op_id);
    if (it == ops_.end())
        return false;
    auto& entry = it->second;
    auto& st = entry.status;
    if (st.state != RescaleState::Draining)
        return false;
    // Idempotent ack: re-acking the same subtask is harmless.
    if (entry.drained_subtasks.insert(subtask_idx).second) {
        st.old_subtasks_drained = static_cast<std::uint32_t>(entry.drained_subtasks.size());
    }
    // All old subtasks drained? -> transition to CuttingOver.
    if (st.old_subtasks_drained >= st.current_parallelism) {
        st.state = RescaleState::CuttingOver;
        clink::metrics::orch::rescale_state_transition("Draining", "CuttingOver");
    }
    return true;
}

bool RescaleCoordinator::mark_new_ready(const std::string& op_id, std::uint32_t subtask_idx) {
    std::lock_guard lock(mu_);
    auto it = ops_.find(op_id);
    if (it == ops_.end())
        return false;
    auto& entry = it->second;
    auto& st = entry.status;
    if (st.state != RescaleState::CuttingOver)
        return false;
    if (entry.ready_subtasks.insert(subtask_idx).second) {
        st.new_subtasks_ready = static_cast<std::uint32_t>(entry.ready_subtasks.size());
    }
    // All new subtasks ready? -> Complete; commit the new parallelism.
    if (st.new_subtasks_ready >= st.target_parallelism) {
        st.current_parallelism = st.target_parallelism;
        st.state = RescaleState::Complete;
        clink::metrics::orch::rescale_state_transition("CuttingOver", "Complete");
    }
    return true;
}

bool RescaleCoordinator::abort(const std::string& op_id, std::string /*reason*/) {
    std::lock_guard lock(mu_);
    auto it = ops_.find(op_id);
    if (it == ops_.end())
        return false;
    auto& entry = it->second;
    auto& st = entry.status;
    if (st.state == RescaleState::Idle || st.state == RescaleState::Complete) {
        return false;  // nothing to abort
    }
    const auto prev = st.state;
    st.state = RescaleState::Aborted;
    st.target_parallelism = st.current_parallelism;
    st.cutover_checkpoint = 0;
    st.old_subtasks_drained = 0;
    st.new_subtasks_ready = 0;
    entry.drained_subtasks.clear();
    entry.ready_subtasks.clear();
    clink::metrics::orch::rescale_state_transition(std::string{to_string(prev)}.c_str(), "Aborted");
    clink::metrics::orch::rescale_aborted();
    return true;
}

std::optional<OperatorRescaleStatus> RescaleCoordinator::status(const std::string& op_id) const {
    std::lock_guard lock(mu_);
    auto it = ops_.find(op_id);
    if (it == ops_.end())
        return std::nullopt;
    return it->second.status;
}

std::vector<OperatorRescaleStatus> RescaleCoordinator::all() const {
    std::lock_guard lock(mu_);
    std::vector<OperatorRescaleStatus> out;
    out.reserve(ops_.size());
    for (const auto& [_, entry] : ops_) {
        out.push_back(entry.status);
    }
    std::sort(
        out.begin(), out.end(), [](const auto& a, const auto& b) { return a.op_id < b.op_id; });
    return out;
}

}  // namespace clink::cluster
