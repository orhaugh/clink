#include "clink/cluster/row_layout_admission.hpp"

#include <algorithm>
#include <exception>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "clink/cluster/coordinator.hpp"
#include "clink/cluster/job_planner.hpp"

namespace clink::cluster {

std::string RowLayoutAdmission::describe() const {
    std::string out = "row_layout=" + std::to_string(layout);
    if (layout == kRowLayoutV1 && no_layout_bearing_op) {
        out += " (no layout-bearing operator)";
    } else if (layout == kRowLayoutV1 && !held_back_by.empty()) {
        out += " (worker '" + held_back_by + "' registered at protocol v" +
               std::to_string(held_back_version) + "; layout 2 needs v" +
               std::to_string(kRowLayoutV2ProtocolVersion) + ")";
    }
    return out;
}

RowLayoutAdmission admit_row_layout(const std::vector<HostingWorker>& hosts) {
    RowLayoutAdmission out;
    if (hosts.empty()) {
        return out;  // nothing hosts it, so nothing is admitted
    }
    const HostingWorker* first_below = nullptr;
    for (const auto& h : hosts) {
        if (effective_protocol_version(h.protocol_version) >= kRowLayoutV2ProtocolVersion) {
            continue;
        }
        if (first_below == nullptr || h.worker_id < first_below->worker_id) {
            first_below = &h;
        }
    }
    if (first_below != nullptr) {
        out.held_back_by = first_below->worker_id;
        out.held_back_version = effective_protocol_version(first_below->protocol_version);
        return out;
    }
    out.layout = kRowLayoutV2;
    return out;
}

RowLayoutAdmission admit_row_layout(const std::vector<HostingWorker>& hosts,
                                    std::size_t layout_bearing_ops) {
    if (layout_bearing_ops == 0) {
        // Nothing in the deployment writes or reads a layout-bearing schema,
        // so nothing it sends can carry a typed column: the workers do not
        // matter.
        RowLayoutAdmission out;
        out.no_layout_bearing_op = true;
        return out;
    }
    return admit_row_layout(hosts);
}

const std::vector<std::string_view>& layout_bearing_op_types() {
    // The SQL factories in src/sql/install.cpp that read the stamp. A test in
    // the SQL suite checks each is registered and honours it.
    static const std::vector<std::string_view> kTypes{
        "json_string_to_row_columnar",
        "tumbling_window_row",
        "hopping_window_row",
        "cumulate_window_row",
        "equi_join_row",
    };
    return kTypes;
}

bool parses_layout_bearing_schema(std::string_view op_type) {
    const auto& types = layout_bearing_op_types();
    return std::find(types.begin(), types.end(), op_type) != types.end();
}

namespace {

// Whether the text names a layout-bearing type at all. Most chains name none,
// and this keeps them from being parsed and re-serialised on every deploy.
bool mentions_layout_bearing_type(const std::string& text) {
    for (const auto type : layout_bearing_op_types()) {
        if (text.find(type) != std::string::npos) {
            return true;
        }
    }
    return false;
}

std::size_t stamp_op(ChainOp& op, const std::string& value) {
    if (!parses_layout_bearing_schema(op.type)) {
        return 0;
    }
    op.params[std::string{kRowLayoutParam}] = value;
    return 1;
}

}  // namespace

namespace {

// The task's chain spec when it names a layout-bearing operator, parsed;
// otherwise empty. A task whose extra_config is not a chain spec (a custom
// role owns its format, and its operators are not SQL factories) is empty
// too.
std::optional<OperatorChainSpec> layout_bearing_chain(const DeploymentTask& task) {
    if (task.extra_config.empty() || !mentions_layout_bearing_type(task.extra_config)) {
        return std::nullopt;
    }
    try {
        return OperatorChainSpec::from_json(task.extra_config);
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

// Apply `visit` to every operator of the chain, fused endpoints included, and
// sum what it returns.
template <typename Chain, typename Visit>
std::size_t visit_chain_ops(Chain& chain, Visit&& visit) {
    std::size_t n = 0;
    for (auto& op : chain.ops) {
        n += visit(op);
    }
    if (chain.fused_source.has_value()) {
        n += visit(*chain.fused_source);
    }
    if (chain.fused_sink.has_value()) {
        n += visit(*chain.fused_sink);
    }
    return n;
}

}  // namespace

std::size_t stamp_row_layout(std::vector<DeploymentTask>& tasks, std::uint32_t layout) {
    if (layout == kRowLayoutV1) {
        return 0;
    }
    const auto value = std::to_string(layout);
    std::size_t stamped = 0;
    for (auto& task : tasks) {
        auto chain = layout_bearing_chain(task);
        if (!chain.has_value()) {
            continue;
        }
        const auto in_task =
            visit_chain_ops(*chain, [&](ChainOp& op) { return stamp_op(op, value); });
        if (in_task != 0) {
            task.extra_config = chain->to_json();
            stamped += in_task;
        }
    }
    return stamped;
}

std::size_t count_layout_bearing_ops(const std::vector<DeploymentTask>& tasks) {
    std::size_t count = 0;
    for (const auto& task : tasks) {
        const auto chain = layout_bearing_chain(task);
        if (!chain.has_value()) {
            continue;
        }
        count += visit_chain_ops(*chain, [](const ChainOp& op) -> std::size_t {
            return parses_layout_bearing_schema(op.type) ? 1 : 0;
        });
    }
    return count;
}

std::optional<HostingWorker> layout_two_cutover_blocker(
    const std::vector<PlannedTask>& tasks, const std::vector<CutoverCandidate>& candidates) {
    std::vector<PlacementWorker> readers;
    const CutoverCandidate* first_older_with_room = nullptr;
    for (const auto& c : candidates) {
        if (c.free_slots == 0) {
            continue;
        }
        if (effective_protocol_version(c.protocol_version) >= kRowLayoutV2ProtocolVersion) {
            readers.push_back(PlacementWorker{c.worker_id, c.free_slots});
        } else if (first_older_with_room == nullptr ||
                   c.worker_id < first_older_with_room->worker_id) {
            first_older_with_room = &c;
        }
    }
    auto placed = tasks;
    if (assign_task_placement(placed, readers) || first_older_with_room == nullptr) {
        return std::nullopt;
    }
    return HostingWorker{first_older_with_room->worker_id,
                         effective_protocol_version(first_older_with_room->protocol_version)};
}

}  // namespace clink::cluster
