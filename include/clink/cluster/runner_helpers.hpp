#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "clink/cluster/job_planner.hpp"
#include "clink/cluster/operator_registry.hpp"  // SelectorRegistry
#include "clink/cluster/runner_registry.hpp"
#include "clink/cluster/type_registry.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/key_groups.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/runtime/network/network_bridge.hpp"

// runner_helpers.hpp - typed building blocks used by SubtaskRunners.
//
// These templates know the concrete C++ type T at compile time and
// build the typed Dag for one subtask: input stage (single bridge or
// union of bridges), the user's operator(s), output stage (single
// bridge, fork, or split routing). The same primitives back both
// built-in factory runners (registered by the clink library on
// startup) and plugin-defined runners (registered when a .so loads).
//
// All helpers live in `clink::cluster` to share namespace with the
// rest of the cluster machinery. They're header-only because each
// instantiation captures T via the type-parameter.

namespace clink::cluster {

template <typename T>
void attach_typed_group_output(Dag& dag,
                               StageHandle<T> handle,
                               const ResolvedOutputGroup& group,
                               const TypeOps& type_ops);

// SideOutputAttacherRegistry stores per-channel-name closures that
// build a typed side-output network sink. Registration is templated on
// T (the side's element type) so the closure captures
// Dag::side_output_by_index<T> and attach_typed_group_output<T>; the
// plugin runner can then wire side outputs through a channel-name
// lookup without re-instantiating templates per (Out, T) pair.
class SideOutputAttacherRegistry {
public:
    using AttachFn = std::function<void(Dag& dag,
                                        std::size_t parent_runner_idx,
                                        const std::string& tag,
                                        const ResolvedOutputGroup& group)>;

    SideOutputAttacherRegistry() = default;
    explicit SideOutputAttacherRegistry(const SideOutputAttacherRegistry* parent)
        : parent_(parent) {}

    // Register the side-output attacher for type T under `channel_name`.
    // Idempotent: re-registering replaces the previous entry. The
    // captured T is the side output's element type; TypeOps for the
    // same channel name (looked up at attach time) supplies the codec
    // for the network sink.
    template <typename T>
    void register_for_channel(std::string channel_name) {
        AttachFn fn = [channel_name](Dag& dag,
                                     std::size_t parent_runner_idx,
                                     const std::string& tag,
                                     const ResolvedOutputGroup& group) {
            const auto* ops = TypeRegistry::default_instance().find(channel_name);
            if (ops == nullptr) {
                throw std::runtime_error("side output: TypeOps missing for channel '" +
                                         channel_name + "' (tag '" + tag + "')");
            }
            auto side_handle =
                dag.template side_output_by_index<T>(parent_runner_idx, OutputTag<T>(tag));
            attach_typed_group_output<T>(dag, side_handle, group, *ops);
        };
        std::lock_guard lock(mu_);
        by_channel_[std::move(channel_name)] = std::move(fn);
    }

    const AttachFn* find(const std::string& channel_name) const {
        {
            std::lock_guard lock(mu_);
            auto it = by_channel_.find(channel_name);
            if (it != by_channel_.end()) {
                return &it->second;
            }
        }
        return parent_ != nullptr ? parent_->find(channel_name) : nullptr;
    }

    static SideOutputAttacherRegistry& default_instance() {
        static SideOutputAttacherRegistry r;
        return r;
    }

private:
    mutable std::mutex mu_;
    std::unordered_map<std::string, AttachFn> by_channel_;
    const SideOutputAttacherRegistry* parent_{nullptr};
};

// Walk a chain's output groups and wire each named side output to a
// typed network sink via the SideOutputAttacherRegistry. Main-output
// groups (empty tag) are skipped - the caller handles those via
// attach_typed_output_groups<MainOut>.
//
// `parent_runner_idx` is the runner index of the operator that emits
// the side records (i.e. the runner that owns the side channel map).
inline void attach_side_output_groups(Dag& dag,
                                      std::size_t parent_runner_idx,
                                      const std::vector<ResolvedOutputGroup>& groups) {
    for (const auto& g : groups) {
        if (g.side_output_tag.empty()) {
            continue;
        }
        if (g.peers.empty()) {
            continue;
        }
        const auto& channel = g.channel_type;
        const auto* attach = SideOutputAttacherRegistry::default_instance().find(channel);
        if (attach == nullptr) {
            throw std::runtime_error(
                "side output: no typed attacher for channel '" + channel + "' (tag '" +
                g.side_output_tag +
                "'); did you call register_type<T> for the side's element type?");
        }
        (*attach)(dag, parent_runner_idx, g.side_output_tag, g);
    }
}

// Filter view: returns only the main-output groups (those with empty
// side_output_tag). Used by runner closures so attach_typed_output_groups
// only sees the main-typed groups.
inline std::vector<ResolvedOutputGroup> main_output_groups_of(
    const std::vector<ResolvedOutputGroup>& groups) {
    std::vector<ResolvedOutputGroup> out;
    out.reserve(groups.size());
    for (const auto& g : groups) {
        if (g.side_output_tag.empty()) {
            out.push_back(g);
        }
    }
    return out;
}

// Cast `bridges` (one per chain.input_edges entry) to typed
// NetworkBridgeSource<T> shared_ptrs and wire them as Dag sources.
// Returns a single StageHandle<T> - if there's >1 input bridge, the
// helper inserts a union_streams to merge them.
template <typename T>
StageHandle<T> build_typed_input_stage(Dag& dag,
                                       const std::vector<std::shared_ptr<void>>& bridges) {
    std::vector<StageHandle<T>> handles;
    handles.reserve(bridges.size());
    for (const auto& b : bridges) {
        auto src = std::static_pointer_cast<network::NetworkBridgeSource<T>>(b);
        handles.push_back(dag.template add_source<T>(src));
    }
    if (handles.size() == 1) {
        return handles.front();
    }
    return dag.template union_streams<T>(std::move(handles));
}

// Attach one resolved output group to a Dag stage handle. Routing:
//   * 1 peer       : single NetworkBridgeSink<T>
//   * Rebalance N  : add_split with round-robin selector, one
//                    NetworkBridgeSink<T> per branch
//   * Forward N    : fork (defensive; planner emits only Rebalance
//                    when N>1)
template <typename T>
void attach_typed_group_output(Dag& dag,
                               StageHandle<T> handle,
                               const ResolvedOutputGroup& group,
                               const TypeOps& type_ops) {
    auto make_sink = [&](const PeerAddress& peer) {
        auto bridge_void = type_ops.connect_outbound_bridge(peer.host, peer.data_port);
        return std::static_pointer_cast<network::NetworkBridgeSink<T>>(bridge_void);
    };

    if (group.peers.size() == 1) {
        dag.template add_sink<T>(handle, make_sink(group.peers.front()));
        return;
    }
    if (group.mode == RoutingMode::Hash) {
        // Resolve the typed key extractor and use it as the split
        // selector. The extractor returns int64_t; reduce modulo peer
        // count to pick the destination subtask. Same key -> same
        // peer, which is what makes keyed state correct.
        if (group.key_extractor_fn.empty()) {
            throw std::runtime_error(
                "runner: Hash routing but no key_extractor_fn set on the output group");
        }
        auto extractor = KeyExtractorRegistry::default_instance().find<T>(type_ops.channel_name,
                                                                          group.key_extractor_fn);
        if (!extractor) {
            throw std::runtime_error("runner: key extractor '" + group.key_extractor_fn +
                                     "' not registered for channel '" + type_ops.channel_name +
                                     "'");
        }
        const std::size_t n = group.peers.size();
        auto selector = [extractor, n](const T& v) {
            const auto k = extractor(v);
            // Route via key_group so the same key always lands on the
            // same subtask at a given parallelism, AND rescaling
            // moves whole groups (not individual keys) - the
            // foundation for hot rescale.
            const auto k_bytes =
                std::span<const std::byte>{reinterpret_cast<const std::byte*>(&k), sizeof(k)};
            const auto group_id = key_group_for_key(k_bytes);
            return static_cast<int>(subtask_for_key_group(group_id, static_cast<std::uint32_t>(n)));
        };
        auto branches = dag.template add_split<T>(handle, std::move(selector), n, "hash");
        for (std::size_t i = 0; i < n; ++i) {
            dag.template add_sink<T>(branches[i], make_sink(group.peers[i]));
        }
        return;
    }
    if (group.mode == RoutingMode::Rebalance) {
        auto counter = std::make_shared<std::atomic<std::size_t>>(0);
        const std::size_t n = group.peers.size();
        auto selector = [counter, n](const T&) {
            return static_cast<int>(counter->fetch_add(1, std::memory_order_relaxed) % n);
        };
        auto branches = dag.template add_split<T>(handle, std::move(selector), n, "rebalance");
        for (std::size_t i = 0; i < n; ++i) {
            dag.template add_sink<T>(branches[i], make_sink(group.peers[i]));
        }
        return;
    }
    // Forward with >1 peers - planner shouldn't produce this; fork as a fallback.
    auto branches = dag.template fork<T>(handle, group.peers.size());
    for (std::size_t i = 0; i < group.peers.size(); ++i) {
        dag.template add_sink<T>(branches[i], make_sink(group.peers[i]));
    }
}

// Attach all output groups to a stage handle. Outer routing across
// groups is broadcast (Dag::fork) by default, or per-record split via
// a named selector when chain.output_routing == Split. The selector is
// looked up in SelectorRegistry keyed on T's channel name.
template <typename T>
void attach_typed_output_groups(Dag& dag,
                                StageHandle<T> handle,
                                const std::vector<ResolvedOutputGroup>& groups,
                                const TypeOps& type_ops,
                                OperatorChainSpec::OutputRouting routing,
                                const std::string& selector_fn) {
    if (groups.empty()) {
        return;
    }
    if (groups.size() == 1) {
        attach_typed_group_output<T>(dag, handle, groups.front(), type_ops);
        return;
    }
    if (routing == OperatorChainSpec::OutputRouting::Split) {
        std::function<int(const T&)> selector;
        if constexpr (std::is_same_v<T, std::int64_t>) {
            const auto* fn = SelectorRegistry::default_instance().find_int64(selector_fn);
            if (fn == nullptr) {
                throw std::runtime_error("runner: int64 selector not registered: " + selector_fn);
            }
            selector = *fn;
        } else if constexpr (std::is_same_v<T, std::string>) {
            const auto* fn = SelectorRegistry::default_instance().find_string(selector_fn);
            if (fn == nullptr) {
                throw std::runtime_error("runner: string selector not registered: " + selector_fn);
            }
            selector = *fn;
        } else {
            throw std::runtime_error(
                "runner: Split routing not supported for plugin types in v1 (need typed "
                "selector registry)");
        }
        auto branches =
            dag.template add_split<T>(handle, std::move(selector), groups.size(), "split");
        for (std::size_t i = 0; i < groups.size(); ++i) {
            attach_typed_group_output<T>(dag, branches[i], groups[i], type_ops);
        }
        return;
    }
    auto branches = dag.template fork<T>(handle, groups.size());
    for (std::size_t i = 0; i < groups.size(); ++i) {
        attach_typed_group_output<T>(dag, branches[i], groups[i], type_ops);
    }
}

}  // namespace clink::cluster
