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
#include "clink/runtime/columnar_split.hpp"
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
void attach_typed_group_output(
    Dag& dag,
    StageHandle<T> handle,
    const ResolvedOutputGroup& group,
    const TypeOps& type_ops,
    const std::function<void(RunnerContext::GroupCutoverHooks)>& register_group_cutover = {});

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
// `attachers` is the registry to resolve against; nullptr means the local
// default_instance(). Callers inside a dlopened plugin MUST pass the host's
// (rctx.side_output_attachers), because the .so's default_instance() is a
// different object under RTLD_LOCAL and holds only what the .so registered
// itself - see RunnerContext::side_output_attachers, and F39.
inline void attach_side_output_groups(Dag& dag,
                                      std::size_t parent_runner_idx,
                                      const std::vector<ResolvedOutputGroup>& groups,
                                      const SideOutputAttacherRegistry* attachers = nullptr) {
    for (const auto& g : groups) {
        if (g.side_output_tag.empty()) {
            continue;
        }
        if (g.peers.empty()) {
            continue;
        }
        const auto& channel = g.channel_type;
        const auto& registry =
            attachers != nullptr ? *attachers : SideOutputAttacherRegistry::default_instance();
        const auto* attach = registry.find(channel);
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
//
// When the chain's input edges carry the rescale annotation (a fan from an
// operator with declared bounds) and a register hook is supplied, the
// input stage is built REBINDABLE: the union gets a UnionRebindSlot even
// at one bridge (a lone bridge has no union to grow otherwise), and the
// registered hook's bind_new_input binds a fresh typed listener for a new
// upstream subtask, hands the worker a pump to run, and splices the stream
// into the union.
template <typename T>
StageHandle<T> build_typed_input_stage(
    Dag& dag,
    const std::vector<std::shared_ptr<void>>& bridges,
    const TypeOps* input_type_ops = nullptr,
    const OperatorChainSpec* chain = nullptr,
    const std::function<void(RunnerContext::InputRebindHooks)>& register_input_rebind = {}) {
    std::vector<StageHandle<T>> handles;
    handles.reserve(bridges.size());
    for (const auto& b : bridges) {
        auto src = std::static_pointer_cast<network::NetworkBridgeSource<T>>(b);
        handles.push_back(dag.template add_source<T>(src));
    }

    std::string upstream_op;
    if (chain != nullptr && input_type_ops != nullptr && register_input_rebind &&
        input_type_ops->bind_inbound_bridge) {
        for (const auto& e : chain->input_edges) {
            if (e.upstream_max_parallelism > 0 && !e.upstream_op_id.empty()) {
                upstream_op = e.upstream_op_id;
                break;
            }
        }
    }
    if (upstream_op.empty()) {
        if (handles.size() == 1) {
            return handles.front();
        }
        return dag.template union_streams<T>(std::move(handles));
    }

    auto slot = std::make_shared<UnionRebindSlot<T>>();
    auto merged = dag.template union_streams<T>(std::move(handles), slot);
    RunnerContext::InputRebindHooks hooks;
    hooks.upstream_op_id = upstream_op;
    hooks.bind_new_input = [slot, bind = input_type_ops->bind_inbound_bridge](
                               std::uint32_t /*new_idx*/) -> RunnerContext::BoundNewInput {
        // The SAME erased builder the original deploy used, so the new
        // listener is wired exactly as a deployed edge would be.
        auto bound = bind();
        auto relay = std::static_pointer_cast<network::NetworkBridgeSource<T>>(bound.bridge);
        // Mirrors the network channel's own receive-queue depth: this
        // channel replaces the per-edge queue an at-deploy bridge gets.
        auto ch = std::make_shared<BoundedChannel<StreamElement<T>>>(256);
        slot->splice(ch);
        RunnerContext::BoundNewInput out;
        out.port = bound.port;
        out.pump = [relay, ch] {
            relay->open();
            Emitter<T> em(ch.get());
            while (relay->produce(em)) {
            }
            ch->close();
        };
        out.cancel = [relay] { relay->cancel(); };
        return out;
    };
    register_input_rebind(std::move(hooks));
    return merged;
}

// Attach one resolved output group to a Dag stage handle. Routing:
//   * 1 peer       : single NetworkBridgeSink<T>
//   * Rebalance N  : add_split with round-robin selector, one
//                    NetworkBridgeSink<T> per branch
//   * Forward N    : fork (defensive; planner emits only Rebalance
//                    when N>1)
//
// A rescale-eligible Hash group (non-zero downstream_max_parallelism and a
// register hook) instead builds the hold-and-swap shape from design record
// 008: a gated split sized to the downstream's ceiling, SwappableBridgeSink
// branches (parked above the live count), a selector that reads the gate's
// live divisor, and a registered {gate, apply_swap} pair the worker
// dispatches arms and CutoverPeerUpdates to.
template <typename T>
void attach_typed_group_output(
    Dag& dag,
    StageHandle<T> handle,
    const ResolvedOutputGroup& group,
    const TypeOps& type_ops,
    const std::function<void(RunnerContext::GroupCutoverHooks)>& register_group_cutover) {
    auto make_sink = [&](const PeerAddress& peer) {
        auto bridge_void = type_ops.connect_outbound_bridge(peer.host, peer.data_port);
        return std::static_pointer_cast<network::NetworkBridgeSink<T>>(bridge_void);
    };

    const bool rescale_eligible = group.downstream_max_parallelism > 0 &&
                                  group.mode == RoutingMode::Hash &&
                                  static_cast<bool>(register_group_cutover);
    if (rescale_eligible) {
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
        const std::uint32_t live = static_cast<std::uint32_t>(group.peers.size());
        // Defensive max: a declared ceiling below the current parallelism
        // would park live branches; the planner validates bounds, this
        // keeps the group buildable regardless.
        const std::size_t max_branches =
            std::max<std::size_t>(group.downstream_max_parallelism, group.peers.size());
        auto gate = std::make_shared<GroupCutoverGate>(live);
        auto selector = [extractor, gate](const T& v) {
            const auto k = extractor(v);
            const auto k_bytes =
                std::span<const std::byte>{reinterpret_cast<const std::byte*>(&k), sizeof(k)};
            const auto group_id = key_group_for_key(k_bytes);
            return static_cast<int>(subtask_for_key_group(group_id, gate->live()));
        };
        // No columnar split on eligible groups yet: the columnar splitter
        // bakes its divisor at build time, and a divisor that disagrees
        // with the gate's live count after a swap would route key groups
        // to the wrong peers. Row-split routing through the live-reading
        // selector is byte-identical in DESTINATION, just not columnar.
        // Making the columnar splitter live-aware is a follow-up with its
        // own A/B; SQL paths are unaffected today because SQL operators
        // declare no rescale bounds.
        auto branches =
            dag.template add_split<T>(handle, std::move(selector), max_branches, "hash", {}, gate);
        // Swap-time inner sinks come from the SAME erased builder the
        // original deploy used, so a swapped endpoint is wired exactly as a
        // deployed one would be.
        auto connect = [connect_outbound = type_ops.connect_outbound_bridge](
                           const typename network::SwappableBridgeSink<T>::Endpoint& ep) {
            return std::static_pointer_cast<network::NetworkBridgeSink<T>>(
                connect_outbound(ep.host, ep.port));
        };
        std::vector<std::shared_ptr<network::SwappableBridgeSink<T>>> sinks;
        sinks.reserve(max_branches);
        for (std::size_t i = 0; i < max_branches; ++i) {
            std::optional<typename network::SwappableBridgeSink<T>::Endpoint> ep;
            if (i < group.peers.size()) {
                ep = {group.peers[i].host, group.peers[i].data_port};
            }
            auto sink = std::make_shared<network::SwappableBridgeSink<T>>(
                connect, std::move(ep), gate, "cutover.branch" + std::to_string(i));
            sinks.push_back(sink);
            dag.template add_sink<T>(branches[i], sink);
        }
        RunnerContext::GroupCutoverHooks hooks;
        hooks.downstream_op_id = group.downstream_op_id;
        hooks.gate = gate;
        hooks.apply_swap =
            [gate, sinks, max_branches](const std::vector<PeerAddress>& new_peers) -> bool {
            if (!gate->await_all_flushed(
                    static_cast<std::uint32_t>(max_branches), cutover_hold_timeout(), nullptr)) {
                return false;
            }
            for (std::size_t i = 0; i < sinks.size(); ++i) {
                std::optional<typename network::SwappableBridgeSink<T>::Endpoint> ep;
                if (i < new_peers.size()) {
                    ep = {new_peers[i].host, new_peers[i].data_port};
                }
                sinks[i]->swap(std::move(ep));
            }
            gate->release(static_cast<std::uint32_t>(new_peers.size()));
            return true;
        };
        register_group_cutover(std::move(hooks));
        return;
    }

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
        // Columnar keyed split: when the channel registered a columnar key
        // extractor (e.g. the SQL Row channel reads the __key sidecar
        // column), the split partitions the Arrow sidecar per peer without
        // materialising rows - the shuffle stays columnar end to end.
        // Routing is byte-identical to `selector` (same key_group maths on
        // the same int64 keys); absent extractor or a nullopt batch falls
        // back to the row split.
        std::function<std::optional<std::vector<Batch<T>>>(const Batch<T>&)> columnar_split;
#ifdef CLINK_HAS_ARROW
        if (auto columnar_keys = KeyExtractorRegistry::default_instance().find_columnar<T>(
                type_ops.channel_name, group.key_extractor_fn);
            columnar_keys) {
            columnar_split = make_keyed_columnar_split<T>(std::move(columnar_keys), n);
        }
#endif
        auto branches = dag.template add_split<T>(
            handle, std::move(selector), n, "hash", std::move(columnar_split));
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
void attach_typed_output_groups(
    Dag& dag,
    StageHandle<T> handle,
    const std::vector<ResolvedOutputGroup>& groups,
    const TypeOps& type_ops,
    OperatorChainSpec::OutputRouting routing,
    const std::string& selector_fn,
    const std::function<void(RunnerContext::GroupCutoverHooks)>& register_group_cutover = {}) {
    if (groups.empty()) {
        return;
    }
    if (groups.size() == 1) {
        attach_typed_group_output<T>(dag, handle, groups.front(), type_ops, register_group_cutover);
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
            attach_typed_group_output<T>(
                dag, branches[i], groups[i], type_ops, register_group_cutover);
        }
        return;
    }
    auto branches = dag.template fork<T>(handle, groups.size());
    for (std::size_t i = 0; i < groups.size(); ++i) {
        attach_typed_group_output<T>(dag, branches[i], groups[i], type_ops, register_group_cutover);
    }
}

}  // namespace clink::cluster
