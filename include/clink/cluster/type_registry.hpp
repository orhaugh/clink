#pragma once

#include <any>
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <span>
#include <stdexcept>
#include <string>
#include <typeinfo>
#include <unordered_map>
#include <utility>
#include <vector>

#include "clink/cluster/operator_registry.hpp"  // KeyExtractorRegistry, SelectorRegistry
#include "clink/cluster/runner_registry.hpp"    // ResolvedOutputGroup, RoutingMode
#include "clink/core/arrow_batcher.hpp"
#include "clink/core/codec.hpp"
#include "clink/core/columnar_batcher.hpp"  // make_auto_arrow_batcher
#include "clink/runtime/dag.hpp"
#include "clink/runtime/key_groups.hpp"
#include "clink/runtime/network/network_bridge.hpp"
#include "clink/runtime/output_tag.hpp"

namespace clink::cluster {

// TypeOps is the per-channel-type record stored in TypeRegistry. Each
// registered channel ("int64", "string", "hello.Greeting", ...) gets
// one TypeOps whose closures bake in the concrete C++ type T and its
// Codec<T> at registration time. The generic role on the TM can then
// build NetworkBridgeSource<T> / NetworkBridgeSink<T> from the channel
// name alone, without dispatching on a closed enum.
//
// The closures return shared_ptr<void> bound to a typed object
// (NetworkBridgeSource<T> / NetworkBridgeSink<T>). Callers that need to
// hand the bridge to a templated Dag method static_pointer_cast back to
// the typed shared_ptr, then rely on derived-to-base implicit
// conversion (NetworkBridgeSource<T> -> Source<T>) at use site.
struct TypeOps {
    // Human-readable channel name, e.g. "hello.Greeting".
    std::string channel_name;

    // Bind a NetworkBridgeSource<T> on an ephemeral port (port=0) and
    // return the bound port + a shared_ptr<void> to the typed source.
    // The generic role calls this once per input edge.
    std::function<std::pair<std::uint16_t, std::shared_ptr<void>>()> bind_inbound_bridge;

    // Construct a NetworkBridgeSink<T> targeting the given peer. Used
    // by the role's output stage after PeerUpdate resolves addresses.
    std::function<std::shared_ptr<void>(const std::string& host, std::uint16_t port)>
        connect_outbound_bridge;

    // Attach a typed side-output channel on the producer op identified
    // by `runner_index` and return its StageHandle<T> wrapped as
    // `std::any`. The in-process `LocalSubmitter` walker uses this to
    // realise `OperatorSpec.side_outputs` declarations. The closure
    // captures T at `register_typed<T>` time so the runtime dispatch
    // needs only the channel-type string.
    std::function<std::any(clink::Dag&, std::size_t /*runner_index*/, std::string /*tag*/)>
        make_side_output_handle;

    // Tee an upstream handle (wrapped as std::any) into N independent
    // branch handles. LocalSubmitter calls this when a producer has
    // more than one downstream consumer; without forking, the consumers
    // compete on the producer's single BoundedChannel (parallelism=1)
    // or per-subtask BoundedChannels (parallelism>1) and each record
    // lands on only one consumer instead of all of them.
    //
    // Dispatches on `parallelism`: at 1 the upstream is a
    // `StageHandle<T>` and the closure calls `Dag::fork<T>`; at >1 it
    // is a `ParallelStageHandle<T>` and the closure calls
    // `Dag::fork_parallel<T>`. The returned branches match the
    // upstream's shape - single handles at p=1, parallel handles at
    // p>1 - so downstream `add_*` calls unpack them normally. T is
    // captured at register_typed<T> time.
    std::function<std::vector<std::any>(clink::Dag&,
                                        const std::any& /*upstream*/,
                                        std::size_t /*n*/,
                                        std::uint32_t /*parallelism*/)>
        make_fork_handles;

    // Chain dispatch (task_manager.cpp length>=2 path): build the
    // chain's input stage from pre-bound NetworkBridgeSource<T>
    // handles. Wraps the resulting StageHandle<T> in std::any so the
    // chain dispatcher can thread it through DagBuilders without
    // knowing T. If N>1 inputs, union_streams<T> is used.
    std::function<std::any(clink::Dag&, const std::vector<std::shared_ptr<void>>&)>
        build_chain_input_stage;

    // Chain dispatch: attach all main-output groups for the chain
    // tail. Handles:
    //   * 1 group  -> attach_typed_group_output<T> on the upstream
    //   * Broadcast across N groups (default) -> fork<T> + attach each
    //   * Split across N groups (is_split=true) -> add_split<T> with
    //     selector (looked up by name in SelectorRegistry) + attach
    //     each branch
    // Internal per-group routing mirrors attach_typed_group_output's
    // logic (Hash via KeyExtractorRegistry, Rebalance counter,
    // single-peer direct, etc.).
    std::function<void(clink::Dag&,
                       std::any /*upstream*/,
                       const std::vector<ResolvedOutputGroup>& /*main_groups*/,
                       bool /*is_split*/,
                       const std::string& /*selector_fn name for SelectorRegistry*/)>
        attach_chain_main_outputs;

    // Source/sink fusion entry points: when the planner has fused a
    // source/sink into the chain, the TM-side dispatch builds the
    // typed source/sink via OperatorRegistry and hands the boxed
    // shared_ptr<void> off to these closures. They unbox to the
    // concrete typed Source<T>/Sink<T> and attach to the dag - no
    // inter-thread channels, no codec serde on the head/tail edges.
    //
    // add_fused_source_to_dag: wraps the result of Dag::add_source<T>
    // in std::any so the chain dispatcher can thread it into the
    // DagBuilder chain identically to the in_bridges path.
    std::function<std::any(clink::Dag&, std::shared_ptr<void> /*source*/)> add_fused_source_to_dag;

    // add_fused_sink_to_dag: terminates the dag by attaching the
    // typed sink to the chain tail's StageHandle<T> (wrapped in
    // std::any).
    std::function<void(clink::Dag&, std::any /*upstream*/, std::shared_ptr<void> /*sink*/)>
        add_fused_sink_to_dag;

    // fused_source_commit_hooks: recover the typed Source<T> from the boxed
    // shared_ptr<void> and return {commit, abort} callbacks bound to its
    // notify_checkpoint_complete / notify_checkpoint_aborted (weak-captured, so a
    // late notification after teardown is a safe no-op). The fused-chain
    // dispatch registers these into the TM's per-subtask committer/aborter
    // buckets, so a source fused into a par-1 chain still gets its broker ack
    // deferred to checkpoint commit - the same wiring the non-fused SubtaskRunner
    // does via register_commit_callbacks.
    std::function<std::pair<std::function<void(std::uint64_t)>, std::function<void(std::uint64_t)>>(
        std::shared_ptr<void> /*source*/)>
        fused_source_commit_hooks;
};

// TypeRegistry maps channel-name strings to TypeOps. It also maintains
// a reverse map (typeid(T).name() → channel-name) so plugin code that
// calls register_source<T>(...) can look up which channel name T is
// associated with. T must be registered via register_typed<T>(name,
// codec) BEFORE any source/operator/sink referencing T.
class TypeRegistry {
public:
    TypeRegistry() = default;
    explicit TypeRegistry(const TypeRegistry* parent) : parent_(parent) {}

    // Registers a channel under `name` for the C++ type T. Stamps out
    // typed bridge builders that capture T, its codec, and (optionally)
    // a specialised ArrowBatcher<T>. Multiple registrations under the
    // same name are accepted; latest wins.
    //
    // The codec-only overload auto-selects the ArrowBatcher: a type that
    // opted in via CLINK_ARROW_FIELDS gets its generated typed columnar
    // batcher, otherwise the binary-fallback batcher. Either way it rides
    // Arrow IPC on the wire; the description just decides typed vs binary
    // columns. Pass an explicit batcher to the 3-arg overload to override.
    template <typename T>
    void register_typed(const std::string& name, Codec<T> codec);

    template <typename T>
    void register_typed(const std::string& name, Codec<T> codec, ArrowBatcher<T> batcher);

    // Lookup. Returns nullptr if the channel isn't registered. Falls
    // through to `parent` on miss (for per-job registries layered over
    // built-ins).
    const TypeOps* find(const std::string& channel_name) const;

    // Reverse lookup: given typeid(T).name(), return the channel name
    // T was registered under. Returns empty string if T wasn't
    // registered. Plugin templates use this to bind sources/sinks/ops
    // to a channel without the user having to repeat the name. Falls
    // through to `parent` on miss.
    std::string channel_for_typeid(const std::string& typeid_name) const;

    static TypeRegistry& default_instance();

private:
    mutable std::mutex mu_;
    std::unordered_map<std::string, TypeOps> by_channel_;
    std::unordered_map<std::string, std::string> typeid_to_channel_;
    const TypeRegistry* parent_{nullptr};
};

// Inline implementation: stamps out typed closures that capture T and
// codec. This template is instantiated in every compilation unit that
// calls it (plugin .so or cluster binary); the resulting code references
// only header-defined templates (NetworkBridgeSource/Sink) plus the
// captured codec, so there's no symbol that needs to cross the dlopen
// boundary at run time.
template <typename T>
inline void TypeRegistry::register_typed(const std::string& name, Codec<T> codec) {
    register_typed<T>(name, codec, make_auto_arrow_batcher<T>(codec));
}

template <typename T>
inline void TypeRegistry::register_typed(const std::string& name,
                                         Codec<T> codec,
                                         ArrowBatcher<T> batcher) {
    TypeOps ops;
    ops.channel_name = name;
    ops.bind_inbound_bridge = [codec,
                               batcher]() -> std::pair<std::uint16_t, std::shared_ptr<void>> {
        auto src = std::make_shared<network::NetworkBridgeSource<T>>(/*port=*/0, codec, batcher);
        const auto port = src->prepare_listen();
        return {port, std::static_pointer_cast<void>(src)};
    };
    ops.connect_outbound_bridge = [codec, batcher](const std::string& host,
                                                   std::uint16_t port) -> std::shared_ptr<void> {
        auto sink = std::make_shared<network::NetworkBridgeSink<T>>(host, port, codec, batcher);
        return std::static_pointer_cast<void>(sink);
    };
    ops.make_side_output_handle =
        [](clink::Dag& dag, std::size_t runner_index, std::string tag) -> std::any {
        auto h = dag.template side_output_by_index<T>(runner_index, clink::OutputTag<T>(tag));
        return std::any{h};
    };
    ops.make_fork_handles = [](clink::Dag& dag,
                               const std::any& upstream,
                               std::size_t n,
                               std::uint32_t parallelism) -> std::vector<std::any> {
        std::vector<std::any> out;
        out.reserve(n);
        if (parallelism <= 1) {
            const auto& h = std::any_cast<const clink::StageHandle<T>&>(upstream);
            auto branches = dag.template fork<T>(h, n);
            for (auto& b : branches) {
                out.emplace_back(std::move(b));
            }
        } else {
            const auto& h = std::any_cast<const clink::Dag::ParallelStageHandle<T>&>(upstream);
            auto branches = dag.template fork_parallel<T>(h, n);
            for (auto& b : branches) {
                out.emplace_back(std::move(b));
            }
        }
        return out;
    };

    // Chain dispatch closures (used by task_manager.cpp's length>=2
    // chain path to build a typed Dag without knowing T at compile
    // time). Captures T, codec, batcher, channel-name.
    ops.build_chain_input_stage =
        [codec, batcher, name](clink::Dag& dag,
                               const std::vector<std::shared_ptr<void>>& bridges) -> std::any {
        std::vector<clink::StageHandle<T>> handles;
        handles.reserve(bridges.size());
        for (const auto& b : bridges) {
            auto src = std::static_pointer_cast<clink::network::NetworkBridgeSource<T>>(b);
            handles.push_back(dag.template add_source<T>(src));
        }
        (void)codec;
        (void)batcher;
        (void)name;
        if (handles.size() == 1) {
            return std::any{std::move(handles.front())};
        }
        return std::any{dag.template union_streams<T>(std::move(handles))};
    };

    // Inline implementation of "attach one group's output" - mirrors
    // attach_typed_group_output<T> in runner_helpers.hpp. Self-
    // contained so we don't need to pull runner_helpers into this
    // header (which would form a circular include).
    auto attach_one_group = [codec, batcher, name](clink::Dag& dag,
                                                   clink::StageHandle<T> handle,
                                                   const ResolvedOutputGroup& group) {
        auto make_sink = [&](const PeerAddress& peer) {
            return std::make_shared<clink::network::NetworkBridgeSink<T>>(
                peer.host, peer.data_port, codec, batcher);
        };
        if (group.peers.size() == 1) {
            dag.template add_sink<T>(handle, make_sink(group.peers.front()));
            return;
        }
        if (group.mode == RoutingMode::Hash) {
            if (group.key_extractor_fn.empty()) {
                throw std::runtime_error("chain dispatch: Hash routing with no key_extractor_fn");
            }
            auto extractor = KeyExtractorRegistry::default_instance().template find<T>(
                name, group.key_extractor_fn);
            if (!extractor) {
                throw std::runtime_error("chain dispatch: key extractor '" +
                                         group.key_extractor_fn + "' not registered for channel '" +
                                         name + "'");
            }
            const std::size_t n = group.peers.size();
            auto selector = [extractor, n](const T& v) {
                const auto k = extractor(v);
                const auto k_bytes =
                    std::span<const std::byte>{reinterpret_cast<const std::byte*>(&k), sizeof(k)};
                const auto group_id = key_group_for_key(k_bytes);
                return static_cast<int>(
                    subtask_for_key_group(group_id, static_cast<std::uint32_t>(n)));
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
        // Forward with multiple peers - fall back to broadcast for
        // safety (planner shouldn't emit this combination).
        auto branches = dag.template fork<T>(handle, group.peers.size());
        for (std::size_t i = 0; i < group.peers.size(); ++i) {
            dag.template add_sink<T>(branches[i], make_sink(group.peers[i]));
        }
    };

    ops.attach_chain_main_outputs = [attach_one_group](
                                        clink::Dag& dag,
                                        std::any upstream,
                                        const std::vector<ResolvedOutputGroup>& main_groups,
                                        bool is_split,
                                        const std::string& selector_fn) {
        if (main_groups.empty()) {
            return;
        }
        auto handle = std::any_cast<clink::StageHandle<T>>(std::move(upstream));
        if (main_groups.size() == 1) {
            attach_one_group(dag, handle, main_groups.front());
            return;
        }
        if (is_split) {
            std::function<int(const T&)> selector;
            if constexpr (std::is_same_v<T, std::int64_t>) {
                const auto* fn = SelectorRegistry::default_instance().find_int64(selector_fn);
                if (fn == nullptr) {
                    throw std::runtime_error("chain dispatch: int64 selector not registered: " +
                                             selector_fn);
                }
                selector = *fn;
            } else if constexpr (std::is_same_v<T, std::string>) {
                const auto* fn = SelectorRegistry::default_instance().find_string(selector_fn);
                if (fn == nullptr) {
                    throw std::runtime_error("chain dispatch: string selector not registered: " +
                                             selector_fn);
                }
                selector = *fn;
            } else {
                throw std::runtime_error(
                    "chain dispatch: Split routing not supported for plugin types (need a "
                    "typed selector registry)");
            }
            auto branches =
                dag.template add_split<T>(handle, std::move(selector), main_groups.size(), "split");
            for (std::size_t i = 0; i < main_groups.size(); ++i) {
                attach_one_group(dag, branches[i], main_groups[i]);
            }
            return;
        }
        // Broadcast (default for multi-consumer fork).
        auto branches = dag.template fork<T>(handle, main_groups.size());
        for (std::size_t i = 0; i < main_groups.size(); ++i) {
            attach_one_group(dag, branches[i], main_groups[i]);
        }
    };

    // Source/sink fusion closures - unbox shared_ptr<void> → typed
    // pointer and attach to the dag. The chain dispatcher in
    // task_manager.cpp calls these when chain.fused_source /
    // chain.fused_sink is set.
    ops.add_fused_source_to_dag = [](clink::Dag& dag, std::shared_ptr<void> source) -> std::any {
        auto typed = std::static_pointer_cast<clink::Source<T>>(std::move(source));
        return std::any{dag.template add_source<T>(std::move(typed))};
    };
    ops.add_fused_sink_to_dag = [](clink::Dag& dag, std::any upstream, std::shared_ptr<void> sink) {
        auto typed_sink = std::static_pointer_cast<clink::Sink<T>>(std::move(sink));
        auto handle = std::any_cast<clink::StageHandle<T>>(std::move(upstream));
        dag.template add_sink<T>(handle, std::move(typed_sink));
    };
    ops.fused_source_commit_hooks = [](std::shared_ptr<void> source)
        -> std::pair<std::function<void(std::uint64_t)>, std::function<void(std::uint64_t)>> {
        std::weak_ptr<clink::Source<T>> weak =
            std::static_pointer_cast<clink::Source<T>>(std::move(source));
        auto commit = [weak](std::uint64_t ckpt) {
            if (auto s = weak.lock()) {
                s->notify_checkpoint_complete(clink::CheckpointId{ckpt});
            }
        };
        auto abort = [weak](std::uint64_t ckpt) {
            if (auto s = weak.lock()) {
                s->notify_checkpoint_aborted(clink::CheckpointId{ckpt});
            }
        };
        return {std::move(commit), std::move(abort)};
    };

    std::lock_guard lock(mu_);
    by_channel_[name] = std::move(ops);
    typeid_to_channel_[typeid(T).name()] = name;
}

}  // namespace clink::cluster
