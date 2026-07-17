#include "clink/cluster/worker.hpp"

#include <array>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/dag_builder_registry.hpp"
#include "clink/cluster/job_bundle.hpp"
#include "clink/cluster/job_planner.hpp"
#include "clink/cluster/messages.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/plugin_cache.hpp"
#include "clink/cluster/plugin_loader.hpp"
#include "clink/cluster/runner_registry.hpp"
#include "clink/cluster/service_discovery.hpp"
#include "clink/cluster/type_registry.hpp"
#include "clink/core/base64.hpp"
#include "clink/core/codec.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/metrics/process_metrics.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/operators/udf_language_registry.hpp"
#include "clink/plugin/plugin.hpp"       // BuildContext
#include "clink/plugin/plugin_impl.hpp"  // make_subtask_job_config
#include "clink/runtime/dag.hpp"
#include "clink/runtime/key_groups.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/runtime/log_buffer.hpp"
#include "clink/runtime/logging.hpp"
#include "clink/runtime/network/network_bridge.hpp"
#include "clink/runtime/network/network_socket.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/state_backend_factory.hpp"

namespace clink::cluster {

namespace {

// Register the job's shipped UDF declarations (DeployMsg.udfs_packed) into
// the process-wide registries via each declaration's LANGUAGE loader. The
// module payload travels base64 in the spec; the local path in
// `definitions` is only meaningful where the CREATE FUNCTION ran, so the
// decoded bytes are what the loader instantiates from. Throws on an
// unknown language, a bad payload, or a loader failure - the caller
// surfaces that as the subtask's error.
void register_shipped_udfs(const std::string& packed) {
    for (const auto& u : unpack_udf_specs(packed)) {
        UdfLanguageRegistry::FunctionDecl decl;
        decl.name = u.name;
        decl.arg_names = u.arg_names;
        decl.arg_types.reserve(u.arg_types.size());
        for (const auto& t : u.arg_types) {
            decl.arg_types.push_back(udf_type_from_wire_name(t));
        }
        if (!u.return_type.empty()) {
            decl.return_type = udf_type_from_wire_name(u.return_type);
        }
        decl.definitions = u.definitions;
        decl.is_aggregate = u.kind == "aggregate";
        if (!u.module_b64.empty()) {
            const auto bytes = base64_decode(u.module_b64);
            if (!bytes) {
                throw std::runtime_error("UDF deploy: invalid base64 module payload for '" +
                                         u.name + "'");
            }
            decl.module_bytes.assign(bytes->begin(), bytes->end());
        }
        UdfLanguageRegistry::global().load(u.language, decl);
    }
}

std::optional<std::vector<std::byte>> read_frame(network::Connection& conn) {
    std::array<std::byte, 4> hdr{};
    if (!conn.recv_all(hdr.data(), hdr.size())) {
        return std::nullopt;
    }
    std::uint32_t len = 0;
    for (std::size_t i = 0; i < hdr.size(); ++i) {
        len = (len << 8) | static_cast<unsigned char>(hdr[i]);
    }
    if (len == 0) {
        return std::vector<std::byte>{};
    }
    std::vector<std::byte> body(len);
    if (!conn.recv_all(body.data(), body.size())) {
        return std::nullopt;
    }
    return body;
}

// Type-erased handle for a bound NetworkBridgeSource. The cast back to
// the typed shared_ptr happens in the channel-type dispatch arms below.
struct InboundBridge {
    std::uint16_t bound_port{0};
    std::shared_ptr<void> bridge;
};

InboundBridge make_inbound_bridge(const ChannelType& ct, const TypeRegistry& type_registry) {
    // TypeRegistry holds typed bridge builders for every registered
    // channel. Built-ins (int64, string) live in the default-instance;
    // plugin types live in the per-job bundle. `type_registry` is the
    // bundle's TR (which parent-falls-through to the default), so a
    // single lookup handles both.
    const auto* ops = type_registry.find(ct);
    if (ops == nullptr || !ops->bind_inbound_bridge) {
        return {};
    }
    auto [port, bridge] = ops->bind_inbound_bridge();
    return InboundBridge{port, std::move(bridge)};
}

// Build the input side of a Dag for a typed channel:
//   1+ inbound bridges -> (union if >1) -> StageHandle<T>
template <typename T>
StageHandle<T> build_input_stage(Dag& dag, const std::vector<std::shared_ptr<void>>& bridges) {
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

// Add the output side of a chain Dag for one resolved group:
//   * Forward + 1 peer:  single NetworkBridgeSink
//   * Rebalance + N peers: split with round-robin selector, one sink
//     per branch (records distribute; watermarks/barriers broadcast)
//   * Forward + N peers:  treat as broadcast fork (defensive; planner
//     normally only produces N peers for Rebalance)
template <typename T>
void attach_group_output(Dag& dag,
                         StageHandle<T> handle,
                         const ResolvedOutputGroup& group,
                         const Codec<T>& codec,
                         const ArrowBatcher<T>& batcher) {
    // Use the 4-arg NetworkBridgeSink ctor so the sender's Arrow schema
    // matches whatever batcher is registered for this type on the
    // receiver side (e.g. columnar for built-in int64/string). Without
    // this, the codec-only ctor would default to the binary-fallback
    // batcher and produce a schema-mismatch with the receiver's
    // registered batcher - UB on parse.
    if (group.peers.size() == 1) {
        auto bridge = std::make_shared<network::NetworkBridgeSink<T>>(
            group.peers.front().host, group.peers.front().data_port, codec, batcher);
        dag.template add_sink<T>(handle, bridge);
        return;
    }
    if (group.mode == RoutingMode::Rebalance) {
        // Round-robin across the downstream subtasks. The selector
        // closure owns a counter that increments per data record.
        auto counter = std::make_shared<std::atomic<std::size_t>>(0);
        const std::size_t n = group.peers.size();
        auto selector = [counter, n](const T&) {
            return static_cast<int>(counter->fetch_add(1, std::memory_order_relaxed) % n);
        };
        auto branches = dag.template add_split<T>(handle, selector, n, "rebalance");
        for (std::size_t i = 0; i < n; ++i) {
            auto bridge = std::make_shared<network::NetworkBridgeSink<T>>(
                group.peers[i].host, group.peers[i].data_port, codec, batcher);
            dag.template add_sink<T>(branches[i], bridge);
        }
        return;
    }
    if (group.mode == RoutingMode::Hash) {
        // Mirror attach_typed_group_output's Hash handling in
        // runner_helpers.hpp: resolve the typed key extractor, route
        // each record to peer = subtask_for_key_group(hash(key), n).
        // Without this branch the chain dispatch path falls through to
        // the broadcast-fork below and silently duplicates records to
        // every peer.
        if (group.key_extractor_fn.empty()) {
            throw std::runtime_error(
                "generic subtask: Hash routing but no key_extractor_fn set on output group");
        }
        auto extractor = KeyExtractorRegistry::default_instance().find<T>(
            std::is_same_v<T, std::int64_t> ? std::string{clink::cluster::kChannelInt64}
                                            : std::string{clink::cluster::kChannelString},
            group.key_extractor_fn);
        if (!extractor) {
            throw std::runtime_error("generic subtask: key extractor '" + group.key_extractor_fn +
                                     "' not registered for chain output");
        }
        const std::size_t n = group.peers.size();
        auto selector = [extractor, n](const T& v) {
            const auto k = extractor(v);
            const auto k_bytes =
                std::span<const std::byte>{reinterpret_cast<const std::byte*>(&k), sizeof(k)};
            const auto group_id = key_group_for_key(k_bytes);
            return static_cast<int>(subtask_for_key_group(group_id, static_cast<std::uint32_t>(n)));
        };
        auto branches = dag.template add_split<T>(handle, std::move(selector), n, "hash");
        for (std::size_t i = 0; i < n; ++i) {
            auto bridge = std::make_shared<network::NetworkBridgeSink<T>>(
                group.peers[i].host, group.peers[i].data_port, codec, batcher);
            dag.template add_sink<T>(branches[i], bridge);
        }
        return;
    }
    // Forward with multiple peers - planner shouldn't produce this, but
    // fall back to broadcast for safety.
    auto branches = dag.template fork<T>(handle, group.peers.size());
    for (std::size_t i = 0; i < group.peers.size(); ++i) {
        auto bridge = std::make_shared<network::NetworkBridgeSink<T>>(
            group.peers[i].host, group.peers[i].data_port, codec, batcher);
        dag.template add_sink<T>(branches[i], bridge);
    }
}

// Attach the full output side: outer routing across consumer groups
// (broadcast fork by default; per-record split via a named selector
// when output_routing == Split), then per-group routing.
template <typename T>
void attach_output_groups(Dag& dag,
                          StageHandle<T> handle,
                          const std::vector<ResolvedOutputGroup>& groups,
                          const Codec<T>& codec,
                          const ArrowBatcher<T>& batcher,
                          OperatorChainSpec::OutputRouting routing,
                          const std::string& selector_fn) {
    if (groups.empty()) {
        return;
    }
    if (groups.size() == 1) {
        attach_group_output<T>(dag, handle, groups.front(), codec, batcher);
        return;
    }
    if (routing == OperatorChainSpec::OutputRouting::Split) {
        std::function<int(const T&)> selector;
        if constexpr (std::is_same_v<T, std::int64_t>) {
            const auto* fn = SelectorRegistry::default_instance().find_int64(selector_fn);
            if (fn == nullptr) {
                throw std::runtime_error("generic subtask: int64 selector not registered: " +
                                         selector_fn);
            }
            selector = *fn;
        } else if constexpr (std::is_same_v<T, std::string>) {
            const auto* fn = SelectorRegistry::default_instance().find_string(selector_fn);
            if (fn == nullptr) {
                throw std::runtime_error("generic subtask: string selector not registered: " +
                                         selector_fn);
            }
            selector = *fn;
        } else {
            throw std::runtime_error("generic subtask: unsupported channel type for Split output");
        }
        auto branches =
            dag.template add_split<T>(handle, std::move(selector), groups.size(), "split");
        for (std::size_t i = 0; i < groups.size(); ++i) {
            attach_group_output<T>(dag, branches[i], groups[i], codec, batcher);
        }
        return;
    }
    auto branches = dag.template fork<T>(handle, groups.size());
    for (std::size_t i = 0; i < groups.size(); ++i) {
        attach_group_output<T>(dag, branches[i], groups[i], codec, batcher);
    }
}

// Resolve the registered ArrowBatcher<T> for a built-in channel type
// keyed by T. The cluster path hardcodes its dispatch on the two
// built-in types (int64, string); the helper centralises the
// type→batcher mapping so any new built-in only needs one spot to
// extend. Plugin types go through TypeOps::connect_outbound_bridge
// (runner_helpers.hpp::attach_typed_group_output) which already
// captures the registered batcher - that path doesn't need this.
template <typename T>
ArrowBatcher<T> builtin_arrow_batcher();

template <>
inline ArrowBatcher<std::int64_t> builtin_arrow_batcher<std::int64_t>() {
    return int64_arrow_batcher();
}

template <>
inline ArrowBatcher<std::string> builtin_arrow_batcher<std::string>() {
    return string_arrow_batcher();
}

// Source kind: factory<Source<Out>> -> output groups
template <typename Out>
void run_source_dispatch(std::shared_ptr<Source<Out>> src,
                         const std::vector<ResolvedOutputGroup>& groups,
                         Codec<Out> codec,
                         OperatorChainSpec::OutputRouting routing,
                         const std::string& selector_fn) {
    Dag dag;
    auto h0 = dag.template add_source<Out>(src);
    attach_output_groups<Out>(
        dag, h0, groups, codec, builtin_arrow_batcher<Out>(), routing, selector_fn);
    LocalExecutor exec(std::move(dag));
    exec.run();
}

// Sink kind: bridges -> union (optional) -> factory<Sink<In>>
template <typename In>
void run_sink_dispatch(const std::vector<std::shared_ptr<void>>& in_bridges,
                       std::shared_ptr<Sink<In>> sink) {
    Dag dag;
    auto h0 = build_input_stage<In>(dag, in_bridges);
    dag.template add_sink<In>(h0, sink);
    LocalExecutor exec(std::move(dag));
    exec.run();
}

// Operator kind: bridges -> union -> Operator<In, Out> -> output groups
template <typename In, typename Out>
void run_operator_dispatch(const std::vector<std::shared_ptr<void>>& in_bridges,
                           std::shared_ptr<Operator<In, Out>> op,
                           const std::vector<ResolvedOutputGroup>& groups,
                           Codec<Out> out_codec,
                           OperatorChainSpec::OutputRouting routing,
                           const std::string& selector_fn) {
    Dag dag;
    auto h0 = build_input_stage<In>(dag, in_bridges);
    auto h1 = dag.template add_operator<In, Out>(h0, op);
    attach_output_groups<Out>(
        dag, h1, groups, out_codec, builtin_arrow_batcher<Out>(), routing, selector_fn);
    LocalExecutor exec(std::move(dag));
    exec.run();
}

// compose_chain_step was removed when chain dispatch was migrated to
// the DagBuilder path (see the length>=2 block in run_generic_subtask_).
// It hardcoded eight channel-type combinations and couldn't handle
// user-registered channel types; the new path builds the typed Dag
// directly via per-op DagBuilders and TypeOps closures.

// Built-in `int64_int64_match_join` SubtaskRunner. Captured here as a
// closure registered via RunnerRegistry::register_join so the planner
// can detect the join type generically (has_join_for_type) and the
// dispatch loop just invokes the runner - no special-case branch in
// the generic-subtask path. Plugin-defined joins register the same
// way (own type name + own dispatch closure).
SubtaskRunner make_int64_int64_match_join_runner() {
    return [](const RunnerContext& ctx) {
        if (ctx.in_bridges.size() != 2) {
            throw std::runtime_error("int64_int64_match_join: needs exactly 2 input bridges, got " +
                                     std::to_string(ctx.in_bridges.size()));
        }
        auto left =
            std::static_pointer_cast<network::NetworkBridgeSource<std::int64_t>>(ctx.in_bridges[0]);
        auto right =
            std::static_pointer_cast<network::NetworkBridgeSource<std::int64_t>>(ctx.in_bridges[1]);
        Dag dag;
        auto lh = dag.add_source<std::int64_t>(left);
        auto rh = dag.add_source<std::int64_t>(right);
        using namespace std::chrono_literals;
        auto joiner = [](const std::optional<std::int64_t>& l,
                         const std::optional<std::int64_t>& r) -> std::string {
            return std::to_string(l.value_or(0)) + ":" + std::to_string(r.value_or(0));
        };
        auto jh = dag.interval_join<std::int64_t, std::int64_t, std::int64_t, std::string>(
            lh,
            rh,
            [](const std::int64_t& v) { return v; },
            [](const std::int64_t& v) { return v; },
            /*lower=*/1h,
            /*upper=*/1h,
            joiner,
            Dag::JoinType::Inner,
            "int64_int64_match_join");
        attach_output_groups<std::string>(dag,
                                          jh,
                                          ctx.output_groups,
                                          string_codec(),
                                          builtin_arrow_batcher<std::string>(),
                                          ctx.chain.output_routing,
                                          ctx.chain.output_selector_fn);
        LocalExecutor exec(std::move(dag));
        exec.run();
    };
}

}  // namespace

void register_builtin_joins(RunnerRegistry& rr) {
    rr.register_join(/*type=*/"int64_int64_match_join",
                     /*in1=*/std::string{kChannelInt64},
                     /*in2=*/std::string{kChannelInt64},
                     /*out=*/std::string{kChannelString},
                     make_int64_int64_match_join_runner());
}

Worker::Worker(std::string worker_id, std::string data_host)
    : Worker(std::move(worker_id), std::move(data_host), Config{}) {}

Worker::Worker(std::string worker_id, std::string data_host, Config cfg)
    : cfg_(cfg), worker_id_(std::move(worker_id)), data_host_(std::move(data_host)) {
    // Auto-register the generic subtask role so any submission can run
    // on this worker without job-specific C++ glue.
    roles_[kGenericSubtaskRole] = [](const DeploymentTask& /*task*/) {
        // Sentinel - run_task_ takes the kGenericSubtaskRole branch
        // before reaching this function. Defensive: if somehow invoked,
        // report a clear error.
        throw std::runtime_error("generic subtask handler reached via legacy dispatch");
    };
    // Default to plain-TCP for the coordinator connection. TLS callers override
    // via set_connect_factory() before connect_to_coordinator.
    connect_factory_ = [](const std::string& host, std::uint16_t port) {
        return network::connect_plain(host, port);
    };
}

Worker::~Worker() {
    stop();
}

void Worker::register_role(std::string role, RoleHandler handler) {
    if (role == kGenericSubtaskRole) {
        // Refuse to overwrite the built-in generic role.
        return;
    }
    roles_[std::move(role)] = std::move(handler);
}

void Worker::connect_to_coordinator(const std::string& coordinator_host,
                                    std::uint16_t coordinator_port) {
    coordinator_host_ = coordinator_host;
    coordinator_port_ = coordinator_port;
    metrics::worker::slot_capacity_set(cfg_.slot_count);
    conn_ = connect_factory_(coordinator_host, coordinator_port);
    if (!conn_) {
        throw std::runtime_error("Worker::connect_to_coordinator: connect failed for " +
                                 coordinator_host + ":" + std::to_string(coordinator_port));
    }

    const auto reg_frame =
        encode_frame(MessageKind::Register,
                     RegisterMsg{worker_id_, data_host_, cfg_.slot_count, cfg_.http_port});
    if (!send_frame_(reg_frame)) {
        throw std::runtime_error("Worker::connect_to_coordinator: Register send failed");
    }

    auto frame = read_frame(*conn_);
    if (!frame.has_value()) {
        throw std::runtime_error("Worker::connect_to_coordinator: connection closed before ack");
    }
    MessageReader r(std::move(*frame));
    const auto kind = static_cast<MessageKind>(r.read_u8());
    if (kind != MessageKind::RegisterAck) {
        throw std::runtime_error("Worker::connect_to_coordinator: unexpected first message");
    }
    const auto ack = decode_register_ack(r);
    if (!ack.ok) {
        throw std::runtime_error("Worker::connect_to_coordinator: register rejected: " +
                                 ack.message);
    }

    reader_ = std::thread([this] { reader_loop_(); });
    if (cfg_.heartbeat_interval.count() > 0) {
        heartbeat_ = std::thread([this] { heartbeat_loop_(); });
    }
}

void Worker::connect_to_coordinator(ServiceDiscovery& sd,
                                    std::chrono::milliseconds discover_timeout) {
    auto ep = sd.discover_coordinator(discover_timeout);
    if (!ep.has_value()) {
        throw std::runtime_error("Worker::connect_to_coordinator: service discovery timed out");
    }
    connect_to_coordinator(ep->host, ep->port);
}

void Worker::heartbeat_loop_() {
    while (!stop_.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(cfg_.heartbeat_interval);
        if (stop_.load(std::memory_order_acquire)) {
            return;
        }
        const auto frame = encode_frame(MessageKind::Heartbeat, HeartbeatMsg{worker_id_});
        if (!send_frame_(frame)) {
            return;
        }
    }
}

void Worker::reader_loop_() {
    while (!stop_.load(std::memory_order_acquire)) {
        if (!conn_) {
            disconnected_.store(true, std::memory_order_release);
            return;
        }
        auto frame = read_frame(*conn_);
        if (!frame.has_value()) {
            // Connection closed; wake any pending awaiters so they can
            // exit instead of hanging forever. Flip disconnected_ so
            // an --ha-dir worker in clink_node can detect the coordinator
            // disconnect and exit (supervisor restarts it; the new
            // process re-reads active-leader.json).
            disconnected_.store(true, std::memory_order_release);
            std::lock_guard lock(mu_);
            for (auto& [_, pt] : pending_) {
                pt->cancelled = true;
                pt->cv.notify_all();
            }
            return;
        }
        MessageReader r(std::move(*frame));
        const auto kind = static_cast<MessageKind>(r.read_u8());
        switch (kind) {
            case MessageKind::Deploy:
                handle_deploy_(r);
                break;
            case MessageKind::PeerUpdate:
                handle_peer_update_(r);
                break;
            case MessageKind::CancelJob: {
                auto cj = decode_cancel_job(r);
                cancelled_.store(true, std::memory_order_release);
                std::lock_guard lock(mu_);
                // Wake any subtasks blocked in await_peer_update_ - for
                // jobs cancelled before peer-update arrived.
                for (auto& [key, pt] : pending_) {
                    if (std::get<0>(key) == cj.job_id) {
                        pt->cancelled = true;
                        pt->cv.notify_all();
                    }
                }
                // Flip the LocalExecutor cancel token for every running
                // subtask of this job. Each executor's stop_predicate
                // observes the flip and winds the runner down; run_task_
                // sends SubtaskFinished with had_error=true, error
                // message "cancelled" once the runner exits.
                auto it = per_job_cancel_tokens_.find(cj.job_id);
                if (it != per_job_cancel_tokens_.end()) {
                    for (auto& [subtask_idx, token] : it->second) {
                        if (token) {
                            token->store(true, std::memory_order_release);
                        }
                    }
                }
                cv_.notify_all();
                // Wake any source blocked in the EOS final-checkpoint waits so
                // it observes the flipped cancel token at once (its predicate
                // checks cancel_token) instead of stalling its 30s wait. The
                // predicate state (the token) is mutated outside final_ckpt_mu_,
                // so briefly take that mutex before notifying: this serialises
                // against a waiter mid-predicate and closes the lost-wakeup window
                // (a notify that would otherwise land before the waiter suspends).
                {
                    std::lock_guard<std::mutex> wake(final_ckpt_mu_);
                }
                final_ckpt_cv_.notify_all();
                break;
            }
            case MessageKind::StartJob:
                break;
            case MessageKind::TriggerCheckpoint:
                handle_trigger_checkpoint_(r);
                break;
            case MessageKind::CommitCheckpoint:
                handle_commit_checkpoint_(r);
                break;
            case MessageKind::FinalCheckpointAssigned:
                handle_final_checkpoint_assigned_(r);
                break;
            case MessageKind::AbortCheckpoint:
                handle_abort_checkpoint_(r);
                break;
            case MessageKind::BeginRescale:
                handle_begin_rescale_(r);
                break;
            default:
                break;
        }
    }
}

void Worker::handle_commit_checkpoint_(MessageReader& r) {
    auto msg = decode_commit_checkpoint(r);
    // Record the committed high-water mark + wake any source blocked in
    // wait_final_committed() for its final checkpoint id (see EOS handling).
    {
        std::lock_guard lk(final_ckpt_mu_);
        auto& hw = final_committed_high_water_[msg.job_id];
        hw = std::max(hw, msg.checkpoint_id);
    }
    final_ckpt_cv_.notify_all();
    // Snapshot per-job commit callbacks under the lock, then dispatch
    // without it. The sink's commit work (atomic rename, Kafka commitTx,
    // SQL COMMIT PREPARED) may block on the external system; doing it
    // outside mu_ keeps reader-loop dispatch responsive.
    std::vector<RunnerContext::CommitCheckpointFn> to_invoke;
    {
        std::lock_guard lock(mu_);
        auto job_it = per_job_committers_.find(msg.job_id);
        if (job_it != per_job_committers_.end()) {
            for (auto& [sub_idx, entry] : job_it->second) {
                for (auto& fn : entry) {
                    to_invoke.push_back(fn);
                }
            }
        }
    }
    for (auto& fn : to_invoke) {
        try {
            fn(msg.checkpoint_id);
        } catch (...) {
            // Best-effort: a single sink failing to commit must not
            // stall the reader. The state-backend handle remains; the
            // next restart's recovery path will retry.
        }
    }

    // Checkpoint retention: a CommitCheckpoint means this checkpoint is
    // globally complete. Record it and purge any now-subsumed older
    // checkpoint from every hosted subtask backend so storage stays
    // bounded. Collect the (backend, id) work under the lock, run the
    // purges (filesystem remove_all / unlink) outside it.
    std::vector<std::pair<std::shared_ptr<StateBackend>, CheckpointId>> to_purge;
    {
        std::lock_guard lock(mu_);
        auto ret_it = per_job_retention_.find(msg.job_id);
        if (ret_it == per_job_retention_.end()) {
            ret_it = per_job_retention_
                         .emplace(msg.job_id, CheckpointRetention{cfg_.checkpoint_num_retained})
                         .first;
        }
        const auto purge_ids = ret_it->second.record_completed(CheckpointId{msg.checkpoint_id});
        if (!purge_ids.empty()) {
            if (auto be_it = per_job_backends_.find(msg.job_id); be_it != per_job_backends_.end()) {
                for (const auto& [_, backend] : be_it->second) {
                    if (!backend) {
                        continue;
                    }
                    for (const auto id : purge_ids) {
                        to_purge.emplace_back(backend, id);
                    }
                }
            }
        }
    }
    for (auto& [backend, id] : to_purge) {
        try {
            backend->purge_checkpoint(id);
        } catch (...) {
            // Best-effort: a failed purge only leaves disk un-reclaimed.
        }
    }
}

void Worker::handle_final_checkpoint_assigned_(MessageReader& r) {
    auto msg = decode_final_checkpoint_assigned(r);
    const std::string key =
        std::to_string(msg.job_id) + ":" + msg.role + ":" + std::to_string(msg.subtask_idx);
    {
        std::lock_guard lk(final_ckpt_mu_);
        // Only fulfil a request that is still waiting; a reply that arrives after
        // the source already timed out + erased its entry is dropped (no leak).
        auto it = final_assigned_.find(key);
        if (it != final_assigned_.end()) {
            it->second = msg.final_checkpoint_id;  // 0 = coordinator declined
        }
    }
    final_ckpt_cv_.notify_all();
}

void Worker::handle_begin_rescale_(MessageReader& r) {
    auto msg = decode_begin_rescale(r);
    // Snapshot the drain callbacks for the addressed (job, op) under
    // the lock, invoke outside it. Same lock-discipline as
    // handle_commit_checkpoint_: drain choreography may block on
    // output-channel push (when downstream is slow), so we must not
    // hold mu_ across the dispatch.
    std::vector<DrainCallback> to_invoke;
    {
        std::lock_guard lock(mu_);
        auto job_it = per_job_drain_callbacks_.find(msg.job_id);
        if (job_it != per_job_drain_callbacks_.end()) {
            auto op_it = job_it->second.find(msg.op_id);
            if (op_it != job_it->second.end()) {
                to_invoke = op_it->second;
            }
        }
    }
    for (auto& fn : to_invoke) {
        try {
            fn(msg.target_parallelism);
        } catch (...) {
            // Best-effort: a single subtask failing to drain isn't
            // fatal to the dispatcher. The coordinator will time out the
            // rescale if the SubtaskFinished ack never arrives.
        }
    }
}

void Worker::handle_abort_checkpoint_(MessageReader& r) {
    auto msg = decode_abort_checkpoint(r);
    // Same dispatch shape as handle_commit_checkpoint_: snapshot the
    // aborter callbacks under the lock, invoke outside it. on_abort
    // must be best-effort (any single failure shouldn't stall the
    // reader); the worst case is a staging file lingering until the
    // next restart's recovery sweep.
    std::vector<AbortCallback> to_invoke;
    {
        std::lock_guard lock(mu_);
        auto job_it = per_job_aborters_.find(msg.job_id);
        if (job_it != per_job_aborters_.end()) {
            for (auto& [sub_idx, entry] : job_it->second) {
                for (auto& fn : entry) {
                    to_invoke.push_back(fn);
                }
            }
        }
    }
    for (auto& fn : to_invoke) {
        try {
            fn(msg.checkpoint_id);
        } catch (...) {
            // Best-effort: a single sink failing to abort isn't fatal -
            // worst case the staging file lingers until recovery.
        }
    }
}

void Worker::handle_trigger_checkpoint_(MessageReader& r) {
    auto msg = decode_trigger_checkpoint(r);
    // Snapshot per-job injectors under the lock; release before
    // invoking so injection (which pushes into BoundedChannels and may
    // block) doesn't contend with deploy / peer-update bookkeeping.
    // If no sources have registered yet for the job, queue the trigger
    // so the first registration can replay it. This eliminates the race
    // where the coordinator fires a periodic trigger while the chain is still
    // coming up locally.
    std::vector<RunnerContext::SourceInjectorFn> to_invoke;
    {
        std::lock_guard lock(mu_);
        auto job_it = per_job_injectors_.find(msg.job_id);
        if (job_it == per_job_injectors_.end() || job_it->second.empty()) {
            pending_triggers_[msg.job_id].push_back(msg.checkpoint_id);
            return;
        }
        for (auto& [sub_idx, entry] : job_it->second) {
            for (auto& fn : entry.injectors) {
                to_invoke.push_back(fn);
            }
        }
    }
    CheckpointBarrier barrier(CheckpointId{msg.checkpoint_id});
    for (auto& fn : to_invoke) {
        try {
            fn(barrier);
        } catch (...) {
            // Best-effort: if a barrier injection fails the coordinator's ack
            // timeout will surface the missed subtask.
        }
    }
}

void Worker::handle_deploy_(MessageReader& r) {
    auto msg = decode_deploy(r);

    // Allocate (or reuse) the per-job bundle. Plugin .so bytes that
    // come with this Deploy will register their op factories INTO this
    // bundle's PluginRegistry view, NOT the worker's default singletons.
    // Subtask dispatch below looks runners up via the bundle (with
    // fallback to default singletons for built-ins).
    JobBundle* job_bundle = nullptr;
    {
        std::lock_guard lock(mu_);
        auto it = per_job_bundle_.find(msg.job_id);
        if (it == per_job_bundle_.end()) {
            it = per_job_bundle_.emplace(msg.job_id, std::make_unique<JobBundle>()).first;
        }
        job_bundle = it->second.get();
    }
    auto bundle_preg = job_bundle->as_plugin_registry();

    // Load plugins bundled with this job before spawning task threads.
    // Any failure here is reported back via a synthetic
    // SubtaskFinished for every task in this deploy, so the coordinator doesn't
    // wedge waiting for a subtask that never started.
    std::string plugin_err;
    for (const auto& plug : msg.plugins) {
        try {
            const auto path = clink::cluster::write_plugin_to_cache(plug);
            auto res =
                clink::cluster::PluginLoader::default_instance().load_into(path, bundle_preg);
            if (!res.ok) {
                plugin_err = "plugin '" + plug.name + "': " + res.error;
                break;
            }
        } catch (const std::exception& e) {
            plugin_err = "plugin '" + plug.name + "': " + e.what();
            break;
        }
    }
    if (!plugin_err.empty()) {
        for (const auto& task : msg.tasks) {
            SubtaskFinishedMsg done;
            done.job_id = msg.job_id;
            done.worker_id = worker_id_;
            done.role = task.role;
            done.subtask_idx = task.subtask_idx;
            done.had_error = true;
            done.error_message = plugin_err;
            send_frame_(encode_frame(MessageKind::SubtaskFinished, done));
        }
        return;
    }

    {
        std::lock_guard lock(mu_);
        in_flight_tasks_ += msg.tasks.size();
        deployed_ = true;
        for (const auto& task : msg.tasks) {
            const PendingKey key{msg.job_id, task.role, task.subtask_idx};
            pending_[key] = std::make_shared<PendingTask>();
        }
        // Stash this job's checkpoint config so the trigger handler
        // can find the right barrier injectors for the right job.
        per_job_checkpoint_[msg.job_id] = JobCheckpointState{
            .checkpoint_dir = msg.checkpoint_dir,
            .restore_from_dir = msg.restore_from_dir,
            .restore_from_checkpoint_id = msg.restore_from_checkpoint_id,
            .state_backend_uri = msg.state_backend_uri,
            .capture_dir = msg.capture_dir,
            .capture_records = msg.capture_records,
        };
    }
    for (auto& task : msg.tasks) {
        const JobId jid = msg.job_id;
        const std::string ckpt_dir = msg.checkpoint_dir;
        const std::string restore_dir = msg.restore_from_dir;
        const std::uint64_t restore_id = msg.restore_from_checkpoint_id;
        const bool unaligned = msg.unaligned_checkpoints;
        const std::string expected_versions = msg.expected_state_versions_packed;
        const std::string udfs = msg.udfs_packed;
        task_threads_.emplace_back([this,
                                    jid,
                                    task = std::move(task),
                                    ckpt_dir,
                                    restore_dir,
                                    restore_id,
                                    unaligned,
                                    expected_versions,
                                    udfs] {
            run_task_(
                jid, task, ckpt_dir, restore_dir, restore_id, unaligned, expected_versions, udfs);
        });
    }
}

void Worker::handle_peer_update_(MessageReader& r) {
    auto msg = decode_peer_update(r);
    std::lock_guard lock(mu_);
    if (msg.tasks.empty()) {
        // Empty PeerUpdate = "go" signal for tasks with no peers. Mark
        // every pending task for this job as ready.
        for (auto& [key, pt] : pending_) {
            if (std::get<0>(key) == msg.job_id && !pt->ready && !pt->cancelled) {
                pt->ready = true;
                pt->cv.notify_all();
            }
        }
        return;
    }
    for (const auto& tp : msg.tasks) {
        const PendingKey key{msg.job_id, tp.role, tp.subtask_idx};
        auto it = pending_.find(key);
        if (it == pending_.end()) {
            continue;
        }
        it->second->resolved_peers = tp.peers;
        it->second->ready = true;
        it->second->cv.notify_all();
    }
    // A PeerUpdate that listed only some tasks still implies "go" for
    // the rest of this job's tasks (those with no peers). Set ready on
    // the unmentioned ones as well.
    for (auto& [key, pt] : pending_) {
        if (std::get<0>(key) == msg.job_id && !pt->ready && !pt->cancelled) {
            pt->ready = true;
            pt->cv.notify_all();
        }
    }
}

std::optional<Worker::ResolvedPeers> Worker::await_peer_update_(JobId job_id,
                                                                const std::string& role,
                                                                std::uint32_t subtask_idx) {
    std::shared_ptr<PendingTask> pt;
    {
        std::lock_guard lock(mu_);
        auto it = pending_.find(PendingKey{job_id, role, subtask_idx});
        if (it == pending_.end()) {
            return std::nullopt;
        }
        pt = it->second;
    }
    std::unique_lock plock(mu_);
    const auto deadline = std::chrono::steady_clock::now() + cfg_.peer_update_timeout;
    while (!pt->ready && !pt->cancelled && !stop_.load(std::memory_order_acquire)) {
        if (pt->cv.wait_until(plock, deadline) == std::cv_status::timeout) {
            return std::nullopt;
        }
    }
    if (pt->cancelled || stop_.load(std::memory_order_acquire)) {
        return std::nullopt;
    }
    return ResolvedPeers{pt->resolved_peers};
}

void Worker::run_task_(JobId job_id,
                       const DeploymentTask& task,
                       const std::string& checkpoint_dir,
                       const std::string& restore_from_dir,
                       std::uint64_t restore_from_checkpoint_id,
                       bool unaligned_checkpoints,
                       const std::string& expected_state_versions_packed,
                       const std::string& udfs_packed) {
    metrics::worker::subtask_started();
    bool had_error = false;
    std::string err_msg;
    try {
        // SQL-declared UDFs shipped with the job: register each before any
        // operator runs so expression evaluation resolves them. Process-wide
        // and idempotent (same-name re-registration replaces), so subtasks
        // racing here is harmless; a failure (unknown language, bad module)
        // fails this subtask with the loader's message.
        if (!udfs_packed.empty()) {
            register_shipped_udfs(udfs_packed);
        }
        if (task.role == kGenericSubtaskRole) {
            run_generic_subtask_(job_id,
                                 task,
                                 checkpoint_dir,
                                 restore_from_dir,
                                 restore_from_checkpoint_id,
                                 unaligned_checkpoints,
                                 expected_state_versions_packed);
        } else {
            auto it = roles_.find(task.role);
            if (it == roles_.end()) {
                had_error = true;
                err_msg = "no handler registered for role '" + task.role + "'";
            } else {
                it->second(task);
            }
        }
    } catch (const std::exception& e) {
        had_error = true;
        err_msg = e.what();
    } catch (...) {
        had_error = true;
        err_msg = "unknown exception";
    }
    if (had_error) {
        metrics::worker::subtask_failed();
        // Test-only (env-gated, default off): delay a FAILED subtask's
        // SubtaskFinished so a peer that finishes cleanly in the same window
        // reports first. Lets the failover bench deterministically exercise the
        // coordinator's finished-peer redeploy path (a subtask errors AFTER its peer
        // already finished -> restart_drain_expected empty -> restart_pending
        // must re-add the finished peer) instead of racing it. No-op in
        // production.
        if (const char* d = std::getenv("CLINK_TEST_DELAY_ERROR_FINISH_MS")) {
            const long ms = std::strtol(d, nullptr, 10);
            if (ms > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(ms));
            }
        }
    } else {
        metrics::worker::subtask_completed_ok();
    }

    SubtaskFinishedMsg done;
    done.job_id = job_id;
    done.worker_id = worker_id_;
    done.role = task.role;
    done.subtask_idx = task.subtask_idx;
    done.had_error = had_error;
    done.error_message = err_msg;
    send_frame_(encode_frame(MessageKind::SubtaskFinished, done));

    {
        std::lock_guard lock(mu_);
        --in_flight_tasks_;
        pending_.erase(PendingKey{job_id, task.role, task.subtask_idx});
    }
    cv_.notify_all();
}

void Worker::run_generic_subtask_(JobId job_id,
                                  const DeploymentTask& task,
                                  const std::string& checkpoint_dir,
                                  const std::string& restore_from_dir,
                                  std::uint64_t restore_from_checkpoint_id,
                                  bool unaligned_ckpt,
                                  const std::string& expected_state_versions_packed) {
    // Built-in channels and op-runners must be present before we look
    // up TypeRegistry entries for bridge construction. Idempotent.
    ensure_built_ins_registered();

    // Per-job bundle for plugin/inline-lambda registrations. Created
    // by handle_deploy_; if missing (legacy non-plugin in-process tests),
    // fall back to the process-wide default singletons.
    const TypeRegistry* job_tr = &TypeRegistry::default_instance();
    const RunnerRegistry* job_rr = nullptr;
    const OperatorRegistry* job_or = nullptr;
    const DagBuilderRegistry* job_dbr = nullptr;
    // Per-subtask state-backend URI (decoupled from checkpoint_dir). Empty
    // keeps the legacy behaviour where checkpoint_dir is the backend URI.
    std::string state_backend_uri;
    // Record-capture flight recorder (empty = off).
    std::string capture_dir;
    std::uint64_t capture_records = 0;
    {
        std::lock_guard lock(mu_);
        auto it = per_job_bundle_.find(job_id);
        if (it != per_job_bundle_.end()) {
            job_tr = &it->second->type_registry();
            job_rr = &it->second->runner_registry();
            job_or = &it->second->operator_registry();
            job_dbr = &it->second->dag_builder_registry();
        }
        if (auto ck = per_job_checkpoint_.find(job_id); ck != per_job_checkpoint_.end()) {
            state_backend_uri = ck->second.state_backend_uri;
            capture_dir = ck->second.capture_dir;
            capture_records = ck->second.capture_records;
        }
    }
    const auto& bundle_tr = *job_tr;

    // 1. Parse the OperatorChainSpec embedded by the JobPlanner.
    auto chain = OperatorChainSpec::from_json(task.extra_config);
    if (chain.ops.empty()) {
        throw std::runtime_error("generic subtask: chain has no ops");
    }
    // For dispatch we use the chain's endpoints: the head op sets the
    // chain's input edges (and acts as the Source if kind=Source); the
    // tail op sets the output edges (and acts as the Sink if kind=Sink).
    // For chains of length >= 2 we fold the ops into a single
    // Operator<chain_in, chain_out> via repeated ChainedOperator.
    const auto& op = chain.ops[0];

    // 2. Bind one NetworkBridgeSource per input edge. With multi-input
    // (union/join), each upstream connects to a distinct port on this
    // subtask. Single-input is just the N=1 case.
    std::vector<std::shared_ptr<void>> in_bridges;
    in_bridges.reserve(chain.input_edges.size());
    SubtaskListeningMsg sl;
    sl.job_id = job_id;
    sl.worker_id = worker_id_;
    sl.role = task.role;
    sl.subtask_idx = task.subtask_idx;
    sl.host = data_host_;
    for (const auto& edge : chain.input_edges) {
        auto inb = make_inbound_bridge(edge.channel_type, bundle_tr);
        if (!inb.bridge) {
            throw std::runtime_error("generic subtask: unsupported in_channel for edge");
        }
        in_bridges.push_back(inb.bridge);
        sl.edge_ports.push_back(SubtaskListeningMsg::EdgePort{
            .upstream_role = edge.peer_role,
            .upstream_subtask_idx = edge.peer_subtask_idx,
            .port = inb.bound_port,
        });
    }

    // 3. Report SubtaskListening - one entry per input edge. Sources
    // send an empty edge_ports list; the coordinator uses this only as a "ready"
    // tick.
    if (!send_frame_(encode_frame(MessageKind::SubtaskListening, sl))) {
        throw std::runtime_error("generic subtask: SubtaskListening send failed");
    }

    // 4. Wait for PeerUpdate.
    auto resolved = await_peer_update_(job_id, task.role, task.subtask_idx);
    if (!resolved.has_value()) {
        throw std::runtime_error("generic subtask: cancelled or peer-update timed out");
    }

    // 4b. Build ResolvedOutputGroups by matching each chain output_group's
    // edges to resolved peers via (peer_role, peer_subtask_idx). The
    // peer ordering inside each group preserves the planner's intent so
    // round-robin selectors see a stable mapping.
    auto find_peer = [&](const std::string& role, std::uint32_t sub) -> const PeerAddress* {
        for (const auto& p : resolved->peers) {
            if (p.role == role && p.subtask_idx == sub) {
                return &p;
            }
        }
        return nullptr;
    };
    std::vector<ResolvedOutputGroup> resolved_groups;
    resolved_groups.reserve(chain.output_groups.size());
    for (const auto& g : chain.output_groups) {
        ResolvedOutputGroup rg;
        rg.mode = g.mode;
        rg.key_extractor_fn = g.key_extractor_fn;
        rg.side_output_tag = g.side_output_tag;
        if (!g.edges.empty()) {
            rg.channel_type = g.edges.front().channel_type;
        }
        rg.peers.reserve(g.edges.size());
        for (const auto& e : g.edges) {
            const auto* p = find_peer(e.peer_role, e.peer_subtask_idx);
            if (p == nullptr) {
                throw std::runtime_error("generic subtask: missing resolved peer for edge");
            }
            rg.peers.push_back(*p);
        }
        resolved_groups.push_back(std::move(rg));
    }

    // 5a. New path: for length-1 chains, look up a SubtaskRunner in
    // RunnerRegistry and call it. Built-ins are registered in the
    // process-wide default-instance via ensure_built_ins_registered()
    // (called below). Plugin/inline-lambda registrations live in this
    // job's bundle, which parent-falls-through to the default for
    // built-ins. Fall through to the legacy OperatorRegistry-based
    // dispatch on a miss so the existing build path keeps working
    // during migration.
    ensure_built_ins_registered();
    // Skip the legacy single-op runner path when fusion is in play:
    // even a chain of one operator with fused source/sink needs the
    // DagBuilder chain dispatch below so the typed source/sink land
    // on the same Dag as the operator.
    const bool has_fused_endpoints = chain.fused_source.has_value() || chain.fused_sink.has_value();
    if (chain.ops.size() == 1 && !has_fused_endpoints) {
        // Use the per-job bundle's RunnerRegistry (looked up at the top
        // of this function), with parent fallback to the default
        // singletons for built-ins. job_rr is nullptr only for legacy
        // in-process callers without a bundle.
        const auto& rreg = job_rr != nullptr ? *job_rr : RunnerRegistry::default_instance();
        const SubtaskRunner* runner = nullptr;
        switch (op.kind) {
            case OperatorKind::Source:
                runner = rreg.find_source(op.type, op.out_channel);
                break;
            case OperatorKind::Operator:
                runner = rreg.find_operator(op.type, op.in_channel, op.out_channel);
                break;
            case OperatorKind::Sink:
                runner = rreg.find_sink(op.type, op.in_channel);
                break;
            case OperatorKind::Join:
                // Join goes through RunnerRegistry::find_join.
                // Built-ins (int64_int64_match_join) self-register via
                // ensure_built_ins_registered → register_builtin_joins.
                // Plugin-defined joins register the same way at .so
                // load time, so the planner's has_join_for_type sees
                // them and the dispatch routes through this code path.
                if (chain.input_edges.size() < 2) {
                    throw std::runtime_error("generic subtask: join needs >= 2 input edges, got " +
                                             std::to_string(chain.input_edges.size()));
                }
                {
                    // Order matches first-occurrence in input_edges.
                    // Like the co_operator path: the closure captured
                    // at registration time partitions in_bridges by
                    // channel_type, so the registry-side ordering of
                    // (in1, in2) matches what the user passed.
                    const auto in1_ct = chain.input_edges[0].channel_type;
                    const auto in2_ct = chain.input_edges[1].channel_type;
                    runner = rreg.find_join(op.type, in1_ct, in2_ct, op.out_channel);
                }
                break;
            case OperatorKind::CoOperator:
                // CoOperators always go through RunnerRegistry. The
                // runner closure (built by PluginRegistry::register_co_operator)
                // partitions in_bridges across In1/In2 by channel_type.
                if (chain.input_edges.size() < 2) {
                    throw std::runtime_error(
                        "generic subtask: co_operator needs >= 2 input edges, got " +
                        std::to_string(chain.input_edges.size()));
                }
                {
                    // Find the two input channel types. Order matches
                    // first-occurrence in input_edges, which is also
                    // the order the user passed register_co_operator
                    // <In1, In2, Out> (the planner emits them in graph
                    // input order). When In1 and In2 share a channel
                    // type (Row/Row/Row co-ops like SQL interval-join
                    // and union-all), the runner closure in
                    // PluginRegistry::register_co_operator falls back
                    // to ordinal partitioning of in_bridges, so we
                    // just pass the same channel type for both sides
                    // here and trust the planner to order inputs.
                    std::string in1_ct;
                    std::string in2_ct;
                    for (const auto& e : chain.input_edges) {
                        if (in1_ct.empty()) {
                            in1_ct = e.channel_type;
                        } else if (e.channel_type != in1_ct && in2_ct.empty()) {
                            in2_ct = e.channel_type;
                        }
                    }
                    if (in1_ct.empty()) {
                        throw std::runtime_error("generic subtask: co_operator has no input edges");
                    }
                    if (in2_ct.empty()) {
                        in2_ct = in1_ct;
                    }
                    runner = rreg.find_co_operator(op.type, in1_ct, in2_ct, op.out_channel);
                }
                break;
        }
        if (runner != nullptr) {
            // Build a checkpoint-ack callback that sends
            // SubtaskCheckpointed back to the coordinator whenever the runner
            // (via the operator runner's snapshot-on-barrier path)
            // completes a checkpoint id.
            auto ack_cb = [this, job_id, role = task.role, sub = task.subtask_idx](
                              std::uint64_t ckpt_id, bool ok, std::string error) {
                SubtaskCheckpointedMsg m;
                m.job_id = job_id;
                m.checkpoint_id = ckpt_id;
                m.role = role;
                m.subtask_idx = sub;
                m.ok = ok;
                m.error = std::move(error);
                send_frame_(encode_frame(MessageKind::SubtaskCheckpointed, m));
            };
            // Cancel token for this subtask: ORed into the runner's stop
            // predicate via JobConfig.external_cancel_token (so should_stop()
            // observes it) AND captured by the EOS final-checkpoint waits below
            // so a CancelJob/worker-stop wakes a source blocked at EOS promptly
            // instead of stalling its bounded 30s wait. Registered under mu_ (and
            // threaded onto rctx) just before the runner starts, lower down.
            auto cancel_token = std::make_shared<std::atomic<bool>>(false);
            // Bounded-source EOS hooks. request_final_checkpoint asks the coordinator for
            // a final coordinated checkpoint id and blocks (bounded) for the
            // reply; wait_final_committed blocks (bounded) until this worker observes
            // CommitCheckpoint for that id. Only a real bounded source at clean
            // EOS invokes these (relays/unbounded sources never do). Both waits
            // also wake on cancel_token / stop_ for prompt teardown.
            auto request_final_ckpt = [this,
                                       job_id,
                                       role = task.role,
                                       sub = task.subtask_idx,
                                       cancel_token]() -> std::uint64_t {
                const std::string key =
                    std::to_string(job_id) + ":" + role + ":" + std::to_string(sub);
                {
                    std::lock_guard lk(final_ckpt_mu_);
                    final_assigned_[key] = std::nullopt;  // register before sending
                }
                RequestFinalCheckpointMsg req;
                req.job_id = job_id;
                req.role = role;
                req.subtask_idx = sub;
                if (!send_frame_(encode_frame(MessageKind::RequestFinalCheckpoint, req))) {
                    std::lock_guard lk(final_ckpt_mu_);
                    final_assigned_.erase(key);
                    return 0;
                }
                std::unique_lock lk(final_ckpt_mu_);
                final_ckpt_cv_.wait_for(lk, std::chrono::seconds(30), [&] {
                    return final_assigned_[key].has_value() ||
                           cancel_token->load(std::memory_order_acquire) ||
                           stop_.load(std::memory_order_acquire);
                });
                const auto v = final_assigned_[key];
                final_assigned_.erase(key);
                return v.value_or(0);  // 0 on cancel/stop -> source skips the commit
            };
            auto wait_final_committed = [this, job_id, cancel_token](
                                            std::uint64_t id,
                                            std::chrono::milliseconds timeout) -> bool {
                std::unique_lock lk(final_ckpt_mu_);
                final_ckpt_cv_.wait_for(lk, timeout, [&] {
                    auto it = final_committed_high_water_.find(job_id);
                    return (it != final_committed_high_water_.end() && it->second >= id) ||
                           cancel_token->load(std::memory_order_acquire) ||
                           stop_.load(std::memory_order_acquire);
                });
                // Return the ACTUAL committed status: a cancel/stop wake returns
                // false so the source exits cleanly (the runner's !should_stop()
                // gate suppresses the throw) rather than claiming the tail durable.
                auto it = final_committed_high_water_.find(job_id);
                return it != final_committed_high_water_.end() && it->second >= id;
            };
            // Callback the source runner uses to register its barrier
            // injectors with the worker. Multiple subtasks per (job, sub_idx)
            // would overwrite; in v1 each subtask has at most one set.
            // On first registration we replay any triggers the coordinator fired
            // before the chain was ready, so periodic checkpoints never
            // drop the leading interval.
            auto register_injectors = [this, job_id, sub = task.subtask_idx](
                                          std::vector<RunnerContext::SourceInjectorFn> injectors) {
                std::vector<std::uint64_t> replay;
                std::vector<RunnerContext::SourceInjectorFn> snapshot;
                {
                    std::lock_guard lock(mu_);
                    auto& entry = per_job_injectors_[job_id][sub];
                    entry.injectors = std::move(injectors);
                    auto pt_it = pending_triggers_.find(job_id);
                    if (pt_it != pending_triggers_.end()) {
                        replay = std::move(pt_it->second);
                        pending_triggers_.erase(pt_it);
                    }
                    if (!replay.empty()) {
                        snapshot = entry.injectors;
                    }
                }
                for (auto ckpt_id : replay) {
                    CheckpointBarrier barrier(CheckpointId{ckpt_id});
                    for (auto& fn : snapshot) {
                        try {
                            fn(barrier);
                        } catch (...) {
                            // Best-effort.
                        }
                    }
                }
            };
            // 2PC commit-callback registration. Sink runners that
            // implement the 2PC protocol register one or more callbacks
            // here; the worker dispatches them on CommitCheckpoint.
            auto register_commits = [this, job_id, sub = task.subtask_idx](
                                        std::vector<RunnerContext::CommitCheckpointFn> cbs) {
                std::lock_guard lock(mu_);
                auto& bucket = per_job_committers_[job_id][sub];
                for (auto& cb : cbs)
                    bucket.push_back(std::move(cb));
            };
            // Abort-callback registration. Mirrors the
            // commit-callback path; sink runners register one or more
            // abort callbacks alongside their commits, and the worker
            // dispatches them on AbortCheckpoint.
            auto register_aborts = [this, job_id, sub = task.subtask_idx](
                                       std::vector<RunnerContext::AbortCheckpointFn> cbs) {
                std::lock_guard lock(mu_);
                auto& bucket = per_job_aborters_[job_id][sub];
                for (auto& cb : cbs)
                    bucket.push_back(std::move(cb));
            };
            // Drain-callback registration. Subtasks
            // participating in adaptive rescaling register their
            // drain closure here; the worker keys the registration by
            // (job_id, role) so BeginRescale dispatch can target the
            // right operator's subtasks. Multiple subtasks of the
            // same operator accumulate in the same vector.
            auto register_drains =
                [this, job_id, role = task.role](std::vector<RunnerContext::DrainFn> cbs) {
                    std::lock_guard lock(mu_);
                    auto& bucket = per_job_drain_callbacks_[job_id][role];
                    for (auto& cb : cbs)
                        bucket.push_back(std::move(cb));
                };
            // Checkpoint-retention registration. make_subtask_job_config
            // calls this with the subtask's freshly-built state backend;
            // CommitCheckpoint then purges its superseded checkpoints.
            auto register_backend =
                [this, job_id, sub = task.subtask_idx](std::shared_ptr<StateBackend> backend) {
                    std::lock_guard lock(mu_);
                    per_job_backends_[job_id][sub] = std::move(backend);
                };
            // Rescale directives: kRestoreFromSelf + {0,0} means "no
            // override" (back-compat). Widen the {0,0} sentinel range
            // to the full [0, kNumKeyGroups) so the runner's eventual
            // restore() call gets a no-op filter rather than dropping
            // every key.
            const std::uint32_t rescale_parent_idx = task.restore_from_subtask_idx;
            const std::uint32_t rescale_parent_count =
                task.restore_from_parent_count == 0 ? 1 : task.restore_from_parent_count;
            clink::KeyGroupRange kg_filter{};
            if (!(task.key_group_first == 0 && task.key_group_last == 0)) {
                kg_filter.first = static_cast<clink::KeyGroup>(task.key_group_first);
                kg_filter.last = static_cast<clink::KeyGroup>(task.key_group_last);
            }
            RunnerContext rctx{
                .in_bridges = in_bridges,
                .output_groups = resolved_groups,
                .chain = chain,
                .checkpoint_dir = checkpoint_dir,
                .restore_from_dir = restore_from_dir,
                .restore_from_checkpoint_id = restore_from_checkpoint_id,
                .capture_dir = capture_dir,
                .capture_records = capture_records,
                .state_backend_uri = state_backend_uri,
                // Build the backend through the HOST's factory (this TU is
                // clink_node, whose singleton has the dynamically-registered
                // remote-read://-style schemes), not the dlopen'd plugin's
                // .so-local singleton which only has the ctor builtins.
                .state_backend_factory = &clink::StateBackendFactory::default_instance(),
                // Capture the HOST's logger + metrics registry in this clink_node
                // TU and carry them across the dlopen boundary by data, so an
                // operator's logs/gauges reach the node (not the .so's private
                // per-RTLD_LOCAL singletons). See RunnerContext::logger.
                .logger = clink::logging::host_logger(),
                .metrics = &clink::MetricsRegistry::global(),
                .unaligned_checkpoints = unaligned_ckpt,
                .expected_state_versions_packed = expected_state_versions_packed,
                .restore_from_subtask_idx = rescale_parent_idx,
                .restore_from_parent_count = rescale_parent_count,
                .restore_key_group_filter = kg_filter,
                .on_checkpoint_ack = std::move(ack_cb),
                .request_final_checkpoint = std::move(request_final_ckpt),
                .wait_final_committed = std::move(wait_final_committed),
                .register_source_injectors = std::move(register_injectors),
                .register_commit_callbacks = std::move(register_commits),
                .register_abort_callbacks = std::move(register_aborts),
                .register_drain_callbacks = std::move(register_drains),
                .register_checkpoint_backend = std::move(register_backend),
                .runner_role = task.role,
            };
            // Register the cancel token (created above so the EOS waits could
            // capture it) under mu_ before the runner starts, so the CancelJob
            // handler sees it immediately; cleared in run_task_'s finally block.
            {
                std::lock_guard lock(mu_);
                per_job_cancel_tokens_[job_id][task.subtask_idx] = cancel_token;
            }
            rctx.cancel_token = cancel_token;
            (*runner)(rctx);
            {
                std::lock_guard lock(mu_);
                auto job_it = per_job_cancel_tokens_.find(job_id);
                if (job_it != per_job_cancel_tokens_.end()) {
                    job_it->second.erase(task.subtask_idx);
                    if (job_it->second.empty()) {
                        per_job_cancel_tokens_.erase(job_it);
                    }
                }
                // Drop registered commit callbacks for this subtask:
                // the sink has finished, so any further CommitCheckpoint
                // for this subtask is moot. Late CommitCheckpoint for
                // already-committed checkpoints will simply find no
                // callbacks registered.
                auto cm_it = per_job_committers_.find(job_id);
                if (cm_it != per_job_committers_.end()) {
                    cm_it->second.erase(task.subtask_idx);
                    if (cm_it->second.empty())
                        per_job_committers_.erase(cm_it);
                }
                // Drop this subtask's retention backend handle. When the
                // last subtask of the job exits, also drop the job's
                // retention tracker so neither map leaks across jobs.
                auto be_it = per_job_backends_.find(job_id);
                if (be_it != per_job_backends_.end()) {
                    be_it->second.erase(task.subtask_idx);
                    if (be_it->second.empty()) {
                        per_job_backends_.erase(be_it);
                        per_job_retention_.erase(job_id);
                    }
                }
                // Same cleanup for abort callbacks - any late
                // AbortCheckpoint for this dead subtask finds no
                // registered callback and falls through silently.
                auto ab_it = per_job_aborters_.find(job_id);
                if (ab_it != per_job_aborters_.end()) {
                    ab_it->second.erase(task.subtask_idx);
                    if (ab_it->second.empty())
                        per_job_aborters_.erase(ab_it);
                }
                // Drain callbacks are keyed by
                // (job_id, role) not (job_id, subtask_idx). Erasing
                // the whole role's entry on any subtask exit would
                // drop callbacks for sibling subtasks still running,
                // so we leave the entry in place; cleanup happens
                // when the job ends (jobs_ teardown elsewhere).
                // Late BeginRescale for an op whose subtasks have
                // all exited finds no callbacks invoked (the
                // closures captured weak refs that no longer lock).
            }
            return;
        }
    }

    // 5. Legacy fallback: typed Dag dispatch via OperatorRegistry.
    // Handles length>=2 chains (composition via ChainedOperator) and
    // any op types not yet migrated to the runner registry.
    //
    // Prefer the per-job bundle's OperatorRegistry (parent-fallback to
    // the default singleton) so chain dispatch finds inline-lambda
    // ops that were mirrored in via PluginRegistry::register_operator.
    // Without that the chain dispatch can't build typed Operators for
    // inline ops and the planner has to leave them unchained, costing
    // a per-record cross-thread channel hop at every op boundary.
    const auto& reg = job_or != nullptr ? *job_or : OperatorRegistry::default_instance();
    OperatorBuildContext ctx;
    ctx.params = op.params;
    ctx.subtask_idx = chain.subtask_idx_in_op;
    ctx.parallelism = op.parallelism;

    // Chained ops: build a single typed Dag containing every chain op
    // wired via DagBuilders. This replaces the previous compose_chain_step
    // path which was limited to the 8 built-in (int64/string) channel-
    // type combinations and couldn't dispatch chains with user-registered
    // types like bench.in/bench.out. The DagBuilder path is fully type-
    // erased: each registered builder captures its T at registration
    // time, threads handles through std::any, and we collect runner
    // indices per op for side-output attachment.
    if (chain.ops.size() >= 2 || has_fused_endpoints) {
        for (const auto& co : chain.ops) {
            if (co.kind != OperatorKind::Operator) {
                throw std::runtime_error("generic subtask: chained ops must all be Operator kind");
            }
        }
        // Validate adjacency before doing any work.
        for (std::size_t i = 1; i < chain.ops.size(); ++i) {
            if (chain.ops[i - 1].out_channel != chain.ops[i].in_channel) {
                throw std::runtime_error("generic subtask: chain channel type mismatch at index " +
                                         std::to_string(i));
            }
        }

        Dag dag;
        // 1. Input stage. Either fused (Dag::add_source from inline
        //    Source<T> built via OperatorRegistry) or normal
        //    (NetworkBridgeSource<T> handles built earlier from
        //    chain.input_edges).
        const auto& chain_head = chain.ops.front();
        const auto* in_ops = job_tr->find(chain_head.in_channel);
        if (in_ops == nullptr) {
            throw std::runtime_error("generic subtask: TypeOps missing for chain input channel '" +
                                     chain_head.in_channel + "'");
        }
        std::any prev;
        if (chain.fused_source.has_value()) {
            const auto& fs = *chain.fused_source;
            const auto* sf = reg.find_source(fs.type, fs.out_channel);
            if (sf == nullptr) {
                throw std::runtime_error(
                    "generic subtask: fused source factory missing for type '" + fs.type +
                    "' on channel '" + fs.out_channel + "'");
            }
            if (!in_ops->add_fused_source_to_dag) {
                throw std::runtime_error(
                    "generic subtask: TypeOps missing add_fused_source_to_dag for channel '" +
                    fs.out_channel + "'");
            }
            OperatorBuildContext bctx;
            bctx.params = fs.params;
            bctx.subtask_idx = chain.subtask_idx_in_op;
            bctx.parallelism = fs.parallelism;
            auto boxed = sf->build(bctx);
            // Wire the fused source's checkpoint commit/abort notifications
            // (messaging sources defer their broker ack to commit). The non-fused
            // SubtaskRunner does this via register_commit_callbacks; the inline
            // fused-chain path has no such wiring, so register directly into the
            // worker's per-subtask committer/aborter buckets - dispatched by
            // handle_commit_checkpoint_ / handle_abort_checkpoint_ the same way. A
            // shared copy of the boxed source is passed (weak-captured inside), so
            // ownership still moves to the dag below.
            if (in_ops->fused_source_commit_hooks) {
                auto [commit_cb, abort_cb] = in_ops->fused_source_commit_hooks(boxed);
                std::lock_guard lock(mu_);
                per_job_committers_[job_id][task.subtask_idx].push_back(std::move(commit_cb));
                per_job_aborters_[job_id][task.subtask_idx].push_back(std::move(abort_cb));
            }
            prev = in_ops->add_fused_source_to_dag(dag, std::move(boxed));
        } else {
            if (!in_ops->build_chain_input_stage) {
                throw std::runtime_error(
                    "generic subtask: chain input builder missing for channel '" +
                    chain_head.in_channel + "' (re-register type T?)");
            }
            prev = in_ops->build_chain_input_stage(dag, in_bridges);
        }

        // 2. DagBuilders for each chain op. Collect per-op runner index
        //    for side-output attachment below. Use the per-job bundle's
        //    DagBuilderRegistry so inline-lambda DagBuilders that the
        //    .so registered via PluginRegistry::register_operator are
        //    visible (the worker's default-instance singleton isn't, due
        //    to RTLD_LOCAL).
        const auto& dbr = job_dbr != nullptr ? *job_dbr : DagBuilderRegistry::default_instance();
        std::vector<std::size_t> runner_indexes;
        runner_indexes.reserve(chain.ops.size());
        for (std::size_t i = 0; i < chain.ops.size(); ++i) {
            const auto& co = chain.ops[i];
            const auto* builder = dbr.find(co.type);
            if (builder == nullptr) {
                throw std::runtime_error("generic subtask: DagBuilder missing for '" + co.type +
                                         "' in chain (op not registered via "
                                         "PluginRegistry::register_operator?)");
            }
            clink::plugin::BuildContext bctx;
            bctx.params = co.params;
            bctx.subtask_idx = chain.subtask_idx_in_op;
            bctx.parallelism = 1;  // chain dispatch is per-subtask
            auto handle = (*builder)(dag, std::vector<std::any>{std::move(prev)}, bctx);
            runner_indexes.push_back(handle.runner_index);
            // Stamp the spec node id + uid so LocalExecutor emits the
            // op_id<->node mapping (clink_op_info). The DagBuilder doesn't carry
            // the chain identity, so do it here where co.id/co.uid are known.
            dag.set_runner_identity(handle.runner_index, co.id, co.uid);
            // Record-capture op-spec sidecar: persist each chained op's build
            // spec next to its epochs so `clink replay` can rebuild it offline.
            // NOTE: set_runner_identity above can re-derive the OperatorId from
            // the uid, so read the id AFTER stamping.
            if (!capture_dir.empty()) {
                clink::capture::write_op_spec(
                    capture_dir,
                    clink::OperatorId{dag.runner_id_at(handle.runner_index)},
                    chain.subtask_idx,
                    clink::capture::OpSpecSidecar{
                        .op_type = co.type,
                        .in_channel = co.in_channel,
                        .out_channel = co.out_channel,
                        .uid = co.uid,
                        .params = co.params,
                    });
            }
            prev = std::move(handle.main_handle);
        }

        // 3. Output stage. Either fused (Dag::add_sink to inline
        //    Sink<T> built via OperatorRegistry) or normal
        //    (peer-bridge groups via attach_chain_main_outputs).
        const auto& chain_tail = chain.ops.back();
        const auto* out_ops = job_tr->find(chain_tail.out_channel);
        if (out_ops == nullptr) {
            throw std::runtime_error("generic subtask: TypeOps missing for chain output channel '" +
                                     chain_tail.out_channel + "'");
        }
        if (chain.fused_sink.has_value()) {
            const auto& fk = *chain.fused_sink;
            const auto* sf = reg.find_sink(fk.type, fk.in_channel);
            if (sf == nullptr) {
                throw std::runtime_error("generic subtask: fused sink factory missing for type '" +
                                         fk.type + "' on channel '" + fk.in_channel + "'");
            }
            if (!out_ops->add_fused_sink_to_dag) {
                throw std::runtime_error(
                    "generic subtask: TypeOps missing add_fused_sink_to_dag for channel '" +
                    fk.in_channel + "'");
            }
            OperatorBuildContext bctx;
            bctx.params = fk.params;
            bctx.subtask_idx = chain.subtask_idx_in_op;
            bctx.parallelism = fk.parallelism;
            auto boxed = sf->build(bctx);
            // Wire the fused 2PC sink's per-checkpoint commit/abort (the inline
            // fused-chain rctx has no register_commit_callbacks; without this a
            // fused 2PC sink would only commit at terminal via the dag path). A
            // shared copy of the boxed sink is passed (weak-captured inside), so
            // ownership still moves to the dag below. The same post-run cleanup
            // that drops the fused source's callbacks covers these.
            if (out_ops->fused_sink_commit_hooks) {
                auto [commit_cb, abort_cb] = out_ops->fused_sink_commit_hooks(boxed);
                std::lock_guard lock(mu_);
                per_job_committers_[job_id][task.subtask_idx].push_back(std::move(commit_cb));
                per_job_aborters_[job_id][task.subtask_idx].push_back(std::move(abort_cb));
            }
            out_ops->add_fused_sink_to_dag(dag, std::move(prev), std::move(boxed));
        } else {
            if (!out_ops->attach_chain_main_outputs) {
                throw std::runtime_error(
                    "generic subtask: chain main-output attacher missing for channel '" +
                    chain_tail.out_channel + "'");
            }
            const auto main_groups = main_output_groups_of(resolved_groups);
            const bool is_split = chain.output_routing == OperatorChainSpec::OutputRouting::Split;
            out_ops->attach_chain_main_outputs(
                dag, std::move(prev), main_groups, is_split, chain.output_selector_fn);
        }

        // 4. Side outputs. Each side-output group names a tag; find which
        //    inner op declared it and attach via that op's runner_index.
        for (const auto& g : resolved_groups) {
            if (g.side_output_tag.empty() || g.peers.empty()) {
                continue;
            }
            std::size_t owner_runner_idx = 0;
            bool found = false;
            for (std::size_t i = 0; i < chain.ops.size(); ++i) {
                for (const auto& so : chain.ops[i].side_outputs) {
                    if (so.tag == g.side_output_tag) {
                        owner_runner_idx = runner_indexes[i];
                        found = true;
                        break;
                    }
                }
                if (found) {
                    break;
                }
            }
            if (!found) {
                throw std::runtime_error("generic subtask: side output tag '" + g.side_output_tag +
                                         "' not declared by any chain op");
            }
            const auto* attach =
                SideOutputAttacherRegistry::default_instance().find(g.channel_type);
            if (attach == nullptr) {
                throw std::runtime_error("generic subtask: no side-output attacher for channel '" +
                                         g.channel_type + "' (tag '" + g.side_output_tag + "')");
            }
            (*attach)(dag, owner_runner_idx, g.side_output_tag, g);
        }

        // Per-operator network bytes attribution for this chain. Input-side
        // runners (bridges + any union) carry the chain head's input bytes
        // (bytes_received); output-side runners (bridges + any split/fork) carry
        // the chain tail's output bytes (bytes_sent). The op runners themselves
        // ignore the attribution. For a single-op chain (the common case) head
        // == tail. The bridges read this via their RuntimeContext.
        if (!runner_indexes.empty()) {
            const auto head_id = dag.runner_id_at(runner_indexes.front());
            const auto tail_id = dag.runner_id_at(runner_indexes.back());
            for (std::size_t i = 0; i < runner_indexes.front(); ++i) {
                dag.set_runner_attributed_op_id(i, head_id);
            }
            for (std::size_t i = runner_indexes.back() + 1; i < dag.runners().size(); ++i) {
                dag.set_runner_attributed_op_id(i, tail_id);
            }
        }

        // Build a RunnerContext so make_subtask_job_config can wire
        // the state backend, restore directory, checkpoint-ack
        // callback, and cancel token onto the JobConfig. Without this
        // the chain task ran with no state backend - operators that
        // checked has_state_backend() (e.g. TumblingWindowOperator's
        // persistent ctor path) silently fell back to the in-memory
        // store, breaking per-record durability assumptions.
        clink::cluster::RunnerContext rctx{
            .in_bridges = in_bridges,
            .output_groups = resolved_groups,
            .chain = chain,
            .checkpoint_dir = checkpoint_dir,
            .restore_from_dir = restore_from_dir,
            .restore_from_checkpoint_id = restore_from_checkpoint_id,
            .capture_dir = capture_dir,
            .capture_records = capture_records,
            .state_backend_uri = state_backend_uri,
            // Host-captured logger + metrics (this is the clink_node TU), carried
            // across the dlopen boundary by data. See RunnerContext::logger.
            .logger = clink::logging::host_logger(),
            .metrics = &clink::MetricsRegistry::global(),
            .unaligned_checkpoints = unaligned_ckpt,
            .expected_state_versions_packed = expected_state_versions_packed,
            .restore_from_subtask_idx = task.restore_from_subtask_idx == kRestoreFromSelf
                                            ? std::numeric_limits<std::uint32_t>::max()
                                            : task.restore_from_subtask_idx,
            .restore_from_parent_count =
                task.restore_from_parent_count == 0 ? 1 : task.restore_from_parent_count,
            .restore_key_group_filter = {},
            // Wire the checkpoint-ack callback so chained subtasks report
            // SubtaskCheckpointed to the coordinator, keyed by the GLOBAL
            // task.subtask_idx (what the coordinator tracks in task_records). The
            // single-op path below does this; without it here, any job
            // with a chained operator never completes a periodic
            // checkpoint - every chained subtask's ack slot stays
            // pending, so latest_completed_checkpoint_id never advances
            // (which in turn blocks rescale and distributed recovery).
            .on_checkpoint_ack =
                [this, job_id, role = task.role, sub = task.subtask_idx](
                    std::uint64_t ckpt_id, bool ok, std::string error) {
                    SubtaskCheckpointedMsg m;
                    m.job_id = job_id;
                    m.checkpoint_id = ckpt_id;
                    m.role = role;
                    m.subtask_idx = sub;
                    m.ok = ok;
                    m.error = std::move(error);
                    send_frame_(encode_frame(MessageKind::SubtaskCheckpointed, m));
                },
            .runner_role = task.role,
        };
        LocalExecutor exec(std::move(dag), clink::plugin::detail::make_subtask_job_config(rctx));
        exec.run();
        // Drop this subtask's fused-source commit/abort callbacks now the runner
        // has exited; a late CommitCheckpoint/AbortCheckpoint then finds none
        // registered (mirrors the SubtaskRunner path's cleanup above).
        {
            std::lock_guard lock(mu_);
            if (auto it = per_job_committers_.find(job_id); it != per_job_committers_.end()) {
                it->second.erase(task.subtask_idx);
                if (it->second.empty()) {
                    per_job_committers_.erase(it);
                }
            }
            if (auto it = per_job_aborters_.find(job_id); it != per_job_aborters_.end()) {
                it->second.erase(task.subtask_idx);
                if (it->second.empty()) {
                    per_job_aborters_.erase(it);
                }
            }
        }
        return;
    }

    if (op.kind == OperatorKind::Source) {
        const auto* fac = reg.find_source(op.type, op.out_channel);
        if (fac == nullptr) {
            throw std::runtime_error("generic subtask: source factory not found for '" + op.type +
                                     "'");
        }
        if (resolved_groups.empty()) {
            throw std::runtime_error("generic subtask: source has no resolved peers");
        }
        auto raw = fac->build(ctx);
        if (op.out_channel == kChannelInt64) {
            auto src = std::static_pointer_cast<Source<std::int64_t>>(raw);
            run_source_dispatch<std::int64_t>(src,
                                              resolved_groups,
                                              int64_codec(),
                                              chain.output_routing,
                                              chain.output_selector_fn);
        } else if (op.out_channel == kChannelString) {
            auto src = std::static_pointer_cast<Source<std::string>>(raw);
            run_source_dispatch<std::string>(src,
                                             resolved_groups,
                                             string_codec(),
                                             chain.output_routing,
                                             chain.output_selector_fn);
        } else {
            throw std::runtime_error("generic subtask: unsupported source out_channel '" +
                                     op.out_channel + "'");
        }
        return;
    }

    // Join kind: dispatched through RunnerRegistry::find_join above.
    // Reaching here with kind==Join + null runner means no registration
    // existed for (op.type, in1, in2, out) - either a typo or a
    // plugin failed to register. Surface a clear error.
    if (op.kind == OperatorKind::Join) {
        throw std::runtime_error(
            "generic subtask: unknown join op type '" + op.type +
            "' (no SubtaskRunner registered via RunnerRegistry::register_join)");
    }

    if (op.kind == OperatorKind::Sink) {
        const auto* fac = reg.find_sink(op.type, op.in_channel);
        if (fac == nullptr) {
            throw std::runtime_error("generic subtask: sink factory not found for '" + op.type +
                                     "'");
        }
        if (in_bridges.empty()) {
            throw std::runtime_error("generic subtask: sink has no input bridges");
        }
        auto raw = fac->build(ctx);
        if (op.in_channel == kChannelInt64) {
            auto sink = std::static_pointer_cast<Sink<std::int64_t>>(raw);
            run_sink_dispatch<std::int64_t>(in_bridges, sink);
        } else if (op.in_channel == kChannelString) {
            auto sink = std::static_pointer_cast<Sink<std::string>>(raw);
            run_sink_dispatch<std::string>(in_bridges, sink);
        } else {
            throw std::runtime_error("generic subtask: unsupported sink in_channel '" +
                                     op.in_channel + "'");
        }
        return;
    }

    // OperatorKind::Operator - mid-chain transform.
    const auto* op_fac = reg.find_operator(op.type, op.in_channel, op.out_channel);
    if (op_fac == nullptr) {
        throw std::runtime_error("generic subtask: operator factory not found for '" + op.type +
                                 "' (in=" + channel_type_name(op.in_channel) +
                                 ", out=" + channel_type_name(op.out_channel) + ")");
    }
    if (in_bridges.empty()) {
        throw std::runtime_error("generic subtask: operator has no input bridges");
    }
    if (resolved_groups.empty()) {
        throw std::runtime_error("generic subtask: operator has no resolved output peers");
    }
    auto raw = op_fac->build(ctx);

    // 2x2 channel-type matrix. Adding a new ChannelType means adding
    // arms here (and to run_source_dispatch / run_sink_dispatch).
    if (op.in_channel == std::string{clink::cluster::kChannelInt64} &&
        op.out_channel == std::string{clink::cluster::kChannelInt64}) {
        auto op_typed = std::static_pointer_cast<Operator<std::int64_t, std::int64_t>>(raw);
        run_operator_dispatch<std::int64_t, std::int64_t>(in_bridges,
                                                          op_typed,
                                                          resolved_groups,
                                                          int64_codec(),
                                                          chain.output_routing,
                                                          chain.output_selector_fn);
    } else if (op.in_channel == std::string{clink::cluster::kChannelString} &&
               op.out_channel == std::string{clink::cluster::kChannelString}) {
        auto op_typed = std::static_pointer_cast<Operator<std::string, std::string>>(raw);
        run_operator_dispatch<std::string, std::string>(in_bridges,
                                                        op_typed,
                                                        resolved_groups,
                                                        string_codec(),
                                                        chain.output_routing,
                                                        chain.output_selector_fn);
    } else if (op.in_channel == std::string{clink::cluster::kChannelInt64} &&
               op.out_channel == std::string{clink::cluster::kChannelString}) {
        auto op_typed = std::static_pointer_cast<Operator<std::int64_t, std::string>>(raw);
        run_operator_dispatch<std::int64_t, std::string>(in_bridges,
                                                         op_typed,
                                                         resolved_groups,
                                                         string_codec(),
                                                         chain.output_routing,
                                                         chain.output_selector_fn);
    } else if (op.in_channel == std::string{clink::cluster::kChannelString} &&
               op.out_channel == std::string{clink::cluster::kChannelInt64}) {
        auto op_typed = std::static_pointer_cast<Operator<std::string, std::int64_t>>(raw);
        run_operator_dispatch<std::string, std::int64_t>(in_bridges,
                                                         op_typed,
                                                         resolved_groups,
                                                         int64_codec(),
                                                         chain.output_routing,
                                                         chain.output_selector_fn);
    } else {
        throw std::runtime_error("generic subtask: unsupported (in,out) channel pair");
    }
}

bool Worker::send_frame_(const std::vector<std::byte>& frame) {
    std::lock_guard lock(send_mu_);
    if (!conn_)
        return false;
    return conn_->send_all(frame.data(), frame.size());
}

bool Worker::await_all_tasks(std::chrono::milliseconds timeout) {
    std::unique_lock lock(mu_);
    return cv_.wait_for(lock, timeout, [this] {
        return cancelled_.load(std::memory_order_acquire) || (deployed_ && in_flight_tasks_ == 0);
    });
}

void Worker::stop() {
    stop_.store(true, std::memory_order_release);
    {
        std::lock_guard lock(mu_);
        for (auto& [_, pt] : pending_) {
            pt->cancelled = true;
            pt->cv.notify_all();
        }
    }
    // Wake any source blocked in the EOS final-checkpoint waits (their
    // predicates check stop_) so the runner threads can be joined promptly.
    // Brief final_ckpt_mu_ acquire serialises against a mid-predicate waiter
    // (stop_ lives outside the mutex), closing the lost-wakeup window.
    {
        std::lock_guard<std::mutex> wake(final_ckpt_mu_);
    }
    final_ckpt_cv_.notify_all();
    if (conn_) {
        conn_->shutdown_read();
    }
    if (reader_.joinable()) {
        reader_.join();
    }
    if (heartbeat_.joinable()) {
        heartbeat_.join();
    }
    for (auto& t : task_threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
    task_threads_.clear();
    conn_.reset();
}

std::optional<Worker::JobStateExport> Worker::export_job_state_arrow(JobId job_id) const {
    // Copy the backend handles out under the lock, export outside it: a
    // backend export takes the backend's own locks and can be slow (a
    // RocksDB iteration), and must not block deploys or heartbeats.
    std::vector<std::shared_ptr<StateBackend>> backends;
    {
        std::lock_guard lock(mu_);
        auto it = per_job_backends_.find(job_id);
        if (it == per_job_backends_.end() || it->second.empty()) {
            return std::nullopt;
        }
        backends.reserve(it->second.size());
        for (const auto& [subtask, backend] : it->second) {
            if (backend) {
                backends.push_back(backend);
            }
        }
    }
    if (backends.empty()) {
        return std::nullopt;
    }
    JobStateExport out;
    std::vector<std::vector<std::byte>> parts;
    parts.reserve(backends.size());
    for (const auto& backend : backends) {
        try {
            parts.push_back(backend->export_arrow_snapshot());
        } catch (const std::exception&) {
            // A backend without a complete live view (disaggregated hot
            // tier) refuses; report it rather than silently omitting.
            ++out.skipped_subtasks;
        }
    }
    out.bytes = InMemoryStateBackend::merge_snapshot_bytes(parts);
    return out;
}

WorkerSnapshot Worker::snapshot_worker() const {
    WorkerSnapshot s;
    s.worker_id = worker_id_;
    s.data_host = data_host_;
    s.slot_capacity = cfg_.slot_count;
    s.coordinator_host = coordinator_host_;
    s.coordinator_port = coordinator_port_;
    std::lock_guard lock(mu_);
    s.slots_in_use = static_cast<std::uint32_t>(in_flight_tasks_);
    s.active_subtasks = in_flight_tasks_;
    return s;
}

std::vector<SubtaskRecord> Worker::snapshot_subtasks() const {
    std::vector<SubtaskRecord> out;
    std::lock_guard lock(mu_);
    out.reserve(pending_.size());
    for (const auto& [key, pt] : pending_) {
        SubtaskRecord r;
        r.job_id = std::get<0>(key);
        r.role = std::get<1>(key);
        r.subtask_idx = std::get<2>(key);
        // ready=true means PeerUpdate arrived and the runner is on
        // its way; pre-PeerUpdate the slot is "pending". cancelled
        // wins over both because the runner never actually starts.
        if (pt->cancelled) {
            r.status = "cancelled";
        } else if (pt->ready) {
            r.status = "ready";
        } else {
            r.status = "pending";
        }
        out.push_back(std::move(r));
    }
    return out;
}

}  // namespace clink::cluster
