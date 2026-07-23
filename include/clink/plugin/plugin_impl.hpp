// Plugin-facing template implementations. Included by plugin.hpp;
// users never include this file directly.
//
// These templates instantiate in the plugin's compilation unit. Each
// instantiation:
//   1. Records typed bridge builders in TypeRegistry (via
//      register_type<T>'s implicit call chain).
//   2. Builds a SubtaskRunner closure that captures T and the user's
//      factory, then stores it in RunnerRegistry keyed by op-type
//      and channel-name.
//
// The runners do the work the generic role used to do via hand-written
// channel-type switches: build the typed input stage from inbound
// bridges, instantiate the user's source/operator/sink, attach the
// typed output groups, run the LocalExecutor.

#pragma once

#include "clink/cluster/dag_builder_registry.hpp"
#include "clink/cluster/runner_helpers.hpp"
#include "clink/core/columnar_batcher.hpp"  // make_auto_arrow_batcher
#include "clink/metrics/metrics_registry.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/state/state_backend_factory.hpp"

namespace clink::plugin {

namespace detail {

// Build a plugin-side BuildContext (with params map + subtask info)
// from a RunnerContext. Pulls params from the leading op of the chain;
// for length-1 chains that's the only op.
inline BuildContext build_ctx_from(const clink::cluster::RunnerContext& rctx) {
    BuildContext bctx;
    if (!rctx.chain.ops.empty()) {
        bctx.params = rctx.chain.ops[0].params;
        bctx.parallelism = rctx.chain.ops[0].parallelism;
    }
    bctx.subtask_idx = rctx.chain.subtask_idx_in_op;
    return bctx;
}

// Apply the leading op's uid + display_name (carried across the wire
// via ChainOp) onto the freshly-built operator instance. The worker-side
// Dag will see uid() / display_name() on this op and derive a stable
// OperatorId via hash("uid/" + uid) if uid is set. Operators built
// from the legacy/in-process path that don't go through ChainOp can
// still set uid directly via op->set_uid before add_*.
template <typename OpPtr>
inline void apply_chain_identity(OpPtr& op, const clink::cluster::RunnerContext& rctx) {
    if (rctx.chain.ops.empty()) {
        return;
    }
    const auto& co = rctx.chain.ops[0];
    if (!co.uid.empty()) {
        op->set_uid(co.uid);
    }
    if (!co.display_name.empty()) {
        op->set_display_name(co.display_name);
    }
    // Stamp the spec graph node id so the Dag can carry it onto the runner and
    // LocalExecutor can emit the op_id<->node mapping (clink_op_info). All
    // operators (custom plugin + inline lambda) flow through here.
    if (!co.id.empty()) {
        op->set_spec_node_id(co.id);
    }
}

// Look up TypeOps for a channel name; throw with a clear message if
// missing. Used by the runners to construct typed output bridges.
inline const clink::cluster::TypeOps& require_type_ops(const clink::cluster::TypeRegistry& reg,
                                                       const std::string& channel) {
    const auto* ops = reg.find(channel);
    if (ops == nullptr) {
        throw std::runtime_error("plugin runner: channel type '" + channel +
                                 "' not registered (call register_type<T>(\"" + channel +
                                 "\", codec) first)");
    }
    return *ops;
}

// JobConfig with a per-subtask state backend, selected via the global
// StateBackendFactory. A bare checkpoint_dir is interpreted as the
// "file" scheme to keep existing CheckpointConfig.checkpoint_dir
// strings working; non-empty restore_from_dir asks the factory to stage
// the snapshot file so LocalExecutor's restore-on-start path can load
// it. Adding a new backend (e.g. s3://) means registering a scheme
// with the factory at startup; no change to this function.
//
// When state_backend_uri is set it overrides checkpoint_dir as the
// backend URI, so a remote/disaggregated backend (e.g. remote-read://)
// can be used while checkpoint_dir stays the coordinator's LOCAL coordination
// directory for COMPLETED-N markers and HA recovery. Empty preserves the
// legacy behaviour where checkpoint_dir doubles as the backend URI.
inline clink::JobConfig make_subtask_job_config(const clink::cluster::RunnerContext& rctx) {
    clink::JobConfig cfg;
    // Record-capture flight recorder: thread the job's capture config through
    // to the runner contexts (the operator runner tees input records into
    // per-epoch .cap files when armed - see runtime/record_capture.hpp).
    cfg.capture_dir = rctx.capture_dir;
    cfg.capture_records = static_cast<std::size_t>(rctx.capture_records);
    cfg.capture_subtask_idx = rctx.chain.subtask_idx;
    // Queryable-state identity: the deployment role + global subtask index
    // external clients address lookups by.
    cfg.runner_role = rctx.runner_role;
    cfg.runner_subtask_idx = rctx.chain.subtask_idx;
    clink::StateBackendSpec spec;
    spec.uri = rctx.state_backend_uri.empty() ? rctx.checkpoint_dir : rctx.state_backend_uri;
    spec.subtask_idx = rctx.chain.subtask_idx;
    spec.restore_uri = rctx.restore_from_dir;
    spec.restore_checkpoint_id = rctx.restore_from_checkpoint_id;
    spec.restore_from_subtask_idx = rctx.restore_from_subtask_idx;
    spec.restore_from_parent_count = rctx.restore_from_parent_count;
    // Build through the HOST's factory when provided (rctx.state_backend_factory):
    // a dlopen'd plugin's .so-local StateBackendFactory singleton only has the
    // ctor builtins (memory/file/changelog), NOT the dynamically-registered
    // schemes (remote-read://, rocksdb://, ...) that install_linked_impls()
    // registered on the host's singleton. Falling back to the .so-local
    // default_instance() (nullptr) preserves the in-process / legacy paths.
    clink::StateBackendFactory& factory = rctx.state_backend_factory != nullptr
                                              ? *rctx.state_backend_factory
                                              : clink::StateBackendFactory::default_instance();
    auto built = factory.build(spec);
    cfg.state_backend = built.backend;
    // Hand the freshly-built backend to the worker (if it registered a hook)
    // so it can purge this subtask's superseded checkpoints under the
    // retention policy. Shared ownership: the executor and the worker both
    // hold the backend; purge only touches old on-disk artefacts.
    if (rctx.register_checkpoint_backend && cfg.state_backend) {
        rctx.register_checkpoint_backend(cfg.state_backend);
    }
    if (built.restore_from.has_value()) {
        cfg.restore_from = *built.restore_from;
        cfg.restore_key_group_filter = rctx.restore_key_group_filter;
    }
    // Bridge the cluster's per-subtask checkpoint-ack callback (which
    // sends SubtaskCheckpointed back to the coordinator) onto JobConfig so the
    // local operator runner can invoke it after snapshotting.
    if (rctx.on_checkpoint_ack) {
        auto ack = rctx.on_checkpoint_ack;
        cfg.on_checkpoint_ack = [ack](clink::CheckpointId id, bool ok, std::string error) {
            ack(id.value(), ok, std::move(error));
        };
    }
    // Bounded-source EOS final-checkpoint hooks (cluster path). Bridge the
    // cluster RunnerContext callbacks onto JobConfig so the source runner can
    // request a coordinator-coordinated final checkpoint at EOS and block until it
    // commits. Empty in in-process paths (source falls back to local terminal).
    if (rctx.request_final_checkpoint) {
        cfg.request_final_checkpoint = rctx.request_final_checkpoint;
    }
    if (rctx.wait_final_committed) {
        cfg.wait_final_committed = rctx.wait_final_committed;
    }
    // Thread the Worker-owned cancel token through so a
    // CancelJob message can wind the LocalExecutor down without a
    // reference to it. nullptr in legacy/in-process paths.
    cfg.external_cancel_token = rctx.cancel_token;
    cfg.unaligned_checkpoints = rctx.unaligned_checkpoints;
    // Schema evolution: unpack the declared expected state-version map so
    // the LocalExecutor auto-migrates restored state to it at start. Empty
    // packed string -> no declared versions -> restore verbatim.
    if (!rctx.expected_state_versions_packed.empty()) {
        cfg.expected_state_versions =
            clink::StateVersionMap::unpack(rctx.expected_state_versions_packed);
    }
    // Metrics registry + host logger. CRITICAL: take these from the
    // host-captured rctx pointers, NOT by resolving the global() / host_logger()
    // singletons here. This function is inline and compiles into the plugin .so
    // too; on the SubtaskRunner dispatch path it RUNS inside the .so, where
    // MetricsRegistry::global() / clink::logging::host_logger() would resolve
    // the .so's OWN private singletons (RTLD_LOCAL + static clink_core), so the
    // operator's gauges and logs would never reach the node's /metrics and
    // /api/v1/logs. rctx.metrics / rctx.logger were captured in the clink_node
    // TU (worker.cpp). Fall back to the in-process global only when the
    // host did not set them (LocalExecutor / legacy same-address-space paths).
    cfg.metrics = rctx.metrics != nullptr ? rctx.metrics : &clink::MetricsRegistry::global();
    cfg.logger = rctx.logger;
    return cfg;
}

// Run a deployed subtask's LocalExecutor to completion and PROPAGATE any
// operator-thread failure. The executor catches operator exceptions into
// operator_errors_ and returns normally (so an in-process caller can inspect
// them); on the cluster path the subtask runner must instead let them escape
// so run_task_ reports had_error=true and the coordinator restarts. Without this, a
// bounded source's end-of-stream final-checkpoint throw (the watchdog-restart
// signal when the final checkpoint times out) would be silently swallowed and
// the job could complete with an uncommitted tail. Skipped when the subtask was
// cancelled: teardown can legitimately throw (closed channels) and a cancelled
// job must not spuriously restart - run_task_ already reports had_error there.
inline void run_subtask_to_completion(clink::LocalExecutor& exec,
                                      const std::shared_ptr<std::atomic<bool>>& cancel_token) {
    exec.run();
    if (cancel_token && cancel_token->load(std::memory_order_acquire)) {
        return;
    }
    auto errs = exec.operator_errors();
    if (!errs.empty()) {
        std::string msg = "subtask operator failure:";
        for (const auto& [op_name, err] : errs) {
            msg.append(" [").append(op_name).append("] ").append(err);
        }
        throw std::runtime_error(msg);
    }
}

}  // namespace detail

template <typename T>
void PluginRegistry::register_type(const std::string& name, Codec<T> codec) {
    // Default Arrow path: pass an empty batcher so the 3-arg overload
    // auto-selects one - typed columnar if T opted in via CLINK_ARROW_FIELDS,
    // else binary fallback. The 3-arg overload below also accepts an explicit
    // batcher for the columnar specialisations wired in built_in_factories.cpp.
    register_type<T>(name, std::move(codec), ArrowBatcher<T>{});
}

template <typename T>
void PluginRegistry::register_type(const std::string& name,
                                   Codec<T> codec,
                                   ArrowBatcher<T> batcher) {
    if (!batcher.build) {
        batcher = make_auto_arrow_batcher<T>(codec);
    }
    // Store the codec by typeid so fluent API methods that need to
    // wire a codec into operators (e.g. RocksDB-backed tumbling
    // aggregate) can find it via codec_for<T>().
    codecs_[typeid(T).name()] = std::make_shared<Codec<T>>(codec);
    type_registry_.register_typed<T>(name, codec, batcher);
    // Install the typed side-output attacher under the same channel
    // name, into THIS PluginRegistry's bundle-scoped attacher.
    side_output_attacher_registry_.template register_for_channel<T>(name);

    // Mirror into the .so's OWN default-instance singletons. clink_core
    // is statically linked into each plugin .so, and dlopen(RTLD_LOCAL)
    // gives each .so its private copy of every function-local-static
    // (including Registry::default_instance()). Plugin runtime code
    // and helper templates in runner_helpers.hpp look up via
    // `Registry::default_instance()` inside the .so - that's the .so's
    // private static, NOT the host's. Writing to both keeps host-side
    // (plan_job, dispatch) and .so-internal (attach_side_output_groups,
    // type_ops resolution) views in sync. Idempotent (last-write-wins
    // semantics in each registry).
    auto& so_tr = clink::cluster::TypeRegistry::default_instance();
    if (&so_tr != &type_registry_) {
        so_tr.register_typed<T>(name, std::move(codec), std::move(batcher));
    }
    auto& so_soar = clink::cluster::SideOutputAttacherRegistry::default_instance();
    if (&so_soar != &side_output_attacher_registry_) {
        so_soar.template register_for_channel<T>(name);
    }
}

template <typename T>
std::shared_ptr<const Codec<T>> PluginRegistry::codec_for() const {
    auto it = codecs_.find(typeid(T).name());
    if (it == codecs_.end()) {
        return nullptr;
    }
    return std::static_pointer_cast<const Codec<T>>(it->second);
}

template <typename T>
void PluginRegistry::register_source(
    const std::string& op_type,
    std::function<std::shared_ptr<Source<T>>(const BuildContext&)> factory) {
    const auto channel = type_registry_.channel_for_typeid(typeid(T).name());
    if (channel.empty()) {
        throw std::runtime_error(
            "register_source<T>: T not registered (call register_type<T>(name, codec) first)");
    }
    // Capture factory + the registries we'll need at run-time. The
    // runner closure does the typed Dag construction when invoked.
    clink::cluster::SubtaskRunner runner =
        [factory, &type_registry = type_registry_](const clink::cluster::RunnerContext& rctx) {
            auto src = factory(detail::build_ctx_from(rctx));
            detail::apply_chain_identity(src, rctx);
            // Checkpoint-completion notifications for a source whose resume is an
            // irreversible broker consume (AMQP/JetStream/Pulsar ack): defer the
            // ack from barrier-emit to global commit. Mirrors the 2PC sink wiring
            // below. Weak-capture: a late CommitCheckpoint/AbortCheckpoint during
            // teardown locks to nullptr after the source is destroyed.
            if (rctx.register_commit_callbacks) {
                std::weak_ptr<clink::Source<T>> weak_src = src;
                rctx.register_commit_callbacks({
                    [weak_src](std::uint64_t ckpt) {
                        if (auto s = weak_src.lock()) {
                            s->notify_checkpoint_complete(CheckpointId{ckpt});
                        }
                    },
                });
            }
            if (rctx.register_abort_callbacks) {
                std::weak_ptr<clink::Source<T>> weak_src = src;
                rctx.register_abort_callbacks({
                    [weak_src](std::uint64_t ckpt) {
                        if (auto s = weak_src.lock()) {
                            s->notify_checkpoint_aborted(CheckpointId{ckpt});
                        }
                    },
                });
            }
            const auto& chain = rctx.chain;
            const auto& out_channel = chain.ops.empty() ? std::string{} : chain.ops[0].out_channel;
            const auto& ops = detail::require_type_ops(type_registry, out_channel);
            clink::Dag dag;
            auto h0 = dag.template add_source<T>(src);
            clink::cluster::attach_typed_output_groups<T>(
                dag,
                h0,
                clink::cluster::main_output_groups_of(rctx.output_groups),
                ops,
                chain.output_routing,
                chain.output_selector_fn);
            clink::cluster::attach_side_output_groups(dag, h0.runner_index, rctx.output_groups);
            // This subtask hosts one logical operator (the source); attribute
            // every network-bridge runner's bytes to it (its output bridges ->
            // bytes_sent). The source itself ignores the attribution.
            dag.set_all_runners_attributed_op_id(dag.runner_id_at(h0.runner_index));
            // Hand the source's barrier injectors to the worker before we
            // hand the Dag off to LocalExecutor (which moves it).
            if (rctx.register_source_injectors) {
                std::vector<clink::cluster::RunnerContext::SourceInjectorFn> injectors(
                    dag.source_injectors().begin(), dag.source_injectors().end());
                rctx.register_source_injectors(std::move(injectors));
            }
            // Bridge BeginRescale dispatch into the source
            // runner. Create a shared atomic signal; register a drain
            // callback that sets it to target_parallelism on dispatch;
            // thread the same atomic onto JobConfig so RuntimeContext /
            // dag.hpp source runner can poll it.
            auto drain_signal = std::make_shared<std::atomic<std::uint32_t>>(0);
            if (rctx.register_drain_callbacks) {
                rctx.register_drain_callbacks({
                    [drain_signal](std::uint32_t target) {
                        drain_signal->store(target, std::memory_order_release);
                    },
                });
            }
            auto cfg = detail::make_subtask_job_config(rctx);
            cfg.drain_target = drain_signal;
            clink::LocalExecutor exec(std::move(dag), std::move(cfg));
            detail::run_subtask_to_completion(exec, rctx.cancel_token);
        };
    runner_registry_.register_source(op_type, channel, std::move(runner));

    // Mirror into OperatorRegistry as a BoxedFactory so the chain-
    // dispatch fused-source path (worker.cpp) can resolve the
    // source by (type, out_channel) and build a typed Source<T>
    // without going through the full SubtaskRunner. Required for the
    // par=1 fusion plan where the source is hosted inline on the
    // chain's dag instead of as its own subtask.
    {
        clink::cluster::SourceFactory sf;
        sf.out = channel;
        sf.build = [factory](const clink::cluster::OperatorBuildContext& obctx) {
            BuildContext bctx;
            bctx.subtask_idx = obctx.subtask_idx;
            bctx.parallelism = obctx.parallelism;
            bctx.params = obctx.params;
            return std::static_pointer_cast<void>(factory(bctx));
        };
        operator_registry_.register_source(op_type, std::move(sf));
    }

    // In-process Dag-builder hook. Stores a typed callback under op_type
    // in DagBuilderRegistry so `clink::cluster::LocalSubmitter::submit(env)`
    // can drive the topology end-to-end without coordinator+worker. The closure
    // captures the user's factory and resolves at run time via the same
    // BuildContext shape the cluster runner uses.
    clink::cluster::DagBuilderRegistry::default_instance().register_builder(
        op_type,
        [factory](clink::Dag& dag,
                  const std::vector<std::any>& upstream,
                  const BuildContext& ctx) -> clink::cluster::DagOpHandle {
            (void)upstream;  // sources have no upstream edges
            if (ctx.parallelism > 1) {
                // Parallel path: build N source instances (one per
                // subtask) and let the Dag own per-subtask emitters.
                auto sub_factory = [factory,
                                    base = ctx](std::size_t subtask) -> std::shared_ptr<Source<T>> {
                    BuildContext local = base;
                    local.subtask_idx = static_cast<std::uint32_t>(subtask);
                    return factory(local);
                };
                auto ph =
                    dag.template add_parallel_source<T>(std::move(sub_factory), ctx.parallelism);
                return clink::cluster::DagOpHandle{
                    std::any{std::move(ph)}, /*runner_index=*/0, ctx.parallelism};
            }
            auto src = factory(ctx);
            auto h = dag.template add_source<T>(src);
            return clink::cluster::DagOpHandle{std::any{h}, h.runner_index, /*parallelism=*/1};
        });
}

template <typename T>
void PluginRegistry::register_sink(
    const std::string& op_type,
    std::function<std::shared_ptr<Sink<T>>(const BuildContext&)> factory) {
    const auto channel = type_registry_.channel_for_typeid(typeid(T).name());
    if (channel.empty()) {
        throw std::runtime_error(
            "register_sink<T>: T not registered (call register_type<T>(name, codec) first)");
    }
    clink::cluster::SubtaskRunner runner = [factory](const clink::cluster::RunnerContext& rctx) {
        auto sink = factory(detail::build_ctx_from(rctx));
        detail::apply_chain_identity(sink, rctx);
        // 2PC support: route CommitCheckpoint -> sink->on_commit.
        // Non-2PC sinks have the default no-op so this is harmless.
        // Weak-capture the sink: the runner returns when the
        // executor exits, but the worker may still receive a late
        // CommitCheckpoint while the sink is shutting down - the
        // weak_ptr lock returns nullptr after destruction.
        if (rctx.register_commit_callbacks) {
            std::weak_ptr<clink::Sink<T>> weak_sink = sink;
            rctx.register_commit_callbacks({
                [weak_sink](std::uint64_t ckpt) {
                    if (auto s = weak_sink.lock()) {
                        s->on_commit(ckpt);
                    }
                },
            });
        }
        // Register abort callback alongside commit. The
        // worker dispatches it on AbortCheckpoint; non-2PC sinks ignore
        // the call (Sink::on_abort default no-op).
        if (rctx.register_abort_callbacks) {
            std::weak_ptr<clink::Sink<T>> weak_sink = sink;
            rctx.register_abort_callbacks({
                [weak_sink](std::uint64_t ckpt) {
                    if (auto s = weak_sink.lock()) {
                        s->on_abort(ckpt);
                    }
                },
            });
        }
        clink::Dag dag;
        auto h0 = clink::cluster::build_typed_input_stage<T>(dag, rctx.in_bridges);
        auto hs = dag.template add_sink<T>(h0, sink);
        // This subtask hosts one logical operator (the sink); its input network
        // bridges' bytes are the sink's bytes_received.
        dag.set_all_runners_attributed_op_id(dag.runner_id_at(hs.runner_index));
        clink::LocalExecutor exec(std::move(dag), detail::make_subtask_job_config(rctx));
        detail::run_subtask_to_completion(exec, rctx.cancel_token);
    };
    runner_registry_.register_sink(op_type, channel, std::move(runner));

    // Mirror into OperatorRegistry as a BoxedFactory so the chain-
    // dispatch fused-sink path can build a typed Sink<T> directly.
    // Symmetric with the register_source mirror above.
    {
        clink::cluster::SinkFactory sk;
        sk.in = channel;
        sk.build = [factory](const clink::cluster::OperatorBuildContext& obctx) {
            BuildContext bctx;
            bctx.subtask_idx = obctx.subtask_idx;
            bctx.parallelism = obctx.parallelism;
            bctx.params = obctx.params;
            return std::static_pointer_cast<void>(factory(bctx));
        };
        operator_registry_.register_sink(op_type, std::move(sk));
    }

    clink::cluster::DagBuilderRegistry::default_instance().register_builder(
        op_type,
        [factory](clink::Dag& dag,
                  const std::vector<std::any>& upstream,
                  const BuildContext& ctx) -> clink::cluster::DagOpHandle {
            if (upstream.size() != 1) {
                throw std::runtime_error(
                    "DagBuilder<sink>: expected exactly one upstream edge, got " +
                    std::to_string(upstream.size()));
            }
            if (ctx.parallelism > 1) {
                // Parallel sink: upstream must also be parallel; we
                // forward 1:1 with matching parallelism. (Fan-in
                // sink at p=1 from a parallel upstream is the other
                // legal shape; the walker decides which based on
                // op.parallelism vs upstream.parallelism and we only
                // see one of them here.)
                auto up_handle = std::any_cast<clink::Dag::ParallelStageHandle<T>>(upstream[0]);
                auto sub_factory = [factory,
                                    base = ctx](std::size_t subtask) -> std::shared_ptr<Sink<T>> {
                    BuildContext local = base;
                    local.subtask_idx = static_cast<std::uint32_t>(subtask);
                    return factory(local);
                };
                dag.template add_parallel_sink<T>(
                    up_handle, std::move(sub_factory), ctx.parallelism);
                return clink::cluster::DagOpHandle{std::any{}, /*runner_index=*/0, ctx.parallelism};
            }
            auto in_handle = std::any_cast<clink::StageHandle<T>>(upstream[0]);
            auto sink = factory(ctx);
            auto h = dag.template add_sink<T>(in_handle, sink);
            // Sinks have no downstream main handle to chain from, but
            // their runner_index is still needed for side-output
            // wiring parity with source / operator.
            return clink::cluster::DagOpHandle{std::any{}, h.runner_index, /*parallelism=*/1};
        });
}

template <typename In, typename Out>
void PluginRegistry::register_operator(
    const std::string& op_type,
    std::function<std::shared_ptr<Operator<In, Out>>(const BuildContext&)> factory) {
    const auto in_channel = type_registry_.channel_for_typeid(typeid(In).name());
    const auto out_channel = type_registry_.channel_for_typeid(typeid(Out).name());
    if (in_channel.empty()) {
        throw std::runtime_error(
            "register_operator<In, Out>: In not registered (call register_type<In> first)");
    }
    if (out_channel.empty()) {
        throw std::runtime_error(
            "register_operator<In, Out>: Out not registered (call register_type<Out> first)");
    }
    // Capture codec: the operator runner's record-capture tee encodes input
    // records with the channel's registered Codec<In>. Resolved once here at
    // registration; null (unregistered codec) leaves capture unavailable for
    // this op type rather than failing registration.
    auto capture_codec = codec_for<In>();
    clink::cluster::SubtaskRunner runner =
        [factory, capture_codec, &type_registry = type_registry_](
            const clink::cluster::RunnerContext& rctx) {
            auto op = factory(detail::build_ctx_from(rctx));
            detail::apply_chain_identity(op, rctx);
            const auto& chain = rctx.chain;
            const auto& out_channel = chain.ops.empty() ? std::string{} : chain.ops[0].out_channel;
            const auto& ops = detail::require_type_ops(type_registry, out_channel);
            clink::Dag dag;
            auto h0 = clink::cluster::build_typed_input_stage<In>(dag, rctx.in_bridges);
            auto h1 = dag.template add_operator<In, Out>(h0, op, capture_codec);
            // Record-capture op-spec sidecar: persist this op's build spec
            // next to its epochs so `clink replay` can rebuild it offline.
            if (capture_codec && !rctx.capture_dir.empty() && !chain.ops.empty()) {
                const auto& co = chain.ops[0];
                clink::capture::write_op_spec(rctx.capture_dir,
                                              clink::OperatorId{dag.runner_id_at(h1.runner_index)},
                                              chain.subtask_idx,
                                              clink::capture::OpSpecSidecar{
                                                  .op_type = co.type,
                                                  .in_channel = co.in_channel,
                                                  .out_channel = co.out_channel,
                                                  .uid = co.uid,
                                                  .params = co.params,
                                              });
            }
            clink::cluster::attach_typed_output_groups<Out>(
                dag,
                h1,
                clink::cluster::main_output_groups_of(rctx.output_groups),
                ops,
                chain.output_routing,
                chain.output_selector_fn);
            clink::cluster::attach_side_output_groups(dag, h1.runner_index, rctx.output_groups);
            // This subtask hosts one logical operator; attribute its input
            // bridges' bytes to it (bytes_received) and its output bridges'
            // bytes to it (bytes_sent).
            dag.set_all_runners_attributed_op_id(dag.runner_id_at(h1.runner_index));
            clink::LocalExecutor exec(std::move(dag), detail::make_subtask_job_config(rctx));
            detail::run_subtask_to_completion(exec, rctx.cancel_token);
        };
    runner_registry_.register_operator(op_type, in_channel, out_channel, std::move(runner));

    // Mirror into OperatorRegistry so the chain dispatcher
    // (worker.cpp) can build typed Operator<In,Out> for ops in
    // chains of length >= 2. Without this, inline-lambda ops can't be
    // chained and run as separate per-op subtask threads, paying the
    // BoundedChannel cost between every pair (parallelism scaling
    // regression).
    clink::cluster::OperatorFactory op_factory;
    op_factory.in = in_channel;
    op_factory.out = out_channel;
    op_factory.build =
        [factory](const clink::cluster::OperatorBuildContext& ctx) -> std::shared_ptr<void> {
        BuildContext pc;
        pc.params = ctx.params;
        pc.subtask_idx = ctx.subtask_idx;
        pc.parallelism = ctx.parallelism;
        auto op = factory(pc);
        return std::static_pointer_cast<void>(op);
    };
    operator_registry_.register_operator(op_type, std::move(op_factory));

    auto db = [factory, capture_codec](clink::Dag& dag,
                                       const std::vector<std::any>& upstream,
                                       const BuildContext& ctx) -> clink::cluster::DagOpHandle {
        if (upstream.size() != 1) {
            throw std::runtime_error(
                "DagBuilder<operator>: expected exactly one upstream edge, got " +
                std::to_string(upstream.size()));
        }
        if (ctx.parallelism > 1) {
            // Parallel forward operator: upstream must be parallel at
            // the same parallelism (caller / walker enforces this).
            // Hash-shuffle for keyed cross-parallelism routing is a
            // follow-up; throw if upstream is single - the walker
            // should have widened it first.
            auto up_handle = std::any_cast<clink::Dag::ParallelStageHandle<In>>(upstream[0]);
            auto sub_factory =
                [factory, base = ctx](std::size_t subtask) -> std::shared_ptr<Operator<In, Out>> {
                BuildContext local = base;
                local.subtask_idx = static_cast<std::uint32_t>(subtask);
                return factory(local);
            };
            auto ph = dag.template add_parallel_operator<In, Out>(
                up_handle, std::move(sub_factory), ctx.parallelism);
            return clink::cluster::DagOpHandle{std::any{std::move(ph)},
                                               /*runner_index=*/0,
                                               ctx.parallelism};
        }
        auto in_handle = std::any_cast<clink::StageHandle<In>>(upstream[0]);
        auto op = factory(ctx);
        // capture_codec arms the record-capture tee for this op when the
        // job names a capture_dir (the chain-dispatch path builds ops
        // through this DagBuilder, so chained subtasks capture too).
        auto h = dag.template add_operator<In, Out>(in_handle, op, capture_codec);
        return clink::cluster::DagOpHandle{std::any{h}, h.runner_index, /*parallelism=*/1};
    };
    // Bundle-scoped write so the worker chain dispatcher's per-job
    // DagBuilderRegistry (with parent fallback to the default
    // singleton) finds this DagBuilder. The .so writes to the .so's
    // default_instance below; with RTLD_LOCAL that singleton isn't
    // visible to the worker process, which is why the per-bundle path is
    // needed.
    dag_builder_registry_.register_builder(op_type, db);
    auto& so_dbr = clink::cluster::DagBuilderRegistry::default_instance();
    if (&so_dbr != &dag_builder_registry_) {
        so_dbr.register_builder(op_type, std::move(db));
    }
}

template <typename In1, typename In2, typename Out>
void PluginRegistry::register_co_operator(
    const std::string& op_type,
    std::function<std::shared_ptr<CoOperator<In1, In2, Out>>(const BuildContext&)> factory) {
    const auto in1_channel = type_registry_.channel_for_typeid(typeid(In1).name());
    const auto in2_channel = type_registry_.channel_for_typeid(typeid(In2).name());
    const auto out_channel = type_registry_.channel_for_typeid(typeid(Out).name());
    if (in1_channel.empty()) {
        throw std::runtime_error(
            "register_co_operator: In1 not registered (call register_type<In1> first)");
    }
    if (in2_channel.empty()) {
        throw std::runtime_error(
            "register_co_operator: In2 not registered (call register_type<In2> first)");
    }
    if (out_channel.empty()) {
        throw std::runtime_error(
            "register_co_operator: Out not registered (call register_type<Out> first)");
    }
    // Codecs for the two inputs, captured now from the type registration
    // that register_type<In1/In2> performed above. Threaded into
    // add_co_operator so it can serialize the unpaused input's in-flight
    // records during an unaligned checkpoint (and replay them on restore).
    std::optional<clink::Codec<In1>> in1_codec_opt;
    std::optional<clink::Codec<In2>> in2_codec_opt;
    if (auto c = this->template codec_for<In1>()) {
        in1_codec_opt = *c;
    }
    if (auto c = this->template codec_for<In2>()) {
        in2_codec_opt = *c;
    }
    clink::cluster::SubtaskRunner runner = [factory,
                                            in1_channel,
                                            in2_channel,
                                            in1_codec_opt,
                                            in2_codec_opt,
                                            &type_registry = type_registry_](
                                               const clink::cluster::RunnerContext& rctx) {
        auto op = factory(detail::build_ctx_from(rctx));
        detail::apply_chain_identity(op, rctx);
        const auto& chain = rctx.chain;
        const auto& out_channel_local =
            chain.ops.empty() ? std::string{} : chain.ops[0].out_channel;
        const auto& ops_for_out = detail::require_type_ops(type_registry, out_channel_local);

        // Partition rctx.in_bridges by channel_type from input_edges.
        // When In1 and In2 share a channel type, channel_type alone
        // can't disambiguate sides; fall back to ordinal position
        // (planner contract: first input goes to side 1, second to
        // side 2). This is the path Row/Row/Row co-ops like the SQL
        // interval-join hit, since both Row sides share the "row"
        // channel.
        std::vector<std::shared_ptr<void>> in1_bridges;
        std::vector<std::shared_ptr<void>> in2_bridges;
        const std::size_t n_inputs = std::min(chain.input_edges.size(), rctx.in_bridges.size());
        if (in1_channel == in2_channel) {
            // Same-type In1/In2 (e.g. SQL Row,Row equi/interval joins): channel
            // type can't tell the sides apart, so split by the planner's per-edge
            // input_index (0 = In1/first input, 1 = In2/second). This handles
            // parallelism > 1, where each side contributes one input bridge per
            // upstream subtask (so the co-op may have many bridges per side, not
            // the two-edge par=1 case the old code hard-assumed).
            for (std::size_t i = 0; i < n_inputs; ++i) {
                const auto idx = chain.input_edges[i].input_index;
                if (idx == 0) {
                    in1_bridges.push_back(rctx.in_bridges[i]);
                } else if (idx == 1) {
                    in2_bridges.push_back(rctx.in_bridges[i]);
                } else {
                    throw std::runtime_error(
                        "co_operator runner: same-type input_index out of range (expected 0/1)");
                }
            }
        } else {
            for (std::size_t i = 0; i < n_inputs; ++i) {
                const auto& ct = chain.input_edges[i].channel_type;
                if (ct == in1_channel) {
                    in1_bridges.push_back(rctx.in_bridges[i]);
                } else if (ct == in2_channel) {
                    in2_bridges.push_back(rctx.in_bridges[i]);
                } else {
                    throw std::runtime_error("co_operator runner: input edge channel '" + ct +
                                             "' matches neither In1 (" + in1_channel +
                                             ") nor In2 (" + in2_channel + ")");
                }
            }
        }
        if (in1_bridges.empty() || in2_bridges.empty()) {
            throw std::runtime_error(
                "co_operator runner: both In1 and In2 must have at least one input edge");
        }

        clink::Dag dag;
        auto h1 = clink::cluster::build_typed_input_stage<In1>(dag, in1_bridges);
        auto h2 = clink::cluster::build_typed_input_stage<In2>(dag, in2_bridges);
        auto h_out =
            dag.template add_co_operator<In1, In2, Out>(h1, h2, op, in1_codec_opt, in2_codec_opt);
        clink::cluster::attach_typed_output_groups<Out>(
            dag,
            h_out,
            clink::cluster::main_output_groups_of(rctx.output_groups),
            ops_for_out,
            chain.output_routing,
            chain.output_selector_fn);
        clink::cluster::attach_side_output_groups(dag, h_out.runner_index, rctx.output_groups);
        // This subtask hosts one logical (co-)operator; attribute both input
        // bridges' bytes (bytes_received) and output bridges' bytes (bytes_sent)
        // to it.
        dag.set_all_runners_attributed_op_id(dag.runner_id_at(h_out.runner_index));
        clink::LocalExecutor exec(std::move(dag), detail::make_subtask_job_config(rctx));
        detail::run_subtask_to_completion(exec, rctx.cancel_token);
    };
    runner_registry_.register_co_operator(
        op_type, in1_channel, in2_channel, out_channel, std::move(runner));

    clink::cluster::DagBuilderRegistry::default_instance().register_builder(
        op_type,
        [factory, in1_codec_opt, in2_codec_opt](
            clink::Dag& dag,
            const std::vector<std::any>& upstream,
            const BuildContext& ctx) -> clink::cluster::DagOpHandle {
            if (upstream.size() != 2) {
                throw std::runtime_error(
                    "DagBuilder<co_operator>: expected exactly two upstream edges, got " +
                    std::to_string(upstream.size()));
            }
            // Inputs preserve the order of the OperatorSpec's `inputs`
            // array, which the fluent builder appends in the order the
            // user passed `.connect<T2, Out>(other, ...)` - so position
            // 0 is In1 and position 1 is In2.
            auto h1 = std::any_cast<clink::StageHandle<In1>>(upstream[0]);
            auto h2 = std::any_cast<clink::StageHandle<In2>>(upstream[1]);
            auto op = factory(ctx);
            auto h_out = dag.template add_co_operator<In1, In2, Out>(
                h1, h2, op, in1_codec_opt, in2_codec_opt);
            return clink::cluster::DagOpHandle{std::any{h_out}, h_out.runner_index};
        });
}

template <typename K, typename In, typename Out>
void PluginRegistry::register_keyed_operator(
    const std::string& op_type,
    std::function<std::shared_ptr<KeyedProcessFunction<K, In, Out>>(const BuildContext&)>
        fn_factory,
    std::function<K(const In&)> key_fn,
    std::function<K(const std::string&)> timer_key_fn) {
    // Wrap the user's KeyedProcessFunction factory in an Operator<In, Out>
    // factory that builds the standard KeyedProcessFunctionAdapter<K, In, Out>
    // around it. From the cluster's point of view this is just a typed
    // mid-chain operator; the adapter's pre-dispatch hooks update the
    // function's current_key before each invocation.
    register_operator<In, Out>(
        op_type,
        [fn_factory, key_fn, timer_key_fn, op_type](
            const BuildContext& ctx) -> std::shared_ptr<Operator<In, Out>> {
            auto fn = fn_factory(ctx);
            return std::make_shared<::clink::detail::KeyedProcessFunctionAdapter<K, In, Out>>(
                fn, key_fn, timer_key_fn, op_type);
        });
}

template <typename K, typename In1, typename In2, typename Out>
void PluginRegistry::register_keyed_co_operator(
    const std::string& op_type,
    std::function<std::shared_ptr<KeyedCoProcessFunction<K, In1, In2, Out>>(const BuildContext&)>
        fn_factory,
    std::function<K(const In1&)> key1,
    std::function<K(const In2&)> key2,
    std::function<K(const std::string&)> timer_key_fn) {
    register_co_operator<In1, In2, Out>(
        op_type,
        [fn_factory, key1, key2, timer_key_fn, op_type](
            const BuildContext& ctx) -> std::shared_ptr<CoOperator<In1, In2, Out>> {
            auto fn = fn_factory(ctx);
            return std::make_shared<
                ::clink::detail::KeyedCoProcessFunctionAdapter<K, In1, In2, Out>>(
                fn, key1, key2, timer_key_fn, op_type);
        });
}

template <typename K, typename In1, typename In2, typename Out>
void PluginRegistry::register_async_keyed_co_operator(
    const std::string& op_type,
    std::function<std::shared_ptr<AsyncKeyedCoProcessFunction<K, In1, In2, Out>>(
        const BuildContext&)> fn_factory,
    std::function<K(const In1&)> key1,
    std::function<K(const In2&)> key2,
    Codec<K> key_codec) {
    register_co_operator<In1, In2, Out>(
        op_type,
        [fn_factory, key1, key2, key_codec, op_type](
            const BuildContext& ctx) -> std::shared_ptr<CoOperator<In1, In2, Out>> {
            auto fn = fn_factory(ctx);
            return std::make_shared<
                ::clink::detail::AsyncKeyedCoProcessFunctionAdapter<K, In1, In2, Out>>(
                fn, key1, key2, key_codec, op_type);
        });
}

template <typename T>
void PluginRegistry::register_key_extractor(const std::string& name,
                                            std::function<std::int64_t(const T&)> fn) {
    const auto channel = type_registry_.channel_for_typeid(typeid(T).name());
    if (channel.empty()) {
        throw std::runtime_error(
            "register_key_extractor<T>: T not registered (call register_type<T> first)");
    }
    key_extractor_registry_.register_extractor<T>(channel, name, fn);
    // Mirror into the .so's local default-instance - see register_type<T>
    // for the rationale (RTLD_LOCAL + static clink_core means inline
    // helper templates in runner_helpers.hpp see the .so's private static,
    // not the host's).
    auto& so_ker = clink::cluster::KeyExtractorRegistry::default_instance();
    if (&so_ker != &key_extractor_registry_) {
        so_ker.register_extractor<T>(channel, name, std::move(fn));
    }
}

template <typename T>
void PluginRegistry::register_columnar_key_extractor(
    const std::string& name,
    std::function<std::optional<std::vector<std::int64_t>>(const Batch<T>&)> fn) {
    const auto channel = type_registry_.channel_for_typeid(typeid(T).name());
    if (channel.empty()) {
        throw std::runtime_error(
            "register_columnar_key_extractor<T>: T not registered (call register_type<T> first)");
    }
    key_extractor_registry_.register_columnar_extractor<T>(channel, name, fn);
    // Same RTLD_LOCAL mirror as register_key_extractor above.
    auto& so_ker = clink::cluster::KeyExtractorRegistry::default_instance();
    if (&so_ker != &key_extractor_registry_) {
        so_ker.register_columnar_extractor<T>(channel, name, std::move(fn));
    }
}

template <typename T>
void PluginRegistry::register_selector(const std::string& name, std::function<int(const T&)> fn) {
    auto& so_sr = clink::cluster::SelectorRegistry::default_instance();
    if constexpr (std::is_same_v<T, std::int64_t>) {
        selector_registry_.register_int64(name, fn);
        if (&so_sr != &selector_registry_) {
            so_sr.register_int64(name, std::move(fn));
        }
    } else if constexpr (std::is_same_v<T, std::string>) {
        selector_registry_.register_string(name, fn);
        if (&so_sr != &selector_registry_) {
            so_sr.register_string(name, std::move(fn));
        }
    } else {
        static_assert(sizeof(T) == 0,
                      "register_selector<T>: only int64_t and std::string supported in v1");
    }
}

}  // namespace clink::plugin

// ============================================================================
// Public macros (the only way to declare a plugin)
// ============================================================================

#if defined(__APPLE__) && defined(__aarch64__)
#define CLINK_PLUGIN_TARGET_TRIPLE "darwin-arm64"
#elif defined(__linux__) && (defined(__x86_64__) || defined(_M_X64))
#define CLINK_PLUGIN_TARGET_TRIPLE "linux-x86_64"
#elif defined(__linux__) && (defined(__aarch64__) || defined(_M_ARM64))
#define CLINK_PLUGIN_TARGET_TRIPLE "linux-arm64"
#else
#define CLINK_PLUGIN_TARGET_TRIPLE "unknown"
#endif

// CLINK_DECLARE_PLUGIN(name, version, description)
//
// Emits the three extern "C" handshake getters the loader uses to
// decide whether to load this plugin. Place once at file scope. All
// strings must be literals.
#define CLINK_DECLARE_PLUGIN(plugin_name, plugin_version, plugin_description)   \
    extern "C" const char* clink_plugin_abi_fingerprint() {                     \
        return ::clink::plugin::kAbiFingerprint;                                \
    }                                                                           \
    extern "C" int clink_plugin_abi_version() {                                 \
        return ::clink::plugin::kAbiVersion;                                    \
    }                                                                           \
    extern "C" const char* clink_plugin_abi_hash() {                            \
        return ::clink::plugin::kAbiHash;                                       \
    }                                                                           \
    extern "C" const char* clink_plugin_target_triple() {                       \
        return CLINK_PLUGIN_TARGET_TRIPLE;                                      \
    }                                                                           \
    extern "C" const ::clink::plugin::PluginMetadata* clink_plugin_metadata() { \
        static const ::clink::plugin::PluginMetadata m{                         \
            (plugin_name), (plugin_version), (plugin_description), nullptr};    \
        return &m;                                                              \
    }                                                                           \
    static_assert(sizeof(::clink::plugin::PluginMetadata) > 0, "plugin header present")

// CLINK_REGISTER_PLUGIN(user_register_function)
//
// Wraps the user-supplied register function into the C-ABI entry the
// loader calls. Catches exceptions, fills a caller-supplied error
// buffer, and returns 0/non-zero. Only POD across the boundary.
#define CLINK_REGISTER_PLUGIN(user_register_fn)                                                 \
    extern "C" int clink_plugin_register(                                                       \
        void* registry_ptr, char* err_buf, ::std::size_t err_buf_size) {                        \
        try {                                                                                   \
            auto* reg = static_cast<::clink::plugin::PluginRegistry*>(registry_ptr);            \
            (user_register_fn)(*reg);                                                           \
            return 0;                                                                           \
        } catch (const ::std::exception& e) {                                                   \
            if (err_buf != nullptr && err_buf_size > 0) {                                       \
                const auto msg = ::std::string{"plugin register threw: "} + e.what();           \
                const auto n = ::std::min(msg.size(), err_buf_size - 1);                        \
                ::std::memcpy(err_buf, msg.data(), n);                                          \
                err_buf[n] = '\0';                                                              \
            }                                                                                   \
            return 1;                                                                           \
        } catch (...) {                                                                         \
            if (err_buf != nullptr && err_buf_size > 0) {                                       \
                const char* m = "plugin register threw an unknown exception";                   \
                ::std::strncpy(err_buf, m, err_buf_size - 1);                                   \
                err_buf[err_buf_size - 1] = '\0';                                               \
            }                                                                                   \
            return 2;                                                                           \
        }                                                                                       \
    }                                                                                           \
    static_assert(::std::is_same_v<decltype(&(user_register_fn)), ::clink::plugin::RegisterFn>, \
                  "CLINK_REGISTER_PLUGIN argument must have signature "                         \
                  "void(clink::plugin::PluginRegistry&)")
