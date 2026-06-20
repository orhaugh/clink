#pragma once

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/core/codec.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/core/types.hpp"
#include "clink/metrics/counter.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/async_execution_controller.hpp"
#ifdef CLINK_HAS_ARROW
#include "clink/runtime/blocking_exchange.hpp"
#endif
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/multi_input_alignment.hpp"
#include "clink/runtime/output_tag.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/runtime/sharded_keyed_stage.hpp"
#include "clink/runtime/snapshot_worker.hpp"
#include "clink/runtime/subtask_emitter.hpp"
#include "clink/state/keyed_state.hpp"

namespace clink {

// A DAG node is one of: Source, Operator, Sink. We capture them via type-erased
// runners; each runner knows how to drive its own operator from input channels
// and into an emitter.
//
// The DAG owns:
//   - the operators themselves (via shared_ptr)
//   - the inter-operator channels (also via shared_ptr)
//   - the runner closures (used by LocalExecutor)
//
// Topology is linear in this MVP: source -> op -> op -> ... -> sink. Branching
// and joins are deliberate next steps.

class Dag;

namespace detail {

// Type-erased runner: a function that, given a runtime context and a "stop"
// predicate, drives one operator until upstream is closed (or stop becomes
// true). The runner attaches the context to its operator before open().
struct OperatorRunner {
    std::string name;
    OperatorId id;
    std::function<void(RuntimeContext& ctx, const std::function<bool()>& should_stop)> run;
    std::function<void()> cancel;
    // Channel introspection for metrics. nullopt for sources/sinks where it
    // doesn't apply.
    std::function<std::size_t()> input_depth;
    std::function<std::size_t()> input_capacity;
    std::function<std::size_t()> input_high_water;
};

}  // namespace detail

// SourceHandle, OpHandle, and SinkHandle are tag types returned by the
// builder. They carry the output channel of the previous stage so the next
// stage can wire its input.
template <typename T>
struct StageHandle {
    using Element = StreamElement<T>;
    using Channel = BoundedChannel<Element>;

    std::shared_ptr<Channel> output;  // nullptr for Sink stages
    std::size_t runner_index{0};
};

// Tuning knobs for Dag::iterate_stream. Defaults preserve the original
// semantics - no max-records cap. Lives at namespace scope rather
// than nested inside Dag so default-initialized member values are
// visible when iterate_stream's default arg is parsed.
struct IterationConfig {
    // Hard cap on records flowing through the iteration head (records,
    // watermarks, barriers all counted). Once exceeded, the head
    // closes merged and the close cascade unwinds the loop. 0 = no
    // cap, the historic shape.
    std::uint64_t max_records{0};

    // Quiescence threshold: after the external input has closed, how
    // many consecutive idle polls (~1ms each) the head waits with
    // both channels empty before deciding the loop is done. Default
    // 100 ≈ 100ms grace, which is plenty for any reasonable in-process
    // body operator and well under human-perceptible latency.
    //
    // Bump this for tests running under sanitizer instrumentation
    // (ASan / TSan slow records-per-second by 5–10×) so feedback
    // records have time to flow before the head gives up. The
    // production default is fine for normal builds.
    int idle_threshold{100};
};

class Dag {
public:
    explicit Dag(std::size_t default_channel_capacity = 1024)
        : default_channel_capacity_(default_channel_capacity) {}

    std::size_t default_channel_capacity() const noexcept { return default_channel_capacity_; }
    void set_default_channel_capacity(std::size_t cap) noexcept { default_channel_capacity_ = cap; }

    // ---- Side outputs ---------------------------------------------------
    //
    // OutputTag-based side outputs (`ctx.output(tag, value)`).
    // Operators emit to a tagged channel via runtime()->side_output<T>(tag).
    //
    // Wiring is a two-step pattern:
    //
    //     auto h = dag.add_operator<In, Out>(upstream, op);
    //     auto side_h = dag.side_output<ErrorT>(h, OutputTag<ErrorT>("errors"));
    //     dag.add_sink<ErrorT>(side_h, error_sink);
    //
    // The side handle behaves like any other StageHandle - feed it into
    // add_operator/add_sink/etc. Tags are scoped per producer stage; the
    // same tag id can re-appear on different stages independently.
    template <typename T, typename ProducerOut>
    StageHandle<T> side_output(StageHandle<ProducerOut> producer, OutputTag<T> tag) {
        return side_output_by_index<T>(producer.runner_index, std::move(tag));
    }

    // Same wiring as side_output<T, ProducerOut>, but addressed directly
    // by the parent operator's runner_index. Used by the cluster's
    // plugin runner when it has the parent's index (not a typed handle)
    // and needs to set up a side channel for a runtime-determined T.
    template <typename T>
    StageHandle<T> side_output_by_index(std::size_t runner_index, OutputTag<T> tag) {
        if (runner_index >= runners_.size()) {
            throw std::runtime_error("Dag::side_output_by_index: runner_index out of range");
        }
        auto& map = side_channels_(runner_index);
        if (map.find(tag.id) != map.end()) {
            throw std::runtime_error("Dag::side_output_by_index: tag '" + tag.id +
                                     "' already registered on this stage");
        }
        auto channel =
            std::make_shared<BoundedChannel<StreamElement<T>>>(default_channel_capacity_);
        SideOutputChannelEntry entry;
        entry.channel = std::static_pointer_cast<void>(channel);
        entry.close_fn = [channel] { channel->close(); };
        map.emplace(tag.id, std::move(entry));
        return StageHandle<T>{channel, runner_index};
    }

    // Access (read-only) the side output channels registered for one
    // runner. Used by LocalExecutor to populate each operator's
    // RuntimeContext. Returns an empty map if the runner has none.
    const SideOutputChannelMap& side_channels_for(std::size_t runner_index) const {
        static const SideOutputChannelMap kEmpty;
        if (runner_index >= side_channels_by_runner_.size()) {
            return kEmpty;
        }
        const auto& p = side_channels_by_runner_[runner_index];
        return p ? *p : kEmpty;
    }

    // ---- Source ----------------------------------------------------------
    template <typename T>
    StageHandle<T> add_source(std::shared_ptr<Source<T>> source) {
        auto channel =
            std::make_shared<BoundedChannel<StreamElement<T>>>(default_channel_capacity_);
        const OperatorId id = derive_id_with_uid_(*source);
        source->set_id(id);
        sources_.push_back(source);
        source_ids_.push_back(id);
        source_bounded_.push_back(source->is_bounded() ? 1 : 0);
        source_split_counts_.push_back(source->split_count());
        // Erased injector: hands a barrier to the source's internal
        // pending queue. The source runner loop (below) drains the
        // queue between produce() calls, calling source->snapshot_
        // offset() before emitting each barrier so the source's offset
        // and the barrier reach durability atomically with respect to
        // the record stream. Acks fire from the runner loop too, after
        // both the snapshot AND the downstream-bound barrier push have
        // happened - that's the "the source has finished its slice of
        // checkpoint K" signal the JM expects.
        auto ack_ref = std::make_shared<std::function<void(CheckpointId, bool, std::string)>>();
        source_injectors_.push_back(
            [source, ack_ref](CheckpointBarrier b) { source->inject_pending_barrier(b); });

        detail::OperatorRunner runner;
        runner.name = source->name();
        runner.id = id;
        runner.run = [source, channel, ack_ref, id](RuntimeContext& ctx,
                                                    const std::function<bool()>& should_stop) {
            source->attach_runtime(&ctx);
            // Wire the source's ack callback so the runner can ack
            // once the barrier is emitted. The ctx outlives the source
            // because run() is the only path that uses it.
            if (const auto& cb = ctx.checkpoint_ack(); cb) {
                *ack_ref = cb;
            }
            // Source-side restore: if a state backend is present and
            // the source persisted an offset during a prior run, load
            // it back BEFORE open() so the source can decide what to
            // do with it. Legacy sources whose restore_offset returns
            // false (the default) just behave as before - they start
            // from scratch.
            if (ctx.has_state_backend()) {
                source->restore_offset(*ctx.state_backend(), id);
            }
            source->open();
            Emitter<T> emitter(channel.get());
            emitter.set_operator_id(id.value());
            // Phase 26a: stamp the job-global checkpoint mode onto
            // every barrier the source emits. Sources stay
            // mode-agnostic; the runner translates
            // JobConfig.unaligned_checkpoints into the wire stamp.
            // Downstream operators read the stamp off the barrier
            // (not off RuntimeContext) when deciding aligned vs
            // unaligned semantics for that checkpoint.
            const auto source_barrier_mode = ctx.barrier_mode_override().value_or(
                ctx.unaligned_checkpoints() ? CheckpointBarrier::Mode::Unaligned
                                            : CheckpointBarrier::Mode::Aligned);
            emitter.set_default_barrier_mode(source_barrier_mode);
            // Drain any barriers that arrived while produce() wasn't
            // running. Each barrier triggers a snapshot of the source's
            // current offset, then the barrier emits to downstream, then
            // we ack. produce() running on this same thread means
            // there's no race between record emission and offset capture.
            auto drain_pending_barriers = [&]() {
                while (auto b = source->take_pending_barrier()) {
                    if (ctx.has_state_backend()) {
                        source->snapshot_offset(*ctx.state_backend(), id, b->id());
                    }
                    emitter.emit_barrier(*b);
                    if (*ack_ref) {
                        (*ack_ref)(b->id(), true, std::string{});
                    }
                }
            };
            drain_pending_barriers();
            while (!should_stop() && source->produce(emitter)) {
                drain_pending_barriers();
            }
            drain_pending_barriers();
            if (!should_stop()) {
                source->flush(emitter);
                // BATCH-1 end-of-input drain: a bounded source that exhausted
                // cleanly has reached genuine end-of-input, so advance event
                // time to its maximum. This fires every downstream event-time
                // window and timer over the records just emitted (and any
                // flushed tail) before the terminal barrier closes the stream.
                // Gated on is_bounded() and the clean exit above (should_stop()
                // false), so cancelling a streaming job never spuriously fires
                // all its windows. Independent of the state backend: windows
                // fire on watermarks whether or not checkpointing is on. Sources
                // that already self-emit max (VectorSource, finite generators)
                // get a harmless monotone-idempotent duplicate.
                if (source->is_bounded()) {
                    emitter.emit_watermark(Watermark::max());
                }
                // Terminal barrier: signals to downstream sinks that
                // the stream has ended, so any 2PC sink can pre-commit
                // + commit the tail records that didn't fall inside a
                // JM-coordinated checkpoint. Skipped for relay sources
                // (NetworkBridgeSource): they only forward whatever
                // arrived from upstream, and the upstream's terminal
                // is already in the relayed stream. Adding a second
                // would make 2PC sinks commit twice and overwrite the
                // first commit. Sentinel is max uint64 so it can't
                // collide with a real ckpt id from periodic
                // checkpointing.
                if (ctx.has_state_backend() && source->emit_terminal_barrier_on_exit()) {
                    channel->push(StreamElement<T>::barrier(
                        CheckpointBarrier{CheckpointId{std::numeric_limits<std::uint64_t>::max()},
                                          /*terminal=*/true,
                                          source_barrier_mode}));
                }
            }
            source->close();
            source->attach_runtime(nullptr);
            channel->close();
        };
        runner.cancel = [source, channel] {
            source->cancel();
            channel->close();
        };
        runner.input_depth = [] { return std::size_t{0}; };
        runner.input_capacity = [] { return std::size_t{0}; };
        runner.input_high_water = [] { return std::size_t{0}; };

        runners_.push_back(std::move(runner));
        return StageHandle<T>{channel, runners_.size() - 1};
    }

    // ---- Operator (single-input single-output) --------------------------
    template <typename In, typename Out>
    StageHandle<Out> add_operator(StageHandle<In> upstream, std::shared_ptr<Operator<In, Out>> op) {
        auto out_channel =
            std::make_shared<BoundedChannel<StreamElement<Out>>>(default_channel_capacity_);
        const OperatorId id = derive_id_with_uid_(*op);
        op->set_id(id);

        auto in_channel = upstream.output;

        detail::OperatorRunner runner;
        runner.name = op->name();
        runner.id = id;
        // Capture this runner's side-channel map by shared_ptr so any
        // Dag::side_output() calls made between add_operator and run()
        // are observed by the closure at runtime.
        auto side_channels = side_channels_ptr_(runners_.size());
        runner.run = [op, in_channel, out_channel, side_channels, id](
                         RuntimeContext& ctx, const std::function<bool()>& should_stop) {
            using namespace std::chrono_literals;
            op->attach_runtime(&ctx);
            // Reload any checkpointed timers (same-parallelism restore)
            // before open() so user open() sees the restored timer set.
            if (ctx.has_state_backend()) {
                op->restore_timers(*ctx.state_backend(), id);
            }
            op->open();
            // If the state backend supports the async checkpoint split,
            // spin up a per-subtask snapshot worker so the durable write
            // lands off this thread. Idle-cheap (one blocked thread) when
            // no checkpoints flow. Backends with no durable write to defer
            // (InMemory, in-RAM changelog, RocksDB) never construct one;
            // FileBacked and disk-backed changelog do.
            std::unique_ptr<SnapshotWorker> snap_worker;
            if (ctx.has_state_backend() && ctx.state_backend()->supports_async_persist()) {
                snap_worker = std::make_unique<SnapshotWorker>();
                snap_worker->start();
            }
            Emitter<Out> emitter(out_channel.get());
            emitter.set_operator_id(id.value());
            // Phase 26b: if this operator carries a per-operator mode
            // override, stamp every emitted barrier with it. Source
            // operators have already stamped from JobConfig in their
            // dedicated runner; for regular operators the override
            // applies on the way out so downstream sees the operator's
            // local policy decision.
            if (auto override = ctx.barrier_mode_override(); override) {
                emitter.set_default_barrier_mode(*override);
            }
            // Async (non-blocking state) execution path (ASYNC-6). Enabled
            // only when BOTH the operator opts in AND the state backend can
            // genuinely defer reads; every other operator stays on the inline
            // synchronous path below, byte-for-byte unchanged. The controller
            // is per-subtask and lives on this runner thread. Note on
            // unaligned checkpoints: an async-state single-input operator
            // FORCE-ALIGNS - it drains to quiescence before capture (below)
            // regardless of the barrier's mode, because drain-to-quiescence is
            // incompatible with unaligned in-flight capture. For a single
            // input this is lossless (there is no cross-input alignment to
            // skip); multi-input async (co-operator) is deferred.
            std::unique_ptr<AsyncExecutionController> aec;
            const bool async_mode = op->supports_async() && ctx.has_state_backend() &&
                                    ctx.state_backend()->supports_async_get();
            if (async_mode) {
                // Tripwire (async-timer gate): under async, the event-time timer
                // path is epoch-gated (watermarks route through aec->on_watermark
                // below, firing on_event_time_timer only after the epoch drains),
                // but the steady-state processing-time fire_due() at the top of
                // the loop runs UNGATED - a processing-time timer callback that
                // touches keyed state could race an in-flight async read for the
                // same key. No production operator is async + timer-bearing today,
                // so this never trips; if one is introduced the per-key-gated
                // timer path must be built first (deferred increment).
                //
                // Only PROCESSING-TIME state-touching timers are unsafe: they
                // fire via fire_due() at the loop top, ungated, and can race an
                // in-flight async read. Event-time timers fire inside the
                // aec->on_watermark release closure below (after the epoch
                // drains), so an event-time-only operator (e.g. an async window
                // aggregate) is admitted - it returns false here while keeping
                // fires_state_touching_timers() true.
                if (op->fires_state_touching_processing_time_timers()) {
                    throw std::logic_error(
                        "clink: operator '" + op->name() +
                        "' fires state-touching processing-time timers under async execution, "
                        "which is not yet supported (the callbacks fire ungated at the loop top "
                        "and can race an in-flight async read for the same key); build the "
                        "per-key-gated timer path before enabling async here");
                }
                aec = std::make_unique<AsyncExecutionController>();
                // Wire the backend's async-read completions to THIS subtask's
                // controller: a deferring backend (RemoteReadBackend, a future
                // ForSt backend) posts a suspended handle here and the
                // controller resumes it on this runner thread. Without this a
                // deferring get_async has nowhere to hand the completion and
                // falls back to an inline blocking load - this is the missing
                // production link for the disaggregated async path. Cleared at
                // teardown (below) before `aec` is destroyed.
                ctx.state_backend()->set_async_resume_scheduler(
                    [aec_ptr = aec.get()](std::coroutine_handle<> h) {
                        aec_ptr->schedule_resume(h);
                    });
            }
            auto* timers = ctx.timer_service();
            // Drive timers through the op-level virtuals so chained
            // operators can fan out polling/firing to their inner ops
            // (each of which has its own per-RC TimerService). Regular
            // ops' default impl just polls ctx.timer_service().
            auto fire_due = [&] { op->fire_due_timers(emitter, timers->now_ms()); };
            while (!should_stop()) {
                // Fire any timers whose deadline has passed before
                // waiting on input.
                fire_due();
                // Wait until the next timer is due, or 30s otherwise.
                // Cancellation propagates via channel close (set on
                // the runner's cancel hook and by the executor's
                // external-cancel watcher), which wakes pop_for
                // immediately - the 30s fallback is only a slow
                // heartbeat so a fully-idle runner with no timers
                // still notices should_stop() changing through paths
                // that bypass channel close (none today). For chained
                // ops the next-deadline reflects the earliest across
                // all inner op TimerServices.
                std::chrono::milliseconds timeout = 30s;
                if (auto next = op->next_timer_deadline_ms(); next.has_value()) {
                    const auto delay = *next - timers->now_ms();
                    timeout = delay > 0 ? std::chrono::milliseconds(delay) : 0ms;
                }
                auto maybe = in_channel->pop_for(timeout);
                if (maybe.has_value()) {
                    if (maybe->is_data()) {
                        clink::metrics::op::records_in_inc(id.value(), maybe->as_data().size());
                    }
                    if (maybe->is_barrier() && ctx.has_state_backend()) {
                        // Snapshot the operator's state slice as the
                        // barrier passes through. The user's process()
                        // is still called so the barrier flows
                        // downstream and any user-side on_barrier hook
                        // runs.
                        // Async-state operators force-align: drain all
                        // in-flight async work to quiescence so the captured
                        // cut reflects every record admitted before this
                        // barrier (no torn state). No-op when not in async mode.
                        if (aec) {
                            aec->drain_for_barrier();
                        }
                        const auto ckpt_id = maybe->as_barrier().id();
                        auto* backend = ctx.state_backend();
                        if (snap_worker) {
                            // Async path: capture a detached point-in-time
                            // blob on this thread, forward the barrier
                            // downstream immediately (the blob already
                            // reflects state at the barrier point, so this
                            // is sound and shaves the durable-write latency
                            // off the critical path), then let the worker
                            // durably persist + ack off-thread. The ack
                            // fires only after persist() returns, so the
                            // ack-after-durable invariant holds.
                            std::string err;
                            bool ok = true;
                            CaptureHandle handle;
                            try {
                                op->snapshot_timers(*backend, id);
                                handle = backend->capture(ckpt_id);
                            } catch (const std::exception& e) {
                                ok = false;
                                err = e.what();
                            }
                            op->process(*maybe, emitter);
                            if (!ok) {
                                // Capture failed on-thread: ack the failure
                                // now; there is nothing for the worker to do.
                                if (const auto& cb = ctx.checkpoint_ack(); cb) {
                                    cb(ckpt_id, false, std::move(err));
                                }
                            } else {
                                snap_worker->enqueue(
                                    SnapshotWorker::Job{.handle = std::move(handle),
                                                        .backend = backend,
                                                        .ack = ctx.checkpoint_ack()});
                            }
                        } else {
                            // Synchronous path (InMemory / Changelog /
                            // RocksDB): snapshot then ack on this thread.
                            std::string err;
                            bool ok = true;
                            try {
                                op->snapshot_timers(*backend, id);
                                backend->snapshot(ckpt_id);
                            } catch (const std::exception& e) {
                                ok = false;
                                err = e.what();
                            }
                            op->process(*maybe, emitter);
                            if (const auto& cb = ctx.checkpoint_ack(); cb) {
                                cb(ckpt_id, ok, std::move(err));
                            }
                        }
                    } else if (aec && maybe->is_watermark()) {
                        // Async mode: the watermark forwards (and fires its due
                        // event-time timers) only after its epoch has drained,
                        // so it never overtakes a record that arrived before it.
                        const Watermark wm = maybe->as_watermark();
                        aec->on_watermark([op, wm, &emitter] { op->on_watermark(wm, emitter); });
                        aec->poll();
                    } else if (aec && maybe->is_data()) {
                        // Async mode: the operator submits one coroutine per
                        // record to the controller; poll services any that
                        // completed inline (a non-deferring backend) this turn.
                        op->process_async(*maybe, emitter, *aec);
                        aec->poll();
                    } else if (op->supports_columnar() && maybe->is_data() &&
                               maybe->as_data().is_columnar() &&
                               op->process_columnar(*maybe, emitter)) {
                        // Columnar-native fast path: the operator consumed the
                        // Arrow sidecar directly (vectorized), no row decode.
                        // A false return falls through to the row path below.
                    } else {
                        op->process(*maybe, emitter);
                    }
                } else if (in_channel->closed()) {
                    break;
                }
            }
            // Wind down the async snapshot worker. On a clean end-of-stream
            // (not cancelled) drain the backlog so any in-flight checkpoint
            // the coordinator is waiting on still persists + acks; on a
            // cancel drop queued captures without acking (the job is being
            // torn down and an un-ack'd checkpoint is simply never
            // completed). Done before out_channel->close() so the worker is
            // fully joined while the runtime context is still alive.
            if (snap_worker) {
                if (should_stop()) {
                    snap_worker->cancel_and_join();
                } else {
                    snap_worker->drain_and_join();
                }
            }
            // Drain any timers still due at shutdown so close-time
            // emissions aren't silently dropped.
            if (!should_stop()) {
                if (aec) {
                    aec->drain();  // finish any in-flight async state work before flush
                }
                fire_due();
                op->flush(emitter);
            }
            // Drop the controller pointer from the backend before `aec` is
            // destroyed at the end of this closure (all async work has been
            // drained above), so a late IO completion can never call into a
            // dangling controller.
            if (async_mode && ctx.has_state_backend()) {
                ctx.state_backend()->set_async_resume_scheduler({});
            }
            op->close();
            op->attach_runtime(nullptr);
            out_channel->close();
            // Close any side output channels so their consumers drain.
            if (side_channels) {
                for (auto& [_, entry] : *side_channels) {
                    if (entry.close_fn) {
                        entry.close_fn();
                    }
                }
            }
        };
        runner.cancel = [in_channel, out_channel] {
            in_channel->close();
            out_channel->close();
        };
        runner.input_depth = [in_channel] { return in_channel->size(); };
        runner.input_capacity = [in_channel] { return in_channel->capacity(); };
        runner.input_high_water = [in_channel] { return in_channel->high_water_mark(); };

        runners_.push_back(std::move(runner));
        operators_.push_back(op);
        return StageHandle<Out>{out_channel, runners_.size() - 1};
    }

#ifdef CLINK_HAS_ARROW
    // ---- Blocking exchange (BATCH-2) ------------------------------------
    //
    // Insert a blocking-edge stage boundary on `upstream`: the producing side is
    // fully materialised (with Arrow-IPC spill to `opts.spill_dir` over
    // `opts.spill_threshold_bytes`) before any record crosses to the consumer.
    // Wires a BlockingExchangeOperator<T> via add_operator and records the
    // runner index as a blocking boundary so a batch scheduler (BATCH-3) can
    // launch the consumer stage only after this producer completes. Returns the
    // downstream handle exactly like add_operator.
    template <typename T>
    StageHandle<T> add_blocking_exchange(StageHandle<T> upstream,
                                         ArrowBatcher<T> batcher,
                                         BlockingExchangeOptions opts = {},
                                         std::string name = "blocking_exchange") {
        auto op = std::make_shared<BlockingExchangeOperator<T>>(
            std::move(batcher), std::move(opts), std::move(name));
        auto handle = add_operator<T, T>(upstream, op);
        blocking_edge_indices_.push_back(handle.runner_index);
        return handle;
    }
#endif

    // ---- Sharded keyed stage (share-nothing parallel keyed operator) -----
    //
    // Fans ONE keyed operator across `num_shards` worker threads, each owning a
    // private state shard, with records routed by key group (see
    // ShardedKeyedStage). This single DAG runner is the coordinator: it pops the
    // upstream channel and drives the stage - data -> submit, watermark ->
    // coordinated min-merge, in-band barrier -> coordinated checkpoint (merged
    // snapshot + ack), end-of-stream -> drain + close.
    //
    // `uid` gives the stage a stable OperatorId so its keyed state restores
    // across runs. `restore_from` (if its bytes are non-empty) is loaded into the
    // shards before the workers start. `on_checkpoint` is the durable-persist
    // seam: it is handed each checkpoint's merged result and RETURNS whether the
    // bytes were durably stored; the runner acks with result.ok && persisted, so
    // a wired store (ShardedCheckpointStore) makes the ack ack-after-durable. A
    // null hook keeps the pure in-process path (persisted=true, snapshot left in
    // RAM, ack on merge - matching the memory:// no-cross-process-restore
    // contract). Pair it with ShardedCheckpointStore + restore_from for durable
    // cross-process / rescale restore.
    template <typename In, typename Out>
    StageHandle<Out> add_sharded_keyed(
        StageHandle<In> upstream,
        std::size_t num_shards,
        typename ShardedKeyedStage<In, Out>::OperatorFactory factory,
        KeyBytesOf<In> key_bytes_of,
        std::string uid = {},
        Snapshot restore_from = {},
        std::function<bool(const typename ShardedKeyedStage<In, Out>::CheckpointResult&)>
            on_checkpoint = {}) {
        auto out_channel =
            std::make_shared<BoundedChannel<StreamElement<Out>>>(default_channel_capacity_);
        OperatorId id;
        if (!uid.empty()) {
            if (assigned_uids_.find(uid) != assigned_uids_.end()) {
                throw std::runtime_error("Dag: duplicate operator uid '" + uid + "'");
            }
            assigned_uids_.insert(uid);
            id = derive_id_from_uid_(uid);
        } else {
            id = derive_id("sharded_keyed_stage");
        }
        auto in_channel = upstream.output;

        detail::OperatorRunner runner;
        runner.name = "sharded_keyed_stage";
        runner.id = id;
        runner.run = [num_shards,
                      factory,
                      key_bytes_of,
                      id,
                      in_channel,
                      out_channel,
                      restore_from,
                      on_checkpoint](RuntimeContext& ctx,
                                     const std::function<bool()>& should_stop) {
            using namespace std::chrono_literals;
            // The stage's workers emit into the (thread-safe) output channel; the
            // coordinator (this thread) forwards barriers/watermarks through the
            // same channel inside checkpoint()/advance_watermark().
            ShardedKeyedStage<In, Out> stage(
                num_shards, id, factory, key_bytes_of, [out_channel](StreamElement<Out> e) {
                    return out_channel->push(std::move(e));
                });
            if (!restore_from.bytes.empty()) {
                stage.restore(restore_from);
            }
            stage.start();
            while (!should_stop()) {
                // 1s is a cancellation heartbeat; the cancel hook closes
                // in_channel which wakes pop_for immediately.
                auto maybe = in_channel->pop_for(1s);
                if (maybe.has_value()) {
                    if (maybe->is_data()) {
                        clink::metrics::op::records_in_inc(id.value(), maybe->as_data().size());
                        stage.submit(std::move(maybe->as_data()));
                    } else if (maybe->is_watermark()) {
                        stage.advance_watermark(maybe->as_watermark());
                    } else if (maybe->is_barrier()) {
                        auto result = stage.checkpoint(maybe->as_barrier());
                        // Persist ONLY a successful checkpoint, THEN ack with the
                        // combined status (ack-after-durable: the JM counts a
                        // subtask checkpointed once its bytes are on stable
                        // storage). A FAILED checkpoint (a worker threw or the
                        // merge failed) carries empty bytes; persisting it would
                        // write an empty snapshot at a HIGHER id that shadows the
                        // last good one on load_latest() -> silent data loss on
                        // restore. So skip the hook when !result.ok (the ack is
                        // false anyway). A null hook keeps the in-process path.
                        bool persisted = true;
                        std::string persist_err;
                        if (result.ok && on_checkpoint) {
                            // A throwing persist hook must not crash the runner;
                            // treat it as a failed (non-durable) checkpoint.
                            try {
                                persisted = on_checkpoint(result);
                            } catch (const std::exception& e) {
                                persisted = false;
                                persist_err = e.what();
                            }
                        }
                        if (const auto& cb = ctx.checkpoint_ack(); cb) {
                            cb(result.id,
                               result.ok && persisted,
                               persisted ? result.error : persist_err);
                        }
                    } else if (maybe->is_drain()) {
                        // Rescale wind-down signal: forward one coordinated drain
                        // downstream after all shards reach it. The subsequent
                        // EOS drives the actual wind-down.
                        stage.drain(maybe->as_drain());
                    }
                } else if (in_channel->closed()) {
                    break;
                }
            }
            // Ordering matters: close_input -> await -> close out_channel, the
            // same flush-then-close order as add_operator. out_channel stays OPEN
            // across await() ON PURPOSE: a worker's flush() may emit residual
            // data, and the downstream sink runner drains out_channel
            // concurrently (LocalExecutor spawns it too), so those pushes make
            // progress. Do NOT close out_channel before await() to "avoid a
            // hang" - that discards flush output and in-flight data. A genuine
            // cancel unblocks any stuck push via the cancel hook, which closes
            // out_channel.
            stage.close_input();
            stage.await();
            out_channel->close();
            // Surface any worker failure so the executor records it (mirrors a
            // throwing single-input operator).
            if (!stage.worker_errors().empty()) {
                throw std::runtime_error(
                    "sharded_keyed_stage: " + std::to_string(stage.worker_errors().size()) +
                    " worker(s) failed; first: " + stage.worker_errors().front().second);
            }
        };
        runner.cancel = [in_channel, out_channel] {
            in_channel->close();
            out_channel->close();
        };
        runner.input_depth = [in_channel] { return in_channel->size(); };
        runner.input_capacity = [in_channel] { return in_channel->capacity(); };
        runner.input_high_water = [in_channel] { return in_channel->high_water_mark(); };

        runners_.push_back(std::move(runner));
        return StageHandle<Out>{out_channel, runners_.size() - 1};
    }

    // ---- Fork (broadcast tee) -------------------------------------------
    //
    // Tees one upstream channel into N independent downstream channels. Data
    // batches, watermarks, and checkpoint barriers are all copied to every
    // branch. This is the simplest form of branching: same data goes to every
    // path, each path does something different with it.
    //
    // Returns a vector of N StageHandles, one per branch. Each handle owns a
    // distinct downstream channel and can be passed into add_operator/add_sink
    // independently.
    template <typename T>
    std::vector<StageHandle<T>> fork(StageHandle<T> upstream, std::size_t n) {
        std::vector<std::shared_ptr<BoundedChannel<StreamElement<T>>>> outs;
        outs.reserve(n);
        for (std::size_t i = 0; i < n; ++i) {
            outs.push_back(
                std::make_shared<BoundedChannel<StreamElement<T>>>(default_channel_capacity_));
        }

        auto in_channel = upstream.output;
        const OperatorId id = derive_id("fork");

        detail::OperatorRunner runner;
        runner.name = "fork_" + std::to_string(n);
        runner.id = id;
        runner.run = [in_channel, outs](RuntimeContext& /*ctx*/,
                                        const std::function<bool()>& should_stop) {
            while (!should_stop()) {
                auto maybe = in_channel->pop();
                if (!maybe.has_value()) {
                    break;
                }
                // Push a copy to every branch except the last; move into the
                // last to avoid one needless copy when n >= 1.
                for (std::size_t i = 0; i + 1 < outs.size(); ++i) {
                    outs[i]->push(*maybe);
                }
                outs.back()->push(std::move(*maybe));
            }
            for (auto& o : outs) {
                o->close();
            }
        };
        runner.cancel = [in_channel, outs] {
            in_channel->close();
            for (auto& o : outs) {
                o->close();
            }
        };
        runner.input_depth = [in_channel] { return in_channel->size(); };
        runner.input_capacity = [in_channel] { return in_channel->capacity(); };
        runner.input_high_water = [in_channel] { return in_channel->high_water_mark(); };

        runners_.push_back(std::move(runner));

        std::vector<StageHandle<T>> handles;
        handles.reserve(n);
        for (auto& o : outs) {
            handles.push_back(StageHandle<T>{o, runners_.size() - 1});
        }
        return handles;
    }

    // ---- Split (1 -> N route by selector, side-output style) ------------
    //
    // Each data record is routed to exactly one downstream branch chosen by
    // the user's selector function. Branch index < 0 or >= branch_count drops
    // the record (useful for "filter and route" patterns). Watermarks and
    // checkpoint barriers are broadcast to all branches so downstream
    // alignment still works.
    //
    // All branches carry the same element type T. To produce different types
    // per branch, follow the split with per-branch add_operator<T, Out>.
    //
    // Models Beam's TupleTag side-output pattern: branch 0 is conventionally
    // the "main" output, branches 1+ are conventionally side outputs.
    template <typename T>
    std::vector<StageHandle<T>> add_split(StageHandle<T> upstream,
                                          std::function<int(const T&)> selector,
                                          std::size_t branch_count,
                                          std::string name = "split") {
        if (branch_count == 0) {
            throw std::invalid_argument("add_split: branch_count must be > 0");
        }

        std::vector<std::shared_ptr<BoundedChannel<StreamElement<T>>>> outs;
        outs.reserve(branch_count);
        for (std::size_t i = 0; i < branch_count; ++i) {
            outs.push_back(
                std::make_shared<BoundedChannel<StreamElement<T>>>(default_channel_capacity_));
        }

        auto in_channel = upstream.output;
        const OperatorId id = derive_id(name);

        detail::OperatorRunner runner;
        runner.name = std::move(name);
        runner.id = id;
        runner.run = [in_channel, outs, selector = std::move(selector), branch_count](
                         RuntimeContext& /*ctx*/, const std::function<bool()>& should_stop) {
            while (!should_stop()) {
                auto maybe = in_channel->pop();
                if (!maybe.has_value()) {
                    break;
                }
                auto& elem = *maybe;
                if (elem.is_data()) {
                    // Route each record in the batch independently. Records
                    // destined for the same branch are gathered into a per-
                    // branch batch so we don't fragment downstream batches
                    // 1:1 with input records.
                    std::vector<Batch<T>> per_branch(branch_count);
                    for (auto& record : elem.as_data()) {
                        const int idx = selector(record.value());
                        if (idx < 0 || static_cast<std::size_t>(idx) >= branch_count) {
                            continue;  // drop
                        }
                        per_branch[static_cast<std::size_t>(idx)].push(std::move(record));
                    }
                    for (std::size_t b = 0; b < branch_count; ++b) {
                        if (!per_branch[b].empty()) {
                            outs[b]->push(StreamElement<T>::data(std::move(per_branch[b])));
                        }
                    }
                } else {
                    // Watermarks and barriers broadcast to all branches.
                    for (std::size_t i = 0; i + 1 < outs.size(); ++i) {
                        outs[i]->push(elem);
                    }
                    outs.back()->push(std::move(elem));
                }
            }
            for (auto& o : outs) {
                o->close();
            }
        };
        runner.cancel = [in_channel, outs] {
            in_channel->close();
            for (auto& o : outs) {
                o->close();
            }
        };
        runner.input_depth = [in_channel] { return in_channel->size(); };
        runner.input_capacity = [in_channel] { return in_channel->capacity(); };
        runner.input_high_water = [in_channel] { return in_channel->high_water_mark(); };

        runners_.push_back(std::move(runner));

        std::vector<StageHandle<T>> handles;
        handles.reserve(branch_count);
        for (auto& o : outs) {
            handles.push_back(StageHandle<T>{o, runners_.size() - 1});
        }
        return handles;
    }

    // ---- Union (N -> 1 merge of homogeneous streams) --------------------
    //
    // Merges N upstream stages of the same type into a single output stream.
    // Data records are forwarded immediately from whichever input has them
    // (order across inputs is unspecified). Watermarks downstream are the
    // running min across inputs. Checkpoint barriers are aligned: an input
    // that has delivered barrier B is paused until every other input has
    // also delivered B; only then is B forwarded downstream.
    //
    // Records arriving on slow inputs (those that haven't reached B yet) are
    // still processed during alignment - they belong to checkpoint B, not
    // B+1. This is the standard Chandy-Lamport rule.
    template <typename T>
    StageHandle<T> union_streams(std::vector<StageHandle<T>> upstreams) {
        const std::size_t n = upstreams.size();
        std::vector<std::shared_ptr<BoundedChannel<StreamElement<T>>>> in_channels;
        in_channels.reserve(n);
        for (auto& u : upstreams) {
            in_channels.push_back(u.output);
        }
        auto out_channel =
            std::make_shared<BoundedChannel<StreamElement<T>>>(default_channel_capacity_);
        const std::string name = "union_" + std::to_string(n);
        const OperatorId id = derive_id(name);

        detail::OperatorRunner runner;
        runner.name = name;
        runner.id = id;
        runner.run = [in_channels, out_channel, n](RuntimeContext& ctx,
                                                   const std::function<bool()>& should_stop) {
            // Unaligned mode flips the alignment state machine so the
            // first barrier from any input forwards immediately. The
            // already-queued records on the other inputs are NOT held
            // back - they get forwarded as the union runner picks them
            // up on subsequent iterations. From the downstream's
            // perspective those records arrive AFTER the barrier, so
            // they belong to the next checkpoint epoch. This matches
            // unaligned semantics where in-flight records past
            // the barrier are captured into the snapshot at the
            // *destination* operator (the one with state) - union has
            // no state itself, so its job is just to let the barrier
            // overtake.
            // Phase 26a: per-barrier mode. The aligner reads each
            // barrier's stamped mode rather than capturing the
            // job-global flag at startup; the coordinator stamps mode
            // when issuing the barrier (defaults derive from
            // JobConfig.unaligned_checkpoints).
            MultiInputAlignment align(n);
            using namespace std::chrono_literals;
            while (!should_stop()) {
                bool any_progress = false;
                for (std::size_t i = 0; i < n; ++i) {
                    // Even when an input is paused (waiting for barrier
                    // alignment), the producer may have closed its
                    // channel after pushing the barrier and exiting.
                    // We must still observe that close - otherwise an
                    // input paused at a barrier that never completes
                    // alignment (e.g. terminal barrier from one source
                    // when the producer immediately closes) wedges
                    // align.all_closed() forever. The par>4 startup-
                    // race deadlock manifested through this path.
                    //
                    // CRUCIAL: only mark closed when the channel is
                    // both closed AND fully drained. closed() returns
                    // true after the producer called close() but the
                    // queue may still hold pre-barrier records that
                    // belong to this checkpoint epoch (or post-barrier
                    // records waiting for alignment to resume). Marking
                    // closed before drain loses those records - which
                    // showed up as 1-2% pane-count loss at par=4/16 in
                    // an earlier version of this fix.
                    if (align.input_paused(i)) {
                        if (!align.input_closed(i) && in_channels[i]->closed() &&
                            in_channels[i]->size() == 0) {
                            align.on_input_closed(i);
                            if (auto wm_adv = align.refresh_watermark(); wm_adv.forward) {
                                out_channel->push(StreamElement<T>::watermark(wm_adv.watermark));
                            }
                        }
                        continue;
                    }
                    auto maybe = in_channels[i]->try_pop();
                    if (!maybe.has_value()) {
                        if (in_channels[i]->closed()) {
                            align.on_input_closed(i);
                            if (auto wm_adv = align.refresh_watermark(); wm_adv.forward) {
                                out_channel->push(StreamElement<T>::watermark(wm_adv.watermark));
                            }
                        }
                        continue;
                    }
                    any_progress = true;
                    if (maybe->is_data()) {
                        out_channel->push(std::move(*maybe));
                    } else if (maybe->is_watermark()) {
                        if (auto adv = align.on_watermark(i, maybe->as_watermark()); adv.forward) {
                            out_channel->push(StreamElement<T>::watermark(adv.watermark));
                        }
                    } else {
                        if (auto adv = align.on_barrier(
                                i, ctx.apply_barrier_mode_override(maybe->as_barrier()));
                            adv.forward) {
                            out_channel->push(StreamElement<T>::barrier(adv.barrier));
                        }
                    }
                }
                if (align.all_closed()) {
                    break;
                }
                if (!any_progress) {
                    std::this_thread::sleep_for(1ms);
                }
            }
            out_channel->close();
        };
        runner.cancel = [in_channels, out_channel] {
            for (auto& c : in_channels) {
                c->close();
            }
            out_channel->close();
        };
        runner.input_depth = [in_channels] {
            std::size_t total = 0;
            for (auto& c : in_channels) {
                total += c->size();
            }
            return total;
        };
        runner.input_capacity = [in_channels] {
            std::size_t total = 0;
            for (auto& c : in_channels) {
                total += c->capacity();
            }
            return total;
        };
        runner.input_high_water = [in_channels] {
            std::size_t hw = 0;
            for (auto& c : in_channels) {
                hw = std::max(hw, c->high_water_mark());
            }
            return hw;
        };

        runners_.push_back(std::move(runner));
        return StageHandle<T>{out_channel, runners_.size() - 1};
    }

    // ---- Iteration (cyclic dataflow) ------------------------------------
    //
    // Adds a feedback edge to the DAG so a downstream stage can route
    // records back into an upstream stage of the same type. The
    // analogue is `DataStream.iterate() + closeWith(...)` - the clink
    // shape is intentionally similar.
    //
    // Wiring:
    //   auto iter = dag.iterate_stream<T>(input);
    //   auto body = dag.add_operator<T, T>(iter.head_output(), ...);
    //   auto branches = dag.add_split<T>(body, /*selector*/, 2);
    //   iter.close_with(branches[0]);              // loops back
    //   dag.add_sink<T>(branches[1], my_sink);     // exits
    //
    // Termination: when the external input closes AND the feedback
    // channel closes (because the body's loop-side branch ran out of
    // records), the head closes its merged output. The body's
    // operators drain, the sink completes. Because the close cascade
    // is the only termination signal, the body should eventually emit
    // zero records to the loop branch for natural convergence.
    //
    // The optional IterationConfig.max_records cap is a safety net for
    // body logic that doesn't converge - once that many records have
    // flowed through the head, it stops accepting from feedback and
    // closes merged, unwinding the loop. Set to 0 (default) to disable.
    //
    // Checkpoint barriers in cycles: v1 does NOT inject barriers
    // through the feedback path. The head's external input is the
    // canonical barrier source; barriers flow through the body
    // once, and the feedback-side records are treated as belonging
    // to the same epoch. Operators with state inside the loop will
    // observe a coherent snapshot at the head's barrier point, but
    // iterations *in flight* at the barrier won't fully settle until
    // checkpoint N+1 - acceptable for most fixed-point / training
    // loops where the eventual state is what matters.
    template <typename T>
    class IterationStream {
    public:
        IterationStream(StageHandle<T> head_output,
                        std::shared_ptr<BoundedChannel<StreamElement<T>>> feedback,
                        Dag* parent_dag)
            : head_output_(std::move(head_output)),
              feedback_(std::move(feedback)),
              parent_(parent_dag) {}

        // The merged (external input + feedback) stream the body
        // should consume. Wire the body's operators downstream of this.
        StageHandle<T> head_output() const { return head_output_; }

        // Tie the loop. Records flowing into `body_loop` (typically
        // a `dag.add_split` branch the user routed for re-iteration)
        // are pumped back into the head's feedback channel. When
        // body_loop closes, the feedback channel closes; combined with
        // the external input closing, the head terminates.
        void close_with(StageHandle<T> body_loop) {
            if (closed_) {
                throw std::runtime_error("IterationStream::close_with called twice");
            }
            closed_ = true;
            parent_->add_iteration_tail_(body_loop, feedback_);
        }

    private:
        StageHandle<T> head_output_;
        std::shared_ptr<BoundedChannel<StreamElement<T>>> feedback_;
        Dag* parent_;
        bool closed_{false};
    };

    // Serialize a vector of typed records into bytes using the given
    // codec. Format: [u32 count] then for each record:
    // [u8 has_event_time][i64 event_time_ms if present][u32 value_len]
    // [value_bytes]. Stable across runs; the cycle-checkpoint and
    // unaligned-checkpoint paths both rely on this layout.
    template <typename T>
    static std::vector<std::byte> serialize_records_(const std::vector<Record<T>>& records,
                                                     const Codec<T>& codec) {
        std::vector<std::byte> out;
        const auto put_u32 = [&](std::uint32_t v) {
            for (int i = 0; i < 4; ++i) {
                out.push_back(static_cast<std::byte>((v >> (i * 8)) & 0xFF));
            }
        };
        const auto put_i64 = [&](std::int64_t v) {
            const auto u = static_cast<std::uint64_t>(v);
            for (int i = 0; i < 8; ++i) {
                out.push_back(static_cast<std::byte>((u >> (i * 8)) & 0xFF));
            }
        };
        put_u32(static_cast<std::uint32_t>(records.size()));
        for (const auto& r : records) {
            const bool has_t = r.event_time().has_value();
            out.push_back(static_cast<std::byte>(has_t ? 1 : 0));
            if (has_t) {
                put_i64(r.event_time()->millis());
            }
            auto bytes = codec.encode(r.value());
            put_u32(static_cast<std::uint32_t>(bytes.size()));
            out.insert(out.end(), bytes.begin(), bytes.end());
        }
        return out;
    }

    template <typename T>
    static std::vector<Record<T>> deserialize_records_(std::span<const std::byte> in,
                                                       const Codec<T>& codec) {
        std::vector<Record<T>> out;
        std::size_t pos = 0;
        const auto read_u32 = [&]() -> std::uint32_t {
            std::uint32_t v = 0;
            for (int i = 0; i < 4; ++i) {
                v |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(in[pos + i])) << (i * 8);
            }
            pos += 4;
            return v;
        };
        const auto read_i64 = [&]() -> std::int64_t {
            std::uint64_t u = 0;
            for (int i = 0; i < 8; ++i) {
                u |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(in[pos + i])) << (i * 8);
            }
            pos += 8;
            return static_cast<std::int64_t>(u);
        };
        if (in.size() < 4) {
            return out;
        }
        const auto count = read_u32();
        out.reserve(count);
        for (std::uint32_t r = 0; r < count; ++r) {
            if (pos >= in.size()) {
                break;
            }
            const bool has_t = static_cast<std::uint8_t>(in[pos++]) != 0;
            std::optional<EventTime> t;
            if (has_t) {
                t = EventTime{read_i64()};
            }
            if (pos + 4 > in.size()) {
                break;
            }
            const auto v_len = read_u32();
            if (pos + v_len > in.size()) {
                break;
            }
            auto decoded = codec.decode(in.subspan(pos, v_len));
            pos += v_len;
            if (!decoded.has_value()) {
                continue;
            }
            if (t.has_value()) {
                out.emplace_back(std::move(*decoded), *t);
            } else {
                out.emplace_back(std::move(*decoded));
            }
        }
        return out;
    }

    template <typename T>
    IterationStream<T> iterate_stream(StageHandle<T> input,
                                      std::optional<Codec<T>> codec = std::nullopt,
                                      IterationConfig config = IterationConfig{}) {
        auto feedback =
            std::make_shared<BoundedChannel<StreamElement<T>>>(default_channel_capacity_);
        auto external = input.output;
        auto merged = std::make_shared<BoundedChannel<StreamElement<T>>>(default_channel_capacity_);

        const std::string name = "iterate_head";
        const OperatorId id = derive_id(name);
        detail::OperatorRunner runner;
        runner.name = name;
        runner.id = id;
        runner.run = [external, feedback, merged, codec, id, config](
                         RuntimeContext& ctx, const std::function<bool()>& should_stop) {
            using namespace std::chrono_literals;
            // Cycle-checkpoint restore. When a codec is wired AND a
            // state backend is present, an earlier run may have
            // persisted in-flight feedback records at barrier time
            // under a fixed slot. Read them back and push into the
            // feedback channel BEFORE polling, so the body sees them
            // in the right relative order - they were emitted by the
            // body in a prior run; replaying them gives the body a
            // chance to continue iterating from where it stopped.
            constexpr const char* kInflightSlot = "__iterate_inflight__";
            const bool ckpt_enabled = codec.has_value() && ctx.has_state_backend();
            if (ckpt_enabled) {
                auto stored = ctx.state_backend()->get(
                    id, StateBackend::KeyView{kInflightSlot, std::strlen(kInflightSlot)});
                if (stored.has_value()) {
                    auto recs = Dag::deserialize_records_(
                        std::span<const std::byte>{stored->data(), stored->size()}, *codec);
                    for (auto& r : recs) {
                        Batch<T> b;
                        b.push(std::move(r));
                        feedback->push(StreamElement<T>::data(std::move(b)));
                    }
                    // Clear so the next snapshot starts fresh; without
                    // this, every snapshot would re-include the
                    // restored set.
                    ctx.state_backend()->erase(
                        id, StateBackend::KeyView{kInflightSlot, std::strlen(kInflightSlot)});
                }
            }

            // Drain the feedback channel into a typed vector, return
            // the records. Called when an external barrier is about
            // to be forwarded - captures the records that were
            // circulating in the loop at the barrier point so the
            // checkpoint is complete.
            auto drain_feedback_records = [&]() {
                std::vector<Record<T>> out;
                while (auto e = feedback->try_pop()) {
                    if (e->is_data()) {
                        for (auto& r : e->as_data()) {
                            out.push_back(std::move(r));
                        }
                    }
                    // Watermarks/barriers from the cycle are dropped
                    // (matches the tail's no-loop-for-control-events
                    // semantics).
                }
                return out;
            };

            // Round-robin poll: read from external, then feedback,
            // then idle. Termination is the subtle part of any cyclic
            // dataflow: the body's output channels can't close until
            // OUR merged output closes (which closes downstream all
            // the way around the loop), so we can't wait for feedback
            // to close. Instead we detect quiescence: once the
            // external input is closed AND both channels have stayed
            // empty for kIdleThreshold consecutive polls, every
            // record that entered the loop has either exited via the
            // user's output branch or is buffered somewhere
            // downstream that won't come back. With the inner sleep
            // at 1ms, 100 ticks ≈ 100ms grace - enough for body
            // operators of any reasonable speed, far short of human
            // latency.
            const int kIdleThreshold = config.idle_threshold > 0 ? config.idle_threshold : 100;
            int consecutive_idle = 0;
            std::uint64_t records_pushed = 0;
            const bool has_cap = config.max_records > 0;
            while (!should_stop()) {
                // Bail out when the user's cap is hit. Closing merged
                // here unwinds the loop through the natural cascade
                // (body sees merged closed -> body exits -> split
                // closes branches -> tail closes feedback -> done).
                // Records still queued in feedback are abandoned; the
                // cap is for runaway loops where that's the right call.
                if (has_cap && records_pushed >= config.max_records) {
                    break;
                }
                bool progressed = false;
                if (auto e = external->try_pop()) {
                    // Cycle checkpointing: when a real barrier (not the
                    // end-of-stream terminal barrier emitted by sources
                    // with a state backend) comes through external,
                    // capture every record currently looping in
                    // feedback into the state backend, then forward
                    // the barrier. The body's downstream snapshot
                    // reflects state up to the barrier; this side-
                    // channel capture covers the iteration records the
                    // body emitted but hadn't re-processed at barrier
                    // time. On restore the head's restore-on-startup
                    // path pushes them back into feedback.
                    //
                    // Terminal barriers are end-of-stream signals - by
                    // definition no further records will arrive, so
                    // draining feedback would silently strand records
                    // that the loop is still trying to converge. Leave
                    // them in feedback so the natural quiescence
                    // detector lets them finish.
                    if (e->is_barrier() && ckpt_enabled && !e->as_barrier().is_terminal()) {
                        auto drained = drain_feedback_records();
                        auto bytes = Dag::serialize_records_(drained, *codec);
                        ctx.state_backend()->put(
                            id,
                            StateBackend::KeyView{kInflightSlot, std::strlen(kInflightSlot)},
                            StateBackend::ValueView{reinterpret_cast<const char*>(bytes.data()),
                                                    bytes.size()});
                    }
                    merged->push(std::move(*e));
                    ++records_pushed;
                    progressed = true;
                }
                if (auto f = feedback->try_pop()) {
                    merged->push(std::move(*f));
                    ++records_pushed;
                    progressed = true;
                }
                if (progressed) {
                    consecutive_idle = 0;
                    continue;
                }
                const bool ext_closed = external->closed();
                if (ext_closed && external->size() == 0 && feedback->size() == 0) {
                    if (++consecutive_idle >= kIdleThreshold) {
                        break;
                    }
                }
                std::this_thread::sleep_for(1ms);
            }
            merged->close();
        };
        runner.cancel = [external, feedback, merged] {
            external->close();
            feedback->close();
            merged->close();
        };
        runner.input_depth = [external, feedback] { return external->size() + feedback->size(); };
        runner.input_capacity = [external, feedback] {
            return external->capacity() + feedback->capacity();
        };
        runner.input_high_water = [external, feedback] {
            return std::max(external->high_water_mark(), feedback->high_water_mark());
        };
        runners_.push_back(std::move(runner));
        return IterationStream<T>{StageHandle<T>{merged, runners_.size() - 1}, feedback, this};
    }

    template <typename T>
    void add_iteration_tail_(StageHandle<T> body_loop,
                             std::shared_ptr<BoundedChannel<StreamElement<T>>> feedback) {
        auto body_out = body_loop.output;
        const std::string name = "iterate_tail";
        const OperatorId id = derive_id(name);
        detail::OperatorRunner runner;
        runner.name = name;
        runner.id = id;
        runner.run = [body_out, feedback](RuntimeContext& /*ctx*/,
                                          const std::function<bool()>& should_stop) {
            using namespace std::chrono_literals;
            // Pump every DATA element from the body's loop branch back
            // into the head's feedback channel. Watermarks and barriers
            // are intentionally dropped: a watermark that loops forever
            // would never let the head's idle detection fire, and
            // barriers in cycles aren't aligned in v1 (see the head
            // comment). Records flowing through the loop carry their
            // own progress; the head's external input is the canonical
            // source of progress signals downstream.
            //
            // When body_out closes (its upstream stage finished
            // emitting), close the feedback so the head can detect
            // quiescence promptly.
            while (!should_stop()) {
                if (auto e = body_out->try_pop()) {
                    if (e->is_data()) {
                        feedback->push(std::move(*e));
                    }
                    continue;
                }
                if (body_out->closed()) {
                    break;
                }
                std::this_thread::sleep_for(1ms);
            }
            feedback->close();
        };
        runner.cancel = [body_out, feedback] {
            body_out->close();
            feedback->close();
        };
        runner.input_depth = [body_out] { return body_out->size(); };
        runner.input_capacity = [body_out] { return body_out->capacity(); };
        runner.input_high_water = [body_out] { return body_out->high_water_mark(); };
        runners_.push_back(std::move(runner));
    }

    // ---- Interval Join (keyed, two heterogeneous streams) --------------
    //
    // For every record (a, t_a) on the left stream and every record (b, t_b)
    // on the right stream sharing the same key, emits joiner(a, b) when:
    //
    //     t_b ∈ [t_a - lower_bound, t_a + upper_bound]
    //
    // i.e. t_b - t_a ∈ [-lower_bound, +upper_bound]. The bounds may be zero
    // or even negative (e.g. lower=-1s says B must lag A by at least 1s).
    //
    // Per-key buffers retain records only while they could still match a
    // future arrival. Once the running watermark passes:
    //   - t_a + upper_bound  → evict A (no future B can be in the interval)
    //   - t_b + lower_bound  → evict B (symmetric)
    //
    // **Join types** (set via the `join_type` param):
    //   - Inner:      emit only when both sides match.
    //   - LeftOuter:  also emit joiner(left, nullopt) at eviction time for
    //                 every left record that never matched.
    //   - RightOuter: symmetric.
    //   - FullOuter:  both LeftOuter and RightOuter behaviour.
    //   - LeftSemi:   emit joiner(left, nullopt) exactly once per left
    //                 record that found at least one match (no inner emit
    //                 with the right side; SQL "WHERE EXISTS").
    //   - LeftAnti:   emit joiner(left, nullopt) at eviction time for left
    //                 records that found NO match (SQL "WHERE NOT EXISTS").
    //   - RightSemi:  symmetric to LeftSemi.
    //   - RightAnti:  symmetric to LeftAnti.
    //
    // The joiner takes std::optional on both sides so a single user function
    // can handle the matched and unmatched cases with the same call shape.
    // For inner joins both optionals are guaranteed populated.
    //
    // Watermarks downstream are min(left_wm, right_wm) via MultiInputAlignment;
    // barriers are Chandy-Lamport aligned across both inputs.
    //
    // The join state is held in-memory in this MVP. Porting onto KeyedState
    // is a small follow-up - the state-shape is `vector<{EventTime, V, bool}>`
    // per key per side, which fits the existing scan-based recovery model.
    enum class JoinType : std::uint8_t {
        Inner,
        LeftOuter,
        RightOuter,
        FullOuter,
        LeftSemi,
        LeftAnti,
        RightSemi,
        RightAnti
    };

    // What to do when a record's event-time has fallen behind the current
    // forwarded watermark - i.e. it arrived too late to ever match anything
    // and would be evicted on the next sweep without producing an emission.
    //
    //   Allow: insert as normal. The record is purely overhead - it will
    //          buffer briefly and then be evicted. Use when you'd rather
    //          tolerate occasional late arrivals than risk dropping
    //          legitimate ones.
    //   Drop:  skip insertion entirely. Increments the per-operator counter
    //          `interval_join.<id>.late_dropped.{left,right}` on the
    //          MetricsRegistry (when one is configured) so the drops are
    //          observable.
    enum class LateArrivalPolicy : std::uint8_t { Allow, Drop };

    template <typename A, typename B, typename K, typename C>
    StageHandle<C> interval_join(
        StageHandle<A> left,
        StageHandle<B> right,
        std::function<K(const A&)> left_key,
        std::function<K(const B&)> right_key,
        std::chrono::milliseconds lower_bound,
        std::chrono::milliseconds upper_bound,
        std::function<C(const std::optional<A>&, const std::optional<B>&)> joiner,
        JoinType join_type = JoinType::Inner,
        std::string name = "interval_join",
        std::optional<Codec<A>> left_codec = std::nullopt,
        std::optional<Codec<B>> right_codec = std::nullopt,
        std::optional<Codec<K>> key_codec = std::nullopt,
        LateArrivalPolicy late_policy = LateArrivalPolicy::Allow) {
        auto out_channel =
            std::make_shared<BoundedChannel<StreamElement<C>>>(default_channel_capacity_);
        const OperatorId id = derive_id(name);
        auto left_ch = left.output;
        auto right_ch = right.output;

        detail::OperatorRunner runner;
        runner.name = name;
        runner.id = id;
        runner.run = [left_ch,
                      right_ch,
                      out_channel,
                      left_key,
                      right_key,
                      joiner,
                      lower_bound,
                      upper_bound,
                      join_type,
                      left_codec,
                      right_codec,
                      key_codec,
                      late_policy,
                      id](RuntimeContext& ctx, const std::function<bool()>& should_stop) {
            using namespace std::chrono_literals;
            // Phase 26a: aligner reads each barrier's stamped mode;
            // can_unalign now gates only the in-flight CAPTURE step
            // (we still need codecs + state backend to serialize the
            // in-flight buffer). If a barrier arrives stamped
            // Unaligned at an operator that can't capture, the aligner
            // still forwards immediately (mode is the coordinator's
            // policy decision) but the records left on the unpaused
            // inputs roll into the next checkpoint epoch rather than
            // being snapshotted into this one. 26b will surface a
            // per-operator override for operators that must force
            // aligned semantics.
            const bool can_unalign =
                left_codec.has_value() && right_codec.has_value() && ctx.has_state_backend();
            MultiInputAlignment align(2);

            struct LeftEntry {
                EventTime t;
                A v;
                bool matched;
            };
            struct RightEntry {
                EventTime t;
                B v;
                bool matched;
            };
            std::unordered_map<K, std::vector<LeftEntry>> left_buf;
            std::unordered_map<K, std::vector<RightEntry>> right_buf;

            // Optional persistent mirror. Active when all three codecs are
            // supplied AND a state backend is configured for the job.
            const bool persist = left_codec.has_value() && right_codec.has_value() &&
                                 key_codec.has_value() && ctx.has_state_backend();

            std::optional<KeyedState<K, std::vector<LeftEntry>>> left_state;
            std::optional<KeyedState<K, std::vector<RightEntry>>> right_state;

            // Inline byte codec for {EventTime, V, bool}. Layout:
            //   [i64 t_le][u8 matched][u32 v_len_le][v_bytes]
            auto make_left_entry_codec = [](Codec<A> a_codec) {
                return Codec<LeftEntry>{
                    .encode =
                        [a_codec](const LeftEntry& e) {
                            std::vector<std::byte> out;
                            const auto u = static_cast<std::uint64_t>(e.t.millis());
                            for (int i = 0; i < 8; ++i) {
                                out.push_back(static_cast<std::byte>((u >> (i * 8)) & 0xFF));
                            }
                            out.push_back(static_cast<std::byte>(e.matched ? 1 : 0));
                            auto vb = a_codec.encode(e.v);
                            const auto len = static_cast<std::uint32_t>(vb.size());
                            for (int i = 0; i < 4; ++i) {
                                out.push_back(static_cast<std::byte>((len >> (i * 8)) & 0xFF));
                            }
                            out.insert(out.end(), vb.begin(), vb.end());
                            return out;
                        },
                    .decode = [a_codec](typename Codec<LeftEntry>::BytesView b)
                        -> std::optional<LeftEntry> {
                        if (b.size() < 13) {
                            return std::nullopt;
                        }
                        std::uint64_t u = 0;
                        for (int i = 0; i < 8; ++i) {
                            u |= static_cast<std::uint64_t>(static_cast<unsigned char>(b[i]))
                                 << (i * 8);
                        }
                        const bool m = static_cast<unsigned char>(b[8]) != 0;
                        std::uint32_t len = 0;
                        for (int i = 0; i < 4; ++i) {
                            len |= static_cast<std::uint32_t>(static_cast<unsigned char>(b[9 + i]))
                                   << (i * 8);
                        }
                        if (b.size() != 13 + len) {
                            return std::nullopt;
                        }
                        auto v = a_codec.decode(b.subspan(13, len));
                        if (!v.has_value()) {
                            return std::nullopt;
                        }
                        return LeftEntry{EventTime{static_cast<std::int64_t>(u)}, std::move(*v), m};
                    }};
            };
            auto make_right_entry_codec = [](Codec<B> b_codec) {
                return Codec<RightEntry>{
                    .encode =
                        [b_codec](const RightEntry& e) {
                            std::vector<std::byte> out;
                            const auto u = static_cast<std::uint64_t>(e.t.millis());
                            for (int i = 0; i < 8; ++i) {
                                out.push_back(static_cast<std::byte>((u >> (i * 8)) & 0xFF));
                            }
                            out.push_back(static_cast<std::byte>(e.matched ? 1 : 0));
                            auto vb = b_codec.encode(e.v);
                            const auto len = static_cast<std::uint32_t>(vb.size());
                            for (int i = 0; i < 4; ++i) {
                                out.push_back(static_cast<std::byte>((len >> (i * 8)) & 0xFF));
                            }
                            out.insert(out.end(), vb.begin(), vb.end());
                            return out;
                        },
                    .decode = [b_codec](typename Codec<RightEntry>::BytesView b)
                        -> std::optional<RightEntry> {
                        if (b.size() < 13) {
                            return std::nullopt;
                        }
                        std::uint64_t u = 0;
                        for (int i = 0; i < 8; ++i) {
                            u |= static_cast<std::uint64_t>(static_cast<unsigned char>(b[i]))
                                 << (i * 8);
                        }
                        const bool m = static_cast<unsigned char>(b[8]) != 0;
                        std::uint32_t len = 0;
                        for (int i = 0; i < 4; ++i) {
                            len |= static_cast<std::uint32_t>(static_cast<unsigned char>(b[9 + i]))
                                   << (i * 8);
                        }
                        if (b.size() != 13 + len) {
                            return std::nullopt;
                        }
                        auto v = b_codec.decode(b.subspan(13, len));
                        if (!v.has_value()) {
                            return std::nullopt;
                        }
                        return RightEntry{
                            EventTime{static_cast<std::int64_t>(u)}, std::move(*v), m};
                    }};
            };

            if (persist) {
                left_state.emplace(ctx.template keyed_state<K, std::vector<LeftEntry>>(
                    "left_buf", *key_codec, vector_codec(make_left_entry_codec(*left_codec))));
                right_state.emplace(ctx.template keyed_state<K, std::vector<RightEntry>>(
                    "right_buf", *key_codec, vector_codec(make_right_entry_codec(*right_codec))));

                // Load any previously-snapshotted state.
                left_state->scan(
                    [&](const K& k, const std::vector<LeftEntry>& vec) { left_buf[k] = vec; });
                right_state->scan(
                    [&](const K& k, const std::vector<RightEntry>& vec) { right_buf[k] = vec; });
            }

            auto sync_left = [&](const K& k) {
                if (!left_state.has_value()) {
                    return;
                }
                auto it = left_buf.find(k);
                if (it == left_buf.end() || it->second.empty()) {
                    left_state->erase(k);
                } else {
                    left_state->put(k, it->second);
                }
            };
            auto sync_right = [&](const K& k) {
                if (!right_state.has_value()) {
                    return;
                }
                auto it = right_buf.find(k);
                if (it == right_buf.end() || it->second.empty()) {
                    right_state->erase(k);
                } else {
                    right_state->put(k, it->second);
                }
            };

            // Late-arrival counters. Resolved once at runner start; null when
            // the job has no MetricsRegistry configured.
            Counter* late_left_counter = nullptr;
            Counter* late_right_counter = nullptr;
            if (ctx.metrics() != nullptr) {
                const std::string base =
                    "interval_join." + std::to_string(ctx.operator_id().value());
                late_left_counter = &ctx.metrics()->counter(base + ".late_dropped.left");
                late_right_counter = &ctx.metrics()->counter(base + ".late_dropped.right");
            }
            const bool drop_late = late_policy == LateArrivalPolicy::Drop;
            const std::int64_t lower_ms = lower_bound.count();
            const std::int64_t upper_ms = upper_bound.count();
            const bool emit_inner_pair =
                join_type == JoinType::Inner || join_type == JoinType::LeftOuter ||
                join_type == JoinType::RightOuter || join_type == JoinType::FullOuter;
            const bool emit_unmatched_left = join_type == JoinType::LeftOuter ||
                                             join_type == JoinType::FullOuter ||
                                             join_type == JoinType::LeftAnti;
            const bool emit_unmatched_right = join_type == JoinType::RightOuter ||
                                              join_type == JoinType::FullOuter ||
                                              join_type == JoinType::RightAnti;
            // Semi-join: emit each matched left/right record exactly once at
            // its eviction time (its first-match flag is on the entry).
            const bool emit_matched_left_semi = join_type == JoinType::LeftSemi;
            const bool emit_matched_right_semi = join_type == JoinType::RightSemi;

            auto in_window = [&](EventTime t_a, EventTime t_b) {
                const std::int64_t delta = t_b.millis() - t_a.millis();
                return delta >= -lower_ms && delta <= upper_ms;
            };

            auto emit_inner = [&, id](
                                  const A& a_val, EventTime a_t, const B& b_val, EventTime b_t) {
                Batch<C> batch;
                batch.emplace(joiner(std::optional<A>{a_val}, std::optional<B>{b_val}),
                              EventTime{std::max(a_t.millis(), b_t.millis())});
                out_channel->push(StreamElement<C>::data(std::move(batch)));
                clink::metrics::op::join_matches_inc(id.value());
                clink::metrics::op::records_out_inc(id.value());
            };
            auto emit_left_only = [&, id](const A& a_val, EventTime a_t) {
                Batch<C> batch;
                batch.emplace(joiner(std::optional<A>{a_val}, std::optional<B>{}), a_t);
                out_channel->push(StreamElement<C>::data(std::move(batch)));
                clink::metrics::op::records_out_inc(id.value());
            };
            auto emit_right_only = [&, id](const B& b_val, EventTime b_t) {
                Batch<C> batch;
                batch.emplace(joiner(std::optional<A>{}, std::optional<B>{b_val}), b_t);
                out_channel->push(StreamElement<C>::data(std::move(batch)));
                clink::metrics::op::records_out_inc(id.value());
            };

            auto evict = [&](EventTime W) {
                std::vector<K> touched_left;
                std::vector<K> touched_right;
                for (auto it = left_buf.begin(); it != left_buf.end();) {
                    auto& vec = it->second;
                    const auto orig_size = vec.size();
                    auto write = vec.begin();
                    for (auto read = vec.begin(); read != vec.end(); ++read) {
                        const bool stale = read->t.millis() + upper_ms < W.millis();
                        if (stale) {
                            if (emit_unmatched_left && !read->matched) {
                                emit_left_only(read->v, read->t);
                            } else if (emit_matched_left_semi && read->matched) {
                                emit_left_only(read->v, read->t);
                            }
                        } else {
                            if (write != read) {
                                *write = std::move(*read);
                            }
                            ++write;
                        }
                    }
                    vec.erase(write, vec.end());
                    if (vec.size() != orig_size) {
                        touched_left.push_back(it->first);
                    }
                    if (vec.empty()) {
                        it = left_buf.erase(it);
                    } else {
                        ++it;
                    }
                }
                for (auto it = right_buf.begin(); it != right_buf.end();) {
                    auto& vec = it->second;
                    const auto orig_size = vec.size();
                    auto write = vec.begin();
                    for (auto read = vec.begin(); read != vec.end(); ++read) {
                        const bool stale = read->t.millis() + lower_ms < W.millis();
                        if (stale) {
                            if (emit_unmatched_right && !read->matched) {
                                emit_right_only(read->v, read->t);
                            } else if (emit_matched_right_semi && read->matched) {
                                emit_right_only(read->v, read->t);
                            }
                        } else {
                            if (write != read) {
                                *write = std::move(*read);
                            }
                            ++write;
                        }
                    }
                    vec.erase(write, vec.end());
                    if (vec.size() != orig_size) {
                        touched_right.push_back(it->first);
                    }
                    if (vec.empty()) {
                        it = right_buf.erase(it);
                    } else {
                        ++it;
                    }
                }
                for (const auto& k : touched_left)
                    sync_left(k);
                for (const auto& k : touched_right)
                    sync_right(k);
            };

            auto handle_left = [&](const StreamElement<A>& el) {
                if (el.is_data()) {
                    for (const auto& rec : el.as_data()) {
                        const A& v = rec.value();
                        const EventTime t = rec.event_time().value_or(EventTime{0});
                        // Late-arrival check: a record with t + upper_ms < W
                        // can never match a future right record because
                        // future t_right >= W. Either drop (per policy) or
                        // proceed and let eviction handle it.
                        if (drop_late) {
                            const auto wm = align.current_watermark();
                            if (t.millis() + upper_ms < wm.timestamp().millis()) {
                                if (late_left_counter != nullptr) {
                                    late_left_counter->increment();
                                }
                                continue;
                            }
                        }
                        const K k = left_key(v);
                        bool any_match = false;
                        if (auto it = right_buf.find(k); it != right_buf.end()) {
                            for (auto& entry : it->second) {
                                if (in_window(t, entry.t)) {
                                    if (emit_inner_pair) {
                                        emit_inner(v, t, entry.v, entry.t);
                                    }
                                    entry.matched = true;
                                    any_match = true;
                                }
                            }
                            if (any_match) {
                                sync_right(k);
                            }
                        }
                        left_buf[k].push_back(LeftEntry{t, v, any_match});
                        sync_left(k);
                    }
                } else if (el.is_watermark()) {
                    if (auto adv = align.on_watermark(0, el.as_watermark()); adv.forward) {
                        evict(adv.watermark.timestamp());
                        out_channel->push(StreamElement<C>::watermark(adv.watermark));
                    }
                } else {
                    auto adv =
                        align.on_barrier(0, ctx.apply_barrier_mode_override(el.as_barrier()));
                    if (adv.forward) {
                        // Terminal barriers signal end-of-stream - drain
                        // would strand records we haven't joined yet,
                        // and unaligned semantics don't apply when no
                        // more records are coming.
                        if (adv.unaligned_first && can_unalign && !adv.barrier.is_terminal()) {
                            std::vector<Record<B>> right_inflight;
                            while (auto m = right_ch->try_pop()) {
                                if (m->is_data()) {
                                    for (auto& r : m->as_data()) {
                                        right_inflight.push_back(std::move(r));
                                    }
                                }
                            }
                            auto bytes = Dag::serialize_records_(right_inflight, *right_codec);
                            ctx.state_backend()->put(
                                id,
                                StateBackend::KeyView{"__interval_join_right_inflight__"},
                                StateBackend::ValueView{reinterpret_cast<const char*>(bytes.data()),
                                                        bytes.size()});
                        }
                        out_channel->push(StreamElement<C>::barrier(adv.barrier));
                    }
                }
            };

            auto handle_right = [&](const StreamElement<B>& el) {
                if (el.is_data()) {
                    for (const auto& rec : el.as_data()) {
                        const B& v = rec.value();
                        const EventTime t = rec.event_time().value_or(EventTime{0});
                        // Symmetric late check: t + lower_ms < W means
                        // future t_left > W >= t + lower_ms, so no future
                        // left can be in the interval anymore.
                        if (drop_late) {
                            const auto wm = align.current_watermark();
                            if (t.millis() + lower_ms < wm.timestamp().millis()) {
                                if (late_right_counter != nullptr) {
                                    late_right_counter->increment();
                                }
                                continue;
                            }
                        }
                        const K k = right_key(v);
                        bool any_match = false;
                        if (auto it = left_buf.find(k); it != left_buf.end()) {
                            for (auto& entry : it->second) {
                                if (in_window(entry.t, t)) {
                                    if (emit_inner_pair) {
                                        emit_inner(entry.v, entry.t, v, t);
                                    }
                                    entry.matched = true;
                                    any_match = true;
                                }
                            }
                            if (any_match) {
                                sync_left(k);
                            }
                        }
                        right_buf[k].push_back(RightEntry{t, v, any_match});
                        sync_right(k);
                    }
                } else if (el.is_watermark()) {
                    if (auto adv = align.on_watermark(1, el.as_watermark()); adv.forward) {
                        evict(adv.watermark.timestamp());
                        out_channel->push(StreamElement<C>::watermark(adv.watermark));
                    }
                } else {
                    auto adv =
                        align.on_barrier(1, ctx.apply_barrier_mode_override(el.as_barrier()));
                    if (adv.forward) {
                        if (adv.unaligned_first && can_unalign && !adv.barrier.is_terminal()) {
                            std::vector<Record<A>> left_inflight;
                            while (auto m = left_ch->try_pop()) {
                                if (m->is_data()) {
                                    for (auto& r : m->as_data()) {
                                        left_inflight.push_back(std::move(r));
                                    }
                                }
                            }
                            auto bytes = Dag::serialize_records_(left_inflight, *left_codec);
                            ctx.state_backend()->put(
                                id,
                                StateBackend::KeyView{"__interval_join_left_inflight__"},
                                StateBackend::ValueView{reinterpret_cast<const char*>(bytes.data()),
                                                        bytes.size()});
                        }
                        out_channel->push(StreamElement<C>::barrier(adv.barrier));
                    }
                }
            };

            // Restore in-flight buffers on startup if a previous run
            // persisted them under our slots. We hold the restored
            // records in local pending queues rather than pushing back
            // into left_ch/right_ch - those upstream channels may have
            // already been closed by their source runners (whose
            // threads spawn concurrently with this one), and pushing
            // into a closed channel silently no-ops. The main poll
            // loop below drains the pending queues first, exactly the
            // way it would drain a channel.
            std::deque<StreamElement<A>> pending_left;
            std::deque<StreamElement<B>> pending_right;
            if (can_unalign) {
                if (auto stored = ctx.state_backend()->get(
                        id, StateBackend::KeyView{"__interval_join_left_inflight__"});
                    stored.has_value()) {
                    auto recs = Dag::deserialize_records_<A>(
                        std::span<const std::byte>{stored->data(), stored->size()}, *left_codec);
                    for (auto& r : recs) {
                        Batch<A> b;
                        b.push(std::move(r));
                        pending_left.push_back(StreamElement<A>::data(std::move(b)));
                    }
                    ctx.state_backend()->erase(
                        id, StateBackend::KeyView{"__interval_join_left_inflight__"});
                }
                if (auto stored = ctx.state_backend()->get(
                        id, StateBackend::KeyView{"__interval_join_right_inflight__"});
                    stored.has_value()) {
                    auto recs = Dag::deserialize_records_<B>(
                        std::span<const std::byte>{stored->data(), stored->size()}, *right_codec);
                    for (auto& r : recs) {
                        Batch<B> b;
                        b.push(std::move(r));
                        pending_right.push_back(StreamElement<B>::data(std::move(b)));
                    }
                    ctx.state_backend()->erase(
                        id, StateBackend::KeyView{"__interval_join_right_inflight__"});
                }
            }

            while (!should_stop()) {
                bool any_progress = false;
                if (!align.input_paused(0)) {
                    // Drain the restore-on-startup buffer before the
                    // live channel so replayed records reach handle_left
                    // before any closing-end signals the channel may
                    // have already queued.
                    if (!pending_left.empty()) {
                        auto m = std::move(pending_left.front());
                        pending_left.pop_front();
                        any_progress = true;
                        handle_left(m);
                    } else if (auto m = left_ch->try_pop(); m.has_value()) {
                        any_progress = true;
                        handle_left(*m);
                    } else if (left_ch->closed()) {
                        align.on_input_closed(0);
                        if (auto wm = align.refresh_watermark(); wm.forward) {
                            evict(wm.watermark.timestamp());
                            out_channel->push(StreamElement<C>::watermark(wm.watermark));
                        }
                    }
                }
                if (!align.input_paused(1)) {
                    if (!pending_right.empty()) {
                        auto m = std::move(pending_right.front());
                        pending_right.pop_front();
                        any_progress = true;
                        handle_right(m);
                    } else if (auto m = right_ch->try_pop(); m.has_value()) {
                        any_progress = true;
                        handle_right(*m);
                    } else if (right_ch->closed()) {
                        align.on_input_closed(1);
                        if (auto wm = align.refresh_watermark(); wm.forward) {
                            evict(wm.watermark.timestamp());
                            out_channel->push(StreamElement<C>::watermark(wm.watermark));
                        }
                    }
                }
                if (align.all_closed()) {
                    break;
                }
                if (!any_progress) {
                    std::this_thread::sleep_for(1ms);
                }
            }
            out_channel->close();
        };
        runner.cancel = [left_ch, right_ch, out_channel] {
            left_ch->close();
            right_ch->close();
            out_channel->close();
        };
        runner.input_depth = [left_ch, right_ch] { return left_ch->size() + right_ch->size(); };
        runner.input_capacity = [left_ch, right_ch] {
            return left_ch->capacity() + right_ch->capacity();
        };
        runner.input_high_water = [left_ch, right_ch] {
            return std::max(left_ch->high_water_mark(), right_ch->high_water_mark());
        };

        runners_.push_back(std::move(runner));
        return StageHandle<C>{out_channel, runners_.size() - 1};
    }

    // ---- Broadcast Connect (main stream + broadcast control stream) -----
    //
    // Two-input operator where inputs are treated asymmetrically:
    //
    //   - The **broadcast** stream carries control records (config updates,
    //     feature-flag changes, slowly-changing-dimension rows, ...). The
    //     user's `on_broadcast` callback updates a per-operator
    //     `BroadcastState<State>` in response to each record.
    //
    //   - The **main** stream carries the high-volume data stream. The
    //     user's `on_main` callback reads the current broadcast state and
    //     returns an `optional<Out>` (nullopt drops the main record).
    //
    // Watermarks downstream are min(main_wm, broadcast_wm) via the standard
    // multi-input alignment, so consumers downstream of this operator see
    // the slower of the two streams. Barriers are Chandy-Lamport aligned
    // across both inputs - the same machinery as union/join.
    //
    // A state backend is required (the operator throws on open() if missing)
    // because broadcast state is the operator's defining feature; without
    // persistence, the abstraction collapses to "have a stateful map
    // operator", which the existing MapOperator already handles.
    template <typename Main, typename Brod, typename Out, typename State>
    StageHandle<Out> broadcast_connect(
        StageHandle<Main> main_in,
        StageHandle<Brod> broadcast_in,
        std::function<void(const Brod&, BroadcastState<State>&)> on_broadcast,
        std::function<std::optional<Out>(const Main&, BroadcastState<State>&)> on_main,
        Codec<State> state_codec,
        std::string slot_name = "broadcast",
        std::string name = "broadcast_connect",
        std::optional<Codec<Main>> main_codec = std::nullopt,
        std::optional<Codec<Brod>> brod_codec = std::nullopt) {
        // Thin wrapper over broadcast_process: the historic
        // single-output shape is "emit zero or one record" which maps
        // cleanly onto the vector<Out>-drain shape (push iff
        // on_main returned a value). on_broadcast already returns void
        // in both APIs, so it forwards unchanged. Kept as a separate
        // function for callsite back-compat; broadcast_process is the
        // preferred surface for emit-many.
        auto pb = [on_broadcast = std::move(on_broadcast)](
                      const Brod& v, BroadcastState<State>& s, std::vector<Out>& /*drain*/) {
            on_broadcast(v, s);
        };
        auto pm = [on_main = std::move(on_main)](
                      const Main& v, BroadcastState<State>& s, std::vector<Out>& drain) {
            if (auto produced = on_main(v, s); produced.has_value()) {
                drain.push_back(std::move(*produced));
            }
        };
        return broadcast_process<Main, Brod, Out, State>(std::move(main_in),
                                                         std::move(broadcast_in),
                                                         std::move(pb),
                                                         std::move(pm),
                                                         std::move(state_codec),
                                                         std::move(slot_name),
                                                         std::move(name),
                                                         std::move(main_codec),
                                                         std::move(brod_codec));
    }

    // Collector-style variant of broadcast_connect. The historical
    // broadcast_connect above wraps an on_main callback that returns
    // std::optional<Out> - fine for filter-with-state but limits the
    // operator to at most one output per input record. broadcast_process
    // gives each callback an output sink (a "drain") so it can emit
    // many records per input, matching // BroadcastProcessFunction.processElement Collector shape.
    //
    // process_broadcast: called for every record on the broadcast
    //   stream. Has read/write access to BroadcastState<State> and
    //   may emit downstream via `out`.
    // process_main: called for every record on the main stream. Has
    //   READ-ONLY access to broadcast state (the BroadcastState&
    //   reference is still passed, but the user is expected not to
    //   mutate from the main path; the runtime can't enforce this in
    //   v1) and may emit many records via `out`.
    template <typename Main, typename Brod, typename Out, typename State>
    StageHandle<Out> broadcast_process(
        StageHandle<Main> main_in,
        StageHandle<Brod> broadcast_in,
        std::function<void(const Brod&, BroadcastState<State>&, std::vector<Out>&)>
            process_broadcast,
        std::function<void(const Main&, BroadcastState<State>&, std::vector<Out>&)> process_main,
        Codec<State> state_codec,
        std::string slot_name = "broadcast",
        std::string name = "broadcast_process",
        std::optional<Codec<Main>> main_codec = std::nullopt,
        std::optional<Codec<Brod>> brod_codec = std::nullopt) {
        auto out_channel =
            std::make_shared<BoundedChannel<StreamElement<Out>>>(default_channel_capacity_);
        const OperatorId id = derive_id(name);
        auto main_ch = main_in.output;
        auto brod_ch = broadcast_in.output;

        detail::OperatorRunner runner;
        runner.name = name;
        runner.id = id;
        runner.run = [main_ch,
                      brod_ch,
                      out_channel,
                      process_broadcast,
                      process_main,
                      state_codec,
                      slot_name,
                      id,
                      main_codec,
                      brod_codec](RuntimeContext& ctx, const std::function<bool()>& should_stop) {
            using namespace std::chrono_literals;
            if (!ctx.has_state_backend()) {
                throw std::runtime_error("broadcast_process requires JobConfig::state_backend");
            }
            auto state = ctx.template broadcast_state<State>(slot_name, state_codec);
            MultiInputAlignment align(2);
            // Unaligned-checkpoint in-flight capture (mirrors
            // add_co_operator). When the first barrier forwards under
            // unaligned mode, drain the other input's queued pre-barrier
            // records into a reserved slot so the snapshot captures them
            // and they replay on restore. Gated on both record codecs +
            // a state backend; without them the barrier still forwards but
            // the unpaused input's records roll into the next epoch.
            static constexpr const char* kMainInflight = "__broadcast_main_inflight__";
            static constexpr const char* kBrodInflight = "__broadcast_brod_inflight__";
            const bool can_unalign =
                main_codec.has_value() && brod_codec.has_value() && ctx.has_state_backend();
            auto capture_main_inflight = [&] {
                std::vector<Record<Main>> inflight;
                while (auto m = main_ch->try_pop()) {
                    if (m->is_data()) {
                        for (auto& r : m->as_data()) {
                            inflight.push_back(std::move(r));
                        }
                    }
                }
                auto bytes = Dag::serialize_records_(inflight, *main_codec);
                ctx.state_backend()->put(
                    id,
                    StateBackend::KeyView{kMainInflight},
                    StateBackend::ValueView{reinterpret_cast<const char*>(bytes.data()),
                                            bytes.size()});
            };
            auto capture_brod_inflight = [&] {
                std::vector<Record<Brod>> inflight;
                while (auto m = brod_ch->try_pop()) {
                    if (m->is_data()) {
                        for (auto& r : m->as_data()) {
                            inflight.push_back(std::move(r));
                        }
                    }
                }
                auto bytes = Dag::serialize_records_(inflight, *brod_codec);
                ctx.state_backend()->put(
                    id,
                    StateBackend::KeyView{kBrodInflight},
                    StateBackend::ValueView{reinterpret_cast<const char*>(bytes.data()),
                                            bytes.size()});
            };

            auto emit_buffer = [&](std::vector<Out>& buf, std::optional<EventTime> et) {
                if (buf.empty()) {
                    return;
                }
                Batch<Out> batch;
                for (auto& v : buf) {
                    if (et.has_value()) {
                        batch.emplace(std::move(v), *et);
                    } else {
                        batch.emplace(std::move(v));
                    }
                }
                out_channel->push(StreamElement<Out>::data(std::move(batch)));
                buf.clear();
            };

            auto handle_brod = [&](const StreamElement<Brod>& el) {
                if (el.is_data()) {
                    std::vector<Out> emitted;
                    for (const auto& rec : el.as_data()) {
                        process_broadcast(rec.value(), state, emitted);
                        emit_buffer(emitted, rec.event_time());
                    }
                } else if (el.is_watermark()) {
                    if (auto adv = align.on_watermark(1, el.as_watermark()); adv.forward) {
                        out_channel->push(StreamElement<Out>::watermark(adv.watermark));
                    }
                } else {
                    auto adv =
                        align.on_barrier(1, ctx.apply_barrier_mode_override(el.as_barrier()));
                    if (adv.forward) {
                        if (adv.unaligned_first && can_unalign && !adv.barrier.is_terminal()) {
                            capture_main_inflight();
                        }
                        out_channel->push(StreamElement<Out>::barrier(adv.barrier));
                    }
                }
            };

            auto handle_main = [&](const StreamElement<Main>& el) {
                if (el.is_data()) {
                    std::vector<Out> emitted;
                    for (const auto& rec : el.as_data()) {
                        process_main(rec.value(), state, emitted);
                        emit_buffer(emitted, rec.event_time());
                    }
                } else if (el.is_watermark()) {
                    if (auto adv = align.on_watermark(0, el.as_watermark()); adv.forward) {
                        out_channel->push(StreamElement<Out>::watermark(adv.watermark));
                    }
                } else {
                    auto adv =
                        align.on_barrier(0, ctx.apply_barrier_mode_override(el.as_barrier()));
                    if (adv.forward) {
                        if (adv.unaligned_first && can_unalign && !adv.barrier.is_terminal()) {
                            capture_brod_inflight();
                        }
                        out_channel->push(StreamElement<Out>::barrier(adv.barrier));
                    }
                }
            };

            // Restore in-flight buffers persisted by a prior run, replayed
            // ahead of the live channels (mirrors add_co_operator).
            std::deque<StreamElement<Main>> pending_main;
            std::deque<StreamElement<Brod>> pending_brod;
            if (can_unalign) {
                if (auto stored =
                        ctx.state_backend()->get(id, StateBackend::KeyView{kMainInflight});
                    stored.has_value()) {
                    auto recs = Dag::deserialize_records_<Main>(
                        std::span<const std::byte>{stored->data(), stored->size()}, *main_codec);
                    for (auto& r : recs) {
                        Batch<Main> b;
                        b.push(std::move(r));
                        pending_main.push_back(StreamElement<Main>::data(std::move(b)));
                    }
                    ctx.state_backend()->erase(id, StateBackend::KeyView{kMainInflight});
                }
                if (auto stored =
                        ctx.state_backend()->get(id, StateBackend::KeyView{kBrodInflight});
                    stored.has_value()) {
                    auto recs = Dag::deserialize_records_<Brod>(
                        std::span<const std::byte>{stored->data(), stored->size()}, *brod_codec);
                    for (auto& r : recs) {
                        Batch<Brod> b;
                        b.push(std::move(r));
                        pending_brod.push_back(StreamElement<Brod>::data(std::move(b)));
                    }
                    ctx.state_backend()->erase(id, StateBackend::KeyView{kBrodInflight});
                }
            }

            while (!should_stop()) {
                bool any_progress = false;
                if (!align.input_paused(0)) {
                    if (!pending_main.empty()) {
                        auto m = std::move(pending_main.front());
                        pending_main.pop_front();
                        any_progress = true;
                        handle_main(m);
                    } else if (auto m = main_ch->try_pop(); m.has_value()) {
                        any_progress = true;
                        handle_main(*m);
                    } else if (main_ch->closed()) {
                        align.on_input_closed(0);
                        if (auto wm = align.refresh_watermark(); wm.forward) {
                            out_channel->push(StreamElement<Out>::watermark(wm.watermark));
                        }
                    }
                }
                if (!align.input_paused(1)) {
                    if (!pending_brod.empty()) {
                        auto b = std::move(pending_brod.front());
                        pending_brod.pop_front();
                        any_progress = true;
                        handle_brod(b);
                    } else if (auto b = brod_ch->try_pop(); b.has_value()) {
                        any_progress = true;
                        handle_brod(*b);
                    } else if (brod_ch->closed()) {
                        align.on_input_closed(1);
                        if (auto wm = align.refresh_watermark(); wm.forward) {
                            out_channel->push(StreamElement<Out>::watermark(wm.watermark));
                        }
                    }
                }
                if (align.all_closed()) {
                    break;
                }
                if (!any_progress) {
                    std::this_thread::sleep_for(1ms);
                }
            }
            out_channel->close();
        };
        runner.cancel = [main_ch, brod_ch, out_channel] {
            main_ch->close();
            brod_ch->close();
            out_channel->close();
        };
        runner.input_depth = [main_ch, brod_ch] { return main_ch->size() + brod_ch->size(); };
        runner.input_capacity = [main_ch, brod_ch] {
            return main_ch->capacity() + brod_ch->capacity();
        };
        runner.input_high_water = [main_ch, brod_ch] {
            return std::max(main_ch->high_water_mark(), brod_ch->high_water_mark());
        };

        runners_.push_back(std::move(runner));
        return StageHandle<Out>{out_channel, runners_.size() - 1};
    }

    // ---- CoOperator (two heterogeneous inputs, single output) ----------
    //
    // The clink the CoProcessFunction. Element-wise
    // dispatch: left elements go to op->process_element1, right elements
    // to op->process_element2. Watermarks downstream are min(left, right)
    // via MultiInputAlignment; barriers are Chandy-Lamport aligned.
    //
    // Unlike interval_join the runtime does not buffer records; the user
    // owns coordination, typically through shared keyed state pulled from
    // runtime()->keyed_state<>().
    template <typename In1, typename In2, typename Out>
    StageHandle<Out> add_co_operator(StageHandle<In1> left,
                                     StageHandle<In2> right,
                                     std::shared_ptr<CoOperator<In1, In2, Out>> op,
                                     std::optional<Codec<In1>> in1_codec = std::nullopt,
                                     std::optional<Codec<In2>> in2_codec = std::nullopt) {
        auto out_channel =
            std::make_shared<BoundedChannel<StreamElement<Out>>>(default_channel_capacity_);
        const OperatorId id = derive_id_with_uid_(*op);
        op->set_id(id);
        auto left_ch = left.output;
        auto right_ch = right.output;

        detail::OperatorRunner runner;
        runner.name = op->name();
        runner.id = id;
        auto side_channels = side_channels_ptr_(runners_.size());
        runner.run = [op, left_ch, right_ch, out_channel, side_channels, id, in1_codec, in2_codec](
                         RuntimeContext& ctx, const std::function<bool()>& should_stop) {
            using namespace std::chrono_literals;
            op->attach_runtime(&ctx);
            // Reload checkpointed timers (same-parallelism restore) before
            // open(); both inputs share the one per-operator TimerService.
            if (ctx.has_state_backend()) {
                op->restore_timers(*ctx.state_backend(), id);
            }
            op->open();
            // Per-subtask async snapshot worker (FileBacked + disk-backed
            // changelog). Same contract as the single-input runner: capture
            // on this thread, forward the barrier, persist + ack off-thread.
            std::unique_ptr<SnapshotWorker> snap_worker;
            if (ctx.has_state_backend() && ctx.state_backend()->supports_async_persist()) {
                snap_worker = std::make_unique<SnapshotWorker>();
                snap_worker->start();
            }
            Emitter<Out> out_emitter(out_channel.get());
            MultiInputAlignment align(2);
            // Unaligned-checkpoint in-flight capture. When the first
            // barrier arrives on one input, we snapshot immediately and
            // forward; the OTHER input's already-queued pre-barrier
            // records would be lost on restore (the upstream is past its
            // own barrier and won't replay them), so we drain them into a
            // reserved state slot that the snapshot then captures, and
            // replay them on restore. Mirrors interval_join. Gated on
            // both codecs + a state backend; otherwise the barrier still
            // forwards (the coordinator owns the mode) but the unpaused
            // input's records roll into the next epoch instead.
            static constexpr const char* kLeftInflight = "__co_op_left_inflight__";
            static constexpr const char* kRightInflight = "__co_op_right_inflight__";
            const bool can_unalign =
                in1_codec.has_value() && in2_codec.has_value() && ctx.has_state_backend();
            auto* timers = ctx.timer_service();
            auto fire_due = [&] {
                timers->poll_due(timers->now_ms(), [&](std::int64_t ts, const std::string& key) {
                    op->on_processing_time_timer(ts, key, out_emitter);
                });
            };

            auto snapshot_and_ack = [&](CheckpointBarrier barrier) {
                if (!ctx.has_state_backend()) {
                    op->on_barrier(barrier, out_emitter);
                    return;
                }
                auto* backend = ctx.state_backend();
                // Any in-flight capture has already put() its rows into the
                // backend before this runs (see handle_left/handle_right),
                // so capture()/snapshot() here includes them.
                if (snap_worker) {
                    std::string err;
                    bool ok = true;
                    CaptureHandle handle;
                    try {
                        op->snapshot_timers(*backend, id);
                        handle = backend->capture(barrier.id());
                    } catch (const std::exception& e) {
                        ok = false;
                        err = e.what();
                    }
                    op->on_barrier(barrier, out_emitter);
                    if (!ok) {
                        if (const auto& cb = ctx.checkpoint_ack(); cb) {
                            cb(barrier.id(), false, std::move(err));
                        }
                    } else {
                        snap_worker->enqueue(SnapshotWorker::Job{.handle = std::move(handle),
                                                                 .backend = backend,
                                                                 .ack = ctx.checkpoint_ack()});
                    }
                } else {
                    std::string err;
                    bool ok = true;
                    try {
                        op->snapshot_timers(*backend, id);
                        backend->snapshot(barrier.id());
                    } catch (const std::exception& e) {
                        ok = false;
                        err = e.what();
                    }
                    op->on_barrier(barrier, out_emitter);
                    if (const auto& cb = ctx.checkpoint_ack(); cb) {
                        cb(barrier.id(), ok, std::move(err));
                    }
                }
            };
            // Drain the still-pending input's queued data records into a
            // reserved state slot BEFORE snapshot_and_ack runs snapshot(),
            // so the in-flight is captured into this checkpoint. Keep only
            // data records: a same-id barrier popped here is absorbed by
            // the aligner anyway (unaligned forwards on first delivery),
            // and watermarks re-establish from the next one.
            auto capture_right_inflight = [&] {
                std::vector<Record<In2>> inflight;
                while (auto m = right_ch->try_pop()) {
                    if (m->is_data()) {
                        for (auto& r : m->as_data()) {
                            inflight.push_back(std::move(r));
                        }
                    }
                }
                auto bytes = Dag::serialize_records_(inflight, *in2_codec);
                ctx.state_backend()->put(
                    id,
                    StateBackend::KeyView{kRightInflight},
                    StateBackend::ValueView{reinterpret_cast<const char*>(bytes.data()),
                                            bytes.size()});
            };
            auto capture_left_inflight = [&] {
                std::vector<Record<In1>> inflight;
                while (auto m = left_ch->try_pop()) {
                    if (m->is_data()) {
                        for (auto& r : m->as_data()) {
                            inflight.push_back(std::move(r));
                        }
                    }
                }
                auto bytes = Dag::serialize_records_(inflight, *in1_codec);
                ctx.state_backend()->put(
                    id,
                    StateBackend::KeyView{kLeftInflight},
                    StateBackend::ValueView{reinterpret_cast<const char*>(bytes.data()),
                                            bytes.size()});
            };

            auto handle_left = [&](const StreamElement<In1>& el) {
                if (el.is_data()) {
                    op->process_element1(el, out_emitter);
                } else if (el.is_watermark()) {
                    if (auto adv = align.on_watermark(0, el.as_watermark()); adv.forward) {
                        op->on_watermark(adv.watermark, out_emitter);
                    }
                } else {
                    auto adv =
                        align.on_barrier(0, ctx.apply_barrier_mode_override(el.as_barrier()));
                    if (adv.forward) {
                        if (adv.unaligned_first && can_unalign && !adv.barrier.is_terminal()) {
                            capture_right_inflight();
                        }
                        snapshot_and_ack(adv.barrier);
                    }
                }
            };
            auto handle_right = [&](const StreamElement<In2>& el) {
                if (el.is_data()) {
                    op->process_element2(el, out_emitter);
                } else if (el.is_watermark()) {
                    if (auto adv = align.on_watermark(1, el.as_watermark()); adv.forward) {
                        op->on_watermark(adv.watermark, out_emitter);
                    }
                } else {
                    auto adv =
                        align.on_barrier(1, ctx.apply_barrier_mode_override(el.as_barrier()));
                    if (adv.forward) {
                        if (adv.unaligned_first && can_unalign && !adv.barrier.is_terminal()) {
                            capture_left_inflight();
                        }
                        snapshot_and_ack(adv.barrier);
                    }
                }
            };

            // Restore in-flight buffers persisted by a prior run. Held in
            // local pending queues (not pushed back into the upstream
            // channels, which may already be closed) and drained ahead of
            // the live channels in the poll loop below.
            std::deque<StreamElement<In1>> pending_left;
            std::deque<StreamElement<In2>> pending_right;
            if (can_unalign) {
                if (auto stored =
                        ctx.state_backend()->get(id, StateBackend::KeyView{kLeftInflight});
                    stored.has_value()) {
                    auto recs = Dag::deserialize_records_<In1>(
                        std::span<const std::byte>{stored->data(), stored->size()}, *in1_codec);
                    for (auto& r : recs) {
                        Batch<In1> b;
                        b.push(std::move(r));
                        pending_left.push_back(StreamElement<In1>::data(std::move(b)));
                    }
                    ctx.state_backend()->erase(id, StateBackend::KeyView{kLeftInflight});
                }
                if (auto stored =
                        ctx.state_backend()->get(id, StateBackend::KeyView{kRightInflight});
                    stored.has_value()) {
                    auto recs = Dag::deserialize_records_<In2>(
                        std::span<const std::byte>{stored->data(), stored->size()}, *in2_codec);
                    for (auto& r : recs) {
                        Batch<In2> b;
                        b.push(std::move(r));
                        pending_right.push_back(StreamElement<In2>::data(std::move(b)));
                    }
                    ctx.state_backend()->erase(id, StateBackend::KeyView{kRightInflight});
                }
            }

            while (!should_stop()) {
                fire_due();
                bool any_progress = false;
                if (!align.input_paused(0)) {
                    if (!pending_left.empty()) {
                        auto m = std::move(pending_left.front());
                        pending_left.pop_front();
                        any_progress = true;
                        handle_left(m);
                    } else if (auto m = left_ch->try_pop(); m.has_value()) {
                        any_progress = true;
                        handle_left(*m);
                    } else if (left_ch->closed()) {
                        align.on_input_closed(0);
                        if (auto wm = align.refresh_watermark(); wm.forward) {
                            op->on_watermark(wm.watermark, out_emitter);
                        }
                    }
                }
                if (!align.input_paused(1)) {
                    if (!pending_right.empty()) {
                        auto m = std::move(pending_right.front());
                        pending_right.pop_front();
                        any_progress = true;
                        handle_right(m);
                    } else if (auto m = right_ch->try_pop(); m.has_value()) {
                        any_progress = true;
                        handle_right(*m);
                    } else if (right_ch->closed()) {
                        align.on_input_closed(1);
                        if (auto wm = align.refresh_watermark(); wm.forward) {
                            op->on_watermark(wm.watermark, out_emitter);
                        }
                    }
                }
                if (align.all_closed()) {
                    break;
                }
                if (!any_progress) {
                    // Sleep at most 1ms; if a timer is due sooner than
                    // that, clamp to the timer's remaining delay so we
                    // wake just in time to fire it (capped at 0 so an
                    // already-due timer fires immediately on the next
                    // iteration).
                    auto sleep_ms = 1ms;
                    if (auto next = timers->next_timestamp(); next.has_value()) {
                        const auto delay = *next - timers->now_ms();
                        if (delay < 1) {
                            sleep_ms = std::chrono::milliseconds(delay > 0 ? delay : 0);
                        }
                    }
                    std::this_thread::sleep_for(sleep_ms);
                }
            }
            // Wind down the async snapshot worker (clean drain on EOS, drop
            // on cancel) before closing the output channel. See the
            // single-input runner for the rationale.
            if (snap_worker) {
                if (should_stop()) {
                    snap_worker->cancel_and_join();
                } else {
                    snap_worker->drain_and_join();
                }
            }
            fire_due();
            op->flush(out_emitter);
            op->close();
            op->attach_runtime(nullptr);
            out_channel->close();
            if (side_channels) {
                for (auto& [_, entry] : *side_channels) {
                    if (entry.close_fn) {
                        entry.close_fn();
                    }
                }
            }
        };
        runner.cancel = [left_ch, right_ch, out_channel] {
            left_ch->close();
            right_ch->close();
            out_channel->close();
        };
        runner.input_depth = [left_ch, right_ch] { return left_ch->size() + right_ch->size(); };
        runner.input_capacity = [left_ch, right_ch] {
            return left_ch->capacity() + right_ch->capacity();
        };
        runner.input_high_water = [left_ch, right_ch] {
            return std::max(left_ch->high_water_mark(), right_ch->high_water_mark());
        };

        runners_.push_back(std::move(runner));
        operators_.push_back(op);
        return StageHandle<Out>{out_channel, runners_.size() - 1};
    }

    // ---- Parallel stages -----------------------------------------------
    //
    // Operator parallelism: each stage can run with N parallel subtasks,
    // each in its own thread, each with its own operator instance and its
    // own RuntimeContext (and thus its own keyed state namespace). Three
    // edge layouts:
    //
    //   * **Forward** (N == M): allocate N channels; subtask i feeds the
    //     same-indexed downstream subtask. No alignment needed downstream
    //     (single input). This is the default.
    //
    //   * **Hash shuffle** (N → M with partitioner): allocate N×M channels.
    //     Each upstream subtask writes to all M outputs and the partitioner
    //     decides which one. Each downstream subtask reads from N inputs
    //     and aligns watermarks/barriers across them via
    //     MultiInputAlignment. Use `add_parallel_operator_shuffled` to
    //     install a shuffle.
    //
    //   * **Fan-in** (N → 1): degenerate hash shuffle with M=1. The single
    //     downstream subtask still aligns over N inputs. Used for sinks.
    //
    // Operators must be supplied as factories - each subtask gets its own
    // instance so per-subtask state stays isolated.
    template <typename T>
    struct ParallelStageHandle {
        std::size_t parallelism{1};
        // Per-subtask emitter; mutated when downstream is added.
        std::vector<std::shared_ptr<SubtaskEmitter<T>>> emitters;
        std::vector<OperatorId> subtask_ids;
    };

    template <typename T>
    using ParallelChannelGrid = std::vector<std::shared_ptr<BoundedChannel<StreamElement<T>>>>;

    // Source factory variant. Most sources are parallelism=1 in this MVP
    // (real partition-aware parallel sources need connector-specific
    // partition assignment, which is on the roadmap).
    template <typename T>
    ParallelStageHandle<T> add_parallel_source(
        std::function<std::shared_ptr<Source<T>>(std::size_t /*subtask*/)> factory,
        std::size_t parallelism = 1) {
        ParallelStageHandle<T> handle;
        handle.parallelism = parallelism;
        handle.emitters.reserve(parallelism);
        handle.subtask_ids.reserve(parallelism);
        for (std::size_t i = 0; i < parallelism; ++i) {
            auto source = factory(i);
            const std::string subtask_name = source->name() + "/sub" + std::to_string(i);
            const OperatorId id = derive_id(subtask_name);
            source->set_id(id);
            sources_.push_back(source);
            source_ids_.push_back(id);
            source_bounded_.push_back(source->is_bounded() ? 1 : 0);
            source_split_counts_.push_back(source->split_count());
            handle.subtask_ids.push_back(id);

            auto emitter = std::make_shared<SubtaskEmitter<T>>();
            handle.emitters.push_back(emitter);

            // Source injector: hand the barrier to the source's pending
            // queue. The runner loop drains it inline so snapshot_offset
            // and the barrier emission happen on the same thread as
            // produce(), eliminating the offset-vs-emit race.
            source_injectors_.push_back(
                [source](CheckpointBarrier b) { source->inject_pending_barrier(b); });

            detail::OperatorRunner runner;
            runner.name = subtask_name;
            runner.id = id;
            const std::uint32_t subtask_idx_for_drain = static_cast<std::uint32_t>(i);
            runner.run = [source, emitter, id, subtask_idx_for_drain](
                             RuntimeContext& ctx, const std::function<bool()>& should_stop) {
                source->attach_runtime(&ctx);
                if (ctx.has_state_backend()) {
                    source->restore_offset(*ctx.state_backend(), id);
                }
                source->open();
                Emitter<T> typed_emitter(emitter.get());
                // Phase 26a: same source-side stamping as the
                // single-subtask path. Sources emit mode-agnostic
                // barriers; the runner stamps from JobConfig.
                typed_emitter.set_default_barrier_mode(ctx.barrier_mode_override().value_or(
                    ctx.unaligned_checkpoints() ? CheckpointBarrier::Mode::Unaligned
                                                : CheckpointBarrier::Mode::Aligned));
                const auto& ack_cb = ctx.checkpoint_ack();
                auto drain_pending_barriers = [&]() {
                    while (auto b = source->take_pending_barrier()) {
                        if (ctx.has_state_backend()) {
                            source->snapshot_offset(*ctx.state_backend(), id, b->id());
                        }
                        typed_emitter.emit_barrier(*b);
                        if (ack_cb) {
                            ack_cb(b->id(), true, std::string{});
                        }
                    }
                };
                drain_pending_barriers();
                bool drained = false;
                while (!should_stop() && source->produce(typed_emitter)) {
                    drain_pending_barriers();
                    // Phase 29d-3: rescale-driven drain. The TM's
                    // BeginRescale dispatch sets ctx.drain_target() to
                    // the rescale's target_parallelism via the
                    // JobConfig-threaded shared atomic; here the
                    // produce loop polls between iterations. When
                    // non-zero, emit a DrainMarker so downstream
                    // consumers know this upstream subtask is leaving
                    // and the rescaled set is taking over for its
                    // key-groups, then bail. The runner thread
                    // unwinds; the TM sees SubtaskFinished + signals
                    // the JM, which calls mark_old_drained on its
                    // RescaleCoordinator.
                    if (auto t = ctx.drain_target(); t > 0) {
                        typed_emitter.emit_drain(DrainMarker{.subtask_idx = subtask_idx_for_drain,
                                                             .target_parallelism = t});
                        drained = true;
                        break;
                    }
                }
                drain_pending_barriers();
                if (!should_stop() && !drained) {
                    source->flush(typed_emitter);
                    // BATCH-1 end-of-input drain (parallel-source path): same
                    // contract as the single-source runner. A bounded source
                    // that exhausted cleanly (not cancelled, not rescale-drained)
                    // emits a max watermark so downstream event-time windows and
                    // timers fire before the channels close.
                    if (source->is_bounded()) {
                        typed_emitter.emit_watermark(Watermark::max());
                    }
                }
                source->close();
                source->attach_runtime(nullptr);
                // Close all downstream channels so consumers drain and
                // exit; without this the parallel pipeline hangs forever
                // after the source exhausts its records.
                emitter->close_all();
            };
            runner.cancel = [source, emitter] {
                source->cancel();
                // Close all output channels owned by this subtask.
                emitter->close_all();
            };
            runner.input_depth = [] { return std::size_t{0}; };
            runner.input_capacity = [] { return std::size_t{0}; };
            runner.input_high_water = [] { return std::size_t{0}; };

            runners_.push_back(std::move(runner));
        }
        return handle;
    }

    // Forward operator: parallelism must equal upstream's. Each subtask gets
    // a single 1:1 channel from its same-indexed upstream subtask.
    template <typename In, typename Out>
    ParallelStageHandle<Out> add_parallel_operator(
        ParallelStageHandle<In> upstream,
        std::function<std::shared_ptr<Operator<In, Out>>(std::size_t /*subtask*/)> factory,
        std::size_t parallelism) {
        if (parallelism != upstream.parallelism) {
            throw std::invalid_argument(
                "add_parallel_operator: parallelism must equal upstream "
                "(use add_parallel_operator_shuffled for cross-parallelism edges)");
        }
        return wire_stage_<In, Out>(upstream,
                                    std::move(factory),
                                    parallelism,
                                    /*shuffle=*/false,
                                    /*partitioner=*/{});
    }

    // Shuffled operator: the upstream emitters are wired with a partitioner
    // and N×M channels are allocated. Each downstream subtask reads from N
    // input channels and aligns over them.
    template <typename In, typename Out>
    ParallelStageHandle<Out> add_parallel_operator_shuffled(
        ParallelStageHandle<In> upstream,
        std::function<std::shared_ptr<Operator<In, Out>>(std::size_t /*subtask*/)> factory,
        std::size_t parallelism,
        std::function<std::size_t(const In&)> partitioner) {
        if (!partitioner) {
            throw std::invalid_argument("add_parallel_operator_shuffled: partitioner is required");
        }
        return wire_stage_<In, Out>(upstream,
                                    std::move(factory),
                                    parallelism,
                                    /*shuffle=*/true,
                                    std::move(partitioner));
    }

    // Sink: forward (M==N) or fan-in (M==1) only. No keyed routing.
    template <typename T>
    void add_parallel_sink(ParallelStageHandle<T> upstream,
                           std::function<std::shared_ptr<Sink<T>>(std::size_t /*subtask*/)> factory,
                           std::size_t parallelism = 1) {
        const bool forward = (parallelism == upstream.parallelism);
        const bool fan_in = (parallelism == 1);
        if (!forward && !fan_in) {
            throw std::invalid_argument(
                "add_parallel_sink: parallelism must equal upstream or be 1 (fan-in)");
        }
        const std::size_t N = upstream.parallelism;
        const std::size_t M = parallelism;

        // Allocate channels.
        ParallelChannelGrid<T> channels;
        if (forward) {
            channels.reserve(N);
            for (std::size_t i = 0; i < N; ++i) {
                channels.push_back(
                    std::make_shared<BoundedChannel<StreamElement<T>>>(default_channel_capacity_));
            }
        } else {
            // Fan-in: N×1 = N channels.
            channels.reserve(N);
            for (std::size_t i = 0; i < N; ++i) {
                channels.push_back(
                    std::make_shared<BoundedChannel<StreamElement<T>>>(default_channel_capacity_));
            }
        }

        // Wire upstream emitters.
        for (std::size_t i = 0; i < N; ++i) {
            std::vector<std::shared_ptr<BoundedChannel<StreamElement<T>>>> outs;
            outs.push_back(channels[i]);
            upstream.emitters[i]->attach(std::move(outs), {});
        }

        // Build sink subtask runners.
        for (std::size_t j = 0; j < M; ++j) {
            std::vector<std::shared_ptr<BoundedChannel<StreamElement<T>>>> ins;
            if (forward) {
                ins.push_back(channels[j]);
            } else {
                // Fan-in: this single sink subtask reads from all N channels.
                for (std::size_t i = 0; i < N; ++i) {
                    ins.push_back(channels[i]);
                }
            }

            auto sink = factory(j);
            const std::string subtask_name = sink->name() + "/sub" + std::to_string(j);
            const OperatorId id = derive_id(subtask_name);
            sink->set_id(id);
            sinks_.push_back(sink);

            detail::OperatorRunner runner;
            runner.name = subtask_name;
            runner.id = id;
            runner.run = [sink, ins](RuntimeContext& ctx,
                                     const std::function<bool()>& should_stop) {
                using namespace std::chrono_literals;
                sink->attach_runtime(&ctx);
                sink->open();
                MultiInputAlignment align(ins.size());
                while (!should_stop()) {
                    bool any_progress = false;
                    for (std::size_t k = 0; k < ins.size(); ++k) {
                        if (align.input_paused(k)) {
                            continue;
                        }
                        auto m = ins[k]->try_pop();
                        if (!m.has_value()) {
                            if (ins[k]->closed()) {
                                align.on_input_closed(k);
                            }
                            continue;
                        }
                        any_progress = true;
                        if (m->is_data()) {
                            sink->on_data(m->as_data());
                        } else if (m->is_watermark()) {
                            if (auto adv = align.on_watermark(k, m->as_watermark()); adv.forward) {
                                sink->on_watermark(adv.watermark);
                            }
                        } else if (m->is_drain()) {
                            // A rescale drain is a no-op at a terminal sink
                            // (ignored, not mis-read as a barrier via as_barrier).
                        } else {
                            if (auto adv = align.on_barrier(
                                    k, ctx.apply_barrier_mode_override(m->as_barrier()));
                                adv.forward) {
                                sink->on_barrier(adv.barrier);
                            }
                        }
                    }
                    if (align.all_closed()) {
                        break;
                    }
                    if (!any_progress) {
                        std::this_thread::sleep_for(1ms);
                    }
                }
                sink->flush();
                sink->close();
                sink->attach_runtime(nullptr);
            };
            runner.cancel = [ins] {
                for (auto& ch : ins) {
                    ch->close();
                }
            };
            const auto ins_copy = ins;
            runner.input_depth = [ins_copy] {
                std::size_t s = 0;
                for (auto& c : ins_copy) {
                    s += c->size();
                }
                return s;
            };
            runner.input_capacity = [ins_copy] {
                std::size_t s = 0;
                for (auto& c : ins_copy) {
                    s += c->capacity();
                }
                return s;
            };
            runner.input_high_water = [ins_copy] {
                std::size_t hw = 0;
                for (auto& c : ins_copy) {
                    hw = std::max(hw, c->high_water_mark());
                }
                return hw;
            };
            runners_.push_back(std::move(runner));
        }
    }

    // Parallel-stage multi-consumer fan-out. Mirrors the single-handle
    // `fork<T>` shape: every record on the producer's main output is
    // broadcast to all M consumer branches.
    //
    // Shape: takes a `ParallelStageHandle<T>` of parallelism N, returns
    // M `ParallelStageHandle<T>`s each of parallelism N. Each branch's
    // emitters are fresh `SubtaskEmitter<T>`s the caller's downstream
    // `add_parallel_*` will attach to.
    //
    // Wiring: per upstream subtask i, allocate one forward channel and
    // attach `upstream.emitters[i]` to it. Spin up one fork-subtask
    // runner per i that drains the channel and broadcasts each
    // StreamElement<T> (data / watermark / barrier) to M emitters -
    // the m-th of which is `branches[m].emitters[i]`. So consumer m
    // sees a complete copy of subtask i's stream on emitters[i],
    // matching the routing layout downstream expects.
    template <typename T>
    std::vector<ParallelStageHandle<T>> fork_parallel(ParallelStageHandle<T> upstream,
                                                      std::size_t m) {
        if (m < 2) {
            throw std::invalid_argument(
                "Dag::fork_parallel: branch count must be >= 2 (use the producer's "
                "handle directly for a single consumer).");
        }
        const std::size_t N = upstream.parallelism;
        if (N == 0 || upstream.emitters.size() != N) {
            throw std::invalid_argument(
                "Dag::fork_parallel: upstream ParallelStageHandle is malformed "
                "(parallelism vs emitter count mismatch).");
        }

        // Per upstream subtask: a single forward channel that upstream
        // writes into. The fork-subtask runner drains this channel.
        std::vector<std::shared_ptr<BoundedChannel<StreamElement<T>>>> in_channels;
        in_channels.reserve(N);
        for (std::size_t i = 0; i < N; ++i) {
            in_channels.push_back(
                std::make_shared<BoundedChannel<StreamElement<T>>>(default_channel_capacity_));
        }
        for (std::size_t i = 0; i < N; ++i) {
            upstream.emitters[i]->attach({in_channels[i]}, {});
        }

        // Build the M output handles. Each branch.emitters[i] is the
        // SubtaskEmitter<T> the fork-subtask runner emits into for
        // branch m; downstream `add_parallel_*` will attach channels
        // to it later.
        std::vector<ParallelStageHandle<T>> branches(m);
        for (std::size_t mi = 0; mi < m; ++mi) {
            branches[mi].parallelism = N;
            branches[mi].emitters.reserve(N);
            branches[mi].subtask_ids.reserve(N);
        }

        for (std::size_t i = 0; i < N; ++i) {
            const OperatorId id = derive_id("fork_parallel/sub" + std::to_string(i));

            std::vector<std::shared_ptr<SubtaskEmitter<T>>> per_branch_emitters;
            per_branch_emitters.reserve(m);
            for (std::size_t mi = 0; mi < m; ++mi) {
                auto e = std::make_shared<SubtaskEmitter<T>>();
                per_branch_emitters.push_back(e);
                branches[mi].emitters.push_back(e);
                branches[mi].subtask_ids.push_back(id);
            }

            auto in_ch = in_channels[i];
            detail::OperatorRunner runner;
            runner.name = "fork_parallel/sub" + std::to_string(i);
            runner.id = id;
            runner.run = [in_ch, per_branch_emitters](RuntimeContext& /*ctx*/,
                                                      const std::function<bool()>& should_stop) {
                while (!should_stop()) {
                    auto maybe = in_ch->pop();
                    if (!maybe.has_value()) {
                        break;
                    }
                    if (maybe->is_data()) {
                        // Copy the batch to every branch except the last;
                        // move into the last to avoid one needless copy.
                        for (std::size_t mi = 0; mi + 1 < per_branch_emitters.size(); ++mi) {
                            Batch<T> copy = maybe->as_data();
                            per_branch_emitters[mi]->emit_data(std::move(copy));
                        }
                        per_branch_emitters.back()->emit_data(std::move(maybe->as_data()));
                    } else if (maybe->is_watermark()) {
                        for (auto& e : per_branch_emitters) {
                            e->emit_watermark(maybe->as_watermark());
                        }
                    } else {
                        for (auto& e : per_branch_emitters) {
                            e->emit_barrier(maybe->as_barrier());
                        }
                    }
                }
                for (auto& e : per_branch_emitters) {
                    e->close_all();
                }
            };
            runner.cancel = [in_ch, per_branch_emitters] {
                in_ch->close();
                for (auto& e : per_branch_emitters) {
                    e->close_all();
                }
            };
            runner.input_depth = [in_ch] { return in_ch->size(); };
            runner.input_capacity = [in_ch] { return in_ch->capacity(); };
            runner.input_high_water = [in_ch] { return in_ch->high_water_mark(); };

            runners_.push_back(std::move(runner));
        }

        return branches;
    }

    // ---- Sink ------------------------------------------------------------
    template <typename In>
    StageHandle<In> add_sink(StageHandle<In> upstream, std::shared_ptr<Sink<In>> sink) {
        const OperatorId id = derive_id_with_uid_(*sink);
        sink->set_id(id);
        sinks_.push_back(sink);
        auto in_channel = upstream.output;

        detail::OperatorRunner runner;
        runner.name = sink->name();
        runner.id = id;
        runner.run = [sink, in_channel](RuntimeContext& ctx,
                                        const std::function<bool()>& should_stop) {
            sink->attach_runtime(&ctx);
            sink->open();
            // Per-subtask async snapshot worker (FileBacked + disk-backed
            // changelog). Only the non-terminal snapshot+ack path uses it;
            // terminal barriers stay fully synchronous (they finalize locally
            // with no JM round-trip).
            std::unique_ptr<SnapshotWorker> snap_worker;
            if (ctx.has_state_backend() && ctx.state_backend()->supports_async_persist()) {
                snap_worker = std::make_unique<SnapshotWorker>();
                snap_worker->start();
            }
            while (!should_stop()) {
                auto maybe = in_channel->pop();
                if (!maybe.has_value()) {
                    break;
                }
                if (maybe->is_data()) {
                    // Pass by rvalue so move-aware sinks (e.g.
                    // NetworkBridgeSink forwarding to a channel) can
                    // take ownership instead of deep-copying.
                    sink->on_data(std::move(maybe->as_data()));
                } else if (maybe->is_watermark()) {
                    sink->on_watermark(maybe->as_watermark());
                } else if (maybe->is_drain()) {
                    // A drain (rescale wind-down of an upstream subtask) is a
                    // no-op at a terminal sink: it keeps consuming the rescaled
                    // stream. Ignored rather than mis-read as a barrier (the
                    // old else-branch called as_barrier() and threw on a drain).
                } else if (maybe->is_barrier()) {
                    // Snapshot any state slice the sink has registered,
                    // hand the barrier to the sink for user hooks, then
                    // ack so the JM coordinator can complete the
                    // checkpoint once every subtask has reported.
                    const auto& barrier = maybe->as_barrier();
                    const auto ckpt_id = barrier.id();
                    if (snap_worker && ctx.has_state_backend() && !barrier.is_terminal()) {
                        // Async path: capture on this thread, run the user
                        // barrier hook, persist + ack off-thread. The ack
                        // fires after persist returns (ack-after-durable).
                        std::string err;
                        bool ok = true;
                        CaptureHandle handle;
                        try {
                            handle = ctx.state_backend()->capture(ckpt_id);
                        } catch (const std::exception& e) {
                            ok = false;
                            err = e.what();
                        }
                        sink->on_barrier(barrier);
                        if (!ok) {
                            if (const auto& cb = ctx.checkpoint_ack(); cb) {
                                cb(ckpt_id, false, std::move(err));
                            }
                        } else {
                            snap_worker->enqueue(SnapshotWorker::Job{.handle = std::move(handle),
                                                                     .backend = ctx.state_backend(),
                                                                     .ack = ctx.checkpoint_ack()});
                        }
                    } else {
                        // Synchronous path: terminal barriers (always - they
                        // commit locally with no JM round-trip) and
                        // non-async backends.
                        std::string err;
                        bool ok = true;
                        if (ctx.has_state_backend() && !barrier.is_terminal()) {
                            try {
                                ctx.state_backend()->snapshot(ckpt_id);
                            } catch (const std::exception& e) {
                                ok = false;
                                err = e.what();
                            }
                        }
                        sink->on_barrier(barrier);
                        if (barrier.is_terminal()) {
                            // Terminal barriers don't go through the JM
                            // commit broadcast - there's no recovery
                            // scenario after end-of-stream. Commit locally
                            // so the sink finalizes its pre-committed
                            // transaction before the runner exits.
                            sink->on_commit(ckpt_id.value());
                        } else if (const auto& cb = ctx.checkpoint_ack(); cb) {
                            cb(ckpt_id, ok, std::move(err));
                        }
                    }
                }
            }
            // Wind down the async snapshot worker before flush/close so any
            // in-flight checkpoint persists + acks (clean EOS) or is dropped
            // without acking (cancel).
            if (snap_worker) {
                if (should_stop()) {
                    snap_worker->cancel_and_join();
                } else {
                    snap_worker->drain_and_join();
                }
            }
            if (!should_stop()) {
                sink->flush();
            }
            sink->close();
            sink->attach_runtime(nullptr);
        };
        runner.cancel = [in_channel] { in_channel->close(); };
        runner.input_depth = [in_channel] { return in_channel->size(); };
        runner.input_capacity = [in_channel] { return in_channel->capacity(); };
        runner.input_high_water = [in_channel] { return in_channel->high_water_mark(); };

        runners_.push_back(std::move(runner));
        return StageHandle<In>{nullptr, runners_.size() - 1};
    }

    const std::vector<detail::OperatorRunner>& runners() const noexcept { return runners_; }

    std::size_t operator_count() const noexcept { return runners_.size(); }

    // Type-erased per-source barrier injectors. Used by
    // CheckpointCoordinator::start_periodic_trigger to push barriers into
    // every source's outbound channel on a cadence.
    using BarrierInjector = std::function<void(CheckpointBarrier)>;
    const std::vector<BarrierInjector>& source_injectors() const noexcept {
        return source_injectors_;
    }
    const std::vector<OperatorId>& source_operator_ids() const noexcept { return source_ids_; }

    // Number of registered source subtasks (BATCH-1).
    [[nodiscard]] std::size_t source_count() const noexcept { return source_bounded_.size(); }

    // True iff the job is bounded: it has at least one source and every source
    // is bounded, so the dataflow is guaranteed to reach end-of-input and
    // terminate (BATCH-1). This is what the run-to-completion / batch path keys
    // off. Any unbounded source (Kafka, CDC) makes this false - a streaming job
    // that runs until cancelled. A source-less DAG is not bounded (there is no
    // end-of-input to reach).
    [[nodiscard]] bool all_sources_bounded() const noexcept {
        return !source_bounded_.empty() &&
               std::all_of(
                   source_bounded_.begin(), source_bounded_.end(), [](char b) { return b != 0; });
    }

    // Runner indices of blocking-exchange stages (BATCH-2). A batch scheduler
    // (BATCH-3) uses these to identify blocking edges and launch the consumer
    // side only after the producer side completes. Empty for a fully pipelined
    // (streaming) job.
    [[nodiscard]] const std::vector<std::size_t>& blocking_edge_runner_indices() const noexcept {
        return blocking_edge_indices_;
    }

    // Per-source split counts (BATCH-3), parallel to source_operator_ids(). The
    // batch planner derives a recommended parallelism from these.
    [[nodiscard]] const std::vector<std::size_t>& source_split_counts() const noexcept {
        return source_split_counts_;
    }

private:
    // Shared body of add_parallel_operator and ..._shuffled. Allocates the
    // edge channels (forward = N channels diagonal, shuffle = N×M),
    // attaches upstream emitters, builds per-subtask runners with their N
    // input channels and a fresh SubtaskEmitter for downstream wiring.
    template <typename In, typename Out>
    ParallelStageHandle<Out> wire_stage_(
        ParallelStageHandle<In> upstream,
        std::function<std::shared_ptr<Operator<In, Out>>(std::size_t)> factory,
        std::size_t parallelism,
        bool shuffle,
        std::function<std::size_t(const In&)> partitioner) {
        const std::size_t N = upstream.parallelism;
        const std::size_t M = parallelism;

        // Allocate channels.
        ParallelChannelGrid<In> channels;
        if (shuffle) {
            channels.reserve(N * M);
            for (std::size_t i = 0; i < N * M; ++i) {
                channels.push_back(
                    std::make_shared<BoundedChannel<StreamElement<In>>>(default_channel_capacity_));
            }
        } else {
            channels.reserve(N);
            for (std::size_t i = 0; i < N; ++i) {
                channels.push_back(
                    std::make_shared<BoundedChannel<StreamElement<In>>>(default_channel_capacity_));
            }
        }

        // Wire upstream emitters.
        for (std::size_t i = 0; i < N; ++i) {
            std::vector<std::shared_ptr<BoundedChannel<StreamElement<In>>>> outs;
            if (shuffle) {
                outs.reserve(M);
                for (std::size_t j = 0; j < M; ++j) {
                    outs.push_back(channels[(i * M) + j]);
                }
                upstream.emitters[i]->attach(std::move(outs), partitioner);
            } else {
                outs.push_back(channels[i]);
                upstream.emitters[i]->attach(std::move(outs), {});
            }
        }

        // Build the new stage's per-subtask runners.
        ParallelStageHandle<Out> handle;
        handle.parallelism = M;
        handle.emitters.reserve(M);
        handle.subtask_ids.reserve(M);

        for (std::size_t j = 0; j < M; ++j) {
            std::vector<std::shared_ptr<BoundedChannel<StreamElement<In>>>> ins;
            if (shuffle) {
                ins.reserve(N);
                for (std::size_t i = 0; i < N; ++i) {
                    ins.push_back(channels[(i * M) + j]);
                }
            } else {
                ins.push_back(channels[j]);
            }

            auto op = factory(j);
            const std::string subtask_name = op->name() + "/sub" + std::to_string(j);
            const OperatorId id = derive_id(subtask_name);
            op->set_id(id);
            operators_.push_back(op);
            handle.subtask_ids.push_back(id);

            auto stage_emitter = std::make_shared<SubtaskEmitter<Out>>();
            handle.emitters.push_back(stage_emitter);

            detail::OperatorRunner runner;
            runner.name = subtask_name;
            runner.id = id;
            runner.run = [op, ins, stage_emitter](RuntimeContext& ctx,
                                                  const std::function<bool()>& should_stop) {
                using namespace std::chrono_literals;
                op->attach_runtime(&ctx);
                op->open();
                Emitter<Out> out_emitter(stage_emitter.get());
                MultiInputAlignment align(ins.size());
                while (!should_stop()) {
                    bool any_progress = false;
                    for (std::size_t k = 0; k < ins.size(); ++k) {
                        if (align.input_paused(k)) {
                            continue;
                        }
                        auto m = ins[k]->try_pop();
                        if (!m.has_value()) {
                            if (ins[k]->closed()) {
                                align.on_input_closed(k);
                            }
                            continue;
                        }
                        any_progress = true;
                        if (m->is_data()) {
                            // For data, dispatch directly to the operator's
                            // process - alignment buffering doesn't apply.
                            op->process(*m, out_emitter);
                        } else if (m->is_watermark()) {
                            if (auto adv = align.on_watermark(k, m->as_watermark()); adv.forward) {
                                op->on_watermark(adv.watermark, out_emitter);
                            }
                        } else {
                            if (auto adv = align.on_barrier(
                                    k, ctx.apply_barrier_mode_override(m->as_barrier()));
                                adv.forward) {
                                op->on_barrier(adv.barrier, out_emitter);
                            }
                        }
                    }
                    if (align.all_closed()) {
                        break;
                    }
                    if (!any_progress) {
                        std::this_thread::sleep_for(1ms);
                    }
                }
                op->flush(out_emitter);
                op->close();
                op->attach_runtime(nullptr);
                stage_emitter->close_all();
            };
            runner.cancel = [ins, stage_emitter] {
                for (auto& ch : ins) {
                    ch->close();
                }
                stage_emitter->close_all();
            };
            const auto ins_copy = ins;
            runner.input_depth = [ins_copy] {
                std::size_t s = 0;
                for (auto& c : ins_copy) {
                    s += c->size();
                }
                return s;
            };
            runner.input_capacity = [ins_copy] {
                std::size_t s = 0;
                for (auto& c : ins_copy) {
                    s += c->capacity();
                }
                return s;
            };
            runner.input_high_water = [ins_copy] {
                std::size_t hw = 0;
                for (auto& c : ins_copy) {
                    hw = std::max(hw, c->high_water_mark());
                }
                return hw;
            };

            runners_.push_back(std::move(runner));
        }

        return handle;
    }

    // OperatorId derivation. Two paths:
    //
    //   * If the operator has a non-empty uid (set via op->set_uid(...)
    //     before add_*), the id is hash("uid/" + uid). Stable across
    //     topology edits - keyed state restores correctly even when
    //     operators are renamed, reordered, or added/removed around
    //     this one. This is the "stable identifier" path
    //     and is recommended for any stateful operator.
    //
    //   * Otherwise (legacy path) the id is hash("stage<idx>/<name>"),
    //     so renaming or adding upstream ops changes ids and stale
    //     state is silently abandoned. Fine for stateless ops, but
    //     fragile for stateful ones - that's exactly the trap uid
    //     was added to solve.
    //
    // We use std::hash<std::string> because all we need is "stable
    // across runs in the same process build" - not cryptographic
    // resistance.
    OperatorId derive_id_from_uid_(const std::string& uid) noexcept {
        // Shared with the API env's expect_state_version (see
        // operator_id_from_uid in core/types.hpp) so the runtime - which
        // stamps state-version metadata under this id - and a job's
        // declared expected-version map agree on keys.
        return operator_id_from_uid(uid);
    }

    OperatorId derive_id(const std::string& op_name) noexcept {
        const std::string keyed = "stage" + std::to_string(runners_.size()) + "/" + op_name;
        const std::uint64_t h = std::hash<std::string>{}(keyed);
        // Avoid id 0 (reserved/sentinel for default-constructed StrongId).
        return OperatorId{h == 0 ? std::uint64_t{1} : h};
    }

    // Uid-aware id derivation: honor the operator's uid when set,
    // fall back to the stage/name hash otherwise. The duplicate-uid
    // check throws - two ops sharing a uid is a programmer error
    // (state collisions, undefined behaviour).
    template <typename Op>
    OperatorId derive_id_with_uid_(const Op& op) {
        if (!op.uid().empty()) {
            const auto id = derive_id_from_uid_(op.uid());
            if (assigned_uids_.find(op.uid()) != assigned_uids_.end()) {
                throw std::runtime_error("Dag: duplicate operator uid '" + op.uid() + "'");
            }
            assigned_uids_.insert(op.uid());
            return id;
        }
        return derive_id(op.name());
    }

    // Lookup-or-create the side output map for runner `idx`. Operator
    // runners grab the shared_ptr at construction time (so the closure
    // captures stable storage even if side outputs are added later via
    // Dag::side_output) and close all entries at shutdown.
    SideOutputChannelMap& side_channels_(std::size_t idx) {
        if (idx >= side_channels_by_runner_.size()) {
            side_channels_by_runner_.resize(idx + 1);
        }
        auto& p = side_channels_by_runner_[idx];
        if (!p) {
            p = std::make_shared<SideOutputChannelMap>();
        }
        return *p;
    }
    // Returns the shared_ptr to runner idx's map, allocating an empty
    // map if needed. Runner closures capture this so they can iterate &
    // close all side channels at end-of-stream.
    std::shared_ptr<SideOutputChannelMap> side_channels_ptr_(std::size_t idx) {
        side_channels_(idx);  // ensure allocated
        return side_channels_by_runner_[idx];
    }

    std::size_t default_channel_capacity_;
    std::vector<std::shared_ptr<void>> sources_;
    std::vector<std::shared_ptr<void>> operators_;
    std::vector<std::shared_ptr<void>> sinks_;
    std::vector<detail::OperatorRunner> runners_;
    std::vector<BarrierInjector> source_injectors_;
    std::vector<OperatorId> source_ids_;
    // Boundedness recorded at registration, parallel to source_ids_ (BATCH-1).
    // sources_ is type-erased (shared_ptr<void>), so we snapshot is_bounded()
    // when each source is added rather than re-deriving it later. Drives
    // all_sources_bounded() and, through it, the run-to-completion path.
    std::vector<char> source_bounded_;
    // Per-source split counts (BATCH-3), parallel to source_ids_. Snapshotted at
    // registration since sources_ is type-erased. Drives the batch planner's
    // recommended parallelism.
    std::vector<std::size_t> source_split_counts_;
    // Runner indices of blocking-exchange stages added via add_blocking_exchange
    // (BATCH-2). Read by a batch scheduler to honour blocking edges.
    std::vector<std::size_t> blocking_edge_indices_;
    // Per-runner side output channels, indexed in parallel with runners_.
    // Held by shared_ptr so runner closures can capture stable storage
    // and observe additions made after construction.
    std::vector<std::shared_ptr<SideOutputChannelMap>> side_channels_by_runner_;
    // Tracks uids that have already been assigned within this Dag.
    // Used to reject duplicate uid registrations early - a uid clash
    // is a programmer error (two operators would share state).
    std::unordered_set<std::string> assigned_uids_;
};

}  // namespace clink
