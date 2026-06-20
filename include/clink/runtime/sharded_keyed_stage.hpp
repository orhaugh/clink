#pragma once

// ShardedKeyedStage: single-writer, share-nothing execution of ONE keyed
// operator across S shard threads (increment 2b of shard-per-core).
//
// The thread-per-operator executor runs a keyed operator on one thread over a
// shared state backend; at parallelism the state mutex is the bottleneck
// (increment 1's ShardedInMemoryStateBackend cut cross-shard contention, but a
// shard is still a lock that any thread may take). This stage goes further: it
// fans the operator across S workers, each pinned to a core and owning a
// PRIVATE InMemoryStateBackend plus its own operator instance. Records are
// demuxed by key group so a given key always reaches the same worker, and that
// worker is the ONLY thread that ever touches its backend - single-writer, so
// the per-shard mutex is uncontended and the shard's working set stays hot on
// its core. This is the share-nothing (Seastar/Redpanda) model.
//
// Snapshots stay format-compatible: each shard snapshots its private backend
// and the S blobs are merged (InMemoryStateBackend::merge_snapshot_bytes) into
// the SAME canonical Arrow IPC the mono/sharded backends emit, so a stage
// snapshot restores into a mono backend and vice versa. Restore splits the blob
// back to each shard by its key-group range, reusing InMemory's kg-filtered
// restore. All the serialise/parse/merge/filter logic is reused; this is a pure
// execution + routing layer.
//
// CONTROL SIGNALS (2c): checkpoint(CheckpointBarrier) and advance_watermark()
// are COORDINATED across the shards via a shared rendezvous (coordinate_). Each
// broadcasts the control element as a per-shard sync point and blocks until
// every worker has reached it, then emits ONE downstream control element:
//   * checkpoint: each worker captures its own (single-stream-consistent)
//     backend at the barrier; the coordinator merges the S blobs into ONE
//     Snapshot and forwards the barrier downstream once. The merged Snapshot is
//     RETURNED; persisting it and acking the JM is the caller's job (the 2c-2
//     DAG runner).
//   * watermark: each worker fires its event-time timers (timer output flows
//     downstream) but does NOT forward the watermark; the coordinator forwards a
//     single min-merged watermark once all shards have drained up to it.
// snapshot()/restore() remain the QUIESCED end-of-stream path. Still
// increment-2c work: the Dag::add_sharded_keyed runner (durable persist + ack +
// LocalExecutor wiring), drain-marker handling, rescale across a changed shard
// count, per-shard metrics, and timer / operator-state coordination across
// shards (this targets keyed operators whose state is keyed_state).
//
// THREADING: submit()/advance_watermark()/checkpoint()/close_input() are called
// by ONE producer thread; each shard queue is then single-producer/single-
// consumer. The Downstream sink is called concurrently by all S workers, so it
// MUST be thread-safe (a shared BoundedChannel is; a plain collector needs a
// lock).
//
// num_shards is clamped to >= 1. Over-subscribing past kNumKeyGroups (128) is
// benign but wasteful: shards above the last populated key group own an empty
// range and receive no records (idle pinned threads), so S in [1, 128] is the
// useful range - typically min(cores, key groups).

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "clink/core/stream_element.hpp"
#include "clink/core/types.hpp"
#include "clink/metrics/operator_metrics.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/async_execution_controller.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/cpu_affinity.hpp"
#include "clink/runtime/key_group_partitioner.hpp"
#include "clink/runtime/key_groups.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/runtime/subtask_emitter.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/state_backend.hpp"

namespace clink {

class MetricsRegistry;

template <typename In, typename Out>
class ShardedKeyedStage {
public:
    using InElement = StreamElement<In>;
    using OutElement = StreamElement<Out>;
    // Builds an independent operator instance for shard `shard_idx`. Each gets
    // its own private state backend, so instances must not share mutable state.
    using OperatorFactory =
        std::function<std::unique_ptr<Operator<In, Out>>(std::size_t shard_idx)>;
    // Receives every worker's output. Called concurrently by all S workers, so
    // it MUST be thread-safe.
    using Downstream = std::function<bool(OutElement)>;

    struct Options {
        bool pin_threads = false;  // pin worker i to core i (best-effort)
        std::size_t queue_capacity = 1024;
        MetricsRegistry* metrics = nullptr;
    };

    // Result of a coordinated in-band checkpoint: the merged subtask snapshot
    // (byte-compatible with the mono/sharded backends) plus the fan-in status.
    // `ok` is false if any shard failed to capture (e.g. a worker threw); the
    // barrier is still forwarded downstream regardless (the global commit
    // decision is the coordinator/JM's, from the per-subtask acks).
    struct CheckpointResult {
        CheckpointId id{};
        bool ok{false};
        Snapshot snapshot{};
        std::string error;
    };

    ShardedKeyedStage(std::size_t num_shards,
                      OperatorId op_id,
                      OperatorFactory factory,
                      KeyBytesOf<In> key_bytes_of,
                      Downstream downstream,
                      Options opts = {})
        : num_shards_(num_shards == 0 ? 1 : num_shards),
          op_id_(op_id),
          factory_(std::move(factory)),
          key_bytes_of_(std::move(key_bytes_of)),
          downstream_(std::move(downstream)),
          opts_(opts),
          partition_(make_key_group_partitioner<In>(key_bytes_of_,
                                                    static_cast<std::uint32_t>(num_shards_))) {
        shards_.reserve(num_shards_);
        for (std::size_t i = 0; i < num_shards_; ++i) {
            shards_.push_back(std::make_unique<Shard>(opts_.queue_capacity, op_id_, opts_.metrics));
        }
    }

    ~ShardedKeyedStage() {
        close_input();
        await();
    }

    ShardedKeyedStage(const ShardedKeyedStage&) = delete;
    ShardedKeyedStage& operator=(const ShardedKeyedStage&) = delete;
    ShardedKeyedStage(ShardedKeyedStage&&) = delete;
    ShardedKeyedStage& operator=(ShardedKeyedStage&&) = delete;

    // Spawn the worker threads. Call restore() (if resuming) BEFORE this.
    void start() {
        running_.store(true, std::memory_order_release);
        for (std::size_t i = 0; i < num_shards_; ++i) {
            shards_[i]->thread =
                std::jthread([this, i](std::stop_token /*tok*/) { worker_loop_(i); });
        }
    }

    // Route a data batch's records to the shard that owns each record's key
    // group. The partitioner returns an index in [0, num_shards_), so each
    // record lands on exactly one worker; the worker's private backend is the
    // only one that key ever touches.
    bool submit(Batch<In> batch) {
        if (batch.empty()) {
            return true;
        }
        std::vector<Batch<In>> sub(num_shards_);
        for (auto& rec : batch) {
            // partition_ already returns an index in [0, num_shards_) (it is
            // built with parallelism == num_shards_); the modulo is a kept-as-
            // defensive no-op guarding against a future partitioner that does
            // not honour that contract.
            const std::size_t s = partition_(rec.value()) % num_shards_;
            sub[s].push(std::move(rec));
        }
        bool ok = true;
        for (std::size_t i = 0; i < num_shards_; ++i) {
            if (!sub[i].empty()) {
                ok &= shards_[i]->queue->push(InElement::data(std::move(sub[i])));
            }
        }
        return ok;
    }

    // Coordinated watermark (cross-shard min-merge). Broadcasts the watermark as
    // a per-shard sync point: every worker fires its event-time timers and
    // processes all records up to the watermark, then signals; once ALL shards
    // have reached it, a SINGLE watermark is forwarded downstream. All shards see
    // the same broadcast watermark, so the min across shards IS that watermark -
    // but it can only be emitted after every shard has drained its data up to it,
    // else a downstream consumer could see the watermark before late data from a
    // slower shard. Workers fire timers locally but do NOT forward the watermark
    // themselves. Synchronous; call from the single producer thread. Throws
    // std::logic_error if called before start().
    void advance_watermark(Watermark wm) {
        if (!running_.load(std::memory_order_acquire)) {
            throw std::logic_error("ShardedKeyedStage::advance_watermark called before start()");
        }
        coordinate_(InElement::watermark(wm));
        downstream_(OutElement::watermark(wm));
    }

    // Coordinated drain marker (rescale wind-down signal). Like a watermark it
    // must respect ordering with data, so it is broadcast as a per-shard sync
    // point and forwarded downstream exactly ONCE after every shard has drained
    // its pre-drain records (not S copies). Workers take no per-shard action -
    // the marker is informational for downstream; the subsequent EOS (upstream
    // close) drives the actual wind-down. Synchronous; throws before start().
    void drain(DrainMarker d) {
        if (!running_.load(std::memory_order_acquire)) {
            throw std::logic_error("ShardedKeyedStage::drain called before start()");
        }
        coordinate_(InElement::drain(d));
        downstream_(OutElement::drain(d));
    }

    // Coordinated in-band checkpoint of the RUNNING stage. Broadcasts the
    // barrier as a per-shard sync point (it lands in each queue AFTER every
    // record submitted before this call, so each shard captures all its
    // pre-barrier records), waits for all shards to deliver their capture,
    // merges them into one Snapshot, forwards the barrier downstream ONCE, and
    // returns the result. Synchronous: call from the single producer thread
    // between submit()s (it does not submit during a checkpoint), exactly where
    // a DAG runner pops an in-band barrier. Throws std::logic_error if called
    // before start().
    CheckpointResult checkpoint(CheckpointBarrier b) {
        const CheckpointId id = b.id();
        if (!running_.load(std::memory_order_acquire)) {
            throw std::logic_error("ShardedKeyedStage::checkpoint called before start()");
        }
        Coordinated c = coordinate_(InElement::barrier(b));
        bool ok = c.ok;
        std::string err = std::move(c.err);
        Snapshot snap{.checkpoint_id = id, .bytes = {}};
        if (ok) {
            try {
                snap.bytes = InMemoryStateBackend::merge_snapshot_bytes(c.blobs);
            } catch (const std::exception& e) {
                ok = false;
                err = e.what();
            }
        }
        downstream_(OutElement::barrier(b));
        return {id, ok, std::move(snap), std::move(err)};
    }

    // Close every shard queue; workers drain, flush, and exit.
    void close_input() {
        for (auto& s : shards_) {
            if (s->queue) {
                s->queue->close();
            }
        }
    }

    // Join all worker threads. Safe to call more than once.
    void await() {
        for (auto& s : shards_) {
            if (s->thread.joinable()) {
                s->thread.join();
            }
        }
        running_.store(false, std::memory_order_release);
    }

    // Per-worker errors captured if an operator method threw (shard index +
    // message). Empty if every worker ran cleanly. Read after await().
    using WorkerError = std::pair<std::size_t, std::string>;
    std::vector<WorkerError> worker_errors() const {
        std::lock_guard lock(errors_mu_);
        return worker_errors_;
    }

    // QUIESCED snapshot: merge the S private backends into one canonical Arrow
    // IPC blob, byte-compatible with InMemory/Sharded. PRECONDITION: no work in
    // flight (call after close_input()+await(), or while otherwise quiesced) -
    // enforced: throws std::logic_error if the workers are running, since a
    // concurrent snapshot would merge a logically torn state.
    Snapshot snapshot(CheckpointId id) {
        if (running_.load(std::memory_order_acquire)) {
            throw std::logic_error(
                "ShardedKeyedStage::snapshot called while running; quiesce first "
                "(close_input()+await())");
        }
        std::vector<std::vector<std::byte>> blobs;
        blobs.reserve(num_shards_);
        for (auto& s : shards_) {
            blobs.push_back(s->backend->snapshot(id).bytes);
        }
        return Snapshot{.checkpoint_id = id,
                        .bytes = InMemoryStateBackend::merge_snapshot_bytes(blobs)};
    }

    // Restore the canonical blob into the shards, each narrowed to the key-group
    // range it owns. PRECONDITION: call before start(); throws std::logic_error
    // if the workers are running. Reuses InMemory's kg-filtered restore so a
    // mono/sharded snapshot loads correctly too.
    void restore(const Snapshot& snap) {
        if (running_.load(std::memory_order_acquire)) {
            throw std::logic_error(
                "ShardedKeyedStage::restore called while running; restore "
                "before start()");
        }
        for (std::size_t i = 0; i < num_shards_; ++i) {
            const auto [first, last] = key_group_range_for_subtask(
                static_cast<std::uint32_t>(i), static_cast<std::uint32_t>(num_shards_));
            shards_[i]->backend->restore(snap, KeyGroupRange{first, last});
        }
    }

    std::size_t num_shards() const noexcept { return num_shards_; }

private:
    struct Shard {
        std::unique_ptr<InMemoryStateBackend> backend;
        std::unique_ptr<RuntimeContext> ctx;
        std::unique_ptr<Operator<In, Out>> op;
        std::shared_ptr<BoundedChannel<InElement>> queue;
        std::jthread thread;

        Shard(std::size_t cap, OperatorId op_id, MetricsRegistry* metrics)
            : backend(std::make_unique<InMemoryStateBackend>()),
              ctx(std::make_unique<RuntimeContext>(
                  op_id, "sharded_keyed_stage", backend.get(), metrics)),
              queue(std::make_shared<BoundedChannel<InElement>>(cap, "sharded_keyed_stage")) {}
    };

    void worker_loop_(std::size_t i) {
        Shard& sh = *shards_[i];
        // Pin (best-effort) and name the worker so the share-nothing layout is
        // visible in top -H / perf. Pinning never affects correctness.
        if (opts_.pin_threads) {
            pin_current_thread_to_core(static_cast<unsigned>(i));
        }
        set_current_thread_name("clk-shard" + std::to_string(i));

        // Forwarding emitter: emit straight into the shared downstream sink.
        Emitter<Out> out(typename Emitter<Out>::Forward(
            [this](OutElement e) { return downstream_(std::move(e)); }));
        out.set_operator_id(op_id_.value());

        // Data-only emitter for firing event-time timers during on_watermark:
        // timer output flows downstream, but the watermark itself is dropped
        // (the coordinator forwards a single min-merged watermark, not S copies).
        Emitter<Out> timer_out(typename Emitter<Out>::Forward([this](OutElement e) {
            if (e.is_data()) {
                return downstream_(std::move(e));
            }
            return true;
        }));

        // The standalone stage has no outer executor try/catch (the DAG path's
        // operator exceptions are caught by LocalExecutor). Catch here so a
        // throwing operator records the error and closes ITS queue (unblocking
        // any producer waiting on push) instead of taking the whole process
        // down via std::terminate. Sibling workers keep running; the caller
        // inspects worker_errors() after await().
        try {
            // Build this shard's operator instance over its PRIVATE backend.
            sh.op = factory_(i);
            sh.op->set_id(op_id_);
            sh.op->attach_runtime(sh.ctx.get());

            sh.op->open();
            // ASYNC-7: per-shard async controller for an async-opting operator
            // (the single-writer-per-key home). The shard is already
            // single-writer over its private backend; the controller adds
            // intra-shard per-key ordering for async reads and a
            // drain-before-capture at barriers. The shard backend is InMemory
            // (reads complete inline today), so this provides the gate + drain
            // wiring; a genuinely-deferring per-shard backend whose reads
            // actually suspend is a follow-on (the remote backend lands then).
            std::unique_ptr<AsyncExecutionController> aec;
            if (sh.op->supports_async()) {
                // Tripwire (async-timer gate): this branch fires event-time
                // timers by calling sh.op->on_watermark inline AFTER a blunt
                // aec->drain() (it bypasses aec->on_watermark's per-epoch FIFO
                // release - over-serialising, but correct), so an event-time-only
                // operator is gated and safe. PROCESSING-time timers, however,
                // have NO fire path in this stage at all: the worker loop below
                // uses a blocking queue pop with no loop-top fire_due, so a
                // processing-time timer would never fire even synchronously.
                // Admitting such an operator would silently swallow its timers,
                // which is worse than refusing it - so this tripwire stays. (The
                // single-input runner DOES now gate processing-time timers via
                // gated_timer_fire.hpp; a deadline-driven pop + per-shard gated
                // fire for this stage is a separate, larger structural change.)
                if (sh.op->fires_state_touching_processing_time_timers()) {
                    throw std::logic_error(
                        "clink: operator '" + sh.op->name() +
                        "' fires state-touching processing-time timers under async execution, "
                        "which the sharded keyed stage cannot serve (it has no processing-time "
                        "fire path); the single-input runner gates these, the sharded stage does "
                        "not yet");
                }
                aec = std::make_unique<AsyncExecutionController>();
            }
            while (auto elem = sh.queue->pop()) {
                const InElement& e = *elem;
                if (e.is_data()) {
                    // Per-shard records-in: reveals shard skew (one hot shard
                    // vs idle peers) for a single keyed operator.
                    clink::metrics::op::shard_records_in_inc(op_id_.value(), i, e.as_data().size());
                    if (aec) {
                        sh.op->process_async(e, out, *aec);
                        aec->poll();
                    } else {
                        sh.op->process(e, out);
                    }
                } else if (e.is_watermark()) {
                    // Fire this shard's event-time timers (timer output flows
                    // downstream via timer_out) but do NOT forward the watermark;
                    // deliver to the coordinator, which forwards one min-merged
                    // watermark once every shard has reached this point.
                    if (aec) {
                        aec->drain();  // complete in-flight async reads before the watermark
                                       // proceeds
                    }
                    sh.op->on_watermark(e.as_watermark(), timer_out);
                    deliver_capture_(i, {}, true, "");
                } else if (e.is_barrier()) {
                    // 2c: capture this shard's state at the barrier point and
                    // hand the blob to the coordinator (checkpoint()), which
                    // merges all shards and forwards the barrier downstream
                    // once. The worker does NOT forward it. The capture is on
                    // this shard's own backend (single-writer, uncontended) and
                    // sits between record processing, so it is a consistent cut
                    // of this shard's stream at the barrier.
                    const CheckpointId cid = e.as_barrier().id();
                    if (aec) {
                        // Drain in-flight async reads so every pre-barrier
                        // record's write has landed: the capture below is then
                        // a consistent cut of this shard's stream at the barrier.
                        aec->drain_for_barrier();
                    }
                    std::vector<std::byte> blob;
                    bool cap_ok = true;
                    std::string cap_err;
                    try {
                        blob = sh.backend->snapshot(cid).bytes;
                    } catch (const std::exception& ex) {
                        cap_ok = false;
                        cap_err = ex.what();
                    }
                    deliver_capture_(i, std::move(blob), cap_ok, std::move(cap_err));
                } else if (e.is_drain()) {
                    // Control signal: acknowledge the rendezvous so the
                    // coordinator (drain()) can forward one marker downstream
                    // after every shard has reached it. In async mode, complete
                    // in-flight reads first so the marker keeps its ordering
                    // with the data ahead of it.
                    if (aec) {
                        aec->drain();
                    }
                    deliver_capture_(i, {}, true, "");
                }
            }
            if (aec) {
                aec->drain();  // finish any in-flight async work before flush
            }
            sh.op->flush(out);
            sh.op->close();
        } catch (const std::exception& ex) {
            {
                std::lock_guard lock(errors_mu_);
                worker_errors_.emplace_back(i, ex.what());
            }
            // If a checkpoint is in flight and this worker had not yet
            // delivered, deliver a failure so the coordinator wakes instead of
            // waiting forever for a capture that will never come. Then close
            // this queue: a later checkpoint's push will fail and the
            // coordinator delivers-on-behalf (idempotent), so no checkpoint can
            // hang on a dead worker.
            deliver_capture_(i, {}, false, "sharded stage worker threw: " + std::string(ex.what()));
            if (sh.queue) {
                sh.queue->close();
            }
        }
        if (sh.op) {
            sh.op->attach_runtime(nullptr);
        }
    }

    // Collected result of a coordinate_() round: per-shard blobs (only
    // meaningful for a barrier/capture round) + the aggregate status.
    struct Coordinated {
        std::vector<std::vector<std::byte>> blobs;
        bool ok{true};
        std::string err;
    };

    // Broadcast `control` (a barrier or watermark) to every shard as a sync
    // point and block until all shards deliver. Shared by checkpoint() and
    // advance_watermark(). Hang-free under worker death: a dead worker's queue
    // is closed so push returns false, and we deliver a failure on its behalf;
    // combined with deliver_capture_'s per-shard ckpt_done_ idempotency, every
    // shard delivers exactly once however the death races the broadcast.
    Coordinated coordinate_(const InElement& control) {
        {
            std::lock_guard lock(ckpt_mu_);
            ckpt_active_ = true;
            ckpt_delivered_ = 0;
            ckpt_ok_ = true;
            ckpt_err_.clear();
            ckpt_blobs_.assign(num_shards_, {});
            ckpt_done_.assign(num_shards_, false);
        }
        for (std::size_t i = 0; i < num_shards_; ++i) {
            if (!shards_[i]->queue->push(control)) {
                deliver_capture_(
                    i, {}, false, "sharded stage: control not delivered (worker dead)");
            }
        }
        Coordinated c;
        {
            std::unique_lock lock(ckpt_mu_);
            ckpt_cv_.wait(lock, [&] { return ckpt_delivered_ == num_shards_; });
            c.blobs = std::move(ckpt_blobs_);
            c.ok = ckpt_ok_;
            c.err = std::move(ckpt_err_);
            ckpt_active_ = false;
        }
        return c;
    }

    // Worker -> coordinator handoff of a per-shard delivery for the in-flight
    // control round (a capture blob for a barrier, empty for a watermark).
    // Idempotent per shard per round (the death path may race a late normal
    // delivery); only the first delivery for shard i counts.
    void deliver_capture_(std::size_t i, std::vector<std::byte> blob, bool ok, std::string err) {
        std::lock_guard lock(ckpt_mu_);
        if (!ckpt_active_ || ckpt_done_[i]) {
            return;
        }
        ckpt_done_[i] = true;
        ckpt_blobs_[i] = std::move(blob);
        if (!ok) {
            ckpt_ok_ = false;
            if (ckpt_err_.empty()) {
                ckpt_err_ = std::move(err);
            }
        }
        if (++ckpt_delivered_ == num_shards_) {
            ckpt_cv_.notify_all();
        }
    }

    std::size_t num_shards_;
    OperatorId op_id_;
    OperatorFactory factory_;
    KeyBytesOf<In> key_bytes_of_;
    Downstream downstream_;
    Options opts_;
    typename SubtaskEmitter<In>::Partitioner partition_;
    std::vector<std::unique_ptr<Shard>> shards_;
    std::atomic<bool> running_{false};
    mutable std::mutex errors_mu_;
    std::vector<WorkerError> worker_errors_;

    // In-band checkpoint coordination (one checkpoint in flight at a time,
    // since checkpoint() is synchronous on the single producer thread).
    std::mutex ckpt_mu_;
    std::condition_variable ckpt_cv_;
    bool ckpt_active_{false};
    std::size_t ckpt_delivered_{0};
    bool ckpt_ok_{true};
    std::string ckpt_err_;
    std::vector<std::vector<std::byte>> ckpt_blobs_;
    std::vector<bool> ckpt_done_;
};

}  // namespace clink
