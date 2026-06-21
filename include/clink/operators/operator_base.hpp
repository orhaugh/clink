#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <utility>

#include "clink/core/stream_element.hpp"
#include "clink/core/types.hpp"
#include "clink/metrics/operator_metrics.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/runtime/subtask_emitter.hpp"
#include "clink/runtime/timer_service.hpp"
#include "clink/state/state_backend.hpp"

namespace clink {

// ASYNC-6: forward-declared so process_async can take it by reference
// without operator_base.hpp depending on the controller header.
class AsyncExecutionController;

namespace detail {

// Reserved operator-state key under which an operator's checkpointed
// timers live. A `slot` suffix namespaces the inner operators of a
// ChainedOperator (each inner op has its own TimerService).
inline constexpr const char* kTimerStateKey = "__clink_timers";

// Persist a TimerService into the backend as operator-state, so the next
// backend.snapshot() captures it. Clears the slot when the service is
// empty so a checkpoint taken after every timer has fired does not
// resurrect stale timers on restore.
inline void snapshot_timer_service(const TimerService& ts,
                                   StateBackend& backend,
                                   OperatorId op,
                                   const std::string& slot) {
    const std::string key = std::string(kTimerStateKey) + slot;
    if (ts.empty() && ts.event_timers_empty()) {
        backend.erase_operator_state(op, key);
        return;
    }
    const auto bytes = ts.serialize();
    backend.put_operator_state(
        op,
        key,
        StateBackend::ValueView{reinterpret_cast<const char*>(bytes.data()), bytes.size()});
}

// Reload a TimerService from operator-state written by
// snapshot_timer_service. A no-op when nothing was stored.
//
// `kg_range` routes timers on a rescale: the timer blob rides operator-state
// (broadcast to every new subtask), so on a narrowed restore each subtask
// keeps only the timers whose key falls in its key-group range - matching
// where the corresponding keyed state landed. The full range (same
// parallelism) keeps every timer.
inline void restore_timer_service(TimerService& ts,
                                  StateBackend& backend,
                                  OperatorId op,
                                  const std::string& slot,
                                  const KeyGroupRange& kg_range) {
    const std::string key = std::string(kTimerStateKey) + slot;
    auto v = backend.get_operator_state(op, key);
    if (!v.has_value()) {
        return;
    }
    if (kg_range.covers_all()) {
        ts.restore_from(*v);
        return;
    }
    ts.restore_from(*v, [&kg_range](const std::string& timer_key) {
        const auto kg = key_group_for_key(std::span<const std::byte>{
            reinterpret_cast<const std::byte*>(timer_key.data()), timer_key.size()});
        return kg_range.contains(kg);
    });
}

}  // namespace detail

// An Emitter is the operator-side handle for sending downstream.
//
// Three construction modes:
//   * Single-channel - the original 1:1 form, used by all parallelism=1
//     stages. Just push everything to one BoundedChannel.
//   * Multi-output - wraps a SubtaskEmitter that owns N downstream channels
//     and routes records via a partitioner (data) or broadcasts (watermark
//     / barrier). Used by parallel stages.
//   * Forwarding - wraps a callable that receives the StreamElement and
//     dispatches it (typically by calling the next operator's process()
//     in a chain). Enables true zero-channel operator chaining: instead
//     of pushing through a BoundedChannel between adjacent operators in
//     the same thread, we forward via a direct std::function call.
//
// Operators see the same API in all three cases.
template <typename Out>
class Emitter {
public:
    using Element = StreamElement<Out>;
    using Channel = BoundedChannel<Element>;
    using Forward = std::function<bool(Element)>;

    explicit Emitter(Channel* downstream) : downstream_(downstream) {}
    explicit Emitter(SubtaskEmitter<Out>* sub) : sub_(sub) {}
    explicit Emitter(Forward forward) : forward_(std::move(forward)) {}

    // Phase 26a: source-side mode override. The source runner sets this
    // from RuntimeContext::unaligned_checkpoints() so user sources don't
    // have to know about barrier modes; every barrier the source emits
    // through this Emitter is stamped with the runner-decided mode.
    // Downstream operators DO NOT set this - they forward barriers with
    // the mode the aligner pinned (which came from upstream's stamp).
    void set_default_barrier_mode(CheckpointBarrier::Mode mode) noexcept {
        default_barrier_mode_ = mode;
    }

    // Per-operator metrics binding. The runner that constructs this
    // Emitter passes the OperatorId so emit_data can route records_out_total
    // through the global registry without each operator having to know
    // its own id. Zero (the default) disables the bump - useful for
    // anonymous internal Emitters in tests.
    void set_operator_id(std::uint64_t op_id) noexcept { op_id_for_metrics_ = op_id; }

    // Blocking emit. Returns false only when the downstream has been closed.
    bool emit(Element e) {
        if (forward_) {
            return forward_(std::move(e));
        }
        if (sub_ != nullptr) {
            if (e.is_data()) {
                return sub_->emit_data(std::move(e.as_data()));
            }
            if (e.is_watermark()) {
                return sub_->emit_watermark(e.as_watermark());
            }
            if (e.is_drain()) {
                return sub_->emit_drain(e.as_drain());
            }
            return sub_->emit_barrier(e.as_barrier());
        }
        if (downstream_ == nullptr) {
            return true;  // headless sink
        }
        return downstream_->push(std::move(e));
    }

    bool emit_data(Batch<Out> batch) {
        if (op_id_for_metrics_ != 0) {
            clink::metrics::op::records_out_inc(op_id_for_metrics_, batch.size());
        }
        if (forward_) {
            return forward_(Element::data(std::move(batch)));
        }
        if (sub_ != nullptr) {
            return sub_->emit_data(std::move(batch));
        }
        return emit(Element::data(std::move(batch)));
    }
    bool emit_watermark(Watermark wm) {
        if (forward_) {
            return forward_(Element::watermark(wm));
        }
        if (sub_ != nullptr) {
            return sub_->emit_watermark(wm);
        }
        return emit(Element::watermark(wm));
    }
    bool emit_barrier(CheckpointBarrier b) {
        if (default_barrier_mode_.has_value() && b.mode() != *default_barrier_mode_) {
            b = CheckpointBarrier{b.id(), b.is_terminal(), *default_barrier_mode_};
        }
        if (forward_) {
            return forward_(Element::barrier(b));
        }
        if (sub_ != nullptr) {
            return sub_->emit_barrier(b);
        }
        return emit(Element::barrier(b));
    }

    // Phase 29d-3: drain marker. Source runners and stateful
    // intermediate operators emit this when the JM has signalled a
    // rescale at their operator; downstream consumers see it
    // immediately before this subtask closes its outputs.
    bool emit_drain(DrainMarker d) {
        if (forward_) {
            return forward_(Element::drain(d));
        }
        if (sub_ != nullptr) {
            return sub_->emit_drain(d);
        }
        return emit(Element::drain(d));
    }

    Channel* channel() noexcept { return downstream_; }

private:
    Channel* downstream_{nullptr};
    SubtaskEmitter<Out>* sub_{nullptr};
    Forward forward_;
    std::optional<CheckpointBarrier::Mode> default_barrier_mode_;
    // OperatorId.value() of the operator this Emitter belongs to.
    // 0 = unbound (skip records_out_total bumps). Set by the runner
    // that builds the Emitter; see dag.hpp's per-operator factories.
    std::uint64_t op_id_for_metrics_{0};
};

// Base interface for any single-input single-output operator.
//
// Sources are modelled separately because they have no input channel.
// Sinks are modelled separately because they have no output channel.
template <typename In, typename Out>
class Operator {
public:
    virtual ~Operator() = default;

    // Lifecycle hooks. Default implementations are intentionally no-ops.
    virtual void open() {}
    virtual void close() {}

    // Process a single element. The operator may emit zero or more elements.
    virtual void process(const StreamElement<In>& element, Emitter<Out>& out) = 0;

    // Columnar-native fast path (opt-in). When supports_columnar() returns
    // true AND an incoming data element carries an Arrow RecordBatch sidecar
    // (Batch<In>::is_columnar()), the runner calls process_columnar() instead
    // of process(). The operator can then run vectorized Arrow compute on the
    // columns and emit a columnar Batch<Out>, with zero row materialization.
    // Returning false means "I can't take this batch columnar - fall back to
    // process()", so a columnar operator paired with an unexpected schema (or
    // a row-only upstream) degrades cleanly. The defaults keep every existing
    // operator strictly row-based and unchanged.
    //
    // CONTRACT: return false ONLY before emitting anything. The runner re-runs
    // process() on a false return, so emitting some output and THEN returning
    // false would double-emit. Decide up front (schema/type checks) whether
    // you can handle the batch; once you emit, you own it and must return true.
    [[nodiscard]] virtual bool supports_columnar() const noexcept { return false; }
    virtual bool process_columnar(const StreamElement<In>& /*element*/, Emitter<Out>& /*out*/) {
        return false;
    }

    // Async (non-blocking state) fast path (opt-in, ASYNC-6). When
    // supports_async() returns true AND the state backend can defer reads
    // (StateBackend::supports_async_get()), the single-input runner routes
    // data elements here instead of process(): the operator submits one
    // coroutine per record to the AsyncExecutionController, each co_awaiting
    // keyed_state.get_async() so a slow (remote) read suspends that record
    // without blocking the runner thread. The controller enforces per-key
    // ordering, watermark/event-time-timer completeness, and a drain at
    // checkpoint barriers. The default keeps every operator on the
    // synchronous process() path, byte-for-byte unchanged. process_async
    // only ever sees data elements; the runner routes watermarks and
    // barriers through the controller itself.
    [[nodiscard]] virtual bool supports_async() const noexcept { return false; }
    virtual void process_async(const StreamElement<In>& /*element*/,
                               Emitter<Out>& /*out*/,
                               AsyncExecutionController& /*aec*/) {}

    // ASYNC-10 opt-in: when true (and supports_async() + the backend defers
    // reads), the single-input runner wraps the state backend in a
    // CoalescingBackend, so the per-record get_async calls in one process_async
    // batch collapse into ONE get_many_async round-trip. Default false: each
    // read issues individually (the byte-identical pre-coalescing path). Worth
    // enabling for a remote/disaggregated backend where a round-trip is costly.
    [[nodiscard]] virtual bool coalesce_reads() const noexcept { return false; }

    // True iff this operator's on_event_time_timer / on_processing_time_timer
    // callbacks read or write keyed state (the ProcessFunction adapter does;
    // window/CEP operators touch state via on_watermark, not timer virtuals, so
    // they return false here). Used by the SHARDED keyed stage, which has no
    // processing-time fire path and refuses such operators under async (via the
    // processing-time refinement below). The single-input runner gates both
    // timer kinds safely (see below), so it does not refuse on this flag.
    [[nodiscard]] virtual bool fires_state_touching_timers() const noexcept { return false; }

    // True iff this operator fires state-touching PROCESSING-TIME timer
    // callbacks. EVENT-TIME timers fire from INSIDE the aec->on_watermark release
    // closure (after the epoch has fully drained), so no in-flight async read for
    // the same key is outstanding when they touch state - they are always safe.
    // PROCESSING-TIME timers fire at the top of the runner loop; the single-input
    // async runner routes them through the per-key gate (gated_timer_fire.hpp) so
    // a state-touching callback serialises behind any in-flight read for the same
    // key - SAFE there, provided the operator registers each processing-time
    // timer with the SAME key bytes it uses as its record gate key in
    // process_async (the documented contract). The SHARDED keyed stage has no
    // processing-time fire path and still refuses an operator that returns true
    // here. An operator that touches keyed state ONLY from event-time timers
    // (e.g. AsyncTumblingWindowOperator) overrides this to return false; the
    // default is conservative (any state-touching timer might be processing-time).
    [[nodiscard]] virtual bool fires_state_touching_processing_time_timers() const noexcept {
        return fires_state_touching_timers();
    }

    // Hooks for time and checkpointing. Default behaviour for watermarks
    // is to fire any event-time timers whose timestamp ≤ the watermark
    // and then forward the watermark unchanged. Operators that override
    // on_watermark (window operators, the ProcessFunction adapter) must
    // call fire_due_event_time_timers themselves if they want timer
    // semantics; ChainedOperator forwards to inner ops' overrides.
    virtual void on_watermark(Watermark wm, Emitter<Out>& out) {
        fire_due_event_time_timers(out, wm.timestamp().millis());
        out.emit_watermark(wm);
    }

    virtual void on_barrier(CheckpointBarrier barrier, Emitter<Out>& out) {
        out.emit_barrier(barrier);
    }

    // Called when a processing-time timer registered via
    // runtime()->timer_service()->register_processing_time_timer(t, k)
    // becomes due. Fired between input pops on the operator's thread, so
    // state access is single-threaded with process(). Default no-op.
    //
    // ASYNC CONTRACT: under async-state execution the single-input runner fires
    // this through the per-key gate, so synchronous keyed-state access here is
    // safe (no same-key async read is outstanding when it runs). For that
    // serialisation to protect the right state, an async operator MUST register
    // each processing-time timer with the SAME key bytes `k` it uses as the
    // record gate key in process_async (the runner passes `k` straight through
    // as the gate key). See gated_timer_fire.hpp.
    virtual void on_processing_time_timer(std::int64_t /*timestamp_ms*/,
                                          const std::string& /*key*/,
                                          Emitter<Out>& /*out*/) {}

    // Called when an event-time timer registered via
    // runtime()->timer_service()->register_event_time_timer(t, k) becomes
    // due - i.e. the operator forwarded a watermark with ts ≥ the
    // registered ts. Same threading discipline as on_processing_time_timer.
    // Default no-op.
    virtual void on_event_time_timer(std::int64_t /*timestamp_ms*/,
                                     const std::string& /*key*/,
                                     Emitter<Out>& /*out*/) {}

    // Drive any event-time timers whose deadline ≤ watermark_ms. The
    // default polls the operator's own RuntimeContext::timer_service().
    // ChainedOperator overrides similarly to fire_due_timers to fan
    // out across inner-op RuntimeContexts.
    virtual void fire_due_event_time_timers(Emitter<Out>& out, std::int64_t watermark_ms) {
        auto* rt = this->runtime();
        if (rt == nullptr) {
            return;
        }
        rt->timer_service()->poll_due_event_time(
            watermark_ms, [this, &out](std::int64_t ts, const std::string& key) {
                this->on_event_time_timer(ts, key, out);
            });
    }

    // Drive any timers whose deadline has passed. The default polls the
    // operator's own RuntimeContext::timer_service(). Operators that
    // *wrap* other operators (currently only ChainedOperator) override
    // this to fan out polling/firing across their sub-operators, with
    // each inner op's fire dispatched through the segment of the chain
    // that lives downstream of that op. The runner calls this between
    // input pops; overrides MUST NOT block.
    virtual void fire_due_timers(Emitter<Out>& out, std::int64_t now_ms) {
        auto* rt = this->runtime();
        if (rt == nullptr) {
            return;
        }
        rt->timer_service()->poll_due(now_ms,
                                      [this, &out](std::int64_t ts, const std::string& key) {
                                          this->on_processing_time_timer(ts, key, out);
                                      });
    }

    // The earliest pending timer deadline across this operator and (if
    // it wraps others) its sub-operators. The runner uses this to size
    // its input-pop timeout so timers fire on time. nullopt means "no
    // pending timers anywhere", in which case the runner picks a
    // cancellation-friendly default (~1s).
    virtual std::optional<std::int64_t> next_timer_deadline_ms() const {
        auto* rt = this->runtime();
        if (rt == nullptr) {
            return std::nullopt;
        }
        return rt->timer_service()->next_timestamp();
    }

    // ---- Timer checkpointing -------------------------------------------
    //
    // Persist / reload this operator's TimerService (processing-time and
    // event-time timers) so timers survive a failover. Mirrors the
    // Source::snapshot_offset / restore_offset pattern: the runner calls
    // snapshot_timers just before it snapshots the state backend at a
    // barrier, and restore_timers at open() (after the backend has been
    // restored). Timers ride operator-state, which is restored whole (not
    // key-group-narrowed); restore_timers therefore no-ops on a rescale
    // (narrowed key groups) to avoid duplicating timers across the new
    // subtasks - rescale timer routing is a follow-on. The default impls
    // cover every operator; ChainedOperator overrides to fan out across
    // its inner ops' per-RC TimerServices (the `slot` namespaces them).
    virtual void snapshot_timers(StateBackend& backend,
                                 OperatorId op_id,
                                 const std::string& slot = "") {
        auto* rt = this->runtime();
        if (rt == nullptr) {
            return;
        }
        detail::snapshot_timer_service(*rt->timer_service(), backend, op_id, slot);
    }
    virtual void restore_timers(StateBackend& backend,
                                OperatorId op_id,
                                const std::string& slot = "") {
        auto* rt = this->runtime();
        if (rt == nullptr) {
            return;
        }
        detail::restore_timer_service(
            *rt->timer_service(), backend, op_id, slot, rt->restore_key_group_range());
    }

    // Called once after the input channel is closed and drained, before
    // close(). Operators that buffer state (windows, sorts, joins) must use
    // this hook to emit any residual output. Default is a no-op.
    virtual void flush(Emitter<Out>& /*out*/) {}

    virtual std::string name() const { return "operator"; }
    OperatorId id() const noexcept { return id_; }
    void set_id(OperatorId id) noexcept { id_ = id; }

    // ---- uid + display_name (stable identity) ---------
    //
    // `uid` is the user-supplied stable identifier used to derive the
    // OperatorId. Same uid across job runs (or across topology edits)
    // produces the same OperatorId, which means keyed state restores
    // correctly even if the topology is reordered, renamed, or has
    // ops added/removed around this one. Strongly recommended for any
    // stateful operator the user expects to survive a savepoint.
    //
    // `display_name` is a human-readable label (for logs, metrics,
    // UI). Defaults to empty; if set, overrides `name()` for display
    // purposes only - the actual `name()` method is what derive_id
    // uses as a fallback when uid is empty.
    void set_uid(std::string s) noexcept { uid_ = std::move(s); }
    [[nodiscard]] const std::string& uid() const noexcept { return uid_; }

    void set_display_name(std::string s) noexcept { display_name_ = std::move(s); }
    [[nodiscard]] const std::string& display_name() const noexcept { return display_name_; }

    // Set by the runtime before open(). Operators that need state, metrics,
    // or their own id reach for runtime() inside open()/process().
    void attach_runtime(RuntimeContext* ctx) noexcept { runtime_ = ctx; }
    RuntimeContext* runtime() const noexcept { return runtime_; }

private:
    OperatorId id_{0};
    std::string uid_;
    std::string display_name_;
    RuntimeContext* runtime_{nullptr};
};

// Sources produce StreamElement<Out> autonomously. The runtime drives them by
// calling produce() until it returns false (meaning the source is exhausted).
//
// run() blocks; cancel() asks the source to stop. The default implementation of
// run() loops over produce() - operators typically only override produce().
template <typename Out>
class Source {
public:
    virtual ~Source() = default;

    virtual void open() {}
    virtual void close() {}

    // Produce returns true if it emitted something (i.e. there might be more)
    // and false if the source is finished.
    virtual bool produce(Emitter<Out>& out) = 0;

    virtual void cancel() { cancelled_.store(true, std::memory_order_relaxed); }
    bool cancelled() const noexcept { return cancelled_.load(std::memory_order_relaxed); }

    // Boundedness contract (BATCH-1). A bounded source emits a finite stream:
    // produce() eventually returns false at genuine end-of-input rather than
    // blocking for more. The runtime uses this two ways:
    //   1. The source runner emits a max watermark once a bounded source
    //      exhausts cleanly, draining every downstream event-time window and
    //      timer before the terminal barrier closes the stream (the
    //      end-of-input drain). A cancel skips it, so tearing down a streaming
    //      job never spuriously fires all its windows.
    //   2. The executor reports the job as bounded iff every source is bounded,
    //      which is what selects the run-to-completion / batch execution path.
    // Default false: an unmarked source is treated as unbounded (a stream that
    // never ends) - the conservative choice that preserves existing behaviour
    // for connectors that haven't opted in (Kafka, CDC).
    [[nodiscard]] virtual bool is_bounded() const noexcept { return false; }

    // Split count (BATCH-3): how many independent splits this source can be read
    // in parallel - the natural parallelism of a bounded source (e.g. number of
    // files, object keys, partitions or row groups). The batch planner derives a
    // recommended per-stage parallelism from this, clamped to operator bounds and
    // key groups. Default 1: a single-split source (one file, one query result)
    // reads sequentially. Unbounded streaming sources can ignore it.
    [[nodiscard]] virtual std::size_t split_count() const noexcept { return 1; }

    // Should the dag-source-runner emit a synthesized terminal
    // CheckpointBarrier after produce() returns false? Default true,
    // for actual sources of a stream (BoundedSlowStringSource, file
    // readers, etc.). NetworkBridgeSource overrides to false: it's a
    // relay forwarding whatever flowed in, so any terminal barrier
    // emitted upstream is already present in its output. A second
    // synthesized terminal on top of the forwarded one causes
    // downstream 2PC sinks to commit twice - once with the real
    // payload, once after the pending file has been re-opened and is
    // empty - clobbering the previously committed file.
    virtual bool emit_terminal_barrier_on_exit() const noexcept { return true; }

    // Called once after produce() returns false, before close(). Sources that
    // have buffered events to emit at end-of-stream use this hook.
    virtual void flush(Emitter<Out>& /*out*/) {}

    virtual std::string name() const { return "source"; }
    OperatorId id() const noexcept { return id_; }
    void set_id(OperatorId id) noexcept { id_ = id; }

    // Stable identity - see Operator<I,O>::set_uid for semantics.
    // Sources rarely hold checkpoint-able state in keyed form (offsets
    // go through snapshot_offset / restore_offset), but having uid
    // here keeps the API uniform and lets users tag sources for
    // metrics / logging purposes.
    void set_uid(std::string s) noexcept { uid_ = std::move(s); }
    [[nodiscard]] const std::string& uid() const noexcept { return uid_; }

    void set_display_name(std::string s) noexcept { display_name_ = std::move(s); }
    [[nodiscard]] const std::string& display_name() const noexcept { return display_name_; }

    void attach_runtime(RuntimeContext* ctx) noexcept { runtime_ = ctx; }
    RuntimeContext* runtime() const noexcept { return runtime_; }

    // ---- Checkpointable-source hooks -----------------------------------
    //
    // The default implementations are no-ops: legacy sources stay at the
    // historic "replay from offset 0 on restart" behaviour (at-least-once
    // at the source boundary). Sources that override these participate
    // in pipeline-wide exactly-once: snapshot_offset() persists whatever
    // "where I am" state the source needs to resume from, and
    // restore_offset() loads it back at startup before produce() is
    // first called. Both run on the source runner thread, never
    // concurrently with produce() - see add_source() in dag.hpp for the
    // exact ordering, which is what makes the protocol race-free.
    virtual void snapshot_offset(StateBackend& /*backend*/,
                                 OperatorId /*op_id*/,
                                 CheckpointId /*ckpt_id*/) {}
    virtual bool restore_offset(StateBackend& /*backend*/, OperatorId /*op_id*/) { return false; }

    // Barrier handoff: the cluster's source-barrier injector pushes
    // here instead of into the downstream channel directly. The source
    // runner loop drains the queue between produce() calls, calling
    // snapshot_offset before each barrier emit so the offset and the
    // barrier reach storage atomically with respect to the record
    // stream. This is the core of the source-side exactly-once
    // protocol - moving the barrier emission inside the produce loop
    // gives us a sync point that doesn't race with produce().
    void inject_pending_barrier(CheckpointBarrier b) {
        std::lock_guard lock(barrier_mu_);
        pending_barriers_.push_back(b);
    }
    std::optional<CheckpointBarrier> take_pending_barrier() {
        std::lock_guard lock(barrier_mu_);
        if (pending_barriers_.empty()) {
            return std::nullopt;
        }
        auto b = pending_barriers_.front();
        pending_barriers_.pop_front();
        return b;
    }

private:
    OperatorId id_{0};
    std::string uid_;
    std::string display_name_;
    std::atomic<bool> cancelled_{false};
    RuntimeContext* runtime_{nullptr};
    std::mutex barrier_mu_;
    std::deque<CheckpointBarrier> pending_barriers_;
};

// ChainedOperator<A, B, C> wraps two operators (op_a: A->B, op_b: B->C)
// into a single Operator<A, C> whose process() dispatches op_b directly
// from op_a's output via a forwarding Emitter, with no BoundedChannel
// in between. This is the  operator-chaining optimisation: when
// two adjacent ops live in the same thread, the second op's process()
// is called from the first op's emit() with zero queueing overhead.
//
// Watermarks, barriers, and end-of-stream flush are forwarded through
// the same direct path. open()/close() cascade in declaration order.
template <typename A, typename B, typename C>
class ChainedOperator final : public Operator<A, C> {
public:
    ChainedOperator(std::shared_ptr<Operator<A, B>> a,
                    std::shared_ptr<Operator<B, C>> b,
                    std::string name = "chained")
        : a_(std::move(a)), b_(std::move(b)), name_(std::move(name)) {}

    void open() override {
        // Each inner op needs its own RuntimeContext so its TimerService
        // stays isolated from the other inner op. We mint per-inner RCs
        // lazily here so we can copy state-backend / metrics /
        // checkpoint-ack / side-output channels from the parent RC that
        // was attached to us by the runner.
        //
        // Without this, both inner ops would write timers into the same
        // TimerService and we'd have no way to route a fire to the
        // right op (a fire is just a (timestamp, key) pair - no op id).
        auto* parent = this->runtime();
        if (parent != nullptr) {
            a_rc_ = std::make_unique<RuntimeContext>(
                parent->operator_id(), a_->name(), parent->state_backend(), parent->metrics());
            b_rc_ = std::make_unique<RuntimeContext>(
                parent->operator_id(), b_->name(), parent->state_backend(), parent->metrics());
            // Carry the checkpoint-ack so an inner op snapshotting via
            // its own RC still notifies the JM. Both inner ops share the
            // same job-level ack callback.
            if (parent->checkpoint_ack()) {
                a_rc_->set_checkpoint_ack(parent->checkpoint_ack());
                b_rc_->set_checkpoint_ack(parent->checkpoint_ack());
            }
            // Propagate side-output channels to both inner ops. The
            // channel map is keyed by OutputTag id (a plain string), so
            // sharing it is safe - multiple inner ops emitting to the
            // same tag write into the same channel, matching // "side output by tag" semantics
            // where any op in the chain can contribute. Without this, inner_op->runtime()->
            // side_output<T>(tag) throws "tag not registered" because
            // the inner RC has an empty channels map.
            a_rc_->set_side_output_channels(parent->side_output_channels());
            b_rc_->set_side_output_channels(parent->side_output_channels());
            // Propagate the restore key-group range so each inner op's
            // restore_timers routes timers the same way the runner-attached
            // parent RC would.
            a_rc_->set_restore_key_group_range(parent->restore_key_group_range());
            b_rc_->set_restore_key_group_range(parent->restore_key_group_range());
        }
        if (a_rc_) {
            a_->attach_runtime(a_rc_.get());
        }
        if (b_rc_) {
            b_->attach_runtime(b_rc_.get());
        }
        a_->open();
        b_->open();
    }

    void close() override {
        b_->close();
        a_->close();
        if (a_rc_) {
            a_->attach_runtime(nullptr);
        }
        if (b_rc_) {
            b_->attach_runtime(nullptr);
        }
    }

    void process(const StreamElement<A>& element, Emitter<C>& out) override {
        // Build a forwarding Emitter<B> whose emit() calls b_->process
        // directly with the supplied stream element. No BoundedChannel,
        // no thread switch - this is the whole point of the chain.
        Emitter<B> forwarder(typename Emitter<B>::Forward([this, &out](StreamElement<B> e) {
            b_->process(e, out);
            return true;
        }));
        a_->process(element, forwarder);
    }

    void on_watermark(Watermark wm, Emitter<C>& out) override {
        Emitter<B> forwarder(typename Emitter<B>::Forward([this, &out](StreamElement<B> e) {
            if (e.is_watermark()) {
                b_->on_watermark(e.as_watermark(), out);
            } else if (e.is_barrier()) {
                b_->on_barrier(e.as_barrier(), out);
            } else {
                // op_a forwarded a data element from on_watermark
                // (unusual but legal). Push it through b_.
                b_->process(e, out);
            }
            return true;
        }));
        a_->on_watermark(wm, forwarder);
    }

    void on_barrier(CheckpointBarrier b, Emitter<C>& out) override {
        Emitter<B> forwarder(typename Emitter<B>::Forward([this, &out](StreamElement<B> e) {
            if (e.is_barrier()) {
                b_->on_barrier(e.as_barrier(), out);
            } else if (e.is_watermark()) {
                b_->on_watermark(e.as_watermark(), out);
            } else {
                b_->process(e, out);
            }
            return true;
        }));
        a_->on_barrier(b, forwarder);
    }

    void flush(Emitter<C>& out) override {
        // a_'s flush may emit residual records; route them through b_.
        // After a_ finishes its flush we flush b_ too.
        Emitter<B> forwarder(typename Emitter<B>::Forward([this, &out](StreamElement<B> e) {
            if (e.is_data()) {
                b_->process(e, out);
            } else if (e.is_watermark()) {
                b_->on_watermark(e.as_watermark(), out);
            } else {
                b_->on_barrier(e.as_barrier(), out);
            }
            return true;
        }));
        a_->flush(forwarder);
        b_->flush(out);
    }

    // Forward timer firing through the chain. Each inner op has its
    // own TimerService (via its own RuntimeContext); a fire in a_'s TS
    // dispatches through a forwarder that routes a_'s emissions into
    // b_->process / b_->on_watermark / b_->on_barrier, the same way
    // process()'s data path does. Fires in b_'s TS go straight to the
    // outer emitter - b_ is the last hop.
    //
    // Inner ops may themselves be ChainedOperators (nested chaining
    // for length>=3); their fire_due_timers recursively dispatches
    // through their own inner ops. The runner only sees the outermost
    // chain; the recursion happens here.
    void fire_due_timers(Emitter<C>& out, std::int64_t now_ms) override {
        Emitter<B> forwarder(typename Emitter<B>::Forward([this, &out](StreamElement<B> e) {
            if (e.is_data()) {
                b_->process(e, out);
            } else if (e.is_watermark()) {
                b_->on_watermark(e.as_watermark(), out);
            } else {
                b_->on_barrier(e.as_barrier(), out);
            }
            return true;
        }));
        a_->fire_due_timers(forwarder, now_ms);
        b_->fire_due_timers(out, now_ms);
    }

    // Fan timer checkpoint/restore out to both inner ops. Each inner op
    // has its own per-RC TimerService, so they get distinct operator-state
    // slots ("0" / "1"); nested chains extend the slot recursively.
    void snapshot_timers(StateBackend& backend,
                         OperatorId op_id,
                         const std::string& slot = "") override {
        a_->snapshot_timers(backend, op_id, slot + "0");
        b_->snapshot_timers(backend, op_id, slot + "1");
    }
    void restore_timers(StateBackend& backend,
                        OperatorId op_id,
                        const std::string& slot = "") override {
        a_->restore_timers(backend, op_id, slot + "0");
        b_->restore_timers(backend, op_id, slot + "1");
    }

    std::optional<std::int64_t> next_timer_deadline_ms() const override {
        const auto a_next = a_->next_timer_deadline_ms();
        const auto b_next = b_->next_timer_deadline_ms();
        if (!a_next.has_value()) {
            return b_next;
        }
        if (!b_next.has_value()) {
            return a_next;
        }
        return std::min(*a_next, *b_next);
    }

    std::string name() const override { return name_; }

private:
    std::shared_ptr<Operator<A, B>> a_;
    std::shared_ptr<Operator<B, C>> b_;
    std::string name_;
    // Per-inner-op RuntimeContexts so each inner op has its own
    // TimerService (and, when Stage F-4 lands, its own side_outputs
    // map). Allocated in open() from the parent RC the runner attached
    // to us; nullptr if the chain runs without a runner-attached RC
    // (e.g. in some unit tests that don't go through the full DAG).
    std::unique_ptr<RuntimeContext> a_rc_;
    std::unique_ptr<RuntimeContext> b_rc_;
};

// CoOperator is a two-input operator. Each input has its own typed
// element stream (In1 / In2) and is dispatched through a separate
// process_element method. The operator emits Out elements via the
// shared Emitter. Watermarks across the two inputs are aligned by the
// runtime (downstream watermark = min); barriers are Chandy-Lamport
// aligned across both inputs.
//
// CoOperator is the clink the CoProcessFunction.
// Shared state (via runtime()->keyed_state<>) is the way both
// process_element methods coordinate.
template <typename In1, typename In2, typename Out>
class CoOperator {
public:
    virtual ~CoOperator() = default;

    virtual void open() {}
    virtual void close() {}

    // Process one element from the left input. Element kind is data,
    // watermark, or barrier; the runtime hands watermarks/barriers to
    // on_watermark/on_barrier directly so process_element1 only needs
    // to handle data (default).
    virtual void process_element1(const StreamElement<In1>& element, Emitter<Out>& out) = 0;
    virtual void process_element2(const StreamElement<In2>& element, Emitter<Out>& out) = 0;

    // Opt-in async-state path (mirrors Operator<In,Out>). When supports_async()
    // and the backend defers reads, the co-operator runner routes data through
    // process_async{1,2} - each submits a coroutine per record to the ONE
    // per-subtask AsyncExecutionController, so a slow/remote read for one key
    // overlaps progress on other keys. Both inputs share the controller (and
    // thus the per-key gate + the epoch), so same-key records from EITHER input
    // serialise and observe each other's writes to the shared keyed state. The
    // sync process_element{1,2} above are the fallback for a non-deferring
    // backend and must produce identical output.
    [[nodiscard]] virtual bool supports_async() const noexcept { return false; }
    virtual void process_async1(const StreamElement<In1>& /*element*/,
                                Emitter<Out>& /*out*/,
                                AsyncExecutionController& /*aec*/) {}
    virtual void process_async2(const StreamElement<In2>& /*element*/,
                                Emitter<Out>& /*out*/,
                                AsyncExecutionController& /*aec*/) {}

    // ASYNC-10 opt-in (mirrors Operator<In,Out>::coalesce_reads): when true (and
    // supports_async() + a deferring backend), the co-op runner wraps the
    // backend in a CoalescingBackend so a process_async1/2 batch's per-record
    // get_async calls collapse into one get_many_async. Default false.
    [[nodiscard]] virtual bool coalesce_reads() const noexcept { return false; }

    // Timer-kind flags (see Operator<In,Out>): the co-op async runner gates
    // processing-time timers through the per-key gate and fires event-time
    // timers inside the merged-watermark epoch release, so both are safe.
    [[nodiscard]] virtual bool fires_state_touching_timers() const noexcept { return false; }
    [[nodiscard]] virtual bool fires_state_touching_processing_time_timers() const noexcept {
        return fires_state_touching_timers();
    }

    // Default watermark handling: fire any event-time timers whose
    // timestamp ≤ wm, then forward unchanged. Barriers forward as-is.
    virtual void on_watermark(Watermark wm, Emitter<Out>& out) {
        fire_due_event_time_timers(out, wm.timestamp().millis());
        out.emit_watermark(wm);
    }
    virtual void on_barrier(CheckpointBarrier barrier, Emitter<Out>& out) {
        out.emit_barrier(barrier);
    }

    // Processing-time timer fired between input pops on the operator's
    // thread. See Operator::on_processing_time_timer.
    virtual void on_processing_time_timer(std::int64_t /*timestamp_ms*/,
                                          const std::string& /*key*/,
                                          Emitter<Out>& /*out*/) {}

    // Event-time timer fired when an upstream watermark advances past
    // the registered timestamp. Same threading discipline as the
    // processing-time variant. Default no-op.
    virtual void on_event_time_timer(std::int64_t /*timestamp_ms*/,
                                     const std::string& /*key*/,
                                     Emitter<Out>& /*out*/) {}

    // Polls + fires every event-time timer due at watermark_ms.
    virtual void fire_due_event_time_timers(Emitter<Out>& out, std::int64_t watermark_ms) {
        if (runtime_ == nullptr) {
            return;
        }
        runtime_->timer_service()->poll_due_event_time(
            watermark_ms, [this, &out](std::int64_t ts, const std::string& key) {
                this->on_event_time_timer(ts, key, out);
            });
    }

    // Timer checkpointing - both inputs share one per-operator
    // TimerService, so this mirrors the single-input Operator hooks. See
    // Operator::snapshot_timers for the same-parallelism-only contract.
    virtual void snapshot_timers(StateBackend& backend,
                                 OperatorId op_id,
                                 const std::string& slot = "") {
        if (runtime_ == nullptr) {
            return;
        }
        detail::snapshot_timer_service(*runtime_->timer_service(), backend, op_id, slot);
    }
    virtual void restore_timers(StateBackend& backend,
                                OperatorId op_id,
                                const std::string& slot = "") {
        if (runtime_ == nullptr) {
            return;
        }
        detail::restore_timer_service(
            *runtime_->timer_service(), backend, op_id, slot, runtime_->restore_key_group_range());
    }

    // Flush hook for residual emissions after both inputs close.
    virtual void flush(Emitter<Out>& /*out*/) {}

    virtual std::string name() const { return "co_operator"; }
    OperatorId id() const noexcept { return id_; }
    void set_id(OperatorId id) noexcept { id_ = id; }

    // Stable identity - see Operator<I,O>::set_uid for semantics.
    void set_uid(std::string s) noexcept { uid_ = std::move(s); }
    [[nodiscard]] const std::string& uid() const noexcept { return uid_; }

    void set_display_name(std::string s) noexcept { display_name_ = std::move(s); }
    [[nodiscard]] const std::string& display_name() const noexcept { return display_name_; }

    void attach_runtime(RuntimeContext* ctx) noexcept { runtime_ = ctx; }
    RuntimeContext* runtime() const noexcept { return runtime_; }

private:
    OperatorId id_{0};
    std::string uid_;
    std::string display_name_;
    RuntimeContext* runtime_{nullptr};
};

// Sinks terminate the DAG - they consume StreamElement<In> and produce nothing.
template <typename In>
class Sink {
public:
    virtual ~Sink() = default;

    virtual void open() {}
    virtual void close() {}

    virtual void on_data(const Batch<In>& batch) = 0;
    // Move-friendly overload: sinks that can take ownership of the
    // incoming batch (NetworkBridgeSink forwarding to a channel,
    // codec-encoding sinks, etc.) override this to avoid the
    // const-ref + internal copy pattern. Default delegates to the
    // const-ref version so existing sinks Just Work. The dag runner
    // owns the StreamElement that holds the batch and does not
    // observe it after the on_data call, so the move is safe.
    virtual void on_data(Batch<In>&& batch) { on_data(static_cast<const Batch<In>&>(batch)); }
    virtual void on_watermark(Watermark /*wm*/) {}
    virtual void on_barrier(CheckpointBarrier /*b*/) {}
    // 2PC phase-2 hook. Called when the JM has confirmed checkpoint
    // `checkpoint_id` is globally durable (every subtask acked, the
    // COMPLETED-N marker is on disk, the JM broadcast CommitCheckpoint).
    // Non-2PC sinks ignore this; 2PC sinks finalise their pre-committed
    // transaction (atomic rename, Kafka commitTransaction, SQL COMMIT
    // PREPARED). Must be idempotent - a recovery-time on_commit for an
    // already-committed checkpoint may legitimately fire.
    virtual void on_commit(std::uint64_t /*checkpoint_id*/) {}
    // Phase 30c hook (declared here, wired later). Called when the JM
    // determines this sink's commit group cannot commit atomically
    // (one or more members of the group failed their pre-commit). The
    // sink rolls back any prepared state: file_2pc unlinks staging
    // files, kafka_2pc calls abort_transaction(). Non-2PC sinks
    // ignore this. Default no-op preserves pre-Phase-30 behaviour.
    virtual void on_abort(std::uint64_t /*checkpoint_id*/) {}
    // Called once after the input channel is closed and drained, before
    // close(). Sinks that buffer (e.g. for batched commits) flush here.
    virtual void flush() {}

    virtual std::string name() const { return "sink"; }
    OperatorId id() const noexcept { return id_; }
    void set_id(OperatorId id) noexcept { id_ = id; }

    // Stable identity - see Operator<I,O>::set_uid for semantics.
    void set_uid(std::string s) noexcept { uid_ = std::move(s); }
    [[nodiscard]] const std::string& uid() const noexcept { return uid_; }

    void set_display_name(std::string s) noexcept { display_name_ = std::move(s); }
    [[nodiscard]] const std::string& display_name() const noexcept { return display_name_; }

    void attach_runtime(RuntimeContext* ctx) noexcept { runtime_ = ctx; }
    RuntimeContext* runtime() const noexcept { return runtime_; }

    // Phase 30a: commit-group declaration. Sinks that share a non-empty
    // commit_group must commit together or all abort together; the
    // JobManager will only broadcast CommitCheckpoint to group
    // members once every member of the group has acked its pre-commit
    // (on_barrier). Empty string (the default) means "this sink is
    // not in any commit group; it commits independently as soon as
    // the JM marks the checkpoint COMPLETED" - the pre-Phase-30
    // behaviour. 30a ships the declaration + accessor only.
    void set_commit_group(std::string group) noexcept { commit_group_ = std::move(group); }
    [[nodiscard]] const std::string& commit_group() const noexcept { return commit_group_; }
    [[nodiscard]] bool has_commit_group() const noexcept { return !commit_group_.empty(); }

private:
    OperatorId id_{0};
    std::string uid_;
    std::string display_name_;
    std::string commit_group_;
    RuntimeContext* runtime_{nullptr};
};

}  // namespace clink
