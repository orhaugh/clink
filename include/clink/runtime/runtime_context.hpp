#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/core/codec.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/core/types.hpp"
#include "clink/metrics/operator_metrics.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/log_buffer.hpp"
#include "clink/runtime/output_tag.hpp"
#include "clink/runtime/timer_service.hpp"
#include "clink/state/broadcast_state.hpp"
#include "clink/state/keyed_state.hpp"
#include "clink/state/state_backend.hpp"
#include "clink/state/typed_state.hpp"

namespace clink {

template <typename T>
class Emitter;

class MetricsRegistry;

// Type-erased entry stored per side output tag. Holds the channel
// shared_ptr (typed as shared_ptr<void>) so the Dag can wire it without
// knowing T; RuntimeContext casts back to BoundedChannel<StreamElement<T>>*
// at the typed accessor. The `close_fn` closes the typed channel from
// type-erased context at end-of-stream, so downstream consumers drain.
struct SideOutputChannelEntry {
    std::shared_ptr<void> channel;   // shared_ptr<BoundedChannel<StreamElement<T>>>
    std::function<void()> close_fn;  // invokes channel->close()
};
using SideOutputChannelMap = std::unordered_map<std::string, SideOutputChannelEntry>;

// A named user accumulator handle, scoped to one operator. Obtained from
// RuntimeContext::accumulator(name). add() folds a delta into a per-operator
// value that merges across every subtask of the operator (within a process via
// one atomic gauge, across TaskManagers via the JM's per-operator aggregation),
// the clink equivalent of a Flink accumulator. Cheap to copy; holds no state of
// its own beyond the (registry, op id, name) it writes through.
class Accumulator {
public:
    Accumulator() = default;
    Accumulator(MetricsRegistry* reg, std::uint64_t op_id, std::string name)
        : reg_(reg), op_id_(op_id), name_(std::move(name)) {}

    // Add `delta` (may be negative). Default +1 for the count case.
    void add(std::int64_t delta = 1) const {
        clink::metrics::op::accumulator_add(reg_, op_id_, name_, delta);
    }

private:
    MetricsRegistry* reg_{nullptr};
    std::uint64_t op_id_{0};
    std::string name_;
};

// RuntimeContext is the per-operator handle the engine hands to user code at
// open() time. It is the only sanctioned way for an operator to reach state,
// metrics, or its own identity.
//
// Lifetime: the executor owns the RuntimeContext for the entire run; the
// operator holds a non-owning pointer via Operator::attach_runtime(). Do
// not stash a copy elsewhere.
class RuntimeContext {
public:
    RuntimeContext(OperatorId op_id,
                   std::string op_name,
                   StateBackend* state_backend,
                   MetricsRegistry* metrics)
        : op_id_(op_id), op_name_(std::move(op_name)), backend_(state_backend), metrics_(metrics) {}

    OperatorId operator_id() const noexcept { return op_id_; }
    const std::string& operator_name() const noexcept { return op_name_; }

    StateBackend* state_backend() const noexcept { return backend_; }
    bool has_state_backend() const noexcept { return backend_ != nullptr; }
    // ASYNC-10: the single-input runner swaps in a CoalescingBackend wrapping
    // this one before the operator binds its KeyedState (and restores it at
    // teardown), so an opted-in operator's reads route through the coalescer
    // transparently. Runner-thread-only, set before open().
    void set_state_backend(StateBackend* b) noexcept { backend_ = b; }

    MetricsRegistry* metrics() const noexcept { return metrics_; }

    // Named user accumulator scoped to this operator. The returned handle's
    // add() merges across all of the operator's subtasks (Flink-style
    // accumulator), surfaced per operator on GET /api/v1/jobs/:id/operators.
    // Routes through the host registry (metrics()), so it is correct on the
    // cluster path; a no-op when no registry is configured.
    [[nodiscard]] Accumulator accumulator(std::string name) {
        return Accumulator{metrics_, op_id_.value(), std::move(name)};
    }

    // Logging seam. The executor sets the host-owned logger (threaded across
    // the plugin boundary by data); operators log via the log_* helpers, which
    // route through clink::logging::op_log so the record reaches the node's
    // sinks and the /api/v1/logs ring with this operator's name as the source.
    // Operators MUST use these rather than the clink::log facade or
    // spdlog::default_logger(), which resolve a private per-.so registry.
    void set_logger(spdlog::logger* lg) noexcept { host_logger_ = lg; }
    spdlog::logger* logger() const noexcept { return host_logger_; }
    void log(LogSeverity level, std::string_view message) const {
        clink::logging::op_log(host_logger_, level, op_name_, message);
    }
    void log_debug(std::string_view message) const { log(LogSeverity::Debug, message); }
    void log_info(std::string_view message) const { log(LogSeverity::Info, message); }
    void log_warn(std::string_view message) const { log(LogSeverity::Warn, message); }
    void log_error(std::string_view message) const { log(LogSeverity::Error, message); }

    // Per-operator processing-time TimerService. Always present; users
    // call timer_service()->register_processing_time_timer(...) from
    // open()/process()/on_*_timer().
    TimerService* timer_service() noexcept { return &timer_service_; }
    const TimerService* timer_service() const noexcept { return &timer_service_; }

    // Optional ack callback the operator runner invokes after a
    // snapshot completes. Mirrors JobConfig::on_checkpoint_ack;
    // LocalExecutor copies it onto each context at startup. Empty for
    // in-process / non-cluster runs.
    using CheckpointAckFn = std::function<void(CheckpointId, bool /*ok*/, std::string /*error*/)>;
    void set_checkpoint_ack(CheckpointAckFn fn) noexcept { ack_fn_ = std::move(fn); }
    const CheckpointAckFn& checkpoint_ack() const noexcept { return ack_fn_; }

    // Bounded-source end-of-stream FINAL checkpoint (cluster path only).
    // At clean bounded EOS the source runner asks the JM to trigger ONE
    // JM-coordinated checkpoint that durably captures the EOS offset and drives
    // the sink's tail commit through the normal ack -> CommitCheckpoint path,
    // then BLOCKS until that checkpoint is committed before the runner returns.
    // Because the runner's return is what emits SubtaskFinished (which drives
    // job completion), this makes "job complete" impossible before the tail is
    // durable, and a crash in the window leaves the source unfinished -> restart
    // -> replay -> re-commit. Both hooks are empty on in-process / non-cluster
    // runs, where the source falls back to the local terminal commit (unchanged).
    // request_final_checkpoint() returns the JM-assigned final checkpoint id, or
    // 0 if the JM declined (job cancelling/completing) or the hook is unwired.
    using RequestFinalCheckpointFn = std::function<std::uint64_t()>;
    using WaitFinalCommittedFn = std::function<bool(std::uint64_t, std::chrono::milliseconds)>;
    void set_request_final_checkpoint(RequestFinalCheckpointFn fn) noexcept {
        request_final_ckpt_ = std::move(fn);
    }
    const RequestFinalCheckpointFn& request_final_checkpoint() const noexcept {
        return request_final_ckpt_;
    }
    void set_wait_final_committed(WaitFinalCommittedFn fn) noexcept {
        wait_final_committed_ = std::move(fn);
    }
    const WaitFinalCommittedFn& wait_final_committed() const noexcept {
        return wait_final_committed_;
    }

    // Aligned vs unaligned checkpoint barrier handling. Set by the
    // executor at open(); multi-input operator runners read it to
    // decide whether to wait for every input to deliver a barrier
    // (aligned) or to forward on the first delivery and capture the
    // in-flight records on the others (unaligned).
    void set_unaligned_checkpoints(bool v) noexcept { unaligned_checkpoints_ = v; }
    bool unaligned_checkpoints() const noexcept { return unaligned_checkpoints_; }

    // The key-group range this run's state restore loaded: the full range
    // {0, kNumKeyGroups} for a plain same-parallelism restore, or a narrowed
    // slice for a rescale. Set by the executor from the restore key-group
    // filter. Operators consult it to ROUTE non-key-group-narrowed state on
    // a rescale: checkpointed timers ride operator-state (broadcast to every
    // new subtask), so restore_timers keeps only the timers whose key falls
    // in this subtask's range. Defaults to the full range (same-parallelism /
    // fresh start, where every timer is kept).
    void set_restore_key_group_range(KeyGroupRange r) noexcept { restore_kg_range_ = r; }
    [[nodiscard]] KeyGroupRange restore_key_group_range() const noexcept {
        return restore_kg_range_;
    }
    [[nodiscard]] bool restore_key_groups_cover_all() const noexcept {
        return restore_kg_range_.covers_all();
    }

    // Phase 29d-3: drain-target signal. The shared atomic (if set
    // by the executor at startup) is the rendezvous between the
    // cluster's BeginRescale dispatch and the source runner: the
    // TM's drain callback sets the atomic to target_parallelism;
    // the source runner's produce loop polls drain_target() and
    // emits a DrainMarker downstream when it sees a non-zero value.
    // Null pointer (LocalExecutor / non-cluster path) means
    // drain_target() always returns 0 - source produces normally.
    void set_drain_target_signal(std::shared_ptr<std::atomic<std::uint32_t>> sig) noexcept {
        drain_target_ = std::move(sig);
    }
    std::uint32_t drain_target() const noexcept {
        if (!drain_target_)
            return 0;
        return drain_target_->load(std::memory_order_acquire);
    }

    // Phase 26b: per-operator alignment mode override. When set, the
    // operator's runner stamps every barrier passing through with
    // this mode before the aligner pins it; the override propagates
    // to downstream operators via the forwarded barrier. Nullopt
    // means "no override at this operator; honour the upstream stamp."
    void set_barrier_mode_override(std::optional<CheckpointBarrier::Mode> mode) noexcept {
        barrier_mode_override_ = mode;
    }
    std::optional<CheckpointBarrier::Mode> barrier_mode_override() const noexcept {
        return barrier_mode_override_;
    }
    // Helper: return `b` with the operator's mode override applied (if
    // set). Used by multi-input operator runners to stamp the override
    // before the aligner pins the per-checkpoint mode. No-op when no
    // override is configured for this operator.
    CheckpointBarrier apply_barrier_mode_override(CheckpointBarrier b) const noexcept {
        if (!barrier_mode_override_) {
            return b;
        }
        return CheckpointBarrier{b.id(), b.is_terminal(), *barrier_mode_override_};
    }

    // Set by the executor before open() with the side output channels
    // the Dag pre-registered for this operator. Operator user code reads
    // the channel via side_output<T>(tag).
    void set_side_output_channels(SideOutputChannelMap channels) {
        side_outputs_ = std::move(channels);
    }
    const SideOutputChannelMap& side_output_channels() const noexcept { return side_outputs_; }

    // Look up the side output channel for `tag` and return a typed
    // Emitter<T> wrapping it. Throws if the tag wasn't registered on
    // this operator's Dag stage.
    template <typename T>
    Emitter<T> side_output(const OutputTag<T>& tag) {
        auto it = side_outputs_.find(tag.id);
        if (it == side_outputs_.end()) {
            throw std::runtime_error("RuntimeContext::side_output: tag '" + tag.id +
                                     "' was not registered via Dag::side_output() on this stage");
        }
        auto* ch = static_cast<BoundedChannel<StreamElement<T>>*>(it->second.channel.get());
        Emitter<T> out(ch);
        // Count emits through this tagged channel toward side_output_records_total
        // (via the host registry), keyed by the producing operator's id.
        out.set_metrics_registry(metrics_);
        out.set_side_output(true);
        // Side-output emits also count toward the producing operator's
        // records_out_total + side_output_records_total. The latter is
        // bumped lazily on each emit through this Emitter via a small
        // forwarding wrapper; for now we just stamp the op_id so the
        // standard records_out bump fires. Tests requiring a dedicated
        // side_output_records_total reading should drive the operator
        // through the registry's snapshot rather than asserting on the
        // raw Emitter object.
        out.set_operator_id(op_id_.value());
        return out;
    }

    // Construct a typed keyed-state slot scoped to this operator. Throws if
    // no state backend is configured for the job.
    template <typename K, typename V>
    KeyedState<K, V> keyed_state(std::string slot_name, Codec<K> kc, Codec<V> vc) {
        if (backend_ == nullptr) {
            throw std::runtime_error(
                "RuntimeContext::keyed_state: no state backend configured "
                "(set JobConfig::state_backend before running)");
        }
        return KeyedState<K, V>(
            *backend_, op_id_, std::move(slot_name), std::move(kc), std::move(vc));
    }

    // TTL-aware overload. Opting in here is per-slot; the TTL stamps a
    // leading 8B expire-at on every put, lazy-purges expired entries on
    // read, and skips them in scan. Matches
    // `StateTtlConfig.newBuilder(Time)` shape - pass
    // `TtlConfig{.ttl=..., .refresh_on_write=true, .refresh_on_read=false}`
    // for the typical "expire N hours after last write" pattern.
    template <typename K, typename V>
    KeyedState<K, V> keyed_state(std::string slot_name, Codec<K> kc, Codec<V> vc, TtlConfig ttl) {
        if (backend_ == nullptr) {
            throw std::runtime_error(
                "RuntimeContext::keyed_state: no state backend configured "
                "(set JobConfig::state_backend before running)");
        }
        return KeyedState<K, V>(
            *backend_, op_id_, std::move(slot_name), std::move(kc), std::move(vc), ttl);
    }

    // Construct a typed broadcast-state slot scoped to this operator. Throws
    // if no state backend is configured for the job.
    template <typename V>
    BroadcastState<V> broadcast_state(std::string slot_name, Codec<V> vc) {
        if (backend_ == nullptr) {
            throw std::runtime_error(
                "RuntimeContext::broadcast_state: no state backend configured");
        }
        return BroadcastState<V>(*backend_, op_id_, std::move(slot_name), std::move(vc));
    }

    // Typed keyed-state primitives (FOUND-1), each scoped to this operator via
    // its own slot name. Throw if no state backend is configured.
    template <typename K, typename E>
    ListState<K, E> list_state(std::string slot_name, Codec<K> kc, Codec<E> ec) {
        require_backend_("list_state");
        return ListState<K, E>(
            *backend_, op_id_, std::move(slot_name), std::move(kc), std::move(ec));
    }

    template <typename K, typename MK, typename MV>
    MapState<K, MK, MV> map_state(std::string slot_name,
                                  Codec<K> kc,
                                  Codec<MK> mkc,
                                  Codec<MV> mvc) {
        require_backend_("map_state");
        return MapState<K, MK, MV>(
            *backend_, op_id_, std::move(slot_name), std::move(kc), std::move(mkc), std::move(mvc));
    }

    template <typename K, typename In, typename Acc, typename Out>
    AggregatingState<K, In, Acc, Out> aggregating_state(
        std::string slot_name,
        Codec<K> kc,
        Codec<Acc> acc_codec,
        typename AggregatingState<K, In, Acc, Out>::Initial initial,
        typename AggregatingState<K, In, Acc, Out>::AddFn add_fn,
        typename AggregatingState<K, In, Acc, Out>::ResultFn result_fn) {
        require_backend_("aggregating_state");
        return AggregatingState<K, In, Acc, Out>(*backend_,
                                                 op_id_,
                                                 std::move(slot_name),
                                                 std::move(kc),
                                                 std::move(acc_codec),
                                                 std::move(initial),
                                                 std::move(add_fn),
                                                 std::move(result_fn));
    }

    template <typename K, typename V>
    ReducingState<K, V> reducing_state(std::string slot_name,
                                       Codec<K> kc,
                                       Codec<V> vc,
                                       typename ReducingState<K, V>::ReduceFn reduce_fn) {
        require_backend_("reducing_state");
        return ReducingState<K, V>(*backend_,
                                   op_id_,
                                   std::move(slot_name),
                                   std::move(kc),
                                   std::move(vc),
                                   std::move(reduce_fn));
    }

private:
    void require_backend_(const char* what) const {
        if (backend_ == nullptr) {
            throw std::runtime_error(std::string("RuntimeContext::") + what +
                                     ": no state backend configured");
        }
    }

    OperatorId op_id_;
    std::string op_name_;
    StateBackend* backend_{nullptr};
    MetricsRegistry* metrics_{nullptr};
    spdlog::logger* host_logger_{nullptr};
    TimerService timer_service_{};
    SideOutputChannelMap side_outputs_;
    CheckpointAckFn ack_fn_;
    RequestFinalCheckpointFn request_final_ckpt_;
    WaitFinalCommittedFn wait_final_committed_;
    bool unaligned_checkpoints_{false};
    KeyGroupRange restore_kg_range_{};  // default {0, kNumKeyGroups} = covers all
    std::optional<CheckpointBarrier::Mode> barrier_mode_override_;
    std::shared_ptr<std::atomic<std::uint32_t>> drain_target_;
};

}  // namespace clink
