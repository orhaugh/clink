#pragma once

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#ifdef CLINK_HAS_ARROW
#include <arrow/api.h>
#endif

#include "clink/core/codec.hpp"
#include "clink/core/hash_map.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/operators/window_state.hpp"
#include "clink/operators/window_trigger.hpp"
#include "clink/runtime/output_tag.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/keyed_state.hpp"

namespace clink {

namespace detail {

// Hash for std::pair<int64_t, Key> used by the in-memory window-state path.
template <typename Key>
struct PairKeyHash {
    std::size_t operator()(const std::pair<std::int64_t, Key>& p) const noexcept {
        return std::hash<std::int64_t>{}(p.first) ^ (std::hash<Key>{}(p.second) << 1);
    }
};

// TriggerContext implementation backed by the operator's view of the
// engine's time signals. Triggers don't need to register timers in the
// MVP - operators dispatch on_event_time directly when the watermark
// advances past a window's end.
class OperatorTriggerContext final : public TriggerContext<TimeWindow> {
public:
    void set_watermark(std::int64_t v) noexcept { wm_ = v; }
    void set_processing_time(std::int64_t v) noexcept { pt_ = v; }

    std::int64_t current_watermark() const noexcept override { return wm_; }
    std::int64_t current_processing_time() const noexcept override { return pt_; }

private:
    std::int64_t wm_{Watermark::min().timestamp().millis()};
    std::int64_t pt_{0};
};

inline std::int64_t now_processing_time_ms() noexcept {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

}  // namespace detail

// Keyed event-time tumbling window. Models // TumblingEventTimeWindows.of(size).
//
// Trigger model:
//   * EventTimeTrigger by default - fires on watermark >= window_end.
//     Matches the hardcoded behavior the operator had before triggers
//     were pluggable.
//   * Override via with_trigger() to plug in ProcessingTimeTrigger,
//     CountTrigger, or any user-supplied Trigger<Value, TimeWindow>.
//
// allowed_lateness still applies on top of the trigger:
//   * If the trigger fires the window (without purging), the window is
//     marked fired - late records arriving in [window_end,
//     window_end + allowed_lateness] update the aggregate AND re-emit.
//   * Purge happens when watermark >= window_end + allowed_lateness OR
//     when the trigger explicitly returns Purge / FireAndPurge.
//
// Two execution modes:
//   * In-memory (default ctor): private std::unordered_map.
//   * Persistent (codec-bearing ctor): KeyedState<StateKey,
//     WindowEntry<Agg>>.
template <typename Key, typename Value, typename Agg>
class TumblingWindowOperator final : public Operator<std::pair<Key, Value>, std::pair<Key, Agg>> {
public:
    using Combiner = std::function<Agg(const Agg&, const Value&)>;
    using Initial = std::function<Agg()>;

    // In-memory ctor.
    TumblingWindowOperator(std::chrono::milliseconds size,
                           Initial initial,
                           Combiner combiner,
                           std::string name = "tumbling_window")
        : window_size_(size),
          initial_(std::move(initial)),
          combiner_(std::move(combiner)),
          name_(std::move(name)),
          trigger_(std::make_unique<EventTimeTrigger<Value>>()) {}

    // Persistent ctor.
    TumblingWindowOperator(std::chrono::milliseconds size,
                           Initial initial,
                           Combiner combiner,
                           Codec<Key> key_codec,
                           Codec<Agg> agg_codec,
                           std::string name = "tumbling_window")
        : window_size_(size),
          initial_(std::move(initial)),
          combiner_(std::move(combiner)),
          name_(std::move(name)),
          key_codec_(std::move(key_codec)),
          agg_codec_(std::move(agg_codec)),
          trigger_(std::make_unique<EventTimeTrigger<Value>>()) {}

    // Configure how long after window_end this operator retains state to
    // accept late records. Default 0 = drop late records.
    TumblingWindowOperator& allowed_lateness(std::chrono::milliseconds v) {
        allowed_lateness_ = v;
        return *this;
    }

    // Register an OutputTag to receive records that arrive AFTER the
    // window has been purged (i.e. when current_watermark >=
    // window_end + allowed_lateness for the record's would-be
    // window). The record is forwarded as-is via the side output,
    // typed on Value - mirrors
    // .sideOutputLateData(OutputTag<T>). When unset, late-late
    // records are silently dropped (the historic behavior).
    //
    // Records that arrive within allowed_lateness are NOT routed to
    // the side output - they go through the normal re-fire path so
    // the aggregate updates. Only records past the lateness deadline
    // hit the side output.
    TumblingWindowOperator& late_output_tag(OutputTag<Value> tag) {
        late_tag_ = std::move(tag);
        return *this;
    }

    // Replace the default EventTimeTrigger. Throws if `t` is null.
    TumblingWindowOperator& with_trigger(std::unique_ptr<Trigger<Value, TimeWindow>> t) {
        if (t == nullptr) {
            throw std::invalid_argument("TumblingWindowOperator: trigger must be non-null");
        }
        trigger_ = std::move(t);
        return *this;
    }

    void open() override {
        if (key_codec_.has_value() && agg_codec_.has_value() && this->runtime() != nullptr &&
            this->runtime()->has_state_backend()) {
            keyed_ = std::make_unique<KeyedState<StateKey, Entry>>(
                this->runtime()->template keyed_state<StateKey, Entry>(
                    "windows",
                    pair_codec<std::int64_t, Key>(int64_codec(), *key_codec_),
                    window_entry_codec<Agg>(*agg_codec_)));
            // Hydrate mem_ from the backing store on startup so the
            // hot path (which reads/writes mem_ only) starts with the
            // post-restore view of state. On a fresh job this scan
            // finds nothing; on a restored job it pulls every entry
            // the coordinator's restore_from snapshot wrote into the backend.
            keyed_->scan([this](const StateKey& sk, const Entry& e) {
                mem_.emplace(sk, e);
                const std::int64_t end = sk.first + window_size_.count();
                if (end < earliest_window_end_) {
                    earliest_window_end_ = end;
                }
            });
            restore_trigger_state_();
        }
    }

    void close() override {
        keyed_.reset();
        trigger_state_.reset();
    }

    void process(const StreamElement<std::pair<Key, Value>>& element,
                 Emitter<std::pair<Key, Agg>>& out) override {
        ctx_.set_processing_time(processing_now_ms_());

        if (element.is_data()) {
            for (const auto& record : element.as_data()) {
                ingest_one_(record.value().first, record.value().second, record.event_time(), out);
            }
        } else if (element.is_watermark()) {
            const Watermark wm = element.as_watermark();
            ctx_.set_watermark(wm.timestamp().millis());
            on_watermark_advance_(wm, out);
            this->on_watermark(wm, out);
        } else {
            this->on_barrier(element.as_barrier(), out);
        }
    }

    void flush(Emitter<std::pair<Key, Agg>>& out) override {
        // End-of-stream: synthesize a max-watermark dispatch so any
        // still-open windows get fired through the trigger.
        ctx_.set_watermark(Watermark::max().timestamp().millis());
        on_watermark_advance_(Watermark::max(), out);
    }

#ifdef CLINK_HAS_ARROW
    // Toggle the columnar-native ingest fast path (default on). Disabling it
    // forces the row path even for a columnar upstream - used by the bench to
    // compare the two ingest paths on the SAME operator + SAME source.
    TumblingWindowOperator& set_columnar_enabled(bool enabled) {
        columnar_enabled_ = enabled;
        return *this;
    }

    // Columnar-native ingest (increment 3). Only int64 key + int64 value can
    // ride the 3-column {event_time, key, value} Arrow sidecar; other key/value
    // types have no columnar producer, so this stays off for them (the batch is
    // never columnar -> process() runs). The fast path folds the key+value+ts
    // buffers straight into the per-(window,key) accumulators with ZERO
    // Record<pair> materialization, by calling the SAME ingest_one_ the row path
    // uses - so triggers, late panes, allowed-lateness and watermark firing are
    // byte-identical to the row path; the only thing skipped is the row decode.
    [[nodiscard]] bool supports_columnar() const noexcept override {
        return columnar_enabled_ && std::is_same_v<Key, std::int64_t> &&
               std::is_same_v<Value, std::int64_t>;
    }

    bool process_columnar(const StreamElement<std::pair<Key, Value>>& element,
                          Emitter<std::pair<Key, Agg>>& out) override {
        if constexpr (std::is_same_v<Key, std::int64_t> && std::is_same_v<Value, std::int64_t>) {
            if (!element.is_data() || !element.as_data().is_columnar()) {
                return false;
            }
            const auto& rb = element.as_data().arrow();
            if (!rb || rb->num_columns() < 3) {
                return false;
            }
            // Schema {event_time(0, nullable), key(1), value(2)}; all int64.
            const auto* ts = dynamic_cast<const arrow::Int64Array*>(rb->column(0).get());
            const auto* key = dynamic_cast<const arrow::Int64Array*>(rb->column(1).get());
            const auto* val = dynamic_cast<const arrow::Int64Array*>(rb->column(2).get());
            if (ts == nullptr || key == nullptr || val == nullptr) {
                return false;
            }
            // All guards passed BEFORE any ingest_one_ (which may emit late
            // panes); a false return here never half-processed the batch, so the
            // runner's fallback to process() cannot double-count.
            ctx_.set_processing_time(processing_now_ms_());
            const std::int64_t n = rb->num_rows();
            for (std::int64_t i = 0; i < n; ++i) {
                const std::optional<EventTime> et =
                    ts->IsValid(i) ? std::optional<EventTime>{EventTime{ts->Value(i)}}
                                   : std::nullopt;
                ingest_one_(key->Value(i), val->Value(i), et, out);
            }
            return true;
        } else {
            (void)element;
            (void)out;
            return false;
        }
    }
#endif  // CLINK_HAS_ARROW

    // Flush the in-memory `mem_` working set through to `keyed_`
    // before forwarding the barrier downstream. This is the durability
    // pivot of the operator: between barriers, `mem_` is the
    // authoritative state and `keyed_->put` may be skipped (write-back
    // cache mode) to avoid the per-record RocksDB hop. Without this
    // override, the engine's checkpoint (taken once the barrier has
    // propagated through every operator) would capture the stale or
    // empty `keyed_` view and a crash + restore would silently drop
    // every state update since the last manual flush.
    //
    // The alternative is to write every record's state straight through
    // to RocksDB (its MemTable) for the same per-barrier durability
    // contract, accepting the per-record cost. We flip that trade-off:
    // write the in-memory map once per barrier, get the cache-hit hot
    // path between barriers.
    //
    // Strict mode (`CLINK_WB_STATE_CACHE!=1`) skips the loop because
    // `store_` already mirrors each record through to `keyed_`; the
    // flush would be wasted I/O. We use the same `wb_cache_skip_()`
    // gate `store_` uses so the two paths stay in lockstep - flipping
    // the env var changes both ends together.
    void on_barrier(CheckpointBarrier barrier, Emitter<std::pair<Key, Agg>>& out) override {
        if (keyed_ && wb_cache_skip_()) {
            for (const auto& [sk, entry] : mem_) {
                keyed_->put(sk, entry);
            }
        }
        persist_trigger_state_();
        out.emit_barrier(barrier);
    }

    std::string name() const override { return name_; }

    using StateKey = std::pair<std::int64_t, Key>;
    using Entry = WindowEntry<Agg>;

    // Per-record entry point exposed so wrapping operators can drive
    // the same state machine without first projecting every record
    // into a Batch<pair<Key, Value>>. The wrapper still owns key
    // extraction; this method takes the key+value pair already split
    // and skips the per-batch allocation/copy of the projected
    // batch. Same semantics as the public process() data path -
    // updates current_watermark, computes window, dispatches the
    // trigger, emits any due pane.
    // Snapshot processing-time once per batch from the wrapping
    // operator; callers invoke this immediately before the per-record
    // loop. Doing the clock read per record (~hundreds of ns each on
    // macOS) shows up loud on the 10M-record bench.
    void begin_batch() { ctx_.set_processing_time(processing_now_ms_()); }

    // Processing time through the operator's TimerService when attached -
    // its default NowFn is the wall clock, so production behaviour is
    // unchanged, while a manual clock (tests, deterministic replay)
    // governs this operator's processing-time view too. Falls back to the
    // wall clock pre-attach.
    [[nodiscard]] std::int64_t processing_now_ms_() const noexcept {
        auto* rt = this->runtime();
        return rt != nullptr ? rt->timer_service()->now_ms() : detail::now_processing_time_ms();
    }

    // Per-record entry point: skip the projected-batch allocation
    // that the public process() data path uses. Caller has already
    // called begin_batch() to snapshot the processing time.
    void process_record(const Key& k,
                        const Value& v,
                        std::int64_t ts,
                        Emitter<std::pair<Key, Agg>>& out) {
        const std::int64_t window_start = (ts / window_size_.count()) * window_size_.count();
        const TimeWindow window{.start = window_start, .end = window_start + window_size_.count()};
        if (late_tag_.has_value()) {
            const std::int64_t purge_at = window.end + allowed_lateness_.count();
            if (ctx_.current_watermark() >= purge_at && this->runtime() != nullptr) {
                auto side = this->runtime()->template side_output<Value>(*late_tag_);
                Batch<Value> b;
                b.emplace(v, EventTime{ts});
                side.emit_data(std::move(b));
                return;
            }
        }
        handle_record_(k, window, v, ts, out);
    }

    void process_watermark(Watermark wm, Emitter<std::pair<Key, Agg>>& out) {
        ctx_.set_watermark(wm.timestamp().millis());
        on_watermark_advance_(wm, out);
        this->on_watermark(wm, out);
    }

    void process_barrier(CheckpointBarrier b, Emitter<std::pair<Key, Agg>>& out) {
        this->on_barrier(b, out);
    }

private:
    // Per-record ingest: window assignment + late-data side-output routing +
    // dispatch to handle_record_. Shared verbatim by the row path (process())
    // and the columnar fast path (process_columnar()), so the two are exactly
    // equivalent - the columnar path only skips the Record<pair> decode.
    void ingest_one_(const Key& k,
                     const Value& v,
                     const std::optional<EventTime>& et,
                     Emitter<std::pair<Key, Agg>>& out) {
        const std::int64_t ts = et.value_or(EventTime{0}).millis();
        const std::int64_t window_start = (ts / window_size_.count()) * window_size_.count();
        const TimeWindow window{.start = window_start, .end = window_start + window_size_.count()};
        // Late-late check: if the user opted into a late-data side output AND
        // the current watermark has already crossed (window_end +
        // allowed_lateness), forward the record to the side output and skip the
        // bucketing path. Without a tag we preserve the historic "create a fresh
        // bucket for the late record" behavior - switching unconditionally to
        // drop-or-route would silently change the semantics of every existing
        // user that doesn't set a tag. Records still within the lateness band
        // fall through to handle_record_ where the existing re-fire path picks
        // them up.
        if (late_tag_.has_value()) {
            const std::int64_t purge_at = window.end + allowed_lateness_.count();
            if (ctx_.current_watermark() >= purge_at && this->runtime() != nullptr) {
                auto side = this->runtime()->template side_output<Value>(*late_tag_);
                Batch<Value> b;
                if (et.has_value()) {
                    b.emplace(v, *et);
                } else {
                    b.emplace(v);
                }
                side.emit_data(std::move(b));
                return;
            }
        }
        handle_record_(k, window, v, ts, out);
    }

    void handle_record_(const Key& k,
                        const TimeWindow& window,
                        const Value& v,
                        std::int64_t ts,
                        Emitter<std::pair<Key, Agg>>& out) {
        const StateKey sk{window.start, k};
        // Direct map-iterator access: load + combine + store fuse into
        // a single insertion-or-lookup. The old shape allocated a stack
        // Entry, copied from the map, modified, then copied back -
        // four Entry traversals + the shared_ptr refcount churn that
        // comes with them. try_emplace gives us a reference straight
        // into the map and we mutate in-place.
        auto [it, inserted] = mem_.try_emplace(sk);
        Entry& entry = it->second;
        if (inserted) {
            entry.agg = initial_();
            const std::int64_t end = window.start + window_size_.count();
            if (end < earliest_window_end_) {
                earliest_window_end_ = end;
            }
        }
        entry.agg = combiner_(entry.agg, v);
        // Mirror to the persistent store when configured, so the
        // recovery and persistent-path tests see the same state the
        // hot path is operating on. For the par=1 bench, set
        // CLINK_WB_STATE_CACHE=1 to skip this (write-back cache
        // pattern: in-mem state authoritative until a barrier).
        // The cache_skip lookup is a function-local static so the
        // env_var only gets parsed once per process; per-record
        // it's a single bool load.
        if (keyed_ && !wb_cache_skip_()) {
            keyed_->put(sk, entry);
        }

        const TriggerResult tr = trigger_->on_element(v, ts, window, ctx_);

        // Pane timing classification:
        //   * if entry.fired (already had on-time pane): Late.
        //   * else if wm < window.end: Early.
        //   * else: OnTime.
        const std::int64_t wm_ms = ctx_.current_watermark();
        const PaneInfo::Timing timing =
            entry.fired ? PaneInfo::Timing::Late
                        : (wm_ms < window.end ? PaneInfo::Timing::Early : PaneInfo::Timing::OnTime);

        if (entry.fired) {
            const bool is_last = should_purge(tr);
            emit_(k,
                  window,
                  entry.agg,
                  timing,
                  /*is_first*/ false,
                  is_last,
                  entry.next_pane_index,
                  out);
            ++entry.next_pane_index;
        } else if (should_fire(tr)) {
            const bool is_last = should_purge(tr);
            emit_(k,
                  window,
                  entry.agg,
                  timing,
                  /*is_first*/ true,
                  is_last,
                  entry.next_pane_index,
                  out);
            ++entry.next_pane_index;
            entry.fired = true;
        }

        if (should_purge(tr)) {
            purge_(sk, window);
        }
    }

    void on_watermark_advance_(Watermark wm, Emitter<std::pair<Key, Agg>>& out) {
        const std::int64_t wm_ms = wm.timestamp().millis();

        // Fast path: watermark hasn't crossed any window's end yet, so
        // no entry can fire and no entry's lateness deadline can have
        // arrived either (lateness_purge_at >= window.end). Avoid
        // walking the (potentially large) keyed state map for the
        // common per-batch tick.
        if (wm_ms < earliest_window_end_) {
            return;
        }

        // Iterate every (key, window_start) entry. For each, dispatch
        // the trigger's on_event_time at window.end if not yet fired,
        // and purge if past the lateness deadline.
        struct Action {
            StateKey sk;
            Entry entry;
            bool fire{false};
            bool purge{false};
        };
        std::vector<Action> actions;

        const auto consider = [&](const StateKey& sk, const Entry& entry) {
            const TimeWindow window{.start = sk.first, .end = sk.first + window_size_.count()};
            const std::int64_t lateness_purge_at = window.end + allowed_lateness_.count();

            Action action;
            action.sk = sk;
            action.entry = entry;

            if (!entry.fired && wm_ms >= window.end) {
                const TriggerResult tr = trigger_->on_event_time(window.end, window, ctx_);
                if (should_fire(tr)) {
                    action.fire = true;
                }
                if (should_purge(tr)) {
                    action.purge = true;
                }
            }
            // Lateness deadline overrides trigger: even if the trigger
            // didn't purge, the cleanup timer wins.
            if (wm_ms >= lateness_purge_at) {
                action.purge = true;
            }
            if (action.fire || action.purge) {
                actions.push_back(std::move(action));
            }
        };

        // mem_ is the hot-path source of truth for both in-memory and
        // persistent modes - handle_record_ writes only there to skip
        // the per-record codec round-trip. keyed_, when set, gets
        // updated in store_ and purge_ so recovery still has a
        // durable view of state.
        for (const auto& [sk, entry] : mem_) {
            consider(sk, entry);
        }

        // Accumulate all on-time emits into a single batch before
        // pushing downstream. The previous shape (one emit_data call
        // per (key, window) firing) cost N BoundedChannel locks; for
        // 100K-window watermark sweeps that dominated wall-time. One
        // big batch keeps the downstream Dag stage seeing one
        // contiguous record stream.
        Batch<std::pair<Key, Agg>> fired_batch;
        for (auto& a : actions) {
            const TimeWindow window{.start = a.sk.first, .end = a.sk.first + window_size_.count()};
            if (a.fire) {
                Record<std::pair<Key, Agg>> r{std::make_pair(a.sk.second, a.entry.agg),
                                              EventTime{window.max_timestamp()}};
                r.set_pane(PaneInfo{.timing = PaneInfo::Timing::OnTime,
                                    .pane_index = a.entry.next_pane_index,
                                    .is_first = a.entry.next_pane_index == 0,
                                    .is_last = a.purge});
                fired_batch.push(std::move(r));
                ++a.entry.next_pane_index;
                a.entry.fired = true;
                store_(a.sk, a.entry);
            }
            if (a.purge) {
                purge_(a.sk, window);
            }
        }
        if (!fired_batch.empty()) {
            out.emit_data(std::move(fired_batch));
        }

        // Recompute earliest_window_end_ from the surviving state.
        // After a batch of fires/purges the previous earliest entry
        // is likely gone; walking the state once now amortises the
        // cost across many subsequent fast-path skips.
        recompute_earliest_window_end_();
    }

    void recompute_earliest_window_end_() {
        std::int64_t earliest = std::numeric_limits<std::int64_t>::max();
        const auto window_ms = window_size_.count();
        if (keyed_) {
            keyed_->scan([&](const StateKey& sk, const Entry&) {
                const std::int64_t end = sk.first + window_ms;
                if (end < earliest) {
                    earliest = end;
                }
            });
        } else {
            for (const auto& [sk, _] : mem_) {
                const std::int64_t end = sk.first + window_ms;
                if (end < earliest) {
                    earliest = end;
                }
            }
        }
        earliest_window_end_ = earliest;
    }

    Entry load_or_init_(const StateKey& sk) {
        if (keyed_) {
            auto cur = keyed_->get(sk);
            if (cur.has_value()) {
                return std::move(*cur);
            }
        } else {
            auto it = mem_.find(sk);
            if (it != mem_.end()) {
                return it->second;
            }
        }
        Entry e;
        e.agg = initial_();
        return e;
    }

    // Trigger-state durability (no-op unless the trigger is stateful). Called
    // from open() inside the persistent branch so runtime()+backend exist; the
    // same shape is mirrored in the sliding + evicting operators.
    //
    // Trigger state is operator-level (not per-user-key): it persists as one
    // blob under a fixed key, so on a RESCALE it lands in a single key-group and
    // only one post-rescale subtask restores it; the others start with empty
    // trigger state. This mirrors the timer-service operator-state model (see
    // operator_base.hpp). A same-parallelism restart - the shipped, tested path
    // - restores it fully; rescale routing for operator-level trigger state is a
    // deferred follow-on, identical to the timer rescale follow-on.
    void restore_trigger_state_() {
        if (!trigger_->is_stateful()) {
            return;
        }
        trigger_state_ = std::make_unique<KeyedState<std::int64_t, std::string>>(
            this->runtime()->template keyed_state<std::int64_t, std::string>(
                "trigger_state", int64_codec(), string_codec()));
        if (auto blob = trigger_state_->get(0); blob.has_value()) {
            trigger_->restore_state(*blob);
        }
    }
    void persist_trigger_state_() {
        if (trigger_state_) {
            trigger_state_->put(0, trigger_->snapshot_state());
        }
    }

    void store_(const StateKey& sk, const Entry& entry) {
        // Always update mem_: it's the hot-path source of truth now,
        // even in the persistent (keyed_) mode. The previous shape
        // wrote to keyed_ XOR mem_, but after the iterator-based
        // handle_record_ landed mem_ became authoritative everywhere
        // and skipping it here let fired/pane state slip out of sync
        // (recovery test would see re-fires because mem_'s entry
        // kept fired=false while keyed_ flipped it true).
        mem_[sk] = entry;
        if (keyed_ && !wb_cache_skip_()) {
            keyed_->put(sk, entry);
        }
        // Keep the earliest-window-end tracker tight on every store.
        // sk.first is window.start; window.end = start + size.
        const std::int64_t end = sk.first + window_size_.count();
        if (end < earliest_window_end_) {
            earliest_window_end_ = end;
        }
    }

    void mark_fired_(const StateKey& sk, Entry entry) {
        entry.fired = true;
        store_(sk, entry);
    }

    void purge_(const StateKey& sk, const TimeWindow& window) {
        if (keyed_) {
            keyed_->erase(sk);
        } else {
            mem_.erase(sk);
        }
        trigger_->clear(window, ctx_);
    }

    void emit_(const Key& k,
               const TimeWindow& window,
               const Agg& agg,
               PaneInfo::Timing timing,
               bool is_first,
               bool is_last,
               std::int64_t pane_index,
               Emitter<std::pair<Key, Agg>>& out) {
        Batch<std::pair<Key, Agg>> b;
        Record<std::pair<Key, Agg>> r{std::make_pair(k, agg), EventTime{window.max_timestamp()}};
        r.set_pane(PaneInfo{
            .timing = timing, .pane_index = pane_index, .is_first = is_first, .is_last = is_last});
        b.push(std::move(r));
        out.emit_data(std::move(b));
        clink::metrics::op::window_panes_fired_inc(
            this->runtime() ? this->runtime()->metrics() : nullptr, this->id().value());
    }

    std::chrono::milliseconds window_size_;
    std::chrono::milliseconds allowed_lateness_{0};
    std::optional<OutputTag<Value>> late_tag_;
    Initial initial_;
    Combiner combiner_;

    // Read once at process startup; per-record this is just a bool
    // load. Avoids an env-var lookup on every record (which would be
    // a noticeable hot-path cost).
    static bool wb_cache_skip_() {
        static const bool v = [] {
            const char* s = std::getenv("CLINK_WB_STATE_CACHE");
            return s != nullptr && std::string_view{s} == "1";
        }();
        return v;
    }
    std::string name_;

    std::unique_ptr<Trigger<Value, TimeWindow>> trigger_;
    detail::OperatorTriggerContext ctx_;

    // Persistent path
    std::optional<Codec<Key>> key_codec_;
    std::optional<Codec<Agg>> agg_codec_;
    std::unique_ptr<KeyedState<StateKey, Entry>> keyed_;
    // Durable per-window state of a stateful trigger (e.g. CountTrigger), one
    // blob under a fixed key. Created only when windows are persisted AND the
    // trigger is stateful; stateless triggers leave this null (zero cost).
    std::unique_ptr<KeyedState<std::int64_t, std::string>> trigger_state_;

#ifdef CLINK_HAS_ARROW
    // Columnar-native ingest fast path toggle (see set_columnar_enabled). Only
    // consulted for int64/int64 instantiations; the bench flips it to force the
    // row path on the same operator. Guarded with the columnar methods that read
    // it so a no-Arrow build carries neither.
    bool columnar_enabled_{true};
#endif

    // In-memory path
    clink::FlatMap<StateKey, Entry, detail::PairKeyHash<Key>> mem_;

    // Earliest still-active window.end seen across all entries. When
    // current_watermark < earliest_window_end_, no entry can fire or
    // purge yet, so on_watermark_advance_ can skip the full state
    // scan. This converts the per-batch O(active_entries) cost into a
    // single compare for the common "watermark advanced but didn't
    // cross any window boundary" case - which is most of them on a
    // dense stream (e.g. 39k batches across 100 1-second windows
    // means 99.7% of watermark calls cross no boundary).
    //
    // Maintenance: handle_record_ updates this whenever it touches a
    // (key, window_start) bucket; on_watermark_advance_ recomputes
    // after a batch that fires/purges, walking the surviving state
    // once. INT64_MAX means "no active entries / unknown" - either
    // forces the next scan to run, which then re-establishes the
    // invariant by walking the state.
    std::int64_t earliest_window_end_{std::numeric_limits<std::int64_t>::max()};
};

}  // namespace clink
