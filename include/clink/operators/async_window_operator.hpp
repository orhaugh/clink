#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "clink/async/task.hpp"
#include "clink/core/codec.hpp"
#include "clink/core/pane_info.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/operators/window_state.hpp"
#include "clink/runtime/async_execution_controller.hpp"
#include "clink/runtime/output_tag.hpp"
#include "clink/state/keyed_state.hpp"

namespace clink {

// AsyncWindowOperator<Key, Value, Agg> - the shared machinery for event-time
// windowed aggregates on the async-state execution path. Concrete operators
// (tumbling, sliding) differ ONLY in how a record maps to window starts; they
// override assign_windows(). Everything else - the disaggregated accumulators,
// the per-key-gated async fold, the epoch-gated watermark firing, the durable
// timer-driven enumeration, allowed-lateness + late panes + a late-output-tag,
// and checkpoint/restore - lives here.
//
//   * Per record: for each window the record belongs to, read-combine-write
//     that window's accumulator in KeyedState (heavy aggregate values live in
//     the backend, not all in RAM) and register a fire timer at the window end.
//     process_async() reads via co_await get_async(), so a slow or remote read
//     suspends that record instead of blocking the runner; the
//     AsyncExecutionController serialises same-(window,key) records (per-key
//     gate) and gates the watermark (epoch).
//   * On watermark: the runner runs on_watermark() INSIDE the controller's
//     release closure - only after every record that arrived before the
//     watermark has drained - so firing observes all of each window's records.
//     The base on_watermark fires every due event-time timer.
//
// Allowed-lateness (mirrors the synchronous TumblingWindowOperator):
//   * The window state is WindowEntry<Agg> (agg + a `fired` flag + a monotonic
//     pane_index), so a late record can tell an already-fired window from one
//     still accumulating.
//   * On-time fire: when the watermark crosses window_end the fire timer emits
//     the aggregate as an OnTime pane and marks the window fired, but does NOT
//     purge it (unless allowed_lateness == 0, where window_end is also the
//     purge deadline so it fires + erases in one tick - byte-identical to the
//     no-lateness default path).
//   * Late record within the band (window_end <= watermark < window_end +
//     allowed_lateness): the fold sees fired == true, updates the aggregate AND
//     immediately re-emits a Late pane (from the resumed coroutine on the async
//     path, inline on the sync path - the same per-record emission
//     KeyedAggregateOperator uses).
//   * Purge: a second timer at window_end + allowed_lateness erases the window
//     (registered only when allowed_lateness > 0).
//   * Late-late record (its window is already purged): routed to the
//     late_output_tag side output if one is set, else dropped. A sliding record
//     is routed once, and only when EVERY window it would land in is past purge
//     (an in-band window still aggregates it).
//
// Which windows are due rides the framework TimerService's event-time queue,
// which the runner CHECKPOINTS (snapshot_timers) and RESTORES (restore_timers)
// automatically; accumulators ride KeyedState. Firing touches state only via
// the event-time timer (gated), so fires_state_touching_processing_time_timers()
// is false and the async runner admits it. The sync process() path is
// byte-identical and used when the backend cannot defer reads.
//
// PaneInfo parity: under the default (and only) event-time trigger the async
// family models, the sync operator sets OnTime.is_last when the window is purged
// in the same sweep as its on-time fire (allowed_lateness == 0, or a watermark
// that jumped past the purge deadline) and never sets is_last on a late pane;
// this reproduces that exactly. The one genuine async limitation is the absence
// of pluggable triggers (no Early panes) - pre-existing and documented. Rescale
// routing of the timers + the "wm" slot is a deferred follow-on (same as the
// no-lateness version).
template <typename Key, typename Value, typename Agg>
class AsyncWindowOperator : public Operator<std::pair<Key, Value>, std::pair<Key, Agg>> {
public:
    using Initial = std::function<Agg()>;
    using Combiner = std::function<Agg(const Agg&, const Value&)>;
    using StateKey = std::pair<std::int64_t, Key>;  // (window_start, key)
    using Entry = WindowEntry<Agg>;

    // Retain window state for this long after window_end to accept late records
    // (default 0 = drop late records at window_end, identical to before).
    AsyncWindowOperator& allowed_lateness(std::chrono::milliseconds v) {
        allowed_lateness_ms_ = v.count();
        return *this;
    }

    // Route records that arrive after their window has been purged
    // (current_watermark >= window_end + allowed_lateness) to this side output,
    // typed on Value and preserving event_time. Within-band records never go to
    // the side output; without a tag, late-late records are dropped.
    AsyncWindowOperator& late_output_tag(OutputTag<Value> tag) {
        late_tag_ = std::move(tag);
        return *this;
    }

    // --- synchronous path (non-deferring backend) ---
    void process(const StreamElement<std::pair<Key, Value>>& element,
                 Emitter<std::pair<Key, Agg>>& out) override {
        if (element.is_data()) {
            auto kv = state_();
            for (const auto& rec : element.as_data()) {
                const auto& [k, v] = rec.value();
                const std::int64_t ts = event_ms_(rec);
                bool any_window = false;
                bool any_in_band = false;
                for (const std::int64_t ws : assign_windows(ts)) {
                    any_window = true;
                    if (is_purged_(ws)) {
                        continue;  // window already purged - candidate for late-late
                    }
                    any_in_band = true;
                    const StateKey sk{ws, k};
                    const Entry e = fold_into_(kv.get(sk), v, sk, out);
                    kv.put(sk, e);
                    register_timers_(ws, encode_state_key_(sk));
                }
                if (any_window && !any_in_band) {
                    route_late_(v, rec.event_time(), out);
                }
            }
        } else if (element.is_watermark()) {
            this->on_watermark(element.as_watermark(), out);
        } else {
            this->on_barrier(element.as_barrier(), out);
        }
    }

    // --- async path (deferring backend) ---
    [[nodiscard]] bool supports_async() const noexcept override { return true; }

    void process_async(const StreamElement<std::pair<Key, Value>>& element,
                       Emitter<std::pair<Key, Agg>>& out,
                       AsyncExecutionController& aec) override {
        // Windows emit on fire (on_watermark) and, for within-band late records,
        // from the fold coroutine - both via `out`, which the runner keeps alive
        // across poll/drain (see KeyedAggregateOperator).
        if (!element.is_data()) {
            return;
        }
        for (const auto& rec : element.as_data()) {
            const auto& [k, v] = rec.value();
            const std::int64_t ts = event_ms_(rec);
            // Partition the record's windows into in-band vs already-purged. The
            // late-late side-output decision is synchronous (mirrors the sync op)
            // and made before any coroutine submit.
            std::vector<std::int64_t> in_band;
            bool any_window = false;
            for (const std::int64_t ws : assign_windows(ts)) {
                any_window = true;
                if (!is_purged_(ws)) {
                    in_band.push_back(ws);
                }
            }
            if (any_window && in_band.empty()) {
                route_late_(v, rec.event_time(), out);
                continue;
            }
            for (const std::int64_t ws : in_band) {
                const StateKey sk{ws, k};
                const std::string gate = encode_state_key_(sk);  // per-key gate + timer key
                auto kv = state_();
                // Capture by value: kv owns only a backend ptr + codecs; sk/ws/v
                // and gate are copied into the coroutine frame; `out` and `this`
                // are captured by reference (both outlive poll/drain). The fold +
                // any late-pane emit + the timer registration run on the runner
                // thread on resume, under the per-key gate.
                auto factory = [this, kv, sk, ws, v, gate, &out]() mutable -> async::Task<void> {
                    auto cur = co_await kv.get_async(sk);
                    const Entry e = fold_into_(std::move(cur), v, sk, out);
                    kv.put(sk, e);
                    register_timers_(ws, gate);
                    co_return;
                };
                while (!aec.submit(gate, factory)) {
                    aec.poll_or_flush();  // flush parked coalesced reads so the cap can free
                }
            }
        }
    }

    // Restore the late-drop boundary on (re)start so a record that is late
    // relative to a PRE-checkpoint watermark stays dropped after a restore.
    void open() override { current_watermark_ = wm_state_().get(0).value_or(0); }

    // Runs inside the AEC's epoch-gated release closure under async (and inline
    // under sync). Advances the late-drop watermark (persisted so it survives a
    // restore), then the base fires every due event-time timer.
    void on_watermark(Watermark wm, Emitter<std::pair<Key, Agg>>& out) override {
        current_watermark_ = std::max(current_watermark_, wm.timestamp().millis());
        wm_state_().put(0, current_watermark_);
        Operator<std::pair<Key, Value>, std::pair<Key, Agg>>::on_watermark(wm, out);
    }

    // A due timer. The same encoded state key carries both the fire timer (at
    // window_end) and, when allowed_lateness > 0, the purge timer (at window_end
    // + allowed_lateness). They are classified by the timer's OWN timestamp: the
    // event-time queue is ordered by (timestamp, key), so the fire tick is always
    // dispatched before the purge tick even when one watermark jumps past both.
    void on_event_time_timer(std::int64_t timer_ts,
                             const std::string& key,
                             Emitter<std::pair<Key, Agg>>& out) override {
        const StateKey sk = decode_state_key_(key);
        const std::int64_t window_end = sk.first + window_size_ms_;
        const std::int64_t purge_at = window_end + allowed_lateness_ms_;
        auto kv = state_();
        auto cur = kv.get(sk);
        if (!cur.has_value()) {
            return;  // already purged / never created - idempotent no-op
        }
        // Purge tick (only exists when allowed_lateness > 0; with lateness == 0
        // window_end == purge_at and we must fire + erase in the fire branch
        // below, never purge-only).
        if (allowed_lateness_ms_ > 0 && timer_ts >= purge_at) {
            kv.erase(sk);
            return;
        }
        // Fire tick (timer_ts == window_end; also the lateness == 0 combined
        // tick). A window already fired here is a stale/duplicate fire timer.
        Entry e = std::move(*cur);
        if (!e.fired) {
            // The window is purged in this same sweep when its lateness deadline
            // has already passed (lateness == 0, or a watermark that jumped past
            // both window_end and purge_at). is_last marks that final pane,
            // matching the sync TumblingWindowOperator's is_last = (fire && purge).
            const bool purge_now = allowed_lateness_ms_ == 0 || current_watermark_ >= purge_at;
            emit_pane_(out,
                       sk,
                       e.agg,
                       PaneInfo::Timing::OnTime,
                       e.next_pane_index,
                       /*is_first=*/e.next_pane_index == 0,
                       /*is_last=*/purge_now);
            ++e.next_pane_index;
            e.fired = true;
            if (purge_now) {
                kv.erase(sk);  // fire + purge in one tick (idempotent vs the purge timer)
            } else {
                kv.put(sk, e);  // keep it alive for the lateness band
            }
        }
    }

    [[nodiscard]] bool fires_state_touching_timers() const noexcept override { return true; }
    [[nodiscard]] bool fires_state_touching_processing_time_timers() const noexcept override {
        return false;  // event-time timers only - they fire in the gated closure
    }

    std::string name() const override { return name_; }

protected:
    AsyncWindowOperator(std::int64_t window_size_ms,
                        Initial initial,
                        Combiner combiner,
                        Codec<Key> key_codec,
                        Codec<Agg> agg_codec,
                        std::string name)
        : window_size_ms_(window_size_ms),
          initial_(std::move(initial)),
          combiner_(std::move(combiner)),
          key_codec_(std::move(key_codec)),
          agg_codec_(std::move(agg_codec)),
          name_(std::move(name)) {
        if (window_size_ms_ <= 0) {
            throw std::invalid_argument("AsyncWindowOperator: window_size_ms must be > 0");
        }
    }

    // The window starts a record at `ts` belongs to. Tumbling returns one;
    // sliding returns the overlapping set. A window spans [start, start +
    // window_size_ms_).
    virtual std::vector<std::int64_t> assign_windows(std::int64_t ts) const = 0;

    std::int64_t window_size_ms_;

private:
    static std::int64_t event_ms_(const Record<std::pair<Key, Value>>& rec) {
        return rec.event_time().value_or(EventTime{0}).millis();
    }
    // A window is purged-or-will-be once the watermark reaches its lateness
    // deadline; a record landing in it is late-late. A record in [window_end,
    // window_end + allowed_lateness) is NOT purged - it re-fires the window.
    bool is_purged_(std::int64_t window_start) const noexcept {
        return window_start + window_size_ms_ + allowed_lateness_ms_ <= current_watermark_;
    }
    // Fold v into the (possibly absent) entry; if the window already fired, emit
    // a Late pane immediately (the within-band re-fire). Shared verbatim by the
    // sync and async paths so they are exactly equivalent.
    Entry fold_into_(std::optional<Entry> cur,
                     const Value& v,
                     const StateKey& sk,
                     Emitter<std::pair<Key, Agg>>& out) {
        Entry e = cur.value_or(Entry{.agg = initial_(), .fired = false, .next_pane_index = 0});
        e.agg = combiner_(e.agg, v);
        if (e.fired) {
            emit_pane_(out,
                       sk,
                       e.agg,
                       PaneInfo::Timing::Late,
                       e.next_pane_index,
                       /*is_first=*/false,
                       /*is_last=*/false);
            ++e.next_pane_index;
        }
        return e;
    }
    void emit_pane_(Emitter<std::pair<Key, Agg>>& out,
                    const StateKey& sk,
                    const Agg& agg,
                    PaneInfo::Timing timing,
                    std::int64_t pane_index,
                    bool is_first,
                    bool is_last) {
        Batch<std::pair<Key, Agg>> batch;
        Record<std::pair<Key, Agg>> r{std::make_pair(sk.second, agg),
                                      EventTime{sk.first + window_size_ms_ - 1}};
        r.set_pane(PaneInfo{
            .timing = timing, .pane_index = pane_index, .is_first = is_first, .is_last = is_last});
        batch.push(std::move(r));
        out.emit_data(std::move(batch));
    }
    void route_late_(const Value& v,
                     const std::optional<EventTime>& et,
                     Emitter<std::pair<Key, Agg>>& /*out*/) {
        if (!late_tag_.has_value() || this->runtime() == nullptr) {
            return;  // no tag - drop
        }
        auto side = this->runtime()->template side_output<Value>(*late_tag_);
        Batch<Value> b;
        if (et.has_value()) {
            b.emplace(v, *et);
        } else {
            b.emplace(v);
        }
        side.emit_data(std::move(b));
    }
    void register_timers_(std::int64_t window_start, const std::string& state_key) {
        // Idempotent (the TimerService dedupes by (timestamp, key)).
        auto* ts = this->runtime()->timer_service();
        ts->register_event_time_timer(window_start + window_size_ms_, state_key);
        if (allowed_lateness_ms_ > 0) {
            ts->register_event_time_timer(window_start + window_size_ms_ + allowed_lateness_ms_,
                                          state_key);
        }
    }
    std::string encode_state_key_(const StateKey& sk) const {
        const auto b = state_key_codec_().encode(sk);
        return std::string(reinterpret_cast<const char*>(b.data()), b.size());
    }
    StateKey decode_state_key_(const std::string& s) const {
        auto v = state_key_codec_().decode(
            std::span<const std::byte>{reinterpret_cast<const std::byte*>(s.data()), s.size()});
        if (!v.has_value()) {
            // A symmetric codec round-trips, so this only fires on genuine
            // corruption. Fail loudly rather than silently read+erase the
            // default StateKey{} (which would strand the real accumulator).
            throw std::runtime_error(name_ +
                                     ": corrupt event-time timer key (state-key decode failed)");
        }
        return *v;
    }
    KeyedState<StateKey, Entry> state_() {
        return this->runtime()->template keyed_state<StateKey, Entry>(
            "win", state_key_codec_(), window_entry_codec<Agg>(agg_codec_));
    }
    // Operator-scoped late-drop boundary, persisted under a fixed key so it
    // survives checkpoint/restore (v1: same-parallelism; like the timer service,
    // rescale routing of this slot is a follow-on).
    KeyedState<std::int64_t, std::int64_t> wm_state_() {
        return this->runtime()->template keyed_state<std::int64_t, std::int64_t>(
            "wm", int64_codec(), int64_codec());
    }
    Codec<StateKey> state_key_codec_() const {
        return pair_codec<std::int64_t, Key>(int64_codec(), key_codec_);
    }

    std::int64_t allowed_lateness_ms_{0};
    std::optional<OutputTag<Value>> late_tag_;
    Initial initial_;
    Combiner combiner_;
    Codec<Key> key_codec_;
    Codec<Agg> agg_codec_;
    std::string name_;
    // Latest watermark seen; persisted in the "wm" slot (see open()).
    std::int64_t current_watermark_{0};
};

}  // namespace clink
