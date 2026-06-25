#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
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
#include "clink/operators/tumbling_window_operator.hpp"  // PairKeyHash, OperatorTriggerContext, now_processing_time_ms
#include "clink/operators/window_state.hpp"
#include "clink/operators/window_trigger.hpp"
#include "clink/runtime/output_tag.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/keyed_state.hpp"

namespace clink {

// Keyed event-time sliding window. Models // SlidingEventTimeWindows.of(size, slide).
//
// Trigger model: same as TumblingWindowOperator. Default is
// EventTimeTrigger. Override via with_trigger() to plug in
// ProcessingTimeTrigger, CountTrigger, or any user trigger.
//
// allowed_lateness applies per-window: each overlapping window has its
// own cleanup deadline at window_end + allowed_lateness.
template <typename Key, typename Value, typename Agg>
class SlidingWindowOperator final : public Operator<std::pair<Key, Value>, std::pair<Key, Agg>> {
public:
    using Combiner = std::function<Agg(const Agg&, const Value&)>;
    using Initial = std::function<Agg()>;

    SlidingWindowOperator(std::chrono::milliseconds size,
                          std::chrono::milliseconds slide,
                          Initial initial,
                          Combiner combiner,
                          std::string name = "sliding_window")
        : window_size_(size),
          slide_size_(slide),
          initial_(std::move(initial)),
          combiner_(std::move(combiner)),
          name_(std::move(name)),
          trigger_(std::make_unique<EventTimeTrigger<Value>>()) {
        validate_();
    }

    SlidingWindowOperator(std::chrono::milliseconds size,
                          std::chrono::milliseconds slide,
                          Initial initial,
                          Combiner combiner,
                          Codec<Key> key_codec,
                          Codec<Agg> agg_codec,
                          std::string name = "sliding_window")
        : window_size_(size),
          slide_size_(slide),
          initial_(std::move(initial)),
          combiner_(std::move(combiner)),
          name_(std::move(name)),
          key_codec_(std::move(key_codec)),
          agg_codec_(std::move(agg_codec)),
          trigger_(std::make_unique<EventTimeTrigger<Value>>()) {
        validate_();
    }

    SlidingWindowOperator& allowed_lateness(std::chrono::milliseconds v) {
        allowed_lateness_ = v;
        return *this;
    }

    // Register an OutputTag to receive records that arrive past every
    // covering window's (end + allowed_lateness) deadline - i.e., the
    // record has no still-live window. Forwarded as-is, preserving
    // event_time. Without a tag, the historic "create fresh bucket"
    // behavior holds. See TumblingWindowOperator::late_output_tag for
    // the full rationale.
    SlidingWindowOperator& late_output_tag(OutputTag<Value> tag) {
        late_tag_ = std::move(tag);
        return *this;
    }

    SlidingWindowOperator& with_trigger(std::unique_ptr<Trigger<Value, TimeWindow>> t) {
        if (t == nullptr) {
            throw std::invalid_argument("SlidingWindowOperator: trigger must be non-null");
        }
        trigger_ = std::move(t);
        return *this;
    }

    void open() override {
        if (key_codec_.has_value() && agg_codec_.has_value() && this->runtime() != nullptr &&
            this->runtime()->has_state_backend()) {
            keyed_ = std::make_unique<KeyedState<StateKey, Entry>>(
                this->runtime()->template keyed_state<StateKey, Entry>(
                    "sliding_windows",
                    pair_codec<std::int64_t, Key>(int64_codec(), *key_codec_),
                    window_entry_codec<Agg>(*agg_codec_)));
            // Intentional write-through model: unlike the tumbling/session/
            // evicting operators (which rehydrate an in-memory map from the
            // backend here via scan()), the sliding hot path reads and writes
            // keyed_ directly (load_or_init_/store_/on_watermark_advance_), so
            // restored state is read live from the backend and there is nothing
            // to rehydrate. mem_ is used ONLY on the no-backend path. Do NOT add
            // a scan()-into-mem_ here unless the hot path is also switched to
            // read mem_ first - a half-applied change would silently strand the
            // restored rows. Durability is pinned by test_sliding_window_persistence.cpp.
            restore_trigger_state_();
        }
    }

    void close() override {
        keyed_.reset();
        trigger_state_.reset();
    }

    // Sliding has no other on_barrier work (write-through), but a stateful
    // trigger's per-window state still has to be captured at the checkpoint.
    void on_barrier(CheckpointBarrier barrier, Emitter<std::pair<Key, Agg>>& out) override {
        persist_trigger_state_();
        out.emit_barrier(barrier);
    }

    void process(const StreamElement<std::pair<Key, Value>>& element,
                 Emitter<std::pair<Key, Agg>>& out) override {
        ctx_.set_processing_time(detail::now_processing_time_ms());

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
        ctx_.set_watermark(Watermark::max().timestamp().millis());
        on_watermark_advance_(Watermark::max(), out);
    }

#ifdef CLINK_HAS_ARROW
    // Toggle the columnar-native ingest fast path (default on). Disabling it
    // forces the row path even for a columnar upstream - used by the bench to
    // compare the two ingest paths on the SAME operator + SAME source.
    SlidingWindowOperator& set_columnar_enabled(bool enabled) {
        columnar_enabled_ = enabled;
        return *this;
    }

    // Columnar-native ingest (increment 4). Mirrors TumblingWindowOperator:
    // only int64 key + int64 value ride the 3-column {event_time, key, value}
    // Arrow sidecar; other key/value types stay on the row path. The fast path
    // folds the key+value+ts buffers into the per-(window,key) accumulators with
    // ZERO Record<pair> materialization, by calling the SAME ingest_one_ the row
    // path uses - so each record's fan-out into its overlapping windows,
    // triggers, late panes and watermark firing are byte-identical to the row
    // path; only the row decode is skipped.
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
            // Guards all passed BEFORE any ingest_one_ (which may emit); a false
            // return never half-processed the batch.
            ctx_.set_processing_time(detail::now_processing_time_ms());
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

    std::string name() const override { return name_; }

private:
    using StateKey = std::pair<std::int64_t, Key>;
    using Entry = WindowEntry<Agg>;

    // Per-record ingest: window fan-out + late-data side-output routing +
    // dispatch to handle_record_ per covering window. Shared verbatim by the
    // row path (process()) and the columnar fast path (process_columnar()), so
    // the two are exactly equivalent - the columnar path only skips the
    // Record<pair> decode.
    void ingest_one_(const Key& k,
                     const Value& v,
                     const std::optional<EventTime>& et,
                     Emitter<std::pair<Key, Agg>>& out) {
        const std::int64_t ts = et.value_or(EventTime{0}).millis();
        const auto starts = window_starts_for_(ts);
        // Late-late check: a sliding record belongs to up to (size/slide)
        // overlapping windows. The latest start has the latest end; if wm has
        // crossed (latest_end + allowed_lateness), every covering window is past
        // its purge deadline. Route to the side output when a tag is set;
        // otherwise fall through to the historic per-window bucketing.
        if (late_tag_.has_value() && !starts.empty()) {
            std::int64_t latest_end = 0;
            for (const auto s : starts) {
                const std::int64_t e = s + window_size_.count();
                if (e > latest_end) {
                    latest_end = e;
                }
            }
            const std::int64_t purge_at = latest_end + allowed_lateness_.count();
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
        for (const auto start : starts) {
            const TimeWindow window{.start = start, .end = start + window_size_.count()};
            handle_record_(k, window, v, ts, out);
        }
    }

    void validate_() const {
        if (slide_size_.count() <= 0) {
            throw std::invalid_argument("SlidingWindowOperator: slide must be > 0");
        }
        if (window_size_.count() <= 0) {
            throw std::invalid_argument("SlidingWindowOperator: size must be > 0");
        }
        if (window_size_.count() % slide_size_.count() != 0) {
            throw std::invalid_argument(
                "SlidingWindowOperator: size must be a multiple of slide "
                "(matches SlidingEventTimeWindows constraint)");
        }
    }

    static std::int64_t floor_div_(std::int64_t a, std::int64_t b) noexcept {
        auto q = a / b;
        auto r = a % b;
        if (r != 0 && ((r < 0) != (b < 0))) {
            --q;
        }
        return q;
    }

    std::vector<std::int64_t> window_starts_for_(std::int64_t ts) const {
        const std::int64_t slide = slide_size_.count();
        const std::int64_t size = window_size_.count();

        const std::int64_t latest_start = floor_div_(ts, slide) * slide;
        const std::int64_t count = size / slide;
        std::vector<std::int64_t> out;
        out.reserve(static_cast<std::size_t>(count));
        for (std::int64_t i = 0; i < count; ++i) {
            const std::int64_t s = latest_start - (i * slide);
            if (s + size > ts && s <= ts) {
                out.push_back(s);
            }
        }
        return out;
    }

    void handle_record_(const Key& k,
                        const TimeWindow& window,
                        const Value& v,
                        std::int64_t ts,
                        Emitter<std::pair<Key, Agg>>& out) {
        const StateKey sk{window.start, k};
        Entry entry = load_or_init_(sk);
        entry.agg = combiner_(entry.agg, v);

        const TriggerResult tr = trigger_->on_element(v, ts, window, ctx_);

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
        store_(sk, entry);

        if (should_purge(tr)) {
            purge_(sk, window);
        }
    }

    void on_watermark_advance_(Watermark wm, Emitter<std::pair<Key, Agg>>& out) {
        const std::int64_t wm_ms = wm.timestamp().millis();

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
            if (wm_ms >= lateness_purge_at) {
                action.purge = true;
            }
            if (action.fire || action.purge) {
                actions.push_back(std::move(action));
            }
        };

        if (keyed_) {
            keyed_->scan([&](const StateKey& sk, const Entry& entry) { consider(sk, entry); });
        } else {
            for (const auto& [sk, entry] : mem_) {
                consider(sk, entry);
            }
        }

        for (auto& a : actions) {
            const TimeWindow window{.start = a.sk.first, .end = a.sk.first + window_size_.count()};
            if (a.fire) {
                emit_(a.sk.second,
                      window,
                      a.entry.agg,
                      PaneInfo::Timing::OnTime,
                      /*is_first*/ a.entry.next_pane_index == 0,
                      /*is_last*/ a.purge,
                      a.entry.next_pane_index,
                      out);
                ++a.entry.next_pane_index;
                a.entry.fired = true;
                store_(a.sk, a.entry);
            }
            if (a.purge) {
                purge_(a.sk, window);
            }
        }
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

    void store_(const StateKey& sk, const Entry& entry) {
        if (keyed_) {
            keyed_->put(sk, entry);
        } else {
            mem_[sk] = entry;
        }
    }

    // Trigger-state durability (no-op unless the trigger is stateful). Mirrors
    // the tumbling + evicting operators. Called from open() inside the
    // persistent branch so runtime()+backend exist.
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
    std::chrono::milliseconds slide_size_;
    std::chrono::milliseconds allowed_lateness_{0};
    std::optional<OutputTag<Value>> late_tag_;
    Initial initial_;
    Combiner combiner_;
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
    // consulted for int64/int64 instantiations. Guarded with the columnar
    // methods that read it so a no-Arrow build carries neither.
    bool columnar_enabled_{true};
#endif

    // In-memory path
    clink::FlatMap<StateKey, Entry, detail::PairKeyHash<Key>> mem_;
};

}  // namespace clink
