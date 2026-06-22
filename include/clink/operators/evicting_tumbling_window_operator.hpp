#pragma once

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "clink/core/codec.hpp"
#include "clink/core/pane_info.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/operators/tumbling_window_operator.hpp"  // PairKeyHash, OperatorTriggerContext, now_processing_time_ms
#include "clink/operators/window_evictor.hpp"
#include "clink/operators/window_state.hpp"  // BufferEntry, buffer_entry_codec
#include "clink/operators/window_trigger.hpp"
#include "clink/runtime/output_tag.hpp"
#include "clink/state/keyed_state.hpp"

namespace clink {

// Evicting variant of TumblingEventTimeWindows. Buffers raw records
// per (key, window) so an Evictor can filter them before the user's
// ProcessWindowFunction runs.
//
// Trigger-Evictor interaction follows :
//   * On Trigger Fire / FireAndPurge: run evictor.evict_before, then
//     run the process function, then evictor.evict_after, then emit.
//   * On Trigger Purge: drop the buffer without running the process
//     function.
//
// Allowed lateness is also supported here: the buffer is retained
// past window_end if allowed_lateness > 0, and late records re-fire.
//
// Durable (FOUND-2): the persistent ctor (Codec<Key> + Codec<Value>) mirrors
// each (window_start, key) buffer to ONE KeyedState row holding the whole
// BufferEntry (slot "evicting_buffers"), so the raw records, fired flag and
// next_pane_index survive snapshot/restore; open() rehydrates buffers_ by
// scanning. The persisted fired flag + monotonic next_pane_index mean a buffer
// fired before a checkpoint does not re-emit its on-time pane after restore, and
// post-restore late re-fires continue the pane numbering. Default ctor =
// in-memory only (unchanged). This is a STATEFUL operator: pin a stable .uid().
// Caveat: the whole record vector is re-put per mutation in strict mode (write
// amplification); CLINK_WB_STATE_CACHE=1 amortises it to one put per barrier,
// and a true incremental ListState (FOUND-1) is the longer-term home.
template <typename Key, typename Value, typename Out>
class EvictingTumblingWindowOperator final
    : public Operator<std::pair<Key, Value>, std::pair<Key, Out>> {
public:
    using ProcessFn =
        std::function<Out(const std::vector<Record<Value>>& records, const TimeWindow& window)>;

    EvictingTumblingWindowOperator(std::chrono::milliseconds size,
                                   ProcessFn process_fn,
                                   std::unique_ptr<Evictor<Value, TimeWindow>> evictor,
                                   std::string name = "evicting_tumbling_window")
        : window_size_(size),
          process_fn_(std::move(process_fn)),
          evictor_(std::move(evictor)),
          name_(std::move(name)),
          trigger_(std::make_unique<EventTimeTrigger<Value>>()) {
        if (window_size_.count() <= 0) {
            throw std::invalid_argument("EvictingTumblingWindowOperator: size must be > 0");
        }
        if (evictor_ == nullptr) {
            throw std::invalid_argument("EvictingTumblingWindowOperator: evictor must be non-null");
        }
    }

    // Persistent ctor: buffers survive checkpoint/restore via a KeyedState row
    // per (window_start, key) (slot "evicting_buffers"). Needs a Value codec
    // because the buffer holds raw records, not an aggregate.
    EvictingTumblingWindowOperator(std::chrono::milliseconds size,
                                   ProcessFn process_fn,
                                   std::unique_ptr<Evictor<Value, TimeWindow>> evictor,
                                   Codec<Key> key_codec,
                                   Codec<Value> value_codec,
                                   std::string name = "evicting_tumbling_window")
        : window_size_(size),
          process_fn_(std::move(process_fn)),
          evictor_(std::move(evictor)),
          name_(std::move(name)),
          trigger_(std::make_unique<EventTimeTrigger<Value>>()),
          key_codec_(std::move(key_codec)),
          value_codec_(std::move(value_codec)) {
        if (window_size_.count() <= 0) {
            throw std::invalid_argument("EvictingTumblingWindowOperator: size must be > 0");
        }
        if (evictor_ == nullptr) {
            throw std::invalid_argument("EvictingTumblingWindowOperator: evictor must be non-null");
        }
    }

    void open() override {
        if (key_codec_.has_value() && value_codec_.has_value() && this->runtime() != nullptr &&
            this->runtime()->has_state_backend()) {
            keyed_ = std::make_unique<KeyedState<StateKey, BufferEntry<Value>>>(
                this->runtime()->template keyed_state<StateKey, BufferEntry<Value>>(
                    "evicting_buffers",
                    pair_codec<std::int64_t, Key>(int64_codec(), *key_codec_),
                    buffer_entry_codec<Value>(*value_codec_)));
            keyed_->scan(
                [this](const StateKey& sk, const BufferEntry<Value>& be) { buffers_[sk] = be; });
            restore_trigger_state_();
        }
    }

    void close() override {
        keyed_.reset();
        trigger_state_.reset();
    }

    EvictingTumblingWindowOperator& allowed_lateness(std::chrono::milliseconds v) {
        allowed_lateness_ = v;
        return *this;
    }

    // Register an OutputTag for records arriving past (window_end +
    // allowed_lateness). Same opt-in semantics as
    // TumblingWindowOperator::late_output_tag: without a tag, the
    // historic "create fresh bucket" behavior is preserved.
    EvictingTumblingWindowOperator& late_output_tag(OutputTag<Value> tag) {
        late_tag_ = std::move(tag);
        return *this;
    }

    EvictingTumblingWindowOperator& with_trigger(std::unique_ptr<Trigger<Value, TimeWindow>> t) {
        if (t == nullptr) {
            throw std::invalid_argument("EvictingTumblingWindowOperator: trigger must be non-null");
        }
        trigger_ = std::move(t);
        return *this;
    }

    void process(const StreamElement<std::pair<Key, Value>>& element,
                 Emitter<std::pair<Key, Out>>& out) override {
        ctx_.set_processing_time(detail::now_processing_time_ms());

        if (element.is_data()) {
            for (const auto& record : element.as_data()) {
                const std::int64_t ts = record.event_time().value_or(EventTime{0}).millis();
                const std::int64_t window_start =
                    (ts / window_size_.count()) * window_size_.count();
                const TimeWindow window{.start = window_start,
                                        .end = window_start + window_size_.count()};

                // Late-late check: same opt-in shape as
                // TumblingWindowOperator. Routes to side output when
                // a tag is set AND the watermark has crossed
                // (window_end + allowed_lateness); otherwise the
                // record falls through and creates a fresh bucket.
                if (late_tag_.has_value()) {
                    const std::int64_t purge_at = window.end + allowed_lateness_.count();
                    if (ctx_.current_watermark() >= purge_at && this->runtime() != nullptr) {
                        auto side = this->runtime()->template side_output<Value>(*late_tag_);
                        Batch<Value> b;
                        if (record.event_time().has_value()) {
                            b.emplace(record.value().second, *record.event_time());
                        } else {
                            b.emplace(record.value().second);
                        }
                        side.emit_data(std::move(b));
                        continue;
                    }
                }

                const StateKey sk{window_start, record.value().first};
                Buffer& buf = buffers_[sk];

                Record<Value> rv{record.value().second};
                if (record.event_time().has_value()) {
                    rv.set_event_time(*record.event_time());
                }
                buf.records.push_back(std::move(rv));

                const TriggerResult tr =
                    trigger_->on_element(record.value().second, ts, window, ctx_);

                if (buf.fired) {
                    fire_(record.value().first,
                          window,
                          buf, /*timing*/
                          PaneInfo::Timing::Late,
                          /*is_first*/ false,
                          /*is_last*/ should_purge(tr),
                          out);
                    if (should_purge(tr)) {
                        buffers_.erase(sk);
                        trigger_->clear(window, ctx_);
                    }
                } else if (should_fire(tr)) {
                    const std::int64_t wm_ms = ctx_.current_watermark();
                    const PaneInfo::Timing timing =
                        wm_ms < window.end ? PaneInfo::Timing::Early : PaneInfo::Timing::OnTime;
                    fire_(record.value().first,
                          window,
                          buf,
                          timing,
                          /*is_first*/ true,
                          /*is_last*/ should_purge(tr),
                          out);
                    buf.fired = true;
                    if (should_purge(tr)) {
                        buffers_.erase(sk);
                        trigger_->clear(window, ctx_);
                    }
                } else if (should_purge(tr)) {
                    buffers_.erase(sk);
                    trigger_->clear(window, ctx_);
                }

                // Mirror the post-trigger buffer to the durable store, or erase
                // it if the trigger purged it (erase is unconditional so a
                // purged buffer never resurrects on restore).
                if (auto it = buffers_.find(sk); it != buffers_.end()) {
                    persist_buf_(sk, it->second);
                } else {
                    erase_buf_(sk);
                }
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

    void flush(Emitter<std::pair<Key, Out>>& out) override {
        ctx_.set_watermark(Watermark::max().timestamp().millis());
        on_watermark_advance_(Watermark::max(), out);
    }

    // Write-back-cache flush: in CLINK_WB_STATE_CACHE mode per-mutation puts are
    // deferred; flush every buffer once per barrier. Erases happen
    // unconditionally at the purge sites, so this is put-only (no orphans).
    void on_barrier(CheckpointBarrier barrier, Emitter<std::pair<Key, Out>>& out) override {
        if (keyed_ && wb_cache_skip_()) {
            for (const auto& [sk, buf] : buffers_) {
                keyed_->put(sk, buf);
            }
        }
        persist_trigger_state_();
        out.emit_barrier(barrier);
    }

    std::string name() const override { return name_; }

private:
    using StateKey = std::pair<std::int64_t, Key>;

    // The durable BufferEntry IS the in-memory buffer (identical fields), so the
    // codec serialises it directly with no conversion copy.
    using Buffer = BufferEntry<Value>;

    void fire_(const Key& k,
               const TimeWindow& window,
               Buffer& buf,
               PaneInfo::Timing timing,
               bool is_first,
               bool is_last,
               Emitter<std::pair<Key, Out>>& out) {
        // Evictor and process_fn see a working copy so we can
        // re-fire under allowed_lateness without losing the original
        // record buffer.
        std::vector<Record<Value>> working = buf.records;
        evictor_->evict_before(working, static_cast<std::int64_t>(working.size()), window, ctx_);
        Out out_val = process_fn_(working, window);
        evictor_->evict_after(working, static_cast<std::int64_t>(working.size()), window, ctx_);
        // The post-fire eviction is the operator's persistent buffer:
        // if the evictor wants to keep the working set bounded between
        // fires, its evict_after applies to the actual buffer.
        buf.records = std::move(working);

        Batch<std::pair<Key, Out>> b;
        Record<std::pair<Key, Out>> rec{std::make_pair(k, std::move(out_val)),
                                        EventTime{window.max_timestamp()}};
        rec.set_pane(PaneInfo{.timing = timing,
                              .pane_index = buf.next_pane_index,
                              .is_first = is_first,
                              .is_last = is_last});
        ++buf.next_pane_index;
        b.push(std::move(rec));
        out.emit_data(std::move(b));
        clink::metrics::op::window_panes_fired_inc(
            this->runtime() ? this->runtime()->metrics() : nullptr, this->id().value());
    }

    void on_watermark_advance_(Watermark wm, Emitter<std::pair<Key, Out>>& out) {
        const std::int64_t wm_ms = wm.timestamp().millis();

        struct Action {
            StateKey sk;
            bool fire{false};
            bool purge{false};
            PaneInfo::Timing timing{PaneInfo::Timing::OnTime};
        };
        std::vector<Action> actions;

        for (const auto& [sk, buf] : buffers_) {
            const TimeWindow window{.start = sk.first, .end = sk.first + window_size_.count()};
            const std::int64_t lateness_purge_at = window.end + allowed_lateness_.count();

            Action action;
            action.sk = sk;

            if (!buf.fired && wm_ms >= window.end) {
                const TriggerResult tr = trigger_->on_event_time(window.end, window, ctx_);
                if (should_fire(tr)) {
                    action.fire = true;
                    action.timing = PaneInfo::Timing::OnTime;
                }
                if (should_purge(tr)) {
                    action.purge = true;
                }
            }
            if (wm_ms >= lateness_purge_at) {
                action.purge = true;
            }
            if (action.fire || action.purge) {
                actions.push_back(action);
            }
        }

        for (const auto& a : actions) {
            const TimeWindow window{.start = a.sk.first, .end = a.sk.first + window_size_.count()};
            auto it = buffers_.find(a.sk);
            if (it == buffers_.end()) {
                continue;
            }
            if (a.fire) {
                fire_(a.sk.second,
                      window,
                      it->second,
                      a.timing,
                      /*is_first*/ it->second.next_pane_index == 0,
                      /*is_last*/ a.purge,
                      out);
                it->second.fired = true;
            }
            if (a.purge) {
                buffers_.erase(it);
                trigger_->clear(window, ctx_);
                erase_buf_(a.sk);  // unconditional: no orphan row after purge
            } else if (a.fire) {
                persist_buf_(a.sk, it->second);  // survived the fire: mirror it
            }
        }
    }

    // Strict-mode write-through of one buffer (write-back mode defers to barrier).
    void persist_buf_(const StateKey& sk, const Buffer& buf) {
        if (keyed_ && !wb_cache_skip_()) {
            keyed_->put(sk, buf);
        }
    }

    // Unconditional erase (both modes) so a purged buffer never resurrects and
    // the barrier flush can stay put-only.
    void erase_buf_(const StateKey& sk) {
        if (keyed_) {
            keyed_->erase(sk);
        }
    }

    // Trigger-state durability (no-op unless the trigger is stateful). Mirrors
    // the tumbling + sliding operators. Called from open() inside the persistent
    // branch so runtime()+backend exist.
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

    static bool wb_cache_skip_() {
        static const bool skip = [] {
            const char* s = std::getenv("CLINK_WB_STATE_CACHE");
            return s != nullptr && std::string(s) == "1";
        }();
        return skip;
    }

    std::chrono::milliseconds window_size_;
    std::chrono::milliseconds allowed_lateness_{0};
    std::optional<OutputTag<Value>> late_tag_;
    ProcessFn process_fn_;
    std::unique_ptr<Evictor<Value, TimeWindow>> evictor_;
    std::unique_ptr<Trigger<Value, TimeWindow>> trigger_;
    std::string name_;
    detail::OperatorTriggerContext ctx_;

    std::unordered_map<StateKey, Buffer, detail::PairKeyHash<Key>> buffers_;

    std::optional<Codec<Key>> key_codec_;
    std::optional<Codec<Value>> value_codec_;
    std::unique_ptr<KeyedState<StateKey, BufferEntry<Value>>> keyed_;
    // Durable per-window state of a stateful trigger (e.g. CountTrigger), one
    // blob under a fixed key. Created only when buffers are persisted AND the
    // trigger is stateful; stateless triggers leave this null (zero cost).
    std::unique_ptr<KeyedState<std::int64_t, std::string>> trigger_state_;
};

}  // namespace clink
