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
#include "clink/operators/operator_base.hpp"
#include "clink/operators/window_state.hpp"
#include "clink/runtime/async_execution_controller.hpp"
#include "clink/runtime/output_tag.hpp"
#include "clink/state/keyed_state.hpp"

namespace clink {

// AsyncSessionWindowOperator<Key, Value, Agg> - event-time SESSION window
// aggregate on the async-state execution path, with fully disaggregated state.
//
// Sessions are different in kind from tumbling/sliding windows: a record does
// not map to a fixed set of window starts, it MERGES sessions. A record at `ts`
// establishes a provisional window [ts, ts + gap]; every existing session for
// the same key that the provisional window overlaps merges into one - bounds
// become the min start and max end across the merged set, the aggregates reduce
// via the user's `merger`, then the record's own value folds in via `combiner`.
// Because a record can bridge two sessions into one, firing cannot be enumerated
// from `ts` alone, and the merge must read ALL of a key's sessions.
//
// The disaggregated shape that supports merge-enumeration WITHOUT a backend scan
// is the one the durable SessionWindowOperator already uses: ONE KeyedState row
// per key holding the whole vector<SessionRow<Agg>> (slot "sessions"). A merge is
// then a single atomic read-modify-write of that row. On the async path the read
// is co_await get_async(); the AsyncExecutionController's per-key gate serialises
// same-key records, so the RMW never races, and the epoch gate holds a watermark
// until every record before it has drained, so firing observes all in-flight
// merges.
//
// Firing rides the framework TimerService: each write registers an event-time
// timer at the (possibly extended) session end, keyed by the encoded user key.
// On a watermark the base on_watermark fires every due timer; on_event_time_timer
// reads the key's session row and sweeps it: every session whose end is at or
// below the watermark fires (and, with allowed_lateness, is kept until its
// lateness deadline), every session past its lateness deadline is purged. A
// session that merged and moved its end forward leaves a now-stale earlier timer
// behind; that timer fires harmlessly (the watermark-driven sweep finds nothing
// newly due). The timer queue, the per-key rows, and the late-drop boundary are
// all checkpointed/restored by the runner, so restore is automatic.
//
// Allowed-lateness (mirrors the synchronous SessionWindowOperator): after a
// session fires on time it is retained until current_watermark >= session_end +
// allowed_lateness. A late record that overlaps an already-fired session merges
// into it (extending bounds and folding its value) and RE-EMITS the merged
// aggregate immediately (from the resumed coroutine on the async path, inline on
// the sync path). A record arriving after its provisional window's lateness
// deadline (current_watermark >= ts + gap + allowed_lateness) is routed to the
// late_output_tag side output if one is set; otherwise it falls through to the
// merge path (overlapping sessions still merge; a fresh non-overlapping one is
// dropped - the at-most-once late-drop the async window family uses). Sessions
// carry NO PaneInfo on any emission, faithfully matching the sync reference.
//
// Firing touches state only via the event-time timer (which fires in the gated
// release closure), so fires_state_touching_processing_time_timers() is false and
// the async runner admits the operator. The sync process() path is semantically
// identical and used when the backend cannot defer reads. v1: no custom triggers
// and no rescale-routed timers/wm slot (deferred follow-ons).
template <typename Key, typename Value, typename Agg>
class AsyncSessionWindowOperator : public Operator<std::pair<Key, Value>, std::pair<Key, Agg>> {
public:
    using Initial = std::function<Agg()>;
    using Combiner = std::function<Agg(const Agg&, const Value&)>;
    using AggMerger = std::function<Agg(const Agg&, const Agg&)>;
    using Sessions = std::vector<SessionRow<Agg>>;

    AsyncSessionWindowOperator(std::int64_t gap_ms,
                               Initial initial,
                               Combiner combiner,
                               AggMerger merger,
                               Codec<Key> key_codec,
                               Codec<Agg> agg_codec,
                               std::string name = "async_session_window")
        : gap_ms_(gap_ms),
          initial_(std::move(initial)),
          combiner_(std::move(combiner)),
          merger_(std::move(merger)),
          key_codec_(std::move(key_codec)),
          agg_codec_(std::move(agg_codec)),
          name_(std::move(name)) {
        if (gap_ms_ <= 0) {
            throw std::invalid_argument("AsyncSessionWindowOperator: gap_ms must be > 0");
        }
    }

    // Retain a session for this long after session_end to accept late records
    // (default 0 = fire and purge at session_end, identical to before).
    AsyncSessionWindowOperator& allowed_lateness(std::chrono::milliseconds v) {
        allowed_lateness_ms_ = v.count();
        return *this;
    }

    // Route records arriving after their provisional window's lateness deadline
    // (current_watermark >= ts + gap + allowed_lateness) to this side output,
    // typed on Value and preserving event_time. Without a tag, such a record
    // falls through to the merge path (overlap merges, fresh is dropped).
    AsyncSessionWindowOperator& late_output_tag(OutputTag<Value> tag) {
        late_tag_ = std::move(tag);
        return *this;
    }

    // --- synchronous path (non-deferring backend) ---
    void process(const StreamElement<std::pair<Key, Value>>& element,
                 Emitter<std::pair<Key, Agg>>& out) override {
        if (element.is_data()) {
            auto kv = sessions_();
            for (const auto& rec : element.as_data()) {
                const auto& [k, v] = rec.value();
                const std::int64_t ts = event_ms_(rec);
                if (route_if_past_deadline_(ts, v, rec.event_time())) {
                    continue;  // late-late: routed to the side output
                }
                Sessions sessions = kv.get(k).value_or(Sessions{});
                const auto res = apply_record_(sessions, ts, v);
                if (!res.has_value()) {
                    continue;  // dropped: pure-late fresh session
                }
                kv.put(k, sessions);
                const std::string gate = encode_key_(k);
                register_fire_(res->merged_end, gate);
                register_purge_(res->merged_end, gate);
                if (res->refire) {
                    emit_session_(out, k, res->merged_agg, res->merged_end);
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
        // On-time fires emit from on_watermark; a within-band late re-fire emits
        // from the fold coroutine - both via `out`, which the runner keeps alive
        // across poll/drain (see KeyedAggregateOperator). The late-late
        // side-output decision is synchronous (time-only) and made before submit.
        if (!element.is_data()) {
            return;
        }
        for (const auto& rec : element.as_data()) {
            const auto& [k, v] = rec.value();
            const std::int64_t ts = event_ms_(rec);
            if (route_if_past_deadline_(ts, v, rec.event_time())) {
                continue;  // late-late: routed to the side output
            }
            const std::string gate = encode_key_(k);  // per-key gate + timer key
            auto kv = sessions_();
            // Capture by value: kv owns only a backend ptr + codecs; k/v/ts and
            // gate are copied into the coroutine frame; `out`/`this` by reference
            // (both outlive poll/drain). The put + any late re-emit + timer
            // registration run on the runner thread on resume (per-key gated).
            auto factory = [this, kv, k, v, ts, gate, &out]() mutable -> async::Task<void> {
                auto cur = co_await kv.get_async(k);
                Sessions sessions = cur.value_or(Sessions{});
                const auto res = apply_record_(sessions, ts, v);
                if (res.has_value()) {
                    kv.put(k, sessions);
                    register_fire_(res->merged_end, gate);
                    register_purge_(res->merged_end, gate);
                    if (res->refire) {
                        emit_session_(out, k, res->merged_agg, res->merged_end);
                    }
                }
                co_return;
            };
            while (!aec.submit(gate, factory)) {
                aec.poll();
            }
        }
    }

    // Restore the late-drop boundary on (re)start so a record that is late
    // relative to a pre-checkpoint watermark stays late after a restore.
    void open() override { current_watermark_ = wm_state_().get(0).value_or(0); }

    // Runs inside the AEC's epoch-gated release closure under async (and inline
    // under sync). Advances the late-drop watermark (persisted so it survives a
    // restore), then the base fires every due event-time timer.
    void on_watermark(Watermark wm, Emitter<std::pair<Key, Agg>>& out) override {
        current_watermark_ = std::max(current_watermark_, wm.timestamp().millis());
        wm_state_().put(0, current_watermark_);
        Operator<std::pair<Key, Value>, std::pair<Key, Agg>>::on_watermark(wm, out);
    }

    // Sweep one key's sessions against the (already-advanced) watermark, driven
    // by current_watermark_ rather than the timer's own ts so a stale timer (left
    // behind when a merge moved an end forward, or by an already-purged session)
    // is a harmless idempotent no-op. A not-yet-fired session whose end is at or
    // below the watermark fires (emit at its max timestamp, mark fired) and is
    // KEPT until its lateness deadline; a session past session_end +
    // allowed_lateness is purged. With allowed_lateness == 0 a due session fires
    // and is purged in the same sweep - byte-identical to before.
    void on_event_time_timer(std::int64_t /*timer_ts*/,
                             const std::string& key,
                             Emitter<std::pair<Key, Agg>>& out) override {
        const Key k = decode_key_(key);
        auto kv = sessions_();
        auto cur = kv.get(k);
        if (!cur.has_value()) {
            return;
        }
        Sessions sessions = std::move(*cur);
        Sessions kept;
        kept.reserve(sessions.size());
        Batch<std::pair<Key, Agg>> batch;
        for (auto& s : sessions) {
            if (!s.fired && current_watermark_ >= s.end) {
                batch.emplace(std::make_pair(k, s.agg), EventTime{s.end - 1});
                s.fired = true;
            }
            if (current_watermark_ >= s.end + allowed_lateness_ms_) {
                continue;  // past the lateness deadline: purge
            }
            kept.push_back(std::move(s));
        }
        if (!batch.empty()) {
            out.emit_data(std::move(batch));
        }
        if (kept.empty()) {
            kv.erase(k);
        } else {
            kv.put(k, kept);
        }
    }

    [[nodiscard]] bool fires_state_touching_timers() const noexcept override { return true; }
    [[nodiscard]] bool fires_state_touching_processing_time_timers() const noexcept override {
        return false;  // event-time timers only - they fire in the gated closure
    }

    std::string name() const override { return name_; }

private:
    static std::int64_t event_ms_(const Record<std::pair<Key, Value>>& rec) {
        return rec.event_time().value_or(EventTime{0}).millis();
    }

    // The outcome of merging one record: the (possibly extended) end of the
    // session it joined (for the fire + purge timers), whether that session had
    // already fired (so the caller re-emits a late pane), and the merged
    // aggregate (for that re-emit).
    struct ApplyResult {
        std::int64_t merged_end;
        bool refire;
        Agg merged_agg;
    };

    // Merge one record into the key's session set in place. Returns the merge
    // outcome, or nullopt when the record is dropped as pure-late. Byte-identical
    // merge semantics to the sync SessionWindowOperator: overlap test is
    // provisional [ts, ts + gap] against each existing [start, end]; merged
    // bounds are the min start / max end; the first overlapping aggregate seeds
    // the reduce, the rest fold via merger_, and the record's value folds in via
    // combiner_. The merged session's fired flag is the OR of every absorbed
    // session's fired flag (mirrors the sync any_fired), so a record that merges
    // into an already-fired session re-emits a late pane and keeps firing on
    // later late records until the lateness deadline purges it.
    std::optional<ApplyResult> apply_record_(Sessions& sessions,
                                             std::int64_t ts,
                                             const Value& v) const {
        const std::int64_t prov_start = ts;
        const std::int64_t prov_end = ts + gap_ms_;

        std::int64_t merged_start = prov_start;
        std::int64_t merged_end = prov_end;
        Agg merged_agg{};
        bool have_existing = false;
        bool any_fired = false;

        Sessions kept;
        kept.reserve(sessions.size());
        for (auto& s : sessions) {
            const bool overlap = s.start <= prov_end && s.end >= prov_start;
            if (!overlap) {
                kept.push_back(std::move(s));
                continue;
            }
            if (!have_existing) {
                merged_agg = s.agg;
                have_existing = true;
            } else {
                merged_agg = merger_(merged_agg, s.agg);
            }
            any_fired = any_fired || s.fired;
            merged_start = std::min(merged_start, s.start);
            merged_end = std::max(merged_end, s.end);
        }

        // Late-drop: a fresh, non-overlapping session whose provisional window is
        // already past its lateness deadline would fire immediately as a redundant
        // late pane covering already-emitted time. Drop it. A record that overlaps
        // an open (or fired-but-not-purged) session is never dropped here.
        if (!have_existing && prov_end + allowed_lateness_ms_ <= current_watermark_) {
            return std::nullopt;
        }

        if (!have_existing) {
            merged_agg = initial_();
        }
        merged_agg = combiner_(merged_agg, v);

        ApplyResult res{.merged_end = merged_end, .refire = any_fired, .merged_agg = merged_agg};
        kept.push_back(SessionRow<Agg>{.start = merged_start,
                                       .end = merged_end,
                                       .fired = any_fired,
                                       .agg = std::move(merged_agg)});
        // Canonical order: sorted by start, matching the sync operator's map.
        std::sort(kept.begin(), kept.end(), [](const SessionRow<Agg>& a, const SessionRow<Agg>& b) {
            return a.start < b.start;
        });
        sessions = std::move(kept);
        return res;
    }

    // Route a record whose provisional window is already past its lateness
    // deadline to the side output (if a tag is set), returning true when routed.
    // Time-only, so it runs synchronously before any deferred read - mirrors the
    // sync SessionWindowOperator::ingest_one_ late-late check.
    bool route_if_past_deadline_(std::int64_t ts,
                                 const Value& v,
                                 const std::optional<EventTime>& et) {
        if (!late_tag_.has_value() || this->runtime() == nullptr) {
            return false;
        }
        if (current_watermark_ < ts + gap_ms_ + allowed_lateness_ms_) {
            return false;
        }
        auto side = this->runtime()->template side_output<Value>(*late_tag_);
        Batch<Value> b;
        if (et.has_value()) {
            b.emplace(v, *et);
        } else {
            b.emplace(v);
        }
        side.emit_data(std::move(b));
        return true;
    }

    // Emit (k, agg) at the session's max timestamp. Sessions carry no PaneInfo
    // (matching the sync SessionWindowOperator, which never sets a pane).
    void emit_session_(Emitter<std::pair<Key, Agg>>& out,
                       const Key& k,
                       const Agg& agg,
                       std::int64_t end) {
        Batch<std::pair<Key, Agg>> b;
        b.emplace(std::make_pair(k, agg), EventTime{end - 1});
        out.emit_data(std::move(b));
    }

    void register_fire_(std::int64_t session_end, const std::string& key) {
        // Idempotent (the TimerService dedupes by (timestamp, key)).
        this->runtime()->timer_service()->register_event_time_timer(session_end, key);
    }
    // Revisit the key after the lateness band closes so a fired-but-not-purged
    // session is swept out even with no further records (no-op when lateness 0).
    void register_purge_(std::int64_t session_end, const std::string& key) {
        if (allowed_lateness_ms_ > 0) {
            this->runtime()->timer_service()->register_event_time_timer(
                session_end + allowed_lateness_ms_, key);
        }
    }

    std::string encode_key_(const Key& k) const {
        const auto b = key_codec_.encode(k);
        return std::string(reinterpret_cast<const char*>(b.data()), b.size());
    }
    Key decode_key_(const std::string& s) const {
        auto v = key_codec_.decode(
            std::span<const std::byte>{reinterpret_cast<const std::byte*>(s.data()), s.size()});
        if (!v.has_value()) {
            // A symmetric codec round-trips, so this only fires on genuine
            // corruption. Fail loudly rather than silently fire the wrong key.
            throw std::runtime_error(name_ + ": corrupt event-time timer key (key decode failed)");
        }
        return *v;
    }

    KeyedState<Key, Sessions> sessions_() {
        return this->runtime()->template keyed_state<Key, Sessions>(
            "sessions",
            key_codec_,
            vector_codec<SessionRow<Agg>>(session_row_codec<Agg>(agg_codec_)));
    }
    // Operator-scoped late-drop boundary, persisted under a fixed key so it
    // survives checkpoint/restore (v1: same-parallelism; rescale routing of this
    // slot is a follow-on, as for the other async window operators).
    KeyedState<std::int64_t, std::int64_t> wm_state_() {
        return this->runtime()->template keyed_state<std::int64_t, std::int64_t>(
            "wm", int64_codec(), int64_codec());
    }

    std::int64_t gap_ms_;
    std::int64_t allowed_lateness_ms_{0};
    std::optional<OutputTag<Value>> late_tag_;
    Initial initial_;
    Combiner combiner_;
    AggMerger merger_;
    Codec<Key> key_codec_;
    Codec<Agg> agg_codec_;
    std::string name_;
    // Latest watermark seen; persisted in the "wm" slot (see open()).
    std::int64_t current_watermark_{0};
};

}  // namespace clink
