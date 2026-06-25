#pragma once

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <limits>
#include <map>
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
#include "clink/operators/window_state.hpp"
#include "clink/runtime/output_tag.hpp"
#include "clink/state/keyed_state.hpp"

namespace clink {

// Keyed event-time session window. Models // EventTimeSessionWindows.withGap(gap):
//
//   * Each new record at event-time ts establishes a provisional window
//     [ts, ts + gap]. If that overlaps any existing session for the same
//     key, all overlapping sessions merge into one - new bounds are the
//     min start and max end across the merged sessions, the aggregate
//     is reduced via the user's `merger`, then the new record's value
//     is folded in via the standard `combiner`.
//
//   * A session fires when the watermark crosses session_end. After
//     firing, the session is purged.
//
//   * flush() at end-of-stream fires every still-open session.
//
//   * Durable (FOUND-2): a per-key set of sessions is persisted as ONE
//     KeyedState row per key holding the whole vector<SessionRow> (slot
//     "session_windows"), reached via the persistent ctor (Codec<Key> +
//     Codec<Agg>). A merge is therefore a single atomic put of the key's
//     vector - no multi-row sequence to tear - which is the same shape the
//     in-tree SessionProcessWindowAdapter uses. The in-memory sorted map stays
//     the authoritative hot path; open() rehydrates it by scanning the rows.
//     The persisted `fired` flag gates re-firing, so a session emitted before a
//     checkpoint does not re-emit after restore + replay. Default ctor =
//     in-memory only (unchanged). This is a STATEFUL operator: pin a stable
//     .uid() so its rows realign on restore. last_watermark_ms_ is not
//     persisted (re-derived from the replayed stream, as tumbling does).
//
// The user supplies three functions:
//   * `initial`    - produces the empty Agg.
//   * `combiner`   - folds a Value into an Agg ((agg, v) -> agg).
//   * `merger`     - combines two Aggs when sessions merge
//                    ((agg, agg) -> agg). Required because session
//                    merges happen between accumulated states; the
//                    combiner can't be applied with no Value to add.
template <typename Key, typename Value, typename Agg>
class SessionWindowOperator final : public Operator<std::pair<Key, Value>, std::pair<Key, Agg>> {
public:
    using Combiner = std::function<Agg(const Agg&, const Value&)>;
    using AggMerger = std::function<Agg(const Agg&, const Agg&)>;
    using Initial = std::function<Agg()>;

    SessionWindowOperator(std::chrono::milliseconds gap,
                          Initial initial,
                          Combiner combiner,
                          AggMerger merger,
                          std::string name = "session_window")
        : gap_(gap),
          initial_(std::move(initial)),
          combiner_(std::move(combiner)),
          merger_(std::move(merger)),
          name_(std::move(name)) {
        if (gap_.count() <= 0) {
            throw std::invalid_argument("SessionWindowOperator: gap must be > 0");
        }
    }

    // Persistent ctor: sessions survive checkpoint/restore via a KeyedState row
    // per key (slot "session_windows"). Codecs for the key and the aggregate.
    SessionWindowOperator(std::chrono::milliseconds gap,
                          Initial initial,
                          Combiner combiner,
                          AggMerger merger,
                          Codec<Key> key_codec,
                          Codec<Agg> agg_codec,
                          std::string name = "session_window")
        : gap_(gap),
          initial_(std::move(initial)),
          combiner_(std::move(combiner)),
          merger_(std::move(merger)),
          name_(std::move(name)),
          key_codec_(std::move(key_codec)),
          agg_codec_(std::move(agg_codec)) {
        if (gap_.count() <= 0) {
            throw std::invalid_argument("SessionWindowOperator: gap must be > 0");
        }
    }

    void open() override {
        if (key_codec_.has_value() && agg_codec_.has_value() && this->runtime() != nullptr &&
            this->runtime()->has_state_backend()) {
            keyed_ = std::make_unique<KeyedState<Key, std::vector<SessionRow<Agg>>>>(
                this->runtime()->template keyed_state<Key, std::vector<SessionRow<Agg>>>(
                    "session_windows",
                    *key_codec_,
                    vector_codec<SessionRow<Agg>>(session_row_codec<Agg>(*agg_codec_))));
            // Rehydrate the in-memory sorted map from the durable rows (the
            // std::map re-sorts on start, so intra-vector order is irrelevant).
            keyed_->scan([this](const Key& k, const std::vector<SessionRow<Agg>>& rows) {
                auto& m = by_key_[k];
                for (const auto& r : rows) {
                    m[r.start] = Session{.end = r.end, .agg = r.agg, .fired = r.fired};
                }
            });
        }
    }

    void close() override { keyed_.reset(); }

    // Configure how long after session_end this operator retains state
    // to accept late records. Mirrors allowedLateness on
    // EventTimeSessionWindows.
    SessionWindowOperator& allowed_lateness(std::chrono::milliseconds v) {
        allowed_lateness_ = v;
        return *this;
    }

    // Register an OutputTag to receive records arriving past the
    // session-lateness deadline (current_watermark > ts + gap +
    // allowed_lateness). Forwarded as-is, preserving event_time.
    // Without a tag, the historic "create / merge sessions
    // regardless of lateness" behavior holds. See
    // TumblingWindowOperator::late_output_tag for the rationale on
    // why this is opt-in.
    SessionWindowOperator& late_output_tag(OutputTag<Value> tag) {
        late_tag_ = std::move(tag);
        return *this;
    }

    void process(const StreamElement<std::pair<Key, Value>>& element,
                 Emitter<std::pair<Key, Agg>>& out) override {
        if (element.is_data()) {
            for (const auto& record : element.as_data()) {
                ingest_one_(record.value().first, record.value().second, record.event_time(), out);
            }
        } else if (element.is_watermark()) {
            const Watermark wm = element.as_watermark();
            last_watermark_ms_ = wm.timestamp().millis();
            fire_due_sessions(wm, out);
            this->on_watermark(wm, out);
        } else {
            this->on_barrier(element.as_barrier(), out);
        }
    }

    void flush(Emitter<std::pair<Key, Agg>>& out) override {
        fire_due_sessions(Watermark::max(), out);
    }

    // In write-back-cache mode (CLINK_WB_STATE_CACHE=1) per-mutation puts are
    // deferred; flush every key's current vector to the durable store once per
    // barrier. Erases happen unconditionally in persist_key_ (so an emptied key
    // is already gone), making this flush put-only with no orphan rows - the
    // same discipline TumblingWindowOperator uses.
    void on_barrier(CheckpointBarrier barrier, Emitter<std::pair<Key, Agg>>& out) override {
        if (keyed_ && wb_cache_skip_()) {
            for (const auto& [k, sessions] : by_key_) {
                if (!sessions.empty()) {
                    keyed_->put(k, flatten_(sessions));
                }
            }
        }
        out.emit_barrier(barrier);
    }

#ifdef CLINK_HAS_ARROW
    // Toggle the columnar-native ingest fast path (default on). Disabling it
    // forces the row path even for a columnar upstream - used by tests to
    // compare the two ingest paths on the SAME operator + SAME source.
    SessionWindowOperator& set_columnar_enabled(bool enabled) {
        columnar_enabled_ = enabled;
        return *this;
    }

    // Columnar-native ingest (increment 5). Mirrors the tumbling/sliding window
    // operators: only int64 key + int64 value ride the 3-column {event_time,
    // key, value} Arrow sidecar; other key/value types stay on the row path. The
    // fast path folds the key+value+ts buffers into the per-key session map with
    // ZERO Record<pair> materialization, by calling the SAME ingest_one_ the row
    // path uses - so the session merge/fire/late-data logic (all inside
    // handle_record_) is byte-identical to the row path; only the row decode is
    // skipped.
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
    struct Session {
        std::int64_t end{};
        Agg agg{};
        bool fired{false};
    };

    // Sessions per key, keyed by session_start. std::map keeps them
    // sorted so we can iterate near a record's provisional window in
    // O(log n) followed by a small linear sweep over overlapping
    // entries.
    clink::FlatMap<Key, std::map<std::int64_t, Session>> by_key_;

    // Per-record ingest: late-data side-output routing + dispatch to
    // handle_record_ (which owns the session merge/create logic). Shared
    // verbatim by the row path (process()) and the columnar fast path
    // (process_columnar()), so the two are exactly equivalent - the columnar
    // path only skips the Record<pair> decode.
    void ingest_one_(const Key& k,
                     const Value& v,
                     const std::optional<EventTime>& et,
                     Emitter<std::pair<Key, Agg>>& out) {
        const EventTime ts = et.value_or(EventTime{0});
        // Late-late check: a record at event_time ts can only belong to a
        // session whose end is >= ts. The latest such session's purge deadline
        // is ts + gap + allowed_lateness; once the watermark crosses that, no
        // session containing ts is still alive. Route to side output if a tag is
        // set; otherwise fall through to the historic merge-or-create path.
        if (late_tag_.has_value()) {
            const std::int64_t purge_at = ts.millis() + gap_.count() + allowed_lateness_.count();
            if (last_watermark_ms_ >= purge_at && this->runtime() != nullptr) {
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
        handle_record_(k, v, ts.millis(), out);
    }

    void handle_record_(const Key& k,
                        const Value& v,
                        std::int64_t ts,
                        Emitter<std::pair<Key, Agg>>& out) {
        auto& sessions = by_key_[k];

        const std::int64_t prov_start = ts;
        const std::int64_t prov_end = ts + gap_.count();

        std::int64_t merged_start = prov_start;
        std::int64_t merged_end = prov_end;
        Agg merged_agg{};
        bool have_existing = false;
        bool any_fired = false;

        auto it = sessions.begin();
        while (it != sessions.end()) {
            const std::int64_t s = it->first;
            const std::int64_t e = it->second.end;
            if (s > prov_end) {
                break;
            }
            if (e < prov_start) {
                ++it;
                continue;
            }
            // Overlap.
            if (!have_existing) {
                merged_agg = it->second.agg;
                have_existing = true;
            } else {
                merged_agg = merger_(merged_agg, it->second.agg);
            }
            if (it->second.fired) {
                any_fired = true;
            }
            merged_start = std::min(merged_start, s);
            merged_end = std::max(merged_end, e);
            it = sessions.erase(it);
        }

        if (!have_existing) {
            merged_agg = initial_();
        }
        merged_agg = combiner_(merged_agg, v);

        // If the merge absorbed an already-fired session, the resulting
        // session is considered fired too - re-emit a late pane with
        // the new aggregate. Subsequent late records keep doing this
        // until the session is purged.
        Session merged{.end = merged_end, .agg = merged_agg, .fired = any_fired};
        sessions[merged_start] = std::move(merged);

        if (any_fired) {
            Batch<std::pair<Key, Agg>> b;
            b.emplace(std::make_pair(k, std::move(merged_agg)), EventTime{merged_end - 1});
            out.emit_data(std::move(b));
            clink::metrics::op::window_panes_fired_inc(
                this->runtime() ? this->runtime()->metrics() : nullptr, this->id().value());
        }

        // Mirror the merged session set to the durable store (one atomic put of
        // the key's whole vector, or erase if it emptied).
        persist_key_(k);
    }

    void fire_due_sessions(Watermark wm, Emitter<std::pair<Key, Agg>>& out) {
        Batch<std::pair<Key, Agg>> emitted;
        const std::int64_t wm_ms = wm.timestamp().millis();

        for (auto& [k, sessions] : by_key_) {
            bool changed = false;
            auto it = sessions.begin();
            while (it != sessions.end()) {
                const std::int64_t end = it->second.end;
                const std::int64_t purge_at = end + allowed_lateness_.count();
                if (!it->second.fired && wm_ms >= end) {
                    emitted.emplace(std::make_pair(k, it->second.agg), EventTime{end - 1});
                    it->second.fired = true;
                    changed = true;
                }
                if (wm_ms >= purge_at) {
                    it = sessions.erase(it);
                    changed = true;
                } else {
                    ++it;
                }
            }
            // Mirror the fired/purged sweep for this key to the durable store.
            if (changed) {
                persist_key_(k);
            }
        }

        if (!emitted.empty()) {
            clink::metrics::op::window_panes_fired_inc(
                this->runtime() ? this->runtime()->metrics() : nullptr,
                this->id().value(),
                emitted.size());
            out.emit_data(std::move(emitted));
        }
    }

    // Serialise a key's sorted session map to the flat durable vector.
    static std::vector<SessionRow<Agg>> flatten_(const std::map<std::int64_t, Session>& m) {
        std::vector<SessionRow<Agg>> out;
        out.reserve(m.size());
        for (const auto& [start, s] : m) {
            out.push_back(
                SessionRow<Agg>{.start = start, .end = s.end, .fired = s.fired, .agg = s.agg});
        }
        return out;
    }

    // Mirror one key's in-memory session set to the durable store. Erase is
    // unconditional (both modes) so an emptied key never resurrects; put is
    // strict-mode-only (write-back mode defers it to on_barrier).
    void persist_key_(const Key& k) {
        if (!keyed_) {
            return;
        }
        auto it = by_key_.find(k);
        if (it == by_key_.end() || it->second.empty()) {
            keyed_->erase(k);
            return;
        }
        if (!wb_cache_skip_()) {
            keyed_->put(k, flatten_(it->second));
        }
    }

    static bool wb_cache_skip_() {
        static const bool skip = [] {
            const char* s = std::getenv("CLINK_WB_STATE_CACHE");
            return s != nullptr && std::string(s) == "1";
        }();
        return skip;
    }

    std::chrono::milliseconds gap_;
    std::chrono::milliseconds allowed_lateness_{0};
    std::optional<OutputTag<Value>> late_tag_;
    std::int64_t last_watermark_ms_{std::numeric_limits<std::int64_t>::min()};
    Initial initial_;
    Combiner combiner_;
    AggMerger merger_;
    std::string name_;
    std::optional<Codec<Key>> key_codec_;
    std::optional<Codec<Agg>> agg_codec_;
    std::unique_ptr<KeyedState<Key, std::vector<SessionRow<Agg>>>> keyed_;
#ifdef CLINK_HAS_ARROW
    // Columnar-native ingest fast path toggle (see set_columnar_enabled). Only
    // consulted for int64/int64 instantiations. Guarded with the columnar
    // methods that read it so a no-Arrow build carries neither.
    bool columnar_enabled_{true};
#endif
};

}  // namespace clink
