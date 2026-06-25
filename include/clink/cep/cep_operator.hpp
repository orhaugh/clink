#pragma once

// CEP operator - NFA-based pattern matcher.
//
// CepOperator<T, U> consumes a stream of T, matches a compiled
// Pattern<T> against the per-key event sequence, and emits U values
// produced by a user-supplied select function each time a full match
// is found.
//
// Runtime semantics:
//
//   * Partial matches are kept per key in
//       KeyedState<int64_t, vector<PartialMatch<T>>>
//     under the slot "__cep_partials__". One row per user key keeps
//     routing-by-key correct under parallelism > 1.
//   * Each PartialMatch tracks its captured events, the current step
//     it's evaluating against, and the count of captures within that
//     step (quantifier support: a one_or_more or times(min,max) step
//     captures multiple events before advancing).
//   * Negative steps (NotNext / NotFollowedBy) don't capture events:
//     a non-matching event passes through and the partial advances
//     to the next step; a matching event kills the partial.
//   * Iterative predicates receive a PatternMatch<T> view of the
//     events captured so far - useful for "next event > last" rules.
//   * On watermark advance: prune partials whose start_ts is older
//     than (wm - within). When a `timed_out_tag` + selector are set,
//     each pruned partial is emitted as a side output via the tag
//     before being dropped.
//
// Non-keyed CEP routes through a sentinel key 0 - the non-keyed
// fluent helper wires a constant int64-extractor. Either path uses
// the same operator.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "clink/cep/pattern.hpp"
#include "clink/core/codec.hpp"
#include "clink/core/hash_map.hpp"
#include "clink/core/types.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/keyed_state.hpp"

namespace clink::cep {

// PatternMatch<T> is defined in pattern.hpp so iterative predicates
// can also reference it.

namespace detail {

// In-memory representation of a partial match.
//
// `start_ts` is the event_time of the first captured event, what
// `within()` measures against.
// `matched` is the flat list of captured events in arrival order.
// `step_names` parallel-tracks which step each captured event belongs
// to - necessary because quantifiers let a single step capture
// multiple events.
// `current_step` is the index into Pattern::steps() the partial is
// currently evaluating against. `current_step_count` is the number
// of captures already taken within that step (for quantifier support).
template <typename T>
struct PartialMatch {
    std::int64_t start_ts{0};
    std::vector<T> matched;
    std::vector<std::string> step_names;
    // Per-captured-event timestamps. Parallels `matched` / `step_names`
    // - entry i is the event_time of matched[i]. Used by after-match
    // skip strategies (SkipToFirst / SkipToLast) which need to look
    // up the timestamp of the first/last event captured for a named
    // step. Defaulted to 0 for legacy partials that lack timestamps;
    // such partials simply can't be eviction targets for the named
    // strategies - acceptable for back-compat restore from an older
    // snapshot format.
    std::vector<std::int64_t> event_timestamps;
    std::uint32_t current_step{0};
    std::uint32_t current_step_count{0};
};

// Codec for PartialMatch<T>. Layout:
//   [int64 start_ts]
//   [u32 current_step]
//   [u32 current_step_count]
//   [vector<string> step_names]
//   [vector<T> matched]
//   [vector<int64> event_timestamps]   (optional - absent in legacy)
template <typename T>
inline Codec<PartialMatch<T>> partial_match_codec(Codec<T> t_codec) {
    auto strings = vector_codec(string_codec());
    auto matched_c = vector_codec(std::move(t_codec));
    auto i64s = vector_codec(int64_codec());
    return Codec<PartialMatch<T>>{
        .encode =
            [strings, matched_c, i64s](const PartialMatch<T>& m) {
                typename Codec<PartialMatch<T>>::Bytes out;
                auto put_i64 = [&out](std::int64_t v) {
                    for (int i = 0; i < 8; ++i) {
                        out.push_back(static_cast<std::byte>(
                            (static_cast<std::uint64_t>(v) >> (i * 8)) & 0xFF));
                    }
                };
                auto put_u32 = [&out](std::uint32_t v) {
                    for (int i = 0; i < 4; ++i) {
                        out.push_back(static_cast<std::byte>((v >> (i * 8)) & 0xFF));
                    }
                };
                put_i64(m.start_ts);
                put_u32(m.current_step);
                put_u32(m.current_step_count);
                auto names_bytes = strings.encode(m.step_names);
                auto matched_bytes = matched_c.encode(m.matched);
                auto ts_bytes = i64s.encode(m.event_timestamps);
                out.insert(out.end(), names_bytes.begin(), names_bytes.end());
                out.insert(out.end(), matched_bytes.begin(), matched_bytes.end());
                out.insert(out.end(), ts_bytes.begin(), ts_bytes.end());
                return out;
            },
        .decode = [strings, matched_c, i64s](typename Codec<PartialMatch<T>>::BytesView b)
            -> std::optional<PartialMatch<T>> {
            if (b.size() < 16) {
                return std::nullopt;
            }
            auto read_i64 = [&b](std::size_t off) -> std::int64_t {
                std::uint64_t v = 0;
                for (int i = 0; i < 8; ++i) {
                    v |= static_cast<std::uint64_t>(static_cast<unsigned char>(b[off + i]))
                         << (i * 8);
                }
                return static_cast<std::int64_t>(v);
            };
            auto read_u32 = [&b](std::size_t off) -> std::uint32_t {
                std::uint32_t v = 0;
                for (int i = 0; i < 4; ++i) {
                    v |= static_cast<std::uint32_t>(static_cast<unsigned char>(b[off + i]))
                         << (i * 8);
                }
                return v;
            };
            // Walks one vector_codec-encoded length-prefixed sequence
            // and returns the number of bytes it occupies starting at
            // `off` within `rest`. Layout per vector_codec:
            //   [u32 count] [u32 len_i] [bytes_i] ... (count times)
            auto vec_span_len = [](std::span<const std::byte> rest,
                                   std::size_t off) -> std::optional<std::size_t> {
                const std::size_t start = off;
                if (off + 4 > rest.size())
                    return std::nullopt;
                std::uint32_t count = 0;
                for (int i = 0; i < 4; ++i) {
                    count |= static_cast<std::uint32_t>(static_cast<unsigned char>(rest[off + i]))
                             << (i * 8);
                }
                off += 4;
                for (std::uint32_t i = 0; i < count; ++i) {
                    if (off + 4 > rest.size())
                        return std::nullopt;
                    std::uint32_t len = 0;
                    for (int k = 0; k < 4; ++k) {
                        len |= static_cast<std::uint32_t>(static_cast<unsigned char>(rest[off + k]))
                               << (k * 8);
                    }
                    off += 4 + len;
                    if (off > rest.size())
                        return std::nullopt;
                }
                return off - start;
            };

            PartialMatch<T> out;
            out.start_ts = read_i64(0);
            out.current_step = read_u32(8);
            out.current_step_count = read_u32(12);
            auto rest = b.subspan(16);
            // step_names span.
            auto names_span = vec_span_len(rest, 0);
            if (!names_span.has_value())
                return std::nullopt;
            auto names_decoded = strings.decode(rest.subspan(0, *names_span));
            if (!names_decoded.has_value())
                return std::nullopt;
            out.step_names = std::move(*names_decoded);

            // matched span.
            auto matched_span = vec_span_len(rest, *names_span);
            if (!matched_span.has_value())
                return std::nullopt;
            auto matched_decoded = matched_c.decode(rest.subspan(*names_span, *matched_span));
            if (!matched_decoded.has_value())
                return std::nullopt;
            out.matched = std::move(*matched_decoded);

            // Optional event_timestamps span (absent in legacy bytes
            // produced before the field was added - decode tolerates
            // both shapes for back-compat).
            const std::size_t after_matched = *names_span + *matched_span;
            if (after_matched < rest.size()) {
                auto ts_decoded = i64s.decode(rest.subspan(after_matched));
                if (!ts_decoded.has_value())
                    return std::nullopt;
                out.event_timestamps = std::move(*ts_decoded);
            }
            return out;
        }};
}

}  // namespace detail

// CepOperator<T, U>: T-in, U-out NFA-based pattern matcher.
template <typename T, typename U>
class CepOperator final : public Operator<T, U> {
public:
    using KeyFn = std::function<std::int64_t(const T&)>;
    using SelectFn = std::function<U(const PatternMatch<T>&)>;
    // FlatSelect: emit 0..N records per pattern match. Mirrors
    // PatternFlatSelectFunction. Mutually exclusive with SelectFn - a
    // CepOperator is built with one or the other; mixing them would
    // make the per-match output count nondeterministic.
    using FlatSelectFn = std::function<std::vector<U>(const PatternMatch<T>&)>;

    CepOperator(
        Pattern<T> pattern, Codec<T> t_codec, KeyFn key_fn, SelectFn select_fn, std::string name)
        : pattern_(std::move(pattern)),
          t_codec_(std::move(t_codec)),
          key_fn_(std::move(key_fn)),
          select_fn_(std::move(select_fn)),
          name_(std::move(name)) {
        if (auto err = pattern_.validate(); !err.empty()) {
            throw std::runtime_error("CepOperator: invalid pattern - " + err);
        }
    }

    // FlatSelect constructor: each completed match calls
    // `flat_select_fn` and emits each U in the returned vector
    // (vector may be empty to skip emission for this match).
    CepOperator(Pattern<T> pattern,
                Codec<T> t_codec,
                KeyFn key_fn,
                FlatSelectFn flat_select_fn,
                std::string name)
        : pattern_(std::move(pattern)),
          t_codec_(std::move(t_codec)),
          key_fn_(std::move(key_fn)),
          flat_select_fn_(std::move(flat_select_fn)),
          name_(std::move(name)) {
        if (auto err = pattern_.validate(); !err.empty()) {
            throw std::runtime_error("CepOperator: invalid pattern - " + err);
        }
    }

    // Register an OutputTag + selector for partials evicted by
    // `within()`. The selector builds the side-output payload from
    // the timed-out partial's PatternMatch view (events captured so
    // far, grouped by step name). Without these, evicted partials
    // are silently dropped (the historic v1 behaviour). The tag's L
    // type can differ from the operator's U - they're independent
    // channels.
    template <typename L>
    CepOperator& with_timed_out_output(OutputTag<L> tag,
                                       std::function<L(const PatternMatch<T>&)> sel) {
        timed_out_tag_id_ = tag.id;
        timed_out_emit_ = [sel = std::move(sel), tag](RuntimeContext* rt,
                                                      const PatternMatch<T>& match) {
            if (rt == nullptr) {
                return;
            }
            auto side = rt->template side_output<L>(tag);
            Batch<L> b;
            b.emplace(sel(match));
            side.emit_data(std::move(b));
        };
        return *this;
    }

    // Vector-returning timed-out selector. Each evicted partial maps
    // to 0..N records on the side output. Mirror of
    // with_timed_out_output for the PatternFlatSelectFunction
    // shape. Mutually exclusive with the scalar variant - last
    // setter wins.
    template <typename L>
    CepOperator& with_timed_out_flat_output(
        OutputTag<L> tag, std::function<std::vector<L>(const PatternMatch<T>&)> flat_sel) {
        timed_out_tag_id_ = tag.id;
        timed_out_emit_ = [flat_sel = std::move(flat_sel), tag](RuntimeContext* rt,
                                                                const PatternMatch<T>& match) {
            if (rt == nullptr) {
                return;
            }
            auto results = flat_sel(match);
            if (results.empty()) {
                return;
            }
            auto side = rt->template side_output<L>(tag);
            Batch<L> b;
            for (auto& r : results) {
                b.emplace(std::move(r));
            }
            side.emit_data(std::move(b));
        };
        return *this;
    }

    void open() override {
        auto* rt = this->runtime();
        if (rt == nullptr || !rt->has_state_backend()) {
            // Without a state backend, fall back to an in-memory shadow
            // that is lost on restart. The default cluster wiring
            // always supplies a backend; this branch is for unit tests
            // using a no-state Dag.
            state_.reset();
            return;
        }
        state_ = std::make_unique<KeyedState<std::int64_t, std::vector<detail::PartialMatch<T>>>>(
            rt->template keyed_state<std::int64_t, std::vector<detail::PartialMatch<T>>>(
                kSlotName, int64_codec(), vector_codec(detail::partial_match_codec<T>(t_codec_))));
    }

    void process(const StreamElement<T>& element, Emitter<U>& out) override {
        if (element.is_data()) {
            Batch<U> batch_out;
            for (const auto& rec : element.as_data()) {
                const auto k = key_fn_(rec.value());
                advance_for_key_(k, rec, batch_out);
            }
            if (!batch_out.empty()) {
                out.emit_data(std::move(batch_out));
            }
        } else if (element.is_watermark()) {
            this->on_watermark(element.as_watermark(), out);
        } else {
            this->on_barrier(element.as_barrier(), out);
        }
    }

    void on_watermark(Watermark wm, Emitter<U>& out) override {
        if (auto within = pattern_.within_duration(); within.has_value()) {
            prune_expired_(wm.timestamp().millis() - within->count(), wm, out);
        }
        Operator<T, U>::on_watermark(wm, out);
    }

    void flush(Emitter<U>& /*out*/) override {
        // End-of-stream: drop incomplete partials silently. // default is the same - partials that
        // never complete are not surfaced as output unless the user wires a timed-out side output,
        // which is deferred in v1.
    }

    std::string name() const override { return name_; }

private:
    static constexpr const char* kSlotName = "__cep_partials__";

    std::vector<detail::PartialMatch<T>> load_partials_(std::int64_t k) const {
        if (!state_) {
            auto it = shadow_.find(k);
            return it == shadow_.end() ? std::vector<detail::PartialMatch<T>>{} : it->second;
        }
        return state_->get(k).value_or(std::vector<detail::PartialMatch<T>>{});
    }

    void save_partials_(std::int64_t k, std::vector<detail::PartialMatch<T>> partials) {
        if (!state_) {
            if (partials.empty()) {
                shadow_.erase(k);
            } else {
                shadow_[k] = std::move(partials);
            }
            return;
        }
        if (partials.empty()) {
            state_->erase(k);
        } else {
            state_->put(k, partials);
        }
    }

    // Evaluate a step's predicate against (event, match-so-far).
    // Iterative predicate (if set) wins over simple; missing
    // predicates accept everything (matches behaviour when
    // .where() is omitted).
    [[nodiscard]] bool evaluate_step_(const Step<T>& step,
                                      const T& v,
                                      const detail::PartialMatch<T>& p) const {
        if (step.iterative) {
            return step.iterative(v, build_match_view_(p));
        }
        if (step.predicate) {
            return step.predicate(v);
        }
        return true;
    }

    // Build a step-name -> events view from a PartialMatch. Used by
    // both iterative predicates and the user-facing emit / timed-out
    // paths.
    [[nodiscard]] PatternMatch<T> build_match_view_(const detail::PartialMatch<T>& p) const {
        PatternMatch<T> match;
        for (std::size_t i = 0; i < p.matched.size(); ++i) {
            match[p.step_names[i]].push_back(p.matched[i]);
        }
        return match;
    }

    // Attempt to advance one partial by consuming `rec`. Returns
    // true if the partial completed and was emitted; false if it
    // survives (push to next_partials by the caller) or died (caller
    // drops it via the third out-parameter).
    enum class StepOutcome : std::uint8_t {
        Keep,       // partial survives unchanged
        Advanced,   // partial captured / advanced; survives
        Completed,  // partial finished - emitted, caller drops
        Died,       // partial dies; caller drops
    };

    StepOutcome advance_one_(detail::PartialMatch<T>& p,
                             const Record<T>& rec,
                             Batch<U>& batch_out) {
        const auto& steps = pattern_.steps();
        // Walk through steps from current_step. A single event may
        // satisfy the current step at max_count and immediately
        // advance into the next step (e.g. one_or_more with a
        // non-matching event after some captures). Iterating bounds
        // by steps.size() to avoid infinite loops on degenerate
        // patterns (validated empty).
        for (std::size_t guard = 0; guard <= steps.size(); ++guard) {
            if (p.current_step >= steps.size()) {
                // Off-the-end - shouldn't happen, treat as completion.
                emit_match_(p, batch_out, rec.event_time());
                return StepOutcome::Completed;
            }
            const auto& step = steps[p.current_step];
            const bool match = evaluate_step_(step, rec.value(), p);

            if (is_negative_edge(step.incoming)) {
                if (match) {
                    return StepOutcome::Died;  // negative violated
                }
                // Trailing NotFollowedBy: stay on this step until the
                // within-window closes (handled by prune_expired_,
                // which completes such partials as a SUCCESS instead
                // of timing them out). Non-match events just keep the
                // partial alive on this step.
                const bool is_trailing = (p.current_step + 1 == steps.size());
                if (is_trailing && step.incoming == EdgeKind::NotFollowedBy) {
                    return StepOutcome::Keep;
                }
                // Non-trailing negative OR trailing NotNext: advance
                // past the negative step.
                ++p.current_step;
                p.current_step_count = 0;
                if (p.current_step >= steps.size()) {
                    // Trailing NotNext that didn't match - the partial
                    // completes as a success. Trailing NotFollowedBy
                    // is handled above (deferred to prune_expired_).
                    emit_match_(p, batch_out, rec.event_time());
                    return StepOutcome::Completed;
                }
                // Continue the loop - re-evaluate `rec` against the
                // next step. NotNext semantics imply this event was
                // consumed by the negative step.
                if (step.incoming == EdgeKind::NotNext) {
                    return StepOutcome::Advanced;
                }
                continue;
            }

            // Positive step.
            if (match) {
                // Lazy advance: if the step is lazy, we've already met
                // min, and the NEXT step's predicate also matches this
                // event, prefer to advance into the next step instead
                // of greedily capturing more here. Negative next steps
                // are skipped over by the negative branch above when
                // re-entering the loop, so we only peek at positive
                // next steps for the lazy-advance check.
                if (step.lazy && p.current_step_count >= step.min_count &&
                    p.current_step + 1 < steps.size()) {
                    const auto& next_step = steps[p.current_step + 1];
                    if (!is_negative_edge(next_step.incoming) &&
                        evaluate_step_(next_step, rec.value(), p)) {
                        ++p.current_step;
                        p.current_step_count = 0;
                        continue;  // re-evaluate this event against the new current_step
                    }
                }
                // Capture if we haven't yet hit max_count.
                if (p.current_step_count < step.max_count) {
                    const std::int64_t event_ts = rec.event_time().value_or(EventTime{0}).millis();
                    if (p.matched.empty()) {
                        p.start_ts = event_ts;
                    }
                    p.matched.push_back(rec.value());
                    p.step_names.push_back(step.name);
                    p.event_timestamps.push_back(event_ts);
                    ++p.current_step_count;
                }
                // If at max, auto-advance to the next step.
                if (p.current_step_count >= step.max_count) {
                    ++p.current_step;
                    p.current_step_count = 0;
                    if (p.current_step >= steps.size()) {
                        emit_match_(p, batch_out, rec.event_time());
                        return StepOutcome::Completed;
                    }
                    return StepOutcome::Advanced;
                }
                return StepOutcome::Advanced;
            }

            // Non-match on a positive step.
            if (p.current_step_count >= step.min_count) {
                // Quantifier minimum is satisfied - advance to next
                // step and re-evaluate this event against it. Useful
                // for one_or_more followed by something else.
                ++p.current_step;
                p.current_step_count = 0;
                if (p.current_step >= steps.size()) {
                    emit_match_(p, batch_out, rec.event_time());
                    return StepOutcome::Completed;
                }
                continue;
            }
            // Quantifier minimum not yet satisfied. Strict Next dies;
            // FollowedBy waits.
            if (step.incoming == EdgeKind::Next) {
                return StepOutcome::Died;
            }
            return StepOutcome::Keep;
        }
        return StepOutcome::Keep;
    }

    void advance_for_key_(std::int64_t k, const Record<T>& rec, Batch<U>& batch_out) {
        auto partials = load_partials_(k);
        std::vector<detail::PartialMatch<T>> next_partials;
        next_partials.reserve(partials.size() + 1);
        const auto& strategy = pattern_.skip_strategy();
        // For skip strategies that suppress sibling partials within
        // the SAME dispatch cycle (SkipPastLastEvent, SkipToNext,
        // SkipToFirst/Last), we apply the strategy after EACH
        // completion to the partials not yet processed and to the
        // already-collected next_partials. This evicts both
        // "earlier" survivors and pending later partials that would
        // otherwise also complete on this event.
        std::vector<detail::PartialMatch<T>> completed_this_event;

        auto apply_to_pending = [&](const detail::PartialMatch<T>& done, std::size_t start_idx) {
            if (strategy.kind == SkipStrategy::Kind::NoSkip) {
                return;
            }
            // Apply to already-collected next_partials (survivors).
            apply_skip_strategy_(strategy, done, next_partials);
            // Apply to not-yet-processed partials by erasing in place.
            partials.erase(std::remove_if(partials.begin() + static_cast<std::ptrdiff_t>(start_idx),
                                          partials.end(),
                                          [&](const detail::PartialMatch<T>& p) {
                                              return should_evict_for_strategy_(strategy, done, p);
                                          }),
                           partials.end());
        };

        for (std::size_t i = 0; i < partials.size(); ++i) {
            auto& p = partials[i];
            switch (advance_one_(p, rec, batch_out)) {
                case StepOutcome::Keep:
                case StepOutcome::Advanced:
                    next_partials.push_back(std::move(p));
                    break;
                case StepOutcome::Completed:
                    completed_this_event.push_back(p);  // copy for skip-ref
                    apply_to_pending(completed_this_event.back(), i + 1);
                    break;
                case StepOutcome::Died:
                    break;
            }
        }

        // Spawn a new partial if the begin step matches.
        // SkipPastLastEvent suppresses fresh spawns at the same event
        // as a completion; the other strategies don't restrict
        // spawning.
        const bool suppress_fresh_spawn =
            strategy.kind == SkipStrategy::Kind::SkipPastLastEvent && !completed_this_event.empty();
        if (!suppress_fresh_spawn) {
            detail::PartialMatch<T> fresh;
            fresh.current_step = 0;
            fresh.current_step_count = 0;
            switch (advance_one_(fresh, rec, batch_out)) {
                case StepOutcome::Advanced:
                    next_partials.push_back(std::move(fresh));
                    break;
                case StepOutcome::Completed:
                    completed_this_event.push_back(fresh);
                    apply_skip_strategy_(strategy, completed_this_event.back(), next_partials);
                    break;
                case StepOutcome::Died:
                case StepOutcome::Keep:
                    // Keep on initial spawn = the first event didn't
                    // even satisfy step[0]; drop.
                    break;
            }
        }

        save_partials_(k, std::move(next_partials));
    }

    // Mirror of apply_skip_strategy_'s per-partial decision, exposed
    // so the dispatch loop can evict partials that haven't been
    // processed yet (in the same dispatch cycle as a completion).
    [[nodiscard]] bool should_evict_for_strategy_(const SkipStrategy& strategy,
                                                  const detail::PartialMatch<T>& completed,
                                                  const detail::PartialMatch<T>& p) const {
        if (strategy.kind == SkipStrategy::Kind::NoSkip) {
            return false;
        }
        const std::int64_t matched_end_ts = completed.event_timestamps.empty()
                                                ? completed.start_ts
                                                : completed.event_timestamps.back();
        switch (strategy.kind) {
            case SkipStrategy::Kind::NoSkip:
                return false;
            case SkipStrategy::Kind::SkipPastLastEvent:
                return p.start_ts <= matched_end_ts;
            case SkipStrategy::Kind::SkipToNext:
                return p.start_ts == completed.start_ts;
            case SkipStrategy::Kind::SkipToFirst:
            case SkipStrategy::Kind::SkipToLast: {
                std::optional<std::int64_t> cutoff;
                for (std::size_t i = 0;
                     i < completed.step_names.size() && i < completed.event_timestamps.size();
                     ++i) {
                    if (completed.step_names[i] == strategy.name_target) {
                        cutoff = completed.event_timestamps[i];
                        if (strategy.kind == SkipStrategy::Kind::SkipToFirst) {
                            break;
                        }
                    }
                }
                if (!cutoff.has_value())
                    return false;
                return p.start_ts < *cutoff;
            }
        }
        return false;
    }

    // Walk `survivors`, evicting in place per the strategy. The
    // `completed` partial is the one that just emitted a match; it's
    // already absent from survivors.
    void apply_skip_strategy_(const SkipStrategy& strategy,
                              const detail::PartialMatch<T>& completed,
                              std::vector<detail::PartialMatch<T>>& survivors) {
        if (strategy.kind == SkipStrategy::Kind::NoSkip) {
            return;
        }
        // Helper: find timestamp of the FIRST event captured for step
        // `name` in `completed`, or nullopt if absent.
        auto first_ts_for = [&](const std::string& name) -> std::optional<std::int64_t> {
            for (std::size_t i = 0;
                 i < completed.step_names.size() && i < completed.event_timestamps.size();
                 ++i) {
                if (completed.step_names[i] == name) {
                    return completed.event_timestamps[i];
                }
            }
            return std::nullopt;
        };
        auto last_ts_for = [&](const std::string& name) -> std::optional<std::int64_t> {
            std::optional<std::int64_t> last;
            for (std::size_t i = 0;
                 i < completed.step_names.size() && i < completed.event_timestamps.size();
                 ++i) {
                if (completed.step_names[i] == name) {
                    last = completed.event_timestamps[i];
                }
            }
            return last;
        };
        const std::int64_t matched_end_ts = completed.event_timestamps.empty()
                                                ? completed.start_ts
                                                : completed.event_timestamps.back();

        auto should_evict = [&](const detail::PartialMatch<T>& p) -> bool {
            switch (strategy.kind) {
                case SkipStrategy::Kind::NoSkip:
                    return false;
                case SkipStrategy::Kind::SkipPastLastEvent:
                    // Drop partials that started before or at the
                    // matched-pattern's last event timestamp.
                    return p.start_ts <= matched_end_ts;
                case SkipStrategy::Kind::SkipToNext:
                    // Drop partials whose start_ts == the matched
                    // start_ts. Later partials survive.
                    return p.start_ts == completed.start_ts;
                case SkipStrategy::Kind::SkipToFirst: {
                    auto cutoff = first_ts_for(strategy.name_target);
                    if (!cutoff.has_value())
                        return false;  // no matching named step → no-op
                    return p.start_ts < *cutoff;
                }
                case SkipStrategy::Kind::SkipToLast: {
                    auto cutoff = last_ts_for(strategy.name_target);
                    if (!cutoff.has_value())
                        return false;
                    return p.start_ts < *cutoff;
                }
            }
            return false;
        };
        survivors.erase(std::remove_if(survivors.begin(), survivors.end(), should_evict),
                        survivors.end());
    }

    void emit_match_(const detail::PartialMatch<T>& p, Batch<U>& out, std::optional<EventTime> ts) {
        PatternMatch<T> match = build_match_view_(p);
        if (flat_select_fn_) {
            auto results = flat_select_fn_(match);
            for (auto& r : results) {
                if (ts.has_value()) {
                    out.emplace(std::move(r), *ts);
                } else {
                    out.emplace(std::move(r));
                }
            }
            return;
        }
        if (ts.has_value()) {
            out.emplace(select_fn_(match), *ts);
        } else {
            out.emplace(select_fn_(match));
        }
    }

    // Returns true if `p` is at a trailing NotFollowedBy step that
    // hasn't been violated. Such partials should be completed (as
    // success) when the within-window closes, not routed to the
    // timed-out side output.
    [[nodiscard]] bool is_pending_trailing_negative_(const detail::PartialMatch<T>& p) const {
        const auto& steps = pattern_.steps();
        if (steps.empty()) {
            return false;
        }
        return p.current_step + 1 == steps.size() &&
               steps[p.current_step].incoming == EdgeKind::NotFollowedBy;
    }

    void prune_expired_(std::int64_t cutoff_ts, Watermark wm, Emitter<U>& out) {
        const auto emit_timed_out = [this](const detail::PartialMatch<T>& m) {
            if (timed_out_emit_ && m.matched.size() > 0) {
                timed_out_emit_(this->runtime(), build_match_view_(m));
            }
        };
        // Successful completion of a trailing-negative partial: emit
        // via the operator's main output. event_time is the watermark
        // - the deadline at which we became sure the negative held.
        Batch<U> success_batch;
        const auto emit_success = [&](const detail::PartialMatch<T>& m) {
            emit_match_(m, success_batch, std::optional<EventTime>{wm.timestamp()});
        };

        if (!state_) {
            for (auto it = shadow_.begin(); it != shadow_.end();) {
                auto& partials = it->second;
                std::vector<detail::PartialMatch<T>> kept;
                kept.reserve(partials.size());
                for (auto& m : partials) {
                    if (m.start_ts < cutoff_ts) {
                        if (is_pending_trailing_negative_(m)) {
                            emit_success(m);
                        } else {
                            emit_timed_out(m);
                        }
                    } else {
                        kept.push_back(std::move(m));
                    }
                }
                if (kept.empty()) {
                    it = shadow_.erase(it);
                } else {
                    it->second = std::move(kept);
                    ++it;
                }
            }
            if (!success_batch.empty()) {
                out.emit_data(std::move(success_batch));
            }
            return;
        }
        // Walk the slot once: for each key whose partial-list has any
        // expired entries, rewrite it; if the list goes empty, erase.
        // KeyedState forbids mutation during scan, so we buffer first.
        std::vector<std::pair<std::int64_t, std::vector<detail::PartialMatch<T>>>> updates;
        std::vector<std::int64_t> to_erase;
        std::vector<detail::PartialMatch<T>> expired;
        state_->scan([&](std::int64_t k, const std::vector<detail::PartialMatch<T>>& partials) {
            std::vector<detail::PartialMatch<T>> kept;
            kept.reserve(partials.size());
            for (const auto& m : partials) {
                if (m.start_ts < cutoff_ts) {
                    expired.push_back(m);
                } else {
                    kept.push_back(m);
                }
            }
            if (kept.size() == partials.size()) {
                return;  // no change
            }
            if (kept.empty()) {
                to_erase.push_back(k);
            } else {
                updates.emplace_back(k, std::move(kept));
            }
        });
        for (const auto& m : expired) {
            if (is_pending_trailing_negative_(m)) {
                emit_success(m);
            } else {
                emit_timed_out(m);
            }
        }
        if (!success_batch.empty()) {
            out.emit_data(std::move(success_batch));
        }
        for (auto k : to_erase) {
            state_->erase(k);
        }
        for (auto& [k, kept] : updates) {
            state_->put(k, kept);
        }
    }

    Pattern<T> pattern_;
    Codec<T> t_codec_;
    KeyFn key_fn_;
    SelectFn select_fn_;
    FlatSelectFn flat_select_fn_;
    std::string name_;

    clink::FlatMap<std::int64_t, std::vector<detail::PartialMatch<T>>> shadow_;
    std::unique_ptr<KeyedState<std::int64_t, std::vector<detail::PartialMatch<T>>>> state_;

    // Optional timed-out side output. timed_out_tag_id_ is just for
    // introspection; the emitter closure (built by with_timed_out_output)
    // captures the tag's typed L and the user's selector so this
    // header doesn't need to know L's type.
    std::string timed_out_tag_id_;
    std::function<void(RuntimeContext*, const PatternMatch<T>&)> timed_out_emit_;
};

}  // namespace clink::cep
