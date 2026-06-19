#pragma once

// Pattern DSL for CEP - Pattern<T, F> analogue. Patterns are
// built fluently with a chain of named steps, each carrying a
// predicate on T and an edge-kind that describes how it relates to
// the previous step:
//
//   * Begin           - the first step. Always present, exactly once.
//   * Next            - strict-contiguity: the very next event must
//                       satisfy this step's predicate, otherwise the
//                       partial match dies.
//   * FollowedBy      - relaxed-contiguity: skip events until one
//                       satisfies this step's predicate.
//   * NotNext         - strict negation: the very next event MUST NOT
//                       satisfy this step's predicate. A matching event
//                       kills the partial; a non-matching event advances
//                       past the negative step without capturing.
//   * NotFollowedBy   - relaxed negation: while waiting for the NEXT
//                       positive step to match, no event may satisfy
//                       this step's predicate. A matching event kills
//                       the partial. (V2 limit: NotFollowedBy at the
//                       very end of a pattern is treated as NotNext.)
//
// Quantifiers (counted repetition on a step) - applied via fluent
// modifiers AFTER `.where(...)` on a step:
//
//   .times(n)          → exactly n matches required (== times(n, n))
//   .times(min, max)   → between min and max matches; greedy by default
//   .one_or_more()     → 1..UINT32_MAX matches (Kleene+)
//   .optional()        → 0 or 1 matches (always advances, even on miss)
//
// Iterative conditions: `.where(iterative_fn)` where the predicate
// also receives a `PatternMatch<T>` view of the events captured so
// far. Useful for "next event must be greater than the last matched
// event" patterns. Simple non-iterative predicates remain supported.
//
// V1+V2 surface:
//
//   auto p = clink::cep::Pattern<Event>::begin("a")
//                .where([](const Event& e){ return e.type == kStart; })
//                .next("b")
//                .where([](const Event& e){ return e.type == kMid; })
//                .one_or_more()
//                .not_followed_by("bad")
//                .where([](const Event& e){ return e.type == kBlock; })
//                .followed_by("c")
//                .where([](const Event& e){ return e.type == kEnd; })
//                .within(std::chrono::seconds(10));
//
// Deferred (still missing  CEP):
//   - lazy / non-greedy quantifier controls (only greedy supported)
//   - after-match skip strategies beyond implicit SKIP_PAST_LAST
//   - PatternFlatSelectFunction (emit 0..N matches per partial)

#include <chrono>
#include <cstdint>
#include <functional>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace clink::cep {

// EdgeKind describes how this step connects to the previous one. The
// first step has Begin; subsequent steps have Next/FollowedBy or
// their negative counterparts depending on which chaining method the
// user called.
enum class EdgeKind : std::uint8_t {
    Begin,
    Next,
    FollowedBy,
    NotNext,
    NotFollowedBy,
};

[[nodiscard]] inline bool is_negative_edge(EdgeKind k) noexcept {
    return k == EdgeKind::NotNext || k == EdgeKind::NotFollowedBy;
}

// After-match skip strategy. Controls what happens to OTHER in-flight
// partial matches when one completes. Matches
// AfterMatchSkipStrategy:
//
//   * NoSkip               - emit every match independently (default).
//   * SkipPastLastEvent    - when a match completes, drop every other
//                            partial that started before or at the
//                            last event in the matched pattern. New
//                            spawns at the same event are suppressed.
//   * SkipToNext           - drop every other partial with the same
//                            start_ts as the completed match. Lets
//                            partials that started LATER survive.
//   * SkipToFirst(name)    - drop every other partial whose start_ts
//                            is strictly earlier than the timestamp
//                            of the FIRST event captured for step
//                            `name` in the completed match.
//   * SkipToLast(name)     - same as SkipToFirst but vs the LAST event
//                            of `name` in the completed match.
struct SkipStrategy {
    enum class Kind : std::uint8_t {
        NoSkip,
        SkipPastLastEvent,
        SkipToNext,
        SkipToFirst,
        SkipToLast,
    };
    Kind kind{Kind::NoSkip};
    std::string name_target;  // only for SkipToFirst / SkipToLast

    [[nodiscard]] static SkipStrategy no_skip() { return SkipStrategy{Kind::NoSkip, {}}; }
    [[nodiscard]] static SkipStrategy skip_past_last_event() {
        return SkipStrategy{Kind::SkipPastLastEvent, {}};
    }
    [[nodiscard]] static SkipStrategy skip_to_next() { return SkipStrategy{Kind::SkipToNext, {}}; }
    [[nodiscard]] static SkipStrategy skip_to_first(std::string name) {
        return SkipStrategy{Kind::SkipToFirst, std::move(name)};
    }
    [[nodiscard]] static SkipStrategy skip_to_last(std::string name) {
        return SkipStrategy{Kind::SkipToLast, std::move(name)};
    }
};

// Simple predicate over the user's event type. Stateless: sees only
// the candidate event.
template <typename T>
using Predicate = std::function<bool(const T&)>;

// PatternMatch view passed to iterative predicates and to user-side
// select / timed-out functions. Forward-declared here so Step can
// hold an iterative predicate that uses the type.
template <typename T>
using PatternMatch = std::unordered_map<std::string, std::vector<T>>;

// Iterative predicate: receives both the candidate event and the
// match-so-far. Useful for "next event > last matched" style rules.
template <typename T>
using IterativePredicate = std::function<bool(const T&, const PatternMatch<T>&)>;

// A single named step in the pattern. `incoming` is the edge from the
// previous step (Begin for the first step). `predicate` is run against
// each candidate event; missing predicates accept every event.
// `iterative` (if set) takes precedence over `predicate` and receives
// the partial-match-so-far. `min_count` / `max_count` carry the
// quantifier range; defaults (1, 1) mean exactly-one - the v1 shape.
template <typename T>
struct Step {
    std::string name;
    EdgeKind incoming{EdgeKind::Begin};
    Predicate<T> predicate;
    IterativePredicate<T> iterative;
    std::uint32_t min_count{1};
    std::uint32_t max_count{1};
    // When true, the quantifier advances as soon as min_count is met
    // - even if the current event would also satisfy this step. Default
    // greedy: keep capturing up to max_count before advancing. Lazy is
    // useful when you want the SHORTEST match (e.g. `.one_or_more().lazy()
    // .followed_by("b")` matches as soon as "b" is possible, instead of
    // greedily consuming every kind=loop event first).
    bool lazy{false};
};

// Pattern<T> is the fluent builder + compiled IR. Build via the static
// `begin(name)` factory; chain `where / next / followed_by / within`.
// Each chaining method returns a new Pattern by value (cheap; only a
// vector of Steps + a few scalars).
//
// The "current" step is always the last one in steps_. .where()
// attaches the predicate to that step. .next() / .followed_by() push
// a new step with the chosen edge kind. Calling .where() before any
// step exists is a programmer error and throws.
template <typename T>
class Pattern {
public:
    static Pattern<T> begin(std::string name) {
        Pattern<T> p;
        p.steps_.push_back(Step<T>{std::move(name), EdgeKind::Begin, Predicate<T>{}});
        return p;
    }

    Pattern<T>& where(Predicate<T> pred) {
        if (steps_.empty()) {
            throw std::logic_error("Pattern::where called before begin()");
        }
        steps_.back().predicate = std::move(pred);
        steps_.back().iterative = {};  // a simple predicate overrides any prior iterative
        return *this;
    }

    // Iterative predicate variant: receives both the candidate event
    // and the partial match (events captured so far, grouped by step
    // name). Either form of predicate may be set on a step - setting
    // one clears the other.
    Pattern<T>& where(IterativePredicate<T> pred) {
        if (steps_.empty()) {
            throw std::logic_error("Pattern::where called before begin()");
        }
        steps_.back().iterative = std::move(pred);
        steps_.back().predicate = {};
        return *this;
    }

    Pattern<T>& next(std::string name) {
        steps_.push_back(Step<T>{std::move(name), EdgeKind::Next});
        return *this;
    }

    Pattern<T>& followed_by(std::string name) {
        steps_.push_back(Step<T>{std::move(name), EdgeKind::FollowedBy});
        return *this;
    }

    // Group-pattern overloads: append every step from `sub` into this
    // pattern. The first step of `sub` inherits the chaining edge
    // (Next or FollowedBy) from the caller; subsequent steps keep
    // their own edges. Useful for composing reusable sub-patterns
    // ("session-start" → "ack" → "end" as a named macro).
    //
    // V1 limit: groups are flattened at construction, NOT preserved
    // as addressable units - `after_match_skip(skip_to_first(group))`
    // and group-level repetition (quantifiers on a sub-pattern as a
    // whole) are not supported. Apply quantifiers / skip targets to
    // individual step names within the sub-pattern instead.
    Pattern<T>& next(const Pattern<T>& sub) { return append_sub_(sub, EdgeKind::Next); }

    Pattern<T>& followed_by(const Pattern<T>& sub) {
        return append_sub_(sub, EdgeKind::FollowedBy);
    }

    // Negative step: the next event MUST NOT satisfy the predicate.
    // A matching event kills the partial; a non-matching event
    // advances past the negative step without capturing the event.
    Pattern<T>& not_next(std::string name) {
        steps_.push_back(Step<T>{std::move(name), EdgeKind::NotNext});
        return *this;
    }

    // Negative step: while waiting for the next positive step's
    // predicate to match, NO event may satisfy this step's predicate.
    // A matching event kills the partial. Implemented as a "shadow"
    // constraint over the FollowedBy zone that precedes the next
    // positive step.
    Pattern<T>& not_followed_by(std::string name) {
        steps_.push_back(Step<T>{std::move(name), EdgeKind::NotFollowedBy});
        return *this;
    }

    // Quantifier modifiers - apply to the current (last-added) step.
    // Defaults are (1, 1). Negative steps don't carry counts (they
    // don't capture events); applying these to a negative step throws.
    Pattern<T>& times(std::uint32_t n) { return times(n, n); }

    Pattern<T>& times(std::uint32_t min, std::uint32_t max) {
        check_quantifiable_("times");
        if (max == 0 || min > max) {
            throw std::logic_error("Pattern::times: require 1 <= min <= max (got min=" +
                                   std::to_string(min) + ", max=" + std::to_string(max) + ")");
        }
        steps_.back().min_count = min;
        steps_.back().max_count = max;
        return *this;
    }

    Pattern<T>& one_or_more() {
        check_quantifiable_("one_or_more");
        steps_.back().min_count = 1;
        steps_.back().max_count = std::numeric_limits<std::uint32_t>::max();
        return *this;
    }

    Pattern<T>& optional() {
        check_quantifiable_("optional");
        steps_.back().min_count = 0;
        steps_.back().max_count = 1;
        return *this;
    }

    // Lazy modifier on the current step's quantifier. Forces the
    // matcher to advance as soon as `min_count` is met - even if the
    // current event would also satisfy this step. Without `.lazy()`,
    // the quantifier is greedy (default documented behaviour). Throws if
    // applied to a step that doesn't have a meaningful quantifier
    // range (max == 1 and min == max).
    Pattern<T>& lazy() {
        check_quantifiable_("lazy");
        const auto& s = steps_.back();
        if (s.min_count == s.max_count && s.max_count == 1) {
            throw std::logic_error("Pattern::lazy: step '" + s.name +
                                   "' has no quantifier (min==max==1); call .times(), "
                                   ".one_or_more(), or .optional() first");
        }
        steps_.back().lazy = true;
        return *this;
    }

    // Bound on event-time span between the first matched event and
    // the last. A partial match whose start_event_time is older than
    // (current_watermark - within) is evicted on watermark advance.
    // Optional - without within(), partials live indefinitely.
    Pattern<T>& within(std::chrono::milliseconds w) {
        within_ = w;
        return *this;
    }

    // Configure what happens to OTHER in-flight partials when a match
    // completes (see SkipStrategy above). Default NoSkip - every
    // match is emitted independently.
    Pattern<T>& after_match_skip(SkipStrategy strategy) {
        skip_ = std::move(strategy);
        return *this;
    }

    [[nodiscard]] const std::vector<Step<T>>& steps() const noexcept { return steps_; }
    [[nodiscard]] std::optional<std::chrono::milliseconds> within_duration() const noexcept {
        return within_;
    }
    [[nodiscard]] const SkipStrategy& skip_strategy() const noexcept { return skip_; }

    // Validate that the pattern is well-formed. Called by the operator
    // at construction time so a malformed pattern surfaces during
    // pipeline build, not at first record. Returns the offending
    // condition as a string (empty = OK).
    [[nodiscard]] std::string validate() const {
        if (steps_.empty()) {
            return "pattern is empty (call begin(...))";
        }
        if (steps_.front().incoming != EdgeKind::Begin) {
            return "first step must have Begin edge kind";
        }
        if (is_negative_edge(steps_.front().incoming)) {
            return "first step cannot be negative (NotNext / NotFollowedBy)";
        }
        for (std::size_t i = 1; i < steps_.size(); ++i) {
            if (steps_[i].incoming == EdgeKind::Begin) {
                return "step '" + steps_[i].name + "' has Begin edge but is not first";
            }
        }
        // Negative steps don't carry counts.
        for (const auto& s : steps_) {
            if (is_negative_edge(s.incoming) && (s.min_count != 1 || s.max_count != 1)) {
                return "negative step '" + s.name + "' cannot carry a quantifier";
            }
        }
        // Trailing NotFollowedBy requires within() - the partial
        // can only complete (or fail) by watching the within-window
        // close; without a bound it would wait forever. Trailing
        // NotNext is fine: it consumes the very next event and is
        // either killed or completed by it.
        if (!steps_.empty() && steps_.back().incoming == EdgeKind::NotFollowedBy &&
            !within_.has_value()) {
            return "trailing not_followed_by step '" + steps_.back().name +
                   "' requires .within(duration) so the negative deadline is bounded";
        }
        // Duplicate names break the PatternMatch<T> output shape.
        for (std::size_t i = 0; i < steps_.size(); ++i) {
            for (std::size_t j = i + 1; j < steps_.size(); ++j) {
                if (steps_[i].name == steps_[j].name) {
                    return "duplicate step name '" + steps_[i].name + "'";
                }
            }
        }
        return {};
    }

private:
    // Append every step from `sub` to this pattern's chain. The
    // FIRST step of `sub` takes `head_edge` (overriding its own
    // Begin); subsequent steps keep their declared edges. The sub
    // pattern's `within_` and `skip_` are ignored - only the host
    // pattern's settings apply.
    Pattern<T>& append_sub_(const Pattern<T>& sub, EdgeKind head_edge) {
        if (sub.steps_.empty()) {
            throw std::logic_error("Pattern::append_sub_: sub-pattern is empty");
        }
        for (std::size_t i = 0; i < sub.steps_.size(); ++i) {
            Step<T> s = sub.steps_[i];
            if (i == 0) {
                s.incoming = head_edge;
            }
            steps_.push_back(std::move(s));
        }
        return *this;
    }

    void check_quantifiable_(const char* op) {
        if (steps_.empty()) {
            throw std::logic_error("Pattern::" + std::string{op} + " called before any step");
        }
        if (is_negative_edge(steps_.back().incoming)) {
            throw std::logic_error("Pattern::" + std::string{op} +
                                   " cannot apply to negative step '" + steps_.back().name + "'");
        }
    }

    std::vector<Step<T>> steps_;
    std::optional<std::chrono::milliseconds> within_;
    SkipStrategy skip_{SkipStrategy::no_skip()};
};

}  // namespace clink::cep
