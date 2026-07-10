#pragma once

// clink::test failure injection - deterministic, explicit, observable.
// The harness mediates every call into the operator, so failures are
// injected AT those call sites; production code needs no counters or
// hooks. An injected failure throws InjectedFailure, which propagates
// to the test exactly like an operator exception would.
//
//   h.failures().fail_once(FailurePoint::AfterProcessElement);
//   EXPECT_THROW(h.process_element(x), clink::test::InjectedFailure);
//   EXPECT_EQ(h.failures().injected_count(), 1);
//
// Part of the public clink testing API (docs/internals/testing-framework.md).

#include <cstdint>
#include <functional>
#include <stdexcept>
#include <string>
#include <vector>

namespace clink::test {

class InjectedFailure : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

enum class FailurePoint {
    BeforeProcessElement,
    AfterProcessElement,
    OnEventTimeTimer,       // before the operator's event-time timer callback
    OnProcessingTimeTimer,  // before the operator's processing-time timer callback
    DuringSnapshot,         // before the state/timers are captured
};

inline const char* to_string(FailurePoint p) {
    switch (p) {
        case FailurePoint::BeforeProcessElement:
            return "BeforeProcessElement";
        case FailurePoint::AfterProcessElement:
            return "AfterProcessElement";
        case FailurePoint::OnEventTimeTimer:
            return "OnEventTimeTimer";
        case FailurePoint::OnProcessingTimeTimer:
            return "OnProcessingTimeTimer";
        case FailurePoint::DuringSnapshot:
            return "DuringSnapshot";
    }
    return "?";
}

// The armed failure set. Deterministic: a rule either fires at its
// call site or it does not - nothing depends on timing.
class FailurePlan {
public:
    // Fail the next time `point` is reached, once.
    void fail_once(FailurePoint point) { rules_.push_back(Rule{point, 1, 0, {}}); }

    // Fail every time `point` is reached while `when(hit_index)` holds
    // (hit_index counts occurrences of the point, starting at 1).
    void fail_when(FailurePoint point, std::function<bool(std::uint64_t)> when) {
        rules_.push_back(Rule{point, 0, 0, std::move(when)});
    }

    // Fail once, the nth time `point` is reached (n starts at 1).
    void fail_on_nth(FailurePoint point, std::uint64_t n) {
        rules_.push_back(Rule{point, 1, 0, [n](std::uint64_t hit) { return hit == n; }});
    }

    std::uint64_t injected_count() const noexcept { return injected_; }
    void clear() noexcept { rules_.clear(); }

    // Called by the harness at each mediated point. Throws when armed.
    void check(FailurePoint point) {
        const auto hit = ++hits_[static_cast<std::size_t>(point)];
        for (auto& r : rules_) {
            if (r.point != point) {
                continue;
            }
            if (r.remaining_once != 0 && r.fired >= r.remaining_once) {
                continue;  // exhausted one-shot
            }
            if (r.when && !r.when(hit)) {
                continue;
            }
            ++r.fired;
            ++injected_;
            throw InjectedFailure(std::string{"injected failure at "} + to_string(point) +
                                  " (occurrence " + std::to_string(hit) + ")");
        }
    }

private:
    struct Rule {
        FailurePoint point;
        std::uint64_t remaining_once;  // 0 = unlimited (predicate-gated)
        std::uint64_t fired;
        std::function<bool(std::uint64_t)> when;
    };
    std::vector<Rule> rules_;
    std::uint64_t hits_[8]{};
    std::uint64_t injected_{0};
};

}  // namespace clink::test
