#pragma once

// clink::test assertion utilities - framework-neutral checks over an
// OutputCapture. Each returns a CheckResult carrying pass/fail plus a
// human-readable diagnostic, so they compose with any test framework:
//
//   auto r = clink::test::values_are(h.output(), {1, 2, 3});
//   EXPECT_TRUE(r) << r.message;          // gtest
//   REQUIRE(r.ok);                        // or anything else
//
// Part of the public clink testing API (docs/internals/testing-framework.md).

#include <cstddef>
#include <list>
#include <ostream>
#include <sstream>
#include <string>
#include <vector>

#include "clink/test/output_capture.hpp"

namespace clink::test {

struct CheckResult {
    bool ok{true};
    std::string message;

    explicit operator bool() const noexcept { return ok; }

    static CheckResult success() { return {}; }
    static CheckResult failure(std::string msg) { return {false, std::move(msg)}; }

    friend std::ostream& operator<<(std::ostream& os, const CheckResult& r) {
        return os << (r.ok ? "ok" : r.message);
    }
};

namespace detail {

template <typename T>
std::string print_value(const T& v) {
    if constexpr (requires(std::ostream& os, const T& x) { os << x; }) {
        std::ostringstream os;
        os << v;
        return os.str();
    } else {
        return "<value>";
    }
}

template <typename T>
std::string print_values(const std::vector<T>& vs) {
    std::string out{"["};
    for (std::size_t i = 0; i < vs.size(); ++i) {
        if (i != 0) {
            out += ", ";
        }
        out += print_value(vs[i]);
    }
    return out + "]";
}

}  // namespace detail

// Exact values in exact emission order.
template <typename T>
CheckResult values_are(const OutputCapture<T>& capture, const std::vector<T>& expected) {
    const auto actual = capture.values();
    if (actual == expected) {
        return CheckResult::success();
    }
    return CheckResult::failure("expected values " + detail::print_values(expected) + " but got " +
                                detail::print_values(actual));
}

// Same multiset of values, any order (needs operator== only).
template <typename T>
CheckResult values_are_unordered(const OutputCapture<T>& capture, const std::vector<T>& expected) {
    const auto actual = capture.values();
    std::list<T> remaining(expected.begin(), expected.end());
    for (const auto& v : actual) {
        bool found = false;
        for (auto it = remaining.begin(); it != remaining.end(); ++it) {
            if (*it == v) {
                remaining.erase(it);
                found = true;
                break;
            }
        }
        if (!found) {
            return CheckResult::failure(
                "unexpected value " + detail::print_value(v) + " (expected some order of " +
                detail::print_values(expected) + ", got " + detail::print_values(actual) + ")");
        }
    }
    if (!remaining.empty()) {
        return CheckResult::failure(
            "missing " + std::to_string(remaining.size()) + " expected value(s), first: " +
            detail::print_value(remaining.front()) + " (got " + detail::print_values(actual) + ")");
    }
    return CheckResult::success();
}

template <typename T>
CheckResult contains_value(const OutputCapture<T>& capture, const T& expected) {
    if (capture.any_value([&](const T& v) { return v == expected; })) {
        return CheckResult::success();
    }
    return CheckResult::failure("value " + detail::print_value(expected) + " not found in " +
                                detail::print_values(capture.values()));
}

// Emitted watermarks never move backwards (ignores idle markers).
template <typename T>
CheckResult watermarks_are_monotonic(const OutputCapture<T>& capture) {
    bool have_prev = false;
    std::int64_t prev = 0;
    for (const auto& wm : capture.watermarks()) {
        if (wm.is_idle()) {
            continue;
        }
        const auto ts = wm.timestamp().millis();
        if (have_prev && ts < prev) {
            return CheckResult::failure("watermark regressed from " + std::to_string(prev) +
                                        "ms to " + std::to_string(ts) + "ms");
        }
        prev = ts;
        have_prev = true;
    }
    return CheckResult::success();
}

}  // namespace clink::test
