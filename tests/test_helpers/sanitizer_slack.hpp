#pragma once

// Sanitizer-aware timing slack for tests.
//
// AddressSanitizer / ThreadSanitizer instrumentation slows the
// process by 5-10x. Tests with tight timeouts, quiescence thresholds,
// or throughput floors that pass cleanly on a normal build can flake
// under sanitizers - not because the code is wrong, but because the
// test's wall-clock budget was sized for the production runtime.
//
// This header exposes a single compile-time multiplier and a couple
// of convenience constants derived from it. Tests pick them up at
// build time; no runtime detection / env-var dance.
//
// Detection covers both clang feature-test (__has_feature) and the
// libstdc++ / GCC-style preprocessor macros for the same sanitizers.

#include <chrono>

namespace clink::test_support {

// Returns true iff THIS translation unit was compiled with one of the
// "slow" sanitizers (ASan or TSan). UBSan is excluded - its overhead
// is negligible enough that production thresholds still hold.
[[nodiscard]] constexpr bool under_slow_sanitizer() {
#if defined(__has_feature)
#  if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer)
    return true;
#  endif
#endif
#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
    return true;
#endif
    return false;
}

[[nodiscard]] constexpr bool under_thread_sanitizer() {
#if defined(__has_feature)
#  if __has_feature(thread_sanitizer)
    return true;
#  endif
#endif
#if defined(__SANITIZE_THREAD__)
    return true;
#endif
    return false;
}

// Multiplier to scale any wall-clock budget by. 1x in normal builds,
// 10x under ASan/TSan. Pick this when you have a "wait up to N ms for
// X to happen" deadline; without scaling, sanitizer slowdown
// trips the deadline before the code under test has had a chance.
constexpr int kTimingSlack = under_slow_sanitizer() ? 10 : 1;

// Quiescence-poll threshold for tests using Dag::iterate_stream that
// depend on the head's "external closed AND both channels empty for N
// consecutive idle polls" termination. The production default is 100
// (~100ms with the head's 1ms poll cycle). Sanitizer slowdown can let
// records still be in flight after 100 ms; 1000 (~1s) leaves enough
// margin even under heavy instrumentation.
constexpr int kIterationIdleThreshold = under_slow_sanitizer() ? 1000 : 100;

// Helper: scale a chrono duration by kTimingSlack. Use when computing
// deadlines: `auto deadline = now + scale_slack(500ms);`
template <typename Rep, typename Period>
[[nodiscard]] constexpr std::chrono::duration<Rep, Period>
scale_slack(std::chrono::duration<Rep, Period> d) {
    return d * kTimingSlack;
}

}  // namespace clink::test_support
