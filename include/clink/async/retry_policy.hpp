#pragma once

// Retry policy for async lookup pipelines.
//
// A small utility that wraps a user callable in a retry-with-backoff
// loop. Mirrors the shape of AsyncRetryStrategy
// (operators/async_map_operator.hpp) but is *generic* and *outside*
// the operator: the same policy is what HttpPool and
// AsyncLookupOperator (eventually) use to absorb transient
// upstream failures.
//
// Design choices:
// - Header-only, no allocations beyond what the user fn itself does.
// - Caller-driven sleep: apply() invokes std::this_thread::sleep_for
//   directly on the calling thread. With the coroutine / io_uring
//   backend, an async variant returning Task<T> will replace the
//   sleep with `co_await sleep_for(...)`.
// - should_retry callback decides which exceptions are transient.
//   Default: retry on every std::exception subclass; surface
//   anything else immediately.

#include <algorithm>
#include <chrono>
#include <cmath>
#include <exception>
#include <functional>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>

#include "clink/metrics/async_metrics.hpp"

namespace clink::async {

struct RetryPolicy {
    // Total number of attempts including the first. 1 = no retry.
    int max_attempts{1};
    // Wait this long before the second attempt (before the first
    // retry). Subsequent waits multiply by backoff_multiplier.
    std::chrono::milliseconds initial_backoff{50};
    // 1.0 = fixed delay; 2.0 = exponential doubling.
    double backoff_multiplier{2.0};
    // Cap on any single backoff after multiplication.
    std::chrono::milliseconds max_backoff{std::chrono::milliseconds{30000}};
    // Predicate. nullptr = retry on every std::exception. Return
    // true to retry, false to surface the exception immediately even
    // if attempts remain.
    std::function<bool(const std::exception&)> should_retry{nullptr};

    // Compute the delay for the i-th retry (0-indexed: the wait
    // before attempt index 1). Public so tests can verify
    // expected backoff values without driving the loop.
    [[nodiscard]] std::chrono::milliseconds backoff_for_attempt(int retry_idx) const noexcept {
        if (retry_idx <= 0)
            return std::chrono::milliseconds{0};
        const double scale = std::pow(backoff_multiplier, retry_idx - 1);
        const auto raw_ms =
            static_cast<long long>(static_cast<double>(initial_backoff.count()) * scale);
        const auto capped =
            std::min<long long>(raw_ms, static_cast<long long>(max_backoff.count()));
        return std::chrono::milliseconds{capped};
    }
};

// Invoke `fn` according to `policy`. On each std::exception thrown,
// consult should_retry; if it returns true and attempts remain, sleep
// for the appropriate backoff and retry. If attempts exhaust or
// should_retry returns false, rethrow.
//
// Non-std::exception throws (raw int, etc.) propagate without retry.
template <typename Fn>
auto retry(const RetryPolicy& policy, Fn&& fn) -> std::invoke_result_t<Fn> {
    const int max = std::max(1, policy.max_attempts);
    for (int attempt = 1; attempt <= max; ++attempt) {
        try {
            if constexpr (std::is_void_v<std::invoke_result_t<Fn>>) {
                fn();
                if (attempt > 1) {
                    clink::metrics::async::retry_succeeded();
                }
                return;
            } else {
                auto result = fn();
                if (attempt > 1) {
                    clink::metrics::async::retry_succeeded();
                }
                return result;
            }
        } catch (const std::exception& e) {
            if (attempt >= max) {
                clink::metrics::async::retry_gave_up();
                throw;
            }
            if (policy.should_retry && !policy.should_retry(e)) {
                throw;
            }
            clink::metrics::async::retry_attempted();
            std::this_thread::sleep_for(policy.backoff_for_attempt(attempt));
        }
        // Non-std::exception throws fall through the catch and
        // propagate naturally - no retry.
    }
    // Unreachable: the loop either returns or throws.
    throw std::logic_error("clink::async::retry: control reached end of loop");
}

}  // namespace clink::async
