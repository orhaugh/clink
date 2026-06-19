// Unit tests for Phase 28d pool primitives:
//   - RetryPolicy + retry() generic invoker.
//   - CircuitBreaker state machine.
//
// These two compose into Phase 28d's HttpPool (and future JdbcPool /
// RedisPool). Test in isolation here so the failure mode is the
// primitive, not the consumer.

#include <atomic>
#include <chrono>
#include <stdexcept>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "clink/async/circuit_breaker.hpp"
#include "clink/async/retry_policy.hpp"

using namespace clink::async;
using namespace std::chrono_literals;

// --- RetryPolicy ----------------------------------------------------

TEST(RetryPolicy, NoRetryByDefault) {
    RetryPolicy p;  // max_attempts = 1
    int attempts = 0;
    EXPECT_THROW(retry(p,
                       [&] {
                           ++attempts;
                           throw std::runtime_error("boom");
                           return 0;
                       }),
                 std::runtime_error);
    EXPECT_EQ(attempts, 1);
}

TEST(RetryPolicy, RetriesUpToMaxAttempts) {
    RetryPolicy p;
    p.max_attempts = 3;
    p.initial_backoff = 0ms;
    p.backoff_multiplier = 1.0;
    int attempts = 0;
    EXPECT_THROW(retry(p,
                       [&] {
                           ++attempts;
                           throw std::runtime_error("boom");
                           return 0;
                       }),
                 std::runtime_error);
    EXPECT_EQ(attempts, 3);
}

TEST(RetryPolicy, SucceedsOnLastAttempt) {
    RetryPolicy p;
    p.max_attempts = 3;
    p.initial_backoff = 0ms;
    int attempts = 0;
    int result = retry(p, [&]() -> int {
        ++attempts;
        if (attempts < 3) {
            throw std::runtime_error("transient");
        }
        return 42;
    });
    EXPECT_EQ(result, 42);
    EXPECT_EQ(attempts, 3);
}

TEST(RetryPolicy, ShouldRetryPredicateGatesRetries) {
    RetryPolicy p;
    p.max_attempts = 5;
    p.initial_backoff = 0ms;
    // Only retry on runtime_errors; logic_errors should surface
    // immediately even with attempts remaining.
    p.should_retry = [](const std::exception& e) {
        return dynamic_cast<const std::runtime_error*>(&e) != nullptr;
    };
    int attempts = 0;
    EXPECT_THROW(retry(p,
                       [&] {
                           ++attempts;
                           throw std::logic_error("non-transient");
                           return 0;
                       }),
                 std::logic_error);
    EXPECT_EQ(attempts, 1) << "non-retriable exception should not trigger retries";
}

TEST(RetryPolicy, BackoffComputationIsExponentialAndCapped) {
    RetryPolicy p;
    p.initial_backoff = 100ms;
    p.backoff_multiplier = 2.0;
    p.max_backoff = 1000ms;
    EXPECT_EQ(p.backoff_for_attempt(0).count(), 0);
    EXPECT_EQ(p.backoff_for_attempt(1).count(), 100);
    EXPECT_EQ(p.backoff_for_attempt(2).count(), 200);
    EXPECT_EQ(p.backoff_for_attempt(3).count(), 400);
    EXPECT_EQ(p.backoff_for_attempt(4).count(), 800);
    EXPECT_EQ(p.backoff_for_attempt(5).count(), 1000) << "should clamp at max_backoff";
    EXPECT_EQ(p.backoff_for_attempt(6).count(), 1000);
}

TEST(RetryPolicy, NonStdExceptionPropagatesWithoutRetry) {
    RetryPolicy p;
    p.max_attempts = 5;
    int attempts = 0;
    try {
        retry(p, [&]() -> int {
            ++attempts;
            throw 7;  // not a std::exception
        });
        FAIL() << "expected throw";
    } catch (int v) {
        EXPECT_EQ(v, 7);
    }
    EXPECT_EQ(attempts, 1) << "raw int throw should not retry";
}

TEST(RetryPolicy, ImmediateSuccessNoSleep) {
    RetryPolicy p;
    p.max_attempts = 10;
    p.initial_backoff = 100ms;  // would dominate if we did sleep
    int attempts = 0;
    const auto start = std::chrono::steady_clock::now();
    int result = retry(p, [&]() -> int {
        ++attempts;
        return 99;
    });
    const auto elapsed = std::chrono::steady_clock::now() - start;
    EXPECT_EQ(result, 99);
    EXPECT_EQ(attempts, 1);
    EXPECT_LT(elapsed, 50ms) << "first-attempt success must not sleep";
}

// --- CircuitBreaker -------------------------------------------------

TEST(CircuitBreaker, StartsClosed) {
    CircuitBreaker cb({.failure_threshold = 3, .cooldown = 100ms});
    EXPECT_EQ(cb.state(), CircuitState::Closed);
    EXPECT_TRUE(cb.allow_call());
    cb.record_success();
    EXPECT_EQ(cb.state(), CircuitState::Closed);
}

TEST(CircuitBreaker, TripsToOpenAfterThresholdFailures) {
    CircuitBreaker cb({.failure_threshold = 3, .cooldown = 100ms});
    for (int i = 0; i < 2; ++i) {
        EXPECT_TRUE(cb.allow_call());
        cb.record_failure();
        EXPECT_EQ(cb.state(), CircuitState::Closed);
    }
    EXPECT_TRUE(cb.allow_call());
    cb.record_failure();
    EXPECT_EQ(cb.state(), CircuitState::Open);
    EXPECT_FALSE(cb.allow_call()) << "OPEN breaker should fast-fail";
}

TEST(CircuitBreaker, SuccessResetsConsecutiveFailures) {
    CircuitBreaker cb({.failure_threshold = 3, .cooldown = 100ms});
    cb.record_failure();
    cb.record_failure();
    EXPECT_EQ(cb.consecutive_failures(), 2);
    cb.record_success();
    EXPECT_EQ(cb.consecutive_failures(), 0);
    // Next two failures don't trip - we're back to 0.
    cb.record_failure();
    cb.record_failure();
    EXPECT_EQ(cb.state(), CircuitState::Closed);
}

TEST(CircuitBreaker, TransitionsToHalfOpenAfterCooldown) {
    CircuitBreaker cb({.failure_threshold = 1, .cooldown = 20ms});
    cb.allow_call();
    cb.record_failure();
    EXPECT_EQ(cb.state(), CircuitState::Open);
    std::this_thread::sleep_for(25ms);
    // First allow_call after cooldown drives the OPEN -> HALF_OPEN
    // transition and grants the probe slot.
    EXPECT_TRUE(cb.allow_call());
    EXPECT_EQ(cb.state(), CircuitState::HalfOpen);
}

TEST(CircuitBreaker, HalfOpenProbeSuccessClosesBreaker) {
    CircuitBreaker cb({.failure_threshold = 1, .cooldown = 5ms});
    cb.allow_call();
    cb.record_failure();
    std::this_thread::sleep_for(10ms);
    EXPECT_TRUE(cb.allow_call());  // transitions to HalfOpen and takes probe
    cb.record_success();
    EXPECT_EQ(cb.state(), CircuitState::Closed);
    EXPECT_TRUE(cb.allow_call());  // now closed; ordinary call
}

TEST(CircuitBreaker, HalfOpenProbeFailureReopens) {
    CircuitBreaker cb({.failure_threshold = 1, .cooldown = 5ms});
    cb.allow_call();
    cb.record_failure();
    std::this_thread::sleep_for(10ms);
    EXPECT_TRUE(cb.allow_call());  // HalfOpen probe
    cb.record_failure();
    EXPECT_EQ(cb.state(), CircuitState::Open);
    EXPECT_FALSE(cb.allow_call()) << "post-probe-failure breaker is OPEN again";
}

TEST(CircuitBreaker, HalfOpenProbeIsExclusive) {
    CircuitBreaker cb({.failure_threshold = 1, .cooldown = 5ms});
    cb.allow_call();
    cb.record_failure();
    std::this_thread::sleep_for(10ms);
    EXPECT_TRUE(cb.allow_call());  // first probe taken
    EXPECT_FALSE(cb.allow_call()) << "concurrent probe attempts must be rejected";
    // Resolution unblocks the breaker.
    cb.record_success();
    EXPECT_EQ(cb.state(), CircuitState::Closed);
}
