// Async primitive metrics: circuit breaker + retry policy.

#include <chrono>
#include <stdexcept>
#include <string>

#include <gtest/gtest.h>

#include "clink/async/circuit_breaker.hpp"
#include "clink/async/retry_policy.hpp"
#include "clink/metrics/async_metrics.hpp"
#include "clink/metrics/metrics_registry.hpp"

using namespace clink;

namespace {

std::uint64_t counter_value(const std::string& name) {
    auto snap = MetricsRegistry::global().snapshot();
    for (const auto& [n, v] : snap.counters) {
        if (n == name) {
            return v;
        }
    }
    return 0;
}

// io_uring completion latency is a histogram now (OBS-1b).
double hist_sum(const std::string& base) {
    return MetricsRegistry::global().histogram(base).snapshot().sum;
}

}  // namespace

TEST(AsyncMetrics, CircuitBreakerTripsAndTransitions) {
    async::CircuitBreakerConfig cfg;
    cfg.failure_threshold = 2;
    cfg.cooldown = std::chrono::milliseconds{50};
    async::CircuitBreaker cb(cfg);

    const auto trips_before = counter_value(metrics::kCbTrips);
    const auto trans_before =
        counter_value(metrics::circuit_state_transition_name("Closed", "Open"));
    const auto allowed_before = counter_value(metrics::kCbAllowed);
    const auto rejected_before = counter_value(metrics::kCbRejected);

    EXPECT_TRUE(cb.allow_call());
    cb.record_failure();
    EXPECT_TRUE(cb.allow_call());
    cb.record_failure();            // 2nd consecutive -> trip
    EXPECT_FALSE(cb.allow_call());  // OPEN

    EXPECT_EQ(counter_value(metrics::kCbTrips) - trips_before, 1u);
    EXPECT_EQ(
        counter_value(metrics::circuit_state_transition_name("Closed", "Open")) - trans_before, 1u);
    EXPECT_GE(counter_value(metrics::kCbAllowed) - allowed_before, 2u);
    EXPECT_GE(counter_value(metrics::kCbRejected) - rejected_before, 1u);
}

TEST(AsyncMetrics, RetryAttemptsAndSuccessAccumulate) {
    async::RetryPolicy p;
    p.max_attempts = 3;
    p.initial_backoff = std::chrono::milliseconds{1};

    const auto attempts_before = counter_value(metrics::kRetryAttempts);
    const auto success_before = counter_value(metrics::kRetrySuccess);

    int calls = 0;
    auto result = async::retry(p, [&calls] {
        ++calls;
        if (calls < 3) {
            throw std::runtime_error("transient");
        }
        return 42;
    });
    EXPECT_EQ(result, 42);
    EXPECT_EQ(counter_value(metrics::kRetryAttempts) - attempts_before, 2u);
    EXPECT_EQ(counter_value(metrics::kRetrySuccess) - success_before, 1u);
}

TEST(AsyncMetrics, RetryGiveupOnPermanentFailure) {
    async::RetryPolicy p;
    p.max_attempts = 2;
    p.initial_backoff = std::chrono::milliseconds{1};

    const auto giveup_before = counter_value(metrics::kRetryGiveup);

    EXPECT_THROW(async::retry(p,
                              [] {
                                  throw std::runtime_error("perma");
                                  return 0;
                              }),
                 std::runtime_error);
    EXPECT_EQ(counter_value(metrics::kRetryGiveup) - giveup_before, 1u);
}

TEST(AsyncMetrics, IoUringAndHttpPoolHelpers) {
    const auto comp_before = counter_value(metrics::kIoUringCompletions);
    const auto block_before = counter_value(metrics::kHttpPoolAcquireBlocks);

    metrics::async::io_uring_completion(2000);
    metrics::async::io_uring_completion(3000);
    metrics::async::http_pool_in_use_set(5);
    metrics::async::http_pool_idle_set(3);
    metrics::async::http_pool_acquire_blocked();

    EXPECT_EQ(counter_value(metrics::kIoUringCompletions) - comp_before, 2u);
    EXPECT_GE(hist_sum(metrics::kIoUringLatNs), 5000.0);
    EXPECT_EQ(counter_value(metrics::kHttpPoolAcquireBlocks) - block_before, 1u);
    auto snap = MetricsRegistry::global().snapshot();
    std::int64_t in_use = 0;
    std::int64_t idle = 0;
    for (const auto& [n, v] : snap.gauges) {
        if (n == metrics::kHttpPoolConnections) {
            in_use = v;
        } else if (n == metrics::kHttpPoolIdleConnections) {
            idle = v;
        }
    }
    EXPECT_EQ(in_use, 5);
    EXPECT_EQ(idle, 3);
}
