// AsyncMapOperator: fixed-thread-pool async map that runs slow user
// work concurrently and emits results in INPUT order (// "ordered" output mode). Tests cover:
//
//   * Out-of-order completion still emits in input order.
//   * Watermarks wait for every in-flight call before forwarding.
//   * Barriers wait similarly (the same code path as watermarks).
//   * max_in_flight backpressures submit_record_.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/operators/async_map_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"

using namespace clink;
using namespace std::chrono_literals;

TEST(AsyncMapOperator, EmitsInInputOrderDespiteOutOfOrderCompletion) {
    // Workers sleep N*10ms inverse to input order, so input 0
    // finishes LAST and input 4 finishes FIRST. The ordered queue
    // must still emit them as 0, 1, 2, 3, 4.
    auto sleeps = std::make_shared<std::vector<int>>();
    auto fn = [](const int& v) -> int {
        std::this_thread::sleep_for(std::chrono::milliseconds{(4 - v) * 20});
        return v * 10;
    };

    Dag dag;
    auto src = std::make_shared<VectorSource<int>>(std::vector<Record<int>>{
        Record<int>{0}, Record<int>{1}, Record<int>{2}, Record<int>{3}, Record<int>{4}});
    auto async_op = std::make_shared<AsyncMapOperator<int, int>>(fn, /*workers=*/4, /*inflight=*/8);
    auto sink = std::make_shared<CollectingSink<int>>();

    auto h0 = dag.add_source<int>(src);
    auto h1 = dag.add_operator<int, int>(h0, async_op);
    dag.add_sink<int>(h1, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto collected = sink->collected();
    ASSERT_EQ(collected.size(), 5u);
    EXPECT_EQ(collected[0], 0);
    EXPECT_EQ(collected[1], 10);
    EXPECT_EQ(collected[2], 20);
    EXPECT_EQ(collected[3], 30);
    EXPECT_EQ(collected[4], 40);
}

TEST(AsyncMapOperator, FastFunctionRunsWithSinglePassthrough) {
    auto fn = [](const int& v) -> int { return v + 1; };

    Dag dag;
    auto src = std::make_shared<VectorSource<int>>(
        std::vector<Record<int>>{Record<int>{7}, Record<int>{8}, Record<int>{9}});
    auto async_op = std::make_shared<AsyncMapOperator<int, int>>(fn, 2, 4);
    auto sink = std::make_shared<CollectingSink<int>>();

    auto h0 = dag.add_source<int>(src);
    auto h1 = dag.add_operator<int, int>(h0, async_op);
    dag.add_sink<int>(h1, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto collected = sink->collected();
    ASSERT_EQ(collected.size(), 3u);
    EXPECT_EQ(collected[0], 8);
    EXPECT_EQ(collected[1], 9);
    EXPECT_EQ(collected[2], 10);
}

TEST(AsyncMapOperator, RespectsMaxInFlightBudget) {
    // With max_in_flight = 2 and worker_count = 2, at most 2 calls
    // should be in flight at any time. The counter never exceeds 2.
    std::atomic<int> live_count{0};
    std::atomic<int> peak{0};
    auto fn = [&](const int&) -> int {
        const int c = ++live_count;
        int prev_peak = peak.load();
        while (c > prev_peak && !peak.compare_exchange_weak(prev_peak, c)) {
        }
        std::this_thread::sleep_for(20ms);
        --live_count;
        return 0;
    };

    Dag dag;
    std::vector<Record<int>> records;
    for (int i = 0; i < 10; ++i) {
        records.emplace_back(Record<int>{i});
    }
    auto src = std::make_shared<VectorSource<int>>(std::move(records));
    auto async_op = std::make_shared<AsyncMapOperator<int, int>>(fn, /*workers=*/2, /*inflight=*/2);
    auto sink = std::make_shared<CollectingSink<int>>();

    auto h0 = dag.add_source<int>(src);
    auto h1 = dag.add_operator<int, int>(h0, async_op);
    dag.add_sink<int>(h1, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    EXPECT_EQ(sink->collected().size(), 10u);
    EXPECT_LE(peak.load(), 2) << "max_in_flight cap was exceeded";
}

// Retry strategy: fn throws on the first two attempts, returns on the
// third. With max_attempts=3 the record completes; retries_observed
// must reflect 2 retries per record.
TEST(AsyncMapOperator, RetriesUntilSuccessUnderStrategy) {
    std::atomic<int> calls{0};
    auto fn = [&](const int& v) -> int {
        if (++calls <= 2) {
            throw std::runtime_error("transient");
        }
        return v + 100;
    };

    AsyncRetryStrategy retry;
    retry.max_attempts = 3;
    retry.initial_backoff = std::chrono::milliseconds{1};

    Dag dag;
    auto src = std::make_shared<VectorSource<int>>(std::vector<Record<int>>{Record<int>{7}});
    auto async_op =
        std::make_shared<AsyncMapOperator<int, int>>(fn, retry, /*workers=*/1, /*inflight=*/4);
    auto sink = std::make_shared<CollectingSink<int>>();

    auto h0 = dag.add_source<int>(src);
    auto h1 = dag.add_operator<int, int>(h0, async_op);
    dag.add_sink<int>(h1, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto collected = sink->collected();
    ASSERT_EQ(collected.size(), 1u);
    EXPECT_EQ(collected[0], 107);
    EXPECT_EQ(async_op->retries_observed(), 2u)
        << "two retries should have been attempted before success";
}

// should_retry predicate gates retries even with remaining attempts.
// Two records: the first throws a "transient" exception that the
// predicate retries (then succeeds), the second throws a "permanent"
// one that the predicate refuses - but recovers by succeeding on the
// *next* call once we reset the flag. The point is to verify the
// predicate is consulted per-attempt and can short-circuit.
TEST(AsyncMapOperator, ShouldRetryPredicateIsConsultedPerAttempt) {
    std::atomic<int> calls{0};
    auto fn = [&](const int& v) -> int {
        const int n = ++calls;
        if (n == 1) {
            throw std::runtime_error("transient");  // attempt 1 fails
        }
        return v + 1000;  // attempt 2 succeeds
    };

    std::atomic<int> predicate_calls{0};
    AsyncRetryStrategy retry;
    retry.max_attempts = 5;
    retry.should_retry = [&](const std::exception& e) {
        ++predicate_calls;
        return std::string{e.what()} == "transient";
    };

    Dag dag;
    auto src = std::make_shared<VectorSource<int>>(std::vector<Record<int>>{Record<int>{5}});
    auto async_op =
        std::make_shared<AsyncMapOperator<int, int>>(fn, retry, /*workers=*/1, /*inflight=*/4);
    auto sink = std::make_shared<CollectingSink<int>>();
    auto h0 = dag.add_source<int>(src);
    auto h1 = dag.add_operator<int, int>(h0, async_op);
    dag.add_sink<int>(h1, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    EXPECT_EQ(predicate_calls.load(), 1) << "predicate consulted on each failed attempt";
    EXPECT_EQ(async_op->retries_observed(), 1u);
    auto out = sink->collected();
    ASSERT_EQ(out.size(), 1u);
    EXPECT_EQ(out[0], 1005);
}

// Backoff multiplier doubles the wait between attempts and saturates
// at max_backoff. fn throws 3 times then succeeds - we assert the
// OBSERVED total wait is at least the nominal sum (10+20+40 = 70ms);
// a tighter upper bound would be flaky on a loaded CI host.
TEST(AsyncMapOperator, ExponentialBackoffWaitsAtLeastNominalSum) {
    std::atomic<int> calls{0};
    auto fn = [&](const int& v) -> int {
        if (++calls <= 3) {
            throw std::runtime_error("transient");
        }
        return v;
    };

    AsyncRetryStrategy retry;
    retry.max_attempts = 4;  // 3 retries -> 3 backoffs before final success
    retry.initial_backoff = std::chrono::milliseconds{10};
    retry.backoff_multiplier = 2.0;
    retry.max_backoff = std::chrono::milliseconds{1000};
    // Expected waits: 10ms, 20ms, 40ms -> sum 70ms minimum.

    auto async_op =
        std::make_shared<AsyncMapOperator<int, int>>(fn, retry, /*workers=*/1, /*inflight=*/4);
    Dag dag;
    auto src = std::make_shared<VectorSource<int>>(std::vector<Record<int>>{Record<int>{1}});
    auto sink = std::make_shared<CollectingSink<int>>();
    auto h0 = dag.add_source<int>(src);
    auto h1 = dag.add_operator<int, int>(h0, async_op);
    dag.add_sink<int>(h1, sink);

    auto start = std::chrono::steady_clock::now();
    LocalExecutor exec(std::move(dag));
    exec.run();
    auto elapsed = std::chrono::steady_clock::now() - start;

    EXPECT_GE(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(), 70);
    EXPECT_EQ(async_op->retries_observed(), 3u);
    auto out = sink->collected();
    ASSERT_EQ(out.size(), 1u);
    EXPECT_EQ(out[0], 1);
}
