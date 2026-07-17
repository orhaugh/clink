// Regression-floor smoke test for in-process pipeline throughput.
//
// Runs the same VectorSource -> 2x MapOperator -> FunctionSink pipeline
// that `clink_bench --scenario=pipe_throughput` measures, but at a
// small record count so it fits in CI runtime, and asserts throughput
// stays above a generous floor. The floor catches gross regressions
// (e.g. accidental O(n²) routing, lost-batch fallback paths) without
// being tight enough to flake on slow runners or sanitizer builds.
//
// If THIS test fails, something significantly slowed the operator hot
// path; treat as a perf regression and investigate before merging.
//
// Why 100 k records / >= 1 M r/s floor:
//   * The bench binary measures ~10-25 M r/s on commodity hardware
//     for this scenario, so 1 M r/s is ~10-25x slack.
//   * 100k records keeps wall time < 100 ms on a fast box, < 1 s on
//     slow/sanitizer builds - under the gtest default per-test budget.
//   * Sanitizer builds (asan/tsan) are typically 3-10x slower; the
//     1 M r/s floor still clears that margin.

#include <chrono>
#include <cstdint>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "clink/operators/map_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"

#include "test_helpers/sanitizer_slack.hpp"

namespace {

using namespace clink;

TEST(PipeThroughputFloor, InProcessPipelineStaysAboveOneMillionRecordsPerSecond) {
    // ThreadSanitizer instruments every load/store with a race-tracking
    // shadow-memory update, which gives a 5–10x throughput hit on the
    // operator hot path. 1 M records/sec under TSan is physically
    // unreachable on commodity hardware - the test would be asserting
    // an instrumentation cost, not a code regression. Skip under TSan
    // and rely on the normal-build + ASan + UBSan paths to catch any
    // real regressions.
    if (clink::test_support::under_thread_sanitizer()) {
        GTEST_SKIP() << "throughput floor unmeasurable under TSan instrumentation";
    }
#ifndef NDEBUG
    // A throughput floor asserts an optimized-code property. An unoptimized
    // (Debug / -O0) build runs the operator hot path several times slower, so
    // the floor would be measuring the lack of optimization, not a regression.
    // The CI gate builds Debug; perf floors belong to Release runs.
    GTEST_SKIP() << "throughput floor unmeasurable in an unoptimized (Debug) build";
#endif
    constexpr std::int64_t kRecords = 100'000;

    std::vector<Record<std::int64_t>> input;
    input.reserve(static_cast<std::size_t>(kRecords));
    for (std::int64_t i = 0; i < kRecords; ++i) {
        input.emplace_back(Record<std::int64_t>{i});
    }

    Dag dag;
    auto src = std::make_shared<VectorSource<std::int64_t>>(std::move(input));
    auto map1 = std::make_shared<MapOperator<std::int64_t, std::int64_t>>(
        [](const std::int64_t& v) { return v * 2; }, "map_x2");
    auto map2 = std::make_shared<MapOperator<std::int64_t, std::int64_t>>(
        [](const std::int64_t& v) { return v + 1; }, "map_plus1");

    std::int64_t consumed = 0;
    auto sink = std::make_shared<FunctionSink<std::int64_t>>(
        [&consumed](const std::int64_t&) { ++consumed; });

    auto h0 = dag.add_source<std::int64_t>(src);
    auto h1 = dag.add_operator<std::int64_t, std::int64_t>(h0, map1);
    auto h2 = dag.add_operator<std::int64_t, std::int64_t>(h1, map2);
    dag.add_sink<std::int64_t>(h2, sink);

    const auto t0 = std::chrono::steady_clock::now();
    LocalExecutor exec(std::move(dag));
    exec.run();
    const auto t1 = std::chrono::steady_clock::now();

    ASSERT_EQ(consumed, kRecords) << "sink dropped records";

    const auto wall_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
    const double seconds = static_cast<double>(wall_ns) / 1'000'000'000.0;
    const double rps = seconds > 0 ? static_cast<double>(kRecords) / seconds : 0;

    // Generous floor: ~10-25x slack vs. measured baseline on the dev
    // hardware. If a change drops below this, we want to see it.
    constexpr double kFloor = 1'000'000.0;
    EXPECT_GT(rps, kFloor) << "throughput regression: " << rps << " records/sec < floor " << kFloor
                           << " (wall_ms = " << static_cast<double>(wall_ns) / 1e6 << ")";
}

}  // namespace
