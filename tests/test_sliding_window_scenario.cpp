// Realistic sliding-window scenario: 1-hour window sliding every 1
// minute, fed 100 events/min of synthetic activity for 70 minutes.
// Equivalent of 's:
//
//   stream
//     .keyBy(...)
//     .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(1)))
//     .aggregate(SumAggregator())
//
// Asserts:
//   * Every minute boundary produces exactly one window emission per
//     key (after enough watermark progress).
//   * Each emission's aggregate equals the sum of values whose
//     event-times fall in the closed-open 60-minute window ending at
//     that minute.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/operators/sink_operator.hpp"
#include "clink/operators/sliding_window_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/time/watermark_strategy.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

// Per-record event injected into the pipeline. Single key for now (the
// "click count for user 1" archetype) - keyed by the int 1.
struct Event {
    std::int64_t ts_ms;
    int value;
};

constexpr std::int64_t kMinute = 60LL * 1000LL;

}  // namespace

TEST(SlidingWindowScenario, OneHourWindowSlidingEveryMinute) {
    // Generate 70 minutes of events at 100 events/min, all keyed by 1.
    // Each event's value equals (minute_index + 1), so the aggregate
    // for window ending at minute m is the sum of values from minutes
    // [m-60, m-1] inclusive (when m >= 60), with cleaner math when we
    // count minutes from 0.
    constexpr int events_per_minute = 100;
    constexpr int total_minutes = 70;

    std::vector<Record<std::pair<int, int>>> input;
    input.reserve(static_cast<std::size_t>(total_minutes * events_per_minute));
    for (int minute = 0; minute < total_minutes; ++minute) {
        const int value = minute + 1;  // 1..70
        for (int i = 0; i < events_per_minute; ++i) {
            // Spread events evenly across the minute so they fall in
            // expected windows without timestamp collisions.
            const std::int64_t ts =
                (static_cast<std::int64_t>(minute) * kMinute) +
                static_cast<std::int64_t>((i * (kMinute - 1)) / events_per_minute);
            input.emplace_back(std::pair<int, int>{1, value}, EventTime{ts});
        }
    }

    auto src = std::make_shared<VectorSource<std::pair<int, int>>>(std::move(input));

    Dag dag;
    auto h0 = dag.add_source<std::pair<int, int>>(src);

    auto win = std::make_shared<SlidingWindowOperator<int, int, std::int64_t>>(
        std::chrono::hours{1},
        std::chrono::minutes{1},
        [] { return std::int64_t{0}; },
        [](std::int64_t agg, int v) { return agg + v; });

    auto h1 = dag.add_operator<std::pair<int, int>, std::pair<int, std::int64_t>>(h0, win);

    auto sink = std::make_shared<CollectingSink<std::pair<int, std::int64_t>>>();
    dag.add_sink<std::pair<int, std::int64_t>>(h1, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    // VectorSource emits every record then a Watermark::max() at end-of-
    // stream, so every started window fires. Some windows started before
    // the first record (end < kMinute) - those have empty aggregates and
    // never receive the "first" record so they don't emit. The first
    // window that contains data and ends >= kMinute is [0, kMinute), end
    // = kMinute. The last window is the one whose start = (70 - 1) min
    // = minute 69, ending at 69 + 60 = minute 129, which contains only
    // a slice of late minutes.
    //
    // Build the expected set. A window ending at minute m (m >= 1) covers
    // event minutes [m - 60, m - 1] (clamped to [0, total_minutes - 1]).
    // Aggregate = events_per_minute * sum_of_values_in_those_minutes.
    std::map<std::int64_t, std::int64_t> expected_by_window_end_ms;
    for (int win_end_min = 1; win_end_min <= total_minutes + 60 - 1; ++win_end_min) {
        const int start_min = std::max(0, win_end_min - 60);
        const int last_min = std::min(total_minutes - 1, win_end_min - 1);
        if (start_min > last_min) {
            continue;
        }
        std::int64_t sum_values = 0;
        for (int m = start_min; m <= last_min; ++m) {
            sum_values += static_cast<std::int64_t>(m + 1);
        }
        const std::int64_t agg = sum_values * static_cast<std::int64_t>(events_per_minute);
        const std::int64_t window_end_ms = static_cast<std::int64_t>(win_end_min) * kMinute;
        expected_by_window_end_ms[window_end_ms] = agg;
    }

    const auto results = sink->collected();
    // Same key in every emission.
    for (const auto& [k, v] : results) {
        EXPECT_EQ(k, 1);
    }
    // Number of emitted windows must match expected.
    EXPECT_EQ(results.size(), expected_by_window_end_ms.size());

    // Match emission counts and per-window aggregates by event-time of
    // the emission. Sliding window emits at window_end - 1 (event-time
    // of the last millisecond inside the window).
    auto emitted = sink->collected_with_event_times();
    std::map<std::int64_t, std::int64_t> got_by_window_end_ms;
    for (const auto& [pair, ts_opt] : emitted) {
        ASSERT_TRUE(ts_opt.has_value()) << "sliding window emissions should carry event time";
        // emission ts is window_end - 1; convert back.
        const std::int64_t window_end_ms = ts_opt->millis() + 1;
        got_by_window_end_ms[window_end_ms] = pair.second;
    }
    EXPECT_EQ(got_by_window_end_ms, expected_by_window_end_ms);

    // Spot-check a couple of midstream windows where the rolling sum
    // should be steady (each minute contributes events_per_minute *
    // value, and value runs from m-59 to m). After enough warm-up:
    //   window ending at minute 60 covers minutes [0..59], sum_vals =
    //     1 + 2 + ... + 60 = 1830, aggregate = 1830 * 100 = 183'000.
    EXPECT_EQ(got_by_window_end_ms.at(60 * kMinute), 183'000);
    //   window ending at minute 61 covers minutes [1..60], sum_vals =
    //     2 + 3 + ... + 61 = 1890, aggregate = 189'000.
    EXPECT_EQ(got_by_window_end_ms.at(61 * kMinute), 189'000);
    //   The increment between consecutive 60-min windows is exactly
    //   the difference of the dropped-out minute and the new one,
    //   times events_per_minute. Verify rolling property holds across
    //   the full middle of the run.
    for (int m = 60; m < total_minutes; ++m) {
        const std::int64_t cur = got_by_window_end_ms.at(static_cast<std::int64_t>(m) * kMinute);
        const std::int64_t next =
            got_by_window_end_ms.at(static_cast<std::int64_t>(m + 1) * kMinute);
        // Values per minute: m+1 - (m+1-60) = 60. Each minute contributes
        // events_per_minute records, so delta = 60 * 100 = 6000.
        EXPECT_EQ(next - cur, 6000)
            << "rolling-window delta wrong between minutes " << m << " and " << (m + 1);
    }
}
