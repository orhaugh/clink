#include <algorithm>
#include <chrono>
#include <memory>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"

using namespace clink;
using namespace std::chrono_literals;

// Self-join smoke test. With Dag::fork tee'ing one stream into two branches
// of an interval_join, every record matches itself plus every other record
// within the window. The test verifies the set of emissions matches the
// closed-form expected pairs and is independent of arrival interleaving.
TEST(IntervalJoin, SelfJoinViaForkProducesAllPairsWithinWindow) {
    Dag dag;

    // Single source emitting four events at 100, 200, 300, 400, all on the
    // same key.
    std::vector<Record<int>> events;
    for (auto t : {100, 200, 300, 400}) {
        events.emplace_back(Record<int>{t, EventTime{t}});
    }

    auto src = std::make_shared<VectorSource<int>>(std::move(events), "events");
    auto h_src = dag.add_source<int>(src);
    auto branches = dag.fork<int>(h_src, 2);
    ASSERT_EQ(branches.size(), 2u);

    // [lower=0, upper=200]: emit (a, b) when t_b ∈ [t_a, t_a + 200].
    auto h_j = dag.interval_join<int, int, int, std::pair<int, int>>(
        branches[0],
        branches[1],
        [](int) { return 0; },
        [](int) { return 0; },
        0ms,
        200ms,
        [](const std::optional<int>& a, const std::optional<int>& b) {
            return std::make_pair(*a, *b);
        });

    auto sink = std::make_shared<CollectingSink<std::pair<int, int>>>();
    dag.add_sink<std::pair<int, int>>(h_j, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto pairs = sink->collected();
    std::sort(pairs.begin(), pairs.end());

    // Closed form: every (i, j) where i, j ∈ {100, 200, 300, 400} and
    // 0 ≤ j - i ≤ 200.
    std::vector<std::pair<int, int>> expected{
        {100, 100},
        {100, 200},
        {100, 300},
        {200, 200},
        {200, 300},
        {200, 400},
        {300, 300},
        {300, 400},
        {400, 400},
    };
    EXPECT_EQ(pairs, expected);
}
