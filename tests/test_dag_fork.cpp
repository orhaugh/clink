#include <algorithm>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "clink/operators/filter_operator.hpp"
#include "clink/operators/map_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"

using namespace clink;

TEST(DagFork, BroadcastsDataAndBarriersToAllBranches) {
    Dag dag;

    std::vector<Record<int>> input;
    for (int i = 1; i <= 6; ++i) {
        input.emplace_back(Record<int>{i});
    }

    auto src = std::make_shared<VectorSource<int>>(std::move(input));

    // Two branches: one keeps evens and doubles them, the other keeps odds
    // and squares them.
    auto evens = std::make_shared<FilterOperator<int>>([](int x) { return x % 2 == 0; });
    auto doubler = std::make_shared<MapOperator<int, int>>([](int x) { return x * 2; });
    auto odds = std::make_shared<FilterOperator<int>>([](int x) { return x % 2 != 0; });
    auto squarer = std::make_shared<MapOperator<int, int>>([](int x) { return x * x; });
    auto sink_a = std::make_shared<CollectingSink<int>>();
    auto sink_b = std::make_shared<CollectingSink<int>>();

    auto h_src = dag.add_source<int>(src);
    auto branches = dag.fork<int>(h_src, 2);
    ASSERT_EQ(branches.size(), 2u);

    auto h_a1 = dag.add_operator<int, int>(branches[0], evens);
    auto h_a2 = dag.add_operator<int, int>(h_a1, doubler);
    dag.add_sink<int>(h_a2, sink_a);

    auto h_b1 = dag.add_operator<int, int>(branches[1], odds);
    auto h_b2 = dag.add_operator<int, int>(h_b1, squarer);
    dag.add_sink<int>(h_b2, sink_b);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto a = sink_a->collected();
    auto b = sink_b->collected();
    std::sort(a.begin(), a.end());
    std::sort(b.begin(), b.end());

    EXPECT_EQ(a, (std::vector<int>{4, 8, 12}));  // 2*2, 4*2, 6*2
    EXPECT_EQ(b, (std::vector<int>{1, 9, 25}));  // 1^2, 3^2, 5^2

    // Both branches should have observed Watermark::max from the source's
    // end-of-stream emit.
    EXPECT_EQ(sink_a->last_watermark(), Watermark::max());
    EXPECT_EQ(sink_b->last_watermark(), Watermark::max());
}

TEST(DagForkParallel, BroadcastsToAllBranchesAtParallelism) {
    // Parallel analogue of the test above. The producer is a parallelism=2
    // source where each subtask emits a known set of records. Two consumer
    // branches each apply a distinct map; each branch keeps the producer's
    // parallelism (2) end-to-end. CollectingSink at the tail aggregates
    // across subtasks via fan-in.
    //
    // Verifies: every record produced by every upstream subtask reaches
    // BOTH consumer branches (not split between them), and watermarks /
    // barriers cross the fork unchanged.
    constexpr std::size_t kParallelism = 2;

    // Each subtask gets its own 3-record VectorSource so the test is
    // deterministic - combined we expect 6 records on each branch.
    const std::vector<std::vector<Record<int>>> per_subtask_input{
        {Record<int>{1}, Record<int>{2}, Record<int>{3}},
        {Record<int>{10}, Record<int>{20}, Record<int>{30}},
    };

    Dag dag;

    auto src_handle = dag.add_parallel_source<int>(
        [&](std::size_t subtask) -> std::shared_ptr<Source<int>> {
            return std::make_shared<VectorSource<int>>(per_subtask_input[subtask],
                                                       "src_sub" + std::to_string(subtask));
        },
        /*parallelism=*/kParallelism);

    auto branches = dag.fork_parallel<int>(src_handle, /*m=*/2);
    ASSERT_EQ(branches.size(), 2u);
    EXPECT_EQ(branches[0].parallelism, kParallelism);
    EXPECT_EQ(branches[1].parallelism, kParallelism);

    auto branch_a = dag.add_parallel_operator<int, int>(
        branches[0],
        [](std::size_t /*subtask*/) -> std::shared_ptr<Operator<int, int>> {
            return std::make_shared<MapOperator<int, int>>([](int x) { return x + 100; },
                                                           "branch_a_add100");
        },
        /*parallelism=*/kParallelism);

    auto branch_b = dag.add_parallel_operator<int, int>(
        branches[1],
        [](std::size_t /*subtask*/) -> std::shared_ptr<Operator<int, int>> {
            return std::make_shared<MapOperator<int, int>>([](int x) { return x * x; },
                                                           "branch_b_square");
        },
        /*parallelism=*/kParallelism);

    auto sink_a = std::make_shared<CollectingSink<int>>();
    auto sink_b = std::make_shared<CollectingSink<int>>();

    dag.add_parallel_sink<int>(
        branch_a,
        [sink_a](std::size_t /*subtask*/) -> std::shared_ptr<Sink<int>> { return sink_a; },
        /*parallelism=*/1);
    dag.add_parallel_sink<int>(
        branch_b,
        [sink_b](std::size_t /*subtask*/) -> std::shared_ptr<Sink<int>> { return sink_b; },
        /*parallelism=*/1);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto a = sink_a->collected();
    auto b = sink_b->collected();
    std::sort(a.begin(), a.end());
    std::sort(b.begin(), b.end());

    // Branch A: every upstream record + 100.
    EXPECT_EQ(a, (std::vector<int>{101, 102, 103, 110, 120, 130}));
    // Branch B: every upstream record squared.
    EXPECT_EQ(b, (std::vector<int>{1, 4, 9, 100, 400, 900}));

    // End-of-stream watermark must cross the fork to every branch.
    EXPECT_EQ(sink_a->last_watermark(), Watermark::max());
    EXPECT_EQ(sink_b->last_watermark(), Watermark::max());
}

TEST(DagForkParallel, RejectsLessThanTwoBranches) {
    Dag dag;
    auto src_handle = dag.add_parallel_source<int>(
        [](std::size_t) -> std::shared_ptr<Source<int>> {
            return std::make_shared<VectorSource<int>>(std::vector<Record<int>>{Record<int>{1}});
        },
        /*parallelism=*/2);
    EXPECT_THROW(dag.fork_parallel<int>(src_handle, /*m=*/1), std::invalid_argument);
}
