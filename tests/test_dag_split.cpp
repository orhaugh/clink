// Tests for Dag::add_split - single-input, N-output routing primitive
// modelled on Beam's TupleTag side-output pattern.

#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"

using namespace clink;

TEST(Split, RoutesRecordsToSelectedBranch) {
    Dag dag;
    std::vector<Record<int>> input;
    input.reserve(10);
    for (int i = 0; i < 10; ++i) {
        input.emplace_back(Record<int>{i});
    }
    auto src = std::make_shared<VectorSource<int>>(std::move(input));
    auto h0 = dag.add_source<int>(src);

    // Branch 0 = even, branch 1 = odd.
    auto branches = dag.add_split<int>(h0, [](const int& v) { return v % 2 == 0 ? 0 : 1; }, 2);

    auto evens = std::make_shared<CollectingSink<int>>();
    auto odds = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(branches[0], evens);
    dag.add_sink<int>(branches[1], odds);

    LocalExecutor exec(std::move(dag));
    exec.run();

    EXPECT_EQ(evens->collected(), (std::vector<int>{0, 2, 4, 6, 8}));
    EXPECT_EQ(odds->collected(), (std::vector<int>{1, 3, 5, 7, 9}));
}

TEST(Split, NegativeIndexDropsRecord) {
    Dag dag;
    std::vector<Record<int>> input;
    for (int i = 0; i < 5; ++i) {
        input.emplace_back(Record<int>{i});
    }
    auto src = std::make_shared<VectorSource<int>>(std::move(input));
    auto h0 = dag.add_source<int>(src);

    // Drop everything < 3, route the rest to branch 0.
    auto branches = dag.add_split<int>(h0, [](const int& v) { return v >= 3 ? 0 : -1; }, 1);

    auto sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(branches[0], sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    EXPECT_EQ(sink->collected(), (std::vector<int>{3, 4}));
}

TEST(Split, OutOfRangeIndexAlsoDrops) {
    Dag dag;
    std::vector<Record<int>> input;
    for (int i = 0; i < 5; ++i) {
        input.emplace_back(Record<int>{i});
    }
    auto src = std::make_shared<VectorSource<int>>(std::move(input));
    auto h0 = dag.add_source<int>(src);

    // Route to branch 5 - but we only have 2 branches. All drops.
    auto branches = dag.add_split<int>(h0, [](const int&) { return 5; }, 2);

    auto a = std::make_shared<CollectingSink<int>>();
    auto b = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(branches[0], a);
    dag.add_sink<int>(branches[1], b);

    LocalExecutor exec(std::move(dag));
    exec.run();

    EXPECT_TRUE(a->collected().empty());
    EXPECT_TRUE(b->collected().empty());
}

TEST(Split, ThreeWayRouteByPredicate) {
    Dag dag;
    std::vector<Record<int>> input;
    for (int i = 0; i < 9; ++i) {
        input.emplace_back(Record<int>{i});
    }
    auto src = std::make_shared<VectorSource<int>>(std::move(input));
    auto h0 = dag.add_source<int>(src);

    // 0=small (<3), 1=medium (3..5), 2=large (>=6).
    auto branches = dag.add_split<int>(
        h0,
        [](const int& v) {
            if (v < 3)
                return 0;
            if (v < 6)
                return 1;
            return 2;
        },
        3);

    auto small = std::make_shared<CollectingSink<int>>();
    auto medium = std::make_shared<CollectingSink<int>>();
    auto large = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(branches[0], small);
    dag.add_sink<int>(branches[1], medium);
    dag.add_sink<int>(branches[2], large);

    LocalExecutor exec(std::move(dag));
    exec.run();

    EXPECT_EQ(small->collected(), (std::vector<int>{0, 1, 2}));
    EXPECT_EQ(medium->collected(), (std::vector<int>{3, 4, 5}));
    EXPECT_EQ(large->collected(), (std::vector<int>{6, 7, 8}));
}

TEST(Split, ZeroBranchesRejected) {
    Dag dag;
    std::vector<Record<int>> input;
    auto src = std::make_shared<VectorSource<int>>(std::move(input));
    auto h0 = dag.add_source<int>(src);

    EXPECT_THROW(dag.add_split<int>(h0, [](const int&) { return 0; }, 0), std::invalid_argument);
}

TEST(Split, EmptyInputProducesEmptyBranches) {
    Dag dag;
    std::vector<Record<int>> empty;
    auto src = std::make_shared<VectorSource<int>>(std::move(empty));
    auto h0 = dag.add_source<int>(src);
    auto branches = dag.add_split<int>(h0, [](const int&) { return 0; }, 2);

    auto a = std::make_shared<CollectingSink<int>>();
    auto b = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(branches[0], a);
    dag.add_sink<int>(branches[1], b);

    LocalExecutor exec(std::move(dag));
    exec.run();

    EXPECT_TRUE(a->collected().empty());
    EXPECT_TRUE(b->collected().empty());
}

TEST(Split, WatermarksReachAllBranches) {
    // Watermarks and barriers must broadcast to every branch so
    // downstream alignment continues to work after the split.
    Dag dag;

    std::vector<Record<int>> input;
    for (int i = 0; i < 4; ++i) {
        input.emplace_back(Record<int>{i, EventTime{i * 100LL}});
    }
    auto src = std::make_shared<VectorSource<int>>(std::move(input));
    auto h0 = dag.add_source<int>(src);

    auto branches = dag.add_split<int>(h0, [](const int& v) { return v % 2; }, 2);

    auto a = std::make_shared<CollectingSink<int>>();
    auto b = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(branches[0], a);
    dag.add_sink<int>(branches[1], b);

    LocalExecutor exec(std::move(dag));
    exec.run();

    // VectorSource emits Watermark::max() at end-of-stream. Both sides
    // should observe it.
    EXPECT_EQ(a->last_watermark(), Watermark::max());
    EXPECT_EQ(b->last_watermark(), Watermark::max());
}
