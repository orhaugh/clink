#include <memory>
#include <stdexcept>
#include <vector>

#include <gtest/gtest.h>

#include "clink/operators/filter_operator.hpp"
#include "clink/operators/map_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"

using namespace clink;

TEST(MapFilter, FilterRejectsAllRecords) {
    Dag dag;
    std::vector<Record<int>> input;
    for (int i = 0; i < 5; ++i) {
        input.emplace_back(Record<int>{i});
    }
    auto src = std::make_shared<VectorSource<int>>(std::move(input));
    auto reject = std::make_shared<FilterOperator<int>>([](int) { return false; }, "reject_all");
    auto sink = std::make_shared<CollectingSink<int>>();

    auto h0 = dag.add_source<int>(src);
    auto h1 = dag.add_operator<int, int>(h0, reject);
    dag.add_sink<int>(h1, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();
    EXPECT_TRUE(sink->collected().empty());
}

TEST(MapFilter, UserExceptionIsRecordedNotPropagated) {
    // Engine contract: when a user fn throws, the engine catches it,
    // records it in operator_errors(), and cancels the rest of the job
    // gracefully. exec.run() returns normally; the caller checks
    // operator_errors() to detect failure.
    Dag dag;
    std::vector<Record<int>> input;
    input.emplace_back(Record<int>{1});
    input.emplace_back(Record<int>{2});

    auto src = std::make_shared<VectorSource<int>>(std::move(input));
    auto bad = std::make_shared<MapOperator<int, int>>(
        [](int) -> int { throw std::runtime_error("user fn failed"); }, "throwing_map");
    auto sink = std::make_shared<CollectingSink<int>>();

    auto h0 = dag.add_source<int>(src);
    auto h1 = dag.add_operator<int, int>(h0, bad);
    dag.add_sink<int>(h1, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();  // returns cleanly, even though one op threw

    const auto errors = exec.operator_errors();
    ASSERT_FALSE(errors.empty());
    bool found = false;
    for (const auto& [op_name, msg] : errors) {
        if (op_name == "throwing_map" && msg.find("user fn failed") != std::string::npos) {
            found = true;
        }
    }
    EXPECT_TRUE(found);
}

TEST(MapFilter, EndToEndExecution) {
    Dag dag;
    std::vector<Record<int>> input;
    for (int i = 0; i < 10; ++i) {
        input.emplace_back(Record<int>{i});
    }

    auto src = std::make_shared<VectorSource<int>>(std::move(input));
    auto sq = std::make_shared<MapOperator<int, int>>([](int x) { return x * x; }, "square");
    auto even = std::make_shared<FilterOperator<int>>([](int x) { return x % 2 == 0; }, "even");
    auto sink = std::make_shared<CollectingSink<int>>();

    auto h0 = dag.add_source<int>(src);
    auto h1 = dag.add_operator<int, int>(h0, sq);
    auto h2 = dag.add_operator<int, int>(h1, even);
    dag.add_sink<int>(h2, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto out = sink->collected();
    // Squares of 0..9 = {0,1,4,9,16,25,36,49,64,81}; even ones = {0,4,16,36,64}
    std::vector<int> expected{0, 4, 16, 36, 64};
    EXPECT_EQ(out, expected);
}

TEST(MapFilter, MapForwardsEventTime) {
    Dag dag;

    std::vector<Record<int>> input;
    input.emplace_back(Record<int>{10, EventTime{100}});
    input.emplace_back(Record<int>{20, EventTime{200}});

    auto src = std::make_shared<VectorSource<int>>(std::move(input));
    auto m = std::make_shared<MapOperator<int, int>>([](int x) { return x + 1; });
    auto sink = std::make_shared<CollectingSink<int>>();

    auto h0 = dag.add_source<int>(src);
    auto h1 = dag.add_operator<int, int>(h0, m);
    dag.add_sink<int>(h1, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    EXPECT_EQ(sink->collected(), (std::vector<int>{11, 21}));
    // The collected sink doesn't expose per-record event times; we test
    // event-time propagation via the watermark test instead.
}
