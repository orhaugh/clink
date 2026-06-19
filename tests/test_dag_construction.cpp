#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/operators/filter_operator.hpp"
#include "clink/operators/map_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"

using namespace clink;

TEST(Dag, BuildsLinearPipelineWithUniqueIds) {
    Dag dag;

    std::vector<Record<int>> input;
    input.emplace_back(Record<int>{1});
    input.emplace_back(Record<int>{2});
    auto src = std::make_shared<VectorSource<int>>(std::move(input));

    auto map = std::make_shared<MapOperator<int, int>>([](int x) { return x + 1; });
    auto filt = std::make_shared<FilterOperator<int>>([](int x) { return x > 0; });
    auto sink = std::make_shared<CollectingSink<int>>();

    auto h_src = dag.add_source<int>(src);
    auto h_map = dag.add_operator<int, int>(h_src, map);
    auto h_filt = dag.add_operator<int, int>(h_map, filt);
    dag.add_sink<int>(h_filt, sink);

    EXPECT_EQ(dag.operator_count(), 4u);

    // Operator ids should be unique and non-zero.
    std::vector<std::uint64_t> ids;
    for (const auto& r : dag.runners()) {
        ids.push_back(r.id.value());
    }
    EXPECT_EQ(ids.size(), 4u);
    for (auto id : ids) {
        EXPECT_GT(id, 0u);
    }
    auto sorted = ids;
    std::sort(sorted.begin(), sorted.end());
    EXPECT_TRUE(std::adjacent_find(sorted.begin(), sorted.end()) == sorted.end());
}

TEST(Dag, NamesPropagateFromOperators) {
    Dag dag;
    auto src = std::make_shared<VectorSource<int>>(std::vector<Record<int>>{}, "my_source");
    auto map = std::make_shared<MapOperator<int, int>>([](int x) { return x; }, "doubler");
    auto sink = std::make_shared<CollectingSink<int>>();

    auto h_src = dag.add_source<int>(src);
    auto h_map = dag.add_operator<int, int>(h_src, map);
    dag.add_sink<int>(h_map, sink);

    const auto& runners = dag.runners();
    EXPECT_EQ(runners[0].name, "my_source");
    EXPECT_EQ(runners[1].name, "doubler");
    EXPECT_EQ(runners[2].name, "collecting_sink");
}
