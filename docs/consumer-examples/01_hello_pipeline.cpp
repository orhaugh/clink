// 01 - Minimal in-process pipeline.
//
// Demonstrates the smallest possible clink pipeline: a bounded source, a
// map step, a filter step, and a sink that prints to stdout. Run it as
// a standalone executable - no clink_node, no cluster, no coordinator.
//
// Pipeline:
//   VectorSource<int64_t> -> Map(*2) -> Filter(>10) -> FunctionSink(print)
//
// Build:  cmake --build build --target 01_hello_pipeline
// Run:    ./build/01_hello_pipeline

#include <cstdint>
#include <iostream>
#include <memory>
#include <vector>

#include <clink/operators/filter_operator.hpp>
#include <clink/operators/map_operator.hpp>
#include <clink/operators/sink_operator.hpp>
#include <clink/operators/source_operator.hpp>
#include <clink/runtime/dag.hpp>
#include <clink/runtime/local_executor.hpp>

int main() {
    using namespace clink;

    std::vector<Record<std::int64_t>> input;
    for (std::int64_t i = 1; i <= 10; ++i) {
        input.emplace_back(Record<std::int64_t>{i, EventTime{i}});
    }

    Dag dag;

    auto src = std::make_shared<VectorSource<std::int64_t>>(std::move(input));
    auto doubler = std::make_shared<MapOperator<std::int64_t, std::int64_t>>(
        [](const std::int64_t& v) { return v * 2; });
    auto big_only = std::make_shared<FilterOperator<std::int64_t>>(
        [](const std::int64_t& v) { return v > 10; });
    auto printer = std::make_shared<FunctionSink<std::int64_t>>(
        [](const std::int64_t& v) { std::cout << v << '\n'; });

    auto s0 = dag.add_source<std::int64_t>(src);
    auto s1 = dag.add_operator<std::int64_t, std::int64_t>(s0, doubler);
    auto s2 = dag.add_operator<std::int64_t, std::int64_t>(s1, big_only);
    dag.add_sink<std::int64_t>(s2, printer);

    LocalExecutor exec(std::move(dag));
    exec.run();
    return 0;
}
