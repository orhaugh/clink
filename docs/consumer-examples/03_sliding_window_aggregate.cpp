// 03 - Sliding window aggregation.
//
// Sums event values per user over 10-second windows that slide every
// 2 seconds. Each event belongs to (size / slide) = 5 overlapping
// windows; output volume scales accordingly.
//
// Pipeline:
//   VectorSource<Trade> -> KeyBy(symbol) -> SlidingWindow(10s, 2s, sum) -> Sink

#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <clink/operators/key_by_operator.hpp>
#include <clink/operators/sink_operator.hpp>
#include <clink/operators/sliding_window_operator.hpp>
#include <clink/operators/source_operator.hpp>
#include <clink/runtime/dag.hpp>
#include <clink/runtime/local_executor.hpp>

struct Trade {
    std::string symbol;
    std::int64_t shares;
};

int main() {
    using namespace clink;
    using namespace std::chrono_literals;

    // 30 trades spaced 1 second apart, alternating between two symbols.
    std::vector<Record<Trade>> input;
    for (int i = 0; i < 30; ++i) {
        Trade t{(i % 2 == 0) ? "AAPL" : "GOOG", 100 + i};
        input.emplace_back(Record<Trade>{std::move(t),
                                         EventTime{static_cast<std::int64_t>(i) * 1000}});
    }

    Dag dag;

    auto src    = std::make_shared<VectorSource<Trade>>(std::move(input));
    auto key_by = std::make_shared<KeyByOperator<Trade, std::string>>(
        [](const Trade& t) { return t.symbol; });
    auto window = std::make_shared<SlidingWindowOperator<std::string, Trade, std::int64_t>>(
        /*size=*/10s,
        /*slide=*/2s,
        []() -> std::int64_t { return 0; },
        [](const std::int64_t& acc, const Trade& t) { return acc + t.shares; });
    auto sink   = std::make_shared<FunctionSink<std::pair<std::string, std::int64_t>>>(
        [](const std::pair<std::string, std::int64_t>& kv) {
            std::cout << "symbol=" << kv.first << " window_shares=" << kv.second << '\n';
        });

    auto s0 = dag.add_source<Trade>(src);
    auto s1 = dag.add_operator<Trade, std::pair<std::string, Trade>>(s0, key_by);
    auto s2 = dag.add_operator<std::pair<std::string, Trade>,
                               std::pair<std::string, std::int64_t>>(s1, window);
    dag.add_sink<std::pair<std::string, std::int64_t>>(s2, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();
    return 0;
}
