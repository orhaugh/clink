#include <algorithm>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/operators/reduce_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"

using namespace clink;

// Job topology:
//
//   VectorSource (p=1)
//        │  pair<user_id, amount>
//        │  hash-shuffle by user_id
//        ▼
//   ReduceOperator (p=4, OnFlush) - each subtask owns a disjoint set of keys
//        │  pair<user_id, total>
//        │  fan-in
//        ▼
//   CollectingSink (p=1)
//
// Verifies: every record for a given user_id is owned by exactly one
// reduce subtask, the per-key totals are correct, and the sink collects
// the union of all subtask outputs.
TEST(ParallelDag, ShuffledReduceProducesCorrectPerKeyTotals) {
    using KV = std::pair<int, std::int64_t>;

    // 7 records across 3 keys.
    std::vector<Record<KV>> input{
        Record<KV>{{1, 10}},
        Record<KV>{{2, 100}},
        Record<KV>{{1, 20}},
        Record<KV>{{3, 1000}},
        Record<KV>{{2, 200}},
        Record<KV>{{1, 30}},
        Record<KV>{{3, 2000}},
    };

    Dag dag;

    auto src_handle = dag.add_parallel_source<KV>(
        [&](std::size_t /*subtask*/) -> std::shared_ptr<Source<KV>> {
            // For p=1 we only get called with subtask=0.
            return std::make_shared<VectorSource<KV>>(input, "src");
        },
        /*parallelism*/ 1);

    auto reduce_handle = dag.add_parallel_operator_shuffled<KV, std::pair<int, std::int64_t>>(
        src_handle,
        [](std::size_t subtask) -> std::shared_ptr<Operator<KV, std::pair<int, std::int64_t>>> {
            return std::make_shared<ReduceOperator<int, std::int64_t, std::int64_t>>(
                []() -> std::int64_t { return 0; },
                [](const std::int64_t& acc, const std::int64_t& v) { return acc + v; },
                "reduce_sub" + std::to_string(subtask),
                ReduceEmitMode::OnFlush);
        },
        /*parallelism*/ 4,
        [](const KV& kv) -> std::size_t { return std::hash<int>{}(kv.first); });

    auto sink = std::make_shared<CollectingSink<std::pair<int, std::int64_t>>>();

    dag.add_parallel_sink<std::pair<int, std::int64_t>>(
        reduce_handle,
        [sink](std::size_t /*subtask*/) -> std::shared_ptr<Sink<std::pair<int, std::int64_t>>> {
            return sink;
        },
        /*parallelism*/ 1);

    LocalExecutor exec(std::move(dag));
    exec.run();

    // Build a map from the collected pairs (each key emitted once at flush
    // time per subtask; since hash partitioning routes a key to exactly
    // one subtask, each key shows up exactly once).
    std::unordered_map<int, std::int64_t> totals;
    for (const auto& [k, v] : sink->collected()) {
        totals[k] = v;
    }

    EXPECT_EQ(totals.size(), 3u);
    EXPECT_EQ(totals[1], 60);    // 10 + 20 + 30
    EXPECT_EQ(totals[2], 300);   // 100 + 200
    EXPECT_EQ(totals[3], 3000);  // 1000 + 2000
}
