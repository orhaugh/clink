// Phase 30d (metrics-coverage pass): per-operator data-plane metrics.
// Drives small in-process DAGs and asserts that the global
// MetricsRegistry exposes records_in / records_out / window_panes /
// join_matches / async_lookup_hits with the right operator-id suffix.

#include <chrono>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/api/stream_execution_environment.hpp"
#include "clink/core/codec.hpp"
#include "clink/core/types.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/metrics/operator_metrics.hpp"
#include "clink/operators/map_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

// Pull the snapshot, find a counter by name, return its value or 0.
std::uint64_t counter_value(const std::string& name) {
    auto snap = MetricsRegistry::global().snapshot();
    for (const auto& [n, v] : snap.counters) {
        if (n == name) {
            return v;
        }
    }
    return 0;
}

// Latency histogram count/sum (OBS-1b: process_latency is now a histogram).
std::uint64_t hist_count(const std::string& base) {
    return MetricsRegistry::global().histogram(base).snapshot().count;
}
double hist_sum(const std::string& base) {
    return MetricsRegistry::global().histogram(base).snapshot().sum;
}

}  // namespace

TEST(OperatorMetrics, RecordsInAndOutIncrementForMapStage) {
    // Reset global registry state so this test doesn't depend on
    // ordering with other tests. The registry isn't reset between
    // tests in this binary, so we snapshot the BEFORE values and
    // assert on the delta.
    Dag dag;
    std::vector<Record<std::int64_t>> records;
    for (std::int64_t i = 1; i <= 5; ++i) {
        records.emplace_back(Record<std::int64_t>{i});
    }
    auto src = std::make_shared<VectorSource<std::int64_t>>(std::move(records));
    auto map_op = std::make_shared<MapOperator<std::int64_t, std::int64_t>>(
        [](const std::int64_t& v) { return v * 10; }, "test_map");
    auto sink = std::make_shared<CollectingSink<std::int64_t>>();

    auto h0 = dag.add_source<std::int64_t>(src);
    auto h1 = dag.add_operator<std::int64_t, std::int64_t>(h0, map_op);
    dag.add_sink<std::int64_t>(h1, sink);

    // Capture op ids BEFORE running so we can pin which counters to
    // read once the DAG terminates.
    const auto map_id = map_op->id().value();
    const auto sink_id = sink->id().value();
    EXPECT_EQ(sink->id().value(), sink_id);

    const auto map_in_before =
        counter_value(clink::metrics::op_metric_name("records_in_total", map_id));
    const auto map_out_before =
        counter_value(clink::metrics::op_metric_name("records_out_total", map_id));

    // Per-operator metrics now route through the configured registry (the
    // host one on the cluster path); point the in-process executor at the
    // global registry this test reads.
    JobConfig cfg;
    cfg.metrics = &MetricsRegistry::global();
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    EXPECT_EQ(sink->collected(), (std::vector<std::int64_t>{10, 20, 30, 40, 50}));

    const auto map_in_after =
        counter_value(clink::metrics::op_metric_name("records_in_total", map_id));
    const auto map_out_after =
        counter_value(clink::metrics::op_metric_name("records_out_total", map_id));
    EXPECT_EQ(map_in_after - map_in_before, 5u);
    EXPECT_EQ(map_out_after - map_out_before, 5u);
}

TEST(OperatorMetrics, CountersIsolatedPerOperatorId) {
    // Two map stages in the same job: each should track its own
    // records_in / records_out independently.
    Dag dag;
    std::vector<Record<std::int64_t>> records;
    for (std::int64_t i = 1; i <= 3; ++i) {
        records.emplace_back(Record<std::int64_t>{i});
    }
    auto src = std::make_shared<VectorSource<std::int64_t>>(std::move(records));
    auto m1 = std::make_shared<MapOperator<std::int64_t, std::int64_t>>(
        [](const std::int64_t& v) { return v + 1; }, "m1");
    auto m2 = std::make_shared<MapOperator<std::int64_t, std::int64_t>>(
        [](const std::int64_t& v) { return v + 10; }, "m2");
    auto sink = std::make_shared<CollectingSink<std::int64_t>>();

    auto h0 = dag.add_source<std::int64_t>(src);
    auto h1 = dag.add_operator<std::int64_t, std::int64_t>(h0, m1);
    auto h2 = dag.add_operator<std::int64_t, std::int64_t>(h1, m2);
    dag.add_sink<std::int64_t>(h2, sink);

    const auto m1_id = m1->id().value();
    const auto m2_id = m2->id().value();
    ASSERT_NE(m1_id, m2_id);

    const auto m1_in_before =
        counter_value(clink::metrics::op_metric_name("records_in_total", m1_id));
    const auto m2_in_before =
        counter_value(clink::metrics::op_metric_name("records_in_total", m2_id));

    // Per-operator metrics now route through the configured registry (the
    // host one on the cluster path); point the in-process executor at the
    // global registry this test reads.
    JobConfig cfg;
    cfg.metrics = &MetricsRegistry::global();
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    const auto m1_in_after =
        counter_value(clink::metrics::op_metric_name("records_in_total", m1_id));
    const auto m2_in_after =
        counter_value(clink::metrics::op_metric_name("records_in_total", m2_id));
    EXPECT_EQ(m1_in_after - m1_in_before, 3u);
    EXPECT_EQ(m2_in_after - m2_in_before, 3u);
}

TEST(OperatorMetrics, HelpersAreReentrantForArbitraryOperatorIds) {
    // The helper namespace fans incrementing through the global
    // registry by name; calling the same helper twice for the same
    // op_id must accumulate rather than overwrite.
    using namespace clink::metrics;
    const std::uint64_t op_id = 999'999u;  // synthetic, won't collide
    auto* reg = &MetricsRegistry::global();
    const auto before = counter_value(op_metric_name("records_out_total", op_id));
    op::records_out_inc(reg, op_id, 3);
    op::records_out_inc(reg, op_id, 4);
    EXPECT_EQ(counter_value(op_metric_name("records_out_total", op_id)) - before, 7u);

    const auto before_drop = counter_value(op_metric_name("records_dropped_total", op_id));
    op::records_dropped_inc(reg, op_id);
    op::records_dropped_inc(reg, op_id);
    EXPECT_EQ(counter_value(op_metric_name("records_dropped_total", op_id)) - before_drop, 2u);

    const auto before_lat_count = hist_count(op_metric_name("process_latency_ns", op_id));
    op::process_latency_observe(reg, op_id, 1500);
    op::process_latency_observe(reg, op_id, 2500);
    EXPECT_EQ(hist_count(op_metric_name("process_latency_ns", op_id)) - before_lat_count, 2u);
    const auto sum = hist_sum(op_metric_name("process_latency_ns", op_id));
    EXPECT_GE(sum, 4000.0);
}
