// BATCH-3: batch scheduler / planner.
//
// Covers the recommended-parallelism clamp, logical stage cutting at blocking
// edges (BATCH-2), the source split-count -> parallelism derivation, and the
// LocalExecutor convenience accessor. The planner is analysis only; it does not
// change execution (the pragmatic scheduler keeps eager launch).

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/arrow_batcher.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/map_operator.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/batch_scheduler.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/key_groups.hpp"
#include "clink/runtime/local_executor.hpp"

using namespace clink;

namespace {

// A bounded source that declares a configurable split count.
class SplitSource final : public Source<std::int64_t> {
public:
    explicit SplitSource(std::size_t splits) : splits_(splits) {}

    [[nodiscard]] bool is_bounded() const noexcept override { return true; }
    [[nodiscard]] std::size_t split_count() const noexcept override { return splits_; }

    bool produce(Emitter<std::int64_t>& out) override {
        if (done_ || this->cancelled()) {
            return false;
        }
        Batch<std::int64_t> b;
        b.emplace(1);
        out.emit_data(std::move(b));
        done_ = true;
        return false;
    }

    std::string name() const override { return "split_source"; }

private:
    std::size_t splits_;
    bool done_{false};
};

class NullSink final : public Sink<std::int64_t> {
public:
    void on_data(const Batch<std::int64_t>&) override {}
    std::string name() const override { return "null_sink"; }
};

std::shared_ptr<MapOperator<std::int64_t, std::int64_t>> identity_map() {
    return std::make_shared<MapOperator<std::int64_t, std::int64_t>>(
        [](const std::int64_t& v) { return v; });
}

}  // namespace

TEST(BatchScheduler, RecommendedParallelismClamps) {
    using batch::recommended_parallelism;
    EXPECT_EQ(recommended_parallelism(8, 0, 128), 8u);  // no operator bound
    EXPECT_EQ(recommended_parallelism(8, 4, 128), 4u);  // operator bound binds
    EXPECT_EQ(recommended_parallelism(8, 16, 4), 4u);   // key groups bind
    EXPECT_EQ(recommended_parallelism(100, 16, 128), 16u);
    EXPECT_EQ(recommended_parallelism(0, 0, 0), 1u);    // floor at 1
    EXPECT_EQ(recommended_parallelism(1, 8, 128), 1u);  // single-split source
}

TEST(BatchScheduler, SingleStageWhenNoBlockingEdge) {
    Dag dag;
    auto src = dag.add_source<std::int64_t>(std::make_shared<SplitSource>(1));
    auto mapped = dag.add_operator<std::int64_t, std::int64_t>(src, identity_map());
    dag.add_sink<std::int64_t>(mapped, std::make_shared<NullSink>());

    const auto plan = batch::compute_batch_plan(dag);
    ASSERT_EQ(plan.stage_count(), 1u);
    EXPECT_EQ(plan.stages[0].index, 0u);
    EXPECT_EQ(plan.stages[0].first_runner, 0u);
    EXPECT_EQ(plan.stages[0].last_runner, 2u);
    EXPECT_EQ(plan.stages[0].runner_count(), 3u);
}

TEST(BatchScheduler, TwoStagesCutAtBlockingExchange) {
    Dag dag;
    auto src = dag.add_source<std::int64_t>(std::make_shared<SplitSource>(1));
    auto ex = dag.add_blocking_exchange<std::int64_t>(src, int64_arrow_batcher());
    dag.add_sink<std::int64_t>(ex, std::make_shared<NullSink>());

    const auto plan = batch::compute_batch_plan(dag);
    ASSERT_EQ(plan.stage_count(), 2u);
    // Stage 0 = source + the exchange (it buffers during the producer phase).
    EXPECT_EQ(plan.stages[0].first_runner, 0u);
    EXPECT_EQ(plan.stages[0].last_runner, 1u);
    // Stage 1 = the consumer (sink).
    EXPECT_EQ(plan.stages[1].first_runner, 2u);
    EXPECT_EQ(plan.stages[1].last_runner, 2u);
}

TEST(BatchScheduler, ThreeStagesWithTwoBlockingExchanges) {
    Dag dag;
    auto src = dag.add_source<std::int64_t>(std::make_shared<SplitSource>(1));
    auto ex1 = dag.add_blocking_exchange<std::int64_t>(src, int64_arrow_batcher());
    auto mapped = dag.add_operator<std::int64_t, std::int64_t>(ex1, identity_map());
    auto ex2 = dag.add_blocking_exchange<std::int64_t>(mapped, int64_arrow_batcher());
    dag.add_sink<std::int64_t>(ex2, std::make_shared<NullSink>());

    const auto plan = batch::compute_batch_plan(dag);
    // runners: 0 src, 1 ex1, 2 map, 3 ex2, 4 sink. cuts at {1,3}.
    // stage_of: 0->0, 1->0, 2->1, 3->1, 4->2.
    ASSERT_EQ(plan.stage_count(), 3u);
    EXPECT_EQ(plan.stages[0].first_runner, 0u);
    EXPECT_EQ(plan.stages[0].last_runner, 1u);
    EXPECT_EQ(plan.stages[1].first_runner, 2u);
    EXPECT_EQ(plan.stages[1].last_runner, 3u);
    EXPECT_EQ(plan.stages[2].first_runner, 4u);
    EXPECT_EQ(plan.stages[2].last_runner, 4u);
}

TEST(BatchScheduler, SourceSplitCountDrivesRecommendation) {
    Dag dag;
    auto src = dag.add_source<std::int64_t>(std::make_shared<SplitSource>(8));
    dag.add_sink<std::int64_t>(src, std::make_shared<NullSink>());

    const auto plan = batch::compute_batch_plan(dag, /*num_key_groups=*/128);
    EXPECT_EQ(plan.max_source_split_count, 8u);
    EXPECT_EQ(plan.recommended_keyed_parallelism(/*operator_max=*/0),
              8u);  // clamped to key groups only
    EXPECT_EQ(plan.recommended_keyed_parallelism(/*operator_max=*/4), 4u);  // operator bound binds
    EXPECT_EQ(plan.num_key_groups, 128u);
}

TEST(BatchScheduler, KeyGroupsClampRecommendation) {
    Dag dag;
    auto src = dag.add_source<std::int64_t>(std::make_shared<SplitSource>(64));
    dag.add_sink<std::int64_t>(src, std::make_shared<NullSink>());

    const auto plan = batch::compute_batch_plan(dag, /*num_key_groups=*/16);
    EXPECT_EQ(plan.max_source_split_count, 64u);
    EXPECT_EQ(plan.recommended_keyed_parallelism(), 16u);  // key groups bind at 16
}

TEST(BatchScheduler, EmptyDagHasNoStages) {
    Dag dag;
    const auto plan = batch::compute_batch_plan(dag);
    EXPECT_EQ(plan.stage_count(), 0u);
    EXPECT_EQ(plan.max_source_split_count, 1u);
}

TEST(BatchScheduler, ExecutorComputeBatchPlanReflectsDag) {
    Dag dag;
    auto src = dag.add_source<std::int64_t>(std::make_shared<SplitSource>(4));
    auto ex = dag.add_blocking_exchange<std::int64_t>(src, int64_arrow_batcher());
    dag.add_sink<std::int64_t>(ex, std::make_shared<NullSink>());

    JobConfig config;
    config.execution_mode = JobConfig::ExecutionMode::Batch;
    LocalExecutor exec(std::move(dag), config);

    const auto plan = exec.compute_batch_plan();
    EXPECT_EQ(plan.stage_count(), 2u);
    EXPECT_EQ(plan.max_source_split_count, 4u);
    EXPECT_EQ(plan.num_key_groups, static_cast<std::size_t>(kNumKeyGroups));
}
