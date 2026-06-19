// BATCH-1: bounded / batch execution contract.
//
// Three things are under test:
//   1. Source::is_bounded() is the boundedness contract, and the source runner
//      emits a max watermark at end-of-input for a bounded source (the
//      "end-of-input drain"). A cancelled or unbounded source does not, so a
//      streaming job is not spuriously drained.
//   2. Dag::all_sources_bounded() reports the job-level finite flag, and
//      LocalExecutor::is_bounded_job() resolves it against JobConfig's mode.
//   3. LocalExecutor::run_to_completion() runs a bounded job to natural
//      termination, and rejects a job forced into Batch mode that has an
//      unbounded source (it would never terminate).
//
// The drain is isolated from end-of-stream flush() by a probe operator whose
// flush() is the default no-op: it only records a max watermark if one actually
// flowed through as a watermark element, which only the source runner produces.

#include <atomic>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/time/event_time.hpp"
#include "clink/time/watermark.hpp"

using namespace clink;

namespace {

// Emits a fixed set of (value, event-time) records in a single produce() call
// and then signals exhaustion - crucially WITHOUT ever emitting a watermark of
// its own. Whether the runtime treats it as bounded is set at construction, so
// the same source exercises both the drain and the no-drain path.
class SilentSource final : public Source<int> {
public:
    SilentSource(std::vector<std::pair<int, std::int64_t>> rows, bool bounded)
        : rows_(std::move(rows)), bounded_(bounded) {}

    [[nodiscard]] bool is_bounded() const noexcept override { return bounded_; }

    bool produce(Emitter<int>& out) override {
        if (emitted_ || this->cancelled()) {
            return false;
        }
        Batch<int> batch;
        for (const auto& [v, ts] : rows_) {
            batch.emplace(v, EventTime::from_millis(ts));
        }
        out.emit_data(std::move(batch));
        emitted_ = true;
        return false;  // exhausted; no watermark emitted
    }

    std::string name() const override { return "silent_source"; }

private:
    std::vector<std::pair<int, std::int64_t>> rows_;
    bool bounded_;
    bool emitted_{false};
};

// Pass-through operator that records the largest watermark timestamp it
// observed via on_watermark. flush() is the inherited no-op, so the recorded
// value reflects only watermark elements that genuinely flowed through.
class WatermarkProbe final : public Operator<int, int> {
public:
    explicit WatermarkProbe(std::shared_ptr<std::atomic<std::int64_t>> max_wm)
        : max_wm_(std::move(max_wm)) {}

    void process(const StreamElement<int>& element, Emitter<int>& out) override {
        if (element.is_data()) {
            out.emit_data(Batch<int>(element.as_data()));
        } else if (element.is_watermark()) {
            this->on_watermark(element.as_watermark(), out);
        } else {
            this->on_barrier(element.as_barrier(), out);
        }
    }

    void on_watermark(Watermark wm, Emitter<int>& out) override {
        const auto ts = wm.timestamp().millis();
        std::int64_t prev = max_wm_->load();
        while (ts > prev && !max_wm_->compare_exchange_weak(prev, ts)) {
        }
        out.emit_watermark(wm);
    }

    std::string name() const override { return "watermark_probe"; }

private:
    std::shared_ptr<std::atomic<std::int64_t>> max_wm_;
};

// Counts the records it receives so a test can confirm the pipeline ran.
class CountingSink final : public Sink<int> {
public:
    explicit CountingSink(std::shared_ptr<std::atomic<std::int64_t>> count)
        : count_(std::move(count)) {}

    void on_data(const Batch<int>& batch) override {
        count_->fetch_add(static_cast<std::int64_t>(batch.size()));
    }

    std::string name() const override { return "counting_sink"; }

private:
    std::shared_ptr<std::atomic<std::int64_t>> count_;
};

constexpr std::int64_t kNoWatermark = std::numeric_limits<std::int64_t>::min();
const std::int64_t kMaxWmMillis = EventTime::max().millis();

// Build source -> probe -> sink and run it, returning (max watermark observed,
// records counted). Whether the source is bounded and the execution mode are
// the knobs.
struct RunResult {
    std::int64_t max_wm{kNoWatermark};
    std::int64_t records{0};
};

RunResult run_probe_pipeline(bool source_bounded,
                             JobConfig::ExecutionMode mode,
                             bool use_run_to_completion) {
    auto max_wm = std::make_shared<std::atomic<std::int64_t>>(kNoWatermark);
    auto count = std::make_shared<std::atomic<std::int64_t>>(0);

    Dag dag;
    auto src = dag.add_source<int>(std::make_shared<SilentSource>(
        std::vector<std::pair<int, std::int64_t>>{{1, 10}, {2, 20}, {3, 30}}, source_bounded));
    auto probe = dag.add_operator<int, int>(src, std::make_shared<WatermarkProbe>(max_wm));
    dag.add_sink<int>(probe, std::make_shared<CountingSink>(count));

    JobConfig config;
    config.execution_mode = mode;
    LocalExecutor exec(std::move(dag), config);
    if (use_run_to_completion) {
        exec.run_to_completion();
    } else {
        exec.run();
    }
    return {max_wm->load(), count->load()};
}

}  // namespace

// A bounded source reaches genuine end-of-input, so the runtime advances event
// time to its maximum: the probe sees a max watermark even though the source
// never emitted one.
TEST(BatchBoundedExecution, BoundedSourceDrainsToMaxWatermark) {
    const auto r = run_probe_pipeline(/*source_bounded=*/true,
                                      JobConfig::ExecutionMode::Auto,
                                      /*use_run_to_completion=*/true);
    EXPECT_EQ(r.records, 3);
    EXPECT_EQ(r.max_wm, kMaxWmMillis);
}

// An unbounded source that exhausts is not a true end-of-input, so no drain
// happens: the probe never sees a watermark (flush() is a no-op). This is the
// control that proves the max watermark comes from the bounded-source drain and
// not from end-of-stream flushing.
TEST(BatchBoundedExecution, UnboundedSourceDoesNotDrain) {
    const auto r = run_probe_pipeline(/*source_bounded=*/false,
                                      JobConfig::ExecutionMode::Auto,
                                      /*use_run_to_completion=*/false);
    EXPECT_EQ(r.records, 3);
    EXPECT_EQ(r.max_wm, kNoWatermark);
}

// Forcing Streaming mode over a bounded source keeps the streaming semantics,
// but the source is still genuinely bounded so the runner still drains it. The
// mode flag governs the executor's run-to-completion guard, not the per-source
// drain (which is keyed off is_bounded()).
TEST(BatchBoundedExecution, StreamingModeOverBoundedSourceStillDrains) {
    const auto r = run_probe_pipeline(/*source_bounded=*/true,
                                      JobConfig::ExecutionMode::Streaming,
                                      /*use_run_to_completion=*/false);
    EXPECT_EQ(r.records, 3);
    EXPECT_EQ(r.max_wm, kMaxWmMillis);
}

// Dag::all_sources_bounded() reflects per-source boundedness.
TEST(BatchBoundedExecution, DagBoundednessQuery) {
    {
        Dag dag;
        dag.add_source<int>(std::make_shared<SilentSource>(
            std::vector<std::pair<int, std::int64_t>>{{1, 1}}, /*bounded=*/true));
        EXPECT_TRUE(dag.all_sources_bounded());
        EXPECT_EQ(dag.source_count(), 1u);
    }
    {
        Dag dag;
        dag.add_source<int>(std::make_shared<SilentSource>(
            std::vector<std::pair<int, std::int64_t>>{{1, 1}}, /*bounded=*/false));
        EXPECT_FALSE(dag.all_sources_bounded());
    }
    {
        // Mixed: one bounded + one unbounded source is not a bounded job.
        Dag dag;
        dag.add_source<int>(std::make_shared<SilentSource>(
            std::vector<std::pair<int, std::int64_t>>{{1, 1}}, /*bounded=*/true));
        dag.add_source<int>(std::make_shared<SilentSource>(
            std::vector<std::pair<int, std::int64_t>>{{2, 2}}, /*bounded=*/false));
        EXPECT_FALSE(dag.all_sources_bounded());
        EXPECT_EQ(dag.source_count(), 2u);
    }
    {
        // A source-less DAG has no end-of-input and is not bounded.
        Dag dag;
        EXPECT_FALSE(dag.all_sources_bounded());
        EXPECT_EQ(dag.source_count(), 0u);
    }
}

// LocalExecutor::is_bounded_job() resolves the configured mode against the
// sources: Batch always claims bounded, Streaming never does, Auto follows the
// sources.
TEST(BatchBoundedExecution, IsBoundedJobResolvesMode) {
    auto make_dag = [](bool bounded) {
        Dag dag;
        dag.add_source<int>(std::make_shared<SilentSource>(
            std::vector<std::pair<int, std::int64_t>>{{1, 1}}, bounded));
        return dag;
    };

    {
        JobConfig c;
        c.execution_mode = JobConfig::ExecutionMode::Auto;
        LocalExecutor exec(make_dag(/*bounded=*/true), c);
        EXPECT_TRUE(exec.is_bounded_job());
    }
    {
        JobConfig c;
        c.execution_mode = JobConfig::ExecutionMode::Auto;
        LocalExecutor exec(make_dag(/*bounded=*/false), c);
        EXPECT_FALSE(exec.is_bounded_job());
    }
    {
        JobConfig c;
        c.execution_mode = JobConfig::ExecutionMode::Streaming;
        LocalExecutor exec(make_dag(/*bounded=*/true), c);
        EXPECT_FALSE(exec.is_bounded_job());
    }
    {
        JobConfig c;
        c.execution_mode = JobConfig::ExecutionMode::Batch;
        LocalExecutor exec(make_dag(/*bounded=*/false), c);
        EXPECT_TRUE(exec.is_bounded_job());  // declared Batch; the guard fires at run time
    }
}

// run_to_completion() refuses a Batch-declared job whose source is unbounded -
// it would block forever - and surfaces a clear error rather than hanging.
TEST(BatchBoundedExecution, RunToCompletionRejectsUnboundedBatch) {
    Dag dag;
    dag.add_source<int>(std::make_shared<SilentSource>(
        std::vector<std::pair<int, std::int64_t>>{{1, 1}}, /*bounded=*/false));
    JobConfig config;
    config.execution_mode = JobConfig::ExecutionMode::Batch;
    LocalExecutor exec(std::move(dag), config);
    EXPECT_THROW(exec.run_to_completion(), std::logic_error);
}

// A Batch-declared job over a genuinely bounded source runs to completion.
TEST(BatchBoundedExecution, RunToCompletionRunsBoundedBatch) {
    const auto r = run_probe_pipeline(/*source_bounded=*/true,
                                      JobConfig::ExecutionMode::Batch,
                                      /*use_run_to_completion=*/true);
    EXPECT_EQ(r.records, 3);
    EXPECT_EQ(r.max_wm, kMaxWmMillis);
}
