// An input that closes can complete a pending barrier's alignment: the other
// inputs have delivered checkpoint 7, the last one finishes without it, and
// the aligner releases 7 at the close. Every multi-input runner used to drop
// that release on the floor, so the barrier never left the operator, its
// snapshot was never taken and the checkpoint could only time out. Each test
// drives one runner directly over pre-loaded, pre-closed inputs, so the order
// is fixed: the barrier on input 0, then input 1 closes without it.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/core/codec.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"

#include "tests/test_helpers/sanitizer_slack.hpp"

using namespace clink;

namespace {

using V = std::int64_t;
using Ch = BoundedChannel<StreamElement<V>>;
constexpr std::uint64_t kCheckpoint = 7;

StreamElement<V> one_value(V v) {
    Batch<V> b;
    b.emplace(v);
    return StreamElement<V>::data(std::move(b));
}

StreamElement<V> aligned_barrier() {
    return StreamElement<V>::barrier(CheckpointBarrier{
        CheckpointId{kCheckpoint}, /*terminal=*/false, CheckpointBarrier::Mode::Aligned});
}

// Input 0 delivers the barrier and closes; input 1 sends a record and closes
// without it.
std::pair<std::shared_ptr<Ch>, std::shared_ptr<Ch>> barrier_then_bare_close() {
    auto in0 = std::make_shared<Ch>(16);
    auto in1 = std::make_shared<Ch>(16);
    in0->push(aligned_barrier());
    in0->close();
    in1->push(one_value(1));
    in1->close();
    return {in0, in1};
}

bool output_carries_the_barrier(Ch& out) {
    while (auto m = out.try_pop()) {
        if (m->is_barrier() && m->as_barrier().id().value() == kCheckpoint) {
            return true;
        }
    }
    return false;
}

void run_runner(Dag& dag, std::size_t index, InMemoryStateBackend& backend) {
    const auto& runner = dag.runners()[index];
    RuntimeContext ctx(runner.id, runner.name, &backend, &MetricsRegistry::global());
    runner.run(ctx, [] { return false; });
}

class PassCoOp final : public CoOperator<V, V, V> {
public:
    void process_element1(const StreamElement<V>& el, Emitter<V>& out) override { out.emit(el); }
    void process_element2(const StreamElement<V>& el, Emitter<V>& out) override { out.emit(el); }
    std::string name() const override { return "pass_co_op"; }
};

class BarrierRecordingSink final : public Sink<V> {
public:
    void on_data(const Batch<V>& /*batch*/) override {}
    void on_barrier(CheckpointBarrier b) override { barriers.push_back(b.id().value()); }
    std::string name() const override { return "barrier_recording_sink"; }
    std::vector<std::uint64_t> barriers;
};

class BarrierRecordingOp final : public Operator<V, V> {
public:
    void process(const StreamElement<V>& el, Emitter<V>& out) override { out.emit(el); }
    void on_barrier(CheckpointBarrier b, Emitter<V>& out) override {
        barriers.push_back(b.id().value());
        Operator<V, V>::on_barrier(b, out);
    }
    std::string name() const override { return "barrier_recording_op"; }
    std::vector<std::uint64_t> barriers;
};

class NeverRunSource final : public Source<V> {
public:
    bool produce(Emitter<V>& /*out*/) override { return false; }
    std::string name() const override { return "never_run_source"; }
};

}  // namespace

TEST(CloseCompletesAlignment, AUnionForwardsTheBarrier) {
    auto [in0, in1] = barrier_then_bare_close();
    Dag dag;
    auto h = dag.union_streams<V>(
        std::vector<StageHandle<V>>{StageHandle<V>{in0, 0}, StageHandle<V>{in1, 0}});
    InMemoryStateBackend backend;
    run_runner(dag, h.runner_index, backend);
    EXPECT_TRUE(output_carries_the_barrier(*h.output));
}

TEST(CloseCompletesAlignment, AnIntervalJoinForwardsTheBarrier) {
    auto [in0, in1] = barrier_then_bare_close();
    Dag dag;
    auto h = dag.interval_join<V, V, V, V>(
        StageHandle<V>{in0, 0},
        StageHandle<V>{in1, 0},
        [](const V& v) { return v; },
        [](const V& v) { return v; },
        std::chrono::milliseconds{-10},
        std::chrono::milliseconds{10},
        [](const std::optional<V>& a, const std::optional<V>& b) {
            return a.value_or(0) + b.value_or(0);
        },
        Dag::JoinType::Inner,
        "interval_join",
        int64_codec(),
        int64_codec(),
        int64_codec());
    InMemoryStateBackend backend;
    run_runner(dag, h.runner_index, backend);
    EXPECT_TRUE(output_carries_the_barrier(*h.output));
}

TEST(CloseCompletesAlignment, ABroadcastConnectForwardsTheBarrier) {
    auto [in0, in1] = barrier_then_bare_close();
    Dag dag;
    auto h = dag.broadcast_connect<V, V, V, V>(
        StageHandle<V>{in0, 0},
        StageHandle<V>{in1, 0},
        [](const V&, BroadcastState<V>&) {},
        [](const V& v, BroadcastState<V>&) { return std::optional<V>{v}; },
        int64_codec(),
        "broadcast",
        "broadcast_connect",
        int64_codec(),
        int64_codec());
    InMemoryStateBackend backend;
    run_runner(dag, h.runner_index, backend);
    EXPECT_TRUE(output_carries_the_barrier(*h.output));
}

TEST(CloseCompletesAlignment, ACoOperatorSnapshotsAndForwardsTheBarrier) {
    auto [in0, in1] = barrier_then_bare_close();
    Dag dag;
    auto op = std::make_shared<PassCoOp>();
    op->set_uid("close_completes_co_op");
    auto h = dag.add_co_operator<V, V, V>(
        StageHandle<V>{in0, 0}, StageHandle<V>{in1, 0}, op, int64_codec(), int64_codec());
    InMemoryStateBackend backend;
    run_runner(dag, h.runner_index, backend);
    EXPECT_TRUE(output_carries_the_barrier(*h.output));
}

TEST(CloseCompletesAlignment, AFanInSinkSeesTheBarrier) {
    Dag dag;
    auto upstream = dag.add_parallel_source<V>(
        [](std::size_t) { return std::make_shared<NeverRunSource>(); }, /*parallelism=*/2);
    auto sink = std::make_shared<BarrierRecordingSink>();
    dag.add_parallel_sink<V>(upstream, [sink](std::size_t) { return sink; }, /*parallelism=*/1);
    upstream.emitters[0]->emit_barrier(CheckpointBarrier{
        CheckpointId{kCheckpoint}, /*terminal=*/false, CheckpointBarrier::Mode::Aligned});
    upstream.emitters[0]->close_all();
    Batch<V> b;
    b.emplace(1);
    upstream.emitters[1]->emit_data(std::move(b));
    upstream.emitters[1]->close_all();

    InMemoryStateBackend backend;
    run_runner(dag, dag.runners().size() - 1, backend);
    EXPECT_EQ(sink->barriers, (std::vector<std::uint64_t>{kCheckpoint}));
}

TEST(CloseCompletesAlignment, AnOperatorFedBySeveralSubtasksSeesTheBarrier) {
    Dag dag;
    auto upstream = dag.add_parallel_source<V>(
        [](std::size_t) { return std::make_shared<NeverRunSource>(); }, /*parallelism=*/2);
    auto op = std::make_shared<BarrierRecordingOp>();
    auto downstream = dag.add_parallel_operator_shuffled<V, V>(
        upstream,
        [op](std::size_t) { return op; },
        /*parallelism=*/1,
        [](const V&) -> std::size_t { return 0; });
    upstream.emitters[0]->emit_barrier(CheckpointBarrier{
        CheckpointId{kCheckpoint}, /*terminal=*/false, CheckpointBarrier::Mode::Aligned});
    upstream.emitters[0]->close_all();
    Batch<V> b;
    b.emplace(1);
    upstream.emitters[1]->emit_data(std::move(b));
    upstream.emitters[1]->close_all();

    const auto& runners = dag.runners();
    const auto it = std::find_if(runners.begin(), runners.end(), [&](const auto& r) {
        return r.id == downstream.subtask_ids[0];
    });
    ASSERT_NE(it, runners.end());
    InMemoryStateBackend backend;
    run_runner(dag, static_cast<std::size_t>(it - runners.begin()), backend);
    EXPECT_EQ(op->barriers, (std::vector<std::uint64_t>{kCheckpoint}));
}

// A close also frees time. The aligner holds a closed input at end-of-time, so
// once the input holding the minimum watermark closes, the runner has a higher
// watermark to forward - and must forward it then, because the survivors may
// stay idle for as long as they like. The cases below close the lagging input
// without a final end-of-time watermark first (a rescale drain, a subtask that
// ends early, a cancel), keep the other input open and quiet, and wait for its
// watermark to come through. Every runner is driven on its own thread so the
// open input really is open.
namespace {

constexpr std::int64_t kLagging = 100;
constexpr std::int64_t kLeading = 200;
constexpr std::uint64_t kLaggingBarrier = 7;
constexpr std::uint64_t kLeadingBarrier = 8;
const std::int64_t kEndOfTime = EventTime::max().millis();

Watermark at(std::int64_t ms) {
    return Watermark{EventTime{ms}};
}

CheckpointBarrier aligned(std::uint64_t id) {
    return CheckpointBarrier{
        CheckpointId{id}, /*terminal=*/false, CheckpointBarrier::Mode::Aligned};
}

// Poll `done` until it holds or a generous deadline passes; the deadline is a
// failure bound, not a delay.
bool await(const std::function<bool()>& done) {
    using namespace std::chrono_literals;
    const auto deadline = std::chrono::steady_clock::now() + test_support::scale_slack(10s);
    while (!done()) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(1ms);
    }
    return true;
}

// Runs one Dag runner on a thread until its inputs close, or until the test
// gives up on it (the destructor stops and joins a runner that is still live).
class RunnerThread {
public:
    RunnerThread(Dag& dag, std::size_t index)
        : thread_([this, &dag, index] {
              const auto& runner = dag.runners()[index];
              InMemoryStateBackend backend;
              RuntimeContext ctx(runner.id, runner.name, &backend, &MetricsRegistry::global());
              runner.run(ctx, [this] { return stop_.load(); });
              done_.store(true);
          }) {}
    RunnerThread(const RunnerThread&) = delete;
    RunnerThread& operator=(const RunnerThread&) = delete;
    ~RunnerThread() {
        stop_.store(true);
        thread_.join();
    }

    // True once the runner returned of its own accord.
    bool finishes() {
        return await([this] { return done_.load(); });
    }

private:
    std::atomic<bool> stop_{false};
    std::atomic<bool> done_{false};
    std::thread thread_;
};

// What a sink or operator was handed, written by the runner thread and read by
// the test thread.
class Seen {
public:
    void watermark(Watermark wm) {
        std::lock_guard lock(mu_);
        watermarks_.push_back(wm.timestamp().millis());
    }
    void barrier(CheckpointBarrier b) {
        std::lock_guard lock(mu_);
        barriers_.push_back(b.id().value());
    }
    std::vector<std::int64_t> watermarks() const {
        std::lock_guard lock(mu_);
        return watermarks_;
    }
    std::vector<std::uint64_t> barriers() const {
        std::lock_guard lock(mu_);
        return barriers_;
    }
    bool reached(std::int64_t ms) const {
        std::lock_guard lock(mu_);
        return !watermarks_.empty() && watermarks_.back() == ms;
    }
    bool saw_barrier(std::uint64_t id) const {
        std::lock_guard lock(mu_);
        return std::find(barriers_.begin(), barriers_.end(), id) != barriers_.end();
    }

private:
    mutable std::mutex mu_;
    std::vector<std::int64_t> watermarks_;
    std::vector<std::uint64_t> barriers_;
};

class TimeRecordingSink final : public Sink<V> {
public:
    void on_data(const Batch<V>& /*batch*/) override {}
    void on_watermark(Watermark wm) override { seen.watermark(wm); }
    void on_barrier(CheckpointBarrier b) override { seen.barrier(b); }
    std::string name() const override { return "time_recording_sink"; }
    Seen seen;
};

class TimeRecordingOp final : public Operator<V, V> {
public:
    void process(const StreamElement<V>& el, Emitter<V>& out) override { out.emit(el); }
    void on_watermark(Watermark wm, Emitter<V>& out) override {
        seen.watermark(wm);
        Operator<V, V>::on_watermark(wm, out);
    }
    void on_barrier(CheckpointBarrier b, Emitter<V>& out) override {
        seen.barrier(b);
        Operator<V, V>::on_barrier(b, out);
    }
    std::string name() const override { return "time_recording_op"; }
    Seen seen;
};

// Two upstream subtasks feeding one fan-in sink subtask. Returns the upstream
// handle (whose emitters the test drives) and the sink runner's index.
std::pair<Dag::ParallelStageHandle<V>, std::size_t> fan_in_sink(
    Dag& dag, const std::shared_ptr<TimeRecordingSink>& sink) {
    auto upstream = dag.add_parallel_source<V>(
        [](std::size_t) { return std::make_shared<NeverRunSource>(); }, /*parallelism=*/2);
    dag.add_parallel_sink<V>(upstream, [sink](std::size_t) { return sink; }, /*parallelism=*/1);
    return {upstream, dag.runners().size() - 1};
}

// Two upstream subtasks shuffled into one operator subtask.
std::pair<Dag::ParallelStageHandle<V>, std::size_t> fan_in_operator(
    Dag& dag, const std::shared_ptr<TimeRecordingOp>& op) {
    auto upstream = dag.add_parallel_source<V>(
        [](std::size_t) { return std::make_shared<NeverRunSource>(); }, /*parallelism=*/2);
    auto downstream = dag.add_parallel_operator_shuffled<V, V>(
        upstream,
        [op](std::size_t) { return op; },
        /*parallelism=*/1,
        [](const V&) -> std::size_t { return 0; });
    const auto& runners = dag.runners();
    const auto it = std::find_if(runners.begin(), runners.end(), [&](const auto& r) {
        return r.id == downstream.subtask_ids[0];
    });
    return {upstream, static_cast<std::size_t>(it - runners.begin())};
}

// Input 0 lags at kLagging and closes (finished or cancelled) with no
// end-of-time watermark; input 1 leads at kLeading and stays open.
void lagging_input_closes(const Dag::ParallelStageHandle<V>& upstream, ChannelCloseReason reason) {
    upstream.emitters[0]->emit_watermark(at(kLagging));
    upstream.emitters[1]->emit_watermark(at(kLeading));
    upstream.emitters[0]->close_all(reason);
}

// As above, but input 0 delivers a barrier before it closes, so the runner has
// it paused for alignment when the close lands; input 1 delivers a different
// barrier, which can only align once input 0's close is seen.
void lagging_input_closes_while_paused(const Dag::ParallelStageHandle<V>& upstream) {
    upstream.emitters[0]->emit_watermark(at(kLagging));
    upstream.emitters[0]->emit_barrier(aligned(kLaggingBarrier));
    upstream.emitters[1]->emit_watermark(at(kLeading));
    upstream.emitters[1]->emit_barrier(aligned(kLeadingBarrier));
    upstream.emitters[0]->close_all();
}

// The shared assertions for a runner that records into `seen`.
void expect_close_frees_time(Seen& seen,
                             RunnerThread& runner,
                             const Dag::ParallelStageHandle<V>& upstream,
                             ChannelCloseReason first_close) {
    EXPECT_TRUE(await([&] { return seen.reached(kLeading); }))
        << "the lagging input closed; the open input's watermark must come through";
    upstream.emitters[1]->close_all();
    ASSERT_TRUE(runner.finishes());
    // The last close is end of stream, which these runners leave to flush():
    // they also serve single-input forward stages, where a finished close is
    // not end of time (see the forward cases below).
    EXPECT_EQ(seen.watermarks(), (std::vector<std::int64_t>{kLagging, kLeading}))
        << (first_close == ChannelCloseReason::Cancelled
                ? "a set of closes that includes a cancel is not end-of-input"
                : "the last close leaves end of stream to flush()");
}

void expect_paused_close_frees_time(Seen& seen,
                                    RunnerThread& runner,
                                    const Dag::ParallelStageHandle<V>& upstream) {
    EXPECT_TRUE(await([&] { return seen.reached(kLeading) && seen.saw_barrier(kLeadingBarrier); }))
        << "a close on a paused input must free its time and the other input's barrier";
    upstream.emitters[1]->close_all();
    ASSERT_TRUE(runner.finishes());
    EXPECT_EQ(seen.watermarks(), (std::vector<std::int64_t>{kLagging, kLeading}));
    EXPECT_EQ(seen.barriers(), (std::vector<std::uint64_t>{kLeadingBarrier, kLaggingBarrier}));
}

}  // namespace

TEST(CloseFreesTime, AFanInSinkAdvancesWhenTheLaggingInputCloses) {
    Dag dag;
    auto sink = std::make_shared<TimeRecordingSink>();
    auto [upstream, index] = fan_in_sink(dag, sink);
    lagging_input_closes(upstream, ChannelCloseReason::Finished);
    RunnerThread runner(dag, index);
    expect_close_frees_time(sink->seen, runner, upstream, ChannelCloseReason::Finished);
}

TEST(CloseFreesTime, AFanInSinkAdvancesOnACancelledCloseButNeverToEndOfTime) {
    Dag dag;
    auto sink = std::make_shared<TimeRecordingSink>();
    auto [upstream, index] = fan_in_sink(dag, sink);
    lagging_input_closes(upstream, ChannelCloseReason::Cancelled);
    RunnerThread runner(dag, index);
    expect_close_frees_time(sink->seen, runner, upstream, ChannelCloseReason::Cancelled);
}

TEST(CloseFreesTime, AFanInSinkSeesACloseOnAPausedInput) {
    Dag dag;
    auto sink = std::make_shared<TimeRecordingSink>();
    auto [upstream, index] = fan_in_sink(dag, sink);
    lagging_input_closes_while_paused(upstream);
    RunnerThread runner(dag, index);
    expect_paused_close_frees_time(sink->seen, runner, upstream);
}

TEST(CloseFreesTime, AnOperatorFedBySeveralSubtasksAdvancesWhenTheLaggingInputCloses) {
    Dag dag;
    auto op = std::make_shared<TimeRecordingOp>();
    auto [upstream, index] = fan_in_operator(dag, op);
    lagging_input_closes(upstream, ChannelCloseReason::Finished);
    RunnerThread runner(dag, index);
    expect_close_frees_time(op->seen, runner, upstream, ChannelCloseReason::Finished);
}

TEST(CloseFreesTime, AnOperatorFedBySeveralSubtasksAdvancesOnACancelledCloseButNeverToEndOfTime) {
    Dag dag;
    auto op = std::make_shared<TimeRecordingOp>();
    auto [upstream, index] = fan_in_operator(dag, op);
    lagging_input_closes(upstream, ChannelCloseReason::Cancelled);
    RunnerThread runner(dag, index);
    expect_close_frees_time(op->seen, runner, upstream, ChannelCloseReason::Cancelled);
}

TEST(CloseFreesTime, AnOperatorFedBySeveralSubtasksSeesACloseOnAPausedInput) {
    Dag dag;
    auto op = std::make_shared<TimeRecordingOp>();
    auto [upstream, index] = fan_in_operator(dag, op);
    lagging_input_closes_while_paused(upstream);
    RunnerThread runner(dag, index);
    expect_paused_close_frees_time(op->seen, runner, upstream);
}

// The parallel sink and operator runners also serve forward stages, one input
// per subtask. A graceful stop of an unbounded source closes that input
// Finished without an end-of-time watermark; turning the close into end of
// time would fire every pending event-time timer after the stop checkpoint,
// and again on resume. End of stream is flush()'s business, as it is in the
// single-input runner.
namespace {

void forward_input_finishes_without_end_of_time(const Dag::ParallelStageHandle<V>& upstream) {
    upstream.emitters[0]->emit_watermark(at(kLagging));
    upstream.emitters[0]->close_all(ChannelCloseReason::Finished);
}

}  // namespace

TEST(CloseFreesTime, AForwardSinkLeavesEndOfStreamToFlush) {
    Dag dag;
    auto sink = std::make_shared<TimeRecordingSink>();
    auto upstream = dag.add_parallel_source<V>(
        [](std::size_t) { return std::make_shared<NeverRunSource>(); }, /*parallelism=*/1);
    dag.add_parallel_sink<V>(upstream, [sink](std::size_t) { return sink; }, /*parallelism=*/1);
    forward_input_finishes_without_end_of_time(upstream);
    RunnerThread runner(dag, dag.runners().size() - 1);
    ASSERT_TRUE(runner.finishes());
    EXPECT_EQ(sink->seen.watermarks(), (std::vector<std::int64_t>{kLagging}))
        << "a finished close of a sole input is not end of time";
}

TEST(CloseFreesTime, AForwardOperatorLeavesEndOfStreamToFlush) {
    Dag dag;
    auto op = std::make_shared<TimeRecordingOp>();
    auto upstream = dag.add_parallel_source<V>(
        [](std::size_t) { return std::make_shared<NeverRunSource>(); }, /*parallelism=*/1);
    auto downstream = dag.add_parallel_operator<V, V>(
        upstream, [op](std::size_t) { return op; }, /*parallelism=*/1);
    const auto& runners = dag.runners();
    const auto it = std::find_if(runners.begin(), runners.end(), [&](const auto& r) {
        return r.id == downstream.subtask_ids[0];
    });
    forward_input_finishes_without_end_of_time(upstream);
    RunnerThread runner(dag, static_cast<std::size_t>(it - runners.begin()));
    ASSERT_TRUE(runner.finishes());
    EXPECT_EQ(op->seen.watermarks(), (std::vector<std::int64_t>{kLagging}))
        << "a finished close of a sole input is not end of time";
}

// The two-input runners poll a paused input only once its barrier aligns, so
// the same paused close is the case to pin for them: input 0 is paused at one
// barrier and closes, input 1 delivers another and stays open.
namespace {

// Reads a runner's output channel from the test thread.
class OutputLog {
public:
    explicit OutputLog(std::shared_ptr<Ch> out) : out_(std::move(out)) {}

    void drain() {
        while (auto m = out_->try_pop()) {
            if (m->is_watermark()) {
                seen.watermark(m->as_watermark());
            } else if (m->is_barrier()) {
                seen.barrier(m->as_barrier());
            }
        }
    }

    Seen seen;

private:
    std::shared_ptr<Ch> out_;
};

std::pair<std::shared_ptr<Ch>, std::shared_ptr<Ch>> paused_lagging_input_and_open_leader() {
    auto in0 = std::make_shared<Ch>(16);
    auto in1 = std::make_shared<Ch>(16);
    in0->push(StreamElement<V>::watermark(at(kLagging)));
    in0->push(StreamElement<V>::barrier(aligned(kLaggingBarrier)));
    in0->close();
    in1->push(StreamElement<V>::watermark(at(kLeading)));
    in1->push(StreamElement<V>::barrier(aligned(kLeadingBarrier)));
    return {in0, in1};
}

void expect_two_input_paused_close_frees_time(Dag& dag, const StageHandle<V>& h, Ch& in1) {
    OutputLog log(h.output);
    RunnerThread runner(dag, h.runner_index);
    EXPECT_TRUE(await([&] {
        log.drain();
        return log.seen.reached(kLeading) && log.seen.saw_barrier(kLeadingBarrier);
    })) << "a close on a paused input must free its time and the other input's barrier";
    in1.close();
    ASSERT_TRUE(runner.finishes());
    log.drain();
    EXPECT_EQ(log.seen.watermarks(), (std::vector<std::int64_t>{kLagging, kLeading, kEndOfTime}));
    EXPECT_EQ(log.seen.barriers(), (std::vector<std::uint64_t>{kLeadingBarrier, kLaggingBarrier}));
}

}  // namespace

TEST(CloseFreesTime, AUnionSeesACloseOnAPausedInput) {
    auto [in0, in1] = paused_lagging_input_and_open_leader();
    Dag dag;
    auto h = dag.union_streams<V>(
        std::vector<StageHandle<V>>{StageHandle<V>{in0, 0}, StageHandle<V>{in1, 0}});
    expect_two_input_paused_close_frees_time(dag, h, *in1);
}

TEST(CloseFreesTime, AnIntervalJoinSeesACloseOnAPausedInput) {
    auto [in0, in1] = paused_lagging_input_and_open_leader();
    Dag dag;
    auto h = dag.interval_join<V, V, V, V>(
        StageHandle<V>{in0, 0},
        StageHandle<V>{in1, 0},
        [](const V& v) { return v; },
        [](const V& v) { return v; },
        std::chrono::milliseconds{-10},
        std::chrono::milliseconds{10},
        [](const std::optional<V>& a, const std::optional<V>& b) {
            return a.value_or(0) + b.value_or(0);
        },
        Dag::JoinType::Inner,
        "interval_join",
        int64_codec(),
        int64_codec(),
        int64_codec());
    expect_two_input_paused_close_frees_time(dag, h, *in1);
}

TEST(CloseFreesTime, ABroadcastConnectSeesACloseOnAPausedInput) {
    auto [in0, in1] = paused_lagging_input_and_open_leader();
    Dag dag;
    auto h = dag.broadcast_connect<V, V, V, V>(
        StageHandle<V>{in0, 0},
        StageHandle<V>{in1, 0},
        [](const V&, BroadcastState<V>&) {},
        [](const V& v, BroadcastState<V>&) { return std::optional<V>{v}; },
        int64_codec(),
        "broadcast",
        "broadcast_connect",
        int64_codec(),
        int64_codec());
    expect_two_input_paused_close_frees_time(dag, h, *in1);
}

TEST(CloseFreesTime, ACoOperatorSeesACloseOnAPausedInput) {
    auto [in0, in1] = paused_lagging_input_and_open_leader();
    Dag dag;
    auto op = std::make_shared<PassCoOp>();
    op->set_uid("close_frees_time_co_op");
    auto h = dag.add_co_operator<V, V, V>(
        StageHandle<V>{in0, 0}, StageHandle<V>{in1, 0}, op, int64_codec(), int64_codec());
    expect_two_input_paused_close_frees_time(dag, h, *in1);
}

// A drain (an armed cutover or a rescale) ends with a drain marker and then a
// finished close. That close frees the survivors' time like any other, but it
// is a handoff rather than end of input: once every input is gone, the
// watermark must not jump to end-of-time and fire every open window.
namespace {

void drained_input_then_survivor_closes(const Dag::ParallelStageHandle<V>& upstream) {
    upstream.emitters[0]->emit_watermark(at(kLagging));
    upstream.emitters[1]->emit_watermark(at(kLeading));
    upstream.emitters[0]->emit_drain(DrainMarker{.subtask_idx = 0, .target_parallelism = 0});
    upstream.emitters[0]->close_all();
}

void expect_drain_frees_time_without_ending_it(Seen& seen,
                                               RunnerThread& runner,
                                               const Dag::ParallelStageHandle<V>& upstream) {
    EXPECT_TRUE(await([&] { return seen.reached(kLeading); }))
        << "the drained input closed; the open input's watermark must come through";
    upstream.emitters[1]->close_all();
    ASSERT_TRUE(runner.finishes());
    EXPECT_EQ(seen.watermarks(), (std::vector<std::int64_t>{kLagging, kLeading}))
        << "a drain is a handoff, not end of input";
}

}  // namespace

TEST(CloseFreesTime, AFanInSinkTreatsADrainAsAHandoffNotAnEnding) {
    Dag dag;
    auto sink = std::make_shared<TimeRecordingSink>();
    auto [upstream, index] = fan_in_sink(dag, sink);
    drained_input_then_survivor_closes(upstream);
    RunnerThread runner(dag, index);
    expect_drain_frees_time_without_ending_it(sink->seen, runner, upstream);
}

TEST(CloseFreesTime, AnOperatorFedBySeveralSubtasksTreatsADrainAsAHandoffNotAnEnding) {
    Dag dag;
    auto op = std::make_shared<TimeRecordingOp>();
    auto [upstream, index] = fan_in_operator(dag, op);
    drained_input_then_survivor_closes(upstream);
    RunnerThread runner(dag, index);
    expect_drain_frees_time_without_ending_it(op->seen, runner, upstream);
}

TEST(CloseFreesTime, AUnionTreatsADrainAsAHandoffNotAnEnding) {
    auto in0 = std::make_shared<Ch>(16);
    auto in1 = std::make_shared<Ch>(16);
    in0->push(StreamElement<V>::watermark(at(kLagging)));
    in1->push(StreamElement<V>::watermark(at(kLeading)));
    in0->push(StreamElement<V>::drain(DrainMarker{.subtask_idx = 0, .target_parallelism = 0}));
    in0->close();
    Dag dag;
    auto h = dag.union_streams<V>(
        std::vector<StageHandle<V>>{StageHandle<V>{in0, 0}, StageHandle<V>{in1, 0}});
    OutputLog log(h.output);
    RunnerThread runner(dag, h.runner_index);
    EXPECT_TRUE(await([&] {
        log.drain();
        return log.seen.reached(kLeading);
    })) << "the drained input closed; the open input's watermark must come through";
    in1->close();
    ASSERT_TRUE(runner.finishes());
    log.drain();
    EXPECT_EQ(log.seen.watermarks(), (std::vector<std::int64_t>{kLagging, kLeading}))
        << "a drain is a handoff, not end of input";
}

// The two-input runners do recompute on the last close, so for them the drain
// is what keeps an all-closed set from reaching end-of-time. Each side has its
// own drain branch; drain each in turn.
namespace {

// Input `drained` lags at kLagging, delivers a drain marker and closes; the
// other input leads at kLeading and stays open. Returns the survivor.
std::pair<std::pair<std::shared_ptr<Ch>, std::shared_ptr<Ch>>, std::shared_ptr<Ch>>
drained_input_and_open_survivor(std::size_t drained) {
    auto in0 = std::make_shared<Ch>(16);
    auto in1 = std::make_shared<Ch>(16);
    auto& gone = drained == 0 ? in0 : in1;
    auto& survivor = drained == 0 ? in1 : in0;
    gone->push(StreamElement<V>::watermark(at(kLagging)));
    gone->push(StreamElement<V>::drain(
        DrainMarker{.subtask_idx = static_cast<std::uint32_t>(drained), .target_parallelism = 0}));
    gone->close();
    survivor->push(StreamElement<V>::watermark(at(kLeading)));
    return {{in0, in1}, survivor};
}

void expect_two_input_drain_frees_time_without_ending_it(Dag& dag,
                                                         const StageHandle<V>& h,
                                                         Ch& survivor) {
    OutputLog log(h.output);
    RunnerThread runner(dag, h.runner_index);
    EXPECT_TRUE(await([&] {
        log.drain();
        return log.seen.reached(kLeading);
    })) << "the drained input closed; the open input's watermark must come through";
    survivor.close();
    ASSERT_TRUE(runner.finishes());
    log.drain();
    EXPECT_EQ(log.seen.watermarks(), (std::vector<std::int64_t>{kLagging, kLeading}))
        << "a drain is a handoff, not end of input";
}

StageHandle<V> interval_join_of(const std::shared_ptr<Ch>& in0,
                                const std::shared_ptr<Ch>& in1,
                                Dag& dag) {
    return dag.interval_join<V, V, V, V>(
        StageHandle<V>{in0, 0},
        StageHandle<V>{in1, 0},
        [](const V& v) { return v; },
        [](const V& v) { return v; },
        std::chrono::milliseconds{-10},
        std::chrono::milliseconds{10},
        [](const std::optional<V>& a, const std::optional<V>& b) {
            return a.value_or(0) + b.value_or(0);
        },
        Dag::JoinType::Inner,
        "interval_join",
        int64_codec(),
        int64_codec(),
        int64_codec());
}

StageHandle<V> broadcast_connect_of(const std::shared_ptr<Ch>& in0,
                                    const std::shared_ptr<Ch>& in1,
                                    Dag& dag) {
    return dag.broadcast_connect<V, V, V, V>(
        StageHandle<V>{in0, 0},
        StageHandle<V>{in1, 0},
        [](const V&, BroadcastState<V>&) {},
        [](const V& v, BroadcastState<V>&) { return std::optional<V>{v}; },
        int64_codec(),
        "broadcast",
        "broadcast_connect",
        int64_codec(),
        int64_codec());
}

StageHandle<V> co_operator_of(const std::shared_ptr<Ch>& in0,
                              const std::shared_ptr<Ch>& in1,
                              Dag& dag) {
    auto op = std::make_shared<PassCoOp>();
    op->set_uid("drain_handoff_co_op");
    return dag.add_co_operator<V, V, V>(
        StageHandle<V>{in0, 0}, StageHandle<V>{in1, 0}, op, int64_codec(), int64_codec());
}

}  // namespace

TEST(CloseFreesTime, AnIntervalJoinTreatsADrainAsAHandoffNotAnEnding) {
    for (std::size_t drained : {0U, 1U}) {
        SCOPED_TRACE(drained == 0 ? "left input drained" : "right input drained");
        auto [ins, survivor] = drained_input_and_open_survivor(drained);
        Dag dag;
        auto h = interval_join_of(ins.first, ins.second, dag);
        expect_two_input_drain_frees_time_without_ending_it(dag, h, *survivor);
    }
}

TEST(CloseFreesTime, ABroadcastConnectTreatsADrainAsAHandoffNotAnEnding) {
    for (std::size_t drained : {0U, 1U}) {
        SCOPED_TRACE(drained == 0 ? "main input drained" : "broadcast input drained");
        auto [ins, survivor] = drained_input_and_open_survivor(drained);
        Dag dag;
        auto h = broadcast_connect_of(ins.first, ins.second, dag);
        expect_two_input_drain_frees_time_without_ending_it(dag, h, *survivor);
    }
}

TEST(CloseFreesTime, ACoOperatorTreatsADrainAsAHandoffNotAnEnding) {
    for (std::size_t drained : {0U, 1U}) {
        SCOPED_TRACE(drained == 0 ? "first input drained" : "second input drained");
        auto [ins, survivor] = drained_input_and_open_survivor(drained);
        Dag dag;
        auto h = co_operator_of(ins.first, ins.second, dag);
        expect_two_input_drain_frees_time_without_ending_it(dag, h, *survivor);
    }
}
