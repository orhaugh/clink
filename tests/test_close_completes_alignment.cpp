// An input that closes can complete a pending barrier's alignment: the other
// inputs have delivered checkpoint 7, the last one finishes without it, and
// the aligner releases 7 at the close. Every multi-input runner used to drop
// that release on the floor, so the barrier never left the operator, its
// snapshot was never taken and the checkpoint could only time out. Each test
// drives one runner directly over pre-loaded, pre-closed inputs, so the order
// is fixed: the barrier on input 0, then input 1 closes without it.

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/core/codec.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"

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
