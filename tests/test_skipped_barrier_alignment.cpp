// A checkpoint barrier that one input of a fan-in never delivers can never
// align there, and the fan-in must not wait for it.
//
// Barriers reach an input in id order. Once an input delivers checkpoint 8
// without having delivered 7, it never will, so 7 cannot align at the operator
// that merges it with the others. The aligner used to keep the inputs that had
// delivered 7 paused for it, and then paused this one too, on 8: every input
// paused, none was read again, and the operator and everything downstream of
// it stopped for good while the job still reported itself running. A source
// upstream that never emitted a checkpoint's barrier is the usual cause (see
// test_source_trigger_handoff.cpp for the one that stalled the broker-outage
// recovery gate). The aligner now drops a barrier an input went past without
// delivering, releases the inputs paused on it, and goes on to the next
// checkpoint, which completes; the skipped one cannot.
//
// The runner cases drive one runner directly over pre-loaded, pre-closed
// inputs, as test_close_completes_alignment.cpp does, with a stop predicate
// that gives up after a bound: a runner that waits for the skipped barrier
// runs into the bound instead of hanging the suite.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/metrics/checkpoint_metrics.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/multi_input_alignment.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"

#include "tests/test_helpers/sanitizer_slack.hpp"

using namespace clink;

namespace {

using V = std::int64_t;
using Ch = BoundedChannel<StreamElement<V>>;

CheckpointBarrier skip_aligned(std::uint64_t id) {
    return CheckpointBarrier{
        CheckpointId{id}, /*terminal=*/false, CheckpointBarrier::Mode::Aligned};
}

Batch<V> skip_batch(V v) {
    Batch<V> b;
    b.emplace(v);
    return b;
}

StreamElement<V> skip_value(V v) {
    return StreamElement<V>::data(skip_batch(v));
}

// Runs one runner until its inputs close, or until the bound runs out; true
// when it finished on its own.
bool skip_run_runner(Dag& dag, std::size_t index, InMemoryStateBackend& backend) {
    const auto& runner = dag.runners()[index];
    RuntimeContext ctx(runner.id, runner.name, &backend, &MetricsRegistry::global());
    const auto deadline = std::chrono::steady_clock::now() +
                          clink::test_support::scale_slack(std::chrono::milliseconds{3000});
    std::atomic<bool> gave_up{false};
    runner.run(ctx, [&] {
        if (std::chrono::steady_clock::now() < deadline) {
            return false;
        }
        gave_up.store(true);
        return true;
    });
    return !gave_up.load();
}

struct SkipOutput {
    std::multiset<V> values;
    std::vector<std::uint64_t> barriers;
};

SkipOutput skip_drain(Ch& out) {
    SkipOutput o;
    while (auto m = out.try_pop()) {
        if (m->is_data()) {
            for (const auto& r : m->as_data()) {
                o.values.insert(r.value());
            }
        } else if (m->is_barrier()) {
            o.barriers.push_back(m->as_barrier().id().value());
        }
    }
    return o;
}

class SkipRecordingSink final : public Sink<V> {
public:
    void on_data(const Batch<V>& batch) override {
        for (const auto& r : batch) {
            values.insert(r.value());
        }
    }
    void on_barrier(CheckpointBarrier b) override { barriers.push_back(b.id().value()); }
    std::string name() const override { return "skip_recording_sink"; }
    std::multiset<V> values;
    std::vector<std::uint64_t> barriers;
};

class SkipRecordingOp final : public Operator<V, V> {
public:
    void process(const StreamElement<V>& el, Emitter<V>& out) override {
        if (el.is_data()) {
            for (const auto& r : el.as_data()) {
                values.insert(r.value());
            }
        }
        out.emit(el);
    }
    void on_barrier(CheckpointBarrier b, Emitter<V>& out) override {
        barriers.push_back(b.id().value());
        Operator<V, V>::on_barrier(b, out);
    }
    std::string name() const override { return "skip_recording_op"; }
    std::multiset<V> values;
    std::vector<std::uint64_t> barriers;
};

class SkipNeverRunSource final : public Source<V> {
public:
    bool produce(Emitter<V>& /*out*/) override { return false; }
    std::string name() const override { return "skip_never_run_source"; }
};

}  // namespace

TEST(SkippedBarrier, AnInputThatGoesPastAPendingBarrierReleasesTheInputsPausedOnIt) {
    MultiInputAlignment a(2);
    EXPECT_FALSE(a.on_barrier(0, skip_aligned(7)).forward);
    EXPECT_TRUE(a.input_paused(0));
    // Input 1 delivers 8 without 7: it will never deliver 7.
    EXPECT_FALSE(a.on_barrier(1, skip_aligned(8)).forward);
    EXPECT_FALSE(a.input_paused(0)) << "input 0 is still paused on checkpoint 7, which input 1 "
                                       "went past; with input 1 paused on 8 nothing is read again";
    EXPECT_TRUE(a.input_paused(1));
    EXPECT_EQ(a.skipped_barriers(), 1U);
    const auto adv = a.on_barrier(0, skip_aligned(8));
    ASSERT_TRUE(adv.forward);
    EXPECT_EQ(adv.barrier.id().value(), 8U);
    EXPECT_FALSE(a.input_paused(0));
    EXPECT_FALSE(a.input_paused(1));
}

TEST(SkippedBarrier, ABarrierAnotherInputAlreadyWentPastIsDroppedWithoutPausing) {
    MultiInputAlignment a(2);
    EXPECT_FALSE(a.on_barrier(1, skip_aligned(8)).forward);
    // Input 0 delivers 7, which input 1 already went past.
    EXPECT_FALSE(a.on_barrier(0, skip_aligned(7)).forward);
    EXPECT_FALSE(a.input_paused(0)) << "input 0 paused on a checkpoint that cannot align";
    EXPECT_EQ(a.skipped_barriers(), 1U);
    const auto adv = a.on_barrier(0, skip_aligned(8));
    ASSERT_TRUE(adv.forward);
    EXPECT_EQ(adv.barrier.id().value(), 8U);
}

TEST(SkippedBarrier, ABarrierAtOrBelowOneItsOwnInputDeliveredIsDropped) {
    MultiInputAlignment a(2);
    EXPECT_FALSE(a.on_barrier(0, skip_aligned(7)).forward);
    ASSERT_TRUE(a.on_barrier(1, skip_aligned(7)).forward);
    // A repeat of 7, and an older 6, on input 0: out of order on its channel.
    EXPECT_FALSE(a.on_barrier(0, skip_aligned(7)).forward);
    EXPECT_FALSE(a.on_barrier(0, skip_aligned(6)).forward);
    EXPECT_FALSE(a.input_paused(0));
    EXPECT_EQ(a.skipped_barriers(), 2U);
    EXPECT_FALSE(a.on_barrier(0, skip_aligned(8)).forward);
    EXPECT_TRUE(a.on_barrier(1, skip_aligned(8)).forward);
}

TEST(SkippedBarrier, EveryInputPausedOnTheSkippedBarrierIsReleased) {
    MultiInputAlignment a(3);
    EXPECT_FALSE(a.on_barrier(0, skip_aligned(7)).forward);
    EXPECT_FALSE(a.on_barrier(1, skip_aligned(7)).forward);
    EXPECT_FALSE(a.on_barrier(2, skip_aligned(8)).forward);
    EXPECT_FALSE(a.input_paused(0));
    EXPECT_FALSE(a.input_paused(1));
    EXPECT_TRUE(a.input_paused(2));
    EXPECT_FALSE(a.on_barrier(0, skip_aligned(8)).forward);
    const auto adv = a.on_barrier(1, skip_aligned(8));
    ASSERT_TRUE(adv.forward);
    EXPECT_EQ(adv.barrier.id().value(), 8U);
}

TEST(SkippedBarrier, AnInputThatEndedWithATerminalBarrierHasGonePastNothing) {
    // A bounded source ends with the terminal sentinel while its sibling goes
    // on checkpointing. The close behind the terminal barrier completes the
    // sibling's checkpoint, as it always did; nothing is dropped.
    MultiInputAlignment a(2);
    const CheckpointBarrier terminal{CheckpointId{UINT64_MAX},
                                     /*terminal=*/true,
                                     CheckpointBarrier::Mode::Aligned};
    EXPECT_FALSE(a.on_barrier(0, terminal).forward);
    EXPECT_FALSE(a.on_barrier(1, skip_aligned(10)).forward);
    EXPECT_EQ(a.skipped_barriers(), 0U);
    const auto adv = a.on_input_closed(0);
    ASSERT_TRUE(adv.forward);
    EXPECT_EQ(adv.barrier.id().value(), 10U);
}

TEST(SkippedBarrier, AClosedInputHasGonePastNothing) {
    MultiInputAlignment a(2);
    EXPECT_FALSE(a.on_barrier(1, skip_aligned(8)).forward);
    EXPECT_FALSE(a.on_input_closed(1).forward);
    // 7 aligns at once: the only input still open delivered it.
    const auto seven = a.on_barrier(0, skip_aligned(7));
    ASSERT_TRUE(seven.forward);
    EXPECT_EQ(seven.barrier.id().value(), 7U);
    const auto eight = a.on_barrier(0, skip_aligned(8));
    ASSERT_TRUE(eight.forward);
    EXPECT_EQ(eight.barrier.id().value(), 8U);
    EXPECT_EQ(a.skipped_barriers(), 0U);
}

TEST(SkippedBarrier, EachDroppedBarrierIsCountedAndOnlyTheFirstIsNoticed) {
    namespace mm = clink::metrics;
    MetricsRegistry registry;
    MultiInputAlignment a(2);
    const std::uint64_t op_id = 4277;
    a.set_operator_id(op_id, &registry);
    std::vector<std::pair<std::uint64_t, std::uint64_t>> noticed;
    a.set_skipped_barrier_notice(
        [&noticed](std::uint64_t ck, std::uint64_t past) { noticed.emplace_back(ck, past); });

    EXPECT_FALSE(a.on_barrier(0, skip_aligned(7)).forward);
    EXPECT_FALSE(a.on_barrier(1, skip_aligned(8)).forward);  // drops 7
    EXPECT_TRUE(a.on_barrier(0, skip_aligned(8)).forward);
    EXPECT_FALSE(a.on_barrier(1, skip_aligned(10)).forward);
    EXPECT_FALSE(a.on_barrier(0, skip_aligned(9)).forward);  // 1 went past 9
    EXPECT_TRUE(a.on_barrier(0, skip_aligned(10)).forward);

    EXPECT_EQ(a.skipped_barriers(), 2U);
    EXPECT_EQ(registry.counter(mm::op_metric_name("barrier_skipped_total", op_id)).value(), 2U);
    ASSERT_EQ(noticed.size(), 1U) << "the notice is a once-per-runner log line";
    EXPECT_EQ(noticed[0], (std::pair<std::uint64_t, std::uint64_t>{7, 8}));
}

// Input 0 delivers 7 and 8; input 1 never delivered 7. Each runner must go on
// from 8, pass every record, and finish when both inputs close.
TEST(SkippedBarrier, AUnionGoesOnFromTheNextCheckpoint) {
    auto in0 = std::make_shared<Ch>(16);
    auto in1 = std::make_shared<Ch>(16);
    in0->push(StreamElement<V>::barrier(skip_aligned(7)));
    in0->push(skip_value(1));
    in0->push(StreamElement<V>::barrier(skip_aligned(8)));
    in0->push(skip_value(2));
    in0->close();
    in1->push(skip_value(10));
    in1->push(StreamElement<V>::barrier(skip_aligned(8)));
    in1->push(skip_value(11));
    in1->close();
    Dag dag;
    auto h = dag.union_streams<V>(
        std::vector<StageHandle<V>>{StageHandle<V>{in0, 0}, StageHandle<V>{in1, 0}});
    InMemoryStateBackend backend;
    EXPECT_TRUE(skip_run_runner(dag, h.runner_index, backend))
        << "the union waited for checkpoint 7, which input 1 went past";
    const auto out = skip_drain(*h.output);
    EXPECT_EQ(out.barriers, (std::vector<std::uint64_t>{8}));
    EXPECT_EQ(out.values, (std::multiset<V>{1, 2, 10, 11}));
}

TEST(SkippedBarrier, AFanInSinkSeesTheNextCheckpoint) {
    Dag dag;
    auto upstream = dag.add_parallel_source<V>(
        [](std::size_t) { return std::make_shared<SkipNeverRunSource>(); }, /*parallelism=*/2);
    auto sink = std::make_shared<SkipRecordingSink>();
    dag.add_parallel_sink<V>(upstream, [sink](std::size_t) { return sink; }, /*parallelism=*/1);
    auto& e0 = *upstream.emitters[0];
    auto& e1 = *upstream.emitters[1];
    e0.emit_barrier(skip_aligned(7));
    e0.emit_data(skip_batch(1));
    e0.emit_barrier(skip_aligned(8));
    e0.emit_data(skip_batch(2));
    e0.close_all();
    e1.emit_data(skip_batch(10));
    e1.emit_barrier(skip_aligned(8));
    e1.emit_data(skip_batch(11));
    e1.close_all();

    InMemoryStateBackend backend;
    EXPECT_TRUE(skip_run_runner(dag, dag.runners().size() - 1, backend))
        << "the sink's runner waited for checkpoint 7, which one input went past";
    EXPECT_EQ(sink->barriers, (std::vector<std::uint64_t>{8}));
    EXPECT_EQ(sink->values, (std::multiset<V>{1, 2, 10, 11}));
}

TEST(SkippedBarrier, AnOperatorFedBySeveralSubtasksSeesTheNextCheckpoint) {
    Dag dag;
    auto upstream = dag.add_parallel_source<V>(
        [](std::size_t) { return std::make_shared<SkipNeverRunSource>(); }, /*parallelism=*/2);
    auto op = std::make_shared<SkipRecordingOp>();
    auto downstream = dag.add_parallel_operator_shuffled<V, V>(
        upstream,
        [op](std::size_t) { return op; },
        /*parallelism=*/1,
        [](const V&) -> std::size_t { return 0; });
    auto& e0 = *upstream.emitters[0];
    auto& e1 = *upstream.emitters[1];
    e0.emit_barrier(skip_aligned(7));
    e0.emit_data(skip_batch(1));
    e0.emit_barrier(skip_aligned(8));
    e0.emit_data(skip_batch(2));
    e0.close_all();
    e1.emit_data(skip_batch(10));
    e1.emit_barrier(skip_aligned(8));
    e1.emit_data(skip_batch(11));
    e1.close_all();

    const auto& runners = dag.runners();
    const auto it = std::find_if(runners.begin(), runners.end(), [&](const auto& r) {
        return r.id == downstream.subtask_ids[0];
    });
    ASSERT_NE(it, runners.end());
    InMemoryStateBackend backend;
    EXPECT_TRUE(skip_run_runner(dag, static_cast<std::size_t>(it - runners.begin()), backend))
        << "the operator's runner waited for checkpoint 7, which one input went past";
    EXPECT_EQ(op->barriers, (std::vector<std::uint64_t>{8}));
    EXPECT_EQ(op->values, (std::multiset<V>{1, 2, 10, 11}));
}
