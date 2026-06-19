// Unaligned-checkpoint in-flight capture for the generic two-input
// co-operator runner (Dag::add_co_operator), which backs every SQL join
// and user CoOperator. When the FIRST barrier arrives on one input under
// unaligned mode, the runner snapshots immediately and forwards; the
// other input's already-queued pre-barrier records would be lost on
// restore, so they are drained into a reserved state slot that the
// snapshot captures and replayed on restore.
//
// These tests drive the runner directly with pre-loaded, pre-closed
// channels. That is fully deterministic: the poll loop always checks
// input 0 before input 1, so a barrier pre-loaded on input 0 is processed
// before input 1 is touched, and the records pre-loaded on input 1 are
// exactly the in-flight set. No threads, no timing.

#include <cstdint>
#include <memory>
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
using V = std::int64_t;

namespace {

// Stateful co-op: records every value it actually processes per side, so
// the test can tell "drained into in-flight" (not processed) from
// "processed via process_element2".
class RecordingCoOp final : public CoOperator<V, V, V> {
public:
    void process_element1(const StreamElement<V>& el, Emitter<V>&) override {
        for (const auto& r : el.as_data()) {
            left_seen.push_back(r.value());
        }
    }
    void process_element2(const StreamElement<V>& el, Emitter<V>&) override {
        for (const auto& r : el.as_data()) {
            right_seen.push_back(r.value());
        }
    }
    std::string name() const override { return "recording_co_op"; }

    std::vector<V> left_seen;
    std::vector<V> right_seen;
};

StreamElement<V> data_batch(std::vector<V> vals) {
    Batch<V> b;
    for (auto v : vals) {
        b.emplace(v);
    }
    return StreamElement<V>::data(std::move(b));
}

constexpr const char* kRightInflight = "__co_op_right_inflight__";

}  // namespace

TEST(CoOperatorUnaligned, CapturesPendingInputInflightAndReplaysOnRestore) {
    auto backend1 = std::make_shared<InMemoryStateBackend>();

    // ---- Phase 1: unaligned barrier on LEFT; RIGHT has queued 10, 20. ----
    auto left1 = std::make_shared<BoundedChannel<StreamElement<V>>>(64);
    auto right1 = std::make_shared<BoundedChannel<StreamElement<V>>>(64);

    Dag dag1;
    auto op1 = std::make_shared<RecordingCoOp>();
    op1->set_uid("co");  // stable id across the two runs
    auto h1 = dag1.add_co_operator<V, V, V>(
        StageHandle<V>{left1, 0}, StageHandle<V>{right1, 0}, op1, int64_codec(), int64_codec());
    const OperatorId id = dag1.runners()[h1.runner_index].id;

    right1->push(data_batch({10, 20}));
    left1->push(StreamElement<V>::barrier(CheckpointBarrier{
        CheckpointId{1}, /*terminal=*/false, CheckpointBarrier::Mode::Unaligned}));
    left1->close();
    right1->close();

    RuntimeContext ctx1(id, "recording_co_op", backend1.get(), &MetricsRegistry::global());
    dag1.runners()[h1.runner_index].run(ctx1, [] { return false; });

    // Right's queued records were drained into the in-flight slot, NOT
    // processed through process_element2.
    EXPECT_TRUE(op1->right_seen.empty());
    auto stored = backend1->get(id, StateBackend::KeyView{kRightInflight});
    ASSERT_TRUE(stored.has_value()) << "pending-input in-flight was not captured";

    // ---- Phase 2: snapshot -> restore into a fresh backend -> a fresh
    // runner with no live input replays the captured in-flight exactly once.
    auto snap = backend1->snapshot(CheckpointId{1});
    auto backend2 = std::make_shared<InMemoryStateBackend>();
    backend2->restore(snap);

    auto left2 = std::make_shared<BoundedChannel<StreamElement<V>>>(64);
    auto right2 = std::make_shared<BoundedChannel<StreamElement<V>>>(64);
    Dag dag2;
    auto op2 = std::make_shared<RecordingCoOp>();
    op2->set_uid("co");
    auto h2 = dag2.add_co_operator<V, V, V>(
        StageHandle<V>{left2, 0}, StageHandle<V>{right2, 0}, op2, int64_codec(), int64_codec());
    ASSERT_EQ(dag2.runners()[h2.runner_index].id, id) << "uid must give a stable id across runs";
    left2->close();
    right2->close();

    RuntimeContext ctx2(id, "recording_co_op", backend2.get(), &MetricsRegistry::global());
    dag2.runners()[h2.runner_index].run(ctx2, [] { return false; });

    // The captured 10, 20 are replayed through process_element2 exactly once.
    EXPECT_EQ(op2->right_seen, (std::vector<V>{10, 20}));
    EXPECT_TRUE(op2->left_seen.empty());
    // And the slot is consumed so a later snapshot can't double-replay.
    EXPECT_FALSE(backend2->get(id, StateBackend::KeyView{kRightInflight}).has_value());
}

TEST(CoOperatorUnaligned, AlignedBarrierDoesNotCaptureInflight) {
    // Contrast: under ALIGNED mode the runner waits for both barriers, so
    // the right record is processed normally and nothing is stashed as
    // in-flight.
    auto backend = std::make_shared<InMemoryStateBackend>();
    auto left = std::make_shared<BoundedChannel<StreamElement<V>>>(64);
    auto right = std::make_shared<BoundedChannel<StreamElement<V>>>(64);

    Dag dag;
    auto op = std::make_shared<RecordingCoOp>();
    op->set_uid("co_aligned");
    auto h = dag.add_co_operator<V, V, V>(
        StageHandle<V>{left, 0}, StageHandle<V>{right, 0}, op, int64_codec(), int64_codec());
    const OperatorId id = dag.runners()[h.runner_index].id;

    right->push(data_batch({7}));
    left->push(StreamElement<V>::barrier(
        CheckpointBarrier{CheckpointId{1}, /*terminal=*/false, CheckpointBarrier::Mode::Aligned}));
    right->push(StreamElement<V>::barrier(
        CheckpointBarrier{CheckpointId{1}, /*terminal=*/false, CheckpointBarrier::Mode::Aligned}));
    left->close();
    right->close();

    RuntimeContext ctx(id, "recording_co_op", backend.get(), &MetricsRegistry::global());
    dag.runners()[h.runner_index].run(ctx, [] { return false; });

    EXPECT_EQ(op->right_seen, (std::vector<V>{7}));  // processed, not stashed
    EXPECT_FALSE(backend->get(id, StateBackend::KeyView{kRightInflight}).has_value());
}

// Same property for the broadcast runner: a barrier on the MAIN input
// under unaligned mode captures the broadcast input's queued control
// records as in-flight and replays them on restore.
TEST(CoOperatorUnaligned, BroadcastCapturesPendingInputInflightAndReplaysOnRestore) {
    using S = std::int64_t;  // broadcast state type (unused by the callbacks here)
    constexpr const char* kBrodInflight = "__broadcast_brod_inflight__";

    // ---- Phase 1: unaligned barrier on MAIN; BROADCAST has queued 100,200.
    auto backend1 = std::make_shared<InMemoryStateBackend>();
    auto brod_seen1 = std::make_shared<std::vector<V>>();
    auto main1 = std::make_shared<BoundedChannel<StreamElement<V>>>(64);
    auto brod1 = std::make_shared<BoundedChannel<StreamElement<V>>>(64);

    Dag dag1;
    auto h1 = dag1.broadcast_process<V, V, V, S>(
        StageHandle<V>{main1, 0},
        StageHandle<V>{brod1, 0},
        [brod_seen1](const V& v, BroadcastState<S>&, std::vector<V>&) { brod_seen1->push_back(v); },
        [](const V&, BroadcastState<S>&, std::vector<V>&) {},
        int64_codec(),
        "bcast",
        "broadcast_process",
        int64_codec(),
        int64_codec());
    const OperatorId id = dag1.runners()[h1.runner_index].id;

    brod1->push(data_batch({100, 200}));
    main1->push(StreamElement<V>::barrier(CheckpointBarrier{
        CheckpointId{1}, /*terminal=*/false, CheckpointBarrier::Mode::Unaligned}));
    main1->close();
    brod1->close();

    RuntimeContext ctx1(id, "broadcast_process", backend1.get(), &MetricsRegistry::global());
    dag1.runners()[h1.runner_index].run(ctx1, [] { return false; });

    EXPECT_TRUE(brod_seen1->empty());  // drained into in-flight, not applied
    ASSERT_TRUE(backend1->get(id, StateBackend::KeyView{kBrodInflight}).has_value());

    // ---- Phase 2: restore replays the captured broadcast records.
    auto snap = backend1->snapshot(CheckpointId{1});
    auto backend2 = std::make_shared<InMemoryStateBackend>();
    backend2->restore(snap);
    auto brod_seen2 = std::make_shared<std::vector<V>>();
    auto main2 = std::make_shared<BoundedChannel<StreamElement<V>>>(64);
    auto brod2 = std::make_shared<BoundedChannel<StreamElement<V>>>(64);

    Dag dag2;
    auto h2 = dag2.broadcast_process<V, V, V, S>(
        StageHandle<V>{main2, 0},
        StageHandle<V>{brod2, 0},
        [brod_seen2](const V& v, BroadcastState<S>&, std::vector<V>&) { brod_seen2->push_back(v); },
        [](const V&, BroadcastState<S>&, std::vector<V>&) {},
        int64_codec(),
        "bcast",
        "broadcast_process",
        int64_codec(),
        int64_codec());
    ASSERT_EQ(dag2.runners()[h2.runner_index].id, id);
    main2->close();
    brod2->close();

    RuntimeContext ctx2(id, "broadcast_process", backend2.get(), &MetricsRegistry::global());
    dag2.runners()[h2.runner_index].run(ctx2, [] { return false; });

    EXPECT_EQ(*brod_seen2, (std::vector<V>{100, 200}));
    EXPECT_FALSE(backend2->get(id, StateBackend::KeyView{kBrodInflight}).has_value());
}
