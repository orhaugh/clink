// Three two-input runners hold state of their own: the co-operator behind
// every SQL join and user CoOperator, the interval join, and the broadcast
// runner. These tests put a barrier stamped Unaligned in front of them.
//
// They align it, as every multi-input runner does: the input that delivered
// the barrier pauses until the other input delivers it too, and the rows the
// waiting input still had ahead of its barrier are processed in the live run,
// inside the checkpoint. These runners used to drain those rows into an
// in-flight state slot when the first barrier arrived and never process them
// in the live run, so a SQL join under unaligned checkpoints lost them. No
// runner writes such a slot now. A checkpoint an earlier release took still
// carries one, and the runner replays it once at restore and erases it.
//
// The runners are driven directly with pre-loaded, pre-closed channels. The
// poll loop checks input 0 before input 1, so the order of every element is
// fixed and nothing depends on timing.

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
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
using namespace std::chrono_literals;
using V = std::int64_t;

namespace {

using Channel = BoundedChannel<StreamElement<V>>;

// Stateful co-op: records every value it actually processes per side, and
// how many it had processed when each checkpoint barrier reached it.
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
    void on_barrier(CheckpointBarrier barrier, Emitter<V>& out) override {
        left_seen_at_barrier.push_back(left_seen.size());
        right_seen_at_barrier.push_back(right_seen.size());
        CoOperator<V, V, V>::on_barrier(barrier, out);
    }
    std::string name() const override { return "recording_co_op"; }

    std::vector<V> left_seen;
    std::vector<V> right_seen;
    std::vector<std::size_t> left_seen_at_barrier;
    std::vector<std::size_t> right_seen_at_barrier;
};

std::shared_ptr<Channel> channel() {
    return std::make_shared<Channel>(64);
}

StreamElement<V> data_batch(std::vector<V> vals) {
    Batch<V> b;
    for (auto v : vals) {
        b.emplace(v);
    }
    return StreamElement<V>::data(std::move(b));
}

StreamElement<V> timed_row(V value, std::int64_t event_time_ms) {
    Batch<V> b;
    b.emplace(value, EventTime{event_time_ms});
    return StreamElement<V>::data(std::move(b));
}

StreamElement<V> barrier(std::uint64_t id, CheckpointBarrier::Mode mode) {
    return StreamElement<V>::barrier(CheckpointBarrier{CheckpointId{id}, /*terminal=*/false, mode});
}

void close_all(std::initializer_list<std::shared_ptr<Channel>> channels) {
    for (const auto& ch : channels) {
        ch->close();
    }
}

// What left a stage, in order: rows by value, barriers by id and mode.
// Watermarks are left out; the closes at the end free time, which is not what
// these tests are about.
template <typename T, typename Describe>
std::vector<std::string> trace(BoundedChannel<StreamElement<T>>& out, Describe describe) {
    std::vector<std::string> seen;
    while (auto e = out.try_pop()) {
        if (e->is_data()) {
            for (const auto& r : e->as_data()) {
                seen.push_back(describe(r.value()));
            }
        } else if (e->is_barrier()) {
            const auto b = e->as_barrier();
            seen.push_back(
                "barrier " + std::to_string(b.id().value()) +
                (b.mode() == CheckpointBarrier::Mode::Aligned ? " aligned" : " unaligned"));
        }
    }
    return seen;
}

std::string describe_row(V v) {
    return "row " + std::to_string(v);
}

std::string describe_pair(const std::pair<V, V>& p) {
    return "pair " + std::to_string(p.first) + " " + std::to_string(p.second);
}

bool has_slot(const StateBackend& backend, OperatorId id, const char* slot) {
    return backend.get(id, StateBackend::KeyView{slot}).has_value();
}

constexpr const char* kLeftInflight = "__co_op_left_inflight__";
constexpr const char* kRightInflight = "__co_op_right_inflight__";
constexpr const char* kJoinLeftInflight = "__interval_join_left_inflight__";
constexpr const char* kJoinRightInflight = "__interval_join_right_inflight__";
constexpr const char* kMainInflight = "__broadcast_main_inflight__";
constexpr const char* kBrodInflight = "__broadcast_brod_inflight__";

// An in-flight slot exactly as v0.10.0 and earlier wrote it: a u32 record
// count, then per record a presence byte for the event time (followed by the
// i64 event time when present), a u32 value length and the value's
// int64_codec bytes, all little-endian. Spelled out byte by byte rather than
// produced by the engine's serialiser, so a change to that serialiser cannot
// quietly change what these tests restore.
std::string earlier_release_slot(
    const std::vector<std::pair<V, std::optional<std::int64_t>>>& rows) {
    std::string out;
    const auto put_le = [&out](std::uint64_t v, int width) {
        for (int i = 0; i < width; ++i) {
            out.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
        }
    };
    put_le(rows.size(), 4);
    for (const auto& [value, event_time] : rows) {
        out.push_back(event_time.has_value() ? '\x01' : '\x00');
        if (event_time.has_value()) {
            put_le(static_cast<std::uint64_t>(*event_time), 8);
        }
        put_le(8, 4);
        put_le(static_cast<std::uint64_t>(value), 8);
    }
    return out;
}

void put_slot(StateBackend& backend, OperatorId id, const char* slot, const std::string& bytes) {
    backend.put(
        id, StateBackend::KeyView{slot}, StateBackend::ValueView{bytes.data(), bytes.size()});
}

StageHandle<std::pair<V, V>> add_join(Dag& dag,
                                      const std::shared_ptr<Channel>& left,
                                      const std::shared_ptr<Channel>& right) {
    return dag.interval_join<V, V, V, std::pair<V, V>>(
        StageHandle<V>{left, 0},
        StageHandle<V>{right, 0},
        [](const V&) -> V { return 0; },
        [](const V&) -> V { return 0; },
        100ms,
        100ms,
        [](const std::optional<V>& a, const std::optional<V>& b) {
            return std::make_pair(a.value_or(-1), b.value_or(-1));
        },
        Dag::JoinType::Inner,
        "unaligned_join",
        int64_codec(),
        int64_codec(),
        int64_codec());
}

// A broadcast runner whose callbacks record what they apply and emit each
// value downstream, so the output shows where each row fell relative to the
// barrier.
StageHandle<V> add_broadcast(Dag& dag,
                             const std::shared_ptr<Channel>& main,
                             const std::shared_ptr<Channel>& brod,
                             const std::shared_ptr<std::vector<V>>& brod_seen) {
    return dag.broadcast_process<V, V, V, V>(
        StageHandle<V>{main, 0},
        StageHandle<V>{brod, 0},
        [brod_seen](const V& v, BroadcastState<V>&, std::vector<V>& out) {
            brod_seen->push_back(v);
            out.push_back(v);
        },
        [](const V& v, BroadcastState<V>&, std::vector<V>& out) { out.push_back(v); },
        int64_codec(),
        "bcast",
        "broadcast_process",
        int64_codec(),
        int64_codec());
}

void run_runner(const Dag& dag, std::size_t runner_index, RuntimeContext& ctx) {
    dag.runners()[runner_index].run(ctx, [] { return false; });
}

}  // namespace

// ---- Live run: an unaligned barrier is aligned, nothing goes to a slot ----

TEST(CoOperatorUnaligned, TheWaitingInputsRowsAreProcessedLiveInsideTheCheckpoint) {
    // LEFT delivers barrier 1 first, then a row of the next interval. RIGHT
    // still has 10 and 20 ahead of its barrier, and 30 after it.
    InMemoryStateBackend backend;
    auto left = channel();
    auto right = channel();
    left->push(barrier(1, CheckpointBarrier::Mode::Unaligned));
    left->push(data_batch({5}));
    right->push(data_batch({10, 20}));
    right->push(barrier(1, CheckpointBarrier::Mode::Unaligned));
    right->push(data_batch({30}));
    close_all({left, right});

    Dag dag;
    auto op = std::make_shared<RecordingCoOp>();
    op->set_uid("co");
    auto h = dag.add_co_operator<V, V, V>(
        StageHandle<V>{left, 0}, StageHandle<V>{right, 0}, op, int64_codec(), int64_codec());
    const OperatorId id = dag.runners()[h.runner_index].id;
    RuntimeContext ctx(id, "recording_co_op", &backend, &MetricsRegistry::global());
    run_runner(dag, h.runner_index, ctx);

    EXPECT_EQ(op->right_seen, (std::vector<V>{10, 20, 30}))
        << "the waiting input's rows were drained into a slot instead of being processed";
    EXPECT_EQ(op->left_seen, (std::vector<V>{5}));
    // The cut: 10 and 20 inside checkpoint 1, 5 and 30 outside it.
    EXPECT_EQ(op->right_seen_at_barrier, (std::vector<std::size_t>{2}));
    EXPECT_EQ(op->left_seen_at_barrier, (std::vector<std::size_t>{0}));
    EXPECT_FALSE(has_slot(backend, id, kRightInflight));
    EXPECT_FALSE(has_slot(backend, id, kLeftInflight));
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

TEST(CoOperatorUnaligned, AnIntervalJoinJoinsTheWaitingInputsRowsLive) {
    // LEFT delivers barrier 7 first, then its row. RIGHT still has 10 and 11
    // ahead of its barrier, and 12 after it. All three are in the window of
    // the left row.
    InMemoryStateBackend backend;
    auto left = channel();
    auto right = channel();
    left->push(barrier(7, CheckpointBarrier::Mode::Unaligned));
    left->push(timed_row(1, 100));
    right->push(timed_row(10, 110));
    right->push(timed_row(11, 120));
    right->push(barrier(7, CheckpointBarrier::Mode::Unaligned));
    right->push(timed_row(12, 130));
    close_all({left, right});

    Dag dag;
    auto h = add_join(dag, left, right);
    const OperatorId id = dag.runners()[h.runner_index].id;
    RuntimeContext ctx(id, "unaligned_join", &backend, &MetricsRegistry::global());
    run_runner(dag, h.runner_index, ctx);

    EXPECT_EQ(
        trace(*h.output, describe_pair),
        (std::vector<std::string>{"barrier 7 aligned", "pair 1 10", "pair 1 11", "pair 1 12"}))
        << "the right input's rows were drained into a slot instead of being joined";
    EXPECT_FALSE(has_slot(backend, id, kJoinRightInflight));
    EXPECT_FALSE(has_slot(backend, id, kJoinLeftInflight));
}

TEST(CoOperatorUnaligned, TheBroadcastRunnerAppliesTheWaitingInputsRowsLive) {
    // MAIN delivers barrier 1 first, then a row of the next interval. The
    // BROADCAST input still has 100 and 200 ahead of its barrier, and 300
    // after it.
    InMemoryStateBackend backend;
    auto main = channel();
    auto brod = channel();
    main->push(barrier(1, CheckpointBarrier::Mode::Unaligned));
    main->push(data_batch({5}));
    brod->push(data_batch({100}));
    brod->push(data_batch({200}));
    brod->push(barrier(1, CheckpointBarrier::Mode::Unaligned));
    brod->push(data_batch({300}));
    close_all({main, brod});

    Dag dag;
    auto brod_seen = std::make_shared<std::vector<V>>();
    auto h = add_broadcast(dag, main, brod, brod_seen);
    const OperatorId id = dag.runners()[h.runner_index].id;
    RuntimeContext ctx(id, "broadcast_process", &backend, &MetricsRegistry::global());
    run_runner(dag, h.runner_index, ctx);

    EXPECT_EQ(*brod_seen, (std::vector<V>{100, 200, 300}))
        << "the broadcast input's rows were drained into a slot instead of being applied";
    EXPECT_EQ(
        trace(*h.output, describe_row),
        (std::vector<std::string>{"row 100", "row 200", "barrier 1 aligned", "row 5", "row 300"}));
    EXPECT_FALSE(has_slot(backend, id, kBrodInflight));
    EXPECT_FALSE(has_slot(backend, id, kMainInflight));
}

// ---- Restore: a slot an earlier release wrote replays once and is erased --

TEST(CoOperatorUnaligned, ACoOperatorReplaysAnEarlierReleasesInflightSlotOnce) {
    InMemoryStateBackend backend;
    auto left = channel();
    auto right = channel();

    Dag dag;
    auto op = std::make_shared<RecordingCoOp>();
    op->set_uid("co_restore");
    auto h = dag.add_co_operator<V, V, V>(
        StageHandle<V>{left, 0}, StageHandle<V>{right, 0}, op, int64_codec(), int64_codec());
    const OperatorId id = dag.runners()[h.runner_index].id;
    // The state a restore of an earlier release's unaligned checkpoint loads:
    // RIGHT's rows 10 and 20, captured when LEFT's barrier overtook them.
    put_slot(backend,
             id,
             kRightInflight,
             earlier_release_slot({{10, std::nullopt}, {20, std::nullopt}}));

    // The restored run takes checkpoint 2.
    left->push(barrier(2, CheckpointBarrier::Mode::Aligned));
    right->push(barrier(2, CheckpointBarrier::Mode::Aligned));
    close_all({left, right});
    std::optional<Snapshot> next_cut;
    RuntimeContext ctx(id, "recording_co_op", &backend, &MetricsRegistry::global());
    ctx.set_checkpoint_ack([&](CheckpointId ckpt, bool ok, std::string error) {
        EXPECT_TRUE(ok) << error;
        next_cut = backend.snapshot(ckpt);
    });
    run_runner(dag, h.runner_index, ctx);

    EXPECT_EQ(op->right_seen, (std::vector<V>{10, 20}));
    EXPECT_EQ(op->right_seen_at_barrier, (std::vector<std::size_t>{2}))
        << "the replayed rows belong ahead of the restored run's first checkpoint";
    EXPECT_FALSE(has_slot(backend, id, kRightInflight)) << "the slot outlived its replay";

    // A restore from checkpoint 2 does not replay them a second time.
    ASSERT_TRUE(next_cut.has_value()) << "checkpoint 2 was not acknowledged";
    InMemoryStateBackend restored;
    restored.restore(*next_cut);
    EXPECT_FALSE(has_slot(restored, id, kRightInflight));
    auto left2 = channel();
    auto right2 = channel();
    close_all({left2, right2});
    Dag dag2;
    auto op2 = std::make_shared<RecordingCoOp>();
    op2->set_uid("co_restore");
    auto h2 = dag2.add_co_operator<V, V, V>(
        StageHandle<V>{left2, 0}, StageHandle<V>{right2, 0}, op2, int64_codec(), int64_codec());
    ASSERT_EQ(dag2.runners()[h2.runner_index].id, id) << "uid must give a stable id across runs";
    RuntimeContext ctx2(id, "recording_co_op", &restored, &MetricsRegistry::global());
    run_runner(dag2, h2.runner_index, ctx2);
    EXPECT_TRUE(op2->right_seen.empty()) << "a slot replayed twice";
}

TEST(CoOperatorUnaligned, AnIntervalJoinReplaysAnEarlierReleasesInflightSlotOnce) {
    InMemoryStateBackend backend;
    auto left = channel();
    auto right = channel();
    Dag dag;
    auto h = add_join(dag, left, right);
    const OperatorId id = dag.runners()[h.runner_index].id;
    put_slot(backend, id, kJoinRightInflight, earlier_release_slot({{99, 150}}));

    left->push(timed_row(7, 100));
    close_all({left, right});
    RuntimeContext ctx(id, "unaligned_join", &backend, &MetricsRegistry::global());
    run_runner(dag, h.runner_index, ctx);

    EXPECT_EQ(trace(*h.output, describe_pair), (std::vector<std::string>{"pair 7 99"}))
        << "the restored right row did not join the live left row exactly once";
    EXPECT_FALSE(has_slot(backend, id, kJoinRightInflight)) << "the slot outlived its replay";
}

TEST(CoOperatorUnaligned, TheBroadcastRunnerReplaysAnEarlierReleasesInflightSlotOnce) {
    InMemoryStateBackend backend;
    auto main = channel();
    auto brod = channel();
    Dag dag;
    auto brod_seen = std::make_shared<std::vector<V>>();
    auto h = add_broadcast(dag, main, brod, brod_seen);
    const OperatorId id = dag.runners()[h.runner_index].id;
    put_slot(backend,
             id,
             kBrodInflight,
             earlier_release_slot({{100, std::nullopt}, {200, std::nullopt}}));

    main->push(barrier(2, CheckpointBarrier::Mode::Aligned));
    brod->push(barrier(2, CheckpointBarrier::Mode::Aligned));
    close_all({main, brod});
    RuntimeContext ctx(id, "broadcast_process", &backend, &MetricsRegistry::global());
    run_runner(dag, h.runner_index, ctx);

    EXPECT_EQ(*brod_seen, (std::vector<V>{100, 200}));
    EXPECT_EQ(trace(*h.output, describe_row),
              (std::vector<std::string>{"row 100", "row 200", "barrier 2 aligned"}))
        << "the replayed rows belong ahead of the restored run's first checkpoint";
    EXPECT_FALSE(has_slot(backend, id, kBrodInflight)) << "the slot outlived its replay";

    // The next run over the same state replays nothing.
    auto main2 = channel();
    auto brod2 = channel();
    close_all({main2, brod2});
    Dag dag2;
    auto brod_seen2 = std::make_shared<std::vector<V>>();
    auto h2 = add_broadcast(dag2, main2, brod2, brod_seen2);
    ASSERT_EQ(dag2.runners()[h2.runner_index].id, id);
    RuntimeContext ctx2(id, "broadcast_process", &backend, &MetricsRegistry::global());
    run_runner(dag2, h2.runner_index, ctx2);
    EXPECT_TRUE(brod_seen2->empty()) << "a slot replayed twice";
}
