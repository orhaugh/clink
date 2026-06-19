// Durable sliding windows: the persistent SlidingWindowOperator survives
// snapshot/restore. Sliding is the one window family whose durable KeyedState
// path had no operator-level persistence coverage, and it is the most fragile -
// a single record fans into N overlapping (window_start, key) rows, so a
// rehydration/round-trip bug could drop or double-fire a subset of the
// overlapping windows silently. These tests drive a true destroy/recreate cycle
// across MULTIPLE keys and MULTIPLE overlapping windows and assert: the on-time
// pane does NOT double-fire after restore, next_pane_index continues
// monotonically, and the per-(window, key) aggregate spans the restore boundary
// for every overlapping window.
//
// Note on the durable model: SlidingWindowOperator is write-through (the hot
// path reads/writes the KeyedState backend directly; the in-memory map is used
// only when there is no backend). So restored state is read live from the
// backend rather than rehydrated into an in-memory map on open(), unlike the
// tumbling/session/evicting operators. These tests pin that contract.

#include <chrono>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/codec.hpp"
#include "clink/core/types.hpp"
#include "clink/operators/sliding_window_operator.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

using Key = std::string;
using Value = std::int64_t;
using Out = std::int64_t;
using Op = SlidingWindowOperator<Key, Value, Out>;

Op make_op() {
    return Op(
        2000ms,  // size: each record lands in 2 overlapping windows (size/slide)
        1000ms,  // slide
        [] { return Out{0}; },
        [](Out a, Value v) { return a + v; },
        string_codec(),
        int64_codec());
}

void send(Op& op, Emitter<std::pair<Key, Out>>& em, const Key& k, Value v, std::int64_t ts) {
    Batch<std::pair<Key, Value>> b;
    b.emplace(std::pair<Key, Value>{k, v}, EventTime{ts});
    op.process(StreamElement<std::pair<Key, Value>>::data(std::move(b)), em);
}

void watermark(Op& op, Emitter<std::pair<Key, Out>>& em, std::int64_t ts) {
    op.process(StreamElement<std::pair<Key, Value>>::watermark(Watermark{EventTime{ts}}), em);
}

void barrier(Op& op, Emitter<std::pair<Key, Out>>& em, std::uint64_t id) {
    op.process(StreamElement<std::pair<Key, Value>>::barrier(CheckpointBarrier{CheckpointId{id}}),
               em);
}

struct Pane {
    Key key;
    Out value{};
    std::int64_t end_ts{-1};  // window.max_timestamp() == end - 1
    std::int64_t pane_index{-1};
    bool is_first{false};
};

std::vector<Pane> panes(BoundedChannel<StreamElement<std::pair<Key, Out>>>& ch) {
    std::vector<Pane> out;
    while (auto e = ch.try_pop()) {
        if (e->is_data()) {
            for (const auto& r : e->as_data()) {
                Pane p;
                p.key = r.value().first;
                p.value = r.value().second;
                p.end_ts = r.event_time().has_value() ? r.event_time()->millis() : -1;
                if (r.pane().has_value()) {
                    p.pane_index = r.pane()->pane_index;
                    p.is_first = r.pane()->is_first;
                }
                out.push_back(p);
            }
        }
    }
    return out;
}

const Pane* find_pane(const std::vector<Pane>& ps, const Key& k, std::int64_t end_ts) {
    for (const auto& p : ps) {
        if (p.key == k && p.end_ts == end_ts) {
            return &p;
        }
    }
    return nullptr;
}

}  // namespace

// MANDATORY: across MULTIPLE keys and MULTIPLE overlapping windows, the on-time
// pane does not double-fire after restore, the per-(window, key) aggregate
// survives, and next_pane_index continues. A record at ts=1500 with size=2000,
// slide=1000 lands in [0,2000) and [1000,3000).
TEST(SlidingWindowPersistence, OverlappingWindowsSurviveRestoreNoDoubleFire) {
    const OperatorId op_id{91};
    InMemoryStateBackend backend_a;
    Op op_a = make_op();
    op_a.allowed_lateness(1000ms);  // [0,2000) stays alive (fired, not purged) until wm>=3000
    RuntimeContext ctx_a(op_id, "sliding", &backend_a, nullptr);
    op_a.attach_runtime(&ctx_a);
    op_a.open();

    BoundedChannel<StreamElement<std::pair<Key, Out>>> ch_a(256);
    Emitter<std::pair<Key, Out>> em_a(&ch_a);

    // Two keys, each landing in [0,2000) and [1000,3000).
    send(op_a, em_a, "a", 10, 1500);
    send(op_a, em_a, "b", 7, 1500);
    // Fire the [0,2000) windows on-time; [1000,3000) is still open (wm < 3000).
    watermark(op_a, em_a, 2000);
    auto first = panes(ch_a);
    ASSERT_EQ(first.size(), 2u) << "both keys fire their [0,2000) on-time pane";
    {
        const Pane* a = find_pane(first, "a", 1999);
        const Pane* b = find_pane(first, "b", 1999);
        ASSERT_NE(a, nullptr);
        ASSERT_NE(b, nullptr);
        EXPECT_EQ(a->value, 10);
        EXPECT_EQ(b->value, 7);
        EXPECT_EQ(a->pane_index, 0);
        EXPECT_TRUE(a->is_first);
    }

    barrier(op_a, em_a, 1);
    auto snap = backend_a.snapshot(CheckpointId{1});
    op_a.close();

    // Restore into a fresh backend + operator (same OperatorId).
    InMemoryStateBackend backend_b;
    backend_b.restore(snap);
    Op op_b = make_op();
    op_b.allowed_lateness(1000ms);
    RuntimeContext ctx_b(op_id, "sliding", &backend_b, nullptr);
    op_b.attach_runtime(&ctx_b);
    op_b.open();

    BoundedChannel<StreamElement<std::pair<Key, Out>>> ch_b(256);
    Emitter<std::pair<Key, Out>> em_b(&ch_b);

    // Replay the firing watermark: [0,2000) already fired, so NO new on-time pane.
    watermark(op_b, em_b, 2000);
    EXPECT_TRUE(panes(ch_b).empty())
        << "an already-fired sliding window must not re-fire on restore";

    // A late record into the fired [0,2000) re-fires: the sum proves the
    // pre-snapshot aggregate survived, and the pane index continues from 1. The
    // same record also accumulates into the still-open [1000,3000).
    send(op_b, em_b, "a", 5, 1500);
    auto late = panes(ch_b);
    ASSERT_EQ(late.size(), 1u) << "only the fired [0,2000) re-fires; [1000,3000) is still open";
    const Pane* a_late = find_pane(late, "a", 1999);
    ASSERT_NE(a_late, nullptr);
    EXPECT_EQ(a_late->value, 15) << "restored [0,2000) agg (10) + late 5 must both be present";
    EXPECT_EQ(a_late->pane_index, 1) << "next_pane_index must continue across restore";
    EXPECT_FALSE(a_late->is_first);

    // Advance past [1000,3000).end: both keys' second overlapping window fires
    // on-time, proving that window's aggregate also survived the restore.
    watermark(op_b, em_b, 3000);
    auto second = panes(ch_b);
    const Pane* a2 = find_pane(second, "a", 2999);
    const Pane* b2 = find_pane(second, "b", 2999);
    ASSERT_NE(a2, nullptr);
    ASSERT_NE(b2, nullptr);
    EXPECT_EQ(a2->value, 15) << "[1000,3000) a: restored 10 + post-restore 5";
    EXPECT_EQ(b2->value, 7) << "[1000,3000) b: restored 7";
    EXPECT_EQ(a2->pane_index, 0) << "first fire of [1000,3000) is pane 0";
    EXPECT_TRUE(a2->is_first);
    op_b.close();
}

// A fire-AND-purge (allowed_lateness=0) erases every overlapping window's
// durable row, so a restore finds nothing and nothing re-fires.
TEST(SlidingWindowPersistence, FireAndPurgeLeavesNoDurableRow) {
    const OperatorId op_id{92};
    InMemoryStateBackend backend_a;
    Op op_a = make_op();  // no allowed_lateness: on-time fire purges (purge_at == window_end)
    RuntimeContext ctx_a(op_id, "sliding", &backend_a, nullptr);
    op_a.attach_runtime(&ctx_a);
    op_a.open();

    BoundedChannel<StreamElement<std::pair<Key, Out>>> ch_a(256);
    Emitter<std::pair<Key, Out>> em_a(&ch_a);

    send(op_a, em_a, "a", 10, 1500);  // lands in [0,2000) and [1000,3000)
    // Advance past both windows' ends so each fires AND purges.
    watermark(op_a, em_a, 3000);
    EXPECT_EQ(panes(ch_a).size(), 2u) << "both overlapping windows fire once";
    barrier(op_a, em_a, 1);

    std::size_t rows = 0;
    backend_a.scan(op_id, [&](auto, auto) { ++rows; });
    EXPECT_EQ(rows, 0u) << "purged sliding windows must leave no durable rows";

    auto snap = backend_a.snapshot(CheckpointId{1});
    op_a.close();

    InMemoryStateBackend backend_b;
    backend_b.restore(snap);
    Op op_b = make_op();
    RuntimeContext ctx_b(op_id, "sliding", &backend_b, nullptr);
    op_b.attach_runtime(&ctx_b);
    op_b.open();
    BoundedChannel<StreamElement<std::pair<Key, Out>>> ch_b(256);
    Emitter<std::pair<Key, Out>> em_b(&ch_b);
    watermark(op_b, em_b, 4000);
    EXPECT_TRUE(panes(ch_b).empty()) << "a purged sliding window must not resurrect on restore";
    op_b.close();
}

// Construction-path symmetry: the in-memory default ctor writes nothing to a
// backend-equipped runtime (mirrors the evicting/tumbling contract).
TEST(SlidingWindowPersistence, DefaultCtorStaysInMemory) {
    const OperatorId op_id{93};
    InMemoryStateBackend backend;
    Op op(
        2000ms, 1000ms, [] { return Out{0}; }, [](Out a, Value v) { return a + v; });  // in-memory
    RuntimeContext ctx(op_id, "sliding", &backend, nullptr);
    op.attach_runtime(&ctx);
    op.open();

    BoundedChannel<StreamElement<std::pair<Key, Out>>> ch(256);
    Emitter<std::pair<Key, Out>> em(&ch);
    send(op, em, "a", 10, 1500);
    send(op, em, "b", 20, 1500);

    std::size_t rows = 0;
    backend.scan(op_id, [&](auto, auto) { ++rows; });
    EXPECT_EQ(rows, 0u) << "in-memory ctor must not touch the backend";
    op.close();
}
