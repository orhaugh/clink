// Durable evicting windows (FOUND-2): the persistent
// EvictingTumblingWindowOperator survives snapshot/restore. Proves the buffered
// raw records are not lost across a restart, the MANDATORY no-double-fire (a
// buffer that fired before the snapshot does not re-emit its on-time pane after
// restore), and that next_pane_index continues monotonically (a post-restore
// late re-fire is is_first=false with the continued index).

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/codec.hpp"
#include "clink/core/types.hpp"
#include "clink/operators/evicting_tumbling_window_operator.hpp"
#include "clink/operators/window_evictor.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

using Key = std::string;
using Value = std::int64_t;
using Out = std::int64_t;
using Op = EvictingTumblingWindowOperator<Key, Value, Out>;

// Sum the (post-eviction) records in the window.
auto sum_records = [](const std::vector<Record<Value>>& recs, const TimeWindow&) -> Out {
    Out s = 0;
    for (const auto& r : recs) {
        s += r.value();
    }
    return s;
};

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
    Out value{};
    std::int64_t pane_index{-1};
    bool is_first{false};
};

std::vector<Pane> panes(BoundedChannel<StreamElement<std::pair<Key, Out>>>& ch) {
    std::vector<Pane> out;
    while (auto e = ch.try_pop()) {
        if (e->is_data()) {
            for (const auto& r : e->as_data()) {
                Pane p;
                p.value = r.value().second;
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

}  // namespace

// MANDATORY: buffered records survive restore, the on-time pane does NOT
// double-fire, and a post-restore late re-fire continues the pane index.
TEST(EvictingWindowPersistence, BufferSurvivesRestoreNoDoubleFire) {
    const OperatorId op_id{77};
    InMemoryStateBackend backend_a;
    Op op_a(1000ms,
            sum_records,
            std::make_unique<CountEvictor<Value>>(10),
            string_codec(),
            int64_codec());
    op_a.allowed_lateness(500ms);  // buffer survives the fire (fired, not purged)
    RuntimeContext ctx_a(op_id, "evicting", &backend_a, nullptr);
    op_a.attach_runtime(&ctx_a);
    op_a.open();

    BoundedChannel<StreamElement<std::pair<Key, Out>>> ch_a(256);
    Emitter<std::pair<Key, Out>> em_a(&ch_a);

    // Three records in window [0, 1000).
    send(op_a, em_a, "k", 10, 100);
    send(op_a, em_a, "k", 20, 200);
    send(op_a, em_a, "k", 30, 300);
    watermark(op_a, em_a, 1000);  // on-time fire: pane sum=60, fired=true, not purged (<1500)
    auto first = panes(ch_a);
    ASSERT_EQ(first.size(), 1u);
    EXPECT_EQ(first[0].value, 60);
    EXPECT_EQ(first[0].pane_index, 0);
    EXPECT_TRUE(first[0].is_first);

    barrier(op_a, em_a, 1);
    auto snap = backend_a.snapshot(CheckpointId{1});  // captures records + fired + next_pane_index
    op_a.close();

    // Restore into a fresh backend + operator (same OperatorId).
    InMemoryStateBackend backend_b;
    backend_b.restore(snap);
    Op op_b(1000ms,
            sum_records,
            std::make_unique<CountEvictor<Value>>(10),
            string_codec(),
            int64_codec());
    op_b.allowed_lateness(500ms);
    RuntimeContext ctx_b(op_id, "evicting", &backend_b, nullptr);
    op_b.attach_runtime(&ctx_b);
    op_b.open();

    BoundedChannel<StreamElement<std::pair<Key, Out>>> ch_b(256);
    Emitter<std::pair<Key, Out>> em_b(&ch_b);

    // Replay a firing watermark: the buffer already fired, so NO new on-time pane.
    watermark(op_b, em_b, 1100);
    EXPECT_TRUE(panes(ch_b).empty()) << "an already-fired buffer must not re-fire its on-time pane";

    // A late record (within window + lateness) re-fires: the sum proves the
    // pre-snapshot records survived, and the pane index continues from 1.
    send(op_b, em_b, "k", 5, 950);
    auto late = panes(ch_b);
    ASSERT_EQ(late.size(), 1u);
    EXPECT_EQ(late[0].value, 65) << "restored records (10+20+30) + 5 must all be present";
    EXPECT_EQ(late[0].pane_index, 1) << "next_pane_index must continue across restore";
    EXPECT_FALSE(late[0].is_first);
    op_b.close();
}

// Coverage (review follow-up): a fire-AND-purge (allowed_lateness=0) erases the
// durable row unconditionally, so a restore finds nothing and nothing re-fires.
TEST(EvictingWindowPersistence, FireAndPurgeLeavesNoDurableRow) {
    const OperatorId op_id{79};
    InMemoryStateBackend backend_a;
    Op op_a(1000ms,
            sum_records,
            std::make_unique<CountEvictor<Value>>(10),
            string_codec(),
            int64_codec());
    // No allowed_lateness: the on-time fire also purges (purge_at == window_end).
    RuntimeContext ctx_a(op_id, "evicting", &backend_a, nullptr);
    op_a.attach_runtime(&ctx_a);
    op_a.open();

    BoundedChannel<StreamElement<std::pair<Key, Out>>> ch_a(256);
    Emitter<std::pair<Key, Out>> em_a(&ch_a);
    send(op_a, em_a, "k", 10, 100);
    send(op_a, em_a, "k", 20, 200);
    watermark(op_a, em_a, 1000);  // fire AND purge
    EXPECT_EQ(panes(ch_a).size(), 1u);
    barrier(op_a, em_a, 1);

    std::size_t rows = 0;
    backend_a.scan(op_id, [&](auto, auto) { ++rows; });
    EXPECT_EQ(rows, 0u) << "a purged buffer must leave no durable row";

    auto snap = backend_a.snapshot(CheckpointId{1});
    op_a.close();

    InMemoryStateBackend backend_b;
    backend_b.restore(snap);
    Op op_b(1000ms,
            sum_records,
            std::make_unique<CountEvictor<Value>>(10),
            string_codec(),
            int64_codec());
    RuntimeContext ctx_b(op_id, "evicting", &backend_b, nullptr);
    op_b.attach_runtime(&ctx_b);
    op_b.open();
    BoundedChannel<StreamElement<std::pair<Key, Out>>> ch_b(256);
    Emitter<std::pair<Key, Out>> em_b(&ch_b);
    watermark(op_b, em_b, 2000);  // nothing to fire
    EXPECT_TRUE(panes(ch_b).empty()) << "a purged buffer must not resurrect on restore";
    op_b.close();
}

// Multiple keys AND multiple windows must rehydrate as DISTINCT (window_start,
// key) buffers - a scan() rehydration bug that collapsed buckets sharing a key
// but differing in window_start (or vice versa) would pass a single-key /
// single-window test. Snapshot mid-window (unfired), restore, then fire all four
// buffers and assert each is reconstructed independently with the right records.
TEST(EvictingWindowPersistence, MultipleKeysAndWindowsSurviveRestore) {
    const OperatorId op_id{80};
    InMemoryStateBackend backend_a;
    Op op_a(1000ms,
            sum_records,
            std::make_unique<CountEvictor<Value>>(10),
            string_codec(),
            int64_codec());
    RuntimeContext ctx_a(op_id, "evicting", &backend_a, nullptr);
    op_a.attach_runtime(&ctx_a);
    op_a.open();

    BoundedChannel<StreamElement<std::pair<Key, Out>>> ch_a(256);
    Emitter<std::pair<Key, Out>> em_a(&ch_a);

    // 2 keys x 2 windows: [0,1000) and [1000,2000). Distinct sums per bucket so a
    // collapse would change an observed value.
    send(op_a, em_a, "a", 10, 100);   // a / [0,1000)
    send(op_a, em_a, "a", 20, 1100);  // a / [1000,2000)
    send(op_a, em_a, "b", 30, 200);   // b / [0,1000)
    send(op_a, em_a, "b", 40, 1200);  // b / [1000,2000)
    // No firing watermark yet: snapshot all four buffers in the OPEN state.
    barrier(op_a, em_a, 1);
    EXPECT_TRUE(panes(ch_a).empty()) << "no window has closed yet";

    std::size_t rows = 0;
    backend_a.scan(op_id, [&](auto, auto) { ++rows; });
    EXPECT_EQ(rows, 4u) << "four distinct (window_start, key) buffers must persist";

    auto snap = backend_a.snapshot(CheckpointId{1});
    op_a.close();

    InMemoryStateBackend backend_b;
    backend_b.restore(snap);
    Op op_b(1000ms,
            sum_records,
            std::make_unique<CountEvictor<Value>>(10),
            string_codec(),
            int64_codec());
    RuntimeContext ctx_b(op_id, "evicting", &backend_b, nullptr);
    op_b.attach_runtime(&ctx_b);
    op_b.open();
    BoundedChannel<StreamElement<std::pair<Key, Out>>> ch_b(256);
    Emitter<std::pair<Key, Out>> em_b(&ch_b);

    // Fire everything. Each (window, key) buffer must emit its own untouched sum.
    watermark(op_b, em_b, 2000);
    auto fired = panes(ch_b);
    ASSERT_EQ(fired.size(), 4u) << "all four buffers fire after restore";
    std::vector<Out> sums;
    sums.reserve(fired.size());
    for (const auto& p : fired) {
        sums.push_back(p.value);
    }
    std::sort(sums.begin(), sums.end());
    EXPECT_EQ(sums, (std::vector<Out>{10, 20, 30, 40}))
        << "no bucket collapsed or cross-contaminated across restore";
    op_b.close();
}

// Construction-path symmetry: the in-memory default ctor writes nothing to a
// backend-equipped runtime.
TEST(EvictingWindowPersistence, DefaultCtorStaysInMemory) {
    const OperatorId op_id{78};
    InMemoryStateBackend backend;
    Op op(1000ms, sum_records, std::make_unique<CountEvictor<Value>>(10));  // in-memory ctor
    RuntimeContext ctx(op_id, "evicting", &backend, nullptr);
    op.attach_runtime(&ctx);
    op.open();

    BoundedChannel<StreamElement<std::pair<Key, Out>>> ch(256);
    Emitter<std::pair<Key, Out>> em(&ch);
    send(op, em, "k", 10, 100);
    send(op, em, "k", 20, 200);

    std::size_t rows = 0;
    backend.scan(op_id, [&](auto, auto) { ++rows; });
    EXPECT_EQ(rows, 0u) << "in-memory ctor must not touch the backend";
    op.close();
}
