// Durable trigger state: a STATEFUL trigger (CountTrigger) must carry its
// per-window progress across checkpoint/restore. Without it, restore resets the
// count to zero and a partially-counted window fires at the wrong record - a
// silent exactly-once break. These tests pin the fix on all three window
// families that expose with_trigger (tumbling, sliding, evicting), per the
// "apply global features globally" rule.
//
// The distinguishing assertion in each test: a window that counted 2 of 3
// records before the checkpoint must fire on the FIRST record after restore
// (2 + 1 == 3). If the count were lost, that record would make the count 1 and
// nothing would fire.

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/codec.hpp"
#include "clink/core/types.hpp"
#include "clink/operators/evicting_tumbling_window_operator.hpp"
#include "clink/operators/sliding_window_operator.hpp"
#include "clink/operators/tumbling_window_operator.hpp"
#include "clink/operators/window_evictor.hpp"
#include "clink/operators/window_trigger.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

using Key = std::string;
using Value = std::int64_t;
using Out = std::int64_t;

template <typename Op>
void send(Op& op, Emitter<std::pair<Key, Out>>& em, const Key& k, Value v, std::int64_t ts) {
    Batch<std::pair<Key, Value>> b;
    b.emplace(std::pair<Key, Value>{k, v}, EventTime{ts});
    op.process(StreamElement<std::pair<Key, Value>>::data(std::move(b)), em);
}

template <typename Op>
void barrier(Op& op, Emitter<std::pair<Key, Out>>& em, std::uint64_t id) {
    op.process(StreamElement<std::pair<Key, Value>>::barrier(CheckpointBarrier{CheckpointId{id}}),
               em);
}

template <typename Op>
std::vector<Out> pane_values(Op& /*op*/, BoundedChannel<StreamElement<std::pair<Key, Out>>>& ch) {
    std::vector<Out> out;
    while (auto e = ch.try_pop()) {
        if (e->is_data()) {
            for (const auto& r : e->as_data()) {
                out.push_back(r.value().second);
            }
        }
    }
    return out;
}

}  // namespace

// Tumbling: CountTrigger(3) partial count survives restore.
TEST(TriggerPersistence, TumblingCountTriggerCountSurvivesRestore) {
    const OperatorId op_id{101};
    InMemoryStateBackend backend_a;
    using Op = TumblingWindowOperator<Key, Value, Out>;

    Op op_a(
        100000ms,  // huge window: only the count-trigger ever fires, never the watermark
        [] { return Out{0}; },
        [](Out a, Value v) { return a + v; },
        string_codec(),
        int64_codec());
    op_a.with_trigger(std::make_unique<CountTrigger<Value>>(3));
    RuntimeContext ctx_a(op_id, "tumbling", &backend_a, nullptr);
    op_a.attach_runtime(&ctx_a);
    op_a.open();

    BoundedChannel<StreamElement<std::pair<Key, Out>>> ch_a(256);
    Emitter<std::pair<Key, Out>> em_a(&ch_a);

    send(op_a, em_a, "k", 10, 100);
    send(op_a, em_a, "k", 20, 200);  // count == 2, no fire yet
    EXPECT_TRUE(pane_values(op_a, ch_a).empty());

    barrier(op_a, em_a, 1);
    auto snap = backend_a.snapshot(CheckpointId{1});
    op_a.close();

    InMemoryStateBackend backend_b;
    backend_b.restore(snap);
    Op op_b(
        100000ms,
        [] { return Out{0}; },
        [](Out a, Value v) { return a + v; },
        string_codec(),
        int64_codec());
    op_b.with_trigger(
        std::make_unique<CountTrigger<Value>>(3));  // fresh trigger, count starts at 0
    RuntimeContext ctx_b(op_id, "tumbling", &backend_b, nullptr);
    op_b.attach_runtime(&ctx_b);
    op_b.open();

    BoundedChannel<StreamElement<std::pair<Key, Out>>> ch_b(256);
    Emitter<std::pair<Key, Out>> em_b(&ch_b);

    // First record after restore: count becomes 2 + 1 == 3, so the window fires.
    send(op_b, em_b, "k", 30, 300);
    auto panes = pane_values(op_b, ch_b);
    ASSERT_EQ(panes.size(), 1u) << "restored count (2) + 1 == 3 must fire; a lost count would not";
    EXPECT_EQ(panes[0], 60) << "fired sum proves both the agg and the count survived restore";
    op_b.close();
}

// Sliding (size == slide => one window per record): same contract.
TEST(TriggerPersistence, SlidingCountTriggerCountSurvivesRestore) {
    const OperatorId op_id{102};
    InMemoryStateBackend backend_a;
    using Op = SlidingWindowOperator<Key, Value, Out>;

    Op op_a(
        100000ms,
        100000ms,
        [] { return Out{0}; },
        [](Out a, Value v) { return a + v; },
        string_codec(),
        int64_codec());
    op_a.with_trigger(std::make_unique<CountTrigger<Value>>(3));
    RuntimeContext ctx_a(op_id, "sliding", &backend_a, nullptr);
    op_a.attach_runtime(&ctx_a);
    op_a.open();

    BoundedChannel<StreamElement<std::pair<Key, Out>>> ch_a(256);
    Emitter<std::pair<Key, Out>> em_a(&ch_a);
    send(op_a, em_a, "k", 10, 100);
    send(op_a, em_a, "k", 20, 200);
    EXPECT_TRUE(pane_values(op_a, ch_a).empty());

    barrier(op_a, em_a, 1);
    auto snap = backend_a.snapshot(CheckpointId{1});
    op_a.close();

    InMemoryStateBackend backend_b;
    backend_b.restore(snap);
    Op op_b(
        100000ms,
        100000ms,
        [] { return Out{0}; },
        [](Out a, Value v) { return a + v; },
        string_codec(),
        int64_codec());
    op_b.with_trigger(std::make_unique<CountTrigger<Value>>(3));
    RuntimeContext ctx_b(op_id, "sliding", &backend_b, nullptr);
    op_b.attach_runtime(&ctx_b);
    op_b.open();

    BoundedChannel<StreamElement<std::pair<Key, Out>>> ch_b(256);
    Emitter<std::pair<Key, Out>> em_b(&ch_b);
    send(op_b, em_b, "k", 30, 300);
    auto panes = pane_values(op_b, ch_b);
    ASSERT_EQ(panes.size(), 1u) << "restored count (2) + 1 == 3 must fire";
    EXPECT_EQ(panes[0], 60);
    op_b.close();
}

// Evicting: the buffered records AND the trigger count both survive restore.
TEST(TriggerPersistence, EvictingCountTriggerCountSurvivesRestore) {
    const OperatorId op_id{103};
    InMemoryStateBackend backend_a;
    using Op = EvictingTumblingWindowOperator<Key, Value, Out>;
    auto sum_records = [](const std::vector<Record<Value>>& recs, const TimeWindow&) -> Out {
        Out s = 0;
        for (const auto& r : recs) {
            s += r.value();
        }
        return s;
    };

    Op op_a(100000ms,
            sum_records,
            std::make_unique<CountEvictor<Value>>(10),  // high: never evicts in this test
            string_codec(),
            int64_codec());
    op_a.with_trigger(std::make_unique<CountTrigger<Value>>(3));
    RuntimeContext ctx_a(op_id, "evicting", &backend_a, nullptr);
    op_a.attach_runtime(&ctx_a);
    op_a.open();

    BoundedChannel<StreamElement<std::pair<Key, Out>>> ch_a(256);
    Emitter<std::pair<Key, Out>> em_a(&ch_a);
    send(op_a, em_a, "k", 10, 100);
    send(op_a, em_a, "k", 20, 200);
    EXPECT_TRUE(pane_values(op_a, ch_a).empty());

    barrier(op_a, em_a, 1);
    auto snap = backend_a.snapshot(CheckpointId{1});
    op_a.close();

    InMemoryStateBackend backend_b;
    backend_b.restore(snap);
    Op op_b(100000ms,
            sum_records,
            std::make_unique<CountEvictor<Value>>(10),
            string_codec(),
            int64_codec());
    op_b.with_trigger(std::make_unique<CountTrigger<Value>>(3));
    RuntimeContext ctx_b(op_id, "evicting", &backend_b, nullptr);
    op_b.attach_runtime(&ctx_b);
    op_b.open();

    BoundedChannel<StreamElement<std::pair<Key, Out>>> ch_b(256);
    Emitter<std::pair<Key, Out>> em_b(&ch_b);
    send(op_b, em_b, "k", 30, 300);
    auto panes = pane_values(op_b, ch_b);
    ASSERT_EQ(panes.size(), 1u) << "restored count (2) + 1 == 3 must fire";
    EXPECT_EQ(panes[0], 60) << "restored buffer (10,20) + 30 summed proves both survived";
    op_b.close();
}

// A stateless trigger (the default EventTimeTrigger) creates no trigger-state
// slot: a backend-equipped persistent window writes only its window rows.
TEST(TriggerPersistence, StatelessTriggerWritesNoTriggerStateRow) {
    const OperatorId op_id{104};
    InMemoryStateBackend backend;
    using Op = TumblingWindowOperator<Key, Value, Out>;
    Op op(
        1000ms,
        [] { return Out{0}; },
        [](Out a, Value v) { return a + v; },
        string_codec(),
        int64_codec());  // default EventTimeTrigger (stateless)
    RuntimeContext ctx(op_id, "tumbling", &backend, nullptr);
    op.attach_runtime(&ctx);
    op.open();

    BoundedChannel<StreamElement<std::pair<Key, Out>>> ch(256);
    Emitter<std::pair<Key, Out>> em(&ch);
    send(op, em, "k", 10, 100);
    barrier(op, em, 1);

    // Exactly one row: the window entry. No trigger_state slot row.
    std::size_t rows = 0;
    backend.scan(op_id, [&](auto, auto) { ++rows; });
    EXPECT_EQ(rows, 1u) << "a stateless trigger must not create a trigger-state row";
    op.close();
}
