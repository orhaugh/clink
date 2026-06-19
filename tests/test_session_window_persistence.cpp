// Durable session windows (FOUND-2): the persistent SessionWindowOperator
// survives snapshot/restore. Proves (a) a merge is ONE atomic durable row per
// key (no stale absorbed rows), (b) sessions + aggregates continue across a
// restart incl. a merge that spans the snapshot, and (c) the MANDATORY
// no-double-fire contract - a session that fired before the snapshot does NOT
// re-emit its pane after restore + a replayed firing watermark.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <string>
#include <tuple>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/codec.hpp"
#include "clink/core/types.hpp"
#include "clink/operators/session_window_operator.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

using Key = std::string;
using Value = int;
using Agg = std::int64_t;
using Op = SessionWindowOperator<Key, Value, Agg>;

auto initial = []() -> Agg { return 0; };
auto combine = [](Agg a, Value v) -> Agg { return a + v; };
auto merge = [](Agg a, Agg b) -> Agg { return a + b; };

void send(Op& op, Emitter<std::pair<Key, Agg>>& em, const Key& k, Value v, std::int64_t ts) {
    Batch<std::pair<Key, Value>> b;
    b.emplace(std::pair<Key, Value>{k, v}, EventTime{ts});
    op.process(StreamElement<std::pair<Key, Value>>::data(std::move(b)), em);
}

void watermark(Op& op, Emitter<std::pair<Key, Agg>>& em, std::int64_t ts) {
    op.process(StreamElement<std::pair<Key, Value>>::watermark(Watermark{EventTime{ts}}), em);
}

// A checkpoint barrier always precedes a snapshot in the engine; in write-back-
// cache mode it is what flushes the deferred per-key puts to the backend (in
// strict mode on_barrier is a no-op beyond forwarding). Sending it keeps these
// tests faithful under both CLINK_WB_STATE_CACHE states.
void barrier(Op& op, Emitter<std::pair<Key, Agg>>& em, std::uint64_t id) {
    op.process(StreamElement<std::pair<Key, Value>>::barrier(CheckpointBarrier{CheckpointId{id}}),
               em);
}

// Flatten all data records emitted onto a channel into (key, agg) pairs.
std::vector<std::pair<Key, Agg>> panes(BoundedChannel<StreamElement<std::pair<Key, Agg>>>& ch) {
    std::vector<std::pair<Key, Agg>> out;
    while (auto e = ch.try_pop()) {
        if (e->is_data()) {
            for (const auto& r : e->as_data()) {
                out.push_back(r.value());
            }
        }
    }
    return out;
}

std::size_t backend_rows(const InMemoryStateBackend& backend, OperatorId op) {
    std::size_t n = 0;
    backend.scan(op, [&](auto, auto) { ++n; });
    return n;
}

}  // namespace

// INC-4: a merge of overlapping sessions leaves EXACTLY ONE durable row for the
// key (the per-key-vector representation - never multiple absorbed rows), and
// the merged aggregate survives restore + continues to merge across it.
TEST(SessionWindowPersistence, MergeIsOneAtomicRowAndSurvivesRestore) {
    const OperatorId op_id{44};
    InMemoryStateBackend backend_a;
    Op op_a(100ms, initial, combine, merge, string_codec(), int64_codec());
    RuntimeContext ctx_a(op_id, "sessions", &backend_a, nullptr);
    op_a.attach_runtime(&ctx_a);
    op_a.open();

    BoundedChannel<StreamElement<std::pair<Key, Agg>>> ch_a(256);
    Emitter<std::pair<Key, Agg>> em_a(&ch_a);

    // Two overlapping records merge into one session [100, 250], agg = 3.
    send(op_a, em_a, "k", 1, 100);     // session [100, 200]
    send(op_a, em_a, "k", 2, 150);     // [150, 250] overlaps -> merged [100, 250], agg 3
    EXPECT_TRUE(panes(ch_a).empty());  // nothing fired yet
    barrier(op_a, em_a, 1);            // flush (WB mode) / no-op (strict)
    EXPECT_EQ(backend_rows(backend_a, op_id), 1u) << "merge must leave ONE row per key";

    auto snap = backend_a.snapshot(CheckpointId{1});
    op_a.close();

    // Restore into a fresh backend + operator (same OperatorId).
    InMemoryStateBackend backend_b;
    backend_b.restore(snap);
    Op op_b(100ms, initial, combine, merge, string_codec(), int64_codec());
    RuntimeContext ctx_b(op_id, "sessions", &backend_b, nullptr);
    op_b.attach_runtime(&ctx_b);
    op_b.open();

    BoundedChannel<StreamElement<std::pair<Key, Agg>>> ch_b(256);
    Emitter<std::pair<Key, Agg>> em_b(&ch_b);

    // A record within the restored session extends + merges it across the
    // restart: [100, 320], agg = 3 + 10 = 13.
    send(op_b, em_b, "k", 10, 220);
    watermark(op_b, em_b, 1000);  // fire
    auto fired = panes(ch_b);
    ASSERT_EQ(fired.size(), 1u);
    EXPECT_EQ(fired[0].first, "k");
    EXPECT_EQ(fired[0].second, 13) << "aggregate must span both phases through the merge";
    op_b.close();
}

// Coverage (review follow-up): a key with MULTIPLE disjoint sessions persists
// as one row holding an N-element vector, and all N rehydrate + fire after
// restore - the un-merged multi-session-vector path.
TEST(SessionWindowPersistence, MultipleDisjointSessionsPerKeySurviveRestore) {
    const OperatorId op_id{47};
    InMemoryStateBackend backend_a;
    Op op_a(100ms, initial, combine, merge, string_codec(), int64_codec());
    RuntimeContext ctx_a(op_id, "sessions", &backend_a, nullptr);
    op_a.attach_runtime(&ctx_a);
    op_a.open();

    BoundedChannel<StreamElement<std::pair<Key, Agg>>> ch_a(256);
    Emitter<std::pair<Key, Agg>> em_a(&ch_a);

    // Two non-overlapping sessions for one key: [100, 200] and [10000, 10100].
    send(op_a, em_a, "k", 1, 100);
    send(op_a, em_a, "k", 2, 10000);
    barrier(op_a, em_a, 1);
    EXPECT_EQ(backend_rows(backend_a, op_id), 1u) << "two sessions of one key = ONE row (a vector)";

    auto snap = backend_a.snapshot(CheckpointId{1});
    op_a.close();

    InMemoryStateBackend backend_b;
    backend_b.restore(snap);
    Op op_b(100ms, initial, combine, merge, string_codec(), int64_codec());
    RuntimeContext ctx_b(op_id, "sessions", &backend_b, nullptr);
    op_b.attach_runtime(&ctx_b);
    op_b.open();

    BoundedChannel<StreamElement<std::pair<Key, Agg>>> ch_b(256);
    Emitter<std::pair<Key, Agg>> em_b(&ch_b);
    watermark(op_b, em_b, 20000);  // fire both rehydrated sessions
    auto fired = panes(ch_b);
    ASSERT_EQ(fired.size(), 2u) << "both rehydrated sessions must fire";
    std::vector<Agg> aggs{fired[0].second, fired[1].second};
    std::sort(aggs.begin(), aggs.end());
    EXPECT_EQ(aggs, (std::vector<Agg>{1, 2}));
    op_b.close();
}

// MANDATORY no-double-fire: a session that FIRED before the snapshot (fired=true
// persisted, kept alive by allowed_lateness) must NOT re-emit its pane after
// restore + a replayed firing watermark.
TEST(SessionWindowPersistence, DoesNotDoubleFireAfterRestore) {
    const OperatorId op_id{45};
    InMemoryStateBackend backend_a;
    Op op_a(100ms, initial, combine, merge, string_codec(), int64_codec());
    op_a.allowed_lateness(100ms);  // session survives the fire (fired, not purged)
    RuntimeContext ctx_a(op_id, "sessions", &backend_a, nullptr);
    op_a.attach_runtime(&ctx_a);
    op_a.open();

    BoundedChannel<StreamElement<std::pair<Key, Agg>>> ch_a(256);
    Emitter<std::pair<Key, Agg>> em_a(&ch_a);

    send(op_a, em_a, "k", 5, 100);  // session [100, 200]
    watermark(op_a, em_a, 200);     // fires on-time: pane (k, 5), fired=true, not purged (<300)
    auto first = panes(ch_a);
    ASSERT_EQ(first.size(), 1u);
    EXPECT_EQ(first[0].second, 5);

    barrier(op_a, em_a, 1);                           // flush the fired session (WB mode)
    auto snap = backend_a.snapshot(CheckpointId{1});  // captures fired=true session
    op_a.close();

    InMemoryStateBackend backend_b;
    backend_b.restore(snap);
    Op op_b(100ms, initial, combine, merge, string_codec(), int64_codec());
    op_b.allowed_lateness(100ms);
    RuntimeContext ctx_b(op_id, "sessions", &backend_b, nullptr);
    op_b.attach_runtime(&ctx_b);
    op_b.open();

    BoundedChannel<StreamElement<std::pair<Key, Agg>>> ch_b(256);
    Emitter<std::pair<Key, Agg>> em_b(&ch_b);

    // Replay a firing watermark (>= session end, still within lateness). The
    // persisted fired flag must suppress a second pane.
    watermark(op_b, em_b, 250);
    EXPECT_TRUE(panes(ch_b).empty()) << "a session fired pre-snapshot must not re-fire on restore";
    op_b.close();
}

// Construction-path symmetry: the in-memory default ctor writes NOTHING to a
// backend-equipped runtime (stays in-memory), so the two ctors are distinct.
TEST(SessionWindowPersistence, DefaultCtorStaysInMemory) {
    const OperatorId op_id{46};
    InMemoryStateBackend backend;
    Op op(100ms, initial, combine, merge);  // in-memory ctor (no codecs)
    RuntimeContext ctx(op_id, "sessions", &backend, nullptr);
    op.attach_runtime(&ctx);
    op.open();

    BoundedChannel<StreamElement<std::pair<Key, Agg>>> ch(256);
    Emitter<std::pair<Key, Agg>> em(&ch);
    send(op, em, "k", 1, 100);
    send(op, em, "k", 2, 150);
    EXPECT_EQ(backend_rows(backend, op_id), 0u) << "in-memory ctor must not touch the backend";
    op.close();
}
