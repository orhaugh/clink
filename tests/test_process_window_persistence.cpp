// Persistence tests for ProcessWindow adapters.
//
// Verifies that Tumbling/Sliding/Session ProcessWindow adapters, when
// constructed with their codec-bearing ctors, route the per-(key,
// window) record buffers through KeyedState - and that
// snapshot/restore correctly carries those buffers across a "process
// restart" without losing any record.
//
// Strategy: build adapter A with persistent codecs + InMemory
// backend, send some records (no watermark yet, so buckets are
// loaded but unfired), snapshot the backend, restore into a fresh
// backend, attach to a fresh adapter B with the same codecs, then
// fire the watermark on B and verify the recorded ProcessWindow
// invocations cover every input element.
//
// Back-compat: the existing test_process_window_lateness.cpp tests
// use the in-memory ctor with no codecs - those continue to pass
// unchanged, exercising the fallback path.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/codec.hpp"
#include "clink/core/types.hpp"
#include "clink/operators/process_function.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

// Test ProcessWindowFunction that records every (key, window,
// elements) tuple it sees. Out type = int (just the count for
// simplicity - the test asserts on the recorded calls, not on the
// emitted output).
template <typename Key, typename In>
struct RecordingFn : public ProcessWindowFunction<Key, In, int> {
    struct Call {
        Key key;
        std::int64_t window_start{0};
        std::int64_t window_end{0};
        std::vector<In> elements;
    };
    std::vector<Call> calls;

    void process(const Key& key,
                 const WindowContext& ctx,
                 const std::vector<In>& elements,
                 Collector<int>& out) override {
        Call c;
        c.key = key;
        c.window_start = ctx.window_start;
        c.window_end = ctx.window_end;
        c.elements = elements;
        calls.push_back(std::move(c));
        out.collect(static_cast<int>(elements.size()));
    }
};

template <typename KK, typename TT>
StreamElement<std::pair<KK, TT>> data_pair(KK key, TT value, std::int64_t ts) {
    Batch<std::pair<KK, TT>> b;
    b.emplace(std::pair<KK, TT>{key, value}, EventTime{ts});
    return StreamElement<std::pair<KK, TT>>::data(std::move(b));
}

}  // namespace

namespace {

// We use int64_t for both Key and In so the existing
// int64_codec() helper works without inventing a new codec.
using K = std::int64_t;
using V = std::int64_t;

}  // namespace

TEST(ProcessWindowPersistence, TumblingBufferSurvivesSnapshotRestore) {
    // Build operator A with persistent ctor, push records
    // into 3 distinct windows for 2 distinct keys, take snapshot.
    InMemoryStateBackend backend_a;
    auto fn_a = std::make_shared<RecordingFn<K, V>>();
    detail::TumblingProcessWindowAdapter<K, V, int> op_a(
        1000ms, fn_a, int64_codec(), int64_codec());

    RuntimeContext ctx_a(OperatorId{42}, "tumbling_pw_persist", &backend_a, nullptr);
    op_a.attach_runtime(&ctx_a);
    op_a.open();

    BoundedChannel<StreamElement<int>> main_ch_a(256);
    Emitter<int> em_a(&main_ch_a);

    // Key 1: window [0, 1000) carries values 10, 11.
    op_a.process(data_pair<K, V>(1, 10, 50), em_a);
    op_a.process(data_pair<K, V>(1, 11, 100), em_a);
    // Key 2: window [0, 1000) carries value 20.
    op_a.process(data_pair<K, V>(2, 20, 200), em_a);
    // Key 1: window [1000, 2000) carries value 12.
    op_a.process(data_pair<K, V>(1, 12, 1500), em_a);

    // No watermark yet - buckets are persisted but unfired.
    EXPECT_TRUE(fn_a->calls.empty());

    auto snap = backend_a.snapshot(CheckpointId{1});

    op_a.close();

    // Build operator B, restore the backend, fire watermark
    // past every window. Every input record must show up in some
    // process() invocation.
    InMemoryStateBackend backend_b;
    backend_b.restore(snap);

    auto fn_b = std::make_shared<RecordingFn<K, V>>();
    detail::TumblingProcessWindowAdapter<K, V, int> op_b(
        1000ms, fn_b, int64_codec(), int64_codec());

    RuntimeContext ctx_b(OperatorId{42}, "tumbling_pw_persist", &backend_b, nullptr);
    op_b.attach_runtime(&ctx_b);
    op_b.open();

    BoundedChannel<StreamElement<int>> main_ch_b(256);
    Emitter<int> em_b(&main_ch_b);

    op_b.process(StreamElement<std::pair<K, V>>::watermark(Watermark{EventTime{2500}}), em_b);

    // We expect three on-time fires (one per (key, window) bucket):
    //   (k=1, win [0,1000)) -> [10, 11]
    //   (k=2, win [0,1000)) -> [20]
    //   (k=1, win [1000,2000)) -> [12]
    ASSERT_EQ(fn_b->calls.size(), 3u);
    // Sort calls into a stable order for comparison.
    std::sort(fn_b->calls.begin(), fn_b->calls.end(), [](const auto& a, const auto& b) {
        if (a.key != b.key)
            return a.key < b.key;
        return a.window_start < b.window_start;
    });
    EXPECT_EQ(fn_b->calls[0].key, 1);
    EXPECT_EQ(fn_b->calls[0].window_start, 0);
    EXPECT_EQ(fn_b->calls[0].elements, (std::vector<V>{10, 11}));
    EXPECT_EQ(fn_b->calls[1].key, 1);
    EXPECT_EQ(fn_b->calls[1].window_start, 1000);
    EXPECT_EQ(fn_b->calls[1].elements, (std::vector<V>{12}));
    EXPECT_EQ(fn_b->calls[2].key, 2);
    EXPECT_EQ(fn_b->calls[2].window_start, 0);
    EXPECT_EQ(fn_b->calls[2].elements, (std::vector<V>{20}));

    op_b.close();
}

TEST(ProcessWindowPersistence, SlidingBufferSurvivesSnapshotRestore) {
    // 1000ms windows sliding by 500ms - every record in [0, 500)
    // belongs to one window; record at 750 belongs to TWO (covering
    // windows starting at 0 and 500).
    InMemoryStateBackend backend_a;
    auto fn_a = std::make_shared<RecordingFn<K, V>>();
    detail::SlidingProcessWindowAdapter<K, V, int> op_a(
        1000ms, 500ms, fn_a, int64_codec(), int64_codec());

    RuntimeContext ctx_a(OperatorId{43}, "sliding_pw_persist", &backend_a, nullptr);
    op_a.attach_runtime(&ctx_a);
    op_a.open();

    BoundedChannel<StreamElement<int>> main_ch_a(256);
    Emitter<int> em_a(&main_ch_a);

    // ts=250 belongs to windows [-500,500) and [0,1000); ts=750
    // belongs to windows [0,1000) and [500,1500). Three distinct
    // buckets across the two records: [-500,500) [100], [0,1000)
    // [100,200], and [500,1500) [200]. (sliding semantics
    // place windows on slide boundaries including pre-zero ones.)
    op_a.process(data_pair<K, V>(1, 100, 250), em_a);
    op_a.process(data_pair<K, V>(1, 200, 750), em_a);
    EXPECT_TRUE(fn_a->calls.empty());

    auto snap = backend_a.snapshot(CheckpointId{1});
    op_a.close();

    InMemoryStateBackend backend_b;
    backend_b.restore(snap);

    auto fn_b = std::make_shared<RecordingFn<K, V>>();
    detail::SlidingProcessWindowAdapter<K, V, int> op_b(
        1000ms, 500ms, fn_b, int64_codec(), int64_codec());
    RuntimeContext ctx_b(OperatorId{43}, "sliding_pw_persist", &backend_b, nullptr);
    op_b.attach_runtime(&ctx_b);
    op_b.open();

    BoundedChannel<StreamElement<int>> main_ch_b(256);
    Emitter<int> em_b(&main_ch_b);

    op_b.process(StreamElement<std::pair<K, V>>::watermark(Watermark{EventTime{2000}}), em_b);

    ASSERT_EQ(fn_b->calls.size(), 3u);
    std::sort(fn_b->calls.begin(), fn_b->calls.end(), [](const auto& a, const auto& b) {
        return a.window_start < b.window_start;
    });
    EXPECT_EQ(fn_b->calls[0].window_start, -500);
    EXPECT_EQ(fn_b->calls[0].elements, (std::vector<V>{100}));
    EXPECT_EQ(fn_b->calls[1].window_start, 0);
    EXPECT_EQ(fn_b->calls[1].elements.size(), 2u);
    EXPECT_EQ(fn_b->calls[2].window_start, 500);
    EXPECT_EQ(fn_b->calls[2].elements, (std::vector<V>{200}));

    op_b.close();
}

TEST(ProcessWindowPersistence, SessionBufferSurvivesSnapshotRestore) {
    // gap = 100ms. Records at 100, 150 form one session [100, 151).
    // Record at 500 starts a new session [500, 501).
    InMemoryStateBackend backend_a;
    auto fn_a = std::make_shared<RecordingFn<K, V>>();
    detail::SessionProcessWindowAdapter<K, V, int> op_a(100ms, fn_a, int64_codec(), int64_codec());

    RuntimeContext ctx_a(OperatorId{44}, "session_pw_persist", &backend_a, nullptr);
    op_a.attach_runtime(&ctx_a);
    op_a.open();

    BoundedChannel<StreamElement<int>> main_ch_a(256);
    Emitter<int> em_a(&main_ch_a);

    op_a.process(data_pair<K, V>(1, 100, 100), em_a);
    op_a.process(data_pair<K, V>(1, 150, 150), em_a);
    op_a.process(data_pair<K, V>(1, 500, 500), em_a);
    EXPECT_TRUE(fn_a->calls.empty());

    auto snap = backend_a.snapshot(CheckpointId{1});
    op_a.close();

    InMemoryStateBackend backend_b;
    backend_b.restore(snap);

    auto fn_b = std::make_shared<RecordingFn<K, V>>();
    detail::SessionProcessWindowAdapter<K, V, int> op_b(100ms, fn_b, int64_codec(), int64_codec());
    RuntimeContext ctx_b(OperatorId{44}, "session_pw_persist", &backend_b, nullptr);
    op_b.attach_runtime(&ctx_b);
    op_b.open();

    BoundedChannel<StreamElement<int>> main_ch_b(256);
    Emitter<int> em_b(&main_ch_b);

    // Watermark high enough to fire both sessions.
    op_b.process(StreamElement<std::pair<K, V>>::watermark(Watermark{EventTime{1000}}), em_b);

    ASSERT_EQ(fn_b->calls.size(), 2u);
    std::sort(fn_b->calls.begin(), fn_b->calls.end(), [](const auto& a, const auto& b) {
        return a.window_start < b.window_start;
    });
    EXPECT_EQ(fn_b->calls[0].window_start, 100);
    // The session adapter's merge order puts the most recent record
    // first (see add_record_'s "new merged starts with new value,
    // then merges in existing sessions"). Order across the snapshot
    // boundary must match - testing the persistence contract, not
    // the merge ordering choice.
    EXPECT_EQ(fn_b->calls[0].elements.size(), 2u);
    EXPECT_EQ((std::set<V>{fn_b->calls[0].elements.begin(), fn_b->calls[0].elements.end()}),
              (std::set<V>{100, 150}));
    EXPECT_EQ(fn_b->calls[1].window_start, 500);
    EXPECT_EQ(fn_b->calls[1].elements, (std::vector<V>{500}));

    op_b.close();
}
