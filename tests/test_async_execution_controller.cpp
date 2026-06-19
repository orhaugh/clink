// ASYNC-3: the AsyncExecutionController core - per-key gate, in-flight
// table, thread-safe ready-queue with a runner wakeup, and run-until-empty
// drain. Driven standalone (no DAG, no backend) so the gate, FIFO
// promotion, distinct-key overlap, backpressure, and the cross-thread
// completion handoff each get a deterministic test.

#include <algorithm>
#include <chrono>
#include <coroutine>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "clink/async/task.hpp"
#include "clink/core/codec.hpp"
#include "clink/runtime/async_execution_controller.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/keyed_state.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

// A one-shot suspension point: co_await parks the coroutine and captures
// its handle; the test resumes it via aec.schedule_resume(gate.handle).
struct Gate {
    std::coroutine_handle<> handle{};
    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) noexcept { handle = h; }
    void await_resume() const noexcept {}
};

// A record that suspends on `g` and, when resumed, appends `tag` to `ev` -
// so a test can observe the order of post-state stages relative to watermark
// releases. Pointers reference test locals that outlive the controller.
AsyncExecutionController::CoroFactory gated(Gate* g,
                                            std::vector<std::string>* ev,
                                            std::string tag) {
    return [g, ev, tag]() -> async::Task<void> {
        co_await *g;
        ev->push_back(tag);
        co_return;
    };
}

}  // namespace

// A record whose body never suspends completes during submit(), leaving
// nothing in flight.
TEST(AsyncExecutionController, SynchronousRecordCompletesOnSubmit) {
    AsyncExecutionController aec;
    int ran = 0;
    bool accepted = aec.submit("k", [&ran]() -> async::Task<void> {
        ++ran;
        co_return;
    });
    EXPECT_TRUE(accepted);
    EXPECT_EQ(ran, 1);
    EXPECT_EQ(aec.in_flight(), 0u);
    EXPECT_EQ(aec.parked(), 0u);
}

// Per-key gate: same-key records run one at a time in FIFO order, and each
// observes the prior same-key write (the gate promotes the next only after
// the current one finishes), regardless of completion timing.
TEST(AsyncExecutionController, PerKeyFifoSerialisesAndObservesPriorWrite) {
    AsyncExecutionController aec;
    std::unordered_map<std::string, int> store;
    std::vector<int> reads;

    Gate g1, g2, g3;
    auto rmw = [&store, &reads](const std::string& key, Gate* g) {
        return [&store, &reads, key, g]() -> async::Task<void> {
            co_await *g;         // suspend until released
            int v = store[key];  // read AFTER promotion -> sees prior write
            reads.push_back(v);
            store[key] = v + 1;  // write
            co_return;
        };
    };

    EXPECT_TRUE(aec.submit("A", rmw("A", &g1)));
    EXPECT_TRUE(aec.submit("A", rmw("A", &g2)));
    EXPECT_TRUE(aec.submit("A", rmw("A", &g3)));

    // Only the first record for key A runs; the other two are parked.
    EXPECT_EQ(aec.in_flight(), 1u);
    EXPECT_EQ(aec.parked(), 2u);

    aec.schedule_resume(g1.handle);
    aec.poll();  // r1 completes, promotes r2 (which suspends on g2)
    EXPECT_EQ(aec.in_flight(), 1u);
    EXPECT_EQ(aec.parked(), 1u);

    aec.schedule_resume(g2.handle);
    aec.poll();  // r2 completes, promotes r3
    EXPECT_EQ(aec.parked(), 0u);

    aec.schedule_resume(g3.handle);
    aec.poll();  // r3 completes
    EXPECT_EQ(aec.in_flight(), 0u);

    EXPECT_EQ(reads, (std::vector<int>{0, 1, 2}));  // each saw the prior write
    EXPECT_EQ(store["A"], 3);
}

// Distinct keys run concurrently: all suspend in flight at once.
TEST(AsyncExecutionController, DistinctKeysOverlap) {
    AsyncExecutionController aec;
    Gate ga, gb, gc;
    auto block_on = [](Gate* g) {
        return [g]() -> async::Task<void> {
            co_await *g;
            co_return;
        };
    };
    aec.submit("A", block_on(&ga));
    aec.submit("B", block_on(&gb));
    aec.submit("C", block_on(&gc));

    EXPECT_EQ(aec.in_flight(), 3u);  // distinct keys do not gate each other
    EXPECT_GE(aec.max_in_flight_observed(), 3u);

    aec.schedule_resume(ga.handle);
    aec.schedule_resume(gb.handle);
    aec.schedule_resume(gc.handle);
    aec.poll();
    EXPECT_EQ(aec.in_flight(), 0u);
}

// Backpressure: submit refuses (returns false) at the in-flight cap without
// enqueuing, then accepts once capacity frees.
TEST(AsyncExecutionController, BackpressureAtCapacity) {
    AsyncExecutionController aec(/*max_in_flight=*/2);
    Gate ga, gb, gc;
    auto block_on = [](Gate* g) {
        return [g]() -> async::Task<void> {
            co_await *g;
            co_return;
        };
    };
    EXPECT_TRUE(aec.submit("A", block_on(&ga)));
    EXPECT_TRUE(aec.submit("B", block_on(&gb)));
    EXPECT_FALSE(aec.submit("C", block_on(&gc)));  // at cap
    EXPECT_EQ(aec.in_flight(), 2u);

    aec.schedule_resume(ga.handle);
    aec.schedule_resume(gb.handle);
    aec.poll();
    EXPECT_EQ(aec.in_flight(), 0u);

    EXPECT_TRUE(aec.submit("C", block_on(&gc)));  // capacity freed
    aec.schedule_resume(gc.handle);
    aec.poll();
}

// drain() empties everything in flight, running each record's post-state
// stage exactly once.
TEST(AsyncExecutionController, DrainRunsEveryRecordOnce) {
    AsyncExecutionController aec;
    std::vector<Gate> gates(5);
    int completions = 0;
    for (int i = 0; i < 5; ++i) {
        aec.submit("key-" + std::to_string(i),
                   [&completions, g = &gates[i]]() -> async::Task<void> {
                       co_await *g;
                       ++completions;
                       co_return;
                   });
    }
    EXPECT_EQ(aec.in_flight(), 5u);
    for (auto& g : gates) {
        aec.schedule_resume(g.handle);
    }
    aec.drain();
    EXPECT_EQ(completions, 5);
    EXPECT_EQ(aec.in_flight(), 0u);
}

// The cross-thread completion contract: a completion signalled from a
// FOREIGN thread is handed back through the ready-queue, and the record's
// post-state stage runs on the RUNNER (drain) thread, never the completion
// thread. This is the property the io_uring/thread-pool completion source
// will rely on (ASYNC-9).
TEST(AsyncExecutionController, Stage3RunsOnRunnerThreadNotCompletionThread) {
    AsyncExecutionController aec;
    Gate gate;
    std::thread::id stage3_tid;
    aec.submit("k", [&stage3_tid, &gate]() -> async::Task<void> {
        co_await gate;
        stage3_tid = std::this_thread::get_id();  // where does post-state run?
        co_return;
    });
    ASSERT_EQ(aec.in_flight(), 1u);
    ASSERT_NE(gate.handle, std::coroutine_handle<>{});  // parked, handle captured

    const auto runner_tid = std::this_thread::get_id();
    std::thread completer([&aec, &gate] {
        std::this_thread::sleep_for(20ms);
        aec.schedule_resume(gate.handle);  // signal completion from another thread
    });
    aec.drain();  // blocks until the foreign signal, then resumes on THIS thread
    completer.join();

    EXPECT_EQ(aec.in_flight(), 0u);
    EXPECT_EQ(stage3_tid, runner_tid);  // ran on the runner thread
    EXPECT_NE(stage3_tid, std::thread::id{});
}

// ---- ASYNC-4: epoch manager (watermark/timer completeness) --------------

// A watermark is released only after every record that arrived before it
// (its epoch) has finished its post-state stage.
TEST(AsyncExecutionController, WatermarkReleasesOnlyAfterEpochFinishes) {
    AsyncExecutionController aec;
    std::vector<std::string> ev;
    Gate g1, g2;
    aec.submit("A", gated(&g1, &ev, "r1"));
    aec.submit("B", gated(&g2, &ev, "r2"));
    aec.on_watermark([&ev] { ev.push_back("wm"); });  // closes epoch 0
    EXPECT_EQ(aec.pending_watermarks(), 1u);

    aec.schedule_resume(g1.handle);
    aec.poll();  // r1 done, but r2 (same epoch) still outstanding
    EXPECT_EQ(ev, (std::vector<std::string>{"r1"}));
    EXPECT_EQ(aec.pending_watermarks(), 1u);

    aec.schedule_resume(g2.handle);
    aec.poll();  // epoch 0 finished -> watermark releases
    EXPECT_EQ(ev, (std::vector<std::string>{"r1", "r2", "wm"}));
    EXPECT_EQ(aec.pending_watermarks(), 0u);
}

// Watermarks release in epoch order even when a later epoch finishes first.
// This is the load-bearing check: r1 (epoch 1) completes before r0 (epoch
// 0), yet wm1 does not release until wm0 has - the exact early-release the
// epoch manager exists to prevent (without it, wm1 would have fired at
// on_watermark time, before r1 even ran).
TEST(AsyncExecutionController, WatermarksReleaseInEpochOrderNotCompletionOrder) {
    AsyncExecutionController aec;
    std::vector<std::string> ev;
    Gate g0, g1;
    aec.submit("A", gated(&g0, &ev, "r0"));  // epoch 0
    aec.on_watermark([&ev] { ev.push_back("wm0"); });
    aec.submit("B", gated(&g1, &ev, "r1"));  // epoch 1
    aec.on_watermark([&ev] { ev.push_back("wm1"); });
    EXPECT_EQ(aec.pending_watermarks(), 2u);

    aec.schedule_resume(g1.handle);
    aec.poll();  // epoch 1 finishes first, but its watermark must wait for epoch 0
    EXPECT_EQ(ev, (std::vector<std::string>{"r1"}));
    EXPECT_EQ(aec.pending_watermarks(), 2u);

    aec.schedule_resume(g0.handle);
    aec.poll();  // epoch 0 finishes -> wm0 releases, then the already-finished epoch 1 -> wm1
    EXPECT_EQ(ev, (std::vector<std::string>{"r1", "r0", "wm0", "wm1"}));
    EXPECT_EQ(aec.pending_watermarks(), 0u);
}

// A watermark closing an epoch with no records releases immediately.
TEST(AsyncExecutionController, EmptyEpochWatermarkReleasesImmediately) {
    AsyncExecutionController aec;
    bool released = false;
    aec.on_watermark([&released] { released = true; });
    EXPECT_TRUE(released);
    EXPECT_EQ(aec.pending_watermarks(), 0u);
}

// Hitting the in-flight cap mid-epoch does not deadlock the epoch: the
// in-flight records still complete (freeing capacity and finishing the
// epoch), the watermark releases, and the deferred record runs next epoch.
TEST(AsyncExecutionController, NoDeadlockWhenCapHitMidEpoch) {
    AsyncExecutionController aec(/*max_in_flight=*/2);
    std::vector<std::string> ev;
    Gate g1, g2, g3;
    EXPECT_TRUE(aec.submit("A", gated(&g1, &ev, "r1")));
    EXPECT_TRUE(aec.submit("B", gated(&g2, &ev, "r2")));
    EXPECT_FALSE(aec.submit("C", gated(&g3, &ev, "r3")));  // at cap, refused
    aec.on_watermark([&ev] { ev.push_back("wm0"); });

    aec.schedule_resume(g1.handle);
    aec.schedule_resume(g2.handle);
    aec.poll();  // epoch 0 drains, watermark releases - no deadlock
    EXPECT_EQ(ev, (std::vector<std::string>{"r1", "r2", "wm0"}));
    EXPECT_EQ(aec.pending_watermarks(), 0u);

    EXPECT_TRUE(aec.submit("C", gated(&g3, &ev, "r3")));  // capacity freed (epoch 1)
    aec.schedule_resume(g3.handle);
    aec.drain();
    EXPECT_EQ(ev.back(), "r3");
    EXPECT_EQ(aec.in_flight(), 0u);
}

// ---- ASYNC-5: checkpoint-barrier drain (consistent cut) -----------------

// drain_for_barrier() drains in-flight async work to quiescence so a
// snapshot taken after it is a consistent cut: every record admitted before
// the barrier has applied its post-state write. The negative control shows
// the same snapshot taken mid-flight (no drain) is torn - proving the drain
// is load-bearing. Uses a real InMemoryStateBackend + KeyedState and a real
// snapshot/restore round-trip; the Gate models the async (remote) read
// latency that suspends a record before its post-state write.
TEST(AsyncExecutionController, BarrierDrainYieldsConsistentSnapshot) {
    AsyncExecutionController aec;
    InMemoryStateBackend backend;
    KeyedState<std::string, std::int64_t> kv(
        backend, OperatorId{1}, "agg", string_codec(), int64_codec());

    constexpr int kN = 4;
    std::vector<Gate> gates(kN);
    for (int i = 0; i < kN; ++i) {
        std::string key = "k" + std::to_string(i);
        aec.submit(key, [&kv, key, g = &gates[i]]() -> async::Task<void> {
            co_await *g;                       // async read latency (suspends)
            auto cur = kv.get(key);            // post-state stage on the runner thread
            kv.put(key, cur.value_or(0) + 1);  // RMW write
            co_return;
        });
    }
    ASSERT_EQ(aec.in_flight(), static_cast<std::size_t>(kN));

    // Negative control: snapshot mid-flight, before the drain. The writes
    // have not landed, so the restored state is torn (empty).
    InMemoryStateBackend torn;
    torn.restore(backend.snapshot(CheckpointId{1}));
    KeyedState<std::string, std::int64_t> kv_torn(
        torn, OperatorId{1}, "agg", string_codec(), int64_codec());
    for (int i = 0; i < kN; ++i) {
        EXPECT_FALSE(kv_torn.get("k" + std::to_string(i)).has_value());
    }

    // The barrier arrives with records in flight; a foreign thread completes
    // the reads, and drain_for_barrier blocks until every write has landed.
    std::thread completer([&] {
        std::this_thread::sleep_for(10ms);
        for (auto& g : gates) {
            aec.schedule_resume(g.handle);
        }
    });
    aec.drain_for_barrier();
    completer.join();
    ASSERT_EQ(aec.in_flight(), 0u);

    // Now the snapshot is a consistent cut: all kN writes are present.
    InMemoryStateBackend good;
    good.restore(backend.snapshot(CheckpointId{2}));
    KeyedState<std::string, std::int64_t> kv_good(
        good, OperatorId{1}, "agg", string_codec(), int64_codec());
    for (int i = 0; i < kN; ++i) {
        auto v = kv_good.get("k" + std::to_string(i));
        ASSERT_TRUE(v.has_value());
        EXPECT_EQ(*v, 1);
    }
}

// The barrier drain on an idle controller (the empty / EOS-flush case) is a
// no-op and returns immediately.
TEST(AsyncExecutionController, BarrierDrainOnIdleIsNoop) {
    AsyncExecutionController aec;
    aec.drain_for_barrier();
    EXPECT_EQ(aec.in_flight(), 0u);
    EXPECT_EQ(aec.parked(), 0u);
}

// A throwing record body must be ACCOUNTED, not leaked: the gate releases and
// the epoch advances so a later drain() terminates, and the error surfaces on
// the next poll. Without this, the record would stay in flight with its key
// held and the next drain() would spin forever (the worker-death hang the
// ASYNC-7 review caught). Mirrors the synchronous operator-throw path.
TEST(AsyncExecutionController, ThrowingRecordIsAccountedAndSurfaced) {
    AsyncExecutionController aec;
    aec.submit("k", []() -> async::Task<void> {
        throw std::runtime_error("boom");
        co_return;  // makes the lambda a coroutine; unreachable
    });
    // Accounted: key released, nothing left in flight (no leak -> no hang).
    EXPECT_EQ(aec.in_flight(), 0u);
    EXPECT_EQ(aec.parked(), 0u);
    // The error is surfaced on the runner thread at the next poll.
    EXPECT_THROW(aec.poll(), std::runtime_error);
    // After surfacing, the controller is clean and drain() returns at once.
    aec.drain();
    EXPECT_EQ(aec.in_flight(), 0u);
}
