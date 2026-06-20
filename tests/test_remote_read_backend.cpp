// RemoteReadBackend (ASYNC-8): the first state backend whose reads genuinely
// block on a remote tier, with get_async riding the AsyncExecutionController.
// Proves: the sync path blocks then caches; a hot key resolves with no IO; a
// cold get_async with no scheduler falls back to an inline load; and - the
// headline - a cold get_async driven through the controller loads on an IO
// thread but RESUMES on the runner thread (the controller's single-thread
// invariant), never on the IO thread.

#include <atomic>
#include <chrono>
#include <coroutine>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "clink/runtime/async_execution_controller.hpp"
#include "clink/state/remote_read_backend.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

std::string_view sv(const char* s) {
    return std::string_view{s};
}

StateBackend::Value to_value(const std::string& s) {
    StateBackend::Value v(s.size());
    if (!s.empty()) {
        std::memcpy(v.data(), s.data(), s.size());
    }
    return v;
}

std::string to_string(const StateBackend::Value& v) {
    std::string out(v.size(), '\0');
    if (!v.empty()) {
        std::memcpy(out.data(), v.data(), v.size());
    }
    return out;
}

}  // namespace

TEST(RemoteReadBackend, SyncGetBlocksOnColdThenServesHot) {
    RemoteReadBackend backend([](OperatorId, std::string k) -> std::optional<StateBackend::Value> {
        if (k == "x") {
            return to_value("loaded");
        }
        return std::nullopt;
    });

    auto v = backend.get(OperatorId{1}, sv("x"));  // cold: blocking remote load
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(to_string(*v), "loaded");
    EXPECT_EQ(backend.remote_loads(), 1u);

    auto v2 = backend.get(OperatorId{1}, sv("x"));  // now hot: no new load
    ASSERT_TRUE(v2.has_value());
    EXPECT_EQ(to_string(*v2), "loaded");
    EXPECT_EQ(backend.remote_loads(), 1u);
    EXPECT_EQ(backend.hot_hits(), 1u);

    EXPECT_FALSE(backend.get(OperatorId{1}, sv("missing")).has_value());
}

TEST(RemoteReadBackend, AsyncHotHitResolvesWithoutIo) {
    RemoteReadBackend backend([](OperatorId, std::string) -> std::optional<StateBackend::Value> {
        ADD_FAILURE() << "loader must not run for a hot key";
        return std::nullopt;
    });
    backend.put(OperatorId{1}, sv("k"), sv("v"));

    auto t = backend.get_async(OperatorId{1}, sv("k"));
    t.resume();  // hot hit: completes in one synchronous step
    ASSERT_TRUE(t.done());
    auto got = t.get();
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(to_string(*got), "v");
    EXPECT_EQ(backend.hot_hits(), 1u);
    EXPECT_EQ(backend.remote_loads(), 0u);
}

TEST(RemoteReadBackend, AsyncWithoutSchedulerFallsBackToInlineLoad) {
    RemoteReadBackend backend([](OperatorId, std::string k) -> std::optional<StateBackend::Value> {
        return k == "c" ? std::optional<StateBackend::Value>{to_value("inline")} : std::nullopt;
    });
    // No resume_scheduler wired: a cold get_async does a safe inline blocking
    // load (correct, just not deferred).
    auto t = backend.get_async(OperatorId{1}, sv("c"));
    t.resume();
    ASSERT_TRUE(t.done());
    auto got = t.get();
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(to_string(*got), "inline");
    EXPECT_EQ(backend.remote_loads(), 1u);
}

// The headline: a cold read suspends the record, loads on an IO thread, and
// resumes on the RUNNER thread via the controller - not on the IO thread.
TEST(RemoteReadBackend, AsyncColdReadRidesControllerAndResumesOnRunner) {
    std::atomic<std::thread::id> io_thread{};
    // Declare the controller BEFORE the backend so the backend (whose IO thread
    // may call schedule_resume) is destroyed/quiesced before aec's
    // condition_variable is destroyed (a TSan data race otherwise).
    AsyncExecutionController aec;
    RemoteReadBackend backend([&](OperatorId, std::string k) -> std::optional<StateBackend::Value> {
        io_thread.store(std::this_thread::get_id(), std::memory_order_relaxed);
        std::this_thread::sleep_for(20ms);  // let the runner reach drain() first
        return k == "cold" ? std::optional<StateBackend::Value>{to_value("remote-v")}
                           : std::nullopt;
    });

    backend.set_async_resume_scheduler(
        [&aec](std::coroutine_handle<> h) { aec.schedule_resume(h); });

    std::optional<std::string> resolved;
    std::thread::id resume_thread;
    const bool accepted = aec.submit("cold", [&]() -> async::Task<void> {
        auto v = co_await backend.get_async(OperatorId{1}, sv("cold"));
        if (v) {
            resolved = to_string(*v);
        }
        resume_thread = std::this_thread::get_id();
        co_return;
    });
    ASSERT_TRUE(accepted);

    aec.drain();  // the runner thread (this test thread) resumes the parked read

    ASSERT_TRUE(resolved.has_value());
    EXPECT_EQ(*resolved, "remote-v");
    EXPECT_EQ(backend.remote_loads(), 1u);
    EXPECT_EQ(resume_thread, std::this_thread::get_id());  // resumed on the runner
    EXPECT_NE(io_thread.load(std::memory_order_relaxed), std::thread::id{});
    EXPECT_NE(resume_thread, io_thread.load(std::memory_order_relaxed));  // NOT the IO thread

    // The loaded value is now hot: a follow-up sync get does no new remote load.
    EXPECT_EQ(to_string(*backend.get(OperatorId{1}, sv("cold"))), "remote-v");
    EXPECT_EQ(backend.remote_loads(), 1u);
}

// Pool-backed productionisation: state lives in a durable RemotePool, snapshot
// commits only the delta against the previous checkpoint (incremental), and
// restore is lazy (cold reads serve the restored checkpoint, nothing loaded
// eagerly). Verified here with the in-memory pool double.
TEST(RemoteReadBackend, PoolBackedIncrementalSnapshotAndLazyRestore) {
    auto pool = std::make_shared<InMemoryRemotePool>();
    const OperatorId op{42};

    {
        RemoteReadBackend b(pool);
        b.put(op, sv("a"), sv("v1"));
        b.put(op, sv("b"), sv("v2"));
        b.put(op, sv("d"), sv("v4"));
        b.snapshot(CheckpointId{1});
        // cp2 delta: update a, delete b, add c, leave d untouched (inherited).
        b.put(op, sv("a"), sv("v1b"));
        b.erase(op, sv("b"));
        b.put(op, sv("c"), sv("v3"));
        b.snapshot(CheckpointId{2});
    }  // backend destroyed; state is durable in the pool

    // Fresh backend, lazy restore from cp2.
    RemoteReadBackend b2(pool);
    Snapshot snap;
    snap.checkpoint_id = CheckpointId{2};
    b2.restore(snap);
    EXPECT_EQ(b2.remote_loads(), 0u);  // restore loaded nothing eagerly

    EXPECT_EQ(to_string(*b2.get(op, sv("a"))), "v1b");  // updated in cp2
    EXPECT_EQ(to_string(*b2.get(op, sv("c"))), "v3");   // added in cp2
    EXPECT_EQ(to_string(*b2.get(op, sv("d"))), "v4");   // inherited from cp1
    EXPECT_FALSE(b2.get(op, sv("b")).has_value());      // deleted in cp2
    EXPECT_GT(b2.remote_loads(), 0u);                   // cold reads fetched lazily from the pool

    // Purging the superseded cp1 does not affect cp2 (full materialised ckpt).
    pool->purge(CheckpointId{1});
    RemoteReadBackend b3(pool);
    Snapshot s2;
    s2.checkpoint_id = CheckpointId{2};
    b3.restore(s2);
    EXPECT_EQ(to_string(*b3.get(op, sv("d"))), "v4");
}

// 2c-1 hot-tier eviction: with a byte budget, CLEAN (committed) keys are
// evicted LRU-first to keep the hot tier bounded, and a later read transparently
// re-fetches them from the pool. This is true state-greater-than-RAM: the
// working set exceeds the hot budget yet every key stays readable.
TEST(RemoteReadBackend, HotTierEvictsCleanKeysAndRefetchesFromPool) {
    auto pool = std::make_shared<InMemoryRemotePool>();
    const OperatorId op{7};
    // entry bytes = key.size() + value.size(); "k<i>"/"v<i>" are 2+2 = 4 each.
    // A 12-byte budget holds ~3 entries; 10 keys force eviction.
    RemoteReadBackend b(pool, /*io_threads=*/1, /*hot_max_bytes=*/12);
    for (int i = 0; i < 10; ++i) {
        const std::string k = "k" + std::to_string(i);
        const std::string v = "v" + std::to_string(i);
        b.put(op, std::string_view{k}, std::string_view{v});
    }
    // Pre-checkpoint every write is dirty, so nothing may be evicted yet.
    EXPECT_EQ(b.hot_evictions(), 0u);
    EXPECT_EQ(b.hot_resident_keys(), 10u);

    b.snapshot(CheckpointId{1});  // commit -> all clean -> evict down to budget
    EXPECT_GT(b.hot_evictions(), 0u);
    EXPECT_LE(b.hot_resident_bytes(), 12u);
    EXPECT_LT(b.hot_resident_keys(), 10u);

    // Every key is still readable; evicted ones cold-fetch from the pool.
    const auto loads_before = b.remote_loads();
    for (int i = 0; i < 10; ++i) {
        const std::string k = "k" + std::to_string(i);
        auto v = b.get(op, std::string_view{k});
        ASSERT_TRUE(v.has_value()) << "key " << k;
        EXPECT_EQ(to_string(*v), "v" + std::to_string(i));
    }
    EXPECT_GT(b.remote_loads(), loads_before);  // evicted keys were refetched
    EXPECT_LE(b.hot_resident_bytes(), 12u);     // still bounded after the scan
}

// The data-loss guard: a key written since the last checkpoint (dirty) is NEVER
// evicted, because the pool does not yet hold its latest value. The hot tier may
// transiently exceed the budget; the next checkpoint makes the keys durable and
// only THEN are they evictable.
TEST(RemoteReadBackend, HotTierNeverEvictsDirtyKeys) {
    auto pool = std::make_shared<InMemoryRemotePool>();
    const OperatorId op{9};
    RemoteReadBackend b(pool, 1, /*hot_max_bytes=*/12);
    for (int i = 0; i < 10; ++i) {
        const std::string k = "k" + std::to_string(i);
        const std::string v = "v" + std::to_string(i);
        b.put(op, std::string_view{k}, std::string_view{v});
    }
    EXPECT_EQ(b.hot_evictions(), 0u);  // all dirty: nothing evictable
    EXPECT_EQ(b.hot_resident_keys(), 10u);
    EXPECT_GT(b.hot_resident_bytes(), 12u);  // transiently over budget (correct)
    for (int i = 0; i < 10; ++i) {           // all held hot, no pool reads
        const std::string k = "k" + std::to_string(i);
        EXPECT_EQ(to_string(*b.get(op, std::string_view{k})), "v" + std::to_string(i));
    }
    EXPECT_EQ(b.remote_loads(), 0u);

    b.snapshot(CheckpointId{1});  // durable now -> eviction catches up
    EXPECT_GT(b.hot_evictions(), 0u);
    EXPECT_LT(b.hot_resident_keys(), 10u);
}

// Eviction composes with incremental checkpoints + lazy restore: after eviction,
// updates, and a second checkpoint, a fresh budgeted backend restores cp2 lazily
// and every key reads back correctly (updated values from cp2, others inherited
// from cp1), with the hot tier bounded throughout.
TEST(RemoteReadBackend, EvictionSurvivesIncrementalCheckpointAndLazyRestore) {
    auto pool = std::make_shared<InMemoryRemotePool>();
    const OperatorId op{11};
    {
        RemoteReadBackend b(pool, 1, /*hot_max_bytes=*/12);
        for (int i = 0; i < 8; ++i) {
            const std::string k = "k" + std::to_string(i);
            const std::string v = "v" + std::to_string(i);
            b.put(op, std::string_view{k}, std::string_view{v});
        }
        b.snapshot(CheckpointId{1});  // some clean keys evicted
        EXPECT_GT(b.hot_evictions(), 0u);
        // Update two keys (re-reads them cold first, then writes -> dirty/pinned).
        b.put(op, std::string_view{"k0"}, std::string_view{"v0b"});
        b.put(op, std::string_view{"k1"}, std::string_view{"v1b"});
        b.snapshot(CheckpointId{2});
    }
    RemoteReadBackend b2(pool, 1, /*hot_max_bytes=*/12);
    Snapshot s;
    s.checkpoint_id = CheckpointId{2};
    b2.restore(s);
    EXPECT_EQ(to_string(*b2.get(op, std::string_view{"k0"})), "v0b");  // updated in cp2
    EXPECT_EQ(to_string(*b2.get(op, std::string_view{"k1"})), "v1b");
    for (int i = 2; i < 8; ++i) {
        const std::string k = "k" + std::to_string(i);
        EXPECT_EQ(to_string(*b2.get(op, std::string_view{k})), "v" + std::to_string(i))
            << "inherited key " << k;
    }
    EXPECT_LE(b2.hot_resident_bytes(), 12u);  // bounded throughout the restore reads
}

// Adversarial-review finding (data-loss): a key erased since the last
// checkpoint is logically absent, even though the pool still holds its
// pre-erase value (the delete only commits at the next checkpoint). A cold read
// must NOT load + return that stale value, and must not re-cache it.
TEST(RemoteReadBackend, ColdReadOfErasedKeyReturnsAbsentBeforeCheckpoint) {
    auto pool = std::make_shared<InMemoryRemotePool>();
    const OperatorId op{13};
    RemoteReadBackend b(pool);
    b.put(op, sv("x"), sv("v0"));
    b.snapshot(CheckpointId{1});  // x=v0 durable in the pool
    b.erase(op, sv("x"));         // erased -> hot miss, delete pending commit

    EXPECT_FALSE(b.get(op, sv("x")).has_value());  // absent, NOT the stale v0
    EXPECT_EQ(b.remote_loads(), 0u);               // short-circuited; never hit the pool

    auto t = b.get_async(op, sv("x"));  // async path honours the delete too
    t.resume();
    ASSERT_TRUE(t.done());
    EXPECT_FALSE(t.get().has_value());

    b.snapshot(CheckpointId{2});  // delete now durable
    EXPECT_FALSE(b.get(op, sv("x")).has_value());

    RemoteReadBackend b2(pool);  // fresh restore from cp2 also sees it gone
    Snapshot s;
    s.checkpoint_id = CheckpointId{2};
    b2.restore(s);
    EXPECT_FALSE(b2.get(op, sv("x")).has_value());
}

// Adversarial-review finding (interaction): restore() must drop a stale hot
// tier so a reused backend instance that restores a DIFFERENT checkpoint does
// not keep serving the previous checkpoint's cached values.
TEST(RemoteReadBackend, RestoreClearsStaleHotTierAcrossCheckpoints) {
    auto pool = std::make_shared<InMemoryRemotePool>();
    const OperatorId op{17};
    {
        RemoteReadBackend w(pool);
        w.put(op, sv("k"), sv("cp1"));
        w.snapshot(CheckpointId{1});
        w.put(op, sv("k"), sv("cp2"));
        w.snapshot(CheckpointId{2});
    }
    RemoteReadBackend b(pool);
    Snapshot s1;
    s1.checkpoint_id = CheckpointId{1};
    b.restore(s1);
    EXPECT_EQ(to_string(*b.get(op, sv("k"))), "cp1");  // cold-loads cp1 into hot_

    Snapshot s2;
    s2.checkpoint_id = CheckpointId{2};
    b.restore(s2);                                     // re-restore a different checkpoint
    EXPECT_EQ(to_string(*b.get(op, sv("k"))), "cp2");  // hot cleared -> serves cp2, not stale cp1
}
