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
    RemoteReadBackend backend([&](OperatorId, std::string k) -> std::optional<StateBackend::Value> {
        io_thread.store(std::this_thread::get_id(), std::memory_order_relaxed);
        std::this_thread::sleep_for(20ms);  // let the runner reach drain() first
        return k == "cold" ? std::optional<StateBackend::Value>{to_value("remote-v")}
                           : std::nullopt;
    });

    AsyncExecutionController aec;
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
