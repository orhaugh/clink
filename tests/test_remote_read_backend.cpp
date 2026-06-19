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
