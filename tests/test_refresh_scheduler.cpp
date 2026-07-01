// RefreshScheduler component tests (no cluster): due-view firing, the background
// loop, resilience to a throwing refresh, and unregister. The refresh callback is a
// plain counter so the test is deterministic and cluster-free.

#include <atomic>
#include <chrono>
#include <stdexcept>
#include <thread>

#include <gtest/gtest.h>

#include "clink/cluster/refresh_scheduler.hpp"

using clink::cluster::RefreshScheduler;
using clink::cluster::RefreshSchedulerConfig;
using namespace std::chrono_literals;

TEST(RefreshScheduler, TickFiresOnlyDueViews) {
    RefreshScheduler sched;
    std::atomic<int> a{0};
    std::atomic<int> b{0};
    sched.register_view("a", 10ms, [&] { a.fetch_add(1); });
    sched.register_view("b", 100s, [&] { b.fetch_add(1); });  // not due for a long time
    std::this_thread::sleep_for(40ms);                        // 'a' becomes due
    EXPECT_EQ(sched.tick(), 1U);                              // only 'a'
    EXPECT_EQ(a.load(), 1);
    EXPECT_EQ(b.load(), 0);
    EXPECT_EQ(sched.size(), 2U);
}

TEST(RefreshScheduler, LoopFiresRepeatedlyThenStops) {
    RefreshSchedulerConfig cfg;
    cfg.tick_period = 20ms;
    RefreshScheduler sched(cfg);
    std::atomic<int> n{0};
    sched.register_view("v", 20ms, [&] { n.fetch_add(1); });
    sched.start();
    for (int i = 0; i < 200 && n.load() < 3; ++i) {
        std::this_thread::sleep_for(10ms);
    }
    sched.stop();
    const int fired = n.load();
    EXPECT_GE(fired, 3);
    // No further fires once stopped.
    std::this_thread::sleep_for(80ms);
    EXPECT_EQ(n.load(), fired);
}

TEST(RefreshScheduler, ThrowingRefreshDoesNotKillTheLoop) {
    RefreshSchedulerConfig cfg;
    cfg.tick_period = 10ms;
    RefreshScheduler sched(cfg);
    std::atomic<int> n{0};
    sched.register_view("bad", 10ms, [&] {
        n.fetch_add(1);
        throw std::runtime_error("boom");
    });
    sched.start();
    for (int i = 0; i < 200 && n.load() < 3; ++i) {
        std::this_thread::sleep_for(10ms);
    }
    sched.stop();
    EXPECT_GE(n.load(), 3);  // kept firing despite the callback throwing
    EXPECT_GE(sched.refreshes(), 3U);
}

TEST(RefreshScheduler, UnregisterStopsFiring) {
    RefreshScheduler sched;
    std::atomic<int> n{0};
    sched.register_view("v", 1ms, [&] { n.fetch_add(1); });
    std::this_thread::sleep_for(5ms);
    EXPECT_EQ(sched.tick(), 1U);
    EXPECT_EQ(n.load(), 1);
    sched.unregister_view("v");
    EXPECT_FALSE(sched.has_view("v"));
    std::this_thread::sleep_for(5ms);
    EXPECT_EQ(sched.tick(), 0U);
    EXPECT_EQ(n.load(), 1);
}
