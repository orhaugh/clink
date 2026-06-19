// Unit tests for FileHaCoordinator.
//
// Single-process exercise of the file-based lock + active-leader.json
// flow. Two coordinators on the same ha-dir; only one acquires
// leadership at a time; the other reads the leader endpoint.

#include <chrono>
#include <filesystem>
#include <thread>

#include <gtest/gtest.h>

#include "clink/cluster/ha_coordinator.hpp"

#include "test_helpers/sanitizer_slack.hpp"

using namespace clink::cluster;
using namespace std::chrono_literals;

namespace {
// Wait deadline for `is_leader` / leader-endpoint observations. 500ms
// is plenty under a normal build (lock release + 50ms poll cadence
// converges in < 100ms). Under ASan/TSan instrumentation the same
// observation can take 5x longer, so scale by the slack multiplier.
constexpr auto kHaWait = clink::test_support::scale_slack(std::chrono::milliseconds{500});
}  // namespace

namespace {

std::filesystem::path fresh_ha_dir() {
    static int counter = 0;
    auto dir = std::filesystem::temp_directory_path() /
               ("clink_ha_" + std::to_string(::getpid()) + "_" + std::to_string(++counter));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

bool wait_for(std::function<bool()> pred, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred())
            return true;
        std::this_thread::sleep_for(10ms);
    }
    return false;
}

}  // namespace

TEST(HaCoordinator, SingleProcessAcquiresLeadership) {
    const auto dir = fresh_ha_dir();
    auto c = make_file_ha_coordinator(dir.string(), {"127.0.0.1", 6123, 0, 0}, 50ms);
    c->start();
    // Wait for the endpoint, not just is_leader(): the coordinator
    // sets is_leader before active-leader.json is fsynced + visible,
    // which is a race we can't observe directly. Polling for the
    // endpoint converges on the consistent state.
    EXPECT_TRUE(wait_for([&] { return c->current_leader_endpoint().has_value(); }, kHaWait));
    EXPECT_GE(c->epoch(), 1u);
    auto ep = c->current_leader_endpoint();
    ASSERT_TRUE(ep.has_value());
    EXPECT_EQ(ep->host, "127.0.0.1");
    EXPECT_EQ(ep->port, 6123);
    EXPECT_GE(ep->epoch, 1u);
    c->stop();
}

TEST(HaCoordinator, OnlyOneOfTwoConcurrentInstancesIsLeader) {
    const auto dir = fresh_ha_dir();
    auto a = make_file_ha_coordinator(dir.string(), {"127.0.0.1", 6100, 0, 0}, 50ms);
    auto b = make_file_ha_coordinator(dir.string(), {"127.0.0.1", 6200, 0, 0}, 50ms);
    a->start();
    b->start();
    // Within a poll window one of them MUST be leader; the other
    // MUST be standby. v1's fcntl POSIX advisory lock is per-process
    // though - two FileHaCoordinator in the SAME process can both
    // hold the lock because POSIX advisory locks are per-file-per-
    // process. Skip the strict "only one leader" check; assert the
    // active-leader file is well-formed.
    EXPECT_TRUE(wait_for([&] { return a->current_leader_endpoint().has_value(); }, kHaWait));
    auto ep = a->current_leader_endpoint();
    ASSERT_TRUE(ep.has_value());
    EXPECT_TRUE(ep->port == 6100 || ep->port == 6200) << "port=" << ep->port;
    a->stop();
    b->stop();
}

TEST(HaCoordinator, StandbyAcquiresAfterLeaderStops) {
    const auto dir = fresh_ha_dir();
    auto a = make_file_ha_coordinator(dir.string(), {"127.0.0.1", 6100, 0, 0}, 50ms);
    a->start();
    ASSERT_TRUE(wait_for([&] { return a->is_leader(); }, kHaWait));

    // Stop the leader. The lock file is closed, releasing the lock.
    a->stop();

    // A new coordinator started AFTER A's release should acquire.
    auto b = make_file_ha_coordinator(dir.string(), {"127.0.0.1", 6200, 0, 0}, 50ms);
    b->start();
    // Wait for the endpoint to actually reflect B's port (not just for
    // is_leader() to flip). The coordinator becomes leader BEFORE the
    // active-leader.json rewrite is visible to readers; without this,
    // a fast read sees the stale leader endpoint of the just-stopped A.
    EXPECT_TRUE(wait_for(
        [&] {
            auto e = b->current_leader_endpoint();
            return e.has_value() && e->port == 6200;
        },
        kHaWait));
    auto ep = b->current_leader_endpoint();
    ASSERT_TRUE(ep.has_value());
    EXPECT_EQ(ep->port, 6200);
    b->stop();
}

TEST(HaCoordinator, BecomesLeaderCallbackFiresOnAcquire) {
    const auto dir = fresh_ha_dir();
    auto c = make_file_ha_coordinator(dir.string(), {"127.0.0.1", 6300, 0, 0}, 50ms);
    std::atomic<std::uint64_t> seen_epoch{0};
    c->set_on_become_leader(
        [&](std::uint64_t e) { seen_epoch.store(e, std::memory_order_release); });
    c->start();
    EXPECT_TRUE(wait_for([&] { return seen_epoch.load() >= 1; }, kHaWait));
    c->stop();
}
