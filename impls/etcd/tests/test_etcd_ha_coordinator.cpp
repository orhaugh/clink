// Integration test for the etcd-backed HaCoordinator. Requires a
// reachable etcd at $CLINK_ETCD_ENDPOINT (defaults to
// http://127.0.0.1:2379); self-skips otherwise so the test suite
// stays green on machines without an etcd instance running.
//
// Coverage:
//   * Single coordinator elects itself leader and publishes its
//     advertised endpoint via current_leader_endpoint().
//   * Two coordinators race; exactly one wins. The loser sees the
//     winner's endpoint.
//   * On stop() the leader releases the lease, and the surviving
//     standby is promoted within roughly one poll cycle.

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>

#include <etcd/Client.hpp>
#include <gtest/gtest.h>

#include "clink/cluster/ha_coordinator.hpp"
#include "clink/etcd/etcd_ha_coordinator.hpp"

using namespace clink::cluster;
using namespace std::chrono_literals;

namespace {

std::string etcd_endpoints() {
    if (const char* env = std::getenv("CLINK_ETCD_ENDPOINT"); env != nullptr && *env != '\0') {
        return env;
    }
    return "http://127.0.0.1:2379";
}

// Probe whether etcd is reachable. The etcd-cpp-apiv3 Client
// constructor doesn't open a connection eagerly, so we issue a
// trivial Get and inspect the response.
bool etcd_reachable() {
    try {
        etcd::Client client(etcd_endpoints());
        auto resp = client.get("/clink/__probe__").get();
        // Any response (even "key not found") means we round-tripped
        // a request, which is enough to declare the endpoint healthy.
        return resp.error_code() == 0 || resp.error_code() == 100;  // 100 = key not found
    } catch (...) {
        return false;
    }
}

// Generate a unique cluster name per test so independent runs and
// independent test cases don't fight over the same leader key.
std::string unique_cluster_name(const std::string& tag) {
    static std::atomic<std::uint64_t> counter{0};
    return "ci-" + tag + "-" +
           std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + "-" +
           std::to_string(++counter);
}

EtcdHaConfig make_config(const std::string& cluster) {
    EtcdHaConfig c;
    c.endpoints = etcd_endpoints();
    c.cluster_name = cluster;
    c.lease_ttl = std::chrono::seconds{3};  // short TTL keeps the test fast
    c.retry_interval = std::chrono::milliseconds{100};
    return c;
}

}  // namespace

TEST(EtcdHaCoordinator, SingleCoordinatorBecomesLeader) {
    if (!etcd_reachable()) {
        GTEST_SKIP() << "etcd not reachable at " << etcd_endpoints();
    }
    const auto cluster = unique_cluster_name("single");

    LeaderEndpoint advertise{.host = "10.0.0.1", .port = 6123};
    auto coord = make_etcd_ha_coordinator(make_config(cluster), advertise);

    std::atomic<bool> became_leader{false};
    std::atomic<std::uint64_t> seen_epoch{0};
    coord->set_on_become_leader([&](std::uint64_t epoch) {
        became_leader.store(true);
        seen_epoch.store(epoch);
    });
    coord->start();

    // Give the worker thread a poll cycle or two to acquire.
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (std::chrono::steady_clock::now() < deadline && !coord->is_leader()) {
        std::this_thread::sleep_for(50ms);
    }
    ASSERT_TRUE(coord->is_leader());
    EXPECT_TRUE(became_leader.load());
    EXPECT_GE(seen_epoch.load(), 1u);

    auto ep = coord->current_leader_endpoint();
    ASSERT_TRUE(ep.has_value());
    EXPECT_EQ(ep->host, "10.0.0.1");
    EXPECT_EQ(ep->port, 6123);
    EXPECT_EQ(ep->epoch, seen_epoch.load());

    coord->stop();
}

TEST(EtcdHaCoordinator, OnlyOneOfTwoCoordinatorsWinsTheElection) {
    if (!etcd_reachable()) {
        GTEST_SKIP() << "etcd not reachable at " << etcd_endpoints();
    }
    const auto cluster = unique_cluster_name("contention");

    auto coord_a = make_etcd_ha_coordinator(make_config(cluster),
                                            LeaderEndpoint{.host = "10.0.0.1", .port = 6123});
    auto coord_b = make_etcd_ha_coordinator(make_config(cluster),
                                            LeaderEndpoint{.host = "10.0.0.2", .port = 6124});

    coord_a->start();
    coord_b->start();

    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (std::chrono::steady_clock::now() < deadline && !coord_a->is_leader() &&
           !coord_b->is_leader()) {
        std::this_thread::sleep_for(50ms);
    }
    // Exactly one of them should hold the lock.
    ASSERT_NE(coord_a->is_leader(), coord_b->is_leader())
        << "both or neither became leader simultaneously";

    // The loser still reads the winner's endpoint via etcd. Poll briefly: the
    // winner writes the leader key atomically on acquire, but the loser observes
    // it via its own client, so allow a moment for the read to reflect it (same
    // tolerance the leadership check above already uses).
    auto* loser = coord_a->is_leader() ? coord_b.get() : coord_a.get();
    std::optional<LeaderEndpoint> winner_ep;
    const auto ep_deadline = std::chrono::steady_clock::now() + 3s;
    while (std::chrono::steady_clock::now() < ep_deadline) {
        winner_ep = loser->current_leader_endpoint();
        if (winner_ep.has_value())
            break;
        std::this_thread::sleep_for(50ms);
    }
    ASSERT_TRUE(winner_ep.has_value());
    EXPECT_TRUE(winner_ep->host == "10.0.0.1" || winner_ep->host == "10.0.0.2");

    coord_a->stop();
    coord_b->stop();
}

TEST(EtcdHaCoordinator, FailoverPromotesStandbyAfterLeaderStops) {
    if (!etcd_reachable()) {
        GTEST_SKIP() << "etcd not reachable at " << etcd_endpoints();
    }
    const auto cluster = unique_cluster_name("failover");

    auto coord_a = make_etcd_ha_coordinator(make_config(cluster),
                                            LeaderEndpoint{.host = "10.0.0.1", .port = 6123});
    coord_a->start();
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (std::chrono::steady_clock::now() < deadline && !coord_a->is_leader()) {
        std::this_thread::sleep_for(50ms);
    }
    ASSERT_TRUE(coord_a->is_leader());

    auto coord_b = make_etcd_ha_coordinator(make_config(cluster),
                                            LeaderEndpoint{.host = "10.0.0.2", .port = 6124});
    coord_b->start();
    // B is standby. Give it a moment, confirm it didn't usurp.
    std::this_thread::sleep_for(500ms);
    EXPECT_TRUE(coord_a->is_leader());
    EXPECT_FALSE(coord_b->is_leader());

    // A releases (the lease revoke makes the key vanish immediately;
    // B's next poll picks it up).
    coord_a->stop();
    const auto b_deadline = std::chrono::steady_clock::now() + 5s;
    while (std::chrono::steady_clock::now() < b_deadline && !coord_b->is_leader()) {
        std::this_thread::sleep_for(50ms);
    }
    EXPECT_TRUE(coord_b->is_leader());

    auto ep = coord_b->current_leader_endpoint();
    ASSERT_TRUE(ep.has_value());
    EXPECT_EQ(ep->host, "10.0.0.2");
    EXPECT_EQ(ep->port, 6124);

    coord_b->stop();
}
