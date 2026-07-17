// Service discovery tests. Cover the three concrete backends and the
// Worker `connect_to_coordinator(ServiceDiscovery&, ...)` overload, so a worker
// can find its coordinator without a hardcoded address.

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <thread>

#include <gtest/gtest.h>

#include "clink/cluster/coordinator.hpp"
#include "clink/cluster/service_discovery.hpp"
#include "clink/cluster/worker.hpp"

using namespace clink::cluster;
using namespace std::chrono_literals;

TEST(ServiceDiscovery, StaticConfigReturnsConfiguredEndpoint) {
    StaticConfigDiscovery sd("198.51.100.7", 5443);
    auto ep = sd.discover_coordinator(0ms);
    ASSERT_TRUE(ep.has_value());
    EXPECT_EQ(ep->host, "198.51.100.7");
    EXPECT_EQ(ep->port, 5443);
}

TEST(ServiceDiscovery, EnvVarReadsFromEnvironment) {
    ::setenv("CLINK_TEST_COORDINATOR_HOST", "10.0.0.42", 1);
    ::setenv("CLINK_TEST_COORDINATOR_PORT", "9000", 1);

    EnvVarDiscovery sd("CLINK_TEST_COORDINATOR_HOST", "CLINK_TEST_COORDINATOR_PORT");
    auto ep = sd.discover_coordinator(0ms);
    ASSERT_TRUE(ep.has_value());
    EXPECT_EQ(ep->host, "10.0.0.42");
    EXPECT_EQ(ep->port, 9000);

    ::unsetenv("CLINK_TEST_COORDINATOR_HOST");
    ::unsetenv("CLINK_TEST_COORDINATOR_PORT");
}

TEST(ServiceDiscovery, EnvVarTimesOutWhenUnset) {
    ::unsetenv("CLINK_TEST_COORDINATOR_HOST_MISSING");
    ::unsetenv("CLINK_TEST_COORDINATOR_PORT_MISSING");

    EnvVarDiscovery sd("CLINK_TEST_COORDINATOR_HOST_MISSING",
                       "CLINK_TEST_COORDINATOR_PORT_MISSING");
    EXPECT_FALSE(sd.discover_coordinator(150ms).has_value());
}

TEST(ServiceDiscovery, EnvVarRegisterPublishesViaSetenv) {
    ::unsetenv("CLINK_TEST_REG_HOST");
    ::unsetenv("CLINK_TEST_REG_PORT");

    EnvVarDiscovery sd("CLINK_TEST_REG_HOST", "CLINK_TEST_REG_PORT");
    sd.register_coordinator(CoordinatorEndpoint{.host = "172.16.0.1", .port = 4711});

    auto ep = sd.discover_coordinator(0ms);
    ASSERT_TRUE(ep.has_value());
    EXPECT_EQ(ep->host, "172.16.0.1");
    EXPECT_EQ(ep->port, 4711);

    ::unsetenv("CLINK_TEST_REG_HOST");
    ::unsetenv("CLINK_TEST_REG_PORT");
}

TEST(ServiceDiscovery, FileBackendRoundTripsEndpoint) {
    auto path = std::filesystem::temp_directory_path() /
                ("clink_sd_" + std::to_string(::getpid()) + ".endpoint");
    std::filesystem::remove(path);

    FileDiscovery sd(path);
    sd.register_coordinator(CoordinatorEndpoint{.host = "192.0.2.5", .port = 6543});

    auto ep = sd.discover_coordinator(0ms);
    ASSERT_TRUE(ep.has_value());
    EXPECT_EQ(ep->host, "192.0.2.5");
    EXPECT_EQ(ep->port, 6543);

    std::filesystem::remove(path);
}

TEST(ServiceDiscovery, FileBackendWaitsForFileToAppear) {
    auto path = std::filesystem::temp_directory_path() /
                ("clink_sd_late_" + std::to_string(::getpid()) + ".endpoint");
    std::filesystem::remove(path);

    // Writer fires after a short delay so the reader has to actually poll.
    std::thread writer([&path] {
        std::this_thread::sleep_for(150ms);
        FileDiscovery w(path);
        w.register_coordinator(CoordinatorEndpoint{.host = "203.0.113.9", .port = 12345});
    });

    FileDiscovery sd(path);
    auto ep = sd.discover_coordinator(2s);
    writer.join();

    ASSERT_TRUE(ep.has_value());
    EXPECT_EQ(ep->host, "203.0.113.9");
    EXPECT_EQ(ep->port, 12345);

    std::filesystem::remove(path);
}

TEST(ServiceDiscovery, FileBackendTimesOutWhenFileMissing) {
    auto path = std::filesystem::temp_directory_path() /
                ("clink_sd_never_" + std::to_string(::getpid()) + ".endpoint");
    std::filesystem::remove(path);

    FileDiscovery sd(path);
    EXPECT_FALSE(sd.discover_coordinator(150ms).has_value());
}

// End-to-end: a coordinator publishes its bound port via FileDiscovery; a worker
// independently uses the same backend to discover and connect.
TEST(ServiceDiscovery, WorkerConnectsToCoordinatorViaDiscovery) {
    auto path = std::filesystem::temp_directory_path() /
                ("clink_sd_e2e_" + std::to_string(::getpid()) + ".endpoint");
    std::filesystem::remove(path);

    Coordinator::Config coordinator_cfg;
    coordinator_cfg.heartbeat_timeout = 3s;
    coordinator_cfg.watchdog_interval = 100ms;
    Coordinator coordinator(coordinator_cfg);

    const auto coordinator_port = coordinator.start(0);  // ephemeral
    coordinator.expect_workers({"worker-discover"});

    {
        FileDiscovery coordinator_publish(path);
        coordinator_publish.register_coordinator(
            CoordinatorEndpoint{.host = "127.0.0.1", .port = coordinator_port});
    }

    Worker worker("worker-discover", "127.0.0.1");
    FileDiscovery sd(path);
    worker.connect_to_coordinator(sd, 2s);

    EXPECT_TRUE(coordinator.await_registrations(2s));

    worker.stop();
    coordinator.stop();
    std::filesystem::remove(path);
}
