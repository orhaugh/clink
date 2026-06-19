// Service discovery tests. Cover the three concrete backends and the
// TaskManager `connect_to_jm(ServiceDiscovery&, ...)` overload, so a TM
// can find its JM without a hardcoded address.

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <thread>

#include <gtest/gtest.h>

#include "clink/cluster/job_manager.hpp"
#include "clink/cluster/service_discovery.hpp"
#include "clink/cluster/task_manager.hpp"

using namespace clink::cluster;
using namespace std::chrono_literals;

TEST(ServiceDiscovery, StaticConfigReturnsConfiguredEndpoint) {
    StaticConfigDiscovery sd("198.51.100.7", 5443);
    auto ep = sd.discover_job_manager(0ms);
    ASSERT_TRUE(ep.has_value());
    EXPECT_EQ(ep->host, "198.51.100.7");
    EXPECT_EQ(ep->port, 5443);
}

TEST(ServiceDiscovery, EnvVarReadsFromEnvironment) {
    ::setenv("CLINK_TEST_JM_HOST", "10.0.0.42", 1);
    ::setenv("CLINK_TEST_JM_PORT", "9000", 1);

    EnvVarDiscovery sd("CLINK_TEST_JM_HOST", "CLINK_TEST_JM_PORT");
    auto ep = sd.discover_job_manager(0ms);
    ASSERT_TRUE(ep.has_value());
    EXPECT_EQ(ep->host, "10.0.0.42");
    EXPECT_EQ(ep->port, 9000);

    ::unsetenv("CLINK_TEST_JM_HOST");
    ::unsetenv("CLINK_TEST_JM_PORT");
}

TEST(ServiceDiscovery, EnvVarTimesOutWhenUnset) {
    ::unsetenv("CLINK_TEST_JM_HOST_MISSING");
    ::unsetenv("CLINK_TEST_JM_PORT_MISSING");

    EnvVarDiscovery sd("CLINK_TEST_JM_HOST_MISSING", "CLINK_TEST_JM_PORT_MISSING");
    EXPECT_FALSE(sd.discover_job_manager(150ms).has_value());
}

TEST(ServiceDiscovery, EnvVarRegisterPublishesViaSetenv) {
    ::unsetenv("CLINK_TEST_REG_HOST");
    ::unsetenv("CLINK_TEST_REG_PORT");

    EnvVarDiscovery sd("CLINK_TEST_REG_HOST", "CLINK_TEST_REG_PORT");
    sd.register_job_manager(JobManagerEndpoint{.host = "172.16.0.1", .port = 4711});

    auto ep = sd.discover_job_manager(0ms);
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
    sd.register_job_manager(JobManagerEndpoint{.host = "192.0.2.5", .port = 6543});

    auto ep = sd.discover_job_manager(0ms);
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
        w.register_job_manager(JobManagerEndpoint{.host = "203.0.113.9", .port = 12345});
    });

    FileDiscovery sd(path);
    auto ep = sd.discover_job_manager(2s);
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
    EXPECT_FALSE(sd.discover_job_manager(150ms).has_value());
}

// End-to-end: a JM publishes its bound port via FileDiscovery; a TM
// independently uses the same backend to discover and connect.
TEST(ServiceDiscovery, TaskManagerConnectsToJmViaDiscovery) {
    auto path = std::filesystem::temp_directory_path() /
                ("clink_sd_e2e_" + std::to_string(::getpid()) + ".endpoint");
    std::filesystem::remove(path);

    JobManager::Config jm_cfg;
    jm_cfg.heartbeat_timeout = 3s;
    jm_cfg.watchdog_interval = 100ms;
    JobManager jm(jm_cfg);

    const auto jm_port = jm.start(0);  // ephemeral
    jm.expect_tms({"tm-discover"});

    {
        FileDiscovery jm_publish(path);
        jm_publish.register_job_manager(JobManagerEndpoint{.host = "127.0.0.1", .port = jm_port});
    }

    TaskManager tm("tm-discover", "127.0.0.1");
    FileDiscovery sd(path);
    tm.connect_to_jm(sd, 2s);

    EXPECT_TRUE(jm.await_registrations(2s));

    tm.stop();
    jm.stop();
    std::filesystem::remove(path);
}
