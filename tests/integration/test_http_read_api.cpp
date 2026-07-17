// Read-API integration test for the HTTP subsystem.
//
// Spawns a coordinator + 2 workers (all with --http-port set), submits a job, then
// scrapes each endpoint and asserts the JSON contains the expected
// fields. Validates the data plane the dashboard SPA will consume.
//
// Coverage matrix (one TEST per endpoint family):
//   GET /api/v1/cluster           - cluster rollup, embedded workers
//   GET /api/v1/config            - coordinator Config snapshot
//   GET /api/v1/jobs              - job list + per-job summary fields
//   GET /api/v1/jobs/:id          - full job detail + 404 path
//   GET /api/v1/workers               - registered workers
//   GET /api/v1/worker (on a worker)      - worker self-view
//   GET /api/v1/worker/subtasks       - subtask list
//   GET /api/v1/config (on a worker)  - worker Config snapshot

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <signal.h>
#include <spawn.h>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>
#include <sys/wait.h>

#include "clink/core/codec.hpp"
#include "clink/runtime/network/network_channel.hpp"
#include "clink/runtime/network/network_socket.hpp"

extern char** environ;

namespace {

using namespace clink;
using namespace clink::network;
using namespace std::chrono_literals;

std::filesystem::path node_binary_path() {
#ifdef CLINK_NODE_BINARY
    return std::filesystem::path{CLINK_NODE_BINARY};
#else
    return {};
#endif
}

std::filesystem::path submit_binary_path() {
#ifdef CLINK_SUBMIT_BINARY
    return std::filesystem::path{CLINK_SUBMIT_BINARY};
#else
    return {};
#endif
}

std::filesystem::path cancel_test_job_path() {
#ifdef CLINK_CANCEL_TEST_JOB_PATH
    return std::filesystem::path{CLINK_CANCEL_TEST_JOB_PATH};
#else
    return {};
#endif
}

pid_t spawn_proc(const std::vector<std::string>& argv, const std::filesystem::path& binary) {
    std::vector<char*> raw;
    raw.reserve(argv.size() + 1);
    for (const auto& s : argv) {
        raw.push_back(const_cast<char*>(s.c_str()));
    }
    raw.push_back(nullptr);
    pid_t pid = -1;
    const auto rc = posix_spawn(&pid, binary.c_str(), nullptr, nullptr, raw.data(), environ);
    return rc == 0 ? pid : -1;
}

void kill_quietly(pid_t pid) {
    if (pid > 0) {
        ::kill(pid, SIGKILL);
        int s = 0;
        ::waitpid(pid, &s, 0);
    }
}

std::uint16_t probe_free_port() {
    NetworkChannelSource<std::int64_t> probe(0, int64_codec());
    return probe.listen();
}

// Tiny synchronous HTTP/1.1 client - same shape as the one in
// test_http_health.cpp. Returns the response body or "" on error.
struct HttpResult {
    int status{0};
    std::string body;
};

HttpResult http_get(const std::string& host, std::uint16_t port, const std::string& path) {
    HttpResult r;
    const int fd = NetworkSocket::connect_to(host, port);
    if (fd < 0) {
        return r;
    }
    const std::string req =
        "GET " + path + " HTTP/1.1\r\nHost: " + host + "\r\nConnection: close\r\n\r\n";
    NetworkSocket::send_all(fd, reinterpret_cast<const std::byte*>(req.data()), req.size());
    std::string buf;
    char chunk[4096];
    while (true) {
        const auto n = ::read(fd, chunk, sizeof(chunk));
        if (n <= 0) {
            break;
        }
        buf.append(chunk, static_cast<std::size_t>(n));
    }
    NetworkSocket::close(fd);
    // Parse status code from "HTTP/1.1 <code> <reason>".
    if (buf.size() >= 12) {
        try {
            r.status = std::stoi(buf.substr(9, 3));
        } catch (...) {
        }
    }
    const auto sep = buf.find("\r\n\r\n");
    if (sep != std::string::npos) {
        r.body = buf.substr(sep + 4);
    }
    return r;
}

bool await_http_ready(std::uint16_t port, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        const auto r = http_get("127.0.0.1", port, "/api/v1/health");
        if (r.status == 200) {
            return true;
        }
        std::this_thread::sleep_for(20ms);
    }
    return false;
}

// Holder for a coordinator + 2 workers all with HTTP ports up. Tests share this
// setup via a fixture-ish helper rather than a true gtest fixture
// because we want each TEST to own its own ports / processes (so
// failures are isolated).
struct Cluster {
    pid_t coordinator_pid{-1};
    std::uint16_t coordinator_http_port{0};
    std::uint16_t coordinator_control_port{0};
    std::vector<pid_t> worker_pids;
    std::vector<std::uint16_t> worker_http_ports;
    std::filesystem::path node_binary;

    Cluster() = default;
    Cluster(const Cluster&) = delete;
    Cluster& operator=(const Cluster&) = delete;
    // Move: leave the source with sentinel pids so its destructor is
    // a no-op. Without this, moving a Cluster into std::optional via
    // `return c;` causes the source's destructor to SIGKILL the same
    // pids the destination is about to use.
    Cluster(Cluster&& other) noexcept
        : coordinator_pid(other.coordinator_pid),
          coordinator_http_port(other.coordinator_http_port),
          coordinator_control_port(other.coordinator_control_port),
          worker_pids(std::move(other.worker_pids)),
          worker_http_ports(std::move(other.worker_http_ports)),
          node_binary(std::move(other.node_binary)) {
        other.coordinator_pid = -1;
    }
    Cluster& operator=(Cluster&& other) noexcept {
        if (this != &other) {
            this->~Cluster();
            new (this) Cluster(std::move(other));
        }
        return *this;
    }

    ~Cluster() {
        for (auto pid : worker_pids) {
            kill_quietly(pid);
        }
        kill_quietly(coordinator_pid);
    }
};

std::optional<Cluster> start_cluster(int n_workers) {
    Cluster c;
    c.node_binary = node_binary_path();
    if (!std::filesystem::exists(c.node_binary)) {
        return std::nullopt;
    }
    c.coordinator_control_port = probe_free_port();
    c.coordinator_http_port = probe_free_port();
    c.coordinator_pid = spawn_proc({"clink_node",
                                    "--role=coordinator",
                                    "--port=" + std::to_string(c.coordinator_control_port),
                                    "--http-port=" + std::to_string(c.coordinator_http_port),
                                    "--http-bind=127.0.0.1"},
                                   c.node_binary);
    if (c.coordinator_pid <= 0) {
        return std::nullopt;
    }
    if (!await_http_ready(c.coordinator_http_port, 2s)) {
        return std::nullopt;
    }
    for (int i = 1; i <= n_workers; ++i) {
        const auto http_port = probe_free_port();
        const pid_t pid =
            spawn_proc({"clink_node",
                        "--role=worker",
                        "--id=worker-read-" + std::to_string(i),
                        "--coordinator-host=127.0.0.1",
                        "--coordinator-port=" + std::to_string(c.coordinator_control_port),
                        "--http-port=" + std::to_string(http_port),
                        "--http-bind=127.0.0.1"},
                       c.node_binary);
        if (pid <= 0 || !await_http_ready(http_port, 2s)) {
            return std::nullopt;
        }
        c.worker_pids.push_back(pid);
        c.worker_http_ports.push_back(http_port);
    }
    // Give the workers a moment to register with the coordinator before tests
    // assert on cluster snapshots.
    std::this_thread::sleep_for(300ms);
    return c;
}

}  // namespace

TEST(HttpReadApi, ClusterEndpointReportsRegisteredWorkers) {
    auto cluster = start_cluster(/*n_workers=*/2);
    if (!cluster.has_value()) {
        GTEST_SKIP() << "cluster startup failed (binaries missing or port collision)";
    }
    const auto r = http_get("127.0.0.1", cluster->coordinator_http_port, "/api/v1/cluster");
    ASSERT_EQ(r.status, 200) << "body: " << r.body;
    EXPECT_NE(r.body.find("\"jobs_total\":0"), std::string::npos);
    EXPECT_NE(r.body.find("\"workers\""), std::string::npos);
    EXPECT_NE(r.body.find("worker-read-1"), std::string::npos)
        << "worker-read-1 not visible in /api/v1/cluster: " << r.body;
    EXPECT_NE(r.body.find("worker-read-2"), std::string::npos)
        << "worker-read-2 not visible in /api/v1/cluster: " << r.body;
}

TEST(HttpReadApi, CoordinatorConfigEndpointReturnsConfigFields) {
    auto cluster = start_cluster(/*n_workers=*/1);
    if (!cluster.has_value()) {
        GTEST_SKIP() << "cluster startup failed";
    }
    const auto r = http_get("127.0.0.1", cluster->coordinator_http_port, "/api/v1/config");
    ASSERT_EQ(r.status, 200) << "body: " << r.body;
    EXPECT_NE(r.body.find("\"bind_host\""), std::string::npos);
    EXPECT_NE(r.body.find("\"heartbeat_timeout_ms\""), std::string::npos);
    EXPECT_NE(r.body.find("\"watchdog_interval_ms\""), std::string::npos);
}

TEST(HttpReadApi, JobsEndpointEmptyThenPopulatedAfterSubmission) {
    const auto submit = submit_binary_path();
    const auto job_so = cancel_test_job_path();
    if (!std::filesystem::exists(submit) || !std::filesystem::exists(job_so)) {
        GTEST_SKIP() << "submitter or cancel_test_job.so not built";
    }
    auto cluster = start_cluster(/*n_workers=*/2);
    if (!cluster.has_value()) {
        GTEST_SKIP() << "cluster startup failed";
    }

    // Before any submit, /api/v1/jobs is an empty list.
    {
        const auto r = http_get("127.0.0.1", cluster->coordinator_http_port, "/api/v1/jobs");
        ASSERT_EQ(r.status, 200) << "body: " << r.body;
        EXPECT_NE(r.body.find("\"jobs\":[]"), std::string::npos)
            << "expected empty job list, got: " << r.body;
    }

    // Submit cancel_test_job (which runs forever). We don't wait for
    // it to finish - we just verify the coordinator tracks it.
    ::setenv("CLINK_CANCEL_TICK_MS", "100", 1);
    const pid_t submit_pid =
        spawn_proc({"clink_submit_job",
                    "--job=" + job_so.string(),
                    "--coordinator-host=127.0.0.1",
                    "--coordinator-port=" + std::to_string(cluster->coordinator_control_port),
                    "--wait-timeout-s=20"},
                   submit);
    ASSERT_GT(submit_pid, 0);
    std::this_thread::sleep_for(2s);

    // /api/v1/jobs should now contain at least job_id=1.
    {
        const auto r = http_get("127.0.0.1", cluster->coordinator_http_port, "/api/v1/jobs");
        ASSERT_EQ(r.status, 200);
        EXPECT_NE(r.body.find("\"id\":1"), std::string::npos)
            << "expected job id=1 in list: " << r.body;
    }

    // /api/v1/jobs/1 should return detail including the tasks array
    // and the per-job worker placement.
    {
        const auto r = http_get("127.0.0.1", cluster->coordinator_http_port, "/api/v1/jobs/1");
        ASSERT_EQ(r.status, 200) << "body: " << r.body;
        EXPECT_NE(r.body.find("\"id\":1"), std::string::npos);
        EXPECT_NE(r.body.find("\"tasks\""), std::string::npos);
        EXPECT_NE(r.body.find("worker-read-"), std::string::npos)
            << "expected a worker id under tasks[].worker_id: " << r.body;
    }

    // 404 path.
    {
        const auto r = http_get("127.0.0.1", cluster->coordinator_http_port, "/api/v1/jobs/9999");
        EXPECT_EQ(r.status, 404);
    }

    kill_quietly(submit_pid);
}

TEST(HttpReadApi, WorkersEndpointListsRegisteredWorkers) {
    auto cluster = start_cluster(/*n_workers=*/2);
    if (!cluster.has_value()) {
        GTEST_SKIP() << "cluster startup failed";
    }
    const auto r = http_get("127.0.0.1", cluster->coordinator_http_port, "/api/v1/workers");
    ASSERT_EQ(r.status, 200);
    EXPECT_NE(r.body.find("worker-read-1"), std::string::npos);
    EXPECT_NE(r.body.find("worker-read-2"), std::string::npos);
    EXPECT_NE(r.body.find("\"slot_capacity\""), std::string::npos);
}

TEST(HttpReadApi, WorkerEndpointReportsSelfView) {
    auto cluster = start_cluster(/*n_workers=*/1);
    if (!cluster.has_value()) {
        GTEST_SKIP() << "cluster startup failed";
    }
    const auto r = http_get("127.0.0.1", cluster->worker_http_ports[0], "/api/v1/worker");
    ASSERT_EQ(r.status, 200);
    EXPECT_NE(r.body.find("worker-read-1"), std::string::npos);
    EXPECT_NE(r.body.find("\"coordinator_host\":\"127.0.0.1\""), std::string::npos);
    EXPECT_NE(r.body.find("\"slot_capacity\""), std::string::npos);
}

TEST(HttpReadApi, WorkerSubtasksEmptyBeforeJob) {
    auto cluster = start_cluster(/*n_workers=*/1);
    if (!cluster.has_value()) {
        GTEST_SKIP() << "cluster startup failed";
    }
    const auto r = http_get("127.0.0.1", cluster->worker_http_ports[0], "/api/v1/worker/subtasks");
    ASSERT_EQ(r.status, 200) << "body: " << r.body;
    EXPECT_NE(r.body.find("\"subtasks\":[]"), std::string::npos)
        << "expected empty subtasks before any deploy: " << r.body;
}

TEST(HttpReadApi, WorkerConfigReturnsSlotCount) {
    auto cluster = start_cluster(/*n_workers=*/1);
    if (!cluster.has_value()) {
        GTEST_SKIP() << "cluster startup failed";
    }
    const auto r = http_get("127.0.0.1", cluster->worker_http_ports[0], "/api/v1/config");
    ASSERT_EQ(r.status, 200);
    EXPECT_NE(r.body.find("\"slot_count\":4"), std::string::npos)
        << "expected default slot_count=4 in config: " << r.body;
}
