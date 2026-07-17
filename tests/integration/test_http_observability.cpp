// HTTP-4 observability tests.
//
//   HTTP-4a/b: /metrics on coordinator and worker returns Prometheus text exposition,
//              with at least the process-level metric names from
//              process_metrics.hpp. After a worker registers, coordinator_workers_registered
//              shows up; after a subtask runs, worker_subtasks_*_total bumps.
//   HTTP-4c:   /api/v1/logs returns a JSON `{"logs": [...]}` with the
//              cluster-lifecycle events we emit (worker register on coordinator,
//              subtask start on worker).
//   federated: GET /api/v1/workers/:id/metrics and /api/v1/workers/:id/logs on the
//              coordinator proxy to the corresponding worker endpoints.

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
#include "clink/metrics/process_metrics.hpp"
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

struct HttpResult {
    int status{0};
    std::string body;
    std::string content_type;
};

HttpResult http_request(const std::string& host,
                        std::uint16_t port,
                        const std::string& method,
                        const std::string& path) {
    HttpResult r;
    const int fd = NetworkSocket::connect_to(host, port);
    if (fd < 0) {
        return r;
    }
    const std::string req = method + " " + path + " HTTP/1.1\r\nHost: " + host +
                            "\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
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
    if (buf.size() >= 12) {
        try {
            r.status = std::stoi(buf.substr(9, 3));
        } catch (...) {
        }
    }
    // Crude Content-Type sniff for the Prometheus assertion.
    const auto ct_key = std::string{"\r\nContent-Type:"};
    auto cti = buf.find(ct_key);
    if (cti != std::string::npos) {
        auto eol = buf.find("\r\n", cti + ct_key.size());
        if (eol != std::string::npos) {
            r.content_type = buf.substr(cti + ct_key.size(), eol - (cti + ct_key.size()));
            while (!r.content_type.empty() && r.content_type.front() == ' ') {
                r.content_type.erase(0, 1);
            }
        }
    }
    const auto sep = buf.find("\r\n\r\n");
    if (sep != std::string::npos) {
        r.body = buf.substr(sep + 4);
    }
    return r;
}

HttpResult http_get(const std::string& host, std::uint16_t port, const std::string& path) {
    return http_request(host, port, "GET", path);
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

struct Cluster {
    pid_t coordinator_pid{-1};
    std::uint16_t coordinator_http_port{0};
    std::uint16_t coordinator_control_port{0};
    std::vector<pid_t> worker_pids;
    std::vector<std::string> worker_ids;
    std::vector<std::uint16_t> worker_http_ports;

    Cluster() = default;
    Cluster(const Cluster&) = delete;
    Cluster& operator=(const Cluster&) = delete;
    Cluster(Cluster&& o) noexcept
        : coordinator_pid(o.coordinator_pid),
          coordinator_http_port(o.coordinator_http_port),
          coordinator_control_port(o.coordinator_control_port),
          worker_pids(std::move(o.worker_pids)),
          worker_ids(std::move(o.worker_ids)),
          worker_http_ports(std::move(o.worker_http_ports)) {
        o.coordinator_pid = -1;
    }
    Cluster& operator=(Cluster&& o) noexcept {
        if (this != &o) {
            this->~Cluster();
            new (this) Cluster(std::move(o));
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
    const auto node = node_binary_path();
    if (!std::filesystem::exists(node)) {
        return std::nullopt;
    }
    c.coordinator_control_port = probe_free_port();
    c.coordinator_http_port = probe_free_port();
    c.coordinator_pid = spawn_proc({"clink_node",
                                    "--role=coordinator",
                                    "--port=" + std::to_string(c.coordinator_control_port),
                                    "--http-port=" + std::to_string(c.coordinator_http_port),
                                    "--http-bind=127.0.0.1"},
                                   node);
    if (c.coordinator_pid <= 0 || !await_http_ready(c.coordinator_http_port, 2s)) {
        return std::nullopt;
    }
    for (int i = 1; i <= n_workers; ++i) {
        const auto http_port = probe_free_port();
        const std::string worker_id = "worker-obs-" + std::to_string(i);
        const pid_t pid =
            spawn_proc({"clink_node",
                        "--role=worker",
                        "--id=" + worker_id,
                        "--coordinator-host=127.0.0.1",
                        "--coordinator-port=" + std::to_string(c.coordinator_control_port),
                        "--http-port=" + std::to_string(http_port),
                        "--http-bind=127.0.0.1"},
                       node);
        if (pid <= 0 || !await_http_ready(http_port, 2s)) {
            return std::nullopt;
        }
        c.worker_pids.push_back(pid);
        c.worker_ids.push_back(worker_id);
        c.worker_http_ports.push_back(http_port);
    }
    std::this_thread::sleep_for(400ms);
    return c;
}

}  // namespace

TEST(HttpObservability, CoordinatorMetricsEndpointReturnsPrometheus) {
    auto c = start_cluster(/*n_workers=*/1);
    if (!c.has_value()) {
        GTEST_SKIP() << "cluster startup failed";
    }
    const auto r = http_get("127.0.0.1", c->coordinator_http_port, "/metrics");
    ASSERT_EQ(r.status, 200);
    EXPECT_NE(r.content_type.find("text/plain"), std::string::npos)
        << "content type: " << r.content_type;
    // Prometheus TYPE lines for the process metrics we wired.
    EXPECT_NE(
        r.body.find("# TYPE " + std::string{metrics::kCoordinatorWorkersRegistered} + " gauge"),
        std::string::npos)
        << "body: " << r.body;
    EXPECT_NE(
        r.body.find("# TYPE " + std::string{metrics::kCoordinatorJobsSubmittedTotal} + " counter"),
        std::string::npos)
        << "body: " << r.body;
    // Single worker was registered before we hit /metrics.
    EXPECT_NE(r.body.find(std::string{metrics::kCoordinatorWorkersRegistered} + " 1"),
              std::string::npos)
        << "body: " << r.body;
}

TEST(HttpObservability, WorkerMetricsEndpointReturnsPrometheus) {
    auto c = start_cluster(/*n_workers=*/1);
    if (!c.has_value()) {
        GTEST_SKIP() << "cluster startup failed";
    }
    const auto r = http_get("127.0.0.1", c->worker_http_ports[0], "/metrics");
    ASSERT_EQ(r.status, 200);
    EXPECT_NE(r.body.find("# TYPE " + std::string{metrics::kWorkerSlotsCapacity} + " gauge"),
              std::string::npos)
        << "body: " << r.body;
    // Default worker slot count = 4 (set in run_worker via --slots default).
    EXPECT_NE(r.body.find(std::string{metrics::kWorkerSlotsCapacity} + " 4"), std::string::npos)
        << "body: " << r.body;
}

TEST(HttpObservability, CoordinatorLogsEndpointHasRegisterEntry) {
    auto c = start_cluster(/*n_workers=*/2);
    if (!c.has_value()) {
        GTEST_SKIP() << "cluster startup failed";
    }
    const auto r = http_get("127.0.0.1", c->coordinator_http_port, "/api/v1/logs");
    ASSERT_EQ(r.status, 200);
    // JSON envelope.
    EXPECT_NE(r.body.find("\"logs\""), std::string::npos) << "body: " << r.body;
    // Both worker registrations should have produced log lines on the coordinator.
    EXPECT_NE(r.body.find("\"source\":\"coordinator.register\""), std::string::npos)
        << "body: " << r.body;
    EXPECT_NE(r.body.find("worker=" + c->worker_ids[0]), std::string::npos) << "body: " << r.body;
}

TEST(HttpObservability, LogsLevelFilterDropsLowerLevels) {
    auto c = start_cluster(/*n_workers=*/1);
    if (!c.has_value()) {
        GTEST_SKIP() << "cluster startup failed";
    }
    // level=error filters out the coordinator.register (info) lines.
    const auto r = http_get("127.0.0.1", c->coordinator_http_port, "/api/v1/logs?level=error");
    ASSERT_EQ(r.status, 200);
    EXPECT_EQ(r.body.find("coordinator.register"), std::string::npos)
        << "info-level register line leaked through error filter; body: " << r.body;
}

TEST(HttpObservability, CoordinatorProxiesWorkerMetricsAndLogs) {
    auto c = start_cluster(/*n_workers=*/1);
    if (!c.has_value()) {
        GTEST_SKIP() << "cluster startup failed";
    }
    const auto direct_m = http_get("127.0.0.1", c->worker_http_ports[0], "/metrics");
    const auto proxied_m = http_get(
        "127.0.0.1", c->coordinator_http_port, "/api/v1/workers/" + c->worker_ids[0] + "/metrics");
    ASSERT_EQ(direct_m.status, 200);
    EXPECT_EQ(proxied_m.status, 200);
    // Body equality: counters can change between direct + proxied (one is a
    // request to the worker, one is a request to the coordinator that proxies to the worker -
    // each bumps clink_http_requests_total on the worker). Instead, assert the
    // proxied body still looks like Prometheus exposition.
    EXPECT_NE(proxied_m.body.find("# TYPE " + std::string{metrics::kWorkerSlotsCapacity}),
              std::string::npos)
        << "proxied body: " << proxied_m.body;

    const auto proxied_l = http_get(
        "127.0.0.1", c->coordinator_http_port, "/api/v1/workers/" + c->worker_ids[0] + "/logs");
    EXPECT_EQ(proxied_l.status, 200);
    EXPECT_NE(proxied_l.body.find("\"logs\""), std::string::npos) << "body: " << proxied_l.body;
}

TEST(HttpObservability, JobSubmissionBumpsCoordinatorCounters) {
    const auto submit = submit_binary_path();
    const auto job_so = cancel_test_job_path();
    if (!std::filesystem::exists(submit) || !std::filesystem::exists(job_so)) {
        GTEST_SKIP() << "submitter or cancel_test_job.so not built";
    }
    auto c = start_cluster(/*n_workers=*/2);
    if (!c.has_value()) {
        GTEST_SKIP() << "cluster startup failed";
    }

    // Baseline: before any submit, jobs_submitted_total = 0.
    {
        const auto r = http_get("127.0.0.1", c->coordinator_http_port, "/metrics");
        ASSERT_EQ(r.status, 200);
        EXPECT_NE(r.body.find(std::string{metrics::kCoordinatorJobsSubmittedTotal} + " 0"),
                  std::string::npos)
            << "baseline body: " << r.body;
    }

    ::setenv("CLINK_CANCEL_TICK_MS", "20", 1);
    const pid_t submit_pid =
        spawn_proc({"clink_submit_job",
                    "--job=" + job_so.string(),
                    "--coordinator-host=127.0.0.1",
                    "--coordinator-port=" + std::to_string(c->coordinator_control_port),
                    "--wait-timeout-s=30"},
                   submit);
    ASSERT_GT(submit_pid, 0);
    std::this_thread::sleep_for(2s);

    // Mid-flight: jobs_submitted_total = 1, jobs_running = 1.
    {
        const auto r = http_get("127.0.0.1", c->coordinator_http_port, "/metrics");
        ASSERT_EQ(r.status, 200);
        EXPECT_NE(r.body.find(std::string{metrics::kCoordinatorJobsSubmittedTotal} + " 1"),
                  std::string::npos)
            << "post-submit body: " << r.body;
        EXPECT_NE(r.body.find(std::string{metrics::kCoordinatorJobsRunning} + " 1"),
                  std::string::npos)
            << "post-submit body: " << r.body;
    }

    kill_quietly(submit_pid);
}
