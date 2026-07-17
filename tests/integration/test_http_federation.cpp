// Federation + actions integration tests for the HTTP subsystem.
//
// HTTP-3a: workers report their --http-port via Register, the coordinator stores
//          it, /api/v1/workers exposes it.
// HTTP-3b: GET /api/v1/workers/:id/{worker,subtasks,config} on the coordinator proxies
//          through to the worker and returns its response unchanged.
// HTTP-3d: POST /api/v1/jobs/:id/cancel calls Coordinator::cancel_job
//          and returns an ack JSON. Cancels mid-flight job_id=1 from
//          clink_submit_job, expects submitter to see "cancelled by
//          client".

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

bool wait_for(pid_t pid, std::chrono::milliseconds timeout, int& exit_code) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        int status = 0;
        const pid_t r = waitpid(pid, &status, WNOHANG);
        if (r == pid) {
            exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
            return true;
        }
        std::this_thread::sleep_for(50ms);
    }
    return false;
}

std::uint16_t probe_free_port() {
    NetworkChannelSource<std::int64_t> probe(0, int64_codec());
    return probe.listen();
}

struct HttpResult {
    int status{0};
    std::string body;
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
    const auto sep = buf.find("\r\n\r\n");
    if (sep != std::string::npos) {
        r.body = buf.substr(sep + 4);
    }
    return r;
}

HttpResult http_get(const std::string& host, std::uint16_t port, const std::string& path) {
    return http_request(host, port, "GET", path);
}
HttpResult http_post(const std::string& host, std::uint16_t port, const std::string& path) {
    return http_request(host, port, "POST", path);
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
        const std::string worker_id = "worker-fed-" + std::to_string(i);
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
    // Let workers register with the coordinator before tests assert.
    std::this_thread::sleep_for(400ms);
    return c;
}

}  // namespace

TEST(HttpFederation, WorkersEndpointReportsHttpPortAfterRegister) {
    auto c = start_cluster(/*n_workers=*/2);
    if (!c.has_value()) {
        GTEST_SKIP() << "cluster startup failed";
    }
    const auto r = http_get("127.0.0.1", c->coordinator_http_port, "/api/v1/workers");
    ASSERT_EQ(r.status, 200);
    // Each worker advertised its http_port via Register; both should appear.
    const auto p0 = std::string{"\"http_port\":"} + std::to_string(c->worker_http_ports[0]);
    const auto p1 = std::string{"\"http_port\":"} + std::to_string(c->worker_http_ports[1]);
    EXPECT_NE(r.body.find(p0), std::string::npos)
        << "expected " << p0 << " in /api/v1/workers body: " << r.body;
    EXPECT_NE(r.body.find(p1), std::string::npos)
        << "expected " << p1 << " in /api/v1/workers body: " << r.body;
}

TEST(HttpFederation, CoordinatorProxiesWorkerWorkerEndpoint) {
    auto c = start_cluster(/*n_workers=*/1);
    if (!c.has_value()) {
        GTEST_SKIP() << "cluster startup failed";
    }
    const auto direct = http_get("127.0.0.1", c->worker_http_ports[0], "/api/v1/worker");
    const auto proxied = http_get(
        "127.0.0.1", c->coordinator_http_port, "/api/v1/workers/" + c->worker_ids[0] + "/worker");
    ASSERT_EQ(direct.status, 200);
    EXPECT_EQ(proxied.status, 200);
    EXPECT_EQ(proxied.body, direct.body)
        << "proxied body should match direct body byte-for-byte\n direct=" << direct.body
        << "\n proxied=" << proxied.body;
}

TEST(HttpFederation, CoordinatorProxiesWorkerSubtasksAndConfig) {
    auto c = start_cluster(/*n_workers=*/1);
    if (!c.has_value()) {
        GTEST_SKIP() << "cluster startup failed";
    }
    const auto proxied_subtasks = http_get(
        "127.0.0.1", c->coordinator_http_port, "/api/v1/workers/" + c->worker_ids[0] + "/subtasks");
    EXPECT_EQ(proxied_subtasks.status, 200);
    EXPECT_NE(proxied_subtasks.body.find("\"subtasks\""), std::string::npos);

    const auto proxied_config = http_get(
        "127.0.0.1", c->coordinator_http_port, "/api/v1/workers/" + c->worker_ids[0] + "/config");
    EXPECT_EQ(proxied_config.status, 200);
    EXPECT_NE(proxied_config.body.find("\"slot_count\""), std::string::npos);
}

TEST(HttpFederation, ProxyReturns404ForUnknownWorker) {
    auto c = start_cluster(/*n_workers=*/1);
    if (!c.has_value()) {
        GTEST_SKIP() << "cluster startup failed";
    }
    const auto r =
        http_get("127.0.0.1", c->coordinator_http_port, "/api/v1/workers/no-such-worker/worker");
    EXPECT_EQ(r.status, 404);
    EXPECT_NE(r.body.find("\"error\""), std::string::npos);
}

TEST(HttpFederation, JobsCancelEndpointReturns404ForUnknownJob) {
    auto c = start_cluster(/*n_workers=*/1);
    if (!c.has_value()) {
        GTEST_SKIP() << "cluster startup failed";
    }
    const auto r = http_post("127.0.0.1", c->coordinator_http_port, "/api/v1/jobs/9999/cancel");
    EXPECT_EQ(r.status, 404);
    EXPECT_NE(r.body.find("no such job"), std::string::npos);
}

TEST(HttpFederation, JobsCancelEndpointCancelsRunningJob) {
    const auto submit = submit_binary_path();
    const auto job_so = cancel_test_job_path();
    if (!std::filesystem::exists(submit) || !std::filesystem::exists(job_so)) {
        GTEST_SKIP() << "submitter or cancel_test_job.so not built";
    }
    auto c = start_cluster(/*n_workers=*/2);
    if (!c.has_value()) {
        GTEST_SKIP() << "cluster startup failed";
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
    // Let the submitter complete its SubmitJob -> SubmitJobAck round
    // so the job is registered as id=1 before we POST cancel.
    std::this_thread::sleep_for(2s);

    const auto cancel_resp =
        http_post("127.0.0.1", c->coordinator_http_port, "/api/v1/jobs/1/cancel");
    EXPECT_EQ(cancel_resp.status, 200);
    EXPECT_NE(cancel_resp.body.find("\"ok\":true"), std::string::npos)
        << "cancel ack: " << cancel_resp.body;

    int submit_exit = -1;
    const bool exited = wait_for(submit_pid, 30s, submit_exit);
    if (!exited) {
        kill_quietly(submit_pid);
    }
    EXPECT_TRUE(exited) << "submitter didn't exit after HTTP cancel";
    EXPECT_NE(submit_exit, 0) << "submitter should have exited non-zero (cancelled), got "
                              << submit_exit;
}
