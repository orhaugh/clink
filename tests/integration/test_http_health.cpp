// HTTP subsystem smoke test - proves clink_node's --http-port flag
// stands up a working HTTP server and the /api/v1/health endpoint
// returns the expected JSON for both roles.
//
// The test does its own minimal HTTP client over NetworkSocket
// primitives so the test infra doesn't need cpp-httplib as a test
// dependency (it's only linked into clink_core internals).

#include <algorithm>
#include <chrono>
#include <cstdint>
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

// Send "GET <path> HTTP/1.1" and return the response body. Returns
// empty string on connect / send / recv error. Tiny, no chunked /
// keep-alive / TLS - enough to scrape a JSON endpoint.
std::string http_get(const std::string& host, std::uint16_t port, const std::string& path) {
    const int fd = NetworkSocket::connect_to(host, port);
    if (fd < 0) {
        return {};
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
    // Strip headers - everything after the first blank line is body.
    const auto sep = buf.find("\r\n\r\n");
    if (sep == std::string::npos) {
        return {};
    }
    return buf.substr(sep + 4);
}

// Wait up to `timeout` for the HTTP server to accept connections - the
// coordinator/worker prints "HTTP on host:port" after binding, but we may try before
// the listener thread loops in. Returns true on first 200 OK.
bool await_http_ready(std::uint16_t port, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        const auto body = http_get("127.0.0.1", port, "/api/v1/health");
        if (!body.empty()) {
            return true;
        }
        std::this_thread::sleep_for(20ms);
    }
    return false;
}

}  // namespace

TEST(HttpHealthEndpoint, CoordinatorExposesHealth) {
    const auto node = node_binary_path();
    if (!std::filesystem::exists(node)) {
        GTEST_SKIP() << "clink_node not built";
    }
    const auto control_port = probe_free_port();
    const auto http_port = probe_free_port();
    const pid_t pid = spawn_proc({"clink_node",
                                  "--role=coordinator",
                                  "--port=" + std::to_string(control_port),
                                  "--http-port=" + std::to_string(http_port),
                                  "--http-bind=127.0.0.1"},
                                 node);
    ASSERT_GT(pid, 0);
    ASSERT_TRUE(await_http_ready(http_port, 2s)) << "HTTP server didn't accept within 2s";

    const auto body = http_get("127.0.0.1", http_port, "/api/v1/health");
    kill_quietly(pid);

    EXPECT_NE(body.find("\"role\":\"coordinator\""), std::string::npos)
        << "expected role=coordinator in body, got: " << body;
    EXPECT_NE(body.find("\"ok\":true"), std::string::npos);
    EXPECT_NE(body.find("\"uptime_s\""), std::string::npos);
}

TEST(HttpHealthEndpoint, WorkerExposesHealth) {
    const auto node = node_binary_path();
    if (!std::filesystem::exists(node)) {
        GTEST_SKIP() << "clink_node not built";
    }
    // worker needs a coordinator to register against.
    const auto coordinator_port = probe_free_port();
    const pid_t coordinator_pid = spawn_proc(
        {"clink_node", "--role=coordinator", "--port=" + std::to_string(coordinator_port)}, node);
    ASSERT_GT(coordinator_pid, 0);
    std::this_thread::sleep_for(200ms);

    const auto worker_http = probe_free_port();
    const pid_t worker_pid = spawn_proc({"clink_node",
                                         "--role=worker",
                                         "--id=worker-http-test",
                                         "--coordinator-host=127.0.0.1",
                                         "--coordinator-port=" + std::to_string(coordinator_port),
                                         "--http-port=" + std::to_string(worker_http),
                                         "--http-bind=127.0.0.1"},
                                        node);
    ASSERT_GT(worker_pid, 0);
    ASSERT_TRUE(await_http_ready(worker_http, 2s)) << "worker HTTP didn't accept within 2s";

    const auto body = http_get("127.0.0.1", worker_http, "/api/v1/health");

    kill_quietly(worker_pid);
    kill_quietly(coordinator_pid);

    EXPECT_NE(body.find("\"role\":\"worker\""), std::string::npos)
        << "expected role=worker in body, got: " << body;
    EXPECT_NE(body.find("\"ok\":true"), std::string::npos);
}

TEST(HttpHealthEndpoint, UnknownPathReturnsError) {
    const auto node = node_binary_path();
    if (!std::filesystem::exists(node)) {
        GTEST_SKIP() << "clink_node not built";
    }
    const auto control_port = probe_free_port();
    const auto http_port = probe_free_port();
    const pid_t pid = spawn_proc({"clink_node",
                                  "--role=coordinator",
                                  "--port=" + std::to_string(control_port),
                                  "--http-port=" + std::to_string(http_port),
                                  "--http-bind=127.0.0.1"},
                                 node);
    ASSERT_GT(pid, 0);
    ASSERT_TRUE(await_http_ready(http_port, 2s));

    // The HttpServer's default error handler emits a 4xx JSON body
    // including the status code as a string. Hitting an unknown path
    // proves the route table is wired correctly.
    const int fd = NetworkSocket::connect_to("127.0.0.1", http_port);
    ASSERT_GE(fd, 0);
    const std::string req =
        "GET /api/v1/does-not-exist HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    NetworkSocket::send_all(fd, reinterpret_cast<const std::byte*>(req.data()), req.size());
    std::string raw;
    char chunk[4096];
    while (true) {
        const auto n = ::read(fd, chunk, sizeof(chunk));
        if (n <= 0) {
            break;
        }
        raw.append(chunk, static_cast<std::size_t>(n));
    }
    NetworkSocket::close(fd);
    kill_quietly(pid);

    // Status line is "HTTP/1.1 404 ..." for a missing route.
    EXPECT_NE(raw.find("404"), std::string::npos)
        << "expected 404 status for unknown path, got: " << raw.substr(0, 200);
}
