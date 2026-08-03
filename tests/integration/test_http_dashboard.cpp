// HTTP-6 dashboard SPA serving tests.
//
// The dashboard is embedded as a string constant at build time; this test
// just checks that:
//   * GET / on the coordinator returns 200 with text/html and the page markers we
//     expect (the <title> and a chunk of the EventSource bootstrap JS),
//   * GET /dashboard on the coordinator serves the same page (-muscle-memory
//     URL parity),
//   * GET / on a worker does NOT serve the dashboard (worker HTTP is JSON-API
//     only; the coordinator is the single human entry point).

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
    for (const auto& s : argv)
        raw.push_back(const_cast<char*>(s.c_str()));
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

HttpResult http_get(const std::string& host, std::uint16_t port, const std::string& path) {
    HttpResult r;
    const int fd = NetworkSocket::connect_to(host, port);
    if (fd < 0)
        return r;
    const std::string req = "GET " + path + " HTTP/1.1\r\nHost: " + host +
                            "\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    NetworkSocket::send_all(fd, reinterpret_cast<const std::byte*>(req.data()), req.size());
    std::string buf;
    char chunk[4096];
    while (true) {
        const auto n = ::read(fd, chunk, sizeof(chunk));
        if (n <= 0)
            break;
        buf.append(chunk, static_cast<std::size_t>(n));
    }
    NetworkSocket::close(fd);
    if (buf.size() >= 12) {
        try {
            r.status = std::stoi(buf.substr(9, 3));
        } catch (...) {
        }
    }
    const auto ct = std::string{"\r\nContent-Type:"};
    auto cti = buf.find(ct);
    if (cti != std::string::npos) {
        auto eol = buf.find("\r\n", cti + ct.size());
        if (eol != std::string::npos) {
            r.content_type = buf.substr(cti + ct.size(), eol - (cti + ct.size()));
            while (!r.content_type.empty() && r.content_type.front() == ' ') {
                r.content_type.erase(0, 1);
            }
        }
    }
    const auto sep = buf.find("\r\n\r\n");
    if (sep != std::string::npos)
        r.body = buf.substr(sep + 4);
    return r;
}

bool await_http_ready(std::uint16_t port, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (http_get("127.0.0.1", port, "/api/v1/health").status == 200)
            return true;
        std::this_thread::sleep_for(20ms);
    }
    return false;
}

struct Cluster {
    pid_t coordinator_pid{-1};
    std::uint16_t coordinator_http_port{0};
    std::uint16_t coordinator_control_port{0};
    std::vector<pid_t> worker_pids;
    std::vector<std::uint16_t> worker_http_ports;

    Cluster() = default;
    Cluster(const Cluster&) = delete;
    Cluster& operator=(const Cluster&) = delete;
    Cluster(Cluster&& o) noexcept
        : coordinator_pid(o.coordinator_pid),
          coordinator_http_port(o.coordinator_http_port),
          coordinator_control_port(o.coordinator_control_port),
          worker_pids(std::move(o.worker_pids)),
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
        for (auto pid : worker_pids)
            kill_quietly(pid);
        kill_quietly(coordinator_pid);
    }
};

std::optional<Cluster> start_cluster(int n_workers) {
    Cluster c;
    const auto node = node_binary_path();
    if (!std::filesystem::exists(node))
        return std::nullopt;
    c.coordinator_control_port = probe_free_port();
    c.coordinator_http_port = probe_free_port();
    c.coordinator_pid = spawn_proc({"clink_node",
                                    "--role=coordinator",
                                    "--port=" + std::to_string(c.coordinator_control_port),
                                    "--http-port=" + std::to_string(c.coordinator_http_port),
                                    "--http-bind=127.0.0.1"},
                                   node);
    if (c.coordinator_pid <= 0 || !await_http_ready(c.coordinator_http_port, 2s))
        return std::nullopt;
    for (int i = 1; i <= n_workers; ++i) {
        const auto http_port = probe_free_port();
        const std::string worker_id = "worker-dash-" + std::to_string(i);
        const pid_t pid =
            spawn_proc({"clink_node",
                        "--role=worker",
                        "--id=" + worker_id,
                        "--coordinator-host=127.0.0.1",
                        "--coordinator-port=" + std::to_string(c.coordinator_control_port),
                        "--http-port=" + std::to_string(http_port),
                        "--http-bind=127.0.0.1"},
                       node);
        if (pid <= 0 || !await_http_ready(http_port, 2s))
            return std::nullopt;
        c.worker_pids.push_back(pid);
        c.worker_http_ports.push_back(http_port);
    }
    return c;
}

}  // namespace

// These two cases asserted an embedded HTML dashboard - a title tag and an
// `new EventSource('/api/v1/events')` bootstrap - that the coordinator no
// longer serves and is not meant to. The console is a separate project
// (clink-fe) mounted with --http-static-dir; without one, `/` answers with a
// signpost so `curl host:8081/` tells a human where the API and metrics are
// rather than returning 404.
//
// They had been failing since that change, unnoticed, because the broad
// integration label ran advisory in CI (W24). Rewritten against the contract
// that exists rather than deleted: `/` still has a job to do, and nothing
// else covered it.
TEST(HttpDashboard, CoordinatorRootSignpostsTheApiWhenNoConsoleIsMounted) {
    auto c = start_cluster(/*n_workers=*/0);
    if (!c.has_value()) {
        GTEST_SKIP() << "cluster startup failed";
    }
    const auto r = http_get("127.0.0.1", c->coordinator_http_port, "/");
    ASSERT_EQ(r.status, 200) << "`/` should answer, not 404: a human curling the port needs to be "
                                "told where to go";
    EXPECT_NE(r.content_type.find("application/json"), std::string::npos)
        << "content_type=" << r.content_type;
    // Each of these is a route an operator would otherwise have to guess.
    EXPECT_NE(r.body.find("\"api\":\"/api/v1\""), std::string::npos)
        << "signpost does not name the API root; body: " << r.body;
    EXPECT_NE(r.body.find("\"metrics\":\"/metrics\""), std::string::npos)
        << "signpost does not name the metrics endpoint; body: " << r.body;
    EXPECT_NE(r.body.find("--http-static-dir"), std::string::npos)
        << "signpost does not say how to mount a console, which is the one thing someone hitting "
           "`/` expecting a UI needs to know; body: "
        << r.body;
}

TEST(HttpDashboard, TheSignpostIsNotServedWhereARealRouteExists) {
    // The signpost is bound to `/` only. A path that no route claims must
    // still 404 rather than absorb everything, or a typo'd API call would
    // come back 200 with a signpost body and read as success.
    auto c = start_cluster(/*n_workers=*/0);
    if (!c.has_value()) {
        GTEST_SKIP() << "cluster startup failed";
    }
    const auto root = http_get("127.0.0.1", c->coordinator_http_port, "/");
    ASSERT_EQ(root.status, 200);

    const auto missing = http_get("127.0.0.1", c->coordinator_http_port, "/no-such-route");
    EXPECT_NE(missing.status, 200)
        << "an unknown path returned 200; body: " << missing.body.substr(0, 200);

    // And a real API route is untouched by it.
    const auto api = http_get("127.0.0.1", c->coordinator_http_port, "/api/v1/workers");
    EXPECT_EQ(api.status, 200) << "the API root stopped answering";
    EXPECT_EQ(api.body.find("--http-static-dir"), std::string::npos)
        << "the signpost body leaked into an API response";
}

TEST(HttpDashboard, WorkerRootDoesNotServeTheCoordinatorSignpost) {
    auto c = start_cluster(/*n_workers=*/1);
    if (!c.has_value()) {
        GTEST_SKIP() << "cluster startup failed";
    }
    const auto r = http_get("127.0.0.1", c->worker_http_ports[0], "/");
    // The worker has no `/` route. What matters is that it does not
    // impersonate the coordinator: a tool that probes `/` to identify a node
    // must not read a worker as one.
    EXPECT_EQ(r.body.find("clink coordinator"), std::string::npos)
        << "worker answers `/` as though it were the coordinator; body head: "
        << r.body.substr(0, 200);
}
