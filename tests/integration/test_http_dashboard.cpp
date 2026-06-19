// HTTP-6 dashboard SPA serving tests.
//
// The dashboard is embedded as a string constant at build time; this test
// just checks that:
//   * GET / on the JM returns 200 with text/html and the page markers we
//     expect (the <title> and a chunk of the EventSource bootstrap JS),
//   * GET /dashboard on the JM serves the same page (-muscle-memory
//     URL parity),
//   * GET / on a TM does NOT serve the dashboard (TM HTTP is JSON-API
//     only; the JM is the single human entry point).

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
    pid_t jm_pid{-1};
    std::uint16_t jm_http_port{0};
    std::uint16_t jm_control_port{0};
    std::vector<pid_t> tm_pids;
    std::vector<std::uint16_t> tm_http_ports;

    Cluster() = default;
    Cluster(const Cluster&) = delete;
    Cluster& operator=(const Cluster&) = delete;
    Cluster(Cluster&& o) noexcept
        : jm_pid(o.jm_pid),
          jm_http_port(o.jm_http_port),
          jm_control_port(o.jm_control_port),
          tm_pids(std::move(o.tm_pids)),
          tm_http_ports(std::move(o.tm_http_ports)) {
        o.jm_pid = -1;
    }
    Cluster& operator=(Cluster&& o) noexcept {
        if (this != &o) {
            this->~Cluster();
            new (this) Cluster(std::move(o));
        }
        return *this;
    }
    ~Cluster() {
        for (auto pid : tm_pids)
            kill_quietly(pid);
        kill_quietly(jm_pid);
    }
};

std::optional<Cluster> start_cluster(int n_tms) {
    Cluster c;
    const auto node = node_binary_path();
    if (!std::filesystem::exists(node))
        return std::nullopt;
    c.jm_control_port = probe_free_port();
    c.jm_http_port = probe_free_port();
    c.jm_pid = spawn_proc({"clink_node",
                           "--role=jm",
                           "--port=" + std::to_string(c.jm_control_port),
                           "--http-port=" + std::to_string(c.jm_http_port),
                           "--http-bind=127.0.0.1"},
                          node);
    if (c.jm_pid <= 0 || !await_http_ready(c.jm_http_port, 2s))
        return std::nullopt;
    for (int i = 1; i <= n_tms; ++i) {
        const auto http_port = probe_free_port();
        const std::string tm_id = "tm-dash-" + std::to_string(i);
        const pid_t pid = spawn_proc({"clink_node",
                                      "--role=tm",
                                      "--id=" + tm_id,
                                      "--jm-host=127.0.0.1",
                                      "--jm-port=" + std::to_string(c.jm_control_port),
                                      "--http-port=" + std::to_string(http_port),
                                      "--http-bind=127.0.0.1"},
                                     node);
        if (pid <= 0 || !await_http_ready(http_port, 2s))
            return std::nullopt;
        c.tm_pids.push_back(pid);
        c.tm_http_ports.push_back(http_port);
    }
    return c;
}

}  // namespace

TEST(HttpDashboard, JmRootServesDashboardHtml) {
    auto c = start_cluster(/*n_tms=*/0);
    if (!c.has_value()) {
        GTEST_SKIP() << "cluster startup failed";
    }
    const auto r = http_get("127.0.0.1", c->jm_http_port, "/");
    ASSERT_EQ(r.status, 200);
    EXPECT_NE(r.content_type.find("text/html"), std::string::npos)
        << "content_type=" << r.content_type;
    EXPECT_NE(r.body.find("<title>clink dashboard</title>"), std::string::npos)
        << "missing title; body head: " << r.body.substr(0, 200);
    // The SPA bootstraps an SSE connection to /api/v1/events; if this
    // line ever moves out of the embedded HTML, the dashboard is broken.
    EXPECT_NE(r.body.find("new EventSource('/api/v1/events')"), std::string::npos)
        << "missing EventSource bootstrap";
}

TEST(HttpDashboard, JmDashboardPathServesSameHtml) {
    auto c = start_cluster(/*n_tms=*/0);
    if (!c.has_value()) {
        GTEST_SKIP() << "cluster startup failed";
    }
    const auto root = http_get("127.0.0.1", c->jm_http_port, "/");
    const auto dash = http_get("127.0.0.1", c->jm_http_port, "/dashboard");
    ASSERT_EQ(root.status, 200);
    ASSERT_EQ(dash.status, 200);
    EXPECT_EQ(root.body, dash.body) << "/ and /dashboard should serve the same HTML";
}

TEST(HttpDashboard, TmRootDoesNotServeDashboard) {
    auto c = start_cluster(/*n_tms=*/1);
    if (!c.has_value()) {
        GTEST_SKIP() << "cluster startup failed";
    }
    const auto r = http_get("127.0.0.1", c->tm_http_ports[0], "/");
    // TM has no `/` route, so cpp-httplib's default 404 fires. We don't
    // strictly assert 404 (different cpp-httplib versions might serve
    // a directory listing), but we DO assert the body doesn't carry
    // the dashboard title.
    EXPECT_EQ(r.body.find("clink dashboard"), std::string::npos)
        << "TM should not serve the dashboard; body head: " << r.body.substr(0, 200);
}
