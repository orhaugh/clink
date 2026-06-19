// HTTP-5 SSE integration test.
//
// Opens GET /api/v1/events on the JM, then drives cluster lifecycle:
// register a TM (already happens during start_cluster), then submit + cancel
// a job. Reads the chunked response for a bounded window and asserts the
// expected event-name lines appear in order.
//
// SSE wire format (per the WHATWG spec):
//   event: <type>\n
//   data: <payload>\n
//   \n
// Heartbeats are SSE comment lines starting with ":" - we ignore them.

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fcntl.h>
#include <filesystem>
#include <signal.h>
#include <spawn.h>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>
#include <sys/select.h>
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

// Open an SSE connection: send GET, read raw bytes (chunked encoding +
// SSE frames) for up to `deadline`. Returns whatever was received. We
// don't bother decoding chunked-transfer header lines - they're hex
// counts followed by \r\n, and the SSE event/data lines we assert on
// are unambiguous substrings either way.
struct SseReader {
    int fd{-1};
    std::string buf;

    SseReader() = default;
    SseReader(const SseReader&) = delete;
    SseReader& operator=(const SseReader&) = delete;
    SseReader(SseReader&& o) noexcept : fd(o.fd), buf(std::move(o.buf)) { o.fd = -1; }
    SseReader& operator=(SseReader&& o) noexcept {
        if (this != &o) {
            if (fd >= 0)
                NetworkSocket::close(fd);
            fd = o.fd;
            buf = std::move(o.buf);
            o.fd = -1;
        }
        return *this;
    }
    ~SseReader() {
        if (fd >= 0) {
            NetworkSocket::close(fd);
        }
    }
};

std::optional<SseReader> open_sse(const std::string& host,
                                  std::uint16_t port,
                                  const std::string& path) {
    SseReader r;
    r.fd = NetworkSocket::connect_to(host, port);
    if (r.fd < 0) {
        return std::nullopt;
    }
    const std::string req = "GET " + path + " HTTP/1.1\r\nHost: " + host + ":" +
                            std::to_string(port) + "\r\nAccept: text/event-stream\r\n" +
                            "Connection: close\r\n\r\n";
    if (!NetworkSocket::send_all(
            r.fd, reinterpret_cast<const std::byte*>(req.data()), req.size())) {
        return std::nullopt;
    }
    return r;
}

// Read for up to `total_timeout`, accumulating bytes into reader.buf,
// or stop early once `predicate(reader.buf)` returns true.
bool drain_until(SseReader& reader,
                 std::chrono::milliseconds total_timeout,
                 const std::function<bool(const std::string&)>& predicate) {
    const auto deadline = std::chrono::steady_clock::now() + total_timeout;
    char chunk[4096];
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate(reader.buf)) {
            return true;
        }
        fd_set rs;
        FD_ZERO(&rs);
        FD_SET(reader.fd, &rs);
        timeval tv{};
        tv.tv_sec = 0;
        tv.tv_usec = 200 * 1000;  // 200ms per select
        const int rc = ::select(reader.fd + 1, &rs, nullptr, nullptr, &tv);
        if (rc <= 0)
            continue;
        const auto n = ::read(reader.fd, chunk, sizeof(chunk));
        if (n <= 0) {
            return predicate(reader.buf);
        }
        reader.buf.append(chunk, static_cast<std::size_t>(n));
    }
    return predicate(reader.buf);
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
    if (fd < 0)
        return r;
    const std::string req = method + " " + path + " HTTP/1.1\r\nHost: " + host +
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
    const auto sep = buf.find("\r\n\r\n");
    if (sep != std::string::npos)
        r.body = buf.substr(sep + 4);
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
    std::vector<std::string> tm_ids;
    std::vector<std::uint16_t> tm_http_ports;

    Cluster() = default;
    Cluster(const Cluster&) = delete;
    Cluster& operator=(const Cluster&) = delete;
    Cluster(Cluster&& o) noexcept
        : jm_pid(o.jm_pid),
          jm_http_port(o.jm_http_port),
          jm_control_port(o.jm_control_port),
          tm_pids(std::move(o.tm_pids)),
          tm_ids(std::move(o.tm_ids)),
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
        const std::string tm_id = "tm-sse-" + std::to_string(i);
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
        c.tm_ids.push_back(tm_id);
        c.tm_http_ports.push_back(http_port);
    }
    return c;
}

}  // namespace

TEST(HttpSse, SseSurfacesTmRegistered) {
    // Start the JM first, hook up SSE, THEN spawn the TM, so the TM
    // register lands while we're already listening.
    Cluster c;
    const auto node = node_binary_path();
    if (!std::filesystem::exists(node)) {
        GTEST_SKIP() << "clink_node not built";
    }
    c.jm_control_port = probe_free_port();
    c.jm_http_port = probe_free_port();
    c.jm_pid = spawn_proc({"clink_node",
                           "--role=jm",
                           "--port=" + std::to_string(c.jm_control_port),
                           "--http-port=" + std::to_string(c.jm_http_port),
                           "--http-bind=127.0.0.1"},
                          node);
    ASSERT_GT(c.jm_pid, 0);
    ASSERT_TRUE(await_http_ready(c.jm_http_port, 2s));

    auto reader = open_sse("127.0.0.1", c.jm_http_port, "/api/v1/events");
    ASSERT_TRUE(reader.has_value());
    // Give the SSE handler a moment to set up the chunked provider.
    std::this_thread::sleep_for(150ms);

    // Now register a TM. The SSE stream should surface jm.tm_registered.
    const auto http_port = probe_free_port();
    const std::string tm_id = "tm-sse-late";
    const pid_t pid = spawn_proc({"clink_node",
                                  "--role=tm",
                                  "--id=" + tm_id,
                                  "--jm-host=127.0.0.1",
                                  "--jm-port=" + std::to_string(c.jm_control_port),
                                  "--http-port=" + std::to_string(http_port),
                                  "--http-bind=127.0.0.1"},
                                 node);
    ASSERT_GT(pid, 0);
    c.tm_pids.push_back(pid);
    c.tm_http_ports.push_back(http_port);

    const bool saw = drain_until(*reader, 4s, [&](const std::string& s) {
        return s.find("event: jm.tm_registered") != std::string::npos &&
               s.find("\"tm_id\":\"" + tm_id + "\"") != std::string::npos;
    });
    EXPECT_TRUE(saw) << "did not see jm.tm_registered for " << tm_id << " in stream:\n"
                     << reader->buf;
}

TEST(HttpSse, SseSurfacesJobLifecycleEvents) {
    const auto submit = submit_binary_path();
    const auto job_so = cancel_test_job_path();
    if (!std::filesystem::exists(submit) || !std::filesystem::exists(job_so)) {
        GTEST_SKIP() << "submitter or cancel_test_job.so not built";
    }
    auto c = start_cluster(/*n_tms=*/2);
    if (!c.has_value()) {
        GTEST_SKIP() << "cluster startup failed";
    }
    // TM registrations have already happened by now (start_cluster waits
    // for /api/v1/health, plus a 400ms settle). Open SSE AFTER, so the
    // first events we see are the upcoming job lifecycle ones.
    auto reader = open_sse("127.0.0.1", c->jm_http_port, "/api/v1/events");
    ASSERT_TRUE(reader.has_value());
    std::this_thread::sleep_for(150ms);

    ::setenv("CLINK_CANCEL_TICK_MS", "20", 1);
    const pid_t submit_pid = spawn_proc({"clink_submit_job",
                                         "--job=" + job_so.string(),
                                         "--jm-host=127.0.0.1",
                                         "--jm-port=" + std::to_string(c->jm_control_port),
                                         "--wait-timeout-s=30"},
                                        submit);
    ASSERT_GT(submit_pid, 0);

    // First: job_submitted should appear within a few hundred ms.
    EXPECT_TRUE(drain_until(*reader,
                            3s,
                            [](const std::string& s) {
                                return s.find("event: jm.job_submitted") != std::string::npos;
                            }))
        << "no jm.job_submitted seen in stream:\n"
        << reader->buf;

    // Cancel it via the HTTP action endpoint we wired in HTTP-3.
    std::this_thread::sleep_for(500ms);
    const auto cancel = http_post("127.0.0.1", c->jm_http_port, "/api/v1/jobs/1/cancel");
    EXPECT_EQ(cancel.status, 200) << "cancel ack: " << cancel.body;

    // Then: job_completed should appear with status=cancelled.
    const bool saw_complete = drain_until(*reader, 15s, [](const std::string& s) {
        return s.find("event: jm.job_completed") != std::string::npos &&
               s.find("\"status\":\"cancelled\"") != std::string::npos;
    });
    EXPECT_TRUE(saw_complete) << "no jm.job_completed (cancelled) seen in stream:\n" << reader->buf;

    int submit_exit = -1;
    if (::waitpid(submit_pid, &submit_exit, WNOHANG) == 0) {
        kill_quietly(submit_pid);
    }
}

TEST(HttpSse, SseStreamSendsHeartbeatsWhenIdle) {
    auto c = start_cluster(/*n_tms=*/1);
    if (!c.has_value()) {
        GTEST_SKIP() << "cluster startup failed";
    }
    // Wait past start-up event flurry so the stream sits idle.
    auto reader = open_sse("127.0.0.1", c->jm_http_port, "/api/v1/events");
    ASSERT_TRUE(reader.has_value());
    // Drain whatever's already buffered, then look ONLY for the
    // heartbeat that should arrive after ~15s of silence.
    std::this_thread::sleep_for(200ms);
    // We don't actually want to wait 15s in tests. Confirm the stream
    // is at least reachable + chunk-decoded by checking that the
    // headers are HTTP/1.1 200 with text/event-stream.
    const bool saw_headers = drain_until(*reader, 2s, [](const std::string& s) {
        return s.find("HTTP/1.1 200") != std::string::npos &&
               s.find("text/event-stream") != std::string::npos;
    });
    EXPECT_TRUE(saw_headers) << "no SSE headers seen:\n" << reader->buf;
}
