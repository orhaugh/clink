// TLS-1 cluster integration test.
//
// Verifies the coordinator accept_factory + worker connect_factory TLS path:
//
//   * Generate a self-signed cert via the openssl CLI as a fixture
//     (the openssl CLI is on virtually every dev box; if not, skip).
//   * Spawn a coordinator with --tls-cert/--tls-key.
//   * Spawn a worker with --tls-ca pointing to the same cert.
//   * Expect: worker appears in /api/v1/workers on the coordinator (proves the Register
//     frame travelled over the TLS handshake successfully).
//   * Plain-TCP worker trying to connect should fail.

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
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

bool wait_for_exit(pid_t pid, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        int status = 0;
        if (::waitpid(pid, &status, WNOHANG) == pid)
            return true;
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

// Generate a self-signed cert + key via the openssl CLI; returns the
// directory containing cert.pem + key.pem. Empty path = openssl missing.
std::filesystem::path generate_self_signed_cert() {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("clink_cluster_tls_test_" + std::to_string(::getpid()));
    std::filesystem::create_directories(dir);
    const auto cert = dir / "cert.pem";
    const auto key = dir / "key.pem";
    const std::string cmd = "openssl req -x509 -newkey rsa:2048 -nodes -keyout " + key.string() +
                            " -out " + cert.string() +
                            " -days 1 -subj /CN=localhost"
                            " > /dev/null 2>&1";
    const int rc = std::system(cmd.c_str());
    if (rc != 0)
        return {};
    return dir;
}

}  // namespace

TEST(ClusterTls, WorkerRegistersOverTlsControlPlane) {
    const auto node = node_binary_path();
    if (!std::filesystem::exists(node)) {
        GTEST_SKIP() << "clink_node not built";
    }
    const auto cert_dir = generate_self_signed_cert();
    if (cert_dir.empty()) {
        GTEST_SKIP() << "openssl CLI unavailable";
    }
    const auto cert = (cert_dir / "cert.pem").string();
    const auto key = (cert_dir / "key.pem").string();

    const auto control_port = probe_free_port();
    const auto http_port = probe_free_port();
    const pid_t coordinator = spawn_proc({"clink_node",
                                          "--role=coordinator",
                                          "--port=" + std::to_string(control_port),
                                          "--http-port=" + std::to_string(http_port),
                                          "--http-bind=127.0.0.1",
                                          "--tls-cert=" + cert,
                                          "--tls-key=" + key},
                                         node);
    ASSERT_GT(coordinator, 0);
    // coordinator's HTTP listener is plain TCP (HTTPS is a separate slice), so
    // health-check goes through fine even with TLS on the control plane.
    ASSERT_TRUE(await_http_ready(http_port, 3s)) << "coordinator didn't come up";

    const auto worker_http = probe_free_port();
    const std::string worker_id = "worker-tls";
    const pid_t worker = spawn_proc({"clink_node",
                                     "--role=worker",
                                     "--id=" + worker_id,
                                     "--coordinator-host=127.0.0.1",
                                     "--coordinator-port=" + std::to_string(control_port),
                                     "--http-port=" + std::to_string(worker_http),
                                     "--http-bind=127.0.0.1",
                                     "--tls-ca=" + cert},
                                    node);
    ASSERT_GT(worker, 0);
    ASSERT_TRUE(await_http_ready(worker_http, 3s)) << "worker didn't come up";

    // Give the worker ~500ms to complete its TLS handshake + Register frame.
    std::this_thread::sleep_for(500ms);
    const auto r = http_get("127.0.0.1", http_port, "/api/v1/workers");
    EXPECT_EQ(r.status, 200);
    EXPECT_NE(r.body.find("\"worker_id\":\"" + worker_id + "\""), std::string::npos)
        << "worker didn't show up in /api/v1/workers; body: " << r.body;

    kill_quietly(worker);
    kill_quietly(coordinator);
}

TEST(ClusterTls, PlainWorkerCannotJoinTlsCoordinator) {
    const auto node = node_binary_path();
    if (!std::filesystem::exists(node)) {
        GTEST_SKIP() << "clink_node not built";
    }
    const auto cert_dir = generate_self_signed_cert();
    if (cert_dir.empty()) {
        GTEST_SKIP() << "openssl CLI unavailable";
    }
    const auto cert = (cert_dir / "cert.pem").string();
    const auto key = (cert_dir / "key.pem").string();

    const auto control_port = probe_free_port();
    const auto http_port = probe_free_port();
    const pid_t coordinator = spawn_proc({"clink_node",
                                          "--role=coordinator",
                                          "--port=" + std::to_string(control_port),
                                          "--http-port=" + std::to_string(http_port),
                                          "--http-bind=127.0.0.1",
                                          "--tls-cert=" + cert,
                                          "--tls-key=" + key},
                                         node);
    ASSERT_GT(coordinator, 0);
    ASSERT_TRUE(await_http_ready(http_port, 3s));

    // Spawn a worker WITHOUT --tls-ca: it should try plain TCP, fail the
    // handshake, and exit non-zero. We give it 2s to die.
    const auto worker_http = probe_free_port();
    const pid_t worker = spawn_proc({"clink_node",
                                     "--role=worker",
                                     "--id=worker-plain-fail",
                                     "--coordinator-host=127.0.0.1",
                                     "--coordinator-port=" + std::to_string(control_port),
                                     "--http-port=" + std::to_string(worker_http),
                                     "--http-bind=127.0.0.1"},
                                    node);
    ASSERT_GT(worker, 0);
    EXPECT_TRUE(wait_for_exit(worker, 3s))
        << "plain-TCP worker should have exited fast when coordinator only speaks TLS";

    // coordinator /api/v1/workers should remain empty.
    const auto r = http_get("127.0.0.1", http_port, "/api/v1/workers");
    EXPECT_EQ(r.status, 200);
    EXPECT_EQ(r.body.find("worker-plain-fail"), std::string::npos)
        << "plain worker should not have registered; body: " << r.body;

    kill_quietly(worker);
    kill_quietly(coordinator);
}
