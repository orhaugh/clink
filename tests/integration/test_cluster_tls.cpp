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

#include "tests/integration/await_port.hpp"

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
// `subject_alt_names` is the certificate's subjectAltName: by default the
// names the workers here connect by, so their hostname check passes.
std::filesystem::path generate_self_signed_cert(
    const std::string& subject_alt_names = "DNS:localhost,IP:127.0.0.1") {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("clink_cluster_tls_test_" + std::to_string(::getpid()));
    std::filesystem::create_directories(dir);
    const auto cert = dir / "cert.pem";
    const auto key = dir / "key.pem";
    const std::string cmd =
        "openssl req -x509 -newkey rsa:2048 -nodes -keyout " + key.string() + " -out " +
        cert.string() + " -days 1 -subj /CN=localhost -addext subjectAltName=" + subject_alt_names +
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

    // Wait for the worker to APPEAR in the coordinator's view, which is precisely
    // what the assertions below check. The 500ms this replaces was a guess at how
    // long a TLS handshake plus a Register frame takes - and on a loaded machine a
    // handshake that had not finished yet read as "TLS registration is broken".
    ASSERT_TRUE(clink::itest::await_condition(
        [&] {
            const auto probe = http_get("127.0.0.1", http_port, "/api/v1/workers");
            return probe.status == 200 &&
                   probe.body.find("\"worker_id\":\"" + worker_id + "\"") != std::string::npos;
        },
        std::chrono::seconds(20)))
        << "the worker never registered over TLS";
    const auto r = http_get("127.0.0.1", http_port, "/api/v1/workers");
    EXPECT_EQ(r.status, 200);
    EXPECT_NE(r.body.find("\"worker_id\":\"" + worker_id + "\""), std::string::npos)
        << "worker didn't show up in /api/v1/workers; body: " << r.body;

    kill_quietly(worker);
    kill_quietly(coordinator);
}

TEST(ClusterTls, PlainWorkerStaysAliveButCannotJoinTlsCoordinator) {
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

    // Spawn a worker WITHOUT --tls-ca. The supervisor cannot distinguish a
    // persistent TLS mismatch from a temporarily unavailable coordinator, so
    // it must stay alive and retry without ever registering.
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
    EXPECT_FALSE(wait_for_exit(worker, 3s))
        << "a recoverable control-plane connection failure killed the worker process";

    // coordinator /api/v1/workers should remain empty.
    const auto r = http_get("127.0.0.1", http_port, "/api/v1/workers");
    EXPECT_EQ(r.status, 200);
    EXPECT_EQ(r.body.find("worker-plain-fail"), std::string::npos)
        << "plain worker should not have registered; body: " << r.body;

    kill_quietly(worker);
    kill_quietly(coordinator);
}

// A worker checks that the coordinator's certificate names the host it
// connects by, not only that its CA signed it. A certificate that names
// another host is refused: the worker stays up, retrying, and its status says
// why and which flag turns the check off. With that flag it joins.
TEST(ClusterTls, AWorkerRefusesACoordinatorCertificateThatDoesNotNameItsHost) {
    const auto node = node_binary_path();
    if (!std::filesystem::exists(node)) {
        GTEST_SKIP() << "clink_node not built";
    }
    const auto cert_dir = generate_self_signed_cert("DNS:coordinator.invalid");
    if (cert_dir.empty()) {
        GTEST_SKIP() << "openssl CLI unavailable";
    }
    // Every process this test starts is killed however the test ends: an
    // assertion that returns early would otherwise leave them holding the
    // test's output, and ctest waits on that until its timeout.
    struct Spawned {
        std::vector<pid_t> pids;
        ~Spawned() {
            for (const auto pid : pids) {
                kill_quietly(pid);
            }
        }
    } spawned;
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
    spawned.pids.push_back(coordinator);
    ASSERT_TRUE(await_http_ready(http_port, 3s)) << "coordinator didn't come up";

    const auto worker_http = probe_free_port();
    const pid_t refused = spawn_proc({"clink_node",
                                      "--role=worker",
                                      "--id=worker-name-mismatch",
                                      "--coordinator-host=127.0.0.1",
                                      "--coordinator-port=" + std::to_string(control_port),
                                      "--http-port=" + std::to_string(worker_http),
                                      "--http-bind=127.0.0.1",
                                      "--tls-ca=" + cert},
                                     node);
    ASSERT_GT(refused, 0);
    spawned.pids.push_back(refused);
    // Not await_http_ready: a worker that cannot reach its coordinator reports
    // itself unhealthy, so its health check never reads 200. The status poll
    // below waits for its HTTP server as well.
    std::string status;
    EXPECT_TRUE(clink::itest::await_condition(
        [&] {
            status = http_get("127.0.0.1", worker_http, "/api/v1/worker").body;
            return status.find("does not match the host 127.0.0.1") != std::string::npos &&
                   status.find("--tls-no-verify-hostname") != std::string::npos;
        },
        std::chrono::seconds(20)))
        << "the worker's status never said its connect was refused for the host name: " << status;
    EXPECT_FALSE(wait_for_exit(refused, 1s))
        << "a refused certificate killed the worker process instead of leaving it retrying";
    const auto before = http_get("127.0.0.1", http_port, "/api/v1/workers");
    EXPECT_EQ(before.body.find("worker-name-mismatch"), std::string::npos)
        << "a worker registered with a coordinator whose certificate does not name its host; "
           "body: "
        << before.body;

    const auto opted_out_http = probe_free_port();
    const pid_t opted_out = spawn_proc({"clink_node",
                                        "--role=worker",
                                        "--id=worker-no-verify-hostname",
                                        "--coordinator-host=127.0.0.1",
                                        "--coordinator-port=" + std::to_string(control_port),
                                        "--http-port=" + std::to_string(opted_out_http),
                                        "--http-bind=127.0.0.1",
                                        "--tls-ca=" + cert,
                                        "--tls-no-verify-hostname"},
                                       node);
    ASSERT_GT(opted_out, 0);
    spawned.pids.push_back(opted_out);
    EXPECT_TRUE(clink::itest::await_condition(
        [&] {
            const auto probe = http_get("127.0.0.1", http_port, "/api/v1/workers");
            return probe.status == 200 &&
                   probe.body.find("\"worker_id\":\"worker-no-verify-hostname\"") !=
                       std::string::npos;
        },
        std::chrono::seconds(20)))
        << "a worker with --tls-no-verify-hostname never registered";

    // Kill them before the certificates go.
    for (const auto pid : spawned.pids) {
        kill_quietly(pid);
    }
    spawned.pids.clear();
    std::error_code ec;
    std::filesystem::remove_all(cert_dir, ec);
}
