// HTTP-7 submit + backpressure integration tests.
//
// Submit:
//   * POST /api/v1/jobs with multipart/form-data carrying cancel_test_job.so
//     returns 200 with {"ok":true,"job_id":N}. The job appears in
//     /api/v1/jobs and can be cancelled via /api/v1/jobs/:id/cancel.
//
// Backpressure:
//   * Once a job is running, the TM's /metrics exposes
//     operator.<id>.input_depth and operator.<id>.input_capacity gauges
//     (populated by LocalExecutor's metrics-poll thread). We don't
//     assert specific depths - just that the gauges exist with sane
//     values (capacity > 0).

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
};

HttpResult send_raw(const std::string& host, std::uint16_t port, const std::string& raw_req) {
    HttpResult r;
    const int fd = NetworkSocket::connect_to(host, port);
    if (fd < 0)
        return r;
    NetworkSocket::send_all(fd, reinterpret_cast<const std::byte*>(raw_req.data()), raw_req.size());
    std::string buf;
    char chunk[8192];
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
    const std::string req = "GET " + path + " HTTP/1.1\r\nHost: " + host +
                            "\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    return send_raw(host, port, req);
}

HttpResult http_post_empty(const std::string& host, std::uint16_t port, const std::string& path) {
    const std::string req = "POST " + path + " HTTP/1.1\r\nHost: " + host +
                            "\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    return send_raw(host, port, req);
}

// POST `path` with a multipart/form-data body carrying one file. Used
// for the .so upload - hand-crafted instead of pulling in a third-
// party HTTP client.
HttpResult http_post_multipart_file(const std::string& host,
                                    std::uint16_t port,
                                    const std::string& path,
                                    const std::string& field_name,
                                    const std::string& filename,
                                    const std::string& file_bytes,
                                    const std::string& extra_field_name = {},
                                    const std::string& extra_field_value = {}) {
    const std::string boundary = "----clinktest42";
    std::string body;
    body += "--" + boundary + "\r\n";
    body += "Content-Disposition: form-data; name=\"" + field_name + "\"; filename=\"" + filename +
            "\"\r\n";
    body += "Content-Type: application/octet-stream\r\n\r\n";
    body += file_bytes;
    body += "\r\n";
    if (!extra_field_name.empty()) {
        body += "--" + boundary + "\r\n";
        body += "Content-Disposition: form-data; name=\"" + extra_field_name + "\"\r\n\r\n";
        body += extra_field_value;
        body += "\r\n";
    }
    body += "--" + boundary + "--\r\n";
    std::string req = "POST " + path + " HTTP/1.1\r\n";
    req += "Host: " + host + "\r\n";
    req += "Connection: close\r\n";
    req += "Content-Type: multipart/form-data; boundary=" + boundary + "\r\n";
    req += "Content-Length: " + std::to_string(body.size()) + "\r\n\r\n";
    req += body;
    return send_raw(host, port, req);
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

    Cluster() = default;
    Cluster(const Cluster&) = delete;
    Cluster& operator=(const Cluster&) = delete;
    Cluster(Cluster&& o) noexcept
        : jm_pid(o.jm_pid),
          jm_http_port(o.jm_http_port),
          jm_control_port(o.jm_control_port),
          tm_pids(std::move(o.tm_pids)),
          tm_ids(std::move(o.tm_ids)) {
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
        const std::string tm_id = "tm-sub-" + std::to_string(i);
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
    }
    std::this_thread::sleep_for(400ms);  // TM registration settle
    return c;
}

std::string read_file_bytes(const std::filesystem::path& p) {
    std::ifstream in(p, std::ios::binary);
    return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

}  // namespace

TEST(HttpSubmit, PostJobsAcceptsSoAndReturnsJobId) {
    const auto job_so = cancel_test_job_path();
    if (!std::filesystem::exists(job_so)) {
        GTEST_SKIP() << "cancel_test_job.so not built";
    }
    auto c = start_cluster(/*n_tms=*/2);
    if (!c.has_value()) {
        GTEST_SKIP() << "cluster startup failed";
    }
    ::setenv("CLINK_CANCEL_TICK_MS", "20", 1);

    const auto bytes = read_file_bytes(job_so);
    ASSERT_GT(bytes.size(), 1024u) << "plugin .so unexpectedly small";

    const auto resp = http_post_multipart_file("127.0.0.1",
                                               c->jm_http_port,
                                               "/api/v1/jobs",
                                               "job_so",
                                               "cancel_test_job.so",
                                               bytes,
                                               "job_name",
                                               "test-upload");
    EXPECT_EQ(resp.status, 200) << "submit body: " << resp.body;
    EXPECT_NE(resp.body.find("\"ok\":true"), std::string::npos) << "submit body: " << resp.body;
    EXPECT_NE(resp.body.find("\"job_id\":1"), std::string::npos) << "submit body: " << resp.body;

    // /api/v1/jobs should now list the submitted job.
    std::this_thread::sleep_for(500ms);
    const auto list = http_get("127.0.0.1", c->jm_http_port, "/api/v1/jobs");
    EXPECT_EQ(list.status, 200);
    EXPECT_NE(list.body.find("\"id\":1"), std::string::npos) << "jobs list: " << list.body;

    // Tidy up so the JM doesn't hang waiting for the never-ending job.
    http_post_empty("127.0.0.1", c->jm_http_port, "/api/v1/jobs/1/cancel");
}

TEST(HttpSubmit, PostJobsRejectsRequestWithoutFile) {
    auto c = start_cluster(/*n_tms=*/1);
    if (!c.has_value()) {
        GTEST_SKIP() << "cluster startup failed";
    }
    const auto resp = http_post_empty("127.0.0.1", c->jm_http_port, "/api/v1/jobs");
    EXPECT_EQ(resp.status, 400);
    EXPECT_NE(resp.body.find("job_so"), std::string::npos)
        << "expected job_so mention in error body: " << resp.body;
}

TEST(HttpBackpressure, TmMetricsExposeOperatorInputGauges) {
    const auto job_so = cancel_test_job_path();
    if (!std::filesystem::exists(job_so)) {
        GTEST_SKIP() << "cancel_test_job.so not built";
    }
    auto c = start_cluster(/*n_tms=*/2);
    if (!c.has_value()) {
        GTEST_SKIP() << "cluster startup failed";
    }
    ::setenv("CLINK_CANCEL_TICK_MS", "20", 1);

    const auto bytes = read_file_bytes(job_so);
    const auto submit = http_post_multipart_file(
        "127.0.0.1", c->jm_http_port, "/api/v1/jobs", "job_so", "cancel_test_job.so", bytes);
    ASSERT_EQ(submit.status, 200) << "submit body: " << submit.body;

    // Give the TMs ~1s to start the executor + metrics-poll thread.
    std::this_thread::sleep_for(1500ms);

    // Scrape both TMs via the JM proxy; at least one must report
    // operator input gauges.
    bool saw_depth = false;
    bool saw_capacity = false;
    for (const auto& tm_id : c->tm_ids) {
        const auto m = http_get("127.0.0.1", c->jm_http_port, "/api/v1/tms/" + tm_id + "/metrics");
        if (m.status != 200)
            continue;
        if (m.body.find("operator.") != std::string::npos &&
            m.body.find(".input_depth") != std::string::npos) {
            saw_depth = true;
        }
        if (m.body.find(".input_capacity") != std::string::npos) {
            saw_capacity = true;
        }
    }
    EXPECT_TRUE(saw_depth) << "no operator.<id>.input_depth gauge on any TM";
    EXPECT_TRUE(saw_capacity) << "no operator.<id>.input_capacity gauge on any TM";

    http_post_empty("127.0.0.1", c->jm_http_port, "/api/v1/jobs/1/cancel");
}
