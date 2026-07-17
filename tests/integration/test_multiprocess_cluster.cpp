// Multi-process cluster integration test, end-to-end submission flow.
//
// Spawns 1x coordinator + 2x workers as real OS processes, then submits a job in-
// process via clink::application::JobSubmitter (no client subprocess,
// no JSON file on disk). Verifies the sink output.

#include <array>
#include <chrono>
#include <cstdio>
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

#include "clink/application/job_submitter.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/core/codec.hpp"
#include "clink/runtime/network/network_channel.hpp"

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

pid_t spawn_node(const std::vector<std::string>& argv, const std::filesystem::path& binary_path) {
    std::vector<char*> raw_argv;
    raw_argv.reserve(argv.size() + 1);
    for (const auto& s : argv) {
        raw_argv.push_back(const_cast<char*>(s.c_str()));
    }
    raw_argv.push_back(nullptr);

    pid_t pid = -1;
    const auto rc =
        posix_spawn(&pid, binary_path.c_str(), nullptr, nullptr, raw_argv.data(), environ);
    if (rc != 0) {
        return -1;
    }
    return pid;
}

[[maybe_unused]] bool wait_for(pid_t pid, std::chrono::milliseconds timeout, int& exit_code) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        int status = 0;
        pid_t r = waitpid(pid, &status, WNOHANG);
        if (r == pid) {
            if (WIFEXITED(status)) {
                exit_code = WEXITSTATUS(status);
                return true;
            }
            if (WIFSIGNALED(status)) {
                exit_code = 128 + WTERMSIG(status);
                return true;
            }
        }
        std::this_thread::sleep_for(50ms);
    }
    return false;
}

void kill_quietly(pid_t pid) {
    if (pid > 0) {
        kill(pid, SIGKILL);
        int s = 0;
        ::waitpid(pid, &s, 0);
    }
}

std::uint16_t probe_free_port() {
    NetworkChannelSource<std::int64_t> probe(0, int64_codec());
    return probe.listen();
}

}  // namespace

TEST(MultiprocessCluster, SubmitJobOverWireProducesExpectedOutput) {
    const auto binary = node_binary_path();
    if (!std::filesystem::exists(binary)) {
        GTEST_SKIP() << "clink_node binary not found at " << binary
                     << "; build with `cmake --build ... --target clink_node`";
    }

    const auto coordinator_port = probe_free_port();
    const auto out_path = std::filesystem::temp_directory_path() / "clink_submit_test_output.txt";
    std::filesystem::remove(out_path);

    // Write the job graph the client will submit. Pipeline:
    //   int64_range_source(count=5, start=100, step=100)
    //     -> file_int64_sink(path=out_path)
    clink::cluster::JobGraphSpec graph;
    {
        clink::cluster::OperatorSpec op;
        op.id = "src";
        op.type = "int64_range_source";
        op.out_channel = "int64";
        op.params = {{"count", "5"}, {"start", "100"}, {"step", "100"}};
        graph.ops.push_back(std::move(op));
    }
    {
        clink::cluster::OperatorSpec op;
        op.id = "snk";
        op.type = "file_int64_sink";
        op.out_channel = "int64";
        op.inputs = {"src"};
        op.params = {{"path", out_path.string()}};
        graph.ops.push_back(std::move(op));
    }

    // 1. coordinator: empty, idles forever waiting for clients.
    const std::vector<std::string> coordinator_argv{
        "clink_node",
        "--role=coordinator",
        "--port=" + std::to_string(coordinator_port),
    };
    const pid_t coordinator_pid = spawn_node(coordinator_argv, binary);
    ASSERT_GT(coordinator_pid, 0);
    std::this_thread::sleep_for(200ms);  // give coordinator time to bind.

    // 2. workers: empty, register and idle.
    const std::vector<std::string> worker_a_argv{
        "clink_node",
        "--role=worker",
        "--id=worker-a",
        "--coordinator-host=127.0.0.1",
        "--coordinator-port=" + std::to_string(coordinator_port),
    };
    const std::vector<std::string> worker_b_argv{
        "clink_node",
        "--role=worker",
        "--id=worker-b",
        "--coordinator-host=127.0.0.1",
        "--coordinator-port=" + std::to_string(coordinator_port),
    };
    const pid_t worker_a_pid = spawn_node(worker_a_argv, binary);
    const pid_t worker_b_pid = spawn_node(worker_b_argv, binary);
    ASSERT_GT(worker_a_pid, 0);
    ASSERT_GT(worker_b_pid, 0);
    std::this_thread::sleep_for(300ms);  // give workers time to register.

    // 3. Submit the graph in-process via JobSubmitter and wait for completion.
    clink::application::JobSubmitter submitter("127.0.0.1", coordinator_port);
    clink::application::SubmitOptions opts;
    opts.wait_timeout = std::chrono::seconds(15);
    const auto result = submitter.submit(graph.to_json(), {}, opts);

    // Tear down coordinator and workers (they idle forever otherwise).
    kill_quietly(coordinator_pid);
    kill_quietly(worker_a_pid);
    kill_quietly(worker_b_pid);

    ASSERT_TRUE(result.completed) << "reject: " << result.reject_message;
    EXPECT_TRUE(result.ok) << "errors: " << (result.errors.empty() ? "(none)" : result.errors[0]);

    // Verify the sink wrote the expected records.
    std::ifstream in(out_path);
    std::vector<std::int64_t> received;
    std::int64_t v = 0;
    while (in >> v) {
        received.push_back(v);
    }
    EXPECT_EQ(received, (std::vector<std::int64_t>{100, 200, 300, 400, 500}));
    std::filesystem::remove(out_path);
}
