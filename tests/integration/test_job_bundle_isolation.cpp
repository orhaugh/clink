// Cross-job bundle-isolation test.
//
// Two CLINK_REGISTER_JOB .so's mint overlapping inline-op names
// (_inline_from_elements_0, _inline_map_1) against different
// pipelines. Submitting both concurrently to the same coordinator+worker cluster
// exercises per-job-bundle isolation: each job's bundle holds its
// own copy of the closures, so the two jobs don't trample each
// other's registrations.
//
// Pass criterion: both sink files exist and contain the values the
// respective pipeline computes - A=[10,20,30,40,50], B=[101,201,301].
// If the bundles weren't isolated, one job would resolve to the
// other's closures and the outputs would be wrong (or one job's sink
// would be empty).

#include <algorithm>
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

std::filesystem::path collision_job_a_path() {
#ifdef CLINK_COLLISION_JOB_A_PATH
    return std::filesystem::path{CLINK_COLLISION_JOB_A_PATH};
#else
    return {};
#endif
}

std::filesystem::path collision_job_b_path() {
#ifdef CLINK_COLLISION_JOB_B_PATH
    return std::filesystem::path{CLINK_COLLISION_JOB_B_PATH};
#else
    return {};
#endif
}

pid_t spawn_proc(const std::vector<std::string>& argv, const std::filesystem::path& binary_path) {
    std::vector<char*> raw_argv;
    raw_argv.reserve(argv.size() + 1);
    for (const auto& s : argv) {
        raw_argv.push_back(const_cast<char*>(s.c_str()));
    }
    raw_argv.push_back(nullptr);
    pid_t pid = -1;
    const auto rc =
        posix_spawn(&pid, binary_path.c_str(), nullptr, nullptr, raw_argv.data(), environ);
    return rc == 0 ? pid : -1;
}

bool wait_for(pid_t pid, std::chrono::milliseconds timeout, int& exit_code) {
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

std::vector<std::int64_t> read_int64_lines(const std::filesystem::path& p) {
    std::vector<std::int64_t> out;
    std::ifstream in(p);
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) {
            continue;
        }
        out.push_back(std::stoll(line));
    }
    return out;
}

}  // namespace

TEST(JobBundleIsolation, ConcurrentJobsWithCollidingInlineNamesDontTrample) {
    const auto node = node_binary_path();
    if (!std::filesystem::exists(node)) {
        GTEST_SKIP() << "clink_node not built";
    }
    const auto submit = submit_binary_path();
    if (!std::filesystem::exists(submit)) {
        GTEST_SKIP() << "clink_submit_job not built";
    }
    const auto job_a = collision_job_a_path();
    const auto job_b = collision_job_b_path();
    if (!std::filesystem::exists(job_a) || !std::filesystem::exists(job_b)) {
        GTEST_SKIP() << "collision_job_{a,b}.so not built";
    }

    const auto out_a = std::filesystem::temp_directory_path() / "clink_bundle_iso_a.txt";
    const auto out_b = std::filesystem::temp_directory_path() / "clink_bundle_iso_b.txt";
    std::filesystem::remove(out_a);
    std::filesystem::remove(out_b);

    // Each job's .so reads its sink path from getenv at build_fn time.
    // Set both BEFORE spawning the cluster so coordinator + workers inherit them.
    ::setenv("CLINK_COLLISION_OUT_A", out_a.c_str(), 1);
    ::setenv("CLINK_COLLISION_OUT_B", out_b.c_str(), 1);

    const auto coordinator_port = probe_free_port();
    const pid_t coordinator_pid = spawn_proc(
        {"clink_node", "--role=coordinator", "--port=" + std::to_string(coordinator_port)}, node);
    ASSERT_GT(coordinator_pid, 0);
    std::this_thread::sleep_for(200ms);

    // 2 subtasks per job (source + sink) * 2 jobs = 4 slots needed.
    // Spawn enough workers (each worker has 1 slot by default).
    std::vector<pid_t> workers;
    for (int i = 1; i <= 4; ++i) {
        workers.push_back(spawn_proc({"clink_node",
                                      "--role=worker",
                                      "--id=worker-iso-" + std::to_string(i),
                                      "--coordinator-host=127.0.0.1",
                                      "--coordinator-port=" + std::to_string(coordinator_port)},
                                     node));
        ASSERT_GT(workers.back(), 0);
    }
    std::this_thread::sleep_for(400ms);

    // Submit both jobs concurrently. The submit CLI is one-shot per
    // invocation; we spawn two processes in parallel so the coordinator sees
    // them overlap (modulo OS scheduling).
    const pid_t submit_a = spawn_proc({"clink_submit_job",
                                       "--job=" + job_a.string(),
                                       "--coordinator-host=127.0.0.1",
                                       "--coordinator-port=" + std::to_string(coordinator_port),
                                       "--wait-timeout-s=30",
                                       "--name=collision-a"},
                                      submit);
    ASSERT_GT(submit_a, 0);
    const pid_t submit_b = spawn_proc({"clink_submit_job",
                                       "--job=" + job_b.string(),
                                       "--coordinator-host=127.0.0.1",
                                       "--coordinator-port=" + std::to_string(coordinator_port),
                                       "--wait-timeout-s=30",
                                       "--name=collision-b"},
                                      submit);
    ASSERT_GT(submit_b, 0);

    int exit_a = -1;
    int exit_b = -1;
    const bool done_a = wait_for(submit_a, 45s, exit_a);
    const bool done_b = wait_for(submit_b, 45s, exit_b);

    kill_quietly(coordinator_pid);
    for (auto pid : workers) {
        kill_quietly(pid);
    }

    ASSERT_TRUE(done_a) << "submit A did not exit within 45s";
    ASSERT_TRUE(done_b) << "submit B did not exit within 45s";
    EXPECT_EQ(exit_a, 0) << "submit A exited non-zero (" << exit_a << ")";
    EXPECT_EQ(exit_b, 0) << "submit B exited non-zero (" << exit_b << ")";

    auto vals_a = read_int64_lines(out_a);
    auto vals_b = read_int64_lines(out_b);
    std::sort(vals_a.begin(), vals_a.end());
    std::sort(vals_b.begin(), vals_b.end());

    EXPECT_EQ(vals_a, (std::vector<std::int64_t>{10, 20, 30, 40, 50}))
        << "job A output is wrong - bundle isolation failed (it may have resolved B's "
           "_inline_map_1)";
    EXPECT_EQ(vals_b, (std::vector<std::int64_t>{101, 201, 301}))
        << "job B output is wrong - bundle isolation failed (it may have resolved A's "
           "_inline_map_1)";

    std::filesystem::remove(out_a);
    std::filesystem::remove(out_b);
}
