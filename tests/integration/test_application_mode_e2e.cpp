// End-to-end test for clink_app ( Application Mode equivalent).
//
// clink_app starts a coordinator in its own process, dlopens a compiled job
// .so locally, submits via the in-process Coordinator API, waits for
// completion, then exits. External workers register with that coordinator in the
// usual way. The test spawns clink_app + the required workers and
// asserts clink_app exits 0 with the expected sink output.
//
// Compared to JobPluginE2E (which exercises clink_submit_job against
// a pre-existing coordinator), this test exercises the coordinator-lives-for-the-job
// shape that's the canonical  Application Mode lifecycle.

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

std::filesystem::path app_binary_path() {
#ifdef CLINK_APP_BINARY
    return std::filesystem::path{CLINK_APP_BINARY};
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

TEST(ApplicationModeE2E, AppBinaryStartsCoordinatorRunsJobThenExits) {
    const auto node = node_binary_path();
    if (!std::filesystem::exists(node)) {
        GTEST_SKIP() << "clink_node not built";
    }
    const auto app = app_binary_path();
    if (!std::filesystem::exists(app)) {
        GTEST_SKIP() << "clink_app not built";
    }
    const auto job_so = collision_job_a_path();
    if (!std::filesystem::exists(job_so)) {
        GTEST_SKIP() << "collision_job_a.so not built";
    }

    const auto out_path = std::filesystem::temp_directory_path() / "clink_app_e2e.txt";
    std::filesystem::remove(out_path);

    // collision_job_a reads its sink path from this env var at build_fn
    // time. Set BEFORE spawning clink_app + workers so all three inherit.
    ::setenv("CLINK_COLLISION_OUT_A", out_path.c_str(), 1);

    const auto port = probe_free_port();

    // Spawn clink_app. It starts a coordinator bound to `port`, dlopens the
    // .so, submits in-process. It will block in submit_job until the
    // workers we spawn next register and provide enough slots.
    const pid_t app_pid = spawn_proc({"clink_app",
                                      "--job=" + job_so.string(),
                                      "--port=" + std::to_string(port),
                                      "--bind=127.0.0.1",
                                      "--advertise=127.0.0.1",
                                      "--wait-slots-s=10",
                                      "--wait-job-s=30",
                                      "--name=app-e2e"},
                                     app);
    ASSERT_GT(app_pid, 0);

    // Small delay so the coordinator is listening before workers try to connect.
    std::this_thread::sleep_for(200ms);

    // collision_job_a: from_elements (1 subtask) + map (1 subtask) +
    // sink (1 subtask) = 3 slots. Spawn 3 workers (each defaults to 1 slot).
    std::vector<pid_t> workers;
    for (int i = 1; i <= 3; ++i) {
        workers.push_back(spawn_proc({"clink_node",
                                      "--role=worker",
                                      "--id=worker-app-" + std::to_string(i),
                                      "--coordinator-host=127.0.0.1",
                                      "--coordinator-port=" + std::to_string(port)},
                                     node));
        ASSERT_GT(workers.back(), 0);
    }

    int app_exit = -1;
    const bool app_done = wait_for(app_pid, 45s, app_exit);

    // workers were children of this test process; clink_app exits as soon
    // as the job completes (its coordinator destructor stops the listener and
    // closes the worker connections), so the workers will fail their next read
    // and exit naturally. We send SIGKILL as a safety net only.
    for (auto pid : workers) {
        kill_quietly(pid);
    }

    ASSERT_TRUE(app_done) << "clink_app did not exit within 45s";
    EXPECT_EQ(app_exit, 0) << "clink_app exited non-zero (" << app_exit << ")";

    auto vals = read_int64_lines(out_path);
    std::sort(vals.begin(), vals.end());
    EXPECT_EQ(vals, (std::vector<std::int64_t>{10, 20, 30, 40, 50}));

    std::filesystem::remove(out_path);
}
