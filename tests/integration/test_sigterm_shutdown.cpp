// SIGTERM graceful-shutdown test for clink_node.
//
// Spawns a coordinator (then a worker in a second sub-test) and sends SIGTERM via
// kill(2). Verifies the process exits 0 within a generous window
// (~2 s), not with a signal-terminated status. Without the handler
// installed in main(), the default action for SIGTERM is terminate,
// so a regression - accidental removal of install_shutdown_signal_handler
// or a syscall blocking the role mainloop past the poll interval -
// would surface here as either WIFSIGNALED=true OR a timeout.

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

std::uint16_t probe_free_port() {
    NetworkChannelSource<std::int64_t> probe(0, int64_codec());
    return probe.listen();
}

// Wait for `pid` to exit. Return (exited, was_signal_terminated, exit_or_signal_code).
struct ExitInfo {
    bool exited{false};
    bool signal_terminated{false};
    int code{-1};
};
ExitInfo wait_for_exit(pid_t pid, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        int status = 0;
        const pid_t r = waitpid(pid, &status, WNOHANG);
        if (r == pid) {
            ExitInfo e;
            e.exited = true;
            if (WIFEXITED(status)) {
                e.code = WEXITSTATUS(status);
            } else if (WIFSIGNALED(status)) {
                e.signal_terminated = true;
                e.code = WTERMSIG(status);
            }
            return e;
        }
        std::this_thread::sleep_for(50ms);
    }
    return ExitInfo{};
}

void hard_kill(pid_t pid) {
    if (pid > 0) {
        ::kill(pid, SIGKILL);
        int s = 0;
        ::waitpid(pid, &s, 0);
    }
}

}  // namespace

TEST(SigtermShutdown, CoordinatorExitsCleanlyOnSIGTERM) {
    const auto node = node_binary_path();
    if (!std::filesystem::exists(node)) {
        GTEST_SKIP() << "clink_node not built";
    }
    const auto port = probe_free_port();
    const pid_t pid =
        spawn_proc({"clink_node", "--role=coordinator", "--port=" + std::to_string(port)}, node);
    ASSERT_GT(pid, 0);
    // Let the coordinator bind + start its accept loop before signalling.
    std::this_thread::sleep_for(300ms);

    ::kill(pid, SIGTERM);
    const auto info = wait_for_exit(pid, 5s);

    if (!info.exited) {
        hard_kill(pid);
    }
    ASSERT_TRUE(info.exited) << "clink_node coordinator did not exit within 5s after SIGTERM";
    EXPECT_FALSE(info.signal_terminated) << "coordinator was killed by signal " << info.code
                                         << " instead of handling SIGTERM cleanly";
    EXPECT_EQ(info.code, 0) << "coordinator exited non-zero (" << info.code << ")";
}

TEST(SigtermShutdown, WorkerExitsCleanlyOnSIGTERM) {
    const auto node = node_binary_path();
    if (!std::filesystem::exists(node)) {
        GTEST_SKIP() << "clink_node not built";
    }

    // We need a coordinator up for the worker to register against - without it, the
    // worker exits with an error immediately and the test asserts nothing
    // useful about shutdown handling.
    const auto port = probe_free_port();
    const pid_t coordinator_pid =
        spawn_proc({"clink_node", "--role=coordinator", "--port=" + std::to_string(port)}, node);
    ASSERT_GT(coordinator_pid, 0);
    std::this_thread::sleep_for(200ms);

    const pid_t worker_pid = spawn_proc({"clink_node",
                                         "--role=worker",
                                         "--id=worker-sigterm",
                                         "--coordinator-host=127.0.0.1",
                                         "--coordinator-port=" + std::to_string(port)},
                                        node);
    ASSERT_GT(worker_pid, 0);
    std::this_thread::sleep_for(400ms);

    ::kill(worker_pid, SIGTERM);
    const auto info = wait_for_exit(worker_pid, 5s);

    hard_kill(coordinator_pid);
    if (!info.exited) {
        hard_kill(worker_pid);
    }
    ASSERT_TRUE(info.exited) << "clink_node worker did not exit within 5s after SIGTERM";
    EXPECT_FALSE(info.signal_terminated)
        << "worker was killed by signal " << info.code << " instead of handling SIGTERM cleanly";
    EXPECT_EQ(info.code, 0) << "worker exited non-zero (" << info.code << ")";
}
