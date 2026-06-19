// TM-crash automatic restart test.
//
// 2-TM cluster. Submit the bounded slow-source -> file_2pc_sink job
// with --max-restarts-on-tm-loss=1 and periodic checkpointing on.
// Wait for COMPLETED-1, SIGKILL TM-1, expect the JM to redeploy the
// affected subtask(s) onto TM-2 with restore_from set, and the
// submitter eventually exits 0 with the job marked complete.

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <set>
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
std::filesystem::path submit_binary_path() {
#ifdef CLINK_SUBMIT_BINARY
    return std::filesystem::path{CLINK_SUBMIT_BINARY};
#else
    return {};
#endif
}
std::filesystem::path two_phase_commit_job_path() {
#ifdef CLINK_TWO_PHASE_COMMIT_JOB_PATH
    return std::filesystem::path{CLINK_TWO_PHASE_COMMIT_JOB_PATH};
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

bool wait_for_exit(pid_t pid, std::chrono::milliseconds timeout, int* exit_code = nullptr) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        int status = 0;
        if (::waitpid(pid, &status, WNOHANG) == pid) {
            if (exit_code != nullptr) {
                *exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
            }
            return true;
        }
        std::this_thread::sleep_for(50ms);
    }
    return false;
}

std::uint16_t probe_free_port() {
    NetworkChannelSource<std::int64_t> probe(0, int64_codec());
    return probe.listen();
}

std::filesystem::path mktmpdir(const std::string& tag) {
    static int counter = 0;
    auto dir = std::filesystem::temp_directory_path() /
               ("clink_tmcrash_" + tag + "_" + std::to_string(::getpid()) + "_" +
                std::to_string(++counter));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

bool await_port_open(std::uint16_t port, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        const int fd = NetworkSocket::connect_to("127.0.0.1", port);
        if (fd >= 0) {
            NetworkSocket::close(fd);
            return true;
        }
        std::this_thread::sleep_for(20ms);
    }
    return false;
}

std::uint64_t latest_completed_checkpoint(const std::filesystem::path& ckpt_dir) {
    std::uint64_t latest = 0;
    if (!std::filesystem::exists(ckpt_dir))
        return 0;
    for (const auto& e : std::filesystem::directory_iterator(ckpt_dir)) {
        if (!e.is_regular_file())
            continue;
        const auto name = e.path().filename().string();
        if (name.rfind("COMPLETED-", 0) != 0)
            continue;
        try {
            const auto id = std::stoull(name.substr(std::string{"COMPLETED-"}.size()));
            if (id > latest)
                latest = id;
        } catch (...) {
        }
    }
    return latest;
}

}  // namespace

TEST(TmCrashRecovery, JobSurvivesTmKillViaRestart) {
    const auto node = node_binary_path();
    const auto submit = submit_binary_path();
    const auto job_so = two_phase_commit_job_path();
    if (!std::filesystem::exists(node) || !std::filesystem::exists(submit) ||
        !std::filesystem::exists(job_so)) {
        GTEST_SKIP() << "node/submit/2pc job binary not built";
    }

    const auto out_dir = mktmpdir("out");
    const auto ckpt_dir = mktmpdir("ckpt");
    ::setenv("CLINK_2PC_OUT_DIR", out_dir.c_str(), 1);
    ::setenv("CLINK_2PC_TOTAL", "30", 1);
    // Slow enough for a couple of checkpoints to land before the
    // source finishes, fast enough that the test completes well under
    // its 30s wait window.
    ::setenv("CLINK_2PC_TICK_MS", "60", 1);

    const auto control_port = probe_free_port();
    const pid_t jm = spawn_proc({"clink_node",
                                 "--role=jm",
                                 "--port=" + std::to_string(control_port),
                                 "--bind-host=127.0.0.1"},
                                node);
    ASSERT_GT(jm, 0);
    ASSERT_TRUE(await_port_open(control_port, 2s));

    // Two TMs with 4 slots each. The job uses 2 subtasks; either TM
    // alone can host both. Killing TM-1 forces the JM to redeploy.
    const pid_t tm1 = spawn_proc({"clink_node",
                                  "--role=tm",
                                  "--id=tm-A",
                                  "--jm-host=127.0.0.1",
                                  "--jm-port=" + std::to_string(control_port),
                                  "--slots=4"},
                                 node);
    const pid_t tm2 = spawn_proc({"clink_node",
                                  "--role=tm",
                                  "--id=tm-B",
                                  "--jm-host=127.0.0.1",
                                  "--jm-port=" + std::to_string(control_port),
                                  "--slots=4"},
                                 node);
    ASSERT_GT(tm1, 0);
    ASSERT_GT(tm2, 0);
    std::this_thread::sleep_for(500ms);  // both TMs register

    // Kick off the job with restart-on-loss enabled.
    const pid_t submit_pid = spawn_proc({"clink_submit_job",
                                         "--job=" + job_so.string(),
                                         "--jm-host=127.0.0.1",
                                         "--jm-port=" + std::to_string(control_port),
                                         "--wait-timeout-s=30",
                                         "--checkpoint-dir=" + ckpt_dir.string(),
                                         "--checkpoint-interval-ms=150",
                                         "--max-restarts-on-tm-loss=2"},
                                        submit);
    ASSERT_GT(submit_pid, 0);

    // Wait for at least one checkpoint to complete before the crash.
    const auto deadline = std::chrono::steady_clock::now() + 3s;
    while (std::chrono::steady_clock::now() < deadline &&
           latest_completed_checkpoint(ckpt_dir) == 0) {
        std::this_thread::sleep_for(50ms);
    }
    const auto ckpt_before_crash = latest_completed_checkpoint(ckpt_dir);
    ASSERT_GT(ckpt_before_crash, 0u) << "no checkpoint completed before SIGKILL";

    // SIGKILL one TM mid-job. The watchdog should detect it within
    // ~heartbeat_timeout (5s default), set awaiting_restart, cancel
    // the survivor's subtasks, and redeploy onto TM-B.
    kill_quietly(tm1);

    // The submitter should still exit 0 - the job completes after the
    // automatic restart.
    int submit_exit = -1;
    const bool exited = wait_for_exit(submit_pid, 25s, &submit_exit);

    kill_quietly(tm2);
    kill_quietly(jm);

    ASSERT_TRUE(exited) << "submitter did not exit within 25s after TM crash";
    EXPECT_EQ(submit_exit, 0) << "submitter exited non-zero after restart; the job did not recover";
    EXPECT_GT(latest_completed_checkpoint(ckpt_dir), ckpt_before_crash)
        << "no new checkpoint completed after restart";
}
