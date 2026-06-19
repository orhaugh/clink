// End-to-end test for client-initiated cancel_job.
//
// Spawns JM + 2 TMs, submits cancel_test_job.so via clink_submit_job
// (the .so emits one int64 every 20 ms forever - only CancelJob stops
// it). After a brief delay we run clink_cancel_job against the JM
// for job_id=1. Expectations:
//   * clink_cancel_job exits 0 with ok=true ack
//   * clink_submit_job exits non-zero (job ended cancelled, not ok)
//   * clink_submit_job's wall time is well under the source's
//     natural lifetime - i.e. the cancel actually shortened the run
//
// This exercises the full pipe: client TCP -> JM handle_cancel_job_ ->
// CancelJob broadcast to each TM -> TM flips per-(job, subtask)
// cancel_token -> LocalExecutor stop_predicate observes it -> source
// produce() returns false -> SubtaskFinished -> signal_job_completion_
// stamps "cancelled by client" -> client sees JobCompleted ok=false.

#include <chrono>
#include <cstdint>
#include <cstdlib>
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

std::filesystem::path submit_binary_path() {
#ifdef CLINK_SUBMIT_BINARY
    return std::filesystem::path{CLINK_SUBMIT_BINARY};
#else
    return {};
#endif
}

std::filesystem::path cancel_binary_path() {
#ifdef CLINK_CANCEL_BINARY
    return std::filesystem::path{CLINK_CANCEL_BINARY};
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

}  // namespace

TEST(CancelJobE2E, ClientInitiatedCancelStopsRunningPipeline) {
    const auto node = node_binary_path();
    if (!std::filesystem::exists(node)) {
        GTEST_SKIP() << "clink_node not built";
    }
    const auto submit = submit_binary_path();
    if (!std::filesystem::exists(submit)) {
        GTEST_SKIP() << "clink_submit_job not built";
    }
    const auto cancel = cancel_binary_path();
    if (!std::filesystem::exists(cancel)) {
        GTEST_SKIP() << "clink_cancel_job not built";
    }
    const auto job_so = cancel_test_job_path();
    if (!std::filesystem::exists(job_so)) {
        GTEST_SKIP() << "cancel_test_job.so not built";
    }

    // Keep the source's tick low so the test runs quickly - the
    // submit-job wall time gives us a margin to fire the cancel
    // before the source has done anything significant.
    ::setenv("CLINK_CANCEL_TICK_MS", "20", 1);

    const auto jm_port = probe_free_port();
    const pid_t jm_pid =
        spawn_proc({"clink_node", "--role=jm", "--port=" + std::to_string(jm_port)}, node);
    ASSERT_GT(jm_pid, 0);
    std::this_thread::sleep_for(200ms);

    std::vector<pid_t> tms;
    for (int i = 1; i <= 2; ++i) {
        tms.push_back(spawn_proc({"clink_node",
                                  "--role=tm",
                                  "--id=tm-cancel-" + std::to_string(i),
                                  "--jm-host=127.0.0.1",
                                  "--jm-port=" + std::to_string(jm_port)},
                                 node));
        ASSERT_GT(tms.back(), 0);
    }
    std::this_thread::sleep_for(300ms);

    const auto t_submit_start = std::chrono::steady_clock::now();
    const pid_t submit_pid = spawn_proc({"clink_submit_job",
                                         "--job=" + job_so.string(),
                                         "--jm-host=127.0.0.1",
                                         "--jm-port=" + std::to_string(jm_port),
                                         "--wait-timeout-s=30",
                                         "--name=cancel-test"},
                                        submit);
    ASSERT_GT(submit_pid, 0);

    // Give the JM time to receive SubmitJob, allocate the JobId,
    // plan + deploy + the TMs time to start their subtask runners.
    // Empirically this needs ~1.5s on dev hardware (the JM blocks in
    // deploy_internal_ until each subtask sends SubtaskListening
    // before issuing PeerUpdate).
    std::this_thread::sleep_for(2s);

    const pid_t cancel_pid = spawn_proc({"clink_cancel_job",
                                         "--job-id=1",
                                         "--jm-host=127.0.0.1",
                                         "--jm-port=" + std::to_string(jm_port)},
                                        cancel);
    ASSERT_GT(cancel_pid, 0);
    int cancel_exit = -1;
    const bool cancel_done = wait_for(cancel_pid, 10s, cancel_exit);

    int submit_exit = -1;
    const bool submit_done = wait_for(submit_pid, 30s, submit_exit);
    const auto t_submit_end = std::chrono::steady_clock::now();

    kill_quietly(jm_pid);
    for (auto pid : tms) {
        kill_quietly(pid);
    }

    ASSERT_TRUE(cancel_done) << "clink_cancel_job did not exit within 10s";
    EXPECT_EQ(cancel_exit, 0) << "clink_cancel_job exited non-zero (" << cancel_exit << ")";

    ASSERT_TRUE(submit_done) << "clink_submit_job did not exit within 30s";
    // The job was cancelled - submit_exit should be non-zero. The
    // exact code (8 = !completed, 9 = !ok) depends on whether the
    // submit tool's JobCompleted arrived before the wait timeout;
    // either way we want a clear "didn't succeed" signal.
    EXPECT_NE(submit_exit, 0)
        << "clink_submit_job exited 0 after cancel - expected non-zero (cancelled)";

    // Cancel arrived ~500 ms in; the submit tool should exit within
    // a few seconds after that, NOT after the source's natural
    // duration. 10 s is a generous ceiling.
    const auto wall_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(t_submit_end - t_submit_start)
            .count();
    EXPECT_LT(wall_ms, 10'000) << "submit wall time " << wall_ms
                               << "ms exceeds ceiling - cancel didn't shorten the run";
}
