// End-to-end test for the "job as plugin .so" contract.
//
// Spawns a coordinator + 6 workers as separate processes via posix_spawn, then
// drives the clink_submit_job CLI to submit a canonical pipeline
// .so (built from examples/canonical_pipeline_job.cpp by
// CLINK_REGISTER_JOB). The .so contains the user's inline lambdas
// (map / filter / key_by / sliding_window / aggregate); the submit
// CLI dlopens it locally to retrieve the JobGraphSpec, the coordinator ships
// the .so to each worker, and each worker dlopens it under std::call_once so
// the inline-op registrations resolve there too.
//
// Asserts the sink wrote the per-window aggregates the pipeline
// computes, which only happens if the build_fn ran on every worker and
// the same _inline_<kind>_<n> op-types resolved in every process.
//
// The job's output path is configured via CLINK_CANONICAL_OUT_PATH;
// we setenv() before spawning coordinator + workers so the child processes inherit it.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
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

std::filesystem::path submit_binary_path() {
#ifdef CLINK_SUBMIT_BINARY
    return std::filesystem::path{CLINK_SUBMIT_BINARY};
#else
    return {};
#endif
}

std::filesystem::path canonical_job_path() {
#ifdef CLINK_CANONICAL_JOB_PATH
    return std::filesystem::path{CLINK_CANONICAL_JOB_PATH};
#else
    return {};
#endif
}

pid_t spawn_proc(const std::vector<std::string>& argv,
                 const std::filesystem::path& binary_path,
                 const std::filesystem::path& stdout_path = {}) {
    std::vector<char*> raw_argv;
    raw_argv.reserve(argv.size() + 1);
    for (const auto& s : argv) {
        raw_argv.push_back(const_cast<char*>(s.c_str()));
    }
    raw_argv.push_back(nullptr);
    pid_t pid = -1;
    posix_spawn_file_actions_t actions;
    posix_spawn_file_actions_t* actions_ptr = nullptr;
    if (!stdout_path.empty()) {
        posix_spawn_file_actions_init(&actions);
        posix_spawn_file_actions_addopen(
            &actions, STDOUT_FILENO, stdout_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        actions_ptr = &actions;
    }
    const auto rc =
        posix_spawn(&pid, binary_path.c_str(), actions_ptr, nullptr, raw_argv.data(), environ);
    if (actions_ptr != nullptr) {
        posix_spawn_file_actions_destroy(&actions);
    }
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

TEST(JobPluginE2E, CanonicalPipelineRunsAcrossSeparateProcesses) {
    const auto node = node_binary_path();
    if (!std::filesystem::exists(node)) {
        GTEST_SKIP() << "clink_node not built";
    }
    const auto submit = submit_binary_path();
    if (!std::filesystem::exists(submit)) {
        GTEST_SKIP() << "clink_submit_job not built";
    }
    const auto job_so = canonical_job_path();
    if (!std::filesystem::exists(job_so)) {
        GTEST_SKIP() << "canonical_pipeline_job.so not built";
    }

    const auto out_path = std::filesystem::temp_directory_path() / "clink_job_plugin_e2e.txt";
    std::filesystem::remove(out_path);

    // The job .so reads its sink path from CLINK_CANONICAL_OUT_PATH at
    // build_fn time. Set it BEFORE spawning the cluster so coordinator + workers
    // inherit it (workers dlopen the .so in their own process and re-run
    // build_fn under std::call_once).
    ::setenv("CLINK_CANONICAL_OUT_PATH", out_path.c_str(), 1);

    const auto coordinator_port = probe_free_port();
    // Captured so worker registration becomes observable - see the wait below.
    const auto coordinator_log = std::filesystem::temp_directory_path() /
                                 ("clink_jpe2e_coordinator_" + std::to_string(::getpid()) + ".log");
    std::filesystem::remove(coordinator_log);
    const pid_t coordinator_pid = clink::itest::spawn_logged(
        {"clink_node", "--role=coordinator", "--port=" + std::to_string(coordinator_port)},
        node,
        coordinator_log);
    ASSERT_GT(coordinator_pid, 0);
    // Wait for the coordinator to be ACCEPTING, not for a duration. A sleep here
    // is a guess at process start-up: too short and the submit below races the
    // bind on a loaded machine, too long and every run pays for it.
    ASSERT_TRUE(clink::itest::await_port_accepting(coordinator_port))
        << "the coordinator never accepted on its control port";

    // Pipeline subtask slots: from_elements + map + filter +
    // ts_monotonic + sliding_aggregate + sink = 6 (each at par=1).
    std::vector<pid_t> workers;
    for (int i = 1; i <= 6; ++i) {
        workers.push_back(spawn_proc({"clink_node",
                                      "--role=worker",
                                      "--id=worker-jpe2e-" + std::to_string(i),
                                      "--coordinator-host=127.0.0.1",
                                      "--coordinator-port=" + std::to_string(coordinator_port)},
                                     node));
        ASSERT_GT(workers.back(), 0);
    }
    // All six, by the coordinator's own count. A submission that arrives before
    // the slots exist is rejected outright, and 400ms was a guess made on macOS -
    // on Linux these processes each dlopen an 84 MB plugin.
    ASSERT_TRUE(clink::itest::await_log_matches(coordinator_log, " slots=", 6))
        << "not all six workers registered with the coordinator";

    const auto submit_out =
        std::filesystem::temp_directory_path() / "clink_job_plugin_e2e_submit.out";
    const pid_t submit_pid = spawn_proc({"clink_submit_job",
                                         "--job=" + job_so.string(),
                                         "--coordinator-host=127.0.0.1",
                                         "--coordinator-port=" + std::to_string(coordinator_port),
                                         "--wait-timeout-s=30",
                                         "--name=canonical-pipeline-e2e"},
                                        submit,
                                        submit_out);
    ASSERT_GT(submit_pid, 0);

    int submit_exit = -1;
    const bool exited = wait_for(submit_pid, 45s, submit_exit);

    kill_quietly(coordinator_pid);
    for (auto pid : workers) {
        kill_quietly(pid);
    }

    ASSERT_TRUE(exited) << "clink_submit_job did not exit within 45s";
    EXPECT_EQ(submit_exit, 0) << "clink_submit_job exited non-zero (" << submit_exit << ")";

    // The submit must report the JOB ID it created, machine-readably. The
    // id is the handle for savepoint, cancel and status, and this was the
    // one submission path that never printed it - which made a compiled
    // job operationally second-class and blocked QUAL-11's driver until it
    // was fixed.
    {
        std::ifstream in(submit_out);
        std::string body((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        const auto pos = body.find("\"job_id\":");
        ASSERT_NE(pos, std::string::npos) << "submit printed no machine-readable job_id: " << body;
        const auto id = std::strtoull(body.c_str() + pos + 9, nullptr, 10);
        EXPECT_GT(id, 0U) << "submit reported job_id 0: " << body;
    }

    // The pipeline emits one int64 per (key, window) on EOS flush.
    // Inputs 1..5 -> *10 -> 10,20,30,40,50; filter >20 -> 30,40,50;
    // key_by (v/10)%2 -> 30 -> 1, 40 -> 0, 50 -> 1; single 60ms window
    // -> aggregates: key 0 -> 40; key 1 -> 30+50 = 80. Sink at par=1
    // writes one int64 per line; emission order between the two keys
    // is undefined, so we sort before asserting.
    ASSERT_TRUE(std::filesystem::exists(out_path))
        << "sink file " << out_path << " was not created";
    std::ifstream in(out_path);
    std::vector<std::int64_t> values;
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) {
            continue;
        }
        values.push_back(std::stoll(line));
    }
    std::sort(values.begin(), values.end());
    EXPECT_EQ(values, (std::vector<std::int64_t>{40, 80}));
    std::filesystem::remove(out_path);
}
