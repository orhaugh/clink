// coordinator HA hot-failover integration test.
//
// 2 coordinators sharing --ha-dir race for leadership. 1 worker also uses --ha-dir
// to discover whichever coordinator is currently leader. After the leader has
// submitted + checkpointed a job, SIGKILL the leader. Expect:
//   * The standby coordinator acquires leadership (its fcntl lock attempt
//     succeeds once the dead leader's lock fd closes).
//   * The standby's on_become_leader callback runs
//     recover_persisted_jobs, which finds the manifest and re-submits
//     the job with restore_from set to the latest COMPLETED-N marker.
//   * The worker detects the coordinator disconnect (reader_loop_ exits) and the
//     clink_node process exits non-zero. The test re-spawns it; the
//     restart re-reads active-leader.json and finds the new leader.
//   * The recovered job completes via the new leader; the submitter
//     (running against coordinator-A's port) DIES with coordinator-A but a SECOND
//     submitter run against the new leader sees the job already in
//     flight (or completed). We assert on the checkpoint side: more
//     COMPLETED-N markers appear after the failover.

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

std::filesystem::path mktmpdir(const std::string& tag) {
    static int counter = 0;
    auto dir =
        std::filesystem::temp_directory_path() /
        ("clink_haf_" + tag + "_" + std::to_string(::getpid()) + "_" + std::to_string(++counter));
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

std::uint64_t latest_completed_checkpoint(const std::filesystem::path& ckpt_dir,
                                          std::uint64_t /*job_id*/) {
    // coordinator writes COMPLETED-N markers at the top of checkpoint_dir (not
    // namespaced per job - single-job runs are the common case).
    if (!std::filesystem::exists(ckpt_dir))
        return 0;
    std::uint64_t latest = 0;
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

bool active_leader_endpoint(const std::filesystem::path& ha_dir, std::uint16_t* out_port) {
    std::ifstream in(ha_dir / "active-leader.json");
    if (!in)
        return false;
    std::string body((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    const auto needle = std::string{"\"port\":"};
    auto pos = body.find(needle);
    if (pos == std::string::npos)
        return false;
    pos += needle.size();
    try {
        *out_port = static_cast<std::uint16_t>(std::stoul(body.substr(pos)));
        return true;
    } catch (...) {
        return false;
    }
}

}  // namespace

TEST(CoordinatorHaFailover, StandbyTakesOverAndRecoversJob) {
    const auto node = node_binary_path();
    const auto submit = submit_binary_path();
    const auto job_so = two_phase_commit_job_path();
    if (!std::filesystem::exists(node) || !std::filesystem::exists(submit) ||
        !std::filesystem::exists(job_so)) {
        GTEST_SKIP() << "node/submit/2pc job binary not built";
    }

    const auto out_dir = mktmpdir("out");
    const auto ckpt_dir = mktmpdir("ckpt");
    const auto ha_dir = mktmpdir("ha");
    ::setenv("CLINK_2PC_OUT_DIR", out_dir.c_str(), 1);
    ::setenv("CLINK_2PC_TOTAL", "30", 1);
    ::setenv("CLINK_2PC_TICK_MS", "60", 1);

    // Spawn coordinator-A (lower port - likely to be leader first).
    const auto port_a = probe_free_port();
    const pid_t coordinator_a = spawn_proc({"clink_node",
                                            "--role=coordinator",
                                            "--port=" + std::to_string(port_a),
                                            "--bind-host=127.0.0.1",
                                            "--ha-dir=" + ha_dir.string()},
                                           node);
    ASSERT_GT(coordinator_a, 0);
    // coordinator-A should bind within ~500ms (poll interval).
    ASSERT_TRUE(await_port_open(port_a, 2s)) << "coordinator-A never bound port " << port_a;

    // Spawn coordinator-B as standby. Its port is different; it sits on the
    // coordinator until coordinator-A dies.
    const auto port_b = probe_free_port();
    const pid_t coordinator_b = spawn_proc({"clink_node",
                                            "--role=coordinator",
                                            "--port=" + std::to_string(port_b),
                                            "--bind-host=127.0.0.1",
                                            "--ha-dir=" + ha_dir.string()},
                                           node);
    ASSERT_GT(coordinator_b, 0);
    std::this_thread::sleep_for(500ms);

    // worker: discovers coordinator-A via active-leader.json, connects, registers.
    auto spawn_worker = [&] {
        return spawn_proc({"clink_node",
                           "--role=worker",
                           "--id=worker-ha-1",
                           "--slots=4",
                           "--ha-dir=" + ha_dir.string()},
                          node);
    };
    pid_t worker = spawn_worker();
    ASSERT_GT(worker, 0);
    std::this_thread::sleep_for(800ms);  // worker register settle

    // Submit a job to coordinator-A. Lasts ~1.8s; we wait for at least one
    // COMPLETED-N marker, then kill coordinator-A.
    const pid_t submit_pid = spawn_proc({"clink_submit_job",
                                         "--job=" + job_so.string(),
                                         "--coordinator-host=127.0.0.1",
                                         "--coordinator-port=" + std::to_string(port_a),
                                         "--wait-timeout-s=20",
                                         "--checkpoint-dir=" + ckpt_dir.string(),
                                         "--checkpoint-interval-ms=150",
                                         "--max-restarts-on-worker-loss=0"},
                                        submit);
    ASSERT_GT(submit_pid, 0);

    // Wait for first checkpoint on job_id=1 BEFORE the crash.
    const auto ckpt_deadline = std::chrono::steady_clock::now() + 4s;
    while (std::chrono::steady_clock::now() < ckpt_deadline &&
           latest_completed_checkpoint(ckpt_dir, 1) == 0) {
        std::this_thread::sleep_for(50ms);
    }
    const auto ckpt_before = latest_completed_checkpoint(ckpt_dir, 1);
    ASSERT_GT(ckpt_before, 0u) << "no checkpoint before failover";

    // SIGKILL coordinator-A. Submitter loses its connection and exits non-zero.
    kill_quietly(coordinator_a);
    kill_quietly(submit_pid);

    // Within ~500ms (poll interval), coordinator-B's coordinator should
    // acquire the lock and bind port_b.
    ASSERT_TRUE(await_port_open(port_b, 3s)) << "coordinator-B never took over";

    // Confirm active-leader.json now points to port_b.
    std::uint16_t leader_port = 0;
    EXPECT_TRUE(active_leader_endpoint(ha_dir, &leader_port));
    EXPECT_EQ(leader_port, port_b) << "active-leader.json didn't flip to coordinator-B";

    // The worker should have exited (it watches the coordinator connection). Reap
    // and respawn - a supervisor would do this automatically. The new
    // worker reads active-leader.json and connects to coordinator-B.
    EXPECT_TRUE(wait_for_exit(worker, 4s)) << "worker didn't exit after coordinator-A crash";
    worker = spawn_worker();
    ASSERT_GT(worker, 0);
    std::this_thread::sleep_for(800ms);

    // The recovered job should make further checkpoint progress under
    // coordinator-B. Wait a few seconds and confirm ckpt_after > ckpt_before.
    const auto progress_deadline = std::chrono::steady_clock::now() + 8s;
    std::uint64_t ckpt_after = ckpt_before;
    while (std::chrono::steady_clock::now() < progress_deadline) {
        ckpt_after = latest_completed_checkpoint(ckpt_dir, 1);
        if (ckpt_after > ckpt_before)
            break;
        std::this_thread::sleep_for(100ms);
    }
    EXPECT_GT(ckpt_after, ckpt_before)
        << "coordinator-B didn't drive any new checkpoints after takeover; "
        << "before=" << ckpt_before << " after=" << ckpt_after;

    kill_quietly(worker);
    kill_quietly(coordinator_b);
}
