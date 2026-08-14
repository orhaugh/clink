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

#include "tests/integration/await_port.hpp"
#include "tests/integration/two_pc_output.hpp"

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
                                          std::uint64_t job_id) {
    // The coordinator writes markers at
    // <checkpoint_dir>/_jobs/<job_id>/COMPLETED-<id>. This scanned the TOP
    // LEVEL and ignored the job id it was handed, with a comment asserting
    // markers were not namespaced per job - true once, and false since they
    // were scoped. It therefore reported 0 for a job that was checkpointing
    // normally, and the failover test read that as "no checkpoint to recover
    // from".
    //
    // Scoped to the job when one is given, recursive otherwise, so a caller
    // that does not know the id still finds markers.
    std::uint64_t latest = 0;
    std::error_code ec;
    const auto scan_root = job_id != 0 ? ckpt_dir / "_jobs" / std::to_string(job_id) : ckpt_dir;
    if (!std::filesystem::exists(scan_root, ec)) {
        return 0;
    }
    for (const auto& e : std::filesystem::recursive_directory_iterator(scan_root, ec)) {
        if (ec) {
            break;
        }
        if (!e.is_regular_file())
            continue;
        const auto name = e.path().filename().string();
        if (name.rfind("COMPLETED-", 0) != 0)
            continue;
        try {
            const auto id = static_cast<std::uint64_t>(
                std::stoull(name.substr(std::string{"COMPLETED-"}.size())));
            if (id > latest)
                latest = id;
        } catch (const std::exception&) {
        }
    }
    return latest;
}

// Kills every registered process on scope exit, INCLUDING the early return
// of a failed ASSERT. Without this, a red assertion leaks the spawned
// clink_node processes; a leaked child holds the test's stdout pipe open,
// so anything reading that pipe (ctest, a shell pipeline) never sees EOF
// and hangs until its own timeout - observed with a deliberately broken
// build of the compound test below. Registered as pointers so it always
// reaps the CURRENT pid after a respawn; mid-test reaps set the variable
// to -1, which kill_quietly ignores.
struct ScopedProcessKills {
    std::vector<pid_t*> pids;
    void watch(pid_t* p) { pids.push_back(p); }
    ~ScopedProcessKills() {
        for (auto* p : pids) {
            kill_quietly(*p);
            *p = -1;
        }
    }
};

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
    // Capture both coordinators' logs so worker REGISTRATION becomes observable.
    // Keyed by pid: ctest runs each test as its own process, and a fixed path would
    // have concurrent tests counting each other's registrations.
    const auto log_a = std::filesystem::temp_directory_path() /
                       ("clink_ha_coordinator_a_" + std::to_string(::getpid()) + ".log");
    const auto log_b = std::filesystem::temp_directory_path() /
                       ("clink_ha_coordinator_b_" + std::to_string(::getpid()) + ".log");

    const auto port_a = probe_free_port();
    ScopedProcessKills reaper;
    pid_t coordinator_a = clink::itest::spawn_logged({"clink_node",
                                                      "--role=coordinator",
                                                      "--port=" + std::to_string(port_a),
                                                      "--bind-host=127.0.0.1",
                                                      "--ha-dir=" + ha_dir.string()},
                                                     node,
                                                     log_a);
    reaper.watch(&coordinator_a);
    ASSERT_GT(coordinator_a, 0);
    // coordinator-A should bind within ~500ms (poll interval).
    ASSERT_TRUE(await_port_open(port_a, 2s)) << "coordinator-A never bound port " << port_a;

    // Spawn coordinator-B as standby. Its port is different; it sits on the
    // coordinator until coordinator-A dies.
    const auto port_b = probe_free_port();
    pid_t coordinator_b = clink::itest::spawn_logged({"clink_node",
                                                      "--role=coordinator",
                                                      "--port=" + std::to_string(port_b),
                                                      "--bind-host=127.0.0.1",
                                                      "--ha-dir=" + ha_dir.string()},
                                                     node,
                                                     log_b);
    reaper.watch(&coordinator_b);
    ASSERT_GT(coordinator_b, 0);
    // A standby used to have no positive signal to wait for - it logged
    // nothing until it WON leadership, which must not happen until
    // coordinator-A is killed further down - so this was a bare 500ms
    // settle for years. The engine now announces standby entry
    // ("standing by for leadership"), which is exactly the condition this
    // wait stood for: the standby process is up and watching the HA dir.
    ASSERT_TRUE(clink::itest::await_log_matches(log_b, "standing by for leadership", 1))
        << "coordinator-B never reached standby";

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
    reaper.watch(&worker);
    ASSERT_GT(worker, 0);
    // The worker REGISTERED with coordinator-A, from A's own log. Spawning the
    // worker process is a different event from the coordinator accepting its
    // registration, and submitting before the slots exist is refused outright.
    ASSERT_TRUE(clink::itest::await_log_matches(log_a, " slots=", 1))
        << "coordinator-A never registered the worker";

    // Submit a job to coordinator-A. Lasts ~1.8s; we wait for at least one
    // COMPLETED-N marker, then kill coordinator-A.
    pid_t submit_pid = spawn_proc({"clink_submit_job",
                                   "--job=" + job_so.string(),
                                   "--coordinator-host=127.0.0.1",
                                   "--coordinator-port=" + std::to_string(port_a),
                                   "--wait-timeout-s=20",
                                   "--checkpoint-dir=" + ckpt_dir.string(),
                                   "--checkpoint-interval-ms=150",
                                   "--max-restarts-on-worker-loss=0"},
                                  submit);
    reaper.watch(&submit_pid);
    ASSERT_GT(submit_pid, 0);

    // Wait for first checkpoint on job_id=1 BEFORE the crash.
    //
    // The bound was 4s, and it is not a bound on the checkpoint interval - it
    // covers submit, plan, deploy and then the checkpoint. The submit alone takes
    // about 2.7s on Linux, where a job plugin is 84 MB because it statically
    // links clink_core, so this asserted before there was a checkpoint and, by
    // returning early, skipped the process cleanup at the end of the test. ctest
    // then reported a 300-second timeout instead of the assertion failure.
    (void)clink::itest::await_condition(
        [&] { return latest_completed_checkpoint(ckpt_dir, 1) != 0; });
    const auto ckpt_before = latest_completed_checkpoint(ckpt_dir, 1);
    ASSERT_GT(ckpt_before, 0u) << "no checkpoint before failover";

    // SIGKILL coordinator-A. Submitter loses its connection and exits non-zero.
    kill_quietly(coordinator_a);
    coordinator_a = -1;
    kill_quietly(submit_pid);
    submit_pid = -1;

    // Within ~500ms (poll interval), coordinator-B's coordinator should
    // acquire the lock and bind port_b.
    ASSERT_TRUE(await_port_open(port_b, 3s)) << "coordinator-B never took over";

    // Confirm active-leader.json flips to port_b. AWAITED, not read once:
    // the flip lands on B's next refresh poll (~500ms) and, since the
    // fenced CAS write, carries an fsync - on a busy CI disk the
    // port-open-to-file-flip gap is real, and an immediate read races it.
    // Seen exactly once on the shared runner (leader_port still A's) while
    // local disks always won the race. The contract is "flips", which is
    // inherently eventual; a flip that never comes still fails here.
    std::uint16_t leader_port = 0;
    ASSERT_TRUE(clink::itest::await_condition(
        [&] { return active_leader_endpoint(ha_dir, &leader_port) && leader_port == port_b; }, 10s))
        << "active-leader.json didn't flip to coordinator-B (still " << leader_port << ", expected "
        << port_b << ")";

    // The worker should have exited (it watches the coordinator connection). Reap
    // and respawn - a supervisor would do this automatically. The new
    // worker reads active-leader.json and connects to coordinator-B.
    EXPECT_TRUE(wait_for_exit(worker, 4s)) << "worker didn't exit after coordinator-A crash";
    worker = spawn_worker();
    ASSERT_GT(worker, 0);
    // The replacement worker registered with coordinator-B, the NEW leader. Checked
    // against B's log rather than a duration: this is the point of the test, so
    // proceeding before it happened would test nothing and fail further down for an
    // unrelated-looking reason.
    ASSERT_TRUE(clink::itest::await_log_matches(log_b, " slots=", 1))
        << "coordinator-B never registered the replacement worker after failover";

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
}

// The compound loss: the coordinator and a worker die TOGETHER, with no
// window between them for the coordinator's worker-loss handling to run.
// The test above covers a coordinator-only loss and asserts checkpoint
// PROGRESS after takeover; this one asserts the claim that actually
// matters to a consumer - the standby recovers the job from the persisted
// manifest, replays from the last completed checkpoint, and the committed
// output is still each record exactly once. The coordinator is killed
// first, then the worker in the same breath, so the failure the standby
// inherits is a manifest pointing at a cluster of which half is gone.
TEST(CoordinatorHaFailover, StandbyRecoversTheJobExactlyOnceWhenCoordinatorAndWorkerDieTogether) {
    const auto node = node_binary_path();
    const auto submit = submit_binary_path();
    const auto job_so = two_phase_commit_job_path();
    if (!std::filesystem::exists(node) || !std::filesystem::exists(submit) ||
        !std::filesystem::exists(job_so)) {
        GTEST_SKIP() << "node/submit/2pc job binary not built";
    }

    constexpr int kTotal = 40;
    const auto out_dir = mktmpdir("cmp_out");
    const auto ckpt_dir = mktmpdir("cmp_ckpt");
    const auto ha_dir = mktmpdir("cmp_ha");
    ::setenv("CLINK_2PC_OUT_DIR", out_dir.c_str(), 1);
    ::setenv("CLINK_2PC_TOTAL", std::to_string(kTotal).c_str(), 1);
    ::setenv("CLINK_2PC_TICK_MS", "60", 1);

    // Log names carry a tag as well as the pid: a manual same-process
    // gtest_filter run of this suite must not count the first test's
    // registrations as this one's.
    const auto log_a = std::filesystem::temp_directory_path() /
                       ("clink_ha_cmp_coordinator_a_" + std::to_string(::getpid()) + ".log");
    const auto log_b = std::filesystem::temp_directory_path() /
                       ("clink_ha_cmp_coordinator_b_" + std::to_string(::getpid()) + ".log");

    const auto port_a = probe_free_port();
    ScopedProcessKills reaper;
    pid_t coordinator_a = clink::itest::spawn_logged({"clink_node",
                                                      "--role=coordinator",
                                                      "--port=" + std::to_string(port_a),
                                                      "--bind-host=127.0.0.1",
                                                      "--ha-dir=" + ha_dir.string()},
                                                     node,
                                                     log_a);
    reaper.watch(&coordinator_a);
    ASSERT_GT(coordinator_a, 0);
    ASSERT_TRUE(await_port_open(port_a, 2s)) << "coordinator-A never bound port " << port_a;

    const auto port_b = probe_free_port();
    pid_t coordinator_b = clink::itest::spawn_logged({"clink_node",
                                                      "--role=coordinator",
                                                      "--port=" + std::to_string(port_b),
                                                      "--bind-host=127.0.0.1",
                                                      "--ha-dir=" + ha_dir.string()},
                                                     node,
                                                     log_b);
    reaper.watch(&coordinator_b);
    ASSERT_GT(coordinator_b, 0);
    ASSERT_TRUE(clink::itest::await_log_matches(log_b, "standing by for leadership", 1))
        << "coordinator-B never reached standby";

    // Two workers, so the job's tasks straddle them and killing one takes
    // real task state down with it.
    auto spawn_worker = [&](const std::string& id) {
        return spawn_proc({"clink_node",
                           "--role=worker",
                           "--id=" + id,
                           "--slots=4",
                           "--ha-dir=" + ha_dir.string()},
                          node);
    };
    pid_t worker0 = spawn_worker("worker-hac-0");
    pid_t worker1 = spawn_worker("worker-hac-1");
    reaper.watch(&worker0);
    reaper.watch(&worker1);
    ASSERT_GT(worker0, 0);
    ASSERT_GT(worker1, 0);
    ASSERT_TRUE(clink::itest::await_log_matches(log_a, " slots=", 2))
        << "coordinator-A never registered both workers";

    pid_t submit_pid = spawn_proc({"clink_submit_job",
                                   "--job=" + job_so.string(),
                                   "--coordinator-host=127.0.0.1",
                                   "--coordinator-port=" + std::to_string(port_a),
                                   "--wait-timeout-s=20",
                                   "--checkpoint-dir=" + ckpt_dir.string(),
                                   "--checkpoint-interval-ms=150",
                                   "--max-restarts-on-worker-loss=2"},
                                  submit);
    reaper.watch(&submit_pid);
    ASSERT_GT(submit_pid, 0);

    // At least one completed checkpoint AND at least one committed record
    // before the compound kill, so the standby has something real to restore
    // from and the run is provably mid-flight.
    (void)clink::itest::await_condition([&] {
        return latest_completed_checkpoint(ckpt_dir, 1) != 0 &&
               !clink::itest::committed_records(out_dir).empty();
    });
    ASSERT_GT(latest_completed_checkpoint(ckpt_dir, 1), 0u) << "no checkpoint before the kill";
    // Vacuity guards: killing after some output is committed but before all
    // of it proves the standby RESTORED rather than re-ran from scratch - a
    // from-scratch re-run would double-commit the pre-kill records and fail
    // the exactly-once verdict below, and a kill after completion would test
    // nothing at all.
    const auto at_kill = clink::itest::verify_exactly_once(out_dir, kTotal);
    ASSERT_GT(at_kill.total_lines, 0u) << "nothing committed before the kill; the recovery "
                                          "claim below would be untestable";
    ASSERT_FALSE(at_kill.missing.empty())
        << "the job already finished before the kill; the compound-loss path never ran";

    // The compound kill: coordinator first so its worker-loss handling never
    // runs, then worker-0 immediately - no reaping in between.
    ASSERT_EQ(::kill(coordinator_a, SIGKILL), 0);
    ASSERT_EQ(::kill(worker0, SIGKILL), 0);
    kill_quietly(coordinator_a);
    coordinator_a = -1;
    kill_quietly(worker0);
    worker0 = -1;
    kill_quietly(submit_pid);  // dies with coordinator-A; reap it either way
    submit_pid = -1;

    // Standby takes over.
    ASSERT_TRUE(await_port_open(port_b, 3s)) << "coordinator-B never took over";
    std::uint16_t leader_port = 0;
    EXPECT_TRUE(active_leader_endpoint(ha_dir, &leader_port));
    EXPECT_EQ(leader_port, port_b) << "active-leader.json didn't flip to coordinator-B";

    // The surviving worker exits on its own when its coordinator connection
    // drops; reap it, then respawn both (the supervisor's job in production).
    EXPECT_TRUE(wait_for_exit(worker1, 4s)) << "worker-1 didn't exit after coordinator-A crash";
    kill_quietly(worker1);
    worker1 = -1;
    worker0 = spawn_worker("worker-hac-0");
    worker1 = spawn_worker("worker-hac-1");
    ASSERT_GT(worker0, 0);
    ASSERT_GT(worker1, 0);
    ASSERT_TRUE(clink::itest::await_log_matches(log_b, " slots=", 2))
        << "coordinator-B never registered the replacement workers";

    // The standby must have RESTORED the job, not re-run it from scratch.
    // The output check below cannot see the difference on its own: the sink
    // names committed files sub<N>-<checkpoint_id>.dat, and a from-scratch
    // re-run regenerates the same checkpoint ids, so its commits atomically
    // overwrite the pre-kill files with re-emitted records and the
    // after-the-fact read converges to each-record-once. An external
    // consumer would still have seen published files replaced under it, so
    // pin the cause here: the recovery line must carry a non-zero restore
    // checkpoint. Checked after the replacement workers registered, because
    // the recovery submit needs slots before it can run and log.
    ASSERT_TRUE(clink::itest::await_log_matches(log_b, "recovered job_id=", 1))
        << "coordinator-B never recovered the persisted job";
    {
        std::ifstream in(log_b);
        const std::string body((std::istreambuf_iterator<char>(in)),
                               std::istreambuf_iterator<char>());
        ASSERT_EQ(body.find("restore_from_ckpt=0"), std::string::npos)
            << "the standby re-submitted the job from scratch instead of restoring from the "
               "last completed checkpoint";
    }

    // The recovered job must run to COMPLETION under coordinator-B - waiting
    // on the completion log line rather than polling the output directory, so
    // the exactly-once verdict below reads a finished run, not a snapshot a
    // late replayed commit could still falsify.
    ASSERT_TRUE(clink::itest::await_log_matches(log_b, "coordinator.complete", 1))
        << "the recovered job never completed under coordinator-B";

    const auto v = clink::itest::verify_exactly_once(out_dir, kTotal);
    EXPECT_TRUE(v.clean()) << "a coordinator+worker compound loss broke exactly-once: "
                           << clink::itest::describe(v);
}
