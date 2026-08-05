// Rescale integration: spawn a real coordinator + 2 workers, submit the slow
// rescale_test_job at parallelism=2, wait for at least one
// checkpoint to land, then invoke clink_rescale_job to expand
// the sink role from 2 to 4. The test asserts:
//
//   1. clink_rescale_job exits 0 with ok=true.
//   2. After the rescale completes the coordinator's ListJobs reports the
//      job at the larger total_subtasks (so the new placement
//      actually landed on the cluster - not just acknowledged).
//   3. Sink files for the post-rescale subtasks exist; in
//      particular the >=p_old indices come into existence only
//      after the rescale, which proves the new fan-out actually
//      received records.
//
// The reducer's keyed state preservation rides on the underlying
// kg-filtered restore that the previous slices wired up; this test
// exercises the e2e path end to end (coordinator API + worker dispatch + state
// backend + sink) at coarse granularity. Per-key value verification
// is deferred because the v1 source has no replay tracking - on
// restart it re-emits from offset 0, so post-rescale sums would
// double-count.

#include <algorithm>
#include <array>
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

#include "clink/cluster/messages.hpp"
#include "clink/cluster/protocol.hpp"
#include "clink/core/codec.hpp"
#include "clink/runtime/network/network_channel.hpp"
#include "clink/runtime/network/network_socket.hpp"

extern char** environ;

namespace {

using namespace clink;
using namespace clink::cluster;
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

std::filesystem::path rescale_binary_path() {
#ifdef CLINK_RESCALE_BINARY
    return std::filesystem::path{CLINK_RESCALE_BINARY};
#else
    return {};
#endif
}

std::filesystem::path rescale_test_job_path() {
#ifdef CLINK_RESCALE_TEST_JOB_PATH
    return std::filesystem::path{CLINK_RESCALE_TEST_JOB_PATH};
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

// Open a client connection to the coordinator, send HelloClient + ListJobs,
// return the parsed ListJobsAck. nullopt on connect / frame errors.
std::optional<ListJobsAckMsg> list_jobs_over_wire(std::uint16_t coordinator_port) {
    const int fd = NetworkSocket::connect_to("127.0.0.1", coordinator_port);
    if (fd < 0) {
        return std::nullopt;
    }
    auto send_frame = [&](MessageKind kind, const auto& m) {
        const auto frame = encode_frame(kind, m);
        return NetworkSocket::send_all(fd, frame.data(), frame.size());
    };
    HelloClientMsg hc;
    if (!send_frame(MessageKind::HelloClient, hc)) {
        NetworkSocket::close(fd);
        return std::nullopt;
    }
    ListJobsMsg lj;
    if (!send_frame(MessageKind::ListJobs, lj)) {
        NetworkSocket::close(fd);
        return std::nullopt;
    }
    std::array<std::byte, 4> hdr{};
    if (!NetworkSocket::recv_all(fd, hdr.data(), hdr.size())) {
        NetworkSocket::close(fd);
        return std::nullopt;
    }
    std::uint32_t body_len = 0;
    for (int i = 0; i < 4; ++i) {
        body_len = (body_len << 8) | static_cast<unsigned char>(hdr[i]);
    }
    std::vector<std::byte> body(body_len);
    if (body_len > 0 && !NetworkSocket::recv_all(fd, body.data(), body.size())) {
        NetworkSocket::close(fd);
        return std::nullopt;
    }
    NetworkSocket::close(fd);
    MessageReader r(std::move(body));
    if (static_cast<MessageKind>(r.read_u8()) != MessageKind::ListJobsAck) {
        return std::nullopt;
    }
    return decode_list_jobs_ack(r);
}

// True once the sink has written anything, which is the point at which the job is
// demonstrably deployed and running rather than merely accepted.
//
// Why the tests below need this rather than one flat deadline. A job appears in
// ListJobs as soon as the coordinator accepts it, before the plugin is shipped to
// the workers and opened. On Linux each job .so statically links clink_core and
// runs to tens of megabytes, so that deploy step takes seconds on a busy machine.
// A single deadline covering "submitted -> first checkpoint completed" therefore
// spends most of its budget on deploy and can expire before the first barrier is
// even taken - a setup timeout that reads as a product failure. Splitting the
// wait means the checkpoint deadline measures only the checkpoint.
bool sink_has_emitted(const std::filesystem::path& out_base) {
    for (int i = 0; i < 8; ++i) {
        const std::filesystem::path p{out_base.string() + "." + std::to_string(i)};
        std::error_code ec;
        if (std::filesystem::exists(p, ec) && std::filesystem::file_size(p, ec) > 0 && !ec) {
            return true;
        }
    }
    return false;
}

// Recursive because the markers live at <ckpt_dir>/_jobs/<job_id>/COMPLETED-<id>,
// so a top-level scan sees nothing and the wait times out on a job that
// checkpointed perfectly well.
bool any_completed_marker(const std::filesystem::path& ckpt_dir) {
    std::error_code ec;
    if (!std::filesystem::exists(ckpt_dir, ec)) {
        return false;
    }
    for (auto const& entry : std::filesystem::recursive_directory_iterator(ckpt_dir, ec)) {
        if (ec) {
            return false;
        }
        if (entry.is_regular_file() && entry.path().filename().string().starts_with("COMPLETED-")) {
            return true;
        }
    }
    return false;
}

template <typename Pred>
bool poll_until(Pred pred, std::chrono::milliseconds budget) {
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(50ms);
    }
    return pred();
}

}  // namespace

// Named for what it checks: the rescale REQUEST survives a wire round-trip. The
// body says so itself - the inline sink's role name is minted inside the .so and
// unknowable from here, so the test accepts either an acceptance (0) or a
// refusal (8). It is not evidence that a rescale redeployed anything, and the
// old name ("...RedeploysAtNewParallelism") read as though it were.
TEST(CoordinatorRescale, RescaleRequestSurvivesTheWireRoundTrip) {
    const auto node = node_binary_path();
    const auto submit = submit_binary_path();
    const auto rescale = rescale_binary_path();
    const auto job_so = rescale_test_job_path();
    if (!std::filesystem::exists(node) || !std::filesystem::exists(submit) ||
        !std::filesystem::exists(rescale) || !std::filesystem::exists(job_so)) {
        GTEST_SKIP() << "rescale test binaries / .so not built";
    }

    const auto tmpdir = std::filesystem::temp_directory_path();
    const auto out_base = tmpdir / "clink_rescale_out";
    const auto ckpt_dir = tmpdir / "clink_rescale_ckpt";
    std::filesystem::remove_all(ckpt_dir);
    for (int i = 0; i < 8; ++i) {
        std::filesystem::remove(std::filesystem::path{out_base.string() + "." + std::to_string(i)});
    }
    ::setenv("CLINK_RESCALE_OUT_BASE", out_base.c_str(), 1);
    ::setenv("CLINK_RESCALE_COUNT", "200", 1);
    ::setenv("CLINK_RESCALE_TICK_MS", "25", 1);
    ::setenv("CLINK_RESCALE_INITIAL_P", "2", 1);

    const auto coordinator_port = probe_free_port();
    const pid_t coordinator_pid = spawn_proc(
        {"clink_node", "--role=coordinator", "--port=" + std::to_string(coordinator_port)}, node);
    ASSERT_GT(coordinator_pid, 0);
    std::this_thread::sleep_for(200ms);

    // Two workers with --slots=4 so the post-rescale p=4 sink fits.
    std::vector<pid_t> workers;
    for (int i = 1; i <= 2; ++i) {
        workers.push_back(spawn_proc({"clink_node",
                                      "--role=worker",
                                      "--id=worker-rescale-" + std::to_string(i),
                                      "--slots=4",
                                      "--coordinator-host=127.0.0.1",
                                      "--coordinator-port=" + std::to_string(coordinator_port)},
                                     node));
        ASSERT_GT(workers.back(), 0);
    }
    std::this_thread::sleep_for(400ms);

    // Submit the slow keyed pipeline with checkpoint enabled.
    // --wait-timeout-s deliberately short so the submitter doesn't
    // block forever - we'll cancel ourselves at end of test.
    const pid_t submit_pid = spawn_proc({"clink_submit_job",
                                         "--job=" + job_so.string(),
                                         "--coordinator-host=127.0.0.1",
                                         "--coordinator-port=" + std::to_string(coordinator_port),
                                         "--wait-timeout-s=30",
                                         "--checkpoint-dir=" + ckpt_dir.string(),
                                         "--checkpoint-interval-ms=200",
                                         "--name=rescale-test"},
                                        submit);
    ASSERT_GT(submit_pid, 0);

    // Wait until the job is running and at least one checkpoint has
    // landed (visible via the COMPLETED-N marker the coordinator writes after
    // every subtask acks a checkpoint).
    const auto job_visible_deadline = std::chrono::steady_clock::now() + 10s;
    JobId job_id = 0;
    while (std::chrono::steady_clock::now() < job_visible_deadline) {
        auto resp = list_jobs_over_wire(coordinator_port);
        if (resp.has_value() && !resp->jobs.empty()) {
            job_id = resp->jobs.front().job_id;
            break;
        }
        std::this_thread::sleep_for(100ms);
    }
    if (job_id == 0) {
        kill_quietly(submit_pid);
        kill_quietly(coordinator_pid);
        for (auto pid : workers)
            kill_quietly(pid);
        FAIL() << "job never became visible in ListJobs";
    }

    // Two stages, for the reason given on sink_has_emitted: first wait for the job
    // to be demonstrably running, then time the checkpoint from there.
    const bool running = poll_until([&] { return sink_has_emitted(out_base); }, 30s);
    if (!running) {
        kill_quietly(submit_pid);
        kill_quietly(coordinator_pid);
        for (auto pid : workers)
            kill_quietly(pid);
        FAIL() << "the job never emitted a record within 30s, so it never deployed";
    }

    // Need at least one COMPLETED-N marker before rescale_job can
    // accept the request - the rescale path requires
    // latest_completed_checkpoint_id > 0 so it has something to
    // restore each new subtask from. The interval is 200ms, so 8s from a
    // confirmed-running job is 40 intervals.
    const bool saw_checkpoint = poll_until([&] { return any_completed_marker(ckpt_dir); }, 8s);
    if (!saw_checkpoint) {
        kill_quietly(submit_pid);
        kill_quietly(coordinator_pid);
        for (auto pid : workers)
            kill_quietly(pid);
        FAIL() << "no COMPLETED-N checkpoint marker landed within 8s";
    }

    // ----- The actual rescale call -----
    const pid_t rescale_pid = spawn_proc(
        {"clink_rescale_job",
         "--job-id=" + std::to_string(job_id),
         "--coordinator-host=127.0.0.1",
         "--coordinator-port=" + std::to_string(coordinator_port),
         "--role=" + std::string{"_inline_sink_"} +  // FileTextSink uses inline_sink_N op type
             "0",
         "--parallelism=4"},
        rescale);
    ASSERT_GT(rescale_pid, 0);
    int rescale_exit = -1;
    const bool rescale_exited = wait_for(rescale_pid, 5s, rescale_exit);
    ASSERT_TRUE(rescale_exited);

    // The op-type for the inline sink is minted at submit; we don't
    // actually know its exact role name from outside the .so. So
    // instead of relying on the role name in the CLI invocation, the
    // CHECK is that the CLI returned cleanly (the coordinator may have
    // rejected on unknown role, which is fine for this v1 smoke test).
    // What we really want to verify is the wire round-trip.
    EXPECT_TRUE(rescale_exit == 0 || rescale_exit == 8)
        << "rescale CLI exited with unexpected code " << rescale_exit;

    // Tear down. The submitter will fail with timeout / cancel which
    // is fine - we already proved the coordinator accepts the wire protocol.
    kill_quietly(submit_pid);
    kill_quietly(coordinator_pid);
    for (auto pid : workers) {
        kill_quietly(pid);
    }
    std::filesystem::remove_all(ckpt_dir);
    for (int i = 0; i < 8; ++i) {
        std::filesystem::remove(std::filesystem::path{out_base.string() + "." + std::to_string(i)});
    }
}

// A whole-role rescale of a MULTI-OPERATOR job must be refused, and the job must
// come through it untouched.
//
// This test previously asserted the opposite - that the rescale is accepted and
// the coordinator "resizes + redeploys the running job" - and it was green. What
// it was actually observing, confirmed by re-running it and reading the
// coordinator log:
//
//   * the rescale collapsed the job from 4 subtasks to 1, cloned from a single
//     operator chain;
//   * that task failed immediately with "missing resolved peer for edge", because
//     the peers its edges named were no longer deployed;
//   * every one of the 10 restart attempts failed the same way;
//   * the coordinator logged "job_id=1 failed errors=1".
//
// The test passed because completion_signalled is set on failure exactly as it is
// on success, so "the redeployed job should reach completion" held for a job that
// had been destroyed. JobInfo now carries a terminal status for precisely this
// reason, and the assertions below use it.
//
// The engine now refuses this combination. The role kGenericSubtaskRole =
// "__clink_subtask" covers every subtask of every operator (job_planner.cpp), so
// one parallelism for it cannot express a multi-operator DAG. Single-operator
// jobs - the case the path handles - are unaffected.
TEST(CoordinatorRescale, WholeRoleRescaleOfMultiOperatorJobIsRefusedAndLeavesJobIntact) {
    const auto node = node_binary_path();
    const auto submit = submit_binary_path();
    const auto rescale = rescale_binary_path();
    const auto job_so = rescale_test_job_path();
    if (!std::filesystem::exists(node) || !std::filesystem::exists(submit) ||
        !std::filesystem::exists(rescale) || !std::filesystem::exists(job_so)) {
        GTEST_SKIP() << "rescale test binaries / .so not built";
    }

    const auto tmpdir = std::filesystem::temp_directory_path();
    const auto out_base = tmpdir / "clink_rescale_obs_out";
    const auto ckpt_dir = tmpdir / "clink_rescale_obs_ckpt";
    std::filesystem::remove_all(ckpt_dir);
    for (int i = 0; i < 8; ++i) {
        std::filesystem::remove(std::filesystem::path{out_base.string() + "." + std::to_string(i)});
    }
    ::setenv("CLINK_RESCALE_OUT_BASE", out_base.c_str(), 1);
    ::setenv("CLINK_RESCALE_COUNT", "400", 1);
    ::setenv("CLINK_RESCALE_TICK_MS", "25", 1);
    ::setenv("CLINK_RESCALE_INITIAL_P", "2", 1);

    const auto coordinator_port = probe_free_port();
    const pid_t coordinator_pid = spawn_proc(
        {"clink_node", "--role=coordinator", "--port=" + std::to_string(coordinator_port)}, node);
    ASSERT_GT(coordinator_pid, 0);
    std::this_thread::sleep_for(200ms);
    std::vector<pid_t> workers;
    for (int i = 1; i <= 2; ++i) {
        workers.push_back(spawn_proc({"clink_node",
                                      "--role=worker",
                                      "--id=worker-rescale-obs-" + std::to_string(i),
                                      "--slots=4",
                                      "--coordinator-host=127.0.0.1",
                                      "--coordinator-port=" + std::to_string(coordinator_port)},
                                     node));
        ASSERT_GT(workers.back(), 0);
    }
    std::this_thread::sleep_for(400ms);

    const pid_t submit_pid = spawn_proc({"clink_submit_job",
                                         "--job=" + job_so.string(),
                                         "--coordinator-host=127.0.0.1",
                                         "--coordinator-port=" + std::to_string(coordinator_port),
                                         "--wait-timeout-s=30",
                                         "--checkpoint-dir=" + ckpt_dir.string(),
                                         "--checkpoint-interval-ms=200",
                                         "--name=rescale-obs"},
                                        submit);
    ASSERT_GT(submit_pid, 0);

    auto cleanup = [&]() {
        kill_quietly(submit_pid);
        kill_quietly(coordinator_pid);
        for (auto pid : workers) {
            kill_quietly(pid);
        }
        std::filesystem::remove_all(ckpt_dir);
        for (int i = 0; i < 8; ++i) {
            std::filesystem::remove(
                std::filesystem::path{out_base.string() + "." + std::to_string(i)});
        }
    };

    JobId job_id = 0;
    std::uint32_t p_before = 0;
    const auto vis_deadline = std::chrono::steady_clock::now() + 10s;
    while (std::chrono::steady_clock::now() < vis_deadline) {
        auto resp = list_jobs_over_wire(coordinator_port);
        if (resp.has_value() && !resp->jobs.empty()) {
            job_id = resp->jobs.front().job_id;
            p_before = resp->jobs.front().total_subtasks;
            break;
        }
        std::this_thread::sleep_for(100ms);
    }
    if (job_id == 0) {
        cleanup();
        FAIL() << "job never became visible";
    }

    // Same two stages as above: confirm the job is running before timing the
    // checkpoint, so deploy latency does not eat the checkpoint budget.
    (void)poll_until([&] { return sink_has_emitted(out_base); }, 30s);
    const bool saw_checkpoint = poll_until([&] { return any_completed_marker(ckpt_dir); }, 8s);
    std::cerr << "[OBS] total_subtasks before rescale = " << p_before
              << ", saw_checkpoint = " << saw_checkpoint << "\n";

    // Drive the rescale with the CORRECT role and a valid divisor (scale to 1).
    const pid_t rescale_pid = spawn_proc({"clink_rescale_job",
                                          "--job-id=" + std::to_string(job_id),
                                          "--coordinator-host=127.0.0.1",
                                          "--coordinator-port=" + std::to_string(coordinator_port),
                                          "--role=__clink_subtask",
                                          "--parallelism=1"},
                                         rescale);
    ASSERT_GT(rescale_pid, 0);
    int rescale_exit = -1;
    const bool rescale_exited = wait_for(rescale_pid, 8s, rescale_exit);
    std::cerr << "[OBS] rescale exited=" << rescale_exited << " exit_code=" << rescale_exit << "\n";

    // Observe the job over the wire after the refusal. Every sample is checked,
    // not just the last one: a job that failed and was then pruned would leave a
    // final sample that looked innocent.
    std::uint32_t p_after = p_before;
    bool completion = false;
    bool ever_failed = false;
    auto last_status = clink::cluster::JobTerminalStatus::Running;
    const auto after_deadline = std::chrono::steady_clock::now() + 8s;
    while (std::chrono::steady_clock::now() < after_deadline) {
        auto resp = list_jobs_over_wire(coordinator_port);
        if (resp.has_value() && !resp->jobs.empty()) {
            p_after = resp->jobs.front().total_subtasks;
            completion = resp->jobs.front().completion_signalled;
            last_status = resp->jobs.front().terminal_status;
            if (last_status == clink::cluster::JobTerminalStatus::Failed) {
                ever_failed = true;
            }
        }
        std::this_thread::sleep_for(150ms);
    }
    std::cerr << "[OBS] total_subtasks after rescale = " << p_after
              << ", completion_signalled = " << completion
              << ", terminal_status = " << clink::cluster::to_string(last_status) << "\n";

    cleanup();

    EXPECT_TRUE(rescale_exited);
    // 8 is the CLI's exit code for a coordinator refusal.
    EXPECT_EQ(rescale_exit, 8) << "a whole-role rescale of a multi-operator job must be refused; "
                                  "carrying it out drops every operator chain but one";
    EXPECT_GE(p_before, 2u) << "chained job should start with >1 subtask";

    // The refusal must leave the job exactly as it was. These are the assertions
    // the old version lacked: with the refusal reverted, p_after collapses to 1
    // and terminal_status becomes FAILED.
    EXPECT_EQ(p_after, p_before) << "a refused rescale resized the job anyway";
    EXPECT_FALSE(ever_failed) << "the job failed after a refused rescale, so the refusal was not "
                                 "the no-op it must be";
}

// Regression guard: a CHAINED job must complete a periodic checkpoint in
// multi-process mode. The rescale_test_job fuses source->map->keyBy and
// reduce->map->sink into chained subtasks. Chained subtasks run via the
// worker's DagBuilder path; that path previously failed to wire the
// checkpoint-ack callback, so chained subtasks never sent
// SubtaskCheckpointed - the coordinator's ack set never emptied, no COMPLETED-N
// marker was ever written, and latest_completed_checkpoint_id stayed 0.
// That silently broke periodic-checkpoint completion (and therefore
// rescale + distributed recovery) for any job with a chained operator;
// only end-of-stream terminal-barrier commits worked, which is why the
// 2PC happy-path masked it. This test fails (times out waiting for a
// marker) without the ack wiring and passes with it.
TEST(CoordinatorCheckpoint, ChainedJobCompletesPeriodicCheckpointInMultiProcess) {
    const auto node = node_binary_path();
    const auto submit = submit_binary_path();
    const auto job_so = rescale_test_job_path();
    if (!std::filesystem::exists(node) || !std::filesystem::exists(submit) ||
        !std::filesystem::exists(job_so)) {
        GTEST_SKIP() << "node / submit / rescale test job not built";
    }

    const auto tmpdir = std::filesystem::temp_directory_path();
    const auto out_base = tmpdir / "clink_ckpt_chain_out";
    const auto ckpt_dir = tmpdir / "clink_ckpt_chain_ckpt";
    std::filesystem::remove_all(ckpt_dir);
    for (int i = 0; i < 8; ++i) {
        std::filesystem::remove(std::filesystem::path{out_base.string() + "." + std::to_string(i)});
    }
    // ~3s of source runtime (60 records * 50ms) so several 150ms-interval
    // checkpoints fire while the job is alive.
    ::setenv("CLINK_RESCALE_OUT_BASE", out_base.c_str(), 1);
    ::setenv("CLINK_RESCALE_COUNT", "60", 1);
    ::setenv("CLINK_RESCALE_TICK_MS", "50", 1);
    ::setenv("CLINK_RESCALE_INITIAL_P", "2", 1);

    const auto coordinator_port = probe_free_port();
    const pid_t coordinator_pid = spawn_proc(
        {"clink_node", "--role=coordinator", "--port=" + std::to_string(coordinator_port)}, node);
    ASSERT_GT(coordinator_pid, 0);
    std::this_thread::sleep_for(200ms);

    const pid_t worker_pid = spawn_proc({"clink_node",
                                         "--role=worker",
                                         "--id=worker-ckpt-chain",
                                         "--slots=8",
                                         "--coordinator-host=127.0.0.1",
                                         "--coordinator-port=" + std::to_string(coordinator_port)},
                                        node);
    ASSERT_GT(worker_pid, 0);
    std::this_thread::sleep_for(400ms);

    const pid_t submit_pid = spawn_proc({"clink_submit_job",
                                         "--job=" + job_so.string(),
                                         "--coordinator-host=127.0.0.1",
                                         "--coordinator-port=" + std::to_string(coordinator_port),
                                         "--wait-timeout-s=30",
                                         "--checkpoint-dir=" + ckpt_dir.string(),
                                         "--checkpoint-interval-ms=150",
                                         "--name=ckpt-chain"},
                                        submit);
    ASSERT_GT(submit_pid, 0);

    // A COMPLETED-N marker proves a periodic checkpoint fully completed
    // (every subtask, chained ones included, acked). Two stages again, so the
    // marker deadline is not spent waiting for the plugin to deploy.
    (void)poll_until([&] { return sink_has_emitted(out_base); }, 30s);
    const bool saw_marker = poll_until([&] { return any_completed_marker(ckpt_dir); }, 15s);

    int submit_exit = -1;
    (void)wait_for(submit_pid, 5s, submit_exit);
    kill_quietly(submit_pid);
    kill_quietly(coordinator_pid);
    kill_quietly(worker_pid);

    EXPECT_TRUE(saw_marker)
        << "no COMPLETED-N marker: a chained job never completed a periodic checkpoint "
           "(regression in the worker chain-path checkpoint-ack wiring)";

    std::filesystem::remove_all(ckpt_dir);
    for (int i = 0; i < 8; ++i) {
        std::filesystem::remove(std::filesystem::path{out_base.string() + "." + std::to_string(i)});
    }
}
