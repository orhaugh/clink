// 2PC sink end-to-end tests against a real cluster.
//
// Test 1 (HappyPath): submit a bounded job (30 records, 50ms tick)
//   piped to file_2pc_sink_string with checkpointing on. Wait for the
//   job to finish. Every record must appear in <out>/committed/ exactly
//   once; <out>/staging/ must be empty.
//
// Test 2 (CrashMidstreamThenRestore): submit the same job, but cut the
//   cluster (SIGKILL JM+TM) ~600ms in, before the bounded source
//   finishes. Inspect <out>/staging/ for leftover pre-committed files
//   whose checkpoint ids are present in the latest COMPLETED-N marker.
//   Restart the cluster from scratch, re-submit with restore_from
//   pointing at the same checkpoint dir; assert: every record from the
//   original run that made it to staging/ (matched by the restored
//   state) now lives in committed/. This proves the sink's
//   recover_pending_() actually fires. Test 2 also asserts PIPELINE-WIDE
//   exactly-once: the source is replay-capable (#54 - it checkpoints its
//   offset via the operator-state seam), so on restart it resumes from the
//   last checkpoint instead of replaying from 0, and no record is committed
//   twice. (Before the source replay seam this was a documented gap; the
//   no-duplicates assertion below is the end-to-end regression guard for it.)

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fcntl.h>
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

// Like spawn_proc, but redirects the child's stdout AND stderr to `log_path`.
// A spawned JM/TM/submitter otherwise INHERITS the harness's stdout. Under a
// captured-pipe harness (ctest) that is a real hazard: a long-lived child holds
// the pipe's write end open, so the reader never sees EOF and the test's own
// process is reported as a hang/timeout even after it finishes - and a child
// blocked writing to a momentarily-full pipe can stall the job itself. Writing
// each child's output to its own file removes both hazards and keeps the logs
// available for diagnosing a failure. (The bench harness does the same thing;
// this is a local copy so the 2PC ITs need no shared header.)
pid_t spawn_proc_logged(const std::vector<std::string>& argv,
                        const std::filesystem::path& binary,
                        const std::filesystem::path& log_path) {
    std::vector<char*> raw;
    raw.reserve(argv.size() + 1);
    for (const auto& s : argv)
        raw.push_back(const_cast<char*>(s.c_str()));
    raw.push_back(nullptr);
    posix_spawn_file_actions_t fa;
    posix_spawn_file_actions_init(&fa);
    posix_spawn_file_actions_addopen(
        &fa, STDOUT_FILENO, log_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    posix_spawn_file_actions_adddup2(&fa, STDOUT_FILENO, STDERR_FILENO);
    pid_t pid = -1;
    const auto rc = posix_spawn(&pid, binary.c_str(), &fa, nullptr, raw.data(), environ);
    posix_spawn_file_actions_destroy(&fa);
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
               ("clink_2pc_int_" + tag + "_" + std::to_string(::getpid()) + "_" +
                std::to_string(++counter));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

std::vector<std::string> read_all_committed_lines(const std::filesystem::path& out_dir) {
    std::vector<std::string> lines;
    const auto committed = out_dir / "committed";
    if (!std::filesystem::exists(committed))
        return lines;
    for (const auto& e : std::filesystem::directory_iterator(committed)) {
        if (!e.is_regular_file())
            continue;
        std::ifstream in(e.path());
        std::string l;
        while (std::getline(in, l))
            lines.push_back(std::move(l));
    }
    return lines;
}

std::vector<std::filesystem::path> list_staging_files(const std::filesystem::path& out_dir) {
    std::vector<std::filesystem::path> files;
    const auto staging = out_dir / "staging";
    if (!std::filesystem::exists(staging))
        return files;
    for (const auto& e : std::filesystem::directory_iterator(staging)) {
        if (e.is_regular_file())
            files.push_back(e.path());
    }
    return files;
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

struct Cluster {
    pid_t jm_pid{-1};
    std::uint16_t jm_port{0};
    pid_t tm_pid{-1};
    std::string tm_id;

    Cluster() = default;
    Cluster(const Cluster&) = delete;
    Cluster& operator=(const Cluster&) = delete;
    Cluster(Cluster&& o) noexcept
        : jm_pid(o.jm_pid), jm_port(o.jm_port), tm_pid(o.tm_pid), tm_id(std::move(o.tm_id)) {
        o.jm_pid = -1;
        o.tm_pid = -1;
    }
    Cluster& operator=(Cluster&& o) noexcept {
        if (this != &o) {
            this->~Cluster();
            new (this) Cluster(std::move(o));
        }
        return *this;
    }
    ~Cluster() {
        kill_quietly(tm_pid);
        kill_quietly(jm_pid);
    }
};

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

std::optional<Cluster> start_cluster() {
    Cluster c;
    const auto node = node_binary_path();
    if (!std::filesystem::exists(node))
        return std::nullopt;
    c.jm_port = probe_free_port();
    c.jm_pid = spawn_proc(
        {"clink_node", "--role=jm", "--port=" + std::to_string(c.jm_port), "--bind-host=127.0.0.1"},
        node);
    if (c.jm_pid <= 0 || !await_port_open(c.jm_port, 2s))
        return std::nullopt;

    c.tm_id = "tm-2pc-1";
    c.tm_pid = spawn_proc({"clink_node",
                           "--role=tm",
                           "--id=" + c.tm_id,
                           "--jm-host=127.0.0.1",
                           "--jm-port=" + std::to_string(c.jm_port),
                           "--slots=4"},
                          node);
    if (c.tm_pid <= 0)
        return std::nullopt;
    std::this_thread::sleep_for(400ms);  // TM register settle
    return c;
}

}  // namespace

TEST(TwoPhaseCommit, HappyPathExactlyOnceCommittedFiles) {
    const auto submit = submit_binary_path();
    const auto job_so = two_phase_commit_job_path();
    if (!std::filesystem::exists(submit) || !std::filesystem::exists(job_so)) {
        GTEST_SKIP() << "submitter or two_phase_commit_job.so not built";
    }
    auto c = start_cluster();
    if (!c.has_value()) {
        GTEST_SKIP() << "cluster startup failed";
    }
    const auto out_dir = mktmpdir("happy_out");
    const auto ckpt_dir = mktmpdir("happy_ckpt");
    ::setenv("CLINK_2PC_OUT_DIR", out_dir.c_str(), 1);
    ::setenv("CLINK_2PC_TOTAL", "30", 1);
    ::setenv("CLINK_2PC_TICK_MS", "20", 1);

    // Checkpoint every 150ms; over the ~600ms the source takes, several
    // checkpoints fire. Each barrier pre-commits the in-flight pending
    // file; each completed checkpoint commits it. End-of-stream flushes
    // any remaining pending data to staging without committing
    // (no terminal barrier yet).
    const pid_t submit_pid = spawn_proc({"clink_submit_job",
                                         "--job=" + job_so.string(),
                                         "--jm-host=127.0.0.1",
                                         "--jm-port=" + std::to_string(c->jm_port),
                                         "--wait-timeout-s=15",
                                         "--checkpoint-dir=" + ckpt_dir.string(),
                                         "--checkpoint-interval-ms=150"},
                                        submit);
    ASSERT_GT(submit_pid, 0);
    int submit_exit = -1;
    ASSERT_TRUE(wait_for_exit(submit_pid, 12s, &submit_exit))
        << "submitter did not exit within 12s";
    EXPECT_EQ(submit_exit, 0) << "submitter exited non-zero";

    // At least one checkpoint must have completed during the run.
    EXPECT_GT(latest_completed_checkpoint(ckpt_dir), 0u)
        << "no COMPLETED-N marker found in " << ckpt_dir;

    // With terminal-barrier emission wired into the source runner, the
    // tail past the last periodic checkpoint now reaches committed/ via
    // a local commit. All 30 records must be present, deduped.
    auto lines = read_all_committed_lines(out_dir);
    std::set<std::string> uniq(lines.begin(), lines.end());
    EXPECT_EQ(lines.size(), uniq.size()) << "duplicate records in committed/";
    EXPECT_EQ(lines.size(), 30u) << "expected all 30 records committed; got " << lines.size();
    std::set<std::string> seen(lines.begin(), lines.end());
    for (int i = 0; i < 30; ++i) {
        EXPECT_TRUE(seen.count("record-" + std::to_string(i)) > 0) << "missing record-" << i;
    }
    EXPECT_TRUE(list_staging_files(out_dir).empty())
        << "after clean completion, staging/ should be empty";
}

TEST(TwoPhaseCommit, RecoveryCommitsPreCommittedFilesOnRestart) {
    // Crash + restart proof. Run 1: submit the bounded job with
    // periodic checkpointing on. SIGKILL the JM mid-stream (before the
    // source finishes). Run 2: spawn a fresh cluster, re-submit the
    // same job with restore_from pointing at run 1's checkpoint dir.
    // The sink's recover_pending_() should commit any leftover staging
    // files whose ids are in the restored state.
    //
    // Caveat: the bounded source has no checkpoint awareness - on
    // restart it replays records 0..29 from the start. So we don't
    // get true pipeline-wide exactly-once. We DO get exactly-once
    // semantics from the SINK: every checkpoint that completed in run
    // 1 has its records in committed/, and run 2's commits land in
    // separate committed/ files (different filenames if the JM
    // allocates new checkpoint ids; same filenames otherwise - content
    // is identical due to source determinism so a rename-overwrite is
    // semantically a no-op).
    const auto submit = submit_binary_path();
    const auto job_so = two_phase_commit_job_path();
    if (!std::filesystem::exists(submit) || !std::filesystem::exists(job_so)) {
        GTEST_SKIP() << "submitter or two_phase_commit_job.so not built";
    }

    const auto out_dir = mktmpdir("crash_out");
    const auto ckpt_dir = mktmpdir("crash_ckpt");
    ::setenv("CLINK_2PC_OUT_DIR", out_dir.c_str(), 1);
    ::setenv("CLINK_2PC_TOTAL", "30", 1);
    ::setenv("CLINK_2PC_TICK_MS", "40", 1);

    // Run 1: start cluster, kick off the job, kill mid-stream.
    {
        auto c = start_cluster();
        ASSERT_TRUE(c.has_value()) << "run-1 cluster startup failed";

        const pid_t submit_pid = spawn_proc({"clink_submit_job",
                                             "--job=" + job_so.string(),
                                             "--jm-host=127.0.0.1",
                                             "--jm-port=" + std::to_string(c->jm_port),
                                             "--wait-timeout-s=5",
                                             "--checkpoint-dir=" + ckpt_dir.string(),
                                             "--checkpoint-interval-ms=100"},
                                            submit);
        ASSERT_GT(submit_pid, 0);

        // Wait until at least one checkpoint has completed, then crash
        // before the source finishes. Bounded source emits 30 records
        // at 40ms each = ~1.2s total; first checkpoint should land
        // within ~200ms.
        const auto deadline = std::chrono::steady_clock::now() + 1s;
        while (std::chrono::steady_clock::now() < deadline &&
               latest_completed_checkpoint(ckpt_dir) == 0) {
            std::this_thread::sleep_for(50ms);
        }
        ASSERT_GT(latest_completed_checkpoint(ckpt_dir), 0u) << "no checkpoint completed in run 1";

        // Hard kill JM + TM. Submitter will fail; that's expected.
        kill_quietly(c->jm_pid);
        kill_quietly(c->tm_pid);
        c->jm_pid = -1;
        c->tm_pid = -1;
        kill_quietly(submit_pid);
    }

    const auto run1_ckpt = latest_completed_checkpoint(ckpt_dir);
    const auto run1_committed = read_all_committed_lines(out_dir).size();
    EXPECT_GT(run1_committed, 0u) << "run 1 should have committed some records";

    // Run 2: fresh cluster, re-submit with restore_from. The sink's
    // open() walks state-backend's restored keys and commits any
    // leftover staging files. The source is replay-capable (#54): it
    // restores its checkpointed offset and resumes from there, so run 2
    // does NOT re-emit the records run 1 already committed (asserted by
    // the no-duplicates check below).
    auto c2 = start_cluster();
    ASSERT_TRUE(c2.has_value()) << "run-2 cluster startup failed";
    const pid_t submit2_pid =
        spawn_proc({"clink_submit_job",
                    "--job=" + job_so.string(),
                    "--jm-host=127.0.0.1",
                    "--jm-port=" + std::to_string(c2->jm_port),
                    "--wait-timeout-s=15",
                    "--checkpoint-dir=" + ckpt_dir.string(),
                    "--checkpoint-interval-ms=150",
                    "--restore-from-dir=" + ckpt_dir.string(),
                    "--restore-from-checkpoint-id=" + std::to_string(run1_ckpt)},
                   submit);
    ASSERT_GT(submit2_pid, 0);
    int submit2_exit = -1;
    ASSERT_TRUE(wait_for_exit(submit2_pid, 12s, &submit2_exit))
        << "run-2 submitter did not exit within 12s";
    EXPECT_EQ(submit2_exit, 0) << "run-2 submitter exited non-zero";

    // After run 2: every line in committed/ is a valid record-N.
    // Total record count is strictly larger than run 1's committed
    // size, proving that run 2 made progress and successfully
    // committed additional checkpoints. We DON'T require all 30
    // records to appear: bounded sources have no terminal-barrier
    // emission, so the tail (records past the last completed
    // checkpoint) sits in staging/ until a future submit. That's a
    // separate gap; the 2PC sink protocol itself is proved by
    // (a) the unit test's recover_pending_ assertion, and (b) here,
    // by the fact that more records got committed after restart.
    auto lines = read_all_committed_lines(out_dir);
    EXPECT_GT(lines.size(), run1_committed)
        << "run 2 did not commit any new records (recovery + replay broken?); "
        << "run1=" << run1_committed << " run2=" << lines.size();
    for (const auto& l : lines) {
        EXPECT_EQ(l.rfind("record-", 0), 0u) << "unexpected line: " << l;
    }
    // #54: the source is now replay-capable (it checkpoints its offset), so
    // run 2 resumes from where run 1 left off instead of replaying from 0.
    // Every committed record therefore appears EXACTLY once across both runs
    // - no duplicates. Before the operator-state replay seam this was a
    // documented gap (run 2 re-emitted from 0 and double-committed the
    // pre-crash records).
    std::set<std::string> distinct(lines.begin(), lines.end());
    EXPECT_EQ(distinct.size(), lines.size())
        << "duplicate committed records after restart (" << lines.size() << " lines, "
        << distinct.size() << " distinct): the source re-emitted instead of resuming";
}

// F1 - end-to-end exactly-once INVARIANCE across an AUTOMATIC (JM-driven)
// TaskManager-loss failover. This is the gap the two tests above each leave
// half-covered:
//   * RecoveryCommitsPreCommittedFilesOnRestart uses a MANUAL restore into a
//     FRESH cluster and explicitly WAIVES no-loss (a fresh JM resets its
//     checkpoint counter, so run-2's committed filenames can collide with
//     run-1's - it can only assert no-duplicates + progress, not full count).
//   * test_tm_crash_recovery drives the automatic failover but asserts nothing
//     about the sink output.
// Here the JM SURVIVES (only the TM hosting the job is SIGKILLed), so the JM's
// per-job checkpoint counter stays monotonic across the redeploy (verified:
// next_checkpoint_id is only ever ++'d, never reset, and rides the surviving
// job state). Monotonic ids mean the 2PC sink's committed/sub<N>-<ckpt>.dat
// filenames never collide, so the committed set is the union of pre- and
// post-crash checkpoints with no overwrite. We can therefore assert the
// strongest property: the committed output equals an uninterrupted run as an
// exact multiset - every record-0..N-1 present EXACTLY once.
//
// Why this catches both exactly-once failure modes:
//   * DUPLICATE (source restored offset 0 and replayed pre-crash records, or
//     the sink committed at the barrier instead of at checkpoint-complete):
//     lines.size() > N and distinct.size() < lines.size().
//   * LOSS (an uncommitted record was skipped on resume): lines.size() < N and
//     distinct != golden.
// Mutation checks that MUST turn this red (proving it is not trivially green):
//   (a) make BoundedSlowStringSource::restore_offset return false / force
//       counter_ = 0 -> replay-from-0 -> duplicates;
//   (b) move the file_2pc sink's staging->committed rename from on_commit
//       (checkpoint-complete) to on_barrier (prepare) -> early commit of the
//       uncommitted tail -> duplicates on resume.
// The uninterrupted golden itself is established empirically by
// HappyPathExactlyOnceCommittedFiles above (an uncrashed run commits exactly
// {record-0..record-(N-1)}); the source is deterministic, so that set is the
// golden this test compares against.
TEST(TwoPhaseCommit, TmKillMidStreamIsExactlyOnce) {
    const auto submit = submit_binary_path();
    const auto job_so = two_phase_commit_job_path();
    const auto node = node_binary_path();
    if (!std::filesystem::exists(submit) || !std::filesystem::exists(job_so) ||
        !std::filesystem::exists(node)) {
        GTEST_SKIP() << "cluster binaries / 2pc job not built (need -DCLINK_INTEGRATION_TESTS=ON)";
    }

    constexpr int kTotal = 80;
    const auto out_dir = mktmpdir("eo_out");
    const auto ckpt_dir = mktmpdir("eo_ckpt");
    const auto log_dir = mktmpdir("eo_log");
    ::setenv("CLINK_2PC_OUT_DIR", out_dir.c_str(), 1);
    ::setenv("CLINK_2PC_TOTAL", std::to_string(kTotal).c_str(), 1);
    ::setenv("CLINK_2PC_TICK_MS", "30", 1);  // ~2.4s of clean runtime

    // JM with a short-but-safe loss window (well above the TM's 500ms
    // heartbeat interval so a healthy TM is never wrongly declared lost). All
    // spawns redirect their output to per-process log files under log_dir (see
    // spawn_proc_logged): a JM/TM inheriting a captured stdout pipe would hang
    // the harness.
    const auto port = probe_free_port();
    const pid_t jm = spawn_proc_logged({"clink_node",
                                        "--role=jm",
                                        "--port=" + std::to_string(port),
                                        "--bind-host=127.0.0.1",
                                        "--heartbeat-timeout-ms=1500",
                                        "--watchdog-interval-ms=100"},
                                       node,
                                       log_dir / "jm.log");
    ASSERT_GT(jm, 0);
    ASSERT_TRUE(await_port_open(port, 2s));

    auto spawn_tm = [&](const std::string& id) {
        return spawn_proc_logged({"clink_node",
                                  "--role=tm",
                                  "--id=" + id,
                                  "--jm-host=127.0.0.1",
                                  "--jm-port=" + std::to_string(port),
                                  "--slots=4"},
                                 node,
                                 log_dir / (id + ".log"));
    };

    // ONE TM at submit time, so both subtasks deterministically land on it
    // and the later kill is guaranteed to hit the TM hosting the job.
    const pid_t tm_a = spawn_tm("tm-eo-A");
    ASSERT_GT(tm_a, 0);
    std::this_thread::sleep_for(500ms);  // tm-A registers

    const pid_t submit_pid = spawn_proc_logged({"clink_submit_job",
                                                "--job=" + job_so.string(),
                                                "--jm-host=127.0.0.1",
                                                "--jm-port=" + std::to_string(port),
                                                "--wait-timeout-s=60",
                                                "--checkpoint-dir=" + ckpt_dir.string(),
                                                "--checkpoint-interval-ms=100",
                                                "--max-restarts-on-tm-loss=3"},
                                               submit,
                                               log_dir / "submit.log");
    ASSERT_GT(submit_pid, 0);

    // Precondition: at least one checkpoint completes before the kill, so the
    // source has a durable offset to resume from. Without this gate a kill
    // could land before any state exists and the run would prove nothing.
    const auto ckpt_deadline = std::chrono::steady_clock::now() + 8s;
    while (std::chrono::steady_clock::now() < ckpt_deadline &&
           latest_completed_checkpoint(ckpt_dir) == 0) {
        std::this_thread::sleep_for(25ms);
    }
    const auto ckpt_before = latest_completed_checkpoint(ckpt_dir);
    ASSERT_GT(ckpt_before, 0u) << "no checkpoint completed before the kill";

    // Bring up the survivor (so a free slot exists for the redeploy), then
    // SIGKILL the TM hosting the job. Watchdog -> tm lost -> redeploy onto
    // tm-B, restoring from the last completed checkpoint.
    const pid_t tm_b = spawn_tm("tm-eo-B");
    ASSERT_GT(tm_b, 0);
    std::this_thread::sleep_for(500ms);  // tm-B registers
    kill_quietly(tm_a);

    // Wait past the submitter's own --wait-timeout-s=60 so its exit code is
    // the authoritative signal: on recovery it exits 0; if the job never
    // recovers it self-times-out and exits non-zero (a clean assertion below,
    // not a harness timeout).
    int submit_exit = -1;
    const bool exited = wait_for_exit(submit_pid, 70s, &submit_exit);

    // Absorb the terminal staging->committed flush lag: the submitter can see
    // JobCompleted a hair before the final rename is on disk.
    std::vector<std::string> lines;
    const auto poll = std::chrono::steady_clock::now() + 10s;
    do {
        lines = read_all_committed_lines(out_dir);
        if (lines.size() >= static_cast<std::size_t>(kTotal))
            break;
        std::this_thread::sleep_for(50ms);
    } while (std::chrono::steady_clock::now() < poll);

    const auto ckpt_after = latest_completed_checkpoint(ckpt_dir);
    kill_quietly(tm_b);
    kill_quietly(jm);

    ASSERT_TRUE(exited) << "submitter never exited after the TM kill";
    EXPECT_EQ(submit_exit, 0) << "job did not recover from the SIGKILL";
    EXPECT_GT(ckpt_after, ckpt_before) << "no NEW checkpoint after restart - job did not resume";

    std::set<std::string> golden;
    for (int i = 0; i < kTotal; ++i)
        golden.insert("record-" + std::to_string(i));
    const std::set<std::string> distinct(lines.begin(), lines.end());

    // (count) exact cardinality: a duplicate pushes it above N, a loss below.
    EXPECT_EQ(lines.size(), static_cast<std::size_t>(kTotal))
        << "committed record count != " << kTotal << " across the kill (duplicate or loss)";
    // (no duplicate) no record committed twice.
    EXPECT_EQ(distinct.size(), lines.size())
        << "a record was committed twice across the kill - not exactly-once (source replayed, "
           "or the sink committed before the checkpoint was durable)";
    // (multiset equality) load-bearing: no loss, no unexpected record, vs the
    // uninterrupted golden run.
    EXPECT_EQ(distinct, golden)
        << "committed set differs from the uninterrupted run - a record was lost or unexpected";
}
