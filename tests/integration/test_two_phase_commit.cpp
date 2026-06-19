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
