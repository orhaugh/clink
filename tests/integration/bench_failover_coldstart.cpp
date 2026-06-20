// Failover + cold-start benchmark for the clink cluster.
//
// Measures two things the throughput benches do not, and that the
// native-binary (no-JVM, no-warmup) design is meant to win on:
//
//   1. COLD START - wall-clock from a cold process launch to a running,
//      producing job. Reported as phases over K fresh JM+TM launches:
//        jm_up      : spawn clink_node JM -> control port accepts TCP
//        tm_register: spawn clink_node TM -> "registered" on its stdout
//        deploy_run : clink_submit_job start -> a small bounded job commits
//        total      : JM spawn -> bounded job committed
//
//   2. FAILOVER RECOVERY - wall-clock from a TaskManager SIGKILL to the
//      job processing again, observed as a NEW durable checkpoint
//      (COMPLETED-N marker with id > the last one before the crash). The
//      JM's TM-loss detection window (--heartbeat-timeout-ms) is set low so
//      the measured number is dominated by the actual recovery work
//      (detect -> redeploy onto the survivor -> restore state -> resume),
//      not by the conservative default detection delay.
//
// This is a clink-only measurement (no cross-engine ratio). It reuses the
// two-phase-commit example job: a bounded slow source that checkpoints its
// offset piped to a 2PC file sink, so the committed/ output proves real,
// exactly-once work both for cold start and across the failover.
//
// Build: -DCLINK_INTEGRATION_TESTS=ON (needs clink_node, clink_submit_job,
// and the 2PC job .so, all from the same git commit so their plugin ABI
// hashes match). Run via ctest -L benchmark, or invoke the binary directly.
// Exit 0 iff both scenarios completed and produced correct output.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
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

#include <sys/wait.h>

#include "clink/core/codec.hpp"
#include "clink/runtime/network/network_channel.hpp"
#include "clink/runtime/network/network_socket.hpp"

extern char** environ;

namespace {

using namespace clink;
using namespace clink::network;
using namespace std::chrono_literals;
namespace fs = std::filesystem;
using clock_t_ = std::chrono::steady_clock;

fs::path node_binary_path() {
#ifdef CLINK_NODE_BINARY
    return fs::path{CLINK_NODE_BINARY};
#else
    return {};
#endif
}
fs::path submit_binary_path() {
#ifdef CLINK_SUBMIT_BINARY
    return fs::path{CLINK_SUBMIT_BINARY};
#else
    return {};
#endif
}
fs::path two_phase_commit_job_path() {
#ifdef CLINK_TWO_PHASE_COMMIT_JOB_PATH
    return fs::path{CLINK_TWO_PHASE_COMMIT_JOB_PATH};
#else
    return {};
#endif
}

double ms_since(clock_t_::time_point t0) {
    return std::chrono::duration<double, std::milli>(clock_t_::now() - t0).count();
}

// posix_spawn with stdout+stderr redirected to log_path, so we can poll
// the child's startup banner ("JM listening", "TM ... registered") for a
// precise readiness timestamp instead of a conservative fixed sleep.
pid_t spawn_logged(const std::vector<std::string>& argv,
                   const fs::path& binary,
                   const fs::path& log_path) {
    std::vector<char*> raw;
    raw.reserve(argv.size() + 1);
    for (const auto& s : argv) {
        raw.push_back(const_cast<char*>(s.c_str()));
    }
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
    const auto deadline = clock_t_::now() + timeout;
    while (clock_t_::now() < deadline) {
        int status = 0;
        if (::waitpid(pid, &status, WNOHANG) == pid) {
            if (exit_code != nullptr) {
                *exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
            }
            return true;
        }
        std::this_thread::sleep_for(10ms);
    }
    return false;
}

std::uint16_t probe_free_port() {
    NetworkChannelSource<std::int64_t> probe(0, int64_codec());
    return probe.listen();
}

// Removes its registered temp directories on destruction, so every exit
// path (including early error returns) cleans up after itself.
struct ScopedDirs {
    std::vector<fs::path> dirs;
    void add(fs::path p) { dirs.push_back(std::move(p)); }
    ~ScopedDirs() {
        for (const auto& d : dirs) {
            std::error_code ec;
            fs::remove_all(d, ec);
        }
    }
};

fs::path mktmpdir(const std::string& tag) {
    static int counter = 0;
    auto dir = fs::temp_directory_path() / ("clink_focs_" + tag + "_" + std::to_string(::getpid()) +
                                            "_" + std::to_string(++counter));
    fs::remove_all(dir);
    fs::create_directories(dir);
    return dir;
}

// Poll a TCP port until it accepts a connection. Returns elapsed ms, or
// -1 on timeout.
double await_port_open(std::uint16_t port, std::chrono::milliseconds timeout) {
    const auto t0 = clock_t_::now();
    const auto deadline = t0 + timeout;
    while (clock_t_::now() < deadline) {
        const int fd = NetworkSocket::connect_to("127.0.0.1", port);
        if (fd >= 0) {
            NetworkSocket::close(fd);
            return ms_since(t0);
        }
        std::this_thread::sleep_for(2ms);
    }
    return -1.0;
}

// Poll a log file until it contains `needle`. Returns elapsed ms (from
// the supplied t0), or -1 on timeout.
double await_log_contains(const fs::path& log_path,
                          const std::string& needle,
                          clock_t_::time_point t0,
                          std::chrono::milliseconds timeout) {
    const auto deadline = clock_t_::now() + timeout;
    while (clock_t_::now() < deadline) {
        std::ifstream in(log_path);
        if (in) {
            std::string content((std::istreambuf_iterator<char>(in)),
                                std::istreambuf_iterator<char>());
            if (content.find(needle) != std::string::npos) {
                return ms_since(t0);
            }
        }
        std::this_thread::sleep_for(2ms);
    }
    return -1.0;
}

std::uint64_t latest_completed_checkpoint(const fs::path& ckpt_dir) {
    std::uint64_t latest = 0;
    if (!fs::exists(ckpt_dir)) {
        return 0;
    }
    for (const auto& e : fs::directory_iterator(ckpt_dir)) {
        if (!e.is_regular_file()) {
            continue;
        }
        const auto name = e.path().filename().string();
        if (name.rfind("COMPLETED-", 0) != 0) {
            continue;
        }
        try {
            const std::uint64_t id = std::stoull(name.substr(std::string{"COMPLETED-"}.size()));
            latest = std::max(latest, id);
        } catch (...) {
        }
    }
    return latest;
}

bool file_contains(const fs::path& path, const std::string& needle) {
    std::ifstream in(path);
    if (!in) {
        return false;
    }
    std::string content((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    return content.find(needle) != std::string::npos;
}

std::vector<std::string> read_committed_lines(const fs::path& out_dir) {
    std::vector<std::string> lines;
    const auto committed = out_dir / "committed";
    if (!fs::exists(committed)) {
        return lines;
    }
    for (const auto& e : fs::directory_iterator(committed)) {
        if (!e.is_regular_file()) {
            continue;
        }
        std::ifstream in(e.path());
        std::string l;
        while (std::getline(in, l)) {
            if (!l.empty()) {
                lines.push_back(l);
            }
        }
    }
    return lines;
}

struct Stats {
    double min{0}, median{0}, max{0};
};
Stats summarise(std::vector<double> xs) {
    Stats s;
    if (xs.empty()) {
        return s;
    }
    std::sort(xs.begin(), xs.end());
    s.min = xs.front();
    s.max = xs.back();
    s.median = xs[xs.size() / 2];
    return s;
}

// ---- cold start ---------------------------------------------------------

struct ColdSample {
    double jm_up{-1}, tm_register{-1}, deploy_run{-1}, total{-1};
    bool ok{false};
    std::size_t committed{0};
};

ColdSample cold_start_once(const fs::path& node,
                           const fs::path& submit,
                           const fs::path& job_so,
                           int iter) {
    ColdSample s;
    ScopedDirs scratch;
    const auto ckpt_dir = mktmpdir("cs_ckpt_" + std::to_string(iter));
    const auto out_dir = mktmpdir("cs_out_" + std::to_string(iter));
    scratch.add(ckpt_dir);
    scratch.add(out_dir);
    const auto jm_log = ckpt_dir / "jm.log";
    const auto tm_log = ckpt_dir / "tm.log";

    // Tiny bounded job: the dominant cost is process launch + plugin
    // dlopen + deploy, not the ~handful of ms of record processing.
    ::setenv("CLINK_2PC_OUT_DIR", out_dir.c_str(), 1);
    ::setenv("CLINK_2PC_TOTAL", "10", 1);
    ::setenv("CLINK_2PC_TICK_MS", "1", 1);

    const auto port = probe_free_port();
    const auto t_jm = clock_t_::now();
    const pid_t jm = spawn_logged(
        {"clink_node", "--role=jm", "--port=" + std::to_string(port), "--bind-host=127.0.0.1"},
        node,
        jm_log);
    if (jm <= 0) {
        return s;
    }
    s.jm_up = await_port_open(port, 5s);

    const auto t_tm = clock_t_::now();
    const pid_t tm = spawn_logged({"clink_node",
                                   "--role=tm",
                                   "--id=tm-cs",
                                   "--jm-host=127.0.0.1",
                                   "--jm-port=" + std::to_string(port),
                                   "--slots=4"},
                                  node,
                                  tm_log);
    if (tm > 0) {
        s.tm_register = await_log_contains(tm_log, "registered", t_tm, 5s);
    }

    const auto t_submit = clock_t_::now();
    const pid_t sub = spawn_logged({"clink_submit_job",
                                    "--job=" + job_so.string(),
                                    "--jm-host=127.0.0.1",
                                    "--jm-port=" + std::to_string(port),
                                    "--wait-timeout-s=30",
                                    "--checkpoint-dir=" + ckpt_dir.string(),
                                    "--checkpoint-interval-ms=5000"},
                                   submit,
                                   ckpt_dir / "submit.log");
    int sub_exit = -1;
    const bool exited = sub > 0 && wait_for_exit(sub, 30s, &sub_exit);
    s.deploy_run = ms_since(t_submit);
    s.total = ms_since(t_jm);

    const auto lines = read_committed_lines(out_dir);
    std::set<std::string> uniq(lines.begin(), lines.end());
    s.committed = uniq.size();
    s.ok = exited && sub_exit == 0 && s.committed == 10 && lines.size() == uniq.size();

    kill_quietly(tm);
    kill_quietly(jm);
    return s;  // scratch dirs cleaned by ScopedDirs
}

// ---- failover -----------------------------------------------------------

struct FailoverResult {
    bool ok{false};
    int heartbeat_ms{0};
    std::uint64_t ckpt_before{0};
    std::uint64_t ckpt_after{0};
    double recovery_ms{-1};
    int submit_exit{-1};
    std::size_t committed{0};
    std::size_t expected{0};
    bool exactly_once{false};
    bool real_failover{false};  // JM watchdog actually detected the TM loss
};

// default_state_backend_uri (when non-empty) is passed to the JM as
// --default-state-backend, so the 2PC job's keyed + operator state (the source
// offset) lives there instead of the local checkpoint dir. With a
// remote-read:// URI the state is in S3 and is LAZILY restored from the last
// committed checkpoint after the crash - the same exactly-once invariant must
// still hold, which proves disaggregated state recovers correctly through a
// failover. checkpoint_dir stays the JM's local coordination dir either way.
FailoverResult failover_once(const fs::path& node,
                             const fs::path& submit,
                             const fs::path& job_so,
                             const std::string& default_state_backend_uri = "") {
    FailoverResult r;
    r.heartbeat_ms = 1000;  // > the TM's 500ms heartbeat interval, << the 5s default
    const int total = 400;
    r.expected = static_cast<std::size_t>(total);

    ScopedDirs scratch;
    const auto ckpt_dir = mktmpdir("fo_ckpt");
    const auto out_dir = mktmpdir("fo_out");
    scratch.add(ckpt_dir);
    scratch.add(out_dir);
    ::setenv("CLINK_2PC_OUT_DIR", out_dir.c_str(), 1);
    ::setenv("CLINK_2PC_TOTAL", std::to_string(total).c_str(), 1);
    ::setenv("CLINK_2PC_TICK_MS", "20", 1);  // ~8s of runtime: room to crash mid-flight

    const auto port = probe_free_port();
    std::vector<std::string> jm_args{"clink_node",
                                     "--role=jm",
                                     "--port=" + std::to_string(port),
                                     "--bind-host=127.0.0.1",
                                     "--heartbeat-timeout-ms=" + std::to_string(r.heartbeat_ms),
                                     "--watchdog-interval-ms=50"};
    if (!default_state_backend_uri.empty()) {
        jm_args.push_back("--default-state-backend=" + default_state_backend_uri);
    }
    const pid_t jm = spawn_logged(jm_args, node, ckpt_dir / "jm.log");
    if (jm <= 0 || await_port_open(port, 5s) < 0) {
        kill_quietly(jm);
        return r;
    }

    // Bring up TM-A alone and submit: with a single TM the whole 2-subtask
    // job deploys onto it deterministically (no placement ambiguity), so
    // killing TM-A is guaranteed to take down every subtask and force a
    // real restore-and-redeploy - not a no-op kill of an idle TM.
    const pid_t tmA = spawn_logged({"clink_node",
                                    "--role=tm",
                                    "--id=tm-A",
                                    "--jm-host=127.0.0.1",
                                    "--jm-port=" + std::to_string(port),
                                    "--slots=4"},
                                   node,
                                   ckpt_dir / "tmA.log");
    if (tmA <= 0 ||
        await_log_contains(ckpt_dir / "tmA.log", "registered", clock_t_::now(), 5s) < 0) {
        kill_quietly(tmA);
        kill_quietly(jm);
        return r;
    }

    const pid_t sub = spawn_logged({"clink_submit_job",
                                    "--job=" + job_so.string(),
                                    "--jm-host=127.0.0.1",
                                    "--jm-port=" + std::to_string(port),
                                    "--wait-timeout-s=60",
                                    "--checkpoint-dir=" + ckpt_dir.string(),
                                    "--checkpoint-interval-ms=150",
                                    "--max-restarts-on-tm-loss=2"},
                                   submit,
                                   ckpt_dir / "submit.log");
    if (sub <= 0) {
        kill_quietly(tmA);
        kill_quietly(jm);
        return r;
    }

    // Let a couple of checkpoints land so there is real state to restore.
    const auto deadline = clock_t_::now() + 10s;
    while (clock_t_::now() < deadline && latest_completed_checkpoint(ckpt_dir) < 2) {
        std::this_thread::sleep_for(10ms);
    }

    // Bring up TM-B as the recovery target only now, AFTER the job is
    // checkpointing on TM-A. It sits idle until TM-A dies, then the JM
    // redeploys the restored job onto it (the proven survivor path).
    const pid_t tmB = spawn_logged({"clink_node",
                                    "--role=tm",
                                    "--id=tm-B",
                                    "--jm-host=127.0.0.1",
                                    "--jm-port=" + std::to_string(port),
                                    "--slots=4"},
                                   node,
                                   ckpt_dir / "tmB.log");
    // TM-B is the mandatory recovery target: if it never registers there is
    // nowhere to redeploy after the crash, so bail rather than report a
    // meaningless number.
    if (tmB <= 0 ||
        await_log_contains(ckpt_dir / "tmB.log", "registered", clock_t_::now(), 5s) < 0) {
        kill_quietly(tmA);
        kill_quietly(tmB);
        kill_quietly(jm);
        return r;
    }
    r.ckpt_before = latest_completed_checkpoint(ckpt_dir);

    // Crash TM-A and time the recovery: from SIGKILL to a NEW durable
    // checkpoint id, which proves the redeployed job is processing again.
    // Detection is via the JM watchdog (no connection-close fast path), so
    // ~heartbeat_timeout of the measured number is the detection window.
    const auto t_crash = clock_t_::now();
    kill_quietly(tmA);
    const auto rec_deadline = clock_t_::now() + 30s;
    while (clock_t_::now() < rec_deadline &&
           latest_completed_checkpoint(ckpt_dir) <= r.ckpt_before) {
        std::this_thread::sleep_for(5ms);
    }
    r.ckpt_after = latest_completed_checkpoint(ckpt_dir);
    if (r.ckpt_after > r.ckpt_before) {
        r.recovery_ms = ms_since(t_crash);
    }
    // Confirm the JM actually detected TM-A's death and ran the restart
    // path - otherwise the kill was a no-op and recovery_ms is meaningless.
    r.real_failover = file_contains(ckpt_dir / "jm.log", "tm lost");

    const bool exited = wait_for_exit(sub, 40s, &r.submit_exit);

    // Absorb terminal-commit flush lag: the submitter can observe
    // JobCompleted a hair before the 2PC sink's final staging->committed
    // rename is visible on disk. Poll committed/ until it reaches the
    // expected count or a short deadline, so a slow final commit is not
    // misread as record loss.
    std::vector<std::string> lines;
    std::set<std::string> uniq;
    const auto commit_deadline = clock_t_::now() + 5s;
    do {
        lines = read_committed_lines(out_dir);
        uniq = std::set<std::string>(lines.begin(), lines.end());
        if (uniq.size() >= r.expected) {
            break;
        }
        std::this_thread::sleep_for(50ms);
    } while (clock_t_::now() < commit_deadline);
    r.committed = uniq.size();
    // Exactly-once across the failover: the bounded source checkpoints its
    // offset and the 2PC sink commits on checkpoint, so the completed job
    // must have committed every record exactly once (no dup, no loss).
    r.exactly_once =
        exited && r.submit_exit == 0 && lines.size() == uniq.size() && r.committed == r.expected;
    r.ok = r.ckpt_before > 0 && r.ckpt_after > r.ckpt_before && r.recovery_ms >= 0 &&
           r.real_failover && r.exactly_once;

    kill_quietly(tmB);
    kill_quietly(jm);
    return r;  // scratch dirs cleaned by ScopedDirs
}

}  // namespace

int main() {
    const auto node = node_binary_path();
    const auto submit = submit_binary_path();
    const auto job_so = two_phase_commit_job_path();
    if (node.empty() || submit.empty() || job_so.empty() || !fs::exists(node) ||
        !fs::exists(submit) || !fs::exists(job_so)) {
        std::fprintf(stderr, "SKIP: clink_node / clink_submit_job / 2PC job .so not built\n");
        return 0;  // not a failure: the bench just cannot run here
    }

    std::printf("==== clink failover + cold-start benchmark ====\n");
    std::printf("node=%s\n", node.c_str());

    // ---- cold start ----
    constexpr int kIters = 5;
    std::vector<double> jm_up, tm_reg, deploy, total;
    int cold_ok = 0;
    for (int i = 0; i < kIters; ++i) {
        const auto s = cold_start_once(node, submit, job_so, i);
        std::printf(
            "cold[%d]: jm_up=%.1fms tm_register=%.1fms deploy_run=%.1fms total=%.1fms "
            "committed=%zu "
            "%s\n",
            i,
            s.jm_up,
            s.tm_register,
            s.deploy_run,
            s.total,
            s.committed,
            s.ok ? "OK" : "FAIL");
        if (s.ok) {
            ++cold_ok;
            jm_up.push_back(s.jm_up);
            tm_reg.push_back(s.tm_register);
            deploy.push_back(s.deploy_run);
            total.push_back(s.total);
        }
    }
    const auto sj = summarise(jm_up), st = summarise(tm_reg), sd = summarise(deploy),
               so = summarise(total);
    std::printf("\nCOLD START (%d/%d ok, median over ok runs):\n", cold_ok, kIters);
    std::printf("  jm_up        min=%.1f median=%.1f max=%.1f ms\n", sj.min, sj.median, sj.max);
    std::printf("  tm_register  min=%.1f median=%.1f max=%.1f ms\n", st.min, st.median, st.max);
    std::printf("  deploy_run   min=%.1f median=%.1f max=%.1f ms\n", sd.min, sd.median, sd.max);
    std::printf("  total        min=%.1f median=%.1f max=%.1f ms\n", so.min, so.median, so.max);

    // ---- failover ----
    std::printf("\n---- failover ----\n");
    const auto f = failover_once(node, submit, job_so);
    std::printf("FAILOVER RECOVERY:\n");
    std::printf("  real failover          %s (JM watchdog detected the TM loss)\n",
                f.real_failover ? "yes" : "NO - kill was a no-op, number invalid");
    std::printf("  heartbeat_timeout      %d ms (TM-loss detection window, tunable)\n",
                f.heartbeat_ms);
    std::printf("  checkpoint before crash COMPLETED-%llu\n",
                static_cast<unsigned long long>(f.ckpt_before));
    std::printf("  checkpoint after  crash COMPLETED-%llu\n",
                static_cast<unsigned long long>(f.ckpt_after));
    std::printf("  recovery_ms            %.1f ms (SIGKILL -> first new durable checkpoint)\n",
                f.recovery_ms);
    std::printf("    of which ~%d ms is the tunable TM-loss detection window;\n", f.heartbeat_ms);
    std::printf("    the remainder is redeploy + state restore + resume.\n");
    std::printf("  job completed          submit_exit=%d\n", f.submit_exit);
    std::printf("  committed records      %zu / %zu (exactly-once across failover: %s)\n",
                f.committed,
                f.expected,
                f.exactly_once ? "yes" : "NO");

    // ---- failover on the disaggregated remote-read:// backend (real S3/MinIO) ----
    // Same scenario, but the job's state lives in S3 instead of a local dir, so
    // recovery must LAZILY restore the source offset from the last committed S3
    // checkpoint - there is no local state to fall back on. The exactly-once
    // invariant holding is the proof that disaggregated state survives failover.
    // Gated on a configured endpoint so the bench still runs (skips this leg)
    // where no MinIO/S3 is available.
    bool remote_ok = true;  // treated as pass when skipped
    const char* s3_endpoint = std::getenv("CLINK_S3_TEST_ENDPOINT");
    const char* s3_bucket = std::getenv("CLINK_S3_TEST_BUCKET");
    std::printf("\n---- failover on remote-read:// (disaggregated S3 state) ----\n");
    if (s3_endpoint != nullptr && s3_bucket != nullptr) {
        // Unique per-process prefix so reruns never read a stale checkpoint.
        const std::string uri = "remote-read://" + std::string{s3_bucket} + "/failover-" +
                                std::to_string(static_cast<long long>(::getpid())) +
                                "?endpoint=" + s3_endpoint + "&region=us-east-1";
        std::printf("  backend=%s\n", uri.c_str());
        const auto rf = failover_once(node, submit, job_so, uri);
        std::printf("  real failover          %s\n",
                    rf.real_failover ? "yes" : "NO - kill was a no-op, number invalid");
        std::printf("  recovery_ms            %.1f ms (lazy S3 restore + redeploy + resume)\n",
                    rf.recovery_ms);
        std::printf("  committed records      %zu / %zu (exactly-once via S3 restore: %s)\n",
                    rf.committed,
                    rf.expected,
                    rf.exactly_once ? "yes" : "NO");
        remote_ok = rf.ok;
    } else {
        std::printf("  SKIPPED (set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET to run)\n");
    }

    const bool ok = cold_ok == kIters && f.ok && remote_ok;
    std::printf("\n==== %s ====\n", ok ? "PASS" : "FAIL");
    return ok ? 0 : 1;
}
