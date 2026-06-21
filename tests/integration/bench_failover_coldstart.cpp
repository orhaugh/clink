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

// Non-blocking single poll: true if the process has already exited (reaps it).
bool process_exited(pid_t pid, int* exit_code = nullptr) {
    int status = 0;
    if (::waitpid(pid, &status, WNOHANG) == pid) {
        if (exit_code != nullptr) {
            *exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
        }
        return true;
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

// Count non-overlapping occurrences of `needle` in `path` (whole-file). Used to
// self-certify, from the JM log, that each sequential crash produced a NEW
// "tm lost" and a NEW single-survivor "jm.restart ... survivors=1" line - i.e.
// every kill was a genuine failover that forced a redeploy, not a no-op.
std::size_t count_occurrences(const fs::path& path, const std::string& needle) {
    std::ifstream in(path);
    if (!in || needle.empty()) {
        return 0;
    }
    std::string content((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    std::size_t n = 0;
    for (std::size_t pos = content.find(needle); pos != std::string::npos;
         pos = content.find(needle, pos + needle.size())) {
        ++n;
    }
    return n;
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
    const auto commit_deadline = clock_t_::now() + 10s;
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

struct SeqFailoverResult {
    std::size_t requested{0};  // N crashes asked for
    std::size_t real_crashes{
        0};  // crashes confirmed genuine (new tm-lost + survivors=1 + ckpt advance)
    std::size_t tm_lost_events{0};  // distinct "tm lost" lines the JM logged
    std::size_t survivor_restarts{
        0};  // distinct single-survivor "jm.restart ... survivors=1" lines
    std::size_t expected{0};
    std::size_t committed{0};
    std::size_t dups{0};         // total_lines - unique (>0 = a record committed twice)
    std::string missing_sample;  // first few missing record-K indices (diagnostic)
    int submit_exit{-1};
    bool monotonic_ckpts{true};  // completed-checkpoint id never went backwards across the run
    bool exactly_once{false};
    bool ok{false};
    double total_recovery_ms{0};
};

// N SEQUENTIAL mid-flight failovers on the given backend. Rolling standby:
// tm[i] is launched and registered BEFORE tm[i-1] is killed, so at the moment
// of each crash exactly one slot-holder remains and the JM's redeploy target is
// deterministic - the whole 2-subtask job (parallelism 1) lands on the single
// survivor, just as failover_once relies on. Each crash is forced mid-flight
// (we wait for fresh checkpoints after the standby is up) and SELF-CERTIFIED
// genuine from the JM log: a NEW "tm lost" line AND a NEW single-survivor
// "jm.restart ... survivors=1" line must appear, and the completed-checkpoint
// id must advance past the kill point (the redeployed job is processing again).
//
// SCOPE of the exactly-once claim this proves: every one of the TOTAL records
// is committed exactly once across a run that suffered N real failovers - no
// duplicate AND no loss, including the post-last-checkpoint tail. Records that
// cross a periodic checkpoint recover from S3 exactly-once; the tail is made
// recoverable too because at clean EOS the bounded source drives one
// JM-coordinated FINAL checkpoint (real id, committed via the normal ack ->
// CommitCheckpoint path) and BLOCKS until it commits before finishing, so a
// crash at end-of-stream replays and re-commits it rather than losing it. The
// check (no dup AND no loss AND submit_exit==0) also certifies S3 was the live
// backend on every restart: a degrade-to-memory would restore empty and replay
// -> duplicates, and a failed redeploy would surface as submit_exit != 0.
SeqFailoverResult failover_sequential(const fs::path& node,
                                      const fs::path& submit,
                                      const fs::path& job_so,
                                      int n_failovers,
                                      const std::string& default_state_backend_uri = "") {
    SeqFailoverResult r;
    r.requested = static_cast<std::size_t>(n_failovers);
    const int total = 1200;  // long-running source: all N back-to-back crashes land
    r.expected = static_cast<std::size_t>(total);

    ScopedDirs scratch;
    const auto ckpt_dir = mktmpdir("sfo_ckpt");
    const auto out_dir = mktmpdir("sfo_out");
    scratch.add(ckpt_dir);
    scratch.add(out_dir);
    ::setenv("CLINK_2PC_OUT_DIR", out_dir.c_str(), 1);
    ::setenv("CLINK_2PC_TOTAL", std::to_string(total).c_str(), 1);
    // ~36s of source runtime at p=1, so N rapid crashes all occur well before
    // end-of-input (each is a genuine mid-flight failover, not an end-of-job race).
    ::setenv("CLINK_2PC_TICK_MS", "30", 1);

    const int heartbeat_ms = 1000;
    const auto port = probe_free_port();
    std::vector<std::string> jm_args{"clink_node",
                                     "--role=jm",
                                     "--port=" + std::to_string(port),
                                     "--bind-host=127.0.0.1",
                                     "--heartbeat-timeout-ms=" + std::to_string(heartbeat_ms),
                                     "--watchdog-interval-ms=50"};
    if (!default_state_backend_uri.empty()) {
        jm_args.push_back("--default-state-backend=" + default_state_backend_uri);
    }
    const pid_t jm = spawn_logged(jm_args, node, ckpt_dir / "jm.log");
    if (jm <= 0 || await_port_open(port, 5s) < 0) {
        kill_quietly(jm);
        return r;
    }
    const auto jm_log = ckpt_dir / "jm.log";

    auto start_tm = [&](int idx) -> pid_t {
        const std::string log = "tm" + std::to_string(idx) + ".log";
        const pid_t p = spawn_logged({"clink_node",
                                      "--role=tm",
                                      "--id=tm-" + std::to_string(idx),
                                      "--jm-host=127.0.0.1",
                                      "--jm-port=" + std::to_string(port),
                                      "--slots=4"},
                                     node,
                                     ckpt_dir / log);
        if (p <= 0 || await_log_contains(ckpt_dir / log, "registered", clock_t_::now(), 5s) < 0) {
            return -1;
        }
        return p;
    };

    pid_t active = start_tm(0);
    if (active <= 0) {
        kill_quietly(jm);
        return r;
    }

    const pid_t sub = spawn_logged({"clink_submit_job",
                                    "--job=" + job_so.string(),
                                    "--jm-host=127.0.0.1",
                                    "--jm-port=" + std::to_string(port),
                                    "--wait-timeout-s=180",
                                    "--checkpoint-dir=" + ckpt_dir.string(),
                                    "--checkpoint-interval-ms=150",
                                    "--max-restarts-on-tm-loss=" + std::to_string(n_failovers + 2)},
                                   submit,
                                   ckpt_dir / "submit.log");
    if (sub <= 0) {
        kill_quietly(active);
        kill_quietly(jm);
        return r;
    }

    // Wait until the completed-checkpoint id reaches `target`, the job exits, or
    // a deadline; also tracks monotonicity (the id must never regress).
    std::uint64_t high_water = 0;
    auto wait_ckpt_ge = [&](std::uint64_t target) {
        const auto dl = clock_t_::now() + 30s;
        while (clock_t_::now() < dl) {
            if (process_exited(sub)) {
                return;
            }
            const auto c = latest_completed_checkpoint(ckpt_dir);
            if (c < high_water) {
                r.monotonic_ckpts = false;  // a committed checkpoint id went backwards
            }
            high_water = std::max(high_water, c);
            if (c >= target) {
                return;
            }
            std::this_thread::sleep_for(10ms);
        }
    };

    bool job_done = false;
    for (int i = 1; i <= n_failovers && !job_done; ++i) {
        if (process_exited(sub, &r.submit_exit)) {
            job_done = true;
            break;  // job finished before we could crash again
        }
        const pid_t standby = start_tm(i);
        if (standby <= 0) {
            break;  // no recovery target -> stop (ok stays false)
        }
        const std::uint64_t base = latest_completed_checkpoint(ckpt_dir);
        wait_ckpt_ge(base + 2);  // genuine mid-flight: fresh state landed since the standby came up
        const std::uint64_t before = latest_completed_checkpoint(ckpt_dir);
        const auto t_crash = clock_t_::now();
        kill_quietly(active);      // SIGKILL the active TM (the sole slot-holder)
        active = standby;          // the standby is the only remaining slot-holder
        wait_ckpt_ge(before + 2);  // the redeployed job is checkpointing again
        const std::uint64_t after = latest_completed_checkpoint(ckpt_dir);
        // Genuine failover: with single-TM placement the killed TM was running
        // the whole job, so the job CAN only advance past the kill point by
        // failing over to the standby and resuming. A checkpoint id strictly
        // beyond the pre-kill id is that proof. We deliberately do NOT read the
        // JM "tm lost" / "survivors=1" counts here per crash: those are logged
        // asynchronously ~heartbeat_timeout after the kill (and restarts are
        // coalesced under rapid succession), so a per-crash count read races the
        // logger. The total tm-lost count is checked once at the end instead,
        // when all logging has settled.
        if (after > before) {
            ++r.real_crashes;
            r.total_recovery_ms += ms_since(t_crash);
        }
    }

    // The bounded source REPLAYS from its last checkpoint after every failover,
    // so N crashes + recoveries push the wall-clock well past the crash-free
    // ~36s; wait generously (the submitter's own --wait-timeout-s=180 bounds it)
    // so a slow-but-completing job is not misread mid-flight as a shortfall.
    const bool exited = wait_for_exit(sub, 150s, &r.submit_exit);
    r.tm_lost_events = count_occurrences(jm_log, "tm lost");
    r.survivor_restarts = count_occurrences(jm_log, "survivors=1");  // informational (coalesced)

    // Absorb terminal-commit flush lag: the submitter can observe JobCompleted a
    // hair before the 2PC sink's final staging->committed rename is visible (the
    // commit flushes in bursts with multi-second gaps). Poll until the count
    // reaches `expected`, or genuinely PLATEAUS (unchanged for a run of
    // CONSECUTIVE reads = a real shortfall, not a between-bursts gap), capped at
    // a generous ceiling.
    std::vector<std::string> lines;
    std::set<std::string> uniq;
    const auto commit_deadline = clock_t_::now() + 60s;
    std::size_t prev = 0;
    int stable = 0;
    while (clock_t_::now() < commit_deadline) {
        lines = read_committed_lines(out_dir);
        uniq = std::set<std::string>(lines.begin(), lines.end());
        if (uniq.size() >= r.expected) {
            break;
        }
        stable = (uniq.size() == prev) ? stable + 1 : 0;
        prev = uniq.size();
        if (stable >= 150) {  // unchanged across 150 consecutive reads (~15s) -> genuine plateau
            break;
        }
        std::this_thread::sleep_for(100ms);
    }
    r.committed = uniq.size();
    r.dups = lines.size() - uniq.size();
    // Diagnostic: if short, which record-K are missing (first few)?
    if (uniq.size() < r.expected) {
        int shown = 0;
        for (int k = 0; k < total && shown < 8; ++k) {
            if (uniq.find("record-" + std::to_string(k)) == uniq.end()) {
                r.missing_sample += (shown ? "," : "") + std::to_string(k);
                ++shown;
            }
        }
    }
    // Exactly-once over the whole bounded output: no duplicate (lines == uniq)
    // AND no loss (uniq == expected) AND the job completed cleanly.
    r.exactly_once =
        exited && r.submit_exit == 0 && lines.size() == uniq.size() && r.committed == r.expected;
    // The proof holds iff every requested crash was a genuine, self-certified
    // sequential failover - the JM detected N distinct TM losses and the job
    // advanced past each kill point (r.real_crashes) - checkpoints never
    // regressed, and the full bounded output is exactly-once. survivor_restarts
    // is NOT gated on (the JM coalesces restarts under rapid succession).
    r.ok = r.exactly_once && r.real_crashes == static_cast<std::size_t>(n_failovers) &&
           r.tm_lost_events >= static_cast<std::size_t>(n_failovers) && r.monotonic_ckpts;

    kill_quietly(active);
    kill_quietly(jm);
    return r;
}

struct EosRecoveryResult {
    std::size_t expected{0};
    std::size_t committed{0};
    std::size_t dups{0};
    int submit_exit{-1};
    bool restart_observed{false};  // a whole-job restart fired (the EOS-timeout surfaced)
    bool exactly_once{false};
    bool ok{false};
};

// NO-CRASH end-of-stream final-checkpoint TIMEOUT, exercised deterministically.
// The JM is told (CLINK_TEST_STALL_FIRST_FINAL_CKPT) to drop every ack for the
// FIRST final checkpoint's first-acking subtask, so that checkpoint never
// completes; with a short EOS-wait timeout (CLINK_EOS_FINAL_CKPT_TIMEOUT_MS) the
// bounded source's wait_final_committed times out WITHOUT any crash, throws, and
// (the fix under test) that surfaces as had_error -> a whole-job rollback to the
// last completed checkpoint -> replay -> the replay's fresh final checkpoint
// completes -> exactly-once. Pre-fix the throw was swallowed and the job
// completed with the tail uncommitted; the assertions below (exactly-once AND a
// whole-job restart) fail pre-fix and pass post-fix. No TM is ever killed.
EosRecoveryResult eos_timeout_recovery(const fs::path& node,
                                       const fs::path& submit,
                                       const fs::path& job_so) {
    EosRecoveryResult r;
    const int total = 300;
    r.expected = static_cast<std::size_t>(total);

    ScopedDirs scratch;
    const auto ckpt_dir = mktmpdir("eos_ckpt");
    const auto out_dir = mktmpdir("eos_out");
    scratch.add(ckpt_dir);
    scratch.add(out_dir);
    ::setenv("CLINK_2PC_OUT_DIR", out_dir.c_str(), 1);
    ::setenv("CLINK_2PC_TOTAL", std::to_string(total).c_str(), 1);
    ::setenv("CLINK_2PC_TICK_MS", "15", 1);
    // The two fault-injection / config hooks the scenario needs.
    ::setenv("CLINK_EOS_FINAL_CKPT_TIMEOUT_MS", "3000", 1);
    ::setenv("CLINK_TEST_STALL_FIRST_FINAL_CKPT", "1", 1);

    const auto port = probe_free_port();
    const pid_t jm = spawn_logged({"clink_node",
                                   "--role=jm",
                                   "--port=" + std::to_string(port),
                                   "--bind-host=127.0.0.1",
                                   "--heartbeat-timeout-ms=1000",
                                   "--watchdog-interval-ms=50"},
                                  node,
                                  ckpt_dir / "jm.log");
    if (jm <= 0 || await_port_open(port, 5s) < 0) {
        kill_quietly(jm);
        ::unsetenv("CLINK_TEST_STALL_FIRST_FINAL_CKPT");
        ::unsetenv("CLINK_EOS_FINAL_CKPT_TIMEOUT_MS");
        return r;
    }
    const pid_t tm = spawn_logged({"clink_node",
                                   "--role=tm",
                                   "--id=tm-0",
                                   "--jm-host=127.0.0.1",
                                   "--jm-port=" + std::to_string(port),
                                   "--slots=4"},
                                  node,
                                  ckpt_dir / "tm0.log");
    if (tm <= 0 ||
        await_log_contains(ckpt_dir / "tm0.log", "registered", clock_t_::now(), 5s) < 0) {
        kill_quietly(tm);
        kill_quietly(jm);
        ::unsetenv("CLINK_TEST_STALL_FIRST_FINAL_CKPT");
        ::unsetenv("CLINK_EOS_FINAL_CKPT_TIMEOUT_MS");
        return r;
    }
    const pid_t sub = spawn_logged({"clink_submit_job",
                                    "--job=" + job_so.string(),
                                    "--jm-host=127.0.0.1",
                                    "--jm-port=" + std::to_string(port),
                                    "--wait-timeout-s=90",
                                    "--checkpoint-dir=" + ckpt_dir.string(),
                                    "--checkpoint-interval-ms=150",
                                    "--max-restarts-on-tm-loss=3"},
                                   submit,
                                   ckpt_dir / "submit.log");
    // The hooks are now in the spawned children's environment; unset in this
    // process so no later leg inherits them.
    ::unsetenv("CLINK_TEST_STALL_FIRST_FINAL_CKPT");
    ::unsetenv("CLINK_EOS_FINAL_CKPT_TIMEOUT_MS");
    const bool exited = (sub > 0) && wait_for_exit(sub, 90s, &r.submit_exit);

    std::vector<std::string> lines;
    std::set<std::string> uniq;
    const auto deadline = clock_t_::now() + 10s;
    while (clock_t_::now() < deadline) {
        lines = read_committed_lines(out_dir);
        uniq = std::set<std::string>(lines.begin(), lines.end());
        if (uniq.size() >= r.expected) {
            break;
        }
        std::this_thread::sleep_for(50ms);
    }
    r.committed = uniq.size();
    r.dups = lines.size() - uniq.size();
    // The EOS-timeout surfaced as a whole-job restart (vs the pre-fix swallow).
    r.restart_observed = file_contains(ckpt_dir / "jm.log", "subtask error -> whole-job restart");
    r.exactly_once =
        exited && r.submit_exit == 0 && lines.size() == uniq.size() && r.committed == r.expected;
    r.ok = r.exactly_once && r.restart_observed;

    kill_quietly(tm);
    kill_quietly(jm);
    return r;
}

struct EosBudgetResult {
    int max_restarts{0};
    std::size_t expected{0};
    std::size_t committed{0};
    std::size_t whole_job_restarts{0};  // "subtask error -> whole-job restart" count
    int submit_exit{-1};
    bool ok{false};
};

// PERMANENT no-crash EOS final-checkpoint failure: prove the whole-job restart
// recovery is BOUNDED (no infinite loop). CLINK_TEST_STALL_EVERY_FINAL_CKPT
// re-arms the stall on EVERY attempt's final checkpoint, so the bounded
// source's wait_final_committed times out and it throws on every replay. With a
// small budget (--max-restarts-on-tm-loss=2) the job must restart EXACTLY that
// many times then FAIL LOUDLY - not retry forever, not silently complete. Only
// the source ever throws on an EOS timeout (sinks don't wait), so exactly one
// restart fires per cycle; the assertions below check the restart count equals
// the budget and the submitter exits non-zero (failed or timed out, never
// 0/success). Companion to eos_timeout_recovery (which proves the RECOVERING
// case); this proves the GIVE-UP case is bounded. No TM is ever killed.
EosBudgetResult eos_budget_exhaustion(const fs::path& node,
                                      const fs::path& submit,
                                      const fs::path& job_so) {
    EosBudgetResult r;
    r.max_restarts = 2;
    const int total = 160;
    r.expected = static_cast<std::size_t>(total);

    ScopedDirs scratch;
    const auto ckpt_dir = mktmpdir("eosbudget_ckpt");
    const auto out_dir = mktmpdir("eosbudget_out");
    scratch.add(ckpt_dir);
    scratch.add(out_dir);
    ::setenv("CLINK_2PC_OUT_DIR", out_dir.c_str(), 1);
    ::setenv("CLINK_2PC_TOTAL", std::to_string(total).c_str(), 1);
    ::setenv("CLINK_2PC_TICK_MS", "10", 1);
    ::setenv("CLINK_EOS_FINAL_CKPT_TIMEOUT_MS", "2000", 1);
    ::setenv("CLINK_TEST_STALL_EVERY_FINAL_CKPT", "1", 1);

    const auto port = probe_free_port();
    const pid_t jm = spawn_logged({"clink_node",
                                   "--role=jm",
                                   "--port=" + std::to_string(port),
                                   "--bind-host=127.0.0.1",
                                   "--heartbeat-timeout-ms=1000",
                                   "--watchdog-interval-ms=50"},
                                  node,
                                  ckpt_dir / "jm.log");
    if (jm <= 0 || await_port_open(port, 5s) < 0) {
        kill_quietly(jm);
        ::unsetenv("CLINK_TEST_STALL_EVERY_FINAL_CKPT");
        ::unsetenv("CLINK_EOS_FINAL_CKPT_TIMEOUT_MS");
        return r;
    }
    const pid_t tm = spawn_logged({"clink_node",
                                   "--role=tm",
                                   "--id=tm-0",
                                   "--jm-host=127.0.0.1",
                                   "--jm-port=" + std::to_string(port),
                                   "--slots=4"},
                                  node,
                                  ckpt_dir / "tm0.log");
    if (tm <= 0 ||
        await_log_contains(ckpt_dir / "tm0.log", "registered", clock_t_::now(), 5s) < 0) {
        kill_quietly(tm);
        kill_quietly(jm);
        ::unsetenv("CLINK_TEST_STALL_EVERY_FINAL_CKPT");
        ::unsetenv("CLINK_EOS_FINAL_CKPT_TIMEOUT_MS");
        return r;
    }
    const pid_t sub = spawn_logged({"clink_submit_job",
                                    "--job=" + job_so.string(),
                                    "--jm-host=127.0.0.1",
                                    "--jm-port=" + std::to_string(port),
                                    "--wait-timeout-s=60",
                                    "--checkpoint-dir=" + ckpt_dir.string(),
                                    "--checkpoint-interval-ms=150",
                                    "--max-restarts-on-tm-loss=" + std::to_string(r.max_restarts)},
                                   submit,
                                   ckpt_dir / "submit.log");
    ::unsetenv("CLINK_TEST_STALL_EVERY_FINAL_CKPT");
    ::unsetenv("CLINK_EOS_FINAL_CKPT_TIMEOUT_MS");
    const bool exited = (sub > 0) && wait_for_exit(sub, 75s, &r.submit_exit);

    const auto lines = read_committed_lines(out_dir);
    const std::set<std::string> uniq(lines.begin(), lines.end());
    r.committed = uniq.size();
    r.whole_job_restarts =
        count_occurrences(ckpt_dir / "jm.log", "subtask error -> whole-job restart");
    // Bounded recovery: EXACTLY max_restarts whole-job restarts (no infinite
    // loop), and the job ends in failure (submit_exit != 0), never success.
    r.ok = exited && r.whole_job_restarts == static_cast<std::size_t>(r.max_restarts) &&
           r.submit_exit != 0;

    kill_quietly(tm);
    kill_quietly(jm);
    return r;
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

    // ---- SEQUENTIAL multi-failover: crash the active TM N times in a row ----
    // Proves exactly-once is preserved not just across one TM loss but across a
    // chain of them, each crash mid-flight on a fresh active TM (rolling
    // standby). Run on the local-dir backend as a control and, when MinIO is
    // configured, on the disaggregated remote-read:// (S3) backend - the latter
    // re-restores the source offset from the last committed S3 checkpoint on
    // EVERY crash. exactly_once over the full bounded output (no dup, no loss)
    // after N self-certified failovers is the proof.
    constexpr int kSeqFailovers = 3;
    std::printf("\n---- sequential multi-failover (%d crashes in a row) ----\n", kSeqFailovers);
    const auto seq_file = failover_sequential(node, submit, job_so, kSeqFailovers);
    std::printf(
        "  [file] real_crashes=%zu/%zu tm_lost=%zu survivor_restarts=%zu "
        "committed=%zu/%zu dups=%zu submit_exit=%d monotonic_ckpts=%s exactly_once=%s%s%s\n",
        seq_file.real_crashes,
        seq_file.requested,
        seq_file.tm_lost_events,
        seq_file.survivor_restarts,
        seq_file.committed,
        seq_file.expected,
        seq_file.dups,
        seq_file.submit_exit,
        seq_file.monotonic_ckpts ? "yes" : "NO",
        seq_file.exactly_once ? "yes" : "NO",
        seq_file.missing_sample.empty() ? "" : " missing=",
        seq_file.missing_sample.c_str());

    bool seq_remote_ok = true;  // pass when skipped
    std::printf("  ---- on remote-read:// (disaggregated S3 state) ----\n");
    if (s3_endpoint != nullptr && s3_bucket != nullptr) {
        const std::string uri = "remote-read://" + std::string{s3_bucket} + "/seq-failover-" +
                                std::to_string(static_cast<long long>(::getpid())) +
                                "?endpoint=" + s3_endpoint + "&region=us-east-1";
        const auto seq_rr = failover_sequential(node, submit, job_so, kSeqFailovers, uri);
        std::printf(
            "  [remote-read] real_crashes=%zu/%zu tm_lost=%zu survivor_restarts=%zu "
            "committed=%zu/%zu dups=%zu submit_exit=%d avg_recovery_ms=%.0f "
            "monotonic_ckpts=%s exactly_once=%s%s%s\n",
            seq_rr.real_crashes,
            seq_rr.requested,
            seq_rr.tm_lost_events,
            seq_rr.survivor_restarts,
            seq_rr.committed,
            seq_rr.expected,
            seq_rr.dups,
            seq_rr.submit_exit,
            seq_rr.real_crashes > 0 ? seq_rr.total_recovery_ms / seq_rr.real_crashes : 0.0,
            seq_rr.monotonic_ckpts ? "yes" : "NO",
            seq_rr.exactly_once ? "yes" : "NO",
            seq_rr.missing_sample.empty() ? "" : " missing=",
            seq_rr.missing_sample.c_str());
        std::printf(
            "  (every record - including the post-last-checkpoint tail - is durably\n"
            "   committed: at clean EOS the source drives one JM-coordinated FINAL\n"
            "   checkpoint and blocks until it commits, so a crash anywhere replays it)\n");
        seq_remote_ok = seq_rr.ok;
    } else {
        std::printf("  SKIPPED (set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET to run)\n");
    }

    // ---- no-crash end-of-stream final-checkpoint TIMEOUT -> recovery ----
    // The one path the failover legs cannot reach (they all crash a TM): a
    // source's EOS final checkpoint stalls while the JM stays alive. It must
    // surface as a whole-job restart + replay, recovering exactly-once, NOT
    // silently complete with an uncommitted tail. Runs last so its test hooks
    // never leak to the other legs. Local-only (no S3 needed).
    std::printf("\n---- end-of-stream final-checkpoint timeout (no crash) ----\n");
    const auto eos = eos_timeout_recovery(node, submit, job_so);
    std::printf(
        "  committed=%zu/%zu dups=%zu submit_exit=%d whole_job_restart=%s exactly_once=%s\n",
        eos.committed,
        eos.expected,
        eos.dups,
        eos.submit_exit,
        eos.restart_observed ? "yes" : "NO",
        eos.exactly_once ? "yes" : "NO");
    std::printf(
        "  (the stalled final checkpoint timed out -> the source threw -> the JM rolled\n"
        "   the whole job back to the last checkpoint and replayed, recovering the tail)\n");

    // ---- PERMANENT EOS final-checkpoint failure -> bounded restart + loud fail ----
    // Same no-crash EOS-timeout path, but the stall RE-ARMS every attempt, so the
    // source errors on every replay. The recovery must be BOUNDED: exactly
    // max_restarts whole-job restarts, then a loud failure - never an infinite
    // restart loop, never silent success. Local-only.
    std::printf("\n---- end-of-stream final-checkpoint PERMANENT failure (bounded restart) ----\n");
    const auto eosb = eos_budget_exhaustion(node, submit, job_so);
    std::printf("  budget=%d whole_job_restarts=%zu submit_exit=%d committed=%zu/%zu bounded=%s\n",
                eosb.max_restarts,
                eosb.whole_job_restarts,
                eosb.submit_exit,
                eosb.committed,
                eosb.expected,
                eosb.ok ? "yes" : "NO");
    std::printf(
        "  (a permanently-stalling final checkpoint restarts the whole job exactly\n"
        "   budget times, then fails loudly - the recovery loop is bounded)\n");

    const bool ok =
        cold_ok == kIters && f.ok && remote_ok && seq_file.ok && seq_remote_ok && eos.ok && eosb.ok;
    std::printf("\n==== %s ====\n", ok ? "PASS" : "FAIL");
    return ok ? 0 : 1;
}
