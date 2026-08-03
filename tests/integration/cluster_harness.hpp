#pragma once

// Deterministic multi-process cluster harness.
//
// Every multi-process integration test in this tree used to carry its own
// copy of spawn_proc / kill_quietly / probe_free_port / await_port_open -
// 26 files duplicated probe_free_port alone. Worse than the duplication is
// what they duplicated: a free-port probe that binds, closes, and returns
// the number, leaving a window for anything else on the box to take it
// before the child binds; and `sleep_for(500ms)` standing in for "the
// worker has registered by now". That is the pre-existing timing tail that
// keeps the whole integration label advisory in CI.
//
// This replaces both patterns:
//
//   * Readiness is a CONDITION that is polled to a monotonic deadline,
//     never a sleep. The deadline is a safety bound that fails the test,
//     not a synchronisation mechanism. await_* returns as soon as the
//     condition holds, so a fast machine is fast and a loaded machine is
//     merely slower rather than wrong.
//   * Ports are held open until the child is spawned (reserve_port), which
//     shrinks the TOCTOU window to the exec itself and, more importantly,
//     makes a collision a loud bind failure in the child's log rather than
//     a silent hang.
//   * Processes are owned. The Cluster destructor reaps everything it
//     spawned, in dependency order, and an abandoned child cannot outlive
//     the test binary.
//   * On failure, every process's stdout/stderr is dumped into the gtest
//     output. A multi-process test that fails with no child logs is
//     un-diagnosable in CI, which is the other half of why these tests
//     never became gates.
//
// Faults are armed in a CHILD via the fault-injection framework's
// environment form, so a test can kill a worker at an exact point in the
// checkpoint protocol rather than at a wall-clock moment:
//
//   ClusterSpec spec;
//   spec.workers = 2;
//   Cluster c(spec);
//   c.start_coordinator();
//   c.start_worker(0);
//   c.start_worker(1, {.fault = "coordinator.before_commit_broadcast=exit:70@2"});
//   ASSERT_TRUE(c.await_workers_registered(2));

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <signal.h>
#include <spawn.h>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>
#include <sys/wait.h>

#include "clink/runtime/network/network_socket.hpp"

extern char** environ;

namespace clink::itest {

using namespace std::chrono_literals;

// How long a condition may take before the test fails. A SAFETY BOUND, not
// a synchronisation delay: every await_* returns the instant its condition
// holds. Generous because a loaded CI runner is slow, not because anything
// here waits for it.
inline constexpr auto kDefaultDeadline = 60s;

// Poll interval for conditions that cannot be waited on directly. Short
// enough not to add meaningful latency, long enough not to spin a core.
inline constexpr auto kPollInterval = 10ms;

// ---------------------------------------------------------------------------
// Condition waiting
// ---------------------------------------------------------------------------

// Poll `cond` until it holds or the monotonic deadline passes. Returns
// true if it held. steady_clock throughout: a wall-clock jump (NTP step,
// container suspend) must not shorten or lengthen a deadline.
template <typename Cond>
bool await(Cond&& cond,
           std::chrono::milliseconds timeout =
               std::chrono::duration_cast<std::chrono::milliseconds>(kDefaultDeadline)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    for (;;) {
        if (cond()) {
            return true;
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::sleep_for(kPollInterval);
    }
}

// ---------------------------------------------------------------------------
// Ports
// ---------------------------------------------------------------------------

// A port held open until the moment its owner is spawned.
//
// The old probe_free_port bound, closed, and returned the number - the
// classic bind-close-race. Holding the listener means the kernel will not
// hand the same port to another concurrent test in the same ctest -j run,
// and release() is called immediately before posix_spawn so the window is
// one exec rather than however long the test spent setting up.
class ReservedPort {
public:
    ReservedPort() {
        // listen_on takes the port by reference and writes back the one the
        // kernel actually assigned when asked for 0.
        std::uint16_t requested = 0;
        fd_ = clink::network::NetworkSocket::listen_on(requested);
        if (fd_ >= 0) {
            port_ = requested;
        }
    }
    ReservedPort(const ReservedPort&) = delete;
    ReservedPort& operator=(const ReservedPort&) = delete;
    ReservedPort(ReservedPort&& other) noexcept : fd_(other.fd_), port_(other.port_) {
        other.fd_ = -1;
    }
    ~ReservedPort() { release(); }

    [[nodiscard]] std::uint16_t port() const noexcept { return port_; }
    [[nodiscard]] bool valid() const noexcept { return port_ != 0; }

    void release() {
        if (fd_ >= 0) {
            clink::network::NetworkSocket::close(fd_);
            fd_ = -1;
        }
    }

private:
    int fd_{-1};
    std::uint16_t port_{0};
};

// True when something is accepting on `port`. The canonical "the process
// is up and listening" condition - strictly better than sleeping, because
// it is the actual thing the next step needs.
inline bool port_accepting(std::uint16_t port) {
    const int fd = clink::network::NetworkSocket::connect_to("127.0.0.1", port);
    if (fd < 0) {
        return false;
    }
    clink::network::NetworkSocket::close(fd);
    return true;
}

// ---------------------------------------------------------------------------
// Processes
// ---------------------------------------------------------------------------

struct ProcOptions {
    // A CLINK_FAULT_INJECT spec armed inside the child. This is how a test
    // kills a process at an exact point in a protocol.
    std::string fault;
    std::vector<std::pair<std::string, std::string>> env;
};

// One spawned process, with its output captured to a file so a failure can
// show what it said.
class Process {
public:
    Process() = default;
    Process(const Process&) = delete;
    Process& operator=(const Process&) = delete;
    Process(Process&& o) noexcept
        : pid_(o.pid_),
          label_(std::move(o.label_)),
          log_(std::move(o.log_)),
          argv_(std::move(o.argv_)) {
        o.pid_ = -1;
    }
    ~Process() { kill_and_reap(); }

    [[nodiscard]] pid_t pid() const noexcept { return pid_; }
    [[nodiscard]] const std::string& label() const noexcept { return label_; }
    [[nodiscard]] const std::filesystem::path& log_path() const noexcept { return log_; }

    // Non-blocking reap. Returns the exit code once the child has been
    // collected, nullopt while it is still alive.
    //
    // This exists because `::kill(pid, 0)` is NOT a liveness test for a
    // child of this process. A SIGKILLed child becomes a zombie and keeps
    // its pid-table entry until its parent reaps it, so kill(pid, 0) keeps
    // returning 0 and the process reads as "running" for ever. Every
    // kill-then-wait scenario in this suite failed that way on the first
    // run. waitpid(WNOHANG) is the actual question.
    std::optional<int> poll_exit() {
        if (pid_ <= 0) {
            return exit_code_;
        }
        int status = 0;
        if (::waitpid(pid_, &status, WNOHANG) != pid_) {
            return std::nullopt;
        }
        pid_ = -1;
        exit_code_ = WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
        return exit_code_;
    }

    [[nodiscard]] bool running() { return !poll_exit().has_value(); }

    // Spawn. Returns false if posix_spawn failed; the caller asserts.
    bool spawn(std::string label,
               const std::filesystem::path& binary,
               std::vector<std::string> argv,
               const std::filesystem::path& log_dir,
               const ProcOptions& opts = {}) {
        label_ = std::move(label);
        argv_ = std::move(argv);
        log_ = log_dir / (label_ + ".log");

        std::vector<char*> raw;
        raw.reserve(argv_.size() + 1);
        for (auto& s : argv_) {
            raw.push_back(s.data());
        }
        raw.push_back(nullptr);

        // Child env = parent env plus the per-process overrides. Built as
        // owned strings so the pointers stay valid across posix_spawn.
        std::vector<std::string> env_storage;
        for (char** e = environ; *e != nullptr; ++e) {
            env_storage.emplace_back(*e);
        }
        if (!opts.fault.empty()) {
            env_storage.push_back("CLINK_FAULT_INJECT=" + opts.fault);
        }
        for (const auto& [k, v] : opts.env) {
            env_storage.push_back(k + "=" + v);
        }
        std::vector<char*> env_raw;
        env_raw.reserve(env_storage.size() + 1);
        for (auto& s : env_storage) {
            env_raw.push_back(s.data());
        }
        env_raw.push_back(nullptr);

        // Redirect both streams into the log so a CI failure carries the
        // child's own account of what happened.
        posix_spawn_file_actions_t actions;
        posix_spawn_file_actions_init(&actions);
        posix_spawn_file_actions_addopen(
            &actions, STDOUT_FILENO, log_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        posix_spawn_file_actions_adddup2(&actions, STDOUT_FILENO, STDERR_FILENO);

        const int rc =
            posix_spawn(&pid_, binary.c_str(), &actions, nullptr, raw.data(), env_raw.data());
        posix_spawn_file_actions_destroy(&actions);
        if (rc != 0) {
            pid_ = -1;
            return false;
        }
        return true;
    }

    // SIGKILL: the point of a crash test is that nothing gets to clean up.
    void kill_hard() {
        if (pid_ > 0) {
            ::kill(pid_, SIGKILL);
        }
    }

    void signal(int sig) {
        if (pid_ > 0) {
            ::kill(pid_, sig);
        }
    }

    // Wait for exit, returning the code (or 128+signal). nullopt on timeout.
    std::optional<int> await_exit(
        std::chrono::milliseconds timeout =
            std::chrono::duration_cast<std::chrono::milliseconds>(kDefaultDeadline)) {
        std::optional<int> code;
        if (!await(
                [&] {
                    code = poll_exit();
                    return code.has_value();
                },
                timeout)) {
            return std::nullopt;
        }
        return code;
    }

    void kill_and_reap() {
        if (pid_ > 0) {
            ::kill(pid_, SIGKILL);
            int status = 0;
            ::waitpid(pid_, &status, 0);
            pid_ = -1;
            exit_code_ = WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
        }
    }

    [[nodiscard]] std::string read_log() const {
        std::ifstream in(log_);
        if (!in) {
            return "(no log)";
        }
        return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
    }

    // True when the log contains `needle`. The log-line form of a
    // readiness condition, for states the control plane does not expose
    // over a socket.
    [[nodiscard]] bool log_contains(std::string_view needle) const {
        return read_log().find(needle) != std::string::npos;
    }

private:
    pid_t pid_{-1};
    // Set once the child has been reaped, so a second query still answers.
    std::optional<int> exit_code_;
    std::string label_;
    std::filesystem::path log_;
    std::vector<std::string> argv_;
};

// ---------------------------------------------------------------------------
// Cluster
// ---------------------------------------------------------------------------

struct ClusterSpec {
    std::filesystem::path node_binary;
    std::filesystem::path root;  // scratch dir; created if empty
    int workers{1};
    int slots_per_worker{4};
    std::int64_t checkpoint_interval_ms{0};  // 0 = no periodic checkpointing
    bool http{false};
    // Run the coordinator(s) under file-based leader election. Standbys
    // hold the control port closed until they win, and workers discover
    // the active leader from <root>/ha/active-leader.json rather than
    // being told an address.
    bool ha{false};
};

// A coordinator plus N workers, owned and reaped as a unit.
class Cluster {
public:
    explicit Cluster(ClusterSpec spec) : spec_(std::move(spec)) {
        if (spec_.root.empty()) {
            const auto* test = ::testing::UnitTest::GetInstance()->current_test_info();
            const std::string tag = test != nullptr
                                        ? std::string(test->test_suite_name()) + "." + test->name()
                                        : std::string("anon");
            std::string safe;
            for (const char c : tag) {
                safe += (std::isalnum(static_cast<unsigned char>(c)) != 0) ? c : '_';
            }
            // pid + test name: ctest -j gives each test its own process, so
            // a fixed path would collide across concurrent tests.
            spec_.root = std::filesystem::temp_directory_path() /
                         ("clink_it_" + std::to_string(::getpid()) + "_" + safe);
        }
        std::filesystem::remove_all(spec_.root);
        std::filesystem::create_directories(spec_.root);
        std::filesystem::create_directories(log_dir());
        std::filesystem::create_directories(checkpoint_dir());
        if (spec_.ha) {
            std::filesystem::create_directories(ha_dir());
        }
    }

    Cluster(const Cluster&) = delete;
    Cluster& operator=(const Cluster&) = delete;

    ~Cluster() {
        // Workers first, then the coordinator: the reverse of start order,
        // so a worker does not spend its shutdown retrying a coordinator
        // that has already gone.
        for (auto& w : workers_) {
            w->kill_and_reap();
        }
        coordinator_.reset();
        for (auto& c : ha_coordinators_) {
            if (c) {
                c->kill_and_reap();
            }
        }
        ha_coordinators_.clear();
        if (!keep_artifacts_) {
            std::error_code ec;
            std::filesystem::remove_all(spec_.root, ec);
        }
    }

    [[nodiscard]] const std::filesystem::path& root() const noexcept { return spec_.root; }
    [[nodiscard]] std::filesystem::path log_dir() const { return spec_.root / "logs"; }
    [[nodiscard]] std::filesystem::path checkpoint_dir() const {
        return spec_.root / "checkpoints";
    }
    [[nodiscard]] std::filesystem::path ha_dir() const { return spec_.root / "ha"; }
    [[nodiscard]] std::uint16_t coordinator_port() const noexcept { return coordinator_port_; }
    [[nodiscard]] std::uint16_t http_port() const noexcept { return http_port_; }
    [[nodiscard]] Process& worker(std::size_t i) { return *workers_.at(i); }
    [[nodiscard]] Process& coordinator() { return *coordinator_; }

    // Keep the scratch tree (and every child log) after the test. Set
    // automatically when a test fails, so CI keeps the evidence.
    void keep_artifacts() { keep_artifacts_ = true; }

    [[nodiscard]] bool start_coordinator(const ProcOptions& opts = {}) {
        ReservedPort rpc;
        ReservedPort http;
        if (!rpc.valid()) {
            return false;
        }
        coordinator_port_ = rpc.port();
        http_port_ = spec_.http ? http.port() : 0;

        std::vector<std::string> argv{spec_.node_binary.string(),
                                      "--role=coordinator",
                                      "--bind-host=127.0.0.1",
                                      "--port=" + std::to_string(coordinator_port_)};
        if (spec_.http) {
            argv.push_back("--http-port=" + std::to_string(http_port_));
        }
        coordinator_ = std::make_unique<Process>();
        // Release immediately before spawn so the child can bind.
        rpc.release();
        http.release();
        if (!coordinator_->spawn(
                "coordinator", spec_.node_binary, std::move(argv), log_dir(), opts)) {
            return false;
        }
        return await_coordinator_ready();
    }

    [[nodiscard]] bool start_worker(std::size_t idx, const ProcOptions& opts = {}) {
        ReservedPort data;
        if (!data.valid()) {
            return false;
        }
        const auto port = data.port();
        std::vector<std::string> argv{spec_.node_binary.string(),
                                      "--role=worker",
                                      // clink_node requires --id for a worker, and the
                                      // coordinator keys registration on it: a restarted
                                      // worker must present the SAME id so it is recognised
                                      // as the same logical worker rather than a new one.
                                      "--id=worker-" + std::to_string(idx),
                                      "--coordinator-host=127.0.0.1",
                                      "--coordinator-port=" + std::to_string(coordinator_port_),
                                      "--port=" + std::to_string(port),
                                      "--slots=" + std::to_string(spec_.slots_per_worker)};
        if (workers_.size() <= idx) {
            workers_.resize(idx + 1);
        }
        workers_[idx] = std::make_unique<Process>();
        worker_ports_.resize(std::max(worker_ports_.size(), idx + 1));
        worker_ports_[idx] = port;
        data.release();
        return workers_[idx]->spawn(
            "worker-" + std::to_string(idx), spec_.node_binary, std::move(argv), log_dir(), opts);
    }

    // Restart a worker that died (or was killed) at the same index. The
    // port is re-reserved, because the old one may still be in TIME_WAIT.
    [[nodiscard]] bool restart_worker(std::size_t idx, const ProcOptions& opts = {}) {
        if (idx < workers_.size() && workers_[idx]) {
            workers_[idx]->kill_and_reap();
        }
        return start_worker(idx, opts);
    }

    // --- HA: two coordinators, one lock ------------------------------------
    //
    // Both processes are given the SAME control port. Only the one that
    // wins the lock on <ha_dir>/leader.lock binds it; the other sits on its
    // poll thread with the port closed. That is what makes "start two, kill
    // one" a real failover rather than a port collision.

    // Bring up `n` coordinator processes under leader election and wait for
    // one to win. Requires ClusterSpec::ha.
    [[nodiscard]] bool start_ha_coordinators(std::size_t n, const ProcOptions& opts = {}) {
        if (!spec_.ha || n == 0) {
            return false;
        }
        ReservedPort rpc;
        if (!rpc.valid()) {
            return false;
        }
        coordinator_port_ = rpc.port();
        rpc.release();
        for (std::size_t i = 0; i < n; ++i) {
            std::vector<std::string> argv{spec_.node_binary.string(),
                                          "--role=coordinator",
                                          "--bind-host=127.0.0.1",
                                          "--advertise-host=127.0.0.1",
                                          "--port=" + std::to_string(coordinator_port_),
                                          "--ha-dir=" + ha_dir().string()};
            ha_coordinators_.push_back(std::make_unique<Process>());
            if (!ha_coordinators_.back()->spawn("coordinator-" + std::to_string(i),
                                                spec_.node_binary,
                                                std::move(argv),
                                                log_dir(),
                                                opts)) {
                return false;
            }
        }
        return await_leader_elected() && await_coordinator_ready();
    }

    // Index of the process currently holding leadership, by its own log.
    // A coordinator that has been superseded keeps its old "became leader"
    // line, so liveness is checked too.
    [[nodiscard]] std::optional<std::size_t> current_leader_index() const {
        for (std::size_t i = 0; i < ha_coordinators_.size(); ++i) {
            const auto& p = ha_coordinators_[i];
            if (p && p->running() &&
                p->read_log().find("coordinator became leader") != std::string::npos) {
                return i;
            }
        }
        return std::nullopt;
    }

    // The epoch a coordinator announced when it took leadership, parsed out
    // of its own startup line. Absent if it never led.
    [[nodiscard]] std::optional<std::uint64_t> announced_epoch(std::size_t idx) const {
        if (idx >= ha_coordinators_.size() || !ha_coordinators_[idx]) {
            return std::nullopt;
        }
        const auto text = ha_coordinators_[idx]->read_log();
        const std::string needle = "coordinator became leader (epoch=";
        const auto pos = text.find(needle);
        if (pos == std::string::npos) {
            return std::nullopt;
        }
        try {
            return std::stoull(text.substr(pos + needle.size()));
        } catch (const std::exception&) {
            return std::nullopt;
        }
    }

    [[nodiscard]] bool await_leader_elected(
        std::chrono::milliseconds t =
            std::chrono::duration_cast<std::chrono::milliseconds>(kDefaultDeadline)) const {
        return await([this] { return current_leader_index().has_value(); }, t);
    }

    // Kill the current leader and wait for a survivor to take over. Returns
    // the index of the killed process.
    [[nodiscard]] std::optional<std::size_t> kill_leader_and_await_failover(
        std::chrono::milliseconds t = std::chrono::seconds{20}) {
        const auto before = current_leader_index();
        if (!before.has_value()) {
            return std::nullopt;
        }
        ha_coordinators_[*before]->kill_and_reap();
        const bool took_over = await(
            [this, before] {
                const auto now = current_leader_index();
                return now.has_value() && *now != *before;
            },
            t);
        return took_over ? before : std::nullopt;
    }

    // Start a worker that finds the coordinator through leader election
    // rather than a fixed address. This is the path a real HA deployment
    // uses, and the only one that survives a failover.
    [[nodiscard]] bool start_ha_worker(std::size_t idx, const ProcOptions& opts = {}) {
        ReservedPort data;
        if (!data.valid()) {
            return false;
        }
        const auto port = data.port();
        std::vector<std::string> argv{spec_.node_binary.string(),
                                      "--role=worker",
                                      "--id=worker-" + std::to_string(idx),
                                      "--ha-dir=" + ha_dir().string(),
                                      "--port=" + std::to_string(port),
                                      "--slots=" + std::to_string(spec_.slots_per_worker)};
        if (workers_.size() <= idx) {
            workers_.resize(idx + 1);
        }
        workers_[idx] = std::make_unique<Process>();
        worker_ports_.resize(std::max(worker_ports_.size(), idx + 1));
        worker_ports_[idx] = port;
        data.release();
        return workers_[idx]->spawn(
            "worker-" + std::to_string(idx), spec_.node_binary, std::move(argv), log_dir(), opts);
    }

    // Restart an HA worker at the same index, rediscovering the leader.
    //
    // A worker exits on coordinator disconnect by design - there is no
    // worker-side re-register path, so a supervisor is what heals the
    // cluster. This is the harness playing that supervisor, and it is
    // needed for any failover test: without it the surviving coordinator
    // has no worker to redeploy onto and the job never resumes.
    [[nodiscard]] bool restart_worker_ha(std::size_t idx, const ProcOptions& opts = {}) {
        if (idx < workers_.size() && workers_[idx]) {
            workers_[idx]->kill_and_reap();
        }
        return start_ha_worker(idx, opts);
    }

    // The epoch a worker bound to, from its own HA discovery line. This is
    // the value the fencing check compares every later frame against.
    [[nodiscard]] std::optional<std::uint64_t> worker_discovered_epoch(std::size_t idx) const {
        if (idx >= workers_.size() || !workers_[idx]) {
            return std::nullopt;
        }
        const auto text = workers_[idx]->read_log();
        const std::string needle = "(epoch=";
        const auto pos = text.rfind(needle);
        if (pos == std::string::npos) {
            return std::nullopt;
        }
        try {
            return std::stoull(text.substr(pos + needle.size()));
        } catch (const std::exception&) {
            return std::nullopt;
        }
    }

    // Frames a worker refused because they came from a superseded
    // coordinator. Non-zero means split brain actually happened.
    [[nodiscard]] std::size_t worker_fenced_frames(std::size_t idx) const {
        if (idx >= workers_.size() || !workers_[idx]) {
            return 0;
        }
        const auto text = workers_[idx]->read_log();
        std::size_t n = 0;
        std::size_t pos = 0;
        const std::string needle = "[worker.fencing]";
        while ((pos = text.find(needle, pos)) != std::string::npos) {
            ++n;
            pos += needle.size();
        }
        return n;
    }

    // --- readiness conditions ---------------------------------------------
    //
    // Each is the actual state the next step depends on, polled to a
    // deadline. None of them is a sleep.

    [[nodiscard]] bool await_coordinator_ready(
        std::chrono::milliseconds t =
            std::chrono::duration_cast<std::chrono::milliseconds>(kDefaultDeadline)) const {
        return await([this] { return port_accepting(coordinator_port_); }, t);
    }

    // Worker registration is observable in the coordinator's log under the
    // "coordinator.register" source (Coordinator::handle_register). Counting
    // occurrences rather than looking for one keeps the condition honest
    // when workers register out of order.
    //
    // This is the condition every one of these tests used to approximate
    // with sleep_for(500ms) - which is both slower than necessary on an
    // idle box and wrong on a loaded one.
    [[nodiscard]] bool await_workers_registered(
        std::size_t n,
        std::chrono::milliseconds t =
            std::chrono::duration_cast<std::chrono::milliseconds>(kDefaultDeadline)) const {
        return await([this, n] { return count_in_coordinator_log("[coordinator.register]") >= n; },
                     t);
    }

    // The coordinator creates <checkpoint_dir>/<job_id>/ once a job is
    // deployed and checkpointing. That is the observable "the job is
    // actually running" signal a test needs before it can meaningfully
    // fault-inject: killing during the deploy window instead tests
    // undefined behaviour (the submission is simply rejected).
    [[nodiscard]] bool await_job_checkpointing(
        std::chrono::milliseconds t =
            std::chrono::duration_cast<std::chrono::milliseconds>(kDefaultDeadline)) const {
        return await(
            [this] {
                std::error_code ec;
                if (!std::filesystem::exists(checkpoint_dir(), ec)) {
                    return false;
                }
                for (const auto& e : std::filesystem::directory_iterator(checkpoint_dir(), ec)) {
                    if (e.is_directory()) {
                        return true;
                    }
                }
                return false;
            },
            t);
    }

    // A COMPLETED-N marker on disk IS the definition of "checkpoint N
    // reached global completion", so this is the exact condition rather
    // than a proxy for it.
    [[nodiscard]] bool await_checkpoint_completed(
        std::uint64_t job_id,
        std::uint64_t checkpoint_id,
        std::chrono::milliseconds t =
            std::chrono::duration_cast<std::chrono::milliseconds>(kDefaultDeadline)) const {
        const auto marker = checkpoint_dir() / "_jobs" / std::to_string(job_id) /
                            ("COMPLETED-" + std::to_string(checkpoint_id));
        return await(
            [&marker] {
                std::error_code ec;
                return std::filesystem::exists(marker, ec);
            },
            t);
    }

    // Any completed checkpoint for the job, whichever id arrives first.
    [[nodiscard]] std::optional<std::uint64_t> await_any_checkpoint_completed(
        std::uint64_t job_id,
        std::chrono::milliseconds t =
            std::chrono::duration_cast<std::chrono::milliseconds>(kDefaultDeadline)) const {
        std::optional<std::uint64_t> found;
        const auto job_dir = checkpoint_dir() / "_jobs" / std::to_string(job_id);
        await(
            [&] {
                std::error_code ec;
                if (!std::filesystem::exists(job_dir, ec)) {
                    return false;
                }
                for (const auto& e : std::filesystem::directory_iterator(job_dir, ec)) {
                    const auto name = e.path().filename().string();
                    if (name.rfind("COMPLETED-", 0) == 0) {
                        try {
                            found = std::stoull(name.substr(10));
                            return true;
                        } catch (const std::exception&) {
                        }
                    }
                }
                return false;
            },
            t);
        return found;
    }

    // Wait until a spawned process has reached a named fault point at
    // least `n` times. Requires the child to have been given an
    // `observe` (or any) rule for that point, and the point's hits to be
    // visible - which for a child process means through its log.
    [[nodiscard]] bool await_process_gone(
        std::size_t worker_idx,
        std::chrono::milliseconds t =
            std::chrono::duration_cast<std::chrono::milliseconds>(kDefaultDeadline)) {
        return await([this, worker_idx] { return !workers_.at(worker_idx)->running(); }, t);
    }

    // Counts across the single coordinator, or across every HA coordinator
    // process - a registration that lands on whichever node is leading is
    // still a registration.
    [[nodiscard]] std::size_t count_in_coordinator_log(std::string_view needle) const {
        const auto count_one = [needle](const Process& p) {
            const auto text = p.read_log();
            std::size_t n = 0;
            std::size_t pos = 0;
            while ((pos = text.find(needle, pos)) != std::string::npos) {
                ++n;
                pos += needle.size();
            }
            return n;
        };
        std::size_t total = 0;
        if (coordinator_) {
            total += count_one(*coordinator_);
        }
        for (const auto& c : ha_coordinators_) {
            if (c) {
                total += count_one(*c);
            }
        }
        return total;
    }

    // --- diagnostics -------------------------------------------------------

    // Dump every process log into the gtest output. Call from a failure
    // path (or let ScopedDiagnostics do it) - a multi-process failure with
    // no child logs cannot be diagnosed from a CI transcript.
    void dump_logs() const {
        const auto emit = [](const Process& p) {
            std::cerr << "\n===== " << p.label() << " (" << p.log_path().string() << ") =====\n"
                      << p.read_log() << "\n";
        };
        if (coordinator_) {
            emit(*coordinator_);
        }
        for (const auto& c : ha_coordinators_) {
            if (c) {
                emit(*c);
            }
        }
        for (const auto& w : workers_) {
            if (w) {
                emit(*w);
            }
        }
    }

private:
    ClusterSpec spec_;
    std::unique_ptr<Process> coordinator_;
    std::vector<std::unique_ptr<Process>> ha_coordinators_;
    std::vector<std::unique_ptr<Process>> workers_;
    std::vector<std::uint16_t> worker_ports_;
    std::uint16_t coordinator_port_{0};
    std::uint16_t http_port_{0};
    bool keep_artifacts_{false};
};

// RAII: on a failing test, dump the cluster's logs and keep its scratch
// tree. Declare one right after the Cluster.
class ScopedDiagnostics {
public:
    explicit ScopedDiagnostics(Cluster& c) : cluster_(c) {}
    ScopedDiagnostics(const ScopedDiagnostics&) = delete;
    ScopedDiagnostics& operator=(const ScopedDiagnostics&) = delete;
    ~ScopedDiagnostics() {
        if (::testing::Test::HasFailure()) {
            cluster_.dump_logs();
            cluster_.keep_artifacts();
            std::cerr << "\n[harness] artifacts kept at " << cluster_.root().string() << "\n";
        }
    }

private:
    Cluster& cluster_;
};

}  // namespace clink::itest
