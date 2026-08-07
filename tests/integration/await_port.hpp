#pragma once

// Wait until a spawned node is accepting on its port.
//
// Exists because the pre-harness integration tests all open the same way:
// spawn a coordinator, sleep 200-300ms "to give it time to bind", then use
// it. That sleep is a guess at how long a process takes to start, and it is
// the single largest source of noise in the suite - each of those tests
// passes alone in under two seconds and fails when 109 multi-process tests
// run back to back, or when a container build is running on the same
// machine. The failure is always reported as a defect in whatever the test
// was actually checking.
//
// The condition is available and cheap: connect to the port. This is not a
// synchronisation delay - it returns the instant the listener is up - and
// the timeout is a failure bound, not a wait.
//
// The full harness (cluster_harness.hpp) already does this and more, but
// converting these tests to it is a larger change than replacing a sleep;
// this is the small step that removes the flakiness now. New tests should
// use the harness.

#include <chrono>
#include <cstdint>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <set>
#include <spawn.h>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

extern "C" char** environ;

namespace clink::itest {

// Wait for an arbitrary condition, polling.
//
// Same contract as `await` in cluster_harness.hpp - it returns the instant the
// condition holds, and the timeout is a FAILURE BOUND, not a wait - but usable
// from the pre-harness tests without pulling the whole harness in.
//
// Written because a fixed sleep after a job submit is not just noisy, it encodes
// a false premise. Those sleeps were set from macOS timings, where a job plugin
// is about 5 MB; on Linux the same module is about 84 MB, because each one
// statically links clink_core, so the submit takes around 2.7 seconds and every
// "sleep 2s then assert the job exists" assertion was measuring the wrong thing.
// A generous bound on an observable condition cannot go stale that way.
template <typename Cond>
[[nodiscard]] inline bool await_condition(
    Cond&& cond,
    std::chrono::milliseconds timeout = std::chrono::seconds{30},
    std::chrono::milliseconds poll = std::chrono::milliseconds{50}) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    for (;;) {
        if (cond()) {
            return true;
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::sleep_for(poll);
    }
}

[[nodiscard]] inline bool await_port_accepting(
    std::uint16_t port, std::chrono::milliseconds timeout = std::chrono::seconds{15}) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    for (;;) {
        const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd >= 0) {
            sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_port = htons(port);
            addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            const bool ok = ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0;
            ::close(fd);
            if (ok) {
                return true;
            }
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
    }
}

// Spawn a child with BOTH streams captured into `log_path`.
//
// The pre-harness tests all define their own spawn_proc with `nullptr` file
// actions, so the child's output goes to the test's own stdout and there is
// nothing to observe. That is WHY those tests sleep: they have no way to see that
// a worker registered, so they guess at how long it takes. A captured log turns
// that guess into a condition - see await_log_matches below.
//
// Returns the pid, or -1.
inline pid_t spawn_logged(const std::vector<std::string>& argv,
                          const std::filesystem::path& binary,
                          const std::filesystem::path& log_path) {
    std::vector<char*> raw;
    raw.reserve(argv.size() + 1);
    for (const auto& a : argv) {
        raw.push_back(const_cast<char*>(a.c_str()));
    }
    raw.push_back(nullptr);

    posix_spawn_file_actions_t actions;
    posix_spawn_file_actions_init(&actions);
    posix_spawn_file_actions_addopen(
        &actions, STDOUT_FILENO, log_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    posix_spawn_file_actions_adddup2(&actions, STDOUT_FILENO, STDERR_FILENO);

    pid_t pid = -1;
    const int rc = posix_spawn(&pid, binary.c_str(), &actions, nullptr, raw.data(), environ);
    posix_spawn_file_actions_destroy(&actions);
    return rc == 0 ? pid : -1;
}

// How many times `needle` appears in the file at `path`. 0 if it does not exist.
[[nodiscard]] inline std::size_t log_count(const std::filesystem::path& path,
                                           std::string_view needle) {
    std::ifstream in(path);
    if (!in || needle.empty()) {
        return 0;
    }
    const std::string body((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    std::size_t n = 0;
    for (std::size_t pos = body.find(needle); pos != std::string::npos;
         pos = body.find(needle, pos + needle.size())) {
        ++n;
    }
    return n;
}

// Wait until `needle` appears at least `want` times in the log at `path`.
//
// This is what replaces "sleep 400ms after spawning the workers". The
// coordinator logs a line per registration, so the number of registrations is
// directly observable; sleeping instead means a slow machine submits a job before
// a slot exists, and the coordinator rejects it - which reads as a submission bug
// rather than as the race it is.
[[nodiscard]] inline bool await_log_matches(
    const std::filesystem::path& path,
    std::string_view needle,
    std::size_t want = 1,
    std::chrono::milliseconds timeout = std::chrono::seconds{30}) {
    return await_condition([&] { return log_count(path, needle) >= want; }, timeout);
}

// Verify a checkpoint directory ACROSS subtasks, not file by file.
//
// `clink checkpoint-verify` answers "is this file intact" - payload against its
// sidecar. That cannot see the failure this exists for: F65 was a snapshot file
// appearing for a checkpoint it was never part of, written by a later topology into
// a directory the checkpoint's restore point still named. Every file involved was
// individually valid; what was wrong was the SET.
//
// The coordinator now records, in each COMPLETED-<id> marker, the state generation
// holding that checkpoint and the subtask indices that acked it. So the set is
// checkable: under <base>/v<generation>/, exactly those subtasks may hold a snapshot
// for that id.
//
// EXTRAS are the assertion. A subtask directory holding a file for a checkpoint it
// never participated in is always wrong. Missing files are reported but not failed:
// a stateless subtask acks a checkpoint without writing state, so absence is
// legitimate and asserting on it would be a false positive.
//
// Returns a human-readable description of every violation; empty means consistent.
[[nodiscard]] inline std::vector<std::string> checkpoint_set_violations(
    const std::filesystem::path& checkpoint_dir) {
    std::vector<std::string> bad;
    std::error_code ec;
    const auto jobs_dir = checkpoint_dir / "_jobs";
    if (!std::filesystem::exists(jobs_dir, ec)) {
        return bad;
    }
    for (const auto& job_entry : std::filesystem::directory_iterator(jobs_dir, ec)) {
        if (ec || !job_entry.is_directory(ec)) {
            continue;
        }
        for (const auto& marker : std::filesystem::directory_iterator(job_entry.path(), ec)) {
            if (ec || !marker.is_regular_file(ec)) {
                continue;
            }
            const auto name = marker.path().filename().string();
            if (!name.starts_with("COMPLETED-")) {
                continue;
            }
            std::ifstream in(marker.path());
            if (!in) {
                continue;
            }
            std::string body((std::istreambuf_iterator<char>(in)),
                             std::istreambuf_iterator<char>());
            const auto field = [&body](std::string_view key) -> std::string {
                const auto k = std::string{key} + "=";
                const auto pos = body.find(k);
                if (pos == std::string::npos) {
                    return {};
                }
                const auto start = pos + k.size();
                const auto end = body.find('\n', start);
                return body.substr(start, end == std::string::npos ? end : end - start);
            };
            const auto gen_s = field("generation");
            const auto subs_s = field("subtasks");
            const auto ckpt_s = name.substr(std::string("COMPLETED-").size());
            if (gen_s.empty()) {
                continue;  // marker predates the participant record; nothing to check
            }
            std::set<std::string> expected;
            for (std::size_t p = 0; p <= subs_s.size();) {
                const auto comma = subs_s.find(',', p);
                const auto tok = subs_s.substr(p, comma == std::string::npos ? comma : comma - p);
                if (!tok.empty()) {
                    expected.insert(tok);
                }
                if (comma == std::string::npos) {
                    break;
                }
                p = comma + 1;
            }
            // Anything holding a snapshot for this id, in ANY generation.
            const std::string snap = "checkpoint-" + ckpt_s + ".snap";
            for (const auto& gen_entry : std::filesystem::directory_iterator(checkpoint_dir, ec)) {
                if (ec || !gen_entry.is_directory(ec)) {
                    continue;
                }
                const auto gname = gen_entry.path().filename().string();
                if (gname.empty() || gname[0] != 'v') {
                    continue;  // _jobs and anything else that is not a generation
                }
                for (const auto& sub : std::filesystem::directory_iterator(gen_entry.path(), ec)) {
                    if (ec || !sub.is_directory(ec)) {
                        continue;
                    }
                    if (!std::filesystem::exists(sub.path() / snap, ec)) {
                        continue;
                    }
                    const auto idx = sub.path().filename().string();
                    const bool right_generation = (gname == "v" + gen_s);
                    if (!right_generation || expected.count(idx) == 0) {
                        bad.push_back("checkpoint " + ckpt_s + " has a snapshot at " + gname + "/" +
                                      idx + " but completed with generation=" + gen_s +
                                      " subtasks=" + subs_s +
                                      ". A file for a checkpoint the subtask never participated "
                                      "in means a later topology wrote where an earlier one's "
                                      "restore point still points.");
                    }
                }
            }
        }
    }
    return bad;
}

}  // namespace clink::itest
