#include "clink/cluster/ha_coordinator.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>
#include <unistd.h>
#include <vector>

#include <sys/file.h>
#include <sys/wait.h>

// For metadata_write_allowed: the epoch rule this file now shares with the
// coordinator's metadata writes, so the two cannot drift apart.
#include "clink/cluster/coordinator.hpp"
#include "clink/http/json_writer.hpp"
#include "clink/metrics/orchestration_metrics.hpp"
#include "clink/runtime/log_buffer.hpp"

namespace clink::cluster {

namespace {

std::int64_t unix_ms_now() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

// Does this directory's filesystem actually honour POSIX write locks between
// processes?
//
// Leadership here IS an fcntl write lock on <ha_dir>/leader.lock, so the whole
// fencing scheme rests on one assumption: that a second process asking for the
// same lock is refused. On a filesystem that does not implement locking, every
// coordinator's F_SETLK "succeeds", every one of them believes it leads, and each
// announces a higher epoch than the last. That is a silent split brain - not a
// crash, not a refusal, just two live leaders and workers registering with
// whichever they read last.
//
// It is not a hypothetical. It was reproduced by accident on a macOS Docker bind
// mount while chasing something else: two coordinators sharing one HA directory
// took leadership at epoch 1 and epoch 2 simultaneously, and the only visible
// symptom was the superseded one logging refusals. Anyone putting an HA directory
// on a bind mount, an NFS export with locking disabled, or a 9p/virtiofs share
// gets the same, and gets no warning.
//
// The probe has to fork, because POSIX record locks are per-PROCESS: a second
// descriptor in this process would simply replace our own lock and report success
// on any filesystem, which measures nothing. The child does nothing but open,
// fcntl and _exit - no allocation, no logging, no locks inherited from the parent
// - so forking from a process that already has threads is safe here.
//
// Returns nullopt when the probe itself could not run (the parent's own lock
// failed, or fork failed). That is inconclusive, not a verdict, and is treated as
// such by the caller: an unusable probe must not condemn a working filesystem.
std::optional<bool> filesystem_honours_locks(const std::string& ha_dir) {
    const std::string probe_path = ha_dir + "/.lock-probe";
    // The child may only use syscalls, so its path argument is built here.
    const std::vector<char> probe_cstr(probe_path.begin(), probe_path.end() + 1);

    const int fd = ::open(probe_path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        return std::nullopt;
    }
    struct flock fl{};
    fl.l_type = F_WRLCK;
    fl.l_whence = SEEK_SET;
    fl.l_start = 0;
    fl.l_len = 0;
    if (::fcntl(fd, F_SETLK, &fl) != 0) {
        // Something else holds it - another coordinator probing concurrently, most
        // likely. Inconclusive rather than a failure.
        ::close(fd);
        return std::nullopt;
    }

    const pid_t child = ::fork();
    if (child < 0) {
        ::close(fd);
        std::error_code ec;
        std::filesystem::remove(probe_path, ec);
        return std::nullopt;
    }
    if (child == 0) {
        // Syscalls only from here.
        const int cfd = ::open(probe_cstr.data(), O_RDWR);
        if (cfd < 0) {
            ::_exit(2);
        }
        struct flock cfl{};
        cfl.l_type = F_WRLCK;
        cfl.l_whence = SEEK_SET;
        cfl.l_start = 0;
        cfl.l_len = 0;
        // 0 means the lock was GRANTED while the parent holds it, which is the
        // broken case.
        ::_exit(::fcntl(cfd, F_SETLK, &cfl) == 0 ? 0 : 1);
    }

    int status = 0;
    while (::waitpid(child, &status, 0) < 0 && errno == EINTR) {
    }
    fl.l_type = F_UNLCK;
    (void)::fcntl(fd, F_SETLK, &fl);
    ::close(fd);
    std::error_code ec;
    std::filesystem::remove(probe_path, ec);

    if (!WIFEXITED(status)) {
        return std::nullopt;
    }
    switch (WEXITSTATUS(status)) {
        case 0:
            return false;  // granted to a second process: locking is not honoured
        case 1:
            return true;  // refused, as it must be
        default:
            return std::nullopt;  // the child could not open the probe
    }
}

}  // namespace

// Separated from the probe so the decision can be tested exhaustively without a
// filesystem that lacks locking - there is no way to conjure one of those in a
// unit test, and a test-only hook in the product would be worse than the gap.
//
// Only a PROVEN-unsafe filesystem refuses. An inconclusive probe must not condemn
// a working one: the probe fails inconclusively when another coordinator holds the
// probe file, which is normal during a concurrent start.
bool ha_should_refuse_leadership(std::optional<bool> locks_honoured, bool allow_unsafe) {
    if (allow_unsafe) {
        return false;
    }
    return locks_honoured.has_value() && !*locks_honoured;
}

namespace {

class FileHaCoordinator final : public HaCoordinator {
public:
    FileHaCoordinator(std::string ha_dir,
                      LeaderEndpoint advertise,
                      std::chrono::milliseconds poll_interval,
                      bool allow_unsafe_locks)
        : ha_dir_(std::move(ha_dir)),
          advertise_(std::move(advertise)),
          poll_interval_(poll_interval),
          allow_unsafe_locks_(allow_unsafe_locks) {
        std::error_code ec;
        std::filesystem::create_directories(ha_dir_, ec);
        if (ec) {
            throw std::runtime_error("FileHaCoordinator: cannot create " + ha_dir_ + ": " +
                                     ec.message());
        }
    }

    ~FileHaCoordinator() override { stop(); }

    void start() override {
        bool expected = false;
        if (!started_.compare_exchange_strong(expected, true))
            return;
        // Once, before the poll loop can acquire anything. A filesystem that does
        // not honour the lock cannot host leadership at all, so finding out after
        // taking it would be too late.
        const auto honoured = filesystem_honours_locks(ha_dir_);
        refuse_leadership_ = ha_should_refuse_leadership(honoured, allow_unsafe_locks_);
        if (refuse_leadership_) {
            clink::log::error(
                "coordinator.ha",
                "refusing to stand for leadership: " + ha_dir_ +
                    " is on a filesystem that GRANTED the leader lock to a second process while "
                    "this one held it, so the lock cannot fence anything and two coordinators "
                    "would both believe they lead. Put the HA directory on a local filesystem, "
                    "or on a network filesystem with locking enabled; bind mounts, 9p/virtiofs "
                    "shares and NFS exports mounted nolock are the usual causes. Pass "
                    "--ha-allow-unsafe-locks to proceed anyway and accept the risk of split "
                    "brain.");
        } else if (honoured.has_value() && !*honoured) {
            clink::log::warn("coordinator.ha",
                             ha_dir_ +
                                 " does not honour POSIX write locks and "
                                 "--ha-allow-unsafe-locks was given: leadership is NOT fenced, "
                                 "and two coordinators may both believe they lead.");
        }
        poll_thread_ = std::thread([this] { poll_loop_(); });
    }

    void stop() override {
        if (!started_.exchange(false))
            return;
        stop_.store(true, std::memory_order_release);
        if (poll_thread_.joinable())
            poll_thread_.join();
        release_lock_();
    }

    bool is_leader() const noexcept override { return is_leader_.load(std::memory_order_acquire); }

    std::uint64_t epoch() const noexcept override { return epoch_.load(std::memory_order_acquire); }

    std::optional<LeaderEndpoint> current_leader_endpoint() override {
        // Parse <ha_dir>/active-leader.json. Format is intentionally
        // hand-crafted (matches JsonWriter output below) so we don't
        // need a full JSON parser dependency here.
        std::ifstream in(active_leader_path_());
        if (!in)
            return std::nullopt;
        std::string body((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        LeaderEndpoint ep;
        ep.host = extract_string_(body, "host");
        ep.port = static_cast<std::uint16_t>(extract_uint_(body, "port"));
        ep.epoch = extract_uint_(body, "epoch");
        ep.updated_unix_ms = static_cast<std::int64_t>(extract_uint_(body, "updated_unix_ms"));
        if (ep.host.empty() || ep.port == 0)
            return std::nullopt;
        return ep;
    }

    void set_on_become_leader(LeaderCallback cb) override {
        std::lock_guard lock(cb_mu_);
        on_become_leader_ = std::move(cb);
    }

private:
    std::string lock_path_() const { return ha_dir_ + "/leader.lock"; }
    std::string active_leader_path_() const { return ha_dir_ + "/active-leader.json"; }

    void poll_loop_() {
        while (!stop_.load(std::memory_order_acquire)) {
            if (refuse_leadership_) {
                // A standby that will never stand. It still serves
                // current_leader_endpoint() so a worker can be told where the
                // leader is; it simply never claims to be one.
                std::this_thread::sleep_for(poll_interval_);
                continue;
            }
            if (!is_leader()) {
                try_acquire_();
            } else {
                refresh_active_leader_();
            }
            std::this_thread::sleep_for(poll_interval_);
        }
    }

    void try_acquire_() {
        // Open (and keep open) the lock file. fcntl F_SETLK is non-
        // blocking: returns success or EAGAIN/EACCES if someone else
        // holds the lock.
        if (lock_fd_ < 0) {
            lock_fd_ = ::open(lock_path_().c_str(), O_RDWR | O_CREAT, 0644);
            if (lock_fd_ < 0)
                return;  // try again next poll
        }
        struct flock fl{};
        fl.l_type = F_WRLCK;
        fl.l_whence = SEEK_SET;
        fl.l_start = 0;
        fl.l_len = 0;
        if (::fcntl(lock_fd_, F_SETLK, &fl) != 0) {
            // Held by another process. Don't close the fd - re-trying
            // on the same fd is cheap and avoids a TOCTOU window
            // around the file's existence.
            return;
        }
        // Acquired. The epoch must be monotonic across LEADERSHIPS, not
        // across this process's lifetime.
        //
        // It used to be a bare per-process counter, and that made fencing
        // inert exactly where it matters: a standby is a FRESH PROCESS, its
        // counter starts at zero, so every new leader announced epoch 1 -
        // the same epoch the leader it displaced had been stamping on every
        // frame. Nothing could ever be refused. Caught by
        // HaFailoverTest.FailoverAdvancesTheEpoch, which killed a real
        // leader and found the successor claiming the same number.
        //
        // The previous leader's epoch is recorded in active-leader.json, so
        // read it and go above it. This runs while holding the lock, after
        // acquisition, so no other leader can be writing that file. The
        // process-local counter is still taken into account for the case
        // where THIS process regains leadership after the file was removed.
        std::uint64_t prior = epoch_.load(std::memory_order_acquire);
        if (const auto previous = current_leader_endpoint(); previous.has_value()) {
            prior = std::max(prior, previous->epoch);
        }
        const auto new_epoch = prior + 1;
        epoch_.store(new_epoch, std::memory_order_release);
        is_leader_.store(true, std::memory_order_release);
        clink::metrics::orch::ha_leader_takeover();
        refresh_active_leader_();
        LeaderCallback cb;
        {
            std::lock_guard lock(cb_mu_);
            cb = on_become_leader_;
        }
        if (cb) {
            try {
                cb(new_epoch);
            } catch (...) {
                // Best-effort: a throwing callback must not crash the
                // coordinator thread.
            }
        }
    }

    void refresh_active_leader_() {
        // Do not overwrite a record written by a LATER epoch.
        //
        // This is the file a worker reads to find the coordinator to register
        // with, and it was previously written unconditionally on every poll of
        // the leadership thread - unlike the job manifests and history records,
        // which go through the coordinator's fenced_write_file. In a split brain
        // that made it the weakest link: both coordinators refresh it every
        // poll, so it flapped between them, and a worker starting in the wrong
        // window registered with the SUPERSEDED coordinator and bound its stale
        // epoch. After that the worker-side epoch check cannot help - the
        // worker's bound epoch IS the stale one, so every subsequent frame from
        // the coordinator that lost leadership looks authoritative.
        //
        // Measured before the guard, with two live leaders at epochs 1 and 2:
        // the epoch-1 coordinator won 19 of 40 samples of the file and the
        // recorded epoch regressed from 2 to 1.
        //
        // The rule is metadata_write_allowed, shared with the coordinator's
        // metadata writes rather than restated, so the two cannot drift. Note
        // it compares the epoch parsed out of THIS file ("epoch"), not the
        // "coordinator_epoch" key that metadata_stored_epoch looks for; the two
        // records spell it differently.
        const auto mine = epoch_.load(std::memory_order_acquire);
        if (const auto existing = current_leader_endpoint();
            existing.has_value() && !metadata_write_allowed(mine, existing->epoch)) {
            clink::log::error(
                "coordinator.ha",
                "refusing to rewrite active-leader.json: it names epoch " +
                    std::to_string(existing->epoch) + " and this coordinator is epoch " +
                    std::to_string(mine) +
                    ". Leadership has moved and this process is no longer authoritative; it is "
                    "still holding a lock and serving, which is a split brain.");
            return;
        }

        // Atomic write: <path>.tmp then rename. Avoids a reader seeing
        // a partial JSON file.
        clink::http::JsonWriter w;
        w.begin_object();
        w.kv("host", advertise_.host);
        w.kv("port", static_cast<std::int64_t>(advertise_.port));
        w.kv("epoch", static_cast<std::int64_t>(epoch_.load(std::memory_order_acquire)));
        w.kv("updated_unix_ms", static_cast<std::int64_t>(unix_ms_now()));
        w.end_object();
        const auto tmp = active_leader_path_() + ".tmp";
        {
            std::ofstream out(tmp, std::ios::trunc);
            if (!out)
                return;
            out << w.str();
        }
        std::error_code ec;
        std::filesystem::rename(tmp, active_leader_path_(), ec);
    }

    void release_lock_() {
        if (lock_fd_ >= 0) {
            // Closing the fd releases the fcntl lock automatically.
            ::close(lock_fd_);
            lock_fd_ = -1;
        }
        is_leader_.store(false, std::memory_order_release);
    }

    // Hand-rolled JSON extractors. Format matches JsonWriter output:
    //   "key":"value"   or   "key":12345
    // Robust enough for the file we write; doesn't claim to be a
    // general parser.
    static std::string extract_string_(const std::string& body, const std::string& key) {
        const auto needle = "\"" + key + "\":\"";
        auto pos = body.find(needle);
        if (pos == std::string::npos)
            return {};
        pos += needle.size();
        const auto end = body.find('"', pos);
        if (end == std::string::npos)
            return {};
        return body.substr(pos, end - pos);
    }
    static std::uint64_t extract_uint_(const std::string& body, const std::string& key) {
        const auto needle = "\"" + key + "\":";
        auto pos = body.find(needle);
        if (pos == std::string::npos)
            return 0;
        pos += needle.size();
        if (pos >= body.size() || body[pos] == '"')
            return 0;
        try {
            return std::stoull(body.substr(pos));
        } catch (...) {
            return 0;
        }
    }

    std::string ha_dir_;
    LeaderEndpoint advertise_;
    std::chrono::milliseconds poll_interval_;
    int lock_fd_{-1};
    std::thread poll_thread_;
    std::atomic<bool> started_{false};
    // Set once in start(), read only by the poll loop it starts afterwards.
    bool allow_unsafe_locks_{false};
    bool refuse_leadership_{false};
    std::atomic<bool> stop_{false};
    std::atomic<bool> is_leader_{false};
    std::atomic<std::uint64_t> epoch_{0};
    std::mutex cb_mu_;
    LeaderCallback on_become_leader_;
};

}  // namespace

std::unique_ptr<HaCoordinator> make_file_ha_coordinator(std::string ha_dir,
                                                        LeaderEndpoint advertise,
                                                        std::chrono::milliseconds poll_interval,
                                                        bool allow_unsafe_locks) {
    return std::make_unique<FileHaCoordinator>(
        std::move(ha_dir), std::move(advertise), poll_interval, allow_unsafe_locks);
}

}  // namespace clink::cluster
