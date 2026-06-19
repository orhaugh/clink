#include "clink/cluster/ha_coordinator.hpp"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <unistd.h>

#include <sys/file.h>

#include "clink/http/json_writer.hpp"
#include "clink/metrics/orchestration_metrics.hpp"

namespace clink::cluster {

namespace {

std::int64_t unix_ms_now() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

class FileHaCoordinator final : public HaCoordinator {
public:
    FileHaCoordinator(std::string ha_dir,
                      LeaderEndpoint advertise,
                      std::chrono::milliseconds poll_interval)
        : ha_dir_(std::move(ha_dir)),
          advertise_(std::move(advertise)),
          poll_interval_(poll_interval) {
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
        // Acquired. Bump epoch, write active-leader.json, fire
        // callback.
        const auto new_epoch = epoch_.fetch_add(1, std::memory_order_acq_rel) + 1;
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
    std::atomic<bool> stop_{false};
    std::atomic<bool> is_leader_{false};
    std::atomic<std::uint64_t> epoch_{0};
    std::mutex cb_mu_;
    LeaderCallback on_become_leader_;
};

}  // namespace

std::unique_ptr<HaCoordinator> make_file_ha_coordinator(std::string ha_dir,
                                                        LeaderEndpoint advertise,
                                                        std::chrono::milliseconds poll_interval) {
    return std::make_unique<FileHaCoordinator>(
        std::move(ha_dir), std::move(advertise), poll_interval);
}

}  // namespace clink::cluster
