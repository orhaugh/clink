#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include "clink/cluster/ha_coordinator.hpp"
#include "clink/cluster/worker.hpp"

namespace clink::cluster {

// Owns the control-session lifecycle of one stable worker process.
//
// Worker deliberately remains a single-use session: it owns task threads,
// per-job plugin registries, checkpoint gates and network endpoints whose clean
// reset boundary is destruction. WorkerSupervisor keeps the OS process alive,
// fences and drains a disconnected session, then constructs a fresh Worker and
// re-registers it under the same stable worker id. This avoids both stale
// in-process state and container-wide churn.
class WorkerSupervisor {
public:
    struct Config {
        Worker::Config worker;
        std::chrono::milliseconds discovery_timeout{1000};
        std::chrono::milliseconds initial_backoff{100};
        std::chrono::milliseconds max_backoff{5000};
    };

    struct Snapshot {
        std::string state{"starting"};
        bool connected{false};
        std::uint64_t session_number{0};
        std::uint64_t connection_attempts{0};
        std::uint64_t successful_reconnections{0};
        std::string coordinator_host;
        std::uint16_t coordinator_port{0};
        std::uint64_t coordinator_epoch{0};
        std::string last_error;
    };

    struct RunResult {
        bool fatal{false};
        std::string error;
    };

    using DiscoverFn =
        std::function<std::optional<LeaderEndpoint>(std::chrono::milliseconds timeout)>;
    using ConnectedFn = std::function<void(
        const Worker& worker, const LeaderEndpoint& endpoint, std::uint64_t session_number)>;

    WorkerSupervisor(std::string worker_id,
                     std::string data_host,
                     Config config,
                     DiscoverFn discover);

    WorkerSupervisor(const WorkerSupervisor&) = delete;
    WorkerSupervisor& operator=(const WorkerSupervisor&) = delete;

    void set_connect_factory(Worker::ConnectFactory factory);
    void set_advertised_http_port(std::uint16_t port) noexcept;
    void set_on_connected(ConnectedFn callback);

    // Runs on the caller's thread until shutdown is requested. Transient
    // discovery and transport failures retry indefinitely with bounded full
    // jitter. A permanent handshake refusal returns fatal=true.
    RunResult run(const std::function<bool()>& shutdown_requested);
    void request_stop() noexcept { stop_requested_.store(true, std::memory_order_release); }

    [[nodiscard]] std::shared_ptr<Worker> active_worker() const;
    [[nodiscard]] Snapshot snapshot() const;
    [[nodiscard]] const Config& config() const noexcept { return config_; }

private:
    bool should_stop_(const std::function<bool()>& external) const;
    bool wait_backoff_(std::chrono::milliseconds cap, const std::function<bool()>& external);
    void publish_session_(std::shared_ptr<Worker> worker,
                          const LeaderEndpoint& endpoint,
                          std::uint64_t session_number,
                          bool reconnect);
    void clear_session_(const std::shared_ptr<Worker>& worker, const std::string& state);
    void set_failure_(std::string state, std::string error);

    std::string worker_id_;
    std::string data_host_;
    Config config_;
    DiscoverFn discover_;
    Worker::ConnectFactory connect_factory_;
    ConnectedFn on_connected_;
    std::uint16_t advertised_http_port_{0};
    std::atomic<bool> stop_requested_{false};

    mutable std::mutex state_mu_;
    std::shared_ptr<Worker> active_worker_;
    Snapshot snapshot_;
};

}  // namespace clink::cluster
