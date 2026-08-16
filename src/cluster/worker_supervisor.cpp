#include "clink/cluster/worker_supervisor.hpp"

#include <algorithm>
#include <chrono>
#include <random>
#include <stdexcept>
#include <thread>
#include <utility>

#include "clink/metrics/process_metrics.hpp"
#include "clink/runtime/log_buffer.hpp"
#include "clink/runtime/logging.hpp"
#include "clink/runtime/network/connection.hpp"

namespace clink::cluster {

using namespace std::chrono_literals;

namespace {

std::chrono::milliseconds next_backoff(std::chrono::milliseconds current,
                                       std::chrono::milliseconds maximum) {
    if (current >= maximum || current > maximum / 2) {
        return maximum;
    }
    return current * 2;
}

}  // namespace

WorkerSupervisor::WorkerSupervisor(std::string worker_id,
                                   std::string data_host,
                                   Config config,
                                   DiscoverFn discover)
    : worker_id_(std::move(worker_id)),
      data_host_(std::move(data_host)),
      config_(std::move(config)),
      discover_(std::move(discover)) {
    if (!discover_) {
        throw std::invalid_argument("WorkerSupervisor: discovery callback is required");
    }
    if (config_.initial_backoff <= 0ms) {
        throw std::invalid_argument("WorkerSupervisor: initial_backoff must be positive");
    }
    if (config_.max_backoff < config_.initial_backoff) {
        throw std::invalid_argument(
            "WorkerSupervisor: max_backoff must be at least initial_backoff");
    }
    if (config_.discovery_timeout <= 0ms) {
        throw std::invalid_argument("WorkerSupervisor: discovery_timeout must be positive");
    }
    connect_factory_ = [](const std::string& host, std::uint16_t port) {
        return network::connect_plain(host, port);
    };
}

void WorkerSupervisor::set_connect_factory(Worker::ConnectFactory factory) {
    if (!factory) {
        throw std::invalid_argument("WorkerSupervisor: connect factory is empty");
    }
    connect_factory_ = std::move(factory);
}

void WorkerSupervisor::set_advertised_http_port(std::uint16_t port) noexcept {
    advertised_http_port_ = port;
}

void WorkerSupervisor::set_on_connected(ConnectedFn callback) {
    on_connected_ = std::move(callback);
}

std::shared_ptr<Worker> WorkerSupervisor::active_worker() const {
    std::lock_guard lock(state_mu_);
    return active_worker_;
}

WorkerSupervisor::Snapshot WorkerSupervisor::snapshot() const {
    std::lock_guard lock(state_mu_);
    return snapshot_;
}

bool WorkerSupervisor::should_stop_(const std::function<bool()>& external) const {
    return stop_requested_.load(std::memory_order_acquire) || (external && external());
}

bool WorkerSupervisor::wait_backoff_(std::chrono::milliseconds cap,
                                     const std::function<bool()>& external) {
    // Full jitter prevents a fleet of workers disconnected by the same leader
    // failure from synchronising every retry. A stable per-process seed keeps
    // tests reproducible enough while still spreading distinct worker ids.
    thread_local std::mt19937_64 rng{
        std::hash<std::string>{}(worker_id_) ^
        static_cast<std::uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count())};
    std::uniform_int_distribution<std::int64_t> distribution(0, cap.count());
    const auto delay = std::chrono::milliseconds{distribution(rng)};
    const auto deadline = std::chrono::steady_clock::now() + delay;
    while (!should_stop_(external) && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(25ms);
    }
    return !should_stop_(external);
}

void WorkerSupervisor::publish_session_(std::shared_ptr<Worker> worker,
                                        const LeaderEndpoint& endpoint,
                                        std::uint64_t session_number,
                                        bool reconnect) {
    {
        std::lock_guard lock(state_mu_);
        active_worker_ = std::move(worker);
        snapshot_.state = "connected";
        snapshot_.connected = true;
        snapshot_.session_number = session_number;
        snapshot_.coordinator_host = endpoint.host;
        snapshot_.coordinator_port = endpoint.port;
        snapshot_.coordinator_epoch = active_worker_->bound_epoch();
        snapshot_.last_error.clear();
        if (reconnect) {
            ++snapshot_.successful_reconnections;
        }
    }
    metrics::worker::control_connected(reconnect);
}

void WorkerSupervisor::clear_session_(const std::shared_ptr<Worker>& worker,
                                      const std::string& state) {
    std::lock_guard lock(state_mu_);
    if (active_worker_ == worker) {
        active_worker_.reset();
    }
    snapshot_.state = state;
    snapshot_.connected = false;
}

void WorkerSupervisor::set_failure_(std::string state, std::string error) {
    std::lock_guard lock(state_mu_);
    snapshot_.state = std::move(state);
    snapshot_.connected = false;
    snapshot_.last_error = std::move(error);
}

WorkerSupervisor::RunResult WorkerSupervisor::run(const std::function<bool()>& shutdown_requested) {
    auto backoff = config_.initial_backoff;
    std::uint64_t highest_epoch = 0;
    std::uint64_t session_number = 0;
    bool connected_once = false;

    while (!should_stop_(shutdown_requested)) {
        set_failure_("discovering", {});
        std::optional<LeaderEndpoint> endpoint;
        try {
            endpoint = discover_(config_.discovery_timeout);
        } catch (const std::exception& e) {
            const std::string error = std::string{"coordinator discovery failed: "} + e.what();
            set_failure_("backoff", error);
            log::warn("worker.recovery", error);
            if (!wait_backoff_(backoff, shutdown_requested)) {
                break;
            }
            backoff = next_backoff(backoff, config_.max_backoff);
            continue;
        }
        if (should_stop_(shutdown_requested)) {
            break;
        }
        if (!endpoint.has_value()) {
            const std::string error = "no coordinator leader was discoverable";
            set_failure_("backoff", error);
            log::warn("worker.recovery", error);
            if (!wait_backoff_(backoff, shutdown_requested)) {
                break;
            }
            backoff = next_backoff(backoff, config_.max_backoff);
            continue;
        }
        if (endpoint->epoch != 0 && endpoint->epoch < highest_epoch) {
            const std::string error =
                "refusing stale discovered coordinator epoch " + std::to_string(endpoint->epoch) +
                " below previously-bound epoch " + std::to_string(highest_epoch);
            set_failure_("backoff", error);
            log::warn("worker.recovery", error);
            if (!wait_backoff_(backoff, shutdown_requested)) {
                break;
            }
            backoff = next_backoff(backoff, config_.max_backoff);
            continue;
        }

        {
            std::lock_guard lock(state_mu_);
            snapshot_.state = connected_once ? "reconnecting" : "connecting";
            snapshot_.coordinator_host = endpoint->host;
            snapshot_.coordinator_port = endpoint->port;
            snapshot_.coordinator_epoch = endpoint->epoch;
            ++snapshot_.connection_attempts;
        }
        metrics::worker::control_connection_attempted();

        auto worker = std::make_shared<Worker>(worker_id_, data_host_, config_.worker);
        worker->set_connect_factory(connect_factory_);
        worker->set_advertised_http_port(advertised_http_port_);
        try {
            worker->connect_to_coordinator(endpoint->host, endpoint->port);
        } catch (const WorkerConnectionError& e) {
            worker->stop();
            if (!e.retryable()) {
                set_failure_("fatal", e.what());
                metrics::worker::control_stopped();
                log::error("worker.recovery", e.what());
                return {.fatal = true, .error = e.what()};
            }
            set_failure_("backoff", e.what());
            log::warn("worker.recovery", e.what());
            if (!wait_backoff_(backoff, shutdown_requested)) {
                break;
            }
            backoff = next_backoff(backoff, config_.max_backoff);
            continue;
        } catch (const std::exception& e) {
            worker->stop();
            set_failure_("backoff", e.what());
            log::warn("worker.recovery", e.what());
            if (!wait_backoff_(backoff, shutdown_requested)) {
                break;
            }
            backoff = next_backoff(backoff, config_.max_backoff);
            continue;
        }

        const auto admitted_epoch = worker->bound_epoch();
        const auto required_epoch = std::max(highest_epoch, endpoint->epoch);
        if (required_epoch != 0 && admitted_epoch < required_epoch) {
            const std::string error = "refusing coordinator registration at epoch " +
                                      std::to_string(admitted_epoch) + " below required epoch " +
                                      std::to_string(required_epoch);
            worker->stop();
            set_failure_("backoff", error);
            log::warn("worker.recovery", error);
            if (!wait_backoff_(backoff, shutdown_requested)) {
                break;
            }
            backoff = next_backoff(backoff, config_.max_backoff);
            continue;
        }
        highest_epoch = std::max(highest_epoch, admitted_epoch);
        ++session_number;
        publish_session_(worker, *endpoint, session_number, connected_once);
        connected_once = true;
        backoff = config_.initial_backoff;
        log::info("worker.recovery",
                  "control session " + std::to_string(session_number) + " registered with " +
                      endpoint->host + ":" + std::to_string(endpoint->port) + " at epoch " +
                      std::to_string(admitted_epoch));
        if (on_connected_) {
            try {
                on_connected_(*worker, *endpoint, session_number);
            } catch (const std::exception& e) {
                log::warn("worker.recovery", std::string{"connected callback failed: "} + e.what());
            }
        }

        while (!should_stop_(shutdown_requested) && !worker->disconnected()) {
            std::this_thread::sleep_for(50ms);
        }
        if (should_stop_(shutdown_requested)) {
            clear_session_(worker, "stopping");
            worker->stop();
            break;
        }

        clear_session_(worker, "draining");
        metrics::worker::control_disconnected();
        // accept_epoch_ may have observed a newer epoch on this connection.
        // Carry that fence into discovery even though the original admission
        // epoch was lower.
        highest_epoch = std::max(highest_epoch, worker->bound_epoch());
        // stop() joins every task and destroys no state until those joins have
        // completed. The next session is never published before this returns,
        // which prevents old and restored subtasks from overlapping.
        worker->stop();
        log::warn("worker.recovery",
                  "control session " + std::to_string(session_number) +
                      " drained; rediscovering the coordinator without exiting the process");
    }

    metrics::worker::control_stopped();
    set_failure_("stopped", {});
    return {};
}

}  // namespace clink::cluster
