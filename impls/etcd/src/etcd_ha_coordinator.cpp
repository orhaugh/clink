#include "clink/etcd/etcd_ha_coordinator.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

#include <etcd/Client.hpp>
#include <etcd/KeepAlive.hpp>
#include <etcd/Response.hpp>

#include "clink/http/json_writer.hpp"

namespace clink::cluster {

namespace {

std::int64_t unix_ms_now() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

// Serialize the leader endpoint into the same JSON shape
// FileHaCoordinator writes to active-leader.json. Lets the existing
// TM-side discovery and dashboard rendering work against either
// coordinator without branching on the kind of storage.
std::string serialize_leader_endpoint(const LeaderEndpoint& ep, std::uint64_t epoch) {
    clink::http::JsonWriter w;
    w.begin_object();
    w.kv("host", ep.host);
    w.kv("port", static_cast<std::int64_t>(ep.port));
    w.kv("epoch", static_cast<std::int64_t>(epoch));
    w.kv("updated_unix_ms", static_cast<std::int64_t>(unix_ms_now()));
    w.end_object();
    return w.str();
}

// Hand-rolled JSON extractors, same as FileHaCoordinator's. Kept
// inline here so this module doesn't pull a JSON parser dep.
std::string extract_string(const std::string& body, const std::string& key) {
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
std::uint64_t extract_uint(const std::string& body, const std::string& key) {
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

class EtcdHaCoordinator final : public HaCoordinator {
public:
    EtcdHaCoordinator(EtcdHaConfig cfg, LeaderEndpoint advertise)
        : cfg_(std::move(cfg)), advertise_(std::move(advertise)) {
        if (cfg_.endpoints.empty()) {
            throw std::runtime_error("EtcdHaCoordinator: endpoints must be non-empty");
        }
        if (cfg_.cluster_name.empty()) {
            throw std::runtime_error("EtcdHaCoordinator: cluster_name must be non-empty");
        }
        leader_key_ = "/clink/jm/" + cfg_.cluster_name + "/leader";
    }

    ~EtcdHaCoordinator() override { stop(); }

    void start() override {
        bool expected = false;
        if (!started_.compare_exchange_strong(expected, true))
            return;
        worker_ = std::thread([this] { worker_loop_(); });
    }

    void stop() override {
        if (!started_.exchange(false))
            return;
        stop_.store(true, std::memory_order_release);
        cv_.notify_all();
        if (worker_.joinable())
            worker_.join();
        release_();
    }

    bool is_leader() const noexcept override { return is_leader_.load(std::memory_order_acquire); }

    std::uint64_t epoch() const noexcept override { return epoch_.load(std::memory_order_acquire); }

    std::optional<LeaderEndpoint> current_leader_endpoint() override {
        try {
            auto client = ensure_client_();
            if (!client)
                return std::nullopt;
            auto resp = client->get(leader_key_).get();
            if (!resp.is_ok())
                return std::nullopt;
            const std::string body = resp.value().as_string();
            LeaderEndpoint ep;
            ep.host = extract_string(body, "host");
            ep.port = static_cast<std::uint16_t>(extract_uint(body, "port"));
            ep.epoch = extract_uint(body, "epoch");
            ep.updated_unix_ms = static_cast<std::int64_t>(extract_uint(body, "updated_unix_ms"));
            if (ep.host.empty() || ep.port == 0)
                return std::nullopt;
            return ep;
        } catch (...) {
            return std::nullopt;
        }
    }

    void set_on_become_leader(LeaderCallback cb) override {
        std::lock_guard lock(cb_mu_);
        on_become_leader_ = std::move(cb);
    }

private:
    std::shared_ptr<etcd::Client> ensure_client_() {
        std::lock_guard lock(client_mu_);
        if (!client_) {
            try {
                client_ = std::make_shared<etcd::Client>(cfg_.endpoints);
            } catch (...) {
                client_.reset();
            }
        }
        return client_;
    }

    void worker_loop_() {
        while (!stop_.load(std::memory_order_acquire)) {
            try {
                if (!is_leader_.load(std::memory_order_acquire)) {
                    try_acquire_();
                } else {
                    // Leader: refresh the published endpoint (so the
                    // updated_unix_ms field tracks liveness) and
                    // re-arm watch on the unlikely case the key was
                    // expired by an etcd-side TTL race. KeepAlive
                    // handles the lease itself.
                    refresh_leader_value_();
                }
            } catch (...) {
                // etcd connection failure: drop leadership (KeepAlive
                // will probably fail next anyway) and back off.
                release_();
            }
            std::unique_lock lock(wait_mu_);
            cv_.wait_for(lock, cfg_.retry_interval, [this] {
                return stop_.load(std::memory_order_acquire);
            });
        }
    }

    void try_acquire_() {
        auto client = ensure_client_();
        if (!client)
            return;

        // 1. Grant a lease. The leader key gets attached to this
        // lease, so a process crash that stops KeepAlive lets etcd
        // delete the key automatically after lease_ttl.
        auto lease_resp = client->leasegrant(static_cast<int>(cfg_.lease_ttl.count())).get();
        if (!lease_resp.is_ok())
            return;
        const std::int64_t lease_id = lease_resp.value().lease();

        // 2. Atomic create. etcd::Client::add only succeeds if the
        // key doesn't already exist - the classic compare-and-set
        // primitive for leader election.
        const auto candidate_epoch = epoch_.load(std::memory_order_acquire) + 1;
        const auto payload = serialize_leader_endpoint(advertise_, candidate_epoch);
        auto add_resp = client->add(leader_key_, payload, lease_id).get();
        if (!add_resp.is_ok()) {
            // Someone else holds the key. Release the lease we just
            // grabbed (don't leak; etcd will GC eventually but
            // prompt cleanup helps under churn).
            (void)client->leaserevoke(lease_id).get();
            return;
        }

        // 3. Hand the lease to KeepAlive so it refreshes ~ttl/3.
        // Owned by us; destroyed in release_() to stop the pings.
        auto keepalive = std::make_unique<etcd::KeepAlive>(
            *client, static_cast<int>(cfg_.lease_ttl.count()), lease_id);

        {
            std::lock_guard lock(state_mu_);
            client_held_ = client;
            keepalive_ = std::move(keepalive);
            lease_id_ = lease_id;
        }
        epoch_.store(candidate_epoch, std::memory_order_release);
        is_leader_.store(true, std::memory_order_release);

        LeaderCallback cb;
        {
            std::lock_guard lock(cb_mu_);
            cb = on_become_leader_;
        }
        if (cb) {
            try {
                cb(candidate_epoch);
            } catch (...) {
                // A throwing callback must not crash the worker.
            }
        }
    }

    void refresh_leader_value_() {
        auto client = ensure_client_();
        if (!client)
            return;
        std::int64_t lease;
        {
            std::lock_guard lock(state_mu_);
            lease = lease_id_;
        }
        if (lease == 0)
            return;
        const auto payload =
            serialize_leader_endpoint(advertise_, epoch_.load(std::memory_order_acquire));
        // etcd::Client::set updates an existing key. The lease
        // attachment is preserved across updates since v3.
        (void)client->set(leader_key_, payload, lease).get();
    }

    void release_() {
        std::unique_ptr<etcd::KeepAlive> ka;
        std::int64_t lease = 0;
        std::shared_ptr<etcd::Client> client;
        {
            std::lock_guard lock(state_mu_);
            ka = std::move(keepalive_);
            lease = lease_id_;
            lease_id_ = 0;
            client = client_held_;
            client_held_.reset();
        }
        is_leader_.store(false, std::memory_order_release);
        // Destroying KeepAlive stops the lease-refresh thread.
        ka.reset();
        if (client && lease != 0) {
            // Revoke explicitly so failover doesn't wait the full
            // lease TTL. Best-effort: an unreachable etcd here is
            // fine - the lease will expire on its own.
            try {
                (void)client->leaserevoke(lease).get();
            } catch (...) {
            }
        }
    }

    EtcdHaConfig cfg_;
    LeaderEndpoint advertise_;
    std::string leader_key_;

    std::atomic<bool> started_{false};
    std::atomic<bool> stop_{false};
    std::atomic<bool> is_leader_{false};
    std::atomic<std::uint64_t> epoch_{0};

    std::thread worker_;
    std::mutex wait_mu_;
    std::condition_variable cv_;

    std::mutex cb_mu_;
    LeaderCallback on_become_leader_;

    std::mutex client_mu_;
    std::shared_ptr<etcd::Client> client_;

    std::mutex state_mu_;
    std::shared_ptr<etcd::Client> client_held_;
    std::unique_ptr<etcd::KeepAlive> keepalive_;
    std::int64_t lease_id_{0};
};

}  // namespace

std::unique_ptr<HaCoordinator> make_etcd_ha_coordinator(EtcdHaConfig config,
                                                        LeaderEndpoint advertise) {
    return std::make_unique<EtcdHaCoordinator>(std::move(config), std::move(advertise));
}

}  // namespace clink::cluster
