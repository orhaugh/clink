#pragma once

// Multi-TM Queryable State client. Wraps the single-TM Client to
// iterate every TM hosting a given job until one returns a hit.
//
// Two ways to construct:
//
//   1. With an explicit list of TM HTTP targets:
//        ClusterClient cc({{"host1", 8081}, {"host2", 8081}, ...});
//
//   2. By discovery against the JM:
//        ClusterClient cc = ClusterClient::from_jm(jm_host, jm_port, job_id);
//      The constructor hits GET /api/v1/queryable_state/job/<id>/tms
//      on the JM and uses the returned list. Discovery happens once;
//      callers re-create the client to pick up rescale-driven changes.
//
// Usage:
//
//   auto v = cc.get<std::string, std::int64_t>("counter", "alpha",
//                                              string_codec(), int64_codec());
//
// Iteration order is the JM's returned order; first non-nullopt wins.
// All-nullopt returns nullopt. Transport / 5xx errors on any TM are
// surfaced as exceptions (we don't silently skip a broken TM -
// otherwise a transient blip could hide a real hit).
//
// V1 limits (see project memory):
//   * Brute-force per-TM iteration: O(parallelism) requests per get.
//   * No caching of the TM list across calls. Callers that issue
//     many queries against the same job should hold a single
//     ClusterClient instance and re-construct on cluster topology
//     changes (e.g., post-rescale).
//
// For key-group-aware single-hop routing, use `RoutedClient` (below):
// it asks the JM which TM hosts the key's key-group, then queries
// that one TM. Two HTTP round-trips per get steady-state (route +
// value); the route can be cached client-side per key for repeated
// lookups against the same key.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "clink/cluster/protocol.hpp"
#include "clink/core/codec.hpp"
#include "clink/http/http_client.hpp"
#include "clink/queryable_state/client.hpp"
#include "clink/queryable_state/server.hpp"
#include "clink/runtime/key_groups.hpp"

namespace clink::queryable_state {

struct TmTarget {
    std::string host;
    std::uint16_t port{0};
    // Populated by RoutedClient's parse_route_ from the JM /route
    // response. Default 0 for ClusterClient's brute-force usage,
    // which doesn't know the subtask layout.
    std::uint32_t subtask_idx{0};
};

class ClusterClient {
public:
    explicit ClusterClient(std::vector<TmTarget> tms) : tms_(std::move(tms)) {}

    // Construct by JM discovery. Issues one HTTP call to fetch the
    // current TM list for `job_id`; if the JM returns an empty list
    // the resulting client will also return nullopt for every query.
    [[nodiscard]] static ClusterClient from_jm(const std::string& jm_host,
                                               std::uint16_t jm_port,
                                               cluster::JobId job_id) {
        http::HttpClient http(jm_host, jm_port);
        const std::string path = "/api/v1/queryable_state/job/" + std::to_string(job_id) + "/tms";
        auto resp = http.get(path);
        if (resp.status == 0) {
            throw std::runtime_error("ClusterClient::from_jm: transport error: " + resp.error);
        }
        if (resp.status != 200) {
            throw std::runtime_error("ClusterClient::from_jm: JM returned status " +
                                     std::to_string(resp.status) + ": " + resp.body);
        }
        return ClusterClient(parse_tm_list_(resp.body));
    }

    // Typed lookup. Iterates the captured TM list and returns the
    // first non-nullopt result. nullopt on all-miss.
    template <typename K, typename V>
    [[nodiscard]] std::optional<V> get(const std::string& slot,
                                       const K& key,
                                       Codec<K> kc,
                                       Codec<V> vc) {
        for (const auto& tm : tms_) {
            Client client(tm.host, tm.port);
            auto v = client.template get<K, V>(slot, key, kc, vc);
            if (v.has_value()) {
                return v;
            }
        }
        return std::nullopt;
    }

    [[nodiscard]] const std::vector<TmTarget>& tms() const noexcept { return tms_; }

private:
    // Cheap parse of `{"tms":[{"host":"...","port":NNN}, ...]}`. The
    // JM side controls the format, so we don't need a full JSON
    // parser - find every `"host":"..."` / `"port":NNN` pair and
    // accumulate them.
    [[nodiscard]] static std::vector<TmTarget> parse_tm_list_(const std::string& body) {
        std::vector<TmTarget> out;
        std::size_t i = 0;
        while (true) {
            const auto host_pos = body.find("\"host\":\"", i);
            if (host_pos == std::string::npos) {
                break;
            }
            const auto host_start = host_pos + std::string{"\"host\":\""}.size();
            const auto host_end = body.find('"', host_start);
            if (host_end == std::string::npos) {
                break;
            }
            const auto port_pos = body.find("\"port\":", host_end);
            if (port_pos == std::string::npos) {
                break;
            }
            const auto port_start = port_pos + std::string{"\"port\":"}.size();
            std::size_t port_end = port_start;
            while (port_end < body.size() && body[port_end] >= '0' && body[port_end] <= '9') {
                ++port_end;
            }
            if (port_end == port_start) {
                break;
            }
            TmTarget t;
            t.host = body.substr(host_start, host_end - host_start);
            t.port = static_cast<std::uint16_t>(
                std::stoul(body.substr(port_start, port_end - port_start)));
            out.push_back(std::move(t));
            i = port_end;
        }
        return out;
    }

    std::vector<TmTarget> tms_;
};

// Key-group-aware single-hop client. Holds a JM target + job_id +
// op-role. Each get() first asks the JM which TM hosts the key's
// key-group via GET /api/v1/queryable_state/job/<id>/op/<role>/route,
// then queries that one TM. Returns nullopt if the JM routes to no
// subtask (job/role unknown, kg uncovered) OR if the routed TM
// returns 404 for the slot/key.
//
// Versus ClusterClient: trades one extra JM round-trip per get for
// O(1) TM queries instead of O(parallelism). At parallelism >= 3 or
// so the routed path wins; at parallelism = 1 ClusterClient is
// slightly faster (no JM hop). Use RoutedClient when you know the
// job has multiple subtasks for the queryable op.
class RoutedClient {
public:
    RoutedClient(std::string jm_host,
                 std::uint16_t jm_port,
                 cluster::JobId job_id,
                 std::string role)
        : jm_(std::move(jm_host), jm_port), job_id_(job_id), role_(std::move(role)) {}

    // Configure the route-cache TTL. The cache is keyed by
    // (key_group, role) and stores the JM's route response for that
    // span; subsequent gets for keys in the SAME key-group skip the
    // JM round-trip until the entry ages out OR the TM returns 404
    // (which suggests the route is stale post-rescale and the entry
    // is evicted). Default 30s. Set to 0 to disable caching.
    RoutedClient& set_route_cache_ttl(std::chrono::milliseconds ttl) noexcept {
        std::lock_guard lock(cache_mu_);
        route_cache_ttl_ = ttl;
        return *this;
    }

    // How often the client checks the JM's topology_version while
    // serving cached lookups. Smaller = quicker rescale detection,
    // more JM traffic. Default 1s. Set to 0 to disable the version
    // check entirely (fall back to TTL + 404-eviction only).
    RoutedClient& set_topology_version_check_interval(std::chrono::milliseconds iv) noexcept {
        std::lock_guard lock(cache_mu_);
        version_check_interval_ = iv;
        return *this;
    }

    // Counter for tests / metrics: how many times the JM /route
    // endpoint has been hit by this client. With caching working,
    // this should grow much slower than the number of get() calls.
    [[nodiscard]] std::uint64_t jm_route_requests() const noexcept {
        return jm_route_requests_.load(std::memory_order_relaxed);
    }

    // How many times the topology_version endpoint was hit. Useful
    // for tests that want to verify the version-check polling rate.
    [[nodiscard]] std::uint64_t jm_topology_version_requests() const noexcept {
        return jm_topology_version_requests_.load(std::memory_order_relaxed);
    }

    template <typename K, typename V>
    [[nodiscard]] std::optional<V> get(const std::string& slot,
                                       const K& key,
                                       Codec<K> kc,
                                       Codec<V> vc) {
        const auto key_bytes = kc.encode(key);
        const auto kg =
            key_group_for_key(std::span<const std::byte>{key_bytes.data(), key_bytes.size()});
        // Refresh the topology version periodically. A change clears
        // the whole route cache (rescale moved key-groups around;
        // every cached entry is suspect).
        maybe_refresh_topology_version_();
        // Cache fast-path: if a fresh entry exists for this kg, skip
        // the JM round-trip and query the cached TM target directly.
        auto cached = cached_target_for_(kg);
        if (cached.has_value()) {
            Client tm_client(cached->host, cached->port);
            auto v = tm_client.template get<K, V>(role_, cached->subtask_idx, slot, key, kc, vc);
            if (!v.has_value()) {
                // 404 on a cached route could be a normal miss OR a
                // stale route post-rescale. Evict and let the next
                // get() refresh from the JM. Simple, slightly more
                // JM traffic than strictly needed but always correct.
                evict_cache_for_(kg);
            }
            return v;
        }
        // Cache miss: hit the JM /route endpoint.
        const auto key_hex =
            detail::hex_encode(std::span<const std::byte>{key_bytes.data(), key_bytes.size()});
        const std::string route_path = "/api/v1/queryable_state/job/" + std::to_string(job_id_) +
                                       "/op/" + role_ + "/route?key=" + key_hex;
        auto route_resp = jm_.get(route_path);
        jm_route_requests_.fetch_add(1, std::memory_order_relaxed);
        if (route_resp.status == 0) {
            throw std::runtime_error("RoutedClient: transport error to JM: " + route_resp.error);
        }
        if (route_resp.status == 404) {
            return std::nullopt;
        }
        if (route_resp.status != 200) {
            throw std::runtime_error("RoutedClient: JM returned status " +
                                     std::to_string(route_resp.status) + ": " + route_resp.body);
        }
        const auto target = parse_route_(route_resp.body);
        if (!target.has_value()) {
            throw std::runtime_error("RoutedClient: malformed route response: " + route_resp.body);
        }
        // /route response also carries the current topology_version;
        // record it so the next cache fast-path knows the baseline.
        if (auto v = parse_topology_version_field_(route_resp.body); v.has_value()) {
            update_topology_version_(*v);
        }
        cache_target_for_(kg, *target);
        Client tm_client(target->host, target->port);
        auto v = tm_client.template get<K, V>(role_, target->subtask_idx, slot, key, kc, vc);
        if (!v.has_value()) {
            // 404 from the routed TM: evict eagerly. The cache entry
            // we just populated is suspect; the next get() should
            // refetch.
            evict_cache_for_(kg);
        }
        return v;
    }

private:
    [[nodiscard]] static std::optional<TmTarget> parse_route_(const std::string& body) {
        const auto host_pos = body.find("\"host\":\"");
        if (host_pos == std::string::npos) {
            return std::nullopt;
        }
        const auto host_start = host_pos + std::string{"\"host\":\""}.size();
        const auto host_end = body.find('"', host_start);
        if (host_end == std::string::npos) {
            return std::nullopt;
        }
        const auto port_pos = body.find("\"port\":", host_end);
        if (port_pos == std::string::npos) {
            return std::nullopt;
        }
        const auto port_start = port_pos + std::string{"\"port\":"}.size();
        std::size_t port_end = port_start;
        while (port_end < body.size() && body[port_end] >= '0' && body[port_end] <= '9') {
            ++port_end;
        }
        if (port_end == port_start) {
            return std::nullopt;
        }
        TmTarget t;
        t.host = body.substr(host_start, host_end - host_start);
        t.port =
            static_cast<std::uint16_t>(std::stoul(body.substr(port_start, port_end - port_start)));
        // Optional subtask_idx field; the JM /route endpoint emits it
        // alongside host/port, but older snapshots may have omitted
        // it. Default 0 if absent.
        const auto sub_pos = body.find("\"subtask_idx\":", port_end);
        if (sub_pos != std::string::npos) {
            const auto sub_start = sub_pos + std::string{"\"subtask_idx\":"}.size();
            std::size_t sub_end = sub_start;
            while (sub_end < body.size() && body[sub_end] >= '0' && body[sub_end] <= '9') {
                ++sub_end;
            }
            if (sub_end > sub_start) {
                t.subtask_idx = static_cast<std::uint32_t>(
                    std::stoul(body.substr(sub_start, sub_end - sub_start)));
            }
        }
        return t;
    }

    struct CachedRoute {
        TmTarget target;
        std::chrono::steady_clock::time_point fetched_at;
    };

    // Cheap u64 field parser used by both topology_version and the
    // optional topology_version field of /route responses.
    [[nodiscard]] static std::optional<std::uint64_t> parse_u64_field_(const std::string& body,
                                                                       std::string_view field) {
        const std::string needle = "\"" + std::string{field} + "\":";
        const auto pos = body.find(needle);
        if (pos == std::string::npos) {
            return std::nullopt;
        }
        std::size_t start = pos + needle.size();
        std::size_t end = start;
        while (end < body.size() && body[end] >= '0' && body[end] <= '9') {
            ++end;
        }
        if (end == start) {
            return std::nullopt;
        }
        try {
            return static_cast<std::uint64_t>(std::stoull(body.substr(start, end - start)));
        } catch (...) {
            return std::nullopt;
        }
    }

    [[nodiscard]] static std::optional<std::uint64_t> parse_topology_version_field_(
        const std::string& body) {
        return parse_u64_field_(body, "topology_version");
    }

    void update_topology_version_(std::uint64_t v) {
        std::lock_guard lock(cache_mu_);
        if (v != known_topology_version_) {
            // Version changed: clear all cached routes. The JM moved
            // key-groups; every entry could be pointing at the wrong
            // subtask now.
            if (known_topology_version_ != 0) {
                cache_.clear();
            }
            known_topology_version_ = v;
        }
        last_version_check_ = std::chrono::steady_clock::now();
    }

    void maybe_refresh_topology_version_() {
        if (version_check_interval_.count() <= 0) {
            return;
        }
        bool need_check = false;
        {
            std::lock_guard lock(cache_mu_);
            const auto since = std::chrono::steady_clock::now() - last_version_check_;
            if (since > std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                            version_check_interval_)) {
                need_check = true;
            }
        }
        if (!need_check) {
            return;
        }
        // Hit the cheap topology_version endpoint.
        const std::string path =
            "/api/v1/queryable_state/job/" + std::to_string(job_id_) + "/topology_version";
        auto resp = jm_.get(path);
        jm_topology_version_requests_.fetch_add(1, std::memory_order_relaxed);
        if (resp.status != 200) {
            // Transient failure: don't tamper with the cache. Next
            // get() will retry on the next interval boundary.
            return;
        }
        if (auto v = parse_u64_field_(resp.body, "version"); v.has_value()) {
            update_topology_version_(*v);
        }
    }

    [[nodiscard]] std::optional<TmTarget> cached_target_for_(KeyGroup kg) {
        std::lock_guard lock(cache_mu_);
        if (route_cache_ttl_.count() <= 0) {
            return std::nullopt;
        }
        auto it = cache_.find(kg);
        if (it == cache_.end()) {
            return std::nullopt;
        }
        const auto age = std::chrono::steady_clock::now() - it->second.fetched_at;
        if (age >
            std::chrono::duration_cast<std::chrono::steady_clock::duration>(route_cache_ttl_)) {
            cache_.erase(it);
            return std::nullopt;
        }
        return it->second.target;
    }
    void cache_target_for_(KeyGroup kg, const TmTarget& target) {
        std::lock_guard lock(cache_mu_);
        if (route_cache_ttl_.count() <= 0) {
            return;
        }
        cache_[kg] = CachedRoute{target, std::chrono::steady_clock::now()};
    }
    void evict_cache_for_(KeyGroup kg) {
        std::lock_guard lock(cache_mu_);
        cache_.erase(kg);
    }

    http::HttpClient jm_;
    cluster::JobId job_id_;
    std::string role_;
    std::atomic<std::uint64_t> jm_route_requests_{0};
    std::atomic<std::uint64_t> jm_topology_version_requests_{0};

    mutable std::mutex cache_mu_;
    std::chrono::milliseconds route_cache_ttl_{30'000};
    std::chrono::milliseconds version_check_interval_{1'000};
    std::uint64_t known_topology_version_{0};
    std::chrono::steady_clock::time_point last_version_check_{};
    std::unordered_map<KeyGroup, CachedRoute> cache_;
};

}  // namespace clink::queryable_state
