#pragma once

// LocalDataPlane: process-wide registry of in-process StreamElement
// channels keyed by (host, port). When two subtasks are colocated on
// the same worker, the receiver registers a typed BoundedChannel under the
// port it would normally listen on; the sender's connect() checks the
// registry first and, on hit, switches to direct typed push - skipping
// codec serialization + TCP loopback + Arrow IPC parsing for every
// record. The socket path remains the fallback for cross-worker hops.
//
// Type erasure: channels are stored as shared_ptr<void> alongside a
// std::type_index so lookups can verify the consumer side requested
// the right T. A type mismatch returns nullptr (the bench-time wiring
// always matches, so this is defense in depth).

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <string>
#include <typeindex>
#include <unordered_map>

#include "clink/operators/operator_base.hpp"
#include "clink/runtime/bounded_channel.hpp"

namespace clink::network {

template <typename T>
using LocalEndpointChannel = BoundedChannel<StreamElement<T>>;

class LocalDataPlane {
public:
    static LocalDataPlane& instance() {
        static LocalDataPlane self;
        return self;
    }

    // The host name PEERS use to address this process, which is not the address
    // it BINDS to. A receiver binds 0.0.0.0 (or 127.0.0.1) but is advertised to
    // the coordinator as, say, "clink-worker3", and a sender resolves the peer
    // through the coordinator - so it looks up "clink-worker3:PORT" while the
    // receiver registered "0.0.0.0:PORT". The keys never match and the whole
    // in-process fast path silently disappears, which is exactly what happened
    // in the containerised benchmark: every co-located edge serialised to Arrow
    // IPC and crossed a TCP socket to its own hostname. It worked everywhere
    // else only because both sides defaulted to 127.0.0.1.
    //
    // Set once at worker startup from --data-host. Endpoints then register under
    // BOTH identities so a lookup by either finds them.
    void set_advertised_host(std::string host) {
        std::lock_guard lock(mu_);
        advertised_host_ = std::move(host);
    }
    std::string advertised_host() const {
        std::lock_guard lock(mu_);
        return advertised_host_;
    }

    template <typename T>
    void register_endpoint(const std::string& host,
                           std::uint16_t port,
                           std::shared_ptr<LocalEndpointChannel<T>> ch) {
        std::lock_guard lock(mu_);
        Entry e{std::move(ch), std::type_index(typeid(T))};
        entries_.insert_or_assign(make_key(host, port), e);
        // Also under the advertised identity (see set_advertised_host): a peer
        // resolves this endpoint through the coordinator and will look it up by
        // the advertised host, never by the bind address.
        if (!advertised_host_.empty() && advertised_host_ != host) {
            entries_.insert_or_assign(make_key(advertised_host_, port), e);
        }
        // A wildcard bind is reachable in-process as loopback too, and some
        // callers resolve a peer to 127.0.0.1 directly.
        if (host == "0.0.0.0") {
            entries_.insert_or_assign(make_key("127.0.0.1", port), e);
        }
    }

    // Drop the registration so a port can be reused on a fresh deploy. Must
    // mirror every alias register_endpoint created, or a stale channel outlives
    // its deploy and the next job pushes into a dead queue.
    void unregister_endpoint(const std::string& host, std::uint16_t port) {
        std::lock_guard lock(mu_);
        entries_.erase(make_key(host, port));
        if (!advertised_host_.empty() && advertised_host_ != host) {
            entries_.erase(make_key(advertised_host_, port));
        }
        if (host == "0.0.0.0") {
            entries_.erase(make_key("127.0.0.1", port));
        }
    }

    // Drop EVERY registration. For tests that build more than one in-process
    // parallel Dag in a single process.
    //
    // The registry is keyed by host:port, and two in-process Dags pick colliding
    // endpoints - so the second one's stages resolve to the FIRST one's channels and
    // the two jobs cross-wire. That is why giving the operators unique names does not
    // help: the collision is on the endpoint key, not on the OperatorId (F78).
    //
    // Not for production use. A worker process runs one Dag, and unregister_endpoint
    // is the path for reclaiming a port on a fresh deploy; clearing the whole
    // registry mid-job would strand every live channel.
    void clear_for_testing() {
        std::lock_guard lock(mu_);
        entries_.clear();
    }

    // Hit/miss counters so a silent regression of the fast path is visible
    // instead of merely slow. Incremented by NetworkChannelSink::connect.
    void note_local_hit() { local_hits_.fetch_add(1, std::memory_order_relaxed); }
    void note_socket_fallback() { socket_fallbacks_.fetch_add(1, std::memory_order_relaxed); }
    std::uint64_t local_hits() const { return local_hits_.load(std::memory_order_relaxed); }
    std::uint64_t socket_fallbacks() const {
        return socket_fallbacks_.load(std::memory_order_relaxed);
    }

    template <typename T>
    std::shared_ptr<LocalEndpointChannel<T>> lookup_endpoint(const std::string& host,
                                                             std::uint16_t port) {
        if (!enabled_.load(std::memory_order_acquire)) {
            return nullptr;
        }
        std::lock_guard lock(mu_);
        auto it = entries_.find(make_key(host, port));
        if (it == entries_.end()) {
            return nullptr;
        }
        if (it->second.type != std::type_index(typeid(T))) {
            return nullptr;
        }
        return std::static_pointer_cast<LocalEndpointChannel<T>>(it->second.channel);
    }

    // Runtime kill-switch for the local fast path. Tests that need to
    // exercise the cross-process socket+codec path force this to
    // false; production leaves it true so colocated subtasks skip
    // serde and TCP loopback. Affects lookup_endpoint only; registration
    // is unconditional so flipping the flag back on still works.
    void set_enabled(bool on) { enabled_.store(on, std::memory_order_release); }
    bool enabled() const { return enabled_.load(std::memory_order_acquire); }

private:
    std::string advertised_host_;
    std::atomic<std::uint64_t> local_hits_{0};
    std::atomic<std::uint64_t> socket_fallbacks_{0};

    // Diagnostic kill-switch: CLINK_DISABLE_LOCAL_DATA_PLANE=1 forces every
    // co-located edge onto the socket+codec path, so the fast path's
    // contribution can be measured A/B on an unmodified binary (and the
    // cross-process wire exercised inside one process).
    LocalDataPlane() {
        const char* off = std::getenv("CLINK_DISABLE_LOCAL_DATA_PLANE");
        if (off != nullptr && off[0] == '1') {
            enabled_.store(false, std::memory_order_release);
        }
    }

    struct Entry {
        std::shared_ptr<void> channel;
        std::type_index type;
    };

    static std::string make_key(const std::string& host, std::uint16_t port) {
        return host + ":" + std::to_string(port);
    }

    mutable std::mutex mu_;  // mutable: advertised_host() is const
    std::unordered_map<std::string, Entry> entries_;
    std::atomic<bool> enabled_{true};
};

// RAII helper for tests that need the cross-process socket+codec path.
// Disables LocalDataPlane lookups for the lifetime of the guard and
// restores the previous state on destruction.
class ScopedDisableLocalDataPlane {
public:
    ScopedDisableLocalDataPlane() : prev_(LocalDataPlane::instance().enabled()) {
        LocalDataPlane::instance().set_enabled(false);
    }
    ~ScopedDisableLocalDataPlane() { LocalDataPlane::instance().set_enabled(prev_); }

    ScopedDisableLocalDataPlane(const ScopedDisableLocalDataPlane&) = delete;
    ScopedDisableLocalDataPlane& operator=(const ScopedDisableLocalDataPlane&) = delete;
    ScopedDisableLocalDataPlane(ScopedDisableLocalDataPlane&&) = delete;
    ScopedDisableLocalDataPlane& operator=(ScopedDisableLocalDataPlane&&) = delete;

private:
    bool prev_;
};

}  // namespace clink::network
