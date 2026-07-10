#pragma once

// LocalDataPlane: process-wide registry of in-process StreamElement
// channels keyed by (host, port). When two subtasks are colocated on
// the same TM, the receiver registers a typed BoundedChannel under the
// port it would normally listen on; the sender's connect() checks the
// registry first and, on hit, switches to direct typed push - skipping
// codec serialization + TCP loopback + Arrow IPC parsing for every
// record. The socket path remains the fallback for cross-TM hops.
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

    template <typename T>
    void register_endpoint(const std::string& host,
                           std::uint16_t port,
                           std::shared_ptr<LocalEndpointChannel<T>> ch) {
        std::lock_guard lock(mu_);
        entries_.insert_or_assign(make_key(host, port),
                                  Entry{std::move(ch), std::type_index(typeid(T))});
    }

    // Drop the registration so a port can be reused on a fresh deploy.
    void unregister_endpoint(const std::string& host, std::uint16_t port) {
        std::lock_guard lock(mu_);
        entries_.erase(make_key(host, port));
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

    std::mutex mu_;
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
