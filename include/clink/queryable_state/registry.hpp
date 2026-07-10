#pragma once

// Queryable State - `.setQueryable(name)` analogue.
//
// Stream operators can opt a keyed-state slot in to external lookup
// by registering it with a QueryableStateRegistry. The registry is a
// process-wide map from slot-name -> byte-level lookup closure.
// Clients hit the TM's HTTP endpoint which consults the registry,
// retrieves the raw value bytes, and returns them. Codec decoding
// happens on the client.
//
// V1 scope (deliberately tight; see project memory for the resolved
// entry and limits):
//
//   * Single TM per slot. Multi-subtask routing (the JM directing a
//     client to the TM that holds a particular key-group) is a v2
//     enhancement. Today's clients query whichever TM happens to host
//     the slot. For a single-subtask deployment that's the only
//     option; for multi-subtask, query each TM.
//   * Reads only. No write-through, no delete.
//   * The registered closure captures references to the operator's
//     KeyedState / StateBackend. The user is responsible for
//     unregistering on close() if the backend goes away while the
//     registry outlives it. For typical pipelines the backend lives
//     for the run.
//   * Byte-level wire: the client sends the user-encoded key bytes
//     hex-encoded in the URL; the server returns the raw value bytes
//     hex-encoded in the response. Both sides agree on the V codec
//     out-of-band.

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <shared_mutex>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

namespace clink::queryable_state {

// Compose a (role, subtask_idx, slot) triple into the Registry's
// composite slot key. The same composition is applied at bind time
// (bind.hpp), at lookup time (server.hpp's subtask-scoped route),
// and on the client (client.hpp's per-subtask Client::get overload)
// so all three stay symmetric.
inline std::string compose_subtask_slot(const std::string& role,
                                        std::uint32_t subtask_idx,
                                        const std::string& slot) {
    return role + ":" + std::to_string(subtask_idx) + ":" + slot;
}

// Lookup closure: given user-key bytes (already encoded by the K
// codec), return user-value bytes (still encoded by the V codec), or
// nullopt for missing keys.
using Lookup = std::function<std::optional<std::vector<std::byte>>(std::span<const std::byte>)>;

// JSON lookup closure: given the key's plain string form, return the
// value as a JSON document, or nullopt for missing keys. The serving
// surface for SQL state: no codec agreement, no hex - a key string in,
// a JSON object out, consumable by anything that speaks HTTP.
using JsonLookup = std::function<std::optional<std::string>(const std::string&)>;

class Registry {
public:
    // Register or replace the lookup for `slot`. Repeated registration
    // of the same slot replaces the prior closure - the typical case
    // is a fresh job-run wiring its operator state.
    void register_slot(const std::string& slot, Lookup lookup) {
        std::unique_lock lock(mu_);
        by_slot_[slot] = std::move(lookup);
    }

    // Remove a slot. Idempotent - unregistering a non-existent slot is
    // a no-op. Call from operator close() if the backing state is
    // about to go away.
    void unregister_slot(const std::string& slot) {
        std::unique_lock lock(mu_);
        by_slot_.erase(slot);
    }

    // Run the registered lookup for `slot` against `key_bytes`. Returns
    // nullopt for both "slot not registered" and "key absent". The
    // caller distinguishes via has_slot() if needed.
    [[nodiscard]] std::optional<std::vector<std::byte>> lookup(
        const std::string& slot, std::span<const std::byte> key_bytes) const {
        std::shared_lock lock(mu_);
        auto it = by_slot_.find(slot);
        if (it == by_slot_.end()) {
            return std::nullopt;
        }
        // Copy the closure under the lock so the closure body can
        // execute outside it (the closure may hit a backend that
        // takes its own locks; nesting under our shared_mutex would
        // be a foot-gun if the user calls register_slot from inside).
        auto fn = it->second;
        lock.unlock();
        return fn(key_bytes);
    }

    [[nodiscard]] bool has_slot(const std::string& slot) const {
        std::shared_lock lock(mu_);
        return by_slot_.find(slot) != by_slot_.end();
    }

    // Enumerate registered slot names. Snapshot; safe to iterate
    // outside the lock.
    [[nodiscard]] std::vector<std::string> slots() const {
        std::shared_lock lock(mu_);
        std::vector<std::string> out;
        out.reserve(by_slot_.size());
        for (const auto& [k, _] : by_slot_) {
            out.push_back(k);
        }
        return out;
    }

    // JSON slots: the parallel, serving-oriented map. Same lifecycle
    // and locking discipline as the byte slots.
    void register_json_slot(const std::string& slot, JsonLookup lookup) {
        std::unique_lock lock(mu_);
        json_by_slot_[slot] = std::move(lookup);
    }

    void unregister_json_slot(const std::string& slot) {
        std::unique_lock lock(mu_);
        json_by_slot_.erase(slot);
    }

    [[nodiscard]] std::optional<std::string> lookup_json(const std::string& slot,
                                                         const std::string& key) const {
        std::shared_lock lock(mu_);
        auto it = json_by_slot_.find(slot);
        if (it == json_by_slot_.end()) {
            return std::nullopt;
        }
        auto fn = it->second;  // copy out, run outside the lock
        lock.unlock();
        return fn(key);
    }

    [[nodiscard]] bool has_json_slot(const std::string& slot) const {
        std::shared_lock lock(mu_);
        return json_by_slot_.find(slot) != json_by_slot_.end();
    }

    [[nodiscard]] std::vector<std::string> json_slots() const {
        std::shared_lock lock(mu_);
        std::vector<std::string> out;
        out.reserve(json_by_slot_.size());
        for (const auto& [k, _] : json_by_slot_) {
            out.push_back(k);
        }
        return out;
    }

    // The process-wide instance the TaskManager's HTTP routes serve and
    // operators bind into (test harnesses construct their own).
    static Registry& global() {
        static Registry instance;
        return instance;
    }

private:
    mutable std::shared_mutex mu_;
    std::unordered_map<std::string, Lookup> by_slot_;
    std::unordered_map<std::string, JsonLookup> json_by_slot_;
};

}  // namespace clink::queryable_state
