#pragma once

// RemotePool - the durable remote tier behind RemoteReadBackend.
//
// State lives here, off the checkpoint-barrier path: per-(operator, key)
// values, checkpoint-scoped for consistency and built INCREMENTALLY. Each
// checkpoint is committed as `base` (the previous committed checkpoint, or 0
// for none) plus only the entries that changed and the keys that were
// deleted since that base; unchanged keys are inherited from `base`. This is
// what lets RemoteReadBackend:
//   * checkpoint cheaply (commit only the delta, not the whole state), and
//   * restore lazily (point cold reads at a checkpoint; fetch a key only when
//     it is first read), which is the fast-recovery / fast-rescale win.
//
// The backend's framework Snapshot becomes a lightweight marker (the
// checkpoint id); the authoritative bytes are in the pool. read() runs on the
// backend's IO threads, so a RemotePool MUST be thread-safe.

#include <atomic>
#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "clink/core/types.hpp"
#include "clink/state/state_backend.hpp"

namespace clink {

struct RemotePoolKey {
    OperatorId op;
    std::string key;
};

struct RemotePoolEntry {
    OperatorId op;
    std::string key;
    StateBackend::Value value;
};

class RemotePool {
public:
    virtual ~RemotePool() = default;

    // Durably commit checkpoint `id` = `base` (CheckpointId{0} == empty) with
    // `changed` entries applied and `deleted` keys removed. Unchanged keys are
    // inherited from `base`. On return the checkpoint is durable and readable.
    // Called on the runner thread at a checkpoint barrier.
    virtual void commit(CheckpointId id,
                        CheckpointId base,
                        const std::vector<RemotePoolEntry>& changed,
                        const std::vector<RemotePoolKey>& deleted) = 0;

    // Read one (op, key) as-of checkpoint `id`; nullopt if absent. Thread-safe
    // (runs on the backend's IO threads).
    [[nodiscard]] virtual std::optional<StateBackend::Value> read(CheckpointId id,
                                                                  OperatorId op,
                                                                  const std::string& key) const = 0;

    // Batched read of many keys as-of `id` (ASYNC-10): one optional per input
    // key, positionally. The default loops read(); a content-addressed pool
    // overrides it to load the checkpoint manifest ONCE and coalesce keys whose
    // values share a content hash into a single object fetch (distinct keys
    // with identical bytes resolve to the same object). Thread-safe (IO threads).
    [[nodiscard]] virtual std::vector<std::optional<StateBackend::Value>> read_many(
        CheckpointId id, OperatorId op, const std::vector<std::string>& keys) const {
        std::vector<std::optional<StateBackend::Value>> out;
        out.reserve(keys.size());
        for (const auto& k : keys) {
            out.push_back(read(id, op, k));
        }
        return out;
    }

    // Drop checkpoint `id`. v1 keeps the retained set small (the coordinator
    // purges superseded checkpoints); a content-addressed pool refcounts
    // shared objects across live checkpoints.
    virtual void purge(CheckpointId id) = 0;

    // Hook called by RemoteReadBackend::restore() BEFORE it points cold reads at
    // `restore_cp`, giving the pool a chance to MATERIALISE that checkpoint under
    // this subtask's own location when the restore is a rescale or cross-location
    // relocate (the committed state lives under OTHER subtasks' prefixes). The
    // pool merges the relevant parent checkpoints, keeps keyed entries whose
    // key-group is in `kg` (operator-state entries are unioned, never filtered),
    // relocates the referenced objects into its own prefix, and writes the merged
    // manifest as cp-`restore_cp` - so the first post-restore incremental commit
    // (base = restore_cp) inherits the full assigned state instead of an empty
    // base (which would silently drop inherited-but-unmodified keys). Default is
    // a no-op: same-location same-parallelism restore needs nothing (cp-restore_cp
    // already exists under this subtask's prefix and cold reads are lazy).
    virtual void prepare_restore(CheckpointId /*restore_cp*/, const KeyGroupRange& /*kg*/) {}
};

// In-memory RemotePool: each checkpoint is a full materialised key->value map
// (commit copies `base` then applies the delta). The default/test double; the
// S3-backed pool is the production binding. Thread-safe (read on IO threads,
// commit/purge on the runner thread).
class InMemoryRemotePool final : public RemotePool {
public:
    void commit(CheckpointId id,
                CheckpointId base,
                const std::vector<RemotePoolEntry>& changed,
                const std::vector<RemotePoolKey>& deleted) override {
        std::lock_guard<std::mutex> lk(mu_);
        Map next;
        if (base.value() != 0) {
            auto it = ckpts_.find(base.value());
            if (it != ckpts_.end()) {
                next = it->second;  // inherit the base checkpoint
            }
        }
        for (const auto& k : deleted) {
            next.erase(compose_(k.op, k.key));
        }
        for (const auto& e : changed) {
            next[compose_(e.op, e.key)] = e.value;
        }
        ckpts_[id.value()] = std::move(next);
    }

    [[nodiscard]] std::optional<StateBackend::Value> read(CheckpointId id,
                                                          OperatorId op,
                                                          const std::string& key) const override {
        read_calls_.fetch_add(1, std::memory_order_relaxed);
        std::lock_guard<std::mutex> lk(mu_);
        return lookup_(id, op, key);
    }

    // Batched read (one round-trip for the whole key set). Counted separately
    // from read() so a test can prove the runner coalesced N keys into one
    // batched call (read_calls()==0, read_many_calls() small) rather than N
    // single-key round-trips.
    [[nodiscard]] std::vector<std::optional<StateBackend::Value>> read_many(
        CheckpointId id, OperatorId op, const std::vector<std::string>& keys) const override {
        read_many_calls_.fetch_add(1, std::memory_order_relaxed);
        std::lock_guard<std::mutex> lk(mu_);
        std::vector<std::optional<StateBackend::Value>> out;
        out.reserve(keys.size());
        for (const auto& k : keys) {
            out.push_back(lookup_(id, op, k));
        }
        return out;
    }

    void purge(CheckpointId id) override {
        std::lock_guard<std::mutex> lk(mu_);
        ckpts_.erase(id.value());
    }

    // Test instrumentation: pool round-trips. read_calls() counts single-key
    // read()s (the non-coalesced path); read_many_calls() counts batched
    // read_many()s (the coalesced path).
    [[nodiscard]] std::uint64_t read_calls() const noexcept {
        return read_calls_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::uint64_t read_many_calls() const noexcept {
        return read_many_calls_.load(std::memory_order_relaxed);
    }

private:
    using Map = std::unordered_map<std::string, StateBackend::Value>;
    static std::string compose_(OperatorId op, const std::string& key) {
        return std::to_string(op.value()) + '\x1f' + key;
    }
    // Caller holds mu_.
    std::optional<StateBackend::Value> lookup_(CheckpointId id,
                                               OperatorId op,
                                               const std::string& key) const {
        auto cit = ckpts_.find(id.value());
        if (cit == ckpts_.end()) {
            return std::nullopt;
        }
        auto vit = cit->second.find(compose_(op, key));
        if (vit == cit->second.end()) {
            return std::nullopt;
        }
        return vit->second;
    }

    mutable std::mutex mu_;
    std::map<std::uint64_t, Map> ckpts_;
    mutable std::atomic<std::uint64_t> read_calls_{0};
    mutable std::atomic<std::uint64_t> read_many_calls_{0};
};

}  // namespace clink
