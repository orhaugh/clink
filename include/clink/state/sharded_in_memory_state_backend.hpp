#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "clink/runtime/key_groups.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/state_backend.hpp"

namespace clink {

// Key-group-sharded in-memory state backend.
//
// The single mutex in InMemoryStateBackend serializes every keyed put/get
// across all operator threads sharing one backend (parallelism > 1, keyed
// fan-out). This splits the state across kNumShards independent
// InMemoryStateBackend shards, each with its own mutex, so concurrent access
// to different key groups proceeds without contention - the foundational
// "sharded keyed state" half of shard-per-core execution.
//
// Opt-in: the default backend stays the single-mutex InMemoryStateBackend
// (sharding is marginally slower for small/low-concurrency jobs and a skewed
// key set still serializes on one shard). Selected via the
// "memory+sharded://" factory scheme.
//
// Backward/format compatible: snapshots are the SAME canonical Arrow IPC the
// mono backend emits (this just merges per-shard blobs), so a mono snapshot
// restores into a sharded backend and vice versa, and the key-group rescale
// filter is unchanged. All the Arrow serialize/parse/kg-filter/operator-state
// max-merge logic is reused from InMemoryStateBackend - this is a pure
// sharding layer with no new serialization code.
//
// The shard index is the leading key byte (which the keyed-state encoder
// already sets to the key group, key_groups.hpp) modulo kNumShards. Raw and
// operator-state keys map deterministically too (operator-state, leading byte
// >= kNumKeyGroups, lands on shard 0). The only invariant that matters is
// "same key -> same shard" on put, get and restore, which holds because the
// key bytes survive a snapshot/restore round-trip verbatim.
class ShardedInMemoryStateBackend final : public StateBackend {
public:
    static constexpr std::size_t kNumShards = 16;

    void put(OperatorId op, KeyView key, ValueView value) override {
        shard_for_(key).put(op, key, value);
    }
    std::optional<Value> get(OperatorId op, KeyView key) const override {
        return shard_for_(key).get(op, key);
    }
    void erase(OperatorId op, KeyView key) override { shard_for_(key).erase(op, key); }

    void scan(OperatorId op, const ScanVisitor& visit) const override {
        for (const auto& shard : shards_) {
            shard.scan(op, visit);
        }
    }

    // Merge the per-shard snapshots into one canonical Arrow IPC blob, so the
    // bytes are interchangeable with InMemoryStateBackend. Shard 0 is first,
    // so its schema metadata (the state-version map) heads the merged stream.
    [[nodiscard]] std::vector<std::byte> export_arrow_snapshot() const override {
        std::vector<std::vector<std::byte>> parts;
        parts.reserve(shards_.size());
        for (const auto& shard : shards_) {
            parts.push_back(shard.export_arrow_snapshot());
        }
        return InMemoryStateBackend::merge_snapshot_bytes(parts);
    }

    Snapshot snapshot(CheckpointId id) override {
        std::vector<std::vector<std::byte>> blobs;
        blobs.reserve(kNumShards);
        for (auto& shard : shards_) {
            blobs.push_back(shard.snapshot(id).bytes);
        }
        return Snapshot{.checkpoint_id = id,
                        .bytes = InMemoryStateBackend::merge_snapshot_bytes(blobs)};
    }

    void restore(const Snapshot& snap, const KeyGroupRange& kg_filter = {}) override {
        // Reuse InMemory's parse + key-group filter + operator-state max-merge
        // in a temp backend, then redistribute its rows to the shards. Restore
        // is a one-time startup cost, not the hot path.
        InMemoryStateBackend tmp;
        tmp.restore(snap, kg_filter);
        const auto versions = tmp.restored_state_versions();
        for (auto& shard : shards_) {
            shard.set_state_versions(versions);
        }
        for (const auto op : tmp.operator_ids()) {
            tmp.scan(op, [&](KeyView k, ValueView v) { shard_for_(k).put(op, k, v); });
        }
    }

    Snapshot combine_snapshots(std::vector<Snapshot> parts) const override {
        std::vector<std::vector<std::byte>> blobs;
        blobs.reserve(parts.size());
        CheckpointId id{};
        for (auto& part : parts) {
            id = part.checkpoint_id;
            blobs.push_back(std::move(part.bytes));
        }
        return Snapshot{.checkpoint_id = id,
                        .bytes = InMemoryStateBackend::merge_snapshot_bytes(blobs)};
    }

    void set_state_versions(StateVersionMap versions) override {
        for (auto& shard : shards_) {
            shard.set_state_versions(versions);
        }
    }
    StateVersionMap restored_state_versions() const override {
        return shards_.front().restored_state_versions();
    }

    std::string description() const override {
        return "sharded in-memory state backend (" + std::to_string(kNumShards) + " shards)";
    }

    // State Processor API parity: enumerate OperatorIds with any entries,
    // deduped across shards.
    [[nodiscard]] std::vector<OperatorId> operator_ids() const {
        std::vector<OperatorId> out;
        std::unordered_set<std::uint64_t> seen;
        for (const auto& shard : shards_) {
            for (const auto op : shard.operator_ids()) {
                if (seen.insert(op.value()).second) {
                    out.push_back(op);
                }
            }
        }
        return out;
    }

private:
    // Routes by the leading key byte, which the keyed-state encoder sets to the
    // key group (< kNumKeyGroups) and operator-state rows set to 0xFF. So keyed
    // rows fan across shards by key group and operator-state rows all collapse
    // onto shard 0. The mapping is a pure function of the key bytes, so the same
    // key always lands on the same shard across put/get/erase/scan/restore (the
    // bytes survive the snapshot round-trip verbatim). This is NOT a general
    // hash-sharded map: a raw key whose leading byte is >= kNumKeyGroups but
    // which is not an encoded operator-state row also collapses onto shard 0.
    static std::size_t shard_index_(KeyView key) noexcept {
        if (key.empty()) {
            return 0;
        }
        const auto leading = static_cast<std::uint8_t>(key.front());
        const std::size_t kg = leading < kNumKeyGroups ? leading : 0;
        return kg % kNumShards;
    }
    InMemoryStateBackend& shard_for_(KeyView key) { return shards_[shard_index_(key)]; }
    const InMemoryStateBackend& shard_for_(KeyView key) const { return shards_[shard_index_(key)]; }

    std::array<InMemoryStateBackend, kNumShards> shards_{};
};

}  // namespace clink
