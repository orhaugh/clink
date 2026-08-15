#pragma once

#include <atomic>
#include <cstddef>
#include <cstring>
#include <functional>
#include <map>
#include <mutex>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "clink/core/hash_map.hpp"
#include "clink/state/state_backend.hpp"

namespace clink {

namespace detail {
// Transparent hash so the inner per-op map can be probed with a KeyView
// (string_view) without constructing a std::string on the hot get/erase/put
// path. Paired with std::equal_to<> (a transparent comparator). std::hash is
// guaranteed to hash a string and the matching string_view identically, so the
// heterogeneous lookup finds the same bucket as a std::string key.
struct TransparentStringHash {
    using is_transparent = void;
    std::size_t operator()(std::string_view s) const noexcept {
        return std::hash<std::string_view>{}(s);
    }
    std::size_t operator()(const std::string& s) const noexcept {
        return std::hash<std::string_view>{}(s);
    }
};
}  // namespace detail

// Default state backend. All keyed state is held in-memory in a nested map.
// This is fine for tests, examples, and small workloads, and it stands in for
// RocksDB until that backend is wired up.
class InMemoryStateBackend final : public StateBackend {
public:
    void put(OperatorId op, KeyView key, ValueView value) override {
        std::lock_guard lock(mu_);
        auto& per_op = state_[op];
        // Probe with the view first (no std::string built). On UPDATE - the
        // common RMW case where the same key is rewritten - assign into the
        // existing Value, reusing its heap capacity, so a hot-key put allocates
        // nothing. Only a first INSERT pays a string + value allocation.
        auto it = per_op.find(key);
        if (it != per_op.end()) {
            it->second.assign(reinterpret_cast<const std::byte*>(value.data()),
                              reinterpret_cast<const std::byte*>(value.data()) + value.size());
            return;
        }
        Value bytes(value.size());
        if (!value.empty()) {
            std::memcpy(bytes.data(), value.data(), value.size());
        }
        per_op.emplace(std::string{key}, std::move(bytes));
    }

    std::optional<Value> get(OperatorId op, KeyView key) const override {
        std::lock_guard lock(mu_);
        auto it = state_.find(op);
        if (it == state_.end()) {
            return std::nullopt;
        }
        auto inner = it->second.find(key);  // heterogeneous: no std::string built
        if (inner == it->second.end()) {
            return std::nullopt;
        }
        return inner->second;
    }

    void erase(OperatorId op, KeyView key) override {
        std::lock_guard lock(mu_);
        auto it = state_.find(op);
        if (it == state_.end()) {
            return;
        }
        // std::unordered_map::erase has no heterogeneous overload (pre-C++23),
        // so find (transparent) then erase by iterator - still no string build.
        auto inner = it->second.find(key);
        if (inner != it->second.end()) {
            it->second.erase(inner);
        }
    }

    // Drop all keyed entries (state-version metadata is left intact). Used by
    // wrapping backends (e.g. RemoteReadBackend's hot tier) to discard a stale
    // cache before repointing at a different checkpoint.
    [[nodiscard]] std::optional<std::uint64_t> last_snapshot_bytes() const override {
        const auto v = last_snapshot_bytes_.load(std::memory_order_relaxed);
        return v == kNoSnapshotYet ? std::nullopt : std::optional<std::uint64_t>{v};
    }

    void clear() {
        std::lock_guard lock(mu_);
        state_.clear();
        staged_.clear();
    }

    void scan(OperatorId op, const ScanVisitor& visit) const override {
        // Snapshot under the lock so the visitor isn't holding it (and so
        // the visitor can call back into put/erase without deadlock).
        std::vector<std::pair<std::string, Value>> copy;
        {
            std::lock_guard lock(mu_);
            auto it = state_.find(op);
            if (it == state_.end()) {
                return;
            }
            copy.reserve(it->second.size());
            for (const auto& [k, v] : it->second) {
                copy.emplace_back(k, v);
            }
        }
        for (const auto& [k, v] : copy) {
            const auto* val_data = reinterpret_cast<const char*>(v.data());
            visit(KeyView{k}, ValueView{val_data, v.size()});
        }
    }

    // Snapshot the entire backend state to bytes. As of the Arrow-IPC
    // migration the byte format is an Apache Arrow IPC stream
    // containing one RecordBatch with three columns:
    //
    //   op_id      : uint64    (operator id)
    //   key_bytes  : binary    (the full encoded key: kg byte + slot|user-key)
    //   value_bytes: binary    (raw value bytes, opaque to the backend)
    //
    // One row per (op, key) entry. Empty backends produce a valid
    // (zero-row) Arrow IPC stream. The on-disk format is therefore
    // readable by any Arrow consumer (pyarrow, DuckDB, Polars, etc.).
    //
    // Implementation lives in src/state/in_memory_state_backend.cpp
    // to keep Arrow headers out of the widely-included
    // in_memory_state_backend.hpp.
    Snapshot snapshot(CheckpointId id) override;
    // Pin a barrier-consistent copy of `op`'s rows for checkpoint `id`;
    // snapshot(id) serialises the pinned copy instead of the live rows and
    // releases every staged entry at or below id. See StateBackend for the
    // source-offset tear this closes.
    void stage_operator_rows(OperatorId op, CheckpointId id) override {
        std::lock_guard lock(mu_);
        auto it = state_.find(op);
        // An op with no rows stages an EMPTY copy on purpose: rows the op
        // gains after the barrier must not appear in this checkpoint.
        staged_[id.value()][op] = (it != state_.end()) ? it->second : PerOp{};
    }
    // Live export: the same canonical bytes snapshot() produces, without
    // the checkpoint bookkeeping/metrics (see StateBackend for the
    // consistency caveat).
    [[nodiscard]] std::vector<std::byte> export_arrow_snapshot() const override;
    void restore(const Snapshot& snap, const KeyGroupRange& kg_filter = {}) override;
    // Merge several InMemory snapshots into one (scale-down): folds the
    // IPC streams via merge_snapshot_bytes.
    Snapshot combine_snapshots(std::vector<Snapshot> parts) const override;

    // Stamp (operator, state_type) -> version on every
    // snapshot, recover the same map on restore. The snapshot embeds
    // the packed map in the Arrow IPC schema metadata under
    // "clink.state_versions", so any standard Arrow consumer can see
    // it and tooling like clink_check_savepoint can compare it
    // against the live job's expected versions before a restart.
    void set_state_versions(StateVersionMap versions) override {
        std::lock_guard lock(mu_);
        state_versions_ = std::move(versions);
    }
    StateVersionMap restored_state_versions() const override {
        std::lock_guard lock(mu_);
        return state_versions_;
    }

    std::string description() const override { return "in-memory state backend"; }

    // Enumerate OperatorIds that currently have any keyed entries. Cheap
    // direct lookup over the in-memory map - surfaced for the State
    // Processor API which can't parse Arrow IPC bytes directly.
    [[nodiscard]] std::vector<OperatorId> operator_ids() const {
        std::lock_guard lock(mu_);
        std::vector<OperatorId> out;
        out.reserve(state_.size());
        for (const auto& [op, _] : state_) {
            out.push_back(op);
        }
        return out;
    }

    // Merge N independent Arrow-IPC snapshot byte blobs (each as
    // produced by snapshot()) into one. Used by the StateBackendFactory
    // scale-down path where multiple parent subtasks' snapshots need
    // to fold into a single new-subtask snapshot. Each input must be a
    // valid Arrow IPC stream with the canonical snapshot schema;
    // record batches from each input are concatenated into the output
    // stream. The output is itself a valid Arrow IPC stream that
    // restore() can consume.
    static std::vector<std::byte> merge_snapshot_bytes(
        std::span<const std::vector<std::byte>> parts);

    // Return a snapshot blob containing only the OPERATOR-state rows of the
    // input (keys whose leading byte is >= kNumKeyGroups, i.e.
    // kOperatorStateKeyPrefix). Used by the rescale path to UNION operator
    // state (source offsets, broadcast slots) across all parents into every
    // new subtask while keyed rows stay assigned + key-group-filtered. The
    // output is a valid Arrow IPC stream restore()/merge() can consume.
    static std::vector<std::byte> extract_operator_state_bytes(
        std::span<const std::byte> snapshot_bytes,
        const std::set<std::pair<std::uint64_t, std::string>>* exclude = nullptr);

    // The (operator, key) pairs of the OPERATOR-state rows in a snapshot.
    //
    // A restoring subtask passes its OWN keys as the exclusion above when
    // pulling in its peers' rows, so peers fill in keys it does not have and
    // never overwrite one it does. That distinction is load-bearing: source
    // offsets keyed per partition (Kafka) must cross subtasks, because the
    // broker decides who owns a partition, while sources that keep their
    // position under one fixed key have one row per subtask under the same
    // name - and the merge's keep-the-greater rule would otherwise hand every
    // subtask the furthest position any of them reached.
    static std::set<std::pair<std::uint64_t, std::string>> operator_state_keys(
        std::span<const std::byte> snapshot_bytes);

private:
    // Transparent hash + comparator give heterogeneous (string_view) lookup so
    // the hot path never builds a std::string just to probe.
    using PerOp =
        clink::FlatMap<std::string, Value, detail::TransparentStringHash, std::equal_to<>>;
    using State = clink::FlatMap<OperatorId, PerOp>;

    mutable std::mutex mu_;
    State state_;
    // Barrier-consistent per-checkpoint row copies, keyed by checkpoint id
    // then operator. Written by stage_operator_rows on the staging thread
    // (the source runner's barrier drain), consumed and pruned by
    // snapshot(id). An ordered map so pruning "everything at or below id"
    // is a range erase.
    std::map<std::uint64_t, std::unordered_map<OperatorId, PerOp>> staged_;
    StateVersionMap state_versions_;
    // Set at snapshot(). Atomic and outside mu_ so the worker's heartbeat thread
    // can read it while an operator thread is mutating state, which is the whole
    // point of sampling a snapshot size rather than measuring live state.
    static constexpr std::uint64_t kNoSnapshotYet = ~std::uint64_t{0};
    std::atomic<std::uint64_t> last_snapshot_bytes_{kNoSnapshotYet};
};

}  // namespace clink
