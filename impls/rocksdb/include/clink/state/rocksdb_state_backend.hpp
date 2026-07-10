#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "clink/state/state_backend.hpp"

namespace clink {

// DISAGG-1: pluggable "where checkpoints live + how they're published/fetched".
// Forward-declared so Options can carry a shared_ptr without this header
// pulling the full definition; the default (null) means LocalSnapshotStore.
class SnapshotStore;

// RocksDB-backed keyed state.
//
// The implementation is in src/state/rocksdb_state_backend.cpp. When CMake
// finds a usable RocksDB install (`find_package(RocksDB CONFIG)`), the .cpp
// is compiled with `CLINK_HAS_ROCKSDB` defined and links against
// RocksDB::rocksdb. When RocksDB isn't available, the .cpp compiles a stub
// implementation that throws on construction.
//
// Public ABI is identical either way, so callers don't need conditional code;
// they just have to handle the construction-time exception when the dep is
// missing.
//
// Incremental checkpoints. RocksDB SST files are immutable once
// written - a new checkpoint differs from the previous one only by
// the few SSTs that LSM compaction created or rewrote since the
// previous checkpoint. snapshot() hard-links the live SSTs into a
// per-checkpoint dir, which means:
//   * SSTs unchanged across N checkpoints share a single physical
//     file via filesystem hard links (incremental space cost).
//   * Each checkpoint dir is a complete, openable RocksDB instance -
//     no need to chase a chain of deltas at restore time.
//   * Deleting a checkpoint dir drops one hard-link to each SST; the
//     filesystem GCs the SST iff no other checkpoint references it.
// `snapshot_stats()` exposes the per-checkpoint SST list for tests
// and for tooling that wants to size up incremental storage.
class RocksDBStateBackend final : public StateBackend {
public:
    struct Options {
        std::string path;
        bool create_if_missing{true};
        // Where checkpoint dirs are published/fetched. Null = LocalSnapshotStore
        // (historic filesystem behaviour); an S3SnapshotStore disaggregates them
        // to object storage. See clink/state/snapshot_store.hpp.
        std::shared_ptr<SnapshotStore> snapshot_store;
        // FOUND-3 (relocatable savepoints): the directory the savepoint's
        // checkpoint dirs now live under. When set, restore() rebases each
        // cp-dir reference by its BASENAME against this dir, so a savepoint
        // moved to a new path (or machine) restores even though snap.bytes
        // embed the capture-time absolute path. Empty = use the embedded path
        // verbatim (same-location restart, the historic behaviour). Only
        // meaningful for the local filesystem store; never set for S3 (object
        // handles relocate via bucket/prefix, not a local rebase).
        std::string restore_base;
    };

    // Per-checkpoint introspection. Returned by snapshot_stats() so
    // tests and dashboards can observe the incremental sharing
    // ratio: out of `total_sst_count` SSTs, how many were already
    // present in the previous checkpoint (`shared_sst_count`).
    struct SnapshotStats {
        std::uint64_t checkpoint_id{0};
        std::vector<std::string> sst_files;  // basenames (e.g. "000123.sst")
        std::size_t total_sst_count{0};
        std::size_t shared_sst_count{0};  // SSTs also in the prior snapshot
    };

    explicit RocksDBStateBackend(Options opts);
    ~RocksDBStateBackend() override;

    RocksDBStateBackend(const RocksDBStateBackend&) = delete;
    RocksDBStateBackend& operator=(const RocksDBStateBackend&) = delete;
    RocksDBStateBackend(RocksDBStateBackend&&) = delete;
    RocksDBStateBackend& operator=(RocksDBStateBackend&&) = delete;

    void put(OperatorId op, KeyView key, ValueView value) override;
    std::optional<Value> get(OperatorId op, KeyView key) const override;
    void erase(OperatorId op, KeyView key) override;
    void scan(OperatorId op, const ScanVisitor& visit) const override;
    Snapshot snapshot(CheckpointId id) override;
    // DISAGG-3: async-persist split, enabled only when the SnapshotStore
    // defers a durable write (a remote store). capture() does the cheap local
    // RocksDB Checkpoint::Create on the operator thread; persist() publishes
    // (uploads) it off-thread on the SnapshotWorker, preserving ack-after-
    // durable. With the default LocalSnapshotStore these stay unused and
    // snapshot() runs fully synchronously.
    [[nodiscard]] bool supports_async_persist() const noexcept override;
    CaptureHandle capture(CheckpointId id) override;
    Snapshot persist(CaptureHandle handle) override;
    // Schema-evolution version stamps. Persisted under a reserved key in
    // the DEFAULT column family (never touched by the keyed path), so
    // every checkpoint carries them: restore recovers the stamps from
    // the checkpoint dir, and the Arrow exports embed them in the
    // stream's schema metadata. Previously RocksDB checkpoints carried
    // no StateVersionMap, so a restore treated any stored state as v1.
    void set_state_versions(StateVersionMap versions) override;
    [[nodiscard]] StateVersionMap restored_state_versions() const override;
    void restore(const Snapshot& snap, const KeyGroupRange& kg_filter = {}) override;
    // FOUND-3: set the relocated savepoint directory used to rebase cp-dir
    // references on the next restore (see Options::restore_base).
    void set_restore_base(const std::string& dir) override;
    // Merge several RocksDB snapshots into one (scale-down): joins their
    // checkpoint-dir paths so restore() iterate-merges them.
    Snapshot combine_snapshots(std::vector<Snapshot> parts) const override;
    std::string description() const override;

    // Returns the SnapshotStats for the most recent snapshot()
    // call. nullopt before the first snapshot. Stats include the SST
    // basenames so tests can stat() each one and verify cross-snapshot
    // hard-link sharing via inode equality.
    [[nodiscard]] std::optional<SnapshotStats> last_snapshot_stats() const;

    // Delete a previously-taken checkpoint dir. Drops one hard-link
    // to each SST; the SST disappears from disk iff no other live
    // checkpoint references it. Safe to call on an unknown id (no-op).
    void purge_checkpoint(CheckpointId id) override;

    // Returns true iff the implementation was compiled with real RocksDB
    // support. Useful for tests that want to skip when the dep is missing.
    static bool is_real_implementation();

    // State-as-data: render the backend's LIVE contents as the engine's
    // canonical Arrow IPC state-snapshot stream (op_id / key_bytes /
    // value_bytes - the exact format the in-memory family snapshots in),
    // so RocksDB-held state is readable by any Arrow consumer and by
    // every snapshot-format tool (state-cat, state-diff, the State
    // Processor, InMemoryStateBackend::restore). Point-in-time view (a
    // RocksDB snapshot pins the iteration); buffered writes are flushed
    // first. Operators are emitted in ascending op-id order and keys in
    // RocksDB's byte order, so the output is deterministic. The backend's
    // StateVersionMap rides the stream's schema metadata (see
    // set_state_versions), so check-savepoint and migrate-at-restore see
    // real stamps on exported RocksDB state. The native SST checkpoint
    // path is unchanged - this is an export, not a replacement.
    [[nodiscard]] std::vector<std::byte> export_arrow_snapshot() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

// Offline companion to export_arrow_snapshot(): open a RocksDB
// checkpoint directory (as produced by RocksDBStateBackend::snapshot -
// a complete read-only RocksDB instance) WITHOUT a live backend and
// render its contents as the same canonical Arrow IPC stream. The dir
// is opened read-only, so a checkpoint shared with a running job is
// never mutated or locked. Throws when the dir is not an openable
// RocksDB instance or the build lacks RocksDB support.
std::vector<std::byte> rocksdb_checkpoint_to_arrow(const std::string& checkpoint_dir);

}  // namespace clink
