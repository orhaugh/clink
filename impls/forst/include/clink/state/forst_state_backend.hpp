#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "clink/state/state_backend.hpp"

namespace clink {

// Pluggable "where checkpoints live + how they're published/fetched".
// Forward-declared so Options can carry a shared_ptr without this header
// pulling the full definition; the default (null) means LocalSnapshotStore.
class SnapshotStore;

// ForSt-backed keyed state.
//
// ForSt (https://github.com/ververica/ForSt) is an LSM key-value store of
// RocksDB lineage. clink builds it from the pinned upstream tag (see
// impls/forst/CMakeLists.txt) and links it statically under its own
// `forstdb` C++ namespace, so this backend coexists with the bundled
// RocksDB backend in one binary. The implementation is in
// impls/forst/src/forst_state_backend.cpp and only exists when the build
// opts in with CLINK_WITH_FORST=ON.
//
// Incremental checkpoints. ForSt SST files are immutable once written -
// a new checkpoint differs from the previous one only by the few SSTs
// that LSM compaction created or rewrote since the previous checkpoint.
// snapshot() hard-links the live SSTs into a per-checkpoint dir, which
// means:
//   * SSTs unchanged across N checkpoints share a single physical file
//     via filesystem hard links (incremental space cost).
//   * Each checkpoint dir is a complete, openable ForSt instance - no
//     need to chase a chain of deltas at restore time.
//   * Deleting a checkpoint dir drops one hard-link to each SST; the
//     filesystem GCs the SST iff no other checkpoint references it.
// `snapshot_stats()` exposes the per-checkpoint SST list for tests and
// for tooling that wants to size up incremental storage.
class ForStStateBackend final : public StateBackend {
public:
    struct Options {
        std::string path;
        bool create_if_missing{true};
        // Where checkpoint dirs are published/fetched. Null = LocalSnapshotStore
        // (plain filesystem behaviour); an S3SnapshotStore disaggregates them
        // to object storage. See clink/state/snapshot_store.hpp.
        std::shared_ptr<SnapshotStore> snapshot_store;
        // Relocatable savepoints: the directory the savepoint's checkpoint
        // dirs now live under. When set, restore() rebases each cp-dir
        // reference by its BASENAME against this dir, so a savepoint moved
        // to a new path (or machine) restores even though snap.bytes embed
        // the capture-time absolute path. Empty = use the embedded path
        // verbatim (same-location restart). Only meaningful for the local
        // filesystem store; never set for S3 (object handles relocate via
        // bucket/prefix, not a local rebase).
        std::string restore_base;
    };

    // Per-checkpoint introspection. Returned by snapshot_stats() so tests
    // and dashboards can observe the incremental sharing ratio: out of
    // `total_sst_count` SSTs, how many were already present in the
    // previous checkpoint (`shared_sst_count`).
    struct SnapshotStats {
        std::uint64_t checkpoint_id{0};
        std::vector<std::string> sst_files;  // basenames (e.g. "000123.sst")
        std::size_t total_sst_count{0};
        std::size_t shared_sst_count{0};  // SSTs also in the prior snapshot
    };

    explicit ForStStateBackend(Options opts);
    ~ForStStateBackend() override;

    ForStStateBackend(const ForStStateBackend&) = delete;
    ForStStateBackend& operator=(const ForStStateBackend&) = delete;
    ForStStateBackend(ForStStateBackend&&) = delete;
    ForStStateBackend& operator=(ForStStateBackend&&) = delete;

    void put(OperatorId op, KeyView key, ValueView value) override;
    std::optional<Value> get(OperatorId op, KeyView key) const override;
    void erase(OperatorId op, KeyView key) override;
    void scan(OperatorId op, const ScanVisitor& visit) const override;
    Snapshot snapshot(CheckpointId id) override;
    // Async-persist split, enabled only when the SnapshotStore defers a
    // durable write (a remote store). capture() does the cheap local
    // Checkpoint::Create on the operator thread; persist() publishes
    // (uploads) it off-thread on the SnapshotWorker, preserving
    // ack-after-durable. With the default LocalSnapshotStore these stay
    // unused and snapshot() runs fully synchronously.
    [[nodiscard]] bool supports_async_persist() const noexcept override;
    CaptureHandle capture(CheckpointId id) override;
    Snapshot persist(CaptureHandle handle) override;
    // Schema-evolution version stamps. Persisted under a reserved key in
    // the DEFAULT column family (never touched by the keyed path), so
    // every checkpoint carries them: restore recovers the stamps from the
    // checkpoint dir, and the Arrow exports embed them in the stream's
    // schema metadata.
    void set_state_versions(StateVersionMap versions) override;
    [[nodiscard]] StateVersionMap restored_state_versions() const override;
    void restore(const Snapshot& snap, const KeyGroupRange& kg_filter = {}) override;
    // Set the relocated savepoint directory used to rebase cp-dir
    // references on the next restore (see Options::restore_base).
    void set_restore_base(const std::string& dir) override;
    // Merge several ForSt snapshots into one (scale-down): joins their
    // checkpoint-dir paths so restore() iterate-merges them.
    Snapshot combine_snapshots(std::vector<Snapshot> parts) const override;
    std::string description() const override;

    // Returns the SnapshotStats for the most recent snapshot() call.
    // nullopt before the first snapshot. Stats include the SST basenames
    // so tests can stat() each one and verify cross-snapshot hard-link
    // sharing via inode equality.
    [[nodiscard]] std::optional<SnapshotStats> last_snapshot_stats() const;

    // Delete a previously-taken checkpoint dir. Drops one hard-link to
    // each SST; the SST disappears from disk iff no other live checkpoint
    // references it. Safe to call on an unknown id (no-op).
    void purge_checkpoint(CheckpointId id) override;

    // State-as-data: render the backend's LIVE contents as the engine's
    // canonical Arrow IPC state-snapshot stream (op_id / key_bytes /
    // value_bytes - the exact format the in-memory family snapshots in),
    // so ForSt-held state is readable by any Arrow consumer and by every
    // snapshot-format tool (state-cat, state-diff, the State Processor,
    // InMemoryStateBackend::restore). Point-in-time view (an engine
    // snapshot pins the iteration); buffered writes are flushed first.
    // Operators are emitted in ascending op-id order and keys in the
    // engine's byte order, so the output is deterministic. The backend's
    // StateVersionMap rides the stream's schema metadata (see
    // set_state_versions), so check-savepoint and migrate-at-restore see
    // real stamps on exported ForSt state. The native SST checkpoint path
    // is unchanged - this is an export, not a replacement.
    [[nodiscard]] std::vector<std::byte> export_arrow_snapshot() const override;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

// Offline companion to export_arrow_snapshot(): open a ForSt checkpoint
// directory (as produced by ForStStateBackend::snapshot - a complete
// read-only ForSt instance) WITHOUT a live backend and render its
// contents as the same canonical Arrow IPC stream. The dir is opened
// read-only, so a checkpoint shared with a running job is never mutated
// or locked. Throws when the dir is not an openable ForSt instance.
std::vector<std::byte> forst_checkpoint_to_arrow(const std::string& checkpoint_dir);

}  // namespace clink
