#pragma once

// ExternalMaterializationStore - pluggable storage for the
// ChangelogStateBackend's materialization payloads.
//
// In the simplest mode, ChangelogStateBackend writes both its
// materialization (a periodic snapshot of the inner backend) AND the
// recent log-entry delta into the same Snapshot blob. That works for
// in-memory inner backends and small materializations, but it
// defeats the "small incremental snapshot" property the changelog
// design is supposed to provide once the materialization grows large.
//
// When a backend is constructed with an ExternalMaterializationStore,
// the materialization payload is written to the store on each
// materialization. The Snapshot blob contains only a *handle*
// (typically a path or URI string) plus the log delta. Restore reads
// the handle from the snapshot, fetches the payload from the store,
// and feeds it to the inner backend.
//
// Two implementations ship in-tree:
//   * FileMaterializationStore (this directory) - files on local disk.
//   * RocksDbMaterializationStore (impls/rocksdb) - values in a
//     dedicated RocksDB database. Linked only when CLINK_BUILD_ROCKSDB
//     is on; not a hard core dependency.
//
// Users can implement their own (S3, GCS, Redis, ...). The handle is
// opaque to the changelog backend.

#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "clink/core/types.hpp"

namespace clink {

class ExternalMaterializationStore {
public:
    virtual ~ExternalMaterializationStore() = default;

    // Persist `bytes` under a key derived from `id`. Returns an opaque
    // handle string that can be stored in a Snapshot row and later
    // passed to read() to recover the bytes. Implementations may
    // overwrite an existing entry for the same `id` - the changelog
    // backend takes a fresh handle on every materialization.
    virtual std::string write(CheckpointId id, std::span<const std::byte> bytes) = 0;

    // Retrieve the bytes previously written under `handle`. Implementations
    // throw std::runtime_error if the handle is unknown or unreadable.
    virtual std::vector<std::byte> read(const std::string& handle) = 0;

    // Optional cleanup of an obsolete handle. The default does
    // nothing - callers that want GC pass the handle through. The
    // changelog backend does not currently invoke this; once a v2
    // pass implements multi-checkpoint retention it can call
    // erase() on the superseded handle. Implementations should
    // tolerate erasing an unknown handle as a no-op.
    virtual void erase(const std::string& /*handle*/) {}

    // Human-readable describe (used in StateBackend::description()).
    [[nodiscard]] virtual std::string description() const = 0;
};

}  // namespace clink
