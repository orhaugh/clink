#pragma once

// SnapshotStore - abstracts WHERE a state backend's per-checkpoint directory
// lives and HOW it is published and fetched, so the same backend can keep its
// checkpoints on the local filesystem (the default) or in object storage (an
// S3SnapshotStore, DISAGG-2) without changing its snapshot/restore logic.
//
// A backend creates each checkpoint as a local directory, then:
//   - write_checkpoint_dir(local_cp_path, id) publishes it and returns an
//     opaque HANDLE to store in the Snapshot (and later fetch/delete by).
//   - fetch_checkpoint_dir(handle) makes the checkpoint available locally
//     again and returns the local path the backend can open. For a local
//     store this is the identity (the handle already names a local dir).
//   - delete_checkpoint(local_cp_path, id) drops the published checkpoint.
//
// Handles are opaque to the backend; only the store interprets them. The
// default LocalSnapshotStore makes the handle equal to the local path and
// every operation a filesystem identity, so a backend wired with it behaves
// byte-for-byte as it did before the seam existed (the DISAGG-1 contract:
// zero behaviour change, zero test deltas).

#include <filesystem>
#include <string>
#include <system_error>

#include "clink/core/types.hpp"

namespace clink {

class SnapshotStore {
public:
    virtual ~SnapshotStore() = default;

    // Publish the just-created local checkpoint dir for `id`; return the
    // handle to embed in the Snapshot. May move/copy the bytes elsewhere
    // (object storage) or leave them in place (local).
    virtual std::string write_checkpoint_dir(const std::string& local_cp_path, CheckpointId id) = 0;

    // Make the checkpoint named by `handle` available locally and return the
    // local path to open. Identity for a local store.
    virtual std::string fetch_checkpoint_dir(const std::string& handle) = 0;

    // Drop the published checkpoint for `id` (best-effort; tolerate unknown).
    // Given the local cp path so a local store can delete it directly and a
    // remote store can derive its handle from `id` the same way write did.
    virtual void delete_checkpoint(const std::string& local_cp_path, CheckpointId id) = 0;

    // True if write_checkpoint_dir does a slow durable write (e.g. a remote
    // upload) that the backend should move OFF the operator thread via the
    // capture()/persist() async-persist split. A local store returns false
    // (publishing is a cheap in-place no-op), so the backend stays synchronous.
    [[nodiscard]] virtual bool defers_durable_write() const noexcept { return false; }

    [[nodiscard]] virtual std::string description() const = 0;
};

// Default store: the checkpoint dir IS the handle (a local path); publish and
// fetch are identities. Preserves the historic filesystem-only behaviour.
class LocalSnapshotStore final : public SnapshotStore {
public:
    std::string write_checkpoint_dir(const std::string& local_cp_path,
                                     CheckpointId /*id*/) override {
        return local_cp_path;
    }
    std::string fetch_checkpoint_dir(const std::string& handle) override { return handle; }
    void delete_checkpoint(const std::string& local_cp_path, CheckpointId /*id*/) override {
        std::error_code ec;
        std::filesystem::remove_all(local_cp_path, ec);
    }
    [[nodiscard]] std::string description() const override { return "local"; }
};

}  // namespace clink
