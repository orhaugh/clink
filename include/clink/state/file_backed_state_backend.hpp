#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/metrics/state_metrics.hpp"
#include "clink/state/durable_file_write.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/state_backend.hpp"

namespace clink {

// FileBackedStateBackend decorates InMemoryStateBackend with disk
// persistence so checkpoints survive process death.
//
// Wire model:
//   - put/get/erase/scan delegate to an in-memory backend (the working
//     copy that operators read and mutate during processing).
//   - snapshot(id) serialises the in-memory state via the inner backend's
//     codec, then writes the bytes to <dir>/checkpoint-<id>.snap.
//   - restore(snap) reads <dir>/checkpoint-<id>.snap from disk into the
//     in-memory state. If the file is missing, the backend is left empty
//     (callers can treat that as "no prior checkpoint").
//
// The returned Snapshot's `bytes` field is empty - the payload lives on
// disk addressable by checkpoint id. Callers that care about the bytes
// (e.g. testing) can read the file directly.
//
// Per-subtask isolation: instantiate one backend per subtask with a
// distinct directory, or include subtask_idx in the path. Multiple
// subtasks writing to the same directory at the same id would race;
// the backend does not arbitrate that.
//
// This is the v1 distributed-checkpointing surface: subtasks point each
// instance at a job/subtask-specific directory; on resubmit they re-
// open the same directory and restore the latest completed checkpoint.
// A future revision will extend snapshot()/restore() with shared remote
// storage (S3, HDFS) behind the same interface.
class FileBackedStateBackend final : public StateBackend {
public:
    explicit FileBackedStateBackend(std::filesystem::path snapshot_dir)
        : snapshot_dir_(std::move(snapshot_dir)) {
        std::error_code ec;
        std::filesystem::create_directories(snapshot_dir_, ec);
        if (ec) {
            throw std::runtime_error("FileBackedStateBackend: cannot create directory " +
                                     snapshot_dir_.string() + ": " + ec.message());
        }
    }

    void put(OperatorId op, KeyView key, ValueView value) override { inner_.put(op, key, value); }
    std::optional<Value> get(OperatorId op, KeyView key) const override {
        return inner_.get(op, key);
    }
    void erase(OperatorId op, KeyView key) override { inner_.erase(op, key); }

    void scan(OperatorId op, const ScanVisitor& visit) const override { inner_.scan(op, visit); }

    // Synchronous snapshot = capture (serialise) + persist (durable
    // write), fused on the operator thread. The async path drives the two
    // halves separately so the slow write lands on the snapshot worker.
    Snapshot snapshot(CheckpointId id) override { return persist(capture(id)); }

    // FileBacked supports the async split: its capture() is already a
    // fully detached byte blob, so the durable write moves off-thread
    // cleanly with no shared mutable state.
    [[nodiscard]] bool supports_async_persist() const noexcept override { return true; }

    // Operator-thread phase: serialise the in-memory state into a detached
    // blob. No disk I/O here. The returned bytes are owned by the handle,
    // so subsequent put/get/erase on the live backend cannot alter them.
    CaptureHandle capture(CheckpointId id) override {
        auto inner_snap = inner_.snapshot(id);
        return CaptureHandle{.checkpoint_id = id, .bytes = std::move(inner_snap.bytes)};
    }

    // Worker-thread phase: write the captured bytes to disk. The on-disk
    // file is the authoritative record; the returned Snapshot.bytes is
    // left empty so callers don't mistakenly treat it as the recovery
    // payload (the payload lives on disk, addressable by checkpoint id).
    Snapshot persist(CaptureHandle handle) override {
        const auto t0 = std::chrono::steady_clock::now();
        write_and_rename_(handle.checkpoint_id, handle.bytes);
        const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
        clink::metrics::state::snapshot_completed(
            "file_backed", handle.bytes.size(), static_cast<std::uint64_t>(dt));
        return Snapshot{.checkpoint_id = handle.checkpoint_id, .bytes = {}};
    }

    void restore(const Snapshot& snap, const KeyGroupRange& kg_filter = {}) override {
        const auto t0 = std::chrono::steady_clock::now();
        const auto path = path_for(snap.checkpoint_id);
        std::error_code ec;
        if (!std::filesystem::exists(path, ec)) {
            // No checkpoint at this id - leave state empty. Callers that
            // need a strict "found" signal should query has_checkpoint()
            // before calling restore.
            const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                std::chrono::steady_clock::now() - t0)
                                .count();
            clink::metrics::state::restore_completed("file_backed", static_cast<std::uint64_t>(dt));
            return;
        }
        std::ifstream in(path, std::ios::binary);
        if (!in) {
            throw std::runtime_error("FileBackedStateBackend: cannot open " + path.string());
        }
        in.seekg(0, std::ios::end);
        const auto size = in.tellg();
        in.seekg(0, std::ios::beg);
        Snapshot inner_snap;
        inner_snap.checkpoint_id = snap.checkpoint_id;
        inner_snap.bytes.resize(static_cast<std::size_t>(size));
        if (size > 0) {
            in.read(reinterpret_cast<char*>(inner_snap.bytes.data()),
                    static_cast<std::streamsize>(size));
        }
        inner_.restore(inner_snap, kg_filter);
        const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
        clink::metrics::state::restore_completed("file_backed", static_cast<std::uint64_t>(dt));
    }

    // Delete the on-disk snapshot file for `id`. No-op if absent. The
    // live in-memory state and any other checkpoint files are untouched.
    void purge_checkpoint(CheckpointId id) override {
        std::error_code ec;
        std::filesystem::remove(path_for(id), ec);
    }

    std::string description() const override {
        return "file-backed state backend at " + snapshot_dir_.string();
    }

    // Returns true if a snapshot exists on disk for `id`. Useful when
    // the caller wants to distinguish "no prior checkpoint, fresh start"
    // from "checkpoint corrupted / unreadable" rather than silently
    // starting empty.
    [[nodiscard]] bool has_checkpoint(CheckpointId id) const noexcept {
        std::error_code ec;
        return std::filesystem::exists(path_for(id), ec);
    }

    [[nodiscard]] const std::filesystem::path& snapshot_dir() const noexcept {
        return snapshot_dir_;
    }

private:
    // Write `bytes` to <dir>/checkpoint-<id>.snap via a temp file then an
    // atomic rename, so a crash mid-write can never leave a partial
    // checkpoint that a later restore would happily load (silently dropping
    // state). Shared by the synchronous and worker-thread persist paths;
    // touches only the (const) snapshot dir, so it is safe to run off the
    // operator thread concurrently with live put/get/erase.
    void write_and_rename_(CheckpointId id, const std::vector<std::byte>& bytes) const {
        const auto path = path_for(id);
        // Unique temp name per write. The in-process executor points every
        // operator's runner at ONE shared backend, so two async snapshot
        // workers (e.g. an operator's and the sink's) can persist the same
        // checkpoint id concurrently on different threads. A shared ".part"
        // name would let them interleave writes (corruption) and race the
        // rename (one rename throws on a vanished source). A per-write temp
        // makes each writer's partial file private; the final rename is
        // atomic, so the last writer wins with a complete snapshot.
        //
        // write_fsync_rename adds the durability the async move makes
        // affordable: fsync the file before the rename and the dir after, so
        // a returned checkpoint is on stable storage before it is ack'd.
        const auto tmp = path.string() + ".part." +
                         std::to_string(part_seq_.fetch_add(1, std::memory_order_relaxed));
        state::detail::write_fsync_rename(path, tmp, bytes.data(), bytes.size());
    }

    [[nodiscard]] std::filesystem::path path_for(CheckpointId id) const {
        return snapshot_dir_ / ("checkpoint-" + std::to_string(id.value()) + ".snap");
    }

    std::filesystem::path snapshot_dir_;
    InMemoryStateBackend inner_;
    // Monotonic counter for unique per-write temp-file names; lets
    // concurrent persists to a shared backend coexist (see write_and_rename_).
    mutable std::atomic<std::uint64_t> part_seq_{0};
};

}  // namespace clink
