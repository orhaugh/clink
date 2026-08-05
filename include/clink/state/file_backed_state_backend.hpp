#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "clink/metrics/state_metrics.hpp"
#include "clink/runtime/log_buffer.hpp"
#include "clink/state/checkpoint_integrity.hpp"
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

    // Set in persist(), which both the sync and async paths go through.
    [[nodiscard]] std::optional<std::uint64_t> last_snapshot_bytes() const override {
        const auto v = last_snapshot_bytes_.load(std::memory_order_relaxed);
        return v == kNoSnapshotYet ? std::nullopt : std::optional<std::uint64_t>{v};
    }
    [[nodiscard]] std::vector<std::byte> export_arrow_snapshot() const override {
        return inner_.export_arrow_snapshot();
    }

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
        last_snapshot_bytes_.store(static_cast<std::uint64_t>(handle.bytes.size()),
                                   std::memory_order_relaxed);
        clink::metrics::state::snapshot_completed(
            "file_backed", handle.bytes.size(), static_cast<std::uint64_t>(dt));
        return Snapshot{.checkpoint_id = handle.checkpoint_id, .bytes = {}};
    }

    void restore(const Snapshot& snap, const KeyGroupRange& kg_filter = {}) override {
        const auto t0 = std::chrono::steady_clock::now();
        // BEFORE the in-memory branch below, deliberately. The point is named
        // "before restore" and has to fire on every restore this backend performs,
        // whichever source the bytes come from. It first sat below that branch, and
        // the branch returns early - so arming state.before_restore stopped killing
        // anything and FaultRecoveryTest.WorkerKilledAtTheStateRestorePointIsRedeployed
        // failed with "the armed worker never reached state.before_restore". Note
        // what check-fault-points.sh can and cannot do: it proves a declared point
        // has a CALL SITE, not that the call site is still reachable.
        CLINK_FAULT_POINT(clink::fault::points::kStateBeforeRestore);
        // Bytes supplied by the caller are authoritative and are NOT looked for on
        // disk. That is how a rescale hands over the state stitched from its parent
        // subtasks: it must not stage those bytes as a file in this subtask's
        // directory first, because global subtask indices shift when an operator is
        // resized, so the staging write lands on a directory that still belongs to
        // another operator - and on the very file that this subtask's siblings are
        // reading as their own parent. See build_file in state_backend_factory.cpp.
        // The changelog builder has always passed bytes this way; the file builder
        // wrote a file instead, and that write was the aliasing.
        if (!snap.bytes.empty()) {
            inner_.restore(snap, kg_filter);
            const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                std::chrono::steady_clock::now() - t0)
                                .count();
            clink::metrics::state::restore_completed("file_backed", static_cast<std::uint64_t>(dt));
            return;
        }
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
        // A payload that exists but does not verify must NOT be loaded. The
        // old behaviour read whatever bytes were there and handed them to
        // the Arrow reader; a truncated stream that happens to end on a
        // record-batch boundary decodes as a smaller, plausible-looking
        // state and the job resumes having silently dropped keys. Raising
        // here is what lets the caller fall back to an older checkpoint
        // (latest_valid_checkpoint below) instead of continuing on state
        // nobody certified.
        if (const auto verdict = state::verify_checkpoint(path); !verdict.ok()) {
            if (!state::unverified_checkpoints_allowed(verdict)) {
                throw state::CheckpointIntegrityError(verdict.status, verdict.detail);
            }
            clink::log::warn("state.restore",
                             "loading an unverified checkpoint because "
                             "CLINK_ALLOW_UNVERIFIED_CHECKPOINTS is set: " +
                                 verdict.detail);
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
        // Sidecar first: a payload with no sidecar reads as incomplete and
        // is skipped, whereas a sidecar with no payload would be a dangling
        // record. Removing in this order means an interrupted purge always
        // leaves the directory in a state recovery already handles.
        std::filesystem::remove(state::meta_path_for(path_for(id)), ec);
        std::filesystem::remove(path_for(id), ec);
    }

    // Verify one checkpoint without loading it.
    [[nodiscard]] state::VerifyResult verify_checkpoint(CheckpointId id) const {
        return state::verify_checkpoint(path_for(id));
    }

    // Highest checkpoint id in this directory that passes verification,
    // considering only ids <= `at_most` (0 = no ceiling).
    //
    // This is the fallback rule the recovery path needs: a corrupt or
    // half-written NEWEST checkpoint must not strand a job that has a
    // perfectly good older one. Every id it rejects is reported through
    // `rejected` so the caller can log precisely what was skipped and why -
    // silently rewinding to an older checkpoint would hide data loss.
    [[nodiscard]] std::optional<CheckpointId> latest_valid_checkpoint(
        std::uint64_t at_most = 0,
        std::vector<std::pair<CheckpointId, state::VerifyResult>>* rejected = nullptr) const {
        std::vector<std::uint64_t> ids;
        std::error_code ec;
        for (const auto& entry : std::filesystem::directory_iterator(snapshot_dir_, ec)) {
            if (!entry.is_regular_file()) {
                continue;
            }
            const auto name = entry.path().filename().string();
            constexpr std::string_view kPrefix = "checkpoint-";
            constexpr std::string_view kSuffix = ".snap";
            if (name.rfind(kPrefix, 0) != 0 || name.size() <= kPrefix.size() + kSuffix.size()) {
                continue;
            }
            if (name.compare(name.size() - kSuffix.size(), kSuffix.size(), kSuffix) != 0) {
                continue;
            }
            const auto digits =
                name.substr(kPrefix.size(), name.size() - kPrefix.size() - kSuffix.size());
            if (digits.empty() || digits.find_first_not_of("0123456789") != std::string::npos) {
                continue;
            }
            const auto id = std::stoull(digits);
            if (at_most != 0 && id > at_most) {
                continue;
            }
            ids.push_back(id);
        }
        std::sort(ids.begin(), ids.end(), std::greater<>());
        for (const auto id : ids) {
            const CheckpointId cid{id};
            auto verdict = state::verify_checkpoint(path_for(cid));
            if (verdict.ok()) {
                return cid;
            }
            if (rejected != nullptr) {
                rejected->emplace_back(cid, std::move(verdict));
            }
        }
        return std::nullopt;
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
    static constexpr std::uint64_t kNoSnapshotYet = ~std::uint64_t{0};
    std::atomic<std::uint64_t> last_snapshot_bytes_{kNoSnapshotYet};

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
        // Publication point. The payload is durable; the sidecar is what
        // certifies it. Written second and separately so that dying between
        // the two leaves an unpublished (incomplete) checkpoint rather than
        // a valid-looking one - see checkpoint_integrity.hpp.
        state::write_checkpoint_meta(path, id.value(), bytes.data(), bytes.size());
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
