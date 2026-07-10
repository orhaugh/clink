#pragma once

// Changelog State Backend - "Generic Log-Based Incremental
// Checkpointing" analogue.
//
// Wraps an inner StateBackend and maintains a write-ahead log of
// every mutation. Snapshots embed (most-recent materialization +
// log delta since that materialization). The log is reset when a
// materialization happens. Reads pass straight through to the inner
// backend - the log is write-side only.
//
// Design choices :
//
//   * Two materialization storage modes (pick at ctor time):
//
//     1. In-blob (default): both the materialization AND the log
//        live inside the same Snapshot byte payload. Restart-safe
//        on its own; ideal for in-memory inner backends and small
//        materializations.
//
//     2. External store: pass an ExternalMaterializationStore at
//        construction. On each materialization, the inner's
//        snapshot bytes are written to the store and only the
//        returned handle (path / URI) is embedded in the Snapshot.
//        Snapshots stay small even as inner-state size grows;
//        restore reads the handle and fetches from the store.
//        Built-ins: FileMaterializationStore (state/) and
//        RocksDbMaterializationStore (impls/rocksdb/).
//
//     Snapshots produced by mode 2 carry row_kind=
//     kRowMaterializationHandle (3) so they can be distinguished
//     at restore time; restoring a mode-2 snapshot in a mode-1
//     backend (or vice versa) throws with a clear error.
//
//   * Materialization is triggered automatically when the log byte
//     estimate exceeds `materialization_threshold_bytes` (default
//     64 KiB), AND can be forced via materialize_now().  uses
//     a timer + size combo; we keep it size-only for now.
//   * Log entries are typed as put-or-erase. Each put captures the
//     full value bytes (no diff encoding); the cost saving relative
//     to a full snapshot comes from including only mutated keys.

#include <atomic>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>

#include "clink/core/types.hpp"
#include "clink/state/external_materialization_store.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/state_backend.hpp"

namespace clink {

class ChangelogStateBackend final : public StateBackend {
public:
    // Convenience ctor: wraps a fresh InMemoryStateBackend, in-blob
    // materialization storage (no external store).
    ChangelogStateBackend() : ChangelogStateBackend(std::make_shared<InMemoryStateBackend>()) {}

    explicit ChangelogStateBackend(std::shared_ptr<StateBackend> inner)
        : ChangelogStateBackend(std::move(inner), nullptr) {}

    // External-materialization-store ctor. When `store` is non-null,
    // each materialization writes the inner's snapshot bytes to the
    // store and the resulting handle is embedded in the Snapshot
    // (instead of the full payload). Restore fetches the bytes back
    // from the store via the handle.
    ChangelogStateBackend(std::shared_ptr<StateBackend> inner,
                          std::shared_ptr<ExternalMaterializationStore> store)
        : inner_(std::move(inner)), ext_store_(std::move(store)) {
        if (!inner_) {
            throw std::runtime_error("ChangelogStateBackend: inner must not be null");
        }
    }

    // ----- StateBackend overrides ----------------------------------

    void put(OperatorId op, KeyView key, ValueView value) override {
        inner_->put(op, key, value);
        std::lock_guard lock(log_mu_);
        append_or_replace_log_(LogEntry{
            .op = op,
            .key = std::string{key},
            .value = bytes_from_view_(value),
            .is_erase = false,
        });
    }

    [[nodiscard]] std::optional<Value> get(OperatorId op, KeyView key) const override {
        return inner_->get(op, key);
    }

    void erase(OperatorId op, KeyView key) override {
        inner_->erase(op, key);
        std::lock_guard lock(log_mu_);
        append_or_replace_log_(LogEntry{
            .op = op,
            .key = std::string{key},
            .value = {},
            .is_erase = true,
        });
    }

    void scan(OperatorId op, const ScanVisitor& visit) const override { inner_->scan(op, visit); }

    // Snapshot/restore use an Arrow IPC stream containing a single
    // record batch with schema:
    //
    //   row_kind   : uint8     (0 = materialization, 1 = put, 2 = erase)
    //   op_id      : uint64
    //   key_bytes  : binary
    //   value_bytes: binary
    //
    // Row 0 (row_kind=0) carries the inner backend's snapshot bytes -
    // opaque to this layer - in value_bytes; op_id and key_bytes are
    // unused. Subsequent rows carry log entries (put / erase) in
    // insertion order. Restore replays the materialization first,
    // then applies log entries in order.
    //
    // Implementation in src/state/changelog_state_backend.cpp to keep
    // Arrow headers out of this header.
    Snapshot snapshot(CheckpointId id) override;
    // Schema-evolution stamps forward to the INNER backend: it owns the
    // materialisation payload (its snapshot bytes carry the stamps - the
    // in-memory inner's Arrow metadata, RocksDB's reserved default-CF
    // key), so a restore that replays a materialisation recovers them.
    // A restore of a log-only snapshot (no materialisation yet) has no
    // stamps to recover, matching a fresh backend.
    void set_state_versions(StateVersionMap versions) override {
        inner_->set_state_versions(std::move(versions));
    }
    [[nodiscard]] StateVersionMap restored_state_versions() const override {
        return inner_->restored_state_versions();
    }
    // Live export = the INNER backend's current view (reads pass through
    // to it, so it holds the up-to-date state); the write-ahead log is a
    // durability artefact, not part of the live contents.
    [[nodiscard]] std::vector<std::byte> export_arrow_snapshot() const override {
        return inner_->export_arrow_snapshot();
    }
    void restore(const Snapshot& snap, const KeyGroupRange& kg_filter = {}) override;

    // Asynchronous checkpoint split. Only worthwhile when a working dir is
    // set: then snapshot() = capture() (serialise the framing blob under the
    // log lock - the on-thread cost) + persist() (the durable write of
    // <dir>/changelog-<id>.snap - off-thread on the snapshot worker). The
    // in-RAM `changelog://` scheme has no durable write to defer, so it
    // stays synchronous (supports_async_persist() returns false). A persist
    // failure is non-lossy: the checkpoint is never ack'd, so the JM rolls
    // the WHOLE job back to the last completed checkpoint and the sources
    // replay their input since then - a per-subtask backend never needs to
    // retain state newer than the last globally-completed checkpoint, so
    // clearing the live log in capture() before a failed persist is safe.
    [[nodiscard]] bool supports_async_persist() const noexcept override {
        return !snapshot_dir_.empty();
    }
    CaptureHandle capture(CheckpointId id) override;
    Snapshot persist(CaptureHandle handle) override;

    // Frame several framing blobs (each as produced by snapshot()) into a
    // single payload that restore() will split and merge. Used by the
    // scale-down factory path where one new subtask inherits several parent
    // subtasks. A single blob is returned verbatim (no framing), so
    // same-parallelism / scale-up restore stays byte-for-byte unchanged.
    [[nodiscard]] static std::vector<std::byte> frame_blobs(
        std::span<const std::vector<std::byte>> blobs);

    // Set the per-subtask working directory. When non-empty, snapshot()
    // self-persists its framing blob to <dir>/changelog-<id>.snap so a
    // fresh process can restore it (mirrors FileBackedStateBackend); the
    // factory points restore_from at that file. Empty (the default, and
    // the in-blob `changelog://` scheme + hand-constructed tests) keeps
    // the historic behaviour: the blob is returned only, never written.
    void set_snapshot_dir(std::filesystem::path dir) { snapshot_dir_ = std::move(dir); }

    // Remove the on-disk artefacts for `id`: this layer's framing blob
    // and the inner backend's checkpoint (e.g. a RocksDB inner's .cp dir).
    // Called by the retention manager so the per-snapshot blob this
    // backend now writes does not accumulate unbounded. The external
    // materialization payload (mat-<id>.bin) is owned by the store and
    // left to a separate GC pass (see ExternalMaterializationStore::erase).
    void purge_checkpoint(CheckpointId id) override {
        if (!snapshot_dir_.empty()) {
            std::error_code ec;
            std::filesystem::remove(blob_path_(id), ec);
        }
        inner_->purge_checkpoint(id);
    }

    [[nodiscard]] std::string description() const override {
        std::string out = "changelog state backend over (" + inner_->description() + ")";
        if (ext_store_) {
            out += " ext_store=" + ext_store_->description();
        }
        return out;
    }

    // Exposed for tests / inspection; nullptr if the backend uses
    // the in-blob materialization path.
    [[nodiscard]] ExternalMaterializationStore* external_store() const noexcept {
        return ext_store_.get();
    }

    // ----- Changelog-specific controls -----------------------------

    // Force a materialization on the next snapshot. Useful when the
    // pipeline knows it's at a quiescent point and wants a small
    // subsequent snapshot. No-op if the log is empty.
    void materialize_now() {
        std::lock_guard lock(log_mu_);
        if (!log_.empty()) {
            materialize_locked_(CheckpointId{0});
        }
    }

    // Tune the threshold (bytes) at which an automatic materialization
    // is triggered on the next snapshot. Defaults to 64 KiB.
    void set_materialization_threshold_bytes(std::size_t threshold) noexcept {
        std::lock_guard lock(log_mu_);
        materialization_threshold_bytes_ = threshold;
    }

    // Toggle in-place log compaction. When enabled (default true),
    // a put/erase to a key that's already in the log REPLACES the
    // existing entry instead of appending. Reduces log size for
    // workloads that update the same keys repeatedly; the trade-off
    // is one extra hash-map lookup per mutation. Disable when the
    // workload is mostly distinct keys (compaction would be pure
    // overhead) or when a tool needs the full ordered mutation
    // history (none in clink today).
    void set_log_compaction_enabled(bool enabled) noexcept {
        std::lock_guard lock(log_mu_);
        log_compaction_enabled_ = enabled;
        if (!enabled) {
            log_index_.clear();
        }
    }

    // ----- Introspection for tests / metrics -----------------------

    [[nodiscard]] std::size_t log_entries() const {
        std::lock_guard lock(log_mu_);
        return log_.size();
    }

    [[nodiscard]] std::size_t log_bytes_estimate() const {
        std::lock_guard lock(log_mu_);
        return log_bytes_estimate_;
    }

    [[nodiscard]] StateBackend& inner() noexcept { return *inner_; }

private:
    struct LogEntry {
        OperatorId op{};
        std::string key;
        Value value;
        bool is_erase{false};
    };

    // Compose the (op_id, key) hash-map key as bytes-of-op_id || key.
    [[nodiscard]] static std::string compose_index_key_(OperatorId op, std::string_view key) {
        std::string out;
        out.resize(8 + key.size());
        const std::uint64_t v = op.value();
        for (int i = 0; i < 8; ++i) {
            out[static_cast<std::size_t>(i)] = static_cast<char>((v >> ((7 - i) * 8)) & 0xFF);
        }
        if (!key.empty()) {
            std::memcpy(out.data() + 8, key.data(), key.size());
        }
        return out;
    }

    [[nodiscard]] static std::size_t entry_bytes_(const LogEntry& e) {
        return sizeof(LogEntry) + e.key.size() + e.value.size();
    }

    // Append the entry, OR replace an existing in-log entry for the
    // same (op, key) if compaction is enabled. Must be called with
    // log_mu_ held.
    void append_or_replace_log_(LogEntry&& entry) {
        if (log_compaction_enabled_) {
            const auto idx_key = compose_index_key_(entry.op, entry.key);
            auto it = log_index_.find(idx_key);
            if (it != log_index_.end()) {
                const auto pos = it->second;
                log_bytes_estimate_ -= entry_bytes_(log_[pos]);
                log_[pos] = std::move(entry);
                log_bytes_estimate_ += entry_bytes_(log_[pos]);
                return;
            }
            log_index_[idx_key] = log_.size();
        }
        log_bytes_estimate_ += entry_bytes_(entry);
        log_.push_back(std::move(entry));
    }

    // Rebuild log_index_ from log_'s current contents. Called after
    // restore replays log entries (which goes through append_or_replace
    // anyway, so this is the second-line defence if the entry path is
    // bypassed).
    void rebuild_log_index_locked_() {
        log_index_.clear();
        if (!log_compaction_enabled_) {
            return;
        }
        for (std::size_t i = 0; i < log_.size(); ++i) {
            log_index_[compose_index_key_(log_[i].op, log_[i].key)] = i;
        }
    }

    void materialize_locked_(CheckpointId id) {
        last_materialization_ = inner_->snapshot(id);
        if (ext_store_) {
            // Write the materialization bytes to the external store
            // and stash the returned handle. The Snapshot row will
            // carry only the handle; raw payload stays in
            // last_materialization_.bytes for in-memory liveness
            // (next snapshot() reuses it without re-snapshotting
            // the inner if the threshold isn't crossed again).
            last_materialization_handle_ =
                ext_store_->write(id,
                                  std::span<const std::byte>{last_materialization_.bytes.data(),
                                                             last_materialization_.bytes.size()});
        } else {
            last_materialization_handle_.clear();
        }
        log_.clear();
        log_index_.clear();
        log_bytes_estimate_ = 0;
    }

    [[nodiscard]] static bool key_in_range_(const KeyGroupRange& filter, const std::string& key) {
        if (filter.covers_all() || key.empty()) {
            return true;
        }
        const auto first = static_cast<std::uint8_t>(key.front());
        // Operator-state rows carry a reserved prefix byte >= kNumKeyGroups
        // (kOperatorStateKeyPrefix); they have no key group and every subtask
        // must keep them, so they are exempt from key-group narrowing.
        if (first >= kNumKeyGroups) {
            return true;
        }
        return filter.contains(static_cast<KeyGroup>(first));
    }

    [[nodiscard]] std::filesystem::path blob_path_(CheckpointId id) const {
        return snapshot_dir_ / ("changelog-" + std::to_string(id.value()) + ".snap");
    }

    [[nodiscard]] static Value bytes_from_view_(ValueView v) {
        Value out(v.size());
        const auto* src = reinterpret_cast<const std::byte*>(v.data());
        for (std::size_t k = 0; k < v.size(); ++k) {
            out[k] = src[k];
        }
        return out;
    }

    std::shared_ptr<StateBackend> inner_;
    std::shared_ptr<ExternalMaterializationStore> ext_store_;
    Snapshot last_materialization_{};
    std::string last_materialization_handle_;  // non-empty only when ext_store_ != nullptr
    mutable std::mutex log_mu_;
    std::vector<LogEntry> log_;
    // (op, key) -> index into log_. Used by in-place compaction so a
    // mutation to a key already in the log REPLACES its prior entry
    // instead of appending. Reset together with log_ at materialize.
    std::unordered_map<std::string, std::size_t> log_index_;
    bool log_compaction_enabled_{true};
    std::size_t log_bytes_estimate_{0};
    std::size_t materialization_threshold_bytes_{64ULL * 1024};
    // Per-subtask working dir; when set, snapshot() persists its framing
    // blob here so a fresh process can restore. Empty = in-RAM only.
    std::filesystem::path snapshot_dir_;
    // Unique per-write temp-file suffix, so concurrent async persists to a
    // shared backend (the in-process executor points every runner at one
    // backend) cannot collide on the same ".part" file. See FileBacked.
    mutable std::atomic<std::uint64_t> part_seq_{0};
};

}  // namespace clink
