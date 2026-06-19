#pragma once

// Durable on-disk store for a ShardedKeyedStage's merged checkpoint snapshots.
//
// ShardedKeyedStage::checkpoint() returns the merged snapshot but does not
// persist it - persistence is the DAG runner's job, via the add_sharded_keyed
// `on_checkpoint` hook. This store is the built-in durable implementation: it
// writes each checkpoint's merged Arrow IPC bytes to
// <dir>/sharded-<label>-<id>.snap with write_fsync_rename (durable before the
// persist call returns, so the runner's ack is honestly ack-after-durable), and
// loads them back on a fresh process for restore.
//
// `label` namespaces the files within a shared dir (use the stage's uid, which
// is also what derives its OperatorId, so the persisted rows and the restored
// stage agree). Cross-process: run 1 persist()s; a fresh run 2 constructs a
// store over the same dir+label, load_latest()s, and hands the snapshot to
// add_sharded_keyed's restore_from. The bytes are the SAME canonical Arrow IPC
// the mono/sharded backends use, so the snapshot is shard-count-agnostic - a
// snapshot taken at S shards restores into a stage with a DIFFERENT shard count
// (rescale), since restore() splits the full blob by each shard's key-group
// range regardless of how many shards wrote it.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <system_error>
#include <vector>

#include "clink/core/types.hpp"
#include "clink/state/durable_file_write.hpp"
#include "clink/state/state_backend.hpp"

namespace clink {

class ShardedCheckpointStore {
public:
    ShardedCheckpointStore(std::filesystem::path dir, std::string label)
        : dir_(std::move(dir)), label_(std::move(label)) {}

    // Durably write the merged snapshot for checkpoint `id`. Returns false on
    // any I/O error (so the caller acks the checkpoint as failed) rather than
    // throwing. Durable-before-return via write_fsync_rename.
    //
    // Refuses an EMPTY snapshot: a successful checkpoint of a running stage is
    // always non-empty (each shard writes schema+batch+EOS, the merge closes a
    // stream-with-schema), so empty bytes mean a FAILED checkpoint. Persisting it
    // would write an empty file at a higher id that shadows the last good
    // checkpoint on load_latest() - data loss. Defends the store even if a caller
    // forwards a failed result.
    [[nodiscard]] bool persist(CheckpointId id, const Snapshot& snap) const {
        if (snap.bytes.empty()) {
            return false;
        }
        std::error_code ec;
        std::filesystem::create_directories(dir_, ec);
        if (ec) {
            return false;
        }
        const std::filesystem::path final_path = dir_ / file_name_(id);
        const std::filesystem::path tmp_path = dir_ / (file_name_(id) + ".tmp");
        try {
            clink::state::detail::write_fsync_rename(
                final_path, tmp_path, snap.bytes.data(), snap.bytes.size());
        } catch (const std::exception&) {
            return false;
        }
        return true;
    }

    // Load the snapshot for a specific checkpoint id, or nullopt if absent.
    [[nodiscard]] std::optional<Snapshot> load(CheckpointId id) const {
        return read_(dir_ / file_name_(id), id);
    }

    // Load the highest-id snapshot present for this label, or nullopt if none.
    [[nodiscard]] std::optional<Snapshot> load_latest() const {
        std::error_code ec;
        bool found = false;
        std::uint64_t best = 0;
        const std::string prefix = "sharded-" + label_ + "-";
        for (const auto& entry : std::filesystem::directory_iterator(dir_, ec)) {
            if (ec) {
                break;
            }
            const std::string name = entry.path().filename().string();
            if (name.size() <= prefix.size() + 5 || name.compare(0, prefix.size(), prefix) != 0 ||
                name.compare(name.size() - 5, 5, ".snap") != 0) {
                continue;
            }
            // Skip a zero-byte file: a successful checkpoint is never empty, so a
            // 0-byte .snap is a failed/partial artefact that must not shadow a
            // good checkpoint (belt-and-braces; persist() also refuses empties).
            std::error_code size_ec;
            if (std::filesystem::file_size(entry.path(), size_ec) == 0 || size_ec) {
                continue;
            }
            const std::string id_str = name.substr(prefix.size(), name.size() - prefix.size() - 5);
            std::uint64_t id = 0;
            try {
                id = std::stoull(id_str);
            } catch (const std::exception&) {
                continue;
            }
            if (!found || id > best) {
                found = true;
                best = id;
            }
        }
        if (!found) {
            return std::nullopt;
        }
        return load(CheckpointId{best});
    }

private:
    std::string file_name_(CheckpointId id) const {
        return "sharded-" + label_ + "-" + std::to_string(id.value()) + ".snap";
    }

    static std::optional<Snapshot> read_(const std::filesystem::path& p, CheckpointId id) {
        std::ifstream in(p, std::ios::binary);
        if (!in) {
            return std::nullopt;
        }
        std::vector<std::byte> bytes;
        for (std::istreambuf_iterator<char> it{in}, end; it != end; ++it) {
            bytes.push_back(static_cast<std::byte>(*it));
        }
        return Snapshot{.checkpoint_id = id, .bytes = std::move(bytes)};
    }

    std::filesystem::path dir_;
    std::string label_;
};

}  // namespace clink
