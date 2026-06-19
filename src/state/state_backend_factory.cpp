#include "clink/state/state_backend_factory.hpp"

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "clink/state/changelog_state_backend.hpp"
#include "clink/state/file_backed_state_backend.hpp"
#include "clink/state/file_materialization_store.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/sharded_in_memory_state_backend.hpp"

namespace clink {

namespace {

// Split "scheme://body" into (scheme, body). Returns ("", uri) when no
// "://" separator exists; callers treat that as "file://" by default.
std::pair<std::string, std::string> split_uri(const std::string& uri) {
    static constexpr std::string_view sep{"://"};
    const auto pos = uri.find(sep);
    if (pos == std::string::npos) {
        return {{}, uri};
    }
    return {uri.substr(0, pos), uri.substr(pos + sep.size())};
}

BuiltStateBackend build_memory(const StateBackendSpec& /*spec*/) {
    BuiltStateBackend out;
    out.backend = std::make_shared<InMemoryStateBackend>();
    return out;
}

// Key-group-sharded in-memory backend (opt-in via "memory+sharded://"):
// removes the single-mutex contention for keyed access under parallelism.
// Snapshots are byte-compatible with the plain memory backend, so it is a
// drop-in for any in-process job that wants concurrent keyed throughput.
BuiltStateBackend build_memory_sharded(const StateBackendSpec& /*spec*/) {
    BuiltStateBackend out;
    out.backend = std::make_shared<ShardedInMemoryStateBackend>();
    return out;
}

// Changelog backend over an in-memory inner with in-blob
// materialization storage. Materialization payloads ride inside each
// Snapshot blob. Suitable for small state or in-process testing.
//
// Does NOT restore across processes by design (the changelog analogue of
// the `memory` scheme): the spec carries no path, the materialization is
// RAM-only inside the framing blob, and the runtime discards that blob.
// Use changelog+file or changelog+rocksdb for durable, restorable state.
BuiltStateBackend build_changelog(const StateBackendSpec& /*spec*/) {
    BuiltStateBackend out;
    out.backend = std::make_shared<ChangelogStateBackend>();
    return out;
}

// changelog+file://<dir>: changelog backend over an in-memory inner,
// materialization payloads written to a FileMaterializationStore
// rooted at <dir>/<subtask_idx>/materializations. Each subtask gets
// its own directory so rescale-aware restore can address them
// independently if the operator chooses.
BuiltStateBackend build_changelog_file(const StateBackendSpec& spec) {
    auto [_, base_path] = split_uri(spec.uri);
    if (base_path.empty()) {
        throw std::runtime_error("state_backend_factory: 'changelog+file' scheme requires a path");
    }
    const bool want_restore = !spec.restore_uri.empty() && spec.restore_checkpoint_id != 0;
    const std::uint32_t parent_count =
        spec.restore_from_parent_count == 0 ? 1 : spec.restore_from_parent_count;

    const std::filesystem::path subtask_dir =
        std::filesystem::path{base_path} / std::to_string(spec.subtask_idx);
    const std::filesystem::path mat_dir = subtask_dir / "materializations";
    auto changelog = std::make_shared<ChangelogStateBackend>(
        std::make_shared<InMemoryStateBackend>(),
        std::make_shared<FileMaterializationStore>(mat_dir));
    // Self-persist framing blobs here so a fresh process can restore.
    changelog->set_snapshot_dir(subtask_dir);

    BuiltStateBackend out;
    out.backend = changelog;

    if (want_restore) {
        auto [restore_scheme, restore_base] = split_uri(spec.restore_uri);
        (void)restore_scheme;
        // Read each assigned parent's framing blob: one for same-parallelism /
        // scale-up, several contiguous parents for scale-down. frame_blobs
        // packs them; restore() splits + merges and narrows by the key-group
        // filter (forwarded by LocalExecutor). Materialization handles inside
        // are absolute paths into each source's dir (same-machine restart).
        const bool is_rescale =
            spec.restore_from_subtask_idx != std::numeric_limits<std::uint32_t>::max();
        const std::uint32_t src_first =
            is_rescale ? spec.restore_from_subtask_idx : spec.subtask_idx;
        std::vector<std::vector<std::byte>> blobs;
        for (std::uint32_t i = 0; i < parent_count; ++i) {
            const std::filesystem::path blob_path =
                std::filesystem::path{restore_base} / std::to_string(src_first + i) /
                ("changelog-" + std::to_string(spec.restore_checkpoint_id) + ".snap");
            std::ifstream in(blob_path, std::ios::binary);
            if (!in) {
                throw std::runtime_error(
                    "state_backend_factory: 'changelog+file' restore requested but snapshot not "
                    "found: " +
                    blob_path.string());
            }
            std::vector<std::byte> bytes;
            for (std::istreambuf_iterator<char> it{in}, end; it != end; ++it) {
                bytes.push_back(static_cast<std::byte>(*it));
            }
            blobs.push_back(std::move(bytes));
        }
        out.restore_from = Snapshot{CheckpointId{spec.restore_checkpoint_id},
                                    ChangelogStateBackend::frame_blobs(blobs)};
    }
    return out;
}

// File builder: working dir is <base>/<subtask_idx>. Restore copies
// <restore_base>/<subtask_idx>/checkpoint-<id>.snap into the working
// dir so the new backend can load it via its own snapshot_dir.
BuiltStateBackend build_file(const StateBackendSpec& spec) {
    auto [_, base_path] = split_uri(spec.uri);
    if (base_path.empty()) {
        throw std::runtime_error("state_backend_factory: 'file' scheme requires a path");
    }
    const std::filesystem::path subtask_dir =
        std::filesystem::path{base_path} / std::to_string(spec.subtask_idx);

    BuiltStateBackend out;
    out.backend = std::make_shared<FileBackedStateBackend>(subtask_dir);

    if (!spec.restore_uri.empty() && spec.restore_checkpoint_id != 0) {
        auto [restore_scheme, restore_path] = split_uri(spec.restore_uri);
        (void)restore_scheme;
        // Rescale: the JM assigns this new subtask one or more parent old
        // subtasks whose KEYED state to inherit (restore_from_subtask_idx set;
        // restore_from_parent_count contiguous parents for scale-down). The
        // sentinel UINT32_MAX is the non-rescale same-subtask-idx path.
        const bool is_rescale =
            spec.restore_from_subtask_idx != std::numeric_limits<std::uint32_t>::max();
        const std::uint32_t src_first =
            is_rescale ? spec.restore_from_subtask_idx : spec.subtask_idx;
        const std::uint32_t parent_count =
            spec.restore_from_parent_count == 0 ? 1 : spec.restore_from_parent_count;
        const std::string ckpt_name =
            "checkpoint-" + std::to_string(spec.restore_checkpoint_id) + ".snap";

        std::error_code ec;
        const std::filesystem::path dst_file = subtask_dir / ckpt_name;
        std::filesystem::create_directories(dst_file.parent_path(), ec);

        const auto read_file = [&](const std::filesystem::path& p) -> std::vector<std::byte> {
            std::vector<std::byte> bytes;
            std::ifstream in(p, std::ios::binary);
            if (!in) {
                return bytes;
            }
            std::istreambuf_iterator<char> it{in}, end;
            for (; it != end; ++it) {
                bytes.push_back(static_cast<std::byte>(*it));
            }
            return bytes;
        };

        // Assigned parents contribute their FULL snapshot: keyed rows (narrowed
        // to this subtask's key-group range on restore) plus their operator
        // rows. A single parent stays a plain copy (no re-streaming).
        std::vector<std::vector<std::byte>> parts;
        parts.reserve(parent_count);
        for (std::uint32_t i = 0; i < parent_count; ++i) {
            auto b = read_file(std::filesystem::path{restore_path} / std::to_string(src_first + i) /
                               ckpt_name);
            if (!b.empty()) {
                parts.push_back(std::move(b));
            }
        }

        // On rescale, OPERATOR state (source offsets, broadcast slots) is
        // broadcast, not partitioned: every new subtask must see all parents'
        // operator rows, then narrow at the source (Kafka's apply-once
        // rebalance cb). So union the operator-only rows from every OTHER
        // parent. The old parallelism is discovered by listing the numeric
        // subdirs of the restore base - no wire change. Same-parallelism
        // restore skips this (each subtask's own dir already has its state),
        // so a large keyed job pays nothing on a plain resubmit.
        if (is_rescale) {
            std::error_code dec;
            for (const auto& entry : std::filesystem::directory_iterator(restore_path, dec)) {
                if (dec) {
                    break;
                }
                if (!entry.is_directory(dec)) {
                    continue;
                }
                const std::string name = entry.path().filename().string();
                if (name.empty() || !std::all_of(name.begin(), name.end(), [](unsigned char c) {
                        return std::isdigit(c) != 0;
                    })) {
                    continue;
                }
                std::uint32_t pidx = 0;
                try {
                    pidx = static_cast<std::uint32_t>(std::stoul(name));
                } catch (const std::exception&) {
                    continue;  // a stray/overlong numeric dir name; not a parent
                }
                if (pidx >= src_first && pidx < src_first + parent_count) {
                    continue;  // an assigned parent: already a full part above
                }
                auto b = read_file(entry.path() / ckpt_name);
                if (!b.empty()) {
                    parts.push_back(InMemoryStateBackend::extract_operator_state_bytes(b));
                }
            }
        }

        if (!parts.empty()) {
            // One part (single assigned parent, no other-parent operator rows)
            // is written verbatim; otherwise stitch into one valid IPC stream.
            // Distinct per-row keys (keyed key-group prefix; per-partition
            // operator keys) mean the union never drops a row.
            std::vector<std::byte> final_bytes =
                parts.size() == 1 ? std::move(parts.front())
                                  : InMemoryStateBackend::merge_snapshot_bytes(parts);
            std::ofstream out_stream(dst_file, std::ios::binary | std::ios::trunc);
            if (out_stream && !final_bytes.empty()) {
                out_stream.write(reinterpret_cast<const char*>(final_bytes.data()),
                                 static_cast<std::streamsize>(final_bytes.size()));
            }
            out.restore_from = Snapshot{CheckpointId{spec.restore_checkpoint_id}, {}};
        }
    }
    return out;
}

}  // namespace

StateBackendFactory& StateBackendFactory::default_instance() {
    static StateBackendFactory inst;
    return inst;
}

StateBackendFactory::StateBackendFactory() {
    // Pre-register the two backends that ship in core. Plugins or user
    // code can call register_scheme() at startup to add more (s3,
    // azure, gcs, redis, ...) without touching this file.
    builders_["memory"] = &build_memory;
    builders_["memory+sharded"] = &build_memory_sharded;
    builders_["file"] = &build_file;
    builders_["changelog"] = &build_changelog;
    builders_["changelog+file"] = &build_changelog_file;
}

void StateBackendFactory::register_scheme(std::string scheme, Builder builder) {
    std::lock_guard lock(mu_);
    builders_[std::move(scheme)] = std::move(builder);
}

bool StateBackendFactory::has_scheme(const std::string& scheme) const {
    std::lock_guard lock(mu_);
    return builders_.find(scheme) != builders_.end();
}

BuiltStateBackend StateBackendFactory::build(const StateBackendSpec& spec) const {
    auto [scheme, _] = split_uri(spec.uri);
    if (spec.uri.empty()) {
        scheme = "memory";
    } else if (scheme.empty()) {
        // Bare path - preserve the historic CheckpointConfig contract
        // where checkpoint_dir is a local filesystem path.
        scheme = "file";
    }
    Builder builder;
    {
        std::lock_guard lock(mu_);
        auto it = builders_.find(scheme);
        if (it == builders_.end()) {
            throw std::runtime_error("state_backend_factory: no builder registered for scheme '" +
                                     scheme + "' (uri='" + spec.uri + "')");
        }
        builder = it->second;
    }
    return builder(spec);
}

}  // namespace clink
