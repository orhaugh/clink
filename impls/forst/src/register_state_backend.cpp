#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "clink/forst/install.hpp"
#include "clink/state/changelog_state_backend.hpp"
#include "clink/state/file_materialization_store.hpp"
#include "clink/state/forst_state_backend.hpp"
#include "clink/state/state_backend_factory.hpp"

namespace clink::forst {

namespace {

std::pair<std::string, std::string> split_uri(const std::string& uri) {
    static constexpr std::string_view sep{"://"};
    const auto pos = uri.find(sep);
    if (pos == std::string::npos) {
        return {{}, uri};
    }
    return {uri.substr(0, pos), uri.substr(pos + sep.size())};
}

// Builds a ForStStateBackend rooted at <base>/<subtask_idx>. When the
// spec carries a restore location, wire out.restore_from to this
// subtask's checkpoint dir so LocalExecutor calls backend->restore()
// before any operator runs. snapshot() writes each checkpoint to
// "<db_path>.cp-<id>" (a sibling of the db dir); restore() re-homes the
// DB onto it.
//
// Restore covers same-parallelism restart and full rescale (scale-up AND
// scale-down): restore() re-homes onto the first assigned parent's
// checkpoint, iterate-merges any further parents, and narrows keyed rows
// by the key-group filter while preserving operator-state rows (source
// offsets).
BuiltStateBackend build_forst(const StateBackendSpec& spec) {
    auto [_, base_path] = split_uri(spec.uri);
    if (base_path.empty()) {
        throw std::runtime_error("forst scheme requires a path");
    }

    const bool want_restore = !spec.restore_uri.empty() && spec.restore_checkpoint_id != 0;
    const std::uint32_t parent_count =
        spec.restore_from_parent_count == 0 ? 1 : spec.restore_from_parent_count;

    const std::filesystem::path subtask_dir =
        std::filesystem::path{base_path} / std::to_string(spec.subtask_idx);
    // The engine's Open only mkdirs the leaf dir, so the parents must
    // exist first (mirrors build_changelog_forst). Without this, opening
    // <base>/<subtask_idx> fails whenever <base> isn't already present.
    std::error_code mkdir_ec;
    std::filesystem::create_directories(subtask_dir, mkdir_ec);

    clink::ForStStateBackend::Options opts;
    opts.path = subtask_dir.string();
    opts.create_if_missing = true;

    BuiltStateBackend out;
    out.backend = std::make_shared<clink::ForStStateBackend>(std::move(opts));

    if (want_restore) {
        auto [restore_scheme, restore_base] = split_uri(spec.restore_uri);
        (void)restore_scheme;
        // One source per assigned parent: the sentinel means "read own
        // subtask_idx" (same parallelism); a set restore_from_subtask_idx +
        // parent_count names the contiguous parents this new subtask
        // inherits (scale-up = 1 parent, scale-down = several). snapshot()
        // wrote each checkpoint to "<db_path>.cp-<id>". restore() re-homes
        // onto the first and iterate-merges the rest; the key-group filter
        // (from LocalExecutor) narrows the keyed rows to this subtask's
        // slice. The paths ride snap.bytes newline-separated.
        const bool is_rescale =
            spec.restore_from_subtask_idx != std::numeric_limits<std::uint32_t>::max();
        const std::uint32_t src_first =
            is_rescale ? spec.restore_from_subtask_idx : spec.subtask_idx;
        std::string joined;
        for (std::uint32_t i = 0; i < parent_count; ++i) {
            const std::string dir =
                (std::filesystem::path{restore_base} / std::to_string(src_first + i)).string() +
                ".cp-" + std::to_string(spec.restore_checkpoint_id);
            if (!std::filesystem::exists(dir)) {
                throw std::runtime_error(
                    "forst scheme: restore requested but checkpoint dir not found: " + dir);
            }
            if (!joined.empty()) {
                joined.push_back('\n');
            }
            joined += dir;
        }
        Snapshot snap;
        snap.checkpoint_id = CheckpointId{spec.restore_checkpoint_id};
        snap.bytes.assign(reinterpret_cast<const std::byte*>(joined.data()),
                          reinterpret_cast<const std::byte*>(joined.data() + joined.size()));
        out.restore_from = std::move(snap);
    }
    return out;
}

// changelog+forst://<dir>: large-state config with a write-ahead log.
//   * Inner backend: ForStStateBackend at <dir>/<subtask_idx>/inner.
//     Reads/writes/scans go to the engine so state can exceed RAM.
//   * Materialization store: FileMaterializationStore at
//     <dir>/<subtask_idx>/mat. Each materialization writes a single
//     <dir>/<subtask_idx>/mat/mat-<ckpt_id>.bin file; the Snapshot blob
//     carries only the path handle.
BuiltStateBackend build_changelog_forst(const StateBackendSpec& spec) {
    auto [_, base_path] = split_uri(spec.uri);
    if (base_path.empty()) {
        throw std::runtime_error("changelog+forst scheme requires a path");
    }
    const bool want_restore = !spec.restore_uri.empty() && spec.restore_checkpoint_id != 0;
    const std::uint32_t parent_count =
        spec.restore_from_parent_count == 0 ? 1 : spec.restore_from_parent_count;

    const std::filesystem::path subtask_dir =
        std::filesystem::path{base_path} / std::to_string(spec.subtask_idx);
    const std::filesystem::path inner_dir = subtask_dir / "inner";
    const std::filesystem::path mat_dir = subtask_dir / "mat";
    // The engine's Open only mkdirs the leaf - the subtask dir must exist
    // first. FileMaterializationStore creates its own dir in its ctor.
    std::error_code ec;
    std::filesystem::create_directories(inner_dir, ec);

    clink::ForStStateBackend::Options opts;
    opts.path = inner_dir.string();
    opts.create_if_missing = true;
    auto inner = std::make_shared<clink::ForStStateBackend>(std::move(opts));
    auto store = std::make_shared<clink::FileMaterializationStore>(mat_dir);
    auto changelog =
        std::make_shared<clink::ChangelogStateBackend>(std::move(inner), std::move(store));
    // Self-persist framing blobs so a fresh process can restore.
    changelog->set_snapshot_dir(subtask_dir);

    BuiltStateBackend out;
    out.backend = changelog;

    if (want_restore) {
        auto [restore_scheme, restore_base] = split_uri(spec.restore_uri);
        (void)restore_scheme;
        // Read each assigned parent's framing blob: one for
        // same-parallelism / scale-up, several contiguous parents for
        // scale-down. frame_blobs packs them; restore() splits + merges
        // (the inner engine folds the parents' checkpoint dirs via
        // combine_snapshots) and narrows by the key-group filter. Each
        // parent's blob, mat file, and inner .cp-<id> dir live under that
        // parent's source subtask dir.
        const bool is_rescale =
            spec.restore_from_subtask_idx != std::numeric_limits<std::uint32_t>::max();
        const std::uint32_t src_first =
            is_rescale ? spec.restore_from_subtask_idx : spec.subtask_idx;
        std::vector<std::vector<std::byte>> blobs;
        for (std::uint32_t i = 0; i < parent_count; ++i) {
            const std::filesystem::path blob =
                std::filesystem::path{restore_base} / std::to_string(src_first + i) /
                ("changelog-" + std::to_string(spec.restore_checkpoint_id) + ".snap");
            std::ifstream in(blob, std::ios::binary);
            if (!in) {
                throw std::runtime_error(
                    "changelog+forst scheme: restore requested but snapshot not found: " +
                    blob.string());
            }
            std::vector<std::byte> bytes;
            for (std::istreambuf_iterator<char> it{in}, end; it != end; ++it) {
                bytes.push_back(static_cast<std::byte>(*it));
            }
            blobs.push_back(std::move(bytes));
        }
        out.restore_from = Snapshot{CheckpointId{spec.restore_checkpoint_id},
                                    clink::ChangelogStateBackend::frame_blobs(blobs)};
    }
    return out;
}

}  // namespace

void install() {
    clink::StateBackendFactory::default_instance().register_scheme("forst", &build_forst);
    clink::StateBackendFactory::default_instance().register_scheme("changelog+forst",
                                                                   &build_changelog_forst);
}

}  // namespace clink::forst
