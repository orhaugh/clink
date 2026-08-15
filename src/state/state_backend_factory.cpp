#include "clink/state/state_backend_factory.hpp"

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <limits>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "clink/runtime/log_buffer.hpp"
#include "clink/state/changelog_state_backend.hpp"
#include "clink/state/checkpoint_integrity.hpp"
#include "clink/state/durable_file_write.hpp"
#include "clink/state/file_backed_state_backend.hpp"
#include "clink/state/file_materialization_store.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/remote_pool.hpp"
#include "clink/state/remote_read_backend.hpp"
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

// The subtask indices a checkpoint's COMPLETED marker records as participants.
//
// Returns nullopt when no single marker can be identified - no _jobs directory, no
// marker for this id, several jobs sharing the checkpoint root, or a marker with no
// subtasks= line. The caller then falls back to directory listing and says so, rather
// than treating "unknown" as "nobody participated" and restoring nothing.
[[nodiscard]] std::optional<std::set<std::uint32_t>> completed_participants(
    const std::string& restore_path, std::uint64_t checkpoint_id) {
    const std::filesystem::path jobs_dir = std::filesystem::path{restore_path} / "_jobs";
    std::error_code ec;
    std::vector<std::filesystem::path> markers;
    for (const auto& entry : std::filesystem::directory_iterator(jobs_dir, ec)) {
        if (ec) {
            return std::nullopt;
        }
        if (!entry.is_directory(ec)) {
            continue;
        }
        auto candidate = entry.path() / ("COMPLETED-" + std::to_string(checkpoint_id));
        if (std::filesystem::exists(candidate, ec)) {
            markers.push_back(std::move(candidate));
        }
    }
    // Exactly one, or the answer is ambiguous and guessing would be worse than
    // falling back.
    if (markers.size() != 1) {
        return std::nullopt;
    }
    std::ifstream in(markers.front());
    if (!in) {
        return std::nullopt;
    }
    const std::string body((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    constexpr std::string_view kKey = "subtasks=";
    const auto at = body.find(kKey);
    if (at == std::string::npos) {
        return std::nullopt;  // a marker predating the participant set
    }
    auto line = body.substr(at + kKey.size());
    if (const auto nl = line.find('\n'); nl != std::string::npos) {
        line = line.substr(0, nl);
    }
    std::set<std::uint32_t> out;
    std::size_t start = 0;
    while (start <= line.size()) {
        const auto comma = line.find(',', start);
        const auto piece =
            line.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
        if (!piece.empty()) {
            try {
                out.insert(static_cast<std::uint32_t>(std::stoul(piece)));
            } catch (const std::exception&) {
                return std::nullopt;  // malformed; do not half-trust it
            }
        }
        if (comma == std::string::npos) {
            break;
        }
        start = comma + 1;
    }
    return out;
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

// disagg-local://[?io_threads=N&hot_max_bytes=M]
//
// A RemoteReadBackend (the same async-deferring class as the S3-backed
// remote-read:// scheme) over a process-local InMemoryRemotePool.
// supports_async_get() is true, so the async/disaggregated execution path
// activates automatically with NO S3 and NO manual opt-in - the point is to
// make that path reachable for correctness coverage and dev/test without an
// object store.
//
// CAVEATS (honest, load-bearing):
//   * The pool lives in process RAM, so state is NOT durable across a process
//     restart: there is no cross-process failover or rescale. snapshot/restore
//     work only WITHIN a process (same-parallelism suspend/resume).
//   * Reads hit RAM, so there is no remote IO latency to overlap: this is NOT a
//     throughput win over memory://. The throughput win needs a real remote
//     tier (remote-read:// on S3).
BuiltStateBackend build_disagg_local(const StateBackendSpec& spec) {
    auto [scheme, base] = split_uri(spec.uri);
    (void)scheme;
    std::size_t io_threads = 1;  // connection-pool size (knob; no real IO here)
    // Default the hot-tier budget to a fraction of physical RAM so working-state-
    // exceeds-RAM (LRU eviction to the pool) is ON by default. An explicit
    // ?hot_max_bytes=N overrides it; ?hot_max_bytes=0 forces the unbounded tier.
    std::size_t hot_max_bytes = default_remote_hot_max_bytes();
    if (const auto q = base.find('?'); q != std::string::npos) {
        const std::string query = base.substr(q + 1);
        for (std::size_t start = 0; start < query.size();) {
            const auto amp = query.find('&', start);
            const std::string kv =
                query.substr(start, amp == std::string::npos ? std::string::npos : amp - start);
            if (const auto eq = kv.find('='); eq != std::string::npos) {
                const std::string k = kv.substr(0, eq);
                const std::string v = kv.substr(eq + 1);
                try {
                    if (k == "io_threads") {
                        io_threads = static_cast<std::size_t>(std::stoull(v));
                    } else if (k == "hot_max_bytes") {
                        hot_max_bytes = static_cast<std::size_t>(std::stoull(v));
                    }
                } catch (...) {
                    // malformed value -> keep the safe default
                }
            }
            if (amp == std::string::npos) {
                break;
            }
            start = amp + 1;
        }
    }
    if (io_threads == 0) {
        io_threads = 1;
    }
    BuiltStateBackend out;
    out.backend = std::make_shared<RemoteReadBackend>(
        std::make_shared<InMemoryRemotePool>(), io_threads, hot_max_bytes);
    // No restore_from: the in-memory pool holds nothing across a restart, so
    // there is no cross-process checkpoint to stage.
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
        state_dir_for(base_path, spec.generation, spec.subtask_idx);
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
                std::filesystem::path{
                    state_dir_for(restore_base, spec.restore_generation, src_first + i)} /
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
        state_dir_for(base_path, spec.generation, spec.subtask_idx);

    BuiltStateBackend out;
    out.backend = std::make_shared<FileBackedStateBackend>(subtask_dir);

    if (!spec.restore_uri.empty() && spec.restore_checkpoint_id != 0) {
        auto [restore_scheme, restore_path] = split_uri(spec.restore_uri);
        (void)restore_scheme;
        // Rescale: the coordinator assigns this new subtask one or more parent old
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

        // The working dir still has to exist - this subtask's own later
        // checkpoints go here - but nothing from the restore is written into it.
        std::error_code ec;
        std::filesystem::create_directories(subtask_dir, ec);

        // Read one parent snapshot, refusing anything that fails its
        // integrity check. A parent whose bytes are damaged would otherwise
        // be merged straight into the child's state and the rescaled job
        // would come up quietly missing (or holding garbage for) whatever
        // key groups that parent owned. Absent files stay non-fatal - not
        // every parent index necessarily has a snapshot.
        const auto read_file = [&](const std::filesystem::path& p) -> std::vector<std::byte> {
            std::vector<std::byte> bytes;
            const auto verdict = clink::state::verify_checkpoint(p);
            if (verdict.status == clink::state::CheckpointStatus::Missing) {
                return bytes;
            }
            if (!verdict.ok() && !clink::state::unverified_checkpoints_allowed(verdict)) {
                // Say whether an older USABLE checkpoint exists, and name it.
                //
                // state::latest_valid_checkpoint_in encodes the fallback rule, and
                // until now nothing on the recovery path consulted it - it had eight
                // tests and zero production callers, so the engine looked like it
                // could survive a corrupt newest checkpoint and could not. A restore
                // that hit one threw this error and the job failed, with no indication
                // that a good older checkpoint was sitting right beside it.
                //
                // It reports rather than rewinds, deliberately. Rewinding
                // automatically is not obviously safe: the coordinator marked the
                // failing checkpoint COMPLETED, so a sink may already have committed
                // its transactions, and silently replaying from an earlier point would
                // duplicate that output. Naming the option leaves that judgement with
                // the operator, who can restore from it explicitly.
                std::string hint;
                if (const auto older = clink::state::latest_valid_checkpoint_in(
                        p.parent_path(),
                        spec.restore_checkpoint_id == 0 ? 0 : spec.restore_checkpoint_id - 1);
                    older.has_value()) {
                    hint = " An older checkpoint in the same directory does verify: " +
                           std::to_string(*older) +
                           ". Restoring from it explicitly is a recovery option, but it "
                           "replays everything after it - check whether any sink has "
                           "already committed output for a later checkpoint first.";
                } else {
                    hint =
                        " No older checkpoint in the same directory verifies either, so "
                        "there is no earlier recovery point to fall back to.";
                }
                throw clink::state::CheckpointIntegrityError(verdict.status, verdict.detail + hint);
            }
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
            auto b = read_file(std::filesystem::path{state_dir_for(
                                   restore_path, spec.restore_generation, src_first + i)} /
                               ckpt_name);
            if (!b.empty()) {
                parts.push_back(std::move(b));
            }
        }

        // A NON-rescale restore whose own snapshot is absent must not proceed.
        //
        // The read above treats an absent file as non-fatal, which is right for a
        // rescale: a new subtask is assigned a range of parent indices and not
        // every one of them necessarily has a snapshot. On the same-subtask path
        // it is not right. The coordinator only ever names a checkpoint it marked
        // COMPLETED, and COMPLETED means every participant acknowledged it - so
        // the file is supposed to be there. snapshot() is unconditional
        // (persist(capture(id))), so even a subtask holding no state writes one;
        // absence is not "this operator had nothing to save".
        //
        // What the old behaviour did with it: `parts` stayed empty, the restore
        // below was skipped, and the subtask came up with EMPTY state while its
        // peers restored fully. A keyed counter silently resets to zero and a
        // source replays from offset zero, and the job reports nothing at all.
        // That is silent state loss on the ordinary recovery path - worker loss,
        // coordinator failover, plain resume - and it is exactly the class of
        // defect where a quiet fallback hides a storage problem for weeks.
        //
        // The escape hatch is for a genuinely new stateful operator added to an
        // existing job, which legitimately has no prior state to restore.
        if (!is_rescale && parts.empty() && !clink::state::allow_missing_restore_state()) {
            const auto missing = std::filesystem::path{state_dir_for(
                                     restore_path, spec.restore_generation, src_first)} /
                                 ckpt_name;
            throw std::runtime_error(
                "refusing to restore subtask " + std::to_string(spec.subtask_idx) +
                " with empty state: checkpoint " + std::to_string(spec.restore_checkpoint_id) +
                " named it as a participant but " + missing.string() +
                " is absent. Continuing would bring this subtask up holding nothing while its "
                "peers restore fully, which loses its state silently. Check whether the "
                "checkpoint directory is complete (clink checkpoint-verify). If this operator "
                "is genuinely new and has no prior state, set "
                "CLINK_ALLOW_MISSING_RESTORE_STATE=1.");
        }

        // OPERATOR state (source offsets, broadcast slots) is broadcast, not
        // partitioned: every restoring subtask must see all parents' operator
        // rows, then narrow at the source (Kafka's apply-once rebalance cb).
        // So union the operator-only rows from every OTHER parent. The old
        // parallelism is discovered by listing the numeric subdirs of the
        // restore GENERATION - no wire change.
        //
        // This used to be gated on is_rescale, on the reasoning that at the
        // same parallelism each subtask's own dir already has its state. That
        // holds for KEYED state, whose key groups are pinned to a subtask
        // index, and fails for operator state whose ownership something
        // outside clink decides. A Kafka source subscribes to a consumer
        // group, so which subtask owns which partition is chosen by the group
        // coordinator and is not stable across a restart: a subtask that came
        // back holding a partition it had not owned before found no restored
        // offset for it and silently resumed from the broker's committed group
        // offset instead of the checkpoint, replaying or skipping records.
        // QUAL-01 caught it as 938 wrong keys in the single window in flight
        // across a worker kill - counted twice for the rewound partitions,
        // lost for the ones that jumped forward.
        //
        // The cost is that a plain restart now reads its peers' snapshot files
        // to extract their operator rows, where before it read only its own.
        // Only the operator rows are kept (extract_operator_state_bytes), but
        // the read is of the whole file, so a large keyed job pays I/O on
        // restore that it did not pay before. That is the right trade: a
        // restore is rare, and the alternative is a silent correctness hole in
        // the one operation whose entire purpose is to be correct.
        //
        // Scoped to the generation that produced the checkpoint, not to the base:
        // scanning the base would now walk every generation the job has ever had
        // and union operator rows from topologies this restore has nothing to do
        // with.
        {
            // This subtask's own operator keys, so a peer can fill in one it
            // does not have and never overwrite one it does.
            //
            // Not all operator state is partition-scoped. A Kafka source keys
            // its offsets per partition, and those must cross subtasks because
            // the broker decides who owns a partition. The file, directory,
            // polling and vector sources each keep their position under one
            // FIXED key, so at parallelism 4 all four subtasks write the same
            // key with four different values - and the merge keeps the greater
            // i64 on collision, which is the safe direction for a partition
            // offset and silent data loss for these, handing every subtask the
            // furthest position any of them reached.
            std::set<std::pair<std::uint64_t, std::string>> own_keys;
            for (const auto& own : parts) {
                auto k = InMemoryStateBackend::operator_state_keys(own);
                own_keys.insert(k.begin(), k.end());
            }
            const auto participants =
                completed_participants(restore_path, spec.restore_checkpoint_id);
            std::error_code dec;
            const std::filesystem::path restore_gen_dir =
                std::filesystem::path{restore_path} /
                ("v" + std::to_string(spec.restore_generation));
            for (const auto& entry : std::filesystem::directory_iterator(restore_gen_dir, dec)) {
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
                // Skip a directory that this checkpoint's participant set does not
                // name (follow-up 49).
                //
                // Parents were discovered purely by LISTING numeric subdirs of the
                // restore generation, so a directory left behind by a topology that
                // never participated in this checkpoint was read like any other and
                // its operator rows - source offsets, broadcast slots - unioned into
                // every restoring subtask. That is the question follow-up 49 left
                // open: whether such a leftover can be READ as though it belonged.
                // It could.
                //
                // The COMPLETED marker already records exactly who participated, so
                // the answer is to consult it rather than to trust the directory
                // listing. When it cannot be identified the listing still stands, and
                // the log says so - "unknown" must not silently become "nobody".
                if (participants.has_value() && participants->count(pidx) == 0) {
                    clink::log::warn(
                        "state.restore",
                        "ignoring " + entry.path().string() + ": checkpoint " +
                            std::to_string(spec.restore_checkpoint_id) +
                            " did not record subtask " + std::to_string(pidx) +
                            " as a participant, so its state belongs to another topology");
                    continue;
                }
                auto b = read_file(entry.path() / ckpt_name);
                if (!b.empty()) {
                    parts.push_back(
                        InMemoryStateBackend::extract_operator_state_bytes(b, &own_keys));
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
            // Handed over IN MEMORY, deliberately not staged as a file here.
            //
            // It used to be written to <base>/<this subtask idx>/checkpoint-<id>.snap
            // so the backend could load it through its own snapshot_dir. That write
            // is an aliasing bug, because the restore base and the working base are
            // the SAME directory in production (the coordinator sets
            // restore_from_dir = checkpoint_dir) and global subtask indices SHIFT
            // when an operator is resized - the planner allocates one contiguous
            // block per operator in graph order. Scaling `counter` from 1 to 4 in a
            // source -> counter -> sink job moves the indices from 0,1,2 to
            // 0,{1,2,3,4},5, so:
            //
            //   - the new counter child at index 2 overwrote the OLD SINK's snapshot,
            //     which the new sink at index 5 then inherited - it came up holding a
            //     counter's keyed state, and for a 2PC sink that file is the commit
            //     handle, so a staged transaction became uncommittable; and
            //   - the child at index 1 rewrote the very file its siblings were still
            //     reading as their parent, so a sibling that read between the payload
            //     rename and the sidecar write saw them disagree and failed its
            //     integrity check ("is 1960 bytes, sidecar declares 1208"), which
            //     restarted and then failed the whole job.
            //
            // The index MAPPING was already correct (F38): each task maps its index
            // within its operator through that operator's old block base. It was the
            // write target that aliased. Nothing needs this staged copy - its only
            // purpose was to feed the load - and a parent snapshot has to stay
            // immutable while other subtasks are still reading it.
            out.restore_from =
                Snapshot{CheckpointId{spec.restore_checkpoint_id}, std::move(final_bytes)};
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
    builders_["disagg-local"] = &build_disagg_local;
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
