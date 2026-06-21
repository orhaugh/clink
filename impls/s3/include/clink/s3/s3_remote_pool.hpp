#pragma once

// S3RemotePool - the production RemotePool behind RemoteReadBackend.
//
// Layout under <bucket>/<prefix>:
//   objects/<sha256-hex>      immutable, content-addressed value objects
//   manifests/cp-<id>         per-checkpoint manifest: (op, key) -> {hash, size}
//
// commit(id, base, changed, deleted) loads the base manifest, applies the
// delta (uploading only objects whose content is new - unchanged values share
// an object, so they are never re-uploaded), and writes the new manifest. A
// cold read loads the checkpoint's manifest once (cached) then GETs one value
// object by hash, so restore is lazy: a key is fetched only when first read.
//
// purge() drops a checkpoint's manifest but leaves its (shared) value objects;
// sweep(min_age) reclaims the orphans - it deletes every objects/<hash> that no
// live manifest references (the live manifest set IS the refcount), mirroring
// S3CasSnapshotStore::sweep.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <arrow/filesystem/s3fs.h>

#include "clink/connectors/parquet_s3_sink.hpp"  // clink::detail::ensure_arrow_s3_initialised
#include "clink/core/sha256.hpp"
#include "clink/core/types.hpp"
#include "clink/runtime/key_groups.hpp"  // kNumKeyGroups, KeyGroup
#include "clink/state/remote_pool.hpp"
#include "clink/state/state_backend.hpp"

namespace clink::s3 {

class S3RemotePool final : public clink::RemotePool {
public:
    struct Options {
        std::string bucket;                            // required
        std::string prefix;                            // per-subtask key prefix, e.g. "job/0"
        std::optional<std::string> region;             // explicit region
        std::optional<std::string> endpoint_override;  // MinIO / LocalStack
        bool allow_anonymous{false};
    };

    explicit S3RemotePool(Options opts) : opts_(std::move(opts)) {
        if (opts_.bucket.empty()) {
            throw std::invalid_argument("S3RemotePool: bucket is required");
        }
    }

    void commit(CheckpointId id,
                CheckpointId base,
                const std::vector<RemotePoolEntry>& changed,
                const std::vector<RemotePoolKey>& deleted) override {
        auto fs = fs_();
        Manifest m;
        if (base.value() != 0) {
            // Inherit the base checkpoint. Prefer a CACHED base: a rescale
            // restore materialises the merged + key-group-filtered base only in
            // the cache (never persisted to this prefix), and the cache also
            // holds the previous commit's manifest, so this avoids re-reading it
            // from S3. Fall back to S3 when the cache is cold (a process that
            // restarted mid-run). Absent == empty: a subtask that committed
            // nothing at `base` (no keyed state, or a source/sink subtask) has no
            // manifest there, which is not an error.
            bool cached = false;
            {
                std::lock_guard<std::mutex> lk(cache_mu_);
                if (auto it = cache_.find(base.value()); it != cache_.end()) {
                    m = it->second;
                    cached = true;
                }
            }
            if (!cached) {
                m = load_manifest_or_empty_(*fs, base);
            }
        }
        for (const auto& d : deleted) {
            m.erase({d.op.value(), d.key});
        }
        for (const auto& e : changed) {
            const std::string hash = sha256_hex_(e.value);
            const std::string obj_key = object_path_(hash);
            if (!object_present_(*fs, obj_key)) {
                write_object_(*fs, obj_key, to_string_(e.value));  // content-addressed: upload once
            }
            m[{e.op.value(), e.key}] = {hash, e.value.size()};
        }
        // Manifest LAST: only after every referenced object durably exists.
        write_object_(*fs, manifest_path_(id), encode_manifest_(m));
        std::lock_guard<std::mutex> lk(cache_mu_);
        cache_[id.value()] = std::move(m);
    }

    [[nodiscard]] std::optional<StateBackend::Value> read(CheckpointId id,
                                                          OperatorId op,
                                                          const std::string& key) const override {
        const Manifest& m = manifest_for_(id);
        auto it = m.find({op.value(), key});
        if (it == m.end()) {
            return std::nullopt;
        }
        auto fs = fs_();
        object_gets_.fetch_add(1, std::memory_order_relaxed);
        const std::string bytes = read_object_(*fs, object_path_(it->second.first));
        return StateBackend::Value{reinterpret_cast<const std::byte*>(bytes.data()),
                                   reinterpret_cast<const std::byte*>(bytes.data() + bytes.size())};
    }

    // Batched read (ASYNC-10): load the manifest ONCE, then coalesce keys that
    // map to the same content hash into a SINGLE object GET (distinct keys with
    // identical bytes share an object - the content-addressed win). Absent keys
    // yield nullopt. The number of object GETs is the number of DISTINCT hashes
    // in the batch, not the number of keys.
    std::vector<std::optional<StateBackend::Value>> read_many(
        CheckpointId id, OperatorId op, const std::vector<std::string>& keys) const override {
        const Manifest& m = manifest_for_(id);  // one manifest load (cached)
        std::vector<std::optional<StateBackend::Value>> out(keys.size());
        std::map<std::string, StateBackend::Value> by_hash;  // hash -> fetched bytes
        auto fs = fs_();
        for (std::size_t i = 0; i < keys.size(); ++i) {
            auto it = m.find({op.value(), keys[i]});
            if (it == m.end()) {
                continue;  // absent -> nullopt (default-constructed slot)
            }
            const std::string& hash = it->second.first;
            auto hit = by_hash.find(hash);
            if (hit == by_hash.end()) {
                object_gets_.fetch_add(1, std::memory_order_relaxed);  // one GET per distinct hash
                const std::string bytes = read_object_(*fs, object_path_(hash));
                StateBackend::Value v{
                    reinterpret_cast<const std::byte*>(bytes.data()),
                    reinterpret_cast<const std::byte*>(bytes.data() + bytes.size())};
                hit = by_hash.emplace(hash, std::move(v)).first;
            }
            out[i] = hit->second;  // scatter the (coalesced) object to this key's slot
        }
        return out;
    }

    // Count of value-object GETs (read + read_many). With coalescing, a
    // read_many over keys sharing a content hash increments this once, not
    // per key - the observable batching/dedup win. Diagnostic, for tests.
    [[nodiscard]] std::uint64_t object_gets() const noexcept {
        return object_gets_.load(std::memory_order_relaxed);
    }

    void purge(CheckpointId id) override {
        std::shared_ptr<arrow::fs::S3FileSystem> fs;
        try {
            fs = fs_();
        } catch (...) {
            return;  // cannot reach the store; leave everything (safe)
        }
        (void)fs->DeleteFile(manifest_path_(id));          // best-effort; objects via sweep()
        (void)fs->DeleteFile(rescale_manifest_path_(id));  // rescale sidecar, if any
        std::lock_guard<std::mutex> lk(cache_mu_);
        cache_.erase(id.value());
    }

    // Orphan reclamation for this subtask's prefix - a SPACE backstop, not
    // retention correctness (purge already bounds the live set). Deletes every
    // objects/<hash> referenced by NO live manifest (the live manifest list is
    // the refcount), reclaiming objects left by a purged checkpoint or an
    // aborted write (object uploaded, manifest never written). Returns the count
    // reclaimed. Mirrors S3CasSnapshotStore::sweep.
    //
    // SAFETY: a sweep races an in-flight checkpoint whose objects are uploaded
    // but whose manifest is not yet written - those look like orphans. `min_age`
    // guards that: an object younger than min_age is never deleted, so as long
    // as min_age exceeds the max checkpoint persist duration an in-flight
    // checkpoint's objects survive until its manifest lands. Conservative: if any
    // manifest is unreadable the sweep does nothing (it cannot prove an object
    // is unreferenced). Intended for an admin / periodic trigger, not the
    // checkpoint hot path; not thread-safe against a concurrent commit() to the
    // SAME object hash within min_age (which min_age also protects).
    std::uint64_t sweep(std::chrono::seconds min_age) {
        std::shared_ptr<arrow::fs::S3FileSystem> fs;
        try {
            fs = fs_();
        } catch (...) {
            return 0;
        }
        // 1. Referenced object hashes across every live manifest.
        std::unordered_set<std::string> live;
        for (const auto& mkey : list_keys_(*fs, base_prefix_() + "/manifests")) {
            Manifest m;
            try {
                m = decode_manifest_(read_object_(*fs, mkey));
            } catch (...) {
                return 0;  // unreadable manifest: cannot safely sweep
            }
            for (const auto& [k, v] : m) {
                live.insert(v.first);  // v.first == value-hash
            }
        }
        // 2. Delete unreferenced objects older than min_age.
        arrow::fs::FileSelector sel;
        sel.base_dir = base_prefix_() + "/objects";
        sel.recursive = false;
        sel.allow_not_found = true;
        auto infos = fs->GetFileInfo(sel);
        if (!infos.ok()) {
            return 0;
        }
        const auto now = std::chrono::system_clock::now();
        std::uint64_t reclaimed = 0;
        for (const auto& info : *infos) {
            if (info.type() != arrow::fs::FileType::File) {
                continue;
            }
            const std::string& path = info.path();
            const auto slash = path.rfind('/');
            const std::string hash = slash == std::string::npos ? path : path.substr(slash + 1);
            if (live.find(hash) != live.end()) {
                continue;  // still referenced by a live manifest
            }
            if (min_age.count() > 0) {
                const auto age =
                    std::chrono::duration_cast<std::chrono::seconds>(now - info.mtime());
                if (age < min_age) {
                    continue;  // too young: may be an in-flight checkpoint's object
                }
            }
            if (fs->DeleteFile(path).ok()) {
                ++reclaimed;
            }
        }
        objects_reclaimed_.fetch_add(reclaimed, std::memory_order_relaxed);
        return reclaimed;
    }

    // Objects reclaimed by sweep() so far (for tests / metrics).
    [[nodiscard]] std::uint64_t objects_reclaimed() const noexcept {
        return objects_reclaimed_.load(std::memory_order_relaxed);
    }

    // Configure rescale restore: the PARENT subtask prefixes (each a full
    // "<job-prefix>/<old-subtask>" sans bucket) whose checkpoint this new
    // subtask inherits. Set by build_remote_read on a rescale deploy; empty
    // (the default) means same-parallelism restore - prepare_restore is a no-op.
    void set_restore_sources(std::vector<std::string> parent_prefixes) {
        restore_parent_prefixes_ = std::move(parent_prefixes);
    }

    // Materialise checkpoint `restore_cp` under THIS subtask's prefix from the
    // configured parent prefixes (see RemotePool::prepare_restore). Merges every
    // parent's manifest cp-restore_cp: keyed entries (key[0] < kNumKeyGroups) are
    // kept only if their key-group is in `kg`; operator-state entries (key[0] ==
    // kOperatorStateKeyPrefix) are unioned across all parents (last parent wins
    // on collision - a documented v1 limitation for monotonic offsets). The
    // referenced objects are server-side-copied into this subtask's objects/ so
    // cold reads + the first incremental commit resolve against this prefix.
    //
    // The merged manifest is written to the SIDECAR (rescaled-cp-restore_cp), NOT
    // to cp-restore_cp: on scale-up several new subtasks share a parent prefix, so
    // overwriting cp-restore_cp there would corrupt a checkpoint a sibling still
    // reads. The sidecar is this subtask's own durable view (the loader prefers
    // it), so a failover before the first post-restore checkpoint still recovers
    // the filtered state, while the parents stay immutable. No-op when no parents
    // are configured (same-location same-parallelism restore - cp-restore_cp
    // already lives under this prefix).
    void prepare_restore(CheckpointId restore_cp, const KeyGroupRange& kg) override {
        if (restore_parent_prefixes_.empty()) {
            return;
        }
        auto fs = fs_();
        Manifest merged;
        std::map<ManifestKey, std::string> src_prefix;  // entry -> the parent it came from
        for (const auto& parent : restore_parent_prefixes_) {
            const Manifest pm = load_prefix_manifest_or_empty_(*fs, parent, restore_cp);
            for (const auto& [k, v] : pm) {
                // key[0] is the key group for a keyed entry; operator-state keys
                // (>= kNumKeyGroups, i.e. 0xFF) and keyless entries are exempt and
                // unioned across all parents (matches InMemoryStateBackend's
                // rescale filter). Guarding empty keys avoids OOB on key[0].
                const bool exempt =
                    k.second.empty() || static_cast<std::uint8_t>(k.second[0]) >= kNumKeyGroups;
                if (exempt) {
                    merged[k] = v;  // union (last parent wins)
                    src_prefix[k] = parent;
                } else if (kg.contains(
                               static_cast<KeyGroup>(static_cast<std::uint8_t>(k.second[0])))) {
                    merged[k] = v;
                    src_prefix[k] = parent;
                }
            }
        }
        // Relocate the referenced objects into THIS subtask's prefix (objects are
        // per-subtask). Content-addressed: skip if already present.
        for (const auto& [k, v] : merged) {
            const std::string dst = object_path_(v.first);
            if (object_present_(*fs, dst)) {
                continue;
            }
            const std::string src = parent_path_(src_prefix[k], "/objects/" + v.first);
            copy_object_(*fs, src, dst);
        }
        // Persist to the sidecar (durable; never clobbers a parent) + cache it so
        // the first post-restore commit inherits it without an S3 round-trip.
        write_object_(*fs, rescale_manifest_path_(restore_cp), encode_manifest_(merged));
        std::lock_guard<std::mutex> lk(cache_mu_);
        cache_[restore_cp.value()] = std::move(merged);
    }

    // Cross-location relocate: copy this subtask's checkpoint `id` (its manifest
    // + every referenced object) to `dst_prefix` (a full "<job-prefix>/<subtask>"
    // sans bucket under a DIFFERENT base). The result is self-contained, so a job
    // pointed at the new base restores it as an ordinary same-location restore.
    // Mirrors S3CasSnapshotStore::export_savepoint; idempotent (write-once hashes).
    //
    // Exports this subtask's EFFECTIVE view: load_manifest_or_empty_ prefers the
    // rescale sidecar, so a rescaled subtask exports its assigned key-group slice
    // (which is the only state it has - a scale-up subtask never wrote a plain
    // cp-<id>), written as a normal cp-<id> at the destination. That is correct
    // for per-subtask relocation; a whole-job relocate exports every subtask, and
    // together they cover the full key space.
    void export_checkpoint(CheckpointId id, const std::string& dst_prefix) {
        auto fs = fs_();
        const Manifest m = load_manifest_or_empty_(*fs, id);
        std::unordered_set<std::string> copied;
        for (const auto& [k, v] : m) {
            if (!copied.insert(v.first).second) {
                continue;  // dedup within the manifest
            }
            const std::string dst = parent_path_(dst_prefix, "/objects/" + v.first);
            if (!object_present_(*fs, dst)) {
                copy_object_(*fs, object_path_(v.first), dst);
            }
        }
        write_object_(*fs,
                      parent_path_(dst_prefix, "/manifests/cp-" + std::to_string(id.value())),
                      encode_manifest_(m));
    }

private:
    // (op, key) -> (value-hash, value-size).
    using ManifestKey = std::pair<std::uint64_t, std::string>;
    using ManifestVal = std::pair<std::string, std::uint64_t>;
    using Manifest = std::map<ManifestKey, ManifestVal>;

    std::string base_prefix_() const {
        return opts_.prefix.empty() ? opts_.bucket : (opts_.bucket + "/" + opts_.prefix);
    }
    std::string object_path_(const std::string& hash) const {
        return base_prefix_() + "/objects/" + hash;
    }
    std::string manifest_path_(CheckpointId id) const {
        return base_prefix_() + "/manifests/cp-" + std::to_string(id.value());
    }
    // Rescale SIDECAR manifest. A rescale target writes its merged +
    // key-group-filtered checkpoint here, NOT to cp-<id>, so it never overwrites
    // the shared parent checkpoint that sibling new subtasks still read on
    // scale-up. The manifest loader prefers it, so it IS this subtask's view of
    // <id> while remaining durable across a failover.
    std::string rescale_manifest_path_(CheckpointId id) const {
        return base_prefix_() + "/manifests/rescaled-cp-" + std::to_string(id.value());
    }
    // A path under an arbitrary prefix (sans bucket), e.g. a parent subtask's or
    // a relocate destination's. `suffix` includes its leading slash.
    std::string parent_path_(const std::string& prefix, const std::string& suffix) const {
        return opts_.bucket + "/" + prefix + suffix;
    }

    static std::string to_string_(const StateBackend::Value& v) {
        return std::string(reinterpret_cast<const char*>(v.data()), v.size());
    }

    static std::string sha256_hex_(const StateBackend::Value& v) {
        clink::Sha256 h;
        h.update(v.data(), v.size());
        return clink::Sha256::to_hex(h.finalize());
    }

    // The manifest for `id`, loaded once and cached (cold reads hit it per key).
    const Manifest& manifest_for_(CheckpointId id) const {
        std::lock_guard<std::mutex> lk(cache_mu_);
        auto it = cache_.find(id.value());
        if (it == cache_.end()) {
            auto fs = fs_();
            it = cache_.emplace(id.value(), load_manifest_or_empty_(*fs, id)).first;
        }
        return it->second;
    }

    Manifest load_manifest_(arrow::fs::S3FileSystem& fs, CheckpointId id) const {
        return decode_manifest_(read_object_(fs, manifest_path_(id)));
    }

    // Load this subtask's manifest for `id`, preferring the rescale sidecar and
    // treating an ABSENT manifest as an empty one. A checkpoint id with no
    // manifest means the subtask committed no state there (empty keyed state, or
    // a source/sink subtask) - "no entries", not an error, matching the in-memory
    // pool double. A present-but-unreadable object still throws (real fault).
    Manifest load_manifest_or_empty_(arrow::fs::S3FileSystem& fs, CheckpointId id) const {
        return load_prefix_manifest_or_empty_(fs, opts_.prefix, id);
    }
    // As above for an arbitrary prefix (a parent subtask's, on rescale). The
    // sidecar (rescaled-cp-<id>) wins over cp-<id>, so a parent that was itself a
    // rescale target contributes its post-rescale view.
    Manifest load_prefix_manifest_or_empty_(arrow::fs::S3FileSystem& fs,
                                            const std::string& prefix,
                                            CheckpointId id) const {
        const auto sidecar =
            parent_path_(prefix, "/manifests/rescaled-cp-" + std::to_string(id.value()));
        if (object_present_(fs, sidecar)) {
            return decode_manifest_(read_object_(fs, sidecar));
        }
        const auto path = parent_path_(prefix, "/manifests/cp-" + std::to_string(id.value()));
        if (!object_present_(fs, path)) {
            return Manifest{};
        }
        return decode_manifest_(read_object_(fs, path));
    }

    // --- binary manifest codec (length-prefixed; keys may be arbitrary bytes) ---

    static void put_u32_(std::string& o, std::uint32_t v) {
        for (int i = 0; i < 4; ++i)
            o.push_back(static_cast<char>((v >> (i * 8)) & 0xFF));
    }
    static void put_u64_(std::string& o, std::uint64_t v) {
        for (int i = 0; i < 8; ++i)
            o.push_back(static_cast<char>((v >> (i * 8)) & 0xFF));
    }
    static std::string encode_manifest_(const Manifest& m) {
        std::string o;
        put_u32_(o, static_cast<std::uint32_t>(m.size()));
        for (const auto& [k, v] : m) {
            put_u64_(o, k.first);  // op
            put_u32_(o, static_cast<std::uint32_t>(k.second.size()));
            o += k.second;  // key bytes
            put_u32_(o, static_cast<std::uint32_t>(v.first.size()));
            o += v.first;           // hash
            put_u64_(o, v.second);  // size
        }
        return o;
    }
    static Manifest decode_manifest_(const std::string& s) {
        Manifest m;
        std::size_t p = 0;
        auto need = [&](std::size_t n) {
            if (p + n > s.size())
                throw std::runtime_error("S3RemotePool: truncated manifest");
        };
        auto u32 = [&]() -> std::uint32_t {
            need(4);
            std::uint32_t v = 0;
            for (int i = 0; i < 4; ++i)
                v |= static_cast<std::uint32_t>(static_cast<unsigned char>(s[p + i])) << (i * 8);
            p += 4;
            return v;
        };
        auto u64 = [&]() -> std::uint64_t {
            need(8);
            std::uint64_t v = 0;
            for (int i = 0; i < 8; ++i)
                v |= static_cast<std::uint64_t>(static_cast<unsigned char>(s[p + i])) << (i * 8);
            p += 8;
            return v;
        };
        auto bytes = [&](std::uint32_t n) -> std::string {
            need(n);
            std::string out = s.substr(p, n);
            p += n;
            return out;
        };
        const std::uint32_t count = u32();
        for (std::uint32_t i = 0; i < count; ++i) {
            const std::uint64_t op = u64();
            const std::string key = bytes(u32());
            const std::string hash = bytes(u32());
            const std::uint64_t size = u64();
            m[{op, key}] = {hash, size};
        }
        return m;
    }

    // --- S3 object I/O (mirrors S3CasSnapshotStore) ---

    static bool object_present_(arrow::fs::S3FileSystem& fs, const std::string& key) {
        auto info = fs.GetFileInfo(key);
        return info.ok() && info->type() == arrow::fs::FileType::File;
    }
    // Server-side object copy (S3-to-S3, no client data transfer). Used to
    // relocate content-addressed objects into a subtask's prefix on rescale or
    // cross-location restore.
    static void copy_object_(arrow::fs::S3FileSystem& fs,
                             const std::string& src,
                             const std::string& dst) {
        if (auto st = fs.CopyFile(src, dst); !st.ok()) {
            throw std::runtime_error("S3RemotePool::copy_object CopyFile(" + src + " -> " + dst +
                                     "): " + st.ToString());
        }
    }
    static void write_object_(arrow::fs::S3FileSystem& fs,
                              const std::string& key,
                              const std::string& bytes) {
        auto out_result = fs.OpenOutputStream(key);
        if (!out_result.ok()) {
            throw std::runtime_error("S3RemotePool::write OpenOutputStream(" + key +
                                     "): " + out_result.status().ToString());
        }
        auto out = *out_result;
        if (!bytes.empty()) {
            if (auto st = out->Write(bytes.data(), static_cast<std::int64_t>(bytes.size()));
                !st.ok()) {
                throw std::runtime_error("S3RemotePool::write Write(" + key +
                                         "): " + st.ToString());
            }
        }
        if (auto st = out->Close(); !st.ok()) {
            throw std::runtime_error("S3RemotePool::write Close(" + key + "): " + st.ToString());
        }
    }
    // List the regular-file keys directly under `dir` (non-recursive). Used by
    // sweep to enumerate live manifests; missing dir -> empty (not an error).
    static std::vector<std::string> list_keys_(arrow::fs::S3FileSystem& fs,
                                               const std::string& dir) {
        arrow::fs::FileSelector sel;
        sel.base_dir = dir;
        sel.recursive = false;
        sel.allow_not_found = true;
        std::vector<std::string> out;
        auto infos = fs.GetFileInfo(sel);
        if (!infos.ok()) {
            return out;
        }
        for (const auto& info : *infos) {
            if (info.type() == arrow::fs::FileType::File) {
                out.push_back(info.path());
            }
        }
        return out;
    }

    static std::string read_object_(arrow::fs::S3FileSystem& fs, const std::string& key) {
        auto file_result = fs.OpenInputFile(key);
        if (!file_result.ok()) {
            throw std::runtime_error("S3RemotePool::read OpenInputFile(" + key +
                                     "): " + file_result.status().ToString());
        }
        auto file = *file_result;
        auto size_result = file->GetSize();
        if (!size_result.ok()) {
            throw std::runtime_error("S3RemotePool::read GetSize(" + key +
                                     "): " + size_result.status().ToString());
        }
        const std::int64_t size = *size_result;
        std::string out(static_cast<std::size_t>(size), '\0');
        if (size > 0) {
            auto n = file->ReadAt(0, size, out.data());
            if (!n.ok()) {
                throw std::runtime_error("S3RemotePool::read ReadAt(" + key +
                                         "): " + n.status().ToString());
            }
        }
        return out;
    }

    std::shared_ptr<arrow::fs::S3FileSystem> fs_() const {
        std::lock_guard<std::mutex> lk(fs_mu_);
        if (fs_cached_) {
            return fs_cached_;
        }
        clink::detail::ensure_arrow_s3_initialised();
        auto s3_opts = arrow::fs::S3Options::Defaults();
        if (opts_.region) {
            s3_opts.region = *opts_.region;
        }
        if (opts_.endpoint_override) {
            s3_opts.endpoint_override = *opts_.endpoint_override;
            s3_opts.scheme = "http";
        }
        if (opts_.allow_anonymous) {
            s3_opts.ConfigureAnonymousCredentials();
        }
        auto fs_result = arrow::fs::S3FileSystem::Make(s3_opts);
        if (!fs_result.ok()) {
            throw std::runtime_error("S3RemotePool: S3FileSystem::Make failed: " +
                                     fs_result.status().ToString());
        }
        fs_cached_ = *fs_result;
        return fs_cached_;
    }

    Options opts_;
    mutable std::mutex fs_mu_;
    mutable std::shared_ptr<arrow::fs::S3FileSystem> fs_cached_;
    mutable std::mutex cache_mu_;
    mutable std::map<std::uint64_t, Manifest> cache_;
    mutable std::atomic<std::uint64_t> object_gets_{0};  // value-object GETs (ASYNC-10 diag)
    std::atomic<std::uint64_t> objects_reclaimed_{0};
    // Parent subtask prefixes to merge on a rescale restore (set by
    // build_remote_read). Empty = same-parallelism restore (prepare_restore
    // is a no-op).
    std::vector<std::string> restore_parent_prefixes_;
};

}  // namespace clink::s3
