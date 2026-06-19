#pragma once

// S3CasSnapshotStore - a content-addressed manifest SnapshotStore (DISAGG-6).
//
// Where S3SnapshotStore (DISAGG-2) uploads every file of every checkpoint under
// cp-<id>/, this store stores each immutable object ONCE, content-addressed by
// the SHA-256 of its bytes, and writes a tiny per-checkpoint manifest that
// REFERENCES those objects. Because RocksDB shares most SSTs across consecutive
// incremental checkpoints, a steady-state checkpoint uploads only the changed
// objects (a HEAD confirms the shared ones already exist) - the checkpoint's
// upload cost is O(changed bytes), not O(total state). An older manifest gives
// free time-travel restore (bounded by checkpoint retention).
//
// Per-subtask key layout under <bucket>/<prefix> (prefix already = job/<subtask>
// via the factory's subtask_prefix):
//   objects/<hash>                     content-addressed blobs (write-once)
//   manifests/cp-<id>.manifest         one per checkpoint; the opaque handle
//
// Write-once content addressing makes concurrency safe: the key IS the hash, so
// two writers of identical content PUT byte-identical bytes to the same key
// (idempotent, never torn). The manifest is written LAST, after every object it
// references durably exists, so a present manifest is always fully resolvable;
// a crash before the manifest write leaves only orphan objects (reclaimable),
// never a dangling manifest (corruption).
//
// GC (refcount over the live manifest set) is delete_checkpoint, added in the
// next increment; this increment ships write + fetch + dedup. The transport
// helpers (S3FileSystem lazy-make, staging dir, object download) mirror
// S3SnapshotStore deliberately - the two are independent checkpoint stores and
// each is kept self-contained.

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <vector>

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/s3fs.h>
#include <arrow/io/api.h>

#include "clink/connectors/parquet_s3_sink.hpp"  // clink::detail::ensure_arrow_s3_initialised
#include "clink/core/sha256.hpp"
#include "clink/core/types.hpp"
#include "clink/metrics/disagg_metrics.hpp"
#include "clink/s3/cas_manifest.hpp"
#include "clink/s3/local_object_cache.hpp"
#include "clink/state/snapshot_store.hpp"

namespace clink::s3 {

class S3CasSnapshotStore final : public clink::SnapshotStore {
public:
    struct Options {
        std::string bucket;                            // required
        std::string prefix;                            // per-subtask key prefix (e.g. "job/0")
        std::uint32_t subtask{0};                      // provenance recorded in the manifest
        std::optional<std::string> region;             // explicit region
        std::optional<std::string> endpoint_override;  // MinIO / localstack
        bool allow_anonymous{false};                   // public-bucket access
        std::filesystem::path local_staging_dir{std::filesystem::temp_directory_path() /
                                                "clink-s3-cas-staging"};
        // Content-addressed local cache of objects (DISAGG-5). When cache_bytes
        // > 0, fetch serves cache hits with zero GetObject and write/fetch
        // populate it - keyed by the object hash, so it is the exact local twin
        // of the remote object pool.
        std::filesystem::path cache_dir;
        std::uint64_t cache_bytes{0};
    };

    explicit S3CasSnapshotStore(Options opts) : opts_(std::move(opts)) {
        if (opts_.bucket.empty()) {
            throw std::invalid_argument("S3CasSnapshotStore: bucket is required");
        }
        if (opts_.cache_bytes > 0) {
            const std::filesystem::path cdir =
                opts_.cache_dir.empty()
                    ? (std::filesystem::temp_directory_path() / "clink-s3-cas-object-cache")
                    : opts_.cache_dir;
            cache_ = std::make_unique<LocalObjectCache>(cdir, opts_.cache_bytes);
        }
    }

    // Objects newly uploaded by write (i.e. cache/skip-if-exists misses).
    [[nodiscard]] std::uint64_t objects_uploaded() const noexcept {
        return uploads_.load(std::memory_order_relaxed);
    }
    // Objects fetched from S3 (i.e. local-cache misses) by fetch.
    [[nodiscard]] std::uint64_t objects_downloaded() const noexcept {
        return downloads_.load(std::memory_order_relaxed);
    }

    std::string write_checkpoint_dir(const std::string& local_cp_path, CheckpointId id) override {
        auto fs = fs_();
        const std::string objects_ns = objects_prefix_();
        CasManifest manifest;
        manifest.checkpoint_id = id.value();
        manifest.subtask = opts_.subtask;

        std::error_code ec;
        std::uint64_t manifest_bytes = 0;
        for (const auto& entry : std::filesystem::directory_iterator(local_cp_path, ec)) {
            if (!entry.is_regular_file()) {
                continue;  // RocksDB checkpoint dirs are flat
            }
            const std::string name = entry.path().filename().string();
            const std::string hash = hash_file_(entry.path());
            const std::uint64_t size = std::filesystem::file_size(entry.path(), ec);
            const std::string obj_key = objects_ns + "/" + hash;

            // Skip-if-exists: a shared object (the common case) is already in
            // the pool, so a HEAD replaces an upload. Content addressing makes
            // a present same-key object byte-identical, so the skip is safe.
            if (!object_present_(*fs, obj_key, size)) {
                upload_file_(*fs, entry.path(), obj_key);
                uploads_.fetch_add(1, std::memory_order_relaxed);
                clink::metrics::disagg::cas_object_uploaded();
            }
            // Populate the local cache from the just-written file so a same-host
            // restore reuses it without a download.
            if (cache_) {
                cache_->put(objects_ns, hash, entry.path());
            }
            manifest.entries.push_back({name, hash, size});
            manifest_bytes += size;
        }

        // Manifest LAST: only after every referenced object durably exists.
        const std::string mkey = manifest_key_(id);
        write_object_(*fs, mkey, manifest.encode());
        // OBS-3: per-checkpoint object footprint (count + total bytes referenced).
        clink::metrics::disagg::checkpoint_written(
            static_cast<std::int64_t>(manifest.entries.size()),
            static_cast<std::int64_t>(manifest_bytes));
        return mkey;
    }

    std::string fetch_checkpoint_dir(const std::string& handle) override {
        auto fs = fs_();
        const std::string manifest_text = read_object_(*fs, handle);
        const auto manifest = CasManifest::decode(manifest_text);
        if (!manifest) {
            throw std::runtime_error("S3CasSnapshotStore::fetch: corrupt/unreadable manifest " +
                                     handle);
        }
        const std::string objects_ns = objects_prefix_();
        const std::filesystem::path staging = next_staging_dir_(handle);
        std::error_code ec;
        std::filesystem::remove_all(staging, ec);
        std::filesystem::create_directories(staging, ec);

        for (const auto& e : manifest->entries) {
            const std::filesystem::path dest = staging / e.name;
            std::filesystem::create_directories(dest.parent_path(), ec);
            // Content-addressed cache hit: serve from local disk, no GetObject.
            // The hash is the content, so the size guard is belt-and-braces.
            if (cache_ && cache_->get(objects_ns, e.hash, dest, e.size)) {
                continue;
            }
            download_object_(*fs, objects_ns + "/" + e.hash, dest);
            downloads_.fetch_add(1, std::memory_order_relaxed);
            clink::metrics::disagg::cas_object_downloaded();
            if (cache_) {
                cache_->put(objects_ns, e.hash, dest);
            }
        }
        return staging.string();
    }

    // Refcount-GC of checkpoint `id` (= M). An object is deleted ONLY if no
    // OTHER live manifest references it, computed by set difference against the
    // actual LIST of manifests at purge time (the list IS the refcount). Safety
    // (see the design's 3-leg argument):
    //   - retained checkpoints: their objects are in LIVE -> subtracted out;
    //   - in-flight newer checkpoint: clink commits N (its manifest durable)
    //     before purge(M) runs, and an SST shared by M and a later checkpoint is
    //     present in every checkpoint between (RocksDB never reuses an SST
    //     number), so it is in the retained N's manifest -> in LIVE -> kept;
    //   - crash-mid-purge: M's manifest is deleted FIRST, so a crash leaves
    //     unreferenced orphans (reclaimable by the sweep), never an unresolvable
    //     live checkpoint.
    // Conservative: if any surviving manifest cannot be read, no object is
    // deleted (leak, never over-delete). Best-effort and idempotent.
    void delete_checkpoint(const std::string& /*local_cp_path*/, CheckpointId id) override {
        std::shared_ptr<arrow::fs::S3FileSystem> fs;
        try {
            fs = fs_();
        } catch (...) {
            return;  // cannot reach the store; leave everything (safe)
        }
        const std::string m_key = manifest_key_(id);
        std::string m_text;
        try {
            m_text = read_object_(*fs, m_key);
        } catch (...) {
            return;  // M's manifest already gone -> nothing to purge here; any
                     // orphans from a prior partial purge are the sweep's job
        }
        const auto m_manifest = CasManifest::decode(m_text);
        if (!m_manifest) {
            (void)fs->DeleteFile(m_key);  // corrupt manifest: drop it, keep objects
            return;
        }

        // Union the hashes of every OTHER live manifest = the keep-set.
        std::unordered_set<std::string> live;
        bool live_incomplete = false;
        for (const auto& other_key : list_manifest_keys_(*fs)) {
            if (other_key == m_key) {
                continue;
            }
            try {
                const auto other = CasManifest::decode(read_object_(*fs, other_key));
                if (!other) {
                    live_incomplete = true;  // unreadable -> treat as referencing all
                    continue;
                }
                for (const auto& e : other->entries) {
                    live.insert(e.hash);
                }
            } catch (...) {
                live_incomplete = true;
            }
        }

        // Candidates = M's objects not referenced by any surviving manifest.
        std::vector<std::string> candidates;
        if (!live_incomplete) {
            std::unordered_set<std::string> seen;
            for (const auto& e : m_manifest->entries) {
                if (live.find(e.hash) == live.end() && seen.insert(e.hash).second) {
                    candidates.push_back(e.hash);
                }
            }
        }

        // Manifest FIRST (M is no longer live), then the now-unreferenced objects.
        (void)fs->DeleteFile(m_key);
        const std::string objects_ns = objects_prefix_();
        for (const auto& hash : candidates) {
            if (fs->DeleteFile(objects_ns + "/" + hash).ok()) {
                deletes_.fetch_add(1, std::memory_order_relaxed);
            }
        }
    }

    // Objects reclaimed by delete_checkpoint + sweep (for tests / metrics).
    [[nodiscard]] std::uint64_t objects_deleted() const noexcept {
        return deletes_.load(std::memory_order_relaxed);
    }

    // Whole-pool orphan reclamation - a SPACE backstop, NOT retention
    // correctness (delete_checkpoint already keeps storage bounded). It deletes
    // every objects/<hash> referenced by no live manifest, reclaiming orphans
    // left by a crash-mid-purge or an aborted write (objects uploaded, manifest
    // never written). Returns the count reclaimed.
    //
    // SAFETY: a whole-pool sweep races an in-flight checkpoint whose objects are
    // uploaded but whose manifest is not yet written - those objects look like
    // orphans. `min_age` guards that: an object younger than min_age is never
    // deleted, so as long as min_age exceeds the maximum checkpoint persist
    // duration, an in-flight checkpoint's objects are protected until its
    // manifest lands. Conservative: if any manifest is unreadable the sweep does
    // nothing (it cannot prove an object is unreferenced). Intended to run from
    // an admin / periodic trigger, not the checkpoint hot path.
    std::uint64_t sweep(std::chrono::seconds min_age) {
        auto fs = fs_();
        std::unordered_set<std::string> live;
        for (const auto& mkey : list_manifest_keys_(*fs)) {
            try {
                const auto m = CasManifest::decode(read_object_(*fs, mkey));
                if (!m) {
                    return 0;  // unreadable manifest: cannot safely sweep
                }
                for (const auto& e : m->entries) {
                    live.insert(e.hash);
                }
            } catch (...) {
                return 0;
            }
        }

        arrow::fs::FileSelector sel;
        sel.base_dir = objects_prefix_();
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
                continue;  // referenced by a live manifest
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
                deletes_.fetch_add(1, std::memory_order_relaxed);
            }
        }
        return reclaimed;
    }

    // FOUND-4: export a self-contained, relocatable savepoint of the checkpoint
    // named by `source_handle` to <sp_bucket>/<sp_prefix>. The live store's
    // manifest references objects only by content hash (no bucket/prefix), so a
    // savepoint is made portable by COPYING the manifest + every object it
    // references under the savepoint prefix. The result is independent of this
    // store's live checkpoint pool - it survives refcount-GC of the source
    // checkpoint, and the whole savepoint prefix can be copied/moved as a unit
    // and restored by pointing an S3CasSnapshotStore at the new location and
    // fetching the returned manifest handle (time-travel: any exported
    // savepoint restores). Objects are server-side CopyObject'd; re-export is
    // idempotent (content-addressed names are write-once).
    std::string export_savepoint(const std::string& source_handle,
                                 const std::string& sp_bucket,
                                 const std::string& sp_prefix) {
        auto fs = fs_();
        const auto manifest = CasManifest::decode(read_object_(*fs, source_handle));
        if (!manifest) {
            throw std::runtime_error("export_savepoint: corrupt/unreadable manifest " +
                                     source_handle);
        }
        std::string sp_base = sp_bucket;
        if (!sp_prefix.empty()) {
            sp_base += "/" + sp_prefix;
        }
        const std::string src_objects = objects_prefix_();
        const std::string sp_objects = sp_base + "/objects";
        std::unordered_set<std::string> copied;
        for (const auto& e : manifest->entries) {
            if (!copied.insert(e.hash).second) {
                continue;  // dedup within the manifest
            }
            const std::string src = src_objects + "/" + e.hash;
            const std::string dst = sp_objects + "/" + e.hash;
            if (auto s = fs->CopyFile(src, dst); !s.ok()) {
                throw std::runtime_error("export_savepoint: CopyFile(" + src + " -> " + dst +
                                         "): " + s.ToString());
            }
            savepoint_objects_.fetch_add(1, std::memory_order_relaxed);
        }
        // Copy the manifest verbatim (same hashes) into the savepoint prefix.
        const std::string sp_handle =
            sp_base + "/manifests/cp-" + std::to_string(manifest->checkpoint_id) + ".manifest";
        write_object_(*fs, sp_handle, read_object_(*fs, source_handle));
        return sp_handle;
    }

    // Objects copied into savepoints by export_savepoint (tests / metrics).
    [[nodiscard]] std::uint64_t savepoint_objects() const noexcept {
        return savepoint_objects_.load(std::memory_order_relaxed);
    }

    // The upload is the slow durable write; the backend runs it off the
    // operator thread via capture()/persist().
    [[nodiscard]] bool defers_durable_write() const noexcept override { return true; }

    [[nodiscard]] std::string description() const override {
        return "s3-cas://" + opts_.bucket + "/" + opts_.prefix;
    }

private:
    std::string base_prefix_() const {
        std::string p = opts_.bucket;
        if (!opts_.prefix.empty()) {
            p += "/" + opts_.prefix;
        }
        return p;
    }
    std::string objects_prefix_() const { return base_prefix_() + "/objects"; }
    std::string manifest_key_(CheckpointId id) const {
        return base_prefix_() + "/manifests/cp-" + std::to_string(id.value()) + ".manifest";
    }

    // Every manifest key under manifests/ (the live-manifest set for GC).
    std::vector<std::string> list_manifest_keys_(arrow::fs::S3FileSystem& fs) const {
        arrow::fs::FileSelector sel;
        sel.base_dir = base_prefix_() + "/manifests";
        sel.recursive = false;
        sel.allow_not_found = true;
        std::vector<std::string> keys;
        auto infos = fs.GetFileInfo(sel);
        if (!infos.ok()) {
            return keys;
        }
        for (const auto& info : *infos) {
            if (info.type() == arrow::fs::FileType::File) {
                keys.push_back(info.path());
            }
        }
        return keys;
    }

    std::filesystem::path next_staging_dir_(const std::string& handle) {
        std::string sanitized;
        sanitized.reserve(handle.size());
        for (char c : handle) {
            sanitized.push_back(c == '/' ? '_' : c);
        }
        static std::atomic<std::uint64_t> seq{0};
        return opts_.local_staging_dir /
               (sanitized + "-" + std::to_string(seq.fetch_add(1, std::memory_order_relaxed)));
    }

    // Stream the file through SHA-256 in fixed chunks (never the whole SST in
    // RAM) and return the 64-hex digest.
    static std::string hash_file_(const std::filesystem::path& path) {
        std::ifstream in(path, std::ios::binary);
        if (!in) {
            throw std::runtime_error("S3CasSnapshotStore: open for hashing " + path.string());
        }
        Sha256 h;
        std::vector<char> buf(1 << 20);
        while (in) {
            in.read(buf.data(), static_cast<std::streamsize>(buf.size()));
            const auto got = in.gcount();
            if (got > 0) {
                h.update(buf.data(), static_cast<std::size_t>(got));
            }
        }
        return Sha256::to_hex(h.finalize());
    }

    // True if obj_key already exists as a file of the expected size.
    static bool object_present_(arrow::fs::S3FileSystem& fs,
                                const std::string& obj_key,
                                std::uint64_t expected_size) {
        auto info = fs.GetFileInfo(obj_key);
        if (!info.ok() || info->type() != arrow::fs::FileType::File) {
            return false;
        }
        // A content-addressed object of the right size is the same bytes; the
        // size check is a cheap sanity guard against a truncated prior upload.
        return info->size() < 0 || static_cast<std::uint64_t>(info->size()) == expected_size;
    }

    static void upload_file_(arrow::fs::S3FileSystem& fs,
                             const std::filesystem::path& src,
                             const std::string& obj_key) {
        std::ifstream in(src, std::ios::binary);
        if (!in) {
            throw std::runtime_error("S3CasSnapshotStore::upload: open " + src.string());
        }
        auto out_result = fs.OpenOutputStream(obj_key);
        if (!out_result.ok()) {
            throw std::runtime_error("S3CasSnapshotStore::upload OpenOutputStream(" + obj_key +
                                     "): " + out_result.status().ToString());
        }
        auto out = *out_result;
        std::vector<char> buf(1 << 20);
        while (in) {
            in.read(buf.data(), static_cast<std::streamsize>(buf.size()));
            const auto got = in.gcount();
            if (got > 0) {
                if (auto s = out->Write(buf.data(), static_cast<std::int64_t>(got)); !s.ok()) {
                    throw std::runtime_error("S3CasSnapshotStore::upload Write(" + obj_key +
                                             "): " + s.ToString());
                }
            }
        }
        if (auto s = out->Close(); !s.ok()) {
            throw std::runtime_error("S3CasSnapshotStore::upload Close(" + obj_key +
                                     "): " + s.ToString());
        }
    }

    static void write_object_(arrow::fs::S3FileSystem& fs,
                              const std::string& key,
                              const std::string& bytes) {
        auto out_result = fs.OpenOutputStream(key);
        if (!out_result.ok()) {
            throw std::runtime_error("S3CasSnapshotStore::write OpenOutputStream(" + key +
                                     "): " + out_result.status().ToString());
        }
        auto out = *out_result;
        if (!bytes.empty()) {
            if (auto s = out->Write(bytes.data(), static_cast<std::int64_t>(bytes.size()));
                !s.ok()) {
                throw std::runtime_error("S3CasSnapshotStore::write Write(" + key +
                                         "): " + s.ToString());
            }
        }
        if (auto s = out->Close(); !s.ok()) {
            throw std::runtime_error("S3CasSnapshotStore::write Close(" + key +
                                     "): " + s.ToString());
        }
    }

    static std::string read_object_(arrow::fs::S3FileSystem& fs, const std::string& key) {
        auto file_result = fs.OpenInputFile(key);
        if (!file_result.ok()) {
            throw std::runtime_error("S3CasSnapshotStore::read OpenInputFile(" + key +
                                     "): " + file_result.status().ToString());
        }
        auto file = *file_result;
        auto size_result = file->GetSize();
        if (!size_result.ok()) {
            throw std::runtime_error("S3CasSnapshotStore::read GetSize(" + key +
                                     "): " + size_result.status().ToString());
        }
        const std::int64_t size = *size_result;
        std::string out(static_cast<std::size_t>(size), '\0');
        if (size > 0) {
            auto n = file->ReadAt(0, size, out.data());
            if (!n.ok()) {
                throw std::runtime_error("S3CasSnapshotStore::read ReadAt(" + key +
                                         "): " + n.status().ToString());
            }
        }
        return out;
    }

    static void download_object_(arrow::fs::S3FileSystem& fs,
                                 const std::string& s3_path,
                                 const std::filesystem::path& dest) {
        auto file_result = fs.OpenInputFile(s3_path);
        if (!file_result.ok()) {
            throw std::runtime_error("S3CasSnapshotStore::fetch OpenInputFile(" + s3_path +
                                     "): " + file_result.status().ToString());
        }
        auto file = *file_result;
        auto size_result = file->GetSize();
        if (!size_result.ok()) {
            throw std::runtime_error("S3CasSnapshotStore::fetch GetSize(" + s3_path +
                                     "): " + size_result.status().ToString());
        }
        std::ofstream out(dest, std::ios::binary | std::ios::trunc);
        if (!out) {
            throw std::runtime_error("S3CasSnapshotStore::fetch: open local " + dest.string());
        }
        const std::int64_t size = *size_result;
        if (size > 0) {
            auto buf_result = file->ReadAt(0, size);
            if (!buf_result.ok()) {
                throw std::runtime_error("S3CasSnapshotStore::fetch ReadAt(" + s3_path +
                                         "): " + buf_result.status().ToString());
            }
            auto buffer = *buf_result;
            out.write(reinterpret_cast<const char*>(buffer->data()),
                      static_cast<std::streamsize>(buffer->size()));
        }
        if (!out.good()) {
            throw std::runtime_error("S3CasSnapshotStore::fetch: short write to " + dest.string());
        }
    }

    std::shared_ptr<arrow::fs::S3FileSystem> fs_() {
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
            throw std::runtime_error("S3CasSnapshotStore: S3FileSystem::Make failed: " +
                                     fs_result.status().ToString());
        }
        fs_cached_ = *fs_result;
        return fs_cached_;
    }

    Options opts_;
    std::mutex fs_mu_;
    std::shared_ptr<arrow::fs::S3FileSystem> fs_cached_;
    std::unique_ptr<LocalObjectCache> cache_;
    std::atomic<std::uint64_t> uploads_{0};
    std::atomic<std::uint64_t> downloads_{0};
    std::atomic<std::uint64_t> deletes_{0};
    std::atomic<std::uint64_t> savepoint_objects_{0};
};

}  // namespace clink::s3
