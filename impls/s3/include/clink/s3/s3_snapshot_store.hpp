#pragma once

// S3SnapshotStore - object-storage implementation of SnapshotStore (DISAGG-2).
//
// Implements the DISAGG-1 seam over Arrow's S3FileSystem: a state backend's
// per-checkpoint directory is uploaded to s3://<bucket>/<prefix>/cp-<id>/ on
// write_checkpoint_dir, and downloaded back to a local staging dir on
// fetch_checkpoint_dir (which the backend's restore re-homes from). delete
// drops the object "directory" for an id. The handle embedded in the Snapshot
// is the s3 directory path "<bucket>/<prefix>/cp-<id>", derivable from the id
// so purge can address it without the handle.
//
// Generic over the directory contents - it copies whatever regular files the
// backend's checkpoint dir holds (RocksDB SSTs/MANIFEST/CURRENT/OPTIONS), so
// it carries no RocksDB dependency and only needs Arrow's S3 transport (no
// AWS-SDK link). The rocksdb+s3 scheme wiring is DISAGG-4.
//
// Transport: the same Arrow S3FileSystem + process-wide init as the Parquet S3
// connector, so the two never double-initialise. Credentials via the standard
// AWS chain; endpoint_override targets MinIO/localstack.
//
// Not yet: cross-snapshot object dedup (each checkpoint uploads its full file
// set; DISAGG-6's content-addressed manifest is the dedup), and staging-dir
// reclamation (DISAGG-5's cache manages local files).

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/s3fs.h>
#include <arrow/io/api.h>

#include "clink/connectors/parquet_s3_sink.hpp"  // clink::detail::ensure_arrow_s3_initialised
#include "clink/core/types.hpp"
#include "clink/s3/local_object_cache.hpp"
#include "clink/state/snapshot_store.hpp"

namespace clink::s3 {

class S3SnapshotStore final : public clink::SnapshotStore {
public:
    struct Options {
        std::string bucket;                            // required
        std::string prefix;                            // object-key prefix (e.g. "job/0")
        std::optional<std::string> region;             // explicit region
        std::optional<std::string> endpoint_override;  // MinIO / localstack
        bool allow_anonymous{false};                   // public-bucket access
        // Where fetched checkpoints are downloaded for the backend to open.
        std::filesystem::path local_staging_dir{std::filesystem::temp_directory_path() /
                                                "clink-s3-snapshot-staging"};
        // DISAGG-5: content-addressed local cache of immutable objects. When
        // cache_bytes > 0, fetch downloads only cache-miss objects (and write
        // populates the cache), so a same-host restart / overlapping rescale
        // re-uses local SSTs. Empty cache_dir + 0 bytes disables it.
        std::filesystem::path cache_dir;
        std::uint64_t cache_bytes{0};
    };

    explicit S3SnapshotStore(Options opts) : opts_(std::move(opts)) {
        if (opts_.bucket.empty()) {
            throw std::invalid_argument("S3SnapshotStore: bucket is required");
        }
        if (opts_.cache_bytes > 0) {
            const std::filesystem::path cdir =
                opts_.cache_dir.empty()
                    ? (std::filesystem::temp_directory_path() / "clink-s3-object-cache")
                    : opts_.cache_dir;
            cache_ = std::make_unique<LocalObjectCache>(cdir, opts_.cache_bytes);
        }
    }

    // Count of objects actually downloaded from S3 by fetch (cache misses).
    // For tests/metrics observability.
    [[nodiscard]] std::uint64_t objects_downloaded() const noexcept {
        return downloads_.load(std::memory_order_relaxed);
    }

    // Upload every regular file in the local checkpoint dir to
    // s3://<bucket>/<prefix>/cp-<id>/<filename>. Returns the s3 dir handle.
    std::string write_checkpoint_dir(const std::string& local_cp_path, CheckpointId id) override {
        auto fs = fs_();
        const std::string handle = dir_handle_(id);
        // Clear a stale prior write of the same id so a re-snapshot is clean.
        (void)fs->DeleteDirContents(handle, /*missing_dir_ok=*/true);

        std::error_code ec;
        for (const auto& entry : std::filesystem::directory_iterator(local_cp_path, ec)) {
            if (!entry.is_regular_file()) {
                continue;  // RocksDB checkpoint dirs are flat; skip any stray dirs
            }
            const std::string obj = handle + "/" + entry.path().filename().string();
            std::ifstream in(entry.path(), std::ios::binary);
            if (!in) {
                throw std::runtime_error("S3SnapshotStore::write: open local " +
                                         entry.path().string());
            }
            const std::vector<char> buf((std::istreambuf_iterator<char>(in)),
                                        std::istreambuf_iterator<char>());
            auto out_result = fs->OpenOutputStream(obj);
            if (!out_result.ok()) {
                throw std::runtime_error("S3SnapshotStore::write OpenOutputStream(" + obj +
                                         "): " + out_result.status().ToString());
            }
            auto out = *out_result;
            if (!buf.empty()) {
                if (auto s = out->Write(buf.data(), static_cast<std::int64_t>(buf.size()));
                    !s.ok()) {
                    throw std::runtime_error("S3SnapshotStore::write Write(" + obj +
                                             "): " + s.ToString());
                }
            }
            if (auto s = out->Close(); !s.ok()) {
                throw std::runtime_error("S3SnapshotStore::write Close(" + obj +
                                         "): " + s.ToString());
            }
            // Populate the cache from the local file so a same-host restore
            // reuses it without re-downloading (no-op for mutable names).
            if (cache_) {
                cache_->put(std::filesystem::path(handle).parent_path().string(),
                            entry.path().filename().string(),
                            entry.path());
            }
        }
        return handle;
    }

    // Download every object under the handle into a fresh local staging dir
    // and return that dir for the backend to re-home from.
    std::string fetch_checkpoint_dir(const std::string& handle) override {
        auto fs = fs_();
        const std::filesystem::path staging = next_staging_dir_(handle);
        std::error_code ec;
        std::filesystem::remove_all(staging, ec);
        std::filesystem::create_directories(staging, ec);

        arrow::fs::FileSelector sel;
        sel.base_dir = handle;
        sel.recursive = true;
        // A missing/empty prefix is not an error: it yields an empty staging
        // dir (e.g. fetching after a delete, or a checkpoint that wrote no
        // files). The backend's restore then sees an empty dir rather than a
        // throw; a genuinely-expected-nonempty fetch is caught by the caller
        // asserting the recovered state.
        sel.allow_not_found = true;
        auto infos_result = fs->GetFileInfo(sel);
        if (!infos_result.ok()) {
            throw std::runtime_error("S3SnapshotStore::fetch GetFileInfo(" + handle +
                                     "): " + infos_result.status().ToString());
        }
        const std::string ns = std::filesystem::path(handle).parent_path().string();
        for (const auto& info : *infos_result) {
            if (info.type() != arrow::fs::FileType::File) {
                continue;
            }
            const std::string& s3_path = info.path();  // <handle>/<relname>
            std::string rel = s3_path.size() > handle.size() ? s3_path.substr(handle.size()) : "";
            if (!rel.empty() && rel.front() == '/') {
                rel.erase(0, 1);
            }
            const std::filesystem::path dest = staging / rel;
            std::filesystem::create_directories(dest.parent_path(), ec);
            // Local object cache: a hit serves the object from local disk (no
            // GetObject); a miss downloads it and populates the cache. The S3
            // object's size (free from the LIST above) guards against serving a
            // stale same-named object from a different lineage.
            const auto expected = info.size() >= 0 ? static_cast<std::uint64_t>(info.size())
                                                   : LocalObjectCache::kUnknownSize;
            if (cache_ && cache_->get(ns, rel, dest, expected)) {
                continue;
            }
            download_object_(*fs, s3_path, dest);
            downloads_.fetch_add(1, std::memory_order_relaxed);
            if (cache_) {
                cache_->put(ns, rel, dest);
            }
        }
        return staging.string();
    }

    void delete_checkpoint(const std::string& /*local_cp_path*/, CheckpointId id) override {
        // Best-effort: drop the object "directory" for this id.
        if (auto fs = try_fs_(); fs) {
            (void)(*fs)->DeleteDir(dir_handle_(id));
        }
    }

    // The upload is the slow durable write; the backend should run it off the
    // operator thread via capture()/persist().
    [[nodiscard]] bool defers_durable_write() const noexcept override { return true; }

    [[nodiscard]] std::string description() const override {
        return "s3://" + opts_.bucket + "/" + opts_.prefix;
    }

private:
    std::string dir_handle_(CheckpointId id) const {
        std::string h = opts_.bucket;
        if (!opts_.prefix.empty()) {
            h += "/" + opts_.prefix;
        }
        h += "/cp-" + std::to_string(id.value());
        return h;
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

    static void download_object_(arrow::fs::S3FileSystem& fs,
                                 const std::string& s3_path,
                                 const std::filesystem::path& dest) {
        auto file_result = fs.OpenInputFile(s3_path);
        if (!file_result.ok()) {
            throw std::runtime_error("S3SnapshotStore::fetch OpenInputFile(" + s3_path +
                                     "): " + file_result.status().ToString());
        }
        auto file = *file_result;
        auto size_result = file->GetSize();
        if (!size_result.ok()) {
            throw std::runtime_error("S3SnapshotStore::fetch GetSize(" + s3_path +
                                     "): " + size_result.status().ToString());
        }
        std::ofstream out(dest, std::ios::binary | std::ios::trunc);
        if (!out) {
            throw std::runtime_error("S3SnapshotStore::fetch: open local " + dest.string());
        }
        const std::int64_t size = *size_result;
        if (size > 0) {
            auto buf_result = file->ReadAt(0, size);
            if (!buf_result.ok()) {
                throw std::runtime_error("S3SnapshotStore::fetch ReadAt(" + s3_path +
                                         "): " + buf_result.status().ToString());
            }
            auto buffer = *buf_result;
            out.write(reinterpret_cast<const char*>(buffer->data()),
                      static_cast<std::streamsize>(buffer->size()));
        }
    }

    std::shared_ptr<arrow::fs::S3FileSystem> fs_() {
        auto result = try_fs_();
        if (!result) {
            throw std::runtime_error("S3SnapshotStore: S3FileSystem::Make failed");
        }
        return *result;
    }

    std::optional<std::shared_ptr<arrow::fs::S3FileSystem>> try_fs_() {
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
            return std::nullopt;
        }
        fs_cached_ = *fs_result;
        return fs_cached_;
    }

    Options opts_;
    std::mutex fs_mu_;
    std::shared_ptr<arrow::fs::S3FileSystem> fs_cached_;
    std::unique_ptr<LocalObjectCache> cache_;  // null = caching disabled
    std::atomic<std::uint64_t> downloads_{0};
};

}  // namespace clink::s3
