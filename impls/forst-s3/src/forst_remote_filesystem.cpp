#include "forst_remote_filesystem.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/s3fs.h>
#include <arrow/io/interfaces.h>

#include "clink/connectors/parquet_s3_sink.hpp"  // clink::detail::ensure_arrow_s3_initialised
#include "clink/state/snapshot_store.hpp"

// ForSt's headers (the imported engine include dir is prepended for this
// target - see impls/forst-s3/CMakeLists.txt). Everything engine-side is
// namespace forstdb, so nothing here collides with the bundled RocksDB.
#include <rocksdb/env.h>
#include <rocksdb/file_system.h>
#include <rocksdb/io_status.h>

namespace clink::forst_s3 {
namespace {

namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// Path mapping. The contract that makes the hybrid layout sound:
//
//   * remote = immutable data files only: "*.sst" under the local root.
//     Write-once (table builder), random-read, delete - object-store
//     friendly by construction, and never renamed by the engine on the
//     keyed-state paths clink uses.
//   * local  = everything else (MANIFEST, CURRENT, WAL *.log, LOCK,
//     OPTIONS*, IDENTITY, LOG*): small, mutable, append-and-rename files
//     that object storage cannot model faithfully.
//
// A remote path maps to <bucket>/<key_prefix>/<path relative to
// local_root>, so the working DB, its sibling ".cp-<id>" checkpoint dirs
// and restore working dirs (all under local_root by construction of the
// scheme) each get a distinct, listable object prefix.
// ---------------------------------------------------------------------------

[[nodiscard]] bool has_sst_suffix(const std::string& name) {
    static constexpr std::string_view kSuffix{".sst"};
    return name.size() >= kSuffix.size() &&
           name.compare(name.size() - kSuffix.size(), kSuffix.size(), kSuffix) == 0;
}

struct Mapping {
    std::string bucket;
    std::string key_prefix;  // no trailing slash
    std::string local_root;  // lexically-normal absolute path

    [[nodiscard]] bool routes(const std::string& fname) const {
        if (!has_sst_suffix(fname)) {
            return false;
        }
        const auto norm = fs::path(fname).lexically_normal().string();
        return norm.size() > local_root.size() &&
               norm.compare(0, local_root.size(), local_root) == 0;
    }

    // Path relative to the root ('/'-separated): the object key tail and
    // the cache key are the same string by design.
    [[nodiscard]] std::string relative(const std::string& fname) const {
        return fs::path(fname).lexically_normal().lexically_relative(local_root).generic_string();
    }

    // "<bucket>/<key_prefix>/<relative>" - the path form arrow's
    // filesystem API takes.
    [[nodiscard]] std::string object_path(const std::string& fname) const {
        const auto rel = relative(fname);
        return bucket + "/" + (key_prefix.empty() ? rel : key_prefix + "/" + rel);
    }

    // The object "directory" for a local dir path (no trailing slash).
    // The root itself maps to the bare key prefix ("." would be an
    // invalid key component).
    [[nodiscard]] std::string object_dir(const std::string& dir) const {
        auto rel = relative(dir);
        if (rel == ".") {
            rel.clear();
        }
        if (rel.empty()) {
            return key_prefix.empty() ? bucket : bucket + "/" + key_prefix;
        }
        return bucket + "/" + (key_prefix.empty() ? rel : key_prefix + "/" + rel);
    }
};

std::shared_ptr<arrow::fs::S3FileSystem> make_arrow_s3(const RemoteSstOptions& o) {
    clink::detail::ensure_arrow_s3_initialised();
    auto s3_opts = arrow::fs::S3Options::Defaults();
    if (!o.region.empty()) {
        s3_opts.region = o.region;
    }
    if (!o.endpoint.empty()) {
        s3_opts.endpoint_override = o.endpoint;
        s3_opts.scheme = "http";
    }
    if (o.anonymous) {
        s3_opts = arrow::fs::S3Options::Anonymous();
        if (!o.region.empty()) {
            s3_opts.region = o.region;
        }
        if (!o.endpoint.empty()) {
            s3_opts.endpoint_override = o.endpoint;
            s3_opts.scheme = "http";
        }
    }
    auto made = arrow::fs::S3FileSystem::Make(s3_opts);
    if (!made.ok()) {
        throw std::runtime_error("forst remote-sst: S3FileSystem::Make failed: " +
                                 made.status().ToString());
    }
    return *made;
}

forstdb::IOStatus to_io_status(const arrow::Status& st, const char* what) {
    if (st.ok()) {
        return forstdb::IOStatus::OK();
    }
    if (st.IsIOError() && st.ToString().find("404") != std::string::npos) {
        return forstdb::IOStatus::NotFound(st.ToString());
    }
    return forstdb::IOStatus::IOError(std::string(what) + ": " + st.ToString());
}

// ---------------------------------------------------------------------------
// Local SST read cache. Populated by write-tee (the bytes land here as
// they upload, so flush/compaction outputs - the bulk of steady-state
// reads - serve locally), keyed by the object-relative path, evicted LRU
// to the byte budget. Advisory by construction: a miss reads S3, an
// eviction unlinks (POSIX keeps in-flight reads on the open fd valid),
// and the index rebuilds from a directory scan at startup so a restart
// reuses what survived. Files are written as "<name>.part" and renamed
// into place at finalize, so a torn tee can never serve.
// ---------------------------------------------------------------------------

class LocalSstCache {
public:
    LocalSstCache(std::string dir, std::uint64_t budget_bytes)
        : dir_(std::move(dir)), budget_(budget_bytes) {
        std::error_code ec;
        fs::create_directories(dir_, ec);
        // Rebuild the index from what survived a previous process. Order
        // by mtime so eviction still drops the coldest first.
        std::vector<std::pair<fs::file_time_type, fs::path>> found;
        for (auto it = fs::recursive_directory_iterator(dir_, ec);
             !ec && it != fs::recursive_directory_iterator();
             ++it) {
            if (!it->is_regular_file()) {
                continue;
            }
            if (it->path().extension() == ".part") {
                fs::remove(it->path(), ec);  // torn tee from a crash
                continue;
            }
            found.emplace_back(fs::last_write_time(it->path(), ec), it->path());
        }
        std::sort(found.begin(), found.end());
        for (const auto& [_, p] : found) {
            std::error_code sec;
            const auto size = fs::file_size(p, sec);
            if (!sec) {
                insert_locked_(p.lexically_relative(dir_).generic_string(),
                               static_cast<std::uint64_t>(size));
            }
        }
        evict_to_budget_locked_();
    }

    // Full local path a finalized entry serves from; nullopt on miss.
    [[nodiscard]] std::optional<std::string> lookup(const std::string& rel) {
        std::lock_guard lock(mu_);
        auto it = index_.find(rel);
        if (it == index_.end()) {
            return std::nullopt;
        }
        lru_.splice(lru_.end(), lru_, it->second.lru_it);  // touch
        return (fs::path(dir_) / rel).string();
    }

    // Path a tee should write to ("<final>.part"); parents created.
    [[nodiscard]] std::string part_path(const std::string& rel) {
        const auto final_path = fs::path(dir_) / rel;
        std::error_code ec;
        fs::create_directories(final_path.parent_path(), ec);
        return final_path.string() + ".part";
    }

    // Promote a completed tee into the cache (rename part -> final).
    void finalize(const std::string& rel) {
        const auto final_path = fs::path(dir_) / rel;
        std::error_code ec;
        fs::rename(final_path.string() + ".part", final_path, ec);
        if (ec) {
            return;  // advisory: a failed promote is just a miss
        }
        const auto size = fs::file_size(final_path, ec);
        if (ec) {
            return;
        }
        std::lock_guard lock(mu_);
        insert_locked_(rel, static_cast<std::uint64_t>(size));
        evict_to_budget_locked_();
    }

    void discard_part(const std::string& rel) {
        std::error_code ec;
        fs::remove((fs::path(dir_) / rel).string() + ".part", ec);
    }

    // Engine GC'd the SST: drop the cached copy too.
    void erase(const std::string& rel) {
        std::lock_guard lock(mu_);
        auto it = index_.find(rel);
        if (it == index_.end()) {
            return;
        }
        std::error_code ec;
        fs::remove(fs::path(dir_) / rel, ec);
        total_ -= it->second.size;
        lru_.erase(it->second.lru_it);
        index_.erase(it);
    }

private:
    struct Entry {
        std::uint64_t size{0};
        std::list<std::string>::iterator lru_it;
    };

    void insert_locked_(const std::string& rel, std::uint64_t size) {
        if (auto it = index_.find(rel); it != index_.end()) {
            total_ -= it->second.size;
            lru_.erase(it->second.lru_it);
            index_.erase(it);
        }
        lru_.push_back(rel);
        index_[rel] = Entry{size, std::prev(lru_.end())};
        total_ += size;
    }

    void evict_to_budget_locked_() {
        std::error_code ec;
        while (total_ > budget_ && !lru_.empty()) {
            const auto victim = lru_.front();
            auto it = index_.find(victim);
            fs::remove(fs::path(dir_) / victim, ec);
            total_ -= it->second.size;
            index_.erase(it);
            lru_.pop_front();
        }
    }

    std::string dir_;
    std::uint64_t budget_;
    std::mutex mu_;
    std::list<std::string> lru_;  // front = coldest
    std::unordered_map<std::string, Entry> index_;
    std::uint64_t total_{0};
};

// ---------------------------------------------------------------------------
// File adapters
// ---------------------------------------------------------------------------

class S3SequentialFile final : public forstdb::FSSequentialFile {
public:
    explicit S3SequentialFile(std::shared_ptr<arrow::io::RandomAccessFile> file)
        : file_(std::move(file)) {}

    forstdb::IOStatus Read(size_t n,
                           const forstdb::IOOptions& /*options*/,
                           forstdb::Slice* result,
                           char* scratch,
                           forstdb::IODebugContext* /*dbg*/) override {
        auto read = file_->ReadAt(offset_, static_cast<std::int64_t>(n), scratch);
        if (!read.ok()) {
            return to_io_status(read.status(), "sequential read");
        }
        offset_ += *read;
        *result = forstdb::Slice(scratch, static_cast<size_t>(*read));
        return forstdb::IOStatus::OK();
    }

    forstdb::IOStatus Skip(uint64_t n) override {
        offset_ += static_cast<std::int64_t>(n);
        return forstdb::IOStatus::OK();
    }

private:
    std::shared_ptr<arrow::io::RandomAccessFile> file_;
    std::int64_t offset_{0};
};

class S3RandomAccessFile final : public forstdb::FSRandomAccessFile {
public:
    explicit S3RandomAccessFile(std::shared_ptr<arrow::io::RandomAccessFile> file)
        : file_(std::move(file)) {}

    forstdb::IOStatus Read(uint64_t offset,
                           size_t n,
                           const forstdb::IOOptions& /*options*/,
                           forstdb::Slice* result,
                           char* scratch,
                           forstdb::IODebugContext* /*dbg*/) const override {
        // arrow's ReadAt is safe for concurrent callers, matching the
        // engine's const/concurrent contract for this method.
        auto read =
            file_->ReadAt(static_cast<std::int64_t>(offset), static_cast<std::int64_t>(n), scratch);
        if (!read.ok()) {
            return to_io_status(read.status(), "random read");
        }
        *result = forstdb::Slice(scratch, static_cast<size_t>(*read));
        return forstdb::IOStatus::OK();
    }

private:
    std::shared_ptr<arrow::io::RandomAccessFile> file_;
};

class S3WritableFile final : public forstdb::FSWritableFile {
public:
    // cache/rel: optional write-tee target (see LocalSstCache).
    S3WritableFile(std::shared_ptr<arrow::io::OutputStream> out,
                   std::shared_ptr<LocalSstCache> cache,
                   std::string rel)
        : out_(std::move(out)), cache_(std::move(cache)), rel_(std::move(rel)) {
        if (cache_) {
            tee_.open(cache_->part_path(rel_), std::ios::binary | std::ios::trunc);
            // A tee that cannot open is just a disabled cache for this
            // file; the upload path is unaffected.
        }
    }

    ~S3WritableFile() override {
        // The engine closes files explicitly on success paths; this is
        // the error-path safety net (an abandoned multipart upload).
        if (!closed_) {
            (void)out_->Close();
            if (cache_) {
                cache_->discard_part(rel_);
            }
        }
    }

    forstdb::IOStatus Append(const forstdb::Slice& data,
                             const forstdb::IOOptions& /*options*/,
                             forstdb::IODebugContext* /*dbg*/) override {
        auto st = out_->Write(data.data(), static_cast<std::int64_t>(data.size()));
        if (!st.ok()) {
            return to_io_status(st, "append");
        }
        if (tee_.is_open()) {
            tee_.write(data.data(), static_cast<std::streamsize>(data.size()));
        }
        size_ += data.size();
        return forstdb::IOStatus::OK();
    }

    forstdb::IOStatus Close(const forstdb::IOOptions& /*options*/,
                            forstdb::IODebugContext* /*dbg*/) override {
        closed_ = true;
        // Completes the multipart upload: this is the durability point
        // for a remote data file. Sound for clink's usage because the
        // engine finishes and closes an SST before the MANIFEST ever
        // references it - a crash mid-write leaves an unreferenced
        // partial upload, never a referenced torn file. (Job-level
        // durability is clink's checkpoint, not engine crash recovery.)
        auto st = to_io_status(out_->Close(), "close");
        if (cache_) {
            if (st.ok() && tee_.is_open()) {
                tee_.close();
                if (tee_.good()) {
                    cache_->finalize(rel_);  // promote the tee: cache hit from now on
                } else {
                    cache_->discard_part(rel_);
                }
            } else {
                if (tee_.is_open()) {
                    tee_.close();
                }
                cache_->discard_part(rel_);
            }
        }
        return st;
    }

    forstdb::IOStatus Flush(const forstdb::IOOptions& /*options*/,
                            forstdb::IODebugContext* /*dbg*/) override {
        return forstdb::IOStatus::OK();
    }

    // Sync/Fsync: object storage has no byte-durability short of
    // completing the upload; Close() above is the durability point (see
    // its comment for why that is sufficient here).
    forstdb::IOStatus Sync(const forstdb::IOOptions& /*options*/,
                           forstdb::IODebugContext* /*dbg*/) override {
        return forstdb::IOStatus::OK();
    }
    forstdb::IOStatus Fsync(const forstdb::IOOptions& /*options*/,
                            forstdb::IODebugContext* /*dbg*/) override {
        return forstdb::IOStatus::OK();
    }

    uint64_t GetFileSize(const forstdb::IOOptions& /*options*/,
                         forstdb::IODebugContext* /*dbg*/) override {
        return size_;
    }

private:
    std::shared_ptr<arrow::io::OutputStream> out_;
    std::shared_ptr<LocalSstCache> cache_;  // null = no tee
    std::string rel_;
    std::ofstream tee_;
    uint64_t size_{0};
    bool closed_{false};
};

// ---------------------------------------------------------------------------
// The routing filesystem
// ---------------------------------------------------------------------------

class RemoteSstFileSystem final : public forstdb::FileSystemWrapper {
public:
    RemoteSstFileSystem(Mapping mapping,
                        std::shared_ptr<arrow::fs::S3FileSystem> s3,
                        std::shared_ptr<LocalSstCache> cache)
        : forstdb::FileSystemWrapper(forstdb::FileSystem::Default()),
          map_(std::move(mapping)),
          s3_(std::move(s3)),
          cache_(std::move(cache)) {}

    static const char* kClassName() { return "clink-forst-remote-sst"; }
    const char* Name() const override { return kClassName(); }

    forstdb::IOStatus NewSequentialFile(const std::string& fname,
                                        const forstdb::FileOptions& file_opts,
                                        std::unique_ptr<forstdb::FSSequentialFile>* result,
                                        forstdb::IODebugContext* dbg) override {
        if (!map_.routes(fname)) {
            return forstdb::FileSystemWrapper::NewSequentialFile(fname, file_opts, result, dbg);
        }
        if (cache_) {
            if (auto hit = cache_->lookup(map_.relative(fname))) {
                // Serve the cached copy through the wrapped local FS.
                return forstdb::FileSystemWrapper::NewSequentialFile(*hit, file_opts, result, dbg);
            }
        }
        auto opened = s3_->OpenInputFile(map_.object_path(fname));
        if (!opened.ok()) {
            return to_io_status(opened.status(), "open sequential");
        }
        *result = std::make_unique<S3SequentialFile>(*opened);
        return forstdb::IOStatus::OK();
    }

    forstdb::IOStatus NewRandomAccessFile(const std::string& fname,
                                          const forstdb::FileOptions& file_opts,
                                          std::unique_ptr<forstdb::FSRandomAccessFile>* result,
                                          forstdb::IODebugContext* dbg) override {
        if (!map_.routes(fname)) {
            return forstdb::FileSystemWrapper::NewRandomAccessFile(fname, file_opts, result, dbg);
        }
        if (cache_) {
            if (auto hit = cache_->lookup(map_.relative(fname))) {
                return forstdb::FileSystemWrapper::NewRandomAccessFile(
                    *hit, file_opts, result, dbg);
            }
        }
        auto opened = s3_->OpenInputFile(map_.object_path(fname));
        if (!opened.ok()) {
            return to_io_status(opened.status(), "open random-access");
        }
        *result = std::make_unique<S3RandomAccessFile>(*opened);
        return forstdb::IOStatus::OK();
    }

    forstdb::IOStatus NewWritableFile(const std::string& fname,
                                      const forstdb::FileOptions& file_opts,
                                      std::unique_ptr<forstdb::FSWritableFile>* result,
                                      forstdb::IODebugContext* dbg) override {
        if (!map_.routes(fname)) {
            return forstdb::FileSystemWrapper::NewWritableFile(fname, file_opts, result, dbg);
        }
        auto out = s3_->OpenOutputStream(map_.object_path(fname));
        if (!out.ok()) {
            return to_io_status(out.status(), "open writable");
        }
        // The tee lands the bytes in the local cache as they upload.
        *result = std::make_unique<S3WritableFile>(*out, cache_, map_.relative(fname));
        return forstdb::IOStatus::OK();
    }

    forstdb::IOStatus FileExists(const std::string& fname,
                                 const forstdb::IOOptions& options,
                                 forstdb::IODebugContext* dbg) override {
        if (!map_.routes(fname)) {
            return forstdb::FileSystemWrapper::FileExists(fname, options, dbg);
        }
        auto info = s3_->GetFileInfo(map_.object_path(fname));
        if (!info.ok()) {
            return to_io_status(info.status(), "stat");
        }
        return info->type() == arrow::fs::FileType::File ? forstdb::IOStatus::OK()
                                                         : forstdb::IOStatus::NotFound(fname);
    }

    forstdb::IOStatus GetFileSize(const std::string& fname,
                                  const forstdb::IOOptions& options,
                                  uint64_t* file_size,
                                  forstdb::IODebugContext* dbg) override {
        if (!map_.routes(fname)) {
            return forstdb::FileSystemWrapper::GetFileSize(fname, options, file_size, dbg);
        }
        auto info = s3_->GetFileInfo(map_.object_path(fname));
        if (!info.ok()) {
            return to_io_status(info.status(), "size");
        }
        if (info->type() != arrow::fs::FileType::File) {
            return forstdb::IOStatus::NotFound(fname);
        }
        *file_size = static_cast<uint64_t>(info->size());
        return forstdb::IOStatus::OK();
    }

    forstdb::IOStatus GetFileModificationTime(const std::string& fname,
                                              const forstdb::IOOptions& options,
                                              uint64_t* file_mtime,
                                              forstdb::IODebugContext* dbg) override {
        if (!map_.routes(fname)) {
            return forstdb::FileSystemWrapper::GetFileModificationTime(
                fname, options, file_mtime, dbg);
        }
        auto info = s3_->GetFileInfo(map_.object_path(fname));
        if (!info.ok() || info->type() != arrow::fs::FileType::File) {
            return forstdb::IOStatus::NotFound(fname);
        }
        *file_mtime = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::seconds>(info->mtime().time_since_epoch())
                .count());
        return forstdb::IOStatus::OK();
    }

    forstdb::IOStatus DeleteFile(const std::string& fname,
                                 const forstdb::IOOptions& options,
                                 forstdb::IODebugContext* dbg) override {
        if (!map_.routes(fname)) {
            return forstdb::FileSystemWrapper::DeleteFile(fname, options, dbg);
        }
        if (cache_) {
            cache_->erase(map_.relative(fname));  // engine GC drops the cached copy too
        }
        return to_io_status(s3_->DeleteFile(map_.object_path(fname)), "delete");
    }

    // Merge view: the engine enumerates a DB dir at open (obsolete-file
    // GC, WAL discovery), so the listing must cover BOTH halves of the
    // layout - the local metadata files and the remote data files.
    forstdb::IOStatus GetChildren(const std::string& dir,
                                  const forstdb::IOOptions& options,
                                  std::vector<std::string>* result,
                                  forstdb::IODebugContext* dbg) override {
        auto st = forstdb::FileSystemWrapper::GetChildren(dir, options, result, dbg);
        if (!st.ok()) {
            return st;
        }
        const auto norm = fs::path(dir).lexically_normal().string();
        if (norm.size() <= map_.local_root.size() ||
            norm.compare(0, map_.local_root.size(), map_.local_root) != 0) {
            return st;  // outside the mapped root: local-only listing
        }
        arrow::fs::FileSelector sel;
        sel.base_dir = map_.object_dir(dir);
        sel.allow_not_found = true;  // fresh dir: no remote objects yet
        sel.recursive = false;
        auto infos = s3_->GetFileInfo(sel);
        if (!infos.ok()) {
            return to_io_status(infos.status(), "list");
        }
        for (const auto& info : *infos) {
            if (info.type() == arrow::fs::FileType::File) {
                result->push_back(info.base_name());
            }
        }
        std::sort(result->begin(), result->end());
        result->erase(std::unique(result->begin(), result->end()), result->end());
        return forstdb::IOStatus::OK();
    }

    // Checkpoints hard-link the immutable data files. Object storage has
    // no links, but it has server-side copy: same effect the engine
    // wants (an independent, cheap reference that survives deletion of
    // the source), no data movement through this process. Semantics
    // note: unlike a hard link, storage is per-copy - a retained
    // checkpoint holds full objects, reclaimed on purge.
    forstdb::IOStatus LinkFile(const std::string& src,
                               const std::string& target,
                               const forstdb::IOOptions& options,
                               forstdb::IODebugContext* dbg) override {
        const bool src_remote = map_.routes(src);
        const bool dst_remote = map_.routes(target);
        if (!src_remote && !dst_remote) {
            return forstdb::FileSystemWrapper::LinkFile(src, target, options, dbg);
        }
        if (src_remote && dst_remote) {
            return to_io_status(s3_->CopyFile(map_.object_path(src), map_.object_path(target)),
                                "link (server-side copy)");
        }
        return forstdb::IOStatus::NotSupported(
            "forst remote-sst: cannot link across the local/remote boundary");
    }

    // Data FILES are never renamed on the paths clink drives (file
    // renames touch only the local metadata set: CURRENT, OPTIONS,
    // dbtmp). DIRECTORY renames do happen: the engine's checkpoint
    // stages into "<cp>.tmp" and renames the dir into place at the end.
    // The base filesystem renames the local metadata dir; any remote
    // data files parked under the old dir's prefix must migrate with it
    // (server-side copy + delete), or the staged checkpoint's SSTs would
    // be stranded at the ".tmp" prefix.
    forstdb::IOStatus RenameFile(const std::string& src,
                                 const std::string& target,
                                 const forstdb::IOOptions& options,
                                 forstdb::IODebugContext* dbg) override {
        if (map_.routes(src) || map_.routes(target)) {
            return forstdb::IOStatus::NotSupported(
                "forst remote-sst: remote data files never rename");
        }
        auto st = forstdb::FileSystemWrapper::RenameFile(src, target, options, dbg);
        if (!st.ok()) {
            return st;
        }
        // Migrate the remote half of a renamed DIRECTORY under the root.
        const auto norm = fs::path(src).lexically_normal().string();
        if (norm.size() > map_.local_root.size() &&
            norm.compare(0, map_.local_root.size(), map_.local_root) == 0) {
            arrow::fs::FileSelector sel;
            sel.base_dir = map_.object_dir(src);
            sel.allow_not_found = true;  // cheap no-op for plain file renames
            sel.recursive = false;
            auto infos = s3_->GetFileInfo(sel);
            if (!infos.ok()) {
                return to_io_status(infos.status(), "rename (list src prefix)");
            }
            for (const auto& info : *infos) {
                if (info.type() != arrow::fs::FileType::File) {
                    continue;
                }
                const auto dst = map_.object_dir(target) + "/" + info.base_name();
                auto copy_st = s3_->CopyFile(info.path(), dst);
                if (!copy_st.ok()) {
                    return to_io_status(copy_st, "rename (migrate object)");
                }
                (void)s3_->DeleteFile(info.path());
            }
        }
        return forstdb::IOStatus::OK();
    }

private:
    Mapping map_;
    std::shared_ptr<arrow::fs::S3FileSystem> s3_;
    std::shared_ptr<LocalSstCache> cache_;  // null = cache disabled
};

// ---------------------------------------------------------------------------
// The mirror (engine-free): the same mapping, exposed to the backend for
// the remote half of restore / purge / stats.
// ---------------------------------------------------------------------------

class S3DataFileMirror final : public clink::DataFileMirror {
public:
    S3DataFileMirror(Mapping mapping, std::shared_ptr<arrow::fs::S3FileSystem> s3)
        : map_(std::move(mapping)), s3_(std::move(s3)) {}

    void copy_dir(const std::string& src_dir, const std::string& dst_dir) override {
        for (const auto& info : list_infos_(src_dir)) {
            const auto dst = map_.object_dir(dst_dir) + "/" + info.base_name();
            auto st = s3_->CopyFile(info.path(), dst);
            if (!st.ok()) {
                throw std::runtime_error("forst remote-sst mirror copy failed: " + st.ToString());
            }
        }
    }

    void delete_dir(const std::string& dir) override {
        for (const auto& info : list_infos_(dir)) {
            (void)s3_->DeleteFile(info.path());  // best-effort, mirrors local remove_all
        }
    }

    [[nodiscard]] std::vector<std::string> list_dir(const std::string& dir) const override {
        std::vector<std::string> out;
        for (const auto& info : list_infos_(dir)) {
            out.push_back(info.base_name());
        }
        std::sort(out.begin(), out.end());
        return out;
    }

private:
    [[nodiscard]] std::vector<arrow::fs::FileInfo> list_infos_(const std::string& dir) const {
        arrow::fs::FileSelector sel;
        sel.base_dir = map_.object_dir(dir);
        sel.allow_not_found = true;
        sel.recursive = false;
        auto infos = s3_->GetFileInfo(sel);
        if (!infos.ok()) {
            throw std::runtime_error("forst remote-sst mirror list failed: " +
                                     infos.status().ToString());
        }
        std::vector<arrow::fs::FileInfo> files;
        for (auto& info : *infos) {
            if (info.type() == arrow::fs::FileType::File) {
                files.push_back(std::move(info));
            }
        }
        return files;
    }

    Mapping map_;
    std::shared_ptr<arrow::fs::S3FileSystem> s3_;
};

// ---------------------------------------------------------------------------
// Metadata store: cross-machine restore. persist() uploads the local cp
// dir's files (metadata only - the data files live remotely already) to
// the SAME object prefix as its data files; fetch() re-materialises them
// locally when the local dir is missing, so a fresh machine restores
// from the bucket alone (same URI, empty disk). Handles stay LOCAL paths
// (the LocalSnapshotStore convention), so the backend's restore flow -
// hard-link re-home + DataFileMirror copy - runs unchanged.
// ---------------------------------------------------------------------------

class S3SstMetadataStore final : public clink::SnapshotStore {
public:
    S3SstMetadataStore(Mapping mapping, std::shared_ptr<arrow::fs::S3FileSystem> s3)
        : map_(std::move(mapping)), s3_(std::move(s3)) {}

    std::string write_checkpoint_dir(const std::string& local_cp_path, CheckpointId id) override {
        (void)id;
        std::error_code ec;
        for (const auto& entry : fs::directory_iterator(local_cp_path, ec)) {
            if (!entry.is_regular_file()) {
                continue;
            }
            upload_file_(entry.path(),
                         map_.object_dir(local_cp_path) + "/" + entry.path().filename().string());
        }
        return local_cp_path;  // the handle stays the local path
    }

    std::string fetch_checkpoint_dir(const std::string& handle) override {
        if (fs::exists(handle)) {
            return handle;  // same-machine restart: nothing to do
        }
        // Fresh machine: re-materialise the metadata files from the
        // checkpoint's object prefix. Data files (*.sst) stay remote -
        // the DataFileMirror replicates them prefix-to-prefix and the
        // engine reads them through its filesystem.
        arrow::fs::FileSelector sel;
        sel.base_dir = map_.object_dir(handle);
        sel.allow_not_found = true;
        sel.recursive = false;
        auto infos = s3_->GetFileInfo(sel);
        if (!infos.ok()) {
            throw std::runtime_error("s3sst metadata fetch: list failed: " +
                                     infos.status().ToString());
        }
        bool any = false;
        for (const auto& info : *infos) {
            if (info.type() == arrow::fs::FileType::File && !has_sst_suffix(info.base_name())) {
                any = true;
                break;
            }
        }
        if (!any) {
            throw std::runtime_error(
                "s3sst metadata fetch: checkpoint found neither locally nor in object storage: " +
                handle);
        }
        std::error_code ec;
        fs::create_directories(handle, ec);
        for (const auto& info : *infos) {
            if (info.type() != arrow::fs::FileType::File || has_sst_suffix(info.base_name())) {
                continue;
            }
            download_file_(info.path(), fs::path(handle) / info.base_name());
        }
        return handle;
    }

    void delete_checkpoint(const std::string& local_cp_path, CheckpointId /*id*/) override {
        // Local half only - the backend's purge_checkpoint follows up
        // with DataFileMirror::delete_dir, which removes EVERY object
        // under the cp prefix (data files and the metadata uploaded
        // here alike).
        std::error_code ec;
        fs::remove_all(local_cp_path, ec);
    }

    // The upload is a slow durable write: route it through the backend's
    // capture/persist split so it runs on the snapshot worker, off the
    // barrier path.
    [[nodiscard]] bool defers_durable_write() const noexcept override { return true; }

    [[nodiscard]] std::string description() const override { return "s3sst-metadata"; }

private:
    void upload_file_(const fs::path& local, const std::string& object) {
        std::ifstream in(local, std::ios::binary);
        if (!in) {
            throw std::runtime_error("s3sst metadata upload: cannot read " + local.string());
        }
        auto out = s3_->OpenOutputStream(object);
        if (!out.ok()) {
            throw std::runtime_error("s3sst metadata upload: " + out.status().ToString());
        }
        char buf[64 * 1024];
        while (in.read(buf, sizeof(buf)) || in.gcount() > 0) {
            auto st = (*out)->Write(buf, static_cast<std::int64_t>(in.gcount()));
            if (!st.ok()) {
                throw std::runtime_error("s3sst metadata upload: " + st.ToString());
            }
        }
        auto st = (*out)->Close();
        if (!st.ok()) {
            throw std::runtime_error("s3sst metadata upload close: " + st.ToString());
        }
    }

    void download_file_(const std::string& object, const fs::path& local) {
        auto in = s3_->OpenInputStream(object);
        if (!in.ok()) {
            throw std::runtime_error("s3sst metadata download: " + in.status().ToString());
        }
        std::ofstream out(local, std::ios::binary | std::ios::trunc);
        if (!out) {
            throw std::runtime_error("s3sst metadata download: cannot write " + local.string());
        }
        char buf[64 * 1024];
        while (true) {
            auto read = (*in)->Read(sizeof(buf), buf);
            if (!read.ok()) {
                throw std::runtime_error("s3sst metadata download: " + read.status().ToString());
            }
            if (*read == 0) {
                break;
            }
            out.write(buf, static_cast<std::streamsize>(*read));
        }
    }

    Mapping map_;
    std::shared_ptr<arrow::fs::S3FileSystem> s3_;
};

Mapping make_mapping(const RemoteSstOptions& o) {
    Mapping m;
    m.bucket = o.bucket;
    m.key_prefix = o.key_prefix;
    while (!m.key_prefix.empty() && m.key_prefix.back() == '/') {
        m.key_prefix.pop_back();
    }
    m.local_root = fs::path(o.local_root).lexically_normal().string();
    return m;
}

std::shared_ptr<LocalSstCache> make_cache(const RemoteSstOptions& o, const Mapping& m) {
    if (o.sst_cache_bytes == 0 && o.sst_cache_dir.empty()) {
        return nullptr;
    }
    std::string dir = o.sst_cache_dir.empty() ? (m.local_root + ".sst-cache") : o.sst_cache_dir;
    const auto norm = fs::path(dir).lexically_normal().string();
    if (norm.size() > m.local_root.size() &&
        norm.compare(0, m.local_root.size() + 1, m.local_root + "/") == 0) {
        // A *.sst under the root would route straight back to S3 - the
        // cache would recurse into itself.
        throw std::runtime_error(
            "forst remote-sst: sst_cache dir must not be under the local "
            "root: " +
            norm);
    }
    const auto budget =
        o.sst_cache_bytes == 0 ? std::numeric_limits<std::uint64_t>::max() : o.sst_cache_bytes;
    return std::make_shared<LocalSstCache>(norm, budget);
}

}  // namespace

RemoteSstEnv make_remote_sst_env(const RemoteSstOptions& opts) {
    auto mapping = make_mapping(opts);
    auto s3 = make_arrow_s3(opts);
    auto cache = make_cache(opts, mapping);
    auto rfs =
        std::make_shared<RemoteSstFileSystem>(std::move(mapping), std::move(s3), std::move(cache));
    std::shared_ptr<forstdb::Env> env = forstdb::NewCompositeEnv(rfs);
    RemoteSstEnv out;
    out.env = env.get();
    out.holder = std::move(env);  // shared_ptr<void> keeps Env + FS alive
    return out;
}

std::shared_ptr<clink::DataFileMirror> make_s3_data_file_mirror(const RemoteSstOptions& opts) {
    return std::make_shared<S3DataFileMirror>(make_mapping(opts), make_arrow_s3(opts));
}

std::shared_ptr<clink::SnapshotStore> make_s3_metadata_store(const RemoteSstOptions& opts) {
    return std::make_shared<S3SstMetadataStore>(make_mapping(opts), make_arrow_s3(opts));
}

std::size_t sweep_stale_staging(const RemoteSstOptions& opts, const std::string& subtask_dir) {
    const auto mapping = make_mapping(opts);
    auto s3 = make_arrow_s3(opts);
    const auto subtask = fs::path(subtask_dir).filename().string();
    // One delimited listing of the root prefix: checkpoint dirs are
    // siblings of the subtask dir, so stale staging shows up as a
    // "directory" named "<subtask>.cp-<id>.tmp".
    arrow::fs::FileSelector sel;
    sel.base_dir = mapping.object_dir(fs::path(subtask_dir).parent_path().string());
    sel.allow_not_found = true;
    sel.recursive = false;
    auto infos = s3->GetFileInfo(sel);
    if (!infos.ok()) {
        throw std::runtime_error("forst remote-sst sweep: list failed: " +
                                 infos.status().ToString());
    }
    const std::string want_prefix = subtask + ".cp-";
    static constexpr std::string_view kTmp{".tmp"};
    std::size_t deleted = 0;
    for (const auto& info : *infos) {
        if (info.type() != arrow::fs::FileType::Directory) {
            continue;
        }
        const auto name = info.base_name();
        const bool is_staging = name.size() > want_prefix.size() + kTmp.size() &&
                                name.compare(0, want_prefix.size(), want_prefix) == 0 &&
                                name.compare(name.size() - kTmp.size(), kTmp.size(), kTmp) == 0;
        if (!is_staging) {
            continue;
        }
        arrow::fs::FileSelector inner;
        inner.base_dir = info.path();
        inner.allow_not_found = true;
        inner.recursive = true;
        auto objs = s3->GetFileInfo(inner);
        if (!objs.ok()) {
            continue;  // best-effort sweep
        }
        for (const auto& obj : *objs) {
            if (obj.type() == arrow::fs::FileType::File && s3->DeleteFile(obj.path()).ok()) {
                ++deleted;
            }
        }
    }
    return deleted;
}

}  // namespace clink::forst_s3
