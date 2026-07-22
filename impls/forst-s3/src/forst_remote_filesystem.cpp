#include "forst_remote_filesystem.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/s3fs.h>
#include <arrow/io/interfaces.h>

#include "clink/connectors/parquet_s3_sink.hpp"  // clink::detail::ensure_arrow_s3_initialised

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

    // "<bucket>/<key_prefix>/<relative>" - the path form arrow's
    // filesystem API takes.
    [[nodiscard]] std::string object_path(const std::string& fname) const {
        const auto rel =
            fs::path(fname).lexically_normal().lexically_relative(local_root).generic_string();
        return bucket + "/" + (key_prefix.empty() ? rel : key_prefix + "/" + rel);
    }

    // The object "directory" for a local dir path (no trailing slash).
    [[nodiscard]] std::string object_dir(const std::string& dir) const {
        const auto rel =
            fs::path(dir).lexically_normal().lexically_relative(local_root).generic_string();
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
    explicit S3WritableFile(std::shared_ptr<arrow::io::OutputStream> out) : out_(std::move(out)) {}

    ~S3WritableFile() override {
        // The engine closes files explicitly on success paths; this is
        // the error-path safety net (an abandoned multipart upload).
        if (!closed_) {
            (void)out_->Close();
        }
    }

    forstdb::IOStatus Append(const forstdb::Slice& data,
                             const forstdb::IOOptions& /*options*/,
                             forstdb::IODebugContext* /*dbg*/) override {
        auto st = out_->Write(data.data(), static_cast<std::int64_t>(data.size()));
        if (!st.ok()) {
            return to_io_status(st, "append");
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
        return to_io_status(out_->Close(), "close");
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
    uint64_t size_{0};
    bool closed_{false};
};

// ---------------------------------------------------------------------------
// The routing filesystem
// ---------------------------------------------------------------------------

class RemoteSstFileSystem final : public forstdb::FileSystemWrapper {
public:
    RemoteSstFileSystem(Mapping mapping, std::shared_ptr<arrow::fs::S3FileSystem> s3)
        : forstdb::FileSystemWrapper(forstdb::FileSystem::Default()),
          map_(std::move(mapping)),
          s3_(std::move(s3)) {}

    static const char* kClassName() { return "clink-forst-remote-sst"; }
    const char* Name() const override { return kClassName(); }

    forstdb::IOStatus NewSequentialFile(const std::string& fname,
                                        const forstdb::FileOptions& file_opts,
                                        std::unique_ptr<forstdb::FSSequentialFile>* result,
                                        forstdb::IODebugContext* dbg) override {
        if (!map_.routes(fname)) {
            return forstdb::FileSystemWrapper::NewSequentialFile(fname, file_opts, result, dbg);
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
        *result = std::make_unique<S3WritableFile>(*out);
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

}  // namespace

RemoteSstEnv make_remote_sst_env(const RemoteSstOptions& opts) {
    auto s3 = make_arrow_s3(opts);
    auto rfs = std::make_shared<RemoteSstFileSystem>(make_mapping(opts), std::move(s3));
    std::shared_ptr<forstdb::Env> env = forstdb::NewCompositeEnv(rfs);
    RemoteSstEnv out;
    out.env = env.get();
    out.holder = std::move(env);  // shared_ptr<void> keeps Env + FS alive
    return out;
}

std::shared_ptr<clink::DataFileMirror> make_s3_data_file_mirror(const RemoteSstOptions& opts) {
    return std::make_shared<S3DataFileMirror>(make_mapping(opts), make_arrow_s3(opts));
}

}  // namespace clink::forst_s3
