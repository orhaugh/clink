#pragma once

// WebHdfsFileSystem - the handful of arrow::fs::FileSystem operations a
// ParquetRollingSink needs, over the WebHDFS REST API: file status
// (GETFILESTATUS), a directory listing (LISTSTATUS), MKDIRS, DELETE, RENAME, and
// an output stream that uploads its bytes with the two-step CREATE on Close().
// Everything else returns NotImplemented; this is a transport for the rolling
// Parquet sink, not a general HDFS filesystem.
//
// A file being written through WebHDFS is visible under its name before the
// upload finishes, so type_name() is "webhdfs" and the rolling sink writes each
// part under a .inprogress name and renames it into place, as on a local disk.
// The output stream buffers one part in memory (WebHDFS has no incremental
// upload), bounded by a checkpoint interval.

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/io/api.h>

#include "clink/config/json.hpp"
#include "clink/connectors/webhdfs_parquet_sink.hpp"  // webhdfs_detail::build_webhdfs_path etc.
#include "clink/http_connector/http_request.hpp"

namespace clink {

class WebHdfsFileSystem final : public arrow::fs::FileSystem {
public:
    struct Options {
        std::string base_url;  // WebHDFS/HttpFS root, e.g. http://nn:9870
        std::optional<std::string> user;
        std::optional<std::string> delegation_token;
        std::optional<std::string> permission;
        bool verify_tls{true};
        int connect_timeout_ms{5000};
        int rw_timeout_ms{30000};
    };

    explicit WebHdfsFileSystem(Options opts) : opts_(std::move(opts)) {}

    std::string type_name() const override { return "webhdfs"; }

    bool Equals(const arrow::fs::FileSystem& other) const override { return &other == this; }

    arrow::Result<arrow::fs::FileInfo> GetFileInfo(const std::string& path) override {
        auto r = client_().get(webhdfs_detail::build_webhdfs_path(path, "GETFILESTATUS", auth_()));
        if (r.status == 0) {
            return arrow::Status::IOError("WebHDFS GETFILESTATUS ", path, ": ", r.error);
        }
        arrow::fs::FileInfo info(path, arrow::fs::FileType::NotFound);
        if (r.status == 404) {
            return info;
        }
        if (r.status < 200 || r.status >= 300) {
            return arrow::Status::IOError(
                "WebHDFS GETFILESTATUS ", path, ": HTTP ", r.status, ": ", r.body);
        }
        const auto root = clink::config::parse(r.body);
        const auto type = root.is_object() && root.contains("FileStatus")
                              ? root.at("FileStatus").string_or("type", "")
                              : std::string{};
        info.set_type(type == "DIRECTORY" ? arrow::fs::FileType::Directory
                                          : arrow::fs::FileType::File);
        return info;
    }

    arrow::Result<arrow::fs::FileInfoVector> GetFileInfo(
        const arrow::fs::FileSelector& select) override {
        auto r = client_().get(
            webhdfs_detail::build_webhdfs_path(select.base_dir, "LISTSTATUS", auth_()));
        if (r.status == 0) {
            return arrow::Status::IOError("WebHDFS LISTSTATUS ", select.base_dir, ": ", r.error);
        }
        arrow::fs::FileInfoVector out;
        if (r.status == 404 && select.allow_not_found) {
            return out;
        }
        if (r.status < 200 || r.status >= 300) {
            return arrow::Status::IOError(
                "WebHDFS LISTSTATUS ", select.base_dir, ": HTTP ", r.status, ": ", r.body);
        }
        const auto root = clink::config::parse(r.body);
        if (!root.is_object() || !root.contains("FileStatuses")) {
            return out;
        }
        const auto& statuses = root.at("FileStatuses");
        if (!statuses.is_object() || !statuses.contains("FileStatus") ||
            !statuses.at("FileStatus").is_array()) {
            return out;
        }
        std::string base = select.base_dir;
        while (base.size() > 1 && base.back() == '/') {
            base.pop_back();
        }
        for (const auto& entry : statuses.at("FileStatus").as_array()) {
            if (!entry.is_object()) {
                continue;
            }
            const auto suffix = entry.string_or("pathSuffix", "");
            if (suffix.empty()) {
                continue;
            }
            const auto type = entry.string_or("type", "");
            out.emplace_back(
                base + "/" + suffix,
                type == "DIRECTORY" ? arrow::fs::FileType::Directory : arrow::fs::FileType::File);
        }
        return out;
    }

    arrow::Status CreateDir(const std::string& path, bool /*recursive*/) override {
        // MKDIRS creates parents and is idempotent.
        auto r = client_().put(webhdfs_detail::build_webhdfs_path(path, "MKDIRS", auth_()),
                               "",
                               "application/octet-stream");
        if (r.status == 0) {
            return arrow::Status::IOError("WebHDFS MKDIRS ", path, ": ", r.error);
        }
        if (r.status < 200 || r.status >= 300) {
            return arrow::Status::IOError(
                "WebHDFS MKDIRS ", path, ": HTTP ", r.status, ": ", r.body);
        }
        return arrow::Status::OK();
    }

    arrow::Status DeleteFile(const std::string& path) override {
        auto r = client_().del(webhdfs_detail::build_webhdfs_path(path, "DELETE", auth_()));
        if (r.status == 0) {
            return arrow::Status::IOError("WebHDFS DELETE ", path, ": ", r.error);
        }
        // 404 (already gone) is fine: a delete is idempotent here.
        if ((r.status < 200 || r.status >= 300) && r.status != 404) {
            return arrow::Status::IOError(
                "WebHDFS DELETE ", path, ": HTTP ", r.status, ": ", r.body);
        }
        return arrow::Status::OK();
    }

    arrow::Status Move(const std::string& src, const std::string& dest) override {
        auto params = auth_();
        params.emplace_back("destination", dest);
        auto r = client_().put(webhdfs_detail::build_webhdfs_path(src, "RENAME", params),
                               "",
                               "application/octet-stream");
        if (r.status == 0) {
            return arrow::Status::IOError("WebHDFS RENAME ", src, ": ", r.error);
        }
        bool renamed = false;
        if (r.status >= 200 && r.status < 300) {
            const auto root = clink::config::parse(r.body);
            renamed = root.is_object() && root.bool_or("boolean", false);
        }
        if (!renamed) {
            return arrow::Status::IOError(
                "WebHDFS RENAME ", src, " -> ", dest, " failed: HTTP ", r.status, ": ", r.body);
        }
        return arrow::Status::OK();
    }

    arrow::Result<std::shared_ptr<arrow::io::OutputStream>> OpenOutputStream(
        const std::string& path,
        const std::shared_ptr<const arrow::KeyValueMetadata>& /*metadata*/) override {
        ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::io::BufferOutputStream::Create(1 << 16));
        return std::make_shared<UploadStream>(this, path, std::move(buffer));
    }

    // Not needed by the rolling sink.
    arrow::Status DeleteDir(const std::string&) override { return not_implemented_(); }
    arrow::Status DeleteDirContents(const std::string&, bool) override {
        return not_implemented_();
    }
    arrow::Status DeleteRootDirContents() override { return not_implemented_(); }
    arrow::Status CopyFile(const std::string&, const std::string&) override {
        return not_implemented_();
    }
    arrow::Result<std::shared_ptr<arrow::io::InputStream>> OpenInputStream(
        const std::string&) override {
        return not_implemented_();
    }
    arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>> OpenInputFile(
        const std::string&) override {
        return not_implemented_();
    }
    arrow::Result<std::shared_ptr<arrow::io::OutputStream>> OpenAppendStream(
        const std::string&, const std::shared_ptr<const arrow::KeyValueMetadata>&) override {
        return not_implemented_();
    }

    using arrow::fs::FileSystem::GetFileInfo;
    using arrow::fs::FileSystem::OpenAppendStream;
    using arrow::fs::FileSystem::OpenOutputStream;

private:
    // Buffers the file and uploads it with the two-step CREATE when closed; an
    // aborted stream uploads nothing.
    class UploadStream final : public arrow::io::OutputStream {
    public:
        UploadStream(WebHdfsFileSystem* fs,
                     std::string path,
                     std::shared_ptr<arrow::io::BufferOutputStream> buffer)
            : fs_(fs), path_(std::move(path)), buffer_(std::move(buffer)) {}

        arrow::Status Write(const void* data, int64_t nbytes) override {
            return buffer_->Write(data, nbytes);
        }
        arrow::Result<int64_t> Tell() const override { return buffer_->Tell(); }
        bool closed() const override { return closed_; }
        arrow::Status Abort() override {
            closed_ = true;
            buffer_.reset();
            return arrow::Status::OK();
        }
        arrow::Status Close() override {
            if (closed_) {
                return arrow::Status::OK();
            }
            closed_ = true;
            ARROW_ASSIGN_OR_RAISE(auto bytes, buffer_->Finish());
            buffer_.reset();
            return fs_->upload_(path_, *bytes);
        }

    private:
        WebHdfsFileSystem* fs_;
        std::string path_;
        std::shared_ptr<arrow::io::BufferOutputStream> buffer_;
        bool closed_{false};
    };

    static arrow::Status not_implemented_() {
        return arrow::Status::NotImplemented("WebHdfsFileSystem: not supported");
    }

    std::vector<std::pair<std::string, std::string>> auth_() const {
        std::vector<std::pair<std::string, std::string>> p;
        if (opts_.user) {
            p.emplace_back("user.name", *opts_.user);
        }
        if (opts_.delegation_token) {
            p.emplace_back("delegation", *opts_.delegation_token);
        }
        return p;
    }

    clink::http_connector::HttpRequest client_() const {
        return clink::http_connector::HttpRequest(webhdfs_detail::http_opts(
            opts_.base_url, opts_.verify_tls, opts_.connect_timeout_ms, opts_.rw_timeout_ms));
    }

    // The two-step CREATE: the namenode redirects to a datanode, which takes the bytes.
    arrow::Status upload_(const std::string& path, const arrow::Buffer& bytes) {
        auto params = auth_();
        params.emplace_back("overwrite", "true");
        if (opts_.permission) {
            params.emplace_back("permission", *opts_.permission);
        }
        auto r1 = client_().put(webhdfs_detail::build_webhdfs_path(path, "CREATE", params),
                                "",
                                "application/octet-stream");
        if (r1.status == 0) {
            return arrow::Status::IOError("WebHDFS CREATE ", path, ": ", r1.error);
        }
        if (r1.status < 300 || r1.status >= 400) {
            return arrow::Status::IOError("WebHDFS CREATE ",
                                          path,
                                          ": expected a 307 redirect to a datanode, got HTTP ",
                                          r1.status,
                                          ": ",
                                          r1.body);
        }
        const auto loc = r1.headers.find("location");
        if (loc == r1.headers.end()) {
            return arrow::Status::IOError("WebHDFS CREATE ", path, ": 307 without a Location");
        }
        const auto [dn_base, dn_rest] = webhdfs_detail::split_url(loc->second);
        clink::http_connector::HttpRequest dn(webhdfs_detail::http_opts(
            dn_base, opts_.verify_tls, opts_.connect_timeout_ms, opts_.rw_timeout_ms));
        std::string body(reinterpret_cast<const char*>(bytes.data()),
                         static_cast<std::size_t>(bytes.size()));
        auto r2 = dn.put(dn_rest, body, "application/octet-stream");
        if (r2.status == 0) {
            return arrow::Status::IOError("WebHDFS datanode PUT ", path, ": ", r2.error);
        }
        if (r2.status < 200 || r2.status >= 300) {
            return arrow::Status::IOError(
                "WebHDFS datanode PUT ", path, ": HTTP ", r2.status, ": ", r2.body);
        }
        return arrow::Status::OK();
    }

    Options opts_;
};

}  // namespace clink
