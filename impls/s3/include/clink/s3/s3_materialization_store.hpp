#pragma once

// S3MaterializationStore - object-storage implementation of
// ExternalMaterializationStore (DISAGG-0, the remote-state beachhead).
//
// When a ChangelogStateBackend is constructed with this store, each
// materialization payload (the periodic full snapshot of the inner backend -
// the part that grows large) is written to an S3 object instead of being
// inlined into the checkpoint blob. The Snapshot blob then carries only the
// returned handle (the s3 object path) plus the small recent-log delta, so
// the incremental checkpoint stays small regardless of total state size.
// Restore reads the handle from the snapshot and fetches the payload back
// from S3. All the changelog machinery (log replay, materialize threshold,
// rescale-aware restore) is reused unchanged; only where the payload bytes
// live changes.
//
// Transport: Arrow's S3FileSystem, the same seam as the Parquet S3 connector
// (and the same process-wide init, so the two never double-initialise S3).
// Credentials resolve via the standard AWS chain; endpoint_override targets
// MinIO/localstack and allow_anonymous targets public buckets.
//
// Object layout: <bucket>/<prefix>/mat-<checkpoint_id>.bin. The handle is the
// full "<bucket>/<prefix>/mat-<id>.bin" path, which read()/erase() pass
// straight back to the filesystem.

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <arrow/filesystem/s3fs.h>
#include <arrow/io/api.h>

#include "clink/connectors/parquet_s3_sink.hpp"  // clink::detail::ensure_arrow_s3_initialised
#include "clink/core/types.hpp"
#include "clink/state/external_materialization_store.hpp"

namespace clink::s3 {

class S3MaterializationStore final : public clink::ExternalMaterializationStore {
public:
    struct Options {
        std::string bucket;                            // required
        std::string prefix;                            // object-key prefix (e.g. "job/0/mat")
        std::optional<std::string> region;             // explicit region
        std::optional<std::string> endpoint_override;  // MinIO / localstack
        bool allow_anonymous{false};                   // public-bucket access
    };

    explicit S3MaterializationStore(Options opts) : opts_(std::move(opts)) {
        if (opts_.bucket.empty()) {
            throw std::invalid_argument("S3MaterializationStore: bucket is required");
        }
    }

    std::string write(clink::CheckpointId id, std::span<const std::byte> bytes) override {
        auto fs = fs_();
        const std::string handle = object_path_(id);
        auto out_result = fs->OpenOutputStream(handle);
        if (!out_result.ok()) {
            throw std::runtime_error("S3MaterializationStore::write OpenOutputStream(" + handle +
                                     "): " + out_result.status().ToString());
        }
        auto out = *out_result;
        if (!bytes.empty()) {
            if (auto s = out->Write(bytes.data(), static_cast<std::int64_t>(bytes.size()));
                !s.ok()) {
                throw std::runtime_error("S3MaterializationStore::write Write(" + handle +
                                         "): " + s.ToString());
            }
        }
        if (auto s = out->Close(); !s.ok()) {
            throw std::runtime_error("S3MaterializationStore::write Close(" + handle +
                                     "): " + s.ToString());
        }
        return handle;
    }

    std::vector<std::byte> read(const std::string& handle) override {
        auto fs = fs_();
        auto file_result = fs->OpenInputFile(handle);
        if (!file_result.ok()) {
            throw std::runtime_error("S3MaterializationStore::read OpenInputFile(" + handle +
                                     "): " + file_result.status().ToString());
        }
        auto file = *file_result;
        auto size_result = file->GetSize();
        if (!size_result.ok()) {
            throw std::runtime_error("S3MaterializationStore::read GetSize(" + handle +
                                     "): " + size_result.status().ToString());
        }
        const std::int64_t size = *size_result;
        std::vector<std::byte> out(static_cast<std::size_t>(size));
        if (size > 0) {
            auto buf_result = file->ReadAt(0, size);
            if (!buf_result.ok()) {
                throw std::runtime_error("S3MaterializationStore::read ReadAt(" + handle +
                                         "): " + buf_result.status().ToString());
            }
            auto buffer = *buf_result;
            std::memcpy(out.data(), buffer->data(), static_cast<std::size_t>(buffer->size()));
        }
        return out;
    }

    void erase(const std::string& handle) override {
        // Best-effort: a failed delete (already gone, permissions) is ignored,
        // matching the FileMaterializationStore contract.
        if (auto fs_result = try_fs_(); fs_result) {
            (void)(*fs_result)->DeleteFile(handle);
        }
    }

    [[nodiscard]] std::string description() const override {
        return "s3://" + opts_.bucket + "/" + opts_.prefix;
    }

private:
    std::string object_path_(clink::CheckpointId id) const {
        std::string key = opts_.prefix;
        if (!key.empty() && key.back() != '/') {
            key.push_back('/');
        }
        key += "mat-" + std::to_string(id.value()) + ".bin";
        return opts_.bucket + "/" + key;
    }

    // Lazily build the S3FileSystem on first use so construction never touches
    // the network (matches the connector convention). Throws on a build error.
    std::shared_ptr<arrow::fs::S3FileSystem> fs_() {
        auto result = try_fs_();
        if (!result) {
            throw std::runtime_error("S3MaterializationStore: S3FileSystem::Make failed");
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
            s3_opts.scheme = "http";  // MinIO / localstack default
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
};

}  // namespace clink::s3
