#pragma once

// ParquetGcsSink<T> - writes Parquet to a gs:// bucket using Arrow's GcsFileSystem as the
// transport. The GCS analogue of ParquetS3Sink<T>: same ArrowBatcher<T> seam, same per-key
// writer model. Unlike S3, GCS needs no process-wide init/finalise (google-cloud-cpp's storage
// client is REST/curl-based with no background event-loop threads), and OpenSSL's atexit is
// already suppressed engine-wide (openssl_atexit_guard), so there is no exit-teardown hook here.
//
// Auth: anonymous (the fake-gcs-server emulator / public buckets), an explicit OAuth2 access
// token, or - by default - Application Default Credentials (GOOGLE_APPLICATION_CREDENTIALS /
// the ambient GCE/GKE/Cloud-Run identity). endpoint_override + scheme target an emulator.

#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

#include <arrow/api.h>
#include <arrow/filesystem/gcsfs.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

#include "clink/core/arrow_batcher.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink {

namespace gcs_detail {

// Build GcsOptions from the connector's auth/endpoint settings (shared by the sink + source).
inline arrow::fs::GcsOptions make_gcs_options(bool anonymous,
                                              const std::optional<std::string>& access_token,
                                              const std::optional<std::string>& endpoint_override,
                                              const std::optional<std::string>& scheme,
                                              const std::optional<std::string>& project_id,
                                              const std::optional<double>& retry_limit_seconds) {
    arrow::fs::GcsOptions opts;  // default ctor == Application Default Credentials
    if (anonymous) {
        opts = arrow::fs::GcsOptions::Anonymous();
    } else if (access_token) {
        // A static token: set a far-future expiry (the caller is responsible for token validity).
        opts = arrow::fs::GcsOptions::FromAccessToken(
            *access_token, std::chrono::system_clock::now() + std::chrono::hours(24));
    }
    if (endpoint_override) {
        opts.endpoint_override = *endpoint_override;
        opts.scheme = scheme.value_or("http");  // emulator default
    } else if (scheme) {
        opts.scheme = *scheme;
    }
    if (project_id) {
        opts.project_id = *project_id;
    }
    if (retry_limit_seconds) {
        // Arrow's default is 15 minutes; a streaming sink wants to fail fast on a dead endpoint.
        opts.retry_limit_seconds = *retry_limit_seconds;
    }
    return opts;
}

}  // namespace gcs_detail

template <typename T>
class ParquetGcsSink final : public Sink<T> {
public:
    struct Options {
        std::string bucket;  // required
        std::string key;     // required when `bucket_assigner` is unset
        bool anonymous{false};
        std::optional<std::string> access_token;
        std::optional<std::string> endpoint_override;  // fake-gcs-server / emulator
        std::optional<std::string> scheme;             // http for the emulator; else https
        std::optional<std::string> project_id;
        std::optional<double> retry_limit_seconds;  // cap retries (Arrow default = 15 min)
        parquet::Compression::type compression{parquet::Compression::ZSTD};
        // Optional per-record object-key assigner (Hive-style partitioning). Empty = single object.
        std::function<std::string(const T&)> bucket_assigner;
    };

    ParquetGcsSink(Options opts, ArrowBatcher<T> batcher, std::string name = "parquet_gcs_sink")
        : opts_(std::move(opts)), batcher_(std::move(batcher)), name_(std::move(name)) {
        if (opts_.bucket.empty()) {
            throw std::invalid_argument("ParquetGcsSink: bucket is required");
        }
        if (!opts_.bucket_assigner && opts_.key.empty()) {
            throw std::invalid_argument(
                "ParquetGcsSink: key is required when no bucket_assigner is set");
        }
        if (!batcher_.schema || !batcher_.build) {
            throw std::invalid_argument(
                "ParquetGcsSink: ArrowBatcher must have schema and build set");
        }
    }

    void open() override {
        auto gcs_opts = gcs_detail::make_gcs_options(opts_.anonymous,
                                                     opts_.access_token,
                                                     opts_.endpoint_override,
                                                     opts_.scheme,
                                                     opts_.project_id,
                                                     opts_.retry_limit_seconds);
        auto fs_result = arrow::fs::GcsFileSystem::Make(gcs_opts);
        if (!fs_result.ok()) {
            throw std::runtime_error("ParquetGcsSink: GcsFileSystem::Make: " +
                                     fs_result.status().ToString());
        }
        fs_ = *fs_result;
        if (!opts_.bucket_assigner) {
            ensure_writer_for_key_(opts_.key);
        }
    }

    void on_data(const Batch<T>& batch) override {
        if (batch.empty()) {
            return;
        }
        if (!opts_.bucket_assigner) {
            write_batch_to_writer_(opts_.key, batch);
            return;
        }
        std::unordered_map<std::string, Batch<T>> by_key;
        for (const auto& rec : batch) {
            const auto k = opts_.bucket_assigner(rec.value());
            auto& sub = by_key[k];
            if (rec.event_time()) {
                sub.emplace(rec.value(), *rec.event_time());
            } else {
                sub.emplace(rec.value());
            }
        }
        for (auto& [k, sub_batch] : by_key) {
            write_batch_to_writer_(k, sub_batch);
        }
    }

    void close() override {
        for (auto& [k, entry] : writers_) {
            if (entry.writer) {
                if (auto s = entry.writer->Close(); !s.ok()) {
                    throw std::runtime_error("ParquetGcsSink: writer close (" + k +
                                             "): " + s.ToString());
                }
                entry.writer.reset();
            }
            if (entry.out) {
                if (auto s = entry.out->Close(); !s.ok()) {
                    throw std::runtime_error("ParquetGcsSink: stream close (" + k +
                                             "): " + s.ToString());
                }
                entry.out.reset();
            }
        }
        writers_.clear();
    }

    std::string name() const override { return name_; }

private:
    struct WriterEntry {
        std::shared_ptr<arrow::io::OutputStream> out;
        std::unique_ptr<parquet::arrow::FileWriter> writer;
    };

    WriterEntry& ensure_writer_for_key_(const std::string& key) {
        auto it = writers_.find(key);
        if (it != writers_.end()) {
            return it->second;
        }
        const auto path = opts_.bucket + "/" + key;
        // GcsFileSystem::OpenOutputStream has no default for the metadata arg (unlike S3's).
        auto out_result = fs_->OpenOutputStream(path, /*metadata=*/{});
        if (!out_result.ok()) {
            throw std::runtime_error("ParquetGcsSink: OpenOutputStream(" + path +
                                     "): " + out_result.status().ToString());
        }
        auto props = parquet::WriterProperties::Builder().compression(opts_.compression)->build();
        auto arrow_props = parquet::ArrowWriterProperties::Builder().store_schema()->build();
        auto schema = batcher_.schema();
        auto writer_result = parquet::arrow::FileWriter::Open(
            *schema, arrow::default_memory_pool(), *out_result, props, arrow_props);
        if (!writer_result.ok()) {
            throw std::runtime_error("ParquetGcsSink: FileWriter::Open(" + path +
                                     "): " + writer_result.status().ToString());
        }
        WriterEntry entry;
        entry.out = *out_result;
        entry.writer = std::move(*writer_result);
        auto [ins, _] = writers_.emplace(key, std::move(entry));
        return ins->second;
    }

    void write_batch_to_writer_(const std::string& key, const Batch<T>& batch) {
        auto& entry = ensure_writer_for_key_(key);
        auto record_batch = batcher_.build(batch);
        if (!record_batch) {
            throw std::runtime_error("ParquetGcsSink: ArrowBatcher.build returned null");
        }
        if (auto s = entry.writer->WriteRecordBatch(*record_batch); !s.ok()) {
            throw std::runtime_error("ParquetGcsSink: WriteRecordBatch (" + key +
                                     "): " + s.ToString());
        }
    }

    Options opts_;
    ArrowBatcher<T> batcher_;
    std::string name_;
    std::shared_ptr<arrow::fs::GcsFileSystem> fs_;
    std::unordered_map<std::string, WriterEntry> writers_;
};

}  // namespace clink
