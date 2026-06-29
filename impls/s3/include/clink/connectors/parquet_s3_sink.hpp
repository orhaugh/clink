#pragma once

// ParquetS3Sink<T> - writes Parquet directly to an s3:// URL using
// Arrow's S3FileSystem as the transport. Same `ArrowBatcher<T>` seam
// as the local ParquetSink; the only difference is the output stream.
//
// Lifecycle:
//   open():     initialises Arrow S3 (idempotent across the process),
//               builds an S3FileSystem from the user-supplied options.
//               If `bucket_assigner` is unset, also eagerly opens the
//               output stream + parquet writer for the static key.
//   on_data():  builds an Arrow RecordBatch via the batcher's build(),
//               writes one row group per call. When `bucket_assigner`
//               is set, the batch is partitioned by the assigner's
//               per-record key and each partition is routed to its own
//               output stream (lazily opened on first write).
//   close():    finalises every open writer (one per active bucket),
//               closes the streams. The S3 multipart uploads complete
//               here.
//
// Credentials: by default Arrow resolves AWS creds via the standard
// chain (env vars AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, instance
// profile, ~/.aws/credentials, IAM role). The Options struct exposes
// `endpoint_override` for localstack/MinIO, `region` for explicit
// region selection, and `allow_anonymous` for public bucket reads.
//
// Bucket assignment: when `bucket_assigner` is set, each record's key
// is computed by the user callback (`BucketAssigner` analog).
// The sink maintains one in-flight FileWriter per active key, opened
// lazily on first write to that key and closed in `close()`. The
// canonical use is date / customer partitioning, e.g.
//   opts.bucket_assigner = [](const RawKafkaRecord& r) {
//       return std::format("year={}/month={}/customer={}",
//                          r.year, r.month, r.customer);
//   };
// No rolling policy in v1: each subtask opens at most one file per
// distinct key during its lifetime. Multi-stream rollover (size or
// time-based, à la RollingPolicy) is the next extension if
// production produces files too large to flush at shutdown.
//
// Without `bucket_assigner` (the historic shape) the sink writes a
// single file at `bucket/key` - backward-compatible with the M10
// IT and every existing typed-helper call site.
//
// Naming: when parallelism > 1, callers prepend / append a subtask
// suffix to `key` (or build it into the assigner's output) so each
// subtask writes to a distinct object.

#include <atomic>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

#include <arrow/api.h>
#include <arrow/filesystem/s3fs.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

#include "clink/connectors/arrow_s3_lifecycle.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink {

namespace detail {

// Process-wide Arrow S3 initialisation. Delegates to the single engine-wide owner of the
// Arrow/AWS-SDK S3 lifecycle (clink::connectors::ensure_arrow_s3_initialised), which inits
// S3 exactly once and registers the atexit FinalizeS3 the pinned Arrow 24 requires. Kept as
// a thin alias so the existing impls/s3 call sites do not all have to change.
inline void ensure_arrow_s3_initialised() {
    clink::connectors::ensure_arrow_s3_initialised();
}

}  // namespace detail

template <typename T>
class ParquetS3Sink final : public Sink<T> {
public:
    struct Options {
        std::string bucket;  // required
        std::string key;     // required when `bucket_assigner` is unset;
                             // used as the destination for every record.
                             // When `bucket_assigner` is set, this is
                             // ignored - the assigner provides per-record
                             // keys.
        std::optional<std::string> region;
        std::optional<std::string> endpoint_override;  // localstack / MinIO
        bool allow_anonymous{false};
        parquet::Compression::type compression{parquet::Compression::ZSTD};

        // Optional per-record bucket key assigner. When set, the sink
        // partitions each batch by the assigner's output and writes to
        // one S3 object per distinct key. Java parity: //
        // `FileSink.forBulkFormat(...).withBucketAssigner(...)` shape.
        //
        // The returned string is appended to `bucket` as the S3 object
        // key (no leading `/`); typical use is
        // "yyyy/MM/dd/customer=<c>/<file>.parquet". The assigner runs
        // on every record so it should be cheap; the sink caches one
        // writer per distinct returned string.
        //
        // Empty / unset = legacy single-object behavior (uses `key`).
        std::function<std::string(const T&)> bucket_assigner;
    };

    ParquetS3Sink(Options opts, ArrowBatcher<T> batcher, std::string name = "parquet_s3_sink")
        : opts_(std::move(opts)), batcher_(std::move(batcher)), name_(std::move(name)) {
        if (opts_.bucket.empty()) {
            throw std::invalid_argument("ParquetS3Sink: bucket is required");
        }
        if (!opts_.bucket_assigner && opts_.key.empty()) {
            throw std::invalid_argument(
                "ParquetS3Sink: key is required when no bucket_assigner is set");
        }
        if (!batcher_.schema || !batcher_.build) {
            throw std::invalid_argument(
                "ParquetS3Sink: ArrowBatcher must have schema and build set");
        }
    }

    void open() override {
        detail::ensure_arrow_s3_initialised();

        auto s3_opts = arrow::fs::S3Options::Defaults();
        if (opts_.region) {
            s3_opts.region = *opts_.region;
        }
        if (opts_.endpoint_override) {
            s3_opts.endpoint_override = *opts_.endpoint_override;
            s3_opts.scheme = "http";  // localstack/MinIO default
        }
        if (opts_.allow_anonymous) {
            s3_opts.ConfigureAnonymousCredentials();
        }

        auto fs_result = arrow::fs::S3FileSystem::Make(s3_opts);
        if (!fs_result.ok()) {
            throw std::runtime_error("ParquetS3Sink: S3FileSystem::Make: " +
                                     fs_result.status().ToString());
        }
        fs_ = *fs_result;

        // Static-key path: open the single writer eagerly so the
        // M10 IT shape (open() must connect) keeps working. When a
        // bucket_assigner is set, writers are opened lazily on first
        // record per key - `open()` does the FS init only.
        if (!opts_.bucket_assigner) {
            ensure_writer_for_key_(opts_.key);
        }
    }

    void on_data(const Batch<T>& batch) override {
        if (batch.empty()) {
            return;
        }
        if (!opts_.bucket_assigner) {
            // Single-writer fast path. Build one Arrow RecordBatch over
            // the entire input Batch and push to the only writer.
            write_batch_to_writer_(opts_.key, batch);
            return;
        }
        // Per-record bucket assignment: partition the batch by the
        // assigner's key, then push each partition to its own writer.
        std::unordered_map<std::string, Batch<T>> by_key;
        for (const auto& rec : batch) {
            const auto k = opts_.bucket_assigner(rec.value());
            auto& sub = by_key[k];
            // Preserve event_time when present.
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
        // Close + release EVERY writer/stream before reporting an error, so a failure on one
        // key does not strand the other keys' streams un-Closed (a partial object) or leave
        // writers_ populated for an unsafe retry. Capture the first error, finish the teardown,
        // then rethrow.
        std::string first_err;
        for (auto& [k, entry] : writers_) {
            if (entry.writer) {
                if (auto s = entry.writer->Close(); !s.ok() && first_err.empty()) {
                    first_err = "ParquetS3Sink: writer close (" + k + "): " + s.ToString();
                }
                entry.writer.reset();
            }
            if (entry.out) {
                if (auto s = entry.out->Close(); !s.ok() && first_err.empty()) {
                    first_err = "ParquetS3Sink: stream close (" + k + "): " + s.ToString();
                }
                entry.out.reset();
            }
        }
        writers_.clear();
        if (!first_err.empty()) {
            throw std::runtime_error(first_err);
        }
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
        auto out_result = fs_->OpenOutputStream(path);
        if (!out_result.ok()) {
            throw std::runtime_error("ParquetS3Sink: OpenOutputStream(" + path +
                                     "): " + out_result.status().ToString());
        }
        auto props = parquet::WriterProperties::Builder().compression(opts_.compression)->build();
        auto arrow_props = parquet::ArrowWriterProperties::Builder().store_schema()->build();
        auto schema = batcher_.schema();
        auto writer_result = parquet::arrow::FileWriter::Open(
            *schema, arrow::default_memory_pool(), *out_result, props, arrow_props);
        if (!writer_result.ok()) {
            throw std::runtime_error("ParquetS3Sink: FileWriter::Open(" + path +
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
            throw std::runtime_error("ParquetS3Sink: ArrowBatcher.build returned null");
        }
        if (auto s = entry.writer->WriteRecordBatch(*record_batch); !s.ok()) {
            throw std::runtime_error("ParquetS3Sink: WriteRecordBatch (" + key +
                                     "): " + s.ToString());
        }
    }

    Options opts_;
    ArrowBatcher<T> batcher_;
    std::string name_;
    std::shared_ptr<arrow::fs::S3FileSystem> fs_;
    std::unordered_map<std::string, WriterEntry> writers_;
};

}  // namespace clink
