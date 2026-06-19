#pragma once

// ParquetS3Source<T> - reads Parquet from an s3:// URL using Arrow's
// S3FileSystem as the transport. Symmetric counterpart to
// ParquetS3Sink<T>.
//
// Lifecycle:
//   open():     initialises Arrow S3 (idempotent), builds an
//               S3FileSystem from options, opens an input file at
//               bucket/key, opens the parquet::arrow::FileReader.
//               Validates the file's schema against the batcher's
//               expected schema and throws on mismatch.
//   produce():  reads the next RecordBatch, parses to Batch<T> via
//               the batcher, emits. Returns false when the reader
//               exhausts the file.
//   close():    releases the reader + input stream.
//
// Single-file source. Multi-file / Hive-partitioned dataset support
// is the obvious follow-up.

#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

#include <arrow/api.h>
#include <arrow/filesystem/s3fs.h>
#include <arrow/io/api.h>
#include <arrow/record_batch.h>
#include <parquet/arrow/reader.h>

#include "clink/connectors/parquet_s3_sink.hpp"  // detail::ensure_arrow_s3_initialised
#include "clink/core/arrow_batcher.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink {

template <typename T>
class ParquetS3Source final : public Source<T> {
public:
    struct Options {
        std::string bucket;  // required
        std::string key;     // required
        std::optional<std::string> region;
        std::optional<std::string> endpoint_override;
        bool allow_anonymous{false};
    };

    ParquetS3Source(Options opts, ArrowBatcher<T> batcher, std::string name = "parquet_s3_source")
        : opts_(std::move(opts)), batcher_(std::move(batcher)), name_(std::move(name)) {
        if (opts_.bucket.empty()) {
            throw std::invalid_argument("ParquetS3Source: bucket is required");
        }
        if (opts_.key.empty()) {
            throw std::invalid_argument("ParquetS3Source: key is required");
        }
        if (!batcher_.schema || !batcher_.parse) {
            throw std::invalid_argument(
                "ParquetS3Source: ArrowBatcher must have schema and parse set");
        }
    }

    // Reading a single Parquet object to its last row group is finite (BATCH-1).
    [[nodiscard]] bool is_bounded() const noexcept override { return true; }

    void open() override {
        detail::ensure_arrow_s3_initialised();

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
            throw std::runtime_error("ParquetS3Source: S3FileSystem::Make: " +
                                     fs_result.status().ToString());
        }
        fs_ = *fs_result;

        const auto path = opts_.bucket + "/" + opts_.key;
        auto in_result = fs_->OpenInputFile(path);
        if (!in_result.ok()) {
            throw std::runtime_error("ParquetS3Source: OpenInputFile(" + path +
                                     "): " + in_result.status().ToString());
        }
        in_ = *in_result;

        auto reader_result = parquet::arrow::OpenFile(in_, arrow::default_memory_pool());
        if (!reader_result.ok()) {
            throw std::runtime_error("ParquetS3Source: OpenFile: " +
                                     reader_result.status().ToString());
        }
        reader_ = std::move(*reader_result);

        std::shared_ptr<arrow::Schema> file_schema;
        if (auto s = reader_->GetSchema(&file_schema); !s.ok()) {
            throw std::runtime_error("ParquetS3Source: GetSchema: " + s.ToString());
        }
        auto expected = batcher_.schema();
        if (!file_schema->Equals(*expected, /*check_metadata=*/false)) {
            throw std::runtime_error("ParquetS3Source: schema mismatch - file has " +
                                     file_schema->ToString() + "; ArrowBatcher expects " +
                                     expected->ToString());
        }

        auto br_result = reader_->GetRecordBatchReader();
        if (!br_result.ok()) {
            throw std::runtime_error("ParquetS3Source: GetRecordBatchReader: " +
                                     br_result.status().ToString());
        }
        batch_reader_ = std::move(*br_result);
    }

    bool produce(Emitter<T>& out) override {
        if (this->cancelled() || !batch_reader_) {
            return false;
        }
        std::shared_ptr<arrow::RecordBatch> rb;
        if (auto s = batch_reader_->ReadNext(&rb); !s.ok()) {
            throw std::runtime_error("ParquetS3Source: ReadNext: " + s.ToString());
        }
        if (!rb) {
            return false;
        }
        auto parsed = batcher_.parse(*rb);
        if (!parsed.has_value()) {
            throw std::runtime_error("ParquetS3Source: ArrowBatcher.parse returned nullopt");
        }
        if (!parsed->empty()) {
            out.emit_data(std::move(*parsed));
        }
        return true;
    }

    void close() override {
        batch_reader_.reset();
        reader_.reset();
        if (in_) {
            (void)in_->Close();
            in_.reset();
        }
    }

    std::string name() const override { return name_; }

private:
    Options opts_;
    ArrowBatcher<T> batcher_;
    std::string name_;
    std::shared_ptr<arrow::fs::S3FileSystem> fs_;
    std::shared_ptr<arrow::io::RandomAccessFile> in_;
    std::unique_ptr<parquet::arrow::FileReader> reader_;
    std::shared_ptr<arrow::RecordBatchReader> batch_reader_;
};

}  // namespace clink
