#pragma once

// ParquetGcsSource<T> - reads Parquet from a gs:// bucket via Arrow's GcsFileSystem. Symmetric
// counterpart to ParquetGcsSink<T> and a direct analogue of ParquetS3Source<T>. Single-object
// source (multi-object / Hive-partitioned dataset reads are the obvious follow-up).

#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

#include <arrow/api.h>
#include <arrow/filesystem/gcsfs.h>
#include <arrow/io/api.h>
#include <arrow/record_batch.h>
#include <parquet/arrow/reader.h>

#include "clink/connectors/parquet_gcs_sink.hpp"  // gcs_detail::make_gcs_options
#include "clink/core/arrow_batcher.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink {

template <typename T>
class ParquetGcsSource final : public Source<T> {
public:
    struct Options {
        std::string bucket;  // required
        std::string key;     // required
        bool anonymous{false};
        std::optional<std::string> access_token;
        std::optional<std::string> endpoint_override;
        std::optional<std::string> scheme;
        std::optional<std::string> project_id;
        std::optional<double> retry_limit_seconds;
    };

    ParquetGcsSource(Options opts, ArrowBatcher<T> batcher, std::string name = "parquet_gcs_source")
        : opts_(std::move(opts)), batcher_(std::move(batcher)), name_(std::move(name)) {
        if (opts_.bucket.empty()) {
            throw std::invalid_argument("ParquetGcsSource: bucket is required");
        }
        if (opts_.key.empty()) {
            throw std::invalid_argument("ParquetGcsSource: key is required");
        }
        if (!batcher_.schema || !batcher_.parse) {
            throw std::invalid_argument(
                "ParquetGcsSource: ArrowBatcher must have schema and parse set");
        }
    }

    // Reading a single Parquet object to its last row group is finite.
    [[nodiscard]] bool is_bounded() const noexcept override { return true; }

    void open() override {
        auto gcs_opts = gcs_detail::make_gcs_options(opts_.anonymous,
                                                     opts_.access_token,
                                                     opts_.endpoint_override,
                                                     opts_.scheme,
                                                     opts_.project_id,
                                                     opts_.retry_limit_seconds);
        auto fs_result = arrow::fs::GcsFileSystem::Make(gcs_opts);
        if (!fs_result.ok()) {
            throw std::runtime_error("ParquetGcsSource: GcsFileSystem::Make: " +
                                     fs_result.status().ToString());
        }
        fs_ = *fs_result;

        const auto path = opts_.bucket + "/" + opts_.key;
        auto in_result = fs_->OpenInputFile(path);
        if (!in_result.ok()) {
            throw std::runtime_error("ParquetGcsSource: OpenInputFile(" + path +
                                     "): " + in_result.status().ToString());
        }
        in_ = *in_result;

        auto reader_result = parquet::arrow::OpenFile(in_, arrow::default_memory_pool());
        if (!reader_result.ok()) {
            throw std::runtime_error("ParquetGcsSource: OpenFile: " +
                                     reader_result.status().ToString());
        }
        reader_ = std::move(*reader_result);

        std::shared_ptr<arrow::Schema> file_schema;
        if (auto s = reader_->GetSchema(&file_schema); !s.ok()) {
            throw std::runtime_error("ParquetGcsSource: GetSchema: " + s.ToString());
        }
        auto expected = batcher_.schema();
        if (!file_schema->Equals(*expected, /*check_metadata=*/false)) {
            throw std::runtime_error("ParquetGcsSource: schema mismatch - file has " +
                                     file_schema->ToString() + "; ArrowBatcher expects " +
                                     expected->ToString());
        }

        auto br_result = reader_->GetRecordBatchReader();
        if (!br_result.ok()) {
            throw std::runtime_error("ParquetGcsSource: GetRecordBatchReader: " +
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
            throw std::runtime_error("ParquetGcsSource: ReadNext: " + s.ToString());
        }
        if (!rb) {
            return false;
        }
        auto parsed = batcher_.parse(*rb);
        if (!parsed.has_value()) {
            throw std::runtime_error("ParquetGcsSource: ArrowBatcher.parse returned nullopt");
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
    std::shared_ptr<arrow::fs::GcsFileSystem> fs_;
    std::shared_ptr<arrow::io::RandomAccessFile> in_;
    std::unique_ptr<parquet::arrow::FileReader> reader_;
    std::shared_ptr<arrow::RecordBatchReader> batch_reader_;
};

}  // namespace clink
