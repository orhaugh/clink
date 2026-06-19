#pragma once

// ParquetSink<T> - writes batches as a single Parquet file using the
// type's ArrowBatcher<T> for schema and row encoding.
//
// Lifecycle:
//   open():     opens the output file and the parquet::arrow::FileWriter
//               with the batcher's schema; ZSTD compression by default.
//   on_data():  converts the Batch<T> to an arrow::RecordBatch via the
//               batcher's build(), writes it as one row group.
//   close():    finalises the file (writes footer, metadata).
//
// Design notes:
//   * One file per sink subtask. Rotation / time-based partitioning is
//     a deliberate v2 - the canonical  FileSink also starts with
//     one-part-file-per-subtask before adding rolling policies.
//   * Default compression is ZSTD (Arrow's recommended default for
//     general workloads - better than Snappy on size, comparable on
//     speed). Configurable via the ctor.
//   * The sink is NOT 2PC-style; a crash mid-write produces a partial
//     Parquet file. A FileParquet2PCSink would mirror file_2pc_sink's
//     staging/ + atomic rename - left for the connector-level 2PC pass.

#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#ifndef CLINK_HAS_PARQUET
#error "ParquetSink<T> requires CLINK_BUILD_ARROW=ON (Parquet ships alongside Arrow)."
#endif

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

#include "clink/core/arrow_batcher.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink {

template <typename T>
class ParquetSink final : public Sink<T> {
public:
    ParquetSink(std::filesystem::path path,
                ArrowBatcher<T> batcher,
                parquet::Compression::type compression = parquet::Compression::ZSTD,
                std::string name = "parquet_sink")
        : path_(std::move(path)),
          batcher_(std::move(batcher)),
          compression_(compression),
          name_(std::move(name)) {
        if (!batcher_.schema || !batcher_.build) {
            throw std::invalid_argument(
                "ParquetSink: ArrowBatcher must have both schema and build set");
        }
    }

    void open() override {
        auto out_result = arrow::io::FileOutputStream::Open(path_.string());
        if (!out_result.ok()) {
            throw std::runtime_error("ParquetSink: open " + path_.string() + ": " +
                                     out_result.status().ToString());
        }
        out_ = *out_result;

        auto props = parquet::WriterProperties::Builder().compression(compression_)->build();
        auto arrow_props = parquet::ArrowWriterProperties::Builder().store_schema()->build();

        auto schema = batcher_.schema();
        auto writer_result = parquet::arrow::FileWriter::Open(
            *schema, arrow::default_memory_pool(), out_, props, arrow_props);
        if (!writer_result.ok()) {
            throw std::runtime_error("ParquetSink: open writer: " +
                                     writer_result.status().ToString());
        }
        writer_ = std::move(*writer_result);
    }

    void on_data(const Batch<T>& batch) override {
        if (batch.empty()) {
            return;
        }
        auto record_batch = batcher_.build(batch);
        if (!record_batch) {
            throw std::runtime_error("ParquetSink: ArrowBatcher.build returned null");
        }
        // WriteRecordBatch produces one row group per call. For very
        // small Batch<T>s this fragments the file; the operator chain
        // upstream already batches records, so one row group per
        // logical batch is the right granularity.
        if (auto s = writer_->WriteRecordBatch(*record_batch); !s.ok()) {
            throw std::runtime_error("ParquetSink: WriteRecordBatch: " + s.ToString());
        }
    }

    void close() override {
        if (writer_) {
            if (auto s = writer_->Close(); !s.ok()) {
                throw std::runtime_error("ParquetSink: close writer: " + s.ToString());
            }
            writer_.reset();
        }
        if (out_) {
            if (auto s = out_->Close(); !s.ok()) {
                throw std::runtime_error("ParquetSink: close file: " + s.ToString());
            }
            out_.reset();
        }
    }

    std::string name() const override { return name_; }

private:
    std::filesystem::path path_;
    ArrowBatcher<T> batcher_;
    parquet::Compression::type compression_;
    std::string name_;
    std::shared_ptr<arrow::io::FileOutputStream> out_;
    std::unique_ptr<parquet::arrow::FileWriter> writer_;
};

}  // namespace clink
