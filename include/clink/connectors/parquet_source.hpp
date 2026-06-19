#pragma once

// ParquetSource<T> - reads a Parquet file and emits one Batch<T> per
// row group via the type's ArrowBatcher<T>.
//
// Lifecycle:
//   open():     opens the file and the parquet::arrow::FileReader. The
//               reader pre-fetches row-group metadata; record-batch
//               reads are streaming.
//   produce():  reads the next RecordBatch via the reader's
//               RecordBatchReader, parses it into a Batch<T> via the
//               batcher's parse(), emits. Returns false when the
//               reader exhausts the file.
//   close():    releases the reader + file.
//
// Design notes:
//   * Schema check: at open() the file's schema must match the
//     batcher's expected schema (modulo metadata). Mismatch throws
//     immediately - better to fail at startup than silently emit
//     garbage rows. This is how ParquetReader behaves too.
//   * No column pushdown / projection. Reading the full row group
//     is correct for v1; column pruning is a perf knob for later.
//   * Bounded source: returns false on EOF without emitting a
//     terminal barrier (the source-runner emits it for us).

#include <array>
#include <cstddef>
#include <cstring>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#ifndef CLINK_HAS_PARQUET
#error "ParquetSource<T> requires CLINK_BUILD_ARROW=ON (Parquet ships alongside Arrow)."
#endif

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <arrow/record_batch.h>
#include <parquet/arrow/reader.h>

#include "clink/core/arrow_batcher.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink {

template <typename T>
class ParquetSource final : public Source<T> {
public:
    ParquetSource(std::filesystem::path path,
                  ArrowBatcher<T> batcher,
                  std::string name = "parquet_source")
        : path_(std::move(path)), batcher_(std::move(batcher)), name_(std::move(name)) {
        if (!batcher_.schema || !batcher_.parse) {
            throw std::invalid_argument(
                "ParquetSource: ArrowBatcher must have both schema and parse set");
        }
    }

    // Reading a Parquet file to its last row group is a finite stream (BATCH-1).
    [[nodiscard]] bool is_bounded() const noexcept override { return true; }

    void open() override {
        auto in_result = arrow::io::ReadableFile::Open(path_.string());
        if (!in_result.ok()) {
            throw std::runtime_error("ParquetSource: open " + path_.string() + ": " +
                                     in_result.status().ToString());
        }
        in_ = *in_result;

        auto reader_result = parquet::arrow::OpenFile(in_, arrow::default_memory_pool());
        if (!reader_result.ok()) {
            throw std::runtime_error("ParquetSource: open reader: " +
                                     reader_result.status().ToString());
        }
        reader_ = std::move(*reader_result);

        std::shared_ptr<arrow::Schema> file_schema;
        if (auto s = reader_->GetSchema(&file_schema); !s.ok()) {
            throw std::runtime_error("ParquetSource: get schema: " + s.ToString());
        }
        auto expected = batcher_.schema();
        if (!file_schema->Equals(*expected, /*check_metadata=*/false)) {
            throw std::runtime_error("ParquetSource: schema mismatch - file has " +
                                     file_schema->ToString() + "; ArrowBatcher expects " +
                                     expected->ToString());
        }

        if (auto s = reader_->GetRecordBatchReader(&batch_reader_); !s.ok()) {
            throw std::runtime_error("ParquetSource: get RecordBatchReader: " + s.ToString());
        }
        // #57: source replay. RecordBatch boundaries are deterministic for a
        // given file, and produce() emits exactly one batch per call, so
        // resuming = re-open then discard the batches emitted before the
        // checkpoint. restore_offset (run before open) sets batches_emitted_.
        for (std::uint64_t skipped = 0; skipped < batches_emitted_; ++skipped) {
            std::shared_ptr<arrow::RecordBatch> rb;
            if (auto s = batch_reader_->ReadNext(&rb); !s.ok()) {
                throw std::runtime_error("ParquetSource: replay skip: " + s.ToString());
            }
            if (!rb) {
                break;  // file shorter than the restored offset; nothing left to skip
            }
        }
    }

    bool produce(Emitter<T>& out) override {
        if (this->cancelled() || !batch_reader_) {
            return false;
        }
        std::shared_ptr<arrow::RecordBatch> rb;
        if (auto s = batch_reader_->ReadNext(&rb); !s.ok()) {
            throw std::runtime_error("ParquetSource: ReadNext: " + s.ToString());
        }
        if (!rb) {
            return false;  // EOF
        }
        ++batches_emitted_;  // #57: count emitted batches for replay
        auto parsed = batcher_.parse(*rb);
        if (!parsed.has_value()) {
            throw std::runtime_error("ParquetSource: ArrowBatcher.parse returned nullopt");
        }
        if (!parsed->empty()) {
            out.emit_data(std::move(*parsed));
        }
        return true;
    }

    // #57: persist the count of RecordBatches emitted so far, so restart resumes
    // at the next unread batch. Runs between produce() calls on the runner
    // thread, so batches_emitted_ is on a clean batch boundary. Operator-state
    // slot (fixed key) - source state is whole-restored per subtask, never
    // narrowed by the rescale restore filter.
    void snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId /*ckpt*/) override {
        std::array<std::byte, 8> bytes{};
        for (int i = 0; i < 8; ++i) {
            bytes[static_cast<std::size_t>(i)] =
                static_cast<std::byte>((batches_emitted_ >> (i * 8)) & 0xFF);
        }
        backend.put_operator_state(
            op_id,
            StateBackend::KeyView{kOffsetKey_, std::strlen(kOffsetKey_)},
            StateBackend::ValueView{reinterpret_cast<const char*>(bytes.data()), bytes.size()});
    }

    bool restore_offset(StateBackend& backend, OperatorId op_id) override {
        auto v = backend.get_operator_state(
            op_id, StateBackend::KeyView{kOffsetKey_, std::strlen(kOffsetKey_)});
        if (!v.has_value() || v->size() < 8) {
            return false;
        }
        std::uint64_t restored = 0;
        for (int i = 0; i < 8; ++i) {
            restored |= static_cast<std::uint64_t>(static_cast<std::uint8_t>((*v)[i])) << (i * 8);
        }
        batches_emitted_ = restored;
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
    static constexpr const char* kOffsetKey_ = "__parquet_source_offset__";

    std::filesystem::path path_;
    ArrowBatcher<T> batcher_;
    std::string name_;
    std::shared_ptr<arrow::io::ReadableFile> in_;
    std::unique_ptr<parquet::arrow::FileReader> reader_;
    std::shared_ptr<arrow::RecordBatchReader> batch_reader_;
    std::uint64_t batches_emitted_ = 0;  // #57: replay cursor (next-batch index)
};

}  // namespace clink
