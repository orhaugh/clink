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
//   * Schema contract at open(): detail::ParquetBatchesInSchema below,
//     the rule every Parquet source reads a file by. An exact
//     file/batcher schema match reads every column. Otherwise the
//     batcher's columns are resolved by name (types must match exactly)
//     and ONLY those are read - Parquet column pruning, driven for SQL by
//     the planner's projected_columns hint. A batcher column missing
//     from the file throws at open(), naming the column, except the
//     engine's event-time column.
//   * Bounded source: returns false on EOF without emitting a
//     terminal barrier (the source-runner emits it for us).

#include <array>
#include <cstddef>
#include <cstring>
#include <filesystem>
#include <memory>
#include <numeric>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

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

namespace detail {

// An open Parquet file's record batches, read in an ArrowBatcher's schema. Every Parquet source
// reads a file by this rule. An exact file/batcher schema match reads every column. Otherwise the
// batcher's columns are resolved by name, with the same type, and only those are read, in the
// batcher's order: a table declared narrower than a file, or with its columns in another order,
// reads it, and a narrowed query skips the unread columns. A batcher column the file lacks, or
// holds as another type, is refused naming it, except the engine's event-time column: a file
// another tool wrote has none, and its rows read as having no event time.
//
// The FileReader must outlive this object. `source` and `file` name the read in every error.
class ParquetBatchesInSchema {
public:
    ParquetBatchesInSchema(parquet::arrow::FileReader& reader,
                           std::shared_ptr<arrow::Schema> schema,
                           std::string source,
                           const std::string& file)
        : schema_(std::move(schema)), source_(std::move(source)) {
        std::shared_ptr<arrow::Schema> file_schema;
        if (auto s = reader.GetSchema(&file_schema); !s.ok()) {
            throw std::runtime_error(source_ + ": get schema of " + file + ": " + s.ToString());
        }
        if (file_schema->Equals(*schema_, /*check_metadata=*/false)) {
            auto all = reader.GetRecordBatchReader();
            if (!all.ok()) {
                throw std::runtime_error(source_ + ": read " + file + ": " +
                                         all.status().ToString());
            }
            batches_ = std::move(*all);
            return;
        }
        std::vector<int> indices;
        indices.reserve(static_cast<std::size_t>(schema_->num_fields()));
        bool event_time_absent = false;
        for (const auto& field : schema_->fields()) {
            const int idx = file_schema->GetFieldIndex(field->name());
            if (idx < 0 && is_event_time_field(*field)) {
                event_time_absent = true;
                continue;
            }
            if (idx < 0) {
                throw std::runtime_error(source_ + ": " + file + " has no column '" +
                                         field->name() +
                                         "' (file schema: " + file_schema->ToString() + ")");
            }
            if (!file_schema->field(idx)->type()->Equals(*field->type())) {
                throw std::runtime_error(source_ + ": column '" + field->name() + "' is " +
                                         file_schema->field(idx)->type()->ToString() + " in " +
                                         file + " but " + field->type()->ToString() +
                                         " in the declared schema");
            }
            indices.push_back(idx);
        }
        std::vector<int> row_groups(static_cast<std::size_t>(reader.num_row_groups()));
        std::iota(row_groups.begin(), row_groups.end(), 0);
        auto projected = reader.GetRecordBatchReader(row_groups, indices);
        if (!projected.ok()) {
            throw std::runtime_error(source_ + ": projected read of " + file + ": " +
                                     projected.status().ToString());
        }
        batches_ = std::move(*projected);
        // The projected reader yields columns in file order; remap to the batcher's order when
        // they differ (-1 marks the absent event-time column, filled with nulls).
        const auto got = batches_->schema();
        if (event_time_absent || !got->Equals(*schema_, /*check_metadata=*/false)) {
            order_.reserve(static_cast<std::size_t>(schema_->num_fields()));
            for (const auto& field : schema_->fields()) {
                const int idx = got->GetFieldIndex(field->name());
                if (idx < 0 && !(event_time_absent && is_event_time_field(*field))) {
                    throw std::runtime_error(source_ + ": the projected read of " + file +
                                             " lost column '" + field->name() + "'");
                }
                order_.push_back(idx);
            }
        }
    }

    // The next batch in the batcher's schema, or null at the end of the file.
    std::shared_ptr<arrow::RecordBatch> next() {
        std::shared_ptr<arrow::RecordBatch> rb;
        if (auto s = batches_->ReadNext(&rb); !s.ok()) {
            throw std::runtime_error(source_ + ": ReadNext: " + s.ToString());
        }
        if (!rb || order_.empty()) {
            return rb;
        }
        auto ordered = batch_in_schema_order(*rb, order_, schema_);
        if (!ordered.ok()) {
            throw std::runtime_error(source_ + ": column reorder: " + ordered.status().ToString());
        }
        return std::move(*ordered);
    }

private:
    std::shared_ptr<arrow::Schema> schema_;
    std::string source_;
    std::shared_ptr<arrow::RecordBatchReader> batches_;
    std::vector<int> order_;  // empty when a read batch is already in the batcher's order
};

}  // namespace detail

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

        batches_.emplace(*reader_, batcher_.schema(), "ParquetSource", path_.string());
        // #57: source replay. RecordBatch boundaries are deterministic for a
        // given file, and produce() emits exactly one batch per call, so
        // resuming = re-open then discard the batches emitted before the
        // checkpoint. restore_offset (run before open) sets batches_emitted_.
        for (std::uint64_t skipped = 0; skipped < batches_emitted_; ++skipped) {
            if (!batches_->next()) {
                break;  // file shorter than the restored offset; nothing left to skip
            }
        }
    }

    bool produce(Emitter<T>& out) override {
        if (this->cancelled() || !batches_) {
            return false;
        }
        auto rb = batches_->next();
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
        batches_.reset();
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
    std::optional<detail::ParquetBatchesInSchema> batches_;  // reads from reader_
    std::uint64_t batches_emitted_ = 0;  // #57: replay cursor (next-batch index)
};

}  // namespace clink
