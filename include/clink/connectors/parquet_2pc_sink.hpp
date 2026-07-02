#pragma once

// ParquetSink2PC<T> - a two-phase-commit Parquet sink.
//
// Rides the generic CommittingSink base (verbs only; the base owns the 2PC
// choreography, handle persistence and recover-at-open). Same staging/ +
// committed/ protocol as FileSink2PC, but each pre-committed transaction is a
// COMPLETE, self-describing Parquet file (footer + Arrow schema), so a
// committed file is readable by any standard Parquet consumer. Two structural
// differences from the text 2PC sink:
//
//   1. A Parquet file cannot be appended incrementally across barriers - it
//      needs a single writer opened with the schema and Close()d to flush the
//      footer. So one writer spans one checkpoint interval: records since the
//      last barrier accumulate as row groups, and prepare_commit finalises them
//      into one valid Parquet file.
//
//   2. The interval's bytes accumulate in an in-memory Arrow buffer, not a temp
//      file on disk. prepare_commit writes that buffer to staging via
//      write_fsync_rename (fsync the bytes, atomic rename, fsync the dir), so a
//      committed file is durable across an OS/power crash, not merely a process
//      crash. A bonus: because nothing touches disk mid-interval, a crash before
//      a barrier leaves NO half-written staging file. Durability is gated by
//      CLINK_STATE_FSYNC (set to 0 to fall back to flush+rename).
//
// Layout under `output_dir`:
//   staging/   pre-committed sub<N>-<ckpt>.parquet (one per (subtask, ckpt))
//   committed/ finalized files; atomic rename from staging is the commit
//
// The committable is the staging file path; commit() derives the committed path
// from its basename.
//
// Exactly-once is per the engine's 2PC contract: a committed file appears iff
// its checkpoint completed globally, and (with fsync on) its bytes are on stable
// storage before the pre-commit is acked.
//
// Memory note: the sink holds one checkpoint interval of (compressed) Parquet
// output in memory before the barrier flushes it. That is the durability-for-
// memory trade the 2PC + fsync contract asks for, bounded by the checkpoint
// interval.

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#ifndef CLINK_HAS_PARQUET
#error "ParquetSink2PC<T> requires CLINK_BUILD_ARROW=ON (Parquet ships alongside Arrow)."
#endif

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/io/memory.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

#include "clink/connectors/committing_sink.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/state/durable_file_write.hpp"

namespace clink {

template <typename T>
class ParquetSink2PC final : public CommittingSink<T, std::string> {
public:
    ParquetSink2PC(std::filesystem::path output_dir,
                   ArrowBatcher<T> batcher,
                   std::uint32_t subtask_idx,
                   parquet::Compression::type compression = parquet::Compression::ZSTD,
                   std::string name = "parquet_2pc_sink")
        : CommittingSink<T, std::string>(subtask_idx),
          output_dir_(std::move(output_dir)),
          batcher_(std::move(batcher)),
          subtask_idx_(subtask_idx),
          compression_(compression),
          name_(std::move(name)) {
        if (!batcher_.schema || !batcher_.build) {
            throw std::invalid_argument(
                "ParquetSink2PC: ArrowBatcher must have both schema and build set");
        }
    }

    void on_open() override {
        std::error_code ec;
        std::filesystem::create_directories(staging_dir(), ec);
        std::filesystem::create_directories(committed_dir(), ec);
        this->recover_legacy_handles("_2pc_pending_");
    }

    void write(const Batch<T>& batch) override {
        if (batch.empty()) {
            return;
        }
        ensure_pending_open_();
        auto record_batch = batcher_.build(batch);
        if (!record_batch) {
            throw std::runtime_error("ParquetSink2PC: ArrowBatcher.build returned null");
        }
        if (auto s = writer_->WriteRecordBatch(*record_batch); !s.ok()) {
            throw std::runtime_error("ParquetSink2PC::write: WriteRecordBatch: " + s.ToString());
        }
    }

    // Finalise the interval writer and durably write the buffer to staging.
    // Opens the writer even if no records flowed this interval, so every
    // (subtask, ckpt) yields a uniform staging file for recovery to find.
    std::optional<std::string> prepare_commit(std::uint64_t checkpoint_id) override {
        ensure_pending_open_();
        auto buffer = finalize_pending_writer_();

        const auto target =
            staging_dir() / (sub_prefix_() + "-" + std::to_string(checkpoint_id) + ".parquet");
        const auto tmp = staging_dir() / (sub_prefix_() + "-pending.tmp");
        clink::state::detail::write_fsync_rename(target,
                                                 tmp,
                                                 reinterpret_cast<const std::byte*>(buffer->data()),
                                                 static_cast<std::size_t>(buffer->size()));
        return target.string();
    }

    bool commit(const std::string& staging_path) override {
        const std::filesystem::path src{staging_path};
        const std::filesystem::path dst = committed_dir() / src.filename();
        std::error_code ec;
        std::filesystem::rename(src, dst, ec);
        if (ec && ec != std::errc::no_such_file_or_directory) {
            throw std::runtime_error("ParquetSink2PC::commit: rename to committed failed: " +
                                     ec.message());
        }
        // The staged bytes are already fsync'd; make the publish durable too.
        if (clink::state::detail::fsync_enabled()) {
            clink::state::detail::fsync_directory_best_effort(committed_dir());
        }
        return true;
    }

    void abort(const std::string& staging_path) override {
        std::error_code ec;
        std::filesystem::remove(std::filesystem::path{staging_path}, ec);
        if (ec && ec != std::errc::no_such_file_or_directory) {
            throw std::runtime_error("ParquetSink2PC::abort: remove of staging file failed: " +
                                     ec.message());
        }
    }

    std::string serialize(const std::string& staging_path) const override { return staging_path; }
    std::string deserialize(std::string_view bytes) const override { return std::string(bytes); }

    void flush() override { abandon_pending_writer_(); }

    void close() override { abandon_pending_writer_(); }

    std::string name() const override { return name_; }

private:
    std::filesystem::path staging_dir() const { return output_dir_ / "staging"; }
    std::filesystem::path committed_dir() const { return output_dir_ / "committed"; }
    std::string sub_prefix_() const { return "sub" + std::to_string(subtask_idx_); }

    void ensure_pending_open_() {
        if (writer_) {
            return;
        }
        auto out_result = arrow::io::BufferOutputStream::Create(/*initial_capacity=*/1 << 16,
                                                                arrow::default_memory_pool());
        if (!out_result.ok()) {
            throw std::runtime_error("ParquetSink2PC: alloc buffer: " +
                                     out_result.status().ToString());
        }
        buf_out_ = *out_result;

        auto props = parquet::WriterProperties::Builder().compression(compression_)->build();
        auto arrow_props = parquet::ArrowWriterProperties::Builder().store_schema()->build();
        auto schema = batcher_.schema();
        auto writer_result = parquet::arrow::FileWriter::Open(
            *schema, arrow::default_memory_pool(), buf_out_, props, arrow_props);
        if (!writer_result.ok()) {
            throw std::runtime_error("ParquetSink2PC: open writer: " +
                                     writer_result.status().ToString());
        }
        writer_ = std::move(*writer_result);
    }

    // Close the writer to flush a complete Parquet footer, then take the
    // in-memory buffer holding the whole file.
    std::shared_ptr<arrow::Buffer> finalize_pending_writer_() {
        if (auto s = writer_->Close(); !s.ok()) {
            throw std::runtime_error("ParquetSink2PC: close writer: " + s.ToString());
        }
        writer_.reset();
        auto buf_result = buf_out_->Finish();
        buf_out_.reset();
        if (!buf_result.ok()) {
            throw std::runtime_error("ParquetSink2PC: finish buffer: " +
                                     buf_result.status().ToString());
        }
        return *buf_result;
    }

    // Drop an in-progress interval. Its bytes live only in the in-memory buffer
    // (never written to disk, never persisted as a handle), so dropping them is
    // the whole rollback - recovery can never commit them.
    void abandon_pending_writer_() {
        if (writer_) {
            (void)writer_->Close();
            writer_.reset();
        }
        buf_out_.reset();
    }

    std::filesystem::path output_dir_;
    ArrowBatcher<T> batcher_;
    std::uint32_t subtask_idx_;
    parquet::Compression::type compression_;
    std::string name_;
    std::shared_ptr<arrow::io::BufferOutputStream> buf_out_;
    std::unique_ptr<parquet::arrow::FileWriter> writer_;
};

}  // namespace clink
