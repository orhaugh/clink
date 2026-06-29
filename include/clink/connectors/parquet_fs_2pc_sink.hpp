#pragma once

// ParquetFsSink2PC<T> - a two-phase-commit Parquet sink over an Arrow filesystem.
//
// The object-store counterpart to the local ParquetSink2PC: same staging/ +
// committed/ protocol and the same exactly-once contract, but the I/O runs
// through an arrow::fs::FileSystem (S3FileSystem, GcsFileSystem, AzureFileSystem
// or LocalFileSystem) instead of std::filesystem, so one implementation gives
// every object-store Parquet connector exactly-once. The filesystem is built
// lazily in open() via a factory callback (keeping Arrow S3/GCS/Azure init on
// the runner thread), the same seam MultiObjectParquetSource uses.
//
// One Parquet file spans one checkpoint interval: records since the last barrier
// accumulate as row groups in an in-memory Arrow buffer; on_barrier finalises
// them into one complete, self-describing Parquet object.
//
// Layout under `base`:
//   <base>/staging/sub<N>-<ckpt>.parquet     pre-committed (one per subtask, ckpt)
//   <base>/committed/sub<N>-<ckpt>.parquet    finalised; the commit copies staging here
// Point a reader (e.g. MultiObjectParquetSource) at <base>/committed to read the
// exactly-once output.
//
// Lifecycle:
//   open()        - build the filesystem, ensure staging/ + committed/ exist, run
//                   recovery (commit any staging object still tracked in state).
//   on_data       - write the batch as a row group into the in-memory writer.
//   on_barrier(b) - finalise the writer and upload the buffer to the staging
//                   object (OpenOutputStream + Write + Close is atomic on object
//                   stores: the object appears whole or not at all). Record the
//                   staging key in operator state.
//   on_commit(id) - copy staging -> committed (streamed via OpenInputFile +
//                   OpenOutputStream, which every Arrow filesystem supports; the
//                   committed object appears atomically), delete staging, erase
//                   state. Idempotent: a missing staging object is a no-op.
//   on_abort(id)  - DeleteFile staging, erase state.
//   flush/close   - drop an in-progress (non-barriered) interval; its bytes were
//                   never staged or state-tracked, so there is nothing to commit.
//
// Exactly-once per the engine's 2PC contract: a committed object exists iff its
// checkpoint completed globally. Durability is the object store's (no fsync).
//
// Memory: one checkpoint interval of (compressed) Parquet output is held in
// memory before the barrier uploads it, bounded by the checkpoint interval.

#include <cstdint>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#ifndef CLINK_HAS_PARQUET
#error "ParquetFsSink2PC<T> requires CLINK_BUILD_ARROW=ON (Parquet ships alongside Arrow)."
#endif

#include <arrow/api.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

#include "clink/core/arrow_batcher.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/state_backend.hpp"

namespace clink {

template <typename T>
class ParquetFsSink2PC final : public Sink<T> {
public:
    using FileSystemFactory = std::function<std::shared_ptr<arrow::fs::FileSystem>()>;

    struct Options {
        std::string base;  // prefix under which staging/ and committed/ live, e.g. "bucket/path"
        int subtask_idx{0};
        parquet::Compression::type compression{parquet::Compression::ZSTD};
    };

    ParquetFsSink2PC(FileSystemFactory fs_factory,
                     Options opts,
                     ArrowBatcher<T> batcher,
                     std::string name = "parquet_fs_2pc_sink")
        : fs_factory_(std::move(fs_factory)),
          opts_(std::move(opts)),
          batcher_(std::move(batcher)),
          name_(std::move(name)) {
        if (!fs_factory_) {
            throw std::invalid_argument("ParquetFsSink2PC: filesystem factory is required");
        }
        if (opts_.base.empty()) {
            throw std::invalid_argument("ParquetFsSink2PC: base is required");
        }
        if (!batcher_.schema || !batcher_.build) {
            throw std::invalid_argument(
                "ParquetFsSink2PC: ArrowBatcher must have both schema and build set");
        }
    }

    void open() override {
        fs_ = fs_factory_();
        if (!fs_) {
            throw std::runtime_error("ParquetFsSink2PC: filesystem factory returned null");
        }
        // Object stores treat prefixes as implicit; LocalFileSystem needs the dirs.
        (void)fs_->CreateDir(staging_prefix_(), /*recursive=*/true);
        (void)fs_->CreateDir(committed_prefix_(), /*recursive=*/true);
        recover_pending_();
    }

    void on_data(const Batch<T>& batch) override {
        if (batch.empty()) {
            return;
        }
        ensure_writer_open_();
        auto record_batch = batcher_.build(batch);
        if (!record_batch) {
            throw std::runtime_error("ParquetFsSink2PC: ArrowBatcher.build returned null");
        }
        if (auto s = writer_->WriteRecordBatch(*record_batch); !s.ok()) {
            throw std::runtime_error("ParquetFsSink2PC::on_data: WriteRecordBatch: " +
                                     s.ToString());
        }
    }

    void on_barrier(CheckpointBarrier b) override {
        const auto ckpt = b.id().value();
        // Open the writer even with no records this interval, so every (subtask, ckpt)
        // yields a uniform staging object for recovery to find.
        ensure_writer_open_();
        auto buffer = finalize_writer_();

        const std::string staging_key = staging_key_(ckpt);
        auto out = fs_->OpenOutputStream(staging_key);
        if (!out.ok()) {
            throw std::runtime_error("ParquetFsSink2PC: OpenOutputStream(" + staging_key +
                                     "): " + out.status().ToString());
        }
        if (auto s = (*out)->Write(buffer->data(), buffer->size()); !s.ok()) {
            throw std::runtime_error("ParquetFsSink2PC: write staging(" + staging_key +
                                     "): " + s.ToString());
        }
        if (auto s = (*out)->Close(); !s.ok()) {
            throw std::runtime_error("ParquetFsSink2PC: close staging(" + staging_key +
                                     "): " + s.ToString());
        }
        write_pending_state_(ckpt, staging_key);
    }

    void on_commit(std::uint64_t checkpoint_id) override {
        auto* state = state_backend_();
        if (state == nullptr) {
            return;
        }
        const auto key = state_key_(checkpoint_id);
        auto stored = state->get(this->id(), key);
        if (!stored.has_value()) {
            return;  // already committed (idempotent)
        }
        const std::string staging_key(reinterpret_cast<const char*>(stored->data()),
                                      stored->size());
        commit_one_(staging_key, committed_key_(checkpoint_id));
        state->erase(this->id(), key);
    }

    void on_abort(std::uint64_t checkpoint_id) override {
        auto* state = state_backend_();
        if (state == nullptr) {
            return;
        }
        const auto key = state_key_(checkpoint_id);
        auto stored = state->get(this->id(), key);
        if (!stored.has_value()) {
            return;
        }
        const std::string staging_key(reinterpret_cast<const char*>(stored->data()),
                                      stored->size());
        delete_if_exists_(staging_key);
        state->erase(this->id(), key);
    }

    void flush() override { abandon_writer_(); }
    void close() override { abandon_writer_(); }

    std::string name() const override { return name_; }

private:
    std::string staging_prefix_() const { return opts_.base + "/staging"; }
    std::string committed_prefix_() const { return opts_.base + "/committed"; }
    std::string sub_prefix_() const { return "sub" + std::to_string(opts_.subtask_idx); }
    std::string staging_key_(std::uint64_t ckpt) const {
        return staging_prefix_() + "/" + sub_prefix_() + "-" + std::to_string(ckpt) + ".parquet";
    }
    std::string committed_key_(std::uint64_t ckpt) const {
        return committed_prefix_() + "/" + sub_prefix_() + "-" + std::to_string(ckpt) + ".parquet";
    }
    std::string state_key_(std::uint64_t ckpt) const {
        return "_2pc_pending_" + sub_prefix_() + "_" + std::to_string(ckpt);
    }

    void ensure_writer_open_() {
        if (writer_) {
            return;
        }
        auto out_result = arrow::io::BufferOutputStream::Create(/*initial_capacity=*/1 << 16,
                                                                arrow::default_memory_pool());
        if (!out_result.ok()) {
            throw std::runtime_error("ParquetFsSink2PC: alloc buffer: " +
                                     out_result.status().ToString());
        }
        buf_out_ = *out_result;
        auto props = parquet::WriterProperties::Builder().compression(opts_.compression)->build();
        auto arrow_props = parquet::ArrowWriterProperties::Builder().store_schema()->build();
        auto schema = batcher_.schema();
        auto writer_result = parquet::arrow::FileWriter::Open(
            *schema, arrow::default_memory_pool(), buf_out_, props, arrow_props);
        if (!writer_result.ok()) {
            throw std::runtime_error("ParquetFsSink2PC: open writer: " +
                                     writer_result.status().ToString());
        }
        writer_ = std::move(*writer_result);
    }

    std::shared_ptr<arrow::Buffer> finalize_writer_() {
        if (auto s = writer_->Close(); !s.ok()) {
            throw std::runtime_error("ParquetFsSink2PC: close writer: " + s.ToString());
        }
        writer_.reset();
        auto buf_result = buf_out_->Finish();
        buf_out_.reset();
        if (!buf_result.ok()) {
            throw std::runtime_error("ParquetFsSink2PC: finish buffer: " +
                                     buf_result.status().ToString());
        }
        return *buf_result;
    }

    void abandon_writer_() {
        if (writer_) {
            (void)writer_->Close();
            writer_.reset();
        }
        buf_out_.reset();
    }

    // Copy staging -> committed, then delete staging. The copy streams the staging bytes through
    // an input + output stream rather than FileSystem::CopyFile, because CopyFile is not
    // implemented uniformly across Arrow's object-store filesystems; OpenInputFile +
    // OpenOutputStream are. The committed object still appears atomically (object stores publish on
    // Close), and the staging file is interval-sized, so the extra round-trip is bounded.
    // Idempotent on replay: a missing staging object means the commit already ran.
    void commit_one_(const std::string& staging_key, const std::string& committed_key) {
        auto info = fs_->GetFileInfo(staging_key);
        if (info.ok() && info->type() == arrow::fs::FileType::NotFound) {
            return;  // already committed and cleaned up
        }
        auto in = fs_->OpenInputFile(staging_key);
        if (!in.ok()) {
            throw std::runtime_error("ParquetFsSink2PC: OpenInputFile(" + staging_key +
                                     "): " + in.status().ToString());
        }
        auto size = (*in)->GetSize();
        if (!size.ok()) {
            throw std::runtime_error("ParquetFsSink2PC: GetSize(" + staging_key +
                                     "): " + size.status().ToString());
        }
        auto buf = (*in)->ReadAt(0, *size);
        if (!buf.ok()) {
            throw std::runtime_error("ParquetFsSink2PC: read staging(" + staging_key +
                                     "): " + buf.status().ToString());
        }
        (void)(*in)->Close();

        auto out = fs_->OpenOutputStream(committed_key);
        if (!out.ok()) {
            throw std::runtime_error("ParquetFsSink2PC: OpenOutputStream(" + committed_key +
                                     "): " + out.status().ToString());
        }
        if (auto s = (*out)->Write((*buf)->data(), (*buf)->size()); !s.ok()) {
            throw std::runtime_error("ParquetFsSink2PC: write committed(" + committed_key +
                                     "): " + s.ToString());
        }
        if (auto s = (*out)->Close(); !s.ok()) {
            throw std::runtime_error("ParquetFsSink2PC: close committed(" + committed_key +
                                     "): " + s.ToString());
        }
        delete_if_exists_(staging_key);
    }

    void delete_if_exists_(const std::string& key) {
        auto info = fs_->GetFileInfo(key);
        if (info.ok() && info->type() == arrow::fs::FileType::NotFound) {
            return;
        }
        if (auto s = fs_->DeleteFile(key); !s.ok()) {
            // A concurrent/duplicate delete that already removed it is fine.
            auto recheck = fs_->GetFileInfo(key);
            if (recheck.ok() && recheck->type() == arrow::fs::FileType::NotFound) {
                return;
            }
            throw std::runtime_error("ParquetFsSink2PC: DeleteFile(" + key + "): " + s.ToString());
        }
    }

    void write_pending_state_(std::uint64_t ckpt, const std::string& staging_key) {
        auto* state = state_backend_();
        if (state == nullptr) {
            return;
        }
        state->put(
            this->id(), state_key_(ckpt), std::string_view{staging_key.data(), staging_key.size()});
    }

    // Recovery: commit every staging object still tracked in state (a crash between
    // pre-commit-ack and commit, or between copy and delete, re-runs idempotently).
    void recover_pending_() {
        auto* state = state_backend_();
        if (state == nullptr) {
            return;
        }
        const std::string prefix = "_2pc_pending_" + sub_prefix_() + "_";
        std::vector<std::pair<std::string, std::string>> to_commit;
        state->scan(this->id(), [&](StateBackend::KeyView k, StateBackend::ValueView v) {
            const std::string key{k};
            if (key.rfind(prefix, 0) != 0) {
                return;
            }
            to_commit.emplace_back(key, std::string{v});
        });
        for (const auto& [key, staging_key] : to_commit) {
            std::uint64_t ckpt = 0;
            try {
                ckpt = std::stoull(key.substr(prefix.size()));
            } catch (...) {
                continue;
            }
            commit_one_(staging_key, committed_key_(ckpt));
            state->erase(this->id(), key);
        }
    }

    StateBackend* state_backend_() const noexcept {
        return this->runtime() != nullptr ? this->runtime()->state_backend() : nullptr;
    }

    FileSystemFactory fs_factory_;
    Options opts_;
    ArrowBatcher<T> batcher_;
    std::string name_;
    std::shared_ptr<arrow::fs::FileSystem> fs_;
    std::shared_ptr<arrow::io::BufferOutputStream> buf_out_;
    std::unique_ptr<parquet::arrow::FileWriter> writer_;
};

}  // namespace clink
