#pragma once

// MultiObjectParquetSource<T> - reads every Parquet object under a prefix on an
// Arrow filesystem, the multi-object counterpart to the single-object Parquet
// sources (local ParquetSource, ParquetS3Source, ParquetGcsSource,
// ParquetAzureSource). It is filesystem-agnostic: it takes a factory that builds
// an arrow::fs::FileSystem (S3FileSystem, GcsFileSystem, AzureFileSystem or
// LocalFileSystem) and a prefix, so one implementation serves every object-store
// connector. The concrete filesystem is built lazily in open() (on the runner
// thread) via the factory, which keeps Arrow S3/GCS/Azure initialisation off the
// job-build path.
//
// Listing and splitting:
//   open() lists the prefix (recursively by default), keeps regular files whose
//   path ends with `suffix` (".parquet"), and sorts them for a deterministic
//   order. With parallelism > 1 the sorted list is sharded round-robin by index
//   (object i is read by subtask i % parallelism), so N subtasks read disjoint
//   subsets that together cover the whole prefix. A prefix that resolves to a
//   single file is read as a one-element list.
//
// Lifecycle:
//   open():     build the filesystem, list + shard the objects, open the first
//               assigned file's reader. Per-file the schema is validated against
//               the batcher and a mismatch throws (as the single-object sources
//               do).
//   produce():  emit the next RecordBatch, transparently advancing to the next
//               assigned file when the current one is exhausted. Returns false
//               once every assigned file is drained.
//   close():    release the current reader + input stream.
//
// Replay (#57): produce() emits exactly one batch per call and the assigned-file
// order is deterministic, so the emitted-batch count is a valid replay cursor.
// snapshot_offset persists it; restore_offset + open re-list, re-shard, and skip
// that many batches across the file sequence, so a restart resumes at the next
// unread batch without duplication or loss (bounded, same-parallelism restore).

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#ifndef CLINK_HAS_PARQUET
#error "MultiObjectParquetSource<T> requires CLINK_BUILD_ARROW=ON (Parquet ships alongside Arrow)."
#endif

#include <arrow/api.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/io/api.h>
#include <arrow/record_batch.h>
#include <parquet/arrow/reader.h>

#include "clink/core/arrow_batcher.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink {

template <typename T>
class MultiObjectParquetSource final : public Source<T> {
public:
    using FileSystemFactory = std::function<std::shared_ptr<arrow::fs::FileSystem>()>;

    struct Options {
        std::string prefix;              // directory / prefix to list, or a single object path
        bool recursive{true};            // descend into sub-directories when listing
        std::string suffix{".parquet"};  // only files whose path ends with this are read
        int subtask_idx{0};              // this subtask's index
        int parallelism{1};              // total subtasks sharing the prefix
        bool require_match{false};       // when true, throw at open() if no object matches
    };

    MultiObjectParquetSource(FileSystemFactory fs_factory,
                             Options opts,
                             ArrowBatcher<T> batcher,
                             std::string name = "multi_object_parquet_source")
        : fs_factory_(std::move(fs_factory)),
          opts_(std::move(opts)),
          batcher_(std::move(batcher)),
          name_(std::move(name)) {
        if (!fs_factory_) {
            throw std::invalid_argument("MultiObjectParquetSource: filesystem factory is required");
        }
        if (opts_.prefix.empty()) {
            throw std::invalid_argument("MultiObjectParquetSource: prefix is required");
        }
        if (opts_.parallelism < 1 || opts_.subtask_idx < 0 ||
            opts_.subtask_idx >= opts_.parallelism) {
            throw std::invalid_argument(
                "MultiObjectParquetSource: invalid subtask_idx/parallelism");
        }
        if (!batcher_.schema || !batcher_.parse) {
            throw std::invalid_argument(
                "MultiObjectParquetSource: ArrowBatcher must have schema and parse set");
        }
    }

    // Reading a fixed set of Parquet objects to their last row group is finite.
    [[nodiscard]] bool is_bounded() const noexcept override { return true; }

    void open() override {
        fs_ = fs_factory_();
        if (!fs_) {
            throw std::runtime_error("MultiObjectParquetSource: filesystem factory returned null");
        }
        files_ = list_assigned_files_();
        if (opts_.require_match && files_.empty()) {
            throw std::runtime_error("MultiObjectParquetSource: no objects matched prefix '" +
                                     opts_.prefix + "' (suffix '" + opts_.suffix + "')");
        }
        next_file_idx_ = 0;
        open_next_reader_();

        // #57 replay: skip the batches already emitted before the restored checkpoint,
        // advancing across the assigned-file sequence.
        for (std::uint64_t skipped = 0; skipped < batches_emitted_; ++skipped) {
            if (!next_batch_()) {
                break;  // fewer batches available than the restored offset; nothing left
            }
        }
    }

    bool produce(Emitter<T>& out) override {
        if (this->cancelled()) {
            return false;
        }
        auto rb = next_batch_();
        if (!rb) {
            return false;  // every assigned file drained
        }
        ++batches_emitted_;
        auto parsed = batcher_.parse(*rb);
        if (!parsed.has_value()) {
            throw std::runtime_error(
                "MultiObjectParquetSource: ArrowBatcher.parse returned nullopt");
        }
        if (!parsed->empty()) {
            out.emit_data(std::move(*parsed));
        }
        return true;
    }

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

    void close() override { close_current_(); }

    std::string name() const override { return name_; }

private:
    static constexpr const char* kOffsetKey_ = "__multi_parquet_source_offset__";

    // List the prefix, keep matching files, sort, and shard round-robin to this subtask.
    std::vector<std::string> list_assigned_files_() {
        std::vector<std::string> all;

        auto stat = fs_->GetFileInfo(opts_.prefix);
        if (stat.ok() && stat->type() == arrow::fs::FileType::File) {
            all.push_back(opts_.prefix);  // prefix is itself a single object
        } else {
            arrow::fs::FileSelector sel;
            sel.base_dir = opts_.prefix;
            sel.recursive = opts_.recursive;
            sel.allow_not_found = true;
            auto infos = fs_->GetFileInfo(sel);
            if (!infos.ok()) {
                throw std::runtime_error("MultiObjectParquetSource: list '" + opts_.prefix +
                                         "': " + infos.status().ToString());
            }
            for (const auto& fi : *infos) {
                if (fi.type() == arrow::fs::FileType::File && ends_with_(fi.path(), opts_.suffix)) {
                    all.push_back(fi.path());
                }
            }
        }
        std::sort(all.begin(), all.end());

        const auto par = static_cast<std::size_t>(opts_.parallelism);
        const auto me = static_cast<std::size_t>(opts_.subtask_idx);
        std::vector<std::string> mine;
        for (std::size_t i = 0; i < all.size(); ++i) {
            if (i % par == me) {
                mine.push_back(all[i]);
            }
        }
        return mine;
    }

    static bool ends_with_(const std::string& s, const std::string& suffix) {
        return suffix.empty() || s.ends_with(suffix);
    }

    void close_current_() {
        batch_reader_.reset();
        reader_.reset();
        if (in_) {
            (void)in_->Close();
            in_.reset();
        }
    }

    // Open the next assigned file's reader, validating its schema. Leaves
    // batch_reader_ null once the assigned files are exhausted.
    void open_next_reader_() {
        while (next_file_idx_ < files_.size()) {
            const std::string& path = files_[next_file_idx_++];
            auto in_result = fs_->OpenInputFile(path);
            if (!in_result.ok()) {
                throw std::runtime_error("MultiObjectParquetSource: OpenInputFile(" + path +
                                         "): " + in_result.status().ToString());
            }
            in_ = *in_result;

            auto reader_result = parquet::arrow::OpenFile(in_, arrow::default_memory_pool());
            if (!reader_result.ok()) {
                throw std::runtime_error("MultiObjectParquetSource: OpenFile(" + path +
                                         "): " + reader_result.status().ToString());
            }
            reader_ = std::move(*reader_result);

            std::shared_ptr<arrow::Schema> file_schema;
            if (auto s = reader_->GetSchema(&file_schema); !s.ok()) {
                throw std::runtime_error("MultiObjectParquetSource: GetSchema(" + path +
                                         "): " + s.ToString());
            }
            auto expected = batcher_.schema();
            if (!file_schema->Equals(*expected, /*check_metadata=*/false)) {
                throw std::runtime_error("MultiObjectParquetSource: schema mismatch in " + path +
                                         " - file has " + file_schema->ToString() +
                                         "; ArrowBatcher expects " + expected->ToString());
            }

            auto br_result = reader_->GetRecordBatchReader();
            if (!br_result.ok()) {
                throw std::runtime_error("MultiObjectParquetSource: GetRecordBatchReader(" + path +
                                         "): " + br_result.status().ToString());
            }
            batch_reader_ = std::move(*br_result);
            return;
        }
        batch_reader_.reset();  // no more files
    }

    // Next RecordBatch across the assigned-file sequence, or null when all drained.
    std::shared_ptr<arrow::RecordBatch> next_batch_() {
        while (batch_reader_) {
            std::shared_ptr<arrow::RecordBatch> rb;
            if (auto s = batch_reader_->ReadNext(&rb); !s.ok()) {
                throw std::runtime_error("MultiObjectParquetSource: ReadNext: " + s.ToString());
            }
            if (rb) {
                return rb;
            }
            close_current_();     // current file exhausted
            open_next_reader_();  // advance (sets batch_reader_ or leaves it null)
        }
        return nullptr;
    }

    FileSystemFactory fs_factory_;
    Options opts_;
    ArrowBatcher<T> batcher_;
    std::string name_;

    std::shared_ptr<arrow::fs::FileSystem> fs_;
    std::vector<std::string> files_;  // assigned to this subtask, in read order
    std::size_t next_file_idx_ = 0;
    std::shared_ptr<arrow::io::RandomAccessFile> in_;
    std::unique_ptr<parquet::arrow::FileReader> reader_;
    std::shared_ptr<arrow::RecordBatchReader> batch_reader_;
    std::uint64_t batches_emitted_ = 0;  // #57 replay cursor (next-batch index)
};

}  // namespace clink
