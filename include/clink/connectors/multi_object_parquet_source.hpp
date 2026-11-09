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
//               assigned file's reader. Per file, the batcher's columns are
//               resolved by name (same type) and only those are read, in the
//               batcher's order, as ParquetSource does; a missing or differently
//               typed column throws naming it.
//   produce():  emit the next RecordBatch, transparently advancing to the next
//               assigned file when the current one is exhausted. Returns false
//               once every assigned file is drained.
//   close():    release the current reader + input stream.
//
// Replay: produce() emits exactly one batch per call and the assigned-file order
// is deterministic, so the count of batches emitted is a replay position while
// the listing stays the same. The checkpoint holds that count with a hash of the
// whole listing (each matching object's path, size and modification time) and
// the subtask and parallelism. A restore lists the prefix again and skips that
// many batches only when the hash, the subtask and the parallelism all match;
// otherwise it reads every assigned object again and logs a warning. An object
// added, removed or rewritten moved the count and changed which subtask read
// which object, so a restore by the count alone skipped batches. A checkpoint
// from an earlier release holds the count alone, with nothing to compare, so a
// restore from it reads every assigned object again; a form neither release
// wrote is refused. Rows are repeated rather than skipped: at least once, and
// exactly once at the source over an unchanged listing.

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
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

#include "clink/connectors/parquet_source.hpp"  // detail::ParquetBatchesInSchema
#include "clink/core/arrow_batcher.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/log_buffer.hpp"

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

        // Skip the batches a restored checkpoint covered, only over the listing
        // it was taken over, by the same subtask of the same parallelism.
        std::uint64_t skip = 0;
        if (restored_) {
            const bool same =
                restored_listing_.has_value() && *restored_listing_ == listing_hash_ &&
                restored_subtask_ == static_cast<std::uint32_t>(opts_.subtask_idx) &&
                restored_parallelism_ == static_cast<std::uint32_t>(opts_.parallelism);
            if (same) {
                skip = restored_batches_;
            } else if (restored_batches_ > 0) {
                clink::log::warn(
                    "source.parquet",
                    name_ + ": the checkpoint covers " + std::to_string(restored_batches_) +
                        " batches of a listing of '" + opts_.prefix +
                        "' that differs from this one, or was taken by another subtask or "
                        "parallelism, or by an earlier release, so every assigned object is "
                        "read again rather than resumed by count");
            }
        }
        batches_emitted_ = 0;
        while (batches_emitted_ < skip && next_batch_()) {
            ++batches_emitted_;
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

    // The checkpoint's form: a version byte, then little-endian the subtask
    // and the parallelism (4 bytes each), the listing's hash and the count of
    // batches emitted (8 bytes each). An earlier release wrote the count alone,
    // in 8 bytes.
    void snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId /*ckpt*/) override {
        std::array<char, kOffsetBytes_> bytes{};
        bytes[0] = static_cast<char>(kOffsetVersion_);
        // Before open() lists the prefix, the position stands as restored.
        const bool listed = fs_ != nullptr;
        put_le_(bytes.data() + 1,
                listed ? static_cast<std::uint32_t>(opts_.subtask_idx) : restored_subtask_,
                4);
        put_le_(bytes.data() + 5,
                listed ? static_cast<std::uint32_t>(opts_.parallelism) : restored_parallelism_,
                4);
        put_le_(bytes.data() + 9, listed ? listing_hash_ : restored_listing_.value_or(0), 8);
        put_le_(bytes.data() + 17, listed ? batches_emitted_ : restored_batches_, 8);
        backend.put_operator_state(op_id,
                                   StateBackend::KeyView{kOffsetKey_, std::strlen(kOffsetKey_)},
                                   StateBackend::ValueView{bytes.data(), bytes.size()});
    }

    bool restore_offset(StateBackend& backend, OperatorId op_id) override {
        auto v = backend.get_operator_state(
            op_id, StateBackend::KeyView{kOffsetKey_, std::strlen(kOffsetKey_)});
        if (!v.has_value()) {
            return false;
        }
        const auto* data = v->data();
        if (v->size() == kOffsetBytes_ &&
            std::to_integer<std::uint8_t>(data[0]) == kOffsetVersion_) {
            restored_subtask_ = static_cast<std::uint32_t>(get_le_(data + 1, 4));
            restored_parallelism_ = static_cast<std::uint32_t>(get_le_(data + 5, 4));
            restored_listing_ = get_le_(data + 9, 8);
            restored_batches_ = get_le_(data + 17, 8);
        } else if (v->size() == 8) {
            restored_listing_.reset();
            restored_batches_ = get_le_(data, 8);
        } else {
            throw std::runtime_error(name_ +
                                     ": the checkpoint's position is in a form this release does "
                                     "not read (" +
                                     std::to_string(v->size()) + " bytes)");
        }
        restored_ = true;
        return true;
    }

    void close() override { close_current_(); }

    std::string name() const override { return name_; }

private:
    static constexpr const char* kOffsetKey_ = "__multi_parquet_source_offset__";
    static constexpr std::uint8_t kOffsetVersion_ = 2;
    static constexpr std::size_t kOffsetBytes_ = 25;
    static constexpr std::uint64_t kFnvOffset_ = 14695981039346656037ULL;
    static constexpr std::uint64_t kFnvPrime_ = 1099511628211ULL;

    static void put_le_(char* out, std::uint64_t v, int n) {
        for (int i = 0; i < n; ++i) {
            out[i] = static_cast<char>((v >> (i * 8)) & 0xFF);
        }
    }

    static std::uint64_t get_le_(const std::byte* in, int n) {
        std::uint64_t v = 0;
        for (int i = 0; i < n; ++i) {
            v |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(in[i])) << (i * 8);
        }
        return v;
    }

    // FNV-1a over `n` bytes, then a separator, so neighbouring fields hash apart.
    static std::uint64_t mix_(std::uint64_t h, const void* data, std::size_t n) {
        const auto* b = static_cast<const unsigned char*>(data);
        for (std::size_t i = 0; i < n; ++i) {
            h ^= b[i];
            h *= kFnvPrime_;
        }
        h ^= 0xFF;
        return h * kFnvPrime_;
    }

    // List the prefix, keep matching files, sort, and shard round-robin to this
    // subtask. The whole listing, before sharding, is hashed for the restore.
    std::vector<std::string> list_assigned_files_() {
        std::vector<arrow::fs::FileInfo> all;

        auto stat = fs_->GetFileInfo(opts_.prefix);
        if (stat.ok() && stat->type() == arrow::fs::FileType::File) {
            all.push_back(*stat);  // prefix is itself a single object
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
                    all.push_back(fi);
                }
            }
        }
        std::sort(all.begin(), all.end(), [](const auto& a, const auto& b) {
            return a.path() < b.path();
        });

        listing_hash_ = kFnvOffset_;
        for (const auto& fi : all) {
            const auto& path = fi.path();
            listing_hash_ = mix_(listing_hash_, path.data(), path.size());
            const auto size = static_cast<std::int64_t>(fi.size());
            listing_hash_ = mix_(listing_hash_, &size, sizeof size);
            const auto mtime = static_cast<std::int64_t>(fi.mtime().time_since_epoch().count());
            listing_hash_ = mix_(listing_hash_, &mtime, sizeof mtime);
        }

        const auto par = static_cast<std::size_t>(opts_.parallelism);
        const auto me = static_cast<std::size_t>(opts_.subtask_idx);
        std::vector<std::string> mine;
        for (std::size_t i = 0; i < all.size(); ++i) {
            if (i % par == me) {
                mine.push_back(all[i].path());
            }
        }
        return mine;
    }

    static bool ends_with_(const std::string& s, const std::string& suffix) {
        return suffix.empty() || s.ends_with(suffix);
    }

    void close_current_() {
        batches_.reset();
        reader_.reset();
        if (in_) {
            (void)in_->Close();
            in_.reset();
        }
    }

    // Open the next assigned file's reader, validating its schema. Leaves
    // batches_ empty once the assigned files are exhausted.
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

            // Per file, as ParquetSource reads one: by name, in the batcher's order.
            batches_.emplace(*reader_, batcher_.schema(), "MultiObjectParquetSource", path);
            return;
        }
        batches_.reset();  // no more files
    }

    // Next RecordBatch across the assigned-file sequence, or null when all drained.
    std::shared_ptr<arrow::RecordBatch> next_batch_() {
        while (batches_) {
            if (auto rb = batches_->next()) {
                return rb;
            }
            close_current_();     // current file exhausted
            open_next_reader_();  // advance (sets batches_ or leaves it empty)
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
    std::optional<detail::ParquetBatchesInSchema> batches_;  // the current file, from reader_
    std::uint64_t batches_emitted_ = 0;                      // batches emitted, read or skipped
    std::uint64_t listing_hash_ = 0;                         // the whole listing open() read
    // What restore_offset() read. restored_listing_ is empty for an earlier
    // release's count, which names no listing.
    bool restored_ = false;
    std::uint32_t restored_subtask_ = 0;
    std::uint32_t restored_parallelism_ = 0;
    std::optional<std::uint64_t> restored_listing_;
    std::uint64_t restored_batches_ = 0;
};

}  // namespace clink
