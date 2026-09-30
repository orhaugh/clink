#pragma once

// ParquetRollingSink<T> - an at-least-once Parquet sink that writes a directory
// of complete part files, one per subtask per checkpoint interval.
//
// A Parquet file cannot be appended to, and its footer is written only when it
// is closed. A sink that writes one file per subtask therefore leaves nothing
// readable when its process is killed, and a restored run can only start that
// file again from empty: every row written before the restore point is lost.
// This sink closes a part at each checkpoint barrier instead, so everything up
// to the last barrier is already in complete files when a crash lands. A
// restored run keeps them and writes new parts beside them.
//
// Layout under `dir` (a local directory, or a bucket prefix on an object store):
//   sub<N>-<run>-<seq>.parquet   subtask N's rows in one checkpoint interval
// <run> is a token unique to one open of the sink (time-ordered, so later runs
// sort after earlier ones) and <seq> numbers its parts in order. No part a
// later run writes can replace one an earlier run wrote. Read the directory as
// a dataset: every file ending in .parquet is a complete Parquet file.
//
// Delivery is at-least-once. A restored run keeps every part already written,
// including parts for checkpoints past the restore point, whose rows the
// sources then replay; dropping them instead would lose the rows of a source
// that cannot replay. delivery_guarantee='exactly_once' (ParquetSink2PC /
// ParquetFsSink2PC) is the path without duplicates.
//
// A run that starts from empty state (no restore point) replaces the previous
// run's output, as a truncating file sink does: each subtask removes its own
// parts at open, and subtask 0 also removes the parts of subtasks at or above
// this run's parallelism, which no subtask of this run would otherwise clear.
// Only names matching the part pattern are touched.
//
// Where a file is visible under its name while it is being written (a local
// disk, WebHDFS) a part is written to <name>.inprogress and renamed into place
// when it is complete, so a kill mid-write never leaves a torn .parquet file;
// leftover .inprogress files are removed at the next open. An object store
// publishes an object only when its upload completes, so parts stream to their
// final key. Either way nothing is buffered beyond what the
// Parquet writer holds for its current row group.

#include <array>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include <arrow/api.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

#ifndef CLINK_HAS_PARQUET
#error "ParquetRollingSink<T> requires CLINK_BUILD_ARROW=ON (Parquet ships alongside Arrow)."
#endif

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/connectors/multi_object_parquet_source.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/runtime_context.hpp"

namespace clink {

template <typename T>
class ParquetRollingSink final : public Sink<T> {
public:
    using FileSystemFactory = std::function<std::shared_ptr<arrow::fs::FileSystem>()>;

    struct Options {
        std::string dir;  // local directory, or "bucket/prefix" on an object store
        std::uint32_t subtask_idx{0};
        std::uint32_t parallelism{1};
        parquet::Compression::type compression{parquet::Compression::ZSTD};
    };

    ParquetRollingSink(FileSystemFactory fs_factory,
                       Options opts,
                       ArrowBatcher<T> batcher,
                       std::string name = "parquet_rolling_sink")
        : fs_factory_(std::move(fs_factory)),
          opts_(std::move(opts)),
          batcher_(std::move(batcher)),
          name_(std::move(name)) {
        if (!fs_factory_) {
            throw std::invalid_argument("ParquetRollingSink: filesystem factory is required");
        }
        if (opts_.dir.empty()) {
            throw std::invalid_argument("ParquetRollingSink: dir is required");
        }
        while (opts_.dir.size() > 1 && opts_.dir.back() == '/') {
            opts_.dir.pop_back();
        }
        if (!batcher_.schema || !batcher_.build) {
            throw std::invalid_argument(
                "ParquetRollingSink: ArrowBatcher must have both schema and build set");
        }
    }

    // A sink destroyed without close() (a teardown that never reached it)
    // abandons its unfinished part, as a kill would: an object store's upload is
    // aborted, so nothing is published, and a local .inprogress file is removed.
    // Its rows are after the last barrier, so a restore replays them.
    ~ParquetRollingSink() override {
        try {
            if (writer_) {
                (void)writer_->Close();
                writer_.reset();
            }
            if (out_) {
                (void)out_->Abort();
                out_.reset();
                if (rename_into_place_ && fs_) {
                    (void)fs_->DeleteFile(final_path_ + ".inprogress");
                }
            }
        } catch (...) {  // NOLINT(bugprone-empty-catch): a destructor must not throw
        }
    }

    ParquetRollingSink(const ParquetRollingSink&) = delete;
    ParquetRollingSink& operator=(const ParquetRollingSink&) = delete;

    // The name of subtask `sub`'s part number `seq` in run `run`.
    [[nodiscard]] static std::string part_name(std::uint32_t sub,
                                               std::string_view run,
                                               std::uint64_t seq) {
        std::string digits = std::to_string(seq);
        if (digits.size() < 6) {
            digits.insert(0, 6 - digits.size(), '0');
        }
        return "sub" + std::to_string(sub) + "-" + std::string(run) + "-" + digits + ".parquet";
    }

    // The subtask index a file name belongs to, if it is one of this sink's
    // parts or an in-progress part; nullopt for any other file.
    [[nodiscard]] static std::optional<std::uint32_t> part_subtask(std::string_view basename) {
        if (basename.rfind("sub", 0) != 0) {
            return std::nullopt;
        }
        const bool finished = ends_with_(basename, ".parquet");
        const bool in_progress = ends_with_(basename, ".parquet.inprogress");
        if (!finished && !in_progress) {
            return std::nullopt;
        }
        std::size_t i = 3;
        std::uint64_t sub = 0;
        while (i < basename.size() && basename[i] >= '0' && basename[i] <= '9') {
            sub = (sub * 10) + static_cast<std::uint64_t>(basename[i] - '0');
            ++i;
        }
        if (i == 3 || i >= basename.size() || basename[i] != '-' || sub > UINT32_MAX) {
            return std::nullopt;
        }
        return static_cast<std::uint32_t>(sub);
    }

    void open() override {
        fs_ = fs_factory_();
        if (!fs_) {
            throw std::runtime_error("ParquetRollingSink: filesystem factory returned null");
        }
        const auto type = fs_->type_name();
        rename_into_place_ = type == "local" || type == "webhdfs";
        // Object stores treat prefixes as implicit; a directory must exist.
        if (auto s = fs_->CreateDir(opts_.dir, /*recursive=*/true); !s.ok() && rename_into_place_) {
            throw std::runtime_error("ParquetRollingSink: create " + opts_.dir + ": " +
                                     s.ToString());
        }
        const bool restored =
            this->runtime() != nullptr && this->runtime()->restore_from_checkpoint_id() > 0;
        clear_parts_(/*finished_too=*/!restored);
        run_ = make_run_token_();
        next_seq_ = 0;
    }

    void on_data(const Batch<T>& batch) override {
        if (batch.empty()) {
            return;
        }
        auto record_batch = batcher_.build(batch);
        if (!record_batch) {
            throw std::runtime_error("ParquetRollingSink: ArrowBatcher.build returned null");
        }
        ensure_part_open_();
        if (auto s = writer_->WriteRecordBatch(*record_batch); !s.ok()) {
            throw std::runtime_error("ParquetRollingSink: WriteRecordBatch: " + s.ToString());
        }
    }

    // The rows received before this barrier become one complete part.
    void on_barrier(CheckpointBarrier /*b*/) override { finish_part_(); }

    // End of input: the rows after the last barrier become the final part.
    void flush() override { finish_part_(); }

    // A close without a clean end of input (a cancel, a failure elsewhere in the
    // job) keeps what was written: at-least-once, since a restore also replays
    // the rows after the last barrier.
    void close() override { finish_part_(); }

    std::string name() const override { return name_; }

private:
    static bool ends_with_(std::string_view s, std::string_view suffix) {
        return s.size() >= suffix.size() && s.substr(s.size() - suffix.size()) == suffix;
    }

    static std::string basename_(const std::string& path) {
        const auto slash = path.rfind('/');
        return slash == std::string::npos ? path : path.substr(slash + 1);
    }

    // Microseconds since the epoch in fixed-width hex, so tokens sort in time
    // order, then 16 random bits against two opens in the same microsecond.
    static std::string make_run_token_() {
        const auto now =
            static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                           std::chrono::system_clock::now().time_since_epoch())
                                           .count());
        std::random_device rd;
        const auto salt = static_cast<std::uint64_t>(rd() & 0xFFFFU);
        std::string token;
        for (const auto [value, width] :
             {std::pair<std::uint64_t, int>{now, 14}, std::pair<std::uint64_t, int>{salt, 4}}) {
            std::array<char, 20> buf{};
            const auto [end, ec] = std::to_chars(buf.data(), buf.data() + buf.size(), value, 16);
            (void)ec;
            std::string hex(buf.data(), end);
            if (static_cast<int>(hex.size()) < width) {
                hex.insert(0, static_cast<std::size_t>(width) - hex.size(), '0');
            }
            token += hex;
        }
        return token;
    }

    // Removes this subtask's in-progress parts (always) and finished parts (when
    // the run starts from empty state). Subtask 0 does the same for subtasks at
    // or above this run's parallelism.
    void clear_parts_(bool finished_too) {
        arrow::fs::FileSelector selector;
        selector.base_dir = opts_.dir;
        selector.allow_not_found = true;
        selector.recursive = false;
        auto listed = fs_->GetFileInfo(selector);
        if (!listed.ok()) {
            throw std::runtime_error("ParquetRollingSink: list " + opts_.dir + ": " +
                                     listed.status().ToString());
        }
        for (const auto& info : *listed) {
            if (info.type() != arrow::fs::FileType::File) {
                continue;
            }
            const auto base = basename_(info.path());
            const auto sub = part_subtask(base);
            if (!sub.has_value()) {
                continue;
            }
            const bool mine =
                *sub == opts_.subtask_idx || (opts_.subtask_idx == 0 && *sub >= opts_.parallelism);
            const bool in_progress = ends_with_(base, ".inprogress");
            if (mine && (in_progress || finished_too)) {
                if (auto s = fs_->DeleteFile(info.path()); !s.ok()) {
                    throw std::runtime_error("ParquetRollingSink: delete " + info.path() + ": " +
                                             s.ToString());
                }
            }
        }
    }

    void ensure_part_open_() {
        if (writer_) {
            return;
        }
        final_path_ = opts_.dir + "/" + part_name(opts_.subtask_idx, run_, next_seq_++);
        const auto path = rename_into_place_ ? final_path_ + ".inprogress" : final_path_;
        auto out = fs_->OpenOutputStream(path);
        if (!out.ok()) {
            throw std::runtime_error("ParquetRollingSink: open " + path + ": " +
                                     out.status().ToString());
        }
        out_ = *out;
        auto props = parquet::WriterProperties::Builder().compression(opts_.compression)->build();
        auto arrow_props = parquet::ArrowWriterProperties::Builder().store_schema()->build();
        auto writer = parquet::arrow::FileWriter::Open(
            *batcher_.schema(), arrow::default_memory_pool(), out_, props, arrow_props);
        if (!writer.ok()) {
            throw std::runtime_error("ParquetRollingSink: open writer: " +
                                     writer.status().ToString());
        }
        writer_ = std::move(*writer);
    }

    // Completes the open part, if any: the footer is written and the file
    // appears under its final name.
    void finish_part_() {
        if (!writer_) {
            return;
        }
        auto closed = writer_->Close();
        writer_.reset();
        if (!closed.ok()) {
            throw std::runtime_error("ParquetRollingSink: close writer: " + closed.ToString());
        }
        const auto path = rename_into_place_ ? final_path_ + ".inprogress" : final_path_;
        auto stream_closed = out_->Close();
        out_.reset();
        if (!stream_closed.ok()) {
            throw std::runtime_error("ParquetRollingSink: close " + path + ": " +
                                     stream_closed.ToString());
        }
        if (rename_into_place_) {
            if (auto s = fs_->Move(path, final_path_); !s.ok()) {
                throw std::runtime_error("ParquetRollingSink: rename " + path + " -> " +
                                         final_path_ + ": " + s.ToString());
            }
        }
    }

    FileSystemFactory fs_factory_;
    Options opts_;
    ArrowBatcher<T> batcher_;
    std::string name_;
    std::shared_ptr<arrow::fs::FileSystem> fs_;
    bool rename_into_place_{false};
    std::string run_;
    std::uint64_t next_seq_{0};
    std::string final_path_;
    std::shared_ptr<arrow::io::OutputStream> out_;
    std::unique_ptr<parquet::arrow::FileWriter> writer_;
};

// The local-disk wiring the `parquet` connector's factories share.
inline std::function<std::shared_ptr<arrow::fs::FileSystem>()> local_parquet_filesystem() {
    return []() -> std::shared_ptr<arrow::fs::FileSystem> {
        return std::make_shared<arrow::fs::LocalFileSystem>();
    };
}

// A rolling sink writing the directory `path` on the local disk.
template <typename T>
std::shared_ptr<Sink<T>> make_local_parquet_rolling_sink(const std::string& path,
                                                         std::uint32_t subtask_idx,
                                                         std::uint32_t parallelism,
                                                         ArrowBatcher<T> batcher,
                                                         std::string name) {
    typename ParquetRollingSink<T>::Options o;
    o.dir = std::filesystem::absolute(path).lexically_normal().string();
    o.subtask_idx = subtask_idx;
    o.parallelism = parallelism;
    return std::make_shared<ParquetRollingSink<T>>(
        local_parquet_filesystem(), std::move(o), std::move(batcher), std::move(name));
}

// Every Parquet file in the local directory `path`, sharded across subtasks, or
// nullptr when `path` is not a directory (the caller then reads it as one file).
// This is how a `parquet` source reads what a rolling sink wrote.
template <typename T>
std::shared_ptr<Source<T>> make_local_parquet_directory_source(const std::string& path,
                                                               std::uint32_t subtask_idx,
                                                               std::uint32_t parallelism,
                                                               ArrowBatcher<T> batcher,
                                                               std::string name) {
    std::error_code ec;
    if (!std::filesystem::is_directory(path, ec)) {
        return nullptr;
    }
    typename MultiObjectParquetSource<T>::Options o;
    o.prefix = std::filesystem::absolute(path).lexically_normal().string();
    o.subtask_idx = static_cast<int>(subtask_idx);
    o.parallelism = static_cast<int>(parallelism);
    o.recursive = false;
    o.suffix = ".parquet";
    return std::make_shared<MultiObjectParquetSource<T>>(
        local_parquet_filesystem(), std::move(o), std::move(batcher), std::move(name));
}

}  // namespace clink
