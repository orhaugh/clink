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
// A part appears under its .parquet name only when it is complete, by one of
// three routes chosen from the filesystem's type:
//   - S3 streams to the final key: an unfinished multipart upload is never
//     visible, and Abort() discards it.
//   - A local disk and WebHDFS show a file under its name while it is being
//     written, so a part is written to <name>.inprogress and renamed into
//     place (on a local disk the file and the directory are fsynced first,
//     unless CLINK_STATE_FSYNC turns durability off, so a part is on stable
//     storage before the checkpoint that relies on it can complete).
//   - Every other store (Azure, GCS, anything unknown) writes <name>.inprogress
//     and copies it to the final key, then deletes the in-progress object:
//     Azure creates a blob at the key as soon as a stream opens, and a GCS
//     stream cannot abort, so streaming to the final key would publish an
//     empty or partial part.
// A kill therefore leaves at most an .inprogress file, which readers skip (it
// does not end in .parquet) and the next open removes. Nothing is buffered
// beyond what the Parquet writer holds for its current row group.

#include <array>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <fcntl.h>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unistd.h>
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
#include "clink/state/durable_file_write.hpp"

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
    // abandons its unfinished part, as a kill would: an S3 upload is aborted,
    // so nothing is published, and an .inprogress file is removed. Its rows are
    // after the last barrier, so a restore replays them.
    ~ParquetRollingSink() override {
        try {
            writer_.reset();
            if (out_) {
                (void)out_->Abort();
                out_.reset();
                if (publish_ != Publish::StreamToFinal && fs_) {
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

    // The subtask index a file name belongs to, if it is exactly one of this
    // sink's part names (sub<N>-<18 hex>-<6+ digits>.parquet, optionally
    // .inprogress), as part_name writes them; nullopt for any other file, so
    // clearing a directory never touches a file the sink did not write.
    [[nodiscard]] static std::optional<std::uint32_t> part_subtask(std::string_view basename) {
        std::string_view rest = basename;
        if (ends_with_(rest, ".inprogress")) {
            rest.remove_suffix(std::string_view{".inprogress"}.size());
        }
        if (!ends_with_(rest, ".parquet") || rest.rfind("sub", 0) != 0) {
            return std::nullopt;
        }
        rest.remove_suffix(std::string_view{".parquet"}.size());
        rest.remove_prefix(3);
        // <N>: decimal, no leading zero, fits in 32 bits.
        std::size_t i = 0;
        while (i < rest.size() && rest[i] >= '0' && rest[i] <= '9') {
            ++i;
        }
        if (i == 0 || i > 10 || (i > 1 && rest[0] == '0')) {
            return std::nullopt;
        }
        std::uint64_t sub = 0;
        for (std::size_t k = 0; k < i; ++k) {
            sub = (sub * 10) + static_cast<std::uint64_t>(rest[k] - '0');
        }
        if (sub > UINT32_MAX) {
            return std::nullopt;
        }
        rest.remove_prefix(i);
        // -<run>: 18 lowercase hex digits.
        if (rest.size() < 1 + kRunTokenLength || rest[0] != '-') {
            return std::nullopt;
        }
        for (std::size_t k = 1; k <= kRunTokenLength; ++k) {
            const char c = rest[k];
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
                return std::nullopt;
            }
        }
        rest.remove_prefix(1 + kRunTokenLength);
        // -<seq>: at least six decimal digits.
        if (rest.size() < 7 || rest[0] != '-') {
            return std::nullopt;
        }
        for (std::size_t k = 1; k < rest.size(); ++k) {
            if (rest[k] < '0' || rest[k] > '9') {
                return std::nullopt;
            }
        }
        return static_cast<std::uint32_t>(sub);
    }

    void open() override {
        fs_ = fs_factory_();
        if (!fs_) {
            throw std::runtime_error("ParquetRollingSink: filesystem factory returned null");
        }
        const auto type = fs_->type_name();
        publish_ = type == "s3"                             ? Publish::StreamToFinal
                   : (type == "local" || type == "webhdfs") ? Publish::RenameIntoPlace
                                                            : Publish::CopyIntoPlace;
        fsync_ = type == "local" && clink::state::detail::fsync_enabled();
        // An earlier single-file sink wrote a file where this sink writes a
        // directory. Refuse by name rather than fail on a raw store error, or on
        // an object store leave the old object to shadow the new parts.
        if (auto info = fs_->GetFileInfo(opts_.dir);
            info.ok() && info->type() == arrow::fs::FileType::File) {
            throw std::runtime_error(
                "ParquetRollingSink: " + opts_.dir +
                " is a file, where this sink writes a directory of part files. It was most "
                "likely written by the single-file Parquet sink of an earlier release; move or "
                "delete it, or point the sink at another path.");
        }
        // Object stores treat prefixes as implicit; a directory must exist.
        if (auto s = fs_->CreateDir(opts_.dir, /*recursive=*/true);
            !s.ok() && publish_ == Publish::RenameIntoPlace) {
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

    // Removes this subtask's in-progress and zero-length parts (always) and its
    // finished parts (when the run starts from empty state). Subtask 0 does the same for subtasks
    // at or above this run's parallelism.
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
            // A zero-length part is never a Parquet file; no route publishes one,
            // but a reader would fail on it, so it goes even on a restore.
            const bool empty = info.size() == 0;
            if (mine && (in_progress || empty || finished_too)) {
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
        const auto path = writing_path_();
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

    [[nodiscard]] std::string writing_path_() const {
        return publish_ == Publish::StreamToFinal ? final_path_ : final_path_ + ".inprogress";
    }

    // Completes the open part, if any: the footer is written and the file
    // appears under its final name, by the route open() chose.
    void finish_part_() {
        if (!writer_) {
            return;
        }
        auto closed = writer_->Close();
        writer_.reset();
        if (!closed.ok()) {
            throw std::runtime_error("ParquetRollingSink: close writer: " + closed.ToString());
        }
        const auto path = writing_path_();
        auto stream_closed = out_->Close();
        out_.reset();
        if (!stream_closed.ok()) {
            throw std::runtime_error("ParquetRollingSink: close " + path + ": " +
                                     stream_closed.ToString());
        }
        switch (publish_) {
            case Publish::StreamToFinal:
                return;
            case Publish::RenameIntoPlace:
                if (fsync_) {
                    fsync_file_(path);
                }
                if (auto s = fs_->Move(path, final_path_); !s.ok()) {
                    throw std::runtime_error("ParquetRollingSink: rename " + path + " -> " +
                                             final_path_ + ": " + s.ToString());
                }
                if (fsync_) {
                    clink::state::detail::fsync_directory_best_effort(opts_.dir);
                }
                return;
            case Publish::CopyIntoPlace:
                if (auto s = fs_->CopyFile(path, final_path_); !s.ok()) {
                    throw std::runtime_error("ParquetRollingSink: copy " + path + " -> " +
                                             final_path_ + ": " + s.ToString());
                }
                // A leftover .inprogress object is harmless (readers skip it and
                // the next open removes it), so a failed delete does not fail
                // the checkpoint.
                (void)fs_->DeleteFile(path);
                return;
        }
    }

    // The part's bytes must be on stable storage before the rename publishes
    // it, as write_fsync_rename does for checkpoint files.
    static void fsync_file_(const std::string& path) {
        const int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            throw std::runtime_error("ParquetRollingSink: open for fsync " + path);
        }
        const int rc = ::fsync(fd);
        ::close(fd);
        if (rc != 0) {
            throw std::runtime_error("ParquetRollingSink: fsync " + path);
        }
    }

    enum class Publish : std::uint8_t { StreamToFinal, RenameIntoPlace, CopyIntoPlace };
    static constexpr std::size_t kRunTokenLength = 18;

    FileSystemFactory fs_factory_;
    Options opts_;
    ArrowBatcher<T> batcher_;
    std::string name_;
    std::shared_ptr<arrow::fs::FileSystem> fs_;
    Publish publish_{Publish::CopyIntoPlace};
    bool fsync_{false};
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
    // An exactly-once Parquet sink writes <path>/staging and <path>/committed,
    // with nothing at the top level. Reading <path> would then silently yield
    // no rows, so say where the committed output is instead.
    bool has_part = false;
    for (const auto& e : std::filesystem::directory_iterator(path, ec)) {
        if (e.is_regular_file(ec) && e.path().extension() == ".parquet") {
            has_part = true;
            break;
        }
    }
    if (!has_part && std::filesystem::is_directory(std::filesystem::path(path) / "committed", ec)) {
        throw std::runtime_error(
            name + ": " + path +
            " holds no Parquet parts but has a committed/ directory, the layout of an "
            "exactly-once Parquet sink; read " +
            (std::filesystem::path(path) / "committed").string() + " instead");
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
