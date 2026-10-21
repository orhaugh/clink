#pragma once

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <ios>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

#include "clink/connectors/text_format.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink {

namespace detail {

// Cuts a line-per-record file back to the end of its last complete line. A
// process that died while a buffer flush was half written leaves a partial
// line at the end, and a restored run appending after it would glue its first
// line to that fragment, corrupting both. Every checkpoint barrier flushes
// whole lines, so a fragment always follows the restored checkpoint's flush,
// and the restored sources replay the record it held.
inline void drop_torn_tail(const std::filesystem::path& path) {
    std::error_code ec;
    const std::uintmax_t size = std::filesystem::file_size(path, ec);
    if (ec || size == 0) {
        return;  // nothing written yet (or no file): nothing to cut
    }
    std::ifstream in(path, std::ios::binary);
    std::uintmax_t end = size;
    char buf[4096];
    while (end > 0) {
        const std::uintmax_t chunk = std::min<std::uintmax_t>(sizeof(buf), end);
        in.seekg(static_cast<std::streamoff>(end - chunk));
        in.read(buf, static_cast<std::streamsize>(chunk));
        if (!in) {
            throw std::runtime_error("cannot read " + path.string() + " to find its last line");
        }
        for (std::uintmax_t i = chunk; i > 0; --i) {
            if (buf[i - 1] == '\n') {
                const std::uintmax_t keep = end - chunk + i;
                if (keep != size) {
                    in.close();
                    std::filesystem::resize_file(path, keep, ec);
                }
                if (ec) {
                    throw std::runtime_error("cannot cut the partial last line of " +
                                             path.string() + ": " + ec.message());
                }
                return;
            }
        }
        end -= chunk;
    }
    in.close();
    std::filesystem::resize_file(path, 0, ec);  // not one complete line
    if (ec) {
        throw std::runtime_error("cannot cut the partial line of " + path.string() + ": " +
                                 ec.message());
    }
}

}  // namespace detail

// FileSink<T> writes one line per record using the supplied TextFormat<T>.
// `append=true` opens the file in append mode (preserves existing contents);
// otherwise the file is truncated on open().
//
// `overwrite=true` (mutually exclusive with append) is the atomic full-overwrite
// mode used by full-refresh materialized tables: the sink writes to a sibling
// staging file (`<path>.staging`) during the job and, on clean end-of-input
// (flush()), atomically renames it over `<path>`. A reader of `<path>` sees the
// previous snapshot for the whole job and then, after the single rename, the new
// one - never a torn file. A mid-job failure tears down through close() WITHOUT the
// rename, so the staging file is orphaned and the published `<path>` is left intact
// (the old snapshot survives); the next run clears the stale staging at open().
// Overwrite is single-file, so it requires parallelism 1 (one output file to swap).
//
// Records are written immediately to the stream and flushed on flush() (which the
// runtime invokes once at end-of-stream).
//
// A run restored from a checkpoint (a failover, or an embedded rerun resuming
// after a kill) continues the file instead of truncating it: the rows written
// before the restore point stay, and the rows after it are written again as the
// sources replay them. That is this sink's at-least-once contract; truncating
// would instead lose every row the restored sources do not replay. An overwrite
// sink likewise keeps its staging file, recovering it from the published file
// when a kill landed between the publish and the job's end. A restored run
// first cuts a partial last line a kill left behind (detail::drop_torn_tail).
// A write the stream refuses, such as one to a full disk, fails the sink at
// once rather than leaving a gap the checkpoint would cover.
template <typename T>
class FileSink final : public Sink<T> {
public:
    FileSink(std::filesystem::path path,
             TextFormat<T> format,
             bool append = false,
             std::string name = "file_sink",
             bool overwrite = false)
        : path_(std::move(path)),
          format_(std::move(format)),
          append_(append),
          name_(std::move(name)),
          overwrite_(overwrite) {
        if (!format_.encode) {
            throw std::invalid_argument("FileSink: TextFormat::encode is required");
        }
        if (append_ && overwrite_) {
            throw std::invalid_argument("FileSink: append and overwrite are mutually exclusive");
        }
    }

    void open() override {
        write_path_ = overwrite_ ? staging_path() : path_;
        const bool restored =
            this->runtime() != nullptr && this->runtime()->restore_from_checkpoint_id() > 0;
        if (overwrite_) {
            std::error_code ec;
            if (!restored) {
                std::filesystem::remove(write_path_, ec);  // clear any stale staging
            } else if (!std::filesystem::exists(write_path_, ec) &&
                       std::filesystem::exists(path_, ec)) {
                std::filesystem::copy_file(path_, write_path_, ec);
            }
        }
        if (restored) {
            detail::drop_torn_tail(write_path_);
        }
        const std::ios::openmode mode = (append_ || restored) ? (std::ios::out | std::ios::app)
                                                              : (std::ios::out | std::ios::trunc);
        stream_.open(write_path_, mode);
        if (!stream_.is_open()) {
            throw std::runtime_error("FileSink: cannot open " + write_path_.string());
        }
    }

    void on_data(const Batch<T>& batch) override {
        for (const auto& r : batch) {
            stream_ << format_.encode(r.value()) << '\n';
        }
        check_stream_("write");
    }

    // A restored run keeps the lines before its checkpoint and replays the
    // rest (see above), so a checkpoint must not complete while any line
    // before its barrier is still in the stream's buffer. Handing them to the
    // OS here is enough for a process failure; a host failure that loses the
    // page cache is not covered. The acknowledgement follows this hook only
    // while the sink owns its chain's checkpoint, so Dag::add_sink refuses
    // another sink beside it.
    void on_barrier(CheckpointBarrier /*b*/) override {
        if (stream_.is_open()) {
            stream_.flush();
            check_stream_("flush at the checkpoint barrier");
        }
    }
    [[nodiscard]] bool gates_checkpoint_ack() const noexcept override { return true; }

    void flush() override {
        if (stream_.is_open()) {
            stream_.flush();
            check_stream_("flush at the end of input");
        }
        // Clean end-of-input: publish the staged output atomically. flush() is the
        // success signal (a failed job tears down through close(), not flush()), so
        // the swap only happens on a completed run.
        if (overwrite_ && !swapped_) {
            if (stream_.is_open()) {
                stream_.close();
            }
            std::error_code ec;
            std::filesystem::rename(write_path_, path_, ec);
            if (ec) {
                throw std::runtime_error("FileSink overwrite: cannot publish " +
                                         write_path_.string() + " -> " + path_.string() + ": " +
                                         ec.message());
            }
            swapped_ = true;
        }
    }

    void close() override {
        if (stream_.is_open()) {
            stream_.close();
        }
        // Overwrite + not swapped => the job did not reach a clean flush(); leave the
        // staging file orphaned and `path_` (the old snapshot) untouched.
    }

    std::string name() const override { return name_; }

private:
    void check_stream_(const char* what) const {
        if (!stream_) {
            throw std::runtime_error("FileSink: " + std::string{what} + " to " +
                                     write_path_.string() + " failed");
        }
    }

    [[nodiscard]] std::filesystem::path staging_path() const {
        return std::filesystem::path(path_.string() + ".staging");
    }

    std::filesystem::path path_;
    TextFormat<T> format_;
    bool append_;
    std::string name_;
    bool overwrite_;
    std::filesystem::path write_path_;
    bool swapped_ = false;
    std::ofstream stream_;
};

}  // namespace clink
