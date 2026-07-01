#pragma once

#include <filesystem>
#include <fstream>
#include <ios>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/connectors/text_format.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink {

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
        if (overwrite_) {
            std::error_code ec;
            std::filesystem::remove(write_path_, ec);  // clear any stale staging
        }
        const std::ios::openmode mode =
            append_ ? (std::ios::out | std::ios::app) : (std::ios::out | std::ios::trunc);
        stream_.open(write_path_, mode);
        if (!stream_.is_open()) {
            throw std::runtime_error("FileSink: cannot open " + write_path_.string());
        }
    }

    void on_data(const Batch<T>& batch) override {
        for (const auto& r : batch) {
            stream_ << format_.encode(r.value()) << '\n';
        }
    }

    void flush() override {
        if (stream_.is_open()) {
            stream_.flush();
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
