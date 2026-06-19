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
// Records are written immediately to the stream and flushed on flush() (which
// the runtime invokes once at end-of-stream).
template <typename T>
class FileSink final : public Sink<T> {
public:
    FileSink(std::filesystem::path path,
             TextFormat<T> format,
             bool append = false,
             std::string name = "file_sink")
        : path_(std::move(path)),
          format_(std::move(format)),
          append_(append),
          name_(std::move(name)) {
        if (!format_.encode) {
            throw std::invalid_argument("FileSink: TextFormat::encode is required");
        }
    }

    void open() override {
        const std::ios::openmode mode =
            append_ ? (std::ios::out | std::ios::app) : (std::ios::out | std::ios::trunc);
        stream_.open(path_, mode);
        if (!stream_.is_open()) {
            throw std::runtime_error("FileSink: cannot open " + path_.string());
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
    }

    void close() override {
        if (stream_.is_open()) {
            stream_.close();
        }
    }

    std::string name() const override { return name_; }

private:
    std::filesystem::path path_;
    TextFormat<T> format_;
    bool append_;
    std::string name_;
    std::ofstream stream_;
};

}  // namespace clink
