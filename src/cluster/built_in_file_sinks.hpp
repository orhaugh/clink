#pragma once

// The built-in line sinks, file_line_sink and file_int64_sink: one record per
// line to the file named by the `path` param. Registered twice, into the
// operator registry (operator_registry.cpp) and through the plugin API into the
// runner registry (built_in_factories.cpp), both from these classes.
//
// At-least-once: a fresh run truncates the file; a run restored from a
// checkpoint keeps the lines before its restore point, which the sources do
// not replay, cuts a partial last line a kill left behind, and appends. Every
// checkpoint barrier flushes the lines before it, and a write the stream
// refuses fails the sink.

#include <cstdint>
#include <fstream>
#include <ios>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/connectors/file_sink.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/runtime_context.hpp"

namespace clink::cluster::detail {

class FileLineSink final : public Sink<std::string> {
public:
    explicit FileLineSink(std::string path) : path_(std::move(path)) {}
    void open() override {
        // A run restored from a checkpoint keeps the lines before its restore
        // point, which the sources do not replay, and appends after them (a
        // partial last line from a kill cut first); a fresh run starts over.
        const bool restored = runtime() != nullptr && runtime()->restore_from_checkpoint_id() > 0;
        if (restored) {
            clink::detail::drop_torn_tail(path_);
        }
        out_.open(path_, restored ? std::ios::app : std::ios::trunc);
        if (!out_.is_open()) {
            throw std::runtime_error("FileLineSink: failed to open " + path_);
        }
    }
    void on_data(const Batch<std::string>& batch) override {
        for (const auto& r : batch) {
            out_ << r.value() << "\n";
        }
    }
    // The checkpoint the barrier starts covers every line before it, and a
    // restore keeps the file, so those lines leave the stream's buffer first.
    void on_barrier(CheckpointBarrier /*b*/) override {
        out_.flush();
        if (!out_) {
            throw std::runtime_error(name() + ": flush to " + path_ + " failed");
        }
    }
    [[nodiscard]] bool gates_checkpoint_ack() const noexcept override { return true; }
    void flush() override {
        out_.flush();
        if (!out_) {
            throw std::runtime_error(name() + ": flush to " + path_ + " failed");
        }
    }
    void close() override { out_.close(); }
    std::string name() const override { return "file_line_sink"; }

private:
    std::string path_;
    std::ofstream out_;
};

class FileInt64Sink final : public Sink<std::int64_t> {
public:
    explicit FileInt64Sink(std::string path) : path_(std::move(path)) {}
    void open() override {
        // A run restored from a checkpoint keeps the lines before its restore
        // point, which the sources do not replay, and appends after them (a
        // partial last line from a kill cut first); a fresh run starts over.
        const bool restored = runtime() != nullptr && runtime()->restore_from_checkpoint_id() > 0;
        if (restored) {
            clink::detail::drop_torn_tail(path_);
        }
        out_.open(path_, restored ? std::ios::app : std::ios::trunc);
        if (!out_.is_open()) {
            throw std::runtime_error("FileInt64Sink: failed to open " + path_);
        }
    }
    void on_data(const Batch<std::int64_t>& batch) override {
        for (const auto& r : batch) {
            out_ << r.value() << "\n";
        }
    }
    // The checkpoint the barrier starts covers every line before it, and a
    // restore keeps the file, so those lines leave the stream's buffer first.
    void on_barrier(CheckpointBarrier /*b*/) override {
        out_.flush();
        if (!out_) {
            throw std::runtime_error(name() + ": flush to " + path_ + " failed");
        }
    }
    [[nodiscard]] bool gates_checkpoint_ack() const noexcept override { return true; }
    void flush() override {
        out_.flush();
        if (!out_) {
            throw std::runtime_error(name() + ": flush to " + path_ + " failed");
        }
    }
    void close() override { out_.close(); }
    std::string name() const override { return "file_int64_sink"; }

private:
    std::string path_;
    std::ofstream out_;
};

}  // namespace clink::cluster::detail
