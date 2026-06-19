#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <ios>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/connectors/text_format.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/time/watermark.hpp"

namespace clink {

// FileSource<T> reads a newline-delimited text file and emits records via the
// supplied TextFormat<T>. Each call to produce() reads up to batch_size lines,
// emits them as a single Batch<T>, and returns true while there is more.
//
// On EOF it returns false. The flush() default (no-op) is fine - there is no
// residual state.
//
// This is intentionally header-only: the implementation has no external
// dependencies and instantiating per-T at the call site is cheaper than a
// type-erased layer. Real connectors with heavy deps (Kafka, S3) are not
// templated.
template <typename T>
class FileSource final : public Source<T> {
public:
    FileSource(std::filesystem::path path,
               TextFormat<T> format,
               std::size_t batch_size = 256,
               std::string name = "file_source")
        : path_(std::move(path)),
          format_(std::move(format)),
          batch_size_(batch_size),
          name_(std::move(name)) {
        if (!format_.decode) {
            throw std::invalid_argument("FileSource: TextFormat::decode is required");
        }
    }

    // Reading a file to EOF is a finite stream (BATCH-1).
    [[nodiscard]] bool is_bounded() const noexcept override { return true; }

    void open() override {
        stream_.open(path_);
        if (!stream_.is_open()) {
            throw std::runtime_error("FileSource: cannot open " + path_.string());
        }
        // Source replay: restore_offset() runs before open() (see
        // dag.hpp add_source) and stashes the byte offset captured at the
        // restored checkpoint. Seek there so produce() resumes at the next
        // un-emitted line instead of re-reading from the top. kConsumed
        // means the prior run had read the whole file; seek to end so the
        // first produce() returns false (emits the EOS watermark once).
        if (has_restored_offset_) {
            if (restored_offset_ == kConsumed_) {
                stream_.seekg(0, std::ios::end);
            } else {
                stream_.seekg(static_cast<std::streamoff>(restored_offset_));
            }
        }
    }

    bool produce(Emitter<T>& out) override {
        if (this->cancelled()) {
            return false;
        }
        if (!stream_.is_open() || stream_.eof()) {
            // EOS reached on a prior call. Emit a final max watermark
            // exactly once so downstream windows / co-operators can
            // flush their state, then signal exhausted to the runtime.
            if (!eos_watermark_emitted_) {
                out.emit_watermark(Watermark::max());
                eos_watermark_emitted_ = true;
            }
            return false;
        }
        Batch<T> batch;
        std::string line;
        std::size_t consumed = 0;
        while (consumed < batch_size_ && std::getline(stream_, line)) {
            if (auto v = format_.decode(line); v.has_value()) {
                batch.emplace(std::move(*v));
            }
            ++consumed;
        }
        if (!batch.empty()) {
            out.emit_data(std::move(batch));
        }
        if (stream_.eof() || stream_.fail()) {
            if (!eos_watermark_emitted_) {
                out.emit_watermark(Watermark::max());
                eos_watermark_emitted_ = true;
            }
            return false;
        }
        return true;
    }

    void close() override {
        if (stream_.is_open()) {
            stream_.close();
        }
    }

    // Source replay: persist the byte offset from which the NEXT produce()
    // would read. snapshot_offset runs between produce() calls (never
    // during one) on the source-runner thread, so the stream is parked on a
    // clean line boundary (getline consumes the trailing newline). A good
    // stream yields tellg(); an exhausted stream (eof set after the last
    // line) yields kConsumed, so restore can seek to end instead of a stale
    // -1. Operator-state slot: fixed key, stored via put_operator_state so
    // the rescale restore filter never narrows it (source state is not keyed
    // state - every subtask restores its offset whole).
    void snapshot_offset(StateBackend& backend,
                         OperatorId op_id,
                         CheckpointId /*ckpt_id*/) override {
        std::uint64_t off = kConsumed_;
        if (stream_.is_open() && stream_.good()) {
            const auto pos = stream_.tellg();
            off = pos < 0 ? kConsumed_ : static_cast<std::uint64_t>(pos);
        }
        std::array<std::byte, 8> bytes{};
        for (int i = 0; i < 8; ++i) {
            bytes[static_cast<std::size_t>(i)] = static_cast<std::byte>((off >> (i * 8)) & 0xFF);
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
        restored_offset_ = restored;
        has_restored_offset_ = true;
        return true;
    }

    std::string name() const override { return name_; }

private:
    static constexpr const char* kOffsetKey_ = "__file_source_offset__";
    // Sentinel for "the whole file was read before the checkpoint".
    static constexpr std::uint64_t kConsumed_ = std::numeric_limits<std::uint64_t>::max();

    std::filesystem::path path_;
    TextFormat<T> format_;
    std::size_t batch_size_;
    std::string name_;
    std::ifstream stream_;
    bool eos_watermark_emitted_{false};
    std::uint64_t restored_offset_{0};
    bool has_restored_offset_{false};
};

}  // namespace clink
