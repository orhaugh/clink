#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <ios>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "clink/connectors/text_format.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/log_buffer.hpp"
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
        buf_.resize(kReadBuf_);
        buf_pos_ = buf_len_ = 0;
        stream_eof_ = false;
        logical_offset_ = 0;
        // Source replay: restore_offset() runs before open() (see
        // dag.hpp add_source) and stashes the byte offset captured at the
        // restored checkpoint. Seek there so produce() resumes at the next
        // un-emitted line instead of re-reading from the top. kConsumed
        // means the prior run had read the whole file; seek to end so the
        // first produce() returns false (emits the EOS watermark once).
        if (has_restored_offset_) {
            if (restored_offset_ == kConsumed_) {
                stream_.seekg(0, std::ios::end);
                stream_eof_ = true;
            } else if (same_file_before_(restored_offset_)) {
                stream_.seekg(static_cast<std::streamoff>(restored_offset_));
                logical_offset_ = restored_offset_;
            } else {
                // A file shorter than the position, or whose bytes before it
                // are not those the checkpoint read, is another file: resumed
                // at the offset it lost every line (a shorter file read as
                // empty) or began mid-line. Read from the start: lines
                // repeated, which at-least-once allows, never skipped.
                clink::log::warn("source.file",
                                 name_ + ": " + path_.string() +
                                     " is not the file the checkpoint read up to byte " +
                                     std::to_string(restored_offset_) +
                                     ", so it is read again from the start");
            }
        }
    }

    bool produce(Emitter<T>& out) override {
        if (this->cancelled()) {
            return false;
        }
        if (!stream_.is_open() || exhausted_()) {
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
        std::string_view line;
        std::size_t consumed = 0;
        while (consumed < batch_size_ && next_line_(line)) {
            if (auto v = format_.decode(line); v.has_value()) {
                batch.emplace(std::move(*v));
            }
            ++consumed;
        }
        if (!batch.empty()) {
            out.emit_data(std::move(batch));
        }
        if (exhausted_()) {
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
    // The checkpoint holds, after a version byte, the offset and a fingerprint
    // of the bytes just before it: their count (up to kWindow_) and an FNV-1a
    // hash, read again from the file, so a restore can tell the file it reads
    // from the one the checkpoint read.
    void snapshot_offset(StateBackend& backend,
                         OperatorId op_id,
                         CheckpointId /*ckpt_id*/) override {
        std::uint64_t off = kConsumed_;
        if (stream_.is_open() && !exhausted_()) {
            off = logical_offset_;
        }
        std::uint32_t window = 0;
        std::uint64_t hash = 0;
        if (off != kConsumed_) {
            window = static_cast<std::uint32_t>(std::min<std::uint64_t>(kWindow_, off));
            if (const auto h = window_hash_(off, window)) {
                hash = *h;
            } else {
                window = 0;  // unreadable now: a restore checks the length alone
            }
        }
        std::array<std::byte, kStateBytes_> bytes{};
        bytes[0] = static_cast<std::byte>(kStateVersion_);
        for (int i = 0; i < 8; ++i) {
            bytes[static_cast<std::size_t>(1 + i)] =
                static_cast<std::byte>((off >> (i * 8)) & 0xFF);
            bytes[static_cast<std::size_t>(13 + i)] =
                static_cast<std::byte>((hash >> (i * 8)) & 0xFF);
        }
        for (int i = 0; i < 4; ++i) {
            bytes[static_cast<std::size_t>(9 + i)] =
                static_cast<std::byte>((window >> (i * 8)) & 0xFF);
        }
        backend.put_operator_state(
            op_id,
            StateBackend::KeyView{kOffsetKey_, std::strlen(kOffsetKey_)},
            StateBackend::ValueView{reinterpret_cast<const char*>(bytes.data()), bytes.size()});
    }

    // An earlier release wrote the offset alone, in 8 bytes; it restores with
    // only the file's length to check it against.
    bool restore_offset(StateBackend& backend, OperatorId op_id) override {
        auto v = backend.get_operator_state(
            op_id, StateBackend::KeyView{kOffsetKey_, std::strlen(kOffsetKey_)});
        if (!v.has_value()) {
            return false;
        }
        const auto byte_at = [&v](std::size_t i) {
            return static_cast<std::uint64_t>(std::to_integer<std::uint8_t>((*v)[i]));
        };
        std::uint64_t restored = 0;
        restored_window_ = 0;
        restored_hash_ = 0;
        if (v->size() == kStateBytes_ && byte_at(0) == kStateVersion_) {
            for (int i = 0; i < 8; ++i) {
                restored |= byte_at(static_cast<std::size_t>(1 + i)) << (i * 8);
                restored_hash_ |= byte_at(static_cast<std::size_t>(13 + i)) << (i * 8);
            }
            for (int i = 0; i < 4; ++i) {
                restored_window_ |=
                    static_cast<std::uint32_t>(byte_at(static_cast<std::size_t>(9 + i)) << (i * 8));
            }
        } else if (v->size() == 8) {
            for (int i = 0; i < 8; ++i) {
                restored |= byte_at(static_cast<std::size_t>(i)) << (i * 8);
            }
        } else {
            throw std::runtime_error(name_ +
                                     ": the checkpoint's position is in a form this "
                                     "release does not read (" +
                                     std::to_string(v->size()) + " bytes)");
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
    static constexpr std::uint64_t kStateVersion_ = 2;
    static constexpr std::size_t kStateBytes_ = 21;  // version, offset, window, hash
    static constexpr std::uint64_t kWindow_ = 64 * 1024;

    // FNV-1a over the `window` bytes of the file before `offset`, read through
    // a stream of its own; nothing when they cannot be read.
    [[nodiscard]] std::optional<std::uint64_t> window_hash_(std::uint64_t offset,
                                                            std::uint32_t window) const {
        std::ifstream in(path_, std::ios::binary);
        if (!in) {
            return std::nullopt;
        }
        in.seekg(static_cast<std::streamoff>(offset - window));
        std::vector<char> bytes(window);
        in.read(bytes.data(), static_cast<std::streamsize>(window));
        if (static_cast<std::uint64_t>(in.gcount()) != window) {
            return std::nullopt;
        }
        std::uint64_t h = 14695981039346656037ULL;
        for (const char c : bytes) {
            h ^= static_cast<std::uint8_t>(c);
            h *= 1099511628211ULL;
        }
        return h;
    }

    // Whether the file holds at least `offset` bytes and, when the checkpoint
    // recorded them, the same bytes before it.
    [[nodiscard]] bool same_file_before_(std::uint64_t offset) const {
        std::error_code ec;
        const auto size = std::filesystem::file_size(path_, ec);
        if (ec || size < offset) {
            return false;
        }
        if (restored_window_ == 0) {
            return true;
        }
        const auto h = window_hash_(offset, restored_window_);
        return h.has_value() && *h == restored_hash_;
    }

    // Buffered line scanning over block reads. The istream getline path
    // read the file a character at a time through the streambuf virtual
    // layer and copied every line; this reads kReadBuf_ blocks and hands
    // decode() a ZERO-COPY string_view into the buffer for the common
    // case (a line entirely inside the window). Only a line spanning the
    // buffer edge is assembled into spill_. Semantics match getline
    // exactly: split on plain LF (a CR stays on the line), empty lines
    // are produced, and a final unterminated line is returned once.
    // logical_offset_ tracks the file offset of the next unreturned byte
    // - the value snapshot_offset persists (the underlying stream's
    // tellg() would report the block-read high-water mark instead).
    bool next_line_(std::string_view& line) {
        const char* base = buf_.data() + buf_pos_;
        const std::size_t avail = buf_len_ - buf_pos_;
        if (avail > 0) {
            if (const void* nl = std::memchr(base, '\n', avail); nl != nullptr) {
                const auto n = static_cast<std::size_t>(static_cast<const char*>(nl) - base);
                line = std::string_view(base, n);
                buf_pos_ += n + 1;
                logical_offset_ += n + 1;
                return true;
            }
        }
        // Slow path: the line continues past the window (or it is empty).
        spill_.assign(base, avail);
        buf_pos_ = buf_len_ = 0;
        while (true) {
            refill_();
            if (buf_len_ == 0) {
                if (spill_.empty()) {
                    return false;  // clean EOF on a line boundary
                }
                line = spill_;
                logical_offset_ += spill_.size();  // final unterminated line
                return true;
            }
            if (const void* nl = std::memchr(buf_.data(), '\n', buf_len_); nl != nullptr) {
                const auto n = static_cast<std::size_t>(static_cast<const char*>(nl) - buf_.data());
                spill_.append(buf_.data(), n);
                buf_pos_ = n + 1;
                line = spill_;
                logical_offset_ += spill_.size() + 1;
                return true;
            }
            spill_.append(buf_.data(), buf_len_);
            buf_pos_ = buf_len_ = 0;
        }
    }

    void refill_() {
        if (stream_eof_) {
            buf_pos_ = buf_len_ = 0;
            return;
        }
        stream_.read(buf_.data(), static_cast<std::streamsize>(buf_.size()));
        buf_len_ = static_cast<std::size_t>(stream_.gcount());
        buf_pos_ = 0;
        if (buf_len_ < buf_.size()) {
            stream_eof_ = true;  // short read on a regular file = EOF (or error)
        }
    }

    [[nodiscard]] bool exhausted_() const { return buf_pos_ >= buf_len_ && stream_eof_; }

    static constexpr std::size_t kReadBuf_ = 256 * 1024;

    std::filesystem::path path_;
    TextFormat<T> format_;
    std::size_t batch_size_;
    std::string name_;
    std::ifstream stream_;
    std::vector<char> buf_;
    std::string spill_;
    std::size_t buf_pos_{0};
    std::size_t buf_len_{0};
    std::uint64_t logical_offset_{0};
    bool stream_eof_{false};
    bool eos_watermark_emitted_{false};
    std::uint64_t restored_offset_{0};
    std::uint32_t restored_window_{0};
    std::uint64_t restored_hash_{0};
    bool has_restored_offset_{false};
};

}  // namespace clink
