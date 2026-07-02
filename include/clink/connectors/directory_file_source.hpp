#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <utility>
#include <vector>

#include "clink/connectors/text_format.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/time/watermark.hpp"

namespace clink {

// DirectoryFileSource<T> reads every newline-delimited text file directly under a
// directory (non-recursive) and emits records via the supplied TextFormat<T>. It is the
// multi-file companion to FileSource<T>: it lets a partitioned or full-refresh
// materialized-view backing (a directory of files written by partition_overwrite_sink)
// be read back into a downstream query.
//
// Files are read in a stable filename-sorted order so replay is deterministic. Each
// produce() fills up to batch_size records, crossing file boundaries as needed. On the
// final file's EOF it emits one max watermark and returns false.
//
// Replay (exactly-once): the checkpoint captures (file_index, line_index) - the file
// currently being read and how many of its lines were already emitted. restore_offset()
// (which runs before open()) stashes that pair; open() re-lists + re-sorts the
// directory, seeks to file_index, and skips line_index lines. The directory is assumed
// stable for the job's lifetime (a materialized-view backing is only republished
// wholesale by an atomic swap, not edited in place); a directory mutated mid-scan can
// skip or repeat files.
//
// Header-only, like FileSource: no external dependency, instantiated per-T at the call
// site.
template <typename T>
class DirectoryFileSource final : public Source<T> {
public:
    DirectoryFileSource(std::filesystem::path dir,
                        TextFormat<T> format,
                        std::size_t batch_size = 256,
                        std::string name = "directory_file_source")
        : dir_(std::move(dir)),
          format_(std::move(format)),
          batch_size_(batch_size),
          name_(std::move(name)) {
        if (!format_.decode) {
            throw std::invalid_argument("DirectoryFileSource: TextFormat::decode is required");
        }
    }

    // Reading a directory to exhaustion is a finite stream.
    [[nodiscard]] bool is_bounded() const noexcept override { return true; }

    void open() override {
        std::error_code ec;
        if (!std::filesystem::is_directory(dir_, ec)) {
            throw std::runtime_error("DirectoryFileSource: not a directory: " + dir_.string());
        }
        files_.clear();
        for (const auto& entry : std::filesystem::directory_iterator(dir_, ec)) {
            if (entry.is_regular_file(ec)) {
                files_.push_back(entry.path());
            }
        }
        // Deterministic order so replay lands on the same file at the same index.
        std::sort(files_.begin(), files_.end());

        file_idx_ = has_restored_ ? restored_file_idx_ : 0;
        const std::size_t skip_lines = has_restored_ ? restored_line_idx_ : 0;
        line_idx_ = 0;
        open_current_file_();
        // Skip already-emitted lines of the current file (mid-file restore).
        for (std::size_t i = 0; i < skip_lines && stream_.is_open(); ++i) {
            std::string discard;
            if (std::getline(stream_, discard)) {
                ++line_idx_;
            } else {
                break;
            }
        }
    }

    bool produce(Emitter<T>& out) override {
        if (this->cancelled()) {
            return false;
        }
        Batch<T> batch;
        std::size_t consumed = 0;
        while (consumed < batch_size_) {
            if (!stream_.is_open()) {
                break;  // no current file open -> exhausted (open_current_file_ failed)
            }
            std::string line;
            if (std::getline(stream_, line)) {
                if (auto v = format_.decode(line); v.has_value()) {
                    batch.emplace(std::move(*v));
                }
                ++line_idx_;
                ++consumed;
                continue;
            }
            // Current file exhausted: advance to the next one.
            stream_.close();
            ++file_idx_;
            line_idx_ = 0;
            open_current_file_();
        }
        if (!batch.empty()) {
            out.emit_data(std::move(batch));
        }
        if (!stream_.is_open()) {
            // All files consumed. Emit the final max watermark exactly once.
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

    // Persist (file_index, line_index): the file being read and how many of its lines
    // were already emitted. Runs between produce() calls, so the position is a clean
    // line boundary. When exhausted, file_idx_ == files_.size() (no sentinel needed;
    // restore simply finds no current file and emits EOS).
    void snapshot_offset(StateBackend& backend,
                         OperatorId op_id,
                         CheckpointId /*ckpt_id*/) override {
        std::array<std::byte, 16> bytes{};
        const std::uint64_t f = static_cast<std::uint64_t>(file_idx_);
        const std::uint64_t l = static_cast<std::uint64_t>(line_idx_);
        for (int i = 0; i < 8; ++i) {
            bytes[static_cast<std::size_t>(i)] = static_cast<std::byte>((f >> (i * 8)) & 0xFF);
            bytes[static_cast<std::size_t>(i + 8)] = static_cast<std::byte>((l >> (i * 8)) & 0xFF);
        }
        backend.put_operator_state(
            op_id,
            StateBackend::KeyView{kOffsetKey_, std::strlen(kOffsetKey_)},
            StateBackend::ValueView{reinterpret_cast<const char*>(bytes.data()), bytes.size()});
    }

    bool restore_offset(StateBackend& backend, OperatorId op_id) override {
        auto v = backend.get_operator_state(
            op_id, StateBackend::KeyView{kOffsetKey_, std::strlen(kOffsetKey_)});
        if (!v.has_value() || v->size() < 16) {
            return false;
        }
        std::uint64_t f = 0;
        std::uint64_t l = 0;
        for (int i = 0; i < 8; ++i) {
            f |= static_cast<std::uint64_t>(static_cast<std::uint8_t>((*v)[i])) << (i * 8);
            l |= static_cast<std::uint64_t>(static_cast<std::uint8_t>((*v)[i + 8])) << (i * 8);
        }
        restored_file_idx_ = static_cast<std::size_t>(f);
        restored_line_idx_ = static_cast<std::size_t>(l);
        has_restored_ = true;
        return true;
    }

    std::string name() const override { return name_; }

private:
    // Open files_[file_idx_] if in range; leave stream closed (exhausted) otherwise.
    void open_current_file_() {
        while (file_idx_ < files_.size()) {
            stream_.open(files_[file_idx_]);
            if (stream_.is_open()) {
                return;
            }
            // A file that vanished between listing and open: skip it.
            ++file_idx_;
            line_idx_ = 0;
        }
    }

    static constexpr const char* kOffsetKey_ = "__directory_file_source_offset__";

    std::filesystem::path dir_;
    TextFormat<T> format_;
    std::size_t batch_size_;
    std::string name_;
    std::vector<std::filesystem::path> files_;
    std::size_t file_idx_{0};
    std::size_t line_idx_{0};
    std::ifstream stream_;
    bool eos_watermark_emitted_{false};
    std::size_t restored_file_idx_{0};
    std::size_t restored_line_idx_{0};
    bool has_restored_{false};
};

}  // namespace clink
