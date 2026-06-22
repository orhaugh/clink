#pragma once

// spdlog sink that rotates the active log file at a size threshold and
// optionally zstd-compresses each rotated segment.
//
// This is a build-private header: it includes <spdlog/...> and <zstd.h> and is
// only consumed by logging.cpp inside clink_core. It is NOT part of clink's
// installed public surface, so it never reaches a dlopen'd plugin .so.
//
// Behaviour mirrors spdlog's rotating_file_sink, with two deliberate
// differences from the prior gateway implementation it is ported from:
//   1. Rotated segments are compressed with zstd (streaming) rather than gzip.
//      zstd is already linked transitively via Arrow, compresses text logs
//      far faster than gzip at a comparable-or-better ratio, and the rotation
//      runs on spdlog's async worker thread so the compression cost is off the
//      operator/daemon threads.
//   2. No-data-loss on compress failure: the active file is deleted ONLY after
//      zstd reports success. On any failure the segment is preserved by a plain
//      rename to `name.N.log`, so a rotated segment is never lost even if the
//      compressor cannot open its output. The mixed-extension directory this
//      can produce is expected and the history reader globs both extensions.

#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <string>
#include <zstd.h>

#include <spdlog/details/file_helper.h>
#include <spdlog/details/null_mutex.h>
#include <spdlog/sinks/base_sink.h>

namespace clink::logging {

template <typename Mutex>
class CompressingRotatingFileSink : public spdlog::sinks::base_sink<Mutex> {
public:
    CompressingRotatingFileSink(std::string base_filename,
                                std::size_t max_size,
                                std::size_t max_files,
                                bool compress,
                                int zstd_level)
        : base_filename_(std::move(base_filename)),
          max_size_(max_size == 0 ? 1 : max_size),
          max_files_(max_files == 0 ? 1 : max_files),
          compress_(compress),
          zstd_level_(zstd_level) {
        if (const auto parent = std::filesystem::path(base_filename_).parent_path();
            !parent.empty()) {
            std::error_code ec;
            std::filesystem::create_directories(parent, ec);
        }
        file_helper_.open(base_filename_, false);
        current_size_ = file_helper_.size();
    }

protected:
    void sink_it_(const spdlog::details::log_msg& msg) override {
        spdlog::memory_buf_t formatted;
        spdlog::sinks::base_sink<Mutex>::formatter_->format(msg, formatted);
        current_size_ += formatted.size();
        if (current_size_ > max_size_) {
            rotate();
            current_size_ = formatted.size();
        }
        file_helper_.write(formatted);
    }

    void flush_() override { file_helper_.flush(); }

private:
    void rotate() {
        file_helper_.close();

        // Drop the oldest segment in either extension.
        remove_quiet(seg_zst(max_files_));
        remove_quiet(seg_plain(max_files_));

        // Shift existing segments up by one, preserving their extension.
        for (std::size_t i = max_files_ - 1; i >= 1; --i) {
            rename_quiet(seg_zst(i), seg_zst(i + 1));
            rename_quiet(seg_plain(i), seg_plain(i + 1));
            if (i == 1) {
                break;  // unsigned guard: do not wrap below 1
            }
        }

        // Move the active file into slot 1. Compress when requested, but only
        // delete the source after a confirmed success; otherwise preserve it.
        bool placed = false;
        if (compress_) {
            if (zstd_compress_file(base_filename_, seg_zst(1))) {
                remove_quiet(base_filename_);
                placed = true;
            } else {
                std::cerr << "[clink][logging] zstd compression of rotated log '" << base_filename_
                          << "' failed; kept uncompressed segment '" << seg_plain(1) << "'\n";
                remove_quiet(seg_zst(1));  // remove any partial output
            }
        }
        if (!placed) {
            rename_quiet(base_filename_, seg_plain(1));
        }

        file_helper_.open(base_filename_, true);
    }

    [[nodiscard]] std::string seg_plain(std::size_t index) const {
        const std::filesystem::path p(base_filename_);
        return (p.parent_path() /
                (p.stem().string() + "." + std::to_string(index) + p.extension().string()))
            .string();
    }

    [[nodiscard]] std::string seg_zst(std::size_t index) const { return seg_plain(index) + ".zst"; }

    static void remove_quiet(const std::string& path) {
        std::error_code ec;
        std::filesystem::remove(path, ec);
    }

    static void rename_quiet(const std::string& src, const std::string& dst) {
        std::error_code ec;
        if (std::filesystem::exists(src, ec)) {
            std::filesystem::rename(src, dst, ec);
        }
    }

    // Drain one input chunk through the compressor, writing all produced
    // output to `out`. Returns false on any zstd or write error.
    static bool drain_chunk(ZSTD_CCtx* cctx,
                            ZSTD_inBuffer& input,
                            std::ofstream& out,
                            std::string& out_buf,
                            bool last_chunk) {
        const ZSTD_EndDirective mode = last_chunk ? ZSTD_e_end : ZSTD_e_continue;
        for (;;) {
            ZSTD_outBuffer output{.dst = out_buf.data(), .size = out_buf.size(), .pos = 0};
            const std::size_t remaining = ZSTD_compressStream2(cctx, &output, &input, mode);
            if (ZSTD_isError(remaining) != 0U) {
                return false;
            }
            if (output.pos > 0) {
                out.write(out_buf.data(), static_cast<std::streamsize>(output.pos));
                if (!out.good()) {
                    return false;
                }
            }
            if (last_chunk ? (remaining == 0) : (input.pos == input.size)) {
                return true;
            }
        }
    }

    // Streaming zstd compression of src -> dst. Returns true only on a clean
    // end-of-frame flush. Frames are written with unknown content size, so a
    // reader must use the streaming decompression API.
    [[nodiscard]] bool zstd_compress_file(const std::string& src, const std::string& dst) const {
        std::ifstream in(src, std::ios::binary);
        std::ofstream out(dst, std::ios::binary | std::ios::trunc);
        if (!in.is_open() || !out.is_open()) {
            return false;
        }
        ZSTD_CCtx* cctx = ZSTD_createCCtx();
        if (cctx == nullptr) {
            return false;
        }
        ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, zstd_level_);

        const std::size_t in_cap = ZSTD_CStreamInSize();
        std::string in_buf(in_cap, '\0');
        std::string out_buf(ZSTD_CStreamOutSize(), '\0');

        bool ok = true;
        for (;;) {
            in.read(in_buf.data(), static_cast<std::streamsize>(in_cap));
            const auto read_n = static_cast<std::size_t>(in.gcount());
            const bool last_chunk = !in.good();  // eof or error after this read
            ZSTD_inBuffer input{.src = in_buf.data(), .size = read_n, .pos = 0};
            if (!drain_chunk(cctx, input, out, out_buf, last_chunk)) {
                ok = false;
                break;
            }
            if (last_chunk) {
                break;
            }
        }

        ZSTD_freeCCtx(cctx);
        out.flush();
        const bool flushed = out.good();
        out.close();
        if (!ok || !flushed) {
            remove_quiet(dst);
            return false;
        }
        return true;
    }

    std::string base_filename_;
    std::size_t max_size_;
    std::size_t max_files_;
    bool compress_;
    int zstd_level_;
    std::size_t current_size_{0};
    spdlog::details::file_helper file_helper_;
};

using CompressingRotatingFileSinkMt = CompressingRotatingFileSink<std::mutex>;

}  // namespace clink::logging
