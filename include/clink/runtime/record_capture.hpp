#pragma once

// Record capture - the flight recorder for time-travel debugging.
//
// When armed (JobConfig::capture_dir non-empty), the single-input operator
// runner tees every input record it hands the operator into a bounded
// per-epoch buffer and, as each checkpoint barrier passes, writes the
// epoch's records to
//
//   <capture_dir>/op-<operator id>/subtask-<idx>/epoch-<checkpoint id>.cap
//
// (plus final.cap for the tail after the last barrier, flushed at runner
// teardown). Restoring state from checkpoint N and feeding the operator
// epoch-(N+1) reproduces exactly what it did next - the deterministic
// replay primitive. Files are bounded by capture_records per epoch; when
// the cap is hit the epoch keeps counting but stops storing, and the file
// header records both the truncation and the true count, so a replay can
// tell "complete epoch" from "sampled epoch".
//
// The record framing is shared with the engine's unaligned-checkpoint
// in-flight capture (Dag::serialize_records_ delegates here): per record a
// presence-tagged event time plus the value's Codec<T> bytes. A .cap file
// prepends a small header: magic "CCAP", format version, truncated flag,
// and the true records-seen count.
//
// Capture is a best-effort debug facility by design: a write failure logs
// once and disarms capture for that runner; it never fails the job.

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "clink/core/codec.hpp"
#include "clink/core/record.hpp"
#include "clink/core/types.hpp"

namespace clink::capture {

// ---- record framing (shared with the in-flight checkpoint capture) --------

template <typename T>
std::vector<std::byte> serialize_records(const std::vector<Record<T>>& records,
                                         const Codec<T>& codec) {
    std::vector<std::byte> out;
    const auto put_u32 = [&](std::uint32_t v) {
        for (int i = 0; i < 4; ++i) {
            out.push_back(static_cast<std::byte>((v >> (i * 8)) & 0xFF));
        }
    };
    const auto put_i64 = [&](std::int64_t v) {
        const auto u = static_cast<std::uint64_t>(v);
        for (int i = 0; i < 8; ++i) {
            out.push_back(static_cast<std::byte>((u >> (i * 8)) & 0xFF));
        }
    };
    put_u32(static_cast<std::uint32_t>(records.size()));
    for (const auto& r : records) {
        const bool has_t = r.event_time().has_value();
        out.push_back(static_cast<std::byte>(has_t ? 1 : 0));
        if (has_t) {
            put_i64(r.event_time()->millis());
        }
        auto bytes = codec.encode(r.value());
        put_u32(static_cast<std::uint32_t>(bytes.size()));
        out.insert(out.end(), bytes.begin(), bytes.end());
    }
    return out;
}

template <typename T>
std::vector<Record<T>> deserialize_records(std::span<const std::byte> in, const Codec<T>& codec) {
    std::vector<Record<T>> out;
    std::size_t pos = 0;
    const auto read_u32 = [&]() -> std::uint32_t {
        std::uint32_t v = 0;
        for (int i = 0; i < 4; ++i) {
            v |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(in[pos + i])) << (i * 8);
        }
        pos += 4;
        return v;
    };
    const auto read_i64 = [&]() -> std::int64_t {
        std::uint64_t u = 0;
        for (int i = 0; i < 8; ++i) {
            u |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(in[pos + i])) << (i * 8);
        }
        pos += 8;
        return static_cast<std::int64_t>(u);
    };
    if (in.size() < 4) {
        return out;
    }
    const auto count = read_u32();
    out.reserve(count);
    for (std::uint32_t r = 0; r < count; ++r) {
        if (pos >= in.size()) {
            break;
        }
        const bool has_t = static_cast<std::uint8_t>(in[pos++]) != 0;
        std::optional<EventTime> t;
        if (has_t) {
            if (pos + 8 > in.size()) {
                break;
            }
            t = EventTime{read_i64()};
        }
        if (pos + 4 > in.size()) {
            break;
        }
        const auto len = read_u32();
        if (pos + len > in.size()) {
            break;
        }
        auto value = codec.decode(in.subspan(pos, len));
        pos += len;
        if (!value.has_value()) {
            continue;  // undecodable record: skip rather than abort the read
        }
        if (t.has_value()) {
            out.emplace_back(std::move(*value), *t);
        } else {
            out.emplace_back(std::move(*value));
        }
    }
    return out;
}

// ---- .cap file format ------------------------------------------------------

inline constexpr char kCaptureMagic[4] = {'C', 'C', 'A', 'P'};
inline constexpr std::uint32_t kCaptureVersion = 1;

struct CaptureFileHeader {
    std::uint32_t version{kCaptureVersion};
    bool truncated{false};
    // Records seen in the epoch, INCLUDING any dropped past the cap. When
    // truncated, the framed payload holds fewer records than this.
    std::uint64_t records_seen{0};
};

// Serialise header + framed records into one .cap byte blob.
template <typename T>
std::vector<std::byte> encode_capture_file(const CaptureFileHeader& h,
                                           const std::vector<Record<T>>& records,
                                           const Codec<T>& codec) {
    std::vector<std::byte> out;
    out.reserve(records.size() * 32 + 32);
    for (const char c : kCaptureMagic) {
        out.push_back(static_cast<std::byte>(c));
    }
    const auto put_u32 = [&](std::uint32_t v) {
        for (int i = 0; i < 4; ++i) {
            out.push_back(static_cast<std::byte>((v >> (i * 8)) & 0xFF));
        }
    };
    put_u32(h.version);
    out.push_back(static_cast<std::byte>(h.truncated ? 1 : 0));
    const auto seen = h.records_seen;
    for (int i = 0; i < 8; ++i) {
        out.push_back(static_cast<std::byte>((seen >> (i * 8)) & 0xFF));
    }
    auto framed = serialize_records(records, codec);
    out.insert(out.end(), framed.begin(), framed.end());
    return out;
}

// Parse a .cap blob's header; returns the payload offset, or nullopt when
// the magic/shape is wrong.
inline std::optional<std::pair<CaptureFileHeader, std::size_t>> decode_capture_header(
    std::span<const std::byte> in) {
    if (in.size() < 4 + 4 + 1 + 8) {
        return std::nullopt;
    }
    if (std::memcmp(in.data(), kCaptureMagic, 4) != 0) {
        return std::nullopt;
    }
    CaptureFileHeader h;
    std::size_t pos = 4;
    h.version = 0;
    for (int i = 0; i < 4; ++i) {
        h.version |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(in[pos + i])) << (i * 8);
    }
    pos += 4;
    h.truncated = static_cast<std::uint8_t>(in[pos++]) != 0;
    h.records_seen = 0;
    for (int i = 0; i < 8; ++i) {
        h.records_seen |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(in[pos + i]))
                          << (i * 8);
    }
    pos += 8;
    return std::make_pair(h, pos);
}

// ---- the per-runner epoch buffer -------------------------------------------

template <typename T>
class EpochCaptureBuffer {
public:
    EpochCaptureBuffer(std::filesystem::path dir,
                       OperatorId op,
                       std::size_t subtask_idx,
                       std::size_t max_records,
                       std::shared_ptr<const Codec<T>> codec)
        : dir_(std::move(dir)),
          op_(op),
          subtask_idx_(subtask_idx),
          max_records_(max_records == 0 ? kDefaultMaxRecords : max_records),
          codec_(std::move(codec)) {}

    static constexpr std::size_t kDefaultMaxRecords = 10'000;

    void on_data(const Batch<T>& batch) {
        if (disarmed_) {
            return;
        }
        for (const auto& r : batch) {
            ++seen_;
            if (buf_.size() < max_records_) {
                buf_.push_back(r);
            } else {
                truncated_ = true;
            }
        }
    }

    // Flush the epoch that ENDED at this barrier. Named by the checkpoint id
    // the barrier carries, so epoch-<N>.cap holds the records between
    // checkpoint N-1 and checkpoint N - i.e. restore checkpoint N-1 + feed
    // epoch-<N>.cap reproduces the operator's path to checkpoint N. The
    // UINT64_MAX sentinel is the terminal end-of-stream barrier: its epoch
    // (everything after the last real checkpoint) is the final tail.
    void on_barrier(std::uint64_t checkpoint_id) {
        if (checkpoint_id == std::numeric_limits<std::uint64_t>::max()) {
            flush_("final.cap");
            return;
        }
        flush_("epoch-" + std::to_string(checkpoint_id) + ".cap");
    }

    // Flush the tail captured after the last barrier (runner teardown).
    void flush_final() { flush_("final.cap"); }

    [[nodiscard]] bool disarmed() const { return disarmed_; }

private:
    void flush_(const std::string& filename) {
        if (disarmed_) {
            return;
        }
        if (buf_.empty() && seen_ == 0) {
            return;  // nothing in this epoch; write no empty files
        }
        try {
            const auto subdir = dir_ / ("op-" + std::to_string(op_.value())) /
                                ("subtask-" + std::to_string(subtask_idx_));
            std::filesystem::create_directories(subdir);
            CaptureFileHeader h;
            h.truncated = truncated_;
            h.records_seen = seen_;
            const auto bytes = encode_capture_file(h, buf_, *codec_);
            std::ofstream out(subdir / filename, std::ios::binary | std::ios::trunc);
            if (!out) {
                throw std::runtime_error("cannot open " + (subdir / filename).string());
            }
            out.write(reinterpret_cast<const char*>(bytes.data()),
                      static_cast<std::streamsize>(bytes.size()));
        } catch (const std::exception&) {
            // Best-effort: disarm this runner's capture; never fail the job.
            disarmed_ = true;
        }
        buf_.clear();
        seen_ = 0;
        truncated_ = false;
    }

    std::filesystem::path dir_;
    OperatorId op_;
    std::size_t subtask_idx_;
    std::size_t max_records_;
    std::shared_ptr<const Codec<T>> codec_;
    std::vector<Record<T>> buf_;
    std::uint64_t seen_{0};
    bool truncated_{false};
    bool disarmed_{false};
};

}  // namespace clink::capture
