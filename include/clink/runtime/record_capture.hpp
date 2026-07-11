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
// The plain record framing is shared with the engine's unaligned-checkpoint
// in-flight capture (Dag::serialize_records_ delegates here): per record a
// presence-tagged event time plus the value's Codec<T> bytes. A .cap file
// prepends a small header: magic "CCAP", format version, truncated flag,
// and the true records-seen count.
//
// FORMAT v2 (current): the epoch payload is an EVENT stream, not a plain
// record list - watermarks and clock advances are interleaved with the
// data records in the exact order the runner observed them. That is what
// makes replay full-fidelity: feeding the events back reproduces
// watermark-driven window fires and processing-time timer fires at their
// production positions, not just the per-record path. v1 files (records
// only) remain readable; read_capture_events() lifts them into a
// data-only event stream.
//
// Capture is a best-effort debug facility by design: a write failure logs
// once and disarms capture for that runner; it never fails the job.

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>
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

// ---- the capture event model (format v2) -----------------------------------
//
// One epoch = the ordered sequence of everything the runner handed the
// operator between two barriers: data records, watermarks, and the clock
// positions at which due processing-time timers fired. Replay feeds them
// back in order through the operator's production paths.

struct WatermarkEvent {
    std::int64_t ts_ms{0};
    bool idle{false};
};

// A processing-time position at which the runner fired due timers. Replay
// moves its manual clock here and fires through fire_due_timers - the
// runner's between-pops poll, reproduced at the captured stream position.
struct ClockEvent {
    std::int64_t now_ms{0};
};

template <typename T>
using CaptureEvent = std::variant<Record<T>, WatermarkEvent, ClockEvent>;

namespace event_tag {
inline constexpr std::uint8_t kRecord = 0;
inline constexpr std::uint8_t kWatermark = 1;
inline constexpr std::uint8_t kClock = 2;
}  // namespace event_tag

template <typename T>
std::vector<std::byte> serialize_events(const std::vector<CaptureEvent<T>>& events,
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
    put_u32(static_cast<std::uint32_t>(events.size()));
    for (const auto& e : events) {
        if (const auto* rec = std::get_if<Record<T>>(&e)) {
            out.push_back(static_cast<std::byte>(event_tag::kRecord));
            const bool has_t = rec->event_time().has_value();
            out.push_back(static_cast<std::byte>(has_t ? 1 : 0));
            if (has_t) {
                put_i64(rec->event_time()->millis());
            }
            auto bytes = codec.encode(rec->value());
            put_u32(static_cast<std::uint32_t>(bytes.size()));
            out.insert(out.end(), bytes.begin(), bytes.end());
        } else if (const auto* wm = std::get_if<WatermarkEvent>(&e)) {
            out.push_back(static_cast<std::byte>(event_tag::kWatermark));
            put_i64(wm->ts_ms);
            out.push_back(static_cast<std::byte>(wm->idle ? 1 : 0));
        } else {
            out.push_back(static_cast<std::byte>(event_tag::kClock));
            put_i64(std::get<ClockEvent>(e).now_ms);
        }
    }
    return out;
}

template <typename T>
std::vector<CaptureEvent<T>> deserialize_events(std::span<const std::byte> in,
                                                const Codec<T>& codec) {
    std::vector<CaptureEvent<T>> out;
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
    for (std::uint32_t r = 0; r < count && pos < in.size(); ++r) {
        const auto tag = static_cast<std::uint8_t>(in[pos++]);
        if (tag == event_tag::kRecord) {
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
                out.emplace_back(Record<T>{std::move(*value), *t});
            } else {
                out.emplace_back(Record<T>{std::move(*value)});
            }
        } else if (tag == event_tag::kWatermark) {
            if (pos + 9 > in.size()) {
                break;
            }
            WatermarkEvent wm;
            wm.ts_ms = read_i64();
            wm.idle = static_cast<std::uint8_t>(in[pos++]) != 0;
            out.emplace_back(wm);
        } else if (tag == event_tag::kClock) {
            if (pos + 8 > in.size()) {
                break;
            }
            out.emplace_back(ClockEvent{read_i64()});
        } else {
            break;  // unknown tag: stop rather than misparse
        }
    }
    return out;
}

// ---- .cap file format ------------------------------------------------------

inline constexpr char kCaptureMagic[4] = {'C', 'C', 'A', 'P'};
inline constexpr std::uint32_t kCaptureVersion = 2;

struct CaptureFileHeader {
    std::uint32_t version{kCaptureVersion};
    bool truncated{false};
    // DATA records seen in the epoch, INCLUDING any dropped past the cap.
    // When truncated, the framed payload holds fewer records than this.
    std::uint64_t records_seen{0};
};

// Serialise header + framed event stream into one .cap byte blob (v2).
template <typename T>
std::vector<std::byte> encode_capture_file(const CaptureFileHeader& h,
                                           const std::vector<CaptureEvent<T>>& events,
                                           const Codec<T>& codec) {
    std::vector<std::byte> out;
    out.reserve(events.size() * 32 + 32);
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
    auto framed = serialize_events(events, codec);
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

// Read a whole .cap blob into (header, events), handling every format
// version: v2 payloads decode directly; v1 payloads (plain records) are
// lifted into a data-only event stream. Returns nullopt on a bad magic.
template <typename T>
std::optional<std::pair<CaptureFileHeader, std::vector<CaptureEvent<T>>>> read_capture_events(
    std::span<const std::byte> in, const Codec<T>& codec) {
    auto hdr = decode_capture_header(in);
    if (!hdr.has_value()) {
        return std::nullopt;
    }
    const auto payload = in.subspan(hdr->second);
    std::vector<CaptureEvent<T>> events;
    if (hdr->first.version >= 2) {
        events = deserialize_events(payload, codec);
    } else {
        for (auto& rec : deserialize_records(payload, codec)) {
            events.emplace_back(std::move(rec));
        }
    }
    return std::make_pair(hdr->first, std::move(events));
}

// ---- the op-spec sidecar (op.json) -----------------------------------------
//
// Replay needs to REBUILD the captured operator offline, so the capture
// layer writes each armed operator's build spec next to its epochs:
// <capture_dir>/op-<id>/subtask-<idx>/op.json holding the factory type,
// params, channels and uid. Written once per runner arm (best-effort,
// like the epochs). The format is a flat JSON object with string values -
// exactly the OperatorSpec params model.

struct OpSpecSidecar {
    std::string op_type;
    std::string in_channel;
    std::string out_channel;
    std::string uid;
    std::map<std::string, std::string> params;
};

inline void write_op_spec(const std::filesystem::path& capture_dir,
                          OperatorId op,
                          std::size_t subtask_idx,
                          const OpSpecSidecar& spec) noexcept {
    try {
        const auto subdir = capture_dir / ("op-" + std::to_string(op.value())) /
                            ("subtask-" + std::to_string(subtask_idx));
        std::filesystem::create_directories(subdir);
        auto esc = [](const std::string& s) {
            std::string out;
            out.reserve(s.size() + 8);
            for (const char c : s) {
                switch (c) {
                    case '"':
                        out += "\\\"";
                        break;
                    case '\\':
                        out += "\\\\";
                        break;
                    case '\n':
                        out += "\\n";
                        break;
                    default:
                        out += c;
                }
            }
            return out;
        };
        std::string json = "{\"op_type\":\"" + esc(spec.op_type) + "\",\"in_channel\":\"" +
                           esc(spec.in_channel) + "\",\"out_channel\":\"" + esc(spec.out_channel) +
                           "\",\"uid\":\"" + esc(spec.uid) + "\",\"params\":{";
        bool first = true;
        for (const auto& [k, v] : spec.params) {
            if (!first) {
                json += ",";
            }
            first = false;
            json += "\"" + esc(k) + "\":\"" + esc(v) + "\"";
        }
        json += "}}";
        std::ofstream out(subdir / "op.json", std::ios::binary | std::ios::trunc);
        out.write(json.data(), static_cast<std::streamsize>(json.size()));
    } catch (const std::exception&) {
        // Best-effort, like every capture write.
    }
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
            if (data_stored_ < max_records_) {
                events_.emplace_back(r);
                ++data_stored_;
            } else {
                truncated_ = true;
            }
        }
    }

    // Control events (watermarks and timer-fire clock positions) ride the
    // same ordered stream as the data. They are small, so they get their
    // own generous budget (4x the record cap) rather than competing with
    // records for slots - a truncated epoch keeps its watermark spine, so
    // a sampled replay still fires windows at the right positions.
    void on_watermark(std::int64_t ts_ms, bool idle) {
        if (disarmed_) {
            return;
        }
        if (control_stored_ < control_cap_()) {
            events_.emplace_back(WatermarkEvent{ts_ms, idle});
            ++control_stored_;
        } else {
            truncated_ = true;
        }
    }

    void on_clock(std::int64_t now_ms) {
        if (disarmed_) {
            return;
        }
        if (control_stored_ < control_cap_()) {
            events_.emplace_back(ClockEvent{now_ms});
            ++control_stored_;
        } else {
            truncated_ = true;
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
        if (events_.empty() && seen_ == 0) {
            return;  // nothing in this epoch; write no empty files
        }
        try {
            const auto subdir = dir_ / ("op-" + std::to_string(op_.value())) /
                                ("subtask-" + std::to_string(subtask_idx_));
            std::filesystem::create_directories(subdir);
            CaptureFileHeader h;
            h.truncated = truncated_;
            h.records_seen = seen_;
            const auto bytes = encode_capture_file(h, events_, *codec_);
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
        events_.clear();
        data_stored_ = 0;
        control_stored_ = 0;
        seen_ = 0;
        truncated_ = false;
    }

    [[nodiscard]] std::size_t control_cap_() const { return max_records_ * 4; }

    std::filesystem::path dir_;
    OperatorId op_;
    std::size_t subtask_idx_;
    std::size_t max_records_;
    std::shared_ptr<const Codec<T>> codec_;
    std::vector<CaptureEvent<T>> events_;
    std::size_t data_stored_{0};
    std::size_t control_stored_{0};
    std::uint64_t seen_{0};
    bool truncated_{false};
    bool disarmed_{false};
};

}  // namespace clink::capture
