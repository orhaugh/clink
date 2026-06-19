// kafka_message_codec - byte-level Codec<KafkaMessage> used to register
// the typed channel `kafka.message` so clink pipelines can carry the
// full broker record (payload + key + headers + offset + partition +
// timestamp) end-to-end without flattening to std::string at the
// connector boundary.
//
// Wire format (all integers little-endian, lengths uint32):
//   [u32 payload_len] payload_bytes
//   [u8 has_key] [u32 key_len key_bytes]   -- present iff has_key=1
//   [u32 headers_count]
//     repeated:
//       [u32 hdr_key_len] hdr_key_bytes
//       [u32 hdr_value_len] hdr_value_bytes
//   [i64 offset]
//   [i32 partition]
//   [i64 timestamp_ms]
//
// All strings are raw byte buffers (no null terminators, no
// transcoding). Header values may contain NULs; the length prefix is
// the source of truth.

#pragma once

#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <vector>

#include "clink/connectors/kafka_message.hpp"
#include "clink/core/codec.hpp"

namespace clink {

namespace detail {

inline void append_u32(Codec<KafkaMessage>::Bytes& out, std::uint32_t v) {
    for (int i = 0; i < 4; ++i) {
        out.push_back(static_cast<std::byte>((v >> (i * 8)) & 0xFF));
    }
}

inline void append_i64(Codec<KafkaMessage>::Bytes& out, std::int64_t v) {
    const auto u = static_cast<std::uint64_t>(v);
    for (int i = 0; i < 8; ++i) {
        out.push_back(static_cast<std::byte>((u >> (i * 8)) & 0xFF));
    }
}

inline void append_i32(Codec<KafkaMessage>::Bytes& out, std::int32_t v) {
    const auto u = static_cast<std::uint32_t>(v);
    append_u32(out, u);
}

inline void append_string(Codec<KafkaMessage>::Bytes& out, const std::string& s) {
    append_u32(out, static_cast<std::uint32_t>(s.size()));
    const auto* p = reinterpret_cast<const std::byte*>(s.data());
    out.insert(out.end(), p, p + s.size());
}

inline bool read_u32(Codec<KafkaMessage>::BytesView buf, std::size_t& pos, std::uint32_t& out) {
    if (pos + 4 > buf.size()) {
        return false;
    }
    out = 0;
    for (int i = 0; i < 4; ++i) {
        out |= static_cast<std::uint32_t>(static_cast<unsigned char>(buf[pos + i])) << (i * 8);
    }
    pos += 4;
    return true;
}

inline bool read_i32(Codec<KafkaMessage>::BytesView buf, std::size_t& pos, std::int32_t& out) {
    std::uint32_t u = 0;
    if (!read_u32(buf, pos, u)) {
        return false;
    }
    out = static_cast<std::int32_t>(u);
    return true;
}

inline bool read_i64(Codec<KafkaMessage>::BytesView buf, std::size_t& pos, std::int64_t& out) {
    if (pos + 8 > buf.size()) {
        return false;
    }
    std::uint64_t u = 0;
    for (int i = 0; i < 8; ++i) {
        u |= static_cast<std::uint64_t>(static_cast<unsigned char>(buf[pos + i])) << (i * 8);
    }
    pos += 8;
    out = static_cast<std::int64_t>(u);
    return true;
}

inline bool read_string(Codec<KafkaMessage>::BytesView buf, std::size_t& pos, std::string& out) {
    std::uint32_t len = 0;
    if (!read_u32(buf, pos, len)) {
        return false;
    }
    if (pos + len > buf.size()) {
        return false;
    }
    out.assign(reinterpret_cast<const char*>(buf.data() + pos), len);
    pos += len;
    return true;
}

}  // namespace detail

inline Codec<KafkaMessage> kafka_message_codec() {
    return Codec<KafkaMessage>{
        .encode =
            [](const KafkaMessage& m) {
                Codec<KafkaMessage>::Bytes out;
                out.reserve(64 + m.payload.size());
                detail::append_string(out, m.payload);
                out.push_back(static_cast<std::byte>(m.key.has_value() ? 1 : 0));
                if (m.key.has_value()) {
                    detail::append_string(out, *m.key);
                }
                detail::append_u32(out, static_cast<std::uint32_t>(m.headers.size()));
                for (const auto& h : m.headers) {
                    detail::append_string(out, h.key);
                    detail::append_string(out, h.value);
                }
                detail::append_i64(out, m.offset);
                detail::append_i32(out, m.partition);
                detail::append_i64(out, m.timestamp_ms);
                return out;
            },
        .decode = [](Codec<KafkaMessage>::BytesView buf) -> std::optional<KafkaMessage> {
            KafkaMessage m;
            std::size_t pos = 0;
            if (!detail::read_string(buf, pos, m.payload)) {
                return std::nullopt;
            }
            if (pos + 1 > buf.size()) {
                return std::nullopt;
            }
            const auto has_key = static_cast<unsigned char>(buf[pos]);
            pos += 1;
            if (has_key != 0) {
                std::string k;
                if (!detail::read_string(buf, pos, k)) {
                    return std::nullopt;
                }
                m.key = std::move(k);
            }
            std::uint32_t hdr_count = 0;
            if (!detail::read_u32(buf, pos, hdr_count)) {
                return std::nullopt;
            }
            m.headers.reserve(hdr_count);
            for (std::uint32_t i = 0; i < hdr_count; ++i) {
                KafkaHeader h;
                if (!detail::read_string(buf, pos, h.key) ||
                    !detail::read_string(buf, pos, h.value)) {
                    return std::nullopt;
                }
                m.headers.push_back(std::move(h));
            }
            if (!detail::read_i64(buf, pos, m.offset) || !detail::read_i32(buf, pos, m.partition) ||
                !detail::read_i64(buf, pos, m.timestamp_ms)) {
                return std::nullopt;
            }
            return m;
        }};
}

// Channel name used when register_type<KafkaMessage>() is called by the
// Kafka impl's install(). Stable string so plugins can reference it.
inline constexpr const char* kChannelKafkaMessage = "kafka.message";

}  // namespace clink
