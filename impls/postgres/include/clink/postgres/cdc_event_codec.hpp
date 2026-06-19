// cdc_event_codec - byte-level Codec<CdcEvent> used to register the
// typed channel `postgres.cdc_event` so clink pipelines can carry
// each logical-decoding row change through the cluster as a typed
// record (op + table + lsn + xid + per-column fields with type info
// and is_null) instead of a JSON-flattened string.
//
// Wire format (little-endian, lengths uint32):
//   [u8  op]            -- CdcEvent::Op enum value
//   [u32 table_len]     table_bytes
//   [u32 lsn_len]       lsn_bytes
//   [i64 xid]
//   [u32 field_count]
//     repeated:
//       [u32 name_len] name_bytes
//       [u32 value_len] value_bytes
//       [u32 type_len] type_bytes
//       [u8  is_null]

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "clink/connectors/cdc_event.hpp"
#include "clink/core/codec.hpp"

namespace clink {

namespace detail {

inline void cdc_append_u32(Codec<CdcEvent>::Bytes& out, std::uint32_t v) {
    for (int i = 0; i < 4; ++i) {
        out.push_back(static_cast<std::byte>((v >> (i * 8)) & 0xFF));
    }
}

inline void cdc_append_i64(Codec<CdcEvent>::Bytes& out, std::int64_t v) {
    const auto u = static_cast<std::uint64_t>(v);
    for (int i = 0; i < 8; ++i) {
        out.push_back(static_cast<std::byte>((u >> (i * 8)) & 0xFF));
    }
}

inline void cdc_append_string(Codec<CdcEvent>::Bytes& out, const std::string& s) {
    cdc_append_u32(out, static_cast<std::uint32_t>(s.size()));
    const auto* p = reinterpret_cast<const std::byte*>(s.data());
    out.insert(out.end(), p, p + s.size());
}

inline bool cdc_read_u32(Codec<CdcEvent>::BytesView buf, std::size_t& pos, std::uint32_t& out) {
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

inline bool cdc_read_i64(Codec<CdcEvent>::BytesView buf, std::size_t& pos, std::int64_t& out) {
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

inline bool cdc_read_string(Codec<CdcEvent>::BytesView buf, std::size_t& pos, std::string& out) {
    std::uint32_t len = 0;
    if (!cdc_read_u32(buf, pos, len)) {
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

inline Codec<CdcEvent> cdc_event_codec() {
    return Codec<CdcEvent>{
        .encode =
            [](const CdcEvent& ev) {
                Codec<CdcEvent>::Bytes out;
                out.push_back(static_cast<std::byte>(static_cast<std::uint8_t>(ev.op)));
                detail::cdc_append_string(out, ev.table);
                detail::cdc_append_string(out, ev.lsn);
                detail::cdc_append_i64(out, ev.xid);
                detail::cdc_append_u32(out, static_cast<std::uint32_t>(ev.values.size()));
                for (const auto& f : ev.values) {
                    detail::cdc_append_string(out, f.name);
                    detail::cdc_append_string(out, f.value);
                    detail::cdc_append_string(out, f.type);
                    out.push_back(static_cast<std::byte>(f.is_null ? 1 : 0));
                }
                return out;
            },
        .decode = [](Codec<CdcEvent>::BytesView buf) -> std::optional<CdcEvent> {
            CdcEvent ev;
            std::size_t pos = 0;
            if (pos + 1 > buf.size()) {
                return std::nullopt;
            }
            const auto op_raw = static_cast<std::uint8_t>(buf[pos]);
            pos += 1;
            // Anything outside the enum range is normalized to Unknown
            // so a future producer adding new op values doesn't crash
            // older decoders.
            if (op_raw > static_cast<std::uint8_t>(CdcEvent::Op::Unknown)) {
                ev.op = CdcEvent::Op::Unknown;
            } else {
                ev.op = static_cast<CdcEvent::Op>(op_raw);
            }
            if (!detail::cdc_read_string(buf, pos, ev.table) ||
                !detail::cdc_read_string(buf, pos, ev.lsn) ||
                !detail::cdc_read_i64(buf, pos, ev.xid)) {
                return std::nullopt;
            }
            std::uint32_t count = 0;
            if (!detail::cdc_read_u32(buf, pos, count)) {
                return std::nullopt;
            }
            ev.values.reserve(count);
            for (std::uint32_t i = 0; i < count; ++i) {
                CdcField f;
                if (!detail::cdc_read_string(buf, pos, f.name) ||
                    !detail::cdc_read_string(buf, pos, f.value) ||
                    !detail::cdc_read_string(buf, pos, f.type)) {
                    return std::nullopt;
                }
                if (pos + 1 > buf.size()) {
                    return std::nullopt;
                }
                f.is_null = static_cast<unsigned char>(buf[pos]) != 0;
                pos += 1;
                ev.values.push_back(std::move(f));
            }
            return ev;
        }};
}

// Channel name used when register_type<CdcEvent>() is called by the
// Postgres impl's install().
inline constexpr const char* kChannelPostgresCdcEvent = "postgres.cdc_event";

}  // namespace clink
