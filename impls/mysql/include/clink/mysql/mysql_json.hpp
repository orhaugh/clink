#pragma once

// Decoder for MySQL's internal binary JSON format (the JSONB-like layout stored
// in a JSON column and shipped verbatim in the binlog row image). mariadb_rpl
// hands a JSON column back as raw bytes (it does not convert to text), so the CDC
// source decodes it here to a JSON text string. Pure + header-only; tested both
// with hand-built bytes and against a real MySQL JSON column in the live IT.
//
// Format reference (MySQL sql/json_binary.cc): a document is [type byte][value].
// Containers (object/array) are [count][size][key-entries?][value-entries][keys?]
// [values]; SMALL uses 2-byte and LARGE 4-byte counts/offsets; scalars that fit
// in the offset width (literal, int16/uint16, and int32/uint32 in LARGE) are
// inlined in the value-entry, otherwise the entry holds an offset (relative to
// the container's start) to the value. On any malformed/out-of-bounds input the
// decode returns nullopt (the caller falls back rather than emitting garbage).

#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "clink/config/json.hpp"

namespace clink::mysql::jsonb {

enum Type : std::uint8_t {
    kSmallObject = 0x00,
    kLargeObject = 0x01,
    kSmallArray = 0x02,
    kLargeArray = 0x03,
    kLiteral = 0x04,
    kInt16 = 0x05,
    kUint16 = 0x06,
    kInt32 = 0x07,
    kUint32 = 0x08,
    kInt64 = 0x09,
    kUint64 = 0x0a,
    kDouble = 0x0b,
    kString = 0x0c,
    kOpaque = 0x0f,
};

namespace detail {

inline bool u16(std::string_view d, std::size_t off, std::uint16_t& out) {
    if (off + 2 > d.size()) {
        return false;
    }
    out = static_cast<std::uint16_t>(static_cast<std::uint8_t>(d[off])) |
          static_cast<std::uint16_t>(static_cast<std::uint8_t>(d[off + 1]) << 8);
    return true;
}
inline bool u32(std::string_view d, std::size_t off, std::uint32_t& out) {
    if (off + 4 > d.size()) {
        return false;
    }
    out = 0;
    for (int i = 0; i < 4; ++i) {
        out |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(d[off + i])) << (8 * i);
    }
    return true;
}
inline bool u64(std::string_view d, std::size_t off, std::uint64_t& out) {
    if (off + 8 > d.size()) {
        return false;
    }
    out = 0;
    for (int i = 0; i < 8; ++i) {
        out |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(d[off + i])) << (8 * i);
    }
    return true;
}
inline bool count_at(std::string_view d, std::size_t off, bool large, std::uint32_t& out) {
    if (large) {
        return u32(d, off, out);
    }
    std::uint16_t v = 0;
    if (!u16(d, off, v)) {
        return false;
    }
    out = v;
    return true;
}
inline std::size_t osz(bool large) {
    return large ? 4 : 2;
}

// MySQL var-length integer: little-endian base-128, high bit = continuation.
inline bool varint(std::string_view d, std::size_t& off, std::uint64_t& out) {
    out = 0;
    int shift = 0;
    for (int i = 0; i < 5; ++i) {
        if (off >= d.size()) {
            return false;
        }
        const std::uint8_t b = static_cast<std::uint8_t>(d[off++]);
        out |= static_cast<std::uint64_t>(b & 0x7F) << shift;
        if ((b & 0x80) == 0) {
            return true;
        }
        shift += 7;
    }
    return false;
}

clink::config::JsonValue parse_value(std::string_view doc,
                                     std::size_t pos,
                                     std::uint8_t type,
                                     bool& ok);

inline clink::config::JsonValue parse_scalar(std::string_view doc,
                                             std::size_t pos,
                                             std::uint8_t type,
                                             bool& ok) {
    ok = true;
    switch (type) {
        case kLiteral: {
            if (pos >= doc.size()) {
                break;
            }
            const std::uint8_t lit = static_cast<std::uint8_t>(doc[pos]);
            if (lit == 0) {
                return clink::config::JsonValue{};
            }
            if (lit == 1) {
                return clink::config::JsonValue{true};
            }
            if (lit == 2) {
                return clink::config::JsonValue{false};
            }
            break;
        }
        case kInt16: {
            std::uint16_t v = 0;
            if (!u16(doc, pos, v)) {
                break;
            }
            return clink::config::JsonValue{
                static_cast<std::int64_t>(static_cast<std::int16_t>(v))};
        }
        case kUint16: {
            std::uint16_t v = 0;
            if (!u16(doc, pos, v)) {
                break;
            }
            return clink::config::JsonValue{static_cast<std::int64_t>(v)};
        }
        case kInt32: {
            std::uint32_t v = 0;
            if (!u32(doc, pos, v)) {
                break;
            }
            return clink::config::JsonValue{
                static_cast<std::int64_t>(static_cast<std::int32_t>(v))};
        }
        case kUint32: {
            std::uint32_t v = 0;
            if (!u32(doc, pos, v)) {
                break;
            }
            return clink::config::JsonValue{static_cast<std::int64_t>(v)};
        }
        case kInt64: {
            std::uint64_t v = 0;
            if (!u64(doc, pos, v)) {
                break;
            }
            return clink::config::JsonValue{static_cast<std::int64_t>(v)};
        }
        case kUint64: {
            std::uint64_t v = 0;
            if (!u64(doc, pos, v)) {
                break;
            }
            return clink::config::JsonValue{static_cast<std::int64_t>(v)};
        }
        case kDouble: {
            std::uint64_t bits = 0;
            if (!u64(doc, pos, bits)) {
                break;
            }
            double dv = 0;
            std::memcpy(&dv, &bits, sizeof(dv));
            return clink::config::JsonValue{dv};
        }
        case kString: {
            std::size_t p = pos;
            std::uint64_t len = 0;
            if (!varint(doc, p, len) || p + len > doc.size()) {
                break;
            }
            return clink::config::JsonValue{std::string(doc.substr(p, len))};
        }
        case kOpaque: {
            // [field-type byte][varint len][bytes]: best-effort, emit the raw
            // payload as a JSON string (covers JSON-stored DECIMAL/DATE/etc.).
            std::size_t p = pos;
            if (p >= doc.size()) {
                break;
            }
            ++p;  // skip the opaque field-type byte
            std::uint64_t len = 0;
            if (!varint(doc, p, len) || p + len > doc.size()) {
                break;
            }
            return clink::config::JsonValue{std::string(doc.substr(p, len))};
        }
        default:
            break;
    }
    ok = false;
    return {};
}

// `base` is the offset of the container's count field; all value/key offsets are
// relative to it.
inline clink::config::JsonValue parse_container(
    std::string_view doc, std::size_t base, bool large, bool is_object, bool& ok) {
    ok = true;
    const std::size_t o = osz(large);
    std::uint32_t count = 0;
    std::uint32_t size = 0;
    if (!count_at(doc, base, large, count) || !count_at(doc, base + o, large, size)) {
        ok = false;
        return {};
    }
    if (base + size > doc.size()) {
        ok = false;
        return {};
    }
    std::size_t p = base + 2 * o;

    struct KeyRef {
        std::uint32_t off;
        std::uint16_t len;
    };
    std::vector<KeyRef> keys;
    if (is_object) {
        keys.reserve(count);
        for (std::uint32_t i = 0; i < count; ++i) {
            std::uint32_t koff = 0;
            std::uint16_t klen = 0;
            if (!count_at(doc, p, large, koff)) {
                ok = false;
                return {};
            }
            p += o;
            if (!u16(doc, p, klen)) {
                ok = false;
                return {};
            }
            p += 2;
            keys.push_back({koff, klen});
        }
    }

    struct ValRef {
        std::uint8_t type;
        std::size_t at;  // inlined: position of the inline field; else base+offset
        bool inlined;
    };
    std::vector<ValRef> vals;
    vals.reserve(count);
    for (std::uint32_t i = 0; i < count; ++i) {
        if (p >= doc.size()) {
            ok = false;
            return {};
        }
        const std::uint8_t vt = static_cast<std::uint8_t>(doc[p]);
        ++p;
        const bool inl = (vt == kLiteral || vt == kInt16 || vt == kUint16 ||
                          (large && (vt == kInt32 || vt == kUint32)));
        ValRef vr{};
        vr.type = vt;
        vr.inlined = inl;
        if (inl) {
            vr.at = p;  // the value bytes live in the o-byte entry field
            p += o;
        } else {
            std::uint32_t voff = 0;
            if (!count_at(doc, p, large, voff)) {
                ok = false;
                return {};
            }
            p += o;
            vr.at = base + voff;
        }
        vals.push_back(vr);
    }

    auto parse_entry = [&](const ValRef& vr, bool& vok) -> clink::config::JsonValue {
        if (vr.inlined) {
            return parse_scalar(doc, vr.at, vr.type, vok);
        }
        return parse_value(doc, vr.at, vr.type, vok);
    };

    if (is_object) {
        clink::config::JsonObject obj;
        for (std::uint32_t i = 0; i < count; ++i) {
            if (base + keys[i].off + keys[i].len > doc.size()) {
                ok = false;
                return {};
            }
            std::string key(doc.substr(base + keys[i].off, keys[i].len));
            bool vok = true;
            clink::config::JsonValue v = parse_entry(vals[i], vok);
            if (!vok) {
                ok = false;
                return {};
            }
            obj[std::move(key)] = std::move(v);
        }
        return clink::config::JsonValue{std::move(obj)};
    }
    clink::config::JsonArray arr;
    for (std::uint32_t i = 0; i < count; ++i) {
        bool vok = true;
        clink::config::JsonValue v = parse_entry(vals[i], vok);
        if (!vok) {
            ok = false;
            return {};
        }
        arr.push_back(std::move(v));
    }
    return clink::config::JsonValue{std::move(arr)};
}

inline clink::config::JsonValue parse_value(std::string_view doc,
                                            std::size_t pos,
                                            std::uint8_t type,
                                            bool& ok) {
    switch (type) {
        case kSmallObject:
            return parse_container(doc, pos, false, true, ok);
        case kLargeObject:
            return parse_container(doc, pos, true, true, ok);
        case kSmallArray:
            return parse_container(doc, pos, false, false, ok);
        case kLargeArray:
            return parse_container(doc, pos, true, false, ok);
        default:
            return parse_scalar(doc, pos, type, ok);
    }
}

}  // namespace detail

// Decode a MySQL binary-JSON column blob ([type byte][value]) to JSON text.
// Returns nullopt on malformed input.
inline std::optional<std::string> decode(std::string_view blob) {
    if (blob.empty()) {
        return std::nullopt;
    }
    bool ok = true;
    clink::config::JsonValue v =
        detail::parse_value(blob, 1, static_cast<std::uint8_t>(blob[0]), ok);
    if (!ok) {
        return std::nullopt;
    }
    return v.serialize(0);
}

}  // namespace clink::mysql::jsonb
