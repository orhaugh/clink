#pragma once

// Codec<T> derived from a CLINK_ARROW_FIELDS description (design record
// 009, increment 1): the field list the columnar batcher already consumes
// also yields the byte-level codec, so a described struct no longer needs
// a hand-written encode/decode pair for state values and the row wire.
//
// THE ENCODING IS A DURABILITY CONTRACT. State persisted through this
// codec must stay readable by every later build, so the layout is
// specified here, pinned by a frozen-bytes fixture
// (tests/fixtures/derived-codec-v1.bin, exercised by
// tests/test_format_fixtures.cpp), and inventoried in
// docs/internals/protocol-compatibility.md. Fields encode in DECLARED
// ORDER, so reordering a CLINK_ARROW_FIELDS list rewrites the bytes:
// that is a schema change and must be treated as one (bump
// SchemaVersionTrait<T> and register a migration).
//
// Layout, version 1 (matches the hand-written idiom the examples use, so
// an idiomatic hand codec migrates byte-identically):
//
//   fixed-width integers   little-endian, natural width
//   bool                   one byte, 0 or 1 (decode refuses other values)
//   float / double         the same-width integer bit pattern, LE
//   std::string            u32 LE length, then the bytes (embedded NULs fine)
//   std::optional<E>       one presence byte (0 or 1), then E when present
//   std::vector<E>         u32 LE count, then the elements
//   std::map<K, V>         u32 LE count, then key/value pairs in key order
//   nested described T     its fields, recursively, in declared order
//
// Strictness (deliberate, stricter than the tolerant hand codecs):
//   * decode consumes the buffer EXACTLY; trailing bytes fail (nullopt)
//   * a length or count that would exceed u32 throws std::length_error
//     at encode rather than silently wrapping
//   * std::vector<bool> is refused at compile time (proxy references);
//     store std::vector<std::uint8_t> instead
//
// encode() is implemented VIA encode_into(), so the two are identical by
// construction (the append contract in codec.hpp).

#include <bit>
#include <cstdint>
#include <limits>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <tuple>
#include <type_traits>
#include <vector>

#include "clink/core/codec.hpp"
#include "clink/core/fields.hpp"

namespace clink {

namespace derived_codec_detail {

using Bytes = std::vector<std::byte>;
using BytesView = std::span<const std::byte>;

template <typename>
struct is_vector : std::false_type {};
template <typename E, typename A>
struct is_vector<std::vector<E, A>> : std::true_type {};

template <typename>
struct is_optional : std::false_type {};
template <typename E>
struct is_optional<std::optional<E>> : std::true_type {};

template <typename>
struct is_map : std::false_type {};
template <typename K, typename V, typename C, typename A>
struct is_map<std::map<K, V, C, A>> : std::true_type {};

// The same leaf universe as the columnar path (columnar_batcher.hpp), by
// the same stdint aliases, so a type either works in both representations
// or in neither.
template <typename F>
constexpr bool is_supported_int() {
    return std::is_same_v<F, std::int8_t> || std::is_same_v<F, std::int16_t> ||
           std::is_same_v<F, std::int32_t> || std::is_same_v<F, std::int64_t> ||
           std::is_same_v<F, std::uint8_t> || std::is_same_v<F, std::uint16_t> ||
           std::is_same_v<F, std::uint32_t> || std::is_same_v<F, std::uint64_t>;
}

template <typename UInt>
inline void put_uint_le(Bytes& out, UInt v) {
    for (std::size_t i = 0; i < sizeof(UInt); ++i) {
        out.push_back(static_cast<std::byte>((v >> (i * 8)) & 0xFF));
    }
}

template <typename UInt>
inline bool read_uint_le(BytesView b, std::size_t& pos, UInt& v) {
    if (pos + sizeof(UInt) > b.size()) {
        return false;
    }
    v = 0;
    for (std::size_t i = 0; i < sizeof(UInt); ++i) {
        v |= static_cast<UInt>(static_cast<unsigned char>(b[pos + i])) << (i * 8);
    }
    pos += sizeof(UInt);
    return true;
}

inline void put_len(Bytes& out, std::size_t n) {
    if (n > std::numeric_limits<std::uint32_t>::max()) {
        throw std::length_error(
            "clink: derived codec: a string/vector/map exceeds the u32 length the "
            "format carries; wrapping would silently corrupt state");
    }
    put_uint_le<std::uint32_t>(out, static_cast<std::uint32_t>(n));
}

template <typename T>
void encode_record(const T& v, Bytes& out);
template <typename T>
bool decode_record(T& v, BytesView b, std::size_t& pos);

template <typename F>
void encode_value(const F& v, Bytes& out) {
    if constexpr (std::is_same_v<F, bool>) {
        out.push_back(std::byte{v ? std::uint8_t{1} : std::uint8_t{0}});
    } else if constexpr (is_supported_int<F>()) {
        using U = std::make_unsigned_t<F>;
        put_uint_le<U>(out, static_cast<U>(v));
    } else if constexpr (std::is_same_v<F, float>) {
        put_uint_le<std::uint32_t>(out, std::bit_cast<std::uint32_t>(v));
    } else if constexpr (std::is_same_v<F, double>) {
        put_uint_le<std::uint64_t>(out, std::bit_cast<std::uint64_t>(v));
    } else if constexpr (std::is_same_v<F, std::string>) {
        put_len(out, v.size());
        const auto* p = reinterpret_cast<const std::byte*>(v.data());
        out.insert(out.end(), p, p + v.size());
    } else if constexpr (is_optional<F>::value) {
        out.push_back(std::byte{v.has_value() ? std::uint8_t{1} : std::uint8_t{0}});
        if (v.has_value()) {
            encode_value<typename F::value_type>(*v, out);
        }
    } else if constexpr (is_vector<F>::value) {
        static_assert(!std::is_same_v<typename F::value_type, bool>,
                      "clink: derived codec: std::vector<bool> is a bit-packed proxy "
                      "container; store std::vector<std::uint8_t> instead");
        put_len(out, v.size());
        for (const auto& e : v) {
            encode_value<typename F::value_type>(e, out);
        }
    } else if constexpr (is_map<F>::value) {
        put_len(out, v.size());
        for (const auto& [k, mv] : v) {
            encode_value<typename F::key_type>(k, out);
            encode_value<typename F::mapped_type>(mv, out);
        }
    } else if constexpr (HasArrowFields<F>) {
        encode_record<F>(v, out);
    } else {
        static_assert(sizeof(F) == 0,
                      "clink: derived codec: unsupported field type. Supported: fixed-width "
                      "integers (8/16/32/64, signed + unsigned), float, double, bool, "
                      "std::string (leaves); std::optional<E>, std::vector<E>, std::map<K,V>, "
                      "and nested CLINK_ARROW_FIELDS structs (composites).");
    }
}

// A presence/bool byte is 0 or 1 by construction; anything else is not a
// value this codec wrote, so decoding it fails closed.
inline bool read_flag_byte(BytesView b, std::size_t& pos, bool& v) {
    std::uint8_t raw = 0;
    if (!read_uint_le<std::uint8_t>(b, pos, raw) || raw > 1) {
        return false;
    }
    v = (raw == 1);
    return true;
}

template <typename F>
bool decode_value(F& v, BytesView b, std::size_t& pos) {
    if constexpr (std::is_same_v<F, bool>) {
        return read_flag_byte(b, pos, v);
    } else if constexpr (is_supported_int<F>()) {
        using U = std::make_unsigned_t<F>;
        U u = 0;
        if (!read_uint_le<U>(b, pos, u)) {
            return false;
        }
        v = static_cast<F>(u);
        return true;
    } else if constexpr (std::is_same_v<F, float>) {
        std::uint32_t u = 0;
        if (!read_uint_le<std::uint32_t>(b, pos, u)) {
            return false;
        }
        v = std::bit_cast<float>(u);
        return true;
    } else if constexpr (std::is_same_v<F, double>) {
        std::uint64_t u = 0;
        if (!read_uint_le<std::uint64_t>(b, pos, u)) {
            return false;
        }
        v = std::bit_cast<double>(u);
        return true;
    } else if constexpr (std::is_same_v<F, std::string>) {
        std::uint32_t len = 0;
        if (!read_uint_le<std::uint32_t>(b, pos, len) || pos + len > b.size()) {
            return false;
        }
        v.assign(reinterpret_cast<const char*>(b.data() + pos), len);
        pos += len;
        return true;
    } else if constexpr (is_optional<F>::value) {
        bool present = false;
        if (!read_flag_byte(b, pos, present)) {
            return false;
        }
        if (!present) {
            v.reset();
            return true;
        }
        typename F::value_type inner{};
        if (!decode_value<typename F::value_type>(inner, b, pos)) {
            return false;
        }
        v = std::move(inner);
        return true;
    } else if constexpr (is_vector<F>::value) {
        std::uint32_t n = 0;
        if (!read_uint_le<std::uint32_t>(b, pos, n)) {
            return false;
        }
        v.clear();
        // Bound the reserve by the bytes that could possibly remain, not by
        // the untrusted count (the item-85 lesson).
        v.reserve(std::min<std::size_t>(n, b.size() - pos));
        for (std::uint32_t i = 0; i < n; ++i) {
            typename F::value_type e{};
            if (!decode_value<typename F::value_type>(e, b, pos)) {
                return false;
            }
            v.push_back(std::move(e));
        }
        return true;
    } else if constexpr (is_map<F>::value) {
        std::uint32_t n = 0;
        if (!read_uint_le<std::uint32_t>(b, pos, n)) {
            return false;
        }
        v.clear();
        for (std::uint32_t i = 0; i < n; ++i) {
            typename F::key_type k{};
            typename F::mapped_type mv{};
            if (!decode_value<typename F::key_type>(k, b, pos) ||
                !decode_value<typename F::mapped_type>(mv, b, pos)) {
                return false;
            }
            v.emplace(std::move(k), std::move(mv));
        }
        return true;
    } else if constexpr (HasArrowFields<F>) {
        return decode_record<F>(v, b, pos);
    } else {
        static_assert(sizeof(F) == 0, "unreachable: encode_value rejects this first");
        return false;
    }
}

template <typename T>
void encode_record(const T& v, Bytes& out) {
    std::apply([&](const auto&... d) { (encode_value(v.*(d.member), out), ...); },
               ArrowFields<T>::descriptors());
}

template <typename T>
bool decode_record(T& v, BytesView b, std::size_t& pos) {
    return std::apply(
        [&](const auto&... d) { return (decode_value(v.*(d.member), b, pos) && ...); },
        ArrowFields<T>::descriptors());
}

}  // namespace derived_codec_detail

// The derived codec for a described type. See the header comment for the
// layout contract and strictness rules.
template <typename T>
    requires HasArrowFields<T>
Codec<T> derived_codec() {
    static_assert(std::is_default_constructible_v<T>,
                  "clink: derived codec: decode default-constructs T then assigns its "
                  "fields, so T must be default-constructible (the same constraint the "
                  "columnar parse path imposes)");
    return Codec<T>{.encode =
                        [](const T& v) {
                            typename Codec<T>::Bytes out;
                            out.reserve(64);
                            derived_codec_detail::encode_record<T>(v, out);
                            return out;
                        },
                    .decode = [](typename Codec<T>::BytesView b) -> std::optional<T> {
                        T v{};
                        std::size_t pos = 0;
                        if (!derived_codec_detail::decode_record<T>(v, b, pos) || pos != b.size()) {
                            return std::nullopt;
                        }
                        return v;
                    },
                    .encode_into =
                        [](const T& v, typename Codec<T>::Bytes& out) {
                            derived_codec_detail::encode_record<T>(v, out);
                        }};
}

}  // namespace clink
