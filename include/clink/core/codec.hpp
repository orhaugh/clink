#pragma once

#include <cstdint>
#include <cstring>
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace clink {

// Codec<T> is the byte-level serialization seam: a pair of callables that
// map between T and a contiguous byte buffer. It mirrors TextFormat<T> but
// is binary-oriented and used by the keyed-state machinery.
//
// Implementations are intentionally just std::function pairs so users can
// supply lambdas at the call site without writing class hierarchies. A
// future revision may add a concept-based static-dispatch flavour for hot
// paths.
template <typename T>
struct Codec {
    using Bytes = std::vector<std::byte>;
    using BytesView = std::span<const std::byte>;
    using Encoder = std::function<Bytes(const T&)>;
    using Decoder = std::function<std::optional<T>(BytesView)>;

    // Optional encode-into-buffer variant. When set, callers on the hot
    // path can pass a caller-owned (thread_local) scratch buffer and
    // avoid the per-record allocation that the returning-by-value Encoder
    // shape forces. KeyedState::put preferentially uses this over encode()
    // when populated.
    //
    // CONTRACT: encode_into APPENDS exactly encode(v)'s bytes to `out` (it
    // does NOT clear out). The caller clears once before the top-level call
    // (KeyedState::put does); combinators then append inner codecs into the
    // SAME buffer with no temporaries (see encode_append / encode_append_lp).
    // The appended bytes MUST be identical to encode(v) - a divergence
    // silently corrupts state on restore. (KeyedState::put clears first, so a
    // legacy overwrite-style impl still works standalone, but it would clobber
    // a prefix if composed - new impls must append.)
    using EncoderInto = std::function<void(const T&, Bytes&)>;

    Encoder encode;
    Decoder decode;
    EncoderInto encode_into;
};

// Append v's encoding to `out` (does NOT clear out - the APPEND contract that
// lets combinators compose without temp buffers). Zero-alloc when the codec
// populates encode_into; otherwise falls back to encode() + a copy. The
// produced bytes are byte-identical to encode(v) either way.
template <typename T>
inline void encode_append(const Codec<T>& c, const T& v, typename Codec<T>::Bytes& out) {
    if (c.encode_into) {
        c.encode_into(v, out);
    } else {
        auto bytes = c.encode(v);
        out.insert(out.end(), bytes.begin(), bytes.end());
    }
}

// Append a 4-byte-little-endian length prefix followed by v's encoding,
// matching the [u32 len][bytes] element framing the container codecs use.
// The length is back-patched after the inner encoding is appended, so this
// stays alloc-free even for variable-length inner codecs.
template <typename T>
inline void encode_append_lp(const Codec<T>& c, const T& v, typename Codec<T>::Bytes& out) {
    const std::size_t len_off = out.size();
    out.push_back(std::byte{0});
    out.push_back(std::byte{0});
    out.push_back(std::byte{0});
    out.push_back(std::byte{0});
    const std::size_t start = out.size();
    encode_append(c, v, out);
    const auto len = static_cast<std::uint32_t>(out.size() - start);
    for (int i = 0; i < 4; ++i) {
        out[len_off + static_cast<std::size_t>(i)] =
            static_cast<std::byte>((len >> (i * 8)) & 0xFF);
    }
}

// ---------------------------------------------------------------------------
// Built-in codecs for common types
// ---------------------------------------------------------------------------

inline Codec<std::string> string_codec() {
    return Codec<std::string>{
        .encode =
            [](const std::string& s) {
                Codec<std::string>::Bytes out(s.size());
                if (!s.empty()) {
                    std::memcpy(out.data(), s.data(), s.size());
                }
                return out;
            },
        .decode = [](Codec<std::string>::BytesView b) -> std::optional<std::string> {
            std::string out(b.size(), '\0');
            if (!b.empty()) {
                std::memcpy(out.data(), b.data(), b.size());
            }
            return out;
        },
        .encode_into =
            [](const std::string& s, Codec<std::string>::Bytes& out) {
                const auto* p = reinterpret_cast<const std::byte*>(s.data());
                out.insert(out.end(), p, p + s.size());
            }};
}

namespace detail {

// Shared 4-byte little-endian count/length-prefix helpers for the container
// codecs (vector, set, map, unordered_*). Defined here (before the container
// codecs) so both their encode/decode and encode_into can use them.
template <typename Bytes>
inline void append_u32_le(Bytes& out, std::uint32_t v) {
    for (int i = 0; i < 4; ++i) {
        out.push_back(static_cast<std::byte>((v >> (i * 8)) & 0xFF));
    }
}

template <typename BytesView>
inline std::optional<std::uint32_t> read_u32_le(BytesView buf, std::size_t pos) {
    if (pos + 4 > buf.size()) {
        return std::nullopt;
    }
    std::uint32_t v = 0;
    for (int i = 0; i < 4; ++i) {
        v |= static_cast<std::uint32_t>(static_cast<unsigned char>(buf[pos + i])) << (i * 8);
    }
    return v;
}

// Little-endian fixed-width integer codec. Stable across platforms because
// we encode/decode byte-by-byte rather than relying on host endianness.
template <typename UInt>
inline Codec<UInt> le_uint_codec() {
    return Codec<UInt>{.encode =
                           [](const UInt& v) {
                               typename Codec<UInt>::Bytes out(sizeof(UInt));
                               for (std::size_t i = 0; i < sizeof(UInt); ++i) {
                                   out[i] = static_cast<std::byte>((v >> (i * 8)) & 0xFF);
                               }
                               return out;
                           },
                       .decode = [](typename Codec<UInt>::BytesView b) -> std::optional<UInt> {
                           if (b.size() != sizeof(UInt)) {
                               return std::nullopt;
                           }
                           UInt v = 0;
                           for (std::size_t i = 0; i < sizeof(UInt); ++i) {
                               v |= static_cast<UInt>(static_cast<unsigned char>(b[i])) << (i * 8);
                           }
                           return v;
                       },
                       .encode_into =
                           [](const UInt& v, typename Codec<UInt>::Bytes& out) {
                               for (std::size_t i = 0; i < sizeof(UInt); ++i) {
                                   out.push_back(static_cast<std::byte>((v >> (i * 8)) & 0xFF));
                               }
                           }};
}

}  // namespace detail

inline Codec<std::uint64_t> uint64_codec() {
    return detail::le_uint_codec<std::uint64_t>();
}
inline Codec<std::uint32_t> uint32_codec() {
    return detail::le_uint_codec<std::uint32_t>();
}

inline Codec<std::int64_t> int64_codec() {
    return Codec<std::int64_t>{
        .encode = [u = uint64_codec()](
                      const std::int64_t& v) { return u.encode(static_cast<std::uint64_t>(v)); },
        .decode =
            [u = uint64_codec()](Codec<std::int64_t>::BytesView b) -> std::optional<std::int64_t> {
            auto x = u.decode(b);
            if (!x.has_value()) {
                return std::nullopt;
            }
            return static_cast<std::int64_t>(*x);
        },
        .encode_into =
            [u = uint64_codec()](const std::int64_t& v, Codec<std::int64_t>::Bytes& out) {
                encode_append(u, static_cast<std::uint64_t>(v), out);
            }};
}

// Reflection-light fallback codec for trivially-copyable structs.
// Clink's the Kryo/POJO fallback: when the user has
// a plain-data struct (no pointers, no virtuals, no heap-allocated
// members), trivial_codec<T> memcpy's it into bytes without needing a
// hand-rolled encoder. The is_trivially_copyable_v constraint rejects
// types that would silently corrupt under memcpy (anything owning heap
// storage like std::string, std::vector, std::unique_ptr).
//
// Caveats vs the hand-rolled codecs above:
//   * Host-endianness. The other codecs serialise byte-by-byte for
//     stable cross-host wire format; trivial_codec is host-native. Safe
//     for within-process state-backend persistence on a single host,
//     not safe to ship bytes between machines of different endianness.
//   * Padding bytes. memcpy copies uninitialized padding too - make
//     sure the struct is value-initialised before encode if those bytes
//     might leak via e.g. a state-backend dump.
template <typename T>
    requires std::is_trivially_copyable_v<T>
inline Codec<T> trivial_codec() {
    return Codec<T>{.encode =
                        [](const T& v) {
                            typename Codec<T>::Bytes out(sizeof(T));
                            std::memcpy(out.data(), &v, sizeof(T));
                            return out;
                        },
                    .decode = [](typename Codec<T>::BytesView b) -> std::optional<T> {
                        if (b.size() != sizeof(T)) {
                            return std::nullopt;
                        }
                        T v;
                        std::memcpy(&v, b.data(), sizeof(T));
                        return v;
                    },
                    .encode_into =
                        [](const T& v, typename Codec<T>::Bytes& out) {
                            const auto off = out.size();
                            out.resize(off + sizeof(T));
                            std::memcpy(out.data() + off, &v, sizeof(T));
                        }};
}

// Vector codec: 4-byte count, then per element a 4-byte length followed by
// that element's bytes. Lets us serialize variable-length user values
// without forcing every codec to be fixed-width.
template <typename T>
inline Codec<std::vector<T>> vector_codec(Codec<T> elem) {
    return Codec<std::vector<T>>{
        .encode =
            [elem](const std::vector<T>& v) {
                typename Codec<std::vector<T>>::Bytes out;
                const std::uint32_t count = static_cast<std::uint32_t>(v.size());
                for (int i = 0; i < 4; ++i) {
                    out.push_back(static_cast<std::byte>((count >> (i * 8)) & 0xFF));
                }
                for (const auto& e : v) {
                    auto bytes = elem.encode(e);
                    const std::uint32_t len = static_cast<std::uint32_t>(bytes.size());
                    for (int i = 0; i < 4; ++i) {
                        out.push_back(static_cast<std::byte>((len >> (i * 8)) & 0xFF));
                    }
                    out.insert(out.end(), bytes.begin(), bytes.end());
                }
                return out;
            },
        .decode =
            [elem](typename Codec<std::vector<T>>::BytesView buf) -> std::optional<std::vector<T>> {
            if (buf.size() < 4) {
                return std::nullopt;
            }
            std::uint32_t count = 0;
            for (int i = 0; i < 4; ++i) {
                count |= static_cast<std::uint32_t>(static_cast<unsigned char>(buf[i])) << (i * 8);
            }
            std::vector<T> out;
            out.reserve(count);
            std::size_t pos = 4;
            for (std::uint32_t i = 0; i < count; ++i) {
                if (pos + 4 > buf.size()) {
                    return std::nullopt;
                }
                std::uint32_t len = 0;
                for (int j = 0; j < 4; ++j) {
                    len |= static_cast<std::uint32_t>(static_cast<unsigned char>(buf[pos + j]))
                           << (j * 8);
                }
                pos += 4;
                if (pos + len > buf.size()) {
                    return std::nullopt;
                }
                auto entry = elem.decode(buf.subspan(pos, len));
                if (!entry.has_value()) {
                    return std::nullopt;
                }
                out.push_back(std::move(*entry));
                pos += len;
            }
            return out;
        },
        .encode_into =
            [elem](const std::vector<T>& v, typename Codec<std::vector<T>>::Bytes& out) {
                detail::append_u32_le(out, static_cast<std::uint32_t>(v.size()));
                for (const auto& e : v) {
                    encode_append_lp(elem, e, out);
                }
            }};
}

// Pair codec: 4-byte length-prefix on the first component, then the second.
// Useful for window-state keys that combine (window_start, user_key).
template <typename A, typename B>
inline Codec<std::pair<A, B>> pair_codec(Codec<A> a, Codec<B> b) {
    return Codec<std::pair<A, B>>{
        .encode =
            [a, b](const std::pair<A, B>& p) {
                auto a_bytes = a.encode(p.first);
                auto b_bytes = b.encode(p.second);
                typename Codec<std::pair<A, B>>::Bytes out;
                out.reserve(4 + a_bytes.size() + b_bytes.size());
                const auto len = static_cast<std::uint32_t>(a_bytes.size());
                for (int i = 0; i < 4; ++i) {
                    out.push_back(static_cast<std::byte>((len >> (i * 8)) & 0xFF));
                }
                out.insert(out.end(), a_bytes.begin(), a_bytes.end());
                out.insert(out.end(), b_bytes.begin(), b_bytes.end());
                return out;
            },
        .decode = [a, b](typename Codec<std::pair<A, B>>::BytesView buf)
            -> std::optional<std::pair<A, B>> {
            if (buf.size() < 4) {
                return std::nullopt;
            }
            std::uint32_t len = 0;
            for (int i = 0; i < 4; ++i) {
                len |= static_cast<std::uint32_t>(static_cast<unsigned char>(buf[i])) << (i * 8);
            }
            if (buf.size() < 4 + len) {
                return std::nullopt;
            }
            auto a_view = buf.subspan(4, len);
            auto b_view = buf.subspan(4 + len);
            auto a_val = a.decode(a_view);
            auto b_val = b.decode(b_view);
            if (!a_val.has_value() || !b_val.has_value()) {
                return std::nullopt;
            }
            return std::make_pair(std::move(*a_val), std::move(*b_val));
        },
        .encode_into =
            [a, b](const std::pair<A, B>& p, typename Codec<std::pair<A, B>>::Bytes& out) {
                encode_append_lp(a, p.first, out);  // 4-byte len prefix + first
                encode_append(b, p.second, out);    // second (unprefixed, as in encode)
            }};
}

// Optional codec: 1-byte presence flag, then the element's bytes when
// present. Wraps any Codec<T> in a `std::optional<T>` shell. Decoding
// fails on truncated buffers and on inner-codec failure; success
// returns either `optional<optional<T>>{nullopt}` (absent) or
// `optional<optional<T>>{T}` (present). The outer optional represents
// codec success/failure; the inner is the user's nullability.
template <typename T>
inline Codec<std::optional<T>> optional_codec(Codec<T> elem) {
    return Codec<std::optional<T>>{
        .encode =
            [elem](const std::optional<T>& v) {
                typename Codec<std::optional<T>>::Bytes out;
                if (!v.has_value()) {
                    out.push_back(std::byte{0});
                    return out;
                }
                auto inner = elem.encode(*v);
                out.reserve(1 + inner.size());
                out.push_back(std::byte{1});
                out.insert(out.end(), inner.begin(), inner.end());
                return out;
            },
        .decode = [elem](typename Codec<std::optional<T>>::BytesView buf)
            -> std::optional<std::optional<T>> {
            if (buf.empty()) {
                return std::nullopt;
            }
            const auto flag = static_cast<unsigned char>(buf[0]);
            if (flag == 0) {
                return std::optional<T>{};
            }
            if (flag != 1) {
                return std::nullopt;
            }
            auto inner = elem.decode(buf.subspan(1));
            if (!inner.has_value()) {
                return std::nullopt;
            }
            return std::optional<T>{std::move(*inner)};
        },
        .encode_into =
            [elem](const std::optional<T>& v, typename Codec<std::optional<T>>::Bytes& out) {
                if (!v.has_value()) {
                    out.push_back(std::byte{0});
                    return;
                }
                out.push_back(std::byte{1});
                encode_append(elem, *v, out);
            }};
}

// Set codec: 4-byte count, then per element a 4-byte length followed by
// the element's bytes. Wire-identical to vector_codec; the only
// difference is the decode reconstructs into a `std::set<T, Compare>`
// (de-duplicating + ordering by the supplied comparator). Encoding
// order follows the set's iteration order so the wire bytes are
// deterministic for a given set value.
template <typename T, typename Compare = std::less<T>>
inline Codec<std::set<T, Compare>> set_codec(Codec<T> elem) {
    return Codec<std::set<T, Compare>>{
        .encode =
            [elem](const std::set<T, Compare>& v) {
                typename Codec<std::set<T, Compare>>::Bytes out;
                detail::append_u32_le(out, static_cast<std::uint32_t>(v.size()));
                for (const auto& e : v) {
                    auto bytes = elem.encode(e);
                    detail::append_u32_le(out, static_cast<std::uint32_t>(bytes.size()));
                    out.insert(out.end(), bytes.begin(), bytes.end());
                }
                return out;
            },
        .decode = [elem](typename Codec<std::set<T, Compare>>::BytesView buf)
            -> std::optional<std::set<T, Compare>> {
            auto count = detail::read_u32_le(buf, 0);
            if (!count.has_value()) {
                return std::nullopt;
            }
            std::size_t pos = 4;
            std::set<T, Compare> out;
            for (std::uint32_t i = 0; i < *count; ++i) {
                auto len = detail::read_u32_le(buf, pos);
                if (!len.has_value()) {
                    return std::nullopt;
                }
                pos += 4;
                if (pos + *len > buf.size()) {
                    return std::nullopt;
                }
                auto entry = elem.decode(buf.subspan(pos, *len));
                if (!entry.has_value()) {
                    return std::nullopt;
                }
                out.insert(std::move(*entry));
                pos += *len;
            }
            return out;
        },
        .encode_into =
            [elem](const std::set<T, Compare>& v,
                   typename Codec<std::set<T, Compare>>::Bytes& out) {
                detail::append_u32_le(out, static_cast<std::uint32_t>(v.size()));
                for (const auto& e : v) {
                    encode_append_lp(elem, e, out);
                }
            }};
}

// Unordered set codec: same wire shape as `set_codec`. Iteration order
// at encode time is implementation-defined, so the encoded bytes are
// NOT deterministic for a given value - fine for keyed-state storage
// where the decoder rebuilds the set, but unsuitable for any path that
// hashes the encoded bytes for equality.
template <typename T, typename Hash = std::hash<T>, typename KeyEq = std::equal_to<T>>
inline Codec<std::unordered_set<T, Hash, KeyEq>> unordered_set_codec(Codec<T> elem) {
    return Codec<std::unordered_set<T, Hash, KeyEq>>{
        .encode =
            [elem](const std::unordered_set<T, Hash, KeyEq>& v) {
                typename Codec<std::unordered_set<T, Hash, KeyEq>>::Bytes out;
                detail::append_u32_le(out, static_cast<std::uint32_t>(v.size()));
                for (const auto& e : v) {
                    auto bytes = elem.encode(e);
                    detail::append_u32_le(out, static_cast<std::uint32_t>(bytes.size()));
                    out.insert(out.end(), bytes.begin(), bytes.end());
                }
                return out;
            },
        .decode = [elem](typename Codec<std::unordered_set<T, Hash, KeyEq>>::BytesView buf)
            -> std::optional<std::unordered_set<T, Hash, KeyEq>> {
            auto count = detail::read_u32_le(buf, 0);
            if (!count.has_value()) {
                return std::nullopt;
            }
            std::size_t pos = 4;
            std::unordered_set<T, Hash, KeyEq> out;
            out.reserve(*count);
            for (std::uint32_t i = 0; i < *count; ++i) {
                auto len = detail::read_u32_le(buf, pos);
                if (!len.has_value()) {
                    return std::nullopt;
                }
                pos += 4;
                if (pos + *len > buf.size()) {
                    return std::nullopt;
                }
                auto entry = elem.decode(buf.subspan(pos, *len));
                if (!entry.has_value()) {
                    return std::nullopt;
                }
                out.insert(std::move(*entry));
                pos += *len;
            }
            return out;
        },
        .encode_into =
            [elem](const std::unordered_set<T, Hash, KeyEq>& v,
                   typename Codec<std::unordered_set<T, Hash, KeyEq>>::Bytes& out) {
                detail::append_u32_le(out, static_cast<std::uint32_t>(v.size()));
                for (const auto& e : v) {
                    encode_append_lp(elem, e, out);
                }
            }};
}

// Map codec: 4-byte count, then per entry a (4-byte K-length, K bytes,
// 4-byte V-length, V bytes) tuple. Wire-identical to a
// vector_codec<pair<K, V>> with the pair_codec's prefix-on-first
// component layout, but reconstructs into a `std::map` and so
// de-duplicates by key (last-write-wins on duplicate-key wire input).
template <typename K, typename V, typename Compare = std::less<K>>
inline Codec<std::map<K, V, Compare>> map_codec(Codec<K> kc, Codec<V> vc) {
    return Codec<std::map<K, V, Compare>>{
        .encode =
            [kc, vc](const std::map<K, V, Compare>& m) {
                typename Codec<std::map<K, V, Compare>>::Bytes out;
                detail::append_u32_le(out, static_cast<std::uint32_t>(m.size()));
                for (const auto& [k, v] : m) {
                    auto kb = kc.encode(k);
                    auto vb = vc.encode(v);
                    detail::append_u32_le(out, static_cast<std::uint32_t>(kb.size()));
                    out.insert(out.end(), kb.begin(), kb.end());
                    detail::append_u32_le(out, static_cast<std::uint32_t>(vb.size()));
                    out.insert(out.end(), vb.begin(), vb.end());
                }
                return out;
            },
        .decode = [kc, vc](typename Codec<std::map<K, V, Compare>>::BytesView buf)
            -> std::optional<std::map<K, V, Compare>> {
            auto count = detail::read_u32_le(buf, 0);
            if (!count.has_value()) {
                return std::nullopt;
            }
            std::size_t pos = 4;
            std::map<K, V, Compare> out;
            for (std::uint32_t i = 0; i < *count; ++i) {
                auto klen = detail::read_u32_le(buf, pos);
                if (!klen.has_value()) {
                    return std::nullopt;
                }
                pos += 4;
                if (pos + *klen > buf.size()) {
                    return std::nullopt;
                }
                auto kval = kc.decode(buf.subspan(pos, *klen));
                if (!kval.has_value()) {
                    return std::nullopt;
                }
                pos += *klen;
                auto vlen = detail::read_u32_le(buf, pos);
                if (!vlen.has_value()) {
                    return std::nullopt;
                }
                pos += 4;
                if (pos + *vlen > buf.size()) {
                    return std::nullopt;
                }
                auto vval = vc.decode(buf.subspan(pos, *vlen));
                if (!vval.has_value()) {
                    return std::nullopt;
                }
                out.emplace(std::move(*kval), std::move(*vval));
                pos += *vlen;
            }
            return out;
        },
        .encode_into =
            [kc, vc](const std::map<K, V, Compare>& m,
                     typename Codec<std::map<K, V, Compare>>::Bytes& out) {
                detail::append_u32_le(out, static_cast<std::uint32_t>(m.size()));
                for (const auto& [k, v] : m) {
                    encode_append_lp(kc, k, out);
                    encode_append_lp(vc, v, out);
                }
            }};
}

// Unordered map codec: same wire shape as `map_codec`. Iteration order
// at encode time is implementation-defined; same determinism caveat as
// `unordered_set_codec` applies.
template <typename K, typename V, typename Hash = std::hash<K>, typename KeyEq = std::equal_to<K>>
inline Codec<std::unordered_map<K, V, Hash, KeyEq>> unordered_map_codec(Codec<K> kc, Codec<V> vc) {
    return Codec<std::unordered_map<K, V, Hash, KeyEq>>{
        .encode =
            [kc, vc](const std::unordered_map<K, V, Hash, KeyEq>& m) {
                typename Codec<std::unordered_map<K, V, Hash, KeyEq>>::Bytes out;
                detail::append_u32_le(out, static_cast<std::uint32_t>(m.size()));
                for (const auto& [k, v] : m) {
                    auto kb = kc.encode(k);
                    auto vb = vc.encode(v);
                    detail::append_u32_le(out, static_cast<std::uint32_t>(kb.size()));
                    out.insert(out.end(), kb.begin(), kb.end());
                    detail::append_u32_le(out, static_cast<std::uint32_t>(vb.size()));
                    out.insert(out.end(), vb.begin(), vb.end());
                }
                return out;
            },
        .decode = [kc, vc](typename Codec<std::unordered_map<K, V, Hash, KeyEq>>::BytesView buf)
            -> std::optional<std::unordered_map<K, V, Hash, KeyEq>> {
            auto count = detail::read_u32_le(buf, 0);
            if (!count.has_value()) {
                return std::nullopt;
            }
            std::size_t pos = 4;
            std::unordered_map<K, V, Hash, KeyEq> out;
            out.reserve(*count);
            for (std::uint32_t i = 0; i < *count; ++i) {
                auto klen = detail::read_u32_le(buf, pos);
                if (!klen.has_value()) {
                    return std::nullopt;
                }
                pos += 4;
                if (pos + *klen > buf.size()) {
                    return std::nullopt;
                }
                auto kval = kc.decode(buf.subspan(pos, *klen));
                if (!kval.has_value()) {
                    return std::nullopt;
                }
                pos += *klen;
                auto vlen = detail::read_u32_le(buf, pos);
                if (!vlen.has_value()) {
                    return std::nullopt;
                }
                pos += 4;
                if (pos + *vlen > buf.size()) {
                    return std::nullopt;
                }
                auto vval = vc.decode(buf.subspan(pos, *vlen));
                if (!vval.has_value()) {
                    return std::nullopt;
                }
                out.emplace(std::move(*kval), std::move(*vval));
                pos += *vlen;
            }
            return out;
        },
        .encode_into =
            [kc, vc](const std::unordered_map<K, V, Hash, KeyEq>& m,
                     typename Codec<std::unordered_map<K, V, Hash, KeyEq>>::Bytes& out) {
                detail::append_u32_le(out, static_cast<std::uint32_t>(m.size()));
                for (const auto& [k, v] : m) {
                    encode_append_lp(kc, k, out);
                    encode_append_lp(vc, v, out);
                }
            }};
}

}  // namespace clink
