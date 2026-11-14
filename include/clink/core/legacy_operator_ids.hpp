#pragma once

// The operator ids clink derived before they were the same everywhere.
//
// Up to and including v0.15.0, and on main until the stable ids replaced them,
// an operator's id was `std::hash<std::string>` of "uid/<uid>", or of
// "stage<idx>/<name>" for an operator without a uid. std::hash is the standard
// library's own function, and the two libraries clink builds with implement it
// differently, so a checkpoint taken by a Linux build (libstdc++) stored its
// state under other ids than a macOS build (libc++) would look for, and the
// state of every operator came back empty. Restoring such a checkpoint means
// knowing both: these are reimplementations of each library's string hash, as
// the builds that wrote those checkpoints computed it, so a restore on either
// platform can recognise state written on either.
//
// Both assume what every build that wrote such a checkpoint was: a 64-bit,
// little-endian target (x86_64 or aarch64). Words are assembled byte by byte
// in little-endian order, so the result is the same on any host. The
// golden vectors in tests/test_operator_ids.cpp were produced by each
// library's own std::hash, and the test also checks the native one against
// std::hash wherever it runs.

#include <array>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <utility>

#include "clink/core/types.hpp"

namespace clink::legacy_ids {

namespace detail {

inline std::uint64_t load64_le(const char* p) noexcept {
    std::uint64_t v = 0;
    for (int i = 7; i >= 0; --i) {
        v = (v << 8) | static_cast<std::uint8_t>(p[i]);
    }
    return v;
}

inline std::uint32_t load32_le(const char* p) noexcept {
    std::uint32_t v = 0;
    for (int i = 3; i >= 0; --i) {
        v = (v << 8) | static_cast<std::uint8_t>(p[i]);
    }
    return v;
}

inline std::uint64_t shift_mix(std::uint64_t v) noexcept {
    return v ^ (v >> 47);
}

// libc++'s CityHash64 helpers (__murmur2_or_cityhash<size_t, 64>).
constexpr std::uint64_t kK0 = 0xc3a5c85c97cb3127ULL;
constexpr std::uint64_t kK1 = 0xb492b66fbe98f273ULL;
constexpr std::uint64_t kK2 = 0x9ae16a3b2f90404fULL;
constexpr std::uint64_t kK3 = 0xc949d7c7509e6557ULL;

inline std::uint64_t rotate(std::uint64_t v, int shift) noexcept {
    return shift == 0 ? v : ((v >> shift) | (v << (64 - shift)));
}

inline std::uint64_t rotate_by_at_least_1(std::uint64_t v, int shift) noexcept {
    return (v >> shift) | (v << (64 - shift));
}

inline std::uint64_t hash_len_16(std::uint64_t u, std::uint64_t v) noexcept {
    constexpr std::uint64_t kMul = 0x9ddfea08eb382d69ULL;
    std::uint64_t a = (u ^ v) * kMul;
    a ^= (a >> 47);
    std::uint64_t b = (v ^ a) * kMul;
    b ^= (b >> 47);
    b *= kMul;
    return b;
}

inline std::uint64_t hash_len_0_to_16(const char* s, std::uint64_t len) noexcept {
    if (len > 8) {
        const std::uint64_t a = load64_le(s);
        const std::uint64_t b = load64_le(s + len - 8);
        return hash_len_16(a, rotate_by_at_least_1(b + len, static_cast<int>(len))) ^ b;
    }
    if (len >= 4) {
        const std::uint32_t a = load32_le(s);
        const std::uint32_t b = load32_le(s + len - 4);
        // The default libc++ ABI shifts in 32 bits (the uncorrected
        // _LIBCPP_ABI_FIX_CITYHASH_IMPLEMENTATION branch), and that is what
        // macOS builds computed.
        return hash_len_16(len + static_cast<std::uint32_t>(a << 3), b);
    }
    if (len > 0) {
        const auto a = static_cast<unsigned char>(s[0]);
        const auto b = static_cast<unsigned char>(s[len >> 1]);
        const auto c = static_cast<unsigned char>(s[len - 1]);
        const std::uint32_t y =
            static_cast<std::uint32_t>(a) + (static_cast<std::uint32_t>(b) << 8);
        const std::uint32_t z =
            static_cast<std::uint32_t>(len) + (static_cast<std::uint32_t>(c) << 2);
        return shift_mix((y * kK2) ^ (z * kK3)) * kK2;
    }
    return kK2;
}

inline std::uint64_t hash_len_17_to_32(const char* s, std::uint64_t len) noexcept {
    const std::uint64_t a = load64_le(s) * kK1;
    const std::uint64_t b = load64_le(s + 8);
    const std::uint64_t c = load64_le(s + len - 8) * kK2;
    const std::uint64_t d = load64_le(s + len - 16) * kK0;
    return hash_len_16(rotate(a - b, 43) + rotate(c, 30) + d, a + rotate(b ^ kK3, 20) - c + len);
}

inline std::pair<std::uint64_t, std::uint64_t> weak_hash_len_32_with_seeds(
    std::uint64_t w,
    std::uint64_t x,
    std::uint64_t y,
    std::uint64_t z,
    std::uint64_t a,
    std::uint64_t b) noexcept {
    a += w;
    b = rotate(b + a + z, 21);
    const std::uint64_t c = a;
    a += x;
    a += y;
    b += rotate(a, 44);
    return {a + z, b + c};
}

inline std::pair<std::uint64_t, std::uint64_t> weak_hash_len_32_with_seeds(
    const char* s, std::uint64_t a, std::uint64_t b) noexcept {
    return weak_hash_len_32_with_seeds(
        load64_le(s), load64_le(s + 8), load64_le(s + 16), load64_le(s + 24), a, b);
}

inline std::uint64_t hash_len_33_to_64(const char* s, std::uint64_t len) noexcept {
    std::uint64_t z = load64_le(s + 24);
    std::uint64_t a = load64_le(s) + (len + load64_le(s + len - 16)) * kK0;
    std::uint64_t b = rotate(a + z, 52);
    std::uint64_t c = rotate(a, 37);
    a += load64_le(s + 8);
    c += rotate(a, 7);
    a += load64_le(s + 16);
    const std::uint64_t vf = a + z;
    const std::uint64_t vs = b + rotate(a, 31) + c;
    a = load64_le(s + 16) + load64_le(s + len - 32);
    z += load64_le(s + len - 8);
    b = rotate(a + z, 52);
    c = rotate(a, 37);
    a += load64_le(s + len - 24);
    c += rotate(a, 7);
    a += load64_le(s + len - 16);
    const std::uint64_t wf = a + z;
    const std::uint64_t ws = b + rotate(a, 31) + c;
    const std::uint64_t r = shift_mix((vf + ws) * kK2 + (wf + vs) * kK0);
    return shift_mix(r * kK0 + vs) * kK2;
}

}  // namespace detail

// libstdc++'s std::hash<std::string>: std::_Hash_bytes(data, size, 0xc70f6907),
// its Murmur2 variant for 64-bit size_t.
inline std::uint64_t libstdcxx_string_hash(std::string_view text) noexcept {
    constexpr std::uint64_t kMul = (std::uint64_t{0xc6a4a793} << 32) + std::uint64_t{0x5bd1e995};
    constexpr std::uint64_t kSeed = 0xc70f6907ULL;
    const char* buf = text.data();
    const std::uint64_t len = text.size();
    const std::uint64_t len_aligned = len & ~std::uint64_t{0x7};
    std::uint64_t hash = kSeed ^ (len * kMul);
    for (std::uint64_t i = 0; i < len_aligned; i += 8) {
        const std::uint64_t data = detail::shift_mix(detail::load64_le(buf + i) * kMul) * kMul;
        hash ^= data;
        hash *= kMul;
    }
    if ((len & 0x7) != 0) {
        // The trailing bytes, the last one most significant.
        std::uint64_t data = 0;
        for (std::uint64_t n = len & 0x7; n > 0; --n) {
            data = (data << 8) + static_cast<unsigned char>(buf[len_aligned + n - 1]);
        }
        hash ^= data;
        hash *= kMul;
    }
    hash = detail::shift_mix(hash) * kMul;
    return detail::shift_mix(hash);
}

// libc++'s std::hash<std::string>: __murmur2_or_cityhash<size_t, 64>, its
// CityHash64.
inline std::uint64_t libcxx_string_hash(std::string_view text) noexcept {
    using namespace detail;
    const char* s = text.data();
    std::uint64_t len = text.size();
    if (len <= 32) {
        return len <= 16 ? hash_len_0_to_16(s, len) : hash_len_17_to_32(s, len);
    }
    if (len <= 64) {
        return hash_len_33_to_64(s, len);
    }
    std::uint64_t x = load64_le(s + len - 40);
    std::uint64_t y = load64_le(s + len - 16) + load64_le(s + len - 56);
    std::uint64_t z = hash_len_16(load64_le(s + len - 48) + len, load64_le(s + len - 24));
    auto v = weak_hash_len_32_with_seeds(s + len - 64, len, z);
    auto w = weak_hash_len_32_with_seeds(s + len - 32, y + kK1, x);
    x = x * kK1 + load64_le(s);
    len = (len - 1) & ~std::uint64_t{63};
    do {
        x = rotate(x + y + v.first + load64_le(s + 8), 37) * kK1;
        y = rotate(y + v.second + load64_le(s + 48), 42) * kK1;
        x ^= w.second;
        y += v.first + load64_le(s + 40);
        z = rotate(z + w.first, 33) * kK1;
        v = weak_hash_len_32_with_seeds(s, v.second * kK1, x + w.first);
        w = weak_hash_len_32_with_seeds(s + 32, z + w.second, y + load64_le(s + 16));
        std::swap(z, x);
        s += 64;
        len -= 64;
    } while (len != 0);
    return hash_len_16(hash_len_16(v.first, w.first) + shift_mix(y) * kK1 + z,
                       hash_len_16(v.second, w.second) + x);
}

// The two ids the old derivation gave an operator for `hashed`, the exact
// string it hashed ("uid/<uid>", or "stage<idx>/<name>"): as a libstdc++ build
// computed it, then as a libc++ build did, each with 0 mapped to 1.
inline std::array<OperatorId, 2> legacy_operator_ids(std::string_view hashed) noexcept {
    const auto as_id = [](std::uint64_t h) { return OperatorId{h == 0 ? std::uint64_t{1} : h}; };
    return {as_id(libstdcxx_string_hash(hashed)), as_id(libcxx_string_hash(hashed))};
}

}  // namespace clink::legacy_ids
