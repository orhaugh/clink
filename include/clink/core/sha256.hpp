#pragma once

// Sha256 - a small, dependency-free, header-only streaming SHA-256.
//
// Used for content-addressing: an object's storage key IS the hash of its
// bytes (DISAGG-6's content-addressed checkpoint manifest). That makes a
// collision a silent-corruption bug, so a cryptographic hash is required - a
// fast non-cryptographic hash (xxhash etc.) is not acceptable here. SHA-256 is
// the standard choice and is not the bottleneck (we hash bytes we are about to
// upload over the network, so the hash is far cheaper than the PUT).
//
// We vendor it rather than link OpenSSL/AWS-SDK crypto: it keeps clink::core
// dependency-free, builds identically on host + Linux (a known portability
// concern), and SHA-256 is a fixed, test-vector-checkable specification. The
// implementation is the standard FIPS 180-4 reference structure.
//
// Streaming: construct, update() with successive chunks, finalize() once for
// the 32-byte digest (the object is then spent). to_hex() gives the 64-char
// lowercase hex used as the on-storage key.

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>

namespace clink {

class Sha256 {
public:
    Sha256() { reset(); }

    void reset() {
        state_ = {0x6a09e667u,
                  0xbb67ae85u,
                  0x3c6ef372u,
                  0xa54ff53au,
                  0x510e527fu,
                  0x9b05688cu,
                  0x1f83d9abu,
                  0x5be0cd19u};
        bitlen_ = 0;
        buflen_ = 0;
    }

    void update(const void* data, std::size_t len) {
        const auto* p = static_cast<const std::uint8_t*>(data);
        for (std::size_t i = 0; i < len; ++i) {
            buf_[buflen_++] = p[i];
            if (buflen_ == 64) {
                transform_(buf_.data());
                bitlen_ += 512;
                buflen_ = 0;
            }
        }
    }

    // Returns the 32-byte digest. The object should be reset() before reuse.
    std::array<std::uint8_t, 32> finalize() {
        const std::uint64_t bits = bitlen_ + static_cast<std::uint64_t>(buflen_) * 8;
        std::size_t i = buflen_;
        buf_[i++] = 0x80;  // append the '1' bit then pad with zeros
        if (i > 56) {
            while (i < 64) {
                buf_[i++] = 0;
            }
            transform_(buf_.data());
            i = 0;
        }
        while (i < 56) {
            buf_[i++] = 0;
        }
        for (int j = 7; j >= 0; --j) {  // 64-bit big-endian length
            buf_[i++] = static_cast<std::uint8_t>((bits >> (j * 8)) & 0xffu);
        }
        transform_(buf_.data());

        std::array<std::uint8_t, 32> out{};
        for (std::size_t j = 0; j < 8; ++j) {
            out[j * 4 + 0] = static_cast<std::uint8_t>((state_[j] >> 24) & 0xffu);
            out[j * 4 + 1] = static_cast<std::uint8_t>((state_[j] >> 16) & 0xffu);
            out[j * 4 + 2] = static_cast<std::uint8_t>((state_[j] >> 8) & 0xffu);
            out[j * 4 + 3] = static_cast<std::uint8_t>((state_[j]) & 0xffu);
        }
        return out;
    }

    static std::string to_hex(const std::array<std::uint8_t, 32>& d) {
        static const char* kHex = "0123456789abcdef";
        std::string s(64, '0');
        for (std::size_t i = 0; i < 32; ++i) {
            s[i * 2] = kHex[d[i] >> 4];
            s[i * 2 + 1] = kHex[d[i] & 0x0f];
        }
        return s;
    }

    // True if `s` is exactly a 64-char lowercase-hex SHA-256 digest (the shape
    // of a content-addressed object key).
    static bool is_hex_digest(const std::string& s) {
        if (s.size() != 64) {
            return false;
        }
        for (char c : s) {
            const bool hex = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
            if (!hex) {
                return false;
            }
        }
        return true;
    }

private:
    static std::uint32_t rotr_(std::uint32_t x, std::uint32_t n) {
        return (x >> n) | (x << (32 - n));
    }

    void transform_(const std::uint8_t* p) {
        static const std::uint32_t kK[64] = {
            0x428a2f98u, 0x71374491u, 0xb5c0fbcfu, 0xe9b5dba5u, 0x3956c25bu, 0x59f111f1u,
            0x923f82a4u, 0xab1c5ed5u, 0xd807aa98u, 0x12835b01u, 0x243185beu, 0x550c7dc3u,
            0x72be5d74u, 0x80deb1feu, 0x9bdc06a7u, 0xc19bf174u, 0xe49b69c1u, 0xefbe4786u,
            0x0fc19dc6u, 0x240ca1ccu, 0x2de92c6fu, 0x4a7484aau, 0x5cb0a9dcu, 0x76f988dau,
            0x983e5152u, 0xa831c66du, 0xb00327c8u, 0xbf597fc7u, 0xc6e00bf3u, 0xd5a79147u,
            0x06ca6351u, 0x14292967u, 0x27b70a85u, 0x2e1b2138u, 0x4d2c6dfcu, 0x53380d13u,
            0x650a7354u, 0x766a0abbu, 0x81c2c92eu, 0x92722c85u, 0xa2bfe8a1u, 0xa81a664bu,
            0xc24b8b70u, 0xc76c51a3u, 0xd192e819u, 0xd6990624u, 0xf40e3585u, 0x106aa070u,
            0x19a4c116u, 0x1e376c08u, 0x2748774cu, 0x34b0bcb5u, 0x391c0cb3u, 0x4ed8aa4au,
            0x5b9cca4fu, 0x682e6ff3u, 0x748f82eeu, 0x78a5636fu, 0x84c87814u, 0x8cc70208u,
            0x90befffau, 0xa4506cebu, 0xbef9a3f7u, 0xc67178f2u};

        std::uint32_t w[64];
        for (int i = 0; i < 16; ++i) {
            w[i] = (static_cast<std::uint32_t>(p[i * 4]) << 24) |
                   (static_cast<std::uint32_t>(p[i * 4 + 1]) << 16) |
                   (static_cast<std::uint32_t>(p[i * 4 + 2]) << 8) |
                   (static_cast<std::uint32_t>(p[i * 4 + 3]));
        }
        for (int i = 16; i < 64; ++i) {
            const std::uint32_t s0 = rotr_(w[i - 15], 7) ^ rotr_(w[i - 15], 18) ^ (w[i - 15] >> 3);
            const std::uint32_t s1 = rotr_(w[i - 2], 17) ^ rotr_(w[i - 2], 19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16] + s0 + w[i - 7] + s1;
        }

        std::uint32_t a = state_[0], b = state_[1], c = state_[2], d = state_[3];
        std::uint32_t e = state_[4], f = state_[5], g = state_[6], h = state_[7];
        for (int i = 0; i < 64; ++i) {
            const std::uint32_t s1 = rotr_(e, 6) ^ rotr_(e, 11) ^ rotr_(e, 25);
            const std::uint32_t ch = (e & f) ^ ((~e) & g);
            const std::uint32_t t1 = h + s1 + ch + kK[i] + w[i];
            const std::uint32_t s0 = rotr_(a, 2) ^ rotr_(a, 13) ^ rotr_(a, 22);
            const std::uint32_t maj = (a & b) ^ (a & c) ^ (b & c);
            const std::uint32_t t2 = s0 + maj;
            h = g;
            g = f;
            f = e;
            e = d + t1;
            d = c;
            c = b;
            b = a;
            a = t1 + t2;
        }
        state_[0] += a;
        state_[1] += b;
        state_[2] += c;
        state_[3] += d;
        state_[4] += e;
        state_[5] += f;
        state_[6] += g;
        state_[7] += h;
    }

    std::array<std::uint32_t, 8> state_{};
    std::uint64_t bitlen_{0};
    std::array<std::uint8_t, 64> buf_{};
    std::size_t buflen_{0};
};

}  // namespace clink
