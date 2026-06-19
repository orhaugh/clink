// Sha256 (clink/core/sha256.hpp): the content-addressing hash. A wrong digest
// = a wrong object key = silent state corruption, so this is pinned against the
// published FIPS 180-4 test vectors and checked for streaming stability (the
// result must not depend on how the bytes are chunked into update() calls).

#include <algorithm>
#include <array>
#include <cstddef>
#include <string>

#include <gtest/gtest.h>

#include "clink/core/sha256.hpp"

using clink::Sha256;

namespace {

std::string hash_str(const std::string& s) {
    Sha256 h;
    h.update(s.data(), s.size());
    return Sha256::to_hex(h.finalize());
}

}  // namespace

TEST(Sha256, KnownVectors) {
    // FIPS 180-4 / RFC 6234 published vectors.
    EXPECT_EQ(hash_str(""), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    EXPECT_EQ(hash_str("abc"), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    EXPECT_EQ(hash_str("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
              "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1");
}

TEST(Sha256, MillionA) {
    // The classic one-million-'a' vector exercises many block transforms.
    Sha256 h;
    const std::string chunk(1000, 'a');
    for (int i = 0; i < 1000; ++i) {
        h.update(chunk.data(), chunk.size());
    }
    EXPECT_EQ(Sha256::to_hex(h.finalize()),
              "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0");
}

TEST(Sha256, StreamingIsChunkIndependent) {
    // The same bytes hashed in one update() vs byte-at-a-time vs odd chunks
    // must all agree (the padding / block boundary logic is the risk).
    const std::string data = "the quick brown fox jumps over the lazy dog, 0123456789!";
    const std::string once = hash_str(data);

    Sha256 byte_at_a_time;
    for (char c : data) {
        byte_at_a_time.update(&c, 1);
    }
    EXPECT_EQ(Sha256::to_hex(byte_at_a_time.finalize()), once);

    Sha256 odd_chunks;
    for (std::size_t i = 0; i < data.size(); i += 7) {
        odd_chunks.update(data.data() + i, std::min<std::size_t>(7, data.size() - i));
    }
    EXPECT_EQ(Sha256::to_hex(odd_chunks.finalize()), once);
}

TEST(Sha256, ResetReuses) {
    Sha256 h;
    h.update("abc", 3);
    (void)h.finalize();
    h.reset();
    h.update("abc", 3);
    EXPECT_EQ(Sha256::to_hex(h.finalize()),
              "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
}

TEST(Sha256, BlockBoundaryLengths) {
    // Lengths around the 55/56/64-byte padding boundary are where the extra-
    // block path triggers; each must equal an independent recompute.
    for (std::size_t n : {54u, 55u, 56u, 57u, 63u, 64u, 65u, 119u, 128u}) {
        const std::string data(n, 'x');
        Sha256 a;
        a.update(data.data(), data.size());
        Sha256 b;
        b.update(data.data(), data.size() / 2);
        b.update(data.data() + data.size() / 2, data.size() - data.size() / 2);
        EXPECT_EQ(Sha256::to_hex(a.finalize()), Sha256::to_hex(b.finalize())) << "n=" << n;
    }
}

TEST(Sha256, IsHexDigest) {
    EXPECT_TRUE(
        Sha256::is_hex_digest("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"));
    EXPECT_FALSE(Sha256::is_hex_digest("short"));
    EXPECT_FALSE(Sha256::is_hex_digest(std::string(64, 'g')));  // non-hex char
    EXPECT_FALSE(Sha256::is_hex_digest(
        "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855"));  // uppercase
    EXPECT_FALSE(Sha256::is_hex_digest(std::string(63, 'a')));                 // too short by one
}
