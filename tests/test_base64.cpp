// base64: RFC 4648 vectors, binary safety, and the decoder's tolerance rules.
//
// This header had NO tests, which a coverage audit turned up. It is not
// incidental code: the worker base64-decodes UDF module bytes off the
// control plane (src/cluster/worker.cpp), the SQL script runner and the
// WASM UDF loader both use it, and the HTTP connectors carry Pub/Sub
// payloads through it. That is untrusted input on one side and
// cross-process interop on the other - the two situations where a
// hand-rolled codec earns tests.
//
// Interop is why the RFC vectors are here rather than only round-trips: a
// round-trip proves this decoder undoes this encoder, which stays true even
// if both are wrong in the same direction. The published vectors are what
// pin the wire format to something another implementation agrees with.

#include <string>
#include <string_view>

#include <gtest/gtest.h>

#include "clink/core/base64.hpp"

using clink::base64_decode;
using clink::base64_encode;

// RFC 4648 section 10. Every padding case (0, 1 and 2 '=') appears here,
// which is what makes this table worth more than any single vector.
TEST(Base64, MatchesTheRfc4648Vectors) {
    const std::pair<std::string, std::string> vectors[] = {
        {"", ""},
        {"f", "Zg=="},
        {"fo", "Zm8="},
        {"foo", "Zm9v"},
        {"foob", "Zm9vYg=="},
        {"fooba", "Zm9vYmE="},
        {"foobar", "Zm9vYmFy"},
    };
    for (const auto& [plain, encoded] : vectors) {
        EXPECT_EQ(base64_encode(plain), encoded) << "encoding '" << plain << "'";
        const auto decoded = base64_decode(encoded);
        ASSERT_TRUE(decoded.has_value()) << "decoding '" << encoded << "'";
        EXPECT_EQ(*decoded, plain);
    }
}

TEST(Base64, RoundTripsEveryByteValue) {
    // Binary safety: the payloads this carries are compiled WASM modules and
    // Pub/Sub blobs, not text. A codec that mangles 0x00 or bytes above 0x7F
    // would corrupt them silently - the encode succeeds, the decode succeeds,
    // and the module simply fails to load much later.
    std::string all;
    all.reserve(256);
    for (int i = 0; i < 256; ++i) {
        all.push_back(static_cast<char>(i));
    }
    const auto decoded = base64_decode(base64_encode(all));
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, all);
}

TEST(Base64, RoundTripsPayloadsLongerThanTheAccumulator) {
    // The decoder accumulates into an int and never masks it, so a long
    // input shifts bits off the top repeatedly. That is well defined since
    // C++20 (signed left-shift wraps rather than being UB) and the emitted
    // byte only ever reads the low bits - but "well defined" and "correct"
    // are different claims, and only a payload long enough to wrap the
    // accumulator many times over tests the second one.
    std::string long_payload;
    long_payload.reserve(4096);
    for (int i = 0; i < 4096; ++i) {
        long_payload.push_back(static_cast<char>((i * 31 + 7) & 0xFF));
    }
    const auto decoded = base64_decode(base64_encode(long_payload));
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->size(), long_payload.size());
    EXPECT_EQ(*decoded, long_payload);
}

TEST(Base64, RejectsAnInvalidCharacterRatherThanManglingIt) {
    // The documented contract, and the one that matters for untrusted input:
    // a malformed payload must be SURFACED. Silently skipping unknown bytes
    // would decode attacker- or corruption-supplied text into a plausible
    // shorter payload that the caller then treats as a valid module.
    EXPECT_FALSE(base64_decode("Zm9v*mFy").has_value());
    EXPECT_FALSE(base64_decode("!!!!").has_value());
    EXPECT_FALSE(base64_decode("Zm9vYmFy\x01").has_value());
    // A byte above 0x7F must not sneak through the signed-char comparisons.
    EXPECT_FALSE(base64_decode("Zm9v\xFFYmFy").has_value());
}

TEST(Base64, ToleratesWrappedLinesAndMissingPadding) {
    // Both tolerances are deliberate (see the header): wire payloads arrive
    // line-wrapped, and some producers omit padding.
    const auto wrapped = base64_decode("Zm9v\r\nYmFy");
    ASSERT_TRUE(wrapped.has_value());
    EXPECT_EQ(*wrapped, "foobar");

    const auto spaced = base64_decode("Zm9v YmFy\t");
    ASSERT_TRUE(spaced.has_value());
    EXPECT_EQ(*spaced, "foobar");

    // Unpadded: 'Zg' carries 12 bits, of which 8 are a whole byte and the
    // leftover 4 are discarded per the tolerant-decoder convention.
    const auto unpadded = base64_decode("Zg");
    ASSERT_TRUE(unpadded.has_value());
    EXPECT_EQ(*unpadded, "f");

    const auto unpadded_two = base64_decode("Zm8");
    ASSERT_TRUE(unpadded_two.has_value());
    EXPECT_EQ(*unpadded_two, "fo");
}

TEST(Base64, PaddingEndsTheDataEvenWithTrailingBytes) {
    // '=' means end-of-data, so anything after it is not decoded. Pinning
    // this stops a future "skip padding and keep going" refactor from
    // quietly changing what a payload decodes to.
    const auto decoded = base64_decode("Zm9v=YmFy");
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, "foo");
}

TEST(Base64, EmptyInputRoundTripsToEmpty) {
    EXPECT_EQ(base64_encode(""), "");
    const auto decoded = base64_decode("");
    ASSERT_TRUE(decoded.has_value());
    EXPECT_TRUE(decoded->empty());
}
