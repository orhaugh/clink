// The derived codec's layout is a durability contract (design record 009,
// increment 1). These tests ARE the specification before the frozen-bytes
// fixture exists: the golden byte sequence is written out by hand, field
// by field, so the encoder is answerable to a reading of the spec rather
// than to itself. The fixture in tests/test_format_fixtures.cpp then pins
// the same bytes across builds.

#include <cstdint>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/derived_codec.hpp"

// Described types live at namespace scope: CLINK_ARROW_FIELDS specialises
// clink::ArrowFields<T>, which an anonymous namespace cannot do. Names are
// Dc-prefixed so nothing collides across test TUs.

struct DcNested {
    std::int32_t n{};
    std::string tag;
    bool operator==(const DcNested&) const = default;
};
CLINK_ARROW_FIELDS(DcNested, n, tag);

struct DcAllKinds {
    std::int8_t i8{};
    std::int16_t i16{};
    std::int32_t i32{};
    std::int64_t i64{};
    std::uint8_t u8{};
    std::uint16_t u16{};
    std::uint32_t u32{};
    std::uint64_t u64{};
    float f{};
    double d{};
    bool flag{};
    std::string s;
    std::optional<std::int64_t> maybe;
    std::vector<std::int32_t> xs;
    std::map<std::int32_t, std::string> m;
    DcNested inner;
    bool operator==(const DcAllKinds&) const = default;
};
// Sixteen fields: deliberately at the macro's documented limit.
CLINK_ARROW_FIELDS(
    DcAllKinds, i8, i16, i32, i64, u8, u16, u32, u64, f, d, flag, s, maybe, xs, m, inner);

// The heavy_pipeline_job shapes with their hand codecs replicated verbatim,
// so byte-identity here is what licenses deleting those hand codecs later
// (increment 4).
struct DcCustomer {
    std::int64_t id{0};
    std::string region;
    std::string product;
    std::int64_t amount{0};
    bool operator==(const DcCustomer&) const = default;
};
CLINK_ARROW_FIELDS(DcCustomer, id, region, product, amount);

struct DcOrder {
    std::string region;
    std::int64_t total_amount{0};
    std::int64_t count{0};
    bool operator==(const DcOrder&) const = default;
};
CLINK_ARROW_FIELDS(DcOrder, region, total_amount, count);

struct DcFp {
    float f{};
    double d{};
};
CLINK_ARROW_FIELDS(DcFp, f, d);

struct DcDeep {
    std::optional<std::vector<std::int64_t>> ov;
    bool operator==(const DcDeep&) const = default;
};
CLINK_ARROW_FIELDS(DcDeep, ov);

namespace {

using Bytes = std::vector<std::byte>;

void dc_put_u32(Bytes& out, std::uint32_t v) {
    for (int i = 0; i < 4; ++i) {
        out.push_back(static_cast<std::byte>((v >> (i * 8)) & 0xFF));
    }
}
void dc_put_i64(Bytes& out, std::int64_t v) {
    const auto u = static_cast<std::uint64_t>(v);
    for (int i = 0; i < 8; ++i) {
        out.push_back(static_cast<std::byte>((u >> (i * 8)) & 0xFF));
    }
}
void dc_put_str(Bytes& out, const std::string& s) {
    dc_put_u32(out, static_cast<std::uint32_t>(s.size()));
    const auto* p = reinterpret_cast<const std::byte*>(s.data());
    out.insert(out.end(), p, p + s.size());
}

// The hand codecs from examples/heavy_pipeline_job.cpp, layout preserved.
clink::Codec<DcCustomer> dc_hand_customer_codec() {
    return clink::Codec<DcCustomer>{.encode =
                                        [](const DcCustomer& c) {
                                            Bytes out;
                                            dc_put_i64(out, c.id);
                                            dc_put_str(out, c.region);
                                            dc_put_str(out, c.product);
                                            dc_put_i64(out, c.amount);
                                            return out;
                                        },
                                    .decode = nullptr,
                                    .encode_into = nullptr};
}
clink::Codec<DcOrder> dc_hand_order_codec() {
    return clink::Codec<DcOrder>{.encode =
                                     [](const DcOrder& o) {
                                         Bytes out;
                                         dc_put_str(out, o.region);
                                         dc_put_i64(out, o.total_amount);
                                         dc_put_i64(out, o.count);
                                         return out;
                                     },
                                 .decode = nullptr,
                                 .encode_into = nullptr};
}

DcAllKinds golden_value() {
    DcAllKinds v;
    v.i8 = -1;
    v.i16 = 2;
    v.i32 = -3;
    v.i64 = 4;
    v.u8 = 5;
    v.u16 = 6;
    v.u32 = 7;
    v.u64 = 8;
    v.f = 1.0F;
    v.d = -2.0;
    v.flag = true;
    v.s = "ab";
    v.maybe = 9;
    v.xs = {1, -1};
    v.m = {{1, "x"}};
    v.inner = DcNested{7, "z"};
    return v;
}

// The specification, written as bytes by hand from the header's layout
// table. 92 bytes. If this array and the encoder ever disagree, the array
// wins: it is the contract.
Bytes golden_bytes() {
    const std::uint8_t raw[] = {
        0xFF,                                            // i8   = -1
        0x02, 0x00,                                      // i16  = 2
        0xFD, 0xFF, 0xFF, 0xFF,                          // i32  = -3
        0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  // i64  = 4
        0x05,                                            // u8   = 5
        0x06, 0x00,                                      // u16  = 6
        0x07, 0x00, 0x00, 0x00,                          // u32  = 7
        0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  // u64  = 8
        0x00, 0x00, 0x80, 0x3F,                          // f    = 1.0f  (0x3F800000)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0,  // d    = -2.0  (0xC000...)
        0x01,                                            // flag = true
        0x02, 0x00, 0x00, 0x00, 0x61, 0x62,              // s    = "ab"
        0x01,                                            // maybe present
        0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  // maybe = 9
        0x02, 0x00, 0x00, 0x00,                          // xs count = 2
        0x01, 0x00, 0x00, 0x00,                          // xs[0] = 1
        0xFF, 0xFF, 0xFF, 0xFF,                          // xs[1] = -1
        0x01, 0x00, 0x00, 0x00,                          // m count = 1
        0x01, 0x00, 0x00, 0x00,                          // m key = 1
        0x01, 0x00, 0x00, 0x00, 0x78,                    // m val = "x"
        0x07, 0x00, 0x00, 0x00,                          // inner.n = 7
        0x01, 0x00, 0x00, 0x00, 0x7A,                    // inner.tag = "z"
    };
    Bytes out;
    for (const auto b : raw) {
        out.push_back(static_cast<std::byte>(b));
    }
    return out;
}

// Byte offsets into the golden encoding, named so the strictness tests
// read as intent rather than arithmetic.
constexpr std::size_t kFlagOffset = 1 + 2 + 4 + 8 + 1 + 2 + 4 + 8 + 4 + 8;  // = 42
constexpr std::size_t kMaybePresenceOffset = kFlagOffset + 1 + (4 + 2);     // = 49

TEST(DerivedCodec, EncodesTheGoldenValueToTheGoldenBytes) {
    const auto codec = clink::derived_codec<DcAllKinds>();
    EXPECT_EQ(codec.encode(golden_value()), golden_bytes());
}

TEST(DerivedCodec, DecodesTheGoldenBytesToTheGoldenValue) {
    const auto codec = clink::derived_codec<DcAllKinds>();
    const auto v = codec.decode(golden_bytes());
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(*v, golden_value());
}

TEST(DerivedCodec, EncodeIntoAppendsExactlyEncodesBytes) {
    const auto codec = clink::derived_codec<DcAllKinds>();
    Bytes out;
    out.push_back(std::byte{0xAA});  // pre-existing prefix must survive
    clink::encode_append(codec, golden_value(), out);
    Bytes expected;
    expected.push_back(std::byte{0xAA});
    const auto enc = codec.encode(golden_value());
    expected.insert(expected.end(), enc.begin(), enc.end());
    EXPECT_EQ(out, expected);
}

TEST(DerivedCodec, MatchesTheHandWrittenIdiomByteForByte) {
    // What licenses deleting the examples' hand codecs: same shapes, same
    // bytes, including empty strings and negative values.
    const auto dcust = clink::derived_codec<DcCustomer>();
    const auto hcust = dc_hand_customer_codec();
    for (const auto& c : {DcCustomer{1, "NA", "widget", 10},
                          DcCustomer{-5, "", "", -1},
                          DcCustomer{INT64_MIN, "EU", std::string("a\0b", 3), INT64_MAX}}) {
        EXPECT_EQ(dcust.encode(c), hcust.encode(c));
    }
    const auto dord = clink::derived_codec<DcOrder>();
    const auto hord = dc_hand_order_codec();
    for (const auto& o : {DcOrder{"NA", 4350, 30}, DcOrder{"", 0, 0}, DcOrder{"x", -1, -1}}) {
        EXPECT_EQ(dord.encode(o), hord.encode(o));
    }
}

TEST(DerivedCodec, RoundTripsDefaultAndEdgeValues) {
    const auto codec = clink::derived_codec<DcAllKinds>();
    for (const auto& v : {DcAllKinds{},  // empty string/vector/map, nullopt, zeros
                          golden_value()}) {
        const auto back = codec.decode(codec.encode(v));
        ASSERT_TRUE(back.has_value());
        EXPECT_EQ(*back, v);
    }
    const auto deep = clink::derived_codec<DcDeep>();
    for (const auto& v : {DcDeep{},                             // absent
                          DcDeep{std::vector<std::int64_t>{}},  // present, empty
                          DcDeep{std::vector<std::int64_t>{1, -2, 3}}}) {
        const auto back = deep.decode(deep.encode(v));
        ASSERT_TRUE(back.has_value());
        EXPECT_EQ(*back, v);
    }
}

TEST(DerivedCodec, PreservesFloatBitPatternsIncludingNaN) {
    const auto codec = clink::derived_codec<DcFp>();
    DcFp v;
    v.f = -0.0F;
    v.d = std::numeric_limits<double>::quiet_NaN();
    const auto back = codec.decode(codec.encode(v));
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(std::bit_cast<std::uint32_t>(back->f), std::bit_cast<std::uint32_t>(v.f));
    EXPECT_EQ(std::bit_cast<std::uint64_t>(back->d), std::bit_cast<std::uint64_t>(v.d));
}

TEST(DerivedCodec, EveryTruncationFailsClosed) {
    const auto codec = clink::derived_codec<DcAllKinds>();
    const auto full = golden_bytes();
    for (std::size_t len = 0; len < full.size(); ++len) {
        const auto v = codec.decode(clink::Codec<DcAllKinds>::BytesView{full.data(), len});
        EXPECT_FALSE(v.has_value()) << "a " << len << "-byte prefix decoded";
    }
}

TEST(DerivedCodec, TrailingBytesAreRefused) {
    // Decision 1 of the wave plan: decode consumes the buffer exactly.
    const auto codec = clink::derived_codec<DcAllKinds>();
    auto padded = golden_bytes();
    padded.push_back(std::byte{0});
    EXPECT_FALSE(codec.decode(padded).has_value());
}

TEST(DerivedCodec, ABoolOrPresenceByteAboveOneIsRefused) {
    const auto codec = clink::derived_codec<DcAllKinds>();
    auto mutant = golden_bytes();
    mutant[kFlagOffset] = std::byte{2};
    EXPECT_FALSE(codec.decode(mutant).has_value()) << "bool byte 2 decoded";
    mutant = golden_bytes();
    mutant[kMaybePresenceOffset] = std::byte{2};
    EXPECT_FALSE(codec.decode(mutant).has_value()) << "presence byte 2 decoded";
}

TEST(DerivedCodec, NoSingleFlippedByteRoundTripsSilently) {
    // The mutation check as a test: flipping any byte of a valid encoding
    // must either fail decode or produce a visibly different value. A
    // flipped byte that decodes back to the ORIGINAL would mean a byte the
    // format ignores, which is a byte the fixture cannot pin.
    const auto codec = clink::derived_codec<DcAllKinds>();
    const auto original = golden_value();
    const auto full = golden_bytes();
    for (std::size_t i = 0; i < full.size(); ++i) {
        auto mutant = full;
        mutant[i] ^= std::byte{0xFF};
        const auto v = codec.decode(mutant);
        EXPECT_TRUE(!v.has_value() || !(*v == original)) << "byte " << i << " is inert";
    }
}

TEST(DerivedCodec, AnOversizeLengthThrowsInsteadOfWrapping) {
    // Decision 2: u32 lengths; wrapping would be silent corruption. Tested
    // at the helper because materialising a >4GiB value in a unit test is
    // not reasonable.
    Bytes out;
    EXPECT_THROW(clink::derived_codec_detail::put_len(out, (5ULL << 30)), std::length_error);
}

}  // namespace
