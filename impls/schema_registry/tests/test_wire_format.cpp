// The Confluent wire format: header layout, the protobuf message-index
// array (including its single-zero shorthand) and the refusals.
#include <gtest/gtest.h>

#include "clink/schema_registry/wire_format.hpp"

namespace clink::schema_registry {
namespace {

TEST(WireFormat, FramesMagicByteAndBigEndianId) {
    const auto f = frame(0x01020304, "payload");
    ASSERT_EQ(f.size(), 5u + 7u);
    EXPECT_EQ(static_cast<unsigned char>(f[0]), 0x00);
    EXPECT_EQ(static_cast<unsigned char>(f[1]), 0x01);
    EXPECT_EQ(static_cast<unsigned char>(f[2]), 0x02);
    EXPECT_EQ(static_cast<unsigned char>(f[3]), 0x03);
    EXPECT_EQ(static_cast<unsigned char>(f[4]), 0x04);
    EXPECT_EQ(f.substr(5), "payload");

    const auto parsed = parse_frame(f, /*with_message_indexes=*/false);
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed->schema_id, 0x01020304);
    EXPECT_TRUE(parsed->message_indexes.empty());
    EXPECT_EQ(parsed->payload, "payload");
}

TEST(WireFormat, RefusesWrongMagicByteAndShortInput) {
    std::string err;
    EXPECT_FALSE(parse_frame(std::string("\x01\x00\x00\x00\x01xyz", 8), false, &err).has_value());
    EXPECT_NE(err.find("magic byte"), std::string::npos) << err;
    EXPECT_FALSE(parse_frame(std::string("\x00\x00\x00", 3), false, &err).has_value());
    EXPECT_NE(err.find("too short"), std::string::npos) << err;
    // An empty payload after a complete header is legal (an empty record).
    const auto p = parse_frame(std::string("\x00\x00\x00\x00\x07", 5), false, &err);
    ASSERT_TRUE(p.has_value());
    EXPECT_EQ(p->schema_id, 7);
    EXPECT_TRUE(p->payload.empty());
}

TEST(WireFormat, ZigzagVarintRoundTrips) {
    for (const std::int64_t v :
         {0LL, 1LL, -1LL, 63LL, 64LL, -64LL, -65LL, 300LL, 1LL << 40, -(1LL << 40)}) {
        std::string out;
        append_zigzag_varint(out, v);
        std::string_view in = out;
        const auto back = read_zigzag_varint(in);
        ASSERT_TRUE(back.has_value()) << v;
        EXPECT_EQ(*back, v);
        EXPECT_TRUE(in.empty());
    }
    std::string_view truncated("\x80", 1);
    EXPECT_FALSE(read_zigzag_varint(truncated).has_value());
}

TEST(WireFormat, ProtobufIndexesUseTheSingleZeroShorthand) {
    const auto f = frame(9, std::vector<std::int32_t>{0}, "p");
    ASSERT_EQ(f.size(), 5u + 1u + 1u);
    EXPECT_EQ(f[5], '\0');
    const auto parsed = parse_frame(f, /*with_message_indexes=*/true);
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed->message_indexes, (std::vector<std::int32_t>{0}));
    EXPECT_EQ(parsed->payload, "p");
}

TEST(WireFormat, ProtobufIndexPathRoundTrips) {
    const std::vector<std::int32_t> path{2, 0, 5};
    const auto f = frame(9, path, "pp");
    // count 3 -> zigzag 6; then 4, 0, 10.
    EXPECT_EQ(static_cast<unsigned char>(f[5]), 6);
    EXPECT_EQ(static_cast<unsigned char>(f[6]), 4);
    EXPECT_EQ(static_cast<unsigned char>(f[7]), 0);
    EXPECT_EQ(static_cast<unsigned char>(f[8]), 10);
    const auto parsed = parse_frame(f, true);
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed->message_indexes, path);
    EXPECT_EQ(parsed->payload, "pp");
}

TEST(WireFormat, MalformedIndexArrayIsRefused) {
    std::string err;
    // count says 2, only one index present and it is truncated.
    std::string bad("\x00\x00\x00\x00\x01\x04\x80", 7);
    EXPECT_FALSE(parse_frame(bad, true, &err).has_value());
    EXPECT_NE(err.find("message-index"), std::string::npos) << err;
    // A negative index (zigzag of -1 is 1).
    std::string neg("\x00\x00\x00\x00\x01\x02\x01", 7);
    EXPECT_FALSE(parse_frame(neg, true, &err).has_value());
}

}  // namespace
}  // namespace clink::schema_registry
