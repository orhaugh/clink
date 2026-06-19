// Avro codec round-trip tests. Uses a hand-built minimal Avro schema +
// generated record so the test doesn't need to invoke avrogencpp at
// build time. The record shape matches what a `record Greeting { long
// id; string message; }` would compile to via avrogencpp; the test
// asserts that
//   * binary_codec round-trips
//   * json_codec round-trips (with schema file on disk)
//   * keyed_record_codec round-trips key + payload

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <utility>

#include <avro/Decoder.hh>
#include <avro/Encoder.hh>
#include <avro/Specific.hh>
#include <gtest/gtest.h>

#include "clink/avro/binary_codec.hpp"
#include "clink/avro/json_codec.hpp"
#include "clink/avro/keyed_record_codec.hpp"

namespace test_avro {

// Hand-written analogue of an avrogencpp-generated record for the
// schema `record Greeting { long id; string message; }`. The struct
// needs an `avro::codec_traits` specialization to be usable with the
// binary/json encoders; that lives just below.
struct Greeting {
    std::int64_t id{0};
    std::string message;

    bool operator==(const Greeting& other) const {
        return id == other.id && message == other.message;
    }
};

}  // namespace test_avro

namespace avro {
template <>
struct codec_traits<test_avro::Greeting> {
    static void encode(Encoder& e, const test_avro::Greeting& v) {
        ::avro::encode(e, v.id);
        ::avro::encode(e, v.message);
    }
    static void decode(Decoder& d, test_avro::Greeting& v) {
        ::avro::decode(d, v.id);
        ::avro::decode(d, v.message);
    }
};
}  // namespace avro

namespace {

constexpr const char* kGreetingSchema = R"({
  "type": "record",
  "name": "Greeting",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "message", "type": "string"}
  ]
})";

std::filesystem::path write_schema() {
    auto p = std::filesystem::temp_directory_path() / "clink_avro_greeting.avsc";
    std::ofstream o(p, std::ios::trunc);
    o << kGreetingSchema;
    return p;
}

}  // namespace

TEST(AvroBinaryCodec, RoundTripsRecord) {
    auto c = clink::avro::binary_codec<test_avro::Greeting>();
    test_avro::Greeting in{42, "hello"};
    auto bytes = c.encode(in);
    auto out = c.decode(bytes);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(*out, in);
}

TEST(AvroBinaryCodec, RoundTripsEmptyMessage) {
    auto c = clink::avro::binary_codec<test_avro::Greeting>();
    test_avro::Greeting in{0, ""};
    auto bytes = c.encode(in);
    auto out = c.decode(bytes);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(*out, in);
}

TEST(AvroJsonCodec, RoundTripsRecord) {
    auto path = write_schema();
    auto c = clink::avro::json_codec<test_avro::Greeting>(path.string());
    test_avro::Greeting in{7, "world"};
    auto bytes = c.encode(in);
    auto out = c.decode(bytes);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(*out, in);
}

TEST(AvroKeyedRecordCodec, RoundTripsKeyAndPayload) {
    auto c = clink::avro::keyed_record_codec<test_avro::Greeting>();
    clink::avro::KeyedRecord<test_avro::Greeting> in{
        .key = "customer-42:cluster-7:endpoint-1",
        .payload = test_avro::Greeting{99, "keyed payload"},
    };
    auto bytes = c.encode(in);
    auto out = c.decode(bytes);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->key, in.key);
    EXPECT_EQ(out->payload, in.payload);
}

TEST(AvroKeyedRecordCodec, RoundTripsEmptyKey) {
    auto c = clink::avro::keyed_record_codec<test_avro::Greeting>();
    clink::avro::KeyedRecord<test_avro::Greeting> in{.key = "",
                                                     .payload = test_avro::Greeting{1, "x"}};
    auto bytes = c.encode(in);
    auto out = c.decode(bytes);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->key, "");
    EXPECT_EQ(out->payload, in.payload);
}

TEST(AvroKeyedRecordCodec, RejectsTruncatedBuffer) {
    auto c = clink::avro::keyed_record_codec<test_avro::Greeting>();
    clink::avro::KeyedRecord<test_avro::Greeting> in{.key = "k",
                                                     .payload = test_avro::Greeting{1, "x"}};
    auto bytes = c.encode(in);
    std::vector<std::byte> truncated(bytes.begin(), bytes.end() - 1);
    EXPECT_FALSE(c.decode(truncated).has_value());
}
