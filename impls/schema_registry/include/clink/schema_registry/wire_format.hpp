#pragma once
//
// The Confluent Schema Registry wire format: the five-byte header every
// registry-framed Kafka value starts with, plus the message-index array the
// Protobuf variant appends to it.
//
//   byte 0        magic byte, always 0x00
//   bytes 1..4    schema id, big-endian int32
//   (protobuf)    message indexes: zigzag-varint count, then that many
//                 zigzag-varint indexes into the schema's (nested) message
//                 list; the common case [0] is written as the single byte 0x00
//   rest          the encoded value (Avro binary, Protobuf binary, JSON text)
//
// Only the framing lives here. What the payload means is the format's business
// (formats.hpp).

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace clink::schema_registry {

inline constexpr std::uint8_t kMagicByte = 0x00;
inline constexpr std::size_t kHeaderSize = 5;

struct Frame {
    std::int32_t schema_id{0};
    // Protobuf only: the path to the message within its schema. {0} when the
    // producer wrote the single-zero shorthand. Empty for the other formats.
    std::vector<std::int32_t> message_indexes;
    // A view into the bytes handed to parse_frame; valid while they are.
    std::string_view payload;
};

// Parse a registry-framed value. `with_message_indexes` selects the Protobuf
// framing (index array after the id). Returns nullopt, with the reason in
// *error when given, for a wrong magic byte, a truncated header, or a
// malformed index array.
std::optional<Frame> parse_frame(std::string_view bytes,
                                 bool with_message_indexes,
                                 std::string* error = nullptr);

// Frame a payload for the Avro and JSON Schema formats.
std::string frame(std::int32_t schema_id, std::string_view payload);

// Frame a payload for the Protobuf format. `message_indexes` is written in
// the shorthand form when it is exactly {0}.
std::string frame(std::int32_t schema_id,
                  const std::vector<std::int32_t>& message_indexes,
                  std::string_view payload);

// The zigzag varint the index array uses (the same encoding as Avro's ints).
void append_zigzag_varint(std::string& out, std::int64_t v);
// Reads one zigzag varint from the front of `in`, advancing it. nullopt on a
// truncated or over-long encoding.
std::optional<std::int64_t> read_zigzag_varint(std::string_view& in);

}  // namespace clink::schema_registry
