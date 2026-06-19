// clink::avro::KeyedRecord<T> - pairs a partition key (UTF-8 string)
// with an Avro-binary-encoded payload of type T. The matching codec
// (`keyed_record_codec<T>()`) wires both pieces into a single
// state-stable byte buffer.
//
// Use case: Kafka-style "keyed record" topics where downstream
// operators need both the key (for partitioning / state-slot naming)
// AND the typed payload.
//
// Wire shape: 4-byte LE key length, key UTF-8 bytes, 4-byte LE payload
// length, Avro-binary payload bytes.

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <utility>

#include "clink/avro/binary_codec.hpp"
#include "clink/core/codec.hpp"

namespace clink::avro {
template <typename T>
struct KeyedRecord {
    std::string key;
    T payload;
};

template <typename T>
inline clink::Codec<KeyedRecord<T>> keyed_record_codec() {
    auto payload_codec = binary_codec<T>();
    return clink::Codec<KeyedRecord<T>>{
        .encode =
            [payload_codec](const KeyedRecord<T> &v) {
                typename clink::Codec<KeyedRecord<T>>::Bytes out;
                const auto key_len = static_cast<std::uint32_t>(v.key.size());
                for (int i = 0; i < 4; ++i) {
                    out.push_back(static_cast<std::byte>((key_len >> (i * 8)) & 0xFF));
                }
                for (char c : v.key) {
                    out.push_back(static_cast<std::byte>(c));
                }
                auto payload_bytes = payload_codec.encode(v.payload);
                const auto payload_len = static_cast<std::uint32_t>(payload_bytes.size());
                for (int i = 0; i < 4; ++i) {
                    out.push_back(static_cast<std::byte>((payload_len >> (i * 8)) & 0xFF));
                }
                for (auto b : payload_bytes) {
                    out.push_back(b);
                }
                return out;
            },
        .decode = [payload_codec](typename clink::Codec<KeyedRecord<T>>::BytesView buf)
            -> std::optional<KeyedRecord<T>> {
            if (buf.size() < 4) {
                return std::nullopt;
            }
            std::uint32_t key_len = 0;
            for (int i = 0; i < 4; ++i) {
                key_len |= static_cast<std::uint32_t>(static_cast<unsigned char>(buf[i]))
                           << (i * 8);
            }
            if (buf.size() < 4 + key_len + 4) {
                return std::nullopt;
            }
            KeyedRecord<T> out;
            out.key.reserve(key_len);
            for (std::size_t i = 0; i < key_len; ++i) {
                out.key.push_back(static_cast<char>(buf[4 + i]));
            }
            const std::size_t after_key = 4 + key_len;
            std::uint32_t payload_len = 0;
            for (int i = 0; i < 4; ++i) {
                payload_len |=
                    static_cast<std::uint32_t>(static_cast<unsigned char>(buf[after_key + i]))
                    << (i * 8);
            }
            if (buf.size() < after_key + 4 + payload_len) {
                return std::nullopt;
            }
            auto payload = payload_codec.decode(buf.subspan(after_key + 4, payload_len));
            if (!payload.has_value()) {
                return std::nullopt;
            }
            out.payload = std::move(*payload);
            return out;
        }};
}
}  // namespace clink::avro
