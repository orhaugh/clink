#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace clink {

// Single Kafka header, byte-transparent (values may contain NULs and
// non-UTF-8 bytes).
struct KafkaHeader {
    std::string key;
    std::string value;
};

// In-flight Kafka record exchanged with the source/sink. The shape is
// intentionally close to the wire so users don't lose fidelity at the
// boundary:
//
//   * payload  - the message value bytes.
//   * key      - partitioning key. nullopt means "let librdkafka decide"
//                on the producer side; on the consumer side, nullopt means
//                the broker delivered no key.
//   * headers  - record headers. Order is preserved on encode; the broker
//                does not reorder them.
//   * offset / partition / timestamp_ms - populated by the source from the
//                broker. Sinks ignore offset and timestamp_ms; partition
//                is honoured if non-negative (overrides the configured
//                fixed partition for that record).
//
// All integer fields use -1 as "unset" so a default-constructed instance
// makes sense as a sink input without forcing the user to init them.
struct KafkaMessage {
    std::string payload;
    std::optional<std::string> key;
    std::vector<KafkaHeader> headers;
    std::int64_t offset{-1};
    std::int32_t partition{-1};
    std::int64_t timestamp_ms{-1};

    KafkaMessage() = default;
    explicit KafkaMessage(std::string payload_value) : payload(std::move(payload_value)) {}
    KafkaMessage(std::string payload_value, std::string key_value)
        : payload(std::move(payload_value)), key(std::move(key_value)) {}
};

}  // namespace clink
