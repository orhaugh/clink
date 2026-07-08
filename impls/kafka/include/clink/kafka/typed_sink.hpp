// Typed Kafka sink helpers - symmetric to typed_source.hpp.
//
// Two flavours:
//
//   * `clink::kafka::text_sink<T>(stream, opts, encode_fn)` - maps
//     each T through `encode_fn` to a string, then sends the strings
//     to `kafka_text_sink`. No `nullopt` drop; the encoder is expected
//     to always succeed (use a `text_sink<std::optional<T>>` shape
//     if you want lossy serialization).
//
//   * `clink::kafka::message_sink(stream, opts)` - consumes a
//     `DataStream<KafkaMessage>` directly via the existing typed
//     `kafka_message_sink` factory.

#pragma once

#include <functional>
#include <map>
#include <string>
#include <utility>

#include "clink/api/descriptors.hpp"
#include "clink/api/kafka_builders.hpp"
#include "clink/api/stream_execution_environment.hpp"
#include "clink/connectors/kafka_message.hpp"
#include "clink/kafka/kafka_message_codec.hpp"

namespace clink::kafka {

struct KafkaSinkOptions {
    std::string brokers;
    std::string topic;
    std::string client_id;
    std::string acks = "all";
    std::string compression;
    // Producer linger.ms; empty keeps the connector default (5ms).
    std::string linger_ms;
};

inline clink::api::SinkDescriptor make_text_sink_descriptor(const KafkaSinkOptions& opts) {
    auto b = clink::api::KafkaTextSink::builder().brokers(opts.brokers).topic(opts.topic);
    if (!opts.client_id.empty()) {
        b.client_id(opts.client_id);
    }
    if (!opts.acks.empty()) {
        b.acks(opts.acks);
    }
    if (!opts.compression.empty()) {
        b.compression(opts.compression);
    }
    if (!opts.linger_ms.empty()) {
        b.linger_ms(opts.linger_ms);
    }
    return b.build();
}

template <typename T>
inline void text_sink(clink::api::DataStream<T> stream,
                      const KafkaSinkOptions& opts,
                      std::function<std::string(const T&)> encode_fn,
                      std::string id = {}) {
    stream.template map<std::string>(std::move(encode_fn))
        .sink(make_text_sink_descriptor(opts), std::move(id));
}

inline void message_sink(clink::api::DataStream<clink::KafkaMessage> stream,
                         const KafkaSinkOptions& opts,
                         std::string id = {}) {
    clink::api::SinkDescriptor d;
    d.op_type = "kafka_message_sink";
    d.channel_type = std::string{clink::kChannelKafkaMessage};
    d.params["brokers"] = opts.brokers;
    d.params["topic"] = opts.topic;
    if (!opts.client_id.empty()) {
        d.params["client_id"] = opts.client_id;
    }
    if (!opts.acks.empty()) {
        d.params["acks"] = opts.acks;
    }
    if (!opts.compression.empty()) {
        d.params["compression"] = opts.compression;
    }
    if (!opts.linger_ms.empty()) {
        d.params["linger_ms"] = opts.linger_ms;
    }
    stream.sink(std::move(d), std::move(id));
}

}  // namespace clink::kafka
