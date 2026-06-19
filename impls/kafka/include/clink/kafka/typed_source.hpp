// Typed Kafka source helper. Composes the existing string-typed
// `KafkaTextSource` builder with a user-supplied decoder map step, so
// the caller gets back a `DataStream<T>` instead of a
// `DataStream<std::string>` they then have to map themselves.
//
// Free function (not a fluent Builder) so the helper can register the
// per-T decode operator inline and append the SourceDescriptor in one
// call. T must already be a registered channel type - see
// `PluginRegistry::register_type<T>(name, codec)`.
//
// Two flavours:
//
//   * `clink::kafka::text_source<T>(env, opts, decode_fn)` -
//     consumes `KafkaTextSource` (payload-as-string) and maps each
//     line through `decode_fn` to T. Drops messages where decode
//     returns `nullopt` (the function signature is
//     `std::function<std::optional<T>(const std::string&)>`).
//
//   * `clink::kafka::message_source(env, opts)` - returns a
//     `DataStream<KafkaMessage>` directly off the existing
//     `kafka_message_source` typed factory. No decode; the user gets
//     payload + key + headers + partition.
//
// Sink helpers symmetric to these live in `typed_sink.hpp`.

#pragma once

#include <functional>
#include <map>
#include <optional>
#include <string>
#include <utility>

#include "clink/api/descriptors.hpp"
#include "clink/api/kafka_builders.hpp"
#include "clink/api/stream_execution_environment.hpp"
#include "clink/connectors/kafka_message.hpp"
#include "clink/kafka/kafka_message_codec.hpp"

namespace clink::kafka {

struct KafkaSourceOptions {
    std::string brokers;
    std::string topic;
    std::string group_id;
    std::string client_id;
    std::string auto_offset_reset;
};

inline clink::api::SourceDescriptor make_text_source_descriptor(const KafkaSourceOptions& opts) {
    auto b = clink::api::KafkaTextSource::builder().brokers(opts.brokers).topic(opts.topic);
    if (!opts.group_id.empty()) {
        b.group_id(opts.group_id);
    }
    if (!opts.client_id.empty()) {
        b.client_id(opts.client_id);
    }
    if (!opts.auto_offset_reset.empty()) {
        b.auto_offset_reset(opts.auto_offset_reset);
    }
    return b.build();
}

template <typename T>
inline clink::api::DataStream<T> text_source(
    clink::api::StreamExecutionEnvironment& env,
    const KafkaSourceOptions& opts,
    std::function<std::optional<T>(const std::string&)> decode_fn,
    std::string id = {}) {
    auto raw = env.template source<std::string>(make_text_source_descriptor(opts), id);
    // FlatMap drops nullopt decode results. We use flat_map because
    // map<T> requires a 1:1 function - `optional<T>` would need a
    // separate filter+map sequence.
    return raw.template flat_map<T>([decode_fn](const std::string& s) {
        std::vector<T> out;
        if (auto v = decode_fn(s); v.has_value()) {
            out.push_back(std::move(*v));
        }
        return out;
    });
}

inline clink::api::DataStream<clink::KafkaMessage> message_source(
    clink::api::StreamExecutionEnvironment& env,
    const KafkaSourceOptions& opts,
    std::string id = {}) {
    clink::api::SourceDescriptor d;
    d.op_type = "kafka_message_source";
    d.channel_type = std::string{clink::kChannelKafkaMessage};
    d.params["brokers"] = opts.brokers;
    d.params["topic"] = opts.topic;
    if (!opts.group_id.empty()) {
        d.params["group_id"] = opts.group_id;
    }
    if (!opts.client_id.empty()) {
        d.params["client_id"] = opts.client_id;
    }
    if (!opts.auto_offset_reset.empty()) {
        d.params["auto_offset_reset"] = opts.auto_offset_reset;
    }
    return env.template source<clink::KafkaMessage>(std::move(d), std::move(id));
}

}  // namespace clink::kafka
