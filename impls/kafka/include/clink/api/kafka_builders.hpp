// Fluent builders for the Kafka text source/sink factories.

#pragma once

#include <string>
#include <utility>

#include "clink/api/descriptors.hpp"

namespace clink::api {

// kafka_text_source: emits each message's payload as std::string.
// Available only when the cluster build linked librdkafka; otherwise
// the underlying connector throws on construction. `brokers` and
// `topic` are required.
class KafkaTextSource {
public:
    class Builder {
    public:
        Builder& brokers(std::string b) {
            brokers_ = std::move(b);
            return *this;
        }
        Builder& topic(std::string t) {
            topic_ = std::move(t);
            return *this;
        }
        Builder& group_id(std::string g) {
            group_id_ = std::move(g);
            return *this;
        }
        Builder& client_id(std::string c) {
            client_id_ = std::move(c);
            return *this;
        }
        Builder& auto_offset_reset(std::string v) {
            auto_offset_reset_ = std::move(v);
            return *this;
        }
        Builder& from_beginning() {
            auto_offset_reset_ = "earliest";
            return *this;
        }

        SourceDescriptor build() const {
            SourceDescriptor d;
            d.op_type = "kafka_text_source";
            d.channel_type = "string";
            d.params["brokers"] = brokers_;
            d.params["topic"] = topic_;
            if (!group_id_.empty()) {
                d.params["group_id"] = group_id_;
            }
            if (!client_id_.empty()) {
                d.params["client_id"] = client_id_;
            }
            if (!auto_offset_reset_.empty()) {
                d.params["auto_offset_reset"] = auto_offset_reset_;
            }
            return d;
        }

    private:
        std::string brokers_;
        std::string topic_;
        std::string group_id_;
        std::string client_id_;
        std::string auto_offset_reset_;
    };

    static Builder builder() { return Builder{}; }
};

class KafkaTextSink {
public:
    class Builder {
    public:
        Builder& brokers(std::string b) {
            brokers_ = std::move(b);
            return *this;
        }
        Builder& topic(std::string t) {
            topic_ = std::move(t);
            return *this;
        }
        Builder& client_id(std::string c) {
            client_id_ = std::move(c);
            return *this;
        }
        Builder& acks(std::string v) {
            acks_ = std::move(v);
            return *this;
        }
        Builder& compression(std::string v) {
            compression_ = std::move(v);
            return *this;
        }

        SinkDescriptor build() const {
            SinkDescriptor d;
            d.op_type = "kafka_text_sink";
            d.channel_type = "string";
            d.params["brokers"] = brokers_;
            d.params["topic"] = topic_;
            if (!client_id_.empty()) {
                d.params["client_id"] = client_id_;
            }
            if (!acks_.empty()) {
                d.params["acks"] = acks_;
            }
            if (!compression_.empty()) {
                d.params["compression"] = compression_;
            }
            return d;
        }

    private:
        std::string brokers_;
        std::string topic_;
        std::string client_id_;
        std::string acks_;
        std::string compression_;
    };

    static Builder builder() { return Builder{}; }
};

}  // namespace clink::api
