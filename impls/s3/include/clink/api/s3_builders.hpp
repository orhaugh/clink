// Fluent builder for the s3_text_sink factory. Lives at
// include/clink/api/ during Phase 1 of the impls split; in Phase 2
// this header moves to impls/s3/include/clink/api/.

#pragma once

#include <cstdint>
#include <string>
#include <utility>

#include "clink/api/descriptors.hpp"

namespace clink::api {

// Writes batches of std::string records to S3 objects under
// bucket/key_prefix. Available only when the cluster build linked the
// AWS SDK; otherwise the underlying connector throws on construction.
class S3TextSink {
public:
    class Builder {
    public:
        Builder& bucket(std::string b) {
            bucket_ = std::move(b);
            return *this;
        }
        Builder& key_prefix(std::string p) {
            key_prefix_ = std::move(p);
            return *this;
        }
        Builder& region(std::string r) {
            region_ = std::move(r);
            return *this;
        }
        Builder& endpoint_override(std::string e) {
            endpoint_override_ = std::move(e);
            return *this;
        }
        Builder& rollover_bytes(std::int64_t b) {
            rollover_bytes_ = b;
            return *this;
        }

        SinkDescriptor build() const {
            SinkDescriptor d;
            d.op_type = "s3_text_sink";
            d.channel_type = "string";
            d.params["bucket"] = bucket_;
            if (!key_prefix_.empty()) {
                d.params["key_prefix"] = key_prefix_;
            }
            if (!region_.empty()) {
                d.params["region"] = region_;
            }
            if (!endpoint_override_.empty()) {
                d.params["endpoint_override"] = endpoint_override_;
            }
            if (rollover_bytes_ > 0) {
                d.params["rollover_bytes"] = std::to_string(rollover_bytes_);
            }
            return d;
        }

    private:
        std::string bucket_;
        std::string key_prefix_;
        std::string region_;
        std::string endpoint_override_;
        std::int64_t rollover_bytes_{0};
    };

    static Builder builder() { return Builder{}; }
};

}  // namespace clink::api
