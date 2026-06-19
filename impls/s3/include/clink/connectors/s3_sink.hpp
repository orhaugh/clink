#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <string>

#include "clink/operators/operator_base.hpp"

namespace clink {

// S3Sink writes batches of std::string records into objects under the given
// bucket/key_prefix. Each batch becomes a single object whose key is
// `<key_prefix>/<batch_index>` (or a user-supplied namer once we add one).
//
// Implementation is in src/connectors/s3_sink.cpp; gated on AWS SDK.
class S3Sink final : public Sink<std::string> {
public:
    struct Options {
        std::string bucket;
        std::string key_prefix;
        std::optional<std::string> region;
        std::optional<std::string> endpoint_override;
        std::size_t rollover_bytes{16 * 1024 * 1024};  // start a new object when reached
    };

    explicit S3Sink(Options opts);
    ~S3Sink() override;

    S3Sink(const S3Sink&) = delete;
    S3Sink& operator=(const S3Sink&) = delete;
    S3Sink(S3Sink&&) = delete;
    S3Sink& operator=(S3Sink&&) = delete;

    void open() override;
    void on_data(const Batch<std::string>& batch) override;
    void flush() override;
    void close() override;

    std::string name() const override { return "s3_sink"; }

    static bool is_real_implementation();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink
