#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "clink/operators/operator_base.hpp"

namespace clink {

// S3Sink writes std::string records, one per line, into objects under the
// given bucket/key_prefix: a part is put when the buffer reaches
// rollover_bytes and at the end of input, as `<key_prefix>/part-<n>`, or
// `<key_prefix>/sub<i>-part-<n>` when the sink runs at parallelism above 1 so
// that subtasks never name the same object. A run restored from a checkpoint
// numbers its parts after the ones already there, whose records its sources
// do not replay; a fresh run starts at 0. Delivery is at-most-once: a failure
// loses what was buffered since the last part.
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
        // The subtask this sink is, of how many: above 1, part keys carry the
        // subtask index.
        std::uint32_t subtask_idx{0};
        std::uint32_t parallelism{1};
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
    [[nodiscard]] std::string part_leaf_prefix_() const;
    std::string next_part_key_();
    [[nodiscard]] std::optional<std::size_t> highest_part_() const;

    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink
