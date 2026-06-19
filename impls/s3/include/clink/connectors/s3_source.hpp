#pragma once

#include <memory>
#include <optional>
#include <string>

#include "clink/operators/operator_base.hpp"

namespace clink {

// S3Source reads newline-delimited objects from an S3 bucket and emits
// std::string records (one per line). Suitable for log files, JSONL, CSV, or
// anything else newline-friendly. Implementation gated on AWS SDK
// (`find_package(AWSSDK COMPONENTS s3 transfer)`).
class S3Source final : public Source<std::string> {
public:
    struct Options {
        std::string bucket;
        std::string key_prefix;  // list and read every object under this prefix
        std::optional<std::string> region;
        std::optional<std::string> endpoint_override;  // e.g. for MinIO / LocalStack
    };

    explicit S3Source(Options opts);
    ~S3Source() override;

    S3Source(const S3Source&) = delete;
    S3Source& operator=(const S3Source&) = delete;
    S3Source(S3Source&&) = delete;
    S3Source& operator=(S3Source&&) = delete;

    void open() override;
    bool produce(Emitter<std::string>& out) override;
    void close() override;

    // Reading every object under a fixed prefix is a finite stream (BATCH-1).
    [[nodiscard]] bool is_bounded() const noexcept override { return true; }

    // #60: source replay. produce() emits one whole object per call, so the
    // cursor is the object index into the (lexicographically sorted) listing -
    // resuming = re-list, then skip the objects already emitted. open() sorts
    // the listing for a stable index and clamps a restored cursor. Exactly-once
    // holds while the object set under the prefix is unchanged between runs;
    // objects added/removed shift the index and can re-emit or skip an object.
    void snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId ckpt) override;
    bool restore_offset(StateBackend& backend, OperatorId op_id) override;

    std::string name() const override { return "s3_source"; }

    static bool is_real_implementation();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink
