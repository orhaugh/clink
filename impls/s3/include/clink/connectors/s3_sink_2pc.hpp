#pragma once

// S3Sink2PC - exactly-once raw-object S3 sink via multipart-upload-complete-on-commit.
//
// The staged-artifact adopter of CommittingSink for object storage. Records
// since the last barrier are buffered; at the barrier the whole interval is
// uploaded as the parts of an S3 multipart upload under a deterministic,
// checkpoint-tagged key, and the multipart handle (key + uploadId + part ETags)
// is the committable. The object does not exist until CompleteMultipartUpload,
// so the framework's on_commit makes it appear atomically once the checkpoint is
// globally durable; on_abort AbortMultipartUploads the parts.
//
// A multipart upload survives the session, so a crash between the barrier (parts
// uploaded) and commit does not lose data: on restart the framework
// CompleteMultipartUploads any handle in the restored checkpoint state. An
// upload whose handle never reached a durable checkpoint (a crash before the
// snapshot) is a benign orphan - it produces no visible object, only holds
// staged part storage - so it is left for an S3 lifecycle rule
// (AbortIncompleteMultipartUpload) to expire rather than reconciled here.
//
// Each input record is one line in the object body (newline-appended), matching
// the at-least-once S3Sink. The object key is
// "<key_prefix>/sub<N>-<ckpt>.ndjson", unique per (subtask, checkpoint).
//
// Memory: one checkpoint interval is buffered before the barrier uploads it
// (bounded by the checkpoint interval), the same trade the Parquet 2PC sinks
// make; commit is then a cheap metadata-only CompleteMultipartUpload.
//
// The AWS SDK stays confined to the .cpp (pImpl); only a POD handle is in the
// header. Gated on CLINK_HAS_AWS_S3.

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "clink/connectors/committing_sink.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink {

// The committable: everything CompleteMultipartUpload needs. POD, no AWS types.
struct S3MultipartHandle {
    std::string key;
    std::string upload_id;
    std::vector<std::pair<int, std::string>> parts;  // (partNumber, ETag), 1-based
};

class S3Sink2PC final : public CommittingSink<std::string, S3MultipartHandle> {
public:
    struct Options {
        std::string bucket;
        std::string key_prefix;
        std::optional<std::string> region;
        std::optional<std::string> endpoint_override;
        std::uint32_t subtask_idx{0};
        // Multipart part size. S3 requires every part except the last to be
        // >= 5 MiB; the default is that minimum.
        std::size_t part_size{5 * 1024 * 1024};
    };

    explicit S3Sink2PC(Options opts);
    ~S3Sink2PC() override;

    S3Sink2PC(const S3Sink2PC&) = delete;
    S3Sink2PC& operator=(const S3Sink2PC&) = delete;
    S3Sink2PC(S3Sink2PC&&) = delete;
    S3Sink2PC& operator=(S3Sink2PC&&) = delete;

    // CommittingSink verbs (defined in the .cpp, where the AWS SDK lives).
    void on_open() override;
    void write(const Batch<std::string>& batch) override;
    std::optional<S3MultipartHandle> prepare_commit(std::uint64_t checkpoint_id) override;
    bool commit(const S3MultipartHandle& handle) override;
    void abort(const S3MultipartHandle& handle) override;
    std::string serialize(const S3MultipartHandle& handle) const override;
    S3MultipartHandle deserialize(std::string_view bytes) const override;
    void close() override;

    std::string name() const override { return "s3_2pc_sink"; }

    static bool is_real_implementation();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink
