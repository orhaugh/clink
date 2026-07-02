#include "clink/connectors/s3_sink_2pc.hpp"

#include <algorithm>
#include <chrono>
#include <stdexcept>
#include <utility>

#include "clink/config/json.hpp"
#include "clink/metrics/connector_metrics.hpp"

#ifdef CLINK_HAS_AWS_S3
#include <aws/core/Aws.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3ClientConfiguration.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompletedMultipartUpload.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>

#include "clink/connectors/aws_sdk_init.hpp"
#endif

namespace clink {

// --- committable codec (available regardless of AWS SDK) --------------------

std::string S3Sink2PC::serialize(const S3MultipartHandle& handle) const {
    clink::config::JsonObject obj;
    obj["key"] = clink::config::JsonValue{handle.key};
    obj["upload_id"] = clink::config::JsonValue{handle.upload_id};
    clink::config::JsonArray parts;
    for (const auto& [n, etag] : handle.parts) {
        clink::config::JsonObject p;
        p["n"] = clink::config::JsonValue{n};
        p["etag"] = clink::config::JsonValue{etag};
        parts.push_back(clink::config::JsonValue{std::move(p)});
    }
    obj["parts"] = clink::config::JsonValue{std::move(parts)};
    return clink::config::JsonValue{std::move(obj)}.serialize(0);
}

S3MultipartHandle S3Sink2PC::deserialize(std::string_view bytes) const {
    const auto js = clink::config::parse(bytes);
    S3MultipartHandle h;
    h.key = js.at("key").as_string();
    h.upload_id = js.at("upload_id").as_string();
    for (const auto& p : js.at("parts").as_array()) {
        h.parts.emplace_back(static_cast<int>(p.at("n").as_number()), p.at("etag").as_string());
    }
    return h;
}

#ifdef CLINK_HAS_AWS_S3

struct S3Sink2PC::Impl {
    Options opts;
    std::unique_ptr<Aws::S3::S3Client> client;
    std::string buffer;  // one checkpoint interval, uploaded at the barrier
};

bool S3Sink2PC::is_real_implementation() {
    return true;
}

S3Sink2PC::S3Sink2PC(Options opts)
    : CommittingSink<std::string, S3MultipartHandle>(opts.subtask_idx),
      impl_(std::make_unique<Impl>()) {
    if (opts.bucket.empty()) {
        throw std::runtime_error("S3Sink2PC: 'bucket' is required");
    }
    impl_->opts = std::move(opts);
}

S3Sink2PC::~S3Sink2PC() = default;

void S3Sink2PC::on_open() {
    clink::aws_sdk::ensure_initialized();
    Aws::S3::S3ClientConfiguration cfg;
    if (impl_->opts.region.has_value()) {
        cfg.region = *impl_->opts.region;
    }
    if (impl_->opts.endpoint_override.has_value()) {
        cfg.endpointOverride = *impl_->opts.endpoint_override;
        // MinIO / LocalStack (any custom endpoint) need path-style addressing.
        cfg.useVirtualAddressing = false;
    }
    impl_->client = std::make_unique<Aws::S3::S3Client>(cfg);
    // No orphaned-upload reconciliation: an incomplete multipart upload produces
    // no visible object (only staged part storage), so it is harmless for
    // correctness. Configure an S3 lifecycle rule (AbortIncompleteMultipartUpload)
    // to expire parts left by a crash before the checkpoint became durable.
}

void S3Sink2PC::write(const Batch<std::string>& batch) {
    std::uint64_t bytes_added = 0;
    for (const auto& r : batch) {
        bytes_added += r.value().size() + 1;
        impl_->buffer.append(r.value());
        impl_->buffer.push_back('\n');
    }
    clink::metrics::connector::records_out_inc("s3", batch.size());
    clink::metrics::connector::bytes_out_inc("s3", bytes_added);
}

namespace {

std::string object_key(const S3Sink2PC::Options& opts, std::uint64_t ckpt) {
    std::string prefix = opts.key_prefix;
    if (!prefix.empty() && prefix.back() == '/') {
        prefix.pop_back();
    }
    const std::string leaf =
        "sub" + std::to_string(opts.subtask_idx) + "-" + std::to_string(ckpt) + ".ndjson";
    return prefix.empty() ? leaf : prefix + "/" + leaf;
}

}  // namespace

std::optional<S3MultipartHandle> S3Sink2PC::prepare_commit(std::uint64_t checkpoint_id) {
    if (impl_->buffer.empty()) {
        return std::nullopt;  // no records this interval -> nothing to commit
    }
    const std::string& bucket = impl_->opts.bucket;
    const std::string key = object_key(impl_->opts, checkpoint_id);

    // Start the multipart upload.
    Aws::S3::Model::CreateMultipartUploadRequest cr;
    cr.SetBucket(bucket);
    cr.SetKey(key);
    auto cout = impl_->client->CreateMultipartUpload(cr);
    if (!cout.IsSuccess()) {
        clink::metrics::connector::error_inc("s3");
        throw std::runtime_error("S3Sink2PC: CreateMultipartUpload(" + key +
                                 "): " + cout.GetError().GetMessage());
    }
    const std::string upload_id = cout.GetResult().GetUploadId();

    // Upload the interval as parts. Every part except the last must be >= 5 MiB;
    // the buffer is chunked at part_size, so only the trailing remainder is smaller.
    S3MultipartHandle handle;
    handle.key = key;
    handle.upload_id = upload_id;
    const std::size_t part_size =
        impl_->opts.part_size < (5u * 1024 * 1024) ? (5u * 1024 * 1024) : impl_->opts.part_size;
    const std::string& buf = impl_->buffer;
    int part_number = 1;
    for (std::size_t off = 0; off < buf.size(); off += part_size, ++part_number) {
        const std::size_t len = std::min(part_size, buf.size() - off);
        auto stream = Aws::MakeShared<Aws::StringStream>("S3Sink2PC", std::ios::in | std::ios::out);
        stream->write(buf.data() + off, static_cast<std::streamsize>(len));
        Aws::S3::Model::UploadPartRequest up;
        up.SetBucket(bucket);
        up.SetKey(key);
        up.SetUploadId(upload_id);
        up.SetPartNumber(part_number);
        up.SetBody(stream);
        auto uout = impl_->client->UploadPart(up);
        if (!uout.IsSuccess()) {
            clink::metrics::connector::error_inc("s3");
            // Best-effort abort so the partial upload does not linger.
            abort(handle);
            throw std::runtime_error("S3Sink2PC: UploadPart(" + key + ", #" +
                                     std::to_string(part_number) +
                                     "): " + uout.GetError().GetMessage());
        }
        handle.parts.emplace_back(part_number, uout.GetResult().GetETag());
    }
    impl_->buffer.clear();
    return handle;
}

bool S3Sink2PC::commit(const S3MultipartHandle& handle) {
    const std::string& bucket = impl_->opts.bucket;
    Aws::S3::Model::CompletedMultipartUpload completed;
    for (const auto& [n, etag] : handle.parts) {
        Aws::S3::Model::CompletedPart p;
        p.SetPartNumber(n);
        p.SetETag(etag);
        completed.AddParts(std::move(p));
    }
    Aws::S3::Model::CompleteMultipartUploadRequest req;
    req.SetBucket(bucket);
    req.SetKey(handle.key);
    req.SetUploadId(handle.upload_id);
    req.SetMultipartUpload(std::move(completed));
    const auto t0 = std::chrono::steady_clock::now();
    auto out = impl_->client->CompleteMultipartUpload(req);
    if (!out.IsSuccess()) {
        // Idempotency: a retry after a successful complete finds the upload gone.
        // If the object already exists, the previous attempt committed it - no-op.
        Aws::S3::Model::HeadObjectRequest head;
        head.SetBucket(bucket);
        head.SetKey(handle.key);
        if (impl_->client->HeadObject(head).IsSuccess()) {
            return true;
        }
        clink::metrics::connector::error_inc("s3");
        throw std::runtime_error("S3Sink2PC: CompleteMultipartUpload(" + handle.key +
                                 "): " + out.GetError().GetMessage());
    }
    const auto dt =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0)
            .count();
    clink::metrics::connector::commit_latency_observe("s3", static_cast<std::uint64_t>(dt));
    return true;
}

void S3Sink2PC::abort(const S3MultipartHandle& handle) {
    Aws::S3::Model::AbortMultipartUploadRequest req;
    req.SetBucket(impl_->opts.bucket);
    req.SetKey(handle.key);
    req.SetUploadId(handle.upload_id);
    // Idempotent: a missing upload (already aborted/completed) is fine.
    (void)impl_->client->AbortMultipartUpload(req);
}

void S3Sink2PC::close() {
    if (impl_) {
        impl_->buffer.clear();  // an un-barriered tail is discarded (not committed)
        impl_->client.reset();
    }
}

#else  // !CLINK_HAS_AWS_S3

struct S3Sink2PC::Impl {};

bool S3Sink2PC::is_real_implementation() {
    return false;
}

S3Sink2PC::S3Sink2PC(Options /*opts*/) {
    throw std::runtime_error(
        "S3Sink2PC: built without AWS SDK. Install aws-sdk-cpp and reconfigure cmake.");
}

S3Sink2PC::~S3Sink2PC() = default;
void S3Sink2PC::on_open() {}
void S3Sink2PC::write(const Batch<std::string>& /*batch*/) {}
std::optional<S3MultipartHandle> S3Sink2PC::prepare_commit(std::uint64_t /*checkpoint_id*/) {
    return std::nullopt;
}
bool S3Sink2PC::commit(const S3MultipartHandle& /*handle*/) {
    return true;
}
void S3Sink2PC::abort(const S3MultipartHandle& /*handle*/) {}
void S3Sink2PC::close() {}

#endif

}  // namespace clink
