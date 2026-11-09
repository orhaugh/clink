#include "clink/connectors/s3_source.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/metrics/connector_metrics.hpp"
#include "clink/runtime/log_buffer.hpp"

#ifdef CLINK_HAS_AWS_S3
#include <memory>

#include <aws/core/Aws.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3ClientConfiguration.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>

#include "clink/connectors/aws_sdk_init.hpp"
#endif

namespace clink {

#ifdef CLINK_HAS_AWS_S3

struct S3Source::Impl {
    Options opts;
    std::unique_ptr<Aws::S3::S3Client> client;
    std::vector<std::string> keys;
    std::size_t next_key{0};
    // The last object emitted, checkpointed: a restore resumes after it.
    std::optional<std::string> last_key;
    // restore_offset() read an earlier release's index, which names no object.
    bool restored_index{false};
};

bool S3Source::is_real_implementation() {
    return true;
}

S3Source::S3Source(Options opts) : impl_(std::make_unique<Impl>()) {
    impl_->opts = std::move(opts);
}

// No ShutdownAPI: the SDK is init-once / never-shutdown via the shared guard.
S3Source::~S3Source() = default;

void S3Source::open() {
    clink::aws_sdk::ensure_initialized();

    Aws::S3::S3ClientConfiguration cfg;
    if (impl_->opts.region.has_value()) {
        cfg.region = *impl_->opts.region;
    }
    if (impl_->opts.endpoint_override.has_value()) {
        cfg.endpointOverride = *impl_->opts.endpoint_override;
        // A custom endpoint (MinIO, LocalStack) is addressed by path, as the
        // S3 sinks address it: by virtual host, an endpoint named by host
        // (http://minio:9000) put the bucket in the host name, which the store
        // does not serve.
        cfg.useVirtualAddressing = false;
    }
    // Fail fast on unreachable endpoints. The AWS SDK's default retry
    // policy keeps retrying with exponential backoff for ~60s, which is
    // way more patience than a streaming source needs - by the time the
    // second attempt would land, the operator has already missed its
    // checkpoint barrier. One quick retry is plenty.
    cfg.connectTimeoutMs = 2000;
    cfg.requestTimeoutMs = 5000;
    cfg.retryStrategy = std::make_shared<Aws::Client::DefaultRetryStrategy>(1, 25);
    impl_->client = std::make_unique<Aws::S3::S3Client>(cfg);

    // A listing answers at most 1000 keys a page; every page is read, where
    // the first alone left the objects after it unread.
    Aws::S3::Model::ListObjectsV2Request req;
    req.SetBucket(impl_->opts.bucket);
    req.SetPrefix(impl_->opts.key_prefix);
    impl_->keys.clear();
    while (true) {
        auto rsp = impl_->client->ListObjectsV2(req);
        if (!rsp.IsSuccess()) {
            throw std::runtime_error("S3Source::open: ListObjectsV2 failed: " +
                                     rsp.GetError().GetMessage());
        }
        for (const auto& o : rsp.GetResult().GetContents()) {
            impl_->keys.push_back(o.GetKey());
        }
        if (!rsp.GetResult().GetIsTruncated()) {
            break;
        }
        const auto& token = rsp.GetResult().GetNextContinuationToken();
        if (token.empty()) {
            throw std::runtime_error(
                "S3Source::open: ListObjectsV2 said the listing continues but gave no "
                "continuation token");
        }
        req.SetContinuationToken(token);
    }
    // Objects are read in key order. A restore resumes after the last object
    // the checkpoint covered, by key: an index into the listing moved when an
    // object before it was removed or added, so the source skipped or repeated
    // one. An object added since with a key that sorts before the last one
    // emitted is not read; nothing in the listing after it is skipped.
    std::sort(impl_->keys.begin(), impl_->keys.end());
    impl_->next_key = 0;
    if (impl_->last_key.has_value()) {
        impl_->next_key = static_cast<std::size_t>(
            std::upper_bound(impl_->keys.begin(), impl_->keys.end(), *impl_->last_key) -
            impl_->keys.begin());
    } else if (impl_->restored_index) {
        clink::log::warn("source.s3",
                         "s3 source: the checkpoint holds an earlier release's position, an "
                         "index that names no object, so every object under the prefix is "
                         "read again rather than resumed by index");
    }
}

bool S3Source::produce(Emitter<std::string>& out) {
    if (this->cancelled() || impl_->next_key >= impl_->keys.size()) {
        return false;
    }
    const std::string& key = impl_->keys[impl_->next_key++];
    Aws::S3::Model::GetObjectRequest req;
    req.SetBucket(impl_->opts.bucket);
    req.SetKey(key);

    auto rsp = impl_->client->GetObject(req);
    if (!rsp.IsSuccess()) {
        clink::metrics::connector::error_inc("s3", "source");
        throw std::runtime_error("S3Source: GetObject failed for " + key + ": " +
                                 rsp.GetError().GetMessage());
    }

    Batch<std::string> batch;
    std::uint64_t bytes_read = 0;
    auto& body = rsp.GetResultWithOwnership().GetBody();
    std::string line;
    while (std::getline(body, line)) {
        bytes_read += line.size() + 1;  // include the consumed newline
        batch.emplace(std::move(line));
    }
    if (!batch.empty()) {
        clink::metrics::connector::records_in_inc("s3", batch.size());
        clink::metrics::connector::bytes_in_inc("s3", bytes_read);
        out.emit_data(std::move(batch));
    }
    impl_->last_key = key;
    return impl_->next_key < impl_->keys.size();
}

void S3Source::close() {
    if (impl_) {
        impl_->client.reset();
    }
}

namespace {
constexpr const char* kS3OffsetKey = "__s3_source_object__";
// The checkpoint's form: a version byte, then a byte saying whether an object
// was emitted, then that object's key. An earlier release wrote an index into
// the listing alone, in 8 bytes.
constexpr std::uint8_t kOffsetVersion = 2;
}  // namespace

void S3Source::snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId) {
    std::string bytes;
    bytes.push_back(static_cast<char>(kOffsetVersion));
    bytes.push_back(impl_->last_key.has_value() ? 1 : 0);
    if (impl_->last_key.has_value()) {
        bytes += *impl_->last_key;
    }
    backend.put_operator_state(op_id,
                               StateBackend::KeyView{kS3OffsetKey, std::strlen(kS3OffsetKey)},
                               StateBackend::ValueView{bytes.data(), bytes.size()});
}

bool S3Source::restore_offset(StateBackend& backend, OperatorId op_id) {
    auto v = backend.get_operator_state(
        op_id, StateBackend::KeyView{kS3OffsetKey, std::strlen(kS3OffsetKey)});
    if (!v.has_value()) {
        return false;
    }
    const auto byte_at = [&](std::size_t i) { return std::to_integer<std::uint8_t>((*v)[i]); };
    impl_->last_key.reset();
    impl_->restored_index = false;
    if (v->size() >= 2 && byte_at(0) == kOffsetVersion && byte_at(1) <= 1 &&
        (byte_at(1) == 1 || v->size() == 2)) {
        if (byte_at(1) == 1) {
            std::string key;
            key.reserve(v->size() - 2);
            for (std::size_t i = 2; i < v->size(); ++i) {
                key.push_back(static_cast<char>(byte_at(i)));
            }
            impl_->last_key = std::move(key);
        }
    } else if (v->size() == 8) {
        impl_->restored_index = true;
    } else {
        throw std::runtime_error(
            "s3 source: the checkpoint's position is in a form this release does not read (" +
            std::to_string(v->size()) + " bytes)");
    }
    return true;
}

#else

struct S3Source::Impl {};

bool S3Source::is_real_implementation() {
    return false;
}

S3Source::S3Source(Options /*opts*/) {
    throw std::runtime_error(
        "S3Source: built without AWS SDK. Install it (e.g. "
        "`brew install aws-sdk-cpp`) and reconfigure cmake - "
        "find_package(AWSSDK COMPONENTS s3) must succeed.");
}

S3Source::~S3Source() = default;
void S3Source::open() {}
bool S3Source::produce(Emitter<std::string>& /*out*/) {
    return false;
}
void S3Source::close() {}
void S3Source::snapshot_offset(StateBackend&, OperatorId, CheckpointId) {}
bool S3Source::restore_offset(StateBackend&, OperatorId) {
    return false;
}

#endif

}  // namespace clink
