#include "clink/connectors/s3_source.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <utility>

#include "clink/metrics/connector_metrics.hpp"

#ifdef CLINK_HAS_AWS_S3
#include <memory>

#include <aws/core/Aws.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#endif

namespace clink {

#ifdef CLINK_HAS_AWS_S3

struct S3Source::Impl {
    Options opts;
    Aws::SDKOptions sdk_options;
    bool sdk_initialized{false};
    std::unique_ptr<Aws::S3::S3Client> client;
    std::vector<std::string> keys;
    std::size_t next_key{0};
};

bool S3Source::is_real_implementation() {
    return true;
}

S3Source::S3Source(Options opts) : impl_(std::make_unique<Impl>()) {
    impl_->opts = std::move(opts);
}

S3Source::~S3Source() {
    if (impl_ && impl_->sdk_initialized) {
        Aws::ShutdownAPI(impl_->sdk_options);
    }
}

void S3Source::open() {
    Aws::InitAPI(impl_->sdk_options);
    impl_->sdk_initialized = true;

    Aws::Client::ClientConfiguration cfg;
    if (impl_->opts.region.has_value()) {
        cfg.region = *impl_->opts.region;
    }
    if (impl_->opts.endpoint_override.has_value()) {
        cfg.endpointOverride = *impl_->opts.endpoint_override;
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

    Aws::S3::Model::ListObjectsV2Request req;
    req.SetBucket(impl_->opts.bucket);
    req.SetPrefix(impl_->opts.key_prefix);
    auto rsp = impl_->client->ListObjectsV2(req);
    if (!rsp.IsSuccess()) {
        throw std::runtime_error("S3Source::open: ListObjectsV2 failed: " +
                                 rsp.GetError().GetMessage());
    }
    for (const auto& o : rsp.GetResult().GetContents()) {
        impl_->keys.push_back(o.GetKey());
    }
    // #60: sort the listing so the replay cursor (object index) is stable
    // across runs regardless of SDK/page ordering, then clamp a cursor restored
    // by restore_offset() (which runs before open()) in case the object set
    // shrank between runs.
    std::sort(impl_->keys.begin(), impl_->keys.end());
    if (impl_->next_key > impl_->keys.size()) {
        impl_->next_key = impl_->keys.size();
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
    return impl_->next_key < impl_->keys.size();
}

void S3Source::close() {
    if (impl_) {
        impl_->client.reset();
        if (impl_->sdk_initialized) {
            Aws::ShutdownAPI(impl_->sdk_options);
            impl_->sdk_initialized = false;
        }
    }
}

namespace {
constexpr const char* kS3OffsetKey = "__s3_source_object__";
}  // namespace

void S3Source::snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId) {
    const auto cursor = static_cast<std::uint64_t>(impl_->next_key);
    std::array<std::byte, 8> bytes{};
    for (int i = 0; i < 8; ++i) {
        bytes[static_cast<std::size_t>(i)] = static_cast<std::byte>((cursor >> (i * 8)) & 0xFF);
    }
    backend.put_operator_state(
        op_id,
        StateBackend::KeyView{kS3OffsetKey, std::strlen(kS3OffsetKey)},
        StateBackend::ValueView{reinterpret_cast<const char*>(bytes.data()), bytes.size()});
}

bool S3Source::restore_offset(StateBackend& backend, OperatorId op_id) {
    auto v = backend.get_operator_state(
        op_id, StateBackend::KeyView{kS3OffsetKey, std::strlen(kS3OffsetKey)});
    if (!v.has_value() || v->size() < 8) {
        return false;
    }
    std::uint64_t restored = 0;
    for (int i = 0; i < 8; ++i) {
        restored |= static_cast<std::uint64_t>(static_cast<std::uint8_t>((*v)[i])) << (i * 8);
    }
    impl_->next_key = static_cast<std::size_t>(restored);
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
