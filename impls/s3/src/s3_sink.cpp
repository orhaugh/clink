#include "clink/connectors/s3_sink.hpp"

#include <algorithm>
#include <chrono>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/metrics/connector_metrics.hpp"

#ifdef CLINK_HAS_AWS_S3
#include <aws/core/Aws.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3ClientConfiguration.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>

#include "clink/connectors/aws_sdk_init.hpp"
#endif

namespace clink {

#ifdef CLINK_HAS_AWS_S3

struct S3Sink::Impl {
    Options opts;
    std::unique_ptr<Aws::S3::S3Client> client;
    std::string buffer;
    std::size_t next_part_index{0};
};

bool S3Sink::is_real_implementation() {
    return true;
}

S3Sink::S3Sink(Options opts) : impl_(std::make_unique<Impl>()) {
    impl_->opts = std::move(opts);
}

// No ShutdownAPI: the SDK is init-once / never-shutdown via the shared guard, so
// closing one connector never tears the SDK down under another's live client.
S3Sink::~S3Sink() = default;

void S3Sink::open() {
    clink::aws_sdk::ensure_initialized();
    Aws::S3::S3ClientConfiguration cfg;
    if (impl_->opts.region.has_value()) {
        cfg.region = *impl_->opts.region;
    }
    if (impl_->opts.endpoint_override.has_value()) {
        cfg.endpointOverride = *impl_->opts.endpoint_override;
        // MinIO / LocalStack (any custom endpoint) need path-style addressing,
        // as for the other S3 sinks.
        cfg.useVirtualAddressing = false;
    }
    impl_->client = std::make_unique<Aws::S3::S3Client>(cfg);
    impl_->next_part_index = 0;
    // A run restored from a checkpoint numbers its parts after the ones
    // already written. Its sources resume after the checkpoint, so the
    // records in those parts are not replayed, and numbering from 0 put the
    // new parts over them. A fresh run replays everything and starts at 0.
    if (runtime() != nullptr && runtime()->restore_from_checkpoint_id() > 0) {
        if (const auto top = highest_part_(); top.has_value()) {
            impl_->next_part_index = *top + 1;
        }
    }
}

std::string S3Sink::part_leaf_prefix_() const {
    // Beside other subtasks, each one names its parts with its index: the
    // same part numbers from two subtasks named one object, and the later put
    // replaced the earlier.
    return impl_->opts.parallelism > 1 ? "sub" + std::to_string(impl_->opts.subtask_idx) + "-part-"
                                       : std::string{"part-"};
}

std::string S3Sink::next_part_key_() {
    return impl_->opts.key_prefix + "/" + part_leaf_prefix_() +
           std::to_string(impl_->next_part_index++);
}

std::optional<std::size_t> S3Sink::highest_part_() const {
    const std::string list_prefix = impl_->opts.key_prefix + "/" + part_leaf_prefix_();
    Aws::S3::Model::ListObjectsV2Request req;
    req.SetBucket(impl_->opts.bucket);
    req.SetPrefix(list_prefix);
    std::optional<std::size_t> best;
    while (true) {
        auto out = impl_->client->ListObjectsV2(req);
        if (!out.IsSuccess()) {
            throw std::runtime_error(
                "S3Sink: ListObjectsV2(" + list_prefix +
                ") to number a restored run's parts: " + out.GetError().GetMessage());
        }
        for (const auto& object : out.GetResult().GetContents()) {
            const std::string key = object.GetKey();
            const std::string digits = key.substr(list_prefix.size());
            if (digits.empty() || digits.size() > 18 ||
                !std::all_of(
                    digits.begin(), digits.end(), [](char c) { return c >= '0' && c <= '9'; })) {
                continue;  // another subtask's layout, or not a part
            }
            const auto n = static_cast<std::size_t>(std::stoull(digits));
            best = best.has_value() ? std::max(*best, n) : n;
        }
        if (!out.GetResult().GetIsTruncated()) {
            return best;
        }
        req.SetContinuationToken(out.GetResult().GetNextContinuationToken());
    }
}

namespace {

void put_object(Aws::S3::S3Client& client,
                const std::string& bucket,
                const std::string& key,
                const std::string& body) {
    auto stream = Aws::MakeShared<Aws::StringStream>("S3Sink", std::ios::in | std::ios::out);
    *stream << body;
    Aws::S3::Model::PutObjectRequest req;
    req.SetBucket(bucket);
    req.SetKey(key);
    req.SetBody(stream);
    auto rsp = client.PutObject(req);
    if (!rsp.IsSuccess()) {
        throw std::runtime_error("S3Sink: PutObject failed for " + key + ": " +
                                 rsp.GetError().GetMessage());
    }
}

}  // namespace

void S3Sink::on_data(const Batch<std::string>& batch) {
    std::uint64_t bytes_added = 0;
    for (const auto& r : batch) {
        bytes_added += r.value().size() + 1;  // +1 for the newline
        impl_->buffer.append(r.value());
        impl_->buffer.push_back('\n');
        if (impl_->buffer.size() >= impl_->opts.rollover_bytes) {
            const std::string key = next_part_key_();
            const auto t0 = std::chrono::steady_clock::now();
            try {
                put_object(*impl_->client, impl_->opts.bucket, key, impl_->buffer);
            } catch (...) {
                clink::metrics::connector::error_inc("s3");
                throw;
            }
            const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                std::chrono::steady_clock::now() - t0)
                                .count();
            clink::metrics::connector::commit_latency_observe("s3", static_cast<std::uint64_t>(dt));
            impl_->buffer.clear();
        }
    }
    clink::metrics::connector::records_out_inc("s3", batch.size());
    clink::metrics::connector::bytes_out_inc("s3", bytes_added);
}

void S3Sink::flush() {
    if (impl_ && impl_->client && !impl_->buffer.empty()) {
        const std::string key = next_part_key_();
        const auto t0 = std::chrono::steady_clock::now();
        try {
            put_object(*impl_->client, impl_->opts.bucket, key, impl_->buffer);
        } catch (...) {
            clink::metrics::connector::error_inc("s3");
            throw;
        }
        const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
        clink::metrics::connector::commit_latency_observe("s3", static_cast<std::uint64_t>(dt));
        impl_->buffer.clear();
    }
}

void S3Sink::close() {
    flush();
    if (impl_) {
        impl_->client.reset();
    }
}

#else

struct S3Sink::Impl {};

bool S3Sink::is_real_implementation() {
    return false;
}

S3Sink::S3Sink(Options /*opts*/) {
    throw std::runtime_error(
        "S3Sink: built without AWS SDK. Install aws-sdk-cpp and reconfigure cmake.");
}

S3Sink::~S3Sink() = default;
void S3Sink::open() {}
void S3Sink::on_data(const Batch<std::string>& /*batch*/) {}
void S3Sink::flush() {}
void S3Sink::close() {}

#endif

}  // namespace clink
