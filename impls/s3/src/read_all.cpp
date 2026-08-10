#include "clink/s3/read_all.hpp"

#include <algorithm>
#include <chrono>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <thread>

#include <aws/s3/S3Client.h>
#include <aws/s3/S3ClientConfiguration.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListMultipartUploadsRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>

#include "clink/connectors/aws_sdk_init.hpp"

namespace clink::s3 {

namespace {

Aws::S3::S3Client make_client(const std::string& endpoint, const std::string& region) {
    clink::aws_sdk::ensure_initialized();
    Aws::S3::S3ClientConfiguration cfg;
    cfg.region = region;
    if (!endpoint.empty()) {
        cfg.endpointOverride = endpoint;
        cfg.useVirtualAddressing = false;  // path-style for LocalStack / MinIO
    }
    return Aws::S3::S3Client{cfg};
}

}  // namespace

void ensure_bucket(const std::string& endpoint,
                   const std::string& region,
                   const std::string& bucket) {
    auto client = make_client(endpoint, region);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    std::string last_err;
    while (std::chrono::steady_clock::now() < deadline) {
        Aws::S3::Model::CreateBucketRequest req;
        req.SetBucket(bucket);
        auto out = client.CreateBucket(req);
        if (out.IsSuccess()) {
            return;
        }
        const auto& err = out.GetError();
        const auto name = err.GetExceptionName();
        if (name == "BucketAlreadyOwnedByYou" || name == "BucketAlreadyExists") {
            return;
        }
        last_err = std::string{name} + ": " + std::string{err.GetMessage()};
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    throw std::runtime_error("clink::s3::ensure_bucket: '" + bucket +
                             "' never became creatable: " + last_err);
}

std::vector<std::string> read_all_lines(const std::string& endpoint,
                                        const std::string& region,
                                        const std::string& bucket,
                                        const std::string& prefix) {
    auto client = make_client(endpoint, region);
    std::vector<std::string> keys;
    Aws::S3::Model::ListObjectsV2Request list;
    list.SetBucket(bucket);
    if (!prefix.empty()) {
        list.SetPrefix(prefix);
    }
    for (;;) {
        auto out = client.ListObjectsV2(list);
        if (!out.IsSuccess()) {
            throw std::runtime_error("clink::s3::read_all_lines: ListObjectsV2 failed: " +
                                     std::string{out.GetError().GetMessage()});
        }
        for (const auto& obj : out.GetResult().GetContents()) {
            keys.emplace_back(obj.GetKey());
        }
        if (!out.GetResult().GetIsTruncated()) {
            break;
        }
        list.SetContinuationToken(out.GetResult().GetNextContinuationToken());
    }
    std::sort(keys.begin(), keys.end());
    std::vector<std::string> lines;
    for (const auto& key : keys) {
        Aws::S3::Model::GetObjectRequest get;
        get.SetBucket(bucket);
        get.SetKey(key);
        auto out = client.GetObject(get);
        if (!out.IsSuccess()) {
            throw std::runtime_error("clink::s3::read_all_lines: GetObject(" + key +
                                     ") failed: " + std::string{out.GetError().GetMessage()});
        }
        std::stringstream body;
        body << out.GetResult().GetBody().rdbuf();
        std::string line;
        while (std::getline(body, line)) {
            if (!line.empty()) {
                lines.push_back(line);
            }
        }
    }
    return lines;
}

std::size_t pending_multipart_count(const std::string& endpoint,
                                    const std::string& region,
                                    const std::string& bucket,
                                    const std::string& prefix) {
    auto client = make_client(endpoint, region);
    Aws::S3::Model::ListMultipartUploadsRequest req;
    req.SetBucket(bucket);
    if (!prefix.empty()) {
        req.SetPrefix(prefix);
    }
    auto out = client.ListMultipartUploads(req);
    if (!out.IsSuccess()) {
        throw std::runtime_error("clink::s3::pending_multipart_count: failed: " +
                                 std::string{out.GetError().GetMessage()});
    }
    return static_cast<std::size_t>(out.GetResult().GetUploads().size());
}

}  // namespace clink::s3
