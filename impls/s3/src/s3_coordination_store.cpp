#include "clink/s3/s3_coordination_store.hpp"

#include <chrono>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <utility>

#include <aws/core/http/HttpResponse.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3ClientConfiguration.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>

#include "clink/cluster/coordinator.hpp"  // metadata_write_allowed
#include "clink/connectors/aws_sdk_init.hpp"

namespace clink::s3 {

namespace {

// One fenced_put re-reads and retries this many times before concluding the
// key is contended beyond reason. Every retry means another writer's PUT
// landed between our read and our conditional write; coordination records
// have a handful of writers, so double digits of consecutive losses is a
// fault, not a workload.
constexpr int kCasAttempts = 8;

// A conditional PUT that lost: 412 is the terminal "your precondition no
// longer holds"; 409 (ConditionalRequestConflict) is "another conditional
// write on this key is in flight, try again".
bool lost_precondition(const Aws::S3::S3Error& err) {
    return err.GetResponseCode() == Aws::Http::HttpResponseCode::PRECONDITION_FAILED;
}

bool conditional_conflict(const Aws::S3::S3Error& err) {
    return err.GetResponseCode() == Aws::Http::HttpResponseCode::CONFLICT;
}

bool not_found(const Aws::S3::S3Error& err) {
    return err.GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY ||
           err.GetErrorType() == Aws::S3::S3Errors::RESOURCE_NOT_FOUND ||
           err.GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND;
}

[[noreturn]] void fail(const std::string& op, const std::string& key, const Aws::S3::S3Error& err) {
    throw std::runtime_error("s3 coordination store: " + op + "(" + key +
                             ") failed: " + std::string{err.GetExceptionName()} + ": " +
                             std::string{err.GetMessage()});
}

std::shared_ptr<Aws::IOStream> body_stream(std::string_view body) {
    auto stream = Aws::MakeShared<Aws::StringStream>("clink-coord-store");
    stream->write(body.data(), static_cast<std::streamsize>(body.size()));
    return stream;
}

}  // namespace

S3CoordinationStore::S3CoordinationStore(Options opts) : opts_(std::move(opts)) {
    if (opts_.bucket.empty()) {
        throw std::runtime_error("s3 coordination store: a bucket is required");
    }
    while (!opts_.prefix.empty() && opts_.prefix.back() == '/') {
        opts_.prefix.pop_back();
    }
    clink::aws_sdk::ensure_initialized();
    Aws::S3::S3ClientConfiguration cfg;
    cfg.region = opts_.region;
    if (!opts_.endpoint.empty()) {
        cfg.endpointOverride = opts_.endpoint;
        cfg.useVirtualAddressing = false;  // path-style for LocalStack / MinIO
    }
    client_ = std::make_unique<Aws::S3::S3Client>(cfg);
}

S3CoordinationStore::~S3CoordinationStore() = default;

std::string S3CoordinationStore::object_key_(std::string_view key) const {
    if (opts_.prefix.empty()) {
        return std::string{key};
    }
    return opts_.prefix + "/" + std::string{key};
}

void S3CoordinationStore::put(std::string_view key, std::string_view body) {
    Aws::S3::Model::PutObjectRequest req;
    req.SetBucket(opts_.bucket);
    req.SetKey(object_key_(key));
    req.SetBody(body_stream(body));
    auto out = client_->PutObject(req);
    if (!out.IsSuccess()) {
        fail("put", std::string{key}, out.GetError());
    }
}

bool S3CoordinationStore::put_if_absent(std::string_view key, std::string_view body) {
    // If-None-Match: "*" is the create-once primitive: exactly one of any
    // number of concurrent writers lands, the rest get 412. A 409 means the
    // race is still being decided (another conditional write in flight), so
    // retry until the key's fate is known.
    for (int attempt = 0;; ++attempt) {
        Aws::S3::Model::PutObjectRequest req;
        req.SetBucket(opts_.bucket);
        req.SetKey(object_key_(key));
        req.SetBody(body_stream(body));
        req.SetIfNoneMatch("*");
        auto out = client_->PutObject(req);
        if (out.IsSuccess()) {
            return true;
        }
        const auto& err = out.GetError();
        if (lost_precondition(err)) {
            return false;
        }
        if (conditional_conflict(err) && attempt < kCasAttempts) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            continue;
        }
        fail("put_if_absent", std::string{key}, err);
    }
}

bool S3CoordinationStore::fenced_put(
    std::string_view key,
    std::string_view body,
    std::uint64_t writer_epoch,
    const std::function<std::uint64_t(const std::string&)>& epoch_of,
    const std::string& caller_context) {
    // The filesystem store makes "check epoch, then write" one critical
    // section with flock; here the conditional PUT is the critical section.
    // We read the record and its ETag, fence against the epoch in the body,
    // and write with If-Match on the ETag we read. If any writer landed in
    // between, the ETag no longer matches, the PUT refuses with 412, and we
    // re-read - so a stale writer can never rename over a fresher record.
    const auto obj_key = object_key_(key);
    for (int attempt = 0; attempt < kCasAttempts; ++attempt) {
        std::optional<std::string> etag;
        std::string existing;
        {
            Aws::S3::Model::GetObjectRequest get;
            get.SetBucket(opts_.bucket);
            get.SetKey(obj_key);
            auto out = client_->GetObject(get);
            if (out.IsSuccess()) {
                std::stringstream buf;
                buf << out.GetResult().GetBody().rdbuf();
                existing = buf.str();
                etag = out.GetResult().GetETag();
            } else if (!not_found(out.GetError())) {
                fail("fenced_put/read", std::string{key}, out.GetError());
            }
        }

        // Absent record = epoch 0; the extractor is only consulted on what
        // was actually read back, matching the filesystem store.
        const std::uint64_t stored = etag.has_value() ? epoch_of(existing) : 0;
        if (!clink::cluster::metadata_write_allowed(writer_epoch, stored)) {
            // A superseded writer must not clobber the leader's record. The
            // caller logs the refusal; caller_context names the write site.
            (void)caller_context;
            return false;
        }

        if (test_hook_between_check_and_put_) {
            auto hook = std::exchange(test_hook_between_check_and_put_, nullptr);
            hook();
        }

        Aws::S3::Model::PutObjectRequest req;
        req.SetBucket(opts_.bucket);
        req.SetKey(obj_key);
        req.SetBody(body_stream(body));
        if (etag.has_value()) {
            req.SetIfMatch(*etag);
        } else {
            req.SetIfNoneMatch("*");
        }
        auto out = client_->PutObject(req);
        if (out.IsSuccess()) {
            return true;
        }
        const auto& err = out.GetError();
        if (lost_precondition(err) || conditional_conflict(err)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            continue;  // the record moved underneath us: re-read, re-fence
        }
        fail("fenced_put/write", std::string{key}, err);
    }
    throw std::runtime_error("s3 coordination store: fenced_put(" + std::string{key} + ") lost " +
                             std::to_string(kCasAttempts) + " consecutive CAS rounds (" +
                             caller_context +
                             "); the key is contended beyond any sane coordination load");
}

std::optional<std::string> S3CoordinationStore::get(std::string_view key) {
    Aws::S3::Model::GetObjectRequest req;
    req.SetBucket(opts_.bucket);
    req.SetKey(object_key_(key));
    auto out = client_->GetObject(req);
    if (!out.IsSuccess()) {
        if (not_found(out.GetError())) {
            return std::nullopt;
        }
        // Absent and unreadable are different answers: a caller who treats
        // a network fault as "record missing" would resolve recovery against
        // records that exist.
        fail("get", std::string{key}, out.GetError());
    }
    std::stringstream buf;
    buf << out.GetResult().GetBody().rdbuf();
    return buf.str();
}

bool S3CoordinationStore::exists(std::string_view key) {
    Aws::S3::Model::HeadObjectRequest req;
    req.SetBucket(opts_.bucket);
    req.SetKey(object_key_(key));
    auto out = client_->HeadObject(req);
    if (out.IsSuccess()) {
        return true;
    }
    if (not_found(out.GetError())) {
        return false;
    }
    fail("exists", std::string{key}, out.GetError());
}

std::vector<std::string> S3CoordinationStore::list(std::string_view prefix) {
    // "<prefix>/" so the boundary is a path segment, matching the directory
    // semantics of the filesystem store: list("_jobs/7") must not surface
    // _jobs/70's records.
    auto full = object_key_(prefix);
    if (!full.empty() && full.back() != '/') {
        full += '/';
    }
    const auto root = opts_.prefix.empty() ? std::string{} : opts_.prefix + "/";

    std::vector<std::string> keys;
    Aws::S3::Model::ListObjectsV2Request req;
    req.SetBucket(opts_.bucket);
    if (!full.empty()) {
        req.SetPrefix(full);
    }
    for (;;) {
        auto out = client_->ListObjectsV2(req);
        if (!out.IsSuccess()) {
            fail("list", std::string{prefix}, out.GetError());
        }
        for (const auto& obj : out.GetResult().GetContents()) {
            const auto& key = obj.GetKey();
            if (std::string_view{key}.starts_with(root)) {
                keys.push_back(key.substr(root.size()));
            }
        }
        if (!out.GetResult().GetIsTruncated()) {
            return keys;
        }
        req.SetContinuationToken(out.GetResult().GetNextContinuationToken());
    }
}

void S3CoordinationStore::remove(std::string_view key) {
    // Best-effort, like the filesystem store: DeleteObject on an absent key
    // already succeeds, and a transient failure is retried by the caller's
    // next retention pass.
    Aws::S3::Model::DeleteObjectRequest req;
    req.SetBucket(opts_.bucket);
    req.SetKey(object_key_(key));
    (void)client_->DeleteObject(req);
}

void S3CoordinationStore::set_test_hook_between_check_and_put(std::function<void()> hook) {
    test_hook_between_check_and_put_ = std::move(hook);
}

}  // namespace clink::s3
