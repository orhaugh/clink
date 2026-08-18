// S3CoordinationStore - the coordination-record store on a bucket.
//
// Implements clink::cluster::CoordinationStore over the raw AWS SDK
// S3Client (the Arrow S3 filesystem has no conditional-write surface, and
// every guarantee here rides on conditional PUTs):
//
//   put            -> PutObject (S3 PUTs are atomic last-writer-wins)
//   put_if_absent  -> PutObject with If-None-Match: "*"
//   fenced_put     -> GetObject (body + ETag), metadata_write_allowed
//                     against the epoch in the body, then PutObject with
//                     If-Match: <etag> (If-None-Match: "*" when absent);
//                     a 412 means the record moved underneath us, so
//                     re-read and re-check - the same "check and write are
//                     one critical section" guarantee the filesystem store
//                     gets from flock, expressed as compare-and-swap on
//                     the object's ETag
//   list           -> ListObjectsV2 under "<prefix>/", keys relative to
//                     the store root
//   remove         -> DeleteObject (deleting an absent key succeeds)
//
// Keys are the exact relative paths the filesystem layout uses
// (_jobs/<id>/COMPLETED-N, _jobs/<id>/receipts/subK-N, jobs/<id>/...), so
// a deployment migrates between substrates by copying objects.
//
// Registered as the "s3" scheme via install_coordination_store():
//   s3://<bucket>/<prefix>[?endpoint=<url>&region=<r>]

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "clink/cluster/coordination_store.hpp"

namespace Aws::S3 {
class S3Client;
}

namespace clink::s3 {

class S3CoordinationStore final : public clink::cluster::CoordinationStore {
public:
    struct Options {
        std::string bucket;
        std::string prefix;    // no trailing '/'; may be empty (bucket root)
        std::string endpoint;  // empty = real AWS; set = path-style (MinIO/LocalStack)
        std::string region{"us-east-1"};
    };

    explicit S3CoordinationStore(Options opts);
    ~S3CoordinationStore() override;

    void put(std::string_view key, std::string_view body) override;
    bool put_if_absent(std::string_view key, std::string_view body) override;
    bool fenced_put(std::string_view key,
                    std::string_view body,
                    std::uint64_t writer_epoch,
                    const std::function<std::uint64_t(const std::string&)>& epoch_of,
                    const std::string& caller_context) override;
    [[nodiscard]] std::optional<std::string> get(std::string_view key) override;
    [[nodiscard]] bool exists(std::string_view key) override;
    [[nodiscard]] std::vector<std::string> list(std::string_view prefix) override;
    void remove(std::string_view key) override;

    // Test seam: runs between fenced_put's epoch check and its conditional
    // PUT, once, then disarms. The read-then-write interleave lives in that
    // window; the contract test uses this to prove the If-Match CAS refuses
    // a write whose read went stale (drop the If-Match and the test fails
    // with a superseded writer's record on top of the leader's).
    void set_test_hook_between_check_and_put(std::function<void()> hook);

private:
    [[nodiscard]] std::string object_key_(std::string_view key) const;

    Options opts_;
    std::unique_ptr<Aws::S3::S3Client> client_;
    std::function<void()> test_hook_between_check_and_put_;
};

}  // namespace clink::s3
