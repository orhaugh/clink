// S3Sink2PC tests - the exactly-once raw-object S3 sink (multipart-upload-
// complete-on-commit) riding CommittingSink.
//
// The codec + construction tests run anywhere the AWS SDK is linked (no server).
// The LIVE integration tests are SKIPPED unless CLINK_S3_TEST_ENDPOINT +
// CLINK_S3_TEST_BUCKET are set (MinIO / LocalStack; credentials via the AWS env
// chain). They prove against a real object store:
//   * commit round-trip: the object is invisible after the barrier and appears
//     atomically on commit with the interval's data;
//   * crash recovery: a multipart upload survives the session, and a fresh sink
//     sharing the checkpoint state CompleteMultipartUploads it at open;
//   * abort discards the staged parts;
//   * idempotent commit; empty interval uploads nothing.

#include <cstdlib>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/connectors/s3_sink_2pc.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"

#ifdef CLINK_HAS_AWS_S3
#include <aws/core/Aws.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3ClientConfiguration.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>

#include "clink/connectors/aws_sdk_init.hpp"
#endif

using clink::Batch;
using clink::CheckpointBarrier;
using clink::CheckpointId;
using clink::InMemoryStateBackend;
using clink::OperatorId;
using clink::RuntimeContext;
using clink::S3MultipartHandle;
using clink::S3Sink2PC;

namespace {

// --- codec + construction (no server) -----------------------------------

TEST(S3Sink2PC, RejectsEmptyBucket) {
    S3Sink2PC::Options o;
    o.key_prefix = "p";
    EXPECT_THROW(S3Sink2PC{std::move(o)}, std::runtime_error);
}

TEST(S3Sink2PC, CommittableCodecRoundTrips) {
    S3Sink2PC::Options o;
    o.bucket = "b";
    S3Sink2PC sink(std::move(o));

    S3MultipartHandle h;
    h.key = "prefix/sub0-7.ndjson";
    h.upload_id = "abc.DEF-123_xyz";
    h.parts = {{1, "\"etag-one\""}, {2, "\"etag-two-2\""}};

    const S3MultipartHandle back = sink.deserialize(sink.serialize(h));
    EXPECT_EQ(back.key, h.key);
    EXPECT_EQ(back.upload_id, h.upload_id);
    ASSERT_EQ(back.parts.size(), 2u);
    EXPECT_EQ(back.parts[0].first, 1);
    EXPECT_EQ(back.parts[0].second, "\"etag-one\"");
    EXPECT_EQ(back.parts[1].first, 2);
    EXPECT_EQ(back.parts[1].second, "\"etag-two-2\"");
}

#ifdef CLINK_HAS_AWS_S3

// --- live integration ---------------------------------------------------

bool s3_configured() {
    return std::getenv("CLINK_S3_TEST_ENDPOINT") != nullptr &&
           std::getenv("CLINK_S3_TEST_BUCKET") != nullptr;
}
std::string s3_endpoint() {
    return std::getenv("CLINK_S3_TEST_ENDPOINT");
}
std::string s3_bucket() {
    return std::getenv("CLINK_S3_TEST_BUCKET");
}
std::string uniq() {
    return std::to_string(static_cast<long>(::getpid()));
}

Aws::S3::S3Client make_client() {
    Aws::S3::S3ClientConfiguration cfg;
    cfg.region = "us-east-1";
    cfg.endpointOverride = s3_endpoint();
    cfg.useVirtualAddressing = false;  // path-style for MinIO
    return Aws::S3::S3Client(cfg);
}

void ensure_bucket() {
    Aws::S3::Model::CreateBucketRequest req;
    req.SetBucket(s3_bucket());
    (void)make_client().CreateBucket(req);  // ignore "already owned by you"
}

bool object_exists(const std::string& key) {
    Aws::S3::Model::HeadObjectRequest req;
    req.SetBucket(s3_bucket());
    req.SetKey(key);
    return make_client().HeadObject(req).IsSuccess();
}

std::string get_object(const std::string& key) {
    Aws::S3::Model::GetObjectRequest req;
    req.SetBucket(s3_bucket());
    req.SetKey(key);
    auto out = make_client().GetObject(req);
    if (!out.IsSuccess()) {
        return {};
    }
    auto& body = out.GetResultWithOwnership().GetBody();
    std::string s((std::istreambuf_iterator<char>(body)), std::istreambuf_iterator<char>());
    return s;
}

S3Sink2PC::Options opts_for(const std::string& prefix) {
    S3Sink2PC::Options o;
    o.bucket = s3_bucket();
    o.key_prefix = prefix;
    o.region = "us-east-1";
    o.endpoint_override = s3_endpoint();
    return o;
}

std::shared_ptr<S3Sink2PC> make_sink(const std::string& prefix, RuntimeContext& rctx) {
    auto sink = std::make_shared<S3Sink2PC>(opts_for(prefix));
    sink->set_id(OperatorId{88});
    sink->set_uid("s3xo");
    sink->attach_runtime(&rctx);
    return sink;
}

Batch<std::string> lines(const std::vector<std::string>& xs) {
    Batch<std::string> b;
    for (const auto& s : xs)
        b.emplace(s);
    return b;
}

#define REQUIRE_LIVE_S3()                                                        \
    do {                                                                         \
        if (!s3_configured())                                                    \
            GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET"; \
        clink::aws_sdk::ensure_initialized();                                    \
        ensure_bucket();                                                         \
    } while (0)

TEST(S3Sink2PCLive, CommitRoundTrips) {
    REQUIRE_LIVE_S3();
    const std::string prefix = "s3xo_commit_" + uniq();
    const std::string key = prefix + "/sub0-1.ndjson";
    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{88}, "s3xo", &state, nullptr);
    auto sink = make_sink(prefix, rctx);

    sink->open();
    sink->on_data(lines({"a", "b", "c"}));
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    // Parts uploaded but the object is not visible until CompleteMultipartUpload.
    EXPECT_FALSE(object_exists(key));

    sink->on_commit(1);
    EXPECT_TRUE(object_exists(key));
    EXPECT_EQ(get_object(key), "a\nb\nc\n");
    sink->close();
}

TEST(S3Sink2PCLive, MultipartUploadSurvivesCrashAndRecovers) {
    REQUIRE_LIVE_S3();
    const std::string prefix = "s3xo_recover_" + uniq();
    const std::string key = prefix + "/sub0-1.ndjson";
    InMemoryStateBackend state;  // survives the "crash"
    RuntimeContext rctx(OperatorId{88}, "s3xo", &state, nullptr);

    {
        auto crashed = make_sink(prefix, rctx);
        crashed->open();
        crashed->on_data(lines({"x", "y"}));
        crashed->on_barrier(CheckpointBarrier{CheckpointId{1}});
        // No on_commit - process "crashes". The multipart upload's parts persist
        // on the server; the handle is durable in state.
    }
    EXPECT_FALSE(object_exists(key));

    auto restarted = make_sink(prefix, rctx);
    restarted->open();  // recover_all_ CompleteMultipartUploads the pending handle
    EXPECT_TRUE(object_exists(key));
    EXPECT_EQ(get_object(key), "x\ny\n");
    restarted->close();
}

TEST(S3Sink2PCLive, AbortDiscardsStagedParts) {
    REQUIRE_LIVE_S3();
    const std::string prefix = "s3xo_abort_" + uniq();
    const std::string key = prefix + "/sub0-1.ndjson";
    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{88}, "s3xo", &state, nullptr);
    auto sink = make_sink(prefix, rctx);

    sink->open();
    sink->on_data(lines({"z"}));
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    sink->on_abort(1);
    EXPECT_FALSE(object_exists(key));
    EXPECT_NO_THROW(sink->on_commit(1));  // key already gone -> no-op
    EXPECT_FALSE(object_exists(key));
    sink->close();
}

TEST(S3Sink2PCLive, CommitIsIdempotent) {
    REQUIRE_LIVE_S3();
    const std::string prefix = "s3xo_idem_" + uniq();
    const std::string key = prefix + "/sub0-1.ndjson";
    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{88}, "s3xo", &state, nullptr);
    auto sink = make_sink(prefix, rctx);

    sink->open();
    sink->on_data(lines({"only"}));
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    sink->on_commit(1);
    EXPECT_NO_THROW(sink->on_commit(1));  // second (recovery-time) commit is a no-op
    EXPECT_TRUE(object_exists(key));
    EXPECT_EQ(get_object(key), "only\n");
    sink->close();
}

TEST(S3Sink2PCLive, EmptyIntervalUploadsNothing) {
    REQUIRE_LIVE_S3();
    const std::string prefix = "s3xo_empty_" + uniq();
    const std::string key = prefix + "/sub0-1.ndjson";
    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{88}, "s3xo", &state, nullptr);
    auto sink = make_sink(prefix, rctx);

    sink->open();
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});  // no data
    EXPECT_NO_THROW(sink->on_commit(1));
    EXPECT_FALSE(object_exists(key));
    sink->close();
}

#endif  // CLINK_HAS_AWS_S3

}  // namespace
