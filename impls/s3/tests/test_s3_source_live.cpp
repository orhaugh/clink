// The raw S3 source against a live store. SKIPPED unless CLINK_S3_TEST_ENDPOINT
// and CLINK_S3_TEST_BUCKET name one (credentials through the AWS environment
// chain); scripts/s3-live.sh runs it against SILO and fails when any case is
// skipped.
//
// Each case puts objects under a prefix of its own and drives the source as a
// job does at the operator level. A restore case reads part of the objects with
// one source and checkpoints it into a state backend, changes the objects or
// not, then restores a second source from the checkpoint and reads to the end.
// An object read twice is allowed (at least once); an object skipped is not.

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/connectors/s3_source.hpp"
#include "clink/core/record.hpp"
#include "clink/state/in_memory_state_backend.hpp"

#ifdef CLINK_HAS_AWS_S3
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3ClientConfiguration.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>

#include "clink/connectors/aws_sdk_init.hpp"
#endif

#ifdef CLINK_HAS_AWS_S3

using clink::CheckpointId;
using clink::Emitter;
using clink::InMemoryStateBackend;
using clink::OperatorId;
using clink::S3Source;
using clink::StreamElement;

namespace {

bool configured() {
    return std::getenv("CLINK_S3_TEST_ENDPOINT") != nullptr &&
           std::getenv("CLINK_S3_TEST_BUCKET") != nullptr;
}

std::string endpoint() {
    return std::getenv("CLINK_S3_TEST_ENDPOINT");
}

std::string bucket() {
    return std::getenv("CLINK_S3_TEST_BUCKET");
}

// The test's own client, for the objects the source reads.
Aws::S3::S3Client client() {
    Aws::S3::S3ClientConfiguration cfg;
    cfg.region = "us-east-1";
    cfg.endpointOverride = endpoint();
    cfg.useVirtualAddressing = false;
    return Aws::S3::S3Client(cfg);
}

#define REQUIRE_STORE()                                                            \
    do {                                                                           \
        if (!configured()) {                                                       \
            GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT and CLINK_S3_TEST_BUCKET " \
                            "(scripts/s3-live.sh does)";                           \
        }                                                                          \
        clink::aws_sdk::ensure_initialized();                                      \
        Aws::S3::Model::CreateBucketRequest create;                                \
        create.SetBucket(bucket());                                                \
        (void)client().CreateBucket(create); /* ignore "already owned by you" */   \
    } while (0)

std::string prefix() {
    static int n = 0;
    return "s3-source-live-" + std::to_string(static_cast<long>(::getpid())) + "-" +
           std::to_string(++n) + "/";
}

void put(Aws::S3::S3Client& c, const std::string& key, const std::string& body) {
    auto stream = Aws::MakeShared<Aws::StringStream>("S3SourceLive", std::ios::in | std::ios::out);
    *stream << body;
    Aws::S3::Model::PutObjectRequest req;
    req.SetBucket(bucket());
    req.SetKey(key);
    req.SetBody(stream);
    const auto out = c.PutObject(req);
    ASSERT_TRUE(out.IsSuccess()) << key << ": " << out.GetError().GetMessage();
}

void delete_object(Aws::S3::S3Client& c, const std::string& key) {
    Aws::S3::Model::DeleteObjectRequest req;
    req.SetBucket(bucket());
    req.SetKey(key);
    const auto out = c.DeleteObject(req);
    ASSERT_TRUE(out.IsSuccess()) << key << ": " << out.GetError().GetMessage();
}

// One object per name, under `p`, holding one line: the name.
void put_named(Aws::S3::S3Client& c, const std::string& p, const std::vector<std::string>& names) {
    for (const auto& n : names) {
        put(c, p + n, n + "\n");
    }
}

S3Source::Options options(const std::string& p) {
    S3Source::Options o;
    o.bucket = bucket();
    o.key_prefix = p;
    o.region = "us-east-1";
    o.endpoint_override = endpoint();
    return o;
}

// The lines `calls` produce() calls emit, one object each, or, with calls < 0,
// every line to the end.
std::vector<std::string> read_lines(S3Source& src, int calls) {
    std::vector<std::string> lines;
    Emitter<std::string> em(Emitter<std::string>::Forward([&lines](StreamElement<std::string> e) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                lines.push_back(r.value());
            }
        }
        return true;
    }));
    if (calls < 0) {
        while (src.produce(em)) {
        }
    } else {
        for (int i = 0; i < calls; ++i) {
            (void)src.produce(em);
        }
    }
    return lines;
}

const OperatorId kOp{5151};

}  // namespace

// A listing answers at most 1000 keys a page. The source read the first page
// alone, so the five objects after it were never read.
TEST(S3SourceLive, EveryObjectUnderAPrefixOfMoreThanOneListingPageIsRead) {
    REQUIRE_STORE();
    auto c = client();
    const auto p = prefix();
    std::vector<std::string> names;
    for (int i = 0; i < 1005; ++i) {
        char name[16];
        std::snprintf(name, sizeof name, "k-%05d", i);
        names.emplace_back(name);
    }
    put_named(c, p, names);
    S3Source src(options(p));
    src.open();
    EXPECT_EQ(read_lines(src, -1), names);
    src.close();
}

// Over the same objects the restored source resumes after the last one the
// checkpoint covered.
TEST(S3SourceLive, ARestoreOverTheSameObjectsResumesAfterTheLastOneEmitted) {
    REQUIRE_STORE();
    auto c = client();
    const auto p = prefix();
    put_named(c, p, {"a", "b", "c", "d"});
    InMemoryStateBackend backend;
    S3Source first(options(p));
    first.open();
    EXPECT_EQ(read_lines(first, 2), (std::vector<std::string>{"a", "b"}));
    first.snapshot_offset(backend, kOp, CheckpointId{1});
    first.close();
    S3Source second(options(p));
    ASSERT_TRUE(second.restore_offset(backend, kOp));
    second.open();
    EXPECT_EQ(read_lines(second, -1), (std::vector<std::string>{"c", "d"}));
    second.close();
}

// An object removed before the checkpoint's position moved the rest up one
// place: resumed by index, the source skipped c. Resumed by key it does not.
TEST(S3SourceLive, ARestoreAfterAnEarlierObjectWasRemovedSkipsNothing) {
    REQUIRE_STORE();
    auto c = client();
    const auto p = prefix();
    put_named(c, p, {"a", "b", "c", "d"});
    InMemoryStateBackend backend;
    S3Source first(options(p));
    first.open();
    EXPECT_EQ(read_lines(first, 2), (std::vector<std::string>{"a", "b"}));
    first.snapshot_offset(backend, kOp, CheckpointId{1});
    first.close();
    delete_object(c, p + "a");
    S3Source second(options(p));
    ASSERT_TRUE(second.restore_offset(backend, kOp));
    second.open();
    EXPECT_EQ(read_lines(second, -1), (std::vector<std::string>{"c", "d"}));
    second.close();
}

// An object added before the checkpoint's position moved the rest down one
// place: resumed by index, the source read c again. Resumed by key it reads
// what follows b, and the object added before it is not read.
TEST(S3SourceLive, ARestoreAfterAnEarlierObjectWasAddedRepeatsNothing) {
    REQUIRE_STORE();
    auto c = client();
    const auto p = prefix();
    put_named(c, p, {"b", "c", "d", "e"});
    InMemoryStateBackend backend;
    S3Source first(options(p));
    first.open();
    EXPECT_EQ(read_lines(first, 2), (std::vector<std::string>{"b", "c"}));
    first.snapshot_offset(backend, kOp, CheckpointId{1});
    first.close();
    put_named(c, p, {"a"});
    S3Source second(options(p));
    ASSERT_TRUE(second.restore_offset(backend, kOp));
    second.open();
    EXPECT_EQ(read_lines(second, -1), (std::vector<std::string>{"d", "e"}));
    second.close();
}

// An endpoint named by host is addressed by path. The source addressed it by
// virtual host, putting the bucket in the host name, so against a store named
// by host (http://minio:9000) it could not list the prefix at all.
TEST(S3SourceLive, AnEndpointNamedByHostIsAddressedByPath) {
    REQUIRE_STORE();
    auto c = client();
    const auto p = prefix();
    put_named(c, p, {"a", "b"});
    auto o = options(p);
    if (const auto at = o.endpoint_override->find("127.0.0.1"); at != std::string::npos) {
        o.endpoint_override->replace(at, std::strlen("127.0.0.1"), "localhost");
    }
    S3Source src(std::move(o));
    src.open();
    EXPECT_EQ(read_lines(src, -1), (std::vector<std::string>{"a", "b"}));
    src.close();
}

// A position from an earlier release is an index, which names no object:
// every object is read again rather than resumed on trust.
TEST(S3SourceLive, AnEarlierReleasesPositionReadsEveryObjectAgain) {
    REQUIRE_STORE();
    auto c = client();
    const auto p = prefix();
    put_named(c, p, {"a", "b", "c"});
    InMemoryStateBackend backend;
    const std::string key = "__s3_source_object__";
    std::string index(8, '\0');
    index[0] = 2;
    backend.put_operator_state(kOp,
                               clink::StateBackend::KeyView{key.data(), key.size()},
                               clink::StateBackend::ValueView{index.data(), index.size()});
    S3Source src(options(p));
    ASSERT_TRUE(src.restore_offset(backend, kOp));
    src.open();
    EXPECT_EQ(read_lines(src, -1), (std::vector<std::string>{"a", "b", "c"}));
    src.close();
}

#endif  // CLINK_HAS_AWS_S3
