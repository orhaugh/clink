// Live instantiation of the public sink contract suite against a real S3
// endpoint (localstack/MinIO in CI, anything S3-shaped locally): the
// multipart 2PC sink's crash windows land on genuine multipart-upload
// state - prepared means parts uploaded but CompleteMultipartUpload not
// yet sent. Self-skips without CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET,
// same convention as S3Sink2PCLive in this directory.

#include <cstdlib>
#include <filesystem>
#include <iterator>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3ClientConfiguration.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <gtest/gtest.h>

#include "clink/connectors/aws_sdk_init.hpp"
#include "clink/connectors/s3_sink_2pc.hpp"
#include "clink/test/sink_contract.hpp"

namespace {

using clink::test::SinkContractFixture;

bool s3_configured() {
    return std::getenv("CLINK_S3_TEST_ENDPOINT") != nullptr &&
           std::getenv("CLINK_S3_TEST_BUCKET") != nullptr;
}
std::string s3_endpoint() {
    const char* v = std::getenv("CLINK_S3_TEST_ENDPOINT");
    return v == nullptr ? std::string{} : std::string{v};
}
std::string s3_bucket() {
    const char* v = std::getenv("CLINK_S3_TEST_BUCKET");
    return v == nullptr ? std::string{} : std::string{v};
}

Aws::S3::S3Client contract_client() {
    Aws::S3::S3ClientConfiguration cfg;
    cfg.region = "us-east-1";
    cfg.endpointOverride = s3_endpoint();
    cfg.useVirtualAddressing = false;  // path-style for MinIO/localstack
    return Aws::S3::S3Client(cfg);
}

void contract_ensure_bucket() {
    Aws::S3::Model::CreateBucketRequest req;
    req.SetBucket(s3_bucket());
    (void)contract_client().CreateBucket(req);  // "already owned by you" is fine
}

// Every line of every COMPLETED object under `prefix`. Multipart uploads in
// flight (started or even fully prepared) have no object here, which is
// exactly the visibility boundary under test.
std::vector<std::string> committed_lines(const std::string& prefix) {
    std::vector<std::string> out;
    auto client = contract_client();
    Aws::S3::Model::ListObjectsV2Request list;
    list.SetBucket(s3_bucket());
    list.SetPrefix(prefix);
    auto listed = client.ListObjectsV2(list);
    if (!listed.IsSuccess()) {
        return out;
    }
    for (const auto& obj : listed.GetResult().GetContents()) {
        Aws::S3::Model::GetObjectRequest get;
        get.SetBucket(s3_bucket());
        get.SetKey(obj.GetKey());
        auto got = client.GetObject(get);
        if (!got.IsSuccess()) {
            continue;
        }
        auto& body = got.GetResultWithOwnership().GetBody();
        std::string line;
        while (std::getline(body, line)) {
            if (!line.empty()) {
                out.push_back(line);
            }
        }
    }
    return out;
}

struct S3Sink2PCContract {
    using Value = std::string;
    static constexpr std::string_view kCapabilityName = "s3_2pc";

    static bool available() { return s3_configured(); }

    static SinkContractFixture<std::string> make(const std::filesystem::path& dir) {
        // Before ANY SDK object exists - a client constructed pre-init
        // segfaults, without a gtest failure line to say so.
        clink::aws_sdk::ensure_initialized();
        contract_ensure_bucket();
        // Per-test object prefix from the suite's scratch-dir name: each
        // test owns its keyspace and a rerun overwrites it.
        const std::string prefix = "contract/" + dir.filename().string() + "/";

        SinkContractFixture<std::string> fx;
        fx.records = {"r1", "r2", "r3", "r4"};
        fx.fresh = [prefix] {
            clink::S3Sink2PC::Options o;
            o.bucket = s3_bucket();
            o.key_prefix = prefix;
            o.region = "us-east-1";
            o.endpoint_override = s3_endpoint();
            return std::make_shared<clink::S3Sink2PC>(o);
        };
        fx.committed = [prefix] { return committed_lines(prefix); };
        return fx;
    }
};

}  // namespace

namespace clink::test {

INSTANTIATE_TYPED_TEST_SUITE_P(S3Sink2PC, SinkContractSuite, S3Sink2PCContract);

}  // namespace clink::test
