// The Firehose sink against an emulator. Every case is skipped unless
// CLINK_FIREHOSE_TEST_ENDPOINT is set (scripts/firehose-live.sh starts LocalStack
// with Firehose, S3 and STS, and sets it with the AWS variables the SDK signs
// with). Each case creates its own bucket and a DirectPut delivery stream into
// it, writes through the production sink and its SDK client, and reads the
// bucket back with the SDK's S3 client: every object split by the delimiter,
// every record compared byte for byte with what was written. Duplicates are
// allowed, as the sink's contract allows them; a missing, extra or corrupted
// record fails the case.
//
// This is emulator evidence, not a qualification against AWS: LocalStack writes
// each batch to S3 at once, without Firehose's buffering, retries or retention,
// and does not enforce the per-record limit (the sink does).

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <future>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <arpa/inet.h>
#include <aws/core/auth/AWSAuthSigner.h>
#include <aws/firehose/FirehoseClient.h>
#include <aws/firehose/model/CreateDeliveryStreamRequest.h>
#include <aws/firehose/model/DeleteDeliveryStreamRequest.h>
#include <aws/firehose/model/DescribeDeliveryStreamRequest.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "clink/aws/aws_client.hpp"
#include "clink/aws/firehose_sink.hpp"
#include "clink/cluster/operator_registry.hpp"

using namespace std::chrono_literals;
using clink::Batch;
using clink::CheckpointBarrier;
using clink::aws::FirehoseSink;
using clink::aws::FirehoseSinkOptions;
using clink::aws::kFirehoseMaxRecordBytes;

namespace {

const char* fh_endpoint() {
    return std::getenv("CLINK_FIREHOSE_TEST_ENDPOINT");
}

clink::aws::AwsClientOptions fh_client_options() {
    clink::aws::AwsClientOptions o;
    o.region = "us-east-1";
    o.endpoint_override = fh_endpoint();
    o.request_timeout_ms = 60000;
    return o;
}

Aws::Firehose::FirehoseClient& fh_admin() {
    static auto client = [] {
        clink::aws::ensure_aws_initialized();
        return std::make_unique<Aws::Firehose::FirehoseClient>(
            clink::aws::make_client_config(fh_client_options()));
    }();
    return *client;
}

// Path-style addressing: the emulator serves buckets under its own host.
Aws::S3::S3Client& fh_s3() {
    static auto client = [] {
        clink::aws::ensure_aws_initialized();
        return std::make_unique<Aws::S3::S3Client>(
            clink::aws::make_client_config(fh_client_options()),
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            false);
    }();
    return *client;
}

std::string fh_unique(const std::string& stem) {
    static int n = 0;
    return "clink-" + stem + "-" + std::to_string(static_cast<long>(::getpid())) + "-" +
           std::to_string(n++);
}

// A bucket, and a DirectPut delivery stream writing into it under `out/`.
void make_destination(const std::string& stream, const std::string& bucket) {
    Aws::S3::Model::CreateBucketRequest b;
    b.SetBucket(bucket);
    const auto made = fh_s3().CreateBucket(b);
    ASSERT_TRUE(made.IsSuccess()) << made.GetError().GetMessage();
    using namespace Aws::Firehose::Model;
    CreateDeliveryStreamRequest req;
    req.SetDeliveryStreamName(stream);
    req.SetDeliveryStreamType(DeliveryStreamType::DirectPut);
    ExtendedS3DestinationConfiguration s3;
    s3.SetRoleARN("arn:aws:iam::000000000000:role/firehose");
    s3.SetBucketARN("arn:aws:s3:::" + bucket);
    s3.SetPrefix("out/");
    s3.SetBufferingHints(BufferingHints().WithSizeInMBs(1).WithIntervalInSeconds(60));
    req.SetExtendedS3DestinationConfiguration(s3);
    const auto created = fh_admin().CreateDeliveryStream(req);
    ASSERT_TRUE(created.IsSuccess()) << created.GetError().GetMessage();
    for (int i = 0; i < 100; ++i) {
        DescribeDeliveryStreamRequest d;
        d.SetDeliveryStreamName(stream);
        const auto r = fh_admin().DescribeDeliveryStream(d);
        if (r.IsSuccess() &&
            r.GetResult().GetDeliveryStreamDescription().GetDeliveryStreamStatus() ==
                DeliveryStreamStatus::ACTIVE) {
            return;
        }
        std::this_thread::sleep_for(100ms);
    }
    FAIL() << "delivery stream " << stream << " never became ACTIVE";
}

// Every object the stream delivered, each split into records at `delimiter`.
// An object that does not end with the delimiter is reported: its last record
// would have been cut.
std::vector<std::string> delivered(const std::string& bucket, const std::string& delimiter) {
    std::vector<std::string> keys;
    Aws::String token;
    do {
        Aws::S3::Model::ListObjectsV2Request req;
        req.SetBucket(bucket);
        req.SetPrefix("out/");
        if (!token.empty()) {
            req.SetContinuationToken(token);
        }
        const auto r = fh_s3().ListObjectsV2(req);
        EXPECT_TRUE(r.IsSuccess()) << r.GetError().GetMessage();
        if (!r.IsSuccess()) {
            break;
        }
        for (const auto& o : r.GetResult().GetContents()) {
            keys.push_back(o.GetKey());
        }
        token = r.GetResult().GetIsTruncated() ? r.GetResult().GetNextContinuationToken() : "";
    } while (!token.empty());
    std::vector<std::string> records;
    for (const auto& key : keys) {
        Aws::S3::Model::GetObjectRequest get;
        get.SetBucket(bucket);
        get.SetKey(key);
        auto r = fh_s3().GetObject(get);
        EXPECT_TRUE(r.IsSuccess()) << key << ": " << r.GetError().GetMessage();
        if (!r.IsSuccess()) {
            continue;
        }
        std::stringstream body;
        body << r.GetResult().GetBody().rdbuf();
        const std::string content = body.str();
        std::size_t start = 0;
        while (start < content.size()) {
            const auto end = content.find(delimiter, start);
            if (end == std::string::npos) {
                ADD_FAILURE() << "object " << key << " ends inside a record";
                break;
            }
            records.push_back(content.substr(start, end - start));
            start = end + delimiter.size();
        }
    }
    return records;
}

struct Comparison {
    std::size_t missing{0};
    std::size_t unexpected{0};
    std::size_t duplicates{0};
};

// What arrived against what was written: every record at least once, nothing
// else. Records here are unique, so a second copy is a duplicate.
Comparison compare(const std::vector<std::string>& written,
                   const std::vector<std::string>& arrived) {
    std::map<std::string, int> want;
    for (const auto& r : written) {
        ++want[r];
    }
    std::map<std::string, int> got;
    for (const auto& r : arrived) {
        ++got[r];
    }
    Comparison c;
    for (const auto& [r, n] : want) {
        const auto it = got.find(r);
        if (it == got.end()) {
            if (++c.missing <= 3) {
                ADD_FAILURE() << "record of " << r.size() << " bytes starting " << r.substr(0, 24)
                              << " never arrived";
            }
        } else if (it->second > n) {
            c.duplicates += static_cast<std::size_t>(it->second - n);
        }
    }
    for (const auto& [r, n] : got) {
        if (want.count(r) == 0) {
            c.unexpected += static_cast<std::size_t>(n);
            if (c.unexpected <= 3) {
                ADD_FAILURE() << "a record of " << r.size() << " bytes starting " << r.substr(0, 24)
                              << " arrived that was never written (corrupted?)";
            }
        }
    }
    return c;
}

// Polls until `want` records have arrived (the emulator writes at once; this
// only absorbs its latency).
std::vector<std::string> await_delivered(const std::string& bucket,
                                         const std::string& delimiter,
                                         std::size_t want) {
    std::vector<std::string> got;
    for (int i = 0; i < 300; ++i) {
        got = delivered(bucket, delimiter);
        if (got.size() >= want) {
            return got;
        }
        std::this_thread::sleep_for(100ms);
    }
    return got;
}

Batch<std::string> fh_live_batch(const std::vector<std::string>& records) {
    Batch<std::string> b;
    for (const auto& r : records) {
        b.emplace(r);
    }
    return b;
}

CheckpointBarrier fh_live_barrier(std::uint64_t id) {
    return CheckpointBarrier{clink::CheckpointId{id}};
}

template <typename F>
std::string fh_live_thrown(F&& f) {
    try {
        f();
    } catch (const std::exception& e) {
        return e.what();
    }
    return "";
}

FirehoseSinkOptions fh_live_options(const std::string& stream) {
    FirehoseSinkOptions o;
    o.delivery_stream = stream;
    o.client = fh_client_options();
    o.client.request_timeout_ms = 30000;
    return o;
}

// A loopback listener that accepts connections and never answers.
class FhSilentServer {
public:
    FhSilentServer() {
        fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        ::bind(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        ::listen(fd_, 8);
        socklen_t len = sizeof(addr);
        ::getsockname(fd_, reinterpret_cast<sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
        thread_ = std::thread([this] {
            for (;;) {
                const int c = ::accept(fd_, nullptr, nullptr);
                if (c < 0) {
                    return;
                }
                accepted_.fetch_add(1);
                held_.push_back(c);
            }
        });
    }
    ~FhSilentServer() {
        ::shutdown(fd_, SHUT_RDWR);
        ::close(fd_);
        thread_.join();
        for (const int c : held_) {
            ::close(c);
        }
    }
    FhSilentServer(const FhSilentServer&) = delete;
    FhSilentServer& operator=(const FhSilentServer&) = delete;
    [[nodiscard]] int port() const { return port_; }
    [[nodiscard]] int accepted() const { return accepted_.load(); }

private:
    int fd_{-1};
    int port_{0};
    std::atomic<int> accepted_{0};
    std::vector<int> held_;
    std::thread thread_;
};

class FirehoseLive : public ::testing::Test {
protected:
    void SetUp() override {
        if (fh_endpoint() == nullptr) {
            GTEST_SKIP() << "set CLINK_FIREHOSE_TEST_ENDPOINT (scripts/firehose-live.sh does)";
        }
    }
    void TearDown() override {
        for (const auto& s : streams_) {
            Aws::Firehose::Model::DeleteDeliveryStreamRequest req;
            req.SetDeliveryStreamName(s);
            (void)fh_admin().DeleteDeliveryStream(req);
        }
    }
    // A fresh stream into a fresh bucket; returns {stream, bucket}.
    std::pair<std::string, std::string> destination(const std::string& stem) {
        const auto stream = fh_unique(stem);
        const auto bucket = fh_unique(stem + "-b");
        make_destination(stream, bucket);
        streams_.push_back(stream);
        return {stream, bucket};
    }
    std::vector<std::string> streams_;
};

}  // namespace

// Through the registered factory, as a job builds it: text, binary bytes, an
// empty record, records at the 1,000 KiB limit and enough small ones for several
// requests reach S3 as written, each once or more and nothing else.
TEST_F(FirehoseLive, EveryRecordReachesTheDestinationAsWritten) {
    const auto [stream, bucket] = destination("records");
    const auto* f = clink::cluster::OperatorRegistry::default_instance().find_sink(
        "firehose_sink", std::string{clink::cluster::kChannelString});
    ASSERT_NE(f, nullptr);
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = {{"delivery_stream", stream},
                  {"region", "us-east-1"},
                  {"endpoint_override", fh_endpoint()},
                  {"connect_timeout_ms", "2000"},
                  {"request_timeout_ms", "30000"}};
    auto sink = std::static_pointer_cast<clink::Sink<std::string>>(f->build(ctx));
    sink->open();
    std::string binary;  // every byte value but the newline delimiter
    for (int i = 0; i < 256; ++i) {
        if (i != '\n') {
            binary.push_back(static_cast<char>(i));
        }
    }
    std::vector<std::string> written = {
        "",
        binary,
        "caf\xC3\xA9 \xF0\x9F\x98\x80 {\"not\":\"parsed\"",
        std::string(kFirehoseMaxRecordBytes - 1, 'A'),  // 1,024,000 with the newline
        std::string(kFirehoseMaxRecordBytes - 1, 'B'),
    };
    for (int i = 0; i < 1200; ++i) {
        written.push_back("rec-" + std::to_string(i) + "-" +
                          std::string(static_cast<std::size_t>(i % 97), 'p'));
    }
    sink->on_data(fh_live_batch(written));
    sink->on_barrier(fh_live_barrier(1));
    sink->close();
    const auto arrived = await_delivered(bucket, "\n", written.size());
    const auto c = compare(written, arrived);
    EXPECT_EQ(c.missing, 0u);
    EXPECT_EQ(c.unexpected, 0u);
    std::printf("[firehose-live] records written=%zu arrived=%zu duplicates=%zu\n",
                written.size(),
                arrived.size(),
                c.duplicates);
}

// A two-byte delimiter splits binary records that hold every other byte value.
TEST_F(FirehoseLive, ABinaryDelimiterSplitsBinaryRecords) {
    const auto [stream, bucket] = destination("binary");
    auto o = fh_live_options(stream);
    o.delimiter = std::string("\x1e\x1f", 2);
    FirehoseSink sink(std::move(o));
    sink.open();
    std::vector<std::string> written;
    for (int r = 0; r < 64; ++r) {
        std::string rec = "b" + std::to_string(r) + ":";
        for (int i = 0; i < 256; ++i) {
            rec.push_back(static_cast<char>((i + r) % 256));
        }
        // No 0x1E 0x1F pair inside a record.
        for (std::size_t i = 0; i + 1 < rec.size(); ++i) {
            if (rec[i] == '\x1e' && rec[i + 1] == '\x1f') {
                rec[i + 1] = '\x20';
            }
        }
        written.push_back(rec);
    }
    written.push_back(std::string("\0\0\0", 3));
    sink.on_data(fh_live_batch(written));
    sink.on_barrier(fh_live_barrier(1));
    const auto arrived = await_delivered(bucket, std::string("\x1e\x1f", 2), written.size());
    const auto c = compare(written, arrived);
    EXPECT_EQ(c.missing, 0u);
    EXPECT_EQ(c.unexpected, 0u);
}

// The emulator takes a record of 1,024,001 bytes; the sink refuses it before
// sending, by the documented limit, and nothing of its batch arrives.
TEST_F(FirehoseLive, ARecordOverTheLimitIsRefusedBeforeItIsSent) {
    const auto [stream, bucket] = destination("limit");
    FirehoseSink sink(fh_live_options(stream));
    sink.open();
    const auto why = fh_live_thrown(
        [&] { sink.on_data(fh_live_batch({"fine", std::string(kFirehoseMaxRecordBytes, 'x')})); });
    EXPECT_NE(why.find("1,024,001 bytes with its 1-byte delimiter"), std::string::npos) << why;
    EXPECT_NE(fh_live_thrown([&] { sink.on_barrier(fh_live_barrier(1)); }), "");
    std::this_thread::sleep_for(500ms);
    EXPECT_TRUE(delivered(bucket, "\n").empty());
}

TEST_F(FirehoseLive, AMissingStreamFailsAndNoBarrierPasses) {
    FirehoseSink sink(fh_live_options(fh_unique("missing")));
    sink.open();
    sink.on_data(fh_live_batch({"a"}));
    const auto why = fh_live_thrown([&] { sink.on_barrier(fh_live_barrier(1)); });
    EXPECT_NE(why.find("PutRecordBatch was refused: ResourceNotFoundException"), std::string::npos)
        << why;
    EXPECT_NE(why.find("Check that the delivery stream exists"), std::string::npos) << why;
    EXPECT_TRUE(sink.failed());
    EXPECT_NE(fh_live_thrown([&] { sink.on_barrier(fh_live_barrier(2)); }).find("failed earlier"),
              std::string::npos);
}

// The production SDK service, with a request in flight to a server that never
// answers: abort() ends it at once and the call reports a cancellation.
TEST_F(FirehoseLive, AbortEndsAnSdkCallInFlight) {
    FhSilentServer server;
    clink::aws::AwsClientOptions o;
    o.region = "us-east-1";
    o.endpoint_override = "http://127.0.0.1:" + std::to_string(server.port());
    o.connect_timeout_ms = 2000;
    o.request_timeout_ms = 30000;
    auto service = clink::aws::make_firehose_sdk_service(o);
    const std::string record = "a\n";
    auto call =
        std::async(std::launch::async, [&] { return service->put_record_batch("s", {&record}); });
    for (int i = 0; i < 200 && server.accepted() == 0; ++i) {
        std::this_thread::sleep_for(10ms);
    }
    ASSERT_GT(server.accepted(), 0) << "the request never reached the server";
    EXPECT_EQ(call.wait_for(300ms), std::future_status::timeout) << "the call ended by itself";
    const auto t0 = std::chrono::steady_clock::now();
    service->abort();
    ASSERT_EQ(call.wait_for(5s), std::future_status::ready) << "abort() did not end the call";
    EXPECT_LT(std::chrono::steady_clock::now() - t0, 3s);
    const auto r = call.get();
    EXPECT_EQ(r.error.fault, clink::aws::FirehoseFault::Cancelled)
        << r.error.code << ": " << r.error.message;
}
