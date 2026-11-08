// The Kinesis sink's production SDK path, deterministically: the sink with
// make_kinesis_write_sdk_service, the AWS SDK's own client signing, serialising
// and sending real requests to an in-process stand-in for the PutRecords
// endpoint (kinesis_http_stub.hpp), which replies as each case scripts. It shows
// what the SDK sends (each record's partition key and bytes, the stream's name
// or ARN), how a response or an error on the wire reaches the sink, that the
// SDK makes one request per attempt the sink counts, and that a cancel ends a
// call in flight. Nothing here talks to a service.

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "clink/aws/kinesis_sink.hpp"
#include "clink/runtime/runtime_context.hpp"

#include "kinesis_http_stub.hpp"

using namespace std::chrono_literals;
using clink::Batch;
using clink::CheckpointBarrier;
using clink::aws::KinesisPutFault;
using clink::aws::KinesisPutRecord;
using clink::aws::KinesisSink;
using clink::aws::KinesisSinkOptions;
using clink::aws::make_kinesis_write_sdk_service;
using clink::aws::test::KinesisHttpStub;

namespace {

// The SDK signs every request, so it needs credentials; dummy ones from the
// environment keep it from searching for others (and from the instance
// metadata service). A value already set is left alone.
void kss_credentials() {
    ::setenv("AWS_ACCESS_KEY_ID", "test", 0);
    ::setenv("AWS_SECRET_ACCESS_KEY", "test", 0);
    ::setenv("AWS_EC2_METADATA_DISABLED", "true", 0);
}

clink::aws::AwsClientOptions kss_client(const KinesisHttpStub& stub) {
    clink::aws::AwsClientOptions o;
    o.region = "us-east-1";
    o.endpoint_override = stub.endpoint();
    o.request_timeout_ms = 2000;
    return o;
}

KinesisSinkOptions kss_options(const KinesisHttpStub& stub, std::string partition_key = "k") {
    kss_credentials();
    KinesisSinkOptions o;
    o.stream = "orders";
    o.partition_key = std::move(partition_key);
    o.client = kss_client(stub);
    o.retry_base_backoff = 1ms;
    o.max_backoff = 8ms;
    o.retry_timeout = 30s;
    return o;
}

Batch<std::string> kss_batch(const std::vector<std::string>& records) {
    Batch<std::string> b;
    for (const auto& r : records) {
        b.emplace(r);
    }
    return b;
}

CheckpointBarrier kss_barrier(std::uint64_t id) {
    return CheckpointBarrier{clink::CheckpointId{id}};
}

template <typename F>
std::string kss_thrown(F&& f) {
    try {
        f();
    } catch (const std::exception& e) {
        return e.what();
    }
    return "";
}

}  // namespace

TEST(KinesisSinkSdk, TheSdkSendsEachRecordWithItsKeyAndBytes) {
    KinesisHttpStub stub;
    KinesisSink s(kss_options(stub));
    s.open();
    s.on_data(kss_batch({R"({"k":"u-1"})",
                         R"({"k":9007199254740993})",
                         R"({"k":1.50})",
                         "{\"k\":\"\xC3\xA9\xF0\x9F\x98\x80\"}"}));
    s.on_barrier(kss_barrier(1));
    const auto requests = stub.requests();
    ASSERT_EQ(requests.size(), 1u);
    EXPECT_EQ(requests[0].target, "Kinesis_20131202.PutRecords");
    EXPECT_EQ(requests[0].stream_name, "orders");
    EXPECT_EQ(requests[0].stream_arn, "");
    EXPECT_EQ(requests[0].records,
              (std::vector<KinesisHttpStub::Record>{
                  {"u-1", R"({"k":"u-1"})"},
                  {"9007199254740993", R"({"k":9007199254740993})"},
                  {"1.5", R"({"k":1.50})"},
                  {"\xC3\xA9\xF0\x9F\x98\x80", "{\"k\":\"\xC3\xA9\xF0\x9F\x98\x80\"}"}}));
    // Opaque payloads, every byte value among them, go whole; an ARN names the
    // stream by StreamARN alone.
    auto o = kss_options(stub, "");
    o.stream = "arn:aws:kinesis:us-east-1:111111111111:stream/orders";
    KinesisSink bare(std::move(o));
    bare.open();
    std::string binary;
    for (int i = 0; i < 256; ++i) {
        binary.push_back(static_cast<char>(i));
    }
    bare.on_data(kss_batch({binary, ""}));
    bare.flush();
    const auto second = stub.requests().back();
    EXPECT_EQ(second.stream_arn, "arn:aws:kinesis:us-east-1:111111111111:stream/orders");
    EXPECT_EQ(second.stream_name, "");
    EXPECT_EQ(second.records, (std::vector<KinesisHttpStub::Record>{{"0", binary}, {"1", ""}}));
}

TEST(KinesisSinkSdk, TheSdkReadsWhatTheResponseSays) {
    KinesisHttpStub stub;
    kss_credentials();
    auto service = make_kinesis_write_sdk_service(kss_client(stub));
    const std::vector<KinesisPutRecord> three = {{"a", "ka"}, {"b", "kb"}, {"c", "kc"}};
    stub.script({KinesisHttpStub::store({1}),
                 KinesisHttpStub::raw(200,
                                      R"({"Records":[{"SequenceNumber":"1","ShardId":"s"},)"
                                      R"({"SequenceNumber":"2","ShardId":"s"}]})"),
                 KinesisHttpStub::error(400, "ProvisionedThroughputExceededException", "Rate"),
                 KinesisHttpStub::error(400, "AccessDeniedException", "denied"),
                 KinesisHttpStub::error(500, "InternalFailure", "oops"),
                 KinesisHttpStub::error(503, "ServiceUnavailable", "later"),
                 KinesisHttpStub::error(400, "KMSThrottlingException", "kms"),
                 KinesisHttpStub::error(400, "ResourceNotFoundException", "no stream")});
    auto r = service->put_records("orders", three);
    ASSERT_TRUE(r.error.ok()) << r.error.code;
    EXPECT_EQ(r.failed_record_count, 1);
    ASSERT_EQ(r.entries.size(), 3u);
    EXPECT_FALSE(r.entries[0].sequence_number.empty());
    EXPECT_EQ(r.entries[0].shard_id, "shardId-000000000000");
    EXPECT_EQ(r.entries[1].error_code, "ProvisionedThroughputExceededException");
    EXPECT_NE(r.entries[1].error_message.find("Rate exceeded"), std::string::npos);
    EXPECT_TRUE(r.entries[1].sequence_number.empty());
    // A response without FailedRecordCount reads 0.
    r = service->put_records("orders", three);
    ASSERT_TRUE(r.error.ok());
    EXPECT_EQ(r.failed_record_count, 0);
    EXPECT_EQ(r.entries.size(), 2u);
    for (const auto& [code, status, fault] :
         std::vector<std::tuple<std::string, int, KinesisPutFault>>{
             {"ProvisionedThroughputExceededException", 400, KinesisPutFault::Throttled},
             {"AccessDeniedException", 400, KinesisPutFault::Permanent},
             {"InternalFailure", 500, KinesisPutFault::Transient},
             {"ServiceUnavailable", 503, KinesisPutFault::Transient},
             {"KMSThrottlingException", 400, KinesisPutFault::Throttled},
             {"ResourceNotFoundException", 400, KinesisPutFault::Permanent}}) {
        r = service->put_records("orders", three);
        EXPECT_EQ(r.error.fault, fault) << code << " read as " << r.error.code;
        EXPECT_NE(r.error.code.find(code), std::string::npos) << r.error.code;
        EXPECT_EQ(r.error.http_status, status) << code;
    }
}

// Through the SDK, a response that does not account for its request fails the
// sink; one the earlier sink took as success (FailedRecordCount 0, entries
// missing) or read past the request's records (a failed entry after them)
// included.
TEST(KinesisSinkSdk, AResponseThatDoesNotAccountForItsRequestFailsTheSink) {
    const std::vector<std::pair<std::string, std::string>> cases = {
        {R"({"FailedRecordCount":0,"Records":[]})", "returned 0 entries for 3 records"},
        {R"({"FailedRecordCount":1,"Records":[{"SequenceNumber":"1","ShardId":"s"},)"
         R"({"SequenceNumber":"2","ShardId":"s"},{"SequenceNumber":"3","ShardId":"s"},)"
         R"({"ErrorCode":"InternalFailure","ErrorMessage":"x"}]})",
         "returned 4 entries for 3 records"},
        {R"({"FailedRecordCount":0,"Records":[{"SequenceNumber":"1","ShardId":"s"},)"
         R"({"SequenceNumber":"2"},{"SequenceNumber":"3","ShardId":"s"}]})",
         "entry 1 has a SequenceNumber but no ShardId"},
        {R"({"FailedRecordCount":0,"Records":[{"SequenceNumber":"1","ShardId":"s"},)"
         R"({"SequenceNumber":"2","ShardId":"s","ErrorCode":"InternalFailure"},)"
         R"({"SequenceNumber":"3","ShardId":"s"}]})",
         "entry 1 holds both"},
    };
    for (const auto& [body, why] : cases) {
        KinesisHttpStub stub;
        stub.script({KinesisHttpStub::raw(200, body)});
        KinesisSink s(kss_options(stub));
        s.open();
        s.on_data(kss_batch({R"({"k":"a"})", R"({"k":"b"})", R"({"k":"c"})"}));
        const auto thrown = kss_thrown([&] { s.on_barrier(kss_barrier(1)); });
        EXPECT_NE(thrown.find("does not account for its request"), std::string::npos) << thrown;
        EXPECT_NE(thrown.find(why), std::string::npos) << body << ": " << thrown;
        EXPECT_TRUE(s.failed());
        EXPECT_EQ(stub.requests().size(), 1u);
    }
}

// The SDK's own retries are off: every attempt the sink makes is one request,
// so max_retries and retry_timeout bound what the stream sees.
TEST(KinesisSinkSdk, TheSdkMakesOneRequestPerAttempt) {
    KinesisHttpStub stub;
    std::vector<KinesisHttpStub::Step> failing(10, KinesisHttpStub::error(500, "InternalFailure"));
    stub.script(failing);
    auto o = kss_options(stub);
    o.max_retries = 2;
    KinesisSink s(std::move(o));
    s.open();
    s.on_data(kss_batch({R"({"k":"a"})"}));
    const auto why = kss_thrown([&] { s.on_barrier(kss_barrier(1)); });
    EXPECT_NE(why.find("after 3 attempt(s) in a row that stored nothing"), std::string::npos)
        << why;
    EXPECT_EQ(stub.requests().size(), 3u);
    // Throttled, then stored: two requests, the record stored once.
    KinesisHttpStub second;
    second.script({KinesisHttpStub::error(400, "ProvisionedThroughputExceededException")});
    KinesisSink t(kss_options(second));
    t.open();
    t.on_data(kss_batch({R"({"k":"b"})"}));
    t.on_barrier(kss_barrier(1));
    EXPECT_EQ(second.requests().size(), 2u);
    EXPECT_EQ(second.stored().size(), 1u);
}

// A reply that never arrives after the stream stored the request: the SDK's
// request timeout ends the call, the sink sends the records again, and they are
// stored twice; none is lost. The SDK asks for 100 Continue and curl measures
// the transfer speed over five seconds, so the call ends about six seconds
// after request_timeout_ms of silence would suggest.
TEST(KinesisSinkSdk, AReplyLostAfterTheStreamStoredItIsSentAgain) {
    KinesisHttpStub stub;
    stub.script({KinesisHttpStub::stall(30000ms)});
    auto o = kss_options(stub);
    o.client.request_timeout_ms = 1000;
    KinesisSink s(std::move(o));
    s.open();
    s.on_data(kss_batch({R"({"k":"a"})", R"({"k":"b"})"}));
    const auto t0 = std::chrono::steady_clock::now();
    s.on_barrier(kss_barrier(1));
    const auto took = std::chrono::steady_clock::now() - t0;
    EXPECT_FALSE(s.failed());
    EXPECT_LT(took, 20s);
    EXPECT_EQ(stub.requests().size(), 2u);
    const auto stored = stub.stored();
    ASSERT_EQ(stored.size(), 4u);
    EXPECT_EQ(stored[0], stored[2]);
    EXPECT_EQ(stored[1], stored[3]);
}

// A loopback listener that accepts connections and never reads or answers.
class KssSilentListener {
public:
    KssSilentListener() {
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
    ~KssSilentListener() {
        ::shutdown(fd_, SHUT_RDWR);
        ::close(fd_);
        thread_.join();
        for (const int c : held_) {
            ::close(c);
        }
    }
    KssSilentListener(const KssSilentListener&) = delete;
    KssSilentListener& operator=(const KssSilentListener&) = delete;
    [[nodiscard]] int port() const { return port_; }
    [[nodiscard]] int accepted() const { return accepted_.load(); }

private:
    int fd_{-1};
    int port_{0};
    std::atomic<int> accepted_{0};
    std::vector<int> held_;
    std::thread thread_;
};

// A cancel while the SDK is still sending (here: waiting for the 100 Continue
// it asked for, then sending the body) ends the call at once.
TEST(KinesisSinkSdk, ACancelWhileTheSdkSendsEndsTheCall) {
    kss_credentials();
    KssSilentListener listener;
    KinesisSinkOptions o;
    o.stream = "orders";
    o.partition_key = "k";
    o.client.region = "us-east-1";
    o.client.endpoint_override = "http://127.0.0.1:" + std::to_string(listener.port());
    o.client.request_timeout_ms = 30000;
    KinesisSink s(std::move(o));
    auto cancel = std::make_shared<std::atomic<bool>>(false);
    clink::RuntimeContext ctx(clink::OperatorId{4848}, "kinesis_sink", nullptr, nullptr);
    ctx.set_cancel_signal(clink::CancelSignal{cancel, nullptr});
    s.attach_runtime(&ctx);
    s.open();
    s.on_data(kss_batch({R"({"k":"a"})"}));
    auto done = std::async(std::launch::async,
                           [&] { return kss_thrown([&] { s.on_barrier(kss_barrier(1)); }); });
    for (int i = 0; i < 200 && listener.accepted() == 0; ++i) {
        std::this_thread::sleep_for(10ms);
    }
    ASSERT_GT(listener.accepted(), 0) << "the request never reached the listener";
    EXPECT_EQ(done.wait_for(300ms), std::future_status::timeout) << "the call ended by itself";
    const auto t0 = std::chrono::steady_clock::now();
    cancel->store(true);
    ASSERT_EQ(done.wait_for(10s), std::future_status::ready);
    EXPECT_LT(std::chrono::steady_clock::now() - t0, 5s);
    EXPECT_NE(done.get().find("cancelled while a PutRecords of 1 record(s) was in flight"),
              std::string::npos);
    EXPECT_TRUE(s.failed());
    s.attach_runtime(nullptr);
}

// Once the request is sent the SDK does not look at the abort until data moves,
// so a cancel while the reply is held ends with the SDK's request timeout; the
// sink still reports the cancellation and never counts the records stored.
TEST(KinesisSinkSdk, ACancelWhileAReplyIsHeldEndsWithTheRequestTimeout) {
    KinesisHttpStub stub;
    stub.script({KinesisHttpStub::hang()});
    auto o = kss_options(stub);
    o.client.request_timeout_ms = 1000;
    KinesisSink s(std::move(o));
    auto cancel = std::make_shared<std::atomic<bool>>(false);
    clink::RuntimeContext ctx(clink::OperatorId{4849}, "kinesis_sink", nullptr, nullptr);
    ctx.set_cancel_signal(clink::CancelSignal{cancel, nullptr});
    s.attach_runtime(&ctx);
    s.open();
    s.on_data(kss_batch({R"({"k":"a"})"}));
    auto done = std::async(std::launch::async,
                           [&] { return kss_thrown([&] { s.on_barrier(kss_barrier(1)); }); });
    while (!stub.hanging()) {
        std::this_thread::sleep_for(5ms);
    }
    cancel->store(true);
    ASSERT_EQ(done.wait_for(20s), std::future_status::ready);
    EXPECT_NE(done.get().find("cancelled while a PutRecords of 1 record(s) was in flight; "
                              "whether they were stored is unknown"),
              std::string::npos);
    EXPECT_TRUE(s.failed());
    EXPECT_EQ(stub.requests().size(), 1u) << "a cancelled sink sends nothing more";
    s.attach_runtime(nullptr);
    stub.release();
}
