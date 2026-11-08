// The Kinesis sink, deterministically: the partition-key rules, the response
// judge and the error classification, pure; and the production KinesisSink
// against a model of PutRecords (kinesis_sink_model.hpp), which keeps every
// record it stores with its partition key, duplicates included, so the cases
// check what a reader of the stream would see, not what the sink believes it
// sent. Nothing here talks to a service.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include <aws/kinesis/model/PutRecordsRequest.h>
#include <aws/kinesis/model/PutRecordsRequestEntry.h>
#include <gtest/gtest.h>

#include "clink/aws/kinesis_sink.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/runtime/dead_letter.hpp"
#include "clink/runtime/runtime_context.hpp"

#include "kinesis_sink_model.hpp"

using namespace std::chrono_literals;
using clink::Batch;
using clink::CheckpointBarrier;
using clink::aws::classify_kinesis_put_error;
using clink::aws::DlqPolicy;
using clink::aws::judge_kinesis_response;
using clink::aws::kinesis_key_chars;
using clink::aws::kinesis_key_problem;
using clink::aws::kinesis_record_key;
using clink::aws::KinesisPutFault;
using clink::aws::KinesisPutResponseEntry;
using clink::aws::KinesisPutResult;
using clink::aws::KinesisSink;
using clink::aws::KinesisSinkOptions;
using clink::aws::kKinesisDefaultMaxRecordBytes;
using clink::aws::kKinesisDefaultRequestBytes;
using clink::aws::kKinesisMaxRecordBytes;
using clink::aws::kKinesisMaxRequestBytes;
using clink::aws::test::KinesisSinkModel;

namespace {

constexpr char kKsStream[] = "orders";

std::shared_ptr<KinesisSinkModel> ks_model(std::size_t max_record_bytes = 1024 * 1024) {
    return std::make_shared<KinesisSinkModel>(kKsStream, max_record_bytes);
}

KinesisSinkOptions ks_options(const std::shared_ptr<KinesisSinkModel>& m,
                              std::string partition_key = "k") {
    KinesisSinkOptions o;
    o.stream = kKsStream;
    o.partition_key = std::move(partition_key);
    o.service = m;
    o.retry_base_backoff = 1ms;
    o.max_backoff = 8ms;
    o.retry_timeout = 30s;
    return o;
}

Batch<std::string> ks_batch(const std::vector<std::string>& records) {
    Batch<std::string> b;
    for (const auto& r : records) {
        b.emplace(r);
    }
    return b;
}

CheckpointBarrier ks_barrier(std::uint64_t id) {
    return CheckpointBarrier{clink::CheckpointId{id}};
}

template <typename F>
std::string ks_thrown(F&& f) {
    try {
        f();
    } catch (const std::exception& e) {
        return e.what();
    }
    return "";
}

// A record whose member "k" holds `key_json`, written as JSON.
std::string ks_record(int id, const std::string& key_json) {
    return "{\"id\":" + std::to_string(id) + ",\"k\":" + key_json + "}";
}

// The key a record routes by under partition_key='k', or "refused: <why>".
std::string ks_key(const std::string& record) {
    const auto r = kinesis_record_key(record, "k");
    return r.ok ? r.key : "refused: " + r.why;
}

std::uint64_t ks_counter(const std::string& name) {
    for (const auto& [k, v] : clink::MetricsRegistry::global().snapshot().counters) {
        if (k == name) {
            return v;
        }
    }
    return 0;
}

class KsDeadLetters final : public clink::DeadLetterQueue {
public:
    void report(const clink::BadRecord& rec) override {
        const std::lock_guard<std::mutex> lock(mu_);
        got_.push_back(rec);
    }
    std::vector<clink::BadRecord> got() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return got_;
    }

private:
    mutable std::mutex mu_;
    std::vector<clink::BadRecord> got_;
};

// A sink attached to a runtime context whose cancel signal the case controls and
// whose dead-letter path it reads.
struct KsAttached {
    explicit KsAttached(KinesisSink& s) : sink(s) {
        ctx = std::make_unique<clink::RuntimeContext>(
            clink::OperatorId{4747}, "kinesis_sink", nullptr, nullptr);
        ctx->set_cancel_signal(clink::CancelSignal{cancel, nullptr});
        ctx->set_dead_letter_queue(&dead);
        sink.attach_runtime(ctx.get());
    }
    ~KsAttached() { sink.attach_runtime(nullptr); }
    KsAttached(const KsAttached&) = delete;
    KsAttached& operator=(const KsAttached&) = delete;
    KinesisSink& sink;
    std::shared_ptr<std::atomic<bool>> cancel = std::make_shared<std::atomic<bool>>(false);
    KsDeadLetters dead;
    std::unique_ptr<clink::RuntimeContext> ctx;
};

KinesisPutResult ks_response(std::int64_t failed_record_count,
                             std::vector<KinesisPutResponseEntry> entries) {
    KinesisPutResult r;
    r.failed_record_count = failed_record_count;
    r.entries = std::move(entries);
    return r;
}

KinesisPutResponseEntry ks_ok(int n) {
    return {
        "49600" + std::to_string(1000 + n), "shardId-00000000000" + std::to_string(n % 4), "", ""};
}

const KinesisPutResponseEntry kKsThrottled{
    "",
    "",
    "ProvisionedThroughputExceededException",
    "Rate exceeded for shard shardId-000000000001 in stream orders under account 111111111111."};
const KinesisPutResponseEntry kKsInternal{"", "", "InternalFailure", "Internal service failure."};

std::vector<std::string> ks_numbered(int from, int to) {
    std::vector<std::string> out;
    for (int i = from; i < to; ++i) {
        out.push_back(ks_record(i, "\"key-" + std::to_string(i) + "\""));
    }
    return out;
}

std::string ks_emoji(std::size_t n) {
    std::string out;
    for (std::size_t i = 0; i < n; ++i) {
        out += "\xF0\x9F\x98\x80";  // U+1F600, two UTF-16 code units
    }
    return out;
}

}  // namespace

// ---- partition keys ---------------------------------------------------------------------

TEST(KinesisSinkKeys, StringsAreUsedAsTheyAre) {
    EXPECT_EQ(ks_key(R"({"k":"u-1"})"), "u-1");
    EXPECT_EQ(ks_key(R"({"k":"a\"b\\c\/d\te"})"), "a\"b\\c/d\te");
    EXPECT_EQ(ks_key(R"({"k":"é😀"})"), "\xC3\xA9\xF0\x9F\x98\x80");
    EXPECT_EQ(ks_key("{\"k\":\"\xE6\x97\xA5\xE6\x9C\xAC\"}"), "\xE6\x97\xA5\xE6\x9C\xAC");
    // Only the top level counts, wherever the member stands.
    EXPECT_EQ(ks_key(R"({"x":{"k":"inner"},"y":[{"k":1}],"k":"outer"})"), "outer");
    EXPECT_EQ(ks_key(R"({"k":"v","a":[1,2.5e3,-0,true,false,null,{"b":"c"},[]],"d":{}})"), "v");
    // A member name compares as decoded, and case matters.
    EXPECT_EQ(ks_key(R"({"k":"escaped name"})"), "escaped name");
    EXPECT_EQ(ks_key(R"({"K":"v"})"), "refused: it has no member \"k\"");
    EXPECT_EQ(ks_key(" \n\t{ \"k\" : \"spaced\" } \r\n"), "spaced");
    EXPECT_EQ(ks_key(R"({"k":true})"), "true");
    EXPECT_EQ(ks_key(R"({"k":false})"), "false");
    // A string and a number with the same text are the same key.
    EXPECT_EQ(ks_key(R"({"k":"42"})"), ks_key(R"({"k":42})"));
}

// A number is its exact value in plain decimal. What the earlier sink made of
// each, through a double and std::to_string, is noted where it differed.
TEST(KinesisSinkKeys, NumbersKeepTheirExactValue) {
    const std::vector<std::pair<std::string, std::string>> cases = {
        // Neighbouring integers past 2^53 (the earlier sink gave all three
        // 9007199254740992).
        {"9007199254740992", "9007199254740992"},
        {"9007199254740993", "9007199254740993"},
        {"9007199254740995", "9007199254740995"},
        {"-9223372036854775809", "-9223372036854775809"},
        {"18446744073709551617", "18446744073709551617"},
        {"123456789012345678901234567890", "123456789012345678901234567890"},
        // Fractions std::to_string collapses (both 0.000000, both 1.000000).
        {"0.0000001", "0.0000001"},
        {"0.0000002", "0.0000002"},
        {"1.0000001", "1.0000001"},
        {"1.0000002", "1.0000002"},
        {"0.1000000000000000000001", "0.1000000000000000000001"},
        // One value, one key, however it is written.
        {"1.5", "1.5"},
        {"1.50", "1.5"},
        {"15E-1", "1.5"},
        {"0.15e1", "1.5"},
        {"1e3", "1000"},
        {"1E+3", "1000"},
        {"1.0", "1"},
        {"100", "100"},
        {"-0", "0"},
        {"-0.0e7", "0"},
        {"0e999999999999999999999", "0"},
        {"-12.340", "-12.34"},
        {"1E-7", "0.0000001"},
        {"123.456e1", "1234.56"},
        {"123.456e-5", "0.00123456"},
        // At the 256-character limit.
        {"1e255", "1" + std::string(255, '0')},
        {"-1e254", "-1" + std::string(254, '0')},
        {"1e-254", "0." + std::string(253, '0') + "1"},
    };
    for (const auto& [json, key] : cases) {
        EXPECT_EQ(ks_key(ks_record(1, json)), key) << json;
    }
    for (const auto* json : {"1e256", "-1e255", "1e-255", "1e999999999999999999999999"}) {
        EXPECT_NE(ks_key(ks_record(1, json)).find("plain decimal form is longer than 256"),
                  std::string::npos)
            << json;
    }
}

TEST(KinesisSinkKeys, RecordsWithoutAUsableKeyAreRefused) {
    const std::vector<std::pair<std::string, std::string>> cases = {
        {"{}", "it has no member \"k\""},
        {R"({"k":null})", "member \"k\" is null"},
        {R"({"k":{"a":1}})", "member \"k\" is an object"},
        {R"({"k":[1]})", "member \"k\" is an array"},
        {R"({"k":""})", "is an empty string"},
        {R"({"k":"a\u0000b"})", "holds U+0000"},
        {R"({"k":"\ud800"})", "escaped surrogate without its pair"},
        {R"({"k":"\udc00x"})", "escaped surrogate without its pair"},
        {R"({"k":"\ud800A"})", "escaped surrogate without its pair"},
        {"{\"k\":\"" + std::string(257, 'x') + "\"}", "is 257 characters long"},
        {R"({"k":"a","k":"b"})", "appears more than once"},
        {R"({"k":"a","k":"b"})", "appears more than once"},
        {"[]", "it is not a JSON object"},
        {"\"k\"", "it is not a JSON object"},
        {"42", "it is not a JSON object"},
        {"", "it is not a JSON object: it is empty"},
        {"   ", "it is not a JSON object: it is empty"},
        {R"({"k":"a")", "a member is not followed by ',' or '}'"},
        {R"({"k":"a",})", "a member name is not a string"},
        {R"({"k":"a"} x)", "something follows the object"},
        {R"({"k":01})", "a member is not followed by ',' or '}'"},
        {R"({"k":1.})", "a number is malformed"},
        {R"({"k":-})", "a number is malformed"},
        {R"({"k":tru})", "an unexpected character"},
        {R"({"x":[1,2},"k":"a"})", "a value is not followed by ',' or ']'"},
        {R"({"x":{"a"1},"k":"a"})", "a member name is not followed by ':'"},
        {"{\"k\":\"a\",\"x\":\"\xff\"}", "not valid UTF-8 at byte"},
        {"{\"k\":\"\xc3\x28\"}", "not valid UTF-8"},
        {"{\"k\":\"\xc0\xaf\"}", "not valid UTF-8"},
        {"{\"k\":\"\xed\xa0\x80\"}", "not valid UTF-8"},
        {"{\"k\":\"a\",\"x\":\"tab\there\"}", "an unescaped control character"},
        {R"({"k":"a","x":"\q"})", "an invalid escape"},
        {R"({"k":"\u12"})", "a \\u escape"},
    };
    for (const auto& [record, why] : cases) {
        const auto r = kinesis_record_key(record, "k");
        EXPECT_FALSE(r.ok) << record;
        EXPECT_NE(r.why.find(why), std::string::npos) << record << ": " << r.why;
    }
}

TEST(KinesisSinkKeys, KeysAreCountedInUtf16CodeUnits) {
    EXPECT_EQ(kinesis_key_chars("abc"), 3u);
    EXPECT_EQ(kinesis_key_chars("\xC3\xA9"), 1u);          // e acute, 2 bytes
    EXPECT_EQ(kinesis_key_chars("\xE6\x97\xA5"), 1u);      // 3 bytes
    EXPECT_EQ(kinesis_key_chars("\xF0\x9F\x98\x80"), 2u);  // U+1F600, 4 bytes
    EXPECT_EQ(kinesis_key_problem(ks_emoji(128)), "");     // 256 units, 512 bytes
    EXPECT_NE(kinesis_key_problem(ks_emoji(129)).find("is 258 characters long"), std::string::npos);
    std::string accents;
    for (int i = 0; i < 256; ++i) {
        accents += "\xC3\xA9";
    }
    EXPECT_EQ(kinesis_key_problem(accents), "");  // 256 characters, 512 bytes
    EXPECT_NE(kinesis_key_problem(accents + "\xC3\xA9").find("is 257"), std::string::npos);
    EXPECT_NE(kinesis_key_problem(std::string(255, 'x') + "\xF0\x9F\x98\x80").find("is 257"),
              std::string::npos);
    EXPECT_EQ(kinesis_key_problem(std::string(256, 'x')), "");
    EXPECT_NE(kinesis_key_problem("\xff").find("not valid UTF-8"), std::string::npos);
    // Through a record, a key of 128 astral characters goes and one of 129 does not.
    EXPECT_EQ(ks_key(ks_record(1, "\"" + ks_emoji(128) + "\"")), ks_emoji(128));
    EXPECT_NE(ks_key(ks_record(1, "\"" + ks_emoji(129) + "\"")).find("258 characters"),
              std::string::npos);
}

// The scan keeps its own stack, so a record nested far deeper than any call
// stack holds is read, or refused, without overflowing.
TEST(KinesisSinkKeys, ADeeplyNestedRecordCostsNoStack) {
    constexpr std::size_t kDepth = 200000;
    const std::string deep = std::string(kDepth, '[') + std::string(kDepth, ']');
    EXPECT_EQ(ks_key("{\"x\":" + deep + ",\"k\":\"after\"}"), "after");
    std::string objects;
    for (std::size_t i = 0; i < kDepth; ++i) {
        objects += "{\"a\":";
    }
    objects += "1" + std::string(kDepth, '}');
    EXPECT_EQ(ks_key("{\"k\":\"before\",\"x\":" + objects + "}"), "before");
    EXPECT_NE(
        ks_key("{\"k\":\"a\",\"x\":" + std::string(kDepth, '[') + "}").find("is not valid JSON"),
        std::string::npos);
}

// What the refusal of U+0000 rests on: the SDK's request body stops a partition
// key at its NUL, while the data goes as base64 of every byte. Should a later
// SDK keep the whole key, this fails, and the refusal can be lifted.
TEST(KinesisSinkKeys, TheSdkCutsKeysAtNulAndSendsDataWhole) {
    const std::string data("a\0b\xff", 4);
    Aws::Kinesis::Model::PutRecordsRequestEntry e;
    e.SetData(
        Aws::Utils::ByteBuffer(reinterpret_cast<const unsigned char*>(data.data()), data.size()));
    e.SetPartitionKey(Aws::String("k\0ey", 4));
    Aws::Kinesis::Model::PutRecordsRequest r;
    r.SetStreamName(kKsStream);
    r.AddRecords(e);
    std::string body;
    for (const char c : r.SerializePayload()) {
        if (c != '\t' && c != '\n') {
            body.push_back(c);
        }
    }
    EXPECT_NE(body.find(R"("PartitionKey":"k")"), std::string::npos) << body;
    EXPECT_EQ(body.find("u0000"), std::string::npos) << body;
    EXPECT_NE(body.find(R"("Data":"YQBi/w==")"), std::string::npos) << body;
}

// ---- the response judge ----------------------------------------------------------------

TEST(KinesisSinkResponse, AValidResponseAccountsForEveryRecord) {
    auto v = judge_kinesis_response(3, ks_response(0, {ks_ok(0), ks_ok(1), ks_ok(2)}));
    EXPECT_TRUE(v.valid) << v.why;
    EXPECT_TRUE(v.failed.empty());
    v = judge_kinesis_response(4, ks_response(2, {ks_ok(0), kKsThrottled, ks_ok(2), kKsInternal}));
    EXPECT_TRUE(v.valid) << v.why;
    EXPECT_EQ(v.failed, (std::vector<std::size_t>{1, 3}));
    v = judge_kinesis_response(2, ks_response(2, {kKsThrottled, kKsInternal}));
    EXPECT_TRUE(v.valid) << v.why;
    EXPECT_EQ(v.failed.size(), 2u);
    // A code the documentation does not list is still a failed entry, retried.
    v = judge_kinesis_response(1, ks_response(1, {{"", "", "SomethingNew", ""}}));
    EXPECT_TRUE(v.valid) << v.why;
    EXPECT_EQ(v.failed, (std::vector<std::size_t>{0}));
    // The smallest and largest sequence numbers the documentation allows.
    v = judge_kinesis_response(
        2,
        ks_response(0,
                    {{"0", "shardId-0", "", ""},
                     {"9" + std::string(128, '1'), std::string(128, 's'), "", ""}}));
    EXPECT_TRUE(v.valid) << v.why;
}

TEST(KinesisSinkResponse, AnyOtherResponseIsNeverASuccess) {
    const KinesisPutResponseEntry both{"496001", "shardId-0", "InternalFailure", "x"};
    const KinesisPutResponseEntry shard_and_error{"", "shardId-0", "InternalFailure", "x"};
    const KinesisPutResponseEntry neither{"", "", "", ""};
    const KinesisPutResponseEntry shard_only{"", "shardId-0", "", ""};
    const KinesisPutResponseEntry sequence_only{"496001", "", "", ""};
    const std::vector<std::tuple<std::string, std::size_t, KinesisPutResult, std::string>> cases = {
        // The false success: FailedRecordCount 0 with entries missing.
        {"no entries", 3, ks_response(0, {}), "returned 0 entries for 3 records"},
        {"short", 3, ks_response(0, {ks_ok(0), ks_ok(1)}), "returned 2 entries for 3 records"},
        // A failed entry past the request's records, which an unchecked index read.
        {"oversized",
         2,
         ks_response(1, {ks_ok(0), ks_ok(1), kKsThrottled}),
         "returned 3 entries for 2 records"},
        {"both", 2, ks_response(1, {ks_ok(0), both}), "entry 1 holds both"},
        {"shard and error", 1, ks_response(1, {shard_and_error}), "entry 0 holds both"},
        {"neither", 2, ks_response(0, {neither, ks_ok(1)}), "entry 0 holds neither"},
        {"shard only", 1, ks_response(0, {shard_only}), "has a ShardId but no SequenceNumber"},
        {"sequence only",
         1,
         ks_response(0, {sequence_only}),
         "has a SequenceNumber but no ShardId"},
        {"leading zero",
         1,
         ks_response(0, {{"0496", "shardId-0", "", ""}}),
         "malformed SequenceNumber"},
        {"letters", 1, ks_response(0, {{"49a6", "shardId-0", "", ""}}), "malformed SequenceNumber"},
        {"too long",
         1,
         ks_response(0, {{"9" + std::string(129, '1'), "shardId-0", "", ""}}),
         "malformed SequenceNumber"},
        {"shard chars", 1, ks_response(0, {{"496", "shard 0", "", ""}}), "malformed ShardId"},
        {"shard length",
         1,
         ks_response(0, {{"496", std::string(129, 's'), "", ""}}),
         "malformed ShardId"},
        {"repeated",
         2,
         ks_response(0, {ks_ok(0), ks_ok(0)}),
         "entry 1 repeats the SequenceNumber of an earlier entry"},
        // A response without FailedRecordCount reads 0, which a failed entry contradicts.
        {"count missing",
         2,
         ks_response(0, {ks_ok(0), kKsThrottled}),
         "reports FailedRecordCount 0 where 1 entries failed"},
        {"count high",
         2,
         ks_response(2, {ks_ok(0), kKsThrottled}),
         "reports FailedRecordCount 2 where 1 entries failed"},
        {"count negative", 1, ks_response(-1, {ks_ok(0)}), "reports FailedRecordCount -1"},
    };
    for (const auto& [name, sent, r, why] : cases) {
        const auto v = judge_kinesis_response(sent, r);
        EXPECT_FALSE(v.valid) << name;
        EXPECT_TRUE(v.failed.empty()) << name;
        EXPECT_NE(v.why.find(why), std::string::npos) << name << ": " << v.why;
    }
}

// ---- classification ----------------------------------------------------------------------

TEST(KinesisSinkErrors, FollowsTheServicesErrors) {
    for (const char* c : {"ProvisionedThroughputExceededException",
                          "LimitExceededException",
                          "KMSThrottlingException",
                          "ThrottlingException"}) {
        EXPECT_EQ(classify_kinesis_put_error(c, 400, true), KinesisPutFault::Throttled) << c;
    }
    EXPECT_EQ(classify_kinesis_put_error("", 429, false), KinesisPutFault::Throttled);
    for (const char* c : {"InternalFailure",
                          "InternalFailureException",
                          "ServiceUnavailable",
                          "ResourceInUseException",
                          "RequestTimeoutException"}) {
        EXPECT_EQ(classify_kinesis_put_error(c, 400, false), KinesisPutFault::Transient) << c;
    }
    EXPECT_EQ(classify_kinesis_put_error("NoResponse", 0, false), KinesisPutFault::Transient);
    EXPECT_EQ(classify_kinesis_put_error("SomethingNew", 503, false), KinesisPutFault::Transient);
    for (const char* c : {"AccessDeniedException",
                          "ResourceNotFoundException",
                          "InvalidArgumentException",
                          "ValidationException",
                          "KMSAccessDeniedException",
                          "KMSDisabledException",
                          "KMSInvalidStateException",
                          "KMSNotFoundException",
                          "KMSOptInRequired",
                          "UnrecognizedClientException",
                          "ExpiredTokenException",
                          "DryRunOperationException",
                          "com.amazonaws.kinesis#ResourceNotFoundException",
                          "ResourceNotFoundException:http://internal.amazon.com/"}) {
        EXPECT_EQ(classify_kinesis_put_error(c, 400, true), KinesisPutFault::Permanent) << c;
    }
    EXPECT_EQ(classify_kinesis_put_error("SomethingNew", 400, false), KinesisPutFault::Permanent);
    EXPECT_EQ(classify_kinesis_put_error("SomethingNew", 400, true), KinesisPutFault::Transient);
}

// ---- options ----------------------------------------------------------------------------

TEST(KinesisSinkConfig, RefusesOptionsOutsideTheirRanges) {
    auto m = ks_model();
    const auto refused = [&](auto edit) {
        auto o = ks_options(m);
        edit(o);
        return ks_thrown([&] { KinesisSink s(std::move(o)); });
    };
    EXPECT_NE(refused([](auto& o) { o.stream.clear(); }).find("'stream' is required"),
              std::string::npos);
    EXPECT_NE(refused([](auto& o) { o.stream = "a b"; }).find("1 to 128 characters"),
              std::string::npos);
    EXPECT_NE(refused([](auto& o) { o.stream = std::string(129, 's'); }).find("1 to 128"),
              std::string::npos);
    EXPECT_EQ(refused([](auto& o) { o.stream = std::string(128, 's'); }), "");
    for (const char* arn : {"arn:aws:kinesis:eu-west-2:123456789012:stream/orders",
                            "arn:aws-cn:kinesis:cn-north-1:123456789012:stream/o.r-d_s"}) {
        EXPECT_EQ(refused([&](auto& o) { o.stream = arn; }), "") << arn;
    }
    for (const char* arn : {"arn:aws:kinesis:eu-west-2:12345:stream/orders",
                            "arn:aws:sqs:eu-west-2:123456789012:stream/orders",
                            "arn:aws:kinesis:eu-west-2:123456789012:stream/",
                            "arn:aws:kinesis:eu-west-2:123456789012:table/orders",
                            "arn:aws:kinesis:eu-west-2:123456789012"}) {
        EXPECT_NE(refused([&](auto& o) { o.stream = arn; }).find("stream ARN"), std::string::npos)
            << arn;
    }
    EXPECT_NE(refused([](auto& o) { o.partition_key = "\xff"; }).find("not valid UTF-8"),
              std::string::npos);
    EXPECT_NE(refused([](auto& o) { o.batch_records = 0; }).find("1 to 500"), std::string::npos);
    EXPECT_NE(refused([](auto& o) { o.batch_records = 501; }).find("1 to 500"), std::string::npos);
    EXPECT_NE(refused([](auto& o) { o.max_record_bytes = 0; }).find("'max_record_bytes' must be"),
              std::string::npos);
    EXPECT_NE(refused([](auto& o) {
                  o.max_record_bytes = kKinesisMaxRecordBytes + 1;
              }).find("'max_record_bytes' must be 1 to 10485760"),
              std::string::npos);
    EXPECT_NE(refused([](auto& o) {
                  o.max_record_bytes = 2 * 1024 * 1024;
                  o.max_request_bytes = 1024 * 1024;
              }).find("the largest record fits alone"),
              std::string::npos);
    EXPECT_NE(refused([](auto& o) {
                  o.max_request_bytes = kKinesisMaxRequestBytes + 1;
              }).find("'max_request_bytes' must be"),
              std::string::npos);
    EXPECT_EQ(refused([](auto& o) {
                  o.max_record_bytes = kKinesisMaxRecordBytes;
                  o.max_request_bytes = kKinesisMaxRequestBytes;
              }),
              "");
    EXPECT_EQ(refused([](auto& o) { o.max_request_bytes = o.max_record_bytes; }), "");
    EXPECT_NE(refused([](auto& o) { o.max_retries = -1; }).find("0 to 20"), std::string::npos);
    EXPECT_NE(refused([](auto& o) { o.max_retries = 21; }).find("0 to 20"), std::string::npos);
    EXPECT_NE(refused([](auto& o) { o.retry_base_backoff = 0ms; }).find("backoff"),
              std::string::npos);
    EXPECT_NE(refused([](auto& o) { o.retry_timeout = 0ms; }).find("retry_timeout_ms"),
              std::string::npos);
    EXPECT_NE(refused([](auto& o) { o.max_age = -1ms; }).find("linger_ms"), std::string::npos);
    EXPECT_NE(refused([](auto& o) { o.client.region = ""; }).find("'region' is empty"),
              std::string::npos);
    EXPECT_NE(refused([](auto& o) { o.client.request_timeout_ms = 0; }).find("request_timeout_ms"),
              std::string::npos);
    const auto why =
        refused([](auto& o) { o.client.endpoint_override = "https://AKIAEXAMPLE:hunter2@host"; });
    EXPECT_NE(why.find("without user information"), std::string::npos) << why;
    EXPECT_EQ(why.find("hunter2"), std::string::npos) << why;
    EXPECT_EQ(refused([](auto& o) { o.client.endpoint_override = "http://127.0.0.1:4566"; }), "");
    EXPECT_TRUE(m->calls().empty());
}

TEST(KinesisSinkConfig, TheFactoryChecksItsOptions) {
    const auto* f = clink::cluster::OperatorRegistry::default_instance().find_sink(
        "kinesis_sink", std::string{clink::cluster::kChannelString});
    ASSERT_NE(f, nullptr);
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = {{"stream", "orders"}};
    EXPECT_NO_THROW((void)f->build(ctx));
    // The request bound follows a raised record size unless it is set.
    auto raised = ctx;
    raised.params["max_record_bytes"] = "8388608";
    EXPECT_NO_THROW((void)f->build(raised));
    raised.params["max_request_bytes"] = "10485760";
    raised.params["dlq"] = "drop";
    raised.params["partition_key"] = "user_id";
    EXPECT_NO_THROW((void)f->build(raised));
    for (const auto& [key, value, rule] :
         std::vector<std::tuple<std::string, std::string, std::string>>{
             {"batch_records", "501", "'batch_records' must be 1 to 500"},
             {"batch_records", "0", "'batch_records' must be 1 to 500"},
             {"max_record_bytes", "0", "'max_record_bytes' must be 1 to 10485760"},
             {"max_record_bytes", "10485761", "'max_record_bytes' must be 1 to 10485760"},
             {"max_request_bytes", "10485761", "'max_request_bytes' must be 1048576 to 10485760"},
             {"max_request_bytes", "1048575", "'max_request_bytes' must be 1048576 to 10485760"},
             {"max_retries", "-1", "'max_retries' must be 0 to 20"},
             {"max_retries", "21", "'max_retries' must be 0 to 20"},
             {"connect_timeout_ms", "4294967396", "'connect_timeout_ms' must be 1 to 600000"},
             {"request_timeout_ms", "0", "'request_timeout_ms' must be 1 to 600000"},
             {"retry_timeout_ms", "3600001", "'retry_timeout_ms' must be 1 to 3600000"},
             {"linger_ms", "-5", "'linger_ms' must be 0 to 3600000"},
             {"dlq", "dorp", "'dlq' must be 'fail' or 'drop'"},
             {"stream", "a b", "1 to 128 characters"}}) {
        auto c = ctx;
        c.params[key] = value;
        EXPECT_NE(ks_thrown([&] { (void)f->build(c); }).find(rule), std::string::npos)
            << key << "=" << value;
    }
}

TEST(KinesisSinkConfig, ErrorsNameTheFixAndNoCredentials) {
    auto m = ks_model();
    m->set_planner([](std::size_t, const auto&) {
        return KinesisSinkModel::error(
            "AccessDeniedException",
            400,
            KinesisPutFault::Permanent,
            false,
            "User: arn:aws:iam::123456789012:user/u with key AKIAABCDEFGHIJKLMNOP is not "
            "authorized; "
            "Credential=AKIAABCDEFGHIJKLMNOP/20261108/eu-west-2/kinesis/aws4_request, "
            "Signature=0123456789abcdef");
    });
    KinesisSink s(ks_options(m));
    s.open();
    s.on_data(ks_batch({ks_record(1, "\"a\"")}));
    const auto why = ks_thrown([&] { s.on_barrier(ks_barrier(1)); });
    EXPECT_NE(why.find("PutRecords was refused: AccessDeniedException (HTTP 400)"),
              std::string::npos)
        << why;
    EXPECT_NE(why.find("kinesis:PutRecords"), std::string::npos) << why;
    EXPECT_EQ(why.find("AKIAABCDEFGHIJKLMNOP"), std::string::npos) << why;
    EXPECT_EQ(why.find("0123456789abcdef"), std::string::npos) << why;
}

// ---- records ----------------------------------------------------------------------------

TEST(KinesisSinkRecords, KeysReachTheServiceAsTheRecordsHoldThem) {
    auto m = ks_model();
    KinesisSink s(ks_options(m));
    s.open();
    const std::vector<std::pair<std::string, std::string>> want = {
        {ks_record(1, "\"u-1\""), "u-1"},
        {ks_record(2, "9007199254740993"), "9007199254740993"},
        {ks_record(3, "9007199254740992"), "9007199254740992"},
        {ks_record(4, "0.0000001"), "0.0000001"},
        {ks_record(5, "0.0000002"), "0.0000002"},
        {ks_record(6, "1.50"), "1.5"},
        {ks_record(7, "true"), "true"},
        {ks_record(8, "\"\xC3\xA9\xF0\x9F\x98\x80\""), "\xC3\xA9\xF0\x9F\x98\x80"},
    };
    std::vector<std::string> records;
    for (const auto& [record, key] : want) {
        records.push_back(record);
    }
    s.on_data(ks_batch(records));
    s.on_barrier(ks_barrier(1));
    EXPECT_EQ(m->stored_pairs(), want);
    // The records' bytes went as they came, the key's member included.
    for (const auto& stored : m->stored()) {
        EXPECT_NE(stored.data.find("\"k\":"), std::string::npos);
    }
}

TEST(KinesisSinkRecords, WithoutAPartitionKeyRecordsAreNumberedAndPayloadsAreOpaque) {
    auto m = ks_model();
    m->set_planner([](std::size_t call, const auto& records) {
        return call == 0 ? KinesisSinkModel::failing(records.size(), {1})
                         : KinesisSinkModel::Plan{};
    });
    KinesisSink s(ks_options(m, ""));
    s.open();
    std::string binary;
    for (int i = 0; i < 256; ++i) {
        binary.push_back(static_cast<char>(i));
    }
    const std::string not_json = "{\"half\": ";
    s.on_data(ks_batch({"", binary, not_json, std::string("a\0b", 3)}));
    s.on_barrier(ks_barrier(1));
    const auto calls = m->calls();
    ASSERT_EQ(calls.size(), 2u);
    // The failed record went again with the key it was first sent with.
    EXPECT_EQ(calls[1], (KinesisSinkModel::Sent{{binary, "1"}}));
    EXPECT_EQ(m->stored_pairs(),
              (KinesisSinkModel::Sent{
                  {"", "0"}, {not_json, "2"}, {std::string("a\0b", 3), "3"}, {binary, "1"}}));
    // Numbering goes on across batches; another instance starts again at 0.
    s.on_data(ks_batch({"next"}));
    s.on_barrier(ks_barrier(2));
    EXPECT_EQ(m->stored_pairs().back(), (std::pair<std::string, std::string>{"next", "4"}));
    KinesisSink fresh(ks_options(m, ""));
    fresh.open();
    fresh.on_data(ks_batch({"first"}));
    fresh.flush();
    EXPECT_EQ(m->stored_pairs().back(), (std::pair<std::string, std::string>{"first", "0"}));
}

TEST(KinesisSinkRecords, ARefusedRecordFailsTheSinkAndAddsNothingOfItsBatch) {
    auto m = ks_model();
    auto o = ks_options(m);
    o.batch_records = 2;
    KinesisSink s(std::move(o));
    s.open();
    s.on_data(ks_batch({ks_record(1, "\"a\""), ks_record(2, "\"b\""), ks_record(3, "\"c\"")}));
    ASSERT_EQ(m->stored().size(), 2u);  // a and b went when the buffer filled
    const auto why = ks_thrown([&] {
        s.on_data(ks_batch({ks_record(4, "\"d\""), R"({"id":5})", ks_record(6, "\"e\"")}));
    });
    EXPECT_NE(why.find("record 2 of a batch of 3 has no usable partition key: it has no member "
                       "\"k\""),
              std::string::npos)
        << why;
    EXPECT_NE(why.find("Nothing of that batch was buffered or sent"), std::string::npos) << why;
    EXPECT_NE(why.find("1 buffered record(s) not yet accepted are dropped"), std::string::npos)
        << why;
    EXPECT_TRUE(s.failed());
    EXPECT_NE(ks_thrown([&] { s.on_barrier(ks_barrier(1)); }).find("failed earlier"),
              std::string::npos);
    EXPECT_EQ(m->stored().size(), 2u);
    EXPECT_EQ(m->calls().size(), 1u);
}

TEST(KinesisSinkRecords, UnderDropRefusedRecordsGoToTheDeadLetterPathAndTheRestAreWritten) {
    auto m = ks_model();
    auto o = ks_options(m);
    o.dlq_policy = DlqPolicy::Drop;
    o.max_record_bytes = 1000;
    KinesisSink s(std::move(o));
    KsAttached a(s);
    s.open();
    const std::string dropped_metric =
        R"(clink_connector_dropped_records_total{connector="kinesis",direction="sink"})";
    const auto before = ks_counter(dropped_metric);
    const std::string oversized = ks_record(4, "\"big\"") + std::string(1000, ' ');
    s.on_data(ks_batch({ks_record(1, "\"a\""),
                        R"({"id":2})",
                        R"({"id":3,"k":null})",
                        oversized,
                        ks_record(5, "\"b\"")}));
    s.on_barrier(ks_barrier(1));
    EXPECT_EQ(m->stored_pairs(),
              (KinesisSinkModel::Sent{{ks_record(1, "\"a\""), "a"}, {ks_record(5, "\"b\""), "b"}}));
    const auto dead = a.dead.got();
    ASSERT_EQ(dead.size(), 3u);
    EXPECT_EQ(dead[0].payload, R"({"id":2})");
    EXPECT_NE(dead[0].error.find("record 2 of a batch of 5 has no usable partition key"),
              std::string::npos)
        << dead[0].error;
    EXPECT_NE(dead[1].error.find("member \"k\" is null"), std::string::npos) << dead[1].error;
    EXPECT_EQ(dead[2].payload, oversized);
    EXPECT_NE(dead[2].error.find("with its 3-byte partition key"), std::string::npos)
        << dead[2].error;
    for (const auto& d : dead) {
        EXPECT_EQ(d.connector, "kinesis");
        EXPECT_EQ(d.direction, "sink");
        EXPECT_EQ(d.location, kKsStream);
    }
    EXPECT_EQ(ks_counter(dropped_metric) - before, 3u);
    EXPECT_FALSE(s.failed());
    // Numbered keys go to the records kept only.
    auto n = ks_model();
    auto no = ks_options(n, "");
    no.dlq_policy = DlqPolicy::Drop;
    no.max_record_bytes = 10;
    KinesisSink numbered(std::move(no));
    numbered.open();
    numbered.on_data(ks_batch({"a", std::string(10, 'x'), "b"}));
    numbered.flush();
    EXPECT_EQ(n->stored_pairs(), (KinesisSinkModel::Sent{{"a", "0"}, {"b", "1"}}));
}

// ---- limits -------------------------------------------------------------------------------

// A record's data and its key's UTF-8 bytes count against the record size.
TEST(KinesisSinkLimits, ARecordAtTheLimitIsSentAndOneByteMoreIsRefused) {
    auto m = ks_model();
    KinesisSink s(ks_options(m));
    s.open();
    // A two-byte key, so data of 1,048,574 bytes makes exactly 1 MiB.
    const std::string head = "{\"k\":\"\xC3\xA9\",\"p\":\"";
    const std::string tail = "\"}";
    const auto pad = kKinesisDefaultMaxRecordBytes - 2 - head.size() - tail.size();
    const std::string at_limit = head + std::string(pad, 'x') + tail;
    s.on_data(ks_batch({at_limit}));
    s.on_barrier(ks_barrier(1));
    ASSERT_EQ(m->stored().size(), 1u);
    EXPECT_EQ(m->stored()[0].data.size() + m->stored()[0].key.size(),
              kKinesisDefaultMaxRecordBytes);
    const std::string over = head + std::string(pad + 1, 'x') + tail;
    const auto why = ks_thrown([&] { s.on_data(ks_batch({ks_record(1, "\"a\""), over})); });
    EXPECT_NE(why.find("record 2 of a batch of 2 is 1,048,577 bytes with its 2-byte partition key"),
              std::string::npos)
        << why;
    EXPECT_NE(why.find("take at most 1,048,576 (max_record_bytes)"), std::string::npos) << why;
    EXPECT_TRUE(s.failed());
    EXPECT_EQ(m->calls().size(), 1u);
    // Numbered keys count too: "0" is one byte.
    KinesisSink numbered(ks_options(m, ""));
    numbered.open();
    numbered.on_data(ks_batch({std::string(kKinesisDefaultMaxRecordBytes - 1, 'y')}));
    numbered.flush();
    EXPECT_EQ(m->stored().back().key, "0");
    EXPECT_NE(ks_thrown([&] {
                  numbered.on_data(ks_batch({std::string(kKinesisDefaultMaxRecordBytes, 'y')}));
              }).find("with its 1-byte partition key"),
              std::string::npos);
}

// Every request stays within batch_records and max_request_bytes of data and
// keys: the buffer is flushed before a record that would push it over is added.
TEST(KinesisSinkLimits, ARequestNeverOutgrowsItsLimits) {
    auto m = ks_model();
    KinesisSink s(ks_options(m, ""));
    s.open();
    std::vector<std::string> records;
    for (int i = 0; i < 12; ++i) {
        records.push_back(std::string(1000000, static_cast<char>('a' + i)));
    }
    for (int i = 0; i < 1200; ++i) {
        records.push_back("small-" + std::to_string(i));
    }
    s.on_data(ks_batch(records));
    s.on_barrier(ks_barrier(1));
    ASSERT_EQ(m->stored().size(), records.size());
    for (const auto& call : m->calls()) {
        std::size_t bytes = 0;
        for (const auto& [data, key] : call) {
            bytes += data.size() + key.size();
        }
        EXPECT_LE(call.size(), 500u);
        EXPECT_LE(bytes, kKinesisDefaultRequestBytes);
    }
    // Records of 1,000,000 bytes go five to a 5 MiB request.
    EXPECT_EQ(m->calls()[0].size(), 5u);
    // Exactly at the bound one request; a byte more, two.
    auto n = ks_model();
    auto o = ks_options(n, "");
    o.max_record_bytes = 1000;
    o.max_request_bytes = 2999;
    KinesisSink tight(std::move(o));
    tight.open();
    // Numbered keys "0", "1" and "2" add a byte each: 1000 + 1000 + 999 = 2999.
    tight.on_data(ks_batch({std::string(999, 'a'), std::string(999, 'b'), std::string(998, 'c')}));
    tight.flush();
    ASSERT_EQ(n->calls().size(), 1u);
    // 1000 + 1000 + 1000 = 3000: the third goes in a request of its own.
    tight.on_data(ks_batch({std::string(999, 'd'), std::string(999, 'e'), std::string(999, 'f')}));
    tight.flush();
    const auto tight_calls = n->calls();
    ASSERT_EQ(tight_calls.size(), 3u);
    EXPECT_EQ(tight_calls[1].size(), 2u);
    EXPECT_EQ(tight_calls[2].size(), 1u);
    EXPECT_EQ(tight_calls[2][0], (std::pair<std::string, std::string>{std::string(999, 'f'), "5"}));
    // batch_records splits by count.
    auto q = ks_model();
    auto qo = ks_options(q);
    qo.batch_records = 3;
    KinesisSink three(std::move(qo));
    three.open();
    three.on_data(ks_batch(ks_numbered(0, 7)));
    three.on_barrier(ks_barrier(1));
    const auto calls = q->calls();
    ASSERT_EQ(calls.size(), 3u);
    EXPECT_EQ(calls[0].size(), 3u);
    EXPECT_EQ(calls[2].size(), 1u);
}

// Records over 1 MiB need the stream raised and max_record_bytes with it; a sink
// set above the stream is refused by the service and fails at once.
TEST(KinesisSinkLimits, LargeRecordsNeedTheStreamRaised) {
    auto raised = ks_model(kKinesisMaxRecordBytes);
    auto o = ks_options(raised, "");
    o.max_record_bytes = kKinesisMaxRecordBytes;
    o.max_request_bytes = kKinesisMaxRequestBytes;
    KinesisSink s(std::move(o));
    s.open();
    s.on_data(ks_batch({std::string(kKinesisMaxRecordBytes - 1, 'z')}));
    s.flush();
    ASSERT_EQ(raised->stored().size(), 1u);
    EXPECT_EQ(raised->stored()[0].data.size() + raised->stored()[0].key.size(),
              kKinesisMaxRecordBytes);
    auto plain = ks_model();
    auto po = ks_options(plain, "");
    po.max_record_bytes = 2 * 1024 * 1024;
    po.max_request_bytes = kKinesisMaxRequestBytes;
    KinesisSink over(std::move(po));
    over.open();
    over.on_data(ks_batch({std::string(1500 * 1024, 'z')}));
    const auto why = ks_thrown([&] { over.flush(); });
    EXPECT_NE(why.find("PutRecords was refused: InvalidArgumentException"), std::string::npos)
        << why;
    EXPECT_NE(why.find("MaxRecordSizeInKiB"), std::string::npos) << why;
    EXPECT_EQ(plain->calls().size(), 1u);
}

// ---- partial acceptance and retries -------------------------------------------------------

TEST(KinesisSinkWrites, OnlyTheFailedRecordsAreSentAgainWithTheirKeys) {
    auto m = ks_model();
    m->set_planner([](std::size_t call, const auto& records) {
        return call == 0 ? KinesisSinkModel::failing(records.size(), {1, 3})
                         : KinesisSinkModel::Plan{};
    });
    KinesisSink s(ks_options(m));
    s.open();
    const auto records = ks_numbered(0, 5);
    s.on_data(ks_batch(records));
    s.on_barrier(ks_barrier(1));
    const auto calls = m->calls();
    ASSERT_EQ(calls.size(), 2u);
    EXPECT_EQ(calls[1], (KinesisSinkModel::Sent{{records[1], "key-1"}, {records[3], "key-3"}}));
    // Each record stored once: the stored ones were not sent again.
    EXPECT_EQ(m->stored_pairs(),
              (KinesisSinkModel::Sent{{records[0], "key-0"},
                                      {records[2], "key-2"},
                                      {records[4], "key-4"},
                                      {records[1], "key-1"},
                                      {records[3], "key-3"}}));
}

TEST(KinesisSinkWrites, FailedRecordsAreRetriedWhileTheyShrink) {
    auto m = ks_model();
    m->set_planner([](std::size_t call, const auto& records) {
        std::set<std::size_t> all;
        for (std::size_t i = 0; i < records.size(); ++i) {
            all.insert(i);
        }
        switch (call) {
            case 0:
                return KinesisSinkModel::failing(records.size(), all);  // nothing stored
            case 1:
                return KinesisSinkModel::failing(
                    records.size(), {0, 1, 2, 3, 4, 5}, "InternalFailure");
            case 2:
                return KinesisSinkModel::failing(records.size(), {0, 1});
            default:
                return KinesisSinkModel::Plan{};
        }
    });
    auto o = ks_options(m);
    o.max_retries = 1;
    KinesisSink s(std::move(o));
    s.open();
    s.on_data(ks_batch(ks_numbered(0, 10)));
    s.on_barrier(ks_barrier(1));
    EXPECT_EQ(m->calls().size(), 4u);
    auto stored = m->stored_pairs();
    std::sort(stored.begin(), stored.end());
    KinesisSinkModel::Sent want;
    for (int i = 0; i < 10; ++i) {
        want.emplace_back(ks_record(i, "\"key-" + std::to_string(i) + "\""),
                          "key-" + std::to_string(i));
    }
    std::sort(want.begin(), want.end());
    EXPECT_EQ(stored, want);
}

TEST(KinesisSinkWrites, AttemptsThatStoreNothingAreBounded) {
    auto m = ks_model();
    m->set_planner([](std::size_t, const auto& records) {
        std::set<std::size_t> all;
        for (std::size_t i = 0; i < records.size(); ++i) {
            all.insert(i);
        }
        return KinesisSinkModel::failing(records.size(), all);
    });
    auto o = ks_options(m);
    o.max_retries = 3;
    KinesisSink s(std::move(o));
    s.open();
    s.on_data(ks_batch({ks_record(1, "\"a\""), ks_record(2, "\"b\"")}));
    const auto why = ks_thrown([&] { s.on_barrier(ks_barrier(1)); });
    EXPECT_NE(why.find("2 of 2 record(s) of a PutRecords were still not stored after 4 attempt(s) "
                       "in a row that stored nothing (max_retries=3"),
              std::string::npos)
        << why;
    EXPECT_NE(why.find("2 of 2 failed, first with ProvisionedThroughputExceededException: Rate "
                       "exceeded for shard"),
              std::string::npos)
        << why;
    EXPECT_EQ(m->calls().size(), 4u);
    EXPECT_TRUE(s.failed());
}

TEST(KinesisSinkWrites, TheRetryBudgetBoundsTheWaiting) {
    auto m = ks_model();
    m->set_planner([](std::size_t, const auto&) {
        return KinesisSinkModel::error(
            "ProvisionedThroughputExceededException", 400, KinesisPutFault::Throttled);
    });
    auto o = ks_options(m);
    o.max_retries = 20;
    o.retry_base_backoff = 20ms;
    o.max_backoff = 40ms;
    o.retry_timeout = 150ms;
    KinesisSink s(std::move(o));
    s.open();
    s.on_data(ks_batch({ks_record(1, "\"a\"")}));
    const auto t0 = std::chrono::steady_clock::now();
    const auto why = ks_thrown([&] { s.on_barrier(ks_barrier(1)); });
    EXPECT_LT(std::chrono::steady_clock::now() - t0, 2s);
    EXPECT_NE(why.find("when the retry budget of 150 ms ran out"), std::string::npos) << why;
    EXPECT_NE(why.find("ProvisionedThroughputExceededException (HTTP 400)"), std::string::npos)
        << why;
}

TEST(KinesisSinkWrites, ThrottledAndTransientRequestsAreSentAgain) {
    auto m = ks_model();
    m->set_planner([](std::size_t call, const auto&) {
        if (call == 0) {
            return KinesisSinkModel::error(
                "ProvisionedThroughputExceededException", 400, KinesisPutFault::Throttled);
        }
        if (call == 1) {
            return KinesisSinkModel::error("InternalFailure", 500, KinesisPutFault::Transient);
        }
        if (call == 2) {
            return KinesisSinkModel::error(
                "KMSThrottlingException", 400, KinesisPutFault::Throttled);
        }
        return KinesisSinkModel::Plan{};
    });
    KinesisSink s(ks_options(m));
    s.open();
    s.on_data(ks_batch({ks_record(1, "\"a\""), ks_record(2, "\"b\"")}));
    s.on_barrier(ks_barrier(1));
    EXPECT_EQ(m->calls().size(), 4u);
    EXPECT_EQ(m->stored_pairs(),
              (KinesisSinkModel::Sent{{ks_record(1, "\"a\""), "a"}, {ks_record(2, "\"b\""), "b"}}));
}

// ---- responses that do not account for their request -----------------------------------

TEST(KinesisSinkResponses, AResponseThatDoesNotAccountForItsRequestFailsTheFlush) {
    struct Case {
        std::string name;
        KinesisPutResult answer;
        std::string expect;
    };
    const std::vector<Case> cases = {
        // Was taken as success: FailedRecordCount 0, entries missing.
        {"no entries", ks_response(0, {}), "returned 0 entries for 3 records"},
        {"short", ks_response(0, {ks_ok(0), ks_ok(1)}), "returned 2 entries for 3 records"},
        // Was an out-of-bounds read: a failed entry past the request's records.
        {"oversized",
         ks_response(1, {ks_ok(0), ks_ok(1), ks_ok(2), kKsThrottled}),
         "returned 4 entries for 3 records"},
        {"both",
         ks_response(1, {ks_ok(0), {"496009", "shardId-0", "InternalFailure", ""}, ks_ok(2)}),
         "entry 1 holds both"},
        {"neither",
         ks_response(0, {ks_ok(0), ks_ok(1), {"", "", "", ""}}),
         "entry 2 holds neither"},
        {"no sequence number",
         ks_response(0, {ks_ok(0), {"", "shardId-0", "", ""}, ks_ok(2)}),
         "entry 1 has a ShardId but no SequenceNumber"},
        {"no shard",
         ks_response(0, {ks_ok(0), ks_ok(1), {"496777", "", "", ""}}),
         "entry 2 has a SequenceNumber but no ShardId"},
        {"count missing",
         ks_response(0, {ks_ok(0), kKsThrottled, ks_ok(2)}),
         "reports FailedRecordCount 0 where 1"},
        {"repeated", ks_response(0, {ks_ok(0), ks_ok(1), ks_ok(1)}), "entry 2 repeats"},
    };
    for (const auto& c : cases) {
        auto m = ks_model();
        m->set_planner([&c](std::size_t, const auto&) {
            KinesisSinkModel::Plan p;
            p.response = c.answer;
            return p;
        });
        KinesisSink s(ks_options(m));
        s.open();
        s.on_data(ks_batch({ks_record(1, "\"a\""), ks_record(2, "\"b\""), ks_record(3, "\"c\"")}));
        const auto why = ks_thrown([&] { s.on_barrier(ks_barrier(1)); });
        EXPECT_NE(why.find("does not account for its request"), std::string::npos)
            << c.name << ": " << why;
        EXPECT_NE(why.find(c.expect), std::string::npos) << c.name << ": " << why;
        EXPECT_NE(why.find("a replay may store some twice"), std::string::npos) << c.name;
        EXPECT_TRUE(s.failed()) << c.name;
        EXPECT_EQ(m->calls().size(), 1u) << c.name << ": an ambiguous response is not retried";
        // The barrier is not acknowledged, now or later.
        EXPECT_NE(ks_thrown([&] { s.on_barrier(ks_barrier(2)); }), "") << c.name;
    }
}

// ---- permanent errors and lost replies -----------------------------------------------------

// Errors from the service are never a bad record: under dlq='drop' they still
// fail the sink, and nothing reaches the dead-letter path.
TEST(KinesisSinkFailures, PermanentErrorsFailAtOnceAndAreNeverDropped) {
    for (const auto& [code, hint] : std::vector<std::pair<std::string, std::string>>{
             {"ResourceNotFoundException", "Check that the stream exists"},
             {"AccessDeniedException", "kinesis:PutRecords"},
             {"UnrecognizedClientException", "default chain"},
             {"KMSDisabledException", "encryption key is not usable"},
             {"InvalidArgumentException", "MaxRecordSizeInKiB"},
             {"ValidationException", "MaxRecordSizeInKiB"}}) {
        auto m = ks_model();
        m->set_planner([&](std::size_t, const auto&) {
            return KinesisSinkModel::error(code, 400, KinesisPutFault::Permanent);
        });
        auto o = ks_options(m);
        o.dlq_policy = DlqPolicy::Drop;
        KinesisSink s(std::move(o));
        KsAttached a(s);
        s.open();
        s.on_data(ks_batch({ks_record(1, "\"a\"")}));
        const auto why = ks_thrown([&] { s.on_barrier(ks_barrier(1)); });
        EXPECT_NE(why.find("PutRecords was refused: " + code), std::string::npos) << why;
        EXPECT_NE(why.find(hint), std::string::npos) << code << ": " << why;
        EXPECT_EQ(m->calls().size(), 1u) << code;
        EXPECT_TRUE(a.dead.got().empty()) << code;
        EXPECT_TRUE(s.failed()) << code;
    }
}

// A reply lost after Kinesis stored the request: the records are sent again and
// stored twice, which at-least-once allows; none is lost.
TEST(KinesisSinkFailures, AReplyLostAfterAcceptanceIsSentAgain) {
    auto m = ks_model();
    m->set_planner([](std::size_t call, const auto&) {
        return call == 0 ? KinesisSinkModel::error(
                               "NoResponse", 0, KinesisPutFault::Transient, /*stored=*/true)
                         : KinesisSinkModel::Plan{};
    });
    KinesisSink s(ks_options(m));
    s.open();
    s.on_data(ks_batch({ks_record(1, "\"a\""), ks_record(2, "\"b\"")}));
    s.on_barrier(ks_barrier(1));
    EXPECT_EQ(m->calls().size(), 2u);
    EXPECT_EQ(m->stored_pairs(),
              (KinesisSinkModel::Sent{{ks_record(1, "\"a\""), "a"},
                                      {ks_record(2, "\"b\""), "b"},
                                      {ks_record(1, "\"a\""), "a"},
                                      {ks_record(2, "\"b\""), "b"}}));
    EXPECT_FALSE(s.failed());
}

// ---- cancellation -------------------------------------------------------------------------

TEST(KinesisSinkCancel, ACancelDuringARetryWaitStopsAtOnce) {
    auto m = ks_model();
    m->set_planner([](std::size_t, const auto&) {
        return KinesisSinkModel::error(
            "ProvisionedThroughputExceededException", 400, KinesisPutFault::Throttled);
    });
    auto o = ks_options(m);
    o.retry_base_backoff = 5000ms;
    o.max_backoff = 5000ms;
    KinesisSink s(std::move(o));
    KsAttached a(s);
    s.open();
    s.on_data(ks_batch({ks_record(1, "\"a\"")}));
    auto done = std::async(std::launch::async,
                           [&] { return ks_thrown([&] { s.on_barrier(ks_barrier(1)); }); });
    while (m->calls().empty()) {
        std::this_thread::sleep_for(5ms);
    }
    const auto t0 = std::chrono::steady_clock::now();
    a.cancel->store(true);
    ASSERT_EQ(done.wait_for(2s), std::future_status::ready);
    EXPECT_LT(std::chrono::steady_clock::now() - t0, 1s);
    EXPECT_NE(done.get().find("cancelled while waiting to retry a PutRecords"), std::string::npos);
    EXPECT_TRUE(s.failed());
}

TEST(KinesisSinkCancel, ACancelDuringACallEndsIt) {
    auto m = ks_model();
    m->set_planner([](std::size_t, const auto&) {
        KinesisSinkModel::Plan p;
        p.hang = true;
        return p;
    });
    KinesisSink s(ks_options(m));
    KsAttached a(s);
    s.open();
    s.on_data(ks_batch({ks_record(1, "\"a\"")}));
    auto done = std::async(std::launch::async,
                           [&] { return ks_thrown([&] { s.on_barrier(ks_barrier(1)); }); });
    while (!m->hanging()) {
        std::this_thread::sleep_for(5ms);
    }
    // The barrier has not returned while its records are unresolved.
    EXPECT_EQ(done.wait_for(200ms), std::future_status::timeout);
    a.cancel->store(true);
    ASSERT_EQ(done.wait_for(2s), std::future_status::ready);
    const auto why = done.get();
    EXPECT_NE(why.find("cancelled while a PutRecords of 1 record(s) was in flight"),
              std::string::npos)
        << why;
    EXPECT_TRUE(m->aborted());
    EXPECT_TRUE(s.failed());
}

// ---- the lifecycle ------------------------------------------------------------------------

TEST(KinesisSinkLifecycle, ABarrierReturnsOnlyOnceEveryRecordBeforeItIsStored) {
    auto m = ks_model();
    m->set_planner([](std::size_t call, const auto& records) {
        return call < 2 ? KinesisSinkModel::failing(records.size(), {0}) : KinesisSinkModel::Plan{};
    });
    KinesisSink s(ks_options(m));
    s.open();
    s.on_data(ks_batch(ks_numbered(0, 3)));
    EXPECT_TRUE(m->calls().empty());  // buffered, below batch_records
    s.on_barrier(ks_barrier(1));
    EXPECT_EQ(m->stored().size(), 3u);
    EXPECT_EQ(s.pending_records(), 0u);
}

TEST(KinesisSinkLifecycle, AFailedSinkRefusesEveryLaterBarrier) {
    auto m = ks_model();
    m->set_planner([](std::size_t, const auto&) {
        return KinesisSinkModel::error(
            "ResourceNotFoundException", 400, KinesisPutFault::Permanent);
    });
    auto o = ks_options(m);
    o.batch_records = 1;
    KinesisSink s(std::move(o));
    s.open();
    EXPECT_NE(ks_thrown([&] { s.on_data(ks_batch(ks_numbered(0, 2))); }), "");
    EXPECT_TRUE(s.failed());
    for (std::uint64_t id = 1; id <= 3; ++id) {
        EXPECT_NE(ks_thrown([&] {
                      s.on_barrier(ks_barrier(id));
                  }).find("failed earlier and dropped records"),
                  std::string::npos)
            << "barrier " << id;
    }
    EXPECT_NE(ks_thrown([&] { s.flush(); }).find("failed earlier"), std::string::npos);
    EXPECT_NE(ks_thrown([&] { s.on_data(ks_batch(ks_numbered(2, 3))); }).find("failed earlier"),
              std::string::npos);
    EXPECT_NO_THROW(s.close());
    EXPECT_EQ(m->calls().size(), 1u);
}

// The end of input: the runtime flushes, then closes. The flush sends what is
// buffered; close sends nothing, whether or not a flush came first.
TEST(KinesisSinkLifecycle, EndOfInputSendsEverythingAndCloseSendsNothing) {
    auto m = ks_model();
    KinesisSink s(ks_options(m));
    s.open();
    s.on_data(ks_batch(ks_numbered(0, 2)));
    s.flush();
    s.close();
    EXPECT_EQ(m->stored().size(), 2u);
    KinesisSink unflushed(ks_options(m));
    unflushed.open();
    unflushed.on_data(ks_batch(ks_numbered(2, 3)));
    unflushed.close();
    EXPECT_EQ(m->calls().size(), 1u);
}

// As a restart does: open again and replay what the last checkpoint did not
// cover. Records the failed instance had stored come back as duplicates; none
// is lost.
TEST(KinesisSinkLifecycle, OpenStartsAfreshAfterAFailureAndAReplayLosesNothing) {
    auto m = ks_model();
    m->set_planner([](std::size_t call, const auto&) {
        if (call == 1) {
            // c and d stored, but the response names one record twice.
            KinesisSinkModel::Plan p;
            p.response = ks_response(0, {ks_ok(7), ks_ok(7)});
            return p;
        }
        return KinesisSinkModel::Plan{};
    });
    KinesisSink s(ks_options(m));
    s.open();
    const auto records = ks_numbered(0, 4);
    s.on_data(ks_batch({records[0], records[1]}));
    s.on_barrier(ks_barrier(1));  // a and b covered by checkpoint 1
    s.on_data(ks_batch({records[2], records[3]}));
    EXPECT_NE(ks_thrown([&] { s.on_barrier(ks_barrier(2)); }), "");
    EXPECT_TRUE(s.failed());
    s.close();
    s.open();
    EXPECT_FALSE(s.failed());
    EXPECT_EQ(s.pending_records(), 0u);
    s.on_data(ks_batch({records[2], records[3]}));
    s.on_barrier(ks_barrier(3));
    std::multiset<std::string> stored;
    for (const auto& r : m->stored()) {
        stored.insert(r.data);
    }
    for (const auto& r : records) {
        EXPECT_GE(stored.count(r), 1u) << r;
    }
    EXPECT_EQ(stored.count(records[0]), 1u);
    EXPECT_EQ(stored.count(records[2]), 2u) << "stored before the failure and by the replay";
}

TEST(KinesisSinkLifecycle, LingerFlushesAnOldPartialBatch) {
    auto m = ks_model();
    auto o = ks_options(m);
    o.max_age = 30ms;
    KinesisSink s(std::move(o));
    s.open();
    s.on_data(ks_batch(ks_numbered(0, 1)));
    EXPECT_TRUE(m->calls().empty());
    std::this_thread::sleep_for(40ms);
    s.on_data(ks_batch(ks_numbered(1, 2)));
    EXPECT_EQ(m->stored().size(), 2u);
}
