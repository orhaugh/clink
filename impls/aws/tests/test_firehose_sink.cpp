// The Firehose sink's write path, deterministically: the production FirehoseSink
// against a model of PutRecordBatch (firehose_model.hpp), and the pure parts it
// rests on (the request accounting, checked against the SDK's own JSON, and the
// response judge). The model keeps every record it accepts, duplicates
// included, so the cases check what the delivery stream would pass on, not what
// the sink believes it sent. Nothing here talks to a service.

#include <atomic>
#include <chrono>
#include <future>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include <aws/firehose/model/PutRecordBatchRequest.h>
#include <aws/firehose/model/Record.h>
#include <gtest/gtest.h>

#include "clink/aws/firehose_sink.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/runtime/runtime_context.hpp"

#include "firehose_model.hpp"

using namespace std::chrono_literals;
using clink::Batch;
using clink::CheckpointBarrier;
using clink::aws::FirehoseBatchResult;
using clink::aws::FirehoseFault;
using clink::aws::FirehoseResponseEntry;
using clink::aws::FirehoseSink;
using clink::aws::FirehoseSinkOptions;
using clink::aws::judge_firehose_response;
using clink::aws::kFirehoseMaxRecordBytes;
using clink::aws::kFirehoseMaxRequestBytes;
using clink::aws::test::FirehoseModel;

namespace {

constexpr char kStream[] = "clicks";

std::shared_ptr<FirehoseModel> fh_model() {
    return std::make_shared<FirehoseModel>(kStream);
}

FirehoseSinkOptions fh_options(const std::shared_ptr<FirehoseModel>& m) {
    FirehoseSinkOptions o;
    o.delivery_stream = kStream;
    o.service = m;
    o.retry_base_backoff = 1ms;
    o.max_backoff = 8ms;
    o.retry_timeout = 30s;
    return o;
}

Batch<std::string> fh_batch(const std::vector<std::string>& records) {
    Batch<std::string> b;
    for (const auto& r : records) {
        b.emplace(r);
    }
    return b;
}

CheckpointBarrier fh_barrier(std::uint64_t id) {
    return CheckpointBarrier{clink::CheckpointId{id}};
}

template <typename F>
std::string fh_thrown(F&& f) {
    try {
        f();
    } catch (const std::exception& e) {
        return e.what();
    }
    return "";
}

std::vector<std::string> numbered(int from, int to) {
    std::vector<std::string> out;
    for (int i = from; i < to; ++i) {
        out.push_back("r" + std::to_string(i));
    }
    return out;
}

std::vector<std::string> with_newlines(std::vector<std::string> records) {
    for (auto& r : records) {
        r += "\n";
    }
    return records;
}

// The SDK's JSON for a request of these records, as the sink's service builds it.
std::size_t sdk_request_bytes(const std::vector<std::string>& records) {
    Aws::Firehose::Model::PutRecordBatchRequest req;
    req.SetDeliveryStreamName(kStream);
    for (const auto& r : records) {
        Aws::Firehose::Model::Record rec;
        rec.SetData(
            Aws::Utils::ByteBuffer(reinterpret_cast<const unsigned char*>(r.data()), r.size()));
        req.AddRecords(rec);
    }
    return req.SerializePayload().size();
}

// The sink's accounting for a request carrying these records.
std::size_t counted_request_bytes(const std::vector<std::string>& records) {
    std::size_t n = clink::aws::firehose_request_envelope_bytes(std::string{kStream}.size());
    for (const auto& r : records) {
        n += clink::aws::firehose_record_request_bytes(r.size());
    }
    return n;
}

// A sink attached to a runtime context whose cancel signal the case controls.
struct FhAttached {
    explicit FhAttached(FirehoseSink& s) : sink(s) {
        ctx = std::make_unique<clink::RuntimeContext>(
            clink::OperatorId{4646}, "firehose_sink", nullptr, nullptr);
        ctx->set_cancel_signal(clink::CancelSignal{cancel, nullptr});
        sink.attach_runtime(ctx.get());
    }
    ~FhAttached() { sink.attach_runtime(nullptr); }
    FhAttached(const FhAttached&) = delete;
    FhAttached& operator=(const FhAttached&) = delete;
    FirehoseSink& sink;
    std::shared_ptr<std::atomic<bool>> cancel = std::make_shared<std::atomic<bool>>(false);
    std::unique_ptr<clink::RuntimeContext> ctx;
};

FirehoseBatchResult response(std::int64_t failed_put_count,
                             std::vector<FirehoseResponseEntry> entries) {
    FirehoseBatchResult r;
    r.failed_put_count = failed_put_count;
    r.entries = std::move(entries);
    return r;
}

const FirehoseResponseEntry kOk{"rid", "", ""};
const FirehoseResponseEntry kFailed{"", "ServiceUnavailableException", "Slow down."};

}  // namespace

// ---- limits -------------------------------------------------------------------------

// The request accounting is never below the JSON the SDK sends: base64 data in an
// indented object per record.
TEST(FirehoseLimits, TheRequestCountIsNeverBelowTheSdksJson) {
    std::string binary;
    for (int i = 0; i < 256; ++i) {
        binary.push_back(static_cast<char>(i));
    }
    for (const std::size_t size : {std::size_t{0},
                                   std::size_t{1},
                                   std::size_t{2},
                                   std::size_t{3},
                                   std::size_t{4},
                                   std::size_t{100},
                                   std::size_t{65536},
                                   kFirehoseMaxRecordBytes}) {
        const std::vector<std::string> one = {std::string(size, 'x')};
        const std::vector<std::string> three = {
            std::string(size, 'x'), binary, std::string(size, '\0')};
        EXPECT_LE(sdk_request_bytes(one), counted_request_bytes(one)) << size;
        EXPECT_LE(sdk_request_bytes(three), counted_request_bytes(three)) << size;
        // Each record's own share, as the difference between requests of 2 and 1.
        const std::vector<std::string> two = {one[0], one[0]};
        EXPECT_LE(sdk_request_bytes(two) - sdk_request_bytes(one),
                  clink::aws::firehose_record_request_bytes(size))
            << size;
    }
}

TEST(FirehoseLimits, TheLargestRecordFitsAloneAndThreeFillARequest) {
    const auto name = std::string{kStream}.size();
    EXPECT_LE(clink::aws::firehose_min_request_bytes(name), kFirehoseMaxRequestBytes);
    const auto record = clink::aws::firehose_record_request_bytes(kFirehoseMaxRecordBytes);
    const auto envelope = clink::aws::firehose_request_envelope_bytes(name);
    // Counted as sent, three records of 1,000 KiB fit 4 MiB and four do not.
    EXPECT_LE(envelope + 3 * record, kFirehoseMaxRequestBytes);
    EXPECT_GT(envelope + 4 * record, kFirehoseMaxRequestBytes);
    EXPECT_EQ(clink::aws::firehose_record_request_bytes(0), 32u);
    EXPECT_EQ(clink::aws::firehose_record_request_bytes(3), 36u);
    EXPECT_EQ(clink::aws::firehose_record_request_bytes(4), 40u);
}

// ---- the response judge ----------------------------------------------------------------

TEST(FirehoseResponse, AValidResponseAccountsForEveryRecord) {
    auto v = judge_firehose_response(3, response(0, {kOk, kOk, kOk}));
    EXPECT_TRUE(v.valid) << v.why;
    EXPECT_TRUE(v.failed.empty());
    v = judge_firehose_response(4, response(2, {kOk, kFailed, kOk, kFailed}));
    EXPECT_TRUE(v.valid) << v.why;
    EXPECT_EQ(v.failed, (std::vector<std::size_t>{1, 3}));
    v = judge_firehose_response(2, response(2, {kFailed, kFailed}));
    EXPECT_TRUE(v.valid) << v.why;
    EXPECT_EQ(v.failed.size(), 2u);
}

TEST(FirehoseResponse, AnyOtherResponseIsNeverASuccess) {
    const FirehoseResponseEntry both{"rid", "InternalFailure", "x"};
    const FirehoseResponseEntry neither{"", "", ""};
    const std::vector<std::tuple<std::string, std::size_t, FirehoseBatchResult, std::string>>
        cases = {
            // The false success: FailedPutCount 0 with entries missing.
            {"no entries", 3, response(0, {}), "returned 0 entries for 3 records"},
            {"short", 3, response(0, {kOk, kOk}), "returned 2 entries for 3 records"},
            // An error entry past the request's records, which an unchecked index read.
            {"oversized", 2, response(1, {kOk, kOk, kFailed}), "returned 3 entries for 2 records"},
            {"both", 2, response(1, {kOk, both}), "entry 1 holds both a RecordId and an ErrorCode"},
            {"neither", 2, response(0, {neither, kOk}), "entry 0 holds neither"},
            {"count low",
             2,
             response(0, {kOk, kFailed}),
             "reports FailedPutCount 0 where 1 entries failed"},
            {"count high",
             2,
             response(2, {kOk, kFailed}),
             "reports FailedPutCount 2 where 1 entries failed"},
            {"count negative", 1, response(-1, {kOk}), "reports FailedPutCount -1"},
        };
    for (const auto& [name, sent, r, why] : cases) {
        const auto v = judge_firehose_response(sent, r);
        EXPECT_FALSE(v.valid) << name;
        EXPECT_TRUE(v.failed.empty()) << name;
        EXPECT_NE(v.why.find(why), std::string::npos) << name << ": " << v.why;
    }
}

// ---- options ----------------------------------------------------------------------------

TEST(FirehoseSinkConfig, RefusesOptionsOutsideTheirRanges) {
    auto m = fh_model();
    const auto refused = [&](auto edit) {
        auto o = fh_options(m);
        edit(o);
        return fh_thrown([&] { FirehoseSink s(std::move(o)); });
    };
    EXPECT_NE(
        refused([](auto& o) { o.delivery_stream.clear(); }).find("'delivery_stream' is required"),
        std::string::npos);
    EXPECT_NE(refused([](auto& o) { o.delivery_stream = "a b"; }).find("1 to 64 characters"),
              std::string::npos);
    EXPECT_NE(refused([](auto& o) { o.delivery_stream = std::string(65, 's'); }).find("1 to 64"),
              std::string::npos);
    EXPECT_EQ(refused([](auto& o) { o.delivery_stream = std::string(64, 's'); }), "");
    EXPECT_NE(refused([](auto& o) {
                  o.delimiter = std::string(kFirehoseMaxRecordBytes + 1, ';');
              }).find("'delimiter' is longer than a whole Firehose record"),
              std::string::npos);
    EXPECT_NE(refused([](auto& o) { o.batch_records = 0; }).find("1 to 500"), std::string::npos);
    EXPECT_NE(refused([](auto& o) { o.batch_records = 501; }).find("1 to 500"), std::string::npos);
    EXPECT_NE(refused([](auto& o) {
                  o.max_bytes = kFirehoseMaxRequestBytes + 1;
              }).find("'max_bytes' must be"),
              std::string::npos);
    EXPECT_NE(refused([](auto& o) { o.max_bytes = 1024 * 1024; }).find("the largest record alone"),
              std::string::npos);
    EXPECT_EQ(refused([](auto& o) {
                  o.max_bytes = clink::aws::firehose_min_request_bytes(std::string{kStream}.size());
              }),
              "");
    EXPECT_NE(refused([](auto& o) { o.max_retries = -1; }).find("0 to 20"), std::string::npos);
    EXPECT_NE(refused([](auto& o) { o.max_retries = 21; }).find("0 to 20"), std::string::npos);
    EXPECT_NE(refused([](auto& o) { o.retry_timeout = 0ms; }).find("retry_timeout_ms"),
              std::string::npos);
    EXPECT_NE(refused([](auto& o) { o.max_age = -1ms; }).find("linger_ms"), std::string::npos);
    EXPECT_NE(refused([](auto& o) { o.client.region = ""; }).find("'region' is empty"),
              std::string::npos);
    EXPECT_NE(
        refused([](auto& o) { o.client.connect_timeout_ms = 600001; }).find("connect_timeout_ms"),
        std::string::npos);
    const auto why =
        refused([](auto& o) { o.client.endpoint_override = "https://AKIAEXAMPLE:hunter2@host"; });
    EXPECT_NE(why.find("without user information"), std::string::npos) << why;
    EXPECT_EQ(why.find("hunter2"), std::string::npos) << why;
    EXPECT_EQ(refused([](auto& o) { o.client.endpoint_override = "http://127.0.0.1:4566"; }), "");
    EXPECT_TRUE(m->calls().empty());
}

TEST(FirehoseSinkConfig, TheFactoryRangeChecksItsNumbers) {
    const auto* f = clink::cluster::OperatorRegistry::default_instance().find_sink(
        "firehose_sink", std::string{clink::cluster::kChannelString});
    ASSERT_NE(f, nullptr);
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = {{"delivery_stream", "clicks"}};
    EXPECT_NO_THROW((void)f->build(ctx));
    for (const auto& [key, value, rule] :
         std::vector<std::tuple<std::string, std::string, std::string>>{
             {"batch_records", "501", "'batch_records' must be 1 to 500"},
             {"batch_records", "0", "'batch_records' must be 1 to 500"},
             {"max_retries", "-1", "'max_retries' must be 0 to 20"},
             {"connect_timeout_ms", "4294967396", "'connect_timeout_ms' must be 1 to 600000"},
             {"request_timeout_ms", "0", "'request_timeout_ms' must be 1 to 600000"},
             {"retry_timeout_ms", "3600001", "'retry_timeout_ms' must be 1 to 3600000"},
             {"linger_ms", "-5", "'linger_ms' must be 0 to 3600000"}}) {
        auto c = ctx;
        c.params[key] = value;
        EXPECT_NE(fh_thrown([&] { (void)f->build(c); }).find(rule), std::string::npos)
            << key << "=" << value;
    }
}

TEST(FirehoseSinkConfig, ErrorsNameTheFixAndNoCredentials) {
    auto m = fh_model();
    m->set_planner([](std::size_t, const auto&) {
        return FirehoseModel::error(
            "AccessDeniedException",
            400,
            FirehoseFault::Permanent,
            false,
            "User: arn:aws:iam::123456789012:user/u with key AKIAABCDEFGHIJKLMNOP is not "
            "authorized; "
            "Credential=AKIAABCDEFGHIJKLMNOP/20261106/eu-west-2/firehose/aws4_request, "
            "Signature=0123456789abcdef");
    });
    FirehoseSink s(fh_options(m));
    s.open();
    s.on_data(fh_batch({"a"}));
    const auto why = fh_thrown([&] { s.on_barrier(fh_barrier(1)); });
    EXPECT_NE(why.find("PutRecordBatch was refused: AccessDeniedException (HTTP 400)"),
              std::string::npos)
        << why;
    EXPECT_NE(why.find("firehose:PutRecordBatch"), std::string::npos) << why;
    EXPECT_EQ(why.find("AKIAABCDEFGHIJKLMNOP"), std::string::npos) << why;
    EXPECT_EQ(why.find("0123456789abcdef"), std::string::npos) << why;
}

// ---- records ----------------------------------------------------------------------------

TEST(FirehoseSinkRecords, ARecordAtTheLimitIsSentAndOneByteMoreIsRefused) {
    auto m = fh_model();
    FirehoseSink s(fh_options(m));
    s.open();
    // 1,023,999 bytes and the newline: exactly 1,024,000.
    s.on_data(fh_batch({std::string(kFirehoseMaxRecordBytes - 1, 'x')}));
    s.on_barrier(fh_barrier(1));
    ASSERT_EQ(m->accepted().size(), 1u);
    EXPECT_EQ(m->accepted()[0].size(), kFirehoseMaxRecordBytes);
    const auto why = fh_thrown(
        [&] { s.on_data(fh_batch({"small", std::string(kFirehoseMaxRecordBytes, 'x')})); });
    EXPECT_NE(why.find("record 2 of a batch of 2 is 1,024,001 bytes with its 1-byte delimiter"),
              std::string::npos)
        << why;
    EXPECT_NE(why.find("at most 1,024,000 bytes (1,000 KiB) per record before base64 encoding"),
              std::string::npos)
        << why;
    EXPECT_NE(why.find("Nothing of that batch was buffered or sent"), std::string::npos) << why;
    EXPECT_TRUE(s.failed());
    EXPECT_EQ(m->calls().size(), 1u);
    // Without a delimiter the whole 1,024,000 bytes are the record's.
    auto o = fh_options(m);
    o.delimiter = "";
    FirehoseSink bare(std::move(o));
    bare.open();
    bare.on_data(fh_batch({std::string(kFirehoseMaxRecordBytes, 'y')}));
    bare.on_barrier(fh_barrier(1));
    EXPECT_EQ(m->accepted().back().size(), kFirehoseMaxRecordBytes);
}

TEST(FirehoseSinkRecords, PayloadsAreSentAsTheyAre) {
    auto m = fh_model();
    FirehoseSink s(fh_options(m));
    s.open();
    std::string binary;
    for (int i = 0; i < 256; ++i) {
        binary.push_back(static_cast<char>(i));
    }
    const std::string not_json = "{\"half\": ";
    s.on_data(fh_batch({"", binary, not_json, std::string("a\0b", 3)}));
    s.on_barrier(fh_barrier(1));
    EXPECT_EQ(
        m->accepted(),
        (std::vector<std::string>{"\n", binary + "\n", not_json + "\n", std::string("a\0b\n", 4)}));
    // With no delimiter an empty record is an empty Data.
    auto o = fh_options(m);
    o.delimiter = "";
    FirehoseSink bare(std::move(o));
    bare.open();
    bare.on_data(fh_batch({""}));
    bare.on_barrier(fh_barrier(1));
    EXPECT_EQ(m->accepted().back(), "");
}

TEST(FirehoseSinkRecords, TheDelimiterIsAppendedAndCounted) {
    auto m = fh_model();
    auto o = fh_options(m);
    o.delimiter = "\r\n";
    FirehoseSink s(std::move(o));
    s.open();
    s.on_data(fh_batch({"a", std::string(kFirehoseMaxRecordBytes - 2, 'x')}));
    s.on_barrier(fh_barrier(1));
    ASSERT_EQ(m->accepted().size(), 2u);
    EXPECT_EQ(m->accepted()[0], "a\r\n");
    EXPECT_EQ(m->accepted()[1].size(), kFirehoseMaxRecordBytes);
    EXPECT_NE(fh_thrown([&] {
                  s.on_data(fh_batch({std::string(kFirehoseMaxRecordBytes - 1, 'x')}));
              }).find("with its 2-byte delimiter"),
              std::string::npos);
}

// Every request stays within 500 records and max_bytes as sent: the buffer is
// flushed before a record that would push it over is added.
TEST(FirehoseSinkRecords, ARequestNeverOutgrowsItsLimits) {
    auto m = fh_model();
    FirehoseSink s(fh_options(m));
    s.open();
    std::vector<std::string> records;
    for (int i = 0; i < 10; ++i) {
        records.push_back(std::string(1000000, static_cast<char>('a' + i)));
    }
    for (int i = 0; i < 1200; ++i) {
        records.push_back("small-" + std::to_string(i));
    }
    s.on_data(fh_batch(records));
    s.on_barrier(fh_barrier(1));
    EXPECT_EQ(m->accepted(), with_newlines(records));
    for (const auto& call : m->calls()) {
        EXPECT_LE(call.size(), 500u);
        EXPECT_LE(counted_request_bytes(call), kFirehoseMaxRequestBytes);
        EXPECT_LE(sdk_request_bytes(call), kFirehoseMaxRequestBytes);
    }
    // Ten records of 1,000,000 bytes go three to a request; the small ones fill
    // 500 a request.
    EXPECT_EQ(m->calls()[0].size(), 3u);
    auto o = fh_options(m);
    o.batch_records = 3;
    FirehoseSink three(std::move(o));
    three.open();
    const auto before = m->calls().size();
    three.on_data(fh_batch(numbered(0, 7)));
    three.on_barrier(fh_barrier(1));
    const auto calls = m->calls();
    ASSERT_EQ(calls.size(), before + 3);
    EXPECT_EQ(calls[before].size(), 3u);
    EXPECT_EQ(calls[before + 2].size(), 1u);
}

TEST(FirehoseSinkRecords, ARefusedRecordAddsNothingOfItsBatch) {
    auto m = fh_model();
    auto o = fh_options(m);
    o.batch_records = 2;
    FirehoseSink s(std::move(o));
    s.open();
    s.on_data(fh_batch({"a", "b", "c"}));  // a and b sent when the buffer filled
    ASSERT_EQ(m->accepted().size(), 2u);
    const auto why = fh_thrown(
        [&] { s.on_data(fh_batch({"d", std::string(kFirehoseMaxRecordBytes, 'x'), "e"})); });
    EXPECT_NE(why.find("record 2 of a batch of 3"), std::string::npos) << why;
    EXPECT_NE(why.find("1 buffered record(s) not yet accepted are dropped"), std::string::npos)
        << why;
    EXPECT_EQ(m->accepted(), (std::vector<std::string>{"a\n", "b\n"}));
}

// ---- partial acceptance and retries -------------------------------------------------------

TEST(FirehoseSinkWrites, OnlyTheFailedRecordsAreSentAgain) {
    auto m = fh_model();
    m->set_planner([](std::size_t call, const auto& records) {
        return call == 0 ? FirehoseModel::failing(records.size(), {1, 3}) : FirehoseModel::Plan{};
    });
    FirehoseSink s(fh_options(m));
    s.open();
    s.on_data(fh_batch(numbered(0, 5)));
    s.on_barrier(fh_barrier(1));
    const auto calls = m->calls();
    ASSERT_EQ(calls.size(), 2u);
    EXPECT_EQ(calls[1], (std::vector<std::string>{"r1\n", "r3\n"}));
    // Each record accepted once: the accepted ones were not sent again.
    EXPECT_EQ(m->accepted(), (std::vector<std::string>{"r0\n", "r2\n", "r4\n", "r1\n", "r3\n"}));
}

TEST(FirehoseSinkWrites, FailedRecordsAreRetriedWhileTheyShrink) {
    auto m = fh_model();
    m->set_planner([](std::size_t call, const auto& records) {
        std::set<std::size_t> all;
        for (std::size_t i = 0; i < records.size(); ++i) {
            all.insert(i);
        }
        switch (call) {
            case 0:
                return FirehoseModel::failing(records.size(), all);  // nothing accepted
            case 1:
                return FirehoseModel::failing(records.size(), {0, 1, 2, 3, 4, 5});
            case 2:
                return FirehoseModel::failing(records.size(), {0, 1});
            default:
                return FirehoseModel::Plan{};
        }
    });
    auto o = fh_options(m);
    o.max_retries = 1;
    FirehoseSink s(std::move(o));
    s.open();
    s.on_data(fh_batch(numbered(0, 10)));
    s.on_barrier(fh_barrier(1));
    EXPECT_EQ(m->calls().size(), 4u);
    auto accepted = m->accepted();
    std::sort(accepted.begin(), accepted.end());
    auto want = with_newlines(numbered(0, 10));
    std::sort(want.begin(), want.end());
    EXPECT_EQ(accepted, want);
}

TEST(FirehoseSinkWrites, AttemptsThatAcceptNothingAreBounded) {
    auto m = fh_model();
    m->set_planner([](std::size_t, const auto& records) {
        std::set<std::size_t> all;
        for (std::size_t i = 0; i < records.size(); ++i) {
            all.insert(i);
        }
        return FirehoseModel::failing(records.size(), all);
    });
    auto o = fh_options(m);
    o.max_retries = 3;
    FirehoseSink s(std::move(o));
    s.open();
    s.on_data(fh_batch({"a", "b"}));
    const auto why = fh_thrown([&] { s.on_barrier(fh_barrier(1)); });
    EXPECT_NE(
        why.find(
            "2 of 2 record(s) of a PutRecordBatch were still not accepted after 4 attempt(s) in "
            "a row that accepted nothing (max_retries=3"),
        std::string::npos)
        << why;
    EXPECT_NE(why.find("2 of 2 failed, first with ServiceUnavailableException: Slow down."),
              std::string::npos)
        << why;
    EXPECT_EQ(m->calls().size(), 4u);
    EXPECT_TRUE(s.failed());
}

TEST(FirehoseSinkWrites, TheRetryBudgetBoundsTheWaiting) {
    auto m = fh_model();
    m->set_planner([](std::size_t, const auto&) {
        return FirehoseModel::error("ServiceUnavailableException", 500, FirehoseFault::Throttled);
    });
    auto o = fh_options(m);
    o.max_retries = 20;
    o.retry_base_backoff = 20ms;
    o.max_backoff = 40ms;
    o.retry_timeout = 150ms;
    FirehoseSink s(std::move(o));
    s.open();
    s.on_data(fh_batch({"a"}));
    const auto t0 = std::chrono::steady_clock::now();
    const auto why = fh_thrown([&] { s.on_barrier(fh_barrier(1)); });
    EXPECT_LT(std::chrono::steady_clock::now() - t0, 2s);
    EXPECT_NE(why.find("when the retry budget of 150 ms ran out"), std::string::npos) << why;
    EXPECT_NE(why.find("ServiceUnavailableException (HTTP 500)"), std::string::npos) << why;
}

TEST(FirehoseSinkWrites, ThrottledAndTransientRequestsAreSentAgain) {
    auto m = fh_model();
    m->set_planner([](std::size_t call, const auto&) {
        if (call == 0) {
            return FirehoseModel::error(
                "ServiceUnavailableException", 500, FirehoseFault::Throttled);
        }
        if (call == 1) {
            return FirehoseModel::error("InternalFailure", 500, FirehoseFault::Transient);
        }
        return FirehoseModel::Plan{};
    });
    FirehoseSink s(fh_options(m));
    s.open();
    s.on_data(fh_batch({"a", "b"}));
    s.on_barrier(fh_barrier(1));
    EXPECT_EQ(m->calls().size(), 3u);
    EXPECT_EQ(m->accepted(), (std::vector<std::string>{"a\n", "b\n"}));
}

// ---- responses that do not account for their request -----------------------------------

TEST(FirehoseSinkResponses, AResponseThatDoesNotAccountForItsRequestFailsTheFlush) {
    struct Case {
        std::string name;
        FirehoseBatchResult answer;
        std::string expect;
    };
    const std::vector<Case> cases = {
        // Was taken as success: FailedPutCount 0, entries missing.
        {"no entries", response(0, {}), "returned 0 entries for 3 records"},
        {"short", response(0, {kOk, kOk}), "returned 2 entries for 3 records"},
        // Was an out-of-bounds read: a failed entry past the request's records.
        {"oversized", response(1, {kOk, kOk, kOk, kFailed}), "returned 4 entries for 3 records"},
        {"both", response(1, {kOk, {"rid", "InternalFailure", ""}, kOk}), "entry 1 holds both"},
        {"neither", response(0, {kOk, kOk, {"", "", ""}}), "entry 2 holds neither"},
        {"count mismatch", response(0, {kOk, kFailed, kOk}), "reports FailedPutCount 0 where 1"},
    };
    for (const auto& c : cases) {
        auto m = fh_model();
        m->set_planner([&c](std::size_t, const auto&) {
            FirehoseModel::Plan p;
            p.response = c.answer;
            return p;
        });
        FirehoseSink s(fh_options(m));
        s.open();
        s.on_data(fh_batch({"a", "b", "c"}));
        const auto why = fh_thrown([&] { s.on_barrier(fh_barrier(1)); });
        EXPECT_NE(why.find("does not account for its request"), std::string::npos)
            << c.name << ": " << why;
        EXPECT_NE(why.find(c.expect), std::string::npos) << c.name << ": " << why;
        EXPECT_NE(why.find("a replay may send some twice"), std::string::npos) << c.name;
        EXPECT_TRUE(s.failed()) << c.name;
        EXPECT_EQ(m->calls().size(), 1u) << c.name << ": an ambiguous response is not retried";
        // The barrier is not acknowledged, now or later.
        EXPECT_NE(fh_thrown([&] { s.on_barrier(fh_barrier(2)); }), "") << c.name;
    }
}

// ---- permanent errors and lost replies -----------------------------------------------------

TEST(FirehoseSinkFailures, PermanentErrorsFailAtOnce) {
    for (const auto& [code, hint] : std::vector<std::pair<std::string, std::string>>{
             {"ResourceNotFoundException", "Check that the delivery stream exists"},
             {"InvalidArgumentException", "may be a defect in the sink"},
             {"InvalidKMSResourceException", "encryption key is not usable"},
             {"AccessDeniedException", "firehose:PutRecordBatch"},
             {"UnrecognizedClientException", "default chain"}}) {
        auto m = fh_model();
        m->set_planner([&](std::size_t, const auto&) {
            return FirehoseModel::error(code, 400, FirehoseFault::Permanent);
        });
        FirehoseSink s(fh_options(m));
        s.open();
        s.on_data(fh_batch({"a"}));
        const auto why = fh_thrown([&] { s.on_barrier(fh_barrier(1)); });
        EXPECT_NE(why.find("PutRecordBatch was refused: " + code), std::string::npos) << why;
        EXPECT_NE(why.find(hint), std::string::npos) << code << ": " << why;
        EXPECT_EQ(m->calls().size(), 1u) << code;
    }
}

// A reply lost after Firehose accepted the request: the records are sent again
// and reach the stream twice, which at-least-once allows; none is lost.
TEST(FirehoseSinkFailures, AReplyLostAfterAcceptanceIsSentAgain) {
    auto m = fh_model();
    m->set_planner([](std::size_t call, const auto&) {
        return call == 0 ? FirehoseModel::error(
                               "NoResponse", 0, FirehoseFault::Transient, /*accepted=*/true)
                         : FirehoseModel::Plan{};
    });
    FirehoseSink s(fh_options(m));
    s.open();
    s.on_data(fh_batch({"a", "b"}));
    s.on_barrier(fh_barrier(1));
    EXPECT_EQ(m->calls().size(), 2u);
    EXPECT_EQ(m->accepted(), (std::vector<std::string>{"a\n", "b\n", "a\n", "b\n"}));
    EXPECT_FALSE(s.failed());
}

// ---- cancellation -------------------------------------------------------------------------

TEST(FirehoseSinkCancel, ACancelDuringARetryWaitStopsAtOnce) {
    auto m = fh_model();
    m->set_planner([](std::size_t, const auto&) {
        return FirehoseModel::error("ServiceUnavailableException", 500, FirehoseFault::Throttled);
    });
    auto o = fh_options(m);
    o.retry_base_backoff = 5000ms;
    o.max_backoff = 5000ms;
    FirehoseSink s(std::move(o));
    FhAttached a(s);
    s.open();
    s.on_data(fh_batch({"a"}));
    auto done = std::async(std::launch::async,
                           [&] { return fh_thrown([&] { s.on_barrier(fh_barrier(1)); }); });
    while (m->calls().empty()) {
        std::this_thread::sleep_for(5ms);
    }
    const auto t0 = std::chrono::steady_clock::now();
    a.cancel->store(true);
    ASSERT_EQ(done.wait_for(2s), std::future_status::ready);
    EXPECT_LT(std::chrono::steady_clock::now() - t0, 1s);
    EXPECT_NE(done.get().find("cancelled while waiting to retry a PutRecordBatch"),
              std::string::npos);
    EXPECT_TRUE(s.failed());
}

TEST(FirehoseSinkCancel, ACancelDuringACallEndsIt) {
    auto m = fh_model();
    m->set_planner([](std::size_t, const auto&) {
        FirehoseModel::Plan p;
        p.hang = true;
        return p;
    });
    FirehoseSink s(fh_options(m));
    FhAttached a(s);
    s.open();
    s.on_data(fh_batch({"a"}));
    auto done = std::async(std::launch::async,
                           [&] { return fh_thrown([&] { s.on_barrier(fh_barrier(1)); }); });
    while (!m->hanging()) {
        std::this_thread::sleep_for(5ms);
    }
    // The barrier has not returned while its records are unresolved.
    EXPECT_EQ(done.wait_for(200ms), std::future_status::timeout);
    a.cancel->store(true);
    ASSERT_EQ(done.wait_for(2s), std::future_status::ready);
    const auto why = done.get();
    EXPECT_NE(why.find("cancelled while a PutRecordBatch of 1 record(s) was in flight"),
              std::string::npos)
        << why;
    EXPECT_TRUE(m->aborted());
    EXPECT_TRUE(s.failed());
}

// ---- the lifecycle ------------------------------------------------------------------------

TEST(FirehoseSinkLifecycle, ABarrierReturnsOnlyOnceEveryRecordBeforeItIsAccepted) {
    auto m = fh_model();
    m->set_planner([](std::size_t call, const auto& records) {
        return call < 2 ? FirehoseModel::failing(records.size(), {0}) : FirehoseModel::Plan{};
    });
    FirehoseSink s(fh_options(m));
    s.open();
    s.on_data(fh_batch({"a", "b", "c"}));
    EXPECT_TRUE(m->calls().empty());  // buffered, below batch_records
    s.on_barrier(fh_barrier(1));
    EXPECT_EQ(m->accepted().size(), 3u);
    EXPECT_EQ(s.pending_records(), 0u);
}

TEST(FirehoseSinkLifecycle, AFailedSinkRefusesEveryLaterBarrier) {
    auto m = fh_model();
    m->set_planner([](std::size_t, const auto&) {
        return FirehoseModel::error("ResourceNotFoundException", 400, FirehoseFault::Permanent);
    });
    auto o = fh_options(m);
    o.batch_records = 1;
    FirehoseSink s(std::move(o));
    s.open();
    EXPECT_NE(fh_thrown([&] { s.on_data(fh_batch({"a", "b"})); }), "");
    EXPECT_TRUE(s.failed());
    for (std::uint64_t id = 1; id <= 3; ++id) {
        EXPECT_NE(fh_thrown([&] {
                      s.on_barrier(fh_barrier(id));
                  }).find("failed earlier and dropped records"),
                  std::string::npos)
            << "barrier " << id;
    }
    EXPECT_NE(fh_thrown([&] { s.flush(); }).find("failed earlier"), std::string::npos);
    EXPECT_NE(fh_thrown([&] { s.on_data(fh_batch({"c"})); }).find("failed earlier"),
              std::string::npos);
    EXPECT_NO_THROW(s.close());
    EXPECT_EQ(m->calls().size(), 1u);
}

// The end of input: the runtime flushes, then closes. The flush sends what is
// buffered; close sends nothing, whether or not a flush came first.
TEST(FirehoseSinkLifecycle, EndOfInputSendsEverythingAndCloseSendsNothing) {
    auto m = fh_model();
    FirehoseSink s(fh_options(m));
    s.open();
    s.on_data(fh_batch({"a", "b"}));
    s.flush();
    s.close();
    EXPECT_EQ(m->accepted(), (std::vector<std::string>{"a\n", "b\n"}));
    FirehoseSink unflushed(fh_options(m));
    unflushed.open();
    unflushed.on_data(fh_batch({"c"}));
    unflushed.close();
    EXPECT_EQ(m->calls().size(), 1u);
}

TEST(FirehoseSinkLifecycle, OpenStartsAfreshAfterAFailure) {
    auto m = fh_model();
    m->set_planner([](std::size_t call, const auto&) {
        return call == 0 ? FirehoseModel::error(
                               "ResourceNotFoundException", 400, FirehoseFault::Permanent)
                         : FirehoseModel::Plan{};
    });
    FirehoseSink s(fh_options(m));
    s.open();
    s.on_data(fh_batch({"a"}));
    EXPECT_NE(fh_thrown([&] { s.on_barrier(fh_barrier(1)); }), "");
    EXPECT_TRUE(s.failed());
    s.close();
    // As a restart would: open again and replay what the checkpoint did not cover.
    s.open();
    EXPECT_FALSE(s.failed());
    EXPECT_EQ(s.pending_records(), 0u);
    s.on_data(fh_batch({"a"}));
    s.on_barrier(fh_barrier(2));
    EXPECT_EQ(m->accepted(), (std::vector<std::string>{"a\n"}));
}

TEST(FirehoseSinkLifecycle, LingerFlushesAnOldPartialBatch) {
    auto m = fh_model();
    auto o = fh_options(m);
    o.max_age = 30ms;
    FirehoseSink s(std::move(o));
    s.open();
    s.on_data(fh_batch({"a"}));
    EXPECT_TRUE(m->calls().empty());
    std::this_thread::sleep_for(40ms);
    s.on_data(fh_batch({"b"}));
    EXPECT_EQ(m->accepted().size(), 2u);
}

// ---- classification -------------------------------------------------------------------------

TEST(FirehoseErrorClassification, FollowsTheServicesErrors) {
    using clink::aws::classify_firehose_error;
    EXPECT_EQ(classify_firehose_error("ServiceUnavailableException", 500, true),
              FirehoseFault::Throttled);
    EXPECT_EQ(classify_firehose_error("ThrottlingException", 400, true), FirehoseFault::Throttled);
    EXPECT_EQ(classify_firehose_error("", 429, true), FirehoseFault::Throttled);
    for (const char* c :
         {"InternalFailure", "InternalServerError", "ResourceInUseException", "RequestTimeout"}) {
        EXPECT_EQ(classify_firehose_error(c, 400, false), FirehoseFault::Transient) << c;
    }
    EXPECT_EQ(classify_firehose_error("NoResponse", 0, false), FirehoseFault::Transient);
    EXPECT_EQ(classify_firehose_error("SomethingNew", 503, false), FirehoseFault::Transient);
    for (const char* c : {"InvalidArgumentException",
                          "InvalidKMSResourceException",
                          "InvalidSourceException",
                          "ResourceNotFoundException",
                          "AccessDeniedException",
                          "UnrecognizedClientException",
                          "com.amazonaws.firehose#ResourceNotFoundException"}) {
        EXPECT_EQ(classify_firehose_error(c, 400, true), FirehoseFault::Permanent) << c;
    }
    EXPECT_EQ(classify_firehose_error("SomethingNew", 400, false), FirehoseFault::Permanent);
    EXPECT_EQ(classify_firehose_error("SomethingNew", 400, true), FirehoseFault::Transient);
}
