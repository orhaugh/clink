// The Kinesis sink LIVE against LocalStack. SKIPPED unless
// CLINK_KINESIS_TEST_ENDPOINT is set (scripts/kinesis-live.sh sets it, and the
// AWS variables the SDK signs with; LocalStack accepts any credentials). Each
// case writes through the production sink and its SDK client, then reads the
// stream back with a separate client of its own (ListShards, GetShardIterator,
// GetRecords) and compares every record's identity, bytes and partition key
// with what was written: duplicates are allowed, a missing, changed or
// unexpected record is not. The emulator (LocalStack 3.8.1) refuses a partition
// key past 256 UTF-16 code units and data past 1 MiB, but does not count the key
// against the record size and has no request limits, so the sink's own limits
// are shown by what never reaches the stream. Throttling, partial failures,
// ambiguous responses and lost replies are the deterministic cases' work
// (test_kinesis_sink.cpp, test_kinesis_sink_sdk.cpp).

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include <aws/kinesis/KinesisClient.h>
#include <aws/kinesis/model/CreateStreamRequest.h>
#include <aws/kinesis/model/DeleteStreamRequest.h>
#include <aws/kinesis/model/DescribeStreamSummaryRequest.h>
#include <aws/kinesis/model/GetRecordsRequest.h>
#include <aws/kinesis/model/GetShardIteratorRequest.h>
#include <aws/kinesis/model/ListShardsRequest.h>
#include <aws/kinesis/model/ShardIteratorType.h>
#include <aws/kinesis/model/StreamStatus.h>
#include <gtest/gtest.h>

#include "clink/aws/aws_client.hpp"
#include "clink/aws/kinesis_sink.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/runtime/dead_letter.hpp"
#include "clink/runtime/runtime_context.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::aws::DlqPolicy;
using clink::aws::KinesisSink;
using clink::aws::KinesisSinkOptions;

namespace {

std::string ksl_endpoint() {
    const char* v = std::getenv("CLINK_KINESIS_TEST_ENDPOINT");
    return v == nullptr ? std::string{} : std::string{v};
}

clink::aws::AwsClientOptions ksl_client() {
    clink::aws::AwsClientOptions o;
    o.region = "us-east-1";
    o.endpoint_override = ksl_endpoint();
    o.request_timeout_ms = 60000;
    return o;
}

std::string ksl_stream_name() {
    static int n = 0;
    return "clink-sink-it-" + std::to_string(static_cast<long>(::getpid())) + "-" +
           std::to_string(n++);
}

CheckpointBarrier ksl_barrier(std::uint64_t id) {
    return CheckpointBarrier{clink::CheckpointId{id}};
}

Batch<std::string> ksl_batch(const std::vector<std::string>& records) {
    Batch<std::string> b;
    for (const auto& r : records) {
        b.emplace(r);
    }
    return b;
}

template <typename F>
std::string ksl_thrown(F&& f) {
    try {
        f();
    } catch (const std::exception& e) {
        return e.what();
    }
    return "";
}

// One record as the reader found it.
struct KslRead {
    std::string shard;
    std::string sequence;
    std::string key;
    std::string data;
};

// The stream, created for the case and deleted after it, and the reader.
class KslStream {
public:
    explicit KslStream(int shards) : name_(ksl_stream_name()) {
        clink::aws::ensure_aws_initialized();
        client_ = std::make_unique<Aws::Kinesis::KinesisClient>(
            clink::aws::make_client_config(ksl_client()));
        Aws::Kinesis::Model::CreateStreamRequest req;
        req.SetStreamName(name_);
        req.SetShardCount(shards);
        client_->CreateStream(req);
        for (int i = 0; i < 300; ++i) {
            Aws::Kinesis::Model::DescribeStreamSummaryRequest d;
            d.SetStreamName(name_);
            const auto r = client_->DescribeStreamSummary(d);
            if (r.IsSuccess() && r.GetResult().GetStreamDescriptionSummary().GetStreamStatus() ==
                                     Aws::Kinesis::Model::StreamStatus::ACTIVE) {
                active_ = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds{100});
        }
    }
    ~KslStream() {
        Aws::Kinesis::Model::DeleteStreamRequest req;
        req.SetStreamName(name_);
        client_->DeleteStream(req);
    }
    KslStream(const KslStream&) = delete;
    KslStream& operator=(const KslStream&) = delete;

    [[nodiscard]] const std::string& name() const { return name_; }
    [[nodiscard]] bool active() const { return active_; }

    // Every record in every shard, each shard read from its oldest record until
    // a read returns nothing.
    std::vector<KslRead> read_all() {
        std::vector<KslRead> out;
        std::vector<std::string> shards;
        Aws::Kinesis::Model::ListShardsRequest list;
        list.SetStreamName(name_);
        const auto lr = client_->ListShards(list);
        if (!lr.IsSuccess()) {
            ADD_FAILURE() << "ListShards: " << lr.GetError().GetMessage();
            return out;
        }
        for (const auto& s : lr.GetResult().GetShards()) {
            shards.push_back(s.GetShardId());
        }
        for (const auto& shard : shards) {
            Aws::Kinesis::Model::GetShardIteratorRequest gi;
            gi.SetStreamName(name_);
            gi.SetShardId(shard);
            gi.SetShardIteratorType(Aws::Kinesis::Model::ShardIteratorType::TRIM_HORIZON);
            const auto ir = client_->GetShardIterator(gi);
            if (!ir.IsSuccess()) {
                ADD_FAILURE() << "GetShardIterator: " << ir.GetError().GetMessage();
                continue;
            }
            auto iterator = ir.GetResult().GetShardIterator();
            for (int round = 0; round < 1000 && !iterator.empty(); ++round) {
                Aws::Kinesis::Model::GetRecordsRequest gr;
                gr.SetShardIterator(iterator);
                gr.SetLimit(10000);
                const auto rr = client_->GetRecords(gr);
                if (!rr.IsSuccess()) {
                    ADD_FAILURE() << "GetRecords: " << rr.GetError().GetMessage();
                    break;
                }
                const auto& records = rr.GetResult().GetRecords();
                for (const auto& r : records) {
                    const auto& d = r.GetData();
                    out.push_back(
                        KslRead{shard,
                                r.GetSequenceNumber(),
                                r.GetPartitionKey(),
                                std::string(reinterpret_cast<const char*>(d.GetUnderlyingData()),
                                            d.GetLength())});
                }
                if (records.empty()) {
                    break;
                }
                iterator = rr.GetResult().GetNextShardIterator();
            }
        }
        return out;
    }

private:
    std::string name_;
    std::unique_ptr<Aws::Kinesis::KinesisClient> client_;
    bool active_{false};
};

// What the stream holds against what was written, record by record: each
// written (data, key) pair must arrive at least once, with that key; nothing
// else may arrive. Returns the duplicates.
std::size_t ksl_compare(const char* name,
                        const std::vector<std::pair<std::string, std::string>>& written,
                        const std::vector<KslRead>& read) {
    std::map<std::pair<std::string, std::string>, int> want;
    for (const auto& w : written) {
        ++want[w];
    }
    std::map<std::pair<std::string, std::string>, int> got;
    for (const auto& r : read) {
        ++got[{r.data, r.key}];
    }
    std::size_t duplicates = 0;
    int missing = 0;
    for (const auto& [w, n] : want) {
        const auto it = got.find(w);
        if (it == got.end()) {
            if (++missing <= 5) {
                ADD_FAILURE() << "record of " << w.first.size() << " bytes with key '"
                              << w.second.substr(0, 40) << "' never reached the stream";
            }
        } else if (it->second > n) {
            duplicates += static_cast<std::size_t>(it->second - n);
        }
    }
    int unexpected = 0;
    for (const auto& [g, n] : got) {
        if (want.count(g) == 0 && ++unexpected <= 5) {
            ADD_FAILURE() << "the stream holds a record of " << g.first.size()
                          << " bytes with key '" << g.second.substr(0, 40)
                          << "' that was never written so";
        }
    }
    EXPECT_EQ(missing, 0);
    EXPECT_EQ(unexpected, 0);
    std::printf(
        "[kinesis-sink-live] case=%s written=%zu read=%zu missing=%d unexpected=%d "
        "duplicates=%zu\n",
        name,
        written.size(),
        read.size(),
        missing,
        unexpected,
        duplicates);
    std::fflush(stdout);
    return duplicates;
}

class KslDeadLetters final : public clink::DeadLetterQueue {
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

std::string ksl_emoji(std::size_t n) {
    std::string out;
    for (std::size_t i = 0; i < n; ++i) {
        out += "\xF0\x9F\x98\x80";
    }
    return out;
}

class KinesisSinkLive : public ::testing::Test {
protected:
    void SetUp() override {
        if (ksl_endpoint().empty()) {
            GTEST_SKIP() << "set CLINK_KINESIS_TEST_ENDPOINT (scripts/kinesis-live.sh does)";
        }
    }

    static KinesisSinkOptions options(const std::string& stream, std::string partition_key) {
        KinesisSinkOptions o;
        o.stream = stream;
        o.partition_key = std::move(partition_key);
        o.client = ksl_client();
        return o;
    }
};

}  // namespace

// Direct construction: every kind of key the sink takes from a record reaches
// the stream as the record holds it, numbers by their exact value, and a key's
// records all share one shard.
TEST_F(KinesisSinkLive, KeysAndPayloadsArriveExactly) {
    KslStream stream(4);
    ASSERT_TRUE(stream.active());
    KinesisSink sink(options(stream.name(), "k"));
    sink.open();
    std::vector<std::pair<std::string, std::string>> written = {
        {R"({"id":1,"k":"user-1"})", "user-1"},
        {R"({"id":2,"k":9007199254740993})", "9007199254740993"},
        {R"({"id":3,"k":9007199254740992})", "9007199254740992"},
        {R"({"id":4,"k":0.0000001})", "0.0000001"},
        {R"({"id":5,"k":0.0000002})", "0.0000002"},
        {R"({"id":6,"k":1.50})", "1.5"},
        {R"({"id":7,"k":15E-1})", "1.5"},
        {R"({"id":8,"k":true})", "true"},
        {"{\"id\":9,\"k\":\"\xC3\xA9\xE6\x97\xA5\xF0\x9F\x98\x80\"}",
         "\xC3\xA9\xE6\x97\xA5\xF0\x9F\x98\x80"},
        {"{\"id\":10,\"k\":\"" + ksl_emoji(128) + "\"}", ksl_emoji(128)},
        {"{\"id\":11,\"k\":\"" + std::string(256, 'a') + "\"}", std::string(256, 'a')},
        {R"({"id":12,"k":"tab\there"})", "tab\there"},
    };
    for (int i = 0; i < 1200; ++i) {
        const std::string key = "k" + std::to_string(i % 24);
        written.emplace_back("{\"id\":" + std::to_string(100 + i) + ",\"k\":\"" + key +
                                 "\",\"pad\":\"" +
                                 std::string(static_cast<std::size_t>(i % 97), 'x') + "\"}",
                             key);
    }
    std::vector<std::string> records;
    for (const auto& [data, key] : written) {
        records.push_back(data);
    }
    sink.on_data(ksl_batch(records));
    sink.on_barrier(ksl_barrier(1));
    sink.close();
    const auto read = stream.read_all();
    const auto duplicates = ksl_compare("keys_and_payloads", written, read);
    EXPECT_EQ(duplicates, 0u) << "nothing failed, so nothing was sent twice";
    std::map<std::string, std::set<std::string>> shards_of;
    for (const auto& r : read) {
        shards_of[r.key].insert(r.shard);
    }
    for (const auto& [key, shards] : shards_of) {
        EXPECT_EQ(shards.size(), 1u) << "key '" << key.substr(0, 40) << "' spread over shards";
    }
    // 1.50 and 15E-1 are one key, so one shard.
    EXPECT_EQ(shards_of.count("1.5"), 1u);
}

// The registered factory, without partition_key: opaque payloads of every byte
// value arrive whole under numbered keys, a record at the limit with its key
// arrives, and one a byte over it never leaves the sink.
TEST_F(KinesisSinkLive, OpaquePayloadsArriveWholeThroughTheFactory) {
    KslStream stream(2);
    ASSERT_TRUE(stream.active());
    const auto* f = clink::cluster::OperatorRegistry::default_instance().find_sink(
        "kinesis_sink", std::string{clink::cluster::kChannelString});
    ASSERT_NE(f, nullptr);
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = {{"stream", stream.name()},
                  {"region", "us-east-1"},
                  {"endpoint_override", ksl_endpoint()},
                  {"request_timeout_ms", "60000"},
                  {"batch_records", "100"}};
    // The factory hands back the sink behind the channel's type.
    const auto sink = std::static_pointer_cast<clink::Sink<std::string>>(f->build(ctx));
    ASSERT_NE(sink, nullptr);
    sink->open();
    std::string every_byte;
    for (int i = 0; i < 256; ++i) {
        every_byte.push_back(static_cast<char>(i));
    }
    // The fourth record's key is "3": one byte, so 1,048,575 bytes of data make 1 MiB.
    const std::vector<std::string> payloads = {every_byte,
                                               "",
                                               std::string("\0\0nul\0", 6),
                                               std::string(1024 * 1024 - 1, 'm'),
                                               "\xff\xfe not UTF-8",
                                               "{\"half\": "};
    sink->on_data(ksl_batch(payloads));
    sink->on_barrier(ksl_barrier(1));
    std::vector<std::pair<std::string, std::string>> written;
    for (std::size_t i = 0; i < payloads.size(); ++i) {
        written.emplace_back(payloads[i], std::to_string(i));
    }
    // The next key is "6", so 1 MiB of data is a byte over.
    const auto why = ksl_thrown([&] { sink->on_data(ksl_batch({std::string(1024 * 1024, 'o')})); });
    EXPECT_NE(why.find("is 1,048,577 bytes with its 1-byte partition key"), std::string::npos)
        << why;
    sink->close();
    EXPECT_EQ(ksl_compare("opaque_payloads_factory", written, stream.read_all()), 0u);
}

// Records without a usable key, or over the limit, never reach the stream:
// under dlq='drop' they go to the dead-letter path and the rest are written;
// under dlq='fail' the sink fails and nothing of their batch is sent.
TEST_F(KinesisSinkLive, RefusedRecordsNeverReachTheStream) {
    KslStream stream(1);
    ASSERT_TRUE(stream.active());
    auto o = options(stream.name(), "k");
    o.dlq_policy = DlqPolicy::Drop;
    KinesisSink drop(std::move(o));
    KslDeadLetters dead;
    clink::RuntimeContext ctx(clink::OperatorId{5151}, "kinesis_sink", nullptr, nullptr);
    ctx.set_dead_letter_queue(&dead);
    drop.attach_runtime(&ctx);
    drop.open();
    const std::string over = R"({"k":"o","pad":")" + std::string(1024 * 1024, 'p') + "\"}";
    drop.on_data(ksl_batch({R"({"k":"a","n":1})",
                            R"({"n":2})",
                            R"({"k":null,"n":3})",
                            "{\"k\":\"" + ksl_emoji(129) + "\"}",
                            over,
                            R"({"k":"b","n":6})"}));
    drop.on_barrier(ksl_barrier(1));
    drop.attach_runtime(nullptr);
    EXPECT_EQ(dead.got().size(), 4u);
    KinesisSink fail(options(stream.name(), "k"));
    fail.open();
    EXPECT_NE(ksl_thrown([&] {
                  fail.on_data(ksl_batch({R"({"k":"c"})", R"({"k":[]})"}));
              }).find("is an array"),
              std::string::npos);
    EXPECT_TRUE(fail.failed());
    EXPECT_EQ(ksl_compare("refused_never_written",
                          {{R"({"k":"a","n":1})", "a"}, {R"({"k":"b","n":6})", "b"}},
                          stream.read_all()),
              0u);
}

// A stream that does not exist fails the flush at once, on the first attempt.
TEST_F(KinesisSinkLive, AMissingStreamFailsAtOnce) {
    auto o = options("clink-no-such-stream-" + std::to_string(static_cast<long>(::getpid())), "");
    o.max_retries = 8;
    KinesisSink sink(std::move(o));
    sink.open();
    sink.on_data(ksl_batch({"a"}));
    const auto t0 = std::chrono::steady_clock::now();
    const auto why = ksl_thrown([&] { sink.on_barrier(ksl_barrier(1)); });
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds{20});
    EXPECT_NE(why.find("PutRecords was refused: ResourceNotFoundException"), std::string::npos)
        << why;
    EXPECT_NE(why.find("Check that the stream exists"), std::string::npos) << why;
    EXPECT_TRUE(sink.failed());
}
