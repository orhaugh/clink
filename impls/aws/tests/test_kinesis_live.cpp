// Kinesis LIVE integration tests against LocalStack. SKIPPED unless
// CLINK_KINESIS_TEST_ENDPOINT is set (scripts/kinesis-live.sh sets it, and the
// AWS variables the SDK signs with; LocalStack accepts any credentials). These
// are the emulator evidence for the source's contract; the deterministic cases
// in test_kinesis_source.cpp cover what the emulator cannot produce (throttling,
// retention loss, expired iterators). Proves, against the emulator's own
// ListShards, SplitShard, MergeShards, UpdateShardCount and iterator types:
//   - a sink->source round trip delivers every record, and a checkpoint resumes
//     a reader with no gap and no record read twice;
//   - at parallelism 1, records of one partition key come out in the order they
//     were written across a split and a merge made while the source reads;
//   - two subtasks with one-shard listing pages own every shard exactly once
//     through a uniform reshard, and keep each shard's order;
//   - latest mode skips earlier records, and a restore from a checkpoint taken
//     before the first record reads what arrived during the outage;
//   - a missing stream fails at once, a recreated stream is refused, and cancel
//     stops the source at once.

#include <chrono>
#include <cstdlib>
#include <map>
#include <set>
#include <string>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/kinesis/KinesisClient.h>
#include <aws/kinesis/model/CreateStreamRequest.h>
#include <aws/kinesis/model/DeleteStreamRequest.h>
#include <aws/kinesis/model/DescribeStreamRequest.h>
#include <aws/kinesis/model/DescribeStreamSummaryRequest.h>
#include <aws/kinesis/model/ListShardsRequest.h>
#include <aws/kinesis/model/MergeShardsRequest.h>
#include <aws/kinesis/model/PutRecordRequest.h>
#include <aws/kinesis/model/SplitShardRequest.h>
#include <aws/kinesis/model/StreamStatus.h>
#include <aws/kinesis/model/UpdateShardCountRequest.h>
#include <gtest/gtest.h>

#include "clink/aws/aws_client.hpp"
#include "clink/aws/kinesis_sink.hpp"
#include "clink/aws/kinesis_source.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using clink::Batch;
using clink::Emitter;
using clink::InMemoryStateBackend;
using clink::OperatorId;
using clink::StreamElement;
using clink::aws::AwsClientOptions;
using clink::aws::ensure_aws_initialized;
using clink::aws::KinesisSink;
using clink::aws::KinesisSinkOptions;
using clink::aws::KinesisSource;
using clink::aws::KinesisSourceOptions;
using clink::aws::make_client_config;

namespace {

bool kinesis_configured() {
    return std::getenv("CLINK_KINESIS_TEST_ENDPOINT") != nullptr;
}
std::string kinesis_endpoint() {
    return std::getenv("CLINK_KINESIS_TEST_ENDPOINT");
}

AwsClientOptions client_opts() {
    AwsClientOptions o;
    o.region = "us-east-1";
    o.endpoint_override = kinesis_endpoint();
    return o;
}

std::string unique_stream() {
    static int n = 0;
    return "clink-it-" + std::to_string(static_cast<long>(::getpid())) + "-" + std::to_string(n++);
}

// Create the stream and wait until it is ACTIVE (LocalStack is near-instant, but
// poll to be safe). Returns false on failure.
bool create_stream(Aws::Kinesis::KinesisClient& c, const std::string& name, int shards = 1) {
    Aws::Kinesis::Model::CreateStreamRequest req;
    req.SetStreamName(name);
    req.SetShardCount(shards);
    c.CreateStream(req);  // ResourceInUse if it already exists - fine
    for (int i = 0; i < 60; ++i) {
        Aws::Kinesis::Model::DescribeStreamRequest d;
        d.SetStreamName(name);
        auto dr = c.DescribeStream(d);
        if (dr.IsSuccess() && dr.GetResult().GetStreamDescription().GetStreamStatus() ==
                                  Aws::Kinesis::Model::StreamStatus::ACTIVE) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{200});
    }
    return false;
}

void delete_stream(Aws::Kinesis::KinesisClient& c, const std::string& name) {
    Aws::Kinesis::Model::DeleteStreamRequest req;
    req.SetStreamName(name);
    c.DeleteStream(req);
}

struct Captured {
    std::vector<std::string> values;
};
Emitter<std::string> capturing(Captured& sink) {
    return Emitter<std::string>{[&sink](StreamElement<std::string> e) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                sink.values.push_back(r.value());
            }
        }
        return true;
    }};
}

KinesisSinkOptions sink_opts(const std::string& stream) {
    KinesisSinkOptions o;
    o.stream = stream;
    o.client = client_opts();
    return o;
}

KinesisSourceOptions source_opts(const std::string& stream) {
    KinesisSourceOptions o;
    o.stream = stream;
    o.client = client_opts();
    o.initial_position = "trim_horizon";
    o.poll_interval = std::chrono::milliseconds{50};  // drain fast in the test
    // Small GetRecords Limit so the checkpoint test can read a bounded PREFIX
    // (one GetRecords would otherwise return the whole stream at once, leaving
    // nothing for the resumed reader and hollowing out the resume assertion).
    o.max_records_per_poll = 5;
    return o;
}

// Drive produce() until `want` records are collected or the deadline passes.
void drain(clink::Source<std::string>& src, Captured& cap, std::size_t want, int timeout_ms) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{timeout_ms};
    while (cap.values.size() < want && std::chrono::steady_clock::now() < deadline) {
        auto em = capturing(cap);
        src.produce(em);
    }
}

// ---- emulator helpers for the reshard cases -----------------------------------

struct ListedShard {
    std::string id;
    std::string parent;
    std::string adjacent_parent;
    std::string start_hash;
    std::string end_hash;
    bool open{true};
};

std::vector<ListedShard> list_all(Aws::Kinesis::KinesisClient& c, const std::string& stream) {
    std::vector<ListedShard> out;
    std::string token;
    do {
        Aws::Kinesis::Model::ListShardsRequest req;
        if (token.empty()) {
            req.SetStreamName(stream);
        } else {
            req.SetNextToken(token);
        }
        auto r = c.ListShards(req);
        if (!r.IsSuccess()) {
            ADD_FAILURE() << "ListShards: " << r.GetError().GetMessage();
            return out;
        }
        for (const auto& sh : r.GetResult().GetShards()) {
            out.push_back(
                ListedShard{sh.GetShardId(),
                            sh.GetParentShardId(),
                            sh.GetAdjacentParentShardId(),
                            sh.GetHashKeyRange().GetStartingHashKey(),
                            sh.GetHashKeyRange().GetEndingHashKey(),
                            sh.GetSequenceNumberRange().GetEndingSequenceNumber().empty()});
        }
        token = r.GetResult().GetNextToken();
    } while (!token.empty());
    return out;
}

bool await_active(Aws::Kinesis::KinesisClient& c, const std::string& stream) {
    for (int i = 0; i < 150; ++i) {
        Aws::Kinesis::Model::DescribeStreamSummaryRequest d;
        d.SetStreamName(stream);
        auto r = c.DescribeStreamSummary(d);
        if (r.IsSuccess() && r.GetResult().GetStreamDescriptionSummary().GetStreamStatus() ==
                                 Aws::Kinesis::Model::StreamStatus::ACTIVE) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
    }
    return false;
}

using u128 = unsigned __int128;
u128 parse_u128(const std::string& s) {
    u128 v = 0;
    for (const char ch : s) {
        v = v * 10 + static_cast<unsigned>(ch - '0');
    }
    return v;
}
std::string format_u128(u128 v) {
    if (v == 0) {
        return "0";
    }
    std::string out;
    while (v > 0) {
        out.insert(out.begin(), static_cast<char>('0' + static_cast<int>(v % 10)));
        v /= 10;
    }
    return out;
}

// Splits an open shard at the middle of its hash key range.
void split_in_half(Aws::Kinesis::KinesisClient& c,
                   const std::string& stream,
                   const std::string& id) {
    for (const auto& sh : list_all(c, stream)) {
        if (sh.id != id) {
            continue;
        }
        const u128 mid = (parse_u128(sh.start_hash) + parse_u128(sh.end_hash)) / 2 + 1;
        Aws::Kinesis::Model::SplitShardRequest req;
        req.SetStreamName(stream);
        req.SetShardToSplit(id);
        req.SetNewStartingHashKey(format_u128(mid));
        auto r = c.SplitShard(req);
        ASSERT_TRUE(r.IsSuccess()) << "SplitShard: " << r.GetError().GetMessage();
        ASSERT_TRUE(await_active(c, stream));
        return;
    }
    FAIL() << "no shard " << id;
}

void merge_pair(Aws::Kinesis::KinesisClient& c,
                const std::string& stream,
                const std::string& a,
                const std::string& b) {
    Aws::Kinesis::Model::MergeShardsRequest req;
    req.SetStreamName(stream);
    req.SetShardToMerge(a);
    req.SetAdjacentShardToMerge(b);
    auto r = c.MergeShards(req);
    ASSERT_TRUE(r.IsSuccess()) << "MergeShards: " << r.GetError().GetMessage();
    ASSERT_TRUE(await_active(c, stream));
}

struct Written {
    std::string shard;
    std::string sequence;
};

// Writes one record and returns where the stream put it.
Written put_record(Aws::Kinesis::KinesisClient& c,
                   const std::string& stream,
                   const std::string& key,
                   const std::string& data) {
    Aws::Kinesis::Model::PutRecordRequest req;
    req.SetStreamName(stream);
    req.SetPartitionKey(key);
    req.SetData(
        Aws::Utils::ByteBuffer(reinterpret_cast<const unsigned char*>(data.data()), data.size()));
    auto r = c.PutRecord(req);
    if (!r.IsSuccess()) {
        ADD_FAILURE() << "PutRecord: " << r.GetError().GetMessage();
        return {};
    }
    return Written{r.GetResult().GetShardId(), r.GetResult().GetSequenceNumber()};
}

// Records "<key>:<ordinal>" for 12 keys; returns each record's shard.
class KeyedWriter {
public:
    KeyedWriter(Aws::Kinesis::KinesisClient& c, std::string stream)
        : c_(c), stream_(std::move(stream)) {}
    void write(int n) {
        for (int i = 0; i < n; ++i) {
            const std::string key = "k" + std::to_string(next_ % 12);
            const std::string data = key + ":" + std::to_string(ordinal_[key]++);
            const auto w = put_record(c_, stream_, key, data);
            where_[data] = w.shard;
            order_.push_back(data);
            ++next_;
        }
    }
    [[nodiscard]] const std::vector<std::string>& order() const { return order_; }
    [[nodiscard]] const std::map<std::string, std::string>& where() const { return where_; }

private:
    Aws::Kinesis::KinesisClient& c_;
    std::string stream_;
    int next_{0};
    std::map<std::string, int> ordinal_;
    std::map<std::string, std::string> where_;
    std::vector<std::string> order_;
};

// Every key's records appear in the order they were written.
void expect_key_order(const std::vector<std::string>& got) {
    std::map<std::string, int> last;
    for (const auto& v : got) {
        const auto colon = v.find(':');
        const auto key = v.substr(0, colon);
        const int ordinal = std::stoi(v.substr(colon + 1));
        const auto it = last.find(key);
        if (it != last.end()) {
            EXPECT_GT(ordinal, it->second) << "key " << key << " out of order at " << v;
        }
        last[key] = ordinal;
    }
}

// Drives several sources round robin until `done` or the deadline.
template <typename Done>
bool drive(std::vector<std::pair<clink::Source<std::string>*, Captured*>> readers,
           Done done,
           int timeout_ms) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{timeout_ms};
    while (!done()) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        for (auto& [src, cap] : readers) {
            auto em = capturing(*cap);
            src->produce(em);
        }
    }
    return true;
}

KinesisSourceOptions fast_source(const std::string& stream) {
    auto o = source_opts(stream);
    o.poll_interval = std::chrono::milliseconds{20};
    o.read_spacing = std::chrono::milliseconds{0};
    o.discovery_interval = std::chrono::milliseconds{200};
    return o;
}

std::string expect_failure(const std::function<void()>& f) {
    try {
        f();
    } catch (const std::exception& e) {
        return e.what();
    }
    ADD_FAILURE() << "expected the source to fail";
    return {};
}

}  // namespace

TEST(KinesisLive, SinkThenSourceRoundTrip) {
    if (!kinesis_configured()) {
        GTEST_SKIP() << "set CLINK_KINESIS_TEST_ENDPOINT (docker/integration-services.yml)";
    }
    ensure_aws_initialized();
    Aws::Kinesis::KinesisClient admin(make_client_config(client_opts()));
    const std::string stream = unique_stream();
    ASSERT_TRUE(create_stream(admin, stream)) << "stream did not become ACTIVE";

    constexpr std::size_t kN = 20;
    {
        KinesisSink sink(sink_opts(stream));
        sink.open();
        Batch<std::string> b;
        for (std::size_t i = 0; i < kN; ++i) {
            b.emplace(R"({"i":)" + std::to_string(i) + "}");
        }
        sink.on_data(b);
        sink.flush();
    }

    KinesisSource src(source_opts(stream));
    src.open();
    Captured cap;
    drain(src, cap, kN, /*timeout_ms=*/30000);
    src.close();
    delete_stream(admin, stream);

    EXPECT_EQ(cap.values.size(), kN) << "every sunk record should be read back";
}

TEST(KinesisLive, SequenceCheckpointResumesWithoutGaps) {
    if (!kinesis_configured()) {
        GTEST_SKIP() << "set CLINK_KINESIS_TEST_ENDPOINT";
    }
    ensure_aws_initialized();
    Aws::Kinesis::KinesisClient admin(make_client_config(client_opts()));
    const std::string stream = unique_stream();
    ASSERT_TRUE(create_stream(admin, stream)) << "stream did not become ACTIVE";

    constexpr std::size_t kN = 30;
    {
        KinesisSink sink(sink_opts(stream));
        sink.open();
        Batch<std::string> b;
        for (std::size_t i = 0; i < kN; ++i) {
            b.emplace(R"({"i":)" + std::to_string(i) + "}");
        }
        sink.on_data(b);
        sink.flush();
    }

    InMemoryStateBackend backend;
    const OperatorId op_id{1};
    std::set<std::string> first;   // records read BEFORE the checkpoint
    std::set<std::string> second;  // records read by the RESUMED reader

    // Read a bounded PREFIX, then checkpoint the per-shard sequence number.
    {
        KinesisSource s1(source_opts(stream));
        s1.open();
        Captured cap;
        drain(s1, cap, /*want=*/10, /*timeout_ms=*/20000);
        s1.snapshot_offset(backend, op_id, clink::CheckpointId{1});
        s1.close();
        for (auto& v : cap.values) {
            first.insert(v);
        }
        ASSERT_GE(first.size(), 10u) << "should have read the first chunk";
        // Load-bearing: s1 must read only a PREFIX. If it drained the whole
        // stream the resume below would have nothing to do and the test would be
        // vacuous (it would pass even if restore_offset were broken).
        ASSERT_LT(first.size(), kN) << "s1 read the whole stream; resume is unverifiable";
    }
    // Resume from the checkpoint: a fresh reader restores and reads the REST.
    {
        KinesisSource s2(source_opts(stream));
        ASSERT_TRUE(s2.restore_offset(backend, op_id));
        s2.open();
        Captured cap;
        drain(s2, cap, /*want=*/kN - first.size(), /*timeout_ms=*/30000);
        s2.close();
        for (auto& v : cap.values) {
            second.insert(v);
        }
    }
    delete_stream(admin, stream);

    // RESUME PROOF: the resumed reader read strictly AFTER the checkpoint - it
    // must NOT re-read any record the first reader already consumed (a broken
    // restore would replay from TRIM_HORIZON and overlap here).
    for (const auto& v : second) {
        EXPECT_EQ(first.count(v), 0u) << "resumed reader re-read a checkpointed record: " << v;
    }
    // And together they cover every record with no gap.
    std::set<std::string> all = first;
    all.insert(second.begin(), second.end());
    EXPECT_EQ(all.size(), kN);
}

// Parallelism 1, with a split and then a merge made while the source reads:
// children wait for their parents, so every key's records come out in the
// order they were written, and each record exactly once.
TEST(KinesisLive, SplitAndMergeAtParallelismOneKeepEveryKeysOrder) {
    if (!kinesis_configured()) {
        GTEST_SKIP() << "set CLINK_KINESIS_TEST_ENDPOINT";
    }
    ensure_aws_initialized();
    Aws::Kinesis::KinesisClient admin(make_client_config(client_opts()));
    const std::string stream = unique_stream();
    ASSERT_TRUE(create_stream(admin, stream, 2));
    KeyedWriter writer(admin, stream);
    writer.write(60);

    auto o = fast_source(stream);
    o.max_records_per_poll = 4;  // the parents drain over many reads
    KinesisSource src(o);
    src.open();
    Captured cap;
    drive({{&src, &cap}}, [&] { return cap.values.size() >= 12; }, 20000);

    const std::string first = "shardId-000000000000";
    split_in_half(admin, stream, first);
    writer.write(60);
    drive({{&src, &cap}}, [&] { return cap.values.size() >= 70; }, 30000);
    std::vector<std::string> children;
    for (const auto& sh : list_all(admin, stream)) {
        if (sh.parent == first) {
            children.push_back(sh.id);
        }
    }
    ASSERT_EQ(children.size(), 2U) << "the split did not happen";
    merge_pair(admin, stream, children[0], children[1]);
    writer.write(60);
    ASSERT_TRUE(drive(
        {{&src, &cap}}, [&] { return cap.values.size() >= 180; }, 60000))
        << "read " << cap.values.size() << " of 180";
    drive({{&src, &cap}}, [] { return false; }, 500);
    src.close();

    std::string merged;
    for (const auto& sh : list_all(admin, stream)) {
        if (sh.parent == children[0] || sh.parent == children[1]) {
            merged = sh.id;
            EXPECT_FALSE(sh.adjacent_parent.empty()) << "the merge did not name two parents";
        }
    }
    delete_stream(admin, stream);
    ASSERT_FALSE(merged.empty()) << "the merge did not happen";
    EXPECT_EQ(std::multiset<std::string>(cap.values.begin(), cap.values.end()),
              std::multiset<std::string>(writer.order().begin(), writer.order().end()));
    expect_key_order(cap.values);
    std::map<std::string, clink::aws::KinesisShardCheckpoint::Status> status;
    for (const auto& s : src.shard_statuses()) {
        status[s.shard_id] = s.status;
    }
    EXPECT_EQ(status[first], clink::aws::KinesisShardCheckpoint::Status::Drained);
    EXPECT_EQ(status[children[0]], clink::aws::KinesisShardCheckpoint::Status::Drained);
    EXPECT_EQ(status[children[1]], clink::aws::KinesisShardCheckpoint::Status::Drained);
    EXPECT_EQ(status[merged], clink::aws::KinesisShardCheckpoint::Status::Reading);
}

// Two subtasks listing one shard per page through a uniform reshard (every
// shard split) and a merge: each shard is owned by exactly one subtask, every
// record is read once, and each shard's records keep their order.
TEST(KinesisLive, TwoSubtasksOwnEveryShardOnceThroughAReshard) {
    if (!kinesis_configured()) {
        GTEST_SKIP() << "set CLINK_KINESIS_TEST_ENDPOINT";
    }
    ensure_aws_initialized();
    Aws::Kinesis::KinesisClient admin(make_client_config(client_opts()));
    const std::string stream = unique_stream();
    ASSERT_TRUE(create_stream(admin, stream, 3));
    KeyedWriter writer(admin, stream);
    writer.write(60);

    std::vector<std::unique_ptr<KinesisSource>> sources;
    std::vector<Captured> caps(2);
    for (std::uint32_t i = 0; i < 2; ++i) {
        auto o = fast_source(stream);
        o.subtask_idx = i;
        o.parallelism = 2;
        o.list_page_size = 1;
        sources.push_back(std::make_unique<KinesisSource>(o));
        sources.back()->open();
    }
    std::vector<std::pair<clink::Source<std::string>*, Captured*>> readers = {
        {sources[0].get(), &caps[0]}, {sources[1].get(), &caps[1]}};
    const auto total = [&] { return caps[0].values.size() + caps[1].values.size(); };
    drive(readers, [&] { return total() >= 20; }, 20000);

    Aws::Kinesis::Model::UpdateShardCountRequest scale;
    scale.SetStreamName(stream);
    scale.SetTargetShardCount(6);
    scale.SetScalingType(Aws::Kinesis::Model::ScalingType::UNIFORM_SCALING);
    auto scaled = admin.UpdateShardCount(scale);
    ASSERT_TRUE(scaled.IsSuccess()) << scaled.GetError().GetMessage();
    ASSERT_TRUE(await_active(admin, stream));
    writer.write(60);
    std::vector<ListedShard> open_shards;
    for (const auto& sh : list_all(admin, stream)) {
        if (sh.open) {
            open_shards.push_back(sh);
        }
    }
    std::sort(open_shards.begin(), open_shards.end(), [](const auto& x, const auto& y) {
        return parse_u128(x.start_hash) < parse_u128(y.start_hash);
    });
    ASSERT_GE(open_shards.size(), 2U);
    merge_pair(admin, stream, open_shards[0].id, open_shards[1].id);
    writer.write(60);
    ASSERT_TRUE(drive(readers, [&] { return total() >= 180; }, 90000)) << "read " << total();
    drive(readers, [] { return false; }, 500);
    const auto listed = list_all(admin, stream);
    delete_stream(admin, stream);

    std::multiset<std::string> read;
    std::set<std::string> owned;
    for (std::uint32_t i = 0; i < 2; ++i) {
        for (const auto& s : sources[i]->shard_statuses()) {
            EXPECT_TRUE(owned.insert(s.shard_id).second) << s.shard_id << " owned twice";
            EXPECT_EQ(clink::aws::kinesis_shard_owner(s.shard_id, 2), i);
        }
        read.insert(caps[i].values.begin(), caps[i].values.end());
        // Each shard's records in the order the shard holds them.
        std::map<std::string, std::vector<std::string>> per_shard;
        for (const auto& v : caps[i].values) {
            per_shard[writer.where().at(v)].push_back(v);
        }
        for (const auto& [shard, values] : per_shard) {
            std::vector<std::string> written;
            for (const auto& v : writer.order()) {
                if (writer.where().at(v) == shard) {
                    written.push_back(v);
                }
            }
            EXPECT_EQ(values, written) << "shard " << shard << " out of order on subtask " << i;
        }
        sources[i]->close();
    }
    EXPECT_EQ(owned.size(), listed.size()) << "a listed shard had no owner";
    EXPECT_GE(listed.size(), 10U) << "the reshard did not happen";
    EXPECT_EQ(read, std::multiset<std::string>(writer.order().begin(), writer.order().end()));
}

// Latest mode against the emulator's AT_TIMESTAMP: records written before the
// start are skipped; a checkpoint taken before the first record restores to
// the same start, so records written while the job was down are read.
TEST(KinesisLive, LatestModeRestoredBeforeTheFirstRecordReadsTheOutage) {
    if (!kinesis_configured()) {
        GTEST_SKIP() << "set CLINK_KINESIS_TEST_ENDPOINT";
    }
    ensure_aws_initialized();
    Aws::Kinesis::KinesisClient admin(make_client_config(client_opts()));
    const std::string stream = unique_stream();
    ASSERT_TRUE(create_stream(admin, stream, 2));
    for (int i = 0; i < 5; ++i) {
        put_record(admin, stream, "k" + std::to_string(i), "before-" + std::to_string(i));
    }
    // Margin for the emulator's clock against this host's: arrival times come
    // from the service, the start time from the source's host.
    std::this_thread::sleep_for(std::chrono::milliseconds{1500});
    auto o = fast_source(stream);
    o.initial_position = "latest";
    InMemoryStateBackend backend;
    {
        KinesisSource first(o);
        first.open();
        Captured cap;
        drive({{&first, &cap}}, [&] { return first.listings() >= 2; }, 20000);
        EXPECT_TRUE(cap.values.empty()) << "latest read a record written before the start";
        first.snapshot_offset(backend, OperatorId{1}, clink::CheckpointId{1});
        first.close();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{1500});
    for (int i = 0; i < 6; ++i) {
        put_record(admin, stream, "k" + std::to_string(i), "outage-" + std::to_string(i));
    }
    KinesisSource second(o);
    ASSERT_TRUE(second.restore_offset(backend, OperatorId{1}));
    second.open();
    Captured cap;
    ASSERT_TRUE(drive({{&second, &cap}}, [&] { return cap.values.size() >= 6; }, 30000));
    drive({{&second, &cap}}, [] { return false; }, 500);
    second.close();
    delete_stream(admin, stream);
    std::set<std::string> got(cap.values.begin(), cap.values.end());
    EXPECT_EQ(got.size(), 6U);
    for (const auto& v : got) {
        EXPECT_EQ(v.rfind("outage-", 0), 0U) << v;
    }
}

// The emulator's errors through the SDK: a missing stream fails at once with
// what to check, not after a retry budget.
TEST(KinesisLive, AMissingStreamFailsAtOnce) {
    if (!kinesis_configured()) {
        GTEST_SKIP() << "set CLINK_KINESIS_TEST_ENDPOINT";
    }
    ensure_aws_initialized();
    KinesisSource src(fast_source("clink-it-missing-" + std::to_string(::getpid())));
    src.open();
    Captured cap;
    const auto started = std::chrono::steady_clock::now();
    const auto msg = expect_failure([&] { drive({{&src, &cap}}, [] { return false; }, 20000); });
    EXPECT_LT(std::chrono::steady_clock::now() - started, std::chrono::seconds{10});
    EXPECT_NE(msg.find("ResourceNotFoundException"), std::string::npos) << msg;
    EXPECT_NE(msg.find("stream exists"), std::string::npos) << msg;
}

// A checkpoint of a stream that was then deleted and created again under the
// same name: the restore refuses rather than read the new stream from the old
// positions.
TEST(KinesisLive, ARecreatedStreamIsRefused) {
    if (!kinesis_configured()) {
        GTEST_SKIP() << "set CLINK_KINESIS_TEST_ENDPOINT";
    }
    ensure_aws_initialized();
    Aws::Kinesis::KinesisClient admin(make_client_config(client_opts()));
    const std::string stream = unique_stream();
    ASSERT_TRUE(create_stream(admin, stream, 1));
    for (int i = 0; i < 4; ++i) {
        put_record(admin, stream, "k", "old-" + std::to_string(i));
    }
    InMemoryStateBackend backend;
    {
        KinesisSource first(fast_source(stream));
        first.open();
        Captured cap;
        ASSERT_TRUE(drive({{&first, &cap}}, [&] { return cap.values.size() >= 4; }, 20000));
        first.snapshot_offset(backend, OperatorId{1}, clink::CheckpointId{1});
        first.close();
    }
    delete_stream(admin, stream);
    for (int i = 0; i < 100; ++i) {
        if (create_stream(admin, stream, 1)) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
    }
    put_record(admin, stream, "k", "new-0");
    KinesisSource second(fast_source(stream));
    second.restore_offset(backend, OperatorId{1});
    second.open();
    Captured cap;
    const auto msg = expect_failure([&] { drive({{&second, &cap}}, [] { return false; }, 20000); });
    delete_stream(admin, stream);
    EXPECT_NE(msg.find("cannot continue"), std::string::npos) << msg;
    EXPECT_TRUE(cap.values.empty()) << "the new stream was read from the old positions";
}

// cancel() from another thread ends a source that is waiting on a caught-up
// shard at once, and later calls report it stopped.
TEST(KinesisLive, CancelStopsTheSourceAtOnce) {
    if (!kinesis_configured()) {
        GTEST_SKIP() << "set CLINK_KINESIS_TEST_ENDPOINT";
    }
    ensure_aws_initialized();
    Aws::Kinesis::KinesisClient admin(make_client_config(client_opts()));
    const std::string stream = unique_stream();
    ASSERT_TRUE(create_stream(admin, stream, 1));
    auto o = fast_source(stream);
    o.poll_interval = std::chrono::milliseconds{10000};
    KinesisSource src(o);
    src.open();
    Captured cap;
    drive({{&src, &cap}}, [&] { return src.listings() >= 1; }, 20000);
    std::thread canceller([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
        src.cancel();
    });
    const auto started = std::chrono::steady_clock::now();
    auto em = capturing(cap);
    while (src.produce(em)) {
    }
    canceller.join();
    EXPECT_LT(std::chrono::steady_clock::now() - started, std::chrono::seconds{2});
    EXPECT_FALSE(src.produce(em));
    src.close();
    delete_stream(admin, stream);
}
