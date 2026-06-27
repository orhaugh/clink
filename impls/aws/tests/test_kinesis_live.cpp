// Kinesis LIVE integration test against LocalStack. SKIPPED unless
// CLINK_KINESIS_TEST_ENDPOINT is set (e.g. http://localhost:4566 from
// docker/integration-services.yml). Also needs AWS creds in the env for the SDK
// to sign (LocalStack accepts any; AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=
// test AWS_DEFAULT_REGION=us-east-1). Proves: a sink->source round-trip delivers
// every record, and a sequence-number checkpoint resumes a reader with no gaps.

#include <chrono>
#include <cstdlib>
#include <set>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <aws/kinesis/KinesisClient.h>
#include <aws/kinesis/model/CreateStreamRequest.h>
#include <aws/kinesis/model/DeleteStreamRequest.h>
#include <aws/kinesis/model/DescribeStreamRequest.h>
#include <aws/kinesis/model/StreamStatus.h>
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
bool create_stream(Aws::Kinesis::KinesisClient& c, const std::string& name) {
    Aws::Kinesis::Model::CreateStreamRequest req;
    req.SetStreamName(name);
    req.SetShardCount(1);
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
