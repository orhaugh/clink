// Kinesis / Firehose sinks: the partial-failure index collection (the core
// correctness point - resend ONLY the failed subset, never the whole batch) and
// the construction-time validation. These need the AWS SDK linked but NO live
// stream.

#include <cstdint>
#include <string>
#include <vector>

#include <aws/firehose/model/PutRecordBatchResponseEntry.h>
#include <aws/kinesis/model/PutRecordsResultEntry.h>
#include <gtest/gtest.h>

#include "clink/aws/firehose_sink.hpp"
#include "clink/aws/kinesis_sink.hpp"
#include "clink/core/record.hpp"
#include "clink/metrics/metrics_registry.hpp"

using clink::Batch;
using clink::aws::DlqPolicy;
using clink::aws::firehose_failed_indices;
using clink::aws::FirehoseSink;
using clink::aws::FirehoseSinkOptions;
using clink::aws::kinesis_failed_indices;
using clink::aws::KinesisSink;
using clink::aws::KinesisSinkOptions;

namespace {
std::uint64_t counter_value(const std::string& name) {
    for (const auto& [k, v] : clink::MetricsRegistry::global().snapshot().counters) {
        if (k == name) {
            return v;
        }
    }
    return 0;
}
}  // namespace

TEST(KinesisFailedIndices, PicksOnlyEntriesWithAnErrorCode) {
    Aws::Vector<Aws::Kinesis::Model::PutRecordsResultEntry> results(4);
    results[0].SetSequenceNumber("seq-0");  // success
    results[1].SetErrorCode("ProvisionedThroughputExceededException");
    results[2].SetSequenceNumber("seq-2");  // success
    results[3].SetErrorCode("InternalFailure");
    auto failed = kinesis_failed_indices(results);
    ASSERT_EQ(failed.size(), 2u);
    EXPECT_EQ(failed[0], 1u);
    EXPECT_EQ(failed[1], 3u);
}

TEST(KinesisFailedIndices, AllSuccessYieldsNoFailures) {
    Aws::Vector<Aws::Kinesis::Model::PutRecordsResultEntry> results(2);
    results[0].SetSequenceNumber("a");
    results[1].SetSequenceNumber("b");
    EXPECT_TRUE(kinesis_failed_indices(results).empty());
}

TEST(FirehoseFailedIndices, PicksOnlyEntriesWithAnErrorCode) {
    Aws::Vector<Aws::Firehose::Model::PutRecordBatchResponseEntry> results(3);
    results[0].SetRecordId("rid-0");  // success
    results[1].SetErrorCode("ServiceUnavailableException");
    results[2].SetRecordId("rid-2");  // success
    auto failed = firehose_failed_indices(results);
    ASSERT_EQ(failed.size(), 1u);
    EXPECT_EQ(failed[0], 1u);
}

TEST(KinesisSinkCtor, RequiresStreamAndClampsBatch) {
    KinesisSinkOptions o;  // no stream
    EXPECT_THROW(KinesisSink{std::move(o)}, std::runtime_error);

    KinesisSinkOptions o2;
    o2.stream = "my-stream";
    o2.batch_records = 9999;  // clamps to 500
    EXPECT_NO_THROW(KinesisSink{std::move(o2)});
}

TEST(FirehoseSinkCtor, RequiresDeliveryStreamAndClampsBatch) {
    FirehoseSinkOptions o;  // no delivery_stream
    EXPECT_THROW(FirehoseSink{std::move(o)}, std::runtime_error);

    FirehoseSinkOptions o2;
    o2.delivery_stream = "my-ds";
    o2.batch_records = 0;  // clamps to 500
    EXPECT_NO_THROW(FirehoseSink{std::move(o2)});
}

TEST(KinesisErrorClassify, TransientVsPermanent) {
    using clink::aws::classify_kinesis_error;
    using clink::aws::FailureClass;
    using E = Aws::Kinesis::KinesisErrors;
    // Permanent: a malformed request (oversized record, bad stream) replays the same.
    EXPECT_EQ(classify_kinesis_error(E::VALIDATION), FailureClass::Permanent);
    EXPECT_EQ(classify_kinesis_error(E::INVALID_ARGUMENT), FailureClass::Permanent);
    EXPECT_EQ(classify_kinesis_error(E::ACCESS_DENIED), FailureClass::Permanent);
    EXPECT_EQ(classify_kinesis_error(E::RESOURCE_NOT_FOUND), FailureClass::Permanent);
    // Transient: throttle / outage / internal -> retry (replay).
    EXPECT_EQ(classify_kinesis_error(E::PROVISIONED_THROUGHPUT_EXCEEDED), FailureClass::Transient);
    EXPECT_EQ(classify_kinesis_error(E::THROTTLING), FailureClass::Transient);
    EXPECT_EQ(classify_kinesis_error(E::SERVICE_UNAVAILABLE), FailureClass::Transient);
    EXPECT_EQ(classify_kinesis_error(E::INTERNAL_FAILURE), FailureClass::Transient);
}

TEST(KinesisDlq, DropPolicyDropsOversizedRecordBeforeBatching) {
    KinesisSinkOptions o;
    o.stream = "s";
    o.max_record_bytes = 8;  // tiny so a normal record is "oversized"
    o.dlq_policy = DlqPolicy::Drop;
    KinesisSink sink{std::move(o)};
    sink.open();  // builds a client; no network until flush (which we never reach)
    const std::string dropped =
        R"(clink_connector_dropped_records_total{connector="kinesis",direction="sink"})";
    const auto before = counter_value(dropped);
    Batch<std::string> b;
    b.emplace(std::string(64, 'x'));  // 64 bytes > 8 -> oversized -> dropped
    EXPECT_NO_THROW(sink.on_data(b));
    EXPECT_EQ(counter_value(dropped) - before, 1u);
}
