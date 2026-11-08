// AWS-family connector factory registration (Kinesis, Firehose, DynamoDB).

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/aws/aws_client.hpp"
#include "clink/aws/dynamodb_sink.hpp"
#include "clink/aws/firehose_sink.hpp"
#include "clink/aws/install.hpp"
#include "clink/aws/kinesis_sink.hpp"
#include "clink/aws/kinesis_source.hpp"
#include "clink/connectors/capability.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/plugin/plugin.hpp"

namespace clink::aws {

namespace {

// Read the shared AWS client options (region / endpoint_override for LocalStack)
// off a BuildContext.
AwsClientOptions client_options_from(const clink::plugin::BuildContext& ctx) {
    AwsClientOptions o;
    const auto region = ctx.param_or("region", "");
    if (!region.empty()) {
        o.region = region;
    }
    const auto endpoint = ctx.param_or("endpoint_override", "");
    if (!endpoint.empty()) {
        o.endpoint_override = endpoint;
    }
    return o;
}

}  // namespace

void install(clink::plugin::PluginRegistry& reg) {
    // Capability records. One per user-visible connector, not per impl
    // directory: kinesis, firehose and dynamodb are distinct systems with
    // distinct guarantees that happen to share the AWS SDK.
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "kinesis",
        .version = "2",
        .is_source = true,
        .is_sink = true,
        .build_dependencies = {"aws-sdk-cpp"},
        .runtime_dependencies = {"kinesis data stream"},
        .formats = {"text", "json", "bytes"},
        .boundedness = clink::connectors::Boundedness::Unbounded,
        // The source checkpoints a per-shard SequenceNumber and resumes
        // from it, so the clink checkpoint is the read position.
        .replayable = true,
        .offset_model = clink::connectors::OffsetModel::OpaqueToken,
        .checkpoint_integrated = true,
        // PutRecords with no producer dedup key: a replayed batch is
        // re-appended.
        .delivery = clink::connectors::DeliveryGuarantee::AtLeastOnce,
        .transactional = false,
        .partition_discovery = true,
        .auth_methods = {"aws-credentials-chain"},
        .tls = true,
        .backpressure = true,
        .retries = true,
        .timeout_options = {"poll_interval_ms",
                            "discovery_interval_ms",
                            "retry_timeout_ms",
                            "connect_timeout_ms",
                            "request_timeout_ms"},
        .available_in_sql = true,
        .limitations = {"source: records of one hash key keep their order across a split or "
                        "merge only at parallelism 1; above it, order holds within a shard",
                        "source: restoring at another parallelism needs every previous "
                        "subtask's source rows at every new subtask (file:// hands them over; "
                        "the source refuses a restore that lacks them)",
                        "source: a shard the source tracks but has not read yet is not checked "
                        "for records lost to retention before its first record",
                        "sink: at-least-once; a record resent after a lost reply or replayed "
                        "after a restart can be stored twice, and Kinesis keeps no producer "
                        "identity",
                        "sink: records sharing a partition key are not promised to arrive in "
                        "order: PutRecords keeps no order within a request, and failed entries "
                        "are resent after later ones were stored",
                        "sink: without partition_key, records are keyed by a number each sink "
                        "instance counts, so a replayed record can land on another shard than "
                        "its first copy",
                        "sink: a record over max_record_bytes, or without a usable partition key "
                        "under partition_key, fails the sink, or goes to the dead-letter path "
                        "under dlq='drop'",
                        "the sink must be the only sink on its chain: Dag::add_sink refuses "
                        "another sink beside it"},
    });
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "firehose",
        .version = "1",
        .is_source = false,
        .is_sink = true,
        .build_dependencies = {"aws-sdk-cpp"},
        .runtime_dependencies = {"kinesis firehose delivery stream"},
        .formats = {"text", "json", "bytes"},
        .boundedness = clink::connectors::Boundedness::Unbounded,
        .replayable = false,
        .offset_model = clink::connectors::OffsetModel::None,
        .checkpoint_integrated = true,
        .delivery = clink::connectors::DeliveryGuarantee::AtLeastOnce,
        .transactional = false,
        .auth_methods = {"aws-credentials-chain"},
        .tls = true,
        .backpressure = true,
        .retries = true,
        .timeout_options = {"connect_timeout_ms", "request_timeout_ms", "retry_timeout_ms"},
        .available_in_sql = true,
        .limitations = {"at-least-once into Firehose: a record resent after a lost reply or "
                        "replayed after a restart can reach the destination twice; no producer "
                        "dedup and no ordering",
                        "a checkpoint covers acceptance by Firehose, not delivery to the "
                        "destination, which the service does after its own buffering and retries",
                        "a record over 1,000 KiB with its delimiter fails the sink, and the job's "
                        "restart replays it",
                        "the sink must be the only sink on its chain: Dag::add_sink refuses "
                        "another sink beside it"},
    });
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "dynamodb",
        .version = "1",
        .is_source = false,
        .is_sink = true,
        .build_dependencies = {"aws-sdk-cpp"},
        .runtime_dependencies = {"dynamodb table"},
        .formats = {"json"},
        .boundedness = clink::connectors::Boundedness::Unbounded,
        .replayable = false,
        .offset_model = clink::connectors::OffsetModel::None,
        .checkpoint_integrated = true,
        // PutItem overwrites by key, so a replayed record rewrites its item -
        // conditional on the table's key identifying the record, and on every
        // record of a key reaching one sink subtask in order.
        .delivery = clink::connectors::DeliveryGuarantee::EffectivelyOnceIdempotent,
        .transactional = false,
        .idempotency_key_option = "partition_key",
        .auth_methods = {"aws-credentials-chain"},
        .tls = true,
        .backpressure = true,
        .retries = true,
        .timeout_options = {"connect_timeout_ms", "request_timeout_ms", "retry_timeout_ms"},
        .available_in_sql = true,
        .limitations = {"at-least-once writes: a restart replays every record after the last "
                        "completed checkpoint, and an item can show an older record's value until "
                        "the replay passes the newer one",
                        "the final state is the last record per item only when every record of an "
                        "item reaches one sink subtask in order; two subtasks or an outside writer "
                        "can leave an older value",
                        "S and N keys only: a binary (B) key table is refused at open",
                        "numbers keep at most 38 significant digits, as DynamoDB does; a record "
                        "with a wider number is refused",
                        "a refused record or a failed write fails the sink, and the job's restart "
                        "replays the record",
                        "the sink must be the only sink on its chain: Dag::add_sink refuses "
                        "another sink beside it"},
    });

    using clink::plugin::BuildContext;

    // dynamodb_sink: JSON-object records written as items via BatchWriteItem,
    // after DescribeTable has given the table's key schema. At-least-once; a
    // replay rewrites the same items. Params:
    //   table (required)          - target table name or ARN
    //   partition_key (required)  - the table's partition-key attribute name
    //   sort_key                  - the sort-key attribute name (required when the table has one)
    //   region, endpoint_override - endpoint_override targets LocalStack/custom
    //   connect_timeout_ms (default 3000), request_timeout_ms (default 10000)
    //   batch_records (default 25; 1 to 25)
    //   max_retries (default 8; 0 to 20) - retries in a row that write nothing
    //   retry_timeout_ms (default 60000) - the most one batch's retries may take
    //   linger_ms (default 0)     - flush a partial batch this old
    reg.register_sink<std::string>(
        "dynamodb_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            // Each number is range-checked here, before it is narrowed.
            const auto ranged =
                [&ctx](const char* key, std::int64_t fallback, std::int64_t lo, std::int64_t hi) {
                    const auto v = ctx.param_int64_or(key, fallback);
                    if (v < lo || v > hi) {
                        throw std::runtime_error(std::string{"dynamodb_sink: '"} + key +
                                                 "' must be " + std::to_string(lo) + " to " +
                                                 std::to_string(hi) + "; it is " +
                                                 std::to_string(v));
                    }
                    return v;
                };
            DynamoDbSinkOptions o;
            o.table = ctx.param_or("table");
            o.partition_key = ctx.param_or("partition_key");
            o.sort_key = ctx.param_or("sort_key", "");
            o.client = client_options_from(ctx);
            o.client.connect_timeout_ms = static_cast<int>(
                ranged("connect_timeout_ms", o.client.connect_timeout_ms, 1, 600000));
            o.client.request_timeout_ms = static_cast<int>(
                ranged("request_timeout_ms", o.client.request_timeout_ms, 1, 600000));
            o.batch_records = static_cast<std::size_t>(ranged("batch_records", 25, 1, 25));
            o.max_retries =
                static_cast<int>(ranged("max_retries", 8, 0, DynamoDbSink::kMaxRetries));
            o.retry_timeout =
                std::chrono::milliseconds{ranged("retry_timeout_ms", 60000, 1, 3600000)};
            o.max_age = std::chrono::milliseconds{ranged("linger_ms", 0, 0, 3600000)};
            o.name = "dynamodb_sink";
            return std::make_shared<DynamoDbSink>(std::move(o));
        });

    // kinesis_sink: PutRecords into a Kinesis Data Stream. At-least-once (no
    // producer dedup key, no per-key order). Params:
    //   stream (required)         - stream name or ARN
    //   partition_key             - the record's top-level JSON member holding its
    //                               partition key (string, number or boolean);
    //                               unset numbers records per sink instance
    //   region, endpoint_override
    //   connect_timeout_ms (default 3000), request_timeout_ms (default 10000)
    //   batch_records (default 500; 1 to 500)
    //   max_record_bytes (default 1048576; 1 to 10485760) - data plus key, the
    //                               stream's maximum record size
    //   max_request_bytes (default the larger of 5242880 and max_record_bytes;
    //                               max_record_bytes to 10485760)
    //   max_retries (default 8; 0 to 20) - retries in a row that store nothing
    //   retry_timeout_ms (default 60000) - the most one request's retries may take
    //   linger_ms (default 0)     - flush a partial batch this old
    //   dlq ("fail" [default] | "drop") - a record refused before sending
    reg.register_sink<std::string>(
        "kinesis_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            // Each number is range-checked here, before it is narrowed.
            const auto ranged =
                [&ctx](const char* key, std::int64_t fallback, std::int64_t lo, std::int64_t hi) {
                    const auto v = ctx.param_int64_or(key, fallback);
                    if (v < lo || v > hi) {
                        throw std::runtime_error(std::string{"kinesis_sink: '"} + key +
                                                 "' must be " + std::to_string(lo) + " to " +
                                                 std::to_string(hi) + "; it is " +
                                                 std::to_string(v));
                    }
                    return v;
                };
            KinesisSinkOptions o;
            o.stream = ctx.param_or("stream");
            o.partition_key = ctx.param_or("partition_key", "");
            o.client = client_options_from(ctx);
            o.client.connect_timeout_ms = static_cast<int>(
                ranged("connect_timeout_ms", o.client.connect_timeout_ms, 1, 600000));
            o.client.request_timeout_ms = static_cast<int>(
                ranged("request_timeout_ms", o.client.request_timeout_ms, 1, 600000));
            o.batch_records = static_cast<std::size_t>(ranged(
                "batch_records", 500, 1, static_cast<std::int64_t>(kKinesisMaxBatchRecords)));
            o.max_record_bytes = static_cast<std::size_t>(
                ranged("max_record_bytes",
                       static_cast<std::int64_t>(kKinesisDefaultMaxRecordBytes),
                       1,
                       static_cast<std::int64_t>(kKinesisMaxRecordBytes)));
            o.max_request_bytes = static_cast<std::size_t>(
                ranged("max_request_bytes",
                       static_cast<std::int64_t>(
                           std::max(kKinesisDefaultRequestBytes, o.max_record_bytes)),
                       static_cast<std::int64_t>(o.max_record_bytes),
                       static_cast<std::int64_t>(kKinesisMaxRequestBytes)));
            o.max_retries = static_cast<int>(ranged("max_retries", 8, 0, KinesisSink::kMaxRetries));
            o.retry_timeout =
                std::chrono::milliseconds{ranged("retry_timeout_ms", 60000, 1, 3600000)};
            o.max_age = std::chrono::milliseconds{ranged("linger_ms", 0, 0, 3600000)};
            const auto dlq = ctx.param_or("dlq", "fail");
            if (dlq != "fail" && dlq != "drop") {
                throw std::runtime_error("kinesis_sink: 'dlq' must be 'fail' or 'drop'; it is '" +
                                         dlq + "'");
            }
            o.dlq_policy = dlq == "drop" ? DlqPolicy::Drop : DlqPolicy::Fail;
            o.name = "kinesis_sink";
            return std::make_shared<KinesisSink>(std::move(o));
        });

    // firehose_sink: PutRecordBatch into an Amazon Data Firehose delivery
    // stream. At-least-once into Firehose; no partition key, no ordering. Params:
    //   delivery_stream (required)
    //   delimiter (default "\n")  - appended to each record's data (empty = none)
    //   region, endpoint_override
    //   connect_timeout_ms (default 3000), request_timeout_ms (default 10000)
    //   batch_records (default 500; 1 to 500)
    //   max_retries (default 8; 0 to 20) - retries in a row that accept nothing
    //   retry_timeout_ms (default 60000) - the most one request's retries may take
    //   linger_ms (default 0)     - flush a partial batch this old
    reg.register_sink<std::string>(
        "firehose_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            // Each number is range-checked here, before it is narrowed.
            const auto ranged =
                [&ctx](const char* key, std::int64_t fallback, std::int64_t lo, std::int64_t hi) {
                    const auto v = ctx.param_int64_or(key, fallback);
                    if (v < lo || v > hi) {
                        throw std::runtime_error(std::string{"firehose_sink: '"} + key +
                                                 "' must be " + std::to_string(lo) + " to " +
                                                 std::to_string(hi) + "; it is " +
                                                 std::to_string(v));
                    }
                    return v;
                };
            FirehoseSinkOptions o;
            o.delivery_stream = ctx.param_or("delivery_stream");
            o.delimiter = ctx.param_or("delimiter", "\n");
            o.client = client_options_from(ctx);
            o.client.connect_timeout_ms = static_cast<int>(
                ranged("connect_timeout_ms", o.client.connect_timeout_ms, 1, 600000));
            o.client.request_timeout_ms = static_cast<int>(
                ranged("request_timeout_ms", o.client.request_timeout_ms, 1, 600000));
            o.batch_records = static_cast<std::size_t>(ranged("batch_records", 500, 1, 500));
            o.max_retries =
                static_cast<int>(ranged("max_retries", 8, 0, FirehoseSink::kMaxRetries));
            o.retry_timeout =
                std::chrono::milliseconds{ranged("retry_timeout_ms", 60000, 1, 3600000)};
            o.max_age = std::chrono::milliseconds{ranged("linger_ms", 0, 0, 3600000)};
            o.name = "firehose_sink";
            return std::make_shared<FirehoseSink>(std::move(o));
        });

    // kinesis_source: read a Kinesis Data Stream (ListShards + GetRecords). Shard
    // s belongs to subtask kinesis_shard_owner(s, parallelism); each owned shard's
    // last emitted record is checkpointed (at-least-once). Params:
    //   stream (required)         - stream name or ARN
    //   initial_position ("trim_horizon" [default] | "latest")
    //   region, endpoint_override
    //   max_records_per_poll (default 1000; GetRecords Limit, 1..10000)
    //   poll_interval_ms (default 250) - a caught-up shard's wait between reads
    //   discovery_interval_ms (default 10000) - ListShards cadence
    //   retry_timeout_ms (default 300000) - how long one call may keep failing
    //     with throttling or transient errors before the task fails
    reg.register_source<std::string>(
        "kinesis_source", [](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
            KinesisSourceOptions o;
            o.stream = ctx.param_or("stream");
            o.initial_position = ctx.param_or("initial_position", "trim_horizon");
            o.subtask_idx = ctx.subtask_idx;
            o.parallelism = ctx.parallelism;
            o.client = client_options_from(ctx);
            o.max_records_per_poll =
                static_cast<int>(ctx.param_int64_or("max_records_per_poll", 1000));
            o.poll_interval =
                std::chrono::milliseconds{ctx.param_int64_or("poll_interval_ms", 250)};
            o.discovery_interval =
                std::chrono::milliseconds{ctx.param_int64_or("discovery_interval_ms", 10000)};
            o.retry_timeout =
                std::chrono::milliseconds{ctx.param_int64_or("retry_timeout_ms", 300000)};
            o.name = "kinesis_source";
            return std::make_shared<KinesisSource>(std::move(o));
        });
}

}  // namespace clink::aws
