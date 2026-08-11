// AWS-family connector factory registration (Kinesis, Firehose, DynamoDB).

#include <chrono>
#include <cstddef>
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
        .version = "1",
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
        .timeout_options = {"poll_interval_ms"},
        .available_in_sql = true,
        .limitations = {"resharding while a job is running is not rebalanced until restart"},
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
        .timeout_options = {},
        .available_in_sql = true,
        .limitations =
            {"no producer dedup: a replayed batch is re-appended to the delivery stream"},
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
        // PutItem overwrites by key, so replays collapse - conditional on
        // the user's partition_key actually identifying the record.
        .delivery = clink::connectors::DeliveryGuarantee::EffectivelyOnceIdempotent,
        .transactional = false,
        .idempotency_key_option = "partition_key",
        .auth_methods = {"aws-credentials-chain"},
        .tls = true,
        .backpressure = true,
        .retries = true,
        .timeout_options = {},
        .available_in_sql = true,
        .limitations = {"UnprocessedItems are retried max_retries times, then the flush fails"},
    });

    using clink::plugin::BuildContext;

    // dynamodb_sink: upsert JSON-object records into a DynamoDB table via
    // BatchWriteItem. At-least-once; effectively-once when the primary key is
    // stable (PutItem overwrites). Params:
    //   table (required)          - target table
    //   partition_key (required)  - the table's partition-key attribute name
    //   sort_key                  - the sort-key attribute name (composite tables)
    //   region, endpoint_override - endpoint_override targets LocalStack/custom
    //   batch_records (default 25; clamped to [1, 25])
    //   max_retries (default 8)   - UnprocessedItems resubmit attempts
    reg.register_sink<std::string>(
        "dynamodb_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            DynamoDbSinkOptions o;
            o.table = ctx.param_or("table");
            o.partition_key = ctx.param_or("partition_key");
            o.sort_key = ctx.param_or("sort_key", "");
            o.client = client_options_from(ctx);
            o.batch_records = static_cast<std::size_t>(ctx.param_int64_or("batch_records", 25));
            o.max_retries = static_cast<int>(ctx.param_int64_or("max_retries", 8));
            o.max_age = std::chrono::milliseconds{ctx.param_int64_or("linger_ms", 0)};
            o.name = "dynamodb_sink";
            return std::make_shared<DynamoDbSink>(std::move(o));
        });

    // kinesis_sink: PutRecords into a Kinesis Data Stream. At-least-once (no
    // producer dedup key). Params:
    //   stream (required)         - stream name or ARN
    //   partition_key             - record field used as the Kinesis PartitionKey
    //                               (else a rotating counter spreads load)
    //   region, endpoint_override
    //   batch_records (default 500; clamped to [1, 500])
    //   max_retries (default 8)   - failed-subset resend attempts
    reg.register_sink<std::string>(
        "kinesis_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            KinesisSinkOptions o;
            o.stream = ctx.param_or("stream");
            o.partition_key = ctx.param_or("partition_key", "");
            o.client = client_options_from(ctx);
            o.batch_records = static_cast<std::size_t>(ctx.param_int64_or("batch_records", 500));
            o.max_retries = static_cast<int>(ctx.param_int64_or("max_retries", 8));
            o.dlq_policy =
                ctx.param_or("dlq", "fail") == "drop" ? DlqPolicy::Drop : DlqPolicy::Fail;
            o.max_age = std::chrono::milliseconds{ctx.param_int64_or("linger_ms", 0)};
            o.name = "kinesis_sink";
            return std::make_shared<KinesisSink>(std::move(o));
        });

    // firehose_sink: PutRecordBatch into an Amazon Data Firehose delivery
    // stream. At-least-once; no partition key, no ordering. Params:
    //   delivery_stream (required)
    //   delimiter (default "\n")  - appended to each record's Data (empty = none)
    //   region, endpoint_override
    //   batch_records (default 500; clamped to [1, 500])
    //   max_retries (default 8)   - failed-subset resend attempts
    reg.register_sink<std::string>(
        "firehose_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            FirehoseSinkOptions o;
            o.delivery_stream = ctx.param_or("delivery_stream");
            o.delimiter = ctx.param_or("delimiter", "\n");
            o.client = client_options_from(ctx);
            o.batch_records = static_cast<std::size_t>(ctx.param_int64_or("batch_records", 500));
            o.max_retries = static_cast<int>(ctx.param_int64_or("max_retries", 8));
            o.max_age = std::chrono::milliseconds{ctx.param_int64_or("linger_ms", 0)};
            o.name = "firehose_sink";
            return std::make_shared<FirehoseSink>(std::move(o));
        });

    // kinesis_source: read a Kinesis Data Stream (ListShards + GetRecords). Each
    // subtask owns a modulo-slice of the shards; per-shard SequenceNumber is
    // checkpointed (at-least-once). Params:
    //   stream (required)         - stream name or ARN
    //   initial_position ("trim_horizon" [default] | "latest")
    //   region, endpoint_override
    //   max_records_per_poll (default 1000; GetRecords Limit)
    //   poll_interval_ms (default 250) - idle backoff
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
            o.name = "kinesis_source";
            return std::make_shared<KinesisSource>(std::move(o));
        });
}

}  // namespace clink::aws
