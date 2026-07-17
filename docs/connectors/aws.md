# AWS (Kinesis / Firehose / DynamoDB)

> A multi-service AWS connector module. It registers a Kinesis Data Streams source and sink, an Amazon Data Firehose sink, and a DynamoDB sink, all built against the AWS SDK for C++.

## Overview

The `clink::aws` module integrates three AWS services through the AWS SDK for C++: Kinesis Data Streams (source and sink), Amazon Data Firehose (sink), and DynamoDB (sink). It is a separate module from `clink::s3`, which targets only the SDK's s3 component. Every factory in this module operates on the `string` channel: each record is a string value, normally a JSON object string. The Kinesis source emits each record's `Data` as a `std::string`; the three sinks send each input string as one service record (the DynamoDB sink parses it as a JSON object and maps it to an item via `BatchWriteItem`). There is no Arrow or Parquet encoding on this path; the wire format is the raw string bytes, with DynamoDB additionally interpreting JSON scalars/objects/arrays into DynamoDB attribute types.

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| aws-sdk-cpp (core, kinesis, firehose, dynamodb) | Compiled from source on host and image | 1.11.795 |

The module links the per-component imported targets `aws-cpp-sdk-kinesis`, `aws-cpp-sdk-firehose`, `aws-cpp-sdk-dynamodb` and `aws-cpp-sdk-core` directly (see `impls/aws/CMakeLists.txt`). Arrow is not on this connector's data path and is not a dependency of the module itself.

## Enabling it

The CMake knob is `CLINK_WITH_AWS` with values `AUTO` (default), `ON`, or `OFF` (`CMakeLists.txt`). On `AUTO` the module configures only if the kinesis, firehose and dynamodb SDK components are all found; otherwise the target is silently not defined. On `ON` a missing component is a fatal configure error. On `OFF` the target is skipped entirely.

The module needs the kinesis, firehose and dynamodb SDK components specifically. The from-source SDK built by `scripts/install-system-deps.sh` (and baked into the `clink-build:latest` Debian image) is configured with `BUILD_ONLY="config;s3;transfer;identity-management;sts"`, which does **not** include those three components. As a result, `clink::aws` is not built in that image (confirmed by `scripts/sanitize_connectors_in_image.sh`). To build it, the kinesis/firehose/dynamodb components must be present, for example via a full aws-sdk-cpp install such as the macOS Homebrew package, against which the module is validated.

```bash
cmake -S . -B build -DCLINK_WITH_AWS=ON
cmake --build build -j
```

## Factories

| Factory name | Direction | Record type |
| --- | --- | --- |
| `kinesis_source` | Source | `std::string` |
| `kinesis_sink` | Sink | `std::string` |
| `firehose_sink` | Sink | `std::string` |
| `dynamodb_sink` | Sink | `std::string` |

All four are registered on the `string` channel in `impls/aws/src/register_factories.cpp`.

## Configuration

All factories share `region` and `endpoint_override` for the AWS client (`endpoint_override` targets LocalStack or a custom endpoint). The client also applies fixed timeouts and a retry count from `AwsClientOptions` (3000 ms connect, 10000 ms request, 3 SDK-level retries); those are not exposed as connector params.

### `kinesis_source`

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `stream` | Yes | (none) | Stream name or ARN. An ARN (`arn:` prefix) sets `StreamARN`, else `StreamName`. |
| `initial_position` | No | `trim_horizon` | Starting position for shards present at open. Must be `trim_horizon` or `latest`; any other value is rejected at construction. |
| `max_records_per_poll` | No | `1000` | `GetRecords` `Limit`. Coerced to `1000` if outside `1..10000`. |
| `poll_interval_ms` | No | `250` | Idle backoff (ms) after a full pass over assigned shards produces nothing. |
| `region` | No | SDK default | AWS region. |
| `endpoint_override` | No | (none) | Custom endpoint URL (e.g. LocalStack). |

### `kinesis_sink`

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `stream` | Yes | (none) | Stream name or ARN. ARN sets `StreamARN`, else `StreamName`. |
| `partition_key` | No | (none) | Record JSON field whose scalar value is used as the Kinesis `PartitionKey`. When absent, empty, or non-scalar, a rotating counter is used to spread records across shards. A value over 256 chars is truncated. |
| `batch_records` | No | `500` | Records per `PutRecords` call. Coerced to `500` if 0 or above 500. |
| `max_retries` | No | `8` | Failed-subset resend attempts. Clamped to `0..20`. |
| `dlq` | No | `fail` | `fail` throws on an oversized record (over the 1 MiB per-record limit); `drop` routes it to the bad-record path and continues. |
| `linger_ms` | No | `0` | Flush a partial batch once it is this old (0 disables linger). |
| `region` | No | SDK default | AWS region. |
| `endpoint_override` | No | (none) | Custom endpoint URL. |

The sink also flushes on an internal byte threshold (just under the 5 MiB request cap); this is not a configurable param.

### `firehose_sink`

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `delivery_stream` | Yes | (none) | Firehose delivery stream name. |
| `delimiter` | No | `\n` | Appended to each record's `Data` so the destination can split records. Empty string appends nothing. |
| `batch_records` | No | `500` | Records per `PutRecordBatch` call. Coerced to `500` if 0 or above 500. |
| `max_retries` | No | `8` | Failed-subset resend attempts. Clamped to `0..20`. |
| `linger_ms` | No | `0` | Flush a partial batch once it is this old (0 disables linger). |
| `region` | No | SDK default | AWS region. |
| `endpoint_override` | No | (none) | Custom endpoint URL. |

The sink also flushes on an internal byte threshold (just under the 4 MiB request cap); this is not a configurable param.

### `dynamodb_sink`

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `table` | Yes | (none) | Target table name. |
| `partition_key` | Yes | (none) | Name of the partition-key attribute, which must be present and non-null in every record. |
| `sort_key` | No | (none) | Name of the sort-key attribute for composite-key tables. |
| `batch_records` | No | `25` | Items per `BatchWriteItem` call. Coerced to `25` if 0 or above 25. |
| `max_retries` | No | `8` | `UnprocessedItems` resubmit attempts. Clamped to `0..20`. |
| `linger_ms` | No | `0` | Flush a partial batch once it is this old (0 disables linger). |
| `region` | No | SDK default | AWS region. |
| `endpoint_override` | No | (none) | Custom endpoint URL. |

Authentication is handled by the AWS SDK's default credential provider chain (environment, profile, instance role, and so on). The connector exposes no explicit credential options; `endpoint_override` is the LocalStack hook, and with LocalStack any credentials are accepted by the service.

## SQL usage

All four are mapped in `src/sql/physical_plan.cpp`. The Kinesis source binds `connector='kinesis'` to `kinesis_source` with a `json_string_to_row` bridge; the sinks bind `connector='kinesis'`, `connector='firehose'` and `connector='dynamodb'` to `kinesis_sink`, `firehose_sink` and `dynamodb_sink` respectively, each with a `row_to_json_string` bridge. All are at-least-once: `delivery_guarantee='exactly_once'` is rejected for all three sinks, and `mode='upsert'` is rejected for all three (the DynamoDB put is already an upsert by primary key, so setting `partition_key`/`sort_key` is the supported route).

```sql
-- Source: read a Kinesis Data Stream
CREATE TABLE clicks (
  user_id BIGINT,
  url     STRING
) WITH (
  connector = 'kinesis',
  format    = 'json',
  stream    = 'clicks-stream',
  region    = 'eu-west-2',
  initial_position = 'trim_horizon'
);

-- Sink: write to a DynamoDB table keyed by user_id
CREATE TABLE click_counts (
  user_id BIGINT,
  n       BIGINT
) WITH (
  connector     = 'dynamodb',
  table         = 'click-counts',
  partition_key = 'user_id',
  region        = 'eu-west-2'
);
```

## Example

Programmatic use of the Kinesis source and sink, following `impls/aws/tests/test_kinesis_live.cpp`:

```cpp
#include "clink/aws/aws_client.hpp"
#include "clink/aws/kinesis_sink.hpp"
#include "clink/aws/kinesis_source.hpp"

using clink::aws::AwsClientOptions;
using clink::aws::KinesisSink;
using clink::aws::KinesisSinkOptions;
using clink::aws::KinesisSource;
using clink::aws::KinesisSourceOptions;

AwsClientOptions client;
client.region = "eu-west-2";
// client.endpoint_override = "http://localhost:4566";  // LocalStack

// Sink: PutRecords into a stream.
KinesisSinkOptions sopts;
sopts.stream = "events-stream";
sopts.partition_key = "user_id";  // record field used as the Kinesis PartitionKey
sopts.client = client;

KinesisSink sink(std::move(sopts));
sink.open();
clink::Batch<std::string> b;
b.emplace(R"({"user_id":"u1","v":42})");
sink.on_data(b);
sink.flush();  // also flushed on a checkpoint barrier

// Source: read the stream; each subtask owns a modulo-slice of the shards.
KinesisSourceOptions ropts;
ropts.stream = "events-stream";
ropts.initial_position = "trim_horizon";
ropts.client = client;

KinesisSource src(std::move(ropts));
src.open();
// drive produce(emitter) in the runner loop; snapshot_offset / restore_offset
// checkpoint and resume the per-shard sequence number.
```

The factories are also reachable through the registry by their registered names (`kinesis_sink`, `kinesis_source`, `firehose_sink`, `dynamodb_sink`) on the `string` channel after `clink::aws::install(reg)`.

## Delivery semantics

- **`kinesis_source`**: at-least-once. Each subtask owns a fixed modulo-slice of the stream's shards; the last `SequenceNumber` per shard is checkpointed as operator state keyed by `ShardId` and resumed with `AFTER_SEQUENCE_NUMBER`. A replay can re-read records committed after the last checkpoint.
- **`kinesis_sink`**: at-least-once. Kinesis has no producer dedup key. `PutRecords` is partial-success; on a partial failure only the failed entries (by response index) are resent, so already-committed records are not re-sent within a call, but a barrier replay can duplicate.
- **`firehose_sink`**: at-least-once, with no partition key and no ordering guarantee. `PutRecordBatch` is partial-success; only failed entries are resent.
- **`dynamodb_sink`**: at-least-once, and effectively-once when the primary key is stable and deterministic from the record, because `PutItem` is an upsert and a replay overwrites the same item. A batch is deduped by primary key (last write wins) before submission, and `UnprocessedItems` are resubmitted with exponential backoff.

None of the sinks offer two-phase-commit exactly-once. On exhausted retries each sink throws so the job replays from the last checkpoint rather than silently dropping records.

## Limitations

- Module only configures when the kinesis, firehose and dynamodb SDK components are present; the prebuilt `clink-build:latest` image ships only the s3 component, so the module is not built there.
- String channel only; every record is a string (JSON object string on the SQL path). No int64 channel, no Arrow/Parquet encoding on this path.
- Firehose, DynamoDB and the Kinesis source/sink are the four registered factories; there is no Kinesis Firehose source and no DynamoDB source.
- `dynamodb_sink`: each record must be a JSON object; the partition key (and sort key, if configured) must be present, non-null and scalar, or the record is rejected. `BatchWriteItem` is capped at 25 items per call. JSON numbers are mapped to DynamoDB `N`.
- `kinesis_sink`: a record over the 1 MiB per-record limit poisons the whole `PutRecords` batch under `dlq=fail`; use `dlq=drop` to route oversized records to the bad-record path instead.
- `firehose_sink`: no partition key and no ordering guarantee; the destination must split concatenated records using the appended delimiter.
- `kinesis_source`: shard assignment is fixed at open. A reshard is picked up by re-listing shards when an owned shard closes, but a parent shard owned by another subtask is not coordinated, so during an active reshard records may be read out of sequence-number order across the split. Steady-state (no active resharding) is unaffected.

## Testing

A live integration test (`impls/aws/tests/test_kinesis_live.cpp`) runs against LocalStack and is skipped unless `CLINK_KINESIS_TEST_ENDPOINT` is set. The remaining test files (`test_factory_registration.cpp`, `test_dynamodb_item.cpp`, `test_kinesis_firehose_sink.cpp`, `test_kinesis_source.cpp`) are in-process and exercise factory registration, JSON-to-attribute mapping, retry/failed-subset logic and shard assignment without a live service.

Stand up LocalStack via `docker/integration-services.yml` (image `localstack/localstack:3`, `SERVICES=kinesis`, port 4566), then point the test at it:

```bash
docker compose -f docker/integration-services.yml up -d localstack
export CLINK_KINESIS_TEST_ENDPOINT=http://localhost:4566
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
ctest --test-dir build -L aws
```

LocalStack accepts any credentials; the dummy keys above just let the SDK sign the requests. The live test proves a sink-to-source round-trip delivers every record and that a sequence-number checkpoint resumes a reader with no gaps and no re-reads.
