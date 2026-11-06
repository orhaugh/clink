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

The module needs the kinesis, firehose and dynamodb SDK components specifically. The from-source SDK that `scripts/install-system-deps.sh` builds (and the toolchain image bakes in) is configured with `BUILD_ONLY="config;s3;transfer;identity-management;sts;kinesis;firehose;dynamodb"`, so the module builds there: CI's build of commit 6ad5168a on 2026-11-05 built `clink_aws_tests` and passed its 22 in-process cases, skipping the 2 that need LocalStack. A host build needs the same components, for example the macOS Homebrew aws-sdk-cpp, which ships them all; an SDK built with a narrower `BUILD_ONLY` leaves the module out under `AUTO`.

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

All factories share `region` and `endpoint_override` for the AWS client (`endpoint_override` targets LocalStack or a custom endpoint). The client also applies fixed timeouts and a retry count from `AwsClientOptions` (3000 ms connect, 10000 ms request, 3 SDK-level retries); those are not exposed as connector params. The Kinesis source turns the SDK's retries off and paces and bounds its own (see [the Kinesis source](#the-kinesis-source)).

### `kinesis_source`

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `stream` | Yes | (none) | Stream name or ARN. An ARN (`arn:` prefix) sets `StreamARN`, else `StreamName`. |
| `initial_position` | No | `trim_horizon` | Where shards start on a fresh start: `trim_horizon` (each shard's oldest retained record) or `latest` (records arriving from the source's first start). Any other value is rejected at construction. |
| `max_records_per_poll` | No | `1000` | `GetRecords` `Limit`, `1..10000`; a value outside is rejected at construction. |
| `poll_interval_ms` | No | `250` | How long a caught-up shard waits before its next `GetRecords`. A shard returning records is read again after 200 ms, the service's five reads a second per shard. |
| `discovery_interval_ms` | No | `10000` | `ListShards` cadence, at least 100. An owned shard read to its end lists the stream sooner (within 500 ms, or the interval if shorter). |
| `retry_timeout_ms` | No | `300000` | How long one call (a listing, or one shard's reads) may keep failing with throttling or transient errors before the task fails. |
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

A NaN or infinite double in a row reaches this sink through the `row_to_json_string` bridge as `null`, or as `"NaN"`, `"Infinity"` or `"-Infinity"` when the sink table sets `json_non_finite='string'`; the sink handles it as it handles any null or string value. See [NaN and infinity in JSON output](README.md#nan-and-infinity-in-json-output).

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

- **`kinesis_source`**: at-least-once. Each shard has one owning subtask; the last record handed downstream from each shard is checkpointed, and a restore resumes right after it once the service has shown that record is still in the stream. A replay re-reads the records emitted after the last completed checkpoint. See [the Kinesis source](#the-kinesis-source) for ordering, start positions, rescaling and failures.
- **`kinesis_sink`**: at-least-once. Kinesis has no producer dedup key. `PutRecords` is partial-success; on a partial failure only the failed entries (by response index) are resent, so already-committed records are not re-sent within a call, but a barrier replay can duplicate.
- **`firehose_sink`**: at-least-once, with no partition key and no ordering guarantee. `PutRecordBatch` is partial-success; only failed entries are resent.
- **`dynamodb_sink`**: at-least-once, and effectively-once when the primary key is stable and deterministic from the record, because `PutItem` is an upsert and a replay overwrites the same item. A batch is deduped by primary key (last write wins) before submission, and `UnprocessedItems` are resubmitted with exponential backoff.

None of the sinks offer two-phase-commit exactly-once. On exhausted retries each sink throws so the job replays from the last checkpoint rather than silently dropping records.

## The Kinesis source

The source lists the stream's shards with `ListShards`, reads each shard it owns with `GetShardIterator` and `GetRecords`, and checkpoints where it is in each one. The logic is `impls/aws/src/kinesis_source.cpp`; the AWS SDK calls are behind the `KinesisShardService` interface in `impls/aws/src/kinesis_sdk_service.cpp`.

### Ownership and parallelism

Shard `s` belongs to subtask `kinesis_shard_owner(s, parallelism)`, the FNV-1a 64 hash of the ShardId modulo the parallelism. The rule depends on the id alone, so subtasks that list the stream at different moments, receive its pages in a different order, or restart, agree on every owner: no shard is read by two subtasks and none by nobody. A subtask index that is not below the parallelism is refused at construction.

### Discovery

Each subtask lists the stream when it starts, every `discovery_interval_ms` after that, and soon after it reads an owned shard to its end (when the shard's children appear). Discovery does not wait for an idle round, and runs on a subtask that owns no shard yet. A listing is applied only when every page has arrived. A failed page is retried after a backoff from its token while the subtask keeps reading its shards; an expired token restarts the listing from the first page, and three expiries in a row fail the task.

### Lineage and ordering

The service names a split child's parent and a merge child's two parents. A child is read once every parent this subtask owns has been read to its end. A parent the listing no longer names has left the stream and holds nothing back. A parent another subtask owns is not waited for: subtasks share nothing while they run, and the source does not coordinate them.

What that gives:

- Within a shard, records are emitted in the order the shard holds them.
- At parallelism 1 the one subtask owns every parent, so records with the same hash key (the partition key, or an explicit hash key) are emitted in the order the stream accepted them, across splits and merges.
- Above parallelism 1 a child can belong to another subtask than its parent, and is then read while the parent may still be read. Records of one key can be emitted out of order around a reshard; order holds within each shard only. Run the source at parallelism 1 when a reshard must not reorder a key.
- Nothing orders records of different shards. The source compares sequence numbers only within one shard.

### Start positions

- `trim_horizon`: every shard starts at its oldest retained record.
- `latest`: every shard starts at the records whose arrival time (the service's `ApproximateArrivalTimestamp`) is at or after the source's first start, read with `AT_TIMESTAMP`. That includes shards created later, which therefore start at their first record. The start time is the Worker's clock; a clock ahead of the service's skips records arriving within the difference, so keep it synchronised.
- A restore keeps the first start's time (the earliest across the restored subtasks), so a restore from a checkpoint taken before the first record reads what arrived meanwhile.
- A job restarted before its first checkpoint completes has nothing to restore and starts afresh; under `latest`, from the restart.

### Checkpoint and restore

The checkpoint holds, as operator state:

- one row per owned shard, `__kinesis_shard__:<ShardId>`: its status (`waiting`, a child not yet eligible; `reading`; `drained`, read to its end), the last record handed downstream (empty before the first), its parents, its starting sequence number, and where it starts while nothing has been read (the latest-mode start time, or its oldest record);
- one row per subtask, `__kinesis_subtask__:<index>`: the parallelism, `initial_position`, the first start's time and the stream. Its presence marks a recovery rather than an initial start.

A shard's position moves only after its records are handed to the runtime, and the checkpoint is taken between reads, so it never names a position past records the runtime holds.

A restore resumes each shard that has a position `AT_SEQUENCE_NUMBER` that position, and the service must return that record first; the source drops it and reads on. An expired iterator is replaced the same way or, before the shard's first record, at the shard's start, never at the tip. The source fails the task, naming the shard and the cause, rather than restart a shard at a later point when:

- the listing no longer names a shard the source was still reading or waiting on: its unread records have passed the retention period, or the stream was deleted;
- a shard's starting sequence number differs from the recorded one: the stream was deleted and created again under the same name;
- the service refuses a saved position, returns another record first, or ends the shard without returning it: the position has passed the retention period;
- the rows name another stream, or are in a format this version does not know.

A drained shard is forgotten once two complete listings no longer name it, and its row goes at the next checkpoint, so the state stays bounded by the stream's own listing.

### Rescaling

A shard's row lives in the state of the subtask that read it. A restore at the same parallelism needs only the subtask's own rows. A restore at another parallelism moves shards to new owners, so every new subtask needs every previous subtask's rows, and the source checks the subtask rows it received:

- `file://` hands every restoring subtask every previous subtask's operator rows, so a rescale works. Tested through the runtime's restore at 2 to 4, 3 to 1, 1 to 4 and 4 to 2.
- `rocksdb://` and `forst://` hand a new subtask only the previous subtasks mapped to it. That covers 1 to N and N to 1 (tested on `rocksdb://`); any other rescale is refused at open, naming the missing subtasks.

The runtime rescales by whole factors only.

### Earlier checkpoints

A checkpoint from an earlier release holds one `__kinesis_seq__:<ShardId>` row per shard it had read from. At parallelism 1 it restores: each position resumes as above, and a shard without a row starts at its oldest record whatever `initial_position` says, because no start time was recorded. Above parallelism 1 it is refused, because the earlier source did not record which subtask held which shard; restore it at parallelism 1 or start the job afresh. The earlier rows do not say whether a shard had been read to its end either, so a shard they name that the stream no longer lists fails the restore as a possible loss. A restore that finds no source rows at all (a source new to a restored job, or an earlier release's checkpoint taken before any record) also starts every shard at its oldest record. The first checkpoint after the restore is in the current format. The change does not go the other way: an earlier release restoring a checkpoint in this format finds none of the rows it knows and starts every shard at its `initial_position`, so do not move a job back to an earlier release with such a checkpoint.

### Failures, retries and cancellation

| Failure | Examples | Handling |
| --- | --- | --- |
| Throttling | `ProvisionedThroughputExceededException`, `LimitExceededException`, `KMSThrottlingException`, HTTP 429 | Back off from 200 ms, doubling to 10 s, and retry. The task fails once one call has kept failing for `retry_timeout_ms`. |
| Transient | `InternalFailure`, `ServiceUnavailable`, `ResourceInUseException` (resharding), other 5xx, no response | As throttling. |
| Expired iterator | `ExpiredIteratorException`, or an iterator refused as invalid | Mint another at the shard's position; five in a row fail the task. |
| Expired listing token | `ExpiredNextTokenException` | List again from the first page; three in a row fail the task. |
| Position unavailable | `GetShardIterator` refuses the shard or the sequence number | Fail: the shard cannot continue without skipping records. |
| Permanent | access denied, unrecognised credentials, KMS errors, `ValidationException`, a missing stream | Fail at once, with what to check. |

The SDK's own retries are off for the source, so these are the only retries. One shard's backoff does not hold back the others, and the source returns to the runtime at least every 100 ms, so checkpoint barriers are not delayed by it. `cancel()` wakes any wait at once and aborts the AWS call in flight; a call the SDK cannot abort ends within the request timeout (10 s).

The source reports `records_in`, `bytes_in`, consumer lag from `MillisBehindLatest` and errors under the connector name `kinesis`, and counts each failure that stops it as a permanent failure. Its `kinesis.source` log names each subtask's shards at its first listing, each shard discovered with its parents, each shard read to its end, each child released, each retry at its first and every power-of-two attempt, and each failure with its cause. The log carries stream and shard ids and the service's error code and message, never credentials.

### Retention

The source owes every record from each shard's position onward, so it must reach each shard before its records pass the stream's retention period (24 hours by default). A saved position that has passed is detected, as above. Not detected: a shard the source tracks but has not read yet, such as a child waiting for a slow parent or any shard while the job is down, whose oldest records pass the retention period before the first read. It starts at what the stream still holds.

## Limitations

- Each sink must be the only sink on its chain: at every checkpoint barrier it writes out what it received before the barrier, which holds only while it owns its chain's checkpoint, so `Dag::add_sink` refuses another sink beside it, whichever is added first. SQL and job graphs plan every sink as a subtask of its own; with the Dag API, put the other sink in a separate Dag (see [checkpointing](../internals/checkpointing.md#guarantees-and-caveats)).
- Module only configures when the kinesis, firehose and dynamodb SDK components are present. The toolchain image has them; an SDK built with only the s3 component leaves the module out under `AUTO`.
- String channel only; every record is a string (JSON object string on the SQL path). No int64 channel, no Arrow/Parquet encoding on this path.
- Firehose, DynamoDB and the Kinesis source/sink are the four registered factories; there is no Kinesis Firehose source and no DynamoDB source.
- `dynamodb_sink`: each record must be a JSON object; the partition key (and sort key, if configured) must be present, non-null and scalar, or the record is rejected. `BatchWriteItem` is capped at 25 items per call. JSON numbers are mapped to DynamoDB `N`.
- `kinesis_sink`: a record over the 1 MiB per-record limit poisons the whole `PutRecords` batch under `dlq=fail`; use `dlq=drop` to route oversized records to the bad-record path instead.
- `firehose_sink`: no partition key and no ordering guarantee; the destination must split concatenated records using the appended delimiter.
- `kinesis_source`: per-key order across a split or merge holds only at parallelism 1; above it, order holds within each shard.
- `kinesis_source`: a restore at another parallelism needs every previous subtask's source rows at every new subtask: `file://` provides them, `rocksdb://` and `forst://` only for 1 to N and N to 1, and the source refuses the rest.
- `kinesis_source`: an earlier release's checkpoint restores at parallelism 1 only.
- `kinesis_source`: records of a tracked shard not yet read can pass the retention period unnoticed (see [Retention](#retention)).
- `kinesis_source`: polling `GetRecords` only; no enhanced fan-out (`SubscribeToShard`).

## Testing

The Kinesis source's evidence comes at three levels, and they are kept apart.

**Deterministic, against a model of the service.** `impls/aws/tests/test_kinesis_source.cpp` runs the production `KinesisSource` against `impls/aws/tests/kinesis_model.hpp`, a model of `ListShards`, `GetShardIterator` and `GetRecords` with parent and child links, arrival times, retention (trimmed records, shards leaving the listing), expiring iterators, paginated and reordered listings, and a hook that fails any call. It covers ownership; discovery by an empty subtask, by a busy one, and across pages in changing order; split and merge lineage with slow parents and parents on another subtask or absent; a restore at each parent and child transition; latest-mode starts and restores; iterator expiry before and after the first record; retention loss of a saved position; missing and recreated shards; earlier checkpoints; rescaling through the runtime's own `file://` and `rocksdb://` restores; throttling, transient and permanent failures; cancellation in a wait and in a call; and bounded state across 25 reshards. The remaining test files (`test_factory_registration.cpp`, `test_dynamodb_item.cpp`, `test_kinesis_firehose_sink.cpp`) cover the sinks and factory registration in-process.

**Against the emulator.** LocalStack (`localstack/localstack:3`, which reported version 3.8.1 here) supports `SplitShard`, `MergeShards`, `UpdateShardCount`, paginated `ListShards`, and the `TRIM_HORIZON`, `AT_TIMESTAMP` and `AT_SEQUENCE_NUMBER` iterators. `impls/aws/tests/test_kinesis_live.cpp` (`KinesisLive`, eight cases) runs a sink to source round trip, a checkpoint resume, a split and a merge while one subtask reads (every key in order), a uniform reshard under two subtasks with one-shard listing pages (every shard owned once, every shard in order), a latest-mode restore before the first record, a missing stream, a recreated stream and a cancel. `tests/integration/test_kinesis_recovery.cpp` (`KinesisRecovery`, three cells) runs a coordinator and two workers reading the emulator's stream into the file two-phase-commit sink: at parallelism 1 with a split, a worker killed with records in flight and a merge after the restore; at parallelism 2 with a uniform reshard and a kill; and in latest mode killed before the first record. Each cell checks the sink's committed files against the records it wrote: every record once, nothing else, and the order the source promises.

**Not verified against AWS.** Nothing here ran against AWS. The emulator does not throttle, refuses a retention period below 24 hours (so it cannot trim records under a test), and answers an unknown sequence number with `ResourceNotFoundException`, where AWS documents `InvalidArgumentException`; throttling, retention loss and iterator expiry are therefore covered by the model only. The source treats both answers to a lost position, a refusal and a later first record, as a lost position. On-demand capacity mode, whose resharding the service drives, was not exercised.

The gate is `scripts/kinesis-live.sh`: it starts LocalStack on a free loopback port, runs every Kinesis suite and the recovery cells, and fails on a missing binary, an emulator that never answers, a failed case, any skip, or a suite that ran fewer cases than it should. It keeps each run's report and output, the cells' coordinator and worker logs, and `provenance.txt` (the commit, the build's configuration, the emulator's version and image) under `<build-dir>/kinesis-live-reports`. It passed locally on commit 99c8ff72 against LocalStack 3.8.1 on 2026-11-06: 46 of 46, the three recovery cells committing every record once (2,400, 2,400 and 600) with one restart each. The `kinesis` CI workflow runs it; it has not run yet.

```bash
cmake --build build --target clink_aws_tests clink_integration_tests clink_node --parallel 10
scripts/kinesis-live.sh build
```

To run the live cases by hand against LocalStack from `docker/integration-services.yml`:

```bash
docker compose -f docker/integration-services.yml up -d localstack
export CLINK_KINESIS_TEST_ENDPOINT=http://localhost:4566
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
ctest --test-dir build -L aws
```

LocalStack accepts any credentials; the dummy keys above only let the SDK sign the requests.
