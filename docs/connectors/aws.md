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

All factories share `region` and `endpoint_override` for the AWS client (`endpoint_override` targets LocalStack or a custom endpoint). Every connector here turns the SDK's own retries off and paces and bounds its own, so each attempt it counts is one request. The client's connect and request timeouts default to 3000 ms and 10000 ms; the three sinks take them as `connect_timeout_ms` and `request_timeout_ms`, and the source uses the defaults.

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
| `stream` | Yes | (none) | Stream name (1 to 128 characters from `a-z`, `A-Z`, `0-9`, `_`, `.` and `-`) or stream ARN (`arn:<partition>:kinesis:<region>:<account>:stream/<name>`). An ARN sets `StreamARN`, else `StreamName`. |
| `partition_key` | No | (none) | The record's top-level JSON member whose value is its partition key: a string as it is, a number as its exact value in plain decimal, a boolean as `true` or `false`. A record without a usable key is refused. When unset, records are numbered per sink instance instead (see [partition keys](#partition-keys)). |
| `batch_records` | No | `500` | Records per `PutRecords` call, 1 to 500. |
| `max_record_bytes` | No | `1048576` | The stream's maximum record size, which a record's data plus its key's UTF-8 bytes must not exceed: 1 to 10485760. Raise it only with the stream's `MaxRecordSizeInKiB`. |
| `max_request_bytes` | No | the larger of `5242880` and `max_record_bytes` | The most one request may carry in data and keys, from `max_record_bytes` to 10485760. |
| `max_retries` | No | `8` | Retries in a row that store nothing (a failed request, or failed entries that did not shrink) before the sink fails, 0 to 20; 0 fails at the first such attempt. |
| `retry_timeout_ms` | No | `60000` | The longest one request's records may take with every retry and wait, 1 to 3600000. |
| `linger_ms` | No | `0` | Flush a partial batch once its oldest record is this old, checked when a record arrives (0 disables linger). |
| `dlq` | No | `fail` | A record refused before it is sent (no usable key, over `max_record_bytes`): `fail` stops the sink, and the job's restart replays it; `drop` sends it to the job's dead-letter path and writes the rest. Errors from the service are never dropped. |
| `region` | No | SDK default | AWS region. |
| `endpoint_override` | No | (none) | Custom endpoint: an `http://` or `https://` URL without user information. |
| `connect_timeout_ms` | No | `3000` | The SDK's connect timeout, 1 to 600000. |
| `request_timeout_ms` | No | `10000` | The SDK's request timeout, 1 to 600000 (see [failures and retries](#failures-and-retries-1) for what it bounds). |

A value outside its range, a `dlq` other than `fail` or `drop`, and a malformed `stream` fail the job's build with the rule they break. The sink needs `kinesis:PutRecords` on its stream.

### `firehose_sink`

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `delivery_stream` | Yes | (none) | Firehose stream name: 1 to 64 characters from `a-z`, `A-Z`, `0-9`, `_`, `.` and `-`. |
| `delimiter` | No | `\n` | Appended to each record's data, and counted in its size, so the destination can split records. Empty appends nothing. |
| `batch_records` | No | `500` | Records per `PutRecordBatch` call, 1 to 500. |
| `max_retries` | No | `8` | Retries in a row that accept nothing (a failed request, or failed entries that did not shrink) before the sink fails, 0 to 20; 0 fails at the first such attempt. |
| `retry_timeout_ms` | No | `60000` | The longest one request's records may take with every retry and wait. |
| `linger_ms` | No | `0` | Flush a partial batch once its oldest record is this old, checked when a record arrives (0 disables linger). |
| `region` | No | SDK default | AWS region. |
| `endpoint_override` | No | (none) | Custom endpoint: an `http://` or `https://` URL without user information. |
| `connect_timeout_ms` | No | `3000` | The SDK's connect timeout, 1 to 600000. |
| `request_timeout_ms` | No | `10000` | The SDK's request timeout, 1 to 600000. |

A value outside its range fails the job's build with the rule it breaks. The C++ options also take `max_bytes`, the most one request may carry counted as sent (see [the Firehose sink](#the-firehose-sink)), from what the largest record needs alone up to 4 MiB, the default. The sink needs `firehose:PutRecordBatch` on its stream.

### `dynamodb_sink`

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `table` | Yes | (none) | Target table name or ARN. |
| `partition_key` | Yes | (none) | The table's partition-key attribute; checked against the table when the sink opens. |
| `sort_key` | When the table has one | (none) | The table's sort-key attribute; checked the same way. |
| `batch_records` | No | `25` | Items per `BatchWriteItem` call, 1 to 25. |
| `max_retries` | No | `8` | Retries in a row that write nothing (a failed request, or `UnprocessedItems` that did not shrink) before the sink fails, 0 to 20; 0 fails at the first such attempt. |
| `retry_timeout_ms` | No | `60000` | The longest one request's items, or the open's `DescribeTable`, may take with every retry and wait. |
| `linger_ms` | No | `0` | Flush a partial batch once its oldest item is this old, checked when a record arrives (0 disables linger). |
| `region` | No | SDK default | AWS region. |
| `endpoint_override` | No | (none) | Custom endpoint: an `http://` or `https://` URL without user information. |
| `connect_timeout_ms` | No | `3000` | The SDK's connect timeout, 1 to 600000. |
| `request_timeout_ms` | No | `10000` | The SDK's request timeout, 1 to 600000. |

A value outside its range fails the job's build with the rule it breaks. Authentication is handled by the AWS SDK's default credential provider chain (environment, profile, instance role, and so on). The connector exposes no explicit credential options; `endpoint_override` is the LocalStack hook, and with LocalStack any credentials are accepted by the service. The DynamoDB sink needs `dynamodb:DescribeTable` and `dynamodb:BatchWriteItem` on its table.

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

A `connector='kinesis'` sink table reads only the options in the table above (plus `format='json'` and the generic table options); any other option is refused when the job is planned, naming the ones it reads, so a misspelt `partition_key` cannot route every row by a number instead. `stream` is required. Each row reaches the sink as one JSON object, so `partition_key` names a column: a `BIGINT` column keys by its exact digits and a `STRING` column by its text.

Each row reaches the DynamoDB sink as one JSON object and is written as one item. A SELECT that produces a changelog is refused at bind time unless the sink table declares `changelog='true'`; with it, every change row is written as an item, a deletion included, because the sink only puts and never deletes.

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
- **`kinesis_sink`**: at-least-once, with no producer deduplication and no per-key order. A checkpoint covers records Kinesis has stored. Only failed entries are resent, with the keys they were sent with; a lost reply, an entry that failed with `InternalFailure` after all, or a replay can store a record twice. See [the Kinesis sink](#the-kinesis-sink) for keys, limits, responses, retries and the routing changes from earlier releases.
- **`firehose_sink`**: at-least-once into Firehose, with no partition key, no ordering and no producer deduplication. A checkpoint covers acceptance by Firehose, not delivery to the destination. See [the Firehose sink](#the-firehose-sink) for limits, responses, retries and duplicates.
- **`dynamodb_sink`**: at-least-once writes. A replayed record rewrites its item rather than add one, and an item ends at the last record written for it when every record of the item reaches one sink subtask in order; it is not exactly-once and does not guard against other writers. See [the DynamoDB sink](#the-dynamodb-sink) for identity, limits, retries and the exact contract.

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

## The Kinesis sink

The sink writes each record's bytes, untouched, as one Kinesis record with `PutRecords`, keyed as below. The logic is `impls/aws/src/kinesis_sink.cpp`; the AWS SDK call is behind the `KinesisWriteService` interface in `impls/aws/src/kinesis_sink_sdk_service.cpp`.

### Partition keys

With `partition_key` set, each record must be one JSON object (RFC 8259, UTF-8) holding that top-level member, and the member's value is the record's partition key:

| Value | Key |
| --- | --- |
| string | The string as it is, escapes decoded. |
| number | Its exact value in plain decimal: no exponent, no leading zeros, no point for an integer and no trailing zeros after one, `-` for a negative value and `0` for zero of either sign. `9007199254740993` stays itself, `1.50` and `15E-1` are both `1.5`, `1e3` is `1000`. Nothing passes through a double. |
| `true`, `false` | The word. |

A number and the string with the same text are the same key. The record is refused, by its position and the rule it breaks, when:

- it is not one JSON object, is not valid JSON, or is not valid UTF-8;
- the member is missing, `null`, an object or an array, or appears twice at the top level;
- the key is empty, or longer than 256 characters;
- the key holds U+0000: the AWS SDK for C++ cuts a string at its first NUL when it writes the request, so the record would be routed by a shorter key;
- a string escapes half of a surrogate pair, which no UTF-8 key can hold;
- a number's plain decimal form is longer than 256 characters.

The documentation limits a key to 256 characters without saying what a character is. The sink counts UTF-16 code units, so a character outside the Basic Multilingual Plane, such as an emoji, counts as two. That reading never accepts a key a count of code points would refuse, and it is how the emulator counts. A key's UTF-8 bytes count against the record size.

Without `partition_key`, the sink numbers records `0`, `1`, `2` and so on per sink instance, so records spread across shards, and never reads them. A key is fixed when its record is buffered, so every retry carries the key the record was first sent with. A restarted instance numbers afresh and each subtask numbers on its own, so a replayed record can land on another shard than its first copy.

On a stream that uses the `AUTO` record distribution strategy (on-demand streams only), Kinesis ignores partition keys and places records itself.

### Records and requests

| Limit | Value | How the sink counts it |
| --- | --- | --- |
| Record | The data (before base64) and the key, against the stream's maximum record size: 1 MiB unless the stream was raised, at most 10 MiB | `max_record_bytes`, the record's bytes plus its key's UTF-8 bytes. A record over it is refused before it is buffered. |
| Records per request | 500 | `batch_records`, at most 500. |
| Request | 10 MiB of data and partition keys | `max_request_bytes`, every record's bytes and key. Default 5 MiB, the limit the documentation gave before records over 1 MiB were introduced in October 2025; raise it to 10 MiB to use the current one. |
| Partition key | 1 to 256 characters | UTF-16 code units, as above. |

The buffer is sent before a record that would take the next request past its count or byte limit is added, so no request is ever built over a limit, and a record that cannot fit alone is refused. An empty record is allowed. A record's bytes are never parsed without `partition_key`, so any bytes go, binary included.

Records over 1 MiB need the stream's maximum raised (`UpdateMaxRecordSize`, or `MaxRecordSizeInKiB` at creation) and `max_record_bytes` with it. The sink does not read the stream's setting: a `max_record_bytes` above it lets records through that the service refuses, which fails the sink as a permanent error naming the setting to check.

### Refused records

Under `dlq='fail'`, the default, a refused record stops the sink: the sink checks a whole input batch before buffering any of it, so a batch with a refused record adds nothing, and the sink fails as described below. Records stored by earlier sends stay stored, and the job's restart replays the rest; a refused record fails the job each time the replay reaches it, so fix it upstream.

Under `dlq='drop'`, each refused record goes to the job's dead-letter path with its bytes, the reason, the connector `kinesis`, the direction `sink` and the stream as its location, and is counted as dropped; the rest of the batch is written. A dead-lettered record is handled, so a later barrier is acknowledged over it. Numbered keys go to the records kept only.

Errors from the service, whatever they are, are never a refused record: they fail the sink under either policy.

### Responses

A response counts only when it accounts for every record sent: one entry per record, in request order, each either stored (a `SequenceNumber` of up to 129 digits with no leading zero, and a `ShardId` of 1 to 128 characters from `a-z`, `A-Z`, `0-9`, `_`, `.` and `-`) or failed (an `ErrorCode`), never both or neither, no sequence number twice, and a `FailedRecordCount` equal to the entries that failed. The SDK reads a response without `FailedRecordCount` as 0, which the entries then contradict if any failed. When a response counts, the records whose entries failed, and only those, are sent again with the keys they were first sent with. Any other response is ambiguous: the sink cannot tell which records Kinesis stored, so it fails rather than count them stored. It never reads a response entry past the request's records.

### Failures and retries

| Answer | Handling |
| --- | --- |
| Every entry stored | The records are stored. |
| Some entries failed: `ProvisionedThroughputExceededException`, `InternalFailure`, or a code the documentation does not list | Send those records again after a backoff. An entry that failed with `InternalFailure` may have been stored after all, so its record can arrive twice. |
| Throttled: `ProvisionedThroughputExceededException`, `LimitExceededException`, `KMSThrottlingException`, `ThrottlingException`, HTTP 429 | Nothing was stored. Send the request again after a backoff. |
| Transient: `InternalFailure`, `ServiceUnavailable`, `ResourceInUseException`, other 5xx, no response | The request may or may not have been stored. Send it again after a backoff; its records can arrive twice. |
| A response that does not account for its request | Ambiguous. Fail. |
| Permanent: `InvalidArgumentException`, `ValidationException`, `ResourceNotFoundException`, the KMS errors other than throttling, access, signature and credential errors, other 4xx the SDK would not retry | Fail at once, with what to check. |
| The task is cancelled | The backoff ends at once and the sink fails. A call still sending its request is aborted; one waiting for its reply ends with the SDK's request timeout, since the SDK looks at an abort only when data moves. |

The backoff starts at 100 ms and doubles to 10 s. A request's records fail the sink once `max_retries` retries in a row have stored nothing (an attempt that stores some resets the count), or once `retry_timeout_ms` has passed since the first attempt. The SDK's own retries are off, so each attempt is one request.

One call is bounded by `connect_timeout_ms` and by `request_timeout_ms`, which the SDK applies as a limit on how long a call may receive nothing, not on its total length: curl measures the transfer rate over five seconds, and the SDK asks for `100 Continue`, so a call whose reply never comes ends about six seconds later than `request_timeout_ms` alone suggests (measured with the host's SDK, 1.11.840 on macOS: 7 s at 1000 ms, 9 s at 3000 ms). A flush therefore takes at most `retry_timeout_ms` and one call.

The sink reports `records_out`, `bytes_out`, the latency of each flush, errors, permanent failures and dropped records under the connector name `kinesis`. Its `kinesis.sink` log names the stream, the key mode and the limits at open, each retry at its first and every power-of-two attempt, dropped records at the first and every power of two, and each failure with its cause. The service's messages are logged with access key ids and signed request values removed. The sink's own log never carries credentials or record contents; a record sent to the dead-letter path goes there whole, and the job's dead-letter queue decides what of it is logged.

### What a checkpoint covers

A barrier returns only once every record received before it has been stored by Kinesis (or, under `dlq='drop'`, dead-lettered), so a checkpoint never covers a record that is outstanding, being retried, or ambiguous.

When the sink fails, by a refused record under `dlq='fail'`, an exhausted retry budget, a permanent error, an ambiguous response or a cancellation, it drops what it buffered and says how many records that was, and refuses every later record, flush and barrier, so it cannot acknowledge a checkpoint over records it dropped. `open()` starts it afresh with an empty buffer, as a restart does; `close()` sends nothing. At a clean end of input the runtime flushes before closing, so everything buffered is sent; a cancelled task's buffer is not.

### Duplicates and order

Delivery is at-least-once. A record can be stored twice:

- when a reply is lost after Kinesis stored the request, or an entry failed with `InternalFailure` after its record was stored, and the sink sends it again;
- when the job restarts: every record after the last completed checkpoint is sent again, including records Kinesis had already stored.

Kinesis keeps no producer identity, so a consumer that needs each record once must deduplicate, by a field of the record.

Records are not promised to arrive in order, even with one partition key. `PutRecords` processes a request's records independently and keeps no order between them, a failed entry is resent after records that followed it were stored, and a replay sends records again after later ones. Sharing a partition key puts records in one shard, not in the order they were produced. The sink has no ordered-write mode.

### Earlier releases

Records keep their bytes. What changes:

- **Numeric keys are exact.** The earlier sink read a number through a double and wrote it with `std::to_string`. An integer within 2^53 routes as before, however it is written (`42`, `42.0`, `4.2e1`). An integer past 2^53 and every fraction route by a different key: the earlier sink sent `9007199254740993` and `9007199254740992` both as `9007199254740992`, `9007199254740995` as `9007199254740996`, `0.0000001` and `0.0000002` both as `0.000000`, and `1.5` as `1.500000`. A changed key moves its records to the shard the new key hashes to. The change loses and duplicates nothing; records the earlier sink put under one key were all written, but shared a shard they now do not.
- **Long keys are refused, not cut.** The earlier sink cut a key at 256 bytes, so two keys sharing their first 256 bytes routed together, and a key cut inside a UTF-8 sequence made a request that is not valid JSON. A key of up to 256 characters that is longer than 256 bytes, such as 100 CJK characters, is now sent whole; a longer key is refused.
- **A record without a usable key is refused.** The earlier sink routed a record with a missing, `null`, empty, object or array key, or that was not JSON, by its counter instead. It now fails the sink or, under `dlq='drop'`, goes to the dead-letter path.
- **Responses are checked.** A response with `FailedRecordCount` 0 counted as success whatever its entries said, and a failed entry past the request's records was read out of bounds; both now fail the sink as ambiguous.
- **A failure is final until reopened.** After a failed flush the earlier sink forgot the records and acknowledged the next barrier; it now refuses it.
- **Retries are bounded and cancellable.** The SDK's three retries no longer stack under the sink's: against a stand-in answering every request with `InternalFailure`, the earlier sink made 36 requests for its 9 attempts. Waits are cut short by a cancellation, and permanent errors fail at once.
- **Options are checked.** `batch_records` outside 1 to 500 and `max_retries` outside 0 to 20 fail the build instead of being clamped, and a `dlq` other than `fail` or `drop` is refused instead of meaning `fail`. `max_retries` counts retries in a row that store nothing. `max_record_bytes` now counts the key and applies under both policies; it used to count the data only, and only under `dlq='drop'`. `max_request_bytes` replaces the C++ `max_bytes`, which counted each record as its data plus 320 bytes and was checked only after a record was added. `retry_timeout_ms`, `connect_timeout_ms`, `request_timeout_ms` and the SQL option check are new.

## The DynamoDB sink

The sink maps each record to an item, buffers items by the item they write, and writes them with `BatchWriteItem`. The record rules and item identity are in `impls/aws/src/dynamodb_item.cpp`, the write loop in `impls/aws/src/dynamodb_sink.cpp`, and the AWS SDK calls behind the `DynamoDbWriteService` interface in `impls/aws/src/dynamodb_sdk_service.cpp`.

### The table

When it opens, the sink reads the table with `DescribeTable` and refuses to start, naming what differs, when:

- `partition_key` is not the table's partition key, or `sort_key` is not its sort key: set when the table has one, left out when it has none;
- a key attribute is binary (`B`): records are JSON, which has no binary type, so the sink writes tables with `S` and `N` keys only;
- the table is neither `ACTIVE` nor `UPDATING`. A table still `CREATING` is waited for, within the same retry bounds as a write.

The sink does not create or alter tables.

### Records and items

| JSON | Attribute |
| --- | --- |
| string | `S` |
| number | `N`, with every significant digit it was written with |
| `true`, `false` | `BOOL` |
| `null` | `NULL` |
| object | `M` |
| array | `L` |

Nothing passes through a double: a number keeps up to DynamoDB's 38 significant digits. Sets and binary values are never written. Each record is checked before it is buffered, and refused, by its position in the batch and the rule it breaks, when:

- it is not one JSON object, is not valid JSON, or is not valid UTF-8;
- a string holds U+0000. The AWS SDK for C++ ends a string at its first NUL when it writes the request, a value's and an attribute name's alike, so the item stored would hold a shorter string, and a key could name another item;
- an attribute name or map key is empty, longer than 64 KB, or appears twice in one object;
- a number has more than 38 significant digits, or lies outside DynamoDB's range: a magnitude from 1E-130 to 9.9999999999999999999999999999999999999E+125, or zero;
- a map or list sits more than 31 levels below its attribute. DynamoDB's documented 32 levels count the item itself; DynamoDB Local refuses a 32nd level, even an empty map;
- a key attribute is missing, null, of another type than the table declares (a string for `S`, a number for `N`), an empty string, or longer than 2,048 bytes for a partition key or 1,024 bytes for a sort key, counted in UTF-8 bytes;
- the item is larger than 400 KB (409,600 bytes) by DynamoDB's size rules: a name or string by its UTF-8 bytes; a number by its digit pairs aligned on the decimal point, plus one (so 1.5 is 3 bytes, where the documentation's approximation says 2, and the sink never counts less than that approximation); a boolean or null 1; a map or list 3, plus each element and 1 more per element.

A table with local secondary indexes counts index entries against the 400 KB as well, which the sink does not see; the service can refuse such an item, which fails the sink as a permanent error.

### Item identity

Buffered records are keyed by the item they write: the key attributes' types and values, compared field by field. A string compares by its UTF-8 bytes, so no character acts as a separator between the partition and sort key, and the two encodings of é are two items, as in DynamoDB. A number compares by value, as DynamoDB compares `N` keys: 1, 1.0, 10E-1 and 0.1E1 are one item, -0 is 0, and 9223372036854775808 and 9223372036854775809 are two. The string "1" and the number 1 differ by type, though a table declares one type per key attribute, so a record with the other type is refused rather than compared.

A record for an item already buffered replaces it: one `BatchWriteItem` never names an item twice, which the service refuses, and the item ends at the later record's value. Numbers are sent in a canonical form (plain decimal up to 40 characters, then scientific); DynamoDB stores the number, not the text.

### Writes and retries

Buffered items are written when `batch_records` distinct items are buffered, when the oldest is `linger_ms` old, and at every checkpoint barrier. A request carries at most `batch_records` items and at most 16 MiB as the SDK sends it, which indents its JSON, so a deeply nested item counts for more than its text; items that do not fit go in the next request.

| Answer | Handling |
| --- | --- |
| Success with no `UnprocessedItems` | The request's items are written. |
| `UnprocessedItems` naming some of the request's own puts | Send those again, and only those, from the sink's own copies, after a backoff. |
| `UnprocessedItems` that are not the request's own puts: another table, a delete, an entry with neither, an item the request did not carry or listed twice, a type the sink never writes, no valid key | Ambiguous: the sink cannot tell what was written. Fail. |
| Throttling: `ProvisionedThroughputExceededException`, `ThrottlingException`, `RequestLimitExceeded`, HTTP 429 | Nothing was written. Send the request again after a backoff. |
| Transient: `InternalServerError`, `ServiceUnavailable`, `ReplicatedWriteConflictException`, `ItemCollectionSizeLimitExceededException`, other 5xx, no response | The request may or may not have been applied. Send it again after a backoff: a put of the same value is the same write, and nothing later for its items has been sent. |
| Permanent: `ValidationException`, `ResourceNotFoundException`, access, signature and credential errors, other 4xx the SDK would not retry | Fail at once, with what to check. |
| The task is cancelled | The backoff ends at once, the call in flight is aborted, and the sink fails. |

The backoff starts at 100 ms and doubles to 10 s. A request's items fail the sink once `max_retries` retries in a row have written nothing (an attempt that writes some of them resets the count), or once `retry_timeout_ms` has passed since the first attempt; DynamoDB's guidance is to stop retrying at about a minute. The SDK's own retries are off, so these are the only ones. DynamoDB lists `UnrecognizedClientException` as retryable; the sink fails at once instead, since the same credentials rarely start working, and the job's restart opens the sink again.

The sink reports `records_out`, the latency of each flush and errors under the connector name `dynamodb`. Its `dynamodb.sink` log names the table's key at open, each retry at its first and every power-of-two attempt, and each failure with its cause. The service's messages are logged with access key ids and signed request values removed; credentials are never written.

### Checkpoints and failure

A barrier returns only once every item received before it is written, so a checkpoint never covers a write that is outstanding, being retried, or ambiguous. The sink fails on a refused record, an exhausted retry budget, a permanent error, an ambiguous response or a cancellation. It then drops what it buffered and says how much the failing flush had written and how much it drops. It also refuses every later record, flush and barrier, so it cannot acknowledge a checkpoint over writes it dropped. The job's restart replays every record after the last completed checkpoint.

The sink checks a whole input batch before buffering any of it, so a batch holding a refused record adds nothing. Items written by earlier flushes stay written, and items buffered from earlier batches are dropped and come back with the replay. A refused record fails the job each time the replay reaches it: there is no dead-letter option, so fix the record upstream.

### Delivery, ordering and the final state

Three separate properties:

- **Writes are at-least-once.** A replayed record is put again, rewriting its item with the same value, so stable keys mean a replay never adds an item. Between a restart and the point the replay reaches, an item can hold an older record's value than it held before the failure.
- **Records are coalesced per sink subtask.** Records for one item are reduced to the latest while buffered, and written in the order they arrived: a flush finishes, its retries included, before the next one starts, so an older record's write never lands after a newer one's.
- **The final state converges under an ownership assumption.** Once the input stops and the replay is through, each item holds the last record written for it, provided every record of the item reaches one sink subtask in the order it was produced and the replay reproduces that order. That holds at parallelism 1, and above it when the input is partitioned by the item's key: for example a Kinesis source feeding the sink at the same parallelism, with the item's key as the partition key and no reshard.

It does not hold when records of one item reach two sink subtasks, as with input not partitioned by the key: their writes race, and the item can end at an older record. Nor does it hold against other writers of the table: the sink overwrites whatever is there, and a replay can overwrite what another writer put after the sink's first write. Stable keys stop a replay from duplicating an item. They do not make delivery exactly-once, and the sink makes no conditional write or version check.

### Earlier releases

The sink's input and output formats are unchanged for every record an earlier release wrote faithfully: the same items, with integer numbers in the same text. What changes:

- **Numbers keep their digits.** A number that an earlier release passed through a double, one with more than 17 significant digits or an integer outside the 64-bit range, is now stored exactly. Such a key names a different item than before, and two such keys an earlier release collapsed into one item are now two.
- **Records an earlier release mishandled are refused.** A string or name holding U+0000 (stored cut short, so two keys could become one), a string value for an `N` key or the reverse, a boolean key, and a duplicate attribute name (the first was kept) are now refused with the rule they break. So is a record the service would refuse for its shape, which used to fail its whole batch with the service's message.
- **Options are checked.** `batch_records` outside 1 to 25 and `max_retries` outside 0 to 20 fail the build instead of being clamped. `max_retries` counts retries in a row that write nothing rather than every retry, and `retry_timeout_ms` bounds the total.
- **The table is read at open.** The sink needs `dynamodb:DescribeTable`, refuses a table with a binary key, which never worked, and checks the key names it is given.

## The Firehose sink

The sink sends each record, as opaque bytes, as one Firehose record with `PutRecordBatch`. It never parses a record: SQL hands it JSON, but any bytes are sent as they are. The logic is `impls/aws/src/firehose_sink.cpp`; the AWS SDK call is behind the `FirehoseWriteService` interface in `impls/aws/src/firehose_sdk_service.cpp`.

### Records and requests

Each record's data is the record's bytes with the delimiter appended. Firehose concatenates records at the destination and adds no boundary of its own, so the delimiter is what lets a reader split them again. The limits, from the service's documentation:

| Limit | Value | How the sink counts it |
| --- | --- | --- |
| Record | 1,000 KiB (1,024,000 bytes) before base64 encoding | The record's bytes and its delimiter. A record over it is refused when it arrives, by its position and size. |
| Records per request | 500 | `batch_records`, at most 500. |
| Request | 4 MiB | The request as the SDK sends it: every record's data base64-encoded (4 bytes per 3, rounded up), inside its indented JSON object. |

The documentation states 4 MiB per call without saying whether base64 counts. Counting the request as sent keeps the sink within either reading; it also means requests of large records hold fewer of them than the raw-data reading would allow: three records of 1,000 KiB, not four. An empty record is allowed (with no delimiter, its data is empty). The buffer is sent before a record that would take the next request past its count or byte limit is added, so no request is ever built over a limit.

### Responses

A response counts only when it accounts for every record sent: one entry per record, in request order, each holding a `RecordId` (accepted) or an `ErrorCode` (failed), never both or neither, and a `FailedPutCount` equal to the entries that failed. The SDK reads a response without `FailedPutCount` as 0, which the entries then contradict if any failed. When a response counts, the records whose entries failed, and only those, are sent again. Any other response is ambiguous: the sink cannot tell which records Firehose accepted, so it fails rather than count them accepted. It never reads a response entry past the request's records.

### Failures and retries

| Answer | Handling |
| --- | --- |
| Every entry accepted | The records are accepted. |
| Some entries failed (`ServiceUnavailableException`, `InternalFailure`) | Send those records again after a backoff. Firehose says such a record "might have failed", so it can arrive twice. |
| Throttled: `ServiceUnavailableException`, `ThrottlingException`, HTTP 429 | Nothing was accepted. Send the request again after a backoff. |
| Transient: `InternalFailure`, `ResourceInUseException`, other 5xx, no response | The request may or may not have been accepted. Send it again after a backoff; its records can arrive twice. |
| A response that does not account for its request | Ambiguous. Fail. |
| Permanent: `InvalidArgumentException`, `ResourceNotFoundException`, `InvalidKMSResourceException`, access, signature and credential errors, other 4xx the SDK would not retry | Fail at once, with what to check. |
| The task is cancelled | The backoff ends at once, the call in flight is aborted, and the sink fails. |

The backoff starts at 100 ms and doubles to 10 s. A request's records fail the sink once `max_retries` retries in a row have accepted nothing (an attempt that accepts some resets the count), or once `retry_timeout_ms` has passed since the first attempt. The SDK's own retries are off, so these are the only ones. The service's messages are logged with access key ids and signed request values removed. The sink reports `records_out`, `bytes_out`, the latency of each flush and errors under the connector name `firehose`; its `firehose.sink` log names the stream and the request limits at open, each retry at its first and every power-of-two attempt, and each failure with its cause.

### What a checkpoint covers

A barrier returns only once every record received before it has been accepted by Firehose, so a checkpoint never covers a record that is outstanding, being retried, or ambiguous. Accepted means accepted into the stream: a successful `PutRecordBatch` is not proof that the destination has the record. Firehose buffers records and delivers them to the destination on its own schedule, retries failed deliveries by the destination's rules, and keeps undelivered data for 24 hours; those are the service's, and the sink does not observe or wait for them.

When the sink fails, by a refused record, an exhausted retry budget, a permanent error, an ambiguous response or a cancellation, it drops what it buffered and refuses every later record, flush and barrier, so it cannot acknowledge a checkpoint over records it dropped. `open()` starts it afresh, with an empty buffer, as a restart does; `close()` sends nothing. At a clean end of input the runtime flushes before closing, so everything buffered is sent. The sink checks a whole input batch before buffering any of it, so a batch with a refused record adds nothing; records accepted by earlier sends stay accepted.

### Duplicates and order

Delivery into Firehose is at-least-once. A record can be accepted twice:

- when a reply is lost after Firehose accepted the request, or a failed entry's record had in fact been processed, and the sink sends it again;
- when the job restarts: every record after the last completed checkpoint is sent again, including records Firehose had already accepted.

Firehose keeps no producer identity, so nothing removes a duplicate before the destination: deduplicate there if it matters, by a field of the record. Records carry no partition key and no order: Firehose may deliver them in any order, and the sink makes no attempt to keep one.

### Earlier releases

The sink sends the same bytes for every record an earlier release sent. What changes:

- **Responses are checked.** A response with `FailedPutCount` 0 used to count as success whatever its entries said, and a failed entry past the request's records was read out of bounds; both now fail the sink as ambiguous. A response with failed entries but no error codes used to resend the whole batch.
- **Records over the limit are refused when they arrive,** by their position, where they used to fail their whole request with the service's message. A request is no longer built past its limit: the byte threshold used to be checked only after a record was added.
- **Retries are bounded and cancellable.** Permanent errors fail at once instead of being retried. Waits are cut short by a cancellation, and the SDK's own retries no longer stack under the sink's.
- **Options are checked.** `batch_records` outside 1 to 500 and `max_retries` outside 0 to 20 fail the build instead of being clamped. `max_retries` counts retries in a row that accept nothing. `max_bytes` (C++ only) now counts the request as sent and must lie between what the largest record needs alone and 4 MiB; it used to count raw bytes plus a fixed 16 per record. `retry_timeout_ms`, `connect_timeout_ms` and `request_timeout_ms` are new.

## Limitations

- Each sink must be the only sink on its chain: at every checkpoint barrier it writes out what it received before the barrier, which holds only while it owns its chain's checkpoint, so `Dag::add_sink` refuses another sink beside it, whichever is added first. SQL and job graphs plan every sink as a subtask of its own; with the Dag API, put the other sink in a separate Dag (see [checkpointing](../internals/checkpointing.md#guarantees-and-caveats)).
- Module only configures when the kinesis, firehose and dynamodb SDK components are present. The toolchain image has them; an SDK built with only the s3 component leaves the module out under `AUTO`.
- String channel only; every record is a string (JSON object string on the SQL path). No int64 channel, no Arrow/Parquet encoding on this path.
- Firehose, DynamoDB and the Kinesis source/sink are the four registered factories; there is no Kinesis Firehose source and no DynamoDB source.
- `dynamodb_sink`: not exactly-once. An item ends at the last record written for it only when every record of the item reaches one sink subtask in order; two subtasks or another writer of the table can leave an older value, and an item can show an older value while a replay runs. No conditional writes or version checks.
- `dynamodb_sink`: `S` and `N` keys only, since records are JSON; a table with a binary key is refused at open, and no binary or set attribute is written.
- `dynamodb_sink`: a string holding U+0000 is refused, because the AWS SDK for C++ cuts a string at its first NUL.
- `dynamodb_sink`: a refused record fails the job each time the replay reaches it; there is no dead-letter option.
- `dynamodb_sink`: the 400 KB check does not count local secondary index entries, so such a table can still refuse an item, which fails the sink.
- `dynamodb_sink`: puts only. With `changelog='true'` a deletion is written as an item, not applied.
- `kinesis_sink`: at-least-once. A record can be stored twice after a lost reply, an `InternalFailure` entry the service had in fact stored, or a replay; there is no producer deduplication.
- `kinesis_sink`: no per-key order. `PutRecords` keeps none within a request, and failed entries are resent after later records were stored.
- `kinesis_sink`: without `partition_key`, records are numbered per sink instance, so a replayed record can land on another shard than its first copy.
- `kinesis_sink`: the sink does not read the stream's maximum record size; `max_record_bytes` must match it, and a larger value fails the sink when the service refuses a record.
- `kinesis_sink`: a cancellation ends a call waiting for its reply only when the SDK's request timeout does, about six seconds after `request_timeout_ms`.
- `firehose_sink`: at-least-once into Firehose. A record can reach the destination twice after a lost reply, a failed entry the service had in fact processed, or a replay; there is no ordering and no producer deduplication, so deduplicate at the destination if it matters.
- `firehose_sink`: a checkpoint covers acceptance by Firehose only. Buffering, delivery to the destination, its retries and the 24-hour retention of undelivered data are the service's, and the sink does not observe them.
- `firehose_sink`: the destination must split concatenated records at the appended delimiter, so the delimiter must not occur inside a record.
- `firehose_sink`: a record over 1,000 KiB with its delimiter fails the job each time the replay reaches it; there is no dead-letter option.
- `kinesis_source`: per-key order across a split or merge holds only at parallelism 1; above it, order holds within each shard.
- `kinesis_source`: a restore at another parallelism needs every previous subtask's source rows at every new subtask: `file://` provides them, `rocksdb://` and `forst://` only for 1 to N and N to 1, and the source refuses the rest.
- `kinesis_source`: an earlier release's checkpoint restores at parallelism 1 only.
- `kinesis_source`: records of a tracked shard not yet read can pass the retention period unnoticed (see [Retention](#retention)).
- `kinesis_source`: polling `GetRecords` only; no enhanced fan-out (`SubscribeToShard`).

## Testing

Each connector's evidence comes at three levels, and they are kept apart: deterministic cases against a model of the service, cases against the emulator, and what neither covers.

### The Kinesis source

**Deterministic, against a model of the service.** `impls/aws/tests/test_kinesis_source.cpp` runs the production `KinesisSource` against `impls/aws/tests/kinesis_model.hpp`, a model of `ListShards`, `GetShardIterator` and `GetRecords` with parent and child links, arrival times, retention (trimmed records, shards leaving the listing), expiring iterators, paginated and reordered listings, and a hook that fails any call. It covers ownership; discovery by an empty subtask, by a busy one, and across pages in changing order; split and merge lineage with slow parents and parents on another subtask or absent; a restore at each parent and child transition; latest-mode starts and restores; iterator expiry before and after the first record; retention loss of a saved position; missing and recreated shards; earlier checkpoints; rescaling through the runtime's own `file://` and `rocksdb://` restores; throttling, transient and permanent failures; cancellation in a wait and in a call; and bounded state across 25 reshards. `test_factory_registration.cpp` covers factory registration; the Kinesis sink's own cases are below.

**Against the emulator.** LocalStack (`localstack/localstack:3`, which reported version 3.8.1 here) supports `SplitShard`, `MergeShards`, `UpdateShardCount`, paginated `ListShards`, and the `TRIM_HORIZON`, `AT_TIMESTAMP` and `AT_SEQUENCE_NUMBER` iterators. `impls/aws/tests/test_kinesis_live.cpp` (`KinesisLive`, eight cases) runs a sink to source round trip, a checkpoint resume, a split and a merge while one subtask reads (every key in order), a uniform reshard under two subtasks with one-shard listing pages (every shard owned once, every shard in order), a latest-mode restore before the first record, a missing stream, a recreated stream and a cancel. `tests/integration/test_kinesis_recovery.cpp` (`KinesisRecovery`, three cells) runs a coordinator and two workers reading the emulator's stream into the file two-phase-commit sink: at parallelism 1 with a split, a worker killed with records in flight and a merge after the restore; at parallelism 2 with a uniform reshard and a kill; and in latest mode killed before the first record. Each cell checks the sink's committed files against the records it wrote: every record once, nothing else, and the order the source promises.

**Not verified against AWS.** Nothing here ran against AWS. The emulator does not throttle, refuses a retention period below 24 hours (so it cannot trim records under a test), and answers an unknown sequence number with `ResourceNotFoundException`, where AWS documents `InvalidArgumentException`; throttling, retention loss and iterator expiry are therefore covered by the model only. The source treats both answers to a lost position, a refusal and a later first record, as a lost position. On-demand capacity mode, whose resharding the service drives, was not exercised.

The gate is `scripts/kinesis-live.sh`: it starts LocalStack on a free loopback port, runs every Kinesis suite and the recovery cells, and fails on a missing binary, an emulator that never answers, a failed case, any skip, or a suite that ran fewer cases than it should. It keeps each run's report and output, the cells' coordinator and worker logs, and `provenance.txt` (the commit, the build's configuration, the emulator's version and image) under `<build-dir>/kinesis-live-reports`. It passed locally on commit 99c8ff72 against LocalStack 3.8.1 on 2026-11-06: 46 of 46, the three recovery cells committing every record once (2,400, 2,400 and 600) with one restart each. It passed in CI on commit b932c745, 46 of 46 (run 42253565015, under the workflow's earlier name `kinesis`); the `aws` CI workflow runs it. The same script then runs the Kinesis sink's phase (below), whose suites the source's phase leaves out; on commit 0c0cb41f (2026-11-08) the source's phase ran 41 of 41, the five cases of the earlier sink it used to pick up having gone, and its recovery cells again committed every record once.

```bash
cmake --build build --target clink_aws_tests clink_integration_tests clink_node --parallel 10
scripts/kinesis-live.sh build
```

### The Kinesis sink

**Deterministic, against a model of the service.** `impls/aws/tests/test_kinesis_sink.cpp` covers the partition-key rules (every value type, exact numbers including neighbouring integers past 2^53 and fractions `std::to_string` collapses, Unicode and the 256-character limit counted in UTF-16 code units, every kind of refused record, a record nested 200,000 levels deep), the SDK's cutting of keys at NUL, the response judge on complete and partial responses and on every kind that does not account for its request (missing, short and oversized entry arrays, entries with both or neither result, a stored entry without its `SequenceNumber` or `ShardId`, malformed and repeated sequence numbers, a missing or contradictory `FailedRecordCount`), and the error classes. It runs the production `KinesisSink` against `impls/aws/tests/kinesis_sink_model.hpp`, a model of `PutRecords` that keeps every record it stores with its key, duplicates included, and refuses what the service refuses for a request's shape. Each call can be scripted: entries stored or failed, an error before or after storing, a hand-written response, or a call that hangs until aborted. The cases cover options and the factory's ranges; keys reaching the service as the records hold them; numbered keys kept across retries; opaque payloads; refused records under each `dlq` policy, with what reaches the dead-letter path; records and requests at their limits and one byte past; a record over the stream's own maximum; partial failures resending only the failed records with their keys; retry bounds in attempts and in time; throttled and transient requests; ambiguous responses, the false success and the out-of-bounds read of the earlier sink among them; permanent errors never dead-lettered; a lost reply; cancellation in a wait and in a call; a barrier that waits for its records; repeated barriers after a failure; the end of input and close; reopening and replay; and linger.

**Deterministic, through the SDK.** `impls/aws/tests/test_kinesis_sink_sdk.cpp` runs the sink with its production SDK client against `impls/aws/tests/kinesis_http_stub.hpp`, an in-process stand-in for the `PutRecords` endpoint that records each request as sent and replies as scripted. It shows each record's key and bytes as the SDK sends them, a stream named by ARN, responses and errors read off the wire (a missing `FailedRecordCount` reads 0), ambiguous responses failing the sink, one request per attempt (the SDK's retries are off), a reply lost after the stream stored the request, and cancellation while the SDK sends and while a reply is held.

**Against the emulator.** LocalStack 3.8.1 refuses a partition key past 256 UTF-16 code units (it accepts 128 emoji and refuses 129) or an empty one, and refuses data past 1 MiB. It does not count the key against the record size, has no request byte or count limit (it took 11 MB and 501 records in one request), never fails an entry, never throttles and has no maximum-record-size setting. `impls/aws/tests/test_kinesis_sink_live.cpp` (`KinesisSinkLive`, four cases) writes through the production sink and reads the stream back with a client of its own, comparing every record's bytes and partition key, and requiring each key's records to share one shard: keys of every type, the exact numbers among them, through direct construction; opaque payloads of every byte value and a record at the limit through the registered factory, and one a byte over it never sent; refused records never reaching the stream under either policy; and a missing stream. `tests/integration/test_kinesis_sink_recovery.cpp` (`KinesisSinkRecovery`, three cells) runs a coordinator and two workers reading one emulator stream into the sink, which writes a second keyed by each record's `key`, while the test writes 1,200 unique records. The cells kill worker 0 parked after a store no checkpoint covered and parked before a send, both at parallelism 1, and a SQL job (a `connector='kinesis'` table into another with `partition_key='key'`) at parallelism 2 parked after a store. Each reads the output stream back and checks every record at least once with its own key, and nothing else; the cells parked after a store also require that request's records to arrive twice, and at parallelism 1 that the output did not move while the sink was parked.

**Not verified against AWS.** Nothing here ran against AWS. Partial failures, throttling, ambiguous responses and lost replies are covered by the model and the stand-in only. The emulator does not enforce the record and request limits the service documents, so the sink's own limits are shown only by what never reaches the stream. How AWS counts a key's 256 characters, and whether a stream outside the large-record regions takes 10 MiB requests, were not tested; the defaults are safe under either reading. The earlier sink was reproduced through the SDK against the stand-in: it collapsed neighbouring integer keys past 2^53 and fractions into one key, cut keys at 256 bytes, routed records without a usable key by its counter, took a response with no entries as success, made 36 requests for 9 attempts, and acknowledged a barrier after a failed flush had dropped its records.

The sink's phase of `scripts/kinesis-live.sh` runs every Kinesis sink suite and the recovery cells against the same LocalStack container, after the source's phase, and keeps its reports, its `provenance.txt` and its cells' logs under `<build-dir>/kinesis-live-reports/sink`, apart from the source's. It fails on a missing binary (`clink_submit_sql` included), a failed case, any skip, or a suite that ran fewer cases than it should. It passed locally on commit 0c0cb41f against LocalStack 3.8.1 on 2026-11-08, on a clean tree: 48 of 48, the live reader finding every record exactly once (1,212, 6 and 2 records), and the three recovery cells every record at least once with its own key and nothing else after one restart each, with 40, 0 and 34 duplicates (the cell killed before a send had stored nothing a checkpoint did not cover). The `aws` CI workflow runs it with the source's phase, and has not run it yet.

### The DynamoDB sink

**Deterministic, against a model of the service.** `impls/aws/tests/test_dynamodb_item.cpp` covers numbers (equal numerals, the canonical text, neighbouring large integers, precision and range, JSON's number grammar, sizes), item identity, the record checks and DynamoDB's size rules, a check that the request bound is never below the JSON the SDK writes, and the SDK's cutting of strings at NUL. `impls/aws/tests/test_dynamodb_sink.cpp` runs the production `DynamoDbSink` against `impls/aws/tests/dynamodb_model.hpp`, a model of `DescribeTable` and `BatchWriteItem` that keeps items by its own key equality and refuses what the service refuses for a request's shape: more than 25 writes, two writes to one item, a key of the wrong type or size, an invalid number. Each call can be scripted: `UnprocessedItems`, an error, a reply lost after the writes were applied, a hand-written response, or a call that hangs until aborted. The cases cover options and the factory's ranges, the table check at open and its retries, credentials removed from messages, identity through the sink, refused records and what they leave written, the 400 KB and 16 MiB limits, partial writes, retry bounds in attempts and in time, every kind of inconsistent response, permanent errors, a lost reply, cancellation in a wait and in a call, a barrier that waits for its writes, the failed state, a restart that converges, linger and close.

**Against the emulator.** LocalStack (`localstack/localstack:3`, version 3.8.1 here) serves DynamoDB with DynamoDB Local. Probed with the AWS CLI, it treats 1, 1.0, 1E0, 10E-1 and 0.1E1 as one key and 0 and -0 as another, refuses a batch naming one item twice, stores strings that differ only after a NUL as different keys, refuses 39 significant digits, a magnitude outside the range, an empty attribute name at any level and a 32nd nesting level, and counts an item's size as the sink does, accepting 409,600 bytes and refusing 409,601, numbers by their aligned digit pairs. `impls/aws/tests/test_dynamodb_live.cpp` (`DynamoDbLive`, six cases) writes through the production sink and its SDK client, and reads each table back with a separate client, comparing complete items. Its cases: every identity case through the registered factory, the key-schema checks at open, items at each limit stored and one past it never sent, a batch past 16 MiB split, a table deleted under the sink (the barrier fails and a restart converges), and an SDK call in flight ended by abort. `tests/integration/test_dynamodb_recovery.cpp` (`DynamoDbRecovery`, three cells) runs a coordinator and two workers reading a Kinesis stream into the sink while the test writes 1,200 records for 48 items, each sort key written as four equal numerals. The cells kill worker 0 parked after a write the service applied and no checkpoint covered, at parallelism 1 and at 2, and parked with a request built and not sent. Each checks every item against the last record written for it, attribute by attribute, and that the table holds nothing else.

**Not verified against AWS.** Nothing here ran against AWS. The emulator does not throttle, never returns `UnprocessedItems`, and has no partitions, permissions or local secondary index accounting, so partial writes, throttling, inconsistent responses and lost replies are covered by the model only. The same emulator ran the sink as it was before this change: it stored two records whose keys differed only around a NUL as one item, under a key neither had; collapsed two integer keys past the 64-bit range into one item holding the second record's value; failed a batch, its valid records included, after coalescing the string "1" with the number 1; and stored 12345678901234567890.123456789 as 12345678901234567168.

The gate is `scripts/dynamodb-live.sh`: it starts LocalStack with DynamoDB and Kinesis on a free loopback port, waits for DynamoDB Local to answer, runs every DynamoDB suite and the recovery cells, and fails on a missing binary, an emulator that never answers, a failed case, any skip, or a suite that ran fewer cases than it should. It keeps each run's report and output, the cells' coordinator and worker logs, and `provenance.txt` (the commit, the build's configuration, the emulator's version and image) under `<build-dir>/dynamodb-live-reports`. It passed locally on commit 0ad045c2 against LocalStack 3.8.1 on 2026-11-06: 60 of 60, the three recovery cells converging on all 48 items with one restart each. It passed in CI on commit fb32551e, 60 of 60 (run 42285865904 of the `aws` workflow), the in-flight abort case included on the toolchain image's SDK.

```bash
cmake --build build --target clink_aws_tests clink_integration_tests clink_node --parallel 10
scripts/dynamodb-live.sh build
```

### The Firehose sink

**Deterministic, against a model of the service.** `impls/aws/tests/test_firehose_sink.cpp` checks the request accounting against the JSON the SDK itself writes, from empty records to records of 1,000 KiB and binary bytes, and the response judge on complete and partial responses and on every kind that does not account for its request: missing, short and oversized entry arrays, entries with both or neither result, and `FailedPutCount` values that contradict the entries. It runs the production `FirehoseSink` against `impls/aws/tests/firehose_model.hpp`, a model of `PutRecordBatch` that keeps every record it accepts, duplicates included, and refuses what the service refuses for a request's shape. Each call can be scripted: records accepted or failed, an error before or after acceptance, a hand-written response, or a call that hangs until aborted. The cases cover options and the factory's ranges; a record at the limit sent and one byte more refused; empty and binary payloads and the delimiter; requests that never outgrow their limits; partial acceptance resending only the failed records; retry bounds in attempts and in time; throttled and transient requests; the regressions of a response taken as success while missing entries and of an entry read past the request; permanent errors; a reply lost after acceptance; cancellation in a wait and in a call; a barrier that waits for acceptance; repeated barriers after a failure; the end of input and close; reopening after a failure; and linger.

**Against the emulator.** LocalStack 3.8.1 serves `PutRecordBatch` with one `RecordId` per record, and delivers a DirectPut stream to its own S3 at once, one object per request with the records' bytes concatenated, once STS is enabled (the stream assumes its role to write). It does not enforce the per-record limit: it accepted a record of 1,024,001 bytes. `impls/aws/tests/test_firehose_live.cpp` (`FirehoseLive`, five cases) writes through the production sink and its SDK client and reads the bucket back with the SDK's S3 client, splitting each object at the delimiter and comparing every record byte for byte, duplicates allowed. Its cases: text, every byte value, an empty record, two records at 1,000 KiB and 1,200 small ones through the registered factory; binary records split by a two-byte delimiter; a record over the limit refused before it is sent; a missing stream; and an SDK call in flight ended by abort. `tests/integration/test_firehose_recovery.cpp` (`FirehoseRecovery`, three cells) runs a coordinator and two workers reading a Kinesis stream into the sink while the test writes 1,200 unique records. The cells kill worker 0 parked after `PutRecordBatch` accepted a batch no checkpoint covered, at parallelism 1 and 2, and parked before a send. Each checks that the bucket holds every record at least once and nothing else; the two parked after an acceptance also require that batch to arrive again.

**Not verified against AWS.** Nothing here ran against AWS. The emulator never fails an entry, never throttles and never loses a reply, so partial acceptance, throttling, ambiguous responses and lost replies are covered by the model only. It delivers at once, without Firehose's buffering, delivery retries or 24-hour retention, so nothing here shows delivery timing or a destination outage. Whether AWS counts base64 against the 4 MiB request limit was not tested; the sink's accounting is safe under either reading.

The gate is `scripts/firehose-live.sh`: it starts LocalStack with Firehose, S3, STS, IAM and Kinesis on a free loopback port, waits for its Firehose API, runs every Firehose suite and the recovery cells, and fails on a missing binary, an emulator that never answers, a failed case, any skip, or a suite that ran fewer cases than it should. It keeps each run's report and output, the cells' coordinator and worker logs, and `provenance.txt` (the commit, the build's configuration, the emulator's version and image) under `<build-dir>/firehose-live-reports`. It passed locally on commit 23c77d0f against LocalStack 3.8.1 on 2026-11-07: 36 of 36, every record of the three recovery cells reaching the bucket (with 52, 40 and 40 duplicates) after one restart each. The `aws` CI workflow runs it after the DynamoDB gate, and passed it on commit 086e6bbe, 36 of 36 (run 42340146607), each recovery cell's records all reaching the bucket with 40 duplicates and nothing missing or unexpected; the same run passed the Kinesis gate, 46 of 46, and the DynamoDB gate, 60 of 60.

```bash
cmake --build build --target clink_aws_tests clink_integration_tests clink_node --parallel 10
scripts/firehose-live.sh build
```

To run the live cases by hand against LocalStack from `docker/integration-services.yml` (its service enables Kinesis and S3; the DynamoDB and Firehose cases need more, as their gates set):

```bash
docker compose -f docker/integration-services.yml run -d -e SERVICES=dynamodb,firehose,s3,sts,iam,kinesis -p 4566:4566 localstack
export CLINK_KINESIS_TEST_ENDPOINT=http://localhost:4566
export CLINK_DYNAMODB_TEST_ENDPOINT=http://localhost:4566
export CLINK_FIREHOSE_TEST_ENDPOINT=http://localhost:4566
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
ctest --test-dir build -L aws
```

LocalStack accepts any credentials; the dummy keys above only let the SDK sign the requests.
