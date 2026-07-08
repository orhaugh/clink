# Apache Kafka

> Source and sink for Apache Kafka topics over librdkafka. It reads and writes both text/JSON payloads (the `string` channel) and full broker records (the `KafkaMessage` channel).

## Overview

The Kafka connector consumes from and produces to Kafka topics using the librdkafka C/C++ client. It exposes two channel shapes: a `std::string` payload channel for plain text or JSON values, and a `KafkaMessage` channel that preserves the full broker record (payload, optional key, headers, offset, partition and timestamp). Records carry their bytes verbatim; the connector does no schema-aware encoding of its own, so the value bytes are whatever the producing or consuming pipeline puts on the channel (JSON when reached through the SQL frontend). The source binds its consumer position to clink checkpoints so it can replay from a recorded per-partition offset on recovery.

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| librdkafka (`rdkafka++`, `rdkafka`) | System package via apt (Debian) / brew (macOS) | Not pinned by clink |

The connector links librdkafka only; it does not use Arrow on its I/O path. CMake discovers the client in three tiers (CMake config package `RdKafka`, then pkg-config `rdkafka`/`rdkafka++`, then a manual header/library probe under the common Homebrew, `/usr/local` and `/usr` prefixes), so Homebrew, vcpkg, Confluent and source builds all resolve without setting `CMAKE_PREFIX_PATH`.

## Enabling it

Controlled by the `CLINK_WITH_KAFKA` CMake option, which defaults to `AUTO`:

- `AUTO`: build the connector if librdkafka is found, otherwise skip it.
- `ON`: require librdkafka; configuration fails if it is not found.
- `OFF`: do not define the target.

```bash
cmake -S . -B build -DCLINK_WITH_KAFKA=ON
cmake --build build -j
```

When librdkafka ships `librdkafka/rdkafka_mock.h` (version 1.3 and later), CMake also sets `CLINK_HAS_KAFKA_MOCK`, which enables the in-process mock-broker test suite. No Arrow build flag is required.

## Factories

| Factory name | Direction | Record type |
| --- | --- | --- |
| `kafka_message_source` | Source | `KafkaMessage` |
| `kafka_message_sink` | Sink | `KafkaMessage` |
| `kafka_text_source` | Source | `std::string` |
| `kafka_source_string` | Source | `std::string` |
| `kafka_text_sink` | Sink | `std::string` |
| `kafka_sink_string` | Sink | `std::string` |
| `kafka_2pc_sink_string` | Sink | `std::string` |
| `kafka_upsert_sink_string` | Sink | `std::string` |

`kafka_text_source` and `kafka_source_string` register the same builder, as do `kafka_text_sink` and `kafka_sink_string`. The `_string` aliases exist because the SQL planner emits those op-type names. These factories become resolvable once `clink::kafka::install(registry)` is called against the plugin registry.

## Configuration

Options are read from `BuildContext` parameters in `impls/kafka/src/register_factories.cpp` and map onto the `KafkaSource::Options` and `KafkaSink::Options` structs in `impls/kafka/include/clink/connectors/kafka_source.hpp` and `kafka_sink.hpp`.

### Source (`kafka_message_source`, `kafka_text_source`, `kafka_source_string`)

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `brokers` | Yes | (none) | Bootstrap broker list, for example `localhost:9092`. |
| `topic` | Yes | (none) | Topic to consume. |
| `group_id` | No | `clink` | Consumer group id. |
| `client_id` | No | `clink-source` | librdkafka client id. |
| `auto_offset_reset` | No | `earliest` | Where to start when no committed/restored offset exists: `earliest`, `latest` or `none`. |
| `batch_max_wait_ms` | No | `5` | Bounds TOTAL batch formation time in the source's poll loop. Waiting for the first record of a batch still blocks up to `poll_timeout` (idle stays cheap); once a batch has begun, the fill loop stops when this bound elapses and emits a partial batch instead of waiting to accumulate `max_batch_size` records. Keeps per-record latency on a paced or trickling input proportional to this bound rather than `max_batch_size / input-rate`; a saturated consumer queue fills `max_batch_size` well inside the bound, so throughput at the ceiling is unaffected. `0` disables the bound. |

The `KafkaSource::Options` struct also carries `poll_timeout` (100 ms), `max_batch_size` (256), `commit_mode` (`Auto`), `enable_debug` (false) and `metric_prefix` (`default`). These are not parsed from `BuildContext` parameters in the registered factories, so they take their struct defaults unless the typed class is constructed directly.

### Sink (`kafka_message_sink`, `kafka_text_sink`, `kafka_sink_string`)

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `brokers` | Yes | (none) | Bootstrap broker list. |
| `topic` | Yes | (none) | Destination topic. |
| `client_id` | No | `clink-sink` | librdkafka client id. |
| `acks` | No | `all` | Producer acknowledgement mode: `all`, `1` or `0`. |
| `compression` | No | `none` | Compression codec: `none`, `gzip`, `snappy`, `lz4` or `zstd`. |
| `linger_ms` | No | `5` | Producer batching delay (librdkafka `linger.ms`), a non-negative integer of milliseconds. `0` sends as soon as the producer loop runs, trading batching efficiency for per-record latency. An invalid value fails the deploy with a clear error. |

The `KafkaSink::Options` struct additionally carries `produce_timeout` (30000 ms), `flush_timeout` (30000 ms), `fixed_partition` (unset) and `metric_prefix` (`default`), which are not parsed from `BuildContext` parameters and take their struct defaults.

### Transactional sink (`kafka_2pc_sink_string`)

Accepts `brokers`, `topic`, `client_id` (default `clink-sink-2pc`), `compression` and `linger_ms`, plus:

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `transactional_id` | Yes | (none) | librdkafka `transactional.id`. Must be unique per producer instance. When `parallelism > 1` the factory appends the subtask index. |
| `commit_group` | No | (none) | Declares commit-group membership so the job manager can gate this sink's commit on its group peers. |

This factory does not parse `acks`; the transactional producer is configured with `enable.idempotence=true` by the underlying sink.

### Upsert sink (`kafka_upsert_sink_string`)

Accepts `brokers`, `topic`, `client_id` (default `clink-sink`), `acks`, `compression` and `linger_ms`, plus:

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `primary_key` | Yes | (none) | Comma-separated list of JSON object fields to extract as the Kafka message key. |

Each incoming row must be a JSON object. A field named `__row_kind` is interpreted as the change kind: `delete` emits a tombstone (empty payload keyed by the primary key), `update_before` is dropped, and `insert`/`update_after` emit the row JSON minus `__row_kind` as the payload.

### Authentication and TLS

All the factories above (sources, sinks, the transactional and upsert sinks) accept SASL and TLS options, mapped onto librdkafka config properties and applied verbatim when the client opens. SASL_PLAINTEXT / SASL_SSL with PLAIN or SCRAM, and SSL / mTLS, are all reachable:

| Option | librdkafka property | Description |
| --- | --- | --- |
| `security_protocol` | `security.protocol` | `plaintext`, `ssl`, `sasl_plaintext` or `sasl_ssl`. |
| `sasl_mechanism` | `sasl.mechanism` | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `GSSAPI`, `OAUTHBEARER`. |
| `sasl_username` / `sasl_password` | `sasl.username` / `sasl.password` | SASL credentials. |
| `ssl_ca_location` | `ssl.ca.location` | CA certificate (path) for verifying the broker. |
| `ssl_certificate_location` / `ssl_key_location` / `ssl_key_password` | `ssl.certificate.location` / `ssl.key.location` / `ssl.key.password` | Client certificate, key and key password for mTLS. |
| `ssl_endpoint_identification_algorithm` | `ssl.endpoint.identification.algorithm` | `https` to verify the broker hostname, `none` to disable. |
| `enable_ssl_certificate_verification` | `enable.ssl.certificate.verification` | `false` to skip broker-cert verification (testing only). |

For any librdkafka property the aliases do not cover, an escape hatch passes it through verbatim: a WITH-option (or programmatic `Options.conf` entry) keyed `kafka.<property>` sets librdkafka `<property>` directly, e.g. `kafka.client.rack`. librdkafka validates each key/value when the client opens and throws on an unknown or unsupported one (for example an SSL setting on a build without SSL support). Credentials are passed through but never logged by the connector.

## SQL usage

Mapped in `src/sql/physical_plan.cpp` under `connector='kafka'`. The planner selects the underlying factory from the table's delivery mode: plain tables use `kafka_source_string` / `kafka_sink_string`, `mode='upsert'` sinks use `kafka_upsert_sink_string`, and exactly-once sinks use `kafka_2pc_sink_string`. Row values are bridged to and from JSON strings (`row_to_json_string` on the sink side, `json_string_to_row` on the source side).

```sql
CREATE TABLE clicks (
  user_id BIGINT,
  url     STRING
) WITH (
  connector = 'kafka',
  format    = 'json',
  brokers   = 'localhost:9092',
  topic     = 'clicks',
  group_id  = 'analytics',
  auto_offset_reset = 'earliest'
);

CREATE TABLE click_counts (
  user_id BIGINT,
  n       BIGINT
) WITH (
  connector   = 'kafka',
  format      = 'json',
  brokers     = 'localhost:9092',
  topic       = 'click-counts',
  mode        = 'upsert',
  primary_key = 'user_id'
);
```

A source table may set `columnar_decode='true'` to swap the row-form JSON bridge for the columnar one (`json_string_to_row_columnar`), which attaches an Arrow sidecar. The default is row-form.

## Example

Programmatic use of the typed connector classes, based on `impls/kafka/tests/test_kafka.cpp`:

```cpp
#include "clink/connectors/kafka_sink.hpp"
#include "clink/connectors/kafka_source.hpp"

using namespace clink;

// Sink: produce ten records to a topic.
KafkaSink::Options sink_opts;
sink_opts.brokers = "localhost:9092";
sink_opts.topic   = "round-trip";
KafkaSink sink(std::move(sink_opts));
sink.open();

Batch<KafkaMessage> batch;
for (int i = 0; i < 10; ++i) {
    batch.emplace(KafkaMessage{"payload-" + std::to_string(i)});
}
sink.on_data(batch);
sink.flush();
// sink.delivered_count() == 10, sink.delivery_error_count() == 0

// Source: consume them back.
KafkaSource::Options src_opts;
src_opts.brokers           = "localhost:9092";
src_opts.topic             = "round-trip";
src_opts.group_id          = "rt-group";
src_opts.auto_offset_reset = "earliest";
KafkaSource source(std::move(src_opts));
source.open();
// source.produce(emitter) emits Batch<KafkaMessage> until cancelled.
```

When going through the plugin registry, call `clink::kafka::install(registry)` once after the built-ins are registered, then look up a factory by one of its registered names.

## Delivery semantics

- Source replay (exactly-once on the read side): the source records a per-partition next-offset map into the clink checkpoint via `snapshot_offset`, and on `restore_offset` it seeks each partition to the restored offset on assignment through a rebalance callback. This makes the clink checkpoint the source of truth for the consumer position on recovery rather than Kafka's own committed offset. The `StringKafkaSource` adapter delegates these hooks to the inner `KafkaSource` so the string and SQL paths retain replay.
- Plain sink (`kafka_text_sink` / `kafka_sink_string`): at-least-once. Records are produced and flushed; on every successful broker ack a `delivered` counter increments, and delivery failures after retry increment `delivery_errors`. With `acks=all` writes are durable, but there is no transactional rollback, so a failure after a partial flush can leave records on the topic.
- Transactional sink (`kafka_2pc_sink_string`): two-phase commit. Records are produced inside an open librdkafka transaction; a checkpoint barrier flushes and records the pending checkpoint id; the broker-side `commitTransaction` happens only when the job manager signals the checkpoint globally durable (`on_commit`). `on_abort` aborts the prepared transaction, and `close` aborts any still-open transaction so records are not left in the prepared state.
- Upsert sink (`kafka_upsert_sink_string`): keyed at-least-once with log-compaction semantics. Each row is keyed by the configured primary key, deletes become tombstones, and replacement relies on Kafka log compaction by key.

## Limitations

- librdkafka commit-mode, batching and timeout knobs (`commit_mode`, `poll_timeout`, `max_batch_size`, `linger_ms`, `produce_timeout`, `flush_timeout`, `fixed_partition`, `metric_prefix`) exist on the `Options` structs but are not parsed from factory parameters, so through the registered factories they always take their struct defaults.
- SASL and TLS are configured through the options in the Authentication and TLS section above (plus the generic `kafka.<property>` escape hatch); other security mechanisms are reachable only via that escape hatch.
- The transactional sink requires a unique `transactional_id`; with `parallelism > 1` the factory appends the subtask index, and the uniqueness contract across producer instances is otherwise the caller's responsibility.
- The upsert sink requires every input value to be a JSON object; malformed or non-object values are silently skipped, and `primary_key` is mandatory.
- Sink delivery is at-least-once unless the transactional (`kafka_2pc_sink_string`) variant is used.

## Testing

The test suite (`impls/kafka/tests/test_kafka.cpp`) drives librdkafka's in-process mock cluster (`rdkafka_mock.h`), which speaks the real Kafka wire protocol on a localhost port. There is no external broker, Docker image or environment-variable gate; the tests run in under a second locally. They self-skip when the build has `CLINK_HAS_KAFKA` off (librdkafka absent) or `CLINK_HAS_KAFKA_MOCK` off (librdkafka older than 1.3, which lacks the mock header).

Run them with the `kafka` ctest label:

```bash
cmake -S . -B build -DCLINK_WITH_KAFKA=ON -DCLINK_BUILD_TESTS=ON
cmake --build build -j
ctest --test-dir build -L kafka
```
