# HTTP connectors

> A family of HTTP(S) connectors (generic webhook sink, Elasticsearch / OpenSearch, Splunk HEC, InfluxDB, Prometheus Pushgateway, an HTTP polling source, and Google Cloud Pub/Sub) built on one vendored HTTP client. The module provides both sinks and sources.

## Overview

`clink::http_connector` is a single module that registers several connectors over a shared keep-alive HTTP/1.1 client. Every connector carries records as `std::string`: on the SQL path each row is rendered to a JSON object string (`row_to_json_string`), and a polling source emits each JSON array element as a single-line JSON string. The sinks buffer records and POST them in batches; the request body framing is per connector (a JSON array, NDJSON action+doc pairs for the `_bulk` API, InfluxDB line protocol, a Prometheus exposition body, or a Pub/Sub `messages` envelope). The transport is the vendored cpp-httplib library, confined behind a pImpl in `http_request.cpp`; HTTPS is available only when that library is built with OpenSSL.

The same `HttpRequest` transport is reused by the WebHDFS connector in a separate module.

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| cpp-httplib | Vendored into the build via FetchContent (header-only) | Not pinned by clink in `scripts/versions.env` |
| OpenSSL | System package via apt (Debian) / brew (macOS); used only for HTTPS, via cpp-httplib's own CMake | Not pinned by clink |

There is no Arrow dependency on this module. The connectors do not link Arrow; records flow as JSON strings.

## Enabling it

The CMake knob is `CLINK_WITH_HTTP` (`AUTO` / `ON` / `OFF`, default `AUTO`, set in the top-level `CMakeLists.txt`). cpp-httplib is vendored at the top level and is header-only, so the module builds whenever the `httplib::httplib` target is present; there is no external client library to install. With `CLINK_WITH_HTTP=ON` the build fails if that target is absent; with `AUTO` it is silently skipped.

HTTPS support is decided by cpp-httplib's own CMake: when it finds OpenSSL it sets `CPPHTTPLIB_OPENSSL_SUPPORT` on the interface target, which propagates to this module. On a build without OpenSSL, `HttpRequest::tls_supported()` returns false and every sink and source rejects an `https://` URL up front with a clear error.

```bash
cmake -S . -B build -DCLINK_WITH_HTTP=ON
```

## Factories

| Factory name | Direction | Record type |
| --- | --- | --- |
| `http_sink` | Sink | `std::string` |
| `elasticsearch_sink` | Sink | `std::string` |
| `opensearch_sink` | Sink | `std::string` |
| `splunk_hec_sink` | Sink | `std::string` |
| `influxdb_sink` | Sink | `std::string` |
| `prometheus_sink` | Sink | `std::string` |
| `http_poll_source` | Source | `std::string` |
| `pubsub_sink` | Sink | `std::string` |
| `pubsub_source` | Source | `std::string` |

All factory names above are the exact strings passed to `register_sink<std::string>` / `register_source<std::string>` in `impls/http_connector/src/register_factories.cpp`.

## Configuration

Options are parsed from the `BuildContext` in `register_factories.cpp` and map onto each connector's `Options` struct. The tables below list the keys per factory.

### Shared options

These keys are read by every connector unless noted otherwise (defaults and the `[0, 20]` `max_retries` clamp come from the bulk-sink base in `batched_http_bulk_sink.hpp` and the per-connector factory code):

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `url` | Yes | (none) | `scheme://host[:port]` base URL. `https://` requires an OpenSSL build. |
| `headers` | No | (empty) | Extra request headers as `"K1: V1; K2: V2"`. Used for auth (e.g. `Authorization: Bearer ...`). |
| `verify_tls` | No | `true` | HTTPS server-certificate verification; `false` skips it. |
| `batch_records` | No | `500` | Flush when this many records are buffered. |
| `batch_bytes` | No | `4194304` | Flush when the buffered body reaches this many bytes. |
| `max_retries` | No | `4` | Retry attempts beyond the first; clamped to `[0, 20]`. |
| `linger_ms` | No | `0` | Flush a partial batch once its oldest record is this old (evaluated on record arrival, not on a timer). `0` disables. |
| `dlq` | No | `fail` | `drop` routes a permanently-rejected (4xx) batch to the dead-letter path (a `BadRecord` report plus metrics) instead of failing the job. Any other value keeps the fail-and-replay default. |

`http_poll_source` and the Prometheus sink do not use all of these; their connector-specific keys are below.

### `http_sink`

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `path` | No | `/` | Request path. |
| `bulk_format` | No | `json_array` | Request body framing: `json_array` (`[rec0,rec1,...]`) or `ndjson` (newline-delimited). Named `bulk_format`, not `format`, because `format` is the SQL channel selector. |
| `content_type` | No | `application/json` | Request `Content-Type`. |

### `elasticsearch_sink` / `opensearch_sink`

Identical `_bulk` API; records are framed as NDJSON action+doc pairs.

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `index` | Yes | (none) | Target index for the `_index` action metadata. |
| `document_id` | No | (empty) | Record field used as the document `_id`; setting it makes re-delivery an idempotent overwrite. |
| `path` | No | `/_bulk` | Bulk endpoint path. |

### `splunk_hec_sink`

Posts events to a Splunk HTTP Event Collector with `Authorization: Splunk <token>`.

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `token` | Yes | (empty) | HEC token; sets the `Authorization` header. |
| `path` | No | `/services/collector/event` | HEC event endpoint. |
| `sourcetype` | No | (empty) | Event `sourcetype` (e.g. `_json` to auto-extract fields). |
| `source` | No | (empty) | Event `source` metadata. |
| `host` | No | (empty) | Event `host` metadata. |
| `index` | No | (empty) | Target index (must be writable by the token). |

### `influxdb_sink`

Converts each JSON-object row to one InfluxDB v2 line-protocol point and POSTs NDJSON batches to `/api/v2/write` with `Authorization: Token <token>`.

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `org` | Yes | (empty) | InfluxDB v2 organisation. |
| `bucket` | Yes | (empty) | InfluxDB v2 bucket. |
| `token` | Yes | (empty) | API token; sets the `Authorization` header. |
| `measurement` | Yes | (empty) | Line-protocol measurement name. |
| `tags` | No | (empty) | CSV of row fields to emit as tags; remaining scalar fields become fields. |
| `timestamp_field` | No | (empty) | Row field holding the point timestamp; otherwise the server assigns it. |
| `precision` | No | `ns` | Write precision: `ns`, `us`, `ms`, or `s`. |

The required fields above are validated by `make_influxdb_sink` and the factory; the factory reads them with empty defaults, so they are required in practice rather than by a `param_or` default.

### `prometheus_sink`

Pushes batched records as gauge samples to a Prometheus Pushgateway. This sink does not read `batch_bytes` or `dlq`.

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `job` | Yes | (empty) | Grouping job (first path segment under `/metrics/job/`). |
| `grouping` | No | (empty) | Extra static path labels as `k=v,k2=v2`. |
| `metric_name` | No | `clink_value` | The single gauge metric name. |
| `value_field` | No | `value` | Record field holding the gauge value. |
| `help` | No | (empty) | Optional `# HELP` text. |
| `batch_records` | No | `500` | Maximum distinct series per push. |

### `http_poll_source`

GETs a JSON REST endpoint on an interval and emits the array elements, with a checkpointed cursor. This source does not use `batch_records` / `batch_bytes` / `dlq` / `linger_ms`.

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `path` | No | `/` | Request path (may carry a fixed query string). |
| `headers` | No | (empty) | Request headers (auth). |
| `verify_tls` | No | `true` | HTTPS certificate verification. |
| `cursor_param` | No | (empty) | Query-parameter name to send the cursor as. |
| `cursor_field` | No | (empty) | Record field holding the next cursor; the API must return records ascending by it. |
| `records_field` | No | (empty) | Response field holding the array; if empty, the response itself is the array. |
| `initial_cursor` | No | (empty) | Starting cursor on a fresh run. |
| `poll_interval_ms` | No | `1000` | Interval between polls. |
| `jitter_frac` | No | `0` | Fraction of jitter applied to the poll interval. |
| `bounded` | No | `false` | `true` stops after a poll returns nothing (one-shot). |
| `max_retries` | No | `4` | Transient-GET retries within one poll; clamped to `[0, 20]`. |
| `retry_base_backoff_ms` | No | `200` | Base backoff between transient retries. |

### `pubsub_sink` / `pubsub_source`

Google Cloud Pub/Sub over the REST v1 API. The base URL resolves in order: the `endpoint` option, else the `PUBSUB_EMULATOR_HOST` environment variable (`host:port` becomes `http://host:port`), else `https://pubsub.googleapis.com`.

Shared Pub/Sub options:

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `endpoint` | No | (env / real service) | Override base URL (e.g. an emulator). |
| `auth_token` | No | (empty) | Static bearer token; sets `Authorization: Bearer <token>`. Valid only for its lifetime (no refresh). |
| `auth_token_file` | No | (empty) | Path to a file holding the bearer token, re-read per request when the file changes. An external refresher (Workload Identity / sidecar / Vault agent) rotating the file is picked up without restarting the job. Takes precedence over `auth_token`. |
| `headers` | No | (empty) | Extra headers, merged after the token (an explicit `Authorization` header wins). |
| `verify_tls` | No | `true` | HTTPS certificate verification. |
| `max_retries` | No | `4` | Retry attempts; clamped to `[0, 20]`. |

`pubsub_sink` additional options:

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `project` | Yes | (none) | GCP project id. |
| `topic` | Yes | (none) | Topic id. |
| `batch_records` | No | `1000` | Messages per publish; capped at the 1000-message Pub/Sub limit. |
| `batch_bytes` | No | `9437184` | Flush threshold in bytes; stays under the request size limit. |
| `dlq` | No | `fail` | Permanent-failure policy (`fail` / `drop`). |
| `linger_ms` | No | `0` | Partial-batch linger. |

`pubsub_source` additional options:

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `project` | Yes | (none) | GCP project id. |
| `subscription` | Yes | (none) | Subscription id. |
| `max_messages` | No | `1000` | Pull `maxMessages`; capped at 1000. |
| `return_immediately` | No | `true` | `Pull` `returnImmediately` flag. |
| `poll_interval_ms` | No | `500` | Sleep after an empty pull. |
| `retry_base_backoff_ms` | No | `200` | Base backoff between transient-pull retries. |

Authentication note: this build does not mint OAuth2 tokens. Supply a bearer token via `auth_token` (static) or `headers`, or via `auth_token_file` (re-read per request, so an external refresher rotating the file is picked up without a restart); against the Pub/Sub emulator no auth is needed.

## SQL usage

Several of these connectors are mapped in `src/sql/physical_plan.cpp`. The SQL `connector='...'` name differs from the factory name in some cases:

| SQL `connector` | Factory | Direction |
| --- | --- | --- |
| `http` | `http_sink` | Sink |
| `elasticsearch` | `elasticsearch_sink` | Sink |
| `opensearch` | `opensearch_sink` | Sink |
| `splunk_hec` (or `splunk`) | `splunk_hec_sink` | Sink |
| `influxdb` | `influxdb_sink` | Sink |
| `prometheus` | `prometheus_sink` | Sink |
| `http_poll` | `http_poll_source` | Source |
| `pubsub` | `pubsub_sink` / `pubsub_source` | Both |

`http_sink`, `elasticsearch_sink`, `opensearch_sink`, `splunk_hec_sink`, `influxdb_sink`, and `prometheus_sink` are not registered as standalone SQL source/sink names; they are reached only through these `connector` aliases. The SQL planner rejects `exactly_once` and `upsert` modes on these sinks (they are at-least-once).

Sink example (Elasticsearch):

```sql
CREATE TABLE es_out (
    id   STRING,
    body STRING
) WITH (
    connector   = 'elasticsearch',
    format      = 'json',
    url         = 'http://es:9200',
    index       = 'events',
    document_id = 'id'
);
```

Source example (HTTP poll):

```sql
CREATE TABLE feed (
    id  BIGINT,
    msg STRING
) WITH (
    connector        = 'http_poll',
    format           = 'json',
    url              = 'http://api:8080',
    path             = '/events',
    records_field    = 'items',
    cursor_param     = 'since',
    cursor_field     = 'id',
    poll_interval_ms = '2000'
);
```

A `connector='http'` table must declare `format='json'` so the row is rendered to a JSON string before the string-channel `http_sink` receives it.

## Example

Programmatic use of the Elasticsearch sink, based on the live test (`tests/test_elasticsearch_sink_live.cpp`). Each record is already a JSON object string:

```cpp
#include "clink/http_connector/bulk_sink_builders.hpp"

using namespace clink::http_connector;

EsBulkOptions o;
o.url = "http://es:9200";
o.index = "events";
o.document_id = "id";   // re-delivery overwrites the same _id
o.batch_records = 100;

auto sink = make_es_bulk_sink(o);
sink->open();
sink->on_data(/* Batch<std::string> of JSON object strings */);
sink->flush();          // also runs on every checkpoint barrier
```

Via the registry (string channel), the same connector is reachable by its factory name:

```cpp
const auto& rr = clink::cluster::RunnerRegistry::default_instance();
auto factory = rr.find_sink("elasticsearch_sink", "string");
```

## Delivery semantics

- Sinks (`http_sink`, `elasticsearch_sink`, `opensearch_sink`, `splunk_hec_sink`, `influxdb_sink`, `prometheus_sink`, `pubsub_sink`) are at-least-once. The shared `BatchedHttpBulkSink` flushes on every checkpoint barrier (before the sink acks), on the size and linger thresholds, and at end-of-stream. On permanent failure with retries exhausted it throws rather than dropping, so the job replays from the last checkpoint. There is no two-phase commit, so a replay re-delivers buffered records.
- `elasticsearch_sink` / `opensearch_sink` upgrade toward effectively-once when `document_id` is set: a replayed document overwrites the same `_id` rather than duplicating. The `_bulk` response is parsed per item, so a transient per-item failure resends only the failed records and a permanent per-item failure is DLQ'd or thrown per the `dlq` policy.
- `influxdb_sink` is effectively-once when `timestamp_field` is set, since an identical point is idempotent.
- `splunk_hec_sink` and `prometheus_sink` are at-least-once with no dedup key; HEC ingestion is append, and the Pushgateway stores the latest value per series (last-write-wins within a push).
- `pubsub_sink` is at-least-once with no producer dedup; a replay re-publishes.
- `http_poll_source` is at-least-once: the cursor is checkpointed. Avoiding re-emission of the boundary record requires an exclusive cursor server-side.
- `pubsub_source` is at-least-once: it pulls, holds the ackIds, and acknowledges on the checkpoint (the ack is the offset commit); unacked messages are redelivered by the server after the subscription `ackDeadline`, which is also the crash-recovery mechanism. Two honest caveats from the source comments: the `:acknowledge` call is not transactional with the global checkpoint, so if a checkpoint acks a batch and the global checkpoint then fails, that one batch is at-most-once; and a message whose `data` is not valid base64 is acked and dropped (with an error metric) rather than redelivered forever.

## Limitations

- All connectors carry records as `std::string` only; there is no int64 or typed-Arrow channel. On the SQL path rows are serialised to JSON strings.
- No two-phase commit on any sink; exactly-once delivery is not offered. The SQL planner rejects `exactly_once` and `upsert` modes on these sinks.
- HTTPS works only if cpp-httplib was built with OpenSSL; otherwise an `https://` URL is rejected at construction.
- `prometheus_sink` targets a Pushgateway, which is a latest-value store for low-cardinality service metrics, not a high-throughput event TSDB. Records whose `value_field` is missing or non-numeric are dropped (no sample). Remote-write is not implemented.
- `http_poll_source` requires the API to return records ascending by `cursor_field`; it has no client-side ordering or de-duplication beyond the cursor.
- Pub/Sub uses the synchronous REST `Pull` with `returnImmediately`; gRPC StreamingPull is not used. The build does not mint OAuth2 tokens itself; a static `auth_token` expires after its lifetime, but `auth_token_file` lets an external refresher rotate the token without a restart. Multiple subtasks pull the same subscription and Pub/Sub load-balances across them (no client-side sharding).
- The bulk sinks buffer rendered records in memory until a flush trigger; `batch_records` / `batch_bytes` / `linger_ms` bound that buffer.
- `linger_ms` is evaluated only when a record arrives (no timer thread), so a fully idle buffer waits for the next checkpoint barrier.

## Testing

The module's tests live in `impls/http_connector/tests/` and carry the ctest label `http_connector`. Most are in-process or mock tests (factory registration, framing, the Splunk and Prometheus body builders) and run in a normal build:

```bash
ctest --test-dir build -L http_connector
```

Three live integration tests are skipped unless their endpoint environment variable is set, and gate on the `Live` name filter. The sidecar services are defined in `docker/integration-services.yml`:

| Test | Env var(s) | Service / image |
| --- | --- | --- |
| Elasticsearch (`test_elasticsearch_sink_live.cpp`) | `CLINK_ELASTICSEARCH_TEST_ENDPOINT` (e.g. `http://localhost:9200`) | `docker.elastic.co/elasticsearch/elasticsearch:8.13.4` |
| InfluxDB (`test_influxdb_live.cpp`) | `CLINK_INFLUXDB_TEST_ENDPOINT`, plus optional `CLINK_INFLUXDB_TEST_ORG` / `_BUCKET` / `_TOKEN` (defaults `clink` / `clink` / `clink-token`) | `influxdb:2` |
| Pub/Sub (`test_pubsub_live.cpp`) | `CLINK_PUBSUB_EMULATOR_HOST` (e.g. `localhost:8085`) | `gcr.io/google.com/cloudsdktool/cloud-sdk:emulators` |

```bash
docker compose -f docker/integration-services.yml up -d
export CLINK_ELASTICSEARCH_TEST_ENDPOINT=http://localhost:9200
ctest --test-dir build -L http_connector -R Live --output-on-failure
docker compose -f docker/integration-services.yml down -v
```

There is no live test for `splunk_hec_sink`, `prometheus_sink`, or `http_poll_source`; these are covered by in-process and mock tests only.
