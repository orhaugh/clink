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
| `dlq` | No | `fail` | `drop` routes records the endpoint refuses as records (HTTP 400, 409 or 422) to the dead-letter path (a `BadRecord` report plus metrics) instead of failing the job. A refusal about the endpoint rather than the records, such as 401, 403, 404, 408 or 413, is never dead-lettered: it is retried and then fails the job, which replays. Any other value keeps the fail-and-replay default. |

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
| `document_id` | No | (empty) | Record field whose value names the document `_id`, so a record replaces its id's document rather than adding one. Every record then needs a value the [id rules](#document-ids) accept: a record without one is refused (dead-lettered under `dlq='drop'`, otherwise the job fails) rather than indexed under a generated `_id` that a replay would duplicate. |
| `path` | No | `/_bulk` | Bulk endpoint path. |
| `tls_ca_file` | No | (empty) | For an `https://` URL: PEM file of the CAs that may sign the server's certificate, in place of the system's trust store (a cluster's own CA). |

The two factories are the same sink; the gates run it against Elasticsearch 8.13.4 and 9.2.0 and OpenSearch 2.17.1 and 3.3.0 (see [Testing](#testing)).

#### Records

Each record must be one JSON object on one line, which the sink sends as the source document after its action line. A record that is not an object, or that spans lines, is refused before it is sent, like a record without a usable id: under `dlq='drop'` it is dead-lettered and the rest of the batch is written, otherwise the job fails. Sent, a record spanning lines breaks the `_bulk` framing and both servers fail the whole request with HTTP 400; a record that is not an object fails on its own, with 400 from OpenSearch but 500 from Elasticsearch.

#### Document ids

With `document_id`, the field's value becomes the `_id` as text:

| Value | `_id` |
| --- | --- |
| String | Itself. Refused when empty or longer than 512 bytes: the servers refuse an empty `_id` for its record and a longer one for the whole request. |
| Integer | Its exact decimal digits at any size, however the record writes it: `42`, `42.0` and `4.2e1` all give `42`; `18446744073709551615` gives `18446744073709551615`. |
| Number with a fraction | Six decimal places (`0.5` gives `0.500000`), when the record's number has at most six decimal places and a double holds it to six places. Refused otherwise, since that form would give it the `_id` of a different number. |
| Boolean | `true` or `false`. |
| Null, absent, object or array | Refused. |

The `_id` is text in one namespace, so values of different JSON types can name the same document: the number `42` and the string `"42"`, `0.5` and `"0.500000"`, `true` and `"true"`. Give the field one type when that matters.

Ids are written as they were before for every value that already had a single `_id` of its own, so replaying older records still replaces the documents they wrote. Three kinds of value changed:
- A number written with a fraction or exponent and past 2^63, or one between 2^53 and 2^63 that a double could not hold exactly, now gives its exact digits. Before, it was rounded through a double, so neighbouring values shared one `_id`, and past 2^63 the `_id` was the double printed to six decimals. A replay of such a record writes a new document beside the one it wrote before.
- A fraction with more than six decimal places, and an object or array, are now refused where they used to be written. The six-decimal form had rounded the fraction onto another number's `_id`; an object or array was written under its JSON text.
- An empty string or one over 512 bytes is now refused before it is sent. The server refused it before too, and a long one failed the whole request with it.

#### When a batch counts as written

`_bulk` answers HTTP 200 even when operations fail, listing each operation's outcome in `items`. A batch counts as written only when the response confirms every operation it sent. That means a JSON object with a boolean `errors` and an `items` array of exactly one entry per operation, each a single `index` result with an HTTP status, and `errors` agreeing with those statuses. Any other 2xx is unconfirmed, for example:
- a body that is not JSON or is cut short;
- a body that is not an object;
- a missing or short `items`, or an entry for another action;
- an item without a status;
- an `errors` that contradicts the items.

The whole batch is then resent, and once `max_retries` run out the job fails and replays from its last checkpoint. Nothing unconfirmed is ever dead-lettered, and a barrier whose flush is unconfirmed fails, so its checkpoint does not complete.

| Outcome | What the sink does |
| --- | --- |
| Item 2xx | Written. |
| Item 400, 409 or 422 about the record (a mapping or parse failure, a version conflict) | Dead-lettered under `dlq='drop'`, otherwise the job fails. |
| Item 400 about the index (`index_closed_exception`, `invalid_index_name_exception`), 403 (a write block), 404 (a missing index), 429, 5xx | Resent with backoff; once `max_retries` run out, the job fails. Never dead-lettered. |
| Whole request answered with any non-2xx, or no response | Resent whole; once `max_retries` run out, the job fails. Never dead-lettered. |
| 2xx that does not confirm every operation | As above. |

A 400 for the whole request used to dead-letter the whole batch under `dlq='drop'`, and the two index-level 400s dead-lettered every record written while the index was closed or misnamed. An HTTP 200 whose body was not an object, or that said `"errors":false` without listing every operation, used to count as written.

When the task is cancelled the flush stops at once. A request in flight is interrupted rather than left to its 30-second read timeout, and the pause before a retry ends early. The flush then fails with its batch still pending. This holds for every sink built on the shared bulk base.

#### Security

Credentials go in `headers`: `Authorization: Basic <base64 of user:password>`, or `Authorization: ApiKey <encoded>` for an Elasticsearch API key. An `https://` URL is verified against the system's trust store, or against `tls_ca_file` for a cluster's own CA; `verify_tls='false'` skips verification and checks nothing about the server. The writing user needs to create and write the target index. Elasticsearch calls these privileges `create_index`, `auto_configure` and `write`; OpenSearch calls them `crud` and `create_index`, with `cluster_composite_ops` for the bulk request.

A wrong password (HTTP 401 for the whole request), a user who may not write the index (403), and a certificate that does not verify are resent and then fail the job; none is dead-lettered. The sink's error names the status and the server's reason, never the credentials.

#### Ordering

Within one sink subtask, records naming the same `document_id` are applied in the order the subtask received them, and once a flush succeeds each of its ids holds its latest record from that flush. Within one request the servers apply operations on one `_id` in order. When an operation fails and is resent, the sink resends it together with every later record of the same flush that names the same id, in order, so the resend cannot land after them and leave the older value. Before, the failed operation was resent alone, so a document could end at an earlier record's value while the flush reported success. Flushes go one at a time, each finishing before the next is sent. Without `document_id` only the failed operations are resent, since each record is a document of its own.

The scope ends there:
- **Across subtasks.** Records of one id are ordered only when they reach the same sink subtask in order: in SQL, when the source already partitions them by that id. The sink does not repartition its input.
- **Other writers.** Writes from outside the job to the same index are not ordered against the sink's; the later write wins.
- **During a replay.** After a failure the job replays from its last checkpoint, rewriting each id from there. A document can therefore show an older record's value until the replay reaches its latest, then converges.
- **One URL.** The sink writes to the one URL it is given, with no list of nodes. In front of a cluster, point it at a load balancer or at a node that stays up. While that node is down, flushes fail and the job replays until it returns. A node it does not write to can fail without loss, once the index has replicas on the others.
- **A request the sink gave up on** (a timeout or a dropped connection) may still be applied by the server after the sink has resent it. Every request that writes an id carries that id's latest record of the flush, so the outcome only differs when the abandoned request's later write for an id fails while its earlier one succeeds.

None of this is exactly-once; the delivery analyser reports these sinks as at-least-once and refuses `exactly_once`.

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

GETs a JSON REST endpoint on an interval and emits the array elements, with a checkpointed cursor. Only subtask 0 polls, whatever the job's parallelism; the other subtasks read nothing. This source does not use `batch_records` / `batch_bytes` / `dlq` / `linger_ms`.

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
| `max_retries` | No | `4` | Retries of a pull or an acknowledge that fails transiently (HTTP 429, 5xx, transport); clamped to 0..20. |
| `retry_base_backoff_ms` | No | `200` | Base backoff between those retries. |
| `ack_deadline_s` | No | `60` | The lease the source takes on each message it holds: set on receipt with `modifyAckDeadline` and renewed at half this period while the message waits for its checkpoint. 10 to 600. |
| `max_lease_s` | No | `3600` | How long one message's lease is renewed. Past it the server delivers the message again under a new ackId. At least `ack_deadline_s`. |
| `max_outstanding` | No | `10000` | Most messages a subtask holds unacknowledged; at the bound pulls pause until a checkpoint completes. |
| `on_decode_error` | No | `fail` | A message whose `data` is not valid base64. `fail` stops the subtask without acknowledging it; `drop` acknowledges it with its checkpoint, emits nothing and counts it in `dropped_events_total`. |

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

A NaN or infinite double in a row reaches this sink through the `row_to_json_string` bridge as `null`, or as `"NaN"`, `"Infinity"` or `"-Infinity"` when the sink table sets `json_non_finite='string'`; the sink handles it as it handles any null or string value. See [NaN and infinity in JSON output](README.md#nan-and-infinity-in-json-output).

## Example

Programmatic use of the Elasticsearch sink, based on the live test (`impls/http_connector/tests/test_elasticsearch_sink_live.cpp`). Each record is already a JSON object string:

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
- `elasticsearch_sink` / `opensearch_sink` are at-least-once. Without `document_id` a resend or a replay indexes the record again; a reply lost after the server applied the request is enough to duplicate a batch. With `document_id` each record replaces its id's document, so a replay converges on each id's latest record, ordered within a sink subtask as [Ordering](#ordering) describes. A batch counts as written only when the bulk response confirms every operation, and failures are classified as [the table above](#when-a-batch-counts-as-written) sets out.
- `influxdb_sink` is effectively-once when `timestamp_field` is set, since an identical point is idempotent.
- `splunk_hec_sink` and `prometheus_sink` are at-least-once with no dedup key; HEC ingestion is append, and the Pushgateway stores the latest value per series (last-write-wins within a push). Each push is a POST, which replaces every series of the metric in its group with the series the push carries, and a push carries the series of the records since the previous one. So a series no record has named since the previous push disappears from the gateway rather than keeping its last value; give every series a record in each checkpoint interval if they must all stay visible.
- `pubsub_sink` is at-least-once with no producer dedup; a replay re-publishes.
- `http_poll_source` is at-least-once: the cursor is checkpointed. Avoiding re-emission of the boundary record requires an exclusive cursor server-side. The `http` capability record declares the source replayable from that cursor, so the delivery analyser lets a job over it declare `at_least_once`, and `exactly_once` with a two-phase-commit sink, which holds when the endpoint honours an exclusive cursor and retains what it served; an endpoint that ignores the cursor delivers everything again after a restart.
- `pubsub_source` is at-least-once. It pulls, leases what it holds, and acknowledges once the checkpoint that covers a message completes; the server delivers again whatever was not acknowledged, which is also the crash-recovery mechanism. The contract, in detail:
    - `snapshot_offset()` seals the messages pulled since the last barrier as that checkpoint's; `notify_checkpoint_complete()` makes every interval at or below the completed id ready, and the next `produce()` turn, or `close()`, posts `:acknowledge` for them. Every request stays on the source thread; the notification only moves ackIds between lists (`include/clink/connectors/checkpoint_ack_ledger.hpp`), and a repeated, late or out-of-order one moves nothing twice and nothing sealed above the id it names.
    - An aborted checkpoint's messages, and those of a checkpoint that never completes, are not acknowledged: the source stops renewing their leases and the server delivers them again. `close()` acknowledges what completed checkpoints cover and hands every other held message back (`ackDeadlineSeconds` 0), so a restart receives them at once rather than when their leases run out.
    - While a message waits for its checkpoint the source holds its lease: `ack_deadline_s` on receipt, renewed at half that period, for at most `max_lease_s`. A message held past `max_lease_s`, or whose renewal fails throughout, is delivered again under a new ackId; the source emits it again and acknowledges each delivery with its own checkpoint. A failed renewal is retried and never fails the subtask, since its cost is a duplicate, not a loss.
    - An acknowledge that fails transiently is retried up to `max_retries`; one that still fails, or fails permanently, fails the subtask. Its messages were not acknowledged (or were, when only the reply was lost), and either way the server delivers again only what no completed checkpoint acknowledged.
    - A checkpoint that completed but whose acknowledge had not landed when the process died is delivered again (duplicates). Completion is not commit confirmation: a job that restores from a checkpoint older than one the source acknowledged (a job with a commit-confirmed sink such as the Kafka transactional sink, or an operator restoring a savepoint) does not see that checkpoint's messages again; the delivery analyser warns about the commit-confirmed combination.
    - A subtask holds at most `max_outstanding` unacknowledged messages. A job that completes no checkpoint (no checkpointing, or a `LocalExecutor` run) acknowledges nothing, so it pulls `max_outstanding` messages and then waits, logging why.
    - Data that is not valid base64 stops the subtask by default (`on_decode_error='fail'`): the pull is handed back unacknowledged and nothing of it is emitted, so the message is delivered again on every restart until it is removed or the subscription's dead-letter policy moves it aside. `on_decode_error='drop'` acknowledges such a message with its checkpoint, emits nothing and counts it, which loses it by choice. A received message with no ackId cannot be acknowledged and is skipped; the server delivers it again.

    The capability record declares the position model as `acknowledgement`, so the delivery analyser reports a job over this source as `AT_LEAST_ONCE_SOURCE`: a sink in it may declare `delivery_guarantee='at_least_once'`, and `exactly_once` is refused. Before this contract the source acknowledged at the barrier, so a batch acknowledged by a checkpoint that then failed was not delivered again, an undecodable message was dropped with no option to stop instead, and the analyser reported such a job as `AT_MOST_ONCE_SOURCE`.

## Limitations

- Each sink must be the only sink on its chain: at every checkpoint barrier it writes out what it received before the barrier, which holds only while it owns its chain's checkpoint, so `Dag::add_sink` refuses another sink beside it, whichever is added first. SQL and job graphs plan every sink as a subtask of its own; with the Dag API, put the other sink in a separate Dag (see [checkpointing](../internals/checkpointing.md#guarantees-and-caveats)).
- All connectors carry records as `std::string` only; there is no int64 or typed-Arrow channel. On the SQL path rows are serialised to JSON strings.
- No two-phase commit on any sink; exactly-once delivery is not offered. The SQL planner rejects `exactly_once` and `upsert` modes on these sinks.
- `elasticsearch_sink` / `opensearch_sink`: records of one `document_id` are ordered only within one sink subtask, and not against other writers; a replay can show a document at an older record's value until it reaches the latest; `document_id` values outside [the id rules](#document-ids) are refused. Only the `index` action is written: no update, delete or external versioning. The sink takes one URL and does not discover other nodes.
- HTTPS works only if cpp-httplib was built with OpenSSL; otherwise an `https://` URL is rejected at construction.
- `prometheus_sink` targets a Pushgateway, which is a latest-value store for low-cardinality service metrics, not a high-throughput event TSDB. Records whose `value_field` is missing or non-numeric are dropped (no sample). Remote-write is not implemented.
- `http_poll_source` requires the API to return records ascending by `cursor_field`; it has no client-side ordering or de-duplication beyond the cursor.
- `pubsub_source` holds at most `max_outstanding` unacknowledged messages per subtask, and a job that completes no checkpoint stops pulling there. A message held past `max_lease_s` is delivered again (a duplicate). Subscriptions with exactly-once delivery enabled are not handled specially.
- Pub/Sub uses the synchronous REST `Pull` with `returnImmediately`; gRPC StreamingPull is not used. The build does not mint OAuth2 tokens itself; a static `auth_token` expires after its lifetime, but `auth_token_file` lets an external refresher rotate the token without a restart. Multiple subtasks pull the same subscription and Pub/Sub load-balances across them (no client-side sharding).
- The bulk sinks buffer rendered records in memory until a flush trigger; `batch_records` / `batch_bytes` / `linger_ms` bound that buffer.
- `linger_ms` is evaluated only when a record arrives (no timer thread), so a fully idle buffer waits for the next checkpoint barrier.

## Testing

The module's tests live in `impls/http_connector/tests/` and carry the ctest label `http_connector`. Most are in-process or mock tests (factory registration, framing, the Splunk and Prometheus body builders) and run in a normal build:

```bash
ctest --test-dir build -L http_connector
```

The live integration tests are skipped unless their endpoint environment variable is set. The sidecar services are defined in `docker/integration-services.yml`:

| Test | Env var(s) | Service / image |
| --- | --- | --- |
| Elasticsearch (`Elasticsearch/SearchSinkLive`, `test_elasticsearch_sink_live.cpp`) | `CLINK_ELASTICSEARCH_TEST_ENDPOINT` | `docker.elastic.co/elasticsearch/elasticsearch:8.13.4` |
| OpenSearch (`OpenSearch/SearchSinkLive`, same file) | `CLINK_OPENSEARCH_TEST_ENDPOINT` | `opensearchproject/opensearch:2.17.1`, security plugin off |
| InfluxDB (`test_influxdb_live.cpp`) | `CLINK_INFLUXDB_TEST_ENDPOINT`, plus optional `CLINK_INFLUXDB_TEST_ORG` / `_BUCKET` / `_TOKEN` (defaults `clink` / `clink` / `clink-token`) | `influxdb:2` |
| Pub/Sub (`test_pubsub_live.cpp`) | `CLINK_PUBSUB_EMULATOR_HOST` (e.g. `localhost:8085`) | `gcr.io/google.com/cloudsdktool/cloud-sdk:emulators` |

There is no live test for `splunk_hec_sink`, `prometheus_sink`, or `http_poll_source`; these are covered by in-process and mock tests (`http_poll_source` also feeds the search recovery cells, from a feed the test serves).

`pubsub_source`'s acknowledgement contract is tested in process by `PubSubSourceAck` (`test_pubsub.cpp`), against a stub of the REST endpoints that leases each delivery under a new ackId, honours `modifyAckDeadline`, acknowledges only through a current ackId and can fail acknowledges: an aborted checkpoint, crashes after the barrier and after completion, failed and retried acknowledges, notifications out of order, leases held across a delayed checkpoint and given up past `max_lease`, the `max_outstanding` bound, close after a cancel, and both `on_decode_error` policies. `PubSubSourceAckLive` (`test_pubsub_live.cpp`) repeats the abort, the crash after completion and the delayed checkpoint against the emulator. `test_factory_registration.cpp` pins the capability record and the analyser's verdict. `scripts/ack-sources-live.sh [build-dir]` is the required gate: it runs `PubSubLive` and `PubSubSourceAckLive` against a one-off emulator container, with the other acknowledging sources' suites, and fails on a missing binary, an emulator that never answers, a failed or skipped case, or a short suite (see the [Redis page](redis.md#testing)). The `ack-sources` CI workflow runs it.

### Elasticsearch and OpenSearch

`scripts/search-live.sh <server> [build-dir]` is the sinks' gate, for one server at a time. The servers are `elasticsearch` (8.13.4), `elasticsearch9` (9.2.0), `opensearch` (2.17.1) and `opensearch3` (3.3.0), run one after the other. Each gate runs four phases, each on its own server, started on a free loopback port and removed when the phase ends:

| Phase | Server | Suites |
| --- | --- | --- |
| main | The pinned image, security off | The offline suites, `<Product>/SearchSinkLive`, `<Product>/SearchRecovery` |
| throttled | The same image with a write pool of one thread and a write queue of one, so the server itself rejects writes with 429 | `<Product>/SearchSinkThrottledLive` |
| secured | The same image with its own security on: HTTPS with a certificate from its own CA, every request authenticated, an administrator's password generated for the run and passed only through the environment | `<Product>/SearchSinkSecuredLive` |
| cluster | Three nodes of the same image (`docker/search-cluster.yml`), the sink writing to node 1 | `<Product>/SearchSinkClusterLive` |

A gate fails on a skipped case, a suite that ran fewer cases than it has, a missing binary, a server that never answers or is not the product named, and the secured phase's password appearing anywhere in what it keeps. It keeps each run's report and output, the recovery cells' coordinator and worker logs, the secured server's CA certificate, and a `provenance.txt` naming the commit, the build's configuration and each phase's server version, image and digest. The `search` CI workflow runs all four gates.

| Suite | Binary | What it checks |
| --- | --- | --- |
| `ElasticsearchSink`, `EsBulkResult`, `EsDocumentId` | `clink_http_connector_tests`, offline | Against a model of the `_bulk` endpoint that applies operations as the servers do: the body; the id rules; every malformed or incomplete 2xx reply resending the batch and never dead-lettering; item and whole-request classification; a resend carrying the later writes of its document; repeated ids across retries and batches; throttling; cancellation, of a pause and of a request in flight |
| `<Product>/SearchSinkLive` | `clink_http_connector_tests`, live | Against the real server, reading the index back: 300 documents equal to their records; 500 records over 37 ids in batches of 23 ending at each id's latest; an earlier write withheld and reported 429 resent with the later one (with a control that resends it alone and leaves the older value); a reply lost after the server applied the batch; garbled replies keeping the batch pending; a closed, write-blocked, missing and misnamed index failing the flush without dead-lettering; a mapping failure dead-lettered; a long id, a multi-line record and a non-object refused alone (with the server's whole-request 400 shown directly); the id rules; throttling; cancellation, including a request the server never answers |
| `<Product>/SearchSinkThrottledLive` | `clink_http_connector_tests`, live | The server's own 429s, counted from its replies, retried until 2,000 records over 400 ids are stored at their latest; with `max_retries=0`, a throttled flush failing, nothing dead-lettered, and the next flush sending the same batch again |
| `<Product>/SearchSinkSecuredLive` | `clink_http_connector_tests`, live | A user who may only write, over HTTPS verified against the server's CA; an API key (Elasticsearch) or a second user (OpenSearch); a wrong password (401) and a user who may only read (403), each resent then failing, nothing dead-lettered and no credential in the error; a certificate that does not verify failing, and `verify_tls='false'` writing without checking |
| `<Product>/SearchSinkClusterLive` | `clink_http_connector_tests`, live | Three shards with a replica each: a node the sink does not write to killed while it writes and started again, nothing lost and every id at its latest; the node the sink writes to killed, the flush failing with nothing dead-lettered, and the pending batch stored once it rejoins |
| `<Product>/SearchRecovery` | `clink_integration_tests`, cluster | A coordinator and two workers polling a feed (`http_poll_source`, 3,000 records over 200 ids, each written 15 times at a rising value) into the sink: a worker killed before a write and after the server confirmed a flush, the second also as a SQL job submitted by `clink_submit_sql` at parallelism 2; replies that confirm nothing until 12 requests were refused, during which no checkpoint completes, the job restarts and nothing is lost; and a reply lost after the server applied a flush, with the worker killed while it was held. Each ends with every id at its latest record |

Where a case needs a fault the server will not produce on demand, it writes through a proxy and says so in its `[search-live]` or `[search-recovery]` line. `BulkFaultProxy` (`tests/integration/bulk_fault_proxy.hpp`) withholds an operation and reports it failed, garbles a reply after the server applied the request, or refuses requests with a status. `TcpFaultProxy` withholds a reply after the server applied the request, or swallows a request, and then resets the connection. What the index holds afterwards is always read from the server. These are the servers' own behaviour:
- the throttled server's 429s, which the proxy only records;
- the security refusals;
- the cluster's node kills;
- the index-level refusals, the mapping failure and the whole-request 400 for a long `_id`;
- the products' different answers to a non-object record.

The search CI workflow passed the gates for Elasticsearch 8.13.4 and OpenSearch 2.17.1 on commit `bf6cae33` (run 42184675107), 43 of 43 each, before the throttled, secured and cluster phases and the newer servers were added.
