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

Converts each JSON-object row to exactly one InfluxDB v2 line-protocol point and POSTs newline-delimited batches to `/api/v2/write` with `Authorization: Token <token>`. The renderer, the point rules and the reading of responses are in `impls/http_connector/include/clink/http_connector/influxdb_sink.hpp`; buffering, retries and cancellation are the shared `BatchedHttpBulkSink`'s. It is tested against InfluxDB 2.7.12.

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `url` | Yes | | `scheme://host[:port]`. |
| `org` | Yes | | InfluxDB v2 organisation. |
| `bucket` | Yes | | InfluxDB v2 bucket. |
| `token` | Yes | | API token; sets the `Authorization` header. `env://VAR` reads it from the worker's environment. |
| `measurement` | Yes | | The measurement name of every point. |
| `tags` | No | | Comma-separated row fields written as tags, in that order; every record must carry each one. |
| `timestamp_field` | No | | Row field holding the point's timestamp; without it the server stamps each point. |
| `precision` | No | `ns` | The timestamps' unit: `ns`, `us`, `ms` or `s`. |
| `number_fields` | No | `float` | `float`: every JSON number a float field. `typed`: an integer an integer field, any other number a float field. |
| `connect_timeout_ms` | No | `5000` | 1 to 600000. |
| `request_timeout_ms` | No | `30000` | 1 to 600000: how long one write may wait for its reply. |
| `batch_records` | No | `500` | 1 to 100000 points per request. |
| `batch_bytes` | No | `4194304` | 1024 to 268435456; keep it under the server's `http-max-body-size` (25,000,000 by default). |
| `max_retries` | No | `4` | 0 to 20 retries of a batch. |
| `linger_ms` | No | `0` | Write out a partial batch this old. |
| `dlq` | No | `fail` | `fail` or `drop`: what a refused record does (below). |
| `verify_tls`, `headers` | No | `true`, none | As the other HTTP sinks. |

Every option is read strictly: a value outside its set or range, or a number with anything after its digits, fails the build instead of falling back to a default.

#### Point identity and timestamps

InfluxDB identifies a point by its measurement, its tag set and its timestamp. The sink refuses to build, naming the option, when `tags` names a field twice, holds an empty entry, names the timestamp field, or names `time`, `_field` or `_measurement`, which InfluxDB refuses as tag keys. Each record must then carry every configured tag, not null and not empty (InfluxDB refuses an empty tag value). A tag may be a string, a boolean, or a number: an integer is written with all its digits, so two tags past 2^53 never merge into one series.

With `timestamp_field`, every record must carry a timestamp: a JSON integer, written without a fraction or exponent, counting `precision` units since the epoch, within InfluxDB's range of plus or minus 9223372036854775806 nanoseconds (plus or minus 9223372036 at `precision='s'`). The value is written with its own digits, never through a double, so neighbouring nanoseconds above 2^53 stay two points. A missing, null, fractional, exponent, string or out-of-range timestamp is refused; the sink never lets the server stamp a record that should carry its own time. Strings are not read as timestamps; a SQL `TIMESTAMP` column needs converting to an epoch integer first. A point at InfluxDB's last nanosecond is written, though a Flux `range()` cannot read it back, its stop being exclusive.

Without `timestamp_field`, the server stamps each point when it writes it.

#### Records and fields

| JSON value | Written as |
| --- | --- |
| string | a string field, quoted, with `"` and `\` escaped; line breaks and tabs kept inside the quotes |
| `true`, `false` | a boolean field |
| integer, `number_fields='float'` | a float field, as earlier releases wrote it; refused past 2^53, which a float cannot hold exactly |
| integer, `number_fields='typed'` | an integer field (`i` suffix), with all its digits |
| other number | a float field, in its shortest exact form |
| `null` | left out: InfluxDB has no null |
| object or array | refused: line protocol has no nested field |

Every field other than the tags and the timestamp field is written, in name order. A record must be one JSON object with at least one field to write; one that is not valid JSON, not an object, or has nothing but tags, the timestamp and nulls is refused. An integer of 2^64 or more is refused by the JSON reader, and one between 2^63 and 2^64 is refused as no field type holds it.

`number_fields` decides a field's type, and InfluxDB fixes a field's type in each shard once it is written: switching an existing measurement from `float` to `typed`, or writing a number into a field another writer made a string, produces the type conflicts described below. A field one record leaves out and another carries is simply missing from the first point.

#### Escaping, and what is refused

Measurement names have commas and spaces escaped, and tag keys, tag values and field keys commas, equals signs and spaces; a backslash elsewhere is literal, as line protocol reads it. Unicode is written as UTF-8. What line protocol cannot carry is refused rather than written in a form that would split, merge or vanish:

- a line feed or carriage return in a measurement, tag key, tag value or field key: a line break ends the point, so the rest would become a second point;
- NUL in any name or string: InfluxDB drops it, changing the value;
- a name or tag value ending in a backslash, which would escape the separator after it;
- a measurement starting with `#`, which makes the line a comment that InfluxDB answers with 204 and stores nothing of;
- a field named `time`.

A refused record never reaches the server. Under `dlq='drop'` it goes to the dead-letter queue with the rule it broke and the rest of the batch is written; under `dlq='fail'` the sink fails, and because it keeps no copy of that record it then refuses every later record and barrier until it opens again. Refusals name the rule and the field, never a value.

#### When a batch counts as written

| Response | Meaning | Handling |
| --- | --- | --- |
| 204 | every point written | the batch is done |
| another 2xx | not a confirmation (InfluxDB answers an accepted write with 204) | sent again, then the sink fails |
| 400 | InfluxDB refused the whole request and wrote none of it: one line it could not parse fails all | the batch's records dead-lettered under `drop`; the sink fails under `fail` |
| 422 | a partial write: InfluxDB wrote some points and dropped others, saying how many (`dropped=N`) but not which, as for a field-type conflict | under `fail` the sink fails, keeping the batch; under `drop` the request's records go to the dead-letter queue as one report that says N of them were refused and the rest written, and the batch is done |
| 401, 403 | the token refused | sent again, then the sink fails; never dead-lettered |
| 404 | no such organisation or bucket | as 401 |
| 413 | the request larger than the server takes | as 401; lower `batch_bytes` |
| 429, 5xx, no reply | throttling, a server error, or a reply lost after the request may have been applied | sent again after a backoff, then the sink fails |

Local checks mean a 400 should not happen; a 422 is the one outcome the response cannot attribute. A server message is repeated with the line it quotes removed, since that line carries the record's values; the token never appears in a message.

#### Checkpoints and failure

A barrier returns only once every point received before it is confirmed with a 204 (or, under `drop`, dead-lettered), so a checkpoint never covers a point whose write is outstanding, refused or unconfirmed. When the sink fails for a write outcome it keeps the batch: a later barrier sends it again and returns only if it is confirmed. A cancelled task stops at once, the request in flight interrupted and the batch kept. The job's restart replays every record after the last completed checkpoint.

#### Delivery, replay and ordering

- **Writes are at-least-once.** A replay, a retry after a lost reply, and a batch resent after a failure all write points again.
- **With `timestamp_field`, a rewritten point lands on its own series and time.** InfluxDB merges a point into an existing point with the same measurement, tag set and timestamp: the field set becomes the union of the two, and a field both carry takes the newer value. So a replay of the same record leaves the point as it was, a record carrying fewer fields leaves the others at their earlier values rather than removing them, and two different records with the same identity become one point. Measured on 2.7.12: a reply withheld after the server applied a batch, then the batch resent, left one point.
- **Without `timestamp_field`, every write is a new point.** The server stamps each write, so a resend or a replay adds points beside the first: the same lost reply left two.
- **Ordering:** the points of one identity end at their last write only when one sink subtask writes them, in order, and nothing else writes that series; two subtasks or another writer can leave an older value. Between a restart and the point the replay reaches, a point can show older field values than before the failure.

None of this is exactly-once delivery, and InfluxDB makes no conditional write. The SQL frontend refuses `exactly_once`, `mode='upsert'` and `changelog='true'` (line protocol has no delete, and the `__row_kind` marker would be written as a field).

#### Earlier releases

Records the previous sink wrote correctly are written the same way, with the same point identities and float fields. What changes:

- **A configured timestamp is required.** A record whose `timestamp_field` was missing or not a number used to be written with the server's time, a different point on every replay; one with a fraction had it cut off. Both are now refused.
- **Tags are required.** A record missing a configured tag, or with it null, used to be written to another series; it is now refused, as is an empty tag value, which the server refused.
- **Unsafe text is refused.** A line break in a name or tag value used to split the record across two lines, and the server refused the whole request (under `dlq='drop'`, every record of the batch was dead-lettered); a NUL was dropped by the server; a `#` measurement was silently discarded. All are refused now.
- **Bad records are refused before they are sent.** Malformed JSON, a non-object and a record without fields used to be sent as broken lines, so the server refused the whole batch, and under `dlq='drop'` every good record of it was dead-lettered. They are now refused alone.
- **Nested values are refused** instead of being left out of the point without a word.
- **Integers past 2^53 are refused under `number_fields='float'`** instead of being rounded; `number_fields='typed'` is new and writes them exactly.
- **Partial writes are reported as such.** A 422 used to dead-letter every record of the request under `drop`, though the server had written most of them.
- **Only 204 confirms a write**, the options are read strictly, and `connect_timeout_ms` and `request_timeout_ms` are new.

### `prometheus_sink`

Pushes each JSON-object row as one gauge sample to a Prometheus Pushgateway. One sink pushes one metric into one group: `job` and the `grouping` labels name the group, `metric_name` the gauge, `value_field` the field holding each sample's value, and every other field is a label. The sample rules, the push path and the reading of responses are in `impls/http_connector/src/prometheus_push_sink.cpp`. It is tested against Pushgateway 1.10.0.

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `url` | Yes | | `scheme://host[:port]` of the gateway. A URL carrying credentials is refused; put them in `headers`. |
| `job` | Yes | | The group's `job` label: any non-empty UTF-8 text. |
| `grouping` | No | | The group's other labels, `name=value` pairs separated by commas, with the spaces around each trimmed. A name must be a label name, appear once and not be `job`; a value may be empty but cannot hold a comma. |
| `metric_name` | No | `clink_value` | The gauge's name, `[a-zA-Z_:][a-zA-Z0-9_:]*`, other than `push_time_seconds` and `push_failure_time_seconds`, which the gateway writes itself, and, with `push_series='all'`, `labels` and `last_push_successful`, which name fields of the gateway's query API. |
| `value_field` | No | `value` | The field holding each sample's value. |
| `help` | No | | `# HELP` text. |
| `push_series` | No | `batch` | Which series each push carries: `batch` or `all` (below). |
| `batch_records` | No | `500` | `batch` only: push once this many series are buffered; 1 to 100000. |
| `max_series` | No | `10000` | `all` only: the most series the sink holds; 1 to 1000000. |
| `batch_bytes` | No | `4194304` | The most bytes one push carries; 1024 to 268435456. |
| `max_sample_bytes` | No | `65536` | The longest sample line, or half a smaller `batch_bytes`; 64 to `batch_bytes`. A record making a longer line is refused. |
| `linger_ms` | No | `0` | Push updates once the oldest is this old, checked when a record arrives; up to 3600000. |
| `max_retries` | No | `4` | 0 to 20 retries of a push. |
| `connect_timeout_ms` | No | `5000` | 1 to 600000. |
| `request_timeout_ms` | No | `30000` | 1 to 600000: how long one push or read may wait for its reply. |
| `dlq` | No | `fail` | `fail` or `drop`: what a refused record does (below). |
| `verify_tls`, `headers` | No | `true`, none | As the other HTTP sinks; `headers` carries basic auth for a gateway or proxy that asks for it. |

Every option is read strictly: a value outside its set or range, or a number with anything after its digits, fails the build. The sink runs on one subtask: the factory refuses a parallelism above 1, and the SQL planner keeps the sink on one subtask whatever the job's parallelism.

#### Series identity

A series is the metric name and its label set, and a record's labels are every field but `value_field`:

| JSON value | Label value |
| --- | --- |
| string | the string, which must be valid UTF-8; an empty string means the label is absent |
| number | its JSON text exactly as written: an integer keeps every digit, past 2^53 and past 64 bits, and `1.50` stays `1.50` |
| `true`, `false` | `true`, `false` |
| `null` | the label is absent |
| object or array | refused |

A label value is text, so the string `"42"` and the number `42` name one series, while `1` and `1.0` name two. An empty string and `null` both mean the label is absent, which is how Prometheus reads an empty label value. A field name must be a Prometheus label name, `[a-zA-Z_][a-zA-Z0-9_]*` and not starting with `__`; any other name is refused, never rewritten, so `device-id` cannot merge with `device_id`. A field named after a grouping label, `job` included, must be absent or carry the group's value: Pushgateway 1.10.0 replaces such a label with the group's value without a word, which would merge distinct series. In a label value, backslash, double quote and line feed are escaped, and every other character, carriage returns, tabs and NUL included, is written as it is; a scrape of the gateway returns each unchanged.

Within the sink a later record replaces an earlier record's value for the same series, so a push carries each series once, as the gateway requires.

#### Values

`value_field` must hold one of:

- a JSON number; an integer within plus or minus 2^53, which a gauge (an IEEE double) holds exactly; any other number becomes the nearest double;
- `true` or `false`, pushed as 1 or 0;
- a string holding a decimal number in JSON's grammar, read without regard to locale, on the same terms;
- the string `NaN`, `Infinity`, `-Infinity`, `+Inf` or `-Inf`.

Anything else is refused: `null`, a missing field, an out-of-range number, or a string such as `" 1"`, `"1,5"` or `"0x10"`. A value is pushed in its shortest exact form, NaN and the infinities as `NaN`, `+Inf` and `-Inf`. A SQL `DOUBLE` that is NaN or infinite reaches the sink as `null` unless the table sets `json_non_finite='string'`, which writes the spellings above.

#### The group and its path

Pushes are POSTs to `/metrics/job/<job>` followed by `/<name>/<value>` for each grouping label. A value of letters, digits, `-`, `_` and `.` is written as it is; any other value, an empty one included, takes the gateway's base64 form (`<name>@base64/<value>`, URL-safe alphabet). The gateway decodes `%2F` before it splits the path, so a slash survives only in base64.

#### Which series each push carries

The gateway's POST replaces every series of the pushed metric in the group with the series the push carries, and leaves the group's other metrics alone. What a scrape of the gateway shows therefore depends on what each push carries:

- **`push_series='batch'`** (the default, and how earlier releases behaved): a push carries the series of the records since the previous push. Pushes happen when `batch_records` series are buffered, before the buffer passes `batch_bytes`, after `linger_ms`, at every checkpoint barrier and at end of input, and each replaces the series of the one before it. A series no record named since the previous push disappears from the gateway.
- **`push_series='all'`**: the sink holds the latest value of every series it was given, up to `max_series` and `batch_bytes`, and every push carries them all, so no trigger hides a series. Pushes happen after `linger_ms`, at end of input and at every checkpoint barrier, where the sink pushes what it holds even when nothing changed, so a gateway that restarted without persistence has the series again. A record that would take the set past `max_series` or `batch_bytes` fails the sink, since a push of part of the set would hide the rest. A series stays until the group is deleted from the gateway. A run restored from a checkpoint first reads the group's series of the metric from the gateway's query API (`/api/v1/metrics`, at most 64 MiB read) and holds them too, the records it receives taking precedence, so the series its predecessor pushed stay visible though the replay never names them again. A fresh start does not read them, and its first push replaces them.

Pushgateway 1.10.0 checks each push against everything it holds. Measured locally with one group of 400,000 series held, a push took 0.2 to 0.8 seconds, against a few milliseconds without it. The gateway is meant for a modest number of series.

#### When a push counts

| Response | Meaning | Handling |
| --- | --- | --- |
| 200 | the gateway applied the push | the push is done |
| 202 | the gateway queued the push without its consistency check (`--push.disable-consistency-check`) | the push is done |
| another 2xx | not a confirmation | sent again, then the sink fails |
| 400 | the gateway refused the push whole and applied none of it, as for a metric another group holds with another type | the sink fails at once, keeping what the push carried |
| 401, 403, 404, 413 | credentials refused, no push API at this URL, a push larger than a proxy takes | sent again, then the sink fails |
| 429, 5xx, no reply | throttling, a server error, or a reply lost after the gateway may have applied the push | sent again after a backoff, then the sink fails |

No response dead-letters a record. A resend carries the same series with the same values, so a resend after a lost reply changes nothing. A gateway message is repeated with every quoted value removed, and header values never appear in a message.

#### Refused records

A record that breaks a rule above never reaches the gateway, and because it is checked whole before anything is buffered it never replaces a valid sample. Under `dlq='drop'` it goes to the dead-letter queue with the rule it broke and is counted in `dropped_records_total`. Under `dlq='fail'` the sink fails, and because it keeps no copy of the record it refuses every later record, flush and barrier until it opens again. Refusals name the rule and the field, never a value.

#### Checkpoints and failure

A barrier returns only once a push carrying everything received before it was confirmed, so a checkpoint never covers a sample that no confirmed push carried. A failed push keeps what it carried, and the next push sends it again. A cancelled task stops at once, interrupting a push in flight or a pause before a retry. `close()` pushes nothing; end of input pushes what is pending first. Records count as written (`records_total`) when a confirmed push first carries them.

#### Delivery, ownership and replay

- **The gateway keeps values, not samples.** Prometheus sees what the gateway holds when it scrapes, so a value replaced before a scrape is never observed, and nothing here says every record reached Prometheus. Samples are durable only once Prometheus has stored them, and the gateway keeps nothing across its own restart unless run with persistence.
- **Pushes are at-least-once.** A restored job pushes again the records after its checkpoint, and each push replaces the series it carries, so the replay ends at each series' latest value. Until it gets there, a series can show an older value than before the failure.
- **One writer per group and metric.** Each push replaces every series of the metric in the group, so another writer's push replaces the sink's series and the sink's replaces theirs, as the live suite shows. The sink runs on one subtask, and no other sink or job may push the same metric to the same group. A series takes its values in the sink's input order.

The SQL frontend refuses `exactly_once`, `mode='upsert'`, `changelog='true'` (a pushed gauge has no delete, and `__row_kind` is not a label name) and options the sink does not read.

#### Earlier releases

Records the previous sink pushed without dropping or rewriting anything are pushed the same way, as the same series, and `push_series='batch'` keeps its replacement behaviour. What changes:

- **Bad records fail the sink by default.** Malformed JSON, a non-object, and a missing, null or non-numeric value used to be dropped without a word, and nested fields left out. They are now refused under `dlq`, `fail` by default; `dlq='drop'` dead-letters them.
- **Names are refused, not rewritten.** A field name outside the label grammar used to be rewritten, so two fields could merge into one label, and the metric name was rewritten too. Both are now refused.
- **Integer labels keep every digit.** Numbers used to be written through a double, so neighbouring integers above 2^53 became one series.
- **Grouping conflicts are refused.** A field named after a grouping label used to be dropped from the push whatever its value; one carrying another value is now refused, and one carrying the group's value is accepted. A `grouping` entry without `=` used to be skipped; it now fails the build, as does a repeated name or `job`.
- **An empty label value is the label absent**, as Prometheus reads it: `{a=""}` and `{}` used to be two series in one push.
- **A failed push keeps what it carried** instead of clearing it, and a 400 fails at once instead of being retried.
- **Values are read strictly.** An integer past 2^53 is refused rather than rounded, and a decimal string is read in JSON's grammar, without regard to locale.
- **New options:** `push_series`, `max_series`, `max_sample_bytes`, `connect_timeout_ms`, `request_timeout_ms` and `dlq`; `batch_bytes` is now read. Options are read strictly, and a parallelism above 1 is refused.

### `http_poll_source`

GETs a JSON endpoint on an interval and emits the elements of the array each response carries, one record per element, as single-line JSON with every token as the endpoint wrote it. Only subtask 0 polls, whatever the job's parallelism; the other subtasks read nothing. This source does not use `batch_records` / `batch_bytes` / `dlq` / `linger_ms`.

#### Modes

| Mode | Set with | What each poll asks for | What a checkpoint holds | After a restore | A bounded run ends |
| --- | --- | --- | --- | --- | --- |
| Cursor | `cursor_param` and `cursor_field` | The records strictly after the cursor, sent as `?<cursor_param>=<cursor>` | The cursor | Polls again from the checkpointed cursor | At the first valid empty page |
| Resource | Neither | The same request every time | No position | Reads the resource as it is then | After the first valid response |

One of `cursor_param` and `cursor_field` without the other is refused, as are `initial_cursor` and `cursor_type` in resource mode and an empty value for any of these options.

#### What cursor mode needs from the endpoint

The source can replay only what the endpoint lets it ask for again. Cursor mode assumes the endpoint:
- answers the records strictly after the cursor it is sent, ascending by `cursor_field`, with each value unique;
- keeps those records long enough for any checkpointed cursor to be asked for again, that is, for as long as a restore can reach back;
- may end a page anywhere, the next poll continuing after the page's last record.

It checks every page against what it can of that, before it emits a record or moves the cursor:
- every record is an object with exactly one `cursor_field`, holding a usable value;
- with `cursor_type='integer'`, the values rise strictly and the first is past the cursor the request carried;
- with `cursor_type='opaque'`, no value repeats within the page and none is the cursor the request carried, since no order is assumed between two tokens.

A page that fails a check fails the poll: nothing of it is emitted, the cursor stays where it was, the subtask fails and the job restarts from its last checkpoint. That is also how an endpoint that ignores the cursor shows up, since it serves the request's own cursor, or records before it, again.

A value shared by two records, as timestamps often are, is refused. The cursor moves past a value, so a page that ended inside a run of equal values would skip the rest of the run, and the source cannot tell whether a page ended there. Give the endpoint a unique field to page on, or an opaque cursor it makes unique, such as a timestamp and an id in one token.

| `cursor_type` | Accepted values | Compared |
| --- | --- | --- |
| `opaque` (default) | A non-empty JSON string, or a number's text as written, at most 1,024 bytes | For equality only; sent back unchanged |
| `integer` | A decimal integer: a JSON integer of up to 64 bits, or a string of one of up to 100 digits | By value, exactly at any size |

Page tokens or next-page links, offsets, cursors made of several fields, and endpoints that include the record at the cursor are not supported.

#### Responses

| Response | What the source does |
| --- | --- |
| HTTP 200 with a JSON body | Reads the array at `records_field`, or the body itself when that is empty, and checks the page as above |
| HTTP 200 with an empty array, or HTTP 204 | An empty page: nothing emitted, the cursor unchanged. A bounded cursor-mode run ends here |
| HTTP 304 to a request that carried `If-None-Match` | Not modified: nothing emitted |
| HTTP 304 to any other request | Fails the poll |
| A missing, null or mistyped records field, a body that is not JSON or is cut short, a page over `max_response_records`, a body over `max_response_bytes` | Fails the poll: nothing emitted, the cursor unchanged |
| A record clink's JSON reader cannot decode: an integer past 64 bits, or a number past a double's range | Fails the poll. Every record the source emits stays readable by the operators after it, SQL's included; an endpoint should send such values as strings |
| Any other 2xx, any 3xx | Fails the poll. Redirects are not followed |
| 408, 425, 429, 5xx other than 501 and 505, no response, a request past `request_timeout_ms` | Retried, as below |
| Any other 4xx, such as 400, 401, 403 or 404 | Fails at once |

Records are emitted as served: the whitespace between tokens goes, and every number and string is kept byte for byte, so an integer past what a double holds reaches the next operator exact.

#### Conditional requests

The source sends `If-None-Match` with the previous response's ETag only when the request repeats that response's exactly: every poll in resource mode, and in cursor mode the poll after an empty page. A cursor that moves changes the request, so no validator is ever sent for another position. The ETag is not checkpointed: after a restore or a reopen the first request carries none, so a validator cannot hide records a restore has to read again. A bounded run sends none. A server whose validator ignores the cursor, answering 304 while its records have moved on, stops the source receiving records until the validator changes; that is a fault in the server the source cannot see.

#### Bounds, retries and cancellation

- `max_response_bytes` bounds a response's body, counted as it is decompressed. A longer body ends the request and fails the poll.
- `max_response_records` bounds the records of one response.
- `request_timeout_ms` bounds one request as a whole, however slowly the server sends, and `connect_timeout_ms` the connection.
- A failed attempt is retried after a backoff that starts at `retry_base_backoff_ms` and doubles up to `max_backoff_ms`, or after as long as `Retry-After` asks (seconds or an HTTP date) when that is longer. A poll makes at most `max_retries` retries, all within `retry_timeout_ms` of its first attempt; a wait that would end past that fails the poll at once rather than waiting.
- The poll interval and every backoff are waited out a slice at a time between calls into the source, so checkpoint barriers pass while it waits. A cancel ends a wait, or a request in flight, at once.
- Errors name the source, the request's path without its query string, and the status or the problem. They never carry a header, the query string or the URL; a URL with a user or password in it is refused without being repeated.

#### Recovery

In cursor mode the checkpoint holds the cursor, as operator state under the key the source has always used, so a checkpoint an earlier release wrote restores. A restore sends the source back to that cursor, and it reads again the records it had emitted since. Into the job that is at-least-once, and into a two-phase-commit sink it is exactly-once, while the endpoint keeps what it served. A restored cursor the configured `cursor_type` cannot read is refused, as is a cursor restored into a source in resource mode.

In resource mode nothing is replayed. A restart polls the resource as it is then: records emitted after the last checkpoint and lost with a worker come back only if the resource still holds them, and a change between two polls is never seen. A job over a resource-mode source cannot declare a delivery guarantee above at-most-once.

#### Options

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `path` | No | `/` | Request path from `/`, which may carry a fixed query string. |
| `headers` | No | (empty) | Request headers (auth). |
| `verify_tls` | No | `true` | HTTPS certificate verification: `true` or `false`. |
| `cursor_param` | No | (empty) | Cursor mode: the query parameter the cursor is sent as. |
| `cursor_field` | No | (empty) | Cursor mode: the record field each record's cursor is read from. |
| `cursor_type` | No | `opaque` | Cursor mode: `opaque` or `integer`, as above. |
| `records_field` | No | (empty) | Response field holding the array; if empty, the response itself is the array. |
| `initial_cursor` | No | (empty) | Cursor mode: where a run without a checkpoint starts; empty sends no cursor. |
| `bounded` | No | `false` | `true` ends the run as the modes table says. |
| `poll_interval_ms` | No | `1000` | From the start of one poll to the start of the next, `0` to `3600000`. |
| `jitter_frac` | No | `0` | Fraction of jitter on the poll interval, `0` to `1`. |
| `max_retries` | No | `4` | Retries one poll may make after its first attempt, `0` to `20`. |
| `retry_base_backoff_ms` | No | `200` | The first backoff, `1` to `60000`; it doubles per retry. |
| `max_backoff_ms` | No | `10000` | The longest backoff, from `retry_base_backoff_ms` to `600000`. |
| `retry_timeout_ms` | No | `60000` | The longest one poll may take from its first attempt to its last, `1` to `3600000`. |
| `connect_timeout_ms` | No | `5000` | `1` to `600000`. |
| `request_timeout_ms` | No | `30000` | One request as a whole, `1` to `600000`. |
| `max_response_bytes` | No | `16777216` | The most a response's body may hold, decompressed, `64` to `1073741824`. |
| `max_response_records` | No | `100000` | The most records one response may carry, `1` to `10000000`. |

Every option is parsed strictly: a value outside its range, or one that is not a number where a number is wanted, fails the job's build rather than falling back to a default.

#### Earlier releases

What changed from the source before this one, which a job moving to this release may notice:
- A response with a missing, null or mistyped records field, or malformed JSON, fails the poll. Before, it read as a page with no records, so it could end a bounded run early. HTTP 204 is an empty page, where it used to fail as malformed JSON, and a 304 to a request that carried no `If-None-Match` fails, where it used to read as no records.
- In cursor mode a record without a usable cursor, a repeated or falling value, or the request's own cursor fails the poll. Before, such records were emitted and the cursor stayed at the last usable value.
- Records are emitted as served. Before, each was written again through clink's JSON value, which rounded integers past 2^63 and decimals past a double's precision and rewrote string escapes, and an integer cursor past 2^63 went back to the endpoint rounded. So a replay across the upgrade can give a record different bytes, and an integer cursor an earlier release rounded is refused when restored under `cursor_type='integer'`.
- A bounded run in resource mode reads once. It used to poll again and emit the same records each time, unless the server answered 304.
- `bounded` and `verify_tls` take only `true` or `false`, and every number must be one, in range. `max_retries` outside `0` to `20` is refused rather than clamped, and a value that does not parse no longer falls back to the default.
- `url` must be `scheme://host[:port]` without a user or password. A URL with a path or a user in it never reached the endpoint, since the client took the whole text for a host name, and the error then repeated it, password included.
- The delivery analyser treats the source as replayable only in cursor mode. Before, its record claimed replay however it was set up, so a job over a resource-mode source could declare `at_least_once` or `exactly_once`; it is now refused.
- Errors no longer carry the URL or the query string.
- The source no longer builds on `PollingSource`, which is unchanged for its other users.

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
| `on_decode_error` | No | `fail` | A message whose `data` is not valid base64. `fail` stops the subtask without acknowledging it; `drop` acknowledges it with its checkpoint, emits nothing and counts it in `dropped_events_total`. In SQL the same option, when set, also decides what the JSON decoding does with data that is not a JSON object it can read; left unset, that decoding drops it (see [Records the JSON decoding cannot read](README.md#records-the-json-decoding-cannot-read)). |

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

For `connector='influxdb'` and `connector='prometheus'` the planner also refuses an option the sink does not read, naming the ones it does, and `changelog='true'`. It keeps a `connector='prometheus'` sink on one subtask whatever the job's parallelism, the operators in front fanning in to it, since a second subtask would replace the first one's series.

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

Source example (HTTP poll, cursor mode on a unique, rising integer id):

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
    cursor_type      = 'integer',
    poll_interval_ms = '2000'
);
```

A sink over this table may declare `delivery_guarantee='exactly_once'` when it is a two-phase-commit sink, and holds it while the endpoint answers strictly after the cursor and keeps what it served. Without `cursor_param` and `cursor_field` the table is in resource mode, and the coordinator refuses any guarantee above `at_most_once` for a job over it.

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

- Sinks (`http_sink`, `elasticsearch_sink`, `opensearch_sink`, `splunk_hec_sink`, `influxdb_sink`, `prometheus_sink`, `pubsub_sink`) are at-least-once. The shared `BatchedHttpBulkSink`, which every one of them but `prometheus_sink` builds on, flushes on every checkpoint barrier (before the sink acks), on the size and linger thresholds, and at end-of-stream. On permanent failure with retries exhausted it throws rather than dropping, keeping the batch, so the job replays from the last checkpoint. A record a sink refuses before sending is dead-lettered under `dlq='drop'`; under `dlq='fail'` the sink throws and, keeping no copy of it, refuses every later record and barrier until it opens again. A response that refuses some records without naming them (an InfluxDB partial write) fails the sink under `fail` and is dead-lettered as one report under `drop`. There is no two-phase commit, so a replay re-delivers buffered records.
- `elasticsearch_sink` / `opensearch_sink` are at-least-once. Without `document_id` a resend or a replay indexes the record again; a reply lost after the server applied the request is enough to duplicate a batch. With `document_id` each record replaces its id's document, so a replay converges on each id's latest record, ordered within a sink subtask as [Ordering](#ordering) describes. A batch counts as written only when the bulk response confirms every operation, and failures are classified as [the table above](#when-a-batch-counts-as-written) sets out.
- `influxdb_sink` is at-least-once. With `timestamp_field` a rewritten point lands on its own series and time, where InfluxDB merges it into the point already there; without it every write is a new point, so a replay duplicates. [Its section](#influxdb_sink) has the contract.
- `splunk_hec_sink` is at-least-once with no dedup key; HEC ingestion is append, so a replay indexes again.
- `prometheus_sink` is at-least-once into a store of latest values: each push replaces the group's series of the metric, so a replay ends at each series' latest value, and `push_series` decides which series each push carries. [Its section](#prometheus_sink) has the contract.
- `pubsub_sink` is at-least-once with no producer dedup; a replay re-publishes.
- `http_poll_source` in cursor mode replays from its checkpointed cursor: at-least-once into the job, and exactly-once into a two-phase-commit sink, while the endpoint answers the records strictly after the cursor, ascending and each value unique, and keeps what it served. Every page is checked against what the source can see of that before anything is emitted, and one that fails fails the poll (see [the source's section](#http_poll_source)). Its capability record, `http_poll`, declares replay only with `cursor_field` and `cursor_param`, so the delivery analyser reports a job over a source in resource mode as `AT_MOST_ONCE_SOURCE` and refuses `at_least_once` and above for it, naming the options that would make it replay. Before, the source shared the `http` record, which claimed replay however the source was set up.
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
- `prometheus_sink` targets a Pushgateway, a store of each series' latest value for a modest number of series, not a time-series database: Prometheus sees only the values present when it scrapes. The sink must be its group's only writer of its metric and runs on one subtask. With `push_series='batch'` a series not named since the previous push disappears; with `'all'` a series stays until the group is deleted. Label names outside the label grammar and integer values past 2^53 are refused. Remote-write is not implemented.
- `http_poll_source` replays only in cursor mode, and only from an endpoint that answers strictly after the cursor, ascending by a unique `cursor_field`, and keeps what it served; it refuses a page that breaks what it can check of that, including a value shared by two records. Page tokens, offsets and cursors of several fields are not supported. One subtask polls. A bounded run ends only on an empty page (cursor mode) or after one response (resource mode). Records with an integer past 64 bits or a number past a double's range are refused, since clink's JSON reader cannot decode them.
- `pubsub_source` holds at most `max_outstanding` unacknowledged messages per subtask, and a job that completes no checkpoint stops pulling there. A message held past `max_lease_s` is delivered again (a duplicate). Subscriptions with exactly-once delivery enabled are not handled specially.
- Pub/Sub uses the synchronous REST `Pull` with `returnImmediately`; gRPC StreamingPull is not used. The build does not mint OAuth2 tokens itself; a static `auth_token` expires after its lifetime, but `auth_token_file` lets an external refresher rotate the token without a restart. Multiple subtasks pull the same subscription and Pub/Sub load-balances across them (no client-side sharding).
- The bulk sinks buffer rendered records in memory until a flush trigger; `batch_records` / `batch_bytes` / `linger_ms` bound that buffer.
- `linger_ms` is evaluated only when a record arrives (no timer thread), so a fully idle buffer waits for the next checkpoint barrier.

## Testing

The module's tests live in `impls/http_connector/tests/` and carry the ctest label `http_connector`. Most are in-process or mock tests (factory registration, framing, the Splunk body builder, the Pushgateway sample rules) and run in a normal build:

```bash
ctest --test-dir build -L http_connector
```

The live integration tests are skipped unless their endpoint environment variable is set. The sidecar services are defined in `docker/integration-services.yml`:

| Test | Env var(s) | Service / image |
| --- | --- | --- |
| Elasticsearch (`Elasticsearch/SearchSinkLive`, `test_elasticsearch_sink_live.cpp`) | `CLINK_ELASTICSEARCH_TEST_ENDPOINT` | `docker.elastic.co/elasticsearch/elasticsearch:8.13.4` |
| OpenSearch (`OpenSearch/SearchSinkLive`, same file) | `CLINK_OPENSEARCH_TEST_ENDPOINT` | `opensearchproject/opensearch:2.17.1`, security plugin off |
| InfluxDB (`InfluxDbLive`, `test_influxdb_live.cpp`) | `CLINK_INFLUXDB_TEST_ENDPOINT`, plus `CLINK_INFLUXDB_TEST_ORG` / `_BUCKET` / `_TOKEN` (defaults `clink` / `clink` / `clink-token`) and `_VERSION` | `influxdb:2.7.12`, pinned by digest in `scripts/influxdb-live.sh` |
| Pub/Sub (`test_pubsub_live.cpp`) | `CLINK_PUBSUB_EMULATOR_HOST` (e.g. `localhost:8085`) | `gcr.io/google.com/cloudsdktool/cloud-sdk:emulators` |
| Pushgateway (`PushgatewayLive`, `test_prometheus_live.cpp`) | `CLINK_PUSHGATEWAY_TEST_ENDPOINT`, plus `CLINK_PUSHGATEWAY_TEST_VERSION` | `prom/pushgateway:v1.10.0`, pinned by digest in `scripts/prometheus-live.sh` |

There is no live test for `splunk_hec_sink`; it is covered by in-process and mock tests. `http_poll_source` has a gate of its own against local servers, below, and also feeds the search recovery cells, from a feed the test serves.

`pubsub_source`'s acknowledgement contract is tested in process by `PubSubSourceAck` (`test_pubsub.cpp`), against a stub of the REST endpoints that leases each delivery under a new ackId, honours `modifyAckDeadline`, acknowledges only through a current ackId and can fail acknowledges: an aborted checkpoint, crashes after the barrier and after completion, failed and retried acknowledges, notifications out of order, leases held across a delayed checkpoint and given up past `max_lease`, the `max_outstanding` bound, close after a cancel, and both `on_decode_error` policies. `PubSubSourceAckLive` (`test_pubsub_live.cpp`) repeats the abort, the crash after completion and the delayed checkpoint against the emulator. `test_factory_registration.cpp` pins the capability record and the analyser's verdict. `scripts/ack-sources-live.sh [build-dir]` is the required gate: it runs `PubSubLive` and `PubSubSourceAckLive` against a one-off emulator container, with the other acknowledging sources' suites, and fails on a missing binary, an emulator that never answers, a failed or skipped case, or a short suite (see the [Redis page](redis.md#testing)). The `ack-sources` CI workflow runs it.

### InfluxDB

Without a server: `InfluxRender` (point identity, every escape and refusal, timestamp boundaries per precision, neighbours above 2^53, float and typed numbers, refusals that name no value, the option checks), `InfluxWriteResult` (each response class) and `InfluxSink`, the sink against a stub `/api/v2/write`: the request's path, precision and token, an unexpected 2xx, refused records under each policy, a 400, a partial write under each policy (the batch kept under `fail`, one report under `drop`), a refused token never dead-lettered, a reply lost after the server received the batch, retries spent and cancelled, the factory's strict options. `SqlPhysical.InfluxDbOptionsAreCheckedWhenTheJobIsPlanned` covers the planner.

`InfluxDbLive` writes to a live server and reads every point back with Flux, comparing tags, times, field values and field types: the version, exact timestamps to the ends of the range, every escape, refused records beside good ones under each policy, float and typed numbers and a type conflict's partial write, merged identities, a reply withheld by a proxy after the server applied the batch (one point with a timestamp, two without), and a refused token and a missing bucket. Flux's CSV writes a line break inside a value as CRLF and drops a lone carriage return, so the reader undoes the first and checks the second through a Flux replace.

`InfluxDbRecovery` (`tests/integration/test_influxdb_recovery.cpp`) runs a coordinator and two workers polling a feed into the sink and kills a worker parked at `influxdb_sink.after_write`, after writes the server confirmed and no checkpoint covered: timestamped, where every record must end as exactly one point with its value; in server-time mode, where every record must be present and the replay's duplicates present too; and as a SQL job at parallelism 2.

`scripts/influxdb-live.sh [build-dir]` is the gate: it starts InfluxDB 2.7.12, pinned by digest, on a free loopback port with a throwaway token, runs the suites above, and fails on a missing binary, a server that never becomes ready, a failed or skipped case or a short suite, keeping each run's report, the cells' cluster logs and a provenance file (never the token) in `build/influxdb-live-reports/`. The `http` CI workflow runs it.

The gate passed locally on commit 50bdbaa6, 37 of 37, on 2026-11-07 (reports not published). In the timestamped cell 3,000 records left exactly 3,000 points after the kill and replay; in server-time mode 294 of the 318 replayed records were written twice, leaving 3,294 points; the SQL cell left 3,000. The previous sink's defects (a missing or fractional timestamp, a missing tag, line breaks in names, malformed records sent as broken lines, nested values dropped, a 422 dead-lettering written points) were found by reading its code and are each covered by a case above; they were not run against the server with the old code.

### Pushgateway

Without a gateway: `PromRender` (series identity, label names refused rather than rewritten, every label value kept and escaped, numbers labelled as written with neighbours above 2^53 and integers past 64 bits, empty and null labels absent, the value's types and refusals, grouping conflicts, the sample bound, refusals that name no value, the path's encoding, the options), `PromPushResult` (each response class, and a 400's quoted values removed) and `PromSink`, the sink against a stub gateway: the push's path, content type and body; repeated updates; each trigger in each `push_series` mode; the bounds in all mode; refused records under each policy, never replacing a valid sample; a 400 refused once and kept; throttling and outages retried; refused credentials, never dead-lettered and never in a message; a reply lost after the gateway received the push; retries ended by a cancel and spent; a push in flight cancelled; a restored run adopting its group, and refusing one it cannot adopt; the factory's strict options and its refusal of a parallelism above 1. `SqlPhysical.PrometheusOptionsAreCheckedWhenTheJobIsPlanned` covers the planner.

`PushgatewayLive` pushes through the sink and reads the gateway back with a scrape of its own: the version; labels with control characters, Unicode and escapes, neighbours above 2^53 and integers past 64 bits; group values a path segment cannot carry; values as the gauge holds them; refused records beside good ones; each batch-mode trigger replacing the previous push while another metric of the group stays; all mode keeping every series at its latest value; a restored run adopting the group's series and a fresh one replacing them; two writers of one group replacing each other; a type conflict with another group refused at once and pushed once it clears; a reply withheld by a proxy after the gateway applied the push; and a group deleted under the sink pushed again at the next barrier.

`PushgatewayRecovery` (`tests/integration/test_prometheus_recovery.cpp`) runs a coordinator and two workers polling a feed into the sink, where record n sets series `k<n mod 100>` to n and five early series appear only in the first 150 records, and kills a worker parked at `prometheus_sink.after_write` after a push the gateway applied and no checkpoint covered. In all mode, as a job graph and as a SQL job at parallelism 2, the gateway must end with every series at its latest value, the early ones included, which only the restored run's adoption of the group can keep, since the job restores past their records; in batch mode, with the series of the last push, each at its latest, and no early one. Each cell checks where the worker parked, one restart, the restore point, and that the parked push carried records past it.

`scripts/prometheus-live.sh [build-dir]` is the gate: it starts Pushgateway 1.10.0, pinned by digest, on a free loopback port, runs the suites above, and fails on a missing binary, a gateway that never becomes ready, a failed or skipped case or a short suite. It keeps each run's report, the cells' cluster logs and final scrapes, and a provenance file in `build/prometheus-live-reports/`. The `http` CI workflow runs it.

### Elasticsearch and OpenSearch

`scripts/search-live.sh <server> [build-dir]` is the sinks' gate, for one server at a time. The servers are `elasticsearch` (8.13.4), `elasticsearch9` (9.2.0), `opensearch` (2.17.1) and `opensearch3` (3.3.0), run one after the other. Each gate runs four phases, each on its own server, started on a free loopback port and removed when the phase ends:

| Phase | Server | Suites |
| --- | --- | --- |
| main | The pinned image, security off | The offline suites, `<Product>/SearchSinkLive`, `<Product>/SearchRecovery` |
| throttled | The same image with a write pool of one thread and a write queue of one, so the server itself rejects writes with 429 | `<Product>/SearchSinkThrottledLive` |
| secured | The same image with its own security on: HTTPS with a certificate from its own CA, every request authenticated, an administrator's password generated for the run and passed only through the environment | `<Product>/SearchSinkSecuredLive` |
| cluster | Three nodes of the same image (`docker/search-cluster.yml`), the sink writing to node 1 | `<Product>/SearchSinkClusterLive` |

A gate fails on a skipped case, a suite that ran fewer cases than it has, a missing binary, a server that never answers or is not the product named, and the secured phase's password appearing anywhere in what it keeps. It keeps each run's report and output, the recovery cells' coordinator and worker logs, the secured server's CA certificate, and a `provenance.txt` naming the commit, the build's configuration and each phase's server version, image and digest. The `http` CI workflow runs all four gates; it was named `search` until the polling source's gate joined it.

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

All four gates passed in local runs on 2026-11-06, 55 of 55 each. Elasticsearch 8.13.4 and OpenSearch 3.3.0 ran on commit `52bfc6f9`. Elasticsearch 9.2.0 and OpenSearch 2.17.1 ran on `a3ecff67`, which adds only the gate's retry of a port Docker refused; on `52bfc6f9`, Docker had refused one port in each of those two gates. In every case the fault landed with records in flight or not yet covered by a checkpoint, and the throttled servers' own 429s ran from 2,999 to 6,940 items. The cluster absorbed a non-sink node's kill without failing a write, so that case shows nothing is lost; the case that kills the sink's own node shows the flush failing and recovering. These reports are not published. The `search` CI workflow then ran all four gates on commit `9102113f` (run 42199665079): 55 of 55 for each server.

### The polling source

`scripts/http-poll-live.sh [build-dir]` is the polling source's gate. It needs no service: every case runs against HTTP servers the tests start themselves. It fails on a skipped case, a suite that ran fewer cases than it has, a missing binary or a failed case, and keeps each run's report and output, the recovery cells' coordinator and worker logs, and a `provenance.txt` naming the commit, the build's configuration and the feed the cells served. The `http` CI workflow runs it after the search gates, whenever the build succeeded.

| Suite | Binary | What it checks |
| --- | --- | --- |
| `HttpPollPage` | `clink_http_connector_tests` | The page reader on its own: an array or a configured envelope; numbers and escapes kept as served; a missing, null, mistyped or repeated records field; 26 malformed or empty bodies; a record clink's JSON reader cannot decode; the record bound; every record's cursor (missing, null, mistyped, repeated, falling, at or before the request's cursor, a run of equal timestamps); integer cursors exact at any size |
| `HttpPollConfig` | `clink_http_connector_tests` | Cursor and resource mode's option combinations; a URL with a password refused without being repeated; every range; the factory's strict parsing of each option |
| `HttpPollSourceCursor` | `clink_http_connector_tests` | Against a scripted endpoint: paging a retained feed; a bounded run ending at the first valid empty page and never on a malformed one; 204; cursors sent exactly and percent-encoded; an endpoint that ignores the cursor; conditional requests only for the same request, against a server whose validator ignores the cursor; a restore into the same and into a fresh source, through the file state backend, dropping the validator and reading again what the checkpoint did not cover; a reopen; an earlier release's checkpoint, and a restored cursor the configuration cannot read |
| `HttpPollSourceResource` | `clink_http_connector_tests` | Every response emitted and an unchanged one not; a bounded run reading once; a 304 to a request that did not ask |
| `HttpPollSourceResponses` | `clink_http_connector_tests` | Other 2xx, redirects and permanent 4xx failing at once, without a retry |
| `HttpPollSourceBounds` | `clink_http_connector_tests` | An oversized body; one that passes the bound only once decompressed; a body sent slowly and headers held back, each bounded by `request_timeout_ms` as a whole |
| `HttpPollSourceRetries` | `clink_http_connector_tests` | Each retried status and a refused connection, retried then exhausted; `retry_timeout_ms`; `Retry-After` in seconds and as a date, and one past the budget failing at once |
| `HttpPollSourceCancel` | `clink_http_connector_tests` | A cancel ending a poll interval, a backoff and a request in flight at once |
| `HttpPollSourceErrors` | `clink_http_connector_tests` | No header, token or query string in any error, a body that echoes them included |
| `HttpPollCapability` | `clink_http_connector_tests` | The `http_poll` record, and the coordinator's verdict: `exactly_once` accepted in cursor mode, and refused in resource mode or with only one cursor option, naming what is missing |
| `HttpPollRecovery` | `clink_integration_tests` | A coordinator and two workers polling a retained feed (2,400 records released at 600 a second, at most 100 a page, each with an integer past int64) into a two-phase-commit file sink; worker 0 parked just after the source emitted a page and killed with it in flight, at parallelism 1 and 2 and as a SQL job submitted by `clink_submit_sql`. Each checks the committed files against the feed's dataset, every record once and, in the C++ cells, byte for byte; that the feed had served past what was committed at the kill; and that the restored source asked again from before the page it had emitted |
| `HttpPollSql` | `clink_integration_tests` | `clink run` reading the feed bounded to its end and finishing, every row once; the coordinator refusing `at_least_once` over a table in resource mode |

What these establish is how the source treats each response a local server gives it. They qualify no third-party API: whether cursor mode is safe against one depends on that API answering strictly after the cursor, on a unique field, and keeping what it served.

The gate passed locally on commit `5c849d4d`, 44 of 44, on 2026-11-07. In each recovery cell the feed had served past what was committed when the worker died (to 1,039, 1,281 and 1,184 records against 831, 1,145 and 773 committed), the restored source asked again from the last completed checkpoint's cursor, and every one of the 2,400 records was committed once after one restart. On the same code the Elasticsearch 8.13.4 recovery cells of the search gate, which poll their feed through the source, passed 5 of 5. These reports are not published. The `http` CI workflow has not run the gate yet.
