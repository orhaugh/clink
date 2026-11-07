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

- Sinks (`http_sink`, `elasticsearch_sink`, `opensearch_sink`, `splunk_hec_sink`, `influxdb_sink`, `prometheus_sink`, `pubsub_sink`) are at-least-once. The shared `BatchedHttpBulkSink` flushes on every checkpoint barrier (before the sink acks), on the size and linger thresholds, and at end-of-stream. On permanent failure with retries exhausted it throws rather than dropping, so the job replays from the last checkpoint. There is no two-phase commit, so a replay re-delivers buffered records.
- `elasticsearch_sink` / `opensearch_sink` are at-least-once. Without `document_id` a resend or a replay indexes the record again; a reply lost after the server applied the request is enough to duplicate a batch. With `document_id` each record replaces its id's document, so a replay converges on each id's latest record, ordered within a sink subtask as [Ordering](#ordering) describes. A batch counts as written only when the bulk response confirms every operation, and failures are classified as [the table above](#when-a-batch-counts-as-written) sets out.
- `influxdb_sink` is effectively-once when `timestamp_field` is set, since an identical point is idempotent.
- `splunk_hec_sink` and `prometheus_sink` are at-least-once with no dedup key; HEC ingestion is append, and the Pushgateway stores the latest value per series (last-write-wins within a push). Each push is a POST, which replaces every series of the metric in its group with the series the push carries, and a push carries the series of the records since the previous one. So a series no record has named since the previous push disappears from the gateway rather than keeping its last value; give every series a record in each checkpoint interval if they must all stay visible.
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
- `prometheus_sink` targets a Pushgateway, which is a latest-value store for low-cardinality service metrics, not a high-throughput event TSDB. Records whose `value_field` is missing or non-numeric are dropped (no sample). Remote-write is not implemented.
- `http_poll_source` replays only in cursor mode, and only from an endpoint that answers strictly after the cursor, ascending by a unique `cursor_field`, and keeps what it served; it refuses a page that breaks what it can check of that, including a value shared by two records. Page tokens, offsets and cursors of several fields are not supported. One subtask polls. A bounded run ends only on an empty page (cursor mode) or after one response (resource mode). Records with an integer past 64 bits or a number past a double's range are refused, since clink's JSON reader cannot decode them.
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

There is no live test for `splunk_hec_sink` or `prometheus_sink`; these are covered by in-process and mock tests. `http_poll_source` has a gate of its own against local servers, below, and also feeds the search recovery cells, from a feed the test serves.

`pubsub_source`'s acknowledgement contract is tested in process by `PubSubSourceAck` (`test_pubsub.cpp`), against a stub of the REST endpoints that leases each delivery under a new ackId, honours `modifyAckDeadline`, acknowledges only through a current ackId and can fail acknowledges: an aborted checkpoint, crashes after the barrier and after completion, failed and retried acknowledges, notifications out of order, leases held across a delayed checkpoint and given up past `max_lease`, the `max_outstanding` bound, close after a cancel, and both `on_decode_error` policies. `PubSubSourceAckLive` (`test_pubsub_live.cpp`) repeats the abort, the crash after completion and the delayed checkpoint against the emulator. `test_factory_registration.cpp` pins the capability record and the analyser's verdict. `scripts/ack-sources-live.sh [build-dir]` is the required gate: it runs `PubSubLive` and `PubSubSourceAckLive` against a one-off emulator container, with the other acknowledging sources' suites, and fails on a missing binary, an emulator that never answers, a failed or skipped case, or a short suite (see the [Redis page](redis.md#testing)). The `ack-sources` CI workflow runs it.

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
