# Redis Streams

> Connects to Redis Streams as both a source (`XREADGROUP` over a consumer group) and a sink (`XADD`), and keeps a key-value view of a changelog with `mode='upsert'` (`SET` and `DEL`).

## Overview

The Redis connector integrates Redis Streams over the synchronous hiredis client. The sink appends each record to a stream with `XADD`; the source reads a stream through a consumer group with `XREADGROUP`, with each parallel subtask joining the group as a distinct consumer so the group hands out disjoint entries. Both connectors operate on the string channel: records are carried as Redis stream entry field values, by default a single field `v` so a sink-then-source round-trip returns each payload verbatim. On the SQL path the record is a JSON object string; a source entry that is not a single `v` field is rendered as a JSON object of all its field/value pairs (all values are Redis bulk strings, so all string-typed).

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| hiredis | System package via apt (Debian `libhiredis-dev`) / brew (macOS) | Not pinned by clink |
| libhiredis_ssl + OpenSSL | System package via apt / brew, optional (enables TLS) | Not pinned by clink |

The connector does not use Arrow; records ride the string channel directly.

## Enabling it

The module is gated by the `CLINK_WITH_REDIS` CMake option, default `AUTO` (`AUTO`/`ON`/`OFF`). Under `AUTO` the target is built only when hiredis is discovered (via pkg-config, a hiredis CONFIG package, or a manual header/library probe of the common Homebrew and Linux paths). With `ON` and no hiredis present, configuration fails with a fatal error. The apt package is `libhiredis-dev` (installed by `scripts/install-connector-deps.sh`).

TLS is a separate, optional capability: when both `libhiredis_ssl` and OpenSSL are found the build defines `CLINK_REDIS_TLS` and `tls=true` works at runtime; otherwise the connector still builds and `tls=true` is rejected at runtime with a clear message.

```bash
cmake -S . -B build -DCLINK_WITH_REDIS=ON
```

## Factories

| Factory name | Direction | Record type |
| --- | --- | --- |
| `redis_source` | Source | `std::string` |
| `redis_sink` | Sink | `std::string` |
| `redis_upsert_sink` | Sink | `std::string`, a JSON object (`SET` or `DEL` by the key its primary key names) |

(Registered in `impls/redis/src/register_factories.cpp`.)

## Configuration

Options are parsed off the `BuildContext` in `register_factories.cpp`; the shared connection options come from the `ConnectOptions` struct in `redis_client.hpp`. The sink-only and source-only options come from `RedisSinkOptions` (`redis_sink.hpp`), `RedisUpsertSinkOptions` (`redis_upsert_sink.hpp`) and `RedisSourceOptions` (`redis_source.hpp`).

The connection options and the sinks' options are read strictly: a number outside its range, or a boolean other than `true` or `false`, fails the job's build rather than reading as the default.

### Connection (both source and sink)

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `host` | No | `localhost` | Redis host. |
| `port` | No | `6379` | Redis port, 1 to 65535. |
| `username` | No | (empty) | ACL username (Redis 6+). Empty means legacy `AUTH <password>`. |
| `password` | No | (empty) | Password for `AUTH`. Empty means no authentication. |
| `db` | No | `0` | Database index, 0 or more; `SELECT <db>` is issued only when non-zero. |
| `tls` | No | `false` | `true` wraps the connection in TLS before `AUTH`. Requires a TLS-enabled build (`CLINK_REDIS_TLS`); otherwise rejected at runtime. |
| `tls_ca` | No | (empty) | Optional PEM path to a CA certificate for server verification. |
| `tls_cert` | No | (empty) | Optional PEM path to a client certificate for mutual TLS. |
| `tls_key` | No | (empty) | Optional PEM path to the client private key for mutual TLS. |
| `tls_sni` | No | (empty) | The name sent as SNI and the one the server's certificate must carry; defaults to `host`. |
| `tls_verify` | No | `true` | `false` skips server-certificate verification (self-signed development certs), the hostname check included. |
| `tls_verify_hostname` | No | `true` | `false` keeps the certificate-chain check but accepts a certificate that names another host. |

Authentication-related options are `username`, `password`, and the `tls_*` set. The TLS handshake runs before `AUTH`, so credentials are sent over the encrypted channel when `tls=true`. A `tls_ca`, `tls_cert`, `tls_key` or `tls_sni` given without `tls=true` is refused, since the connection would be made in the clear; so is a `tls` other than `true` or `false` (a `TRUE` once read as false).

With `tls_verify` on, the handshake checks both that the server's certificate chains to a trusted CA (`tls_ca`, or the system store when it is empty) and that it names the host connected to (`tls_sni` when set, otherwise `host`). A name is matched against the certificate's DNS subjectAltNames, or its subject CN when it has none, with a wildcard covering one whole left-most label; an IP address is matched against its IP subjectAltNames only. A mismatch fails the connect with an error that names the host, what the certificate names, and `tls_verify_hostname=false`. hiredis itself checks only the chain, so the connector builds the TLS session and asks for the name before the handshake.

### Sink (`redis_sink`)

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `stream` | Yes | (none) | Target stream key. Construction fails if empty. |
| `field` | No | `v` | Field name holding each record's payload. Must not be empty. |
| `maxlen` | No | `0` | `XADD MAXLEN` cap on stream length; `0` means unbounded. |
| `approx_maxlen` | No | `1` | `1` or `true` uses approximate trim (`MAXLEN ~`); `0` or `false` exact (`MAXLEN =`). |
| `batch_records` | No | `1000` | Flush the pipelined buffer after this many records, 1 to 1,000,000. |
| `max_bytes` | No | `0` | Flush after this many buffered payload bytes, up to 1 GiB; `0` means 16 MiB. |
| `max_record_bytes` | No | `16777216` | A longer record fails the sink before anything of its flush is sent; 1 byte to 512 MiB. |
| `command_timeout_ms` | No | `30000` | The longest a command waits for its reply, 1 to 3,600,000 ms; past it the flush's outcome is unknown and the sink fails. |
| `linger_ms` | No | `0` | Flush a partial batch once it is this old, 0 to 3,600,000 ms; `0` disables linger. |

### Keyed sink (`redis_upsert_sink`)

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `key_columns` | Yes | (none) | The primary key, comma-separated. On the SQL path the planner sets it from the table's `PRIMARY KEY`. |
| `key_prefix` | No | (empty) | Prefix of every key the sink writes. |
| `batch_records` | No | `1000` | Flush after this many buffered keys, 1 to 1,000,000; changes to one key are one key. |
| `max_bytes` | No | `0` | Flush after this many buffered bytes of keys and values, up to 1 GiB; `0` means 16 MiB. |
| `max_record_bytes` | No | `16777216` | A longer record fails the sink; 1 byte to 512 MiB. |
| `command_timeout_ms` | No | `30000` | As for `redis_sink`. |

### Source (`redis_source`)

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `stream` | Yes | (none) | Stream key. Construction fails if empty. |
| `group` | Yes | (none) | Consumer-group name. Construction fails if empty. |
| `consumer_prefix` | No | `clink` | Consumer name is `<prefix>-<subtask_idx>`. |
| `field` | No | `v` | Single-field round-trip: an entry holding only this field has its value emitted verbatim. |
| `count` | No | `500` | `XREADGROUP COUNT` per call. Values below 1 are clamped to 1. |
| `block_ms` | No | `500` | `XREADGROUP BLOCK` in milliseconds; bounds cancel latency. Non-positive values are normalised to 500 (a `BLOCK 0` would block forever and wedge the runner thread). |
| `start_id` | No | `$` | Group-create position: `$` reads new entries only, `0` reads existing history. |
| `max_pending` | No | `100000` | Most entries a subtask holds delivered but unacknowledged, each waiting for the checkpoint that covers it to complete. At the bound the source stops reading until one completes. Must be at least 1. |

## SQL usage

Redis is mapped in `src/sql/physical_plan.cpp` to the `connector='redis'` string for both source (`redis_source`) and sink (`redis_sink`). On the SQL path the row is bridged to and from a JSON object string (`json_string_to_row` for the source, `row_to_json_string` for the sink). The default Streams sink is at-least-once; `mode='upsert'` with a PRIMARY KEY selects the changelog-aware key-value sink (`redis_upsert_sink`, see [the sinks](#the-sinks)) instead. A query that produces a changelog reaches the Streams sink only with `changelog='true'`, and its records then keep their `__row_kind` marker.

A sink table's options are checked when the job is planned (`check_redis_sink_options`): one the sinks do not read is refused, naming those they read; `maxlen`, `approx_maxlen` and `linger_ms` are refused with `mode='upsert'`, and `key_prefix` without it; `key_columns` is refused, since the table's PRIMARY KEY supplies it. The source's options pass, so one table can serve as both.

```sql
CREATE TABLE events_in (
  id    BIGINT,
  payload STRING
) WITH (
  connector = 'redis',
  host = 'localhost',
  port = '6379',
  stream = 'events',
  group = 'analytics',
  start_id = '0'
);

CREATE TABLE events_out (
  id    BIGINT,
  payload STRING
) WITH (
  connector = 'redis',
  host = 'localhost',
  port = '6379',
  stream = 'events_processed'
);

-- One key per customer, replaced as the totals change: customer:<id>.
CREATE TABLE totals (
  customer_id BIGINT,
  total       DECIMAL(18, 2)
) WITH (
  connector = 'redis',
  host = 'localhost',
  mode = 'upsert',
  primary_key = 'customer_id',
  key_prefix = 'customer:'
);
```

A NaN or infinite double in a row reaches this sink through the `row_to_json_string` bridge as `null`, or as `"NaN"`, `"Infinity"` or `"-Infinity"` when the sink table sets `json_non_finite='string'`; the sink handles it as it handles any null or string value. See [NaN and infinity in JSON output](README.md#nan-and-infinity-in-json-output).

## Example

Programmatic use via the typed classes and their option structs, mirroring `impls/redis/tests/test_redis_live.cpp`:

```cpp
#include "clink/redis/redis_sink.hpp"
#include "clink/redis/redis_source.hpp"

using clink::redis::RedisSink;
using clink::redis::RedisSinkOptions;
using clink::redis::RedisSource;
using clink::redis::RedisSourceOptions;

// Sink: XADD each record onto the "events" stream.
RedisSinkOptions sink_opts;
sink_opts.conn.host = "localhost";
sink_opts.conn.port = 6379;
sink_opts.stream = "events";
RedisSink sink(std::move(sink_opts));
sink.open();
clink::Batch<std::string> batch;
batch.emplace(R"({"i":0})");
batch.emplace(R"({"i":1})");
sink.on_data(batch);
sink.flush();
sink.close();

// Source: read "events" through a consumer group from the start of history.
RedisSourceOptions src_opts;
src_opts.conn.host = "localhost";
src_opts.conn.port = 6379;
src_opts.stream = "events";
src_opts.group = "g1";
src_opts.start_id = "0";  // "$" for new entries only
RedisSource src(std::move(src_opts));
src.open();
clink::Emitter<std::string> out{/* ... */};
src.produce(out);  // emits each entry's "v" value verbatim
src.close();
```

The factories can also be resolved through the runner registry by name (`redis_source` / `redis_sink` on the `string` channel), as in `impls/redis/tests/test_factory_registration.cpp`.

## The sinks

The Streams sink and the keyed sink share one core, `impls/redis/src/redis_sink_core.cpp`: the connection and its command timeout, the cancel watch, the failure state, and a pipelined write that reads every reply. The keyed sink's key rule is in `impls/redis/src/redis_sink_rules.cpp`; the JSON reader, the changelog marker and the removal of the marker from the stored value are the record rules the SQL sinks share (`include/clink/connectors/sink_record_rules.hpp`).

### Modes

| Mode | Selected by | Commands | Delivery |
| --- | --- | --- | --- |
| Append to a stream | `redis_sink`, or SQL `connector='redis'` | `XADD <stream> [MAXLEN ~\|= n] * <field> <record>` per record | At-least-once: a replay appends its records again. |
| Key-value view of a changelog | `redis_upsert_sink`, or SQL `mode='upsert'` | `SET <key> <value>` for `insert`, `update_after` and a record with no `__row_kind`; `DEL <key>` for `delete` and `update_before` | Effectively-once on the keyspace, for a stable key and a deterministic query. |

### Writes and failures

Records are buffered and written at the count, byte and linger thresholds and at every checkpoint barrier, so a completed checkpoint covers only records Redis holds. A flush sends its commands pipelined and reads every reply.

- A command Redis refuses (an error reply, such as `WRONGTYPE` on a key that is not a stream, or `OOM` under `maxmemory`) does not stop the others, which Redis runs regardless. The flush then fails, saying how many commands were refused, the first refusal, and how many were applied.
- A reply that does not come, because the connection was lost, the command timeout passed or the task was cancelled, leaves the commands from there on unknown: each may or may not have run. A command that outlasts the timeout can still run afterwards; the live suite pauses the server's writes past the timeout and sees the server apply the flush once they resume. The error says how many were applied before the reply stopped and how many are unknown.
- When the task's cancel is requested during a flush, a watch thread shuts the connection's socket, so the flush fails at once rather than waiting out the command timeout.

Any failure fails the sink: every later record, barrier and flush is refused, an empty flush included, until the sink is opened again, which the job's restart does. The restart writes again what its last checkpoint did not cover. `close()` discards what is buffered, releases the connection and never throws; a clean end of input flushes before it. Errors never quote a record's contents.

A reply means the server applied the command in memory. Persistence (`appendfsync`) and replication are the server's, and the sink waits for neither, so a primary that fails before persisting or replicating can lose acknowledged writes.

### Bounds

A flush holds at most `batch_records` records (or keys) and `max_bytes` bytes (16 MiB unless set). A record longer than `max_record_bytes` (16 MiB unless set) fails the sink before anything of its flush is sent, rather than reaching the server's own limit (`proto-max-bulk-len`, 512 MB by default), which would close the connection and leave the flush unknown. A command waits at most `command_timeout_ms` (30 s unless set) for its reply.

### The Streams sink

Each record is stored as written under one field (default `v`), byte for byte, so `redis_source` reads it back as written. `XADD` has no producer key, so the records a restored job writes again are appended again. `MAXLEN` trims the oldest entries as the stream grows, whether or not a consumer group has read them: the live suite writes twelve entries under `MAXLEN = 5` to a stream whose group has read nothing, and the group then reads five.

### The keyed sink

Each record is read strictly: one JSON object, UTF-8 throughout, no repeated member names, numbers kept as written. Its key is `key_prefix` and the primary key as text:

- a string member is its characters;
- a number is its exact plain decimal, so `1.50` names the key `1.5` and `1e3` the key `1000`, and `9007199254740993` and `9007199254740992` stay two keys;
- `true` and `false` are themselves;
- the members of a composite key are joined by the unit separator (U+001F), in `key_columns` order.

A record whose key member is missing, null, an object or an array fails the sink, as does a number whose plain form passes 4,096 characters, and, in a composite key, a string holding U+001F, which would let two rows name one key. A `__row_kind` other than `insert`, `update_after`, `delete` or `update_before` fails the sink too.

The stored value is the record as written without its `__row_kind` member: member order, number spellings and string escapes stay as they arrived. A flush nets the changelog by key, so a key's last change in the flush is the one written. The sink is not transactional: a reader can see a flush partly applied, and a key can show an earlier value while a restored job catches up to where it was.

## Delivery semantics

Both ends are at-least-once, and the keyed sink is effectively-once on the keyspace (see [the sinks](#the-sinks)). Neither sink uses two-phase commit.

Source: the consumer group's per-consumer pending-entries list (PEL) is the durable cursor. On `open()` each consumer first re-drains its PEL (id `0`), re-delivering anything delivered but not acknowledged before a crash, then switches to new entries (`>`).

`XACK` waits for the checkpoint that covers an entry to complete. `snapshot_offset()` seals the entries delivered since the last barrier as that checkpoint's, `notify_checkpoint_complete()` makes every interval at or below the completed id ready, and the source's next `produce()` turn, or its `close()`, sends the `XACK`. Every Redis call stays on the source thread; the notification, which arrives on the worker's commit-dispatch thread, only moves entries between lists (`include/clink/connectors/checkpoint_ack_ledger.hpp`). A repeated, late or out-of-order notification moves nothing twice and nothing sealed above the id it names. An aborted checkpoint's entries, and those of a checkpoint that never completes, stay pending, and the restart reads them again. So:

- Nothing in flight is lost on a crash, a cancel or a failed checkpoint.
- A checkpoint that completed but whose `XACK` had not landed when the process died is delivered again after the restart, as duplicates. A failed `XACK`, or one whose reply is lost, fails the subtask; the restart reads again exactly what is still pending.
- Completion is not commit confirmation. A job that restores from a checkpoint older than one the source acknowledged (a job with a commit-confirmed sink such as the Kafka transactional sink, whose restore selects the newest confirmed checkpoint, or an operator restoring a savepoint) does not see that checkpoint's entries again. The delivery analyser warns about the commit-confirmed combination.
- A subtask holds at most `max_pending` unacknowledged entries. A job that completes no checkpoint (no checkpointing, or a `LocalExecutor` run, where nothing dispatches completions) acknowledges nothing, so it reads `max_pending` entries and then waits, logging why.

The capability record declares the position model as `acknowledgement`, so the delivery analyser reports a job over this source as `AT_LEAST_ONCE_SOURCE`: a sink in it may declare `delivery_guarantee='at_least_once'`, and `exactly_once` is refused. Before this contract the source acknowledged at the barrier, an entry acknowledged by a checkpoint that then failed was not delivered again, and the analyser reported such a job as `AT_MOST_ONCE_SOURCE`.

### Rescaling the source

A consumer is named `<prefix>-<subtask_idx>`, and a restart reads only its own PEL, so a scale-down would leave the removed subtasks' consumers holding pending entries that no remaining subtask reads. The source does not claim them: Redis keeps no lease on a consumer, so a consumer whose subtask has gone looks the same as one whose process is still reading (a zombie worker, or another job on the same group and prefix), and claiming from a consumer that is only slow would hand its entries to two readers. Instead:

- The capability record declares a scale-down refusal, and the coordinator refuses an operator rescale of a `redis_source` to fewer subtasks, from the CLI, the HTTP API or the autoscaler, before anything drains. SQL declares no rescale bounds on sources, so a SQL job cannot rescale one at all; a C++ job can, upwards.
- A source that opens beside entries pending on a consumer outside its layout (a run at a lower parallelism than the one before, started fresh) refuses to start and names the consumers, the entries they hold and the parallelism that reads them again. Run at that parallelism, or move the entries to a consumer the job runs (`XCLAIM`) or drop them (`XGROUP DELCONSUMER`), then start again.
- A job submitted with a restore (a savepoint or an explicit checkpoint) keeps the layout it was taken at; any job's restore at another layout is refused at submit (see [fault tolerance and rescale](../internals/fault-tolerance-and-rescale.md)).

Same-parallelism restarts and scale-up are unaffected.

## Limitations

- Each sink must be the only sink on its chain: at every checkpoint barrier it writes out what it received before the barrier, which holds only while it owns its chain's checkpoint, so `Dag::add_sink` refuses another sink beside it, whichever is added first. SQL and job graphs plan every sink as a subtask of its own; with the Dag API, put the other sink in a separate Dag (see [checkpointing](../internals/checkpointing.md#guarantees-and-caveats)).
- String channel only; the record type is `std::string` for every factory.
- The Streams sink is append-only at-least-once with no two-phase commit and no producer dedup; `mode='upsert'` selects the key-value sink instead.
- `MAXLEN` trims entries a consumer group has not read.
- A sink's write is acknowledged once the server applied it in memory; the sinks wait for neither persistence nor replication.
- The keyed sink is not transactional: a reader can see a flush partly applied, and a key can show an earlier value while a restored job catches up.
- A command that outlasts `command_timeout_ms` fails the sink with the flush's outcome unknown; the restart writes the flush again.
- Redis Cluster is not supported: the connector holds one connection to one server and does not follow `MOVED` or `ASK` redirections.
- A source entry round-trips verbatim only when it carries the single configured `field` (default `v`); any other entry is rendered as a JSON object of all field/value pairs, with all values string-typed (Redis bulk strings).
- An `XDEL`-tombstoned entry that is still pending is paged past but not emitted, and acknowledged with its checkpoint.
- The source acknowledges at checkpoint completion, so a crash between a completion and its `XACK` delivers that interval again (duplicates), and a restore from a checkpoint older than one it acknowledged does not see that checkpoint's entries again (see Delivery semantics).
- A job that completes no checkpoint reads at most `max_pending` entries per subtask and then waits.
- The source cannot be rescaled to fewer subtasks; the coordinator refuses it, and a source that finds entries stranded by a lower parallelism refuses to start (see Rescaling the source).
- TLS is available only when the build finds `libhiredis_ssl` and OpenSSL (`CLINK_REDIS_TLS`); otherwise `tls=true` is rejected at runtime, and `clink connectors` reports the connector without TLS.

## Testing

The connector has in-process logic tests (`test_redis_sink_logic.cpp`, `test_redis_source_logic.cpp`, which include the rule for which consumers a lower parallelism strands), capability-record and factory tests (`test_factory_registration.cpp`: the acknowledgement model, `AT_LEAST_ONCE_SOURCE`, `exactly_once` refused, the scale-down refusal, the sinks' records and their options read strictly), server-certificate tests (`test_redis_tls.cpp`), and env-gated live integration tests (`test_redis_live.cpp`).

The sinks have their own suites. `RedisSinkRules` (`test_redis_sink_rules.cpp`) holds the key rule: strings, exact plain decimals, booleans, composite keys, and the keys refused. `RedisSinkFailures` and `RedisUpsertSinkFailures` (`test_redis_sink_failures.cpp`) drive both sinks against a scripted server: a refused command and a lost reply, each followed by a record, a barrier and a flush; reopening; a cancel during a flush and before one; a queueing failure; `close()`; the record, count, byte and linger bounds; the command timeout's default; and the keyed sink's netting, values as written, exact number keys and refusals. `RedisSinkContractLive` (`test_redis_sink_contract_live.cpp`) holds them to their contract against the server: entries exactly as written (a NUL byte, UTF-8, an empty record, 1 MiB), `MAXLEN` trimming entries a group has not read, a `WRONGTYPE` refusal and the reopen after it, a command that outlasts the timeout (the server's writes paused past it, and the flush applied once they resume), a cancel during a paused flush, a refused `AUTH` leaving no connection open, keyed values and keys exactly as written, netting and composite keys, a flush the server partly refuses under `maxmemory` (`SET` refused, `DEL` applied), and both factories writing to a selected database. `RedisUpsertSinkLive` and the shared upsert contract suite (`RedisUpsert/UpsertContractSuite`) run against the server too.

The `RedisSinkRecovery` cells (`tests/integration/test_redis_sink_recovery.cpp`) run a coordinator and two `clink_node` workers polling a feed the test serves into each sink, with worker 0 killed at `redis.sink_before_flush` (a barrier's commands built and none sent) and at `redis.sink_after_flush` (the flush applied and the barrier not acknowledged), and a SQL job at parallelism 2 whose GROUP BY counts replace by key. They judge by what Redis holds, read over a RESP connection of their own: in the Streams cells every record is in the stream exactly as written, and the records appended twice are exactly those flushed after the checkpoint the job restored (none before the flush, the parked flush's after it); in the keyed cells every key holds its last record as written and every deleted key is absent. The acknowledgement ledger the source shares with the Pub/Sub source has its own tests (`tests/test_checkpoint_ack_ledger.cpp`), and `tests/test_source_ack_hooks.cpp` runs a source acknowledging through it in an in-process cluster, with and without par-1 fusion and across a checkpoint that never completes, judging every acknowledgement against the coordinator's `COMPLETED` markers.

The certificate tests run in every TLS-enabled build without a Redis server: they connect to a local TLS listener (`tests/test_helpers/tls_handshake_server.hpp`), since the handshake completes before the client sends any command. They cover a certificate that names the address, one that names another host (refused, with the error), `tls_sni` as the name checked, wildcards, the hostname opt-out still checking the chain, and `tls_verify=false`.

The live tests are skipped unless `CLINK_REDIS_TEST_URL` is set (for example `redis://localhost:6379`). The integration service is the `redis:7` Docker image defined in `docker/integration-services.yml`. `RedisLive` covers a sink-to-source round-trip, PEL replay of an un-acked consumer on restart, that acknowledged entries are not replayed, multi-field JSON-object rendering, and idempotent `XGROUP CREATE`. `RedisSourceAckLive` holds the source to its acknowledgement contract against the server, judged by `XPENDING` and by what a restarted consumer reads: an aborted checkpoint, a crash after the barrier, a crash after completion before the `XACK`, a killed connection at the `XACK`, an `XACK` whose reply is lost, intervals and notifications out of order, the `max_pending` bound, close after a cancel, and a scale-down beside stranded entries.

`scripts/brokers-live.sh [build-dir]` is the required gate for the broker connectors (Redis, Pub/Sub, RabbitMQ, NATS, Pulsar and MQTT): it runs each broker's phases against a one-off container of its compose service, one broker at a time on free loopback ports. The sources phase runs every acknowledging source's round trip and contract; for Redis the sinks phase runs the sinks' suites above and the `RedisSinkRecovery` cells. It fails on a missing binary, a service that never answers, a failed or skipped case, a suite that ran fewer cases than it should, or, with both phases run, a case of `clink_redis_tests` that neither phase ran (the TLS cases aside). The `brokers` CI workflow runs it.

```bash
scripts/brokers-live.sh build                                         # every broker, both phases
BROKERS_LIVE_ONLY=redis BROKERS_LIVE_PHASES=sinks scripts/brokers-live.sh build
```

The TLS round-trip test additionally requires `CLINK_REDIS_TLS_TEST_URL` (set to a TLS-enabled redis `host:port`) and a build with `CLINK_REDIS_TLS`. The `-L redis` label runs the connector's test executable.
