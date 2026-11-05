# Redis Streams

> Connects to Redis Streams as both a source (`XREADGROUP` over a consumer group) and a sink (`XADD`).

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
| `redis_upsert_sink` | Sink | `Row` (SET/DEL by the primary-key-derived key) |

(Registered in `impls/redis/src/register_factories.cpp`.)

## Configuration

Options are parsed off the `BuildContext` in `register_factories.cpp`; the shared connection options come from the `ConnectOptions` struct in `redis_client.hpp`. The sink-only and source-only options come from `RedisSinkOptions` (`redis_sink.hpp`) and `RedisSourceOptions` (`redis_source.hpp`).

### Connection (both source and sink)

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `host` | No | `localhost` | Redis host. |
| `port` | No | `6379` | Redis port. |
| `username` | No | (empty) | ACL username (Redis 6+). Empty means legacy `AUTH <password>`. |
| `password` | No | (empty) | Password for `AUTH`. Empty means no authentication. |
| `db` | No | `0` | Database index; `SELECT <db>` is issued only when non-zero. |
| `tls` | No | `false` | `true` wraps the connection in TLS before `AUTH`. Requires a TLS-enabled build (`CLINK_REDIS_TLS`); otherwise rejected at runtime. |
| `tls_ca` | No | (empty) | Optional PEM path to a CA certificate for server verification. |
| `tls_cert` | No | (empty) | Optional PEM path to a client certificate for mutual TLS. |
| `tls_key` | No | (empty) | Optional PEM path to the client private key for mutual TLS. |
| `tls_sni` | No | (empty) | The name sent as SNI and the one the server's certificate must carry; defaults to `host`. |
| `tls_verify` | No | `true` | `false` skips server-certificate verification (self-signed development certs), the hostname check included. |
| `tls_verify_hostname` | No | `true` | `false` keeps the certificate-chain check but accepts a certificate that names another host. |

Authentication-related options are `username`, `password`, and the `tls_*` set. The TLS handshake runs before `AUTH`, so credentials are sent over the encrypted channel when `tls=true`.

With `tls_verify` on, the handshake checks both that the server's certificate chains to a trusted CA (`tls_ca`, or the system store when it is empty) and that it names the host connected to (`tls_sni` when set, otherwise `host`). A name is matched against the certificate's DNS subjectAltNames, or its subject CN when it has none, with a wildcard covering one whole left-most label; an IP address is matched against its IP subjectAltNames only. A mismatch fails the connect with an error that names the host, what the certificate names, and `tls_verify_hostname=false`. hiredis itself checks only the chain, so the connector builds the TLS session and asks for the name before the handshake.

### Sink (`redis_sink`)

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `stream` | Yes | (none) | Target stream key. Construction fails if empty. |
| `field` | No | `v` | Field name holding each record's payload. Must not be empty. |
| `maxlen` | No | `0` | `XADD MAXLEN` cap on stream length; `0` means unbounded. |
| `approx_maxlen` | No | `1` (true) | Non-zero uses approximate trim (`MAXLEN ~`); `0` uses exact (`MAXLEN =`). |
| `batch_records` | No | `1000` | Flush the pipelined buffer after this many records. |
| `max_bytes` | No | `0` | Flush after this many buffered payload bytes; `0` disables the byte threshold. |
| `linger_ms` | No | `0` | Flush a partial batch once it is this old, in milliseconds; `0` disables linger. |

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

Redis is mapped in `src/sql/physical_plan.cpp` to the `connector='redis'` string for both source (`redis_source`) and sink (`redis_sink`). On the SQL path the row is bridged to and from a JSON object string (`json_string_to_row` for the source, `row_to_json_string` for the sink). The default Streams sink is at-least-once; `mode='upsert'` with a PRIMARY KEY selects the changelog-aware key-value sink (`redis_upsert_sink`, see Delivery semantics) instead.

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

## Delivery semantics

Both ends are at-least-once.

Sink (`redis_sink`): `XADD` is append-only with no producer dedup key. Records are buffered and pipelined, then flushed on the count / byte / linger threshold and on every checkpoint barrier, so everything buffered is durable in the stream by the barrier. On a flush error the connection is dropped and the exception is rethrown, so the job replays the buffered batch from the last checkpoint, which re-appends and produces duplicates downstream. There is no two-phase commit.

Upsert sink (`redis_upsert_sink`, `mode='upsert'`): a separate key-value view (not Streams). Maintains one Redis key per PRIMARY KEY tuple - `SET <key_prefix><pk>` for insert/update_after, where a text key is its own characters, a number its digits, and the columns of a composite key are joined by the unit separator (0x1f), `DEL` for delete/update_before - netted by key within a flush and pipelined. Effectively-once on the keyspace for a stable PRIMARY KEY and a deterministic defining query (SET/DEL are keyed and idempotent, so a replay converges). The stored value is the row's JSON with the synthetic `__row_kind` removed. Lets a retracting query maintain a Redis key-value view.

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
- String channel only; record type is fixed to `std::string` for both factories.
- The Streams sink is append-only at-least-once with no two-phase commit and no producer dedup; `mode='upsert'` selects the key-value sink instead.
- A source entry round-trips verbatim only when it carries the single configured `field` (default `v`); any other entry is rendered as a JSON object of all field/value pairs, with all values string-typed (Redis bulk strings).
- An `XDEL`-tombstoned entry that is still pending is paged past but not emitted, and acknowledged with its checkpoint.
- The source acknowledges at checkpoint completion, so a crash between a completion and its `XACK` delivers that interval again (duplicates), and a restore from a checkpoint older than one it acknowledged does not see that checkpoint's entries again (see Delivery semantics).
- A job that completes no checkpoint reads at most `max_pending` entries per subtask and then waits.
- The source cannot be rescaled to fewer subtasks; the coordinator refuses it, and a source that finds entries stranded by a lower parallelism refuses to start (see Rescaling the source).
- TLS is available only when the build finds `libhiredis_ssl` and OpenSSL (`CLINK_REDIS_TLS`); otherwise `tls=true` is rejected at runtime, and `clink connectors` reports the connector without TLS.

## Testing

The connector has in-process logic tests (`test_redis_sink_logic.cpp`, `test_redis_source_logic.cpp`, which include the rule for which consumers a lower parallelism strands), capability-record tests (`test_factory_registration.cpp`: the acknowledgement model, `AT_LEAST_ONCE_SOURCE`, `exactly_once` refused, the scale-down refusal), server-certificate tests (`test_redis_tls.cpp`), and env-gated live integration tests (`test_redis_live.cpp`). The acknowledgement ledger the source shares with the Pub/Sub source has its own tests (`tests/test_checkpoint_ack_ledger.cpp`), and `tests/test_source_ack_hooks.cpp` runs a source acknowledging through it in an in-process cluster, with and without par-1 fusion and across a checkpoint that never completes, judging every acknowledgement against the coordinator's `COMPLETED` markers.

The certificate tests run in every TLS-enabled build without a Redis server: they connect to a local TLS listener (`tests/test_helpers/tls_handshake_server.hpp`), since the handshake completes before the client sends any command. They cover a certificate that names the address, one that names another host (refused, with the error), `tls_sni` as the name checked, wildcards, the hostname opt-out still checking the chain, and `tls_verify=false`.

The live tests are skipped unless `CLINK_REDIS_TEST_URL` is set (for example `redis://localhost:6379`). The integration service is the `redis:7` Docker image defined in `docker/integration-services.yml`. `RedisLive` covers a sink-to-source round-trip, PEL replay of an un-acked consumer on restart, that acknowledged entries are not replayed, multi-field JSON-object rendering, and idempotent `XGROUP CREATE`. `RedisSourceAckLive` holds the source to its acknowledgement contract against the server, judged by `XPENDING` and by what a restarted consumer reads: an aborted checkpoint, a crash after the barrier, a crash after completion before the `XACK`, a killed connection at the `XACK`, an `XACK` whose reply is lost, intervals and notifications out of order, the `max_pending` bound, close after a cancel, and a scale-down beside stranded entries.

`scripts/ack-sources-live.sh [build-dir]` is the required gate for every source that acknowledges at checkpoint completion (Redis Streams, Pub/Sub, RabbitMQ, NATS and Pulsar): it runs their live suites against one-off containers of the compose services, one service at a time on free loopback ports, and fails on a missing binary, a service that never answers, a failed or skipped case, or a suite that ran fewer cases than it should. The `ack-sources` CI workflow runs it.

```bash
scripts/ack-sources-live.sh build
```

The TLS round-trip test additionally requires `CLINK_REDIS_TLS_TEST_URL` (set to a TLS-enabled redis `host:port`) and a build with `CLINK_REDIS_TLS`. The `-L redis` label runs the connector's test executable.
