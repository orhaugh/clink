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
| `tls_sni` | No | (empty) | Overrides the SNI / verify hostname; defaults to `host`. |
| `tls_verify` | No | `true` | `false` skips server-certificate verification (self-signed development certs). |

Authentication-related options are `username`, `password`, and the `tls_*` set. The TLS handshake runs before `AUTH`, so credentials are sent over the encrypted channel when `tls=true`.

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

Upsert sink (`redis_upsert_sink`, `mode='upsert'`): a separate key-value view (not Streams). Maintains one Redis key per PRIMARY KEY tuple - `SET <key_prefix><pk>` for insert/update_after, `DEL` for delete/update_before - netted by key within a flush and pipelined. Effectively-once on the keyspace for a stable PRIMARY KEY and a deterministic defining query (SET/DEL are keyed and idempotent, so a replay converges). The stored value is the row's JSON with the synthetic `__row_kind` removed. Lets a retracting query maintain a Redis key-value view.

Source: the consumer group's per-consumer pending-entries list (PEL) is the durable cursor. On `open()` each consumer first re-drains its PEL (id `0`), re-delivering anything delivered-but-not-acked before a crash, then switches to new entries (`>`). `XACK` is the offset commit and rides `snapshot_offset()`, so entries delivered since the last checkpoint are acknowledged when the checkpoint is taken.

Honest caveat (stated in `redis_source.hpp`): `XACK` is an external side effect that is not transactional with the global checkpoint. If `snapshot_offset` acks a batch and the global checkpoint then fails, those entries are gone from the PEL and not replayed, which is at-most-once for that one batch. Strict exactly-once would need a source on-commit hook, which is deferred. A reply-level `XACK` error keeps the ids buffered so the next checkpoint retries, and the entries remain in the PEL meanwhile, preserving at-least-once either way.

## Limitations

- String channel only; record type is fixed to `std::string` for both factories.
- Sink is append-only at-least-once with no two-phase commit and no producer dedup; `mode='upsert'` is rejected on the SQL path.
- A source entry round-trips verbatim only when it carries the single configured `field` (default `v`); any other entry is rendered as a JSON object of all field/value pairs, with all values string-typed (Redis bulk strings).
- An `XDEL`-tombstoned entry that is still pending is acknowledged and paged past but not emitted.
- The `XACK`-on-checkpoint commit is not transactional with the global checkpoint, so a single batch can be at-most-once if a checkpoint fails after the ack (see Delivery semantics).
- Rescale caveat: a consumer is named `<prefix>-<subtask_idx>` and a restart re-drains only its own PEL. On a scale-down the removed subtasks' consumers are never recreated, so their un-acked (pending) entries are orphaned in the group and not redelivered. Reclaiming them would need `XAUTOCLAIM` over the dropped consumers and is not done; op-level rescale is not currently reachable from the SQL path. Same-parallelism and scale-up restarts are unaffected.
- TLS is available only when the build finds `libhiredis_ssl` and OpenSSL (`CLINK_REDIS_TLS`); otherwise `tls=true` is rejected at runtime.

## Testing

The connector has both in-process logic tests (`test_redis_sink_logic.cpp`, `test_redis_source_logic.cpp`) and an env-gated live integration test (`test_redis_live.cpp`).

The live test is skipped unless `CLINK_REDIS_TEST_URL` is set (for example `redis://localhost:6379`). The integration service is the `redis:7` Docker image defined in `docker/integration-services.yml`. It covers a sink-to-source round-trip, PEL replay of an un-acked consumer on restart, that acked entries are not replayed, multi-field JSON-object rendering, and idempotent `XGROUP CREATE`.

```bash
export CLINK_REDIS_TEST_URL=redis://localhost:6379
ctest --test-dir build -L redis -R Live --output-on-failure
```

The TLS round-trip test additionally requires `CLINK_REDIS_TLS_TEST_URL` (set to a TLS-enabled redis `host:port`) and a build with `CLINK_REDIS_TLS`. The `-L redis` label runs the connector's test executable.
