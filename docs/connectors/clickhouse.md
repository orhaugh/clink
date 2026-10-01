# ClickHouse

> Connects to a ClickHouse server over the native (TCP) protocol. Both a source (a bounded `SELECT`) and a sink (`INSERT`).

## Overview

The ClickHouse connector integrates a ClickHouse server through the `clickhouse-cpp` native client. The sink batches incoming `std::string` records and issues `INSERT INTO db.table FORMAT ...` statements, where each record is one row encoded as either TSV or JSONEachRow. The source executes a single `SELECT` statement and drains the result blocks; it emits either typed `ClickHouseRow` records (column names plus stringified cell values) or, on the string channel, each row flattened to a delimiter-joined string or to a JSON object keyed by column name. Cell values are kept as text so the source stays schema-agnostic, with downstream operators parsing to concrete types.

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| `clickhouse-cpp` | Built from source by `scripts/build-clickhouse-cpp.sh` into `CLINK_DEPS_PREFIX/clickhouse-cpp`: static, Release whatever the build type, with TLS (OpenSSL). Homebrew's or the system's copy is the fallback, and the configure log names the version it took | `2.6.2` (`CLICKHOUSE_CPP_VERSION` in `scripts/versions.env`, checksum-pinned) |
| cityhash | Bundled with the pinned client | the client's own |
| lz4, zstd, abseil | System packages, the copies the rest of the binary links | not pinned by clink |
| OpenSSL | System; linked when found, for the client TLS path | not pinned by clink |

The sink and source path do not use Arrow; records cross the connector boundary as `std::string` (or typed `ClickHouseRow`), not Arrow batches.

## Enabling it

Controlled by the `CLINK_WITH_CLICKHOUSE` CMake option (`AUTO` / `ON` / `OFF`, default `AUTO`). Under `AUTO` the `clink::clickhouse` target is defined only if `clickhouse-cpp` is found (via `find_package(clickhouse-cpp CONFIG)` or by locating the headers and `clickhouse-cpp-lib`); with `ON` a missing client is a fatal configure error; `OFF` skips the target entirely. When the target is built, it compiles with `CLINK_HAS_CLICKHOUSE` defined; without the client the sink and source throw on construction.

The pinned client is built by `scripts/build-clickhouse-cpp.sh` on both the host and the Debian image (`scripts/install-system-deps.sh` calls it). It is built in Release even when the rest of the toolchain is Debug, because the encoding cost of a Debug client would be in every throughput figure.

```bash
cmake -S . -B build -DCLINK_WITH_CLICKHOUSE=ON
```

## Factories

| Factory name | Direction | Record type |
| --- | --- | --- |
| `clickhouse_sink` | sink | `std::string` |
| `clickhouse_row_source` | source | `ClickHouseRow` |
| `clickhouse_text_source` | source | `std::string` (columns joined by `delim`) |
| `clickhouse_source` | source | `std::string` (JSON object keyed by column name) |

The connector also registers the `ClickHouseRow` typed channel (`kChannelClickHouseRow`) with its codec so rows can travel end to end across the cluster without flattening.

## Configuration

### Sink (`clickhouse_sink`)

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `table` | yes | (none) | Target table name. Construction fails if empty. |
| `host` | no | `localhost` | ClickHouse server host. |
| `port` | no | `9000` | Native protocol port. |
| `database` | no | `default` | Target database. |
| `user` | no | `default` | Auth: username. |
| `password` | no | `` (empty) | Auth: password. |
| `format` | no | `tsv` | Row encoding: `tsv` or `jsoneachrow` (also accepts `JSONEachRow`). Any other value falls back to TSV. |
| `batch_rows` | no | `1000` | Buffered rows before a flush is forced. |
| `batch_interval_ms` | no | `1000` | Time-based flush interval in milliseconds. |

### Sources (`clickhouse_row_source`, `clickhouse_text_source`, `clickhouse_source`)

All three sources share the same option parser.

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `query` | yes | (none) | Single `SELECT` statement, no trailing semicolon. Construction fails if empty. |
| `host` | no | `localhost` | ClickHouse server host. |
| `port` | no | `9000` | Native protocol port. |
| `database` | no | `default` | Database to connect to. |
| `user` | no | `default` | Auth: username. |
| `password` | no | `` (empty) | Auth: password. |
| `batch_size` | no | `1024` | Rows emitted per `produce()` call. A single server block may straddle multiple calls. |
| `delim` | no | `\|` | `clickhouse_text_source` only: delimiter joining a row's column values into one string. Ignored by the other source factories. |

Sources expose `host`, `port`, `database`, `user`, `password`, `query` and `batch_size`; the source `Options` struct has no `table` field (the table comes from the `query`).

## SQL usage

Mapped in `src/sql/physical_plan.cpp` as `connector='clickhouse'` for both a sink and a source.

The SQL source binds to the `clickhouse_source` factory (each row arrives as a JSON object keyed by column name and is bridged to a Row table via `json_string_to_row`). The SQL sink binds to `clickhouse_sink` with `format=jsoneachrow` forced by the binding, since each Row is serialised to a JSON object before insertion.

```sql
-- Source: read a bounded SELECT into a Row table
CREATE TABLE events_in (
  id   BIGINT,
  name VARCHAR
) WITH (
  connector = 'clickhouse',
  host      = 'clickhouse.internal',
  port      = '9000',
  database  = 'analytics',
  user      = 'reader',
  password  = 'secret',
  query     = 'SELECT id, name FROM analytics.events'
);

-- Sink: INSERT rows as JSONEachRow
CREATE TABLE events_out (
  id   BIGINT,
  name VARCHAR
) WITH (
  connector = 'clickhouse',
  host      = 'clickhouse.internal',
  port      = '9000',
  database  = 'analytics',
  table     = 'events_copy',
  user      = 'writer',
  password  = 'secret',
  format    = 'json'
);
```

`mode='upsert'` and exactly-once delivery are rejected at planning time for the SQL sink.

## Example

Programmatic use through the fluent builder for the sink (`clink/api/clickhouse_builders.hpp`), which produces a `SinkDescriptor` for the `clickhouse_sink` factory:

```cpp
#include "clink/api/clickhouse_builders.hpp"

auto sink = clink::api::ClickHouseSink::builder()
                .host("clickhouse.internal")
                .port(9000)
                .database("analytics")
                .table("events")
                .user("writer")
                .password("secret")
                .format("jsoneachrow")
                .batch_rows(5000)
                .batch_interval_ms(2000)
                .build();
// `sink` is a SinkDescriptor (op_type = "clickhouse_sink", channel = "string").
```

Using the typed connector class directly:

```cpp
#include "clink/connectors/clickhouse_sink.hpp"

clink::ClickHouseSink::Options opts;
opts.host = "clickhouse.internal";
opts.table = "events";
opts.format = clink::ClickHouseSink::Format::JSONEachRow;
clink::ClickHouseSink sink(std::move(opts));
sink.open();
// sink.on_data(batch); sink.flush(); sink.close();
```

The source side is reached through the registered factories (`clickhouse_row_source`, `clickhouse_text_source`, `clickhouse_source`), each requiring a `query`.

## Delivery semantics

Sink: at-least-once. Records are buffered and flushed by row count (`batch_rows`), by time (`batch_interval_ms`), and at every checkpoint barrier (`on_barrier` flushes before the runner snapshots and acks, so no row consumed before a completed checkpoint can still be sitting in the buffer when the process dies); `close()` flushes the remaining buffer. Before that barrier flush existed (up to and including v0.8.0) a row buffered across a checkpoint was lost if the process died before the next size or time flush, which `batch_rows='1'` avoids on those versions. There is no two-phase commit and no row deduplication, so an INSERT replayed after a failure re-inserts its rows; a `ReplacingMergeTree` keyed by the row's natural key absorbs the duplicates on the ClickHouse side. The SQL planner reflects this: it rejects `exactly_once` and `mode='upsert'`.

Source: a `SELECT` materialises a finite (bounded) result set. The source persists a cursor (the row index into the materialised snapshot) and can resume mid result-set after a restart; `open()` clamps a restored cursor to the re-materialised row count. Exactly-once at the source boundary holds only for a deterministically ordered query (an explicit `ORDER BY`) over data unchanged between runs, because row index N is "the same row" only under those conditions. The SQL source binding treats it as a bounded query with no cursor checkpoint.

## Limitations

- Sink input is a single `std::string` per record, interpreted as one row (TSV or JSONEachRow). It is not a multi-column typed insert at the C++ sink layer; multi-column Rows are serialised to a JSON object string upstream (the SQL path) before the sink sees them.
- Sink batches are concatenated in memory and inserted with `client.Execute()`; there is no streaming insert, no 2PC, and no upsert/dedup. A failed flush at a barrier throws, which fails that checkpoint rather than completing it over rows ClickHouse never received.
- Source is a one-shot bounded `SELECT`, not a streaming tail or CDC feed. Result-row order is arbitrary without an explicit `ORDER BY`.
- The `clickhouse_source` (string-channel JSON) requires column names from the server; if they are absent it fails loudly rather than emit positional keys.
- The connector path is not Arrow-native; records cross the boundary as `std::string` or typed `ClickHouseRow` text values, with type coercion left to downstream operators.
- Sink unsupported in SQL with `exactly_once` or `mode='upsert'` (rejected at planning time).

## Testing

The sink and source tests are in-process smoke tests. They do not stand up a ClickHouse server; they gate on whether the build linked `clickhouse-cpp` (the server pins below are the live tests):

- If `clickhouse-cpp` is not linked, `ClickHouseSink::is_real_implementation()` / `ClickHouseSource::is_real_implementation()` return false and the real-impl tests `GTEST_SKIP()`.
- When linked, the tests exercise the constructor and the lifecycle (`open()` against an unreachable port should throw cleanly; `flush()` / `close()` before `open()` must be safe), plus factory registration and the fluent builder.

Run them with:

```bash
cmake -S . -B build -DCLINK_WITH_CLICKHOUSE=ON -DCLINK_BUILD_TESTS=ON
cmake --build build -j --target clink_clickhouse_tests
ctest --test-dir build -L clickhouse
```

To exercise against a real server, point the sink/source options (`host`, `port`, `database`, `table` / `query`, `user`, `password`) at a running ClickHouse instance and run a job manually.

### Server pins

`impls/clickhouse/tests/test_clickhouse_pins_live.cpp` holds what the native sink's design takes as given about the server, checked against a live server on every supported line. It drives the native protocol directly, with the `BeginInsert` / `SendInsertBlock` / `EndInsert` shape the sink uses, and skips unless `CLINK_CLICKHOUSE_TEST_HOST` (and optionally `CLINK_CLICKHOUSE_TEST_PORT`) names a server. `scripts/clickhouse-pins.sh` starts each line from `docker/integration-services.yml`, pinned by digest and with an embedded Keeper and a one-shard cluster, runs the suite, and tears it down; the `clickhouse-pins` CI job runs it on every push. A failing pin changes the design, not the expectation.

| Pin | What holds on 26.3 and 26.8 |
|---|---|
| P1 | SETTINGS written into the `BeginInsert` text take effect (`system.query_log` shows them) |
| P2 | A token deduplicates its own resend; identical data under a new token lands |
| P3 | Without a token, two identical, separate blocks deduplicate on ReplicatedMergeTree, so every INSERT needs a token |
| P4 | A multi-partition INSERT whose first partitions landed, resent whole under its token, lands each partition once |
| P5 | Several `SendInsertBlock` calls in one INSERT squash into one part, and the INSERT resent under its token lands once |
| P6 | A table-level `async_insert=1` overrides a query's `async_insert=0`, as does a server-wide `<merge_tree>` default, and behind a Distributed table the shard-local table's setting applies |
| P7 | With conversion off, a Nullable block is refused for a non-Nullable column; String is accepted into LowCardinality(String) |
| P8 | FINAL hides a key whose latest version is deleted, without the cleanup setting; merges never collapse a key across partitions |
| P9 | `max(ver)` is cheap: about 20 ms over ten million rows on a laptop (a measurement, not a pass mark) |
| P11 | Every setting name the sink sends exists, and `use_strict_insert_block_limits` exists from 26.8 |
| P12 | `distributed_foreground_insert=1` makes an INSERT into a Distributed table synchronous |
| P13 | `max_execution_time` with `timeout_overflow_mode='throw'` ends a SELECT with TIMEOUT_EXCEEDED |

P4 reproduces the partial landing by inserting the prefix of the same block's partitions under the token first, because a real failure lands the partitions written before it. P6's Distributed case sets `prefer_localhost_replica=0`: its shard is the same server, and with the default the Distributed table writes the local table in-process, where the shard's own decision never runs.
