# ClickHouse

> Connects to a ClickHouse server over the native (TCP) protocol. Both a source (a bounded `SELECT`) and a sink (`INSERT`).

## Overview

The ClickHouse connector integrates a ClickHouse server through the `clickhouse-cpp` native client. The sink batches incoming `std::string` records and issues `INSERT INTO db.table FORMAT ...` statements, where each record is one row encoded as either TSV or JSONEachRow. The source executes a single `SELECT` statement and drains the result blocks; it emits either typed `ClickHouseRow` records (column names plus stringified cell values) or, on the string channel, each row flattened to a delimiter-joined string or to a JSON object keyed by column name. Cell values are kept as text so the source stays schema-agnostic, with downstream operators parsing to concrete types.

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| `clickhouse-cpp` | Debian: built from source (`scripts/install-system-deps.sh`); macOS: system package via brew | `v2.5.1` on Debian; brew version not pinned by clink |
| cityhash, lz4, zstd | Sibling archives linked alongside `clickhouse-cpp` (system) | not pinned by clink |
| OpenSSL | System; linked when found, for the client TLS path | not pinned by clink |

The sink and source path do not use Arrow; records cross the connector boundary as `std::string` (or typed `ClickHouseRow`), not Arrow batches.

## Enabling it

Controlled by the `CLINK_WITH_CLICKHOUSE` CMake option (`AUTO` / `ON` / `OFF`, default `AUTO`). Under `AUTO` the `clink::clickhouse` target is defined only if `clickhouse-cpp` is found (via `find_package(clickhouse-cpp CONFIG)` or by locating the headers and `clickhouse-cpp-lib`); with `ON` a missing client is a fatal configure error; `OFF` skips the target entirely. When the target is built, it compiles with `CLINK_HAS_CLICKHOUSE` defined; without the client the sink and source throw on construction.

On Debian the client is built from source by `scripts/install-system-deps.sh` (cloned at tag `v2.5.1`); on macOS it comes from brew.

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

Sink: at-least-once. Records are buffered and flushed by row count (`batch_rows`) or time (`batch_interval_ms`), and `close()` flushes the remaining buffer. There is no two-phase commit and no row deduplication, so an INSERT replayed after a failure re-inserts its rows. The SQL planner reflects this: it rejects `exactly_once` and `mode='upsert'`.

Source: a `SELECT` materialises a finite (bounded) result set. The source persists a cursor (the row index into the materialised snapshot) and can resume mid result-set after a restart; `open()` clamps a restored cursor to the re-materialised row count. Exactly-once at the source boundary holds only for a deterministically ordered query (an explicit `ORDER BY`) over data unchanged between runs, because row index N is "the same row" only under those conditions. The SQL source binding treats it as a bounded query with no cursor checkpoint.

## Limitations

- Sink input is a single `std::string` per record, interpreted as one row (TSV or JSONEachRow). It is not a multi-column typed insert at the C++ sink layer; multi-column Rows are serialised to a JSON object string upstream (the SQL path) before the sink sees them.
- Sink batches are concatenated in memory and inserted with `client.Execute()`; there is no streaming insert, no 2PC, and no upsert/dedup.
- Source is a one-shot bounded `SELECT`, not a streaming tail or CDC feed. Result-row order is arbitrary without an explicit `ORDER BY`.
- The `clickhouse_source` (string-channel JSON) requires column names from the server; if they are absent it fails loudly rather than emit positional keys.
- The connector path is not Arrow-native; records cross the boundary as `std::string` or typed `ClickHouseRow` text values, with type coercion left to downstream operators.
- Sink unsupported in SQL with `exactly_once` or `mode='upsert'` (rejected at planning time).

## Testing

The in-tree tests are in-process smoke tests, not live-server integration tests. They do not stand up a ClickHouse server and there is no `CLINK_*_TEST_ENDPOINT` gate. Instead they gate on whether the build linked `clickhouse-cpp`:

- If `clickhouse-cpp` is not linked, `ClickHouseSink::is_real_implementation()` / `ClickHouseSource::is_real_implementation()` return false and the real-impl tests `GTEST_SKIP()`.
- When linked, the tests exercise the constructor and the lifecycle (`open()` against an unreachable port should throw cleanly; `flush()` / `close()` before `open()` must be safe), plus factory registration and the fluent builder.

Run them with:

```bash
cmake -S . -B build -DCLINK_WITH_CLICKHOUSE=ON -DCLINK_BUILD_TESTS=ON
cmake --build build -j --target clink_clickhouse_tests
ctest --test-dir build -L clickhouse
```

To exercise against a real server, point the sink/source options (`host`, `port`, `database`, `table` / `query`, `user`, `password`) at a running ClickHouse instance and run a job manually; there is no automated live test wired into the suite.
