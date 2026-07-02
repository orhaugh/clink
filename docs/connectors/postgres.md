# PostgreSQL

> Reads from PostgreSQL via a bounded SELECT source and a logical-replication CDC source, and writes via a batched INSERT sink, all over libpq.

## Overview

The PostgreSQL connector integrates a PostgreSQL server through the libpq client library. It provides two read paths and one write path. The SELECT source runs a single query at `open()` and emits the materialised result set as a finite stream. The CDC source subscribes to a logical replication slot and emits transaction boundaries plus row-level INSERT/UPDATE/DELETE changes. The sink buffers records and writes them as batched multi-row INSERT statements. Records cross the connector boundary either on the typed channels (`PostgresRow`, `CdcEvent`) or as JSON-object strings keyed by column name; the SQL frontend uses the string/JSON path.

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| libpq (PostgreSQL client) | System package via apt (`libpq-dev`, Debian) / brew (macOS) | Not pinned by clink; resolved via CMake `find_package(PostgreSQL)` |

Arrow is not used by this connector; records are carried as typed C++ records or as JSON strings, not as Arrow batches.

## Enabling it

Controlled by the CMake cache option `CLINK_WITH_POSTGRES` (`AUTO` / `ON` / `OFF`, default `AUTO`). Under `AUTO` the target is built only if `find_package(PostgreSQL)` succeeds; under `ON` a missing libpq is a fatal configure error; under `OFF` the target is not defined. When built, the target compiles with `CLINK_HAS_POSTGRES` defined.

Build-env requirement: the libpq development package must be present. On the Debian image this is installed by `scripts/install-connector-deps.sh` (`libpq-dev`); on macOS it is provided by Homebrew.

```bash
cmake -S . -B build -DCLINK_WITH_POSTGRES=ON
cmake --build build -j
```

## Factories

| Factory name | Direction | Record type |
| --- | --- | --- |
| `postgres_source` | Source | `std::string` (JSON object keyed by column name) |
| `postgres_text_source` | Source | `std::string` (column values joined by `delim`) |
| `postgres_row_source` | Source | `PostgresRow` (typed channel) |
| `postgres_cdc_source` | Source | `std::string` (flat JSON per data-change event) |
| `postgres_cdc_text_source` | Source | `std::string` (nested JSON line per CDC event) |
| `postgres_cdc_event_source` | Source | `CdcEvent` (typed channel) |
| `postgres_sink` | Sink | `std::string` (JSON object per row) |

`postgres_source`, `postgres_text_source` and `postgres_row_source` are the SELECT source in three encodings. `postgres_cdc_source`, `postgres_cdc_text_source` and `postgres_cdc_event_source` are the logical-replication CDC source in three encodings. The typed `PostgresRow` and `CdcEvent` channels are registered with the registry by `install()` so pipelines can carry full row/event records without flattening to a string.

## Configuration

### SELECT source (`postgres_source`, `postgres_text_source`, `postgres_row_source`)

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `conninfo` | Yes | none | libpq connection string, e.g. `host=... port=... user=... password=... dbname=...`. Carries authentication. |
| `query` | Yes | none | Single SELECT statement, no trailing semicolon. |
| `batch_size` | No | `256` | Rows emitted per `produce()` call. |
| `delim` | No | `\|` | `postgres_text_source` only: separator used to join column values into one string. |

### CDC source (`postgres_cdc_source`, `postgres_cdc_text_source`, `postgres_cdc_event_source`)

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `conninfo` | Yes | none | libpq connection string. The source adds `replication=database` automatically. The server must run with `wal_level=logical` (verified at `open()`). |
| `slot_name` | Yes | none | Logical replication slot name. Created at `open()` if `create_slot` is true and it does not exist. |
| `plugin` | No | `test_decoding` | Logical decoding plugin: `test_decoding` (text output) or `pgoutput` (binary). |
| `publication_names` | Conditional | none | Comma-separated publication name(s). Required when `plugin=pgoutput`. |
| `create_slot` | No | `true` | Create the slot on `open()` if missing. Idempotent. |
| `drop_slot_on_close` | No | `false` | Drop the replication slot on `close()` via a fresh admin connection. Intended for ephemeral / per-job slots. |
| `on_decode_error` | No | `drop` | `drop` counts an undecodable change event and continues (at-most-once for that event); `fail` throws so the job restarts and replays from the slot. |

Additional options exist on the `PostgresCdcSource::Options` struct but are not parsed from `BuildContext` params, so they are reachable only via the programmatic API: `poll_interval` (default 50 ms), `proto_version` (pgoutput, default 1), `standby_status_interval` (default 10 s), and the snapshot-then-stream options `enable_initial_snapshot` / `initial_snapshot`.

### Sink (`postgres_sink`)

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `conninfo` | Yes | none | libpq connection string. Carries authentication. |
| `table` | Yes | none | Target table. Validated as a quoted identifier at construction. |
| `columns` | Yes (see note) | derived | Comma-separated projection. A column absent from or null in a row becomes SQL NULL. On the SQL path it defaults to the declared table schema (`schema_columns`). |
| `on_conflict` | No | `""` | `""` (plain INSERT), `update` (ON CONFLICT DO UPDATE), or `nothing` (ON CONFLICT DO NOTHING). |
| `conflict_columns` | Conditional | none | Comma-separated ON CONFLICT target. Required when `on_conflict` is `update` or `nothing`. |
| `update_columns` | No | all non-conflict | Comma-separated DO UPDATE SET list. Empty means all non-conflict columns. |
| `batch_records` | No | `1000` | Flush threshold in records. A value of 0 is clamped to 1. |
| `max_bytes` | No | `0` | Byte-based flush threshold; 0 disables it. |
| `linger_ms` | No | `0` | Maximum buffer age before flush, in milliseconds; 0 disables it. |

Note: `columns` is required at the `PostgresJsonSink` level; on the SQL Row path it is filled from the table DDL via `schema_columns`, so it need not be repeated in the WITH clause.

## SQL usage

The connector is mapped in `src/sql/physical_plan.cpp` under `connector='postgres'`. A `mode='cdc'` table property selects the CDC source (`postgres_cdc_source`); otherwise the SELECT source (`postgres_source`) is used. The sink maps to `postgres_sink`.

Bounded SELECT source:

```sql
CREATE TABLE users (
  id   BIGINT,
  name STRING
) WITH (
  connector = 'postgres',
  format    = 'json',
  conninfo  = 'host=localhost port=5432 user=postgres password=postgres dbname=postgres',
  query     = 'SELECT id, name FROM public.users'
);
```

CDC source:

```sql
CREATE TABLE user_changes (
  id   BIGINT,
  name STRING
) WITH (
  connector         = 'postgres',
  format            = 'json',
  mode              = 'cdc',
  conninfo          = 'host=localhost port=5432 user=postgres password=postgres dbname=postgres',
  slot_name         = 'clink_slot',
  plugin            = 'pgoutput',
  publication_names = 'clink_pub'
);
```

Sink:

```sql
CREATE TABLE users_out (
  id   BIGINT,
  name STRING
) WITH (
  connector        = 'postgres',
  format           = 'json',
  conninfo         = 'host=localhost port=5432 user=postgres password=postgres dbname=postgres',
  table            = 'public.users',
  on_conflict      = 'update',
  conflict_columns = 'id'
);
```

For at-least-once delivery use `on_conflict='update'` with `conflict_columns` for idempotent insert-or-update by key. For true exactly-once, set `delivery_guarantee='exactly_once'` (see below). For a retracting query (GROUP BY, TOP-N, outer join) that must maintain a table, set `mode='upsert'` with a PRIMARY KEY (see below).

### Changelog upsert sink (`mode='upsert'`)

`mode='upsert'` with a declared PRIMARY KEY selects a changelog-aware sink that maintains the table by key:

```sql
CREATE TABLE agg_sink (user_id BIGINT, total BIGINT) WITH (
  connector   = 'postgres',
  conninfo    = 'host=localhost port=5432 user=postgres password=postgres dbname=postgres',
  "table"     = 'public.totals',
  mode        = 'upsert',
  primary_key = 'user_id'
);
```

It consumes the clink changelog (`__row_kind`): insert/update_after `INSERT ... ON CONFLICT (<pk>) DO UPDATE` (upsert), delete/update_before `DELETE ... WHERE <pk> IN (...)`. Within a flush the changelog is netted by primary key (last op wins) and applied in one transaction. This is **effectively-once on the sink table** for a stable primary key and a deterministic defining query - every applied statement is keyed and idempotent, so a replay converges the table to the same final state. It is not two-phase commit; for that use `delivery_guarantee='exactly_once'`. This is what lets a retracting SQL query maintain a Postgres table (the append-only `postgres_sink` would drop the retract records).

### Exactly-once sink (`postgres_2pc_sink`)

`delivery_guarantee='exactly_once'` selects a two-phase-commit sink built on `PREPARE TRANSACTION` / `COMMIT PREPARED`:

```sql
CREATE TABLE users_sink (id BIGINT, name STRING) WITH (
  connector          = 'postgres',
  conninfo           = 'host=localhost port=5432 user=postgres password=postgres dbname=postgres',
  "table"            = 'public.users',
  delivery_guarantee = 'exactly_once'
);
```

Rows since the last checkpoint barrier are `INSERT`ed into an open transaction; at the barrier the transaction is `PREPARE`d under a deterministic global id (`clink_<uid>_sub<N>_<ckpt>`), and the framework `COMMIT PREPARED`s it once the checkpoint is globally durable (`ROLLBACK PREPARED` on abort). A prepared transaction survives the session, so a crash between prepare and commit does not lose it: on restart the sink commits any gid in the restored checkpoint state and rolls back any of its own prepared transactions that are not (their checkpoint never became durable). It participates in `commit_group` for atomic multi-sink commit.

Requires the server's `max_prepared_transactions > 0` (it defaults to `0`, which disables `PREPARE TRANSACTION`); a prepare against a server with it disabled fails the checkpoint loudly. Reusing the same operator uid across two jobs that write to the same server is a misconfiguration (their global ids collide).

## Example

Programmatic construction of the CDC source, based on the connector's tests:

```cpp
#include "clink/connectors/postgres_cdc_source.hpp"

clink::PostgresCdcSource::Options opts;
opts.conninfo          = "host=localhost port=5432 user=postgres password=postgres dbname=postgres";
opts.slot_name         = "clink_slot";
opts.plugin            = "pgoutput";
opts.publication_names = "clink_pub";   // required for pgoutput
opts.create_slot       = true;
opts.drop_slot_on_close = true;          // ephemeral slot for a per-job run

clink::PostgresCdcSource src(std::move(opts));
src.open();   // creates the slot and verifies wal_level=logical
// src.produce(emitter) emits a stream of CdcEvent (begin/commit + I/U/D rows)
```

Registering the factories with a runtime registry (callers invoke this after `ensure_built_ins_registered()`):

```cpp
#include "clink/postgres/install.hpp"

clink::api::StreamExecutionEnvironment env;
clink::postgres::install(env.registry());
// "postgres_source", "postgres_cdc_source", "postgres_sink", etc. are now resolvable,
// and the PostgresRow / CdcEvent typed channels are registered.
```

## Delivery semantics

Sink (default, `postgres_sink`): at-least-once. A replay after a failed checkpoint re-runs the buffered INSERT, appending duplicates. With `on_conflict='update'` and `conflict_columns` the INSERT is idempotent by key, which is effectively-once for keyed upserts. On a flush error the connection is dropped, the buffer cleared, and the exception propagated so the job replays from the last checkpoint. The sink flushes on every checkpoint barrier.

Sink (`mode='upsert'`, `postgres_upsert_sink`): effectively-once on the sink table for a stable PRIMARY KEY and a deterministic defining query. Applies the changelog by key (upsert on insert/update_after, DELETE on delete/update_before), netted per key within a flush and applied in one transaction; a replay re-applies keyed idempotent statements. See the changelog upsert sink section above.

Sink (`delivery_guarantee='exactly_once'`, `postgres_2pc_sink`): exactly-once via two-phase commit (`PREPARE TRANSACTION` / `COMMIT PREPARED`). Rows land iff their checkpoint completes globally; a prepared-but-uncommitted transaction survives a crash and is committed on restart, and an orphaned prepared transaction from a checkpoint that never became durable is rolled back at open. Requires `max_prepared_transactions > 0` on the server. See the exactly-once sink section above.

CDC source: the cursor is the received WAL LSN, persisted through `snapshot_offset` / `restore_offset`. A restart resumes `START_REPLICATION` from the checkpointed LSN, which is exactly-once at the source boundary for the decodable change stream, provided the slot still retains WAL from that LSN. Records between the last checkpoint and a crash are replayed and must be reconciled downstream. A change event that the decoder cannot decode is dropped (the checkpointed LSN advances past it) unless `on_decode_error=fail`, so undecodable events are at-most-once under the default policy. Because the cursor is the received WAL LSN rather than the last decoded commit LSN, a resume can re-read from the start of an in-flight transaction.

SELECT source: the cursor is the row index into the materialised result set. A restart resumes mid result set, which is exactly-once at the source boundary only for a deterministically ordered query (ORDER BY) over data unchanged between runs; a restored cursor is clamped to the re-materialised row count.

## Limitations

- The default sink (`postgres_sink`) writes one batched multi-row INSERT per flush and is at-least-once. Exactly-once needs `delivery_guarantee='exactly_once'` (the `postgres_2pc_sink` variant) and a server with `max_prepared_transactions > 0`.
- `mode='upsert'` requires a PRIMARY KEY (the conflict target + delete key) and is effectively-once, not two-phase commit. `mode='upsert'` combined with `delivery_guarantee='exactly_once'` is rejected (pick one). For a plain append stream, `on_conflict='update'` on the default sink is the lighter idempotent-by-key option.
- The SELECT source is snapshot mode only: it runs the query once at `open()` and closes when the result set is exhausted. There is no incremental polling or `refresh_interval` behaviour (the field is reserved and ignored).
- The CDC source requires the server to run with `wal_level=logical`, verified at `open()`; the connection is forced to `replication=database`.
- `pgoutput` requires `publication_names`; construction throws otherwise.
- The CDC default `on_decode_error=drop` is at-most-once for any change event the decoder cannot decode; `on_decode_error=fail` halts the pipeline with no backoff or poison-quarantine and re-throws on every restart for a permanently undecodable event.
- Dropping a replication slot requires that no session is using it; `drop_slot()` must be called after `close()`, or via `drop_slot_on_close=true`.
- Several CDC options (poll interval, pgoutput protocol version, standby status interval, initial-snapshot configuration) are not exposed through `BuildContext` params and are reachable only via the programmatic `Options` struct.

## Testing

Live integration tests run against a real PostgreSQL server and are skipped unless `CLINK_POSTGRES_CDC_TEST_DSN` is set to a libpq DSN, for example `host=localhost port=5432 user=postgres password=postgres dbname=postgres`. The reference server is the `postgres:16` service in `docker/integration-services.yml`, started with `wal_level=logical` for the CDC tests. The same DSN drives the sink live tests (a plain conninfo is sufficient for INSERT/SELECT).

```bash
export CLINK_POSTGRES_CDC_TEST_DSN="host=localhost port=5432 user=postgres password=postgres dbname=postgres"
ctest --test-dir build -L postgres
```

Tests that do not require a live server (decoder, factory registration, codecs, replay-offset logic) run unconditionally under the same `postgres` label. The convenience script `scripts/sanitize_connectors.sh` networks a `postgres:16` instance against the `clink-build:latest` image and sets the DSN automatically.
