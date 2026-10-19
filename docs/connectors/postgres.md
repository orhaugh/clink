# PostgreSQL

> Reads from PostgreSQL via a bounded SELECT source and a logical-replication CDC source that captures a table (its existing rows, then every committed change), and writes via INSERT, upsert and two-phase-commit sinks, all over libpq.

## Overview

The PostgreSQL connector integrates a PostgreSQL server through the libpq client library. It provides two read paths and one write path. The SELECT source runs a single query at `open()` and emits the materialised result set as a finite stream. The CDC source subscribes to a logical replication slot and emits transaction boundaries plus row-level INSERT/UPDATE/DELETE changes, optionally preceded by a snapshot of the rows already in the table; through SQL it captures one table, keyed by its primary key, as a changelog or as one row per change (see [Capturing a table](#capturing-a-table-cdc)). The sink buffers records and writes them as batched multi-row INSERT statements. Records cross the connector boundary either on the typed channels (`PostgresRow`, `CdcEvent`) or as JSON-object strings keyed by column name; the SQL frontend uses the string/JSON path.

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
| `postgres_cdc_source` | Source | `std::string` (one JSON row per change: the SQL CDC contract with `changelog_mode`, the earlier flat form without) |
| `postgres_cdc_text_source` | Source | `std::string` (nested JSON line per CDC event) |
| `postgres_cdc_event_source` | Source | `CdcEvent` (typed channel) |
| `postgres_sink` | Sink | `std::string` (JSON object per row) |
| `postgres_2pc_sink` | Sink | `std::string` (JSON object per row; two-phase commit via `PREPARE TRANSACTION`) |
| `postgres_upsert_sink` | Sink | `Row` (changelog upsert/delete by PRIMARY KEY) |

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

Every option is checked when the job is built: a value outside an option's domain (`create_slot='yes'`, `snapshot_mode='always'`) is refused with the option named, rather than read as its default. Several defaults depend on `changelog_mode`, which selects the [SQL CDC contract](#capturing-a-table-cdc) and is accepted by `postgres_cdc_source` only.

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `conninfo` | Yes | none | libpq connection string or URI. The source streams over a `replication=database` connection and runs its checks and the snapshot over plain ones. Unless the string sets them, every connection uses `application_name=clink_cdc_<slot>` and the session settings `TimeZone=UTC`, `DateStyle=ISO,MDY`, `IntervalStyle=postgres` and `extra_float_digits=3`; `client_encoding` is always UTF8. A string that sets `options` replaces those session settings and must keep `DateStyle` ISO, or dates and timestamps are refused as unreadable. `env://VAR` reads the value from the environment. |
| `slot_name` | Yes | none | Logical replication slot the job owns (see [Operating the replication slot](#operating-the-replication-slot)). |
| `plugin` | No | `test_decoding`; `pgoutput` with `changelog_mode` | Logical decoding plugin: `pgoutput` (the server's built-in protocol, version 1) or `test_decoding` (text output, for tests). The SQL contract needs `pgoutput`. |
| `publication_names` | With `pgoutput` | none | Comma-separated publication name(s). |
| `table` | With `changelog_mode` or `snapshot_mode='initial'` | none | The schema-qualified table to capture. Changes to other tables of the publication are skipped. |
| `primary_key` | With `table` | the table's `PRIMARY KEY` | The table's primary key columns, from the SQL table's `PRIMARY KEY` or this option. Checked against the server at open. |
| `changelog_mode` | No | unset | `upsert` or `append`; selects the SQL CDC contract. |
| `snapshot_mode` | No | `never`; `initial` with `changelog_mode` | `initial` reads the rows already in the table before streaming its changes; `never` streams changes only. |
| `on_decode_error` | No | `drop`; `fail` with `changelog_mode` | What to do with a change that cannot be decoded, or under the contract cannot be represented. `fail` stops the job on it and never moves past it; `drop` counts it in `dropped_events_total` and continues (the change is lost). |
| `create_slot` | No | `true` | Create the slot when it does not exist and the job has no checkpoint that depends on it. A restore never recreates a missing slot. |
| `drop_slot_on_close` | No | `false` | Drop the slot when the source closes, for throwaway jobs. A job that drops its slot cannot be restored from its checkpoints. |
| `snapshot_batch_rows` | No | `1000` | Rows per snapshot fetch from a server-side cursor; also the most snapshot rows one call emits. |
| `poll_interval_ms` | No | `50` | How long the source waits for data before it returns control to the runtime. |
| `status_interval_ms` | No | `10000` | Period of the status updates that confirm the slot and ask the server to answer. A connection that has been silent for three periods (at least 30 s) is taken as lost. `0` sends only the updates that are due. |
| `connect_timeout_ms` | No | `30000` | How long one connection attempt may take. |
| `reconnect_timeout_ms` | No | `60000` | How long the source keeps trying to resume a lost replication connection before it fails the task. `0` fails at once. |

The programmatic `PostgresCdcSource::Options` also carries `proto_version` (1; strict decoding takes nothing else) and `max_batch_events` (1024, the most changes one call emits), and takes the snapshot as a list of `PostgresCdcSnapshotQuery` (a table name and a SELECT) instead of `table`.

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

## Transport security

Every factory takes a raw libpq `conninfo`, so TLS is configured the way
libpq documents it: `sslmode=require` to demand encryption,
`sslmode=verify-full` with `sslrootcert=/path/ca.pem` to demand it and
verify the server, `sslmode=disable` to state plaintext deliberately.

What clink adds is the assertion. libpq's default is `sslmode=prefer`,
which tries TLS and **silently falls back to an unencrypted connection**
when the server offers none - so a conninfo that names a certificate, or
an operator who assumed a managed database meant an encrypted link, got
plaintext with nothing said. On every connection clink now reads the
transport that was actually established (`PQsslInUse`) and:

- refuses, rather than continuing, if `sslmode=require` / `verify-ca` /
  `verify-full` was asked for and the link is not encrypted;
- logs a warning naming `sslmode` when no mode was stated and the link
  came up unencrypted - the silent-downgrade case, now stated;
- records at info that the link is encrypted when it is, so the transport
  is provable from the log rather than assumed.

Plaintext to a database on a trusted private network remains a supported
deployment. Only the silence is removed.

## SQL usage

The connector is mapped in `src/sql/physical_plan.cpp` under `connector='postgres'`. A `mode='cdc'` table property selects the CDC source (`postgres_cdc_source`); otherwise the SELECT source (`postgres_source`) is used. The sink maps to `postgres_sink`. A CDC source always runs as one subtask, whatever the job's parallelism, since a replication slot has one reader, and so does every operator behind it that is not keyed, which keeps each key's changes in order.

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

A CDC table (`mode='cdc'`) is described in [Capturing a table](#capturing-a-table-cdc) below.

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

Rows since the last checkpoint barrier are `INSERT`ed into an open transaction; at the barrier the transaction is `PREPARE`d under a deterministic global id (`clink_<uid>_g<G>_sub<N>_<ckpt>`, `G` the job's topology generation), and the framework `COMMIT PREPARED`s it once the checkpoint is globally durable (`ROLLBACK PREPARED` on abort). A prepared transaction survives the session, so a crash between prepare and commit does not lose it: on restart the sink commits any gid in the restored checkpoint state and rolls back any of its own prepared transactions that are not (their checkpoint never became durable). After a rescale each old subtask's transactions are reconciled by the one new subtask that succeeds it, and a rescale starts a new generation, so that reconcile never touches a transaction the new run is preparing under the same subtask index. Ids prepared by a release that predates generations (`clink_<uid>_sub<N>_<ckpt>`) are still reconciled. It participates in `commit_group` for atomic multi-sink commit.

Requires the server's `max_prepared_transactions > 0` (it defaults to `0`, which disables `PREPARE TRANSACTION`); a prepare against a server with it disabled fails the checkpoint loudly. Reusing the same operator uid across two jobs that write to the same server is a misconfiguration (their global ids collide).

A NaN or infinite double in a row reaches this sink through the `row_to_json_string` bridge as `null`, or as `"NaN"`, `"Infinity"` or `"-Infinity"` when the sink table sets `json_non_finite='string'`; the sink handles it as it handles any null or string value. See [NaN and infinity in JSON output](README.md#nan-and-infinity-in-json-output).

## Capturing a table (CDC)

A CDC table with `changelog_mode` set captures one PostgreSQL table: its existing rows first (`snapshot_mode='initial'`, the default under the contract), then every committed change, keyed by the table's primary key. Each value is converted to the declared column type, and a value that type cannot hold exactly stops the job (`on_decode_error='fail'`, the default under the contract) rather than reaching the sink rounded, truncated or as the wrong kind.

```sql
CREATE TABLE orders_cdc (
  id         BIGINT PRIMARY KEY,
  status     VARCHAR,
  amount     DECIMAL(12, 2),
  updated_at TIMESTAMP(3) WITH TIME ZONE
) WITH (
  connector         = 'postgres',
  format            = 'json',
  mode              = 'cdc',
  changelog_mode    = 'upsert',
  conninfo          = 'host=pg port=5432 user=cdc password=env://PGPASSWORD dbname=shop',
  slot_name         = 'clink_orders',
  publication_names = 'clink_orders',
  "table"           = 'public.orders'
);
```

### What the server must provide

| Requirement | Why | Refused at open with |
| --- | --- | --- |
| `wal_level=logical`, a free replication slot and walsender | logical decoding | the setting's name |
| A role with `REPLICATION` (or superuser), and `SELECT` on the table | streaming, and the snapshot | the `ALTER ROLE` or `GRANT` to run |
| An ordinary table (not partitioned) whose primary key is the declared key | changes are applied by that key | both keys |
| Replica identity `DEFAULT` (the primary key), `USING INDEX` on the primary key's columns, or `FULL`; not `NOTHING` | a delete must carry its key | the identity to set |
| A publication, named in `publication_names`, that contains the table and publishes inserts, updates and deletes, with no row filter and with every declared column in its column list (if it has one) | a change the publication leaves out never arrives | the publication and what it lacks |

Out-of-line (TOAST) values need `REPLICA IDENTITY FULL` in one case: an UPDATE that leaves a large `text`, `jsonb` or `bytea` value unchanged does not carry it, and only the old row that `FULL` adds to the change holds it. Under `FULL` the source takes the value from there. Without `FULL`, such an UPDATE to a declared column stops the source with a message that names the column and the `ALTER TABLE` to run; the source warns at open when a declared column can be stored out of line and the identity is not `FULL`. `FULL` also gives every delete the deleted row's values rather than its key alone.

An UPDATE that changes the primary key is emitted as a delete of the old key followed by the new row. A TRUNCATE of the table stops the source, since it cannot be expressed as row changes; leave `truncate` out of the publication's `publish` list only if the downstream copy should keep the rows a TRUNCATE removes. A declared column that is dropped or renamed on the server stops the source at the next change; there is no schema evolution.

### Supported types

| Declared SQL type | PostgreSQL types | Value |
| --- | --- | --- |
| `BIGINT` | `int2`, `int4`, `int8` | integer |
| `INTEGER` | `int2`, `int4` | integer |
| `SMALLINT` | `int2` | integer |
| `DOUBLE` | `float4`, `float8` | number; `NaN` and the infinities are refused |
| `REAL` | `float4` | number; `NaN` and the infinities are refused |
| `DECIMAL(p, s)` | `numeric`, `int2`, `int4`, `int8` | exact; a value with more than `p - s` integer digits or more than `s` fraction digits, or `NaN`, is refused |
| `BOOLEAN` | `bool` | `true` / `false` |
| `DATE` | `date` | `YYYY-MM-DD`; infinite and BC dates are refused |
| `TIMESTAMP(p)` | `timestamp` | epoch milliseconds, the wall time read as UTC |
| `TIMESTAMP(p) WITH TIME ZONE` | `timestamptz` | epoch milliseconds |
| `VARCHAR` | any type | PostgreSQL's text form, verbatim (`uuid`, `jsonb`, enums, arrays and the rest) |

The engine holds every TIMESTAMP as epoch milliseconds, so a timestamp with non-zero digits below the millisecond is refused: store `timestamp(3)` / `timestamptz(3)` in PostgreSQL, or declare the column `VARCHAR` to carry the full text. Any other declared type is refused when the job is built, and a pair outside the table at open, with the column named. NULL is carried in every type.

### Change metadata

Declared columns with these reserved names carry the change rather than the row:

| Column | Type | Value |
| --- | --- | --- |
| `__op` | `VARCHAR` | `snapshot`, `insert`, `update` or `delete` |
| `__version` | `BIGINT` | the change's WAL position as a number, `0` for a snapshot row. For any one key it increases with every change, so it versions a table that keeps the latest row per key |
| `__deleted` | `BOOLEAN` | `true` for a delete |
| `__lsn` | `VARCHAR` | the change's WAL position as text (`0/16E2A38`); for a snapshot row, the position streaming starts from |
| `__table` | `VARCHAR` | the schema-qualified table |
| `__xid` | `BIGINT` | the transaction id (`0` for a snapshot row) |

### `changelog_mode='upsert'`: a changelog keyed by the primary key

Each row carries its changelog kind: `insert` for a snapshot row or an INSERT, `update_after` for an UPDATE, `delete` for a DELETE. An update carries the new row only, never the row it replaced, so the table is a changelog only to a consumer that applies rows by that key. The binder holds that line:

- allowed between the table and the sink: projections that keep each key column (renamed or not) and WHERE conditions on key columns;
- allowed sinks: `mode='upsert'` keyed by the table's primary key, carried unchanged, or a sink that writes the changelog as it is (`connector='changelog'`, `'blackhole'`, `'print'`, or `changelog='true'`);
- refused, with the reason and `changelog_mode='append'` named: grouping, joins and every other operator, a WHERE on a column an update can change (a row moved out of the condition would never be removed), an upsert sink keyed otherwise, and an append-only sink.

```sql
CREATE TABLE orders_replica (
  id BIGINT PRIMARY KEY, status VARCHAR, amount DECIMAL(12, 2)
) WITH (connector = 'postgres', format = 'json', mode = 'upsert',
        conninfo = '...', "table" = 'public.orders_replica');

INSERT INTO orders_replica SELECT id, status, amount FROM orders_cdc;
```

### `changelog_mode='append'`: one row per change

Each change is an appended row, the deleted row's included, described by the metadata columns. Any query reads it as the stream of changes it is: `COUNT(*)` counts changes, not rows. To keep the table's current state in a store that applies rows by key, write the change rows with `__version` and `__deleted` and let the store keep the latest version per key. In ClickHouse that is a `ReplacingMergeTree(__version, __deleted)` ordered by the primary key, read with `FINAL`.

```sql
CREATE TABLE orders_changes (
  id BIGINT PRIMARY KEY, status VARCHAR, amount DECIMAL(12, 2),
  __version BIGINT, __deleted BOOLEAN
) WITH (connector = 'postgres', format = 'json', mode = 'cdc', changelog_mode = 'append',
        conninfo = '...', slot_name = 'clink_orders', publication_names = 'clink_orders',
        "table" = 'public.orders');
```

Without `changelog_mode` a `mode='cdc'` table keeps its earlier form: `test_decoding` unless `plugin` says otherwise, no snapshot, `on_decode_error='drop'`, every value as text, every table of the publication, and each change, delete included, an appended row with `__op`, `__table`, `__lsn` and `__xid`. It is kept for existing jobs; a new job should set `changelog_mode`.

## Position, snapshot and recovery

The source keeps its replication position in the checkpoint and confirms the slot only as far as a completed checkpoint, so a restore can always read the changes after its cut, and refuses to start rather than let the server skip any.

**Position.** A checkpoint records the end LSN of the last transaction whose changes were all emitted. `pgoutput` version 1 sends nothing of a transaction until it commits, and then sends it whole, in commit order, so restarting the stream at that position resends exactly the transactions not yet emitted. A checkpoint can fall inside a transaction, since the source emits at most 1024 changes per call; the checkpoint then also records that transaction's commit LSN and how many of its changes were emitted, and after a restore the source skips those changes of the resent transaction. Replay from a checkpoint is therefore exact: no change before the cut is emitted again by the source, and none after it is missed. Checkpoints written by earlier releases (the received LSN alone) are read as a position with no transaction in flight.

**Acknowledgement.** The status update's flush position, which PostgreSQL takes as the slot's `confirmed_flush_lsn`, is the position recorded by the newest checkpoint the runtime has reported complete. A checkpoint that aborts confirms nothing. Earlier releases confirmed whatever had been received, on a timer and on every keepalive, so the slot could run ahead of the last completed checkpoint, and a restore was then started by PostgreSQL at the slot's later position, silently skipping the changes in between.

**Refusals.** Before streaming starts, and again before every reconnect, the source reads the slot. It refuses when the slot does not exist although the job's checkpoint resumes in it, when it decodes with another plugin or belongs to another database, when it has lost WAL it needs (`wal_status=lost`), and when it is confirmed beyond the position the job resumes from. Each refusal names the slot and the positions; none is retried. After a restart, if the stream does not start with the transaction a checkpoint interrupted, or that transaction comes back shorter, the source refuses rather than guess. Recovering from a refusal means starting the job from scratch, with a new slot and snapshot, once the downstream copy has been reset.

**Snapshot.** When the job creates its slot, the slot exports its snapshot. The source imports it into a `REPEATABLE READ` transaction, reads the table through a server-side cursor, `snapshot_batch_rows` at a time, and then streams from the slot's consistent point. A change committed during the snapshot is not in it and arrives through the stream, so the boundary is exact. There is no such boundary when the job is restored from a checkpoint taken during the snapshot, or starts with no checkpoint on a slot that already exists (a job that died before its first checkpoint). The snapshot is then taken again from scratch in a fresh `REPEATABLE READ` transaction, and streaming starts at the slot's retained position, before that snapshot. Changes the snapshot already reflects are replayed after it; every row and change carries the whole row and is applied by key, so the downstream converges on the table's state once the stream has passed the point the snapshot was taken, though it may briefly hold an older version of a row on the way. The source does not resume a snapshot part-way.

**Lost connections.** A replication connection that drops, is ended by the server or goes silent is re-established from the source's position within `reconnect_timeout_ms`, with the same slot checks and the same skip of an interrupted transaction, and no job restart. A slot still held by the session of a consumer that has just died (its server-side session can take up to `wal_sender_timeout` to notice) is retried in the same window. Past it the task fails and the job restarts from its last checkpoint. A lost snapshot connection fails the task, and the restore takes the snapshot again.

**Cancellation.** Every wait on the server, including connecting, a slot creation waiting for open transactions to finish, a snapshot fetch and an idle stream, observes cancellation within about 100 ms. A cancelled slot creation asks the server to abandon it, so no half-made slot is left behind.

These boundaries carry named fault points (`postgres_cdc.after_slot_created`, `snapshot_batch`, `snapshot_done`, `stream_started`, `stream_batch`, `offset_recorded`, `wal_confirmed`) for the recovery tests, compiled into test builds only.

## Operating the replication slot

A slot belongs to one job. Another consumer reading from it, or `pg_replication_slot_advance` run against it, moves its confirmed position past the job's, and the job's next restore refuses. The slot must outlive restarts of the job, which is why `drop_slot_on_close` defaults to `false`.

The slot keeps every WAL segment from its confirmed position on, so WAL grows while the job is stopped, failing or not completing checkpoints: a job that runs without checkpointing never confirms the slot at all. Watch the slot, and cap what it may hold:

```sql
SELECT slot_name, active, confirmed_flush_lsn, wal_status, safe_wal_size
FROM pg_replication_slots WHERE slot_name = 'clink_orders';
-- A cap; past it the slot loses WAL (wal_status = 'lost') and the job refuses to resume.
ALTER SYSTEM SET max_slot_wal_keep_size = '10GB';
SELECT pg_reload_conf();
```

The streaming session appears in `pg_stat_replication` as `clink_cdc_<slot>`. To retire a job for good, cancel it, then drop its slot once no session holds it:

```sql
SELECT pg_drop_replication_slot('clink_orders');
```

## Example

Programmatic construction of the CDC source, based on the connector's tests:

```cpp
#include "clink/connectors/postgres_cdc_source.hpp"

clink::PostgresCdcSource::Options opts;
opts.conninfo          = "host=localhost port=5432 user=postgres password=postgres dbname=postgres";
opts.slot_name         = "clink_slot";
opts.plugin            = "pgoutput";
opts.publication_names = "clink_pub";   // required for pgoutput
opts.enable_initial_snapshot = true;    // the existing rows first
opts.initial_snapshot  = {{.table = "public.users", .query = "SELECT * FROM public.users"}};

clink::PostgresCdcSource src(std::move(opts));
src.open();   // checks the server and the slot, creates the slot if it is missing
// src.produce(emitter) emits the snapshot rows as Insert events, then the stream:
// Begin / Insert / Update / Delete / Commit. The runtime calls snapshot_offset at
// each checkpoint barrier and notify_checkpoint_complete when the checkpoint is
// durable; only then is the slot confirmed.
```

Registering the factories with a runtime registry (callers invoke this after `ensure_built_ins_registered()`):

```cpp
#include "clink/postgres/install.hpp"

clink::api::Pipeline env;
clink::postgres::install(env.registry());
// "postgres_source", "postgres_cdc_source", "postgres_sink", etc. are now resolvable,
// and the PostgresRow / CdcEvent typed channels are registered.
```

## Delivery semantics

Sink (default, `postgres_sink`): at-least-once. A replay after a failed checkpoint re-runs the buffered INSERT, appending duplicates. With `on_conflict='update'` and `conflict_columns` the INSERT is idempotent by key, which is effectively-once for keyed upserts. On a flush error the connection is dropped, the buffer cleared, and the exception propagated so the job replays from the last checkpoint. The sink flushes on every checkpoint barrier.

Sink (`mode='upsert'`, `postgres_upsert_sink`): effectively-once on the sink table for a stable PRIMARY KEY and a deterministic defining query. Applies the changelog by key (upsert on insert/update_after, DELETE on delete/update_before), netted per key within a flush and applied in one transaction; a replay re-applies keyed idempotent statements. See the changelog upsert sink section above.

Sink (`delivery_guarantee='exactly_once'`, `postgres_2pc_sink`): exactly-once via two-phase commit (`PREPARE TRANSACTION` / `COMMIT PREPARED`). Rows land iff their checkpoint completes globally; a prepared-but-uncommitted transaction survives a crash and is committed on restart, and an orphaned prepared transaction from a checkpoint that never became durable is rolled back at open. Requires `max_prepared_transactions > 0` on the server. See the exactly-once sink section above.

CDC source, stated part by part (see [Position, snapshot and recovery](#position-snapshot-and-recovery)):

- Source replay: from a checkpoint taken while streaming, exact. Every change committed after the checkpoint's cut is emitted, and none before it, including when the cut fell inside a transaction. A snapshot interrupted by a failure is taken again from scratch and converges rather than resuming.
- Duplicates: after a job restart, everything emitted since the last completed checkpoint is emitted again, so a sink without two-phase commit receives those changes twice. A snapshot taken again re-emits every row. A reconnect within `reconnect_timeout_ms` emits nothing twice.
- Transaction visibility: an aborted transaction, or a rolled-back savepoint, is never emitted; committed transactions arrive in commit order. A transaction's changes can be split across checkpoints and across the sink's batches, so a reader of the downstream copy may see part of a transaction.
- Downstream state: a consumer that applies rows by the primary key (an upsert sink, or a store that keeps the highest `__version` per key and removes `__deleted` keys) converges on the table's state once it has caught up.
- Losses: none without a refusal. A change the contract cannot carry stops the job and is met again on every restart (`on_decode_error='fail'`), and the slot is never confirmed past it. `on_decode_error='drop'` loses that change, counted in `dropped_events_total`. Without `changelog_mode` the default is still `drop`.
- End to end: the source on its own is not a delivery guarantee; the job's guarantee is the weaker of the source's replay and the sink's. Into the at-least-once ClickHouse sink the result is at-least-once delivery of every change and a correct current state through `__version`; into the PostgreSQL upsert sink it is effectively-once on the replica table.

SELECT source: the cursor is the row index into the materialised result set. A restart resumes mid result set, which is exactly-once at the source boundary only for a deterministically ordered query (ORDER BY) over data unchanged between runs; a restored cursor is clamped to the re-materialised row count.

## Limitations

- The default sink (`postgres_sink`) writes one batched multi-row INSERT per flush and is at-least-once. Exactly-once needs `delivery_guarantee='exactly_once'` (the `postgres_2pc_sink` variant) and a server with `max_prepared_transactions > 0`.
- `mode='upsert'` requires a PRIMARY KEY (the conflict target + delete key) and is effectively-once, not two-phase commit. `mode='upsert'` combined with `delivery_guarantee='exactly_once'` is rejected (pick one). For a plain append stream, `on_conflict='update'` on the default sink is the lighter idempotent-by-key option.
- The SELECT source is snapshot mode only: it runs the query once at `open()` and closes when the result set is exhausted. There is no incremental polling or `refresh_interval` behaviour (the field is reserved and ignored).
- The CDC source needs `wal_level=logical`, a role with `REPLICATION`, and `pgoutput` for the SQL contract. It captures one table per source: no multi-table transaction atomicity, partitioned tables, publication row filters or schema evolution.
- A snapshot is not resumable. A failure during the snapshot takes it again from scratch, and the downstream converges through the replay rather than seeing an exact boundary.
- Timestamps carry milliseconds; a PostgreSQL value with finer digits is refused (declare `VARCHAR` to keep the text).
- Without `REPLICA IDENTITY FULL`, an UPDATE that leaves an out-of-line value of a declared column unchanged stops the source.
- A TRUNCATE of the captured table stops the source.
- The slot is confirmed only by completed checkpoints, so WAL is retained for as long as the job is stopped or not checkpointing. A job containing a sink whose commit cannot be re-executed (the Kafka transactional sink) can be restored to a checkpoint older than one already reported complete; the CDC source then refuses, since the slot was confirmed past it.
- `on_decode_error=fail` stops the pipeline with no quarantine, and does so again on every restart for a permanently undecodable change.
- Dropping a replication slot requires that no session is using it; `drop_slot()` must be called after `close()`, or via `drop_slot_on_close=true`.

## Testing

Tests that need no server run unconditionally under the `postgres` label: the pgoutput decoder (including strict decoding of unchanged TOAST values, key changes and TRUNCATE), the SQL contract's row encoder and type conversions, the position and acknowledgement rules (`test_cdc_cursor.cpp`), the CDC factories' option checks, the replay-offset encodings, and the sink and codec units. The binder's rules for the upsert changelog and the planner's singleton placement are in `clink_sql_tests` (`SqlBinderUpsertCdc.*`, `SqlPhysical.PostgresCdc*`).

Live tests run against a real PostgreSQL server and are skipped unless `CLINK_POSTGRES_CDC_TEST_DSN` names one started with `wal_level=logical`. The reference server is the `postgres-cdc` service in `docker/integration-services.yml`, PostgreSQL 16.15 pinned by digest. The same DSN drives the sink live tests.

```bash
docker compose -f docker/integration-services.yml up -d --wait postgres-cdc
export CLINK_POSTGRES_CDC_TEST_DSN="host=localhost port=5432 user=postgres password=postgres dbname=postgres"
ctest --test-dir build -L postgres -j1
```

`PostgresCdcContractLive.*` drives the source through its checkpoint hooks, with a model of a transactional downstream that a restore rewinds, and checks every case against two oracles: the table itself, and every row version the workload committed. It covers a snapshot taken while the table changes, rolled-back work, a transaction spanning a checkpoint, a crash during snapshot setup, during the snapshot rows and at the transition to streaming, a restore after the transition, acknowledgement only by completed checkpoints (an aborted or pending one confirms nothing), a walsender ended inside a large transaction, a missing slot and a slot advanced past the checkpoint, decode-error refusal and the drop policy, the open-time refusals, cancellation from every wait, the supported types with NULLs and unchanged TOAST values, and the upsert changelog's kinds. `PostgresCdcSql.*` (`clink_postgres_sql_tests`) runs a `changelog_mode='upsert'` table through the SQL frontend into the upsert sink and compares the replica with its source.

The CDC recovery cells (`Cells/PostgresCdcClickHouseRecovery.*` in `clink_integration_tests`) run the SQL job in a cluster, a coordinator and two `clink_node` workers, capturing a table with `changelog_mode='append'` into a ClickHouse change log while a workload commits inserts, updates and deletes. A ClickHouse materialised view feeds a `ReplacingMergeTree(__version, __deleted)`. Each cell parks the source's worker at a fault point and kills it once the fault framework's witness line shows the phase was reached: during the snapshot after a checkpoint completed in it, at the transition to streaming, after a streaming checkpoint recorded its position but before it completed, and just after a completed checkpoint was confirmed to the slot. A fifth restarts the PostgreSQL server while the job streams, which the source must ride out with no job restart. After a marker row arrives through the stream, every cell requires the table read with `FINAL`, and rebuilt from the log with `argMax`, to equal the PostgreSQL table in every column; every committed row version and delete to be in the log; and exactly the restarts it names. `PostgresCdcSlot.ASlotThatLostItsWalIsRefused` gives a slot a low `max_slot_wal_keep_size` on its own server, lets the server invalidate it, and requires the restore to refuse. These start their own PostgreSQL and ClickHouse containers.

`scripts/postgres-cdc-live.sh` runs all of the above, with each binary's googletest JSON report, then `scripts/check-gtest-reports.py`, which fails the run on any skipped or failed case and on any required suite that ran fewer cases than it should: a live test skips when its server is missing, and googletest counts a skip as a pass. The `postgres-cdc` workflow (`.github/workflows/postgres-cdc.yml`) runs it on every push and pull request, against the pinned `postgres-cdc` service, and keeps the reports and the cells' evidence lines. Run the PostgreSQL live tests one process at a time: the two-phase-commit sink cases name their prepared transactions, which collide across processes on one server.

```bash
docker compose -f docker/integration-services.yml up -d --wait postgres-cdc
CLINK_POSTGRES_CDC_TEST_DSN="host=localhost port=5432 user=postgres password=postgres dbname=postgres" \
  scripts/postgres-cdc-live.sh build
```

The convenience script `scripts/sanitize_connectors.sh` networks a PostgreSQL instance against the `clink-build:latest` image and sets the DSN automatically.
