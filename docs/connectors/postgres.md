# PostgreSQL

> Reads from PostgreSQL via a bounded SELECT source and a logical-replication CDC source that captures a table (its existing rows, then every committed change), and writes via INSERT, upsert and two-phase-commit sinks, all over libpq.

## Overview

The PostgreSQL connector integrates a PostgreSQL server through the libpq client library. It provides two read paths and four write paths. The SELECT source runs a single query at `open()` and emits the materialised result set as a finite stream. The CDC source subscribes to a logical replication slot and emits transaction boundaries plus row-level INSERT/UPDATE/DELETE changes, optionally preceded by a snapshot of the rows already in the table; through SQL it captures one table, keyed by its primary key, as a changelog or as one row per change (see [Capturing a table](#capturing-a-table-cdc)). The at-least-once sink writes records as batched multi-row INSERT statements, optionally `ON CONFLICT` by key; the changelog sink maintains a table by its key; the two-phase-commit sink commits each checkpoint's rows at completion; and, from C++, `PostgresSink` binds rows into a parameterised statement (see [The sinks](#the-sinks)). Records cross the connector boundary either on the typed channels (`PostgresRow`, `CdcEvent`) or as JSON-object strings keyed by column name; the SQL frontend uses the string/JSON path.

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| libpq (PostgreSQL client) | System package via apt (`libpq-dev`, Debian) / brew (macOS) | Not pinned by clink; resolved via CMake `find_package(PostgreSQL)` |

Arrow is not used by this connector; records are carried as typed C++ records or as JSON strings, not as Arrow batches.

## Enabling it

Controlled by the CMake cache option `CLINK_WITH_POSTGRES` (`AUTO` / `ON` / `OFF`, default `AUTO`). Under `AUTO` the target is built only if `find_package(PostgreSQL)` succeeds; under `ON` a missing libpq is a fatal configure error; under `OFF` the target is not defined. When built, the target compiles with `CLINK_HAS_POSTGRES` defined.

Build-env requirement: the libpq development package must be present. On the Debian image this is installed by `scripts/install-connector-deps.sh` (`libpq-dev`); on macOS it is provided by Homebrew.

The prebuilt Linux SDK tarball on each release includes this connector, so its binaries need libpq at run time: on Ubuntu 24.04, `apt-get install libpq5`. A C++ project whose `find_package(clink)` against the tarball names no components resolves this connector's dependency as well, so it needs `libpq-dev` to configure; naming components without `postgres`, as `find_package(clink REQUIRED COMPONENTS core)` does, avoids both. The runtime image carries libpq already.

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
| `postgres_upsert_sink` | Sink | `std::string` (JSON object per change; `__row_kind` marks a delete) |

`postgres_source`, `postgres_text_source` and `postgres_row_source` are the SELECT source in three encodings. `postgres_cdc_source`, `postgres_cdc_text_source` and `postgres_cdc_event_source` are the logical-replication CDC source in three encodings. The typed `PostgresRow` and `CdcEvent` channels are registered with the registry by `install()` so pipelines can carry full row/event records without flattening to a string.

## Configuration

### SELECT source (`postgres_source`, `postgres_text_source`, `postgres_row_source`)

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `conninfo` | Yes | none | libpq connection string, e.g. `host=... port=... user=... password=... dbname=...`. Carries authentication. |
| `query` | Yes | none | Single SELECT statement, no trailing semicolon. |
| `batch_size` | No | `256` | Rows emitted per `produce()` call, 1 to 1,000,000; any other value is refused when the job is built. |
| `delim` | No | `\|` | `postgres_text_source` only: separator used to join column values into one string. |

Only subtask 0 runs the query, whatever the job's parallelism: one result does not divide between subtasks, so the others read nothing.

The source holds its whole result in memory, since libpq reads a query's result whole, and once the query is sent it runs to the end before the task's cancel is seen. A restore runs the query again: see [Delivery semantics](#delivery-semantics).

### CDC source (`postgres_cdc_source`, `postgres_cdc_text_source`, `postgres_cdc_event_source`)

A replication slot has one reader, so only subtask 0 streams and other subtasks are dormant. SQL plans the source on one subtask in any case.

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
| `keep_rows` | No | `false` | With `changelog_mode` only. `true` keeps the latest row of every key in the job's state, behind the source, which fills in unchanged out-of-line values, turns a TRUNCATE into deletes, and gives an upsert table's updates and deletes their old rows; see [Keeping the rows](#keeping-the-rows-keep_rows). |
| `on_sub_millisecond` | No | `fail` | With `changelog_mode` only. What a `TIMESTAMP` value with non-zero digits below the millisecond does: `fail` refuses it, as any value its type cannot hold; `truncate` drops those digits, rounding the value down to the millisecond, counts it in `clink_connector_timestamps_truncated_total{connector="postgres_cdc"}` and names the column in the log the first time. A primary key column is never truncated, since two keys could become one. |
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
| `conninfo` | Yes | none | libpq connection string. Carries authentication. Its own settings override the sink's connection defaults (a 10-second connect timeout, TCP keepalives and a 60-second `tcp_user_timeout`). |
| `table` | Yes | none | Target table, `name` or `schema.name`. Validated as a quoted identifier at construction. |
| `columns` | Yes (see note) | derived | Comma-separated projection. A column absent from or null in a row becomes SQL NULL. On the SQL path it defaults to the declared table schema (`schema_columns`). |
| `on_conflict` | No | `""` | `""` (plain INSERT), `update` (ON CONFLICT DO UPDATE), or `nothing` (ON CONFLICT DO NOTHING). Any other value is refused. |
| `conflict_columns` | Conditional | none | The ON CONFLICT target, required with `on_conflict`; each must be one of `columns`. |
| `update_columns` | No | all non-conflict | The DO UPDATE SET list; empty means every column outside the conflict target. Taken only with `on_conflict='update'`, and each must be one of `columns`. |
| `batch_records` | No | `1000` | Flush at this many buffered rows, 1 to 1000000. |
| `max_bytes` | No | `16777216` | Flush once the buffered rows reach this many bytes of SQL text, 0 to 1073741824; `0` means the default, 16 MiB. |
| `max_record_bytes` | No | `1048576` | The most one record's row may take as SQL text, 64 up to `max_statement_bytes`. A larger record fails the sink. |
| `max_statement_bytes` | No | `4194304` | The most one statement may take, 1024 to 1073741824. |
| `linger_ms` | No | `0` | Flush rows buffered this long, checked as each record arrives; 0 (off) to 3600000. |
| `lock_wait_timeout_s` | No | `30` | The session's `lock_timeout`, 1 to 3600 seconds; its `statement_timeout` is a minute longer. |
| `max_retries` | No | `3` | How many times a flush that met a deadlock, a serialization failure or a lock wait timeout is tried again, 0 to 10. |
| `timestamp_columns` | No | derived | Comma-separated `<column>:ts` or `<column>:tstz`: the columns written as timestamps (see [Values](#values)). On the SQL path the planner names every `TIMESTAMP` column. |

Every option is read strictly: a value the sink cannot use (a number with trailing text, a value out of range, an `on_conflict` other than `update` or `nothing`) is refused when the job is built, never replaced by the default.

Note: `columns` is required at the `PostgresJsonSink` level; on the SQL Row path it is filled from the table DDL via `schema_columns`, so it need not be repeated in the WITH clause.

### Changelog upsert sink (`postgres_upsert_sink`)

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `conninfo`, `table`, `columns` | As for `postgres_sink` | | |
| `key_columns` | Yes | none | The row's identity: the columns of the table's PRIMARY KEY or UNIQUE index. On the SQL path, the table's PRIMARY KEY. |
| `batch_records` | No | `1000` | Flush at this many buffered keys, 1 to 1000000. A key's later change replaces its earlier one in the buffer. |
| `max_record_bytes` | No | `1048576` | The most one record's row, or its key for a delete, may take as SQL text. |
| `max_bytes`, `max_statement_bytes`, `lock_wait_timeout_s`, `max_retries`, `timestamp_columns` | No | as for `postgres_sink` | As for `postgres_sink`. |

The upsert sink sets every non-key column, takes no `linger_ms`, and reads its options as strictly.

### C++ `PostgresSink`

`clink::PostgresSink` (`clink/connectors/postgres_sink.hpp`) binds each record, a `std::vector<std::string>`, into a parameterised statement of the caller's (`INSERT INTO t (a, b) VALUES ($1, $2)`), every value as text, never NULL. It writes at `batch_rows` rows, after `batch_interval` and at every checkpoint barrier, each flush one transaction of one statement per row, and takes `lock_wait_timeout` and `max_retries` as the JSON sinks do. It checks no table, since the statement is the caller's. It has no factory.

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

## The sinks

The at-least-once and upsert sinks, and the C++ `PostgresSink`, share one core, `impls/postgres/src/pg_sink_core.cpp`: the session, the table check, each flush's transaction, the failure state, retries and cancellation. Values, keys and statements in PostgreSQL's dialect are in `impls/postgres/src/pg_sink_rules.cpp`; the JSON reader, stream keys, the changelog marker, statement packing and the changelog buffer are the record rules the SQL sinks share (`include/clink/connectors/sink_record_rules.hpp`). The two-phase-commit sink keeps its own path (see [the exactly-once sink](#exactly-once-sink-postgres_2pc_sink)).

### Modes

| Mode | Selected by | Statements | Delivery |
| --- | --- | --- | --- |
| Append | `postgres_sink` | `INSERT INTO t (...) VALUES (...),(...)` | At-least-once: a replay appends its rows again. |
| Insert-or-update | `postgres_sink` with `on_conflict='update'` | `INSERT ... ON CONFLICT (k) DO UPDATE SET c = EXCLUDED.c, ...` | At-least-once; a replay rewrites rows by key. |
| Insert-or-skip | `postgres_sink` with `on_conflict='nothing'` | `INSERT ... ON CONFLICT (k) DO NOTHING` | At-least-once; a replay finds its rows there and leaves them. |
| Changelog upsert | `postgres_upsert_sink`, or SQL `mode='upsert'` | the insert-or-update for `insert` and `update_after`, and `DELETE FROM t WHERE k IN (...)` for `delete` and `update_before` | Effectively-once on the table, for a stable key, a deterministic query and one writer per key. |

A record is one JSON object. A sink writes the members named in `columns`; a member the record lacks is written as NULL, as a null member is, so a column's default does not apply. In the changelog sink a `__row_kind` of `insert` or `update_after`, or none, upserts the row, and `delete` or `update_before` deletes it by key; any other value, or one that is not a string, fails the sink. The append sink refuses `delete` and `update_before` records, which it cannot apply: appended, a retraction would add the row it removes. In the keyed modes a record whose key member is missing or null fails the sink: a NULL never conflicts, so a replay would insert the row again.

### The table each mode needs

When a sink opens, it reads the table's definition from `pg_catalog` and refuses a table its mode cannot write as declared, naming every problem:

- Every mode: the table exists, every column in `columns` exists, is not a generated column and is not `GENERATED ALWAYS AS IDENTITY`, and a column in `timestamp_columns` is `timestamp` or `timestamptz`.
- The keyed modes (`on_conflict` and the changelog sink): a table or partitioned table, not a view or a foreign table, with a PRIMARY KEY or UNIQUE index on exactly the key columns, over whole values (not an expression) and every row (not a partial index), which `ON CONFLICT` needs to write by.
- Append: a table, or a view PostgreSQL can insert through.

Another unique index on the table is PostgreSQL's to enforce, unlike MySQL's `ON DUPLICATE KEY UPDATE`: a row that collides on it fails its flush with a unique violation, and the replay fails the same way until the conflict is resolved.

### Keys and order

The at-least-once sink writes its rows in the order they arrived, and in the keyed modes never puts two rows of one key in a statement, which `ON CONFLICT DO UPDATE` refuses: a key's later row goes in a later statement, so the row ends as applying each record in order leaves it.

The changelog sink buffers changes by the key as the stream writes it: the key members' JSON kinds and texts, with a number taken by its value, so `1`, `1.0` and `1.00` are one key. Within a flush a key's later change replaces its earlier one, and the surviving changes are applied in the order in which each key last changed: consecutive upserts as one `INSERT`, consecutive deletes as one `DELETE`, the runs in order, all in one transaction.

Which keys are one row is the table's to decide: two keys the stream holds apart and the table treats as one (`'abc'` and `'ABC'` under a nondeterministic collation, `'a'` and `'a '` in a `char(n)` column, `1` and `"1"` in an `integer` column) reach the table in the order they changed. When two of them fall in one `INSERT`, PostgreSQL refuses it (SQLSTATE 21000, a command that would change one row twice), and the sink rolls the flush back and writes it again a row to a statement, in the same order, so the row ends as the last change left it. The live suite checks this against a reference table to which PostgreSQL applies each change in turn.

That holds within one subtask. With more than one, two changes to a row reach the table in either order unless they reach the same subtask, so a parallel changelog sink needs key columns that PostgreSQL tells apart exactly as the stream does: an integer type, `numeric`, `text` or `varchar` under a deterministic collation, or a `timestamp` or `timestamptz` of at least millisecond precision written from epoch milliseconds. A table with any other key column (`char(n)`, a nondeterministic collation, `uuid`, `date`) is refused when the sink opens. Each record's key must also be written as its column compares, a number for a numeric or timestamp column and a string for a `text` or `varchar` one, or the record fails the sink. The SQL planner routes a sink's rows by the key's typed value; with the Dag API, partition the stream by the value the table keys by.

The sinks assume they own the rows they write. One writer per key is a condition of the changelog sink's convergence: a replay rewrites the rows its checkpoint did not cover, and can overwrite a change another writer made to them since. A row another writer inserted is updated by the sink's upsert, not duplicated; a row another writer deleted is inserted again by the sink's next upsert of its key.

### Values

Records are read strictly: one JSON object, valid UTF-8, with no member repeated at the top level and at most 256 levels of nesting; anything else fails the sink. Each value is written as a PostgreSQL literal:

| JSON value | Written as |
| --- | --- |
| `null`, or a missing member | `NULL` |
| `true`, `false` | `TRUE`, `FALSE` |
| A string | a quoted literal escaped by libpq for the connection; refused when it holds U+0000, which PostgreSQL text cannot hold |
| A string, for a `bytea` column | its UTF-8 bytes, U+0000 included, in hex |
| An object or array | its JSON text, quoted, for a `json` or `jsonb` column |
| A number | exactly: its plain decimal up to 4096 characters, else the token itself, which PostgreSQL reads as an exact `numeric` too |
| A number, for a `real` or `double precision` column | the number as written, which the server reads as the nearest value of the type; one past a `double precision`'s range fails the sink |
| A number, for a column in `timestamp_columns` | its UTC time, from epoch milliseconds |

PostgreSQL rounds some values without a word, so the sink refuses them before they are buffered: a number with a fraction for an `integer`, `bigint` or `smallint` column; a number, or a string holding one, with more digits after the point than a `numeric(p, s)` column keeps (or, from PostgreSQL 15, not a multiple of the power of ten a negative scale keeps); and a string longer than a `varchar(n)` or `char(n)` column holds, counted in characters, which PostgreSQL would otherwise cut when the excess is spaces. Everything else a column cannot hold (a number out of its range, a string that is not a date, `NULL` for a `NOT NULL` column, a failed check constraint) the server refuses, and the flush fails.

**Timestamps.** The SQL Row path carries a `TIMESTAMP` as epoch milliseconds, and the sink writes a column named in `timestamp_columns` as a timestamp literal instead of that number: a `TIMESTAMP` as its time in UTC (`'2026-10-24 17:41:28.105'`), for a `timestamp` column, and a `TIMESTAMP WITH TIME ZONE` as that instant (`'2026-10-24 17:41:28.105+00'`), so a `timestamptz` column stores the same instant whatever the session's time zone. A value that is not a whole number of milliseconds, that lies outside years 1 to 9999, or whose milliseconds a `timestamp(0)`, `(1)` or `(2)` column would round away fails the sink. To store epoch milliseconds in a `bigint` column, declare the SQL column `BIGINT`. A string written to a `timestamp` or `date` column is PostgreSQL's to parse, and it rounds fractional seconds to the column's precision.

**The session.** Each connection runs with `TIME ZONE 'UTC'`, `client_encoding 'UTF8'` and `standard_conforming_strings on`, whatever the server's, the database's or the role's settings, so a database set to another time zone or to `standard_conforming_strings off` stores the same values. Its `lock_timeout` is `lock_wait_timeout_s` and its `statement_timeout` a minute longer.

### Transactions, failures and acknowledgement

A flush is one transaction: `BEGIN`, its statements in order, `COMMIT`. A sink flushes at its count, byte and linger thresholds and at every checkpoint barrier, and passes the barrier on, so acknowledging the checkpoint, only after that flush has committed. A checkpoint therefore completes only once every row the sink received before it is committed, and a restored job writes the rows after its checkpoint again.

A failure fails the sink. A failed sink refuses every later record, barrier and flush, an empty flush included, until it is opened again, which the job's restart does before it replays from its last checkpoint:

| Failure | What the table holds | What the error says |
| --- | --- | --- |
| A record that cannot be read or written as declared, or one too large for a statement | Nothing of the record: it fails the sink before it is buffered | Which member, and why, without its value |
| A statement the server refuses | Nothing of the flush, which is rolled back | The SQLSTATE and the server's primary message, with the values it quotes replaced by `<value>`; never its DETAIL, which can quote a whole row |
| A deadlock (40P01), a serialization failure (40001) or a lock wait past `lock_timeout` (55P03) | Nothing: the transaction is rolled back and tried again up to `max_retries` times, after 100 ms, then twice as long each time, up to 5 s; then the sink fails | The last error and the retries spent |
| Two keys of one table row in one upsert (21000) | Nothing of the first try: the flush is written again a row to a statement | Nothing, unless the second try fails too |
| The connection lost before `COMMIT` | Nothing of the flush | That nothing was committed |
| `COMMIT` refused by the server, or answered `ROLLBACK` | Nothing of the flush | That nothing was committed |
| `COMMIT` sent and its reply lost | Unknown: the flush may or may not have been committed | That the outcome is unknown; the job replays, so a committed flush is written twice by the append sink and rewritten by key otherwise |
| The task cancelled | The statement in flight is cancelled through libpq's cancel request and the transaction rolled back; a cancel during `COMMIT` is the unknown outcome above | That the task was cancelled |

A statement waits for a lock for at most `lock_wait_timeout_s`, and runs for at most a minute longer, after which the server cancels it; the connection's TCP keepalives and send timeout end a statement whose server has gone away. A cancel ends a statement in flight, or a wait between tries, within about 20 ms.

`close()` never throws: it discards what is buffered (a restored job writes it again), rolls back a transaction left open and releases the connection. A clean end of input flushes before `close()`; a cancelled task does not. Neither sink is two-phase commit, and neither is exactly-once; for that, use the [exactly-once sink](#exactly-once-sink-postgres_2pc_sink).

### Bounds

| Bound | Option | Default |
| --- | --- | --- |
| Rows buffered (`postgres_sink`), or keys buffered (changelog) | `batch_records` | 1000 |
| SQL text buffered | `max_bytes` | 16 MiB |
| One record's row or key, as SQL text | `max_record_bytes` | 1 MiB |
| One statement | `max_statement_bytes` | 4 MiB |
| One wait for a lock | `lock_wait_timeout_s` | 30 s |
| One statement's run | `lock_wait_timeout_s` plus 60 s | 90 s |
| Tries after a deadlock, a serialization failure or a lock wait | `max_retries` | 3 |

The thresholds are checked as each record is buffered, so a large batch flushes part-way through. The byte bounds count the SQL text the records render to, escapes included, which is what the server receives; they are not a bound on the sink's memory, which also holds each buffered key's bookkeeping in the changelog sink. A flush longer than one statement is split into several, all in its one transaction, and a record longer than one statement can hold fails the sink before it is buffered.

### Changes from earlier releases

The at-least-once and upsert sinks, and `PostgresSink`, were rewritten in this release. Against earlier releases:

- A failed flush fails the sink until it is opened again. Earlier releases cleared the rows they had buffered and let a later empty flush, and so a checkpoint, succeed; `PostgresSink` could also run the next flush inside a transaction its failed `COMMIT` had left.
- `close()` discards what is buffered. Earlier releases flushed it, even when the task was cancelled (`PostgresSink` swallowing the error).
- The changelog sink applies a flush's changes in the order each key last changed, and writes a flush again a row to a statement when two of its keys are one table row. Earlier releases applied every upsert before every delete, and keyed the buffer by the key's JSON text, so `1` and `1.0` were two keys in one statement, which PostgreSQL refused and the replay refused again.
- An unknown `__row_kind`, or one that is not a string, fails the changelog sink, where earlier releases upserted the record; the append sink refuses retractions, which earlier releases appended; a keyed sink refuses a null key.
- The sinks check the table when they open: missing, generated and identity columns, and for the keyed modes a missing, partial or expression unique index, which earlier releases met as an error at the first flush, and a view. A parallel changelog sink refuses key columns PostgreSQL does not tell apart as the stream does.
- Numbers are written exactly. Earlier releases wrote a decimal through a double, losing digits past the seventeenth, and could not read an integer outside the signed 64-bit range. A value PostgreSQL would round or cut is refused (see [Values](#values)).
- The session is UTC, UTF8 and `standard_conforming_strings on`; earlier releases took the server's settings.
- A deadlock, serialization failure or lock wait is tried again, up to `max_retries` times, where earlier releases failed the flush; a statement's lock wait and run are bounded.
- Options are read strictly: an `on_conflict` other than `update` or `nothing`, `update_columns` without `on_conflict='update'`, a conflict column outside `columns`, and a number out of range are refused. `batch_records` must be at least 1, where earlier releases took `0` as 1, and `max_bytes='0'` means the 16 MiB default, where earlier releases took it as no byte threshold. `max_bytes` counts SQL text rather than record bytes.
- Errors no longer include records, SQL or the server's DETAIL. Earlier releases put the whole statement, and so every buffered record, in the upsert sink's message.
- A SQL sink table takes only the options its sink reads (see [SQL usage](#sql-usage)).

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

It consumes the clink changelog (`__row_kind`): insert/update_after `INSERT ... ON CONFLICT (<pk>) DO UPDATE` (upsert), delete/update_before `DELETE ... WHERE <pk> IN (...)`. Within a flush a key's later change replaces its earlier one, and the changes are applied in the order each key last changed, in one transaction (see [Keys and order](#keys-and-order)). This is effectively-once on the sink table for a stable primary key, a deterministic defining query and one writer per key: a replay converges the table to the same rows. It is not two-phase commit; for that use `delivery_guarantee='exactly_once'`. This is what lets a retracting SQL query maintain a PostgreSQL table; a changelog into a table without `mode='upsert'` is refused when it is planned.

An at-least-once or upsert sink table takes only the options its sink reads: `conninfo`, the sink's options above, the source options (a table can be read and written), and the options the planner interprets (`connector`, `format`, `mode`, `primary_key` and the like). The planner sets `key_columns` from the PRIMARY KEY and `timestamp_columns` from the `TIMESTAMP` columns, so a table that sets either is refused, as is one with `mode='upsert'` that sets `on_conflict`, `conflict_columns`, `update_columns` or `linger_ms`, and one that sets any other option. The exactly-once sink's table keeps its own options (`commit_group`).

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

Rows since the last checkpoint barrier are `INSERT`ed into an open transaction; at the barrier the transaction is `PREPARE`d under a deterministic global id (`clink_<uid>_g<G>_sub<N>_<ckpt>`, `G` the job's topology generation), and the framework `COMMIT PREPARED`s it once the checkpoint is globally durable (`ROLLBACK PREPARED` on abort). A prepared transaction survives the session, so a crash between prepare and commit does not lose it: on restart the sink commits any gid in the restored checkpoint state and rolls back any of its own prepared transactions that are not. No restore chooses those checkpoints again: one at or below the restore point is behind its marker, and one above it behind the job's restore record, which the coordinator puts before the deploy and a takeover reads before it picks a marker (see [the takeover](../internals/distributed-runtime.md)). Before the record, a `COMPLETED` marker whose write was still out when the job restored from below it landed afterwards, a later takeover restored from it, and the transaction this open had rolled back was gone, with the source resumed past its interval. After a rescale each old subtask's transactions are reconciled by the one new subtask that succeeds it, and a rescale starts a new generation, so that reconcile never touches a transaction the new run is preparing under the same subtask index. Ids prepared by a release that predates generations (`clink_<uid>_sub<N>_<ckpt>`) are still reconciled. Setting `commit_group` makes a failed acknowledgement abort the group at once; it does not make the commits of several sinks atomic, which no option does (see [the commit phase and commit groups](../internals/checkpointing.md#the-commit-phase-and-commit-groups-cluster-path)).

Each subtask holds two connections: one carries the interval's transaction (`BEGIN` at its first flush, the `INSERT`s, `PREPARE` at the barrier) and the other finalises prepared transactions (`COMMIT PREPARED`, `ROLLBACK PREPARED`, the `pg_prepared_xacts` lookups), so the server must allow two connections for every subtask of the sink. They cannot be one: once a write fills `batch_records` or `max_bytes` before the barrier, the next interval's transaction is open, and the previous checkpoint's commit arrives while it is, which PostgreSQL refuses inside a transaction block.

Requires the server's `max_prepared_transactions > 0` (it defaults to `0`, which disables `PREPARE TRANSACTION`); a prepare against a server with it disabled fails the checkpoint loudly. Reusing the same operator uid across two jobs that write to the same server is a misconfiguration (their global ids collide).

A NaN or infinite double in a row reaches this sink through the `row_to_json_string` bridge as `null`, or as `"NaN"`, `"Infinity"` or `"-Infinity"` when the sink table sets `json_non_finite='string'`; the sink handles it as it handles any null or string value. See [NaN and infinity in JSON output](README.md#nan-and-infinity-in-json-output).

## Capturing a table (CDC)

A CDC table with `changelog_mode` set captures one PostgreSQL table (tables that name the same slot share one source, see [Several tables on one slot](#several-tables-on-one-slot)): its existing rows first (`snapshot_mode='initial'`, the default under the contract), then every committed change, keyed by the table's primary key. Each value is converted to the declared column type, and a value that type cannot hold exactly stops the job (`on_decode_error='fail'`, the default under the contract) rather than reaching the sink rounded, truncated or as the wrong kind.

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
| An ordinary table, or a partitioned one on PostgreSQL 13 or later, whose primary key is the declared key | changes are applied by that key | both keys |
| Replica identity `DEFAULT` (the primary key), `USING INDEX` on the primary key's columns, or `FULL`; not `NOTHING`. For a partitioned table, every partition's | a delete must carry its key | the identity to set, and the partition |
| A publication, named in `publication_names`, that contains the table and publishes inserts, updates and deletes, under one row filter for all three if it has one, and with every declared column in its column list (if it has one). For a partitioned table, the table itself with `publish_via_partition_root = true`, and none of its partitions on their own | a change the publication leaves out never arrives | the publication and what it lacks |

Out-of-line (TOAST) values need `REPLICA IDENTITY FULL` or `keep_rows='true'` in one case: an UPDATE that leaves a large `text`, `jsonb` or `bytea` value unchanged does not carry it, and only the old row that `FULL` adds to the change holds it. Under `FULL` the source takes the value from there; with `keep_rows` the job takes it from the row it keeps. With neither, such an UPDATE to a declared column stops the source with a message that names the column and both remedies; the source warns at open when a declared column can be stored out of line and the identity is not `FULL`. `FULL` also gives every delete the deleted row's values rather than its key alone.

An UPDATE that changes the primary key is emitted as a delete of the old key followed by the new row. A TRUNCATE of the table is a delete of every row with `keep_rows='true'`, and otherwise stops the source, since without the rows it cannot be expressed as row changes; leave `truncate` out of the publication's `publish` list only if the downstream copy should keep the rows a TRUNCATE removes. A declared column that is dropped or renamed on the server stops the source at the next change; a column added to the table can be captured from then on, see [Adding a column](#adding-a-column).

### Row filters

A publication with a row filter (PostgreSQL 15 or later) publishes the changes of the rows that match it, and the source applies the same filter to the snapshot, so the downstream copy holds exactly the rows that match. When several publications in `publication_names` contain the table, PostgreSQL publishes a change that matches any of their filters, and none is applied when one of them has no filter; the snapshot combines them the same way. An UPDATE that moves a row out of the filter arrives as a delete, and one that moves a row in arrives as an insert of the whole row.

```sql
CREATE PUBLICATION clink_open_orders FOR TABLE orders WHERE (status <> 'archived');
```

The filter must be the same for inserts, updates and deletes: publications that publish different actions under different filters are refused at open, since a row could then be published by one action and its removal not. Under `REPLICA IDENTITY DEFAULT` PostgreSQL allows a filter to name only key columns, and an UPDATE that brings a row into the filter does not carry the out-of-line values it left unchanged; the source stops on such a change and names `REPLICA IDENTITY FULL`, which avoids it. A snapshot query given in `initial_snapshot` (C++ only) cannot be combined with a row filter.

### Partitioned tables

A partitioned table is captured through its root. The publication publishes the root with `publish_via_partition_root = true`, so a change in any partition arrives under the root's name and columns, and the snapshot reads the root, which reads every partition. A row an UPDATE moves to another partition arrives as a delete and an insert. At open the source checks the replica identity of every partition, refuses a partition that is a foreign table, and refuses a publication that would send a partition's changes under the partition's own name (without `publish_via_partition_root`, or with the partition published on its own), since those changes would never reach it.

```sql
CREATE PUBLICATION clink_events FOR TABLE events WITH (publish_via_partition_root = true);
```

PostgreSQL publishes no change for three operations on a partitioned table, so a job does not see them and the downstream copy keeps the rows as they were: attaching a partition (its rows are not inserted), detaching one (its rows are not deleted), and a TRUNCATE of one partition (only a TRUNCATE of the root is published). A partition attached after the job opened is not checked either; give it the same replica identity as the others before attaching it.

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

The engine holds every TIMESTAMP as epoch milliseconds, so a timestamp with non-zero digits below the millisecond is refused: store `timestamp(3)` / `timestamptz(3)` in PostgreSQL, declare the column `VARCHAR` to carry the full text, or set `on_sub_millisecond='truncate'` to round such values down to the millisecond (before 1970 too: `1969-12-31 23:59:59.9995` becomes `-1`). Truncation never applies to a primary key column, whose value is still refused, and every truncated value is counted. Any other declared type is refused when the job is built, and a pair outside the table at open, with the column named. NULL is carried in every type.

### Change metadata

Declared columns with these reserved names carry the change rather than the row:

| Column | Type | Value |
| --- | --- | --- |
| `__op` | `VARCHAR` | `snapshot`, `insert`, `update` or `delete` |
| `__version` | `BIGINT` | the change's WAL position as a number, `0` for a snapshot row. For any one key it increases with every change, so it versions a table that keeps the latest row per key. With `keep_rows='true'` a row that would not outrank the key's previous row carries a higher number instead, a backfilled row among them: see [Versions with `keep_rows`](#versions-with-keep_rows) |
| `__deleted` | `BOOLEAN` | `true` for a delete |
| `__lsn` | `VARCHAR` | the change's WAL position as text (`0/16E2A38`); for a snapshot row, the position streaming starts from |
| `__table` | `VARCHAR` | the schema-qualified table |
| `__xid` | `BIGINT` | the transaction id (`0` for a snapshot row) |

### `changelog_mode='upsert'`: a changelog keyed by the primary key

Each row carries its changelog kind: `insert` for a snapshot row or an INSERT, `update_after` for an UPDATE, `delete` for a DELETE. An update carries the new row only, never the row it replaced, so the table is a changelog only to a consumer that applies rows by that key. The binder holds that line, unless the table is declared with `keep_rows='true'` (below), which gives every update and delete its old row:

- allowed between the table and the sink: projections that keep each key column (renamed or not) and WHERE conditions on key columns;
- allowed sinks: `mode='upsert'` keyed by the table's primary key, carried unchanged, or a sink that writes the changelog as it is (`connector='changelog'`, `'blackhole'`, `'print'`, or `changelog='true'`);
- refused, with the reason and `keep_rows='true'` and `changelog_mode='append'` named: grouping, joins and every other operator, a WHERE on a column an update can change (a row moved out of the condition would never be removed), an upsert sink keyed otherwise, and an append-only sink.

```sql
CREATE TABLE orders_replica (
  id BIGINT PRIMARY KEY, status VARCHAR, amount DECIMAL(12, 2)
) WITH (connector = 'postgres', format = 'json', mode = 'upsert',
        conninfo = '...', "table" = 'public.orders_replica');

INSERT INTO orders_replica SELECT id, status, amount FROM orders_cdc;
```

### `changelog_mode='append'`: one row per change

Each change is an appended row, the deleted row's included, described by the metadata columns. Any query reads it as the stream of changes it is: `COUNT(*)` counts changes, not rows. To keep the table's current state in a store that applies rows by key, write the change rows with `__version` and `__deleted` and let the store keep the latest version per key. In ClickHouse that is a `ReplacingMergeTree(__version, __deleted)` ordered by the primary key, read with `FINAL`; the [PostgreSQL to ClickHouse example](https://github.com/orhaugh/clink/tree/main/examples/postgres-cdc-to-clickhouse) builds one, kills the Worker while a workload runs, and compares the result with the PostgreSQL table.

```sql
CREATE TABLE orders_changes (
  id BIGINT PRIMARY KEY, status VARCHAR, amount DECIMAL(12, 2),
  __version BIGINT, __deleted BOOLEAN
) WITH (connector = 'postgres', format = 'json', mode = 'cdc', changelog_mode = 'append',
        conninfo = '...', slot_name = 'clink_orders', publication_names = 'clink_orders',
        "table" = 'public.orders');
```

### Keeping the rows (`keep_rows`)

`keep_rows='true'` puts a stage behind the source that keeps the latest row of every primary key in the job's keyed state: one copy of the declared columns, checkpointed with the job and held by its state backend (RocksDB included), on one subtask like the source. From the row it keeps it

- fills in an out-of-line value an UPDATE left unchanged without carrying it, so `REPLICA IDENTITY FULL` is not needed for large values, including when the UPDATE also changed the key;
- turns a TRUNCATE of the table into a delete of every row it keeps;
- in `changelog_mode='upsert'`, emits an update as `update_before` with the row it replaced followed by `update_after`, and a delete with the row it removed, so the table is a changelog any consumer can retract from: grouping, joins and a WHERE on any column are allowed. An update that changes nothing is not emitted;
- in `changelog_mode='append'`, gives a delete row the values of the row it removed and the delete's own `__op`, `__version` and `__deleted`, the version raised where it would not outrank that row (below);
- drops a change no newer than the row it keeps, which only a replay can deliver, and a snapshot row identical to the row it keeps (a snapshot read again after a restore).

```sql
CREATE TABLE orders_cdc (
  id BIGINT PRIMARY KEY, status VARCHAR, amount DECIMAL(12, 2), notes VARCHAR
) WITH (connector = 'postgres', format = 'json', mode = 'cdc', changelog_mode = 'upsert',
        keep_rows = 'true', conninfo = '...', slot_name = 'clink_orders',
        publication_names = 'clink_orders', "table" = 'public.orders');

CREATE TABLE totals (status VARCHAR PRIMARY KEY, n BIGINT, total DECIMAL(14, 2))
  WITH (connector = 'postgres', format = 'json', mode = 'upsert', conninfo = '...',
        "table" = 'public.totals');

INSERT INTO totals
SELECT status, COUNT(*) AS n, SUM(amount) AS total FROM orders_cdc GROUP BY status
ALLOW UNBOUNDED STATE;
```

A windowless grouping keeps one accumulator per group for the life of the job, which the planner admits only with `ALLOW UNBOUNDED STATE` (or a `state_ttl`). A group whose rows have all gone stays in the target with a count of 0.

A change to a key the stage keeps no row for is one it cannot complete. After a snapshot taken fresh (a restore during the snapshot, or a slot that already existed), the stream replays changes the snapshot already holds, among them updates of rows deleted before the snapshot was read; until the stream passes the WAL position that snapshot was read at, which the source keeps in its checkpoint, such updates are passed over, and the deletes that follow remove nothing. Outside that window an update of a key it keeps no row for is emitted as an insert, and stops the job when it left an out-of-line value unchanged: the row reached the table some way the change stream does not carry (a partition attached, rows restored outside the slot). A delete of such a key is passed on with the columns it carries when every operator between the stage and the sink applies rows by key (it can remove a row an earlier run wrote), and dropped when a consumer retracts, which would retract a row it never counted.

#### Versions with `keep_rows`

With `__version` declared, every row the stage emits for a key carries a higher `__version` than any row it emitted for that key before. A store that keeps the highest version per key, such as a `ReplacingMergeTree(__version, __deleted)` read with `FINAL` or an `argMax` over `__version`, therefore keeps the row emitted last, whatever order the rows reach it in. Two different rows of one key under one version would leave the choice to the store, and `argMax` returns either. In `changelog_mode='upsert'`, an `update_before` and a delete repeat the row they retract, its version included.

The version is the change's WAL position, which already increases with every change to a key, except where that would not outrank what the stage emitted for the key before:

- A snapshot row of a key the stage keeps a row for refreshes that row: a backfill after a column was declared ([Adding a column](#adding-a-column)), or a snapshot taken again after a restore. It carries one above the kept row's version. A refresh that changes none of the row's columns is not emitted.
- A change of such a key, and in `changelog_mode='append'` a delete, carries one above the kept row's version when that is higher than its WAL position. Two changes of one key are at least one WAL record apart, 24 bytes or more, so this needs at least as many refreshes since the key's last change as there are bytes between the two changes.
- A row of a key the stage keeps no row for, one deleted earlier or never seen, outranks every delete emitted before it. A snapshot row carries one above the highest version a delete carried, or `0` while no delete has been emitted, as in the first snapshot. A change keeps its WAL position unless a delete was given a version above its own, and then carries one above the highest such version.

Each version follows from the job's checkpointed state and the changes after it, so a restore gives every row it emits again the version it had the first time, and the store absorbs the repeat. A snapshot taken again after a failure reads the table as it is then: a row that changed since the first attempt read it gets the same version as that attempt's row, and the change that made the difference, which the stream replays after the snapshot, outranks both. The highest delete versions are kept in the job's state beside the rows. A checkpoint taken before the stage kept them restores without them: a snapshot row of a key deleted before that checkpoint can then rank below the delete, as it did before, until the stream replays the insert that brought the key back.

Turning `keep_rows` on or off changes the job's plan: start the job afresh, with a new slot and snapshot, rather than restoring a savepoint taken under the other setting. A stage that starts with no rows cannot complete the changes of rows it never saw.

### Several tables on one slot

CDC tables that name the same `conninfo` and `slot_name` are read by one source: one replication slot, one walsender, and the WAL decoded once for all of them. The publication names every table, and each table keeps its own declaration: key, columns, `changelog_mode`, `keep_rows`. The source checks each table at open, reads each one's snapshot from the same exported snapshot, and streams their changes in commit order, routing each row to its own table's branch of the plan. The options that belong to the slot (`publication_names`, `snapshot_mode`, `create_slot`, `on_decode_error`, the intervals and timeouts) must be the same on every table that names it, and the planner refuses a job where they differ.

A slot has one reader, so every query over its tables must be in one job: in one `INSERT` (a join of two of them, for one), or in one [statement set](../sql.md#several-statements-as-one-job). Two separate jobs naming the same slot are refused when the script is compiled.

```sql
CREATE TABLE orders_cdc (id BIGINT PRIMARY KEY, customer_id BIGINT, amount DECIMAL(12, 2))
  WITH (connector = 'postgres', format = 'json', mode = 'cdc', changelog_mode = 'upsert',
        conninfo = '...', slot_name = 'shop', publication_names = 'shop',
        "table" = 'public.orders');
CREATE TABLE customers_cdc (id BIGINT PRIMARY KEY, name VARCHAR)
  WITH (connector = 'postgres', format = 'json', mode = 'cdc', changelog_mode = 'upsert',
        conninfo = '...', slot_name = 'shop', publication_names = 'shop',
        "table" = 'public.customers');

EXECUTE STATEMENT SET BEGIN
  INSERT INTO orders_copy SELECT * FROM orders_cdc;
  INSERT INTO customers_copy SELECT * FROM customers_cdc;
END;
```

The tables share the slot's position and its checkpoints, not a transaction boundary downstream: each table's rows reach their own sink, so a transaction that changed both can be seen by a reader of the targets half applied.

### Adding a column

A job captures the columns it declares and ignores the rest, so a column added to the table runs past it until it is declared. To capture it:

1. Add the column in PostgreSQL (`ALTER TABLE orders ADD COLUMN note text`) and to the target table.
2. Stop the job with `clink stop --job-id=N`, which takes a final checkpoint and prints its id.
3. Declare the column in the CDC table (and read it in the query), and resubmit the job restoring from that checkpoint: `clink_submit_sql ... --restore-from-dir <checkpoint dir> --restore-from-checkpoint-id <id>`. When other jobs share the checkpoint directory, name this one by its marker directory, `--restore-from-dir <checkpoint dir>/_jobs/<job id>`.

The restored source compares the columns declared now with those its checkpoint recorded. For a column declared since, it reads the table again, with a snapshot taken fresh (a backfill), before it resumes the stream from its checkpoint: every row is delivered with the new column, rows no change has touched since the `ALTER TABLE` included. The changes it then replays from before the `ALTER TABLE` do not have the column, and the job takes it from the rows it keeps, so this needs `keep_rows='true'`; a restore that declares a new column without it is refused, with the column named, and the job has to start afresh. A backfilled row carries a `__version` one above the row it refreshes, and every later change of the key a higher one ([Versions with `keep_rows`](#versions-with-keep_rows)), so a store that keeps the highest version per key keeps the backfilled row until a change supersedes it. Restore from the checkpoint the stop printed, not an older one: the changes the job emitted after an older checkpoint are replayed with the new column under the versions they were first emitted with, and such a store keeps either of the two rows. A column declared with another type is read again the same way.

Declare a column only once it exists in PostgreSQL: open refuses a declared column the table does not have. Dropping, renaming or retyping a declared column in PostgreSQL stops the job at its next change.

Without `changelog_mode` a `mode='cdc'` table keeps its earlier form: `test_decoding` unless `plugin` says otherwise, no snapshot, `on_decode_error='drop'`, every value as text, every table of the publication, and each change, delete included, an appended row with `__op`, `__table`, `__lsn` and `__xid`. It is kept for existing jobs; a new job should set `changelog_mode`.

## Position, snapshot and recovery

The source keeps its replication position in the checkpoint and confirms the slot only as far as a completed checkpoint, so a restore can always read the changes after its cut, and refuses to start rather than let the server skip any.

**Position.** A checkpoint records the end LSN of the last transaction whose changes were all emitted. `pgoutput` version 1 sends nothing of a transaction until it commits, and then sends it whole, in commit order, so restarting the stream at that position resends exactly the transactions not yet emitted. A checkpoint can fall inside a transaction, since the source emits at most 1024 changes per call; the checkpoint then also records that transaction's commit LSN and how many of its changes were emitted, and after a restore the source skips those changes of the resent transaction. Replay from a checkpoint is therefore exact: no change before the cut is emitted again by the source, and none after it is missed. Checkpoints written by earlier releases (the received LSN alone) are read as a position with no transaction in flight.

**Acknowledgement.** The status update's flush position, which PostgreSQL takes as the slot's `confirmed_flush_lsn`, is the position recorded by the newest checkpoint the runtime has reported complete. A checkpoint that aborts confirms nothing. Earlier releases confirmed whatever had been received, on a timer and on every keepalive, so the slot could run ahead of the last completed checkpoint, and a restore was then started by PostgreSQL at the slot's later position, silently skipping the changes in between.

**Refusals.** Before streaming starts, and again before every reconnect, the source reads the slot. It refuses when the slot does not exist although the job's checkpoint resumes in it, when it decodes with another plugin or belongs to another database, when it has lost WAL it needs (`wal_status=lost`), and when it is confirmed beyond the position the job resumes from. Each refusal names the slot and the positions; none is retried. After a restart, if the stream does not start with the transaction a checkpoint interrupted, or that transaction comes back shorter, the source refuses rather than guess. Recovering from a refusal means starting the job from scratch, with a new slot and snapshot, once the downstream copy has been reset.

**Snapshot.** When the job creates its slot, the slot exports its snapshot. The source imports it into a `REPEATABLE READ` transaction, reads the table through a server-side cursor, `snapshot_batch_rows` at a time, and then streams from the slot's consistent point. A change committed during the snapshot is not in it and arrives through the stream, so the boundary is exact. There is no such boundary when the job is restored from a checkpoint taken during the snapshot, or starts with no checkpoint on a slot that already exists (a job that died before its first checkpoint). The snapshot is then taken again from scratch in a fresh `REPEATABLE READ` transaction, and streaming starts at the slot's retained position, before that snapshot. Changes the snapshot already reflects are replayed after it; every row and change carries the whole row and is applied by key, so the downstream converges on the table's state once the stream has passed the point the snapshot was taken, though it may briefly hold an older version of a row on the way. The source does not resume a snapshot part-way. A restore that declares a column the checkpoint did not also reads the table again this way before it resumes ([Adding a column](#adding-a-column)).

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

Sink (default, `postgres_sink`): at-least-once. Each flush is one transaction, committed before the barrier that asked for it is passed on, and a restored job writes again the rows after its checkpoint: a plain insert appends them a second time, `on_conflict='update'` rewrites them by key, and `on_conflict='nothing'` leaves the rows it finds. Rows are applied in the order they arrived, a key's later row in a later statement. The recovery cells show both sides: killed before a flush's `COMMIT`, no record ends in two rows; killed after it and before its checkpoint completed, exactly the records committed after the restored checkpoint do. A failure fails the sink until it is opened again, and a `COMMIT` whose reply is lost leaves its flush's outcome unknown. See [The sinks](#the-sinks).

Sink (`mode='upsert'`, `postgres_upsert_sink`): effectively-once on the sink table for a stable key, a deterministic defining query and one writer per key. Applies the changelog by key (upsert on insert/update_after, DELETE on delete/update_before) in the order each key last changed, one transaction per flush, so a replay converges the table to the same rows whichever keys the table treats as one. Not two-phase commit.

Sink (`delivery_guarantee='exactly_once'`, `postgres_2pc_sink`): exactly-once via two-phase commit (`PREPARE TRANSACTION` / `COMMIT PREPARED`). Rows land iff their checkpoint completes globally; a prepared-but-uncommitted transaction survives a crash and is committed on restart, and a prepared transaction the restore does not hold is rolled back at open, its checkpoint one no restore chooses again. Requires `max_prepared_transactions > 0` on the server. See the exactly-once sink section above.

CDC source, stated part by part (see [Position, snapshot and recovery](#position-snapshot-and-recovery)):

- Source replay: from a checkpoint taken while streaming, exact. Every change committed after the checkpoint's cut is emitted, and none before it, including when the cut fell inside a transaction. A snapshot interrupted by a failure is taken again from scratch and converges rather than resuming.
- Duplicates: after a job restart, everything emitted since the last completed checkpoint is emitted again, so a sink without two-phase commit receives those changes twice. A snapshot taken again re-emits every row. A reconnect within `reconnect_timeout_ms` emits nothing twice.
- Transaction visibility: an aborted transaction, or a rolled-back savepoint, is never emitted; committed transactions arrive in commit order. A transaction's changes can be split across checkpoints and across the sink's batches, so a reader of the downstream copy may see part of a transaction.
- Downstream state: a consumer that applies rows by the primary key (an upsert sink, or a store that keeps the highest `__version` per key and removes `__deleted` keys) converges on the table's state once it has caught up. With `keep_rows='true'` so does a consumer that retracts, such as a grouping, which works from whole rows.
- Losses: none without a refusal. A change the contract cannot carry stops the job and is met again on every restart (`on_decode_error='fail'`), and the slot is never confirmed past it. `on_decode_error='drop'` loses that change, counted in `dropped_events_total`. Without `changelog_mode` the default is still `drop`.
- End to end: the source on its own is not a delivery guarantee; the job's guarantee is the weaker of the source's replay and the sink's. Into the at-least-once ClickHouse sink the result is at-least-once delivery of every change and a correct current state through `__version`; into the PostgreSQL upsert sink it is effectively-once on the replica table.

SELECT source: at-least-once. The checkpoint holds how many rows the source emitted and a hash of them. A restore runs the query again and resumes after those rows only when the query's first rows hash the same, which holds for a query ordered by a unique key (`ORDER BY id`) over rows unchanged up to there, rows added after them included: then the source is exactly-once at its boundary. Otherwise it emits every row again, and says so in its log. A query without `ORDER BY` can return the same rows in another order (a parallel plan, or a scan that starts where another is reading), and a row deleted, added or changed among those emitted moves the rest; before, the source resumed by position regardless, and skipped rows: in the live suite, after one of the first four rows was deleted, the row that moved into fourth place was never emitted. A checkpoint from an earlier release holds the position alone, with nothing to compare, so a restore from it emits every row again.

## Limitations

- Each sink must be the only sink on its chain: at every checkpoint barrier the at-least-once and upsert sinks write out what they received before the barrier, and the exactly-once sink prepares its transaction and stages its handle in the checkpoint, which holds only while it owns its chain's checkpoint, so `Dag::add_sink` refuses another sink beside it, whichever is added first. SQL and job graphs plan every sink as a subtask of its own; with the Dag API, put the other sink in a separate Dag (see [checkpointing](../internals/checkpointing.md#guarantees-and-caveats)).
- The default sink (`postgres_sink`) is at-least-once: it writes replayed rows again, and a `COMMIT` whose reply is lost leaves its flush's outcome unknown. Exactly-once needs `delivery_guarantee='exactly_once'` (the `postgres_2pc_sink` variant) and a server with `max_prepared_transactions > 0`.
- The keyed modes need a table with a PRIMARY KEY or UNIQUE index on exactly their key, over whole values and every row; with more than one subtask the changelog sink's key columns must be of the types in [Keys and order](#keys-and-order). The changelog sink converges only with one writer per key.
- The sinks' byte bounds count SQL text, and are not a bound on their memory. The sinks are tested against PostgreSQL 16 only.
- `mode='upsert'` requires a PRIMARY KEY (the conflict target + delete key) and is effectively-once, not two-phase commit. `mode='upsert'` combined with `delivery_guarantee='exactly_once'` is rejected (pick one). For a plain append stream, `on_conflict='update'` on the default sink is the lighter idempotent-by-key option.
- The SELECT source is snapshot mode only: it runs the query once at `open()` and closes when the result set is exhausted. There is no incremental polling or `refresh_interval` behaviour (the field is reserved and ignored). It holds its whole result in memory, its query once sent runs to the end before a cancel is seen, and a restore repeats every row unless the query returns the rows it emitted again, in the same order.
- The CDC source needs `wal_level=logical`, a role with `REPLICATION`, and `pgoutput` for the SQL contract. Several tables can share a slot and its source, with no transaction atomicity across them downstream. Of a table's schema changes it follows one, a column added and then declared on a restore with `keep_rows='true'`.
- A partitioned table is captured through its root, on PostgreSQL 13 or later. Attaching or detaching a partition, and a TRUNCATE of one partition, publish nothing, so the downstream copy keeps the rows they add or remove.
- A snapshot is not resumable. A failure during the snapshot takes it again from scratch, and the downstream converges through the replay rather than seeing an exact boundary.
- Timestamps carry milliseconds. A PostgreSQL value with finer digits is refused, or rounded down with `on_sub_millisecond='truncate'`; a primary key timestamp is always refused. Declare `VARCHAR` to keep the full text.
- Without `REPLICA IDENTITY FULL` or `keep_rows='true'`, an UPDATE that leaves an out-of-line value of a declared column unchanged stops the source.
- Without `keep_rows='true'`, a TRUNCATE of the captured table stops the source.
- `keep_rows='true'` keeps a copy of the captured columns in the job's state, on one subtask.
- The slot is confirmed only by completed checkpoints, so WAL is retained for as long as the job is stopped or not checkpointing. A job containing a sink whose commit cannot be re-executed (the Kafka transactional sink) can be restored to a checkpoint older than one already reported complete; the CDC source then refuses, since the slot was confirmed past it.
- `on_decode_error=fail` stops the pipeline with no quarantine, and does so again on every restart for a permanently undecodable change.
- Dropping a replication slot requires that no session is using it; `drop_slot()` must be called after `close()`, or via `drop_slot_on_close=true`.

## Testing

Tests that need no server run unconditionally under the `postgres` label: the pgoutput decoder (including strict decoding of unchanged TOAST values, key changes and TRUNCATE), the SQL contract's row encoder and type conversions, the position and acknowledgement rules (`test_cdc_cursor.cpp`), the CDC factories' option checks, the replay-offset encodings, and the sink and codec units. The binder's rules for the upsert changelog, the planner's singleton placement and the row-keeping stage (`keep_rows`: old rows, filled values, truncates, replays, the window after a fresh snapshot, the versions it emits, restore from a checkpoint) are in `clink_sql_tests` (`SqlBinderUpsertCdc.*`, `SqlPhysical.PostgresCdc*`, `SqlCdcKeepRows.*`).

Live tests run against a real PostgreSQL server and are skipped unless `CLINK_POSTGRES_CDC_TEST_DSN` names one started with `wal_level=logical`. The reference server is the `postgres-cdc` service in `docker/integration-services.yml`, PostgreSQL 16.15 pinned by digest. The same DSN drives the sink live tests.

```bash
docker compose -f docker/integration-services.yml up -d --wait postgres-cdc
export CLINK_POSTGRES_CDC_TEST_DSN="host=localhost port=5432 user=postgres password=postgres dbname=postgres"
ctest --test-dir build -L postgres -j1
```

`PostgresCdcContractLive.*` drives the source through its checkpoint hooks, with a model of a transactional downstream that a restore rewinds, and checks every case against two oracles: the table itself, and every row version the workload committed. It covers a snapshot taken while the table changes, rolled-back work, a transaction spanning a checkpoint, a crash during snapshot setup, during the snapshot rows and at the transition to streaming, a restore after the transition, acknowledgement only by completed checkpoints (an aborted or pending one confirms nothing), a walsender ended inside a large transaction, a missing slot and a slot advanced past the checkpoint, decode-error refusal and the drop policy, the open-time refusals, cancellation from every wait, the supported types with NULLs and unchanged TOAST values, microsecond timestamps refused by default and rounded down on request, a partitioned table captured through its root (a row moved between partitions included, and the partition TRUNCATE that PostgreSQL does not publish) with the publications that would hide its partitions' changes refused, publication row filters applied to the snapshot and the stream (rows moving in and out, two publications combined) with filters that differ by action refused, and the upsert changelog's kinds. `PostgresCdcSql.*` (`clink_postgres_sql_tests`) runs a `changelog_mode='upsert'` table through the SQL frontend into the upsert sink and compares the replica with its source; with `keep_rows='true'` under `REPLICA IDENTITY DEFAULT` it keeps a grouping and a whole-row replica equal to the table through updates that leave large values unchanged, a key change, deletes and a TRUNCATE, and converges after a snapshot taken fresh on a slot an earlier run left. `PostgresCdcContractLive.AColumnDeclaredSinceTheCheckpoint*` restores a source declaring a column added while it was stopped: read again with it, the replayed changes that predate it marked for the kept rows, and refused without `keep_rows`. `PostgresCdcContractLive.SeveralTablesShareOneSlot` captures two tables through one source, a transaction across both and a restore included, and `PostgresCdcSql.TablesOnOneSlotFeedTheirTargetsFromOneSource` runs a statement set of two of them into their replicas with one walsender. `PostgresCdcAddColumn.*` (`clink_integration_tests`) follows the documented procedure in a cluster: the job stopped with `clink stop`, the column added, the job resubmitted from the stop's checkpoint, and the target compared with the table in every column. `PostgresCdcClickHouseBackfill.*` follows it with `changelog_mode='append'` into a ClickHouse change log and the `ReplacingMergeTree(__version, __deleted)` it feeds, once uninterrupted and once with the source's worker killed after a checkpoint completed inside the backfill. Before and after later updates and deletes it requires that no key has two different rows under one `__version` in the log, and that the table read with `FINAL`, and rebuilt from the log with `argMax`, equals the PostgreSQL table in every column, the added one included on rows no change has touched since it was added.

The CDC recovery cells (`Cells/PostgresCdcClickHouseRecovery.*` in `clink_integration_tests`) run the SQL job in a cluster, a coordinator and two `clink_node` workers, capturing a table with `changelog_mode='append'` into a ClickHouse change log while a workload commits inserts, updates and deletes. A ClickHouse materialised view feeds a `ReplacingMergeTree(__version, __deleted)`. Each cell parks the source's worker at a fault point and kills it once the fault framework's witness line shows the phase was reached: during the snapshot after a checkpoint completed in it, at the transition to streaming, after a streaming checkpoint recorded its position but before it completed, and just after a completed checkpoint was confirmed to the slot. A fifth restarts the PostgreSQL server while the job streams, which the source must ride out with no job restart. After a marker row arrives through the stream, every cell requires the table read with `FINAL`, and rebuilt from the log with `argMax`, to equal the PostgreSQL table in every column; every committed row version and delete to be in the log; and exactly the restarts it names. `PostgresCdcSlot.ASlotThatLostItsWalIsRefused` gives a slot a low `max_slot_wal_keep_size` on its own server, lets the server invalidate it, and requires the restore to refuse. These start their own PostgreSQL and ClickHouse containers.

The sinks' PostgreSQL rules (`test_pg_sink_rules.cpp`: values and the roundings they refuse, key spelling, statement heads, message redaction, the table checks) and the record rules the SQL sinks share (`tests/test_sink_record_rules.cpp`) run without a server, and their failure state runs against a scripted server (`test_pg_sink_failures.cpp`, `PgSinkFailures`): every failure followed by another record, barrier and flush, reopening, refused records and markers, a failed statement, a `COMMIT` refused, answered `ROLLBACK` and lost, deadlock and lock wait retries, the one-row replan after 21000, a cancel during a statement and during `COMMIT`, `close()`, statement splitting, the byte threshold, the table checks, order and netting, and `PostgresSink`. `test_pg_sink_contract_live.cpp` (`PgSinkContractLive`) covers the contract against the server: keys the table treats as one (a nondeterministic ICU collation, `char(n)` padding, numbers written as strings, with decimals and exponents), applied in one flush and in a flush per change and compared row for row with a reference table to which PostgreSQL applied each change in turn; composite and timestamp keys; two keys of one row in one flush; the tables each mode refuses; exact values (`bigint` at both ends, a 60-digit `numeric`, a 100-digit plain `numeric`, characters outside the Basic Multilingual Plane, quotes and backslashes, `bytea` with NUL and control bytes, `jsonb`, `timestamp` and `timestamptz` at their limits, a large `double precision`, null and missing members); values the server refuses, each failing its flush with nothing committed; values the column would round or cut, refused before they are written; a database set to another time zone and to `standard_conforming_strings off`; 40 statements rolled back as one flush; another session's lock failing a flush within its bound; a cancel ending a flush that waits for a lock; a connection ended while its `COMMIT` is on its way, reported as unknown; the factories' sinks; and `PostgresSink`.

The sinks' recovery cells (`tests/integration/test_postgres_sink_recovery.cpp`, `PostgresSinkRecovery`) run a coordinator and two workers polling a feed the test serves into each sink, and kill a worker at `postgres.sink_before_commit`, with a flush's statements executed and not committed, and at `postgres.sink_after_commit`, with the flush committed and its checkpoint not complete. Read through a libpq connection of the test's own: for the append sink, every record's row present with its columns, and the records with two rows exactly those committed after the checkpoint the job restored, none when the kill came before `COMMIT`; for the upsert sink, and for a SQL job writing it at parallelism 2, the table holds every row the input's last records leave, column for column, and none of the rows they delete. Each cell checks the single restart, the replay, and the fault firing where it names, and before `COMMIT` that the server held a writing transaction open.

The SELECT source is tested by `PostgresSourceReplay` (its position and hash through a checkpoint, an earlier release's position, a form neither release wrote), `PostgresRowCodec`, and `PostgresSourceLive` (`test_postgres_source_live.cpp`) against the server, at the operator level: one source reads part of the result and is checkpointed, the table changes or not, and another restores and reads to the end. Values and NULLs arrive as read; over the same rows the restore resumes after them; after an earlier row is deleted or changed it emits every row again, nothing skipped; after rows are added at the end it resumes and reads them; from an earlier release's position it emits every row again.

`scripts/postgres-live.sh` runs all of the above in three phases: `cdc`, the change stream's suites and cells, `sink`, the sinks', and `select`, the SELECT source's. It keeps each phase's googletest JSON reports and runs `scripts/check-gtest-reports.py`, which fails the run on any skipped or failed case, on any required suite that ran fewer cases than it should, and on a case outside every phase: a live test skips when its server is missing, and googletest counts a skip as a pass. The `postgres-cdc` workflow (`.github/workflows/postgres-cdc.yml`) runs it on every push and pull request, against the pinned `postgres-cdc` service, and keeps the reports, a provenance file and the cells' evidence lines. Run the PostgreSQL live tests one process at a time: the two-phase-commit sink cases name their prepared transactions, which collide across processes on one server.

```bash
docker compose -f docker/integration-services.yml up -d --wait postgres-cdc
CLINK_POSTGRES_CDC_TEST_DSN="host=localhost port=5432 user=postgres password=postgres dbname=postgres" \
  scripts/postgres-live.sh build
```

The convenience script `scripts/sanitize_connectors.sh` networks a PostgreSQL instance against the `clink-build:latest` image and sets the DSN automatically.

The two-phase-commit sink's exactly-once suite (`PostgresExactlyOnceTest.*` in `clink_integration_tests`) runs the sink in a cluster, a coordinator and workers, against a PostgreSQL container it starts itself, and requires every record exactly once: a clean run, a worker killed, a worker killed inside the commit window, a PostgreSQL outage during recovery, and two kills on a longer run. `scripts/docker-suites-live.sh` runs it, with the pipeline and sink suites (`PostgresIntegration.*`, `PostgresSinkIntegration.*`) and the transport refusals (`Qual12PgTransportLive.*`), which also start their own PostgreSQL, and fails on any skipped case; the `docker-suites` workflow runs it on every push and pull request.
