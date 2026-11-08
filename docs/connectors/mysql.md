# MySQL connector

> Connects to a MySQL server as a source (incremental cursor SELECT, or the binlog change stream) and as batched INSERT and upsert sinks, exchanging rows as JSON-object strings.

## Overview

This connector integrates a MySQL server over the synchronous C API of mariadb-connector-c. The client library being MariaDB's does not make a MariaDB server supported: the change stream refuses one at start, the sinks' keyed statements use a row alias a MariaDB server does not accept, and the polling source is tested against MySQL only. It provides four factories on the `std::string` channel: a polling cursor source (`mysql_source`) that issues incremental `SELECT` statements, a binlog CDC source (`mysql_cdc_source`) that streams row-level changes from the master's binary log, a sink (`mysql_sink`) that appends records, or inserts or updates them by key, in batched multi-row `INSERT` statements, and a changelog sink (`mysql_upsert_sink`) that maintains a table by its key. Every record on the wire is a JSON-object string keyed by column name; the SQL Row path bridges these via `json_string_to_row` (source) and `row_to_json_string` (sink). Both sources deliver every cell as a JSON string (the cursor source speaks the MySQL text protocol, and the CDC decoder renders each binlog value as text), and the SQL bridge takes a numeric token into a declared `BIGINT`, `INTEGER`, `DOUBLE` or `REAL` column as that type, so a `TINYINT(1)` declared `BIGINT` arrives as `1`, not `"1"`. The CDC source emits flat JSON change rows carrying `__op`, `__table`, `__lsn` and `__xid` metadata.

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| mariadb-connector-c (libmariadb) | System package via apt (Debian `libmariadb-dev`) / brew (macOS `mariadb-connector-c`) | Not pinned by clink |

The change stream and the sinks are tested against MySQL 8.0 and 8.4: the connector's live gate pins MySQL 8.0.46 and 8.4.13 by digest (see [Testing](#testing)). The sinks' insert-or-update and changelog statements use the row alias of MySQL 8.0.19 (`INSERT ... AS clink_new ON DUPLICATE KEY UPDATE`), so they need 8.0.19 or later. MySQL 5.7 is not tested, and a MariaDB server is not supported.

TLS support is built into mariadb-connector-c, so no separate TLS dependency is required. This connector does not use Apache Arrow on its data path; records are JSON-object strings.

## Enabling it

The module is gated by the CMake option `CLINK_WITH_MYSQL`, which defaults to `AUTO`:

- `AUTO` (default): the connector is built only if mariadb-connector-c is discovered.
- `ON`: build is required; CMake fails with a fatal error if the client library is not found.
- `OFF`: the target is not defined.

The client library is discovered three ways, most portable first: a pkg-config imported target (`libmariadb`, as shipped by apt's `libmariadb-dev`), the `mariadb` CONFIG package, then a manual header and library probe (Homebrew `mariadb-connector-c` or a bare install). On Debian, install the dependency with the connector deps script or directly:

```bash
apt-get install -y libmariadb-dev

cmake -S . -B build -DCLINK_WITH_MYSQL=ON
```

The target compiles only where the client library is present, defining `CLINK_HAS_MYSQL`.

## Factories

| Factory name | Direction | Record type |
| --- | --- | --- |
| `mysql_source` | Source | `std::string` (JSON object per row) |
| `mysql_cdc_source` | Source | `std::string` (flat JSON change row) |
| `mysql_sink` | Sink | `std::string` (JSON object per row) |
| `mysql_upsert_sink` | Sink | `std::string` (JSON object per change; `__row_kind` marks a delete) |

## Configuration

### Connection options (shared by every factory)

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `host` | No | `localhost` | Server host. |
| `port` | No | `3306` | Server port. |
| `user` | No | `` (empty) | Connection user. |
| `password` | No | `` (empty) | Connection password; an empty value connects without a password. |
| `database` | No | `` (empty) | Database selected in-band at connect. |
| `ssl` | No | `false` | `ssl='true'` encrypts the connection (the server is required to support TLS; a plaintext server is refused). |
| `ssl_ca` | No | `` (empty) | Optional PEM path to the CA used to verify the server certificate. |
| `ssl_cert` | No | `` (empty) | Optional PEM path to the client certificate (mutual TLS). |
| `ssl_key` | No | `` (empty) | Optional PEM path to the client key (mutual TLS). |
| `ssl_verify` | No | `true` | `ssl_verify='false'` skips server-certificate verification (encrypted but not authenticated, for example a self-signed development certificate). |

Source: `conn_options_from` in `impls/mysql/src/register_factories.cpp` and the `ConnectOptions` struct in `impls/mysql/include/clink/mysql/mysql_client.hpp`.

### `mysql_sink`

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `table` | Yes | (none) | Target table, `name` or `database.name`. |
| `columns` | See note | (none) | Comma-separated projection and column order. On the SQL path it defaults to the declared table schema (`schema_columns`), so it is only required when the sink is built outside SQL. Each column is looked up by name in the record's JSON object; an absent or null member becomes SQL NULL. |
| `on_duplicate` | No | `` (empty) | Empty writes a plain `INSERT`. `'update'` writes `INSERT ... ON DUPLICATE KEY UPDATE` by the table's one PRIMARY KEY or UNIQUE index (see [Modes](#modes)). Any other value is refused. This is not clink `mode='upsert'`, the changelog contract of `mysql_upsert_sink`. |
| `update_columns` | No | `` (empty) | The `ON DUPLICATE KEY UPDATE` SET list; empty sets every column. Taken only with `on_duplicate='update'`, and each must be one of `columns`. |
| `batch_records` | No | `1000` | Flush at this many buffered rows, 1 to 1000000. |
| `max_bytes` | No | `16777216` | Flush once the buffered rows reach this many bytes of SQL text, 0 to 1073741824; `0` means the default, 16 MiB. |
| `max_record_bytes` | No | `1048576` | The most one record's row may take as SQL text, 64 up to `max_statement_bytes`. A larger record fails the sink. |
| `max_statement_bytes` | No | `4194304` | The most one statement may take, 1024 to 1073741824, and never more than the server's `max_allowed_packet` less 1024 bytes. |
| `linger_ms` | No | `0` | Flush rows buffered this long, checked as each record arrives; 0 (off) to 3600000. |
| `lock_wait_timeout_s` | No | `30` | The session's `innodb_lock_wait_timeout` and `lock_wait_timeout`, 1 to 3600 seconds. |
| `max_retries` | No | `3` | How many times a flush that met a deadlock or a lock wait timeout is tried again, 0 to 10. |
| `timestamp_columns` | No | derived | Comma-separated `<column>:ts` or `<column>:tstz`: the columns written as timestamps (see [Values](#values)). On the SQL path the planner names every `TIMESTAMP` column. |

Every option is read strictly: a value the sink cannot use (a number with trailing text, a value out of range, an `on_duplicate` other than `'update'`) is refused when the job is built, never replaced by the default.

Source: the `mysql_sink` lambda in `impls/mysql/src/register_factories.cpp` and `MysqlSinkOptions` in `impls/mysql/include/clink/mysql/mysql_sink.hpp`.

### `mysql_upsert_sink`

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `table` | Yes | (none) | Target table. |
| `columns` | See note | (none) | The full projection, as for `mysql_sink`. |
| `key_columns` | Yes | (none) | The row's identity: the columns of the table's PRIMARY KEY or UNIQUE index. On the SQL path, the table's PRIMARY KEY. |
| `update_columns` | No | `` (empty) | The upsert's SET list; empty sets every column. When set it must name every non-key column. Not taken on the SQL path. |
| `batch_records` | No | `1000` | Flush at this many buffered keys, 1 to 1000000. A key's later change replaces its earlier one in the buffer. |
| `max_record_bytes` | No | `1048576` | The most one record's row, or its key for a delete, may take as SQL text. |
| `max_bytes`, `max_statement_bytes`, `lock_wait_timeout_s`, `max_retries`, `timestamp_columns` | No | as for `mysql_sink` | As for `mysql_sink`. |

The upsert sink takes no `linger_ms`, and reads its options as strictly.

Source: the `mysql_upsert_sink` lambda in `impls/mysql/src/register_factories.cpp` and `MysqlJsonUpsertSinkOptions` in `impls/mysql/include/clink/mysql/mysql_json_upsert_sink.hpp`.

### `mysql_source` (incremental cursor)

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `table` | Yes | (none) | Source table. |
| `cursor_column` | Yes | (none) | Ascending, NOT NULL cursor column. Must be unique unless `id_column` is set. The cursor comparison is exclusive (`>`), so the boundary row is never re-emitted. |
| `id_column` | No | `` (empty) | Optional unique tie-breaker for keyset pagination, so a non-unique `cursor_column` does not drop rows sharing a boundary value at a page (LIMIT) boundary. |
| `initial_cursor` | No | `` (empty) | Cold-start cursor value; empty re-scans from the beginning until the first checkpoint. |
| `batch_size` | No | `1000` | Rows per poll (`LIMIT`). |
| `poll_ms` | No | `1000` | Poll interval in milliseconds. |
| `jitter_frac` | No | `0` | Fractional jitter applied to the poll interval. |
| `bounded` / `mode` | No | `false` | `bounded='true'` or `mode='snapshot'` reads the table once and then finishes, rather than tailing it forever. |

Only subtask 0 polls the table, whatever the job's parallelism; the other subtasks read nothing.

Source: the `mysql_source` lambda in `impls/mysql/src/register_factories.cpp` and `MysqlPollOptions` in `impls/mysql/include/clink/mysql/mysql_source.hpp`.

### `mysql_cdc_source` (binlog CDC)

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `server_id` | Yes | (none) | Replica server id, 1 to 4294967295, unique among the server's replicas (a second dump under the same id disconnects the first). |
| `tables` | No | `` (empty) | Comma-separated allowlist of `db.table` or bare table names; empty captures every table. Must be non-empty and fully qualified when `initial_snapshot` is enabled. |
| `start_file` | No | `` (empty) | Cold-start binlog file; empty starts from the server's current coordinate. Must name a file the server still has. |
| `start_pos` | No | `4` | Cold-start binlog position, at least 4 and inside `start_file`. |
| `initial_snapshot` | No | `false` | `'true'` reads the existing rows of `tables` before streaming. See [The initial snapshot](#the-initial-snapshot). |
| `snapshot_lock` | No | `true` | `'true'` takes `FLUSH TABLES WITH READ LOCK` (needs RELOAD) while the snapshot point is read; `'false'` takes `LOCK TABLES ... READ` on the snapshot tables instead (needs LOCK TABLES). |
| `snapshot_lock_wait_s` | No | `10` | How long either snapshot lock may wait, 1 to 3600 seconds. The wait blocks the server's writers, so the source gives up and fails rather than wait longer. |
| `heartbeat_ms` | No | `1000` | Master heartbeat period, 1 to 3600000 milliseconds; bounds cancel and checkpoint latency on an idle server. |
| `on_decode_error` | No | `fail` | `'fail'` stops the source at an event it cannot decode; `'drop'` skips it, losing its changes. See [Decode failures](#decode-failures-and-on_decode_error). |

Every option is read strictly: a value the source cannot use (a boolean other than `'true'` or `'false'`, a number with trailing text, a value out of range) is refused when the job is built, never replaced by the default. Only subtask 0 reads the binlog; the other subtasks of a parallel source stay idle.

Source: the `mysql_cdc_source` lambda in `impls/mysql/src/register_factories.cpp` and `MysqlCdcOptions` in `impls/mysql/include/clink/mysql/mysql_cdc_source.hpp`; the event processing in `impls/mysql/src/mysql_cdc_processor.cpp`, and the checkpoint row, event and statement classes in `impls/mysql/src/mysql_cdc_rules.cpp`.

## The change stream

### Server requirements and the checks at start

- A MySQL server (8.0 or 8.4) writing `binlog_format=ROW` and `binlog_row_image=FULL`. The source checks both at start and refuses a server writing anything else. A MariaDB server is refused at start: its binlog has its own rows, GTID and compressed events, and the decoding is verified against MySQL's only.
- The connecting user needs `REPLICATION SLAVE` and `REPLICATION CLIENT`, and for the snapshot `SELECT` on its tables and either `RELOAD` (`snapshot_lock='true'`) or `LOCK TABLES` (`snapshot_lock='false'`).
- The source reads the server's coordinate with `SHOW BINARY LOG STATUS` and falls back to `SHOW MASTER STATUS`, so it starts on MySQL 8.4, which removed the latter.
- A session can override `binlog_format` and `binlog_row_image` for itself. A change such a session makes to a captured table reaches the binlog as a statement, or as a row image that lacks columns; the source refuses either (see [Decode failures](#decode-failures-and-on_decode_error)).

### Transactions and checkpoints

The source holds checkpoint barriers while it is inside a transaction, from the transaction's GTID event to its `XID` or `COMMIT`, and through the initial snapshot. The runner keeps a held barrier pending and goes on calling the source until it may pass (`Source::barrier_ready`, see [checkpointing](../internals/checkpointing.md#injecting-barriers-into-sources)), so a checkpoint covers whole transactions and a restore never starts inside one. A heartbeat or a rotation never moves the checkpoint's coordinate into a transaction: it moves only while the source is between transactions. A transaction that takes the source a long time to read delays every checkpoint by as long. On a graceful stop the source reads to the end of the open transaction before the final checkpoint.

The checkpoint row holds the binlog coordinate of the last transaction boundary, the server's `@@server_uuid`, and the definition in force of each captured table (see [Schema changes](#schema-changes)).

A restore streams again every transaction after the checkpoint's coordinate: the stream is at-least-once, so a sink sees those transactions twice unless it commits with the checkpoint or writes by key. A restore is strict, and the source never starts from the server's current coordinate in its place:

| On restore | The source |
| --- | --- |
| A row this source did not write (bad length, unknown version, a field out of range, a malformed definition) | Fails as corrupt, naming the field |
| A binlog file the server no longer has (purged, or another server's) | Fails, naming the file |
| A position past the end of its file | Fails, naming the position and the file's size |
| Another server's UUID | Fails: a binlog coordinate names a position only on the server that wrote it |
| A checkpoint taken during the initial snapshot | Fails: the state restored with it would hold part of the snapshot |

The fix in each case is to restore another checkpoint or to start the job afresh. Keep the binlog long enough (`binlog_expire_logs_seconds`) to cover the longest outage plus a checkpoint interval. There is no GTID positioning: after a failover to another server the source refuses its checkpoint.

### The initial snapshot

On a first start with `initial_snapshot='true'`, the source reads each table in `tables` as of a binlog coordinate P, emitting every row as an insert, then streams from P. Every snapshot table must exist and be InnoDB; another engine, or a view, is refused, as only InnoDB gives the consistent-snapshot read view the scan uses.

With `snapshot_lock='true'` the source takes `FLUSH TABLES WITH READ LOCK`, reads P and the tables' definitions, opens a consistent-snapshot transaction and releases the lock. The lock waits for every transaction in flight to commit, so P is exact: no change appears in both the snapshot and the stream, and none in neither. The wait for the lock is bounded by `snapshot_lock_wait_s`.

With `snapshot_lock='false'` there is no global lock: the source takes `LOCK TABLES ... READ` on the snapshot tables while it reads P and their definitions, and releases it as the consistent-snapshot transaction opens. A change committed in that instant can be in both the snapshot and the stream, so the boundary is at-least-once; no change is lost. MySQL writes a transaction to the binlog before it commits in InnoDB, so without any lock P could include a change the view did not show, which neither the snapshot nor the stream would carry: earlier releases took no lock here and could lose such a change.

The source holds every barrier until the snapshot has finished, so no checkpoint covers part of it. A failure during the snapshot restores the state from before it, or none on a first run, and the restarted source reads the snapshot again from the beginning. A graceful stop during the snapshot (a stop with a savepoint) reads the rest of it before the final checkpoint; a cancel does not wait. On every failure the snapshot connection is closed, which ends its lock and its read view on the server.

### Schema changes

With `binlog_row_metadata=FULL` (recommended), each TABLE_MAP names its columns and lists its ENUM and SET members, so every event is decoded with the definition it was written with. Columns added, dropped, renamed, reordered or retyped need nothing, and a restore to a checkpoint before such a change decodes the events on both sides of it with their own definitions.

With `binlog_row_metadata=MINIMAL` (MySQL's default), an event carries its columns' types and signedness but not their names. The source decodes such events with each captured table's definition in force at the coordinate it starts from, never with what `information_schema` says later:

- At a fresh start it reads the definitions at the start coordinate: under the snapshot's lock, or by reading them before and after the coordinate and requiring the two reads to agree.
- Every checkpoint carries them, and a restore takes them from the checkpoint.
- Every TABLE_MAP is checked against the definition in force: its column count, each column's type and its signedness. A disagreement is a schema mismatch, one of the decode faults below.
- A DDL statement that may change a captured table's columns (an `ALTER TABLE` adding, dropping, renaming, modifying or converting columns, a `CREATE TABLE` or `RENAME` into a captured name, a `RENAME TABLE` swap) ends what the source knows of that table's definition. The table's next MINIMAL TABLE_MAP stops the source, whatever `on_decode_error` says, naming the statement's coordinate. The source decodes everything before the statement and nothing of the table after it. Index, constraint, table option, partitioning and column default changes leave the definition alone.

To continue after such a stop, start the source from a new snapshot (a new job, or one whose checkpoints are cleared), which reads the definitions afresh. Setting `binlog_row_metadata=FULL` prevents the stop for later changes; only events written after the setting carry names. Two cases are outside what the source can see. A change made with `sql_log_bin=0` or on another server never reaches the binlog, so a column renamed to another name of the same type there is decoded under its old name. A checkpoint written by an earlier release records no definitions, so on such a restore the source takes today's from `information_schema`, as earlier releases did, and logs a warning. A stream started from `start_file` has no definitions for that coordinate, and its MINIMAL events stop the source.

### Decode failures and `on_decode_error`

The source names each event it cannot handle as one of six faults: an unsupported event, a corrupt row payload, a row event without its TABLE_MAP, a schema mismatch, a partial row image (a session with `binlog_row_image` other than FULL), or a change not logged as rows. The last covers a statement-logged change to a captured table, `TRUNCATE`, `DROP TABLE`, `RENAME TABLE` of a captured table, `DROP DATABASE`, an `ALTER TABLE` that drops, truncates, exchanges, discards or imports a partition or tablespace, and a row change inside an XA transaction (logged at `XA PREPARE`, before the commit or rollback). An XA transaction that changes no captured table passes.

| `on_decode_error` | An event the source cannot decode |
| --- | --- |
| `'fail'` (default) | Stops the source at the event, after passing on every event before it and nothing of it. The error names the fault, the coordinate just after the event, the table and the reason, never row contents or credentials. The job restarts from its last checkpoint and stops at the same event each time until the cause is removed or the table says `'drop'`. |
| `'drop'` | Skips the event and loses its changes. Each is counted in `clink_connector_dropped_events_total` and reported, with its payload (the row image in hex, or the statement), to the job's dead-letter queue under the connector name `mysql_cdc`. The delivery analyser reports the job as at-most-once at its source and refuses a stronger requested guarantee. |

Some events fail the source under either value: an `INCIDENT` event (the server lost events), a MINIMAL TABLE_MAP of a table whose definition the source does not know (see above), and the two event types mariadb-connector-c itself refuses (verified with 3.4.8): a compressed transaction (`binlog_transaction_compression=ON`) and a partial JSON update (`binlog_row_value_options=PARTIAL_JSON`). The library cannot pass either on, so the source cannot skip them; its error names the setting that wrote them, and the fix is to turn that setting off on the server.

On the SQL path one `on_decode_error` on a `mode='cdc'` table governs both the change stream and the JSON bridge in front of it. A resumed job may change it: it is not part of the job's fingerprint.

### Compatibility

- MySQL 8.4 is supported from this release. Earlier releases read `SHOW MASTER STATUS`, which 8.4 removed, so they could start there only from `start_file` or a checkpoint, never from the current coordinate or with a snapshot.
- A MariaDB server is refused at start. Earlier releases accepted one.
- The checkpoint row has a new layout. A row an earlier release wrote restores, with the MINIMAL caveat above; a row this release writes is refused by an earlier release.
- Options are read strictly, where earlier releases used the default in place of a value they could not read.
- `snapshot_lock='false'` now needs the `LOCK TABLES` privilege on the snapshot tables.
- An event earlier releases skipped now stops the source unless the table says `'drop'`: they dropped a row event they could not decode and counted it, and did not check for statement-logged changes, partial row images or XA transactions at all.
- The row decoder refuses a width or precision outside MySQL's limits (earlier releases read it), binary JSON it cannot parse (earlier releases wrote `null`), and an ENUM index or SET member past the definition's members (earlier releases wrote the number, or dropped the member). An ENUM column's error value, index 0, now arrives as the empty string, as MySQL shows it; earlier releases sent `0`.

## The sinks

Both sinks share one core, `impls/mysql/src/mysql_sink_core.cpp`: the session, the table checks, each flush's transaction, the failure state, retries, cancellation and the statement bound. Values, keys and statements in MySQL's dialect are in `impls/mysql/src/mysql_sink_rules.cpp`; the JSON reader, stream keys, the changelog marker, statement packing and the changelog buffer are the record rules the SQL sinks share (`include/clink/connectors/sink_record_rules.hpp`).

### Modes

| Mode | Selected by | Statements | Delivery |
| --- | --- | --- | --- |
| Append | `mysql_sink` | `INSERT INTO t (...) VALUES (...),(...)` | At-least-once: a replay appends its rows again. |
| Insert-or-update | `mysql_sink` with `on_duplicate='update'` | `INSERT ... AS clink_new ON DUPLICATE KEY UPDATE c = clink_new.c, ...` | At-least-once; a replay rewrites rows by key. |
| Changelog upsert | `mysql_upsert_sink`, or SQL `mode='upsert'` | that upsert for `insert` and `update_after`, and `DELETE FROM t WHERE k IN (...)` for `delete` and `update_before` | Effectively-once on the table, for a stable key, a deterministic query and one writer per key. |

A record is one JSON object. A sink writes the members named in `columns`; a member the record lacks is written as NULL, as a null member is, so a column's default does not apply. In the changelog sink a `__row_kind` of `insert` or `update_after`, or none, upserts the row, and `delete` or `update_before` deletes it by key; any other value, or one that is not a string, fails the sink. The append sink refuses `delete` and `update_before` records, which it cannot apply: appended, a retraction would add the row it removes.

### The table each mode needs

When a sink opens, it reads the table's definition from `information_schema` and refuses a table its mode cannot write as declared, naming every problem:

- Every mode: the table exists, every column in `columns` exists and is not generated, and a column in `timestamp_columns` is `DATETIME` or `TIMESTAMP`.
- Insert-or-update: exactly one PRIMARY KEY or UNIQUE index, all of whose columns the sink writes. With none, a replay inserts every row again; with a second, `ON DUPLICATE KEY UPDATE` updates whichever row a new row collides with first, which need not be the row the key names.
- Changelog upsert: a transactional engine (InnoDB); a PRIMARY KEY or UNIQUE index on exactly `key_columns`; no other unique index, since an upsert colliding with it would update the row that index names; and `update_columns`, when set, naming every non-key column, so that a row always holds its key's last record.
- Append: any engine, and a view. On a table without transactions (MyISAM), a failed flush leaves what its statements wrote before the failure, and the flush is not tried again.

Both keyed modes refuse a view, which has no keys to write by, and a unique index on a prefix of a column (`UNIQUE KEY (name(10))`), on which rows whose values differ after the prefix collide.

### Keys and order

The changelog sink buffers changes by the key as the stream writes it: the key members' JSON kinds and texts, with a number taken by its value, so `1`, `1.0` and `1.00` are one key. Within a flush a key's later change replaces its earlier one, and the surviving changes are applied in the order in which each key last changed: consecutive upserts as one `INSERT`, consecutive deletes as one `DELETE`, the runs in order, all in one transaction. Which keys are one row is the table's to decide: two keys the stream holds apart and the table treats as one (`'abc'` and `'ABC'` under a case-insensitive collation, `'a'` and `'a '` under a PAD SPACE collation, `1` and `"1"` in an `INT` column) reach the table in the order they changed, and the row ends as the last change left it. The live suite checks this against a reference table to which MySQL applies each change in turn.

That holds within one subtask. With more than one, two changes to a row reach the table in either order unless they reach the same subtask, so a parallel sink needs key columns that MySQL tells apart exactly as the stream does: an integer type, `DECIMAL`, `VARBINARY`, a `VARCHAR` with a binary NO PAD collation (`utf8mb4_0900_bin`; `utf8mb4_bin` pads), or a `DATETIME` or `TIMESTAMP` of at least millisecond precision written from epoch milliseconds. A table with any other key column is refused when the sink opens. Each record's key must also be written as its column compares, a number for a numeric or timestamp column and a string for a `VARCHAR` or `VARBINARY` one, or the record fails the sink. The SQL planner routes a sink's rows by the key's typed value; with the Dag API, partition the stream by the value the table keys by.

The sinks assume they own the rows they write. One writer per key is a condition of the changelog sink's convergence: a replay rewrites the rows its checkpoint did not cover, and can overwrite a change another writer made to them since. A row another writer inserted is updated by the sink's upsert, not duplicated; a row another writer deleted is inserted again by the sink's next upsert of its key.

### Values

Records are read strictly: one JSON object, valid UTF-8, with no member repeated at the top level and at most 256 levels of nesting; anything else fails the sink. Each value is written as a SQL literal:

| JSON value | Written as |
| --- | --- |
| `null`, or a missing member | `NULL` |
| `true`, `false` | `1`, `0` |
| A string | a quoted literal, escaped by the client library for the connection's character set, `utf8mb4` |
| An object or array | its JSON text, quoted |
| A number | its exact plain decimal (`1.5E3` as `1500`, `18446744073709551615` as written), so a `DECIMAL` or `BIGINT UNSIGNED` column takes it exactly; one longer than 100 characters fails the sink |
| A number, for a `FLOAT` or `DOUBLE` column | the number as written, which the server reads as the nearest value of the type; one past a `DOUBLE`'s range fails the sink |
| A number, for a column in `timestamp_columns` | its UTC time, from epoch milliseconds |

**Timestamps.** The SQL Row path carries a `TIMESTAMP` as epoch milliseconds, and the sink writes a column named in `timestamp_columns` as its time in UTC (`'2026-10-24 17:41:28.105'`) instead of that number. Both sinks run their session with `time_zone = '+00:00'`, so a `DATETIME` column holds the UTC wall time and a `TIMESTAMP` column the same instant whatever the server's time zone. A value that is not a whole number of milliseconds, that lies outside years 1 to 9999, or whose milliseconds the column's fractional precision would round away fails the sink; a `TIMESTAMP` column's narrower range, 1970 to 2038, is the server's to refuse. To store epoch milliseconds in a `BIGINT` column, declare the SQL column `BIGINT`.

**A strict session.** Each sink sets its session's `sql_mode` to `STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION`, whatever the server's global mode (the changelog sink adds `NO_AUTO_VALUE_ON_ZERO`, so a key of 0 in an `AUTO_INCREMENT` column names that row). A value a column cannot hold fails the statement rather than land converted: a string too long for its column, a number out of range, a date that does not exist, a character the column's character set lacks, `NULL` for a `NOT NULL` column. The sink also reads every statement's warnings and fails the flush on any, notes included, so a rounding the column makes (`1.234` into `DECIMAL(10,2)`) is not a successful write. Escaping is the client library's and the session's mode is the sink's, so a server whose global mode is `NO_BACKSLASH_ESCAPES` stores the same bytes.

### Transactions, failures and acknowledgement

A flush is one transaction: `START TRANSACTION`, its statements in order, `COMMIT`. A sink flushes at its count, byte and linger thresholds and at every checkpoint barrier, and passes the barrier on, so acknowledging the checkpoint, only after that flush has committed. A checkpoint therefore completes only once every row the sink received before it is committed, and a restored job writes the rows after its checkpoint again.

A failure fails the sink. A failed sink refuses every later record, barrier and flush, an empty flush included, until it is opened again, which the job's restart does before it replays from its last checkpoint:

| Failure | What the table holds | What the error says |
| --- | --- | --- |
| A record that cannot be read or written as declared, or one too large for a statement | Nothing of the record: it fails the sink before it is buffered | Which member, and why, without its value |
| A statement the server refuses, or one that raises a warning | Nothing of the flush, which is rolled back (on a table without transactions, what its earlier statements wrote stays) | The server's code and message, with the values it quotes replaced by `<value>` |
| A deadlock or lock wait timeout | Nothing: the transaction is rolled back and tried again up to `max_retries` times, after 100 ms, then twice as long each time, up to 5 s; then the sink fails | The last error and the retries spent |
| The connection lost before `COMMIT` | Nothing of the flush | That nothing was committed |
| `COMMIT` refused by the server | Nothing of the flush | That nothing was committed |
| `COMMIT` sent and its reply lost | Unknown: the flush may or may not have been committed | That the outcome is unknown; the job replays, so a committed flush is written twice by the append sink and rewritten by key otherwise |
| The task cancelled | The statement in flight is ended and the server rolls its transaction back; a cancel during `COMMIT` is the unknown outcome above | That the task was cancelled |

A statement waits for a lock for at most `lock_wait_timeout_s`, and the connection's read and write timeouts are that plus 60 seconds, so a server that stops answering fails the flush rather than holding it. A cancel ends a statement in flight, or a wait between tries, within about 20 ms.

`close()` never throws: it discards what is buffered (a restored job writes it again), rolls back a transaction left open and releases the connection. A clean end of input flushes before `close()`; a cancelled task does not. Neither sink is two-phase commit, and neither is exactly-once.

### Bounds

| Bound | Option | Default |
| --- | --- | --- |
| Rows buffered (append), or keys buffered (changelog) | `batch_records` | 1000 |
| SQL text buffered | `max_bytes` | 16 MiB |
| One record's row or key, as SQL text | `max_record_bytes` | 1 MiB |
| One statement | `max_statement_bytes`, and the server's `max_allowed_packet` less 1024 bytes | 4 MiB |
| One wait for a lock | `lock_wait_timeout_s` | 30 s |
| Tries after a deadlock or lock wait timeout | `max_retries` | 3 |

The thresholds are checked as each record is buffered, so a large batch flushes part-way through. The byte bounds count the SQL text the records render to, escapes included, which is what the server receives; they are not a bound on the sink's memory, which also holds each buffered key's bookkeeping in the changelog sink. A flush longer than one statement is split into several, all in its one transaction, and a record longer than one statement can hold fails the sink before it is buffered.

### Changes from earlier releases

The sinks were rewritten in this release. Against earlier releases:

- A failed flush fails the sink until it is opened again. Earlier releases cleared the rows they had buffered and let a later empty flush, and so a checkpoint, succeed.
- `close()` discards what is buffered. Earlier releases flushed it, even when the task was cancelled.
- The changelog sink applies a flush's changes in the order each key last changed. Earlier releases applied every upsert before every delete, which left the wrong row when two keys the table treats as one changed in turn.
- An unknown `__row_kind`, or one that is not a string, fails the changelog sink, where earlier releases upserted the record; the append sink refuses retractions, which earlier releases appended.
- The sinks check the table when they open and refuse one their mode cannot write: no unique index, a second unique index or a prefix index for either keyed mode, no transactions for `mysql_upsert_sink`, and `update_columns` that leave out a non-key column. A parallel `mysql_upsert_sink` refuses key columns MySQL does not tell apart as the stream does.
- Numbers are written exactly. Earlier releases wrote a decimal through a double, losing digits past the seventeenth, and refused an integer outside the signed 64-bit range.
- The session is strict, and a statement's warnings fail the flush. Earlier releases wrote under the server's mode, so a server that was not strict stored converted values.
- Options are read strictly: an `on_duplicate` other than `'update'`, `update_columns` without it, and a number out of range are refused. `batch_records` must be at least 1, where earlier releases took `0` as 1, and `max_bytes='0'` means the 16 MiB default, where earlier releases took it as no byte threshold. `max_bytes` counts SQL text rather than record bytes.
- Errors no longer include records. Earlier releases put the whole record in the message.
- A deadlock or lock wait timeout is tried again, up to `max_retries` times, where earlier releases failed the flush.
- The capability records state MySQL 8.0.19 or later for the insert-or-update and upsert statements, whose row alias has always needed it; earlier records said MySQL 5.7.
- A SQL sink table takes only the options its sink reads (see [SQL usage](#sql-usage)).

## SQL usage

The connector is mapped under `connector='mysql'` in `src/sql/physical_plan.cpp`. The source factory is selected by the `mode` property: `mode='cdc'` selects `mysql_cdc_source`, otherwise `mysql_source`. A sink table maps to `mysql_sink`, or with `mode='upsert'` to `mysql_upsert_sink`.

Source table (incremental cursor):

```sql
CREATE TABLE orders (
    id BIGINT,
    customer VARCHAR,
    amount DOUBLE
) WITH (
    connector = 'mysql',
    host = 'db.internal',
    port = '3306',
    user = 'reader',
    password = 'secret',
    database = 'shop',
    table = 'orders',
    cursor_column = 'id',
    batch_size = '500',
    poll_ms = '1000'
);
```

Source table (binlog CDC):

```sql
CREATE TABLE orders_cdc (
    id BIGINT,
    customer VARCHAR,
    amount DOUBLE
) WITH (
    connector = 'mysql',
    mode = 'cdc',
    host = 'db.internal',
    user = 'repl',
    password = 'secret',
    server_id = '42',
    tables = 'shop.orders'
);
```

Sink table:

```sql
CREATE TABLE orders_out (
    id BIGINT,
    customer VARCHAR,
    amount DOUBLE
) WITH (
    connector = 'mysql',
    host = 'db.internal',
    user = 'writer',
    password = 'secret',
    database = 'shop',
    table = 'orders_out',
    on_duplicate = 'update'
);
```

On the SQL path, `columns` defaults to the declared table schema, so it does not need to be repeated. `mode='upsert'` with a PRIMARY KEY (or `primary_key='...'`) selects the changelog sink, `mysql_upsert_sink`, which lets a retracting query (a GROUP BY, TOP-N or outer join) maintain the table; `on_duplicate='update'` writes an append-only query's rows by key on the default sink. A query that produces a changelog into a table without `mode='upsert'` is refused when it is planned. Exactly-once (XA two-phase commit) delivery is not supported.

A sink table takes only the options its sink reads: the connection options, the sink's options above, the source options (a table can be read and written), and the options the planner interprets (`connector`, `format`, `mode`, `primary_key` and the like). The planner sets `key_columns` from the PRIMARY KEY and `timestamp_columns` from the `TIMESTAMP` columns, so a table that sets either is refused, as is one with `mode='upsert'` that sets `on_duplicate`, `update_columns` or `linger_ms`, and one that sets any other option. With more than one subtask the changelog sink takes the key rules in [Keys and order](#keys-and-order).

A NaN or infinite double in a row reaches this sink through the `row_to_json_string` bridge as `null`, or as `"NaN"`, `"Infinity"` or `"-Infinity"` when the sink table sets `json_non_finite='string'`; the sink handles it as it handles any null or string value. See [NaN and infinity in JSON output](README.md#nan-and-infinity-in-json-output).

## Example

The sink and cursor source can be constructed directly from their typed Options, as the live test does in `impls/mysql/tests/test_mysql_live.cpp`:

```cpp
#include "clink/mysql/mysql_sink.hpp"
#include "clink/mysql/mysql_source.hpp"

using namespace clink::mysql;

ConnectOptions conn;
conn.host = "db.internal";
conn.user = "writer";
conn.password = "secret";
conn.database = "shop";

// Sink: batched INSERT of JSON-object rows.
MysqlSinkOptions sink_opts;
sink_opts.conn = conn;
sink_opts.table = "orders_out";
sink_opts.columns = {"id", "val"};
sink_opts.upsert = true;  // INSERT ... ON DUPLICATE KEY UPDATE
MysqlSink sink(std::move(sink_opts));
sink.open();
// sink.on_data(batch); ... flushed on count/byte/linger thresholds and barriers.

// Source: incremental cursor SELECT.
MysqlPollOptions src_opts;
src_opts.conn = conn;
src_opts.table = "orders";
src_opts.cursor_column = "id";
src_opts.batch_size = 500;
auto source = make_mysql_poll_source(src_opts);
```

The factories are also reachable through the registry on the `string` channel as `mysql_sink`, `mysql_upsert_sink`, `mysql_source` and `mysql_cdc_source` once `clink::mysql::install(registry)` has run.

## Delivery semantics

- `mysql_sink`: at-least-once. Each flush is one transaction, committed before the barrier that asked for it is passed on, and a restored job writes again the rows after its checkpoint: a plain insert appends them a second time, and `on_duplicate='update'` rewrites them by key. The recovery cells show both sides: killed before a flush's `COMMIT`, no record ends in two rows; killed after it and before its checkpoint completed, exactly the records committed after the restored checkpoint do. A failure fails the sink until it is opened again, and a `COMMIT` whose reply is lost leaves its flush's outcome unknown. Not two-phase commit; exactly-once is not supported.
- `mysql_upsert_sink` (`mode='upsert'`): effectively-once on the table for a stable key, a deterministic defining query and one writer per key. A flush's changes are applied in the order each key last changed, in one transaction, so a replay converges the table to the same rows whichever keys the table treats as one. Not two-phase commit. See [The sinks](#the-sinks).
- `mysql_source`: at-least-once. The cursor is checkpointed as operator state (via `PollingSource`) and replayed on restart; the exclusive `>` comparison avoids re-emitting the boundary row. A NULL cursor or id value in a delivered row is fatal rather than silently stalling the cursor.
- `mysql_cdc_source`: at-least-once. The checkpoint holds the binlog coordinate of the last transaction boundary, and the source holds barriers inside a transaction and through the initial snapshot, so a restore replays whole transactions from that coordinate and never part of a snapshot. An event the source cannot decode stops it by default; with `on_decode_error='drop'` it is skipped and reported, and the job is at-most-once at its source. The snapshot-to-stream boundary is exact with `snapshot_lock='true'` and at-least-once with `snapshot_lock='false'`. See [The change stream](#the-change-stream).

## Limitations

- Each sink must be the only sink on its chain: at every checkpoint barrier it writes out what it received before the barrier, which holds only while it owns its chain's checkpoint, so `Dag::add_sink` refuses another sink beside it, whichever is added first. SQL and job graphs plan every sink as a subtask of its own; with the Dag API, put the other sink in a separate Dag (see [checkpointing](../internals/checkpointing.md#guarantees-and-caveats)).
- The cursor source uses the MySQL text protocol, so every cell is delivered as a string; numeric and date typing depends on the downstream Row bridge coercing to declared column types.
- A VARCHAR `cursor_column` orders lexically (so `'10' < '9'`); choose a numeric or zero-padded key.
- A non-unique `cursor_column` can drop rows at a page boundary unless `id_column` is set as a unique tie-breaker.
- `mysql_cdc_source` requires `binlog_format=ROW` and `binlog_row_image=FULL`, and supports MySQL servers only (8.0 and 8.4 tested).
- A binlog stream is single-reader: only subtask 0 streams in a parallel CDC job; other subtasks emit nothing.
- A checkpoint waits for an open transaction to end and for the initial snapshot to finish, and a failure during the snapshot reads it again from the beginning: there is no resumable snapshot.
- With `binlog_row_metadata=MINIMAL`, a change to a captured table's columns stops the change stream; there is no schema history. Compressed transactions, partial JSON updates and XA transactions on captured tables are not supported, and binlog coordinates do not survive a failover to another server.
- CDC `initial_snapshot` requires `tables` to be a non-empty list of existing, fully-qualified InnoDB tables.
- `mysql_sink` does not implement clink `mode='upsert'`: on a sink table, `mode='upsert'` selects `mysql_upsert_sink` instead. For idempotent insert-or-update on the append sink, use `on_duplicate='update'`.
- Neither sink is exactly-once or two-phase commit: the append sink writes replayed rows again, and a `COMMIT` whose reply is lost leaves its flush's outcome unknown.
- The sinks' insert-or-update and changelog statements need MySQL 8.0.19 or later; the sinks are tested against MySQL 8.0 and 8.4, and a MariaDB server is not supported.
- `mysql_upsert_sink` needs a transactional engine and one unique index, on exactly its key; with more than one subtask its key columns must be of the types in [Keys and order](#keys-and-order). It converges only with one writer per key.
- The sinks' byte bounds count SQL text, and are not a bound on their memory.
- The channel is `std::string` only (JSON-object rows); there is no typed int64 channel and no Arrow or Parquet encoding on this connector's data path.

## Testing

In-process tests cover factory registration and options, the polling source's SQL, row decoding and snapshot logic without a server (`impls/mysql/tests/test_factory_registration.cpp`, `test_mysql_sink_logic.cpp`, `test_mysql_source_logic.cpp`, `test_mysql_cdc_decode.cpp`, `test_mysql_snapshot.cpp`). The change stream's rules (`test_mysql_cdc_rules.cpp`: the checkpoint row and its strict reading, event and statement classes, the transaction tracker, TABLE_MAP metadata, definitions) and its event processing (`test_mysql_cdc_processor.cpp`: boundaries, both decode policies, partial images, definitions in force, DDL, XA, unsupported events) run from events built by hand. The sinks' MySQL rules (`test_mysql_sink_rules.cpp`: values, key spelling, statement heads, message redaction, the table checks) and the record rules the SQL sinks share (`tests/test_sink_record_rules.cpp` in `clink_core_tests`: the JSON reader, exact numbers, stream keys, the changelog marker, statement packing, the changelog buffer) run without a server, and their failure state runs against a scripted server (`test_mysql_sink_failures.cpp`, `MysqlSinkFailures`): every failure followed by another record, barrier and flush, reopening, refused records and markers, a failed statement, a lost and a refused `COMMIT`, deadlock and lock wait retries, a table without transactions, warnings, a cancel during a statement and during `COMMIT`, `close()`, statement splitting and the packet limit, the byte threshold, order and netting in the changelog buffer, and the identities a sink refuses.

The live suites are skipped unless `CLINK_MYSQL_TEST_DSN` names a server, a space-separated `key=value` DSN:

```bash
export CLINK_MYSQL_TEST_DSN="host=localhost port=3306 user=root password=mysql database=test"
```

`test_mysql_cdc_live.cpp` (`MysqlCdcLive`) covers the change stream's round trips and `test_mysql_cdc_contract_live.cpp` (`MysqlCdcContractLive`) its contract: signedness after YEAR and BIT columns under both metadata modes, long CHAR columns, transactions larger than one `produce()` call, checkpoints inside and after them, rotation, purged, corrupt, foreign and earlier-release checkpoints, statement-logged changes, partial row images, servers without row logging, compressed transactions, partial JSON updates, XA, TRUNCATE, column changes under MINIMAL and FULL metadata and restores across them, the snapshot under concurrent writes with and without the lock, the snapshot's lock wait, a failed snapshot's connection, and snapshot table checks. Each case reads its expectation from the server, not from what the source emitted.

`test_mysql_sink_contract_live.cpp` (`MysqlSinkContractLive`) covers the sinks' contract against the server. Keys the table treats as one (a case- and accent-insensitive collation, trailing spaces under PAD SPACE and NO PAD collations, numbers written as strings, with decimals and with exponents) are applied in one flush and in a flush per change, and the table is compared row for row with a reference table to which MySQL applied each change in turn; composite and timestamp keys are checked too. The suite also covers the tables each mode refuses (two unique indexes, none, a prefix index, MyISAM for the changelog sink, partial `update_columns`, a case-insensitive key at parallelism 2, a generated column, a missing table); exact values (`BIGINT UNSIGNED`'s maximum, `DECIMAL(65,30)`, characters outside the Basic Multilingual Plane, quotes and backslashes, NUL and control bytes, JSON, `DATETIME` and `TIMESTAMP` at their limits, a large `DOUBLE`, null and missing members); values a column cannot hold, each failing its flush with nothing committed, the sink writing again once opened; a server whose global mode is `NO_BACKSLASH_ESCAPES` and not strict; 40 statements rolled back as one flush; statements kept under a 64 KiB `max_allowed_packet`; another session's lock failing a flush within its bound; a cancel ending a flush that waits for a lock; a connection killed while its `COMMIT` is on its way, reported as unknown; and the factories' sinks writing a table.

The recovery cells run a coordinator and two workers, and read their results through the mysql client in the server's container, never from what clink reports. The change stream's (`tests/integration/test_mysql_cdc_recovery.cpp`, `MysqlCdcRecovery`) read the binlog into the file two-phase-commit sink, one of them a SQL job, and kill a worker inside the initial snapshot and inside a 20000-row transaction. They check that the coordinator triggered checkpoints and none completed while the snapshot or transaction was open, and that the committed output equals the table, with every row committed once. The sinks' (`tests/integration/test_mysql_sink_recovery.cpp`, `MysqlSinkRecovery`) poll a feed the test serves into each sink and kill a worker at `mysql.sink_before_commit`, with a flush's statements executed and not committed, and at `mysql.sink_after_commit`, with the flush committed and its checkpoint not complete. For the append sink, every record's row is present with its columns, and the records with two rows are exactly those committed after the checkpoint the job restored: none when the kill came before `COMMIT`, about one checkpoint interval's when it came after. For the upsert sink, and for a SQL job writing it at parallelism 2 through a binary NO PAD key, the table holds every row the input's last records leave, column for column, and none of the rows they delete. Each cell checks the single restart, the replay, and the fault firing where it names, and before `COMMIT` that the server held the flush's rows in an open transaction.

`scripts/mysql-live.sh` runs all of this against MySQL 8.0.46 and 8.4.13 (pinned by digest), one server after the other, in two phases on each: `cdc`, the change stream's suites and recovery cells, and `sink`, the sinks'. It fails on any skipped case, a suite that ran short, a case outside both phases, a missing binary or a server that never answers. It keeps each phase's reports and the cells' logs, with a provenance file (commit, build configuration, client library, each server's version, image and binlog settings, each phase's result), without credentials. The `mysql` CI workflow runs it.
