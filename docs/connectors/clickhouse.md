# ClickHouse

> Connects to a ClickHouse server over the native (TCP) protocol. A source (a bounded `SELECT`) and two sinks: a text sink that sends rows as TSV or JSONEachRow, and a native sink that sends typed Native blocks for high-volume ingest from SQL.

## Overview

The ClickHouse connector integrates a ClickHouse server through the `clickhouse-cpp` native client.

- The text sink, `clickhouse_sink`, batches incoming `std::string` records and sends each batch as one `INSERT` statement with its rows inline, each record one row of TSV or JSONEachRow. It is the default for SQL tables.
- The native sink, `clickhouse_native_sink`, takes SQL Rows, converts them into typed columns that match the target table, and streams them as compressed Native blocks through the client's `BeginInsert` / `SendInsertBlock` / `EndInsert` calls, with a dedicated writer thread per subtask, retries inside the INSERT and bounded backpressure. SQL tables choose it with `insert_format='native'`. It is opt-in in this release.
- The source executes a single `SELECT` statement and drains the result blocks. It emits either typed `ClickHouseRow` records (column names plus stringified cell values) or, on the string channel, each row flattened to a delimiter-joined string or to a JSON object keyed by column name. Cell values are kept as text so the source stays schema-agnostic, with downstream operators parsing to concrete types.

Both sinks are at-least-once. The native sink checks the server and the target table before it writes, and refuses configurations under which that guarantee would not hold. Between the declared column type and the ClickHouse column it never truncates, floors or silently casts a value: a value the target column cannot hold fails the row with the column named. How a Row cell becomes a value of its declared type follows the engine's own rules (see [Type mapping](#type-mapping)).

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| `clickhouse-cpp` | Built from source by `scripts/build-clickhouse-cpp.sh`, on the host into `CLINK_DEPS_PREFIX/clickhouse-cpp` and in the image into `/usr/local`: static, Release whatever the build type, with TLS (OpenSSL). Homebrew's or the system's copy is the fallback, and the configure log names the version it took | `2.6.2` (`CLICKHOUSE_CPP_VERSION` in `scripts/versions.env`, checksum-pinned) |
| cityhash | Bundled with the pinned client | the client's own |
| lz4, zstd, abseil | System packages, the copies the rest of the binary links | not pinned by clink |
| OpenSSL | System. Required by the pinned client: without it CMake skips the pinned copy and falls back to the system's client | not pinned by clink |

The text sink and the source do not use Arrow; records cross the connector boundary as `std::string` (or typed `ClickHouseRow`). The native sink converts each Row batch into an Arrow chunk on the task thread and each chunk into Native columns on its writer thread.

## Enabling it

Controlled by the `CLINK_WITH_CLICKHOUSE` CMake option (`AUTO` / `ON` / `OFF`, default `AUTO`). Under `AUTO` the `clink::clickhouse` target is defined only if `clickhouse-cpp` is found (via `find_package(clickhouse-cpp CONFIG)` or by locating the headers and `clickhouse-cpp-lib`); with `ON` a missing client is a fatal configure error; `OFF` skips the target entirely. When the target is built, it compiles with `CLINK_HAS_CLICKHOUSE` defined; without the client the text sink and source throw on construction.

The pinned client is built by `scripts/build-clickhouse-cpp.sh` on both the host and the Debian image (`scripts/install-system-deps.sh` calls it). It is built in Release even when the rest of the toolchain is Debug, because the encoding cost of a Debug client would be in every throughput figure.

```bash
cmake -S . -B build -DCLINK_WITH_CLICKHOUSE=ON
```

Two further conditions apply to the native sink, both decided at configure time:

- It needs clickhouse-cpp 2.6.2 or later. Against an older client, or one whose version the configure step cannot read, only its registration builds, and the factory refuses every job with `clickhouse.native_unavailable`. The capability record says so in its limitations.
- Its TLS path needs a client built with OpenSSL. The configure step compiles and links a small program against the chosen client to find out; when that fails, `secure='true'` refuses `clickhouse.tls_unavailable`. The pinned client always passes.

By distribution: a host build with the pinned client and the runtime image (built on the toolchain image, which installs the pinned client) carry the native sink with TLS. The pyclink wheels do not include the ClickHouse connector.

## Factories

| Factory name | Direction | Record type |
| --- | --- | --- |
| `clickhouse_sink` | sink | `std::string` |
| `clickhouse_native_sink` | sink | SQL `Row` (Native blocks; capability record `clickhouse_native`) |
| `clickhouse_row_source` | source | `ClickHouseRow` |
| `clickhouse_text_source` | source | `std::string` (columns joined by `delim`) |
| `clickhouse_source` | source | `std::string` (JSON object keyed by column name) |

The connector also registers the `ClickHouseRow` typed channel (`kChannelClickHouseRow`) with its codec so rows can travel end to end across the cluster without flattening.

The native sink is built for SQL: it needs the declared SQL type of every column, which the planner passes as an internal parameter, `sql_column_types`. A Dag-direct or plugin job can reach the factory by name only if it passes that parameter in the planner's spelling; without it the factory refuses `clickhouse.option_invalid` with "the native sink is built from SQL; a Dag-direct job must pass sql_column_types".

## Configuration

### Text sink (`clickhouse_sink`)

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `table` | yes | (none) | Target table name, quoted as an identifier in the INSERT. Construction fails if empty. |
| `host` | no | `localhost` | ClickHouse server host. |
| `port` | no | `9000` | Native protocol port, an integer from 1 to 65535. |
| `database` | no | `default` | Target database, quoted as an identifier in the INSERT. |
| `user` | no | `default` | Auth: username. |
| `password` | no | `` (empty) | Auth: password. |
| `format` | no | `tsv` | Row encoding, matched without regard to case: `tsv` for TSV, `json` or `jsoneachrow` for JSONEachRow. Any other value is sent as TSV, and the sink logs a warning naming it at open. |
| `batch_rows` | no | `1000` | Buffered rows that force a flush. A positive integer. |
| `batch_interval_ms` | no | `1000` | A batch that arrives at least this many milliseconds after the last flush triggers one. There is no timer, so rows wait for the next batch, barrier or close. A positive integer. |
| `connect_timeout_ms` | no | `5000` | Connection timeout in milliseconds, from 1 to 600000. |
| `send_timeout_ms` | no | `30000` | Socket send timeout in milliseconds, from 1 to 600000. |
| `receive_timeout_ms` | no | `30000` | Socket receive timeout in milliseconds, from 1 to 600000. A flush waiting on a server that has stopped answering fails after this long instead of holding the checkpoint. |

The integer options are parsed strictly. The whole value must be the number, and a value that is malformed or out of range refuses the job at deploy with a message naming the key; it does not fall back to the default.

Every INSERT names its own settings instead of taking them from the user's profile:

```sql
INSERT INTO `analytics`.`events`
SETTINGS async_insert=0, wait_for_async_insert=1, insert_deduplication_token='...'
FORMAT JSONEachRow
```

`async_insert=0` and `wait_for_async_insert=1` make the INSERT synchronous, so a flush returns only once its rows are in the table. The deduplication token is fresh for every statement, a resend after a failed flush included, and its random part is drawn again at each open, so a token never repeats one from an earlier run. On a replicated table, two batches with identical rows therefore both land; without a token the server would drop the second as a duplicate of the first. The same token makes a replayed batch land again (see [Delivery semantics](#delivery-semantics)).

### Native sink (`clickhouse_native_sink`)

Every option is parsed strictly at deploy. Integers must be the whole value, booleans are exactly `true` or `false`, and a malformed or out-of-range value refuses the job with `clickhouse.option_invalid`, naming the key. Any value may be written as `env://VAR`, which is read from the worker's environment; a variable that is unset or empty refuses `clickhouse.secret_unset`, and a refusal of a value read that way names the reference, never its contents.

| Option | Required | Default | Accepted values |
| --- | --- | --- | --- |
| `table` | yes | (none) | Target table name, not empty. The table must already exist; the sink does not create tables. |
| `host` | no | `localhost` | Server host, not empty. |
| `port` | no | `9000`, or `9440` when `secure='true'` | Native protocol port, 1 to 65535. |
| `endpoints` | no | (none) | Comma-separated `host:port` list of replicas of one shard, at most 16 entries, each port 1 to 65535. Write an IPv6 address in brackets, `[addr]:port`. Setting it together with `host` or `port` refuses `clickhouse.option_conflict`. |
| `database` | no | `default` | Target database, not empty. |
| `user` | no | `default` | Username. |
| `password` | no | (empty) | Password. Never logged; the options line shows only `set` or `unset`. |
| `insert_format` | no | (none) | Absent or `native`. SQL sets it to choose this sink. |
| `secure` | no | `false` | `true` or `false`. TLS on the native port. |
| `tls_ca_file` | no | (none) | Path of a CA certificate file, not empty. Only that CA is trusted. Needs `secure='true'`, or refuses `clickhouse.option_conflict`. |
| `tls_ca_dir` | no | (none) | Path of a directory of hashed CA certificates, not empty. Only those CAs are trusted. Needs `secure='true'`. |
| `tls_verify` | no | `true` | `true` or `false`. `false` skips certificate verification. Needs `secure='true'`. |
| `batch_rows` | no | `1048449` | Rows at which an INSERT closes, 1 to 2147483647. The default equals ClickHouse's default `min_insert_block_size_rows`; the sink does not read it from the server. |
| `batch_bytes` | no | `67108864` (64 MiB) | Converted payload at which an INSERT closes, 1048576 (1 MiB) to 1099511627776 (1 TiB). Reduced at open to fit a memory budget (see [Sizing](#sizing)). |
| `batch_interval_ms` | no | `1000` | Milliseconds after an INSERT's first row at which it closes, 1 to 3600000. A timer, so an idle stream still flushes. |
| `compression` | no | `lz4` | `lz4`, `zstd` or `none`, for the blocks on the wire. |
| `connect_timeout_ms` | no | `5000` | TCP connect timeout, 1 to 600000. |
| `send_timeout_ms` | no | `30000` | Socket send timeout, 1 to 600000. |
| `receive_timeout_ms` | no | `30000` | Socket receive timeout, 1 to 600000. Also bounds each metadata read at open, rounded up to whole seconds and held between 1 s and 10 s. A server silent for longer than this during an INSERT fails the attempt in doubt (see [Retries, timeouts and cancel](#retries-timeouts-and-cancel)). |
| `retry_window_ms` | no | `600000` (10 min) | How long one INSERT, or the open, keeps retrying a transient failure, 1000 to 86400000. |

`tls_server_name` is not accepted. Any key outside this table and the tolerated keys below refuses `clickhouse.unknown_option`, listing the accepted options.

Tolerated keys are accepted and ignored, because the SQL planner, the materialised-view code or the ClickHouse source puts them on the sink's operator: `batch_size`, `bounded`, `changelog`, `columnar_decode`, `commit_group`, `decimal_columns`, `definition_sql`, `delim`, `delivery_guarantee`, `forced_singleton`, `freshness`, `freshness_ms`, `mode`, `partition_by`, `primary_key`, `query`, `refresh_arm`, `schema_columns`, `state_ttl`, `state_ttl_domain`, `view_kind`, `watermark_delay_ms`, `write_mode`. Four of them are read, because a value would change the guarantee, and are refused at the factory as well as by the planner: `mode` other than `append`, a `delivery_guarantee` stronger than `at_least_once`, `changelog='true'` and `write_mode='overwrite'` all refuse `clickhouse.delivery_unsupported`. An unrecognised `delivery_guarantee`, or a `changelog` other than `true` or `false`, refuses `clickhouse.option_invalid`.

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

The SQL source binds to the `clickhouse_source` factory (each row arrives as a JSON object keyed by column name and is bridged to a Row table via `json_string_to_row`).

A SQL sink table chooses its sink with `insert_format`:

- absent or `insert_format='jsoneachrow'` (the default): `clickhouse_sink`, with `format=jsoneachrow` forced by the binding, since each Row is serialised to a JSON object before insertion;
- `insert_format='native'`: `clickhouse_native_sink`, which takes the Rows directly. The planner passes the declared type of every column with them.

Any other value is refused at `CREATE TABLE`, and so is `insert_format` on a connector other than `clickhouse` ("insert_format applies to connector='clickhouse' only; this table's connector is '...'"), because there it would do nothing.

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

-- Text sink: INSERT rows as JSONEachRow
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

-- Native sink: typed Native blocks
CREATE TABLE events_native (
  id     BIGINT,
  name   VARCHAR,
  ts     TIMESTAMP(3),
  amount DECIMAL(18, 2)
) WITH (
  connector     = 'clickhouse',
  insert_format = 'native',
  endpoints     = 'ch-1.internal:9000,ch-2.internal:9000',
  database      = 'analytics',
  table         = 'events',
  user          = 'writer',
  password      = 'env://CH_PASSWORD'
);
```

For the native sink the SELECT's columns bind by position to the table's declared columns, and the declared columns bind by exact, case-sensitive name to the ClickHouse table's columns (see [Column matching](#column-matching)). The native sink writes typed columns, so the table must declare them; a table read as one text column refuses with "connector='clickhouse' insert_format='native' writes typed columns: declare the table's columns, or format='json'".

A script with two INSERTs from one source runs them as two jobs, each reading the source itself, so one source table feeding a native ClickHouse table and another sink never puts the two sinks on one chain. The source is read once per job, with its own connection and its own checkpoints.

Both sinks append only. `mode='upsert'` and exactly-once delivery are rejected at planning time for either. A materialised view over a keyed `GROUP BY` gets `mode='upsert'` automatically when it sets no mode, so a ClickHouse backing table for such a view must set `mode='append'` itself. Every updated aggregate then lands as a new row, and collapsing them to the latest per key is left to ClickHouse (a `ReplacingMergeTree` keyed on the group columns, for example). A full-refresh materialised view cannot use the native sink: each refresh would append the whole result again, and the planner refuses it. The planner also refuses `changelog='true'` on a native table.

## Native sink

### What it guarantees

At-least-once, under the conditions below:

- Every row covered by a completed checkpoint is in the target at least once after automatic recovery. The barrier returns only after every INSERT of the interval has been acknowledged.
- Rows after the last completed checkpoint are replayed, and may appear twice.
- An in-doubt INSERT resent with a fresh token may appear twice. One resent with its own token is deduplicated only on targets that keep a deduplication log (Replicated* by default, or MergeTree with `non_replicated_deduplication_window` > 0) and on the lines where pin P4 passes, 26.3 and 26.8.

The conditions, each enforced rather than assumed:

- Aligned checkpoints. The sink refuses unaligned and adaptive checkpoints (see [Topology and checkpoint limits](#topology-and-checkpoint-limits)).
- The sink alone on its chain. `Dag::add_sink` refuses another sink beside it.
- A synchronous INSERT. Every INSERT sends `async_insert=0, wait_for_async_insert=1`, and the sink refuses a target whose table or server settings would make it asynchronous anyway.
- For a Distributed target, a synchronous write to the shards. Every INSERT sends `distributed_foreground_insert=1`, so the rows are on the shards when the INSERT is acknowledged. A Distributed table does not deduplicate, so every resend into one counts as possibly duplicated.

A token is kept on a resend only when the server taking the resend is on 26.3 or 26.8 and is the same server (endpoint, name and version) that took the previous attempt. Otherwise the resend takes a fresh token, which can duplicate but never lose. The close summary counts both cases: `rows_resent_with_token` for rows resent under their own token on a target that keeps a log, and `rows_maybe_duplicated` for every other row resent after it was sent. A `ReplacingMergeTree` keyed by the row's natural key absorbs duplicates on the ClickHouse side.

Server lines: 26.3 and 26.8 are tested. Any other line that has the settings the sink sends is accepted with a warning at open, and on it a resent INSERT always takes a fresh token.

### What it checks at open

All of this happens before the first INSERT:

1. The checkpoint mode, and the memory budget against the batch size. Neither needs a server, so neither waits out an outage.
2. The server's settings: every setting the sink sends with an INSERT must exist on the server, and `min_insert_block_size_rows` and `min_insert_block_size_bytes` must read as whole numbers, because the sink pins them.
3. The target table: it must exist, and its engine must be a MergeTree or ReplicatedMergeTree family table, Distributed, or Null.
4. The effective `async_insert`: the table's own setting if it has one, otherwise the server-wide default in `<merge_tree>` (or `<replicated_merge_tree>` for a Replicated table, read from `system.replicated_merge_tree_settings`). Behind a Distributed table this is checked for the local table on every replica of the cluster, each replica named as `host:port`. A cluster argument written with macros, such as `'{cluster}'`, is expanded from `system.macros` first, as the server expands it.
5. The deduplication window, reported, never required.
6. The column plan: every declared column against the target's columns and types.

Steps 2 to 5 run again on every new client the writer builds after a failure, because a reconnect may reach another replica or a server restarted into another version. The columns are not planned again; instead every INSERT's header is checked against the plan made at open (`clickhouse.header_drift`).

Every metadata read carries `max_execution_time` with `timeout_overflow_mode='throw'`, so a slow system table cannot hold the open. The limit is `receive_timeout_ms` rounded up to whole seconds, held between 1 s and 10 s. The reads across a Distributed table's cluster also send `skip_unavailable_shards=0`, so an unreachable replica fails the read, and is retried, rather than being left out of it.

The open runs inside the retry window, which starts when the open does. A connection failure, a timeout or an unreachable replica behind a Distributed target is retried with backoff, rotating through `endpoints` on a connect failure, until `retry_window_ms` runs out, so an outage at open shorter than the window costs nothing. An unknown server code is retried three times and fails on its fourth failure, as during an INSERT. A refusal is never retried: it rests on what a server said or on the options, which a retry does not change. A refusal fails the subtask, which spends a restart; the restarted subtask refuses again, so the job fails once its restart budget is spent. Each refusal counts in `clink_clickhouse_refusals_total{reason=<code>}`.

### Refusals and how to fix them

Every refusal and permanent failure is a `NativeSinkError` whose message starts with its code in brackets, for example `[clickhouse.target_async_insert]`.

At deploy, from the options:

| Code | Cause | Fix |
| --- | --- | --- |
| `clickhouse.unknown_option` | A key the sink neither reads nor tolerates, such as `tls_server_name` or a misspelling. | Correct or remove the key. The message lists the accepted options. |
| `clickhouse.option_invalid` | A value that is malformed or out of range; a missing `table`; an `insert_format` other than `native`; a missing or unparsable `sql_column_types`. | Use a value from the [option table](#native-sink-clickhouse_native_sink). |
| `clickhouse.option_conflict` | `endpoints` together with `host` or `port`; `tls_ca_file`, `tls_ca_dir` or `tls_verify` without `secure='true'`. | List every server in `endpoints`, or turn TLS on. |
| `clickhouse.secret_unset` | An `env://VAR` value whose variable is unset or empty on the worker. | Set the variable in every worker's environment. |
| `clickhouse.delivery_unsupported` | `mode` other than `append`, a `delivery_guarantee` above `at_least_once`, `changelog='true'` or `write_mode='overwrite'`. | Remove the setting; the sink appends at least once. For a keyed view, set `mode='append'`. |
| `clickhouse.tls_unavailable` | `secure='true'` on a build whose client has no TLS socket factory. | Build against the pinned client, or turn TLS off. |
| `clickhouse.native_unavailable` | A build whose client is older than 2.6.2, or of a version it could not read. | Rebuild against the pinned client, or use `insert_format='jsoneachrow'`. |

At open, before any network work:

| Code | Cause | Fix |
| --- | --- | --- |
| `clickhouse.barrier_mode_unsupported` | The job runs unaligned or adaptive checkpoints, or the sink's operator carries an unaligned barrier-mode override. | Run the job with aligned checkpoints, the default (`--alignment aligned` when submitting SQL with `clink_submit_sql`). |
| `clickhouse.memory_budget_too_small` | The subtask's memory budget leaves less than 1 MiB of `batch_bytes` once the sink plans its buffers into half of it. | Give each subtask a budget of at least 37748736 bytes (36 MiB), or none. |

At open, from the server and the table. A table-level or configuration change fixes each of these; the sink does not change the table itself.

| Code | Cause | Fix |
| --- | --- | --- |
| `clickhouse.access_denied` | Authentication failed, the user is unknown, access is denied, or the user is read-only (codes 164 outside quorum inserts, 192, 291, 497, 516). Also raised at run time. | Check the user and password. The user needs `INSERT` on the table and must be able to read the system tables the open checks use: `system.settings`, `system.tables`, `system.columns`, `system.merge_tree_settings` and `system.replicated_merge_tree_settings`. |
| `clickhouse.tls_verify_failed` | The server's certificate did not verify. | Point `tls_ca_file` or `tls_ca_dir` at the CA that signed it, or check the host name in `host` or `endpoints`. `tls_verify='false'` skips the check. |
| `clickhouse.option_invalid` | `tls_ca_file` or `tls_ca_dir` does not exist, is the wrong kind of path, cannot be read or searched, or is a file OpenSSL cannot load; or, with neither set, OpenSSL cannot set up its default CA locations. | Fix the path or the certificate. |
| `clickhouse.server_settings_unsupported` | The server lacks a setting the sink sends with every INSERT (including both `deduplicate_insert` and `insert_deduplicate`), or reports `min_insert_block_size_rows` or `min_insert_block_size_bytes` as something other than a whole number. | Use a server line that has them; 26.3 and 26.8 are tested. |
| `clickhouse.target_missing` | The table does not exist, or behind a Distributed table the local table is missing on some replicas (the message names those that have it). | Create the table first, on every replica. |
| `clickhouse.target_engine_unsupported` | A SharedMergeTree family table, the target or a replica's local table behind a Distributed target ("SharedMergeTree targets are not supported yet, because nothing tests their deduplication"), or an engine other than MergeTree, ReplicatedMergeTree, Distributed and Null (Buffer, Memory, the Log family, Kafka, View, MaterializedView and the rest). | Write to a MergeTree, ReplicatedMergeTree, Distributed or Null table. A Null table with materialised views is accepted. |
| `clickhouse.target_async_insert` | The table takes inserts asynchronously, which the sink refuses while upstream issue #121174 can lose acknowledged rows on that path. Also raised when the table setting or the server default cannot be read as 0 or 1, when the table's definition cannot be scanned for its settings, and when a Replicated table sets no `async_insert` and its `<replicated_merge_tree>` default cannot be read (a replica of the cluster lacks `system.replicated_merge_tree_settings`). Behind a Distributed table, also when a replica's local table is not MergeTree family or the cluster read was denied (codes 291, 497). | Run the `ALTER` the message gives, `ALTER TABLE <db>.<table> MODIFY SETTING async_insert = 0` (on the named `host:port`, for a Distributed target). For a server-wide default, set `<merge_tree><async_insert>0</async_insert></merge_tree>`, or the same under `<replicated_merge_tree>` when the message names that section. For a denied cluster read, the message lists the grants the check needs: `REMOTE ON *.*`, and `SELECT` on `system.clusters`, `system.tables`, `system.merge_tree_settings` and `system.replicated_merge_tree_settings`, plus `system.macros` when the cluster argument uses macros. |
| `clickhouse.target_unreadable` | The sink cannot read what it must check: `system.columns` lists no columns for the table (most likely a missing `SHOW COLUMNS` grant); a column has a default kind or position it does not know; a Distributed table's engine arguments are not a cluster, a database and a table; the cluster is not in `system.clusters`; the cluster argument uses a macro `system.macros` does not define (`{server_uuid}` among them), leaves a `{` unclosed or nests macros more than ten deep; or a replica is missing from a settings read across the cluster, or gives one setting two values in it. | Grant `SHOW COLUMNS` on the table; write the Distributed engine's first three arguments as strings or identifiers; check the cluster name, or name the cluster in the table definition instead of through a macro. |
| `clickhouse.column_plan` | One or more declared columns cannot be written into the target. Every problem is listed in one message (see [Type mapping](#type-mapping)). | Change the clink table's declared types, widen the target, or CAST in the SELECT, as each line says. |
| `clickhouse.retry_window_exhausted` | The server stayed unreachable, or kept failing transiently, for the whole `retry_window_ms`. The message names the last failure and the attempt count. | Restore the server or the network; raise `retry_window_ms` for longer outages. |

A column-plan refusal looks like this:

```
[clickhouse.column_plan] `analytics`.`events` cannot take this table's rows:
  - target column `tenant` has no default and the query does not produce it
  - column `amount`: DECIMAL(18, 2) into Decimal(18, 4) leaves 14 integer digits for 16; widen the target or declare the clink column DECIMAL(16, 2)
  - column `score`: DOUBLE into Float32 narrows; use REAL in the clink table or CAST in the SELECT
```

At run time, after open. Each fails the task, and the job's restart replays from the last completed checkpoint:

| Code | Cause | Fix |
| --- | --- | --- |
| `clickhouse.barrier_mode_unsupported` | A checkpoint barrier arrived unaligned although the job runs aligned checkpoints, which an unaligned barrier-mode override on an upstream operator does. The checkpoint is not acknowledged, so nothing is lost, but the job spends restarts until it fails. | Remove the override from the operators upstream of the sink. |
| `clickhouse.header_drift` | The table changed since open: the INSERT's header no longer matches the column plan. | Restart the job so that the sink plans against the table as it is now. |
| `clickhouse.conversion_failed` | A value the target column cannot hold (see [Type mapping](#type-mapping)). The error line names the column and shows the row with its values redacted. | Fix the data, or the declared type or the target type. A replay meets the same row, so this fails again until it is fixed. |
| `clickhouse.too_many_partitions` | One INSERT touches more partitions than the server's `max_partitions_per_insert_block` allows. | Make the table's `PARTITION BY` coarser, or lower `batch_rows` so each INSERT spans fewer partitions. |
| `clickhouse.insert_failed` | A permanent server error (unknown table, database, setting, column or identifier, a syntax, type or parse error, and the others ClickHouse reports as permanent); an unknown server code on its fourth failure, after three retries; a client state error that a fresh client did not clear; or an unexpected exception. Also raised at open. The message carries the server's text. | Act on the server's text. |
| `clickhouse.retry_window_exhausted` | One INSERT stayed unacknowledged for the whole `retry_window_ms`. | As at open. |
| `clickhouse.cancelled` | The task was cancelled while the sink was opening or writing. Not a fault of the sink. | None. |

The server checks a new client runs after a failure can raise any of the open-time refusals at run time as well, for example when a reconnect reaches a replica whose table takes inserts asynchronously. The INSERT then fails with that code.

A permanent failure logs one error line on `sink.clickhouse` before it throws. A conversion failure's line shows every column with its target type and, for the offending column alone, its value when that is a number, a boolean, a decimal, a date or a timestamp, or only its length when it is text or a composite value (`email=String(len 23)`), because row data may be confidential. A cell that fails on the way to its declared type is described the same way, by its kind, a number shown and text only by its length.

### Column matching

- Columns match by exact, case-sensitive name. A declared column that differs from a target column only in case is reported as missing, and the message says so.
- A target column with no default that the clink table does not produce is a problem. One with `DEFAULT` or `EPHEMERAL` is left out of the INSERT and takes its default.
- A declared column that matches a `MATERIALIZED` or `ALIAS` column is a problem: the server computes it, so drop it from the SELECT. A declared column the target lacks is a problem too, and so is one declared twice.
- Each matched pair must be one of the pairs below, and the target type must be one clickhouse-cpp can build a column for.

### Type mapping

The sink converts in two steps, and the rules differ by step.

The declared types are the table's SQL column types as the planner renders them. `TIMESTAMP(0)`, `TIMESTAMPTZ(0)` and scale-0 decimals such as `DECIMAL(10, 0)` can be declared; a `TIMESTAMP` precision is rendered as 0, 3, 6 or 9, the next of those at or above the one declared, and `TIMESTAMP` alone as `TIMESTAMP(6)`. `TINYINT` has no DDL spelling, so a SQL table cannot declare it; it is listed below for completeness.

From the Row value to the declared type. A JSON null, or an absent field of a ROW, is NULL for every type. For the types the engine's own Row batcher handles, a top-level cell of the wrong JSON kind, or out of range for its type, becomes NULL, exactly as it does on every other columnar path: it then lands as NULL in a Nullable target and fails the row in a non-Nullable one. Every other type, and every element inside an ARRAY, MAP or ROW, fails the row instead.

| Declared type | Accepted Row values | Otherwise |
| --- | --- | --- |
| `BIGINT`, `INTEGER` | an integer in range, read exactly; a number with a fraction is truncated | NULL |
| `REAL`, `DOUBLE` | a number (for REAL, within float's range) | NULL |
| `BOOLEAN` | `true` or `false` | NULL |
| `VARCHAR` | any value; numbers and booleans are written as their text, a double in its shortest round-trip form, an array or object as its JSON text | (never) |
| `DECIMAL(p, s)` | a decimal, or a number with no fraction, rounded to scale `s` half away from zero, with at most `p` digits | NULL |
| `SMALLINT`, `TINYINT` | an integer, a number with no fraction, or digit text, within the type's range | fails the row |
| `TIMESTAMP(p)`, `TIMESTAMP(p) WITH TIME ZONE` | epoch milliseconds, whatever `p`: an integer, a number with no fraction, or digit text with an optional `-` | fails the row; ISO-8601 text is not epoch milliseconds |
| `DATE` | days since 1970-01-01 as an integer, a number with no fraction or digit text, or text exactly `YYYY-MM-DD` (an optional leading `-`) naming a real date | fails the row |
| `T ARRAY` | a JSON array, or its JSON text, each element by T's rule as an element | fails the row |
| `MAP<K, V>` | a JSON object, or its JSON text; each key is read from its text by K's rule, and two keys that convert to the same value fail | fails the row |
| `ROW<f T, ...>` | a JSON object, or its JSON text, fields by name | fails the row |

Inside a composite the shared types fail rather than becoming NULL. BIGINT and INTEGER there take an integer or a number with no fraction (a fraction fails rather than being truncated, and digit text fails), and DECIMAL takes any number, read through its shortest round-trip text.

The engine represents every TIMESTAMP at run time as epoch milliseconds, so the declared precision says nothing about the value. A timestamp therefore goes into any DateTime64 precision, and a value finer than the target fails the row: it is never floored.

From the declared type to the ClickHouse column:

| Declared type | ClickHouse targets | Conversion |
| --- | --- | --- |
| `BIGINT` | `Int64`, `Int128`; `Int8`, `Int16`, `Int32`, `UInt8` to `UInt64` | exact; a narrower or unsigned target checks each value's range |
| `INTEGER` | `Int32`, `Int64`, `Int128`; `Int8`, `Int16`, `UInt8` to `UInt64` | as above |
| `SMALLINT`, `TINYINT` | any `Int8` to `Int128` and `UInt8` to `UInt64` | as above |
| `REAL` | `Float32`, `Float64` | exact or widened |
| `DOUBLE` | `Float64` | exact. `Float32` is refused: use REAL or CAST |
| `BOOLEAN` | `Bool`, `UInt8` | |
| `VARCHAR` | `String` | zero-copy |
| `VARCHAR` | `FixedString(N)` | at most N bytes, zero-padded; longer fails |
| `VARCHAR` | `Enum8`, `Enum16` | the exact item name; any other text fails |
| `VARCHAR` | `UUID` | canonical `8-4-4-4-12` hexadecimal text |
| `VARCHAR` | `IPv4` | dotted quad |
| `VARCHAR` | `IPv6` | IPv6 text, or a dotted IPv4 address, written as `::ffff:a.b.c.d` |
| `DECIMAL(p, s)` | `Decimal(P, S)`, `Decimal32/64/128(S)` | needs `S >= s`, as many integer digits (`P - S >= p - s`), `P <= 38` and `S - s <= 18`, so that every declared value fits. `P >= p` alone is not enough: `DECIMAL(10, 2)` into `Decimal(10, 4)` leaves 6 integer digits for 8 |
| `TIMESTAMP(p)` | `DateTime64(P[, tz])`, any P from 0 to 9 | milliseconds scaled exactly to the target's unit; a value with digits the target cannot hold fails ("1700000000123 ms has sub-second digits DateTime64(0) cannot hold"). Range 1900-01-01 00:00:00 to 2299-12-31 23:59:59 |
| `TIMESTAMP(p)` | `DateTime[(tz)]` | whole seconds only; range 1970-01-01 00:00:00 to 2106-02-07 06:28:15 |
| `DATE` | `Date32` | range 1900-01-01 to 2299-12-31 |
| `DATE` | `Date` | range 1970-01-01 to 2149-06-06 |
| `T ARRAY` | `Array(T')`, for any accepted pair T into T' | a NULL array fails |
| `MAP<K, V>` | `Map(K', V')`, for accepted key and value pairs | a NULL map fails |
| `ROW<...>` | `Tuple(...)` with the same number of elements, matched by position; a named Tuple's names must equal the field names | a NULL ROW fails |

A target time zone is display metadata, since the stored value is the UTC epoch, so any `tz` is accepted.

Nullability and LowCardinality apply to every row above. `Nullable(T)` takes NULLs; a NULL into a non-Nullable column fails the row ("null in non-Nullable column `x`"). `LowCardinality(String)`, `LowCardinality(FixedString(N))` and `LowCardinality(Nullable(String))` are written as their inner type.

Refused at open, by name, in the column-plan message:

| Type | Why |
| --- | --- |
| `Int256`, `UInt256`, `BFloat16`, `Variant`, `Dynamic`, `JSON`, `Object`, `Time`, `Time64`, `Point`, `Ring`, `Polygon`, `MultiPolygon`, `AggregateFunction`, `SimpleAggregateFunction`, `Nothing`, and any type name the sink does not know | not supported by the native sink |
| `Decimal(P, S)` with P above 38, `Decimal256` | clickhouse-cpp holds a decimal in at most 128 bits |
| `LowCardinality` of anything other than `String`, `FixedString(N)` or `Nullable(String)` | clickhouse-cpp builds LowCardinality only over those |
| a declared `BYTEA` or `TIME` column | no mapping in the native sink; leave the column out or declare another type |
| any other pair, `UInt128` among them | "`<SQL>` into `<ClickHouse>` is not supported; CAST in the SELECT" |

### What each INSERT sends

```
INSERT INTO `db`.`t` (`c1`, `c2`) SETTINGS async_insert=0, wait_for_async_insert=1, deduplicate_insert='enable', insert_deduplication_token='clink1-<nonce>-<seq>', input_format_native_allow_types_conversion=0, input_format_null_as_default=0, throw_on_max_partitions_per_insert_block=1, distributed_foreground_insert=1, min_insert_block_size_rows=<R>, min_insert_block_size_bytes=<B>, use_strict_insert_block_limits=0, log_comment='clink:<sink>:sub<K>:<seq>' VALUES
```

- `deduplicate_insert='enable'` where the server has that setting, `insert_deduplicate=1` otherwise. `use_strict_insert_block_limits=0` only where the server has it (26.8 does, 26.3 does not).
- The token's 128-bit nonce is drawn at every open on every subtask, and `<seq>` counts the subtask's INSERTs, so two distinct INSERTs never share a token. A retry before anything was sent keeps its token.
- `min_insert_block_size_rows` and `min_insert_block_size_bytes` are the values the server reported at open, pinned for the writer's life, so that a resend under the same token is squashed into the same parts as the first attempt whatever profile or replica it meets. Pin P17 shows why: without them a kept-token resend under a different profile duplicated a block, and once lost one.
- `log_comment` carries the sink's operator name, the subtask and the token's sequence, so an INSERT can be found in `system.query_log`.

### Retries, timeouts and cancel

Each INSERT has its own retry window, `retry_window_ms`, which starts at its first failure. Backoff starts at 100 ms and grows to at most 10 s, with full jitter. After any failure the client is discarded, its socket cut first so that a half-sent INSERT can never be committed by accident, and the next attempt builds a new client (and re-runs the server checks). A failure before anything was sent resends under the same token. One after the blocks were sent is in doubt: the server may have written them, so the resend follows the token rule in [What it guarantees](#what-it-guarantees).

Retried: connection and socket errors, timeouts, protocol and TLS errors other than a verify failure; server codes for network errors, timeouts, an unreachable shard, too many simultaneous queries, an overloaded server, a read-only replica, a lost Keeper session, an aborted or cancelled query and an unknown insert status; "too many parts" merge back-pressure (counted in `clink_clickhouse_parts_backoff_total`); and quorum failures on a server with `insert_quorum` set. A server memory-limit error (241) on an INSERT of more than 1000 rows splits it into two INSERTs, each under a fresh token and within the same window; a smaller one is retried as it is. An unknown server code is retried three times and fails on its fourth failure. Permanent errors fail at once.

A retrying INSERT holds the sink, so the checkpoint barrier waits, upstream operators on the chain wait with it, and the sink's bounded queue fills and backpressures the job. On the periodic checkpoint path a held barrier stalls the checkpoint without failing it, so an outage shorter than the window costs no restart. Meanwhile the coordinator keeps triggering checkpoints at its interval: they queue behind the held one and complete in turn once the INSERT is acknowledged. Two engine paths bound that wait below the default window; see [Topology and checkpoint limits](#topology-and-checkpoint-limits).

The calls that wait for a server reply (`BeginInsert` and `EndInsert`) run under a deadline, the end of the INSERT's window, so a server that keeps a call busy with progress packets cannot hold an attempt without a total bound. The case that matters in practice is the opposite one. On 26.3 and 26.8 the server sends nothing at all while dependent materialised views run (pin P20), so a target whose views take longer than `receive_timeout_ms` (30 s by default) fails the attempt in doubt; the INSERT is resent, and the target and its views can receive duplicates. For such targets, set `receive_timeout_ms` above the slowest INSERT's processing time, views included.

A cancel is seen within about 50 ms. A writer or opener blocked in a socket call is woken by shutting its socket down. One still inside DNS resolution, the TCP connect or the TLS handshake, where no socket can be cut, is left behind after 5 s, and its client can never commit. Either way the sink's open and cancelled close return within about 5 s.

A refusal at run time, an exhausted window or a permanent failure fails the subtask and spends a restart. With a checkpoint directory set, a job has 10 restarts by default (`max_restarts_on_worker_loss`), and the budget is forgiven once a checkpoint completes at least 10 minutes after the last restart (`restart_budget_reset_after`), so occasional outages longer than the window do not add up to a failed job. See [fault tolerance](../internals/fault-tolerance-and-rescale.md).

### Topology and checkpoint limits

The sink must be the only sink on its chain. Its guarantee rests on writing out the interval inside the barrier, before the chain's checkpoint is acknowledged, which holds only while the sink owns that checkpoint, and a second sink on the chain would take ownership away. `Dag::add_sink` refuses the combination in either order, with a message that names the sink and says it must be the only sink on its chain; give the other sink its own subtask. See [checkpointing](../internals/checkpointing.md#guarantees-and-caveats).

Unaligned and adaptive checkpoints are refused (`clickhouse.barrier_mode_unsupported`). Under them a fan-in upstream of the sink forwards a barrier at once without capturing the rows still in flight on its other inputs, so a row could reach the sink after a barrier its upstream had already snapshotted past, and a crash would lose it. A cluster sink subtask has a fan-in in front of it whenever the upstream parallelism is above 1, so the refusal applies at any parallelism. Adaptive mode would turn unaligned under backpressure, which this sink applies on purpose while it retries.

Two engine paths bound how long a barrier may be held, and both bounds are below the default `retry_window_ms` of 600000:

- the final checkpoint at the end of a bounded job, and the commit wait of a hot cutover, wait at most `CLINK_EOS_FINAL_CKPT_TIMEOUT_MS` (default 30000);
- a split stage's cutover hold waits at most `CLINK_CUTOVER_HOLD_TIMEOUT_MS` (default 300000).

A retry that holds the barrier past either bound fails that path and spends a restart; the replay from the last completed checkpoint loses nothing. For bounded jobs and jobs you rescale, set both variables on the workers at or above `retry_window_ms`, or lower `retry_window_ms` to fit. The open line reports the bound the worker read as `eos_bound_ms`.

### Sizing

An INSERT closes at the first of: `batch_rows` rows; `batch_bytes` of converted payload; `batch_interval_ms` after its first row; a checkpoint barrier or the end of input; its first failure, which freezes its content for the retries; or its memory charge reaching twice `batch_bytes`. Within an INSERT the rows go as blocks of at most 16 MiB of payload (a single larger row goes alone). While an INSERT stays under the server's squash thresholds, `min_insert_block_size_rows` and `min_insert_block_size_bytes`, its blocks squash into one part per partition on the server. The defaults aim at large INSERTs: `batch_rows` equals ClickHouse's default row threshold and `batch_bytes` is 64 MiB, so at volume the one-second interval closes most INSERTs.

Parts. On a steady stream each subtask closes about one INSERT per `batch_interval_ms`, plus one per checkpoint, so the job sends about `parallelism × 1000 / batch_interval_ms` INSERTs a second, and each makes at least one part per partition it touches. That is the `inserts_per_s_est` figure in the open line, a floor. Many small INSERTs slow the table's merges; a few large ones do not. The sink warns at open when `batch_rows` is below 10000. At run time each subtask warns, at most once every 10 minutes, when over a minute its own INSERT rate times the parallelism comes to more than one INSERT a second and its INSERTs averaged fewer than 10000 rows. The remedy is a larger `batch_interval_ms` or a lower sink parallelism. Keep the table's `PARTITION BY` coarse: an INSERT that touches more partitions than `max_partitions_per_insert_block` fails with `clickhouse.too_many_partitions`.

Memory, per subtask. The queue between the task thread and the writer holds up to 16 MiB or 256 chunks; when it is full the task thread waits, which backpressures the job (`clink_clickhouse_backpressure_blocked_ns`). The writer holds the open INSERT's blocks until the server acknowledges them, for a resend. At the defaults that is about 80 MiB for a table of fixed-width columns, and up to about 144 MiB for one with String columns, whose converted blocks refer to the queued chunks rather than copying them (16 bytes per string value on top). A memory-limit split adds the copied halves while they are in flight.

- With a memory budget whose limit is L, the sink plans its buffers into half of it. If 16 MiB plus twice `batch_bytes` exceeds L / 2, it reduces `batch_bytes` to (L / 2 - 16 MiB) / 2 and logs a warning; the open line shows the reduced value. At the defaults the limit must be at least 301989888 bytes (288 MiB) to keep `batch_bytes` unreduced. Below 37748736 bytes (36 MiB) open refuses `clickhouse.memory_budget_too_small`. For SQL and the embedded runtime, `CLINK_EXECUTION_MEMORY_LIMIT_BYTES` sets the budget, one per local subtask execution (see [memory management](../internals/memory-management.md)).
- The cap is advisory against a parent budget: it plans against the limit of the budget the sink sees, which may be shared with state and channels, and does not know how much of a parent's headroom other operators use. A reservation that would exceed the budget fails the task rather than waiting, as everywhere else in the engine.
- Without a budget the sink's buffers are unaccounted.

Compression: `lz4`, the default, is cheap on CPU. `zstd` sends fewer bytes for more CPU on both ends. `none` suits a fast local link.

### TLS and failover

With `secure='true'` the sink connects to port 9440 unless `port` or `endpoints` says otherwise, sends the host name for SNI, and verifies the server's certificate and host name. `tls_ca_file` or `tls_ca_dir` replaces the default CA locations, so only the named CAs are trusted. `tls_verify='false'` skips verification; the open line then shows `tls=on (verify off)`. The CA is loaded when the client is built, and a CA that cannot be loaded refuses `clickhouse.option_invalid` at open rather than being retried.

`endpoints` names replicas of one shard; any replica accepts an INSERT. The sink connects to one endpoint at a time, moves to the next after a connect failure, and stays on one that works. Every new client re-runs the server checks before its first INSERT and counts in `clink_clickhouse_reconnects_total`. A client that reaches a different server (name, version or endpoint) logs one Warn line with the old and new identity.

A Distributed target is accepted, and the sink checks the local table on every replica of its cluster at open through `clusterAllReplicas`, so the sink's user needs the `REMOTE` grant and read access to those system tables (see the `clickhouse.target_async_insert` row). Replicas are named in reports and remedies as `host:port`, with the server's UUID added when two share both. A replica that is down at open is retried within the window, not refused.

### Metrics and logs

The cross-connector series carry `connector="clickhouse_native"`, separate from the text sink's `clickhouse`: `clink_connector_records_total{direction="sink"}` counts rows when their INSERT is acknowledged, never when buffered; `clink_connector_bytes_total{direction="sink"}` counts the bytes the writer puts on the socket, compressed blocks and protocol framing together; `clink_connector_errors_total{direction="sink"}` counts failed attempts, at open and in the writer, conversion failures included.

The sink's own series are tagged with its `op_id`:

| Name | Type | Extra tag | Meaning |
| --- | --- | --- | --- |
| `clink_clickhouse_inserts_total` | counter | `outcome` = `ok`, `retried_ok`, `failed`, `abandoned` | INSERTs by final outcome |
| `clink_clickhouse_rows_total` | counter | | rows acknowledged |
| `clink_clickhouse_insert_latency_ns` | histogram | | first `BeginInsert` to acknowledgement, retries included |
| `clink_clickhouse_block_rows` | histogram | | rows per sent block |
| `clink_clickhouse_retries_total` | counter | `class` = `transient`, `in_doubt`, `merge_backpressure`, `resource`, `client_defect`, `unclassified` | retry attempts, while opening and during INSERTs |
| `clink_clickhouse_in_doubt_total` | counter | | failures after the blocks were sent |
| `clink_clickhouse_retry_wait_ns` | histogram | | each backoff wait between attempts, while opening and during INSERTs |
| `clink_clickhouse_queue_bytes` | gauge | | bytes queued for the writer |
| `clink_clickhouse_backpressure_blocked_ns` | histogram | | time the task thread waited for queue space |
| `clink_clickhouse_barrier_flush_ns` | histogram | | time a checkpoint barrier waited for the INSERTs before it |
| `clink_clickhouse_parts_backoff_total` | counter | | "too many parts" retries |
| `clink_clickhouse_refusals_total` | counter | `reason` = the refusal code | refusals at deploy, open and run time. A deploy refusal comes before any operator id exists, so it carries `reason` only |
| `clink_clickhouse_reconnects_total` | counter | | clients built after a failure, at open or by the writer |
| `clink_clickhouse_rows_maybe_duplicated_total` | counter | | rows resent after they were sent, other than under their own token to a target that keeps a log |

The `_ns` histograms have buckets from 1e5 to 1.2e11 ns, and `block_rows` from 1 to 2e6 rows.

Retries while the sink opens move the same series as retries during INSERTs: `retries_total`, `retry_wait_ns` and, for merge back-pressure, `parts_backoff_total`, each counted once the retry window allows the retry. Each failed attempt also counts in `clink_connector_errors_total` and logs a Warn line, and `reconnects_total` rises once a later attempt connects, so an outage that keeps a restarted job from opening is visible on the same alerts.

Log lines go to the `sink.clickhouse` source. Under `clink run` and pyclink, which have no metrics listener, they are the sink's report. Each subtask logs one line at open, at Info:

```
clickhouse native sink open: subtask=3/8 factory=clickhouse_native_sink mode=append delivery=at_least_once table=`analytics`.`events` engine=ReplicatedMergeTree server=26.8.15 (tested) endpoint=ch-1:9000 tls=off compression=lz4 dedup_setting=deduplicate_insert strict_limits=sent token_on_resend=kept dedup_window=replicated_deduplication_window=10000 (server default) async_insert=0 (table unset, server default 0) columns=12 omitted=2 batch_rows=1048449 batch_bytes=67108864 batch_interval_ms=1000 retry_window_ms=600000 inserts_per_s_est=8.0+ckpt eos_bound_ms=30000
```

- `server` is `(tested)` on 26.3 and 26.8 and `(accepted, untested)` otherwise.
- `tls` is `off`, `on` or `on (verify off)`; `dedup_setting` is `deduplicate_insert` or `insert_deduplicate`; `strict_limits` is `sent` or `not_sent`; `token_on_resend` is `kept` or `fresh`.
- `dedup_window` and `async_insert` say where each value came from (the table, the server default, or, for Distributed, each replica by `host:port`). A Null target reports `async_insert=0 (Null table, the query's own setting)` and `dedup_window=none (Null table)`.
- `batch_bytes` carries `(reduced from N for the memory budget)` when the budget capped it.
- `eos_bound_ms` is `CLINK_EOS_FINAL_CKPT_TIMEOUT_MS` as this worker read it, reported whether or not the job is bounded.

Warnings may follow it: the server is on an untested line; the target keeps no deduplication log (also true of every Null and Distributed target), so an INSERT resent after a failure that left it in doubt may land twice; `batch_rows` is below 10000; `batch_bytes` was reduced for the memory budget. Subtask 0 also logs the column plan, one line per column with its declared type, target type and conversion, and the effective options, with the password shown as `set` or `unset`.

Each retry logs a Warn line with the attempt, the phase, the class, the error and the wait; a retry at open also names the endpoint. A clean close logs a summary at Info, and a cancelled or failed task logs the same fields at Warn with the prefix `clickhouse native sink cancelled:`:

```
clickhouse native sink closed: subtask=3/8 rows_acknowledged=10485760 inserts=11 retries=transient:2,in_doubt:1,merge_backpressure:0,resource:0,client_defect:0,unclassified:0 in_doubt=1 rows_resent_with_token=953211 rows_maybe_duplicated=0 abandoned_rows=0 wire_bytes=391002113 elapsed_ms=61210
```

`inserts` counts the INSERTs the server acknowledged, both halves of a split included. `abandoned_rows` counts every submitted row the server had not acknowledged when the sink stopped: the INSERT in flight, the rest of a chunk the writer was part-way through, and the rows still waiting in its queue. With `rows_acknowledged` it accounts for every row the sink took. Either way they were after the last completed checkpoint, so the restart replays them; an abandoned INSERT larger than the server's squash threshold may already have written some parts, so duplicates are possible and loss is not.

## Example

Programmatic use through the fluent builder for the text sink (`clink/api/clickhouse_builders.hpp`), which produces a `SinkDescriptor` for the `clickhouse_sink` factory:

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

The native sink has no installed header or builder; SQL reaches it through `insert_format='native'`. The source side is reached through the registered factories (`clickhouse_row_source`, `clickhouse_text_source`, `clickhouse_source`), each requiring a `query`.

## Delivery semantics

Native sink: at-least-once, as set out in [What it guarantees](#what-it-guarantees).

Text sink: at-least-once, when it is the only sink on its chain. Records are buffered and flushed by row count (`batch_rows`), by time (`batch_interval_ms`), at every checkpoint barrier, and at `close()`. The barrier flush runs in `on_barrier`, before the runner snapshots and acks, and the INSERT is synchronous, so no row consumed before a completed checkpoint can still be sitting in the buffer when the process dies. A flush that fails at the barrier throws, which fails that checkpoint. Before the barrier flush existed (up to and including v0.8.0) a row buffered across a checkpoint was lost if the process died before the next size or time flush, which `batch_rows='1'` avoids on those versions.

That ordering needs the text sink to own its chain's checkpoint, which it does only while it is the only sink on the chain. A second sink on the same chain hands the checkpoint back to the upstream operator, which acks at its own barrier without waiting for either sink. A checkpoint can then complete while this sink still holds rows from before the barrier, and a restore from it never replays them. Nothing refuses that topology for the text sink, so keep it alone on its chain.

There is no two-phase commit, and a replay is not deduplicated. After a failure the job resumes from the last completed checkpoint and the rows since then are inserted again. Because every INSERT carries a fresh deduplication token, a replayed batch lands again on a replicated table even when its rows match a batch that landed before the restart; without a token the server's content hashing could absorb such an identical replay. A `ReplacingMergeTree` keyed by the row's natural key absorbs the duplicates on the ClickHouse side. The SQL planner reflects this: it rejects `exactly_once` and `mode='upsert'`.

Source: a `SELECT` materialises a finite (bounded) result set. The source persists a cursor (the row index into the materialised snapshot) and can resume mid result-set after a restart; `open()` clamps a restored cursor to the re-materialised row count. Exactly-once at the source boundary holds only for a deterministically ordered query (an explicit `ORDER BY`) over data unchanged between runs, because row index N is "the same row" only under those conditions. The SQL source binding treats it as a bounded query with no cursor checkpoint.

## Limitations

- Text sink input is a single `std::string` per record, interpreted as one row (TSV or JSONEachRow). It is not a multi-column typed insert at the C++ sink layer; multi-column Rows are serialised to a JSON object string upstream (the SQL path) before the sink sees them.
- Text sink batches are concatenated in memory and inserted with `client.Execute()`; there is no streaming insert, no 2PC, no upsert, and no deduplication of a replayed batch. A failed flush at a barrier throws, which fails that checkpoint rather than completing it over rows ClickHouse never received.
- The text sink is at-least-once only as the only sink on its chain; beside a second sink a checkpoint can complete over rows it has not yet written.
- The native sink appends only: no exactly-once, no `mode='upsert'`, no `changelog='true'`, no full-refresh materialised views.
- The native sink refuses SharedMergeTree targets, asynchronous-insert targets, and unaligned and adaptive checkpoints, and must be the only sink on its chain.
- A resent INSERT is deduplicated only on targets that keep a deduplication log, and only on 26.3 and 26.8.
- A retry that holds the barrier beyond `CLINK_EOS_FINAL_CKPT_TIMEOUT_MS` (default 30 s) at the end of a bounded job or at a hot cutover spends a restart; nothing is lost.
- The native sink takes Rows only: a columnar batch that reaches it is materialised through its row accessors before conversion.
- The native sink does not accept `tls_server_name`, does not pin a certificate, and does not create or alter tables. It authenticates by user and password only.
- Source is a one-shot bounded `SELECT`, not a streaming tail or CDC feed. Result-row order is arbitrary without an explicit `ORDER BY`.
- The `clickhouse_source` (string-channel JSON) requires column names from the server; if they are absent it fails loudly rather than emit positional keys.
- The text sink and the source are not Arrow-native; records cross the boundary as `std::string` or typed `ClickHouseRow` text values, with type coercion left to downstream operators.

## Testing

The text sink and source tests are in-process smoke tests. They do not stand up a ClickHouse server; they gate on whether the build linked `clickhouse-cpp`. `scripts/clickhouse-live.sh` runs the SQL-linked `ClickHouseLegacySqlLive.*` against real servers on 25.3, 26.3 and 26.8: through an ordinary user with only INSERT on the table, `format='json'` and `batch_rows='1'` land every row once, and `system.query_log` shows each INSERT carried `async_insert=0`, `wait_for_async_insert=1` and its own deduplication token. The in-process tests:

- If `clickhouse-cpp` is not linked, `ClickHouseSink::is_real_implementation()` / `ClickHouseSource::is_real_implementation()` return false and the real-impl tests `GTEST_SKIP()`.
- When linked, the tests exercise the constructor and the lifecycle (`open()` against an unreachable port should throw cleanly; `flush()` / `close()` before `open()` must be safe), plus factory registration and the fluent builder.

The native sink's tests, built when the client is 2.6.2 or later:

- Unit and in-process suites in `clink_clickhouse_tests` (the `Native*` suites, `--gtest_filter='Native*'`), label `clickhouse`. They cover options, statements, error classes, retry pacing, the type parser and column plan, both converters, the target probe and the writer and sink against an in-process fake server (`impls/clickhouse/tests/fake_transport.hpp`), with no ClickHouse needed.
- SQL-linked suite, `clink_clickhouse_sql_tests` (`tests/test_clickhouse_native_sql.cpp`), built with the SQL frontend, label `clickhouse`: the sink driven through SQL against the fake server, including a differential against the collect sink, the keys the planner puts on the op, held barriers on the periodic and bounded paths, and two INSERTs from one source. It runs with `CLINK_EOS_FINAL_CKPT_TIMEOUT_MS=3000` set by CTest.
- Live suites, `impls/clickhouse/tests/test_native_live.cpp` in `clink_clickhouse_tests` (`ClickHouseNativeLive`, `ClickHouseNativeLiveReplicated`, `ClickHouseNativeLiveTls`), and `ClickHouseNativeSqlLive` in `clink_clickhouse_sql_tests`, which writes DATE, every TIMESTAMP precision, TIMESTAMPTZ, a BIGINT past 2^53, a decimal and nullable columns through SQL into a real table and compares what ClickHouse holds with what the collect sink saw. All skip unless `CLINK_CLICKHOUSE_TEST_HOST` names a server.
- Kill matrix, `tests/integration/test_clickhouse_native_recovery.cpp` in `clink_integration_tests`: a coordinator and two workers against real servers, with faults at the sink's own points, in the network and in the server. Nine cells, each on both lines and into both a ReplicatedMergeTree and a plain MergeTree, with no loss as the pass mark in every cell.

Run the in-process suites with:

```bash
cmake -S . -B build -DCLINK_WITH_CLICKHOUSE=ON -DCLINK_BUILD_TESTS=ON
cmake --build build --parallel 10 --target clink_clickhouse_tests clink_clickhouse_sql_tests
ctest --test-dir build -L clickhouse
```

### Live suites and the kill matrix

`scripts/clickhouse-live.sh` runs the live suites against real servers, which it starts from `docker/integration-services.yml` (pinned by digest) one profile at a time and removes, with their data volumes, as soon as each profile is done and again on exit. It needs Docker and the built test binaries.

```bash
scripts/clickhouse-live.sh                          # 26.3 and 26.8, binaries under build/
CLICKHOUSE_LINES="26.8" scripts/clickhouse-live.sh  # one line
```

For each line it runs, in order:

| Profile | Services | Host ports (26.3 / 26.8) | What runs |
| --- | --- | --- | --- |
| Plain | the line's server and its async-default twin, whose `<merge_tree>` sets `async_insert=1` | 19103 and 19113 / 19108 and 19118 | `ClickHouseNativeLive.*`, then the SQL-linked `ClickHouseNativeSqlLive.*` and `ClickHouseLegacySqlLive.*` |
| Replicated | two replicas sharing one Keeper, whose `<replicated_merge_tree>` sets `async_insert=1` | 19133 and 19143 / 19138 and 19148 | pin P18, then `ClickHouseNativeLiveReplicated.*`, whose failover case shuts the first replica down |
| TLS, 26.8 only | a server with the native port over TLS; the script generates a CA, a certificate for `localhost` and an unrelated CA | 19158 plain, 19458 TLS | `ClickHouseNativeLiveTls.*` |

Last, it runs `ClickHouseLegacySqlLive.*` against a 25.3 server on port 19203, for the legacy sink's round trip. A failing case fails the run.

It sets these for the test binaries, per profile: `CLINK_CLICKHOUSE_TEST_HOST`, `CLINK_CLICKHOUSE_TEST_PORT`, `CLINK_CLICKHOUSE_TEST_ASYNC_DEFAULT_PORT`, `CLINK_CLICKHOUSE_TEST_REPLICA_PORTS` (`<r1>,<r2>`), `CLINK_CLICKHOUSE_TEST_RMT_ASYNC_DEFAULT=1`, `CLINK_CLICKHOUSE_TEST_TLS_PORT`, `CLINK_CLICKHOUSE_TEST_TLS_CA`, `CLINK_CLICKHOUSE_TEST_TLS_WRONG_CA` and `CLINK_CLICKHOUSE_TEST_LINE`. `CLINK_CLICKHOUSE_TEST_USER` and `CLINK_CLICKHOUSE_TEST_PASSWORD` pass through when set. Its own knobs:

| Variable | Effect |
| --- | --- |
| `CLICKHOUSE_LINES` | the lines to run, default `26.3 26.8` |
| `BUILD_DIR` | the build to take the binaries from, default `build/` |
| `BIN` | the `clink_clickhouse_tests` binary to run, overriding the one in `BUILD_DIR` |
| `SQL_BIN` | the `clink_clickhouse_sql_tests` binary, likewise |
| `CLICKHOUSE_LIVE_SQL=0` | skip the SQL-linked steps, for a build without that binary |
| `PIN_RUNNER` | a prefix for every test command; CI runs the binaries inside the toolchain image this way |
| `CLICKHOUSE_LIVE_TMPDIR` | where the TLS material is generated, default `TMPDIR` |

`scripts/clickhouse-live.sh` and `scripts/clickhouse-pins.sh` use the same host ports, so run one at a time.

The kill matrix needs `-DCLINK_INTEGRATION_TESTS=ON`, Docker, and the build's `clink_node` and `clink_submit_sql` with fault injection, which a test build compiles in by default. It starts its own uniquely named ClickHouse and Keeper containers on free ports, from the images and configuration the compose file pins, so it does not collide with a compose stack, plus a Kafka broker of its own. It skips when Docker or the binaries are missing. A full pass of both lines takes about 45 minutes, so CI runs it nightly rather than on every push: `.github/workflows/clickhouse-recovery.yml`, also started by hand from the Actions tab, fails if any cell fails or if the matrix ran fewer than its 36 cells, so a run that skipped for want of Docker cannot read as green. Locally:

```bash
cmake -S . -B build -DCLINK_WITH_CLICKHOUSE=ON -DCLINK_BUILD_TESTS=ON -DCLINK_INTEGRATION_TESTS=ON
cmake --build build --parallel 10 --target clink_integration_tests clink_node clink_submit_sql
build/tests/clink_integration_tests --gtest_filter='BothLines/ClickHouseNativeRecovery.*'
```

Each cell prints one `[kill-matrix]` line with what it measured. `CLINK_KILL_MATRIX_KEEP=1` keeps every cell's logs and checkpoints, passing or not, and prints where they are.

### Server pins

`impls/clickhouse/tests/test_clickhouse_pins_live.cpp` holds what the native sink's design takes as given about the server, checked against a live server on every supported line. It drives the native protocol directly, with the `BeginInsert` / `SendInsertBlock` / `EndInsert` shape the sink uses, and skips unless `CLINK_CLICKHOUSE_TEST_HOST` (and optionally `CLINK_CLICKHOUSE_TEST_PORT`) names a server. `scripts/clickhouse-pins.sh` starts each line's plain and async-default servers from `docker/integration-services.yml`, pinned by digest and with an embedded Keeper and a one-shard cluster, runs the suite, and tears them down. P18 needs a server whose `<replicated_merge_tree>` sets `async_insert=1`, so `scripts/clickhouse-live.sh` runs it on its replicated profile instead. The `clickhouse-pins` CI job ("ClickHouse pins and live suite") runs both scripts on every push and pull request to `main`. A failing pin changes the design, not the expectation.

Every pin below passes on 26.3 and 26.8. P9, P16, P17 and P20 also record a measurement, given with the pin.

| Pin | What it checks |
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
| P11 | Every setting name the sink sends exists, `min_insert_block_size_bytes` included, and `use_strict_insert_block_limits` exists from 26.8 |
| P12 | `distributed_foreground_insert=1` makes an INSERT into a Distributed table synchronous |
| P13 | `max_execution_time` with `timeout_overflow_mode='throw'` ends a SELECT with TIMEOUT_EXCEEDED |
| P14 | A Bool column sent as the client's `ColumnBool` is accepted into a Bool target with conversion off |
| P15 | String is accepted into LowCardinality(Nullable(String)), and FixedString(N) into LowCardinality(FixedString(N)), with conversion off |
| P16 | The Date32 and DateTime64 bounds the converter enforces, 1900-01-01 and 2299-12-31 (23:59:59.999 for DateTime64(3)), round-trip exactly. One unit past them is recorded, and the lines differ: 26.8 stores and reads back 1899-12-31, 2300-01-01, 1899-12-31 23:59:59.999 and 2300-01-01 00:00:00.000 unchanged, while 26.3 clamps Date32 to 1900-01-01 and 2299-12-31 and reads DateTime64(3) back as 1900-01-01 00:00:00.999 and 2299-12-31 23:00:00.000 |
| P17 | A kept-token resend under a profile with a different `min_insert_block_size_rows` lands exactly once when the statement pins both squash thresholds, as the sink's does: 3000 of 3000 rows in 5 of 5 runs on each line. Without them it landed 4000 rows, 1000 duplicated, on both lines; in exploratory runs with a real connection drop mid-INSERT on 26.8, an unpinned resend duplicated a block in some runs and once lost one (2000 distinct rows of 3000) |
| P18 | A server-wide `<replicated_merge_tree>` `async_insert=1` reaches ReplicatedMergeTree tables, which take the INSERT asynchronously despite `async_insert=0` in the query; `system.replicated_merge_tree_settings` reports it and `system.merge_tree_settings` does not |
| P19 | Every metadata SELECT of the open checks, as the sink's statement builders write it, returns only String columns, and the real transport reads each one: the cluster reads with their leading host:port and `serverUUID()` columns, and the `system.macros` read (whose rows only the fake server exercises, since the pin servers define no macros) |
| P20 | While two dependent materialised views sleep, the server sends nothing until the INSERT is done: `EndInsert` took about 5.02 s, with 3 socket reads in all and one silence of the whole 5 s, on both lines (a measurement, not a pass mark) |

P4 reproduces the partial landing by inserting the prefix of the same block's partitions under the token first, because a real failure lands the partitions written before it. P17 reproduces the landed first part the same way, because when a part lands mid-INSERT varies from run to run. P6's Distributed case sets `prefer_localhost_replica=0`: its shard is the same server, and with the default the Distributed table writes the local table in-process, where the shard's own decision never runs.

What they mean for the sink. P16: on 26.3 a value past the bounds would change silently, so the converter's refusal outside 1900 to 2299 is required there; on 26.8 it is narrower than what the server keeps. P17 is why every INSERT pins the thresholds the server reported at open. P20 is why a slow view needs a `receive_timeout_ms` above its processing time (see [Retries, timeouts and cancel](#retries-timeouts-and-cancel)).

P10 is on the engine side, in the SQL-linked suite (`ClickHouseNativeSql.TheJsonDecodeHandsTimestampsAndDatesOnAsWritten`): TIMESTAMP(0), TIMESTAMP(3), TIMESTAMP(6), TIMESTAMP(9), TIMESTAMP WITH TIME ZONE and DATE values pass through both JSON decodes, the row bridge and the columnar one, unscaled and exactly as written, so a Row timestamp is epoch milliseconds whatever its declared precision.
