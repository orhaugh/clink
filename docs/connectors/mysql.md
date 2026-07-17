# MySQL / MariaDB connector

> Connects to a MySQL or MariaDB server as both a source (incremental cursor SELECT or binlog CDC) and a batched INSERT sink, exchanging rows as JSON-object strings.

## Overview

This connector integrates a MySQL or MariaDB server over the synchronous mariadb-connector-c C API. It provides three factories on the `std::string` channel: a polling cursor source (`mysql_source`) that issues incremental `SELECT` statements, a binlog CDC source (`mysql_cdc_source`) that streams row-level changes from the master's binary log, and a sink (`mysql_sink`) that writes records as batched multi-row `INSERT` statements. Every record on the wire is a JSON-object string keyed by column name; the SQL Row path bridges these via `json_string_to_row` (source) and `row_to_json_string` (sink). The cursor source uses the MySQL text protocol, so every cell is delivered as a JSON string and the downstream Row bridge coerces it to the declared column type. The CDC source emits flat JSON change rows carrying `__op`, `__table`, `__lsn` and `__xid` metadata.

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| mariadb-connector-c (libmariadb) | System package via apt (Debian `libmariadb-dev`) / brew (macOS `mariadb-connector-c`) | Not pinned by clink |

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
| `mysql_upsert_sink` | Sink | `Row` (changelog upsert/delete by PRIMARY KEY) |

## Configuration

### Connection options (shared by all three factories)

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
| `table` | Yes | (none) | Target table. |
| `columns` | See note | (none) | Comma-separated projection and column order. On the SQL path it defaults to the declared table schema (`schema_columns`), so it is only required when the sink is built outside SQL. Each column is looked up by name in the row's JSON object; an absent or null value becomes SQL NULL. |
| `on_duplicate` | No | `` (empty) | Empty performs a plain `INSERT`. `on_duplicate='update'` appends `ON DUPLICATE KEY UPDATE` for an idempotent insert-or-update by primary key on replay. This is not clink `mode='upsert'` (a changelog contract with delete tombstones, which this sink does not implement). |
| `update_columns` | No | `` (empty) | Comma-separated `ON DUPLICATE KEY UPDATE` SET list; empty means all columns. |
| `batch_records` | No | `1000` | Flush threshold by record count; `0` is coerced to `1`. |
| `max_bytes` | No | `0` | Byte-based flush threshold; `0` disables it. |
| `linger_ms` | No | `0` | Linger before flushing a partial batch, in milliseconds; `0` disables it. |

Source: the `mysql_sink` lambda in `impls/mysql/src/register_factories.cpp` and `MysqlSinkOptions` in `impls/mysql/include/clink/mysql/mysql_sink.hpp`.

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

Source: the `mysql_source` lambda in `impls/mysql/src/register_factories.cpp` and `MysqlPollOptions` in `impls/mysql/include/clink/mysql/mysql_source.hpp`.

### `mysql_cdc_source` (binlog CDC)

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `server_id` | Yes | `0` | Replica server-id; must be non-zero and unique against the master. A value of `0` is rejected. |
| `tables` | No | `` (empty) | Comma-separated `db.table` allowlist; empty means all tables. Must be fully qualified `db.table` when `initial_snapshot` is enabled. |
| `start_file` | No | `` (empty) | Cold-start binlog file; empty starts from the current master head. |
| `start_pos` | No | `4` | Cold-start binlog position (4 is just past the magic header). |
| `initial_snapshot` | No | `false` | `initial_snapshot='true'` bootstraps existing rows of `tables` as Insert changes before streaming, captured at a consistent binlog point so existing data and ongoing changes form one gap-free stream. |
| `snapshot_lock` | No | `true` | With `true`, briefly takes `FLUSH TABLES WITH READ LOCK` (needs the RELOAD privilege) for an exact, overlap-free snapshot point. `snapshot_lock='false'` skips the lock, making the snapshot-to-stream boundary at-least-once. |
| `heartbeat_ms` | No | `1000` | Master heartbeat period in milliseconds; bounds cancel and checkpoint latency on an idle stream. |

The CDC source requires the master to run `binlog_format=ROW` with `binlog_row_image=FULL`, and the connecting user to hold `REPLICATION SLAVE` and `REPLICATION CLIENT`. Column names and signedness are resolved from `information_schema`. A binlog stream is single-reader: only subtask 0 streams, and other subtasks are dormant.

Source: the `mysql_cdc_source` lambda in `impls/mysql/src/register_factories.cpp` and `MysqlCdcOptions` in `impls/mysql/include/clink/mysql/mysql_cdc_source.hpp`.

## SQL usage

The connector is mapped under `connector='mysql'` in `src/sql/physical_plan.cpp`. The source factory is selected by the `mode` property: `mode='cdc'` selects `mysql_cdc_source`, otherwise `mysql_source`. The sink maps to `mysql_sink`.

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

On the SQL path, `columns` defaults to the declared table schema, so it does not need to be repeated. `mode='upsert'` with a PRIMARY KEY selects the changelog-aware upsert sink (`mysql_upsert_sink`, see Delivery semantics), which lets a retracting query maintain the table; `on_duplicate='update'` is the lighter append-stream idempotent-by-key option on the default sink. Exactly-once (XA 2PC) delivery is not supported.

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

The factories are also reachable through the registry on the `string` channel as `mysql_sink`, `mysql_source` and `mysql_cdc_source` once `clink::mysql::install(registry)` has run.

## Delivery semantics

- `mysql_sink`: at-least-once. A replay after a failed checkpoint re-runs the `INSERT`. A plain insert appends duplicates on replay; with `on_duplicate='update'` (and a PRIMARY KEY or UNIQUE index) the insert-or-update is idempotent and so effectively-once by key. Buffered rows are flushed on the count, byte and linger thresholds and on every checkpoint barrier. A flush failure drops the connection, clears the pending buffer and rethrows so the job replays from the last checkpoint. Exactly-once (XA 2PC) is not supported.
- `mysql_upsert_sink` (`mode='upsert'`): effectively-once on the sink table for a stable PRIMARY KEY and a deterministic defining query. Consumes the changelog by key - insert/update_after `INSERT ... ON DUPLICATE KEY UPDATE`, delete/update_before `DELETE ... WHERE <pk> IN (...)` - netted by key within a flush and applied in one transaction, so a replay converges the table to the same state. Lets a retracting query (GROUP BY, TOP-N, outer join) maintain a MySQL table. Not two-phase commit.
- `mysql_source`: at-least-once. The cursor is checkpointed as operator state (via `PollingSource`) and replayed on restart; the exclusive `>` comparison avoids re-emitting the boundary row. A NULL cursor or id value in a delivered row is fatal rather than silently stalling the cursor.
- `mysql_cdc_source`: at-least-once for the decodable change stream, with the checkpoint cursor being `(binlog_file, position)`. An undecodable row event (schema drift or an unseen table-map) is dropped and counted, so it is at-most-once for those events; it is never emitted half-populated. With `initial_snapshot` enabled and `snapshot_lock=true` the snapshot-to-stream boundary is exact; with `snapshot_lock=false` it is at-least-once. The snapshot is not checkpointed mid-way, so a crash during it re-snapshots from scratch on restart.

## Limitations

- The cursor source uses the MySQL text protocol, so every cell is delivered as a string; numeric and date typing depends on the downstream Row bridge coercing to declared column types.
- A VARCHAR `cursor_column` orders lexically (so `'10' < '9'`); choose a numeric or zero-padded key.
- A non-unique `cursor_column` can drop rows at a page boundary unless `id_column` is set as a unique tie-breaker.
- `mysql_cdc_source` requires `binlog_format=ROW` and `binlog_row_image=FULL` on the master, and `REPLICATION SLAVE` plus `REPLICATION CLIENT` privileges; column names are resolved from `information_schema`.
- A binlog stream is single-reader: only subtask 0 streams in a parallel CDC job; other subtasks emit nothing.
- CDC `initial_snapshot` requires `tables` to be a non-empty, fully-qualified `db.table` list, and a consistent snapshot relies on InnoDB MVCC (a non-transactional engine such as MyISAM has no consistent-snapshot view).
- The sink does not implement clink `mode='upsert'` (changelog delete tombstones); for idempotent insert-or-update use `on_duplicate='update'`.
- Exactly-once delivery is not supported by the sink.
- The channel is `std::string` only (JSON-object rows); there is no typed int64 channel and no Arrow or Parquet encoding on this connector's data path.

## Testing

In-process tests cover factory registration, sink and source SQL building, CDC decode and snapshot logic without a server (`impls/mysql/tests/test_factory_registration.cpp`, `test_mysql_sink_logic.cpp`, `test_mysql_source_logic.cpp`, `test_mysql_cdc_decode.cpp`, `test_mysql_snapshot.cpp`).

The live integration tests (`test_mysql_live.cpp` and `test_mysql_cdc_live.cpp`) are skipped unless the environment variable `CLINK_MYSQL_TEST_DSN` is set; each is a space-separated `key=value` DSN, for example:

```bash
export CLINK_MYSQL_TEST_DSN="host=localhost port=3306 user=root password=mysql database=test"
```

The fixture server is the `mysql:8.0` image defined in `docker/integration-services.yml`, started with `--server-id=1 --log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL` so the binlog CDC source can stream changes. Run the live tests with:

```bash
ctest --test-dir build -L mysql -R Live --output-on-failure
```
