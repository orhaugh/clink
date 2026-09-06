# Delta Lake

> Write a stream as a Delta Lake table (`connector='delta'`): typed Parquet data files plus the `_delta_log` transaction log, at a local path or an `s3://` table root. Append-only, single-writer, at-least-once. Compiled into the SQL frontend, no client library.

## Overview

The Delta sink is a SQL Row-channel sink registered by the SQL frontend itself (`src/sql/install.cpp`), so it is available in every build with `CLINK_BUILD_SQL=ON`. It writes the same typed-columnar Parquet data files as the [`parquet` row sink](local.md) (one Arrow column per declared column, through the shared `ArrowBatcher<Row>` seam) and adds the Delta transaction-log layer, so the result is a Delta table that delta-rs, DuckDB's `delta` extension and Spark read as a table rather than as loose Parquet files.

The transaction-log layer (`include/clink/connectors/delta_log.hpp`, namespace `clink::delta`) is pure: it maps an Arrow schema to the Delta schema string and builds the action JSON lines with no I/O, so the protocol is unit-tested without writing a table. The sink (`include/clink/sql/delta_row_sink.hpp`, `clink::sql::DeltaRowSink`) writes the data files and commits the lines it produces.

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| SQL frontend (`CLINK_BUILD_SQL`) | Built in | n/a |
| Apache Arrow / Parquet | From-source toolchain | `24.0.0` |
| aws-sdk-cpp (only for `s3://` table roots) | From-source toolchain, via Arrow's S3FileSystem | `1.11.795` |

No `CLINK_WITH_*` knob: the sink is compiled whenever the SQL frontend and Arrow are.

## Factories

| Factory name | Direction | Channel | Notes |
| --- | --- | --- | --- |
| `delta_row_sink` | sink | row | one Delta commit per checkpoint interval; only subtask 0 writes |

There is no Delta source. Read a clink-written table back with any Delta reader, or with the [`parquet` source](local.md) over the data files directly.

## Configuration

Options are read from `BuildContext` params in `src/sql/install.cpp`. On the SQL path they are the `WITH (...)` properties; the planner (`src/sql/physical_plan.cpp`) forwards every WITH option and derives `schema_columns` from the declared columns.

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `path` (alias `table_root`) | Yes | (none) | The table root: a local directory path, a `file://` URI, or an `s3://bucket/prefix` URI. Created if absent; an existing table is appended to. |
| `format` | Yes | (none) | `'json'` selects the Row channel the sink requires (or declare more than one column). |
| `event_time_column`, `watermark_lag_ms` | No | unset | As for every table; the leading `event_time` column of the data files carries the record's event time (null when none is assigned). |

For an `s3://` root the region and credentials come from the URI and the AWS environment chain, the same way as the [S3 Parquet connector](s3-parquet.md). There is no `endpoint_override` option, so an S3-compatible store on a custom endpoint (MinIO, LocalStack) cannot be targeted; real S3 works.

Options the sink refuses at compile time: `mode='upsert'` (the table is append-only) and `delivery_guarantee='exactly_once'` (the sink is at-least-once). `partition_by` is not honoured: the table is written unpartitioned.

## SQL usage

```sql
CREATE TABLE orders (usr VARCHAR, amount BIGINT) WITH (
  connector = 'file', format = 'json', path = '/data/orders.ndjson');

CREATE TABLE orders_delta (usr VARCHAR, amount BIGINT) WITH (
  connector = 'delta',
  format    = 'json',
  path      = '/lake/orders'            -- or 's3://bucket/lake/orders'
);

INSERT INTO orders_delta SELECT usr, amount FROM orders;
```

The run above produces `/lake/orders/part-00000000000000000000.parquet` and `/lake/orders/_delta_log/00000000000000000000.json`. Version 0 carries `protocol` (reader 1, writer 2), `metaData` (the schema string, `parquet` format, no partition columns), `commitInfo` (`engineInfo` `clink`, operation `WRITE`) and one `add` per data file with its path, size, modification time and row-count statistics; later versions carry `commitInfo` and their `add` actions. Read it back:

```sql
-- DuckDB
INSTALL delta; LOAD delta;
SELECT usr, amount FROM delta_scan('/lake/orders') ORDER BY usr;
```

## Commit cadence

During a checkpoint interval the sink writes the buffered rows into one Parquet data file, opened lazily on the first record and extended one row group per batch. On the checkpoint barrier it closes that file and appends the next log version, `_delta_log/<N>.json`, whose `add` action references it; `close()` commits the tail interval the same way, so a bounded job with no checkpointing configured still ends in a committed table. `N` is `(max existing version) + 1`, read by listing `_delta_log` on `open()`, which is what lets a restarted job append to the table it was writing.

## Delivery semantics

At-least-once, append-only, single-writer. The commit is the appearance of `_delta_log/<N>.json`, which is correct because the sink is the sole writer of the table: only subtask 0 is active, every other subtask stays dormant, so a higher sink parallelism buys nothing. A crash between the data-file write and the global checkpoint replays the interval from the last completed checkpoint, and the replayed rows are written again as a new version, so a downstream reader may see duplicate rows for that interval. Deduplicate downstream or compact later if that matters.

The sink carries no capability record, so it does not appear in `clink --capabilities` and the submit-time delivery analyser cannot classify a pipeline ending in it.

## Limitations

- Append-only: no `UPDATE`, `DELETE`, `MERGE` or compaction (these need deletion vectors or a Delta kernel), and `mode='upsert'` is refused.
- At-least-once only: `delivery_guarantee='exactly_once'` is refused.
- Unpartitioned tables only; `partition_by` is not honoured.
- Single-writer: concurrent writers to one table would need conditional-put commits, which are not implemented. Do not point two jobs at one table root.
- Minimal protocol feature set (reader 1, writer 2): no deletion vectors, column mapping or table features.
- `s3://` roots use the AWS credential chain and region resolution only; no custom endpoint.
- No Delta source.

Tests: `tests/test_delta_sink.cpp` (the log layer's version file naming, schema mapping, `add`, `protocol` and `metaData` actions, and a two-commit local round trip read back through the Parquet source).
