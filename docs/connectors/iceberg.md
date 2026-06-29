# Apache Iceberg

> Sink only. Writes a stream of SQL `Row` records as a real Apache Iceberg table (typed Parquet data files plus Iceberg snapshots) via the native `iceberg-cpp` library and a SQLite or REST catalog.

## Overview

The Iceberg connector is a table-format sink for the SQL `Row` channel. Each checkpoint interval is written as one Parquet data file per partition value and then committed as a single Iceberg snapshot (a `FastAppend`), so the result is a standard Iceberg table that Spark, Trino, PyIceberg or DuckDB can read. Data files are produced by the same row columnar Arrow batcher used by the Parquet sink, and the manifests, table metadata and the atomic catalog commit are handled by `iceberg-cpp`. The connector is append-only by default; an upsert mode maintains the table by primary key using Iceberg v2 equality deletes. It is single-writer and supports local filesystem, S3 (or S3-compatible) and REST-catalog-managed warehouses.

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| Apache iceberg-cpp | Built from source (not in apt/brew); installed via `scripts/setup-build-env.sh` for CI/Docker | v0.3.0 |
| Apache Arrow + Parquet | Built from source at pinned version; `iceberg-cpp` must link the same Arrow as clink | 24.0.0 |

`iceberg-cpp` must be built with `ICEBERG_BUILD_BUNDLE=ON`, `ICEBERG_BUILD_SQL_CATALOG=ON` and `ICEBERG_SQL_SQLITE=ON`. The bundle (\"battery-included\") library carries the Arrow/Parquet/Avro write backend; the component libraries alone do not. It must be built against clink's pinned Arrow (24.0.0) so there is no ABI clash.

## Enabling it

The CMake knob is `CLINK_WITH_ICEBERG` (`AUTO` / `ON` / `OFF`), default `AUTO`. Under `AUTO` the target is defined only if `find_package(iceberg CONFIG)` locates the bundle and SQL catalog targets (`iceberg::iceberg_bundle_static` and `iceberg::iceberg_sql_catalog_static`); otherwise it is skipped. Under `ON` a missing `iceberg-cpp` is a fatal configure error.

Two further build-env requirements:

- Point CMake at the `iceberg-cpp` install prefix (`CMAKE_PREFIX_PATH`).
- If the pinned Arrow was built with `ARROW_AZURE` (the default in `scripts/build-arrow.sh`), the Azure SDK is bundled into `libarrow_bundled_dependencies.a`, which is pulled in transitively here. System `libxml2` is then required to resolve its XML symbols (`libxml2-dev` on Debian; bundled with the macOS SDK).

```bash
cmake -S . -B build -DCLINK_WITH_ICEBERG=ON -DCMAKE_PREFIX_PATH=/path/to/iceberg-cpp-prefix
```

## Factories

| Factory name | Direction | Record type |
| --- | --- | --- |
| `iceberg_row_sink` | Sink | `clink::sql::Row` |

## Configuration

All options are parsed in `impls/iceberg/src/register_factories.cpp` from the `BuildContext` params into `IcebergRowSinkOptions` (declared in `impls/iceberg/include/clink/iceberg/iceberg_row_sink.hpp`).

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `warehouse` | Yes | (empty) | Catalog warehouse location. A local filesystem path, or an `s3://bucket/prefix` URI to write data and table metadata to S3 or MinIO. Accepts `path` as an alias if `warehouse` is unset. |
| `table` | Yes | (empty) | Iceberg table name. |
| `namespace` | No | `default` | Table namespace. Dotted (`a.b.c`) selects a multi-level namespace. |
| `schema_columns` | Yes | (none) | Typed column schema used to build the Arrow batcher for the Parquet data files. Supplied automatically by the SQL frontend from the table column definitions. |
| `partition_by` | No | (empty) | Comma-separated identity partition columns. Empty means an unpartitioned table. v1 supports identity partitioning only, on INT/LONG/BOOL/STRING columns, and partition values must be present and non-null. |
| `equality_key` | No | (empty) | Comma-separated primary-key columns. Non-empty selects upsert mode: the sink consumes a changelog (the `__row_kind` field) and maintains the table by key via Iceberg v2 equality deletes. Empty means plain append. v1 upsert is unpartitioned and keys must be present and non-null on every record. |
| `catalog_uri` | No | `<warehouse>/catalog.db` (SQLite, for a local warehouse) | Catalog selector. For an `s3://` warehouse with the SQLite catalog this is required and must be a local path (the catalog file cannot live on S3). An `http(s)://` URI selects the REST catalog, which resolves its own FileIO from the server config and `file_io_props`. |
| `rest_auth_token` | No | (empty) | Bearer token for a REST catalog, sent as `Authorization: Bearer <token>`. |
| `s3_endpoint` | No | (unset) | S3 endpoint (`s3.endpoint`). For an `s3://` warehouse. |
| `s3_region` | No | (unset) | S3 region (`s3.region`). |
| `s3_access_key` | No | (unset) | S3 access key ID (`s3.access-key-id`). Auth. |
| `s3_secret_key` | No | (unset) | S3 secret access key (`s3.secret-access-key`). Auth. |
| `s3_session_token` | No | (unset) | S3 session token (`s3.session-token`). Auth. |
| `s3_path_style` | No | (unset) | Path-style access (`s3.path-style-access`); set `true` for MinIO. |

Any S3 property left unset falls back to the standard AWS environment and credential chain (including `AWS_ENDPOINT_URL`). The `subtask_idx` is supplied by the runtime, not a user param; only subtask 0 writes.

## SQL usage

Mapped in `src/sql/physical_plan.cpp` as `connector='iceberg'`, binding to the `iceberg_row_sink` factory. WITH options are passed through to the sink params; the column schema (`schema_columns`) is injected automatically from the table definition. `mode='upsert'` requires a `PRIMARY KEY` (or `primary_key=`), which becomes the `equality_key`. The explicit `exactly_once` DDL flag is rejected (exactly-once is provided automatically via two-phase commit when the job checkpoints; see Delivery semantics).

Append sink to a local warehouse:

```sql
CREATE TABLE events (
    id   BIGINT,
    name VARCHAR
) WITH (
    connector = 'iceberg',
    warehouse = '/data/warehouse',
    \"table\"   = 'events',
    namespace = 'default'
);
```

Upsert sink (primary key maintained via equality deletes), data and metadata on S3/MinIO:

```sql
CREATE TABLE events (
    id   BIGINT,
    name VARCHAR,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    connector     = 'iceberg',
    mode          = 'upsert',
    warehouse     = 's3://bucket/warehouse',
    \"table\"       = 'events',
    catalog_uri   = '/local/catalog.db',
    s3_endpoint   = 'http://minio:9000',
    s3_region     = 'us-east-1',
    s3_path_style = 'true',
    s3_access_key = 'minioadmin',
    s3_secret_key = 'minioadmin'
);
```

## Example

Based on `impls/iceberg/tests/test_iceberg_sink.cpp`. A typed `Row` sink writing one Iceberg snapshot per checkpoint interval to a local warehouse:

```cpp
#include \"clink/iceberg/iceberg_row_sink.hpp\"
#include \"clink/sql/row.hpp\"
#include \"clink/sql/row_columnar_batcher.hpp\"

using clink::sql::Row;
using clink::sql::RowColumn;
using clink::iceberg::IcebergRowSinkOptions;
using clink::iceberg::make_iceberg_row_sink;

IcebergRowSinkOptions o;
o.warehouse        = \"/data/warehouse\";
o.namespace_levels = {\"default\"};
o.table            = \"events\";
o.batcher          = clink::sql::make_row_columnar_arrow_batcher(
    {{\"id\", arrow::int64()}, {\"name\", arrow::utf8()}});

auto sink = make_iceberg_row_sink(std::move(o));
sink->open();

Batch<Row> b;
Row r;
r.values[\"id\"]   = clink::config::JsonValue{std::int64_t{1}};
r.values[\"name\"] = clink::config::JsonValue{std::string{\"a\"}};
b.emplace(std::move(r));
sink->on_data(b);
sink->on_barrier(CheckpointBarrier{CheckpointId{1}});  // writes one data file + snapshot
sink->close();                                          // commits the tail interval
```

To resolve the sink through the plugin registry instead, look up the `iceberg_row_sink` factory and supply `warehouse`, `table`, `namespace` and `schema_columns` as `BuildContext` params (the form the SQL frontend uses).

## Delivery semantics

The sink stages data on the checkpoint barrier and commits the snapshot only on `on_commit`, after the checkpoint is globally durable. The commit is idempotent: each snapshot is tagged with a `clink.checkpoint-id` summary property, so a redelivered commit or a recovery replay never double-commits.

- Wired into the engine's two-phase commit (with a state backend and job manager), delivery is exactly-once. `on_barrier` stages the Parquet data file without creating a snapshot; `on_commit` creates the snapshot; `on_abort` deletes the staged, unreferenced data file and creates no snapshot. A sink that staged a checkpoint and then crashed before `on_commit` commits the pending staged data when a replacement sink re-opens against the same state backend and operator id.
- In standalone use (no state backend, no job manager), it falls back to at-least-once: the barrier commits immediately.

A data file staged before a crash whose checkpoint never completed is an orphan until the engine's abort deletes it or Iceberg orphan-file maintenance reclaims it.

## Limitations

- Sink only; there is no Iceberg source.
- Single-writer: only subtask 0 writes; other subtasks are dormant and produce no files. The `clink.checkpoint-id` idempotency marker protects against this writer's own redelivery or replay only. It is a precondition, not enforced, that no other job writes the same table concurrently; a foreign concurrent writer could defeat the idempotency scan.
- Partitioning is identity only, on INT/LONG/BOOL/STRING columns; bucket, truncate and temporal transforms are not supported, nor are float/double partition columns. Partition values must be present and non-null. A high-cardinality partition column produces many open writers and small files per interval, so prefer bounded-cardinality keys and run Iceberg compaction.
- Upsert mode is unpartitioned (it cannot be combined with `partition_by`, which is rejected at `open()`), and keys must be present and non-null on every record.
- An `s3://` warehouse using the SQLite catalog requires an explicit local `catalog_uri`; the catalog file cannot live on S3. This precondition throws in `open()`.
- The explicit `exactly_once` SQL DDL flag is not yet wired and is rejected (exactly-once is still provided automatically via 2PC on a checkpointing job).
- The connector is built only when `iceberg-cpp` (bundle plus SQL catalog) is present at configure time; otherwise the target is not defined.

## Testing

The in-process tests live in `impls/iceberg/tests/test_iceberg_sink.cpp` (ctest label `iceberg`) and run by default once the connector is built. They drive `make_iceberg_row_sink` end to end against a local SQLite-catalog warehouse and assert the on-disk structure (catalog DB, `.metadata.json`, `.avro` manifests, Parquet data files, `snap-` snapshot lists), covering append, 2PC stage/commit/abort, recovery, identity partitioning and upsert.

Three tests are environment-gated and skip unless their variables are set:

- S3 round-trip (`IcebergS3Live.WritesToS3AndReopens`): set `CLINK_S3_TEST_ENDPOINT` (and optionally `CLINK_S3_TEST_BUCKET`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`). Writes a table whose data and metadata live on S3 (MinIO/LocalStack) with a local SQLite catalog, then re-opens it. Skips in CI.
- REST catalog round-trip (`IcebergRestLive.WritesViaRestCatalog`): set `CLINK_ICEBERG_REST_URI` (and optionally `CLINK_ICEBERG_REST_WAREHOUSE`, `CLINK_ICEBERG_REST_TOKEN`, the S3 environment). Writes via a REST catalog server (Polaris/Nessie/iceberg-rest-fixture) and re-opens via `LoadTable`.

Set `CLINK_ICEBERG_KEEP_DIR` to retain the local warehouse so an external reader (PyIceberg, Spark) can verify the table. PyIceberg cannot read equality deletes, so the upsert merge-on-read result is verified with an external `iceberg-cpp` reader.
