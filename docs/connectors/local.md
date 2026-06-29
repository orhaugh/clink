# Local files and Parquet

> Read and write data on the local filesystem: line/JSON text files (`connector='file'`) and Parquet files or directories (`connector='parquet'`). Built into the engine core, no external dependency.

## Overview

These are clink's built-in local-filesystem connectors, registered by `clink::core` itself rather than an optional `impls/` module, so they are always available. Two connector families:

- **`file`** (alias `filesystem`): newline-delimited text or JSON records. A single TEXT column is read/written as one line per record (the `string` channel); a multi-column table with `format='json'` is read/written as one JSON object per line (the `row` channel).
- **`parquet`**: Apache Parquet files. The `string`/`int64` channels read and write single-column files through the shared `ArrowBatcher<T>` seam; the `row` channel (`format='json'`) reads and writes typed-columnar Parquet where each declared column is its own Arrow column. A `parquet` source reads one file (`path`) or a whole directory (`prefix`).

For Parquet on object stores or HDFS instead of the local disk, see [S3](s3-parquet.md), [GCS](gcs-parquet.md), [Azure](azure-parquet.md) and [WebHDFS](webhdfs-parquet.md), which ride the same Parquet seams over a remote filesystem.

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| Engine core (`clink::core`) | Built in | n/a |
| Apache Arrow / Parquet | From-source toolchain (the `parquet` connector only) | `24.0.0` |

No client library and no `CLINK_WITH_*` knob: the `file` connector needs nothing beyond the standard library, and the `parquet` connector is built whenever Arrow is (`CLINK_BUILD_ARROW`, on by default).

## Factories

| Factory name | Direction | Channel | Notes |
| --- | --- | --- | --- |
| `file_text_source` | source | string | one line per record |
| `file_json_source` | source | row | one JSON object per line |
| `file_text_sink` | sink | string | one line per record |
| `file_json_sink` | sink | row | one JSON object per line |
| `file_json_upsert_sink` | sink | row | nets a changelog by primary key |
| `partition_file_sink` | sink | row | one file per `partition_by` value (append-only) |
| `file_2pc_sink_row` / `file_2pc_sink_string` | sink | row / string | exactly-once (stage then atomic rename) |
| `parquet_string_source` / `parquet_int64_source` | source | string / int64 | `path` (one file) or `prefix` (a directory) |
| `parquet_row_source` | source | row | typed-columnar (one Arrow column per declared column) |
| `parquet_string_sink` / `parquet_int64_sink` | sink | string / int64 | one file per subtask |
| `parquet_row_sink` | sink | row | typed-columnar |
| `parquet_row_2pc_sink` | sink | row | exactly-once typed-columnar |

## Configuration

Options are read from `BuildContext` params in `src/cluster/built_in_factories.cpp` (the `file`/`parquet` source/sink factories) and `src/sql/install.cpp` (the row-channel factories). On the SQL path they are the `WITH (...)` properties.

| Option | Applies to | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `path` | both | one of `path`/`prefix` | (none) | File path. For a parallel sink, `.<subtask_idx>` (text) or `.<subtask_idx>.parquet` is appended so each subtask writes a distinct file. |
| `prefix` | `parquet` source | one of `path`/`prefix` | (none) | A directory to read recursively; every matching Parquet file is read, sharded round-robin across subtasks (file `i` is read by subtask `i % parallelism`). |
| `recursive` | `parquet` source (`prefix`) | No | `true` | Descend into sub-directories when listing. |
| `suffix` | `parquet` source (`prefix`) | No | `.parquet` | Only files whose name ends with this are read. |
| `format` | `file` | No | line text | `format='json'` selects the multi-column row channel (one JSON object per line). |
| `partition_by` | `file` sink | No | unset | Route rows to one file per distinct value of the named column(s); append-only. |
| `mode` | `file` sink | No | append | `mode='upsert'` nets a changelog by the table's PRIMARY KEY. |
| `delivery_guarantee` | `file` / `parquet` sink | No | at-least-once | `delivery_guarantee='exactly_once'` selects the 2PC sink. |

The `parquet` row source derives its Arrow schema from the table's declared columns (`schema_columns`), so no separate schema option is needed.

## SQL usage

`connector='file'` (or `filesystem`) and `connector='parquet'` are both first-class SQL source and sink connectors. The channel follows the table shape: a single TEXT column uses the string channel, `format='json'` uses the row channel.

```sql
-- Read a directory of Parquet files, aggregate, write JSON lines.
CREATE TABLE events (user_id BIGINT, amount BIGINT) WITH (
  connector = 'parquet',
  prefix    = '/data/events'          -- every *.parquet under here, sharded across subtasks
);

CREATE TABLE totals (user_id BIGINT, total BIGINT) WITH (
  connector = 'file',
  format    = 'json',
  path      = '/out/totals.ndjson'
);

INSERT INTO totals SELECT user_id, SUM(amount) AS total FROM events GROUP BY user_id;
```

Exactly-once to local Parquet, partitioned text output, and upsert:

```sql
-- exactly-once typed-columnar Parquet (staging/ then atomic rename on checkpoint).
CREATE TABLE pq_out (id BIGINT, px DOUBLE) WITH (
  connector = 'parquet', path = '/out/pq', delivery_guarantee = 'exactly_once');

-- one file per 'region' value (append-only).
CREATE TABLE by_region (region STRING, n BIGINT) WITH (
  connector = 'file', format = 'json', path = '/out/regions', partition_by = 'region');
```

## Example

Programmatic local Parquet round-trip (single file), plus a multi-object directory read:

```cpp
#include "clink/connectors/parquet_sink.hpp"
#include "clink/connectors/parquet_source.hpp"
#include "clink/connectors/multi_object_parquet_source.hpp"
#include "clink/core/arrow_batcher.hpp"
#include <arrow/filesystem/localfs.h>

using namespace clink;

// Write one file.
ParquetSink<std::int64_t> sink("/data/part-0.parquet", int64_arrow_batcher());
sink.open();
Batch<std::int64_t> b;
b.emplace(1);
b.emplace(2);
sink.on_data(b);
sink.close();

// Read a whole directory, sharded for subtask 0 of 4.
MultiObjectParquetSource<std::int64_t>::Options o;
o.prefix = "/data";       // every *.parquet under /data
o.subtask_idx = 0;
o.parallelism = 4;
MultiObjectParquetSource<std::int64_t> src(
    []() -> std::shared_ptr<arrow::fs::FileSystem> {
        return std::make_shared<arrow::fs::LocalFileSystem>();
    },
    o, int64_arrow_batcher());
src.open();
// drive src.produce(emitter) until it returns false; src.close();
```

## Delivery semantics

- **`file` text/JSON sink** (`file_text_sink` / `file_json_sink`): at-least-once. The file is truncated and rewritten on open, so a replay from checkpoint re-writes it.
- **`file` 2PC sink** (`delivery_guarantee='exactly_once'`, `file_2pc_sink_*`): exactly-once. Each checkpoint interval is staged then atomically renamed into `committed/` on the global commit, with `write_fsync_rename` durability (toggle with `CLINK_STATE_FSYNC`). This is the local reference implementation the object-store 2PC sinks mirror.
- **`parquet` single-file sink** (`parquet_*_sink`, `parquet_row_sink`): at-least-once; the file is finalised on `close()`.
- **`parquet` 2PC row sink** (`delivery_guarantee='exactly_once'`, `parquet_row_2pc_sink`): exactly-once, the same staging then atomic-rename protocol with complete, externally-readable Parquet files.
- **Sources** (`file_*_source`, `parquet_*_source`): bounded, and replay from a checkpoint by re-opening and skipping the record/batch count already emitted (deterministic for a fixed file or a fixed directory listing).

## Limitations

- The `parquet` source's multi-object directory read (`prefix`) shards by object across same-parallelism subtasks; cross-parallelism rescale of the assignment is not coordinated.
- No Hive-partition pruning or column projection on the Parquet read path; the whole file is read.
- The `file` text sink truncates on open (no append-to-existing), and the partitioning sink is append-only.
- The typed-columnar `parquet_row_*` factories carry one Arrow column per declared column; the `parquet_string_source`/`_sink` factories carry a single column, so a multi-object `prefix` read on the string channel expects single-column files.
