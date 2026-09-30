# Amazon S3 (Parquet)

> Reads and writes Parquet objects on Amazon S3 (or any S3-compatible store) over Arrow's S3FileSystem; available as both a source and a sink.

## Overview

This connector moves Parquet files to and from S3 using Arrow's `S3FileSystem` as the transport. The sink factories write a directory of part objects under `bucket/prefix` through the shared `ParquetRollingSink<T>`: each subtask streams one complete Parquet object per checkpoint interval, published when its upload completes. The programmatic `ParquetS3Sink<T>` still writes a single object at `bucket/key` for direct C++ use. The source (`ParquetS3Source<T>`) opens a single object and emits its row groups as batches via a `parquet::arrow::FileReader`. Both ride the same `ArrowBatcher<T>` seam as the local Parquet connector, so the records are encoded as Parquet and the only registered channel types are `int64` and `string`. Credentials resolve through the standard AWS chain (environment variables, instance profile, `~/.aws/credentials`, IAM role); `endpoint_override` redirects to localstack or MinIO.

Defined in `include/clink/connectors/parquet_rolling_sink.hpp` (the factories' sink), `impls/s3/include/clink/connectors/parquet_s3_sink.hpp`, `impls/s3/include/clink/connectors/parquet_s3_source.hpp`, and registered in `impls/s3/src/register_factories.cpp`.

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| Apache Arrow / Parquet | Compiled from source into `CLINK_DEPS_PREFIX` on both the macOS host and the Debian image | 24.0.0 |
| aws-sdk-cpp (S3 transport for Arrow's S3FileSystem) | System package: built from source in the Debian image, Homebrew `aws-sdk-cpp` on macOS. Not bundled; used as the S3 transport layer only | 1.11.795 |

Arrow 24's own bundled aws-c-* CRT is not used; the system aws-sdk supplies S3 transport, which is also how Homebrew builds Arrow. The pinned tag keeps the two platforms close.

## Enabling it

The connector is gated on the `CLINK_WITH_AWS_S3` CMake option (`AUTO` / `ON` / `OFF`), defined in `impls/s3/CMakeLists.txt`:

- `OFF`: the `clink::s3` target is not defined.
- `AUTO` (default behaviour): the target is built only if `find_package(AWSSDK CONFIG COMPONENTS s3)` succeeds; otherwise it is skipped silently.
- `ON`: the target is required and configuration fails (`FATAL_ERROR`) if the AWS SDK is not found.

When built, the target compiles with `CLINK_HAS_AWS_S3` / `CLINK_HAS_S3` defined and links `clink::core` plus the AWS SDK S3 component. Arrow must be built with its S3 filesystem enabled, which the pinned from-source Arrow provides.

```bash
cmake -S . -B build -DCLINK_WITH_AWS_S3=ON
cmake --build build --parallel 10
```

## Factories

| Factory name | Direction | Record type |
| --- | --- | --- |
| `s3_parquet_int64_sink` | Sink | `int64` |
| `s3_parquet_string_sink` | Sink | `string` |
| `s3_parquet_2pc_int64_sink` | Sink | `int64` (two-phase commit, one staged file per checkpoint) |
| `s3_parquet_2pc_string_sink` | Sink | `string` (two-phase commit, one staged file per checkpoint) |
| `s3_parquet_int64_source` | Source | `int64` |
| `s3_parquet_string_source` | Source | `string` |

The separate `s3_text_sink` factory in the same file is a different connector (line-text objects via the AWS SDK directly) and is not covered here.

## Configuration

Parsed in `impls/s3/src/register_factories.cpp` and validated by the `Options` structs in the sink and source headers.

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `bucket` | Yes | none | S3 bucket name. Construction throws if empty. |
| `prefix` | Sink: one of `prefix`/`key`. Source: one of `key`/`prefix` | none | Sink: the prefix its part objects are written under (`bucket/prefix/sub<N>-...parquet`). Source: read every matching object under `bucket/prefix`. |
| `key` | Sink: one of `prefix`/`key`. Source: one of `key`/`prefix` | none | Source: read the single object `bucket/key`. Sink: accepted as the prefix, for configurations written before the sink wrote a directory. |
| `region` | No | unset (Arrow default region resolution) | Explicit AWS region. Forwarded only when non-empty. |
| `endpoint_override` | No | unset | Override endpoint for localstack or MinIO. When set, the scheme is forced to `http`. |

Authentication is not configured through these options. AWS credentials resolve via the standard chain. The header `Options` structs also expose `allow_anonymous` (anonymous credentials, for public-bucket reads), and the sink exposes `compression` (`parquet::Compression`, default `ZSTD`) and a `bucket_assigner` callback for per-record key partitioning; these are programmatic-only and are not surfaced through the factory parameter parsing or the SQL frontend.

Part objects: each subtask names its parts `sub<N>-<run>-<seq>.parquet`, so parallel subtasks and successive runs never write the same key. A run that starts from empty state deletes the previous run's parts under the prefix (only keys matching that pattern); a run restored from a checkpoint keeps them.

Multi-object source: the source factories accept a `prefix` instead of a `key` to read every Parquet object beneath it (via the shared `MultiObjectParquetSource`). The objects are listed, sorted for a deterministic order, and sharded round-robin across subtasks (object `i` is read by subtask `i % parallelism`), so a parallel source covers the whole prefix disjointly. Replay across the object set is preserved (emitted-batch cursor). Source-only options for this mode:

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `recursive` | No | `true` | Descend into sub-prefixes when listing. |
| `suffix` | No | `.parquet` | Only objects whose key ends with this are read. |
| `anonymous` | No | `false` | Use anonymous credentials (public-bucket reads). |

## SQL usage

Mapped in `src/sql/physical_plan.cpp` as `connector='s3_parquet'`, which resolves to the string-typed factories (`s3_parquet_string_source` / `s3_parquet_string_sink`). The remaining `WITH` properties pass through as factory parameters.

```sql
CREATE TABLE s3_out (
  payload STRING
) WITH (
  connector = 's3_parquet',
  bucket = 'my-bucket',
  prefix = 'events/out',   -- parts land at events/out/sub<N>-...parquet
  region = 'eu-west-2'
);
```

Read it back with a table on the same `prefix`.

For a local MinIO or localstack target, add `endpoint_override = 'http://localhost:9000'`. The `int64` factories are reachable only through the programmatic API.

## Example

Based on the construction shape exercised in `impls/s3/tests/test_parquet_s3.cpp`. Writing `int64` records to a single Parquet object on S3 with the programmatic `ParquetS3Sink<T>`:

```cpp
#include "clink/connectors/parquet_s3_sink.hpp"
#include "clink/core/arrow_batcher.hpp"

using namespace clink;

ParquetS3Sink<std::int64_t>::Options opts;
opts.bucket = "my-bucket";
opts.key = "events/out.parquet";
opts.region = "eu-west-2";
// opts.endpoint_override = "http://localhost:9000";  // MinIO / localstack
// opts.compression defaults to parquet::Compression::ZSTD

auto sink = std::make_shared<ParquetS3Sink<std::int64_t>>(
    std::move(opts), int64_arrow_batcher());
```

Reading the same object back through the source:

```cpp
#include "clink/connectors/parquet_s3_source.hpp"

ParquetS3Source<std::int64_t>::Options in_opts;
in_opts.bucket = "my-bucket";
in_opts.key = "events/out.parquet";
in_opts.region = "eu-west-2";

auto source = std::make_shared<ParquetS3Source<std::int64_t>>(
    std::move(in_opts), int64_arrow_batcher());
```

The source validates the file schema against the batcher's expected schema in `open()` and throws on a mismatch.

## Delivery semantics

The factory sink is at-least-once. Each subtask closes a part at every checkpoint barrier and at the end of input, and S3 publishes the object only when its multipart upload completes, so a kill never leaves a partial object and everything up to the last barrier is already published. A run restored from a checkpoint keeps every part, including those past the restore point, whose rows the sources then replay. The programmatic `ParquetS3Sink<T>` completes its single object only in `close()`, so a failure before then publishes nothing.

For exactly-once, use the 2PC sink: `s3_parquet_2pc_{int64,string}_sink` programmatically, or `delivery_guarantee='exactly_once'` in SQL with a `prefix` instead of a `key`. It stages one Parquet file per checkpoint interval under `<bucket>/<prefix>/staging` and promotes it to `<bucket>/<prefix>/committed` (a streamed copy that appears atomically on close) only when the checkpoint completes globally; a crash between pre-commit and commit is recovered on open. Read the result with the source pointed at `<prefix>/committed`.

The source reads a single object to its last row group and reports itself as bounded (`is_bounded()` returns true). It does not track or replay from an offset within the object.

## Limitations

- The factory sink writes part objects under one prefix; there is no Hive-partitioned write. The programmatic `ParquetS3Sink<T>` writes one object at `bucket/key` (or one per distinct `bucket_assigner` key). The source reads one object with `key` or every object under `prefix` (sharded across subtasks), but neither side performs Hive-partition pruning or column projection.
- Channel types are limited to `int64` and `string` (the four registered factories); other record types are not registered.
- The SQL frontend exposes only the `string` factories under `connector='s3_parquet'`; `int64` is programmatic-only.
- Parts roll at checkpoint barriers only; there is no size-based rollover, so a job without checkpointing writes one part per subtask.
- `compression` and `bucket_assigner` are not configurable through the factory parameters or SQL; they are set only via the programmatic `Options`.
- Authentication relies on the ambient AWS credential chain; there are no inline access-key options.
- Arrow S3 initialisation is process-wide and idempotent, with an `atexit` `FinalizeS3` that the pinned Arrow 24 requires.

## Testing

`impls/s3/tests/test_parquet_s3.cpp` contains in-process lifecycle and validation tests only. They cover constructor validation (empty bucket/key rejection, valid-options acceptance, bucket-assigner acceptance) and the `open()` failure path against the deliberately unreachable endpoint `http://127.0.0.1:1`, asserting a clean `runtime_error` rather than an abort or deadlock. They do not speak to a real S3 backend.

`impls/s3/tests/test_parquet_rolling_s3.cpp` runs the factories' rolling sink against MinIO when `CLINK_S3_TEST_ENDPOINT` and `CLINK_S3_TEST_BUCKET` are set: parts published at a barrier, an unfinished part leaving no object, and a restored run keeping the parts already written. `tests/test_parquet_rolling_sink.cpp` covers the same sink on the local disk and on Arrow's in-memory mock filesystem. Run the in-process tests with the s3 impl built:

```bash
cmake -S . -B build -DCLINK_WITH_AWS_S3=ON -DCLINK_BUILD_TESTS=ON
cmake --build build --parallel 10
ctest --test-dir build -L s3
```
