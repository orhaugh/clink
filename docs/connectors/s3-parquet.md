# Amazon S3 (Parquet)

> Reads and writes Parquet objects on Amazon S3 (or any S3-compatible store) over Arrow's S3FileSystem; available as both a source and a sink.

## Overview

This connector moves Parquet files to and from S3 using Arrow's `S3FileSystem` as the transport. The sink (`ParquetS3Sink<T>`) opens an Arrow `OutputStream` at `bucket/key` and writes one Parquet row group per input batch through a `parquet::arrow::FileWriter`; the source (`ParquetS3Source<T>`) opens a single object and emits its row groups as batches via a `parquet::arrow::FileReader`. Both ride the same `ArrowBatcher<T>` seam as the local Parquet connector, so the records are encoded as Parquet and the only registered channel types are `int64` and `string`. Credentials resolve through the standard AWS chain (environment variables, instance profile, `~/.aws/credentials`, IAM role); `endpoint_override` redirects to localstack or MinIO.

Defined in `impls/s3/include/clink/connectors/parquet_s3_sink.hpp`, `impls/s3/include/clink/connectors/parquet_s3_source.hpp`, and registered in `impls/s3/src/register_factories.cpp`.

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
| `s3_parquet_int64_source` | Source | `int64` |
| `s3_parquet_string_source` | Source | `string` |

The separate `s3_text_sink` factory in the same file is a different connector (line-text objects via the AWS SDK directly) and is not covered here.

## Configuration

Parsed in `impls/s3/src/register_factories.cpp` (the `parquet_s3_options` helper) and validated by the `Options` structs in the sink and source headers.

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `bucket` | Yes | none | S3 bucket name. Construction throws if empty. |
| `key` | Sink: yes. Source: one of `key`/`prefix` | none | Object key within the bucket. The full path is `bucket/key`. Construction throws if empty (sink: required when no bucket assigner is set). |
| `region` | No | unset (Arrow default region resolution) | Explicit AWS region. Forwarded only when non-empty. |
| `endpoint_override` | No | unset | Override endpoint for localstack or MinIO. When set, the scheme is forced to `http`. |

Authentication is not configured through these options. AWS credentials resolve via the standard chain. The header `Options` structs also expose `allow_anonymous` (anonymous credentials, for public-bucket reads), and the sink exposes `compression` (`parquet::Compression`, default `ZSTD`) and a `bucket_assigner` callback for per-record key partitioning; these are programmatic-only and are not surfaced through the factory parameter parsing or the SQL frontend.

Subtask suffixing: for the two sink factories, when `parallelism > 1` the registration appends `.<subtask_idx>.parquet` to `key` so each subtask writes a distinct object.

Multi-object source: the source factories accept a `prefix` instead of a `key` to read every Parquet object beneath it (via the shared `MultiObjectParquetSource`). The objects are listed, sorted for a deterministic order, and sharded round-robin across subtasks (object `i` is read by subtask `i % parallelism`), so a parallel source covers the whole prefix disjointly. Replay across the object set is preserved (emitted-batch cursor). Source-only options for this mode:

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `prefix` | One of `key`/`prefix` | unset | Read every matching object under `bucket/prefix`. Used instead of `key`. |
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
  key = 'events/out.parquet',
  region = 'eu-west-2'
);
```

For a local MinIO or localstack target, add `endpoint_override = 'http://localhost:9000'`. The `int64` factories are reachable only through the programmatic API.

## Example

Based on the construction shape exercised in `impls/s3/tests/test_parquet_s3.cpp` and the registration in `register_factories.cpp`. Writing `int64` records to a Parquet object on S3:

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

The S3 multipart upload completes in the sink's `close()`, when every open `FileWriter` and stream is finalised. Until `close()` runs, no object is committed, so there is no transactional, per-checkpoint two-phase commit and no exactly-once guarantee on the sink path. A failure before `close()` leaves no committed object; a re-run rewrites the object from scratch. `close()` closes and releases every writer before reporting an error so that a failure on one key does not strand other keys' streams as partial objects.

The source reads a single object to its last row group and reports itself as bounded (`is_bounded()` returns true). It does not track or replay from an offset within the object.

## Limitations

- The sink writes one file at `bucket/key` (or one file per distinct `bucket_assigner` key); there is no glob or partitioned write. The source reads one object with `key` or every object under `prefix` (sharded across subtasks), but neither side performs Hive-partition pruning or column projection.
- Channel types are limited to `int64` and `string` (the four registered factories); other record types are not registered.
- The SQL frontend exposes only the `string` factories under `connector='s3_parquet'`; `int64` is programmatic-only.
- No rolling policy: each subtask opens at most one file per distinct key for its lifetime; there is no size or time-based rollover within a key.
- `compression` and `bucket_assigner` are not configurable through the factory parameters or SQL; they are set only via the programmatic `Options`.
- Authentication relies on the ambient AWS credential chain; there are no inline access-key options.
- Arrow S3 initialisation is process-wide and idempotent, with an `atexit` `FinalizeS3` that the pinned Arrow 24 requires.

## Testing

`impls/s3/tests/test_parquet_s3.cpp` contains in-process lifecycle and validation tests only. They cover constructor validation (empty bucket/key rejection, valid-options acceptance, bucket-assigner acceptance) and the `open()` failure path against the deliberately unreachable endpoint `http://127.0.0.1:1`, asserting a clean `runtime_error` rather than an abort or deadlock. They do not speak to a real S3 backend.

There is no dedicated env-gated live test for these Parquet-over-S3 factories in this test file; a round-trip test against a real S3 or MinIO instance would belong with the integration suite. (The repository's cluster-scale S3 exercise lives elsewhere and gates on `CLINK_S3_TEST_ENDPOINT` against a local MinIO image; it is not part of `test_parquet_s3.cpp`.) Run the in-process tests with the s3 impl built:

```bash
cmake -S . -B build -DCLINK_WITH_AWS_S3=ON -DCLINK_BUILD_TESTS=ON
cmake --build build --parallel 10
ctest --test-dir build -L s3
```
