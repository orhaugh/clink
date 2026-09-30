# Google Cloud Storage (Parquet)

> Reads and writes Parquet objects on Google Cloud Storage through Arrow's `GcsFileSystem`, providing both a source and a sink for the int64 and string channels.

## Overview

This connector integrates Google Cloud Storage as a Parquet store. It uses Arrow's `arrow::fs::GcsFileSystem` as the transport, so there is no separate GCS client dependency: the google-cloud-cpp storage client is statically bundled into `libarrow` when Arrow is built with `ARROW_GCS=ON`. Records are encoded as Parquet via the same `ArrowBatcher<T>` seam used by the operator wire and the local Parquet connector. Two record channels are supported, `std::int64_t` and `std::string`, each with a sink and a source. The sink writes with ZSTD compression by default.

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| Apache Arrow + Parquet | Compiled from source (`scripts/build-arrow.sh`), built with `ARROW_GCS=ON` | 24.0.0 |
| google-cloud-cpp storage client | Statically bundled inside `libarrow` by the Arrow GCS build; not a separate clink dependency | bundled with Arrow 24.0.0 |

There is no apt or brew client library and no separate link dependency. The GCS support rides the Arrow that `clink::core` already links (`impls/gcs/CMakeLists.txt`).

## Enabling it

The module is gated by the `CLINK_WITH_GCS` CMake option (`AUTO` / `ON` / `OFF`), which defaults to `AUTO` (`CMakeLists.txt`). The real gate is whether the pinned Arrow was compiled with GCS support: `find_package(Arrow)` sets `ARROW_GCS`, and the module only builds when that is true.

- `AUTO`: builds the connector if `ARROW_GCS` is set, otherwise skips it silently.
- `ON`: fails configuration with a fatal error if Arrow was built without GCS support.
- `OFF`: the target is not defined.

To guarantee GCS support, build Arrow with `-DARROW_GCS=ON` (already set in `scripts/build-arrow.sh`) and re-bootstrap the dependency prefix.

```bash
cmake -S . -B build -DCLINK_WITH_GCS=ON
cmake --build build -j
```

## Factories

| Factory name | Direction | Record type |
| --- | --- | --- |
| `gcs_parquet_int64_sink` | Sink | `int64` |
| `gcs_parquet_string_sink` | Sink | `string` |
| `gcs_parquet_2pc_int64_sink` | Sink | `int64` (two-phase commit, one staged file per checkpoint) |
| `gcs_parquet_2pc_string_sink` | Sink | `string` (two-phase commit, one staged file per checkpoint) |
| `gcs_parquet_int64_source` | Source | `int64` |
| `gcs_parquet_string_source` | Source | `string` |

These are the exact names passed to `register_sink` / `register_source` in `impls/gcs/src/register_factories.cpp`.

## Configuration

The sink and source share the same auth and endpoint parameters, parsed by `apply_gcs_params` in `impls/gcs/src/register_factories.cpp` onto the `Options` structs in `parquet_gcs_sink.hpp` and `parquet_gcs_source.hpp`.

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `bucket` | Yes | (none) | Target GCS bucket. The factory throws if it is empty. |
| `prefix` | Sink: one of `prefix`/`key` | (none) | Sink: the prefix its part objects are written under (`bucket/prefix/sub<N>-...parquet`). Source: read every matching object under it. |
| `key` | Source: one of `key`/`prefix` | (none) | Source: the single object `bucket + "/" + key`. Sink: accepted as the prefix, for configurations written before the sink wrote a directory. |
| `anonymous` | No | `false` | When `true`, uses anonymous access (`GcsOptions::Anonymous()`), suitable for the fake-gcs-server emulator or a public bucket. |
| `access_token` | No | (unset) | An explicit OAuth2 access token. Static, with a 24-hour far-future expiry and no refresh; the caller is responsible for token validity. Prefer `credentials_file`/`credentials_json` or Application Default Credentials for a long-running job. |
| `credentials_file` / `credentials_json` | No | (unset) | A service-account key, as a file path or inline JSON, mapped to `GcsOptions::FromServiceAccountCredentials`. Auto-refreshing: google-cloud-cpp mints and renews the OAuth token over the filesystem's lifetime. Takes precedence over `access_token`. |
| `endpoint_override` | No | (unset) | Overrides the GCS API endpoint, for targeting an emulator such as fake-gcs-server. When set, `scheme` defaults to `http`. |
| `scheme` | No | `https` (or `http` when `endpoint_override` is set) | Connection scheme. |
| `project_id` | No | (unset) | GCP project id, applied to the `GcsOptions`. |
| `retry_limit_seconds` | No | Arrow default (15 minutes) | Caps the GCS retry window. Parsed from `retry_limit_seconds` as an integer; only applied when greater than 0. The streaming sink uses this to fail fast on a dead endpoint. |

Authentication and refresh: the credential precedence is `anonymous` > a service-account key (`credentials_file`/`credentials_json`) > a static `access_token` > Application Default Credentials (the default, `GOOGLE_APPLICATION_CREDENTIALS` or the ambient GCE/GKE/Cloud Run identity). Both the service-account key and Application Default Credentials auto-refresh, that is google-cloud-cpp mints and renews the OAuth token over the filesystem's lifetime, so they suit a long-running job; only the explicit `access_token` is static and expires on its own (`make_gcs_options` in `parquet_gcs_sink.hpp`).

The sink `Options` struct also carries `compression` (defaults to `parquet::Compression::ZSTD`) and an optional `bucket_assigner` for per-record Hive-style object keys. Neither is wired through the factory parameter parsing; they are only reachable via the programmatic API.

Each subtask names its part objects `sub<N>-<run>-<seq>.parquet`, so parallel subtasks and successive runs never write the same key (`register_factories.cpp`).

## SQL usage

Mapped in `src/sql/physical_plan.cpp` under `connector='gcs_parquet'`. The SQL frontend resolves this to the string-channel factories (`gcs_parquet_string_source` / `gcs_parquet_string_sink`).

```sql
CREATE TABLE gcs_out (
    line STRING
) WITH (
    connector = 'gcs_parquet',
    bucket    = 'my-bucket',
    key       = 'output/data.parquet',
    project_id = 'my-gcp-project'
);
```

Other option keys from the table above (`anonymous`, `access_token`, `endpoint_override`, `scheme`, `retry_limit_seconds`) are passed through the same `WITH (...)` properties.

## Example

Programmatic use of the typed classes with their `Options`, based on `impls/gcs/tests/test_parquet_gcs_live.cpp`. Write a Parquet object, then read it back on the int64 channel.

```cpp
#include "clink/connectors/parquet_gcs_sink.hpp"
#include "clink/connectors/parquet_gcs_source.hpp"
#include "clink/core/arrow_batcher.hpp"

using clink::Batch;
using clink::int64_arrow_batcher;
using clink::ParquetGcsSink;
using clink::ParquetGcsSource;

// Write
ParquetGcsSink<std::int64_t>::Options so;
so.bucket = "my-bucket";
so.key = "data.parquet";
so.project_id = "my-gcp-project";  // Application Default Credentials by default

ParquetGcsSink<std::int64_t> sink(so, int64_arrow_batcher());
sink.open();
Batch<std::int64_t> b;
b.emplace(10);
b.emplace(20);
b.emplace(30);
sink.on_data(b);
sink.close();  // finalises the Parquet object

// Read back
ParquetGcsSource<std::int64_t>::Options ro;
ro.bucket = "my-bucket";
ro.key = "data.parquet";
ro.project_id = "my-gcp-project";

ParquetGcsSource<std::int64_t> src(ro, int64_arrow_batcher());
src.open();
clink::Emitter<std::int64_t> em(/* ... */);
while (src.produce(em)) {
}
src.close();
```

Via the registry, look up the factory by name and channel, for example `gcs_parquet_string_sink` / `string`, as exercised in `impls/gcs/tests/test_factory_registration.cpp`.

## Delivery semantics

The factory sink is at-least-once and writes a directory of part objects under `bucket/prefix` through the shared `ParquetRollingSink<T>` (`include/clink/connectors/parquet_rolling_sink.hpp`). Each subtask closes a part at every checkpoint barrier and at the end of input, and the store publishes an object only when its upload completes, so a kill never leaves a partial object and everything up to the last barrier is already published. A run restored from a checkpoint keeps every part, including those past the restore point, whose rows the sources then replay; a run that starts from empty state deletes the previous run's parts under the prefix (only keys named `sub<N>-*.parquet`). Read the directory back with the source on the same `prefix`. The programmatic `ParquetGcsSink<T>` writes a single object and finalises it only in `close()`.

For exactly-once, use the 2PC sink: `gcs_parquet_2pc_{int64,string}_sink` programmatically, or `delivery_guarantee='exactly_once'` in SQL with a `prefix` instead of a `key`. It stages one Parquet file per checkpoint interval under `<bucket>/<prefix>/staging` and promotes it to `<bucket>/<prefix>/committed` only when the checkpoint completes globally; a crash between pre-commit and commit is recovered on open. Read the result with the source pointed at `<prefix>/committed`.

The source reads a single Parquet object to its last row group and reports `is_bounded() == true` (`parquet_gcs_source.hpp`). It does not track or replay from an offset. On `open()` it validates that the file schema equals the `ArrowBatcher` schema (ignoring metadata) and throws on a mismatch.

## Limitations

- The source reads one object with `key`, or every object under `prefix` (a `prefix` instead of a `key` routes to the shared `MultiObjectParquetSource`, which lists the prefix, sorts, and shards objects round-robin across subtasks: object `i` is read by subtask `i % parallelism`). Optional multi-object source params: `recursive` (default `true`), `suffix` (default `.parquet`). There is no Hive-partition pruning or column projection on either path.
- Channels are limited to `int64` and `string`; there are no other typed factories.
- The factory sink rolls parts at checkpoint barriers only (no size-based rollover) and is at-least-once. For exactly-once use the 2PC sink (`prefix` + `delivery_guarantee='exactly_once'`), which stages and atomically promotes one file per checkpoint.
- `compression` and the per-record `bucket_assigner` exist on the sink `Options` but are not configurable through the factory parameters or SQL; they require the programmatic API. The compression default is ZSTD.
- `access_token` is static (fixed 24-hour expiry, no refresh); use `credentials_file`/`credentials_json` (service account) or Application Default Credentials, both of which auto-refresh, for a long-running job.
- The source requires the on-disk Parquet schema to match the `ArrowBatcher` schema exactly (metadata aside), otherwise `open()` throws.

## Testing

A live integration test exists at `impls/gcs/tests/test_parquet_gcs_live.cpp` (`GcsParquetLive.WriteThenReadRoundTrip`). It is skipped unless the environment variable `CLINK_GCS_TEST_ENDPOINT` is set to a GCS-API endpoint `host:port` (for example `localhost:4443` from a fake-gcs-server emulator). The test creates a bucket up front via `GcsFileSystem` (writes do not auto-create buckets), writes int64 records through the sink, then reads them back through the source and asserts the round trip.

A non-gated unit test, `impls/gcs/tests/test_factory_registration.cpp`, verifies that all four factories are registered on the runner registry without needing any GCS endpoint.
