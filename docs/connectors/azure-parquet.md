# Azure Blob Storage (Parquet)

> Reads and writes Parquet objects in Azure Blob Storage containers through Arrow's AzureFileSystem; available as both a source and a sink.

## Overview

This connector stores stream records as Parquet objects in an Azure Blob Storage container, using Arrow's `AzureFileSystem` as the transport. The sink (`ParquetAzureSink<T>`) writes records through the shared `ArrowBatcher<T>` seam and one `parquet::arrow::FileWriter` per blob key; the source (`ParquetAzureSource<T>`) reads a single Parquet object back, validates its schema against the batcher, and emits the rows. Two record channels are registered, `int64` and `string`, and the on-disk encoding is Parquet (ZSTD-compressed on the sink by default). Paths are formed as `<container>/<blob>`, with the storage account named separately.

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| Apache Arrow + Parquet | Compiled from source (`scripts/build-arrow.sh`) on both the macOS host and the Debian image, built with `ARROW_AZURE=ON` | 24.0.0 |
| azure-sdk-for-cpp | Statically bundled into `libarrow` by the Arrow build; no separate clink dependency | As bundled by Arrow 24.0.0 |

The module declares no client library of its own. Arrow built with `ARROW_AZURE=ON` statically bundles `azure-sdk-for-cpp` into `libarrow`, which `clink::core` already links, so the connector rides that same Arrow (see `impls/azure/CMakeLists.txt`).

## Enabling it

The build knob is `CLINK_WITH_AZURE` (`AUTO` / `ON` / `OFF`), defaulting to `AUTO` (`CMakeLists.txt`). The connector is gated on whether the linked Arrow was built with Azure support, detected via the `ARROW_AZURE` variable set by `find_package(Arrow)`:

- `AUTO`: the target is built when `ARROW_AZURE` is set, otherwise it is silently skipped.
- `ON` with an Arrow lacking Azure support is a fatal configure error.
- `OFF` never defines the target.

The build-env requirement is that the pinned Arrow is compiled with `-DARROW_AZURE=ON`, which `scripts/build-arrow.sh` already does. No additional apt or brew package is needed.

```bash
cmake -S . -B build -DCLINK_WITH_AZURE=ON
```

## Factories

| Factory name | Direction | Record type |
| --- | --- | --- |
| `azure_parquet_int64_sink` | Sink | `int64` |
| `azure_parquet_string_sink` | Sink | `string` |
| `azure_parquet_int64_source` | Source | `int64` |
| `azure_parquet_string_source` | Source | `string` |

Registered in `impls/azure/src/register_factories.cpp`.

## Configuration

All four factories share the same parameter parsing (`apply_azure_params` in `register_factories.cpp`). `container`, `key` and `account_name` are required; the call throws if any is empty.

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `container` | Yes | none | Azure Blob container name. First path segment of `<container>/<blob>`. |
| `key` | Yes | none | Blob key (object path) within the container. |
| `account_name` | Yes | none | Azure storage account name. |
| `anonymous` | No | `false` | `true` uses an anonymous credential (public container or emulator). Takes precedence over `account_key` / `sas_token` when set. |
| `account_key` | No | unset | Shared-key credential. Also how the Azurite emulator authenticates. |
| `sas_token` | No | unset | SAS-token credential. |
| `use_default_credential` | No | `false` | `true` forces the `DefaultAzureCredential` chain (environment, workload identity, managed identity, Azure CLI). |
| `blob_storage_authority` | No | unset | Override the blob endpoint authority (`host:port`), for example to target an Azurite emulator. Mirrored onto the DFS authority. |
| `blob_storage_scheme` | No | unset | Override the blob endpoint scheme, for example `http` for an emulator. Mirrored onto the DFS scheme. |

Authentication precedence (from `azure_detail::make_azure_options` in `parquet_azure_sink.hpp`): `anonymous`, then `account_key`, then `sas_token`, then `use_default_credential`. With none of these set, the `DefaultAzureCredential` chain is used.

The sink's `Options` struct (`parquet_azure_sink.hpp`) carries two further fields that are not exposed through `apply_azure_params` and are only reachable via the programmatic API:

- `compression` (`parquet::Compression::type`), default `ZSTD`.
- `bucket_assigner` (`std::function<std::string(const T&)>`), an optional per-record blob-key assigner for Hive-style partitioning. When set, `key` becomes optional.

When `parallelism > 1`, the sink factory suffixes the configured `key` with `.<subtask_idx>.parquet` so each subtask writes a distinct blob (`register_factories.cpp`). The source factory applies no such suffix.

## SQL usage

Mapped in `src/sql/physical_plan.cpp` as `connector='azure_parquet'`, resolving to `azure_parquet_string_source` and `azure_parquet_string_sink` (the string channel).

```sql
CREATE TABLE azure_out (
    line STRING
) WITH (
    connector = 'azure_parquet',
    account_name = 'mystorageacct',
    container = 'analytics',
    key = 'exports/output.parquet',
    sas_token = 'sv=2022-11-02&ss=b&srt=co&sp=rwl&...'
);
```

The same property keys apply to a source table. Authentication can instead use `account_key`, `anonymous = 'true'`, or `use_default_credential = 'true'`; for an Azurite emulator, set `blob_storage_authority` and `blob_storage_scheme = 'http'`.

## Example

Programmatic use with the typed sink and source classes, mirroring `impls/azure/tests/test_parquet_azure_live.cpp`:

```cpp
#include "clink/connectors/parquet_azure_sink.hpp"
#include "clink/connectors/parquet_azure_source.hpp"
#include "clink/core/arrow_batcher.hpp"

using clink::ParquetAzureSink;
using clink::ParquetAzureSource;
using clink::int64_arrow_batcher;

// Write a Parquet blob.
ParquetAzureSink<std::int64_t>::Options so;
so.container = "analytics";
so.key = "data.parquet";
so.account_name = "mystorageacct";
so.account_key = std::string("...");  // shared-key auth
ParquetAzureSink<std::int64_t> sink(so, int64_arrow_batcher());
sink.open();
clink::Batch<std::int64_t> b;
b.emplace(10);
b.emplace(20);
b.emplace(30);
sink.on_data(b);
sink.close();  // finalises the Parquet blob

// Read it back.
ParquetAzureSource<std::int64_t>::Options ro;
ro.container = "analytics";
ro.key = "data.parquet";
ro.account_name = "mystorageacct";
ro.account_key = std::string("...");
ParquetAzureSource<std::int64_t> src(ro, int64_arrow_batcher());
src.open();
clink::Emitter<std::int64_t> em([](clink::StreamElement<std::int64_t> e) -> bool {
    // consume e.as_data() ...
    return true;
});
while (src.produce(em)) {
}
src.close();
```

Through the registry, look the factory up by its registered name (for example `azure_parquet_string_sink`) and supply the parameters listed above via the `BuildContext`.

## Delivery semantics

The Parquet blob is finalised only when the sink's `close()` runs, which closes each writer and output stream and flushes Arrow's background write buffer. There is no two-phase commit and no per-checkpoint blob rotation in this connector, so a sink failure or job restart before `close()` can leave a partial or zero-length blob; the writer's `close()` is written to close and release every per-key stream before reporting the first error, so teardown does not strand other keys' streams (`parquet_azure_sink.hpp`). Treat the sink as at-least-once at best, with no exactly-once guarantee.

The source reads a single Parquet object to its last row group and reports `is_bounded() == true` (`parquet_azure_source.hpp`). It validates the file schema against the `ArrowBatcher` schema (ignoring metadata) and throws on mismatch. It carries no offset or replay tracking, so it is a bounded one-shot read rather than a checkpointed, replayable source.

## Limitations

- Sink commit is whole-blob at `close()`; no two-phase commit, no incremental or per-checkpoint commit, so failures can leave partial blobs.
- The source reads one object with `key`, or every object under `prefix` (a `prefix` instead of a `key` routes to the shared `MultiObjectParquetSource`, which lists the prefix, sorts, and shards objects round-robin across subtasks: object `i` is read by subtask `i % parallelism`). Optional multi-object source params: `recursive` (default `true`), `suffix` (default `.parquet`). Hive-partition pruning and column projection are not performed.
- Source has no replay or offset tracking; it is a bounded one-shot read.
- Record channels are limited to `int64` and `string`. Only those four factories are registered.
- The source enforces an exact schema match (excluding metadata) against the batcher and throws otherwise.
- `compression` and `bucket_assigner` on the sink are reachable only through the programmatic API; the factory parameter parsing does not expose them.
- Writes do not auto-create the container; it must exist beforehand (see the live test, which creates it via `AzureFileSystem::CreateDir`).

## Testing

Offline tests in `impls/azure/tests/test_parquet_azure.cpp` cover required-parameter validation and clean failure against a dead endpoint; they need no Azure access and run as part of the normal suite.

The end-to-end write-then-read round-trip lives in `impls/azure/tests/test_parquet_azure_live.cpp` and is skipped unless `CLINK_AZURE_TEST_ENDPOINT` is set. The variable is an Azure Blob endpoint authority (`host:port`), for example `127.0.0.1:10000` from an Azurite emulator. The test authenticates with Azurite's well-known development account (`devstoreaccount1`) and shared key over `http`, creates a per-PID container, writes three `int64` records through the sink, and reads them back through the source.

```bash
# With an Azurite emulator listening on the blob port (default 10000):
CLINK_AZURE_TEST_ENDPOINT=127.0.0.1:10000 ctest --test-dir build -R AzureParquetLive
```
