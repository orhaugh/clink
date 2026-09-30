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
| `azure_parquet_2pc_int64_sink` | Sink | `int64` (two-phase commit, one staged file per checkpoint) |
| `azure_parquet_2pc_string_sink` | Sink | `string` (two-phase commit, one staged file per checkpoint) |
| `azure_parquet_int64_source` | Source | `int64` |
| `azure_parquet_string_source` | Source | `string` |

Registered in `impls/azure/src/register_factories.cpp`.

## Configuration

The sources parse their parameters with `apply_azure_params`, and the sink factories with `azure_sink_fs_factory` (both in `register_factories.cpp`), sharing the auth and endpoint keys. `container` and `account_name` are always required; a source needs `key` or `prefix`, a sink `prefix` (or `key`, taken as the prefix).

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `container` | Yes | none | Azure Blob container name. First path segment of `<container>/<blob>`. |
| `prefix` | Sink: one of `prefix`/`key` | none | Sink: the prefix its part blobs are written under (`container/prefix/sub<N>-...parquet`). Source: read every matching blob under it. |
| `key` | Source: one of `key`/`prefix` | none | Source: the single blob `container/key`. Sink: accepted as the prefix, for configurations written before the sink wrote a directory. |
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

Each subtask names its part blobs `sub<N>-<run>-<seq>.parquet`, so parallel subtasks and successive runs never write the same key (`register_factories.cpp`).

## SQL usage

Mapped in `src/sql/physical_plan.cpp` as `connector='azure_parquet'`, resolving to `azure_parquet_string_source` and `azure_parquet_string_sink` (the string channel).

```sql
CREATE TABLE azure_out (
    line STRING
) WITH (
    connector = 'azure_parquet',
    account_name = 'mystorageacct',
    container = 'analytics',
    prefix = 'exports/output',
    sas_token = 'sv=2022-11-02&ss=b&srt=co&sp=rwdl&...'   -- read, write, delete, list
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

The factory sink is at-least-once and writes a directory of part objects under `container/prefix` through the shared `ParquetRollingSink<T>` (`include/clink/connectors/parquet_rolling_sink.hpp`). Each subtask closes a part at every checkpoint barrier and at the end of input. A part is written as `<name>.inprogress` and copied to its final `.parquet` key when complete, then the in-progress object is deleted: Arrow's Azure stream creates the blob as soon as it opens, so writing to the final key would expose an empty blob for the whole interval and leave it behind after a kill. So the prefix never shows a partial `.parquet` object, everything up to the last barrier is already published when a kill lands, and a leftover `.inprogress` object is skipped by readers and removed by the next run. A run restored from a checkpoint keeps every part, including those past the restore point, whose rows the sources then replay; a run that starts from empty state deletes the previous run's parts under the prefix (only keys named exactly as the sink names its parts). The sink therefore needs list and delete permission as well as write. Read the directory back with the source on the same `prefix`. The programmatic `ParquetAzureSink<T>` writes a single object and finalises it only in `close()`.

For exactly-once, use the 2PC sink: `azure_parquet_2pc_{int64,string}_sink` programmatically, or `delivery_guarantee='exactly_once'` in SQL with a `prefix` instead of a `key`. It stages one Parquet blob per checkpoint interval under `<container>/<prefix>/staging` and promotes it to `<container>/<prefix>/committed` only when the checkpoint completes globally; a crash between pre-commit and commit is recovered on open. Read the result with the source pointed at `<prefix>/committed`.

The source reads a single Parquet object to its last row group and reports `is_bounded() == true` (`parquet_azure_source.hpp`). It validates the file schema against the `ArrowBatcher` schema (ignoring metadata) and throws on mismatch. It carries no offset or replay tracking, so it is a bounded one-shot read rather than a checkpointed, replayable source.

## Limitations

- The factory sink rolls parts at checkpoint barriers only (no size-based rollover) and is at-least-once. For exactly-once use the 2PC sink (`prefix` + `delivery_guarantee='exactly_once'`), which stages and atomically promotes one blob per checkpoint.
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
