# WebHDFS / HttpFS (Parquet)

> Reads and writes Parquet files on HDFS over the WebHDFS / HttpFS REST API. Both a source and a sink.

## Overview

This connector moves Parquet data to and from HDFS using the WebHDFS / HttpFS HTTP protocol rather than the native HDFS client. It reuses `clink::http_connector` (the vendored cpp-httplib client) for transport and clink's Arrow/Parquet for encoding, so there is no JVM and no `libhdfs` dependency. The sink builds a single Parquet file in memory and uploads it on `close()`; the source fetches a single Parquet file whole and parses it through Arrow's Parquet reader. Both ride the shared `ArrowBatcher<T>` seam, with `int64` and `string` record channels registered as built-in factories. Transport uses WebHDFS's two-step request model: a `CREATE` or `OPEN` call that returns a `307 Temporary Redirect` to a datanode `Location`, followed by the data transfer to that location (see `impls/webhdfs/include/clink/connectors/webhdfs_parquet_sink.hpp`).

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| `clink::http_connector` (cpp-httplib) | Vendored into `impls/http_connector` | Vendored, not separately pinned |
| Arrow / Parquet | Compiled from source into `CLINK_DEPS_PREFIX` on both host and image | 24.0.0 |

The module adds no client library of its own. Its only build dependency is the `clink::http_connector` target, which carries the vendored HTTP client. TLS (`https://` endpoints) requires the `http_connector` module to have been built with OpenSSL; the constructor throws `std::invalid_argument` if an `https://` `base_url` is supplied to a build without TLS support (`webhdfs_parquet_sink.hpp`, `webhdfs_parquet_source.hpp`).

## Enabling it

The CMake knob is `CLINK_WITH_WEBHDFS` (`AUTO` / `ON` / `OFF`, default `AUTO`), declared in the top-level `CMakeLists.txt`. The module is gated on the presence of `clink::http_connector`: with `AUTO` it is built only when the HTTP connector is available, and with `ON` it fails configuration if the HTTP connector (which itself needs `CLINK_WITH_HTTP` and the vendored httplib) is absent (`impls/webhdfs/CMakeLists.txt`). It adds no apt or brew package and no Arrow build flag of its own.

```bash
cmake -S . -B build -DCLINK_WITH_WEBHDFS=ON
```

## Factories

| Factory name | Direction | Record type |
| --- | --- | --- |
| `webhdfs_parquet_int64_sink` | sink | `int64` |
| `webhdfs_parquet_string_sink` | sink | `string` |
| `webhdfs_parquet_int64_source` | source | `int64` |
| `webhdfs_parquet_string_source` | source | `string` |

Exact registered names from `impls/webhdfs/src/register_factories.cpp`.

## Configuration

The keys below are parsed from `BuildContext` in `register_factories.cpp` (`apply_common_params`, `make_sink`, `make_source`) and map onto the `Options` structs in the sink and source headers. `base_url` and `path` are required; the factory throws `webhdfs_parquet: 'base_url' and 'path' are required` if either is empty.

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `base_url` | Yes | (none) | WebHDFS NameNode or HttpFS gateway root, e.g. `http://nn:9870` or `http://httpfs:14000`. |
| `path` | Yes | (none) | HDFS file path, e.g. `/clink/out.parquet`. |
| `user` | No | unset | Sets `user.name` on the WebHDFS request (the HDFS user). Auth option. |
| `delegation_token` | No | unset | Sets the `delegation` query parameter. Auth option. |
| `verify_tls` | No | `true` | When `false`, skips server-certificate verification for `https://` endpoints. |
| `connect_timeout_ms` | No | `5000` | HTTP connect timeout in milliseconds. Applied only when supplied and greater than 0. |
| `rw_timeout_ms` | No | `30000` | HTTP read/write timeout in milliseconds. Applied only when supplied and greater than 0. |
| `overwrite` | No (sink only) | `true` | Sets the `overwrite` query parameter on `CREATE`. |
| `permission` | No (sink only) | unset | Sets the `permission` query parameter (octal, e.g. `644`). |

Notes:

- The source accepts `base_url`, `path`, `user`, `delegation_token`, `verify_tls`, `connect_timeout_ms` and `rw_timeout_ms`. `overwrite` and `permission` apply to the sink only.
- The sink's `Options` struct also carries a `compression` field defaulting to `parquet::Compression::ZSTD`; this is not parsed from `BuildContext` and so is fixed to the default through the factory path.
- When `parallelism > 1`, the sink appends `.<subtask_idx>.parquet` to `path` so each subtask writes a distinct file (`make_sink` in `register_factories.cpp`).

## SQL usage

Mapped in `src/sql/physical_plan.cpp` to the connector string `webhdfs_parquet` (string channel only: `webhdfs_parquet_string_source` / `webhdfs_parquet_string_sink`).

```sql
CREATE TABLE hdfs_out (
    v STRING
) WITH (
    connector = 'webhdfs_parquet',
    base_url  = 'http://httpfs:14000',
    path      = '/clink/out.parquet',
    user      = 'clink',
    overwrite = 'true'
);
```

## Example

Based on the round-trip in `impls/webhdfs/tests/test_webhdfs_parquet.cpp`: write three `int64` records to a Parquet file on HDFS, then read them back.

```cpp
#include "clink/connectors/webhdfs_parquet_sink.hpp"
#include "clink/connectors/webhdfs_parquet_source.hpp"
#include "clink/core/arrow_batcher.hpp"

using clink::Batch;
using clink::Emitter;
using clink::int64_arrow_batcher;
using clink::StreamElement;
using clink::WebHdfsParquetSink;
using clink::WebHdfsParquetSource;

const std::string base_url = "http://httpfs:14000";  // HttpFS gateway root
const std::string path = "/clink/out.parquet";

// Sink: builds the Parquet file in memory, uploads it on close().
{
    WebHdfsParquetSink<std::int64_t>::Options so;
    so.base_url = base_url;
    so.path = path;
    so.overwrite = true;
    WebHdfsParquetSink<std::int64_t> sink(so, int64_arrow_batcher());
    sink.open();
    Batch<std::int64_t> b;
    b.emplace(10);
    b.emplace(20);
    b.emplace(30);
    sink.on_data(b);
    sink.close();  // finalises and uploads
}

// Source: fetches the file whole, parses it, emits records.
std::vector<std::int64_t> got;
Emitter<std::int64_t> em([&](StreamElement<std::int64_t> e) -> bool {
    if (e.is_data()) {
        for (const auto& rec : e.as_data()) {
            got.push_back(rec.value());
        }
    }
    return true;
});
WebHdfsParquetSource<std::int64_t>::Options ro;
ro.base_url = base_url;
ro.path = path;
WebHdfsParquetSource<std::int64_t> src(ro, int64_arrow_batcher());
src.open();
while (src.produce(em)) {
}
src.close();
```

In a deployed job the factories are usually looked up by name from the plugin registry (`webhdfs_parquet_int64_sink` and so on) rather than constructed directly.

## Delivery semantics

The sink is at-least-once. The Parquet file is created and finalised in a single upload on `close()`; a failure mid-upload throws and the job replays from the last checkpoint (`webhdfs_parquet_sink.hpp`). There is no two-phase commit and no incremental upload. The two-step write fails loudly if `CREATE` does not return a `307` redirect to a datanode, because `CREATE` carries no body and a non-redirect `2xx` would create an empty file rather than upload the Parquet bytes; a gateway that only does single-request inline writes is not supported.

The source reports `is_bounded() == true`: it reads a single Parquet object to its last row group and then stops. On `OPEN` it follows a `307` redirect and GETs the datanode bytes, or accepts a direct `2xx` body if the gateway returns the file inline (`webhdfs_parquet_source.hpp`).

## Limitations

- Single-object source: it reads one Parquet file. There is no directory or glob support (noted as the obvious follow-up in `webhdfs_parquet_source.hpp`).
- The sink buffers the whole Parquet file in memory before upload, since WebHDFS has no incremental Arrow output stream; file size is bounded by available memory.
- The sink is at-least-once only, with no two-phase commit.
- Record channels are limited to the registered `int64` and `string` types.
- SQL exposes the `string` channel only.
- Sink `compression` is fixed at ZSTD through the factory path; it is not configurable via the SQL or `BuildContext` keys.
- A datanode `Location` returned by a NameNode must be resolvable by this process. An HttpFS gateway (single endpoint, no per-datanode redirect target) avoids that constraint and is the simplest target.
- `https://` endpoints require the `http_connector` module to have been built with OpenSSL; otherwise the constructor throws.

## Testing

In-process tests run without external Hadoop. `impls/webhdfs/tests/test_webhdfs_parquet.cpp` stands up an httplib server that emulates the WebHDFS / HttpFS `CREATE` / `OPEN` 307-redirect protocol over a real socket and exercises required-param validation, clean failure against a dead endpoint, and `int64` and `string` write-then-read round-trips.

A gated live test, `impls/webhdfs/tests/test_webhdfs_parquet_live.cpp`, runs the same round-trip against a real WebHDFS NameNode or HttpFS gateway. It is skipped unless `CLINK_WEBHDFS_TEST_ENDPOINT` is set (e.g. `http://127.0.0.1:14000` for an HttpFS gateway). The optional `CLINK_WEBHDFS_TEST_USER` sets `user.name` to an HDFS user with write permission. No specific Docker image is named by the test; point the endpoint at any reachable WebHDFS or HttpFS instance.
