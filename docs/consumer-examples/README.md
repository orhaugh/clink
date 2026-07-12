# clink consumer examples

A working set of self-contained examples for consumers of the installed
`clink` library. Every example here is built from this directory alone -
no in-tree clink checkout required, just a system install of clink and
its dependencies.

## Prerequisites

1. clink installed on the system. See the [top-level README](../../README.md)
   for build/install instructions. The short version:
   ```bash
   cmake -S /path/to/clink -B build -DCMAKE_INSTALL_PREFIX=/usr/local
   cmake --build build --parallel 10
   sudo cmake --install build
   ```
2. A C++23 compiler and CMake 3.24+.

## Build & run

Out-of-tree, against the installed package:

```bash
cd docs/consumer-examples
cmake -S . -B build               # find_package(clink REQUIRED) does the rest
cmake --build build --parallel 10

./build/01_hello_pipeline
./build/02_event_time_tumbling
./build/03_sliding_window_aggregate
./build/04_keyed_process_state
./build/05_interval_join
./build/06_file_io
./build/07_parquet_io
```

If clink was installed under a non-standard prefix, set `CMAKE_PREFIX_PATH`
at configure time: `cmake -S . -B build -DCMAKE_PREFIX_PATH=/opt/clink`.

The plugin example (`08_cluster_job_plugin`) builds a .so and is gated:

```bash
cmake -S . -B build -DCLINK_BUILD_PLUGIN_EXAMPLE=ON
cmake --build build --target 08_cluster_job_plugin --parallel 10
# Then start a JM + TM with clink_node and submit with `clink run` - see
# the comment block at the top of 08_cluster_job_plugin.cpp.
```

## The examples

| # | File | Concepts covered |
|---|------|------------------|
| 01 | [`01_hello_pipeline.cpp`](01_hello_pipeline.cpp) | The bare-minimum DAG: `VectorSource` → `MapOperator` → `FilterOperator` → `FunctionSink`. `LocalExecutor::run()` blocks until the source is drained. |
| 02 | [`02_event_time_tumbling.cpp`](02_event_time_tumbling.cpp) | Keyed event-time tumbling windows. `KeyByOperator` partitions by user, `TumblingWindowOperator` aggregates per (key, window). Source emits `Watermark::max()` at end-of-stream, flushing all windows. |
| 03 | [`03_sliding_window_aggregate.cpp`](03_sliding_window_aggregate.cpp) | Sliding event-time windows. Same shape as #02 but the operator emits each window once per slide; each event contributes to `size/slide` overlapping windows. |
| 04 | [`04_keyed_process_state.cpp`](04_keyed_process_state.cpp) | `KeyedProcessFunction` + per-key persistent state via `RuntimeContext::keyed_state()`. Backed by `InMemoryStateBackend` here; swap to `RocksDbStateBackend` (linked via `clink::rocksdb`) for durability. |
| 05 | [`05_interval_join.cpp`](05_interval_join.cpp) | Two-stream interval join on a shared key with an event-time window `[lower, upper]`. Default is inner join; pass a `Dag::JoinType::LeftOuter` (and friends) for outer joins with watermark-driven emission of unmatched rows. |
| 06 | [`06_file_io.cpp`](06_file_io.cpp) | `FileSource<string>` + `FileSink<string>` against a `TextFormat<T>` codec. Word-count over newline-delimited input, written as TSV. |
| 07 | [`07_parquet_io.cpp`](07_parquet_io.cpp) | `ParquetSink<T>` + `ParquetSource<T>` over the shared `ArrowBatcher<T>` seam. The resulting file is a vanilla Parquet stream - open it from pyarrow / duckdb / polars to confirm. |
| 08 | [`08_cluster_job_plugin.cpp`](08_cluster_job_plugin.cpp) | Pipeline packaged as a job plugin `.so` using `StreamExecutionEnvironment` + `CLINK_REGISTER_JOB`. Submit to a running cluster via `clink run`. |
| 09 | [`09_testing_framework.cpp`](09_testing_framework.cpp) | Testing a stateful operator with `clink::test_support` (the public testing framework): a `KeyedProcessFunction` driven through `make_keyed_process_function_harness`, per-key state inspected via the production read path, and a snapshot → restore round trip. Links `clink::test_support` and registers with CTest (it exits non-zero on failure). |

## Picking targets vs. linking everything

The CMakeLists in this directory links every example to `clink::core`
(the engine, no connectors). If your real-world job uses Kafka, Postgres,
or S3, switch to:

```cmake
target_link_libraries(my_pipeline PRIVATE clink::core clink::kafka)
# or, the kitchen-sink alias:
target_link_libraries(my_pipeline PRIVATE clink::clink)
```

After `find_package(clink REQUIRED)`, the `clink_AVAILABLE_IMPLS`
variable holds the semicolon-separated list of impls that were installed
on this machine. Gate optional features on it:

```cmake
if("kafka" IN_LIST clink_AVAILABLE_IMPLS)
    target_compile_definitions(my_pipeline PRIVATE HAS_KAFKA=1)
    target_link_libraries(my_pipeline PRIVATE clink::kafka)
endif()
```

For tests, link `clink::test_support` alongside `clink::core` to pull in the
public testing framework (operator harnesses, state inspection, snapshot /
restore, deterministic time) - example 09 shows the pattern.

A lean install without any connectors is available: build clink with
`-DCLINK_BUILD_IMPLS=OFF` and its `clinkConfig` requires only Arrow / Parquet /
Threads (no connector SDKs), shipping `clink::core`, `clink::clink` and
`clink::test_support` without the runnable `clink_node`.

## Where to go next

* [`include/clink/api/stream_execution_environment.hpp`](../../include/clink/api/stream_execution_environment.hpp)
  - the higher-level fluent builder used by #08. Comparable to `DataStream` API.
* [`include/clink/runtime/dag.hpp`](../../include/clink/runtime/dag.hpp)
  - the lower-level DAG used by #01-#07. Comparable to `StreamGraph`. The fluent env lowers to this.
* [`docs/architecture.md`](../architecture.md) - engine concepts in
  more depth (watermarks, checkpoints, state, alignment, …).
* [`examples/`](../../examples/) - additional in-tree examples that are
  built and run as part of the upstream test suite.
