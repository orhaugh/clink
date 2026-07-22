# clink

[![ci](https://github.com/orhaugh/clink/actions/workflows/ci.yml/badge.svg)](https://github.com/orhaugh/clink/actions/workflows/ci.yml)
[![licence](https://img.shields.io/badge/licence-Apache--2.0-blue.svg)](LICENSE)
[![changelog](https://img.shields.io/badge/changelog-v0.1.0-lightgrey.svg)](CHANGELOG.md)

`clink` is a semantics-first, Arrow-native, stateful stream
processing engine in modern C++ (C++23).

Most native stream engines fall into one of two camps: small projects that
skip the hard correctness work (no event time, no checkpoints, no keyed
state), or production systems tied so closely to a particular runtime
(Seastar, gRPC, Arrow Flight) that they are hard to evolve. clink is built
from the primitives that matter - typed operator DAGs, in-band watermarks,
in-band checkpoint barriers, keyed state, bounded backpressure - without
binding them to a particular runtime, executor, or wire format.

clink is heavily inspired by Apache Flink. Flink's model of typed operator
DAGs, event-time processing, in-band watermarks and checkpoint barriers, keyed
state, and exactly-once semantics is the conceptual foundation this engine
builds on. clink reworks that model in modern C++ around Arrow-native columnar
execution and JVM-free deployment.

The core is implemented and covered by tests: the operator model, in-process
and distributed runtimes, event-time windowing, exactly-once checkpointing
(with rescale and restart-from-checkpoint), in-memory / RocksDB / changelog /
file-backed state, the Arrow columnar wire format, a Coordinator/Worker
cluster control plane, the connector suite, and a SQL frontend. It is still a
young project rather than a battle-tested production deployment: some features
are config-gated or carry correctness caveats, and those are called out in the
Status section below.

## Contents

- [Quick start](#quick-start)
- [Status](#status)
- [Build & test](#build--test)
  - [Reproducible build + sanitizer matrix](#reproducible-build--sanitizer-matrix)
  - [Optional dependencies](#optional-dependencies)
- [Connectors](#connectors)
- [Internals](#internals)
- [Installing clink](#installing-clink)
- [Using clink as a library](#using-clink-as-a-library)
  - [One-call default registration](#one-call-default-registration)
  - [Typed fluent helpers (Kafka, ClickHouse, Avro, S3 Parquet)](#typed-fluent-helpers-kafka-clickhouse-avro-s3-parquet)
- [Code examples](#code-examples)
  - [Hello pipeline (map → filter → sink)](#hello-pipeline-map--filter--sink)
  - [Event-time tumbling window](#event-time-tumbling-window)
  - [Keyed state in a process function](#keyed-state-in-a-process-function)
  - [Interval join across two streams](#interval-join-across-two-streams)
  - [Cluster job plugin (Pipeline)](#cluster-job-plugin-pipeline)
- [SQL frontend](#sql-frontend)
- [Time-travel debugging](#time-travel-debugging)
- [State as data](#state-as-data)
- [Testing your pipelines](#testing-your-pipelines)
- [Running the in-tree examples](#running-the-in-tree-examples)
- [Linting & formatting](#linting--formatting)
- [Repository layout](#repository-layout)
- [Reading order (for new contributors)](#reading-order-for-new-contributors)
- [Versioning and stability](#versioning-and-stability)
- [Getting help](#getting-help)
- [License and attribution](#license-and-attribution)

## Quick start

Everything builds from source today (prebuilt images and wheels are
planned). The one-time bootstrap compiles a pinned Apache Arrow into
`~/.clink-deps` (20-40 minutes, cached across builds); the clink build
itself is quick.

```bash
git clone https://github.com/orhaugh/clink && cd clink
scripts/build-arrow.sh && scripts/build-iceberg-cpp.sh   # one-time (slow, cached)
cmake -S . -B build -DCLINK_BUILD_SQL=ON
cmake --build build --parallel 10
```

Run a first pipeline - one process, no daemons to set up:

```bash
printf '{"usr":"alice","amount":12}\n{"usr":"bob","amount":7}\n{"usr":"alice","amount":5}\n' > /tmp/orders.ndjson

./build/clink run -e "CREATE TABLE orders (usr VARCHAR, amount BIGINT) \
      WITH (connector='file', format='json', path='/tmp/orders.ndjson'); \
    SELECT usr, SUM(amount) AS total FROM orders GROUP BY usr"
```

```
{"total":12,"usr":"alice"}
{"total":7,"usr":"bob"}
{"total":17,"usr":"alice"}
```

Totals stream as they update; the last row per key is the current
aggregate. From here:

- a local distributed cluster with the dashboard:
  `docker build -t clink-build:latest -f docker/Dockerfile .` then
  `docker compose up --build`, and open <http://localhost:8081>
  (see [docker-compose.yml](docker-compose.yml));
- the same engine embedded in Python, results as Arrow:
  [python/README.md](python/README.md);
- the full C++ API: [Code examples](#code-examples) below and the
  compileable [`docs/consumer-examples/`](docs/consumer-examples/).

Supported platforms: macOS (Apple Silicon is the primary development
platform) and Linux (Debian-family, exercised in CI). Windows is not
supported.

## Status

Grouped by area. A capability is listed as implemented only where it is backed
by code and tests. Where a feature is config-gated, partial, or carries a
correctness caveat, the caveat is stated in the row.

### Runtime and operators

| Subsystem                   | Status                                              |
|----------------------------|-----------------------------------------------------|
| Core domain types          | implemented                                         |
| Local in-process runtime   | implemented (`LocalExecutor`, one `std::jthread` per operator) |
| Bounded channel / backpressure | implemented                                     |
| Map / Filter operators     | implemented                                         |
| ReduceOperator             | per-key streaming reduce, `OnEachInput` or `OnFlush` emit modes |
| KeyBy operator             | implemented; per-subtask keyed-state isolation (no in-operator cross-partition reshuffle) |
| Operator parallelism       | `Dag::add_parallel_{source,operator,operator_shuffled,sink}`; hash-partitioned shuffle on `add_parallel_operator_shuffled`; per-subtask `OperatorId` + `RuntimeContext` so keyed state stays isolated across subtasks |
| Branching DAG              | broadcast `Dag::fork<T>` (tee to N branches)        |
| Multi-input alignment      | `MultiInputAlignment` + `Dag::union_streams<T>(...)` (watermark = min, Chandy-Lamport barrier alignment) |
| Fluent API                 | `Pipeline` / `DataStream<T>` builder chain |
| Operator-thread errors     | `LocalExecutor::operator_errors()` captures worker-thread exceptions instead of terminating the process |
| Stable `OperatorId`        | hash of `(stage_index, operator_name)`; same DAG → same ids across restarts |
| Columnar execution         | `ArrowBatcher<T>`; operators opt in via `supports_columnar()` + `process_columnar()` and read columns through `Batch<T>::is_columnar()` / `arrow()`; built-in int/string batchers, binary-column fallback for user types |

### Time, windows, CEP

| Subsystem                   | Status                                              |
|----------------------------|-----------------------------------------------------|
| Watermarks                 | assigner + strategies (monotonic, bounded-out-of-orderness) |
| Tumbling window            | implemented (flush hook fires residual windows)     |
| Sliding window             | implemented; supports custom triggers, `allowed_lateness`, `late_output_tag` |
| Session window             | implemented; supports `allowed_lateness`, `late_output_tag` (no custom triggers). Evictors are not supported on any window operator |
| Interval join              | `Dag::interval_join<A,B,K,C>`; keyed stream-stream join with `[lower, upper]` event-time window, watermark-driven eviction, all 8 join types (`Inner`, `Left/Right/FullOuter`, `Left/RightSemi`, `Left/RightAnti`), `KeyedState`-backed persistence, `LateArrivalPolicy::{Allow, Drop}` with per-side drop counters, self-join via `fork` + `interval_join` |
| Complex event processing   | `CEPOperator` + fluent `Pattern` DSL (NFA-based). v1 subset: linear patterns, greedy quantifiers, strict/relaxed contiguity. Also reachable from SQL `MATCH_RECOGNIZE` |

### State and checkpointing

| Subsystem                   | Status                                              |
|----------------------------|-----------------------------------------------------|
| Operator-side keyed state  | `RuntimeContext::keyed_state<K,V>` over `Codec<T>` codecs |
| Broadcast state            | `RuntimeContext::broadcast_state<V>` for non-keyed values; `Dag::broadcast_connect<Main, Brod, Out, State>` |
| In-memory state backend    | implemented (Arrow IPC snapshot/restore)            |
| RocksDB state backend      | always built (bundled via `FetchContent`); native SST checkpoints, incremental via hard-link |
| ForSt state backend        | opt-in (`CLINK_WITH_FORST=ON`, built from the pinned upstream tag via ExternalProject); `forst://` and `changelog+forst://`, behavioural parity with the RocksDB backend, coexists with it in one binary. `s3+forst://` / `changelog+s3+forst://` publish checkpoints to object storage with the async-persist split; `s3sst+forst://` runs the engine with its immutable data files LIVE on object storage (metadata local), so state is bounded by the object store, not local disk |
| Changelog state backend    | write-ahead log + periodic materialisation; in-blob or external-store modes |
| File-backed state backend  | Arrow IPC snapshots                                 |
| State recovery on startup  | `JobConfig::restore_from` runs `StateBackend::restore` before any operator |
| Checkpoint coordinator     | barrier creation + per-operator ack tracking; barriers injected into sources |
| Exactly-once               | snapshot-on-barrier + a generic sink committer (`CommittingSink`: prepare at barrier, commit when the checkpoint is globally durable, recover-and-re-commit at open), source-offset recovery. True 2PC/atomic-publish sinks: file, Kafka, Parquet (local + object-store), raw S3 (multipart-complete-on-commit), and Postgres (`PREPARE TRANSACTION`). A separate effectively-once `mode='upsert'` path (changelog upsert + delete by PRIMARY KEY) covers Postgres/MySQL/Cassandra/Redis. Other sinks are at-least-once. See `docs/internals/sink-committer-framework.md` |
| Unaligned checkpoints      | barriers overtake in-flight records at multi-input operators (captured into state for replay); mode set per `JobConfig`. An adaptive backpressure-driven resolver seam exists; the backpressure signal wiring is left to the hosting runtime |
| Checkpoint rescale         | key-group repartitioning on deploy; scale-up and scale-down for file / RocksDB / changelog backends without full replay. Caveat: sources must store offsets as operator-state to survive rescale |
| Async snapshot worker      | off-thread durable write for FileBacked and disk-backed changelog backends; capture runs on the operator thread, persist + ack run on the worker. In-memory and RAM-only changelog stay synchronous |
| Fsync durability           | `write_fsync_rename` fsyncs file + directory before checkpoint ack; toggle off with `CLINK_STATE_FSYNC=0` for fsync-hostile CI or throughput benchmarks |
| State schema evolution     | migrate-at-restore via a migration registry; version map carried in `JobGraphSpec`; pre-deploy compatibility gate (best-effort, real enforcement at restore time) |
| Savepoint / state-processor| offline savepoint read + transform API (`state_processor/savepoint.hpp`); tested |
| Queryable state            | Live keyed state as a serving surface: a SQL GROUP BY exposes its running aggregates automatically, and `GET /api/v1/queryable_state/job/:id/op/:role/json/agg?key=alice` on the Coordinator returns the key's current finalised row as JSON - no sink round-trip, no user code, readable mid-run. Whole-slot scans back state-as-table: `connector='queryable_state'` lets one job SELECT from (or join against) another job's live state, and `.../scan.arrow` returns the same scan as one Arrow IPC stream over plain HTTP - live state as a pyarrow/polars/duckdb DataFrame, no client library, no gRPC. Byte-level slots + coordinator key routing remain for custom operators; `RemoteReadBackend` offers two-tier reads (hot in-memory tier + remote tier) |

### Distributed and cluster

| Subsystem                   | Status                                              |
|----------------------------|-----------------------------------------------------|
| TCP transport              | `NetworkChannelSink<T>` / `NetworkChannelSource<T>` round-trip `StreamElement<T>`s over TCP with length-prefixed framing; same push/pop semantics as `BoundedChannel` |
| Distributed bridges        | `NetworkBridgeSink<T>` / `NetworkBridgeSource<T>` adapt the TCP transport into the operator interface so two `LocalExecutor`s link via a `NetworkChannel` |
| Coordinator / Worker   | cluster control plane: binary length-prefixed protocol over TCP, register/deploy/finish/cancel/heartbeat; coordinator watchdog declares lost workers at `heartbeat_timeout` and broadcasts `CancelJob`; `JobGraphSpec` carries serialized `(op_type, params)` chains; `clink_node --role=coordinator|worker` for multi-process deployment (verified end-to-end via a `posix_spawn` integration test) |
| Failover / restart-from-checkpoint | coordinator watchdog detects a lost worker, surviving subtasks drain (bounded by `restart_drain_timeout`), then the job redeploys from the latest completed checkpoint with state restored per subtask. Default is fail-fast: `max_restarts_on_worker_loss = 0` and a checkpoint dir must be configured |
| Autoscaler                 | load-driven rescale trigger (`autoscaler.hpp`)      |
| TLS / mTLS transport       | `clink_node --tls-cert` / `--tls-ca` (and `--tls-client-*` for mTLS) install custom accept/connect factories on coordinator and worker |
| etcd HA coordinator        | leader election via etcd v3 (optional, `CLINK_WITH_ETCD`). Job persistence is filesystem-backed (`--ha-dir`) regardless of the election backend |
| HTTP / JSON API + dashboard| `CLINK_BUILD_HTTP` (on by default): `clink_node` serves `/api/v1/jobs`, `/api/v1/jobs/:id`, `/api/v1/cluster`, `/metrics` (Prometheus), an SSE event stream, and an embedded SPA at `/`. Off unless `--http-port` is set (default 0 = disabled) |

### Connectors

| Connector                   | Status                                              |
|----------------------------|-----------------------------------------------------|
| File source / sink         | header-only, `TextFormat<T>` codec                  |
| Parquet source / sink      | header-only, ZSTD-compressed, shares `ArrowBatcher<T>` with the wire; `parquet_{int64,string}_{sink,source}` built-ins; files readable by pyarrow / duckdb / polars |
| S3 + Parquet source / sink | reads/writes Parquet directly to `s3://bucket/key` via Arrow's S3FileSystem; AWS credential chain; `endpoint_override` for MinIO / localstack |
| Kafka source / sink        | typed `message_source` / `message_sink`; behind `CLINK_WITH_KAFKA` |
| PostgresSource             | snapshot reader: runs a query, emits typed `PostgresRow`s, closes |
| PostgresCdcSource          | streaming CDC via `test_decoding` and `pgoutput`; type fidelity via OID table; Standby Status Update keepalives; optional snapshot-then-stream |
| Postgres sink              | typed insert sink; all three behind `CLINK_WITH_POSTGRES` |
| ClickHouse source / sink   | typed source + typed sink (row-binary / JSONEachRow); behind `CLINK_WITH_CLICKHOUSE` |
| Avro                       | header-only `binary_codec` / `json_codec` / `keyed_record_codec`; behind `CLINK_WITH_AVRO`; no registration step required |

### SQL and Table API

See the [SQL frontend](#sql-frontend) section for the full supported-feature
list. Built behind `CLINK_BUILD_SQL=ON` (default off). A programmatic Table API
produces the same `JobGraphSpec` as SQL.

### Observability and wire format

| Subsystem                   | Status                                              |
|----------------------------|-----------------------------------------------------|
| Metrics registry           | implemented (counter + gauge)                       |
| Data lineage               | per-job source/sink dataset graph (plus column-level lineage for SQL) over `GET /api/v1/jobs/:id/lineage` and the event stream; pluggable `LineageListener` export with a built-in OpenLineage exporter |
| OpenTelemetry integration  | boundary defined (`otel_boundary.hpp`); no exporter wired yet |
| Arrow wire format          | every operator-to-operator data frame is an Arrow IPC stream (`Kind::ArrowBatch=7`); columnar schemas for built-in types, binary-column fallback for user types; control frames stay compact; `clink_serde_bench` shows roughly 4-9x over the per-record `Codec<T>` path |

## Build & test

The fast path:

```bash
cmake -S . -B build
cmake --build build --parallel 10
ctest --test-dir build --parallel 8
```

Common test labels: `-L core` runs `clink_core_tests`; `-L kafka`, `-L postgres`,
`-L clickhouse`, `-L s3`, `-L rocksdb`, `-L tls`, `-L integration` hit the
per-impl test exes.

Required deps on the host: a C++23 compiler (Clang 17+ / GCC 13+), CMake
3.24+, and Apache Arrow + Parquet at the pinned version - run
`scripts/build-arrow.sh` once to build it into `~/.clink-deps`, where every
configure finds it automatically. GoogleTest is pulled in via `FetchContent`
so you don't have to install it.

### Reproducible build + sanitizer matrix

`build_and_test.sh` drives the build in a dedicated directory per
configuration; each sanitizer pass uses its own build dir (`build`,
`build-asan`, ...) and is wiped on success.

```bash
./build_and_test.sh                       # normal build + ctest
./build_and_test.sh --sanitizer asan      # AddressSanitizer
./build_and_test.sh --sanitizer tsan      # ThreadSanitizer (Kafka tests skipped - librdkafka isn't TSan-clean)
./build_and_test.sh --sanitizer ubsan     # UndefinedBehaviourSanitizer
./build_and_test.sh --sanitizer all       # normal + asan + tsan + ubsan, sequentially
./build_and_test.sh --sanitizer coverage  # coverage build + lcov / SonarQube reports under coverage-report/
```

For the hermetic CI-matching path, run the same script inside the project's
Debian 13 image (the image bakes in `librdkafka`, `aws-sdk-cpp`,
`clickhouse-cpp`, etc.):

```bash
docker build -t clink-build:latest -f docker/Dockerfile .
./build_and_test.sh --image clink-build:latest --sanitizer all
```

First image build is ~20-30 minutes because it compiles `aws-sdk-cpp` and
`clickhouse-cpp` from source; subsequent builds reuse the cached layers.

### Optional dependencies

Each connector / state-backend impl is gated by `find_package()`. When the
dep is present, `clink::<impl>` is built; when absent, the impl module
returns early and the target is simply not defined - call sites should
guard on `if(TARGET clink::<impl>)`. The table below covers the core deps; the
[connector reference](docs/connectors/README.md) documents every connector and
its pinned version in full.

| Connector / backend     | Detected via                                    | Debian package(s) / source              |
|-------------------------|-------------------------------------------------|-----------------------------------------|
| Apache Arrow + Parquet  | `find_package(Arrow)` + `find_package(Parquet)` | `libarrow-dev libparquet-dev` (required)|
| RocksDB state backend   | bundled via `FetchContent` (always built)       | upstream tag pinned in CMake            |
| ForSt state backend     | opt-in `CLINK_WITH_FORST=ON` (ExternalProject)  | upstream tag pinned in CMake            |
| Kafka source / sink     | three-tier: CMake → pkg-config → manual probe   | `librdkafka-dev librdkafka++1`          |
| S3 + Parquet S3         | `find_package(AWSSDK COMPONENTS s3)` + Arrow S3 | built from source in setup script       |
| ClickHouse source / sink| CMake config → manual probe fallback            | built from source in setup script       |
| PostgreSQL source / CDC | `find_package(PostgreSQL)`                      | `libpq-dev`                             |
| Avro codec              | `find_package(AvroCpp)`                         | `avro-cpp` (source / homebrew)          |
| TLS transport           | `find_package(OpenSSL)`                         | `libssl-dev`                            |
| etcd HA coordinator     | `find_package(etcd-cpp-apiv3)`                  | built from source                       |

Each `CLINK_WITH_*` cache variable is `AUTO` by default (the dep is used when
found and skipped otherwise). Set `=ON` to make a missing dep a hard configure
error or `=OFF` to always skip the impl. RocksDB is the exception: it is always
built and cannot be turned off. Separately, the `CLINK_BUILD_*` options toggle
whole subsystems: `CLINK_BUILD_TESTS` / `CLINK_BUILD_EXAMPLES` (on),
`CLINK_BUILD_HTTP` (on), `CLINK_BUILD_SQL` (off), `CLINK_BUILD_BENCH` (off).

## Installing clink

clink ships as a regular CMake package: install it once, then consume it
from any downstream project with `find_package(clink REQUIRED)`.

```bash
cmake -S . -B build -DCLINK_BUILD_TESTS=OFF -DCLINK_BUILD_EXAMPLES=OFF \
                    -DCMAKE_INSTALL_PREFIX=/usr/local
cmake --build build --parallel 10
sudo cmake --install build
```

This writes:

| Path                               | Contents                                                                 |
|------------------------------------|--------------------------------------------------------------------------|
| `<prefix>/include/clink/`          | Public headers (core + every built impl)                                 |
| `<prefix>/lib/libclink_core.a`     | Engine static library                                                    |
| `<prefix>/lib/libclink_<impl>.a`   | One static lib per built impl (`kafka`, `postgres`, `s3`, `rocksdb`, …)  |
| `<prefix>/lib/librocksdb.a`        | Bundled RocksDB (transitive dep of `clink::rocksdb`)                     |
| `<prefix>/bin/clink`               | client CLI (`run`, `run-application`, `cancel`, `savepoint`, `check-savepoint`, `rescale`, `rescale-op`, `list`) |
| `<prefix>/bin/clink_node`          | Server daemon (run as `--role=coordinator` or `--role=worker` for cluster mode)       |
| `<prefix>/bin/clink_{submit_job,app,cancel_job,rescale_job,savepoint}` | Standalone client binaries (the `clink` subcommands dispatch to these) |
| `<prefix>/lib/cmake/clink/`        | `clinkConfig.cmake`, `clinkTargets.cmake`, version file                  |

`clink_submit_sql` is installed when `CLINK_BUILD_SQL=ON`. `clink_check_savepoint`
and `clink_rescale_op` are built but not added to the install rule; copy them
from the build directory or extend `install(TARGETS ...)` if you need them on
the path.

Install to a non-system prefix (e.g. `~/.local` or `/opt/clink`) by setting
`CMAKE_INSTALL_PREFIX` accordingly; downstream consumers then need
`CMAKE_PREFIX_PATH` pointed at it.

## Connectors

clink ships source and sink connectors for messaging systems, object stores,
databases, and HTTP / observability endpoints. Each is an optional module gated
by a `CLINK_WITH_<NAME>` CMake option (default `AUTO`), and each has a reference
page covering its dependency and pinned version, the exact factory names, every
configuration option, SQL usage where available, an example, and delivery
semantics.

The full set lives in the [connector reference](docs/connectors/README.md). In brief:

- Messaging and streaming: [Kafka](docs/connectors/kafka.md), [Pulsar](docs/connectors/pulsar.md), [RabbitMQ](docs/connectors/rabbitmq.md), [NATS JetStream](docs/connectors/nats.md), [MQTT](docs/connectors/mqtt.md)
- Object storage and table formats (Parquet): [S3](docs/connectors/s3-parquet.md), [GCS](docs/connectors/gcs-parquet.md), [Azure Blob](docs/connectors/azure-parquet.md), [WebHDFS / HttpFS](docs/connectors/webhdfs-parquet.md), [Iceberg](docs/connectors/iceberg.md)
- Databases and key-value: [PostgreSQL](docs/connectors/postgres.md), [MySQL / MariaDB](docs/connectors/mysql.md), [ClickHouse](docs/connectors/clickhouse.md), [Cassandra / ScyllaDB](docs/connectors/cassandra.md), [MongoDB](docs/connectors/mongodb.md), [Redis](docs/connectors/redis.md)
- Cloud services and HTTP: [AWS (Kinesis / Firehose / DynamoDB)](docs/connectors/aws.md), [HTTP (Elasticsearch, OpenSearch, Splunk, InfluxDB, Prometheus, poll, Pub/Sub)](docs/connectors/http.md)
- Serialization: [Avro](docs/connectors/avro.md)

## Internals

For a deeper look at how the engine works inside, the [internals
reference](docs/internals/README.md) documents each subsystem and names the
source files behind it. Start with [Architecture and component
stack](docs/internals/architecture.md), which links out to the rest:

- Engine core and runtime: [operator model](docs/internals/operator-model.md), [task lifecycle](docs/internals/task-lifecycle.md), [jobs and scheduling](docs/internals/jobs-and-scheduling.md)
- Distribution and data movement: [distributed runtime](docs/internals/distributed-runtime.md), [network stack](docs/internals/network-stack.md)
- Time and state: [time, watermarks and windows](docs/internals/time-and-windowing.md), [state and backends](docs/internals/state-and-backends.md)
- Reliability: [checkpointing](docs/internals/checkpointing.md), [fault tolerance and rescale](docs/internals/fault-tolerance-and-rescale.md)
- Execution paths: [columnar execution](docs/internals/columnar-execution.md), [async execution and disaggregated state](docs/internals/async-state-execution.md)
- [SQL frontend internals](docs/internals/sql-frontend.md)
- Observability: [data lineage](docs/internals/data-lineage.md)

## Using clink as a library

Once installed, link `clink::clink` (or pick targets explicitly) from any
downstream CMake project:

```cmake
cmake_minimum_required(VERSION 3.24)
project(my_pipeline LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(clink REQUIRED)

add_executable(my_pipeline src/main.cpp)
target_link_libraries(my_pipeline PRIVATE clink::clink)   # everything
# or pick à la carte:
#   clink::core         (engine only, no connectors)
#   clink::kafka        (KafkaSource / KafkaSink)
#   clink::postgres     (PostgresSource, PostgresSink, PostgresCdcSource)
#   clink::clickhouse   (ClickHouseSource, ClickHouseSink)
#   clink::s3           (S3 + Parquet-S3 source/sink)
#   clink::rocksdb      (RocksDB state backend)
#   clink::forst        (ForSt state backend, opt-in build)
#   clink::rocksdb_s3   (S3 remote state backend with a RocksDB tier)
#   clink::tls          (mTLS transport)
#   clink::etcd         (etcd HA coordinator)
#   clink::avro         (Avro codec helpers)
```

After `find_package(clink REQUIRED)` succeeds, `clink_AVAILABLE_IMPLS` is set
to the semicolon-separated list of impls that were built and installed - gate
optional features on it (e.g. `if("kafka" IN_LIST clink_AVAILABLE_IMPLS)`).

### libclink: embed from any language (pure-C ABI)

When the SQL frontend is built, `libclink.so`/`.dylib` packages the whole
engine - runtime, SQL, every built connector - behind one pure-C header
(`clink/embed/clink.h`). Open an engine (in this process, no daemons), run
SQL, and read a `connector='collect'` table's rows as typed Arrow batches
through the Arrow C stream interface - zero-copy into pyarrow, DuckDB,
polars, or Arrow C++:

The same engine also serves **Arrow Flight SQL** (`clink flight-sql`):
queries stream typed Arrow batches to any Flight SQL / ADBC / JDBC client
(DBeaver, pandas, dbt adapters), updates run DDL and bounded INSERTs, and
retracting queries carry a leading `row_kind` column - one wire protocol
into the whole SQL surface, no client library required.

```c
#include <clink/embed/clink.h>

clink_engine* e = clink_engine_open(NULL);
clink_exec(e,
    "CREATE TABLE orders (user_id BIGINT, amount BIGINT)"
    "  WITH (connector='file', format='json', path='/tmp/orders.ndjson');"
    "CREATE TABLE results (user_id BIGINT, amount BIGINT)"
    "  WITH (connector='collect');"
    "INSERT INTO results SELECT user_id, amount FROM orders");

struct ArrowArrayStream s;
clink_collect_stream(e, "results", &s);   /* blocks per get_next; ends when the job does */
/* ... drain via the Arrow C stream callbacks, or import into Arrow C++ /
   pyarrow / DuckDB ... */
s.release(&s);
clink_engine_close(e);
```

See [docs/internals/embedded.md](docs/internals/embedded.md) for the
semantics (one consumer per collect table, end-of-stream and cancellation
rules, append-only v1).

### pyclink: the same engine from Python

Pure Python over the C ABI (ctypes, no compiled extension), results as
pyarrow - pandas and polars follow for free. `pip install ./python`, point
`CLINK_LIB` at the built library, and:

```python
import pyclink

with pyclink.Engine() as e:
    e.execute("""
        CREATE TABLE orders (usr VARCHAR, amount BIGINT)
          WITH (connector='file', format='json', path='/tmp/orders.ndjson');
        CREATE TABLE totals (usr VARCHAR, total BIGINT) WITH (connector='collect');
        INSERT INTO totals SELECT usr, SUM(amount) AS total FROM orders GROUP BY usr
    """)
    table = e.collect("totals").read_all()   # pyarrow.Table, zero-copy
    e.await_all()
```

See [python/README.md](python/README.md).

A complete set of consumer-facing examples - every core feature wired up as
its own standalone executable, all built by a single `find_package`-based
`CMakeLists.txt` - lives in [`docs/consumer-examples/`](docs/consumer-examples/).
That's the recommended starting point for embedding clink in your own service.

### One-call default registration

Inside a plugin `.so` or any process building a fluent topology, replace the
hand-rolled `ensure_built_ins_registered()` +
`clink::<impl>::install(reg)` sequence (one call per linked impl) with a
single `install_defaults(env.registry())`:

```cpp
#include <clink/plugin/install_defaults.hpp>
#include <clink/api/pipeline.hpp>

clink::api::Pipeline pipeline;
clink::plugin::install_defaults(pipeline.registry());  // built-ins + every
                                                        // linked impl, in order
```

`install_defaults` is idempotent and conditional on the `CLINK_HAS_<NAME>`
defines the impl static libs propagate, so it's safe across configurations
that link different impl subsets. Internally it calls
`cluster::ensure_built_ins_registered()` followed by `kafka::install`,
`postgres::install`, `clickhouse::install`, `s3::install`, `rocksdb::install`,
and `tls::install` as available. Avro is not in this list: its codecs are
header-only with no operator factories to register, so you call them directly.

### Typed fluent helpers (Kafka, ClickHouse, Avro, S3 Parquet)

The `<impl>::install` factories register every built-in op type by `(in_type,
op_type)` pair; on the fluent API side, the matching typed helpers attach
sources/sinks to a `DataStream<T>` for any registered channel type `T`:

```cpp
// Kafka - typed message source/sink with KafkaMessageCodec framing.
auto raw = clink::kafka::message_source(env,
    clink::kafka::KafkaSourceOptions{.brokers = "kafka:9092", .topic = "in"});
clink::kafka::message_sink(raw,
    clink::kafka::KafkaSinkOptions{.brokers = "kafka:9092", .topic = "out"});

// ClickHouse - typed sink; the encoder turns each record into a row string.
clink::clickhouse::sink<MyEvent>(stream,
    clink::clickhouse::SinkOptions{.host = "ch", .port = 9000,
                                    .database = "warehouse", .table = "events"},
    [](const MyEvent& e) { return to_jsoneachrow(e); });

// S3 Parquet - typed parquet sink for any registered T, default batcher
// frames as a single `value_bytes:binary` column via the supplied Codec<T>.
clink::s3::parquet_sink<MyEvent>(stream,
    clink::s3::ParquetSinkOptions{.bucket = "raw", .key = "events.parquet"},
    my_event_codec());

// Avro - codec helpers for generated record types (no install step needed).
auto codec = clink::avro::binary_codec<MyAvroRecord>();
```

Same shape as the typed source/sink builder API: each helper mints an
inline op type via `mint_inline_op_type`, registers a per-T factory via
`PluginRegistry::register_source<T>` / `register_sink<T>`, and appends the
matching descriptor on the pipeline. No JSON wiring; no manual op_type strings.

## Code examples

The snippets below are condensed views of the matching files under
[`docs/consumer-examples/`](docs/consumer-examples/), which are the
copy-pasteable, fully-compileable versions.

### Hello pipeline (map → filter → sink)

The smallest possible clink job: a bounded source, a `MapOperator`, a
`FilterOperator`, and a sink that prints to stdout. Runs in-process via
`LocalExecutor` - no Coordinator, no Worker.

```
VectorSource<int64_t> -> Map(v * 2) -> Filter(v > 10) -> FunctionSink(print)
```

```cpp
#include <clink/operators/filter_operator.hpp>
#include <clink/operators/map_operator.hpp>
#include <clink/operators/sink_operator.hpp>
#include <clink/operators/source_operator.hpp>
#include <clink/runtime/dag.hpp>
#include <clink/runtime/local_executor.hpp>

using namespace clink;

std::vector<Record<std::int64_t>> input;
for (std::int64_t i = 1; i <= 10; ++i) {
    input.emplace_back(Record<std::int64_t>{i, EventTime{i}});
}

Dag dag;
auto s0 = dag.add_source<std::int64_t>(
    std::make_shared<VectorSource<std::int64_t>>(std::move(input)));
auto s1 = dag.add_operator<std::int64_t, std::int64_t>(
    s0, std::make_shared<MapOperator<std::int64_t, std::int64_t>>(
            [](const std::int64_t& v) { return v * 2; }));
auto s2 = dag.add_operator<std::int64_t, std::int64_t>(
    s1, std::make_shared<FilterOperator<std::int64_t>>(
            [](const std::int64_t& v) { return v > 10; }));
dag.add_sink<std::int64_t>(s2,
    std::make_shared<FunctionSink<std::int64_t>>(
        [](const std::int64_t& v) { std::cout << v << '\n'; }));

LocalExecutor(std::move(dag)).run();
```

→ Full source: [`01_hello_pipeline.cpp`](docs/consumer-examples/01_hello_pipeline.cpp)

### Event-time tumbling window

Counts events per user in 1-second tumbling windows. The window fires
when the watermark crosses each window boundary. `VectorSource` emits
`Watermark::max()` at end-of-stream so all outstanding windows flush
before the sink shuts down.

```
VectorSource<Event> -> KeyBy(user_id) -> TumblingWindow(1s, count) -> Sink
```

```cpp
struct Event { std::string user_id; };

auto src    = std::make_shared<VectorSource<Event>>(std::move(input));
auto key_by = std::make_shared<KeyByOperator<Event, std::string>>(
    [](const Event& e) { return e.user_id; });
auto window = std::make_shared<TumblingWindowOperator<std::string, Event, std::uint64_t>>(
    1000ms,
    []() -> std::uint64_t { return 0; },
    [](const std::uint64_t& acc, const Event& /*e*/) { return acc + 1; });
auto sink   = std::make_shared<FunctionSink<std::pair<std::string, std::uint64_t>>>(
    [](const auto& kv) {
        std::cout << "user=" << kv.first << " count=" << kv.second << '\n';
    });

auto s0 = dag.add_source<Event>(src);
auto s1 = dag.add_operator<Event, std::pair<std::string, Event>>(s0, key_by);
auto s2 = dag.add_operator<std::pair<std::string, Event>,
                           std::pair<std::string, std::uint64_t>>(s1, window);
dag.add_sink<std::pair<std::string, std::uint64_t>>(s2, sink);
```

→ Full source: [`02_event_time_tumbling.cpp`](docs/consumer-examples/02_event_time_tumbling.cpp).
Also see [`03_sliding_window_aggregate.cpp`](docs/consumer-examples/03_sliding_window_aggregate.cpp)
for `SlidingWindowOperator` with `size` + `slide`.

### Keyed state in a process function

A `KeyedProcessFunction` maintains per-key state through
`RuntimeContext::keyed_state<K, V>(...)`. The same code works against
`InMemoryStateBackend` (shown), the file-backed backend, or
`clink::rocksdb` for durability - just swap the backend instance on
`JobConfig`.

```cpp
class RunningTotal final
    : public KeyedProcessFunction<std::int64_t, std::int64_t, std::int64_t> {
public:
    void open(RuntimeContext& ctx) override {
        state_ = std::make_unique<KeyedState<std::int64_t, std::int64_t>>(
            ctx.keyed_state<std::int64_t, std::int64_t>(
                "running_total", int64_codec(), int64_codec()));
    }
    void process_element(const std::int64_t& v,
                         ProcessFunctionContext<std::int64_t>& /*ctx*/,
                         Collector<std::int64_t>& out) override {
        const auto next = state_->get(current_key()).value_or(0) + v;
        state_->put(current_key(), next);
        out.collect(next);
    }
private:
    std::unique_ptr<KeyedState<std::int64_t, std::int64_t>> state_;
};

JobConfig cfg;
cfg.state_backend = std::make_shared<InMemoryStateBackend>();
// ... build dag with detail::KeyedProcessFunctionAdapter wrapping RunningTotal ...
LocalExecutor(std::move(dag), std::move(cfg)).run();
```

→ Full source: [`04_keyed_process_state.cpp`](docs/consumer-examples/04_keyed_process_state.cpp)

### Interval join across two streams

Joins clicks against orders for the same user when
`delta = order_time - click_time` falls in `[-lower, +upper]`. Default
join type is `Inner`; pass `Dag::JoinType::LeftOuter` (and friends) to
emit unmatched left rows after the watermark advances past their
upper-bound deadline.

```
VectorSource<Click>  ─┐
                      ├── interval_join(key=user_id, [-50ms, +200ms]) -> Sink<Joined>
VectorSource<Order>  ─┘
```

```cpp
auto h_clicks = dag.add_source<Click>(
    std::make_shared<VectorSource<Click>>(std::move(clicks), "clicks"));
auto h_orders = dag.add_source<Order>(
    std::make_shared<VectorSource<Order>>(std::move(orders), "orders"));

auto h_joined = dag.interval_join<Click, Order, std::string, Joined>(
    h_clicks, h_orders,
    [](const Click& c) { return c.user_id; },
    [](const Order& o) { return o.user_id; },
    50ms,    // look-back: order may precede click by up to 50ms
    200ms,   // look-ahead: order may follow click by up to 200ms
    [](const std::optional<Click>& c, const std::optional<Order>& o) {
        return Joined{c->user_id, c->url, o->sku};
    });
dag.add_sink<Joined>(h_joined, sink);
```

→ Full source: [`05_interval_join.cpp`](docs/consumer-examples/05_interval_join.cpp)

### Cluster job plugin (`Pipeline`)

The fluent API. The build function is packaged into a `.so`
via `CLINK_REGISTER_JOB(...)` and submitted to a running cluster:

```bash
clink_node --role=coordinator --rpc-port=6123 &
clink_node --role=worker --coordinator-host=127.0.0.1 --coordinator-port=6123 &
clink run --coordinator 127.0.0.1:6123 ./my_job.so
```

```cpp
#include <clink/api/builtin_connectors.hpp>
#include <clink/api/pipeline.hpp>
#include <clink/job/register_job.hpp>

void define_job(clink::api::Pipeline& pipeline) {
    using namespace std::chrono_literals;

    pipeline.from_elements<std::int64_t>({1, 2, 3, 4, 5})
        .map<std::int64_t>([](const std::int64_t& v) { return v * 10; })
        .filter([](const std::int64_t& v) { return v > 20; })
        .assign_timestamps_monotonic(
            [](const std::int64_t& v) { return clink::EventTime{v}; })
        .key_by([](const std::int64_t& v) { return (v / 10) % 2; })
        .sliding_window(60ms, 60ms)
        .aggregate<std::int64_t>(
            []() -> std::int64_t { return 0; },
            [](const std::int64_t& acc, const std::int64_t& v) { return acc + v; })
        .sink(clink::api::FileInt64Sink::builder().path("/tmp/out.txt").build());
}

CLINK_REGISTER_JOB("my-job", "1.0", "demo pipeline", define_job);
```

→ Full source: [`08_cluster_job_plugin.cpp`](docs/consumer-examples/08_cluster_job_plugin.cpp).
Build the plugin module from your downstream CMake project using
`add_library(my_job MODULE ...)`; see the `CLINK_BUILD_PLUGIN_EXAMPLE`
branch of [`docs/consumer-examples/CMakeLists.txt`](docs/consumer-examples/CMakeLists.txt).

## SQL frontend

Built when `CLINK_BUILD_SQL=ON` (default off; flip it on to opt in). Compiles a
PostgreSQL-shaped subset of SQL to a `JobGraphSpec` and either runs it embedded
(the whole engine in one process, no daemons) or submits it to a running
cluster through the same HTTP path as a compiled job plugin.

Embedded execution is the fastest way to run a pipeline:

```bash
# One process, no cluster: a bare SELECT prints its rows to stdout.
clink run -e "CREATE TABLE orders (usr VARCHAR, amount BIGINT) \
                WITH (connector='file', format='json', path='/tmp/orders.ndjson'); \
              SELECT usr, SUM(amount) AS total FROM orders GROUP BY usr"

# The same script file, unchanged, on a real cluster: add one flag.
clink run pipeline.sql
clink run pipeline.sql --coordinator-host=coordinator.prod --coordinator-port=8081
```

First result row lands on stdout in ~155 ms from process start (median of 20
runs, Release build, 12-core Apple Silicon, file-source query; engine bring-up
dominates, the operator chain barely registers). A `Release`-only ctest,
`embedded_first_row_budget`, gates that number against a budget so it cannot
silently regress; `benchmarks/embedded_footprint.py` reproduces it and the
memory figures. Ctrl-C stops an unbounded pipeline and drains it cleanly;
`--checkpoint-dir` / `--state-backend` / `--parallelism` behave as they do on a
cluster. See
[docs/internals/embedded.md](docs/internals/embedded.md).

Pipeline: `libpg_query` parses the input; an AST builder normalises
the parse tree into a parser-agnostic AST under `clink::sql::ast`;
the binder type-checks against a `Catalog`, lowers expressions to
JSON predicates / value-expressions, and emits `LogicalPlan` nodes;
the optimizer runs predicate pushdown, cost-based join reordering
(driven by `ANALYZE TABLE` statistics, DP-optimal order selection,
never applied unless estimated cheaper), and projection pushdown into
the source's column hint; the physical planner lowers the logical tree
to a `JobGraphSpec` of registered operators. All SQL ops live in `clink::sql::install(reg)`
which `clink_node` calls at startup when the SQL frontend is linked.

Supported today:

DDL and basics

- `CREATE TABLE t (col TYPE, ...) WITH (connector=..., format='json', path=...,
  bootstrap=..., topic=..., event_time_column=..., watermark_lag_ms=...)`
- `INSERT INTO sink SELECT ... FROM source`
- `SHOW TABLES`, `DROP TABLE [IF EXISTS]`; the catalog is session-scoped by
  default and persists as one JSON file per table when a catalog directory
  is set (`--sql-catalog-dir` on `clink_node`, `--catalog-dir` on `clink run`)
- `EXPLAIN <stmt>` prints the optimized `LogicalPlan` tree (the plan that
  would run, join reorders and pushdowns applied), each node annotated with
  its estimated output rows; a scan with no declared statistics is flagged
- `ANALYZE TABLE t` scans a bounded table and writes exact statistics (row
  count, per-column NDV, histograms, most-common values) into the catalog,
  grounding the estimates the cost-based optimizer compares

Projection and filtering

- `WHERE` with `AND` / `OR` / `NOT`, `IS [NOT] NULL`, `[NOT] LIKE`,
  `[NOT] BETWEEN`, `[NOT] IN (...)`, three-valued null semantics
- Expression `SELECT`: arithmetic, concat (`||` / `CONCAT`), `CASE WHEN`,
  string functions (`UPPER` / `LOWER` / `LENGTH` / `SUBSTRING` / `TRIM` /
  `REPLACE` / `POSITION`), numeric functions (`ABS` / `ROUND` / `CEIL` /
  `FLOOR` / `MOD`), `COALESCE` / `NULLIF` / `GREATEST` / `LEAST`,
  `CAST(... AS BIGINT|DOUBLE|TEXT)`
- `SELECT DISTINCT` (dedupe across the run)
- `ORDER BY` with `LIMIT n` / `OFFSET m` (sorted, emitted at end-of-stream)

Aggregation and windows

- `GROUP BY` over `TUMBLE`, `HOP`, `SESSION`, `CUMULATE`, or plain columns
  (unbounded upsert-style aggregate); `SUM` / `COUNT` / `MIN` / `MAX` / `AVG`,
  the variance family (`STDDEV` / `VARIANCE` / `_POP` / `_SAMP`),
  `PERCENTILE` / `APPROX_PERCENTILE`, `STRING_AGG` / `LISTAGG`, `ARRAY_AGG`,
  `COLLECT`; `COUNT(DISTINCT ...)` and friends with retraction-exact
  multiplicity tracking
- `HAVING` referencing aggregate aliases or group columns
- `OVER` aggregates (running `SUM`/`COUNT`/`AVG`/`MIN`/`MAX`) with bounded
  `ROWS` / `RANGE <n> PRECEDING ... CURRENT ROW` frames (bounded frames are for
  aggregates only); navigation functions `FIRST_VALUE` / `LAST_VALUE` / `LAG`
  on the running frame
- `ROW_NUMBER()` / `RANK()` / `DENSE_RANK()` as bounded Top-N-per-partition

Joins

- Stream-stream equi joins `JOIN ... ON a.k = b.k` in `INNER`, `LEFT`,
  `RIGHT`, and `FULL OUTER` variants, with first-class `WHERE` / projection /
  `GROUP BY` / `ORDER BY` over the join output; multi-way `INNER` join trees
  are reordered cost-based when table statistics say a cheaper order exists
- Join columns are referencable as the flat `<alias>_<col>` output names or
  by the natural qualified `alias.col` spelling
- Stream-stream interval joins `JOIN ... ON a.k = b.k AND a.ts BETWEEN b.ts +
  low AND b.ts + high`, in `INNER`, `LEFT`, `RIGHT`, and `FULL OUTER` variants
  (unmatched rows null-padded at watermark eviction, append-only)
- Lookup / temporal-enrichment joins: a `connector='lookup'` table whose
  `function=` names a registered async `Row -> Task<Row>` coroutine, joined via
  `JOIN` (INNER) or `LEFT JOIN` (processing-time, right side only)

Complex types and exact numerics

- `ARRAY` types and literals, element access `a[i]`, `array_agg([DISTINCT] x)`
  (with retraction and SESSION-window merge)
- `ROW(...)` and `MAP(...)` construction, field access `(r).f`, element access
  `m['k']`; roundtrip as nested JSON. Whole ROW/MAP values flow to matching
  typed sink columns
- `MULTISET<t>` columns built by the `COLLECT(x)` aggregate (bag semantics:
  multiplicity preserved, retraction-correct), accessed with `CARDINALITY(x)`
  and `ELEMENT(x)` (sole element of a one-element multiset)
- Set operations `UNION` / `INTERSECT` / `EXCEPT` (and `ALL` multiset forms,
  per-key count model for retraction correctness)
- Exact `DECIMAL(p, s)` 128-bit arithmetic (`+ - * / mod`, comparison, `CAST`,
  `SUM`/`MIN`/`MAX`), HALF_UP rounding, overflow to NULL. Caveat: source values
  beyond ~15-17 significant digits are limited by JSON double precision; mixed
  decimal/double demotes to double

Subqueries

- Scalar subqueries in the `SELECT` list (uncorrelated single-aggregate) and in
  `WHERE` comparisons
- `IN` / `NOT IN` (single- and multi-column, NULL-aware anti), `EXISTS` /
  `NOT EXISTS`
- Common table expressions `WITH cte AS (...)` (nested bodies; each CTE
  referenced at most once per query)

Advanced and extensibility (each a documented v1 subset)

- `MATCH_RECOGNIZE (PARTITION BY ... ORDER BY ... PATTERN ... DEFINE ...
  MEASURES ...)`: linear greedy patterns, `PREV(col)` in DEFINE (compare
  against the previous matched row), FIRST/LAST/CLASSIFIER measures, `ONE ROW
  PER MATCH` or `ALL ROWS PER MATCH` (every matched row with its pattern
  variable), `AFTER MATCH SKIP PAST LAST ROW`; lowered onto the CEP engine
- `CREATE MATERIALIZED VIEW ... AS <SELECT>`: continuous maintenance (keyed GROUP
  BY auto-derives upsert mode and primary key), or a scheduled full-refresh arm via
  `WITH ('freshness'='1h')` that recomputes over bounded sources and atomically
  overwrites the backing, driven by a Coordinator-side scheduler and `REFRESH
  MATERIALIZED VIEW`; `partition_by` writes a per-partition set swapped atomically
- SQL-native AI: `CREATE MODEL name INPUT (...) OUTPUT (...) WITH (provider=...)`
  registers a model, and `ML_PREDICT(TABLE t, MODEL m, DESCRIPTOR(cols))` applies
  it per row (a built-in HTTP inference provider that runs async with many inferences
  in flight, or batched - many rows per request - when the model sets max_batch_size;
  a local ONNX Runtime provider; and a C++-closure provider SPI; model declarations
  persist in the catalog and survive a restart);
  `VECTOR_SEARCH(TABLE t, query_col, vec_table,
  DESCRIPTOR(idx), top_k [, metric])` returns each row's top-K nearest vectors + a
  score, with SIMD distance kernels (SimSIMD) and exact-flat or approximate-HNSW
  (usearch) indexing
- Process table functions: a C++-registered `KeyedProcessFunction<string,Row,
  Row>` callable as `fn(TABLE t PARTITION BY cols)` with isolated per-key state
  (v1 is timerless)
- Scalar UDFs and aggregate UDFs (UDAFs): C++-registered closures invoked by
  name. A UDAF takes 0..N column arguments (rows with any NULL argument are
  skipped) and accepts `DISTINCT` (dedup on the argument tuple, retraction
  kept exact by multiplicity tracking); it needs `retract` for changelog
  GROUP BY and `merge` for SESSION windows
- `CREATE FUNCTION with_tax(amount BIGINT) RETURNS BIGINT AS 'amount + amount
  / 10' LANGUAGE SQL`: expression-bodied scalar UDFs written in SQL itself, no
  module or extra build flag. The body is any SELECT expression over the named
  parameters; it is lowered and compiled once at CREATE and ships to the
  cluster as text
- `CREATE FUNCTION f(x BIGINT) RETURNS BIGINT LANGUAGE wasm AS
  '/path/module.wasm'`: scalar UDFs declared in SQL itself, run in-process in
  a WebAssembly sandbox (wasmtime, opt-in `CLINK_WITH_WASM`). Modules must be
  self-contained (imports rejected), every call runs under a fuel budget so
  runaway code fails instead of hanging, NULL in is NULL out, and the export's
  signature is validated against the declared SQL types at load. Value model:
  BIGINT/INTEGER/DOUBLE/REAL, plus TEXT via guest memory (argument = ptr+len
  pair, result = packed i64; the module exports `memory` and `alloc`, with an
  optional `dealloc` the host calls after copy-out; all guest pointers are
  bounds-checked). Works embedded (`clink run`, libclink) and on a cluster: a
  submitted job ships the module bytes in its spec and every Worker
  registers the function at deploy, no shared filesystem needed. Declarations
  persist in the catalog (payload included, so a restart never needs the
  original module path) and `DROP FUNCTION [IF EXISTS]` removes them; calls
  run against a pool of instances, so parallel plans do not serialise.
  Aggregates too: `CREATE AGGREGATE f(BIGINT) (language='wasm',
  module='/path.wasm', result_type='BIGINT')` maps `_init` / `_accumulate` /
  `_result` (+ optional `_retract` / `_merge`) exports onto the UDAF
  machinery, with the accumulator as opaque guest bytes that checkpoint like
  built-in aggregate state - usable in plain and windowed GROUP BY,
  changelog retraction, and SESSION merge
- Programmatic Table API (v1): a fluent C++ builder, `from(...)` then optional
  `filter(...)` then either `select(...)` or `group_by(...).agg(...)` then
  `insert_into(...)`, sharing the binder's lowering so it produces a
  `JobGraphSpec` identical to the equivalent SQL

Submit via the CLI:

```bash
clink_node --role=coordinator --rpc-port=6123 &
clink_node --role=worker --coordinator-host=127.0.0.1 --coordinator-port=6123 &

clink_submit_sql --coordinator 127.0.0.1:6123 <<'SQL'
CREATE TABLE clicks (user_id BIGINT, ts BIGINT, url TEXT)
    WITH (connector='file', format='json', path='/tmp/clicks.ndjson',
          event_time_column='ts');
CREATE TABLE per_user (user_id BIGINT, total BIGINT)
    WITH (connector='file', format='json', path='/tmp/per_user.ndjson');

INSERT INTO per_user
SELECT user_id, COUNT(*) AS total
FROM clicks
GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE), user_id
HAVING total > 0;
SQL
```

Or compile and submit programmatically:

```cpp
#include <clink/sql/binder.hpp>
#include <clink/sql/catalog.hpp>
#include <clink/sql/parser.hpp>
#include <clink/sql/physical_plan.hpp>

clink::sql::Catalog cat;
// ... register tables via cat.register_table(...) ...

clink::sql::Binder b(cat);
auto plan = b.bind_insert(std::get<clink::sql::ast::InsertStmt>(
    clink::sql::parse("INSERT INTO out SELECT ...").statements[0]));
auto spec = clink::sql::PhysicalPlanner{}.compile(
    static_cast<const clink::sql::LogicalSink&>(*plan));

clink::application::JobSubmitter submitter("127.0.0.1", coordinator_port);
submitter.submit(spec.to_json(), /*plugins=*/{}, opts);
```

End-to-end coverage lives in `tests/test_sql_runtime.cpp`: compiles
SQL, submits to an in-process coordinator + worker pair, and asserts the sink file
content across the supported feature surface.

## Time-travel debugging

When a running total looks wrong, you should not have to re-run the job
with print statements. clink records enough at runtime to answer "why is
this value what it is" offline: checkpoints double as queryable state
snapshots, and an opt-in flight recorder (`--capture-dir`) tees the
records each operator consumed into per-checkpoint-epoch files. Three CLI
verbs close the loop: `state-diff` finds which key moved and when,
`capture-cat` shows what the operator saw, and `replay` re-executes the
operator over exactly those records, deterministically and offline, with no
cluster.

In the walk-through below, a 1.4M-order pipeline computes per-user
totals, and alice's comes out at roughly five times everyone else's:

```bash
$ clink run orders.sql --checkpoint-dir=ckpt --checkpoint-interval-ms=400 \
    --capture-dir=capture --capture-records=2000000
$ cat totals.ndjson
{"total":12332536,"usr":"alice"}
{"total":2332647,"usr":"bob"}
{"total":2334163,"usr":"carol"}
```

**1. What did the flight recorder capture?** One `.cap` file per operator
per checkpoint epoch; epoch N holds exactly the records between
checkpoints N-1 and N:

```bash
$ clink capture-cat --dir=capture
  op-5824225372086884863/subtask-1/epoch-1.cap  seen=121344
  op-5824225372086884863/subtask-1/epoch-2.cap  seen=99840
  ...
  op-5824225372086884863/subtask-1/epoch-9.cap  seen=100096
  ...
```

**2. Which checkpoint window did alice move in?** Diff consecutive
checkpoints. Keys render readably; values are the operator's raw
accumulator bytes, and between checkpoints 8 and 9 only alice's bucket
changes magnitude (the little-endian f64 inside reads 1,374,339 before
and 11,540,851 after - a 10.17M jump against ~168k of normal flow):

```bash
$ clink state-diff --dir=ckpt --from=8 --to=9
  ~ kg=31 key ""alice""  0x...83f8344100... [108 bytes] -> 0x...2e03664100... [109 bytes]
  ...
```

**3. What did the aggregate consume in epoch 9?** The culprit is one
grep away, sitting at record 24,915 of the epoch:

```bash
$ clink capture-cat --file=capture/op-5824225372086884863/subtask-1/epoch-9.cap \
    --max-rows=0 | grep 9999999
  "{"__key":-5626395697980741632,"amount":9999999,"usr":"alice"}"
```

**4. Confirm it.** Replay the epoch: rebuild the operator from its capture
sidecar, restore its keyed state from checkpoint 8, feed exactly the
captured records, and print every emission. The jump lands at precisely
that record - and running it twice produces 100,102 byte-identical
emissions:

```bash
$ clink replay --capture-dir=capture --checkpoint-dir=ckpt \
    --op=5824225372086884863 --subtask=1 --epoch=9 --max-rows=0 | sed -n '24916,24917p'
{"total":1416073,"usr":"alice"}
{"total":11416072,"usr":"alice"}
```

Capture is best-effort and bounded (`--capture-records` per epoch; the
file header records truncation, so a replay can tell a complete epoch
from a sampled one). The captured epoch is a full event stream - data
records, watermarks, and timer-fire clock positions in observed order -
so windowed and timer-driven operators replay their fires at the exact
production positions, not just the per-record path (captures from older
builds replay data-only, and the tool says so). Omit `--op` to replay
every captured operator of the job in one command, and add `--verify`
to replay each epoch twice and byte-compare the emissions - the
determinism gate
([docs/internals/replay-determinism.md](docs/internals/replay-determinism.md)):

```bash
$ clink replay --capture-dir=capture --checkpoint-dir=ckpt --epoch=9 --verify
replay: 3 captured operator(s), epoch 9, verifying determinism (2 runs each)
  op 5824225372086884863 (aggregate_row) subtask 1: 100096 records, ... [deterministic]
  ...
deterministic: every replayed operator byte-identical across 2 runs
```

And the cross-version question - "what would the fix have produced on
last night's bytes?" - is one diff: replay the epoch through two builds
(`--plugin=<candidate.so>` for compiled jobs) with `--out=<file>`, then
`clink replay-diff a.ndjson b.ndjson` reports the first divergence, or
`identical`. `--emit-test=<dir>` goes one further and freezes the epoch
into a self-contained regression bundle (capture + state + golden
emissions + a generated gtest source): the incident becomes a permanent,
byte-exact test in one command. `clink capture-push` / `capture-fetch`
ship capture trees to object storage beside the checkpoints and pull
them back (optionally one epoch) onto any machine - so "reproduce the
3am incident locally, in a debugger" starts with a fetch.

Details:
[docs/internals/fault-tolerance-and-rescale.md](docs/internals/fault-tolerance-and-rescale.md).

## State as data

A pipeline's state is an open dataset. Snapshots are
plain Apache Arrow IPC streams (a documented, stable format - see
`docs/internals/state-snapshot-format.md`), so a checkpoint is readable
by pyarrow, DuckDB or Polars directly, and the CLI closes the loop for
every backend, RocksDB's native checkpoints included:

```bash
# Any checkpoint, savepoint or RocksDB checkpoint dir -> an open file.
$ clink state-export --from=ckpt/0/checkpoint-9.snap --out=state.parquet
state-export: ckpt/0/checkpoint-9.snap -> state.parquet (parquet, ...)

# Or straight into a catalogued Apache Iceberg table (one snapshot per
# export, so the lake table time-travels across them).
$ clink state-export --from=ckpt/0/checkpoint-9.snap --format=iceberg \
    --warehouse=/data/warehouse --table=job_state

# Or query it in place - the engine's own SQL over the snapshot,
# results as NDJSON. GROUP BY and DISTINCT net to their final rows.
$ clink state-query --from=ckpt/0/checkpoint-9.snap \
    --sql="SELECT slot, count(*) AS keys FROM state GROUP BY slot"
{"keys":184,"slot":"counts"}
```

Multi-subtask checkpoints merge with `--dir=<root> --id=N`. Schema
version stamps ride the snapshots (all backends), so
`clink check-savepoint` can gate a deploy against a savepoint's actual
state versions before anything restores.

## Testing your pipelines

`clink::test` (link the test-only `clink::test_support` CMake target) is
the supported public API for unit-testing everything you build on clink -
deterministically, in-process, no threads, no wall clock, milliseconds
per test. The harnesses do not mock the engine: they compose the same
production pieces an operator runner would (a real `RuntimeContext`, the
operator's own `TimerService` on a manual clock, the engine's `Emitter`)
and deliver elements exactly as the runner delivers them.

```cpp
#include "clink/test/keyed_harness.hpp"

auto h = clink::test::make_keyed_process_function_harness(
    CountPerUser{}, [](const Purchase& p) { return p.user; });
h.open();
h.process_element(Purchase{"alice", 10}, /*event_time_ms=*/1000);
h.process_watermark(2500);                                    // fires due timers
EXPECT_EQ(h.state_value<std::int64_t>("alice", "count"), 1);  // production read path
```

One-input, keyed, and two-input harnesses (the latter with the engine's
real watermark combination and idleness); typed state inspection and
seeding; side-output capture; snapshot/restore round trips through the
production checkpoint cycle; deterministic failure injection; scripted
`TestSource`/`CollectSink`/`TransactionalTestSink` endpoints;
`LocalTestEnvironment` for whole pipelines on the real local runtime;
an in-process `TestCluster` (real Coordinator + Workers); assertion
helpers and platform-stable property-testing shuffles. The framework's
own suite dogfoods it against clink's production window operator. Full
guide: [`docs/internals/testing-framework.md`](docs/internals/testing-framework.md).

## Running the in-tree examples

The examples bundled with the source tree are built into `build/examples/`:

```bash
./build/examples/clink_word_count
./build/examples/clink_event_time_tumbling
./build/examples/clink_file_word_count
./build/examples/clink_clickstream_join
```

For consumer-style examples that you can copy/paste into your own project,
see [`docs/consumer-examples/`](docs/consumer-examples/).

## Linting & formatting

Three CMake targets, declared in [`cmake/ClangTools.cmake`](cmake/ClangTools.cmake):

```bash
cmake --build build --target format        # format every source file in place
cmake --build build --target format-check  # CI-friendly: dry-run, exits non-zero on violations
cmake --build build --target tidy          # run clang-tidy across the tree
```

Targets glob `src/`, `include/clink/`, `tests/`, `tools/`, and `examples/`,
so newly-added files are picked up at the next configure. `compile_commands.json`
is always emitted (`CMAKE_EXPORT_COMPILE_COMMANDS=ON`) so `clang-tidy` and
`clangd` can resolve per-TU flags without extra plumbing. The targets degrade
gracefully - they print a status line if `clang-format` / `clang-tidy` aren't
installed instead of failing the configure.

A pre-commit git hook is installed automatically at configure time
(see [`cmake/GitHooks.cmake`](cmake/GitHooks.cmake)). It invokes the
`format-check` target so commits fail on unformatted code; if you don't have
a `cmake-build-debug/` build dir, the hook skips silently.

The format and tidy rulesets live in `.clang-format` and `.clang-tidy` at
the repo root.

## Repository layout

```
include/clink/
    api/             # public API facade (Pipeline, descriptors)
    application/     # application-mode lifecycle and configuration
    async/           # async utilities and continuations (Task<T>)
    cep/             # complex event processing (pattern DSL + operator)
    checkpoint/      # barriers + coordinator + 2PC sinks
    cluster/         # Coordinator, Worker, HA, autoscaler, rescale
    config/          # configuration types (incl. exact decimal)
    connectors/      # built-in sources/sinks (file, parquet, text, 2PC)
    core/            # types, records, batches, stream elements, Arrow batcher
    http/            # HTTP server + JSON API + dashboard assets
    job/             # job registration and lifecycle (CLINK_REGISTER_JOB)
    metrics/         # registry, counter, gauge, otel boundary
    operators/       # source/map/filter/key_by/window/reduce/sink, UDF registries
    plugin/          # plugin registry + install_defaults
    queryable_state/ # external state query registry + routing
    runtime/         # bounded channel, dag, executor, key groups, network
    sql/             # AST, catalog, binder, optimizer, planner, Table API
    state/           # state backend interface + in-memory/file/rocksdb/changelog
    state_processor/ # offline savepoint read + transform API
    test/            # public testing framework (harnesses, test sources/sinks)
    time/            # event time, watermark, timers
src/                 # out-of-line impls: application, async, checkpoint, cluster,
                     #   config, http, metrics, runtime, sql, state
                     #   (template-heavy generic code stays header-only)
impls/               # optional connectors/backends: kafka, postgres, clickhouse,
                     #   s3, rocksdb, rocksdb-s3, avro, etcd, tls
tests/               # GoogleTest suites (unit + integration)
examples/            # in-tree example jobs
docs/                # connector + internals references, consumer-examples/
benchmarks/          # clink_bench, clink_serde_bench, failover_coldstart,
                     #   inproc_compare, prod_compare, flink_compare
tools/               # clink (unified CLI), clink_node, clink_submit_sql, and
                     #   the standalone client binaries
```

## Reading order (for new contributors)

1. `docs/internals/architecture.md` - the conceptual model
2. `include/clink/core/stream_element.hpp` - the wire format between operators
3. `include/clink/runtime/dag.hpp` - how the DAG is built and run
4. `tests/test_map_filter.cpp` - the smallest end-to-end integration

## Versioning and stability

The current release is v0.1.0; changes land in the
[CHANGELOG](CHANGELOG.md). clink is pre-1.0, so public C++ APIs may still
change between minor releases - every such change is called out. Durable
state is treated more conservatively: snapshots carry schema versions with a
migrate-at-restore path (see
[state snapshot format](docs/internals/state-snapshot-format.md) and
[fault tolerance and rescale](docs/internals/fault-tolerance-and-rescale.md)),
so an upgrade does not silently invalidate checkpoints or savepoints.

## Getting help

- Questions, bug reports, feature requests:
  [GitHub issues](https://github.com/orhaugh/clink/issues).
- Contributing a change: [CONTRIBUTING.md](CONTRIBUTING.md).
- Reporting a security issue privately: [SECURITY.md](SECURITY.md).

## License and attribution

clink is licensed under the Apache License 2.0. See [`LICENSE`](LICENSE) and
[`NOTICE`](NOTICE).

You are free to use clink for any purpose, including in commercial and
closed-source products, with no obligation beyond the licence. If you build on
it, an attribution or a link back to this repository is genuinely appreciated.
For academic or technical write-ups, please cite it using
[`CITATION.cff`](CITATION.cff) - GitHub's "Cite this repository" button turns
that file into a ready-made reference.
