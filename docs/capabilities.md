# Capability catalogue

The complete shipped feature surface, one area per section, each row linking
to the page that documents it in depth. A capability appears here only when it
is backed by code and tests in the repository; config-gated or partial
features carry their caveat in the row. The
[README status tables](https://github.com/orhaugh/clink#status) remain the
fine-grained per-subsystem inventory; this page is the navigable map.

## Execution model

| Capability | Notes | Reference |
| --- | --- | --- |
| Typed operator DAGs | Map, filter, reduce, keyed process, branching (`fork`), union with barrier alignment | [Operator model](internals/operator-model.md) |
| Fluent API | `Pipeline` / `DataStream<T>` / `KeyedDataStream<T>` builder chain | [Operator model](internals/operator-model.md) |
| Operator parallelism | Per-subtask keyed-state isolation, hash-partitioned shuffle | [Jobs and scheduling](internals/jobs-and-scheduling.md) |
| Backpressure | Bounded channels end to end, in-band flow control | [Task lifecycle](internals/task-lifecycle.md) |
| Columnar execution | Operators opt in via `process_columnar()`; Arrow sidecar batches flow through shuffles without row materialisation | [Columnar execution](internals/columnar-execution.md) |
| Async state execution | Off-thread state reads with deadline-based resume and read coalescing for deferring backends | [Async state execution](internals/async-state-execution.md) |

## Time, windows, CEP

| Capability | Notes | Reference |
| --- | --- | --- |
| Event time and watermarks | Assigner strategies: monotonic, bounded out-of-orderness | [Time and windowing](internals/time-and-windowing.md) |
| Windows | Tumbling, sliding, session; custom triggers (tumbling/sliding), `allowed_lateness`, late output tags. No evictors | [Time and windowing](internals/time-and-windowing.md) |
| Interval join | Keyed stream-stream join, all 8 join types, watermark-driven eviction, late-arrival policy | [Time and windowing](internals/time-and-windowing.md) |
| Complex event processing | NFA-based `Pattern` DSL: linear patterns, greedy quantifiers, strict/relaxed contiguity; reachable from SQL `MATCH_RECOGNIZE` | [Time and windowing](internals/time-and-windowing.md) |

## SQL

Built behind `CLINK_BUILD_SQL=ON` (default off). One SQL file runs embedded or
submits to a cluster, unchanged.

| Capability | Notes | Reference |
| --- | --- | --- |
| DDL and catalog | `CREATE TABLE ... WITH (connector=...)`, session or directory-persisted catalog, `SHOW`/`DROP`, `EXPLAIN` with row estimates, `ANALYZE TABLE` statistics | [SQL frontend](internals/sql-frontend.md) |
| Queries | Projection and filtering with three-valued null semantics, expressions, aggregates, `GROUP BY` (including windowed), `HAVING`, `ORDER BY` / `LIMIT`, subqueries, `DISTINCT`, top-N | [SQL frontend](internals/sql-frontend.md) |
| Joins | Stream-stream interval joins, multi-way INNER joins with cost-based reordering (applied only when estimated cheaper), lookup joins | [SQL frontend](internals/sql-frontend.md) |
| Types | `BIGINT`/`DOUBLE`/`VARCHAR`/`BOOLEAN`/timestamps, `DECIMAL` (exact, 128-bit), `ARRAY`/`MAP`/`ROW`, `MULTISET` | [SQL frontend](internals/sql-frontend.md) |
| Pattern matching | `MATCH_RECOGNIZE` v1 on the CEP engine | [SQL frontend](internals/sql-frontend.md) |
| Extensibility | Scalar UDFs (`LANGUAGE SQL` and native), UDAFs, the Table API producing the same `JobGraphSpec` | [SQL frontend](internals/sql-frontend.md) |
| SQL-native ML | `CREATE MODEL` / `ML_PREDICT` (HTTP, ONNX opt-in, native closures), `VECTOR_SEARCH`, full-refresh materialized tables | [SQL frontend](internals/sql-frontend.md) |

## State

| Capability | Notes | Reference |
| --- | --- | --- |
| Keyed and broadcast state | `keyed_state<K,V>` / `broadcast_state<V>` on the operator `RuntimeContext` | [State and backends](internals/state-and-backends.md) |
| Backends | In-memory, file-backed, changelog (WAL + materialisation), RocksDB (always built), ForSt (opt-in) including object-store-resident variants | [State and backends](internals/state-and-backends.md) |
| Open snapshot format | Snapshots are documented Arrow IPC; checkpoints open directly in pyarrow, DuckDB, Polars; Parquet and Iceberg export | [State snapshot format](internals/state-snapshot-format.md) |
| Queryable state | Live keyed state served over HTTP: JSON point lookups, whole-slot scans, Arrow IPC streams; one job can `SELECT` from another job's live state | [State and backends](internals/state-and-backends.md) |
| Savepoints and state processor | Offline savepoint read and transform API | [Fault tolerance and rescale](internals/fault-tolerance-and-rescale.md) |
| Schema evolution | Migrate-at-restore with a migration registry and a pre-deploy compatibility gate | [Fault tolerance and rescale](internals/fault-tolerance-and-rescale.md) |

## Delivery guarantees

| Capability | Notes | Reference |
| --- | --- | --- |
| Checkpointing | Chandy-Lamport barrier alignment; unaligned checkpoints at multi-input operators; async snapshot workers; fsync-gated acks | [Checkpointing](internals/checkpointing.md) |
| Exactly-once sinks | Generic committer (prepare at barrier, commit on global durability, recover-and-re-commit): file, Kafka, Parquet, raw S3 multipart, Postgres `PREPARE TRANSACTION` | [Sink committer framework](internals/sink-committer-framework.md) |
| Effectively-once upserts | Changelog upsert and delete by `PRIMARY KEY`: Postgres, MySQL, Cassandra, Redis | [Sink committer framework](internals/sink-committer-framework.md) |
| Source replay | Source-offset recovery generalised across connectors | [Checkpointing](internals/checkpointing.md) |

## Scale and operations

| Capability | Notes | Reference |
| --- | --- | --- |
| Cluster runtime | Coordinator/Worker control plane over a length-prefixed TCP protocol; jobs deploy as compiled plugins or SQL | [Distributed runtime](internals/distributed-runtime.md) |
| Failover | Lost-worker detection, drain, redeploy from the latest completed checkpoint (fail-fast by default, config-gated restarts) | [Fault tolerance and rescale](internals/fault-tolerance-and-rescale.md) |
| Rescale | Change one operator's parallelism on a running job (`clink rescale-op`, HTTP, or the autoscaler). An operator whose edges are all keyed or parallelism-mismatched cuts over hot at one checkpoint barrier: sources do not rewind, unaffected operators keep running, key-group state repartitions onto the new subtasks. Anything ineligible, and any failed cutover, falls back to drain, replan and redeploy from the last completed checkpoint. Integer factors, declared bounds required. Whole-job rescale by role is refused for multi-operator jobs | [Fault tolerance and rescale](internals/fault-tolerance-and-rescale.md) |
| High availability | Multi-coordinator leader election via etcd (opt-in); filesystem-backed job persistence; fencing epoch on every control frame so a superseded coordinator cannot deploy, cancel or commit | [Distributed runtime](internals/distributed-runtime.md) |
| Configuration checking | `clink lint` reports settings that would be accepted and then ignored, or that contradict each other, without contacting a cluster; exits non-zero on anything a submission would refuse, and shares its parsing with `clink run` so the two cannot disagree | [Distributed runtime](internals/distributed-runtime.md) |
| Security | TLS and mTLS on the cluster transport | [Network stack](internals/network-stack.md) |
| Kubernetes | Helm chart and a `ClinkCluster`/`ClinkJob` operator with savepoint-on-upgrade | [Distributed runtime](internals/distributed-runtime.md) |
| HTTP API and console | JSON API, Prometheus metrics, SSE events, embedded dashboard; the full [operations console](https://github.com/orhaugh/clink-fe) is a separate project | [Distributed runtime](internals/distributed-runtime.md) |
| Efficiency | Measured 1.9x to 5.3x less CPU per event than a JVM stream processor (median 2.45x, all 17 nexmark queries, five-node cluster, correctness-gated, raw per-run data published); a separate page prices it in instances, dollars and modelled CO2e | [Benchmarks](benchmarks.md) / [Cost and environmental footprint](efficiency.md) |
| Allocator choice | jemalloc as the process allocator: on by default in the runtime image, opt-in for source builds (`CLINK_WITH_JEMALLOC=ON`, Linux). Steady state: +5% throughput on a windowed query, neutral elsewhere, no memory change. Under repeated recovery (QUAL-10): a glibc worker retained 3.3 GB after 27 restarts with the job gone; jemalloc with prompt purging plus Arrow's pool routed to it (`ARROW_DEFAULT_MEMORY_POOL=system`) cut per-restart growth from ~14 MiB to under 1 MiB and returned the memory. Allocator retention, not a leak - measured, not inferred. The allocator in use is reported by `clink_node --version` and at node startup | [Build options](https://github.com/orhaugh/clink#allocator-clink_with_jemalloc-linux-on-by-default-in-the-runtime-image) |

## Observability and debugging

| Capability | Notes | Reference |
| --- | --- | --- |
| Metrics | Counter and gauge registry, Prometheus exposition, per-process system gauges | [Distributed runtime](internals/distributed-runtime.md) |
| Structured logging | `clink::log` facade with an in-memory ring served over HTTP and zstd-rotated files | [Distributed runtime](internals/distributed-runtime.md) |
| Data lineage | Per-job source/sink dataset graph with column-level lineage for SQL; built-in OpenLineage exporter | [Data lineage](internals/data-lineage.md) |
| Deterministic replay | Flight recorder captures per-epoch operator input; `clink replay` re-executes byte-identically offline and can freeze an incident into a regression test | [Replay determinism](internals/replay-determinism.md) |

## Embedding and APIs

| Capability | Notes | Reference |
| --- | --- | --- |
| Embedded engine | `clink run pipeline.sql`: one process, no daemons; first result in ~155 ms (gated by a release test) | [Embedded execution](internals/embedded.md) |
| C ABI | `libclink` embeds the engine behind a pure-C ABI with Arrow C stream results | [Embedded execution](internals/embedded.md) |
| Python | `pyclink` returns results as pyarrow tables | [Embedded execution](internals/embedded.md) |
| Arrow wire format | Every operator-to-operator data frame is an Arrow IPC stream; columnar schemas for built-in types, binary fallback for user types | [Network stack](internals/network-stack.md) |
| Testing framework | Public `clink::test` harnesses: state inspection, snapshot/restore, failure injection, `MiniCluster` | [Testing framework](internals/testing-framework.md) |

## Connectors

Twenty-plus sources and sinks, each documented with dependencies, factory
names, options, and SQL usage in the [connector catalogue](connectors/README.md):
Kafka, Postgres (snapshot, CDC, sink), MySQL, ClickHouse, Cassandra, MongoDB,
Redis, S3 and S3 Parquet, GCS Parquet, Azure Parquet, WebHDFS Parquet,
Iceberg, Avro, HTTP, MQTT, NATS, Pulsar, RabbitMQ, file and built-ins.
