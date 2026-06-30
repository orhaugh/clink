# Data lineage

clink can describe, for every job, the external datasets it reads from and writes
to, and emit that description to an external lineage system. The intent is
integration: an operator running a metadata catalogue or a lineage store wants to
know that "job 42 reads Kafka topic `orders` and writes ClickHouse table
`db.sales`" without parsing clink internals. The feature has two halves, kept
deliberately separate:

- **Capture** is engine-internal and vendor-neutral. It derives a small lineage
  graph from the submitted job and exposes it over HTTP and on the event bus.
- **Export** is pluggable. A `LineageListener` translates the graph plus run-state
  into whatever an external system speaks. A built-in exporter ships the
  [OpenLineage](https://openlineage.io) run-event format, which DataHub, Apache
  Atlas, Marquez and others ingest; bespoke backends implement the listener
  interface directly.

The two are connected by one in-process event and one structured graph, so the
engine never depends on any particular lineage backend.

## The lineage model

The graph is a graph of connectors and datasets, not of physical operators. It is
shaped after the OpenLineage data model (a `(namespace, name)` dataset identity
plus open, additive facets) so the common adapter is a near-trivial mapping.

| Type | What it is |
| --- | --- |
| `LineageDataset` | One external data entity. `ns` is the storage system's address (`kafka://broker:9092`, `postgres://db:5432`, `s3://bucket`, `file`); `name` is the entity within it (a topic, a table, an object key, a path); `facets` is open string metadata (connector family, record format, element-schema hint, delivery mode). |
| `LineageVertex` | One connector: a source or a sink. Keyed by the graph-local operator `id`, with the stable `uid` when set. Source vertices also carry `boundedness` (`bounded` / `unbounded` / `unknown`). |
| `LineageEdge` | A coarse source-vertex to sink-vertex dependency. |
| `LineageGraph` | `sources`, `sinks`, `edges`. `to_json()` / `from_json()` round-trip it. |

Defined in `include/clink/lineage/lineage_graph.hpp`.

## Capture

`extract_lineage(const JobGraphSpec&)` (`src/lineage/lineage_graph.cpp`) is a pure
function of the submitted job spec. It needs no SQL context and no runtime, so it
covers both SQL-submitted and programmatic-API jobs through one path:

- **Source / sink classification** mirrors the planner and `snapshot_job_graph`: a
  source is an operator with no inputs; a sink is an operator nothing reads from.
  Side-output and split input refs (`id::tag`, `id.N`) resolve to the upstream id.
- **Edges** are real reachability over the operator DAG, not all-to-all. For each
  source, a forward walk collects every sink it can reach. A linear job yields one
  edge; a two-input join yields two.
- **Dataset identity** comes from each operator's `type` (the connector factory
  name) and `params` (the connector options). `connector_family()` strips the
  channel and direction tokens from the factory type (`kafka_2pc_sink_string` to
  `kafka`, `s3_parquet_string_source` to `s3_parquet`), and `dataset_for()` maps
  the family plus its params to a `(namespace, name)` pair.

### Dataset identity normalisation

`build_params` does not carry the `connector` key into the operator params, so the
family is read from the factory `type`. The namespace and name are then drawn from
the connector's locator params:

| Connector family | namespace | name |
| --- | --- | --- |
| kafka | `kafka://<brokers>` | topic |
| pulsar / nats / rabbitmq | `<scheme>://<service url or host>` | topic / subject / queue |
| kinesis / firehose / dynamodb | `<scheme>://<region>` | stream / table |
| pubsub | `pubsub://<project>` | topic / subscription |
| redis | `redis://<host:port>` | stream / key / channel |
| postgres / mysql / clickhouse / cassandra | `<scheme>://<host:port>` (host/port parsed from a libpq conninfo when present) | `<database>.<table>` |
| s3 / s3_parquet | `s3://<bucket>` | key / prefix |
| gcs_parquet / azure_parquet / webhdfs_parquet | `gs://` / `azure://` / `webhdfs://` + bucket or host | key / prefix / path |
| file / filesystem / parquet | `file` | path |
| iceberg | catalog uri or warehouse | `<namespace>.<table>` |
| delta | `delta` | path / table |
| elasticsearch / opensearch / splunk / prometheus / influxdb / http | `<scheme>://<host or url>` | index / job / measurement / url |
| (anything else) | `<family>://<authority>` from the common locator keys | first present of topic / table / path / key / ... |

The generic fallback guarantees that a connector not enumerated above still
produces a usable identity. Adding a connector means adding a branch to
`dataset_for()`; the family list it mirrors lives in `src/sql/physical_plan.cpp`.

## Lifecycle and exposure

The capture side rides the existing in-process `EventBus`
(`include/clink/runtime/event_bus.hpp`), the same bus that backs the
`/api/v1/events` SSE stream:

- At submit, `JobManager::submit_job` publishes `jm.job_lineage` with the lineage
  graph (payload `{"job_id":N,"job_name":"...","lineage":{...}}`). Best-effort: a
  job is never failed because of lineage.
- At termination, the existing `jm.job_completed` event carries the outcome
  (payload `{"job_id":N,"job_name":"...","status":"...","errors":<count>}`, plus
  an `"error"` string on failure). The `job_name` and `error` keys are additive;
  `errors` stays a count for existing consumers.

The graph is also available to poll, for tools that prefer pull over the event
stream:

```
GET /api/v1/jobs/:id/lineage
  -> {"job_id":N,"available":true,"lineage":{"sources":[...],"sinks":[...],"edges":[...]}}
```

served by `JobManager::snapshot_job_lineage`, a mirror of `snapshot_job_graph`
that reads the retained `JobGraphSpec` and runs the extractor. `available` is
false when the job exists but no graph was retained.

## Export

A `LineageListener` (`include/clink/lineage/lineage_listener.hpp`) is the export
hook. Listeners are **host-side** components, constructed inside the `clink_node`
process, never on the job-plugin `.so` path: a `.so` links its own private
`EventBus` singleton and would never see the host's events.

```mermaid
flowchart LR
  submit["submit_job"] -->|"jm.job_lineage"| bus["EventBus (host)"]
  done["job termination"] -->|"jm.job_completed"| bus
  bus --> disp["LineageDispatcher"]
  disp -->|"LineageEvent"| ol["OpenLineageExporter"]
  disp -->|"LineageEvent"| custom["your LineageListener"]
  ol -->|"START / COMPLETE run events"| ext["OpenLineage receiver<br/>(Marquez, DataHub, Atlas)"]
  jm["snapshot_job_lineage"] -->|"GET /api/v1/jobs/:id/lineage"| poll["polling tools"]
```

- `LineageDispatcher` subscribes to the bus, reconstructs a structured
  `LineageEvent` (`JobStarted` carries the graph; `JobCompleted` carries the
  status), and fans it out to its listeners. It runs on the publish thread, so a
  listener that does network I/O must not block there.
- `LineageListenerRegistry` is a named-factory registry mirroring the engine's
  other registries. `register_builtin_lineage_listeners()` registers the
  `openlineage` factory when the build includes the HTTP client.

### The OpenLineage exporter

`OpenLineageExporter` (`src/lineage/openlineage_exporter.cpp`) maps a
`JobStarted` to a START run event (sources as inputs, sinks as outputs) and a
`JobCompleted` to a COMPLETE / FAIL / ABORT, correlated by a job-derived `runId`.
A FAIL event carries the first failure as an OpenLineage `errorMessage` run
facet. Delivery is asynchronous: `on_event` serialises the event and pushes it
onto a bounded outbox; a worker thread POSTs from the outbox so the publish
thread is never blocked. Overflow drops the oldest queued event and counts the
drop.

The OpenLineage job name is the submitter's job name (`JobGraphSpec::name`),
carried through to both events; it falls back to `job_<id>` only when the job was
submitted unnamed. The name is set per submission path: the `?name=` query (or a
`name` in the spec body) for `POST /api/v1/jobs/spec` and `/api/v1/jobs`, and the
per-statement name for SQL. The name also rides the retained graph and the HA
manifest, so it survives a leader takeover.

Enable it on the JobManager:

```
clink_node --role=jm --http-port=8081 \
  --lineage-listener=openlineage \
  --lineage-endpoint=http://marquez:5000 \
  --lineage-namespace=prod
```

`--lineage-endpoint` is http only (the built-in client is plain HTTP). The
capture side (the HTTP endpoint and the event stream) is always on; the listener
flag only wires an outbound exporter. Under HA only the leader submits jobs, so
there is no double-emit and no leadership gating is needed.

## Scope

v1 is dataset-level lineage. Out of scope:

- **Column-level lineage.** Which source column feeds which sink column lives in
  the lowered SQL plan, not in the job spec. The facets are open, so this can be
  added later as a schema/column facet without breaking the model.
- **Lookup-join dimension tables.** A `connector='lookup'` dimension is an async
  function in the plan, not a source operator, so it is not captured as a source.
- **Cross-job dataset stitching.** Correlating one job's sink topic with another
  job's source topic is left to the external lineage system, which already does
  this by `(namespace, name)`.

## Source files

| File | Role |
| --- | --- |
| `include/clink/lineage/lineage_graph.hpp`, `src/lineage/lineage_graph.cpp` | The model, `extract_lineage`, `connector_family`, `dataset_for`, JSON round-trip. |
| `include/clink/lineage/lineage_listener.hpp`, `src/lineage/lineage_listener.cpp` | `LineageEvent`, `LineageListener`, the registry, and the `LineageDispatcher` EventBus bridge. |
| `include/clink/lineage/openlineage_exporter.hpp`, `src/lineage/openlineage_exporter.cpp` | The built-in OpenLineage HTTP exporter. |
| `src/cluster/job_manager.cpp` | `snapshot_job_lineage` and the `jm.job_lineage` emit in `submit_job`. |
| `tools/clink_node.cpp` | The `GET /api/v1/jobs/:id/lineage` route and the dispatcher wiring (`--lineage-*` flags). |
| `tests/test_lineage.cpp` | Unit tests for the extractor, the normaliser, JSON, and the dispatcher. |
