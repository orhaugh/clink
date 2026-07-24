# 003: State is an open dataset, not an internal format

**Status:** adopted; the snapshot format is a documented contract.

## Context

A stateful pipeline's keyed state is often its most valuable output: the
running aggregates, the joined context, the per-entity accumulations. In most
engines that state is trapped in a private checkpoint format - readable only
by the engine version that wrote it, inspectable only by restoring a job, and
exportable only by writing a second pipeline to sink it somewhere readable.

With [Arrow as the data plane](001-arrow-native-data-plane.md) already
decided, the marginal cost of making state readable was small; the value of
committing to it as a contract is large.

## Decision

Treat streaming state as an open Arrow dataset with three surfaces:

- **At rest**: checkpoints and savepoints are documented Arrow IPC
  ([format contract](../internals/state-snapshot-format.md)). Backends with
  native internal files (RocksDB SSTs) still export the canonical Arrow
  form. Snapshots open directly in pyarrow, DuckDB, and Polars, and export
  to Parquet and Iceberg.
- **Live**: a running job's keyed state is a serving surface. A SQL
  `GROUP BY` exposes its running aggregates automatically over HTTP - JSON
  point lookups per key, whole-slot scans, and the same scan as one Arrow
  IPC stream, so live state loads as a DataFrame with no client library.
- **Composable**: `connector='queryable_state'` lets one job `SELECT` from
  (or join against) another job's live state, making state a first-class
  input, not just an artefact.

## Consequences

- Inspecting, auditing, or migrating state needs no special tooling and no
  running job; an offline savepoint API covers read-and-transform.
- The snapshot format is now a compatibility promise. Changing it means
  versioning a documented contract, not refactoring an internal detail -
  that friction is accepted deliberately.
- Serving reads from a live job costs isolation work (routing, slot
  addressing, topology versions) that a closed engine would not need. The
  queryable-state layer carries that complexity so operators do not.

See [State and backends](../internals/state-and-backends.md) for the full
backend matrix and the serving surfaces.
