# 001: Arrow is the data plane, not an integration

**Status:** adopted, load-bearing across the engine.

## Context

A stream engine moves typed records between operators, across process
boundaries, into snapshots, and out to files and object stores. Each of those
crossings needs a serialisation format, and the conventional choice is a
private one per crossing: a wire codec here, a snapshot format there, a
separate writer for every columnar file format. Every private format is code
to maintain, a compatibility surface to version, and a wall between the
engine's data and the rest of the ecosystem.

Apache Arrow already defines a typed, columnar, language-independent memory
layout with an IPC stream format, mature file writers (Parquet), and readers
in every analytical tool that matters.

## Decision

Arrow IPC is the engine's one representation for data at rest and in motion:

- **On the wire**, every operator-to-operator data frame is an Arrow IPC
  stream (`Kind::ArrowBatch`). Built-in types get real columnar schemas; user
  types ride a binary-column fallback, so the frame format is uniform even
  when the payload is opaque. Control frames (watermarks, barriers) stay
  compact and separate.
- **In snapshots**, keyed-state checkpoints and savepoints are documented
  Arrow IPC (see [the format contract](../internals/state-snapshot-format.md)).
  Backends that keep native files internally still export the canonical Arrow
  form.
- **In operators**, execution itself can stay columnar: batches carry an Arrow
  sidecar, operators opt in via `process_columnar()`, and the keyed shuffle
  splits columnar batches without materialising rows.
- **At the edges**, Parquet and object-store connectors reuse the same
  batchers, and query surfaces serve live state as Arrow IPC streams.

## Consequences

- One serialisation seam (`ArrowBatcher<T>` alongside `Codec<T>`) instead of
  a format per crossing. The measured columnar path is several times faster
  than per-record encoding for built-in types (`clink_serde_bench`).
- Everything the engine persists or serves is readable by pyarrow, DuckDB,
  and Polars without clink code, which is what makes
  [state as open data](003-state-as-open-data.md) possible at all.
- The trade-offs accepted: Arrow (and for files, Parquet) became pinned,
  non-negotiable dependencies with a real toolchain cost, and user-defined
  types get the uniform binary fallback rather than automatic columnar
  schemas - deriving true columns for arbitrary structs is deliberately
  opt-in rather than magical.
