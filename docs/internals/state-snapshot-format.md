# The state snapshot format (stable contract)

> The on-disk and on-wire encoding of a job's keyed state - an Apache Arrow IPC stream any Arrow consumer can open - documented as a stable public contract, with its key encoding, metadata, derived projections and evolution policy.

## Overview

Every snapshot of clink keyed state is one Apache Arrow IPC stream: a schema, one RecordBatch, and an end-of-stream marker. Checkpoints and savepoints from the in-memory backend family are this format natively; RocksDB state renders to it on demand through the Arrow export; `clink state-export` writes it as a file. Because it is plain Arrow, pyarrow, DuckDB, Polars, Spark or any other Arrow reader opens it directly - no clink code required. This page is the format's contract: what a reader may rely on, what a writer must produce, and how the format is allowed to change.

The single in-tree writer is `SnapshotArrowWriter` (`include/clink/state/snapshot_arrow_writer.hpp`); every producer routes through it, so the bytes agree by construction. The reference reader is `InMemoryStateBackend::restore`.

## The stream

One Arrow IPC **stream** (not the file/Feather format): schema, exactly one RecordBatch, EOS. A zero-row batch is a valid, restorable snapshot of empty state. The stream is self-contained; there are no side files.

| Column | Arrow type | Nullable | Meaning |
| --- | --- | --- | --- |
| `op_id` | `uint64` | no | The operator id (`OperatorId::value()`) owning the entry |
| `key_bytes` | `binary` | no | The full encoded state key (layout below) |
| `value_bytes` | `binary` | no | The raw stored value bytes (ownership below) |

One row per `(operator, key)` entry. Row order is unspecified; readers must not rely on it. Writers currently emit deterministic orders (the RocksDB export sorts operators ascending with keys in byte order), but that is a convenience, not a guarantee.

## Schema metadata

The schema's `KeyValueMetadata` carries:

| Key | Value | Presence |
| --- | --- | --- |
| `clink.format_version` | `"1"` | Stamped on every stream the writer produces. A reader MUST treat absence as version 1 (streams written before the marker existed). |
| `clink.state_versions` | packed `StateVersionMap` | Present when the job registered schema-evolution stamps; absent means "no stamps recorded" (the control plane then assumes version 1 per state type). |

The `StateVersionMap` packing is line-oriented text: one `<op_id>|<state_type>|<version>` triple per line, `\n`-separated, empty string for an empty map (`StateVersionMap::pack` / `unpack`, `include/clink/state/schema_version.hpp`). Unparseable content is a corrupt snapshot, not an absent map.

Readers MUST ignore metadata keys they do not recognise; writers MAY add new keys without a format-version bump (metadata additions are compatible changes).

## key_bytes encoding

The stored key layout (produced by `KeyedState`, `include/clink/state/keyed_state.hpp`):

```
keyed state:     [ kg byte (0..127) ][ slot name ][ '|' ][ user key bytes ]
operator state:  [ 0xFF ][ slot name ][ '|' ][ user key bytes ]
```

- The leading byte of a keyed row is its **key group**: FNV-1a over the user key bytes, mod 128 (`kNumKeyGroups`). It is the rescale partitioning unit - a single-byte compare filters a snapshot by key-group range with no knowledge of codecs or slots.
- A leading byte `>= 128` (in practice `0xFF`, `kOperatorStateKeyPrefix`) marks **operator state** (source offsets, broadcast slots). These rows have no key group and are restored whole by every subtask.
- The slot name never contains `'|'` or `'\n'` (enforced at slot creation), so the first `'|'` after the prefix byte splits slot from user key unambiguously.
- A key with no `'|'` separator is legacy/raw; tooling surfaces it under the reserved slot name `<raw>`.

User key bytes are the operator's key codec output. The built-in conventions worth knowing when reading externally: an 8-byte user key is typically a little-endian int64 (the built-in int64 codec); string keys are raw UTF-8.

## value_bytes ownership

Value bytes belong to the operator's value codec; the format deliberately does not describe their internal layout. Two engine-level wrappers a reader may encounter:

- **TTL slots** prefix every value with `[ 8-byte expire-at-ms, little-endian ]` before the codec bytes.
- **Collection state** (`ListState`, `MapState`, ...) serialises the whole collection as one value per key.
- **Source offsets** (operator-state rows) are typically bare 8-byte little-endian int64s; the restore merge relies on this to keep the greater offset when subtask snapshots collide on the same key.

## The changelog variant

`ChangelogStateBackend` snapshots use the same encoding with one extra **leading** column:

| Column | Arrow type | Meaning |
| --- | --- | --- |
| `row_kind` | `uint8` | 0 = materialisation row, 1 = put, 2 = erase |

Restore replays the materialisation rows first, then applies the log rows in order.

## Writers and readers

| Component | Role |
| --- | --- |
| `SnapshotArrowWriter` | The one canonical writer (in-memory family snapshots, operator-state extraction, RocksDB exports) |
| `InMemoryStateBackend::restore` | Reference reader (also the sharded/file-backed/savepoint loader) |
| `InMemoryStateBackend::merge_snapshot_bytes` | Concatenates streams (scale-down form); preserves the first non-empty input's metadata |
| `RocksDBStateBackend::export_arrow_snapshot` / `rocksdb_checkpoint_to_arrow` | Render RocksDB state to this format |
| `clink state-export / state-cat / state-diff / state-query / check-savepoint` | CLI surface over the format |

## Derived projections (not restorable)

Three derived layouts exist for analytics; they decode `key_bytes` into columns and are NOT inputs to restore:

- **Parquet export** (`state-export --format=parquet`): `op_id: uint64, key_group: uint8, slot: utf8, user_key: binary, value_bytes: binary`, versions in the file's key-value metadata.
- **Iceberg export** (`--format=iceberg`): the same five columns with Iceberg types (`op_id` bit-cast to signed `long`).
- **Query projection** (`state-query`, `write_state_query_parquet`): adds rendered text and int64-reading columns for direct SQL filtering.

## Stability and evolution policy

Version 1 (current) guarantees:

1. The three-column shape, column names, types and non-nullability are stable.
2. The `key_bytes` layout (kg byte, slot, `'|'`, user key; `0xFF` operator-state prefix; 128 key groups) is stable.
3. Zero-row streams are valid. Row order is unspecified.
4. Metadata is open for additive extension; readers ignore unknown keys.

A change that any version-1 reader would misread (column changes, key-layout changes, container changes) bumps `clink.format_version`, and readers MUST reject a version above the highest they know rather than guess. Absence of the marker always reads as version 1. Compatible changes (new metadata keys, new derived projections, new writers) do not bump the version.

## Reading it externally

```python
import pyarrow.ipc as ipc
t = ipc.open_stream("checkpoint-9.snap").read_all()
print(t.schema.metadata)          # format version + packed state versions
print(t.num_rows, t.column("op_id")[0])
```

```sql
-- DuckDB: a checkpoint as a table (via the Parquet projection)
-- clink state-export --from=ckpt.snap --out=state.parquet
SELECT slot, count(*) FROM 'state.parquet' GROUP BY slot;
```

## Related

- [./state-and-backends.md](./state-and-backends.md) - the backends that produce and restore this format
- [./fault-tolerance-and-rescale.md](./fault-tolerance-and-rescale.md) - key groups, rescale filtering, and the state CLI verbs
- [./checkpointing.md](./checkpointing.md) - when snapshots are taken and acknowledged
